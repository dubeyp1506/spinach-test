package events

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/spinach/martech-engine/internal/eventlog"
	"github.com/spinach/martech-engine/internal/queue"
)

const (
	reconcileInterval = 5 * time.Second // CONTRACTS §2: reconciler ~every 5s
	outboxBatch       = 500
	retryPause        = time.Second // backoff after read/DB errors
	pruneEveryTicks   = 12          // event_logs retention prune ≈ once a minute
)

// Run is the worker entry point shared by cmd/worker and the api binary's
// embedded-worker mode (RUN_EMBEDDED_WORKER, CONTRACTS §9). It blocks until
// ctx is cancelled, then waits for its goroutines to stop.
func Run(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config, p Processor) {
	streams := queue.NewStreams(rdb)
	group := cfg.WorkerConsumerGroup
	// Stable consumer name per replica process (hostname+pid): on pod-style
	// deploys the replacement process reuses the name, so a crashed worker
	// doesn't leak a dead consumer into the group on every restart. Its stale
	// pending entries are migrated by ClaimStale regardless.
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	consumer := host + "-" + strconv.Itoa(os.Getpid())

	// Every log line and event_logs row from this worker names the consumer.
	ctx = eventlog.WithWorker(ctx, consumer)
	ctx = core.WithLogger(ctx, slog.Default().With("worker", consumer))
	log := core.Log(ctx)
	log.Info("event worker starting", "group", group,
		"batch_size", cfg.WorkerBatchSize, "concurrency", cfg.WorkerConcurrency,
		"event_log_mode", cfg.EventLogMode)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); consumeLoop(ctx, pool, streams, cfg, group, consumer, p) }()
	go func() { defer wg.Done(); reconcileLoop(ctx, pool, streams, cfg) }()
	go func() { defer wg.Done(); claimLoop(ctx, pool, streams, cfg, group, consumer, p) }()
	wg.Wait()

	log.Info("event worker stopped")
}

// consumeLoop XREADGROUPs batches of new messages and processes each batch
// with a bounded fan-out of cfg.WorkerConcurrency goroutines. processMessage
// is concurrency-safe: every event is its own tx with SELECT ... FOR UPDATE,
// so contending workers serialize on the row lock and the loser sees a
// terminal status.
func consumeLoop(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, cfg *config.Config, group, consumer string, p Processor) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, err := streams.Read(ctx, group, consumer, cfg.WorkerBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			core.Log(ctx).Error("worker read", "err", err)
			sleepCtx(ctx, retryPause)
			continue
		}
		processBatch(ctx, pool, streams, group, p, msgs, cfg)
	}
}

// processBatch runs processMessage over one batch with up to
// cfg.WorkerConcurrency goroutines (semaphore + WaitGroup). Concurrency <= 1
// keeps the old serial behavior.
func processBatch(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, group string, p Processor, msgs []queue.Message, cfg *config.Config) {
	if cfg.WorkerConcurrency <= 1 {
		for _, m := range msgs {
			processMessage(ctx, pool, streams, group, p, m, cfg)
		}
		return
	}
	sem := make(chan struct{}, cfg.WorkerConcurrency)
	var wg sync.WaitGroup
	for _, m := range msgs {
		sem <- struct{}{}
		wg.Add(1)
		go func(m queue.Message) {
			defer wg.Done()
			defer func() { <-sem }()
			processMessage(ctx, pool, streams, group, p, m, cfg)
		}(m)
	}
	wg.Wait()
}

// claimLoop XAUTOCLAIMs messages idle longer than cfg.WorkerClaimIdleMs
// (failed/crashed workers, or retry-after-error deliveries) and processes them.
func claimLoop(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, cfg *config.Config, group, consumer string, p Processor) {
	// Clamp so a tiny/zero idle setting can't steal in-flight messages.
	idleMs := cfg.WorkerClaimIdleMs
	if idleMs < 1000 {
		idleMs = 1000
	}
	interval := time.Duration(idleMs) * time.Millisecond
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		msgs, err := streams.ClaimStale(ctx, group, consumer, idleMs, cfg.WorkerBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			core.Log(ctx).Error("worker claim", "err", err)
			continue
		}
		if len(msgs) > 0 {
			core.Log(ctx).Warn("reclaimed stale messages from idle consumers", "count", len(msgs))
		}
		processBatch(ctx, pool, streams, group, p, msgs, cfg)
	}
}

// processMessage implements the atomic-processing rule (CONTRACTS §2): ONE
// Postgres tx — SELECT events FOR UPDATE → skip terminal rows →
// p.ProcessTx → mark processed → COMMIT → then XACK. A crash before ACK is
// harmless: redelivery sees status='processed' and acks without reprocessing.
func processMessage(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, group string, p Processor, m queue.Message, cfg *config.Config) {
	start := time.Now()
	log := core.Log(ctx).With("event_db_id", m.Payload.EventDBID, "event_id", m.Payload.EventID, "stream_id", m.ID)

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Error("worker begin tx", "err", err)
		return // unacked → reclaimed by ClaimStale
	}
	defer tx.Rollback(ctx)

	var evt StoredEvent
	var requestID *string
	err = tx.QueryRow(ctx, `
		SELECT id, event_id, customer_id, campaign_id, channel, type,
		       occurred_at, received_at, payload, status, attempts, last_error, processed_at,
		       request_id
		FROM events WHERE id = $1 FOR UPDATE`, m.Payload.EventDBID,
	).Scan(&evt.ID, &evt.EventID, &evt.CustomerID, &evt.CampaignID, &evt.Channel,
		&evt.Type, &evt.OccurredAt, &evt.ReceivedAt, &evt.Payload, &evt.Status,
		&evt.Attempts, &evt.LastError, &evt.ProcessedAt, &requestID)
	if errors.Is(err, pgx.ErrNoRows) {
		// Event row is gone; nothing to process. Ack so it doesn't linger.
		log.Warn("stream message points at missing event row; acking")
		_ = streams.Ack(ctx, group, m.ID)
		return
	}
	if err != nil {
		log.Error("worker lock event", "err", err)
		return
	}
	if requestID != nil {
		evt.RequestID = *requestID
		log = log.With("request_id", evt.RequestID)
	}
	ctx = core.WithLogger(ctx, log)

	// Idempotent redelivery / terminal states: processed+duplicate per
	// contract; 'failed' is also terminal (already sent to DLQ).
	if evt.Status == "processed" || evt.Status == "duplicate" || evt.Status == "failed" {
		log.Debug("redelivery of terminal event; acking without work", "status", evt.Status)
		_ = tx.Commit(ctx)
		_ = streams.Ack(ctx, group, m.ID)
		return
	}

	if err := p.ProcessTx(ctx, tx, evt); err != nil {
		_ = tx.Rollback(ctx)
		recordFailure(ctx, pool, streams, group, m, evt, err, cfg)
		return
	}

	if _, err := tx.Exec(ctx,
		`UPDATE events SET status = 'processed', processed_at = now() WHERE id = $1`, evt.ID,
	); err != nil {
		log.Error("worker mark processed", "err", err)
		return // rollback via defer → redelivery reprocesses
	}
	tookMs := time.Since(start).Milliseconds()
	// In the processing tx: the "processed" row commits iff the event did.
	if err := eventlog.WriteIsolated(ctx, tx, eventlog.Mode(cfg.EventLogMode),
		lifecycleEntry(ctx, evt, eventlog.StageProcessed, eventlog.LevelInfo,
			"event applied to profile and campaign metrics", evt.Attempts+1,
			map[string]any{"took_ms": tookMs, "type": evt.Type, "channel": evt.Channel}),
	); err != nil {
		log.Warn("event log write failed; processing unaffected", "err", err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Error("worker commit", "err", err)
		return
	}
	if err := streams.Ack(ctx, group, m.ID); err != nil {
		log.Error("worker ack", "err", err)
	}
	// Debug, not info: at thousands of events/s a per-event info line is the
	// log bill. LOG_LEVEL=debug turns it on when tracing an issue.
	log.Debug("event processed", "type", evt.Type, "channel", evt.Channel,
		"attempt", evt.Attempts+1, "took_ms", tookMs)
}

// lifecycleEntry builds the event_logs row for one processing step.
func lifecycleEntry(ctx context.Context, evt StoredEvent, stage eventlog.Stage, level eventlog.Level, msg string, attempt int, details map[string]any) eventlog.Entry {
	e := eventlog.Entry{
		EventID:    evt.EventID,
		CustomerID: evt.CustomerID,
		Stage:      stage,
		Level:      level,
		Message:    msg,
		RequestID:  evt.RequestID,
		Worker:     eventlog.WorkerFrom(ctx),
		Attempt:    attempt,
		Details:    details,
	}
	if evt.CampaignID != nil {
		e.CampaignID = *evt.CampaignID
	}
	return e
}

// recordFailure runs in a separate tx after the processing tx rolled back:
// attempts++ + last_error; when attempts reaches cfg.WorkerMaxAttempts the
// event goes status='failed' + events_dlq row + XADD stream:events:dlq + XACK.
// Otherwise it stays pending and unacked → retried via XAUTOCLAIM.
func recordFailure(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, group string, m queue.Message, evt StoredEvent, procErr error, cfg *config.Config) {
	errMsg := procErr.Error()
	log := core.Log(ctx)

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Error("worker failure tx", "err", err)
		return
	}
	defer tx.Rollback(ctx)

	var attempts int
	if err := tx.QueryRow(ctx,
		`UPDATE events SET attempts = attempts + 1, last_error = $2 WHERE id = $1 RETURNING attempts`,
		evt.ID, errMsg,
	).Scan(&attempts); err != nil {
		log.Error("worker bump attempts", "err", err)
		return
	}
	mode := eventlog.Mode(cfg.EventLogMode)

	if !shouldDeadLetter(attempts, cfg.WorkerMaxAttempts) {
		if err := eventlog.WriteIsolated(ctx, tx, mode,
			lifecycleEntry(ctx, evt, eventlog.StageRetry, eventlog.LevelWarn,
				"processing failed; will retry", attempts, eventlog.ErrorDetail(errMsg)),
		); err != nil {
			log.Warn("event log write failed", "err", err)
		}
		if err := tx.Commit(ctx); err != nil {
			log.Error("worker attempts commit", "err", err)
			return
		}
		log.Warn("event processing failed; will retry",
			"attempt", attempts, "max_attempts", cfg.WorkerMaxAttempts, "err", errMsg)
		return
	}

	if _, err := tx.Exec(ctx,
		`UPDATE events SET status = 'failed' WHERE id = $1`, evt.ID,
	); err != nil {
		log.Error("worker mark failed", "err", err)
		return
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO events_dlq (event_id, payload, error, attempts) VALUES ($1, $2::jsonb, $3, $4)`,
		evt.EventID, evt.Payload, errMsg, attempts,
	); err != nil {
		log.Error("worker insert dlq", "err", err)
		return
	}
	if err := eventlog.WriteIsolated(ctx, tx, mode,
		lifecycleEntry(ctx, evt, eventlog.StageDeadLettered, eventlog.LevelError,
			"retries exhausted; moved to dead-letter queue", attempts, eventlog.ErrorDetail(errMsg)),
	); err != nil {
		log.Warn("event log write failed", "err", err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Error("worker dlq commit", "err", err)
		return
	}
	log.Error("event dead-lettered", "attempts", attempts, "err", errMsg)

	// Post-commit, best effort — the events_dlq row is the durable record.
	if err := streams.PublishDLQ(ctx, m.Payload, errMsg); err != nil {
		log.Error("worker publish dlq stream", "err", err)
	}
	if err := streams.Ack(ctx, group, m.ID); err != nil {
		log.Error("worker ack dlq", "err", err)
	}
}

// reconcileLoop is the outbox publisher + pending sweeper (CONTRACTS §2):
// every 5s it publishes event_outbox rows whose handler-side XADD never
// happened (deleting them on success), then re-enqueues events stranded at
// status='pending' by a lost/evicted stream entry. Double-publish is safe —
// consumers are idempotent.
func reconcileLoop(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, cfg *config.Config) {
	tick := time.NewTicker(reconcileInterval)
	defer tick.Stop()
	log := core.Log(ctx)
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := reconcileOnce(ctx, pool, streams); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("outbox reconcile", "err", err)
		}
		if err := sweepPendingOnce(ctx, pool, streams); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("pending sweep", "err", err)
		}
		if n%pruneEveryTicks == 0 {
			if removed, err := eventlog.Prune(ctx, pool, cfg.EventLogRetentionDays); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("event log prune", "err", err)
			} else if removed > 0 {
				log.Info("event log pruned", "rows", removed, "retention_days", cfg.EventLogRetentionDays)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func reconcileOnce(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT o.id, o.event_db_id, e.event_id
		FROM event_outbox o
		JOIN events e ON e.id = o.event_db_id
		WHERE o.published_at IS NULL
		ORDER BY o.id
		LIMIT $1
		FOR UPDATE OF o SKIP LOCKED`, outboxBatch)
	if err != nil {
		return err
	}
	type outboxRow struct {
		id        int64
		eventDBID int64
		eventID   string
	}
	var pending []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.eventDBID, &r.eventID); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(pending) == 0 {
		return tx.Commit(ctx)
	}

	// XADD outside the tx's query stream. The outbox row's only job is to
	// carry the id to the stream, so once the XADD lands the terminal action
	// is DELETE — stamping published_at would leave millions of dead rows
	// per day. The rows stay locked FOR UPDATE until COMMIT, so a crash or
	// commit failure rolls back the delete and the row is republished next
	// cycle (consumers are idempotent). Failed XADDs keep their rows.
	published := make([]int64, 0, len(pending))
	for _, r := range pending {
		if err := streams.Enqueue(ctx, queue.EventMessage{
			EventDBID: r.eventDBID,
			EventID:   r.eventID,
		}); err != nil {
			core.Log(ctx).Error("outbox xadd", "outbox_id", r.id, "err", err)
			continue
		}
		published = append(published, r.id)
	}
	if len(published) > 0 {
		if _, err := tx.Exec(ctx,
			`DELETE FROM event_outbox WHERE id = ANY($1)`, published,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Non-zero means the ingest fast path failed to publish (Redis blip or a
	// crash after commit) — the reconciler is doing its job, worth seeing.
	if len(published) > 0 {
		core.Log(ctx).Info("outbox reconciler republished events", "count", len(published))
	}
	return nil
}

// sweepPendingOnce is the recovery net for the "published but lost" hole:
// if a stream entry is evicted (MAXLEN) or the group/pending state is lost,
// nothing revisits events stuck at status='pending'. Re-XADD any pending row
// older than 30s (idx_events_status makes the lookup cheap). No outbox row
// is written — the outbox's job ended at publish — and lock-then-check in
// processMessage makes duplicate deliveries harmless.
func sweepPendingOnce(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams) error {
	rows, err := pool.Query(ctx, `
		SELECT id, event_id
		FROM events
		WHERE status = 'pending'
		  AND received_at < now() - interval '30 seconds'
		ORDER BY id
		LIMIT $1`, outboxBatch)
	if err != nil {
		return err
	}
	var stranded []queue.EventMessage
	for rows.Next() {
		var m queue.EventMessage
		if err := rows.Scan(&m.EventDBID, &m.EventID); err != nil {
			rows.Close()
			return err
		}
		stranded = append(stranded, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(stranded) > 0 {
		core.Log(ctx).Warn("re-enqueueing events stuck in pending", "count", len(stranded))
	}
	for _, m := range stranded {
		if err := streams.Enqueue(ctx, m); err != nil {
			core.Log(ctx).Error("sweep xadd", "event_db_id", m.EventDBID, "err", err)
			continue // row stays pending → retried next cycle
		}
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
