package events

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/queue"
)

const (
	reconcileInterval = 5 * time.Second // CONTRACTS §2: reconciler ~every 5s
	outboxBatch       = 500
	retryPause        = time.Second // backoff after read/DB errors
)

// Run is the worker entry point shared by cmd/worker and the api binary's
// embedded-worker mode (RUN_EMBEDDED_WORKER, CONTRACTS §9). It blocks until
// ctx is cancelled, then waits for its goroutines to stop.
func Run(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config, p Processor) {
	streams := queue.NewStreams(rdb)
	group := cfg.WorkerConsumerGroup
	// Unique consumer name per run: a crashed worker's pending entries are
	// migrated here by ClaimStale instead of being read by a stale name.
	consumer := "worker-" + uuid.NewString()[:8]

	slog.Info("event worker starting", "group", group, "consumer", consumer)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); consumeLoop(ctx, pool, streams, cfg, group, consumer, p) }()
	go func() { defer wg.Done(); reconcileLoop(ctx, pool, streams) }()
	go func() { defer wg.Done(); claimLoop(ctx, pool, streams, cfg, group, consumer, p) }()
	wg.Wait()

	slog.Info("event worker stopped")
}

// consumeLoop XREADGROUPs new messages and processes each in its own tx.
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
			slog.Error("worker read", "err", err)
			sleepCtx(ctx, retryPause)
			continue
		}
		for _, m := range msgs {
			processMessage(ctx, pool, streams, group, p, m, cfg)
		}
	}
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
			slog.Error("worker claim", "err", err)
			continue
		}
		for _, m := range msgs {
			processMessage(ctx, pool, streams, group, p, m, cfg)
		}
	}
}

// processMessage implements the atomic-processing rule (CONTRACTS §2): ONE
// Postgres tx — SELECT events FOR UPDATE → skip terminal rows →
// p.ProcessTx → mark processed → COMMIT → then XACK. A crash before ACK is
// harmless: redelivery sees status='processed' and acks without reprocessing.
func processMessage(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, group string, p Processor, m queue.Message, cfg *config.Config) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		slog.Error("worker begin tx", "err", err)
		return // unacked → reclaimed by ClaimStale
	}
	defer tx.Rollback(ctx)

	var evt StoredEvent
	err = tx.QueryRow(ctx, `
		SELECT id, event_id, customer_id, campaign_id, channel, type,
		       occurred_at, received_at, payload, status, attempts, last_error, processed_at
		FROM events WHERE id = $1 FOR UPDATE`, m.Payload.EventDBID,
	).Scan(&evt.ID, &evt.EventID, &evt.CustomerID, &evt.CampaignID, &evt.Channel,
		&evt.Type, &evt.OccurredAt, &evt.ReceivedAt, &evt.Payload, &evt.Status,
		&evt.Attempts, &evt.LastError, &evt.ProcessedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Event row is gone; nothing to process. Ack so it doesn't linger.
		_ = streams.Ack(ctx, group, m.ID)
		return
	}
	if err != nil {
		slog.Error("worker lock event", "event_db_id", m.Payload.EventDBID, "err", err)
		return
	}

	// Idempotent redelivery / terminal states: processed+duplicate per
	// contract; 'failed' is also terminal (already sent to DLQ).
	if evt.Status == "processed" || evt.Status == "duplicate" || evt.Status == "failed" {
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
		slog.Error("worker mark processed", "event_db_id", evt.ID, "err", err)
		return // rollback via defer → redelivery reprocesses
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("worker commit", "event_db_id", evt.ID, "err", err)
		return
	}
	if err := streams.Ack(ctx, group, m.ID); err != nil {
		slog.Error("worker ack", "stream_id", m.ID, "err", err)
	}
}

// recordFailure runs in a separate tx after the processing tx rolled back:
// attempts++ + last_error; when attempts reaches cfg.WorkerMaxAttempts the
// event goes status='failed' + events_dlq row + XADD stream:events:dlq + XACK.
// Otherwise it stays pending and unacked → retried via XAUTOCLAIM.
func recordFailure(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams, group string, m queue.Message, evt StoredEvent, procErr error, cfg *config.Config) {
	errMsg := procErr.Error()

	tx, err := pool.Begin(ctx)
	if err != nil {
		slog.Error("worker failure tx", "event_db_id", evt.ID, "err", err)
		return
	}
	defer tx.Rollback(ctx)

	var attempts int
	if err := tx.QueryRow(ctx,
		`UPDATE events SET attempts = attempts + 1, last_error = $2 WHERE id = $1 RETURNING attempts`,
		evt.ID, errMsg,
	).Scan(&attempts); err != nil {
		slog.Error("worker bump attempts", "event_db_id", evt.ID, "err", err)
		return
	}

	if !shouldDeadLetter(attempts, cfg.WorkerMaxAttempts) {
		if err := tx.Commit(ctx); err != nil {
			slog.Error("worker attempts commit", "event_db_id", evt.ID, "err", err)
		}
		return
	}

	if _, err := tx.Exec(ctx,
		`UPDATE events SET status = 'failed' WHERE id = $1`, evt.ID,
	); err != nil {
		slog.Error("worker mark failed", "event_db_id", evt.ID, "err", err)
		return
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO events_dlq (event_id, payload, error, attempts) VALUES ($1, $2::jsonb, $3, $4)`,
		evt.EventID, evt.Payload, errMsg, attempts,
	); err != nil {
		slog.Error("worker insert dlq", "event_db_id", evt.ID, "err", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("worker dlq commit", "event_db_id", evt.ID, "err", err)
		return
	}

	// Post-commit, best effort — the events_dlq row is the durable record.
	if err := streams.PublishDLQ(ctx, m.Payload, errMsg); err != nil {
		slog.Error("worker publish dlq stream", "event_db_id", evt.ID, "err", err)
	}
	if err := streams.Ack(ctx, group, m.ID); err != nil {
		slog.Error("worker ack dlq", "stream_id", m.ID, "err", err)
	}
}

// reconcileLoop is the outbox publisher (CONTRACTS §2): every 5s it publishes
// event_outbox rows whose handler-side XADD never happened or was lost, then
// stamps published_at. Double-publish is safe — consumers are idempotent.
func reconcileLoop(ctx context.Context, pool *pgxpool.Pool, streams *queue.Streams) {
	tick := time.NewTicker(reconcileInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := reconcileOnce(ctx, pool, streams); err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("outbox reconcile", "err", err)
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

	// XADD outside the tx's query stream; rows that fail stay unpublished and
	// are retried next cycle.
	published := make([]int64, 0, len(pending))
	for _, r := range pending {
		if err := streams.Enqueue(ctx, queue.EventMessage{
			EventDBID: r.eventDBID,
			EventID:   r.eventID,
		}); err != nil {
			slog.Error("outbox xadd", "outbox_id", r.id, "err", err)
			continue
		}
		published = append(published, r.id)
	}
	if len(published) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE event_outbox SET published_at = now() WHERE id = ANY($1)`, published,
		); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
