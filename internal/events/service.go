package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/spinach/martech-engine/internal/eventlog"
	"github.com/spinach/martech-engine/internal/queue"
)

// maxBodyBytes caps the ingest request body so ShouldBindJSON cannot buffer
// an unbounded payload before the maxBatch check; 4 MiB comfortably holds
// 500 events with realistic payloads.
const maxBodyBytes = 4 << 20

// Service owns the ingestion HTTP surface (POST /api/v1/events).
type Service struct {
	pool *pgxpool.Pool
	rdb  *redis.Client
	cfg  *config.Config
}

// NewService is the registration convention consumed by cmd/api (CONTRACTS §1).
func NewService(pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config) *Service {
	return &Service{pool: pool, rdb: rdb, cfg: cfg}
}

func (s *Service) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/events", s.ingest)
}

// ingest handles POST /api/v1/events — CONTRACTS §2. Always async: 202 with
// {accepted, duplicates, rejected[]}. Malformed body or oversized batch → 400.
// The batch is persisted in ONE transaction; if that tx fails the whole
// request gets 503 (the client retries the batch idempotently) rather than a
// 202 itemizing every event as rejected.
func (s *Service) ingest(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
	var req batchRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Events == nil {
		core.BadRequest(c, "malformed request body", nil)
		return
	}
	if len(req.Events) > maxBatch {
		core.BadRequest(c, "batch exceeds 500 events", nil)
		return
	}
	// The limiter charges one token per EVENT, not per request — a single
	// call can carry up to maxBatch events.
	if !s.rateLimit(c, len(req.Events)) {
		return // 429 already written, or fail-open
	}

	ctx := c.Request.Context()
	log := core.Log(ctx)
	start := time.Now()
	now := start.UTC()
	resp := batchResponse{Rejected: []rejectedItem{}}
	items := make([]validatedItem, 0, len(req.Events))
	for i, raw := range req.Events {
		var in eventInput
		if err := json.Unmarshal(raw, &in); err != nil {
			resp.Rejected = append(resp.Rejected, rejectedItem{Index: i, Reason: "malformed event object"})
			continue
		}
		if reason := validateEvent(in, now); reason != "" {
			resp.Rejected = append(resp.Rejected, rejectedItem{Index: i, Reason: reason})
			continue
		}
		items = append(items, validatedItem{index: i, in: in})
	}

	if err := s.storeBatch(ctx, c.GetString("request_id"), items, &resp); err != nil {
		log.Error("ingest batch tx failed", "batch_size", len(req.Events), "err", err)
		core.Unavailable(c, "database")
		return
	}
	// storeBatch appends resolution rejections after validation rejections;
	// restore input order so rejected[] reads exactly like the old per-item loop.
	slices.SortFunc(resp.Rejected, func(a, b rejectedItem) int { return a.Index - b.Index })

	core.Metrics.EventsIngested.Add(int64(resp.Accepted))
	core.Metrics.Duplicates.Add(int64(resp.Duplicates))
	log.Info("ingest batch",
		"batch_size", len(req.Events),
		"accepted", resp.Accepted,
		"duplicates", resp.Duplicates,
		"rejected", len(resp.Rejected),
		"took_ms", time.Since(start).Milliseconds(),
	)
	if log.Enabled(ctx, slog.LevelDebug) {
		for _, r := range resp.Rejected {
			log.Debug("ingest item rejected", "index", r.Index, "reason", r.Reason)
		}
	}
	c.JSON(http.StatusAccepted, resp)
}

// validatedItem pairs an event that passed per-item validation with its
// index in the request, needed for rejected[] bookkeeping inside storeBatch.
type validatedItem struct {
	index int
	in    eventInput
}

// resolvedItem is a validatedItem whose external ids resolved to internal
// BIGINTs, with occurred_at/payload in insert-ready form.
type resolvedItem struct {
	index      int
	in         eventInput
	customerID int64
	campaignID *int64
	occurredAt time.Time
	payload    string
}

// storeBatch persists a batch of validated events in ONE transaction
// (CONTRACTS §2): two ANY($1) lookups resolve every distinct external_id, one
// multi-row INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING id,
// event_id splits accepted from duplicates (Postgres UNIQUE is the ONLY dedup
// authority), and accepted rows get their event_outbox rows in the same tx.
// The ingest request_id is stored on every inserted row (events.request_id)
// and the event_logs rows for the batch are written in the same tx.
// Any DB error aborts the whole batch — the caller answers 503.
func (s *Service) storeBatch(ctx context.Context, requestID string, items []validatedItem, resp *batchResponse) error {
	if len(items) == 0 {
		return nil
	}

	// Distinct external_ids → two lookup queries per batch instead of two
	// per event (~6 RTTs/event → a handful per batch).
	custExt := map[string]struct{}{}
	campExt := map[string]struct{}{}
	for _, it := range items {
		custExt[it.in.CustomerID] = struct{}{}
		if it.in.CampaignID != "" {
			campExt[it.in.CampaignID] = struct{}{}
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	customers, err := resolveIDs(ctx, tx, "customers", keys(custExt))
	if err != nil {
		return err
	}
	campaigns, err := resolveIDs(ctx, tx, "campaigns", keys(campExt))
	if err != nil {
		return err
	}

	// Same rejection shape as before: unknown references reject the item,
	// the rest of the batch still lands.
	resolved := make([]resolvedItem, 0, len(items))
	for _, it := range items {
		cid, ok := customers[it.in.CustomerID]
		if !ok {
			resp.Rejected = append(resp.Rejected, rejectedItem{Index: it.index, Reason: "unknown customer_id"})
			continue
		}
		var camp *int64
		if it.in.CampaignID != "" {
			id, ok := campaigns[it.in.CampaignID]
			if !ok {
				resp.Rejected = append(resp.Rejected, rejectedItem{Index: it.index, Reason: "unknown campaign_id"})
				continue
			}
			camp = &id
		}
		occ, _ := time.Parse(time.RFC3339, it.in.OccurredAt) // already validated
		payload := it.in.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		resolved = append(resolved, resolvedItem{
			index: it.index, in: it.in, customerID: cid,
			campaignID: camp, occurredAt: occ, payload: string(payload),
		})
	}
	if len(resolved) == 0 {
		return nil // read-only tx; deferred rollback is enough
	}

	// One multi-row INSERT for the batch via pgx.NamedArgs. ON CONFLICT
	// (event_id) DO NOTHING silently skips existing event_ids — including a
	// repeated event_id inside this batch — and RETURNING hands back only the
	// rows that actually inserted, keyed by the unique event_id.
	var sb strings.Builder
	sb.WriteString(`INSERT INTO events (event_id, customer_id, campaign_id, channel, type, occurred_at, payload, request_id) VALUES `)
	args := pgx.NamedArgs{"rid": nilIfEmpty(requestID)}
	for i, r := range resolved {
		if i > 0 {
			sb.WriteByte(',')
		}
		k := "r" + strconv.Itoa(i)
		fmt.Fprintf(&sb, `(@%[1]s_eid, @%[1]s_cid, @%[1]s_camp, @%[1]s_ch, @%[1]s_ty, @%[1]s_occ, @%[1]s_pay::jsonb, @rid)`, k)
		args[k+"_eid"] = r.in.EventID
		args[k+"_cid"] = r.customerID
		args[k+"_camp"] = r.campaignID
		args[k+"_ch"] = r.in.Channel
		args[k+"_ty"] = r.in.Type
		args[k+"_occ"] = r.occurredAt
		args[k+"_pay"] = r.payload
	}
	sb.WriteString(` ON CONFLICT (event_id) DO NOTHING RETURNING id, event_id`)

	rows, err := tx.Query(ctx, sb.String(), args)
	if err != nil {
		return err
	}
	inserted := make(map[string]int64, len(resolved))
	for rows.Next() {
		var dbID int64
		var eventID string
		if err := rows.Scan(&dbID, &eventID); err != nil {
			rows.Close()
			return err
		}
		inserted[eventID] = dbID
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// The first input occurrence of each returned event_id claims the new
	// row; in-batch repeats and rows skipped by ON CONFLICT count as
	// duplicates — identical classification to the old per-item loop.
	accepted := make([]int64, 0, len(resolved))
	acceptedEventIDs := make([]string, 0, len(resolved))
	logs := make([]eventlog.Entry, 0, len(resolved))
	for _, r := range resolved {
		entry := eventlog.Entry{
			EventID:    r.in.EventID,
			CustomerID: r.customerID,
			RequestID:  requestID,
			Level:      eventlog.LevelInfo,
			Details:    map[string]any{"channel": r.in.Channel, "type": r.in.Type},
		}
		if r.campaignID != nil {
			entry.CampaignID = *r.campaignID
		}
		if dbID, ok := inserted[r.in.EventID]; ok {
			delete(inserted, r.in.EventID)
			accepted = append(accepted, dbID)
			acceptedEventIDs = append(acceptedEventIDs, r.in.EventID)
			resp.Accepted++
			entry.Stage, entry.Message = eventlog.StageIngested, "event accepted"
		} else {
			resp.Duplicates++
			entry.Stage, entry.Message = eventlog.StageDuplicate, "duplicate event_id ignored"
		}
		logs = append(logs, entry)
	}
	// Same tx as the insert, so a rolled-back batch leaves no "ingested"
	// rows; isolated in a savepoint so a log failure can't fail the batch.
	if err := eventlog.WriteIsolated(ctx, tx, eventlog.Mode(s.cfg.EventLogMode), logs...); err != nil {
		core.Log(ctx).Warn("event log write failed; batch still committed", "err", err)
	}

	// Transactional outbox: same tx, one multi-row insert for accepted rows.
	if len(accepted) > 0 {
		if _, err := tx.Exec(ctx,
			`INSERT INTO event_outbox (event_db_id) SELECT * FROM unnest($1::bigint[])`,
			accepted,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	s.publishAccepted(ctx, accepted, acceptedEventIDs)
	return nil
}

// publishAccepted does the post-commit best-effort work (CONTRACTS §2): all
// XADDs go out in ONE pipelined RTT, then the outbox rows are DELETED for the
// entries that published — their only job was carrying the id to the stream,
// and the reconciler deletes rows it publishes, so stamping published_at
// would leak ~10M dead rows/day. Every failure is warn-only — unpublished
// outbox rows are the reconciler's job, lost stream entries the sweeper's.
func (s *Service) publishAccepted(ctx context.Context, dbIDs []int64, eventIDs []string) {
	if len(dbIDs) == 0 {
		return
	}
	pipe := s.rdb.Pipeline()
	for i := range dbIDs {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: queue.StreamEvents,
			Values: map[string]any{
				"event_db_id": dbIDs[i],
				"event_id":    eventIDs[i],
			},
		})
	}
	cmds, _ := pipe.Exec(ctx)

	published := make([]int64, 0, len(cmds))
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			core.Log(ctx).Warn("ingest xadd failed; reconciler will republish", "event_db_id", dbIDs[i], "err", err)
			continue
		}
		published = append(published, dbIDs[i])
	}
	if len(published) == 0 {
		return
	}
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM event_outbox WHERE event_db_id = ANY($1)`,
		published,
	); err != nil {
		core.Log(ctx).Warn("ingest delete published outbox", "err", err)
	}
}

// resolveIDs maps external_id → internal BIGINT id for one lookup table in a
// single ANY($1) query. Callers pass table literals only.
func resolveIDs(ctx context.Context, tx pgx.Tx, table string, extIDs []string) (map[string]int64, error) {
	m := make(map[string]int64, len(extIDs))
	if len(extIDs) == 0 {
		return m, nil
	}
	rows, err := tx.Query(ctx,
		fmt.Sprintf(`SELECT id, external_id FROM %s WHERE external_id = ANY($1)`, table),
		extIDs,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var ext string
		if err := rows.Scan(&id, &ext); err != nil {
			return nil, err
		}
		m[ext] = id
	}
	return m, rows.Err()
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
