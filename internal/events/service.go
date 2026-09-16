package events

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/spinach/martech-engine/internal/queue"
)

// Service owns the ingestion HTTP surface (POST /api/v1/events).
type Service struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client
	cfg      *config.Config
	producer queue.Producer
}

// NewService is the registration convention consumed by cmd/api (CONTRACTS §1).
func NewService(pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config) *Service {
	return &Service{pool: pool, rdb: rdb, cfg: cfg, producer: queue.NewStreams(rdb)}
}

func (s *Service) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/events", s.rateLimit(), s.ingest)
}

// ingest handles POST /api/v1/events — CONTRACTS §2. Always async: 202 with
// {accepted, duplicates, rejected[]}. Malformed body or oversized batch → 400.
func (s *Service) ingest(c *gin.Context) {
	var req batchRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Events == nil {
		core.BadRequest(c, "malformed request body", nil)
		return
	}
	if len(req.Events) > maxBatch {
		core.BadRequest(c, "batch exceeds 500 events", nil)
		return
	}

	now := time.Now().UTC()
	resp := batchResponse{Rejected: []rejectedItem{}}
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
		outcome, reason := s.storeEvent(c.Request.Context(), in)
		switch outcome {
		case outcomeAccepted:
			resp.Accepted++
		case outcomeDuplicate:
			resp.Duplicates++
		default:
			resp.Rejected = append(resp.Rejected, rejectedItem{Index: i, Reason: reason})
		}
	}
	core.Metrics.EventsIngested.Add(int64(resp.Accepted))
	core.Metrics.Duplicates.Add(int64(resp.Duplicates))
	c.JSON(http.StatusAccepted, resp)
}

// storeEvent persists one validated event. Postgres events.event_id UNIQUE is
// the dedup authority: INSERT ... ON CONFLICT DO NOTHING RETURNING id — no row
// means duplicate. A real insert writes event_outbox in the SAME transaction
// (transactional outbox, CONTRACTS §2); after commit we best-effort XADD for
// low latency (the reconciler covers failures) and SETNX a dedup hint that is
// never used as authority.
func (s *Service) storeEvent(ctx context.Context, in eventInput) (ingestOutcome, string) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		slog.Error("ingest begin tx", "err", err)
		return outcomeRejected, "internal error"
	}
	defer tx.Rollback(ctx)

	var customerID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM customers WHERE external_id = $1`, in.CustomerID,
	).Scan(&customerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return outcomeRejected, "unknown customer_id"
	}
	if err != nil {
		slog.Error("ingest resolve customer", "err", err)
		return outcomeRejected, "internal error"
	}

	var campaignID *int64
	if in.CampaignID != "" {
		var cid int64
		err = tx.QueryRow(ctx,
			`SELECT id FROM campaigns WHERE external_id = $1`, in.CampaignID,
		).Scan(&cid)
		if errors.Is(err, pgx.ErrNoRows) {
			return outcomeRejected, "unknown campaign_id"
		}
		if err != nil {
			slog.Error("ingest resolve campaign", "err", err)
			return outcomeRejected, "internal error"
		}
		campaignID = &cid
	}

	occurredAt, _ := time.Parse(time.RFC3339, in.OccurredAt) // already validated
	payload := in.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	var dbID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO events (event_id, customer_id, campaign_id, channel, type, occurred_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING id`,
		in.EventID, customerID, campaignID, in.Channel, in.Type, occurredAt, payload,
	).Scan(&dbID)
	if errors.Is(err, pgx.ErrNoRows) {
		return outcomeDuplicate, "" // ON CONFLICT DO NOTHING returned no row
	}
	if err != nil {
		slog.Error("ingest insert event", "err", err)
		return outcomeRejected, "internal error"
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO event_outbox (event_db_id) VALUES ($1)`, dbID,
	); err != nil {
		slog.Error("ingest insert outbox", "err", err)
		return outcomeRejected, "internal error"
	}

	if err := tx.Commit(ctx); err != nil {
		slog.Error("ingest commit", "err", err)
		return outcomeRejected, "internal error"
	}

	// Post-commit hints — best effort only, errors intentionally ignored.
	// A missed XADD is republished by the reconciler; the SETNX is a read
	// optimization and never consulted to classify duplicates.
	_ = s.producer.Enqueue(ctx, queue.EventMessage{EventDBID: dbID, EventID: in.EventID})
	_ = s.rdb.SetNX(ctx, "dedup:"+in.EventID, 1, 24*time.Hour).Err()

	return outcomeAccepted, ""
}
