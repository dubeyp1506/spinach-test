// Package system implements the operational endpoints owned by the ingestion
// workstream: GET /system/health, GET /system/dlq, POST /system/dlq/{id}/replay.
// Contract: CONTRACTS.md §4.
package system

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
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

var startedAt = time.Now()

type handlers struct {
	pool    *pgxpool.Pool
	rdb     *redis.Client
	streams *queue.Streams
	logMode eventlog.Mode
}

// RegisterRoutes is the registration convention consumed by cmd/api.
func RegisterRoutes(rg *gin.RouterGroup, pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config) {
	h := &handlers{pool: pool, rdb: rdb, streams: queue.NewStreams(rdb), logMode: eventlog.Mode(cfg.EventLogMode)}
	rg.GET("/system/health", h.health)
	rg.GET("/system/dlq", h.listDLQ)
	rg.POST("/system/dlq/:id/replay", h.replayDLQ)
}

type healthResponse struct {
	Status     string  `json:"status"`
	Postgres   string  `json:"postgres"`
	Redis      string  `json:"redis"`
	QueueDepth int64   `json:"queue_depth"`
	DLQSize    int64   `json:"dlq_size"`
	UptimeS    float64 `json:"uptime_s"`
	// Version is the deployed git commit. Render injects RENDER_GIT_COMMIT;
	// the CD job polls this until it equals the pushed SHA, so "deployed"
	// means the new code is serving, not just that a deploy was requested.
	Version string `json:"version,omitempty"`
}

// version is read once: the commit can't change for a running process.
var version = os.Getenv("RENDER_GIT_COMMIT")

// GET /api/v1/system/health — CONTRACTS §4. 200 when postgres+redis are up,
// else 503 via core.Unavailable naming the failed dependencies.
func (h *handlers) health(c *gin.Context) {
	// Bound the whole check so a hung dependency can't hang the endpoint.
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	pgOK := h.pool.Ping(ctx) == nil
	redisOK := h.rdb.Ping(ctx).Err() == nil

	var depth, dlqSize int64
	if redisOK {
		// Pending (delivered-not-acked) count, not all-time XLEN.
		depth, _ = h.streams.Depth(ctx)
	}
	if pgOK {
		// Planner estimate — O(1) at any DLQ size, accurate enough for health.
		_ = h.pool.QueryRow(ctx,
			`SELECT reltuples::bigint FROM pg_class WHERE relname = 'events_dlq'`).Scan(&dlqSize)
	}

	if !pgOK || !redisOK {
		var down []string
		if !pgOK {
			down = append(down, "postgres")
		}
		if !redisOK {
			down = append(down, "redis")
		}
		core.Unavailable(c, strings.Join(down, ","))
		return
	}

	c.JSON(http.StatusOK, healthResponse{
		Status:     "ok",
		Postgres:   "up",
		Redis:      "up",
		QueueDepth: depth,
		DLQSize:    dlqSize,
		UptimeS:    time.Since(startedAt).Seconds(),
		Version:    version,
	})
}

type dlqEntry struct {
	ID         int64           `json:"id"`
	EventID    *string         `json:"event_id,omitempty"`
	Payload    json.RawMessage `json:"payload"`
	Error      string          `json:"error"`
	Attempts   int             `json:"attempts"`
	FailedAt   time.Time       `json:"failed_at"`
	ReplayedAt *time.Time      `json:"replayed_at,omitempty"`
}

// GET /api/v1/system/dlq — CONTRACTS §4. Cursor-paginated by row id via
// core.ParsePage / core.ListResponse.
func (h *handlers) listDLQ(c *gin.Context) {
	page := core.ParsePage(c)
	var cursor int64
	if page.Cursor != "" {
		n, err := strconv.ParseInt(page.Cursor, 10, 64)
		if err != nil || n < 0 {
			core.BadRequest(c, "invalid cursor", nil)
			return
		}
		cursor = n
	}

	rows, err := h.pool.Query(c.Request.Context(), `
		SELECT id, event_id, payload, error, attempts, failed_at, replayed_at
		FROM events_dlq WHERE id > $1 ORDER BY id LIMIT $2`,
		cursor, page.Limit+1)
	if err != nil {
		core.Internal(c, err)
		return
	}
	defer rows.Close()

	entries := []dlqEntry{}
	for rows.Next() {
		var e dlqEntry
		if err := rows.Scan(&e.ID, &e.EventID, &e.Payload, &e.Error,
			&e.Attempts, &e.FailedAt, &e.ReplayedAt); err != nil {
			core.Internal(c, err)
			return
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		core.Internal(c, err)
		return
	}

	resp := core.ListResponse[dlqEntry]{Data: entries}
	if len(entries) > page.Limit {
		entries = entries[:page.Limit]
		resp.Data = entries
		resp.HasMore = true
		resp.NextCursor = strconv.FormatInt(entries[len(entries)-1].ID, 10)
	}
	core.NoteActivity(c, "listed %d dead-lettered events", len(resp.Data))
	c.JSON(http.StatusOK, resp)
}

// POST /api/v1/system/dlq/{id}/replay — CONTRACTS §4. Re-publishes the failed
// event onto stream:events and stamps replayed_at. When the backing events row
// still exists it is reset to pending so the worker reprocesses it; a DLQ row
// with no matching event (e.g. seeded invalid payloads) is republished as-is
// and harmlessly dropped by the consumer.
func (h *handlers) replayDLQ(c *gin.Context) {
	dlqID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || dlqID <= 0 {
		core.BadRequest(c, "invalid dlq id", nil)
		return
	}
	ctx := c.Request.Context()

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		core.Internal(c, err)
		return
	}
	defer tx.Rollback(ctx)

	var eventID *string
	var payload json.RawMessage
	err = tx.QueryRow(ctx,
		`SELECT event_id, payload FROM events_dlq WHERE id = $1`, dlqID,
	).Scan(&eventID, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		core.NotFound(c, "dlq entry")
		return
	}
	if err != nil {
		core.Internal(c, err)
		return
	}

	var eventDBID, customerID int64
	var campaignID *int64
	var requestID *string
	if eventID != nil {
		_ = tx.QueryRow(ctx,
			`SELECT id, customer_id, campaign_id, request_id FROM events WHERE event_id = $1`, *eventID,
		).Scan(&eventDBID, &customerID, &campaignID, &requestID)
	}
	if eventDBID > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE events SET status = 'pending', attempts = 0 WHERE id = $1 AND status = 'failed'`,
			eventDBID,
		); err != nil {
			core.Internal(c, err)
			return
		}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE events_dlq SET replayed_at = now() WHERE id = $1`, dlqID,
	); err != nil {
		core.Internal(c, err)
		return
	}
	// Operator action → audit row (recorded in "errors" mode too). The
	// replaying request's id is kept in details; request_id stays the
	// original ingest id so the whole history of the event shares one key.
	entry := eventlog.Entry{
		CustomerID: customerID,
		Stage:      eventlog.StageReplayed,
		Level:      eventlog.LevelInfo,
		Message:    "dead-lettered event replayed by operator",
		Details:    map[string]any{"dlq_id": dlqID, "replay_request_id": c.GetString("request_id")},
	}
	if eventID != nil {
		entry.EventID = *eventID
	}
	if campaignID != nil {
		entry.CampaignID = *campaignID
	}
	if requestID != nil {
		entry.RequestID = *requestID
	}
	if err := eventlog.WriteIsolated(ctx, tx, h.logMode, entry); err != nil {
		core.Log(ctx).Warn("event log write failed", "err", err)
	}
	if err := tx.Commit(ctx); err != nil {
		core.Internal(c, err)
		return
	}

	msg := queue.EventMessage{EventDBID: eventDBID}
	if eventID != nil {
		msg.EventID = *eventID
	}
	if err := h.streams.Enqueue(ctx, msg); err != nil {
		core.Unavailable(c, "queue")
		return
	}
	core.Log(ctx).Info("dlq entry replayed", "dlq_id", dlqID, "event_db_id", eventDBID)
	if eventID != nil {
		core.NoteActivity(c, "replayed dead-lettered event %s", *eventID)
	} else {
		core.NoteActivity(c, "replayed dlq entry %d", dlqID)
	}
	c.JSON(http.StatusOK, gin.H{"replayed": true})
}
