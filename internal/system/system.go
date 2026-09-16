// Package system implements the operational endpoints owned by the ingestion
// workstream: GET /system/health, GET /system/dlq, POST /system/dlq/{id}/replay.
// Contract: CONTRACTS.md §4.
package system

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/spinach/martech-engine/internal/config"
	"github.com/spinach/martech-engine/internal/core"
	"github.com/spinach/martech-engine/internal/queue"
)

var startedAt = time.Now()

type handlers struct {
	pool    *pgxpool.Pool
	rdb     *redis.Client
	streams *queue.Streams
}

// RegisterRoutes is the registration convention consumed by cmd/api.
func RegisterRoutes(rg *gin.RouterGroup, pool *pgxpool.Pool, rdb *redis.Client, cfg *config.Config) {
	h := &handlers{pool: pool, rdb: rdb, streams: queue.NewStreams(rdb)}
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
}

// GET /api/v1/system/health — CONTRACTS §4. 200 when postgres+redis are up,
// else 503 via core.Unavailable naming the failed dependencies.
func (h *handlers) health(c *gin.Context) {
	ctx := c.Request.Context()

	pgOK := h.pool.Ping(ctx) == nil
	redisOK := h.rdb.Ping(ctx).Err() == nil

	var depth, dlqSize int64
	if redisOK {
		depth, _ = h.streams.Depth(ctx)
	}
	if pgOK {
		_ = h.pool.QueryRow(ctx, `SELECT count(*) FROM events_dlq`).Scan(&dlqSize)
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

	var eventDBID int64
	if eventID != nil {
		_ = tx.QueryRow(ctx,
			`SELECT id FROM events WHERE event_id = $1`, *eventID,
		).Scan(&eventDBID)
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
	c.JSON(http.StatusOK, gin.H{"replayed": true})
}
