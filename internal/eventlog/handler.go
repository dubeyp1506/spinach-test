package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spinach/martech-engine/internal/core"
)

// RegisterRoutes mounts GET /api/v1/logs.
func RegisterRoutes(rg *gin.RouterGroup, pool *pgxpool.Pool) {
	h := &handler{pool: pool}
	rg.GET("/logs", h.list)
}

type handler struct{ pool *pgxpool.Pool }

// LogEntry is one row of GET /logs. Internal ids never leave the API
// (CONTRACTS §6): customer and campaign are reported by external_id.
type LogEntry struct {
	ID         int64           `json:"id"`
	At         time.Time       `json:"at"`
	Stage      string          `json:"stage"`
	Level      string          `json:"level"`
	Message    string          `json:"message"`
	EventID    *string         `json:"event_id,omitempty"`
	CustomerID *string         `json:"customer_id,omitempty"`
	CampaignID *string         `json:"campaign_id,omitempty"`
	RequestID  *string         `json:"request_id,omitempty"`
	Worker     *string         `json:"worker,omitempty"`
	Attempt    *int            `json:"attempt,omitempty"`
	Details    json.RawMessage `json:"details"`
}

var validStages = map[string]bool{
	string(StageIngested): true, string(StageDuplicate): true, string(StageProcessed): true,
	string(StageRetry): true, string(StageDeadLettered): true, string(StageReplayed): true,
}

// levelsAtLeast implements ?level= as a minimum: level=warn returns warn and
// error — "show me the problems" is the common question.
var levelsAtLeast = map[string][]string{
	"debug": {"debug", "info", "warn", "error"},
	"info":  {"info", "warn", "error"},
	"warn":  {"warn", "error"},
	"error": {"error"},
}

// errNoMatch means a filter referenced an unknown customer/campaign: the
// answer is an empty page, not an error.
var errNoMatch = errors.New("no match")

// list handles GET /api/v1/logs — newest first, cursor = last seen id.
// Filters (all optional, ANDed): event_id, customer_id, campaign_id
// (external ids), request_id, stage, level (minimum), since (RFC3339).
//
// The WHERE clause is built from only the filters present, so each
// combination gets a plan that can use its index (idx_event_logs_event,
// _customer, _request, _problems) instead of a generic "$n IS NULL OR …"
// plan that can't.
func (h *handler) list(c *gin.Context) {
	ctx := c.Request.Context()
	page := core.ParsePage(c)

	var (
		conds []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		conds = append(conds, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}

	if page.Cursor != "" {
		n, err := strconv.ParseInt(page.Cursor, 10, 64)
		if err != nil || n <= 0 {
			core.BadRequest(c, "invalid cursor", nil)
			return
		}
		add("l.id < ?", n)
	}
	if v := c.Query("event_id"); v != "" {
		add("l.event_id = ?", v)
	}
	if v := c.Query("request_id"); v != "" {
		add("l.request_id = ?", v)
	}
	if v := c.Query("stage"); v != "" {
		if !validStages[v] {
			core.BadRequest(c, "invalid stage filter", map[string]any{"allowed": keys(validStages)})
			return
		}
		add("l.stage = ?", v)
	}
	if v := c.Query("level"); v != "" {
		lv, ok := levelsAtLeast[v]
		if !ok {
			core.BadRequest(c, "invalid level filter", map[string]any{"allowed": []string{"debug", "info", "warn", "error"}})
			return
		}
		if v == "warn" || v == "error" {
			// Matches the partial index predicate so idx_event_logs_problems applies.
			conds = append(conds, "l.level IN ('warn','error')")
		}
		add("l.level = ANY(?)", lv)
	}
	if v := c.Query("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			core.BadRequest(c, "since must be RFC3339", nil)
			return
		}
		add("l.created_at >= ?", t)
	}
	for _, f := range []struct{ param, table, col string }{
		{"customer_id", "customers", "l.customer_id"},
		{"campaign_id", "campaigns", "l.campaign_id"},
	} {
		v := c.Query(f.param)
		if v == "" {
			continue
		}
		id, err := resolve(ctx, h.pool, f.table, v)
		if errors.Is(err, errNoMatch) {
			c.JSON(http.StatusOK, core.ListResponse[LogEntry]{Data: []LogEntry{}})
			return
		}
		if err != nil {
			core.Log(ctx).Error("logs resolve filter", "param", f.param, "err", err)
			core.Internal(c, err)
			return
		}
		add(f.col+" = ?", id)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, page.Limit+1)
	sql := `
		SELECT l.id, l.created_at, l.stage, l.level, l.message, l.event_id,
		       cu.external_id, ca.external_id, l.request_id, l.worker, l.attempt, l.details
		FROM event_logs l
		LEFT JOIN customers cu ON cu.id = l.customer_id
		LEFT JOIN campaigns ca ON ca.id = l.campaign_id
		` + where + `
		ORDER BY l.id DESC
		LIMIT $` + strconv.Itoa(len(args))

	rows, err := h.pool.Query(ctx, sql, args...)
	if err != nil {
		core.Log(ctx).Error("logs query", "err", err)
		core.Internal(c, err)
		return
	}
	defer rows.Close()

	out := []LogEntry{}
	for rows.Next() {
		var e LogEntry
		if err := rows.Scan(&e.ID, &e.At, &e.Stage, &e.Level, &e.Message, &e.EventID,
			&e.CustomerID, &e.CampaignID, &e.RequestID, &e.Worker, &e.Attempt, &e.Details); err != nil {
			core.Log(ctx).Error("logs scan", "err", err)
			core.Internal(c, err)
			return
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		core.Log(ctx).Error("logs rows", "err", err)
		core.Internal(c, err)
		return
	}

	resp := core.ListResponse[LogEntry]{Data: out}
	if len(out) > page.Limit {
		out = out[:page.Limit]
		resp.Data = out
		resp.HasMore = true
		resp.NextCursor = strconv.FormatInt(out[len(out)-1].ID, 10)
	}
	c.JSON(http.StatusOK, resp)
}

// resolve maps an external_id to its internal id. Table names are literals
// supplied by list, never user input.
func resolve(ctx context.Context, pool *pgxpool.Pool, table, externalID string) (int64, error) {
	var id int64
	err := pool.QueryRow(ctx, `SELECT id FROM `+table+` WHERE external_id = $1`, externalID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, errNoMatch
	}
	return id, err
}

func keys(m map[string]bool) []string {
	return slices.Sorted(maps.Keys(m))
}
