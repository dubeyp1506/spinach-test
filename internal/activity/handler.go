package activity

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spinach/martech-engine/internal/core"
)

// RegisterRoutes mounts GET /api/v1/activity.
func RegisterRoutes(rg *gin.RouterGroup, pool *pgxpool.Pool) {
	h := &handler{pool: pool}
	rg.GET("/activity", h.list)
}

type handler struct{ pool *pgxpool.Pool }

// Row is one entry of GET /activity.
type Row struct {
	ID        int64     `json:"id"`
	At        time.Time `json:"at"`
	Action    string    `json:"action"`
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	Query     *string   `json:"query,omitempty"`
	Entity    *string   `json:"entity,omitempty"`
	Status    int       `json:"status"`
	LatencyMs int       `json:"latency_ms"`
	RequestID *string   `json:"request_id,omitempty"`
	ClientIP  *string   `json:"client_ip,omitempty"`
	Summary   *string   `json:"summary,omitempty"`
	Error     *string   `json:"error,omitempty"`
}

// list handles GET /api/v1/activity — newest first, cursor = last seen id.
// Filters (optional, ANDed): action (exact, or a prefix ending in "." such
// as "customer."), entity, request_id, outcome=ok|error, since (RFC3339).
// Only the filters present go into the WHERE clause, so each combination
// can use its index.
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
		add("id < ?", n)
	}
	if v := c.Query("action"); v != "" {
		if strings.HasSuffix(v, ".") {
			add("action LIKE ?", v+"%")
		} else {
			add("action = ?", v)
		}
	}
	if v := c.Query("entity"); v != "" {
		add("entity = ?", v)
	}
	if v := c.Query("request_id"); v != "" {
		add("request_id = ?", v)
	}
	switch c.Query("outcome") {
	case "":
	case "ok":
		conds = append(conds, "status < 400")
	case "error":
		conds = append(conds, "status >= 400") // matches idx_activity_failures
	default:
		core.BadRequest(c, "outcome must be ok or error", nil)
		return
	}
	if v := c.Query("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			core.BadRequest(c, "since must be RFC3339", nil)
			return
		}
		add("created_at >= ?", t)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, page.Limit+1)
	rows, err := h.pool.Query(ctx, `
		SELECT id, created_at, action, method, path, query, entity, status, latency_ms,
		       request_id, client_ip, summary, error
		FROM activity_logs `+where+`
		ORDER BY id DESC
		LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		core.Log(ctx).Error("activity query", "err", err)
		core.Internal(c, err)
		return
	}
	defer rows.Close()

	out := []Row{}
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.At, &r.Action, &r.Method, &r.Path, &r.Query, &r.Entity,
			&r.Status, &r.LatencyMs, &r.RequestID, &r.ClientIP, &r.Summary, &r.Error); err != nil {
			core.Log(ctx).Error("activity scan", "err", err)
			core.Internal(c, err)
			return
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		core.Log(ctx).Error("activity rows", "err", err)
		core.Internal(c, err)
		return
	}

	resp := core.ListResponse[Row]{Data: out}
	if len(out) > page.Limit {
		out = out[:page.Limit]
		resp.Data = out
		resp.HasMore = true
		resp.NextCursor = strconv.FormatInt(out[len(out)-1].ID, 10)
	}
	c.JSON(http.StatusOK, resp)
}
