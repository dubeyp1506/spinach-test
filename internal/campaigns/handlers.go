package campaigns

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/spinach/martech-engine/internal/core"
)

var (
	campaignStatuses = map[string]bool{"draft": true, "active": true, "paused": true, "completed": true}
	campaignChannels = map[string]bool{"email": true, "sms": true, "whatsapp": true, "push": true, "web": true}
)

// RegisterRoutes mounts the A4 campaign endpoints (CONTRACTS §4) on the
// /api/v1 router group wired in cmd/api.
func (s *Service) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/campaigns", s.listCampaigns)
	rg.GET("/campaigns/:id/analytics", s.campaignAnalytics)
}

// campaignSummary is one row of GET /campaigns (CONTRACTS §4).
type campaignSummary struct {
	ExternalID     string     `json:"external_id"`
	Name           string     `json:"name"`
	Objective      string     `json:"objective"`
	Channel        string     `json:"channel"`
	Status         string     `json:"status"`
	StartedAt      *time.Time `json:"started_at"`
	Sends          int64      `json:"sends"`
	OpenRate       float64    `json:"open_rate"`
	ConversionRate float64    `json:"conversion_rate"`
}

// GET /api/v1/campaigns — CONTRACTS §4. Cursor-paginated (cursor = internal
// campaign id) with ?status= and ?channel= filters. One LEFT JOIN + GROUP BY
// over the campaign_metrics rollup for summary metrics — no N+1, and no
// per-page scan of the events table.
func (s *Service) listCampaigns(c *gin.Context) {
	page := core.ParsePage(c)

	status := c.Query("status")
	if status != "" && !campaignStatuses[status] {
		core.BadRequest(c, "invalid status filter", nil)
		return
	}
	channel := c.Query("channel")
	if channel != "" && !campaignChannels[channel] {
		core.BadRequest(c, "invalid channel filter", nil)
		return
	}
	var after int64
	if page.Cursor != "" {
		n, err := strconv.ParseInt(page.Cursor, 10, 64)
		if err != nil || n < 0 {
			core.BadRequest(c, "invalid cursor", nil)
			return
		}
		after = n
	}

	rows, err := s.pool.Query(c.Request.Context(), `
		SELECT c.id, c.external_id, c.name, c.objective, c.channel, c.status, c.started_at,
		       COALESCE(SUM(m.count) FILTER (WHERE m.event_type = 'sent'), 0)::bigint      AS sends,
		       COALESCE(SUM(m.count) FILTER (WHERE m.event_type = 'delivered'), 0)::bigint AS delivered,
		       COALESCE(SUM(m.count) FILTER (WHERE m.event_type = 'opened'), 0)::bigint    AS opens,
		       COALESCE(SUM(m.count) FILTER (WHERE m.event_type = 'converted'), 0)::bigint AS conversions
		FROM campaigns c
		LEFT JOIN campaign_metrics m ON m.campaign_id = c.id
		WHERE c.id > $1
		  AND ($2::text = '' OR c.status = $2)
		  AND ($3::text = '' OR c.channel = $3)
		GROUP BY c.id
		ORDER BY c.id
		LIMIT $4`, after, status, channel, page.Limit+1)
	if err != nil {
		slog.Error("list campaigns", "request_id", c.GetString("request_id"), "err", err)
		core.Internal(c, err)
		return
	}
	defer rows.Close()

	items := []campaignSummary{}
	var ids []int64
	for rows.Next() {
		var it campaignSummary
		var id, delivered, opens, conv int64
		if err := rows.Scan(&id, &it.ExternalID, &it.Name, &it.Objective, &it.Channel,
			&it.Status, &it.StartedAt, &it.Sends, &delivered, &opens, &conv); err != nil {
			slog.Error("scan campaign", "request_id", c.GetString("request_id"), "err", err)
			core.Internal(c, err)
			return
		}
		it.OpenRate = rate(opens, delivered)
		it.ConversionRate = rate(conv, delivered)
		items = append(items, it)
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		slog.Error("list campaigns rows", "request_id", c.GetString("request_id"), "err", err)
		core.Internal(c, err)
		return
	}

	resp := core.ListResponse[campaignSummary]{Data: items}
	if len(items) > page.Limit {
		resp.HasMore = true
		items = items[:page.Limit]
		resp.Data = items
		resp.NextCursor = strconv.FormatInt(ids[len(items)-1], 10)
	}
	core.NoteActivity(c, "listed %d campaigns", len(resp.Data))
	c.JSON(http.StatusOK, resp)
}

// analyticsResponse flattens the §3 CampaignMetrics and adds the §4 extras.
type analyticsResponse struct {
	*CampaignMetrics
	Anomalies        []string         `json:"anomalies"`
	PlatformBaseline PlatformBaseline `json:"platform_baseline"`
}

// GET /api/v1/campaigns/{id}/analytics — CONTRACTS §4. {id} is the row's
// external_id. Returns CampaignMetrics + anomaly flags + platform baseline.
func (s *Service) campaignAnalytics(c *gin.Context) {
	ext := c.Param("id")

	var id int64
	var status string
	err := s.pool.QueryRow(c.Request.Context(),
		`SELECT id, status FROM campaigns WHERE external_id = $1`, ext,
	).Scan(&id, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		core.NotFound(c, "campaign")
		return
	}
	if err != nil {
		slog.Error("campaign lookup", "request_id", c.GetString("request_id"), "err", err)
		core.Internal(c, err)
		return
	}

	a, err := s.Analytics(c.Request.Context(), id, status)
	if err != nil {
		slog.Error("campaign analytics", "request_id", c.GetString("request_id"), "err", err)
		core.Internal(c, err)
		return
	}
	core.NoteActivity(c, "viewed analytics: %d sends, %d anomalies", a.Metrics.Sends, len(a.Anomalies))
	c.JSON(http.StatusOK, analyticsResponse{
		CampaignMetrics:  a.Metrics,
		Anomalies:        a.Anomalies,
		PlatformBaseline: a.Baseline,
	})
}
