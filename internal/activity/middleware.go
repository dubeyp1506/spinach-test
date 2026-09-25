package activity

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spinach/martech-engine/internal/core"
)

// actions names each route as an operation a person would recognise. A route
// missing here is still recorded, as "<method> <route>", so nothing is lost
// when an endpoint is added without updating this table.
var actions = map[string]string{
	"POST /api/v1/events":                  "events.ingest",
	"GET /api/v1/customers/:id":            "customer.view",
	"GET /api/v1/customers/:id/timeline":   "customer.timeline",
	"GET /api/v1/campaigns":                "campaign.list",
	"GET /api/v1/campaigns/:id/analytics":  "campaign.analytics",
	"POST /api/v1/audience/recommend":      "audience.recommend",
	"POST /api/v1/campaigns/:id/analyze":   "ai.analyze",
	"POST /api/v1/campaigns/:id/recommend": "ai.recommend",
	"POST /api/v1/predictions/channel":     "prediction.channel",
	"GET /api/v1/system/dlq":               "dlq.list",
	"POST /api/v1/system/dlq/:id/replay":   "dlq.replay",
	"GET /api/v1/logs":                     "logs.events",
	"GET /api/v1/activity":                 "logs.activity",
	"GET /api/v1/system/health":            "system.health",
}

// skipped routes are machine traffic or self-referential: Render's health
// check hits /system/health every few seconds, and recording "viewed the
// activity log" each time the Logs page refreshes would bury real activity.
var skipped = map[string]bool{
	"system.health": true,
	"logs.events":   true,
	"logs.activity": true,
}

const (
	maxQueryLen     = 500
	maxUserAgentLen = 200
)

// ActionFor maps a request to its action name ("" route = unmatched path).
func ActionFor(method, route string) string {
	if route == "" {
		return "unknown"
	}
	if a, ok := actions[method+" "+route]; ok {
		return a
	}
	return method + " " + route
}

// apiPrefix scopes recording to API calls: static UI files and redirects
// are not operations.
const apiPrefix = "/api/"

// Middleware records one Entry per API request after the handler runs.
// Mount it on the engine (not the /api/v1 group) after core.RequestID: Gin
// runs group middleware only for matched routes, so a group mount would
// silently miss requests to unknown API paths.
func Middleware(r *Recorder) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !strings.HasPrefix(c.Request.URL.Path, apiPrefix) {
			c.Next()
			return
		}
		start := time.Now()
		c.Next()

		action := ActionFor(c.Request.Method, c.FullPath())
		if skipped[action] {
			return
		}
		e := Entry{
			At:        start,
			RequestID: c.GetString("request_id"),
			Action:    action,
			Method:    c.Request.Method,
			Route:     c.FullPath(),
			Path:      c.Request.URL.Path,
			Query:     truncate(c.Request.URL.RawQuery, maxQueryLen),
			Entity:    c.Param("id"),
			Status:    c.Writer.Status(),
			LatencyMs: time.Since(start).Milliseconds(),
			ClientIP:  c.ClientIP(),
			UserAgent: truncate(c.Request.UserAgent(), maxUserAgentLen),
			Summary:   c.GetString(core.ActivitySummaryKey),
			Error:     c.GetString(core.ActivityErrorKey),
		}
		if e.Error == "" && e.Status >= 500 {
			e.Error = "internal error" // panics recovered by gin.Recovery
		}
		r.Record(e)
	}
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
