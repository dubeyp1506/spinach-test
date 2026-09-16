package core

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Metrics are minimal in-process counters for /system/health and docs.
// Production design replaces these with Prometheus/OTel — see SYSTEM_DESIGN.md.
var Metrics struct {
	Requests       atomic.Int64
	Errors5xx      atomic.Int64
	EventsIngested atomic.Int64
	Duplicates     atomic.Int64
}

// RequestID attaches a correlation id to every request/response/log line.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

// AccessLog emits one structured line per request with status + latency.
func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		Metrics.Requests.Add(1)
		if c.Writer.Status() >= 500 {
			Metrics.Errors5xx.Add(1)
		}
		slog.Info("request",
			"request_id", c.GetString("request_id"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
		)
	}
}
