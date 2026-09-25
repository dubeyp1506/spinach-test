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

// maxRequestIDLen bounds a client-supplied X-Request-ID: it is echoed into
// every log line and stored on events rows, so an unbounded header would be
// a cheap way to bloat both.
const maxRequestIDLen = 128

// RequestID attaches a correlation id to every request/response/log line.
// It also stores a request-scoped logger (request_id pre-attached) on the
// request context — handlers log via core.Log(ctx).
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if id == "" || len(id) > maxRequestIDLen {
			id = uuid.NewString()
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Request = c.Request.WithContext(
			WithLogger(c.Request.Context(), slog.Default().With("request_id", id)))
		c.Next()
	}
}

// AccessLog emits one structured line per request with status + latency.
// Level follows the outcome so LOG_LEVEL=warn keeps only failures:
// 5xx → error, 4xx → warn, everything else → info.
func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		status := c.Writer.Status()
		Metrics.Requests.Add(1)
		level := slog.LevelInfo
		switch {
		case status >= 500:
			Metrics.Errors5xx.Add(1)
			level = slog.LevelError
		case status >= 400:
			level = slog.LevelWarn
		}
		Log(c.Request.Context()).Log(c.Request.Context(), level, "request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"route", c.FullPath(),
			"status", status,
			"latency_ms", time.Since(start).Milliseconds(),
			"bytes", c.Writer.Size(),
			"client_ip", c.ClientIP(),
		)
	}
}
