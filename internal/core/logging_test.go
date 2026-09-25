package core

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLevel(t *testing.T) {
	assert.Equal(t, slog.LevelDebug, ParseLevel("DEBUG"))
	assert.Equal(t, slog.LevelWarn, ParseLevel("warning"))
	assert.Equal(t, slog.LevelError, ParseLevel("error"))
	assert.Equal(t, slog.LevelInfo, ParseLevel("nonsense"))
}

func TestLogFallsBackToDefault(t *testing.T) {
	assert.Same(t, slog.Default(), Log(context.Background()))
	l := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	assert.Same(t, l, Log(WithLogger(context.Background(), l)))
}

// Every request log line carries the request_id, and the id is echoed back.
// An oversized client-supplied id is replaced rather than logged verbatim.
func TestRequestIDAndAccessLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	r := gin.New()
	r.Use(RequestID(), AccessLog())
	r.GET("/ok", func(c *gin.Context) {
		Log(c.Request.Context()).Info("handler line")
		c.Status(http.StatusOK)
	})
	r.GET("/boom", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	req.Header.Set("X-Request-ID", "req-123")
	r.ServeHTTP(w, req)
	assert.Equal(t, "req-123", w.Header().Get("X-Request-ID"))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)
	for _, ln := range lines {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(ln), &m))
		assert.Equal(t, "req-123", m["request_id"], ln)
	}
	var access map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &access))
	assert.Equal(t, "INFO", access["level"])
	assert.Equal(t, "/ok", access["route"])

	buf.Reset()
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/boom", nil)
	req.Header.Set("X-Request-ID", strings.Repeat("a", 500))
	r.ServeHTTP(w, req)
	assert.Len(t, w.Header().Get("X-Request-ID"), 36, "oversized id replaced by a UUID")
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	assert.Equal(t, "ERROR", m["level"], "5xx logs at error level")
}
