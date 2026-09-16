// Verifies CONTRACTS.md §4 GET /system/health: 200 with the full field set
// {status, postgres, redis, queue_depth, dlq_size, uptime_s} while the live
// dependencies are up.
package tests

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemHealth(t *testing.T) {
	requireStack(t)

	status, raw := doJSON(t, http.MethodGet, "/system/health", nil)
	require.Equal(t, http.StatusOK, status, "body=%s", raw)

	var h struct {
		Status     string  `json:"status"`
		Postgres   string  `json:"postgres"`
		Redis      string  `json:"redis"`
		QueueDepth int64   `json:"queue_depth"`
		DLQSize    int64   `json:"dlq_size"`
		UptimeS    float64 `json:"uptime_s"`
	}
	require.NoError(t, json.Unmarshal(raw, &h))

	assert.Equal(t, "ok", h.Status)
	assert.Equal(t, "up", h.Postgres)
	assert.Equal(t, "up", h.Redis)
	assert.GreaterOrEqual(t, h.QueueDepth, int64(0))
	assert.GreaterOrEqual(t, h.DLQSize, int64(0))
	assert.Greater(t, h.UptimeS, 0.0)

	// The reported DLQ size must reflect the real table (seeded rows exist).
	assert.Equal(t, queryInt(t, `SELECT count(*) FROM events_dlq`), h.DLQSize)
}
