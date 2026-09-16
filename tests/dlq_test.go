// Verifies CONTRACTS.md §4 system endpoints: GET /system/dlq returns
// cursor-paginated DLQ rows with id/event_id/error/attempts/failed_at, and
// POST /system/dlq/{id}/replay re-enqueues an entry ({replayed:true} +
// replayed_at stamped) while a bogus id returns 404.
package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dlqEntry struct {
	ID         int64          `json:"id"`
	EventID    *string        `json:"event_id"`
	Payload    map[string]any `json:"payload"`
	Error      string         `json:"error"`
	Attempts   int            `json:"attempts"`
	FailedAt   time.Time      `json:"failed_at"`
	ReplayedAt *time.Time     `json:"replayed_at"`
}

type dlqPage struct {
	Data       []dlqEntry `json:"data"`
	NextCursor string     `json:"next_cursor"`
	HasMore    bool       `json:"has_more"`
}

// The seed writes events_dlq rows directly (CONTRACTS §10), so the listing
// must be populated and honor the ListResponse cursor contract.
func TestDLQListPaginated(t *testing.T) {
	requireStack(t)

	status, raw := doJSON(t, http.MethodGet, "/system/dlq?limit=5", nil)
	require.Equal(t, http.StatusOK, status, "body=%s", raw)
	var page dlqPage
	require.NoError(t, json.Unmarshal(raw, &page))

	require.Len(t, page.Data, 5)
	for _, e := range page.Data {
		assert.Greater(t, e.ID, int64(0))
		assert.NotEmpty(t, e.Error, "every DLQ row carries an error")
		assert.Greater(t, e.Attempts, 0)
		assert.False(t, e.FailedAt.IsZero(), "failed_at must be set")
		assert.NotEmpty(t, e.Payload)
	}
	require.True(t, page.HasMore, "seeded DLQ holds >5 rows")
	require.NotEmpty(t, page.NextCursor)

	// Follow the cursor: next page must continue strictly after it.
	status, raw = doJSON(t, http.MethodGet,
		fmt.Sprintf("/system/dlq?limit=5&cursor=%s", page.NextCursor), nil)
	require.Equal(t, http.StatusOK, status, "body=%s", raw)
	var page2 dlqPage
	require.NoError(t, json.Unmarshal(raw, &page2))
	require.NotEmpty(t, page2.Data)
	lastID := page.Data[len(page.Data)-1].ID
	for _, e := range page2.Data {
		assert.Greater(t, e.ID, lastID, "cursor pages must not overlap")
	}
}

// Replay stamps replayed_at and returns {replayed:true} (§4 bonus endpoint).
func TestDLQReplay(t *testing.T) {
	requireStack(t)

	// Use a synthetic seeded row (evt_dlq_* has no backing events row, so the
	// replay publishes a harmless no-op message) that hasn't been replayed.
	id := queryInt(t, `
		SELECT id FROM events_dlq
		WHERE replayed_at IS NULL
		  AND (event_id IS NULL OR event_id LIKE 'evt\_dlq\_%')
		ORDER BY id DESC LIMIT 1`)

	status, raw := doJSON(t, http.MethodPost, fmt.Sprintf("/system/dlq/%d/replay", id), nil)
	require.Equal(t, http.StatusOK, status, "body=%s", raw)
	assert.Contains(t, string(raw), `"replayed":true`)

	assert.EqualValues(t, 1, queryInt(t,
		`SELECT count(*) FROM events_dlq WHERE id=$1 AND replayed_at IS NOT NULL`, id),
		"replay must stamp replayed_at")
}

func TestDLQReplayNotFound(t *testing.T) {
	requireStack(t)
	status, _ := doJSON(t, http.MethodPost, "/system/dlq/999999999/replay", nil)
	assert.Equal(t, http.StatusNotFound, status)

	status, _ = doJSON(t, http.MethodPost, "/system/dlq/notanumber/replay", nil)
	assert.Equal(t, http.StatusBadRequest, status)
}
