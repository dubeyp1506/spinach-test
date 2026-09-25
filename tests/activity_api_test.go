// Verifies the activity log: every API operation — successful or failed —
// becomes a row in GET /api/v1/activity with its action, entity, status,
// summary or error, and request id. Writes are async (batched ~500 ms), so
// the test polls.
package tests

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type activityRow struct {
	ID        int64   `json:"id"`
	Action    string  `json:"action"`
	Method    string  `json:"method"`
	Path      string  `json:"path"`
	Entity    *string `json:"entity"`
	Status    int     `json:"status"`
	LatencyMs int     `json:"latency_ms"`
	RequestID *string `json:"request_id"`
	Summary   *string `json:"summary"`
	Error     *string `json:"error"`
}

func getActivity(t *testing.T, q url.Values) (int, []activityRow) {
	t.Helper()
	status, raw := doJSON(t, http.MethodGet, "/activity?"+q.Encode(), nil)
	var page struct {
		Data []activityRow `json:"data"`
	}
	if status == http.StatusOK {
		require.NoError(t, json.Unmarshal(raw, &page), string(raw))
	}
	return status, page.Data
}

// callWithRequestID performs one API call tagged with reqID.
func callWithRequestID(t *testing.T, method, path, reqID, body string) int {
	t.Helper()
	req, err := http.NewRequest(method, apiBase+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", reqID)
	resp, err := httpClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	return resp.StatusCode
}

func waitForActivity(t *testing.T, reqID string) activityRow {
	t.Helper()
	var rows []activityRow
	pollUntil(t, 10*time.Second, "activity row for "+reqID, func() bool {
		_, rows = getActivity(t, url.Values{"request_id": {reqID}})
		return len(rows) == 1
	})
	return rows[0]
}

func TestActivityRecordsOperations(t *testing.T) {
	requireStack(t)

	// A successful read.
	cust := pickHighActivityCustomer(t)
	viewID := "it-act-view-" + runTag
	require.Equal(t, http.StatusOK, callWithRequestID(t, http.MethodGet, "/customers/"+cust, viewID, ""))
	row := waitForActivity(t, viewID)
	assert.Equal(t, "customer.view", row.Action)
	assert.Equal(t, http.StatusOK, row.Status)
	require.NotNil(t, row.Entity)
	assert.Equal(t, cust, *row.Entity)
	require.NotNil(t, row.Summary)
	assert.Contains(t, *row.Summary, "viewed profile")
	assert.Nil(t, row.Error)

	// A write with a handler summary.
	predID := "it-act-pred-" + runTag
	require.Equal(t, http.StatusOK, callWithRequestID(t, http.MethodPost, "/predictions/channel", predID,
		`{"objective":"conversion"}`))
	row = waitForActivity(t, predID)
	assert.Equal(t, "prediction.channel", row.Action)
	require.NotNil(t, row.Summary)
	assert.Contains(t, *row.Summary, "recommended")

	// A failed operation keeps its error.
	badID := "it-act-bad-" + runTag
	require.Equal(t, http.StatusBadRequest, callWithRequestID(t, http.MethodPost, "/predictions/channel", badID,
		`{"objective":"fame"}`))
	row = waitForActivity(t, badID)
	assert.Equal(t, http.StatusBadRequest, row.Status)
	require.NotNil(t, row.Error)
	assert.Contains(t, *row.Error, "validation_failed")

	// Unknown API path is recorded too.
	missID := "it-act-404-" + runTag
	require.Equal(t, http.StatusNotFound, callWithRequestID(t, http.MethodGet, "/no-such-endpoint", missID, ""))
	row = waitForActivity(t, missID)
	assert.Equal(t, "unknown", row.Action)
	assert.Equal(t, http.StatusNotFound, row.Status)
}

func TestActivityFilters(t *testing.T) {
	requireStack(t)

	// Health checks and log reads are not recorded (machine / self traffic).
	hID := "it-act-health-" + runTag
	callWithRequestID(t, http.MethodGet, "/system/health", hID, "")
	time.Sleep(1500 * time.Millisecond) // > one flush interval
	_, rows := getActivity(t, url.Values{"request_id": {hID}})
	assert.Empty(t, rows)

	_, rows = getActivity(t, url.Values{"outcome": {"error"}, "limit": {"50"}})
	for _, r := range rows {
		assert.GreaterOrEqual(t, r.Status, 400)
	}
	_, rows = getActivity(t, url.Values{"action": {"customer."}, "limit": {"50"}})
	for _, r := range rows {
		assert.True(t, strings.HasPrefix(r.Action, "customer."), r.Action)
	}

	for _, q := range []url.Values{
		{"outcome": {"maybe"}}, {"since": {"yesterday"}}, {"cursor": {"-1"}},
	} {
		status, _ := getActivity(t, q)
		assert.Equal(t, http.StatusBadRequest, status, q.Encode())
	}
}
