// Verifies GET /api/v1/logs and request correlation: an event ingested with
// X-Request-ID gets "ingested" and "processed" rows carrying that id (the
// worker reads it from events.request_id), a re-post gets a "duplicate" row,
// and filters validate. Assumes the API runs with EVENT_LOG_MODE=all (the
// default).
package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logEntry struct {
	ID         int64          `json:"id"`
	Stage      string         `json:"stage"`
	Level      string         `json:"level"`
	Message    string         `json:"message"`
	EventID    *string        `json:"event_id"`
	CustomerID *string        `json:"customer_id"`
	RequestID  *string        `json:"request_id"`
	Worker     *string        `json:"worker"`
	Attempt    *int           `json:"attempt"`
	Details    map[string]any `json:"details"`
}

type logsPage struct {
	Data       []logEntry `json:"data"`
	NextCursor string     `json:"next_cursor"`
	HasMore    bool       `json:"has_more"`
}

func getLogs(t *testing.T, q url.Values) (int, logsPage) {
	t.Helper()
	status, raw := doJSON(t, http.MethodGet, "/logs?"+q.Encode(), nil)
	var page logsPage
	if status == http.StatusOK {
		require.NoError(t, json.Unmarshal(raw, &page), string(raw))
	}
	return status, page
}

// postWithRequestID posts one event batch with an explicit X-Request-ID.
func postWithRequestID(t *testing.T, reqID string, events any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"events": events})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, apiBase+"/events", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", reqID)
	resp, err := httpClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))
	assert.Equal(t, reqID, resp.Header.Get("X-Request-ID"))
}

func stagesOf(entries []logEntry) map[string]logEntry {
	m := map[string]logEntry{}
	for _, e := range entries {
		m[e.Stage] = e
	}
	return m
}

func TestLogsEventLifecycle(t *testing.T) {
	requireStack(t)

	cust := newTestCustomer(t)
	eventID := uniqueEventID("logs")
	reqID := "it-req-" + runTag
	postWithRequestID(t, reqID, []any{validEvent(eventID, cust)})

	var page logsPage
	pollUntil(t, 20*time.Second, "ingested+processed log rows", func() bool {
		_, page = getLogs(t, url.Values{"event_id": {eventID}})
		st := stagesOf(page.Data)
		_, ing := st["ingested"]
		_, proc := st["processed"]
		return ing && proc
	})
	st := stagesOf(page.Data)
	for _, stage := range []string{"ingested", "processed"} {
		e := st[stage]
		require.NotNil(t, e.RequestID, stage)
		assert.Equal(t, reqID, *e.RequestID, "%s row carries the ingest request id", stage)
		require.NotNil(t, e.CustomerID, stage)
		assert.Equal(t, cust, *e.CustomerID, "external customer id, never internal")
	}
	require.NotNil(t, st["processed"].Worker, "processed row names the worker")
	assert.Contains(t, st["processed"].Details, "took_ms")
	assert.Greater(t, page.Data[0].ID, page.Data[len(page.Data)-1].ID, "newest first")

	// Re-post → duplicate row, same event_id.
	postWithRequestID(t, reqID+"-retry", []any{validEvent(eventID, cust)})
	_, page = getLogs(t, url.Values{"event_id": {eventID}, "stage": {"duplicate"}})
	require.Len(t, page.Data, 1)
	assert.Equal(t, reqID+"-retry", *page.Data[0].RequestID)

	// Request-id filter finds the rows for that call.
	_, page = getLogs(t, url.Values{"request_id": {reqID}})
	assert.NotEmpty(t, page.Data)
	for _, e := range page.Data {
		assert.Equal(t, reqID, *e.RequestID)
	}

	// Customer filter by external id.
	_, page = getLogs(t, url.Values{"customer_id": {cust}})
	assert.GreaterOrEqual(t, len(page.Data), 3)
}

func TestLogsFiltersAndPaging(t *testing.T) {
	requireStack(t)

	status, _ := getLogs(t, url.Values{"stage": {"exploded"}})
	assert.Equal(t, http.StatusBadRequest, status)
	status, _ = getLogs(t, url.Values{"level": {"loud"}})
	assert.Equal(t, http.StatusBadRequest, status)
	status, _ = getLogs(t, url.Values{"since": {"yesterday"}})
	assert.Equal(t, http.StatusBadRequest, status)
	status, _ = getLogs(t, url.Values{"cursor": {"-4"}})
	assert.Equal(t, http.StatusBadRequest, status)

	status, page := getLogs(t, url.Values{"customer_id": {"cust_does_not_exist_" + runTag}})
	assert.Equal(t, http.StatusOK, status)
	assert.Empty(t, page.Data, "unknown customer → empty page, not an error")

	_, page = getLogs(t, url.Values{"level": {"warn"}, "limit": {"50"}})
	for _, e := range page.Data {
		assert.Contains(t, []string{"warn", "error"}, e.Level, "level is a minimum")
	}

	// Cursor paging walks strictly older rows without overlap.
	_, first := getLogs(t, url.Values{"limit": {"2"}})
	if first.HasMore {
		_, second := getLogs(t, url.Values{"limit": {"2"}, "cursor": {first.NextCursor}})
		require.NotEmpty(t, second.Data)
		assert.Less(t, second.Data[0].ID, first.Data[len(first.Data)-1].ID)
	}
}
