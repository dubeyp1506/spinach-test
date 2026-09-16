// Verifies CONTRACTS.md §2 per-customer serialization: concurrent events for
// the SAME customer are serialized by SELECT ... FOR UPDATE on the
// engagement_profiles row inside the atomic processing tx — no lost updates.
// Must be clean under `go test -race`.
package tests

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Thirty goroutines each post a distinct event for one customer. After the
// worker drains them, total_events must have increased by exactly 30.
func TestConcurrentEventsSameCustomerNoLostUpdates(t *testing.T) {
	requireStack(t)
	cust := newTestCustomer(t) // private customer → exactly-30 assertion is safe

	const n = 30
	eventIDs := make([]string, n)
	resps := make([]ingestResponse, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		eventIDs[i] = uniqueEventID("conc")
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ev := validEvent(eventIDs[i], cust)
			status, raw := doJSON(t, http.MethodPost, "/events", map[string]any{"events": []any{ev}})
			if status != http.StatusAccepted {
				t.Errorf("POST /events status=%d body=%s", status, raw)
				return
			}
			if err := json.Unmarshal(raw, &resps[i]); err != nil {
				t.Errorf("decode: %v", err)
			}
		}(i)
	}
	wg.Wait()

	for i, r := range resps {
		require.Equal(t, 1, r.Accepted, "event %d was not accepted: %+v", i, r)
		require.Zero(t, r.Duplicates)
		require.Empty(t, r.Rejected)
	}

	// Wait for the worker to drain all 30 messages (each commits atomically).
	pollUntil(t, 60*time.Second, "total_events to reach 30", func() bool {
		return profileTotalEvents(t, cust) >= n
	})

	// Exactness is the point: FOR UPDATE serialization means no lost update.
	assert.EqualValues(t, n, profileTotalEvents(t, cust),
		"total_events must be exactly 30 — a lost update means broken row locking")

	processed := queryInt(t,
		`SELECT count(*) FROM events WHERE event_id = ANY($1) AND status='processed'`,
		eventIDs)
	assert.EqualValues(t, n, processed, "all submitted events must end processed")
}
