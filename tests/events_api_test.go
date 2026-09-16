// Verifies CONTRACTS.md §2 (POST /events ingestion contract): per-item
// validation rejections carry index+reason while valid items are accepted,
// malformed bodies and >500 batches are 400, unknown customers reject, and
// PostgreSQL event_id UNIQUE is the dedup authority — including under
// concurrent identical submissions.
package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mixed batch: every invalid item is rejected per-item with its index and a
// reason; valid items in the same batch are still accepted (§2 rules).
func TestIngestMixedBatchValidation(t *testing.T) {
	requireStack(t)
	cust := pickCustomer(t)

	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)

	events := []any{
		validEvent(uniqueEventID("mix-ok"), cust), // 0: valid
		map[string]any{ // 1: bad channel
			"event_id": uniqueEventID("mix-badch"), "customer_id": cust,
			"channel": "carrier-pigeon", "type": "opened", "occurred_at": past,
		},
		map[string]any{ // 2: missing event_id
			"customer_id": cust, "channel": "email", "type": "opened",
			"occurred_at": past,
		},
		map[string]any{ // 3: missing occurred_at
			"event_id": uniqueEventID("mix-noocc"), "customer_id": cust,
			"channel": "email", "type": "opened",
		},
		map[string]any{ // 4: occurred_at in the future
			"event_id": uniqueEventID("mix-future"), "customer_id": cust,
			"channel": "email", "type": "opened", "occurred_at": future,
		},
		map[string]any{ // 5: bad type
			"event_id": uniqueEventID("mix-badtype"), "customer_id": cust,
			"channel": "email", "type": "teleported", "occurred_at": past,
		},
		42, // 6: not an object → malformed event object
		map[string]any{ // 7: unknown customer (passes shape validation, fails store)
			"event_id": uniqueEventID("mix-nocust"), "customer_id": "cust_it_no_such_" + runTag,
			"channel": "email", "type": "opened", "occurred_at": past,
		},
	}

	resp := postEventBatch(t, events)
	assert.Equal(t, 1, resp.Accepted, "exactly one valid item")
	assert.Equal(t, 0, resp.Duplicates)
	require.Len(t, resp.Rejected, 7, "items 1..7 must each be rejected")

	reasons := map[int]string{}
	for _, r := range resp.Rejected {
		reasons[r.Index] = r.Reason
	}
	assert.Contains(t, reasons[1], "channel")
	assert.Contains(t, reasons[2], "event_id")
	assert.Contains(t, reasons[3], "occurred_at")
	assert.Contains(t, reasons[4], "future")
	assert.Contains(t, reasons[5], "type")
	assert.Contains(t, reasons[6], "malformed")
	assert.Contains(t, reasons[7], "unknown customer_id")
}

// Whole-request malformed bodies are a 400, not a per-item rejection.
func TestIngestMalformedRequestBody(t *testing.T) {
	requireStack(t)

	for name, body := range map[string]string{
		"truncated JSON": `{"events": [`,
		"not JSON":       `this is not json`,
		"missing events": `{}`,
		"null events":    `{"events": null}`,
	} {
		t.Run(name, func(t *testing.T) {
			status, raw := doJSON(t, http.MethodPost, "/events", body)
			assert.Equal(t, http.StatusBadRequest, status, "body=%s", raw)
		})
	}
}

// Batches over the 500-item cap are rejected wholesale with 400 (§2,
// openapi EventBatchRequest maxItems).
func TestIngestBatchOver500(t *testing.T) {
	requireStack(t)
	events := make([]any, 501)
	for i := range events {
		events[i] = map[string]any{"event_id": fmt.Sprintf("x%d", i)}
	}
	status, raw := doJSON(t, http.MethodPost, "/events", map[string]any{"events": events})
	assert.Equal(t, http.StatusBadRequest, status, "body=%s", raw)
}

// Re-posting an accepted event_id is counted in `duplicates`, not an error
// (§2 idempotent ingestion).
func TestIngestDuplicateEventID(t *testing.T) {
	requireStack(t)
	cust := pickCustomer(t)
	eid := uniqueEventID("dupe")
	ev := validEvent(eid, cust)

	first := postEventBatch(t, []any{ev})
	require.Equal(t, 1, first.Accepted)

	second := postEventBatch(t, []any{ev})
	assert.Equal(t, 0, second.Accepted)
	assert.Equal(t, 1, second.Duplicates)
	assert.Empty(t, second.Rejected)
}

// §2: Postgres UNIQUE is the ONLY dedup authority. Twenty simultaneous
// submissions of the SAME event_id must yield exactly one accept, nineteen
// duplicates, and exactly one events row — no unique-violation errors leak.
func TestIngestConcurrentIdempotency(t *testing.T) {
	requireStack(t)
	cust := pickCustomer(t)
	eid := uniqueEventID("race-dupe")
	ev := validEvent(eid, cust)

	const n = 20
	resps := make([]ingestResponse, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, raw := doJSON(t, http.MethodPost, "/events", map[string]any{"events": []any{ev}})
			if status != http.StatusAccepted {
				t.Errorf("POST /events status=%d body=%s", status, raw)
				return
			}
			var out ingestResponse
			if err := json.Unmarshal(raw, &out); err != nil {
				t.Errorf("decode: %v", err)
				return
			}
			resps[i] = out
		}(i)
	}
	wg.Wait()

	accepted, duplicates, rejected := 0, 0, 0
	for _, r := range resps {
		accepted += r.Accepted
		duplicates += r.Duplicates
		rejected += len(r.Rejected)
	}
	assert.Equal(t, 1, accepted, "exactly one concurrent submit wins")
	assert.Equal(t, n-1, duplicates, "the rest classify as duplicates")
	assert.Equal(t, 0, rejected)
	assert.EqualValues(t, 1, queryInt(t,
		`SELECT count(*) FROM events WHERE event_id = $1`, eid),
		"exactly one events row for the raced event_id")
}
