// Verifies CONTRACTS.md §2 processing semantics end-to-end: a POSTed event
// flows ingest → outbox/stream → embedded worker → atomic profile mutation
// and lands as events.status='processed'; channel/type counters merge; and
// out-of-order events are counted without regressing last_event_at (max
// semantics / GREATEST).
package tests

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A valid event for a real customer increments engagement.total_events,
// merges into channel_counts[channel][type], and is marked processed in
// Postgres (§2 atomic processing).
func TestEventProcessedEndToEnd(t *testing.T) {
	requireStack(t)
	cust := newTestCustomer(t) // private customer → exact deltas, no collisions

	eid := uniqueEventID("proc")
	ev := validEvent(eid, cust)
	ev["channel"] = "web"
	ev["type"] = "clicked"

	resp := postEventBatch(t, []any{ev})
	require.Equal(t, 1, resp.Accepted)

	// Poll the read API until the worker's tx commits the profile update.
	var prof customerProfile
	pollUntil(t, 15*time.Second, "engagement.total_events to reach 1", func() bool {
		prof = getCustomer(t, cust)
		return prof.Engagement.TotalEvents >= 1
	})
	assert.Equal(t, 1, prof.Engagement.ChannelCounts["web"]["clicked"],
		"channel_counts must count the event under web/clicked")
	assert.Equal(t, 1, prof.Engagement.PositiveEvents)

	// Durable truth: the events row is processed.
	pollUntil(t, 15*time.Second, "events.status='processed'", func() bool {
		return eventStatus(t, eid) == "processed"
	})
	processedAt := queryInt(t,
		`SELECT count(*) FROM events WHERE event_id=$1 AND processed_at IS NOT NULL`, eid)
	assert.EqualValues(t, 1, processedAt)
}

// §2 out-of-order rule: an event whose occurred_at predates the profile's
// last_event_at is still counted (commutative counters) but last_event_at
// must not regress.
func TestOutOfOrderEventDoesNotRegressLastEventAt(t *testing.T) {
	requireStack(t)
	cust := newTestCustomer(t)

	// Establish a baseline: one event at T-1h, wait for it to process.
	newer := time.Now().Add(-time.Hour).UTC()
	e1 := validEvent(uniqueEventID("ooo-new"), cust)
	e1["occurred_at"] = newer.Format(time.RFC3339)
	require.Equal(t, 1, postEventBatch(t, []any{e1}).Accepted)

	var prof customerProfile
	pollUntil(t, 15*time.Second, "baseline event processed", func() bool {
		prof = getCustomer(t, cust)
		return prof.Engagement.TotalEvents >= 1
	})
	require.NotNil(t, prof.Engagement.LastEventAt)
	baselineLast := *prof.Engagement.LastEventAt

	// Now send an older event (occurred_at BEFORE last_event_at).
	e2 := validEvent(uniqueEventID("ooo-old"), cust)
	e2["channel"] = "sms"
	e2["type"] = "opened"
	e2["occurred_at"] = newer.Add(-2 * time.Hour).Format(time.RFC3339)
	require.Equal(t, 1, postEventBatch(t, []any{e2}).Accepted)

	pollUntil(t, 15*time.Second, "out-of-order event counted", func() bool {
		prof = getCustomer(t, cust)
		return prof.Engagement.TotalEvents >= 2
	})
	assert.Equal(t, 1, prof.Engagement.ChannelCounts["sms"]["opened"])
	require.NotNil(t, prof.Engagement.LastEventAt)
	assert.False(t, prof.Engagement.LastEventAt.Before(baselineLast),
		"last_event_at regressed: was %s, now %s", baselineLast, prof.Engagement.LastEventAt)
	assert.WithinDuration(t, baselineLast, *prof.Engagement.LastEventAt, time.Second)
}

// Sanity: an unknown customer id on GET returns the §4 404 error envelope.
func TestGetUnknownCustomer404(t *testing.T) {
	requireStack(t)
	status, raw := doJSON(t, http.MethodGet, "/customers/cust_it_no_such_"+runTag, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, string(raw), "not_found")
}
