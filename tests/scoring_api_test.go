// Verifies CONTRACTS.md §4 GET /customers/{id} response shape and §7 scoring
// semantics: the profile carries the normalized score plus raw_score, trend,
// preferred_channel and channel_counts; a fresh positive event must not
// decrease the score (additive decayed weights, §7).
package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var validTrends = map[string]bool{"rising": true, "stable": true, "declining": true}
var validChannels = map[string]bool{
	"email": true, "sms": true, "whatsapp": true, "push": true, "web": true,
}

// Shape contract for the §4 customer profile on a high-activity seeded
// customer: score normalized to [0,1], raw_score unclamped, trend in the
// enum, preferred_channel valid-or-null, channel_counts populated.
func TestCustomerProfileShape(t *testing.T) {
	requireStack(t)
	cust := pickHighActivityCustomer(t)

	prof := getCustomer(t, cust)
	assert.Equal(t, cust, prof.CustomerID)
	assert.True(t, prof.IsActive)

	e := prof.Engagement
	assert.Greater(t, e.TotalEvents, 0)
	assert.GreaterOrEqual(t, e.Score, 0.0)
	assert.Less(t, e.Score, 1.0, "normalized score must stay in [0,1)")
	assert.True(t, validTrends[e.Trend], "trend %q not in enum", e.Trend)
	if e.PreferredChannel != nil {
		assert.True(t, validChannels[*e.PreferredChannel],
			"preferred_channel %q not a channel enum", *e.PreferredChannel)
	}
	require.NotEmpty(t, e.ChannelCounts, "high-activity customer must have channel_counts")
	sum := 0
	for _, types := range e.ChannelCounts {
		for _, n := range types {
			sum += n
		}
	}
	assert.Equal(t, e.TotalEvents, sum,
		"channel_counts must account for every counted event")
}

// §7: weights are additive, so a fresh positive event (converted, w=+5)
// cannot lower the score — it dominates sub-second decay between reads.
func TestScoreNonDecreasingAfterPositiveEvent(t *testing.T) {
	requireStack(t)
	cust := pickHighActivityCustomer(t)

	before := getCustomer(t, cust)

	ev := validEvent(uniqueEventID("score"), cust)
	ev["type"] = "converted"
	ev["occurred_at"] = time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)
	require.Equal(t, 1, postEventBatch(t, []any{ev}).Accepted)

	var after customerProfile
	pollUntil(t, 15*time.Second, "event folded into profile", func() bool {
		after = getCustomer(t, cust)
		return after.Engagement.TotalEvents > before.Engagement.TotalEvents
	})

	assert.GreaterOrEqual(t, after.Engagement.Score, before.Engagement.Score,
		"score must be non-decreasing after a +5 converted event (was %v, now %v)",
		before.Engagement.Score, after.Engagement.Score)
	assert.Greater(t, after.Engagement.RawScore, before.Engagement.RawScore-1e-9,
		"raw decayed score must grow after a positive event")
	// >= +1 (not == +1): another test could theoretically land a converted
	// event on this shared seeded customer in the same window.
	assert.GreaterOrEqual(t, after.Engagement.Conversions, before.Engagement.Conversions+1)
}
