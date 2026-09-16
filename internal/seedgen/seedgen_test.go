package seedgen

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testNow = time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

func testConfig() Config {
	return Config{Customers: 4000, Events: 5000, Campaigns: 20, Seed: 42, Now: testNow}
}

// countByType tallies events by type, optionally restricted to
// campaign-funnel events (CampaignIdx >= 0) with status processed.
func countByType(ds *Dataset, campaignOnly bool) map[string]int {
	m := map[string]int{}
	for _, e := range ds.Events {
		if e.Status != "processed" {
			continue
		}
		if campaignOnly && e.CampaignIdx < 0 {
			continue
		}
		m[e.Type]++
	}
	return m
}

func eventsPerCustomer(ds *Dataset) map[int]int {
	m := map[int]int{}
	for _, e := range ds.Events {
		m[e.CustomerIdx]++
	}
	return m
}

// --- funnel ratios -----------------------------------------------------------

func TestFunnelRatios(t *testing.T) {
	ds := Generate(testConfig())
	c := countByType(ds, true)

	cases := []struct {
		name string
		got  float64
		lo   float64
		hi   float64
	}{
		{"delivered/sent", float64(c["delivered"]) / float64(c["sent"]), 0.88, 0.99},
		{"opened/delivered", float64(c["opened"]) / float64(c["delivered"]), 0.30, 0.40},
		{"clicked/opened", float64(c["clicked"]) / float64(c["opened"]), 0.10, 0.20},
		{"converted/clicked", float64(c["converted"]) / float64(c["clicked"]), 0.04, 0.18},
		{"bounced/sent", float64(c["bounced"]) / float64(c["sent"]), 0.01, 0.05},
		{"unsubscribed/sent", float64(c["unsubscribed"]) / float64(c["sent"]), 0.0, 0.01},
		{"complained/sent", float64(c["complained"]) / float64(c["sent"]), 0.0, 0.01},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.GreaterOrEqual(t, tc.got, tc.lo)
			assert.LessOrEqual(t, tc.got, tc.hi)
		})
	}
}

// --- structural invariants ----------------------------------------------------

func TestCountsAndUniqueness(t *testing.T) {
	ds := Generate(testConfig())

	assert.Len(t, ds.Customers, 4000)
	assert.Len(t, ds.Campaigns, 20)
	assert.GreaterOrEqual(t, len(ds.Events), testConfig().Events,
		"HF floor should exceed the small -events target")

	seen := make(map[string]struct{}, len(ds.Events))
	for _, e := range ds.Events {
		require.NotEmpty(t, e.EventID)
		_, dup := seen[e.EventID]
		assert.False(t, dup, "duplicate event_id %s", e.EventID)
		seen[e.EventID] = struct{}{}
		assert.Contains(t, Channels, e.Channel)
		assert.Contains(t, EventTypes, e.Type)
		assert.False(t, e.OccurredAt.After(testNow), "occurred_at in future")
		assert.False(t, e.OccurredAt.Before(testNow.Add(-96*24*time.Hour)))
		assert.False(t, e.ReceivedAt.IsZero())
		if e.Status == "processed" {
			require.NotNil(t, e.ProcessedAt)
			assert.Nil(t, e.LastError)
		} else {
			assert.Equal(t, "failed", e.Status)
			assert.Nil(t, e.ProcessedAt)
			require.NotNil(t, e.LastError)
			assert.Equal(t, 5, e.Attempts)
		}
	}
}

func TestOutOfOrderDisplacement(t *testing.T) {
	ds := Generate(testConfig())

	// ~1% of events are displaced within their group; accept a broad
	// statistical band around p=0.01.
	frac := float64(ds.OutOfOrderCount) / float64(len(ds.Events))
	assert.GreaterOrEqual(t, frac, 0.002)
	assert.LessOrEqual(t, frac, 0.03)

	// There must exist at least one customer whose INSERTION order inverts
	// occurred_at order — the property that exercises OOO handling.
	lastSeen := map[int]time.Time{}
	inversions := 0
	for _, e := range ds.Events {
		if last, ok := lastSeen[e.CustomerIdx]; ok && e.OccurredAt.Before(last) {
			inversions++
		}
		if e.OccurredAt.After(lastSeen[e.CustomerIdx]) {
			lastSeen[e.CustomerIdx] = e.OccurredAt
		}
	}
	assert.Greater(t, inversions, 0)
}

func TestFailedAndDLQCounts(t *testing.T) {
	ds := Generate(testConfig())

	failedFrac := float64(ds.FailedCount) / float64(len(ds.Events))
	assert.GreaterOrEqual(t, failedFrac, 0.001)
	assert.LessOrEqual(t, failedFrac, 0.01)

	// ~0.5% of event volume lands in the DLQ (min 50).
	dlqFrac := float64(len(ds.DLQ)) / float64(len(ds.Events))
	assert.GreaterOrEqual(t, dlqFrac, 0.003)
	assert.LessOrEqual(t, dlqFrac, 0.008)
	for _, d := range ds.DLQ {
		assert.NotEmpty(t, d.Error)
		assert.Equal(t, 5, d.Attempts)
		assert.NotEmpty(t, d.Payload)
	}
}

func TestDupeBatch(t *testing.T) {
	ds := Generate(testConfig())

	require.GreaterOrEqual(t, len(ds.Dupes.Events), 2000)

	seeded := make(map[string]bool, len(ds.Events))
	for _, e := range ds.Events {
		seeded[e.EventID] = true
	}
	dupeIDs := map[string]bool{}
	for _, d := range ds.Dupes.Events {
		assert.True(t, seeded[d.EventID], "dupe re-emits a seeded event_id")
		assert.Regexp(t, `^cust_\d{5}$`, d.CustomerID)
		if d.CampaignID != "" {
			assert.Regexp(t, `^camp_\d{3}$`, d.CampaignID)
		}
		_, err := time.Parse(time.RFC3339, d.OccurredAt)
		assert.NoError(t, err)
		assert.False(t, dupeIDs[d.EventID], "dupe batch itself should not repeat ids")
		dupeIDs[d.EventID] = true
	}
}

func TestSendsMatchProcessedSentEvents(t *testing.T) {
	ds := Generate(testConfig())
	want := 0
	for _, e := range ds.Events {
		if e.Type == "sent" && e.Status == "processed" && e.CampaignIdx >= 0 {
			want++
		}
	}
	assert.Equal(t, want, len(ds.Sends))
	for _, s := range ds.Sends {
		assert.GreaterOrEqual(t, s.CampaignIdx, 0)
		assert.GreaterOrEqual(t, s.CustomerIdx, 0)
		assert.False(t, s.SentAt.IsZero())
	}
}

// --- customer cohorts ---------------------------------------------------------

func TestCustomerCohorts(t *testing.T) {
	ds := Generate(testConfig())
	perCust := eventsPerCustomer(ds)

	// min/max occurred_at per customer, computed once.
	maxAt := map[int]time.Time{}
	for _, e := range ds.Events {
		if e.OccurredAt.After(maxAt[e.CustomerIdx]) {
			maxAt[e.CustomerIdx] = e.OccurredAt
		}
	}

	inactive, hf, regular := 0, 0, 0
	for i, c := range ds.Customers {
		switch {
		case !c.IsActive:
			inactive++
			// inactive: no events, or only old ones (>70d ago).
			if last, ok := maxAt[i]; ok {
				assert.False(t, last.After(testNow.Add(-70*24*time.Hour)),
					"inactive customer has a recent event")
			}
		case c.HighFreq:
			hf++
			n := perCust[i]
			assert.GreaterOrEqual(t, n, 50, "HF customer below 50 events")
			assert.LessOrEqual(t, n, 500, "HF customer above 500 events")
		default:
			regular++
		}
	}
	assert.InDelta(t, 0.10, float64(inactive)/float64(len(ds.Customers)), 0.01)
	assert.InDelta(t, 0.05, float64(hf)/float64(len(ds.Customers)), 0.01)
	assert.Equal(t, len(ds.Customers), inactive+hf+regular)

	// regular customers: power-law-ish small counts (mean well under HF's).
	sum, maxN := 0, 0
	for i, c := range ds.Customers {
		if c.IsActive && !c.HighFreq {
			sum += perCust[i]
			if perCust[i] > maxN {
				maxN = perCust[i]
			}
		}
	}
	assert.Less(t, float64(sum)/float64(regular), 4.0)
	assert.LessOrEqual(t, maxN, 21)
}

func TestInactiveCustomersHaveNoRecentEvents(t *testing.T) {
	ds := Generate(testConfig())
	maxAt := map[int]time.Time{}
	for _, e := range ds.Events {
		if e.OccurredAt.After(maxAt[e.CustomerIdx]) {
			maxAt[e.CustomerIdx] = e.OccurredAt
		}
	}
	for i, c := range ds.Customers {
		if c.IsActive {
			continue
		}
		if last, ok := maxAt[i]; ok {
			require.True(t, last.Before(testNow.Add(-70*24*time.Hour)),
				"inactive customer %s has a recent event", c.ExternalID)
		}
	}
}

// --- determinism ---------------------------------------------------------------

func TestDeterministicSeed(t *testing.T) {
	a := Generate(testConfig())
	b := Generate(testConfig())
	require.Equal(t, len(a.Events), len(b.Events))
	require.Equal(t, len(a.Sends), len(b.Sends))
	assert.Equal(t, a.OutOfOrderCount, b.OutOfOrderCount)
	assert.Equal(t, a.FailedCount, b.FailedCount)
	for i := range a.Events {
		require.Equal(t, a.Events[i].EventID, b.Events[i].EventID)
		require.True(t, a.Events[i].OccurredAt.Equal(b.Events[i].OccurredAt))
	}
}

// --- §7 scoring fold -----------------------------------------------------------

func ev(typ string, at time.Time) Event {
	return Event{Type: typ, Channel: "email", OccurredAt: at, Status: "processed", CampaignIdx: 0, Payload: map[string]any{}}
}

// Hand-computed CONTRACTS §7 fold. The fold sorts by occurred_at first, so
// λ = ln2/14d gives exp(-λ·7d) = 2^-0.5 ≈ 0.707107 and exp(-λ·3.5d) ≈ 0.840896.
func TestFoldEventsScoreMath(t *testing.T) {
	t0 := testNow.Add(-30 * 24 * time.Hour)
	d := 24 * time.Hour

	all := []Event{
		ev("opened", t0),                 // w=1 -> raw 1, anchor t0
		ev("clicked", t0.Add(7*d)),       // w=2, folded after 'converted' (sorted)
		ev("converted", t0.Add(35*d/10)), // w=5, occurred t0+3.5d
		ev("complained", t0.Add(14*d)),   // w=-5
	}
	// Sorted fold: opened@0 -> raw 1, anchor t0.
	// converted@3.5d: raw = 1·0.840896 + 5 = 5.840896, anchor t0+3.5d.
	// clicked@7d:     raw = 5.840896·0.840896 + 2 = 6.911699, anchor t0+7d.
	// complained@14d: raw = 6.911699·0.707107 - 5 = -0.1123 -> clamp 0, anchor 14d.
	scrambled := []Event{all[2], all[0], all[3], all[1]} // insertion order must not matter

	cases := []struct {
		name     string
		evts     []Event
		wantRaw  float64
		wantAncr time.Time
	}{
		{"single event", scrambled[:1], 5, t0.Add(35 * d / 10)},
		{"in-order pair", []Event{all[0], all[1]}, 2.707107, t0.Add(7 * d)},
		{"sorted triplet", []Event{all[0], all[1], all[2]}, 6.9117, t0.Add(7 * d)},
		{"clamp negative", scrambled, 0, t0.Add(14 * d)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := FoldEvents(0, tc.evts, testNow)
			assert.InDelta(t, tc.wantRaw, p.EngagementScore, 1e-3)
			assert.True(t, tc.wantAncr.Equal(p.ScoreUpdatedAt), "anchor mismatch")
		})
	}
}

func TestFoldEventsCounters(t *testing.T) {
	t0 := testNow.Add(-2 * 24 * time.Hour)
	evts := []Event{
		ev("sent", t0),
		ev("delivered", t0.Add(time.Hour)),
		{Type: "opened", Channel: "sms", OccurredAt: t0.Add(2 * time.Hour), Status: "processed", CampaignIdx: 0},
		{Type: "clicked", Channel: "sms", OccurredAt: t0.Add(3 * time.Hour), Status: "processed", CampaignIdx: 0},
		ev("converted", t0.Add(4*time.Hour)),
		ev("bounced", t0.Add(5*time.Hour)),
	}
	p := FoldEvents(7, evts, testNow)

	assert.Equal(t, 6, p.TotalEvents)
	assert.Equal(t, 3, p.PositiveEvents) // opened+clicked+converted
	assert.Equal(t, 1, p.NegativeEvents) // bounced
	assert.Equal(t, 1, p.Conversions)
	assert.Equal(t, 1, p.ChannelCounts["email"]["sent"])
	assert.Equal(t, 1, p.ChannelCounts["email"]["delivered"])
	assert.Equal(t, 1, p.ChannelCounts["email"]["converted"])
	assert.Equal(t, 1, p.ChannelCounts["sms"]["opened"])
	assert.Equal(t, 1, p.ChannelCounts["sms"]["clicked"])
	assert.Equal(t, "bounced", p.LastEventType)
	assert.True(t, evts[len(evts)-1].OccurredAt.Equal(p.LastEventAt))
	assert.Equal(t, "sms", p.PreferredChannel) // sms has 2 positives vs email 1
	assert.Equal(t, "rising", p.ActivityTrend) // all events in last 7d
	assert.True(t, p.EngagementScore > 0)
}

func TestActivityTrend(t *testing.T) {
	cases := []struct {
		name string
		evts []Event
		want string
	}{
		{"rising", []Event{
			ev("opened", testNow.Add(-24*time.Hour)),
			ev("opened", testNow.Add(-48*time.Hour)),
		}, "rising"},
		{"declining", []Event{
			ev("opened", testNow.Add(-10*24*time.Hour)),
			ev("clicked", testNow.Add(-12*24*time.Hour)),
		}, "declining"},
		{"stable", []Event{
			ev("opened", testNow.Add(-24*time.Hour)),
			ev("opened", testNow.Add(-10*24*time.Hour)),
		}, "stable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := FoldEvents(0, tc.evts, testNow)
			assert.Equal(t, tc.want, p.ActivityTrend)
		})
	}
}

func TestProfilesConsistency(t *testing.T) {
	ds := Generate(testConfig())

	// every profile's counters must agree with the processed events folded.
	byCust := map[int][]Event{}
	for _, e := range ds.Events {
		if e.Status == "processed" {
			byCust[e.CustomerIdx] = append(byCust[e.CustomerIdx], e)
		}
	}
	require.Len(t, ds.Profiles, len(byCust))
	for _, p := range ds.Profiles {
		assert.Equal(t, len(byCust[p.CustomerIdx]), p.TotalEvents)
		assert.False(t, p.ScoreUpdatedAt.IsZero())
		assert.Contains(t, []string{"rising", "stable", "declining"}, p.ActivityTrend)
		if p.PositiveEvents == 0 {
			assert.Empty(t, p.PreferredChannel)
		} else {
			assert.Contains(t, Channels, p.PreferredChannel)
		}
		// raw score can be 0 (clamped) or positive, never negative.
		assert.GreaterOrEqual(t, p.EngagementScore, 0.0)
	}
}

func TestLambdaMatchesSpec(t *testing.T) {
	// §7: half-life ~14d => an event's weight halves after 14 days.
	assert.InDelta(t, 0.5, math.Exp(-Lambda()*14*24*3600), 1e-9)
}
