package seedgen

import (
	"math"
	"sort"
	"time"
)

// Engagement weights from CONTRACTS §7. 'sent' is neutral (0) — it is a
// campaign action, not a customer signal.
var eventWeights = map[string]float64{
	"opened":       1,
	"clicked":      2,
	"converted":    5,
	"delivered":    0.2,
	"bounced":      -1,
	"unsubscribed": -3,
	"complained":   -5,
}

// Weight returns the §7 weight for an event type (0 for 'sent'/unknown).
func Weight(eventType string) float64 { return eventWeights[eventType] }

// Lambda is the §7 decay rate: half-life 14 days => λ = ln2 / 14d, in s⁻¹.
const halfLifeSeconds = 14 * 24 * 3600

var lambda = math.Ln2 / halfLifeSeconds

// Lambda returns the §7 decay constant (s⁻¹).
func Lambda() float64 { return lambda }

var positiveTypes = map[string]bool{"opened": true, "clicked": true, "converted": true}
var negativeTypes = map[string]bool{"bounced": true, "unsubscribed": true, "complained": true}

// Profile mirrors an engagement_profiles row (CONTRACTS §7 semantics).
// EngagementScore is the RAW decayed score — never normalized (§7:
// normalization happens only at read/display time).
type Profile struct {
	CustomerIdx      int
	TotalEvents      int
	ChannelCounts    map[string]map[string]int // {"email":{"opened":3},...}
	PositiveEvents   int
	NegativeEvents   int
	Conversions      int
	LastEventAt      time.Time
	LastEventType    string
	EngagementScore  float64
	ScoreUpdatedAt   time.Time // the §7 anchor
	ActivityTrend    string    // rising|stable|declining
	PreferredChannel string    // "" -> SQL NULL (no positive events)
}

// buildProfiles folds each customer's processed events into a Profile.
// Failed events are excluded — they were never processed (CONTRACTS §2).
func buildProfiles(ds *Dataset, now time.Time) []Profile {
	byCust := make(map[int][]Event)
	for i := range ds.Events {
		e := ds.Events[i]
		if e.Status == "processed" {
			byCust[e.CustomerIdx] = append(byCust[e.CustomerIdx], e)
		}
	}
	// Deterministic output order (map iteration is random): by customer idx.
	idxs := make([]int, 0, len(byCust))
	for custIdx := range byCust {
		idxs = append(idxs, custIdx)
	}
	sort.Ints(idxs)
	profiles := make([]Profile, 0, len(byCust))
	for _, custIdx := range idxs {
		profiles = append(profiles, FoldEvents(custIdx, byCust[custIdx], now))
	}
	return profiles
}

// FoldEvents collapses one customer's events into a Profile using the
// out-of-order-safe score math of CONTRACTS §7:
//
//	score(now) = Σ w_i · exp(-λ·(now − t_i)), λ = ln2 / 14d
//	t >= anchor: raw' = raw · exp(-λ·(t − anchor)) + w; anchor = t
//	t <  anchor: raw' = raw + w · exp(-λ·(anchor − t)); anchor unchanged
//
// Events may arrive in any insertion order; they are folded in occurred_at
// order. Negative running totals are clamped to 0 (§7). The stored score is
// RAW — normalization is a read-time concern.
func FoldEvents(customerIdx int, evts []Event, now time.Time) Profile {
	sorted := sortedEventsCopy(evts)
	p := Profile{CustomerIdx: customerIdx, ChannelCounts: map[string]map[string]int{}}

	var raw float64
	var anchor time.Time
	recent, prior := 0, 0 // 7d vs prior-7d windows for activity_trend
	week := 7 * 24 * time.Hour

	posByChannel := map[string]int{}
	for _, e := range sorted {
		// counters / channel histogram (all folded events are 'processed')
		p.TotalEvents++
		cc := p.ChannelCounts[e.Channel]
		if cc == nil {
			cc = map[string]int{}
			p.ChannelCounts[e.Channel] = cc
		}
		cc[e.Type]++
		if positiveTypes[e.Type] {
			p.PositiveEvents++
			posByChannel[e.Channel]++
		}
		if negativeTypes[e.Type] {
			p.NegativeEvents++
		}
		if e.Type == "converted" {
			p.Conversions++
		}
		if now.Sub(e.OccurredAt) <= week {
			recent++
		} else if now.Sub(e.OccurredAt) <= 2*week {
			prior++
		}

		// §7 decayed fold
		w := eventWeights[e.Type]
		switch {
		case anchor.IsZero():
			raw = w
			anchor = e.OccurredAt
		case !e.OccurredAt.Before(anchor):
			raw = raw*math.Exp(-lambda*e.OccurredAt.Sub(anchor).Seconds()) + w
			anchor = e.OccurredAt
		default: // out-of-order event: contributes decayed, anchor unchanged
			raw += w * math.Exp(-lambda*anchor.Sub(e.OccurredAt).Seconds())
		}
		if raw < 0 {
			raw = 0 // §7: clamp negative totals at 0
		}
	}

	if n := len(sorted); n > 0 {
		p.LastEventAt = sorted[n-1].OccurredAt
		p.LastEventType = sorted[n-1].Type
	}
	p.EngagementScore = raw
	p.ScoreUpdatedAt = anchor

	// preferred_channel = argmax positive events per channel; deterministic
	// tie-break via fixed Channels order.
	best, bestN := "", 0
	for _, ch := range Channels {
		if posByChannel[ch] > bestN {
			best, bestN = ch, posByChannel[ch]
		}
	}
	p.PreferredChannel = best // "" when the customer has no positive events

	switch {
	case recent > prior:
		p.ActivityTrend = "rising"
	case recent < prior:
		p.ActivityTrend = "declining"
	default:
		p.ActivityTrend = "stable"
	}
	return p
}
