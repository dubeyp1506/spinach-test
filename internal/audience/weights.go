package audience

import (
	"math"
	"time"
)

// Objective weighting — CONTRACTS §5 step 6.
//
// The SQL pre-filter already delivers rows ordered by raw engagement_score.
// Here each candidate gets a small additive, log-scaled boost so a whale with
// 10 000 events can't drown the base signal:
//
//	weighted = raw
//	         + wConv · log1p(conversions)
//	         + wPos  · log1p(positive_events)
//	         + wDorm · log1p(days_since_last_event)
//
// conversion    → boost conversions
// engagement    → boost positive signals (opens/clicks/conversions)
// reactivation  → invert recency: reward dormant customers that still have a
//
//	history (their positive/conversion counts persist)
//
// retention/awareness → balanced, base score carries the ranking
var objectiveBoosts = map[string]struct {
	conversions, positive, dormancy float64
}{
	"conversion":   {conversions: 0.30, positive: 0.05},
	"engagement":   {conversions: 0.05, positive: 0.25},
	"reactivation": {conversions: 0.10, positive: 0.05, dormancy: 0.30},
	"retention":    {conversions: 0.10, positive: 0.10},
	"awareness":    {},
}

// weightedScore applies the objective boost map. Pure, O(1).
func weightedScore(objective string, c candidate, now time.Time) float64 {
	b, ok := objectiveBoosts[objective]
	if !ok {
		return c.rawScore
	}
	s := c.rawScore +
		b.conversions*math.Log1p(float64(c.conversions)) +
		b.positive*math.Log1p(float64(c.positiveEvents))
	if b.dormancy > 0 && c.lastEventAt != nil {
		if days := now.Sub(*c.lastEventAt).Hours() / 24; days > 0 {
			s += b.dormancy * math.Log1p(days)
		}
	}
	return s
}

// displayScore squashes the unbounded weighted score into [0,1) via the
// monotone map s/(s+1). Ranking is preserved (monotone transform) while the
// response shape matches the contract example ("score":0.83).
func displayScore(weighted float64) float64 {
	if weighted <= 0 {
		return 0
	}
	return weighted / (weighted + 1)
}
