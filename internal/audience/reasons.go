package audience

import (
	"fmt"
	"time"
)

// buildReasons produces 2–4 human-readable, data-backed explanations for why
// a candidate was selected — CONTRACTS §5 step 5. Pure, O(1).
func buildReasons(c candidate, req *RecommendRequest, rank, considered int, now time.Time) []string {
	reasons := make([]string, 0, 4)

	// Score reason, pool-relative: "top-quartile" is only claimed when the
	// candidate's rank sits in the top 25% of everything considered.
	disp := displayScore(c.weighted)
	if considered >= 4 && rank <= (considered+3)/4 {
		reasons = append(reasons, fmt.Sprintf("score=%.2f top-quartile of %d considered", disp, considered))
	} else {
		reasons = append(reasons, fmt.Sprintf("score=%.2f", disp))
	}

	if req.Channel != "" && c.preferredChannel == req.Channel {
		reasons = append(reasons, fmt.Sprintf("preferred_channel=%s matches", c.preferredChannel))
	}

	// Objective-specific evidence.
	switch req.Objective {
	case "conversion":
		if c.conversions > 0 {
			reasons = append(reasons, fmt.Sprintf("converted %dx", c.conversions))
		}
	case "engagement":
		if c.positiveEvents > 0 {
			reasons = append(reasons, fmt.Sprintf("%d positive events", c.positiveEvents))
		}
	case "reactivation":
		if days := daysSince(c.lastEventAt, now); days > 0 {
			reasons = append(reasons, fmt.Sprintf("dormant %dd with %d events history", days, c.totalEvents))
		}
	}

	if days := daysSince(c.lastEventAt, now); days >= 0 {
		if days == 0 {
			reasons = append(reasons, "active today")
		} else {
			reasons = append(reasons, fmt.Sprintf("active %dd ago", days))
		}
	}
	if c.trend == "rising" {
		reasons = append(reasons, "activity_trend=rising")
	}

	if len(reasons) < 2 {
		reasons = append(reasons, "matches all audience filters")
	}
	if len(reasons) > 4 {
		reasons = reasons[:4]
	}
	return reasons
}

// daysSince returns whole days between lastEventAt and now, or -1 when the
// customer has no recorded activity.
func daysSince(lastEventAt *time.Time, now time.Time) int {
	if lastEventAt == nil {
		return -1
	}
	d := int(now.Sub(*lastEventAt).Hours() / 24)
	if d < 0 {
		return 0 // clock skew / future timestamps clamp to "today"
	}
	return d
}
