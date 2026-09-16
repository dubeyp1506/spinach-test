package campaigns

import (
	"context"
	"fmt"
	"math"
	"time"
)

// PlatformBaseline holds platform-wide engagement rates computed across all
// campaigns — the yardstick for per-campaign anomaly detection.
type PlatformBaseline struct {
	OpenRate       float64 `json:"open_rate"`
	ClickRate      float64 `json:"click_rate"`
	ConversionRate float64 `json:"conversion_rate"`
}

// CampaignAnalytics bundles the deterministic "facts" handed to the AI layer
// (A5) and returned by GET /campaigns/{id}/analytics (CONTRACTS §4).
type CampaignAnalytics struct {
	Metrics   *CampaignMetrics
	Baseline  PlatformBaseline
	Anomalies []string
}

// Analytics gathers the deterministic facts for one campaign in a fixed number
// of queries (metrics pivot + baseline + daily conversions + 48h delivered) —
// no N+1. status is the campaign row's status, already resolved by the caller.
func (s *Service) Analytics(ctx context.Context, campaignID int64, status string) (*CampaignAnalytics, error) {
	m, err := s.Metrics(ctx, campaignID)
	if err != nil {
		return nil, err
	}
	base, err := s.platformBaseline(ctx)
	if err != nil {
		return nil, err
	}
	daily, err := s.dailyConversions(ctx, campaignID)
	if err != nil {
		return nil, err
	}
	var delivered48h int64
	if status == "active" {
		delivered48h, err = s.deliveredSince(ctx, campaignID, time.Now().Add(-48*time.Hour))
		if err != nil {
			return nil, err
		}
	}
	return &CampaignAnalytics{
		Metrics:   m,
		Baseline:  base,
		Anomalies: detectAnomalies(m, base, daily, status == "active", delivered48h),
	}, nil
}

// platformBaseline aggregates event-type totals across ALL campaigns into
// platform-wide rates. One GROUP BY query.
func (s *Service) platformBaseline(ctx context.Context) (PlatformBaseline, error) {
	rows, err := s.pool.Query(ctx, `SELECT type, COUNT(*) FROM events GROUP BY type`)
	if err != nil {
		return PlatformBaseline{}, err
	}
	defer rows.Close()

	totals := map[string]int64{}
	for rows.Next() {
		var typ string
		var n int64
		if err := rows.Scan(&typ, &n); err != nil {
			return PlatformBaseline{}, err
		}
		totals[typ] = n
	}
	if err := rows.Err(); err != nil {
		return PlatformBaseline{}, err
	}
	return baselineFromTotals(totals), nil
}

// baselineFromTotals computes platform rates from event-type totals.
// Same denominator convention as pivotMetrics (delivered). Pure function.
func baselineFromTotals(totals map[string]int64) PlatformBaseline {
	delivered := totals["delivered"]
	return PlatformBaseline{
		OpenRate:       rate(totals["opened"], delivered),
		ClickRate:      rate(totals["clicked"], delivered),
		ConversionRate: rate(totals["converted"], delivered),
	}
}

// dailyCount is one day of conversion events, ascending by day.
type dailyCount struct {
	Day   string
	Count int64
}

// dailyConversions returns per-day conversion counts for a campaign, used for
// the day-over-day 3σ spike check.
func (s *Service) dailyConversions(ctx context.Context, campaignID int64) ([]dailyCount, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT occurred_at::date::text AS day, COUNT(*) AS n
		FROM events
		WHERE campaign_id = $1 AND type = 'converted'
		GROUP BY occurred_at::date
		ORDER BY day`, campaignID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []dailyCount
	for rows.Next() {
		var d dailyCount
		if err := rows.Scan(&d.Day, &d.Count); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// deliveredSince counts delivered events for a campaign since a cutoff —
// feeds the "zero delivered in 48h on active campaign" rule.
func (s *Service) deliveredSince(ctx context.Context, campaignID int64, since time.Time) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM events
		WHERE campaign_id = $1 AND type = 'delivered' AND occurred_at > $2`,
		campaignID, since).Scan(&n)
	return n, err
}

// detectAnomalies turns metrics into human-readable fact strings for the AI
// layer. Pure function — unit-tested without a DB. Rules, in order:
//  1. open_rate < 50% of platform baseline (only when the campaign has
//     deliveries — a dead campaign is covered by rule 4 instead)
//  2. latest-day conversions > mean + 3σ over prior daily counts (needs ≥3 days)
//  3. bounce_rate (bounces/sends) > 5%
//  4. zero delivered events in the trailing 48h on an active campaign
func detectAnomalies(m *CampaignMetrics, b PlatformBaseline, daily []dailyCount, active bool, delivered48h int64) []string {
	out := []string{}

	if m.Delivered > 0 && b.OpenRate > 0 && m.OpenRate < 0.5*b.OpenRate {
		out = append(out, fmt.Sprintf(
			"open_rate %.0f%% vs platform avg %.0f%% (%+.0f%%)",
			m.OpenRate*100, b.OpenRate*100,
			(m.OpenRate-b.OpenRate)/b.OpenRate*100))
	}

	if n := len(daily); n >= 3 {
		prev := daily[:n-1]
		var sum float64
		for _, d := range prev {
			sum += float64(d.Count)
		}
		mean := sum / float64(len(prev))
		var sq float64
		for _, d := range prev {
			diff := float64(d.Count) - mean
			sq += diff * diff
		}
		sd := math.Sqrt(sq / float64(len(prev))) // population stddev
		if last := daily[n-1]; float64(last.Count) > mean+3*sd {
			out = append(out, fmt.Sprintf(
				"conversions spiked: %d on %s vs mean %.1f/day (σ=%.1f, >3σ)",
				last.Count, last.Day, mean, sd))
		}
	}

	if br := rate(m.Bounces, m.Sends); br > 0.05 {
		out = append(out, fmt.Sprintf(
			"bounce_rate %.1f%% exceeds 5%% threshold (%d bounces / %d sends)",
			br*100, m.Bounces, m.Sends))
	}

	if active && delivered48h == 0 {
		out = append(out, "zero delivered events in last 48h on active campaign")
	}
	return out
}

// rate is divide-by-zero safe: denominator <= 0 yields 0, never NaN.
func rate(num, den int64) float64 {
	if den <= 0 {
		return 0
	}
	return float64(num) / float64(den)
}
