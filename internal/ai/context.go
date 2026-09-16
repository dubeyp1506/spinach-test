// Package ai implements the A5 AI layer: an LLM provider chain
// (groq → gemini → deterministic rule-based fallback), §8 workload-isolation
// bulkheads (semaphore, per-provider circuit breakers, Redis response cache,
// tighter rate limit), and the /campaigns/{id}/analyze|recommend endpoints.
// See docs/CONTRACTS.md §3 (interfaces), §4 (endpoints), §8 (isolation).
package ai

import (
	"context"
	"math"
	"time"
)

// MetricsProvider is the consumer-defined seam to the campaigns module
// (CONTRACTS §3). Integration adapts campaigns.AnalyticsService to satisfy it;
// the AI layer never imports internal/campaigns.
type MetricsProvider interface {
	// Metrics returns computed campaign metrics. campaignID is the internal
	// BIGINT id resolved from the external_id path param.
	Metrics(ctx context.Context, campaignID int64) (*Metrics, error)
}

// Metrics mirrors campaigns.CampaignMetrics (CONTRACTS §3). Rates are
// fractions in [0,1]; they are re-derived from counts in BuildFacts when
// denominators are non-zero so the facts object is always self-consistent.
type Metrics struct {
	CampaignID     int64
	Sends          int64
	Delivered      int64
	Opens          int64
	Clicks         int64
	Conversions    int64
	Bounces        int64
	Unsubscribes   int64
	OpenRate       float64
	ClickRate      float64
	ConversionRate float64
	ByChannel      map[string]ChannelMetrics
	FirstEventAt   time.Time
	LastEventAt    time.Time
}

// ChannelMetrics mirrors the per-channel breakdown in §3 CampaignMetrics.
type ChannelMetrics struct {
	Sends          int64
	Delivered      int64
	Opens          int64
	Clicks         int64
	Conversions    int64
	Bounces        int64
	Unsubscribes   int64
	OpenRate       float64
	ClickRate      float64
	ConversionRate float64
}

// minSampleSends is the minimum send volume for rates to be treated as
// reliable. Below it DataSufficient=false and outputs carry low confidence.
const minSampleSends = 30

// Benchmarks are deterministic thresholds used by the rule-based fallback.
// They live inside the facts JSON so fallback text that cites them still
// satisfies the number-containment validation rule.
type Benchmarks struct {
	OpenRatePctGood       float64 `json:"open_rate_pct_good"`
	ClickRatePctGood      float64 `json:"click_rate_pct_good"`
	ConversionRatePctGood float64 `json:"conversion_rate_pct_good"`
	BounceRatePctBad      float64 `json:"bounce_rate_pct_bad"`
	UnsubRatePctBad       float64 `json:"unsub_rate_pct_bad"`
}

var defaultBenchmarks = Benchmarks{
	OpenRatePctGood:       20,
	ClickRatePctGood:      3,
	ConversionRatePctGood: 1,
	BounceRatePctBad:      5,
	UnsubRatePctBad:       0.5,
}

// ChannelFacts is the per-channel slice of Facts (counts + percent rates).
type ChannelFacts struct {
	Sends             int64   `json:"sends"`
	Delivered         int64   `json:"delivered"`
	Opens             int64   `json:"opens"`
	Clicks            int64   `json:"clicks"`
	Conversions       int64   `json:"conversions"`
	Bounces           int64   `json:"bounces"`
	Unsubscribes      int64   `json:"unsubscribes"`
	OpenRatePct       float64 `json:"open_rate_pct"`
	ClickRatePct      float64 `json:"click_rate_pct"`
	ConversionRatePct float64 `json:"conversion_rate_pct"`
}

// Facts is the deterministic metrics object computed in Go. It is embedded in
// the prompt (LLM narrates, never computes), returned verbatim in the response
// `facts` field, and is the ground truth for number-containment validation.
// No customer PII ever appears here — aggregates only.
type Facts struct {
	CampaignID        string                  `json:"campaign_id"` // external_id
	Objective         string                  `json:"objective,omitempty"`
	Sends             int64                   `json:"sends"`
	Delivered         int64                   `json:"delivered"`
	Opens             int64                   `json:"opens"`
	Clicks            int64                   `json:"clicks"`
	Conversions       int64                   `json:"conversions"`
	Bounces           int64                   `json:"bounces"`
	Unsubscribes      int64                   `json:"unsubscribes"`
	OpenRate          float64                 `json:"open_rate"`
	ClickRate         float64                 `json:"click_rate"`
	ConversionRate    float64                 `json:"conversion_rate"`
	OpenRatePct       float64                 `json:"open_rate_pct"`
	ClickRatePct      float64                 `json:"click_rate_pct"`
	ConversionRatePct float64                 `json:"conversion_rate_pct"`
	BounceRatePct     float64                 `json:"bounce_rate_pct"`
	UnsubRatePct      float64                 `json:"unsub_rate_pct"`
	MinSampleSends    int64                   `json:"min_sample_sends"`
	DataSufficient    bool                    `json:"data_sufficient"`
	ByChannel         map[string]ChannelFacts `json:"by_channel,omitempty"`
	FirstEventAt      *time.Time              `json:"first_event_at,omitempty"`
	LastEventAt       *time.Time              `json:"last_event_at,omitempty"`
	Benchmarks        Benchmarks              `json:"benchmarks"`
}

// BuildFacts converts provider metrics into the facts object. Rates are
// recomputed from counts when a denominator exists so counts and rates can
// never disagree; otherwise the provider-supplied rate is used.
func BuildFacts(externalID string, m *Metrics, objective string) *Facts {
	if m == nil {
		m = &Metrics{}
	}
	openRate := derivedRate(m.Opens, m.Delivered, m.OpenRate)
	clickRate := derivedRate(m.Clicks, m.Delivered, m.ClickRate)
	convRate := derivedRate(m.Conversions, m.Delivered, m.ConversionRate)
	f := &Facts{
		CampaignID:        externalID,
		Objective:         objective,
		Sends:             m.Sends,
		Delivered:         m.Delivered,
		Opens:             m.Opens,
		Clicks:            m.Clicks,
		Conversions:       m.Conversions,
		Bounces:           m.Bounces,
		Unsubscribes:      m.Unsubscribes,
		OpenRate:          round4(openRate),
		ClickRate:         round4(clickRate),
		ConversionRate:    round4(convRate),
		OpenRatePct:       pct1(openRate),
		ClickRatePct:      pct1(clickRate),
		ConversionRatePct: pct1(convRate),
		BounceRatePct:     pct1(derivedRate(m.Bounces, m.Sends, 0)),
		UnsubRatePct:      pct1(derivedRate(m.Unsubscribes, m.Delivered, 0)),
		MinSampleSends:    minSampleSends,
		DataSufficient:    m.Sends >= minSampleSends,
		Benchmarks:        defaultBenchmarks,
	}
	if !m.FirstEventAt.IsZero() {
		t := m.FirstEventAt.UTC()
		f.FirstEventAt = &t
	}
	if !m.LastEventAt.IsZero() {
		t := m.LastEventAt.UTC()
		f.LastEventAt = &t
	}
	if len(m.ByChannel) > 0 {
		f.ByChannel = make(map[string]ChannelFacts, len(m.ByChannel))
		for ch, cm := range m.ByChannel {
			f.ByChannel[ch] = ChannelFacts{
				Sends:             cm.Sends,
				Delivered:         cm.Delivered,
				Opens:             cm.Opens,
				Clicks:            cm.Clicks,
				Conversions:       cm.Conversions,
				Bounces:           cm.Bounces,
				Unsubscribes:      cm.Unsubscribes,
				OpenRatePct:       pct1(derivedRate(cm.Opens, cm.Delivered, cm.OpenRate)),
				ClickRatePct:      pct1(derivedRate(cm.Clicks, cm.Delivered, cm.ClickRate)),
				ConversionRatePct: pct1(derivedRate(cm.Conversions, cm.Delivered, cm.ConversionRate)),
			}
		}
	}
	return f
}

// derivedRate returns num/den when den>0, else the supplied fallback rate.
func derivedRate(num, den int64, fallback float64) float64 {
	if den > 0 {
		return float64(num) / float64(den)
	}
	return fallback
}

func round4(v float64) float64 { return math.Round(v*1e4) / 1e4 }

// pct1 converts a fraction to a 1-decimal percent (0.215 → 21.5).
func pct1(v float64) float64 { return math.Round(v*1000) / 10 }
