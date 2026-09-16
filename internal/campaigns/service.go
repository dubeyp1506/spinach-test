// Package campaigns implements campaign metrics & analytics (CONTRACTS §3, §4).
// Owned exclusively by workstream A4.
package campaigns

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spinach/martech-engine/internal/config"
)

// CampaignMetrics is the CONTRACTS §3 contract struct consumed by the A5 AI
// workstream — do not change field names/types without updating CONTRACTS.md.
type CampaignMetrics struct {
	CampaignID     int64                     `json:"campaign_id"`
	Sends          int64                     `json:"sends"`
	Delivered      int64                     `json:"delivered"`
	Opens          int64                     `json:"opens"`
	Clicks         int64                     `json:"clicks"`
	Conversions    int64                     `json:"conversions"`
	Bounces        int64                     `json:"bounces"`
	Unsubscribes   int64                     `json:"unsubscribes"`
	OpenRate       float64                   `json:"open_rate"`
	ClickRate      float64                   `json:"click_rate"`
	ConversionRate float64                   `json:"conversion_rate"`
	ByChannel      map[string]ChannelMetrics `json:"by_channel"`
	FirstEventAt   time.Time                 `json:"first_event_at"`
	LastEventAt    time.Time                 `json:"last_event_at"`
}

// ChannelMetrics is the per-channel pivot of CampaignMetrics.ByChannel.
// Same counters as the top level, plus complained (no top-level field in §3).
type ChannelMetrics struct {
	Sends          int64   `json:"sends"`
	Delivered      int64   `json:"delivered"`
	Opens          int64   `json:"opens"`
	Clicks         int64   `json:"clicks"`
	Conversions    int64   `json:"conversions"`
	Bounces        int64   `json:"bounces"`
	Unsubscribes   int64   `json:"unsubscribes"`
	Complained     int64   `json:"complained"`
	OpenRate       float64 `json:"open_rate"`
	ClickRate      float64 `json:"click_rate"`
	ConversionRate float64 `json:"conversion_rate"`
}

// AnalyticsService is the CONTRACTS §3 seam: A4 implements, A5 (AI) consumes.
type AnalyticsService interface {
	// Metrics returns aggregated metrics for one campaign by internal id.
	Metrics(ctx context.Context, campaignID int64) (*CampaignMetrics, error)
}

// Service implements campaigns.AnalyticsService (CONTRACTS §3) and serves the
// campaign HTTP endpoints (§4).
type Service struct {
	pool *pgxpool.Pool
	cfg  *config.Config
}

var _ AnalyticsService = (*Service)(nil)

func New(pool *pgxpool.Pool, cfg *config.Config) *Service {
	return &Service{pool: pool, cfg: cfg}
}

// Metrics implements campaigns.AnalyticsService (CONTRACTS §3).
// Single GROUP BY query over events; the channel×type pivot happens in Go —
// no N+1. A campaignID with no events yields a zero-valued struct, not an error.
func (s *Service) Metrics(ctx context.Context, campaignID int64) (*CampaignMetrics, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT channel, type, COUNT(*) AS n,
		       MIN(occurred_at) AS first_at, MAX(occurred_at) AS last_at
		FROM events
		WHERE campaign_id = $1
		GROUP BY channel, type`, campaignID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groups []channelTypeCount
	var first, last time.Time
	for rows.Next() {
		var g channelTypeCount
		var mn, mx time.Time
		if err := rows.Scan(&g.Channel, &g.Type, &g.Count, &mn, &mx); err != nil {
			return nil, err
		}
		if first.IsZero() || mn.Before(first) {
			first = mn
		}
		if mx.After(last) {
			last = mx
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pivotMetrics(campaignID, groups, first, last), nil
}

// channelTypeCount is one row of the channel×type GROUP BY result.
type channelTypeCount struct {
	Channel string
	Type    string
	Count   int64
}

// pivotMetrics folds channel×type count rows into the §3 metrics struct.
// Rates use delivered as the denominator; a 0 denominator yields 0, never NaN.
// Pure function — unit-tested without a DB.
func pivotMetrics(campaignID int64, groups []channelTypeCount, first, last time.Time) *CampaignMetrics {
	m := &CampaignMetrics{
		CampaignID:   campaignID,
		ByChannel:    map[string]ChannelMetrics{},
		FirstEventAt: first,
		LastEventAt:  last,
	}
	for _, g := range groups {
		cm := m.ByChannel[g.Channel]
		switch g.Type {
		case "sent":
			m.Sends += g.Count
			cm.Sends += g.Count
		case "delivered":
			m.Delivered += g.Count
			cm.Delivered += g.Count
		case "opened":
			m.Opens += g.Count
			cm.Opens += g.Count
		case "clicked":
			m.Clicks += g.Count
			cm.Clicks += g.Count
		case "converted":
			m.Conversions += g.Count
			cm.Conversions += g.Count
		case "bounced":
			m.Bounces += g.Count
			cm.Bounces += g.Count
		case "unsubscribed":
			m.Unsubscribes += g.Count
			cm.Unsubscribes += g.Count
		case "complained":
			cm.Complained += g.Count
		}
		m.ByChannel[g.Channel] = cm
	}
	m.OpenRate = rate(m.Opens, m.Delivered)
	m.ClickRate = rate(m.Clicks, m.Delivered)
	m.ConversionRate = rate(m.Conversions, m.Delivered)
	for ch, cm := range m.ByChannel {
		cm.OpenRate = rate(cm.Opens, cm.Delivered)
		cm.ClickRate = rate(cm.Clicks, cm.Delivered)
		cm.ConversionRate = rate(cm.Conversions, cm.Delivered)
		m.ByChannel[ch] = cm
	}
	return m
}
