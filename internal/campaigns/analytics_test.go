package campaigns

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaselineFromTotals(t *testing.T) {
	t.Run("rates over delivered", func(t *testing.T) {
		b := baselineFromTotals(map[string]int64{
			"sent": 1000, "delivered": 900,
			"opened": 225, "clicked": 45, "converted": 9,
		})
		assert.InDelta(t, 0.25, b.OpenRate, 1e-9)
		assert.InDelta(t, 0.05, b.ClickRate, 1e-9)
		assert.InDelta(t, 0.01, b.ConversionRate, 1e-9)
	})
	t.Run("empty totals yield zero rates", func(t *testing.T) {
		b := baselineFromTotals(map[string]int64{})
		assert.Zero(t, b.OpenRate)
		assert.Zero(t, b.ClickRate)
		assert.Zero(t, b.ConversionRate)
	})
}

func TestDetectAnomalies(t *testing.T) {
	flatDaily := []dailyCount{
		{Day: "2026-09-16", Count: 5},
		{Day: "2026-09-17", Count: 5},
		{Day: "2026-09-18", Count: 5},
		{Day: "2026-09-19", Count: 5},
	}

	tests := []struct {
		name         string
		metrics      *CampaignMetrics
		baseline     PlatformBaseline
		daily        []dailyCount
		active       bool
		delivered48h int64
		want         []string // exact expected anomaly strings
	}{
		{
			name:     "open rate below 50% of baseline",
			metrics:  &CampaignMetrics{Delivered: 100, Opens: 11, OpenRate: 0.11, Sends: 100},
			baseline: PlatformBaseline{OpenRate: 0.24},
			// strictly below half: 0.11 < 0.5*0.24=0.12 (0.12 vs 0.24 is
			// exactly 50% and must not flag — see boundary case below)
			want: []string{"open_rate 11% vs platform avg 24% (-54%)"},
		},
		{
			name:     "open rate healthy",
			metrics:  &CampaignMetrics{Delivered: 100, Opens: 18, OpenRate: 0.18, Sends: 100},
			baseline: PlatformBaseline{OpenRate: 0.24},
			want:     []string{},
		},
		{
			name:     "open rate at exactly half baseline does not flag",
			metrics:  &CampaignMetrics{Delivered: 100, Opens: 15, OpenRate: 0.15, Sends: 100},
			baseline: PlatformBaseline{OpenRate: 0.30},
			// 0.15 < 0.5*0.30 is false (strict less-than; 0.30/2 == 0.15 in float64)
			want: []string{},
		},
		{
			name:         "no deliveries skips open-rate check and flags nothing when inactive",
			metrics:      &CampaignMetrics{Sends: 100},
			baseline:     PlatformBaseline{OpenRate: 0.24},
			active:       false,
			delivered48h: 0,
			want:         []string{},
		},
		{
			name:     "bounce rate over 5%",
			metrics:  &CampaignMetrics{Sends: 1000, Bounces: 60},
			baseline: PlatformBaseline{},
			want:     []string{"bounce_rate 6.0% exceeds 5% threshold (60 bounces / 1000 sends)"},
		},
		{
			name:     "bounce rate at exactly 5% does not flag",
			metrics:  &CampaignMetrics{Sends: 1000, Bounces: 50},
			baseline: PlatformBaseline{},
			want:     []string{},
		},
		{
			name:     "bounce rate safe with zero sends",
			metrics:  &CampaignMetrics{Bounces: 10},
			baseline: PlatformBaseline{},
			want:     []string{},
		},
		{
			name:     "conversion spike over 3 sigma",
			metrics:  &CampaignMetrics{},
			baseline: PlatformBaseline{},
			daily: []dailyCount{
				{Day: "2026-09-16", Count: 5},
				{Day: "2026-09-17", Count: 5},
				{Day: "2026-09-18", Count: 5},
				{Day: "2026-09-19", Count: 5},
				{Day: "2026-09-20", Count: 50},
			},
			want: []string{"conversions spiked: 50 on 2026-09-20 vs mean 5.0/day (σ=0.0, >3σ)"},
		},
		{
			name:     "flat conversions do not flag",
			metrics:  &CampaignMetrics{},
			baseline: PlatformBaseline{},
			daily:    flatDaily,
			want:     []string{},
		},
		{
			name:     "spike needs at least 3 days of data",
			metrics:  &CampaignMetrics{},
			baseline: PlatformBaseline{},
			daily: []dailyCount{
				{Day: "2026-09-19", Count: 1},
				{Day: "2026-09-20", Count: 100},
			},
			want: []string{},
		},
		{
			name:         "zero delivered in 48h on active campaign",
			metrics:      &CampaignMetrics{Sends: 100},
			baseline:     PlatformBaseline{},
			active:       true,
			delivered48h: 0,
			want:         []string{"zero delivered events in last 48h on active campaign"},
		},
		{
			name:         "zero delivered ignored when campaign not active",
			metrics:      &CampaignMetrics{Sends: 100},
			baseline:     PlatformBaseline{},
			active:       false,
			delivered48h: 0,
			want:         []string{},
		},
		{
			name:         "deliveries present on active campaign",
			metrics:      &CampaignMetrics{Sends: 100, Delivered: 90, Opens: 30, OpenRate: 1.0 / 3.0},
			baseline:     PlatformBaseline{OpenRate: 0.24},
			active:       true,
			delivered48h: 5,
			want:         []string{},
		},
		{
			name: "multiple anomalies combined",
			metrics: &CampaignMetrics{
				Sends: 1000, Delivered: 900, Opens: 90, Bounces: 80,
				OpenRate: 0.10,
			},
			baseline:     PlatformBaseline{OpenRate: 0.30},
			daily:        flatDaily,
			active:       true,
			delivered48h: 0,
			want: []string{
				"open_rate 10% vs platform avg 30% (-67%)",
				"bounce_rate 8.0% exceeds 5% threshold (80 bounces / 1000 sends)",
				"zero delivered events in last 48h on active campaign",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectAnomalies(tt.metrics, tt.baseline, tt.daily, tt.active, tt.delivered48h)
			require.NotNil(t, got, "anomalies must serialize as [] not null")
			assert.Equal(t, tt.want, got)
		})
	}
}
