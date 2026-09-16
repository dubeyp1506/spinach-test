package campaigns

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRate(t *testing.T) {
	tests := []struct {
		name     string
		num, den int64
		want     float64
	}{
		{"zero denominator", 5, 0, 0},
		{"both zero", 0, 0, 0},
		{"negative denominator", 3, -1, 0},
		{"quarter", 1, 4, 0.25},
		{"half", 3, 6, 0.5},
		{"all", 7, 7, 1.0},
		{"zero numerator", 0, 10, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rate(tt.num, tt.den)
			assert.Equal(t, tt.want, got)
			assert.False(t, math.IsNaN(got), "rate must never be NaN")
		})
	}
}

func TestPivotMetrics(t *testing.T) {
	first := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	last := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	t.Run("channel x type pivot with rates", func(t *testing.T) {
		groups := []channelTypeCount{
			{Channel: "email", Type: "sent", Count: 100},
			{Channel: "email", Type: "delivered", Count: 95},
			{Channel: "email", Type: "opened", Count: 40},
			{Channel: "email", Type: "clicked", Count: 10},
			{Channel: "email", Type: "converted", Count: 4},
			{Channel: "email", Type: "bounced", Count: 5},
			{Channel: "email", Type: "unsubscribed", Count: 2},
			{Channel: "email", Type: "complained", Count: 1},
			{Channel: "sms", Type: "sent", Count: 50},
			{Channel: "sms", Type: "delivered", Count: 50},
			{Channel: "sms", Type: "opened", Count: 20},
			{Channel: "sms", Type: "converted", Count: 3},
		}
		m := pivotMetrics(7, groups, first, last)

		assert.Equal(t, int64(7), m.CampaignID)
		// top-level totals across channels
		assert.Equal(t, int64(150), m.Sends)
		assert.Equal(t, int64(145), m.Delivered)
		assert.Equal(t, int64(60), m.Opens)
		assert.Equal(t, int64(10), m.Clicks)
		assert.Equal(t, int64(7), m.Conversions)
		assert.Equal(t, int64(5), m.Bounces)
		assert.Equal(t, int64(2), m.Unsubscribes)
		// rates over delivered
		assert.InDelta(t, 60.0/145.0, m.OpenRate, 1e-9)
		assert.InDelta(t, 10.0/145.0, m.ClickRate, 1e-9)
		assert.InDelta(t, 7.0/145.0, m.ConversionRate, 1e-9)
		assert.Equal(t, first, m.FirstEventAt)
		assert.Equal(t, last, m.LastEventAt)
		// per-channel pivot
		require.Len(t, m.ByChannel, 2)
		em := m.ByChannel["email"]
		assert.Equal(t, int64(100), em.Sends)
		assert.Equal(t, int64(95), em.Delivered)
		assert.Equal(t, int64(40), em.Opens)
		assert.Equal(t, int64(1), em.Complained)
		assert.InDelta(t, 40.0/95.0, em.OpenRate, 1e-9)
		assert.InDelta(t, 4.0/95.0, em.ConversionRate, 1e-9)
		sm := m.ByChannel["sms"]
		assert.Equal(t, int64(50), sm.Sends)
		assert.Equal(t, int64(20), sm.Opens)
		assert.InDelta(t, 20.0/50.0, sm.OpenRate, 1e-9)
	})

	t.Run("empty events yield zeros not NaN", func(t *testing.T) {
		m := pivotMetrics(9, nil, time.Time{}, time.Time{})
		assert.Equal(t, int64(9), m.CampaignID)
		assert.Zero(t, m.Sends)
		assert.Zero(t, m.Delivered)
		assert.Zero(t, m.OpenRate)
		assert.Zero(t, m.ClickRate)
		assert.Zero(t, m.ConversionRate)
		assert.Empty(t, m.ByChannel)
		assert.True(t, m.FirstEventAt.IsZero())
		assert.False(t, math.IsNaN(m.OpenRate))
	})

	t.Run("sends but zero delivered yields zero rates", func(t *testing.T) {
		m := pivotMetrics(3, []channelTypeCount{
			{Channel: "push", Type: "sent", Count: 10},
			{Channel: "push", Type: "bounced", Count: 10},
		}, first, last)
		assert.Equal(t, int64(10), m.Sends)
		assert.Zero(t, m.Delivered)
		assert.Zero(t, m.OpenRate)
		assert.Zero(t, m.ByChannel["push"].ConversionRate)
	})
}
