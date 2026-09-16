package ai

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// varied metrics the fallback must narrate without ever emitting a number
// absent from the facts (verified by round-tripping through the validators).
func variedMetrics() []struct {
	name string
	m    *Metrics
} {
	return []struct {
		name string
		m    *Metrics
	}{
		{"zero events", &Metrics{CampaignID: 1}},
		{"tiny insufficient sample", &Metrics{
			CampaignID: 2, Sends: 10, Delivered: 9, Opens: 9, Clicks: 8, Conversions: 2}},
		{"bounce heavy", &Metrics{
			CampaignID: 3, Sends: 1000, Delivered: 920, Opens: 100, Clicks: 10,
			Conversions: 1, Bounces: 80, Unsubscribes: 8}},
		{"clicks exceed opens anomaly", &Metrics{
			CampaignID: 4, Sends: 500, Delivered: 480, Opens: 50, Clicks: 60, Conversions: 5}},
		{"healthy large", &Metrics{
			CampaignID: 5, Sends: 100000, Delivered: 99000, Opens: 40000, Clicks: 8000,
			Conversions: 1500, Bounces: 200, Unsubscribes: 100}},
	}
}

func TestFallbackOutputAlwaysValid(t *testing.T) {
	fb := NewRuleBasedProvider()
	ctx := context.Background()

	for _, tc := range variedMetrics() {
		t.Run(tc.name+" analyze", func(t *testing.T) {
			factsJSON := factsFixtureJSON(t, tc.m)
			raw, err := fb.Complete(ctx, AnalyzePrompt(factsJSON))
			require.NoError(t, err)
			a, err := ParseAnalysis(raw, factsJSON)
			require.NoError(t, err, "fallback output must pass its own validator")
			assert.NotEmpty(t, a.Summary)
		})
		t.Run(tc.name+" recommend", func(t *testing.T) {
			factsJSON := factsFixtureJSON(t, tc.m)
			raw, err := fb.Complete(ctx, RecommendPrompt(factsJSON))
			require.NoError(t, err)
			recs, err := ParseRecommendations(raw, factsJSON)
			require.NoError(t, err, "fallback output must pass its own validator")
			assert.NotEmpty(t, recs)
		})
	}
}

func TestFallbackAnalyzeZeroEvents(t *testing.T) {
	fb := NewRuleBasedProvider()
	facts := BuildFacts("camp_empty", &Metrics{CampaignID: 9}, "")
	a := fb.Analyze(facts)
	assert.LessOrEqual(t, a.Confidence, 0.3)
	require.NotEmpty(t, a.Weaknesses)
	assert.Contains(t, a.Weaknesses[0], "Insufficient data")
}

func TestFallbackAnalyzeAnomalies(t *testing.T) {
	fb := NewRuleBasedProvider()

	facts := BuildFacts("c", &Metrics{Sends: 1000, Delivered: 920, Opens: 100,
		Clicks: 10, Conversions: 1, Bounces: 80}, "")
	a := fb.Analyze(facts)
	assert.NotEmpty(t, a.Anomalies, "high bounce rate should flag an anomaly")

	facts = BuildFacts("c", &Metrics{Sends: 500, Delivered: 480, Opens: 50, Clicks: 60}, "")
	a = fb.Analyze(facts)
	require.NotEmpty(t, a.Anomalies, "clicks>opens should flag an anomaly")
	assert.Contains(t, a.Anomalies[0], "exceed")
}

func TestFallbackRecommend(t *testing.T) {
	fb := NewRuleBasedProvider()

	// Zero events → single strategy rec, low confidence.
	recs := fb.Recommend(BuildFacts("c", &Metrics{}, ""), "conversion")
	require.Len(t, recs, 1)
	assert.Equal(t, "strategy", recs[0].Area)
	assert.LessOrEqual(t, recs[0].Confidence, 0.3)

	// Healthy campaign → at least one recommendation, valid areas.
	recs = fb.Recommend(BuildFacts("c", &Metrics{
		Sends: 100000, Delivered: 99000, Opens: 40000, Clicks: 8000,
		Conversions: 1500, Bounces: 200, Unsubscribes: 100}, ""), "")
	assert.NotEmpty(t, recs)
	for _, r := range recs {
		assert.True(t, validAreas[r.Area], "area %q", r.Area)
		assert.GreaterOrEqual(t, r.Confidence, 0.0)
		assert.LessOrEqual(t, r.Confidence, 1.0)
	}
}

func TestFallbackCompleteBadPrompt(t *testing.T) {
	fb := NewRuleBasedProvider()
	_, err := fb.Complete(context.Background(), "no facts here")
	require.Error(t, err)
}
