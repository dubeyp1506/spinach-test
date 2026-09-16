package ai

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixtureMetrics yields a facts JSON with known numbers:
// sends=1000, delivered=950, opens=300, clicks=60, conversions=12,
// bounces=20, unsubs=4; open 31.6%, click 6.3%, conversion 1.3%,
// bounce 2%, unsub 0.4%; benchmarks 20/3/1/5/0.5; min_sample 30.
func fixtureMetrics() *Metrics {
	return &Metrics{
		CampaignID:   1,
		Sends:        1000,
		Delivered:    950,
		Opens:        300,
		Clicks:       60,
		Conversions:  12,
		Bounces:      20,
		Unsubscribes: 4,
	}
}

func factsFixtureJSON(t *testing.T, m *Metrics) []byte {
	t.Helper()
	fj, err := json.Marshal(BuildFacts("camp_007", m, ""))
	require.NoError(t, err)
	return fj
}

func TestParseAnalysis(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())

	valid := `{"summary":"The campaign logged 1000 sends with a 31.6% open rate.",
		"strengths":["Open rate 31.6% beats the 20% benchmark."],
		"weaknesses":[],"anomalies":[],"confidence":0.8}`

	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"valid", valid, false},
		{"fenced", "```json\n" + valid + "\n```", false},
		{"fenced-no-lang", "```\n" + valid + "\n```", false},
		{"prose around json", "Here you go:\n" + valid + "\nDone.", false},
		{"malformed", "{not json", true},
		{"empty", "", true},
		{"missing summary", `{"strengths":[],"weaknesses":[],"anomalies":[],"confidence":0.5}`, true},
		{"blank summary", `{"summary":"  ","strengths":[],"weaknesses":[],"anomalies":[],"confidence":0.5}`, true},
		{"missing arrays", `{"summary":"Saw 1000 sends.","confidence":0.5}`, true},
		{"confidence too high", `{"summary":"Saw 1000 sends.","strengths":[],"weaknesses":[],"anomalies":[],"confidence":1.5}`, true},
		{"confidence negative", `{"summary":"Saw 1000 sends.","strengths":[],"weaknesses":[],"anomalies":[],"confidence":-0.1}`, true},
		{"invented number", `{"summary":"The campaign logged 999999 sends.","strengths":[],"weaknesses":[],"anomalies":[],"confidence":0.5}`, true},
		{"invented in weakness", `{"summary":"Saw 1000 sends.","strengths":[],"weaknesses":["Only 47 opens."],"anomalies":[],"confidence":0.5}`, true},
		{"percent form of fraction", `{"summary":"Click rate 6.3% on 950 deliveries.","strengths":[],"weaknesses":[],"anomalies":[],"confidence":0.5}`, false},
		{"benchmark cited", `{"summary":"Open rate trails the 20% benchmark.","strengths":[],"weaknesses":[],"anomalies":[],"confidence":0.5}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := ParseAnalysis(tc.raw, factsJSON)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, a)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, a)
			assert.NotEmpty(t, a.Summary)
			assert.GreaterOrEqual(t, a.Confidence, 0.0)
			assert.LessOrEqual(t, a.Confidence, 1.0)
		})
	}
}

func TestParseRecommendations(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())

	valid := `{"recommendations":[{"area":"timing","suggestion":"Shift sends earlier.",
		"reasoning":"Open rate is 31.6% across 950 deliveries.","confidence":0.6}]}`

	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"valid", valid, false},
		{"fenced", "```json\n" + valid + "\n```", false},
		{"malformed", "oops", true},
		{"empty list", `{"recommendations":[]}`, true},
		{"missing list", `{"other":1}`, true},
		{"bad area", `{"recommendations":[{"area":"creative","suggestion":"x","reasoning":"y","confidence":0.5}]}`, true},
		{"empty suggestion", `{"recommendations":[{"area":"timing","suggestion":" ","reasoning":"y","confidence":0.5}]}`, true},
		{"empty reasoning", `{"recommendations":[{"area":"timing","suggestion":"x","reasoning":"","confidence":0.5}]}`, true},
		{"bad confidence", `{"recommendations":[{"area":"timing","suggestion":"x","reasoning":"y","confidence":2}]}`, true},
		{"invented number", `{"recommendations":[{"area":"timing","suggestion":"x","reasoning":"Only 777 clicks total.","confidence":0.5}]}`, true},
		{"multi valid", `{"recommendations":[` +
			`{"area":"risk","suggestion":"Clean list.","reasoning":"Bounce rate is 2%.","confidence":0.7},` +
			`{"area":"audience","suggestion":"Grow lookalikes.","reasoning":"12 conversions observed.","confidence":0.5}]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recs, err := ParseRecommendations(tc.raw, factsJSON)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, recs)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, recs)
			for _, r := range recs {
				assert.True(t, validAreas[r.Area])
			}
		})
	}
}
