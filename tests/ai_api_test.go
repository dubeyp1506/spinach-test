// Verifies CONTRACTS.md §4 AI endpoints and §8 resilience: POST
// /campaigns/{id}/analyze returns {facts, analysis, provider, fallback_used}
// and /recommend returns {facts, recommendations[]} — always served, because
// the chain groq → gemini → rule-based-fallback never fails the request.
package tests

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var validRecAreas = map[string]bool{
	"audience": true, "channel": true, "timing": true,
	"segmentation": true, "strategy": true, "risk": true,
}

type analyzeResponse struct {
	Facts    map[string]any `json:"facts"`
	Analysis struct {
		Summary    string   `json:"summary"`
		Strengths  []string `json:"strengths"`
		Weaknesses []string `json:"weaknesses"`
		Anomalies  []string `json:"anomalies"`
		Confidence float64  `json:"confidence"`
	} `json:"analysis"`
	Provider     string `json:"provider"`
	FallbackUsed bool   `json:"fallback_used"`
}

type recommendResponse struct {
	Facts           map[string]any `json:"facts"`
	Recommendations []struct {
		Area       string  `json:"area"`
		Suggestion string  `json:"suggestion"`
		Reasoning  string  `json:"reasoning"`
		Confidence float64 `json:"confidence"`
	} `json:"recommendations"`
	Provider     string `json:"provider"`
	FallbackUsed bool   `json:"fallback_used"`
}

// llmKeysConfigured reports whether the test environment carries provider
// credentials. The API process shares this deployment's env, so absence means
// the deterministic fallback must serve — documenting §8 provider-down
// resilience. When keys ARE present the invariant still holds: fallback_used
// iff provider == rule-based-fallback.
func llmKeysConfigured() bool {
	return os.Getenv("GROQ_API_KEY") != "" || os.Getenv("GEMINI_API_KEY") != ""
}

func assertProviderInvariant(t *testing.T, provider string, fallbackUsed bool) {
	t.Helper()
	assert.Contains(t, []string{"groq", "gemini", "rule-based-fallback"}, provider)
	assert.Equal(t, provider == "rule-based-fallback", fallbackUsed,
		"fallback_used must be true iff the fallback served")
	if !llmKeysConfigured() {
		assert.Equal(t, "rule-based-fallback", provider,
			"no LLM keys configured — chain must land on the deterministic fallback")
		assert.True(t, fallbackUsed)
	}
}

func TestCampaignAnalyze(t *testing.T) {
	requireStack(t)
	camp := pickCampaign(t)

	status, raw := doJSON(t, http.MethodPost, "/campaigns/"+camp+"/analyze", nil)
	require.Equal(t, http.StatusOK, status, "body=%s", raw)

	var out analyzeResponse
	require.NoError(t, json.Unmarshal(raw, &out))

	require.NotEmpty(t, out.Facts, "facts object must carry deterministic metrics")
	assert.Equal(t, camp, out.Facts["campaign_id"])
	assert.NotEmpty(t, out.Analysis.Summary)
	assert.GreaterOrEqual(t, out.Analysis.Confidence, 0.0)
	assert.LessOrEqual(t, out.Analysis.Confidence, 1.0)
	assertProviderInvariant(t, out.Provider, out.FallbackUsed)
}

func TestCampaignRecommend(t *testing.T) {
	requireStack(t)
	camp := pickCampaign(t)

	status, raw := doJSON(t, http.MethodPost, "/campaigns/"+camp+"/recommend",
		map[string]any{"objective": "conversion"})
	require.Equal(t, http.StatusOK, status, "body=%s", raw)

	var out recommendResponse
	require.NoError(t, json.Unmarshal(raw, &out))

	require.NotEmpty(t, out.Facts)
	require.NotEmpty(t, out.Recommendations, "recommendations[] must be non-empty")
	for _, r := range out.Recommendations {
		assert.True(t, validRecAreas[r.Area], "area %q outside enum", r.Area)
		assert.NotEmpty(t, r.Suggestion)
		assert.NotEmpty(t, r.Reasoning)
		assert.GreaterOrEqual(t, r.Confidence, 0.0)
		assert.LessOrEqual(t, r.Confidence, 1.0)
	}
	assertProviderInvariant(t, out.Provider, out.FallbackUsed)
}

// Unknown campaign → §4 404 envelope (resolved before any LLM work).
func TestCampaignAnalyzeNotFound(t *testing.T) {
	requireStack(t)
	status, raw := doJSON(t, http.MethodPost,
		"/campaigns/camp_it_no_such_"+runTag+"/analyze", nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, string(raw), "not_found")
}
