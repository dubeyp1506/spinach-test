// Verifies POST /api/v1/predictions/channel for all three scopes against
// the seeded dataset: ranking and probability invariants, scope-specific
// evidence, and request validation.
package tests

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type channelPrediction struct {
	Rank          int        `json:"rank"`
	Channel       string     `json:"channel"`
	PredictedRate float64    `json:"predicted_rate"`
	Interval90    [2]float64 `json:"interval_90"`
	ProbBest      float64    `json:"prob_best"`
	Evidence      struct {
		TargetDelivered *int64 `json:"target_delivered"`
	} `json:"evidence"`
	Reasons []string `json:"reasons"`
}

type predictionResponse struct {
	Objective          string              `json:"objective"`
	Scope              string              `json:"scope"`
	SuccessEvent       string              `json:"success_event"`
	RecommendedChannel string              `json:"recommended_channel"`
	Confidence         string              `json:"confidence"`
	Advice             string              `json:"advice"`
	Channels           []channelPrediction `json:"channels"`
	Meta               struct {
		LookbackDays int    `json:"lookback_days"`
		AudienceSize *int64 `json:"audience_size"`
		Model        string `json:"model"`
	} `json:"meta"`
}

func postPrediction(t *testing.T, body map[string]any) (int, predictionResponse) {
	t.Helper()
	status, raw := doJSON(t, http.MethodPost, "/predictions/channel", body)
	var out predictionResponse
	if status == http.StatusOK {
		require.NoError(t, json.Unmarshal(raw, &out), string(raw))
	}
	return status, out
}

func assertPredictionInvariants(t *testing.T, out predictionResponse, wantChannels int) {
	t.Helper()
	require.Len(t, out.Channels, wantChannels)
	assert.Equal(t, out.Channels[0].Channel, out.RecommendedChannel)
	assert.Contains(t, []string{"high", "medium", "low"}, out.Confidence)
	assert.NotEmpty(t, out.Advice)
	var sum float64
	for i, c := range out.Channels {
		assert.Equal(t, i+1, c.Rank)
		if i > 0 {
			assert.LessOrEqual(t, c.PredictedRate, out.Channels[i-1].PredictedRate, "ranked by rate")
		}
		assert.LessOrEqual(t, c.Interval90[0], c.PredictedRate)
		assert.GreaterOrEqual(t, c.Interval90[1], c.PredictedRate)
		assert.NotEmpty(t, c.Reasons)
		sum += c.ProbBest
	}
	assert.InDelta(t, 1.0, sum, 0.01, "P(best) sums to 1")
}

func TestPredictChannelPlatform(t *testing.T) {
	requireStack(t)
	status, out := postPrediction(t, map[string]any{"objective": "conversion"})
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "platform", out.Scope)
	assert.Equal(t, "converted", out.SuccessEvent)
	assert.Equal(t, 90, out.Meta.LookbackDays)
	assert.NotEmpty(t, out.Meta.Model)
	assertPredictionInvariants(t, out, 5)
	assert.Nil(t, out.Channels[0].Evidence.TargetDelivered, "no target evidence at platform scope")

	// Restricting candidates restricts the answer.
	status, out = postPrediction(t, map[string]any{"objective": "engagement", "channels": []string{"sms", "push"}})
	require.Equal(t, http.StatusOK, status)
	assertPredictionInvariants(t, out, 2)
	assert.Contains(t, []string{"sms", "push"}, out.RecommendedChannel)
}

func TestPredictChannelAudienceAndCustomer(t *testing.T) {
	requireStack(t)

	status, out := postPrediction(t, map[string]any{
		"objective": "retention",
		"audience":  map[string]any{"min_score": 0.1, "last_active_days": 60},
	})
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "audience", out.Scope)
	require.NotNil(t, out.Meta.AudienceSize)
	assertPredictionInvariants(t, out, 5)

	cust := pickHighActivityCustomer(t)
	status, out = postPrediction(t, map[string]any{"objective": "conversion", "customer_id": cust})
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "customer", out.Scope)
	assertPredictionInvariants(t, out, 5)
	require.NotNil(t, out.Channels[0].Evidence.TargetDelivered, "customer scope reports target evidence")

	status, _ = postPrediction(t, map[string]any{"objective": "conversion", "customer_id": "cust_nobody_" + runTag})
	assert.Equal(t, http.StatusNotFound, status)
}

func TestPredictChannelValidation(t *testing.T) {
	requireStack(t)
	for name, body := range map[string]map[string]any{
		"bad objective": {"objective": "world-domination"},
		"bad channel":   {"objective": "conversion", "channels": []string{"pigeon"}},
		"both scopes":   {"objective": "conversion", "customer_id": "x", "audience": map[string]any{}},
		"bad min_score": {"objective": "conversion", "audience": map[string]any{"min_score": 1.5}},
		"bad lookback":  {"objective": "conversion", "lookback_days": 1000},
	} {
		status, _ := postPrediction(t, body)
		assert.Equal(t, http.StatusBadRequest, status, name)
	}
	status, _ := doJSON(t, http.MethodPost, "/predictions/channel", "{not json")
	assert.Equal(t, http.StatusBadRequest, status)
}
