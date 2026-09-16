// Verifies CONTRACTS.md §5 (POST /audience/recommend): top-K candidates are
// rank-ordered with unique ranks, monotonically non-increasing scores and
// non-empty reasons; meta reports the selection funnel; respect_frequency_cap
// excludes customers already over the campaign's send cap; size=0 → 400.
package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type audienceCandidate struct {
	CustomerID string   `json:"customer_id"`
	Score      float64  `json:"score"`
	Rank       int      `json:"rank"`
	Reasons    []string `json:"reasons"`
}

type audienceResponse struct {
	Candidates []audienceCandidate `json:"candidates"`
	Meta       struct {
		CandidatesConsidered int   `json:"candidates_considered"`
		FilteredOut          int   `json:"filtered_out"`
		TookMs               int64 `json:"took_ms"`
	} `json:"meta"`
}

func postAudience(t *testing.T, req map[string]any) (int, audienceResponse) {
	t.Helper()
	status, raw := doJSON(t, http.MethodPost, "/audience/recommend", req)
	if status != http.StatusOK {
		return status, audienceResponse{}
	}
	var out audienceResponse
	require.NoError(t, json.Unmarshal(raw, &out))
	return status, out
}

// §5 response contract: ≤size candidates, ranks 1..N unique and sequential,
// scores monotonically non-increasing, reasons present, meta funnel fields.
func TestAudienceRecommendShape(t *testing.T) {
	requireStack(t)

	status, out := postAudience(t, map[string]any{
		"objective": "engagement", "channel": "email", "size": 50,
	})
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, out.Candidates)
	assert.LessOrEqual(t, len(out.Candidates), 50)

	seen := map[string]bool{}
	prev := 2.0 // above max display score
	for i, c := range out.Candidates {
		assert.Equal(t, i+1, c.Rank, "ranks must be 1..N sequential")
		assert.NotEmpty(t, c.CustomerID)
		assert.False(t, seen[c.CustomerID], "duplicate customer_id %s", c.CustomerID)
		seen[c.CustomerID] = true
		assert.LessOrEqual(t, c.Score, prev, "scores must be non-increasing")
		prev = c.Score
		assert.NotEmpty(t, c.Reasons, "candidate %d must carry reasons", c.Rank)
	}
	assert.GreaterOrEqual(t, out.Meta.CandidatesConsidered, len(out.Candidates))
	assert.Equal(t, out.Meta.CandidatesConsidered-len(out.Candidates), out.Meta.FilteredOut)
	assert.GreaterOrEqual(t, out.Meta.TookMs, int64(0))
}

// Request validation: size and enums are contract-bounded (§5 / openapi).
func TestAudienceRecommendValidation(t *testing.T) {
	requireStack(t)
	for name, req := range map[string]map[string]any{
		"size zero":       {"objective": "engagement", "channel": "email", "size": 0},
		"size too big":    {"objective": "engagement", "channel": "email", "size": 100001},
		"bad objective":   {"objective": "world-domination", "channel": "email", "size": 10},
		"bad channel":     {"objective": "engagement", "channel": "smoke-signals", "size": 10},
		"negative min":    {"objective": "engagement", "channel": "email", "size": 10, "conditions": map[string]any{"min_score": -1}},
	} {
		t.Run(name, func(t *testing.T) {
			status, raw := doJSON(t, http.MethodPost, "/audience/recommend", req)
			assert.Equal(t, http.StatusBadRequest, status, "body=%s", raw)
		})
	}
}

// §5 frequency cap: a customer whose sends in the campaign's window already
// meet the cap must be excluded when respect_frequency_cap is on, and must
// reappear when it is off — proving the drop is the cap, not ranking.
//
// The (channel, objective, cap, customer) tuple is discovered from the live
// data, mirroring the module's own cap-context rules (most relevant campaign
// for the pair, all sends in window count toward the cap).
func TestAudienceFrequencyCap(t *testing.T) {
	requireStack(t)

	var cust, channel, objective string
	var cap, windowHours int
	err := db.QueryRow(context.Background(), `
		WITH ctx AS (
			SELECT DISTINCT ON (channel, objective)
			       channel, objective, frequency_cap, frequency_window_hours
			FROM campaigns
			ORDER BY channel, objective,
			         CASE WHEN status='active' THEN 0 ELSE 1 END, created_at DESC
		)
		SELECT c.external_id, ctx.channel, ctx.objective,
		       ctx.frequency_cap, ctx.frequency_window_hours
		FROM sends s
		JOIN customers c ON c.id = s.customer_id
		JOIN engagement_profiles p ON p.customer_id = c.id
		CROSS JOIN ctx
		WHERE c.is_active
		  AND s.sent_at > now() - make_interval(hours => ctx.frequency_window_hours)
		GROUP BY c.external_id, ctx.channel, ctx.objective,
		         ctx.frequency_cap, ctx.frequency_window_hours, p.engagement_score
		HAVING count(*) >= ctx.frequency_cap
		ORDER BY p.engagement_score DESC
		LIMIT 1`,
	).Scan(&cust, &channel, &objective, &cap, &windowHours)
	require.NoError(t, err, "seed should contain an over-cap customer")
	t.Logf("over-cap target: %s (%s/%s cap=%d window=%dh)", cust, channel, objective, cap, windowHours)

	// Size 5000 comfortably includes any over-cap customer (the heaviest
	// over-cap profile sits well inside the top few thousand by score), and
	// the capped heap keeps 2*size survivors, so presence under cap=off
	// guarantees the customer was a cap-checkable survivor under cap=on.
	req := map[string]any{
		"objective": objective, "channel": channel, "size": 5000,
		"conditions": map[string]any{"respect_frequency_cap": false},
	}
	_, uncapped := postAudience(t, req)
	found := false
	for _, c := range uncapped.Candidates {
		if c.CustomerID == cust {
			found = true
			break
		}
	}
	require.True(t, found, "over-cap customer %s must rank when cap is off", cust)

	req["conditions"] = map[string]any{"respect_frequency_cap": true}
	_, capped := postAudience(t, req)
	for _, c := range capped.Candidates {
		assert.NotEqual(t, cust, c.CustomerID,
			"customer over frequency cap must be excluded")
	}
}
