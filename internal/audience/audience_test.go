package audience

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// referenceTopK is the O(N log N) full-sort oracle the heap must match.
func referenceTopK(in []candidate, k int) []candidate {
	s := append([]candidate(nil), in...)
	sort.Slice(s, func(i, j int) bool { return better(s[i], s[j]) })
	if k < len(s) {
		s = s[:k]
	}
	return s
}

// Property-style: the heap must return exactly the same set, in the same
// order, as a full sort — including tie-breaking on customerID.
func TestTopKHeap_MatchesReferenceSort(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const N, K = 1000, 50
	cands := make([]candidate, N)
	for i := range cands {
		cands[i] = candidate{customerID: int64(i + 1), weighted: rng.Float64() * 100}
	}
	// Collide some scores to exercise the deterministic tie-break.
	for i := 0; i < 100; i++ {
		cands[i].weighted = 42
	}

	th := newTopKHeap(K)
	for _, c := range cands {
		th.Add(c)
	}

	require.Equal(t, referenceTopK(cands, K), th.Sorted())
}

func TestTopKHeap_EdgeCases(t *testing.T) {
	mk := func(ids ...int64) []candidate {
		out := make([]candidate, len(ids))
		for i, id := range ids {
			out[i] = candidate{customerID: id, weighted: float64(id)}
		}
		return out
	}

	t.Run("K greater than N returns all sorted", func(t *testing.T) {
		in := mk(3, 1, 2)
		th := newTopKHeap(10)
		for _, c := range in {
			th.Add(c)
		}
		require.Equal(t, referenceTopK(in, 10), th.Sorted())
	})

	t.Run("empty input", func(t *testing.T) {
		th := newTopKHeap(5)
		assert.Empty(t, th.Sorted())
	})

	t.Run("K zero and negative keep nothing", func(t *testing.T) {
		for _, k := range []int{0, -3} {
			th := newTopKHeap(k)
			for _, c := range mk(1, 2, 3) {
				th.Add(c)
			}
			assert.Empty(t, th.Sorted())
		}
	})

	t.Run("K one keeps the single best", func(t *testing.T) {
		th := newTopKHeap(1)
		for _, c := range mk(1, 9, 4) {
			th.Add(c)
		}
		got := th.Sorted()
		require.Len(t, got, 1)
		assert.Equal(t, int64(9), got[0].customerID)
	})

	t.Run("equal scores order by customerID", func(t *testing.T) {
		th := newTopKHeap(3)
		for _, c := range mk(5, 2, 8, 1) { // all weighted == id? make equal
			c.weighted = 7
			th.Add(c)
		}
		got := th.Sorted()
		require.Len(t, got, 3)
		assert.Equal(t, []int64{1, 2, 5}, []int64{got[0].customerID, got[1].customerID, got[2].customerID})
	})
}

func TestDedupByCustomer(t *testing.T) {
	in := []candidate{
		{customerID: 1, weighted: 9},
		{customerID: 2, weighted: 8},
		{customerID: 1, weighted: 7}, // dup — first occurrence wins
		{customerID: 3, weighted: 6},
	}
	got := dedupByCustomer(in)
	require.Len(t, got, 3)
	assert.Equal(t, []int64{1, 2, 3}, []int64{got[0].customerID, got[1].customerID, got[2].customerID})
	assert.Equal(t, 9.0, got[0].weighted) // kept the higher-ranked copy

	assert.Empty(t, dedupByCustomer(nil))
}

func TestWeightedScore_ObjectiveReweight(t *testing.T) {
	now := time.Now()
	dormant := now.Add(-60 * 24 * time.Hour)
	recent := now.Add(-24 * time.Hour)

	highConv := candidate{customerID: 1, rawScore: 1.0, conversions: 10, lastEventAt: &recent}
	highBase := candidate{customerID: 2, rawScore: 1.2, lastEventAt: &recent}

	t.Run("conversion boosts conversions and can flip ranking", func(t *testing.T) {
		assert.Greater(t,
			weightedScore("conversion", highConv, now),
			weightedScore("conversion", highBase, now))
	})

	t.Run("awareness is balanced — raw score order preserved", func(t *testing.T) {
		assert.Greater(t,
			weightedScore("awareness", highBase, now),
			weightedScore("awareness", highConv, now))
	})

	t.Run("engagement boosts positive events", func(t *testing.T) {
		talkative := candidate{customerID: 3, rawScore: 1.0, positiveEvents: 30, lastEventAt: &recent}
		assert.Greater(t,
			weightedScore("engagement", talkative, now),
			weightedScore("engagement", highBase, now))
	})

	t.Run("reactivation rewards dormant customers with history", func(t *testing.T) {
		sleeper := candidate{customerID: 4, rawScore: 1.0, conversions: 3, lastEventAt: &dormant}
		assert.Greater(t,
			weightedScore("reactivation", sleeper, now),
			weightedScore("reactivation", highBase, now))
	})

	t.Run("unknown objective falls back to raw score", func(t *testing.T) {
		assert.Equal(t, 1.2, weightedScore("bogus", highBase, now))
	})

	t.Run("nil lastEventAt gets no dormancy boost", func(t *testing.T) {
		neverActive := candidate{customerID: 5, rawScore: 1.0}
		assert.Equal(t, 1.0, weightedScore("reactivation", neverActive, now))
	})
}

func TestDisplayScore(t *testing.T) {
	assert.Equal(t, 0.0, displayScore(0))
	assert.Equal(t, 0.0, displayScore(-2))
	assert.InDelta(t, 0.5, displayScore(1), 1e-9)
	// Monotone: preserves ranking.
	assert.Greater(t, displayScore(9), displayScore(3))
	assert.Less(t, displayScore(100), 1.0)
}

func TestBuildReasons(t *testing.T) {
	now := time.Now()
	recent := now.Add(-2 * 24 * time.Hour)
	old := now.Add(-45 * 24 * time.Hour)

	base := candidate{
		customerID: 1, rawScore: 4.9, weighted: 4.9,
		preferredChannel: "email", conversions: 2, positiveEvents: 12,
		totalEvents: 40, lastEventAt: &recent, trend: "rising",
	}
	req := &RecommendRequest{Objective: "conversion", Channel: "email", Size: 10}

	t.Run("produces 2-4 data-backed reasons", func(t *testing.T) {
		got := buildReasons(base, req, 1, 100, now)
		require.GreaterOrEqual(t, len(got), 2)
		require.LessOrEqual(t, len(got), 4)
		assert.Contains(t, got[0], "top-quartile")
		assert.Contains(t, got, "preferred_channel=email matches")
		assert.Contains(t, got, "converted 2x")
		assert.Contains(t, got, "active 2d ago")
	})

	t.Run("no quartile claim when rank is below top 25%", func(t *testing.T) {
		got := buildReasons(base, req, 80, 100, now)
		assert.NotContains(t, got[0], "top-quartile")
		assert.Contains(t, got[0], "score=")
	})

	t.Run("channel mismatch omits the match reason", func(t *testing.T) {
		smsReq := &RecommendRequest{Objective: "conversion", Channel: "sms", Size: 10}
		got := buildReasons(base, smsReq, 1, 100, now)
		for _, r := range got {
			assert.NotContains(t, r, "matches")
		}
	})

	t.Run("reactivation cites dormancy", func(t *testing.T) {
		sleeper := base
		sleeper.lastEventAt = &old
		reactReq := &RecommendRequest{Objective: "reactivation", Channel: "email", Size: 10}
		got := buildReasons(sleeper, reactReq, 1, 100, now)
		assert.Contains(t, got, "dormant 45d with 40 events history")
	})

	t.Run("sparse candidate still reaches 2 reasons", func(t *testing.T) {
		bare := candidate{customerID: 9, weighted: 0.4}
		got := buildReasons(bare, req, 3, 10, now)
		require.GreaterOrEqual(t, len(got), 2)
	})
}

func TestValidate(t *testing.T) {
	ok := RecommendRequest{Objective: "conversion", Channel: "email", Size: 100}
	require.NoError(t, ok.validate())

	cases := []struct {
		name string
		mut  func(*RecommendRequest)
	}{
		{"bad objective", func(r *RecommendRequest) { r.Objective = "spam" }},
		{"empty objective", func(r *RecommendRequest) { r.Objective = "" }},
		{"bad channel", func(r *RecommendRequest) { r.Channel = "pigeon" }},
		{"size zero", func(r *RecommendRequest) { r.Size = 0 }},
		{"size too big", func(r *RecommendRequest) { r.Size = 100001 }},
		{"negative min_score", func(r *RecommendRequest) { r.Conditions.MinScore = -0.1 }},
		{"negative last_active_days", func(r *RecommendRequest) { r.Conditions.LastActiveDays = -1 }},
		{"bad condition channel", func(r *RecommendRequest) { r.Conditions.Channels = []string{"email", "fax"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := ok
			tc.mut(&r)
			assert.Error(t, r.validate())
		})
	}
}

func TestRespectCapDefault(t *testing.T) {
	var req RecommendRequest
	assert.True(t, req.respectCap(), "absent flag defaults to true (openapi default)")

	f := false
	req.Conditions.RespectFrequencyCap = &f
	assert.False(t, req.respectCap())

	tr := true
	req.Conditions.RespectFrequencyCap = &tr
	assert.True(t, req.respectCap())
}
