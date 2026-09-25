package predict

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Beta sampler is the base of every interval and P(best); check its
// mean for common shapes and for shape < 1 (rare events under a small cap).
func TestSampleBetaMean(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, tc := range []struct{ a, b float64 }{
		{2, 8}, {50, 950}, {0.6, 29.4}, {0.3, 0.7},
	} {
		const n = 60000
		var sum float64
		for range n {
			x := sampleBeta(rng, tc.a, tc.b)
			require.True(t, x >= 0 && x <= 1, "draw out of [0,1]: %v", x)
			sum += x
		}
		want := tc.a / (tc.a + tc.b)
		assert.InDelta(t, want, sum/n, 0.01, "Beta(%v,%v) mean", tc.a, tc.b)
	}
}

func evidence(ch string, objS, objN, otherS, otherN int64) ChannelEvidence {
	return ChannelEvidence{
		Channel:   ch,
		Objective: Counts{Successes: objS, Delivered: objN},
		OtherObj:  Counts{Successes: otherS, Delivered: otherN},
	}
}

// Shrinkage: a channel with no history lands on the platform mean; a
// channel with lots of history lands on its own rate.
func TestPosteriorsShrinkage(t *testing.T) {
	ev := []ChannelEvidence{
		evidence("email", 4000, 100000, 3000, 100000), // 4% on this objective
		evidence("push", 0, 0, 0, 0),                  // nothing at all
	}
	posts := Posteriors(ev, "converted", ScopePlatform)
	mg := platformMean(ev, "converted") // 7000 / 200000 = 3.5%

	assert.InDelta(t, 0.04, posts[0].Mean(), 0.001, "data-rich channel ≈ its own rate")
	assert.InDelta(t, mg, posts[1].Mean(), 1e-9, "no-data channel = platform mean")
	// No data → wide belief; lots of data → narrow belief.
	assert.Less(t, posts[1].A+posts[1].B, 200.0)
	assert.Greater(t, posts[0].A+posts[0].B, 100000.0)
}

// Layer 1 borrows strength from other objectives when this objective has
// no history on a channel.
func TestPosteriorsBorrowFromOtherObjectives(t *testing.T) {
	ev := []ChannelEvidence{
		evidence("sms", 0, 0, 900, 10000), // 9% on other objectives
		evidence("email", 200, 10000, 200, 10000),
	}
	posts := Posteriors(ev, "converted", ScopePlatform)
	assert.Greater(t, posts[0].Mean(), 0.07, "sms estimate should lean on its 9% history elsewhere")
}

func TestPlatformMeanEmptySystemUsesDefault(t *testing.T) {
	assert.Equal(t, 0.20, platformMean([]ChannelEvidence{{Channel: "email"}}, "opened"))
	assert.Equal(t, 0.02, platformMean(nil, "converted"))
}

// Successes > delivered (missing delivered events) must not produce a
// negative Beta shape.
func TestCountsClamp(t *testing.T) {
	ev := []ChannelEvidence{evidence("web", 50, 10, 0, 0)}
	p := Posteriors(ev, "clicked", ScopePlatform)[0]
	assert.Greater(t, p.B, 0.0)
	assert.LessOrEqual(t, p.Mean(), 1.0)
}

func TestClearWinnerIsHighConfidence(t *testing.T) {
	ev := []ChannelEvidence{
		evidence("email", 5000, 100000, 0, 0), // 5%
		evidence("sms", 1000, 100000, 0, 0),   // 1%
		evidence("push", 500, 100000, 0, 0),   // 0.5%
	}
	resp := buildResponse(Request{Objective: "conversion", LookbackDays: 90}, ScopePlatform, "converted", ev)
	assert.Equal(t, "email", resp.RecommendedChannel)
	assert.Equal(t, "high", resp.Confidence)
	assert.Greater(t, resp.Channels[0].ProbBest, 0.99)
	for i, c := range resp.Channels {
		assert.Equal(t, i+1, c.Rank)
		assert.LessOrEqual(t, c.Interval90[0], c.PredictedRate)
		assert.GreaterOrEqual(t, c.Interval90[1], c.PredictedRate)
		assert.NotEmpty(t, c.Reasons)
	}
	var sum float64
	for _, c := range resp.Channels {
		sum += c.ProbBest
	}
	assert.InDelta(t, 1.0, sum, 0.001, "P(best) sums to 1 across channels")
}

// Two channels with similar rates on thin data: the honest answer is "we
// don't know yet — test it", not a confident pick.
func TestCloseRaceIsLowConfidenceWithABAdvice(t *testing.T) {
	ev := []ChannelEvidence{
		evidence("email", 6, 150, 0, 0),
		evidence("whatsapp", 5, 140, 0, 0),
	}
	resp := buildResponse(Request{Objective: "conversion", LookbackDays: 90}, ScopePlatform, "converted", ev)
	assert.Equal(t, "low", resp.Confidence)
	assert.Contains(t, resp.Advice, "A/B")
	assert.True(t, resp.Channels[0].LowData)
}

// Layer 3: one customer's strong SMS history flips the platform answer.
func TestCustomerHistoryMovesPrediction(t *testing.T) {
	platform := []ChannelEvidence{
		evidence("email", 3000, 100000, 0, 0), // 3%
		evidence("sms", 1000, 100000, 0, 0),   // 1%
	}
	base := buildResponse(Request{Objective: "conversion", LookbackDays: 90}, ScopePlatform, "converted", platform)
	require.Equal(t, "email", base.RecommendedChannel)

	cust := []ChannelEvidence{platform[0], platform[1]}
	cust[0].Target = Counts{Successes: 0, Delivered: 40} // ignores email
	cust[1].Target = Counts{Successes: 8, Delivered: 20} // converts on SMS
	resp := buildResponse(Request{Objective: "conversion", CustomerID: "cust_1", LookbackDays: 90}, ScopeCustomer, "converted", cust)
	assert.Equal(t, "sms", resp.RecommendedChannel)
	require.NotNil(t, resp.Channels[0].Evidence.TargetDelivered)
}

// The same inputs always give the same answer (fixed RNG seed): cacheable
// and testable.
func TestSimulateDeterministic(t *testing.T) {
	posts := []Posterior{{"a", 5, 95}, {"b", 6, 94}}
	assert.Equal(t, Simulate(posts), Simulate(posts))
}

func TestRiskFlags(t *testing.T) {
	ev := []ChannelEvidence{
		evidence("email", 3000, 100000, 0, 0),
		evidence("sms", 3100, 100000, 0, 0),
	}
	ev[0].DeliveredAll, ev[0].OptOuts, ev[0].Sent, ev[0].Bounced = 100000, 200, 101000, 1000
	ev[1].DeliveredAll, ev[1].OptOuts, ev[1].Sent, ev[1].Bounced = 100000, 3000, 101000, 1000
	resp := buildResponse(Request{Objective: "conversion", LookbackDays: 90}, ScopePlatform, "converted", ev)
	for _, c := range resp.Channels {
		joined := strings.Join(c.Reasons, " | ")
		if c.Channel == "sms" {
			assert.Contains(t, joined, "high opt-out risk")
		} else {
			assert.NotContains(t, joined, "high opt-out risk")
		}
	}
}

func TestValidate(t *testing.T) {
	req := Request{Objective: "conversion", Channels: []string{"email", "sms", "email"}}
	chs, errs := validate(&req)
	assert.Empty(t, errs)
	assert.Equal(t, []string{"email", "sms"}, chs, "deduped, order kept")
	assert.Equal(t, defaultLookbackDays, req.LookbackDays)

	req = Request{Objective: "conversion"}
	chs, _ = validate(&req)
	assert.Equal(t, allChannels, chs, "defaults to every channel")

	for name, r := range map[string]Request{
		"objective":          {Objective: "fame"},
		"channels":           {Objective: "conversion", Channels: []string{"fax"}},
		"customer_id":        {Objective: "conversion", CustomerID: "c", Audience: &AudienceFilter{}},
		"audience.min_score": {Objective: "conversion", Audience: &AudienceFilter{MinScore: 1}},
		"lookback_days":      {Objective: "conversion", LookbackDays: 400},
	} {
		_, errs := validate(&r)
		assert.Contains(t, errs, name, name)
	}
}

func TestConfidenceBands(t *testing.T) {
	assert.Equal(t, "high", Confidence(0.95))
	assert.Equal(t, "medium", Confidence(0.7))
	assert.Equal(t, "low", Confidence(0.5))
}

func TestFormatting(t *testing.T) {
	assert.Equal(t, "1,234,567", thousands(1234567))
	assert.Equal(t, "999", thousands(999))
	assert.Equal(t, "4.1%", pct(0.0412))
	assert.Equal(t, 0.0412, round4(0.041249))
	assert.True(t, math.IsInf(-rawMinScore(0), 1) || rawMinScore(0) < -1e300)
	assert.InDelta(t, 2.5, rawMinScore(0.2), 1e-9)
}

// Customer scope: a channel the customer was never reached on must not be
// more certain than one they were — the cap applies to every channel.
func TestCustomerScopeCapsUntriedChannels(t *testing.T) {
	ev := []ChannelEvidence{
		evidence("email", 3000, 100000, 0, 0),
		evidence("sms", 1000, 100000, 0, 0),
	}
	ev[1].Target = Counts{Successes: 1, Delivered: 5}
	posts := Posteriors(ev, "converted", ScopeCustomer)
	assert.LessOrEqual(t, posts[0].A+posts[0].B, customerCapK+1e-9, "untried channel capped")
	assert.InDelta(t, 0.03, posts[0].Mean(), 0.001, "…but keeps the platform mean")
}

// Low-confidence advice names the channel most likely to beat the leader,
// even when that is a thin-data channel ranked lower by mean.
func TestAdviceNamesStrongestChallenger(t *testing.T) {
	preds := []ChannelPrediction{
		{Channel: "email", ProbBest: 0.30},
		{Channel: "whatsapp", ProbBest: 0.20},
		{Channel: "push", ProbBest: 0.45},
		{Channel: "sms", ProbBest: 0.05},
	}
	a := advice(preds, "low", "clicked", ScopeAudience)
	assert.Contains(t, a, "email and push")
	assert.NotContains(t, advice(preds, "low", "clicked", ScopeCustomer), "A/B",
		"an A/B split is meaningless for one customer")
}
