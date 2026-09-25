// Package predict answers "which channel will work best for this upcoming
// campaign?" — for the whole platform, a target audience, or one customer
// (POST /api/v1/predictions/channel).
//
// Model: hierarchical Beta-Binomial (empirical Bayes). For every channel the
// success rate for the objective's success event (conversion → converted,
// engagement → clicked, retention/reactivation/awareness → opened) per
// delivered message is estimated in three layers, each shrinking toward the
// one above so thin data can't produce a confident answer:
//
//  1. channel prior   — the channel's rate on OTHER objectives, shrunk toward
//     the platform-wide rate (strength alpha0 pseudo-deliveries)
//  2. objective layer — prior(1) with strength alpha1, updated with this
//     objective's successes/deliveries on the channel (last lookback days)
//  3. target layer    — the audience's or customer's own campaign responses
//     (campaign-attributed events, same window), with layer 2 as prior
//     capped at K pseudo-deliveries so their behaviour can move the estimate
//
// The answer is not just "highest mean wins": Monte Carlo draws from each
// channel's posterior give P(channel is best), and the confidence label and
// A/B-test advice come from that probability.
//
// Everything in this file is pure (no DB, deterministic RNG seed) and
// unit-tested in model_test.go.
package predict

import (
	"math"
	"math/rand/v2"
	"slices"
	"sort"
)

// Hyperparameters, in pseudo-deliveries. Larger = more trust in the layer
// above. Chosen so a channel needs a few hundred of its own deliveries
// before its data dominates the prior.
const (
	alpha0        = 100.0 // channel-general rate ← platform rate
	alpha1        = 50.0  // objective rate ← channel-general rate
	audienceCapK  = 500.0 // cap on layer-2 strength when adding audience data
	customerCapK  = 30.0  // cap when adding one customer's data
	lowDataBelow  = 200   // deliveries of direct evidence below which low_data=true
	samples       = 4000  // Monte Carlo draws per channel
	credibleLower = 0.05  // 90% credible interval
	credibleUpper = 0.95
	rngSeed1      = 0x6d61727465636821 // fixed: same data → same answer
	rngSeed2      = 0x7072656469637421
)

// successEvent maps a campaign objective to the event type that counts as
// success. Denominator is always delivered.
var successEvent = map[string]string{
	"conversion":   "converted",
	"engagement":   "clicked",
	"retention":    "opened",
	"reactivation": "opened",
	"awareness":    "opened",
}

// defaultRate is the platform prior when there is no data at all, per
// success event — typical industry magnitudes, only used on an empty system.
var defaultRate = map[string]float64{
	"converted": 0.02,
	"clicked":   0.03,
	"opened":    0.20,
}

// Counts is successes out of deliveries for one slice of history.
type Counts struct {
	Successes int64
	Delivered int64
}

func (c Counts) rate() float64 {
	if c.Delivered <= 0 {
		return 0
	}
	return float64(c.Successes) / float64(c.Delivered)
}

// clamp keeps counts consistent: successes can't exceed deliveries (late or
// missing delivered events would otherwise give rates > 1).
func (c Counts) clamp() Counts {
	if c.Successes < 0 {
		c.Successes = 0
	}
	if c.Delivered < c.Successes {
		c.Delivered = c.Successes
	}
	return c
}

// ChannelEvidence is everything known about one channel.
type ChannelEvidence struct {
	Channel      string
	Objective    Counts // this objective, platform-wide, lookback window
	OtherObj     Counts // other objectives, platform-wide, lookback window
	Target       Counts // audience or customer history (zero when scope=platform)
	Sent         int64  // risk inputs, all objectives, lookback window
	Bounced      int64
	OptOuts      int64 // unsubscribed + complained
	DeliveredAll int64
}

// Scope says which layer-3 evidence applies.
type Scope string

const (
	ScopePlatform Scope = "platform"
	ScopeAudience Scope = "audience"
	ScopeCustomer Scope = "customer"
)

// Posterior is a Beta(A, B) belief about one channel's success rate.
type Posterior struct {
	Channel string
	A, B    float64
}

// Mean is the posterior expected success rate.
func (p Posterior) Mean() float64 { return p.A / (p.A + p.B) }

// platformMean is the pooled rate across all channels and objectives, the
// top of the hierarchy.
func platformMean(ev []ChannelEvidence, success string) float64 {
	var s, n int64
	for _, e := range ev {
		o, x := e.Objective.clamp(), e.OtherObj.clamp()
		s += o.Successes + x.Successes
		n += o.Delivered + x.Delivered
	}
	if n == 0 {
		if r, ok := defaultRate[success]; ok {
			return r
		}
		return 0.02
	}
	// Keep the prior strictly inside (0,1) so Beta shapes stay positive.
	return math.Min(math.Max(float64(s)/float64(n), 1e-4), 1-1e-4)
}

// Posteriors runs the three-layer update for every channel.
func Posteriors(ev []ChannelEvidence, success string, scope Scope) []Posterior {
	mg := platformMean(ev, success)
	out := make([]Posterior, 0, len(ev))
	for _, e := range ev {
		o, x, t := e.Objective.clamp(), e.OtherObj.clamp(), e.Target.clamp()

		// Layer 1: channel-general mean shrunk toward the platform.
		mc := (float64(x.Successes) + alpha0*mg) / (float64(x.Delivered) + alpha0)

		// Layer 2: objective-specific counts on top of Beta(alpha1·mc, alpha1·(1−mc)).
		a := alpha1*mc + float64(o.Successes)
		b := alpha1*(1-mc) + float64(o.Delivered-o.Successes)

		// Layer 3: target history, with layer 2 re-expressed as a prior of
		// capped strength. Without the cap, 100k platform deliveries would
		// drown a customer's 40 deliveries and every customer would get the
		// platform answer. The cap applies even when the target has no
		// history on a channel: the question is about THIS target, so an
		// untried channel must not look more certain than a tried one.
		if scope != ScopePlatform {
			k := audienceCapK
			if scope == ScopeCustomer {
				k = customerCapK
			}
			strength := math.Min(a+b, k)
			mean := a / (a + b)
			a = strength*mean + float64(t.Successes)
			b = strength*(1-mean) + float64(t.Delivered-t.Successes)
		}
		out = append(out, Posterior{Channel: e.Channel, A: a, B: b})
	}
	return out
}

// Simulation is the Monte Carlo summary for one channel.
type Simulation struct {
	Channel  string
	Mean     float64
	Lower    float64 // 5th percentile
	Upper    float64 // 95th percentile
	ProbBest float64
}

// Simulate draws from every posterior and reports credible intervals and
// the probability each channel has the highest true rate. Ties between
// draws are broken by input order (never happens with continuous draws in
// practice). Deterministic: fixed seed.
func Simulate(posts []Posterior) []Simulation {
	rng := rand.New(rand.NewPCG(rngSeed1, rngSeed2))
	draws := make([][]float64, len(posts))
	for i, p := range posts {
		draws[i] = make([]float64, samples)
		for j := range samples {
			draws[i][j] = sampleBeta(rng, p.A, p.B)
		}
	}
	wins := make([]int, len(posts))
	for j := range samples {
		best := 0
		for i := 1; i < len(posts); i++ {
			if draws[i][j] > draws[best][j] {
				best = i
			}
		}
		wins[best]++
	}
	out := make([]Simulation, len(posts))
	for i, p := range posts {
		sorted := slices.Clone(draws[i])
		sort.Float64s(sorted)
		out[i] = Simulation{
			Channel:  p.Channel,
			Mean:     p.Mean(),
			Lower:    quantile(sorted, credibleLower),
			Upper:    quantile(sorted, credibleUpper),
			ProbBest: float64(wins[i]) / samples,
		}
	}
	return out
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Round(q * float64(len(sorted)-1)))
	return sorted[idx]
}

// sampleBeta draws Beta(a, b) as X/(X+Y) with X~Gamma(a), Y~Gamma(b).
func sampleBeta(rng *rand.Rand, a, b float64) float64 {
	x := sampleGamma(rng, a)
	y := sampleGamma(rng, b)
	if x+y == 0 {
		return a / (a + b)
	}
	return x / (x + y)
}

// sampleGamma draws Gamma(shape, 1) with Marsaglia & Tsang (2000); shapes
// below 1 use the boost Gamma(k) = Gamma(k+1)·U^(1/k). Tiny shapes occur for
// rare events (a 2% rate under a 30-delivery customer cap → shape 0.6).
func sampleGamma(rng *rand.Rand, shape float64) float64 {
	if shape <= 0 {
		return 0
	}
	if shape < 1 {
		u := rng.Float64()
		for u == 0 {
			u = rng.Float64()
		}
		return sampleGamma(rng, shape+1) * math.Pow(u, 1/shape)
	}
	d := shape - 1.0/3
	c := 1 / math.Sqrt(9*d)
	for {
		x := rng.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := rng.Float64()
		if u < 1-0.0331*x*x*x*x {
			return d * v
		}
		if math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}

// Confidence turns the winner's P(best) into a label a marketer can act on.
func Confidence(probBest float64) string {
	switch {
	case probBest >= 0.90:
		return "high"
	case probBest >= 0.65:
		return "medium"
	default:
		return "low"
	}
}
