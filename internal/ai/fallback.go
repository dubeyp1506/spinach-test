package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// FallbackProviderName identifies the deterministic last link of the chain.
const FallbackProviderName = "rule-based-fallback"

// RuleBasedProvider is the never-failing last link of the chain (CONTRACTS §3).
// It produces the same output schema as the LLM providers using threshold
// rules on the facts alone — no model, no network, no invented statistics.
type RuleBasedProvider struct{}

func NewRuleBasedProvider() *RuleBasedProvider { return &RuleBasedProvider{} }

func (p *RuleBasedProvider) Name() string { return FallbackProviderName }

// Complete recovers the embedded facts block from the prompt and emits the
// deterministic result as JSON, dispatching on the TASK marker.
func (p *RuleBasedProvider) Complete(ctx context.Context, prompt string) (string, error) {
	factsJSON, err := extractFactsJSON(prompt)
	if err != nil {
		return "", err
	}
	var f Facts
	if err := json.Unmarshal(factsJSON, &f); err != nil {
		return "", fmt.Errorf("ai: fallback facts decode: %w", err)
	}
	if isRecommendTask(prompt) {
		out, _ := json.Marshal(struct {
			Recommendations []Recommendation `json:"recommendations"`
		}{Recommendations: p.Recommend(&f, f.Objective)})
		return string(out), nil
	}
	out, _ := json.Marshal(p.Analyze(&f))
	return string(out), nil
}

// fnum formats a float minimally ("21.5", "20", "0.5") so cited values match
// the numbers serialized in the facts JSON.
func fnum(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// Analyze applies threshold rules on rates; every cited number is a field of
// the facts object so the output always passes number-containment validation.
func (p *RuleBasedProvider) Analyze(f *Facts) *Analysis {
	a := &Analysis{
		Strengths:  []string{},
		Weaknesses: []string{},
		Anomalies:  []string{},
	}
	b := f.Benchmarks

	if f.Sends == 0 {
		a.Summary = "The campaign has no recorded events yet."
		a.Weaknesses = append(a.Weaknesses, fmt.Sprintf(
			"Insufficient data: %d sends recorded; at least %d are required for meaningful analysis.",
			f.Sends, f.MinSampleSends))
		a.Confidence = 0.1
		return a
	}

	a.Summary = fmt.Sprintf(
		"The campaign recorded %d sends and %d deliveries, with %s%% open, %s%% click and %s%% conversion rates.",
		f.Sends, f.Delivered, fnum(f.OpenRatePct), fnum(f.ClickRatePct), fnum(f.ConversionRatePct))

	if f.Delivered == 0 {
		a.Anomalies = append(a.Anomalies, fmt.Sprintf(
			"%d sends produced %d deliveries.", f.Sends, f.Delivered))
	}
	if f.Clicks > f.Opens {
		a.Anomalies = append(a.Anomalies, fmt.Sprintf(
			"%d clicks exceed %d opens, indicating inconsistent tracking.", f.Clicks, f.Opens))
	}
	if f.BounceRatePct > b.BounceRatePctBad {
		a.Anomalies = append(a.Anomalies, fmt.Sprintf(
			"Bounce rate %s%% exceeds the %s%% threshold, suggesting list-quality issues.",
			fnum(f.BounceRatePct), fnum(b.BounceRatePctBad)))
	}

	if f.OpenRatePct >= b.OpenRatePctGood {
		a.Strengths = append(a.Strengths, fmt.Sprintf(
			"Open rate %s%% meets or beats the %s%% benchmark.",
			fnum(f.OpenRatePct), fnum(b.OpenRatePctGood)))
	} else {
		a.Weaknesses = append(a.Weaknesses, fmt.Sprintf(
			"Open rate %s%% is below the %s%% benchmark.",
			fnum(f.OpenRatePct), fnum(b.OpenRatePctGood)))
	}
	if f.ClickRatePct >= b.ClickRatePctGood {
		a.Strengths = append(a.Strengths, fmt.Sprintf(
			"Click rate %s%% meets or beats the %s%% benchmark.",
			fnum(f.ClickRatePct), fnum(b.ClickRatePctGood)))
	} else {
		a.Weaknesses = append(a.Weaknesses, fmt.Sprintf(
			"Click rate %s%% is below the %s%% benchmark.",
			fnum(f.ClickRatePct), fnum(b.ClickRatePctGood)))
	}
	if f.ConversionRatePct >= b.ConversionRatePctGood {
		a.Strengths = append(a.Strengths, fmt.Sprintf(
			"Conversion rate %s%% meets or beats the %s%% benchmark.",
			fnum(f.ConversionRatePct), fnum(b.ConversionRatePctGood)))
	} else {
		a.Weaknesses = append(a.Weaknesses, fmt.Sprintf(
			"Conversion rate %s%% is below the %s%% benchmark.",
			fnum(f.ConversionRatePct), fnum(b.ConversionRatePctGood)))
	}
	if f.UnsubRatePct > b.UnsubRatePctBad {
		a.Weaknesses = append(a.Weaknesses, fmt.Sprintf(
			"Unsubscribe rate %s%% exceeds the %s%% threshold.",
			fnum(f.UnsubRatePct), fnum(b.UnsubRatePctBad)))
	}
	if !f.DataSufficient {
		a.Weaknesses = append(a.Weaknesses, fmt.Sprintf(
			"Insufficient data: %d sends is below the %d minimum sample, so rates are directional only.",
			f.Sends, f.MinSampleSends))
	}

	conf := 0.75
	if !f.DataSufficient {
		conf = 0.3
	}
	conf -= 0.1 * float64(len(a.Anomalies))
	if conf < 0.05 {
		conf = 0.05
	}
	a.Confidence = conf
	return a
}

// objectiveArea maps a campaign objective to the recommendation area that
// serves it best; matching recommendations are stably moved to the front.
var objectiveArea = map[string]string{
	"conversion":   "segmentation",
	"engagement":   "timing",
	"retention":    "risk",
	"reactivation": "audience",
	"awareness":    "channel",
}

// Recommend emits deterministic recommendations from threshold rules. It
// always returns at least one entry so the endpoint never fails from AI.
func (p *RuleBasedProvider) Recommend(f *Facts, objective string) []Recommendation {
	b := f.Benchmarks
	var recs []Recommendation

	if f.Sends == 0 {
		return []Recommendation{{
			Area:       "strategy",
			Suggestion: "Run a small pilot send to a seed segment before scaling spend.",
			Reasoning: fmt.Sprintf(
				"The campaign has %d sends; at least %d are needed before performance-based recommendations are meaningful.",
				f.Sends, f.MinSampleSends),
			Confidence: 0.2,
		}}
	}

	if f.BounceRatePct > b.BounceRatePctBad {
		recs = append(recs, Recommendation{
			Area:       "risk",
			Suggestion: "Suppress hard-bouncing addresses and re-verify the acquisition source before the next send.",
			Reasoning: fmt.Sprintf(
				"Bounce rate %s%% exceeds the %s%% threshold.",
				fnum(f.BounceRatePct), fnum(b.BounceRatePctBad)),
			Confidence: 0.8,
		})
	}
	if f.UnsubRatePct > b.UnsubRatePctBad {
		recs = append(recs, Recommendation{
			Area:       "risk",
			Suggestion: "Reduce send frequency and add a preference check to the target segment.",
			Reasoning: fmt.Sprintf(
				"Unsubscribe rate %s%% exceeds the %s%% threshold.",
				fnum(f.UnsubRatePct), fnum(b.UnsubRatePctBad)),
			Confidence: 0.7,
		})
	}
	if f.OpenRatePct < b.OpenRatePctGood {
		recs = append(recs, Recommendation{
			Area:       "timing",
			Suggestion: "Split-test subject lines and send-time windows on a holdout slice.",
			Reasoning: fmt.Sprintf(
				"Open rate %s%% is below the %s%% benchmark.",
				fnum(f.OpenRatePct), fnum(b.OpenRatePctGood)),
			Confidence: 0.6,
		})
	}
	if f.OpenRatePct >= b.OpenRatePctGood && f.ClickRatePct < b.ClickRatePctGood {
		recs = append(recs, Recommendation{
			Area:       "strategy",
			Suggestion: "Strengthen the primary call-to-action and tighten message-to-audience relevance.",
			Reasoning: fmt.Sprintf(
				"Open rate %s%% is healthy but click rate %s%% trails the %s%% benchmark.",
				fnum(f.OpenRatePct), fnum(f.ClickRatePct), fnum(b.ClickRatePctGood)),
			Confidence: 0.6,
		})
	}
	if f.ConversionRatePct < b.ConversionRatePctGood {
		recs = append(recs, Recommendation{
			Area:       "segmentation",
			Suggestion: "Narrow targeting to recently engaged, high-score customers.",
			Reasoning: fmt.Sprintf(
				"Conversion rate %s%% is below the %s%% benchmark.",
				fnum(f.ConversionRatePct), fnum(b.ConversionRatePctGood)),
			Confidence: 0.55,
		})
	}
	if len(recs) == 0 {
		recs = append(recs,
			Recommendation{
				Area:       "audience",
				Suggestion: "Expand reach with a lookalike audience built from converted customers.",
				Reasoning: fmt.Sprintf(
					"All rates are at or above benchmark: open %s%%, click %s%%, conversion %s%%.",
					fnum(f.OpenRatePct), fnum(f.ClickRatePct), fnum(f.ConversionRatePct)),
				Confidence: 0.5,
			},
			Recommendation{
				Area:       "channel",
				Suggestion: "Test one adjacent channel against the current best performer.",
				Reasoning:  "Performance is healthy, so incremental channel testing can lift total conversions.",
				Confidence: 0.4,
			})
	}
	if !f.DataSufficient {
		for i := range recs {
			if recs[i].Confidence > 0.35 {
				recs[i].Confidence = 0.35
			}
		}
		recs = append(recs, Recommendation{
			Area:       "strategy",
			Suggestion: "Collect more campaign events before committing additional budget.",
			Reasoning: fmt.Sprintf(
				"Only %d sends observed; the minimum reliable sample is %d.",
				f.Sends, f.MinSampleSends),
			Confidence: 0.3,
		})
	}
	if area, ok := objectiveArea[objective]; ok {
		sort.SliceStable(recs, func(i, j int) bool {
			return recs[i].Area == area && recs[j].Area != area
		})
	}
	return recs
}
