package ai

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// numLiteral extracts numeric literals from free text (campaign metrics are
// non-negative, so signs are not part of the grammar).
var numLiteral = regexp.MustCompile(`\d+(?:\.\d+)?`)

// Analysis is the validated output shape for /analyze (openapi
// AIAnalysisResponse.analysis).
type Analysis struct {
	Summary    string   `json:"summary"`
	Strengths  []string `json:"strengths"`
	Weaknesses []string `json:"weaknesses"`
	Anomalies  []string `json:"anomalies"`
	Confidence float64  `json:"confidence"`
}

// Recommendation is one entry of the /recommend output
// (openapi AIRecommendResponse.recommendations[]).
type Recommendation struct {
	Area       string  `json:"area"` // audience|channel|timing|segmentation|strategy|risk
	Suggestion string  `json:"suggestion"`
	Reasoning  string  `json:"reasoning"`
	Confidence float64 `json:"confidence"`
}

var validAreas = map[string]bool{
	"audience": true, "channel": true, "timing": true,
	"segmentation": true, "strategy": true, "risk": true,
}

// stripToJSON removes markdown fences and surrounding prose, leaving the
// widest {...} span — LLMs frequently wrap output in ```json blocks.
func stripToJSON(raw string) string {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "```") {
		if i := strings.IndexByte(s, '\n'); i >= 0 {
			s = s[i+1:]
		}
		s = strings.TrimSpace(s)
		if strings.HasSuffix(s, "```") {
			s = strings.TrimSpace(s[:len(s)-3])
		}
	}
	if i := strings.IndexByte(s, '{'); i >= 0 {
		if j := strings.LastIndexByte(s, '}'); j > i {
			s = s[i : j+1]
		}
	}
	return s
}

// FactNumberSet collects every numeric literal present in the facts JSON —
// both numeric values and digits inside string values (e.g. timestamps) — into
// a set of float64s for the number-containment check.
func FactNumberSet(factsJSON []byte) map[float64]struct{} {
	set := map[float64]struct{}{}
	var v any
	if err := json.Unmarshal(factsJSON, &v); err != nil {
		return set
	}
	collectNumbers(v, set)
	return set
}

func collectNumbers(v any, set map[float64]struct{}) {
	switch t := v.(type) {
	case float64:
		set[t] = struct{}{}
	case string:
		for _, m := range numLiteral.FindAllString(t, -1) {
			if f, err := strconv.ParseFloat(m, 64); err == nil {
				set[f] = struct{}{}
			}
		}
	case []any:
		for _, e := range t {
			collectNumbers(e, set)
		}
	case map[string]any:
		for _, e := range t {
			collectNumbers(e, set)
		}
	}
}

// numberContained reports whether n appears in the facts number set, allowing
// for percent/fraction re-expression (21.5 ↔ 0.215).
func numberContained(set map[float64]struct{}, n float64) bool {
	if _, ok := set[n]; ok {
		return true
	}
	if _, ok := set[n/100]; ok {
		return true
	}
	if _, ok := set[n*100]; ok {
		return true
	}
	return false
}

// checkNumbers enforces the hallucination guard: every numeric literal in the
// text must be present in the facts JSON.
func checkNumbers(text string, factsNums map[float64]struct{}) error {
	for _, m := range numLiteral.FindAllString(text, -1) {
		f, err := strconv.ParseFloat(m, 64)
		if err != nil {
			continue
		}
		if !numberContained(factsNums, f) {
			return fmt.Errorf("ai: cited number %s not present in facts", m)
		}
	}
	return nil
}

// ParseAnalysis validates raw LLM output for /analyze. Any failure returns an
// error so the chain can retry once and then advance to the next provider.
func ParseAnalysis(raw string, factsJSON []byte) (*Analysis, error) {
	var a Analysis
	if err := json.Unmarshal([]byte(stripToJSON(raw)), &a); err != nil {
		return nil, fmt.Errorf("ai: malformed analysis JSON: %w", err)
	}
	if strings.TrimSpace(a.Summary) == "" {
		return nil, errors.New("ai: analysis missing summary")
	}
	if a.Strengths == nil || a.Weaknesses == nil || a.Anomalies == nil {
		return nil, errors.New("ai: analysis missing strengths/weaknesses/anomalies arrays")
	}
	if a.Confidence < 0 || a.Confidence > 1 {
		return nil, fmt.Errorf("ai: confidence %v out of [0,1]", a.Confidence)
	}
	nums := FactNumberSet(factsJSON)
	texts := make([]string, 0, 1+len(a.Strengths)+len(a.Weaknesses)+len(a.Anomalies))
	texts = append(texts, a.Summary)
	texts = append(texts, a.Strengths...)
	texts = append(texts, a.Weaknesses...)
	texts = append(texts, a.Anomalies...)
	for _, t := range texts {
		if err := checkNumbers(t, nums); err != nil {
			return nil, err
		}
	}
	return &a, nil
}

// ParseRecommendations validates raw LLM output for /recommend.
func ParseRecommendations(raw string, factsJSON []byte) ([]Recommendation, error) {
	var w struct {
		Recommendations []Recommendation `json:"recommendations"`
	}
	if err := json.Unmarshal([]byte(stripToJSON(raw)), &w); err != nil {
		return nil, fmt.Errorf("ai: malformed recommendations JSON: %w", err)
	}
	if len(w.Recommendations) == 0 {
		return nil, errors.New("ai: empty recommendations")
	}
	nums := FactNumberSet(factsJSON)
	for i, r := range w.Recommendations {
		if !validAreas[r.Area] {
			return nil, fmt.Errorf("ai: recommendation %d has invalid area %q", i, r.Area)
		}
		if strings.TrimSpace(r.Suggestion) == "" || strings.TrimSpace(r.Reasoning) == "" {
			return nil, fmt.Errorf("ai: recommendation %d missing suggestion/reasoning", i)
		}
		if r.Confidence < 0 || r.Confidence > 1 {
			return nil, fmt.Errorf("ai: recommendation %d confidence %v out of [0,1]", i, r.Confidence)
		}
		if err := checkNumbers(r.Suggestion+" "+r.Reasoning, nums); err != nil {
			return nil, err
		}
	}
	return w.Recommendations, nil
}
