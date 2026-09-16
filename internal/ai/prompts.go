package ai

import (
	"errors"
	"strings"
)

// PromptVersion is bumped whenever prompt shape/wording changes; it is part of
// the Redis cache key (ai:{campaign_id}:{metrics_version}:{prompt_version}).
const PromptVersion = "v1"

// Markers delimit the machine-readable facts block inside a prompt so the
// deterministic fallback can recover it without parsing prose.
const (
	factsBegin = "FACTS_JSON_BEGIN"
	factsEnd   = "FACTS_JSON_END"
)

// AnalyzePrompt builds the /analyze prompt. The LLM only narrates the facts;
// it must cite numbers already present and emit strict JSON (CONTRACTS §4).
func AnalyzePrompt(factsJSON []byte) string {
	var b strings.Builder
	b.WriteString("You are a marketing campaign analytics engine. The facts below were computed deterministically by code; narrate them, never recompute or invent statistics.\n")
	b.WriteString("TASK: analyze\n")
	b.WriteString(factsBegin + "\n")
	b.Write(factsJSON)
	b.WriteString("\n" + factsEnd + "\n")
	b.WriteString("Respond ONLY with JSON of the form {\"summary\": string, \"strengths\": [string], \"weaknesses\": [string], \"anomalies\": [string], \"confidence\": number between 0 and 1}.\n")
	b.WriteString("Rules: cite only numbers present in the facts; if the data is insufficient say so in weaknesses and use low confidence; do not quote the campaign_id; never include customer PII.\n")
	return b.String()
}

// RecommendPrompt builds the /recommend prompt. The objective (optional) is
// already embedded in the facts JSON.
func RecommendPrompt(factsJSON []byte) string {
	var b strings.Builder
	b.WriteString("You are a marketing campaign optimization engine. The facts below were computed deterministically by code; recommend actions grounded only in these numbers, never invent statistics.\n")
	b.WriteString("TASK: recommend\n")
	b.WriteString(factsBegin + "\n")
	b.Write(factsJSON)
	b.WriteString("\n" + factsEnd + "\n")
	b.WriteString("Respond ONLY with JSON of the form {\"recommendations\": [{\"area\": \"audience|channel|timing|segmentation|strategy|risk\", \"suggestion\": string, \"reasoning\": string, \"confidence\": number between 0 and 1}]}.\n")
	b.WriteString("Rules: cite only numbers present in the facts; if an objective is present in the facts, prioritize recommendations that serve it; if the data is insufficient say so and use low confidence; never include customer PII.\n")
	return b.String()
}

// extractFactsJSON recovers the facts block embedded between the markers.
func extractFactsJSON(prompt string) ([]byte, error) {
	i := strings.Index(prompt, factsBegin)
	j := strings.Index(prompt, factsEnd)
	if i < 0 || j < 0 || j <= i+len(factsBegin) {
		return nil, errors.New("ai: facts block missing from prompt")
	}
	return []byte(strings.TrimSpace(prompt[i+len(factsBegin) : j])), nil
}

// isRecommendTask reports whether the prompt is a recommend task.
func isRecommendTask(prompt string) bool {
	return strings.Contains(prompt, "TASK: recommend")
}
