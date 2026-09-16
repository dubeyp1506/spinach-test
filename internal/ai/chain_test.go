package ai

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeResp struct {
	raw string
	err error
}

// fakeProvider plays a scripted sequence of responses, one per call.
type fakeProvider struct {
	name      string
	available bool
	calls     int
	script    []fakeResp
}

func (f *fakeProvider) Name() string    { return f.name }
func (f *fakeProvider) Available() bool { return f.available }
func (f *fakeProvider) Complete(_ context.Context, _ string) (string, error) {
	f.calls++
	if len(f.script) == 0 {
		return "", errors.New("unexpected call")
	}
	r := f.script[0]
	f.script = f.script[1:]
	return r.raw, r.err
}

func remoteEntry(p LLMProvider) *providerEntry {
	return &providerEntry{p: p, remote: true, br: newCircuitBreaker()}
}

func fallbackEntry() *providerEntry {
	return &providerEntry{p: NewRuleBasedProvider(), remote: false, br: newCircuitBreaker()}
}

func testChain(entries ...*providerEntry) *Chain {
	return &Chain{providers: entries, sem: make(chan struct{}, MaxConcurrentLLM)}
}

func validAnalysisRaw(t *testing.T) string {
	t.Helper()
	raw, err := NewRuleBasedProvider().Complete(
		context.Background(), AnalyzePrompt(factsFixtureJSON(t, fixtureMetrics())))
	require.NoError(t, err)
	return raw
}

func TestChainErrorAdvances(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())
	p1 := &fakeProvider{name: "p1", available: true,
		script: []fakeResp{{err: errors.New("boom")}}}
	p2 := &fakeProvider{name: "p2", available: true,
		script: []fakeResp{{raw: validAnalysisRaw(t)}}}
	ch := testChain(remoteEntry(p1), remoteEntry(p2), fallbackEntry())

	res, err := ch.Execute(context.Background(), AnalyzePrompt(factsJSON),
		func(raw string) (any, error) { return ParseAnalysis(raw, factsJSON) })
	require.NoError(t, err)
	assert.Equal(t, "p2", res.Provider)
	assert.False(t, res.FallbackUsed)
	assert.Equal(t, 1, p1.calls, "transport errors do not retry")
}

func TestChainInvalidOutputRetriesOnce(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())
	p1 := &fakeProvider{name: "p1", available: true,
		script: []fakeResp{{raw: "garbage"}, {raw: "still garbage"}}}
	p2 := &fakeProvider{name: "p2", available: true,
		script: []fakeResp{{raw: validAnalysisRaw(t)}}}
	ch := testChain(remoteEntry(p1), remoteEntry(p2), fallbackEntry())

	res, err := ch.Execute(context.Background(), AnalyzePrompt(factsJSON),
		func(raw string) (any, error) { return ParseAnalysis(raw, factsJSON) })
	require.NoError(t, err)
	assert.Equal(t, "p2", res.Provider)
	assert.Equal(t, 2, p1.calls, "invalid output retries exactly once")
}

func TestChainAllFailUsesFallback(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())
	p1 := &fakeProvider{name: "p1", available: true,
		script: []fakeResp{{err: errors.New("down")}}}
	p2 := &fakeProvider{name: "p2", available: true,
		script: []fakeResp{{raw: "not json"}}} // invalid once, retried → exhausted script
	ch := testChain(remoteEntry(p1), remoteEntry(p2), fallbackEntry())

	res, err := ch.Execute(context.Background(), AnalyzePrompt(factsJSON),
		func(raw string) (any, error) { return ParseAnalysis(raw, factsJSON) })
	require.NoError(t, err)
	assert.Equal(t, FallbackProviderName, res.Provider)
	assert.True(t, res.FallbackUsed)
	a, ok := res.Output.(*Analysis)
	require.True(t, ok)
	assert.NotEmpty(t, a.Summary)
}

func TestChainSkipsUnavailable(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())
	p1 := &fakeProvider{name: "p1", available: false} // empty API key analog
	p2 := &fakeProvider{name: "p2", available: true,
		script: []fakeResp{{raw: validAnalysisRaw(t)}}}
	ch := testChain(remoteEntry(p1), remoteEntry(p2), fallbackEntry())

	res, err := ch.Execute(context.Background(), AnalyzePrompt(factsJSON),
		func(raw string) (any, error) { return ParseAnalysis(raw, factsJSON) })
	require.NoError(t, err)
	assert.Equal(t, "p2", res.Provider)
	assert.Equal(t, 0, p1.calls, "unavailable provider must be skipped, not called")
}

func TestChainOnlyFallbackWhenNoKeys(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())
	p1 := &fakeProvider{name: "p1", available: false}
	ch := testChain(remoteEntry(p1), fallbackEntry())

	res, err := ch.Execute(context.Background(), AnalyzePrompt(factsJSON),
		func(raw string) (any, error) { return ParseAnalysis(raw, factsJSON) })
	require.NoError(t, err)
	assert.Equal(t, FallbackProviderName, res.Provider)
	assert.True(t, res.FallbackUsed)
}

func TestChainSemaphoreLimit(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())
	p1 := &fakeProvider{name: "p1", available: true,
		script: []fakeResp{{raw: validAnalysisRaw(t)}}}
	ch := testChain(remoteEntry(p1), fallbackEntry())
	for i := 0; i < MaxConcurrentLLM; i++ {
		ch.sem <- struct{}{} // simulate MaxConcurrentLLM in-flight LLM calls
	}
	_, err := ch.Execute(context.Background(), AnalyzePrompt(factsJSON),
		func(raw string) (any, error) { return ParseAnalysis(raw, factsJSON) })
	require.ErrorIs(t, err, ErrOverloaded)
	assert.Equal(t, 0, p1.calls, "must not call provider when semaphore is full")
}

func TestChainOpenCircuitSkipped(t *testing.T) {
	factsJSON := factsFixtureJSON(t, fixtureMetrics())
	p1 := &fakeProvider{name: "p1", available: true,
		script: []fakeResp{{err: errors.New("boom")}}}
	p2 := &fakeProvider{name: "p2", available: true,
		script: []fakeResp{{raw: validAnalysisRaw(t)}}}
	e1 := remoteEntry(p1)
	ch := testChain(e1, remoteEntry(p2), fallbackEntry())
	parse := func(raw string) (any, error) { return ParseAnalysis(raw, factsJSON) }
	prompt := AnalyzePrompt(factsJSON)

	// Trip p1's breaker: 3 consecutive failures.
	for i := 0; i < breakerFailureThreshold; i++ {
		p1.script = []fakeResp{{err: errors.New("boom")}}
		p2.script = []fakeResp{{raw: validAnalysisRaw(t)}}
		_, err := ch.Execute(context.Background(), prompt, parse)
		require.NoError(t, err)
	}
	// Breaker now open: p1 must not be called again.
	before := p1.calls
	p2.script = []fakeResp{{raw: validAnalysisRaw(t)}}
	_, err := ch.Execute(context.Background(), prompt, parse)
	require.NoError(t, err)
	assert.Equal(t, before, p1.calls, "open circuit must skip provider")
}
