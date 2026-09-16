package ai

import (
	"context"
	"errors"
	"log/slog"

	"github.com/spinach/martech-engine/internal/config"
)

// MaxConcurrentLLM caps in-flight LLM calls (CONTRACTS §8); excess → 429.
const MaxConcurrentLLM = 8

// ErrOverloaded is returned when the LLM concurrency semaphore is full;
// handlers map it to HTTP 429 via core.RespondError.
var ErrOverloaded = errors.New("ai: llm concurrency limit reached")

// errProviderUnavailable is returned when a provider lacks credentials; the
// chain skips such providers before calling Complete.
var errProviderUnavailable = errors.New("ai: provider unavailable")

// LLMProvider is the §3 contract every provider sits behind.
type LLMProvider interface {
	Name() string
	Complete(ctx context.Context, prompt string) (string, error)
}

// availabler is implemented by remote providers that can report missing
// credentials (empty API key) so the chain skips them entirely.
type availabler interface {
	Available() bool
}

type providerEntry struct {
	p      LLMProvider
	remote bool // remote providers consume a semaphore slot; the local fallback does not
	br     *circuitBreaker
}

// Chain is the ordered provider cascade: groq → gemini → rule-based fallback
// (or cfg.LLMPrimary first). The fallback never fails, so Execute only errors
// on overload — requests never fail from AI.
type Chain struct {
	providers []*providerEntry
	sem       chan struct{}
}

// NewChain wires the contract order. cfg.LLMPrimary ("groq"|"gemini", default
// "groq") selects which remote provider leads; the other follows; the
// deterministic fallback is always last.
func NewChain(cfg *config.Config) *Chain {
	llms := []LLMProvider{NewGroqProvider(cfg), NewGeminiProvider(cfg)}
	if cfg != nil && cfg.LLMPrimary == "gemini" {
		llms[0], llms[1] = llms[1], llms[0]
	}
	c := &Chain{sem: make(chan struct{}, MaxConcurrentLLM)}
	for _, p := range llms {
		c.providers = append(c.providers, &providerEntry{p: p, remote: true, br: newCircuitBreaker()})
	}
	c.providers = append(c.providers, &providerEntry{p: NewRuleBasedProvider(), br: newCircuitBreaker()})
	return c
}

// Result reports which provider served and whether the fallback was used.
type Result struct {
	Output       any    // validated output (*Analysis or []Recommendation)
	Raw          string // raw provider text, for logs/debugging
	Provider     string // e.g. "groq", "gemini", "rule-based-fallback"
	FallbackUsed bool   // true when the deterministic fallback served
}

// Execute runs the cascade for one prompt. parse both unmarshals and validates
// the raw output; invalid output is retried once on the same provider, then
// the chain advances. Transport errors advance immediately. Unavailable
// providers (no API key) and open circuits are skipped.
func (c *Chain) Execute(ctx context.Context, prompt string, parse func(raw string) (any, error)) (*Result, error) {
	var lastErr error
	for _, e := range c.providers {
		if av, ok := e.p.(availabler); ok && !av.Available() {
			continue
		}
		if !e.br.allow() {
			continue
		}
		raw, out, err := c.attempt(ctx, e, prompt, parse)
		if err != nil {
			if errors.Is(err, ErrOverloaded) {
				return nil, err
			}
			e.br.recordFailure()
			lastErr = err
			slog.Warn("ai provider failed", "provider", e.p.Name(), "err", err)
			continue
		}
		e.br.recordSuccess()
		return &Result{
			Output:       out,
			Raw:          raw,
			Provider:     e.p.Name(),
			FallbackUsed: !e.remote,
		}, nil
	}
	if lastErr == nil {
		lastErr = errors.New("ai: no provider available")
	}
	return nil, lastErr
}

// attempt performs one provider turn: call → validate → on invalid output,
// retry once → still invalid means the provider's turn fails.
func (c *Chain) attempt(ctx context.Context, e *providerEntry, prompt string, parse func(string) (any, error)) (string, any, error) {
	raw, err := c.complete(ctx, e, prompt)
	if err != nil {
		return "", nil, err
	}
	out, perr := parse(raw)
	if perr == nil {
		return raw, out, nil
	}
	// Malformed/invalid output → retry once (then the chain advances).
	raw, err = c.complete(ctx, e, prompt)
	if err != nil {
		return "", nil, err
	}
	out, perr = parse(raw)
	if perr != nil {
		return "", nil, perr
	}
	return raw, out, nil
}

// complete runs one provider call, holding a semaphore slot for remote
// providers. A full semaphore returns ErrOverloaded immediately (429), per §8.
func (c *Chain) complete(ctx context.Context, e *providerEntry, prompt string) (string, error) {
	if e.remote {
		select {
		case c.sem <- struct{}{}:
			defer func() { <-c.sem }()
		default:
			return "", ErrOverloaded
		}
	}
	return e.p.Complete(ctx, prompt)
}
