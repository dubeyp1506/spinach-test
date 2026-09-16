package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spinach/martech-engine/internal/config"
)

const groqURL = "https://api.groq.com/openai/v1/chat/completions"

// GroqProvider calls Groq's OpenAI-compatible chat completions endpoint with a
// dedicated http.Client bound to cfg.LLMTimeoutMs (CONTRACTS §8).
type GroqProvider struct {
	client *http.Client
	apiKey string
	model  string
}

func NewGroqProvider(cfg *config.Config) *GroqProvider {
	p := &GroqProvider{
		client: &http.Client{Timeout: 15 * time.Second},
		model:  "llama-3.1-8b-instant",
	}
	if cfg != nil {
		if cfg.LLMTimeoutMs > 0 {
			p.client.Timeout = time.Duration(cfg.LLMTimeoutMs) * time.Millisecond
		}
		if cfg.GroqModel != "" {
			p.model = cfg.GroqModel
		}
		p.apiKey = cfg.GroqAPIKey
	}
	return p
}

func (p *GroqProvider) Name() string { return "groq" }

// Available reports whether credentials exist; the chain skips unavailable
// providers (empty key ⇒ not configured).
func (p *GroqProvider) Available() bool { return p.apiKey != "" }

func (p *GroqProvider) Complete(ctx context.Context, prompt string) (string, error) {
	if !p.Available() {
		return "", errProviderUnavailable
	}
	reqBody, err := json.Marshal(map[string]any{
		"model":       p.model,
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"temperature": 0.2,
		"max_tokens":  1024,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, groqURL, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("groq: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("groq: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("groq: status %d: %s", resp.StatusCode, snippet(body))
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("groq: decode: %w", err)
	}
	if len(out.Choices) == 0 || out.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("groq: empty completion")
	}
	return out.Choices[0].Message.Content, nil
}

// snippet bounds error strings from upstream response bodies.
func snippet(b []byte) string {
	const max = 256
	if len(b) > max {
		return string(b[:max])
	}
	return string(b)
}
