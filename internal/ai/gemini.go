package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/spinach/martech-engine/internal/config"
)

const geminiURL = "https://generativelanguage.googleapis.com/v1beta/models/"

// GeminiProvider calls the Gemini generateContent API with a dedicated
// http.Client bound to cfg.LLMTimeoutMs (CONTRACTS §8).
type GeminiProvider struct {
	client *http.Client
	apiKey string
	model  string
}

func NewGeminiProvider(cfg *config.Config) *GeminiProvider {
	p := &GeminiProvider{
		client: &http.Client{Timeout: 15 * time.Second},
		model:  "gemini-1.5-flash",
	}
	if cfg != nil {
		if cfg.LLMTimeoutMs > 0 {
			p.client.Timeout = time.Duration(cfg.LLMTimeoutMs) * time.Millisecond
		}
		if cfg.GeminiModel != "" {
			p.model = cfg.GeminiModel
		}
		p.apiKey = cfg.GeminiAPIKey
	}
	return p
}

func (p *GeminiProvider) Name() string { return "gemini" }

// Available reports whether credentials exist; the chain skips unavailable
// providers (empty key ⇒ not configured).
func (p *GeminiProvider) Available() bool { return p.apiKey != "" }

func (p *GeminiProvider) Complete(ctx context.Context, prompt string) (string, error) {
	if !p.Available() {
		return "", errProviderUnavailable
	}
	u := fmt.Sprintf("%s%s:generateContent?key=%s",
		geminiURL, url.PathEscape(p.model), url.QueryEscape(p.apiKey))
	reqBody, err := json.Marshal(map[string]any{
		"contents": []map[string]any{
			{"parts": []map[string]string{{"text": prompt}}},
		},
		"generationConfig": map[string]any{
			"temperature":     0.2,
			"maxOutputTokens": 1024,
		},
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gemini: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("gemini: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("gemini: status %d: %s", resp.StatusCode, snippet(body))
	}
	var out struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("gemini: decode: %w", err)
	}
	if len(out.Candidates) == 0 || len(out.Candidates[0].Content.Parts) == 0 ||
		out.Candidates[0].Content.Parts[0].Text == "" {
		return "", fmt.Errorf("gemini: empty completion")
	}
	return out.Candidates[0].Content.Parts[0].Text, nil
}
