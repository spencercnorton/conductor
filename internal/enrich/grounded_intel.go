// grounded-match client for grounded fuzzy title matching.
//
// grounded-match exposes ask_grounded — auto-gathers semantic chunks from
// indexed corpora plus optional context, hands it to a local LLM and
// returns a short answer with [#N] citations.
//
// Conductor uses this in two ways:
//
//  1. EPG title disambiguation. When TMDb returns multiple plausible
//     matches for an episode title (esp. for talk shows / news / sports),
//     ask_grounded against the prior 30 days of normalized EPG entries
//     for the same channel to pick the most consistent series.
//
//  2. Sports event matching. When TheSportsDB doesn't have the event,
//     ask_grounded against a curated week-of-sports schedule corpus.
//
// This file is the HTTP client; the prompt construction lives in matcher.go
// (Phase 3).
package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type IntelClient struct {
	BaseURL string // e.g. http://image-gen.internal:8930
	APIKey  string // configured out of band
	HC      *http.Client
}

func NewIntel(baseURL, apiKey string) *IntelClient {
	return &IntelClient{
		BaseURL: baseURL,
		APIKey:  apiKey,
		HC:      &http.Client{Timeout: 60 * time.Second},
	}
}

type AskGroundedRequest struct {
	Question string   `json:"question"`
	Repos    []string `json:"repos,omitempty"`     // restrict semantic search scope
	Context  string   `json:"extra_context,omitempty"` // free text appended to the prompt
}

type AskGroundedResponse struct {
	Answer  string         `json:"answer"`
	Sources []IntelSource  `json:"sources"`
	Tokens  map[string]int `json:"tokens,omitempty"`
}

type IntelSource struct {
	Index    int    `json:"index"`
	Path     string `json:"path"`
	Snippet  string `json:"snippet"`
}

// AskGrounded calls grounded-match's ask_grounded endpoint.
func (c *IntelClient) AskGrounded(ctx context.Context, req AskGroundedRequest) (*AskGroundedResponse, error) {
	if c == nil || c.BaseURL == "" {
		return nil, fmt.Errorf("grounded-match not configured")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/ask_grounded", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		r.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.HC.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("grounded-match %d: %s", resp.StatusCode, string(b))
	}
	var out AskGroundedResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}
