// Package enrich houses the EPG enrichment workers and the asynchronous
// Image-generation integration used only after normal artwork sources
// have missed.
package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spencercnorton/conductor/internal/poster"
)

const (
	defaultImageGenPoll     = 5 * time.Second
	defaultImageGenMaxWait  = 20 * time.Minute
	defaultImageGenMaxBytes = int64(20 << 20)
)

type ImageGenClient struct {
	BaseURL       string
	APIKey        string
	HC            *http.Client
	PollInterval  time.Duration
	MaxWait       time.Duration
	MaxImageBytes int64
}

func NewImageGen(baseURL, apiKey string) *ImageGenClient {
	return &ImageGenClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HC: &http.Client{
			Timeout: 60 * time.Second,
			// X-API-Key is not one of the headers net/http strips on a
			// cross-origin redirect. The gateway API never needs redirects, so
			// reject all of them rather than risk credential disclosure.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		PollInterval:  defaultImageGenPoll,
		MaxWait:       defaultImageGenMaxWait,
		MaxImageBytes: defaultImageGenMaxBytes,
	}
}

type GenerateRequest struct {
	Prompt         string `json:"prompt"`
	NegativePrompt string `json:"negative_prompt,omitempty"`
	Width          int    `json:"width,omitempty"`
	Height         int    `json:"height,omitempty"`
	Steps          int    `json:"steps,omitempty"`
	Model          string `json:"model,omitempty"`
	BatchSize      int    `json:"batch_size,omitempty"`
}

type submitGenerationResponse struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	PollURL string `json:"poll_url"`
}

type imageGenJob struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  string `json:"error"`
	Images []struct {
		URL string `json:"url"`
	} `json:"images"`
}

type imageGenHTTPError struct {
	Status int
	Body   string
}

func (e *imageGenHTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body)
}

// TerminalGenerationError means polling this remote job again cannot help;
// the caller may clear the persisted job id and submit a fresh job later.
type TerminalGenerationError struct{ Message string }

func (e *TerminalGenerationError) Error() string { return e.Message }

func IsTerminalGenerationError(err error) bool {
	var terminal *TerminalGenerationError
	return errors.As(err, &terminal)
}

// Generate submits one job, polls it to completion, and immediately fetches
// the authenticated output. Callers must persist the returned bytes before
// another system lifecycle can remove the gateway copy.
func (c *ImageGenClient) Generate(ctx context.Context, req GenerateRequest) ([]byte, error) {
	jobID, err := c.Submit(ctx, req)
	if err != nil {
		return nil, err
	}
	return c.Wait(ctx, jobID)
}

// Submit creates one remote job. The returned id must be persisted before
// waiting so a local timeout/restart resumes instead of duplicating GPU work.
func (c *ImageGenClient) Submit(ctx context.Context, req GenerateRequest) (string, error) {
	base, err := c.validBaseURL()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return "", fmt.Errorf("image-gen prompt is required")
	}
	if req.Model == "" {
		req.Model = "schnell"
	}
	if req.Width == 0 {
		req.Width = 1024
	}
	if req.Height == 0 {
		req.Height = 1536
	}
	if req.Steps == 0 {
		req.Steps = 4
	}
	if req.BatchSize == 0 {
		req.BatchSize = 1
	}

	var submitted submitGenerationResponse
	if err := c.doJSON(ctx, http.MethodPost, base.ResolveReference(&url.URL{Path: "/v1/generate"}), req, &submitted, http.StatusAccepted); err != nil {
		return "", fmt.Errorf("submit image generation: %w", err)
	}
	if submitted.JobID == "" {
		return "", fmt.Errorf("submit image generation: empty job id")
	}
	return submitted.JobID, nil
}

// Wait resumes an existing remote job and fetches its completed PNG.
func (c *ImageGenClient) Wait(ctx context.Context, jobID string) ([]byte, error) {
	base, err := c.validBaseURL()
	if err != nil {
		return nil, err
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("image-gen job id is required")
	}
	maxWait := c.MaxWait
	if maxWait <= 0 {
		maxWait = defaultImageGenMaxWait
	}
	jobCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	poll := c.PollInterval
	if poll <= 0 {
		poll = defaultImageGenPoll
	}
	jobURL := base.ResolveReference(&url.URL{Path: "/v1/jobs/" + url.PathEscape(jobID)})
	for {
		var job imageGenJob
		if err := c.doJSON(jobCtx, http.MethodGet, jobURL, nil, &job, http.StatusOK); err != nil {
			var httpErr *imageGenHTTPError
			if errors.As(err, &httpErr) && (httpErr.Status == http.StatusNotFound || httpErr.Status == http.StatusGone) {
				return nil, &TerminalGenerationError{Message: "image generation job expired or was lost"}
			}
			return nil, fmt.Errorf("poll image generation: %w", err)
		}
		switch strings.ToLower(job.Status) {
		case "completed":
			if len(job.Images) == 0 || job.Images[0].URL == "" {
				return nil, &TerminalGenerationError{Message: "image generation completed without an image"}
			}
			return c.fetchPNG(jobCtx, base, job.Images[0].URL)
		case "failed", "cancelled":
			return nil, &TerminalGenerationError{Message: fmt.Sprintf(
				"image generation %s: %s", job.Status, strings.TrimSpace(job.Error))}
		case "queued", "starting", "running":
			// Continue below.
		default:
			return nil, &TerminalGenerationError{Message: fmt.Sprintf(
				"image generation returned unknown status %q", job.Status)}
		}

		t := time.NewTimer(poll)
		select {
		case <-jobCtx.Done():
			t.Stop()
			return nil, fmt.Errorf("image generation wait: %w", jobCtx.Err())
		case <-t.C:
		}
	}
}

func (c *ImageGenClient) validBaseURL() (*url.URL, error) {
	if c == nil || c.BaseURL == "" || c.APIKey == "" {
		return nil, fmt.Errorf("image-gen URL and API key are required")
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return nil, fmt.Errorf("invalid image-gen base URL")
	}
	return u, nil
}

func (c *ImageGenClient) doJSON(ctx context.Context, method string, endpoint *url.URL, body any, dst any, expected int) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return err
	}
	r.Header.Set("X-API-Key", c.APIKey)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient().Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != expected {
		return &imageGenHTTPError{Status: resp.StatusCode, Body: readCapped(resp.Body, 64<<10)}
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *ImageGenClient) fetchPNG(ctx context.Context, base *url.URL, raw string) ([]byte, error) {
	ref, err := url.Parse(raw)
	if err != nil {
		return nil, &TerminalGenerationError{Message: "invalid image URL"}
	}
	imageURL := base.ResolveReference(ref)
	if !strings.EqualFold(imageURL.Scheme, base.Scheme) || !strings.EqualFold(imageURL.Host, base.Host) {
		return nil, &TerminalGenerationError{Message: "image URL points outside configured gateway"}
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL.String(), nil)
	if err != nil {
		return nil, err
	}
	r.Header.Set("X-API-Key", c.APIKey)
	resp, err := c.httpClient().Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message := fmt.Sprintf("image fetch HTTP %d: %s", resp.StatusCode, readCapped(resp.Body, 64<<10))
		if terminalCompletedOutputStatus(resp.StatusCode) {
			return nil, &TerminalGenerationError{Message: message}
		}
		return nil, fmt.Errorf("%s", message)
	}
	maxBytes := c.MaxImageBytes
	if maxBytes <= 0 {
		maxBytes = defaultImageGenMaxBytes
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxBytes {
		return nil, &TerminalGenerationError{Message: fmt.Sprintf("generated image exceeds %d bytes", maxBytes)}
	}
	if _, _, err := poster.ValidateGeneratedPNG(b); err != nil {
		return nil, &TerminalGenerationError{Message: "invalid completed gateway output: " + err.Error()}
	}
	return b, nil
}

func terminalCompletedOutputStatus(status int) bool {
	if status >= 300 && status < 400 {
		// Redirects are intentionally disabled; retrying the same completed
		// result cannot make an unsafe output URL become same-origin.
		return true
	}
	if status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests {
		return false
	}
	return status >= 400 && status < 500
}

func (c *ImageGenClient) httpClient() *http.Client {
	if c.HC != nil {
		return c.HC
	}
	return &http.Client{Timeout: 60 * time.Second}
}

func readCapped(r io.Reader, n int64) string {
	b, _ := io.ReadAll(io.LimitReader(r, n))
	return strings.TrimSpace(string(b))
}

// SportsMatchupPrompt avoids literal team-name typography: FLUX-schnell is
// good at vertical broadcast illustration but unreliable at readable text.
func SportsMatchupPrompt(title, subTitle, league string) string {
	detail := strings.TrimSpace(strings.TrimSpace(title + " " + subTitle))
	return fmt.Sprintf(
		"Vertical 2:3 premium sports broadcast key art inspired by %s in %s, two opposing teams in dramatic arena lighting, energetic cinematic composition, team-color atmosphere, polished television graphic, no typography, no letters, no numbers, no signage, no logos, no watermark",
		detail, league,
	)
}
