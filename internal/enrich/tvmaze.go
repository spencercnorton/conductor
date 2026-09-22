// Package enrich — TVmaze.com TV-series fallback client.
//
// TVmaze is free, no-auth, and rate-limited (20 req/s per IP). It's
// TV-only — no movies — so the enricher only routes non-movie programs
// here. Used as a third fallback after TMDb (and TVDb, when configured)
// because (a) coverage of US/UK TV is good, and (b) it doesn't require
// the operator to sign up for an account or pay a subscription.
//
// TVmaze's `externals` field exposes TVDB and IMDb IDs for most shows,
// so Plex's TVDB-agent matching still works downstream — we just got
// the IDs from TVmaze instead of TVDb directly.
//
// Why a separate client (vs. extending TVDbClient): different auth
// model, different response shape, different rate-limit behaviour, and
// TVmaze responses cache cleanly without the JWT lifecycle that
// TVDbClient has to deal with.
package enrich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// TVmazeClient wraps the public TVmaze JSON API. No auth.
type TVmazeClient struct {
	BaseURL string // default https://api.tvmaze.com
	HC      *http.Client
	Cache   TVmazeCache
}

// TVmazeCache mirrors TVDbCache/TMDbCache: same DB-backed shape, just
// a different `kind` namespace ("tvmaze_series" / "tvmaze_show").
type TVmazeCache interface {
	Get(ctx context.Context, kind, queryHash string) (json.RawMessage, bool, error)
	Put(ctx context.Context, kind, queryHash, queryLabel string, response json.RawMessage) error
}

// NewTVmazeClient builds a client with sensible defaults.
func NewTVmazeClient(baseURL string) *TVmazeClient {
	if baseURL == "" {
		baseURL = "https://api.tvmaze.com"
	}
	return &TVmazeClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HC:      &http.Client{Timeout: 10 * time.Second},
	}
}

// ─────────────────────── Types ───────────────────────

// TVmazeShow is the trimmed view of `show` we care about. TVmaze
// returns many more fields; we only decode the ones the enricher uses.
type TVmazeShow struct {
	ID        int                 `json:"id"`
	Name      string              `json:"name"`
	Premiered string              `json:"premiered"` // YYYY-MM-DD or ""
	Summary   string              `json:"summary"`   // HTML — caller strips tags
	Language  string              `json:"language"`
	Image     *TVmazeImage        `json:"image"`
	Externals TVmazeExternals     `json:"externals"`
	Network   *TVmazeNetwork      `json:"network"`
	WebChan   *TVmazeNetwork      `json:"webChannel"`
}

// TVmazeImage carries the two poster sizes TVmaze publishes.
type TVmazeImage struct {
	Medium   string `json:"medium"`
	Original string `json:"original"`
}

// TVmazeExternals carries cross-reference IDs to other databases.
// TheTVDB and IMDb are the load-bearing fields for Plex matching.
type TVmazeExternals struct {
	TVRage int    `json:"tvrage"`
	TheTVDB int   `json:"thetvdb"`
	IMDb   string `json:"imdb"`
}

// TVmazeNetwork is the broadcaster/streamer info; we use it only for
// debug context in match logs.
type TVmazeNetwork struct {
	Name string `json:"name"`
}

// tvmazeSearchHit wraps each show in /search/shows responses. The
// caller cares about `.show`; `score` is ignored (we re-score via
// the shared matcher).
type tvmazeSearchHit struct {
	Score float64    `json:"score"`
	Show  TVmazeShow `json:"show"`
}

// ─────────────────────── Search ───────────────────────

// SearchShows queries /search/shows?q=. Returns up to ~10 matches per
// TVmaze's default (the API doesn't accept a limit param). year is
// optional: when set, matches are filtered by premiered-year, with the
// same "fall back to unfiltered if filter wipes everything" semantic
// as the TVDb client.
func (c *TVmazeClient) SearchShows(ctx context.Context, query string, year int) ([]TVmazeShow, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query required")
	}
	cacheKey := hashQuery("tvmaze_search", query, year, 0, 0)

	body, err := c.fetchCached(ctx, "tvmaze_search", cacheKey, query,
		"/search/shows?q="+url.QueryEscape(query))
	if err != nil {
		return nil, err
	}

	var hits []tvmazeSearchHit
	if err := json.Unmarshal(body, &hits); err != nil {
		return nil, fmt.Errorf("decode tvmaze search: %w", err)
	}
	shows := make([]TVmazeShow, 0, len(hits))
	for _, h := range hits {
		shows = append(shows, h.Show)
	}

	if year > 0 {
		filtered := make([]TVmazeShow, 0, len(shows))
		for _, s := range shows {
			if parseYearPrefix(s.Premiered) == year {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) > 0 {
			shows = filtered
		}
	}
	return shows, nil
}

// GetShow fetches the full show record by ID. Not used by the
// enricher today (SearchShows already returns enough) — kept so future
// callers (manual override lookup, IDE preview tools) have it.
func (c *TVmazeClient) GetShow(ctx context.Context, id int) (*TVmazeShow, error) {
	if id <= 0 {
		return nil, fmt.Errorf("id required")
	}
	cacheKey := hashQuery("tvmaze_show", "", id, 0, 0)
	body, err := c.fetchCached(ctx, "tvmaze_show", cacheKey, fmt.Sprintf("show#%d", id),
		fmt.Sprintf("/shows/%d", id))
	if err != nil {
		return nil, err
	}
	var show TVmazeShow
	if err := json.Unmarshal(body, &show); err != nil {
		return nil, fmt.Errorf("decode tvmaze show: %w", err)
	}
	return &show, nil
}

// PosterURL picks the largest available image. Returns "" when the
// show has no artwork (which happens for older or low-priority shows).
func (s TVmazeShow) PosterURL() string {
	if s.Image == nil {
		return ""
	}
	if s.Image.Original != "" {
		return s.Image.Original
	}
	return s.Image.Medium
}

// CleanSummary strips TVmaze's HTML wrappers from the summary string.
// TVmaze ships <p>…</p> markup which Plex's XMLTV parser doesn't
// gracefully render in the guide tooltip; the enricher's downstream
// xmltv writer expects plain text.
//
// Conservative: only strips the common tags TVmaze emits. We don't
// reach for a full HTML parser because the summary is short and
// well-formed in practice.
func CleanSummary(s string) string {
	if s == "" {
		return ""
	}
	// Strip the handful of tags TVmaze actually emits.
	for _, t := range []string{"<p>", "</p>", "<b>", "</b>", "<i>", "</i>", "<br>", "<br/>", "<br />"} {
		s = strings.ReplaceAll(s, t, "")
	}
	return strings.TrimSpace(s)
}

// ─────────────────────── HTTP plumbing ───────────────────────

func (c *TVmazeClient) fetchCached(ctx context.Context, kind, cacheKey, label, path string) (json.RawMessage, error) {
	if c.Cache != nil {
		if cached, ok, err := c.Cache.Get(ctx, kind, cacheKey); err == nil && ok {
			return cached, nil
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HC.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tvmaze request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("tvmaze read: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		// TVmaze caps at 20 req/s per IP — the worker's BetweenRequest
		// pacing keeps us well under, but surface 429 distinctly so
		// the worker's pass-level logs can spot a rate-limit storm.
		return nil, fmt.Errorf("tvmaze rate limited (429)")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("tvmaze HTTP %d: %s", resp.StatusCode, snip(string(respBody), 256))
	}

	if c.Cache != nil {
		_ = c.Cache.Put(ctx, kind, cacheKey, label, respBody)
	}
	return respBody, nil
}
