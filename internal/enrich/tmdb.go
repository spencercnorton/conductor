// Package enrich implements EPG enrichment per spec §6.3 — for every
// epg_program row, look up matching metadata (poster, backdrop, overview,
// IMDb id, cast, runtime, …) from external sources and persist it to
// epg_program_enrichment. Once enriched, the XMLTV output emits the
// poster/desc/category that Plex shows the user.
//
// Stage order (this file is the TMDb client; matcher.go is the scoring;
// enricher.go orchestrates):
//
//   pending epg_program → enricher.Enrich(...) → matcher → tmdb.Search
//                                                           ↓
//                                              tmdb_response_cache
//                                                           ↓
//                                            confidence + best match
//                                                           ↓
//                                             epg_program_enrichment row
//
// The Phase 0 image_gen.go + grounded_intel.go stubs stay in this
// package; Phase 4+ uses them for sports composites + grounded title
// disambiguation when TMDb confidence is low.
package enrich

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// TMDbClient hits api.themoviedb.org. Auth via either the v3 ?api_key=…
// query param OR the v4 Bearer token; this uses v3 for simplicity, which is
// the shape CONDUCTOR_TMDB_API_KEY is expected to have.
type TMDbClient struct {
	BaseURL string // default https://api.themoviedb.org/3
	APIKey  string // v3 api_key
	HC      *http.Client

	// Optional cache. When set, search responses are cached for 30 days
	// (per spec §6.3) so re-running enrichment doesn't re-hit TMDb.
	Cache TMDbCache
}

type TMDbCache interface {
	Get(ctx context.Context, kind, queryHash string) (json.RawMessage, bool, error)
	Put(ctx context.Context, kind, queryHash, queryLabel string, response json.RawMessage) error
}

// NewTMDbClient builds a default client. baseURL == "" → TMDb production.
//
// Transport hardening, all motivated by 2026-05-07 production failures
// when CONDUCTOR_TMDB_API_KEY was first wired up:
//
//  1. tcp4 dialer — TMDb's IPv6 path lands on a CDN edge that returns
//     HTTP 502 ("openresty") while IPv4 works. Go's Happy Eyeballs prefers
//     v6 and won't fall back once v6 connects at TCP+TLS, so v4 must be
//     forced.
//
//  2. HTTP/1.1 only (ForceAttemptHTTP2 + TLSNextProto disabled). Even on
//     v4, Go's HTTP/2 transport got 502s at ~50% rate while wget (HTTP/1.1)
//     was 5/5 from the same container. The simplest hypothesis is HTTP/2
//     framing/keepalive interacting poorly with TMDb's edge for this egress
//     IP. HTTP/1.1 matches what wget reliably does.
//
//  3. Retry-on-5xx — even with v4+HTTP/1.1, TMDb's edge still occasionally
//     5xx's. fetchCached retries up to 2 extra times on 502/503/504/520/522
//     with a small backoff; non-retryable codes (4xx, 200) return immediately.
func NewTMDbClient(baseURL, apiKey string) *TMDbClient {
	if baseURL == "" {
		baseURL = "https://api.themoviedb.org/3"
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp4", addr)
		},
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// Disable HTTP/2 — TMDb's edge 5xx's HTTP/2 traffic from this
		// network at ~50% rate; HTTP/1.1 is reliable.
		ForceAttemptHTTP2: false,
		TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	return &TMDbClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		HC:      &http.Client{Timeout: 15 * time.Second, Transport: transport},
	}
}

// ─────────────────────── Movie search ───────────────────────

type TMDbMovie struct {
	ID            int     `json:"id"`
	Title         string  `json:"title"`
	OriginalTitle string  `json:"original_title"`
	ReleaseDate   string  `json:"release_date"` // YYYY-MM-DD
	Overview      string  `json:"overview"`
	PosterPath    string  `json:"poster_path"`
	BackdropPath  string  `json:"backdrop_path"`
	Popularity    float64 `json:"popularity"`
	VoteAverage   float64 `json:"vote_average"`
	GenreIDs      []int   `json:"genre_ids"`
}

type tmdbSearchMoviesResp struct {
	Page         int          `json:"page"`
	TotalResults int          `json:"total_results"`
	Results      []TMDbMovie  `json:"results"`
}

// SearchMovie queries /search/movie. year is optional (0 = ignore).
func (c *TMDbClient) SearchMovie(ctx context.Context, title string, year int) ([]TMDbMovie, error) {
	if title == "" {
		return nil, fmt.Errorf("title required")
	}
	q := url.Values{}
	q.Set("query", title)
	q.Set("include_adult", "false")
	if year > 0 {
		q.Set("year", strconv.Itoa(year))
	}

	cacheKey := hashQuery("movie", title, year, 0, 0)
	cacheLabel := title
	if year > 0 {
		cacheLabel = fmt.Sprintf("%s (%d)", title, year)
	}

	body, err := c.fetchCached(ctx, "movie", cacheKey, cacheLabel,
		"/search/movie", q)
	if err != nil {
		return nil, err
	}
	var out tmdbSearchMoviesResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode movie search: %w", err)
	}
	return out.Results, nil
}

// ─────────────────────── TV search ───────────────────────

type TMDbTVShow struct {
	ID             int     `json:"id"`
	Name           string  `json:"name"`
	OriginalName   string  `json:"original_name"`
	FirstAirDate   string  `json:"first_air_date"`
	Overview       string  `json:"overview"`
	PosterPath     string  `json:"poster_path"`
	BackdropPath   string  `json:"backdrop_path"`
	Popularity     float64 `json:"popularity"`
	VoteAverage    float64 `json:"vote_average"`
	GenreIDs       []int   `json:"genre_ids"`
}

type tmdbSearchTVResp struct {
	Page         int         `json:"page"`
	TotalResults int         `json:"total_results"`
	Results      []TMDbTVShow `json:"results"`
}

// SearchTV queries /search/tv. year is optional (matches first_air_date).
func (c *TMDbClient) SearchTV(ctx context.Context, name string, year int) ([]TMDbTVShow, error) {
	if name == "" {
		return nil, fmt.Errorf("name required")
	}
	q := url.Values{}
	q.Set("query", name)
	q.Set("include_adult", "false")
	if year > 0 {
		q.Set("first_air_date_year", strconv.Itoa(year))
	}

	cacheKey := hashQuery("tv", name, year, 0, 0)
	cacheLabel := name

	body, err := c.fetchCached(ctx, "tv", cacheKey, cacheLabel, "/search/tv", q)
	if err != nil {
		return nil, err
	}
	var out tmdbSearchTVResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode tv search: %w", err)
	}
	return out.Results, nil
}

// ─────────────────────── Episode lookup ───────────────────────

type TMDbEpisode struct {
	ID            int     `json:"id"`
	Name          string  `json:"name"`
	Overview      string  `json:"overview"`
	StillPath     string  `json:"still_path"`
	AirDate       string  `json:"air_date"`
	SeasonNumber  int     `json:"season_number"`
	EpisodeNumber int     `json:"episode_number"`
	VoteAverage   float64 `json:"vote_average"`
}

// GetEpisode queries /tv/{tv_id}/season/{n}/episode/{m}. Used after
// SearchTV identifies the show.
func (c *TMDbClient) GetEpisode(ctx context.Context, tvID, season, episode int) (*TMDbEpisode, error) {
	if tvID <= 0 || season < 0 || episode < 0 {
		return nil, fmt.Errorf("invalid episode args")
	}
	cacheKey := hashQuery("episode", "", 0, season, episode) + ":" + strconv.Itoa(tvID)

	body, err := c.fetchCached(ctx, "episode", cacheKey,
		fmt.Sprintf("tv=%d s=%d e=%d", tvID, season, episode),
		fmt.Sprintf("/tv/%d/season/%d/episode/%d", tvID, season, episode),
		nil)
	if err != nil {
		return nil, err
	}
	var ep TMDbEpisode
	if err := json.Unmarshal(body, &ep); err != nil {
		return nil, fmt.Errorf("decode episode: %w", err)
	}
	return &ep, nil
}

// ─────────────────────── External IDs (IMDb) ───────────────────────

type TMDbExternalIDs struct {
	IMDBID  string `json:"imdb_id"`
	TVDBID  int    `json:"tvdb_id"`
}

// GetMovieExternalIDs fetches /movie/{id}/external_ids — returns IMDb id
// for a TMDb movie. Plex matches against IMDb id with high priority.
func (c *TMDbClient) GetMovieExternalIDs(ctx context.Context, tmdbID int) (*TMDbExternalIDs, error) {
	cacheKey := "movie-ext:" + strconv.Itoa(tmdbID)
	body, err := c.fetchCached(ctx, "movie", cacheKey, cacheKey,
		fmt.Sprintf("/movie/%d/external_ids", tmdbID), nil)
	if err != nil {
		return nil, err
	}
	var ids TMDbExternalIDs
	if err := json.Unmarshal(body, &ids); err != nil {
		return nil, err
	}
	return &ids, nil
}

// GetTVExternalIDs fetches /tv/{id}/external_ids — returns the TVDB id (and
// IMDb id) for a TMDb series. TMDb is our primary EPG match source but
// Sonarr keys on TVDB ids, so this crosswalk is what lets a TMDb-enriched
// program be found by a TVDB-id consumer (reconciler series match + the
// reactive Torznab `tvdbid` search).
func (c *TMDbClient) GetTVExternalIDs(ctx context.Context, tvID int) (*TMDbExternalIDs, error) {
	cacheKey := "tv-ext:" + strconv.Itoa(tvID)
	body, err := c.fetchCached(ctx, "tv", cacheKey, cacheKey,
		fmt.Sprintf("/tv/%d/external_ids", tvID), nil)
	if err != nil {
		return nil, err
	}
	var ids TMDbExternalIDs
	if err := json.Unmarshal(body, &ids); err != nil {
		return nil, err
	}
	return &ids, nil
}

// ─────────────────────── Image URL helpers ───────────────────────

// PosterURL builds an absolute poster URL from a relative path. Empty path
// returns empty string. Standard sizes: w92, w154, w185, w342, w500, w780, original.
func (c *TMDbClient) PosterURL(path string) string  { return imageURL(path, "w500") }
func (c *TMDbClient) BackdropURL(path string) string { return imageURL(path, "w1280") }
func (c *TMDbClient) StillURL(path string) string    { return imageURL(path, "w300") }

func imageURL(path, size string) string {
	if path == "" {
		return ""
	}
	// TMDb's image base URL is stable; the configuration endpoint says
	// "use https://image.tmdb.org/t/p/<size><path>".
	return "https://image.tmdb.org/t/p/" + size + path
}

// ─────────────────────── HTTP + cache plumbing ───────────────────────

// fetchCached is the only function that hits TMDb. It checks the cache
// first; on miss, fetches and writes back. Cache entries are scoped per
// (kind, queryHash) so movie/tv searches don't collide.
func (c *TMDbClient) fetchCached(
	ctx context.Context, kind, cacheKey, label, path string, q url.Values,
) (json.RawMessage, error) {
	if c.Cache != nil {
		if cached, ok, err := c.Cache.Get(ctx, kind, cacheKey); err == nil && ok {
			return cached, nil
		}
	}

	if q == nil {
		q = url.Values{}
	}
	q.Set("api_key", c.APIKey)
	q.Set("language", "en-US")

	body, status, err := c.doWithRetry(ctx, c.BaseURL+path+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if status >= 400 {
		return nil, fmt.Errorf("tmdb HTTP %d: %s", status, snip(string(body), 256))
	}

	if c.Cache != nil {
		if err := c.Cache.Put(ctx, kind, cacheKey, label, body); err != nil {
			// Cache failure is non-fatal — we still return the response.
			_ = err
		}
	}
	return body, nil
}

// retryable5xx is the set of upstream-transient HTTP statuses that
// fetchCached retries. 4xx (except 429) is treated as caller error and
// returned immediately to avoid masking a real bug.
var retryable5xx = map[int]bool{
	http.StatusBadGateway:         true, // 502
	http.StatusServiceUnavailable: true, // 503
	http.StatusGatewayTimeout:     true, // 504
	520:                           true, // Cloudflare unknown
	522:                           true, // Cloudflare conn timeout
	http.StatusTooManyRequests:    true, // 429 — rate limited, also retry
}

// doWithRetry performs the GET with up to 2 extra attempts on retryable
// statuses, with a small linear backoff. The body is read in full each
// time so callers don't have to retry bookkeeping themselves.
//
// On final failure the last response's body and status are returned —
// caller decides whether to surface as an error.
func (c *TMDbClient) doWithRetry(ctx context.Context, fullURL string) ([]byte, int, error) {
	var (
		lastBody   []byte
		lastStatus int
		lastErr    error
	)
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Accept", "application/json")
		// User-Agent: TMDb's edge appears to treat default Go-http-client
		// less generously than known clients. Identify as Conductor.
		req.Header.Set("User-Agent", "Conductor/1.0 (+https://github.com/spencercnorton/conductor)")

		resp, err := c.HC.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("tmdb request: %w", err)
			// Network-level errors are also retryable up to maxAttempts.
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					return nil, 0, ctx.Err()
				case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
				}
				continue
			}
			return nil, 0, lastErr
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB cap
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("tmdb read: %w", readErr)
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					return nil, 0, ctx.Err()
				case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
				}
				continue
			}
			return nil, 0, lastErr
		}
		lastBody = body
		lastStatus = resp.StatusCode
		lastErr = nil

		if !retryable5xx[resp.StatusCode] {
			return body, resp.StatusCode, nil
		}
		// Retryable: back off then loop.
		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
	}
	return lastBody, lastStatus, lastErr
}

// hashQuery computes a stable cache key. Order-independent for the
// non-string fields, so swapping season/episode args doesn't surprise.
func hashQuery(kind, title string, year, season, episode int) string {
	h := sha256.New()
	h.Write([]byte(kind + "|"))
	h.Write([]byte(strings.ToLower(strings.TrimSpace(title)) + "|"))
	_, _ = bytes.NewBufferString(fmt.Sprintf("%d|%d|%d", year, season, episode)).WriteTo(h)
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// ErrNotFound is returned when TMDb has no match for the query.
var ErrNotFound = fmt.Errorf("tmdb: not found")

func snip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
