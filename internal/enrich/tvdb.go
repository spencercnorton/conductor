package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// TVDbClient is the v4 TheTVDB.com API client. Used as a fallback when
// TMDb misses (per spec §6.3). v4 auth: POST /login with {apikey, pin?} →
// JWT bearer token, valid for ~1 month.
//
// Conductor caches the token in-process (refreshes lazily on 401) and
// caches search responses for 30 days via the same TVDbCache interface
// pattern that TMDb uses.
type TVDbClient struct {
	BaseURL string // default https://api4.thetvdb.com/v4
	APIKey  string // v4 project API key
	PIN     string // optional subscriber PIN (empty for free tier)
	HC      *http.Client
	Cache   TVDbCache

	tokenMu  sync.Mutex
	token    string
	tokenExp time.Time
}

// TVDbCache is the same shape as TMDbCache, but kind values are
// "series" / "episode" / "movie" instead of TMDb's set.
type TVDbCache interface {
	Get(ctx context.Context, kind, queryHash string) (json.RawMessage, bool, error)
	Put(ctx context.Context, kind, queryHash, queryLabel string, response json.RawMessage) error
}

func NewTVDbClient(baseURL, apiKey, pin string) *TVDbClient {
	if baseURL == "" {
		baseURL = "https://api4.thetvdb.com/v4"
	}
	return &TVDbClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		PIN:     pin,
		HC:      &http.Client{Timeout: 10 * time.Second},
	}
}

// ─────────────────────── Auth ───────────────────────

type tvdbLoginResp struct {
	Status string `json:"status"`
	Data   struct {
		Token string `json:"token"`
	} `json:"data"`
}

// loginIfNeeded refreshes the JWT when it's missing or near-expired.
// Caller-side concurrency: token mutex held during refresh.
func (c *TVDbClient) loginIfNeeded(ctx context.Context) error {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if c.token != "" && time.Now().Before(c.tokenExp) {
		return nil
	}
	if c.APIKey == "" {
		return errors.New("tvdb api key not configured")
	}

	body, _ := json.Marshal(map[string]string{
		"apikey": c.APIKey,
		"pin":    c.PIN,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/login", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HC.Do(req)
	if err != nil {
		return fmt.Errorf("tvdb login: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("tvdb login HTTP %d: %s", resp.StatusCode, snip(string(respBody), 256))
	}

	var login tvdbLoginResp
	if err := json.Unmarshal(respBody, &login); err != nil {
		return fmt.Errorf("tvdb login decode: %w", err)
	}
	if login.Data.Token == "" {
		return errors.New("tvdb login returned empty token")
	}
	c.token = login.Data.Token
	// TVDb tokens are documented as 1-month TTL. We use 28 days to leave
	// a safety margin for clock skew + slow refreshes.
	c.tokenExp = time.Now().Add(28 * 24 * time.Hour)
	return nil
}

// ─────────────────────── Series search ───────────────────────

type TVDbSeries struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Overview   string `json:"overview"`
	Image      string `json:"image"`       // poster URL
	FirstAired string `json:"first_aired"` // YYYY-MM-DD
	Year       string `json:"year"`        // sometimes provided alongside FirstAired
	IMDbID     string `json:"imdb_id"`
}

type tvdbSearchResp struct {
	Status string         `json:"status"`
	Data   []TVDbSeries   `json:"data"`
}

// SearchSeries looks up a TV series by name. Year is optional; when set,
// it filters results whose first_aired year matches.
func (c *TVDbClient) SearchSeries(ctx context.Context, name string, year int) ([]TVDbSeries, error) {
	if name == "" {
		return nil, fmt.Errorf("name required")
	}

	cacheKey := hashTVDb("series", name, year, 0, 0)

	body, err := c.fetchCached(ctx, "series", cacheKey, name,
		"/search?type=series&query="+escape(name), nil)
	if err != nil {
		return nil, err
	}
	var out tvdbSearchResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode tvdb search: %w", err)
	}

	if year > 0 {
		filtered := make([]TVDbSeries, 0, len(out.Data))
		for _, s := range out.Data {
			if parseYearPrefix(s.FirstAired) == year || s.Year == fmt.Sprint(year) {
				filtered = append(filtered, s)
			}
		}
		// If filtering wiped everything, fall back to the unfiltered list —
		// matcher.MatchScore will lower confidence for the year mismatch.
		if len(filtered) > 0 {
			out.Data = filtered
		}
	}
	return out.Data, nil
}

// ─────────────────────── Movie search ───────────────────────

type TVDbMovie struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Overview string `json:"overview"`
	Image    string `json:"image"`
	Year     string `json:"year"`
	IMDbID   string `json:"imdb_id"`
}

type tvdbMovieSearchResp struct {
	Status string      `json:"status"`
	Data   []TVDbMovie `json:"data"`
}

// SearchMovie looks up a movie by title. year is optional.
func (c *TVDbClient) SearchMovie(ctx context.Context, title string, year int) ([]TVDbMovie, error) {
	if title == "" {
		return nil, fmt.Errorf("title required")
	}
	cacheKey := hashTVDb("movie", title, year, 0, 0)

	body, err := c.fetchCached(ctx, "movie", cacheKey, title,
		"/search?type=movie&query="+escape(title), nil)
	if err != nil {
		return nil, err
	}
	var out tvdbMovieSearchResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode tvdb movie search: %w", err)
	}

	if year > 0 {
		filtered := make([]TVDbMovie, 0, len(out.Data))
		for _, m := range out.Data {
			if m.Year == fmt.Sprint(year) {
				filtered = append(filtered, m)
			}
		}
		if len(filtered) > 0 {
			out.Data = filtered
		}
	}
	return out.Data, nil
}

// ─────────────────────── HTTP plumbing ───────────────────────

func (c *TVDbClient) fetchCached(
	ctx context.Context, kind, cacheKey, label, path string, body io.Reader,
) (json.RawMessage, error) {
	if c.Cache != nil {
		if cached, ok, err := c.Cache.Get(ctx, kind, cacheKey); err == nil && ok {
			return cached, nil
		}
	}

	if err := c.loginIfNeeded(ctx); err != nil {
		return nil, err
	}

	method := http.MethodGet
	if body != nil {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	c.tokenMu.Lock()
	tok := c.token
	c.tokenMu.Unlock()
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HC.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tvdb request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("tvdb read: %w", err)
	}

	// On 401, refresh token and retry once.
	if resp.StatusCode == http.StatusUnauthorized {
		c.tokenMu.Lock()
		c.token = ""
		c.tokenMu.Unlock()
		if err := c.loginIfNeeded(ctx); err != nil {
			return nil, err
		}
		// Recursive call retries with fresh token. Bounded by the empty-token
		// check at the top — if login still produces 401, the 2nd call's
		// loginIfNeeded errors before another retry.
		return c.fetchCached(ctx, kind, cacheKey, label, path, body)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("tvdb HTTP %d: %s", resp.StatusCode, snip(string(respBody), 256))
	}

	if c.Cache != nil {
		_ = c.Cache.Put(ctx, kind, cacheKey, label, respBody)
	}
	return respBody, nil
}

func hashTVDb(kind, title string, year, season, episode int) string {
	return hashQuery("tvdb_"+kind, title, year, season, episode)
}

func escape(s string) string {
	// Minimal URL-encode for the query param — TVDb is forgiving.
	return strings.ReplaceAll(strings.ReplaceAll(s, " ", "%20"), "&", "%26")
}
