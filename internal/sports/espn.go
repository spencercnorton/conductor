// espn.go — ESPN public scoreboard API client.
//
// ESPN exposes a documentation-free but stable JSON API at
// `site.api.espn.com/apis/site/v2/sports/<sport>/<league>/scoreboard`.
// It returns the full daily slate with team names, kickoff times,
// venue, and broadcaster info — typically 15-20× more events per call
// than TheSportsDB's free tier (1 event per league).
//
// We use it as the primary fetcher and fall back to TheSportsDB for
// leagues ESPN doesn't cover (or when ESPN is unreachable).
//
// No auth, no rate limit. ESPN occasionally returns 5xx during
// scoreboard transitions — caller is expected to retry on next pass.
package sports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// ESPNClient wraps the public scoreboard endpoint.
type ESPNClient struct {
	BaseURL string // default: https://site.api.espn.com/apis/site/v2/sports
	HTTP    *http.Client
	Logger  *slog.Logger // optional; per-day window failures log here

	// PerRequestTimeout bounds a single scoreboard-day fetch. It's
	// applied as a context deadline on each HTTP call so a slow ESPN
	// edge (the "awaiting headers" stall behind Akamai we saw in the
	// 2026-06 episode) can't consume the whole pass before failing.
	// Zero falls back to defaultESPNPerRequestTimeout.
	PerRequestTimeout time.Duration
	// Retries is the number of *additional* attempts per day after the
	// first on a transient error (timeout / 5xx / transport error).
	// Zero falls back to defaultESPNRetries. Retries back off linearly
	// by RetryBackoff.
	Retries int
	// RetryBackoff is the base delay between retry attempts (attempt N
	// waits N*RetryBackoff). Zero falls back to defaultESPNRetryBackoff.
	RetryBackoff time.Duration
}

const (
	// defaultESPNPerRequestTimeout: ESPN's scoreboard endpoint normally
	// answers in well under a second; 12s tolerates a slow Akamai edge
	// while still leaving headroom under the http.Client wall-clock cap.
	defaultESPNPerRequestTimeout = 12 * time.Second
	defaultESPNRetries           = 2
	defaultESPNRetryBackoff      = 500 * time.Millisecond
	// espnClientTimeout is the http.Client wall-clock cap. It sits above
	// PerRequestTimeout so the per-request context is the deadline that
	// actually fires (giving us a clean, retryable error) rather than the
	// client's blunt cancel.
	espnClientTimeout = 20 * time.Second
)

// NewESPNClient builds a client with sensible defaults.
func NewESPNClient() *ESPNClient {
	return &ESPNClient{
		BaseURL:           "https://site.api.espn.com/apis/site/v2/sports",
		HTTP:              &http.Client{Timeout: espnClientTimeout},
		PerRequestTimeout: defaultESPNPerRequestTimeout,
		Retries:           defaultESPNRetries,
		RetryBackoff:      defaultESPNRetryBackoff,
	}
}

func (c *ESPNClient) perRequestTimeout() time.Duration {
	if c.PerRequestTimeout > 0 {
		return c.PerRequestTimeout
	}
	return defaultESPNPerRequestTimeout
}

func (c *ESPNClient) retries() int {
	if c.Retries > 0 {
		return c.Retries
	}
	return defaultESPNRetries
}

func (c *ESPNClient) retryBackoff() time.Duration {
	if c.RetryBackoff > 0 {
		return c.RetryBackoff
	}
	return defaultESPNRetryBackoff
}

// espnPath maps our internal league key to the ESPN URL pair
// (sport, league). Returns "", "" for leagues ESPN doesn't host.
func espnPath(league string) (sport, eleague string) {
	switch strings.ToLower(league) {
	case "nfl":
		return "football", "nfl"
	case "nba":
		return "basketball", "nba"
	case "nhl":
		return "hockey", "nhl"
	case "mlb":
		return "baseball", "mlb"
	case "mls":
		return "soccer", "usa.1"
	case "ncaaf":
		return "football", "college-football"
	case "ncaab":
		return "basketball", "mens-college-basketball"
	case "epl":
		return "soccer", "eng.1"
	case "laliga":
		return "soccer", "esp.1"
	case "bundesliga":
		return "soccer", "ger.1"
	case "seriea":
		return "soccer", "ita.1"
	case "ufc":
		return "mma", "ufc"
	case "f1":
		return "racing", "f1"
	}
	return "", ""
}

// Scoreboard returns all events ESPN knows about for a league on a
// specific date. Pass a zero `day` to fetch today's slate.
func (c *ESPNClient) Scoreboard(ctx context.Context, league string, day time.Time) ([]Event, error) {
	sport, eleague := espnPath(league)
	if sport == "" {
		return nil, fmt.Errorf("espn: unknown league %q", league)
	}
	url := fmt.Sprintf("%s/%s/%s/scoreboard",
		strings.TrimRight(c.BaseURL, "/"), sport, eleague)
	if !day.IsZero() {
		url += "?dates=" + day.UTC().Format("20060102")
	}
	return c.fetchEvents(ctx, url, league)
}

// ScoreboardWindow fetches the scoreboard for each day in [from, to]
// and concatenates results. ESPN's API returns at most one day per
// call, so multi-day calls cost N HTTP roundtrips. We keep the
// window narrow on purpose — 7 days = 7 calls per league per pass.
func (c *ESPNClient) ScoreboardWindow(ctx context.Context, league string, from, to time.Time) ([]Event, error) {
	if to.Before(from) {
		return nil, fmt.Errorf("espn: window end %v is before start %v", to, from)
	}
	day := from.UTC().Truncate(24 * time.Hour)
	end := to.UTC().Truncate(24 * time.Hour)

	var all []Event
	seen := map[string]struct{}{}
	days, failed := 0, 0
	var lastErr error
	for !day.After(end) {
		days++
		evs, err := c.Scoreboard(ctx, league, day)
		if err != nil {
			// Per-day errors don't abort the window — stale-day
			// failures are common during ESPN's daily transitions
			// (~midnight ET). But they must not be silent either:
			// the 2026-06 outage (DNS-level block of the API) ran
			// 3 days with zero log lines because this loop swallowed
			// every error (audit E6).
			failed++
			lastErr = err
			if c.Logger != nil {
				c.Logger.Warn("espn scoreboard day failed",
					"league", league, "day", day.Format("2006-01-02"), "err", err)
			}
			day = day.Add(24 * time.Hour)
			continue
		}
		for _, e := range evs {
			if _, ok := seen[e.ExternalID]; ok {
				continue
			}
			seen[e.ExternalID] = struct{}{}
			all = append(all, e)
		}
		day = day.Add(24 * time.Hour)
	}
	// Every day failing means the fetcher itself is down (DNS, network,
	// API change) — surface it so the worker's monitor/alerting sees a
	// failed pass instead of an innocuous empty result.
	if days > 0 && failed == days {
		return nil, fmt.Errorf("espn: all %d scoreboard days failed for %s: %w", days, league, lastErr)
	}
	return all, nil
}

// fetchEvents fetches one scoreboard day with a bounded per-request
// deadline and a small retry budget. Transient failures (context
// deadline / transport error / 5xx) are retried with linear backoff;
// permanent failures (4xx, decode errors, parent-ctx cancellation)
// return immediately. The parent ctx still governs the whole window —
// if it's cancelled we stop retrying.
func (c *ESPNClient) fetchEvents(ctx context.Context, url, league string) ([]Event, error) {
	attempts := c.retries() + 1
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			// Back off, but never past the parent ctx deadline.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * c.retryBackoff()):
			}
		}
		out, err := c.fetchEventsOnce(ctx, url, league)
		if err == nil {
			return out, nil
		}
		lastErr = err
		// Stop early on parent cancellation or non-retryable errors.
		if ctx.Err() != nil || !transientErr(err) {
			return nil, err
		}
	}
	return nil, lastErr
}

// fetchEventsOnce does a single attempt, bounded by PerRequestTimeout
// (derived from the parent ctx so a parent cancel still wins).
func (c *ESPNClient) fetchEventsOnce(ctx context.Context, url, league string) ([]Event, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.perRequestTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Conductor/sports-ingester (github.com/spencercnorton/conductor)")
	// Akamai's edge scores UA/header consistency: this UA with no Accept
	// header gets a 403 block page, while the identical request plus an
	// explicit Accept passes (measured 2026-08-29 via a Go probe from the
	// container's own netns — matrix in AGENTS/journal.md). Keep the
	// honest UA; send the Accept a JSON API client should send anyway.
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		// 512 bytes is enough to keep the status text and an Akamai
		// reference id without repeating a 4KB block page per WARN line
		// (the 2026-08 outage logged the full page 56× per pass).
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, &espnStatusError{
			url:    url,
			status: resp.StatusCode,
			body:   string(body),
		}
	}

	var raw struct {
		Events []espnEvent `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(raw.Events))
	for _, r := range raw.Events {
		ev, ok := r.normalize(league)
		if !ok {
			continue
		}
		out = append(out, ev)
	}
	return out, nil
}

// espnStatusError carries a non-2xx response so transientErr can decide
// whether a retry is worthwhile (5xx = retry, 4xx = give up).
type espnStatusError struct {
	url    string
	status int
	body   string
}

func (e *espnStatusError) Error() string {
	return fmt.Sprintf("espn %s returned %d: %s", e.url, e.status, e.body)
}

// transientErr reports whether err is worth retrying within the pass:
// per-request timeouts, transport-level errors, and 5xx responses are
// transient; 4xx and decode errors are permanent.
func transientErr(err error) bool {
	if err == nil {
		return false
	}
	var se *espnStatusError
	if errors.As(err, &se) {
		return se.status >= 500
	}
	// Per-request deadline (context.DeadlineExceeded) or any transport
	// error from http.Client.Do (DNS, connection refused, header stall)
	// is worth one more shot. A parent-ctx cancellation is handled by the
	// caller's ctx.Err() check before this is consulted, so treating the
	// deadline case as transient here only ever retries the per-request
	// timeout, not a genuine shutdown.
	return true
}

// espnEvent mirrors ESPN's shape — only the fields we use.
type espnEvent struct {
	ID           string            `json:"id"`
	Date         string            `json:"date"` // ISO 8601 with TZ ("2026-05-08T23:00Z")
	Name         string            `json:"name"`
	ShortName    string            `json:"shortName"`
	Status       espnStatus        `json:"status"`
	Competitions []espnCompetition `json:"competitions"`
}

type espnStatus struct {
	Type struct {
		Description string `json:"description"`
		State       string `json:"state"` // "pre" | "in" | "post"
	} `json:"type"`
}

type espnCompetition struct {
	Date        string           `json:"date"`
	Venue       espnVenue        `json:"venue"`
	Broadcasts  []espnBroadcast  `json:"broadcasts"`
	Competitors []espnCompetitor `json:"competitors"`
}

type espnVenue struct {
	FullName string `json:"fullName"`
}

type espnBroadcast struct {
	Names []string `json:"names"`
}

type espnCompetitor struct {
	HomeAway string   `json:"homeAway"`
	Team     espnTeam `json:"team"`
}

type espnTeam struct {
	DisplayName string `json:"displayName"`
}

func (r espnEvent) normalize(league string) (Event, bool) {
	if r.ID == "" || r.Date == "" {
		return Event{}, false
	}
	t, err := time.Parse(time.RFC3339, normalizeESPNTime(r.Date))
	if err != nil {
		// ESPN's "Z" suffix is sometimes "+00:00", sometimes "Z", and
		// rare entries have no offset at all (treat as UTC).
		t, err = time.Parse("2006-01-02T15:04Z", r.Date)
		if err != nil {
			return Event{}, false
		}
	}

	var home, away, broadcaster, venue string
	if len(r.Competitions) > 0 {
		comp := r.Competitions[0]
		venue = comp.Venue.FullName
		// Broadcasters: ESPN packs networks (TNT, FOX, ESPN) into
		// `Names[0]`. Multiple broadcasts join with commas so the
		// downstream filter logic stays simple.
		bcasts := make([]string, 0, len(comp.Broadcasts))
		for _, b := range comp.Broadcasts {
			for _, n := range b.Names {
				bcasts = append(bcasts, n)
			}
		}
		broadcaster = strings.Join(bcasts, ", ")
		for _, c := range comp.Competitors {
			if c.HomeAway == "home" {
				home = c.Team.DisplayName
			} else if c.HomeAway == "away" {
				away = c.Team.DisplayName
			}
		}
	}

	return Event{
		ExternalID:  "espn-" + r.ID,
		League:      strings.ToLower(league),
		HomeTeam:    home,
		AwayTeam:    away,
		KickoffAt:   t.UTC(),
		Duration:    3 * time.Hour,
		Venue:       venue,
		Broadcaster: broadcaster,
	}, true
}

// normalizeESPNTime: ESPN sometimes returns "2026-05-08T23:00Z"
// (missing seconds) which time.RFC3339 rejects. Pad with seconds.
func normalizeESPNTime(s string) string {
	// "...:00Z" or "...:00+HH:MM" passes through.
	// "2026-05-08T23:00Z" → add ":00" before "Z".
	if strings.HasSuffix(s, "Z") && len(s) == len("2006-01-02T15:04Z") {
		return s[:len(s)-1] + ":00Z"
	}
	return s
}
