// Package sports ingests live game schedules from a sports schedule
// API (TheSportsDB by default) and exposes the parsed events for the
// EPG injector.
//
// Why TheSportsDB:
//   - free tier (key "123" or "3") works without an account
//   - covers NFL/NBA/NHL/MLB/MLS, top European football, F1, PGA,
//     boxing/MMA — the leagues we actually want
//   - exposes per-event TV-station strings, which we use to route the
//     same event onto the right Conductor channel
//
// Authenticated keys (paid tier) get higher rate limits + access to
// /eventsschedule + live-score endpoints. The base URL is
// configurable so we can swap in another provider later (ESPN's
// hidden API, sportradar) without rewriting the worker.
//
// All times are returned as UTC time.Time (TheSportsDB exposes
// `strTimestamp` already in UTC for events with kickoff times).
package sports

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client wraps TheSportsDB's free JSON API.
type Client struct {
	BaseURL string // default: https://www.thesportsdb.com/api/v1/json/3
	APIKey  string // default: "3" (free tier key); paid keys go here
	HTTP    *http.Client
}

// NewClient builds a client with sensible defaults. APIKey "" means
// the free public key.
func NewClient(apiKey string) *Client {
	if apiKey == "" {
		apiKey = "3"
	}
	return &Client{
		BaseURL: "https://www.thesportsdb.com/api/v1/json",
		APIKey:  apiKey,
		HTTP:    &http.Client{Timeout: 15 * time.Second},
	}
}

// Event is the normalised view of a sports event used by the EPG
// injector. Source-specific fields stay in the response cache; this
// struct carries only what the injector needs.
type Event struct {
	ExternalID  string // TheSportsDB idEvent
	League      string // "nfl", "nba", etc. (lowercase)
	HomeTeam    string
	AwayTeam    string
	KickoffAt   time.Time // UTC
	Duration    time.Duration
	Venue       string
	Broadcaster string // TV station / network string (free text)
	Season      string // e.g. "2024-2025"
	Round       string // e.g. "Week 17", "Conference Final"
}

// LeagueID is the TheSportsDB numeric league id (idLeague) for a given
// sport key. Matches the `sport_league` Postgres enum used by
// channel_sport_mapping. Subset that we actively support — extend as
// new leagues come online.
var LeagueID = map[string]string{
	"nfl":        "4391",
	"nba":        "4387",
	"nhl":        "4380",
	"mlb":        "4424",
	"mls":        "4346",
	"ncaaf":      "4479",
	"ncaab":      "4607",
	"epl":        "4328", // English Premier League
	"laliga":     "4335",
	"bundesliga": "4331",
	"seriea":     "4332",
	"f1":         "4370",
	"pga":        "4425",
	"tennis":     "4464", // ATP Tour
	"ufc":        "4443",
}

// SeasonNext returns the next 15 events for a league. TheSportsDB's
// `eventsnextleague.php?id=<league_id>` endpoint serves the upcoming
// schedule pre-broadcast — typically 7-14 days out.
func (c *Client) SeasonNext(ctx context.Context, league string) ([]Event, error) {
	id := LeagueID[strings.ToLower(league)]
	if id == "" {
		return nil, fmt.Errorf("unknown league %q", league)
	}
	endpoint := fmt.Sprintf("%s/%s/eventsnextleague.php?id=%s",
		strings.TrimRight(c.BaseURL, "/"), c.APIKey, id)
	return c.fetchEvents(ctx, endpoint, league)
}

// SeasonLast returns the last 15 played events for a league. Useful
// for catch-up after a multi-day Conductor outage so the EPG doesn't
// have stale "upcoming" rows.
func (c *Client) SeasonLast(ctx context.Context, league string) ([]Event, error) {
	id := LeagueID[strings.ToLower(league)]
	if id == "" {
		return nil, fmt.Errorf("unknown league %q", league)
	}
	endpoint := fmt.Sprintf("%s/%s/eventspastleague.php?id=%s",
		strings.TrimRight(c.BaseURL, "/"), c.APIKey, id)
	return c.fetchEvents(ctx, endpoint, league)
}

// EventsOnDay returns all events for a league on a specific date.
// TheSportsDB's date filter is the most reliable way to pull a full
// day's slate (e.g. all NFL Sunday games).
func (c *Client) EventsOnDay(ctx context.Context, league string, day time.Time) ([]Event, error) {
	id := LeagueID[strings.ToLower(league)]
	if id == "" {
		return nil, fmt.Errorf("unknown league %q", league)
	}
	endpoint := fmt.Sprintf("%s/%s/eventsday.php?d=%s&l=%s",
		strings.TrimRight(c.BaseURL, "/"), c.APIKey,
		day.Format("2006-01-02"), url.QueryEscape(leagueDisplayName(league)))
	return c.fetchEvents(ctx, endpoint, league)
}

// leagueDisplayName maps our internal league key to the league name
// TheSportsDB uses in the `eventsday` endpoint's `l=` parameter.
// (eventsday filters by name, not numeric id — grumble.)
func leagueDisplayName(league string) string {
	switch strings.ToLower(league) {
	case "nfl":
		return "NFL"
	case "nba":
		return "NBA"
	case "nhl":
		return "NHL"
	case "mlb":
		return "MLB"
	case "mls":
		return "American Major League Soccer"
	case "ncaaf":
		return "NCAA Football"
	case "ncaab":
		return "NCAA Basketball"
	case "epl":
		return "English Premier League"
	case "laliga":
		return "Spanish La Liga"
	case "bundesliga":
		return "German Bundesliga"
	case "seriea":
		return "Italian Serie A"
	case "f1":
		return "Formula 1"
	case "pga":
		return "PGA Tour"
	case "tennis":
		return "ATP Tour"
	case "ufc":
		return "Ultimate Fighting Championship"
	}
	return league
}

// fetchEvents is the shared HTTP+JSON path. TheSportsDB returns:
//
//	{"events": [{...}]} | {"events": null}
//
// The null-events case is normal off-season (no upcoming games).
func (c *Client) fetchEvents(ctx context.Context, endpoint, league string) ([]Event, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Conductor/sports-ingester (github.com/spencercnorton/conductor)")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("sports api %s returned %d: %s",
			endpoint, resp.StatusCode, string(body))
	}
	var raw struct {
		Events []rawEvent `json:"events"`
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

// rawEvent mirrors TheSportsDB's JSON. Strings dominate because their
// API returns "" for missing fields rather than nulls — easier to
// json.Unmarshal cleanly.
type rawEvent struct {
	IDEvent      string `json:"idEvent"`
	StrEvent     string `json:"strEvent"`
	StrHomeTeam  string `json:"strHomeTeam"`
	StrAwayTeam  string `json:"strAwayTeam"`
	StrSeason    string `json:"strSeason"`
	IntRound     string `json:"intRound"`
	StrLeague    string `json:"strLeague"`
	StrVenue     string `json:"strVenue"`
	StrTimestamp string `json:"strTimestamp"` // ISO 8601 UTC
	StrTime      string `json:"strTime"`      // HH:MM:SS UTC (fallback)
	DateEvent    string `json:"dateEvent"`    // YYYY-MM-DD UTC (fallback)
	StrTVStation string `json:"strTVStation"` // Free-text TV networks; comma- or "/"-separated
	IntDuration  string `json:"intDuration"`  // minutes; often empty
}

func (r rawEvent) normalize(league string) (Event, bool) {
	if r.IDEvent == "" {
		return Event{}, false
	}
	t, ok := r.kickoff()
	if !ok {
		// TheSportsDB occasionally returns events with date but no
		// time-of-day (rare; usually playoff TBD). We skip them rather
		// than guessing — Plex would render them at 00:00 UTC which is
		// useless.
		return Event{}, false
	}
	dur := 3 * time.Hour
	if r.IntDuration != "" {
		if mins, err := strconv.Atoi(r.IntDuration); err == nil && mins > 0 {
			dur = time.Duration(mins) * time.Minute
		}
	}
	return Event{
		ExternalID:  r.IDEvent,
		League:      strings.ToLower(league),
		HomeTeam:    r.StrHomeTeam,
		AwayTeam:    r.StrAwayTeam,
		KickoffAt:   t,
		Duration:    dur,
		Venue:       r.StrVenue,
		Broadcaster: r.StrTVStation,
		Season:      r.StrSeason,
		Round:       r.IntRound,
	}, true
}

func (r rawEvent) kickoff() (time.Time, bool) {
	if r.StrTimestamp != "" {
		// TheSportsDB returns ISO 8601 in UTC ("2024-12-25T17:30:00").
		// Some entries omit the trailing Z; we parse both forms.
		layouts := []string{
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, r.StrTimestamp); err == nil {
				return t.UTC(), true
			}
		}
	}
	// Fallback: dateEvent + strTime, both UTC per docs.
	if r.DateEvent != "" && r.StrTime != "" {
		layouts := []string{
			"2006-01-02 15:04:05",
			"2006-01-02 15:04",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, r.DateEvent+" "+r.StrTime); err == nil {
				return t.UTC(), true
			}
		}
	}
	return time.Time{}, false
}

var _ = errors.New // reserved for future error sentinels
