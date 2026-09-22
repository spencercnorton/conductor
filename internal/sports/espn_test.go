package sports

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestESPNPath(t *testing.T) {
	cases := []struct {
		league, sport, eleague string
	}{
		{"nfl", "football", "nfl"},
		{"nba", "basketball", "nba"},
		{"nhl", "hockey", "nhl"},
		{"mlb", "baseball", "mlb"},
		{"ncaaf", "football", "college-football"},
		{"epl", "soccer", "eng.1"},
		{"ufc", "mma", "ufc"},
		{"unknown-league", "", ""},
	}
	for _, c := range cases {
		s, l := espnPath(c.league)
		if s != c.sport || l != c.eleague {
			t.Errorf("espnPath(%q) = (%q, %q); want (%q, %q)",
				c.league, s, l, c.sport, c.eleague)
		}
	}
}

// TestScoreboard_HappyPath: stand up a fake ESPN endpoint, return a
// canned event, confirm the client decodes it.
func TestScoreboard_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/hockey/nhl/scoreboard") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		// Akamai serves this UA a 403 block page unless an Accept header
		// is present (measured 2026-08-29) — a regression here re-darkens
		// the whole fetcher.
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept header = %q, want application/json", got)
		}
		if ua := r.Header.Get("User-Agent"); !strings.Contains(ua, "Conductor") {
			t.Errorf("User-Agent = %q, want the honest Conductor UA", ua)
		}
		_, _ = w.Write([]byte(`{
			"events": [
				{
					"id": "401746234",
					"date": "2026-05-08T23:00Z",
					"name": "Carolina Hurricanes at Buffalo Sabres",
					"competitions": [{
						"date": "2026-05-08T23:00Z",
						"venue": {"fullName": "KeyBank Center"},
						"broadcasts": [{"names": ["TNT", "HBO Max"]}],
						"competitors": [
							{"homeAway": "home", "team": {"displayName": "Buffalo Sabres"}},
							{"homeAway": "away", "team": {"displayName": "Carolina Hurricanes"}}
						]
					}]
				}
			]
		}`))
	}))
	defer srv.Close()

	c := NewESPNClient()
	c.BaseURL = srv.URL
	events, err := c.Scoreboard(context.Background(), "nhl", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.HomeTeam != "Buffalo Sabres" {
		t.Errorf("home got %q", ev.HomeTeam)
	}
	if ev.AwayTeam != "Carolina Hurricanes" {
		t.Errorf("away got %q", ev.AwayTeam)
	}
	if ev.Broadcaster != "TNT, HBO Max" {
		t.Errorf("broadcaster got %q", ev.Broadcaster)
	}
	if ev.Venue != "KeyBank Center" {
		t.Errorf("venue got %q", ev.Venue)
	}
	if ev.ExternalID != "espn-401746234" {
		t.Errorf("external id got %q (must be espn-prefixed to avoid clash with TheSportsDB)", ev.ExternalID)
	}
	if !ev.KickoffAt.Equal(time.Date(2026, 5, 8, 23, 0, 0, 0, time.UTC)) {
		t.Errorf("kickoff got %v", ev.KickoffAt)
	}
}

// TestScoreboard_UnknownLeague: ESPN doesn't carry every league we
// model — return a useful error so callers can fall back to
// TheSportsDB.
func TestScoreboard_UnknownLeague(t *testing.T) {
	c := NewESPNClient()
	if _, err := c.Scoreboard(context.Background(), "quidditch", time.Time{}); err == nil {
		t.Error("expected error for unknown league")
	}
}

// TestScoreboardWindow_DedupAcrossDays: when two consecutive day
// fetches return the same event id (rare but possible during ESPN's
// midnight transitions) the worker should dedup so we don't double-
// inject.
func TestScoreboardWindow_DedupAcrossDays(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{
			"events": [
				{"id": "1", "date": "2026-05-08T23:00Z",
				 "name": "A vs B",
				 "competitions": [{"date":"2026-05-08T23:00Z",
				   "competitors":[
				     {"homeAway":"home","team":{"displayName":"B"}},
				     {"homeAway":"away","team":{"displayName":"A"}}
				   ]}]}
			]
		}`))
	}))
	defer srv.Close()

	c := NewESPNClient()
	c.BaseURL = srv.URL
	from := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC) // 2 day windows
	events, err := c.ScoreboardWindow(context.Background(), "nhl", from, to)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("expected 2 day calls, got %d", calls)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 deduped event, got %d", len(events))
	}
}

// TestNormalizeESPNTime_AddsSeconds: ESPN often returns
// "2026-05-08T23:00Z" missing seconds; normalizeESPNTime fixes it.
func TestNormalizeESPNTime_AddsSeconds(t *testing.T) {
	cases := []struct{ in, want string }{
		{"2026-05-08T23:00Z", "2026-05-08T23:00:00Z"},
		{"2026-05-08T23:00:00Z", "2026-05-08T23:00:00Z"}, // already RFC3339
		{"2026-05-08T23:00:00+00:00", "2026-05-08T23:00:00+00:00"},
	}
	for _, c := range cases {
		got := normalizeESPNTime(c.in)
		if got != c.want {
			t.Errorf("normalizeESPNTime(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestScoreboardWindow_AllDaysFailed_ReturnsError: a fetcher that fails for
// every day in the window must surface an error (so the worker's monitor
// records a failed pass) instead of returning an innocuous empty slice —
// the 2026-06 DNS-block outage ran 3 days silently because of that (E6).
func TestScoreboardWindow_AllDaysFailed_ReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer srv.Close()

	c := NewESPNClient()
	c.BaseURL = srv.URL
	c.RetryBackoff = time.Millisecond // keep retries fast in tests
	from := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 12, 0, 0, 0, 0, time.UTC)
	if _, err := c.ScoreboardWindow(context.Background(), "nba", from, to); err == nil {
		t.Fatal("expected error when every day fails")
	}
}

// TestScoreboardWindow_PartialDayFailure_StillReturnsEvents: one bad day in
// an otherwise-healthy window keeps the partial results and no error.
func TestScoreboardWindow_PartialDayFailure_StillReturnsEvents(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "transition wobble", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{
			"events": [
				{"id": "9", "date": "2026-06-11T23:00Z",
				 "name": "A vs B",
				 "competitions": [{"date":"2026-06-11T23:00Z",
				   "broadcasts":[{"names":["ABC"]}],
				   "competitors":[
				     {"homeAway":"home","team":{"displayName":"B"}},
				     {"homeAway":"away","team":{"displayName":"A"}}
				   ]}]}
			]
		}`))
	}))
	defer srv.Close()

	c := NewESPNClient()
	c.BaseURL = srv.URL
	c.Retries = 1 // disable extra retries so day-1 fails as intended (not retried into the day-2 success)
	from := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)
	events, err := c.ScoreboardWindow(context.Background(), "nba", from, to)
	if err != nil {
		t.Fatalf("partial failure must not error: %v", err)
	}
	if len(events) != 1 || events[0].Broadcaster != "ABC" {
		t.Fatalf("expected 1 event with ABC broadcaster, got %+v", events)
	}
}

// TestFetchEvents_RetriesTransient: a 5xx on the first attempt is
// retried and the second-attempt success is returned. Confirms the
// in-pass retry budget actually recovers a transient ESPN edge wobble
// (the failure mode behind audit E1) instead of failing the whole day.
func TestFetchEvents_RetriesTransient(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "edge stall", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{
			"events": [
				{"id": "42", "date": "2026-06-10T23:00Z",
				 "name": "A vs B",
				 "competitions": [{"date":"2026-06-10T23:00Z",
				   "broadcasts":[{"names":["ESPN"]}],
				   "competitors":[
				     {"homeAway":"home","team":{"displayName":"B"}},
				     {"homeAway":"away","team":{"displayName":"A"}}
				   ]}]}
			]
		}`))
	}))
	defer srv.Close()

	c := NewESPNClient()
	c.BaseURL = srv.URL
	c.RetryBackoff = time.Millisecond
	events, err := c.Scoreboard(context.Background(), "nba", time.Time{})
	if err != nil {
		t.Fatalf("transient 5xx should be retried into success: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls (1 fail + 1 retry), got %d", calls)
	}
	if len(events) != 1 || events[0].Broadcaster != "ESPN" {
		t.Fatalf("expected 1 event from retry, got %+v", events)
	}
}

// TestFetchEvents_NoRetryOn4xx: a 4xx is permanent — give up immediately
// without burning the retry budget.
func TestFetchEvents_NoRetryOn4xx(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewESPNClient()
	c.BaseURL = srv.URL
	c.RetryBackoff = time.Millisecond
	if _, err := c.Scoreboard(context.Background(), "nba", time.Time{}); err == nil {
		t.Fatal("expected error on 404")
	}
	if calls != 1 {
		t.Fatalf("4xx must not be retried; expected 1 call, got %d", calls)
	}
}
