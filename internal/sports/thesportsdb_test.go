package sports

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRawEvent_Normalize_TimestampVariants(t *testing.T) {
	cases := []struct {
		name string
		raw  rawEvent
		want time.Time
		ok   bool
	}{
		{
			name: "ISO 8601 with Z",
			raw: rawEvent{
				IDEvent:      "1",
				StrTimestamp: "2026-01-15T17:30:00Z",
			},
			want: time.Date(2026, 1, 15, 17, 30, 0, 0, time.UTC),
			ok:   true,
		},
		{
			name: "ISO 8601 without Z",
			raw: rawEvent{
				IDEvent:      "2",
				StrTimestamp: "2026-01-15T17:30:00",
			},
			want: time.Date(2026, 1, 15, 17, 30, 0, 0, time.UTC),
			ok:   true,
		},
		{
			name: "fallback dateEvent + strTime",
			raw: rawEvent{
				IDEvent:   "3",
				DateEvent: "2026-01-15",
				StrTime:   "17:30:00",
			},
			want: time.Date(2026, 1, 15, 17, 30, 0, 0, time.UTC),
			ok:   true,
		},
		{
			name: "missing timestamps → skip",
			raw:  rawEvent{IDEvent: "4"},
			ok:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, ok := tc.raw.normalize("nfl")
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if !ev.KickoffAt.Equal(tc.want) {
				t.Errorf("kickoff got %v want %v", ev.KickoffAt, tc.want)
			}
			if ev.League != "nfl" {
				t.Errorf("league got %q want nfl", ev.League)
			}
		})
	}
}

func TestRawEvent_Normalize_DurationFromAPIFallsBackTo3h(t *testing.T) {
	// API duration empty → 3h default.
	ev, ok := rawEvent{
		IDEvent:      "x",
		StrTimestamp: "2026-01-15T17:30:00Z",
	}.normalize("nba")
	if !ok {
		t.Fatal("normalize")
	}
	if ev.Duration != 3*time.Hour {
		t.Errorf("default duration got %v want 3h", ev.Duration)
	}
	// API duration set → honoured.
	ev, _ = rawEvent{
		IDEvent:      "y",
		StrTimestamp: "2026-01-15T17:30:00Z",
		IntDuration:  "120",
	}.normalize("nba")
	if ev.Duration != 120*time.Minute {
		t.Errorf("honoured duration got %v want 120m", ev.Duration)
	}
}

// TestSeasonNext_HappyPath: serve a canned TheSportsDB response and
// confirm the client decodes events into the normalised shape.
func TestSeasonNext_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/eventsnextleague.php") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"events":[
			{"idEvent":"100","strHomeTeam":"Cowboys","strAwayTeam":"Eagles",
			 "strTimestamp":"2026-01-19T18:00:00Z","strSeason":"2025-2026",
			 "strVenue":"AT&T Stadium","strTVStation":"FOX, NFL Network",
			 "intDuration":"180"}
		]}`))
	}))
	defer srv.Close()

	c := NewClient("test-key")
	c.BaseURL = srv.URL
	events, err := c.SeasonNext(context.Background(), "nfl")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.HomeTeam != "Cowboys" || ev.AwayTeam != "Eagles" {
		t.Errorf("home/away got %q vs %q", ev.HomeTeam, ev.AwayTeam)
	}
	if ev.Broadcaster != "FOX, NFL Network" {
		t.Errorf("broadcaster got %q", ev.Broadcaster)
	}
	if ev.Duration != 3*time.Hour {
		t.Errorf("duration got %v want 3h", ev.Duration)
	}
}

func TestSeasonNext_UnknownLeague(t *testing.T) {
	c := NewClient("")
	if _, err := c.SeasonNext(context.Background(), "quidditch"); err == nil {
		t.Error("expected error for unknown league")
	}
}

func TestSeasonNext_NullEvents(t *testing.T) {
	// Off-season — TheSportsDB returns {"events": null}. Should
	// surface as zero events with no error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"events":null}`))
	}))
	defer srv.Close()
	c := NewClient("k")
	c.BaseURL = srv.URL
	events, err := c.SeasonNext(context.Background(), "mlb")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Errorf("want 0 events, got %d", len(events))
	}
}
