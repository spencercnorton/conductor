package sports

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/store"
)

// fakeStore is an in-memory implementation of the Store interface,
// good enough for verifying the worker's pass logic without spinning
// up Postgres.
type fakeStore struct {
	mu       sync.Mutex
	mappings []store.ChannelSportMapping
	events   map[string]map[string]store.SportsEvent // league → external_id → event
	programs []store.EPGProgram
}

func newFakeStore() *fakeStore {
	return &fakeStore{events: map[string]map[string]store.SportsEvent{}}
}

func (f *fakeStore) ListChannelSportMappings(ctx context.Context) ([]store.ChannelSportMapping, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.ChannelSportMapping, len(f.mappings))
	copy(out, f.mappings)
	return out, nil
}

func (f *fakeStore) UpsertSportsEvent(ctx context.Context, e store.SportsEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.events[e.League] == nil {
		f.events[e.League] = map[string]store.SportsEvent{}
	}
	f.events[e.League][e.ExternalID] = e
	return nil
}

func (f *fakeStore) ListSportsEventsForLeague(ctx context.Context, league string, from, to time.Time) ([]store.SportsEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.SportsEvent
	for _, ev := range f.events[league] {
		if ev.KickoffAt.Before(from) || ev.KickoffAt.After(to) {
			continue
		}
		out = append(out, ev)
	}
	return out, nil
}

func (f *fakeStore) UpsertEPGProgram(ctx context.Context, p store.EPGProgram) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.programs = append(f.programs, p)
	return "added", nil
}

// TestMappingMatchesEvent: filter logic is the heart of the routing
// — wrong filter = wrong channel.
func TestMappingMatchesEvent(t *testing.T) {
	cases := []struct {
		name string
		m    store.ChannelSportMapping
		ev   store.SportsEvent
		want bool
	}{
		{
			name: "no filters → match",
			m:    store.ChannelSportMapping{League: "nfl"},
			ev:   store.SportsEvent{HomeTeam: "Eagles", AwayTeam: "Cowboys"},
			want: true,
		},
		{
			name: "team filter matches home",
			m:    store.ChannelSportMapping{TeamName: "Eagles"},
			ev:   store.SportsEvent{HomeTeam: "Philadelphia Eagles"},
			want: true,
		},
		{
			name: "team filter matches away (case-insensitive)",
			m:    store.ChannelSportMapping{TeamName: "cowboys"},
			ev:   store.SportsEvent{HomeTeam: "Eagles", AwayTeam: "Dallas Cowboys"},
			want: true,
		},
		{
			name: "team filter does not match",
			m:    store.ChannelSportMapping{TeamName: "Steelers"},
			ev:   store.SportsEvent{HomeTeam: "Eagles", AwayTeam: "Cowboys"},
			want: false,
		},
		{
			name: "broadcaster filter matches",
			m:    store.ChannelSportMapping{BroadcasterFilter: "ESPN"},
			ev:   store.SportsEvent{Broadcaster: "ESPN, ESPN+"},
			want: true,
		},
		{
			name: "broadcaster filter no match",
			m:    store.ChannelSportMapping{BroadcasterFilter: "FS1"},
			ev:   store.SportsEvent{Broadcaster: "ESPN"},
			want: false,
		},
		{
			name: "all filters combined match",
			m: store.ChannelSportMapping{
				TeamName:          "Eagles",
				BroadcasterFilter: "ESPN",
			},
			ev: store.SportsEvent{
				HomeTeam:    "Eagles",
				AwayTeam:    "Cowboys",
				Broadcaster: "ESPN, NFL Network",
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mappingMatchesEvent(tc.m, tc.ev)
			if got != tc.want {
				t.Errorf("got %v want %v", got, tc.want)
			}
		})
	}
}

// TestBuildEPGProgram_TitleAndCategory: synthesised programmes have
// the expected matchup title + Sports/<League> category list +
// is_live=true so the EPG output emits <live/>.
func TestBuildEPGProgram_TitleAndCategory(t *testing.T) {
	chID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ev := store.SportsEvent{
		League:      "nfl",
		ExternalID:  "12345",
		HomeTeam:    "Cowboys",
		AwayTeam:    "Eagles",
		KickoffAt:   time.Date(2026, 1, 19, 18, 0, 0, 0, time.UTC),
		DurationMin: 180,
		Broadcaster: "FOX",
	}
	prog := buildEPGProgram(chID, ev)
	if prog.Title != "Eagles @ Cowboys" {
		t.Errorf("title got %q want 'Eagles @ Cowboys'", prog.Title)
	}
	if !prog.IsLive {
		t.Error("expected IsLive=true on synthesised sports row")
	}
	if prog.SourceHash != "sports:nfl:12345:Eagles @ Cowboys" {
		t.Errorf("source_hash got %q want 'sports:nfl:12345:Eagles @ Cowboys'", prog.SourceHash)
	}
	if prog.SourcePriority != store.PrioritySports {
		t.Errorf("source_priority got %d want %d", prog.SourcePriority, store.PrioritySports)
	}
	if len(prog.Category) < 2 || prog.Category[0] != "Sports" || prog.Category[1] != "NFL" {
		t.Errorf("category got %v want [Sports NFL]", prog.Category)
	}
	if !prog.EndAt.Equal(prog.StartAt.Add(180 * time.Minute)) {
		t.Errorf("end_at got %v want start+3h", prog.EndAt)
	}
}

// TestRunOnce_E2E: with one mapping + one cached event, the worker
// should fetch nothing (we provide a nil client), route the event to
// the mapped channel.
//
// Edge case: empty mapping list short-circuits without API calls.
func TestRunOnce_NoMappings_Skips(t *testing.T) {
	fs := newFakeStore()
	w := New(fs, NewClient(""), nil)
	w.runOnce(context.Background())
	if len(fs.programs) != 0 {
		t.Errorf("expected no programs, got %d", len(fs.programs))
	}
}

// TestRunOnce_RoutingWithCachedEvent: pre-load a cached event +
// matching mapping, then call runOnce. The fake store's
// ListChannelSportMappings + UpsertSportsEvent are both wired, but the
// API call would fail — we use it as a smoke test that the worker
// gracefully skips API errors and proceeds to inject from cache.
//
// The injected program should have the matchup title + correct
// channel id + sports source_hash.
func TestRunOnce_RoutingWithCachedEvent(t *testing.T) {
	fs := newFakeStore()
	chID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	fs.mappings = []store.ChannelSportMapping{
		{
			ID:                uuid.New(),
			ChannelID:         chID,
			League:            "nfl",
			BroadcasterFilter: "FOX",
			Enabled:           true,
		},
	}
	fs.events["nfl"] = map[string]store.SportsEvent{
		"42": {
			League:      "nfl",
			ExternalID:  "42",
			HomeTeam:    "Cowboys",
			AwayTeam:    "Giants",
			KickoffAt:   time.Now().Add(48 * time.Hour),
			DurationMin: 180,
			Broadcaster: "FOX, NFL Network",
		},
	}
	// Use a client that points at an unreachable URL so SeasonNext
	// errors out — the worker should still inject from cache.
	c := NewClient("")
	c.BaseURL = "http://127.0.0.1:1" // closed port → fast fail
	c.HTTP.Timeout = 100 * time.Millisecond
	w := New(fs, c, nil)
	// New() wires a real ESPN client at the live API; this test is about
	// routing from cache, and until 2026-08-29 it only stayed hermetic
	// because ESPN happened to reject the fetch. Disable it explicitly.
	w.ESPN = nil
	w.LookAheadDays = 14
	w.runOnce(context.Background())

	if len(fs.programs) != 1 {
		t.Fatalf("expected 1 injected program, got %d", len(fs.programs))
	}
	p := fs.programs[0]
	if p.ChannelID != chID {
		t.Errorf("channel id got %v want %v", p.ChannelID, chID)
	}
	if p.SourceHash != "sports:nfl:42:Giants @ Cowboys" {
		t.Errorf("source_hash got %q want sports:nfl:42:Giants @ Cowboys", p.SourceHash)
	}
}

// ── WorkerMonitor wiring (audit 2026-06-09, E5) ─────────────────────────────

// TestRunOnce_AllFetchersFail_MarksMonitorFailure: both fetchers dead
// (ESPN disabled, TheSportsDB at a closed port) → the pass records a
// monitor failure, so a permanently-failed worker alerts instead of
// just logging.
func TestRunOnce_AllFetchersFail_MarksMonitorFailure(t *testing.T) {
	fs := newFakeStore()
	fs.mappings = []store.ChannelSportMapping{
		{ID: uuid.New(), ChannelID: uuid.New(), League: "nfl", Enabled: true},
	}
	c := NewClient("")
	c.BaseURL = "http://127.0.0.1:1" // closed port → fast fail
	c.HTTP.Timeout = 100 * time.Millisecond
	w := New(fs, c, nil)
	w.ESPN = nil
	w.Monitor = alerts.NewWorkerMonitor("sports", time.Nanosecond, nil)
	w.Monitor.Start()
	time.Sleep(time.Millisecond) // ensure the stall deadline has elapsed

	w.runOnce(context.Background())

	snap := w.Monitor.Snapshot()
	if !snap.Stalled {
		t.Fatalf("expected stalled monitor, got %+v", snap)
	}
}

// TestRunOnce_NoMappings_MarksMonitorSuccess: an idle worker (nothing
// mapped) is healthy, not stalled.
func TestRunOnce_NoMappings_MarksMonitorSuccess(t *testing.T) {
	fs := newFakeStore()
	w := New(fs, NewClient(""), nil)
	w.ESPN = nil
	w.Monitor = alerts.NewWorkerMonitor("sports", time.Nanosecond, nil)
	w.Monitor.Start()

	w.runOnce(context.Background())

	snap := w.Monitor.Snapshot()
	if snap.Stalled {
		t.Fatalf("idle worker must not stall: %+v", snap)
	}
}

// TestRunOnce_OneFetcherAnswers_MarksMonitorSuccess: TheSportsDB
// answering (even with zero events) keeps the worker healthy.
func TestRunOnce_OneFetcherAnswers_MarksMonitorSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events": []}`))
	}))
	defer srv.Close()

	fs := newFakeStore()
	fs.mappings = []store.ChannelSportMapping{
		{ID: uuid.New(), ChannelID: uuid.New(), League: "nfl", Enabled: true},
	}
	c := NewClient("")
	c.BaseURL = srv.URL
	w := New(fs, c, nil)
	w.ESPN = nil
	w.Monitor = alerts.NewWorkerMonitor("sports", time.Nanosecond, nil)
	w.Monitor.Start()
	time.Sleep(time.Millisecond)

	w.runOnce(context.Background())

	snap := w.Monitor.Snapshot()
	if snap.Stalled {
		t.Fatalf("answering fetcher must keep worker healthy: %+v", snap)
	}
}

// TestRunOnce_ESPNBreakerLifecycle: a fully-failed ESPN pass opens the
// breaker (later passes make ZERO ESPN calls), an expired cooldown
// re-probes with exactly one canary request (a failed canary doubles the
// cooldown), and a passing canary closes the breaker and resumes the full
// fan-out. Guards the 2026-08 Akamai-403 retry storm fix: without the
// breaker a hard-blocked endpoint ate ~56 requests per pass, every pass.
func TestRunOnce_ESPNBreakerLifecycle(t *testing.T) {
	var espnHits atomic.Int32
	var espnBlocked atomic.Bool
	espnBlocked.Store(true)
	espnSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		espnHits.Add(1)
		if espnBlocked.Load() {
			http.Error(w, "Access Denied", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events": []}`))
	}))
	defer espnSrv.Close()
	sdbSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events": []}`))
	}))
	defer sdbSrv.Close()

	fs := newFakeStore()
	fs.mappings = []store.ChannelSportMapping{
		{ID: uuid.New(), ChannelID: uuid.New(), League: "nfl", Enabled: true},
		{ID: uuid.New(), ChannelID: uuid.New(), League: "nhl", Enabled: true},
	}
	c := NewClient("")
	c.BaseURL = sdbSrv.URL
	w := New(fs, c, nil)
	w.ESPN.BaseURL = espnSrv.URL

	cur := time.Now()
	w.now = func() time.Time { return cur }
	ctx := context.Background()

	// Pass 1: ESPN hard-403s every day of every league → breaker trips.
	w.runOnce(ctx)
	afterFirst := espnHits.Load()
	if afterFirst == 0 {
		t.Fatal("first pass should have hit ESPN")
	}
	if w.espnFails != 1 {
		t.Fatalf("espnFails = %d after first failed pass, want 1", w.espnFails)
	}

	// Pass 2, cooldown not elapsed: zero ESPN requests.
	w.runOnce(ctx)
	if got := espnHits.Load(); got != afterFirst {
		t.Fatalf("open breaker still made %d ESPN calls", got-afterFirst)
	}

	// Pass 3, cooldown elapsed, still blocked: exactly one canary,
	// cooldown doubles.
	cur = w.espnRetryAt.Add(time.Second)
	w.runOnce(ctx)
	if got := espnHits.Load(); got != afterFirst+1 {
		t.Fatalf("half-open pass made %d ESPN calls, want exactly 1 canary", got-afterFirst)
	}
	if w.espnFails != 2 {
		t.Fatalf("espnFails = %d after failed canary, want 2", w.espnFails)
	}
	if got, want := w.espnRetryAt.Sub(cur), 2*w.Interval; got != want {
		t.Fatalf("second cooldown = %v, want %v", got, want)
	}

	// Pass 4, cooldown elapsed, ESPN back: canary passes, the full
	// fan-out resumes, and the breaker closes.
	espnBlocked.Store(false)
	cur = w.espnRetryAt.Add(time.Second)
	before := espnHits.Load()
	w.runOnce(ctx)
	if got := espnHits.Load() - before; got <= 1 {
		t.Fatalf("recovered pass made %d ESPN calls, want canary + full window", got)
	}
	if w.espnFails != 0 || !w.espnRetryAt.IsZero() {
		t.Fatalf("breaker not reset: fails=%d retryAt=%v", w.espnFails, w.espnRetryAt)
	}
}
