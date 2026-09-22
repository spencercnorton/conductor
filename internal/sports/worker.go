// worker.go — sports schedule sync + EPG injection.
//
// Runs on a configurable interval (default 30 min). Each pass:
//
//  1. Lists active channel→league mappings.
//  2. For each unique league, pulls upcoming events from the API and
//     upserts them into sports_event_cache.
//  3. For each channel with a mapping, picks the cached events that
//     match (broadcaster filter + optional team filter) and upserts
//     them as epg_program rows with source_hash="sports:<league>:<id>".
//
// The dedupe index on epg_program (channel_id, start_at, source_hash)
// makes step 3 idempotent — re-running the worker won't duplicate
// programmes, just refreshes cached metadata when the upstream
// kickoff/duration changes.
//
// is_live records that these are live-origin broadcasts. Output projects the
// separate Plex-visible currently-live cue from each stored [start_at,end_at)
// window, so the worker does not rewrite rows as wall-clock time advances.
package sports

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/store"
)

// Store is the subset of *store.DB the worker needs. Defined here so
// tests can stub it without the full DB.
type Store interface {
	ListChannelSportMappings(ctx context.Context) ([]store.ChannelSportMapping, error)
	UpsertSportsEvent(ctx context.Context, e store.SportsEvent) error
	ListSportsEventsForLeague(ctx context.Context, league string, from, to time.Time) ([]store.SportsEvent, error)
	UpsertEPGProgram(ctx context.Context, p store.EPGProgram) (string, error)
}

// Worker syncs sports schedules. Two fetchers run per pass:
//
//   - ESPN's public scoreboard API (rich daily slates, broadcaster
//     info, ~15-20 events per league per call)
//   - TheSportsDB (fallback, ~1 event per league on the free tier)
//
// Both write to the same `sports_event_cache` table so duplicate
// events land idempotently on (league, external_id). Worker uses
// "espn-<id>" or the raw TheSportsDB id as the external_id to keep
// the namespaces from clashing.
type Worker struct {
	Store    Store
	Client   *Client       // TheSportsDB fallback
	ESPN     *ESPNClient   // primary; nil disables (worker still works)
	Interval time.Duration // default 30 min
	Logger   *slog.Logger

	// LookAheadDays controls how far ahead we inject programmes. The
	// EPG output emits 14 days; we sync 7 to match the ESPN
	// per-day-call cost (lower = fewer HTTP calls per pass).
	LookAheadDays int

	// Monitor tracks pass success/failure for stall alerting +
	// /admin/epg/health. Optional; nil no-ops.
	Monitor *alerts.WorkerMonitor

	// Alerts posts source-level events (ESPN breaker open/recovered).
	// Optional; nil disables. Distinct from Monitor: a pass still counts
	// as successful while TheSportsDB answers, so a dark ESPN never trips
	// the stall alert — the 2026-08 Akamai 403 block ran for weeks with
	// nothing but per-day WARN lines.
	Alerts *alerts.Client

	// espnFails/espnRetryAt are the cross-pass circuit breaker for the
	// ESPN fetcher. A pass where every league's window fails trips it;
	// while open, passes skip ESPN entirely until espnRetryAt, then send
	// one cheap canary request instead of the full N-league × 8-day
	// fan-out. The cooldown doubles per consecutive trip, from Interval
	// up to espnMaxCooldown. Only touched from runOnce (single goroutine).
	espnFails   int
	espnRetryAt time.Time

	now func() time.Time // test seam; nil means time.Now
}

const (
	// espnMaxCooldown caps the breaker backoff: a hard block (Akamai 403)
	// still gets probed a few times a day, ~5 requests instead of the
	// ~2,700/day the un-broken loop produced against a dead endpoint.
	espnMaxCooldown = 6 * time.Hour
	// espnAlertAfterTrips: alert once the breaker has tripped this many
	// consecutive times (~1.5-2h continuously dark at a 30m interval) —
	// late enough to skip midnight-transition blips, early enough to see
	// a real block the same day.
	espnAlertAfterTrips = 3
)

func (w *Worker) clock() time.Time {
	if w.now == nil {
		return time.Now()
	}
	return w.now()
}

// New constructs a Worker with sensible defaults.
func New(s Store, c *Client, logger *slog.Logger) *Worker {
	espn := NewESPNClient()
	espn.Logger = logger
	return &Worker{
		Store:         s,
		Client:        c,
		ESPN:          espn,
		Interval:      30 * time.Minute,
		Logger:        logger,
		LookAheadDays: 7,
	}
}

// Run blocks until ctx is cancelled, ticking on Interval. First tick
// fires immediately so a fresh container syncs without waiting.
func (w *Worker) Run(ctx context.Context) {
	if w.Logger != nil {
		w.Logger.Info("sports worker starting", "interval", w.Interval, "look_ahead_days", w.LookAheadDays)
	}
	if w.Monitor != nil && w.Monitor.StallAfter <= 0 {
		w.Monitor.StallAfter = 3 * w.Interval
	}
	w.Monitor.Start()
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		w.runOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runOnce does one full sync pass. Errors per-league don't abort the
// pass — we log and move on so a single flaky league doesn't starve
// the others.
func (w *Worker) runOnce(ctx context.Context) {
	mappings, err := w.Store.ListChannelSportMappings(ctx)
	if err != nil {
		if w.Logger != nil {
			w.Logger.Error("sports list mappings", "err", err)
		}
		w.Monitor.Failure(ctx, "list mappings: "+err.Error())
		return
	}
	if len(mappings) == 0 {
		// No channels mapped → nothing to sync. Don't even hit the API.
		// Vacuously successful — an idle worker isn't a stalled worker.
		w.Monitor.Success(ctx)
		return
	}

	leagues := uniqueLeagues(mappings)

	// Step 1: fetch + cache events for each league, once per pass.
	// Two fetchers in series: ESPN (rich) then TheSportsDB (fallback).
	// Both upsert into the same cache table; ESPN events are namespaced
	// "espn-<id>" so they don't collide with TheSportsDB ids.
	//
	// A pass counts as successful when at least one fetcher answered for
	// at least one league — injected=0 with healthy fetchers is "no
	// matching games", not a stall (broadcaster-matching quality is
	// audit item E6, tracked separately).
	from := time.Now()
	to := time.Now().Add(time.Duration(w.LookAheadDays) * 24 * time.Hour)
	totalCached := 0
	fetchOK := 0
	var lastFetchErr error

	// ESPN circuit breaker: skip the fan-out while cooling down, and
	// re-enter through a single canary request rather than the full
	// N-league × per-day burst.
	espnEnabled := w.ESPN != nil
	if espnEnabled && w.espnRetryAt.After(w.clock()) {
		espnEnabled = false
		if w.Logger != nil {
			w.Logger.Warn("espn breaker open, skipping fetch",
				"consecutive_trips", w.espnFails,
				"retry_at", w.espnRetryAt.UTC().Format(time.RFC3339))
		}
	}
	if espnEnabled && w.espnFails > 0 {
		// The canary must be a league ESPN hosts — probing a
		// TheSportsDB-only league (pga, tennis) would fail forever and
		// wedge the breaker open.
		canary := ""
		for _, l := range leagues {
			if s, _ := espnPath(l); s != "" {
				canary = l
				break
			}
		}
		if canary == "" {
			espnEnabled = false // nothing ESPN-hosted mapped; nothing to probe
		} else if _, err := w.ESPN.Scoreboard(ctx, canary, time.Time{}); err != nil {
			w.tripESPN(ctx, err)
			espnEnabled = false
		}
	}

	espnOK, espnFailed := 0, 0
	var lastESPNErr error
	for _, league := range leagues {
		// Primary: ESPN scoreboard window.
		if espnEnabled {
			events, err := w.ESPN.ScoreboardWindow(ctx, league, from, to)
			if err != nil {
				espnFailed++
				lastESPNErr = err
				lastFetchErr = err
				if w.Logger != nil {
					w.Logger.Warn("espn fetch", "league", league, "err", err)
				}
			} else {
				espnOK++
				fetchOK++
			}
			for _, ev := range events {
				totalCached += w.cacheEvent(ctx, league, ev)
			}
		}
		// Fallback: TheSportsDB.
		events, err := w.Client.SeasonNext(ctx, league)
		if err != nil {
			lastFetchErr = err
			if w.Logger != nil {
				w.Logger.Warn("sports api fetch", "league", league, "err", err)
			}
			continue
		}
		fetchOK++
		for _, ev := range events {
			totalCached += w.cacheEvent(ctx, league, ev)
		}
	}

	if espnEnabled {
		switch {
		case espnOK > 0:
			w.resetESPN(ctx)
		case espnFailed > 0:
			w.tripESPN(ctx, lastESPNErr)
		}
	}

	if fetchOK == 0 {
		detail := "all sports fetchers failed"
		if lastFetchErr != nil {
			detail += ": " + lastFetchErr.Error()
		}
		w.Monitor.Failure(ctx, detail)
	} else {
		w.Monitor.Success(ctx)
	}

	// Step 2: emit epg_program rows per (channel, mapping). Group
	// mappings by league so each cached lookup is one DB call.
	// `from` and `to` were computed above for the fetch window —
	// reuse them for the injection window so we don't pull events
	// that just expired.
	totalInjected := 0
	for _, league := range leagues {
		events, err := w.Store.ListSportsEventsForLeague(ctx, league, from, to)
		if err != nil {
			if w.Logger != nil {
				w.Logger.Warn("sports cached lookup", "league", league, "err", err)
			}
			continue
		}
		for _, m := range mappings {
			if m.League != league {
				continue
			}
			for _, ev := range events {
				if !mappingMatchesEvent(m, ev) {
					continue
				}
				prog := buildEPGProgram(m.ChannelID, ev)
				if _, err := w.Store.UpsertEPGProgram(ctx, prog); err != nil {
					if w.Logger != nil {
						w.Logger.Warn("sports epg upsert", "channel", m.ChannelID, "ev", ev.ExternalID, "err", err)
					}
					continue
				}
				totalInjected++
			}
		}
	}

	if w.Logger != nil {
		w.Logger.Info("sports sync pass complete",
			"leagues", len(leagues),
			"mappings", len(mappings),
			"cached", totalCached,
			"injected", totalInjected,
			"espn_breaker_open", w.espnRetryAt.After(w.clock()))
	}
}

// tripESPN records one more consecutive ESPN-fully-failed pass and extends
// the breaker cooldown (Interval doubling per trip, capped). One alert at
// the espnAlertAfterTrips crossing; resetESPN sends the matching recovery.
func (w *Worker) tripESPN(ctx context.Context, err error) {
	w.espnFails++
	cooldown := w.Interval
	if cooldown <= 0 {
		cooldown = 30 * time.Minute
	}
	for i := 1; i < w.espnFails && cooldown < espnMaxCooldown; i++ {
		cooldown *= 2
	}
	if cooldown > espnMaxCooldown {
		cooldown = espnMaxCooldown
	}
	w.espnRetryAt = w.clock().Add(cooldown)
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	if w.Logger != nil {
		w.Logger.Warn("espn breaker tripped",
			"consecutive_trips", w.espnFails,
			"cooldown", cooldown.String(),
			"retry_at", w.espnRetryAt.UTC().Format(time.RFC3339),
			"err", errText)
	}
	if w.espnFails == espnAlertAfterTrips && w.Alerts != nil {
		w.Alerts.Send(ctx, alerts.EPGSourceFailing("espn scoreboard", w.espnFails, errText))
	}
}

// resetESPN closes the breaker after a successful ESPN fetch.
func (w *Worker) resetESPN(ctx context.Context) {
	if w.espnFails == 0 {
		return
	}
	trips := w.espnFails
	w.espnFails = 0
	w.espnRetryAt = time.Time{}
	if w.Logger != nil {
		w.Logger.Info("espn breaker closed, fetcher recovered", "after_trips", trips)
	}
	if trips >= espnAlertAfterTrips && w.Alerts != nil {
		w.Alerts.Send(ctx, alerts.EPGSourceRecovered("espn scoreboard", trips))
	}
}

// cacheEvent upserts one Event into sports_event_cache. Returns 1 on
// successful write, 0 on error (logged). Pulled out so both ESPN and
// TheSportsDB code paths share the same "convert + upsert" boilerplate.
func (w *Worker) cacheEvent(ctx context.Context, league string, ev Event) int {
	row := store.SportsEvent{
		League:      ev.League,
		ExternalID:  ev.ExternalID,
		HomeTeam:    ev.HomeTeam,
		AwayTeam:    ev.AwayTeam,
		KickoffAt:   ev.KickoffAt,
		DurationMin: int(ev.Duration / time.Minute),
		Venue:       ev.Venue,
		Broadcaster: ev.Broadcaster,
	}
	if err := w.Store.UpsertSportsEvent(ctx, row); err != nil {
		if w.Logger != nil {
			w.Logger.Warn("sports cache upsert", "league", league, "id", ev.ExternalID, "err", err)
		}
		return 0
	}
	return 1
}

// uniqueLeagues collapses the mapping list to a deduplicated league
// slice while preserving stable order (helps reproducible logs).
func uniqueLeagues(mappings []store.ChannelSportMapping) []string {
	seen := make(map[string]struct{}, len(mappings))
	out := make([]string, 0, len(mappings))
	for _, m := range mappings {
		if _, ok := seen[m.League]; ok {
			continue
		}
		seen[m.League] = struct{}{}
		out = append(out, m.League)
	}
	return out
}

// mappingMatchesEvent applies the optional team / broadcaster filters
// from a mapping against one cached event. Empty filters match
// anything (the common case for "league-wide" channels like NFL
// Network or NBA TV).
func mappingMatchesEvent(m store.ChannelSportMapping, ev store.SportsEvent) bool {
	if m.TeamName != "" {
		t := strings.ToLower(m.TeamName)
		if !strings.Contains(strings.ToLower(ev.HomeTeam), t) &&
			!strings.Contains(strings.ToLower(ev.AwayTeam), t) {
			return false
		}
	}
	if m.BroadcasterFilter != "" {
		f := strings.ToLower(m.BroadcasterFilter)
		if !strings.Contains(strings.ToLower(ev.Broadcaster), f) {
			return false
		}
	}
	return true
}

// buildEPGProgram synthesises the epg_program row for one
// (channel, event) pair.
//
// Conventions:
//   - title: "{Away} @ {Home}" (American sports convention)
//   - sub_title: round/week info if present in extras (later phase)
//   - description: venue + broadcaster
//   - category: ["Sports", <league as Title-cased>]
//   - is_live: true as live-origin metadata. The visible currently-live cue is
//     projected at output time without rewriting the stored row.
//
// The source_hash is "sports:<league>:<external_id>" so the dedupe
// index in epg_program (channel_id, start_at, source_hash) lets re-
// syncs upsert in place (when kickoff time changes) and keeps these
// synthetic rows from clashing with dispatcharr/SD ingest.
func buildEPGProgram(channelID uuid.UUID, ev store.SportsEvent) store.EPGProgram {
	title := matchupTitle(ev)
	desc := buildDescription(ev)
	cat := []string{"Sports", titleCase(ev.League)}
	dur := time.Duration(ev.DurationMin) * time.Minute
	if dur <= 0 {
		dur = 3 * time.Hour
	}
	return store.EPGProgram{
		ChannelID:   channelID,
		StartAt:     ev.KickoffAt,
		EndAt:       ev.KickoffAt.Add(dur),
		Title:       title,
		Description: desc,
		Category:    cat,
		// Sports events ARE live by nature; mark them so EPG output
		// can retain <live/> for compatible clients. Plex's separate visible
		// cue is projected only inside the stored airing window.
		IsLive: true,
		// Title in the hash: a TBD placeholder upgrading to the real
		// matchup is a content change the upsert's IS DISTINCT guard
		// must see, or the row never refreshes in place.
		SourceHash:     fmt.Sprintf("sports:%s:%s:%s", ev.League, ev.ExternalID, title),
		SourcePriority: store.PrioritySports,
	}
}

// matchupTitle prefers the "Away @ Home" format. Falls back to the
// home team alone (or vice versa) when one side is empty (rare;
// usually the case is a tournament with a single named participant
// like a F1 Grand Prix).
func matchupTitle(ev store.SportsEvent) string {
	switch {
	case ev.HomeTeam != "" && ev.AwayTeam != "":
		return ev.AwayTeam + " @ " + ev.HomeTeam
	case ev.HomeTeam != "":
		return ev.HomeTeam
	case ev.AwayTeam != "":
		return ev.AwayTeam
	}
	return titleCase(ev.League) + " Event"
}

func buildDescription(ev store.SportsEvent) string {
	parts := []string{}
	if ev.Venue != "" {
		parts = append(parts, ev.Venue)
	}
	if ev.Broadcaster != "" {
		parts = append(parts, "Broadcast: "+ev.Broadcaster)
	}
	return strings.Join(parts, " — ")
}

// titleCase converts league key (e.g. "nfl") to display ("NFL"). Most
// of our leagues are acronyms; we uppercase those and lowercase-with-
// initial-caps for the rest.
func titleCase(league string) string {
	switch strings.ToLower(league) {
	case "nfl", "nba", "nhl", "mlb", "mls", "ncaaf", "ncaab",
		"epl", "f1", "pga", "ufc":
		return strings.ToUpper(league)
	}
	if league == "" {
		return ""
	}
	return strings.ToUpper(league[:1]) + strings.ToLower(league[1:])
}
