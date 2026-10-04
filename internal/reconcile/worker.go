package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/store"
)

// Worker is the background goroutine that reconciles Sonarr/Radarr
// wanted-lists against the upcoming EPG and schedules DVR recordings for
// matches. It produces dvr_recording rows; the existing dvr.Scheduler/
// Recorder pick them up unchanged.
type Worker struct {
	Logger *slog.Logger
	DB     *store.DB

	// Either may be nil — the worker reconciles whichever *arr is wired.
	Sonarr *SonarrClient
	Radarr *RadarrClient

	// Plex is optional. When set, the worker also runs the owned-but-
	// incomplete gap diff: episodes of series you own in Plex but are
	// missing (and Sonarr isn't covering). Gap matches are preview-only
	// unless GapAutoRecord is set.
	Plex          *PlexClient
	GapAutoRecord bool

	Interval    time.Duration // how often to reconcile (default 1h)
	Window      time.Duration // how far ahead to look in the EPG (default 14d)
	Threshold   float64       // min confidence to auto-record (default 0.80)
	MaxPerCycle int           // cap on auto-records scheduled per cycle (default 20)
	DryRun      bool          // when true, log would-record matches; insert nothing
	OutputDir   string        // CONDUCTOR_DVR_OUTPUT_DIR — recording root
	Priority    int           // lower wins DVR capacity conflicts
	// AdmissionPolicy keeps schedule-time conflict metadata aligned with the
	// scheduler's reserve and recorder padding. nil uses store defaults.
	AdmissionPolicy *store.DVRAdmissionPolicy

	// unwantedSeen counts consecutive cycles each scheduled row has gone
	// unclaimed by any want (see cancelUnwanted). Touched only by reconcile.
	unwantedSeen map[uuid.UUID]int
}

// cancelGuard keeps cancellation clear of the scheduler, which looks about a
// minute ahead: a recording this close to its start is left alone.
const cancelGuard = 15 * time.Minute

// cancelConfirmCycles is how many consecutive cycles a scheduled row must go
// unclaimed before it is cancelled, so one short or glitched wanted-list read
// cannot drop a recording that is still wanted. A cancelled airing is never
// booked again (its exact-airing key stays taken), so this errs on keeping.
const cancelConfirmCycles = 2

// eligible reports whether a match should be auto-recorded: confidence at or
// above the threshold, and — for Plex gap matches, which are inferred rather
// than monitored — only when gap-autorecord is explicitly enabled.
func (w *Worker) eligible(m Match) bool {
	if m.Source == "plex-gap" {
		// Gap matches are inferred (owned-but-missing, no *arr id) and are
		// pinned at confPlexGap below the global threshold, so gating them on
		// Threshold made GapAutoRecord unreachable: enabling it changed
		// nothing (D3, 2026-08-29). Opting in IS accepting the gap class at
		// its own confidence.
		return w.GapAutoRecord && m.Confidence >= confPlexGap
	}
	return m.Confidence >= w.Threshold
}

// Run blocks until ctx is cancelled, ticking immediately then every Interval.
func (w *Worker) Run(ctx context.Context) {
	w.applyDefaults()
	w.Logger.Info("dvr reconciler starting",
		"interval", w.Interval, "window", w.Window,
		"threshold", w.Threshold, "max_per_cycle", w.MaxPerCycle,
		"dry_run", w.DryRun,
		"sonarr", w.Sonarr != nil, "radarr", w.Radarr != nil,
		"plex_gap", w.Plex != nil, "gap_autorecord", w.GapAutoRecord)

	t := time.NewTicker(w.Interval)
	defer t.Stop()
	w.reconcile(ctx)
	for {
		select {
		case <-ctx.Done():
			w.Logger.Info("dvr reconciler stopping")
			return
		case <-t.C:
			w.reconcile(ctx)
		}
	}
}

func (w *Worker) applyDefaults() {
	if w.Interval <= 0 {
		w.Interval = time.Hour
	}
	if w.Window <= 0 {
		w.Window = 14 * 24 * time.Hour
	}
	if w.Threshold <= 0 {
		w.Threshold = 0.80
	}
	if w.MaxPerCycle <= 0 {
		w.MaxPerCycle = 20
	}
}

// gathered is one cycle's view: the EPG snapshot, every match against it
// sorted earliest-airing-first, and which requested_by sources
// ("reconcile:sonarr", "reconcile:radarr") returned a complete wanted-list.
type gathered struct {
	progs   []Program
	matches []Match
	pulled  []string
}

// gatherMatches snapshots the EPG, pulls Sonarr/Radarr wanted-lists, and
// returns every match sorted earliest-airing-first. Errors from any single
// source are logged and gathering continues with whatever it could collect —
// a hard error is returned only when the EPG snapshot itself fails.
func (w *Worker) gatherMatches(ctx context.Context) (g gathered, err error) {
	progs, err := w.DB.ListUpcomingForReconcile(ctx, w.Window)
	if err != nil {
		return gathered{}, err
	}
	g.progs = progs
	if len(progs) == 0 {
		return g, nil
	}
	var matches []Match

	if w.Radarr != nil {
		if wants, err := w.Radarr.WantedMovies(ctx); err != nil {
			w.Logger.Warn("reconcile: radarr wanted failed", "err", err)
		} else {
			matches = append(matches, MatchMovies(progs, wants)...)
			g.pulled = append(g.pulled, "reconcile:radarr")
			w.Logger.Info("reconcile: radarr wanted pulled", "wanted", len(wants))
		}
	}
	// Series keys covered by the Sonarr wanted path, so the Plex gap diff
	// doesn't duplicate them.
	wantedSeries := map[string]bool{}
	if w.Sonarr != nil {
		if wants, err := w.Sonarr.WantedEpisodes(ctx); err != nil {
			w.Logger.Warn("reconcile: sonarr wanted failed", "err", err)
		} else {
			matches = append(matches, MatchEpisodes(progs, wants)...)
			g.pulled = append(g.pulled, "reconcile:sonarr")
			for _, wt := range wants {
				wantedSeries[titleKey(wt.SeriesTitle)] = true
			}
			w.Logger.Info("reconcile: sonarr wanted pulled", "wanted", len(wants))
		}
	}

	// Plex owned-but-incomplete gap diff (preview-only unless GapAutoRecord).
	if w.Plex != nil {
		if owned, err := w.Plex.OwnedSeriesEpisodes(ctx); err != nil {
			w.Logger.Warn("reconcile: plex library scan failed", "err", err)
		} else {
			gaps := MatchPlexGaps(progs, owned, wantedSeries)
			matches = append(matches, gaps...)
			w.Logger.Info("reconcile: plex gap diff",
				"owned_series", owned.SeriesCount(), "gap_matches", len(gaps))
		}
	}

	// Earliest airings first, so the per-cycle cap records the soonest
	// opportunities rather than an arbitrary slice.
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Program.StartAt.Before(matches[j].Program.StartAt)
	})
	g.matches = matches
	return g, nil
}

// reconcile runs one full cycle: gather matches and schedule (or, in
// dry-run, log) the ones at/above the confidence threshold, then cancel the
// recordings it booked earlier that no want claims any more.
func (w *Worker) reconcile(ctx context.Context) {
	w.applyDefaults()
	g, err := w.gatherMatches(ctx)
	if err != nil {
		w.Logger.Warn("reconcile: epg snapshot failed", "err", err)
		return
	}
	upcoming, matches := len(g.progs), g.matches
	if upcoming == 0 {
		w.Logger.Info("reconcile: no upcoming programs in window")
		return
	}

	var scheduled, deferred, skipped int
	for _, m := range matches {
		if !w.eligible(m) {
			deferred++
			w.Logger.Info("reconcile: candidate not recorded",
				"want", m.WantLabel, "program", m.Program.Title,
				"channel", m.Program.ChannelName, "start", m.Program.StartAt,
				"confidence", m.Confidence, "reason", m.Reason, "source", m.Source)
			continue
		}
		if scheduled >= w.MaxPerCycle {
			skipped++
			continue
		}
		if w.scheduleOne(ctx, m) {
			scheduled++
		}
	}
	cancelled := w.cancelUnwanted(ctx, g)

	w.Logger.Info("reconcile cycle complete",
		"upcoming", upcoming, "matches", len(matches),
		"scheduled", scheduled, "not_recorded", deferred,
		"over_cap_skipped", skipped, "cancelled", cancelled, "dry_run", w.DryRun)
}

// cancelUnwanted cancels the recordings this worker booked from a Sonarr or
// Radarr wanted-list once that list stops claiming them — the *arr grabbed the
// episode or movie elsewhere, or it was unmonitored. Left alone, the recording
// runs anyway, holds a scarce provider slot for its whole window and is then
// thrown away: the *arr will not import a copy it no longer wants.
//
// Only sources whose wanted-list came back complete this cycle are judged, and
// only airings the EPG snapshot still shows, so a failed *arr call or a guide
// gap never cancels anything. Returns how many rows were (or, in dry-run,
// would be) cancelled.
func (w *Worker) cancelUnwanted(ctx context.Context, g gathered) int {
	if len(g.pulled) == 0 {
		return 0
	}
	after := time.Now().Add(cancelGuard)
	rows, err := w.DB.ListUnstartedDVRRecordingsByRequester(ctx, g.pulled, after)
	if err != nil {
		w.Logger.Warn("reconcile: list booked recordings failed", "err", err)
		return 0
	}
	n := 0
	for _, r := range w.confirmUnwanted(unwantedRecordings(rows, g, w.outputFor)) {
		if w.DryRun {
			w.Logger.Info("reconcile: WOULD cancel recording no longer wanted (dry-run)",
				"id", r.ID, "program", r.Title, "start", r.ScheduledStart,
				"requested_by", r.RequestedBy, "output", r.OutputPath)
			n++
			continue
		}
		ok, err := w.DB.CancelUnstartedDVRRecording(ctx, r.ID, after,
			"cancelled: "+r.RequestedBy+" no longer wants this recording")
		if err != nil {
			w.Logger.Warn("reconcile: cancel failed", "id", r.ID, "err", err)
			continue
		}
		if ok {
			n++
			w.Logger.Info("reconcile: cancelled recording no longer wanted",
				"id", r.ID, "program", r.Title, "start", r.ScheduledStart,
				"requested_by", r.RequestedBy, "output", r.OutputPath)
		}
	}
	return n
}

// unwantedRecordings returns the booked rows that no current want claims,
// among airings the EPG snapshot still shows. A row is still claimed when a
// wanted-list match targets its output path (the want) or its exact airing
// (channel, start, title — the key it was booked under), whatever that
// match's confidence: a dip below the auto-record threshold is not the *arr
// dropping the want.
func unwantedRecordings(rows []store.DVRRecording, g gathered, outputFor func(Match) string) []store.DVRRecording {
	type slot struct {
		channel uuid.UUID
		start   int64
	}
	type airing struct {
		slot
		title string
	}
	shown := make(map[slot]bool, len(g.progs))
	for _, p := range g.progs {
		shown[slot{p.ChannelID, p.StartAt.UnixMicro()}] = true
	}
	paths, airings := map[string]bool{}, map[airing]bool{}
	for _, m := range g.matches {
		if m.Source == "plex-gap" {
			continue // an inferred gap is not the *arr wanting it
		}
		paths[outputFor(m)] = true
		airings[airing{slot{m.Program.ChannelID, m.Program.StartAt.UnixMicro()}, m.Program.Title}] = true
	}
	var out []store.DVRRecording
	for _, r := range rows {
		s := slot{r.ChannelID, r.ScheduledStart.UnixMicro()}
		if !shown[s] || paths[r.OutputPath] || airings[airing{s, r.Title}] {
			continue
		}
		out = append(out, r)
	}
	return out
}

// confirmUnwanted returns the rows found unwanted for cancelConfirmCycles
// consecutive cycles. A row missing from this cycle's list starts over.
func (w *Worker) confirmUnwanted(rows []store.DVRRecording) []store.DVRRecording {
	seen := make(map[uuid.UUID]int, len(rows))
	var due []store.DVRRecording
	for _, r := range rows {
		seen[r.ID] = w.unwantedSeen[r.ID] + 1
		if seen[r.ID] >= cancelConfirmCycles {
			due = append(due, r)
		}
	}
	w.unwantedSeen = seen
	return due
}

// PreviewItem is one match as rendered for the admin preview endpoint.
type PreviewItem struct {
	Want        string    `json:"want"`
	Kind        string    `json:"kind"`
	Source      string    `json:"source"` // "wanted" | "plex-gap"
	Program     string    `json:"program"`
	Channel     string    `json:"channel"`
	StartAt     time.Time `json:"start_at"`
	Confidence  float64   `json:"confidence"`
	Reason      string    `json:"reason"`
	WouldRecord bool      `json:"would_record"`
	OutputPath  string    `json:"output_path"`
}

// PreviewReport is the GET /admin/dvr/reconcile/preview payload: the exact
// matches the next cycle would act on, WITHOUT scheduling anything. Lets an
// operator eyeball the matcher before flipping off dry-run.
type PreviewReport struct {
	GeneratedAt      time.Time     `json:"generated_at"`
	DryRun           bool          `json:"dry_run"`
	Threshold        float64       `json:"threshold"`
	MaxPerCycle      int           `json:"max_per_cycle"`
	WindowHours      float64       `json:"window_hours"`
	UpcomingPrograms int           `json:"upcoming_programs"`
	TotalMatches     int           `json:"total_matches"`
	WouldRecord      int           `json:"would_record"`
	LowConfidence    int           `json:"low_confidence"`
	Items            []PreviewItem `json:"items"`
}

// Preview gathers matches and renders them as a report without touching the
// database. Safe to call on demand from the admin API.
func (w *Worker) Preview(ctx context.Context) (*PreviewReport, error) {
	w.applyDefaults()
	g, err := w.gatherMatches(ctx)
	if err != nil {
		return nil, err
	}
	upcoming, matches := len(g.progs), g.matches
	rep := &PreviewReport{
		GeneratedAt: time.Now(), DryRun: w.DryRun,
		Threshold: w.Threshold, MaxPerCycle: w.MaxPerCycle,
		WindowHours: w.Window.Hours(), UpcomingPrograms: upcoming,
		TotalMatches: len(matches),
	}
	recordable := 0
	for _, m := range matches {
		would := w.eligible(m) && recordable < w.MaxPerCycle
		if w.eligible(m) {
			if recordable < w.MaxPerCycle {
				recordable++
			}
			rep.WouldRecord++
		} else {
			rep.LowConfidence++
		}
		rep.Items = append(rep.Items, PreviewItem{
			Want: m.WantLabel, Kind: m.Kind, Source: sourceLabel(m.Source),
			Program: m.Program.Title,
			Channel: m.Program.ChannelName, StartAt: m.Program.StartAt,
			Confidence: m.Confidence, Reason: m.Reason,
			WouldRecord: would, OutputPath: w.outputFor(m),
		})
	}
	return rep, nil
}

// scheduleOne inserts (or, in dry-run, logs) the recording for one match.
// Returns true when a recording was scheduled (or would have been).
func (w *Worker) scheduleOne(ctx context.Context, m Match) bool {
	onscreenSE := onscreenFor(m)
	output := w.buildOutputPath(m, onscreenSE)

	if w.DryRun {
		w.Logger.Info("reconcile: WOULD record (dry-run)",
			"want", m.WantLabel, "program", m.Program.Title,
			"channel", m.Program.ChannelName, "start", m.Program.StartAt,
			"confidence", m.Confidence, "reason", m.Reason, "output", output)
		return true
	}

	progID := m.Program.ProgramID
	intent := store.DVRRecording{
		ChannelID:          m.Program.ChannelID,
		ProgramID:          &progID,
		Title:              m.Program.Title,
		SubTitle:           m.Program.SubTitle,
		EpisodeNumOnscreen: onscreenSE,
		IsMovie:            m.Program.IsMovie,
		ScheduledStart:     m.Program.StartAt,
		ScheduledEnd:       m.Program.EndAt,
		Priority:           w.Priority,
		RequestedBy:        requestedBy(m),
		OutputPath:         output,
	}
	var policies []store.DVRAdmissionPolicy
	if w.AdmissionPolicy != nil {
		policies = append(policies, *w.AdmissionPolicy)
	}
	rec, err := dvr.CreateDVRRecordingChecked(ctx, w.DB, intent, policies...)
	if errors.Is(err, store.ErrOutputPathBusy) ||
		errors.Is(err, store.ErrValidatedArtifactExists) {
		// A different airing is active for this canonical output, or a validated
		// artifact already owns it. Treat both as handled; only the authenticated,
		// target-bound replacement path may supersede validated media.
		w.Logger.Info("reconcile: output already has active or validated ownership; skipping",
			"want", m.WantLabel, "program", m.Program.Title,
			"existing", rec.ID, "existing_start", rec.ScheduledStart, "output", output)
		return true
	}
	if err != nil {
		w.Logger.Warn("reconcile: schedule failed",
			"want", m.WantLabel, "program", m.Program.Title, "err", err)
		return false
	}
	w.Logger.Info("reconcile: scheduled recording",
		"id", rec.ID, "want", m.WantLabel, "program", m.Program.Title,
		"channel", m.Program.ChannelName, "start", m.Program.StartAt,
		"confidence", m.Confidence, "reason", m.Reason, "output", output)
	return true
}

// onscreenFor is the SxxExx a match is booked under: the want's numbers,
// else the EPG's own; empty for movies.
func onscreenFor(m Match) string {
	if m.Kind != "episode" {
		return ""
	}
	if se := onscreen(m.Season, m.Episode); se != "" {
		return se
	}
	return m.Program.EpisodeNumOnscreen
}

// outputFor is the output path scheduleOne books a match under. The cancel
// pass keys on it too, so the two cannot disagree about which want owns a row.
func (w *Worker) outputFor(m Match) string {
	return w.buildOutputPath(m, onscreenFor(m))
}

// buildOutputPath mirrors internal/dvr/schedule.go's layout so recordings
// land where Sonarr/Radarr's library scan expects them:
//   - Movies/<Title>/<Title>.ts
//   - TV/<Show>/Season NN/<Show>.SxxExx.ts
//   - TV/<Show>/<Show>.<timestamp>.ts   (episode with no S/E)
func (w *Worker) buildOutputPath(m Match, onscreenSE string) string {
	root := w.OutputDir
	// Prefer the *arr's title: it is what *arr resolves the series from on
	// import, and it differs from the EPG's whenever the match came through
	// the TVDb/TMDb id union rather than the title. See Match.ArrTitle.
	title := m.Program.Title
	if m.ArrTitle != "" {
		title = m.ArrTitle
	}
	if m.Program.IsMovie {
		base := safeFilename(title)
		return filepath.Join(root, "Movies", base, base+".ts")
	}
	show := safeFilename(title)
	if onscreenSE != "" {
		seasonDir := fmt.Sprintf("Season %02d", m.Season)
		name := show + "." + onscreenSE
		if m.Program.SubTitle != "" {
			name += "." + safeFilename(m.Program.SubTitle)
		}
		return filepath.Join(root, "TV", show, seasonDir, name+".ts")
	}
	return filepath.Join(root, "TV", show,
		show+"."+m.Program.StartAt.UTC().Format("20060102T150405")+".ts")
}

// requestedBy tags the dvr_recording with the reconciler source that
// produced it, so the operator dashboard can attribute the recording.
func requestedBy(m Match) string {
	if m.Source == "plex-gap" {
		return "reconcile:plex-gap"
	}
	if m.Kind == "movie" {
		return "reconcile:radarr"
	}
	return "reconcile:sonarr"
}

// sourceLabel normalizes the match source for the preview payload; an empty
// source (the wanted-list path) renders as "wanted".
func sourceLabel(s string) string {
	if s == "" {
		return "wanted"
	}
	return s
}

func safeFilename(s string) string {
	r := strings.NewReplacer("/", "-", "\\", "-", ":", "-", "*", "", "?", "",
		`"`, "", "<", "", ">", "", "|", "", "\x00", "")
	return strings.TrimSpace(r.Replace(s))
}
