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
}

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

// gatherMatches snapshots the EPG, pulls Sonarr/Radarr wanted-lists, and
// returns every match sorted earliest-airing-first (plus the upcoming
// program count for reporting). Errors from any single source are logged
// and gathering continues with whatever it could collect — a hard error is
// returned only when the EPG snapshot itself fails.
func (w *Worker) gatherMatches(ctx context.Context) (matches []Match, upcoming int, err error) {
	progs, err := w.DB.ListUpcomingForReconcile(ctx, w.Window)
	if err != nil {
		return nil, 0, err
	}
	upcoming = len(progs)
	if upcoming == 0 {
		return nil, 0, nil
	}

	if w.Radarr != nil {
		if wants, err := w.Radarr.WantedMovies(ctx); err != nil {
			w.Logger.Warn("reconcile: radarr wanted failed", "err", err)
		} else {
			matches = append(matches, MatchMovies(progs, wants)...)
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
	return matches, upcoming, nil
}

// reconcile runs one full cycle: gather matches and schedule (or, in
// dry-run, log) the ones at/above the confidence threshold.
func (w *Worker) reconcile(ctx context.Context) {
	w.applyDefaults()
	matches, upcoming, err := w.gatherMatches(ctx)
	if err != nil {
		w.Logger.Warn("reconcile: epg snapshot failed", "err", err)
		return
	}
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

	w.Logger.Info("reconcile cycle complete",
		"upcoming", upcoming, "matches", len(matches),
		"scheduled", scheduled, "not_recorded", deferred,
		"over_cap_skipped", skipped, "dry_run", w.DryRun)
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
	matches, upcoming, err := w.gatherMatches(ctx)
	if err != nil {
		return nil, err
	}
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
		onscreenSE := ""
		if m.Kind == "episode" {
			if onscreenSE = onscreen(m.Season, m.Episode); onscreenSE == "" {
				onscreenSE = m.Program.EpisodeNumOnscreen
			}
		}
		rep.Items = append(rep.Items, PreviewItem{
			Want: m.WantLabel, Kind: m.Kind, Source: sourceLabel(m.Source),
			Program: m.Program.Title,
			Channel: m.Program.ChannelName, StartAt: m.Program.StartAt,
			Confidence: m.Confidence, Reason: m.Reason,
			WouldRecord: would, OutputPath: w.buildOutputPath(m, onscreenSE),
		})
	}
	return rep, nil
}

// scheduleOne inserts (or, in dry-run, logs) the recording for one match.
// Returns true when a recording was scheduled (or would have been).
func (w *Worker) scheduleOne(ctx context.Context, m Match) bool {
	onscreenSE := ""
	if m.Kind == "episode" {
		onscreenSE = onscreen(m.Season, m.Episode)
		if onscreenSE == "" {
			onscreenSE = m.Program.EpisodeNumOnscreen
		}
	}
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
