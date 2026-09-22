package epg

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/store"
)

// Defaults for the freshness alarms (2026-06-09 live-TV audit, E5). Settable
// on the Worker before Run for tests/tuning; not env-plumbed yet.
const (
	DefaultFailureAlertThreshold = 3              // consecutive fetch failures per source
	DefaultHorizonAlertDays      = 3.0            // median per-channel future-EPG days
	horizonRealertEvery          = 24 * time.Hour // re-alert cadence while below threshold
	sourceRealertEvery           = 24 * time.Hour // re-alert cadence while a source stays failing
)

// Worker runs scheduled XMLTV ingest. One pass per Interval, plus a
// best-effort pass at startup so a fresh deploy populates EPG without
// waiting an hour.
type Worker struct {
	logger   *slog.Logger
	db       *store.DB
	interval time.Duration
	hc       *http.Client

	// Alerts is optional; when set the worker raises source-failure,
	// source-recovery, and guide-horizon events through it.
	Alerts *alerts.Client
	// FailureAlertThreshold is the consecutive-failure count at which a
	// source alert fires (on crossing, then re-fired every
	// sourceRealertEvery while the source stays at/past the threshold).
	FailureAlertThreshold int
	// HorizonAlertDays alerts when the median per-channel future-EPG
	// horizon drops below this many days.
	HorizonAlertDays float64

	// pollStateMu keeps the durable poll-state transition and its external
	// alert in the same observable order across concurrent RunOnce calls. It
	// intentionally does not cover HTTP fetches or snapshot publication.
	pollStateMu sync.Mutex

	horizonMu        sync.Mutex
	lastHorizonAlert time.Time // dampens horizon re-alerts to horizonRealertEvery

	sourceAlertMu   sync.Mutex
	lastSourceAlert map[string]time.Time // per-source dampening for stuck-failing re-alerts
}

func NewWorker(logger *slog.Logger, db *store.DB, interval time.Duration) *Worker {
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	return &Worker{
		logger:                logger,
		db:                    db,
		interval:              interval,
		hc:                    &http.Client{Timeout: 5 * time.Minute},
		FailureAlertThreshold: DefaultFailureAlertThreshold,
		HorizonAlertDays:      DefaultHorizonAlertDays,
	}
}

// Run blocks until ctx is cancelled. Safe to call exactly once.
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("epg worker starting", "interval", w.interval)

	// Startup pass — best-effort, don't fail boot if this errors.
	w.runOnce(ctx)

	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("epg worker stopped")
			return
		case <-t.C:
			w.runOnce(ctx)
		}
	}
}

// RunOnce triggers an immediate ingest pass. Used by POST /admin/epg/refresh.
func (w *Worker) RunOnce(ctx context.Context) IngestStats {
	return w.runOnce(ctx)
}

func (w *Worker) runOnce(ctx context.Context) IngestStats {
	runID, err := w.db.StartEPGIngestRun(ctx)
	if err != nil {
		w.logger.Warn("epg ingest start failed", "err", err)
		return IngestStats{}
	}

	sources, err := w.db.ListEnabledEPGSources(ctx)
	if err != nil {
		w.logger.Warn("epg ingest: list sources failed", "err", err)
		_ = w.db.FinishEPGIngestRun(ctx, runID, 0, 0, 0, 0, 0, 0, 0, err.Error())
		return IngestStats{}
	}

	var stats IngestStats
	stats.SourcesAttempted = len(sources)

	// Retain the established worst-priority-first polling order. Publication
	// correctness no longer depends on it: sources keep independent snapshots
	// and every commit reruns deterministic channel-wide canonical selection.
	for i := len(sources) - 1; i >= 0; i-- {
		src := sources[i]
		select {
		case <-ctx.Done():
			break
		default:
		}
		attemptStartedAt, beginErr := w.db.BeginEPGSourcePollAttempt(ctx, src)
		if errors.Is(beginErr, store.ErrStaleEPGPollAttempt) {
			stats.SourcesNoChange++
			w.logger.Info("epg ingest source config superseded before fetch", "src", src.Name)
			continue
		}
		if beginErr != nil {
			stats.SourcesFailed++
			w.logger.Warn("epg ingest: begin poll attempt failed", "src", src.Name, "err", beginErr)
			continue
		}
		added, updated, unchanged, status, etag, lastModified, ierr := Ingest(ctx, w.db, src, w.hc)
		if errors.Is(ierr, store.ErrStaleEPGSnapshot) {
			// This response was valid for an older source configuration or
			// generation. It is intentional concurrency fencing, not an upstream
			// failure: do not attribute it to the current source's failure streak
			// and do not raise an upstream-dead alert.
			stats.SourcesNoChange++
			w.logger.Info("epg ingest source response superseded", "src", src.Name)
			continue
		}

		pollExpected := src
		// A publishing pass reports "ok" or "ok (skipped N malformed: …)" —
		// both bumped the snapshot generation inside Ingest, so both must be
		// anticipated here. An exact match fenced every publish-with-skips
		// pass as "health superseded", freezing the source's failure streak
		// at its pre-recovery value (observed on the iBoost XMLTV, which has
		// no HTTP validators and republishes every pass).
		if ierr == nil && strings.HasPrefix(status, "ok") {
			pollExpected.SnapshotGeneration++
			pollExpected.LastETag = etag
			pollExpected.LastModified = lastModified
		}
		w.pollStateMu.Lock()
		prevFails, curFails, pollErr := w.db.UpdateEPGSourcePollState(
			ctx, pollExpected, attemptStartedAt, status, ierr == nil,
		)
		if pollErr == nil {
			w.alertSourceTransition(ctx, src.Name, prevFails, curFails, ierr)
		}
		w.pollStateMu.Unlock()
		if errors.Is(pollErr, store.ErrStaleEPGPollAttempt) {
			stats.SourcesNoChange++
			w.logger.Info("epg ingest source health superseded", "src", src.Name)
			continue
		}
		if pollErr != nil {
			w.logger.Warn("epg ingest: update poll state failed", "src", src.Name, "err", pollErr)
		}

		switch {
		case ierr != nil:
			stats.SourcesFailed++
			w.logger.Warn("epg ingest source failed",
				"src", src.Name, "url", src.URL, "err", ierr)
		case status == "no_change":
			stats.SourcesNoChange++
			w.logger.Info("epg ingest source no_change", "src", src.Name)
		default:
			stats.SourcesOK++
			stats.ProgramsAdded += added
			stats.ProgramsUpdated += updated
			stats.ProgramsUnchanged += unchanged
			w.logger.Info("epg ingest source ok",
				"src", src.Name, "added", added, "updated", updated, "unchanged", unchanged)
		}
	}

	// Migration 0023 preserves unattributed guide rows during bootstrap so one
	// slow provider cannot blank Plex. Once every enabled source has published a
	// source-owned snapshot, retire any recognizable legacy XMLTV residue (for
	// example channels that belonged only to a source disabled before upgrade).
	if retired, err := w.db.RetireLegacyEPGRowsAfterSnapshotBootstrap(ctx); err != nil {
		w.logger.Warn("epg legacy snapshot retirement failed", "err", err)
	} else if retired > 0 {
		w.logger.Info("epg legacy snapshot retirement", "rows", retired)
	}

	// Best-effort retention sweep — keep 14 days of past programs for
	// the Plex "currently airing" footprint + history queries.
	if purged, err := w.db.PurgeOldPrograms(ctx, 14); err != nil {
		w.logger.Warn("epg purge failed", "err", err)
	} else if purged > 0 {
		w.logger.Info("epg purge", "rows", purged)
	}

	if err := w.db.FinishEPGIngestRun(ctx, runID,
		stats.SourcesAttempted, stats.SourcesOK, stats.SourcesNoChange, stats.SourcesFailed,
		stats.ProgramsAdded, stats.ProgramsUpdated, stats.ProgramsUnchanged, ""); err != nil {
		w.logger.Warn("epg ingest finalize failed", "err", err)
	}

	w.checkGuideHorizon(ctx)

	w.logger.Info("epg ingest pass complete",
		"sources_ok", stats.SourcesOK,
		"sources_no_change", stats.SourcesNoChange,
		"sources_failed", stats.SourcesFailed,
		"programs_added", stats.ProgramsAdded,
		"programs_unchanged", stats.ProgramsUnchanged,
	)
	return stats
}

// alertSourceTransition raises an alert when a source is at/past the
// consecutive-failure threshold — immediately on the first observation of
// that state, then at most every sourceRealertEvery while it persists — and
// one recovery alert when it next succeeds. Short blips below the threshold
// stay quiet; the horizon alarm is the escalating backstop if decay actually
// reaches the guide.
//
// The dampening stamp is in-memory only, deliberately: a crossing-only alert
// was lost whenever the container restarted mid-streak (the restarted worker
// saw prevFails already past the threshold and stayed silent forever —
// 2026-08-29: two sources dark 41h with zero pages). After a restart the
// empty stamp map re-fires for any source still failing on the first pass.
func (w *Worker) alertSourceTransition(ctx context.Context, srcName string, prevFails, curFails int, ierr error) {
	threshold := w.FailureAlertThreshold
	if w.Alerts == nil || threshold <= 0 {
		return
	}
	switch {
	case ierr != nil && curFails >= threshold:
		w.sourceAlertMu.Lock()
		last, seen := w.lastSourceAlert[srcName]
		fire := !seen || time.Since(last) >= sourceRealertEvery
		if fire {
			if w.lastSourceAlert == nil {
				w.lastSourceAlert = map[string]time.Time{}
			}
			w.lastSourceAlert[srcName] = time.Now()
		}
		w.sourceAlertMu.Unlock()
		if fire {
			w.Alerts.Send(ctx, alerts.EPGSourceFailing(srcName, curFails, ierr.Error()))
		}
	case ierr == nil && prevFails >= threshold:
		w.sourceAlertMu.Lock()
		delete(w.lastSourceAlert, srcName)
		w.sourceAlertMu.Unlock()
		w.Alerts.Send(ctx, alerts.EPGSourceRecovered(srcName, prevFails))
	}
}

// checkGuideHorizon measures remaining future guide data after each ingest
// pass and alerts when the median per-channel horizon drops below
// HorizonAlertDays. Re-alerts at most every horizonRealertEvery while the
// condition persists (the worker passes every w.interval, which is usually
// much shorter than a day).
func (w *Worker) checkGuideHorizon(ctx context.Context) {
	if w.Alerts == nil || w.HorizonAlertDays <= 0 {
		return
	}
	days, channels, err := w.db.EPGGuideHorizonDays(ctx)
	if err != nil {
		w.logger.Warn("epg horizon query failed", "err", err)
		return
	}
	w.logger.Info("epg guide horizon", "median_days", days, "channels", channels)
	if channels == 0 || days >= w.HorizonAlertDays {
		return
	}
	// runOnce can run concurrently (ticker pass + POST /admin/epg/refresh),
	// so the dampening stamp is lock-guarded.
	w.horizonMu.Lock()
	dampened := time.Since(w.lastHorizonAlert) < horizonRealertEvery
	if !dampened {
		w.lastHorizonAlert = time.Now()
	}
	w.horizonMu.Unlock()
	if dampened {
		return
	}
	w.Alerts.Send(ctx, alerts.EPGHorizonLow(days, channels))
}
