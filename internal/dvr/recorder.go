package dvr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/postprocess"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

// Recorder runs one DVR recording end-to-end:
//
//  1. ensure output dir exists
//  2. create a recording-ID-scoped capture file without replacing anything
//  3. open the live channel via stream.Pool.ServeWriter (which leases a
//     credential slot via the same FOR UPDATE SKIP LOCKED path Plex tunes use)
//  4. write bytes for the program window
//  5. normalize clocks and validate packet continuity, A/V coverage, and
//     known filler signatures into a second recording-ID-scoped candidate
//  6. publish without replacement, or atomically replace only the explicitly
//     bound predecessor, then commit the exact artifact evidence in PostgreSQL
//  7. on failure: preserve the best diagnostic candidate and keep any prior
//     validated recording current
//
// Recordings end when EITHER the program's scheduled_end is reached OR the
// upstream errors persistently. Recorder runs slightly past scheduled_end
// to capture overruns (sports especially); duration capped by maxOverrun.
type Recorder struct {
	Logger *slog.Logger
	DB     *store.DB
	Pool   *stream.Pool
	Alerts *alerts.Client

	// PostProcess runs after a recording completes successfully. nil = no
	// post-processing (recording lands as-is and that's it).
	PostProcess    *postprocess.Pipeline
	MediaProcessor MediaProcessor

	// Slack — start the recording N seconds early to catch the cold-cache
	// upstream warmup, end N seconds late to catch overruns. Tunable for
	// channels with sloppy timing.
	//
	// These are NOT free. A recording holds one of the provider's few
	// concurrent stream slots for its entire window, padding included, so
	// StartSlack+MaxOverrun is exactly how much two adjacent recordings
	// overlap. Programs start on the hour, so with the original 30s/5m every
	// padding tail sat on top of the next hour's recordings for 5.5 minutes —
	// 4 of 9 observed "all sources exhausted" failures had a blocker that was
	// in padding, recording nothing.
	StartSlack time.Duration // default DefaultStartSlack
	MaxOverrun time.Duration // default DefaultMaxOverrun
}

// Defaults for Recorder padding. Overridable via CONDUCTOR_DVR_START_SLACK and
// CONDUCTOR_DVR_MAX_OVERRUN.
const (
	DefaultStartSlack = 30 * time.Second
	DefaultMaxOverrun = 90 * time.Second

	// Completion is a two-resource handoff: the final file exists before the
	// database advertises it as completed. A lost COMMIT response therefore
	// needs several fresh, detached database attempts with one durable operation
	// UUID before finalize is allowed to move the file back to .partial.
	dvrCompletionReconcileBudget = 5 * time.Second
	dvrCompletionAttemptTimeout  = 1250 * time.Millisecond
	dvrCompletionRetryDelay      = 100 * time.Millisecond
)

// ErrRecordingClaimLost is a benign cross-scheduler race: another process
// claimed or cancelled the row after this scheduler reserved capacity.
var ErrRecordingClaimLost = errors.New("recording is no longer scheduled")

// ErrRecordingAdmissionExpired means capacity was reserved near the retry
// boundary but the atomic database claim correctly refused to start late.
var ErrRecordingAdmissionExpired = errors.New("recording admission deadline expired")

// ErrRecordingTerminalState marks a post-claim DB transition that did not
// persist. Scheduler.Wait treats this as failed quiescence so runtime ownership
// is not cleanly handed off while a row remains stuck `recording`.
var ErrRecordingTerminalState = errors.New("recording terminal state was not persisted")

// ErrRecordingCompletionUnresolved means all bounded, exact-operation reads
// failed, so PostgreSQL may or may not have committed completed state. The
// final file must remain in place: moving it back would guess that COMMIT
// rolled back and could leave a durable completed row pointing at no media.
var ErrRecordingCompletionUnresolved = errors.New("recording completion state is unresolved")

// ErrRecordingIncompleteCoverage means media arrived, but not across the
// complete scheduled program window. This is intentionally terminal: a late
// capacity admission must leave its .partial for inspection rather than
// rename/import a truncated capture merely because the end deadline fired.
var ErrRecordingIncompleteCoverage = errors.New("recording did not cover the complete scheduled window")

// ErrRecordingRealMediaGap marks a material in-program interval in which this
// DVR subscriber received no real upstream chunks. Timestamp normalization may
// close that hole on disk, so packet continuity alone cannot recover or prove
// the missing programme content.
var ErrRecordingRealMediaGap = errors.New("recording contains an in-program real-media outage")

const maxDVRRealMediaGap = 10 * time.Second

// runPostProcess kicks off the postprocess pipeline (if configured) on a
// background ctx. Detached from the recorder lifetime so a long-running
// pipeline doesn't hold a scheduler in-flight slot.
//
// Per-channel postprocess_enabled gates this — operator can opt out for
// channels where commskip / whisper would burn cycles fruitlessly
// (e.g., music channels, kids' channels with no commercials worth skipping).
func (r *Recorder) runPostProcess(rec store.DVRRecording, bytesWritten int64) {
	if r.PostProcess == nil || len(r.PostProcess.Stages) == 0 {
		return
	}
	enabled, err := r.DB.ChannelPostProcessEnabled(context.Background(), rec.ChannelID)
	if err != nil {
		r.Logger.Warn("postprocess gate lookup failed", "rec", rec.ID, "err", err)
		return
	}
	if !enabled {
		r.Logger.Info("postprocess skipped (disabled per-channel)",
			"rec", rec.ID, "channel", rec.ChannelID)
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		runID, err := r.DB.StartPostProcessRun(ctx, rec.ID, rec.OutputPath)
		if err != nil {
			r.Logger.Warn("postprocess start failed", "rec", rec.ID, "err", err)
			return
		}

		results, allOK := r.PostProcess.Run(ctx, &postprocess.Input{
			RecordingID:     rec.ID.String(),
			FilePath:        rec.OutputPath,
			Title:           rec.Title,
			SubTitle:        rec.SubTitle,
			EpisodeOnscreen: rec.EpisodeNumOnscreen,
			IsMovie:         rec.IsMovie,
			BytesWritten:    bytesWritten,
		})
		ok := 0
		fail := 0
		for _, r := range results {
			if r.Success {
				ok++
			} else {
				fail++
			}
		}
		_ = r.DB.FinishPostProcessRun(ctx, runID, allOK,
			len(results), ok, fail, postprocess.SummaryLog(results))
	}()
}

func NewRecorder(logger *slog.Logger, db *store.DB, pool *stream.Pool, alerts *alerts.Client) *Recorder {
	return &Recorder{
		Logger:         logger,
		DB:             db,
		Pool:           pool,
		Alerts:         alerts,
		MediaProcessor: newFFmpegMediaProcessor(pool.FFmpegBinary),
		StartSlack:     DefaultStartSlack,
		MaxOverrun:     DefaultMaxOverrun,
	}
}

// Run captures one DVR recording. Blocks until the recording finishes
// (success or failure). Safe to call from a goroutine per recording.
func (r *Recorder) Run(ctx context.Context, rec store.DVRRecording) error {
	reservation, err := r.Pool.ReserveWriter(ctx, rec.ChannelID, 0)
	if err != nil {
		return err
	}
	return r.RunReserved(ctx, rec, reservation)
}

// RunReserved captures one DVR recording using an already-acquired provider
// reservation. The atomic scheduled->recording claim happens before any file
// is opened; if it loses, the reservation is released without truncating the
// winner's .partial file.
func (r *Recorder) RunReserved(ctx context.Context, rec store.DVRRecording, reservation *stream.WriterReservation) error {
	return r.runReserved(ctx, rec, reservation, time.Time{})
}

// RunReservedBefore is the scheduler-only deadline-aware variant. The claim
// condition is enforced by PostgreSQL, not merely a wall-clock pre-check.
func (r *Recorder) RunReservedBefore(
	ctx context.Context,
	rec store.DVRRecording,
	reservation *stream.WriterReservation,
	admissionDeadline time.Time,
) error {
	return r.runReserved(ctx, rec, reservation, admissionDeadline)
}

func (r *Recorder) runReserved(
	ctx context.Context,
	rec store.DVRRecording,
	reservation *stream.WriterReservation,
	admissionDeadline time.Time,
) error {
	if reservation == nil {
		return errors.New("recording has no stream reservation")
	}
	if rec.OutputPath == "" {
		reservation.Release()
		return errors.New("recording has no output_path")
	}

	claimCtx := ctx
	claimCancel := func() {}
	if !admissionDeadline.IsZero() {
		// A row/advisory/pool wait during the atomic claim must not inherit the
		// much longer recording lifetime. context.WithDeadline also preserves an
		// earlier service-shutdown deadline from the parent.
		claimCtx, claimCancel = context.WithDeadline(ctx, admissionDeadline)
	}
	claimed, err := r.DB.TryMarkDVRRecordingStartedBefore(
		claimCtx, rec.ID, rec.OutputPath, admissionDeadline)
	claimCancel()
	if err != nil {
		reservation.Release()
		claimErr := fmt.Errorf("claim recording: %w", err)
		if !admissionDeadline.IsZero() &&
			!time.Now().Before(admissionDeadline) &&
			errors.Is(err, context.DeadlineExceeded) {
			return errors.Join(ErrRecordingAdmissionExpired, claimErr)
		}
		return claimErr
	}
	if !claimed {
		reservation.Release()
		if !admissionDeadline.IsZero() && !time.Now().Before(admissionDeadline) {
			return ErrRecordingAdmissionExpired
		}
		return ErrRecordingClaimLost
	}
	capturePath := recordingCapturePath(rec.OutputPath, rec.ID)
	if err := r.DB.MarkDVRArtifactStage(ctx, rec.ID, "capturing", capturePath); err != nil {
		reservation.Release()
		return r.recordFailure(rec.ID, err,
			"record artifact capture stage: "+err.Error(), 0)
	}
	if err := ctx.Err(); err != nil {
		reservation.Release()
		return errors.Join(err, r.markCancelled(rec.ID))
	}

	if err := os.MkdirAll(filepath.Dir(rec.OutputPath), 0o755); err != nil {
		reservation.Release()
		if ctx.Err() != nil {
			return errors.Join(err, r.markCancelled(rec.ID))
		} else {
			return r.recordFailure(rec.ID, err, "mkdir: "+err.Error(), 0)
		}
	}

	partial := capturePath
	// Every attempt owns a UUID-qualified capture. O_EXCL preserves a prior
	// interrupted attempt even if the same row is accidentally replayed.
	f, err := os.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		reservation.Release()
		if ctx.Err() != nil {
			return errors.Join(err, r.markCancelled(rec.ID))
		} else {
			return r.recordFailure(rec.ID, err, "open: "+err.Error(), 0)
		}
	}

	r.Logger.Info("dvr recording started",
		"id", rec.ID, "title", rec.Title, "channel", rec.ChannelID,
		"path", partial, "until", rec.ScheduledEnd)

	// Window: from now (or scheduled_start, whichever is later) to
	// scheduled_end + MaxOverrun. ServeWriter respects ctx — Done aborts.
	windowEnd := rec.ScheduledEnd.Add(r.MaxOverrun)
	deadline := time.Now().Add(time.Until(windowEnd))
	if deadline.Before(time.Now().Add(time.Minute)) {
		// Sanity: don't run a recording whose window is already in the past.
		deadline = time.Now().Add(time.Minute)
	}
	recCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// Heartbeat goroutine — writes bytes_written to DB every 5s so the
	// admin dashboard reflects progress without extra fsstat noise.
	stopHB := make(chan struct{})
	defer close(stopHB)
	go func() {
		t := time.NewTicker(progressSampleInterval)
		defer t.Stop()
		for {
			select {
			case <-stopHB:
				return
			case <-t.C:
				if fi, err := os.Stat(partial); err == nil {
					_ = r.DB.MarkDVRRecordingProgress(ctx, rec.ID, fi.Size())
				}
			}
		}
	}()

	// fw stamps the moment the first byte actually lands in the file. Run-entry
	// time is not a substitute: mkdir, file open, the started-at DB write, slot
	// acquisition and up to 15s waiting on upstream headers all sit between
	// them, so a slow start can cross scheduled_start and lose the beginning of
	// the program while wall-clock still says we began during pre-roll.
	fw := &firstWriteAt{w: f}
	bytesWritten, mediaCoverage, serveErr := reservation.ServeWithCoverage(
		recCtx, fw, rec.ScheduledStart, rec.ScheduledEnd)
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil || closeErr != nil {
		serveErr = errors.Join(serveErr, fmt.Errorf("%w: flush capture: %w",
			stream.ErrSinkWrite, errors.Join(syncErr, closeErr)))
	}

	// Decide outcome. Neither end of this is wall-clock:
	//   start — fw.at, when bytes first hit the file.
	//   end   — lastRealChunk, when REAL upstream content last arrived. The
	//           pump retries a dead upstream for up to ~90s before giving up,
	//           and the file keeps growing throughout (Source Unavailable
	//           slate, plus upstreams that answer a dead channel with a finite
	//           black placeholder), so neither the error time nor file growth
	//           can prove the program was still arriving.
	endpointCoverage := coveredWholeProgram(fw.at, mediaCoverage.LastRealChunk,
		rec.ScheduledStart, rec.ScheduledEnd)
	gapErr := validateDVRRealMediaCoverage(mediaCoverage)
	covered := endpointCoverage && gapErr == nil
	switch decideOutcome(serveErr, bytesWritten, covered) {
	case outcomeComplete:
		return r.finalizeArtifact(ctx, rec, partial, bytesWritten,
			mediaCoverage.UpstreamInterruptions)

	case outcomeSalvage:
		// Upstream died during the post-roll padding, i.e. after the whole
		// scheduled program had already been captured. Discarding that is how
		// a complete 1.9 GB capture ended up marked 'failed' and abandoned as
		// a .partial. Keep the show; the padding is not worth it.
		r.Logger.Info("dvr upstream failed after scheduled end; finalizing captured program",
			"id", rec.ID, "bytes", bytesWritten, "err", serveErr)
		return r.finalizeArtifact(ctx, rec, partial, bytesWritten,
			mediaCoverage.UpstreamInterruptions)

	case outcomeCancelled:
		// Operator cancelled (or shutdown). The caller context is necessarily
		// cancelled here, so use a bounded teardown context for the durable state
		// transition. Otherwise Scheduler.Cancel releases capacity but can leave
		// the row stuck in recording forever.
		if bytesWritten == 0 {
			_ = os.Remove(partial)
		}
		return errors.Join(serveErr, r.markCancelled(rec.ID))

	default:
		// Hard failure — upstream couldn't be served, or it arrived too late to
		// cover the scheduled program. The latter remains a .partial: a normal
		// context deadline proves only that we reached the end bound, not that
		// capacity was admitted early enough to capture the beginning.
		failureErr := serveErr
		if !endpointCoverage {
			failureErr = errors.Join(failureErr, fmt.Errorf(
				"%w: first_write=%s scheduled_start=%s last_real_chunk=%s scheduled_end=%s",
				ErrRecordingIncompleteCoverage,
				formatCoverageTime(fw.at), formatCoverageTime(rec.ScheduledStart),
				formatCoverageTime(mediaCoverage.LastRealChunk), formatCoverageTime(rec.ScheduledEnd)))
		}
		failureErr = errors.Join(failureErr, gapErr)
		if failureErr == nil {
			failureErr = errors.New("recording stream ended before completion")
		}
		failureResult := r.recordFailure(rec.ID, failureErr, failureErr.Error(), bytesWritten)
		if failureResult == nil {
			r.Logger.Info("dvr cancellation won recording failure race",
				"id", rec.ID, "recording_err", failureErr)
			return nil
		}
		r.Logger.Warn("dvr recording failed",
			"id", rec.ID, "err", failureErr, "bytes_written", bytesWritten)
		if r.Alerts != nil {
			r.Alerts.Send(context.Background(), alerts.Event{
				Severity: alerts.SevError,
				Source:   "conductor.dvr",
				Title:    "DVR recording failed: " + rec.Title,
				Detail:   failureErr.Error(),
				Tags: map[string]any{
					"recording_id": rec.ID.String(),
					"channel_id":   rec.ChannelID.String(),
				},
			})
		}
		return failureResult
	}
}

func validateDVRRealMediaCoverage(coverage stream.DVRMediaCoverage) error {
	if coverage.MaxRealGap <= maxDVRRealMediaGap {
		return nil
	}
	return fmt.Errorf(
		"%w: gap=%s start=%s end=%s limit=%s",
		ErrRecordingRealMediaGap, coverage.MaxRealGap,
		formatCoverageTime(coverage.MaxGapStart),
		formatCoverageTime(coverage.MaxGapEnd), maxDVRRealMediaGap)
}

// outcome is what Run should do with a capture that has stopped.
type outcome int

const (
	outcomeFailed outcome = iota
	outcomeComplete
	outcomeCancelled
	// outcomeSalvage: upstream errored, but only after the program's own
	// scheduled window had elapsed, so the bytes on disk are the whole show.
	outcomeSalvage
)

// progressSampleInterval is how often the heartbeat stats the .partial.
const progressSampleInterval = 5 * time.Second

// coverageTolerance is how far short of scheduled_end the last real chunk may
// land and still count as full coverage. Upstream chunks arrive continuously
// while content is flowing, so this only absorbs the tail of one chunk plus
// scheduling jitter — it is not a licence to lose content.
const coverageTolerance = 5 * time.Second

// firstWriteAt wraps the recording sink to stamp when the first byte actually
// lands. Only the ServeWriter goroutine writes, so no locking is needed.
type firstWriteAt struct {
	w  io.Writer
	at time.Time
}

func (f *firstWriteAt) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if n > 0 && f.at.IsZero() {
		f.at = time.Now()
	}
	return n, err
}

// coveredWholeProgram reports whether this attempt actually captured the
// program's entire scheduled window: bytes landing by the time it started, and
// REAL upstream content still arriving at its end.
//
// Both halves are load-bearing.
//
// firstWrite (not Run-entry time): mkdir, file open, the started-at DB write,
// slot acquisition and up to 15s of waiting on upstream headers all sit between
// entering Run and the first byte, so entering during pre-roll does not prove
// we were capturing by scheduled_start. It also covers the stale-row case —
// ListPendingDVRRecordings has no lower bound on scheduled_start, so a row left
// 'scheduled' while Conductor was down is picked up long after its program
// aired and Run's sanity clamp still gives it a one-minute window, which would
// otherwise make "we are past scheduled_end" trivially true for a fragment.
// A zero value (nothing ever written) can never satisfy it.
//
// lastRealChunk: neither wall-clock stop time nor file growth can answer this.
// The pump retries a dead upstream for up to ~90s before giving up, so an
// outage beginning BEFORE scheduled_end surfaces as an error well after it —
// and the file keeps growing throughout, because the Source Unavailable slate
// is being written and because a dead upstream can answer with a finite black
// placeholder that downloads far faster than real time. Only real upstream
// content advances lastRealChunk, so only it distinguishes "we have the show"
// from "we lost the last minute of it".
func coveredWholeProgram(firstWrite, lastRealChunk, scheduledStart, scheduledEnd time.Time) bool {
	if firstWrite.IsZero() {
		return false // nothing was ever written
	}
	return !firstWrite.After(scheduledStart.Add(coverageTolerance)) &&
		!lastRealChunk.Before(scheduledEnd.Add(-coverageTolerance))
}

// decideOutcome is pure so the salvage rule is testable without a DB or a
// live upstream.
func decideOutcome(serveErr error, bytesWritten int64, coveredProgram bool) outcome {
	switch {
	case errors.Is(serveErr, context.Canceled), errors.Is(serveErr, stream.ErrPoolClosed):
		return outcomeCancelled
	case errors.Is(serveErr, stream.ErrSinkWrite):
		// Our own write to disk failed (full volume, unwritable mount). The
		// bytes on disk are not what we think they are, so this is never
		// salvageable no matter how much of the window we covered.
		return outcomeFailed
	case bytesWritten <= 0 || !coveredProgram:
		return outcomeFailed
	case serveErr == nil || errors.Is(serveErr, context.DeadlineExceeded):
		// A clean EOF or normal recording deadline is complete only when the
		// first and last real chunks prove the full scheduled window arrived.
		return outcomeComplete
	case coveredProgram:
		return outcomeSalvage
	default:
		return outcomeFailed
	}
}

func formatCoverageTime(t time.Time) string {
	if t.IsZero() {
		return "none"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

const dvrArtifactValidatorVersion = "dvr-media-v1"

// finalizeArtifact performs the mandatory integrity pipeline. Best-effort
// post-processing is intentionally later: no commskip/import stage may observe
// media before packet clocks and durable artifact ownership are proven.
func (r *Recorder) finalizeArtifact(
	ctx context.Context,
	rec store.DVRRecording,
	capture string,
	bytesWritten int64,
	upstreamInterruptions int,
) error {
	if r.MediaProcessor == nil {
		err := errors.New("DVR media processor is not configured")
		return r.rejectArtifactFailure(rec, err, capture, bytesWritten, MediaReport{})
	}
	if err := r.DB.MarkDVRArtifactStage(ctx, rec.ID, "normalizing", capture); err != nil {
		return r.rejectArtifactFailure(rec, err, capture, bytesWritten, MediaReport{})
	}

	normalized := recordingNormalizedPath(rec.OutputPath, rec.ID)
	processCtx, processCancel := context.WithTimeout(ctx, mediaProcessTimeout)
	report, err := r.MediaProcessor.NormalizeAndValidate(
		processCtx, capture, normalized, rec.ScheduledEnd.Sub(rec.ScheduledStart))
	processCancel()
	if err != nil {
		if upstreamInterruptions > 0 {
			// Provenance for the operator: distinguishes our own mid-recording
			// reconnect splices from damage already present in the provider feed.
			err = fmt.Errorf("%w (capture saw %d upstream interruptions)",
				err, upstreamInterruptions)
		}
		diagnosticPath := capture
		if info, statErr := os.Stat(normalized); statErr == nil && info.Size() > 0 {
			diagnosticPath = normalized
		}
		r.Logger.Warn("dvr normalized artifact rejected",
			"id", rec.ID, "path", diagnosticPath, "report", report,
			"upstream_interruptions", upstreamInterruptions, "err", err)
		return r.rejectArtifactFailure(rec, err, diagnosticPath, bytesWritten, report)
	}

	sha, err := hex.DecodeString(report.SHA256)
	if err != nil {
		validationErr := fmt.Errorf("decode normalized artifact SHA-256 evidence %q: %w",
			report.SHA256, err)
		return r.rejectArtifactFailure(rec, validationErr, normalized, bytesWritten, report)
	}
	if len(sha) != sha256.Size {
		validationErr := fmt.Errorf(
			"invalid normalized artifact SHA-256 evidence length %d, want %d",
			len(sha), sha256.Size)
		return r.rejectArtifactFailure(rec, validationErr, normalized, bytesWritten, report)
	}
	if rec.ReplacesRecordingID != nil {
		predecessor, predecessorErr := r.DB.GetDVRRecording(ctx, *rec.ReplacesRecordingID)
		if predecessorErr != nil {
			return r.rejectArtifactFailure(rec,
				fmt.Errorf("load DVR artifact predecessor: %w", predecessorErr),
				normalized, bytesWritten, report)
		}
		check, checkErr := ReconcileCurrentDVRArtifact(ctx, r.DB, predecessor)
		if checkErr != nil {
			return r.rejectArtifactFailure(rec, checkErr, normalized, bytesWritten, report)
		}
		switch {
		case check.Demoted:
			// The transaction also detached this active successor. A file that
			// reappears after the check is still protected by no-replace publish.
			rec.ReplacesRecordingID = nil
			rec.ExplicitRerecord = false
		case !check.Usable:
			return r.rejectArtifactFailure(rec,
				store.ErrArtifactReplacementMismatch, normalized, bytesWritten, report)
		}
	}
	backup := ""
	if rec.ReplacesRecordingID != nil {
		backup = recordingSupersededPath(rec.OutputPath, *rec.ReplacesRecordingID)
	}
	if err := r.DB.MarkDVRArtifactStage(ctx, rec.ID, "publishing", normalized); err != nil {
		return r.rejectArtifactFailure(rec, err, normalized, bytesWritten, report)
	}
	if err := publishRecordingCandidate(rec.OutputPath, normalized, backup); err != nil {
		if errors.Is(err, errArtifactPublishRecoveryRequired) {
			r.Logger.Error("dvr artifact publish rollback unresolved; preserving recovery provenance",
				"id", rec.ID, "path", rec.OutputPath, "candidate", normalized,
				"backup", backup, "err", err)
			return r.terminalStateError("publish artifact recovery", rec.ID, err)
		}
		return r.rejectArtifactFailure(rec, err, normalized, bytesWritten, report)
	}
	publishedInfo, publishedStatErr := os.Stat(rec.OutputPath)
	filesystemID := ""
	if publishedStatErr == nil {
		switch {
		case !publishedInfo.Mode().IsRegular():
			publishedStatErr = fmt.Errorf(
				"published DVR artifact is not regular: mode=%s", publishedInfo.Mode())
		case publishedInfo.Size() != report.ArtifactBytes:
			publishedStatErr = fmt.Errorf(
				"published DVR artifact byte evidence changed: filesystem=%d validated=%d",
				publishedInfo.Size(), report.ArtifactBytes)
		default:
			var ok bool
			filesystemID, ok = artifactFilesystemID(publishedInfo)
			if !ok {
				publishedStatErr = errors.New(
					"published DVR artifact filesystem identity is unavailable")
			}
		}
	}
	if publishedStatErr != nil {
		return r.rollbackRejectedArtifact(
			rec, normalized, backup, "", publishedStatErr,
			r.DB.FinishDVRPublishRecovery)
	}

	state, stateErr := r.promoteArtifact(
		rec.ID, report, sha, filesystemID, rec.OutputPath, backup)
	if errors.Is(stateErr, ErrRecordingCompletionUnresolved) {
		// The filesystem publication is durable while PostgreSQL's answer is
		// unknown. Reversing it would guess whether COMMIT rolled back. Startup
		// recovery uses the retained publishing stage if the operation did not win.
		r.Logger.Error("dvr artifact promotion unresolved; preserving publish seam",
			"id", rec.ID, "path", rec.OutputPath, "candidate", normalized,
			"backup", backup, "err", stateErr)
		return stateErr
	}
	if state == "completed" && stateErr != nil {
		return stateErr
	}
	if stateErr != nil || state != "completed" {
		return r.rollbackRejectedArtifact(
			rec, normalized, backup, state, stateErr, r.DB.FinishDVRPublishRecovery)
	}

	// First publication hard-links the normalized candidate; replacement
	// publication renames it. Both outcomes converge here.
	if err := os.Remove(normalized); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("recording completion is durable; remove normalized alias: %w", err)
	}
	if err := os.Remove(capture); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("recording completion is durable; remove raw capture: %w", err)
	}
	r.Logger.Info("dvr recording artifact validated and completed",
		"id", rec.ID, "bytes", report.ArtifactBytes, "path", rec.OutputPath,
		"duration", report.Duration, "av_start_delta", report.AVStartDelta,
		"av_end_delta", report.AVEndDelta, "av_drift", report.AVDrift,
		"upstream_interruptions", upstreamInterruptions,
		"bitstream_errors", report.BitstreamErrors,
		"replaces", rec.ReplacesRecordingID)
	r.runPostProcess(rec, report.ArtifactBytes)
	return nil
}

type finishDVRPublishRecoveryFunc func(context.Context, uuid.UUID, string, string) error

// rollbackRejectedArtifact restores filesystem ownership first, then clears
// the durable publishing marker. If the database cleanup is uncertain, the
// safe filesystem state remains replayable by startup recovery.
func (r *Recorder) rollbackRejectedArtifact(
	rec store.DVRRecording,
	normalized, backup, state string,
	stateErr error,
	finishRecovery finishDVRPublishRecoveryFunc,
) error {
	rollbackErr := rollbackPublishedCandidate(rec.OutputPath, normalized, backup)
	if rollbackErr != nil {
		rollbackErr = errors.Join(errArtifactPublishRecoveryRequired, rollbackErr)
		r.Logger.Error("dvr terminal race artifact rollback unresolved",
			"id", rec.ID, "state", state, "err", rollbackErr)
	}
	if rollbackErr == nil {
		reason := "rolled back DVR artifact publication after terminal state race"
		if stateErr != nil {
			reason = "rolled back rejected DVR artifact publication: " + stateErr.Error()
		}
		recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 5*time.Second)
		recoveryErr := finishRecovery(recoveryCtx, rec.ID, reason, normalized)
		recoveryCancel()
		if recoveryErr != nil {
			// Filesystem ownership is safe, but retain the durable publishing
			// marker so startup can replay the database half of recovery.
			return errors.Join(stateErr, r.terminalStateError(
				"finish artifact publish rollback", rec.ID, recoveryErr))
		}
	}
	if state == "cancelled" && stateErr == nil {
		return rollbackErr
	}
	return errors.Join(stateErr, rollbackErr)
}

func storeMediaReport(report MediaReport) store.DVRMediaReport {
	return store.DVRMediaReport{
		DurationMS:      report.Duration.Milliseconds(),
		MaxGapMS:        report.MaxGap.Milliseconds(),
		Discontinuities: report.Discontinuities,
		AVStartDeltaMS:  report.AVStartDelta.Milliseconds(),
		AVEndDeltaMS:    report.AVEndDelta.Milliseconds(),
		AVDriftMS:       report.AVDrift.Milliseconds(),
	}
}

func (r *Recorder) rejectArtifactFailure(
	rec store.DVRRecording,
	primary error,
	path string,
	bytesWritten int64,
	report MediaReport,
) error {
	artifactBytes := bytesWritten
	if report.ArtifactBytes > 0 {
		artifactBytes = report.ArtifactBytes
	}
	stateCtx, stateCancel := context.WithTimeout(context.Background(), 5*time.Second)
	updated, err := r.DB.RejectDVRRecordingArtifact(
		stateCtx, rec.ID, primary.Error(), path, artifactBytes, storeMediaReport(report))
	state, stateErr := r.resolveTerminalCAS(
		stateCtx, "reject artifact", "failed", rec.ID, updated, err)
	stateCancel()
	// Only this successful recording->failed CAS owns the notification. A
	// reread of an already-failed row is not ownership: replay/concurrent
	// terminalizers must not resend, and cancellation/uncertain writes cannot
	// announce a rejection we did not durably establish. Delivery remains the
	// existing best-effort webhook; no new retry or recovery alert is added.
	if updated && err == nil && r.Alerts != nil {
		r.Alerts.Send(context.Background(), alerts.Event{
			Severity: alerts.SevError,
			Source:   "conductor.dvr",
			Title:    "DVR recording rejected: " + rec.Title,
			Detail:   primary.Error(),
			Tags: map[string]any{
				"recording_id":  rec.ID.String(),
				"channel_id":    rec.ChannelID.String(),
				"failure_stage": "artifact_rejection",
			},
		})
	}
	if state == "cancelled" && stateErr == nil {
		return nil
	}
	return errors.Join(primary, stateErr)
}

type promoteDVRArtifactFunc func(
	context.Context,
	uuid.UUID,
	uuid.UUID,
	int64,
	store.DVRMediaReport,
	[]byte,
	string,
	string,
	string,
	string,
) (store.DVRCompletionResult, error)

func (r *Recorder) promoteArtifact(
	id uuid.UUID,
	report MediaReport,
	sha []byte,
	filesystemID, canonicalPath, predecessorBackupPath string,
) (string, error) {
	return r.promoteArtifactWith(
		id, report, sha, filesystemID, canonicalPath, predecessorBackupPath,
		r.DB.PromoteDVRRecordingArtifact)
}

func (r *Recorder) promoteArtifactWith(
	id uuid.UUID,
	report MediaReport,
	sha []byte,
	filesystemID, canonicalPath, predecessorBackupPath string,
	promote promoteDVRArtifactFunc,
) (string, error) {
	operationID := uuid.New()
	deadline := time.Now().Add(dvrCompletionReconcileBudget)
	attempts := 0
	ambiguous := false
	var firstErr, lastErr error

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		attemptCtx, attemptCancel := context.WithTimeout(
			context.Background(), min(dvrCompletionAttemptTimeout, remaining))
		result, err := promote(
			attemptCtx, id, operationID, report.ArtifactBytes,
			storeMediaReport(report), sha, filesystemID, dvrArtifactValidatorVersion,
			canonicalPath, predecessorBackupPath)
		attemptCancel()
		attempts++
		if err == nil {
			switch result.State {
			case "completed":
				if result.OperationOwned {
					return result.State, nil
				}
				return result.State, r.terminalStateError("promote artifact", id,
					errors.New("completed artifact is owned by a different or legacy operation"))
			case "cancelled":
				if result.OperationOwned {
					return result.State, r.terminalStateError("promote artifact", id,
						errors.New("cancelled state unexpectedly owns completion operation"))
				}
				return result.State, nil
			default:
				return result.State, r.terminalStateError("promote artifact", id,
					fmt.Errorf("artifact promotion stopped in unexpected state %q", result.State))
			}
		}
		if errors.Is(err, store.ErrDVRArtifactCommitOutcomeUnknown) {
			ambiguous = true
		} else if !ambiguous {
			// Transaction-body/precondition failures are authoritative: the
			// transaction did not commit, so the caller may roll publication back.
			return "", r.terminalStateError("promote artifact", id, err)
		}
		if firstErr == nil {
			firstErr = err
		}
		lastErr = err
		remaining = time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timer := time.NewTimer(min(dvrCompletionRetryDelay, remaining))
		<-timer.C
	}
	return "", r.terminalStateError("promote artifact reconcile", id, fmt.Errorf(
		"%w: operation %s unresolved after %d fresh attempts: first error: %v; last error: %w",
		ErrRecordingCompletionUnresolved, operationID, attempts, firstErr, lastErr))
}

// finalize is retained only as a focused harness for the pre-FX476 exact-
// completion protocol regressions below. Production capture always calls
// finalizeArtifact and cannot bypass media validation.
func (r *Recorder) finalize(rec store.DVRRecording, partial string, bytesWritten int64) error {
	return r.finalizeWith(rec, partial, bytesWritten, r.markCompleted)
}

type recordDVRFailureFunc func(uuid.UUID, error, string, int64) error
type runDVRPostProcessFunc func(store.DVRRecording, int64)

func (r *Recorder) finalizeWith(
	rec store.DVRRecording,
	partial string,
	bytesWritten int64,
	markCompleted func(uuid.UUID, int64) (string, error),
) error {
	return r.finalizeWithActions(
		rec, partial, bytesWritten, markCompleted, r.recordFailure, r.runPostProcess)
}

func (r *Recorder) finalizeWithActions(
	rec store.DVRRecording,
	partial string,
	bytesWritten int64,
	markCompleted func(uuid.UUID, int64) (string, error),
	recordFailure recordDVRFailureFunc,
	runPostProcess runDVRPostProcessFunc,
) error {
	// Link is the no-replace publication primitive. Unlike Rename on Unix, it
	// fails with EEXIST when an earlier completed airing already owns the final
	// path. Both names refer to the same inode until exact database ownership is
	// established, so every uncertain or losing terminal outcome can preserve
	// the new capture as .partial without ever overwriting known-good media.
	if err := os.Link(partial, rec.OutputPath); err != nil {
		publishErr := fmt.Errorf("publish final recording without replacing existing path: %w", err)
		r.Logger.Warn("dvr no-replace publish failed; preserving partial and final path",
			"id", rec.ID, "partial", partial, "path", rec.OutputPath, "err", err)
		return recordFailure(rec.ID, publishErr, "publish: "+publishErr.Error(), bytesWritten)
	}
	state, stateErr := markCompleted(rec.ID, bytesWritten)
	if errors.Is(stateErr, ErrRecordingCompletionUnresolved) {
		// The first resource transition (partial linked at final) is already durable,
		// while the database transition has no authoritative answer. Reversing
		// the link now would guess that the DB rolled back; if it committed, the
		// completed row would point at a missing file. Preserve both names, do not
		// post-process, and surface failed quiescence for reconciliation.
		r.Logger.Error("dvr completion unresolved; preserving final and partial aliases",
			"id", rec.ID, "path", rec.OutputPath, "partial", partial, "err", stateErr)
		return stateErr
	}
	if state == "completed" && stateErr != nil {
		// A completed row owned by another/legacy operation is authoritative
		// database state, but not permission for this caller to post-process it.
		// Preserve both aliases instead of making that completed row point at a
		// path this caller removed or discarding its salvageable partial.
		r.Logger.Error("dvr completion owned elsewhere; preserving final and partial aliases",
			"id", rec.ID, "path", rec.OutputPath, "partial", partial, "err", stateErr)
		return stateErr
	}
	if stateErr != nil || state != "completed" {
		// The final path is a second link to the partial's inode. If cancellation
		// or failure linearized first, remove only that just-published alias. The
		// partial remains untouched for salvage.
		unpublishErr := unlinkPublishedFinal(partial, rec.OutputPath)
		if unpublishErr != nil {
			r.Logger.Error("dvr terminal race unpublish failed; preserving both paths",
				"id", rec.ID, "state", state, "path", rec.OutputPath,
				"partial", partial, "err", unpublishErr)
		}
		if state == "cancelled" && stateErr == nil {
			r.Logger.Info("dvr cancellation won completion race", "id", rec.ID)
			return unpublishErr
		}
		return errors.Join(stateErr, unpublishErr)
	}

	// Exact durable ownership is the only permission to remove the salvage
	// alias or start post-processing. A failed unlink cannot invalidate the
	// completed row or final path, so report it as a cleanup error (not a
	// terminal-state failure) and skip post-processing without rewriting DB
	// state.
	if err := os.Remove(partial); err != nil {
		cleanupErr := fmt.Errorf(
			"recording completion is durable; remove partial alias before postprocess: %w", err)
		r.Logger.Error("dvr completion durable but partial alias cleanup failed; skipping postprocess",
			"id", rec.ID, "path", rec.OutputPath, "partial", partial, "err", err)
		return cleanupErr
	}
	r.Logger.Info("dvr recording completed",
		"id", rec.ID, "bytes", bytesWritten, "path", rec.OutputPath)

	// Post-process pipeline (commskip-auto, etc.) — fire-and-forget on
	// a fresh ctx so a long Comskip+Whisper run doesn't block the
	// scheduler's in-flight slot.
	runPostProcess(rec, bytesWritten)

	return nil
}

// unlinkPublishedFinal removes the final name only when it still identifies
// the same inode as the salvageable partial. Conductor's runtime singleton and
// active-output-path admission make replacement impossible internally, but the
// identity check keeps cleanup fail-closed if an operator or external process
// changes either path in the terminal-state seam.
func unlinkPublishedFinal(partial, output string) error {
	partialInfo, err := os.Lstat(partial)
	if err != nil {
		return fmt.Errorf("inspect partial before unpublishing final: %w", err)
	}
	outputInfo, err := os.Lstat(output)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect final before unpublishing: %w", err)
	}
	if !os.SameFile(partialInfo, outputInfo) {
		return errors.New("refusing to remove final path because it no longer aliases the partial")
	}
	if err := os.Remove(output); err != nil {
		return fmt.Errorf("remove published final alias: %w", err)
	}
	return nil
}

func (r *Recorder) markCancelled(id uuid.UUID) error {
	stateCtx, stateCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := r.DB.MarkDVRRecordingCancelled(stateCtx, id)
	stateCancel()
	return r.terminalStateError("mark cancelled", id, err)
}

func (r *Recorder) markFailed(id uuid.UUID, reason string, bytesWritten int64) (string, error) {
	stateCtx, stateCancel := context.WithTimeout(context.Background(), 5*time.Second)
	updated, err := r.DB.MarkDVRRecordingFailed(stateCtx, id, reason, bytesWritten)
	state, stateErr := r.resolveTerminalCAS(stateCtx, "mark failed", "failed", id, updated, err)
	stateCancel()
	return state, stateErr
}

func (r *Recorder) markCompleted(id uuid.UUID, bytesWritten int64) (string, error) {
	return r.markCompletedWith(id, bytesWritten, r.DB.MarkDVRRecordingCompleted)
}

type markDVRCompletionFunc func(
	context.Context,
	uuid.UUID,
	uuid.UUID,
	int64,
) (store.DVRCompletionResult, error)

// markCompletedWith retries an uncertain completion with one operation UUID.
// Every attempt gets a new background-derived context: the context whose
// COMMIT response was lost may already be cancelled, and reusing it would make
// reconciliation fail without ever reaching PostgreSQL. The repository's
// recording-scoped advisory lock makes each retry wait for the predecessor,
// then read a fresh READ COMMITTED snapshot of its durable operation token.
func (r *Recorder) markCompletedWith(
	id uuid.UUID,
	bytesWritten int64,
	mark markDVRCompletionFunc,
) (string, error) {
	operationID := uuid.New()
	deadline := time.Now().Add(dvrCompletionReconcileBudget)
	attempts := 0
	var firstErr, lastErr error

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		attemptTimeout := min(dvrCompletionAttemptTimeout, remaining)
		attemptCtx, attemptCancel := context.WithTimeout(context.Background(), attemptTimeout)
		result, err := mark(attemptCtx, id, operationID, bytesWritten)
		attemptCancel()
		attempts++

		if err == nil {
			switch result.State {
			case "completed":
				if result.OperationOwned {
					return result.State, nil
				}
				return result.State, r.terminalStateError("mark completed", id,
					fmt.Errorf("completed state is owned by a different or legacy operation"))
			case "cancelled":
				if result.OperationOwned {
					return result.State, r.terminalStateError("mark completed", id,
						errors.New("cancelled state unexpectedly owns completion operation"))
				}
				return result.State, nil
			default:
				return result.State, r.terminalStateError("mark completed", id,
					fmt.Errorf("completion compare-and-set stopped in unexpected state %q", result.State))
			}
		}

		if firstErr == nil {
			firstErr = err
		}
		lastErr = err
		if errors.Is(err, store.ErrNotFound) {
			return "", r.terminalStateError("mark completed", id, err)
		}

		remaining = time.Until(deadline)
		if remaining <= 0 {
			break
		}
		delay := min(dvrCompletionRetryDelay, remaining)
		timer := time.NewTimer(delay)
		<-timer.C
	}

	return "", r.terminalStateError("mark completed reconcile", id, fmt.Errorf(
		"%w: operation %s unresolved after %d fresh attempts: first error: %v; last error: %w",
		ErrRecordingCompletionUnresolved, operationID, attempts, firstErr, lastErr))
}

func (r *Recorder) recordFailure(
	id uuid.UUID,
	primary error,
	reason string,
	bytesWritten int64,
) error {
	state, stateErr := r.markFailed(id, reason, bytesWritten)
	if state == "cancelled" && stateErr == nil {
		return nil
	}
	return errors.Join(primary, stateErr)
}

func (r *Recorder) resolveTerminalCAS(
	ctx context.Context,
	operation, target string,
	id uuid.UUID,
	updated bool,
	err error,
) (string, error) {
	if err != nil {
		return "", r.terminalStateError(operation, id, err)
	}
	if updated {
		return target, nil
	}
	current, getErr := r.DB.GetDVRRecording(ctx, id)
	if getErr != nil {
		return "", r.terminalStateError(operation+" inspect lost CAS", id, getErr)
	}
	if current.State == "cancelled" || current.State == target {
		return current.State, nil
	}
	return current.State, r.terminalStateError(operation, id,
		fmt.Errorf("compare-and-set lost in unexpected state %q", current.State))
}

func (r *Recorder) terminalStateError(operation string, id uuid.UUID, err error) error {
	if err == nil {
		return nil
	}
	r.Logger.Error("dvr terminal state update failed",
		"operation", operation, "recording_id", id, "err", err)
	return fmt.Errorf("%w: %s for %s: %w",
		ErrRecordingTerminalState, operation, id, err)
}
