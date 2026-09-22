package dvr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

// Scheduler is the background goroutine that watches dvr_recording for
// rows whose scheduled_start is approaching, kicks off the Recorder, and
// tracks in-flight recordings so the same row isn't started twice.
//
// Run cadence: TickInterval (default 30s). Lookahead window: WindowAhead
// (default 1m). The database query horizon is widened to Recorder.StartSlack
// when configured pre-roll is larger, so every recording enters the candidate
// set by its padded start.
type Scheduler struct {
	Logger       *slog.Logger
	DB           *store.DB
	Recorder     *Recorder
	TickInterval time.Duration
	WindowAhead  time.Duration
	// LiveReserve is the number of slots each candidate capacity domain should
	// keep available for live viewers. Store clamps it to capacity-1 so an
	// isolated one-slot provider can still record.
	LiveReserve int
	// CapacityRetryInterval is the fallback cadence when no release edge is
	// observed. CapacityRetryWindow bounds lateness from padded start; the
	// deadline is also capped at scheduled_end.
	CapacityRetryInterval time.Duration
	CapacityRetryWindow   time.Duration
	// AdmissionAttemptTimeout bounds one synchronous row-lock/pool attempt.
	// It is deliberately shorter than the retry cadence so one contended
	// provider cannot consume that full cadence for every later due row.
	AdmissionAttemptTimeout time.Duration
	// RecordingHeartbeatTimeout protects ownerless recording rows after their
	// ordinary capture window. RecordingFinalizationMargin is added after the
	// media processor's own timeout to form the absolute local-worker ceiling.
	RecordingHeartbeatTimeout   time.Duration
	RecordingFinalizationMargin time.Duration

	mu       sync.Mutex
	inFlight map[uuid.UUID]context.CancelCauseFunc

	// Deterministic seams for focused reconciliation tests. Production leaves
	// these nil and uses DB plus the wall clock directly.
	listStaleRecordings func(context.Context, time.Duration, time.Duration, time.Duration) ([]store.DVRRecording, error)
	markRecordingFailed func(context.Context, uuid.UUID, string, int64) (bool, error)
	now                 func() time.Time

	// lifecycleMu closes the WaitGroup Add/Wait seam. Close sets closing under
	// this lock; Run registration and every recording Add happen under the same
	// lock, so Wait can never return before a late recorder goroutine appears.
	lifecycleMu   sync.Mutex
	lifecycleWG   sync.WaitGroup
	closing       bool
	running       bool
	runCancel     context.CancelFunc
	lifecycleErrs []error
}

const (
	DefaultLiveReserve                 = 1
	DefaultCapacityRetryInterval       = 5 * time.Second
	DefaultCapacityRetryWindow         = 5 * time.Minute
	DefaultAdmissionAttemptTimeout     = 250 * time.Millisecond
	DefaultRecordingHeartbeatTimeout   = 30 * time.Second
	DefaultRecordingFinalizationMargin = 2 * time.Minute
)

var errRecordingAbsoluteRuntimeCeiling = errors.New("DVR recording exceeded absolute runtime ceiling")

func NewScheduler(logger *slog.Logger, db *store.DB, recorder *Recorder) *Scheduler {
	return &Scheduler{
		Logger:                      logger,
		DB:                          db,
		Recorder:                    recorder,
		TickInterval:                30 * time.Second,
		WindowAhead:                 1 * time.Minute,
		LiveReserve:                 DefaultLiveReserve,
		CapacityRetryInterval:       DefaultCapacityRetryInterval,
		CapacityRetryWindow:         DefaultCapacityRetryWindow,
		AdmissionAttemptTimeout:     DefaultAdmissionAttemptTimeout,
		RecordingHeartbeatTimeout:   DefaultRecordingHeartbeatTimeout,
		RecordingFinalizationMargin: DefaultRecordingFinalizationMargin,
		inFlight:                    make(map[uuid.UUID]context.CancelCauseFunc),
	}
}

// Run blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ctx, ok := s.beginRun(ctx)
	if !ok {
		return
	}
	defer s.endRun()

	s.Logger.Info("dvr scheduler starting",
		"tick", s.TickInterval, "window_ahead", s.WindowAhead,
		"live_reserve", s.LiveReserve,
		"capacity_retry_interval", s.CapacityRetryInterval,
		"capacity_retry_window", s.CapacityRetryWindow,
		"admission_attempt_timeout", s.admissionAttemptTimeout(),
		"recording_heartbeat_timeout", s.recordingHeartbeatTimeout(),
		"recording_absolute_grace", s.recordingAbsoluteGrace())

	t := time.NewTicker(s.TickInterval)
	defer t.Stop()
	retryInterval := s.CapacityRetryInterval
	if retryInterval <= 0 {
		retryInterval = DefaultCapacityRetryInterval
	}
	retry := time.NewTicker(retryInterval)
	defer retry.Stop()

	// Migration 0019 backfills existing future rows as pending. Refresh once
	// on every startup so an upgrade exposes their advisory conflicts even if
	// no new schedule request arrives. Failure is non-fatal: the transactional
	// runtime reservation below remains authoritative.
	s.refreshForecast(ctx, "startup")

	// Tick once immediately so a freshly-restarted Conductor catches any
	// imminent recordings without waiting a full interval.
	s.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			s.Logger.Info("dvr scheduler stopping")
			s.cancelAll()
			return
		case <-t.C:
			// Forecast metadata is advisory and must converge after external
			// cancellation, completion, or source/credential edits. Bound its
			// staleness to the normal scheduler cadence; the faster retry ticker
			// remains runtime-admission-only.
			s.refreshForecast(ctx, "periodic")
			s.tick(ctx)
		case <-retry.C:
			s.tick(ctx)
		case <-s.Recorder.Pool.CapacityChanges():
			// A release edge is only a hint. Move the visible retry timestamp
			// forward, then run the same DB-backed reservation path as every
			// other attempt. Correctness does not depend on this UPDATE because
			// every tick lists all due queued rows chronologically.
			if err := s.DB.WakeQueuedDVRRecordings(ctx); err != nil {
				s.Logger.Warn("dvr capacity wake failed", "err", err)
			}
			s.tick(ctx)
		}
	}
}

// Cancel asks the scheduler to stop one in-flight recording. Idempotent.
// The recording will be marked cancelled when the recorder unwinds.
func (s *Scheduler) Cancel(id uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cancel, ok := s.inFlight[id]
	if !ok {
		return false
	}
	cancel(context.Canceled)
	return true
}

func (s *Scheduler) tick(ctx context.Context) {
	s.reconcileRecordingZombies(ctx)
	if ctx.Err() != nil {
		return
	}
	// Every boundary includes all due queued rows. admission_retry_at is a
	// visible next-attempt estimate, never a filter that can hide an earlier
	// non-preemptible row while a later row overtakes it.
	pending, err := s.DB.ListPendingDVRRecordings(ctx, s.queryLookahead())
	if err != nil {
		s.Logger.Warn("dvr scheduler list failed", "err", err)
		return
	}

	for _, rec := range pending {
		now := time.Now()
		paddedStart := rec.ScheduledStart.Add(-s.Recorder.StartSlack)
		if now.Before(paddedStart) {
			continue
		}

		// Skip if already in-flight (we picked it up on a prior tick and
		// the atomic scheduled->recording claim has not committed yet).
		s.mu.Lock()
		_, running := s.inFlight[rec.ID]
		s.mu.Unlock()
		if running {
			continue
		}

		deadline := s.capacityRetryDeadline(rec)
		if !now.Before(deadline) {
			reason := fmt.Sprintf("dvr admission deadline %s expired",
				deadline.UTC().Format(time.RFC3339))
			if rec.AdmissionReason != "" {
				reason += ": last admission result: " + rec.AdmissionReason
			}
			s.failAdmission(ctx, rec, reason)
			s.Logger.Warn("dvr admission deadline missed",
				"id", rec.ID, "title", rec.Title, "priority", rec.Priority,
				"deadline", deadline, "attempts", rec.AdmissionAttempts)
			continue
		}

		// Provider/channel locks and pool checkout are database I/O and can
		// block. Use a short per-row budget independent of the slower fallback
		// retry cadence so unrelated due recordings keep progressing.
		attemptDeadline := now.Add(s.admissionAttemptTimeout())
		if attemptDeadline.After(deadline) {
			attemptDeadline = deadline
		}
		attemptCtx, attemptCancel := context.WithDeadline(ctx, attemptDeadline)
		reservation, err := s.Recorder.Pool.ReserveWriter(
			attemptCtx, rec.ChannelID, s.LiveReserve)
		attemptFinished := time.Now()
		attemptCancel()
		if ctx.Err() != nil {
			if reservation != nil {
				reservation.Release()
			}
			return
		}
		retryableContention := errors.Is(err, store.ErrAdmissionContention) &&
			!errors.Is(err, store.ErrLeaseOperational)
		if !attemptFinished.Before(deadline) {
			if reservation != nil {
				reservation.Release()
			}
			if err != nil && !errors.Is(err, store.ErrNoSlot) && !retryableContention {
				reason := fmt.Sprintf("dvr admission failed at deadline %s: %s",
					deadline.UTC().Format(time.RFC3339Nano), err)
				s.failAdmission(ctx, rec, reason)
				continue
			}
			detail := "reservation completed too late"
			if err != nil {
				detail = err.Error()
			} else if rec.AdmissionReason != "" {
				detail += "; last admission result: " + rec.AdmissionReason
			}
			reason := fmt.Sprintf("dvr admission deadline %s expired after slot contention: %s",
				deadline.UTC().Format(time.RFC3339Nano), detail)
			s.failAdmission(ctx, rec, reason)
			continue
		}
		if err == nil && !attemptFinished.Before(attemptDeadline) {
			reservation.Release()
			s.queueCapacityRetry(ctx, rec, attemptFinished, deadline,
				"provider admission attempt timed out before claim")
			continue
		}
		if errors.Is(err, store.ErrNoSlot) {
			s.queueCapacityRetry(ctx, rec, attemptFinished, deadline,
				"provider/domain capacity unavailable")
			continue
		}
		if retryableContention {
			s.queueCapacityRetry(ctx, rec, attemptFinished, deadline,
				"provider admission lock contended: "+err.Error())
			continue
		}
		if err != nil {
			reason := "dvr admission failed: " + err.Error()
			s.failAdmission(ctx, rec, reason)
			s.Logger.Warn("dvr recording admission failed", "id", rec.ID, "err", err)
			continue
		}

		s.launchRecording(ctx, rec, reservation, deadline)
	}
}

type recordingReapAction int

const (
	recordingReapNone recordingReapAction = iota
	recordingReapOwnerless
	recordingReapAbsolute
)

// reconcileRecordingZombies repairs rows whose recorder goroutine disappeared
// and imposes one absolute bound on a live worker, including media
// normalization. A stale DB heartbeat never terminates a locally-owned worker
// before that absolute deadline: the in-memory owner is authoritative while
// the runtime singleton is held.
func (s *Scheduler) reconcileRecordingZombies(ctx context.Context) {
	if ctx.Err() != nil || s.Recorder == nil {
		return
	}
	list := s.listStaleRecordings
	if list == nil {
		if s.DB == nil {
			return
		}
		list = s.DB.ListStaleDVRRecordings
	}

	hardGrace := s.recordingAbsoluteGrace()
	recordings, err := list(
		ctx,
		s.Recorder.MaxOverrun,
		hardGrace,
		s.recordingHeartbeatTimeout(),
	)
	if err != nil {
		if ctx.Err() == nil {
			s.Logger.Warn("dvr stale recording scan failed", "err", err)
		}
		return
	}
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	for _, rec := range recordings {
		if ctx.Err() != nil {
			return
		}
		s.reconcileRecordingZombie(ctx, rec, now, hardGrace)
	}
}

func (s *Scheduler) reconcileRecordingZombie(
	ctx context.Context,
	rec store.DVRRecording,
	now time.Time,
	hardGrace time.Duration,
) {
	s.mu.Lock()
	cancel, locallyOwned := s.inFlight[rec.ID]
	s.mu.Unlock()

	action := recordingReapActionAt(
		rec,
		now,
		s.Recorder.MaxOverrun,
		hardGrace,
		s.recordingHeartbeatTimeout(),
		locallyOwned,
	)
	if action == recordingReapNone {
		return
	}

	ordinaryDeadline := rec.ScheduledEnd.Add(s.Recorder.MaxOverrun)
	hardDeadline := ordinaryDeadline.Add(hardGrace)
	lastHeartbeat := dvrRecordingHeartbeat(rec)
	var reason string
	switch action {
	case recordingReapAbsolute:
		reason = fmt.Sprintf(
			"%s: scheduled_end=%s max_overrun=%s media_process_timeout=%s margin=%s absolute_deadline=%s",
			errRecordingAbsoluteRuntimeCeiling,
			rec.ScheduledEnd.UTC().Format(time.RFC3339Nano),
			s.Recorder.MaxOverrun,
			mediaProcessTimeout,
			s.recordingFinalizationMargin(),
			hardDeadline.UTC().Format(time.RFC3339Nano),
		)
	case recordingReapOwnerless:
		reason = fmt.Sprintf(
			"recording worker missing after capture window and stale heartbeat: ordinary_deadline=%s last_heartbeat=%s heartbeat_timeout=%s",
			ordinaryDeadline.UTC().Format(time.RFC3339Nano),
			formatSchedulerHeartbeat(lastHeartbeat),
			s.recordingHeartbeatTimeout(),
		)
	default:
		return
	}

	markFailed := s.markRecordingFailed
	if markFailed == nil {
		if s.DB == nil {
			return
		}
		markFailed = s.DB.MarkDVRRecordingFailed
	}
	updated, err := markFailed(ctx, rec.ID, reason, rec.BytesWritten)
	if err != nil {
		if ctx.Err() == nil {
			s.Logger.Warn("dvr stale recording failure state update failed",
				"id", rec.ID, "action", action, "err", err)
		}
		return
	}
	if !updated {
		return
	}

	if action == recordingReapAbsolute && locallyOwned {
		// Persist failed first. Recorder's cancellation cleanup is compare-and-set
		// safe and cannot rewrite this reason to cancelled; the distinct context
		// cause also lets runRecording suppress the expected lost-terminal-CAS
		// result while preserving real lifecycle failures.
		cancel(errRecordingAbsoluteRuntimeCeiling)
	}
	s.Logger.Warn("dvr stale recording reaped",
		"id", rec.ID,
		"title", rec.Title,
		"locally_owned", locallyOwned,
		"absolute", action == recordingReapAbsolute,
		"reason", reason)
}

func recordingReapActionAt(
	rec store.DVRRecording,
	now time.Time,
	maxOverrun, hardGrace, heartbeatTimeout time.Duration,
	locallyOwned bool,
) recordingReapAction {
	ordinaryDeadline := rec.ScheduledEnd.Add(maxOverrun)
	if now.Before(ordinaryDeadline) {
		return recordingReapNone
	}
	if !now.Before(ordinaryDeadline.Add(hardGrace)) {
		return recordingReapAbsolute
	}
	if locallyOwned {
		return recordingReapNone
	}
	heartbeat := dvrRecordingHeartbeat(rec)
	if !heartbeat.IsZero() && !heartbeat.Before(now.Add(-heartbeatTimeout)) {
		return recordingReapNone
	}
	return recordingReapOwnerless
}

func dvrRecordingHeartbeat(rec store.DVRRecording) time.Time {
	if rec.RecordingHeartbeatAt != nil {
		return *rec.RecordingHeartbeatAt
	}
	if rec.StartedAt != nil {
		return *rec.StartedAt
	}
	return rec.CreatedAt
}

func formatSchedulerHeartbeat(heartbeat time.Time) string {
	if heartbeat.IsZero() {
		return "none"
	}
	return heartbeat.UTC().Format(time.RFC3339Nano)
}

func (s *Scheduler) recordingHeartbeatTimeout() time.Duration {
	if s.RecordingHeartbeatTimeout <= 0 {
		return DefaultRecordingHeartbeatTimeout
	}
	return s.RecordingHeartbeatTimeout
}

func (s *Scheduler) recordingFinalizationMargin() time.Duration {
	if s.RecordingFinalizationMargin <= 0 {
		return DefaultRecordingFinalizationMargin
	}
	return s.RecordingFinalizationMargin
}

func (s *Scheduler) recordingAbsoluteGrace() time.Duration {
	return mediaProcessTimeout + s.recordingFinalizationMargin()
}

// queryLookahead returns the database horizon needed to observe every row by
// its padded start. WindowAhead still controls the normal scheduler horizon,
// while a larger configured StartSlack must widen the query so pre-roll does
// not begin late.
func (s *Scheduler) queryLookahead() time.Duration {
	lookahead := s.WindowAhead
	if s.Recorder != nil && s.Recorder.StartSlack > lookahead {
		lookahead = s.Recorder.StartSlack
	}
	return lookahead
}

func (s *Scheduler) beginRun(parent context.Context) (context.Context, bool) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closing || s.running {
		return nil, false
	}
	ctx, cancel := context.WithCancel(parent)
	s.running = true
	s.runCancel = cancel
	s.lifecycleWG.Add(1)
	return ctx, true
}

func (s *Scheduler) endRun() {
	s.lifecycleMu.Lock()
	s.running = false
	s.runCancel = nil
	s.lifecycleMu.Unlock()
	s.lifecycleWG.Done()
}

func (s *Scheduler) launchRecording(
	parent context.Context,
	rec store.DVRRecording,
	reservation *stream.WriterReservation,
	admissionDeadline time.Time,
) {
	recCtx, cancel := context.WithCancelCause(parent)
	s.lifecycleMu.Lock()
	if s.closing {
		s.lifecycleMu.Unlock()
		cancel(context.Canceled)
		reservation.Release()
		return
	}
	s.lifecycleWG.Add(1)
	s.mu.Lock()
	s.inFlight[rec.ID] = cancel
	s.mu.Unlock()
	s.lifecycleMu.Unlock()

	go func() {
		defer s.lifecycleWG.Done()
		s.runRecording(recCtx, rec, reservation, admissionDeadline)
	}()
}

// Close prevents new recording goroutines, cancels the Run loop and every
// recorder context, and is idempotent. Recorder cleanup still uses the live
// Pool/DB, so callers must Wait before closing those dependencies.
func (s *Scheduler) Close() {
	s.lifecycleMu.Lock()
	s.closing = true
	cancel := s.runCancel
	s.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.cancelAll()
}

// Wait closes the scheduler and waits for Run plus every recorder to finish
// reservation release, file close, and durable terminal-state cleanup.
func (s *Scheduler) Wait(ctx context.Context) error {
	s.Close()
	done := make(chan struct{})
	go func() {
		s.lifecycleWG.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		s.lifecycleMu.Lock()
		err := errors.Join(s.lifecycleErrs...)
		s.lifecycleMu.Unlock()
		return err
	}
}

func (s *Scheduler) queueCapacityRetry(
	ctx context.Context,
	rec store.DVRRecording,
	now, deadline time.Time,
	detail string,
) {
	retryAt := now.Add(s.capacityRetryInterval())
	if retryAt.After(deadline) {
		retryAt = deadline
	}
	reason := fmt.Sprintf(
		"queued for upstream capacity (%s; priority=%d, live_reserve=%d, deadline=%s)",
		detail, rec.Priority, s.LiveReserve, deadline.UTC().Format(time.RFC3339Nano))
	if err := s.DB.QueueDVRRecordingForCapacity(ctx, rec.ID, retryAt, reason); err != nil {
		s.Logger.Warn("dvr queue capacity conflict failed", "id", rec.ID, "err", err)
		return
	}
	s.Logger.Info("dvr recording queued for capacity",
		"id", rec.ID, "title", rec.Title, "priority", rec.Priority,
		"retry_at", retryAt, "deadline", deadline, "detail", detail)
}

func (s *Scheduler) refreshForecast(ctx context.Context, phase string) {
	if err := s.RefreshAdmissionForecast(ctx); err != nil && ctx.Err() == nil {
		s.Logger.Warn("dvr admission forecast refresh failed", "phase", phase, "err", err)
	}
}

// RefreshAdmissionForecast synchronously recomputes schedule-time conflicts
// with the scheduler's exact runtime policy. The authenticated provider PATCH
// uses this after a capacity-domain edit so its response is a convergence
// boundary rather than waiting for the next periodic tick.
func (s *Scheduler) RefreshAdmissionForecast(ctx context.Context) error {
	policy := store.DVRAdmissionPolicy{LiveReserve: s.LiveReserve}
	if s.Recorder != nil {
		policy.StartSlack = s.Recorder.StartSlack
		policy.MaxOverrun = s.Recorder.MaxOverrun
	}
	return s.DB.RefreshDVRAdmissionForecast(ctx, policy)
}

func (s *Scheduler) failAdmission(ctx context.Context, rec store.DVRRecording, reason string) {
	failed, err := s.DB.FailDVRRecordingAdmission(ctx, rec.ID, reason)
	if err != nil {
		s.Logger.Warn("dvr admission failure state update failed", "id", rec.ID, "err", err)
		return
	}
	if !failed || s.Recorder.Alerts == nil {
		return
	}
	go s.Recorder.Alerts.Send(context.Background(), alerts.Event{
		Severity: alerts.SevError,
		Source:   "conductor.dvr",
		Title:    "DVR admission failed: " + rec.Title,
		Detail:   reason,
		Tags: map[string]any{
			"recording_id": rec.ID.String(),
			"channel_id":   rec.ChannelID.String(),
			"priority":     rec.Priority,
		},
	})
}

func (s *Scheduler) capacityRetryInterval() time.Duration {
	if s.CapacityRetryInterval <= 0 {
		return DefaultCapacityRetryInterval
	}
	return s.CapacityRetryInterval
}

func (s *Scheduler) admissionAttemptTimeout() time.Duration {
	timeout := s.AdmissionAttemptTimeout
	if timeout <= 0 {
		timeout = DefaultAdmissionAttemptTimeout
	}
	// An explicitly faster retry cadence is also a reasonable upper bound for
	// one attempt; never let the attempt outlive the next fallback opportunity.
	if retry := s.capacityRetryInterval(); retry < timeout {
		return retry
	}
	return timeout
}

func (s *Scheduler) capacityRetryDeadline(rec store.DVRRecording) time.Time {
	window := s.CapacityRetryWindow
	if window <= 0 {
		window = DefaultCapacityRetryWindow
	}
	deadline := rec.ScheduledStart.Add(-s.Recorder.StartSlack).Add(window)
	if deadline.After(rec.ScheduledEnd) {
		deadline = rec.ScheduledEnd
	}
	return deadline
}

func (s *Scheduler) runRecording(
	ctx context.Context,
	rec store.DVRRecording,
	reservation *stream.WriterReservation,
	admissionDeadline time.Time,
) {
	defer func() {
		s.mu.Lock()
		delete(s.inFlight, rec.ID)
		s.mu.Unlock()
	}()

	if ctx.Err() != nil {
		reservation.Release()
		return
	}

	// Run the recording against the exact lease admitted above. A lost atomic
	// claim is benign; RunReserved releases without touching the file.
	if err := s.Recorder.RunReservedBefore(ctx, rec, reservation, admissionDeadline); err != nil {
		// The watchdog already persisted the distinct failed state before it
		// cancelled this exact owner. Recorder cleanup may consequently observe
		// either context cancellation or a lost terminal CAS; both are expected,
		// and must not turn Scheduler.Wait into a false quiescence failure.
		if errors.Is(context.Cause(ctx), errRecordingAbsoluteRuntimeCeiling) &&
			(errors.Is(err, context.Canceled) || errors.Is(err, ErrRecordingTerminalState)) {
			return
		}
		if errors.Is(err, ErrRecordingTerminalState) {
			s.lifecycleMu.Lock()
			s.lifecycleErrs = append(s.lifecycleErrs, err)
			s.lifecycleMu.Unlock()
		}
		if errors.Is(err, ErrRecordingAdmissionExpired) {
			reason := fmt.Sprintf("dvr admission deadline %s expired before claim",
				admissionDeadline.UTC().Format(time.RFC3339Nano))
			s.failAdmission(ctx, rec, reason)
			return
		}
		if !errors.Is(err, ErrRecordingClaimLost) && !errors.Is(err, context.Canceled) {
			s.Logger.Debug("dvr recorder returned", "id", rec.ID, "err", err)
		}
	}
}

func (s *Scheduler) cancelAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.inFlight {
		c(context.Canceled)
	}
}
