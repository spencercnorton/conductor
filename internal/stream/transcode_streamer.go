package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/transcode"
)

var errLocalTranscodeFailure = errors.New("local transcode pipeline failure")
var errUncertainTranscodeInput = errors.New("transcode input was not sustained at startup deadline")
var errTranscodeOutputIdle = errors.New("local transcode output made no progress")

const (
	confirmedInputMinBytes  = 64 * 1024
	confirmedInputMinWrites = 4
	confirmedInputMinSpan   = 500 * time.Millisecond
	confirmedInputMaxGap    = 500 * time.Millisecond
	// A bounded recovery can reject at most one second after the buffered
	// output starts. Leave one further diagnostic threshold for ffmpeg's
	// provider-input-to-output delay, then treat older gaps as unrelated.
	transcodeRecoveryReadGapWindow = maxTransportAddedLatency + diagnosticGapThreshold
	// Leave the provider-body startup Read deadline an attribution margin over
	// simultaneous silent stdout. This is a local processing bound, not Plex's
	// playback buffer: once live, a quiet provider Read can legitimately outlast
	// it. The output watchdog distinguishes that pause from a stuck transcoder.
	transcodeOutputAttributionMargin      = 250 * time.Millisecond
	defaultTranscodeOutputProgressTimeout = defaultIdleReadTimeout + transcodeOutputAttributionMargin
	providerReadWatchdogInterval          = 25 * time.Millisecond
)

// upstreamFlowEvidence distinguishes an actively flowing input from a short
// fragment that arrived and then stalled. Only the former is strong enough to
// blame a no-output startup timeout on local ffmpeg. Atomics keep the stdout
// reader and stdin pump independent without putting locks on the hot path.
type upstreamFlowEvidence struct {
	totalBytes        atomic.Int64
	streakBytes       atomic.Int64
	streakWrites      atomic.Int64
	streakStartUnixNS atomic.Int64
	lastUnixNS        atomic.Int64
}

func (e *upstreamFlowEvidence) record(n int, now time.Time) {
	if n <= 0 {
		return
	}
	ns := now.UnixNano()
	e.totalBytes.Add(int64(n))
	previous := e.lastUnixNS.Load()
	if previous == 0 || ns <= previous || time.Duration(ns-previous) > confirmedInputMaxGap {
		// Publish a reset before the new last timestamp. A concurrent reader
		// can therefore see either the old (now stale) streak or the new short
		// streak, never old byte volume paired with a fresh timestamp.
		e.streakBytes.Store(int64(n))
		e.streakWrites.Store(1)
		e.streakStartUnixNS.Store(ns)
	} else {
		e.streakBytes.Add(int64(n))
		e.streakWrites.Add(1)
	}
	e.lastUnixNS.Store(ns)
}

func (e *upstreamFlowEvidence) confirmed(now time.Time) bool {
	first, last := e.streakStartUnixNS.Load(), e.lastUnixNS.Load()
	return e.streakBytes.Load() >= confirmedInputMinBytes &&
		e.streakWrites.Load() >= confirmedInputMinWrites &&
		first > 0 && last >= first && time.Duration(last-first) >= confirmedInputMinSpan &&
		now.Sub(time.Unix(0, last)) <= confirmedInputMaxGap
}

type localTranscodeError struct{ cause error }

func (e *localTranscodeError) Error() string {
	return fmt.Sprintf("%s: %v", errLocalTranscodeFailure, e.cause)
}
func (e *localTranscodeError) Unwrap() error { return e.cause }
func (e *localTranscodeError) Is(target error) bool {
	return target == errLocalTranscodeFailure || errors.Is(e.cause, target)
}

func localTranscodeFailure(err error) error {
	if err == nil || errors.Is(err, errLocalTranscodeFailure) {
		return err
	}
	return &localTranscodeError{cause: err}
}

// checkedTranscodeInput withholds arbitrary body-read fragments until they
// form complete MPEG-TS packets and any selected PES optional timestamp header
// is structurally complete. This is the last gate before FFmpeg stdin: a later
// byte may prove a split packet/header corrupt, so no prefix of that unit can be
// written early and then become impossible to recall.
type checkedTranscodeInput struct {
	buffer                []byte
	pending               []byte
	tsPendingStart        int
	synced                bool
	bypass                bool
	packetsObserved       int
	programBound          bool
	nextProgramScan       int
	programScans          int
	programScanBytes      int
	safety                publicationAVGate
	mapObserver           selectedProgramMapObserver
	audioOriginCorrection time.Duration
	allowAudioClockRepair bool
}

const (
	maxCheckedTranscodeInputPending   = 2 * 1024 * 1024
	maxCheckedInputProgramScanSpacing = 64 * 1024
)

func newCheckedTranscodeInput() *checkedTranscodeInput {
	return &checkedTranscodeInput{}
}

func checkedInputProgram(identity selectedProgramMapIdentity) (int, bool) {
	if !identity.valid() {
		return 0, false
	}
	var accumulator *patTableAccumulator
	for _, raw := range identity.patSections {
		section, ok := parsePATSection(raw)
		if !ok || !section.current {
			return 0, false
		}
		if accumulator == nil {
			accumulator = newPATTableAccumulator(section)
		}
		if !accumulator.add(section, 0, raw) {
			continue
		}
		table, ok := accumulator.activate()
		if !ok {
			return 0, false
		}
		return len(table.programs), true
	}
	return 0, false
}

type checkedInputProgramSelection struct {
	program avProgramPIDs
	start   int
	prepend []byte
}

func selectCheckedInputProgram(data []byte) (checkedInputProgramSelection, bool, error) {
	programs := scanTSProgramMaps(data, "")
	if len(programs) == 0 {
		return checkedInputProgramSelection{}, false, nil
	}
	programCount, ok := checkedInputProgram(programs[0].mapIdentity)
	if !ok {
		return checkedInputProgramSelection{}, false, nil
	}
	// FFmpeg maps 0:v:0 and 0:a:0, but its cross-program selection in a true
	// MPTS is not represented in Conductor. Never guess and then validate a
	// different programme: reject the attempt privately before stdin instead.
	if programCount != 1 || len(programs) != 1 {
		return checkedInputProgramSelection{}, false, fmt.Errorf(
			"%w: MPEG-TS input declares %d programmes", errUncertainTranscodeInput, programCount)
	}
	selected := programs[0]
	if selected.audioCount > maxAVTimelineAudioPIDs {
		return checkedInputProgramSelection{}, false, fmt.Errorf(
			"%w: selected programme declares %d audio tracks (safety limit %d)",
			errUncertainTranscodeInput, selected.audioCount, maxAVTimelineAudioPIDs)
	}
	return checkedInputProgramSelection{
		program: avProgramPIDs{
			videoPID: selected.videoPID, pcrPID: selected.pcrPID,
			audioPIDs: selected.audioPIDs, audioCount: selected.audioCount,
			mapIdentity: selected.mapIdentity.clone(),
		},
		start:   selected.safeStart,
		prepend: append([]byte(nil), selected.psiPrefix...),
	}, true, nil
}

func (c *checkedTranscodeInput) bindAndValidatePending(observedAt time.Time) ([]byte, error) {
	c.programScans++
	input := c.pending[c.tsPendingStart:]
	c.programScanBytes += len(input)
	selection, ready, err := selectCheckedInputProgram(input)
	if err != nil || !ready {
		return nil, err
	}
	if selection.start < 0 || selection.start > len(input) {
		return nil, fmt.Errorf("%w: selected release barrier %d exceeds %d bytes",
			errUncertainTranscodeInput, selection.start, len(input))
	}
	candidate := make([]byte, 0, len(selection.prepend)+len(input)-selection.start)
	candidate = append(candidate, selection.prepend...)
	candidate = append(candidate, input[selection.start:]...)
	c.pending = candidate
	c.tsPendingStart = 0
	c.programBound = true
	c.safety = publicationAVGate{}
	if c.allowAudioClockRepair {
		c.safety.mode = publicationGateRepairableInput
	}
	c.safety.timeline.setFirstAudioOriginCorrection(c.audioOriginCorrection)
	c.safety.bindSelectedProgram(selection.program, observedAt, true)
	c.mapObserver.configure(selection.program.mapIdentity)
	mapPending := false
	for pos := c.tsPendingStart; pos+tsPacketSize <= len(c.pending); pos += tsPacketSize {
		var changed bool
		mapPending, changed = c.mapObserver.observeAt(c.pending[pos:pos+tsPacketSize], observedAt)
		if changed {
			c.pending = nil
			return nil, c.mapObserver.failure()
		}
	}
	checked, _, fault := c.safety.push(c.pending[c.tsPendingStart:], observedAt)
	if fault != nil {
		c.pending = nil
		return nil, fault
	}
	if mapPending || len(checked) == 0 {
		return nil, nil
	}
	out := append([]byte(nil), c.pending...)
	c.pending = nil
	c.tsPendingStart = 0
	return out, nil
}

func (c *checkedTranscodeInput) shouldScanProgram() bool {
	if c.programBound || c.packetsObserved < transportSyncPacketCount {
		return false
	}
	bytesObserved := len(c.pending) - c.tsPendingStart
	if c.nextProgramScan == 0 {
		c.nextProgramScan = transportSyncPacketCount * tsPacketSize
	}
	return bytesObserved >= c.nextProgramScan || len(c.pending) >= maxCheckedTranscodeInputPending
}

func (c *checkedTranscodeInput) scheduleNextProgramScan() {
	bytesObserved := len(c.pending) - c.tsPendingStart
	next := c.nextProgramScan
	if next == 0 {
		next = transportSyncPacketCount * tsPacketSize
	}
	for next <= bytesObserved {
		if next >= maxCheckedTranscodeInputPending/2 {
			next = maxCheckedTranscodeInputPending
			break
		}
		next *= 2
	}
	// Pure doubling bounds total work but can defer a just-arrived PMT by almost
	// a megabyte near the 2 MiB ceiling—seconds on a low-bitrate live source.
	// Keep discovery within 64 KiB while retaining a small, deterministic number
	// of whole-prefix scans. The resulting worst-case work is tens of MiB, not
	// the prior multi-gigabyte tiny-read O(n²) path.
	next = min(next, bytesObserved+maxCheckedInputProgramScanSpacing)
	c.nextProgramScan = next
}

func (c *checkedTranscodeInput) push(data []byte, observedAt time.Time) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if c.bypass {
		return data, nil
	}
	c.buffer = append(c.buffer, data...)
	var staged []byte
	if !c.synced {
		offset, ok := findTSSyncOffset(c.buffer)
		if !ok {
			if len(c.buffer) <= tsPacketSize*(transportSyncPacketCount+1) {
				return nil, nil
			}
			// Preserve compatibility with configured non-TS FFmpeg inputs. They do
			// not receive MPEG-TS continuity guarantees, but are not truncated.
			c.bypass = true
			out := append([]byte(nil), c.buffer...)
			c.buffer = nil
			return out, nil
		}
		if offset > 0 {
			c.pending = append(c.pending, c.buffer[:offset]...)
			c.buffer = c.buffer[offset:]
		}
		c.tsPendingStart = len(c.pending)
		c.synced = true
	}

	consumed := 0
	for consumed+tsPacketSize <= len(c.buffer) {
		packet := c.buffer[consumed : consumed+tsPacketSize]
		if packet[0] != 0x47 {
			return nil, &avContinuityError{kind: avFaultTransport}
		}
		c.pending = append(c.pending, packet...)
		c.packetsObserved++
		if c.programBound {
			mapPending, changed := c.mapObserver.observeAt(packet, observedAt)
			if changed {
				c.pending = nil
				return nil, c.mapObserver.failure()
			}
			checked, _, fault := c.safety.push(packet, observedAt)
			if fault != nil {
				c.pending = nil
				return nil, fault
			}
			if !mapPending && len(checked) > 0 {
				staged = append(staged, c.pending...)
				c.pending = nil
			}
			if mapPending && len(c.pending) > maxCheckedTranscodeInputPending {
				c.pending = nil
				return nil, errTransportProgramMapStall
			}
		}
		if len(c.pending) > maxCheckedTranscodeInputPending {
			c.pending = nil
			return nil, &avContinuityError{kind: avFaultTransport}
		}
		consumed += tsPacketSize
	}
	if consumed > 0 {
		copy(c.buffer, c.buffer[consumed:])
		c.buffer = c.buffer[:len(c.buffer)-consumed]
	}
	if c.shouldScanProgram() {
		bound, bindErr := c.bindAndValidatePending(observedAt)
		if bindErr != nil {
			return nil, bindErr
		}
		staged = append(staged, bound...)
		if !c.programBound {
			c.scheduleNextProgramScan()
		}
	}
	return staged, nil
}

// TranscodeStreamer is a drop-in alternative to Streamer for upstreams that
// have a transcode profile attached. It pumps upstream → ffmpeg.stdin and
// fans ffmpeg.stdout out to N subscribers, identical interface to Streamer
// (so Pool can substitute it without touching the rest of the pipeline).
//
// Resilience (audit S6, completing the Phase 5 treatment): the pump
// distinguishes which half died —
//
//   - upstream fault (read error, idle watchdog, HTTP error): same handling
//     as the passthrough pump — OnUpstreamDown re-leases (failover) and the
//     loop reconnects with capped backoff;
//   - ffmpeg fault (process exited / stdin write failed while the upstream
//     was healthy): respawn ffmpeg on the SAME source without dinging
//     source health, counted in conductor_ffmpeg_restarts_total.
//
// Both halves share the give-up window, the startup grace, the pre-roll
// ring discipline (cleared at gap start), and the Source Unavailable slate
// via the embedded subscriberSet.
type TranscodeStreamer struct {
	subscriberSet

	id           string
	upstream     string // current resolved URL; replaced on failover (pump goroutine only)
	profile      *transcode.Profile
	ffmpegBinary string
	logger       *slog.Logger
	hc           *http.Client

	idleReadTimeout        time.Duration
	stallTolerance         time.Duration
	outputProgressTimeout  time.Duration
	reconnectWindow        time.Duration
	stableDuration         time.Duration
	sourceStartupTimeout   time.Duration
	initialStartupDeadline time.Time
	// startupLead: see Streamer.startupLead and holdStartupLead.
	startupLead time.Duration
	// startupHoldDeadline: see Streamer.startupHoldDeadline. Source-attempt
	// budgets remain independent of the initiating subscriber's deadline.
	startupHoldDeadline time.Time
	classifier          finiteMediaClassifier
	prefixValidator     mediaPrefixValidator
	avContinuityPolicy  avContinuityPolicy
	// attemptFlowDuration measures actual output activity, not process setup
	// or teardown wall time. Pump goroutine only; see Streamer.
	attemptFlowDuration time.Duration
	// inputPumpHooks is a deterministic white-box seam for the exact provider
	// Read-deadline winner race. Production leaves it nil.
	inputPumpHooks *transcodeInputPumpTestHooks

	bytesOut   atomic.Int64
	mediaReady atomic.Bool

	ctMu        sync.Mutex
	contentType string

	OnRunning  func()
	OnStable   func()
	OnBytes    func(n int) error
	OnStopping func()
	OnExit     func(err error)

	// OnUpstreamDown — see Streamer. Consulted only for upstream-side
	// faults; ffmpeg deaths respawn locally.
	OnUpstreamDown func(ctx context.Context, cause error) (newURL string, retry bool, terminalErr error)

	// DiagDir, when non-empty, receives bounded refusal-head dumps from the
	// startup gates (see saveRefusalHead). CONDUCTOR_DIAG_DIR.
	DiagDir string
}

type transcodeInputPumpTestHooks struct {
	onPacerWait       func()
	onReadStart       func()
	onWriteStart      func()
	afterTimeoutWin   func()
	onTimeoutObserved func()
	onPumpExit        func()
	onOutputIdle      func(stdinWriteInFlight bool)
}

// NewTranscodeStreamer prepares a Streamer that runs ffmpeg.
// ffmpegBinary == "" → "ffmpeg" on PATH.
func NewTranscodeStreamer(id, upstream string, profile *transcode.Profile, ffmpegBinary string, logger *slog.Logger, hc *http.Client) *TranscodeStreamer {
	if hc == nil {
		hc = &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				ResponseHeaderTimeout: defaultSourceStartupTimeout,
				IdleConnTimeout:       30 * time.Second,
				DisableCompression:    true,
			},
		}
	}
	s := &TranscodeStreamer{
		id:                    id,
		upstream:              upstream,
		profile:               profile,
		ffmpegBinary:          ffmpegBinary,
		logger:                logger,
		hc:                    hc,
		idleReadTimeout:       defaultIdleReadTimeout,
		stallTolerance:        defaultLiveStallTolerance,
		outputProgressTimeout: defaultTranscodeOutputProgressTimeout,
		reconnectWindow:       defaultReconnectWindow,
		stableDuration:        defaultStableStreamDuration,
		sourceStartupTimeout:  defaultSourceStartupTimeout,
		classifier:            newFFmpegFiniteMediaClassifier(ffmpegBinary),
		avContinuityPolicy:    defaultAVContinuityPolicy(),
	}
	s.subscriberSet.init(id, logger)
	return s
}

// Subscribe / SubscriberCount come from the embedded subscriberSet, so the
// Pool can treat TranscodeStreamer and Streamer interchangeably.

func (s *TranscodeStreamer) BytesOut() int64  { return s.bytesOut.Load() }
func (s *TranscodeStreamer) MediaReady() bool { return s.mediaReady.Load() }

func (s *TranscodeStreamer) HeadersReady() bool { return s.MediaReady() }
func (s *TranscodeStreamer) ContentType() string {
	s.ctMu.Lock()
	defer s.ctMu.Unlock()
	if s.contentType == "" {
		// Conductor's transcode output is always MPEG-TS (per BuildArgs).
		return "video/mp2t"
	}
	return s.contentType
}

// Run drives connect → transcode → fanout attempts until the subscribers
// all leave, the context is cancelled, or downtime outlasts the reconnect
// window. Safe to call exactly once per TranscodeStreamer.
func (s *TranscodeStreamer) Run(ctx context.Context) {
	var runErr error
	defer func() {
		s.markStopping()
		if s.OnStopping != nil {
			s.OnStopping()
		}
		s.setTerminalError(runErr)
		s.closeAll()
		if st, dr := s.Stalls(), s.SubDrops(); st > 0 || dr > 0 {
			s.logger.Info("transcode streamer fanout stats",
				"stream", s.id, "stalls", st, "sub_drops", dr)
		}
		if s.OnExit != nil {
			s.OnExit(runErr)
		}
	}()

	started := time.Now()
	backoff := reconnectInitialBackoff
	var downSince time.Time

	for {
		gotBytes, upstreamFault, err := s.runOnce(ctx)
		if gotBytes && s.attemptFlowDuration >= s.stableDuration {
			downSince = time.Time{}
			backoff = reconnectInitialBackoff
			if s.OnStable != nil {
				s.OnStable()
			}
		}

		if ctx.Err() != nil {
			// Startup cleanup may cancel the pump at the same instant a
			// confirmed local ffmpeg fault returns. Preserve that attribution
			// for neutral logging/health handling; unrelated cancellation stays
			// a clean exit.
			if errors.Is(err, errLocalTranscodeFailure) {
				runErr = err
			}
			s.markStopping()
			return
		}
		var aborted *errPumpAborted
		if errors.As(err, &aborted) {
			runErr = aborted.err
			s.markStopping()
			return
		}
		// Every input/transcode attempt boundary invalidates pre-roll. Keep
		// advancing the epoch across repeated short fragments inside one
		// reconnect gap; otherwise media from attempt B can seed attempt C.
		s.clearRing()
		if s.tryMarkStoppingIfIdle(
			s.bytesOut.Load() > 0 || time.Since(started) > startupGrace) {
			runErr = err
			return
		}

		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		if !upstreamFault && !s.MediaReady() && errors.Is(err, errLocalTranscodeFailure) &&
			!s.initialStartupDeadline.IsZero() {
			remaining := time.Until(s.initialStartupDeadline)
			if remaining <= 0 || remaining <= backoff {
				// Do not start another request when a confirmed local ffmpeg
				// failure consumed the absolute budget (or left less than the
				// required retry backoff). Its cancellation could otherwise be
				// misattributed to the provider during asynchronous teardown.
				runErr = err
				s.markStopping()
				return
			}
		}
		if downSince.IsZero() {
			downSince = time.Now()
		}
		if time.Since(downSince) > s.reconnectWindow {
			runErr = fmt.Errorf("transcode pipeline down for %s, giving up: %w",
				time.Since(downSince).Round(time.Second), err)
			s.markStopping()
			return
		}

		s.startRecoveryFillerForFailure(ctx, err, downSince)

		if upstreamFault {
			if s.OnUpstreamDown == nil {
				runErr = err
				s.markStopping()
				return
			}
			newURL, retry, terminalErr := s.OnUpstreamDown(ctx, err)
			if !retry {
				runErr = err
				if terminalErr != nil {
					runErr = terminalErr
				}
				s.markStopping()
				return
			}
			if newURL != "" {
				s.upstream = newURL
			}
		} else {
			s.logLocalPipelineRestart(err)
		}
		if gotBytes {
			// Pumping continues past an attempt that already fanned out bytes:
			// the next attempt's media joins those bytes as a splice.
			s.noteUpstreamInterruption()
		}

		gapElapsed := time.Since(downSince)
		if requiresImmediateContinuityFiller(err) && gapElapsed < s.slateAfter {
			gapElapsed = s.slateAfter
		}
		if !s.waitBeforeRetry(ctx, backoff, gapElapsed) {
			s.markStopping()
			return // cancelled
		}
		backoff = min(backoff*2, reconnectMaxBackoff)
	}
}

// logLocalPipelineRestart records a same-source local pipeline restart.
func (s *TranscodeStreamer) logLocalPipelineRestart(err error) {
	// Retain the existing aggregate restart counter and sentinel thresholds.
	// Guard-directed respawns are real restarts, but do not prove FFmpeg exited.
	var attemptErr *sourceAttemptError
	errors.As(err, &attemptErr)
	if errors.Is(err, errRelayReadAheadFull) {
		s.logger.Warn("relay output buffer exhausted; restarting local pipeline",
			"stream", s.id, "err", err)
	} else if errors.Is(err, errTransportRecoveryBacklog) ||
		errors.Is(err, errTransportStaleChunk) ||
		errors.Is(err, errTransportRelayBacklog) ||
		errors.Is(err, errTransportAttemptBoundary) {
		s.logger.Warn("relay pacing backlog exceeded live bound; restarting local pipeline",
			"stream", s.id, "err", err)
	} else if errors.Is(err, errOutputClockBoundary) {
		metrics.FFmpegRestarts.Inc()
		s.logger.Warn("output clock join refused; restarting local pipeline",
			"stream", s.id, "err", err)
	} else if attemptErr != nil && attemptErr.stage == "output_av_publication" {
		metrics.FFmpegRestarts.Inc()
		s.logger.Warn("output A/V publication guard refused media; restarting local pipeline",
			"stream", s.id, "err", err)
	} else if errors.Is(err, errTranscodeOutputIdle) {
		metrics.FFmpegRestarts.Inc()
		s.logger.Warn("transcode output stalled; restarting local pipeline",
			"stream", s.id, "err", err)
	} else {
		metrics.FFmpegRestarts.Inc()
		s.logger.Warn("ffmpeg exited mid-stream; respawning",
			"stream", s.id, "err", err)
	}
}

// runOnce performs one upstream-connect + ffmpeg-spawn + pump attempt.
// upstreamFault reports whether the terminating failure was on the
// provider side (drives failover) as opposed to the ffmpeg side (drives a
// local respawn).
func (s *TranscodeStreamer) runOnce(ctx context.Context) (gotBytes, upstreamFault bool, err error) {
	s.attemptFlowDuration = 0
	var firstByteAt time.Time
	// 1. Open and, when finite, classify the upstream before ffmpeg or fanout.
	attemptTimeout := sourceAttemptBudget(
		s.sourceStartupTimeout, s.initialStartupDeadline, s.MediaReady())
	attemptStarted := time.Now()
	attempt := newUpstreamAttempt(ctx, s.upstream, s.id, s.logger, attemptTimeout)
	attempt.allowStartupProgressExtension(
		startupExtensionWall(s.initialStartupDeadline, s.MediaReady()))
	// Once stdin feeding has begun, upstream flow alone must not renew the
	// startup lease while ffmpeg has produced nothing — that state is the
	// local no-first-output fault and its bound stays unmodified. Pre-stdin
	// input-gate proving (post-break garbage) renews freely.
	var stdinFed atomic.Bool
	attempt.leaseVeto = func() bool {
		return stdinFed.Load() && attempt.diag.outputBytes.Load() == 0
	}
	// The profile maps 0:a:0 explicitly. Unlike passthrough, the transcode
	// pipeline therefore has a provable required audio PID when an input PMT
	// declares multiple tracks.
	attempt.diag.inputTimeline.requireFirstAudio = true
	attempt.diag.outputTimeline.requireFirstAudio = true
	defer func() { attempt.finish(err) }()
	if err = attempt.open(s.hc, s.upstream, "Conductor/stream", s.classifier); err != nil {
		return false, true, err
	}
	prefetched, audioOriginCorrection, preflightErr := preflightTranscodeAudioOrigin(
		attempt, s.profile, audioOriginProbeBudget(attemptTimeout, time.Since(attemptStarted)))
	if preflightErr != nil {
		saveRefusalHead(s.DiagDir, "audio-origin-refusal", s.id, prefetched, s.logger)
		return false, true, attempt.wrap("audio_origin", preflightErr)
	}
	attempt.diag.audioOriginCorrection = audioOriginCorrection
	attempt.diag.inputTimeline.setFirstAudioOriginCorrection(audioOriginCorrection)
	if audioOriginCorrection != 0 {
		s.logger.Info("applying bounded first-audio origin correction",
			"stream", s.id,
			"profile", s.profile.Name,
			"correction", audioOriginCorrection.Round(time.Millisecond))
	}

	// 2. Spawn ffmpeg.
	runner := transcode.New(s.ffmpegBinary, s.logger)
	if err := runner.StartWithAudioOriginCorrection(
		attempt.ctx, s.profile, audioOriginCorrection); err != nil {
		// Spawn failure is an ffmpeg-side fault (bad binary/args), not the
		// source's — but it is also not retry-curable if persistent; the
		// reconnect window bounds the futility either way.
		return false, false, localTranscodeFailure(err)
	}
	defer func() {
		_ = runner.Close()
	}()

	// 3. Pump upstream → ffmpeg.stdin in a goroutine, with the no-bytes
	// watchdog (audit S3) on the upstream read. Closing ffmpeg's stdin on
	// exit lets ffmpeg flush + EOF its stdout so the main loop ends instead
	// of hanging. upErr carries the upstream-side cause (nil = the upstream
	// was fine; the write side failed because ffmpeg died).
	upErr := make(chan error, 1)
	inputPumpDone := make(chan struct{})
	var idleKilled atomic.Bool
	var inputUnsafe atomic.Bool
	// A clean FFmpeg exit alone is insufficient: it can also flush after a
	// failed provider read closes stdin. Preserve raw, fully consumed entity
	// EOF independently of the live-source io.ErrUnexpectedEOF classification.
	var cleanProviderEOF atomic.Bool
	var inputSafetyFaultMu sync.Mutex
	var inputSafetyFault error
	var inputContinuityFaultMu sync.Mutex
	var inputContinuityFault *avContinuityError
	var stdinWriteInFlight atomic.Bool
	var providerReadInFlight atomic.Bool
	var providerReadStartedUnixNS atomic.Int64
	var upstreamFlow upstreamFlowEvidence
	inputSafety := newCheckedTranscodeInput()
	inputSafety.audioOriginCorrection = audioOriginCorrection
	// Only an owned, unoverridden encoded-audio filter establishes repair
	// capability. Copy/custom/operator-overridden paths retain strict raw clock
	// publication. Structural/PES/map checks and immediate gross-clock fences
	// remain mandatory even for eligible profiles.
	inputSafety.allowAudioClockRepair = s.profile.SupportsInputAudioClockRepair()
	storeInputSafetyFault := func(fault error) {
		inputSafetyFaultMu.Lock()
		inputSafetyFault = fault
		inputSafetyFaultMu.Unlock()
		inputUnsafe.Store(true)
	}
	loadInputSafetyFault := func() error {
		inputSafetyFaultMu.Lock()
		defer inputSafetyFaultMu.Unlock()
		if inputSafetyFault == nil {
			return &avContinuityError{kind: avFaultTransport}
		}
		return inputSafetyFault
	}
	storeInputContinuityFault := func(fault *avContinuityError) {
		inputContinuityFaultMu.Lock()
		if inputContinuityFault == nil {
			inputContinuityFault = fault
		}
		inputContinuityFaultMu.Unlock()
	}
	loadInputContinuityFault := func() *avContinuityError {
		inputContinuityFaultMu.Lock()
		defer inputContinuityFaultMu.Unlock()
		return inputContinuityFault
	}
	// Attempt-local readiness (see Streamer.runOnce): reconnect attempts fail
	// dead sources at the startup timeout, not the live tolerance.
	var attemptMediaLive atomic.Bool
	attemptIdleTimeout := func() time.Duration {
		if attemptMediaLive.Load() {
			return s.stallTolerance
		}
		return s.idleReadTimeout
	}
	// A proven input fault closes the attempt while output may be blocked in
	// pacing or validation. Those operations return cancellation, not the
	// input cause. Recover only cancellation-derived exits after the later
	// teardown defer joins the input pump and watchdog; explicit output faults
	// and cancellation of the parent viewer remain unchanged.
	defer func() {
		if ctx.Err() != nil || !errors.Is(err, context.Canceled) {
			return
		}
		var aborted *errPumpAborted
		if errors.As(err, &aborted) || errors.Is(err, errLocalTranscodeFailure) {
			// A callback abort is an explicit never-retry instruction; a
			// confirmed local failure also keeps its own evidence and scope.
			return
		}
		if idleKilled.Load() {
			upstreamFault = true
			err = attempt.wrap("stream", fmt.Errorf("%w (%s)", errUpstreamIdle, attemptIdleTimeout()))
			return
		}
		if inputUnsafe.Load() {
			fault := loadInputSafetyFault()
			var continuityFault *avContinuityError
			if errors.As(fault, &continuityFault) {
				recordAVContinuityFailure(true, continuityFault)
			}
			upstreamFault, err = true, attempt.wrap("input_transport", fault)
			return
		}
		if fault := loadInputContinuityFault(); fault != nil {
			recordAVContinuityFailure(true, fault)
			upstreamFault, err = true, attempt.wrap("input_av_continuity", fault)
		}
	}()
	readWatchdogDone := make(chan struct{})
	go func() {
		defer close(readWatchdogDone)
		ticker := time.NewTicker(providerReadWatchdogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-attempt.ctx.Done():
				return
			case now := <-ticker.C:
				started := providerReadStartedUnixNS.Load()
				if !providerReadInFlight.Load() || started <= 0 ||
					now.Sub(time.Unix(0, started)) < attemptIdleTimeout() {
					continue
				}
				if providerReadInFlight.CompareAndSwap(true, false) {
					if hooks := s.inputPumpHooks; hooks != nil && hooks.afterTimeoutWin != nil {
						hooks.afterTimeoutWin()
					}
					idleKilled.Store(true)
					attempt.Close()
					return
				}
			}
		}
	}()
	go func() {
		var cause error
		defer func() {
			upErr <- cause
			if wc, ok := runner.Stdin().(io.WriteCloser); ok {
				_ = wc.Close()
			}
			if hooks := s.inputPumpHooks; hooks != nil && hooks.onPumpExit != nil {
				hooks.onPumpExit()
			}
			close(inputPumpDone)
		}()
		buf := make([]byte, chunkSize)
		for {
			// The provider watchdog governs only the blocking body Read. Time
			// spent writing into FFmpeg is local backpressure; leaving the same
			// timer armed across Stdin.Write would falsely blame a healthy origin
			// when FFmpeg wedges both input and output. The attempt-local ticker
			// observes atomic phase/deadline state, avoiding one timer allocation
			// per small MPEG-TS body read.
			var input []byte
			var rerr error
			readWon := true
			if len(prefetched) > 0 {
				input = prefetched
				prefetched = nil
			} else {
				providerReadStartedUnixNS.Store(time.Now().UnixNano())
				providerReadInFlight.Store(true)
				if hooks := s.inputPumpHooks; hooks != nil && hooks.onReadStart != nil {
					hooks.onReadStart()
				}
				n, readErr := attempt.Read(buf)
				input, rerr = buf[:n], readErr
				readWon = providerReadInFlight.CompareAndSwap(true, false)
			}
			if !readWon {
				// The deadline callback won the exact read boundary. Do not feed a
				// concurrently returned tail into FFmpeg after the provider attempt
				// has already been closed. Losing the CAS is itself definitive
				// timeout evidence: the watcher may not have stored idleKilled or
				// canceled the context yet, so never derive the cause solely from a
				// momentarily nil attempt.ctx.Err().
				input = nil
				idleKilled.Store(true)
				rerr = fmt.Errorf("%w (%s)", errUpstreamIdle, attemptIdleTimeout())
				if hooks := s.inputPumpHooks; hooks != nil && hooks.onTimeoutObserved != nil {
					hooks.onTimeoutObserved()
				}
			}
			if len(input) > 0 && readWon {
				// Evaluate provider media while this exact body Read has just made
				// progress, before a subsequent FFmpeg stdin Write can block. A
				// confirmed one-sided A/V clock fault here is independent source
				// evidence; it must survive later local backpressure and outrank a
				// coincident stdout-idle timer.
				if fault := attempt.diag.inputTimeline.continuityFault(
					time.Now(), s.avContinuityPolicy); inputFaultProvesUpstreamWithoutOutput(fault) {
					storeInputContinuityFault(fault)
					cause = fault
					attempt.Close()
					return
				}
			}
			if len(input) > 0 {
				checked, safetyErr := inputSafety.push(input, time.Now())
				if safetyErr != nil {
					// Never allow a selected TEI/CC/scramble/malformed timestamp
					// packet to poison FFmpeg and surface seconds later as lost audio.
					// The decision is bound to this exact Read and happens before
					// Stdin.Write; cancellation also prevents buffered output from
					// escaping under the damaged input attempt.
					storeInputSafetyFault(safetyErr)
					cause = safetyErr
					attempt.Close()
					return
				}
				if len(checked) == 0 {
					if rerr != nil {
						// Preserve the terminal provider cause after safely withholding
						// an incomplete trailing transport unit.
						if !errors.Is(rerr, io.EOF) && ctx.Err() == nil {
							cause = rerr
						} else if errors.Is(rerr, io.EOF) {
							cause = io.ErrUnexpectedEOF
						}
						return
					}
					continue
				}
				stdinFed.Store(true)
				stdinWriteInFlight.Store(true)
				if hooks := s.inputPumpHooks; hooks != nil && hooks.onWriteStart != nil {
					hooks.onWriteStart()
				}
				written, werr := runner.Stdin().Write(checked)
				stdinWriteInFlight.Store(false)
				if written > 0 {
					upstreamFlow.record(written, time.Now())
				}
				if werr != nil {
					return // ffmpeg side died; cause stays nil
				}
			}
			if rerr != nil {
				if idleKilled.Load() {
					cause = fmt.Errorf("%w (%s)", errUpstreamIdle, attemptIdleTimeout())
				} else if !errors.Is(rerr, io.EOF) && ctx.Err() == nil {
					cause = rerr
				} else if errors.Is(rerr, io.EOF) {
					if ctx.Err() == nil && attempt.ctx.Err() == nil &&
						len(inputSafety.buffer) == 0 && len(inputSafety.pending) == 0 {
						cleanProviderEOF.Store(true)
					}
					cause = io.ErrUnexpectedEOF // live upstream ended
				}
				return
			}
		}
	}()

	s.ctMu.Lock()
	s.contentType = "video/mp2t"
	s.ctMu.Unlock()

	// 4. Pump ffmpeg.stdout → subscribers (the main loop).
	out := runner.Stdout()
	attemptReady := false
	validatedStartPending := false
	gate := newStartupMediaGate(s.prefixValidator)
	defer attempt.recordMediaPrefix(gate)
	pacer := &transportPCRPacer{}
	if hooks := s.inputPumpHooks; hooks != nil && hooks.onPacerWait != nil {
		// Synchronize fault tests only after the pacer has chosen a real
		// delay. The ordinary context-aware timer remains authoritative.
		pacer.wait = func(ctx context.Context, delay time.Duration) error {
			hooks.onPacerWait()
			return waitTransportDelay(ctx, delay)
		}
	}
	var publicationGate publicationAVGate
	var pesGate pesPublicationGate
	var groupAVFence completeGroupAVFence
	var clockEpoch *outputClockEpoch
	var clockProgram avProgramPIDs
	var clockProgramBound bool
	var lastAcceptedOutput time.Time
	var privateReleaseAt time.Time
	var pacingRelease chan struct{}
	var publicationProgress atomic.Int64
	readAhead := startRelayReadAhead
	if !attempt.replay {
		pacingRelease = make(chan struct{})
		readAhead = func(
			ctx context.Context,
			reader io.ReadCloser,
			onRead func([]byte, time.Time, time.Time),
			onDone func(),
		) (<-chan relayReadResult, <-chan error, <-chan struct{}) {
			return startLiveRelayReadAhead(ctx, reader, onRead, onDone, pacingRelease, &publicationProgress)
		}
	}
	reads, readOverflow, readDone := readAhead(attempt.ctx, out,
		func(data []byte, started, ended time.Time) {
			attempt.recordOutputData(data, ended.Sub(started))
		}, nil)
	defer func() {
		attempt.Close()
		// A local FFmpeg backpressure fault can leave this goroutine blocked in
		// Stdin.Write even after the provider body is canceled. Close our pipe
		// endpoint before joining so an old attempt cannot overlap a retry or
		// retain a write into the next process lifetime.
		if wc, ok := runner.Stdin().(io.WriteCloser); ok {
			_ = wc.Close()
		}
		<-readDone
		<-inputPumpDone
		<-readWatchdogDone
	}()
	outputIdle := time.NewTimer(time.Hour)
	if !outputIdle.Stop() {
		<-outputIdle.C
	}
	defer outputIdle.Stop()
	var outputIdleC <-chan time.Time
	outputProgressTimeout := s.outputProgressTimeout
	if outputProgressTimeout <= 0 {
		outputProgressTimeout = defaultTranscodeOutputProgressTimeout
	}
	armOutputIdle := func(timeout time.Duration) {
		if !outputIdle.Stop() {
			select {
			case <-outputIdle.C:
			default:
			}
		}
		outputIdle.Reset(timeout)
		outputIdleC = outputIdle.C
	}
	var lastPublication time.Time
	var pausedReadStarted, pausedInputLast int64
	// One input-resumption allowance per actual publication. Repeated reads or
	// writes without output must never keep a hung process alive indefinitely.
	pauseGraceUsed := false
	publishGroups := func(groups [][]byte, paceArrival time.Time) (bool, error) {
		if clockEpoch == nil && len(groups) > 0 {
			clockEpoch = newOutputClockEpoch(clockProgram, clockProgramBound, attempt.replay, groups)
		}
		for _, chunk := range groups {
			if fault := groupAVFence.inspect(chunk, time.Now()); fault != nil {
				recordAVContinuityFailure(false, fault)
				return false, attempt.wrap("output_av_publication", localTranscodeFailure(fault))
			}
			partLen := len(chunk)
			// Advance the ordinary bounded PCR schedule privately. Only the
			// whole completed group crosses the subscriber boundary.
			if len(chunk) > publicationChunkSize {
				pacer.PrepareSharedArrivalBatch(chunk, publicationChunkSize)
			}
			// All private pacing parts share one release edge. Refreshing it
			// per part would hide accumulated delay before the atomic fanout.
			if !attempt.replay {
				paceArrival = time.Now()
			}
			for remaining := chunk; len(remaining) > 0; {
				n := min(len(remaining), publicationChunkSize)
				pacer.NoteBacklog(len(reads))
				if paceErr := pacer.Wait(attempt.ctx, remaining[:n], paceArrival); paceErr != nil {
					if errors.Is(paceErr, errTransportSkipStale) {
						paceErr = fmt.Errorf("%w: %w", errTransportStaleChunk, paceErr)
					}
					upstreamFault, pacingErr := transcodePacingFailure(attempt, paceErr)
					return upstreamFault, pacingErr
				}
				remaining = remaining[n:]
				publicationProgress.Store(time.Now().UnixNano())
			}
			if paceErr := pacer.rejectStaleBeforeFanout(paceArrival); paceErr != nil {
				upstreamFault, pacingErr := transcodePacingFailure(attempt, paceErr)
				return upstreamFault, pacingErr
			}
			if clockErr := s.fanoutClockedReal(chunk, validatedStartPending, clockEpoch); clockErr != nil {
				if errors.Is(clockErr, context.Canceled) {
					return false, clockErr
				}
				return false, attempt.wrap("output_clock", localTranscodeFailure(clockErr))
			}
			publishedAt := time.Now()
			if firstByteAt.IsZero() {
				firstByteAt = publishedAt
			}
			s.attemptFlowDuration = publishedAt.Sub(firstByteAt)
			validatedStartPending = false
			publicationProgress.Store(time.Now().UnixNano())
			// Subscriber-visible bytes are the real output-progress contract.
			// Reset here (not merely on a pipe read) so a paced buffered prefix
			// remains healthy while it is actively feeding Plex.
			gotBytes = true
			s.bytesOut.Add(int64(partLen))
			s.mediaReady.Store(true)
			attemptMediaLive.Store(true)
			// Only real publication renews eligibility for another input
			// pause. A reconnect starts with its own empty progress state.
			lastPublication = publishedAt
			pausedReadStarted, pausedInputLast = 0, 0
			pauseGraceUsed = false
			armOutputIdle(outputProgressTimeout)
			if s.OnBytes != nil {
				if cbErr := s.OnBytes(partLen); cbErr != nil {
					return false, &errPumpAborted{err: cbErr}
				}
			}
		}
		return false, nil
	}
	for {
		var result relayReadResult
		select {
		case overflowErr := <-readOverflow:
			upstreamFault, relayErr := transcodeRelayBufferFailure(attempt, overflowErr)
			return gotBytes, upstreamFault, relayErr
		case next, ok := <-reads:
			if !ok {
				if overflowErr := takeRelayReadAheadError(readOverflow); overflowErr != nil {
					upstreamFault, relayErr := transcodeRelayBufferFailure(attempt, overflowErr)
					return gotBytes, upstreamFault, relayErr
				}
				result.err = attempt.ctx.Err()
				if result.err == nil {
					result.err = io.EOF
				}
			} else {
				result = next
			}
		case <-attempt.ctx.Done():
			attempt.Close()
			<-readDone
			if overflowErr := takeRelayReadAheadError(readOverflow); overflowErr != nil {
				upstreamFault, relayErr := transcodeRelayBufferFailure(attempt, overflowErr)
				return gotBytes, upstreamFault, relayErr
			}
			result.err = attempt.ctx.Err()
		case <-outputIdleC:
			if hooks := s.inputPumpHooks; hooks != nil && hooks.onOutputIdle != nil {
				hooks.onOutputIdle(stdinWriteInFlight.Load())
			}
			// Independent provider/transport evidence outranks an output timeout,
			// including while the output watchdog is deferring to a quiet Read.
			if idleKilled.Load() {
				return gotBytes, true, attempt.wrap("stream",
					fmt.Errorf("%w (%s)", errUpstreamIdle, attemptIdleTimeout()))
			}
			if inputUnsafe.Load() {
				fault := loadInputSafetyFault()
				var continuityFault *avContinuityError
				if errors.As(fault, &continuityFault) {
					recordAVContinuityFailure(true, continuityFault)
				}
				return gotBytes, true, attempt.wrap("input_transport", fault)
			}
			if inputFault := loadInputContinuityFault(); inputFault != nil {
				recordAVContinuityFailure(true, inputFault)
				return gotBytes, true, attempt.wrap("input_av_continuity", inputFault)
			}
			now := time.Now()
			inputFault := attempt.diag.inputTimeline.continuityFault(now, s.avContinuityPolicy)
			if !stdinWriteInFlight.Load() && upstreamFlow.confirmed(now) &&
				inputFaultProvesUpstreamWithoutOutput(inputFault) {
				recordAVContinuityFailure(true, inputFault)
				return gotBytes, true, attempt.wrap("input_av_continuity", inputFault)
			}
			readStarted := providerReadStartedUnixNS.Load()
			inputLast := upstreamFlow.lastUnixNS.Load()
			quietRead := providerReadInFlight.Load() && !stdinWriteInFlight.Load()
			if pausedReadStarted != 0 {
				if quietRead && readStarted == pausedReadStarted && inputLast == pausedInputLast {
					// The existing provider watchdog still bounds this exact Read.
					// Poll only while deferred so resumption is noticed promptly;
					// sleeping until the source deadline would hide a local hang.
					armOutputIdle(providerReadWatchdogInterval)
					continue
				}
				pausedReadStarted = 0
				pauseGraceUsed = true
				// Give newly available input one processing window, then require
				// output. This also lets a provider deadline's CAS winner publish
				// its cause before an input-phase change can be called local.
				armOutputIdle(outputProgressTimeout)
				continue
			}
			if !attempt.replay && attemptMediaLive.Load() && !pauseGraceUsed &&
				quietRead && readStarted > 0 && inputLast > 0 &&
				!lastPublication.IsZero() && (inputLast <= lastPublication.UnixNano() ||
				pesGate.waiting() && inputLast <= lastAcceptedOutput.UnixNano()) {
				// Output followed the last admitted stdin bytes, either published
				// or accepted into the pending PES group. The provider is now quiet;
				// there is no evidence yet that restarting
				// FFmpeg helps. This ordering is conservative evidence, not proof
				// that every internal media buffer has drained.
				pausedReadStarted, pausedInputLast = readStarted, inputLast
				armOutputIdle(providerReadWatchdogInterval)
				continue
			}
			return gotBytes, false, transcodeOutputIdleFailure(
				attempt, outputProgressTimeout)
		}
		if inputUnsafe.Load() {
			fault := loadInputSafetyFault()
			var continuityFault *avContinuityError
			if errors.As(fault, &continuityFault) {
				recordAVContinuityFailure(true, continuityFault)
			}
			return gotBytes, true, attempt.wrap("input_transport", fault)
		}
		if inputFault := loadInputContinuityFault(); inputFault != nil {
			recordAVContinuityFailure(true, inputFault)
			return gotBytes, true, attempt.wrap("input_av_continuity", inputFault)
		}
		n, rerr := len(result.data), result.err
		if rerr != nil && n == 0 && !attemptReady && gate.BufferedBytes() > 0 {
			media, ready, gateErr := gate.Finalize(attempt.ctx)
			if gateErr != nil {
				return gotBytes, true, attempt.wrap("media_prefix",
					errors.Join(errUncertainMediaPrefix, gateErr))
			}
			if ready {
				result.data = media
				n = len(media)
			}
		}
		if n > 0 {
			var media []byte
			var ready bool
			var gateErr error
			paceArrival := result.arrivedAt
			if !attempt.replay {
				// Dequeue is the live edge (see the passthrough read loop): the
				// stdout reservoir deliberately holds a transcoded burst, and
				// that hold must not be charged against the pacer's contract.
				paceArrival = time.Now()
			}
			if attemptReady {
				paceArrival = privateReleaseArrival(paceArrival, &privateReleaseAt)
			}
			if rerr != nil {
				media, ready, gateErr = gate.PushFinal(attempt.ctx, result.data)
			} else {
				media, ready, gateErr = gate.Push(attempt.ctx, result.data)
			}
			if gateErr != nil {
				return gotBytes, true, attempt.wrap("media_prefix",
					errors.Join(errUncertainMediaPrefix, gateErr))
			}
			if ready {
				initialPrefix := !attemptReady
				if !attemptReady {
					pcrPID, havePCRPID := gate.PCRPID()
					if !havePCRPID {
						pcrPID = -1
					}
					pacer.Configure(pcrPID, attempt.replay)
					program, haveProgram := gate.SelectedProgram()
					clockProgram, clockProgramBound = program, haveProgram
					pacer.ConfigureSelectedProgram(program, haveProgram)
					pesGate.configure(program, haveProgram)
					groupAVFence.configure(program, haveProgram, attempt.replay, paceArrival)
					if haveProgram {
						publicationGate.bindSelectedProgram(program, paceArrival, true)
					}
					if paceErr := pacer.PrepareBufferedPrefix(media, publicationChunkSize); paceErr != nil {
						upstreamFault, pacingErr := transcodePacingFailure(attempt, paceErr)
						return false, upstreamFault, pacingErr
					}
					if readyErr := attempt.markMediaReady(); readyErr != nil {
						return false, true, attempt.wrap("media_prefix",
							errors.Join(errUncertainMediaPrefix, readyErr))
					}
					attemptReady = true
					validatedStartPending = true
					if initialPrefix && s.OnRunning != nil {
						// Complete the bounded durable-running callback while the
						// prefix is private, then choose its pacing release edge.
						s.OnRunning()
					}
					if initialPrefix && s.startupLead > 0 && !s.mediaReady.Load() {
						// See Streamer.runOnce: validated media is live for the
						// provider read watchdog before the hold starts, and a
						// respawn never holds. The output watchdog is armed only
						// by publication, so it does not run during the hold.
						attemptMediaLive.Store(true)
						if holdErr := holdStartupLead(attempt.ctx, s.startupLead,
							s.startupHoldDeadline); holdErr != nil {
							return false, false, attempt.wrap("stream", holdErr)
						}
					}
					// Validation and schedule profiling happen behind a private
					// gate, as does the one-time running-state transition. Give the
					// complete initial slice one release-edge source timestamp;
					// queued post-gate reads retain their real arrivals.
					privateReleaseAt = time.Now()
					paceArrival = privateReleaseAt
					if pacingRelease != nil {
						close(pacingRelease)
						pacingRelease = nil
					}
				}
				media, gateErr = pacer.TransformForFanoutAt(media, paceArrival)
				if gateErr != nil {
					upstreamFault, pacingErr := transcodePacingFailure(attempt, gateErr)
					return gotBytes, upstreamFault, pacingErr
				}
				media, paceArrival, fault := publicationGate.push(media, paceArrival)
				if fault != nil {
					inputFault := attempt.diag.inputTimeline.continuityFault(
						time.Now(), s.avContinuityPolicy)
					selected, inputScope := selectTranscodeContinuityFault(inputFault, fault)
					recordAVContinuityFailure(inputScope, selected)
					if inputScope {
						return gotBytes, true, attempt.wrap("input_av_safety", selected)
					}
					return gotBytes, false, attempt.wrap("output_av_safety",
						localTranscodeFailure(selected))
				}
				now := time.Now()
				inputFault := attempt.diag.inputTimeline.continuityFault(now, s.avContinuityPolicy)
				outputFault := publicationGate.continuityFault(now, s.avContinuityPolicy)
				if fault, inputScope := selectTranscodeContinuityFault(inputFault, outputFault); fault != nil {
					recordAVContinuityFailure(inputScope, fault)
					if inputScope {
						return gotBytes, true, attempt.wrap("input_av_continuity", fault)
					}
					return gotBytes, false, attempt.wrap("output_av_continuity",
						localTranscodeFailure(fault))
				}
				groups, groupErr := pesGate.push(media)
				if groupErr != nil {
					upstreamFault, pacingErr := transcodePacingFailure(attempt, groupErr)
					return gotBytes, upstreamFault, pacingErr
				}
				if len(media) > 0 {
					lastAcceptedOutput = time.Now()
				}
				if upstreamFault, pubErr := publishGroups(groups, paceArrival); pubErr != nil {
					return gotBytes, upstreamFault, pubErr
				}
			}
		}
		if rerr != nil {
			// Only naturally completed, successful mux output can close its final
			// unbounded video PES. Canceled/failed attempts never flush this tail.
			if errors.Is(rerr, io.EOF) && cleanProviderEOF.Load() && attemptReady && ctx.Err() == nil && attempt.ctx.Err() == nil && !inputUnsafe.Load() && !idleKilled.Load() {
				if closeErr := runner.Close(); closeErr == nil && ctx.Err() == nil && attempt.ctx.Err() == nil {
					groups, groupErr := pesGate.finishCleanEOF()
					if groupErr != nil {
						up, pacingErr := transcodePacingFailure(attempt, groupErr)
						return gotBytes, up, pacingErr
					}
					if up, pubErr := publishGroups(groups, time.Now()); pubErr != nil {
						return gotBytes, up, pubErr
					}
				}
			}
			if ctx.Err() != nil && attemptReady {
				return gotBytes, false, ctx.Err()
			}
			// The upstream watchdog closes the attempt context to unblock both
			// sides of the ffmpeg pipeline. Preserve that independently observed
			// cause instead of misclassifying it as an output-startup timeout.
			if idleKilled.Load() {
				return gotBytes, true, attempt.wrap("stream",
					fmt.Errorf("%w (%s)", errUpstreamIdle, attemptIdleTimeout()))
			}
			// A startup deadline that killed ffmpeg after real upstream bytes
			// reached its stdin is a local no-first-output failure. Keep the
			// Plex bound, but neither relocate nor penalize provider health.
			if attempt.ctx.Err() != nil && !attemptReady {
				if attempt.diag.outputBytes.Load() > 0 {
					// ffmpeg did produce transport bytes, but they never formed a
					// decodable PAT/PMT + configuration + random-access window.
					// Relocate as an unusable media prefix rather than falsely
					// reporting that the local process emitted no output.
					return gotBytes, true, attempt.wrap("media_prefix",
						errors.Join(errUncertainMediaPrefix, errMediaPrefixNotReady))
				}
				upstreamFault, timeoutErr := transcodeNoOutputTimeout(attempt, &upstreamFlow)
				return gotBytes, upstreamFault, timeoutErr
			}
			// stdout ended: attribute the failure. The stdin pump always
			// reports (buffered chan) once the body read unblocks — which
			// runner.Close (deferred) forces by killing ffmpeg, and the
			// body close on ctx cancel forces too. Wait briefly; on
			// timeout assume ffmpeg-side.
			var cause error
			select {
			case cause = <-upErr:
			case <-attempt.ctx.Done():
				if !attemptReady {
					upstreamFault, timeoutErr := transcodeNoOutputTimeout(attempt, &upstreamFlow)
					return gotBytes, upstreamFault, timeoutErr
				}
				cause = errSourceStartupTimeout
			case <-time.After(2 * time.Second):
			}
			if cause != nil {
				if !attemptReady && errors.Is(cause, io.ErrUnexpectedEOF) &&
					upstreamFlow.totalBytes.Load() > 0 && !upstreamFlow.confirmed(time.Now()) {
					// A fragment that ended before ffmpeg could establish output
					// is no stronger evidence than one that stalled. Relocate, but
					// keep source-health attribution neutral.
					return gotBytes, true, attempt.wrap("transcode_input", errUncertainTranscodeInput)
				}
				if attempt.replay && errors.Is(cause, io.ErrUnexpectedEOF) &&
					errors.Is(rerr, io.EOF) {
					// FFmpeg has flushed the complete classified finite input into the
					// subscriber queues. Preserve its terminal output chunk before Run
					// advances the reconnect epoch, exactly as the passthrough path does.
					s.waitForDeliveryBoundary(ctx, s.deliveryBoundaryTimeout())
				}
				return gotBytes, true, attempt.wrap("stream", cause)
			}
			if ctx.Err() != nil {
				return gotBytes, false, ctx.Err()
			}
			if errors.Is(rerr, io.EOF) {
				rerr = nil
			}
			return gotBytes, false, localTranscodeFailure(fmt.Errorf(
				"ffmpeg pipeline ended (read err: %v); stderr tail: %s",
				rerr, runner.StderrTail(400)))
		}
		// Same idle-exit semantics as plain Streamer.
		if s.SubscriberCount() == 0 && s.bytesOut.Load() > 0 {
			select {
			case <-ctx.Done():
				return gotBytes, false, ctx.Err()
			case <-time.After(2 * time.Second):
			}
			if s.SubscriberCount() == 0 {
				return gotBytes, false, nil
			}
		}
	}
}

func selectTranscodeContinuityFault(
	input, output *avContinuityError,
) (*avContinuityError, bool) {
	if input != nil && isAVStarvationFault(input.kind) {
		// Missing selected input clocks cannot be manufactured by FFmpeg. Move
		// to another independently admitted source even if buffered output has
		// not exposed the same starvation yet.
		return input, true
	}
	if output == nil {
		// Encoded-audio profiles deliberately repair bad input clock slope. Do
		// not relocate merely because the input guard sees drift/skew or a
		// transient transport fence while the relay output remains coherent.
		return nil, false
	}
	if input != nil && input.kind == avFaultTransport {
		// TEI, counter, scrambling, or structurally malformed selected PES is
		// independently proven provider-input damage. If FFmpeg output also has
		// any confirmed continuity failure, relocate even when the decoder turns
		// that damage into a different symptom (commonly missing audio). A healthy
		// output above remains evidence that FFmpeg repaired the transient fault.
		return input, true
	}
	if input != nil && input.kind == output.kind {
		// The same proven failure exists on both sides of FFmpeg, so it belongs
		// to the source media rather than the local output pipeline.
		return input, true
	}
	return output, false
}

func isAVStarvationFault(kind avContinuityFaultKind) bool {
	switch kind {
	case avFaultAudioStarvation, avFaultVideoStarvation, avFaultMediaStarvation:
		return true
	default:
		return false
	}
}

func inputFaultProvesUpstreamWithoutOutput(fault *avContinuityError) bool {
	if fault == nil {
		return false
	}
	// Missing selected input clocks and confirmed selected-transport
	// corruption cannot be manufactured by FFmpeg. Drift/skew remain eligible
	// for local audio-clock repair and require matching output evidence.
	return isAVStarvationFault(fault.kind) || fault.kind == avFaultTransport
}

func transcodeOutputIdleFailure(
	attempt *upstreamAttempt,
	timeout time.Duration,
) error {
	metrics.TranscodeOutputStalls.Inc()
	return attempt.wrap("transcode_output", localTranscodeFailure(fmt.Errorf(
		"%w (%s)", errTranscodeOutputIdle, timeout)))
}

func transcodeRelayBufferFailure(attempt *upstreamAttempt, cause error) (bool, error) {
	// This queue sits on ffmpeg stdout, after the provider input. Exhaustion is
	// a local relay/output-path failure: restart on the same source and keep
	// provider exclusion/health accounting untouched.
	return false, attempt.wrap("relay_buffer", cause)
}

func transcodePacingFailure(attempt *upstreamAttempt, cause error) (bool, error) {
	if errors.Is(cause, errTransportAttemptBoundary) ||
		errors.Is(cause, errTransportProgramMapStall) {
		// The output clock crossed an explicit or corrupt boundary. Restart the
		// local pipeline on this source so the next epoch passes through the
		// startup decoder gate; output-only evidence must not decay provider
		// health.
		return false, attempt.wrap("transport_pace", localTranscodeFailure(cause))
	}
	if errors.Is(cause, errTransportStaleChunk) {
		return false, attempt.wrap("transport_pace", localTranscodeFailure(cause))
	}
	if errors.Is(cause, errTransportRelayBacklog) {
		// The private lookahead proved this exact source prefix cannot be
		// delivered under the live contract. Try an alternate without decaying
		// provider health; reopening identical media cannot change the result.
		return true, attempt.wrap("transport_pace", cause)
	}
	if !errors.Is(cause, errTransportRecoveryBacklog) {
		return false, attempt.wrap("transport_pace", cause)
	}
	// The pacer observes ffmpeg stdout, so a recovery backlog alone cannot
	// identify which side stalled. Relocate and decay provider health only when
	// this exact attempt just measured a provider-body Read gap. Otherwise
	// restart the local pipeline on the same source and keep health neutral.
	if attempt.diag.readGaps.recent(time.Now(), transcodeRecoveryReadGapWindow) {
		return true, attempt.wrap("transport_pace", cause)
	}
	return false, attempt.wrap("transport_pace", localTranscodeFailure(cause))
}

func transcodeNoOutputTimeout(attempt *upstreamAttempt, flow *upstreamFlowEvidence) (bool, error) {
	if flow.confirmed(time.Now()) {
		return false, attempt.wrap("transcode_output",
			localTranscodeFailure(errTranscodeStartupTimeout))
	}
	// Some input arrived, but not recently and continuously enough to prove
	// the local pipeline was at fault. Relocate without penalizing health.
	if flow.totalBytes.Load() > 0 {
		return true, attempt.wrap("transcode_input", errUncertainTranscodeInput)
	}
	return true, attempt.wrap("stream", errSourceStartupTimeout)
}
