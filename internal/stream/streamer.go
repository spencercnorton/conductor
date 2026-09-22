// streamer.go — one upstream connection, fanned out to N client subscribers.
//
// This is the in-process refcounted upstream from spec §3.3:
//
//	"Two-level model: one upstream connection can fan out to many client
//	 connections. This is critical — it both protects credential slots and
//	 reduces upstream load when multiple Plex clients (or Plex + a phone)
//	 tune the same channel."
//
// One Streamer per active_stream. Subscribers attach via Subscribe and read
// from a buffered channel; the upstream pump reads from the upstream HTTP
// body once and writes the same chunk to every subscriber. A slow subscriber
// gets bounded blocking then disconnect (see subscribers.go) — mid-stream
// bytes are never dropped.
//
// Phase 5 resilience (audit S1/S3): the pump survives upstream failures.
// On error/EOF while subscribers remain, it asks the Pool (via
// OnUpstreamDown) for a fresh lease — same source first, then the next
// source by priority/health — and reconnects with capped backoff, keeping
// every client connection open across the gap. A no-bytes watchdog kills
// reads that stall without erroring, so a wedged upstream can't hold a
// goroutine + provider connection forever.
//
// Late joiners miss the bytes that flowed before they joined. Acceptable
// for live: Plex starts a fresh decoder anyway; a few seconds of pre-roll
// data wouldn't help.
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
)

// chunkSize is the per-read buffer. 64KB keeps allocation overhead low while
// staying small enough that channel-buffer fill stays sensitive to slow
// subscribers (a 64KB chunk × 32-buffer = 2MB queued before the stall path).
const chunkSize = 64 * 1024

// publicationChunkSize ends full publications on TS packet boundaries when
// the transformed media starts aligned. This guarantees packet framing only;
// PES completion and decoder random access remain separate concerns.
// Producer reads and reservoir byte budgets retain chunkSize. Bypassed data
// remains byte-exact with this smaller publication partition.
const publicationChunkSize = chunkSize - chunkSize%tsPacketSize

// subscriberBuffer bounds the startup-tagged handoff and pre-roll count. The
// ordinary HTTP handoff is deliberately unbuffered so an attempt boundary can
// retract all undispatched bytes; steady-state jitter lives only in the private
// subscriberQueueBuffer below.
const subscriberBuffer = 32

// subscriberQueueBuffer is the pump-facing per-subscriber jitter queue. It
// is the sole steady-state jitter buffer: the delivery worker's
// five-second timer, not an ordinary provider burst, should decide that a
// sink is stalled. At 64 KiB chunks the worst retained payload for one stuck
// subscriber is 64 MiB, while a typical 4–16 Mbit/s live stream retains only
// 2.5–10 MiB before the timer disconnects it. Chunk payloads are shared
// immutable slices across subscribers; the queue stores references only.
const subscriberQueueBuffer = 1024

const (
	// defaultIdleReadTimeout bounds how long the pump tolerates an open
	// upstream that sends no media. Plex/tvOS abandons a Live TV input after
	// roughly ten seconds without useful progress. 4.25 seconds remains safely
	// beyond the independent four-second startup timer, while leaving a
	// bounded relocation, retry backoff, and one complete alternate-source
	// attempt inside ClientStartupBudget instead of discovering the stall only
	// after the client has already returned to the guide.
	defaultIdleReadTimeout = 4250 * time.Millisecond

	reconnectInitialBackoff = 250 * time.Millisecond
	reconnectMaxBackoff     = 5 * time.Second

	// liveStallTolerance replaces the startup-oriented idle timeout once media
	// is flowing. Bursty origins pause 3-14s between bursts as their normal
	// delivery rhythm (measured p99=11s, max=14s); the live reservoir keeps the
	// output flowing through those pauses, so killing the read at 4.25s only
	// converts a bridgeable pause into a teardown plus 1.3-2s of startup-gate
	// revalidation. Startup keeps the fast timeout: a source that never
	// delivers a first byte should fail over quickly.
	defaultLiveStallTolerance = 20 * time.Second

	// defaultReconnectWindow caps continuous upstream downtime. Past it the
	// pump gives up and closes subscribers so clients surface a real error
	// and re-tune, instead of hanging forever on a silent 200.
	defaultReconnectWindow = 90 * time.Second

	// defaultStableStreamDuration is how long an attempt must deliver bytes
	// before it counts as a recovered stream. Some providers accept a
	// reconnect with HTTP 200, emit a small fragment, and immediately EOF.
	// Treating any byte as recovery resets the reconnect window and the
	// Pool's failed-source set, producing an endless A→B→A loop. Five seconds
	// is far below a useful viewing interval while filtering those fragments.
	defaultStableStreamDuration = 5 * time.Second

	// startupGrace is how long a pump with zero subscribers and zero bytes
	// keeps retrying after an initiating client departs during startup.
	startupGrace = 15 * time.Second

	// startupLeadDeadlineReserve is the part of the initiating client's
	// absolute startup budget the lead hold must leave untouched: enough for
	// the prefix release and for Plex's segmenter to publish its first segment.
	startupLeadDeadlineReserve = 2 * time.Second
)

// holdStartupLead delays the initial attempt's first fanout by up to lead so
// the live reservoir accumulates that much media before the client starts
// consuming at media rate. The pacer keeps whatever is queued as lead (see the
// underflow re-anchor in transportPCRPacer.Wait), so this is the only margin
// a source that never delivers ahead of realtime will ever have: measured
// 2026-09-02 on ABC ch2, the origin stalled 1-2.5s at a time and never caught
// up, and Plex/tvOS holds ~3s of its own. The hold never spends the
// initiating client's absolute startup budget below the reserve, and only the
// initial attempt holds; a reconnect must publish immediately because the
// previous attempt's lead is already draining into the client. A hold that
// outlives the attempt context returns that context's error.
func holdStartupLead(ctx context.Context, lead time.Duration, startupDeadline time.Time) error {
	if lead <= 0 {
		return nil
	}
	if !startupDeadline.IsZero() {
		if remaining := time.Until(startupDeadline) - startupLeadDeadlineReserve; remaining < lead {
			lead = remaining
		}
	}
	if lead <= 0 {
		return nil
	}
	timer := time.NewTimer(lead)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// errUpstreamIdle marks a read that the no-bytes watchdog killed.
var errUpstreamIdle = errors.New("upstream idle: no bytes within timeout")

// Streamer pumps an upstream HTTP body into N subscriber channels.
//
// Lifetime: created when a new upstream is leased. Run blocks for the
// lifetime of the upstream. When the last subscriber disconnects the
// pump exits (via the OnIdle callback, which the Pool wires to release
// the lease).
type Streamer struct {
	subscriberSet

	id       string
	upstream string // current resolved URL; replaced on failover (pump goroutine only)
	logger   *slog.Logger
	hc       *http.Client

	idleReadTimeout        time.Duration
	stallTolerance         time.Duration
	reconnectWindow        time.Duration
	stableDuration         time.Duration
	sourceStartupTimeout   time.Duration
	initialStartupDeadline time.Time
	// startupLead is how long the initial attempt holds its validated prefix
	// so the reservoir fills with lead first; see holdStartupLead.
	startupLead time.Duration
	// startupHoldDeadline caps only optional buffering against the initiating
	// subscriber's deadline. It never limits shared source attempts and is
	// immutable after pump construction, including when later viewers attach.
	startupHoldDeadline time.Time
	classifier          finiteMediaClassifier
	prefixValidator     mediaPrefixValidator
	avContinuityPolicy  avContinuityPolicy
	// attemptFlowDuration measures first-output-byte to last-output-byte for
	// the current runOnce call. Wall time is deliberately excluded: connect,
	// ffmpeg teardown, or an EOF wait must not make a tiny fragment "stable".
	// Pump goroutine only.
	attemptFlowDuration time.Duration

	bytesOut   atomic.Int64
	mediaReady atomic.Bool

	ctMu        sync.Mutex
	contentType string

	// Optional callbacks the Pool wires up. All fire from the pump goroutine.
	OnRunning  func()            // upstream connected (transition starting → running)
	OnStable   func()            // attempt delivered bytes for stableDuration
	OnBytes    func(n int) error // every chunk written; return non-nil to abort pump
	OnStopping func()            // detach before terminal/closed state is published
	OnExit     func(err error)   // pump exited; err nil = clean stop (idle / cancel)

	// OnUpstreamDown fires when the upstream dies while subscribers remain.
	// The Pool re-leases (same source first, then next by priority/health,
	// excluding sources already failed this gap) and returns the new
	// resolved URL. Return retry=false to stop reconnecting. newURL == ""
	// with retry=true retries the current URL after backoff.
	// (SlateFn + the slate feed live on the embedded subscriberSet, shared
	// with TranscodeStreamer.)
	OnUpstreamDown func(ctx context.Context, cause error) (newURL string, retry bool, terminalErr error)
}

// NewStreamer prepares a Streamer; call Run to start the pump.
func NewStreamer(id, upstream string, logger *slog.Logger, hc *http.Client) *Streamer {
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
	s := &Streamer{
		id:                   id,
		upstream:             upstream,
		logger:               logger,
		hc:                   hc,
		idleReadTimeout:      defaultIdleReadTimeout,
		stallTolerance:       defaultLiveStallTolerance,
		reconnectWindow:      defaultReconnectWindow,
		stableDuration:       defaultStableStreamDuration,
		sourceStartupTimeout: defaultSourceStartupTimeout,
		classifier:           newFFmpegFiniteMediaClassifier(""),
		avContinuityPolicy:   defaultAVContinuityPolicy(),
	}
	s.subscriberSet.init(id, logger)
	return s
}

// BytesOut returns total bytes pumped from upstream (across reconnects).
func (s *Streamer) BytesOut() int64 { return s.bytesOut.Load() }

// ContentType returns the upstream's content type. Empty until headers
// arrive. Used by handlers to mirror it back to clients.
func (s *Streamer) ContentType() string {
	s.ctMu.Lock()
	defer s.ctMu.Unlock()
	return s.contentType
}

// MediaReady reports whether validated media bytes have reached fanout.
// HTTP 200/headers alone are deliberately insufficient: providers can return
// a stalled body or a finite dead-media placeholder behind successful headers.
// It stays true across mid-stream reconnects because existing subscribers
// retain their response while the pump relocates.
func (s *Streamer) MediaReady() bool { return s.mediaReady.Load() }

// HeadersReady remains as a compatibility alias for out-of-package callers.
// Its semantics are intentionally strengthened to validated media readiness.
func (s *Streamer) HeadersReady() bool { return s.MediaReady() }

// errPumpAborted wraps an OnBytes callback error — a deliberate abort from
// the Pool, never retried.
type errPumpAborted struct{ err error }

func (e *errPumpAborted) Error() string { return "pump aborted: " + e.err.Error() }
func (e *errPumpAborted) Unwrap() error { return e.err }

// Run pumps upstream → subscribers until the subscribers all leave, the
// context is cancelled, or the upstream stays down past reconnectWindow.
// Safe to call exactly once per Streamer.
func (s *Streamer) Run(ctx context.Context) {
	var runErr error
	defer func() {
		s.markStopping()
		if s.OnStopping != nil {
			s.OnStopping()
		}
		s.setTerminalError(runErr)
		s.closeAll()
		if st, dr := s.Stalls(), s.SubDrops(); st > 0 || dr > 0 {
			s.logger.Info("streamer fanout stats",
				"stream", s.id, "stalls", st, "sub_drops", dr)
		}
		if s.OnExit != nil {
			s.OnExit(runErr)
		}
	}()

	started := time.Now()
	backoff := reconnectInitialBackoff
	var downSince time.Time // zero = upstream healthy

	for {
		gotBytes, err := s.runOnce(ctx)
		if gotBytes && s.attemptFlowDuration >= s.stableDuration {
			// A sustained attempt ended: any prior outage is over. Do not reset
			// on a 200 + tiny fragment + EOF response; that is still the same
			// continuous outage and must remain bounded by reconnectWindow.
			downSince = time.Time{}
			backoff = reconnectInitialBackoff
			if s.OnStable != nil {
				s.OnStable()
			}
		}

		// Cancellation (sweeper reap, pool shutdown) is a clean stop, not a
		// source failure — don't ding source health for it.
		if ctx.Err() != nil {
			s.markStopping()
			return
		}
		var aborted *errPumpAborted
		if errors.As(err, &aborted) {
			runErr = aborted.err
			s.markStopping()
			return
		}
		// Every completed upstream attempt is a discontinuity, even while an
		// existing reconnect gap remains active. A 200 + short fragment + EOF
		// must not leave that fragment in the ring for the next attempt or keep
		// a queued startup handoff on the same media epoch.
		s.clearRing()
		// Nobody is watching: stop — unless this stream never produced a
		// byte and is still inside startupGrace, where the first client is
		// likely polling HeadersReady and a retry (or failover) can still
		// rescue the tune.
		if s.tryMarkStoppingIfIdle(
			s.bytesOut.Load() > 0 || time.Since(started) > startupGrace) {
			runErr = err
			return
		}

		// Someone is (or may soon be) watching and the upstream died. A
		// live stream EOFing mid-watch is a failure for reconnect purposes
		// even though the read was "clean".
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		localRelayRetry := isLocalRelayRetry(err)
		if downSince.IsZero() {
			downSince = time.Now()
		}
		if time.Since(downSince) > s.reconnectWindow {
			if localRelayRetry {
				runErr = fmt.Errorf("local relay unavailable for %s, giving up: %w",
					time.Since(downSince).Round(time.Second), err)
			} else {
				runErr = fmt.Errorf("upstream down for %s, giving up: %w",
					time.Since(downSince).Round(time.Second), err)
			}
			s.markStopping()
			return
		}
		s.startRecoveryFillerForFailure(ctx, err, downSince)
		if localRelayRetry {
			s.logger.Warn("local relay backlog exceeded its bound; reopening same source",
				"stream", s.id, "err", err)
		} else {
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
		}
		if gotBytes {
			// Pumping continues past an attempt that already fanned out bytes:
			// the next attempt's media joins those bytes as a splice.
			s.noteUpstreamInterruption()
		}

		gapElapsed := time.Since(downSince)
		if requiresImmediateContinuityFiller(err) && gapElapsed < s.slateAfter {
			// A hard no-media/A/V-progress failure has already consumed the
			// viewer's silence budget. Feed a pre-rendered decodable fragment during
			// this retry backoff instead of waiting three more seconds before slate.
			gapElapsed = s.slateAfter
		}
		if !s.waitBeforeRetry(ctx, backoff, gapElapsed) {
			s.markStopping()
			return // cancelled
		}
		backoff = min(backoff*2, reconnectMaxBackoff)
	}
}

func requiresImmediateContinuityFiller(err error) bool {
	return errors.Is(err, errUpstreamIdle) ||
		errors.Is(err, errOutputClockBoundary) ||
		errors.Is(err, errAVContinuity) ||
		errors.Is(err, errTransportAttemptBoundary) ||
		errors.Is(err, errTransportProgramMapStall) ||
		errors.Is(err, errTranscodeOutputIdle)
}

func isLocalRelayRetry(err error) bool {
	return errors.Is(err, errRelayReadAheadFull) ||
		errors.Is(err, errTransportStaleChunk) ||
		errors.Is(err, errTransportAttemptBoundary) ||
		errors.Is(err, errTranscodeOutputIdle)
}

// runOnce performs one connect-and-pump attempt. Returns gotBytes=true if
// at least one chunk was fanned out. A nil error means the upstream EOFed
// or the idle-exit (no subscribers) fired.
func (s *Streamer) runOnce(ctx context.Context) (gotBytes bool, err error) {
	s.attemptFlowDuration = 0
	var firstByteAt time.Time
	attemptTimeout := sourceAttemptBudget(
		s.sourceStartupTimeout, s.initialStartupDeadline, s.MediaReady())
	attempt := newUpstreamAttempt(ctx, s.upstream, s.id, s.logger, attemptTimeout)
	attempt.allowStartupProgressExtension(
		startupExtensionWall(s.initialStartupDeadline, s.MediaReady()))
	defer func() { attempt.finish(err) }()
	if err = attempt.open(s.hc, s.upstream, "Conductor/stream", s.classifier); err != nil {
		return false, err
	}

	s.ctMu.Lock()
	s.contentType = attempt.contentType
	s.ctMu.Unlock()

	// No-bytes watchdog (audit S3): Body.Read on a stalled-but-open upstream
	// blocks forever (the client deliberately has no overall timeout — live
	// streams are unbounded). Force-close the body when no bytes arrive
	// within idleReadTimeout; the Read unblocks with an error and the
	// reconnect loop takes over.
	// 0 = producer active, 1 = producer reached a terminal read, 2 = idle
	// watchdog won. Once EOF/error is queued, buffered media is allowed to
	// drain at PCR pace without a stale arrival timer discarding its tail.
	var producerState atomic.Int32
	// Readiness is attempt-local: a reconnect attempt must fail a source that
	// never delivers startup bytes at the fast startup timeout, regardless of
	// what an earlier attempt proved (review finding). The
	// streamer-lifetime mediaReady keeps serving its other consumers.
	var attemptMediaLive atomic.Bool
	attemptIdleTimeout := func() time.Duration {
		if attemptMediaLive.Load() {
			return s.stallTolerance
		}
		return s.idleReadTimeout
	}
	idle := time.AfterFunc(s.idleReadTimeout, func() {
		if producerState.CompareAndSwap(0, 2) {
			attempt.Close()
		}
	})
	defer idle.Stop()
	lastReset := time.Now()
	attemptReady := false
	validatedStartPending := false
	gate := newStartupMediaGate(s.prefixValidator)
	defer attempt.recordMediaPrefix(gate)
	pacer := &transportPCRPacer{}
	var publicationGate publicationAVGate
	var pesGate pesPublicationGate
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
	reads, readOverflow, readDone := readAhead(attempt.ctx, relayReaderCloser{
		Reader: attempt,
		close:  attempt.closeBody,
	},
		func(data []byte, started, ended time.Time) {
			attempt.recordOutputData(data, ended.Sub(started))
			// Throttle timer resets to ~1/s so the hot reader does not hammer
			// the runtime timer on every transport chunk.
			if producerState.Load() == 0 && ended.Sub(lastReset) > time.Second {
				d := attemptIdleTimeout()
				idle.Reset(d)
				// The consumer may flip media-ready between the evaluation and
				// the Reset; re-check so a startup-length deadline armed in that
				// window cannot survive into a normal live pause.
				if nd := attemptIdleTimeout(); nd != d {
					idle.Reset(nd)
				}
				lastReset = ended
			}
		}, func() {
			producerState.CompareAndSwap(0, 1)
			idle.Stop()
		})
	defer func() {
		attempt.Close()
		<-readDone
	}()

	publishGroups := func(groups [][]byte, paceArrival time.Time) error {
		for _, chunk := range groups {
			partLen := len(chunk)
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
					return attempt.wrap("transport_pace", paceErr)
				}
				remaining = remaining[n:]
				publicationProgress.Store(time.Now().UnixNano())
			}
			if paceErr := pacer.rejectStaleBeforeFanout(paceArrival); paceErr != nil {
				return attempt.wrap("transport_pace", paceErr)
			}
			publishedAt := time.Now()
			if firstByteAt.IsZero() {
				firstByteAt = publishedAt
			}
			s.attemptFlowDuration = publishedAt.Sub(firstByteAt)
			if validatedStartPending {
				s.fanoutValidatedStart(chunk)
				validatedStartPending = false
			} else {
				s.fanout(chunk)
			}
			publicationProgress.Store(time.Now().UnixNano())
			gotBytes = true
			s.bytesOut.Add(int64(partLen))
			s.mediaReady.Store(true)
			if !attemptMediaLive.Swap(true) && producerState.Load() == 0 {
				// The deadline armed during startup reads is still
				// ticking. Re-arm it for live: the origin may now
				// legitimately pause for its normal burst interval
				// before the producer ever resets the timer again.
				idle.Reset(s.stallTolerance)
			}
			if s.OnBytes != nil {
				if cbErr := s.OnBytes(partLen); cbErr != nil {
					return &errPumpAborted{err: cbErr}
				}
			}
		}
		return nil
	}
	for {
		var result relayReadResult
		select {
		case overflowErr := <-readOverflow:
			return gotBytes, attempt.wrap("relay_buffer", overflowErr)
		case next, ok := <-reads:
			if !ok {
				if overflowErr := takeRelayReadAheadError(readOverflow); overflowErr != nil {
					return gotBytes, attempt.wrap("relay_buffer", overflowErr)
				}
				if producerState.Load() == 2 {
					return gotBytes, attempt.wrap("stream",
						fmt.Errorf("%w (%s)", errUpstreamIdle, attemptIdleTimeout()))
				}
				if attempt.ctx.Err() != nil {
					return gotBytes, attempt.wrap("stream", attempt.ctx.Err())
				}
				return gotBytes, nil
			}
			result = next
		case <-attempt.ctx.Done():
			// The producer owns definitive queue-exhaustion evidence. Join it
			// before interpreting cancellation so concurrent startup timeout and
			// local backpressure cannot randomly change source-health attribution.
			attempt.Close()
			<-readDone
			if overflowErr := takeRelayReadAheadError(readOverflow); overflowErr != nil {
				return gotBytes, attempt.wrap("relay_buffer", overflowErr)
			}
			if producerState.Load() == 2 {
				return gotBytes, attempt.wrap("stream",
					fmt.Errorf("%w (%s)", errUpstreamIdle, s.idleReadTimeout))
			}
			return gotBytes, attempt.wrap("stream", attempt.ctx.Err())
		}
		n, rerr := len(result.data), result.err
		if rerr != nil && n == 0 && !attemptReady && gate.BufferedBytes() > 0 {
			media, ready, gateErr := gate.Finalize(attempt.ctx)
			if gateErr != nil {
				return gotBytes, attempt.wrap("media_prefix",
					classifyMediaPrefixValidationError(gateErr))
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
				// Dequeue is the live edge the rest of the pipeline reasons
				// about. The reservoir deliberately holds a delivery burst for
				// up to its depth; charging that hold against the pacer's
				// added-latency contract or the A/V clock ages would make the
				// reservoir read as stale media and a paused source at once.
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
				return gotBytes, attempt.wrap("media_prefix",
					classifyMediaPrefixValidationError(gateErr))
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
					pacer.ConfigureSelectedProgram(program, haveProgram)
					pesGate.configure(program, haveProgram)
					if haveProgram {
						publicationGate.bindSelectedProgram(program, paceArrival, false)
					}
					if paceErr := pacer.PrepareBufferedPrefix(media, publicationChunkSize); paceErr != nil {
						return false, attempt.wrap("transport_pace", paceErr)
					}
					if readyErr := attempt.markMediaReady(); readyErr != nil {
						return false, readyErr
					}
					attemptReady = true
					validatedStartPending = true
					if initialPrefix && s.OnRunning != nil {
						// Complete the bounded durable-running callback while the
						// prefix is private, then choose its pacing release edge.
						s.OnRunning()
					}
					if initialPrefix && s.startupLead > 0 && !s.mediaReady.Load() {
						// The hold is this relay's own delay on media the gate has
						// already validated, so upstream silence during it is judged
						// by the live tolerance, not the startup watchdog: origins
						// pause for their burst interval (5-6 s on the transcode
						// hosts) right after the lead they deliver at connect. A
						// reconnect never holds: the pump has published, and the
						// client's startup budget is not renewed by a respawn.
						if !attemptMediaLive.Swap(true) && producerState.Load() == 0 {
							idle.Reset(s.stallTolerance)
						}
						if holdErr := holdStartupLead(attempt.ctx, s.startupLead,
							s.startupHoldDeadline); holdErr != nil {
							return false, attempt.wrap("stream", holdErr)
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
					return gotBytes, attempt.wrap("transport_boundary", gateErr)
				}
				media, paceArrival, fault := publicationGate.push(media, paceArrival)
				if fault != nil {
					recordAVContinuityFailure(true, fault)
					return gotBytes, attempt.wrap("input_transport", fault)
				}
				if fault := publicationGate.continuityFault(
					time.Now(), s.avContinuityPolicy); fault != nil {
					recordAVContinuityFailure(true, fault)
					return gotBytes, attempt.wrap("input_av_continuity", fault)
				}
				groups, groupErr := pesGate.push(media)
				if groupErr != nil {
					return gotBytes, attempt.wrap("transport_boundary", groupErr)
				}
				if pubErr := publishGroups(groups, paceArrival); pubErr != nil {
					return gotBytes, pubErr
				}
			}
		}
		if rerr != nil {
			if producerState.Load() == 2 {
				return gotBytes, attempt.wrap("stream", fmt.Errorf("%w (%s)", errUpstreamIdle, s.idleReadTimeout))
			}
			if errors.Is(rerr, io.EOF) {
				// A clean HTTP entity end closes its final unbounded PES. An
				// interrupted/mismatched body returns a different error and cannot
				// flush an unknown tail into a recovery splice.
				if attemptReady && ctx.Err() == nil && attempt.ctx.Err() == nil {
					groups, groupErr := pesGate.finishCleanEOF()
					if groupErr != nil {
						return gotBytes, attempt.wrap("transport_boundary", groupErr)
					}
					if pubErr := publishGroups(groups, time.Now()); pubErr != nil {
						return gotBytes, pubErr
					}
				}
				if attempt.replay {
					// A finite response has already been classified and fully buffered.
					// Let every still-healthy worker hand off its terminal chunk before
					// clearRing resets the epoch; the same budget that governs ordinary
					// delivery keeps this boundary bounded.
					s.waitForDeliveryBoundary(ctx, s.deliveryBoundaryTimeout())
				}
				return gotBytes, nil
			}
			return gotBytes, attempt.wrap("stream", rerr)
		}
		// Idle-exit: if we've had subscribers and now have none, exit.
		// Keep pumping while count==0 only briefly so a fast channel-change
		// can re-attach without renegotiating upstream.
		if s.SubscriberCount() == 0 && s.bytesOut.Load() > 0 {
			select {
			case <-ctx.Done():
				return gotBytes, ctx.Err()
			case <-time.After(2 * time.Second):
			}
			if s.SubscriberCount() == 0 {
				return gotBytes, nil
			}
		}
	}
}

type httpStatusError struct {
	Code int
}

func (e *httpStatusError) Error() string {
	return "upstream returned HTTP " + http.StatusText(e.Code)
}
