// Package stream owns the upstream→client copy path: slot allocation,
// upstream connection lifecycle, refcounting, watchdogs, and (later) ring
// buffering and PCR-aware rewriting.
//
// Phase 1 scope:
//   - real credential leasing via store.AcquireLease (FOR UPDATE SKIP LOCKED
//     per spec §4.1)
//   - one upstream HTTP connection per (channel_source) shared by N clients
//     via Streamer
//   - DB-backed active_stream / refcount tracking
//   - orphan upstream sweeper (separate file: watchdog.go)
//
// Phase 5 layers ring buffering, PCR rewriting, transparent reconnect, and
// the "Source Unavailable" placeholder onto the same surface.
package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/transcode"
)

// streamPump is the small interface that BOTH Streamer (passthrough) and
// TranscodeStreamer satisfy. Pool holds a heterogeneous map without caring
// which kind it is.
type streamPump interface {
	Run(ctx context.Context)
	SubscribeForStartup(clientID string) (<-chan startupChunk, <-chan startupChunk, <-chan struct{}, func())
	SubscriberCount() int
	LastRealChunk() time.Time
	UpstreamInterruptions() uint64
	MediaReady() bool
	MediaEpoch() uint64
	Stopping() bool
	ContentType() string
	TerminalError() error
}

// hardStaleActivityTerminator is implemented by the production pump types
// through their embedded subscriberSet. It atomically checks local real-media
// activity, closes publication/admission, and independently terminates every
// Plex-facing subscriber for the exact pump generation before the watchdog's
// durable CAS. Physical source/ffmpeg ownership remains tracked until OnExit.
type hardStaleActivityTerminator interface {
	tryTerminateHardStale(cutoff, installedAt time.Time) bool
}

// ErrUpstreamNotReady signals that no source delivered validated media within
// the startup deadline. Serve returns it BEFORE writing anything, so the HTTP
// handler can 503 and Plex takes its retry path instead of treating headers,
// an empty body, or dead placeholder media as a successful tune.
var ErrUpstreamNotReady = errors.New("upstream did not start in time")

// errPumpStoppedBeforeMedia identifies a subscriber that won the process-map
// race just before OnStopping detached a terminal pump. No response bytes were
// written, so the same durable token may attach/install a replacement within
// its original absolute startup deadline.
var errPumpStoppedBeforeMedia = errors.New("attached pump stopped before first media")

// errPumpDiscontinuityBeforeMedia means startup media was validated against an
// attempt that entered a reconnect gap before the caller accepted it. The
// durable lease remains valid; attach again to the same pump and wait for the
// new epoch rather than writing pre-gap bytes.
var errPumpDiscontinuityBeforeMedia = errors.New("stream changed source before first media")

// ErrPoolClosed prevents a lease acquired concurrently with runtime shutdown
// from installing a new process-local pump after ownership has been lost.
var ErrPoolClosed = errors.New("stream pool closed")

// shouldMarkSourceFailure centralizes the attribution boundary used by both
// final pump teardown and mid-gap relocation. Local ffmpeg faults and input
// evidence too weak to blame the provider must never decay source health,
// even if a future TranscodeStreamer path defensively reaches relocation.
func shouldMarkSourceFailure(err error) bool {
	return err != nil &&
		!errors.Is(err, errLocalTranscodeFailure) &&
		!errors.Is(err, errUncertainTranscodeInput) &&
		!errors.Is(err, errClassifierCapacity) &&
		!errors.Is(err, errClassifierTimeout) &&
		!errors.Is(err, errUncertainMediaPrefix) &&
		!errors.Is(err, errRelayReadAheadFull) &&
		!errors.Is(err, errTransportStaleChunk) &&
		!errors.Is(err, errTransportRelayBacklog) &&
		!errors.Is(err, errTransportAttemptBoundary) &&
		!errors.Is(err, store.ErrLeaseOperational) &&
		!errors.Is(err, store.ErrRelocationStateUnknown)
}

// shouldMarkSourceSuccess requires positive media evidence from the source
// that is still current when the pump exits. Pump cancellation is
// intentionally reported as a clean exit by Streamer, including when Plex
// abandons startup before any validated bytes arrive; that neutral abort must
// not improve source health. A prior source's stable media likewise must not
// credit a replacement selected during the final reconnect gap.
func shouldMarkSourceSuccess(
	err error,
	hasDeliveredMedia bool,
	stableSourceID, currentSourceID uuid.UUID,
) bool {
	return err == nil && hasDeliveredMedia &&
		stableSourceID != uuid.Nil && stableSourceID == currentSourceID
}

// markSourceFailureAdvisory persists health off the load-bearing relocation
// path. A locked channel_source row must not spend the 500ms pre-media budget
// before source two is even considered; the explicit failed-source exclusion
// already makes relocation correct without waiting for this score update.
func (p *Pool) markSourceFailureAdvisory(sourceID uuid.UUID) {
	p.cleanupWG.Add(1)
	go func() {
		defer p.cleanupWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), sourceHealthUpdateTimeout)
		defer cancel()
		if err := p.db.MarkSourceFailure(ctx, sourceID); err != nil &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			p.logger.Debug("source health failure update did not persist",
				"source", sourceID, "err", err)
		}
	}()
}

const (
	// Bounds for retryLeaseCleanup. The same replay-safe loop backs ordinary
	// client teardown, aborted startups, and ambiguous startup reconciliation.
	leaseCleanupAttemptTimeout = time.Second
	leaseCleanupRetryWindow    = 30 * time.Second
	leaseCleanupInitialBackoff = 100 * time.Millisecond
	leaseCleanupMaxBackoff     = 2 * time.Second
	startupRelocateTimeout     = 500 * time.Millisecond
	// A midstream Plex viewer has the same finite silence tolerance as a new
	// tune. Bound the durable source move so silence detection + relocation +
	// retry backoff + one alternate source attempt stays below
	// ClientStartupBudget. PostgreSQL contention fails closed into a retry of
	// the current durable lease; it may not consume an open-ended ten seconds.
	midstreamRelocateTimeout   = 250 * time.Millisecond
	sourceHealthUpdateTimeout  = 4 * time.Second
	hardStaleFinalizeTimeout   = 5 * time.Second
	closedPumpAttachRetryLimit = 2
)

// ErrStreamEnded is returned when a pump closes a subscriber without a
// terminal upstream error and without the subscriber context being done.
// Live streams do not have a natural EOF; treating that case as success made
// the DVR finalize truncated .partial files as completed recordings.
var ErrStreamEnded = errors.New("live stream ended unexpectedly")

// ErrSinkWrite wraps a failure writing to the caller's destination (a full
// disk, an unwritable mount) as opposed to an upstream failure. DVR must be
// able to tell them apart: an upstream that dies after the program was fully
// captured is salvageable, but a local write failure means the bytes on disk
// are not what we think they are and the recording must not be finalized.
var ErrSinkWrite = errors.New("recording sink write failed")

// pumpEntry pairs a running pump with the cancel func for its context, so
// the sweeper (and Close) can tear down wedged pumps instead of leaking the
// goroutine + provider-side connection (audit S3).
type pumpEntry struct {
	pump         streamPump
	cancel       context.CancelFunc
	physicalExit <-chan struct{}
	generation   uuid.UUID
	installedAt  time.Time
}

// WriterReservation is a real active_stream client lease held for one DVR
// recording. Reserving happens before the recorder claims its DB row or
// touches the output file, so temporary capacity exhaustion stays retryable.
// Release is idempotent and must be called when the recording claim loses.
type WriterReservation struct {
	pool        *Pool
	lease       store.Lease
	liveReserve int

	mu       sync.Mutex
	serving  bool
	released bool
}

type streamClientRoles struct {
	live        int
	dvrReserves map[int]int
}

// contextGate is a one-token mutex whose wait respects a request/startup
// deadline. A same-channel acquisition must not make Plex's bounded source
// relocation wait on an uninterruptible sync.Mutex.
type contextGate struct{ token chan struct{} }

func newContextGate() *contextGate {
	g := &contextGate{token: make(chan struct{}, 1)}
	g.token <- struct{}{}
	return g
}

func (g *contextGate) Lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
		// A canceled context and ready token may both win a select. Return the
		// token before entering DB work when cancellation was already visible.
		if err := ctx.Err(); err != nil {
			g.Unlock()
			return err
		}
		return nil
	}
}

func (g *contextGate) Unlock() { g.token <- struct{}{} }

// Pool is the per-process registry of in-flight Streamers.
type Pool struct {
	logger   *slog.Logger
	db       *store.DB
	resolver store.CredentialResolver

	// Alerts is optional. When set, slot exhaustion + source failover
	// events emit to the ops bot via HMAC webhook.
	Alerts *alerts.Client

	// LiveStartupLead is how long a live pump's initial attempt holds its
	// validated prefix so the reservoir fills with lead before the first
	// fanout (CONDUCTOR_LIVE_STARTUP_LEAD). See holdStartupLead.
	LiveStartupLead time.Duration

	// DiagDir, when non-empty, receives bounded refusal-head dumps from
	// stream startup gates (CONDUCTOR_DIAG_DIR).
	DiagDir string

	// FFmpegBinary path. Empty = "ffmpeg" on PATH. Set when running in a
	// container that has ffmpeg at a non-standard location.
	FFmpegBinary string

	hc *http.Client

	// placeholders is shared by every pump so one black-placeholder verdict
	// covers retries, re-tunes and other viewers of the same upstream URL.
	placeholders *placeholderCooldown

	// Required process-wide "Source Unavailable" slate. Production renders it
	// before HTTP admission and treats failure as fatal, because mid-gap lazy
	// rendering or silent disablement breaks the Plex continuity contract.
	slateOnce sync.Once
	slateClip *slate
	slateErr  error

	mu        sync.Mutex
	streamers map[uuid.UUID]pumpEntry // keyed by active_stream.id
	// exitingPumps keeps terminal generations visible to capacity teardown
	// after they are unpublished from attachment but before OnExit completes.
	// A replacement may coexist under streamers for the same durable row only
	// after every prior generation has signalled physical upstream/ffmpeg exit.
	exitingPumps map[uuid.UUID][]pumpEntry
	closed       bool
	pumpWG       sync.WaitGroup
	cleanupWG    sync.WaitGroup

	// pumpExitAfterMarkHook is a deterministic integration-test seam for the
	// MarkDead(false) → final-client-release → local-exit-removal race.
	pumpExitAfterMarkHook func()
	// hardStaleFinalizeHook is a focused unit-test seam for the durable CAS.
	// Production leaves it nil and calls DB.FinalizeHardStaleStream.
	hardStaleFinalizeHook func(
		context.Context,
		store.HardStaleStreamCandidate,
		time.Time,
		bool,
	) (bool, error)

	// Role registration is process-local like the pump registry. A per-channel
	// mutex closes each acquire→register seam against that channel's relocation
	// without letting a blocked provider stall unrelated pumps. rolesMu only
	// protects the short map operations; it is never held across DB I/O.
	channelRoleLocks sync.Map // channel UUID -> *contextGate
	rolesMu          sync.Mutex
	clientRoles      map[uuid.UUID]*streamClientRoles

	// Edge-triggered hint for the DVR scheduler. The buffered channel may
	// coalesce releases; every wake still revalidates against the DB lease
	// transaction, so correctness never depends on receiving every edge.
	capacityChanged chan struct{}
}

// PrepareSlate renders the process-wide outage slate before the tuner starts
// serving. Midstream recovery must not spend the viewer's remaining silence
// budget launching a lazy FFmpeg render after a 4.25-second media-progress
// trip. The one-time result (including failure) is immutable for this Pool.
func (p *Pool) PrepareSlate(ctx context.Context) error {
	p.slateOnce.Do(func() {
		sl, err := generateSlate(ctx, p.FFmpegBinary, p.logger)
		if err != nil {
			p.slateErr = err
			return
		}
		p.slateClip = sl
	})
	return p.slateErr
}

// slateFn is the pump callback. Production pre-renders via PrepareSlate before
// HTTP admission. The bounded fallback preserves direct Pool construction in
// tests and alternate embeddings without allowing a 45-second mid-gap block.
func (p *Pool) slateFn() *slate {
	ctx, cancel := context.WithTimeout(context.Background(), ClientStartupBudget)
	defer cancel()
	_ = p.PrepareSlate(ctx)
	return p.slateClip
}

func NewPool(logger *slog.Logger, db *store.DB, resolver store.CredentialResolver) *Pool {
	return &Pool{
		logger:          logger,
		db:              db,
		resolver:        resolver,
		streamers:       make(map[uuid.UUID]pumpEntry),
		exitingPumps:    make(map[uuid.UUID][]pumpEntry),
		clientRoles:     make(map[uuid.UUID]*streamClientRoles),
		capacityChanged: make(chan struct{}, 1),
		placeholders:    newPlaceholderCooldown(),
		hc: &http.Client{
			Timeout: 0, // long-lived live streams
			Transport: &http.Transport{
				ResponseHeaderTimeout: defaultSourceStartupTimeout,
				IdleConnTimeout:       30 * time.Second,
				DisableCompression:    true,
			},
		},
	}
}

// SetUpstreamProxy sends every provider fetch — the panel request and the
// origin it redirects to — through proxy, so both see the same egress IP.
// Call before serving; nil keeps them direct.
func (p *Pool) SetUpstreamProxy(proxy *url.URL) {
	p.hc.Transport.(*http.Transport).Proxy = http.ProxyURL(proxy)
}

// CapacityChanges exposes coalesced slot-release hints. Consumers must treat
// a receive as "retry now", not as proof that capacity still exists.
func (p *Pool) CapacityChanges() <-chan struct{} { return p.capacityChanged }

func (p *Pool) notifyCapacityChange() {
	select {
	case p.capacityChanged <- struct{}{}:
	default:
	}
}

// Active returns the count of in-flight streams (DB-backed truth).
func (p *Pool) Active(ctx context.Context) int {
	rows, err := p.db.ListActiveStreams(ctx)
	if err != nil {
		return 0
	}
	return len(rows)
}

// ServeWriter is the io.Writer-flavored sibling of Serve. Used by the DVR
// recorder so a scheduled recording reuses the same slot leasing + Streamer
// fanout machinery as a Plex tune (one upstream, N subscribers — recorder
// is just another subscriber alongside any concurrent Plex clients).
//
// Blocks until ctx is cancelled OR the upstream EOFs OR w errors.
// Writes are flushed if w implements http.Flusher (it usually doesn't for
// file destinations, but the test path uses httptest recorders).
//
// On disconnect: decrements refcount on the active_stream the same way
// Serve does. Caller is expected to close w themselves.
//
// The returned lastRealChunk is the arrival time carried by the last real
// upstream chunk this DVR subscriber successfully wrote. It is subscriber-
// specific: another viewer sharing the pump cannot make an incomplete DVR
// capture appear to cover the scheduled end. Synthetic filler carries no
// real-media timestamp and never advances it.
func (p *Pool) ServeWriter(ctx context.Context, w io.Writer, channelID uuid.UUID) (bytesWritten int64, lastRealChunk time.Time, err error) {
	reservation, err := p.ReserveWriter(ctx, channelID, 0)
	if err != nil {
		if errors.Is(err, store.ErrNoSlot) && p.Alerts != nil {
			go p.Alerts.Send(context.Background(), alerts.Event{
				Severity: alerts.SevError,
				Source:   "conductor.dvr",
				Title:    "DVR slot exhausted on channel " + channelID.String(),
				Detail:   "All credentials for every source on this channel are at max_streams; recording will not run.",
				Tags:     map[string]any{"channel_id": channelID.String()},
			})
		}
		return 0, time.Time{}, err
	}
	return reservation.Serve(ctx, w)
}

// DVRMediaCoverage is subscriber-specific wall-clock evidence for real
// upstream media. Synthetic Conductor slate never advances it. MaxRealGap is
// clipped to the requested programme window, so outages confined to pre/post
// roll do not invalidate an otherwise complete recording.
type DVRMediaCoverage struct {
	FirstRealChunk time.Time
	LastRealChunk  time.Time
	MaxRealGap     time.Duration
	MaxGapStart    time.Time
	MaxGapEnd      time.Time

	// UpstreamInterruptions is how many times the shared pump tore down a
	// byte-delivering upstream connection and kept pumping while this
	// recording was subscribed. Each one is a potential corrupt-slice splice
	// in the capture even when packet clocks stay continuous, so finalization
	// records it as capture provenance. It can include interruptions confined
	// to pre/post-roll padding; the artifact bitstream scan is the verdict.
	UpstreamInterruptions int
}

// ReserveWriter obtains the actual provider lease the recorder will use.
// Same-channel attaches do not consume another slot and bypass LiveReserve;
// new upstreams retain that many slots on the candidate provider (subject to
// the one-slot-provider clamp in store.AcquireDVRLease).
func (p *Pool) ReserveWriter(ctx context.Context, channelID uuid.UUID, liveReserve int) (*WriterReservation, error) {
	if p.isClosed() {
		return nil, ErrPoolClosed
	}
	if liveReserve < 0 {
		liveReserve = 0
	}
	roleLock := p.channelRoleLock(channelID)
	if err := roleLock.Lock(ctx); err != nil {
		return nil, fmt.Errorf("%w: wait for channel DVR admission: %w",
			store.ErrAdmissionContention, err)
	}
	defer roleLock.Unlock()
	if p.isClosed() {
		return nil, ErrPoolClosed
	}
	clientID := uuid.New()
	lease, err := p.db.AcquireDVRLeaseForClient(ctx, channelID, p.resolver, store.DVRLeasePolicy{
		LiveReserve: liveReserve,
	}, clientID)
	if err != nil {
		if errors.Is(err, store.ErrLeaseCommitAmbiguous) && lease.ActiveStreamID != uuid.Nil {
			p.retryAmbiguousLeaseCleanup(lease)
		}
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		// Runtime ownership has already been lost. Do not return while a
		// successfully committed lease remains visibly active; the shutdown
		// caller cannot safely assume cleanup from an untracked request goroutine.
		p.releaseAndMaybeDrain(lease, nil)
		return nil, ErrPoolClosed
	}
	p.addDVRRole(lease.ActiveStreamID, liveReserve)
	p.mu.Unlock()
	return &WriterReservation{
		pool: p, lease: lease, liveReserve: liveReserve,
	}, nil
}

// Release gives up a reservation that never reached Serve (for example when
// another scheduler won the atomic recording claim). Once Serve owns the
// reservation, its deferred release is authoritative.
func (r *WriterReservation) Release() {
	if r == nil || r.pool == nil {
		return
	}
	r.mu.Lock()
	if r.released || r.serving {
		r.mu.Unlock()
		return
	}
	r.released = true
	r.mu.Unlock()
	// The local client is gone even if PostgreSQL teardown failed. Keeping a
	// stale role would corrupt later live-vs-DVR relocation policy; DB cleanup
	// remains the watchdog's separate responsibility.
	r.pool.releaseClientRole(r.lease, false, r.liveReserve, nil)
}

// Serve consumes a reservation exactly once and streams it into w.
func (r *WriterReservation) Serve(ctx context.Context, w io.Writer) (bytesWritten int64, lastRealChunk time.Time, err error) {
	bytesWritten, coverage, err := r.ServeWithCoverage(ctx, w, time.Time{}, time.Time{})
	return bytesWritten, coverage.LastRealChunk, err
}

// ServeWithCoverage is the DVR recorder path. Programme bounds enable exact
// mid-window outage evidence while preserving Serve's compatibility for
// callers that only need the last-real-media timestamp.
func (r *WriterReservation) ServeWithCoverage(
	ctx context.Context,
	w io.Writer,
	programmeStart, programmeEnd time.Time,
) (bytesWritten int64, coverage DVRMediaCoverage, err error) {
	if r == nil || r.pool == nil {
		return 0, DVRMediaCoverage{}, errors.New("nil writer reservation")
	}
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return 0, DVRMediaCoverage{}, errors.New("writer reservation released")
	}
	if r.serving {
		r.mu.Unlock()
		return 0, DVRMediaCoverage{}, errors.New("writer reservation already served")
	}
	r.serving = true
	r.mu.Unlock()

	p := r.pool
	lease := r.lease
	startupCtx, cancelStartup, _ := startupContext(ctx)
	defer cancelStartup()

	s, chunks, unsubscribe, firstChunk, startErr := p.subscribeForStartup(
		startupCtx, lease, lease.LeaseClientID.String())
	if startErr != nil {
		r.abortStartupLease()
		if startupErr := startupPhaseError(ctx, startupCtx, startErr); startupErr != nil {
			return 0, DVRMediaCoverage{}, startupErr
		}
		return 0, DVRMediaCoverage{}, startErr
	}

	cancelStartup()

	// Interruptions before this subscription belong to earlier viewers; only
	// the delta across the serve window is this recording's provenance.
	interruptionsAtSubscribe := s.UpstreamInterruptions()
	defer func() {
		coverage.UpstreamInterruptions =
			int(s.UpstreamInterruptions() - interruptionsAtSubscribe)
		unsubscribe()
		r.releaseWithPump(s)
	}()

	writeChunk := func(chunk startupChunk) error {
		n, werr := writeDVRChunk(w, chunk)
		bytesWritten += int64(n)
		if n > 0 && !chunk.realAt.IsZero() {
			coverage.observe(chunk.realAt, programmeStart, programmeEnd)
		}
		if werr == nil && !chunk.realAt.IsZero() && n != len(chunk.data) {
			werr = io.ErrShortWrite
		}
		if werr != nil {
			return fmt.Errorf("%w: %w", ErrSinkWrite, werr)
		}
		return nil
	}

	werr := writeChunk(firstChunk)
	if werr != nil {
		return bytesWritten, coverage, werr
	}

	for {
		select {
		case <-ctx.Done():
			return bytesWritten, coverage, ctx.Err()
		case chunk, ok := <-chunks:
			if !ok {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return bytesWritten, coverage, ctxErr
				}
				if pumpErr := s.TerminalError(); pumpErr != nil {
					return bytesWritten, coverage, pumpErr
				}
				return bytesWritten, coverage, ErrStreamEnded
			}
			if werr := writeChunk(chunk); werr != nil {
				return bytesWritten, coverage, werr
			}
		}
	}
}

func (c *DVRMediaCoverage) observe(at, programmeStart, programmeEnd time.Time) {
	if at.IsZero() {
		return
	}
	if c.FirstRealChunk.IsZero() {
		c.FirstRealChunk = at
		c.LastRealChunk = at
		return
	}
	previous := c.LastRealChunk
	if at.After(previous) && programmeEnd.After(programmeStart) {
		gapStart := previous
		if programmeStart.After(gapStart) {
			gapStart = programmeStart
		}
		gapEnd := at
		if programmeEnd.Before(gapEnd) {
			gapEnd = programmeEnd
		}
		if gapEnd.After(gapStart) && gapEnd.Sub(gapStart) > c.MaxRealGap {
			c.MaxRealGap = gapEnd.Sub(gapStart)
			c.MaxGapStart = gapStart
			c.MaxGapEnd = gapEnd
		}
	}
	if at.After(c.LastRealChunk) {
		c.LastRealChunk = at
	}
}

// writeDVRChunk omits Conductor-generated Source Unavailable slate by exact
// provenance. It deliberately does not inspect luminance: legitimate dark
// scenes are real upstream media and must be recorded unchanged. Finite black
// provider placeholders are rejected before fanout by the bounded startup
// classifier, so they never acquire real-media provenance here.
func writeDVRChunk(w io.Writer, chunk startupChunk) (int, error) {
	if chunk.realAt.IsZero() {
		return 0, nil
	}
	return w.Write(chunk.data)
}

func (r *WriterReservation) releaseWithPump(s streamPump) {
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return
	}
	r.released = true
	r.mu.Unlock()
	r.pool.releaseClientRole(r.lease, false, r.liveReserve, s)
}

func (r *WriterReservation) abortStartupLease() {
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return
	}
	r.released = true
	r.mu.Unlock()
	r.pool.abortStartupLease(r.lease, false, r.liveReserve, nil)
}

func (p *Pool) channelRoleLock(channelID uuid.UUID) *contextGate {
	lock, _ := p.channelRoleLocks.LoadOrStore(channelID, newContextGate())
	return lock.(*contextGate)
}

func (p *Pool) addDVRRole(streamID uuid.UUID, liveReserve int) {
	p.rolesMu.Lock()
	defer p.rolesMu.Unlock()
	roles := p.clientRoles[streamID]
	if roles == nil {
		roles = &streamClientRoles{dvrReserves: make(map[int]int)}
		p.clientRoles[streamID] = roles
	}
	roles.dvrReserves[liveReserve]++
}

func (p *Pool) addLiveRole(streamID uuid.UUID) {
	p.rolesMu.Lock()
	defer p.rolesMu.Unlock()
	roles := p.clientRoles[streamID]
	if roles == nil {
		roles = &streamClientRoles{dvrReserves: make(map[int]int)}
		p.clientRoles[streamID] = roles
	}
	roles.live++
}

func (p *Pool) removeClientRole(
	streamID uuid.UUID,
	live bool,
	liveReserve int,
) {
	p.rolesMu.Lock()
	defer p.rolesMu.Unlock()
	roles := p.clientRoles[streamID]
	if roles == nil {
		return
	}
	if live {
		if roles.live > 0 {
			roles.live--
		}
	} else if roles.dvrReserves[liveReserve] <= 1 {
		delete(roles.dvrReserves, liveReserve)
	} else {
		roles.dvrReserves[liveReserve]--
	}
	if roles.live == 0 && len(roles.dvrReserves) == 0 {
		delete(p.clientRoles, streamID)
	}
}

// relocateStream gives live clients precedence: a viewer+DVR shared pump is a
// live upstream and may consume the reserved live slot. Only an all-DVR pump
// preserves the strongest attached DVR reserve on a cross-provider move.
func (p *Pool) relocateStream(
	ctx context.Context,
	streamID, channelID uuid.UUID,
	exclude []uuid.UUID,
) (store.Lease, error) {
	roleLock := p.channelRoleLock(channelID)
	if err := roleLock.Lock(ctx); err != nil {
		return store.Lease{}, fmt.Errorf("%w: wait for channel relocation policy: %w",
			store.ErrAdmissionContention, err)
	}
	defer roleLock.Unlock()
	p.rolesMu.Lock()
	roles := p.clientRoles[streamID]
	liveClients := 0
	var reserves map[int]int
	if roles != nil {
		liveClients = roles.live
		reserves = make(map[int]int, len(roles.dvrReserves))
		for reserve, count := range roles.dvrReserves {
			reserves[reserve] = count
		}
	}
	p.rolesMu.Unlock()
	var (
		next store.Lease
		err  error
	)
	if liveClients > 0 || len(reserves) == 0 {
		next, err = p.db.RelocateStream(ctx, streamID, channelID, exclude, p.resolver)
	} else {
		liveReserve := 0
		for reserve := range reserves {
			if reserve > liveReserve {
				liveReserve = reserve
			}
		}
		next, err = p.db.RelocateDVRStream(ctx, streamID, channelID, exclude, p.resolver,
			store.DVRLeasePolicy{LiveReserve: liveReserve})
	}
	if err == nil {
		// A cross-provider move frees old capacity. The hint is intentionally
		// coalesced and safe on same-provider reconnects too; DB admission
		// remains authoritative.
		p.notifyCapacityChange()
	}
	return next, err
}

// localPumpForTeardown includes unpublished terminal generations. Those
// pumps are intentionally unavailable for new subscribers but still own
// physical provider teardown until their OnExit callback completes.
func (p *Pool) localPumpForTeardown(streamID uuid.UUID) streamPump {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.streamers[streamID]; ok {
		return entry.pump
	}
	if exiting := p.exitingPumps[streamID]; len(exiting) > 0 {
		return exiting[0].pump
	}
	return nil
}

func (p *Pool) releaseAndMaybeDrain(lease store.Lease, s streamPump) {
	if lease.ActiveStreamID == uuid.Nil || lease.LeaseClientID == uuid.Nil {
		return
	}
	streamID := lease.ActiveStreamID
	// Keep the healthy-path release authoritative before a same-channel caller
	// can attach to a pump whose last subscriber has already gone. PostgreSQL
	// lock latency is still bounded to one cleanup attempt; an error or ambiguous
	// result falls through to the replay-safe background reconciler below.
	releaseCtx, cancel := context.WithTimeout(context.Background(), leaseCleanupAttemptTimeout)
	defer cancel()
	newCount, err := p.db.ReleaseLeaseClient(
		releaseCtx, streamID, lease.LeaseClientID)
	if err != nil {
		p.logger.Warn("release client", "stream", streamID, "err", err)
		p.retryLeaseCleanup(lease, s)
		return
	}
	if newCount != 0 {
		return
	}
	// An unserved reservation may not carry its pump pointer. Consult the
	// process-local active+exiting registries before deciding whether physical
	// provider teardown exists.
	if s == nil {
		s = p.localPumpForTeardown(streamID)
	}
	if s != nil && s.SubscriberCount() != 0 {
		return
	}
	if s == nil {
		capacityFree, finalizeErr := p.db.FinalizeIdleStreamWithoutPump(releaseCtx, streamID)
		if finalizeErr != nil {
			p.logger.Warn("finalize pump-less stream", "stream", streamID, "err", finalizeErr)
			p.retryLeaseCleanup(lease, nil)
			return
		}
		if capacityFree {
			p.notifyCapacityChange()
		} else {
			// A replay may find a zero-client row already draining after an
			// earlier teardown attempt committed but its pump-less MarkDead step
			// failed. Recheck process-local ownership without cancelling a pump
			// that an intervening client may have installed, then conditionally
			// complete only a still-zero-client draining row.
			p.finalizeDrainingIfPumpLess(lease.ChannelID, streamID)
		}
		return
	}
	cancelPump, finalizeErr := p.db.FinalizeIdleStreamForDrain(releaseCtx, streamID)
	if finalizeErr != nil {
		p.logger.Warn("finalize released stream draining", "stream", streamID, "err", finalizeErr)
		p.retryLeaseCleanup(lease, s)
		return
	}
	if cancelPump {
		// Draining remains capacity-counted. OnExit publishes the wake only
		// after the upstream connection or ffmpeg process has torn down.
		p.CancelPumps([]uuid.UUID{streamID})
	}
}

func waitForSubscriberMedia(ctx context.Context, s streamPump, firstReal <-chan startupChunk) (startupChunk, error) {
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return startupChunk{}, ErrUpstreamNotReady
			}
			return startupChunk{}, ctx.Err()
		case ready, ok := <-firstReal:
			if !ok {
				return startupChunk{}, subscriberClosedBeforeMediaError(s)
			}
			// If media and the absolute cutoff become ready together, the
			// deadline wins. Returning the chunk here could otherwise commit a
			// late 200 after Plex has already abandoned the tuner startup.
			if ctxErr := ctx.Err(); ctxErr != nil {
				if errors.Is(ctxErr, context.DeadlineExceeded) {
					return startupChunk{}, ErrUpstreamNotReady
				}
				return startupChunk{}, ctxErr
			}
			if s.MediaEpoch() != ready.epoch {
				return startupChunk{}, errors.Join(ErrUpstreamNotReady,
					errPumpDiscontinuityBeforeMedia)
			}
			if len(ready.data) > 0 {
				return ready, nil
			}
		}
	}
}

func subscriberClosedBeforeMediaError(s streamPump) error {
	if terminal := s.TerminalError(); terminal != nil {
		if errors.Is(terminal, store.ErrRelocationStateUnknown) {
			// Preserve only credential-safe typed markers across the pre-byte API
			// boundary. The terminal may wrap a raw COMMIT/resolver error that is
			// valuable in private logs but must never be reflected to Plex.
			return errors.Join(ErrUpstreamNotReady, errPumpStoppedBeforeMedia,
				store.ErrLeaseOperational, store.ErrRelocationStateUnknown)
		}
		return fmt.Errorf("%w: %w: %s", ErrUpstreamNotReady,
			errPumpStoppedBeforeMedia, diagnosticReason(terminal))
	}
	return fmt.Errorf("%w: %w", ErrUpstreamNotReady, errPumpStoppedBeforeMedia)
}

func startupSubscriberTerminalError(s streamPump, terminal <-chan struct{}) error {
	if terminal == nil {
		return errors.Join(ErrUpstreamNotReady, errUncertainMediaPrefix)
	}
	select {
	case <-terminal:
		return subscriberClosedBeforeMediaError(s)
	default:
		return nil
	}
}

// validateLateSubscriberStart keeps an unmarked ring tail private until the
// same bounded validator used at pump startup proves a fresh decoder-safe
// random-access window. The validated-start marker is present only while the
// ring still begins at the pump's original proven prefix; its absence means a
// late join may begin after PAT/PMT, codec configuration, or IDR/IRAP data.
func validateLateSubscriberStart(
	ctx context.Context,
	s streamPump,
	first startupChunk,
	chunks <-chan startupChunk,
	subscriberTerminal <-chan struct{},
	validator mediaPrefixValidator,
) (startupChunk, error) {
	if first.validatedStart {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return startupChunk{}, ErrUpstreamNotReady
			}
			return startupChunk{}, ctxErr
		}
		if s.MediaEpoch() != first.epoch {
			return startupChunk{}, errors.Join(ErrUpstreamNotReady,
				errPumpDiscontinuityBeforeMedia)
		}
		if terminalErr := startupSubscriberTerminalError(s, subscriberTerminal); terminalErr != nil {
			return startupChunk{}, terminalErr
		}
		return first, nil
	}
	if validator == nil {
		return startupChunk{}, errors.Join(ErrUpstreamNotReady,
			errUncertainMediaPrefix)
	}

	epoch := first.epoch
	latestRealAt := first.realAt
	gate := newStartupMediaGate(validator)
	current := first
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return startupChunk{}, ErrUpstreamNotReady
			}
			return startupChunk{}, ctxErr
		}
		if current.epoch != epoch || s.MediaEpoch() != epoch {
			return startupChunk{}, errors.Join(ErrUpstreamNotReady,
				errPumpDiscontinuityBeforeMedia)
		}
		if terminalErr := startupSubscriberTerminalError(s, subscriberTerminal); terminalErr != nil {
			return startupChunk{}, terminalErr
		}
		if current.realAt.After(latestRealAt) {
			latestRealAt = current.realAt
		}
		media, ready, gateErr := gate.Push(ctx, current.data)
		if terminalErr := startupSubscriberTerminalError(s, subscriberTerminal); terminalErr != nil {
			return startupChunk{}, terminalErr
		}
		if gateErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				if errors.Is(ctxErr, context.DeadlineExceeded) {
					return startupChunk{}, ErrUpstreamNotReady
				}
				return startupChunk{}, ctxErr
			}
			return startupChunk{}, fmt.Errorf("%w: late subscriber media prefix: %w",
				ErrUpstreamNotReady, classifyMediaPrefixValidationError(gateErr))
		}
		if ready {
			return releaseLateSubscriberStart(
				ctx, s, gate, epoch, latestRealAt, media, subscriberTerminal)
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return startupChunk{}, ErrUpstreamNotReady
			}
			return startupChunk{}, ctx.Err()
		case <-subscriberTerminal:
			return startupChunk{}, subscriberClosedBeforeMediaError(s)
		case next, ok := <-chunks:
			if !ok {
				if ctxErr := ctx.Err(); ctxErr != nil {
					if errors.Is(ctxErr, context.DeadlineExceeded) {
						return startupChunk{}, ErrUpstreamNotReady
					}
					return startupChunk{}, ctxErr
				}
				// A closed continuation channel means this subscriber worker is
				// already terminal. Finalizing and publishing its buffered candidate
				// could commit HTTP 200 followed immediately by EOF while the shared
				// pump itself remains healthy (for example, after a subscriber-local
				// delivery timeout during ffprobe). There is no successful live-start
				// path without an owned continuation, even if the buffered bytes would
				// independently decode.
				return startupChunk{}, subscriberClosedBeforeMediaError(s)
			}
			// As in waitForSubscriberMedia, an absolute startup cutoff that becomes
			// ready with a chunk wins before any response bytes can be committed.
			if ctxErr := ctx.Err(); ctxErr != nil {
				if errors.Is(ctxErr, context.DeadlineExceeded) {
					return startupChunk{}, ErrUpstreamNotReady
				}
				return startupChunk{}, ctxErr
			}
			current = next
		}
	}
}

func releaseLateSubscriberStart(
	ctx context.Context,
	s streamPump,
	gate *startupMediaGate,
	epoch uint64,
	realAt time.Time,
	media []byte,
	subscriberTerminal <-chan struct{},
) (startupChunk, error) {
	// Pump startup historically permits a non-TS bypass when ffprobe is not
	// installed. A known MPEG-TS late join cannot use that fallback: doing so
	// would recreate the exact unconfigured ring-tail failure this gate fixes.
	if gate.Reason() == "ffprobe_unavailable" {
		return startupChunk{}, errors.Join(ErrUpstreamNotReady,
			errUncertainMediaPrefix)
	}
	media = markInitialTSDiscontinuity(media)
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			return startupChunk{}, ErrUpstreamNotReady
		}
		return startupChunk{}, ctxErr
	}
	if s.MediaEpoch() != epoch {
		return startupChunk{}, errors.Join(ErrUpstreamNotReady,
			errPumpDiscontinuityBeforeMedia)
	}
	if terminalErr := startupSubscriberTerminalError(s, subscriberTerminal); terminalErr != nil {
		return startupChunk{}, terminalErr
	}
	return startupChunk{
		data:           media,
		epoch:          epoch,
		realAt:         realAt,
		validatedStart: true,
	}, nil
}

func startupPhaseError(parent, startup context.Context, cause error) error {
	var phaseErr error
	if err := parent.Err(); err != nil {
		phaseErr = err
	} else if errors.Is(startup.Err(), context.DeadlineExceeded) {
		phaseErr = ErrUpstreamNotReady
	}
	if phaseErr == nil {
		return nil
	}
	if errors.Is(cause, store.ErrLeaseOperational) ||
		errors.Is(cause, store.ErrAdmissionContention) ||
		errors.Is(cause, store.ErrNoSlot) ||
		errors.Is(cause, ErrPoolClosed) ||
		errors.Is(cause, store.ErrRelocationStateUnknown) {
		// Deadline remains visible to Plex, while safe typed local-capacity/
		// operational causes survive for scheduler classification and private
		// diagnostics. Neither marker contains credential or upstream URL data.
		return errors.Join(phaseErr, cause)
	}
	return phaseErr
}

// abortStartupLease normally keeps durable cleanup off Plex's response path.
// Once Pool.Close has begun, runtime handoff instead requires the committed
// token to be released synchronously before the stopped caller returns.
func (p *Pool) abortStartupLease(
	lease store.Lease,
	live bool,
	liveReserve int,
	s streamPump,
) {
	p.removeClientRole(lease.ActiveStreamID, live, liveReserve)
	if p.isClosed() {
		p.releaseAndMaybeDrain(lease, s)
		return
	}
	p.retryLeaseCleanup(lease, s)
}

func (p *Pool) retryLeaseCleanup(lease store.Lease, hintedPump streamPump) {
	if lease.ActiveStreamID == uuid.Nil || lease.LeaseClientID == uuid.Nil {
		return
	}
	p.cleanupWG.Add(1)
	go func() {
		defer p.cleanupWG.Done()
		overall, cancel := context.WithTimeout(
			context.Background(), leaseCleanupRetryWindow)
		defer cancel()
		backoff := leaseCleanupInitialBackoff
		attempt := 0
		for {
			attempt++
			attemptCtx, attemptCancel := context.WithTimeout(
				overall, leaseCleanupAttemptTimeout)
			newCount, err := p.db.ReleaseLeaseClient(
				attemptCtx, lease.ActiveStreamID, lease.LeaseClientID)
			if err == nil && newCount != 0 {
				attemptCancel()
				return // this client is gone; another subscriber owns the pump
			}
			if err == nil {
				s := hintedPump
				if s == nil {
					s = p.localPumpForTeardown(lease.ActiveStreamID)
				}
				if s != nil && s.SubscriberCount() != 0 {
					attemptCancel()
					return
				}
				if s == nil {
					var capacityFree bool
					capacityFree, err = p.db.FinalizeIdleStreamWithoutPump(
						attemptCtx, lease.ActiveStreamID)
					if err == nil {
						attemptCancel()
						if capacityFree {
							p.notifyCapacityChange()
						} else {
							// See releaseAndMaybeDrain: false may be a replay of an
							// already-draining, pump-less row rather than proof that a
							// client still owns it. Revalidate without cancelling an
							// intervening client's pump.
							p.finalizeDrainingIfPumpLess(lease.ChannelID, lease.ActiveStreamID)
						}
						return
					}
				} else {
					var cancelPump bool
					cancelPump, err = p.db.FinalizeIdleStreamForDrain(
						attemptCtx, lease.ActiveStreamID)
					if err == nil {
						attemptCancel()
						if cancelPump {
							p.CancelPumps([]uuid.UUID{lease.ActiveStreamID})
						}
						return
					}
				}
			}
			attemptCancel()
			if overall.Err() != nil {
				p.logger.Warn("lease client cleanup exhausted",
					"stream", lease.ActiveStreamID, "attempts", attempt, "err", err)
				return
			}
			p.logger.Warn("lease client cleanup retry",
				"stream", lease.ActiveStreamID, "attempt", attempt, "err", err)
			timer := time.NewTimer(backoff)
			select {
			case <-overall.Done():
				timer.Stop()
				p.logger.Warn("lease client cleanup exhausted",
					"stream", lease.ActiveStreamID, "attempts", attempt,
					"err", overall.Err())
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, leaseCleanupMaxBackoff)
		}
	}()
}

// retryAmbiguousLeaseCleanup first waits on the same channel advisory lock as
// acquire. That lock is the outcome boundary: once obtained, found=false means
// the uncertain transaction rolled back, while found=true yields the exact
// committed token/row that normal idempotent cleanup can release.
func (p *Pool) retryAmbiguousLeaseCleanup(provisional store.Lease) {
	if provisional.ChannelID == uuid.Nil || provisional.ActiveStreamID == uuid.Nil ||
		provisional.LeaseClientID == uuid.Nil {
		return
	}
	p.cleanupWG.Add(1)
	go func() {
		defer p.cleanupWG.Done()
		overall, cancel := context.WithTimeout(
			context.Background(), leaseCleanupRetryWindow)
		defer cancel()
		backoff := leaseCleanupInitialBackoff
		attempt := 0
		for {
			attempt++
			attemptCtx, attemptCancel := context.WithTimeout(
				overall, leaseCleanupAttemptTimeout)
			lease, found, err := p.db.ResolveAmbiguousLeaseClient(attemptCtx, provisional)
			attemptCancel()
			if err == nil {
				if found {
					p.releaseAndMaybeDrain(lease, nil)
				}
				return
			}
			if overall.Err() != nil {
				p.logger.Warn("ambiguous lease cleanup exhausted",
					"stream", provisional.ActiveStreamID, "attempts", attempt, "err", err)
				return
			}
			timer := time.NewTimer(backoff)
			select {
			case <-overall.Done():
				timer.Stop()
				p.logger.Warn("ambiguous lease cleanup exhausted",
					"stream", provisional.ActiveStreamID, "attempts", attempt,
					"err", overall.Err())
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, leaseCleanupMaxBackoff)
		}
	}()
}

func (p *Pool) releaseLiveClient(lease store.Lease, s streamPump) {
	p.releaseClientRole(lease, true, 0, s)
}

func (p *Pool) releaseClientRole(
	lease store.Lease,
	unrestricted bool,
	liveReserve int,
	s streamPump,
) {
	// The local client is already gone. Remove its role in the short map
	// critical section before potentially slow DB teardown. A relocation that
	// already snapshotted the old role may validly finish with that policy; a
	// new bounded startup relocation never waits behind release cleanup.
	p.removeClientRole(lease.ActiveStreamID, unrestricted, liveReserve)
	// The common uncontended release completes before the next same-channel
	// acquisition, preserving exact pump-generation ordering. A locked row or
	// lost COMMIT is bounded by leaseCleanupAttemptTimeout and automatically
	// continues in retryLeaseCleanup, so Plex/DVR teardown never inherits the
	// longer reconciliation window.
	p.releaseAndMaybeDrain(lease, s)
}

// Serve handles one Plex client tune. It leases (or attaches to) an upstream,
// streams to the client, and decrements refcount on disconnect. Blocks for
// the lifetime of the client connection.
func (p *Pool) Serve(ctx context.Context, w http.ResponseWriter, r *http.Request, channelID uuid.UUID) error {
	if p.isClosed() {
		return ErrPoolClosed
	}
	startupCtx, cancelStartup, _ := startupContext(ctx)
	defer cancelStartup()

	roleLock := p.channelRoleLock(channelID)
	if err := roleLock.Lock(startupCtx); err != nil {
		return fmt.Errorf("%w: wait for live channel admission: %w",
			store.ErrAdmissionContention, err)
	}
	if p.isClosed() {
		roleLock.Unlock()
		return ErrPoolClosed
	}
	clientID := uuid.New()
	lease, err := p.db.AcquireLeaseForClient(startupCtx, channelID, p.resolver, clientID)
	if err == nil {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			roleLock.Unlock()
			p.releaseAndMaybeDrain(lease, nil)
			return ErrPoolClosed
		}
		p.addLiveRole(lease.ActiveStreamID)
		p.mu.Unlock()
	}
	roleLock.Unlock()
	if err != nil {
		if errors.Is(err, store.ErrLeaseCommitAmbiguous) && lease.ActiveStreamID != uuid.Nil {
			p.retryAmbiguousLeaseCleanup(lease)
		}
		if startupErr := startupPhaseError(ctx, startupCtx, err); startupErr != nil {
			return startupErr
		}
		if errors.Is(err, store.ErrNoSlot) {
			// Live-tune slot exhaustion is normal capacity backpressure: the
			// client just gets a no-slot error and Plex retries. It is NOT an
			// ops incident, so log it instead of alerting the ops topic. (The
			// DVR path in ServeWriter still alerts — a missed recording is
			// actionable.)
			p.logger.Info("live tune slot exhausted; no free credential",
				"channel", channelID)
		}
		return err
	}

	s, chunks, unsubscribe, firstChunk, startErr := p.subscribeForStartup(
		startupCtx, lease, clientID.String())
	if startErr != nil {
		p.abortStartupLease(lease, true, 0, nil)
		if startupErr := startupPhaseError(ctx, startupCtx, startErr); startupErr != nil {
			return startupErr
		}
		return startErr
	}

	cancelStartup()

	defer func() {
		unsubscribe()
		p.releaseLiveClient(lease, s)
	}()

	// Mirror upstream headers (or fall back to MPEG-TS, which Plex prefers).
	if ct := s.ContentType(); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "video/mp2t")
	}
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	if _, werr := w.Write(firstChunk.data); werr != nil {
		return werr
	}
	if flusher != nil {
		flusher.Flush()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-chunks:
			if !ok {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				if pumpErr := s.TerminalError(); pumpErr != nil {
					return pumpErr
				}
				return ErrStreamEnded
			}
			if _, werr := w.Write(chunk.data); werr != nil {
				return werr
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// subscribeForStartup retains the durable lease token across the narrow race
// where getOrStart found an existing pump immediately before OnStopping
// detached and closed it. Since no media was written, retrying the no-pump path
// is safe and lets a DVR reservation install a replacement instead of failing
// after its recording claim. Both newly-created and pre-existing pumps get a
// bounded pre-byte retry under the caller's original absolute deadline.
func (p *Pool) subscribeForStartup(
	ctx context.Context,
	lease store.Lease,
	clientID string,
) (streamPump, <-chan startupChunk, func(), startupChunk, error) {
	for retry := 0; ; retry++ {
		s, firstReal, chunks, subscriberTerminal, unsubscribe, err := p.getOrStartStreamer(
			ctx, lease, clientID)
		if err != nil {
			return nil, nil, nil, startupChunk{}, err
		}
		firstChunk, waitErr := waitForSubscriberMedia(ctx, s, firstReal)
		// A subscriber can win the ring-preload mutex immediately before the
		// pump commits to stopping. Never let that stale pre-roll become the
		// DVR's first byte; retain the durable token and install/attach to the
		// replacement instead.
		staleStoppingPump := startupMediaInvalidatedByStop(s, waitErr)
		if waitErr == nil && !staleStoppingPump {
			firstChunk, waitErr = validateLateSubscriberStart(
				ctx, s, firstChunk, chunks, subscriberTerminal,
				newFFprobeMediaPrefixValidator(p.FFmpegBinary),
			)
			staleStoppingPump = startupMediaInvalidatedByStop(s, waitErr)
			if waitErr == nil && !staleStoppingPump {
				return s, chunks, unsubscribe, firstChunk, nil
			}
		}
		unsubscribe()
		if staleStoppingPump {
			waitErr = fmt.Errorf("%w: %w", ErrUpstreamNotReady,
				errPumpStoppedBeforeMedia)
		}
		if !retryStartupAttachment(waitErr) ||
			ctx.Err() != nil || retry >= closedPumpAttachRetryLimit {
			return nil, nil, nil, startupChunk{}, waitErr
		}
		p.logger.Info("startup attachment invalidated; retrying bounded attachment",
			"stream", lease.ActiveStreamID, "retry", retry+1)
	}
}

func startupMediaInvalidatedByStop(s streamPump, waitErr error) bool {
	return waitErr == nil && s.Stopping()
}

// retryStartupAttachment retains the same durable token across a pre-byte
// discontinuity or terminal transition. Stopping is marked before terminal
// error publication, so retry cannot depend on seeing a typed relocation
// marker in that narrow window. The caller's absolute deadline and retry cap
// bound ordinary exhausted-source terminals too.
func retryStartupAttachment(err error) bool {
	return errors.Is(err, errPumpDiscontinuityBeforeMedia) ||
		errors.Is(err, errPumpStoppedBeforeMedia)
}

func (p *Pool) effectiveTranscodeProfile(
	ctx context.Context,
	sourceID uuid.UUID,
) (*store.TranscodeProfile, error) {
	profile, err := p.db.EffectiveTranscodeProfileForLease(ctx, sourceID)
	if err == nil {
		return profile, nil
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("%w: transcode profile lookup for source %s: %w",
			store.ErrLeaseOperational, sourceID, err)
	}
	p.logger.Warn("transcode profile lookup failed; falling back to passthrough",
		"source", sourceID, "err", err)
	return nil, nil
}

// getOrStartStreamer returns the Streamer for this active_stream, starting
// one if there isn't one already. Concurrency-safe.
//
// Selects TranscodeStreamer when a transcode profile is configured for
// the source (or its parent channel); otherwise plain passthrough Streamer.
func (p *Pool) getOrStartStreamer(
	ctx context.Context,
	lease store.Lease,
	clientID string,
) (streamPump, <-chan startupChunk, <-chan startupChunk, <-chan struct{}, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, nil, nil, nil, ErrPoolClosed
	}
	if e, ok := p.streamers[lease.ActiveStreamID]; ok && !e.pump.Stopping() {
		firstReal, chunks, subscriberTerminal, unsubscribe := e.pump.SubscribeForStartup(clientID)
		p.mu.Unlock()
		return e.pump, firstReal, chunks, subscriberTerminal, unsubscribe, nil
	}
	// A pump marks itself stopping at the terminal decision, before its
	// OnStopping callback can acquire p.mu. Unpublish it here so an attacher
	// does not burn its bounded retries on the same already-closed generation.
	// Its delayed identity-aware callback cannot remove the replacement.
	var stoppingCancel context.CancelFunc
	if e, ok := p.streamers[lease.ActiveStreamID]; ok && e.pump.Stopping() {
		stoppingCancel = p.trackExitingPumpLocked(lease.ActiveStreamID, e)
	}
	p.mu.Unlock()
	if stoppingCancel != nil {
		stoppingCancel()
	}

	// Profile lookup can block on PostgreSQL independently for each request.
	// Do it before the channel creation gate so one long lookup cannot consume
	// a later subscriber's shorter absolute startup deadline. The source is
	// revalidated after claiming the authoritative pump generation below.
	profileSourceID := lease.ChannelSourceID
	profile, err := p.effectiveTranscodeProfile(ctx, profileSourceID)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Serialize the no-pump path with same-channel acquire and relocation. In
	// addition to keeping the authoritative reload stable, this ensures exactly
	// one creator claims a persisted pump generation; concurrent creators either
	// find that installed pump on the recheck or wait within their own deadline.
	creationLock := p.channelRoleLock(lease.ChannelID)
	if err := creationLock.Lock(ctx); err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf(
			"%w: wait for channel pump creation: %w", store.ErrAdmissionContention, err)
	}
	defer creationLock.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, nil, nil, nil, ErrPoolClosed
	}
	if e, ok := p.streamers[lease.ActiveStreamID]; ok && !e.pump.Stopping() {
		firstReal, chunks, subscriberTerminal, unsubscribe := e.pump.SubscribeForStartup(clientID)
		p.mu.Unlock()
		return e.pump, firstReal, chunks, subscriberTerminal, unsubscribe, nil
	}
	if e, ok := p.streamers[lease.ActiveStreamID]; ok && e.pump.Stopping() {
		p.trackExitingPumpLocked(lease.ActiveStreamID, e)
	}
	exiting := append([]pumpEntry(nil), p.exitingPumps[lease.ActiveStreamID]...)
	p.mu.Unlock()
	if err := cancelAndWaitForPhysicalExit(ctx, exiting); err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf(
			"%w: wait for prior pump physical teardown: %w",
			store.ErrAdmissionContention, err)
	}

	// A durable reservation can outlive the shared process-local pump that was
	// running when it was admitted. Reload the current row before installing a
	// replacement and atomically claim a new generation: that pump may have
	// relocated, so the reservation's original source/credential/URL snapshot
	// is not authoritative, and an old OnExit must not terminalize this pump.
	currentLease, err := p.db.ClaimPumpLeaseForClient(
		ctx, lease.ActiveStreamID, lease.LeaseClientID)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf(
			"%w: refresh admitted lease: %w", store.ErrLeaseOperational, err)
	}
	lease = currentLease
	if lease.ChannelSourceID != profileSourceID {
		// A held reservation may outlive a relocation. Its optimistic profile
		// lookup used the old source, so resolve the authoritative source before
		// constructing the replacement pump.
		profile, err = p.effectiveTranscodeProfile(ctx, lease.ChannelSourceID)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	streamID := lease.ActiveStreamID
	channelID := lease.ChannelID

	// srcID is shared by the callbacks below and rewritten on failover.
	// All callbacks fire from the single pump goroutine, so plain access
	// is safe.
	srcID := lease.ChannelSourceID
	hasDeliveredMedia := false
	stableSourceID := uuid.Nil
	// Construct the heartbeat worker only after the pump wins the final
	// installation gates below; callbacks cannot fire before runPump starts.
	var persistence *pumpPersistence

	// failedThisGap tracks sources already tried during the current
	// reconnect gap so RelocateStream advances to the NEXT candidate
	// (candidate order is priority-first — a dead priority-0 source never
	// drops below a healthy priority-1 on health score alone). It is cleared
	// only after an attempt delivers stable bytes, not merely when the
	// provider returns HTTP 200: some origins return 200 + a short fragment +
	// EOF after their connection cap, and clearing on headers made those
	// sources ping-pong forever. Only the pump goroutine touches it.
	failedThisGap := make(map[uuid.UUID]bool)
	var pump streamPump
	physicalExit := make(chan struct{})

	onRunning := func() {
		// This callback runs while the validated prefix is still private and
		// before its release-edge timestamp is chosen. Bound the durable state
		// transition tightly, then pace/fanout without DB latency in between.
		bg, cancel := context.WithTimeout(context.Background(), pumpMarkRunningTimeout)
		defer cancel()
		_ = p.db.MarkRunningForPump(bg, streamID, lease.PumpGeneration)
	}
	onStable := func() {
		stableSourceID = srcID
		clear(failedThisGap)
	}
	onExit := func(err error) {
		// Streamer/TranscodeStreamer invokes OnExit only after the current
		// upstream attempt (and ffmpeg child, when present) has closed. Publish
		// that physical boundary before bounded persistence/DB finalization so a
		// same-row replacement cannot overlap provider connections.
		close(physicalExit)
		if persistence != nil && !persistence.Stop(pumpPersistenceStopMax) {
			p.logger.Warn("pump persistence worker exceeded bounded stop",
				"stream", streamID, "generation", lease.PumpGeneration)
		}
		bg, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err != nil {
			metrics.StreamsFailed.Inc()
			localPipelineFailure := errors.Is(err, errLocalTranscodeFailure)
			uncertainInput := errors.Is(err, errUncertainTranscodeInput)
			uncertainPrefix := errors.Is(err, errUncertainMediaPrefix)
			relayBufferFull := errors.Is(err, errRelayReadAheadFull)
			staleRelayChunk := errors.Is(err, errTransportStaleChunk)
			relayPacingBacklog := errors.Is(err, errTransportRelayBacklog)
			localRecoveryBacklog := localPipelineFailure && errors.Is(err, errTransportRecoveryBacklog)
			relocationUnknown := errors.Is(err, store.ErrRelocationStateUnknown)
			if relocationUnknown {
				p.logger.Warn("stream stopped after unresolved relocation state",
					"stream", streamID, "err", err)
			} else if staleRelayChunk {
				p.logger.Warn("local relay chunk exceeded live latency bound",
					"stream", streamID, "err", err)
			} else if relayPacingBacklog {
				p.logger.Warn("validated source prefix exceeded live pacing bound",
					"stream", streamID, "err", err)
			} else if localRecoveryBacklog {
				p.logger.Warn("local transcode pacing backlog exceeded live bound",
					"stream", streamID, "err", err)
			} else if localPipelineFailure {
				p.logger.Warn("local transcode pipeline failed",
					"stream", streamID, "err", err)
			} else if uncertainInput {
				p.logger.Warn("transcode source evidence inconclusive",
					"stream", streamID, "err", err)
			} else if uncertainPrefix {
				p.logger.Warn("local media-prefix validation inconclusive",
					"stream", streamID, "err", err)
			} else if relayBufferFull {
				p.logger.Warn("bounded relay read-ahead exhausted",
					"stream", streamID, "err", err)
			} else {
				p.logger.Warn("upstream exited with error",
					"stream", streamID, "err", err)
			}
			// A single upstream exit is transient: MarkSourceFailure feeds the
			// source health score and the pump relocates to the next candidate
			// on its own, so this self-heals. It's already logged above; don't
			// alert the ops topic — per-stream upstream blips are not
			// actionable. Sustained trouble surfaces via the EPG-source and
			// worker-stall alerts instead.
			if shouldMarkSourceFailure(err) {
				_ = p.db.MarkSourceFailure(bg, srcID)
			}
		} else if shouldMarkSourceSuccess(err, hasDeliveredMedia, stableSourceID, srcID) {
			_ = p.db.MarkSourceSuccess(bg, srcID)
		}
		p.finishPump(streamID, lease.PumpGeneration, pump)
	}
	onBytes := func(n int) error {
		if n > 0 {
			hasDeliveredMedia = true
			metrics.UpstreamBytes.Add(uint64(n))
			if persistence != nil {
				persistence.AddBytes(int64(n))
			}
		}
		return nil
	}

	onUpstreamDown := func(_ context.Context, cause error) (string, bool, error) {
		metrics.ReconnectAttempts.Inc()
		// A prior stable attempt cannot credit the source selected for the next
		// attempt (including a same-source reconnect). Only that new attempt's
		// own OnStable callback may restore success eligibility.
		stableSourceID = uuid.Nil
		relocateTimeout := midstreamRelocateTimeout
		if !hasDeliveredMedia {
			// Initial relocation is bounded independently of whichever viewer
			// happened to create the shared pump. Each subscriber has its own
			// absolute Plex startup deadline; a later viewer must retain enough
			// time to reach source two even if the creator is about to time out.
			relocateTimeout = startupRelocateTimeout
		}
		failedSourceID := srcID
		if shouldMarkSourceFailure(cause) {
			p.markSourceFailureAdvisory(failedSourceID)
		}
		failedThisGap[failedSourceID] = true

		// Relocation gets its own complete budget. Health persistence above is
		// advisory and may be lock-delayed without stealing Plex startup time.
		bg, cancel := context.WithTimeout(context.Background(), relocateTimeout)
		defer cancel()

		exclude := make([]uuid.UUID, 0, len(failedThisGap))
		for id := range failedThisGap {
			exclude = append(exclude, id)
		}
		next, err := p.relocateStream(bg, streamID, channelID, exclude)
		if errors.Is(err, store.ErrNoSlot) {
			// Every non-excluded source returned no slot. Distinguish two
			// cases (audit S2):
			//
			//  - Multi-source channel where every source has been tried AND
			//    failed this gap (failedThisGap now covers them all): there is
			//    nowhere healthy left to fail over to. Restarting the cycle
			//    here is what made the pump PING-PONG between two dead sources
			//    forever and never render the slate. Instead, stay on the
			//    current (just-failed) URL and signal retry: the reconnect
			//    loop feeds the Source-Unavailable slate during the backoff
			//    wait and the growing backoff + reconnectWindow terminate the
			//    stream cleanly. failedThisGap is left intact so subsequent
			//    cycles short-circuit straight back here (no fresh relocate),
			//    keeping it a backoff loop, not a tight spin. onRunning clears
			//    failedThisGap if a source ever comes back, re-enabling
			//    failover.
			//
			//  - Single-source channel (or a genuinely transient slot blip):
			//    excluding the only source always yields ErrNoSlot, and the
			//    correct behavior is a plain reconnect to that same source.
			total, cErr := p.db.CountEnabledSourcesForChannel(bg, channelID)
			if cErr == nil && total > 0 && len(failedThisGap) >= total {
				metrics.SlateHolds.Inc()
				p.logger.Info("all sources exhausted this gap; holding on slate",
					"stream", streamID, "channel", channelID,
					"sources_tried", len(failedThisGap), "cause", cause)
				return "", true, nil // retry current URL after backoff; slate feeds the gap
			}
			// Fresh cycle: current (just-failed) source included.
			clear(failedThisGap)
			next, err = p.relocateStream(bg, streamID, channelID, nil)
		}
		if err != nil {
			if errors.Is(err, store.ErrRelocationStateUnknown) {
				p.logger.Error("relocation commit state unresolved; stopping pump",
					"stream", streamID, "channel", channelID, "err", err)
				// The raw transaction error stays in the private log above. The
				// terminal path carries only the credential-safe relocation marker.
				// The pre-byte waiter adds ErrLeaseOperational for a retryable 503;
				// after media, keeping it absent prevents the API from appending an
				// HTTP error body to the MPEG-TS response.
				return "", false, store.ErrRelocationStateUnknown
			}
			if errors.Is(err, store.ErrStreamGone) {
				return "", false, nil // row reaped — nobody left to stream to
			}
			p.logger.Warn("mid-stream relocate failed; will retry current source",
				"stream", streamID, "channel", channelID, "cause", cause, "err", err)
			return "", true, nil
		}
		if next.ChannelSourceID == srcID {
			p.logger.Info("upstream reconnect on same source",
				"stream", streamID, "source", srcID, "cause", cause)
		} else {
			metrics.Failovers.Inc()
			p.logger.Info("mid-stream failover to next source",
				"stream", streamID, "from_source", srcID,
				"to_source", next.ChannelSourceID, "cause", cause)
		}
		srcID = next.ChannelSourceID
		return next.UpstreamURL, true, nil
	}

	onStopping := func() {
		// Both pump types invoke this as the first terminal action, before they
		// publish TerminalError or close subscribers. p.mu is the linearization:
		// once stopping wins it, no attacher can discover a terminal pump.
		// Identity checking prevents an old callback from deleting a replacement
		// installed under the same stream ID.
		p.detachPump(streamID, pump)
	}
	// Every pump log line, including the per-attempt diagnostic, carries the
	// channel and source it served so refusal paths can be counted per channel
	// in LogsQL without joining on stream ids.
	pumpLogger := p.logger.With(
		"channel_id", lease.ChannelID.String(),
		"source_id", lease.ChannelSourceID.String())
	// Limit the optional initial buffering hold to this subscriber's remaining
	// startup budget. Keep source attempts independent: a later subscriber may
	// still reach an alternate source after the creator's deadline has expired.
	startupHoldDeadline, _ := ctx.Deadline()
	if profile == nil {
		s := NewStreamer(streamID.String(), lease.UpstreamURL, pumpLogger, p.hc)
		s.classifier = newFFmpegFiniteMediaClassifier(p.FFmpegBinary)
		s.placeholders = p.placeholders
		s.prefixValidator = newFFprobeMediaPrefixValidator(p.FFmpegBinary)
		s.startupLead = p.LiveStartupLead
		s.startupHoldDeadline = startupHoldDeadline
		s.OnRunning = onRunning
		s.OnStable = onStable
		s.OnExit = onExit
		s.OnBytes = onBytes
		s.OnStopping = onStopping
		s.OnUpstreamDown = onUpstreamDown
		s.SlateFn = p.slateFn
		pump = s
	} else {
		// Adapt store.TranscodeProfile to transcode.Profile.
		tp := storeProfileToTranscode(profile)
		p.logger.Info("starting transcode pump",
			"stream", streamID, "source", srcID,
			"profile", profile.Name, "kind", profile.Kind,
			"nvenc", profile.UseNVENC, "nvdec", profile.UseNVDEC)
		s := NewTranscodeStreamer(streamID.String(), lease.UpstreamURL,
			tp, p.FFmpegBinary, pumpLogger, p.hc)
		s.classifier = newFFmpegFiniteMediaClassifier(p.FFmpegBinary)
		s.placeholders = p.placeholders
		s.prefixValidator = newFFprobeMediaPrefixValidator(p.FFmpegBinary)
		s.DiagDir = p.DiagDir
		s.startupLead = p.LiveStartupLead
		s.startupHoldDeadline = startupHoldDeadline
		s.OnRunning = onRunning
		s.OnStable = onStable
		s.OnExit = onExit
		s.OnBytes = onBytes
		s.OnStopping = onStopping
		s.OnUpstreamDown = onUpstreamDown
		s.SlateFn = p.slateFn
		pump = s
	}

	// The pump outlives the first client's request context on purpose
	// (later clients attach to it), but it must still be cancellable — by
	// the orphan sweeper when it reaps the row, and by Close on shutdown.
	// context.Background() here was audit finding S3: a wedged upstream
	// held the goroutine + provider connection until process restart.
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		pumpCancel()
		return nil, nil, nil, nil, nil, ErrPoolClosed
	}
	if err := ctx.Err(); err != nil {
		p.mu.Unlock()
		pumpCancel()
		return nil, nil, nil, nil, nil, err
	}
	persistence = newPumpPersistence(func(bg context.Context, batch int64) {
		_ = p.db.HeartbeatForPump(bg, streamID, lease.PumpGeneration, batch)
	})
	// Register the initiating client before the new pump can publish even one
	// byte. This closes the create/start race for fast accepted finite media;
	// concurrent creators cannot reach this block because creationLock remains
	// held from the authoritative generation claim through installation.
	firstReal, chunks, subscriberTerminal, unsubscribe := pump.SubscribeForStartup(clientID)
	p.streamers[streamID] = pumpEntry{
		pump:         pump,
		cancel:       pumpCancel,
		physicalExit: physicalExit,
		generation:   lease.PumpGeneration,
		installedAt:  time.Now(),
	}
	p.pumpWG.Add(1)
	p.mu.Unlock()

	metrics.StreamsStarted.Inc()
	go p.runPump(pumpCtx, pump)
	return pump, firstReal, chunks, subscriberTerminal, unsubscribe, nil
}

func (p *Pool) runPump(ctx context.Context, pump streamPump) {
	defer p.pumpWG.Done()
	pump.Run(ctx)
}

// cancelAndWaitForPhysicalExit prevents a replacement from opening an
// upstream connection while an unpublished generation still owns the old
// provider socket or ffmpeg process. The caller holds the per-channel creation
// gate, so no competing creator can bypass this bounded fence. A missing exit
// signal is unknowable ownership and therefore fails closed.
func cancelAndWaitForPhysicalExit(ctx context.Context, entries []pumpEntry) error {
	for _, entry := range entries {
		if entry.cancel != nil {
			entry.cancel()
		}
	}
	seen := make(map[<-chan struct{}]struct{}, len(entries))
	for _, entry := range entries {
		if entry.physicalExit == nil {
			return errors.New("prior pump has no physical-exit proof")
		}
		if _, duplicate := seen[entry.physicalExit]; duplicate {
			continue
		}
		seen[entry.physicalExit] = struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-entry.physicalExit:
		}
	}
	return nil
}

// trackExitingPumpLocked removes one attachment-visible generation while
// retaining it as a physical teardown owner. p.mu must be held. Identity
// checks at the caller prevent an old callback from moving a replacement.
func (p *Pool) trackExitingPumpLocked(
	streamID uuid.UUID,
	entry pumpEntry,
) context.CancelFunc {
	delete(p.streamers, streamID)
	if p.exitingPumps == nil {
		p.exitingPumps = make(map[uuid.UUID][]pumpEntry)
	}
	p.exitingPumps[streamID] = append(p.exitingPumps[streamID], entry)
	return entry.cancel
}

// detachPump makes pump shutdown structurally invisible before subscriber
// channels close, but keeps it capacity-visible until OnExit. expected makes
// a delayed old callback unable to erase a replacement pump installed under
// the same durable stream identity.
func (p *Pool) detachPump(streamID uuid.UUID, expected streamPump) {
	p.mu.Lock()
	entry, ok := p.streamers[streamID]
	var cancel context.CancelFunc
	if ok && entry.pump == expected {
		cancel = p.trackExitingPumpLocked(streamID, entry)
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel() // release the pump context's resources
	}
}

func (p *Pool) completePumpExit(streamID uuid.UUID, expected streamPump) bool {
	p.mu.Lock()
	exiting := p.exitingPumps[streamID]
	for i := range exiting {
		if exiting[i].pump != expected {
			continue
		}
		copy(exiting[i:], exiting[i+1:])
		exiting[len(exiting)-1] = pumpEntry{}
		exiting = exiting[:len(exiting)-1]
		break
	}
	if len(exiting) == 0 {
		delete(p.exitingPumps, streamID)
	} else {
		p.exitingPumps[streamID] = exiting
	}
	_, active := p.streamers[streamID]
	noLocalPump := !active && len(exiting) == 0
	p.mu.Unlock()
	return noLocalPump
}

// finishPump performs DB-only terminalization after upstream/ffmpeg teardown.
// The process map was already detached by OnStopping before subscribers were
// closed. Capacity is published only when no durable lease client remains; an
// admitted DVR token keeps the row active for a replacement pump.
func (p *Pool) finishPump(
	streamID, pumpGeneration uuid.UUID,
	expected streamPump,
) {
	markCtx, markCancel := context.WithTimeout(context.Background(), 5*time.Second)
	capacityFreed, err := p.db.MarkDeadAfterPumpExit(markCtx, streamID, pumpGeneration)
	markCancel()
	if p.pumpExitAfterMarkHook != nil {
		p.pumpExitAfterMarkHook()
	}
	noLocalPump := p.completePumpExit(streamID, expected)
	if err != nil {
		p.logger.Warn("finalize exited stream", "stream", streamID, "err", err)
	}
	if capacityFreed {
		p.notifyCapacityChange()
	}
	if noLocalPump {
		// MarkDeadAfterPumpExit can validly return false while a durable token
		// still exists. If that final token transitions the row to draining while
		// this generation remains in exitingPumps, its CancelPumps call defers to
		// us. Recheck after removing the last exiting generation; the conditional
		// state transition is safe against a replacement that already moved the
		// row back to starting/running.
		recheckCtx, recheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		marked, recheckErr := p.db.MarkDeadIfDraining(recheckCtx, streamID)
		recheckCancel()
		if recheckErr != nil {
			p.logger.Warn("finalize draining stream after pump exit",
				"stream", streamID, "err", recheckErr)
		} else if marked {
			p.notifyCapacityChange()
		}
	}
}

// finalizeDrainingIfPumpLess rechecks process-local ownership before completing
// a conditional draining->dead transition. It deliberately does not cancel an
// active pump: FinalizeIdleStreamWithoutPump can return false because an
// intervening client attached, not only because an earlier attempt already
// committed draining state.
func (p *Pool) finalizeDrainingIfPumpLess(channelID, id uuid.UUID) {
	// Bind the second local-map proof to pump creation for this channel. Without
	// this gate, an intervening client could install, fail, and begin physical
	// teardown between the map check and MarkDeadIfDraining.
	gateCtx, gateCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer gateCancel()
	roleLock := p.channelRoleLock(channelID)
	if err := roleLock.Lock(gateCtx); err != nil {
		p.logger.Warn("wait to finalize pump-less draining stream",
			"stream", id, "channel", channelID, "err", err)
		return
	}
	defer roleLock.Unlock()

	p.mu.Lock()
	_, active := p.streamers[id]
	exiting := len(p.exitingPumps[id]) > 0
	p.mu.Unlock()
	if active || exiting {
		return
	}
	markCtx, markCancel := context.WithTimeout(context.Background(), 5*time.Second)
	marked, err := p.db.MarkDeadIfDraining(markCtx, id)
	markCancel()
	if err != nil {
		p.logger.Warn("mark pump-less draining stream dead", "stream", id, "err", err)
		return
	}
	if marked {
		p.notifyCapacityChange()
	}
}

// CancelPumps cancels pump goroutines for rows transitioned to `draining` by
// final-client release or orphan detection. A tracked pump remains counted
// until OnExit; a draining ID absent from the singleton runtime's local map is
// proven pump-less and can become dead immediately. Dead rows returned only
// for physical deletion are harmless no-ops here.
func (p *Pool) CancelPumps(ids []uuid.UUID) {
	var cancels []context.CancelFunc
	var withoutPump []uuid.UUID
	p.mu.Lock()
	for _, id := range ids {
		if e, ok := p.streamers[id]; ok {
			if e.cancel != nil {
				cancels = append(cancels, e.cancel)
			}
			continue
		} else if exiting := p.exitingPumps[id]; len(exiting) > 0 {
			// OnStopping already cancelled/unpublished these generations. They
			// remain capacity owners until finishPump removes them after OnExit.
			continue
		} else {
			withoutPump = append(withoutPump, id)
		}
	}
	p.mu.Unlock()
	for _, c := range cancels {
		c()
	}
	// Under the runtime singleton, an ID absent from both the attachment map and
	// exiting-teardown registry cannot own a real pump. Complete a stale draining
	// transition immediately; tracked teardown wakes capacity only from OnExit.
	for _, id := range withoutPump {
		markCtx, markCancel := context.WithTimeout(context.Background(), 5*time.Second)
		marked, err := p.db.MarkDeadIfDraining(markCtx, id)
		markCancel()
		if err != nil {
			p.logger.Warn("mark pump-less draining stream dead", "stream", id, "err", err)
			continue
		}
		if marked {
			p.notifyCapacityChange()
		}
	}
}

// ReapHardStaleStreams completes the process-local half of hard zombie
// detection. The database heartbeat is only one signal: a matching pump with
// recent real-media activity is never cancelled merely because PostgreSQL
// heartbeat writes stalled. Under the runtime singleton, absence from both
// local pump registries proves that no provider connection remains and lets a
// generation-bound stale row become dead immediately.
func (p *Pool) ReapHardStaleStreams(
	ctx context.Context,
	candidates []store.HardStaleStreamCandidate,
	cutoff time.Time,
) int {
	reaped := 0
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			break
		}
		roleLock := p.channelRoleLock(candidate.ChannelID)
		if err := roleLock.Lock(ctx); err != nil {
			if ctx.Err() == nil {
				p.logger.Warn("wait to reap hard-stale stream",
					"stream", candidate.ID, "channel", candidate.ChannelID, "err", err)
			}
			continue
		}

		finalized := p.reapHardStaleStreamLocked(ctx, candidate, cutoff)
		roleLock.Unlock()
		if finalized {
			reaped++
		}
	}
	return reaped
}

// reapHardStaleStreamLocked requires the per-channel role lock. Keeping that
// lock through the generation CAS and local detach follows the same
// local-role-lock -> PostgreSQL advisory-lock order as pump creation and
// relocation, so a replacement generation cannot appear in the proof seam.
func (p *Pool) reapHardStaleStreamLocked(
	ctx context.Context,
	candidate store.HardStaleStreamCandidate,
	cutoff time.Time,
) bool {
	p.mu.Lock()
	entry, hasLocalPump := p.findPumpGenerationLocked(
		candidate.ID, candidate.PumpGeneration)
	p.mu.Unlock()

	// Atomically close real-media publication before the durable CAS. Holding a
	// media mutex across PostgreSQL can age a packet past the live-latency and
	// subscriber-stall budgets; releasing it after a rejected or ambiguous CAS
	// can instead splice old media into a recovered timeline. Once both stale
	// signals agree, fail closed and let Plex reconnect this exact generation.
	localCancelIssued := false
	if hasLocalPump {
		terminator, ok := entry.pump.(hardStaleActivityTerminator)
		if !ok {
			return false
		}
		if entry.cancel == nil {
			// Unknown cancellation ownership cannot promise eventual physical
			// teardown, so retain the durable row and fail closed.
			return false
		}
		if !terminator.tryTerminateHardStale(cutoff, entry.installedAt) {
			return false
		}
		// Cancellation is deliberately before database I/O. OnStopping detaches
		// the terminal pump and OnExit retains the physical teardown boundary;
		// a slow or outcome-ambiguous COMMIT cannot freeze a viewer and later
		// release stale bytes. The durable operation below is replay-safe.
		entry.cancel()
		localCancelIssued = true
	}

	finalize := p.hardStaleFinalizeHook
	if finalize == nil {
		finalize = p.db.FinalizeHardStaleStream
	}
	finalizeCtx, finalizeCancel := context.WithTimeout(ctx, hardStaleFinalizeTimeout)
	finalized, err := finalize(finalizeCtx, candidate, cutoff, hasLocalPump)
	finalizeCancel()
	if err != nil {
		if ctx.Err() == nil {
			p.logger.Warn("finalize hard-stale stream",
				"stream", candidate.ID,
				"generation", candidate.PumpGeneration,
				"err", err)
		}
		return false
	}
	if !finalized {
		return false
	}

	// The durable CAS consumed every exact token for this generation. Retire
	// its process-local role snapshot too; subscriber cleanup replays remain
	// safe because removeClientRole is idempotent.
	p.rolesMu.Lock()
	delete(p.clientRoles, candidate.ID)
	p.rolesMu.Unlock()

	if !hasLocalPump {
		p.notifyCapacityChange()
		return true
	}

	// The pump may have entered OnStopping/OnExit while the DB transaction ran.
	// Cancel only the exact generation from the candidate. If it already left
	// both maps, complete the generation-bound draining->dead transition now;
	// an old callback can never terminalize a replacement generation.
	var cancel context.CancelFunc
	p.mu.Lock()
	if active, ok := p.streamers[candidate.ID]; ok &&
		active.generation == candidate.PumpGeneration {
		cancel = p.trackExitingPumpLocked(candidate.ID, active)
	} else {
		for _, exiting := range p.exitingPumps[candidate.ID] {
			if exiting.generation == candidate.PumpGeneration {
				cancel = exiting.cancel
				break
			}
		}
	}
	p.mu.Unlock()
	if cancel != nil {
		if !localCancelIssued {
			cancel()
		}
		return true
	}

	markCtx, markCancel := context.WithTimeout(context.Background(), 5*time.Second)
	marked, markErr := p.db.MarkDeadAfterPumpExit(
		markCtx, candidate.ID, candidate.PumpGeneration)
	markCancel()
	if markErr != nil {
		p.logger.Warn("finalize disappeared hard-stale pump",
			"stream", candidate.ID,
			"generation", candidate.PumpGeneration,
			"err", markErr)
	} else if marked {
		p.notifyCapacityChange()
	}
	return true
}

// findPumpGenerationLocked searches both attachment-visible and physically
// exiting registries. p.mu must be held. A nil generation never identifies a
// local pump: real pumps claim a non-zero durable generation before install.
func (p *Pool) findPumpGenerationLocked(
	streamID, generation uuid.UUID,
) (pumpEntry, bool) {
	if generation == uuid.Nil {
		return pumpEntry{}, false
	}
	if entry, ok := p.streamers[streamID]; ok && entry.generation == generation {
		return entry, true
	}
	for _, entry := range p.exitingPumps[streamID] {
		if entry.generation == generation {
			return entry, true
		}
	}
	return pumpEntry{}, false
}

// storeProfileToTranscode maps the store row to the transcode package's view.
// Kept here (in stream) so neither store nor transcode needs to import the other.
func storeProfileToTranscode(p *store.TranscodeProfile) *transcode.Profile {
	if p == nil {
		return nil
	}
	return &transcode.Profile{
		ID: p.ID, Name: p.Name, Description: p.Description, Kind: transcode.Kind(p.Kind),
		VideoCodec: p.VideoCodec, VideoBitrateKbps: p.VideoBitrateKbps,
		VideoMaxBitrateKbps: p.VideoMaxBitrateKbps, VideoHeight: p.VideoHeight,
		VideoPreset: p.VideoPreset, VideoTune: p.VideoTune, VideoKeyint: p.VideoKeyint,
		Deinterlace: p.Deinterlace, HDRtoSDR: p.HDRtoSDR,
		AudioCodec: p.AudioCodec, AudioBitrateKbps: p.AudioBitrateKbps,
		AudioChannels: p.AudioChannels, AudioNormalize: p.AudioNormalize,
		UseNVENC: p.UseNVENC, UseNVDEC: p.UseNVDEC,
		FixTimestamps: p.FixTimestamps, LowLatency: p.LowLatency,
		DropSubtitles: p.DropSubtitles,
		FFmpegArgs:    p.FFmpegArgs, IsBuiltin: p.IsBuiltin, Enabled: p.Enabled,
	}
}

// Close stops accepting new streams and cancels every in-flight pump
// (their upstream connections tear down promptly instead of lingering
// until subscribers disconnect). DB rows are cleaned up by the orphan
// sweeper.
func (p *Pool) Close() {
	var cancels []context.CancelFunc
	p.mu.Lock()
	p.closed = true
	for _, e := range p.streamers {
		if e.cancel != nil {
			cancels = append(cancels, e.cancel)
		}
	}
	for _, exiting := range p.exitingPumps {
		for _, e := range exiting {
			if e.cancel != nil {
				cancels = append(cancels, e.cancel)
			}
		}
	}
	p.mu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// WaitForPumps waits until every pump Run method, including its OnExit
// teardown, and every bounded lease-cleanup retry has returned. Close and HTTP
// shutdown must happen first, preventing new work while this method waits. Runtime
// ownership stays held until this succeeds under the required stop → old PID
// exit → replacement start contract; a timeout hard-exits without voluntarily
// unlocking while a provider connection or ffmpeg process may still teardown.
func (p *Pool) WaitForPumps(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.pumpWG.Wait()
		p.cleanupWG.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (p *Pool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// PassthroughResolver leaves the URL template unchanged. Useful for plain
// M3U sources where the URL is already absolute and credential-baked.
type PassthroughResolver struct{}

func (PassthroughResolver) Resolve(_ context.Context, _ store.ProviderCredential, urlTemplate string) (string, error) {
	if urlTemplate == "" {
		return "", fmt.Errorf("upstream URL is empty")
	}
	return urlTemplate, nil
}
