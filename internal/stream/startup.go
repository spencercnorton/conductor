package stream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

const (
	// ClientStartupBudget is the end-to-end live/DVR tune budget. HTTP handler
	// channel lookup, lease acquisition, profile lookup, source attempts, and
	// readiness all consume this one absolute deadline; no layer resets it.
	ClientStartupBudget = 9 * time.Second

	// A complete first-source attempt plus the existing 250 ms failover
	// backoff leaves a second source another complete attempt and still fits
	// inside Plex's roughly ten-second tuner startup deadline.
	defaultSourceStartupTimeout = 4 * time.Second

	// Progress-leased startup extension: a source that is FLOWING real bytes
	// but not yet provably decodable (post-commercial-break garbage before
	// the next clean GOP) must not be killed and retried on the dead-source
	// budget. 2026-08-29 14:16Z: after a genuine 8.8s stall, four
	// consecutive ~5s reconnect attempts each read ~6.5 MB from Discovery's
	// origin and were cancelled mid media-prefix proof, stretching the stall
	// into a 21s outage — and the DVR validator then discarded the entire 2h
	// recording for the gap. A SILENT source still dies on the unmodified
	// initial budget: leases renew only on real byte flow.
	startupProgressWindow = 4 * time.Second  // lease granted per qualifying flow
	startupProgressFloor  = 256 * 1024       // bytes required to renew a lease
	maxStartupAttemptWall = 20 * time.Second // hard ceiling, extensions included

	// Only bounded, declared finite responses are held for classification.
	// The production placeholder is about 14 MiB. Ordinary live responses are
	// chunked and stay on the allocation-sensitive streaming path.
	minFinitePreflightBytes int64 = 1 << 20
	maxFinitePreflightBytes int64 = 32 << 20

	placeholderMinDuration = 9 * time.Minute
	placeholderMaxDuration = 11 * time.Minute

	placeholderSampleWidth   = 64
	placeholderSampleHeight  = 36
	placeholderFramesPerSeek = 3
	placeholderAudioRate     = 8000
	placeholderAudioWindow   = 250 * time.Millisecond

	// Decoding every declared track is required before silence can contribute
	// to a terminal placeholder verdict. Keep that work bounded by accepting
	// unusual topologies instead of guessing: the provider objects observed in
	// production use one or two tracks with no more than stereo audio.
	maxPlaceholderAudioStreams  = 4
	maxPlaceholderAudioChannels = 8

	// Each finite classification fans out six short ffmpeg seeks. Two
	// concurrent classifications preserve both real provider slots' startup
	// progress while bounding the process-wide subprocess fan-out at twelve.
	// Configurations with more provider capacity remain safe: extra classifiers
	// wait on their existing source-attempt context and never extend Plex's
	// absolute deadline. A wait that exhausts that context follows ordinary
	// credential-safe failed-source relocation without publishing finite bytes.
	maxConcurrentFiniteClassifications = 2

	classifierExecutionTimeoutReason = "classifier_execution_timeout"
)

var processFiniteClassificationSlots = make(chan struct{}, maxConcurrentFiniteClassifications)

type startupDeadlineContextKey struct{}

// WithStartupDeadline carries an absolute startup deadline without applying
// it to the eventual long-lived stream context. The API establishes it before
// channel lookup; Pool derives a temporary deadline context for startup work
// and then returns to the caller's ordinary lifetime context after readiness.
func WithStartupDeadline(ctx context.Context, deadline time.Time) context.Context {
	return context.WithValue(ctx, startupDeadlineContextKey{}, deadline)
}

func startupContext(ctx context.Context) (context.Context, context.CancelFunc, time.Time) {
	deadline := time.Now().Add(ClientStartupBudget)
	if carried, ok := ctx.Value(startupDeadlineContextKey{}).(time.Time); ok && carried.Before(deadline) {
		deadline = carried
	}
	startupCtx, cancel := context.WithDeadline(ctx, deadline)
	return startupCtx, cancel, deadline
}

// sourceAttemptBudget preserves the configured per-source ceiling while also
// consuming the one absolute client startup deadline. Once accepted media has
// arrived, later reconnects return to the ordinary per-source timeout.
func sourceAttemptBudget(configured time.Duration, startupDeadline time.Time, mediaReady bool) time.Duration {
	if configured <= 0 {
		configured = defaultSourceStartupTimeout
	}
	if mediaReady || startupDeadline.IsZero() {
		return configured
	}
	remaining := time.Until(startupDeadline)
	if remaining <= 0 {
		// newUpstreamAttempt treats non-positive as "use default"; retain the
		// expired deadline with the smallest useful positive duration instead.
		return time.Nanosecond
	}
	return min(configured, remaining)
}

// startupExtensionWall bounds progress-leased extensions: reconnect
// attempts get the full wall, while an initial tune must stay inside the
// one absolute client startup deadline.
func startupExtensionWall(startupDeadline time.Time, mediaReady bool) time.Duration {
	if mediaReady || startupDeadline.IsZero() {
		return maxStartupAttemptWall
	}
	remaining := time.Until(startupDeadline)
	if remaining <= 0 {
		return 0
	}
	return min(maxStartupAttemptWall, remaining)
}

var (
	// ErrPlaceholderMedia is returned only after every independent finite-TS
	// and decoded-frame signal agrees. It follows the normal failed-source
	// relocation path; its bytes are never published to a viewer or recorder.
	ErrPlaceholderMedia        = errors.New("finite all-black placeholder media")
	errSourceStartupTimeout    = errors.New("source startup deadline exceeded")
	errTranscodeStartupTimeout = errors.New("local transcode produced no output before startup deadline")
	errClassifierCapacity      = errors.New("local finite classifier capacity unavailable")
	errClassifierTimeout       = errors.New("local finite classifier execution exceeded source deadline")
)

type mediaClassification string

const (
	classificationNotChecked       mediaClassification = "not_checked"
	classificationFiniteAccepted   mediaClassification = "finite_non_placeholder"
	classificationIndeterminate    mediaClassification = "indeterminate"
	classificationBlackPlaceholder mediaClassification = "finite_black_placeholder"
)

type classificationResult struct {
	kind   mediaClassification
	reason string
}

type finiteMediaClassifier interface {
	Classify(ctx context.Context, data []byte) classificationResult
}

// sourceAttemptError deliberately omits the upstream URL and the wrapped
// error text. Provider URLs carry credentials in path/query components, and
// network errors commonly echo the whole URL. Unwrap preserves errors.Is
// behavior without exposing those values in logs or HTTP errors.
type sourceAttemptError struct {
	stage          string
	host           string
	classification mediaClassification
	reason         string
	cause          error
}

func (e *sourceAttemptError) Error() string {
	return fmt.Sprintf("source attempt failed: stage=%s host=%s classification=%s reason=%s",
		e.stage, e.host, e.classification, e.reason)
}

func (e *sourceAttemptError) Unwrap() error { return e.cause }

type attemptDiagnostics struct {
	host                  string
	ttfb                  time.Duration
	contentLength         int64
	classification        mediaClassification
	classReason           string
	bytes                 atomic.Int64
	lastByteUnixNS        atomic.Int64
	outputBytes           atomic.Int64
	readGaps              gapDiagnostics
	outputGaps            gapDiagnostics
	mediaPrefixBytes      int64
	mediaPrefixWait       time.Duration
	mediaPrefixReason     string
	inputTimeline         transportAVTimeline
	outputTimeline        transportAVTimeline
	audioOriginCorrection time.Duration
}

const diagnosticGapThreshold = time.Second

type gapDiagnostics struct {
	count      atomic.Uint64
	maxNS      atomic.Int64
	lastUnixNS atomic.Int64
}

func (g *gapDiagnostics) record(gap time.Duration) bool {
	return g.recordAt(gap, time.Now())
}

func (g *gapDiagnostics) recordAt(gap time.Duration, observedAt time.Time) bool {
	if gap < diagnosticGapThreshold {
		return false
	}
	g.count.Add(1)
	for old := g.maxNS.Load(); int64(gap) > old; old = g.maxNS.Load() {
		if g.maxNS.CompareAndSwap(old, int64(gap)) {
			break
		}
	}
	if !observedAt.IsZero() {
		g.lastUnixNS.Store(observedAt.UnixNano())
	}
	return true
}

func (g *gapDiagnostics) snapshot() (uint64, time.Duration) {
	return g.count.Load(), time.Duration(g.maxNS.Load())
}

func (g *gapDiagnostics) recent(now time.Time, within time.Duration) bool {
	last := g.lastUnixNS.Load()
	if last <= 0 || within < 0 {
		return false
	}
	age := now.Sub(time.Unix(0, last))
	return age >= 0 && age <= within
}

// upstreamAttempt owns one request, its hard startup timer, optional finite
// response replay, and its sanitized diagnostics. It implements io.Reader so
// buffered preflight bytes are not counted twice in the upstream byte total.
type upstreamAttempt struct {
	ctx    context.Context
	cancel context.CancelFunc
	timer  *time.Timer

	body        io.ReadCloser
	reader      io.Reader
	replay      bool
	contentType string

	diag       attemptDiagnostics
	logger     *slog.Logger
	streamID   string
	stage      string
	mediaReady atomic.Bool

	// Progress-leased startup extension (see the constants). timerMu makes
	// lease renewals race-safe against the markMediaReady/Close timer stop.
	// leaseWall <= 0 disables extensions entirely (the default).
	timerMu       sync.Mutex
	timerStopped  bool
	leaseArmed    atomic.Bool
	leaseStarted  time.Time
	leaseWall     time.Duration
	leaseWindow   time.Duration
	leaseFloor    int64
	leaseBytes    int64
	leaseRenewals int
	// leaseVeto, when set, can refuse a renewal at evaluation time. The
	// transcode path uses it for the "stdin fed but ffmpeg emitted nothing"
	// state, where the unmodified budget must classify the LOCAL fault
	// (transcodeNoOutputTimeout) instead of upstream flow deferring it.
	leaseVeto func() bool

	bodyCloseOnce sync.Once
	closeOnce     sync.Once
}

// allowStartupProgressExtension arms progress leasing up to wall of total
// attempt time. Call before the first Read; wall <= 0 leaves the fixed
// startup budget in force.
func (a *upstreamAttempt) allowStartupProgressExtension(wall time.Duration) {
	a.leaseWall = wall
	a.leaseWindow = startupProgressWindow
	a.leaseFloor = startupProgressFloor
}

// armStartupProgressLease activates renewals. Called only once open() has
// settled on the STREAMING path: the finite-preflight download and the
// classifier wait deliberately stay bounded by the unmodified startup timer
// (a wedged finite download or exhausted classifier capacity must fail at
// dead-source speed — TestClassifierCapacityTimeoutRelocates... pins this).
func (a *upstreamAttempt) armStartupProgressLease() {
	a.leaseArmed.Store(true)
}

// noteStartupProgress renews the startup timer while real bytes keep
// flowing, up to the armed wall. A silent source never renews, so the
// unmodified initial budget still fails it at dead-source speed.
func (a *upstreamAttempt) noteStartupProgress(n int) {
	if a.leaseWall <= 0 || n <= 0 || !a.leaseArmed.Load() || a.mediaReady.Load() {
		return
	}
	if a.leaseVeto != nil && a.leaseVeto() {
		return
	}
	a.leaseBytes += int64(n)
	if a.leaseBytes < a.leaseFloor {
		return
	}
	a.timerMu.Lock()
	defer a.timerMu.Unlock()
	if a.timerStopped || a.ctx.Err() != nil {
		return
	}
	remaining := a.leaseWall - time.Since(a.leaseStarted)
	if remaining <= 0 {
		return
	}
	a.leaseBytes = 0
	a.leaseRenewals++
	a.timer.Reset(min(a.leaseWindow, remaining))
}

func newUpstreamAttempt(parent context.Context, rawURL, streamID string, logger *slog.Logger, timeout time.Duration) *upstreamAttempt {
	if timeout <= 0 {
		timeout = defaultSourceStartupTimeout
	}
	ctx, cancel := context.WithCancel(parent)
	a := &upstreamAttempt{
		ctx:      ctx,
		cancel:   cancel,
		logger:   logger,
		streamID: streamID,
		stage:    "request",
	}
	a.diag.host = sanitizedHostname(rawURL)
	a.diag.contentLength = -1
	a.diag.classification = classificationNotChecked
	a.leaseStarted = time.Now()
	a.timer = time.AfterFunc(timeout, cancel)
	return a
}

func (a *upstreamAttempt) open(hc *http.Client, rawURL, userAgent string, classifier finiteMediaClassifier) error {
	started := time.Now()
	req, err := http.NewRequestWithContext(a.ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return a.wrap("request", err)
	}
	req.Header.Set("User-Agent", userAgent)

	a.stage = "headers"
	resp, err := hc.Do(req)
	a.diag.ttfb = time.Since(started)
	if err != nil {
		return a.wrap("headers", err)
	}
	a.body = resp.Body
	a.reader = resp.Body
	a.diag.contentLength = resp.ContentLength
	a.contentType = resp.Header.Get("Content-Type")
	if resp.Request != nil && resp.Request.URL != nil {
		if host := resp.Request.URL.Hostname(); host != "" {
			a.diag.host = host
		}
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return a.wrap("headers", &httpStatusError{Code: resp.StatusCode})
	}

	if resp.ContentLength < minFinitePreflightBytes || resp.ContentLength > maxFinitePreflightBytes {
		a.stage = "stream"
		a.armStartupProgressLease()
		return nil
	}

	// A finite candidate must be received completely and exactly before any
	// byte can reach the ring/fanout. The request context owns the body, so the
	// same four-second timer also unblocks a slow or wedged finite download.
	a.stage = "finite_read"
	data, readErr := io.ReadAll(io.LimitReader(a, resp.ContentLength+1))
	if readErr != nil {
		return a.wrap("finite_read", readErr)
	}
	if int64(len(data)) != resp.ContentLength {
		return a.wrap("finite_read", io.ErrUnexpectedEOF)
	}
	if a.ctx.Err() != nil {
		return a.wrap("finite_read", a.ctx.Err())
	}

	a.stage = "classify"
	result := classificationResult{kind: classificationIndeterminate, reason: "classifier_unavailable"}
	if classifier != nil {
		result = classifier.Classify(a.ctx, data)
	}
	a.diag.classification = result.kind
	a.diag.classReason = boundedDiagnostic(result.reason)
	if result.reason == "classifier_capacity_unavailable" {
		return a.wrap("classify", errClassifierCapacity)
	}
	if result.reason == classifierExecutionTimeoutReason {
		return a.wrap("classify", errClassifierTimeout)
	}
	if a.ctx.Err() != nil {
		// Once the complete finite body has entered local classification,
		// exhausting the attempt context is classifier execution time rather
		// than fresh provider evidence. Capacity has its own typed cause above;
		// every ordinary classifier result loses this cancellation race to the
		// health-neutral local timeout before bytes can be replayed.
		return a.wrap("classify", errClassifierTimeout)
	}
	if result.kind == classificationBlackPlaceholder {
		return a.wrap("classify", ErrPlaceholderMedia)
	}

	// The finite body is already at EOF. Replay the validated/declined bytes
	// from memory; Read must not count them a second time in diagnostics.
	a.closeBody()
	a.reader = bytes.NewReader(data)
	a.replay = true
	a.stage = "stream"
	return nil
}

func (a *upstreamAttempt) Read(p []byte) (int, error) {
	if a.reader == nil {
		return 0, io.EOF
	}
	started := time.Now()
	n, err := a.reader.Read(p)
	if n > 0 && !a.replay {
		now := time.Now()
		a.diag.inputTimeline.feedAt(p[:n], now)
		a.recordRead(n, now.Sub(started), now)
		a.noteStartupProgress(n)
	}
	if err != nil && a.ctx.Err() != nil {
		return n, a.ctx.Err()
	}
	return n, err
}

func (a *upstreamAttempt) recordRead(n int, wait time.Duration, now time.Time) {
	if n <= 0 {
		return
	}
	a.diag.bytes.Add(int64(n))
	a.diag.lastByteUnixNS.Store(now.UnixNano())
	if a.diag.readGaps.recordAt(wait, now) {
		metrics.UpstreamReadGaps.Inc()
		metrics.UpstreamReadGapMilliseconds.Add(uint64(wait / time.Millisecond))
	}
}

func (a *upstreamAttempt) recordOutput(n int, wait time.Duration) {
	if n <= 0 {
		return
	}
	a.diag.outputBytes.Add(int64(n))
	if a.diag.outputGaps.record(wait) {
		metrics.RelayOutputGaps.Inc()
		metrics.RelayOutputGapMilliseconds.Add(uint64(wait / time.Millisecond))
	}
}

func (a *upstreamAttempt) recordOutputData(data []byte, wait time.Duration) {
	if len(data) == 0 {
		return
	}
	a.diag.outputTimeline.feed(data)
	a.recordOutput(len(data), wait)
}

func (a *upstreamAttempt) recordMediaPrefix(gate *startupMediaGate) {
	if gate == nil {
		return
	}
	a.diag.mediaPrefixBytes = int64(gate.BufferedBytes())
	a.diag.mediaPrefixWait = gate.Wait()
	a.diag.mediaPrefixReason = boundedDiagnostic(gate.Reason())
}

// markMediaReady stops the hard startup deadline. A timer already firing is
// treated as a timeout, even if a byte arrived concurrently at the boundary.
func (a *upstreamAttempt) markMediaReady() error {
	a.timerMu.Lock()
	stopped := a.timer.Stop()
	a.timerStopped = true
	a.timerMu.Unlock()
	if a.ctx.Err() != nil || !stopped {
		return a.wrap("media", errSourceStartupTimeout)
	}
	a.mediaReady.Store(true)
	a.stage = "stream"
	return nil
}

func (a *upstreamAttempt) wrap(stage string, cause error) error {
	a.stage = stage
	if a.ctx.Err() != nil && !a.mediaReady.Load() &&
		!errors.Is(cause, ErrPlaceholderMedia) &&
		!errors.Is(cause, errTranscodeStartupTimeout) &&
		!errors.Is(cause, errUncertainTranscodeInput) &&
		!errors.Is(cause, errClassifierCapacity) &&
		!errors.Is(cause, errClassifierTimeout) &&
		!errors.Is(cause, errUncertainMediaPrefix) &&
		!errors.Is(cause, errUpstreamIdle) &&
		!errors.Is(cause, errRelayReadAheadFull) &&
		!errors.Is(cause, errTransportRecoveryBacklog) &&
		!errors.Is(cause, errTransportStaleChunk) &&
		!errors.Is(cause, errTransportRelayBacklog) &&
		!errors.Is(cause, errTransportAttemptBoundary) &&
		!errors.Is(cause, errTranscodeOutputIdle) {
		cause = errSourceStartupTimeout
	}
	return &sourceAttemptError{
		stage:          stage,
		host:           a.diag.host,
		classification: a.diag.classification,
		reason:         diagnosticReason(cause),
		cause:          cause,
	}
}

func (a *upstreamAttempt) finish(err error) {
	a.Close()
	if a.logger == nil {
		return
	}
	lastGap := "none"
	if ns := a.diag.lastByteUnixNS.Load(); ns > 0 {
		lastGap = time.Since(time.Unix(0, ns)).Round(time.Millisecond).String()
	}
	readGapCount, maxReadGap := a.diag.readGaps.snapshot()
	outputGapCount, maxOutputGap := a.diag.outputGaps.snapshot()
	inputTimeline := a.diag.inputTimeline.snapshot()
	outputTimeline := a.diag.outputTimeline.snapshot()
	recordAVTimelineMetrics(inputTimeline, outputTimeline)
	result := "ended"
	if err != nil {
		result = diagnosticReason(err)
	}
	attrs := []any{
		"stream", a.streamID,
		"effective_host", a.diag.host,
		"stage", a.stage,
		"ttfb", a.diag.ttfb.Round(time.Millisecond),
		"bytes", a.diag.bytes.Load(),
		"output_bytes", a.diag.outputBytes.Load(),
		"last_byte_gap", lastGap,
		"upstream_read_gaps", readGapCount,
		"upstream_max_read_gap", maxReadGap.Round(time.Millisecond),
		"relay_output_gaps", outputGapCount,
		"relay_max_output_gap", maxOutputGap.Round(time.Millisecond),
		"media_prefix_bytes", a.diag.mediaPrefixBytes,
		"media_prefix_wait", a.diag.mediaPrefixWait.Round(time.Millisecond),
		"media_prefix_result", a.diag.mediaPrefixReason,
		"input_av_timeline_valid", inputTimeline.valid,
		"input_video_pts_samples", inputTimeline.videoSamples,
		"input_audio_pts_samples", inputTimeline.audioSamples,
		"input_av_initial_skew", inputTimeline.initialSkew.Round(time.Millisecond),
		"input_av_final_skew", inputTimeline.finalSkew.Round(time.Millisecond),
		"input_av_drift", inputTimeline.drift.Round(time.Millisecond),
		"input_video_pts_gap", inputTimeline.videoLastGap.Round(time.Millisecond),
		"input_audio_pts_gap", inputTimeline.audioLastGap.Round(time.Millisecond),
		"input_audio_tracks", inputTimeline.audioTracks,
		"input_av_continuity_enforced", inputTimeline.continuityEnforced,
		"input_av_continuity_reason", inputTimeline.continuityReason,
		"input_transport_corruptions", inputTimeline.transportCorruptions,
		"input_audio_origin_correction", a.diag.audioOriginCorrection.Round(time.Millisecond),
		"output_av_timeline_valid", outputTimeline.valid,
		"output_video_pts_samples", outputTimeline.videoSamples,
		"output_audio_pts_samples", outputTimeline.audioSamples,
		"output_av_initial_skew", outputTimeline.initialSkew.Round(time.Millisecond),
		"output_av_final_skew", outputTimeline.finalSkew.Round(time.Millisecond),
		"output_av_drift", outputTimeline.drift.Round(time.Millisecond),
		"output_video_pts_gap", outputTimeline.videoLastGap.Round(time.Millisecond),
		"output_audio_pts_gap", outputTimeline.audioLastGap.Round(time.Millisecond),
		"output_audio_tracks", outputTimeline.audioTracks,
		"output_av_continuity_enforced", outputTimeline.continuityEnforced,
		"output_av_continuity_reason", outputTimeline.continuityReason,
		"output_transport_corruptions", outputTimeline.transportCorruptions,
		"content_length", a.diag.contentLength,
		"classification", a.diag.classification,
		"classification_reason", a.diag.classReason,
		"result", result,
	}
	if err == nil || errors.Is(err, context.Canceled) {
		a.logger.Info("upstream attempt diagnostic", attrs...)
	} else {
		a.logger.Warn("upstream attempt diagnostic", attrs...)
	}
}

func (a *upstreamAttempt) closeBody() {
	a.bodyCloseOnce.Do(func() {
		if a.body != nil {
			_ = a.body.Close()
		}
	})
}

func (a *upstreamAttempt) Close() {
	a.closeOnce.Do(func() {
		if a.timer != nil {
			a.timerMu.Lock()
			a.timer.Stop()
			a.timerStopped = true
			a.timerMu.Unlock()
		}
		a.closeBody()
		if a.cancel != nil {
			a.cancel()
		}
	})
}

func sanitizedHostname(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return "unknown"
	}
	return u.Hostname()
}

func diagnosticReason(err error) string {
	if err == nil {
		return "eof"
	}
	var attemptErr *sourceAttemptError
	if errors.As(err, &attemptErr) {
		return attemptErr.reason
	}
	var statusErr *httpStatusError
	if errors.As(err, &statusErr) {
		return "http_" + strconv.Itoa(statusErr.Code)
	}
	var continuityErr *avContinuityError
	if errors.As(err, &continuityErr) {
		return string(continuityErr.kind)
	}
	switch {
	case errors.Is(err, errOutputClockBoundary):
		return "output_clock_boundary"
	case errors.Is(err, ErrPlaceholderMedia):
		return "black_placeholder"
	case errors.Is(err, errTranscodeStartupTimeout):
		return "transcode_startup_timeout"
	case errors.Is(err, errTranscodeOutputIdle):
		return "transcode_output_idle"
	case errors.Is(err, errUncertainTranscodeInput):
		var refusal *audioOriginRefusalError
		if errors.As(err, &refusal) {
			return "transcode_input_uncertain:" + refusal.slug
		}
		return "transcode_input_uncertain"
	case errors.Is(err, errClassifierCapacity):
		return "classifier_capacity_unavailable"
	case errors.Is(err, errClassifierTimeout):
		return classifierExecutionTimeoutReason
	case errors.Is(err, errUncertainMediaPrefix):
		return "media_prefix_validation_uncertain"
	case errors.Is(err, errRelayReadAheadFull):
		return "relay_buffer_full"
	case errors.Is(err, errTransportRecoveryBacklog):
		return "relay_pacing_rate_backlog"
	case errors.Is(err, errTransportStaleChunk):
		return "relay_stale_chunk"
	case errors.Is(err, errTransportRelayBacklog):
		return "relay_pacing_backlog"
	case errors.Is(err, errTransportProgramMapStall):
		return "program_map_stall"
	case errors.Is(err, errTransportAttemptBoundary):
		return "transport_attempt_boundary"
	case errors.Is(err, errSourceStartupTimeout), errors.Is(err, context.DeadlineExceeded):
		return "startup_timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, errUpstreamIdle):
		return "idle_timeout"
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "unexpected_eof"
	default:
		return "io_error"
	}
}

func boundedDiagnostic(s string) string {
	const max = 80
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	if len(s) > max {
		return s[:max]
	}
	return s
}

// ffmpegFiniteMediaClassifier performs no network I/O. It writes the already
// bounded response to a private temporary file so ffprobe can confirm the
// container/duration and ffmpeg can seek to distributed decoded frames.
type ffmpegFiniteMediaClassifier struct {
	ffmpegBinary        string
	ffprobeBinary       string
	classificationSlots chan struct{}
}

func newFFmpegFiniteMediaClassifier(ffmpegBinary string) *ffmpegFiniteMediaClassifier {
	if ffmpegBinary == "" {
		ffmpegBinary = "ffmpeg"
	}
	return &ffmpegFiniteMediaClassifier{
		ffmpegBinary:        ffmpegBinary,
		ffprobeBinary:       companionFFprobe(ffmpegBinary),
		classificationSlots: processFiniteClassificationSlots,
	}
}

func companionFFprobe(ffmpegBinary string) string {
	if ffmpegBinary == "" || ffmpegBinary == "ffmpeg" {
		return "ffprobe"
	}
	base := filepath.Base(ffmpegBinary)
	if strings.HasSuffix(base, "ffmpeg") {
		candidate := filepath.Join(filepath.Dir(ffmpegBinary), strings.TrimSuffix(base, "ffmpeg")+"ffprobe")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "ffprobe"
}

type finiteProbe struct {
	formatName       string
	duration         time.Duration
	size             int64
	video            []int
	audio            []finiteAudioStream
	topologyComplete bool
}

type finiteAudioStream struct {
	index    int
	channels int
}

type decodedMediaSample struct {
	frames   []byte
	audioPCM [][]byte
}

func (c *ffmpegFiniteMediaClassifier) Classify(ctx context.Context, data []byte) classificationResult {
	slots := c.classificationSlots
	if slots == nil {
		// Preserve direct test construction and any future internal callers
		// while keeping every production classifier on the shared limiter.
		slots = processFiniteClassificationSlots
	}
	select {
	case slots <- struct{}{}:
		// Install the release only after a successful send. The process-wide
		// channel is never closed, so every return (including panic unwinding)
		// releases exactly one permit without a send-on-closed-channel path.
		defer func() { <-slots }()
	case <-ctx.Done():
		return classificationResult{classificationIndeterminate, "classifier_capacity_unavailable"}
	}

	f, err := os.CreateTemp("", "conductor-finite-preflight-*.ts")
	if err != nil {
		return classificationResult{classificationIndeterminate, "temp_create_failed"}
	}
	path := f.Name()
	defer os.Remove(path)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return classificationResult{classificationIndeterminate, "temp_permissions_failed"}
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return classificationResult{classificationIndeterminate, "temp_write_failed"}
	}
	if err := f.Close(); err != nil {
		return classificationResult{classificationIndeterminate, "temp_close_failed"}
	}

	probe, err := c.probe(ctx, path)
	if err != nil {
		if ctx.Err() != nil {
			return classificationResult{classificationIndeterminate, classifierExecutionTimeoutReason}
		}
		return classificationResult{classificationIndeterminate, "probe_failed"}
	}
	if !strings.Contains(probe.formatName, "mpegts") {
		return classificationResult{classificationFiniteAccepted, "not_mpegts"}
	}
	if len(probe.video) == 0 || probe.duration <= 0 {
		return classificationResult{classificationIndeterminate, "video_or_duration_missing"}
	}
	if probe.size != int64(len(data)) {
		return classificationResult{classificationIndeterminate, "probe_size_mismatch"}
	}
	if probe.duration < placeholderMinDuration || probe.duration > placeholderMaxDuration {
		return classificationResult{classificationFiniteAccepted, "duration_not_near_600s"}
	}
	if !finiteMediaTopologySupported(probe) {
		return classificationResult{classificationIndeterminate, "media_topology_unproven"}
	}
	samples, err := c.sampleDistributedMedia(ctx, path, probe.duration, probe.video[0], probe.audio)
	if err != nil {
		if ctx.Err() != nil {
			return classificationResult{classificationIndeterminate, classifierExecutionTimeoutReason}
		}
		return classificationResult{classificationIndeterminate, "media_decode_failed"}
	}
	if ctx.Err() != nil {
		return classificationResult{classificationIndeterminate, classifierExecutionTimeoutReason}
	}
	return classifyDecodedMedia(probe, int64(len(data)), samples)
}

func (c *ffmpegFiniteMediaClassifier) probe(ctx context.Context, path string) (finiteProbe, error) {
	cmd := exec.CommandContext(ctx, c.ffprobeBinary,
		"-v", "error",
		"-show_entries", "format=format_name,duration,size:stream=index,codec_type,channels",
		"-of", "json",
		path,
	)
	out, err := cmd.Output()
	if err != nil {
		return finiteProbe{}, err
	}
	return parseFiniteProbe(out)
}

func parseFiniteProbe(out []byte) (finiteProbe, error) {
	var raw struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
			Index     *int   `json:"index"`
			Channels  *int   `json:"channels"`
		} `json:"streams"`
		Format struct {
			FormatName string `json:"format_name"`
			Duration   string `json:"duration"`
			Size       string `json:"size"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return finiteProbe{}, err
	}
	durationSeconds, err := strconv.ParseFloat(raw.Format.Duration, 64)
	if err != nil || math.IsNaN(durationSeconds) || math.IsInf(durationSeconds, 0) {
		return finiteProbe{}, errors.New("duration unavailable")
	}
	size, err := strconv.ParseInt(raw.Format.Size, 10, 64)
	if err != nil {
		return finiteProbe{}, errors.New("size unavailable")
	}
	video := make([]int, 0, 1)
	var audio []finiteAudioStream
	topologyComplete := len(raw.Streams) > 0
	seen := make(map[int]struct{}, len(raw.Streams))
	for _, stream := range raw.Streams {
		index := -1
		if stream.Index != nil {
			index = *stream.Index
		}
		if index < 0 {
			topologyComplete = false
		} else if _, duplicate := seen[index]; duplicate {
			topologyComplete = false
		} else {
			seen[index] = struct{}{}
		}
		switch stream.CodecType {
		case "video":
			video = append(video, index)
		case "audio":
			channels := 0
			if stream.Channels != nil {
				channels = *stream.Channels
			}
			audio = append(audio, finiteAudioStream{index: index, channels: channels})
		default:
			// Passthrough publishes the whole transport. A subtitle, data,
			// attachment, or unknown stream may carry legitimate content that
			// this bounded A/V classifier did not sample, so it vetoes terminal
			// placeholder rejection.
			topologyComplete = false
		}
	}
	return finiteProbe{
		formatName:       raw.Format.FormatName,
		duration:         time.Duration(durationSeconds * float64(time.Second)),
		size:             size,
		video:            video,
		audio:            audio,
		topologyComplete: topologyComplete,
	}, nil
}

func finiteMediaTopologySupported(probe finiteProbe) bool {
	if !probe.topologyComplete || len(probe.video) != 1 || probe.video[0] < 0 ||
		!finiteAudioTopologySupported(probe.audio) {
		return false
	}
	for _, stream := range probe.audio {
		if stream.index == probe.video[0] {
			return false
		}
	}
	return true
}

func finiteAudioTopologySupported(streams []finiteAudioStream) bool {
	if len(streams) == 0 || len(streams) > maxPlaceholderAudioStreams {
		return false
	}
	seen := make(map[int]struct{}, len(streams))
	for _, stream := range streams {
		if stream.index < 0 || stream.channels < 1 ||
			stream.channels > maxPlaceholderAudioChannels {
			return false
		}
		if _, duplicate := seen[stream.index]; duplicate {
			return false
		}
		seen[stream.index] = struct{}{}
	}
	return true
}

func finiteAudioWindowBytes(channels int) int {
	samplesPerChannel := int(placeholderAudioWindow * placeholderAudioRate / time.Second)
	return samplesPerChannel * channels * 2
}

func finiteAudioWindowComplete(pcm []byte, channels int) bool {
	return channels >= 1 && channels <= maxPlaceholderAudioChannels &&
		len(pcm) == finiteAudioWindowBytes(channels)
}

// sampleDistributedMedia keeps the concurrent process fan-out at six per
// classification: each worker decodes its short video group, then the matching
// window from every bounded audio stream in sequence. Active programme audio
// is an independent veto against a placeholder verdict; a uniformly black
// ten-minute scene must not be treated as dead media merely because its
// pictures match the provider placeholder.
func (c *ffmpegFiniteMediaClassifier) sampleDistributedMedia(
	ctx context.Context,
	path string,
	duration time.Duration,
	videoIndex int,
	audioStreams []finiteAudioStream,
) ([]decodedMediaSample, error) {
	if videoIndex < 0 || !finiteAudioTopologySupported(audioStreams) {
		return nil, errors.New("media topology is outside the bounded classifier")
	}
	positions := []float64{0.05, 0.20, 0.40, 0.60, 0.80, 0.95}
	type sampleResult struct {
		index int
		data  decodedMediaSample
		err   error
	}
	results := make(chan sampleResult, len(positions))
	var wg sync.WaitGroup
	for i, fraction := range positions {
		at := time.Duration(float64(duration) * fraction)
		if at < time.Second {
			at = time.Second
		}
		if latest := duration - time.Second; at > latest {
			at = max(time.Duration(0), latest)
		}
		wg.Add(1)
		go func(index int, at time.Duration) {
			defer wg.Done()
			args := []string{
				"-v", "error",
				"-ss", strconv.FormatFloat(at.Seconds(), 'f', 3, 64),
				"-i", path,
				"-map", fmt.Sprintf("0:%d", videoIndex),
				"-frames:v", strconv.Itoa(placeholderFramesPerSeek),
				"-vf", fmt.Sprintf("scale=%d:%d:flags=area,format=gray", placeholderSampleWidth, placeholderSampleHeight),
				"-an", "-sn", "-dn", "-threads", "1",
				"-f", "rawvideo", "-pix_fmt", "gray", "pipe:1",
			}
			frames, err := exec.CommandContext(ctx, c.ffmpegBinary, args...).Output()
			if err != nil {
				results <- sampleResult{index: index, err: err}
				return
			}
			audioPCM := make([][]byte, len(audioStreams))
			for audioIndex, stream := range audioStreams {
				pcm, audioErr := exec.CommandContext(ctx, c.ffmpegBinary,
					"-v", "error",
					"-ss", strconv.FormatFloat(at.Seconds(), 'f', 3, 64),
					"-i", path,
					"-map", fmt.Sprintf("0:%d", stream.index),
					"-t", strconv.FormatFloat(placeholderAudioWindow.Seconds(), 'f', 3, 64),
					"-vn", "-sn", "-dn", "-threads", "1",
					"-ar", strconv.Itoa(placeholderAudioRate),
					"-f", "s16le", "pipe:1",
				).Output()
				if audioErr != nil {
					results <- sampleResult{index: index, err: audioErr}
					return
				}
				audioPCM[audioIndex] = pcm
			}
			results <- sampleResult{index: index, data: decodedMediaSample{
				frames:   frames,
				audioPCM: audioPCM,
			}}
		}(i, at)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	samples := make([]decodedMediaSample, len(positions))
	var firstErr error
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		samples[result.index] = result.data
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return samples, nil
}

func classifyDecodedMedia(
	probe finiteProbe,
	declaredBytes int64,
	samples []decodedMediaSample,
) classificationResult {
	if !strings.Contains(probe.formatName, "mpegts") {
		return classificationResult{classificationFiniteAccepted, "not_mpegts_video"}
	}
	if probe.size <= 0 || declaredBytes <= 0 || probe.size != declaredBytes {
		return classificationResult{classificationIndeterminate, "finite_size_signal_missing"}
	}
	if probe.duration < placeholderMinDuration || probe.duration > placeholderMaxDuration {
		return classificationResult{classificationFiniteAccepted, "duration_not_near_600s"}
	}
	if !finiteMediaTopologySupported(probe) {
		return classificationResult{classificationIndeterminate, "media_topology_unproven"}
	}
	if len(samples) != 6 {
		return classificationResult{classificationIndeterminate, "distributed_samples_missing"}
	}
	frameSize := placeholderSampleWidth * placeholderSampleHeight
	var firstHash [sha256.Size]byte
	haveHash := false
	for _, sample := range samples {
		if len(sample.frames) != frameSize*placeholderFramesPerSeek {
			return classificationResult{classificationIndeterminate, "decoded_frame_count_mismatch"}
		}
		for offset := 0; offset < len(sample.frames); offset += frameSize {
			frame := sample.frames[offset : offset+frameSize]
			if !nearBlackFrame(frame) {
				return classificationResult{classificationFiniteAccepted, "decoded_frame_not_black"}
			}
			hash := sha256.Sum256(frame)
			if !haveHash {
				firstHash = hash
				haveHash = true
			} else if hash != firstHash {
				return classificationResult{classificationFiniteAccepted, "decoded_frames_not_identical"}
			}
		}
	}
	if !haveHash {
		return classificationResult{classificationIndeterminate, "decoded_frames_missing"}
	}
	for _, sample := range samples {
		if len(sample.audioPCM) != len(probe.audio) {
			return classificationResult{classificationIndeterminate, "decoded_audio_streams_missing"}
		}
		for audioIndex, stream := range probe.audio {
			pcm := sample.audioPCM[audioIndex]
			if !finiteAudioWindowComplete(pcm, stream.channels) {
				return classificationResult{classificationIndeterminate, "decoded_audio_sample_size_mismatch"}
			}
			if !nearSilentPCM16(pcm) {
				return classificationResult{classificationFiniteAccepted, "decoded_audio_not_silent"}
			}
		}
	}
	return classificationResult{classificationBlackPlaceholder, "finite_mpegts_600s_uniform_identical_black_and_silent"}
}

func nearBlackFrame(frame []byte) bool {
	if len(frame) == 0 {
		return false
	}
	minY, maxY := byte(255), byte(0)
	var sum uint64
	for _, y := range frame {
		if y < minY {
			minY = y
		}
		if y > maxY {
			maxY = y
		}
		sum += uint64(y)
	}
	mean := float64(sum) / float64(len(frame))
	return maxY <= 4 && maxY-minY <= 4 && mean <= 2
}

func nearSilentPCM16(pcm []byte) bool {
	if len(pcm) < 2 || len(pcm)%2 != 0 {
		return false
	}
	var sumSquares float64
	var peak int32
	for offset := 0; offset < len(pcm); offset += 2 {
		sample := int32(int16(binary.LittleEndian.Uint16(pcm[offset : offset+2])))
		magnitude := sample
		if magnitude < 0 {
			magnitude = -magnitude
		}
		peak = max(peak, magnitude)
		sumSquares += float64(sample * sample)
	}
	rms := math.Sqrt(sumSquares / float64(len(pcm)/2))
	return peak <= 500 && rms <= 100
}
