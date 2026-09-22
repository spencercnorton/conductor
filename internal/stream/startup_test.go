package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/transcode"
)

type staticFiniteClassifier struct {
	result classificationResult
	calls  atomic.Int32
}

type blockingFiniteClassifier struct {
	calls atomic.Int32
}

type expiredContextResultClassifier struct {
	result classificationResult
	calls  atomic.Int32
}

func TestGapDiagnosticsThresholdCountAndMax(t *testing.T) {
	var gaps gapDiagnostics

	if observed := gaps.record(0); observed {
		t.Fatal("zero-duration read was counted as a gap")
	}
	if observed := gaps.record(diagnosticGapThreshold - time.Millisecond); observed {
		t.Fatal("sub-threshold read was counted as a gap")
	}
	if count, maxGap := gaps.snapshot(); count != 0 || maxGap != 0 {
		t.Fatalf("snapshot after ignored gap = (%d, %v), want (0, 0)", count, maxGap)
	}

	if observed := gaps.record(1250 * time.Millisecond); !observed {
		t.Fatal("1.25s blocking read was not counted")
	}
	if observed := gaps.record(1100 * time.Millisecond); !observed {
		t.Fatal("1.1s blocking read was not counted")
	}
	if observed := gaps.record(250 * time.Millisecond); observed {
		t.Fatal("final 250ms read was counted as a gap")
	}

	if count, maxGap := gaps.snapshot(); count != 2 || maxGap != 1250*time.Millisecond {
		t.Fatalf("final snapshot = (%d, %v), want (2, 1.25s)", count, maxGap)
	}
}

func TestGapDiagnosticsRecentRequiresBoundedNonnegativeAge(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var gaps gapDiagnostics
	if gaps.recent(now, time.Second) {
		t.Fatal("empty diagnostics reported a recent gap")
	}
	if gaps.recordAt(diagnosticGapThreshold-time.Millisecond, now) {
		t.Fatal("sub-threshold gap was recorded")
	}
	if gaps.recent(now, time.Second) {
		t.Fatal("sub-threshold gap supplied attribution evidence")
	}
	if !gaps.recordAt(diagnosticGapThreshold, now.Add(-time.Second)) {
		t.Fatal("threshold gap was not recorded")
	}
	if !gaps.recent(now, time.Second) {
		t.Fatal("boundary-age gap was not recent")
	}
	if gaps.recent(now.Add(time.Nanosecond), time.Second) {
		t.Fatal("stale gap remained recent")
	}
	if gaps.recent(now.Add(-2*time.Second), time.Second) {
		t.Fatal("future observation supplied attribution evidence")
	}
}

func TestAttemptWrapPreservesPacingEvidenceAtStartupDeadline(t *testing.T) {
	for _, cause := range []error{
		errTransportRecoveryBacklog,
		errTransportStaleChunk,
		errTransportRelayBacklog,
		errTransportAttemptBoundary,
		errClassifierTimeout,
	} {
		t.Run(diagnosticReason(cause), func(t *testing.T) {
			attempt := newUpstreamAttempt(context.Background(), "http://provider.test/live",
				"pacing-deadline", testLogger(), time.Minute)
			attempt.cancel()
			defer attempt.Close()
			err := attempt.wrap("transport_pace", cause)
			if !errors.Is(err, cause) {
				t.Fatalf("startup race rewrote %v as %v", cause, err)
			}
			if errors.Is(err, errSourceStartupTimeout) {
				t.Fatalf("startup race collapsed %v into source timeout: %v", cause, err)
			}
		})
	}
}

func TestAttemptGapMetricsCountAndTotalMilliseconds(t *testing.T) {
	upstreamCountBefore := metrics.UpstreamReadGaps.Value()
	upstreamTotalBefore := metrics.UpstreamReadGapMilliseconds.Value()

	attempt := &upstreamAttempt{ctx: context.Background()}
	attempt.recordRead(1, 1500*time.Millisecond, time.Now())
	readCount, maxReadGap := attempt.diag.readGaps.snapshot()
	if readCount != 1 || maxReadGap < diagnosticGapThreshold {
		t.Fatalf("upstream diagnostics = (%d, %v), want one gap >= %v", readCount, maxReadGap, diagnosticGapThreshold)
	}
	if got := metrics.UpstreamReadGaps.Value() - upstreamCountBefore; got != 1 {
		t.Fatalf("upstream gap counter delta = %d, want 1", got)
	}
	if got, want := metrics.UpstreamReadGapMilliseconds.Value()-upstreamTotalBefore, uint64(maxReadGap/time.Millisecond); got != want {
		t.Fatalf("upstream gap millisecond delta = %d, want %d", got, want)
	}

	// A normal follow-up read updates byte diagnostics but contributes no new
	// gap sample or elapsed time.
	attempt.recordRead(1, 250*time.Millisecond, time.Now())
	if got := metrics.UpstreamReadGaps.Value() - upstreamCountBefore; got != 1 {
		t.Fatalf("upstream gap counter after normal read = %d, want 1", got)
	}
	if got, want := metrics.UpstreamReadGapMilliseconds.Value()-upstreamTotalBefore, uint64(maxReadGap/time.Millisecond); got != want {
		t.Fatalf("upstream gap total after normal read = %d, want %d", got, want)
	}

	relayCountBefore := metrics.RelayOutputGaps.Value()
	relayTotalBefore := metrics.RelayOutputGapMilliseconds.Value()
	attempt.recordOutput(1, 0)
	attempt.recordOutput(1, 999*time.Millisecond)
	attempt.recordOutput(1, 1201*time.Millisecond)
	attempt.recordOutput(1, 1250*time.Millisecond)

	outputCount, maxOutputGap := attempt.diag.outputGaps.snapshot()
	if outputCount != 2 || maxOutputGap != 1250*time.Millisecond {
		t.Fatalf("relay diagnostics = (%d, %v), want (2, 1.25s)", outputCount, maxOutputGap)
	}
	if got := metrics.RelayOutputGaps.Value() - relayCountBefore; got != 2 {
		t.Fatalf("relay gap counter delta = %d, want 2", got)
	}
	if got, want := metrics.RelayOutputGapMilliseconds.Value()-relayTotalBefore, uint64(1201+1250); got != want {
		t.Fatalf("relay gap millisecond delta = %d, want %d", got, want)
	}
}

// A relocation whose COMMIT outcome cannot be proven is an operational
// startup failure, not evidence that the provider is dead. Both pump types
// must retain the typed marker before the first response byte while stripping
// the raw database cause from the API-facing error.
func TestInitialRelocationAmbiguityPreservesSafeMarker(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusBadGateway)
	}))
	defer dead.Close()

	tests := []struct {
		name string
		new  func() streamPump
	}{
		{
			name: "passthrough",
			new: func() streamPump {
				s := NewStreamer("relocation-unknown-pass", dead.URL, testLogger(), nil)
				s.OnUpstreamDown = relocationUnknownTestCallback
				return s
			},
		},
		{
			name: "transcode",
			new: func() streamPump {
				s := NewTranscodeStreamer("relocation-unknown-transcode", dead.URL,
					stubProfile(), "unused-ffmpeg", testLogger(), nil)
				s.OnUpstreamDown = relocationUnknownTestCallback
				return s
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pump := tt.new()
			firstReal, _, _, unsubscribe := pump.SubscribeForStartup("viewer")
			defer unsubscribe()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() {
				pump.Run(ctx)
				close(done)
			}()

			_, err := waitForSubscriberMedia(ctx, pump, firstReal)
			if !errors.Is(err, ErrUpstreamNotReady) ||
				!errors.Is(err, store.ErrLeaseOperational) ||
				!errors.Is(err, store.ErrRelocationStateUnknown) {
				t.Fatalf("startup error=%v, want readiness + operational + relocation markers", err)
			}
			if strings.Contains(err.Error(), "credential-secret") {
				t.Fatalf("startup error leaked raw relocation cause: %v", err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("pump did not stop after relocation ambiguity")
			}
			if terminal := pump.TerminalError(); shouldMarkSourceFailure(terminal) {
				t.Fatalf("%s pump attributed relocation ambiguity to source health: %v",
					tt.name, terminal)
			}
		})
	}
}

func relocationUnknownTestCallback(context.Context, error) (string, bool, error) {
	return "", false, errors.Join(store.ErrLeaseOperational,
		fmt.Errorf("%w: credential-secret", store.ErrRelocationStateUnknown))
}

func TestStoppedPumpStartupRetryPolicy(t *testing.T) {
	relocationUnknown := errors.Join(ErrUpstreamNotReady,
		errPumpStoppedBeforeMedia, store.ErrLeaseOperational,
		store.ErrRelocationStateUnknown)
	ordinaryStop := errors.Join(ErrUpstreamNotReady, errPumpStoppedBeforeMedia)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "existing terminal pump", err: ordinaryStop, want: true},
		{name: "created ordinary exhaustion", err: ordinaryStop, want: true},
		{name: "created relocation handoff", err: relocationUnknown, want: true},
		{name: "attempt epoch changed", err: errPumpDiscontinuityBeforeMedia, want: true},
		{name: "unrelated failure", err: store.ErrLeaseOperational, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryStartupAttachment(tt.err); got != tt.want {
				t.Fatalf("retryStartupAttachment(err=%v)=%v, want %v",
					tt.err, got, tt.want)
			}
		})
	}
}

func TestStoppingVetoAppliesToNewPumpFirstMedia(t *testing.T) {
	s := NewStreamer("creator-stopped-after-first", "http://unused.test", testLogger(), nil)
	s.markStopping() // firstReal became ready, then terminal decision won.
	if !startupMediaInvalidatedByStop(s, nil) {
		t.Fatal("newly created stopping pump was allowed to commit first media")
	}
	if startupMediaInvalidatedByStop(s, ErrUpstreamNotReady) {
		t.Fatal("pre-existing wait failure was misclassified as post-media stop")
	}
}

func (c *staticFiniteClassifier) Classify(context.Context, []byte) classificationResult {
	c.calls.Add(1)
	return c.result
}

func (c *blockingFiniteClassifier) Classify(ctx context.Context, _ []byte) classificationResult {
	c.calls.Add(1)
	<-ctx.Done()
	return classificationResult{classificationIndeterminate, classifierExecutionTimeoutReason}
}

func (c *expiredContextResultClassifier) Classify(
	ctx context.Context,
	_ []byte,
) classificationResult {
	c.calls.Add(1)
	<-ctx.Done()
	return c.result
}

func markerStreamServer(t *testing.T, marker byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		chunk := bytes.Repeat([]byte{marker}, 188*10)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
}

// Source one consumes its complete default four-second request/read/media
// budget after returning HTTP 200 with no media. Source two must still be
// attempted and produce validated bytes with clear margin inside Plex's
// observed ten-second tuner deadline.
func TestStartupTimeoutTriesAlternateBeforePlexDeadline(t *testing.T) {
	var sourceOneRequests atomic.Int32
	sourceOneCanceled := make(chan struct{})
	var canceledOnce sync.Once
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceOneRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		canceledOnce.Do(func() { close(sourceOneCanceled) })
	}))
	defer sourceOne.Close()
	sourceTwo := markerStreamServer(t, 0xbc)
	defer sourceTwo.Close()

	s := NewStreamer("startup-deadline", sourceOne.URL, testLogger(), nil)
	var failovers atomic.Int32
	var running atomic.Int32
	s.OnRunning = func() { running.Add(1) }
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if !errors.Is(cause, errSourceStartupTimeout) {
			t.Errorf("source-one cause = %v, want startup timeout", cause)
		}
		failovers.Add(1)
		return sourceTwo.URL, true, nil
	}

	chunks, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	started := time.Now()
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case chunk, ok := <-chunks:
		if !ok {
			t.Fatal("subscriber closed before alternate source produced media")
		}
		if len(chunk) == 0 || chunk[0] != 0xbc {
			t.Fatalf("first published media came from wrong source: %x", chunk[:min(len(chunk), 8)])
		}
	case <-time.After(9500 * time.Millisecond):
		t.Fatal("alternate source did not become ready before Plex deadline")
	}
	elapsed := time.Since(started)
	if elapsed >= 9*time.Second {
		t.Fatalf("source-one timeout to source-two media took %v, want <9s (<10s Plex deadline)", elapsed)
	}
	if !s.MediaReady() {
		t.Fatal("MediaReady false after alternate media was published")
	}
	if sourceOneRequests.Load() != 1 || failovers.Load() != 1 {
		t.Fatalf("source-one requests=%d failovers=%d, want 1/1", sourceOneRequests.Load(), failovers.Load())
	}
	if running.Load() != 1 {
		t.Fatalf("OnRunning calls=%d, want only source two", running.Load())
	}
	select {
	case <-sourceOneCanceled:
	case <-time.After(time.Second):
		t.Fatal("source-one request/body was not canceled before alternate delivery")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streamer did not exit after cancellation")
	}
}

func finiteResponseServer(t *testing.T, fill byte) *httptest.Server {
	t.Helper()
	data := bytes.Repeat([]byte{fill}, int(minFinitePreflightBytes))
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	}))
}

func TestClassifiedPlaceholderNeverReachesFanoutAndRelocates(t *testing.T) {
	placeholder := finiteResponseServer(t, 0xaa)
	defer placeholder.Close()
	healthy := markerStreamServer(t, 0xbc)
	defer healthy.Close()

	classifier := &staticFiniteClassifier{result: classificationResult{
		kind: classificationBlackPlaceholder, reason: "test_high_confidence",
	}}
	s := NewStreamer("placeholder-relocate", placeholder.URL, testLogger(), nil)
	s.classifier = classifier
	s.sourceStartupTimeout = 2 * time.Second
	var running atomic.Int32
	s.OnRunning = func() { running.Add(1) }
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if !errors.Is(cause, ErrPlaceholderMedia) {
			t.Errorf("placeholder cause = %v, want ErrPlaceholderMedia", cause)
		}
		return healthy.URL, true, nil
	}

	chunks, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case chunk, ok := <-chunks:
		if !ok {
			t.Fatal("subscriber closed before alternate media")
		}
		if len(chunk) == 0 || chunk[0] != 0xbc {
			t.Fatalf("classified placeholder leaked to fanout: %x", chunk[:min(len(chunk), 8)])
		}
	case <-time.After(4 * time.Second):
		t.Fatal("healthy alternate did not produce media")
	}
	if classifier.calls.Load() != 1 {
		t.Fatalf("classifier calls=%d, want 1", classifier.calls.Load())
	}
	if running.Load() != 1 {
		t.Fatalf("OnRunning calls=%d, placeholder must never mark running", running.Load())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streamer did not exit after cancellation")
	}
}

func TestTranscodeRejectsPlaceholderBeforeStartingFFmpeg(t *testing.T) {
	placeholder := finiteResponseServer(t, 0xaa)
	defer placeholder.Close()
	classifier := &staticFiniteClassifier{result: classificationResult{
		kind: classificationBlackPlaceholder, reason: "test_high_confidence",
	}}
	profile := &transcode.Profile{Name: "must-not-start", Kind: transcode.KindCPUTranscode}
	s := NewTranscodeStreamer("transcode-placeholder", placeholder.URL, profile,
		filepath.Join(t.TempDir(), "missing-ffmpeg"), testLogger(), nil)
	s.classifier = classifier
	s.sourceStartupTimeout = 2 * time.Second
	s.OnUpstreamDown = func(context.Context, error) (string, bool, error) { return "", false, nil }

	chunks, unsubscribe := s.Subscribe("recorder-or-viewer")
	defer unsubscribe()
	var exitErr error
	exited := make(chan struct{})
	s.OnExit = func(err error) {
		exitErr = err
		close(exited)
	}
	go s.Run(context.Background())

	select {
	case <-exited:
	case <-time.After(4 * time.Second):
		t.Fatal("transcode pump did not reject placeholder")
	}
	if !errors.Is(exitErr, ErrPlaceholderMedia) {
		t.Fatalf("exit error=%v, want ErrPlaceholderMedia (ffmpeg must not start)", exitErr)
	}
	if s.MediaReady() {
		t.Fatal("classified placeholder marked transcode pump ready")
	}
	select {
	case chunk, ok := <-chunks:
		if ok || len(chunk) != 0 {
			t.Fatalf("classified transcode placeholder published %d bytes", len(chunk))
		}
	default:
		t.Fatal("subscriber channel was not closed after placeholder rejection")
	}
}

func uniformSamples(value byte) []decodedMediaSample {
	frameSize := placeholderSampleWidth * placeholderSampleHeight
	samples := make([]decodedMediaSample, 6)
	for i := range samples {
		samples[i].frames = bytes.Repeat([]byte{value}, frameSize*placeholderFramesPerSeek)
	}
	return samples
}

func cloneSamples(in []decodedMediaSample) []decodedMediaSample {
	out := make([]decodedMediaSample, len(in))
	for i := range in {
		out[i].frames = append([]byte(nil), in[i].frames...)
		out[i].audioPCM = make([][]byte, len(in[i].audioPCM))
		for audioIndex := range in[i].audioPCM {
			out[i].audioPCM[audioIndex] = append([]byte(nil), in[i].audioPCM[audioIndex]...)
		}
	}
	return out
}

func silentSamplesForProbe(probe finiteProbe) []decodedMediaSample {
	samples := uniformSamples(0)
	for i := range samples {
		samples[i].audioPCM = make([][]byte, len(probe.audio))
		for audioIndex, stream := range probe.audio {
			if stream.channels > 0 && stream.channels <= maxPlaceholderAudioChannels {
				samples[i].audioPCM[audioIndex] = make([]byte,
					finiteAudioWindowBytes(stream.channels))
			}
		}
	}
	return samples
}

func TestFiniteClassifierTopologyAndPCMProofBoundaries(t *testing.T) {
	const declared = int64(14_472_616)
	base := finiteProbe{
		formatName:       "mpegts",
		duration:         600*time.Second + 46*time.Millisecond,
		size:             declared,
		video:            []int{3},
		audio:            []finiteAudioStream{{index: 7, channels: 2}},
		topologyComplete: true,
	}
	run := func(name string, probe finiteProbe, samples []decodedMediaSample,
		want mediaClassification, wantReason string,
	) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			got := classifyDecodedMedia(probe, declared, samples)
			if got.kind != want || (wantReason != "" && got.reason != wantReason) {
				t.Fatalf("classification=%s (%s), want %s (%s)",
					got.kind, got.reason, want, wantReason)
			}
		})
	}

	run("exact bounded topology rejects placeholder", base, silentSamplesForProbe(base),
		classificationBlackPlaceholder, "")

	fourAudio := base
	fourAudio.audio = []finiteAudioStream{
		{index: 7, channels: 1}, {index: 8, channels: 2},
		{index: 9, channels: 6}, {index: 10, channels: 8},
	}
	run("exactly four audio streams are proved", fourAudio, silentSamplesForProbe(fourAudio),
		classificationBlackPlaceholder, "")
	fiveAudio := fourAudio
	fiveAudio.audio = append(append([]finiteAudioStream(nil), fourAudio.audio...),
		finiteAudioStream{index: 11, channels: 2})
	run("five audio streams fail open", fiveAudio, uniformSamples(0),
		classificationIndeterminate, "media_topology_unproven")

	for _, tt := range []struct {
		name  string
		probe finiteProbe
	}{
		{name: "missing video", probe: func() finiteProbe {
			p := base
			p.video = nil
			return p
		}()},
		{name: "negative video index", probe: func() finiteProbe {
			p := base
			p.video = []int{-1}
			return p
		}()},
		{name: "multiple video streams", probe: func() finiteProbe {
			p := base
			p.video = []int{3, 4}
			return p
		}()},
		{name: "unproved subtitle or data stream", probe: func() finiteProbe {
			p := base
			p.topologyComplete = false
			return p
		}()},
		{name: "duplicate absolute stream index", probe: func() finiteProbe {
			p := base
			p.audio = []finiteAudioStream{{index: 3, channels: 2}}
			return p
		}()},
		{name: "negative audio index", probe: func() finiteProbe {
			p := base
			p.audio = []finiteAudioStream{{index: -1, channels: 2}}
			return p
		}()},
		{name: "zero audio channels", probe: func() finiteProbe {
			p := base
			p.audio = []finiteAudioStream{{index: 7, channels: 0}}
			return p
		}()},
		{name: "more than eight audio channels", probe: func() finiteProbe {
			p := base
			p.audio = []finiteAudioStream{{index: 7, channels: 9}}
			return p
		}()},
	} {
		run(tt.name, tt.probe, uniformSamples(0),
			classificationIndeterminate, "media_topology_unproven")
	}

	wantPCM := finiteAudioWindowBytes(base.audio[0].channels)
	for _, tt := range []struct {
		name string
		size int
	}{
		{name: "short even PCM window", size: wantPCM - 2},
		{name: "short odd PCM window", size: wantPCM - 1},
		{name: "overlong PCM window", size: wantPCM + 2},
	} {
		samples := silentSamplesForProbe(base)
		samples[2].audioPCM[0] = make([]byte, tt.size)
		run(tt.name, base, samples,
			classificationIndeterminate, "decoded_audio_sample_size_mismatch")
	}
}

func TestFiniteProbeRequiresFullyProvedPublishedTopology(t *testing.T) {
	const (
		format     = `"format":{"format_name":"mpegts","duration":"600.046","size":"14472616"}`
		validVideo = `{"index":3,"codec_type":"video"}`
		validAudio = `{"index":7,"codec_type":"audio","channels":2}`
	)
	tests := []struct {
		name      string
		streams   string
		supported bool
	}{
		{name: "one absolute video and audio", streams: validVideo + "," + validAudio, supported: true},
		{name: "black primary plus unproved secondary video", streams: validVideo + `,{"index":4,"codec_type":"video"},` + validAudio},
		{name: "subtitle stream", streams: validVideo + "," + validAudio + `,{"index":8,"codec_type":"subtitle"}`},
		{name: "data stream", streams: validVideo + "," + validAudio + `,{"index":8,"codec_type":"data"}`},
		{name: "unknown stream", streams: validVideo + "," + validAudio + `,{"index":8,"codec_type":""}`},
		{name: "missing absolute index", streams: `{"codec_type":"video"},` + validAudio},
		{name: "duplicate absolute index", streams: validVideo + `,{"index":3,"codec_type":"audio","channels":2}`},
		{name: "missing channels", streams: validVideo + `,{"index":7,"codec_type":"audio"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe, err := parseFiniteProbe([]byte(`{"streams":[` + tt.streams + `],` + format + `}`))
			if err != nil {
				t.Fatal(err)
			}
			if got := finiteMediaTopologySupported(probe); got != tt.supported {
				t.Fatalf("topology=%+v supported=%v, want %v", probe, got, tt.supported)
			}
		})
	}
}

func TestHighConfidencePlaceholderConjunctionAndFalsePositiveControls(t *testing.T) {
	const declared = int64(14_472_616)
	validProbe := finiteProbe{
		formatName: "mpegts", duration: 600*time.Second + 46*time.Millisecond,
		size: declared, video: []int{0}, topologyComplete: true,
	}
	black := uniformSamples(0)
	audioProbe := validProbe
	audioProbe.audio = []finiteAudioStream{{index: 1, channels: 1}}
	silentAudio := silentSamplesForProbe(audioProbe)
	darkScene := cloneSamples(silentAudio)
	darkScene[3].frames[100] = 1 // near-black but not the same decoded-frame hash
	fade := cloneSamples(silentAudio)
	copy(fade[4].frames, bytes.Repeat([]byte{32}, placeholderSampleWidth*placeholderSampleHeight))
	lowLight := cloneSamples(silentAudio)
	for i := range lowLight {
		lowLight[i].frames = bytes.Repeat([]byte{8},
			placeholderSampleWidth*placeholderSampleHeight*placeholderFramesPerSeek)
	}
	missing := cloneSamples(silentAudio)
	missing[2].frames = nil
	activeAudio := cloneSamples(silentAudio)
	activeAudio[4].audioPCM[0] = bytes.Repeat([]byte{0x10, 0x27}, 2000)
	missingAudio := cloneSamples(silentAudio)
	missingAudio[2].audioPCM[0] = nil
	unsupportedAudioProbe := validProbe
	unsupportedAudioProbe.audio = []finiteAudioStream{{index: 1, channels: 0}}
	unsupportedAudio := cloneSamples(silentAudio)
	overLimitAudioProbe := validProbe
	for i := 0; i <= maxPlaceholderAudioStreams; i++ {
		overLimitAudioProbe.audio = append(overLimitAudioProbe.audio,
			finiteAudioStream{index: i + 1, channels: 2})
	}

	tests := []struct {
		name     string
		probe    finiteProbe
		declared int64
		samples  []decodedMediaSample
		want     mediaClassification
	}{
		{"black video without declared audio fails open", validProbe, declared, black, classificationIndeterminate},
		{"black video with distributed silent audio is dead media", audioProbe, declared, silentAudio, classificationBlackPlaceholder},
		{"black video with active programme audio is accepted", audioProbe, declared, activeAudio, classificationFiniteAccepted},
		{"missing declared audio sample fails open", audioProbe, declared, missingAudio, classificationIndeterminate},
		{"unproven declared audio topology fails open", unsupportedAudioProbe, declared, unsupportedAudio, classificationIndeterminate},
		{"unbounded multi audio topology fails open", overLimitAudioProbe, declared, black, classificationIndeterminate},
		{"ordinary dark scene hashes vary", audioProbe, declared, darkScene, classificationFiniteAccepted},
		{"fade contains a nonblack decoded frame", audioProbe, declared, fade, classificationFiniteAccepted},
		{"uniform low-light is not black", audioProbe, declared, lowLight, classificationFiniteAccepted},
		{"missing decoded sample fails open", audioProbe, declared, missing, classificationIndeterminate},
		{"missing declared finite length fails open", validProbe, 0, black, classificationIndeterminate},
		{"probe size mismatch fails open", validProbe, declared - 1, black, classificationIndeterminate},
		{"non TS media is accepted", finiteProbe{formatName: "matroska", duration: 600 * time.Second, size: declared, video: []int{0}}, declared, black, classificationFiniteAccepted},
		{"non placeholder duration is accepted", finiteProbe{formatName: "mpegts", duration: 5 * time.Minute, size: declared, video: []int{0}}, declared, black, classificationFiniteAccepted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyDecodedMedia(tt.probe, tt.declared, tt.samples)
			if got.kind != tt.want {
				t.Fatalf("classification=%s (%s), want %s", got.kind, got.reason, tt.want)
			}
		})
	}
}

func TestFiniteClassifierCapacityWaitHonorsContext(t *testing.T) {
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary:        filepath.Join(t.TempDir(), "must-not-run-ffmpeg"),
		ffprobeBinary:       filepath.Join(t.TempDir(), "must-not-run-ffprobe"),
		classificationSlots: slots,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := classifier.Classify(ctx, []byte("not inspected while capacity is unavailable"))
	if got.kind != classificationIndeterminate || got.reason != "classifier_capacity_unavailable" {
		t.Fatalf("classification=%s (%s), want indeterminate capacity diagnostic",
			got.kind, got.reason)
	}
	if len(slots) != 1 {
		t.Fatalf("classifier consumed saturated slot after cancellation: len=%d", len(slots))
	}
}

func TestFiniteClassifiersShareProcessCapacity(t *testing.T) {
	first := newFFmpegFiniteMediaClassifier("ffmpeg")
	second := newFFmpegFiniteMediaClassifier("ffmpeg")
	if first.classificationSlots != processFiniteClassificationSlots ||
		second.classificationSlots != processFiniteClassificationSlots ||
		first.classificationSlots != second.classificationSlots {
		t.Fatal("finite classifiers do not share the process-wide capacity bound")
	}
	if cap(first.classificationSlots) != maxConcurrentFiniteClassifications {
		t.Fatalf("classification capacity=%d, want %d",
			cap(first.classificationSlots), maxConcurrentFiniteClassifications)
	}
}

func TestFiniteClassifierReleasesCapacityOnEarlyReturn(t *testing.T) {
	slots := make(chan struct{}, 1)
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary:        filepath.Join(t.TempDir(), "missing-ffmpeg"),
		ffprobeBinary:       filepath.Join(t.TempDir(), "missing-ffprobe"),
		classificationSlots: slots,
	}

	got := classifier.Classify(context.Background(), []byte("invalid finite media"))
	if got.kind != classificationIndeterminate || got.reason != "probe_failed" {
		t.Fatalf("classification=%s (%s), want indeterminate probe failure",
			got.kind, got.reason)
	}
	if len(slots) != 0 {
		t.Fatalf("classifier leaked capacity permit after early return: len=%d", len(slots))
	}
	select {
	case slots <- struct{}{}:
		<-slots
	default:
		t.Fatal("classification capacity remained unavailable after early return")
	}
}

func TestFiniteClassifierExecutionCancellationHasTypedReason(t *testing.T) {
	slots := make(chan struct{}, 1)
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary:        filepath.Join(t.TempDir(), "must-not-run-ffmpeg"),
		ffprobeBinary:       stubBinary(t, "exec sleep 30"),
		classificationSlots: slots,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	started := time.Now()
	got := classifier.Classify(ctx, []byte("bounded finite response"))
	if got.kind != classificationIndeterminate || got.reason != classifierExecutionTimeoutReason {
		t.Fatalf("classification=%s (%s), want typed execution timeout reason",
			got.kind, got.reason)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("canceled classifier returned after %v, want bounded process teardown", elapsed)
	}
	if len(slots) != 0 {
		t.Fatalf("classifier execution timeout leaked capacity permit: len=%d", len(slots))
	}
}

func TestClassifierExecutionTimeoutRelocatesWithoutPublishingFiniteBody(t *testing.T) {
	finite := finiteResponseServer(t, 0xaa)
	defer finite.Close()
	healthy := markerStreamServer(t, 0xbc)
	defer healthy.Close()

	classifier := &blockingFiniteClassifier{}
	s := NewStreamer("classifier-execution-relocate", finite.URL, testLogger(), nil)
	s.classifier = classifier
	s.sourceStartupTimeout = 250 * time.Millisecond
	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if !errors.Is(cause, errClassifierTimeout) || errors.Is(cause, errSourceStartupTimeout) {
			t.Errorf("classifier failure=%v, want typed local execution timeout", cause)
		}
		if got := diagnosticReason(cause); got != classifierExecutionTimeoutReason {
			t.Errorf("classifier diagnostic=%q, want %q", got, classifierExecutionTimeoutReason)
		}
		if shouldMarkSourceFailure(cause) {
			t.Errorf("local classifier timeout was attributed to provider: %v", cause)
		}
		failovers.Add(1)
		return healthy.URL, true, nil
	}

	chunks, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	started := time.Now()
	go func() {
		s.Run(ctx)
		close(done)
	}()
	select {
	case chunk, ok := <-chunks:
		if !ok {
			t.Fatal("subscriber closed before alternate media")
		}
		if len(chunk) == 0 || chunk[0] != 0xbc {
			t.Fatalf("finite body leaked before alternate media: %x", chunk[:min(len(chunk), 8)])
		}
	case <-time.After(9 * time.Second):
		t.Fatal("classifier execution timeout did not relocate inside Plex deadline")
	}
	if elapsed := time.Since(started); elapsed >= 9*time.Second {
		t.Fatalf("alternate media arrived after %v, want <9s Plex handoff", elapsed)
	}
	if classifier.calls.Load() != 1 || failovers.Load() != 1 {
		t.Fatalf("classifier calls=%d failovers=%d, want 1/1",
			classifier.calls.Load(), failovers.Load())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streamer did not exit after cancellation")
	}
}

func TestClassifierContextExpiryOverridesOrdinaryResultAndRelocates(t *testing.T) {
	for _, returned := range []classificationResult{
		{kind: classificationFiniteAccepted, reason: "ordinary_accepted_after_cancel"},
		{kind: classificationIndeterminate, reason: "ordinary_indeterminate_after_cancel"},
	} {
		t.Run(string(returned.kind), func(t *testing.T) {
			finite := finiteResponseServer(t, 0xaa)
			defer finite.Close()
			healthy := markerStreamServer(t, 0xbc)
			defer healthy.Close()

			classifier := &expiredContextResultClassifier{result: returned}
			s := NewStreamer("classifier-cancel-race-"+string(returned.kind),
				finite.URL, testLogger(), nil)
			s.classifier = classifier
			s.sourceStartupTimeout = 250 * time.Millisecond
			var failovers atomic.Int32
			s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
				if !errors.Is(cause, errClassifierTimeout) ||
					errors.Is(cause, errSourceStartupTimeout) ||
					errors.Is(cause, errClassifierCapacity) {
					t.Errorf("classifier cancellation race=%v, want typed execution timeout", cause)
				}
				if got := diagnosticReason(cause); got != classifierExecutionTimeoutReason {
					t.Errorf("classifier diagnostic=%q, want %q",
						got, classifierExecutionTimeoutReason)
				}
				if shouldMarkSourceFailure(cause) {
					t.Errorf("ordinary result after classifier expiry decayed provider health: %v", cause)
				}
				failovers.Add(1)
				return healthy.URL, true, nil
			}

			chunks, unsubscribe := s.Subscribe("viewer")
			defer unsubscribe()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			started := time.Now()
			go func() {
				s.Run(ctx)
				close(done)
			}()
			select {
			case chunk, ok := <-chunks:
				if !ok {
					t.Fatal("subscriber closed before alternate media")
				}
				if len(chunk) == 0 || chunk[0] != 0xbc {
					t.Fatalf("finite body leaked after classifier expiry: %x",
						chunk[:min(len(chunk), 8)])
				}
			case <-time.After(9 * time.Second):
				t.Fatal("ordinary classifier result prevented alternate handoff")
			}
			if elapsed := time.Since(started); elapsed >= 9*time.Second {
				t.Fatalf("alternate media arrived after %v, want <9s Plex handoff", elapsed)
			}
			if classifier.calls.Load() != 1 || failovers.Load() != 1 {
				t.Fatalf("classifier calls=%d failovers=%d, want 1/1",
					classifier.calls.Load(), failovers.Load())
			}

			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("streamer did not exit after cancellation")
			}
		})
	}
}

func TestClassifierCapacityTimeoutRelocatesWithoutPublishingFiniteBody(t *testing.T) {
	finite := finiteResponseServer(t, 0xaa)
	defer finite.Close()
	healthy := markerStreamServer(t, 0xbc)
	defer healthy.Close()

	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary:        filepath.Join(t.TempDir(), "must-not-run-ffmpeg"),
		ffprobeBinary:       filepath.Join(t.TempDir(), "must-not-run-ffprobe"),
		classificationSlots: slots,
	}
	s := NewStreamer("classifier-capacity-relocate", finite.URL, testLogger(), nil)
	s.classifier = classifier
	s.sourceStartupTimeout = 250 * time.Millisecond
	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if !errors.Is(cause, errClassifierCapacity) || errors.Is(cause, errSourceStartupTimeout) {
			t.Errorf("capacity failure=%v, want typed health-neutral classifier capacity cause", cause)
		}
		if got := diagnosticReason(cause); got != "classifier_capacity_unavailable" {
			t.Errorf("capacity diagnostic=%q, want classifier_capacity_unavailable", got)
		}
		if shouldMarkSourceFailure(cause) {
			t.Errorf("local classifier capacity was attributed to provider: %v", cause)
		}
		failovers.Add(1)
		return healthy.URL, true, nil
	}

	chunks, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()
	select {
	case chunk, ok := <-chunks:
		if !ok {
			t.Fatal("subscriber closed before alternate media")
		}
		if len(chunk) == 0 || chunk[0] != 0xbc {
			t.Fatalf("finite body leaked before alternate media: %x", chunk[:min(len(chunk), 8)])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("classifier capacity timeout did not relocate to healthy source")
	}
	if failovers.Load() != 1 {
		t.Fatalf("failovers=%d, want 1", failovers.Load())
	}
	if len(slots) != 1 {
		t.Fatalf("saturated capacity changed during canceled wait: len=%d", len(slots))
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streamer did not exit after cancellation")
	}
}

// This is the executable-level contract for the assumptions above: ffprobe
// must identify a finite near-600s MPEG-TS with declared audio, and six
// concurrent ffmpeg seeks must decode identical near-black gray frames plus
// silence inside one source budget.
func TestFFmpegClassifierRecognizesGeneratedTenMinuteBlackTS(t *testing.T) {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffmpeg missing from supported CI image")
		}
		t.Skip("ffmpeg not on PATH")
	}
	ffprobePath, err := exec.LookPath("ffprobe")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffprobe missing from supported CI image")
		}
		t.Skip("ffprobe not on PATH")
	}
	path := filepath.Join(t.TempDir(), "black-600s.ts")
	generateCtx, generateCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer generateCancel()
	cmd := exec.CommandContext(generateCtx, ffmpegPath,
		"-v", "error",
		"-f", "lavfi", "-i", "color=c=black:s=128x72:r=5:d=600",
		"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000:d=600",
		"-map", "0:v:0", "-map", "1:a:0", "-t", "600", "-shortest",
		"-c:v", "libx264", "-preset", "ultrafast",
		"-g", "5", "-keyint_min", "5", "-sc_threshold", "0",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k",
		"-f", "mpegts", "-y", path,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate black TS: %v: %s", err, out)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary: ffmpegPath, ffprobeBinary: ffprobePath,
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultSourceStartupTimeout)
	defer cancel()
	started := time.Now()
	got := classifier.Classify(ctx, data)
	if got.kind != classificationBlackPlaceholder {
		t.Fatalf("classification=%s (%s), want black placeholder", got.kind, got.reason)
	}
	if elapsed := time.Since(started); elapsed >= defaultSourceStartupTimeout {
		t.Fatalf("classification took %v, exceeds source budget %v", elapsed, defaultSourceStartupTimeout)
	}
}

func TestAttemptDiagnosticsUseEffectiveHostAndNeverLogCredentials(t *testing.T) {
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer final.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL+"/resolved", http.StatusFound)
	}))
	defer redirect.Close()

	initial := strings.Replace(redirect.URL, "127.0.0.1", "localhost", 1)
	cred := "user" + ":" + "pw" + "@"
	initial = strings.Replace(initial, "http://", "http://"+cred, 1)
	initial += "/private-path?token=credential-token"
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	s := NewStreamer("diagnostic-redaction", initial, logger, nil)
	s.sourceStartupTimeout = 100 * time.Millisecond
	s.OnUpstreamDown = func(context.Context, error) (string, bool, error) { return "", false, nil }
	_, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	var exitErr error
	exited := make(chan struct{})
	s.OnExit = func(err error) {
		exitErr = err
		close(exited)
	}
	go s.Run(context.Background())
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
		t.Fatal("diagnostic source did not exit")
	}

	combined := logs.String() + "\n" + fmt.Sprint(exitErr)
	for _, secret := range []string{"username", "supersecret", "private-path", "credential-token"} {
		if strings.Contains(combined, secret) {
			t.Fatalf("diagnostic leaked %q: %s", secret, combined)
		}
	}
	for _, field := range []string{
		"effective_host=127.0.0.1", "ttfb=", "bytes=0",
		"last_byte_gap=none", "classification=not_checked", "stage=stream",
		"result=startup_timeout",
	} {
		if !strings.Contains(logs.String(), field) {
			t.Errorf("diagnostic missing %q: %s", field, logs.String())
		}
	}
	if exitErr == nil || strings.Contains(exitErr.Error(), "http://") {
		t.Fatalf("terminal error is absent or contains a URL: %v", exitErr)
	}
}

func TestSubscriberStartupWaitsForRealMediaAfterHistoricalReconnect(t *testing.T) {
	s := NewStreamer("late-reconnect-subscriber", "http://unused.test", testLogger(), nil)
	s.mediaReady.Store(true) // historical pump-wide readiness from source one
	s.fanout([]byte("old-ring-media"))
	s.clearRing() // reconnect gap: a late joiner has no real pre-roll

	firstReal, chunks, _, unsubscribe := s.SubscribeForStartup("late-viewer")
	defer unsubscribe()
	s.fanoutSynthetic([]byte("source-unavailable-slate"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	result := make(chan startupChunk, 1)
	errs := make(chan error, 1)
	go func() {
		chunk, err := waitForSubscriberMedia(ctx, s, firstReal)
		result <- chunk
		errs <- err
	}()

	select {
	case <-result:
		t.Fatal("historical MediaReady or synthetic slate satisfied a new subscriber")
	case <-time.After(50 * time.Millisecond):
	}

	wantFirst := []byte("new-source-real-media")
	s.fanout(wantFirst)
	if got := <-result; !bytes.Equal(got.data, wantFirst) {
		t.Fatalf("first real chunk=%q, want %q", got.data, wantFirst)
	}
	if err := <-errs; err != nil {
		t.Fatalf("subscriber readiness error=%v", err)
	}
	select {
	case duplicate := <-chunks:
		t.Fatalf("startup slate/first real chunk was duplicated into data channel: %q", duplicate.data)
	default:
	}

	wantNext := []byte("next-real-media")
	s.fanout(wantNext)
	select {
	case got := <-chunks:
		if !bytes.Equal(got.data, wantNext) {
			t.Fatalf("next chunk=%q, want %q", got.data, wantNext)
		}
	case <-time.After(time.Second):
		t.Fatal("ordinary delivery did not resume after subscriber readiness")
	}
}

// Even if a late startup attachment wins immediately before the pump clears
// its ring for an EOF/reconnect gap, historical bytes cannot satisfy startup.
// Both pump implementations embed this subscriber state, so exercise each to
// prevent either Run path from reintroducing pre-gap readiness.
func TestStartupSubscriptionNeverCommitsHistoricalPreRoll(t *testing.T) {
	tests := []struct {
		name string
		set  func() *subscriberSet
	}{
		{
			name: "passthrough",
			set: func() *subscriberSet {
				s := NewStreamer("late-eof-pass", "http://unused.test", testLogger(), nil)
				return &s.subscriberSet
			},
		},
		{
			name: "transcode",
			set: func() *subscriberSet {
				s := NewTranscodeStreamer("late-eof-transcode", "http://unused.test",
					stubProfile(), "unused-ffmpeg", testLogger(), nil)
				return &s.subscriberSet
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := tt.set()
			ss.fanout([]byte("old-attempt-ring"))
			firstReal, chunks, _, unsubscribe := ss.SubscribeForStartup("late-viewer")
			defer unsubscribe()

			select {
			case got := <-firstReal:
				t.Fatalf("historical ring satisfied startup before validation: %q", got.data)
			default:
			}
			ss.clearRing()
			want := []byte("new-attempt-live-edge")
			ss.fanout(want)
			select {
			case got := <-firstReal:
				if !bytes.Equal(got.data, want) {
					t.Fatalf("first real chunk=%q, want current-attempt %q", got.data, want)
				}
			case <-time.After(time.Second):
				t.Fatal("current-attempt media did not satisfy startup")
			}
			select {
			case got := <-chunks:
				t.Fatalf("historical/current first chunk leaked into data channel: %q", got.data)
			default:
			}
		})
	}
}

func TestStartupPreRollRequiresCurrentEpochValidation(t *testing.T) {
	s := NewStreamer("validated-preroll", "http://unused.test", testLogger(), nil)
	old := []byte("eligible-current-attempt-preroll")
	validation := []byte("next-live-validation")
	s.fanout(old)
	firstReal, chunks, _, unsubscribe := s.SubscribeForStartup("late-viewer")
	defer unsubscribe()
	select {
	case got := <-firstReal:
		t.Fatalf("pre-roll escaped before live validation: %q", got.data)
	default:
	}
	s.fanout(validation)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := waitForSubscriberMedia(ctx, s, firstReal)
	if err != nil {
		t.Fatalf("validated pre-roll readiness: %v", err)
	}
	if !bytes.Equal(got.data, old) {
		t.Fatalf("first startup media=%q, want eligible pre-roll %q", got.data, old)
	}
	select {
	case got := <-chunks:
		if !bytes.Equal(got.data, validation) {
			t.Fatalf("post-pre-roll chunk=%q, want validation chunk %q", got.data, validation)
		}
	case <-time.After(time.Second):
		t.Fatal("validating live chunk was not queued after pre-roll")
	}
}

func TestStartupPreRollEpochChangeForcesRetry(t *testing.T) {
	s := NewStreamer("invalidated-preroll", "http://unused.test", testLogger(), nil)
	s.fanout([]byte("old-preroll"))
	firstReal, _, _, unsubscribe := s.SubscribeForStartup("late-viewer")
	defer unsubscribe()
	s.fanout([]byte("old-attempt-validation"))
	s.clearRing() // EOF/gap wins before Pool accepts the queued handoff.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := waitForSubscriberMedia(ctx, s, firstReal)
	if got.data != nil || !errors.Is(err, errPumpDiscontinuityBeforeMedia) {
		t.Fatalf("epoch-invalid startup got=%q err=%v, want retryable discontinuity", got.data, err)
	}
}

func TestSubscriberStartupRejectsClosedPumpBeforeRealByte(t *testing.T) {
	s := NewStreamer("closed-startup-pump", "http://unused.test", testLogger(), nil)
	s.setTerminalError(ErrPlaceholderMedia)
	s.closeAll()
	firstReal, _, _, unsubscribe := s.SubscribeForStartup("late-viewer")
	defer unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	chunk, err := waitForSubscriberMedia(ctx, s, firstReal)
	if chunk.data != nil {
		t.Fatalf("closed pump returned startup bytes: %x", chunk.data)
	}
	if !errors.Is(err, ErrUpstreamNotReady) || !strings.Contains(err.Error(), "black_placeholder") {
		t.Fatalf("closed pump error=%v, want bounded placeholder diagnostic", err)
	}
}

func TestSubscriberStartupDeadlineWinsOverBufferedRealMedia(t *testing.T) {
	s := NewStreamer("deadline-first-real", "http://unused.test", testLogger(), nil)
	firstReal, _, _, unsubscribe := s.SubscribeForStartup("late-viewer")
	defer unsubscribe()
	s.fanout([]byte("arrived-at-cutoff"))

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	chunk, err := waitForSubscriberMedia(ctx, s, firstReal)
	if chunk.data != nil {
		t.Fatalf("expired startup returned buffered media: %q", chunk.data)
	}
	if !errors.Is(err, ErrUpstreamNotReady) {
		t.Fatalf("expired startup error=%v, want ErrUpstreamNotReady", err)
	}
}

var _ io.Reader = (*upstreamAttempt)(nil)
