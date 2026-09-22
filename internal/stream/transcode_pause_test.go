package stream

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

// Exercise the actual stdout timer after validated fanout, twice in one
// attempt. The old 4.5s stdout deadline killed a healthy provider before its
// ordinary 5-6s pause ended, despite the provider's 20s live-read allowance.
// No <3s viewer-gap promise: this fixture supplies too little buffered media
// to bridge these pauses.
func TestTranscodeDefaultOutputDeadlineSurvivesRepeatedProviderPauses(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, pts, videoCC, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	pauses := []time.Duration{5500 * time.Millisecond, 6 * time.Second}
	tails := make([][]byte, len(pauses))
	for i := range tails {
		for len(tails[i]) < 2*chunkSize {
			tails[i] = append(tails[i], joinTSPackets(
				testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC),
				testTimestampedPESPacket(audioPID, 0xc0, pts, audioCC))...)
			pts += transportPTSRate / 50
			videoCC, audioCC = (videoCC+1)&0x0f, (audioCC+1)&0x0f
		}
	}
	published := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		flusher := w.(http.Flusher)
		if _, err := w.Write(prefix); err != nil {
			return
		}
		flusher.Flush()
		for i, pause := range pauses {
			select {
			case <-published[i]:
			case <-r.Context().Done():
				return
			}
			timer := time.NewTimer(pause)
			select {
			case <-timer.C:
			case <-r.Context().Done():
				timer.Stop()
				return
			}
			if _, err := w.Write(tails[i]); err != nil {
				return
			}
			flusher.Flush()
		}
		// Keep a live chunked HTTP response; EOF must not select finite replay.
		<-r.Context().Done()
	}))
	defer srv.Close()
	s := NewTranscodeStreamer("provider-pause-default-output-deadline", srv.URL,
		stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	if s.outputProgressTimeout != 4500*time.Millisecond {
		t.Fatalf("default stdout deadline=%s; regression requires real 4.5s deadline", s.outputProgressTimeout)
	}
	viewer, unsubscribe := s.Subscribe("provider-pause-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 16*time.Second)
	type outcome struct {
		bytes, upstream bool
		err             error
	}
	result := make(chan outcome, 1)
	runDone := make(chan struct{})
	go func() {
		gotBytes, upstream, err := s.runOnce(ctx)
		result <- outcome{gotBytes, upstream, err}
		close(runDone)
	}()
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(3 * time.Second):
			t.Error("pause attempt did not stop after cancellation")
		}
	}()
	received := make([]byte, 0, len(prefix)+len(tails)*len(tails[0]))
	prefixPublished, tailPublished := false, 0
	for tailPublished < len(tails) {
		select {
		case chunk, ok := <-viewer:
			if !ok {
				t.Fatal("subscriber closed before provider resumed")
			}
			received = append(received, chunk...)
			if !prefixPublished && len(received) >= len(prefix) {
				prefixPublished = true
				close(published[0])
			}
			marker := tails[tailPublished][len(tails[tailPublished])-tsPacketSize:]
			if bytes.Contains(received, marker) {
				tailPublished++
				if tailPublished < len(published) {
					close(published[tailPublished])
				}
			}
		case got := <-result:
			t.Fatalf("attempt ended before resumed real media: tails=%d/%d bytes=%v upstream=%v error=%v",
				tailPublished, len(tails), got.bytes, got.upstream, got.err)
		case <-ctx.Done():
			t.Fatalf("viewer received %d/%d resumed tails: %v", tailPublished, len(tails), ctx.Err())
		}
	}
	cancel()
	select {
	case got := <-result:
		if !got.bytes || got.upstream || (got.err != nil && !errors.Is(got.err, context.Canceled)) {
			t.Fatalf("completed pause attempt=(bytes=%v upstream=%v err=%v)", got.bytes, got.upstream, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pause attempt did not join after consumer completion")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("provider requests=%d, want same attempt across both pauses", got)
	}
}

// A controllable live provider: pause only after validated output is published,
// then resume continuously advancing A/V clocks. Keeping the body open and
// flushing prevents startup from accidentally testing the finite-input path.
func pauseControlProvider(t *testing.T, pause time.Duration) (
	*httptest.Server, chan struct{}, *atomic.Int64,
) {
	t.Helper()
	const pmtPID, videoPID, audioPID = 0x1000, 0x0100, 0x0101
	prefix, pts, videoCC, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	published := make(chan struct{})
	resumedAt := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher := w.(http.Flusher)
		if _, err := w.Write(prefix); err != nil {
			return
		}
		flusher.Flush()
		select {
		case <-published:
		case <-r.Context().Done():
			return
		}
		timer := time.NewTimer(pause)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		resumedAt.Store(time.Now().UnixNano())
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		// State is handler-local so test cleanup/retries cannot race packet clocks.
		clock, vcc, acc := pts, videoCC, audioCC
		for {
			chunk := joinTSPackets(
				testTimestampedPESPacket(videoPID, 0xe0, clock, vcc),
				testTimestampedPESPacket(audioPID, 0xc0, clock, acc))
			if _, err := w.Write(chunk); err != nil {
				return
			}
			flusher.Flush()
			clock += transportPTSRate / 50
			vcc, acc = (vcc+1)&0x0f, (acc+1)&0x0f
			select {
			case <-ticker.C:
			case <-r.Context().Done():
				return
			}
		}
	}))
	return srv, published, resumedAt
}

func TestTranscodeProviderPauseBeyondLiveToleranceRemainsUpstream(t *testing.T) {
	srv, published, resumedAt := pauseControlProvider(t, 2*time.Second)
	defer srv.Close()
	s := NewTranscodeStreamer("provider-pause-expired", srv.URL,
		stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 150 * time.Millisecond
	s.stallTolerance = 450 * time.Millisecond
	s.outputProgressTimeout = 200 * time.Millisecond
	var once sync.Once
	s.OnBytes = func(int) error { once.Do(func() { close(published) }); return nil }
	defer drainTranscodeTestSubscriber(s, "provider-pause-expired-viewer")()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	gotBytes, upstream, err := s.runOnce(ctx)
	if !gotBytes || !upstream || !errors.Is(err, errUpstreamIdle) || errors.Is(err, errTranscodeOutputIdle) {
		t.Fatalf("expired live provider pause=(bytes=%v upstream=%v err=%v), want bounded upstream idle", gotBytes, upstream, err)
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("expired provider pause took %s, want live deadline without waiting for resumed input", elapsed)
	}
	if resumedAt.Load() != 0 {
		t.Fatal("source resumed before its shorter live timeout fired")
	}
}

func TestTranscodeResumedInputCannotKeepHungStdoutAlive(t *testing.T) {
	const pause = 600 * time.Millisecond
	srv, published, resumedAt := pauseControlProvider(t, pause)
	defer srv.Close()
	s := NewTranscodeStreamer("provider-resumed-stdout-hung", srv.URL,
		stubProfile(), hungStdoutTranscoder(t), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 150 * time.Millisecond
	s.stallTolerance = 2 * time.Second
	s.outputProgressTimeout = 200 * time.Millisecond
	var once sync.Once
	s.OnBytes = func(int) error { once.Do(func() { close(published) }); return nil }
	var resumedWrites atomic.Int32
	s.inputPumpHooks = &transcodeInputPumpTestHooks{onWriteStart: func() {
		if resumedAt.Load() > 0 {
			resumedWrites.Add(1)
		}
	}}
	defer drainTranscodeTestSubscriber(s, "provider-resumed-stdout-hung-viewer")()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	gotBytes, upstream, err := s.runOnce(ctx)
	if !gotBytes || upstream || !errors.Is(err, errTranscodeOutputIdle) || !errors.Is(err, errLocalTranscodeFailure) {
		t.Fatalf("resumed provider / hung stdout=(bytes=%v upstream=%v err=%v), want bounded local output idle", gotBytes, upstream, err)
	}
	resumedNS := resumedAt.Load()
	if resumedNS <= 0 || resumedWrites.Load() < 2 {
		t.Fatalf("stdout timed out before resumed input reached the child: resumed=%v writes=%d", resumedNS > 0, resumedWrites.Load())
	}
	// Give the recovered input one fresh output allowance, not a renewal for
	// every subsequent successful body read or stdin write.
	if elapsed := time.Since(time.Unix(0, resumedNS)); elapsed < s.outputProgressTimeout-75*time.Millisecond || elapsed > s.outputProgressTimeout+600*time.Millisecond {
		t.Fatalf("hung stdout ended %s after provider resumed; want one bounded fresh %s output allowance", elapsed, s.outputProgressTimeout)
	}
	if shouldMarkSourceFailure(err) {
		t.Fatal("local hung stdout would penalize the recovered provider")
	}
}

func TestTranscodeCancellationDuringDeferredProviderPauseJoinsPromptly(t *testing.T) {
	srv, published, resumedAt := pauseControlProvider(t, 3*time.Second)
	defer srv.Close()
	s := NewTranscodeStreamer("provider-pause-cancel", srv.URL,
		stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 150 * time.Millisecond
	s.stallTolerance = 3 * time.Second
	s.outputProgressTimeout = 200 * time.Millisecond
	var once sync.Once
	s.OnBytes = func(int) error { once.Do(func() { close(published) }); return nil }
	defer drainTranscodeTestSubscriber(s, "provider-pause-cancel-viewer")()
	beforeOutputStalls := metrics.TranscodeOutputStalls.Value()
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		bytes, upstream bool
		err             error
	}
	result := make(chan outcome, 1)
	done := make(chan struct{})
	go func() {
		gotBytes, upstream, err := s.runOnce(ctx)
		result <- outcome{gotBytes, upstream, err}
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("deferred-pause attempt did not join on cleanup")
		}
	}()
	select {
	case <-published:
	case got := <-result:
		t.Fatalf("attempt ended before validated fanout: %+v", got)
	case <-time.After(2 * time.Second):
		t.Fatal("attempt never published initial real media")
	}
	// Pass several ordinary stdout deadlines while a legitimate live provider
	// read is still pending, then cancel before either input resume or expiry.
	wait := time.NewTimer(3 * s.outputProgressTimeout)
	defer wait.Stop()
	select {
	case <-wait.C:
	case got := <-result:
		t.Fatalf("attempt ended during eligible provider pause: %+v", got)
	}
	started := time.Now()
	cancel()
	select {
	case got := <-result:
		if !got.bytes || got.upstream || !errors.Is(got.err, context.Canceled) || errors.Is(got.err, errTranscodeOutputIdle) {
			t.Fatalf("deferred-pause cancellation=(bytes=%v upstream=%v err=%v), want cancellation without an output-idle fault", got.bytes, got.upstream, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation waited for the deferred provider deadline")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("deferred-pause cancellation took %s", elapsed)
	}
	if resumedAt.Load() != 0 {
		t.Fatal("fixture resumed instead of cancelling the outstanding provider read")
	}
	if delta := metrics.TranscodeOutputStalls.Value() - beforeOutputStalls; delta != 0 {
		t.Fatalf("cancellation recorded %d local stdout stalls, want zero", delta)
	}
}
