package stream

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecoveryFillerPacesMediaWithoutUnboundedCatchup(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		name                 string
		late, duration, want time.Duration
	}{
		{"on_time", 0, 40 * time.Millisecond, 40 * time.Millisecond},
		{"timer_jitter", time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond},
		{"long_stall", 3 * time.Second, 40 * time.Millisecond, 3040 * time.Millisecond},
		{"clockless_metadata", time.Millisecond, 0, time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextRecoveryFillerDeadline(base, base.Add(tc.late), tc.duration); !got.Equal(base.Add(tc.want)) {
				t.Fatalf("next media edge=%s, want %s", got.Sub(base), tc.want)
			}
		})
	}
}

func TestRecoveryFillerDoesNotEstablishInitialTuneReadiness(t *testing.T) {
	var set subscriberSet
	set.init("initial-tune", testLogger())
	defer set.closeAll()
	var requested atomic.Int64
	set.SlateFn = func() *slate {
		requested.Add(1)
		return &slate{data: []byte{0xab}, duration: time.Millisecond}
	}
	viewer, unsubscribe := set.Subscribe("initial-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !set.waitBeforeRetry(ctx, 30*time.Millisecond, time.Hour) {
		t.Fatal("backoff ended without cancellation")
	}
	if requested.Load() != 0 || !set.LastRealChunk().IsZero() {
		t.Fatal("initial tune requested fallback or acquired real-media evidence")
	}
	select {
	case <-viewer:
		t.Fatal("initial tune received fallback as readiness")
	default:
	}
}

func TestRecoveryFillerConcurrentTakeoverAndTerminalizationJoin(t *testing.T) {
	for i := 0; i < 20; i++ {
		var set subscriberSet
		set.init("takeover-close", testLogger())
		set.slateAfter = 0
		started := make(chan struct{})
		var once sync.Once
		set.SlateFn = func() *slate {
			once.Do(func() { close(started) })
			return &slate{data: bytes.Repeat([]byte{0xab}, 1024), duration: time.Millisecond}
		}
		_, unsubscribe := set.Subscribe("stalled-viewer")
		set.fanout([]byte{0x11})
		set.startRecoveryFiller(context.Background(), time.Hour)
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("filler did not start")
		}
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); set.fanoutValidatedStart([]byte{0x22}) }()
		go func() { defer wg.Done(); set.closeAll() }()
		go func() { defer wg.Done(); unsubscribe() }()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("takeover/terminalization did not join the synthetic writer")
		}
	}
}

// These non-TS fixtures and the cat subprocess isolate actual retry-loop
// lifetime/ownership. Real media splice and cadence tests live with the
// completed-PES publisher; a byte marker is not decoder evidence.
func TestRecoveryFillerContinuesThroughPrivateSourceStartup(t *testing.T) {
	for _, kind := range []string{"passthrough", "transcode"} {
		t.Run(kind, func(t *testing.T) {
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "video/mp2t")
				w.(http.Flusher).Flush()
				_, _ = w.Write(bytes.Repeat([]byte{0x11}, 128*1024))
				w.(http.Flusher).Flush()
				// Let the first real item reach the reader before this attempt
				// ends and the delivery epoch is intentionally reset.
				_ = waitTransportDelay(r.Context(), 100*time.Millisecond)
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "video/mp2t")
				w.(http.Flusher).Flush()
				if waitTransportDelay(r.Context(), 800*time.Millisecond) != nil {
					return
				}
				for {
					if _, err := w.Write(bytes.Repeat([]byte{0x22}, 8192)); err != nil {
						return
					}
					w.(http.Flusher).Flush()
					if waitTransportDelay(r.Context(), 20*time.Millisecond) != nil {
						return
					}
				}
			}))
			defer second.Close()
			var ss *subscriberSet
			var run func(context.Context)
			relocate := func(ctx context.Context, _ error) (string, bool, error) {
				if err := waitTransportDelay(ctx, 400*time.Millisecond); err != nil {
					return "", false, err
				}
				return second.URL, true, nil
			}
			if kind == "passthrough" {
				s := NewStreamer(kind, first.URL, testLogger(), nil)
				s.classifier = nil
				s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
				s.OnUpstreamDown = relocate
				ss, run = &s.subscriberSet, s.Run
			} else {
				s := NewTranscodeStreamer(kind, first.URL, stubProfile(), stubBinary(t, "cat"), testLogger(), nil)
				s.classifier = nil
				s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
				s.OnUpstreamDown = relocate
				ss, run = &s.subscriberSet, s.Run
			}
			ss.slateAfter = 0
			ss.SlateFn = func() *slate { return &slate{data: bytes.Repeat([]byte{0xab}, 1024), duration: 20 * time.Millisecond} }
			viewer, unsubscribe := ss.Subscribe("recovery-viewer")
			defer unsubscribe()
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); run(ctx) }()
			defer func() { cancel(); <-done }()
			var lastSlate, lastFirst, recoveredAt time.Time
			var maxGap time.Duration
			sawFirst := false
			for recoveredAt.IsZero() || time.Since(recoveredAt) < 150*time.Millisecond {
				select {
				case <-ctx.Done():
					t.Fatal("recovery did not finish within source/test deadlines")
				case chunk, ok := <-viewer:
					if !ok || len(chunk) == 0 {
						t.Fatal("viewer closed or empty during recovery")
					}
					now := time.Now()
					switch chunk[0] {
					case 0x11:
						sawFirst = true
						lastFirst = now
					case 0xab:
						if !recoveredAt.IsZero() {
							t.Fatal("synthetic media interleaved after real takeover")
						}
						if lastSlate.IsZero() && !lastFirst.IsZero() {
							maxGap = max(maxGap, now.Sub(lastFirst))
						}
						if !lastSlate.IsZero() {
							maxGap = max(maxGap, now.Sub(lastSlate))
						}
						lastSlate = now
					case 0x22:
						if recoveredAt.IsZero() {
							if !sawFirst || lastSlate.IsZero() {
								t.Fatal("fixture did not include initial real media and recovery slate")
							}
							maxGap = max(maxGap, now.Sub(lastSlate))
							recoveredAt = now
						}
					}
				}
			}
			t.Logf("largest fallback/startup arrival gap=%s", maxGap)
			if maxGap > 250*time.Millisecond {
				t.Fatalf("fallback stopped during private source startup: gap=%s", maxGap)
			}
		})
	}
}
