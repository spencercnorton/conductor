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

// A latched input fault must survive its cancellation interrupting output
// pacing. Synchronize at the real pacer boundary after validated publication.
func TestTranscodePacingCancellationPreservesInputFault(t *testing.T) {
	for _, tc := range []struct {
		name         string
		sendFault    bool
		cancelParent bool
	}{
		{"input_guard_fault_wins", true, false},
		{"parent_cancel_only", false, true},
		{"parent_cancellation_after_input_fault_is_neutral", true, true},
	} {
		name := tc.name
		t.Run(name, func(t *testing.T) {
			corruptionBefore := metrics.InputTransportCorruptionEvents.Value()
			chunks := burstyAVPCRTestChunks(8, 100*time.Millisecond)
			good := bytes.Join(chunks, nil)
			good = append(good, pcrTestNonPCRPacket(0x1ffe, false)[len(good)%tsPacketSize:]...)
			bad := make([]byte, tsPacketSize) // one complete, aligned invalid sync packet
			c := newCheckedTranscodeInput()
			if _, err := c.push(good, time.Now()); err != nil {
				t.Fatalf("healthy fixture rejected: %v", err)
			}
			if _, err := c.push(bad, time.Now()); err == nil {
				t.Fatal("fixture did not independently prove an input transport fault")
			}

			sendFault := make(chan struct{})
			faultSent := make(chan struct{})
			providerClosed := make(chan struct{})
			var providerCloseOnce sync.Once
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer providerCloseOnce.Do(func() { close(providerClosed) })
				w.Header().Set("Content-Type", "video/mp2t")
				w.WriteHeader(200)
				flush := w.(http.Flusher)
				flush.Flush()
				if _, err := w.Write(good); err != nil {
					return
				}
				flush.Flush()
				select {
				case <-sendFault:
					if _, err := w.Write(bad); err != nil {
						return
					}
					flush.Flush()
					close(faultSent)
				case <-r.Context().Done():
					return
				}
				<-r.Context().Done()
			}))
			defer srv.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := NewTranscodeStreamer(name, srv.URL, stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
			s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
			defer drainTranscodeTestSubscriber(s, name+"-viewer")()
			var published atomic.Int64
			var held atomic.Bool
			atPace := make(chan struct{})
			releasePace := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releasePace) }) }
			s.OnBytes = func(n int) error { published.Add(int64(n)); return nil }
			s.inputPumpHooks = &transcodeInputPumpTestHooks{onPacerWait: func() {
				// Complete the initial publication and its small marker remainder.
				// The next part carries a later PCR and reaches a real paced wait.
				if published.Load() >= chunkSize && held.CompareAndSwap(false, true) {
					close(atPace)
					select {
					case <-releasePace:
					case <-ctx.Done():
					}
				}
			}}
			type outcome struct {
				got, upstream bool
				err           error
			}
			result := make(chan outcome, 1)
			joined := make(chan struct{})
			go func() { got, up, err := s.runOnce(ctx); result <- outcome{got, up, err}; close(joined) }()
			defer func() {
				cancel()
				release()
				select {
				case <-joined:
				case <-time.After(2 * time.Second):
					t.Error("input pump/pacer did not join")
				}
			}()
			select {
			case <-atPace:
			case r := <-result:
				t.Fatalf("attempt ended before synchronized paced publication: got=%v upstream=%v err=%v", r.got, r.upstream, r.err)
			case <-time.After(2 * time.Second):
				t.Fatal("never reached post-publication pacer boundary")
			}
			if !tc.sendFault {
				cancel()
			} else {
				close(sendFault)
				select {
				case <-faultSent:
				case <-time.After(time.Second):
					t.Fatal("fault bytes were not sent")
				}
			}
			select {
			case <-providerClosed:
			case <-time.After(time.Second):
				t.Fatal("provider read did not close after chosen cancellation cause")
			}
			if tc.sendFault && ctx.Err() != nil {
				t.Fatalf("parent canceled before input fault assertion: %v", ctx.Err())
			}
			if tc.cancelParent {
				cancel()
			}
			release()
			var got outcome
			select {
			case got = <-result:
			case <-time.After(time.Second):
				t.Fatal("canceled pacer did not return")
			}
			if !got.got {
				t.Fatal("fixture never published validated media")
			}
			if tc.cancelParent {
				if got.upstream || !errors.Is(got.err, context.Canceled) {
					t.Fatalf("parent cancellation became a provider fault: upstream=%v err=%v", got.upstream, got.err)
				}
				if delta := metrics.InputTransportCorruptionEvents.Value() - corruptionBefore; delta != 0 {
					t.Fatalf("parent cancellation counted %d input corruption events", delta)
				}
				return
			}
			var fault *avContinuityError
			if !got.upstream || !errors.As(got.err, &fault) || fault.kind != avFaultTransport {
				t.Fatalf("stored input fault lost at canceled pacer: upstream=%v err=%v parent=%v; want upstream transport_corruption", got.upstream, got.err, ctx.Err())
			}
			if delta := metrics.InputTransportCorruptionEvents.Value() - corruptionBefore; delta != 1 {
				t.Fatalf("input fault counted %d times, want exactly one", delta)
			}
			if !shouldMarkSourceFailure(got.err) {
				t.Fatal("proven input fault lost source-health attribution")
			}
		})
	}
}

func TestTranscodeCallbackCancellationRemainsAbort(t *testing.T) {
	before := metrics.InputTransportCorruptionEvents.Value()
	good, _, _, _ := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
	sendFault := make(chan struct{})
	providerClosed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(providerClosed)
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(200)
		flush := w.(http.Flusher)
		flush.Flush()
		if _, e := w.Write(good); e != nil {
			return
		}
		flush.Flush()
		select {
		case <-sendFault:
		case <-r.Context().Done():
			return
		}
		if _, e := w.Write(make([]byte, tsPacketSize)); e != nil {
			return
		}
		flush.Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s := NewTranscodeStreamer("callback-abort-precedence", srv.URL, stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	defer drainTranscodeTestSubscriber(s, "callback-abort-viewer")()
	s.OnBytes = func(int) error {
		close(sendFault)
		select {
		case <-providerClosed:
		case <-ctx.Done():
		}
		return context.Canceled
	}
	got, up, err := s.runOnce(ctx)
	var aborted *errPumpAborted
	if !got || up || !errors.As(err, &aborted) || !errors.Is(err, context.Canceled) || ctx.Err() != nil {
		t.Fatalf("explicit callback abort overwritten: bytes=%v upstream=%v err=%v parent=%v; want errPumpAborted/context.Canceled and no relocation", got, up, err, ctx.Err())
	}
	if delta := metrics.InputTransportCorruptionEvents.Value() - before; delta != 0 {
		t.Fatalf("callback abort counted %d input corruption events", delta)
	}
}
