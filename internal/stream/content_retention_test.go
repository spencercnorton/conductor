package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

func TestTranscodeRetainsValidatedTransportAcrossPacingCap(t *testing.T) {
	const count = 16
	chunks := burstyAVPCRTestChunks(count, 50*time.Millisecond)
	// A common A/V/PCR forward step is valid programme media. The first
	// affected output part reaches the live pacing cap; a second part of the
	// same transformed read must remain attached to its partial TS packet.
	for i := 1; i < count; i++ {
		packetOffset := ((i*chunkSize + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
		clock := time.Duration(i)*50*time.Millisecond + 1500*time.Millisecond
		localOffset := packetOffset - i*chunkSize
		copy(chunks[i][localOffset+6:localOffset+12], slateTestPCR(durationTicks(clock, transportClockRate)))
		copy(chunks[i][localOffset+tsPacketSize:localOffset+2*tsPacketSize],
			testTimestampedPESPacket(0x100, 0xe0, durationTicks(clock, transportPTSRate), byte(i&15)))
		copy(chunks[i][localOffset+2*tsPacketSize:localOffset+3*tsPacketSize],
			testTimestampedPESPacket(0x101, 0xc0, durationTicks(clock, transportPTSRate), byte(i&15)))
	}
	input := bytes.Join(chunks, nil)
	input = append(input, pcrTestNonPCRPacket(0x1ffe, false)[len(input)%tsPacketSize:]...)
	declareSyntheticTimestampPESLengths(input)
	checked, err := newCheckedTranscodeInput().push(input, time.Now())
	if err != nil || !bytes.Equal(checked, input) {
		t.Fatalf("fixture input safety bytes=%d/%d err=%v", len(checked), len(input), err)
	}
	selection, ready, err := selectCheckedInputProgram(input)
	if err != nil || !ready {
		t.Fatalf("programme selection: ready=%v err=%v", ready, err)
	}
	var expectedTransform selectedProgramBoundaryScanner
	expectedTransform.configure(selection.program, true)
	want, err := expectedTransform.transform(input)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(input)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	s := NewTranscodeStreamer("retained-pacing-cap", srv.URL, stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
	s.sourceStartupTimeout = 5 * time.Second
	viewer, unsubscribe := s.Subscribe("retained-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	received := make(chan []byte, 1)
	go func() {
		var wire []byte
		for len(wire) < len(want) {
			select {
			case chunk, ok := <-viewer:
				if !ok {
					received <- wire
					return
				}
				wire = append(wire, chunk...)
			case <-ctx.Done():
				received <- wire
				return
			}
		}
		received <- wire
		cancel()
	}()
	beforeSkips := metrics.RelaySkipAheadChunks.Value()
	gotBytes, upstream, runErr := s.runOnce(ctx)
	wire := <-received
	if !gotBytes || upstream || (runErr != nil && !errors.Is(runErr, context.Canceled)) {
		t.Fatalf("live attempt bytes=%v upstream=%v err=%v received=%d/%d", gotBytes, upstream, runErr, len(wire), len(want))
	}
	if requests.Load() != 1 {
		t.Fatalf("upstream requests=%d, want1", requests.Load())
	}
	if delta := metrics.RelaySkipAheadChunks.Value() - beforeSkips; delta != 0 {
		t.Errorf("discarded transport chunks=%d", delta)
	}
	if !bytes.Equal(wire, want) {
		t.Fatalf("subscriber media changed: got=%d want=%d", len(wire), len(want))
	}
	for off := 0; off < len(wire); off += tsPacketSize {
		if wire[off] != 0x47 {
			t.Fatalf("subscriber TS sync lost at %d", off)
		}
	}
}

func TestLiveReadAheadPublicationProgressKeepsFullQueueBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	close(release)
	var publication atomic.Int64
	observed := make(chan struct{}, 4)
	const timeout = 100 * time.Millisecond
	reads, terminal, done := startRelayReadAheadWithPolicy(ctx, io.NopCloser(bytes.NewReader(make([]byte, 4*chunkSize))),
		func([]byte, time.Time, time.Time) { observed <- struct{}{} }, nil, 1, release, timeout, &publication)
	for i := 0; i < 2; i++ {
		select {
		case <-observed:
		case err := <-terminal:
			t.Fatalf("setup read %d failed: %v", i, err)
		case <-time.After(time.Second):
			t.Fatalf("setup read %d did not arrive", i)
		}
	}
	// No dequeue occurs here: publication drains an independently held, bounded
	// validated slice. The producer must stay blocked without growing its queue.
	for i := 0; i < 6; i++ {
		publication.Store(time.Now().UnixNano())
		select {
		case err := <-terminal:
			t.Fatalf("full queue failed during publication: %v", err)
		case <-time.After(timeout / 3):
		}
		if len(reads) != 1 {
			t.Fatalf("queue grew/shrank without dequeue: %d", len(reads))
		}
	}
	stoppedAt := time.Now()
	select {
	case err := <-terminal:
		if !errors.Is(err, errRelayReadAheadFull) {
			t.Fatalf("stalled consumer err=%v", err)
		}
	case <-time.After(3 * timeout):
		t.Fatal("publication stopped but full queue did not terminate")
	}
	if time.Since(stoppedAt) > 2*timeout {
		t.Fatal("full queue extended no-progress timeout")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full queue worker failed to join")
	}
}

func TestSelectedTSPublicationRemainsAlignedAcrossAbortAndRetry(t *testing.T) {
	for _, transcodePath := range []bool{false, true} {
		name := "passthrough"
		if transcodePath {
			name = "transcode"
		}
		t.Run(name, func(t *testing.T) {
			input := bytes.Join(burstyAVPCRTestChunks(4, 50*time.Millisecond), nil)
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "video/mp2t")
				_, _ = w.Write(input)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			abort := errors.New("abort after first complete transport publication")
			var viewer <-chan []byte
			var wire []byte
			captureAndAbort := func(n int) error {
				select {
				case chunk, ok := <-viewer:
					if !ok {
						t.Fatal("subscriber closed before abort boundary")
					}
					if len(chunk) != n {
						t.Fatalf("publication bytes=%d, callback bytes=%d", len(chunk), n)
					}
					wire = append(wire, chunk...)
				case <-ctx.Done():
					t.Fatal("subscriber did not receive abort boundary")
				}
				return abort
			}
			var run func() (bool, error)
			var clearRing func()
			if transcodePath {
				s := NewTranscodeStreamer(name, srv.URL, stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
				s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
				s.OnBytes = captureAndAbort
				var unsubscribe func()
				viewer, unsubscribe = s.Subscribe("packet-boundary-viewer")
				defer unsubscribe()
				clearRing = s.clearRing
				run = func() (bool, error) {
					got, _, err := s.runOnce(ctx)
					return got, err
				}
			} else {
				s := NewStreamer(name, srv.URL, testLogger(), nil)
				s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
				s.OnBytes = captureAndAbort
				var unsubscribe func()
				viewer, unsubscribe = s.Subscribe("packet-boundary-viewer")
				defer unsubscribe()
				clearRing = s.clearRing
				run = func() (bool, error) { return s.runOnce(ctx) }
			}
			for attempt := 0; attempt < 2; attempt++ {
				got, err := run()
				if !got || !errors.Is(err, abort) {
					t.Fatalf("attempt %d bytes=%v err=%v", attempt, got, err)
				}
				if len(wire)%tsPacketSize != 0 {
					t.Fatalf("attempt %d left %d bytes of a partial transport packet", attempt, len(wire)%tsPacketSize)
				}
				// Match Run's retry boundary after the prior publication has
				// reached this same subscriber. This tests packet framing only;
				// completing a PES or video reference chain is a separate contract.
				clearRing()
			}
			if requests.Load() != 2 {
				t.Fatalf("upstream attempts=%d, want 2", requests.Load())
			}
			for off := 0; off < len(wire); off += tsPacketSize {
				if wire[off] != 0x47 {
					t.Fatalf("retry wire lost TS sync at %d", off)
				}
			}
		})
	}
}

func TestTransportPCRPacerProfilesAlignedPublicationParts(t *testing.T) {
	// The narrower publication partition adds a sparse-PCR tail part. The
	// profiler must include its estimated step, not model producer read sizes.
	chunks := pcrPatternTestChunks(8, map[int]time.Duration{
		0: 0,
		6: 100 * time.Millisecond,
		7: 1950 * time.Millisecond,
	})
	prefix := bytes.Join(chunks, nil)
	wall := time.Unix(1_700_000_000, 0)
	arrival := wall
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	if err := pacer.PrepareBufferedPrefix(prefix, publicationChunkSize); err != nil {
		t.Fatal(err)
	}
	if pacer.pendingBudget <= 1950*time.Millisecond {
		t.Fatal("profile excluded the final unclocked publication part")
	}
	previous := wall
	for offset := 0; offset < len(prefix); offset += publicationChunkSize {
		end := min(offset+publicationChunkSize, len(prefix))
		if err := pacer.Wait(context.Background(), prefix[offset:end], arrival); err != nil {
			t.Fatalf("aligned part %d: %v", offset/publicationChunkSize, err)
		}
		if offset > 0 && !wall.After(previous) {
			t.Fatalf("aligned part %d did not retain a positive interval", offset/publicationChunkSize)
		}
		previous = wall
	}
	if elapsed := wall.Sub(arrival); elapsed <= 0 || elapsed > maxTransportAddedLatency-transportWakeReserve {
		t.Fatalf("aligned prefix elapsed=%s, want positive and within pacing window", elapsed)
	}
}

func TestTransportPrefixProfilesOnlyItsPublishablePacketTail(t *testing.T) {
	input := bytes.Join(burstyAVPCRTestChunks(2, 50*time.Millisecond), nil)
	selection, ready, err := selectCheckedInputProgram(input)
	if err != nil || !ready {
		t.Fatalf("fixture programme ready=%v err=%v", ready, err)
	}
	for _, tc := range []struct {
		name      string
		data      []byte
		bound     bool
		wantError bool
	}{
		{name: "retained incomplete packet", data: input[:chunkSize], bound: true},
		{name: "complete unclocked packet", data: input[:publicationChunkSize+tsPacketSize], bound: true, wantError: true},
		{name: "unbound data unchanged", data: input[:chunkSize], wantError: true},
		{name: "leading bytes unchanged", data: append([]byte{0xff}, input[:chunkSize]...), bound: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pacer := testTransportPCRPacer(false)
			pacer.ConfigureSelectedProgram(selection.program, tc.bound)
			err := pacer.PrepareBufferedPrefix(tc.data, publicationChunkSize)
			if errors.Is(err, errTransportRelayBacklog) != tc.wantError {
				t.Fatalf("prefix err=%v, want backlog rejection=%v", err, tc.wantError)
			}
		})
	}
}
