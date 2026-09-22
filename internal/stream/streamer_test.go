// streamer_test.go — unit tests for the Phase 5 resilience layer: isolated
// per-subscriber delivery (S2), the no-bytes watchdog + cancellable pump (S3),
// and mid-stream reconnect via OnUpstreamDown (S1). All white-box, no DB —
// the Pool/store integration lives in the *_integration_test files.
package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestDefaultMidstreamRecoveryFitsPlexSilenceBudget(t *testing.T) {
	bound := defaultIdleReadTimeout + midstreamRelocateTimeout +
		reconnectInitialBackoff + defaultSourceStartupTimeout
	if bound >= ClientStartupBudget {
		t.Fatalf("worst-case midstream recovery=%s, must stay below Plex/Conductor budget %s",
			bound, ClientStartupBudget)
	}
	if defaultIdleReadTimeout <= defaultSourceStartupTimeout {
		t.Fatalf("idle watchdog=%s must not race the independent startup timeout %s",
			defaultIdleReadTimeout, defaultSourceStartupTimeout)
	}
	transcodeBound := defaultTranscodeOutputProgressTimeout +
		reconnectInitialBackoff + defaultSourceStartupTimeout
	if transcodeBound >= ClientStartupBudget {
		t.Fatalf("worst-case local transcode recovery=%s, must stay below Plex/Conductor budget %s",
			transcodeBound, ClientStartupBudget)
	}
	transcodeInputBound := defaultIdleReadTimeout + providerReadWatchdogInterval +
		midstreamRelocateTimeout + reconnectInitialBackoff + defaultSourceStartupTimeout
	if transcodeInputBound >= ClientStartupBudget {
		t.Fatalf("worst-case transcode input relocation=%s, must stay below Plex/Conductor budget %s",
			transcodeInputBound, ClientStartupBudget)
	}
	if transcodeOutputAttributionMargin <= providerReadWatchdogInterval {
		t.Fatalf("output attribution margin=%s must exceed input watchdog jitter=%s",
			transcodeOutputAttributionMargin, providerReadWatchdogInterval)
	}
	if !requiresImmediateContinuityFiller(errUpstreamIdle) ||
		!requiresImmediateContinuityFiller(&avContinuityError{kind: avFaultMediaStarvation}) ||
		!requiresImmediateContinuityFiller(localTranscodeFailure(errTranscodeOutputIdle)) {
		t.Fatal("hard silence/continuity failures would wait for the ordinary slate delay")
	}
	if !isLocalRelayRetry(localTranscodeFailure(errTranscodeOutputIdle)) {
		t.Fatal("hung FFmpeg stdout would leave the same-source local retry path")
	}
}

type cancelFirstMarkerPrefixValidator struct{}

func (cancelFirstMarkerPrefixValidator) Validate(ctx context.Context, data []byte) (prefixDecision, error) {
	if len(data) > 0 && data[0] == 0xaa {
		<-ctx.Done()
		return prefixDecision{}, ctx.Err()
	}
	return prefixDecision{kind: prefixReady, reason: "test_alternate_ready"}, nil
}

type exactProgramMapTestValidator struct {
	readyAt int
	delay   time.Duration
}

func (v exactProgramMapTestValidator) Validate(ctx context.Context, data []byte) (prefixDecision, error) {
	if len(data) < v.readyAt {
		return prefixDecision{kind: prefixNeedMore, reason: "test_buffering"}, nil
	}
	programs := scanTSProgramMaps(data, "")
	if len(programs) == 0 {
		return prefixDecision{kind: prefixNeedMore, reason: "test_program_map_missing"}, nil
	}
	if v.delay > 0 {
		timer := time.NewTimer(v.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return prefixDecision{}, ctx.Err()
		}
	}
	program := programs[0]
	return prefixDecision{
		kind: prefixReady, reason: "test_exact_program_map", mediaDuration: time.Second,
		videoPID: program.videoPID, audioPIDs: program.audioPIDs, audioCount: program.audioCount,
		pcrPID: program.pcrPID, havePCRPID: program.pcrPID >= 0,
		haveProgram: true, mapIdentity: program.mapIdentity.clone(),
	}, nil
}

func testStreamContainsPTS(data []byte, pid int, want uint64) bool {
	for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		packetPID := int(packet[1]&0x1f)<<8 | int(packet[2])
		if packet[0] != 0x47 || packetPID != pid || packet[1]&0x40 == 0 {
			continue
		}
		payload, ok := tsPayload(packet)
		if !ok {
			continue
		}
		pts, status := parsePESPTSStatus(payload)
		if status == pesPTSPresent && pts == want {
			return true
		}
	}
	return false
}

func testAVPrefixWithMapAt(
	patPacket, pmtPacket []byte,
	videoPID, audioPID int,
	startPTS uint64,
) (prefix []byte, nextPTS uint64, nextVideoCC, nextAudioCC byte) {
	packets := [][]byte{patPacket, pmtPacket}
	nextPTS = startPTS
	for len(packets)*tsPacketSize < minPrefixDecisionBytes+2*tsPacketSize {
		packets = append(packets,
			testTimestampedPESPacket(videoPID, 0xe0, nextPTS, nextVideoCC),
			testTimestampedPESPacket(audioPID, 0xc0, nextPTS, nextAudioCC))
		nextVideoCC, nextAudioCC = (nextVideoCC+1)&0x0f, (nextAudioCC+1)&0x0f
		nextPTS += transportPTSRate / 50
	}
	return joinTSPackets(packets...), nextPTS, nextVideoCC, nextAudioCC
}

const (
	delayedPrefixChunkCount     = 25
	delayedLivePrefixChunkCount = 1 + prefixProbeByteStep/chunkSize
)

func delayedPrefixStreamServer(t *testing.T, pause time.Duration) *httptest.Server {
	return delayedPrefixStreamServerMode(t, pause, false)
}

func delayedPrefixLiveStreamServer(t *testing.T, pause time.Duration) *httptest.Server {
	return delayedPrefixStreamServerMode(t, pause, true)
}

func delayedPrefixStreamServerMode(t *testing.T, pause time.Duration, holdOpen bool) *httptest.Server {
	t.Helper()
	chunkCount := delayedPrefixChunkCount
	if holdOpen {
		// A live response has no terminal EOF to force gate validation. Send
		// through the second byte-step probe so readiness is deterministic while
		// keeping the request open until the test cancels it.
		chunkCount = delayedLivePrefixChunkCount
	}
	chunks := pcrSparseTestChunks(chunkCount, 50*time.Millisecond)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		split := len(chunks) / 2
		for _, chunk := range chunks[:split] {
			_, _ = w.Write(chunk)
		}
		if flusher != nil {
			flusher.Flush()
		}
		timer := time.NewTimer(pause)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		for _, chunk := range chunks[split:] {
			_, _ = w.Write(chunk)
		}
		if flusher != nil {
			flusher.Flush()
		}
		if holdOpen {
			<-r.Context().Done()
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// patternServer streams batches of 8-byte big-endian sequence numbers forever
// (or until stop is closed / the client goes away). Batching keeps the fixture
// at a realistic video bitrate now that relay read-ahead deliberately
// coalesces short network reads into byte-accounted chunks.
func patternServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	return patternServerAt(t, 0)
}

// patternServerAt is patternServer with sequence numbers starting at base —
// distinct bases let tests tell two upstreams' bytes apart.
func patternServerAt(t *testing.T, base uint64) (*httptest.Server, func()) {
	t.Helper()
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		fl, _ := w.(http.Flusher)
		seq := base
		const sequencesPerWrite = 1024
		buf := make([]byte, 8*sequencesPerWrite)
		for {
			select {
			case <-stop:
				return
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			for i := 0; i < sequencesPerWrite; i++ {
				binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], seq)
				seq++
			}
			if _, err := w.Write(buf); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	var once atomic.Bool
	return srv, func() {
		if once.CompareAndSwap(false, true) {
			close(stop)
			srv.CloseClientConnections()
			srv.Close()
		}
	}
}

// fragmentEOFServer models the provider failure that motivated the stable
// attempt gate: every request succeeds with HTTP 200 and a small body, then
// EOFs before the stream is useful. Returning a request counter lets both the
// passthrough and transcode tests prove that reconnects happened before the
// continuous-outage window stopped them.
func fragmentEOFServer(t *testing.T, lifetime time.Duration) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	fragment := bytes.Repeat([]byte{0x47}, 32*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(fragment)
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		time.Sleep(lifetime)
	}))
	t.Cleanup(srv.Close)
	return srv, &requests
}

// drainSeqs reads chunks off ch and verifies the embedded 8-byte sequence
// numbers are strictly contiguous — any silently dropped chunk errors here.
// Returns error (not t.Fatal) so it is safe from non-test goroutines.
func drainSeqs(ch <-chan []byte, want int) error {
	deadline := time.After(10 * time.Second)
	var expected uint64
	sawAny := false
	got := 0
	for got < want {
		select {
		case <-deadline:
			return fmt.Errorf("timed out after %d/%d contiguous chunks", got, want)
		case chunk, ok := <-ch:
			if !ok {
				return fmt.Errorf("channel closed after %d/%d chunks", got, want)
			}
			// The pump may coalesce several 8-byte writes into one chunk.
			for off := 0; off+8 <= len(chunk); off += 8 {
				seq := binary.BigEndian.Uint64(chunk[off : off+8])
				if sawAny && seq != expected {
					return fmt.Errorf("sequence gap: want %d got %d (silent drop?)", expected, seq)
				}
				expected = seq + 1
				sawAny = true
				got++
			}
		}
	}
	return nil
}

func drainSeqsUntil(ctx context.Context, ch <-chan []byte) error {
	var expected uint64
	sawAny := false
	for {
		select {
		case <-ctx.Done():
			if !sawAny {
				return errors.New("continuous drain stopped before receiving media")
			}
			return nil
		case chunk, ok := <-ch:
			if !ok {
				return errors.New("continuous drain channel closed")
			}
			for off := 0; off+8 <= len(chunk); off += 8 {
				seq := binary.BigEndian.Uint64(chunk[off : off+8])
				if sawAny && seq != expected {
					return fmt.Errorf("sequence gap: want %d got %d (silent drop?)", expected, seq)
				}
				expected = seq + 1
				sawAny = true
			}
		}
	}
}

// S2: a healthy subscriber must receive a gap-free stream even while a
// stalled subscriber is being disconnected next to it.
func TestFanoutDisconnectsStalledSubscriberKeepsHealthyIntact(t *testing.T) {
	t.Parallel()
	srv, stopSrv := patternServer(t)
	defer stopSrv()

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	s.stallBudget = 300 * time.Millisecond

	healthy, cancelHealthy := s.Subscribe("healthy")
	defer cancelHealthy()
	stalled, cancelStalled := s.Subscribe("stalled")
	defer cancelStalled()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	// Healthy drains contiguously the whole time — a single dropped chunk
	// fails the sequence check.
	healthyDrainCtx, stopHealthyDrain := context.WithCancel(context.Background())
	defer stopHealthyDrain()
	healthyErr := make(chan error, 1)
	go func() { healthyErr <- drainSeqsUntil(healthyDrainCtx, healthy) }()

	// `stalled` drains one chunk per 500ms — slower than the 300ms budget —
	// so the pump must classify it as stalled and close its channel. Once
	// the drop is recorded, drain at full speed so the buffered backlog
	// doesn't delay our *observation* of the close.
	closed := make(chan struct{})
	go func() {
		for {
			if _, ok := <-stalled; !ok {
				close(closed)
				return
			}
			if s.SubDrops() == 0 {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	select {
	case <-time.After(15 * time.Second):
		t.Fatalf("stalled subscriber was never disconnected: bytes=%d subscribers=%d drops=%d stalls=%d",
			s.BytesOut(), s.SubscriberCount(), s.SubDrops(), s.Stalls())
	case <-closed:
	}

	if got := s.SubDrops(); got < 1 {
		t.Fatalf("SubDrops = %d, want >= 1", got)
	}

	stopHealthyDrain()
	if err := <-healthyErr; err != nil {
		t.Fatalf("healthy subscriber: %v", err)
	}
	cancel()
	<-done
}

// fanout itself must never inherit a slow subscriber's wait. The
// worker may spend its full budget on the blocked sink, but a burst is queued
// independently and a healthy viewer receives every chunk immediately.
func TestFanoutSlowSinkNeverBlocksPumpOrHealthyViewer(t *testing.T) {
	var ss subscriberSet
	ss.init("isolated-fanout", testLogger())
	// Keep the blocked subscriber attached for the whole proof. Adjacent tests
	// cover deadline-based eviction; this test proves the stronger property that
	// a healthy worker and the pump progress while another worker is explicitly
	// held at its subscriber-local delivery barrier. The timeouts below are only
	// deadlock guards, not performance assertions about a loaded race runner.
	ss.stallBudget = 30 * time.Second
	slowBlocked := make(chan struct{})
	releaseSlow := make(chan struct{})
	var slowBlockedOnce, releaseSlowOnce sync.Once
	ss.onDeliveryBlockStart = func(id string) {
		if id == "blocked-dvr" {
			slowBlockedOnce.Do(func() {
				close(slowBlocked)
				<-releaseSlow
			})
		}
	}
	release := func() { releaseSlowOnce.Do(func() { close(releaseSlow) }) }
	t.Cleanup(func() {
		release()
		ss.closeAll()
	})

	healthy, _ := ss.Subscribe("healthy-viewer")
	_, _ = ss.Subscribe("blocked-dvr")

	const chunks = 512
	healthyResult := make(chan error, 1)
	go func() { healthyResult <- drainSeqs(healthy, chunks) }()

	// Put the slow delivery worker into its public-channel wait before the
	// burst. Completion below therefore cannot be attributed to the slow sink
	// briefly accepting data or timing out first.
	first := make([]byte, 8)
	binary.BigEndian.PutUint64(first, 0)
	ss.fanout(first)
	select {
	case <-slowBlocked:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked DVR worker never entered its subscriber-local wait")
	}

	fanoutDone := make(chan struct{})
	go func() {
		defer close(fanoutDone)
		for seq := 1; seq < chunks; seq++ {
			chunk := make([]byte, 8)
			binary.BigEndian.PutUint64(chunk, uint64(seq))
			ss.fanout(chunk)
		}
	}()
	select {
	case <-fanoutDone:
	case <-time.After(5 * time.Second):
		t.Fatal("fanout blocked behind the deliberately stalled DVR worker")
	}

	select {
	case err := <-healthyResult:
		if err != nil {
			t.Fatalf("healthy viewer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("healthy viewer did not drain while the DVR worker remained blocked")
	}

	if got := ss.SubscriberCount(); got != 2 {
		t.Fatalf("subscriber count=%d, want both workers attached during isolation proof", got)
	}
	if got := ss.SubDrops(); got != 0 {
		t.Fatalf("slow-sink drops=%d before isolation proof completed, want 0", got)
	}

	release()
	ss.closeAll()
}

func TestFanoutDropsContinuallySlowSinkByEndToEndQueueAge(t *testing.T) {
	var ss subscriberSet
	ss.init("queue-age-fanout", testLogger())
	ss.stallBudget = 80 * time.Millisecond

	healthy, leaveHealthy := ss.Subscribe("healthy-viewer")
	defer leaveHealthy()
	slow, leaveSlow := ss.Subscribe("consistently-slow-dvr")
	defer leaveSlow()

	const chunks = 300
	healthyResult := make(chan error, 1)
	go func() { healthyResult <- drainSeqs(healthy, chunks) }()
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		for range slow {
			time.Sleep(4 * time.Millisecond)
		}
	}()

	for seq := range chunks {
		chunk := make([]byte, 8)
		binary.BigEndian.PutUint64(chunk, uint64(seq))
		ss.fanout(chunk)
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-healthyResult:
		if err != nil {
			t.Fatalf("healthy viewer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy viewer did not receive the full sequence")
	}
	select {
	case <-slowDone:
	case <-time.After(2 * ss.stallBudget):
		t.Fatal("continually slow sink was not disconnected by queue age")
	}
	if got := ss.SubDrops(); got != 1 {
		t.Fatalf("slow-sink drops=%d, want 1", got)
	}
	if got := ss.SubscriberCount(); got != 1 {
		t.Fatalf("subscriber count=%d, want only healthy viewer", got)
	}

	ss.closeAll()
}

func TestCloseAllConcurrentCallersWaitForTerminalization(t *testing.T) {
	var ss subscriberSet
	ss.init("concurrent-close", testLogger())
	ss.stallBudget = 2 * time.Second
	workerBlocked := make(chan struct{})
	releaseWorker := make(chan struct{})
	var blockOnce sync.Once
	ss.onDeliveryBlockStart = func(string) {
		blockOnce.Do(func() {
			close(workerBlocked)
			<-releaseWorker
		})
	}
	viewer, unsubscribe := ss.Subscribe("viewer")
	defer unsubscribe()

	// Fill the public channel and wedge its delivery worker inside the test
	// callback. The winning closeAll caller can remove the subscriber map, but
	// cannot finish its worker join until the callback is released.
	for seq := range subscriberBuffer + 2 {
		chunk := make([]byte, 8)
		binary.BigEndian.PutUint64(chunk, uint64(seq))
		ss.fanout(chunk)
	}
	select {
	case <-workerBlocked:
	case <-time.After(time.Second):
		t.Fatal("delivery worker did not reach the terminalization barrier")
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
	defer release()

	firstDone := make(chan struct{})
	go func() {
		ss.closeAll()
		close(firstDone)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		if ss.SubscriberCount() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first closeAll did not remove the subscriber before its worker join")
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan struct{})
	go func() {
		ss.closeAll()
		close(secondDone)
	}()

	select {
	case <-firstDone:
		t.Fatal("first closeAll returned before its subscriber worker completed")
	case <-secondDone:
		t.Fatal("concurrent closeAll returned before terminalization completed")
	case <-time.After(20 * time.Millisecond):
	}

	release()
	for name, done := range map[string]<-chan struct{}{
		"first":  firstDone,
		"second": secondDone,
	} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s closeAll caller did not complete", name)
		}
	}
	viewerClosed := make(chan struct{})
	go func() {
		for range viewer {
		}
		close(viewerClosed)
	}()
	select {
	case <-viewerClosed:
	case <-time.After(time.Second):
		t.Fatal("viewer channel was not closed by terminalization")
	}
}

// S2: a subscriber that stalls briefly (under budget) must not be
// disconnected and must not miss a single byte.
func TestFanoutStallUnderBudgetRecoversWithoutLoss(t *testing.T) {
	var ss subscriberSet
	ss.init("under-budget-stall", testLogger())
	ss.stallBudget = 2 * time.Second
	blocked := make(chan struct{})
	var blockedOnce sync.Once
	ss.onDeliveryBlockStart = func(string) {
		blockedOnce.Do(func() { close(blocked) })
	}

	ch, unsubscribe := ss.Subscribe("c1")
	t.Cleanup(func() {
		ss.closeAll()
		unsubscribe()
	})
	// Queue exactly enough unique chunks to fill the public channel, block the
	// worker on one more, and leave a final sentinel behind that blocked send.
	// The slow-path observer is a precise barrier: the worker has already failed
	// its non-blocking send and cannot finish until this test starts receiving.
	const chunks = subscriberBuffer + 2
	beforeMetric := metrics.SubscriberStalls.Value()
	for seq := range chunks {
		chunk := make([]byte, 8)
		binary.BigEndian.PutUint64(chunk, uint64(seq))
		ss.fanout(chunk)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	select {
	case <-blocked:
	case <-deadline.C:
		t.Fatalf("delivery worker did not enter the blocked-send path: public=%d/%d",
			len(ch), cap(ch))
	}

	// Receiving the final sentinel is an ordering barrier: the worker can only
	// send it after the blocked predecessor recovers and records the stall.
	if err := drainSeqs(ch, chunks); err != nil {
		t.Fatal(err)
	}

	if drops := ss.SubDrops(); drops != 0 {
		t.Fatalf("SubDrops = %d, want 0 (stall was under budget)", drops)
	}
	stalls := ss.Stalls()
	if stalls < 1 {
		t.Fatal("Stalls = 0 after explicitly entering and recovering the blocked-send path")
	}
	if delta := metrics.SubscriberStalls.Value() - beforeMetric; delta != uint64(stalls) {
		t.Fatalf("subscriber stall metric delta = %d, want recovered-stall count %d", delta, stalls)
	}
}

func TestAttemptBoundaryPurgesPrivateAndPublicSubscriberBacklog(t *testing.T) {
	var ss subscriberSet
	ss.init("epoch-purge", testLogger())
	ss.stallBudget = 5 * time.Second
	blocked := make(chan struct{})
	var blockedOnce sync.Once
	ss.onDeliveryBlockStart = func(string) {
		blockedOnce.Do(func() { close(blocked) })
	}

	ch, unsubscribe := ss.Subscribe("slow-plex")
	t.Cleanup(func() {
		unsubscribe()
		ss.closeAll()
	})

	// Leave old-attempt chunks in the private jitter queue while the worker is
	// blocked on the unbuffered HTTP-facing public handoff.
	for seq := 0; seq < subscriberBuffer+24; seq++ {
		chunk := []byte{0xa0, byte(seq)}
		ss.fanout(chunk)
	}
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatalf("slow client never built public/private backlog: public=%d", len(ch))
	}

	clearDone := make(chan struct{})
	go func() {
		ss.clearRing()
		close(clearDone)
	}()
	deadline := time.Now().Add(time.Second)
	for ss.MediaEpoch() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ss.MediaEpoch() == 0 {
		t.Fatal("attempt boundary did not advance the media epoch")
	}

	// These deliveries intentionally race the reset. epochDeliveryMu makes the
	// ordering structural: they cannot enter either queue until every worker has
	// drained old private and already-enqueued public bytes and acknowledged the
	// new epoch.
	slate := []byte("new-epoch-slate")
	fresh := []byte("new-source-media")
	newDone := make(chan struct{})
	go func() {
		ss.fanoutSynthetic(slate)
		ss.fanout(fresh)
		close(newDone)
	}()
	select {
	case <-clearDone:
	case <-time.After(time.Second):
		t.Fatal("attempt boundary waited for the subscriber stall budget")
	}
	select {
	case <-newDone:
	case <-time.After(time.Second):
		t.Fatal("new epoch remained blocked behind reset")
	}

	for index, want := range [][]byte{slate, fresh} {
		select {
		case got := <-ch:
			if !bytes.Equal(got, want) {
				t.Fatalf("post-boundary item %d=%q, want %q; stale epoch leaked", index, got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("post-boundary item %d was not delivered", index)
		}
	}
	select {
	case got := <-ch:
		t.Fatalf("unexpected queued byte after new epoch: %q", got)
	default:
	}
}

func TestFiniteDeliveryBoundaryWaitsForQueuedTail(t *testing.T) {
	var ss subscriberSet
	ss.init("finite-delivery-boundary", testLogger())
	ch, unsubscribe := ss.Subscribe("plex")
	t.Cleanup(func() {
		unsubscribe()
		ss.closeAll()
	})

	want := [][]byte{[]byte("first"), []byte("middle"), []byte("terminal")}
	for _, chunk := range want {
		ss.fanout(chunk)
	}
	boundaryDone := make(chan bool, 1)
	go func() {
		boundaryDone <- ss.waitForDeliveryBoundary(context.Background(), time.Second)
	}()
	select {
	case <-boundaryDone:
		t.Fatal("delivery boundary passed while the terminal chunk was still private")
	case <-time.After(25 * time.Millisecond):
	}

	for i, expected := range want {
		select {
		case got := <-ch:
			if !bytes.Equal(got, expected) {
				t.Fatalf("delivered chunk %d=%q, want %q", i, got, expected)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out draining chunk %d", i)
		}
	}
	select {
	case drained := <-boundaryDone:
		if !drained {
			t.Fatal("healthy finite delivery boundary reported an incomplete handoff")
		}
	case <-time.After(time.Second):
		t.Fatal("delivery boundary did not observe the drained terminal chunk")
	}
}

func TestFiniteDeliveryBoundaryDoesNotWaitPastBoundForStalledSubscriber(t *testing.T) {
	var ss subscriberSet
	ss.init("finite-delivery-boundary-stalled", testLogger())
	_, unsubscribe := ss.Subscribe("stalled-plex")
	t.Cleanup(func() {
		unsubscribe()
		ss.closeAll()
	})
	ss.fanout([]byte("blocked"))

	started := time.Now()
	if drained := ss.waitForDeliveryBoundary(context.Background(), 25*time.Millisecond); drained {
		t.Fatal("stalled subscriber unexpectedly crossed the delivery boundary")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("stalled delivery boundary took %v, want a bounded return", elapsed)
	}
}

func TestFiniteDeliveryBoundaryUsesOneStallBudgetForHealthyAndStalledSubscribers(t *testing.T) {
	var ss subscriberSet
	ss.init("finite-delivery-boundary-mixed", testLogger())
	ss.stallBudget = 500 * time.Millisecond
	stalledBlocked := make(chan struct{})
	releaseStalled := make(chan struct{})
	boundaryQueued := make(chan struct{})
	var stalledOnce, releaseOnce, boundaryOnce sync.Once
	ss.onDeliveryBlockStart = func(id string) {
		if id == "stalled-plex" {
			stalledOnce.Do(func() {
				close(stalledBlocked)
				<-releaseStalled
			})
		}
	}
	ss.onDeliveryBoundaryQueued = func() {
		boundaryOnce.Do(func() { close(boundaryQueued) })
	}
	release := func() { releaseOnce.Do(func() { close(releaseStalled) }) }
	healthy, unsubscribeHealthy := ss.Subscribe("healthy-plex")
	_, unsubscribeStalled := ss.Subscribe("stalled-plex")
	t.Cleanup(func() {
		release()
		unsubscribeHealthy()
		unsubscribeStalled()
		ss.closeAll()
	})

	terminal := []byte("terminal-finite-chunk")
	ss.fanout(terminal)
	select {
	case <-stalledBlocked:
	case <-time.After(5 * time.Second):
		t.Fatal("stalled subscriber did not enter its delivery wait")
	}
	started := time.Now()
	boundaryDone := make(chan bool, 1)
	go func() {
		boundaryDone <- ss.waitForDeliveryBoundary(
			context.Background(), ss.deliveryBoundaryTimeout())
	}()
	select {
	case <-boundaryQueued:
	case <-time.After(5 * time.Second):
		t.Fatal("mixed delivery boundary was not queued behind both subscribers")
	}
	if got := ss.SubscriberCount(); got != 2 {
		t.Fatalf("subscriber count before boundary release=%d, want 2", got)
	}
	release()
	select {
	case got := <-healthy:
		if !bytes.Equal(got, terminal) {
			t.Fatalf("healthy terminal chunk=%q, want %q", got, terminal)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy subscriber did not receive the finite terminal chunk")
	}
	select {
	case drained := <-boundaryDone:
		if drained {
			t.Fatal("mixed boundary ignored the stalled subscriber")
		}
	case <-time.After(time.Second):
		t.Fatal("mixed delivery boundary exceeded its shared stall budget")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("mixed delivery boundary elapsed=%v, want one bounded 500ms budget", elapsed)
	}

	clearDone := make(chan struct{})
	go func() {
		ss.clearRing()
		close(clearDone)
	}()
	select {
	case <-clearDone:
	case <-time.After(time.Second):
		t.Fatal("epoch reset deadlocked with an expired finite-delivery boundary")
	}
	fresh := []byte("fresh-epoch")
	ss.fanout(fresh)
	select {
	case got := <-healthy:
		if !bytes.Equal(got, fresh) {
			t.Fatalf("healthy post-reset chunk=%q, want %q", got, fresh)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy subscriber did not resume after mixed boundary reset")
	}
}

func TestDeliveryBoundaryTimeoutTracksConfiguredStallPolicy(t *testing.T) {
	var ss subscriberSet
	ss.init("delivery-boundary-budget", testLogger())
	ss.stallBudget = 175 * time.Millisecond
	if got := ss.deliveryBoundaryTimeout(); got != 175*time.Millisecond {
		t.Fatalf("configured boundary timeout=%v, want 175ms", got)
	}
	ss.stallBudget = defaultStallBudget + time.Second
	if got := ss.deliveryBoundaryTimeout(); got != defaultStallBudget {
		t.Fatalf("oversized boundary timeout=%v, want default cap %v", got, defaultStallBudget)
	}
	ss.stallBudget = 0
	if got := ss.deliveryBoundaryTimeout(); got != defaultStallBudget {
		t.Fatalf("zero boundary timeout=%v, want default %v", got, defaultStallBudget)
	}
}

func TestAttemptBoundarySentinelCannotRaceConcurrentTerminalization(t *testing.T) {
	var ss subscriberSet
	ss.init("epoch-terminal-fence", testLogger())
	firstReal, _, _, unsubscribe := ss.SubscribeForStartup("startup-plex")
	defer unsubscribe()

	ss.fanout([]byte("validated-old-epoch"))
	select {
	case got := <-firstReal:
		if string(got.data) != "validated-old-epoch" {
			t.Fatalf("first startup media=%q", got.data)
		}
	case <-time.After(time.Second):
		t.Fatal("startup subscriber never validated the old epoch")
	}

	ss.mu.Lock()
	sub := ss.subs["startup-plex"]
	ss.mu.Unlock()
	if sub == nil {
		t.Fatal("startup subscriber was not admitted")
	}

	// Hold the exact per-subscriber point after clearRing owns the pump-wide
	// lifecycle fence but before it can publish the retry sentinel. A concurrent
	// terminal close must wait; otherwise the worker closes firstReal and the
	// later sentinel send panics.
	sub.epochSendMu.Lock()
	epochSendLocked := true
	t.Cleanup(func() {
		// All successful paths unlock below. This keeps a failed assertion from
		// stranding a goroutine and obscuring the test failure.
		if epochSendLocked {
			sub.epochSendMu.Unlock()
		}
	})
	clearResult := make(chan any, 1)
	go func() {
		defer func() { clearResult <- recover() }()
		ss.clearRing()
	}()
	deadline := time.Now().Add(time.Second)
	for ss.MediaEpoch() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ss.MediaEpoch() == 0 {
		sub.epochSendMu.Unlock()
		epochSendLocked = false
		t.Fatal("clearRing never reached the blocked sentinel handoff")
	}

	closeDone := make(chan struct{})
	go func() {
		ss.closeAll()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		sub.epochSendMu.Unlock()
		epochSendLocked = false
		t.Fatal("closeAll bypassed an in-flight attempt-boundary lifecycle fence")
	case <-time.After(25 * time.Millisecond):
	}

	sub.epochSendMu.Unlock()
	epochSendLocked = false
	select {
	case recovered := <-clearResult:
		if recovered != nil {
			t.Fatalf("clearRing panicked after concurrent terminalization: %v", recovered)
		}
	case <-time.After(time.Second):
		t.Fatal("clearRing did not finish after releasing the sentinel handoff")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("closeAll did not resume after clearRing released the lifecycle fence")
	}
}

func TestAttemptBoundaryStartupInvalidationCannotRaceUnsubscribeClose(t *testing.T) {
	var ss subscriberSet
	ss.init("epoch-unsubscribe-fence", testLogger())
	firstReal, _, _, unsubscribe := ss.SubscribeForStartup("departing-startup-plex")
	ss.fanout([]byte("validated-old-epoch"))
	select {
	case <-firstReal:
	case <-time.After(time.Second):
		t.Fatal("startup subscriber never validated the old epoch")
	}

	ss.mu.Lock()
	sub := ss.subs["departing-startup-plex"]
	ss.mu.Unlock()
	if sub == nil {
		t.Fatal("startup subscriber was not admitted")
	}

	// Force clearRing to snapshot the live subscriber and stop immediately
	// before epoch invalidation. Unsubscribe may now let the worker close every
	// public channel; because only that worker can publish the retry sentinel,
	// releasing the boundary cannot become send-on-closed-channel.
	sub.epochSendMu.Lock()
	epochSendLocked := true
	t.Cleanup(func() {
		if epochSendLocked {
			sub.epochSendMu.Unlock()
		}
		ss.closeAll()
	})
	clearResult := make(chan any, 1)
	go func() {
		defer func() { clearResult <- recover() }()
		ss.clearRing()
	}()
	deadline := time.Now().Add(time.Second)
	for ss.MediaEpoch() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ss.MediaEpoch() == 0 {
		t.Fatal("clearRing never reached the blocked subscriber handoff")
	}

	unsubscribe()
	select {
	case <-sub.workerDone:
	case <-time.After(time.Second):
		t.Fatal("unsubscribe did not let the delivery worker close its channels")
	}
	sub.epochSendMu.Unlock()
	epochSendLocked = false
	select {
	case recovered := <-clearResult:
		if recovered != nil {
			t.Fatalf("clearRing panicked after ordinary startup unsubscribe: %v", recovered)
		}
	case <-time.After(time.Second):
		t.Fatal("clearRing did not finish after departed startup subscriber")
	}
}

func TestAttemptBoundaryPreemptsActivelyDrainingOldQueue(t *testing.T) {
	var ss subscriberSet
	ss.init("epoch-priority", testLogger())
	ss.stallBudget = 2 * time.Second
	ch, unsubscribe := ss.Subscribe("active-plex")
	t.Cleanup(func() {
		unsubscribe()
		ss.closeAll()
	})

	// Build a deep old epoch before allowing the sink to drain. Once draining
	// starts, both queue and receiver are continuously ready—the exact shape in
	// which an ordinary Go select could starve an unbuffered reset case.
	for i := 0; i < 512; i++ {
		ss.fanout([]byte{0xa0, byte(i)})
	}
	type observation struct {
		data []byte
		at   time.Time
	}
	observed := make(chan observation, 1024)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case data := <-ch:
				observed <- observation{data: append([]byte(nil), data...), at: time.Now()}
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("active sink never began draining old epoch")
	}

	ss.clearRing()
	boundaryReturned := time.Now()
	fresh := []byte("fresh-after-priority-reset")
	ss.fanout(fresh)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case got := <-observed:
			if bytes.Equal(got.data, fresh) {
				return
			}
			if !got.at.Before(boundaryReturned) {
				t.Fatalf("old epoch %x reached active public sink after clearRing returned", got.data)
			}
		case <-deadline.C:
			t.Fatal("fresh epoch did not preempt active old queue")
		}
	}
}

func TestSubscribeAdmissionCannotRetainEpochFromBeforeClearRing(t *testing.T) {
	for _, startup := range []bool{false, true} {
		name := "ordinary"
		if startup {
			name = "startup"
		}
		t.Run(name, func(t *testing.T) {
			var ss subscriberSet
			ss.init("subscribe-epoch-race", testLogger())
			reached := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			ss.onSubscribeBeforeAdmission = func() {
				once.Do(func() { close(reached) })
				<-release
			}
			type subscription struct {
				ordinary <-chan []byte
				first    <-chan startupChunk
				startup  <-chan startupChunk
				cleanup  func()
			}
			ready := make(chan subscription, 1)
			go func() {
				if startup {
					first, chunks, _, cleanup := ss.SubscribeForStartup("racing-client")
					ready <- subscription{first: first, startup: chunks, cleanup: cleanup}
					return
				}
				chunks, cleanup := ss.Subscribe("racing-client")
				ready <- subscription{ordinary: chunks, cleanup: cleanup}
			}()
			select {
			case <-reached:
			case <-time.After(time.Second):
				t.Fatal("subscribe did not reach the pre-admission race barrier")
			}
			ss.clearRing()
			if ss.MediaEpoch() != 1 {
				t.Fatalf("media epoch=%d, want boundary epoch 1", ss.MediaEpoch())
			}
			close(release)
			var sub subscription
			select {
			case sub = <-ready:
			case <-time.After(time.Second):
				t.Fatal("subscribe did not complete after boundary")
			}
			t.Cleanup(func() {
				sub.cleanup()
				ss.closeAll()
			})
			fresh := []byte("new-epoch-after-admission-race")
			ss.fanout(fresh)
			if startup {
				select {
				case got := <-sub.first:
					if got.epoch != ss.MediaEpoch() || !bytes.Equal(got.data, fresh) {
						t.Fatalf("startup first chunk=(epoch=%d data=%q), want epoch=%d data=%q",
							got.epoch, got.data, ss.MediaEpoch(), fresh)
					}
				case <-time.After(time.Second):
					t.Fatal("startup subscriber dropped the new epoch after admission race")
				}
				return
			}
			select {
			case got := <-sub.ordinary:
				if !bytes.Equal(got, fresh) {
					t.Fatalf("ordinary new-epoch data=%q, want %q", got, fresh)
				}
			case <-time.After(time.Second):
				t.Fatal("ordinary subscriber dropped the new epoch after admission race")
			}
		})
	}
}

// S2 ownership: an unsubscribe while fanout is blocked on that subscriber
// must wake the pump immediately (done channel), not burn the stall budget,
// and must never panic with send-on-closed-channel.
func TestUnsubscribeWakesBlockedFanout(t *testing.T) {
	t.Parallel()
	srv, stopSrv := patternServer(t)
	defer stopSrv()

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	s.stallBudget = 30 * time.Second // huge: test fails by timeout if done isn't honored

	healthy, cancelHealthy := s.Subscribe("healthy")
	defer cancelHealthy()
	_, cancelStuck := s.Subscribe("stuck")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	// Wait for the stuck subscriber's buffer to fill so fanout blocks on it.
	time.Sleep(700 * time.Millisecond)
	start := time.Now()
	cancelStuck()

	// The healthy subscriber must resume receiving promptly — if the pump
	// were stuck waiting out a 30s budget this drain would time out.
	if err := drainSeqs(healthy, 200); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("pump stayed blocked %v after unsubscribe", elapsed)
	}

	cancel()
	<-done
}

// S1: when the upstream dies mid-stream with a subscriber attached, the pump
// must consult OnUpstreamDown, reconnect to the replacement URL, and keep the
// subscriber's channel open across the gap.
func TestReconnectAcrossUpstreamFailure(t *testing.T) {
	t.Parallel()
	srvA, stopA := patternServer(t)
	defer stopA()
	srvB, stopB := patternServer(t)
	defer stopB()

	s := NewStreamer("test", srvA.URL, testLogger(), nil)

	var failovers atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if cause == nil {
			t.Error("OnUpstreamDown got nil cause")
		}
		failovers.Add(1)
		return srvB.URL, true, nil
	}

	ch, unsub := s.Subscribe("viewer")
	defer unsub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan struct{})
	go func() { s.Run(ctx); close(exited) }()

	// Phase 1: receive from server A.
	if err := drainSeqs(ch, 50); err != nil {
		t.Fatal(err)
	}

	// Kill server A mid-stream.
	stopA()

	// Phase 2: the same channel must deliver server B's stream without ever
	// closing. Sequence numbers restart at 0 on the new upstream, so drain
	// fresh rather than asserting continuity across the boundary.
	deadline := time.After(15 * time.Second)
	gotPostFailover := 0
	for gotPostFailover < 50 {
		select {
		case <-deadline:
			t.Fatalf("only %d chunks after failover", gotPostFailover)
		case chunk, ok := <-ch:
			if !ok {
				t.Fatal("subscriber channel closed across the failover gap")
			}
			if failovers.Load() > 0 {
				gotPostFailover += len(chunk) / 8
			}
		}
	}
	if failovers.Load() < 1 {
		t.Fatal("OnUpstreamDown never fired")
	}
	// Server A delivered bytes and the pump kept going: exactly the splice
	// provenance DVR captures sample across their serve window.
	if got := s.UpstreamInterruptions(); got < 1 {
		t.Fatalf("UpstreamInterruptions=%d after byte-delivering upstream died mid-pump, want >=1", got)
	}

	cancel()
	<-exited
}

// S3: an upstream that sends headers + a trickle then goes silent must be
// killed by the no-bytes watchdog, not block forever.
func TestIdleUpstreamKilledByWatchdog(t *testing.T) {
	t.Parallel()
	hang := make(chan struct{})
	defer close(hang)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write([]byte("0123456789abcdef"))
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		select { // stall with the connection open
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	s.idleReadTimeout = 400 * time.Millisecond
	// The trickle reaches fanout, so the pump is media-ready and the live
	// stall tolerance governs; inject it too so the watchdog property stays
	// observable at test scale.
	s.stallTolerance = 400 * time.Millisecond

	var sawIdle atomic.Bool
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if errors.Is(cause, errUpstreamIdle) {
			sawIdle.Store(true)
		}
		return "", false, nil // stop after the first detection
	}

	var exitErr error
	exitCh := make(chan struct{})
	s.OnExit = func(err error) { exitErr = err; close(exitCh) }

	ch, unsub := s.Subscribe("viewer")
	defer unsub()
	go func() {
		for range ch { // drain so fanout never stalls
		}
	}()

	go s.Run(context.Background())

	select {
	case <-exitCh:
	case <-time.After(10 * time.Second):
		t.Fatal("pump did not exit — watchdog never fired")
	}
	if !sawIdle.Load() {
		t.Fatalf("OnUpstreamDown cause was not errUpstreamIdle (exit err: %v)", exitErr)
	}
	if exitErr == nil {
		t.Fatal("OnExit got nil err for an idle-killed upstream")
	}
}

func TestConnectedVideoOnlySourceFailsOverInsideClientBudgetAndKeepsSharedPump(t *testing.T) {
	const (
		pmtOne, videoOne, audioOne       = 0x1000, 0x0100, 0x0101
		pmtTwo, videoTwo, audioTwo       = 0x1001, 0x0110, 0x0111
		pmtSlate, videoSlate, audioSlate = 0x1002, 0x0120, 0x0121
	)
	sourceOneMedia := testAVLivePrefix(pmtOne, videoOne, audioOne, 70*transportPTSRate, minPrefixDecisionBytes+tsPacketSize)
	sourceTwoMedia := testAVLivePrefix(pmtTwo, videoTwo, audioTwo, 0, minPrefixDecisionBytes+tsPacketSize)
	slateMedia := testAVLivePrefix(pmtSlate, videoSlate, audioSlate, 0, 0)

	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(sourceOneMedia)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Keep the connection, HTTP handler, and timestamped video progressing
		// while audio stops. Whole-input liveness therefore remains healthy; only
		// the established elementary-clock guard can recover this exact reported
		// failure mode.
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		pts := uint64(71) * transportPTSRate
		continuity := byte(8)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := w.Write(joinTSPackets(
					testTimestampedPESPacket(videoOne, 0xe0, pts, continuity),
					pcrTestNonPCRPacket(0x1fff, false))); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				pts += transportPTSRate / 10
				continuity = (continuity + 1) & 0x0f
			}
		}
	}))
	defer sourceOne.Close()

	var sourceTwoRequests atomic.Int32
	sourceTwo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceTwoRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		for {
			if _, err := w.Write(sourceTwoMedia); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}))
	defer sourceTwo.Close()

	s := NewStreamer("shared-plex-silence", sourceOne.URL, testLogger(), nil)
	s.idleReadTimeout = 2 * time.Second
	s.sourceStartupTimeout = 400 * time.Millisecond
	s.avContinuityPolicy = avContinuityPolicy{
		starvationThreshold: 120 * time.Millisecond,
		driftThreshold:      5 * time.Second,
		skewThreshold:       5 * time.Second,
		confirmation:        40 * time.Millisecond,
		minSamples:          4,
	}
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: minPrefixDecisionBytes, mediaDuration: time.Second,
	}
	// The fixture contains eight timestamped 100 ms samples (0 through 700 ms).
	s.SlateFn = func() *slate { return &slate{data: slateMedia, duration: 800 * time.Millisecond} }
	var failovers atomic.Int32
	continuityCause := make(chan *avContinuityError, 1)
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if errors.Is(cause, errUpstreamIdle) {
			return "", false, errors.New("whole-input idle watchdog incorrectly triggered")
		}
		var continuityErr *avContinuityError
		if !errors.As(cause, &continuityErr) || continuityErr.kind != avFaultAudioStarvation {
			return "", false, fmt.Errorf("source-one cause=%v, want audio starvation", cause)
		}
		continuityCause <- continuityErr
		failovers.Add(1)
		return sourceTwo.URL, true, nil
	}

	viewer, leaveViewer := s.Subscribe("plex-viewer")
	defer leaveViewer()
	recorder, leaveRecorder := s.Subscribe("shared-recorder")
	defer leaveRecorder()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s.OnExit = func(err error) { done <- err }
	go s.Run(ctx)

	type observation struct {
		name               string
		err                error
		sawSlate           bool
		sourceTwoAudioPTS  uint64
		sourceTwoAudioDisc bool
		sourceTwoVideoDisc bool
		maxGap             time.Duration
	}
	observe := func(name string, chunks <-chan []byte) <-chan observation {
		out := make(chan observation, 1)
		go func() {
			result := observation{name: name}
			var media []byte
			scan := 0
			lastChunk := time.Now()
			silence := time.NewTimer(350 * time.Millisecond)
			defer silence.Stop()
			for {
				select {
				case chunk, ok := <-chunks:
					now := time.Now()
					if gap := now.Sub(lastChunk); gap > result.maxGap {
						result.maxGap = gap
					}
					lastChunk = now
					if !silence.Stop() {
						select {
						case <-silence.C:
						default:
						}
					}
					silence.Reset(350 * time.Millisecond)
					if !ok {
						result.err = errors.New("shared subscriber closed during recovery")
						out <- result
						return
					}
					media = append(media, chunk...)
					for scan+tsPacketSize <= len(media) {
						packet := media[scan : scan+tsPacketSize]
						scan += tsPacketSize
						if packet[0] != 0x47 {
							result.err = errors.New("relay lost MPEG-TS alignment across attempt boundary")
							out <- result
							return
						}
						pid := int(packet[1]&0x1f)<<8 | int(packet[2])
						if pid == audioSlate || pid == videoSlate {
							result.sawSlate = true
						}
						discontinuity := packet[4] >= 1 && packet[5]&0x80 != 0
						switch pid {
						case videoTwo:
							result.sourceTwoVideoDisc = result.sourceTwoVideoDisc || discontinuity
						case audioTwo:
							result.sourceTwoAudioDisc = result.sourceTwoAudioDisc || discontinuity
							if payload, ok := tsPayload(packet); ok {
								if pts, ok := parsePESPTS(payload); ok {
									result.sourceTwoAudioPTS = pts
									if result.sawSlate && result.sourceTwoVideoDisc && result.sourceTwoAudioDisc {
										out <- result
										return
									}
								}
							}
						}
					}
				case <-silence.C:
					result.err = errors.New("client survival budget expired without decodable bytes")
					out <- result
					return
				}
			}
		}()
		return out
	}

	viewerResult, recorderResult := observe("viewer", viewer), observe("recorder", recorder)
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	results := make([]observation, 0, 2)
	for len(results) < 2 {
		select {
		case result := <-viewerResult:
			results = append(results, result)
			viewerResult = nil
		case result := <-recorderResult:
			results = append(results, result)
			recorderResult = nil
		case <-deadline.C:
			cancel()
			t.Fatal("shared pump did not deliver the alternate inside scaled Plex budget")
		}
	}
	if got := s.SubscriberCount(); got != 2 {
		cancel()
		t.Fatalf("shared pump subscriber count=%d, want both clients still attached", got)
	}
	for _, result := range results {
		if result.err != nil {
			cancel()
			t.Fatalf("%s observation failed (max gap %s): %v", result.name, result.maxGap, result.err)
		}
		if !result.sawSlate {
			cancel()
			t.Fatalf("%s saw no immediate pre-rendered continuity slate", result.name)
		}
		if !result.sourceTwoVideoDisc || !result.sourceTwoAudioDisc {
			cancel()
			t.Fatalf("%s alternate lacked A/V discontinuity: video=%v audio=%v",
				result.name, result.sourceTwoVideoDisc, result.sourceTwoAudioDisc)
		}
		if result.sourceTwoAudioPTS != 0 {
			cancel()
			t.Fatalf("%s alternate audio PTS=%d, want source reset preserved at zero",
				result.name, result.sourceTwoAudioPTS)
		}
	}
	if failovers.Load() != 1 || sourceTwoRequests.Load() != 1 {
		cancel()
		t.Fatalf("bounded failover callbacks=%d source-two requests=%d, want 1/1",
			failovers.Load(), sourceTwoRequests.Load())
	}
	select {
	case fault := <-continuityCause:
		if fault.kind != avFaultAudioStarvation {
			cancel()
			t.Fatalf("continuity fault=%v, want %s", fault, avFaultAudioStarvation)
		}
	default:
		cancel()
		t.Fatal("audio-starvation cause was not preserved through failover callback")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shared pump exit after test cancellation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shared pump did not stop after cancellation")
	}
}

func TestStreamerFanoutMarksLateSelectedAudioAndDistinctPCR(t *testing.T) {
	const (
		pmtPID   = 0x1000
		pcrPID   = 0x0102
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefixPackets := [][]byte{
		testPATPacket(pmtPID),
		testAVPMTPacketWithPCR(pmtPID, videoPID, audioPID, pcrPID),
	}
	videoCC := byte(0)
	videoPTS := uint64(0)
	for len(prefixPackets)*tsPacketSize < minPrefixDecisionBytes+2*tsPacketSize {
		prefixPackets = append(prefixPackets,
			testTimestampedPESPacket(videoPID, 0xe0, videoPTS, videoCC),
			pcrTestNonPCRPacket(0x1ffe, false))
		videoCC = (videoCC + 1) & 0x0f
		videoPTS += transportPTSRate / 25
	}
	prefix := joinTSPackets(prefixPackets...)
	lateAudio := testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, videoPTS, 7)
	declareSyntheticTimestampPESLengths(prefix, lateAudio)
	latePCR := pcrTestChunkForPID(pcrPID, 0, false)[:tsPacketSize]
	latePCR[3] = latePCR[3]&0xf0 | 9

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		if _, err := w.Write(prefix); err != nil {
			return
		}
		flusher, _ := w.(http.Flusher)
		if flusher != nil {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
		if _, err := w.Write(joinTSPackets(lateAudio, latePCR)); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewStreamer("late-selected-pids", srv.URL, testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	viewer, unsubscribe := s.Subscribe("plex-after-slate")
	defer unsubscribe()
	// Model a continuity-slate audio packet already consumed by this decoder.
	// The new source does not expose its declared audio in the private prefix.
	priorSlateAudio := testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, 50*transportPTSRate, 3)
	s.fanout(priorSlateAudio)
	select {
	case got := <-viewer:
		if !bytes.Equal(got, priorSlateAudio) {
			t.Fatal("failed to establish the decoder's prior slate audio epoch")
		}
	case <-time.After(time.Second):
		t.Fatal("prior slate audio was not delivered before source startup")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s.OnExit = func(err error) { done <- err }
	go s.Run(ctx)

	var buffered []byte
	seenSourceVideo := false
	expectImmediatePID := -1
	audioState, pcrState := 0, 0
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for audioState < 2 || pcrState < 2 {
		select {
		case chunk, ok := <-viewer:
			if !ok {
				cancel()
				t.Fatal("viewer closed before late selected PIDs were published")
			}
			buffered = append(buffered, chunk...)
			for len(buffered) >= tsPacketSize {
				packet := append([]byte(nil), buffered[:tsPacketSize]...)
				buffered = buffered[tsPacketSize:]
				if packet[0] != 0x47 {
					cancel()
					t.Fatal("fanout lost TS alignment while inserting late PID marker")
				}
				pid := int(packet[1]&0x1f)<<8 | int(packet[2])
				if expectImmediatePID >= 0 && pid != expectImmediatePID {
					cancel()
					t.Fatalf("PID %#x marker was separated from its first packet by PID %#x", expectImmediatePID, pid)
				}
				switch pid {
				case audioPID:
					if audioState == 0 {
						if !tsPacketHasDiscontinuity(packet) || (packet[3]>>4)&0x03 != 2 ||
							!seenSourceVideo || packet[3]&0x0f != lateAudio[3]&0x0f {
							cancel()
							t.Fatalf("late audio marker was not adjacent with matching CC: %x", packet[:6])
						}
						audioState = 1
						expectImmediatePID = audioPID
					} else if audioState == 1 {
						if !bytes.Equal(packet, lateAudio) {
							cancel()
							t.Fatal("late audio payload changed before fanout")
						}
						audioState = 2
						expectImmediatePID = -1
					}
				case pcrPID:
					if pcrState == 0 {
						if !tsPacketHasDiscontinuity(packet) || (packet[3]>>4)&0x03 != 2 ||
							!seenSourceVideo || packet[3]&0x0f != latePCR[3]&0x0f {
							cancel()
							t.Fatalf("distinct PCR marker was not adjacent with matching CC: %x", packet[:6])
						}
						pcrState = 1
						expectImmediatePID = pcrPID
					} else if pcrState == 1 {
						if !bytes.Equal(packet, latePCR) {
							cancel()
							t.Fatal("late distinct PCR packet changed before fanout")
						}
						pcrState = 2
						expectImmediatePID = -1
					}
				}
				if pid == videoPID && (packet[3]>>4)&0x03 != 2 {
					seenSourceVideo = true
				}
			}
		case <-deadline.C:
			cancel()
			t.Fatalf("late selected PID fanout incomplete: audio=%d PCR=%d", audioState, pcrState)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("late selected PID streamer exit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late selected PID streamer did not stop")
	}
}

func TestStreamerWithholdsExactCorruptFeedBeforeAlternateFanout(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	badPTS := uint64(4 * 60 * 60 * transportPTSRate)
	alternatePTS := uint64(10 * transportPTSRate)
	firstPrefix, _, _, firstAudioCC := testAVPrefixWithMapAt(
		testPATPacket(pmtPID), testAVPMTPacket(pmtPID, videoPID, audioPID),
		videoPID, audioPID, 0)
	alternatePrefix, _, _, _ := testAVPrefixWithMapAt(
		testPATPacket(pmtPID), testAVPMTPacket(pmtPID, videoPID, audioPID),
		videoPID, audioPID, alternatePTS)
	badThenSane := joinTSPackets(
		testTimestampedPESPacket(audioPID, 0xc0, badPTS, firstAudioCC),
		testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, (firstAudioCC+1)&0x0f))

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := requests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		prefix := firstPrefix
		if request > 1 {
			prefix = alternatePrefix
		}
		if _, err := w.Write(prefix); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		if request == 1 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			_, _ = w.Write(badThenSane) // one exact/coalesced bad+sane result
			if flusher != nil {
				flusher.Flush()
			}
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	validator := &thresholdPrefixValidator{
		readyAt: minPrefixDecisionBytes, mediaDuration: time.Second,
		videoPID: videoPID, audioCount: 1, pcrPID: videoPID,
		havePCRPID: true, haveProgram: true,
	}
	for i := range validator.audioPIDs {
		validator.audioPIDs[i] = -1
	}
	validator.audioPIDs[0] = audioPID
	s := NewStreamer("exact-corrupt-feed", srv.URL, testLogger(), nil)
	s.prefixValidator = validator
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 2 * time.Second
	var relocations atomic.Int32
	causeSeen := make(chan struct{}, 1)
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		var continuityErr *avContinuityError
		if !errors.As(cause, &continuityErr) || continuityErr.kind != avFaultTransport {
			return "", false, fmt.Errorf("corrupt-feed cause=%v, want transport corruption", cause)
		}
		relocations.Add(1)
		causeSeen <- struct{}{}
		return srv.URL, true, nil
	}

	viewer, unsubscribe := s.Subscribe("plex-corrupt-feed")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s.OnExit = func(err error) { done <- err }
	go s.Run(ctx)

	var received []byte
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for !testStreamContainsPTS(received, audioPID, alternatePTS) {
		select {
		case chunk, ok := <-viewer:
			if !ok {
				cancel()
				t.Fatal("viewer closed before alternate media")
			}
			received = append(received, chunk...)
		case <-deadline.C:
			cancel()
			t.Fatalf("alternate not delivered: requests=%d relocations=%d", requests.Load(), relocations.Load())
		}
	}
	if testStreamContainsPTS(received, audioPID, badPTS) {
		cancel()
		t.Fatal("implausible PTS packet reached the subscriber before relocation")
	}
	if relocations.Load() != 1 || requests.Load() < 2 {
		cancel()
		t.Fatalf("relocations=%d requests=%d, want one provider relocation and alternate attempt",
			relocations.Load(), requests.Load())
	}
	select {
	case <-causeSeen:
	default:
		cancel()
		t.Fatal("transport-corruption cause was not preserved")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streamer did not stop after exact corrupt-feed test")
	}
}

func TestStreamerProgramMapChangeReentersPrivateGateBeforeNewMedia(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		oldAudio = 0x0101
		newAudio = 0x0102
	)
	oldPMT := testAVPMTPacket(pmtPID, videoPID, oldAudio)
	payload, _ := tsPayload(oldPMT)
	section := append([]byte(nil), payload[1+int(payload[0]):]...)
	total := 3 + (int(section[1]&0x0f)<<8 | int(section[2]))
	section = section[:total]
	section[5] = section[5]&0xc1 | 1<<1
	section[18], section[19] = 0xe0|byte(newAudio>>8), byte(newAudio&0xff)
	writeMPEG2CRC(section)
	newPMT := testExactPayloadPacket(pmtPID, true, 0, append([]byte{0}, section...))
	changedPMT := testExactPayloadPacket(pmtPID, true, 1, append([]byte{0}, section...))
	oldPrefix, _, _, _ := testAVPrefixWithMapAt(
		testPATPacket(pmtPID), oldPMT, videoPID, oldAudio, 0)
	newPTS := uint64(10 * transportPTSRate)
	newPrefix, _, _, _ := testAVPrefixWithMapAt(
		testPATPacket(pmtPID), newPMT, videoPID, newAudio, newPTS)
	rejectedPTS := uint64(4 * 60 * 60 * transportPTSRate)
	changedFeed := joinTSPackets(
		changedPMT,
		testTimestampedPESPacket(newAudio, 0xc0, rejectedPTS, 0))

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := requests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		prefix := oldPrefix
		if request > 1 {
			prefix = newPrefix
		}
		if _, err := w.Write(prefix); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		if request == 1 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			_, _ = w.Write(changedFeed)
			if flusher != nil {
				flusher.Flush()
			}
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewStreamer("programme-map-boundary", srv.URL, testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = 2 * time.Second
	var upstreamFailures atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		upstreamFailures.Add(1)
		return "", true, nil
	}
	viewer, unsubscribe := s.Subscribe("plex-programme-transition")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	s.OnExit = func(err error) { done <- err }
	go s.Run(ctx)

	var received []byte
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for !testStreamContainsPTS(received, newAudio, newPTS) {
		select {
		case chunk, ok := <-viewer:
			if !ok {
				cancel()
				t.Fatal("viewer closed during programme-map restart")
			}
			received = append(received, chunk...)
		case <-deadline.C:
			cancel()
			t.Fatalf("fresh changed programme never passed private gate: requests=%d", requests.Load())
		}
	}
	if testStreamContainsPTS(received, newAudio, rejectedPTS) {
		cancel()
		t.Fatal("post-change media escaped before the changed programme was privately revalidated")
	}
	if upstreamFailures.Load() != 0 || requests.Load() < 2 {
		cancel()
		t.Fatalf("programme-map restart attribution: provider callbacks=%d requests=%d, want local retry",
			upstreamFailures.Load(), requests.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streamer did not stop after programme-map boundary test")
	}
}

func testAVLivePrefix(pmtPID, videoPID, audioPID int, basePTS uint64, minimum int) []byte {
	packets := [][]byte{testPATPacket(pmtPID), testAVPMTPacket(pmtPID, videoPID, audioPID)}
	for i := 0; i < 8; i++ {
		pts := basePTS + uint64(i)*transportPTSRate/10
		packets = append(packets,
			testTimestampedPESPacket(videoPID, 0xe0, pts, byte(i)),
			testTimestampedPESPacket(audioPID, 0xc0, pts, byte(i)))
	}
	media := joinTSPackets(packets...)
	for len(media) < minimum {
		media = append(media, pcrTestNonPCRPacket(0x1ffe, false)...)
	}
	return media
}

func TestFiniteReplayDrainsPCRPacedTailPastIdleWatchdog(t *testing.T) {
	const (
		groups       = 20
		packetsGroup = 350
	)
	payload := make([]byte, 0, groups*packetsGroup*tsPacketSize)
	for group := 0; group < groups; group++ {
		payload = append(payload, pcrTestChunk(
			durationTicks(time.Duration(group)*50*time.Millisecond, transportClockRate), false)...)
		for packet := transportSyncPacketCount; packet < packetsGroup; packet++ {
			payload = append(payload, pcrTestNonPCRPacket(0x1fff, false)...)
		}
	}
	if int64(len(payload)) < minFinitePreflightBytes {
		t.Fatalf("finite PCR fixture=%d bytes, want >=%d", len(payload), minFinitePreflightBytes)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	s := NewStreamer("finite-paced-tail", srv.URL, testLogger(), nil)
	s.classifier = &staticFiniteClassifier{result: classificationResult{
		kind: classificationFiniteAccepted, reason: "test_finite_live_media",
	}}
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: minPrefixDecisionBytes, mediaDuration: time.Second,
		pcrPID: 0x100, havePCRPID: true,
	}
	s.idleReadTimeout = 250 * time.Millisecond
	viewer, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()

	var received atomic.Int64
	allDelivered := make(chan struct{})
	var delivered atomic.Bool
	go func() {
		for chunk := range viewer {
			if received.Add(int64(len(chunk))) == int64(len(payload)) && delivered.CompareAndSwap(false, true) {
				close(allDelivered)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := time.Now()
	gotBytes, runErr := s.runOnce(ctx)
	elapsed := time.Since(started)
	if runErr != nil {
		t.Fatalf("finite paced replay failed after %s: %v", elapsed, runErr)
	}
	if !gotBytes {
		t.Fatal("finite paced replay published no media")
	}
	if elapsed <= s.idleReadTimeout*2 {
		t.Fatalf("fixture drained in %s, not long enough to exercise %s idle watchdog", elapsed, s.idleReadTimeout)
	}
	select {
	case <-allDelivered:
	case <-time.After(time.Second):
		t.Fatalf("paced finite tail delivered %d/%d bytes", received.Load(), len(payload))
	}
	s.closeAll()
}

func TestLocalPrefixValidationTimeoutRelocatesWithoutSourceBlame(t *testing.T) {
	sourceOne := markerStreamServer(t, 0xaa)
	defer sourceOne.Close()
	sourceTwo := markerStreamServer(t, 0xbc)
	defer sourceTwo.Close()

	s := NewStreamer("local-prefix-timeout", sourceOne.URL, testLogger(), nil)
	s.prefixValidator = cancelFirstMarkerPrefixValidator{}
	s.sourceStartupTimeout = 350 * time.Millisecond
	s.initialStartupDeadline = time.Now().Add(2 * time.Second)
	var failovers atomic.Int32
	causeCh := make(chan error, 1)
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		if failovers.Add(1) == 1 {
			causeCh <- cause
		}
		return sourceTwo.URL, true, nil
	}

	viewer, unsubscribe := s.Subscribe("viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	select {
	case chunk, ok := <-viewer:
		if !ok || len(chunk) == 0 {
			cancel()
			t.Fatal("subscriber closed before validated alternate media")
		}
		if chunk[0] != 0xbc {
			cancel()
			t.Fatalf("unvalidated source-one prefix leaked marker %#x", chunk[0])
		}
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("local validation timeout did not try the alternate inside startup budget")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("streamer did not exit after cancellation")
	}
	var observed error
	select {
	case observed = <-causeCh:
	default:
		t.Fatal("local validation relocation cause was not captured")
	}
	if failovers.Load() != 1 {
		t.Fatalf("failovers=%d, want one bounded alternate try", failovers.Load())
	}
	if !errors.Is(observed, errUncertainMediaPrefix) {
		t.Fatalf("relocation cause=%v, want neutral media-prefix uncertainty", observed)
	}
	if shouldMarkSourceFailure(observed) {
		t.Fatalf("local validation cancellation would decay provider health: %v", observed)
	}
}

func TestLocalRelayBacklogsRetrySameSourceWithoutHealthDecay(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "read-ahead capacity", err: errRelayReadAheadFull},
		{name: "stale source-arrival age", err: errTransportStaleChunk},
		{name: "transport clock boundary", err: errTransportAttemptBoundary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("transport relay: %w", tc.err)
			if !isLocalRelayRetry(wrapped) {
				t.Fatalf("%v would invoke provider relocation instead of reopening the same source", wrapped)
			}
			if shouldMarkSourceFailure(wrapped) {
				t.Fatalf("%v would decay provider health", wrapped)
			}
		})
	}
	if isLocalRelayRetry(errTransportRecoveryBacklog) {
		t.Fatal("fresh-arrival provider recovery backlog was classified as a local retry")
	}
	if !shouldMarkSourceFailure(errTransportRecoveryBacklog) {
		t.Fatal("direct provider recovery backlog lost provider attribution")
	}
	if isLocalRelayRetry(errTransportRelayBacklog) {
		t.Fatal("deterministic private-prefix backlog would retry identical source")
	}
	if shouldMarkSourceFailure(errTransportRelayBacklog) {
		t.Fatal("private-prefix relocation would decay provider health")
	}
	if !requiresImmediateContinuityFiller(fmt.Errorf("pace: %w", errTransportAttemptBoundary)) {
		t.Fatal("clock-boundary attempt restart would wait the ordinary slate delay")
	}
}

func TestStreamerStartsPacingAtValidatedPrefixRelease(t *testing.T) {
	srv := delayedPrefixStreamServer(t, maxTransportAddedLatency+100*time.Millisecond)
	s := NewStreamer("stale-prefix", srv.URL, testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: delayedPrefixChunkCount * chunkSize, mediaDuration: 1200 * time.Millisecond,
		pcrPID: 0x100, havePCRPID: true, delay: 450 * time.Millisecond,
	}
	s.sourceStartupTimeout = 3 * time.Second
	_, unsubscribe := s.Subscribe("prefix-release-test")
	defer unsubscribe()
	gotBytes, err := s.runOnce(context.Background())
	if err != nil {
		t.Fatalf("valid private prefix failed after provider pause: %v", err)
	}
	if !gotBytes {
		t.Fatal("validated private prefix published no media")
	}
	if got := s.bytesOut.Load(); got != delayedPrefixChunkCount*chunkSize {
		t.Fatalf("validated prefix bytes=%d, want %d", got, delayedPrefixChunkCount*chunkSize)
	}
}

func TestStreamerPacesPrefixAfterSlowOnRunning(t *testing.T) {
	srv := delayedPrefixLiveStreamServer(t, 0)
	s := NewStreamer("slow-running-callback", srv.URL, testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: delayedLivePrefixChunkCount * chunkSize, mediaDuration: 1600 * time.Millisecond,
		pcrPID: 0x100, havePCRPID: true,
	}
	// This regression targets the post-validation release edge, not the
	// production startup budget. Leave headroom for race/CI instrumentation to
	// move 2 MiB through the deterministic live fixture before validation.
	s.sourceStartupTimeout = 5 * time.Second
	s.OnRunning = func() { time.Sleep(maxTransportAddedLatency + 50*time.Millisecond) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		delivered int
		fanoutAt  []time.Time
	)
	s.OnBytes = func(n int) error {
		fanoutAt = append(fanoutAt, time.Now())
		delivered += n
		if delivered == delayedLivePrefixChunkCount*chunkSize {
			cancel()
		}
		return nil
	}
	_, unsubscribe := s.Subscribe("slow-running-callback-test")
	defer unsubscribe()
	gotBytes, err := s.runOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("slow OnRunning prefix failed: %v", err)
	}
	if !gotBytes || s.bytesOut.Load() != delayedLivePrefixChunkCount*chunkSize {
		t.Fatalf("slow OnRunning gotBytes=%v bytes=%d, want %d",
			gotBytes, s.bytesOut.Load(), delayedLivePrefixChunkCount*chunkSize)
	}
	if len(fanoutAt) < 2 {
		t.Fatalf("slow OnRunning fanout callbacks=%d, want at least 2", len(fanoutAt))
	}
	if interval := fanoutAt[1].Sub(fanoutAt[0]); interval < 15*time.Millisecond {
		t.Fatalf("first two prefix chunks were %s apart; callback time replaced pacing cadence", interval)
	}
}

func TestStreamerHoldsStartupLeadBeforeFirstFanout(t *testing.T) {
	run := func(t *testing.T, lead time.Duration, deadline time.Time) (runningAt, firstFanout time.Time) {
		t.Helper()
		srv := delayedPrefixLiveStreamServer(t, 0)
		s := NewStreamer("startup-lead", srv.URL, testLogger(), nil)
		s.prefixValidator = &thresholdPrefixValidator{
			readyAt: delayedLivePrefixChunkCount * chunkSize, mediaDuration: 1600 * time.Millisecond,
			pcrPID: 0x100, havePCRPID: true,
		}
		s.sourceStartupTimeout = 5 * time.Second
		s.startupLead = lead
		s.startupHoldDeadline = deadline
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.OnRunning = func() { runningAt = time.Now() }
		s.OnBytes = func(int) error {
			if firstFanout.IsZero() {
				firstFanout = time.Now()
				cancel()
			}
			return nil
		}
		_, unsubscribe := s.Subscribe("startup-lead-test")
		defer unsubscribe()
		if _, err := s.runOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("startup lead run: %v", err)
		}
		if runningAt.IsZero() || firstFanout.IsZero() {
			t.Fatalf("running=%v fanout=%v, want both", runningAt, firstFanout)
		}
		return runningAt, firstFanout
	}

	t.Run("holds the validated prefix for the configured lead", func(t *testing.T) {
		runningAt, firstFanout := run(t, 300*time.Millisecond, time.Time{})
		if held := firstFanout.Sub(runningAt); held < 300*time.Millisecond {
			t.Fatalf("first fanout %s after running, want >= 300ms startup lead", held)
		}
	})

	t.Run("never spends the client's startup budget below the reserve", func(t *testing.T) {
		deadline := time.Now().Add(startupLeadDeadlineReserve + 50*time.Millisecond)
		runningAt, firstFanout := run(t, 2*time.Second, deadline)
		if held := firstFanout.Sub(runningAt); held > 400*time.Millisecond {
			t.Fatalf("first fanout %s after running, want the hold clipped by the client deadline", held)
		}
	})

	t.Run("zero lead publishes at once", func(t *testing.T) {
		runningAt, firstFanout := run(t, 0, time.Time{})
		if held := firstFanout.Sub(runningAt); held > 200*time.Millisecond {
			t.Fatalf("first fanout %s after running, want immediate", held)
		}
	})
}

func TestStreamerReleasesResultsQueuedDuringSlowOnRunningAtPrivateEdge(t *testing.T) {
	chunks := burstyAVPCRTestChunks(4, 25*time.Millisecond)
	rawBytes := len(chunks) * chunkSize
	tail := pcrTestNonPCRPacket(0x1ffe, false)[rawBytes%tsPacketSize:]
	chunks = append(chunks, tail)
	declareSyntheticTimestampPESLengths(chunks...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range chunks {
			_, _ = w.Write(chunk)
			if flusher != nil {
				flusher.Flush()
			}
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewStreamer("queued-slow-running", srv.URL, testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
	s.sourceStartupTimeout = 5 * time.Second
	s.OnRunning = func() { time.Sleep(maxTransportAddedLatency + 50*time.Millisecond) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	want := rawBytes + len(tail) + 2*tsPacketSize // initial video/audio markers
	delivered := 0
	s.OnBytes = func(n int) error {
		delivered += n
		if delivered >= want {
			cancel()
		}
		return nil
	}
	_, unsubscribe := s.Subscribe("queued-slow-running-viewer")
	defer unsubscribe()
	gotBytes, err := s.runOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("queued private-release media failed: %v", err)
	}
	if !gotBytes || delivered != want || s.bytesOut.Load() != int64(want) {
		t.Fatalf("queued private release got=%v delivered=%d bytes=%d want=%d err=%v",
			gotBytes, delivered, s.bytesOut.Load(), want, err)
	}
	if errors.Is(err, errTransportStaleChunk) ||
		(!errors.Is(err, context.Canceled) && shouldMarkSourceFailure(err)) {
		t.Fatalf("local private callback delay was attributed to provider: %v", err)
	}
}

func TestStreamerReleasesResultsQueuedDuringSlowValidationAtPrivateEdge(t *testing.T) {
	chunks := burstyAVPCRTestChunks(4, 25*time.Millisecond)
	rawBytes := len(chunks) * chunkSize
	tail := pcrTestNonPCRPacket(0x1ffe, false)[rawBytes%tsPacketSize:]
	chunks = append(chunks, tail)
	declareSyntheticTimestampPESLengths(chunks...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range chunks {
			_, _ = w.Write(chunk)
			if flusher != nil {
				flusher.Flush()
			}
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewStreamer("queued-slow-validation", srv.URL, testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{
		readyAt: chunkSize,
		// While the first result is synchronously private here, the reservoir
		// accepts the remaining burst. Those source arrivals are older than the
		// stale-media ceiling when validation returns.
		delay: maxTransportAddedLatency + 50*time.Millisecond,
	}
	s.sourceStartupTimeout = 5 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	want := rawBytes + len(tail) + 2*tsPacketSize // initial video/audio markers
	delivered := 0
	s.OnBytes = func(n int) error {
		delivered += n
		if delivered >= want {
			cancel()
		}
		return nil
	}
	_, unsubscribe := s.Subscribe("queued-slow-validation-viewer")
	defer unsubscribe()
	gotBytes, err := s.runOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("queued slow-validation media failed: %v", err)
	}
	if !gotBytes || delivered != want || s.bytesOut.Load() != int64(want) {
		t.Fatalf("queued validation release got=%v delivered=%d bytes=%d want=%d err=%v",
			gotBytes, delivered, s.bytesOut.Load(), want, err)
	}
	if errors.Is(err, errTransportStaleChunk) || errors.Is(err, errTransportRecoveryBacklog) {
		t.Fatalf("pre-release queued tail retained stale source age: %v", err)
	}
}

// S3: cancelling the pump context must stop a wedged upstream read promptly
// and report a clean (nil) exit — cancellation is not a source failure.
func TestCancelStopsWedgedPump(t *testing.T) {
	t.Parallel()
	hang := make(chan struct{})
	defer close(hang)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write([]byte("0123456789abcdef"))
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	// Watchdog long enough that only ctx cancel can be the cause of exit.
	s.idleReadTimeout = 5 * time.Minute

	exitErr := errors.New("sentinel: OnExit not called")
	exitCh := make(chan struct{})
	s.OnExit = func(err error) { exitErr = err; close(exitCh) }

	ch, unsub := s.Subscribe("viewer")
	defer unsub()
	go func() {
		for range ch {
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)

	time.Sleep(300 * time.Millisecond) // let it connect and wedge
	start := time.Now()
	cancel()

	select {
	case <-exitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("pump did not exit after ctx cancel")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("pump took %v to exit after cancel", elapsed)
	}
	if exitErr != nil {
		t.Fatalf("OnExit err = %v, want nil for ctx cancel", exitErr)
	}
}

// S1: when every reconnect attempt fails, the pump gives up after the
// reconnect window and closes subscribers (clients re-tune) instead of
// hanging forever.
func TestGiveUpAfterReconnectWindow(t *testing.T) {
	t.Parallel()
	srv, stopSrv := patternServer(t)
	defer stopSrv()

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	s.reconnectWindow = 1200 * time.Millisecond
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		return "", true, nil // keep retrying the (dead) URL
	}

	var exitErr error
	exitCh := make(chan struct{})
	s.OnExit = func(err error) { exitErr = err; close(exitCh) }

	ch, unsub := s.Subscribe("viewer")
	defer unsub()

	go s.Run(context.Background())
	if err := drainSeqs(ch, 20); err != nil { // healthy first
		t.Fatal(err)
	}
	stopSrv() // and the upstream never comes back

	go func() {
		for range ch { // drain until close
		}
	}()

	select {
	case <-exitCh:
	case <-time.After(20 * time.Second):
		t.Fatal("pump never gave up")
	}
	if exitErr == nil {
		t.Fatal("OnExit err = nil, want give-up error")
	}
	if !errors.Is(s.TerminalError(), exitErr) {
		t.Fatalf("TerminalError = %v, want %v", s.TerminalError(), exitErr)
	}
}

// A 200 response that emits a short fragment before EOF is not a recovered
// live stream. Each fragment used to reset downSince, so the pump retried
// forever and the Pool repeatedly forgot which sources had already failed.
func TestFragmentedEOFDoesNotResetReconnectWindow(t *testing.T) {
	t.Parallel()
	srv, requests := fragmentEOFServer(t, 20*time.Millisecond)

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	s.stableDuration = 150 * time.Millisecond
	s.reconnectWindow = 900 * time.Millisecond
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		return "", true, nil
	}
	var stable atomic.Int32
	s.OnStable = func() { stable.Add(1) }

	viewer, unsub := s.Subscribe("viewer")
	defer unsub()
	go func() {
		for range viewer {
		}
	}()

	var exitErr error
	exited := make(chan struct{})
	s.OnExit = func(err error) {
		exitErr = err
		close(exited)
	}
	go s.Run(context.Background())

	select {
	case <-exited:
	case <-time.After(6 * time.Second):
		t.Fatal("fragmented 200/EOF attempts reset the reconnect window")
	}
	if exitErr == nil {
		t.Fatal("OnExit err = nil, want continuous-outage give-up error")
	}
	if !errors.Is(s.TerminalError(), exitErr) {
		t.Fatalf("TerminalError = %v, want %v", s.TerminalError(), exitErr)
	}
	if got := requests.Load(); got < 2 {
		t.Fatalf("requests = %d, want multiple reconnect attempts", got)
	} else if epoch := s.MediaEpoch(); epoch < uint64(got) {
		t.Fatalf("media epoch=%d for %d fragment attempts; ring was not invalidated per attempt",
			epoch, got)
	}
	if got := stable.Load(); got != 0 {
		t.Fatalf("OnStable fired %d times for sub-threshold fragments", got)
	}
}

// Phase 5 ring buffer: a late joiner gets instant pre-roll from the ring —
// recent bytes, not the stream start, and contiguous into the live edge.
func TestPrerollForLateJoiner(t *testing.T) {
	t.Parallel()
	srv, stopSrv := patternServer(t)
	defer stopSrv()

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	// Tiny ring (~64 seqs) so eviction provably happens before the late join.
	s.ringCap = 512

	first, unsub1 := s.Subscribe("first")
	defer unsub1()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	// Push well past the ring capacity so seq 0 has been evicted.
	if err := drainSeqs(first, 300); err != nil {
		t.Fatal(err)
	}

	late, unsub2 := s.Subscribe("late")
	defer unsub2()

	// Pre-roll is seeded into the channel at Subscribe time — the first
	// chunk must be there ~immediately and must not be the stream start.
	select {
	case chunk, ok := <-late:
		if !ok {
			t.Fatal("late subscriber channel closed")
		}
		if len(chunk) < 8 {
			t.Fatalf("preroll chunk too short: %d bytes", len(chunk))
		}
		if seq := binary.BigEndian.Uint64(chunk[:8]); seq == 0 {
			t.Fatal("preroll began at stream start — ring never evicted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no instant preroll chunk for late joiner")
	}
	// Pre-roll must flow into the live stream without a gap.
	if err := drainSeqs(late, 100); err != nil {
		t.Fatalf("late joiner after preroll: %v", err)
	}

	cancel()
	<-done
}

// Phase 5 ring buffer: the ring is cleared at gap start, so a joiner after
// a failover never receives pre-discontinuity bytes in its pre-roll.
func TestRingClearedAcrossFailover(t *testing.T) {
	t.Parallel()
	const baseB = uint64(1) << 32 // makes server B's bytes distinguishable
	srvA, stopA := patternServer(t)
	defer stopA()
	srvB, stopB := patternServerAt(t, baseB)
	defer stopB()

	s := NewStreamer("test", srvA.URL, testLogger(), nil)
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		return srvB.URL, true, nil
	}

	viewer, unsub := s.Subscribe("viewer")
	defer unsub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	if err := drainSeqs(viewer, 50); err != nil { // server A flowing
		t.Fatal(err)
	}
	stopA()

	// Wait until the viewer is demonstrably receiving server B's bytes.
	deadline := time.After(15 * time.Second)
	for sawB := false; !sawB; {
		select {
		case <-deadline:
			t.Fatal("failover to server B never delivered bytes")
		case chunk, ok := <-viewer:
			if !ok {
				t.Fatal("viewer channel closed across failover")
			}
			if len(chunk) >= 8 && binary.BigEndian.Uint64(chunk[:8]) >= baseB {
				sawB = true
			}
		}
	}

	// A post-failover joiner's pre-roll must contain ONLY server B bytes.
	late, unsub2 := s.Subscribe("late")
	defer unsub2()
	select {
	case chunk, ok := <-late:
		if !ok {
			t.Fatal("late subscriber channel closed")
		}
		if len(chunk) >= 8 {
			if seq := binary.BigEndian.Uint64(chunk[:8]); seq < baseB {
				t.Fatalf("preroll leaked pre-gap bytes (seq %d) — ring not cleared", seq)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no preroll for post-failover joiner")
	}

	cancel()
	<-done
}

// Phase 5 placeholder: once a gap outlasts slateAfter, subscribers receive
// the Source Unavailable slate (paced, channel stays open) instead of
// silence.
func TestSlateFedDuringGap(t *testing.T) {
	t.Parallel()
	srv, stopSrv := patternServer(t)
	defer stopSrv()

	s := NewStreamer("test", srv.URL, testLogger(), nil)
	s.slateAfter = 100 * time.Millisecond
	fake := &slate{data: bytes.Repeat([]byte{0xAB}, 48*1024), duration: 200 * time.Millisecond}
	var slateAsked atomic.Int32
	s.SlateFn = func() *slate { slateAsked.Add(1); return fake }
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		return "", true, nil // keep retrying the dead URL — permanent outage
	}

	viewer, unsub := s.Subscribe("viewer")
	defer unsub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()

	if err := drainSeqs(viewer, 20); err != nil { // healthy first
		t.Fatal(err)
	}
	stopSrv() // outage begins, nothing ever comes back

	// Real pattern bytes start 0x00 (big-endian high byte); slate bytes are
	// 0xAB — unambiguous.
	deadline := time.After(8 * time.Second)
	sawSlate := false
	for !sawSlate {
		select {
		case <-deadline:
			t.Fatalf("never received slate bytes during gap (SlateFn calls: %d)", slateAsked.Load())
		case chunk, ok := <-viewer:
			if !ok {
				t.Fatal("viewer channel closed during slated gap")
			}
			if len(chunk) > 0 && chunk[0] == 0xAB {
				sawSlate = true
			}
		}
	}

	cancel()
	<-done
}

// statusServer always answers with the given HTTP status (no body). Models a
// source that is up at the TCP level but refuses to stream — the real S2 case
// was both sources returning 405 Method Not Allowed.
func statusServer(t *testing.T, code int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// S2: when EVERY source for a channel is unhealthy (both return 405), the
// mid-stream failover loop must NOT ping-pong between the dead sources
// forever. Once the exclude-list covers all sources, the pump terminates to
// the Source-Unavailable slate (slate bytes flow) and gives up at the
// reconnect window — it does not endlessly relocate.
//
// This drives the streamer with an OnUpstreamDown that replays the fixed
// Pool.onUpstreamDown decision over a two-source set: relocate to the next
// untried source, and when the exclude-list covers them all, hold on the
// current URL (return "", true) so the reconnect loop feeds the slate. The
// pre-fix behavior cleared the exclude-list and relocated again, oscillating
// A→B→A→B forever and never feeding the slate.
func TestAllSourcesFailTerminatesToSlateNoPingPong(t *testing.T) {
	t.Parallel()
	healthy, stopHealthy := patternServer(t)
	defer stopHealthy()
	// Two permanently-unhealthy sources (both 405) — the real S2 topology.
	dead := []string{statusServer(t, http.StatusMethodNotAllowed).URL,
		statusServer(t, http.StatusMethodNotAllowed).URL}

	s := NewStreamer("test", healthy.URL, testLogger(), nil)
	s.slateAfter = 100 * time.Millisecond
	s.reconnectWindow = 3 * time.Second
	fake := &slate{data: bytes.Repeat([]byte{0xAB}, 48*1024), duration: 200 * time.Millisecond}
	s.SlateFn = func() *slate { return fake }

	// failedThisGap mirrors the Pool closure's per-gap exclude set. relocations
	// counts how many times we actually switched to a fresh source — the
	// ping-pong bug made this grow without bound; the fix caps it at len(dead).
	failedThisGap := map[int]bool{}
	curSrc := -1 // -1 = the initial healthy URL; 0,1 = dead[] indices
	var relocations atomic.Int32
	var heldOnSlate atomic.Int32
	s.OnUpstreamDown = func(_ context.Context, _ error) (string, bool, error) {
		// Mark the source that just failed.
		failedThisGap[curSrc] = true
		// Find the next source not yet tried this gap.
		for i := range dead {
			if !failedThisGap[i] {
				curSrc = i
				relocations.Add(1)
				return dead[i], true, nil
			}
		}
		// Every source tried-and-failed this gap: hold on the current (dead)
		// URL so the reconnect loop feeds the slate, and back off. Crucially we
		// do NOT clear failedThisGap — that is what would restart the cycle and
		// ping-pong. (Pre-fix bug.)
		heldOnSlate.Add(1)
		return "", true, nil
	}

	viewer, unsub := s.Subscribe("viewer")
	defer unsub()

	var exitErr error
	exitCh := make(chan struct{})
	s.OnExit = func(err error) { exitErr = err; close(exitCh) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	if err := drainSeqs(viewer, 20); err != nil { // healthy first
		t.Fatal(err)
	}
	stopHealthy() // both real sources are now permanently 405

	// Drain the viewer until the pump gives up (channel closes). We must see
	// slate bytes (0xAB) before the close, proving the gap was filled instead
	// of ping-ponging silently.
	sawSlate := false
	for {
		select {
		case chunk, ok := <-viewer:
			if !ok {
				goto gaveUp
			}
			if len(chunk) > 0 && chunk[0] == 0xAB {
				sawSlate = true
			}
		case <-time.After(10 * time.Second):
			t.Fatal("pump never gave up — likely still ping-ponging between dead sources")
		}
	}
gaveUp:
	select {
	case <-exitCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnExit never fired after channel close")
	}

	if !sawSlate {
		t.Fatal("never received slate bytes during all-sources-down gap")
	}
	if exitErr == nil {
		t.Fatal("OnExit err = nil, want give-up error after reconnect window")
	}
	// The whole point: with two dead sources we relocate at most twice (try B,
	// try the other), then hold on slate. The pre-fix bug relocated on every
	// single attempt for the life of the gap.
	if r := relocations.Load(); r > int32(len(dead)) {
		t.Fatalf("relocated %d times across %d sources — ping-pong not fixed", r, len(dead))
	}
	if heldOnSlate.Load() == 0 {
		t.Fatal("never reached the all-exhausted slate-hold branch")
	}
}

// A deadline armed during startup reads must not survive into live pauses.
// The idle timer is armed with the startup timeout while the prefix bursts in;
// media-readiness is established by the consumer afterwards. If the origin
// then pauses for its normal burst interval (measured 3-14s live), the stale
// startup deadline used to fire mid-pause and kill a healthy attempt on the
// first pause after every tune-in (review finding).
func TestIdleDeadlineSurvivesMediaReadyTransition(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, pts, videoCC, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	tail := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC),
		testTimestampedPESPacket(audioPID, 0xc0, pts, audioCC))
	// Startup timeout is tiny; the pause after the prefix is several times
	// longer than it but well inside the live stall tolerance.
	const startupIdle = 150 * time.Millisecond
	const pause = 4 * startupIdle
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prefix)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-time.After(pause):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(tail)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer srv.Close()

	s := NewStreamer("idle-transition", srv.URL, testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 2 * time.Second
	s.idleReadTimeout = startupIdle
	s.stallTolerance = 5 * time.Second
	sub, cancelSub := s.Subscribe("viewer")
	defer cancelSub()
	go func() {
		for range sub {
		}
	}()

	gotBytes, err := s.runOnce(context.Background())
	if !gotBytes {
		t.Fatal("attempt delivered no bytes")
	}
	if errors.Is(err, errUpstreamIdle) {
		t.Fatalf("startup idle deadline killed a live pause shorter than the stall tolerance: %v", err)
	}
}

// A reconnect attempt must fail a source that never delivers startup bytes at
// the fast startup timeout: readiness is attempt-local, and the previous
// attempt having proven media must not widen a new attempt's idle deadline to
// the live stall tolerance (review finding).
func TestReconnectAttemptKeepsStartupIdleTimeout(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, _, _, _ := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	hang := make(chan struct{})
	defer close(hang)
	// Flush-and-hold keeps the response on the LIVE path (see the transcode
	// variant); attempt 1 then ends via its own watchdog under equal phase
	// timeouts.
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prefix)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer healthy.Close()
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		// The leak lived in the producer's timer resets, which are throttled
		// to one per second — so the trickle must span the throttle window
		// before going silent. (The initial arm is always startup-fast.)
		for _, wait := range []time.Duration{0, 1200 * time.Millisecond} {
			select {
			case <-time.After(wait):
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write(make([]byte, 16))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	defer silent.Close()

	s := NewStreamer("reconnect-startup", healthy.URL, testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	sub, cancelSub := s.Subscribe("viewer")
	defer cancelSub()
	go func() {
		for range sub {
		}
	}()

	s.idleReadTimeout = 800 * time.Millisecond
	s.stallTolerance = 800 * time.Millisecond
	if gotBytes, _ := s.runOnce(context.Background()); !gotBytes {
		t.Fatal("first attempt delivered no bytes")
	}
	if !s.MediaReady() {
		t.Fatal("streamer-lifetime readiness not established by the first attempt")
	}

	s.sourceStartupTimeout = 6 * time.Second
	s.idleReadTimeout = 1600 * time.Millisecond
	s.stallTolerance = 8 * time.Second
	s.upstream = silent.URL
	started := time.Now()
	_, err := s.runOnce(context.Background())
	elapsed := time.Since(started)
	t.Logf("reconnect attempt: err=%v elapsed=%s", err, elapsed)
	if !errors.Is(err, errUpstreamIdle) {
		t.Fatalf("silent reconnect result=%v after %s, want %v", err, elapsed, errUpstreamIdle)
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("silent reconnect detected in %s — the live tolerance leaked into a startup phase (startup timeout %s)",
			elapsed, s.idleReadTimeout)
	}
}

// A transcode-host origin pauses for its burst interval (5-6 s measured on
// 192.0.2.20, 9.7 s right after the connect lead) as soon as the lead it
// delivered at connect is consumed. v0.56.0's startup hold sat between the
// validated prefix and first fanout, so that ordinary pause fell inside the
// 4.25 s startup watchdog and killed three of four transcode starts,
// including a recording. The hold is the relay's own delay on validated
// media: upstream silence during it is judged by the live tolerance.
func TestStreamerStartupLeadHoldSurvivesOriginPause(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	prefix, pts, videoCC, audioCC := healthyAVTranscodePrefix(
		pmtPID, videoPID, audioPID, minPrefixDecisionBytes+2*tsPacketSize)
	tail := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, pts, videoCC),
		testTimestampedPESPacket(audioPID, 0xc0, pts, audioCC))
	const startupIdle = 150 * time.Millisecond
	const pause = 3 * startupIdle
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prefix)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-time.After(pause):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(tail)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer srv.Close()

	s := NewStreamer("hold-origin-pause", srv.URL, testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes}
	s.sourceStartupTimeout = 3 * time.Second
	s.idleReadTimeout = startupIdle
	s.stallTolerance = 5 * time.Second
	s.startupLead = 2 * startupIdle
	sub, cancelSub := s.Subscribe("viewer")
	defer cancelSub()
	go func() {
		for range sub {
		}
	}()

	gotBytes, err := s.runOnce(context.Background())
	if !gotBytes {
		t.Fatalf("attempt delivered no bytes: %v", err)
	}
	if errors.Is(err, errUpstreamIdle) {
		t.Fatalf("startup watchdog killed the origin's burst pause during the startup hold: %v", err)
	}
}

// A reconnect attempt must not hold again: the pump has already published,
// and a respawn does not renew the client's startup budget (Plex and the DVR
// give up ~9 s after the tune).
func TestStreamerSkipsStartupLeadOnReconnect(t *testing.T) {
	const lead = 300 * time.Millisecond
	srv := delayedPrefixStreamServerMode(t, 0, false)
	s := NewStreamer("startup-lead-reconnect", srv.URL, testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{readyAt: delayedPrefixChunkCount * chunkSize}
	s.sourceStartupTimeout = 5 * time.Second
	s.startupLead = lead
	var firstFanout time.Time
	s.OnBytes = func(int) error {
		if firstFanout.IsZero() {
			firstFanout = time.Now()
		}
		return nil
	}
	_, unsubscribe := s.Subscribe("startup-lead-reconnect")
	defer unsubscribe()

	started := time.Now()
	if gotBytes, _ := s.runOnce(context.Background()); !gotBytes {
		t.Fatal("first attempt delivered no bytes")
	}
	if held := firstFanout.Sub(started); held < lead {
		t.Fatalf("first attempt published %s after start, want >= %s hold", held, lead)
	}

	firstFanout = time.Time{}
	started = time.Now()
	if gotBytes, _ := s.runOnce(context.Background()); !gotBytes {
		t.Fatal("reconnect attempt delivered no bytes")
	}
	if held := firstFanout.Sub(started); held >= lead {
		t.Fatalf("reconnect attempt published %s after start, want no startup hold (< %s)", held, lead)
	}
}
