package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

type cappedReadSizeReader struct {
	r   io.Reader
	max int
}

func (r cappedReadSizeReader) Read(p []byte) (int, error) {
	if len(p) > r.max {
		p = p[:r.max]
	}
	return r.r.Read(p)
}

type partialThenEOFReader struct {
	first   []byte
	last    []byte
	step    int
	release <-chan struct{}
}

type blockingRelayReadCloser struct {
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func (r *blockingRelayReadCloser) Read([]byte) (int, error) {
	r.startOnce.Do(func() { close(r.started) })
	<-r.closed
	return 0, io.ErrClosedPipe
}

func (r *blockingRelayReadCloser) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func (r *partialThenEOFReader) Read(p []byte) (int, error) {
	switch r.step {
	case 0:
		r.step++
		return copy(p, r.first), nil
	case 1:
		r.step++
		if r.release != nil {
			<-r.release
			return 0, io.EOF
		}
		return copy(p, r.last), io.EOF
	default:
		return 0, io.EOF
	}
}

func TestRelayReadAheadExcludesConsumerPauseFromReadGaps(t *testing.T) {
	attempt := &upstreamAttempt{
		ctx:    context.Background(),
		reader: bytes.NewReader(make([]byte, 2*1024*1024)),
	}
	reads, terminal, done := startRelayReadAhead(context.Background(), relayReaderCloser{
		Reader: attempt,
		close:  attempt.closeBody,
	},
		func(data []byte, started, ended time.Time) {
			attempt.recordOutputData(data, ended.Sub(started))
		}, nil)
	// Deliberately pause longer than the diagnostic threshold. The producer
	// drains independently, so neither input nor output read time may inherit
	// this consumer-side pause.
	time.Sleep(diagnosticGapThreshold + 50*time.Millisecond)
	for range reads {
	}
	<-done
	select {
	case err := <-terminal:
		t.Fatalf("unexpected read-ahead terminal error: %v", err)
	default:
	}
	if count, maxGap := attempt.diag.readGaps.snapshot(); count != 0 || maxGap != 0 {
		t.Fatalf("consumer pause became upstream gap: count=%d max=%s", count, maxGap)
	}
	if count, maxGap := attempt.diag.outputGaps.snapshot(); count != 0 || maxGap != 0 {
		t.Fatalf("consumer pause became relay gap: count=%d max=%s", count, maxGap)
	}
}

func TestRelayReadAheadFailsBoundedlyWithoutDropping(t *testing.T) {
	payload := make([]byte, (relayReadAheadChunks+2)*chunkSize)
	reads, terminal, done := startRelayReadAhead(context.Background(), io.NopCloser(bytes.NewReader(payload)), nil, nil)
	<-done
	select {
	case err := <-terminal:
		if !errors.Is(err, errRelayReadAheadFull) {
			t.Fatalf("terminal error=%v, want read-ahead full", err)
		}
	default:
		t.Fatal("full read-ahead queue closed without its typed terminal error")
	}
	// Exactly the bounded queue is retained; the overflowing chunk was not
	// silently inserted or substituted.
	if got := len(reads); got != relayReadAheadChunks {
		t.Fatalf("queued chunks=%d, want bounded capacity %d", got, relayReadAheadChunks)
	}
}

func TestLiveRelayReadAheadBackpressuresBurstWithoutDropping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	close(release)
	payload := make([]byte, 4*chunkSize)
	readsObserved := make(chan struct{}, 4)
	reads, terminal, done := startRelayReadAheadWithPolicy(
		ctx,
		io.NopCloser(bytes.NewReader(payload)),
		func([]byte, time.Time, time.Time) { readsObserved <- struct{}{} },
		nil,
		// The mechanism under test is depth-independent backpressure; a
		// one-slot queue makes it observable in two reads. The production
		// reservoir depth is exercised by the fixture replay tests.
		1,
		release,
		500*time.Millisecond,
		nil,
	)

	// One chunk occupies the live handoff and the next is held by the
	// coalescer. The raw reader must not pull a third burst chunk until the
	// consumer advances, which is the backpressure missing from v0.44.0.
	for i := 0; i < 2; i++ {
		select {
		case <-readsObserved:
		case <-time.After(time.Second):
			t.Fatalf("raw burst stopped before read %d", i+1)
		}
	}
	select {
	case <-readsObserved:
		t.Fatal("live reader consumed a third chunk while its one-slot handoff was full")
	case <-time.After(25 * time.Millisecond):
	}

	result := <-reads
	bytesRead := len(result.data)
	select {
	case <-readsObserved:
	case <-time.After(time.Second):
		t.Fatal("live reader did not resume after consumer progress")
	}
	for result := range reads {
		bytesRead += len(result.data)
	}
	<-done
	if err := takeRelayReadAheadError(terminal); err != nil {
		t.Fatalf("bounded live backpressure became a retry: %v", err)
	}
	if bytesRead != len(payload) {
		t.Fatalf("backpressured live bytes=%d, want %d", bytesRead, len(payload))
	}
}

func TestLiveRelayReadAheadStallDeadlineStartsAtPrefixRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	readsObserved := make(chan struct{}, 3)
	reads, terminal, done := startRelayReadAheadWithPolicy(
		ctx,
		io.NopCloser(bytes.NewReader(make([]byte, 3*chunkSize))),
		func([]byte, time.Time, time.Time) { readsObserved <- struct{}{} },
		nil,
		1, // one-slot: see TestLiveRelayReadAheadBackpressuresBurstWithoutDropping
		release,
		25*time.Millisecond,
		nil,
	)
	defer func() {
		cancel()
		for range reads {
		}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-readsObserved:
		case <-time.After(time.Second):
			t.Fatalf("raw startup burst stopped before read %d", i+1)
		}
	}

	// Prefix validation may take seconds in production. The absolute startup
	// context owns that private interval; the one-second local-stall contract
	// begins only when validated media is released to the pacer.
	select {
	case <-done:
		t.Fatal("private startup validation consumed the post-release stall budget")
	case <-time.After(75 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("undrained released live handoff did not fail boundedly")
	}
	if err := takeRelayReadAheadError(terminal); !errors.Is(err, errRelayReadAheadFull) {
		t.Fatalf("released live stall error=%v, want %v", err, errRelayReadAheadFull)
	}
}

func TestPrivateReleaseArrivalRebasesOnlyPreReleaseReads(t *testing.T) {
	releaseAt := time.Unix(1_700_000_000, 500)
	oldest := releaseAt.Add(-3 * time.Second)
	if got := privateReleaseArrival(oldest, &releaseAt); !got.Equal(releaseAt) {
		t.Fatalf("pre-release arrival=%s, want release edge %s", got, releaseAt)
	}
	if got := privateReleaseArrival(releaseAt, &releaseAt); !got.Equal(releaseAt) {
		t.Fatalf("release-edge arrival=%s, want %s", got, releaseAt)
	}

	postRelease := releaseAt.Add(7 * time.Millisecond)
	if got := privateReleaseArrival(postRelease, &releaseAt); !got.Equal(postRelease) {
		t.Fatalf("post-release arrival=%s, want preserved %s", got, postRelease)
	}
	if !releaseAt.IsZero() {
		t.Fatalf("post-release source progress did not end private rebasing: %s", releaseAt)
	}
	// Once real post-release source progress is visible, an out-of-order caller
	// cannot make later media look fresh by reviving the old release edge.
	if got := privateReleaseArrival(oldest, &releaseAt); !got.Equal(oldest) {
		t.Fatalf("arrival after release epoch was falsified to %s, want %s", got, oldest)
	}
}

func TestRelayReadAheadFitsMaximumFiniteReplayIncludingEOF(t *testing.T) {
	payload := make([]byte, int(maxFinitePreflightBytes))
	reads, terminal, done := startRelayReadAhead(
		context.Background(), io.NopCloser(cappedReadSizeReader{r: bytes.NewReader(payload), max: 8 * 1024}), nil, nil)
	<-done
	if err := takeRelayReadAheadError(terminal); err != nil {
		t.Fatalf("maximum accepted finite replay overflowed: %v", err)
	}
	var bytesRead int
	var sawEOF bool
	for result := range reads {
		bytesRead += len(result.data)
		if errors.Is(result.err, io.EOF) {
			sawEOF = true
		}
	}
	if bytesRead != len(payload) {
		t.Fatalf("finite replay bytes=%d, want %d", bytesRead, len(payload))
	}
	if !sawEOF {
		t.Fatal("maximum finite replay did not retain its terminal EOF result")
	}
}

func TestRelayReadAheadCoalescesShortTSReadsBeforeSubscriberFanout(t *testing.T) {
	// Seven 188-byte TS packets is a common chunked-HTTP write size. Before
	// coalescing, this payload consumed 1,200 relay/subscriber queue entries:
	// enough to overflow the 513-entry relay queue and disconnect an otherwise
	// healthy subscriber despite representing less than 2 MiB of media.
	payload := make([]byte, 1200*1316)
	for offset := 0; offset+tsPacketSize <= len(payload); offset += tsPacketSize {
		payload[offset] = 0x47
		for i := 1; i < tsPacketSize; i++ {
			payload[offset+i] = byte((offset/tsPacketSize + i) % 251)
		}
	}
	reader := cappedReadSizeReader{r: bytes.NewReader(payload), max: 1316}
	reads, terminal, done := startRelayReadAhead(context.Background(), io.NopCloser(reader), nil, nil)
	<-done
	if err := takeRelayReadAheadError(terminal); err != nil {
		t.Fatalf("short TS reads exhausted byte-bounded relay queue: %v", err)
	}

	var ss subscriberSet
	ss.init("short-read-fanout", nil)
	_, leave := ss.Subscribe("plex-analysis-pause")
	defer leave()
	var bytesRead int
	var chunks int
	coalesced := make([]byte, 0, len(payload))
	for result := range reads {
		bytesRead += len(result.data)
		if len(result.data) > 0 {
			chunks++
			coalesced = append(coalesced, result.data...)
			ss.fanout(result.data)
		}
	}
	if bytesRead != len(payload) {
		t.Fatalf("coalesced bytes=%d, want %d", bytesRead, len(payload))
	}
	if !bytes.Equal(coalesced, payload) {
		t.Fatal("coalescing changed or reordered short-read media bytes")
	}
	chunkCeiling := (len(payload)+chunkSize-1)/chunkSize + 2
	if chunks > chunkCeiling {
		t.Fatalf("published chunks=%d, want byte coalescing ceiling <=%d",
			chunks, chunkCeiling)
	}
	if drops := ss.SubDrops(); drops != 0 {
		t.Fatalf("short-but-healthy TS reads dropped subscriber %d times", drops)
	}
}

func TestRelayReadAheadPreservesOldestCoalescedArrival(t *testing.T) {
	payload := bytes.Repeat([]byte{0x47}, chunkSize)
	reader := cappedReadSizeReader{r: bytes.NewReader(payload), max: chunkSize / 2}
	arrivals := make([]time.Time, 0, 2)
	reads, terminal, done := startRelayReadAhead(
		context.Background(), io.NopCloser(reader),
		func(_ []byte, _ time.Time, ended time.Time) {
			arrivals = append(arrivals, ended)
			if len(arrivals) == 1 {
				time.Sleep(10 * time.Millisecond)
			}
		}, nil)
	<-done
	if err := takeRelayReadAheadError(terminal); err != nil {
		t.Fatal(err)
	}
	results := make([]relayReadResult, 0, 2)
	for result := range reads {
		results = append(results, result)
	}
	if len(arrivals) != 2 || !arrivals[1].After(arrivals[0]) {
		t.Fatalf("raw arrival timestamps=%v, want two ordered reads", arrivals)
	}
	if len(results) < 1 || len(results[0].data) != chunkSize {
		t.Fatalf("coalesced results=%d first_bytes=%d, want one full chunk",
			len(results), len(results[0].data))
	}
	if !results[0].arrivedAt.Equal(arrivals[0]) {
		t.Fatalf("coalesced arrival=%s, want oldest raw arrival %s (newest %s)",
			results[0].arrivedAt, arrivals[0], arrivals[1])
	}
}

func TestRelayReadAheadPreservesTerminalPartialDataAndEOF(t *testing.T) {
	first := bytes.Repeat([]byte{0x47}, 1316)
	last := bytes.Repeat([]byte{0x11}, 188)
	payload := append(append([]byte(nil), first...), last...)
	reads, terminal, done := startRelayReadAhead(
		context.Background(), io.NopCloser(&partialThenEOFReader{first: first, last: last}), nil, nil)
	<-done
	if err := takeRelayReadAheadError(terminal); err != nil {
		t.Fatal(err)
	}
	results := make([]relayReadResult, 0, 1)
	for result := range reads {
		results = append(results, result)
	}
	if len(results) != 1 {
		t.Fatalf("terminal results=%d, want one partial result", len(results))
	}
	if !bytes.Equal(results[0].data, payload) || !errors.Is(results[0].err, io.EOF) {
		t.Fatalf("terminal result bytes=%d err=%v, want %d bytes + EOF",
			len(results[0].data), results[0].err, len(payload))
	}
	if cap(results[0].data) != len(results[0].data) {
		t.Fatalf("terminal partial retained %d-byte backing storage for %d bytes",
			cap(results[0].data), len(results[0].data))
	}
	if results[0].arrivedAt.IsZero() {
		t.Fatal("terminal coalesced result lost its source-arrival timestamp")
	}
}

func TestCompactRelayReadBufferDetachesPartialAndReusesFullBatch(t *testing.T) {
	partial := make([]byte, 1316, chunkSize)
	partial[0] = 0x47
	compacted := compactRelayReadBuffer(partial)
	partial[0] = 0x11
	if compacted[0] != 0x47 {
		t.Fatal("partial compaction still aliases the 64 KiB coalescer allocation")
	}
	if cap(compacted) != len(compacted) {
		t.Fatalf("partial compaction capacity=%d, want exact %d", cap(compacted), len(compacted))
	}

	full := make([]byte, chunkSize)
	full[0] = 0x47
	reused := compactRelayReadBuffer(full)
	full[0] = 0x11
	if reused[0] != 0x11 {
		t.Fatal("full relay batch was copied instead of transferring ownership")
	}
}

func TestRelayReadAheadFlushesPartialLiveReadWithinBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	payload := bytes.Repeat([]byte{0x47}, 1316)
	reader := &partialThenEOFReader{first: payload, release: release}
	reads, terminal, done := startRelayReadAhead(ctx, io.NopCloser(reader), nil, nil)

	started := time.Now()
	select {
	case result := <-reads:
		if !bytes.Equal(result.data, payload) || result.err != nil {
			t.Fatalf("timed partial bytes=%d err=%v, want %d bytes and no error",
				len(result.data), result.err, len(payload))
		}
		if cap(result.data) != len(result.data) {
			t.Fatalf("timed partial retained %d-byte backing storage for %d bytes",
				cap(result.data), len(result.data))
		}
		if elapsed := time.Since(started); elapsed > relayCoalesceMaxDelay+250*time.Millisecond {
			t.Fatalf("partial live read took %s, want <= %s",
				elapsed, relayCoalesceMaxDelay+250*time.Millisecond)
		}
	case <-time.After(relayCoalesceMaxDelay + 500*time.Millisecond):
		t.Fatal("partial live read stayed hidden behind a full-chunk wait")
	}

	cancel()
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("read-ahead did not stop after cancellation released its reader")
	}
	if err := takeRelayReadAheadError(terminal); err != nil {
		t.Fatalf("partial live read reported overflow on cancellation: %v", err)
	}
}

func TestRelayReadAheadCancellationInterruptsBlockedRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &blockingRelayReadCloser{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	reads, terminal, done := startRelayReadAhead(ctx, reader, nil, nil)
	select {
	case <-reader.started:
	case <-time.After(time.Second):
		t.Fatal("raw worker never entered its blocking Read")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close and join the blocked raw reader")
	}
	for range reads {
	}
	if err := takeRelayReadAheadError(terminal); err != nil {
		t.Fatalf("blocked-read cancellation reported overflow: %v", err)
	}
}

func TestTransportPCRPacerContinuesAcrossBufferedTail(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()
	if err := testPCRWait(&pacer, ctx, pcrTestChunk(0, true)); err != nil {
		t.Fatal(err)
	}
	if first := time.Since(started); first > 20*time.Millisecond {
		t.Fatalf("first PCR chunk delayed by %s", first)
	}
	if err := testPCRWait(&pacer, ctx, pcrTestChunk(durationTicks(80*time.Millisecond, transportClockRate), false)); err != nil {
		t.Fatal(err)
	}
	second := time.Since(started)
	if second < 60*time.Millisecond || second > 250*time.Millisecond {
		t.Fatalf("second buffered chunk emitted at %s, want PCR-paced near 80ms", second)
	}
	if err := testPCRWait(&pacer, ctx, pcrTestChunk(durationTicks(160*time.Millisecond, transportClockRate), false)); err != nil {
		t.Fatal(err)
	}
	third := time.Since(started)
	if third < 135*time.Millisecond || third > 350*time.Millisecond {
		t.Fatalf("post-prefix tail emitted at %s, want continued PCR pacing near 160ms", third)
	}
}

func TestTransportPCRPacerSchedulesFromFirstPCRInEachMuxChunk(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	if err := testPCRWait(&pacer, ctx, pcrTestProgressiveChunk(0, 20*time.Millisecond, true)); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 25*time.Millisecond {
		t.Fatalf("first mux chunk delayed by %s", elapsed)
	}
	if err := testPCRWait(&pacer, ctx, pcrTestProgressiveChunk(100*time.Millisecond, 20*time.Millisecond, false)); err != nil {
		t.Fatal(err)
	}
	// The first PCR in chunk two is 100ms after the first PCR in chunk one.
	if elapsed := time.Since(started); elapsed < 70*time.Millisecond || elapsed > 160*time.Millisecond {
		t.Fatalf("second mux chunk emitted at %s, want first-PCR boundary near 100ms", elapsed)
	}
}

func TestTransportPCRPacerDoesNotStarveOnUnequalVBRChunkSpans(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := testPCRWait(&pacer, ctx, pcrTestProgressiveChunk(0, 5*time.Millisecond, false)); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	// The next fixed-size chunk starts at 40ms but spans through 1040ms.
	// Scheduling by its last PCR would withhold a full second of already
	// available media while the client has only ~20ms buffered.
	if err := testPCRWait(&pacer, ctx, pcrTestProgressiveChunk(40*time.Millisecond, 250*time.Millisecond, false)); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Fatalf("unequal-span VBR chunk emitted after %s, want first-PCR pacing near 40ms", elapsed)
	}
}

func TestPCRScannerWrapAndOtherPIDDiscontinuityStayContinuous(t *testing.T) {
	var scanner pcrClockScanner
	scanner.selectPID(0x100)
	nearWrap := pcrModulus - durationTicks(50*time.Millisecond, transportClockRate)
	if _, observed, _ := scanner.feed(pcrTestChunk(nearWrap, false)); !observed {
		t.Fatal("initial near-wrap PCR not observed")
	}
	clock, observed, reset := scanner.feed(pcrTestChunk(durationTicks(50*time.Millisecond, transportClockRate), false))
	if !observed || reset {
		t.Fatalf("PCR wrap observed=%v reset=%v, want continuous", observed, reset)
	}
	if got := pcrTicksDuration(clock); got < 99*time.Millisecond || got > 101*time.Millisecond {
		t.Fatalf("unwrapped PCR duration=%s, want 100ms", got)
	}

	// A discontinuity marker on a non-PCR PID must not reset the selected
	// video PCR clock.
	chunk := append(pcrTestNonPCRPacket(0x101, true), pcrTestChunk(durationTicks(100*time.Millisecond, transportClockRate), false)...)
	_, observed, reset = scanner.feed(chunk)
	if !observed || reset {
		t.Fatalf("other-PID discontinuity observed=%v reset=%v, want continuous video PCR", observed, reset)
	}

	// The selected PCR PID may declare its epoch break on an adaptation-only
	// packet, before the next packet that actually carries PCR.
	chunk = append(pcrTestNonPCRPacket(0x100, true), pcrTestChunk(durationTicks(150*time.Millisecond, transportClockRate), false)...)
	_, observed, reset = scanner.feed(chunk)
	if !observed || !reset {
		t.Fatalf("same-PID pending discontinuity observed=%v reset=%v, want reset", observed, reset)
	}
}

func TestPCRScannerHandlesArbitraryByteSplits(t *testing.T) {
	payload := append(pcrTestChunk(0, false),
		pcrTestChunk(durationTicks(120*time.Millisecond, transportClockRate), false)...)
	var scanner pcrClockScanner
	scanner.selectPID(0x100)
	var clock uint64
	var observed bool
	for offset := 0; offset < len(payload); {
		end := min(offset+137, len(payload)) // deliberately not 188-byte aligned
		if next, ok, reset := scanner.feed(payload[offset:end]); reset {
			t.Fatalf("split transport unexpectedly reset at byte %d", end)
		} else if ok {
			clock, observed = next, true
		}
		offset = end
	}
	if !observed {
		t.Fatal("no PCR observed across arbitrary byte splits")
	}
	if got := pcrTicksDuration(clock); got < 119*time.Millisecond || got > 121*time.Millisecond {
		t.Fatalf("split PCR duration=%s, want 120ms", got)
	}
}

func TestTransportPCRPacerReanchorsAfterInputUnderflow(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	if err := testPCRWait(&pacer, context.Background(), pcrTestChunk(0, false)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(650 * time.Millisecond)
	if err := testPCRWait(&pacer, context.Background(),
		pcrTestChunk(durationTicks(50*time.Millisecond, transportClockRate), false)); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	if err := testPCRWait(&pacer, context.Background(),
		pcrTestChunk(durationTicks(100*time.Millisecond, transportClockRate), false)); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Fatalf("post-underflow PCR emitted after %s, want adaptive ~25ms recovery pacing", elapsed)
	}
}

func TestTransportPCRPacerRejectsPreAgedLiveChunksBeforeFanout(t *testing.T) {
	beforeFailures := metrics.RelayStaleChunkFailures.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }

	// The live-age contract applies even before the first PCR origin exists.
	err := pacer.Wait(context.Background(), pcrTestChunk(0, false),
		wall.Add(-maxTransportAddedLatency-time.Nanosecond))
	if !errors.Is(err, errTransportStaleChunk) {
		t.Fatalf("pre-aged initial chunk error=%v, want %v", err, errTransportStaleChunk)
	}
	if pacer.haveOrigin {
		t.Fatal("pre-aged initial chunk established a PCR origin before rejection")
	}

	// A chunk without a PCR sample is still media and must not bypass the age
	// bound while waiting for the next clock-bearing packet.
	noPCR := make([]byte, tsPacketSize)
	noPCR[0] = 0x47
	noPCR[1] = 0x01
	noPCR[2] = 0x00
	noPCR[3] = 0x10
	err = pacer.Wait(context.Background(), noPCR,
		wall.Add(-maxTransportAddedLatency-time.Nanosecond))
	if !errors.Is(err, errTransportStaleChunk) {
		t.Fatalf("pre-aged no-PCR chunk error=%v, want %v", err, errTransportStaleChunk)
	}
	if delta := metrics.RelayStaleChunkFailures.Value() - beforeFailures; delta != 2 {
		t.Fatalf("pre-aged failure metric delta=%d, want 2", delta)
	}
}

func TestTransportPCRPacerRejectsPreAgedFirstRecoveryChunk(t *testing.T) {
	beforeEpochs := metrics.RelayRecoveryEpochs.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	if err := pacer.Wait(context.Background(), pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	}

	wall = wall.Add(8500 * time.Millisecond)
	err := pacer.Wait(context.Background(), pcrTestChunk(
		durationTicks(50*time.Millisecond, transportClockRate), false),
		wall.Add(-maxTransportAddedLatency-time.Nanosecond))
	if !errors.Is(err, errTransportStaleChunk) {
		t.Fatalf("pre-aged recovery error=%v, want %v", err, errTransportStaleChunk)
	}
	if pacer.recoveryRate != 0 {
		t.Fatalf("pre-aged recovery entered rate %d before rejection", pacer.recoveryRate)
	}
	if delta := metrics.RelayRecoveryEpochs.Value() - beforeEpochs; delta != 0 {
		t.Fatalf("pre-aged chunk opened %d recovery epochs, want zero", delta)
	}
}

func TestTransportPCRPacerRejectsLateTimerWakeBeforeFanout(t *testing.T) {
	beforeFailures := metrics.RelayStaleChunkFailures.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		// A capped target retains a small runtime scheduling reserve. Exceed the
		// actual one-second source-arrival limit by one nanosecond.
		wall = wall.Add(delay + transportWakeReserve + time.Nanosecond)
		return nil
	}
	if err := pacer.Wait(context.Background(), pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	}
	initialEmission := pacer.lastEmission
	arrivedAt := wall.Add(50 * time.Millisecond)
	wall = arrivedAt
	err := pacer.Wait(context.Background(), pcrTestChunk(
		durationTicks(2*time.Second, transportClockRate), false), arrivedAt)
	if !errors.Is(err, errTransportStaleChunk) {
		t.Fatalf("late timer wake error=%v, want %v", err, errTransportStaleChunk)
	}
	if !pacer.lastEmission.Equal(initialEmission) {
		t.Fatalf("late timer wake recorded emission at %v before fanout; want %v",
			pacer.lastEmission, initialEmission)
	}
	if delta := metrics.RelayStaleChunkFailures.Value() - beforeFailures; delta != 1 {
		t.Fatalf("late-wake failure metric delta=%d, want 1", delta)
	}
}

func TestTransportPCRPacerRejectsSharedArrivalTailBeforeZeroInterval(t *testing.T) {
	beforeEpisodes := metrics.RelaySkipAheadEpisodes.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	arrival := wall
	chunks := pcrSparseTestChunks(30, 50*time.Millisecond)
	lastEmission := wall
	var got error
	for i, chunk := range chunks {
		got = pacer.Wait(context.Background(), chunk, arrival)
		if got != nil {
			break
		}
		if i > 0 {
			if interval := wall.Sub(lastEmission); interval <= 0 {
				t.Fatalf("shared-arrival chunk %d emitted after %s", i, interval)
			}
		}
		lastEmission = wall
	}
	if !errors.Is(got, errTransportSkipStale) {
		t.Fatalf("shared-arrival tail error=%v, want %v", got, errTransportSkipStale)
	}
	if wall.Sub(arrival) > maxTransportAddedLatency {
		t.Fatalf("shared-arrival tail emitted for %s past live bound", wall.Sub(arrival))
	}
	if delta := metrics.RelaySkipAheadEpisodes.Value() - beforeEpisodes; delta != 1 {
		t.Fatalf("skip-ahead episode metric delta=%d, want 1", delta)
	}
}

func TestTransportPCRPacerKeepsPauseBacklogAsLead(t *testing.T) {
	beforeEpochs := metrics.RelayRecoveryEpochs.Value()
	beforeReanchors := metrics.RelayUnderflowReanchors.Value()
	beforeSkips := metrics.RelaySkipAheadEpisodes.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}

	chunks := pcrSparseTestChunks(301, 50*time.Millisecond)
	if err := pacer.Wait(context.Background(), chunks[0], wall); err != nil {
		t.Fatal(err)
	}
	// The source stopped delivering for 8.5 seconds, then returned several
	// seconds of media in one burst. Until 2026-09-02 the pacer compressed the
	// debt at 10x to rejoin the live edge, which emptied the reservoir at the
	// end of every pause and left the HLS client with only its own ~3s. The
	// owner ruled margin over the live edge: the schedule re-anchors on the
	// first returned chunk and the burst stays queued as lead, so each chunk
	// is dequeued fresh from the reservoir and emitted at its media cadence.
	wall = wall.Add(8500 * time.Millisecond)
	lastEmission := wall
	for i := 1; i <= 120; i++ {
		arrivedAt := wall // dequeue restamps arrival
		if err := pacer.Wait(context.Background(), chunks[i], arrivedAt); err != nil {
			t.Fatalf("post-pause chunk %d: %v", i, err)
		}
		if i > 1 {
			if interval := wall.Sub(lastEmission); interval != 50*time.Millisecond {
				t.Fatalf("post-pause chunk %d emitted after %s, want 50ms media cadence", i, interval)
			}
		}
		lastEmission = wall
	}
	if pacer.recoveryRate != 0 {
		t.Fatalf("recovery rate=%d after a pause, want media cadence", pacer.recoveryRate)
	}
	if pacer.skipEpisode {
		t.Fatal("pause backlog armed a skip episode")
	}

	// A second pause re-anchors again without compressing anything.
	wall = wall.Add(8500 * time.Millisecond)
	lastEmission = wall
	for i := 121; i <= 240; i++ {
		if err := pacer.Wait(context.Background(), chunks[i], wall); err != nil {
			t.Fatalf("second post-pause chunk %d: %v", i, err)
		}
		if i > 121 {
			if interval := wall.Sub(lastEmission); interval != 50*time.Millisecond {
				t.Fatalf("second post-pause chunk %d emitted after %s, want 50ms", i, interval)
			}
		}
		lastEmission = wall
	}
	if delta := metrics.RelayUnderflowReanchors.Value() - beforeReanchors; delta != 2 {
		t.Fatalf("underflow re-anchor metric delta=%d, want 2", delta)
	}
	if delta := metrics.RelayRecoveryEpochs.Value() - beforeEpochs; delta != 0 {
		t.Fatalf("recovery epoch metric delta=%d, want 0: a pause must not accelerate", delta)
	}
	if delta := metrics.RelaySkipAheadEpisodes.Value() - beforeSkips; delta != 0 {
		t.Fatalf("skip-ahead metric delta=%d, want 0", delta)
	}
}

func TestTransportPCRPacerContinuesAtBoundedBackpressuredSourceEdgeAfterPause(t *testing.T) {
	beforeFailures := metrics.RelayRecoveryBacklogFailures.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	const mediaStep = 50 * time.Millisecond
	clocks := map[int]time.Duration{
		0: 0,
		1: mediaStep,
		2: 2 * mediaStep,
		3: 3 * mediaStep,
		// A long but valid PCR interval can follow an HLS segment edge. Keep
		// it below the three-second corruption fence and make its pacing target
		// land exactly at the near-stale wall time exercised below.
		4: 1049 * time.Millisecond,
		5: 1099 * time.Millisecond,
	}
	for i := 6; i < 10; i++ {
		clocks[i] = clocks[i-1] + mediaStep
	}
	chunks := pcrPatternTestChunks(10, clocks)
	if err := pacer.Wait(context.Background(), chunks[0], wall); err != nil {
		t.Fatal(err)
	}

	// A provider/HLS starvation re-anchors the schedule on the first returned
	// chunk without opening an accelerated epoch (the backlog is lead now).
	// A one-result read-ahead queue plus its producer-held transport chunk can
	// leave every consumer-visible arrival more than 100ms old even after the
	// queue has reached a stable source edge; ordinary pacing must ride that
	// bounded age without a backlog failure.
	wall = wall.Add(8500 * time.Millisecond)
	burstArrival := wall
	if err := pacer.Wait(context.Background(), chunks[1], burstArrival); err != nil {
		t.Fatal(err)
	}
	if pacer.recoveryRate != 0 {
		t.Fatalf("recovery rate=%d after a pause, want media cadence", pacer.recoveryRate)
	}
	if !pacer.originWall.Equal(burstArrival) {
		t.Fatalf("origin wall=%v after a pause, want re-anchored at %v", pacer.originWall, burstArrival)
	}

	queuedArrival := burstArrival.Add(mediaStep/2 - time.Nanosecond)
	wall = queuedArrival.Add(150 * time.Millisecond)
	if err := pacer.Wait(context.Background(), chunks[2], queuedArrival); err != nil {
		t.Fatal(err)
	}

	// The next PCR advances at full source cadence while retaining the harmless
	// 150ms age observed for CBS-sized transport chunks.
	sourceArrival := queuedArrival.Add(mediaStep)
	wall = sourceArrival.Add(150 * time.Millisecond)
	if err := pacer.Wait(context.Background(), chunks[3], sourceArrival); err != nil {
		t.Fatal(err)
	}

	// Move close to the absolute stale limit without crossing it. Both the old
	// recovery path and ordinary pacing can emit this already-due chunk.
	sourceArrival = sourceArrival.Add(mediaStep)
	wall = sourceArrival.Add(maxTransportAddedLatency - time.Millisecond)
	if err := pacer.Wait(context.Background(), chunks[4], sourceArrival); err != nil {
		t.Fatalf("near-limit source-edge chunk: %v", err)
	}

	// The following source arrival still advances by more than half a media
	// step, but leaves only 4ms before the pacer's 975ms target. Ordinary mode
	// uses its guarded 16x floor (3.125ms), caps the mapping, and continues.
	sourceArrival = sourceArrival.Add(28 * time.Millisecond)
	lastEmission := wall
	if err := pacer.Wait(context.Background(), chunks[5], sourceArrival); err != nil {
		t.Fatalf("bounded source-edge continuation: %v", err)
	}
	if interval := wall.Sub(lastEmission); interval <= 0 {
		t.Fatalf("bounded source-edge continuation interval=%s, want positive", interval)
	}
	if pacer.recoveryRate != 0 {
		t.Fatalf("recovery rate=%d at bounded source edge, want ordinary pacing", pacer.recoveryRate)
	}

	// Ordinary arrivals remain on positive media cadence at the bounded edge.
	for i := 6; i < len(chunks); i++ {
		sourceArrival = sourceArrival.Add(mediaStep)
		lastEmission = wall
		if err := pacer.Wait(context.Background(), chunks[i], sourceArrival); err != nil {
			t.Fatalf("post-recovery source-edge chunk %d: %v", i, err)
		}
		if interval := wall.Sub(lastEmission); interval <= 0 {
			t.Fatalf("post-recovery source-edge chunk %d interval=%s, want positive", i, interval)
		}
		if lag := wall.Sub(sourceArrival); lag < 0 || lag > maxTransportAddedLatency {
			t.Fatalf("post-recovery source-edge chunk %d lag=%s, want bounded", i, lag)
		}
	}
	if delta := metrics.RelayRecoveryBacklogFailures.Value() - beforeFailures; delta != 0 {
		t.Fatalf("bounded source-edge backlog failure metric delta=%d, want 0", delta)
	}
}

func TestTransportPCRPacerPacesSparsePCRChunksAfterPause(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	// One selected-program PCR every five 64KiB chunks models a high-bitrate
	// MPTS. The 50ms PCR interval therefore represents about 10ms per chunk.
	chunks := pcrEveryNTestChunks(90, 5, 50*time.Millisecond)
	sourceStart := wall
	for i := 0; i <= 10; i++ {
		arrivedAt := sourceStart.Add(time.Duration(i) * 10 * time.Millisecond)
		if wall.Before(arrivedAt) {
			wall = arrivedAt
		}
		if err := pacer.Wait(context.Background(), chunks[i], arrivedAt); err != nil {
			t.Fatalf("steady sparse chunk %d: %v", i, err)
		}
	}
	if pacer.chunkStep != 10*time.Millisecond {
		t.Fatalf("learned sparse chunk step=%s, want 10ms", pacer.chunkStep)
	}

	// Finish the current interval, then reproduce the Fox-style starvation on
	// the next clock-bearing chunk. The schedule re-anchors there and every
	// following PCR and no-PCR chunk keeps its learned 10ms cadence: the
	// returned burst is lead, not debt to compress.
	for i := 11; i < 15; i++ {
		arrivedAt := sourceStart.Add(time.Duration(i) * 10 * time.Millisecond)
		if wall.Before(arrivedAt) {
			wall = arrivedAt
		}
		if err := pacer.Wait(context.Background(), chunks[i], arrivedAt); err != nil {
			t.Fatal(err)
		}
	}
	wall = wall.Add(8500 * time.Millisecond)
	burstArrival := wall
	lastEmission := pacer.lastEmission
	for i := 15; i < 75; i++ {
		if err := pacer.Wait(context.Background(), chunks[i], wall); err != nil {
			t.Fatalf("sparse post-pause chunk %d: %v", i, err)
		}
		interval := wall.Sub(lastEmission)
		if i > 15 && interval != 10*time.Millisecond {
			t.Fatalf("sparse post-pause chunk %d interval=%s, want learned 10ms cadence", i, interval)
		}
		lastEmission = wall
	}
	if elapsed := wall.Sub(burstArrival); elapsed != 59*10*time.Millisecond {
		t.Fatalf("sparse post-pause elapsed=%s, want 59 chunks at 10ms", elapsed)
	}
	if pacer.recoveryRate != 0 {
		t.Fatalf("recovery rate=%d after a pause, want media cadence", pacer.recoveryRate)
	}
}

func TestTransportPCRPacerKeepsExcessiveUnderflowBacklog(t *testing.T) {
	beforeEpisodes := metrics.RelaySkipAheadEpisodes.Value()
	beforeReanchors := metrics.RelayUnderflowReanchors.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	if err := pacer.Wait(context.Background(), pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	}
	// A 16.5s pause used to exceed the 16x recovery ceiling and discard the
	// backlog to rejoin the live edge. The backlog is exactly the margin the
	// viewer wants: re-anchor and keep it.
	wall = wall.Add(16500 * time.Millisecond)
	err := pacer.Wait(context.Background(), pcrTestChunk(
		durationTicks(50*time.Millisecond, transportClockRate), false), wall)
	if err != nil {
		t.Fatalf("excessive underflow error=%v, want the backlog kept", err)
	}
	if pacer.skipEpisode {
		t.Fatal("skip episode armed after a long pause")
	}
	if pacer.recoveryRate != 0 || !pacer.originWall.Equal(wall) {
		t.Fatalf("rate=%d origin=%v after a long pause, want media cadence re-anchored at %v",
			pacer.recoveryRate, pacer.originWall, wall)
	}
	if delta := metrics.RelaySkipAheadEpisodes.Value() - beforeEpisodes; delta != 0 {
		t.Fatalf("skip episode metric delta=%d, want 0", delta)
	}
	if delta := metrics.RelayUnderflowReanchors.Value() - beforeReanchors; delta != 1 {
		t.Fatalf("underflow re-anchor metric delta=%d, want 1", delta)
	}
}

func TestTransportPCRPacerRestartsValidatedAttemptOnMidstreamDiscontinuity(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	if err := pacer.Wait(context.Background(), pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	}
	wall = wall.Add(8500 * time.Millisecond)
	if err := pacer.Wait(context.Background(), pcrTestChunk(
		durationTicks(50*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatal(err)
	}
	if pacer.recoveryRate != 0 || !pacer.originWall.Equal(wall) {
		t.Fatalf("underflow rate=%d origin=%v, want media cadence re-anchored at %v",
			pacer.recoveryRate, pacer.originWall, wall)
	}

	pacer.Configure(0x100, false)
	if pacer.recoveryRate != 0 || !pacer.lastEmission.IsZero() || pacer.haveOrigin {
		t.Fatalf("Configure retained recovery state: rate=%d emission=%v origin=%v",
			pacer.recoveryRate, pacer.lastEmission, pacer.haveOrigin)
	}
	if err := pacer.Wait(context.Background(), pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	}
	wall = wall.Add(8500 * time.Millisecond)
	if err := pacer.Wait(context.Background(), pcrTestChunk(
		durationTicks(50*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatal(err)
	}
	if pacer.recoveryRate != 0 || !pacer.originWall.Equal(wall) {
		t.Fatalf("second underflow rate=%d origin=%v, want media cadence re-anchored at %v",
			pacer.recoveryRate, pacer.originWall, wall)
	}
	discontinuous := pcrTestChunk(durationTicks(100*time.Millisecond, transportClockRate), true)
	err := pacer.Wait(context.Background(), discontinuous, wall)
	if !errors.Is(err, errTransportAttemptBoundary) {
		t.Fatalf("declared discontinuity error=%v, want %v", err, errTransportAttemptBoundary)
	}
	if discontinuous[5]&0x80 == 0 {
		t.Fatal("declared discontinuity was not retained in transport")
	}
}

func TestTransportPCRPacerPrefixEpochEndsAtPrefixNotSourceEdge(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	// A 2.95s validated prefix (60 chunks, one 50ms PCR each) needs 4x to fit
	// the 975ms release window.
	clocks := map[int]time.Duration{}
	for i := 0; i < 80; i++ {
		clocks[i] = time.Duration(i) * 50 * time.Millisecond
	}
	stream := pcrPatternTestChunks(80, clocks)
	prefix, reservoir := stream[:60], stream[60:]
	if err := pacer.PrepareBufferedPrefix(bytes.Join(prefix, nil), chunkSize); err != nil {
		t.Fatal(err)
	}
	if pacer.pendingRate != 4 {
		t.Fatalf("prefix rate=%d, want 4x", pacer.pendingRate)
	}
	release := wall
	lastEmission := wall
	for i, chunk := range prefix {
		if err := pacer.Wait(context.Background(), chunk, release); err != nil {
			t.Fatalf("prefix chunk %d: %v", i, err)
		}
		if i > 0 {
			if interval := wall.Sub(lastEmission); interval != 12500*time.Microsecond {
				t.Fatalf("prefix chunk %d interval=%s, want 12.5ms at 4x", i, interval)
			}
		}
		lastEmission = wall
	}

	// Media queued behind the prefix in the reservoir is dequeued fresh, one
	// chunk per emission. Until 2026-09-02 the epoch stayed at 4x until
	// arrivals advanced by at least half a media step, which at 4x they never
	// do while the reservoir has media: the whole reservoir drained into the
	// client at 4x. The epoch now ends with the prefix's last clocked chunk,
	// so every reservoir chunk paces at media cadence and the reservoir stays
	// queued as lead.
	for i, chunk := range reservoir {
		if err := pacer.Wait(context.Background(), chunk, wall); err != nil {
			t.Fatalf("reservoir chunk %d: %v", i, err)
		}
		if interval := wall.Sub(lastEmission); interval != 50*time.Millisecond {
			t.Fatalf("reservoir chunk %d interval=%s, want 50ms media cadence", i, interval)
		}
		lastEmission = wall
	}
	if pacer.recoveryRate != 0 {
		t.Fatalf("recovery rate=%d after the prefix, want media cadence", pacer.recoveryRate)
	}
}

func TestTransportPCRPacerTrimsRetainedLeadAboveHighWater(t *testing.T) {
	beforeTrims := metrics.RelayLeadTrimEpochs.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	const step = 50 * time.Millisecond
	next := 0
	feed := func() time.Duration {
		clock := durationTicks(time.Duration(next)*step, transportClockRate)
		next++
		before := wall
		if err := pacer.Wait(context.Background(), pcrTestChunk(clock, false), wall); err != nil {
			t.Fatalf("chunk %d: %v", next-1, err)
		}
		return wall.Sub(before)
	}
	for i := 0; i < 10; i++ {
		feed()
	}
	if pacer.chunkStep != step {
		t.Fatalf("learned chunk step=%s, want %s", pacer.chunkStep, step)
	}

	// The reservoir crossed the high-water mark: a 2x epoch runs until the
	// OBSERVED backlog reaches the low-water mark, then media cadence resumes.
	// The producer keeps adding at media rate meanwhile, so at 2x the queue
	// only shrinks by one chunk per two emitted (review finding).
	backlog := leadTrimHighWaterChunks + 5
	pacer.NoteBacklog(backlog)
	excess := backlog - leadTrimLowWaterChunks
	emitted := 0
	for backlog > leadTrimLowWaterChunks {
		if interval := feed(); interval != step/2 {
			t.Fatalf("trim chunk %d interval=%s, want %s at 2x", emitted, interval, step/2)
		}
		emitted++
		if emitted%2 == 0 {
			backlog--
		}
		pacer.NoteBacklog(backlog)
	}
	if emitted != 2*excess {
		t.Fatalf("trim emitted %d chunks, want %d (net one chunk per two at 2x)", emitted, 2*excess)
	}
	if delta := metrics.RelayLeadTrimEpochs.Value() - beforeTrims; delta != 1 {
		t.Fatalf("lead trim metric delta=%d, want 1", delta)
	}
	// The chunk that observes the low-water mark takes the final 2x step;
	// media cadence follows.
	feed()
	for i := 0; i < 5; i++ {
		if interval := feed(); interval != step {
			t.Fatalf("post-trim chunk %d interval=%s, want %s", i, interval, step)
		}
	}
	if pacer.recoveryRate != 0 || pacer.trimEpoch {
		t.Fatalf("rate=%d trim=%v after the trim, want media cadence", pacer.recoveryRate, pacer.trimEpoch)
	}
}

func TestTransportPCRPacerDrainsValidatedPrefixWithinLiveBound(t *testing.T) {
	beforeEpochs := metrics.RelayRecoveryEpochs.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrSparseTestChunks(100, 50*time.Millisecond)
	prefix := bytes.Join(chunks[:80], nil) // 3.95 seconds of validated media.
	if err := pacer.PrepareBufferedPrefix(prefix, chunkSize); err != nil {
		t.Fatal(err)
	}
	if pacer.pendingRate != 5 {
		t.Fatalf("validated-prefix rate=%d, want minimum bounded 5x", pacer.pendingRate)
	}

	arrival := wall
	lastEmission := wall
	for i, chunk := range chunks[:80] {
		if err := pacer.Wait(context.Background(), chunk, arrival); err != nil {
			t.Fatalf("prefix chunk %d: %v", i, err)
		}
		if i > 0 {
			if interval := wall.Sub(lastEmission); interval != 10*time.Millisecond {
				t.Fatalf("prefix chunk %d interval=%s, want 10ms at 5x", i, interval)
			}
		}
		lastEmission = wall
	}
	if elapsed := wall.Sub(arrival); elapsed <= 0 || elapsed > maxTransportAddedLatency {
		t.Fatalf("validated prefix drained in %s, want nonzero and <=%s",
			elapsed, maxTransportAddedLatency)
	}

	// Continue accelerated while queued post-gate reads catch the live edge,
	// then return to source cadence without a zero-interval transition.
	sourceArrival := arrival
	for i := 80; i < len(chunks); i++ {
		sourceArrival = sourceArrival.Add(50 * time.Millisecond)
		if wall.Before(sourceArrival) {
			wall = sourceArrival
		}
		if err := pacer.Wait(context.Background(), chunks[i], sourceArrival); err != nil {
			t.Fatalf("post-prefix chunk %d: %v", i, err)
		}
		if interval := wall.Sub(lastEmission); interval <= 0 {
			t.Fatalf("post-prefix chunk %d interval=%s, want positive", i, interval)
		}
		lastEmission = wall
	}
	if pacer.recoveryRate != 0 {
		t.Fatalf("validated-prefix recovery rate=%d after source-edge convergence", pacer.recoveryRate)
	}
	if delta := metrics.RelayRecoveryEpochs.Value() - beforeEpochs; delta != 1 {
		t.Fatalf("validated-prefix recovery epoch delta=%d, want 1", delta)
	}
}

func TestTransportPCRPacerPacesPreparedPrefixBeforeFirstPCR(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrPatternTestChunks(11, map[int]time.Duration{
		5:  0,
		10: 50 * time.Millisecond,
	})
	if err := pacer.PrepareBufferedPrefix(bytes.Join(chunks, nil), chunkSize); err != nil {
		t.Fatal(err)
	}
	if pacer.pendingStep != 10*time.Millisecond {
		t.Fatalf("pre-PCR prepared step=%s, want 10ms", pacer.pendingStep)
	}
	arrival := wall
	lastEmission := wall
	for i, chunk := range chunks {
		if err := pacer.Wait(context.Background(), chunk, arrival); err != nil {
			t.Fatalf("prepared pre-PCR chunk %d: %v", i, err)
		}
		if i > 0 {
			if interval := wall.Sub(lastEmission); interval != 10*time.Millisecond {
				t.Fatalf("prepared pre-PCR chunk %d interval=%s, want 10ms", i, interval)
			}
		}
		lastEmission = wall
	}
}

func TestTransportPCRPacerProfilesTrailingSparseChunksAtRateBoundary(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	// The selected PCR span fits the 975ms live pacing window exactly, but four
	// valid no-PCR chunks follow it. Their learned 25ms cadence must participate
	// in rate selection instead of failing after the final PCR is emitted.
	chunks := pcrPatternTestChunks(44, map[int]time.Duration{
		0:  0,
		39: 975 * time.Millisecond,
	})
	if err := pacer.PrepareBufferedPrefix(bytes.Join(chunks, nil), chunkSize); err != nil {
		t.Fatal(err)
	}
	if pacer.pendingStep != 25*time.Millisecond {
		t.Fatalf("trailing-prefix step=%s, want 25ms", pacer.pendingStep)
	}
	if pacer.pendingRate != 2 {
		t.Fatalf("trailing-prefix rate=%d, want bounded 2x", pacer.pendingRate)
	}

	arrival := wall
	lastEmission := wall
	for i, chunk := range chunks {
		if err := pacer.Wait(context.Background(), chunk, arrival); err != nil {
			t.Fatalf("trailing-prefix chunk %d: %v", i, err)
		}
		if i > 0 {
			if interval := wall.Sub(lastEmission); interval != 12500*time.Microsecond {
				t.Fatalf("trailing-prefix chunk %d interval=%s, want 12.5ms", i, interval)
			}
		}
		lastEmission = wall
	}
	if elapsed := wall.Sub(arrival); elapsed <= 0 || elapsed > maxTransportAddedLatency {
		t.Fatalf("trailing-prefix elapsed=%s, want nonzero and <=%s",
			elapsed, maxTransportAddedLatency)
	}
}

func TestTransportPCRPacerAccountsForPerChunkCeilingAtRateBoundary(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrPatternTestChunks(8, map[int]time.Duration{
		0: 0,
		7: 1950 * time.Millisecond,
	})
	if err := pacer.PrepareBufferedPrefix(bytes.Join(chunks, nil), chunkSize); err != nil {
		t.Fatal(err)
	}
	// Aggregate division suggests 2x, but ceil(1.95s/7/2) repeated seven
	// times is 975,000,005ns. Select 3x so the real per-call schedule fits.
	if pacer.pendingRate != 3 {
		t.Fatalf("ceiling-boundary rate=%d, want bounded 3x", pacer.pendingRate)
	}
	arrival := wall
	for i, chunk := range chunks {
		if err := pacer.Wait(context.Background(), chunk, arrival); err != nil {
			t.Fatalf("ceiling-boundary chunk %d: %v", i, err)
		}
	}
	if elapsed := wall.Sub(arrival); elapsed <= 0 || elapsed > maxTransportAddedLatency-transportWakeReserve {
		t.Fatalf("ceiling-boundary elapsed=%s, want nonzero and <=%s",
			elapsed, maxTransportAddedLatency-transportWakeReserve)
	}
}

func TestTransportPCRPacerProfilesVariableSparsePrefixWithoutRetry(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	// A low-rate first PCR interval followed by a dense final interval makes a
	// single prefix-wide average optimistic: 3x exhausts the live window after
	// the first six chunks have already consumed that average cadence. The
	// exact schedule requires 4x.
	chunks := pcrPatternTestChunks(8, map[int]time.Duration{
		0: 0,
		6: 100 * time.Millisecond,
		7: 1950 * time.Millisecond,
	})
	if err := pacer.PrepareBufferedPrefix(bytes.Join(chunks, nil), chunkSize); err != nil {
		t.Fatal(err)
	}
	if pacer.pendingRate != 4 {
		t.Fatalf("variable-prefix rate=%d, want bounded 4x", pacer.pendingRate)
	}
	arrival := wall
	for i, chunk := range chunks {
		if err := pacer.Wait(context.Background(), chunk, arrival); err != nil {
			t.Fatalf("variable-prefix chunk %d: %v", i, err)
		}
	}
	if elapsed := wall.Sub(arrival); elapsed <= 0 || elapsed > maxTransportAddedLatency-transportWakeReserve {
		t.Fatalf("variable-prefix elapsed=%s, want nonzero and <=%s",
			elapsed, maxTransportAddedLatency-transportWakeReserve)
	}
}

func TestTransportPCRPacerPreparedScheduleProperty(t *testing.T) {
	seed := uint64(0x474f0b)
	next := func(limit int) int {
		seed = seed*6364136223846793005 + 1442695040888963407
		return int(seed % uint64(limit))
	}
	for scenario := 0; scenario < 200; scenario++ {
		count := 4 + next(17)
		first := next(max(1, count/3))
		last := count - 1 - next(max(1, count/3))
		if last <= first {
			last = first + 1
		}
		clocks := map[int]time.Duration{first: 0}
		clock := time.Duration(0)
		for index := first + 1; index < last; index++ {
			if next(3) == 0 {
				clock += time.Duration(1+next(2000)) * time.Millisecond
				clocks[index] = clock
			}
		}
		clock += time.Duration(1+next(2000)) * time.Millisecond
		clocks[last] = clock
		chunks := pcrPatternTestChunks(count, clocks)

		wall := time.Unix(1_700_000_000, 0)
		arrival := wall
		pacer := testTransportPCRPacer(false)
		pacer.now = func() time.Time { return wall }
		pacer.wait = func(_ context.Context, delay time.Duration) error {
			wall = wall.Add(delay)
			return nil
		}
		if err := pacer.PrepareBufferedPrefix(bytes.Join(chunks, nil), chunkSize); err != nil {
			if errors.Is(err, errTransportRelayBacklog) {
				continue
			}
			t.Fatalf("scenario %d prepare: %v", scenario, err)
		}
		lastEmission := wall
		for index, chunk := range chunks {
			if err := pacer.Wait(context.Background(), chunk, arrival); err != nil {
				t.Fatalf("scenario %d chunk %d rate=%d clocks=%v: %v",
					scenario, index, pacer.recoveryRate, clocks, err)
			}
			if index > 0 && !wall.After(lastEmission) {
				t.Fatalf("scenario %d chunk %d had nonpositive interval at rate=%d clocks=%v",
					scenario, index, pacer.recoveryRate, clocks)
			}
			lastEmission = wall
		}
		if elapsed := wall.Sub(arrival); elapsed > maxTransportAddedLatency-transportWakeReserve {
			t.Fatalf("scenario %d elapsed=%s rate=%d clocks=%v, want <=%s",
				scenario, elapsed, pacer.recoveryRate, clocks,
				maxTransportAddedLatency-transportWakeReserve)
		}
	}
}

func TestTransportPCRPacerRejectsCorruptFuturePCRInPreparedPrefix(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	chunks := pcrPatternTestChunks(4, map[int]time.Duration{
		0: 0,
		1: 4 * time.Second,
	})
	if err := pacer.PrepareBufferedPrefix(bytes.Join(chunks, nil), chunkSize); !errors.Is(err, errTransportRelayBacklog) {
		t.Fatalf("future-PCR prefix error=%v, want %v", err, errTransportRelayBacklog)
	}
	if pacer.pendingRate != 0 || pacer.pendingStep != 0 {
		t.Fatalf("rejected future-PCR prefix retained rate=%d step=%s",
			pacer.pendingRate, pacer.pendingStep)
	}
}

func TestTransportPCRPacerRejectsUnclockedMultiChunkPrefix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		clocks map[int]time.Duration
	}{
		{name: "no selected PCR", clocks: map[int]time.Duration{}},
		{name: "one selected PCR", clocks: map[int]time.Duration{1: 0}},
		{name: "frozen selected PCR", clocks: map[int]time.Duration{0: 0, 2: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pacer := testTransportPCRPacer(false)
			prefix := bytes.Join(pcrPatternTestChunks(3, tc.clocks), nil)
			if err := pacer.PrepareBufferedPrefix(prefix, chunkSize); !errors.Is(err, errTransportRelayBacklog) {
				t.Fatalf("unclocked prefix error=%v, want %v", err, errTransportRelayBacklog)
			}
			if pacer.pendingRate != 0 || pacer.pendingStep != 0 {
				t.Fatalf("rejected unclocked prefix retained rate=%d step=%s",
					pacer.pendingRate, pacer.pendingStep)
			}
		})
	}
}

func TestTransportPCRPacerPCRAnchorClosesSparseDensityChange(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrPatternTestChunks(13, map[int]time.Duration{
		0:  0,
		10: 50 * time.Millisecond,
		12: 100 * time.Millisecond,
	})
	for i := 0; i <= 10; i++ {
		if err := pacer.Wait(context.Background(), chunks[i], wall); err != nil {
			t.Fatal(err)
		}
	}
	if pacer.chunkStep != 5*time.Millisecond {
		t.Fatalf("initial learned chunk step=%s, want 5ms", pacer.chunkStep)
	}
	pacer.recoveryRate = 10
	previousPCR := pacer.lastPCREmit
	if err := pacer.Wait(context.Background(), chunks[11], wall); err != nil {
		t.Fatal(err)
	}
	if err := pacer.Wait(context.Background(), chunks[12], wall); err != nil {
		t.Fatal(err)
	}
	if interval := pacer.lastPCREmit.Sub(previousPCR); interval != 5*time.Millisecond {
		t.Fatalf("density-change PCR interval=%s, want 5ms at 10x", interval)
	}
	if minimum := divideDurationCeil(50*time.Millisecond, maxTransportRecoveryRate); wall.Sub(previousPCR) < minimum {
		t.Fatalf("density-change window=%s, want >= hard 16x floor %s",
			wall.Sub(previousPCR), minimum)
	}
}

func TestTransportPCRPacerRejectsValidatedPrefixBeyondMaximumRate(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	// Use production-sized relay chunks: 19.8 seconds across 100 chunks cannot
	// drain inside 975ms even at the maximum 16x rate.
	prefix := bytes.Join(pcrSparseTestChunks(100, 200*time.Millisecond), nil)
	if err := pacer.PrepareBufferedPrefix(prefix, chunkSize); !errors.Is(err, errTransportRelayBacklog) {
		t.Fatalf("oversized validated prefix error=%v, want %v", err, errTransportRelayBacklog)
	}
	if pacer.pendingRate != 0 {
		t.Fatalf("rejected validated prefix retained rate %d", pacer.pendingRate)
	}
}

func TestTransportPCRPacerRejectsMultiEpochValidatedPrefix(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	prefix := append([]byte{}, pcrTestChunk(0, true)...)
	prefix = append(prefix, pcrTestChunk(durationTicks(50*time.Millisecond, transportClockRate), false)...)
	prefix = append(prefix, pcrTestChunk(durationTicks(100*time.Millisecond, transportClockRate), true)...)
	prefix = append(prefix, pcrTestChunk(durationTicks(150*time.Millisecond, transportClockRate), false)...)
	if err := pacer.PrepareBufferedPrefix(prefix, chunkSize); !errors.Is(err, errTransportRelayBacklog) {
		t.Fatalf("multi-epoch validated prefix error=%v, want %v", err, errTransportRelayBacklog)
	}
}

func TestTransportPCRPacerRejectsRapidDistinctArrivalsBeyondMaximumRate(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	start := wall
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrSparseTestChunks(80, 50*time.Millisecond)
	minimumInterval := divideDurationCeil(50*time.Millisecond, maxTransportRecoveryRate)
	lastEmission := wall
	var got error
	for i, chunk := range chunks {
		arrivedAt := start.Add(time.Duration(i) * time.Millisecond)
		if wall.Before(arrivedAt) {
			wall = arrivedAt
		}
		got = pacer.Wait(context.Background(), chunk, arrivedAt)
		if got != nil {
			break
		}
		if i > 0 {
			if interval := wall.Sub(lastEmission); interval < minimumInterval {
				t.Fatalf("rapid distinct chunk %d interval=%s, want >=%s",
					i, interval, minimumInterval)
			}
		}
		lastEmission = wall
	}
	if !errors.Is(got, errTransportSkipStale) {
		t.Fatalf("rapid distinct arrivals error=%v, want %v", got, errTransportSkipStale)
	}
}

func TestTransportPCRPacerKeepsBackpressured24xAnd15xBurstsAtMediaCadence(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	started := wall
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	const mediaStep = 50 * time.Millisecond
	chunks := pcrSparseTestChunks(261, mediaStep)
	sourceArrival := started
	lastEmission := wall
	for i, chunk := range chunks[:241] {
		rate := 24
		if i >= 121 {
			rate = 15
		}
		if i > 0 {
			sourceArrival = sourceArrival.Add(divideDurationCeil(mediaStep, rate))
		}
		// A one-chunk handoff lets the origin/FFmpeg advertise its burst but
		// prevents subsequent reads from completing until the pacer advances.
		// Preserve that real read-completion time rather than manufacturing a
		// fresh timestamp inside the pacer.
		arrivedAt := sourceArrival
		if wall.After(arrivedAt) {
			arrivedAt = wall
		}
		if wall.Before(arrivedAt) {
			wall = arrivedAt
		}
		if err := pacer.Wait(context.Background(), chunk, arrivedAt); err != nil {
			t.Fatalf("backpressured burst chunk %d rate=%dx: %v", i, rate, err)
		}
		if i > 0 {
			if interval := wall.Sub(lastEmission); interval != mediaStep {
				t.Fatalf("backpressured burst chunk %d interval=%s, want uncompressed %s",
					i, interval, mediaStep)
			}
		}
		if pacer.recoveryRate != 0 {
			t.Fatalf("backpressured burst chunk %d entered recovery rate %d",
				i, pacer.recoveryRate)
		}
		lastEmission = wall
	}
	if elapsed := wall.Sub(started); elapsed != 12*time.Second {
		t.Fatalf("burst media emitted in %s, want exact 12s PCR cadence", elapsed)
	}
	if sourceElapsed := sourceArrival.Sub(started); sourceElapsed >= wall.Sub(started) {
		t.Fatalf("fixture source elapsed=%s, want a real faster-than-media burst", sourceElapsed)
	}

	// Once the network-held burst drains, ordinary source arrivals become the
	// edge again without a clock reset, recovery epoch, or compressed handoff.
	for i := 241; i < len(chunks); i++ {
		sourceArrival = wall.Add(mediaStep)
		wall = sourceArrival
		if err := pacer.Wait(context.Background(), chunks[i], sourceArrival); err != nil {
			t.Fatalf("source-edge chunk %d: %v", i, err)
		}
		if pacer.recoveryRate != 0 {
			t.Fatalf("source-edge chunk %d retained recovery rate %d", i, pacer.recoveryRate)
		}
	}
	if lag := wall.Sub(sourceArrival); lag != 0 {
		t.Fatalf("post-burst source-edge lag=%s, want 0", lag)
	}
}

func TestTransportPCRPacerRejectsOversizedRecoveryTail(t *testing.T) {
	beforeEpisodes := metrics.RelaySkipAheadEpisodes.Value()
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrSparseTestChunks(241, 50*time.Millisecond)
	if err := pacer.Wait(context.Background(), chunks[0], wall); err != nil {
		t.Fatal(err)
	}
	wall = wall.Add(8500 * time.Millisecond)
	burstArrival := wall
	var got error
	for i := 1; i <= 240; i++ {
		got = pacer.Wait(context.Background(), chunks[i], burstArrival)
		if got != nil {
			break
		}
	}
	// Skip-ahead: the tail that cannot fit any bounded acceleration is
	// discarded to rejoin the live edge, never emitted past the latency bound
	// and never a failed attempt.
	if !errors.Is(got, errTransportSkipStale) {
		t.Fatalf("oversized recovery error=%v, want %v", got, errTransportSkipStale)
	}
	if !pacer.skipEpisode {
		t.Fatal("skip episode not armed on the oversized tail")
	}
	if elapsed := wall.Sub(burstArrival); elapsed > maxTransportAddedLatency {
		t.Fatalf("oversized recovery emitted past latency bound: %s", elapsed)
	}
	if delta := metrics.RelaySkipAheadEpisodes.Value() - beforeEpisodes; delta != 1 {
		t.Fatalf("skip-ahead episode metric delta=%d, want 1", delta)
	}
}

// A steady-state relay dequeues a multi-chunk batch whenever the reservoir
// coalesced reads while the pacer waited out the previous chunk, and the read
// loop paces every chunk of that batch against one shared paceArrival. The
// batch spans real media clock, the shared paceLimit does not move, and no
// recovery epoch opens because the reservoir kept the pacer fed — so before
// PrepareSharedArrivalBatch the 8th 150 ms-PCR chunk failed at exactly 975 ms,
// tearing down a healthy source for delivering a burst the reservoir was built
// to absorb.
func TestTransportPCRPacerPacesSharedArrivalBatch(t *testing.T) {
	const steady, batch = 10, 30
	chunks := pcrSparseTestChunks(steady+batch+1, 150*time.Millisecond)

	run := func(prepare bool) (int, time.Duration, error) {
		wall := time.Unix(1_700_000_000, 0)
		pacer := testTransportPCRPacer(false)
		pacer.now = func() time.Time { return wall }
		pacer.wait = func(_ context.Context, delay time.Duration) error {
			wall = wall.Add(delay)
			return nil
		}
		for i := 0; i < steady; i++ {
			if err := pacer.Wait(context.Background(), chunks[i], wall); err != nil {
				t.Fatalf("steady chunk %d: %v", i, err)
			}
			wall = wall.Add(150 * time.Millisecond)
		}
		batchArrival := wall
		if prepare {
			var whole []byte
			for _, c := range chunks[steady : steady+batch] {
				whole = append(whole, c...)
			}
			pacer.PrepareSharedArrivalBatch(whole, chunkSize)
		}
		for i := steady; i < steady+batch; i++ {
			if err := pacer.Wait(context.Background(), chunks[i], batchArrival); err != nil {
				return i - steady, wall.Sub(batchArrival), err
			}
		}
		// The next dequeue carries a fresh arrival at source cadence; the
		// accelerated epoch must hand back to normal pacing, not fail it.
		wall = wall.Add(150 * time.Millisecond)
		if err := pacer.Wait(context.Background(), chunks[steady+batch], wall); err != nil {
			return batch, wall.Sub(batchArrival), err
		}
		return batch + 1, wall.Sub(batchArrival), nil
	}

	emitted, _, err := run(false)
	if !errors.Is(err, errTransportSkipStale) {
		t.Fatalf("unprepared shared-arrival batch err=%v after %d chunks, want %v (without Prepare the batch tail is stale-skipped)",
			err, emitted, errTransportSkipStale)
	}

	emitted, span, err := run(true)
	if err != nil {
		t.Fatalf("prepared batch failed after %d chunks: %v", emitted, err)
	}
	if emitted != batch+1 {
		t.Fatalf("prepared batch emitted %d chunks, want %d", emitted, batch+1)
	}
	if span > maxTransportAddedLatency+150*time.Millisecond {
		t.Fatalf("prepared batch drained in %s, want within the shared latency budget", span)
	}
}

// A pause beyond the recoverable-debt ceiling now opens a live-edge skip
// episode instead of failing the attempt (skip-ahead): the stale chunk
// is discarded, and the attempt rejoins at the live edge with declared
// discontinuities.

func TestTransportPCRPacerSparseCadenceRejectsDeclaredClockResetBeforeFanout(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	arrival := wall
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrEveryNTestChunks(20, 5, 50*time.Millisecond)
	chunkStart := 15 * chunkSize
	packetOffset := ((chunkStart + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
	chunks[15][packetOffset-chunkStart+5] |= 0x80

	for i := 0; i < 15; i++ {
		if err := pacer.Wait(context.Background(), chunks[i], arrival); err != nil {
			t.Fatalf("pre-reset chunk %d: %v", i, err)
		}
	}
	if pacer.chunkStep != 10*time.Millisecond {
		t.Fatalf("learned chunk step=%s, want 10ms", pacer.chunkStep)
	}
	if err := pacer.Wait(context.Background(), chunks[15], arrival); !errors.Is(err, errTransportAttemptBoundary) {
		t.Fatalf("declared reset error=%v, want %v", err, errTransportAttemptBoundary)
	}
}

func TestTransportPCRPacerRecoveryWaitIsCancelable(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	if err := pacer.Wait(context.Background(), pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	}
	wall = wall.Add(8500 * time.Millisecond)
	if err := pacer.Wait(context.Background(), pcrTestChunk(
		durationTicks(50*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := pacer.Wait(ctx, pcrTestChunk(
		durationTicks(100*time.Millisecond, transportClockRate), false), wall)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recovery wait error=%v, want deadline", err)
	}
}

func TestTransportPCRPacerBoundsCorruptFutureJumpAndLongSessionMath(t *testing.T) {
	if got := pcrTicksDuration(durationTicks(34*time.Minute, transportClockRate)); got != 34*time.Minute {
		t.Fatalf("34-minute PCR conversion=%s, want exact duration", got)
	}
	pacer := testTransportPCRPacer(false)
	if err := testPCRWait(&pacer, context.Background(), pcrTestChunk(0, false)); err != nil {
		t.Fatal(err)
	}
	jump := pcrTestChunk(durationTicks(30*time.Second, transportClockRate), false)
	started := time.Now()
	if err := testPCRWait(&pacer, context.Background(), jump); !errors.Is(err, errTransportAttemptBoundary) {
		t.Fatalf("corrupt jump error=%v, want %v", err, errTransportAttemptBoundary)
	}
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("corrupt PCR jump slept %s instead of resetting", elapsed)
	}
	if jump[5]&0x80 != 0 {
		t.Fatal("corrupt PCR jump was mutated instead of rejected before fanout")
	}
}

func TestTransportPCRPacerRejectsClockBoundariesBeforePayloadOnlyAVFanout(t *testing.T) {
	const (
		videoPID = 0x101
		audioPID = 0x102
	)
	for _, tc := range []struct {
		name          string
		initial       time.Duration
		boundary      time.Duration
		discontinuity bool
	}{
		{name: "declared reset", initial: 10 * time.Second, boundary: 11 * time.Second, discontinuity: true},
		{name: "undeclared backward epoch", initial: 10 * time.Second, boundary: 9 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pacer := testTransportPCRPacer(false)
			if err := testPCRWait(&pacer, context.Background(), pcrTestChunk(
				durationTicks(tc.initial, transportClockRate), false)); err != nil {
				t.Fatal(err)
			}
			boundary := joinTSPackets(
				pcrTestChunk(durationTicks(tc.boundary, transportClockRate), tc.discontinuity),
				testPayloadOnlyTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 0),
				testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 0),
			)
			original := append([]byte(nil), boundary...)
			err := testPCRWait(&pacer, context.Background(), boundary)
			if !errors.Is(err, errTransportAttemptBoundary) {
				t.Fatalf("boundary error=%v, want %v", err, errTransportAttemptBoundary)
			}
			if !bytes.Equal(boundary, original) {
				t.Fatal("rejected boundary chunk was mutated before the caller withheld fanout")
			}
			for _, pid := range []int{videoPID, audioPID} {
				if packet := firstTSPacketForPID(boundary, pid); packet == nil || (packet[3]>>4)&0x03 != 1 {
					t.Fatalf("PID %#x fixture is not payload-only", pid)
				}
			}
		})
	}
}

func TestTransportPCRPacerRejectsAnySelectedProgramDiscontinuityBeforeFanout(t *testing.T) {
	const (
		pcrPID   = 0x100
		videoPID = 0x101
		audioPID = 0x102
	)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: pcrPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID

	for _, tc := range []struct {
		name string
		pid  int
	}{
		{name: "adaptation-only PCR without PCR sample", pid: pcrPID},
		{name: "selected video", pid: videoPID},
		{name: "selected audio", pid: audioPID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pacer := testTransportPCRPacer(false)
			pacer.ConfigureSelectedProgram(program, true)
			// These are the private gate's decoder-visible initial boundaries.
			// They must establish, not recursively reject, the fresh attempt.
			initial := joinTSPackets(
				testAdaptationOnlyDiscontinuity(videoPID, 0),
				testPayloadOnlyTimestampedPESPacket(videoPID, 0xe0, 0, 0),
				testAdaptationOnlyDiscontinuity(audioPID, 0),
				testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, 0, 0),
				testAdaptationOnlyDiscontinuity(pcrPID, 0),
				pcrTestChunkForPID(pcrPID, 0, false),
			)
			prepared, err := pacer.TransformForFanout(initial)
			if err != nil {
				t.Fatalf("initial selected-program transform failed: %v", err)
			}
			if err := testPCRWait(&pacer, context.Background(), prepared); err != nil {
				t.Fatalf("initial selected-program markers self-triggered: %v", err)
			}

			boundary := testAdaptationOnlyDiscontinuity(tc.pid, 1)
			original := append([]byte(nil), boundary...)
			if _, err := pacer.TransformForFanout(boundary); !errors.Is(err, errTransportAttemptBoundary) {
				t.Fatalf("selected PID %#x boundary error=%v, want %v",
					tc.pid, err, errTransportAttemptBoundary)
			}
			if !bytes.Equal(boundary, original) {
				t.Fatal("selected boundary was mutated before the caller withheld fanout")
			}
			if tc.pid == pcrPID && boundary[5]&0x10 != 0 {
				t.Fatal("PCR boundary fixture unexpectedly carried a PCR sample")
			}
		})
	}
}

func TestSelectedProgramBoundaryScannerHandlesArbitraryByteSplits(t *testing.T) {
	const videoPID = 0x101
	program := avProgramPIDs{videoPID: videoPID, pcrPID: -1}
	var scanner selectedProgramBoundaryScanner
	scanner.configure(program, true)
	stream := joinTSPackets(
		testAdaptationOnlyDiscontinuity(videoPID, 0),
		testPayloadOnlyTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffe, false),
		testAdaptationOnlyDiscontinuity(videoPID, 1),
	)
	found := false
	for offset := 0; offset < len(stream); {
		end := min(offset+137, len(stream))
		if _, err := scanner.transform(stream[offset:end]); errors.Is(err, errTransportAttemptBoundary) {
			found = true
			break
		}
		offset = end
	}
	if !found {
		t.Fatal("split selected-video discontinuity did not become an attempt boundary")
	}
}

func TestSelectedProgramBoundaryTransformMarksLateSelectedPIDs(t *testing.T) {
	const (
		pcrPID     = 0x100
		videoPID   = 0x101
		audioPID   = 0x102
		slateAudio = 0x120
	)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: pcrPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID
	initial := joinTSPackets(
		testAdaptationOnlyDiscontinuity(videoPID, 0),
		testPayloadOnlyTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffe, false))

	transformSplit := func(t *testing.T, pacer *transportPCRPacer, data []byte, step int) ([]byte, error) {
		t.Helper()
		var output []byte
		for offset := 0; offset < len(data); {
			end := min(offset+step, len(data))
			part, err := pacer.TransformForFanout(data[offset:end])
			if err != nil {
				return output, err
			}
			output = append(output, part...)
			offset = end
		}
		return output, nil
	}
	newPacer := func(t *testing.T) *transportPCRPacer {
		t.Helper()
		pacer := testTransportPCRPacer(false)
		pacer.ConfigureSelectedProgram(program, true)
		prepared, err := transformSplit(t, &pacer, initial, 137)
		if err != nil {
			t.Fatalf("initial transform: %v", err)
		}
		if !bytes.Equal(prepared, initial) {
			t.Fatal("already-marked initial video prefix changed")
		}
		return &pacer
	}
	assertMarkerThenPacket := func(t *testing.T, output, original []byte, pid int) {
		t.Helper()
		if len(output) != 2*tsPacketSize {
			t.Fatalf("late PID output bytes=%d, want marker+packet=%d", len(output), 2*tsPacketSize)
		}
		marker := output[:tsPacketSize]
		packet := output[tsPacketSize:]
		if gotPID := int(marker[1]&0x1f)<<8 | int(marker[2]); gotPID != pid ||
			(marker[3]>>4)&0x03 != 2 || marker[5]&0x80 == 0 {
			t.Fatalf("PID %#x missing adaptation-only initial marker: %x", pid, marker[:6])
		}
		if marker[3]&0x0f != original[3]&0x0f {
			t.Fatalf("PID %#x marker CC=%d, packet CC=%d", pid, marker[3]&0x0f, original[3]&0x0f)
		}
		if !bytes.Equal(packet, original) {
			t.Fatalf("PID %#x first late packet changed", pid)
		}
	}

	for _, tc := range []struct {
		name string
		late func() []byte
	}{
		{name: "payload-only late audio", late: func() []byte {
			return testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, 0, 7)
		}},
		{name: "adaptation-and-payload late audio", late: func() []byte {
			return testExactPayloadPacket(audioPID, true, 7, slateTestPES(0xc0, 0, 0, false))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pacer := newPacer(t)
			late := tc.late()
			output, err := transformSplit(t, pacer, late, 37)
			if err != nil {
				t.Fatalf("late audio transform: %v", err)
			}
			assertMarkerThenPacket(t, output, late, audioPID)

			// Model old slate audio immediately before the new source's late PID.
			// The inserted marker must be the first new audio packet after the
			// source/slate concatenation boundary.
			combined := joinTSPackets(
				testPayloadOnlyTimestampedPESPacket(slateAudio, 0xc0, 10*transportPTSRate, 3),
				output)
			if marker := combined[tsPacketSize : 2*tsPacketSize]; marker[5]&0x80 == 0 {
				t.Fatal("source/slate concatenation lacks a boundary before late audio")
			}

			ordinary := testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 8)
			if got, err := pacer.TransformForFanout(ordinary); err != nil || !bytes.Equal(got, ordinary) {
				t.Fatalf("ordinary post-marker audio=(bytes=%d err=%v), want unchanged", len(got), err)
			}
			if _, err := pacer.TransformForFanout(testAdaptationOnlyDiscontinuity(audioPID, 9)); !errors.Is(err, errTransportAttemptBoundary) {
				t.Fatalf("subsequent real audio discontinuity=%v, want attempt boundary", err)
			}
		})
	}

	t.Run("distinct late PCR", func(t *testing.T) {
		pacer := newPacer(t)
		late := pcrTestChunkForPID(pcrPID, 0, false)[:tsPacketSize]
		late[3] = late[3]&0xf0 | 9
		output, err := transformSplit(t, pacer, late, 73)
		if err != nil {
			t.Fatalf("late PCR transform: %v", err)
		}
		assertMarkerThenPacket(t, output, late, pcrPID)
		ordinary := pcrTestChunkForPID(
			pcrPID, durationTicks(time.Second, transportClockRate), false)[:tsPacketSize]
		if _, err := pacer.TransformForFanout(ordinary); err != nil {
			t.Fatalf("ordinary PCR after initial marker self-triggered: %v", err)
		}
		if _, err := pacer.TransformForFanout(testAdaptationOnlyDiscontinuity(pcrPID, 1)); !errors.Is(err, errTransportAttemptBoundary) {
			t.Fatalf("subsequent real PCR discontinuity=%v, want attempt boundary", err)
		}
	})

	t.Run("first explicit late boundary establishes rather than self-triggers", func(t *testing.T) {
		pacer := newPacer(t)
		first := testAdaptationOnlyDiscontinuity(audioPID, 7)
		if output, err := transformSplit(t, pacer, first, 29); err != nil || !bytes.Equal(output, first) {
			t.Fatalf("first explicit late marker=(bytes=%d err=%v), want accepted unchanged", len(output), err)
		}
		if _, err := pacer.TransformForFanout(testAdaptationOnlyDiscontinuity(audioPID, 7)); !errors.Is(err, errTransportAttemptBoundary) {
			t.Fatalf("second explicit audio boundary=%v, want attempt boundary", err)
		}
	})
}

func TestSelectedProgramInitializationMarkersCannotEraseGrossAVFault(t *testing.T) {
	const (
		videoPID    = 0x0100
		firstAudio  = 0x0101
		secondAudio = 0x0102
		distinctPCR = 0x0103
	)
	base := time.Unix(1_700_000_093, 0)
	assertWithheldSkew := func(t *testing.T, program avProgramPIDs, input []byte) {
		t.Helper()
		var scanner selectedProgramBoundaryScanner
		scanner.configure(program, true)
		transformed, err := scanner.transform(input)
		if err != nil {
			t.Fatal(err)
		}
		var gate publicationAVGate
		gate.bindSelectedProgram(program, base, false)
		out, _, fault := gate.push(transformed, base)
		if len(out) != 0 || fault == nil || fault.kind != avFaultTimelineSkew {
			t.Fatalf("initial marker sequence=(bytes=%d fault=%v), want unpublished gross skew", len(out), fault)
		}
	}

	t.Run("later language marker cannot erase an earlier poisoned track", func(t *testing.T) {
		program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 2}
		for i := range program.audioPIDs {
			program.audioPIDs[i] = -1
		}
		program.audioPIDs[0], program.audioPIDs[1] = firstAudio, secondAudio
		assertWithheldSkew(t, program, joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 60*transportPTSRate, 0),
			testTimestampedPESPacket(firstAudio, 0xc0, (60+9*60)*transportPTSRate, 0),
			testTimestampedPESPacket(secondAudio, 0xc1, 60*transportPTSRate, 0),
			testTimestampedPESPacket(videoPID, 0xe0, 60*transportPTSRate+durationTicks(40*time.Millisecond, transportPTSRate), 1),
			testTimestampedPESPacket(secondAudio, 0xc1, 60*transportPTSRate+durationTicks(40*time.Millisecond, transportPTSRate), 1),
			pcrTestNonPCRPacket(0x1fff, false),
		))
	})

	t.Run("distinct PCR marker cannot erase an earlier poisoned track", func(t *testing.T) {
		program := avProgramPIDs{videoPID: videoPID, pcrPID: distinctPCR, audioCount: 1}
		for i := range program.audioPIDs {
			program.audioPIDs[i] = -1
		}
		program.audioPIDs[0] = firstAudio
		pcr := pcrTestChunkForPID(distinctPCR, 0, false)[:tsPacketSize]
		pcr[3] = pcr[3]&0xf0 | 9
		assertWithheldSkew(t, program, joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 60*transportPTSRate, 0),
			testTimestampedPESPacket(firstAudio, 0xc0, (60+9*60)*transportPTSRate, 0),
			pcr,
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1ffe, false),
		))
	})
}

func TestSelectedProgramBoundaryTransformDoesNotDelayAudioLessStartup(t *testing.T) {
	const videoPID = 0x101
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID}
	pacer := testTransportPCRPacer(false)
	pacer.ConfigureSelectedProgram(program, true)
	initial := joinTSPackets(
		testAdaptationOnlyDiscontinuity(videoPID, 0),
		testPayloadOnlyTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffe, false))
	output, err := pacer.TransformForFanout(initial)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output, initial) {
		t.Fatal("audio-less startup was buffered or rewritten after its video boundary")
	}
}

func TestSelectedProgramBoundaryTransformRejectsChangedProgramMapBeforeFanout(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	patSection := testPATSection(pmtPID)
	pmtPacket := testAVPMTPacket(pmtPID, videoPID, audioPID)
	pmtPayload, ok := tsPayload(pmtPacket)
	if !ok {
		t.Fatal("PMT fixture has no payload")
	}
	pmtSection := append([]byte(nil), pmtPayload[1+int(pmtPayload[0]):]...)
	pmtTotal := 3 + (int(pmtSection[1]&0x0f)<<8 | int(pmtSection[2]))
	pmtSection = pmtSection[:pmtTotal]
	identity := selectedProgramMapIdentity{
		transportStreamID: 1,
		patVersion:        0,
		patSections:       [][]byte{append([]byte(nil), patSection...)},
		programNumber:     1,
		pmtPID:            pmtPID,
		pmtSection:        append([]byte(nil), pmtSection...),
	}
	program := avProgramPIDs{
		videoPID: videoPID, pcrPID: videoPID, audioCount: 1, mapIdentity: identity,
	}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID

	newPacer := func(t *testing.T) *transportPCRPacer {
		t.Helper()
		pacer := testTransportPCRPacer(false)
		pacer.ConfigureSelectedProgram(program, true)
		initial := joinTSPackets(
			testPATPacket(pmtPID), pmtPacket,
			testAdaptationOnlyDiscontinuity(videoPID, 0),
			testPayloadOnlyTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			pcrTestNonPCRPacket(0x1ffe, false),
			pcrTestNonPCRPacket(0x1ffe, false))
		if _, err := pacer.TransformForFanout(initial); err != nil {
			t.Fatalf("initial exact programme map: %v", err)
		}
		return &pacer
	}
	transformSplit := func(pacer *transportPCRPacer, data []byte, step int) ([]byte, error) {
		var output []byte
		for offset := 0; offset < len(data); {
			end := min(offset+step, len(data))
			part, err := pacer.TransformForFanout(data[offset:end])
			if err != nil {
				return output, err
			}
			output = append(output, part...)
			offset = end
		}
		return output, nil
	}

	t.Run("identical PAT and split PMT repeat", func(t *testing.T) {
		pacer := newPacer(t)
		repeatPAT := testPATPacketForTableContinuity(1, 1, 0, true, 0, 0,
			testPATProgram{programNumber: 1, pmtPID: pmtPID})
		if got, err := transformSplit(pacer, repeatPAT, 41); err != nil || !bytes.Equal(got, repeatPAT) {
			t.Fatalf("identical PAT repeat=(bytes=%d err=%v)", len(got), err)
		}
		packets := testPSISectionPackets(pmtPID, 1, bytes.Repeat([]byte{0}, 170), pmtSection)
		repeatPMT := joinTSPackets(packets...)
		got, err := transformSplit(pacer, repeatPMT, 53)
		if err != nil || !bytes.Equal(got, repeatPMT) {
			t.Fatalf("split identical PMT repeat=(bytes=%d err=%v), want %d unchanged",
				len(got), err, len(repeatPMT))
		}
	})

	t.Run("incomplete runtime PMT cannot silence fanout past live deadline", func(t *testing.T) {
		pacer := newPacer(t)
		base := time.Unix(1_700_000_120, 0)
		// A syntactically plausible maximum-sized current PMT that never
		// completes keeps HTTP bytes flowing but cannot safely be shown to Plex.
		incomplete := testExactPayloadPacket(pmtPID, true, 1,
			[]byte{0, 0x02, 0xb3, 0xfd, 0, 1, 0xc1, 0, 0})
		if got, err := pacer.TransformForFanoutAt(incomplete[:67], base); err != nil || len(got) != 0 {
			t.Fatalf("partial TS fragment=(bytes=%d err=%v), want private", len(got), err)
		}
		if got, err := pacer.TransformForFanoutAt(incomplete[67:], base); err != nil || len(got) != 0 {
			t.Fatalf("incomplete PMT=(bytes=%d err=%v), want private", len(got), err)
		}
		ordinary := pcrTestNonPCRPacket(0x1ffe, false)
		if got, err := pacer.TransformForFanoutAt(
			ordinary, base.Add(maxPublicationAVGateHold-time.Millisecond)); err != nil || len(got) != 0 {
			t.Fatalf("pre-deadline map hold=(bytes=%d err=%v), want private", len(got), err)
		}
		got, err := pacer.TransformForFanoutAt(
			ordinary, base.Add(maxPublicationAVGateHold+time.Millisecond))
		if len(got) != 0 || !errors.Is(err, errTransportProgramMapStall) {
			t.Fatalf("expired map hold=(bytes=%d err=%v), want typed unpublished stall", len(got), err)
		}
		if isLocalRelayRetry(err) || !requiresImmediateContinuityFiller(err) ||
			!shouldMarkSourceFailure(err) {
			t.Fatalf("map-stall attribution local=%v immediate=%v source=%v",
				isLocalRelayRetry(err), requiresImmediateContinuityFiller(err), shouldMarkSourceFailure(err))
		}
	})

	t.Run("high-rate incomplete runtime PMT retains provider attribution", func(t *testing.T) {
		pacer := newPacer(t)
		incomplete := testExactPayloadPacket(pmtPID, true, 1,
			[]byte{0, 0x02, 0xb3, 0xfd, 0, 1, 0xc1, 0, 0})
		nullPacket := pcrTestNonPCRPacket(0x1ffe, false)
		count := maxSelectedProgramMapHoldBytes/tsPacketSize + 2
		burst := make([]byte, 0, len(incomplete)+count*tsPacketSize)
		burst = append(burst, incomplete...)
		for i := 0; i < count; i++ {
			burst = append(burst, nullPacket...)
		}
		got, err := pacer.TransformForFanoutAt(burst, time.Unix(1_700_000_122, 0))
		if len(got) != 0 || !errors.Is(err, errTransportProgramMapStall) {
			t.Fatalf("map byte-cap=(bytes=%d err=%v), want typed unpublished stall", len(got), err)
		}
		if isLocalRelayRetry(err) || !shouldMarkSourceFailure(err) ||
			!requiresImmediateContinuityFiller(err) {
			t.Fatalf("map byte-cap attribution local=%v source=%v immediate=%v",
				isLocalRelayRetry(err), shouldMarkSourceFailure(err), requiresImmediateContinuityFiller(err))
		}
	})

	addProgramDescriptor := func(section []byte) []byte {
		withDescriptor := make([]byte, len(section)+2)
		copy(withDescriptor[:12], section[:12])
		withDescriptor[10], withDescriptor[11] = 0xf0, 2
		withDescriptor[12], withDescriptor[13] = 0x52, 0
		copy(withDescriptor[14:], section[12:len(section)-4])
		sectionLength := len(withDescriptor) - 3
		withDescriptor[1] = 0xb0 | byte(sectionLength>>8)&0x0f
		withDescriptor[2] = byte(sectionLength)
		writeMPEG2CRC(withDescriptor)
		return withDescriptor
	}
	mutations := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "version", mutate: func(section []byte) []byte {
			section[5] = section[5]&0xc1 | 1<<1
			writeMPEG2CRC(section)
			return section
		}},
		{name: "PCR PID", mutate: func(section []byte) []byte {
			section[8], section[9] = 0xe1, 0x10
			writeMPEG2CRC(section)
			return section
		}},
		{name: "audio PID", mutate: func(section []byte) []byte {
			section[18], section[19] = 0xe1, 0x11
			writeMPEG2CRC(section)
			return section
		}},
		{name: "video codec", mutate: func(section []byte) []byte {
			section[12] = 0x24
			writeMPEG2CRC(section)
			return section
		}},
		{name: "descriptor", mutate: addProgramDescriptor},
	}
	for _, tc := range mutations {
		t.Run("changed "+tc.name, func(t *testing.T) {
			pacer := newPacer(t)
			changed := tc.mutate(append([]byte(nil), pmtSection...))
			packets := testPSISectionPackets(pmtPID, 1, bytes.Repeat([]byte{0}, 170), changed)
			feed := joinTSPackets(packets...)
			got, err := transformSplit(pacer, feed, 47)
			if !errors.Is(err, errTransportAttemptBoundary) {
				t.Fatalf("changed split PMT err=%v, want boundary", err)
			}
			if len(got) != 0 {
				t.Fatalf("changed split PMT released %d bytes before private restart", len(got))
			}
		})
	}

	t.Run("changed PAT version and programme rebind", func(t *testing.T) {
		for _, changedPAT := range [][]byte{
			testPATPacketForTableContinuity(1, 1, 1, true, 0, 0,
				testPATProgram{programNumber: 1, pmtPID: pmtPID}),
			testPATPacketForTableContinuity(1, 1, 0, true, 0, 0,
				testPATProgram{programNumber: 1, pmtPID: pmtPID + 1}),
		} {
			pacer := newPacer(t)
			got, err := transformSplit(pacer, changedPAT, 37)
			if !errors.Is(err, errTransportAttemptBoundary) || len(got) != 0 {
				t.Fatalf("changed PAT=(bytes=%d err=%v), want unpublished boundary", len(got), err)
			}
		}
	})

	t.Run("inactive future PMT is ignored but malformed active PSI is withheld", func(t *testing.T) {
		pacer := newPacer(t)
		future := append([]byte(nil), pmtSection...)
		future[5] &^= 0x01
		future[3], future[4] = 0, 2 // a different future subtable is still inactive
		future[18], future[19] = 0xe1, 0x11
		writeMPEG2CRC(future)
		futurePacket := testExactPayloadPacket(pmtPID, true, 1, append([]byte{0}, future...))
		if got, err := transformSplit(pacer, futurePacket, 31); err != nil || !bytes.Equal(got, futurePacket) {
			t.Fatalf("inactive future PMT=(bytes=%d err=%v), want ignored unchanged", len(got), err)
		}

		badCRC := append([]byte(nil), pmtSection...)
		badCRC[len(badCRC)-1] ^= 0xff
		badPacket := testExactPayloadPacket(pmtPID, true, 2, append([]byte{0}, badCRC...))
		if got, err := transformSplit(pacer, badPacket, 29); !errors.Is(err, errTransportAttemptBoundary) || len(got) != 0 {
			t.Fatalf("CRC-invalid PMT=(bytes=%d err=%v), want unpublished boundary", len(got), err)
		}

		for name, packet := range map[string][]byte{
			"CRC-invalid PAT": func() []byte {
				section := append([]byte(nil), patSection...)
				section[len(section)-1] ^= 0xff
				return testExactPayloadPacket(0, true, 1, append([]byte{0}, section...))
			}(),
			"invalid PMT section length": func() []byte {
				section := []byte{0x02, 0xb4, 0x00}
				return testExactPayloadPacket(pmtPID, true, 1, append([]byte{0}, section...))
			}(),
			"empty PAT PUSI": testExactPayloadPacket(0, true, 1, []byte{0}),
			"empty PMT PUSI": testExactPayloadPacket(pmtPID, true, 1, []byte{0}),
			"stuffing-only PAT PUSI": testExactPayloadPacket(0, true, 1,
				append([]byte{0}, bytes.Repeat([]byte{0xff}, 8)...)),
			"stuffing-only PMT PUSI": testExactPayloadPacket(pmtPID, true, 1,
				append([]byte{0}, bytes.Repeat([]byte{0xff}, 8)...)),
		} {
			pacer := newPacer(t)
			if got, err := transformSplit(pacer, packet, 23); !errors.Is(err, errTransportAttemptBoundary) || len(got) != 0 {
				t.Fatalf("%s=(bytes=%d err=%v), want unpublished boundary", name, len(got), err)
			}
		}
	})

	t.Run("split PSI replacement and composite outcomes remain private", func(t *testing.T) {
		for name, fixture := range map[string]struct {
			pid     int
			section []byte
		}{
			"PAT": {pid: 0, section: patSection},
			"PMT": {pid: pmtPID, section: pmtSection},
		} {
			t.Run(name+" incomplete replacement", func(t *testing.T) {
				pacer := newPacer(t)
				packets := testPSISectionPackets(fixture.pid, 1,
					bytes.Repeat([]byte{0}, 170), fixture.section)
				if len(packets) < 2 {
					t.Fatal("split PSI fixture did not span packets")
				}
				if got, err := transformSplit(pacer, packets[0], 43); err != nil || len(got) != 0 {
					t.Fatalf("incomplete PSI prefix=(bytes=%d err=%v), want privately held", len(got), err)
				}
				replacement := testExactPayloadPacket(fixture.pid, true, 2,
					append([]byte{0}, fixture.section...))
				if got, err := transformSplit(pacer, replacement, 37); !errors.Is(err, errTransportAttemptBoundary) || len(got) != 0 {
					t.Fatalf("PUSI replacement=(bytes=%d err=%v), want old incomplete bytes withheld", len(got), err)
				}
			})
		}

		t.Run("exact first section plus malformed trailing section", func(t *testing.T) {
			for _, fixture := range []struct {
				pid     int
				section []byte
			}{
				{pid: 0, section: patSection},
				{pid: pmtPID, section: pmtSection},
			} {
				pacer := newPacer(t)
				payload := append([]byte{0}, fixture.section...)
				payload = append(payload, 0x02, 0xb4, 0x00)
				packet := testExactPayloadPacket(fixture.pid, true, 1, payload)
				if got, err := transformSplit(pacer, packet, 31); !errors.Is(err, errTransportAttemptBoundary) || len(got) != 0 {
					t.Fatalf("composite malformed PSI PID %#x=(bytes=%d err=%v)", fixture.pid, len(got), err)
				}
			}
		})

		t.Run("legal pointer completion followed by exact section", func(t *testing.T) {
			pacer := newPacer(t)
			packets := testPSISectionPackets(pmtPID, 1,
				bytes.Repeat([]byte{0}, 170), pmtSection)
			first := packets[0]
			if got, err := transformSplit(pacer, first, 41); err != nil || len(got) != 0 {
				t.Fatalf("first split PMT=(bytes=%d err=%v), want held", len(got), err)
			}
			firstPayload, _ := tsPayload(first)
			sectionBytesInFirst := len(firstPayload) - 1 - int(firstPayload[0])
			remaining := pmtSection[sectionBytesInFirst:]
			payload := []byte{byte(len(remaining))}
			payload = append(payload, remaining...)
			payload = append(payload, pmtSection...)
			completion := testExactPayloadPacket(pmtPID, true, 2, payload)
			got, err := transformSplit(pacer, completion, 37)
			want := joinTSPackets(first, completion)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("legal pointer completion=(bytes=%d err=%v), want %d released", len(got), err, len(want))
			}
		})
	})
}

func TestTransportPCRPacerBindsConfiguredMPTSProgramPCR(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	var waited time.Duration
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		waited += delay
		wall = wall.Add(delay)
		return nil
	}

	first := append(
		pcrTestChunkForPID(0x080, durationTicks(5*time.Second, transportClockRate), false),
		pcrTestChunkForPID(0x100, 0, false)...,
	)
	if err := pacer.Wait(context.Background(), first, wall); err != nil {
		t.Fatal(err)
	}
	second := append(
		pcrTestChunkForPID(0x080, durationTicks(9*time.Second, transportClockRate), false),
		pcrTestChunkForPID(0x100, durationTicks(120*time.Millisecond, transportClockRate), false)...,
	)
	if err := pacer.Wait(context.Background(), second, wall); err != nil {
		t.Fatal(err)
	}
	if waited < 119*time.Millisecond || waited > 121*time.Millisecond {
		t.Fatalf("MPTS pace wait=%s, want selected program PCR cadence 120ms", waited)
	}
}

func TestTransportPCRPacerCapsSustainedFastLiveClockWithoutResetOrBurst(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	sourceStart := wall
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	waits := make([]time.Duration, 0, 120)
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		wall = wall.Add(delay)
		return nil
	}
	beforeCaps := metrics.RelayPacingLatencyCaps.Value()
	beforeCappedMillis := metrics.RelayPacingCappedMillis.Value()
	beforeResets := metrics.RelayClockResets.Value()

	var maxLatency time.Duration
	for i := 0; i < 120; i++ {
		arrivedAt := sourceStart.Add(time.Duration(i) * 50 * time.Millisecond)
		if wall.Before(arrivedAt) {
			wall = arrivedAt
		}
		clock := time.Duration(i) * 70 * time.Millisecond // sustained 1.4x PCR clock
		if err := pacer.Wait(context.Background(), pcrTestChunk(
			durationTicks(clock, transportClockRate), false), arrivedAt); err != nil {
			t.Fatal(err)
		}
		if latency := wall.Sub(arrivedAt); latency > maxLatency {
			maxLatency = latency
		}
	}
	if maxLatency > maxTransportAddedLatency+time.Millisecond {
		t.Fatalf("fast live PCR accumulated %s latency, cap=%s", maxLatency, maxTransportAddedLatency)
	}
	if delta := metrics.RelayPacingLatencyCaps.Value() - beforeCaps; delta == 0 {
		t.Fatal("fast live PCR hit latency bound without an observable cap event")
	}
	if delta := metrics.RelayPacingCappedMillis.Value() - beforeCappedMillis; delta == 0 {
		t.Fatal("fast live PCR discarded schedule debt without an observable duration")
	}
	if delta := metrics.RelayClockResets.Value() - beforeResets; delta != 0 {
		t.Fatalf("sustained PCR-rate correction was mislabeled as %d clock resets", delta)
	}
	if len(waits) < 10 {
		t.Fatalf("only %d paced waits observed", len(waits))
	}
	for i, delay := range waits[len(waits)-10:] {
		if delay < 49*time.Millisecond || delay > 51*time.Millisecond {
			t.Fatalf("post-cap wait[%d]=%s, want source-cadence 50ms without catch-up burst", i, delay)
		}
	}
}

func TestTransportPCRPacerFiniteReplayKeepsFullCadencePastLiveCap(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	sourceStart := wall
	pacer := testTransportPCRPacer(true)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	beforeCaps := metrics.RelayPacingLatencyCaps.Value()
	for i := 0; i < 80; i++ {
		arrivedAt := sourceStart.Add(time.Duration(i) * 50 * time.Millisecond)
		if wall.Before(arrivedAt) {
			wall = arrivedAt
		}
		clock := time.Duration(i) * 70 * time.Millisecond
		if err := pacer.Wait(context.Background(), pcrTestChunk(
			durationTicks(clock, transportClockRate), false), arrivedAt); err != nil {
			t.Fatal(err)
		}
	}
	if delta := metrics.RelayPacingLatencyCaps.Value() - beforeCaps; delta != 0 {
		t.Fatalf("finite replay unexpectedly used live latency cap %d times", delta)
	}
	want := 79 * 70 * time.Millisecond
	if elapsed := wall.Sub(sourceStart); elapsed < want-time.Millisecond || elapsed > want+time.Millisecond {
		t.Fatalf("finite replay elapsed=%s, want full PCR cadence %s", elapsed, want)
	}
}

func TestTransportPCRPacerFiniteReplayPacesSparsePCRPastLiveCap(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	started := wall
	pacer := testTransportPCRPacer(true)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	beforeCaps := metrics.RelayPacingLatencyCaps.Value()
	chunks := pcrEveryNTestChunks(31, 5, 250*time.Millisecond)
	for i, chunk := range chunks {
		if err := pacer.Wait(context.Background(), chunk, started); err != nil {
			t.Fatalf("finite sparse chunk %d: %v", i, err)
		}
	}
	if elapsed := wall.Sub(started); elapsed != 1500*time.Millisecond {
		t.Fatalf("finite sparse replay elapsed=%s, want full 1.5s PCR cadence", elapsed)
	}
	if delta := metrics.RelayPacingLatencyCaps.Value() - beforeCaps; delta != 0 {
		t.Fatalf("finite sparse replay used live latency cap %d times", delta)
	}
}

func TestTransportPCRPacerFiniteReplayKeepsLowBitratePCRSteps(t *testing.T) {
	wall := time.Unix(1_700_000_000, 0)
	sourceArrival := wall
	pacer := testTransportPCRPacer(true)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	beforeResets := metrics.RelayClockResets.Value()
	for _, clock := range []time.Duration{0, 4200 * time.Millisecond, 9700 * time.Millisecond} {
		chunk := pcrTestChunk(durationTicks(clock, transportClockRate), false)
		if err := pacer.Wait(context.Background(), chunk, sourceArrival); err != nil {
			t.Fatal(err)
		}
		if chunk[5]&0x80 != 0 {
			t.Fatalf("valid finite PCR step at %s was marked as a discontinuity", clock)
		}
	}
	if elapsed := wall.Sub(sourceArrival); elapsed != 9700*time.Millisecond {
		t.Fatalf("finite low-bitrate replay elapsed=%s, want full PCR cadence 9.7s", elapsed)
	}
	if delta := metrics.RelayClockResets.Value() - beforeResets; delta != 0 {
		t.Fatalf("valid finite low-bitrate PCR steps caused %d clock resets", delta)
	}
}

func TestTransportPCRPacerWaitIsCancelable(t *testing.T) {
	pacer := testTransportPCRPacer(false)
	if err := testPCRWait(&pacer, context.Background(), pcrTestChunk(0, false)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := testPCRWait(&pacer, ctx, pcrTestChunk(durationTicks(time.Second, transportClockRate), false))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pacer error=%v, want deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("pacer cancellation took %s", elapsed)
	}
}

func testTransportPCRPacer(finiteReplay bool) transportPCRPacer {
	var pacer transportPCRPacer
	pacer.Configure(0x100, finiteReplay)
	return pacer
}

func testPCRWait(pacer *transportPCRPacer, ctx context.Context, data []byte) error {
	return pacer.Wait(ctx, data, time.Now())
}

func firstTSPacketForPID(data []byte, wanted int) []byte {
	for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] == 0x47 && int(packet[1]&0x1f)<<8|int(packet[2]) == wanted {
			return packet
		}
	}
	return nil
}

func pcrTestChunk(raw uint64, discontinuity bool) []byte {
	return pcrTestChunkForPID(0x100, raw, discontinuity)
}

func pcrTestChunkForPID(pid int, raw uint64, discontinuity bool) []byte {
	chunk := make([]byte, 0, tsPacketSize*transportSyncPacketCount)
	for i := 0; i < transportSyncPacketCount; i++ {
		packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
		packet[0] = 0x47
		packet[1] = byte((pid >> 8) & 0x1f)
		packet[2] = byte(pid)
		packet[3] = 0x30 | byte(i&0x0f)
		packet[4] = 7
		packet[5] = 0x10
		if discontinuity && i == 0 {
			packet[5] |= 0x80
		}
		copy(packet[6:12], slateTestPCR(raw))
		chunk = append(chunk, packet...)
	}
	return chunk
}

func TestPCRClockScannerRejectsMalformedAdaptationClocks(t *testing.T) {
	build := func(mutate func([]byte)) []byte {
		packets := make([][]byte, 0, transportSyncPacketCount)
		for i := 0; i < transportSyncPacketCount; i++ {
			packet := append([]byte(nil), pcrTestChunkForPID(0x100, uint64(i)*27_000, false)[:tsPacketSize]...)
			mutate(packet)
			packets = append(packets, packet)
		}
		return joinTSPackets(packets...)
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "reserved bits", mutate: func(packet []byte) { packet[10] &^= 0x7e }},
		{name: "extension 300", mutate: func(packet []byte) {
			packet[10] = packet[10]&0xfe | 0x01
			packet[11] = 44
		}},
		{name: "truncated flagged PCR", mutate: func(packet []byte) { packet[4] = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scanner pcrClockScanner
			scanner.selectPID(0x100)
			if _, observed, reset := scanner.feed(build(tc.mutate)); observed || reset {
				t.Fatalf("malformed PCR observed=%v reset=%v", observed, reset)
			}
		})
	}
}

func TestTSPacketStructureAcceptsBoundedRareAdaptationFields(t *testing.T) {
	pcr := slateTestPCR(27_000)
	opcr := slateTestPCR(54_000)
	adaptation := []byte{0x1f}
	adaptation = append(adaptation, pcr...)
	adaptation = append(adaptation, opcr...)
	adaptation = append(adaptation, 0x7f)             // splice countdown
	adaptation = append(adaptation, 2, 0xaa, 0xbb)    // private data
	adaptation = append(adaptation, 3, 0x01, 0, 0xff) // bounded extension
	packet := testAdaptationPayloadPacket(0x100, false, 0, adaptation, []byte{0xcc})
	if !tsPacketStructureValid(packet) {
		t.Fatal("valid PCR/OPCR/splice/private/extension adaptation was rejected")
	}

	// AFC2 uses the same grammar followed by unconstrained stuffing.
	adaptationOnly := bytes.Repeat([]byte{0xff}, 183)
	copy(adaptationOnly, adaptation)
	packet = bytes.Repeat([]byte{0xff}, tsPacketSize)
	packet[0], packet[1], packet[2], packet[3], packet[4] = 0x47, 0x01, 0x00, 0x20, 183
	copy(packet[5:], adaptationOnly)
	if !tsPacketStructureValid(packet) {
		t.Fatal("valid rare AFC2 adaptation was rejected")
	}
}

func pcrTestProgressiveChunk(start, step time.Duration, discontinuity bool) []byte {
	chunk := make([]byte, 0, tsPacketSize*transportSyncPacketCount)
	for i := 0; i < transportSyncPacketCount; i++ {
		part := pcrTestChunk(durationTicks(start+time.Duration(i)*step, transportClockRate),
			discontinuity && i == 0)
		chunk = append(chunk, part[:tsPacketSize]...)
	}
	return chunk
}

func pcrSparseTestChunks(count int, step time.Duration) [][]byte {
	// Production pacing sees 64 KiB coalesced reads with PCR packets sparse
	// inside one continuous transport stream, and boundaries may split a
	// 188-byte packet. Build that exact shape instead of placing five duplicate
	// PCRs in every small synthetic call.
	totalBytes := count * chunkSize
	transportBytes := ((totalBytes + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
	stream := make([]byte, transportBytes)
	nullPacket := pcrTestNonPCRPacket(0x1ffe, false)
	for offset := 0; offset < len(stream); offset += tsPacketSize {
		copy(stream[offset:offset+tsPacketSize], nullPacket)
	}
	for i := 0; i < count; i++ {
		chunkStart := i * chunkSize
		packetOffset := ((chunkStart + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
		packet := pcrTestChunk(durationTicks(time.Duration(i)*step, transportClockRate), false)
		packet[3] = packet[3]&0xf0 | byte(i&0x0f)
		copy(stream[packetOffset:packetOffset+tsPacketSize], packet[:tsPacketSize])
	}
	// Keep the synthetic stream valid for the transcode input safety gate. The
	// selected PCR/video PID must not precede the PMT that declares it; place a
	// complete single-program PAT/PMT first, then restore the clock-zero PCR.
	if len(stream) >= 3*tsPacketSize {
		copy(stream[:tsPacketSize], testPATPacket(0x1000))
		copy(stream[tsPacketSize:2*tsPacketSize], testVideoOnlyPMTPacket(0x1000, 0x100))
		firstPCR := pcrTestChunk(0, false)
		copy(stream[2*tsPacketSize:3*tsPacketSize], firstPCR[:tsPacketSize])
	}
	chunks := make([][]byte, count)
	for i := range chunks {
		chunks[i] = stream[i*chunkSize : (i+1)*chunkSize]
	}
	return chunks
}

func pcrEveryNTestChunks(count, every int, pcrStep time.Duration) [][]byte {
	if every <= 0 {
		every = 1
	}
	totalBytes := count * chunkSize
	transportBytes := ((totalBytes + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
	stream := make([]byte, transportBytes)
	nullPacket := pcrTestNonPCRPacket(0x1ffe, false)
	for offset := 0; offset < len(stream); offset += tsPacketSize {
		copy(stream[offset:offset+tsPacketSize], nullPacket)
	}
	for i := 0; i < count; i += every {
		chunkStart := i * chunkSize
		packetOffset := ((chunkStart + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
		packet := pcrTestChunk(durationTicks(time.Duration(i/every)*pcrStep, transportClockRate), false)
		copy(stream[packetOffset:packetOffset+tsPacketSize], packet[:tsPacketSize])
	}
	chunks := make([][]byte, count)
	for i := range chunks {
		chunks[i] = stream[i*chunkSize : (i+1)*chunkSize]
	}
	return chunks
}

func pcrPatternTestChunks(count int, clocks map[int]time.Duration) [][]byte {
	totalBytes := count * chunkSize
	transportBytes := ((totalBytes + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
	stream := make([]byte, transportBytes)
	nullPacket := pcrTestNonPCRPacket(0x1ffe, false)
	for offset := 0; offset < len(stream); offset += tsPacketSize {
		copy(stream[offset:offset+tsPacketSize], nullPacket)
	}
	for index, clock := range clocks {
		chunkStart := index * chunkSize
		packetOffset := ((chunkStart + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
		packet := pcrTestChunk(durationTicks(clock, transportClockRate), false)
		copy(stream[packetOffset:packetOffset+tsPacketSize], packet[:tsPacketSize])
	}
	chunks := make([][]byte, count)
	for i := range chunks {
		chunks[i] = stream[i*chunkSize : (i+1)*chunkSize]
	}
	return chunks
}

func pcrTestNonPCRPacket(pid int, discontinuity bool) []byte {
	packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
	packet[0] = 0x47
	packet[1] = byte((pid >> 8) & 0x1f)
	packet[2] = byte(pid)
	packet[3] = 0x30
	packet[4] = 1
	if discontinuity {
		packet[5] = 0x80
	}
	return packet
}
