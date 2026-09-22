package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

func resyncTestProgram() avProgramPIDs {
	program := avProgramPIDs{videoPID: 0x100, pcrPID: 0x100, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = 0x101
	return program
}

// resyncFillerPacket is a payload-only packet: no adaptation field, so it can
// never read as a discontinuity (pcrTestNonPCRPacket's 0xff fill accidentally
// sets that bit when its discontinuity parameter is false).
func resyncFillerPacket(pid int, continuity byte) []byte {
	packet := make([]byte, tsPacketSize)
	for i := range packet {
		packet[i] = 0xff
	}
	packet[0] = 0x47
	packet[1] = byte((pid >> 8) & 0x1f)
	packet[2] = byte(pid)
	packet[3] = 0x10 | continuity&0x0f
	return packet
}

func resyncTestPacer() *transportPCRPacer {
	pacer := testTransportPCRPacer(false)
	pacer.boundaries.configure(resyncTestProgram(), true)
	return &pacer
}

func countDiscontinuityMarkers(data []byte, pid int) int {
	count := 0
	for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] != 0x47 || int(packet[1]&0x1f)<<8|int(packet[2]) != pid {
			continue
		}
		if tsPacketHasDiscontinuity(packet) {
			count++
		}
	}
	return count
}

// A mid-stream sync loss whose clock continues must realign in place: garbage
// dropped, selected PIDs re-marked with decoder-visible discontinuities, and
// the attempt kept — before this fix the first misaligned byte failed the
// whole attempt through both the boundary scanner and the pacer's reset path.
func TestTransportResyncContinuesSameClockAcrossSyncLoss(t *testing.T) {
	beforeResyncs := metrics.RelayTransportResyncs.Value()
	pacer := resyncTestPacer()
	wall := time.Unix(1_700_000_000, 0)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}

	feed := func(data []byte, arrival time.Time) ([]byte, error) {
		media, err := pacer.TransformForFanoutAt(data, arrival)
		if err != nil {
			return nil, err
		}
		if len(media) == 0 {
			return media, nil
		}
		return media, pacer.Wait(context.Background(), media, arrival)
	}

	// Healthy pre-loss stream: establish origin and cadence.
	pre := pcrTestChunk(0, false)
	for i := 0; i < 6; i++ {
		pre = append(pre, resyncFillerPacket(0x100, byte(5+i))...)
	}
	if _, err := feed(pre, wall); err != nil {
		t.Fatalf("pre-loss feed: %v", err)
	}
	wall = wall.Add(150 * time.Millisecond)
	if _, err := feed(pcrTestChunk(durationTicks(150*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatalf("pre-loss pcr step: %v", err)
	}

	// The loss: garbage that also shifts the grid, then the SAME clock two
	// seconds later (matching the wall gap a real loss spans).
	garbage := make([]byte, 5000)
	for i := range garbage {
		garbage[i] = 0xAA
	}
	wall = wall.Add(2 * time.Second)
	post := pcrTestChunk(durationTicks(2150*time.Millisecond, transportClockRate), false)
	for i := 0; i < 6; i++ {
		post = append(post, resyncFillerPacket(0x100, byte(5+i))...)
	}
	media, err := feed(append(garbage, post...), wall)
	if err != nil {
		t.Fatalf("resync feed failed: %v", err)
	}
	if markers := countDiscontinuityMarkers(media, 0x100); markers < 1 {
		t.Fatalf("post-resync output carries %d discontinuity markers on the selected PID, want >=1", markers)
	}
	if delta := metrics.RelayTransportResyncs.Value() - beforeResyncs; delta != 1 {
		t.Fatalf("resync metric delta=%d, want 1", delta)
	}

	// The attempt keeps flowing at normal cadence afterwards.
	wall = wall.Add(150 * time.Millisecond)
	if _, err := feed(pcrTestChunk(durationTicks(2300*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatalf("post-resync cadence: %v", err)
	}
}

// A provider whose clock restarts BEHIND the old one at the loss edge (the
// real capture's shape) is rebased in place: the resync has already declared
// the boundary on every selected PID, so the pacer re-origins instead of
// failing the repaired attempt — and pacing continues at normal cadence on
// the rebased clock. A backward PCR WITHOUT a resync stays an attempt
// boundary (covered by the pacer's existing reset tests).
func TestTransportResyncRebasesBackwardClockAtLossEdge(t *testing.T) {
	pacer := resyncTestPacer()
	wall := time.Unix(1_700_000_000, 0)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	if media, err := pacer.TransformForFanoutAt(pcrTestChunk(durationTicks(10*time.Second, transportClockRate), false), wall); err != nil {
		t.Fatal(err)
	} else if err := pacer.Wait(context.Background(), media, wall); err != nil {
		t.Fatal(err)
	}

	garbage := make([]byte, 3000)
	for i := range garbage {
		garbage[i] = 0x55
	}
	wall = wall.Add(2 * time.Second)
	post := pcrTestChunk(durationTicks(1*time.Second, transportClockRate), false) // restarted behind
	media, err := pacer.TransformForFanoutAt(append(garbage, post...), wall)
	if err != nil {
		t.Fatalf("transform rejected resync unexpectedly: %v", err)
	}
	if err := pacer.Wait(context.Background(), media, wall); err != nil {
		t.Fatalf("rebased epoch failed: %v", err)
	}
	// Pacing continues on the rebased clock at source cadence.
	wall = wall.Add(150 * time.Millisecond)
	next, err := pacer.TransformForFanoutAt(pcrTestChunk(durationTicks(1150*time.Millisecond, transportClockRate), false), wall)
	if err != nil {
		t.Fatal(err)
	}
	if err := pacer.Wait(context.Background(), next, wall); err != nil {
		t.Fatalf("post-rebase cadence: %v", err)
	}
}

// A stream that cannot realign inside the bounded garbage budget is dying,
// not resyncing — the attempt still fails.
func TestTransportResyncBoundsDroppedGarbage(t *testing.T) {
	pacer := resyncTestPacer()
	wall := time.Unix(1_700_000_000, 0)
	if _, err := pacer.TransformForFanoutAt(pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	}
	garbage := make([]byte, maxTransportResyncDropBytes+64*1024)
	for i := range garbage {
		garbage[i] = 0xAA
	}
	var err error
	for off := 0; off < len(garbage); off += chunkSize {
		end := min(off+chunkSize, len(garbage))
		_, err = pacer.TransformForFanoutAt(garbage[off:end], wall)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, errTransportAttemptBoundary) {
		t.Fatalf("oversized garbage err=%v, want %v", err, errTransportAttemptBoundary)
	}
}

// review finding 1: a loss BEFORE any PCR is observed must not leave a
// stale continuation armed — the first marked PCR establishes the origin and
// consumes it, and a later unrelated declared discontinuity with a backward
// clock is still an attempt boundary.
func TestTransportResyncBeforeFirstPCRLeavesNoStaleContinuation(t *testing.T) {
	pacer := resyncTestPacer()
	wall := time.Unix(1_700_000_000, 0)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	feed := func(data []byte) ([]byte, error) {
		media, err := pacer.TransformForFanoutAt(data, wall)
		if err != nil {
			return nil, err
		}
		if len(media) > 0 {
			return media, pacer.Wait(context.Background(), media, wall)
		}
		return media, nil
	}
	// Sync established on payload-only packets; no PCR yet.
	var pre []byte
	for i := 0; i < 8; i++ {
		pre = append(pre, resyncFillerPacket(0x100, byte(i))...)
	}
	if _, err := feed(pre); err != nil {
		t.Fatalf("pre feed: %v", err)
	}
	// Loss before the first PCR, then the first PCR ever.
	garbage := make([]byte, 2000)
	for i := range garbage {
		garbage[i] = 0xAA
	}
	if _, err := feed(append(garbage, pcrTestChunk(durationTicks(5*time.Second, transportClockRate), false)...)); err != nil {
		t.Fatalf("first PCR after pre-origin loss: %v", err)
	}
	if pacer.scanner.resyncContinuations != 0 || pacer.resyncEpochsPending != 0 {
		t.Fatalf("stale resync state: continuations=%d epochsPending=%d",
			pacer.scanner.resyncContinuations, pacer.resyncEpochsPending)
	}
	// A later unrelated declared discontinuity with a backward clock must
	// still fail the attempt — nothing left armed to soften it.
	wall = wall.Add(150 * time.Millisecond)
	media, err := pacer.TransformForFanoutAt(pcrTestChunkForPID(0x100, durationTicks(1*time.Second, transportClockRate), true), wall)
	if err == nil && len(media) > 0 {
		err = pacer.Wait(context.Background(), media, wall)
	}
	if err == nil {
		t.Fatal("declared backward discontinuity after resolved resync was accepted")
	}
}

// review finding 2: two realignments completing in one transform call
// must hand the PCR scanner two continuations — the second marked PCR is an
// edge, not an ordinary declared reset.
func TestTransportResyncCountsDoubleRealignmentInOneCall(t *testing.T) {
	pacer := resyncTestPacer()
	wall := time.Unix(1_700_000_000, 0)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	if media, err := pacer.TransformForFanoutAt(pcrTestChunk(0, false), wall); err != nil {
		t.Fatal(err)
	} else if err := pacer.Wait(context.Background(), media, wall); err != nil {
		t.Fatal(err)
	}
	g1 := make([]byte, 1500)
	g2 := make([]byte, 1500)
	for i := range g1 {
		g1[i] = 0xAA
		g2[i] = 0x55
	}
	wall = wall.Add(2 * time.Second)
	double := append(append([]byte{}, g1...), pcrTestChunk(durationTicks(2*time.Second, transportClockRate), false)...)
	double = append(double, g2...)
	double = append(double, pcrTestChunk(durationTicks(2150*time.Millisecond, transportClockRate), false)...)
	media, err := pacer.TransformForFanoutAt(double, wall)
	if err != nil {
		t.Fatalf("double realignment transform: %v", err)
	}
	if err := pacer.Wait(context.Background(), media, wall); err != nil {
		t.Fatalf("double realignment pacing: %v", err)
	}
	if pacer.scanner.resyncContinuations != 0 || pacer.resyncEpochsPending != 0 {
		t.Fatalf("unconsumed resync state: continuations=%d epochsPending=%d",
			pacer.scanner.resyncContinuations, pacer.resyncEpochsPending)
	}
	// Normal cadence continues on the final epoch.
	wall = wall.Add(150 * time.Millisecond)
	next, err := pacer.TransformForFanoutAt(pcrTestChunk(durationTicks(2300*time.Millisecond, transportClockRate), false), wall)
	if err != nil {
		t.Fatal(err)
	}
	if err := pacer.Wait(context.Background(), next, wall); err != nil {
		t.Fatalf("post-double cadence: %v", err)
	}
}

// The soak-measured residual band: a pause inside the 20s stall
// tolerance but beyond the ~14.6s recoverable-debt ceiling used to survive the
// watchdogs and then fail the attempt on its own catch-up burst. Skip-ahead
// discards the stale backlog and rejoins the live edge with declared
// discontinuities; the viewer keeps the session.
func TestTransportLongPauseKeepsBacklogInsteadOfSkipping(t *testing.T) {
	beforeReanchors := metrics.RelayUnderflowReanchors.Value()
	beforeEpisodes := metrics.RelaySkipAheadEpisodes.Value()
	pacer := resyncTestPacer()
	wall := time.Unix(1_700_000_000, 0)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	feed := func(data []byte, arrival time.Time) error {
		media, err := pacer.TransformForFanoutAt(data, arrival)
		if err != nil {
			return err
		}
		if len(media) == 0 {
			return nil
		}
		return pacer.Wait(context.Background(), media, arrival)
	}
	// Steady cadence.
	for i := 0; i < 5; i++ {
		if err := feed(pcrTestChunk(durationTicks(time.Duration(i)*150*time.Millisecond, transportClockRate), false), wall); err != nil {
			t.Fatalf("steady %d: %v", i, err)
		}
		wall = wall.Add(150 * time.Millisecond)
	}
	// 17s pause (inside the 20s stall tolerance, beyond the old 14.6s debt
	// cap). Until 2026-09-02 the catch-up chunk was discarded to rejoin the
	// live edge; the backlog is the margin the viewer wants, so the schedule
	// re-anchors on it and every backlog chunk is emitted at media cadence.
	wall = wall.Add(17 * time.Second)
	if err := feed(pcrTestChunk(durationTicks(750*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatalf("catch-up chunk after the pause: %v", err)
	}
	if pacer.skipEpisode {
		t.Fatal("skip episode armed after a long pause")
	}
	lastEmission := wall
	for i := 1; i <= 4; i++ {
		clock := durationTicks(time.Duration(750+i*150)*time.Millisecond, transportClockRate)
		if err := feed(pcrTestChunk(clock, false), wall); err != nil {
			t.Fatalf("backlog chunk %d: %v", i, err)
		}
		if interval := wall.Sub(lastEmission); interval != 150*time.Millisecond {
			t.Fatalf("backlog chunk %d interval=%s, want 150ms media cadence", i, interval)
		}
		lastEmission = wall
	}
	if delta := metrics.RelayUnderflowReanchors.Value() - beforeReanchors; delta != 1 {
		t.Fatalf("underflow re-anchor metric delta=%d, want 1", delta)
	}
	if delta := metrics.RelaySkipAheadEpisodes.Value() - beforeEpisodes; delta != 0 {
		t.Fatalf("skip-ahead episode metric delta=%d, want 0", delta)
	}
}

// review finding (high): a catch-up backlog can span multiple reads, so
// re-entry must bind to genuinely fresh media, not the next stale read. The
// read loops gate on socket-arrival freshness and hand stale batches to
// ConsumeStaleBatch, which keeps PSI and clock continuity without consuming
// the armed edge; the edge then re-origins on the first FRESH batch.
func TestTransportSkipAheadConsumesMultiBatchBacklogBeforeRejoining(t *testing.T) {
	// A skip episode now opens only from the pacing cap (see the pacer tests);
	// arm it directly here to exercise the backlog-consumption contract.
	pacer := resyncTestPacer()
	wall := time.Unix(1_700_000_000, 0)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	feed := func(data []byte, arrival time.Time) error {
		media, err := pacer.TransformForFanoutAt(data, arrival)
		if err != nil {
			return err
		}
		if len(media) == 0 {
			return nil
		}
		return pacer.Wait(context.Background(), media, arrival)
	}
	for i := 0; i < 5; i++ {
		if err := feed(pcrTestChunk(durationTicks(time.Duration(i)*150*time.Millisecond, transportClockRate), false), wall); err != nil {
			t.Fatalf("steady %d: %v", i, err)
		}
		wall = wall.Add(150 * time.Millisecond)
	}
	wall = wall.Add(17 * time.Second)
	pacer.beginSkipEpisode()
	if !pacer.skipEpisode {
		t.Fatal("skip episode not armed")
	}
	beforeChunks := metrics.RelaySkipAheadChunks.Value()
	// The backlog continues across further reads: consumed, never emitted, and
	// the armed edge survives them (PSI + clock stay coherent).
	for i := 1; i <= 3; i++ {
		pacer.ConsumeStaleBatch(pcrTestChunk(durationTicks(time.Duration(750+i*150)*time.Millisecond, transportClockRate), false))
	}
	if pacer.scanner.resyncContinuations != 1 || pacer.resyncEpochsPending != 1 {
		t.Fatalf("armed edge consumed by stale backlog: continuations=%d epochsPending=%d",
			pacer.scanner.resyncContinuations, pacer.resyncEpochsPending)
	}
	if delta := metrics.RelaySkipAheadChunks.Value() - beforeChunks; delta != 3 {
		t.Fatalf("discarded chunk metric delta=%d, want 3", delta)
	}
	// The fresh live-edge batch re-marks, re-origins, and emits.
	wall = wall.Add(150 * time.Millisecond)
	if err := feed(pcrTestChunk(durationTicks(17900*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatalf("live-edge rejoin: %v", err)
	}
	if pacer.skipEpisode || pacer.resyncEpochsPending != 0 {
		t.Fatalf("episode not closed at the fresh edge: skip=%v pending=%d", pacer.skipEpisode, pacer.resyncEpochsPending)
	}
	wall = wall.Add(150 * time.Millisecond)
	if err := feed(pcrTestChunk(durationTicks(18050*time.Millisecond, transportClockRate), false), wall); err != nil {
		t.Fatalf("post-rejoin cadence: %v", err)
	}
}
