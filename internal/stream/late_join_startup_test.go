package stream

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingLateJoinValidator struct {
	calls atomic.Int32
}

func (v *countingLateJoinValidator) Validate(context.Context, []byte) (prefixDecision, error) {
	v.calls.Add(1)
	return prefixDecision{kind: prefixNeedMore, reason: "unexpected_probe"}, nil
}

type blockedLateJoinValidator struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (v *blockedLateJoinValidator) Validate(ctx context.Context, _ []byte) (prefixDecision, error) {
	v.once.Do(func() { close(v.started) })
	select {
	case <-ctx.Done():
		return prefixDecision{}, ctx.Err()
	case <-v.release:
		return prefixDecision{kind: prefixReady, reason: "test_ready"}, nil
	}
}

type needMoreLateJoinValidator struct{}

func (needMoreLateJoinValidator) Validate(context.Context, []byte) (prefixDecision, error) {
	return prefixDecision{kind: prefixNeedMore, reason: "configured_random_access_missing"}, nil
}

type finalizeReadyLateJoinValidator struct {
	calls atomic.Int32
}

func (v *finalizeReadyLateJoinValidator) Validate(context.Context, []byte) (prefixDecision, error) {
	if v.calls.Add(1) == 1 {
		return prefixDecision{kind: prefixNeedMore, reason: "awaiting_continuation"}, nil
	}
	return prefixDecision{kind: prefixReady, reason: "finalized_after_close"}, nil
}

type readyLateJoinValidator struct {
	calls atomic.Int32
}

func (v *readyLateJoinValidator) Validate(context.Context, []byte) (prefixDecision, error) {
	v.calls.Add(1)
	return prefixDecision{kind: prefixReady, reason: "immediate_ready"}, nil
}

func TestLateSubscriberMarkedPumpPrefixSkipsSecondProbe(t *testing.T) {
	pump := &Streamer{}
	validator := &countingLateJoinValidator{}
	first := startupChunk{
		data:           []byte("already-validated"),
		epoch:          pump.MediaEpoch(),
		validatedStart: true,
	}

	got, err := validateLateSubscriberStart(
		context.Background(), pump, first, make(chan startupChunk),
		make(chan struct{}), validator)
	if err != nil {
		t.Fatal(err)
	}
	if !got.validatedStart || !bytes.Equal(got.data, first.data) {
		t.Fatalf("marked startup changed: %+v", got)
	}
	if calls := validator.calls.Load(); calls != 0 {
		t.Fatalf("marked pump prefix was re-probed %d times", calls)
	}
}

func TestLateSubscriberEpochChangeKeepsCandidatePrivate(t *testing.T) {
	pump := &Streamer{}
	validator := &blockedLateJoinValidator{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	first := startupChunk{
		data:  bytes.Repeat([]byte{0xa5}, minPrefixDecisionBytes),
		epoch: pump.MediaEpoch(),
	}
	type result struct {
		chunk startupChunk
		err   error
	}
	done := make(chan result, 1)
	subscriberTerminal := make(chan struct{})
	go func() {
		chunk, err := validateLateSubscriberStart(
			context.Background(), pump, first, make(chan startupChunk),
			subscriberTerminal, validator)
		done <- result{chunk: chunk, err: err}
	}()

	select {
	case <-validator.started:
	case <-time.After(time.Second):
		t.Fatal("late-join validator did not start")
	}
	pump.mediaEpoch.Add(1)
	close(validator.release)
	select {
	case got := <-done:
		if !errors.Is(got.err, ErrUpstreamNotReady) ||
			!errors.Is(got.err, errPumpDiscontinuityBeforeMedia) {
			t.Fatalf("epoch-change error=%v, want retryable pre-media discontinuity", got.err)
		}
		if len(got.chunk.data) != 0 {
			t.Fatalf("epoch-change published %d candidate bytes", len(got.chunk.data))
		}
	case <-time.After(time.Second):
		t.Fatal("epoch-change validation did not return")
	}
}

func TestLateSubscriberDeadlineRejectsUnconfiguredTailBeforePublication(t *testing.T) {
	pump := &Streamer{}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	first := startupChunk{
		data:  lateJoinTSPackets(0x101, 350, 0xa5),
		epoch: pump.MediaEpoch(),
	}

	got, err := validateLateSubscriberStart(
		ctx, pump, first, make(chan startupChunk), make(chan struct{}),
		needMoreLateJoinValidator{})
	if !errors.Is(err, ErrUpstreamNotReady) {
		t.Fatalf("deadline error=%v, want ErrUpstreamNotReady", err)
	}
	if len(got.data) != 0 {
		t.Fatalf("deadline published %d candidate bytes", len(got.data))
	}
}

func TestLateSubscriberClosedContinuationRejectsFinalizableCandidate(t *testing.T) {
	pump := &Streamer{}
	validator := &finalizeReadyLateJoinValidator{}
	chunks := make(chan startupChunk)
	close(chunks)
	first := startupChunk{
		data:  bytes.Repeat([]byte{0xa5}, minPrefixDecisionBytes),
		epoch: pump.MediaEpoch(),
	}

	got, err := validateLateSubscriberStart(
		context.Background(), pump, first, chunks, make(chan struct{}), validator)
	if !errors.Is(err, ErrUpstreamNotReady) ||
		!errors.Is(err, errPumpStoppedBeforeMedia) {
		t.Fatalf("closed continuation error=%v, want retryable pre-media close", err)
	}
	if len(got.data) != 0 {
		t.Fatalf("closed continuation published %d buffered bytes", len(got.data))
	}
	if calls := validator.calls.Load(); calls != 1 {
		t.Fatalf("closed continuation re-probed buffered media %d times, want 1", calls)
	}
}

func TestLateSubscriberPreclosedTerminalRejectsImmediatelyReadyCandidate(t *testing.T) {
	pump := &Streamer{}
	validator := &readyLateJoinValidator{}
	subscriberTerminal := make(chan struct{})
	close(subscriberTerminal)
	first := startupChunk{
		data:  bytes.Repeat([]byte{0xa5}, minPrefixDecisionBytes),
		epoch: pump.MediaEpoch(),
	}

	got, err := validateLateSubscriberStart(
		context.Background(), pump, first, make(chan startupChunk),
		subscriberTerminal, validator)
	if !errors.Is(err, ErrUpstreamNotReady) ||
		!errors.Is(err, errPumpStoppedBeforeMedia) {
		t.Fatalf("preclosed terminal error=%v, want retryable pre-media close", err)
	}
	if len(got.data) != 0 {
		t.Fatalf("preclosed terminal published %d buffered bytes", len(got.data))
	}
	if calls := validator.calls.Load(); calls != 0 {
		t.Fatalf("preclosed terminal invoked validator %d times, want 0", calls)
	}
}

func TestLateSubscriberTerminalDuringReadyProbeKeepsCandidatePrivate(t *testing.T) {
	pump := &Streamer{}
	validator := &blockedLateJoinValidator{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var subscribers subscriberSet
	subscribers.init("terminal-during-probe", nil)
	_, _, subscriberTerminal, unsubscribe := subscribers.SubscribeForStartup("viewer")
	defer subscribers.closeAll()
	type result struct {
		chunk startupChunk
		err   error
	}
	done := make(chan result, 1)
	go func() {
		chunk, err := validateLateSubscriberStart(
			context.Background(), pump, startupChunk{
				data:  bytes.Repeat([]byte{0xa5}, minPrefixDecisionBytes),
				epoch: pump.MediaEpoch(),
			}, make(chan startupChunk), subscriberTerminal, validator)
		done <- result{chunk: chunk, err: err}
	}()

	select {
	case <-validator.started:
	case <-time.After(time.Second):
		t.Fatal("late-join validator did not start")
	}
	unsubscribe()
	close(validator.release)
	select {
	case got := <-done:
		if !errors.Is(got.err, ErrUpstreamNotReady) ||
			!errors.Is(got.err, errPumpStoppedBeforeMedia) {
			t.Fatalf("terminal-during-probe error=%v, want retryable pre-media close", got.err)
		}
		if len(got.chunk.data) != 0 {
			t.Fatalf("terminal-during-probe published %d bytes", len(got.chunk.data))
		}
	case <-time.After(time.Second):
		t.Fatal("terminal-during-probe validation did not return")
	}
}

func TestLateSubscriberValidatedCandidateIsMarkedAndContinuesExactlyOnce(t *testing.T) {
	const pid = 0x101
	unsafe := lateJoinTSPackets(pid, 300, 0x11)
	selected := lateJoinTSPackets(pid, 100, 0x22)
	continued := lateJoinTSPackets(pid, 40, 0x33)
	selectedSentinel := []byte{0x22, 0x22, 0x22, 0x22, 0x22, 0x22}
	continuedSentinel := []byte{0x33, 0x33, 0x33, 0x33, 0x33, 0x33}
	validator := &thresholdPrefixValidator{
		readyAt: len(unsafe) + len(selected),
		start:   len(unsafe),
	}
	pump := &Streamer{}
	chunks := make(chan startupChunk, 2)
	selectedAt := time.Now()
	chunks <- startupChunk{data: selected, epoch: pump.MediaEpoch(), realAt: selectedAt}
	chunks <- startupChunk{data: continued, epoch: pump.MediaEpoch(), realAt: selectedAt.Add(time.Second)}

	got, err := validateLateSubscriberStart(context.Background(), pump, startupChunk{
		data: unsafe, epoch: pump.MediaEpoch(), realAt: selectedAt.Add(-time.Second),
	}, chunks, make(chan struct{}), validator)
	if err != nil {
		t.Fatal(err)
	}
	if !got.validatedStart {
		t.Fatal("derived late-join prefix was not marked as validated")
	}
	if !got.realAt.Equal(selectedAt) {
		t.Fatalf("derived realAt=%s, want latest consumed real media %s", got.realAt, selectedAt)
	}
	if bytes.Count(got.data, selectedSentinel) == 0 {
		t.Fatal("validated selected media was omitted")
	}
	if bytes.Count(got.data, continuedSentinel) != 0 {
		t.Fatal("unconsumed continuation was duplicated into the startup prefix")
	}
	if !lateJoinPIDStartsWithDiscontinuity(got.data, pid) {
		t.Fatal("derived late-join prefix lacks an initial TS discontinuity")
	}
	select {
	case next := <-chunks:
		if !bytes.Equal(next.data, continued) {
			t.Fatal("ordinary continuation changed after private validation")
		}
	default:
		t.Fatal("private validation consumed the post-readiness continuation")
	}
}

func TestSubscriberRingMarkerExpiresWithValidatedPrefix(t *testing.T) {
	var retained subscriberSet
	retained.init("retained-marker", nil)
	retained.fanoutValidatedStart([]byte("validated"))
	firstReal, chunks, _, unsubscribe := retained.SubscribeForStartup("retained")
	retained.fanout([]byte("next"))
	first := <-firstReal
	if !first.validatedStart || string(first.data) != "validated" {
		t.Fatalf("retained ring start=%+v, want validated marker", first)
	}
	if next := <-chunks; string(next.data) != "next" {
		t.Fatalf("retained continuation=%q, want next", next.data)
	}
	unsubscribe()
	retained.closeAll()

	var evicted subscriberSet
	evicted.init("evicted-marker", nil)
	evicted.ringCap = 4
	evicted.fanoutValidatedStart([]byte("v"))
	evicted.fanout([]byte("tail"))
	firstReal, chunks, _, unsubscribe = evicted.SubscribeForStartup("evicted")
	evicted.fanout([]byte("n"))
	first = <-firstReal
	if first.validatedStart || string(first.data) != "tail" {
		t.Fatalf("evicted ring start=%+v, want unmarked tail", first)
	}
	if next := <-chunks; string(next.data) != "n" {
		t.Fatalf("evicted continuation=%q, want n", next.data)
	}
	unsubscribe()
	evicted.closeAll()
}

func TestPumpTypesMarkValidatedAttemptStart(t *testing.T) {
	t.Run("passthrough", func(t *testing.T) {
		srv := delayedPrefixLiveStreamServer(t, 0)
		defer srv.Close()
		s := NewStreamer("validated-start-passthrough", srv.URL, testLogger(), nil)
		s.prefixValidator = &thresholdPrefixValidator{
			readyAt:       delayedLivePrefixChunkCount * chunkSize,
			mediaDuration: 1600 * time.Millisecond,
			pcrPID:        0x100,
			havePCRPID:    true,
		}
		s.sourceStartupTimeout = 5 * time.Second
		firstReal, _, _, unsubscribe := s.SubscribeForStartup("validated-start-client")
		defer unsubscribe()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := s.runOnce(ctx)
			done <- err
		}()
		select {
		case first := <-firstReal:
			if !first.validatedStart || len(first.data) == 0 {
				t.Fatalf("passthrough first chunk=%+v, want validated attempt marker", first)
			}
		case <-time.After(6 * time.Second):
			t.Fatal("passthrough did not publish validated attempt start")
		}
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("passthrough shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("passthrough did not stop after marker assertion")
		}
	})

	t.Run("transcode", func(t *testing.T) {
		srv, _ := delayedTranscodePrefixLiveStreamServer(t)
		defer srv.Close()
		s := NewTranscodeStreamer("validated-start-transcode", srv.URL, stubProfile(),
			stubBinary(t, "exec cat"), testLogger(), nil)
		s.prefixValidator = &thresholdPrefixValidator{
			readyAt:       delayedLivePrefixChunkCount * chunkSize,
			mediaDuration: 1600 * time.Millisecond,
			pcrPID:        0x100,
			havePCRPID:    true,
		}
		s.sourceStartupTimeout = 5 * time.Second
		firstReal, _, _, unsubscribe := s.SubscribeForStartup("validated-start-client")
		defer unsubscribe()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, _, err := s.runOnce(ctx)
			done <- err
		}()
		select {
		case first := <-firstReal:
			if !first.validatedStart || len(first.data) == 0 {
				t.Fatalf("transcode first chunk=%+v, want validated attempt marker", first)
			}
		case <-time.After(6 * time.Second):
			t.Fatal("transcode did not publish validated attempt start")
		}
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("transcode shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("transcode did not stop after marker assertion")
		}
	})
}

func TestLateSubscriberFailsClosedWhenTSProbeUnavailable(t *testing.T) {
	pump := &Streamer{}
	first := startupChunk{
		data:  lateJoinTSPackets(0x101, 350, 0x44),
		epoch: pump.MediaEpoch(),
	}
	got, err := validateLateSubscriberStart(context.Background(), pump, first,
		make(chan startupChunk), make(chan struct{}), &ffprobeMediaPrefixValidator{})
	if !errors.Is(err, ErrUpstreamNotReady) || !errors.Is(err, errUncertainMediaPrefix) {
		t.Fatalf("unavailable TS probe error=%v, want fail-closed local uncertainty", err)
	}
	if len(got.data) != 0 {
		t.Fatalf("unavailable TS probe published %d bytes", len(got.data))
	}
}

func TestLateSubscriberLongGOPRingTailWaitsForFreshRandomAccess(t *testing.T) {
	ffmpeg, ffprobe := requireLateJoinFFmpeg(t)
	media := generateLateJoinLongGOPTS(t, ffmpeg)
	validator := &ffprobeMediaPrefixValidator{ffprobeBinary: ffprobe, available: true}
	ringStart, ring, program := findLateJoinUndecodableRing(t, media)
	if len(ring) != ringCapBytes {
		t.Fatalf("ring bytes=%d, want exact production cap %d", len(ring), ringCapBytes)
	}
	alignedAt, ok := findTSSyncOffset(ring)
	if !ok {
		t.Fatal("exact ring tail lost MPEG-TS sync")
	}
	patPackets, pmtPackets := lateJoinPSIPacketCounts(ring[alignedAt:], program.mapIdentity.pmtPID)
	if patPackets < 2 || pmtPackets < 2 {
		t.Fatalf("ring PAT/PMT packets=%d/%d, want repeated guide-rich transport metadata",
			patPackets, pmtPackets)
	}
	decision, err := validator.Validate(context.Background(), ring)
	if err != nil {
		t.Fatal(err)
	}
	if decision.kind == prefixReady {
		t.Fatalf("raw production-sized ring unexpectedly decodable: %s", decision.reason)
	}
	if _, randomAccessAt, _ := findConfiguredRandomAccessNALProgram(
		ring[alignedAt:], "h264", program.videoPID); randomAccessAt >= 0 {
		t.Fatalf("raw ring contains configured random access at %d", randomAccessAt)
	}

	pump := &Streamer{}
	future := media[ringStart+len(ring):]
	chunks := make(chan startupChunk, (len(future)+chunkSize-1)/chunkSize)
	realAt := time.Now()
	for offset := 0; offset < len(future); offset += chunkSize {
		end := min(offset+chunkSize, len(future))
		realAt = realAt.Add(time.Millisecond)
		chunks <- startupChunk{
			data: future[offset:end], epoch: pump.MediaEpoch(), realAt: realAt,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), ClientStartupBudget)
	defer cancel()
	got, err := validateLateSubscriberStart(ctx, pump, startupChunk{
		data: ring, epoch: pump.MediaEpoch(), realAt: time.Now(),
	}, chunks, make(chan struct{}), validator)
	if err != nil {
		t.Fatal(err)
	}
	if !got.validatedStart {
		t.Fatal("fresh long-GOP random access was not marked validated")
	}
	released, err := validator.Validate(ctx, got.data)
	if err != nil {
		t.Fatal(err)
	}
	if released.kind != prefixReady {
		t.Fatalf("released late-join candidate=%v (%s), want independently decodable",
			released.kind, released.reason)
	}
}

func lateJoinTSPackets(pid, count int, fill byte) []byte {
	media := make([]byte, count*tsPacketSize)
	for i := 0; i < count; i++ {
		packet := media[i*tsPacketSize : (i+1)*tsPacketSize]
		for j := range packet {
			packet[j] = fill
		}
		packet[0] = 0x47
		packet[1] = byte(pid >> 8)
		packet[2] = byte(pid)
		packet[3] = 0x10 | byte(i&0x0f)
	}
	return media
}

func lateJoinPIDStartsWithDiscontinuity(data []byte, wantedPID int) bool {
	syncAt, ok := findTSSyncOffset(data)
	if !ok {
		return false
	}
	for pos := syncAt; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid == wantedPID {
			return tsPacketHasDiscontinuity(packet)
		}
	}
	return false
}

func requireLateJoinFFmpeg(t *testing.T) (string, string) {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffmpeg missing from supported CI image")
		}
		t.Skip("ffmpeg not on PATH")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffprobe missing from supported CI image")
		}
		t.Skip("ffprobe not on PATH")
	}
	return ffmpeg, ffprobe
}

func generateLateJoinLongGOPTS(t *testing.T, ffmpeg string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30:duration=14",
		"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=14",
		"-t", "14", "-shortest",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-g", "180", "-keyint_min", "180", "-sc_threshold", "0",
		"-b:v", "4M", "-maxrate", "4M", "-bufsize", "8M",
		"-c:a", "aac", "-b:a", "96k", "-muxrate", "8M",
		"-mpegts_flags", "+resend_headers", "-f", "mpegts", "pipe:1")
	media, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate long-GOP MPEG-TS: %v", err)
	}
	if len(media) < 4*ringCapBytes {
		t.Fatalf("long-GOP fixture=%d bytes, want at least %d", len(media), 4*ringCapBytes)
	}
	return media
}

func findLateJoinUndecodableRing(t *testing.T, media []byte) (int, []byte, tsVideoProgram) {
	t.Helper()
	for start := chunkSize; start+ringCapBytes+2*chunkSize < len(media); start += chunkSize {
		ring := media[start : start+ringCapBytes]
		syncAt, ok := findTSSyncOffset(ring)
		if !ok {
			continue
		}
		programs := scanTSProgramMaps(ring[syncAt:], "h264")
		if len(programs) == 0 {
			continue
		}
		for _, program := range programs {
			_, randomAccessAt, selectedPID := findConfiguredRandomAccessNALProgram(
				ring[syncAt:], "h264", program.videoPID)
			if randomAccessAt < 0 && selectedPID < 0 {
				return start, ring, program
			}
		}
	}
	t.Fatal("generated fixture did not contain a production-sized PAT/PMT-rich long-GOP tail")
	return 0, nil, tsVideoProgram{}
}

func lateJoinPSIPacketCounts(data []byte, pmtPID int) (pat, pmt int) {
	for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] != 0x47 {
			break
		}
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		switch pid {
		case 0:
			pat++
		case pmtPID:
			pmt++
		}
	}
	return pat, pmt
}
