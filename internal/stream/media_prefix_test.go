package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestFFprobeMediaPrefixRequiresDecodableRandomAccessWindow(t *testing.T) {
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

	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30:duration=4",
		"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=4",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-g", "30", "-keyint_min", "30", "-sc_threshold", "0",
		"-b:v", "2M", "-maxrate", "2M", "-bufsize", "4M",
		"-c:a", "aac", "-f", "mpegts", "pipe:1")
	media, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate prefix fixture: %v", err)
	}
	if len(media) < minTSProbeBytes*2 {
		t.Fatalf("fixture too small for bounded prefix test: %d bytes", len(media))
	}

	validator := &ffprobeMediaPrefixValidator{ffprobeBinary: ffprobe, available: true}
	ctx, cancel := context.WithTimeout(context.Background(), defaultSourceStartupTimeout)
	defer cancel()

	garbagePrefix := bytes.Repeat([]byte{0xa5}, 37)
	withGarbage := append(garbagePrefix, media...)
	decision, err := validator.Validate(ctx, withGarbage)
	if err != nil {
		t.Fatal(err)
	}
	if decision.kind != prefixReady {
		t.Fatalf("full prefix decision=%v reason=%s, want ready", decision.kind, decision.reason)
	}
	if decision.mediaDuration < minUsefulMediaDuration || decision.mediaDuration > 4*time.Second {
		t.Fatalf("validated media duration=%s, want useful bounded fixture span", decision.mediaDuration)
	}
	if decision.start < len(garbagePrefix) || decision.start >= len(withGarbage) {
		t.Fatalf("safe start=%d outside media window [%d,%d)",
			decision.start, len(garbagePrefix), len(withGarbage))
	}
	released := append(append([]byte(nil), decision.prepend...), withGarbage[decision.start:]...)
	packet := released[:tsPacketSize]
	if packet[0] != 0x47 || int(packet[1]&0x1f)<<8|int(packet[2]) != 0 {
		t.Fatalf("validated prefix does not begin on PAT: %x", packet[:4])
	}
	if len(decision.prepend) != 0 {
		t.Fatalf("ordinary current PMT unexpectedly rewrote %d PSI bytes", len(decision.prepend))
	}
	gate := newStartupMediaGate(validator)
	gated, ready, err := gate.PushFinal(ctx, withGarbage)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("validated MPEG-TS did not pass the startup gate")
	}
	if emitted, emittedErr := validator.Validate(ctx, gated); emittedErr != nil || emitted.kind != prefixReady {
		t.Fatalf("rewritten coherent prefix was not independently decodable: decision=%v err=%v",
			emitted.kind, emittedErr)
	}

	multiPacketPMT, replacements := expandPMTSectionsForTest(t, media)
	if replacements == 0 {
		t.Fatal("fixture did not contain a replaceable PMT")
	}
	if _, ok := videoPIDFromTS(multiPacketPMT); !ok {
		t.Fatal("video PID was not recovered from a multi-packet PMT")
	}
	decision, err = validator.Validate(ctx, multiPacketPMT)
	if err != nil {
		t.Fatal(err)
	}
	if decision.kind != prefixReady {
		t.Fatalf("multi-packet PMT decision=%v reason=%s, want ready", decision.kind, decision.reason)
	}

	decision, err = validator.Validate(ctx, media[:minTSProbeBytes-1])
	if err != nil {
		t.Fatal(err)
	}
	if decision.kind != prefixNeedMore {
		t.Fatalf("short prefix decision=%v reason=%s, want more bytes", decision.kind, decision.reason)
	}

	withoutPAT := append([]byte(nil), media...)
	for pos := 0; pos+tsPacketSize <= len(withoutPAT); pos += tsPacketSize {
		packet := withoutPAT[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid == 0 {
			packet[1] = (packet[1] & 0xe0) | 0x1f
			packet[2] = 0xff
		}
	}
	decision, err = validator.Validate(ctx, withoutPAT)
	if err != nil {
		t.Fatal(err)
	}
	if decision.kind == prefixReady {
		t.Fatal("MPEG-TS with every PAT removed was accepted as a safe startup prefix")
	}
}

func TestLowBitrateMPEGTSBecomesReadyInsidePlexDeadline(t *testing.T) {
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

	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=4",
		"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=4",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-g", "25", "-keyint_min", "25", "-sc_threshold", "0",
		"-b:v", "250k", "-maxrate", "250k", "-bufsize", "500k",
		"-c:a", "aac", "-b:a", "64k", "-muxrate", "384k",
		"-mpegts_flags", "+resend_headers", "-f", "mpegts", "pipe:1")
	media, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate low-bitrate MPEG-TS: %v", err)
	}
	if len(media) < minTSProbeBytes {
		t.Fatalf("low-bitrate fixture=%d bytes, want at least %d", len(media), minTSProbeBytes)
	}
	var timeline transportAVTimeline
	for offset := 0; offset < len(media); {
		end := min(offset+137, len(media))
		timeline.feed(media[offset:end])
		offset = end
	}
	if snapshot := timeline.snapshot(); !snapshot.valid ||
		snapshot.videoSamples < 2 || snapshot.audioSamples < 2 {
		t.Fatalf("production-layout TS PTS diagnostics unavailable: %+v", snapshot)
	}

	gate := newStartupMediaGate(&ffprobeMediaPrefixValidator{
		ffprobeBinary: ffprobe, available: true,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	started := time.Now()
	for offset := 0; offset < len(media); {
		end := min(offset+4096, len(media))
		_, ready, gateErr := gate.Push(ctx, media[offset:end])
		if gateErr != nil {
			t.Fatal(gateErr)
		}
		if ready {
			if elapsed := time.Since(started); elapsed >= 4*time.Second {
				t.Fatalf("low-bitrate media readiness took %s, want <4s", elapsed)
			}
			return
		}
		offset = end
		timer := time.NewTimer(85 * time.Millisecond) // approximately 384 kbit/s
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal("low-bitrate valid MPEG-TS missed the per-source startup budget")
		case <-timer.C:
		}
	}
	t.Fatal("complete low-bitrate valid MPEG-TS never became ready")
}

type thresholdPrefixValidator struct {
	readyAt       int
	start         int
	prepend       []byte
	mediaDuration time.Duration
	videoPID      int
	audioPIDs     [maxAVTimelineAudioPIDs]int
	audioCount    int
	pcrPID        int
	havePCRPID    bool
	haveProgram   bool
	mapIdentity   selectedProgramMapIdentity
	delay         time.Duration
	calls         int
}

func (v *thresholdPrefixValidator) Validate(ctx context.Context, data []byte) (prefixDecision, error) {
	v.calls++
	if len(data) < v.readyAt {
		return prefixDecision{kind: prefixNeedMore, reason: "test_buffering"}, nil
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
	return prefixDecision{
		kind: prefixReady, start: v.start, prepend: v.prepend,
		reason: "test_ready", mediaDuration: v.mediaDuration,
		videoPID: v.videoPID, audioPIDs: v.audioPIDs, audioCount: v.audioCount,
		pcrPID: v.pcrPID, havePCRPID: v.havePCRPID, haveProgram: v.haveProgram,
		mapIdentity: v.mapIdentity.clone(),
	}, nil
}

func TestStartupMediaGatePrependsValidatedPSIAndDropsEarlierBytes(t *testing.T) {
	validator := &thresholdPrefixValidator{
		readyAt: 1,
		start:   len("stale:"),
		prepend: []byte("PAT+PMT:"),
	}
	gate := newStartupMediaGate(validator)
	media, ready, err := gate.PushFinal(context.Background(), []byte("stale:fresh-media"))
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("validated prefix did not make the gate ready")
	}
	if got, want := string(media), "PAT+PMT:fresh-media"; got != want {
		t.Fatalf("released media=%q, want %q", got, want)
	}
}

func TestStartupMediaGateRejectsUntrackedNinthAudioPIDBeforePublication(t *testing.T) {
	validator := &thresholdPrefixValidator{
		readyAt: 1, haveProgram: true, videoPID: 0x100, pcrPID: 0x100,
		audioCount: maxAVTimelineAudioPIDs + 1,
	}
	gate := newStartupMediaGate(validator)
	media, ready, err := gate.PushFinal(context.Background(), []byte("validated-candidate"))
	if err == nil || ready || len(media) != 0 || !errors.Is(err, errMediaPrefixNotReady) {
		t.Fatalf("nine-track gate=(bytes=%d ready=%v err=%v), want unpublished safety rejection",
			len(media), ready, err)
	}
}

func TestStartupMediaGateMarksInitialTSDiscontinuity(t *testing.T) {
	const packets = 8
	media := make([]byte, packets*tsPacketSize)
	for i := 0; i < packets; i++ {
		packet := media[i*tsPacketSize : (i+1)*tsPacketSize]
		for j := range packet {
			packet[j] = 0xff
		}
		packet[0] = 0x47
		pid := 0x100 + i%2
		packet[1] = byte(pid >> 8)
		packet[2] = byte(pid)
		if i < 2 {
			packet[3] = 0x10 | byte(i&0x0f) // payload only: cannot carry the flag
			continue
		}
		packet[3] = 0x30 | byte(i&0x0f)
		packet[4] = 1
		packet[5] = 0
	}

	original := append([]byte(nil), media...)
	gate := newStartupMediaGate(&thresholdPrefixValidator{readyAt: 1})
	marked, ready, err := gate.PushFinal(context.Background(), media)
	if err != nil || !ready {
		t.Fatalf("startup gate result=(ready=%v err=%v), want validated media", ready, err)
	}
	if got, want := len(marked), len(original)+2*tsPacketSize; got != want {
		t.Fatalf("marked length=%d, want %d with two payload-only PID markers", got, want)
	}
	for pidOffset, pid := range []int{0x100, 0x101} {
		markerAt := pidOffset * 2 * tsPacketSize
		marker := marked[markerAt : markerAt+tsPacketSize]
		payload := marked[markerAt+tsPacketSize : markerAt+2*tsPacketSize]
		if gotPID := int(marker[1]&0x1f)<<8 | int(marker[2]); gotPID != pid ||
			(marker[3]>>4)&0x03 != 2 || marker[5]&0x80 == 0 {
			t.Fatalf("PID %#x first output is not an adaptation-only discontinuity: %x", pid, marker[:6])
		}
		if marker[3]&0x0f != payload[3]&0x0f {
			t.Fatalf("PID %#x marker CC=%d, first payload CC=%d", pid, marker[3]&0x0f, payload[3]&0x0f)
		}
		wantPayload := original[pidOffset*tsPacketSize : (pidOffset+1)*tsPacketSize]
		if !bytes.Equal(payload, wantPayload) {
			t.Fatalf("PID %#x first payload changed while inserting boundary", pid)
		}
	}
	// No later packet for either PID is spuriously marked: the boundary was
	// already decoder-visible before its first payload.
	for pos := 4 * tsPacketSize; pos < len(marked); pos += tsPacketSize {
		packet := marked[pos : pos+tsPacketSize]
		if packet[5]&0x80 != 0 {
			t.Fatalf("later packet at output index %d was also flagged", pos/tsPacketSize)
		}
	}
}

func TestStartupMediaGateKeepsArbitrarilySplitPrefixPrivate(t *testing.T) {
	validator := &thresholdPrefixValidator{readyAt: minPrefixDecisionBytes, start: 37}
	gate := newStartupMediaGate(validator)
	payload := make([]byte, minPrefixDecisionBytes+997)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	var output []byte
	for offset := 0; offset < len(payload); {
		end := min(offset+137, len(payload)) // deliberately not TS/chunk aligned
		got, ready, err := gate.Push(context.Background(), payload[offset:end])
		if err != nil {
			t.Fatal(err)
		}
		if !ready && len(got) != 0 {
			t.Fatalf("gate exposed %d bytes before validation", len(got))
		}
		if ready {
			output = append(output, got...)
		}
		offset = end
	}
	if validator.calls == 0 {
		t.Fatal("prefix validator was never called")
	}
	if !bytes.Equal(output, payload[validator.start:]) {
		t.Fatalf("validated output mismatch: got=%d want=%d", len(output), len(payload)-validator.start)
	}
	if gate.Reason() != "test_ready" {
		t.Fatalf("gate reason=%q, want test_ready", gate.Reason())
	}
}

func TestStartupMediaGateWaitFreezesAtDecision(t *testing.T) {
	var audioPIDs [maxAVTimelineAudioPIDs]int
	for i := range audioPIDs {
		audioPIDs[i] = -1
	}
	audioPIDs[0], audioPIDs[1] = 0x102, 0x103
	gate := newStartupMediaGate(&thresholdPrefixValidator{
		readyAt: minPrefixDecisionBytes, videoPID: 0x101,
		audioPIDs: audioPIDs, audioCount: 2,
		pcrPID: 0x321, havePCRPID: true, haveProgram: true,
	})
	if _, ready, err := gate.Push(context.Background(), make([]byte, minPrefixDecisionBytes)); err != nil {
		t.Fatal(err)
	} else if !ready {
		t.Fatal("gate did not reach its deterministic decision")
	}
	first := gate.Wait()
	time.Sleep(20 * time.Millisecond)
	if second := gate.Wait(); second != first {
		t.Fatalf("completed validation wait moved from %s to %s", first, second)
	}
	if pcrPID, ok := gate.PCRPID(); !ok || pcrPID != 0x321 {
		t.Fatalf("gate PCR PID=(%#x,%v), want selected PMT PCR PID (0x321,true)", pcrPID, ok)
	}
	program, ok := gate.SelectedProgram()
	if !ok || program.videoPID != 0x101 || program.pcrPID != 0x321 ||
		program.audioCount != 2 || program.audioPIDs[0] != 0x102 || program.audioPIDs[1] != 0x103 {
		t.Fatalf("gate selected program=(%+v,%v), want exact validated A/V/PCR PIDs", program, ok)
	}
}

func TestStartupMediaGateFinalizesFastFiniteBodyAtEOF(t *testing.T) {
	validator := &thresholdPrefixValidator{readyAt: int(minFinitePreflightBytes)}
	gate := newStartupMediaGate(validator)
	payload := make([]byte, int(minFinitePreflightBytes))
	if media, ready, err := gate.Push(context.Background(), payload[:minPrefixDecisionBytes]); err != nil {
		t.Fatal(err)
	} else if ready || len(media) != 0 {
		t.Fatal("short first probe unexpectedly published finite media")
	}
	if media, ready, err := gate.Push(context.Background(), payload[minPrefixDecisionBytes:]); err != nil {
		t.Fatal(err)
	} else if ready || len(media) != 0 {
		t.Fatal("fast sub-2MiB replay bypassed the normal probe cadence")
	}
	if validator.calls != 1 {
		t.Fatalf("normal cadence validator calls=%d, want first 64KiB probe only", validator.calls)
	}
	media, ready, err := gate.Finalize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ready || len(media) != len(payload) {
		t.Fatalf("EOF finalization ready=%v bytes=%d, want full %d-byte body",
			ready, len(media), len(payload))
	}
	if validator.calls != 2 {
		t.Fatalf("final validator calls=%d, want one forced EOF pass", validator.calls)
	}
}

func TestStartupMediaGateValidatesAtCapWithoutDroppingCrossingRead(t *testing.T) {
	validator := &thresholdPrefixValidator{readyAt: maxMediaPrefixBytes}
	gate := newStartupMediaGate(validator)
	gate.buffer = bytes.Repeat([]byte{0x31}, maxMediaPrefixBytes-7)
	gate.nextProbe = maxMediaPrefixBytes + prefixProbeByteStep
	tail := bytes.Repeat([]byte{0x72}, 19)

	media, ready, err := gate.Push(context.Background(), tail)
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("valid prefix at the buffer cap was rejected")
	}
	if validator.calls != 1 {
		t.Fatalf("cap validator calls=%d, want one forced decision", validator.calls)
	}
	wantBytes := maxMediaPrefixBytes - 7 + len(tail)
	if len(media) != wantBytes {
		t.Fatalf("released bytes=%d, want %d", len(media), wantBytes)
	}
	if !bytes.Equal(media[len(media)-len(tail):], tail) {
		t.Fatal("the read crossing the cap was not preserved exactly")
	}
}

func TestStartupMediaGateRejectsCapOnlyAfterFinalDecision(t *testing.T) {
	validator := &thresholdPrefixValidator{readyAt: maxMediaPrefixBytes + 1}
	gate := newStartupMediaGate(validator)
	gate.buffer = make([]byte, maxMediaPrefixBytes-3)
	gate.nextProbe = maxMediaPrefixBytes + prefixProbeByteStep

	if _, ready, err := gate.Push(context.Background(), make([]byte, 9)); !errors.Is(err, errMediaPrefixNotReady) || ready {
		t.Fatalf("cap decision ready=%v err=%v, want bounded not-ready error", ready, err)
	}
	if validator.calls != 1 {
		t.Fatalf("cap validator calls=%d, want one forced decision", validator.calls)
	}
}

func TestMPEG2PSICRCValidationKnownVector(t *testing.T) {
	// CRC-32/MPEG-2's published check value for "123456789" is 0x0376E6E7.
	section := append([]byte("123456789"), 0x03, 0x76, 0xe6, 0xe7)
	if !mpeg2PSICRCValid(section) {
		t.Fatal("known MPEG-2 CRC check vector did not produce a zero remainder")
	}
	section[len(section)-1] ^= 0x01
	if mpeg2PSICRCValid(section) {
		t.Fatal("corrupted MPEG-2 CRC check vector was accepted")
	}
}

func TestConfiguredRandomAccessRejectsOpenGOPNonIDR(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testPMTPacket(pmtPID, videoPID, 0x1b),
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		pcrTestNonPCRPacket(0x1fff, false),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x61, 0x88}), // non-IDR I picture
	)
	if configurationAt, randomAccessAt := findConfiguredRandomAccessNAL(data, "h264"); configurationAt >= 0 || randomAccessAt >= 0 {
		t.Fatalf("open-GOP non-IDR accepted: configuration=%d random_access=%d",
			configurationAt, randomAccessAt)
	}
}

func TestConfiguredRandomAccessRequiresFreshConfigAfterDiscontinuity(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	stale := joinTSPackets(
		testPATPacket(pmtPID),
		testPMTPacket(pmtPID, videoPID, 0x1b),
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		testAdaptationOnlyDiscontinuity(videoPID, 1),
		testPESPacket(videoPID, 2, []byte{0, 0, 1, 0x65, 0x88}),
	)
	if configurationAt, randomAccessAt := findConfiguredRandomAccessNAL(stale, "h264"); configurationAt >= 0 || randomAccessAt >= 0 {
		t.Fatalf("IDR reused configuration across discontinuity: configuration=%d random_access=%d",
			configurationAt, randomAccessAt)
	}

	fresh := joinTSPackets(
		testPATPacket(pmtPID),
		testPMTPacket(pmtPID, videoPID, 0x1b),
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		testAdaptationOnlyDiscontinuity(videoPID, 1),
		testPESPacket(videoPID, 2, h264ConfigurationNALs()),
		testPESPacket(videoPID, 3, []byte{0, 0, 1, 0x65, 0x88}),
	)
	configurationAt, randomAccessAt := findConfiguredRandomAccessNAL(fresh, "h264")
	if configurationAt < 0 || randomAccessAt <= configurationAt {
		t.Fatalf("fresh post-discontinuity configuration rejected: configuration=%d random_access=%d",
			configurationAt, randomAccessAt)
	}
}

func TestConfiguredRandomAccessRejectsTruncatedConfigurationNAL(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	tests := []struct {
		name          string
		configuration []byte
	}{
		{
			name: "header-only SPS",
			configuration: []byte{
				0, 0, 1, 0x67,
				0, 0, 1, 0x68, 0xce, 0x06, 0xe2,
			},
		},
		{
			name: "header-only PPS",
			configuration: []byte{
				0, 0, 1, 0x67, 0x42, 0, 0x1e,
				0, 0, 1, 0x68,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := joinTSPackets(
				testPATPacket(pmtPID),
				testPMTPacket(pmtPID, videoPID, 0x1b),
				testPESPacket(videoPID, 0, tt.configuration),
				testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
			)
			configurationAt, randomAccessAt := findConfiguredRandomAccessNAL(data, "h264")
			if configurationAt >= 0 || randomAccessAt >= 0 {
				t.Fatalf("truncated configuration accepted: configuration=%d random_access=%d",
					configurationAt, randomAccessAt)
			}
		})
	}
}

func TestProgramMapReleaseStartsAfterSelectedClockDiscontinuity(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		pcrPID   = 0x0150
	)
	for _, tt := range []struct {
		name             string
		discontinuityPID int
		payloadConfig    bool
	}{
		{name: "video", discontinuityPID: videoPID},
		{name: "video payload carries fresh config", discontinuityPID: videoPID, payloadConfig: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			staleMarker := []byte{0, 0, 1, 0x61, 0xde, 0xad, 0xbe, 0xef}
			packets := [][]byte{
				testPATPacket(pmtPID),
				testPMTPacketForProgramVersion(0, 1, pmtPID, videoPID, pcrPID, 0x1b, 0),
				testPESPacket(videoPID, 0, h264ConfigurationNALs()),
				testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
				testPESPacket(videoPID, 2, staleMarker),
			}
			discontinuityStart := len(packets) * tsPacketSize
			if tt.payloadConfig {
				barrier := testPESPacket(videoPID, 3, h264ConfigurationNALs())
				barrier[5] |= 0x80
				packets = append(packets, barrier)
			} else {
				packets = append(packets,
					testAdaptationOnlyDiscontinuity(tt.discontinuityPID, 2),
					testPESPacket(videoPID, 3, h264ConfigurationNALs()),
				)
			}
			packets = append(packets, testPESPacket(videoPID, 4, []byte{0, 0, 1, 0x65, 0x88}))
			data := joinTSPackets(packets...)
			configurationAt, randomAccessAt, gotPID :=
				findConfiguredRandomAccessNALProgram(data, "h264", videoPID)
			if gotPID != videoPID || configurationAt < discontinuityStart || randomAccessAt <= configurationAt {
				t.Fatalf("post-discontinuity random access=(pid=%#x config=%d random=%d), barrier=%d",
					gotPID, configurationAt, randomAccessAt, discontinuityStart)
			}
			program, ok := findSafeTSProgramPrefix(data, configurationAt, "h264", videoPID)
			wantStart := discontinuityStart
			if !ok || program.safeStart != wantStart || len(program.psiPrefix) == 0 {
				t.Fatalf("release program=(ok=%v start=%d psi=%d), want synthesized start %d",
					ok, program.safeStart, len(program.psiPrefix), wantStart)
			}
			released := append(append([]byte(nil), program.psiPrefix...), data[program.safeStart:]...)
			if bytes.Contains(released, staleMarker) {
				t.Fatal("release retained media from before the selected clock discontinuity")
			}
			released = markInitialTSDiscontinuity(released)
			if markedConfiguration, markedRandomAccess, markedPID :=
				findConfiguredRandomAccessNALProgram(released, "h264", videoPID); markedPID != videoPID || markedConfiguration < 0 || markedRandomAccess <= markedConfiguration {
				t.Fatalf("initial discontinuity marking invalidated released config: pid=%#x config=%d random=%d",
					markedPID, markedConfiguration, markedRandomAccess)
			}
		})
	}
}

func TestProgramMapReleaseDropsSelectedPacketsBeforeDeclaringPMT(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
		pcrPID   = 0x0102
	)
	pcrOnly := func(raw uint64, continuity byte) []byte {
		packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
		packet[0] = 0x47
		packet[1] = byte(pcrPID>>8) & 0x1f
		packet[2] = byte(pcrPID & 0xff)
		packet[3] = 0x20 | continuity&0x0f
		packet[4] = 183
		packet[5] = 0x10
		copy(packet[6:12], slateTestPCR(raw))
		return packet
	}
	preAudio := testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, 123*transportPTSRate, 3)
	prePCR := pcrOnly(123*transportClockRate, 9)
	postAudio := testPayloadOnlyTimestampedPESPacket(audioPID, 0xc0, 0, 4)
	postPCR := pcrOnly(0, 10)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		preAudio,
		prePCR,
		testAVPMTPacketWithPCR(pmtPID, videoPID, audioPID, pcrPID),
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
		postAudio,
		postPCR,
		pcrTestNonPCRPacket(0x1fff, false),
	)
	configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", videoPID)
	if gotPID != videoPID || configurationAt < 0 || randomAccessAt <= configurationAt {
		t.Fatalf("fixture random access=(pid=%#x config=%d random=%d)",
			gotPID, configurationAt, randomAccessAt)
	}
	program, ok := findSafeTSProgramPrefix(data, configurationAt, "h264", videoPID)
	wantStart := 4 * tsPacketSize // after the PMT that first declares A/V/PCR PIDs
	if !ok || program.safeStart != wantStart || len(program.psiPrefix) == 0 {
		t.Fatalf("selected-before-PMT release=(ok=%v start=%d psi=%d), want synthesized start %d",
			ok, program.safeStart, len(program.psiPrefix), wantStart)
	}
	released := append(append([]byte(nil), program.psiPrefix...), data[program.safeStart:]...)
	if bytes.Contains(released, preAudio) || bytes.Contains(released, prePCR) {
		t.Fatal("release retained selected packets consumed before their declaring PMT")
	}

	selected := avProgramPIDs{
		videoPID: program.videoPID, pcrPID: program.pcrPID,
		audioPIDs: program.audioPIDs, audioCount: program.audioCount,
		mapIdentity: program.mapIdentity,
	}
	var scanner selectedProgramBoundaryScanner
	scanner.configure(selected, true)
	transformed, err := scanner.transform(released)
	if err != nil {
		t.Fatal(err)
	}
	pmtPosition := -1
	for pos := 0; pos+tsPacketSize <= len(transformed); pos += tsPacketSize {
		packet := transformed[pos : pos+tsPacketSize]
		if int(packet[1]&0x1f)<<8|int(packet[2]) == pmtPID {
			pmtPosition = pos
		}
	}
	if pmtPosition < 0 {
		t.Fatal("synthesized release lost selected PMT")
	}
	for _, tc := range []struct {
		name   string
		pid    int
		packet []byte
	}{
		{name: "audio", pid: audioPID, packet: postAudio},
		{name: "distinct PCR", pid: pcrPID, packet: postPCR},
	} {
		position := bytes.Index(transformed, tc.packet)
		if position < pmtPosition+tsPacketSize || position < tsPacketSize {
			t.Fatalf("%s first retained packet position=%d, PMT=%d", tc.name, position, pmtPosition)
		}
		marker := transformed[position-tsPacketSize : position]
		if markerPID := int(marker[1]&0x1f)<<8 | int(marker[2]); markerPID != tc.pid ||
			!tsPacketHasDiscontinuity(marker) || marker[3]&0x0f != tc.packet[3]&0x0f {
			t.Fatalf("%s retained packet lacks adjacent post-PMT marker with matching CC: %x",
				tc.name, marker[:6])
		}
	}
}

func TestConfiguredRandomAccessCarriesSplitPESHeader(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	pes := append(testPESHeader(), h264ConfigurationNALs()...)
	pes = append(pes, []byte{0, 0, 1, 0x65, 0x88}...)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testPMTPacket(pmtPID, videoPID, 0x1b),
		testExactPayloadPacket(videoPID, true, 0, pes[:6]),
		testExactPayloadPacket(videoPID, false, 1, pes[6:]),
		pcrTestNonPCRPacket(0x1fff, false),
	)
	configurationAt, randomAccessAt := findConfiguredRandomAccessNAL(data, "h264")
	if configurationAt < 0 || randomAccessAt < configurationAt {
		t.Fatalf("split PES header lost configured IDR: configuration=%d random_access=%d",
			configurationAt, randomAccessAt)
	}
}

func TestConfiguredRandomAccessCarriesMultiPacketPMTWithPointer(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	section := testPMTSection(videoPID, 0x1b)
	section = addPMTProgramDescriptor(t, section, 178)
	pmtPackets := testPSISectionPackets(pmtPID, 0, []byte{0xaa, 0xbb, 0xcc}, section)
	if len(pmtPackets) < 2 {
		t.Fatalf("PMT used %d packet, want multi-packet fixture", len(pmtPackets))
	}
	packets := [][]byte{testPATPacket(pmtPID)}
	packets = append(packets, pmtPackets...)
	packets = append(packets,
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		pcrTestNonPCRPacket(0x1fff, false),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
	)
	data := joinTSPackets(packets...)
	if got, ok := videoPIDFromTS(data); !ok || got != videoPID {
		t.Fatalf("multi-packet PMT video PID=(%#x,%v), want (%#x,true)", got, ok, videoPID)
	}
	configurationAt, randomAccessAt := findConfiguredRandomAccessNAL(data, "h264")
	if configurationAt < 0 || randomAccessAt <= configurationAt {
		t.Fatalf("multi-packet PMT lost configured IDR: configuration=%d random_access=%d",
			configurationAt, randomAccessAt)
	}
}

func TestConfiguredRandomAccessSelectsRequestedCodecFromLaterPATProgram(t *testing.T) {
	tests := []struct {
		name              string
		codec             string
		decoyStreamType   byte
		wantedStreamType  byte
		configurationNALs []byte
		randomAccessNALs  []byte
	}{
		{
			name: "h264_after_hevc", codec: "h264",
			decoyStreamType: 0x24, wantedStreamType: 0x1b,
			configurationNALs: h264ConfigurationNALs(),
			randomAccessNALs:  []byte{0, 0, 1, 0x65, 0x88},
		},
		{
			name: "hevc_after_h264", codec: "hevc",
			decoyStreamType: 0x1b, wantedStreamType: 0x24,
			configurationNALs: []byte{
				0, 0, 1, 32 << 1, 1, 0x80,
				0, 0, 1, 33 << 1, 1, 0x80,
				0, 0, 1, 34 << 1, 1, 0x80,
			},
			randomAccessNALs: []byte{0, 0, 1, 19 << 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				decoyVideoPID  = 0x0100
				wantedVideoPID = 0x0200
			)
			pmtPIDs := make([]int, 44)
			for i := range pmtPIDs {
				pmtPIDs[i] = 0x1000 + i
			}
			patPackets := testPSISectionPackets(
				0, 0, []byte{0xaa, 0xbb, 0xcc}, testPATSection(pmtPIDs...),
			)
			if len(patPackets) < 2 {
				t.Fatalf("PAT used %d packet, want multi-packet MPTS fixture", len(patPackets))
			}

			wantedPMT := addPMTProgramDescriptor(
				t, testPMTSectionForProgram(len(pmtPIDs), wantedVideoPID, tt.wantedStreamType), 178,
			)
			wantedPMTPackets := testPSISectionPackets(
				pmtPIDs[len(pmtPIDs)-1], 0, []byte{0xdd, 0xee}, wantedPMT,
			)
			if len(wantedPMTPackets) < 2 {
				t.Fatalf("wanted PMT used %d packet, want multi-packet fixture", len(wantedPMTPackets))
			}

			packets := append([][]byte(nil), patPackets...)
			packets = append(packets,
				testPMTPacket(pmtPIDs[0], decoyVideoPID, tt.decoyStreamType),
			)
			packets = append(packets, wantedPMTPackets...)
			packets = append(packets,
				testPESPacket(wantedVideoPID, 0, tt.configurationNALs),
				pcrTestNonPCRPacket(0x1fff, false),
				testPESPacket(wantedVideoPID, 1, tt.randomAccessNALs),
			)
			data := joinTSPackets(packets...)

			if got, ok := videoPIDFromTS(data); !ok || got != decoyVideoPID {
				t.Fatalf("generic first video PID=(%#x,%v), want decoy (%#x,true)",
					got, ok, decoyVideoPID)
			}
			configurationAt, randomAccessAt, gotPID :=
				findConfiguredRandomAccessNALProgram(data, tt.codec, wantedVideoPID)
			if gotPID != wantedVideoPID || configurationAt < 0 || randomAccessAt <= configurationAt {
				t.Fatalf("requested %s program=(pid=%#x configuration=%d random_access=%d), want later PID %#x",
					tt.codec, gotPID, configurationAt, randomAccessAt, wantedVideoPID)
			}
			safeStart, ok := findSafeTSPrefixForProgram(
				data, configurationAt, tt.codec, gotPID,
			)
			if !ok || safeStart != 0 {
				t.Fatalf("requested %s safe PAT=(%d,%v), want (0,true)",
					tt.codec, safeStart, ok)
			}
		})
	}
}

func TestFFprobeTSPIDParsingIsStrictAndBounded(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
		ok   bool
	}{
		{name: "quoted hex", raw: `"0x100"`, want: 0x100, ok: true},
		{name: "quoted decimal", raw: `"256"`, want: 0x100, ok: true},
		{name: "number", raw: `256`, want: 0x100, ok: true},
		{name: "max PID", raw: `"0x1fff"`, want: 0x1fff, ok: true},
		{name: "missing", raw: ``, ok: false},
		{name: "null", raw: `null`, ok: false},
		{name: "negative", raw: `-1`, ok: false},
		{name: "fractional", raw: `256.0`, ok: false},
		{name: "trailing junk", raw: `"0x100x"`, ok: false},
		{name: "out of range", raw: `8192`, ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseFFprobeTSPID(json.RawMessage(tt.raw))
			if ok != tt.ok || got != tt.want {
				t.Fatalf("parseFFprobeTSPID(%q)=(%#x,%v), want (%#x,%v)",
					tt.raw, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestFFprobeValidatorBindsSelectedTSPIDAndFrameStream(t *testing.T) {
	const (
		selectedPMTPID = 0x1000
		decoyPMTPID    = 0x1001
		selectedPID    = 0x0100
		decoyPID       = 0x0200
		streamIndex    = 7
	)
	packets := [][]byte{
		testPATPacketForTable(1, 0, true, 0, 0,
			testPATProgram{programNumber: 1, pmtPID: selectedPMTPID},
			testPATProgram{programNumber: 2, pmtPID: decoyPMTPID},
		),
		testPMTPacketForProgram(1, selectedPMTPID, selectedPID, 0x1b),
		testPMTPacketForProgram(2, decoyPMTPID, decoyPID, 0x1b),
		testPESPacket(decoyPID, 0, h264ConfigurationNALs()),
		pcrTestNonPCRPacket(0x1fff, false),
		testPESPacket(decoyPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
	}
	data := joinTSPackets(packets...)
	for len(data) < minTSProbeBytes+tsPacketSize*16 {
		data = append(data, pcrTestNonPCRPacket(0x1fff, false)...)
	}
	_, decoyRandomAccess, gotPID := findConfiguredRandomAccessNALProgram(data, "h264", decoyPID)
	if decoyRandomAccess < 0 || gotPID != decoyPID {
		t.Fatalf("decoy fixture random access=(%d,%#x), want configured PID %#x",
			decoyRandomAccess, gotPID, decoyPID)
	}
	if configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", selectedPID); configurationAt >= 0 || randomAccessAt >= 0 || gotPID >= 0 {
		t.Fatalf("selected PID borrowed decoy media: configuration=%d random_access=%d pid=%#x",
			configurationAt, randomAccessAt, gotPID)
	}

	tests := []struct {
		name             string
		id               any
		frameStreamIndex int
		wantKind         prefixDecisionKind
		wantReason       string
	}{
		{
			name: "selected PID cannot borrow same-codec program", id: "0x100",
			frameStreamIndex: streamIndex, wantKind: prefixNeedMore,
			wantReason: "configured_idr_or_irap_missing",
		},
		{
			name: "invalid PID stays private", id: "not-a-pid",
			frameStreamIndex: streamIndex, wantKind: prefixNeedMore,
			wantReason: "mpegts_video_pid_missing",
		},
		{
			name: "missing PID stays private", id: nil,
			frameStreamIndex: streamIndex, wantKind: prefixNeedMore,
			wantReason: "mpegts_video_pid_missing",
		},
		{
			name: "frame from another stream is ignored", id: "0x200",
			frameStreamIndex: streamIndex + 1, wantKind: prefixNeedMore,
			wantReason: "random_access_frame_missing",
		},
		{
			name: "exact PID and frame stream validate", id: "0x200",
			frameStreamIndex: streamIndex, wantKind: prefixReady,
			wantReason: "mpegts_pat_pmt_keyframe_buffered",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probeOutput := testPrefixProbeJSON(
				t, tt.id, streamIndex, tt.frameStreamIndex, decoyRandomAccess,
			)
			validator := &ffprobeMediaPrefixValidator{
				ffprobeBinary: testFakeFFprobe(t, probeOutput), available: true,
			}
			decision, err := validator.Validate(context.Background(), data)
			if err != nil {
				t.Fatal(err)
			}
			if decision.kind != tt.wantKind || decision.reason != tt.wantReason {
				t.Fatalf("decision=(%v,%q), want (%v,%q)",
					decision.kind, decision.reason, tt.wantKind, tt.wantReason)
			}
			if tt.wantKind == prefixReady && (!decision.havePCRPID || decision.pcrPID != decoyPID) {
				t.Fatalf("ready decision PCR PID=(%#x,%v), want exact ffprobe program %#x",
					decision.pcrPID, decision.havePCRPID, decoyPID)
			}
		})
	}
}

func TestFFprobeValidatorRejectsMalformedPostBarrierConfiguration(t *testing.T) {
	const (
		pmtPID      = 0x1000
		videoPID    = 0x0100
		streamIndex = 7
	)
	validConfiguration := h264ConfigurationNALs()
	malformedConfiguration := []byte{
		0, 0, 1, 0x67, 0xff, // structurally delimited but unusable SPS
		0, 0, 1, 0x68, 0xff, // structurally delimited but unusable PPS
	}
	packets := [][]byte{
		testPATPacket(pmtPID),
		testPMTPacketForProgramVersion(0, 1, pmtPID, videoPID, videoPID, 0x1b, 31),
		testPESPacket(videoPID, 0, validConfiguration),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
		testPMTPacketForProgramVersion(1, 1, pmtPID, videoPID, videoPID, 0x1b, 0),
		// A delayed stale table advances the release barrier again. Valid
		// pre-barrier decoder state must not validate what follows it.
		testPMTPacketForProgramVersion(2, 1, pmtPID, videoPID, videoPID, 0x1b, 31),
		testPESPacket(videoPID, 2, malformedConfiguration),
		testPESPacket(videoPID, 3, []byte{0, 0, 1, 0x65, 0x88}),
	}
	data := joinTSPackets(packets...)
	for len(data) < minTSProbeBytes+tsPacketSize*16 {
		data = append(data, pcrTestNonPCRPacket(0x1fff, false)...)
	}
	configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", videoPID)
	if gotPID != videoPID || configurationAt < 0 || randomAccessAt <= configurationAt {
		t.Fatalf("malformed fixture did not reach the exact-probe seam: pid=%#x config=%d random=%d",
			gotPID, configurationAt, randomAccessAt)
	}
	program, ok := findSafeTSProgramPrefix(data, configurationAt, "h264", videoPID)
	wantStart := 6 * tsPacketSize // immediately after the delayed stale PMT
	if !ok || program.safeStart != wantStart || len(program.psiPrefix) == 0 {
		t.Fatalf("barrier program=(ok=%v start=%d psi=%d), want synthesized start %d",
			ok, program.safeStart, len(program.psiPrefix), wantStart)
	}

	var fullProbe prefixProbeOutput
	if err := json.Unmarshal(
		testPrefixProbeJSON(t, "0x100", streamIndex, streamIndex, randomAccessAt),
		&fullProbe,
	); err != nil {
		t.Fatal(err)
	}
	releaseProbe := fullProbe
	releaseProbe.Streams = append(releaseProbe.Streams[:0:0], fullProbe.Streams...)
	releaseProbe.Streams[0].ExtradataSize = 0
	releaseProbe.Streams[0].Extradata = ""

	probeCalls := 0
	validator := &ffprobeMediaPrefixValidator{
		available: true,
		probePrefix: func(_ context.Context, candidate []byte) (prefixProbeOutput, string, error) {
			probeCalls++
			switch probeCalls {
			case 1:
				if !bytes.Contains(candidate, validConfiguration) {
					t.Fatal("wide probe fixture lost its valid pre-barrier configuration")
				}
				return fullProbe, "", nil
			case 2:
				if bytes.Contains(candidate, validConfiguration) {
					t.Fatal("exact release probe retained valid pre-barrier configuration")
				}
				if !bytes.Contains(candidate, malformedConfiguration) {
					t.Fatal("exact release probe did not receive the malformed post-barrier candidate")
				}
				return releaseProbe, "", nil
			default:
				t.Fatalf("unexpected probe call %d", probeCalls)
				return prefixProbeOutput{}, "", nil
			}
		},
	}
	decision, err := validator.Validate(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	if probeCalls != 2 || decision.kind != prefixNeedMore ||
		decision.reason != "release_codec_configuration_missing" {
		t.Fatalf("decision=(calls=%d kind=%v reason=%q), want exact-release rejection",
			probeCalls, decision.kind, decision.reason)
	}
}

func TestProgramMapEpochRetiresRemovedProgramAfterCompleteMultiSectionPAT(t *testing.T) {
	const (
		oldPMTPID   = 0x1000
		oldVideoPID = 0x0100
		newPMTPID   = 0x1100
		newVideoPID = 0x0200
	)
	packets := [][]byte{
		testPATPacketForTableContinuity(0, 9, 0, true, 0, 0,
			testPATProgram{programNumber: 1, pmtPID: oldPMTPID},
		),
		testPMTPacketForProgram(1, oldPMTPID, oldVideoPID, 0x1b),
		testPESPacket(oldVideoPID, 0, h264ConfigurationNALs()),
		testPESPacket(oldVideoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
	}
	newPATStart := len(packets) * tsPacketSize
	newPMTStart := (len(packets) + 5) * tsPacketSize
	packets = append(packets,
		testPATPacketForTableContinuity(1, 9, 1, true, 1, 1,
			testPATProgram{programNumber: 2, pmtPID: newPMTPID},
		),
	)
	partial := joinTSPackets(packets...)
	if configurationAt, randomAccessAt, _ :=
		findConfiguredRandomAccessNALProgram(partial, "h264", oldVideoPID); configurationAt < 0 || randomAccessAt < configurationAt {
		t.Fatalf("partial newer PAT retired active epoch early: configuration=%d random_access=%d",
			configurationAt, randomAccessAt)
	}

	packets = append(packets,
		testPATPacketForTableContinuity(2, 9, 1, true, 0, 1), // completes v1 atomically
		testPMTPacketForProgram(1, oldPMTPID, oldVideoPID, 0x1b),
		testPESPacket(oldVideoPID, 2, h264ConfigurationNALs()),
		testPESPacket(oldVideoPID, 3, []byte{0, 0, 1, 0x65, 0x88}),
		testPMTPacketForProgram(2, newPMTPID, newVideoPID, 0x1b),
		// Same-version and future-table announcements cannot erase v1's PMT.
		testPATPacketForTableContinuity(3, 9, 1, true, 0, 1),
		testPATPacketForTableContinuity(4, 9, 2, false, 0, 0),
		testPESPacket(newVideoPID, 0, h264ConfigurationNALs()),
		testPESPacket(newVideoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
	)
	data := joinTSPackets(packets...)
	if configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", oldVideoPID); configurationAt >= 0 || randomAccessAt >= 0 || gotPID >= 0 {
		t.Fatalf("removed old program survived v1 activation: configuration=%d random_access=%d pid=%#x",
			configurationAt, randomAccessAt, gotPID)
	}
	configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", newVideoPID)
	if configurationAt < 0 || randomAccessAt <= configurationAt || gotPID != newVideoPID {
		t.Fatalf("new multi-section epoch rejected: configuration=%d random_access=%d pid=%#x",
			configurationAt, randomAccessAt, gotPID)
	}
	safeStart, ok := findSafeTSPrefixForProgram(data, configurationAt, "h264", newVideoPID)
	program, programOK := findSafeTSProgramPrefix(data, configurationAt, "h264", newVideoPID)
	wantStart := newPMTStart + tsPacketSize
	if !ok || !programOK || safeStart != wantStart || program.safeStart != wantStart || len(program.psiPrefix) == 0 {
		t.Fatalf("multi-section PAT release=(safe=%d/%d ok=%v/%v psi=%d), want synthesized start %d after PMT (PAT began %d)",
			safeStart, program.safeStart, ok, programOK, len(program.psiPrefix), wantStart, newPATStart)
	}
}

func TestProgramMapSupersedesPMTVersionAcrossRolloverAndRejectsStaleArrival(t *testing.T) {
	const (
		pmtPID    = 0x1000
		videoPID  = 0x0100
		oldPCRPID = 0x0150
		newPCRPID = 0x0250
	)
	packets := [][]byte{
		testPATPacket(pmtPID),
		testPMTPacketForProgramVersion(0, 1, pmtPID, videoPID, oldPCRPID, 0x1b, 31),
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
	}
	newPMTStart := len(packets) * tsPacketSize
	// Version 31 -> 0 is the MPEG 5-bit version rollover. The later
	// multi-packet PMT keeps the same video PID but moves PCR to another PID.
	newPMTSection := addPMTProgramDescriptor(t,
		testPMTSectionForProgramVersion(1, videoPID, newPCRPID, 0x1b, 0), 178)
	newPMTPackets := testPSISectionPackets(pmtPID, 1, nil, newPMTSection)
	if len(newPMTPackets) != 2 {
		t.Fatalf("new PMT packets=%d, want two", len(newPMTPackets))
	}
	packets = append(packets, newPMTPackets...)
	packets = append(packets,
		// A delayed copy of version 31 must not restore the superseded map.
		testPMTPacketForProgramVersion(3, 1, pmtPID, videoPID, oldPCRPID, 0x1b, 31),
		testPESPacket(videoPID, 2, h264ConfigurationNALs()),
	)
	// The retained same-current repetition must continue from the synthesized
	// multi-packet PMT without a continuity-counter gap.
	packets = append(packets, testPSISectionPackets(pmtPID, 4, nil, newPMTSection)...)
	packets = append(packets, testPESPacket(videoPID, 3, []byte{0, 0, 1, 0x65, 0x88}))
	data := joinTSPackets(packets...)

	configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", videoPID)
	if configurationAt < 0 || randomAccessAt <= configurationAt || gotPID != videoPID {
		t.Fatalf("rolled-over PMT media rejected: configuration=%d random_access=%d pid=%#x",
			configurationAt, randomAccessAt, gotPID)
	}
	program, ok := findSafeTSProgramPrefix(data, configurationAt, "h264", videoPID)
	if !ok {
		t.Fatal("rolled-over PMT did not produce a safe program prefix")
	}
	if program.pmtStart != newPMTStart {
		t.Fatalf("selected PMT start=%d, want current rolled-over PMT at %d",
			program.pmtStart, newPMTStart)
	}
	if program.pcrPID != newPCRPID {
		t.Fatalf("selected PCR PID=%#x, want current PMT PCR PID %#x",
			program.pcrPID, newPCRPID)
	}
	if wantStart := newPMTStart + 3*tsPacketSize; program.safeStart != wantStart {
		t.Fatalf("released source start=%d, want %d after delayed stale PMT",
			program.safeStart, wantStart)
	}
	if configurationAt < program.safeStart || randomAccessAt <= configurationAt {
		t.Fatalf("fresh media does not follow release barrier: start=%d configuration=%d random_access=%d",
			program.safeStart, configurationAt, randomAccessAt)
	}
	released := append(append([]byte(nil), program.psiPrefix...), data[program.safeStart:]...)
	if pid := int(released[1]&0x1f)<<8 | int(released[2]); pid != 0 {
		t.Fatalf("coherent release begins on PID %#x, want PAT PID 0", pid)
	}
	maps := scanTSProgramMaps(program.psiPrefix, "h264")
	if len(maps) != 1 || maps[0].videoPID != videoPID || maps[0].pcrPID != newPCRPID {
		t.Fatalf("coherent PSI maps=%+v, want video=%#x PCR=%#x",
			maps, videoPID, newPCRPID)
	}
	patAssembler := &psiSectionAssembler{start: -1}
	var releasedPATSections int
	for pos := 0; pos+tsPacketSize <= len(program.psiPrefix); pos += tsPacketSize {
		packet := program.psiPrefix[pos : pos+tsPacketSize]
		if pid := int(packet[1]&0x1f)<<8 | int(packet[2]); pid != 0 {
			continue
		}
		for _, section := range patAssembler.push(packet, pos) {
			releasedPATSections++
			if !bytes.Equal(section.data, testPATSection(pmtPID)) {
				t.Fatal("synthesized PAT did not preserve the complete section and CRC")
			}
		}
	}
	if releasedPATSections != 1 {
		t.Fatalf("released PAT sections=%d, want one complete current section", releasedPATSections)
	}
	pmtAssembler := &psiSectionAssembler{start: -1}
	pmtSections := 0
	for pos := 0; pos+tsPacketSize <= len(released); pos += tsPacketSize {
		packet := released[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid != pmtPID {
			continue
		}
		for _, section := range pmtAssembler.push(packet, pos) {
			pmtSections++
			if !bytes.Equal(section.data, newPMTSection) {
				t.Fatal("synthesized/retained PMT did not preserve the selected complete section and CRC")
			}
			parsed, parsedOK := parsePMTForCodecSection(section.data, "h264")
			if !parsedOK || parsed.version != 0 || parsed.pcrPID != newPCRPID {
				t.Fatalf("released stale/incoherent PMT: parsed=%+v ok=%v", parsed, parsedOK)
			}
		}
	}
	if pmtSections != 2 {
		t.Fatalf("released PMT sections=%d, want synthesized current plus retained current repeat", pmtSections)
	}
}

func TestProgramMapIgnoresCRCInvalidPATBeforeValidSameVersion(t *testing.T) {
	const (
		badPMTPID  = 0x1001
		goodPMTPID = 0x1000
		videoPID   = 0x0100
		pcrPID     = 0x0150
	)
	invalidPAT := testPATSectionForTable(7, 5, true, 0, 0,
		testPATProgram{programNumber: 1, pmtPID: badPMTPID})
	invalidPAT[len(invalidPAT)-1] ^= 0x01
	validPAT := testPATSectionForTable(7, 5, true, 0, 0,
		testPATProgram{programNumber: 1, pmtPID: goodPMTPID})
	if _, ok := parsePATPMTPIDSection(invalidPAT); ok {
		t.Fatal("CRC-invalid PAT was accepted by the single-section helper")
	}
	if got, ok := parsePATPMTPIDSection(validPAT); !ok || got != goodPMTPID {
		t.Fatalf("valid PAT PMT PID=(%#x,%v), want (%#x,true)", got, ok, goodPMTPID)
	}

	data := joinTSPackets(
		testExactPayloadPacket(0, true, 0, append([]byte{0}, invalidPAT...)),
		testExactPayloadPacket(0, true, 1, append([]byte{0}, validPAT...)),
		testPMTPacketForProgramVersion(0, 1, goodPMTPID, videoPID, pcrPID, 0x1b, 0),
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
	)
	configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", videoPID)
	if configurationAt < 0 || randomAccessAt <= configurationAt || gotPID != videoPID {
		t.Fatalf("valid same-version PAT did not recover startup: configuration=%d random_access=%d pid=%#x",
			configurationAt, randomAccessAt, gotPID)
	}
	program, ok := findSafeTSProgramPrefix(data, configurationAt, "h264", videoPID)
	if !ok || program.safeStart != tsPacketSize || program.pcrPID != pcrPID || len(program.psiPrefix) != 0 {
		t.Fatalf("recovered PAT program=%+v ok=%v, want original valid PAT at packet one and PCR %#x",
			program, ok, pcrPID)
	}
}

func TestProgramMapIgnoresCRCInvalidSplitPMTBeforeValidSameVersion(t *testing.T) {
	const (
		pmtPID      = 0x1000
		videoPID    = 0x0100
		oldPCRPID   = 0x0150
		wrongPCRPID = 0x01f0
		newPCRPID   = 0x0250
	)
	validPMT := addPMTProgramDescriptor(t,
		testPMTSectionForProgramVersion(1, videoPID, newPCRPID, 0x1b, 0), 178)
	invalidPMT := append([]byte(nil), validPMT...)
	invalidPMT[8], invalidPMT[9] = 0xe0|byte(wrongPCRPID>>8), byte(wrongPCRPID&0xff)
	if _, ok := parsePMTVideoPIDSection(invalidPMT); ok {
		t.Fatal("CRC-invalid PMT was accepted by the single-section helper")
	}
	if got, ok := parsePMTVideoPIDSection(validPMT); !ok || got != videoPID {
		t.Fatalf("valid PMT video PID=(%#x,%v), want (%#x,true)", got, ok, videoPID)
	}

	packets := [][]byte{
		testPATPacket(pmtPID),
		testPMTPacketForProgramVersion(0, 1, pmtPID, videoPID, oldPCRPID, 0x1b, 31),
	}
	invalidPackets := testPSISectionPackets(pmtPID, 1, nil, invalidPMT)
	if len(invalidPackets) != 2 {
		t.Fatalf("CRC-invalid PMT packets=%d, want two", len(invalidPackets))
	}
	packets = append(packets, invalidPackets...)
	validPMTStart := len(packets) * tsPacketSize
	validPackets := testPSISectionPackets(pmtPID, 3, nil, validPMT)
	if len(validPackets) != 2 {
		t.Fatalf("valid PMT packets=%d, want two", len(validPackets))
	}
	packets = append(packets, validPackets...)
	packets = append(packets,
		testPESPacket(videoPID, 0, h264ConfigurationNALs()),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
	)
	data := joinTSPackets(packets...)
	configurationAt, randomAccessAt, gotPID :=
		findConfiguredRandomAccessNALProgram(data, "h264", videoPID)
	if configurationAt < 0 || randomAccessAt <= configurationAt || gotPID != videoPID {
		t.Fatalf("valid same-version PMT did not recover startup: configuration=%d random_access=%d pid=%#x",
			configurationAt, randomAccessAt, gotPID)
	}
	program, ok := findSafeTSProgramPrefix(data, configurationAt, "h264", videoPID)
	if !ok || program.pmtStart != validPMTStart || program.pcrPID != newPCRPID {
		t.Fatalf("selected program=(ok=%v pmt=%d PCR=%#x), want valid PMT at %d and PCR %#x",
			ok, program.pmtStart, program.pcrPID, validPMTStart, newPCRPID)
	}
	if len(program.psiPrefix) == 0 || program.safeStart != validPMTStart+2*tsPacketSize {
		t.Fatalf("valid replacement PMT prefix=(psi=%d start=%d), want synthesized PSI and start %d",
			len(program.psiPrefix), program.safeStart, validPMTStart+2*tsPacketSize)
	}
	maps := scanTSProgramMaps(program.psiPrefix, "h264")
	if len(maps) != 1 || maps[0].pcrPID != newPCRPID || maps[0].pcrPID == wrongPCRPID {
		t.Fatalf("synthesized CRC-valid PMT maps=%+v, want PCR %#x and never %#x",
			maps, newPCRPID, wrongPCRPID)
	}
	pmtAssembler := &psiSectionAssembler{start: -1}
	var sections int
	for pos := 0; pos+tsPacketSize <= len(program.psiPrefix); pos += tsPacketSize {
		packet := program.psiPrefix[pos : pos+tsPacketSize]
		if pid := int(packet[1]&0x1f)<<8 | int(packet[2]); pid != pmtPID {
			continue
		}
		for _, section := range pmtAssembler.push(packet, pos) {
			sections++
			if !bytes.Equal(section.data, validPMT) || !mpeg2PSICRCValid(section.data) {
				t.Fatal("synthesized PMT did not preserve the valid complete section and CRC")
			}
		}
	}
	if sections != 1 {
		t.Fatalf("synthesized PMT sections=%d, want one", sections)
	}
}

func TestProgramMapRejectsWrongProgramAndFuturePMT(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	tests := []struct {
		name      string
		mutatePMT func([]byte)
	}{
		{
			name: "wrong program number",
			mutatePMT: func(section []byte) {
				section[3], section[4] = 0, 8
				writeMPEG2CRC(section)
			},
		},
		{
			name: "future PMT",
			mutatePMT: func(section []byte) {
				section[5] &^= 0x01
				writeMPEG2CRC(section)
			},
		},
		{
			name: "segmented PMT",
			mutatePMT: func(section []byte) {
				section[6], section[7] = 1, 1
				writeMPEG2CRC(section)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			section := testPMTSectionForProgram(7, videoPID, 0x1b)
			tt.mutatePMT(section)
			data := joinTSPackets(
				testPATPacketForTable(1, 0, true, 0, 0,
					testPATProgram{programNumber: 7, pmtPID: pmtPID},
				),
				testExactPayloadPacket(pmtPID, true, 0, append([]byte{0}, section...)),
				testPESPacket(videoPID, 0, h264ConfigurationNALs()),
				testPESPacket(videoPID, 1, []byte{0, 0, 1, 0x65, 0x88}),
			)
			if configurationAt, randomAccessAt, gotPID :=
				findConfiguredRandomAccessNALProgram(data, "h264", videoPID); configurationAt >= 0 || randomAccessAt >= 0 || gotPID >= 0 {
				t.Fatalf("invalid PMT accepted: configuration=%d random_access=%d pid=%#x",
					configurationAt, randomAccessAt, gotPID)
			}
		})
	}
}

func TestProgramMapRejectsMultiPacketPMTContinuityGap(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	section := addPMTProgramDescriptor(t, testPMTSection(videoPID, 0x1b), 178)
	pmtPackets := testPSISectionPackets(pmtPID, 0, nil, section)
	if len(pmtPackets) != 2 {
		t.Fatalf("PMT used %d packets, want exactly two for continuity-gap test", len(pmtPackets))
	}
	pmtPackets[1][3] = (pmtPackets[1][3] & 0xf0) | 0x02 // expected 1
	data := joinTSPackets(
		testPATPacket(pmtPID),
		pmtPackets[0],
		pmtPackets[1],
		pcrTestNonPCRPacket(0x1fff, false),
		pcrTestNonPCRPacket(0x1fff, false),
	)
	if got, ok := videoPIDFromTS(data); ok {
		t.Fatalf("continuity-gapped PMT produced video PID %#x", got)
	}
}

func TestConfiguredHEVCRandomAccessRequiresVPSPPSAndSPS(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
	)
	configuration := []byte{
		0, 0, 1, 32 << 1, 1, 0x80,
		0, 0, 1, 33 << 1, 1, 0x80,
		0, 0, 1, 34 << 1, 1, 0x80,
	}
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testPMTPacket(pmtPID, videoPID, 0x24),
		testPESPacket(videoPID, 0, configuration),
		pcrTestNonPCRPacket(0x1fff, false),
		testPESPacket(videoPID, 1, []byte{0, 0, 1, 19 << 1, 1}),
	)
	configurationAt, randomAccessAt := findConfiguredRandomAccessNAL(data, "hevc")
	if configurationAt < 0 || randomAccessAt <= configurationAt {
		t.Fatalf("configured HEVC IRAP rejected: configuration=%d random_access=%d",
			configurationAt, randomAccessAt)
	}
}

func joinTSPackets(packets ...[]byte) []byte {
	return bytes.Join(packets, nil)
}

func testPATPacket(pmtPID int) []byte {
	section := testPATSection(pmtPID)
	payload := append([]byte{0}, section...)
	return testExactPayloadPacket(0, true, 0, payload)
}

func testPATPacketForTable(
	transportStreamID uint16,
	version byte,
	current bool,
	sectionNumber byte,
	lastSectionNumber byte,
	programs ...testPATProgram,
) []byte {
	return testPATPacketForTableContinuity(
		0, transportStreamID, version, current, sectionNumber, lastSectionNumber, programs...,
	)
}

func testPATPacketForTableContinuity(
	continuity byte,
	transportStreamID uint16,
	version byte,
	current bool,
	sectionNumber byte,
	lastSectionNumber byte,
	programs ...testPATProgram,
) []byte {
	section := testPATSectionForTable(
		transportStreamID, version, current, sectionNumber, lastSectionNumber, programs...,
	)
	payload := append([]byte{0}, section...)
	return testExactPayloadPacket(0, true, continuity&0x0f, payload)
}

func testPATSection(pmtPIDs ...int) []byte {
	programs := make([]testPATProgram, 0, len(pmtPIDs))
	for i, pmtPID := range pmtPIDs {
		programs = append(programs, testPATProgram{programNumber: i + 1, pmtPID: pmtPID})
	}
	return testPATSectionForTable(1, 0, true, 0, 0, programs...)
}

type testPATProgram struct {
	programNumber int
	pmtPID        int
}

func testPATSectionForTable(
	transportStreamID uint16,
	version byte,
	current bool,
	sectionNumber byte,
	lastSectionNumber byte,
	programs ...testPATProgram,
) []byte {
	section := make([]byte, 12+4*len(programs))
	sectionLength := len(section) - 3
	section[0] = 0x00
	section[1] = 0xb0 | byte(sectionLength>>8)&0x0f
	section[2] = byte(sectionLength)
	section[3], section[4] = byte(transportStreamID>>8), byte(transportStreamID)
	section[5] = 0xc0 | (version&0x1f)<<1
	if current {
		section[5] |= 0x01
	}
	section[6], section[7] = sectionNumber, lastSectionNumber
	for i, program := range programs {
		pos := 8 + 4*i
		section[pos], section[pos+1] = byte(program.programNumber>>8), byte(program.programNumber)
		section[pos+2], section[pos+3] = 0xe0|byte(program.pmtPID>>8), byte(program.pmtPID)
	}
	writeMPEG2CRC(section)
	return section
}

func testPrefixProbeJSON(
	t *testing.T,
	id any,
	streamIndex int,
	frameStreamIndex int,
	keyPosition int,
) []byte {
	t.Helper()
	frames := make([]map[string]any, 0, minUsefulDecodedFrames)
	for i := 0; i < minUsefulDecodedFrames; i++ {
		frames = append(frames, map[string]any{
			"stream_index": frameStreamIndex,
			"key_frame": func() int {
				if i == 0 {
					return 1
				}
				return 0
			}(),
			"pkt_pos":                    strconv.Itoa(keyPosition + i*tsPacketSize),
			"best_effort_timestamp_time": fmt.Sprintf("%.3f", float64(i)*0.125),
		})
	}
	output, err := json.Marshal(map[string]any{
		"streams": []map[string]any{{
			"index": streamIndex, "id": id, "codec_name": "h264",
			"codec_type": "video", "extradata": "00", "extradata_size": 1,
		}},
		"frames": frames,
	})
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func testFakeFFprobe(t *testing.T, output []byte) string {
	t.Helper()
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "output.json")
	if err := os.WriteFile(outputPath, output, 0o600); err != nil {
		t.Fatal(err)
	}
	binaryPath := filepath.Join(dir, "ffprobe")
	script := []byte("#!/bin/sh\n/bin/cat >/dev/null\nexec /bin/cat \"$(dirname \"$0\")/output.json\"\n")
	if err := os.WriteFile(binaryPath, script, 0o700); err != nil {
		t.Fatal(err)
	}
	return binaryPath
}

func testPMTPacket(pmtPID, videoPID int, streamType byte) []byte {
	return testPMTPacketForProgram(1, pmtPID, videoPID, streamType)
}

func testPMTPacketForProgram(programNumber, pmtPID, videoPID int, streamType byte) []byte {
	return testPMTPacketForProgramVersion(0, programNumber, pmtPID, videoPID, videoPID, streamType, 0)
}

func testPMTPacketForProgramVersion(
	continuity byte,
	programNumber, pmtPID, videoPID, pcrPID int,
	streamType, version byte,
) []byte {
	section := testPMTSectionForProgramVersion(programNumber, videoPID, pcrPID, streamType, version)
	payload := append([]byte{0}, section...)
	return testExactPayloadPacket(pmtPID, true, continuity, payload)
}

func testPMTSection(videoPID int, streamType byte) []byte {
	return testPMTSectionForProgram(1, videoPID, streamType)
}

func testPMTSectionForProgram(programNumber, videoPID int, streamType byte) []byte {
	return testPMTSectionForProgramVersion(programNumber, videoPID, videoPID, streamType, 0)
}

func testPMTSectionForProgramVersion(
	programNumber, videoPID, pcrPID int,
	streamType, version byte,
) []byte {
	section := make([]byte, 21)
	section[0] = 0x02
	section[1], section[2] = 0xb0, 18
	section[3], section[4] = byte(programNumber>>8), byte(programNumber)
	section[5] = 0xc1 | (version&0x1f)<<1
	section[8], section[9] = 0xe0|byte(pcrPID>>8), byte(pcrPID)
	section[12] = streamType
	section[13], section[14] = 0xe0|byte(videoPID>>8), byte(videoPID)
	section[15], section[16] = 0xf0, 0
	writeMPEG2CRC(section)
	return section
}

func testPESHeader() []byte {
	return []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0}
}

func h264ConfigurationNALs() []byte {
	return []byte{
		0, 0, 1, 0x67, 0x42, 0, 0x1e,
		0, 0, 1, 0x68, 0xce, 0x06, 0xe2,
	}
}

func testPESPacket(pid int, continuity byte, elementary []byte) []byte {
	payload := append(testPESHeader(), elementary...)
	return testExactPayloadPacket(pid, true, continuity, payload)
}

func testExactPayloadPacket(pid int, payloadStart bool, continuity byte, payload []byte) []byte {
	if len(payload) > 182 {
		panic("test TS payload exceeds adaptation-backed packet capacity")
	}
	packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
	packet[0] = 0x47
	packet[1] = byte(pid>>8) & 0x1f
	if payloadStart {
		packet[1] |= 0x40
	}
	packet[2] = byte(pid)
	packet[3] = 0x30 | continuity&0x0f
	adaptationLength := 183 - len(payload)
	packet[4] = byte(adaptationLength)
	if adaptationLength > 0 {
		packet[5] = 0
	}
	copy(packet[5+adaptationLength:], payload)
	return packet
}

func testAdaptationPayloadPacket(pid int, payloadStart bool, continuity byte, adaptation, payload []byte) []byte {
	if len(adaptation)+len(payload) > 182 {
		panic("test TS adaptation+payload exceeds packet capacity")
	}
	packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
	packet[0] = 0x47
	packet[1] = byte(pid>>8) & 0x1f
	if payloadStart {
		packet[1] |= 0x40
	}
	packet[2] = byte(pid)
	packet[3] = 0x30 | continuity&0x0f
	packet[4] = byte(len(adaptation))
	copy(packet[5:], adaptation)
	copy(packet[5+len(adaptation):], payload)
	return packet
}

func testPSISectionPackets(pid int, continuity byte, pointerBytes, section []byte) [][]byte {
	payload := append([]byte{byte(len(pointerBytes))}, pointerBytes...)
	payload = append(payload, section...)
	var packets [][]byte
	for len(payload) > 0 {
		take := min(184, len(payload))
		packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
		packet[0] = 0x47
		packet[1] = byte(pid>>8) & 0x1f
		if len(packets) == 0 {
			packet[1] |= 0x40
		}
		packet[2] = byte(pid)
		packet[3] = 0x10 | continuity&0x0f
		copy(packet[4:], payload[:take])
		packets = append(packets, packet)
		payload = payload[take:]
		continuity = (continuity + 1) & 0x0f
	}
	return packets
}

func addPMTProgramDescriptor(t *testing.T, section []byte, payloadLength int) []byte {
	t.Helper()
	if payloadLength < 0 || payloadLength > 255 || len(section) < 16 {
		t.Fatalf("invalid PMT descriptor fixture: section=%d payload=%d", len(section), payloadLength)
	}
	sectionLength := int(section[1]&0x0f)<<8 | int(section[2])
	if total := 3 + sectionLength; total != len(section) {
		t.Fatalf("PMT section bytes=%d, header declares %d", len(section), total)
	}
	programInfoLength := int(section[10]&0x0f)<<8 | int(section[11])
	streamStart := 12 + programInfoLength
	crcStart := len(section) - 4
	if streamStart > crcStart {
		t.Fatalf("PMT program descriptors end at %d beyond CRC at %d", streamStart, crcStart)
	}
	descriptor := append([]byte{0x80, byte(payloadLength)}, bytes.Repeat([]byte{0x5a}, payloadLength)...)
	expanded := make([]byte, 0, len(section)+len(descriptor))
	expanded = append(expanded, section[:streamStart]...)
	expanded = append(expanded, descriptor...)
	expanded = append(expanded, section[streamStart:crcStart]...)
	expanded = append(expanded, make([]byte, 4)...)
	newProgramInfoLength := programInfoLength + len(descriptor)
	expanded[10] = (expanded[10] & 0xf0) | byte(newProgramInfoLength>>8)&0x0f
	expanded[11] = byte(newProgramInfoLength)
	newSectionLength := len(expanded) - 3
	expanded[1] = (expanded[1] & 0xf0) | byte(newSectionLength>>8)&0x0f
	expanded[2] = byte(newSectionLength)
	writeMPEG2CRC(expanded)
	return expanded
}

func expandPMTSectionsForTest(t *testing.T, media []byte) ([]byte, int) {
	t.Helper()
	pmtPID := -1
	var pmtSection []byte
	for pos := 0; pos+tsPacketSize <= len(media); pos += tsPacketSize {
		packet := media[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid == 0 && pmtPID < 0 {
			if parsed, ok := parsePATPMTPID(packet); ok {
				pmtPID = parsed
			}
			continue
		}
		if pmtPID < 0 || pid != pmtPID || packet[1]&0x40 == 0 {
			continue
		}
		payload, ok := tsPayload(packet)
		if !ok || len(payload) == 0 {
			continue
		}
		pointer := int(payload[0])
		if 1+pointer+3 > len(payload) {
			continue
		}
		candidate := payload[1+pointer:]
		sectionLength := int(candidate[1]&0x0f)<<8 | int(candidate[2])
		total := 3 + sectionLength
		if total <= len(candidate) {
			pmtSection = append([]byte(nil), candidate[:total]...)
			break
		}
	}
	if pmtPID < 0 || len(pmtSection) == 0 {
		t.Fatalf("could not extract single-packet PMT from fixture (pid=%d section=%d)", pmtPID, len(pmtSection))
	}
	expanded := addPMTProgramDescriptor(t, pmtSection, 180)
	if len(expanded) <= 183 {
		t.Fatalf("expanded PMT=%d bytes, want more than first-packet capacity", len(expanded))
	}
	out := make([]byte, 0, len(media)+len(media)/100)
	continuity := byte(0)
	replacements := 0
	for pos := 0; pos+tsPacketSize <= len(media); pos += tsPacketSize {
		packet := media[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid != pmtPID {
			out = append(out, packet...)
			continue
		}
		if packet[1]&0x40 == 0 {
			continue
		}
		if replacements == 0 {
			continuity = packet[3] & 0x0f
		}
		packets := testPSISectionPackets(pmtPID, continuity, nil, expanded)
		for _, replacement := range packets {
			out = append(out, replacement...)
		}
		continuity = (continuity + byte(len(packets))) & 0x0f
		replacements++
	}
	return out, replacements
}

func writeMPEG2CRC(section []byte) {
	if len(section) < 4 {
		return
	}
	crc := uint32(0xffffffff)
	for _, value := range section[:len(section)-4] {
		crc ^= uint32(value) << 24
		for bit := 0; bit < 8; bit++ {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	binary.BigEndian.PutUint32(section[len(section)-4:], crc)
}

func testAdaptationOnlyDiscontinuity(pid int, continuity byte) []byte {
	packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
	packet[0] = 0x47
	packet[1] = byte(pid>>8) & 0x1f
	packet[2] = byte(pid)
	packet[3] = 0x20 | continuity&0x0f
	packet[4] = 183
	packet[5] = 0x80
	return packet
}
