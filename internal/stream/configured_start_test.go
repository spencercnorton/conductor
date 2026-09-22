package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestLateSubscriberReleasesConfiguredVideoFromFirstSlice(t *testing.T) {
	ffmpeg, ffprobe := requireLateJoinFFmpeg(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	media := generateConfiguredStartMedia(t, ctx, ffmpeg)
	// Start at a later PAT in the first GOP, retaining its PMT and subsequent
	// predicted frames but omitting that GOP's configuration and IDR.
	start := -1
	patCount := 0
	for pos := tsPacketSize; pos+tsPacketSize <= len(media); pos += tsPacketSize {
		p := media[pos : pos+tsPacketSize]
		if int(p[1]&0x1f)<<8|int(p[2]) == 0 {
			patCount++
			if patCount < 2 {
				continue
			}
			start = pos
			break
		}
	}
	if start < 0 {
		t.Fatal("fixture lacks repeated PAT")
	}
	tail := media[start:]
	if firstConfiguredSliceForTest(t, tail) {
		t.Fatal("fixture does not reproduce an unconfigured ring tail")
	}
	validator := &ffprobeMediaPrefixValidator{ffprobeBinary: ffprobe, available: true}
	healthy, err := validator.Validate(ctx, media)
	if err != nil || healthy.kind != prefixReady || len(healthy.prepend) != 0 {
		t.Fatalf("healthy prefix changed: kind=%v prepend=%d err=%v", healthy.kind, len(healthy.prepend), err)
	}
	firstPAT := -1
	for pos := 0; pos+tsPacketSize <= len(media); pos += tsPacketSize {
		if int(media[pos+1]&31)<<8|int(media[pos+2]) == 0 {
			firstPAT = pos
			break
		}
	}
	if healthy.start != firstPAT {
		t.Fatalf("healthy start changed: %d want%d", healthy.start, firstPAT)
	}
	pump := &Streamer{}
	got, err := validateLateSubscriberStart(ctx, pump, startupChunk{data: tail, epoch: pump.MediaEpoch(), realAt: time.Now()},
		make(chan startupChunk), make(chan struct{}), &ffprobeMediaPrefixValidator{ffprobeBinary: ffprobe, available: true})
	if err != nil {
		t.Fatal(err)
	}
	if !firstConfiguredSliceForTest(t, got.data) {
		t.Error("late join released selected VCL before configured random access")
	}
	decode := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-")
	decode.Stdin = bytes.NewReader(got.data)
	diagnostics, err := decode.CombinedOutput()
	if err != nil || len(diagnostics) != 0 {
		t.Errorf("released prefix decode: %v\n%s", err, diagnostics)
	}

	count := exec.CommandContext(ctx, ffprobe, "-v", "error", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames", "-of", "json", "pipe:0")
	count.Stdin = bytes.NewReader(got.data)
	counted, err := count.Output()
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
			Frames    string `json:"nb_read_frames"`
		}
	}
	if err := json.Unmarshal(counted, &report); err != nil {
		t.Fatal(err)
	}
	videoFrames, audioFrames := 0, 0
	for _, stream := range report.Streams {
		n, _ := strconv.Atoi(stream.Frames)
		if stream.CodecType == "video" {
			videoFrames = n
		}
		if stream.CodecType == "audio" {
			audioFrames = n
		}
	}
	if videoFrames != 100 || audioFrames < 180 {
		t.Fatalf("retained decoded frames video=%d audio=%d, want all100 video and matching audio", videoFrames, audioFrames)
	}
	t.Logf("generated late join input=%d release=%d video_frames=%d audio_frames=%d; full A/V decode clean", len(tail), len(got.data), videoFrames, audioFrames)
}

// This fixture inspector deliberately does not use the production RAP scanner.
// Generated PES headers fit their PUSI packet; NAL start codes may span TS packets.
func firstConfiguredSliceForTest(t *testing.T, media []byte) bool {
	t.Helper()
	pid, ok := videoPIDFromTS(media)
	if !ok {
		t.Fatal("fixture selected video PID missing")
	}
	var es []byte
	for pos := 0; pos+tsPacketSize <= len(media); pos += tsPacketSize {
		p := media[pos : pos+tsPacketSize]
		if int(p[1]&0x1f)<<8|int(p[2]) != pid {
			continue
		}
		payload, ok := tsPayload(p)
		if !ok {
			continue
		}
		if p[1]&0x40 != 0 {
			if len(payload) < 9 || !bytes.Equal(payload[:3], []byte{0, 0, 1}) || 9+int(payload[8]) > len(payload) {
				t.Fatal("fixture PES header is not in first packet")
			}
			payload = payload[9+int(payload[8]):]
		}
		es = append(es, payload...)
	}
	sps, pps := false, false
	for i := 0; i+3 < len(es); i++ {
		if es[i] != 0 || es[i+1] != 0 || es[i+2] != 1 {
			continue
		}
		typ := es[i+3] & 31
		if typ == 7 {
			sps = true
		}
		if typ == 8 {
			pps = true
		}
		if typ >= 1 && typ <= 5 {
			return typ == 5 && sps && pps
		}
	}
	t.Fatal("fixture lacks selected VCL")
	return false
}

func TestConfiguredStartupRetainsCrossingAudioPESAndPacketOrder(t *testing.T) {
	const video, audio, pmt = 256, 257, 4096
	a := publicationTestHeader(400)
	// Split the audio PES header itself before configuration; dropping just the
	// packet adjacent to the video cut would destroy this complete audio unit.
	audioPackets := [][]byte{
		publicationTestPacket(audio, true, a[:2], 0),
		publicationTestPacket(audio, false, a[2:186], 1),
		publicationTestPacket(audio, false, a[186:370], 2),
		publicationTestPacket(audio, false, a[370:], 3),
	}
	stale := testPESPacket(video, 0, []byte{0, 0, 1, 0x61, 0x88})
	config := testPESPacket(video, 1, h264ConfigurationNALs())
	rap := testPESPacket(video, 2, []byte{0, 0, 1, 0x65, 0x88})
	data := joinTSPackets(testPATPacket(pmt), testAVPMTPacketWithPCR(pmt, video, audio, video), stale, audioPackets[0], audioPackets[1], config, audioPackets[2], rap, audioPackets[3], testPESPacket(video, 3, []byte{0, 0, 1, 0x61, 0x88}))
	configuration, _, _ := findConfiguredRandomAccessNALProgram(data, "h264", video)
	program, ok := findSafeTSProgramPrefix(data, configuration, "h264", video)
	if !ok {
		t.Fatal("program missing")
	}
	got, ok := trimConfiguredStartupPrefix(data, program, configuration)
	if !ok {
		t.Fatal("complete crossing audio refused")
	}
	if bytes.Contains(got, stale) {
		t.Fatal("predicted video retained")
	}
	if !firstProgramVCLIsConfiguredRandomAccess(got, "h264", video) {
		t.Fatal("configuration in earlier PES lost")
	}
	want := joinTSPackets(audioPackets[0], audioPackets[1], config, audioPackets[2], rap, audioPackets[3], testPESPacket(video, 3, []byte{0, 0, 1, 0x61, 0x88}))
	if !bytes.Equal(got[2*tsPacketSize:], want) {
		t.Fatal("retained selected packets changed or reordered")
	}
	var gate pesPublicationGate
	gate.configure(avProgramPIDs{videoPID: video, audioPIDs: [maxAVTimelineAudioPIDs]int{audio}, audioCount: 1}, true)
	if _, err := gate.push(got); err != nil {
		t.Fatalf("retained PES is incomplete: %v", err)
	}
}

func TestConfiguredStartupRejectsPredictedSliceInConfigurationPES(t *testing.T) {
	for _, beforeConfig := range []bool{true, false} {
		config := h264ConfigurationNALs()
		predicted := []byte{0, 0, 1, 0x61, 0x88}
		if beforeConfig {
			config = append(predicted, config...)
		} else {
			config = append(config, predicted...)
		}
		data := joinTSPackets(testPATPacket(4096), testPMTPacket(4096, 256, 0x1b), testPESPacket(256, 0, config), testPESPacket(256, 1, []byte{0, 0, 1, 0x65, 0x88}))
		at, _, _ := findConfiguredRandomAccessNALProgram(data, "h264", 256)
		program, ok := findSafeTSProgramPrefix(data, at, "h264", 256)
		if !ok {
			t.Fatal("program missing")
		}
		got, ok := trimConfiguredStartupPrefix(data, program, at)
		if ok && firstProgramVCLIsConfiguredRandomAccess(got, "h264", 256) {
			t.Fatal("configuration PES containing a predicted first slice became releasable")
		}
	}
}

func TestConfiguredStartupCannotBorrowConfigurationAcrossFence(t *testing.T) {
	data := joinTSPackets(testPATPacket(4096), testPMTPacket(4096, 256, 0x1b), testPESPacket(256, 0, h264ConfigurationNALs()), testAdaptationOnlyDiscontinuity(256, 0), testPESPacket(256, 1, []byte{0, 0, 1, 0x65, 0x88}))
	if firstProgramVCLIsConfiguredRandomAccess(data, "h264", 256) {
		t.Fatal("configuration crossed shared transport fence")
	}
}

func TestConfiguredStartupAudioHeadProof(t *testing.T) {
	for _, name := range []string{"adaptation-only PUSI is not a PES start", "invalid PES head", "incomplete declared head", "continuation without head"} {
		t.Run(name, func(t *testing.T) {
			const v, a, pmt = 256, 257, 4096
			pes := publicationTestHeader(300)
			prefix := [][]byte{testPATPacket(pmt), testAVPMTPacketWithPCR(pmt, v, a, v), testPESPacket(v, 0, []byte{0, 0, 1, 0x61, 0x88})}
			switch name {
			case "adaptation-only PUSI is not a PES start":
				p := testAdaptationOnlyDiscontinuity(a, 0)
				p[1] |= 0x40
				p[5] = 0 // no epoch boundary, no payload
				prefix = append(prefix, publicationTestPacket(a, true, pes[:184], 0), p)
			case "invalid PES head":
				prefix = append(prefix, publicationTestPacket(a, true, []byte{0xde, 0xad, 0xbe, 0xef, 0, 0}, 0))
			case "incomplete declared head":
				prefix = append(prefix, publicationTestPacket(a, true, pes[:184], 0))
			}
			prefix = append(prefix, testPESPacket(v, 1, h264ConfigurationNALs()))
			if name == "adaptation-only PUSI is not a PES start" {
				prefix = append(prefix, publicationTestPacket(a, false, pes[184:], 1))
			}
			if name == "continuation without head" {
				prefix = append(prefix, publicationTestPacket(a, false, pes[184:], 1))
			}
			prefix = append(prefix, testPESPacket(v, 2, []byte{0, 0, 1, 0x65, 0x88}), testPESPacket(v, 3, []byte{0, 0, 1, 0x61, 0x88}))
			data := joinTSPackets(prefix...)
			at, _, _ := findConfiguredRandomAccessNALProgram(data, "h264", v)
			program, ok := findSafeTSProgramPrefix(data, at, "h264", v)
			if !ok {
				t.Fatal("program missing")
			}
			_, ok = trimConfiguredStartupPrefix(data, program, at)
			want := name == "adaptation-only PUSI is not a PES start"
			if ok != want {
				t.Fatalf("audio head accepted=%v want=%v", ok, want)
			}
		})
	}
}

func TestConfiguredStartupRetainsPartialFinalPacket(t *testing.T) {
	data := joinTSPackets(testPATPacket(4096), testPMTPacket(4096, 256, 0x1b), testPESPacket(256, 0, []byte{0, 0, 1, 0x61, 0x88}), testPESPacket(256, 1, h264ConfigurationNALs()), testPESPacket(256, 2, []byte{0, 0, 1, 0x65, 0x88}), testPESPacket(256, 3, []byte{0, 0, 1, 0x61, 0x88}))
	at, _, _ := findConfiguredRandomAccessNALProgram(data, "h264", 256)
	program, ok := findSafeTSProgramPrefix(data, at, "h264", 256)
	if !ok {
		t.Fatal("program missing")
	}
	base, ok := trimConfiguredStartupPrefix(data, program, at)
	if !ok {
		t.Fatal("base refused")
	}
	next := testPESPacket(256, 4, []byte{0, 0, 1, 0x61, 0x99})
	for n := 1; n < tsPacketSize; n++ {
		candidate := append(append([]byte(nil), data...), next[:n]...)
		got, ready := trimConfiguredStartupPrefix(candidate, program, at)
		if n < 4 {
			if ready {
				t.Fatalf("unidentifiable %d-byte packet header released", n)
			}
			continue
		}
		if !ready || !bytes.Equal(got, append(append([]byte(nil), base...), next[:n]...)) {
			t.Fatalf("partial packet %d not preserved exactly (ready=%v)", n, ready)
		}
	}
}

func TestConfiguredStartupPIDReuseRequiresCurrentConfiguration(t *testing.T) {
	data := joinTSPackets(testPATPacket(4096), testPMTPacketForProgramVersion(0, 1, 4096, 256, 256, 0x1b, 0), testPESPacket(256, 0, h264ConfigurationNALs()), testPESPacket(256, 1, []byte{0, 0, 1, 0x65, 0x88}), testPMTPacketForProgramVersion(1, 1, 4096, 256, 258, 0x1b, 1), testPESPacket(256, 2, []byte{0, 0, 1, 0x65, 0x88}))
	if firstProgramVCLIsConfiguredRandomAccess(data, "h264", 256) {
		t.Fatal("reused PID borrowed obsolete PMT configuration")
	}
	data = append(data, joinTSPackets(testAdaptationOnlyDiscontinuity(256, 2), testPESPacket(256, 3, h264ConfigurationNALs()), testPESPacket(256, 4, []byte{0, 0, 1, 0x65, 0x88}))...)
	at, _, _ := findConfiguredRandomAccessNALProgram(data, "h264", 256)
	program, ok := findSafeTSProgramPrefix(data, at, "h264", 256)
	if !ok {
		t.Fatal("fresh configuration missing")
	}
	release := append(append([]byte(nil), program.psiPrefix...), data[program.safeStart:]...)
	if !firstProgramVCLIsConfiguredRandomAccess(release, "h264", 256) {
		t.Fatal("fresh fenced configuration refused")
	}
}

func TestConfiguredStartupHEVCFirstVCL(t *testing.T) {
	config := []byte{0, 0, 1, 32 << 1, 1, 0x80, 0, 0, 1, 33 << 1, 1, 0x80, 0, 0, 1, 34 << 1, 1, 0x80}
	rap := []byte{0, 0, 1, 19 << 1, 1, 0x80}
	predicted := []byte{0, 0, 1, 1 << 1, 1, 0x80}
	for _, leadingPredicted := range []bool{false, true} {
		packets := [][]byte{testPATPacket(4096), testPMTPacket(4096, 256, 0x24)}
		cc := byte(0)
		if leadingPredicted {
			packets = append(packets, testPESPacket(256, cc, predicted))
			cc++
		}
		packets = append(packets, testPESPacket(256, cc, config), testPESPacket(256, cc+1, rap), testPESPacket(256, cc+2, predicted))
		data := joinTSPackets(packets...)
		if got := firstProgramVCLIsConfiguredRandomAccess(data, "hevc", 256); got == leadingPredicted {
			t.Fatalf("HEVC first VCL ready=%v predicted=%v", got, leadingPredicted)
		}
		at, _, _ := findConfiguredRandomAccessNALProgram(data, "hevc", 256)
		program, ok := findSafeTSProgramPrefix(data, at, "hevc", 256)
		if !ok {
			t.Fatal("HEVC program missing")
		}
		got, ok := trimConfiguredStartupPrefix(data, program, at)
		if !ok || !firstProgramVCLIsConfiguredRandomAccess(got, "hevc", 256) {
			t.Fatal("configured HEVC IRAP not retained")
		}
	}
}

func generateConfiguredStartMedia(t *testing.T, ctx context.Context, ffmpeg string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=6",
		"-f", "lavfi", "-i", "sine=sample_rate=48000:duration=6",
		"-c:v", "libx264", "-threads", "2", "-preset", "ultrafast", "-tune", "zerolatency",
		"-g", "50", "-keyint_min", "50", "-sc_threshold", "0", "-b:v", "2M",
		"-c:a", "aac", "-b:a", "128k", "-f", "mpegts", "pipe:1")
	media, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}

	return media
}

func TestConfiguredStartupRejectsMalformedFirstNominalIDR(t *testing.T) {
	ffmpeg, ffprobe := requireLateJoinFFmpeg(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	media := generateConfiguredStartMedia(t, ctx, ffmpeg)
	validator := &ffprobeMediaPrefixValidator{ffprobeBinary: ffprobe, available: true}
	pid, ok := videoPIDFromTS(media)
	if !ok {
		t.Fatal("PID missing")
	}
	_, rap, _ := findConfiguredRandomAccessNALProgram(media, "h264", pid)
	clean, reason, err := validator.probe(ctx, media)
	if err != nil || reason != "" {
		t.Fatalf("clean probe: %s %v", reason, err)
	}
	if len(clean.Frames) == 0 || clean.Frames[0].KeyFrame != 1 || clean.Frames[0].PacketPosition != strconv.Itoa(rap) {
		t.Fatalf("first real decoded frame not bound to RAP PES %d", rap)
	}
	t.Logf("real first decoded keyframe pkt_pos=%s equals configured RAP PES=%d", clean.Frames[0].PacketPosition, rap)
	broken := append([]byte(nil), media...)
	changed := 0
	for pos := rap; pos+tsPacketSize <= len(broken); pos += tsPacketSize {
		p := broken[pos : pos+tsPacketSize]
		if int(p[1]&31)<<8|int(p[2]) != pid {
			continue
		}
		if pos > rap && p[1]&0x40 != 0 {
			break
		}
		payload, ok := tsPayload(p)
		if !ok {
			continue
		}
		at := bytes.Index(payload, []byte{0, 0, 1, 0x65})
		if at < 0 {
			continue
		}
		rbsp := payload[at+4:]
		// Mutate every slice of the first IDR picture, not just its first
		// slice: a decoder may conceal a missing slice of an otherwise decoded
		// keyframe. Skip the first two Exp-Golomb values to locate PPS0.
		bit := 0
		for field := 0; field < 2; field++ {
			zeros := 0
			for bit < len(rbsp)*8 && rbsp[bit/8]&(1<<uint(7-bit%8)) == 0 {
				zeros++
				bit++
			}
			bit += 1 + zeros
		}
		if bit+2 >= len(rbsp)*8 || rbsp[bit/8]&(1<<uint(7-bit%8)) == 0 {
			t.Fatal("generated PPS0 not located")
		}
		for j, value := range []byte{0, 1, 0} {
			at := bit + j
			mask := byte(1 << uint(7-at%8))
			rbsp[at/8] &= ^mask
			if value != 0 {
				rbsp[at/8] |= mask
			}
		}
		changed++
	}
	if changed == 0 {
		t.Fatal("first IDR not mutated")
	}
	t.Logf("mutated all%d slices in first IDR PES", changed)
	probe, reason, err := validator.probe(ctx, broken)
	if err != nil || reason != "" {
		t.Fatalf("broken probe: %s %v", reason, err)
	}
	laterKey := false
	for _, f := range probe.Frames {
		p, _ := strconv.Atoi(f.PacketPosition)
		if f.KeyFrame == 1 && p > rap {
			laterKey = true
		}
	}
	if !laterKey {
		t.Fatal("fixture lacks later independently decoded good IDR")
	}
	got, err := validator.Validate(ctx, broken)
	if err != nil || got.kind != prefixNeedMore {
		t.Fatalf("malformed first nominal IDR admitted: kind=%v reason=%s err=%v", got.kind, got.reason, err)
	}
}
