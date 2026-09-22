package dvr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type triggeredArtifactContext struct {
	context.Context
	done chan struct{}
	err  error
}

func newTriggeredArtifactContext(err error) *triggeredArtifactContext {
	return &triggeredArtifactContext{
		Context: context.Background(),
		done:    make(chan struct{}),
		err:     err,
	}
}

func (c *triggeredArtifactContext) Done() <-chan struct{} { return c.done }

func (c *triggeredArtifactContext) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}

func (c *triggeredArtifactContext) trigger() { close(c.done) }

func TestNormalizeAndValidateRepairsMPEGTSTimestampReset(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	segment := filepath.Join(dir, "segment.ts")
	makeArtifactAVSegment(t, ffmpeg, segment, 2*time.Second, artifactVideoTestSrc, artifactAudioTone)
	input := filepath.Join(dir, "reset.ts")
	concatenateArtifactFiles(t, input, segment, segment)

	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderSampling = false
	rawVideo, err := processor.probeTimeline(context.Background(), input, "v:0")
	if err != nil {
		t.Fatal(err)
	}
	if rawVideo.discontinuities == 0 {
		t.Fatal("fixture has no timestamp reset")
	}

	output := filepath.Join(dir, "normalized.ts")
	report, err := processor.NormalizeAndValidate(context.Background(), input, output, 4*time.Second)
	if err != nil {
		t.Fatalf("normalize reset MPEG-TS: %v", err)
	}
	if report.Discontinuities != 0 {
		t.Fatalf("normalized report=%+v, want no discontinuities", report)
	}
	if report.Duration < 3800*time.Millisecond || report.Duration > 4400*time.Millisecond {
		t.Fatalf("normalized duration=%s, want about four seconds", report.Duration)
	}
	if report.AVDrift > 300*time.Millisecond || report.SHA256 == "" || report.ArtifactBytes <= 0 {
		t.Fatalf("normalized evidence incomplete: %+v", report)
	}
}

// corruptVideoSlicePayloads damages H.264 slice data in the middle third of a
// fixture the way a mid-GOP splice does: only video-PID continuation packets
// (no PUSI, so no PES header) have their payload tails flipped, leaving every
// timestamp, table, and continuity counter untouched. The result is exactly
// the artifact class the packet-clock validator cannot see.
func corruptVideoSlicePayloads(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// ffmpeg's mpegts muxer assigns the first stream PID 0x100.
	const videoPID = 0x100
	corrupted := 0
	// len(data)/3 is packet-aligned only when the packet count divides by
	// three; snap to the boundary so the sync-byte guard proves damage, not
	// fixture length.
	start := len(data) / 3 / artifactTSPacketSize * artifactTSPacketSize
	for offset := start; offset+artifactTSPacketSize <= 2*len(data)/3; offset += artifactTSPacketSize {
		packet := data[offset : offset+artifactTSPacketSize]
		if packet[0] != 0x47 {
			t.Fatalf("fixture lost TS sync at offset %d", offset)
		}
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		pusi := packet[1]&0x40 != 0
		if pid != videoPID || pusi {
			continue
		}
		for i := 50; i < artifactTSPacketSize; i++ {
			packet[i] ^= 0xff
		}
		corrupted++
	}
	if corrupted < 20 {
		t.Fatalf("only corrupted %d video continuation packets; fixture or PID assumption broke", corrupted)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeAndValidateRejectsCorruptVideoSlices(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	clean := filepath.Join(dir, "clean.ts")
	makeArtifactAVSegment(t, ffmpeg, clean, 12*time.Second, artifactVideoTestSrc, artifactAudioTone)

	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderSampling = false

	// Control: the identical capture without damage passes with a clean scan,
	// so the rejection below can only come from the injected corruption.
	cleanOut := filepath.Join(dir, "clean-normalized.ts")
	report, err := processor.NormalizeAndValidate(context.Background(), clean, cleanOut, 12*time.Second)
	if err != nil {
		t.Fatalf("clean fixture rejected: %v", err)
	}
	if report.BitstreamErrors != 0 {
		t.Fatalf("clean fixture scan reported %d corruption events, want 0", report.BitstreamErrors)
	}

	corrupt := filepath.Join(dir, "corrupt.ts")
	if err := os.WriteFile(corrupt, mustReadArtifactFile(t, clean), 0o644); err != nil {
		t.Fatal(err)
	}
	corruptVideoSlicePayloads(t, corrupt)

	corruptOut := filepath.Join(dir, "corrupt-normalized.ts")
	report, err = processor.NormalizeAndValidate(context.Background(), corrupt, corruptOut, 12*time.Second)
	if err == nil {
		t.Fatal("corrupt video slices were published")
	}
	if !strings.Contains(err.Error(), "bitstream is corrupt") {
		t.Fatalf("corrupt fixture rejected for the wrong reason: %v", err)
	}
	if report.BitstreamErrors == 0 {
		t.Fatal("rejection did not report the observed corruption count")
	}

	// The tolerance knob keeps the same damaged artifact when raised, and the
	// negative sentinel skips the scan entirely — the documented escape hatch.
	for _, tc := range []struct {
		name      string
		tolerance int
		wantScan  bool
	}{
		{name: "raised-tolerance", tolerance: 1 << 30, wantScan: true},
		{name: "scan-disabled", tolerance: -1, wantScan: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tolerant := newFFmpegMediaProcessor(ffmpeg)
			tolerant.placeholderSampling = false
			tolerant.scanMaxDecodeErrors = tc.tolerance
			out := filepath.Join(dir, tc.name+".ts")
			report, err := tolerant.NormalizeAndValidate(context.Background(), corrupt, out, 12*time.Second)
			if err != nil {
				t.Fatalf("tolerant processor rejected: %v", err)
			}
			if tc.wantScan && report.BitstreamErrors == 0 {
				t.Fatal("raised tolerance suppressed the corruption count")
			}
			if !tc.wantScan && report.BitstreamErrors != 0 {
				t.Fatalf("disabled scan still reported %d corruption events", report.BitstreamErrors)
			}
		})
	}
}

// A decode that dies part-way proved nothing about the unscanned tail, so the
// observed count comes back WITH an error: under-tolerance partial scans must
// never publish, while an already-over-tolerance count can still carry the
// more informative corruption verdict.
func TestScanVideoBitstreamFailsClosedOnIncompleteDecode(t *testing.T) {
	dir := t.TempDir()
	writeStub := func(name, script string) *ffmpegMediaProcessor {
		t.Helper()
		stub := filepath.Join(dir, name)
		if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		return &ffmpegMediaProcessor{ffmpeg: stub}
	}

	dying := writeStub("ffmpeg-dying",
		"#!/bin/sh\necho '[h264 @ 0x1] error while decoding MB 1 1, bytestream -5' >&2\nexit 1\n")
	count, sample, err := dying.scanVideoBitstream(context.Background(), "unused.ts")
	if err == nil {
		t.Fatal("mid-scan decoder death was treated as a complete scan")
	}
	if count != 1 || !strings.Contains(sample, "error while decoding") {
		t.Fatalf("partial scan lost its observed corruption: count=%d sample=%q", count, sample)
	}

	silent := writeStub("ffmpeg-silent", "#!/bin/sh\nexit 1\n")
	count, _, err = silent.scanVideoBitstream(context.Background(), "unused.ts")
	if err == nil || count != 0 {
		t.Fatalf("silent decoder death: count=%d err=%v, want 0 and an error", count, err)
	}

	clean := writeStub("ffmpeg-clean", "#!/bin/sh\nexit 0\n")
	count, _, err = clean.scanVideoBitstream(context.Background(), "unused.ts")
	if err != nil || count != 0 {
		t.Fatalf("clean decode: count=%d err=%v, want 0 and nil", count, err)
	}
}

func mustReadArtifactFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestNormalizeAndValidateRejectsIncompleteCoverage(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "short.ts")
	makeArtifactAVSegment(t, ffmpeg, input, 2*time.Second, artifactVideoTestSrc, artifactAudioTone)
	output := filepath.Join(dir, "rejected.ts")

	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderSampling = false
	report, err := processor.NormalizeAndValidate(context.Background(), input, output, 10*time.Second)
	if err == nil || !strings.Contains(err.Error(), "covers") {
		t.Fatalf("short coverage report=%+v err=%v, want coverage rejection", report, err)
	}
	if info, statErr := os.Stat(output); statErr != nil || info.Size() == 0 {
		t.Fatalf("rejected candidate was not retained for diagnosis: info=%v err=%v", info, statErr)
	}
}

func TestNormalizeAndValidateRejectsFixedAVStartOffset(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "offset.ts")
	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", artifactVideoTestSrc,
		"-itsoffset", "2.5", "-f", "lavfi", "-i", artifactAudioTone,
		"-t", "6", "-map", "0:v:0", "-map", "1:a:0",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "mpegts", input)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate offset fixture: %v: %s", err, diagnostic)
	}
	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderSampling = false
	report, err := processor.NormalizeAndValidate(
		context.Background(), input, filepath.Join(dir, "normalized.ts"), 3*time.Second)
	if err == nil || report.AVStartDelta <= time.Second {
		t.Fatalf("offset report=%+v err=%v, want bounded-start rejection", report, err)
	}
}

func TestNormalizeAndValidateRepairsOrRejectsProgressiveAudioClockSkew(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "audio-skew.ts")
	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", artifactVideoTestSrc+":duration=8",
		"-f", "lavfi", "-i", artifactAudioTone+":duration=8",
		"-map", "0:v:0", "-map", "1:a:0",
		"-filter:a", "asetpts=PTS*1.25",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "mpegts", input)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate skew fixture: %v: %s", err, diagnostic)
	}

	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderSampling = false
	rawVideo, err := processor.probeTimeline(context.Background(), input, "v:0")
	if err != nil {
		t.Fatal(err)
	}
	rawAudio, err := processor.probeTimeline(context.Background(), input, "a:0")
	if err != nil {
		t.Fatal(err)
	}
	rawStartOffset := rawAudio.first - rawVideo.first
	rawEndOffset := rawAudio.lastEnd - rawVideo.lastEnd
	if absDuration(rawEndOffset-rawStartOffset) < time.Second {
		t.Fatalf("fixture lacks progressive A/V skew: video=%+v audio=%+v", rawVideo, rawAudio)
	}

	report, normalizeErr := processor.NormalizeAndValidate(
		context.Background(), input, filepath.Join(dir, "normalized.ts"), 6*time.Second)
	if normalizeErr == nil && (report.AVStartDelta > time.Second ||
		report.AVEndDelta > 2*time.Second || report.AVDrift > time.Second) {
		t.Fatalf("skewed media was accepted without repair: %+v", report)
	}
	if normalizeErr != nil && !strings.Contains(normalizeErr.Error(), "A/V") &&
		!strings.Contains(normalizeErr.Error(), "covers") {
		t.Fatalf("skew failed for an unrelated reason: report=%+v err=%v", report, normalizeErr)
	}
}

func TestNormalizeAndValidateRejectsUniformBlackSilentPlaceholder(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "black-silent.ts")
	makeArtifactAVSegment(t, ffmpeg, input, 4*time.Second, artifactVideoBlack, artifactAudioSilence)

	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderMin = 3 * time.Second
	processor.placeholderMax = 5 * time.Second
	report, err := processor.NormalizeAndValidate(
		context.Background(), input, filepath.Join(dir, "normalized.ts"), 4*time.Second)
	if err == nil || report.FillerReason == "" || !strings.Contains(err.Error(), "black placeholder") {
		t.Fatalf("placeholder report=%+v err=%v, want high-confidence rejection", report, err)
	}
}

func TestNormalizeAndValidateKeepsNonblackSilentMediaInPlaceholderBand(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "nonblack-silent.ts")
	makeArtifactAVSegment(t, ffmpeg, input, 4*time.Second,
		artifactVideoTestSrc, artifactAudioSilence)

	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderMin = 3 * time.Second
	processor.placeholderMax = 5 * time.Second
	report, err := processor.NormalizeAndValidate(
		context.Background(), input, filepath.Join(dir, "normalized.ts"), 4*time.Second)
	if err != nil {
		t.Fatalf("nonblack silent media rejected: report=%+v err=%v", report, err)
	}
	if report.FillerReason != "" || report.SHA256 == "" {
		t.Fatalf("nonblack silent media lacks accepted-artifact evidence: %+v", report)
	}
}

func TestNormalizeAndValidateClassifierContextTerminationIsTerminal(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "black-silent.ts")
	makeArtifactAVSegment(t, ffmpeg, input, 4*time.Second,
		artifactVideoBlack, artifactAudioSilence)

	for _, termination := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, returned := range []struct {
			name  string
			black bool
		}{
			{name: "returns-nonfiller", black: false},
			{name: "returns-filler", black: true},
		} {
			t.Run(termination.Error()+"/"+returned.name, func(t *testing.T) {
				ctx := newTriggeredArtifactContext(termination)
				processor := newFFmpegMediaProcessor(ffmpeg)
				processor.placeholderMin = 3 * time.Second
				processor.placeholderMax = 5 * time.Second
				processor.placeholderClassifier = func(
					classifierCtx context.Context, _, _ string, _ time.Duration,
				) (bool, string, error) {
					ctx.trigger()
					<-classifierCtx.Done()
					return returned.black, "ordinary_result_after_context_expiry", nil
				}
				report, err := processor.NormalizeAndValidate(ctx, input,
					filepath.Join(dir, "normalized-"+
						strings.ReplaceAll(termination.Error(), " ", "-")+"-"+returned.name+".ts"),
					4*time.Second)
				if !errors.Is(err, termination) ||
					!strings.Contains(err.Error(), "classify normalized placeholder") {
					t.Fatalf("classifier termination report=%+v err=%v, want terminal %v",
						report, err, termination)
				}
				if report.SHA256 != "" || report.FillerReason != "" {
					t.Fatalf("expired classifier result was published or classified: %+v", report)
				}
			})
		}
	}
}

func TestNormalizeAndValidateRejectsExactObservedPlaceholderBackup(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "observed-placeholder.ts")
	generateObservedArtifactPlaceholder(t, ffmpeg, input)
	info, err := os.Stat(input)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != observedArtifactPlaceholderBytes {
		t.Fatalf("observed backup bytes=%d, want %d",
			info.Size(), observedArtifactPlaceholderBytes)
	}

	processor := newFFmpegMediaProcessor(ffmpeg)
	report, err := processor.NormalizeAndValidate(context.Background(), input,
		filepath.Join(dir, "normalized.ts"), 600*time.Second+46*time.Millisecond)
	if err == nil || report.FillerReason == "" ||
		!strings.Contains(err.Error(), "black placeholder") {
		t.Fatalf("exact observed backup report=%+v err=%v, want high-confidence rejection",
			report, err)
	}
}

func TestArtifactClassifierTopologyAndPCMProofBoundaries(t *testing.T) {
	base := artifactMediaTopology{
		video:    []int{3},
		audio:    []artifactAudioStream{{index: 7, channels: 2}},
		complete: true,
	}
	if !artifactMediaTopologySupported(base) {
		t.Fatal("exact one-video/one-audio topology was not supported")
	}
	fourAudio := base
	fourAudio.audio = []artifactAudioStream{
		{index: 7, channels: 1}, {index: 8, channels: 2},
		{index: 9, channels: 6}, {index: 10, channels: 8},
	}
	if !artifactMediaTopologySupported(fourAudio) {
		t.Fatal("exactly four bounded audio streams were not supported")
	}
	fiveAudio := fourAudio
	fiveAudio.audio = append(append([]artifactAudioStream(nil), fourAudio.audio...),
		artifactAudioStream{index: 11, channels: 2})

	for _, tt := range []struct {
		name     string
		topology artifactMediaTopology
	}{
		{name: "five audio streams", topology: fiveAudio},
		{name: "missing video", topology: artifactMediaTopology{
			audio: base.audio, complete: true,
		}},
		{name: "negative video index", topology: artifactMediaTopology{
			video: []int{-1}, audio: base.audio, complete: true,
		}},
		{name: "multiple video streams", topology: artifactMediaTopology{
			video: []int{3, 4}, audio: base.audio, complete: true,
		}},
		{name: "unproved subtitle or data", topology: artifactMediaTopology{
			video: base.video, audio: base.audio,
		}},
		{name: "duplicate absolute index", topology: artifactMediaTopology{
			video: []int{3}, audio: []artifactAudioStream{{index: 3, channels: 2}}, complete: true,
		}},
		{name: "negative audio index", topology: artifactMediaTopology{
			video: base.video, audio: []artifactAudioStream{{index: -1, channels: 2}}, complete: true,
		}},
		{name: "zero audio channels", topology: artifactMediaTopology{
			video: base.video, audio: []artifactAudioStream{{index: 7, channels: 0}}, complete: true,
		}},
		{name: "more than eight audio channels", topology: artifactMediaTopology{
			video: base.video, audio: []artifactAudioStream{{index: 7, channels: 9}}, complete: true,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if artifactMediaTopologySupported(tt.topology) {
				t.Fatalf("unproved topology was supported: %+v", tt.topology)
			}
		})
	}

	wantPCM := artifactAudioWindowBytes(2)
	for _, tt := range []struct {
		name string
		size int
		want bool
	}{
		{name: "exact PCM window", size: wantPCM, want: true},
		{name: "short even PCM window", size: wantPCM - 2},
		{name: "short odd PCM window", size: wantPCM - 1},
		{name: "overlong PCM window", size: wantPCM + 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := artifactAudioWindowComplete(make([]byte, tt.size), 2); got != tt.want {
				t.Fatalf("window bytes=%d complete=%v, want %v", tt.size, got, tt.want)
			}
		})
	}
}

func TestArtifactProbeRequiresFullyProvedPublishedTopology(t *testing.T) {
	const (
		validVideo = `{"index":3,"codec_type":"video"}`
		validAudio = `{"index":7,"codec_type":"audio","channels":2}`
	)
	tests := []struct {
		name      string
		streams   string
		supported bool
	}{
		{name: "one absolute video and audio", streams: validVideo + "," + validAudio, supported: true},
		{name: "secondary video", streams: validVideo + `,{"index":4,"codec_type":"video"},` + validAudio},
		{name: "subtitle stream", streams: validVideo + "," + validAudio + `,{"index":8,"codec_type":"subtitle"}`},
		{name: "data stream", streams: validVideo + "," + validAudio + `,{"index":8,"codec_type":"data"}`},
		{name: "unknown stream", streams: validVideo + "," + validAudio + `,{"index":8,"codec_type":""}`},
		{name: "missing index", streams: `{"codec_type":"video"},` + validAudio},
		{name: "duplicate index", streams: validVideo + `,{"index":3,"codec_type":"audio","channels":2}`},
		{name: "missing channels", streams: validVideo + `,{"index":7,"codec_type":"audio"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology, err := parseArtifactMediaTopology(
				[]byte(`{"streams":[` + tt.streams + `]}`))
			if err != nil {
				t.Fatal(err)
			}
			if got := artifactMediaTopologySupported(topology); got != tt.supported {
				t.Fatalf("topology=%+v supported=%v, want %v",
					topology, got, tt.supported)
			}
		})
	}
}

func TestNormalizeAndValidateKeepsLegitimateBlackVideoWithAudio(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "black-with-audio.ts")
	makeArtifactAVSegment(t, ffmpeg, input, 4*time.Second, artifactVideoBlack, artifactAudioTone)

	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderMin = 3 * time.Second
	processor.placeholderMax = 5 * time.Second
	report, err := processor.NormalizeAndValidate(
		context.Background(), input, filepath.Join(dir, "normalized.ts"), 4*time.Second)
	if err != nil {
		t.Fatalf("legitimate dark media rejected: report=%+v err=%v", report, err)
	}
	if report.FillerReason != "" {
		t.Fatalf("legitimate dark media classified as filler: %+v", report)
	}
}

func TestNormalizeAndValidateChecksAudioTracksWithoutMixingChannels(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	if artifactAudioTopologySupported(nil) {
		t.Fatal("missing declared audio must not support a terminal placeholder verdict")
	}
	silence := artifactAudioSilence + ":duration=4"
	tests := []struct {
		name         string
		audioSources []string
	}{
		{
			name: "silent primary with active secondary",
			audioSources: []string{
				silence,
				artifactAudioTone + ":duration=4",
			},
		},
		{
			name: "active antiphase stereo",
			audioSources: []string{
				"aevalsrc=0.5*sin(2*PI*440*t)|-0.5*sin(2*PI*440*t):s=48000:d=4:c=stereo",
			},
		},
		{
			name: "unbounded audio topology fails open",
			audioSources: []string{
				silence, silence, silence, silence, silence,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			input := filepath.Join(dir, "black-audio.ts")
			makeArtifactMultiAudioSegment(t, ffmpeg, input, tt.audioSources)

			processor := newFFmpegMediaProcessor(ffmpeg)
			processor.placeholderMin = 3 * time.Second
			processor.placeholderMax = 5 * time.Second
			report, err := processor.NormalizeAndValidate(
				context.Background(), input, filepath.Join(dir, "normalized.ts"), 4*time.Second)
			if err != nil {
				t.Fatalf("valid or unproven dark media rejected: report=%+v err=%v", report, err)
			}
			if report.FillerReason != "" {
				t.Fatalf("dark media classified as filler: %+v", report)
			}
		})
	}
}

func TestNormalizeAndValidatePublishesOnlyTheValidatedPrimaryAudioTrack(t *testing.T) {
	ffmpeg := requireArtifactFFmpeg(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "two-audio.ts")
	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", artifactVideoTestSrc+":duration=4",
		"-f", "lavfi", "-i", artifactAudioTone+":duration=4",
		"-f", "lavfi", "-i", "sine=frequency=880:sample_rate=48000:duration=4",
		"-map", "0:v:0", "-map", "1:a:0", "-map", "2:a:0",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "mpegts", input)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate multi-audio fixture: %v: %s", err, diagnostic)
	}

	output := filepath.Join(dir, "normalized.ts")
	processor := newFFmpegMediaProcessor(ffmpeg)
	processor.placeholderSampling = false
	if report, err := processor.NormalizeAndValidate(context.Background(), input, output, 4*time.Second); err != nil {
		t.Fatalf("normalize primary audio report=%+v err=%v", report, err)
	}
	probe := exec.Command(processor.ffprobe,
		"-v", "error", "-select_streams", "a", "-show_entries", "stream=index",
		"-of", "json", output)
	streams, err := probe.Output()
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Streams []struct {
			Index int `json:"index"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(streams, &catalog); err != nil {
		t.Fatal(err)
	}
	if got := len(catalog.Streams); got != 1 {
		t.Fatalf("published audio tracks=%d (%q), want exactly validated primary", got, streams)
	}
}

func TestPublishRecordingCandidatePreservesAndRollsBackCurrentArtifact(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "episode.ts")
	candidate := filepath.Join(dir, "candidate.partial")
	predecessor := uuid.New()
	backup := recordingSupersededPath(canonical, predecessor)
	writeArtifactTestFile(t, canonical, "known-good")
	writeArtifactTestFile(t, candidate, "replacement")

	if err := publishRecordingCandidate(canonical, candidate, ""); err == nil {
		t.Fatal("unarmed first publication replaced an existing canonical path")
	}
	assertArtifactTestFile(t, canonical, "known-good")
	assertArtifactTestFile(t, candidate, "replacement")

	if err := publishRecordingCandidate(canonical, candidate, backup); err != nil {
		t.Fatal(err)
	}
	assertArtifactTestFile(t, canonical, "replacement")
	assertArtifactTestFile(t, candidate, "replacement")
	assertArtifactTestFile(t, backup, "known-good")
	canonicalInfo, err := os.Stat(canonical)
	if err != nil {
		t.Fatal(err)
	}
	candidateInfo, err := os.Stat(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(canonicalInfo, candidateInfo) {
		t.Fatal("published canonical does not retain the validated candidate alias")
	}
	if _, err := os.Stat(recordingPublishExchangePath(candidate)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verified exchange alias still exists: %v", err)
	}

	if err := rollbackPublishedCandidate(canonical, candidate, backup); err != nil {
		t.Fatal(err)
	}
	assertArtifactTestFile(t, canonical, "known-good")
	assertArtifactTestFile(t, candidate, "replacement")
	if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rollback backup still exists: %v", err)
	}
}

func TestPublishRecordingCandidatePreservesConcurrentCanonicalReplacement(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "episode.ts")
	candidate := filepath.Join(dir, "candidate.partial")
	foreign := filepath.Join(dir, "foreign.ts")
	backup := filepath.Join(dir, "episode.superseded")
	writeArtifactTestFile(t, canonical, "known-good")
	writeArtifactTestFile(t, candidate, "validated-candidate")
	writeArtifactTestFile(t, foreign, "foreign-replacement")

	err := publishRecordingCandidateAfterBackup(
		canonical, candidate, backup,
		func() error {
			// The hook runs only after the predecessor backup and candidate exchange
			// alias are durable. This is the exact race the former os.Rename would
			// resolve by silently destroying the foreign entry.
			assertArtifactTestFile(t, backup, "known-good")
			return os.Rename(foreign, canonical)
		},
	)
	if !errors.Is(err, errArtifactPublishConflict) ||
		!errors.Is(err, errArtifactPublishRecoveryRequired) {
		t.Fatalf("publish error=%v, want conflict requiring recovery", err)
	}
	assertArtifactTestFile(t, canonical, "foreign-replacement")
	assertArtifactTestFile(t, candidate, "validated-candidate")
	assertArtifactTestFile(t, backup, "known-good")
	if _, statErr := os.Stat(foreign); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("foreign source path survived its own rename: %v", statErr)
	}
	if _, statErr := os.Stat(recordingPublishExchangePath(candidate)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("reversed exchange alias remains: %v", statErr)
	}
}

func TestFirstPublishUsesNoReplaceAndCanRollback(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "first.ts")
	candidate := filepath.Join(dir, "first.partial")
	writeArtifactTestFile(t, candidate, "first-recording")

	if err := publishRecordingCandidate(canonical, candidate, ""); err != nil {
		t.Fatal(err)
	}
	assertArtifactTestFile(t, canonical, "first-recording")
	assertArtifactTestFile(t, candidate, "first-recording")
	if err := rollbackPublishedCandidate(canonical, candidate, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(canonical); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rollback retained first canonical: %v", err)
	}
	assertArtifactTestFile(t, candidate, "first-recording")
}

func TestRecoverInterruptedArtifactPublishIsIdempotentAtEveryFilesystemSeam(t *testing.T) {
	t.Run("first publication after link preserves both names", func(t *testing.T) {
		dir := t.TempDir()
		canonical := filepath.Join(dir, "episode.ts")
		candidate := filepath.Join(dir, "candidate.partial")
		foreign := filepath.Join(dir, "foreign.ts")
		writeArtifactTestFile(t, candidate, "candidate")
		if err := publishRecordingCandidate(canonical, candidate, ""); err != nil {
			t.Fatal(err)
		}
		err := recoverInterruptedArtifactPublish(canonical, candidate, "")
		if !errors.Is(err, errArtifactPublishRecoveryRequired) {
			t.Fatalf("same-file recovery error=%v, want recovery required", err)
		}
		assertArtifactTestFile(t, canonical, "candidate")
		assertArtifactTestFile(t, candidate, "candidate")

		writeArtifactTestFile(t, foreign, "foreign-canonical")
		if err := os.Rename(foreign, canonical); err != nil {
			t.Fatal(err)
		}
		err = recoverInterruptedArtifactPublish(canonical, candidate, "")
		if !errors.Is(err, errArtifactPublishRecoveryRequired) {
			t.Fatalf("changed-path recovery error=%v, want recovery required", err)
		}
		assertArtifactTestFile(t, canonical, "foreign-canonical")
		assertArtifactTestFile(t, candidate, "candidate")
	})

	t.Run("first publication missing candidate preserves foreign canonical", func(t *testing.T) {
		dir := t.TempDir()
		canonical := filepath.Join(dir, "episode.ts")
		candidate := filepath.Join(dir, "candidate.partial")
		writeArtifactTestFile(t, canonical, "foreign-canonical")

		for i := 0; i < 2; i++ {
			err := recoverInterruptedArtifactPublish(canonical, candidate, "")
			if !errors.Is(err, errArtifactPublishRecoveryRequired) {
				t.Fatalf("recovery %d error=%v, want recovery required", i, err)
			}
			assertArtifactTestFile(t, canonical, "foreign-canonical")
			if _, statErr := os.Stat(candidate); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("recovery %d created candidate from unproved canonical: %v", i, statErr)
			}
		}
	})

	t.Run("replacement before predecessor link", func(t *testing.T) {
		dir := t.TempDir()
		canonical := filepath.Join(dir, "episode.ts")
		candidate := filepath.Join(dir, "candidate.partial")
		backup := filepath.Join(dir, "episode.superseded")
		writeArtifactTestFile(t, canonical, "known-good")
		writeArtifactTestFile(t, candidate, "candidate")
		for i := 0; i < 2; i++ {
			if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
				t.Fatalf("recovery %d: %v", i, err)
			}
		}
		assertArtifactTestFile(t, canonical, "known-good")
		assertArtifactTestFile(t, candidate, "candidate")
	})

	t.Run("replacement after predecessor link", func(t *testing.T) {
		dir := t.TempDir()
		canonical := filepath.Join(dir, "episode.ts")
		candidate := filepath.Join(dir, "candidate.partial")
		backup := filepath.Join(dir, "episode.superseded")
		writeArtifactTestFile(t, canonical, "known-good")
		writeArtifactTestFile(t, candidate, "candidate")
		if err := os.Link(canonical, backup); err != nil {
			t.Fatal(err)
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatal(err)
		}
		assertArtifactTestFile(t, canonical, "known-good")
		assertArtifactTestFile(t, candidate, "candidate")
		if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pre-rename backup survived recovery: %v", err)
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatalf("idempotent recovery: %v", err)
		}
	})

	t.Run("replacement after atomic exchange before alias cleanup", func(t *testing.T) {
		dir := t.TempDir()
		canonical := filepath.Join(dir, "episode.ts")
		candidate := filepath.Join(dir, "candidate.partial")
		backup := filepath.Join(dir, "episode.superseded")
		exchange := recordingPublishExchangePath(candidate)
		writeArtifactTestFile(t, canonical, "known-good")
		writeArtifactTestFile(t, candidate, "candidate")
		if err := os.Link(canonical, backup); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(candidate, exchange); err != nil {
			t.Fatal(err)
		}
		if err := atomicExchangeArtifactPaths(exchange, canonical); err != nil {
			t.Fatal(err)
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatal(err)
		}
		assertArtifactTestFile(t, canonical, "known-good")
		assertArtifactTestFile(t, candidate, "candidate")
		for _, path := range []string{backup, exchange} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovery alias %q remains: %v", path, err)
			}
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatalf("idempotent recovery: %v", err)
		}
	})

	t.Run("replacement after rollback exchange before alias cleanup", func(t *testing.T) {
		dir := t.TempDir()
		canonical := filepath.Join(dir, "episode.ts")
		candidate := filepath.Join(dir, "candidate.partial")
		backup := filepath.Join(dir, "episode.superseded")
		writeArtifactTestFile(t, canonical, "candidate")
		if err := os.Link(canonical, candidate); err != nil {
			t.Fatal(err)
		}
		writeArtifactTestFile(t, backup, "known-good")
		if err := atomicExchangeArtifactPaths(canonical, backup); err != nil {
			t.Fatal(err)
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatal(err)
		}
		assertArtifactTestFile(t, canonical, "known-good")
		assertArtifactTestFile(t, candidate, "candidate")
		if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rollback alias remains: %v", err)
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatalf("idempotent recovery: %v", err)
		}
	})

	t.Run("replacement after candidate exchange", func(t *testing.T) {
		dir := t.TempDir()
		canonical := filepath.Join(dir, "episode.ts")
		candidate := filepath.Join(dir, "candidate.partial")
		backup := filepath.Join(dir, "episode.superseded")
		writeArtifactTestFile(t, canonical, "known-good")
		writeArtifactTestFile(t, candidate, "candidate")
		if err := publishRecordingCandidate(canonical, candidate, backup); err != nil {
			t.Fatal(err)
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatal(err)
		}
		assertArtifactTestFile(t, canonical, "known-good")
		assertArtifactTestFile(t, candidate, "candidate")
		if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("post-rename backup survived recovery: %v", err)
		}
		if err := recoverInterruptedArtifactPublish(canonical, candidate, backup); err != nil {
			t.Fatalf("idempotent recovery: %v", err)
		}
	})
}

const (
	artifactVideoTestSrc             = "testsrc2=size=160x90:rate=25"
	artifactVideoBlack               = "color=c=black:size=160x90:rate=25"
	artifactAudioTone                = "sine=frequency=1000:sample_rate=48000"
	artifactAudioSilence             = "anullsrc=channel_layout=stereo:sample_rate=48000"
	artifactTSPacketSize             = 188
	observedArtifactPlaceholderBytes = int64(14_472_616)
)

func requireArtifactFFmpeg(t *testing.T) string {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffmpeg missing from supported CI image")
		}
		t.Skip("ffmpeg is not installed")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffprobe missing from supported CI image")
		}
		t.Skip("ffprobe is not installed")
	}
	return ffmpeg
}

func makeArtifactAVSegment(
	t *testing.T,
	ffmpeg, output string,
	duration time.Duration,
	videoSource, audioSource string,
) {
	t.Helper()
	cmd := exec.Command(ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", videoSource,
		"-f", "lavfi", "-i", audioSource,
		"-t", formatFFmpegSeconds(duration),
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "mpegts", output)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate MPEG-TS fixture: %v: %s", err, diagnostic)
	}
}

func generateObservedArtifactPlaceholder(t *testing.T, ffmpeg, output string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", "color=c=black:size=128x72:rate=5:duration=600.046",
		"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000:duration=600.046",
		"-t", "600.046", "-shortest",
		"-c:v", "libx264", "-preset", "ultrafast",
		"-g", "5", "-keyint_min", "5", "-sc_threshold", "0",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k",
		"-muxrate", "180k", "-f", "mpegts", output,
	)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate observed placeholder: %v: %s", err, diagnostic)
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	remaining := observedArtifactPlaceholderBytes - info.Size()
	if remaining < 0 {
		t.Fatalf("generated placeholder bytes=%d exceed observed %d",
			info.Size(), observedArtifactPlaceholderBytes)
	}
	if remaining%artifactTSPacketSize != 0 {
		t.Fatalf("placeholder padding=%d is not TS-packet aligned", remaining)
	}
	if remaining == 0 {
		return
	}
	nullPacket := make([]byte, artifactTSPacketSize)
	for i := range nullPacket {
		nullPacket[i] = 0xff
	}
	nullPacket[0], nullPacket[1], nullPacket[2], nullPacket[3] = 0x47, 0x1f, 0xff, 0x10
	padding := make([]byte, int(remaining))
	for offset := 0; offset < len(padding); offset += artifactTSPacketSize {
		copy(padding[offset:], nullPacket)
	}
	f, err := os.OpenFile(output, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(padding); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func makeArtifactMultiAudioSegment(
	t *testing.T,
	ffmpeg, output string,
	audioSources []string,
) {
	t.Helper()
	args := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", artifactVideoBlack + ":duration=4",
	}
	for _, source := range audioSources {
		args = append(args, "-f", "lavfi", "-i", source)
	}
	args = append(args, "-t", "4", "-shortest", "-map", "0:v:0")
	for i := range audioSources {
		args = append(args, "-map", strconv.Itoa(i+1)+":a:0")
	}
	args = append(args,
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "mpegts", output,
	)
	if diagnostic, err := exec.Command(ffmpeg, args...).CombinedOutput(); err != nil {
		t.Fatalf("generate multi-audio MPEG-TS fixture: %v: %s", err, diagnostic)
	}
}

func concatenateArtifactFiles(t *testing.T, output string, inputs ...string) {
	t.Helper()
	dst, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range inputs {
		src, err := os.Open(path)
		if err != nil {
			_ = dst.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(dst, src)
		closeErr := src.Close()
		if copyErr != nil || closeErr != nil {
			_ = dst.Close()
			t.Fatalf("concatenate %s: copy=%v close=%v", path, copyErr, closeErr)
		}
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeArtifactTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertArtifactTestFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s=%q, want %q", path, got, want)
	}
}
