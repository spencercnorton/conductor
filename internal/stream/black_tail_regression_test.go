package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const observedBlackPlaceholderBytes = 14_472_616

func TestFiniteClassifierChecksEveryAudioTrackWithoutMixingChannels(t *testing.T) {
	ffmpeg, ffprobe := requireBlackTailFFmpeg(t)
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary: ffmpeg, ffprobeBinary: ffprobe,
	}
	tests := []struct {
		name         string
		audioSources []string
		wantChannels []int
	}{
		{
			name: "silent primary with active secondary",
			audioSources: []string{
				"anullsrc=channel_layout=stereo:sample_rate=48000:duration=8",
				"sine=frequency=440:sample_rate=48000:duration=8",
			},
			wantChannels: []int{2, 1},
		},
		{
			name: "active antiphase stereo",
			audioSources: []string{
				"aevalsrc=0.5*sin(2*PI*440*t)|-0.5*sin(2*PI*440*t):s=48000:d=8:c=stereo",
			},
			wantChannels: []int{2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := generateBlackAudioTopologyTS(t, ffmpeg, tt.name+".ts", tt.audioSources)
			ctx, cancel := context.WithTimeout(context.Background(), defaultSourceStartupTimeout)
			defer cancel()
			probe, err := classifier.probe(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if len(probe.audio) != len(tt.wantChannels) {
				t.Fatalf("probed audio streams=%+v, want channels=%v", probe.audio, tt.wantChannels)
			}
			for i, want := range tt.wantChannels {
				if probe.audio[i].channels != want {
					t.Fatalf("audio stream %d channels=%d, want %d", i, probe.audio[i].channels, want)
				}
			}
			samples, err := classifier.sampleDistributedMedia(
				ctx, path, probe.duration, probe.video[0], probe.audio)
			if err != nil {
				t.Fatal(err)
			}
			// The short fixture keeps this topology regression quick. The exact
			// 600.046-second/14,472,616-byte provider-shaped object is exercised
			// end to end below; only the duration signal is substituted here.
			probe.duration = 600*time.Second + 46*time.Millisecond
			got := classifyDecodedMedia(probe, probe.size, samples)
			if got.kind != classificationFiniteAccepted || got.reason != "decoded_audio_not_silent" {
				t.Fatalf("classification=%s (%s), want active-audio acceptance", got.kind, got.reason)
			}
		})
	}
}

func TestFiniteClassifierFailsOpenOnBlackPrimaryWithActiveSecondaryVideo(t *testing.T) {
	ffmpeg, ffprobe := requireBlackTailFFmpeg(t)
	path := filepath.Join(t.TempDir(), "black-primary-active-secondary.ts")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", "color=c=black:size=160x90:rate=25:duration=8",
		"-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25:duration=8",
		"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000:duration=8",
		"-t", "8", "-shortest",
		"-map", "0:v:0", "-map", "1:v:0", "-map", "2:a:0",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k",
		"-f", "mpegts", path,
	)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate multi-video MPEG-TS: %v: %s", err, diagnostic)
	}
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary: ffmpeg, ffprobeBinary: ffprobe,
	}
	probe, err := classifier.probe(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(probe.video) != 2 || len(probe.audio) != 1 || !probe.topologyComplete {
		t.Fatalf("probed topology=%+v, want two absolute video streams and one audio stream", probe)
	}
	probe.duration = 600*time.Second + 46*time.Millisecond
	probe.size = observedBlackPlaceholderBytes
	got := classifyDecodedMedia(probe, observedBlackPlaceholderBytes,
		silentSamplesForProbe(probe))
	if got.kind != classificationIndeterminate || got.reason != "media_topology_unproven" {
		t.Fatalf("classification=%s (%s), want multi-video fail-open",
			got.kind, got.reason)
	}
}

func TestFiniteClassifierMapsSingleVideoByAbsoluteIndex(t *testing.T) {
	ffmpeg, ffprobe := requireBlackTailFFmpeg(t)
	path := filepath.Join(t.TempDir(), "audio-before-video.ts")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000:duration=8",
		"-f", "lavfi", "-i", "color=c=black:size=160x90:rate=25:duration=8",
		"-t", "8", "-shortest", "-map", "0:a:0", "-map", "1:v:0",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k",
		"-f", "mpegts", path,
	)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate reordered MPEG-TS: %v: %s", err, diagnostic)
	}
	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary: ffmpeg, ffprobeBinary: ffprobe,
	}
	probe, err := classifier.probe(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(probe.video) != 1 || probe.video[0] != 1 || len(probe.audio) != 1 ||
		probe.audio[0].index != 0 || !finiteMediaTopologySupported(probe) {
		t.Fatalf("absolute reordered topology=%+v, want audio 0/video 1", probe)
	}
	samples, err := classifier.sampleDistributedMedia(
		ctx, path, probe.duration, probe.video[0], probe.audio)
	if err != nil {
		t.Fatal(err)
	}
	probe.duration = 600*time.Second + 46*time.Millisecond
	got := classifyDecodedMedia(probe, probe.size, samples)
	if got.kind != classificationBlackPlaceholder {
		t.Fatalf("classification=%s (%s), want absolute-index black/silent proof",
			got.kind, got.reason)
	}
}

// TestFiniteProgrammeThenObservedBlackTailNeverReachesDVRCapture is the
// executable regression for the provider failure observed in production: a
// valid finite MPEG-TS programme ends, relocation returns the finite 600.046s
// all-black/silent object, and that second object's bytes must remain private.
//
// The fixtures are generated in t.TempDir: no multi-megabyte binary is kept in
// git. The placeholder is padded with valid null TS packets to the exact
// observed 14,472,616-byte Content-Length without changing its media clocks.
func TestFiniteProgrammeThenObservedBlackTailNeverReachesDVRCapture(t *testing.T) {
	ffmpeg, ffprobe := requireBlackTailFFmpeg(t)
	programme := generateBlackTailTS(t, ffmpeg, "programme.ts",
		"testsrc2=size=320x180:rate=25:duration=2.400",
		"sine=frequency=1000:sample_rate=48000:duration=2.400",
		"2.400", "5M", "25")
	if got := int64(len(programme)); got < minFinitePreflightBytes || got > maxFinitePreflightBytes {
		t.Fatalf("programme fixture bytes=%d, want finite classifier band %d..%d",
			got, minFinitePreflightBytes, maxFinitePreflightBytes)
	}

	placeholder := generateBlackTailTS(t, ffmpeg, "placeholder.ts",
		"color=c=black:size=128x72:rate=5:duration=600.046",
		"anullsrc=channel_layout=stereo:sample_rate=48000:duration=600.046",
		"600.046", "180k", "5")
	placeholder = padMPEGTSToObservedSize(t, placeholder)

	darkWithAudio := generateBlackTailTS(t, ffmpeg, "dark-with-audio.ts",
		"color=c=black:size=128x72:rate=5:duration=600.046",
		"sine=frequency=440:sample_rate=48000:duration=600.046",
		"600.046", "180k", "5")
	darkWithAudio = padMPEGTSToObservedSize(t, darkWithAudio)

	classifier := &ffmpegFiniteMediaClassifier{
		ffmpegBinary: ffmpeg, ffprobeBinary: ffprobe,
	}
	classify := func(media []byte) classificationResult {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), defaultSourceStartupTimeout)
		defer cancel()
		return classifier.Classify(ctx, media)
	}
	if got := classify(placeholder); got.kind != classificationBlackPlaceholder {
		t.Fatalf("observed placeholder classification=%s (%s), want black placeholder",
			got.kind, got.reason)
	}
	if got := classify(darkWithAudio); got.kind != classificationFiniteAccepted ||
		got.reason != "decoded_audio_not_silent" {
		t.Fatalf("legitimate dark-with-audio classification=%s (%s), want accepted audio veto",
			got.kind, got.reason)
	}

	serveFinite := func(media []byte) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "video/mp2t")
			w.Header().Set("Content-Length", strconv.Itoa(len(media)))
			_, _ = w.Write(media)
		}))
	}
	programmeOrigin := serveFinite(programme)
	defer programmeOrigin.Close()
	placeholderOrigin := serveFinite(placeholder)
	defer placeholderOrigin.Close()

	s := NewStreamer("op476-finite-black-tail", programmeOrigin.URL, testLogger(), nil)
	s.classifier = classifier
	s.sourceStartupTimeout = defaultSourceStartupTimeout
	s.reconnectWindow = 8 * time.Second
	var failovers atomic.Int32
	var programmeLastReal, placeholderLastReal atomic.Int64
	var programmeBytes, placeholderBytes atomic.Int64
	causes := make(chan error, 2)
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		causes <- cause
		switch failovers.Add(1) {
		case 1:
			programmeLastReal.Store(s.LastRealChunk().UnixNano())
			programmeBytes.Store(s.BytesOut())
			return placeholderOrigin.URL, true, nil
		case 2:
			placeholderLastReal.Store(s.LastRealChunk().UnixNano())
			placeholderBytes.Store(s.BytesOut())
			return "", false, ErrPlaceholderMedia
		default:
			return "", false, errors.New("unexpected extra upstream retry")
		}
	}

	first, chunks, _, unsubscribe := s.SubscribeForStartup("op476-dvr-capture")
	defer unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	exited := make(chan error, 1)
	s.OnExit = func(err error) { exited <- err }
	go s.Run(ctx)

	var capture bytes.Buffer
	var coverage DVRMediaCoverage
	consume := func(chunk startupChunk) {
		t.Helper()
		n, err := writeDVRChunk(&capture, chunk)
		if err != nil || n != len(chunk.data) {
			t.Fatalf("capture write=(%d,%v), want %d", n, err, len(chunk.data))
		}
		if n > 0 {
			coverage.observe(chunk.realAt, time.Time{}, time.Time{})
		}
	}
	select {
	case chunk, ok := <-first:
		if !ok {
			t.Fatal("startup closed before valid programme")
		}
		consume(chunk)
	case <-ctx.Done():
		t.Fatal("timed out waiting for valid programme startup")
	}
	for chunk := range chunks {
		consume(chunk)
	}

	var exitErr error
	select {
	case exitErr = <-exited:
	case <-time.After(time.Second):
		t.Fatal("streamer closed capture without reporting terminal cause")
	}
	if !errors.Is(exitErr, ErrPlaceholderMedia) {
		t.Fatalf("terminal error=%v, want ErrPlaceholderMedia", exitErr)
	}
	if failovers.Load() != 2 {
		t.Fatalf("upstream transitions=%d, want valid EOF then placeholder rejection", failovers.Load())
	}
	firstCause := <-causes
	secondCause := <-causes
	if !errors.Is(firstCause, io.ErrUnexpectedEOF) {
		t.Fatalf("programme transition cause=%v, want finite EOF", firstCause)
	}
	if !errors.Is(secondCause, ErrPlaceholderMedia) {
		t.Fatalf("placeholder transition cause=%v, want ErrPlaceholderMedia", secondCause)
	}
	if got := capture.Bytes(); !bytes.Equal(got, programme) {
		t.Fatalf("DVR capture bytes=%d, want exact %d-byte programme and zero placeholder bytes",
			len(got), len(programme))
	}
	if programmeBytes.Load() != int64(len(programme)) ||
		placeholderBytes.Load() != programmeBytes.Load() {
		t.Fatalf("fanout bytes changed across placeholder: programme=%d placeholder=%d want=%d",
			programmeBytes.Load(), placeholderBytes.Load(), len(programme))
	}
	if coverage.LastRealChunk.IsZero() || programmeLastReal.Load() == 0 {
		t.Fatal("valid programme did not establish LastRealChunk evidence")
	}
	if placeholderLastReal.Load() != programmeLastReal.Load() {
		t.Fatalf("placeholder advanced LastRealChunk: %d -> %d",
			programmeLastReal.Load(), placeholderLastReal.Load())
	}
}

func requireBlackTailFFmpeg(t *testing.T) (string, string) {
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

func generateBlackTailTS(
	t *testing.T,
	ffmpeg, name, videoSource, audioSource, duration, muxRate, keyframe string,
) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", videoSource,
		"-f", "lavfi", "-i", audioSource,
		"-t", duration, "-shortest",
		"-c:v", "libx264", "-preset", "ultrafast",
		"-g", keyframe, "-keyint_min", keyframe, "-sc_threshold", "0",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k",
		"-muxrate", muxRate, "-f", "mpegts", path,
	)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate %s: %v: %s", name, err, diagnostic)
	}
	media, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return media
}

func generateBlackAudioTopologyTS(
	t *testing.T,
	ffmpeg, name string,
	audioSources []string,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	args := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", "color=c=black:size=160x90:rate=25:duration=8",
	}
	for _, source := range audioSources {
		args = append(args, "-f", "lavfi", "-i", source)
	}
	args = append(args, "-t", "8", "-shortest", "-map", "0:v:0")
	for i := range audioSources {
		args = append(args, "-map", strconv.Itoa(i+1)+":a:0")
	}
	args = append(args,
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k",
		"-f", "mpegts", path,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if diagnostic, err := exec.CommandContext(ctx, ffmpeg, args...).CombinedOutput(); err != nil {
		t.Fatalf("generate %s: %v: %s", name, err, diagnostic)
	}
	return path
}

func padMPEGTSToObservedSize(t *testing.T, media []byte) []byte {
	t.Helper()
	if len(media) > observedBlackPlaceholderBytes {
		t.Fatalf("generated placeholder bytes=%d exceed observed %d",
			len(media), observedBlackPlaceholderBytes)
	}
	remaining := observedBlackPlaceholderBytes - len(media)
	if remaining%tsPacketSize != 0 {
		t.Fatalf("placeholder padding=%d is not TS-packet aligned", remaining)
	}
	nullPacket := bytes.Repeat([]byte{0xff}, tsPacketSize)
	nullPacket[0], nullPacket[1], nullPacket[2], nullPacket[3] = 0x47, 0x1f, 0xff, 0x10
	for remaining > 0 {
		media = append(media, nullPacket...)
		remaining -= len(nullPacket)
	}
	return media
}
