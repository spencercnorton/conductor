package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/transcode"
)

type audioOriginProbeStream struct {
	Index     int    `json:"index"`
	CodecType string `json:"codec_type"`
	Tags      struct {
		Language string `json:"language"`
	} `json:"tags"`
}

func supportedAudioOriginProfile() *transcode.Profile {
	return &transcode.Profile{
		Name: "cable-clean", Kind: transcode.KindCPUTranscode,
		VideoCodec: "copy",
		AudioCodec: "aac", AudioBitrateKbps: 192, AudioChannels: 2,
		FixTimestamps: true, DropSubtitles: true,
	}
}

type audioOriginRoundTripFunc func(*http.Request) (*http.Response, error)

func (f audioOriginRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type audioOriginBlockingBody struct {
	data      []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func newAudioOriginBlockingBody(data []byte) *audioOriginBlockingBody {
	return &audioOriginBlockingBody{data: append([]byte(nil), data...), closed: make(chan struct{})}
}

func (b *audioOriginBlockingBody) Read(p []byte) (int, error) {
	if len(b.data) > 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	<-b.closed
	return 0, io.EOF
}

func (b *audioOriginBlockingBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func audioOriginHTTPClient(body io.ReadCloser) *http.Client {
	return &http.Client{Transport: audioOriginRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/octet-stream"}},
			Body:       body,
			Request:    req,
		}, nil
	})}
}

func captureAudioOriginRunOnce(
	t *testing.T,
	s *TranscodeStreamer,
	ctx context.Context,
) (bool, bool, error, []byte) {
	t.Helper()
	chunks, unsubscribe := s.Subscribe("audio-origin-capture")
	defer unsubscribe()
	stop := make(chan struct{})
	done := make(chan []byte, 1)
	progress := make(chan struct{}, 1)
	var received atomic.Int64
	go func() {
		var output []byte
		for {
			select {
			case chunk := <-chunks:
				output = append(output, chunk...)
				received.Add(int64(len(chunk)))
				select {
				case progress <- struct{}{}:
				default:
				}
			case <-stop:
				done <- output
				return
			}
		}
	}()
	gotBytes, upstreamFault, runErr := s.runOnce(ctx)
	want := s.BytesOut()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for received.Load() < want {
		select {
		case <-progress:
		case <-deadline.C:
			close(stop)
			<-done
			t.Fatalf("subscriber captured %d/%d published bytes", received.Load(), want)
		}
	}
	close(stop)
	return gotBytes, upstreamFault, runErr, <-done
}

func TestRealFFmpegCorrectsPrimaryEnglishOriginAndNeverSelectsAlignedSpanish(t *testing.T) {
	ffmpeg, ffprobe := requireAudioOriginTools(t)
	input := generateMultiAudioOriginFixture(t, ffmpeg)

	probe := &audioOriginProbe{}
	var ready bool
	var correction time.Duration
	var probeErr error
	base := time.Unix(1_700_001_200, 0)
	for offset := 0; offset < len(input) && !ready && probeErr == nil; offset += chunkSize {
		end := min(offset+chunkSize, len(input))
		ready, correction, probeErr = probe.push(input[offset:end], base)
	}
	if probeErr != nil || !ready {
		probe.timeline.mu.Lock()
		videoEvidence := probe.timeline.video
		audioEvidence := probe.timeline.audio[0]
		corruptions := probe.timeline.transportCorruptions
		boundaries := probe.timeline.sharedBoundaries
		probe.timeline.mu.Unlock()
		t.Fatalf("real MPEG-TS origin proof=(ready=%v correction=%s err=%v input=%d probe=%d video=%+v audio=%+v corrupt=%d boundaries=%d)",
			ready, correction, probeErr, len(input), len(probe.buffer),
			videoEvidence, audioEvidence, corruptions, boundaries)
	}
	if correction < 2900*time.Millisecond || correction > 3100*time.Millisecond {
		t.Fatalf("real primary-audio correction=%s, want approximately +3s", correction)
	}

	profile := supportedAudioOriginProfile()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg, profile.BuildArgsWithAudioOriginCorrection(correction)...)
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("run corrected profile: %v%s", err, audioOriginCommandStderr(err))
	}

	streams := probeAudioOriginStreams(t, ffprobe, output)
	var video, audio *audioOriginProbeStream
	audioCount := 0
	for i := range streams {
		switch streams[i].CodecType {
		case "video":
			if video == nil {
				video = &streams[i]
			}
		case "audio":
			audioCount++
			if audio == nil {
				audio = &streams[i]
			}
		}
	}
	if video == nil || audio == nil || audioCount != 1 {
		t.Fatalf("corrected output streams=%+v, want one video and exactly one audio", streams)
	}
	if audio.Tags.Language != "eng" {
		t.Fatalf("selected audio language=%q, want primary eng (never aligned spa)", audio.Tags.Language)
	}
	videoStart, videoEnd := probeAudioOriginPacketSpan(t, ffprobe, "v:0", output)
	audioStart, audioEnd := probeAudioOriginPacketSpan(t, ffprobe, "a:0", output)
	if skew := audioStart - videoStart; skew < -0.10 || skew > 0.10 {
		t.Fatalf("corrected output initial A/V skew=%.3fs, want within 100ms", skew)
	}
	if drift := (audioEnd - audioStart) - (videoEnd - videoStart); drift < -0.12 || drift > 0.12 {
		t.Fatalf("corrected output A/V span drift=%.3fs, want within 120ms", drift)
	}
}

func TestTranscodeRunOnceSupportedProfilePreservesAndCorrectsAudioOrigins(t *testing.T) {
	ffmpeg, ffprobe := requireAudioOriginTools(t)
	tests := []struct {
		name       string
		inputSkew  time.Duration
		outputSkew time.Duration
	}{
		{name: "aligned", inputSkew: 0, outputSkew: 0},
		{name: "sub-two negative preserved", inputSkew: -1500 * time.Millisecond, outputSkew: -1500 * time.Millisecond},
		{name: "sub-two positive preserved", inputSkew: 1500 * time.Millisecond, outputSkew: 1500 * time.Millisecond},
		{name: "negative three corrected", inputSkew: -3 * time.Second, outputSkew: 0},
		{name: "positive three corrected", inputSkew: 3 * time.Second, outputSkew: 0},
		{name: "negative live boundary corrected", inputSkew: -4650 * time.Millisecond, outputSkew: 0},
		{name: "positive live boundary corrected", inputSkew: 4650 * time.Millisecond, outputSkew: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := generateMultiAudioOriginFixtureAtOffset(t, ffmpeg, tt.inputSkew)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "video/mp2t")
				w.WriteHeader(http.StatusOK)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush() // keep Content-Length unknown: this is a live attempt
				}
				_, _ = w.Write(input)
			}))
			defer srv.Close()

			s := NewTranscodeStreamer("supported-origin-"+tt.name, srv.URL,
				supportedAudioOriginProfile(), ffmpeg, testLogger(), nil)
			s.classifier = nil
			s.prefixValidator = &thresholdPrefixValidator{readyAt: 1}
			s.sourceStartupTimeout = 4 * time.Second
			gotBytes, _, runErr, output := captureAudioOriginRunOnce(t, s, context.Background())
			if !gotBytes || len(output) == 0 || !errors.Is(runErr, io.ErrUnexpectedEOF) {
				t.Fatalf("supported runOnce=(bytes=%v output=%d err=%v), want published finite boundary",
					gotBytes, len(output), runErr)
			}
			videoStart, videoEnd := probeAudioOriginPacketSpan(t, ffprobe, "v:0", output)
			audioStart, audioEnd := probeAudioOriginPacketSpan(t, ffprobe, "a:0", output)
			if skew := time.Duration((audioStart - videoStart) * float64(time.Second)); absoluteDuration(skew-tt.outputSkew) > 150*time.Millisecond {
				t.Fatalf("output skew=%s, want %s +/-150ms", skew, tt.outputSkew)
			}
			if drift := (audioEnd - audioStart) - (videoEnd - videoStart); drift < -0.12 || drift > 0.12 {
				t.Fatalf("output A/V span drift=%.3fs, want within 120ms", drift)
			}
		})
	}
}

func TestTranscodeRunOnceSupportedProfileReplaysConfiguredNonTSExactlyOnce(t *testing.T) {
	input := bytes.Repeat([]byte{0xbc}, minPrefixDecisionBytes)
	body := newAudioOriginBlockingBody(input)
	profile := supportedAudioOriginProfile()
	s := NewTranscodeStreamer("supported-non-ts", "http://audio-origin.test/live", profile,
		stubBinary(t, "exec cat"), testLogger(), audioOriginHTTPClient(body))
	s.classifier = nil
	s.prefixValidator = markerOutputPrefixValidator{}
	s.sourceStartupTimeout = 4 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var writes atomic.Int32
	s.inputPumpHooks = &transcodeInputPumpTestHooks{onWriteStart: func() { writes.Add(1) }}
	s.OnBytes = func(n int) error {
		if s.BytesOut() >= int64(len(input)) {
			cancel()
		}
		return nil
	}
	gotBytes, upstreamFault, runErr, output := captureAudioOriginRunOnce(t, s, ctx)
	if !gotBytes || upstreamFault || !errors.Is(runErr, context.Canceled) {
		t.Fatalf("non-TS runOnce=(bytes=%v upstream=%v err=%v)", gotBytes, upstreamFault, runErr)
	}
	if !bytes.Equal(output, input) || s.BytesOut() != int64(len(input)) || writes.Load() != 1 {
		t.Fatalf("non-TS replay=(captured=%d published=%d writes=%d equal=%v), want one exact replay",
			len(output), s.BytesOut(), writes.Load(), bytes.Equal(output, input))
	}
}

func TestTranscodeRunOnceSupportedProfileStartsSparseNormalOriginWithoutCorrectionProof(t *testing.T) {
	input := sparseNormalAudioOriginFixture(t)
	input = append(input, bytes.Repeat(pcrTestNonPCRPacket(0x1fff, false),
		(2*chunkSize)/tsPacketSize+1)...)
	body := newAudioOriginBlockingBody(input)
	s := NewTranscodeStreamer("supported-sparse-normal-origin", "http://audio-origin.test/live",
		supportedAudioOriginProfile(), stubBinary(t, "exec cat"), testLogger(), audioOriginHTTPClient(body))
	s.classifier = nil
	s.prefixValidator = &thresholdPrefixValidator{readyAt: 1}
	s.sourceStartupTimeout = 4 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var writes atomic.Int32
	s.inputPumpHooks = &transcodeInputPumpTestHooks{onWriteStart: func() { writes.Add(1) }}
	s.OnBytes = func(int) error {
		cancel()
		return nil
	}
	started := time.Now()
	gotBytes, upstreamFault, runErr, output := captureAudioOriginRunOnce(t, s, ctx)
	if !gotBytes || upstreamFault || !errors.Is(runErr, context.Canceled) || len(output) == 0 {
		t.Fatalf("sparse normal runOnce=(bytes=%v upstream=%v output=%d writes=%d err=%v)",
			gotBytes, upstreamFault, len(output), writes.Load(), runErr)
	}
	if elapsed := time.Since(started); elapsed >= s.sourceStartupTimeout {
		t.Fatalf("sparse normal startup took %s, want inside %s source deadline",
			elapsed, s.sourceStartupTimeout)
	}
}

func TestTranscodeRunOnceSupportedProfileRejectsAmbiguousAndMalformedTS(t *testing.T) {
	packet := pcrTestNonPCRPacket(0x1fff, false)
	tests := []struct {
		name  string
		input []byte
	}{
		{name: "ambiguous sync without programme", input: bytes.Repeat(packet, transportSyncPacketCount+2)},
		{name: "malformed selected PES", input: audioOriginFaultFixture(t, "malformed pes", audioOriginTestFirst)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := newAudioOriginBlockingBody(tt.input)
			s := NewTranscodeStreamer("supported-reject-"+tt.name, "http://audio-origin.test/live",
				supportedAudioOriginProfile(), stubBinary(t, "exec cat"), testLogger(), audioOriginHTTPClient(body))
			s.classifier = nil
			s.sourceStartupTimeout = 4 * time.Second
			var writes atomic.Int32
			s.inputPumpHooks = &transcodeInputPumpTestHooks{onWriteStart: func() { writes.Add(1) }}
			started := time.Now()
			gotBytes, upstreamFault, runErr := s.runOnce(context.Background())
			if gotBytes || !upstreamFault || !errors.Is(runErr, errUncertainTranscodeInput) || writes.Load() != 0 {
				t.Fatalf("unsafe TS runOnce=(bytes=%v upstream=%v writes=%d err=%v), want private rejection",
					gotBytes, upstreamFault, writes.Load(), runErr)
			}
			proofDeadline := maxAudioOriginProbeWall + 250*time.Millisecond
			if elapsed := time.Since(started); elapsed > proofDeadline {
				t.Fatalf("unsafe TS proof took %s, want bounded <=%s", elapsed, proofDeadline)
			}
		})
	}
}

func TestSupportedAudioOriginProofTimeoutFailsOverInsidePlexDeadline(t *testing.T) {
	ambiguous := bytes.Repeat(pcrTestNonPCRPacket(0x1fff, false), transportSyncPacketCount+2)
	sourceOneCanceled := make(chan struct{})
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(ambiguous)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		close(sourceOneCanceled)
	}))
	defer sourceOne.Close()
	sourceTwo := markerStreamServer(t, 0xbc)
	defer sourceTwo.Close()

	s := NewTranscodeStreamer("supported-origin-timeout", sourceOne.URL,
		supportedAudioOriginProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.classifier = nil
	s.prefixValidator = markerOutputPrefixValidator{}
	s.sourceStartupTimeout = 4 * time.Second
	s.initialStartupDeadline = time.Now().Add(ClientStartupBudget)
	causeCh := make(chan error, 1)
	s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
		causeCh <- cause
		return sourceTwo.URL, true, nil
	}
	viewer, unsubscribe := s.Subscribe("supported-origin-timeout-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	started := time.Now()
	go func() {
		s.Run(ctx)
		close(done)
	}()
	select {
	case chunk := <-viewer:
		if len(chunk) == 0 || chunk[0] != 0xbc {
			t.Fatalf("alternate output=%x, want 0xbc marker", chunk)
		}
	case <-time.After(9 * time.Second):
		t.Fatal("supported-profile alternate missed Plex deadline")
	}
	if elapsed := time.Since(started); elapsed >= 9*time.Second {
		t.Fatalf("supported-profile failover took %s, want <9s", elapsed)
	}
	select {
	case cause := <-causeCh:
		if !errors.Is(cause, errUncertainTranscodeInput) {
			t.Fatalf("proof timeout cause=%v, want uncertain input", cause)
		}
	default:
		t.Fatal("proof timeout did not request alternate")
	}
	select {
	case <-sourceOneCanceled:
	case <-time.After(time.Second):
		t.Fatal("proof timer did not close source-one body")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("supported-profile failover streamer did not stop")
	}
}

func requireAudioOriginTools(t *testing.T) (string, string) {
	t.Helper()
	ffmpeg, ffmpegErr := exec.LookPath("ffmpeg")
	ffprobe, ffprobeErr := exec.LookPath("ffprobe")
	if ffmpegErr == nil && ffprobeErr == nil {
		return ffmpeg, ffprobe
	}
	if os.Getenv("CI") != "" {
		t.Fatalf("supported CI image requires ffmpeg and ffprobe: ffmpeg=%v ffprobe=%v",
			ffmpegErr, ffprobeErr)
	}
	t.Skipf("ffmpeg/ffprobe not on PATH: ffmpeg=%v ffprobe=%v", ffmpegErr, ffprobeErr)
	return "", ""
}

func generateMultiAudioOriginFixture(t *testing.T, ffmpeg string) []byte {
	return generateMultiAudioOriginFixtureAtOffset(t, ffmpeg, -3*time.Second)
}

func generateMultiAudioOriginFixtureAtOffset(
	t *testing.T,
	ffmpeg string,
	primaryOffset time.Duration,
) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25:duration=6",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=6",
		"-f", "lavfi", "-i", "sine=frequency=880:sample_rate=48000:duration=6",
		"-filter_complex", "[0:v]setpts=PTS+10/TB[v];[1:a]asetpts=PTS+10/TB[a1];[2:a]asetpts=PTS+10/TB[a2]",
		"-map", "[v]", "-map", "[a1]", "-map", "[a2]",
		"-metadata:s:a:0", "language=eng", "-metadata:s:a:1", "language=spa",
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "25", "-keyint_min", "25",
		"-sc_threshold", "0", "-c:a", "aac", "-b:a", "96k",
		"-mpegts_copyts", "1", "-muxdelay", "0", "-muxpreload", "0",
		"-f", "mpegts", "pipe:1")
	media, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate multi-audio origin fixture: %v%s", err, audioOriginCommandStderr(err))
	}
	if len(media) == 0 || len(media) > maxAudioOriginProbeBytes {
		t.Fatalf("fixture bytes=%d, want 1..%d", len(media), maxAudioOriginProbeBytes)
	}
	selection, ready, selectionErr := selectCheckedInputProgram(media)
	if selectionErr != nil || !ready || selection.program.firstAudioPID() < 0 {
		t.Fatalf("select generated first audio: ready=%v err=%v program=%+v",
			ready, selectionErr, selection.program)
	}
	// Model a restream clock origin, not an intentional audio pre-roll: keep
	// packet arrival/interleave unchanged and shift only primary-English PES
	// clocks. Spanish remains aligned and therefore proves the detector never
	// chooses a nicer secondary track.
	firstAudioPID := uint16(selection.program.firstAudioPID())
	delta := durationTicks(absoluteDuration(primaryOffset), transportPTSRate)
	if primaryOffset < 0 {
		delta = ptsModulus - delta
	}
	for start := 0; start+tsPacketSize <= len(media); start += tsPacketSize {
		packet := media[start : start+tsPacketSize]
		pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
		if primaryOffset != 0 && pid == firstAudioPID && packet[1]&0x40 != 0 {
			rewritePESTimestamps(media, start, firstAudioPID, delta)
		}
	}
	return media
}

func probeAudioOriginStreams(t *testing.T, ffprobe string, media []byte) []audioOriginProbeStream {
	t.Helper()
	cmd := exec.Command(ffprobe,
		"-v", "error",
		"-show_entries", "stream=index,codec_type,start_time,duration:stream_tags=language",
		"-of", "json", "pipe:0")
	cmd.Stdin = bytes.NewReader(media)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("probe corrected output: %v%s", err, audioOriginCommandStderr(err))
	}
	var result struct {
		Streams []audioOriginProbeStream `json:"streams"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("decode ffprobe output: %v", err)
	}
	return result.Streams
}

func parseProbeSeconds(t *testing.T, raw string) float64 {
	t.Helper()
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("parse ffprobe seconds %q: %v", raw, err)
	}
	return value
}

func probeAudioOriginPacketSpan(
	t *testing.T,
	ffprobe, selector string,
	media []byte,
) (float64, float64) {
	t.Helper()
	cmd := exec.Command(ffprobe,
		"-v", "error", "-select_streams", selector,
		"-show_entries", "packet=pts_time,duration_time", "-of", "json", "pipe:0")
	cmd.Stdin = bytes.NewReader(media)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("probe %s packets: %v%s", selector, err, audioOriginCommandStderr(err))
	}
	var result struct {
		Packets []struct {
			PTS      string `json:"pts_time"`
			Duration string `json:"duration_time"`
		} `json:"packets"`
	}
	if err := json.Unmarshal(out, &result); err != nil || len(result.Packets) < 2 {
		t.Fatalf("decode %s packet timeline: packets=%d err=%v", selector, len(result.Packets), err)
	}
	first := parseProbeSeconds(t, result.Packets[0].PTS)
	lastPacket := result.Packets[len(result.Packets)-1]
	last := parseProbeSeconds(t, lastPacket.PTS)
	if lastPacket.Duration != "" {
		last += parseProbeSeconds(t, lastPacket.Duration)
	}
	return first, last
}

func audioOriginCommandStderr(err error) string {
	if exitErr, ok := err.(*exec.ExitError); ok && len(exitErr.Stderr) > 0 {
		return ": " + string(exitErr.Stderr)
	}
	return ""
}
