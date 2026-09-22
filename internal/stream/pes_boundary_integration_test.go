package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/transcode"
)

// FFmpeg 4.4 uses -vsync; current local FFmpeg has removed it in favor of
// -fps_mode. Both controls preserve decoded timestamps without frame synthesis.
func passthroughDecodeTimingArgs(t *testing.T, ctx context.Context, ffmpeg string) []string {
	t.Helper()
	help, err := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-h", "full").Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(help), "-fps_mode") {
		return []string{"-fps_mode", "passthrough"}
	}
	return []string{"-vsync", "0"}
}

func TestRealFFmpegAbortSlateRetryHasCompleteMedia(t *testing.T) {
	ffmpeg, _ := requireAudioOriginTools(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	generate := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=640x360:rate=25",
		"-f", "lavfi", "-i", "sine=frequency=997:sample_rate=48000", "-t", "6",
		"-c:v", "libx264", "-preset", "ultrafast", "-threads", "2", "-g", "25", "-bf", "0", "-b:v", "2000k",
		"-c:a", "aac", "-b:a", "128k", "-muxdelay", "0", "-muxpreload", "0", "-f", "mpegts", "pipe:1")
	input, err := generate.Output()
	if err != nil {
		t.Fatalf("generate: %v%s", err, audioOriginCommandStderr(err))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.(http.Flusher).Flush()
		_, _ = w.Write(input)
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()
	profile := &transcode.Profile{Name: "complete-pes-remux", Kind: transcode.KindCPURemux, VideoCodec: "copy", AudioCodec: "copy", DropSubtitles: true}
	s := NewTranscodeStreamer("abort-slate-retry", srv.URL, profile, ffmpeg, testLogger(), nil)
	s.prefixValidator = newFFprobeMediaPrefixValidator(ffmpeg)
	s.classifier = nil
	s.sourceStartupTimeout = 8 * time.Second
	viewer, unsubscribe := s.Subscribe("complete-pes-viewer")
	defer unsubscribe()
	abort := errors.New("fixture abort after delivered publication")
	var wire []byte
	s.OnBytes = func(n int) error {
		select {
		case b := <-viewer:
			wire = append(wire, b...)
		case <-ctx.Done():
			t.Fatal("publication did not reach viewer")
		}
		return abort
	}
	got, _, err := s.runOnce(ctx)
	if !got || !errors.Is(err, abort) {
		t.Fatalf("first attempt got=%v err=%v", got, err)
	}
	firstBytes := len(wire)
	s.clearRing()
	slate, err := generateSlate(ctx, ffmpeg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	s.fanoutSynthetic(slate.data)
	select {
	case b := <-viewer:
		wire = append(wire, b...)
	case <-ctx.Done():
		t.Fatal("slate not received")
	}
	// The second attempt starts at a validated configured IDR and is stopped
	// only after at least 400,000 bytes reach the same subscriber. The
	// callback boundary must itself be complete regardless of chunk size.
	secondBytes := 0
	s.OnBytes = func(n int) error {
		select {
		case b := <-viewer:
			wire = append(wire, b...)
			secondBytes += len(b)
		case <-ctx.Done():
			t.Fatal("retry not received")
		}
		if secondBytes >= 400000 {
			return abort
		}
		return nil
	}
	got, _, err = s.runOnce(ctx)
	if !got || !errors.Is(err, abort) {
		t.Fatalf("retry got=%v err=%v", got, err)
	}
	decodeArgs := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0",
		"-map", "0:v:0", "-map", "0:a:0", "-threads", "2"}
	decodeArgs = append(decodeArgs, passthroughDecodeTimingArgs(t, ctx, ffmpeg)...)
	decodeArgs = append(decodeArgs, "-f", "null", "-")
	decode := exec.CommandContext(ctx, ffmpeg, decodeArgs...)
	decode.Stdin = bytes.NewReader(wire)
	var stderr bytes.Buffer
	decode.Stderr = &stderr
	err = decode.Run()
	// Decoder-output timebase collisions are not compressed-media corruption.
	var faults []byte
	for _, line := range bytes.Split(stderr.Bytes(), []byte("\n")) {
		if bytes.Contains(line, []byte("non monotonically increasing dts")) {
			continue
		}
		if len(line) > 0 {
			faults = append(faults, line...)
			faults = append(faults, '\n')
		}
	}
	if err != nil || len(faults) > 0 {
		t.Fatalf("abort/retry decode: first=%d retry=%d err=%v diagnostics=%s", firstBytes, secondBytes, err, faults)
	}
}

type publicationTerminalErrorBody struct {
	*bytes.Reader
	terminal error
}

func (b *publicationTerminalErrorBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if errors.Is(err, io.EOF) {
		return n, b.terminal
	}
	return n, err
}
func (b *publicationTerminalErrorBody) Close() error { return nil }

func TestTranscodePESFinalTailRequiresCleanProviderEOF(t *testing.T) {
	input := bytes.Join(burstyAVPCRTestChunks(4, 25*time.Millisecond), nil)
	input = append(input, pcrTestNonPCRPacket(0x1ffe, false)[len(input)%tsPacketSize:]...)
	reset := errors.New("fixture provider connection reset")
	for _, tc := range []struct {
		name       string
		terminal   error
		tornPacket bool
		wantAll    bool
	}{
		{name: "clean entity", terminal: io.EOF, wantAll: true},
		{name: "connection reset", terminal: reset},
		{name: "truncated entity", terminal: io.ErrUnexpectedEOF},
		{name: "clean entity with withheld partial TS", terminal: io.EOF, tornPacket: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := append([]byte(nil), input...)
			if tc.tornPacket {
				body = append(body, 0x47)
			}
			s := NewTranscodeStreamer("provider-eof-provenance", "http://fixture.invalid/live", stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
			s.hc = audioOriginHTTPClient(&publicationTerminalErrorBody{Reader: bytes.NewReader(body), terminal: tc.terminal})
			s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			viewer, unsubscribe := s.Subscribe("provider-eof-viewer")
			defer unsubscribe()
			var wire []byte
			s.OnBytes = func(int) error {
				select {
				case b := <-viewer:
					wire = append(wire, b...)
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			}
			got, upstream, err := s.runOnce(ctx)
			wantErr := tc.terminal
			if errors.Is(wantErr, io.EOF) {
				wantErr = io.ErrUnexpectedEOF
			}
			if !got || !upstream || !errors.Is(err, wantErr) {
				t.Fatalf("provider terminal attribution: got=%v upstream=%v err=%v", got, upstream, err)
			}
			wantBytes := len(input) + 2*tsPacketSize
			if tc.wantAll && len(wire) != wantBytes || !tc.wantAll && len(wire) >= wantBytes {
				t.Fatalf("tail release: published=%d total=%d wantAll=%v", len(wire), wantBytes, tc.wantAll)
			}
		})
	}
}

func TestTranscodePendingPESWaitsThroughOrdinaryProviderPause(t *testing.T) {
	input := bytes.Join(burstyAVPCRTestChunks(6, 50*time.Millisecond), nil)
	pauseAfterPublication := make(chan struct{})
	resumed := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(input[:3*chunkSize])
		w.(http.Flusher).Flush()
		select {
		case <-pauseAfterPublication:
		case <-r.Context().Done():
			return
		}
		select {
		case <-time.After(5500 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(input[3*chunkSize:])
		w.(http.Flusher).Flush()
		close(resumed)
		<-r.Context().Done()
	}))
	defer srv.Close()
	s := NewTranscodeStreamer("pending-pes-provider-pause", srv.URL, stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
	viewer, unsubscribe := s.Subscribe("pending-pes-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := s.runOnce(ctx); done <- err }()
	first := false
	afterResume := false
	for !afterResume {
		select {
		case b := <-viewer:
			if len(b) == 0 {
				t.Fatal("empty or closed publication")
			}
			if !first {
				first = true
				close(pauseAfterPublication)
			}
			select {
			case <-resumed:
				afterResume = true
			default:
			}
		case err := <-done:
			t.Fatalf("pending complete-PES wait restarted before provider resumed: %v", err)
		case <-ctx.Done():
			t.Fatal("resumed media not delivered")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending PES cancellation did not join")
	}
}

func TestTranscodeLargePESPublicationRemainsOneItemAtAbort(t *testing.T) {
	const videoPID, audioPID = 0x100, 0x101
	input := joinTSPackets(testPATPacket(0x1000), testAVPMTPacketWithPCR(0x1000, videoPID, audioPID, videoPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0))
	for i := 1; i <= 420; i++ {
		input = append(input, publicationTestPacket(videoPID, false, bytes.Repeat([]byte{0x55}, 184), byte(i&15))...)
	}
	audio := testTimestampedPESPacket(audioPID, 0xc0, 0, 0)
	declareSyntheticTimestampPESLengths(audio)
	input = append(input, audio...)
	completeBytes := len(input) + 2*tsPacketSize // initial selected-PID markers
	input = append(input, testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate/10, byte(421&15))...)
	if completeBytes <= publicationChunkSize {
		t.Fatal("fixture must exceed ordinary publication parts")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(input)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	s := NewTranscodeStreamer("large-complete-pes-abort", srv.URL, stubProfile(), stubBinary(t, "exec cat"), testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: chunkSize}
	viewer, unsubscribe := s.Subscribe("large-pes-viewer")
	defer unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	abort := errors.New("abort after complete queue item")
	var wire []byte
	publications := 0
	s.OnBytes = func(n int) error {
		publications++
		select {
		case b := <-viewer:
			wire = append(wire, b...)
			if len(b) != n {
				t.Fatalf("queue item=%d callback=%d", len(b), n)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		return abort
	}
	got, upstream, err := s.runOnce(ctx)
	if !got || upstream || !errors.Is(err, abort) || publications != 1 || len(wire) != completeBytes {
		t.Fatalf("atomic abort: got=%v upstream=%v err=%v publications=%d bytes=%d want=%d", got, upstream, err, publications, len(wire), completeBytes)
	}
	s.clearRing()
	select {
	case b := <-viewer:
		t.Fatalf("aborted attempt leaked an additional %d bytes", len(b))
	default:
	}
}
