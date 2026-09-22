package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/transcode"
)

func TestRealFFmpegRetainedLiveTransportDecodesEveryFrame(t *testing.T) {
	ffmpeg, ffprobe := requireAudioOriginTools(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	generate := exec.CommandContext(ctx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25",
		"-f", "lavfi", "-i", "sine=frequency=997:sample_rate=48000",
		"-t", "6", "-map", "0:v:0", "-map", "1:a:0",
		"-c:v", "libx264", "-preset", "ultrafast", "-threads", "2", "-g", "25", "-bf", "0", "-b:v", "500k",
		"-c:a", "aac", "-b:a", "96k", "-muxrate", "1000000",
		"-muxdelay", "0", "-muxpreload", "0", "-f", "mpegts", "pipe:1")
	input, err := generate.Output()
	if err != nil {
		t.Fatalf("generate media: %v%s", err, audioOriginCommandStderr(err))
	}
	// A complete burst goes through real FFmpeg prefix validation, remux,
	// pacing and fanout. Every generated frame must survive the subscriber
	// byte stream; the synthetic regression separately forces the pacing cap.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.(http.Flusher).Flush() // unknown-length live path, then a deliberate finite test endpoint
		_, _ = w.Write(input)
	}))
	defer srv.Close()
	profile := &transcode.Profile{Name: "retention-real-remux", Kind: transcode.KindCPURemux, VideoCodec: "copy", AudioCodec: "copy", DropSubtitles: true}
	s := NewTranscodeStreamer("real-retained-media", srv.URL, profile, ffmpeg, testLogger(), nil)
	s.prefixValidator = newFFprobeMediaPrefixValidator(ffmpeg)
	s.classifier = nil
	s.sourceStartupTimeout = 6 * time.Second
	gotBytes, _, runErr, wire := captureAudioOriginRunOnce(t, s, ctx)
	if !gotBytes || !errors.Is(runErr, io.ErrUnexpectedEOF) {
		t.Fatalf("real live attempt bytes=%v output=%d err=%v", gotBytes, len(wire), runErr)
	}
	if len(wire)%tsPacketSize != 0 {
		t.Fatalf("partial final TS packet: %d bytes", len(wire))
	}
	for off := 0; off < len(wire); off += tsPacketSize {
		if wire[off] != 0x47 {
			t.Fatalf("subscriber TS sync lost at %d", off)
		}
	}
	decode := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0",
		"-map", "0:v:0", "-map", "0:a:0", "-threads", "2", "-f", "null", "-")
	decode.Stdin = bytes.NewReader(wire)
	var diagnostics bytes.Buffer
	decode.Stderr = &diagnostics
	_, err = decode.Output()
	if err != nil || diagnostics.Len() != 0 {
		t.Fatalf("subscriber decode err=%v diagnostics=%s", err, diagnostics.String())
	}
	frameCounts := func(media []byte) map[string]int {
		t.Helper()
		count := exec.CommandContext(ctx, ffprobe, "-v", "error", "-threads", "2", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames", "-of", "json", "pipe:0")
		count.Stdin = bytes.NewReader(media)
		count.Stderr = &diagnostics
		counts, countErr := count.Output()
		var parsed struct {
			Streams []struct {
				Kind   string `json:"codec_type"`
				Frames string `json:"nb_read_frames"`
			} `json:"streams"`
		}
		if countErr != nil || json.Unmarshal(counts, &parsed) != nil || len(parsed.Streams) != 2 || diagnostics.Len() != 0 {
			t.Fatalf("frame count err=%v output=%s diagnostics=%s", countErr, counts, diagnostics.String())
		}
		result := make(map[string]int)
		for _, stream := range parsed.Streams {
			result[stream.Kind], _ = strconv.Atoi(stream.Frames)
		}
		return result
	}
	wantFrames, gotFrames := frameCounts(input), frameCounts(wire)
	if gotFrames["video"] != 150 || gotFrames["audio"] == 0 || gotFrames["audio"] != wantFrames["audio"] {
		t.Fatalf("decoded frames=%v, want all generated frames=%v", gotFrames, wantFrames)
	}
}
