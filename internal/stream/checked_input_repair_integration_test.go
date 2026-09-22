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
	"testing"
	"time"
)

// The same encoded samples with a later bad audio-clock slope must reach the
// existing repair filter. Abrupt clock jumps still fail the raw input gate:
// finite fixture EOF can hide a delayed bad mux tail from output arbitration.
// No provider or recorded-media fixture is used.
func TestCableCleanRepairsEstablishedInputClockWithoutRelaxingOutput(t *testing.T) {
	ffmpeg, ffprobe := requireAudioOriginTools(t)
	clean := generateCheckedClockRepairMedia(t, ffmpeg)
	var alignedWire []byte
	for _, name := range []string{"aligned", "repairable_slope", "positive_jump", "negative_jump"} {
		t.Run(name, func(t *testing.T) {
			input := mutateCheckedAudioClock(t, clean, name)
			srv := checkedClockRepairServer(input)
			defer srv.Close()
			s := NewTranscodeStreamer(name, srv.URL, supportedAudioOriginProfile(), ffmpeg, testLogger(), nil)
			s.prefixValidator = newFFprobeMediaPrefixValidator(ffmpeg)
			s.classifier = nil
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			started := time.Now()
			gotBytes, upstream, runErr, wire := captureAudioOriginRunOnce(t, s, ctx)
			t.Logf("elapsed=%s published=%d got=%v upstream=%v error=%v", time.Since(started), len(wire), gotBytes, upstream, runErr)
			if name == "positive_jump" || name == "negative_jump" {
				var attemptErr *sourceAttemptError
				if !gotBytes || !upstream || errors.Is(runErr, io.ErrUnexpectedEOF) || !errors.As(runErr, &attemptErr) ||
					attemptErr.stage != "input_transport" {
					t.Fatalf("unrepairable jump did not fail the raw input guard: %v", runErr)
				}
				assertCheckedClockOutputRejects(t, ctx, ffmpeg, input)
				return
			}
			if !gotBytes || !errors.Is(runErr, io.ErrUnexpectedEOF) {
				t.Fatalf("repairable input stopped before fixture EOF: %v", runErr)
			}
			if name == "aligned" {
				alignedWire = append([]byte(nil), wire...)
			} else if !bytes.Equal(wire, alignedWire) {
				t.Fatalf("repaired wire differs from aligned control: got=%d want=%d", len(wire), len(alignedWire))
			}
			decode := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2",
				"-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-")
			decode.Stdin = bytes.NewReader(wire)
			if diagnostic, err := decode.CombinedOutput(); err != nil || len(diagnostic) != 0 {
				t.Fatalf("published repair decode: %v %s", err, diagnostic)
			}
			count := exec.CommandContext(ctx, ffprobe, "-v", "error", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames", "-of", "json", "pipe:0")
			count.Stdin = bytes.NewReader(wire)
			out, err := count.Output()
			var streams struct {
				Streams []struct {
					Kind   string `json:"codec_type"`
					Frames string `json:"nb_read_frames"`
				} `json:"streams"`
			}
			if err != nil || json.Unmarshal(out, &streams) != nil || len(streams.Streams) != 2 {
				t.Fatalf("count frames: %v %s", err, out)
			}
			for _, stream := range streams.Streams {
				want := "300"
				if stream.Kind == "audio" {
					want = "564"
				}
				if stream.Frames != want {
					t.Fatalf("%s frames=%s, want %s", stream.Kind, stream.Frames, want)
				}
			}
		})
	}
}

// Also expose the complete ungated mux tail. A finite runOnce can end before
// delayed output clocks mature, and successful decoding alone says nothing
// about their A/V alignment. This directly exercises the unchanged client gate.
func assertCheckedClockOutputRejects(t *testing.T, ctx context.Context, ffmpeg string, input []byte) {
	t.Helper()
	args := append([]string{"-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2"}, supportedAudioOriginProfile().BuildArgs()...)
	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("ungated negative transcode: %v %s", err, audioOriginCommandStderr(err))
	}
	selection, ready, err := selectCheckedInputProgram(output)
	if err != nil || !ready {
		t.Fatalf("negative output programme: ready=%v err=%v", ready, err)
	}
	base := time.Unix(1700000000, 0)
	var gate publicationAVGate
	gate.bindSelectedProgram(selection.program, base, true)
	published, _, immediate := gate.push(output, base)
	_ = gate.continuityFault(base, defaultAVContinuityPolicy())
	fault := gate.continuityFault(base.Add(2100*time.Millisecond), defaultAVContinuityPolicy())
	if len(published) != 0 || immediate != nil || fault == nil || fault.kind != avFaultTimelineDrift {
		t.Fatalf("literal jump output escaped: bytes=%d immediate=%v confirmed=%v", len(published), immediate, fault)
	}
}

func generateCheckedClockRepairMedia(t *testing.T, ffmpeg string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=12",
		"-f", "lavfi", "-i", "sine=frequency=997:sample_rate=48000:duration=12",
		"-map", "0:v:0", "-map", "1:a:0", "-c:v", "libx264", "-preset", "ultrafast", "-threads", "2",
		"-g", "25", "-bf", "0", "-b:v", "500k", "-c:a", "aac", "-b:a", "192k", "-muxrate", "1000000",
		"-muxdelay", "0", "-muxpreload", "0", "-f", "mpegts", "pipe:1")
	data, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate clock fixture: %v %s", err, audioOriginCommandStderr(err))
	}
	return data
}

func mutateCheckedAudioClock(t *testing.T, clean []byte, mode string) []byte {
	t.Helper()
	input := append([]byte(nil), clean...)
	if mode == "aligned" {
		return input
	}
	selection, ready, err := selectCheckedInputProgram(input)
	if err != nil || !ready {
		t.Fatalf("select fixture programme: ready=%v err=%v", ready, err)
	}
	var timeline transportAVTimeline
	timeline.feed(input)
	threshold := timeline.video.minRaw + 6*transportPTSRate
	changed := 0
	for offset := 0; offset+tsPacketSize <= len(input); offset += tsPacketSize {
		packet := input[offset : offset+tsPacketSize]
		pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
		if int(pid) != selection.program.firstAudioPID() || packet[1]&0x40 == 0 {
			continue
		}
		payload, ok := tsPayload(packet)
		if !ok {
			continue
		}
		pts, ok := parsePESPTS(payload)
		if !ok || pts < threshold {
			continue
		}
		var delta uint64
		switch mode {
		case "repairable_slope":
			delta = (pts - threshold) * 7 / 10
		case "positive_jump":
			delta = durationTicks(3800*time.Millisecond, transportPTSRate)
		case "negative_jump":
			delta = ptsModulus - durationTicks(2800*time.Millisecond, transportPTSRate)
		default:
			t.Fatalf("unknown clock mutation %q", mode)
		}
		rewritePESTimestamps(input, offset, pid, delta)
		changed++
	}
	if changed == 0 {
		t.Fatal("fixture changed no selected audio timestamps")
	}
	return input
}

func checkedClockRepairServer(input []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.(http.Flusher).Flush()
		// Establish five seconds of clean media, then spend actual wall time
		// delivering the later defect at the declared 1 Mbps mux byte rate.
		started := time.Now()
		const initialBurst = (625000 / tsPacketSize) * tsPacketSize
		for offset := 0; offset < len(input); {
			due := started.Add(time.Duration(max(0, offset-initialBurst)) * 8 * time.Second / 1000000)
			if delay := time.Until(due); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-r.Context().Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			end := min(offset+100*tsPacketSize, len(input))
			if _, err := w.Write(input[offset:end]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			offset = end
		}
	}))
}
