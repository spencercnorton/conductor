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

	"github.com/spencercnorton/conductor/internal/transcode"
)

// Real FFmpeg generates complete, decodable TS.
// Compare the actual selected-program publication gate with its sustained
// continuity guard under an explicitly modeled two-second confirmation wait.
// This is not a provider-paced replay or a complete cable-clean source attempt.
func TestRealFFmpegAVContinuityUsesEstablishedProgress(t *testing.T) {
	ffmpeg, _ := requireAudioOriginTools(t)
	for _, tc := range []struct {
		name, filter string
	}{
		{"aligned_start", "anull"},
		{"fixed_offset", "asetpts=PTS+1.8/TB"},
		{"late_audio_start", "atrim=start=1.8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			input := generateJointProgressMedia(t, ffmpeg, tc.filter)
			prefix := newStartupMediaGate(newFFprobeMediaPrefixValidator(ffmpeg))
			media, ready, err := prefix.Push(ctx, input)
			if err != nil || !ready {
				t.Fatalf("prefix ready=%v err=%v reason=%s", ready, err, prefix.Reason())
			}
			program, bound := prefix.SelectedProgram()
			if !bound {
				t.Fatal("validator did not select a programme")
			}
			var pacer transportPCRPacer
			pcrPID, havePCR := prefix.PCRPID()
			if !havePCR {
				t.Fatal("selected programme has no PCR")
			}
			pacer.Configure(pcrPID, false)
			pacer.ConfigureSelectedProgram(program, true)
			now := time.Now()
			media, err = pacer.TransformForFanoutAt(media, now)
			if err != nil {
				t.Fatalf("boundary transform: %v", err)
			}
			var gate publicationAVGate
			gate.bindSelectedProgram(program, now, true)
			out, _, fault := gate.push(media, now)
			if fault != nil || len(out) == 0 {
				t.Fatalf("publication gate released=%d fault=%v", len(out), fault)
			}
			snapshot := gate.timeline.snapshotAt(now)
			t.Logf("prefix=%s selected video=%#x audio=%#x bytes=%d published=%d snapshot=%+v", prefix.Reason(), program.videoPID, program.firstAudioPID(), len(input), len(out), snapshot)
			policy := defaultAVContinuityPolicy()
			if fault := gate.continuityFault(now, policy); fault != nil {
				t.Fatalf("unexpected immediate continuity fault: %v", fault)
			}
			if fault := gate.continuityFault(now.Add(policy.confirmation+time.Millisecond), policy); fault != nil {
				t.Fatalf("accepted clean fixture rejected after modeled confirmation: kind=%s drift=%s skew=%s", fault.kind, fault.drift, fault.skew)
			}
		})
	}
}

func generateJointProgressMedia(t *testing.T, ffmpeg, filter string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration=12",
		"-f", "lavfi", "-i", "sine=frequency=997:sample_rate=48000:duration=12",
		"-map", "0:v:0", "-map", "1:a:0", "-af", filter,
		"-c:v", "libx264", "-preset", "ultrafast", "-threads", "2", "-g", "25", "-bf", "0", "-b:v", "500k",
		"-c:a", "aac", "-b:a", "192k", "-muxrate", "1000000",
		"-muxdelay", "0", "-muxpreload", "0", "-f", "mpegts", "pipe:1")
	input, err := cmd.Output()
	if err != nil {
		t.Fatalf("fixture generation: %v%s", err, audioOriginCommandStderr(err))
	}
	decodeJointProgressMedia(t, ffmpeg, input)
	return input
}

func decodeJointProgressMedia(t *testing.T, ffmpeg string, media []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	decode := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0",
		"-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-")
	decode.Stdin = bytes.NewReader(media)
	if out, err := decode.CombinedOutput(); err != nil || len(out) != 0 {
		t.Fatalf("media decode err=%v stderr=%s", err, out)
	}
}

// Full read/FFmpeg/validation/publication/pacing runs use actual timer waits.
// The healthy transcode run retains every decoded frame. The drift control uses
// the passthrough Streamer: a transcoder is allowed to repair/reorder source PTS,
// so malformed input alone cannot prove that its published output still drifts.
func TestRealMediaStreamRetainsLateAudioAndRejectsLaterDrift(t *testing.T) {
	ffmpeg, ffprobe := requireAudioOriginTools(t)
	for _, tc := range []struct {
		name, filter string
		wantDrift    bool
	}{
		{"late_audio_start", "atrim=start=1.8", false},
		{"new_drift_after_establishment", "atrim=start=1.8", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := generateJointProgressMedia(t, ffmpeg, tc.filter)
			if tc.wantDrift {
				// Change the selected clock AFTER muxing, retaining packet
				// arrival order and every encoded sample. Asking the muxer to
				// create a timestamp gap instead reorders packets into a real
				// audio absence/tail, which is not simultaneous clock drift.
				selection, ready, err := selectCheckedInputProgram(input)
				if err != nil || !ready {
					t.Fatalf("select generated clock-defect programme: ready=%v err=%v", ready, err)
				}
				var timeline transportAVTimeline
				timeline.feed(input)
				threshold := timeline.video.minRaw + 6*transportPTSRate
				audioPID := uint16(selection.program.firstAudioPID())
				changed := 0
				for offset := 0; offset+tsPacketSize <= len(input); offset += tsPacketSize {
					packet := input[offset : offset+tsPacketSize]
					if uint16(packet[1]&0x1f)<<8|uint16(packet[2]) != audioPID || packet[1]&0x40 == 0 {
						continue
					}
					payload, ok := tsPayload(packet)
					if !ok {
						continue
					}
					pts, ok := parsePESPTS(payload)
					if ok && pts >= threshold {
						rewritePESTimestamps(input, offset, audioPID, durationTicks(1100*time.Millisecond, transportPTSRate))
						changed++
					}
				}
				if changed == 0 {
					t.Fatal("clock-defect fixture changed no selected PES timestamps")
				}
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "video/mp2t")
				w.(http.Flusher).Flush() // unknown-length live path; deliberate fixture EOF
				// Provide five seconds of startup media immediately so the real
				// checked-input gate can establish the deliberately late audio
				// inside its unchanged one-second hold. Then deliver the 1 Mbps
				// mux at its declared byte rate, letting the real two-second
				// continuity confirmation elapse before fixture EOF.
				started := time.Now()
				const initialBurst = (625_000 / tsPacketSize) * tsPacketSize
				for offset := 0; offset < len(input); {
					due := started.Add(time.Duration(max(0, offset-initialBurst)) * 8 * time.Second / 1_000_000)
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
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			started := time.Now()
			if tc.wantDrift {
				s := NewStreamer(tc.name, srv.URL, testLogger(), nil)
				s.prefixValidator = newFFprobeMediaPrefixValidator(ffmpeg)
				s.classifier = nil
				viewer, unsubscribe := s.Subscribe("joint-progress-drift-control")
				defer unsubscribe()
				drained := make(chan struct{})
				go func() {
					defer close(drained)
					for {
						select {
						case <-viewer:
						case <-ctx.Done():
							return
						}
					}
				}()
				gotBytes, runErr := s.runOnce(ctx)
				cancel()
				<-drained
				t.Logf("passthrough elapsed=%s published=%d gotBytes=%v err=%v", time.Since(started), s.BytesOut(), gotBytes, runErr)
				var fault *avContinuityError
				if !gotBytes || !errors.As(runErr, &fault) || fault.kind != avFaultTimelineDrift || fault.drift < time.Second {
					t.Fatalf("later clock divergence err=%v, want confirmed positive timeline drift after publication", runErr)
				}
				return
			}
			profile := &transcode.Profile{Name: "joint-progress-remux", Kind: transcode.KindCPURemux, VideoCodec: "copy", AudioCodec: "copy", DropSubtitles: true}
			s := NewTranscodeStreamer(tc.name, srv.URL, profile, ffmpeg, testLogger(), nil)
			s.prefixValidator = newFFprobeMediaPrefixValidator(ffmpeg)
			s.classifier = nil
			gotBytes, _, runErr, wire := captureAudioOriginRunOnce(t, s, ctx)
			t.Logf("transcode elapsed=%s published=%d gotBytes=%v err=%v", time.Since(started), len(wire), gotBytes, runErr)
			if !gotBytes || !errors.Is(runErr, io.ErrUnexpectedEOF) {
				t.Fatalf("healthy late audio stopped early: bytes=%d err=%v", len(wire), runErr)
			}
			decodeJointProgressMedia(t, ffmpeg, wire)
			count := exec.CommandContext(ctx, ffprobe, "-v", "error", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames", "-of", "json", "pipe:0")
			count.Stdin = bytes.NewReader(wire)
			out, err := count.Output()
			var parsed struct {
				Streams []struct {
					Kind   string `json:"codec_type"`
					Frames string `json:"nb_read_frames"`
				} `json:"streams"`
			}
			if err != nil || json.Unmarshal(out, &parsed) != nil || len(parsed.Streams) != 2 {
				t.Fatalf("frame count err=%v output=%s", err, out)
			}
			for _, st := range parsed.Streams {
				want := "300"
				if st.Kind == "audio" {
					want = "480"
				}
				if st.Frames != want {
					t.Fatalf("retained %s frames=%s, want %s", st.Kind, st.Frames, want)
				}
			}
		})
	}
}
