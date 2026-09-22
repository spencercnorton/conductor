package stream

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/transcode"
)

// This couples actual Run retry ownership with real TS publication and fallback.
// Sources have distinct decoded colors and tones. Only the finite-placeholder
// classifier is disabled: these deliberately short test entities are intended
// to exercise live recovery. Prefix validation, pacing and watchdogs are real.
func TestRealMediaRecoveryFillerSurvivesRelocationAndStartup(t *testing.T) {
	ffmpeg, _ := requireAudioOriginTools(t)
	firstMedia := generateRecoveryMedia(t, ffmpeg, "red", 440, 2)
	secondMedia := generateRecoveryMedia(t, ffmpeg, "blue", 660, 8)
	firstRed, _ := countRecoveryFrameColors(t, ffmpeg, firstMedia)
	if firstRed < 10 {
		t.Fatalf("first source has only %d red frames", firstRed)
	}
	prepareCtx, prepareCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer prepareCancel()
	sl, err := generateSlate(prepareCtx, ffmpeg, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	for _, kind := range []string{"passthrough", "transcode_remux", "transcode_aac"} {
		t.Run(kind, func(t *testing.T) {
			var attemptLog bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&attemptLog, &slog.HandlerOptions{Level: slog.LevelInfo}))
			t.Cleanup(func() {
				if t.Failed() {
					data := attemptLog.String()
					if len(data) > 12000 {
						data = data[len(data)-12000:]
					}
					t.Logf("task-local attempt diagnostics: %s", data)
				}
			})
			var firstRequests, secondRequests, relocations, recoveryPhase atomic.Int32
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				firstRequests.Add(1)
				w.Header().Set("Content-Type", "video/mp2t")
				w.Header().Set("Content-Length", strconv.Itoa(len(firstMedia)))
				_, _ = w.Write(firstMedia)
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				secondRequests.Add(1)
				w.Header().Set("Content-Type", "video/mp2t")
				w.(http.Flusher).Flush() // live, unknown-length body
				if waitTransportDelay(r.Context(), 800*time.Millisecond) != nil {
					return
				}
				recoveryPhase.Store(3)
				// Supply approximately six media seconds promptly so default
				// FFmpeg probing can finish, then continue input across the
				// validator's 500 ms probe cadence. The remaining quarter uses
				// a synthetic two-second byte schedule, not a provider replay.
				burst := (len(secondMedia) * 3 / 4 / tsPacketSize) * tsPacketSize
				started := time.Now()
				for offset := 0; offset < len(secondMedia); {
					due := started.Add(time.Duration(max(0, offset-burst)) * 2 * time.Second / time.Duration(len(secondMedia)-burst))
					if delay := time.Until(due); delay > 0 && waitTransportDelay(r.Context(), delay) != nil {
						return
					}
					end := min(offset+100*tsPacketSize, len(secondMedia))
					if _, err := w.Write(secondMedia[offset:end]); err != nil {
						return
					}
					w.(http.Flusher).Flush()
					offset = end
				}
				<-r.Context().Done() // observer stops at a received publication
			}))
			defer second.Close()
			relocate := func(ctx context.Context, cause error) (string, bool, error) {
				relocations.Add(1)
				recoveryPhase.Store(1)
				t.Logf("first-source relocation cause=%v", cause)
				if err := waitTransportDelay(ctx, 400*time.Millisecond); err != nil {
					return "", false, err
				}
				recoveryPhase.Store(2)
				return second.URL, true, nil
			}
			var set *subscriberSet
			var run func(context.Context)
			if kind == "passthrough" {
				s := NewStreamer(kind, first.URL, logger, nil)
				s.prefixValidator = newFFprobeMediaPrefixValidator(ffmpeg)
				s.classifier = nil
				s.OnUpstreamDown = relocate
				set, run = &s.subscriberSet, s.Run
			} else {
				profile := &transcode.Profile{Name: "recovery-media-remux", Kind: transcode.KindCPURemux, VideoCodec: "copy", AudioCodec: "copy", DropSubtitles: true}
				if kind == "transcode_aac" {
					profile = &transcode.Profile{Name: "recovery-media-cable-clean", Kind: transcode.KindCPUTranscode, VideoCodec: "copy", AudioCodec: "aac", AudioBitrateKbps: 192, AudioChannels: 2, FixTimestamps: true, DropSubtitles: true}
				}
				s := NewTranscodeStreamer(kind, first.URL, profile, ffmpeg, logger, nil)
				s.prefixValidator = newFFprobeMediaPrefixValidator(ffmpeg)
				s.classifier = nil
				s.OnUpstreamDown = relocate
				set, run = &s.subscriberSet, s.Run
			}
			set.slateAfter = 0 // isolate recovery continuity from intentional delay
			set.SlateFn = func() *slate { return sl }
			firstReal, continuation, _, unsubscribe := set.SubscribeForStartup("real-recovery-viewer")
			defer unsubscribe()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); run(ctx) }()
			defer func() { cancel(); <-done }()
			var firstChunk startupChunk
			select {
			case chunk, ok := <-firstReal:
				if !ok || len(chunk.data) == 0 || chunk.realAt.IsZero() || !chunk.validatedStart {
					t.Fatal("initial source did not publish validated real media")
				}
				firstChunk = chunk
			case <-ctx.Done():
				t.Fatal("initial source startup timed out")
			}
			var firstClockEpoch *outputClockEpoch
			if kind != "passthrough" {
				set.epochDeliveryMu.Lock()
				clock := set.outputClock
				set.epochDeliveryMu.Unlock()
				if !clock.enabled || clock.offset != 0 {
					t.Fatalf("actual startup release bypassed clock or changed its initial origin: enabled=%v offset=%d", clock.enabled, clock.offset)
				}
				firstClockEpoch = clock.epoch
			}
			wire := append([]byte(nil), firstChunk.data...)
			var recovered []byte
			var firstWireBytes int
			var recoveryTimeline transportAVTimeline
			previous := time.Now()
			var maxArrivalGap time.Duration
			var firstSlate, lastSlate, recoveredAt time.Time
			var slateChunks, recoveredChunks, relocationSlate, startupSlate int
			for {
				select {
				case <-ctx.Done():
					t.Fatalf("recovery timeout: requests=%d/%d relocations=%d slate=%d recovered=%d", firstRequests.Load(), secondRequests.Load(), relocations.Load(), slateChunks, recoveredChunks)
				case chunk, ok := <-continuation:
					if !ok || len(chunk.data) == 0 {
						t.Fatal("subscriber ended before complete real recovery")
					}
					if kind != "passthrough" {
						set.epochDeliveryMu.Lock()
						enabled := set.outputClock.enabled
						set.epochDeliveryMu.Unlock()
						if !enabled {
							t.Fatal("actual Run recovery silently disabled the established clock")
						}
					}
					now := time.Now()
					maxArrivalGap = max(maxArrivalGap, now.Sub(previous))
					previous = now
					wire = append(wire, chunk.data...)
					if chunk.realAt.IsZero() {
						if !recoveredAt.IsZero() {
							t.Fatal("synthetic publication followed real takeover")
						}
						if firstSlate.IsZero() {
							firstSlate = now
							firstWireBytes = len(wire) - len(chunk.data)
						}
						lastSlate = now
						slateChunks++
						switch recoveryPhase.Load() {
						case 1:
							relocationSlate++
						case 2:
							startupSlate++
						}
					} else if !firstSlate.IsZero() {
						if recoveredAt.IsZero() {
							recoveredAt = now
						}
						recoveredChunks++
						recovered = append(recovered, chunk.data...)
						recoveryTimeline.feedAt(chunk.data, now)
						if recoveryTimeline.snapshotAt(now).videoSamples >= 12 && now.Sub(recoveredAt) >= 250*time.Millisecond {
							// Cancellation occurs only after this whole public unit
							// was received; no socket write or chunk is cut by test code.
							cancel()
							<-done
							goto captured
						}
					}
				}
			}
		captured:
			if kind != "passthrough" {
				clock := set.outputClock // Run and filler have both joined above.
				if !clock.enabled || clock.epoch == firstClockEpoch || clock.offset == 0 {
					t.Fatalf("recovered actual Run epoch was not translated: enabled=%v offset=%d", clock.enabled, clock.offset)
				}
			}
			if firstRequests.Load() != 1 || secondRequests.Load() != 1 || relocations.Load() != 1 {
				t.Fatalf("unexpected retries: first=%d second=%d relocations=%d", firstRequests.Load(), secondRequests.Load(), relocations.Load())
			}
			if slateChunks < 5 || lastSlate.Sub(firstSlate) < 800*time.Millisecond || recoveredChunks < 2 {
				t.Fatalf("did not exercise sustained fallback/takeover: slate=%d span=%s recovered=%d", slateChunks, lastSlate.Sub(firstSlate), recoveredChunks)
			}
			// A complete slate group may itself cover the 400 ms callback;
			// count actual delivery in each phase without assuming a frame rate.
			if relocationSlate < 1 || startupSlate < 1 {
				t.Errorf("fallback did not span both private phases: relocation=%d startup=%d", relocationSlate, startupSlate)
			}
			if maxArrivalGap > 750*time.Millisecond {
				t.Errorf("arrival gap crossed relocation/private startup: %s", maxArrivalGap)
			}
			// Fully decode every retained byte. The second decode classifies
			// actual frame colors; no single FPS assumption spans the real slate.
			decodeRecoveryMedia(t, ffmpeg, wire)
			initialWireRed, _ := countRecoveryFrameColors(t, ffmpeg, wire[:firstWireBytes])
			red, blue := countRecoveryFrameColors(t, ffmpeg, wire)
			_, recoveredBlue := countRecoveryFrameColors(t, ffmpeg, recovered)
			// Epoch reset may discard a whole queued final source group. Report
			// exact input/standalone/transition counts, but do not turn this
			// recovery test into a claim of lossless queue retention.
			t.Logf("initial-source frame evidence: input=%d standaloneWire=%d transitionWire=%d", firstRed, initialWireRed, red)
			if initialWireRed < 10 || red < 10 || blue < 8 || recoveredBlue < 8 {
				t.Fatalf("decoded source colors: red=%d/%d initialWireRed=%d blue=%d recoveredBlue=%d", red, firstRed, initialWireRed, blue, recoveredBlue)
			}
			t.Logf("wire=%d recovered=%d slateChunks=%d slateSpan=%s relocationSlate=%d startupSlate=%d recoveryChunks=%d maxGap=%s decodedRed=%d decodedBlue=%d", len(wire), len(recovered), slateChunks, lastSlate.Sub(firstSlate), relocationSlate, startupSlate, recoveredChunks, maxArrivalGap, red, blue)
		})
	}
}

func generateRecoveryMedia(t *testing.T, ffmpeg, color string, tone, seconds int) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", fmt.Sprintf("color=c=%s:size=320x180:rate=25:duration=%d", color, seconds),
		"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=%d:sample_rate=48000:duration=%d", tone, seconds),
		"-map", "0:v:0", "-map", "1:a:0", "-vf", "noise=alls=30:allf=t", "-c:v", "libx264", "-preset", "ultrafast", "-threads", "2", "-g", "25", "-bf", "0", "-b:v", "700k",
		"-c:a", "aac", "-b:a", "128k", "-muxrate", "1000000", "-muxdelay", "0", "-muxpreload", "0", "-f", "mpegts", "pipe:1")
	data, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate %s: %v%s", color, err, audioOriginCommandStderr(err))
	}
	decodeRecoveryMedia(t, ffmpeg, data)
	return data
}

func recoveryMediaTimingArgs(t *testing.T, ctx context.Context, ffmpeg string) []string {
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

func decodeRecoveryMedia(t *testing.T, ffmpeg string, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0", "-threads", "2"}
	args = append(args, recoveryMediaTimingArgs(t, ctx, ffmpeg)...)
	args = append(args, "-f", "null", "-")
	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	cmd.Stdin = bytes.NewReader(data)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var faults []string
	var dtsWarnings int
	for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
		if strings.Contains(line, "non monotonically increasing dts") {
			dtsWarnings++ // output-mux warning, counted separately from decode
		} else if line != "" {
			faults = append(faults, line)
		}
	}
	if dtsWarnings > 0 {
		t.Logf("full decode output-mux DTS warnings=%d", dtsWarnings)
	}
	if err != nil || len(faults) != 0 {
		t.Fatalf("full retained-wire decode: err=%v diagnostics=%s", err, strings.Join(faults, "\n"))
	}
}

func countRecoveryFrameColors(t *testing.T, ffmpeg string, data []byte) (red, blue int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0", "-map", "0:v:0", "-an", "-vf", "scale=1:1:flags=area,format=rgb24", "-threads", "2"}
	args = append(args, recoveryMediaTimingArgs(t, ctx, ffmpeg)...)
	args = append(args, "-f", "rawvideo", "pipe:1")
	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	cmd.Stdin = bytes.NewReader(data)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	pixels, err := cmd.Output()
	var faults []string
	var dtsWarnings int
	for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
		if strings.Contains(line, "non monotonically increasing dts") {
			dtsWarnings++
		} else if line != "" {
			faults = append(faults, line)
		}
	}
	if dtsWarnings > 0 {
		t.Logf("color decode output-mux DTS warnings=%d", dtsWarnings)
	}
	if err != nil || len(faults) > 0 || len(pixels)%3 != 0 {
		t.Fatalf("decode color evidence: err=%v bytes=%d diagnostics=%s", err, len(pixels), strings.Join(faults, "\n"))
	}
	for i := 0; i < len(pixels); i += 3 {
		r, g, b := pixels[i], pixels[i+1], pixels[i+2]
		if r > 150 && g < 70 && b < 70 {
			red++
		}
		if b > 150 && g < 70 && r < 70 {
			blue++
		}
	}
	return red, blue
}
