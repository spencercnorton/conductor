package transcode_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/transcode"
)

type packetTimeline struct {
	first     float64
	end       float64
	previous  float64
	packets   int
	monotonic bool
}

// TestCableCleanUsesOneStableAudioClock is a real FFmpeg regression for the
// 2026-08-22 Comedy Central incident. Plex stayed in "playing" while its live
// timeline fell to roughly 0.714x and accumulated minutes of lag. The old
// aresample=async=1000:first_pts=0 followed a malformed audio PTS slope next
// to copied video; successful process exit alone could not catch that.
func TestCableCleanUsesOneStableAudioClock(t *testing.T) {
	ffmpeg, ffprobe := requireTimelineTools(t)

	profile := &transcode.Profile{
		Name: "cable-clean", Kind: transcode.KindCPUTranscode,
		VideoCodec: "copy",
		AudioCodec: "aac", AudioBitrateKbps: 192, AudioChannels: 2,
		FixTimestamps: true, DropSubtitles: true,
	}

	tests := []struct {
		name        string
		inputFilter string
		assert      func(t *testing.T, video, audio packetTimeline)
	}{
		{
			name:        "gradual one percent audio clock skew is removed",
			inputFilter: "asetpts=PTS*1.01",
			assert: func(t *testing.T, video, audio packetTimeline) {
				if delta := timelineSpan(audio) - timelineSpan(video); delta < -0.12 || delta > 0.12 {
					t.Fatalf("output A/V span delta %.3fs, want within 120ms (video %.3fs, audio %.3fs)",
						delta, timelineSpan(video), timelineSpan(audio))
				}
			},
		},
		{
			name:        "incident scale forty percent audio clock skew is removed",
			inputFilter: "asetpts=PTS*1.4",
			assert: func(t *testing.T, video, audio packetTimeline) {
				if delta := timelineSpan(audio) - timelineSpan(video); delta < -0.12 || delta > 0.12 {
					t.Fatalf("incident-scale output A/V span delta %.3fs, want within 120ms (video %.3fs, audio %.3fs)",
						delta, timelineSpan(video), timelineSpan(audio))
				}
			},
		},
		{
			name:        "fixed initial offset is preserved",
			inputFilter: "asetpts=PTS+1/TB",
			assert: func(t *testing.T, video, audio packetTimeline) {
				if delta := audio.first - video.first; delta < 0.80 || delta > 1.20 {
					t.Fatalf("output initial A/V offset %.3fs, want about 1s", delta)
				}
				if delta := timelineSpan(audio) - timelineSpan(video); delta < -0.12 || delta > 0.12 {
					t.Fatalf("fixed offset changed media span by %.3fs", delta)
				}
			},
		},
		{
			name:        "large timestamp gap remains real silence",
			inputFilter: `asetpts=PTS+2/TB*gte(T\,5)`,
			assert: func(t *testing.T, video, audio packetTimeline) {
				if delta := timelineSpan(audio) - timelineSpan(video); delta < 1.75 || delta > 2.25 {
					t.Fatalf("preserved hard-gap delta %.3fs, want about 2s", delta)
				}
			},
		},
		{
			name:        "missing audio packets retain programme duration",
			inputFilter: `aselect=not(between(t\,4\,6))`,
			assert: func(t *testing.T, video, audio packetTimeline) {
				if delta := timelineSpan(audio) - timelineSpan(video); delta < -0.12 || delta > 0.12 {
					t.Fatalf("packet-loss gap compressed by %.3fs (video %.3fs, audio %.3fs)",
						delta, timelineSpan(video), timelineSpan(audio))
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := generateTimelineFixture(t, ffmpeg, tc.inputFilter)
			output := runProfile(t, ffmpeg, profile, input)
			video := probeTimeline(t, ffprobe, "v:0", output)
			audio := probeTimeline(t, ffprobe, "a:0", output)
			if !audio.monotonic {
				t.Fatal("normalized audio PTS moved backward")
			}
			tc.assert(t, video, audio)
		})
	}
}

func TestCableCleanWallStarvationDoesNotCreateAVSkew(t *testing.T) {
	ffmpeg, ffprobe := requireTimelineTools(t)
	profile := &transcode.Profile{
		Name: "cable-clean", Kind: transcode.KindCPUTranscode,
		VideoCodec: "copy",
		AudioCodec: "aac", AudioBitrateKbps: 192, AudioChannels: 2,
		FixTimestamps: true, DropSubtitles: true,
	}

	input := generateTimelineFixture(t, ffmpeg, "anull")
	// Fox Business delivered several seconds of media after an 8.5-second
	// starvation interval. A shorter real wall pause exercises the same FFmpeg
	// pipe boundary without making the regression itself take eight seconds.
	output := runProfileWithInputPause(t, ffmpeg, profile, input, 250*time.Millisecond)
	video := probeTimeline(t, ffprobe, "v:0", output)
	audio := probeTimeline(t, ffprobe, "a:0", output)
	if !video.monotonic || !audio.monotonic {
		t.Fatal("wall-starved cable-clean output moved backward")
	}
	if delta := timelineSpan(audio) - timelineSpan(video); delta < -0.12 || delta > 0.12 {
		t.Fatalf("wall starvation created %.3fs A/V span delta (video %.3fs, audio %.3fs)",
			delta, timelineSpan(video), timelineSpan(audio))
	}
}

// These pathologies must reach asetpts before an MPEG-TS muxer can discard or
// rewrite them, so this companion regression applies the exact profile filter
// directly to decoded samples. It covers the two clock edges that are awkward
// to preserve in a generated transport fixture: a backward reset and the
// 2^33/90kHz MPEG-TS PTS wrap.
func TestCableCleanAudioFilterHandlesBackwardResetAndMPEGTSWrap(t *testing.T) {
	ffmpeg, ffprobe := requireTimelineTools(t)
	profile := &transcode.Profile{
		Kind: transcode.KindCPUTranscode, VideoCodec: "copy", AudioCodec: "aac",
		FixTimestamps: true, DropSubtitles: true,
	}
	filter := profileFilterArg(t, profile, "-af")

	for _, tc := range []struct {
		name      string
		pathology string
	}{
		{name: "backward timestamp reset", pathology: `asetpts=PTS-2/TB*gte(T\,5)`},
		{name: "mpeg ts pts wrap", pathology: `asetpts=mod(PTS+95440/TB\,95443.7176888889/TB)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(ffmpeg,
				"-hide_banner", "-loglevel", "error", "-nostdin",
				"-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000:duration=12",
				"-af", tc.pathology+","+filter,
				"-c:a", "pcm_s16le", "-f", "nut", "pipe:1")
			media, err := cmd.Output()
			if err != nil {
				t.Fatalf("normalize clock pathology: %v%s", err, commandStderr(err))
			}
			timeline := probeTimeline(t, ffprobe, "a:0", media)
			if !timeline.monotonic {
				t.Fatal("normalized audio PTS moved backward")
			}
			if span := timelineSpan(timeline); span < 11.8 || span > 12.2 {
				t.Fatalf("normalized audio span %.3fs, want about 12s", span)
			}
		})
	}
}

func requireTimelineTools(t *testing.T) (string, string) {
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

func profileFilterArg(t *testing.T, profile *transcode.Profile, flag string) string {
	t.Helper()
	args := profile.BuildArgs()
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	t.Fatalf("profile args contain no %s filter: %v", flag, args)
	return ""
}

func generateTimelineFixture(t *testing.T, ffmpeg, audioFilter string) []byte {
	t.Helper()
	args := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin",
		"-f", "lavfi", "-i", "testsrc2=size=160x90:rate=30:duration=12",
		"-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000:duration=12",
		"-filter:a", audioFilter,
		"-c:v", "libx264", "-preset", "ultrafast", "-g", "30",
		"-c:a", "aac", "-f", "mpegts", "pipe:1",
	}
	cmd := exec.Command(ffmpeg, args...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("generate timeline fixture: %v%s", err, commandStderr(err))
	}
	return out
}

func runProfile(t *testing.T, ffmpeg string, profile *transcode.Profile, input []byte) []byte {
	t.Helper()
	args := append([]string{"-hide_banner", "-loglevel", "error", "-nostdin"}, profile.BuildArgs()...)
	cmd := exec.Command(ffmpeg, args...)
	cmd.Stdin = bytes.NewReader(input)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run profile: %v%s", err, commandStderr(err))
	}
	return out
}

func runProfileWithInputPause(t *testing.T, ffmpeg string, profile *transcode.Profile, input []byte, pause time.Duration) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := append([]string{"-hide_banner", "-loglevel", "error", "-nostdin"}, profile.BuildArgs()...)
	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("open profile stdin: %v", err)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	started, waited := false, false
	t.Cleanup(func() {
		cancel()
		_ = stdin.Close()
		if started && !waited {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			_ = cmd.Wait()
		}
	})
	if err := cmd.Start(); err != nil {
		t.Fatalf("start profile: %v", err)
	}
	started = true

	split := len(input) / 2
	split -= split % 188
	_, feedErr := io.Copy(stdin, bytes.NewReader(input[:split]))
	if feedErr == nil {
		timer := time.NewTimer(pause)
		select {
		case <-timer.C:
		case <-ctx.Done():
			_ = timer.Stop()
		}
		if ctx.Err() == nil {
			_, feedErr = io.Copy(stdin, bytes.NewReader(input[split:]))
		}
	}
	closeErr := stdin.Close()
	waitErr := cmd.Wait()
	waited = true
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("paused profile exceeded 30s deadline: %s", strings.TrimSpace(stderr.String()))
	}
	if feedErr != nil {
		t.Fatalf("feed paused profile input: %v", feedErr)
	}
	if closeErr != nil {
		t.Fatalf("close paused profile input: %v", closeErr)
	}
	if waitErr != nil {
		t.Fatalf("run paused profile: %v: %s", waitErr, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes()
}

func probeTimeline(t *testing.T, ffprobe, selector string, media []byte) packetTimeline {
	t.Helper()
	cmd := exec.Command(ffprobe,
		"-hide_banner", "-loglevel", "error",
		"-select_streams", selector,
		"-show_entries", "packet=pts_time,duration_time",
		"-of", "csv=p=0", "pipe:0")
	cmd.Stdin = bytes.NewReader(media)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("probe %s timeline: %v%s", selector, err, commandStderr(err))
	}

	stats := packetTimeline{monotonic: true}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Split(strings.TrimSpace(line), ",")
		if len(fields) < 2 || fields[0] == "N/A" || fields[1] == "N/A" {
			continue
		}
		pts, ptsErr := strconv.ParseFloat(fields[0], 64)
		duration, durationErr := strconv.ParseFloat(fields[1], 64)
		if ptsErr != nil || durationErr != nil {
			t.Fatalf("parse %s packet %q: pts=%v duration=%v", selector, line, ptsErr, durationErr)
		}
		if stats.packets == 0 {
			stats.first = pts
		} else if pts+0.000001 < stats.previous {
			stats.monotonic = false
		}
		stats.previous = pts
		stats.end = max(stats.end, pts+duration)
		stats.packets++
	}
	if stats.packets == 0 {
		t.Fatalf("probe %s returned no timestamped packets", selector)
	}
	return stats
}

func timelineSpan(stats packetTimeline) float64 { return stats.end - stats.first }

func commandStderr(err error) string {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return ""
	}
	return ": " + strings.TrimSpace(string(exitErr.Stderr))
}
