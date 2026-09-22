package stream

// Offline replay of saved audio-origin refusal heads through the private
// probe, to see which constraint kept refusing. Env-gated: the fixtures are
// the .ts files conductor persisted under CONDUCTOR_DIAG_DIR on refusal.
//
//	CONDUCTOR_AUDIO_ORIGIN_FIXTURES=/path/to/audio-origin-fixtures \
//	go test ./internal/stream/ -run TestFixtureAudioOriginRefusalHeads -v

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFixtureAudioOriginRefusalHeads(t *testing.T) {
	dir := os.Getenv("CONDUCTOR_AUDIO_ORIGIN_FIXTURES")
	if dir == "" {
		t.Skip("CONDUCTOR_AUDIO_ORIGIN_FIXTURES not set")
	}
	files, err := filepath.Glob(filepath.Join(dir, "audio-origin-refusal-*.ts"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no fixtures in %s: %v", dir, err)
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		probe := &audioOriginProbe{}
		wall := time.Unix(1_700_000_000, 0)
		var ready bool
		var correction time.Duration
		var probeErr error
		decidedAt := -1
		for off := 0; off < len(data); off += chunkSize {
			end := min(off+chunkSize, len(data))
			wall = wall.Add(20 * time.Millisecond)
			ready, correction, probeErr = probe.push(data[off:end], wall)
			if ready || probeErr != nil {
				decidedAt = end
				break
			}
		}
		probe.timeline.mu.Lock()
		v := &probe.timeline.video
		a := &probe.timeline.audio[0]
		fmt.Printf("%s bytes=%d bound=%v video_pid=%#x audio_pids=%d first_audio=%#x | video: samples=%d span=%s steps=%v/%v have=%v pes_active=%v | audio: samples=%d span=%s steps=%v/%v have=%v pes_active=%v | corrupt=%d shared=%d gathering=%v pairs=%d | decision: ready=%v corr=%s err=%v at=%d\n",
			filepath.Base(path), len(data), probe.bound, probe.program.videoPID, probe.program.audioCount, probe.program.firstAudioPID(),
			v.samples, ticksSignedDuration(v.spanTicks()), ticksSignedDuration(v.minStepTicks), ticksSignedDuration(v.maxStepTicks), v.stepHave, probe.timeline.videoPES.active,
			a.samples, ticksSignedDuration(a.spanTicks()), ticksSignedDuration(a.minStepTicks), ticksSignedDuration(a.maxStepTicks), a.stepHave, probe.timeline.audioPES[0].active,
			probe.timeline.transportCorruptions, probe.timeline.sharedBoundaries, probe.timeline.boundaryGathering, probe.pairs.count,
			ready, correction, probeErr, decidedAt)
		if probe.bound && v.samples > 0 && a.samples > 0 {
			fmt.Printf("    initial=%s final=%s drift=%s\n",
				ptsSignedDuration(a.minRaw, v.minRaw), ptsSignedDuration(a.maxRaw, v.maxRaw),
				ticksSignedDuration(a.spanTicks()-v.spanTicks()))
		}
		probe.timeline.mu.Unlock()
	}
}
