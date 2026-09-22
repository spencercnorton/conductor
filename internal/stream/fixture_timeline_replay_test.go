package stream

// Fixture replay harness: feed a captured REAL provider stream through the
// production input timeline exactly as the transcode input guard configures it,
// and report what it counts. Run with:
//   CONDUCTOR_FIXTURE=/path/to/fixtures/tlc-2audio-raw-90s.ts go test -run TestFixtureReplayTimeline -v
// Skips when the fixture env var is unset so CI is unaffected.

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestFixtureReplayTimeline(t *testing.T) {
	path := os.Getenv("CONDUCTOR_FIXTURE")
	if path == "" {
		t.Skip("CONDUCTOR_FIXTURE not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	total := (len(data) / tsPacketSize) * tsPacketSize
	// TLC fixture layout (ffprobe): video 0x100, audio 0x101+0x102, dvb_sub 0x103.
	program := avProgramPIDs{videoPID: 0x100, pcrPID: 0x100, audioCount: 2}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = 0x101
	program.audioPIDs[1] = 0x102

	base := time.Unix(1_700_000_000, 0)
	var m transportAVTimeline
	m.requireFirstAudio = true // transcode input guard configuration (transcode_streamer.go:625)
	m.bindSelectedProgram(program, base)

	policy := defaultAVContinuityPolicy()
	captureWall := 90 * time.Second // wall-clock duration of the capture
	const chunk = 348 * tsPacketSize

	firstFaultAt := time.Duration(-1)
	faultCount := 0
	var firstFault string
	for off := 0; off < total; off += chunk {
		end := off + chunk
		if end > total {
			end = total
		}
		at := base.Add(time.Duration(float64(off) / float64(total) * float64(captureWall)))
		if f := m.feedAtAndReportFault(data[off:end], at); f != nil {
			faultCount++
			if firstFaultAt < 0 {
				firstFaultAt = at.Sub(base)
				firstFault = string(f.kind)
			}
		}
		if f := m.continuityFault(at, policy); f != nil {
			faultCount++
			if firstFaultAt < 0 {
				firstFaultAt = at.Sub(base)
				firstFault = "policy:" + string(f.kind)
			}
		}
	}
	snap := m.snapshotAt(base.Add(captureWall))
	fmt.Printf("fixture=%s bytes=%d\n", path, total)
	fmt.Printf("video_samples=%d audio_samples=%d audio_tracks=%d\n",
		snap.videoSamples, snap.audioSamples, snap.audioTracks)
	fmt.Printf("continuity_enforced=%v reason=%s corruptions=%d\n",
		snap.continuityEnforced, snap.continuityReason, snap.transportCorruptions)
	fmt.Printf("initial_skew=%s final_skew=%s drift=%s\n",
		snap.initialSkew, snap.finalSkew, snap.drift)
	fmt.Printf("video_last_gap=%s audio_last_gap=%s\n", snap.videoLastGap, snap.audioLastGap)
	fmt.Printf("faults=%d first_fault=%q at=%s\n", faultCount, firstFault, firstFaultAt)
	// Ground truth from ffprobe on this fixture: 7191 video frames, 2810 PES
	// packets on audio 0x101, largest real audio gap 43ms. If the timeline
	// disagrees wildly, the undercount is reproduced offline.
}
