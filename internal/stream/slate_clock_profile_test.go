package stream

import (
	"context"
	"testing"
	"time"
)

// A clock-continuous recovery splice needs the generated filler to share the
// live mux's decode/PCR phase, with audio published close to its video. Prove
// the rendered transport, rather than just asserting FFmpeg argument strings.
func TestGeneratedSlateHasImmediateMuxClockAndAudio(t *testing.T) {
	ffmpeg, _ := requireAudioOriginTools(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sl, err := generateSlate(ctx, ffmpeg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	selection, ok, err := selectCheckedInputProgram(sl.data)
	if err != nil || !ok || selection.program.audioCount != 1 {
		t.Fatalf("generated selected program: ok=%v err=%v", ok, err)
	}
	videoPID := uint16(selection.program.videoPID)
	clocks := slateTestAllPESClocks(sl.data)
	video := clocks[videoPID]
	if len(video) != 125 {
		t.Fatalf("generated video PES=%d, want 125", len(video))
	}
	for i, clock := range video {
		if clock.hasDTS && clock.dts != clock.pts {
			t.Fatalf("slate reorders video at %d: PTS=%d DTS=%d", i, clock.pts, clock.dts)
		}
		if i > 0 && clock.pts-video[i-1].pts != 3600 {
			t.Fatalf("slate video cadence at %d is %d ticks", i, clock.pts-video[i-1].pts)
		}
	}
	firstPCR := slateTestPCRClock(t, sl.data, uint16(selection.program.pcrPID))
	if firstPCR != video[0].pts*300 {
		t.Fatalf("slate PCR/decode phase differs: PCR=%d/27MHz first decode=%d/90kHz", firstPCR, video[0].pts)
	}
	pcrSamples := 0
	for offset := 0; offset < len(sl.data); offset += tsPacketSize {
		packet := sl.data[offset : offset+tsPacketSize]
		pid := int(packet[1]&31)<<8 | int(packet[2])
		if pid != selection.program.pcrPID || packet[3]&0x20 == 0 || packet[4] < 7 || packet[5]&0x10 == 0 {
			continue
		}
		payload, ok := tsPayloadStart(packet)
		if !ok || packet[1]&0x40 == 0 {
			t.Fatal("generated slate PCR is not attached to its video PES")
		}
		pts, ok := parsePESPTS(packet[payload:])
		if !ok || decodePCR(packet[6:12]) != pts*300 {
			t.Fatalf("generated slate changes PCR/decode phase at packet %d", offset/tsPacketSize)
		}
		pcrSamples++
	}
	if pcrSamples < 2 {
		t.Fatal("generated slate did not provide repeated PCR evidence")
	}
	audio := clocks[uint16(selection.program.audioPIDs[0])]
	// Zero mux delay publishes each 48 kHz AAC access unit promptly. Include
	// priming/padding in the encoded frame count and verify the actual end.
	if len(audio) != 234 || audio[0].pts != 0 || audio[len(audio)-1].pts+1920 != 449280 {
		t.Fatalf("generated AAC clocks/count differ: count=%d clocks=%v", len(audio), audio)
	}
	for i := 1; i < len(audio); i++ {
		if audio[i].pts-audio[i-1].pts != 1920 {
			t.Fatalf("generated AAC clock step at %d is %d", i, audio[i].pts-audio[i-1].pts)
		}
	}

	cursor := newSlateLoopCursor(sl)
	var timeline transportAVTimeline
	now := time.Unix(1700000000, 0)
	timeline.bindSelectedProgram(selection.program, now)
	var duration time.Duration
	for groups := 0; duration < 2*sl.duration && groups < 2000; groups++ {
		group, cadence, err := cursor.nextComplete()
		if err != nil || len(group) == 0 {
			t.Fatalf("slate complete group: bytes=%d err=%v", len(group), err)
		}
		if cadence > 80*time.Millisecond {
			t.Fatalf("slate group represents an avoidable burst: %s", cadence)
		}
		timeline.feedAt(group, now.Add(duration))
		snapshot := timeline.snapshotAt(now.Add(duration))
		if snapshot.videoSamples > 0 && snapshot.audioSamples > 0 && absoluteDuration(snapshot.finalSkew) > 80*time.Millisecond {
			t.Fatalf("published slate group leaves audio/video endpoints apart: %s", snapshot.finalSkew)
		}
		duration += cadence
	}
	if duration < 2*sl.duration {
		t.Fatalf("slate groups did not advance the complete two-loop clock: %s", duration)
	}
	decodeRecoveryMedia(t, ffmpeg, sl.data)
}
