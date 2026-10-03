package stream

import (
	"errors"
	"testing"
)

// outputClockDriftGroups is a delivered epoch whose contiguous AAC clock loses
// ground on its video: the audio is aligned at the first group and ends lag
// ticks behind the last picture, as a transcode's output does when it drops
// sub-frame audio or an attempt is cut off mid-interleave.
func outputClockDriftGroups(count int, start, lag int64) [][]byte {
	var groups [][]byte
	audioIndex := int64(0)
	for i := 0; i < count; i++ {
		dts := start + int64(i)*3600
		g := outputClockTestPES(0x100, dts, dts, byte(i), true)
		due := int64(i+1)*3600 - lag*int64(i+1)/int64(count)
		for audioIndex*1920 < due {
			pts := start + audioIndex*1920
			g = append(g, outputClockTestPES(0x101, pts, pts, byte(audioIndex), false)...)
			audioIndex++
		}
		groups = append(groups, g)
	}
	return groups
}

// Recorded refusal loops. Each began when an attempt ended with its delivered
// audio behind its delivered video. Every later attempt, on the same origin and
// on a distinct backup, started with its audio 9-71 ms from its video, and the
// output clock still refused them all until the 90 s reconnect budget ran out.
// The recordings on those channels failed, and so did the slate, which is just
// as aligned. The incoming epochs here start 37 ms apart, the median of the
// 2026-09-26 respawns.
func TestOutputClockAlignedEpochRejoinsDeliveredImbalance(t *testing.T) {
	p := outputClockTestProgram(t)
	for _, tc := range []struct {
		name string
		lag  int64 // delivered audio end behind delivered video end
	}{
		{"cut_mid_interleave_170ms", 15300}, // 2026-09-23, unexpected EOF
		{"cut_mid_interleave_239ms", 21510}, // 2026-09-17, unexpected EOF
		{"output_av_drift_3075ms", 276750},  // 2026-09-26, output A/V drift fault
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivered := outputClockDriftGroups(240, 900000, tc.lag)
			e := newOutputClockEpoch(p, true, false, delivered)
			var c pumpOutputClock
			for _, g := range delivered {
				_, next, err := c.prepare(g, e, true)
				if err != nil {
					t.Fatal(err)
				}
				c = next
			}
			if !c.enabled {
				t.Fatal("delivered epoch did not enable the output clock")
			}
			imbalance := c.maxPTS + c.cadence - c.audioEnd
			if imbalance < tc.lag-1920 || imbalance > tc.lag+1920 {
				t.Fatalf("fixture delivered imbalance=%d ticks, want %d", imbalance, tc.lag)
			}

			for attempt, incoming := range [][][]byte{
				outputClockTestGroups(8, 0, 0, -3330),         // the restarted transcode
				outputClockTestGroups(8, 5_000_000, 0, -3330), // the next one, or the slate
			} {
				ie := newOutputClockEpoch(p, true, false, incoming)
				g, err := inspectOutputClockGroup(incoming[0], p, outputClockNative{})
				if err != nil {
					t.Fatal(err)
				}
				old := c
				_, next, err := c.prepare(incoming[0], ie, true)
				if err != nil {
					t.Fatalf("attempt %d: aligned epoch refused after a %d-tick delivered imbalance: %v", attempt, tc.lag, err)
				}
				// Video continues on the delivered picture grid: no new hole.
				if next.maxPTS != old.maxPTS+old.cadence {
					t.Fatalf("attempt %d: video joined at %d, want %d", attempt, next.maxPTS, old.maxPTS+old.cadence)
				}
				// The audio hole is the delivered imbalance plus the incoming
				// epoch's own 37 ms lead, nothing more.
				if hole := g.firstAudio + next.offset - old.audioEnd; hole != old.maxPTS+old.cadence-old.audioEnd-3330 {
					t.Fatalf("attempt %d: audio hole=%d ticks, want %d", attempt, hole, old.maxPTS+old.cadence-old.audioEnd-3330)
				}
				c = next
				for _, rest := range incoming[1:] {
					if _, next, err = c.prepare(rest, ie, true); err != nil {
						t.Fatal(err)
					}
					c = next
				}
			}
		})
	}
}

// The credit is only for an incoming epoch that is itself aligned: a source
// whose own audio sits seconds from its video must still be refused, however
// far apart the delivered endpoints ended.
func TestOutputClockMisalignedEpochGetsNoImbalanceCredit(t *testing.T) {
	p := outputClockTestProgram(t)
	delivered := outputClockDriftGroups(240, 900000, 276750)
	e := newOutputClockEpoch(p, true, false, delivered)
	var c pumpOutputClock
	for _, g := range delivered {
		_, next, err := c.prepare(g, e, true)
		if err != nil {
			t.Fatal(err)
		}
		c = next
	}
	for _, audioOffset := range []int64{180000, -180000} {
		bad := outputClockTestGroups(8, 0, 0, audioOffset)
		if _, _, err := c.prepare(bad[0], newOutputClockEpoch(p, true, false, bad), true); !errors.Is(err, errOutputClockBoundary) {
			t.Fatalf("epoch with its own %d-tick A/V offset accepted: %v", audioOffset, err)
		}
	}
}
