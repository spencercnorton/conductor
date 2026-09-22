package stream

// Stage-by-stage accounting for one audio PID of a captured real stream:
// where do 4 of 5 audio PTS clocks go?

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestFixtureAudioStageHistogram(t *testing.T) {
	path := os.Getenv("CONDUCTOR_FIXTURE")
	if path == "" {
		t.Skip("CONDUCTOR_FIXTURE not set")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	total := (len(data) / tsPacketSize) * tsPacketSize

	for _, pid := range []int{0x101, 0x102, 0x100} {
		var cc tsPayloadContinuity
		var pes pesPTSAssembler
		var track ptsTimelineTrack
		stats := map[string]int{}
		base := time.Unix(1_700_000_000, 0)
		npk, pusi := 0, 0
		for off := 0; off+tsPacketSize <= total; off += tsPacketSize {
			p := data[off : off+tsPacketSize]
			if p[0] != 0x47 || int(p[1]&0x1f)<<8|int(p[2]) != pid {
				continue
			}
			npk++
			if p[1]&0x40 != 0 {
				pusi++
			}
			at := base.Add(time.Duration(off) * 90 * time.Second / time.Duration(total))
			accepted, duplicate, corrupt := cc.accept(p)
			switch {
			case corrupt:
				stats["cc_corrupt"]++
				continue
			case duplicate:
				stats["cc_duplicate"]++
				continue
			case !accepted:
				stats["cc_not_accepted"]++
				continue
			}
			payload, ok := tsPayload(p)
			if !ok {
				stats["no_payload"]++
				continue
			}
			pts, status := pes.feed(payload, p[1]&0x40 != 0)
			switch status {
			case pesPTSIncomplete:
				stats["pes_incomplete"]++
			case pesPTSAbsent:
				stats["pes_absent"]++
			case pesPTSMalformed:
				stats["pes_malformed"]++
			case pesPTSPresent:
				if track.recordAt(pts, at) {
					stats["recorded"]++
				} else {
					stats["record_rejected"]++
				}
			}
		}
		fmt.Printf("PID 0x%03x: packets=%d pusi(PES starts)=%d samples_recorded=%d\n", pid, npk, pusi, track.samples)
		for _, k := range []string{"recorded", "record_rejected", "pes_incomplete", "pes_absent", "pes_malformed", "cc_not_accepted", "cc_duplicate", "cc_corrupt", "no_payload"} {
			if stats[k] > 0 {
				fmt.Printf("    %-18s %d\n", k, stats[k])
			}
		}
	}
}
