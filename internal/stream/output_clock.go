package stream

import (
	"errors"
)

var errOutputClockBoundary = errors.New("output clock cannot represent a continuous epoch join")

// outputClockEpoch contains private, exact-programme metadata. It never owns
// the delivered clock. Source attempts and slate cursors have distinct
// identities; a restarted worker reuses its cursor's epoch and pending unit.
type outputClockEpoch struct {
	program    avProgramPIDs
	supported  bool
	cadence    int64
	minimumPTS int64
}

func newOutputClockEpoch(program avProgramPIDs, bound, replay bool, groups [][]byte) *outputClockEpoch {
	e := &outputClockEpoch{program: program}
	if !bound || replay || !outputClockProgramSupported(program) {
		return e
	}
	var native outputClockNative
	havePTS := false
	for _, data := range groups {
		g, err := inspectOutputClockGroup(data, program, native)
		if err != nil {
			return e
		}
		native = g.native
		if g.haveVideo {
			if !havePTS || g.minPTS < e.minimumPTS {
				e.minimumPTS = g.minPTS
			}
			havePTS = true
		}
	}
	e.cadence = native.cadence
	e.supported = native.haveVideo && native.haveAudio && native.havePCR && e.cadence > 0
	return e
}

func outputClockProgramSupported(program avProgramPIDs) bool {
	if program.audioCount != 1 || !program.mapIdentity.valid() {
		return false
	}
	s := program.mapIdentity.pmtSection
	if len(s) < 16 || s[0] != 2 {
		return false
	}
	start := 12 + (int(s[10]&15)<<8 | int(s[11]))
	end := len(s) - 4
	video, audio := false, false
	for pos := start; pos+5 <= end; {
		pid := int(s[pos+1]&31)<<8 | int(s[pos+2])
		n := int(s[pos+3]&15)<<8 | int(s[pos+4])
		if pos+5+n > end {
			return false
		}
		if pid == program.videoPID {
			video = s[pos] == 0x1b || s[pos] == 0x24
		}
		if pid == program.firstAudioPID() {
			audio = s[pos] == 0x0f
		}
		pos += 5 + n
	}
	return video && audio && program.videoPID != program.firstAudioPID() && program.pcrPID >= 0 && program.pcrPID < 0x1fff
}

// pumpOutputClock is guarded by subscriberSet.epochDeliveryMu. prepare returns
// owned bytes and a candidate value; only successful fanout commits that value.
// clearRing and per-subscriber replay must never reset or advance it.
type pumpOutputClock struct {
	decided, enabled                   bool
	epoch                              *outputClockEpoch
	native                             outputClockNative
	offset                             int64
	haveVideo, haveAudio, havePCR      bool
	lastDTS, maxPTS, audioEnd, lastPCR int64
	cadence, audioStep                 int64
}

func (c pumpOutputClock) prepare(data []byte, epoch *outputClockEpoch, real bool) ([]byte, pumpOutputClock, error) {
	next := c
	if !c.decided {
		if !real {
			return data, c, nil
		}
		next.decided = true
		if epoch == nil || !epoch.supported {
			return data, next, nil
		}
	} else if !c.enabled {
		return data, c, nil
	}
	if epoch == nil || !epoch.supported {
		return nil, c, outputClockParseError("next programme is outside the established clock scope")
	}
	fresh := c.epoch != epoch
	previous := c.native
	if fresh {
		previous = outputClockNative{}
	}
	g, err := inspectOutputClockGroup(data, epoch.program, previous)
	if err != nil {
		if !c.decided {
			next.enabled = false
			return data, next, nil
		}
		return nil, c, err
	}
	if fresh && (!g.haveVideo || !g.haveAudio || !g.havePCR) {
		if !c.decided {
			next.enabled = false
			return data, next, nil
		}
		return nil, c, outputClockParseError("first complete group lacks joint clocks")
	}
	if !c.decided {
		// Ordinary unsupported initial alignments keep their exact legacy
		// behavior for this pump. Do not make a previously playable constant
		// audio origin (for example +1.8s) permanently fail during recovery.
		bound := epoch.cadence + g.audioStep + 1
		// PCR phase cannot borrow the composition allowance avAligned grants.
		if !g.avAligned(epoch.cadence) || absOutputClock(g.firstDTS*300-g.firstPCR) > bound*300 {
			next.enabled = false
			return data, next, nil
		}
		next.enabled = true
		next.offset = 0
	} else if fresh {
		offset, err := c.joinOffset(g, epoch)
		if err != nil {
			return nil, c, err
		}
		next.offset = offset
	}
	modOffset := next.offset % int64(ptsModulus)
	if modOffset < 0 {
		modOffset += int64(ptsModulus)
	}
	out := data
	if modOffset != 0 {
		out = append([]byte(nil), data...)
		for _, patch := range g.patches {
			if patch.pcr {
				shiftPCR(out[patch.positions[0]:patch.positions[0]+6], uint64(modOffset)*300)
			} else {
				shiftMPEGTimestamp(out, patch.positions, uint64(modOffset))
			}
		}
		// Suppress duplicate semantic samples while preserving every physical
		// duplicate, including copies of a split timestamp header.
		for _, duplicate := range g.duplicates {
			if duplicate.source >= 0 {
				copy(out[duplicate.destination:duplicate.destination+tsPacketSize], out[duplicate.source:duplicate.source+tsPacketSize])
			} else {
				copy(out[duplicate.destination:duplicate.destination+tsPacketSize], duplicate.previous[:])
			}
		}
	}
	for pid, pos := range g.lastPacketPositions {
		p := g.native.packets[pid]
		copy(p.translated[:], out[pos:pos+tsPacketSize])
		g.native.packets[pid] = p
	}
	next.epoch = epoch
	next.native = g.native
	if g.haveVideo {
		if !next.haveVideo {
			next.maxPTS = g.maxPTS + next.offset
		} else {
			next.maxPTS = max(next.maxPTS, g.maxPTS+next.offset)
		}
		next.haveVideo = true
		next.lastDTS = g.lastDTS + next.offset
	}
	if g.haveAudio {
		if !next.haveAudio {
			next.audioEnd = g.audioEnd + next.offset
		} else {
			next.audioEnd = max(next.audioEnd, g.audioEnd+next.offset)
		}
		next.haveAudio = true
		next.audioStep = g.audioStep
	}
	if g.havePCR {
		next.havePCR = true
		next.lastPCR = g.lastPCR + next.offset*300
	}
	next.cadence = max(epoch.cadence, g.native.cadence)
	return out, next, nil
}

func (c pumpOutputClock) joinOffset(g outputClockGroup, epoch *outputClockEpoch) (int64, error) {
	if !c.haveVideo || !c.haveAudio || !c.havePCR {
		return 0, outputClockParseError("delivered epoch lacks joint endpoint")
	}
	cadence := max(c.cadence, epoch.cadence)
	if cadence <= 0 || g.audioStep <= 0 {
		return 0, outputClockParseError("unknown frame cadence")
	}
	// The preview minimum is only a conservative incoming presentation floor;
	// later private frames never supply output progress or an outgoing endpoint.
	minPTS := min(g.minPTS, epoch.minimumPTS)
	presentationFloor := c.maxPTS + c.cadence - minPTS
	audioFloor := c.audioEnd - g.firstAudio
	mediaFloor := max(presentationFloor, audioFloor)
	dtsFloor := c.lastDTS + 1 - g.firstDTS
	pcrFloor := ceilOutputClock(c.lastPCR+1-g.firstPCR, 300)
	offset := max(mediaFloor, max(dtsFloor, pcrFloor))
	// A change in proved PTS-DTS composition lead may need a larger decode
	// interval while previously queued pictures still cover presentation time.
	// Bound unexplained presentation/audio holes, not the raw DTS interval.
	composition := max(int64(0), g.firstPTS-g.firstDTS)
	oldQueued := max(int64(0), c.maxPTS-c.lastDTS)
	// Only the incoming decode requirement can justify composition credit.
	// A later PCR floor must not create its own allowance for a clock-phase
	// change unrelated to queued pictures.
	provedComposition := min(max(int64(0), dtsFloor-mediaFloor), max(int64(0), composition-oldQueued))
	// The outgoing presentation queue was already delivered. Replacing a
	// reordered epoch with bf=0 media must wait for those pictures, which can
	// leave an explicit AAC hole beyond ordinary mux interleave. Permit only
	// that measured composition lead, never an unrelated audio deficit.
	allowed := cadence + max(c.audioStep, g.audioStep) + 1 + provedComposition
	audioAllowed := allowed + oldQueued
	videoGap := minPTS + offset - (c.maxPTS + c.cadence)
	audioGap := g.firstAudio + offset - c.audioEnd
	// The delivered epoch can itself end with its audio behind its video: an
	// attempt cut off mid-interleave, or killed by the output A/V drift guard
	// after its audio clock lost samples (measured 0.17-3.1 s). A continuous
	// video join must then leave that delivered deficit as an audio hole. It is
	// history, not a new gap, and refusing it refuses every later attempt and
	// the slate alike, because none of them can change what was delivered: the
	// pump stays dark until its reconnect budget expires. Credit exactly that
	// deficit, and only to an incoming epoch whose own audio starts with its
	// video, so a misaligned source still cannot carry its offset into the
	// output.
	var deliveredDeficit int64
	if g.avAligned(cadence) {
		deliveredDeficit = max(0, c.maxPTS+c.cadence-c.audioEnd)
	}
	if videoGap < 0 || audioGap < 0 || videoGap > allowed || audioGap > audioAllowed+deliveredDeficit {
		return 0, outputClockParseError("A/V presentation endpoints cannot share a bounded offset")
	}
	// PCR phase changes cannot borrow an unrelated B-frame allowance.
	if pcrFloor > mediaFloor+allowed {
		return 0, outputClockParseError("PCR phase cannot share the presentation offset")
	}
	return offset, nil
}

func absOutputClock(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
func ceilOutputClock(v, divisor int64) int64 {
	q := v / divisor
	if v%divisor > 0 {
		q++
	}
	return q
}

// avAligned reports whether a group's first AAC starts within one picture
// cadence and one audio step of its first picture's decode time. A B-frame
// composition lead is not a different audio origin, so the proved lead is
// allowed on top; a RAP-trimmed prefix may retain matching earlier AAC.
func (g outputClockGroup) avAligned(cadence int64) bool {
	composition := max(int64(0), g.firstPTS-g.firstDTS)
	return absOutputClock(g.firstAudio-g.firstDTS) <= cadence+g.audioStep+1+composition
}
