package stream

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Timestamp-only complete-PES fixtures. The ADTS headers/frame lengths are
// valid for duration accounting; their dummy AAC/VCL payloads are not decoded.
func outputClockTestProgram(t *testing.T) avProgramPIDs {
	t.Helper()
	data := joinTSPackets(testPATPacket(0x1000), testAVPMTPacket(0x1000, 0x100, 0x101), pcrTestNonPCRPacket(0x1ffe, false), pcrTestNonPCRPacket(0x1ffe, false), pcrTestNonPCRPacket(0x1ffe, false))
	programs := scanTSProgramMaps(data, "")
	if len(programs) != 1 {
		t.Fatal("fixture programme missing")
	}
	p := programs[0]
	return avProgramPIDs{videoPID: p.videoPID, pcrPID: p.pcrPID, audioPIDs: p.audioPIDs, audioCount: p.audioCount, mapIdentity: p.mapIdentity}
}

func outputClockTestRaw(v int64) uint64 {
	v %= int64(ptsModulus)
	if v < 0 {
		v += int64(ptsModulus)
	}
	return uint64(v)
}

func outputClockTestADTS() []byte {
	b := bytes.Repeat([]byte{0x33}, 31)
	copy(b, []byte{0xff, 0xf1, 0x4c, 0x80, 0, 0, 0xfc})
	b[3] |= byte(len(b) >> 11)
	b[4] = byte(len(b) >> 3)
	b[5] = byte(len(b)<<5) | 0x1f
	return b
}

func outputClockTestPES(pid int, pts, dts int64, cc byte, video bool) []byte {
	id := byte(0xc0)
	var content []byte
	if video {
		id = 0xe0
		content = []byte{0, 0, 1, 0x65, 0x88}
	} else {
		content = outputClockTestADTS()
	}
	p := slateTestPES(id, outputClockTestRaw(pts), outputClockTestRaw(dts), video && pts != dts)
	p = append(p[:len(p)-1], content...)
	n := len(p) - 6
	p[4], p[5] = byte(n>>8), byte(n)
	b := publicationTestPacket(pid, true, p, cc&15)
	if video {
		b[5] |= 0x10
		copy(b[6:12], slateTestPCR(outputClockTestRaw(dts)*300))
	}
	return b
}

func outputClockTestGroups(count int, start, composition, audioOffset int64) [][]byte {
	var groups [][]byte
	audioIndex := int64(0)
	for i := 0; i < count; i++ {
		dts := start + int64(i)*3600
		g := outputClockTestPES(0x100, dts+composition, dts, byte(i), true)
		for audioIndex*1920 < int64(i+1)*3600 {
			pts := start + audioOffset + audioIndex*1920
			g = append(g, outputClockTestPES(0x101, pts, pts, byte(audioIndex), false)...)
			audioIndex++
		}
		groups = append(groups, g)
	}
	return groups
}

func TestOutputClockFirstEpochAndOrdinaryBytesStayExact(t *testing.T) {
	program := outputClockTestProgram(t)
	for _, tc := range []struct {
		name   string
		offset int64
		replay bool
	}{
		{"supported", 0, false}, {"constant_1_8s_legacy", 162000, false}, {"finite_replay_legacy", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups := outputClockTestGroups(8, 900000, 0, tc.offset)
			e := newOutputClockEpoch(program, true, tc.replay, groups)
			var c pumpOutputClock
			for _, g := range groups {
				out, next, err := c.prepare(g, e, true)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(out, g) || &out[0] != &g[0] {
					t.Fatal("first native epoch was copied or changed")
				}
				c = next
			}
			if c.enabled != (tc.name == "supported") {
				t.Fatalf("scope enabled=%v", c.enabled)
			}
		})
	}
}

func TestOutputClockRealSlateRealCommonOffsetAndOwnership(t *testing.T) {
	p := outputClockTestProgram(t)
	var c pumpOutputClock
	for epochIndex, groups := range [][][]byte{outputClockTestGroups(8, 900000, 0, 0), outputClockTestGroups(8, 1920, 0, -1920), outputClockTestGroups(8, 480, 3060, -480)} {
		e := newOutputClockEpoch(p, true, false, groups)
		for index, g := range groups {
			before := bytes.Clone(g)
			old := c
			out, next, err := c.prepare(g, e, true)
			if err != nil {
				t.Fatalf("epoch%d group%d: %v", epochIndex, index, err)
			}
			if !bytes.Equal(g, before) {
				t.Fatal("mutated producer-owned group")
			}
			if old.haveVideo && (next.lastDTS < old.lastDTS || next.maxPTS < old.maxPTS || next.lastPCR < old.lastPCR) {
				t.Fatal("output clock reversed")
			}
			if epochIndex > 0 && index == 0 && next.audioEnd <= old.audioEnd {
				t.Fatal("new AAC epoch did not advance past delivered audio")
			}
			parsed, err := inspectOutputClockGroup(g, p, oldNativeForEpoch(old, e))
			if err != nil {
				t.Fatal(err)
			}
			maskedBefore, maskedAfter := bytes.Clone(g), bytes.Clone(out)
			for _, patch := range parsed.patches {
				for _, position := range patch.positions {
					maskedBefore[position] = 0
					maskedAfter[position] = 0
				}
			}
			if !bytes.Equal(maskedBefore, maskedAfter) {
				t.Fatal("common offset changed transport or encoded payload outside clock fields")
			}
			// Preparing and discarding a private candidate must leave the
			// committed cursor and its packet histories unchanged.
			if c.epoch != old.epoch || c.lastDTS != old.lastDTS {
				t.Fatal("prepare committed output progress")
			}
			c = next
		}
	}
}

func oldNativeForEpoch(c pumpOutputClock, e *outputClockEpoch) outputClockNative {
	if c.epoch == e {
		return c.native
	}
	return outputClockNative{}
}

func TestOutputClockWrapAndNegativeAACEnd(t *testing.T) {
	p := outputClockTestProgram(t)
	negative := joinTSPackets(outputClockTestPES(0x100, 0, 0, 0, true), outputClockTestPES(0x101, -4000, -4000, 0, false))
	g, err := inspectOutputClockGroup(negative, p, outputClockNative{})
	if err != nil || g.audioEnd != -2080 {
		t.Fatalf("negative AAC end=%d err=%v", g.audioEnd, err)
	}
	groups := outputClockTestGroups(8, int64(ptsModulus)-7200, 0, 0)
	e := newOutputClockEpoch(p, true, false, groups)
	var c pumpOutputClock
	for _, g := range groups {
		_, next, err := c.prepare(g, e, true)
		if err != nil {
			t.Fatal(err)
		}
		c = next
	}
	if c.maxPTS < int64(ptsModulus) {
		t.Fatalf("33bit wrap was not unwrapped: %d", c.maxPTS)
	}
	returning := outputClockTestGroups(8, 0, 0, 0)
	nextEpoch := newOutputClockEpoch(p, true, false, returning)
	out, next, err := c.prepare(returning[0], nextEpoch, true)
	if err != nil || next.lastPCR <= c.lastPCR {
		t.Fatalf("wrapped join err=%v", err)
	}
	if bytes.Equal(out, returning[0]) {
		t.Fatal("wrapped replacement epoch did not translate")
	}
}

func TestOutputClockRejectsUnsupportedJoinWithoutPrivateCredit(t *testing.T) {
	p := outputClockTestProgram(t)
	groups := outputClockTestGroups(8, 900000, 0, 0)
	e := newOutputClockEpoch(p, true, false, groups)
	var c pumpOutputClock
	for _, g := range groups {
		_, next, err := c.prepare(g, e, true)
		if err != nil {
			t.Fatal(err)
		}
		c = next
	}
	bad := outputClockTestGroups(8, 0, 0, 180000)
	badEpoch := newOutputClockEpoch(p, true, false, bad)
	_, _, err := c.prepare(bad[0], badEpoch, true)
	if !errors.Is(err, errOutputClockBoundary) {
		t.Fatalf("unrepresentable common offset accepted: %v", err)
	}
	if !requiresImmediateContinuityFiller(localTranscodeFailure(err)) || shouldMarkSourceFailure(localTranscodeFailure(err)) {
		t.Fatal("clock refusal lost immediate source-neutral classification")
	}
	good := outputClockTestGroups(8, 0, 0, 0)
	goodEpoch := newOutputClockEpoch(p, true, false, good)
	_, a, err := c.prepare(good[0], goodEpoch, true)
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := c.prepare(good[0], goodEpoch, true)
	if err != nil || a.offset != b.offset {
		t.Fatal("discarded private candidate changed next offset")
	}
}

func TestOutputClockSplitHeaderDuplicatesStayIdentical(t *testing.T) {
	p := outputClockTestProgram(t)
	initial := outputClockTestGroups(8, 900000, 0, 0)
	e := newOutputClockEpoch(p, true, false, initial)
	var c pumpOutputClock
	for _, g := range initial {
		_, next, err := c.prepare(g, e, true)
		if err != nil {
			t.Fatal(err)
		}
		c = next
	}
	for _, split := range []int{11, 16} {
		t.Run(string(rune('a'+split)), func(t *testing.T) {
			pes := append(slateTestPES(0xe0, 3600, 0, true), []byte{0, 0, 1, 0x65, 0x88}...)
			n := len(pes) - 6
			pes[4], pes[5] = byte(n>>8), byte(n)
			first := publicationTestPacket(0x100, true, pes[:split], 0)
			first[5] |= 0x10
			copy(first[6:12], slateTestPCR(0))
			last := publicationTestPacket(0x100, false, pes[split:], 1)
			group := joinTSPackets(first, first, last, last, outputClockTestPES(0x101, 0, 0, 0, false))
			groups := outputClockTestGroups(8, 0, 3600, 0)
			groups[0] = group
			nextEpoch := newOutputClockEpoch(p, true, false, groups)
			out, _, err := c.prepare(group, nextEpoch, true)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out[:188], out[188:376]) || !bytes.Equal(out[376:564], out[564:752]) {
				t.Fatal("physical retransmission clocks differ")
			}
			if bytes.Equal(out, group) {
				t.Fatal("duplicate fixture never translated")
			}
			parsed, err := inspectOutputClockGroup(out, p, outputClockNative{})
			if err != nil || parsed.firstPTS-parsed.firstDTS != 3600 {
				t.Fatalf("split PTS-DTS changed: %+v %v", parsed, err)
			}
		})
	}
}

func TestOutputClockParserKeepsUnselectedPolicyAndRefusesMissingVideoPTS(t *testing.T) {
	p := outputClockTestProgram(t)
	g := outputClockTestGroups(1, 0, 0, 0)[0]
	other := pcrTestNonPCRPacket(0x1ffe, false)
	other[1] |= 0x80
	if _, err := inspectOutputClockGroup(append(bytes.Clone(g), other...), p, outputClockNative{}); err != nil {
		t.Fatalf("unselected TEI became selected clock failure: %v", err)
	}
	bad := bytes.Clone(g)
	payload, _ := tsPayloadStart(bad[:188])
	bad[payload+7] = 0
	if _, err := inspectOutputClockGroup(bad, p, outputClockNative{}); err == nil {
		t.Fatal("clockless video advanced an enabled output floor")
	}
}

func TestOutputClockPublicationPersistsClearAndDeniedDelivery(t *testing.T) {
	p := outputClockTestProgram(t)
	groups := outputClockTestGroups(8, 900000, 0, 0)
	e := newOutputClockEpoch(p, true, false, groups)
	var ss subscriberSet
	ss.init("output-clock-ownership", testLogger())
	viewer, leave := ss.Subscribe("clock-viewer")
	defer leave()
	defer ss.closeAll()
	if err := ss.fanoutClockedReal(groups[0], true, e); err != nil {
		t.Fatal(err)
	}
	select {
	case <-viewer:
	case <-time.After(time.Second):
		t.Fatal("no first group")
	}
	before := ss.outputClock
	ss.clearRing()
	if ss.outputClock.epoch != before.epoch || ss.outputClock.lastDTS != before.lastDTS {
		t.Fatal("clearRing reset delivered clock")
	}
	ss.markStopping()
	if err := ss.fanoutClockedReal(groups[1], false, e); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed admission=%v", err)
	}
	if ss.outputClock.lastDTS != before.lastDTS {
		t.Fatal("denied fanout credited private clock")
	}
}

func TestOutputClockFillerRetainsFetchedGroupAfterFailedTakeover(t *testing.T) {
	p := outputClockTestProgram(t)
	initial := outputClockTestGroups(8, 900000, 0, 0)
	initialEpoch := newOutputClockEpoch(p, true, false, initial)
	slateGroups := outputClockTestGroups(125, 1920, 0, -1920)
	slateData := joinTSPackets(testPATPacket(0x1000), testAVPMTPacket(0x1000, 0x100, 0x101), bytes.Join(slateGroups, nil))
	sl := &slate{data: slateData, duration: 5 * time.Second}
	var ss subscriberSet
	ss.init("clock-filler-resume", testLogger())
	ss.SlateFn = func() *slate { return sl }
	viewer, leave := ss.Subscribe("resume-viewer")
	received := make(chan []byte, 1024)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for b := range viewer {
			received <- b
		}
	}()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }); ss.closeAll(); leave(); <-readerDone }()
	for _, g := range initial {
		if err := ss.fanoutClockedReal(g, false, initialEpoch); err != nil {
			t.Fatal(err)
		}
	}
	if !ss.outputClock.enabled {
		t.Fatal("resume fixture bypassed clock normalization")
	}
	fetched := make(chan context.Context, 1)
	heldOnce := false
	ss.onFillerGroupFetched = func(ctx context.Context) {
		if heldOnce {
			return
		}
		for off := 0; off < len(ss.slatePending); off += 188 {
			packet := ss.slatePending[off : off+188]
			if int(packet[1]&31)<<8|int(packet[2]) != 0x100 || packet[1]&0x40 == 0 {
				continue
			}
			payload, ok := tsPayload(packet)
			if !ok || len(payload) < 14 {
				continue
			}
			if decodeMPEGTimestamp(payload[9:14]) >= 181920 {
				heldOnce = true
				fetched <- ctx
				<-release
				return
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	ss.startRecoveryFiller(ctx, ss.slateAfter)
	var workerCtx context.Context
	select {
	case workerCtx = <-fetched:
	case <-ctx.Done():
		t.Fatal("filler did not reach held two-second group")
	}
	before := ss.outputClock
	epoch := ss.slateClockEpoch
	held := bytes.Clone(ss.slatePending)
	bad := outputClockTestGroups(8, 0, 0, 180000)
	badEpoch := newOutputClockEpoch(p, true, false, bad)
	failed := make(chan error, 1)
	go func() { failed <- ss.fanoutClockedReal(bad[0], true, badEpoch) }()
	select {
	case <-workerCtx.Done():
	case <-ctx.Done():
		t.Fatal("takeover did not cancel held filler")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-failed:
		if !errors.Is(err, errOutputClockBoundary) {
			t.Fatalf("unjoinable real takeover=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("takeover did not join filler")
	}
	if !bytes.Equal(ss.slatePending, held) || ss.slateClockEpoch != epoch || ss.outputClock.lastDTS != before.lastDTS {
		t.Fatal("failed takeover lost pending bytes, epoch or delivered floor")
	}
	if !ss.waitForDeliveryBoundary(ctx, time.Second) {
		t.Fatal("prior filler did not finish delivery")
	}
	for len(received) > 0 {
		<-received
	}
	want, next, err := before.prepare(held, epoch, false)
	if err != nil {
		t.Fatal(err)
	}
	if next.lastDTS-before.lastDTS != 3600 {
		t.Fatalf("retained video cadence=%d ticks", next.lastDTS-before.lastDTS)
	}
	ss.startRecoveryFiller(ctx, ss.slateAfter)
	select {
	case got := <-received:
		if !bytes.Equal(got, want) {
			t.Fatal("resumed filler skipped or changed its unaccepted complete group")
		}
	case <-ctx.Done():
		t.Fatal("filler did not resume after rejected real takeover")
	}
	ss.stopRecoveryFiller()
}

func TestOutputClockOutgoingQueuedPresentationIsExplicitJoinAllowance(t *testing.T) {
	// Exact outgoing tick values from the retained v0.56.7 capture after the
	// endpoint fence withholds the 199,280-byte audio-tail group. A 59.94fps
	// cadence rounds up to 1502 ticks; no unseen picture earns clock progress.
	p := outputClockTestProgram(t)
	c := pumpOutputClock{decided: true, enabled: true, haveVideo: true, haveAudio: true, havePCR: true,
		lastDTS: 74241781, maxPTS: 74249251, audioEnd: 74242980, lastPCR: 74241781 * 300,
		cadence: 1502, audioStep: 1920}
	groups := outputClockTestGroups(8, 1920, 0, -1920)
	e := newOutputClockEpoch(p, true, false, groups)
	g, err := inspectOutputClockGroup(groups[0], p, outputClockNative{})
	if err != nil {
		t.Fatal(err)
	}
	offset, err := c.joinOffset(g, e)
	if err != nil {
		t.Fatalf("83ms of actually delivered queued presentation refused: %v", err)
	}
	if got := g.firstAudio + offset - c.audioEnd; got != 5853 {
		t.Fatalf("explicit AAC join gap=%d ticks, want 5853 (65.033ms)", got)
	}
	// The same endpoint discrepancy with no queued B-frame presentation is
	// not entitled to that allowance. Nor may an actual multi-second deficit
	// be borrowed from the finite outgoing composition lead.
	noQueued := c
	noQueued.lastDTS, noQueued.lastPCR = c.maxPTS, c.maxPTS*300
	if _, err := noQueued.joinOffset(g, e); !errors.Is(err, errOutputClockBoundary) {
		t.Fatalf("unproved endpoint gap accepted: %v", err)
	}
	largeGap := c
	largeGap.audioEnd -= 90000
	if _, err := largeGap.joinOffset(g, e); !errors.Is(err, errOutputClockBoundary) {
		t.Fatalf("one-second audio deficit accepted: %v", err)
	}
	videoGap := c
	videoGap.audioEnd = c.maxPTS + c.cadence + 7200 - g.minPTS + g.firstAudio
	if _, err := videoGap.joinOffset(g, e); !errors.Is(err, errOutputClockBoundary) {
		t.Fatalf("outgoing composition excused a new80ms video hole: %v", err)
	}
	pcrGap := c
	presentationOffset := c.maxPTS + c.cadence - g.minPTS
	pcrGap.lastPCR = (g.firstPCR/300 + presentationOffset + 7200) * 300
	if _, err := pcrGap.joinOffset(g, e); !errors.Is(err, errOutputClockBoundary) {
		t.Fatalf("outgoing composition excused an unrelated80ms PCR hole: %v", err)
	}
}

func TestOutputClockObservedCadenceIncludesJitterWithoutJumpCredit(t *testing.T) {
	if got := observedOutputClockCadence([]int64{1440, 1530, 1440, 1530}); got != 1530 {
		t.Fatalf("16/17ms quantization envelope=%d", got)
	}
	if got := observedOutputClockCadence([]int64{1440, 1530, 540000, 1440}); got != 1530 {
		t.Fatalf("six-second clock jump earned cadence=%d", got)
	}
}

func TestOutputClockPCRCannotEarnIncomingCompositionCredit(t *testing.T) {
	clock := pumpOutputClock{
		haveVideo: true, haveAudio: true, havePCR: true,
		lastDTS: 900000, maxPTS: 900000, audioEnd: 903600,
		lastPCR: 900000 * 300, cadence: 3600, audioStep: 1920,
	}
	group := outputClockGroup{
		haveVideo: true, haveAudio: true, havePCR: true,
		firstDTS: 900000, firstPTS: 909000, minPTS: 909000, maxPTS: 909000, lastDTS: 900000,
		firstAudio: 909000, audioEnd: 910920, audioStep: 1920,
		firstPCR: 900000 * 300, lastPCR: 900000 * 300,
	}
	epoch := &outputClockEpoch{cadence: 3600, minimumPTS: 909000}
	// A real 100ms composition lead with matching decode/PCR phase remains
	// representable. Its decode floor requires only one offset tick.
	if offset, err := clock.joinOffset(group, epoch); err != nil || offset != 1 {
		t.Fatalf("proved incoming composition: offset=%d err=%v", offset, err)
	}
	// Changing only PCR phase by100ms formerly earned more composition credit
	// from the resulting offset, accepting160.011ms video and audio holes.
	group.firstPCR -= 9000 * 300
	group.lastPCR = group.firstPCR
	if _, err := clock.joinOffset(group, epoch); !errors.Is(err, errOutputClockBoundary) {
		t.Fatalf("unrelated PCR phase borrowed incoming composition credit: %v", err)
	}
}

func TestOutputClockBFrameNativeStartEnablesAndRetainsBytes(t *testing.T) {
	p := outputClockTestProgram(t)
	var source [][]byte
	audioIndex := int64(0)
	for i := int64(0); i < 20; i++ {
		dts := 480 + i*1502
		g := outputClockTestPES(0x100, dts+3060, dts, byte(i), true)
		for audioIndex*1920 < dts+1502 {
			g = append(g, outputClockTestPES(0x101, audioIndex*1920, audioIndex*1920, byte(audioIndex), false)...)
			audioIndex++
		}
		source = append(source, g)
	}
	var c pumpOutputClock
	for index, groups := range [][][]byte{source, outputClockTestGroups(8, 1920, 0, -1920), source} {
		e := newOutputClockEpoch(p, true, false, groups)
		for groupIndex, group := range groups {
			out, next, err := c.prepare(group, e, true)
			if err != nil {
				t.Fatalf("epoch%d group%d: %v", index, groupIndex, err)
			}
			if !next.enabled {
				t.Fatal("native34ms composition lead silently bypassed the pump clock")
			}
			if index == 0 && (!bytes.Equal(out, group) || &out[0] != &group[0]) {
				t.Fatal("healthy initial B-frame epoch changed")
			}
			if index > 0 && groupIndex == 0 && (next.lastDTS <= c.lastDTS || next.maxPTS <= c.maxPTS || next.lastPCR <= c.lastPCR) {
				t.Fatal("B-frame/slate transition did not preserve output floors")
			}
			c = next
		}
	}
}

func TestOutputClockRAPTrimmedNativeAudioLeadStaysEnabled(t *testing.T) {
	p := outputClockTestProgram(t)
	// Exact first clocks from the retained native MTV warm-prefix. Timestamp
	// cadence is quantized to16ms here; this test is clock arithmetic, not a
	// claim that the dummy encoded payload is decodable.
	groups := outputClockTestGroups(8, 1081080, 3060, -3540)
	e := newOutputClockEpoch(p, true, false, groups)
	e.cadence = 1440
	var c pumpOutputClock
	out, next, err := c.prepare(groups[0], e, true)
	if err != nil || !next.enabled || &out[0] != &groups[0][0] {
		t.Fatalf("captured RAP/AAC clock origin bypassed or changed: enabled=%v err=%v", next.enabled, err)
	}
}

func TestOutputClockMalformedOrReversedGroupDoesNotAdvance(t *testing.T) {
	p := outputClockTestProgram(t)
	groups := outputClockTestGroups(8, 900000, 0, 0)
	e := newOutputClockEpoch(p, true, false, groups)
	_, c, err := (pumpOutputClock{}).prepare(groups[0], e, true)
	if err != nil || !c.enabled {
		t.Fatal("initial fixture did not enable the clock")
	}
	for _, tc := range []struct {
		name   string
		change func([]byte)
	}{
		{"selected_tei", func(g []byte) { g[1] |= 0x80 }},
		{"selected_scrambling", func(g []byte) { g[3] |= 0x80 }},
		{"pts_marker", func(g []byte) { at, _ := tsPayloadStart(g[:188]); g[at+9] &^= 1 }},
		{"pcr_reverse", func(g []byte) { copy(g[6:12], slateTestPCR(899000*300)) }},
		{"video_reverse", func(g []byte) {
			copy(g[:188], outputClockTestPES(0x100, 899000, 899000, 1, true))
			copy(g[6:12], slateTestPCR(903600*300))
		}},
		{"adts_incomplete", func(g []byte) { at, _ := tsPayloadStart(g[188:376]); g[188+at+14+4]++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(groups[1])
			tc.change(bad)
			out, next, err := c.prepare(bad, e, true)
			if !errors.Is(err, errOutputClockBoundary) || out != nil {
				t.Fatalf("malformed/reversed clock accepted: %v", err)
			}
			if next.lastDTS != c.lastDTS || next.audioEnd != c.audioEnd || next.lastPCR != c.lastPCR {
				t.Fatal("refused private group changed delivered endpoints")
			}
		})
	}
}
