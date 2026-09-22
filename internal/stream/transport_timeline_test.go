package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

func TestParsePESPTSStatusStrictlyValidatesTimestampClaims(t *testing.T) {
	const wantPTS = uint64(17 * transportPTSRate)
	clone := func(payload []byte) []byte { return append([]byte(nil), payload...) }
	assert := func(name string, payload []byte, wantStatus pesPTSParseStatus) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			gotPTS, gotStatus := parsePESPTSStatus(payload)
			if gotStatus != wantStatus {
				t.Fatalf("parse status=%d, want %d (payload=%x)", gotStatus, wantStatus, payload)
			}
			if wantStatus == pesPTSPresent && gotPTS != wantPTS {
				t.Fatalf("PTS=%d, want %d", gotPTS, wantPTS)
			}
		})
	}

	ptsOnly := slateTestPES(0xc0, wantPTS, 0, false)
	ptsDTS := slateTestPES(0xe0, wantPTS, wantPTS-transportPTSRate/25, true)
	assert("valid PTS-only prefix and markers", ptsOnly, pesPTSPresent)
	assert("valid PTS+DTS prefixes and markers", ptsDTS, pesPTSPresent)

	headerOnly := clone(ptsOnly[:len(ptsOnly)-1])
	headerOnly[4], headerOnly[5] = 0, 8 // control(3) + header_data(5), no ES bytes
	assert("bounded header-only PES is absence, not progress", headerOnly, pesPTSAbsent)
	assert("unbounded PES waits for its first ES byte", ptsOnly[:len(ptsOnly)-1], pesPTSIncomplete)

	flags00 := clone(ptsOnly)
	flags00[7] = 0
	flags00[8] = 0
	assert("flags 00 is legitimate no-PTS", flags00, pesPTSAbsent)
	longFlags00 := []byte{0, 0, 1, 0xc0, 0, 24, 0x80, 0, 20}
	longFlags00 = append(longFlags00, make([]byte, 20)...)
	longFlags00 = append(longFlags00, 0xaa)
	assert("flags 00 waits for complete declared header", longFlags00[:20], pesPTSIncomplete)
	assert("complete flags 00 header with ES is absence", longFlags00, pesPTSAbsent)

	badStartCode := clone(ptsOnly)
	badStartCode[2] = 0x02
	assert("selected PUSI requires PES start code", badStartCode, pesPTSMalformed)

	unsupportedMPEG1 := clone(ptsOnly)
	unsupportedMPEG1[6] = 0x40
	assert("non-MPEG-2 optional header is privately unsupported", unsupportedMPEG1, pesPTSMalformed)

	flags01 := clone(ptsOnly)
	flags01[7] = 0x40
	assert("reserved flags 01 is malformed", flags01, pesPTSMalformed)

	shortPTSHeader := clone(ptsOnly)
	shortPTSHeader[8] = 4
	assert("claimed PTS header shorter than five bytes", shortPTSHeader, pesPTSMalformed)
	shortEmptyPTS := clone(ptsOnly[:13])
	shortEmptyPTS[4], shortEmptyPTS[5] = 0, 7
	shortEmptyPTS[8] = 4
	assert("empty PES cannot excuse short PTS claim", shortEmptyPTS, pesPTSMalformed)

	badPTSPrefix := clone(ptsOnly)
	badPTSPrefix[9] = badPTSPrefix[9]&0x0f | 0x30
	assert("PTS-only field requires 0010 prefix", badPTSPrefix, pesPTSMalformed)

	badPTSMarker := clone(ptsOnly)
	badPTSMarker[11] &^= 0x01
	assert("PTS field requires all marker bits", badPTSMarker, pesPTSMalformed)

	shortDTSHeader := clone(ptsDTS)
	shortDTSHeader[8] = 5
	assert("claimed PTS+DTS header shorter than ten bytes", shortDTSHeader, pesPTSMalformed)
	shortEmptyDTS := clone(ptsDTS[:18])
	shortEmptyDTS[4], shortEmptyDTS[5] = 0, 12
	shortEmptyDTS[8] = 9
	assert("empty PES cannot excuse short PTS+DTS claim", shortEmptyDTS, pesPTSMalformed)
	assert("split PTS+DTS header remains incomplete", ptsDTS[:len(ptsDTS)-1], pesPTSIncomplete)

	badDTSPrefix := clone(ptsDTS)
	badDTSPrefix[14] = badDTSPrefix[14]&0x0f | 0x20
	assert("DTS field requires 0001 prefix", badDTSPrefix, pesPTSMalformed)

	badDTSMarker := clone(ptsDTS)
	badDTSMarker[18] &^= 0x01
	assert("DTS field requires all marker bits", badDTSMarker, pesPTSMalformed)

	grossDTS := slateTestPES(0xe0, wantPTS, wantPTS+9*60*transportPTSRate, true)
	assert("incident-scale PTS DTS separation is malformed", grossDTS, pesPTSMalformed)

	t.Run("normal decode reorder across 33-bit wrap remains valid", func(t *testing.T) {
		const wrapPTS = uint64(1000)
		payload := slateTestPES(0xe0, wrapPTS, ptsModulus-2000, true)
		got, status := parsePESPTSStatus(payload)
		if status != pesPTSPresent || got != wrapPTS {
			t.Fatalf("wrap reorder=(PTS=%d status=%d), want (%d,present)", got, status, wrapPTS)
		}
	})
}

func TestTransportAVTimelineAssemblesPESHeadersAcrossSafeBoundaries(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_000, 0)
	programMap := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		pcrTestNonPCRPacket(0x1fff, false),
		pcrTestNonPCRPacket(0x1fff, false))
	videoPES := slateTestPES(0xe0, transportPTSRate, transportPTSRate-3600, true)
	audioPES := slateTestPES(0xc0, transportPTSRate, 0, false)

	t.Run("valid optional headers can straddle TS packets", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(
			programMap,
			testExactPayloadPacket(videoPID, true, 0, videoPES[:8]),
			testExactPayloadPacket(audioPID, true, 0, audioPES[:8])), base)
		if snapshot := timeline.snapshotAt(base); snapshot.videoSamples != 0 ||
			snapshot.audioSamples != 0 || snapshot.transportCorruptions != 0 {
			t.Fatalf("incomplete headers counted as progress/corruption: %+v", snapshot)
		}
		timeline.feedAt(joinTSPackets(
			testExactPayloadPacket(videoPID, false, 1, videoPES[8:]),
			testExactPayloadPacket(audioPID, false, 1, audioPES[8:])), base.Add(10*time.Millisecond))
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 2*transportPTSRate, 2),
			testTimestampedPESPacket(audioPID, 0xc0, 2*transportPTSRate, 2)), base.Add(20*time.Millisecond))
		if snapshot := timeline.snapshotAt(base.Add(20 * time.Millisecond)); !snapshot.valid ||
			snapshot.videoSamples != 2 || snapshot.audioSamples != 2 ||
			snapshot.transportCorruptions != 0 || snapshot.drift != 0 {
			t.Fatalf("split valid headers snapshot=%+v", snapshot)
		}
	})

	t.Run("new PUSI superseding an incomplete prior header is corrupt", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(
			programMap,
			testExactPayloadPacket(audioPID, true, 0, audioPES[:8])), base)
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 1),
			testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 0)), base.Add(10*time.Millisecond))
		if snapshot := timeline.snapshotAt(base.Add(10 * time.Millisecond)); snapshot.audioSamples != 0 ||
			snapshot.videoSamples != 1 || snapshot.transportCorruptions != 1 {
			t.Fatalf("replacement PUSI did not fence the abandoned partial header: %+v", snapshot)
		}
	})

	t.Run("adaptation-only packet neither appends nor refreshes progress", func(t *testing.T) {
		var timeline transportAVTimeline
		adaptationOnly := bytes.Repeat([]byte{0xff}, tsPacketSize)
		adaptationOnly[0] = 0x47
		adaptationOnly[1] = byte(audioPID >> 8)
		adaptationOnly[2] = byte(audioPID & 0xff)
		adaptationOnly[3] = 0x20
		adaptationOnly[4] = 183
		adaptationOnly[5] = 0
		timeline.feedAt(joinTSPackets(
			programMap,
			testExactPayloadPacket(audioPID, true, 0, audioPES[:8])), base)
		timeline.feedAt(adaptationOnly, base.Add(time.Second))
		if snapshot := timeline.snapshotAt(base.Add(time.Second)); snapshot.audioSamples != 0 ||
			snapshot.transportCorruptions != 0 {
			t.Fatalf("adaptation-only packet changed PES progress: %+v", snapshot)
		}
		timeline.feedAt(testExactPayloadPacket(audioPID, false, 1, audioPES[8:]), base.Add(2*time.Second))
		if snapshot := timeline.snapshotAt(base.Add(2 * time.Second)); snapshot.audioSamples != 1 ||
			snapshot.transportCorruptions != 0 {
			t.Fatalf("adaptation-only packet broke retained assembly: %+v", snapshot)
		}
	})

	for _, tc := range []struct {
		name        string
		trigger     func() []byte
		corruptions uint64
	}{
		{name: "continuity gap resets assembly", corruptions: 1, trigger: func() []byte {
			return testExactPayloadPacket(audioPID, false, 2, audioPES[8:])
		}},
		{name: "TEI resets assembly", corruptions: 1, trigger: func() []byte {
			packet := testExactPayloadPacket(audioPID, false, 1, audioPES[8:])
			packet[1] |= 0x80
			return packet
		}},
		{name: "scrambling resets assembly", corruptions: 1, trigger: func() []byte {
			packet := testExactPayloadPacket(audioPID, false, 1, audioPES[8:])
			packet[3] |= 0x80
			return packet
		}},
		{name: "declared discontinuity resets assembly", corruptions: 0, trigger: func() []byte {
			return joinTSPackets(
				testAdaptationOnlyDiscontinuity(audioPID, 0),
				testExactPayloadPacket(audioPID, false, 1, audioPES[8:]))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var timeline transportAVTimeline
			timeline.feedAt(joinTSPackets(
				programMap,
				testExactPayloadPacket(audioPID, true, 0, audioPES[:8])), base)
			timeline.feedAt(tc.trigger(), base.Add(10*time.Millisecond))
			if snapshot := timeline.snapshotAt(base.Add(10 * time.Millisecond)); snapshot.audioSamples != 0 || snapshot.transportCorruptions != tc.corruptions {
				t.Fatalf("reset boundary consumed partial PES: %+v", snapshot)
			}
			// A complete new PUSI pair must recover cleanly without incorporating
			// bytes or timestamps from the abandoned optional header.
			timeline.feedAt(joinTSPackets(
				testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 3),
				testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 3)), base.Add(20*time.Millisecond))
			if fault := timeline.continuityFault(base.Add(20*time.Millisecond), defaultAVContinuityPolicy()); fault != nil {
				t.Fatalf("fresh PUSI failed to recover abandoned assembly: %v", fault)
			}
		})
	}
}

func TestTransportAVTimelineMeasuresIncidentScaleSlopeAcrossByteSplits(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		pcrTestNonPCRPacket(0x1fff, false),
		testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 1),
		testTimestampedPESPacket(audioPID, 0xc0, 14*transportPTSRate, 1),
	)

	var timeline transportAVTimeline
	for offset := 0; offset < len(data); {
		end := min(offset+137, len(data))
		timeline.feed(data[offset:end])
		offset = end
	}
	snapshot := timeline.snapshot()
	if !snapshot.valid || snapshot.videoSamples != 2 || snapshot.audioSamples != 2 {
		t.Fatalf("timeline snapshot=%+v, want two PTS samples per stream", snapshot)
	}
	if snapshot.initialSkew != 0 || snapshot.finalSkew != 4*time.Second || snapshot.drift != 4*time.Second {
		t.Fatalf("timeline initial=%s final=%s drift=%s, want 0s/4s/4s",
			snapshot.initialSkew, snapshot.finalSkew, snapshot.drift)
	}

	beforeEvents := metrics.InputAVDriftEvents.Value()
	beforeMillis := metrics.InputAVDriftMilliseconds.Value()
	recordAVTimelineMetrics(snapshot, avTimelineSnapshot{})
	if delta := metrics.InputAVDriftEvents.Value() - beforeEvents; delta != 1 {
		t.Fatalf("input drift events delta=%d, want 1", delta)
	}
	if delta := metrics.InputAVDriftMilliseconds.Value() - beforeMillis; delta != 4000 {
		t.Fatalf("input drift milliseconds delta=%d, want 4000", delta)
	}
}

func TestTransportAVContinuityRequiresEstablishedTimestampedMedia(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_000, 0)
	initial := [][]byte{testPATPacket(pmtPID), testAVPMTPacket(pmtPID, videoPID, audioPID)}
	for i := 0; i < int(policy.minSamples); i++ {
		pts := uint64(i) * transportPTSRate / 10
		initial = append(initial,
			testTimestampedPESPacket(videoPID, 0xe0, pts, byte(i)),
			testTimestampedPESPacket(audioPID, 0xc0, pts, byte(i)))
	}

	t.Run("audio PES starvation while video advances", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(initial...), base)
		first := base.Add(policy.starvationThreshold + 10*time.Millisecond)
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, 3*transportPTSRate, 4), first)
		if fault := timeline.continuityFault(first, policy); fault != nil {
			t.Fatalf("unconfirmed audio starvation fault=%v", fault)
		}
		confirmed := first.Add(policy.confirmation + time.Millisecond)
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, 4*transportPTSRate, 5), confirmed)
		fault := timeline.continuityFault(confirmed, policy)
		if fault == nil || fault.kind != avFaultAudioStarvation || !errors.Is(fault, errAVContinuity) {
			t.Fatalf("confirmed audio starvation fault=%v, want %s", fault, avFaultAudioStarvation)
		}
	})

	t.Run("both A/V clocks stop while null and PSI bytes continue", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(initial...), base)
		first := base.Add(policy.starvationThreshold + 10*time.Millisecond)
		timeline.feedAt(joinTSPackets(
			pcrTestNonPCRPacket(0x1fff, false), testPATPacket(pmtPID)), first)
		if fault := timeline.continuityFault(first, policy); fault != nil {
			t.Fatalf("unconfirmed clock starvation fault=%v", fault)
		}
		confirmed := first.Add(policy.confirmation + time.Millisecond)
		timeline.feedAt(pcrTestNonPCRPacket(0x1fff, false), confirmed)
		fault := timeline.continuityFault(confirmed, policy)
		if fault == nil || fault.kind != avFaultMediaStarvation {
			t.Fatalf("connected-but-no-useful-media fault=%v, want %s", fault, avFaultMediaStarvation)
		}
	})

	t.Run("sparse null keepalive cannot postpone known starvation onset", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(initial...), base)
		// Model a connected origin dripping non-program TS while the
		// established A/V clocks have already been absent past
		// threshold+confirmation. The first observable keepalive after
		// ripeness must trip immediately rather than starting a new
		// confirmation window and waiting for the next drip. (This is the
		// timeline's own evidence; in production the byte-level stall
		// watchdog races the same window independently.)
		now := base.Add(policy.starvationThreshold + policy.confirmation + 100*time.Millisecond)
		timeline.feedAt(pcrTestNonPCRPacket(0x1fff, false), now)
		fault := timeline.continuityFault(now, policy)
		if fault == nil || fault.kind != avFaultMediaStarvation {
			t.Fatalf("sparse keepalive fault=%v, want immediate %s", fault, avFaultMediaStarvation)
		}
	})

	t.Run("quiet or still content keeps timestamped tracks healthy", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(initial...), base)
		for i := 1; i <= 5; i++ {
			now := base.Add(time.Duration(i) * time.Second)
			pts := uint64(i+4) * transportPTSRate
			// These packets can contain silence and repeated video pictures; the
			// guard depends only on their advancing PES clocks.
			timeline.feedAt(joinTSPackets(
				testTimestampedPESPacket(videoPID, 0xe0, pts, byte(i+8)),
				testTimestampedPESPacket(audioPID, 0xc0, pts, byte(i+8))), now)
			if fault := timeline.continuityFault(now, policy); fault != nil {
				t.Fatalf("timestamped quiet/still media fault at %s: %v", now.Sub(base), fault)
			}
		}
	})
}

func TestTransportAVContinuityEnforcesDeclaredAudioEstablishmentAndResume(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_050, 0)
	deadline := base.Add(policy.starvationThreshold + policy.confirmation + time.Millisecond)

	t.Run("declared audio never starts", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(
			testPATPacket(pmtPID),
			testAVPMTPacket(pmtPID, videoPID, audioPID),
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1fff, false)), base)
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1), deadline)
		fault := timeline.continuityFault(deadline, policy)
		if fault == nil || fault.kind != avFaultAudioStarvation {
			t.Fatalf("never-established audio fault=%v, want %s", fault, avFaultAudioStarvation)
		}
	})

	t.Run("audio dies before four PES samples", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(
			testPATPacket(pmtPID),
			testAVPMTPacket(pmtPID, videoPID, audioPID),
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false)), base)
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1), deadline)
		fault := timeline.continuityFault(deadline, policy)
		if fault == nil || fault.kind != avFaultAudioStarvation {
			t.Fatalf("early audio death fault=%v, want %s", fault, avFaultAudioStarvation)
		}
	})

	t.Run("audio must resume after explicit epoch", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(
			testPATPacket(pmtPID),
			testAVPMTPacket(pmtPID, videoPID, audioPID),
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false)), base)
		boundary := base.Add(time.Second)
		timeline.feedAt(joinTSPackets(
			testAdaptationOnlyDiscontinuity(audioPID, 1),
			testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 7)), boundary)
		confirmed := boundary.Add(policy.starvationThreshold + policy.confirmation + time.Millisecond)
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, 11*transportPTSRate, 8), confirmed)
		fault := timeline.continuityFault(confirmed, policy)
		if fault == nil || fault.kind != avFaultAudioStarvation {
			t.Fatalf("post-boundary missing audio fault=%v, want %s", fault, avFaultAudioStarvation)
		}
	})

	t.Run("audio-less PMT remains accepted", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(joinTSPackets(
			testPATPacket(pmtPID),
			testVideoOnlyPMTPacket(pmtPID, videoPID),
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1fff, false)), base)
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1), deadline)
		if fault := timeline.continuityFault(deadline, policy); fault != nil {
			t.Fatalf("audio-less program fault=%v", fault)
		}
		snapshot := timeline.snapshotAt(deadline)
		if snapshot.continuityEnforced || snapshot.continuityReason != "audio_not_declared" {
			t.Fatalf("audio-less policy snapshot=%+v", snapshot)
		}
	})
}

func TestTransportAVContinuityRejectsPathologicalFixedSkewAfterDiscontinuity(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_100, 0)
	buildEpoch := func(offset time.Duration) []byte {
		packets := [][]byte{
			testPATPacket(pmtPID),
			testAVPMTPacket(pmtPID, videoPID, audioPID),
			testAdaptationOnlyDiscontinuity(audioPID, 0),
		}
		offsetTicks := uint64(offset) * transportPTSRate / uint64(time.Second)
		for i := 0; i < int(policy.minSamples); i++ {
			videoPTS := uint64(100+i) * transportPTSRate
			packets = append(packets,
				testTimestampedPESPacket(videoPID, 0xe0, videoPTS, byte(i+1)),
				testTimestampedPESPacket(audioPID, 0xc0, videoPTS+offsetTicks, byte(i+1)))
		}
		return joinTSPackets(packets...)
	}

	t.Run("three second fixed offset is sustained failure", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(buildEpoch(3*time.Second), base)
		if snapshot := timeline.snapshotAt(base); !snapshot.valid || snapshot.drift != 0 || snapshot.finalSkew != 3*time.Second {
			t.Fatalf("fixed-skew epoch snapshot=%+v", snapshot)
		}
		if fault := timeline.continuityFault(base, policy); fault != nil {
			t.Fatalf("unconfirmed fixed skew fault=%v", fault)
		}
		confirmed := base.Add(policy.confirmation + time.Millisecond)
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 110*transportPTSRate, 5),
			testTimestampedPESPacket(audioPID, 0xc0, 113*transportPTSRate, 5)), confirmed)
		fault := timeline.continuityFault(confirmed, policy)
		if fault == nil || fault.kind != avFaultTimelineSkew {
			t.Fatalf("confirmed fixed skew fault=%v, want %s", fault, avFaultTimelineSkew)
		}
	})

	t.Run("one second encoder offset remains accepted", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(buildEpoch(time.Second), base)
		if fault := timeline.continuityFault(base, policy); fault != nil {
			t.Fatalf("normal fixed offset fault=%v", fault)
		}
		later := base.Add(policy.confirmation + time.Second)
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 110*transportPTSRate, 5),
			testTimestampedPESPacket(audioPID, 0xc0, 111*transportPTSRate, 5)), later)
		if fault := timeline.continuityFault(later, policy); fault != nil {
			t.Fatalf("normal sustained fixed offset fault=%v", fault)
		}
	})
}

func TestPTSTimelineTrackHandlesWrapAndBFrameReorder(t *testing.T) {
	var track ptsTimelineTrack
	track.record(ptsModulus - transportPTSRate)
	track.record(transportPTSRate)
	if got := ticksSignedDuration(track.spanTicks()); got != 2*time.Second {
		t.Fatalf("wrapped PTS span=%s, want 2s", got)
	}
	track.record(transportPTSRate / 2) // transport-order B-frame PTS
	if track.samples != 3 || ticksSignedDuration(track.spanTicks()) != 2*time.Second {
		t.Fatalf("B-frame reorder reset or inflated epoch: samples=%d span=%s",
			track.samples, ticksSignedDuration(track.spanTicks()))
	}
}

func TestTransportAVTimelineFencesImplausiblePTSWithoutPoisoningExtrema(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_070, 0)
	initial := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		pcrTestNonPCRPacket(0x1fff, false),
	)
	extreme := uint64((6*time.Hour)/time.Second) * transportPTSRate

	t.Run("one valid-marker mutation recovers on prior sane epoch", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(initial, base)
		badAt := base.Add(100 * time.Millisecond)
		// CC=1 is the exact expected successor. Only the otherwise valid PTS
		// step is corrupt, so continuity-counter fencing cannot mask the test.
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, extreme, 1), badAt)
		if fault := timeline.continuityFault(badAt, policy); fault != nil {
			t.Fatalf("single implausible PTS confirmed without bounded window: %v", fault)
		}
		if snapshot := timeline.snapshotAt(badAt); snapshot.transportCorruptions != 1 ||
			snapshot.videoSamples != 0 || snapshot.audioSamples != 0 {
			t.Fatalf("implausible PTS entered extrema instead of fencing the pair: %+v", snapshot)
		}

		recoveredAt := badAt.Add(100 * time.Millisecond)
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 2),
			testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 1),
			testTimestampedPESPacket(videoPID, 0xe0, 2*transportPTSRate, 3),
			testTimestampedPESPacket(audioPID, 0xc0, 2*transportPTSRate, 2),
			pcrTestNonPCRPacket(0x1fff, false)), recoveredAt)
		if fault := timeline.continuityFault(recoveredAt, policy); fault != nil {
			t.Fatalf("sane prior epoch did not recover after transient PTS mutation: %v", fault)
		}
		snapshot := timeline.snapshotAt(recoveredAt)
		if !snapshot.valid || snapshot.drift != 0 || snapshot.initialSkew != 0 ||
			snapshot.finalSkew != 0 || snapshot.videoSamples != 2 || snapshot.audioSamples != 2 {
			t.Fatalf("transient PTS poisoned recovered extrema: %+v", snapshot)
		}
	})

	t.Run("sustained undeclared epoch remains confirmed corruption", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(initial, base)
		badAt := base.Add(100 * time.Millisecond)
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, extreme, 1),
			testTimestampedPESPacket(audioPID, 0xc0, extreme, 1)), badAt)
		confirmed := badAt.Add(policy.confirmation + time.Millisecond)
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, extreme+transportPTSRate, 2),
			testTimestampedPESPacket(audioPID, 0xc0, extreme+transportPTSRate, 2)), confirmed)
		fault := timeline.continuityFault(confirmed, policy)
		if fault == nil || fault.kind != avFaultTransport {
			t.Fatalf("sustained undeclared PTS epoch fault=%v, want %s", fault, avFaultTransport)
		}
		if snapshot := timeline.snapshotAt(confirmed); snapshot.valid ||
			snapshot.videoSamples != 0 || snapshot.audioSamples != 0 {
			t.Fatalf("sustained corrupt epoch re-established poisoned extrema: %+v", snapshot)
		}
	})
}

func TestTransportAVTimelineStillDetectsGradualPlausibleDrift(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_072, 0)
	packets := [][]byte{testPATPacket(pmtPID), testAVPMTPacket(pmtPID, videoPID, audioPID)}
	for i := 0; i < int(policy.minSamples); i++ {
		videoPTS := uint64(i) * transportPTSRate
		audioPTS := uint64(i) * transportPTSRate * 14 / 10
		packets = append(packets,
			testTimestampedPESPacket(videoPID, 0xe0, videoPTS, byte(i)),
			testTimestampedPESPacket(audioPID, 0xc0, audioPTS, byte(i)))
	}
	var timeline transportAVTimeline
	timeline.feedAt(joinTSPackets(packets...), base)
	if fault := timeline.continuityFault(base, policy); fault != nil {
		t.Fatalf("plausible drift confirmed without bounded window: %v", fault)
	}
	confirmed := base.Add(policy.confirmation + time.Millisecond)
	timeline.feedAt(joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, 4*transportPTSRate, 4),
		testTimestampedPESPacket(audioPID, 0xc0, 56*transportPTSRate/10, 4)), confirmed)
	fault := timeline.continuityFault(confirmed, policy)
	if fault == nil || fault.kind != avFaultTimelineDrift {
		t.Fatalf("gradual plausible slope fault=%v, want %s", fault, avFaultTimelineDrift)
	}
}

func TestTransportAVTimelineMeasuresBFramePresentationExtrema(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	packets := [][]byte{
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
	}
	// Representative decode-order video PTS: each I/P frame is followed by
	// reordered B-frame presentation timestamps. Audio remains monotonic.
	videoPTS := []uint64{0, 3, 1, 2, 6, 4, 5}
	for i, seconds := range videoPTS {
		packets = append(packets,
			testTimestampedPESPacket(videoPID, 0xe0, seconds*transportPTSRate, byte(i)),
			testTimestampedPESPacket(audioPID, 0xc0, uint64(i)*transportPTSRate, byte(i)))
	}
	var timeline transportAVTimeline
	timeline.feed(joinTSPackets(packets...))
	snapshot := timeline.snapshot()
	if !snapshot.valid || snapshot.videoSamples != 7 || snapshot.audioSamples != 7 {
		t.Fatalf("B-frame timeline unavailable or reset: %+v", snapshot)
	}
	if snapshot.initialSkew != 0 || snapshot.finalSkew != 0 || snapshot.drift != 0 {
		t.Fatalf("B-frame reorder invented A/V drift: %+v", snapshot)
	}
}

func TestTransportAVTimelineMultiAudioPolicyMatchesPassthroughAndTranscodeSelection(t *testing.T) {
	const (
		pmtPID      = 0x1000
		videoPID    = 0x0100
		firstAudio  = 0x0101
		secondAudio = 0x0102
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_075, 0)
	initial := joinTSPackets(
		testPATPacket(pmtPID),
		testMultiAudioPMTPacket(pmtPID, videoPID, firstAudio, secondAudio),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(firstAudio, 0xc0, 0, 0),
		testTimestampedPESPacket(secondAudio, 0xc1, 0, 0),
		testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1),
		testTimestampedPESPacket(firstAudio, 0xc0, transportPTSRate, 1),
		testTimestampedPESPacket(secondAudio, 0xc1, transportPTSRate, 1),
	)
	later := base.Add(policy.starvationThreshold + policy.confirmation + time.Millisecond)
	progressOnSecondOnly := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, 2*transportPTSRate, 2),
		testTimestampedPESPacket(secondAudio, 0xc1, 2*transportPTSRate, 2),
	)

	var passthrough transportAVTimeline
	passthrough.feedAt(initial, base)
	passthrough.feedAt(progressOnSecondOnly, later)
	if fault := passthrough.continuityFault(later, policy); fault != nil {
		t.Fatalf("passthrough relocated while a declared audio track advanced: %v", fault)
	}
	snapshot := passthrough.snapshotAt(later)
	if snapshot.audioTracks != 2 || !snapshot.continuityEnforced ||
		snapshot.continuityReason != "multi_audio_any_track" {
		t.Fatalf("passthrough multi-audio policy snapshot=%+v", snapshot)
	}

	// Transcode matches passthrough: a declared first track may legitimately
	// sleep for minutes (measured on a TLC-class mux) while another declared
	// track carries the programme. Relocating for the mapped track's silence
	// lands on the same origin with the same sleeping track; liveness verdicts
	// therefore consult the freshest declared clock in both modes.
	var transcoded transportAVTimeline
	transcoded.requireFirstAudio = true
	transcoded.feedAt(initial, base)
	transcoded.feedAt(progressOnSecondOnly, later)
	if fault := transcoded.continuityFault(later, policy); fault != nil {
		t.Fatalf("0:a:0 transcode relocated while a declared audio track advanced: %v", fault)
	}
	if snapshot := transcoded.snapshotAt(later); snapshot.continuityReason != "multi_audio_first_mapped" {
		t.Fatalf("transcode multi-audio policy snapshot=%+v", snapshot)
	}

	badFirst := testTimestampedPESPacket(firstAudio, 0xc0, 99*transportPTSRate, 1)
	var passthroughCorrupt transportAVTimeline
	passthroughCorrupt.feedAt(initial, base)
	passthroughCorrupt.feedAt(badFirst, base.Add(100*time.Millisecond))
	passthroughCorrupt.feedAt(progressOnSecondOnly, later)
	if fault := passthroughCorrupt.continuityFault(later, policy); fault != nil {
		t.Fatalf("unused passthrough audio corruption relocated healthy alternate track: %v", fault)
	}
	if snapshot := passthroughCorrupt.snapshotAt(later); snapshot.transportCorruptions != 1 {
		t.Fatalf("unused-track corruption missing telemetry: %+v", snapshot)
	}

	var transcodeCorrupt transportAVTimeline
	transcodeCorrupt.requireFirstAudio = true
	transcodeCorrupt.feedAt(initial, base)
	transcodeCorrupt.feedAt(badFirst, base.Add(100*time.Millisecond))
	// Damage on the mapped track is detected once confirmed...
	confirmed := base.Add(100*time.Millisecond + policy.confirmation + time.Millisecond)
	if fault := transcodeCorrupt.continuityFault(confirmed, policy); fault == nil || fault.kind != avFaultTransport {
		t.Fatalf("mapped 0:a:0 corruption fault=%v, want %s", fault, avFaultTransport)
	}
	// ...but recovery completes when any declared track returns to a sane
	// epoch. Requiring the mapped track specifically held the feed in a
	// corruption state for as long as that track chose to sleep, which no
	// relocation can fix. (The input safety gate independently keeps the
	// damaged bytes out of FFmpeg.)
	transcodeCorrupt.feedAt(progressOnSecondOnly, later)
	if fault := transcodeCorrupt.continuityFault(later, policy); fault != nil {
		t.Fatalf("recovered multi-audio feed still faulted: %v", fault)
	}

	badSecond := testTimestampedPESPacket(secondAudio, 0xc1, 99*transportPTSRate, 1)
	var passthroughSafety transportAVTimeline
	passthroughSafety.feedAt(initial, base)
	if fault := passthroughSafety.feedAtAndReportFault(badSecond, base.Add(100*time.Millisecond)); fault == nil || fault.kind != avFaultTransport {
		t.Fatalf("secondary passthrough corruption fault=%v, want unpublished transport damage", fault)
	}
	var transcodeSafety transportAVTimeline
	transcodeSafety.requireFirstAudio = true
	transcodeSafety.feedAt(initial, base)
	if fault := transcodeSafety.feedAtAndReportFault(badSecond, base.Add(100*time.Millisecond)); fault != nil {
		t.Fatalf("unmapped secondary transcode track poisoned 0:a:0 pipeline: %v", fault)
	}
	if snapshot := transcodeSafety.snapshotAt(base.Add(100 * time.Millisecond)); snapshot.transportCorruptions != 1 {
		t.Fatalf("unmapped-track corruption missing telemetry: %+v", snapshot)
	}

	allStopped := later.Add(policy.starvationThreshold + policy.confirmation + time.Millisecond)
	passthrough.feedAt(testTimestampedPESPacket(videoPID, 0xe0, 3*transportPTSRate, 3), allStopped)
	if fault := passthrough.continuityFault(allStopped, policy); fault == nil || fault.kind != avFaultAudioStarvation {
		t.Fatalf("all declared audio stopped fault=%v, want %s", fault, avFaultAudioStarvation)
	}
}

func TestTransportAVTimelineIgnoresCRCInvalidProgramMapThenRecovers(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	invalidPMT := testAVPMTPacket(pmtPID, videoPID, audioPID)
	payload, ok := tsPayload(invalidPMT)
	if !ok {
		t.Fatal("diagnostic PMT fixture has no payload")
	}
	pointer := int(payload[0])
	section := payload[1+pointer:]
	total := 3 + (int(section[1]&0x0f)<<8 | int(section[2]))
	section[total-1] ^= 0x01

	var timeline transportAVTimeline
	timeline.feed(joinTSPackets(
		testPATPacket(pmtPID),
		invalidPMT,
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		pcrTestNonPCRPacket(0x1fff, false),
	))
	if snapshot := timeline.snapshot(); snapshot.valid || snapshot.continuityEnforced ||
		snapshot.continuityReason != "pmt_unassembled_or_invalid" {
		t.Fatalf("CRC-invalid diagnostic PMT policy=%+v", snapshot)
	}

	timeline.feed(joinTSPackets(
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 1),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 1),
		testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 2),
		testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 2),
	))
	if snapshot := timeline.snapshot(); !snapshot.valid || snapshot.drift != 0 {
		t.Fatalf("diagnostic timeline did not recover on a valid same-version PMT: %+v", snapshot)
	}
}

func TestTransportAVTimelineValidatesSelectedPacketTransportBeforePTS(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_080, 0)
	initial := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		pcrTestNonPCRPacket(0x1fff, false),
	)

	t.Run("exact duplicate is ignored and cannot refresh progress", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(initial, base)
		confirmed := base.Add(policy.starvationThreshold + policy.confirmation + time.Millisecond)
		timeline.feedAt(testTimestampedPESPacket(videoPID, 0xe0, 0, 0), confirmed)
		if snapshot := timeline.snapshotAt(confirmed); snapshot.videoSamples != 1 || snapshot.transportCorruptions != 0 {
			t.Fatalf("legal duplicate changed progress/corruption telemetry: %+v", snapshot)
		}
		if fault := timeline.continuityFault(confirmed, policy); fault == nil || fault.kind != avFaultMediaStarvation {
			t.Fatalf("duplicate-masked starvation fault=%v, want %s", fault, avFaultMediaStarvation)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "same CC different payload", mutate: func(packet []byte) {
			copy(packet, testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 0))
		}},
		{name: "continuity gap", mutate: func(packet []byte) {
			copy(packet, testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 2))
		}},
		{name: "transport error indicator", mutate: func(packet []byte) {
			copy(packet, testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1))
			packet[1] |= 0x80
		}},
		{name: "scrambled payload", mutate: func(packet []byte) {
			copy(packet, testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1))
			packet[3] |= 0x80
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var timeline transportAVTimeline
			timeline.feedAt(initial, base)
			bad := make([]byte, tsPacketSize)
			tc.mutate(bad)
			observed := base.Add(100 * time.Millisecond)
			timeline.feedAt(bad, observed)
			if fault := timeline.continuityFault(observed, policy); fault != nil {
				t.Fatalf("transport fault confirmed without bounded window: %v", fault)
			}
			confirmed := observed.Add(policy.confirmation + time.Millisecond)
			timeline.feedAt(pcrTestNonPCRPacket(0x1fff, false), confirmed)
			fault := timeline.continuityFault(confirmed, policy)
			if fault == nil || fault.kind != avFaultTransport {
				t.Fatalf("confirmed transport fault=%v, want %s", fault, avFaultTransport)
			}
			if snapshot := timeline.snapshotAt(confirmed); snapshot.transportCorruptions != 1 ||
				snapshot.videoSamples != 0 || snapshot.audioSamples != 0 {
				t.Fatalf("corrupt packet fed PTS/extrema: %+v", snapshot)
			}
		})
	}

	t.Run("invalid PTS marker is fenced then prior sane epoch recovers", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(initial, base)
		invalidVideo := testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 1)
		invalidAudio := testTimestampedPESPacket(audioPID, 0xc0, 200*transportPTSRate, 1)
		for _, packet := range [][]byte{invalidVideo, invalidAudio} {
			payload, ok := tsPayload(packet)
			if !ok {
				t.Fatal("invalid-PTS fixture lacks payload")
			}
			payload[9] &^= 0x01
		}
		invalidAt := base.Add(100 * time.Millisecond)
		timeline.feedAt(joinTSPackets(invalidVideo, invalidAudio), invalidAt)
		if fault := timeline.continuityFault(invalidAt, policy); fault != nil {
			t.Fatalf("malformed timestamp confirmed without bounded window: %v", fault)
		}
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 2),
			testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 2)), base.Add(200*time.Millisecond))
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 2*transportPTSRate, 3),
			testTimestampedPESPacket(audioPID, 0xc0, 2*transportPTSRate, 3)), base.Add(300*time.Millisecond))
		snapshot := timeline.snapshotAt(base.Add(300 * time.Millisecond))
		if !snapshot.valid || snapshot.drift != 0 || snapshot.finalSkew != 0 ||
			snapshot.transportCorruptions != 2 {
			t.Fatalf("invalid PTS poisoned/falsely faulted timeline: %+v", snapshot)
		}
		if fault := timeline.continuityFault(base.Add(300*time.Millisecond), policy); fault != nil {
			t.Fatalf("sane epoch did not recover malformed PTS fence: %v", fault)
		}
	})
}

func TestTransportAVTimelineMalformedTimestampClaimsConfirmTransportFault(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	policy := defaultAVContinuityPolicy()
	base := time.Unix(1_700_000_085, 0)
	initial := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		pcrTestNonPCRPacket(0x1fff, false))

	for _, tc := range []struct {
		name    string
		makePES func() []byte
	}{
		{name: "reserved PTS_DTS flags", makePES: func() []byte {
			payload := slateTestPES(0xc0, transportPTSRate, 0, false)
			payload[7] = 0x40
			return payload
		}},
		{name: "wrong PTS prefix", makePES: func() []byte {
			payload := slateTestPES(0xc0, transportPTSRate, 0, false)
			payload[9] = payload[9]&0x0f | 0x30
			return payload
		}},
		{name: "missing PTS marker", makePES: func() []byte {
			payload := slateTestPES(0xc0, transportPTSRate, 0, false)
			payload[13] &^= 0x01
			return payload
		}},
		{name: "short claimed DTS header", makePES: func() []byte {
			payload := slateTestPES(0xc0, transportPTSRate, transportPTSRate-3600, true)
			payload[8] = 5
			return payload
		}},
		{name: "wrong DTS prefix", makePES: func() []byte {
			payload := slateTestPES(0xc0, transportPTSRate, transportPTSRate-3600, true)
			payload[14] = payload[14]&0x0f | 0x20
			return payload
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var timeline transportAVTimeline
			timeline.feedAt(initial, base)
			badAt := base.Add(100 * time.Millisecond)
			bad := testExactPayloadPacket(audioPID, true, 1, tc.makePES())
			timeline.feedAt(bad, badAt)
			if fault := timeline.continuityFault(badAt, policy); fault != nil {
				t.Fatalf("malformed timestamp confirmed without window: %v", fault)
			}
			confirmed := badAt.Add(policy.confirmation + time.Millisecond)
			timeline.feedAt(testExactPayloadPacket(audioPID, true, 2, tc.makePES()), confirmed)
			fault := timeline.continuityFault(confirmed, policy)
			if fault == nil || fault.kind != avFaultTransport {
				t.Fatalf("confirmed malformed timestamp fault=%v, want %s", fault, avFaultTransport)
			}
			if snapshot := timeline.snapshotAt(confirmed); snapshot.transportCorruptions != 2 ||
				snapshot.videoSamples != 0 || snapshot.audioSamples != 0 {
				t.Fatalf("malformed timestamp fed progress/extrema: %+v", snapshot)
			}
		})
	}

	t.Run("flags 00 does not claim progress and exposes audio starvation", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(initial, base)
		noPTS := slateTestPES(0xc0, transportPTSRate, 0, false)
		noPTS[7] = 0
		noPTS[8] = 0
		confirmed := base.Add(policy.starvationThreshold + policy.confirmation + time.Millisecond)
		timeline.feedAt(joinTSPackets(
			testExactPayloadPacket(audioPID, true, 1, noPTS),
			testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1)), confirmed)
		fault := timeline.continuityFault(confirmed, policy)
		if fault == nil || fault.kind != avFaultAudioStarvation {
			t.Fatalf("no-PTS audio fault=%v, want %s", fault, avFaultAudioStarvation)
		}
		if snapshot := timeline.snapshotAt(confirmed); snapshot.transportCorruptions != 0 ||
			snapshot.audioSamples != 1 {
			t.Fatalf("legitimate no-PTS packet treated as corruption/progress: %+v", snapshot)
		}
	})

	t.Run("valid split headers recover a malformed timestamp fence", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.feedAt(initial, base)
		bad := slateTestPES(0xc0, transportPTSRate, 0, false)
		bad[7] = 0x40
		timeline.feedAt(testExactPayloadPacket(audioPID, true, 1, bad), base.Add(100*time.Millisecond))

		videoPES := slateTestPES(0xe0, transportPTSRate, transportPTSRate-3600, true)
		audioPES := slateTestPES(0xc0, transportPTSRate, 0, false)
		timeline.feedAt(joinTSPackets(
			testExactPayloadPacket(videoPID, true, 1, videoPES[:8]),
			testExactPayloadPacket(audioPID, true, 2, audioPES[:8])), base.Add(200*time.Millisecond))
		timeline.feedAt(joinTSPackets(
			testExactPayloadPacket(videoPID, false, 2, videoPES[8:]),
			testExactPayloadPacket(audioPID, false, 3, audioPES[8:])), base.Add(210*time.Millisecond))
		timeline.feedAt(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 2*transportPTSRate, 3),
			testTimestampedPESPacket(audioPID, 0xc0, 2*transportPTSRate, 4)), base.Add(300*time.Millisecond))

		snapshot := timeline.snapshotAt(base.Add(300 * time.Millisecond))
		if !snapshot.valid || snapshot.videoSamples != 2 || snapshot.audioSamples != 2 ||
			snapshot.transportCorruptions != 1 || snapshot.drift != 0 || snapshot.finalSkew != 0 {
			t.Fatalf("split-header recovery snapshot=%+v", snapshot)
		}
		if fault := timeline.continuityFault(base.Add(300*time.Millisecond), policy); fault != nil {
			t.Fatalf("valid split headers did not recover transport fence: %v", fault)
		}
	})
}

func TestTransportAVTimelineUnsafeEvidenceIsBoundToExactFeed(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_087, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID
	newSafety := func(t *testing.T) *transportAVTimeline {
		t.Helper()
		timeline := &transportAVTimeline{}
		timeline.bindSelectedProgram(program, base)
		if unsafe := timeline.feedAtAndReportUnsafe(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1fff, false)), base); unsafe {
			t.Fatal("valid establishment feed was marked unsafe")
		}
		return timeline
	}
	saneAudio := func(cc byte) []byte {
		return testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, cc)
	}

	for _, tc := range []struct {
		name string
		bad  func() []byte
		sane func() []byte
	}{
		{name: "implausible PTS then sane", bad: func() []byte {
			return testTimestampedPESPacket(audioPID, 0xc0, 4*60*60*transportPTSRate, 1)
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "malformed PTS then sane", bad: func() []byte {
			packet := testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 1)
			payload, _ := tsPayload(packet)
			payload[9] = payload[9]&0x0f | 0x30
			return packet
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "invalid PES start code then sane", bad: func() []byte {
			packet := saneAudio(1)
			payload, _ := tsPayload(packet)
			payload[2] = 0x02
			return packet
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "unsupported MPEG-1 header then sane", bad: func() []byte {
			packet := saneAudio(1)
			payload, _ := tsPayload(packet)
			payload[6] = 0x40
			return packet
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "PES packet length contradicts optional header then sane", bad: func() []byte {
			packet := saneAudio(1)
			payload, _ := tsPayload(packet)
			payload[4], payload[5] = 0, 1
			return packet
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "incident-scale DTS offset then sane", bad: func() []byte {
			return testExactPayloadPacket(audioPID, true, 1,
				slateTestPES(0xc0, transportPTSRate, (1+9*60)*transportPTSRate, true))
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "TEI then sane", bad: func() []byte {
			packet := saneAudio(1)
			packet[1] |= 0x80
			return packet
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "scramble then sane", bad: func() []byte {
			packet := saneAudio(1)
			packet[3] |= 0x80
			return packet
		}, sane: func() []byte { return saneAudio(2) }},
		{name: "CC loss then sane", bad: func() []byte {
			return saneAudio(2)
		}, sane: func() []byte { return saneAudio(3) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeline := newSafety(t)
			// A sane successor in the same 64 KiB/coalesced result may repair
			// timeline state, but it must never erase the immutable decision to
			// withhold the exact result that also contained damaged transport.
			if unsafe := timeline.feedAtAndReportUnsafe(
				joinTSPackets(tc.bad(), tc.sane()), base.Add(time.Second)); !unsafe {
				t.Fatal("bad+sane exact feed cleared unsafe publication evidence")
			}
			// Evidence is per-result, not a global poison bit: a later independently
			// sane result can enter the mandatory fresh private attempt without
			// blaming an older queued result or future producer-edge parsing.
			freshAttempt := newSafety(t)
			if unsafe := freshAttempt.feedAtAndReportUnsafe(
				joinTSPackets(
					testTimestampedPESPacket(videoPID, 0xe0, 2*transportPTSRate, 1),
					testTimestampedPESPacket(audioPID, 0xc0, 2*transportPTSRate, 1)),
				base.Add(2*time.Second)); unsafe {
				t.Fatal("later sane feed inherited unsafe evidence from older result")
			}
		})
	}

	t.Run("flags 00 remains safe absence", func(t *testing.T) {
		timeline := newSafety(t)
		noPTS := slateTestPES(0xc0, transportPTSRate, 0, false)
		noPTS[7], noPTS[8] = 0, 0
		if unsafe := timeline.feedAtAndReportUnsafe(
			testExactPayloadPacket(audioPID, true, 1, noPTS), base.Add(time.Second)); unsafe {
			t.Fatal("legitimate PES without a claimed PTS was marked corrupt")
		}
	})

	t.Run("impossible selected adaptation lengths are withheld", func(t *testing.T) {
		for _, pid := range []int{videoPID, audioPID, program.pcrPID} {
			for _, tc := range []struct {
				name    string
				control byte
				length  byte
			}{
				{name: "adaptation-plus-payload", control: 3, length: 184},
				{name: "adaptation-only", control: 2, length: 182},
			} {
				t.Run(fmt.Sprintf("pid_%#x/%s", pid, tc.name), func(t *testing.T) {
					timeline := newSafety(t)
					packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
					packet[0] = 0x47
					packet[1] = byte(pid>>8) & 0x1f
					packet[2] = byte(pid & 0xff)
					packet[3] = tc.control << 4
					packet[4] = tc.length
					fault := timeline.feedAtAndReportFault(packet, base.Add(time.Second))
					if fault == nil || fault.kind != avFaultTransport {
						t.Fatalf("invalid selected packet fault=%v, want transport rejection", fault)
					}
				})
			}
		}
	})

	t.Run("malformed selected adaptation subfields are withheld", func(t *testing.T) {
		validPCR := slateTestPCR(0)
		badReserved := append([]byte(nil), validPCR...)
		badReserved[4] &^= 0x7e
		badExtension := append([]byte(nil), validPCR...)
		badExtension[4] = badExtension[4]&0xfe | 0x01
		badExtension[5] = 44 // 0x12c == 300, outside the MPEG PCR range
		for _, tc := range []struct {
			name       string
			adaptation []byte
		}{
			{name: "missing PCR", adaptation: []byte{0x10}},
			{name: "OPCR without PCR", adaptation: append([]byte{0x08}, validPCR...)},
			{name: "missing OPCR", adaptation: append([]byte{0x18}, validPCR...)},
			{name: "missing splice countdown", adaptation: []byte{0x04}},
			{name: "private data overruns field", adaptation: []byte{0x02, 5, 0xaa}},
			{name: "extension overruns field", adaptation: []byte{0x01, 5, 0xaa}},
			{name: "PCR reserved bits", adaptation: append([]byte{0x10}, badReserved...)},
			{name: "PCR extension 300", adaptation: append([]byte{0x10}, badExtension...)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				timeline := newSafety(t)
				packet := testAdaptationPayloadPacket(audioPID, true, 1, tc.adaptation,
					slateTestPES(0xc0, transportPTSRate, 0, false))
				fault := timeline.feedAtAndReportFault(packet, base.Add(time.Second))
				if fault == nil || fault.kind != avFaultTransport {
					t.Fatalf("invalid selected adaptation fault=%v, want transport rejection", fault)
				}
				if snapshot := timeline.snapshotAt(base.Add(time.Second)); snapshot.audioSamples != 0 {
					// The transport fence starts a fresh private epoch; the rejected
					// packet must not establish a clock in that epoch.
					t.Fatalf("invalid adaptation refreshed selected clock: %+v", snapshot)
				}
			})
		}
	})
}

func TestTransportAVTimelineImmediatelyWithholdsIncidentScaleClockOffsets(t *testing.T) {
	const (
		videoPID    = 0x0100
		firstAudio  = 0x0101
		secondAudio = 0x0102
	)
	base := time.Unix(1_700_000_088, 0)
	newTimeline := func(audioPIDs ...int) *transportAVTimeline {
		program := avProgramPIDs{
			videoPID: videoPID, pcrPID: videoPID, audioCount: len(audioPIDs),
		}
		for i := range program.audioPIDs {
			program.audioPIDs[i] = -1
		}
		copy(program.audioPIDs[:], audioPIDs)
		timeline := &transportAVTimeline{}
		timeline.bindSelectedProgram(program, base)
		return timeline
	}

	t.Run("one sample proves nine minute fixed skew", func(t *testing.T) {
		timeline := newTimeline(firstAudio)
		fault := timeline.feedAtAndReportFault(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 0),
			testTimestampedPESPacket(firstAudio, 0xc0, (100+9*60)*transportPTSRate, 0),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1ffe, false),
			pcrTestNonPCRPacket(0x1ffd, false)), base)
		if fault == nil || fault.kind != avFaultTimelineSkew || fault.skew != 9*time.Minute {
			t.Fatalf("gross fixed-skew fault=%+v, want immediate nine-minute skew", fault)
		}
	})

	t.Run("plausible steps cannot hide gross slope divergence", func(t *testing.T) {
		timeline := newTimeline(firstAudio)
		packets := make([][]byte, 0, 8)
		for i := 0; i < 4; i++ {
			videoPTS := uint64(1000*transportPTSRate) + uint64(i)*durationTicks(10*time.Millisecond, transportPTSRate)
			audioPTS := uint64(971*transportPTSRate) + uint64(i)*durationTicks(19*time.Second, transportPTSRate)
			packets = append(packets,
				testTimestampedPESPacket(videoPID, 0xe0, videoPTS, byte(i)),
				testTimestampedPESPacket(firstAudio, 0xc0, audioPTS, byte(i)))
		}
		fault := timeline.feedAtAndReportFault(joinTSPackets(packets...), base)
		if fault == nil || fault.kind != avFaultTimelineDrift ||
			absoluteDuration(fault.drift) < immediateGrossAVOffset ||
			absoluteDuration(fault.skew) >= immediateGrossAVOffset {
			t.Fatalf("gross slope fault=%+v, want immediate drift with sub-gross final skew", fault)
		}
	})

	t.Run("secondary passthrough track is also protected", func(t *testing.T) {
		timeline := newTimeline(firstAudio, secondAudio)
		fault := timeline.feedAtAndReportFault(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 0),
			testTimestampedPESPacket(firstAudio, 0xc0, 100*transportPTSRate, 0),
			testTimestampedPESPacket(secondAudio, 0xc1, (100+9*60)*transportPTSRate, 0),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1ffe, false)), base)
		if fault == nil || fault.kind != avFaultTimelineSkew {
			t.Fatalf("secondary-track gross fault=%+v, want immediate skew", fault)
		}
	})

	t.Run("ordinary fixed offset retains confirmation", func(t *testing.T) {
		policy := defaultAVContinuityPolicy()
		timeline := newTimeline(firstAudio)
		var packets [][]byte
		for i := 0; i < int(policy.minSamples); i++ {
			pts := uint64(100+i) * transportPTSRate
			packets = append(packets,
				testTimestampedPESPacket(videoPID, 0xe0, pts, byte(i)),
				testTimestampedPESPacket(firstAudio, 0xc0, pts+3*transportPTSRate, byte(i)))
		}
		if fault := timeline.feedAtAndReportFault(joinTSPackets(packets...), base); fault != nil {
			t.Fatalf("ordinary offset was immediately withheld: %v", fault)
		}
		if fault := timeline.continuityFault(base, policy); fault != nil {
			t.Fatalf("ordinary offset skipped confirmation: %v", fault)
		}
		fault := timeline.continuityFault(base.Add(policy.confirmation+time.Millisecond), policy)
		if fault == nil || fault.kind != avFaultTimelineSkew {
			t.Fatalf("confirmed ordinary offset=%v, want %s", fault, avFaultTimelineSkew)
		}
	})
}

func TestPublicationAVGatePrivatelyReestablishesSelectedEpoch(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_089, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID
	initialVideo := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		pcrTestNonPCRPacket(0x1fff, false),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffd, false),
		pcrTestNonPCRPacket(0x1ffc, false))
	boundary := testAdaptationOnlyDiscontinuity(audioPID, 0)

	t.Run("late gross audio never escapes before video proves skew", func(t *testing.T) {
		var gate publicationAVGate
		gate.bindSelectedProgram(program, base, false)
		if out, _, fault := gate.push(initialVideo, base); fault != nil || len(out) != 0 {
			t.Fatalf("video-only startup=(bytes=%d fault=%v), want held", len(out), fault)
		}
		grossAudio := testTimestampedPESPacket(audioPID, 0xc0, 9*60*transportPTSRate, 1)
		out, _, fault := gate.push(joinTSPackets(boundary, grossAudio), base.Add(100*time.Millisecond))
		if len(out) != 0 || fault == nil || fault.kind != avFaultTimelineSkew || len(gate.pending) != 0 {
			t.Fatalf("gross late-PID initialization=(bytes=%d fault=%v pending=%d), want zero publication",
				len(out), fault, len(gate.pending))
		}
	})

	t.Run("sane re-establishment releases held bytes in order", func(t *testing.T) {
		var gate publicationAVGate
		gate.bindSelectedProgram(program, base, false)
		_, _, _ = gate.push(initialVideo, base)
		saneAudio := testTimestampedPESPacket(audioPID, 0xc0, transportPTSRate, 1)
		out, arrival, fault := gate.push(joinTSPackets(boundary, saneAudio), base.Add(100*time.Millisecond))
		want := joinTSPackets(initialVideo, boundary, saneAudio)
		release := base.Add(100 * time.Millisecond)
		if fault != nil || !bytes.Equal(out, want) || !arrival.Equal(release) {
			t.Fatalf("sane re-establishment=(bytes=%d arrival=%v fault=%v), want %d ordered bytes from %v",
				len(out), arrival, fault, len(want), release)
		}
		freshVideo := testTimestampedPESPacket(videoPID, 0xe0, transportPTSRate, 1)
		if out, arrival, fault := gate.push(freshVideo, base.Add(100*time.Millisecond)); fault != nil ||
			!bytes.Equal(out, freshVideo) || !arrival.Equal(base.Add(100*time.Millisecond)) {
			t.Fatalf("post-establishment video=(bytes=%d arrival=%v fault=%v), want direct safe publication",
				len(out), arrival, fault)
		}
	})
}

func TestPublicationAVGateRejectsEpochBeforePacerLiveDeadline(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_094, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID
	var gate publicationAVGate
	gate.bindSelectedProgram(program, base, false)
	video := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		pcrTestNonPCRPacket(0x1fff, false),
		pcrTestNonPCRPacket(0x1ffe, false),
		pcrTestNonPCRPacket(0x1ffd, false),
		pcrTestNonPCRPacket(0x1ffc, false),
	)
	if out, _, fault := gate.push(video, base); len(out) != 0 || fault != nil {
		t.Fatalf("video-only epoch=(bytes=%d fault=%v), want privately held", len(out), fault)
	}
	late := base.Add(maxPublicationAVGateHold + time.Millisecond)
	out, arrival, fault := gate.push(
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0), late)
	if len(out) != 0 || fault == nil || fault.kind != avFaultAudioStarvation ||
		len(gate.pending) != 0 || !arrival.Equal(late) {
		t.Fatalf("late epoch completion=(bytes=%d arrival=%v fault=%v pending=%d), want bounded audio starvation",
			len(out), arrival, fault, len(gate.pending))
	}
	if maxPublicationAVGateHold >= maxTransportAddedLatency ||
		maxPublicationAVGateHold >= defaultAVContinuityPolicy().starvationThreshold {
		t.Fatalf("publication hold=%s must precede pacing=%s and continuity=%s",
			maxPublicationAVGateHold, maxTransportAddedLatency,
			defaultAVContinuityPolicy().starvationThreshold)
	}
	if isLocalRelayRetry(fault) || !requiresImmediateContinuityFiller(fault) ||
		!shouldMarkSourceFailure(fault) {
		t.Fatalf("direct gate fault attribution local=%v immediate=%v source=%v",
			isLocalRelayRetry(fault), requiresImmediateContinuityFiller(fault), shouldMarkSourceFailure(fault))
	}
	if selected, inputScope := selectTranscodeContinuityFault(nil, fault); selected != fault || inputScope {
		t.Fatalf("output-only transcode gate fault=(%v,input=%v), want local output restart", selected, inputScope)
	}
	if selected, inputScope := selectTranscodeContinuityFault(fault, fault); selected != fault || !inputScope {
		t.Fatalf("matching input transcode gate fault=(%v,input=%v), want provider relocation", selected, inputScope)
	}
}

func TestPublicationAVGateKeepsOrdinarySkewPrivateUntilResolvedOrBounded(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_096, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID
	newGate := func() *publicationAVGate {
		gate := &publicationAVGate{}
		gate.bindSelectedProgram(program, base, false)
		return gate
	}
	threeSkewedPairs := func(gate *publicationAVGate) {
		for index := 0; index < 3; index++ {
			videoPTS := uint64(index) * transportPTSRate
			pair := joinTSPackets(
				testTimestampedPESPacket(videoPID, 0xe0, videoPTS, byte(index)),
				testTimestampedPESPacket(audioPID, 0xc0, videoPTS+3*transportPTSRate, byte(index)),
			)
			if out, _, fault := gate.push(pair, base.Add(time.Duration(index)*100*time.Millisecond)); len(out) != 0 || fault != nil {
				t.Fatalf("skewed pair %d=(bytes=%d fault=%v), want private candidate", index+1, len(out), fault)
			}
		}
	}

	t.Run("counterpart catch-up releases the complete private prefix", func(t *testing.T) {
		gate := newGate()
		threeSkewedPairs(gate)
		catchUp := joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 6*transportPTSRate, 3),
			testTimestampedPESPacket(audioPID, 0xc0, 6*transportPTSRate, 3),
		)
		// The release instant, not the first held byte's arrival: the gate's
		// private hold must not consume the pacer's independent live deadline.
		release := base.Add(300 * time.Millisecond)
		out, arrival, fault := gate.push(catchUp, release)
		if fault != nil || len(out) != 8*tsPacketSize || !arrival.Equal(release) {
			t.Fatalf("catch-up release=(bytes=%d arrival=%v fault=%v), want 8 packets from release instant",
				len(out), arrival, fault)
		}
	})

	t.Run("persistent skew becomes typed fault before pacing staleness", func(t *testing.T) {
		gate := newGate()
		threeSkewedPairs(gate)
		out, _, fault := gate.push(joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 3*transportPTSRate, 3),
			testTimestampedPESPacket(audioPID, 0xc0, 6*transportPTSRate, 3),
		), base.Add(maxPublicationAVGateHold+time.Millisecond))
		if len(out) != 0 || fault == nil || fault.kind != avFaultTimelineSkew || len(gate.pending) != 0 {
			t.Fatalf("persistent skew=(bytes=%d fault=%v pending=%d), want bounded typed rejection",
				len(out), fault, len(gate.pending))
		}
	})
}

func TestPublicationAVGateRejectsSupersededIncompleteSelectedPES(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_097, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID
	for _, track := range []struct {
		name     string
		pid      int
		streamID byte
	}{
		{name: "video", pid: videoPID, streamID: 0xe0},
		{name: "audio", pid: audioPID, streamID: 0xc0},
	} {
		for _, header := range []struct {
			name    string
			withDTS bool
			cut     int
		}{
			{name: "base", cut: 7},
			{name: "PTS", cut: 11},
			{name: "PTS+DTS", withDTS: true, cut: 15},
		} {
			t.Run(track.name+"/"+header.name, func(t *testing.T) {
				var gate publicationAVGate
				gate.bindSelectedProgram(program, base, false)
				initial := joinTSPackets(
					testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
					testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
					pcrTestNonPCRPacket(0x1fff, false),
					pcrTestNonPCRPacket(0x1ffe, false),
					pcrTestNonPCRPacket(0x1ffd, false),
				)
				if out, _, fault := gate.push(initial, base); fault != nil || len(out) != len(initial) {
					t.Fatalf("initial epoch=(bytes=%d fault=%v)", len(out), fault)
				}
				pes := slateTestPES(track.streamID, transportPTSRate, transportPTSRate-3600, header.withDTS)
				partial := testExactPayloadPacket(track.pid, true, 1, pes[:header.cut])
				if out, _, fault := gate.push(partial, base.Add(time.Millisecond)); fault != nil || len(out) != 0 {
					t.Fatalf("incomplete header=(bytes=%d fault=%v), want held", len(out), fault)
				}
				replacement := testTimestampedPESPacket(track.pid, track.streamID, transportPTSRate, 2)
				out, _, fault := gate.push(replacement, base.Add(2*time.Millisecond))
				if len(out) != 0 || fault == nil || fault.kind != avFaultTransport || len(gate.pending) != 0 {
					t.Fatalf("superseding PUSI=(bytes=%d fault=%v pending=%d), want exact-feed transport rejection",
						len(out), fault, len(gate.pending))
				}

				var fresh publicationAVGate
				fresh.bindSelectedProgram(program, base.Add(time.Second), false)
				freshMedia := joinTSPackets(
					testTimestampedPESPacket(videoPID, 0xe0, 2*transportPTSRate, 0),
					testTimestampedPESPacket(audioPID, 0xc0, 2*transportPTSRate, 0),
					pcrTestNonPCRPacket(0x1fff, false),
					pcrTestNonPCRPacket(0x1ffe, false),
					pcrTestNonPCRPacket(0x1ffd, false),
				)
				if out, _, fault := fresh.push(freshMedia, base.Add(time.Second)); fault != nil || len(out) != len(freshMedia) {
					t.Fatalf("fresh attempt recovery=(bytes=%d fault=%v)", len(out), fault)
				}
			})
		}
	}

	for _, track := range []struct {
		name     string
		pid      int
		streamID byte
	}{
		{name: "video", pid: videoPID, streamID: 0xe0},
		{name: "audio", pid: audioPID, streamID: 0xc0},
	} {
		t.Run(track.name+"/long optional header completes before release", func(t *testing.T) {
			var gate publicationAVGate
			gate.bindSelectedProgram(program, base, false)
			initial := joinTSPackets(
				testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
				testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
				pcrTestNonPCRPacket(0x1fff, false),
				pcrTestNonPCRPacket(0x1ffe, false),
				pcrTestNonPCRPacket(0x1ffd, false),
			)
			if out, _, fault := gate.push(initial, base); fault != nil || len(out) != len(initial) {
				t.Fatalf("initial epoch=(bytes=%d fault=%v)", len(out), fault)
			}
			pes := slateTestPES(track.streamID, transportPTSRate, 0, false)
			pes[4], pes[5] = 0, 204 // three control + 200 optional + one ES byte
			pes[8] = 200
			pes = append(pes, make([]byte, 9+200+1-len(pes))...)
			first := testExactPayloadPacket(track.pid, true, 1, pes[:182])
			second := testExactPayloadPacket(track.pid, false, 2, pes[182:])
			if out, _, fault := gate.push(first, base.Add(time.Millisecond)); fault != nil || len(out) != 0 {
				t.Fatalf("partial long header=(bytes=%d fault=%v), want held", len(out), fault)
			}
			out, _, fault := gate.push(second, base.Add(2*time.Millisecond))
			if fault != nil || !bytes.Equal(out, joinTSPackets(first, second)) {
				t.Fatalf("completed long header=(bytes=%d fault=%v), want atomic two-packet release", len(out), fault)
			}
		})
	}
}

func TestPublicationAVGateRequiresElementaryPayloadForSelectedClockProgress(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_098, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID

	for _, track := range []struct {
		name       string
		pid        int
		streamID   byte
		counterPID int
		counterID  byte
	}{
		{name: "audio", pid: audioPID, streamID: 0xc0, counterPID: videoPID, counterID: 0xe0},
		{name: "video", pid: videoPID, streamID: 0xe0, counterPID: audioPID, counterID: 0xc0},
	} {
		t.Run(track.name, func(t *testing.T) {
			var gate publicationAVGate
			gate.bindSelectedProgram(program, base, false)
			headerOnly := slateTestPES(track.streamID, 0, 0, false)
			headerOnly = headerOnly[:len(headerOnly)-1]
			headerOnly[4], headerOnly[5] = 0, 8
			first := joinTSPackets(
				testExactPayloadPacket(track.pid, true, 0, headerOnly),
				testTimestampedPESPacket(track.counterPID, track.counterID, 0, 0),
				pcrTestNonPCRPacket(0x1fff, false),
				pcrTestNonPCRPacket(0x1ffe, false),
				pcrTestNonPCRPacket(0x1ffd, false),
			)
			if out, _, fault := gate.push(first, base); fault != nil || len(out) != 0 {
				t.Fatalf("header-only epoch=(bytes=%d fault=%v), want private no-progress", len(out), fault)
			}
			snapshot := gate.timeline.snapshotAt(base)
			if track.pid == audioPID && snapshot.audioSamples != 0 ||
				track.pid == videoPID && snapshot.videoSamples != 0 {
				t.Fatalf("header-only selected PES counted as progress: %+v", snapshot)
			}

			// A bounded empty PES completed as safe absence, so the next real PUSI
			// is a clean resynchronization rather than an abandoned-header fault.
			real := testTimestampedPESPacket(track.pid, track.streamID, 0, 1)
			out, _, fault := gate.push(real, base.Add(time.Millisecond))
			if fault != nil || !bytes.Equal(out, append(first, real...)) {
				t.Fatalf("real ES recovery=(bytes=%d fault=%v), want atomic release", len(out), fault)
			}
		})
	}
}

func TestTransportAVTimelineWithholdsFlags00UntilDeclaredHeaderCompletes(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
	)
	base := time.Unix(1_700_000_099, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID
	longNoPTS := []byte{0, 0, 1, 0xc0, 0, 24, 0x80, 0, 20}
	longNoPTS = append(longNoPTS, make([]byte, 20)...)
	longNoPTS = append(longNoPTS, 0xaa)

	t.Run("split complete header is safe absence", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.bindSelectedProgram(program, base)
		timeline.feedAt(joinTSPackets(
			testExactPayloadPacket(audioPID, true, 0, longNoPTS[:20]),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1ffe, false),
			pcrTestNonPCRPacket(0x1ffd, false),
			pcrTestNonPCRPacket(0x1ffc, false)), base)
		if !timeline.hasIncompleteSelectedPES() {
			t.Fatal("partial no-PTS optional header was not retained")
		}
		timeline.feedAt(testExactPayloadPacket(audioPID, false, 1, longNoPTS[20:]), base.Add(time.Millisecond))
		if snapshot := timeline.snapshotAt(base.Add(time.Millisecond)); snapshot.audioSamples != 0 ||
			snapshot.transportCorruptions != 0 || timeline.hasIncompleteSelectedPES() {
			t.Fatalf("complete no-PTS header was not safe absence: %+v", snapshot)
		}
	})

	t.Run("replacement before complete header is corrupt", func(t *testing.T) {
		var timeline transportAVTimeline
		timeline.bindSelectedProgram(program, base)
		timeline.feedAt(joinTSPackets(
			testExactPayloadPacket(audioPID, true, 0, longNoPTS[:20]),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1ffe, false),
			pcrTestNonPCRPacket(0x1ffd, false),
			pcrTestNonPCRPacket(0x1ffc, false)), base)
		fault := timeline.feedAtAndReportFault(
			testTimestampedPESPacket(audioPID, 0xc0, 0, 1), base.Add(time.Millisecond))
		if fault == nil || fault.kind != avFaultTransport {
			t.Fatalf("superseded no-PTS header fault=%v, want transport", fault)
		}
	})
}

func TestTransportAVTimelineFailsOpenForAmbiguousOrUnassembledProgramMaps(t *testing.T) {
	const (
		pmtPID   = 0x1000
		otherPMT = 0x1001
		videoPID = 0x0100
		audioPID = 0x0101
	)
	buildBody := func(table []byte) []byte {
		return joinTSPackets(
			table,
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false),
			pcrTestNonPCRPacket(0x1fff, false),
		)
	}

	t.Run("multi-program PAT is not bound by PAT order", func(t *testing.T) {
		pat := testPATPacketForTable(1, 0, true, 0, 0,
			testPATProgram{programNumber: 1, pmtPID: pmtPID},
			testPATProgram{programNumber: 2, pmtPID: otherPMT})
		var timeline transportAVTimeline
		timeline.feed(buildBody(pat))
		snapshot := timeline.snapshot()
		if snapshot.continuityEnforced || snapshot.continuityReason != "ambiguous_multi_program_pat" {
			t.Fatalf("ambiguous MPTS policy=%+v", snapshot)
		}
	})

	t.Run("multi-section PAT remains observable but unenforced", func(t *testing.T) {
		pat := testPATPacketForTable(1, 0, true, 0, 1,
			testPATProgram{programNumber: 1, pmtPID: pmtPID})
		var timeline transportAVTimeline
		timeline.feed(buildBody(pat))
		snapshot := timeline.snapshot()
		if snapshot.continuityEnforced || snapshot.continuityReason != "pat_unassembled_or_invalid" {
			t.Fatalf("multi-section PAT policy=%+v", snapshot)
		}
	})

	t.Run("multi-section PMT remains observable but unenforced", func(t *testing.T) {
		pmt := testAVPMTPacket(pmtPID, videoPID, audioPID)
		payload, ok := tsPayload(pmt)
		if !ok {
			t.Fatal("split-PMT fixture lacks payload")
		}
		section := payload[1+int(payload[0]):]
		total := 3 + (int(section[1]&0x0f)<<8 | int(section[2]))
		section[7] = 1
		writeMPEG2CRC(section[:total])
		var timeline transportAVTimeline
		timeline.feed(joinTSPackets(
			testPATPacket(pmtPID), pmt,
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false)))
		snapshot := timeline.snapshot()
		if snapshot.continuityEnforced || snapshot.continuityReason != "pmt_unassembled_or_invalid" {
			t.Fatalf("multi-section PMT policy=%+v", snapshot)
		}
	})

	t.Run("CRC-valid PMT with trailing partial ES is rejected", func(t *testing.T) {
		valid := testAVPMTPacket(pmtPID, videoPID, audioPID)
		payload, ok := tsPayload(valid)
		if !ok {
			t.Fatal("malformed-PMT fixture lacks payload")
		}
		oldSection := payload[1+int(payload[0]):]
		oldTotal := 3 + (int(oldSection[1]&0x0f)<<8 | int(oldSection[2]))
		malformedSection := make([]byte, oldTotal+2)
		copy(malformedSection, oldSection[:oldTotal-4])
		malformedSection[oldTotal-4] = 0x0f
		malformedSection[oldTotal-3] = 0xe1
		sectionLength := len(malformedSection) - 3
		malformedSection[1] = 0xb0 | byte(sectionLength>>8)&0x0f
		malformedSection[2] = byte(sectionLength)
		writeMPEG2CRC(malformedSection)
		malformed := testExactPayloadPacket(pmtPID, true, 0,
			append([]byte{0}, malformedSection...))
		var timeline transportAVTimeline
		timeline.feed(joinTSPackets(
			testPATPacket(pmtPID), malformed,
			testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
			testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
			pcrTestNonPCRPacket(0x1fff, false)))
		snapshot := timeline.snapshot()
		if snapshot.continuityEnforced || snapshot.continuityReason != "pmt_unassembled_or_invalid" {
			t.Fatalf("partial-ES PMT policy=%+v", snapshot)
		}
	})
}

func TestTransportAVTimelineDiscontinuityStartsFreshDiagnosticEpoch(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 1),
		testTimestampedPESPacket(audioPID, 0xc0, 14*transportPTSRate, 1),
		testAdaptationOnlyDiscontinuity(videoPID, 2),
		testAdaptationOnlyDiscontinuity(audioPID, 2),
		testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 3),
		testTimestampedPESPacket(audioPID, 0xc0, 100*transportPTSRate, 3),
		testTimestampedPESPacket(videoPID, 0xe0, 110*transportPTSRate, 4),
		testTimestampedPESPacket(audioPID, 0xc0, 110*transportPTSRate, 4),
	)
	var timeline transportAVTimeline
	timeline.feed(data)
	snapshot := timeline.snapshot()
	if !snapshot.valid || snapshot.videoSamples != 2 || snapshot.audioSamples != 2 ||
		snapshot.drift != 0 || snapshot.initialSkew != 0 || snapshot.finalSkew != 0 {
		t.Fatalf("post-discontinuity diagnostic retained stale epoch: %+v", snapshot)
	}
}

func TestTransportAVTimelineAudioPIDChangeStartsSharedEpoch(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		oldAudio = 0x0101
		newAudio = 0x0102
	)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, oldAudio),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(oldAudio, 0xc0, 5*transportPTSRate, 0),
		testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 1),
		testTimestampedPESPacket(oldAudio, 0xc0, 15*transportPTSRate, 1),
		// Only the audio PID changes. The video track must still begin a new
		// comparison epoch so its old span is not compared with fresh audio.
		testAVPMTPacket(pmtPID, videoPID, newAudio),
		testTimestampedPESPacket(oldAudio, 0xc0, 500*transportPTSRate, 2),
		testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 2),
		testTimestampedPESPacket(newAudio, 0xc0, 100*transportPTSRate, 0),
		testTimestampedPESPacket(videoPID, 0xe0, 110*transportPTSRate, 3),
		testTimestampedPESPacket(newAudio, 0xc0, 110*transportPTSRate, 1),
	)
	var timeline transportAVTimeline
	timeline.feed(data)
	assertFreshHealthyTimeline(t, timeline.snapshot(), "audio PID change")
}

func TestTransportAVTimelineAudioOnlyDiscontinuityStartsSharedEpoch(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
	)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacket(pmtPID, videoPID, audioPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 5*transportPTSRate, 0),
		testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 1),
		testTimestampedPESPacket(audioPID, 0xc0, 15*transportPTSRate, 1),
		testAdaptationOnlyDiscontinuity(audioPID, 2),
		testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 2),
		testTimestampedPESPacket(audioPID, 0xc0, 100*transportPTSRate, 3),
		testTimestampedPESPacket(videoPID, 0xe0, 110*transportPTSRate, 3),
		testTimestampedPESPacket(audioPID, 0xc0, 110*transportPTSRate, 4),
	)
	var timeline transportAVTimeline
	timeline.feed(data)
	assertFreshHealthyTimeline(t, timeline.snapshot(), "audio-only discontinuity")
}

func TestTransportAVTimelinePATRebindClearsStaleProgramUntilFreshPMT(t *testing.T) {
	const (
		oldPMTPID = 0x1000
		newPMTPID = 0x1100
		videoPID  = 0x0100
		audioPID  = 0x0101
		oldPCRPID = 0x0150
		newPCRPID = 0x0250
	)
	invalidNewPMT := testAVPMTPacketWithPCR(newPMTPID, videoPID, audioPID, newPCRPID)
	invalidPayload, ok := tsPayload(invalidNewPMT)
	if !ok {
		t.Fatal("new diagnostic PMT fixture has no payload")
	}
	invalidSection := invalidPayload[1+int(invalidPayload[0]):]
	invalidTotal := 3 + (int(invalidSection[1]&0x0f)<<8 | int(invalidSection[2]))
	invalidSection[invalidTotal-1] ^= 0x01
	data := joinTSPackets(
		testPATPacket(oldPMTPID),
		testAVPMTPacketWithPCR(oldPMTPID, videoPID, audioPID, oldPCRPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 1),
		testTimestampedPESPacket(audioPID, 0xc0, 10*transportPTSRate, 1),
		// Rebind the PAT first. Delayed packets from the old program reuse the
		// same ES PIDs but must remain unselected through a CRC-invalid table and
		// until the new PMT is valid.
		testPATPacket(newPMTPID),
		invalidNewPMT,
		testTimestampedPESPacket(videoPID, 0xe0, 500*transportPTSRate, 2),
		testTimestampedPESPacket(audioPID, 0xc0, 700*transportPTSRate, 2),
		testAVPMTPacketWithPCR(newPMTPID, videoPID, audioPID, newPCRPID),
		testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 3),
		testTimestampedPESPacket(audioPID, 0xc0, 100*transportPTSRate, 3),
		testTimestampedPESPacket(videoPID, 0xe0, 110*transportPTSRate, 4),
		testTimestampedPESPacket(audioPID, 0xc0, 110*transportPTSRate, 4),
	)
	var timeline transportAVTimeline
	timeline.feed(data)
	snapshot := timeline.snapshot()
	assertFreshHealthyTimeline(t, snapshot, "PAT/PMT rebind")
	if timeline.pmtPID != newPMTPID || timeline.pcrPID != newPCRPID {
		t.Fatalf("selected program=(PMT %#x PCR %#x), want (%#x %#x)",
			timeline.pmtPID, timeline.pcrPID, newPMTPID, newPCRPID)
	}
}

func TestTransportAVTimelinePCRDiscontinuityStartsSharedEpoch(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
		pcrPID   = 0x0150
	)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacketWithPCR(pmtPID, videoPID, audioPID, pcrPID),
		pcrTestChunkForPID(pcrPID, 0, false)[:tsPacketSize],
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 5*transportPTSRate, 0),
		testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 1),
		testTimestampedPESPacket(audioPID, 0xc0, 15*transportPTSRate, 1),
		testAdaptationOnlyDiscontinuity(pcrPID, 0),
		testTimestampedPESPacket(videoPID, 0xe0, 100*transportPTSRate, 2),
		testTimestampedPESPacket(audioPID, 0xc0, 100*transportPTSRate, 2),
		testTimestampedPESPacket(videoPID, 0xe0, 110*transportPTSRate, 3),
		testTimestampedPESPacket(audioPID, 0xc0, 110*transportPTSRate, 3),
	)
	var timeline transportAVTimeline
	timeline.feed(data)
	assertFreshHealthyTimeline(t, timeline.snapshot(), "PCR-only discontinuity")
}

func TestTransportAVTimelineUnchangedTablesPreserveEpoch(t *testing.T) {
	const (
		pmtPID   = 0x1000
		videoPID = 0x0100
		audioPID = 0x0101
		pcrPID   = 0x0150
	)
	data := joinTSPackets(
		testPATPacket(pmtPID),
		testAVPMTPacketWithPCR(pmtPID, videoPID, audioPID, pcrPID),
		testTimestampedPESPacket(videoPID, 0xe0, 0, 0),
		testTimestampedPESPacket(audioPID, 0xc0, 0, 0),
		testPATPacket(pmtPID),
		testAVPMTPacketWithPCR(pmtPID, videoPID, audioPID, pcrPID),
		testTimestampedPESPacket(videoPID, 0xe0, 10*transportPTSRate, 1),
		testTimestampedPESPacket(audioPID, 0xc0, 10*transportPTSRate, 1),
	)
	var timeline transportAVTimeline
	timeline.feed(data)
	snapshot := timeline.snapshot()
	if !snapshot.valid || snapshot.videoSamples != 2 || snapshot.audioSamples != 2 ||
		snapshot.drift != 0 || snapshot.initialSkew != 0 || snapshot.finalSkew != 0 {
		t.Fatalf("unchanged PAT/PMT reset or corrupted epoch: %+v", snapshot)
	}
}

func assertFreshHealthyTimeline(t *testing.T, snapshot avTimelineSnapshot, transition string) {
	t.Helper()
	if !snapshot.valid || snapshot.videoSamples != 2 || snapshot.audioSamples != 2 ||
		snapshot.drift != 0 || snapshot.initialSkew != 0 || snapshot.finalSkew != 0 {
		t.Fatalf("%s retained a cross-epoch comparison: %+v", transition, snapshot)
	}
}

func testAVPMTPacket(pmtPID, videoPID, audioPID int) []byte {
	return testAVPMTPacketWithPCR(pmtPID, videoPID, audioPID, videoPID)
}

func testAVPMTPacketWithPCR(pmtPID, videoPID, audioPID, pcrPID int) []byte {
	section := make([]byte, 26)
	section[0] = 0x02
	section[1], section[2] = 0xb0, 23
	section[3], section[4] = 0, 1
	section[5] = 0xc1
	section[8], section[9] = 0xe0|byte(pcrPID>>8), byte(pcrPID)
	section[10], section[11] = 0xf0, 0
	section[12] = 0x1b
	section[13], section[14] = 0xe0|byte(videoPID>>8), byte(videoPID)
	section[15], section[16] = 0xf0, 0
	section[17] = 0x0f
	section[18], section[19] = 0xe0|byte(audioPID>>8), byte(audioPID)
	section[20], section[21] = 0xf0, 0
	writeMPEG2CRC(section)
	payload := append([]byte{0}, section...)
	return testExactPayloadPacket(pmtPID, true, 0, payload)
}

func testVideoOnlyPMTPacket(pmtPID, videoPID int) []byte {
	section := make([]byte, 21)
	section[0] = 0x02
	section[1], section[2] = 0xb0, 18
	section[3], section[4] = 0, 1
	section[5] = 0xc1
	section[8], section[9] = 0xe0|byte(videoPID>>8), byte(videoPID)
	section[10], section[11] = 0xf0, 0
	section[12] = 0x1b
	section[13], section[14] = 0xe0|byte(videoPID>>8), byte(videoPID)
	section[15], section[16] = 0xf0, 0
	writeMPEG2CRC(section)
	payload := append([]byte{0}, section...)
	return testExactPayloadPacket(pmtPID, true, 0, payload)
}

func testMultiAudioPMTPacket(pmtPID, videoPID, firstAudioPID, secondAudioPID int) []byte {
	section := make([]byte, 31)
	section[0] = 0x02
	section[1], section[2] = 0xb0, 28
	section[3], section[4] = 0, 1
	section[5] = 0xc1
	section[8], section[9] = 0xe0|byte(videoPID>>8), byte(videoPID)
	section[10], section[11] = 0xf0, 0
	section[12] = 0x1b
	section[13], section[14] = 0xe0|byte(videoPID>>8), byte(videoPID)
	section[15], section[16] = 0xf0, 0
	section[17] = 0x0f
	section[18], section[19] = 0xe0|byte(firstAudioPID>>8), byte(firstAudioPID)
	section[20], section[21] = 0xf0, 0
	section[22] = 0x0f
	section[23], section[24] = 0xe0|byte(secondAudioPID>>8), byte(secondAudioPID)
	section[25], section[26] = 0xf0, 0
	writeMPEG2CRC(section)
	payload := append([]byte{0}, section...)
	return testExactPayloadPacket(pmtPID, true, 0, payload)
}

func testTimestampedPESPacket(pid int, streamID byte, pts uint64, continuity byte) []byte {
	payload := slateTestPES(streamID, pts, 0, false)
	return testExactPayloadPacket(pid, true, continuity, payload)
}

func testPayloadOnlyTimestampedPESPacket(pid int, streamID byte, pts uint64, continuity byte) []byte {
	payload := slateTestPES(streamID, pts, 0, false)
	packet := make([]byte, tsPacketSize)
	for i := range packet {
		packet[i] = 0xff
	}
	packet[0] = 0x47
	packet[1] = 0x40 | byte(pid>>8)&0x1f
	packet[2] = byte(pid)
	packet[3] = 0x10 | continuity&0x0f
	copy(packet[4:], payload)
	return packet
}

// The production path feeds a publication gate's complete released prefix to
// transportPCRPacer in chunkSize slices. A timestamp-only assertion misses the
// failure: with v0.44.6's first-held-byte timestamp, a clocked multi-chunk
// release near the gate boundary leaves less than one maximum-rate PCR interval
// before paceLimit and reaches errTransportRecoveryBacklog. The release instant
// gives that same validated media its independent pacing window without
// weakening either the gate's hold bound or the pacer's real source-age bound.
func TestPublicationAVGateReleasePreservesPacerBudget(t *testing.T) {
	const (
		videoPID = 0x0100
		audioPID = 0x0101
		pcrPID   = 0x0102
	)
	base := time.Unix(1_700_000_096, 0)
	program := avProgramPIDs{videoPID: videoPID, pcrPID: pcrPID, audioCount: 1}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioPID

	// Build one continuous, packet-aligned prefix spanning more than three
	// production fanout chunks. Three selected-PCR samples establish a real
	// positive pacing schedule; three ordinary 3s-skewed A/V pairs keep the
	// entire prefix private until the counterpart catches up.
	prefixBytes := 3 * chunkSize
	transportBytes := ((prefixBytes + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
	prefix := make([]byte, transportBytes)
	nullPacket := pcrTestNonPCRPacket(0x1fff, false)
	for offset := 0; offset < len(prefix); offset += tsPacketSize {
		copy(prefix[offset:offset+tsPacketSize], nullPacket)
	}
	packetIndex := 0
	for index := 0; index < 3; index++ {
		videoPTS := uint64(index) * transportPTSRate
		for _, packet := range [][]byte{
			testTimestampedPESPacket(videoPID, 0xe0, videoPTS, byte(index)),
			testTimestampedPESPacket(audioPID, 0xc0, videoPTS+3*transportPTSRate, byte(index)),
		} {
			copy(prefix[packetIndex*tsPacketSize:], packet)
			packetIndex++
		}
	}
	for index := 0; index < 3; index++ {
		offset := ((index*chunkSize + tsPacketSize - 1) / tsPacketSize) * tsPacketSize
		if offset < packetIndex*tsPacketSize {
			offset = packetIndex * tsPacketSize
		}
		packet := pcrTestChunkForPID(pcrPID,
			durationTicks(time.Duration(index)*50*time.Millisecond, transportClockRate), false)[:tsPacketSize]
		packet[3] = packet[3]&0xf0 | byte(index)
		copy(prefix[offset:offset+tsPacketSize], packet)
	}
	catchUp := joinTSPackets(
		testTimestampedPESPacket(videoPID, 0xe0, 6*transportPTSRate, 3),
		testTimestampedPESPacket(audioPID, 0xc0, 6*transportPTSRate, 3),
	)

	newPendingGate := func(t *testing.T) *publicationAVGate {
		t.Helper()
		gate := &publicationAVGate{}
		gate.bindSelectedProgram(program, base, false)
		if out, _, fault := gate.push(prefix, base); len(out) != 0 || fault != nil {
			t.Fatalf("clocked skewed prefix=(bytes=%d fault=%v), want private candidate", len(out), fault)
		}
		return gate
	}
	releaseGate := func(t *testing.T, hold time.Duration) (*publicationAVGate, []byte, time.Time) {
		t.Helper()
		gate := newPendingGate(t)
		release := base.Add(hold)
		out, arrival, fault := gate.push(catchUp, release)
		if fault != nil || len(out) != len(prefix)+len(catchUp) || !arrival.Equal(release) {
			t.Fatalf("catch-up release=(bytes=%d arrival=%v fault=%v), want %d bytes from %v",
				len(out), arrival, fault, len(prefix)+len(catchUp), release)
		}
		return gate, out, arrival
	}
	newPacer := func(t *testing.T, wall *time.Time) *transportPCRPacer {
		t.Helper()
		pacer := &transportPCRPacer{}
		pacer.Configure(pcrPID, false)
		pacer.now = func() time.Time { return *wall }
		pacer.wait = func(_ context.Context, delay time.Duration) error {
			*wall = wall.Add(delay)
			return nil
		}
		if err := pacer.PrepareBufferedPrefix(prefix, chunkSize); err != nil {
			t.Fatalf("prepare clocked prefix: %v", err)
		}
		return pacer
	}
	pace := func(pacer *transportPCRPacer, media []byte, arrivedAt time.Time) error {
		for len(media) > 0 {
			partLen := min(len(media), chunkSize)
			if err := pacer.Wait(context.Background(), media[:partLen], arrivedAt); err != nil {
				return err
			}
			media = media[partLen:]
		}
		return nil
	}

	t.Run("old first-arrival semantics reach real pacing backlog", func(t *testing.T) {
		hold := maxPublicationAVGateHold - time.Millisecond
		_, out, _ := releaseGate(t, hold)
		wall := base.Add(hold)
		pacer := newPacer(t, &wall)
		// v0.44.6 returned gate.pendingArrival (base) for this release. The
		// budget exhaustion it proves now terminates as a live-edge skip
		// (skip-ahead) rather than a failed attempt.
		if err := pace(pacer, out, base); !errors.Is(err, errTransportSkipStale) {
			t.Fatalf("old first-arrival pacing error=%v, want %v", err, errTransportSkipStale)
		}
	})

	for _, hold := range []time.Duration{
		300 * time.Millisecond,
		500 * time.Millisecond,
		900 * time.Millisecond,
		maxPublicationAVGateHold - time.Millisecond,
	} {
		t.Run("fixed "+hold.String(), func(t *testing.T) {
			release := base.Add(hold)
			_, out, arrival := releaseGate(t, hold)
			wall := release
			pacer := newPacer(t, &wall)
			if err := pace(pacer, out, arrival); err != nil {
				t.Fatalf("release pacing at hold %s: %v", hold, err)
			}
			if elapsed := wall.Sub(arrival); elapsed <= 0 || elapsed > maxTransportAddedLatency {
				t.Fatalf("release pacing elapsed=%s, want positive and <=%s", elapsed, maxTransportAddedLatency)
			}
		})
	}

	for _, age := range []time.Duration{
		maxPublicationAVGateHold,
		maxPublicationAVGateHold + time.Nanosecond,
	} {
		t.Run("gate rejects "+age.String(), func(t *testing.T) {
			gate := newPendingGate(t)
			deadline := base.Add(age)
			out, arrival, fault := gate.push(catchUp, deadline)
			if len(out) != 0 || fault == nil || fault.kind != avFaultTimelineSkew ||
				!arrival.Equal(deadline) || len(gate.pending) != 0 {
				t.Fatalf("bounded gate=(bytes=%d arrival=%v fault=%v pending=%d), want private skew rejection",
					len(out), arrival, fault, len(gate.pending))
			}
		})
	}

	t.Run("genuinely pre-aged post-release media still fails", func(t *testing.T) {
		hold := 500 * time.Millisecond
		gate, out, arrival := releaseGate(t, hold)
		wall := base.Add(hold)
		pacer := newPacer(t, &wall)
		if err := pace(pacer, out, arrival); err != nil {
			t.Fatalf("initial release pacing: %v", err)
		}
		postReleaseArrival := wall
		postRelease := joinTSPackets(
			testTimestampedPESPacket(videoPID, 0xe0, 7*transportPTSRate, 4),
			testTimestampedPESPacket(audioPID, 0xc0, 7*transportPTSRate, 4),
			pcrTestChunkForPID(pcrPID, durationTicks(150*time.Millisecond, transportClockRate), false)[:tsPacketSize],
		)
		postRelease[2*tsPacketSize+3] = postRelease[2*tsPacketSize+3]&0xf0 | 3
		media, directArrival, fault := gate.push(postRelease, postReleaseArrival)
		if fault != nil || !bytes.Equal(media, postRelease) || !directArrival.Equal(postReleaseArrival) {
			t.Fatalf("direct post-release media=(bytes=%d arrival=%v fault=%v), want unchanged",
				len(media), directArrival, fault)
		}
		wall = postReleaseArrival.Add(maxTransportAddedLatency + time.Nanosecond)
		if err := pace(pacer, media, directArrival); !errors.Is(err, errTransportStaleChunk) {
			t.Fatalf("pre-aged post-release pacing error=%v, want %v", err, errTransportStaleChunk)
		}
	})
}
