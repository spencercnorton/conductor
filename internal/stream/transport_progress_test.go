package stream

import (
	"testing"
	"time"
)

func TestTransportAVJointProgressRequiresAnotherPESFromBothTracks(t *testing.T) {
	var timeline transportAVTimeline
	at := time.Unix(1_700_000_000, 0)
	timeline.feedAt(joinTSPackets(
		testPATPacket(0x1000), testAVPMTPacket(0x1000, 0x100, 0x101),
		testTimestampedPESPacket(0x100, 0xe0, 90000, 0),
		testTimestampedPESPacket(0x101, 0xc0, 90000, 0),
	), at)
	if drift := timeline.sustainedDriftLocked(&timeline.audio[0]); drift != 0 {
		t.Fatalf("first coexistence drift=%s, want zero", drift)
	}
	timeline.feedAt(testTimestampedPESPacket(0x100, 0xe0, 189000, 1), at)
	if drift := timeline.sustainedDriftLocked(&timeline.audio[0]); drift != 0 {
		t.Fatalf("unmatched video advance charged as joint drift: %s", drift)
	}
	// An accepted PES with a frozen audio clock still completes the pair;
	// requiring a positive timestamp step would hide this real divergence.
	timeline.feedAt(testTimestampedPESPacket(0x101, 0xc0, 90000, 1), at)
	if drift := timeline.sustainedDriftLocked(&timeline.audio[0]); drift != -1100*time.Millisecond {
		t.Fatalf("paired frozen audio drift=%s, want -1.1s", drift)
	}
}

// Audio appears after 1.8 seconds of video, then the two clocks advance
// together. All packets intentionally enter ONE feed: establishing the drift
// origin on the first later guard query would erase the divergence controls.
func jointProgressTestEpoch(audioMode string) []byte {
	const videoPID, audioPID, pmtPID = 0x100, 0x101, 0x1000
	packets := [][]byte{testPATPacket(pmtPID), testAVPMTPacket(pmtPID, videoPID, audioPID)}
	var videoCC, audioCC byte
	for i := 0; i <= 18; i++ {
		packets = append(packets, testTimestampedPESPacket(videoPID, 0xe0, uint64(i)*9000, videoCC))
		videoCC++
	}
	packets = append(packets, testTimestampedPESPacket(audioPID, 0xc0, 18*9000, audioCC))
	audioCC++
	for i := 1; i <= 30; i++ {
		videoPTS := uint64(18+i) * 9000
		audioPTS := videoPTS
		switch audioMode {
		case "new_drift":
			// A new 1.1-second divergence after establishment is inside the
			// initial 1.8-second envelope and leaves raw span drift below 1s.
			if i > 10 {
				audioPTS += uint64(min(i-10, 10)) * 9900
			}
		case "frozen_clock":
			audioPTS = 18 * 9000
		}
		packets = append(packets,
			testTimestampedPESPacket(videoPID, 0xe0, videoPTS, videoCC),
			testTimestampedPESPacket(audioPID, 0xc0, audioPTS, audioCC))
		videoCC++
		audioCC++
	}
	return joinTSPackets(packets...)
}

func TestTransportAVContinuityUsesEstablishedProgress(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wantDrift bool
	}{
		{"healthy", false},
		{"new_drift", true},
		{"frozen_clock", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var timeline transportAVTimeline
			base := time.Unix(1_700_000_000, 0)
			timeline.feedAt(jointProgressTestEpoch(tc.name), base)
			snapshot := timeline.snapshotAt(base)
			if !snapshot.valid || snapshot.transportCorruptions != 0 || snapshot.initialSkew != 1800*time.Millisecond {
				t.Fatalf("invalid establishment fixture: %+v", snapshot)
			}
			if tc.name == "new_drift" && (snapshot.finalSkew != 1100*time.Millisecond || absoluteDuration(snapshot.drift) >= time.Second) {
				t.Fatalf("divergence did not remain inside original envelope/raw drift threshold: %+v", snapshot)
			}
			policy := defaultAVContinuityPolicy()
			if fault := timeline.continuityFault(base, policy); fault != nil {
				t.Fatalf("unconfirmed fault=%v", fault)
			}
			fault := timeline.continuityFault(base.Add(policy.confirmation+time.Millisecond), policy)
			if tc.wantDrift {
				if fault == nil || fault.kind != avFaultTimelineDrift {
					t.Fatalf("post-establishment divergence fault=%+v, want timeline drift; raw snapshot=%+v", fault, snapshot)
				}
			} else if fault != nil {
				t.Fatalf("unequal initial track coverage became a sustained fault: %+v; raw snapshot=%+v", fault, snapshot)
			}
		})
	}
}

func TestTransportAVJointProgressResetsAtSharedBoundaryAndCorruption(t *testing.T) {
	for _, boundary := range []string{"declared_discontinuity", "transport_corruption"} {
		t.Run(boundary, func(t *testing.T) {
			var timeline transportAVTimeline
			base := time.Unix(1_700_000_000, 0)
			policy := defaultAVContinuityPolicy()
			timeline.feedAt(jointProgressTestEpoch("new_drift"), base)
			_ = timeline.continuityFault(base, policy)
			if fault := timeline.continuityFault(base.Add(policy.confirmation+time.Millisecond), policy); fault == nil || fault.kind != avFaultTimelineDrift {
				t.Fatalf("old epoch did not establish drift: %v", fault)
			}
			next := base.Add(3 * time.Second)
			if boundary == "declared_discontinuity" {
				timeline.feedAt(joinTSPackets(testAdaptationOnlyDiscontinuity(0x100, 0), testAdaptationOnlyDiscontinuity(0x101, 0)), next)
			} else {
				corrupt := testTimestampedPESPacket(0x100, 0xe0, 7*transportPTSRate, 1)
				corrupt[1] |= 0x80
				if fault := timeline.feedAtAndReportFault(corrupt, next); fault == nil || fault.kind != avFaultTransport {
					t.Fatalf("corruption boundary fault=%v, want immediate transport fault", fault)
				}
			}
			// A sane, later epoch resumes within the checked PTS recovery step.
			// Its unequal initial coverage must not inherit the old drift anchor.
			media := jointProgressTestEpoch("healthy")
			for offset := 0; offset < len(media); offset += tsPacketSize {
				packet := media[offset : offset+tsPacketSize]
				pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
				if pid == 0x100 || pid == 0x101 {
					rewritePESTimestamps(media, offset, pid, 10*transportPTSRate)
				}
			}
			timeline.feedAt(media, next)
			if fault := timeline.continuityFault(next, policy); fault != nil {
				t.Fatalf("new epoch inherited old confirmed fault: %v", fault)
			}
			if fault := timeline.continuityFault(next.Add(policy.confirmation+time.Millisecond), policy); fault != nil {
				t.Fatalf("healthy resumed epoch retained prior comparison: %v; snapshot=%+v", fault, timeline.snapshotAt(next))
			}
		})
	}
}

func TestTransportAVJointProgressKeepsMappedOriginAcrossOtherAudioLifecycle(t *testing.T) {
	for _, lifecycle := range []string{"unmapped_corruption", "unmapped_reset"} {
		t.Run(lifecycle, func(t *testing.T) {
			const secondPID = 0x102
			base := time.Unix(1_700_000_100, 0)
			policy := defaultAVContinuityPolicy()
			media := jointProgressTestEpoch("new_drift")
			copy(media[tsPacketSize:2*tsPacketSize], testMultiAudioPMTPacket(0x1000, 0x100, 0x101, secondPID))
			// The second language establishes at zero; the mapped first
			// language establishes at 1.8s and later accumulates +1.1s drift.
			var timeline transportAVTimeline
			timeline.requireFirstAudio = true
			timeline.feedAt(joinTSPackets(media[:2*tsPacketSize],
				testTimestampedPESPacket(secondPID, 0xc1, 0, 0), media[2*tsPacketSize:]), base)
			secondAt := base.Add(10 * time.Millisecond)
			timeline.feedAt(testTimestampedPESPacket(secondPID, 0xc1, 48*9000, 1), secondAt)
			if timeline.requiredAudioTrackLocked() != &timeline.audio[1] {
				t.Fatal("second language did not become freshest")
			}
			if fault := timeline.continuityFault(secondAt, policy); fault != nil {
				t.Fatalf("alternate language inherited mapped drift: %v", fault)
			}
			if lifecycle == "unmapped_corruption" {
				bad := testTimestampedPESPacket(secondPID, 0xc1, 49*9000, 2)
				bad[1] |= 0x80
				if fault := timeline.feedAtAndReportFault(bad, secondAt); fault != nil {
					t.Fatalf("unmapped corruption poisoned selected input: %v", fault)
				}
			} else {
				// Exercise the isolated reset used while assembling a boundary;
				// a complete shared epoch reset has its own control above.
				timeline.resetSelectedTrackForBoundary(secondPID)
			}
			firstAt := base.Add(20 * time.Millisecond)
			timeline.feedAt(testTimestampedPESPacket(0x101, 0xc0, 59*9000, 31), firstAt)
			if timeline.requiredAudioTrackLocked() != &timeline.audio[0] {
				t.Fatal("mapped language did not regain selection")
			}
			_ = timeline.continuityFault(firstAt, policy)
			fault := timeline.continuityFault(firstAt.Add(policy.confirmation+time.Millisecond), policy)
			if fault == nil || fault.kind != avFaultTimelineDrift || fault.drift != 1100*time.Millisecond {
				t.Fatalf("other language lifecycle erased/replaced mapped origin: fault=%+v", fault)
			}
		})
	}
}
