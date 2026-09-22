package stream

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestCheckedInputRepairDelegatesOnlyEstablishedAudioSlope(t *testing.T) {
	for _, name := range []string{"strict", "audio_slope", "video_changed", "abrupt_audio", "backward_audio"} {
		t.Run(name, func(t *testing.T) {
			base := time.Unix(1700000000, 0)
			prefix, pts, vcc, acc := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
			checked := newCheckedTranscodeInput()
			checked.allowAudioClockRepair = name != "strict"
			if out, err := checked.push(prefix, base); err != nil || !bytes.Equal(out, prefix) {
				t.Fatalf("prefix bytes=%d error=%v", len(out), err)
			}
			var fault error
			var emitted, sent []byte
			for i := 0; i < 80 && fault == nil; i++ {
				videoPTS := pts + uint64(i)*transportPTSRate/50
				if name == "video_changed" {
					videoPTS = pts + uint64(i)*transportPTSRate/25
				}
				audioPTS := pts + uint64(i)*transportPTSRate/10
				if name == "abrupt_audio" {
					audioPTS += durationTicks(3800*time.Millisecond, transportPTSRate)
				}
				if name == "backward_audio" {
					audioPTS = (audioPTS + ptsModulus - durationTicks(2800*time.Millisecond, transportPTSRate)) % ptsModulus
				}
				data := joinTSPackets(testTimestampedPESPacket(0x100, 0xe0, videoPTS, vcc), testTimestampedPESPacket(0x101, 0xc0, audioPTS, acc))
				sent = append(sent, data...)
				var out []byte
				out, fault = checked.push(data, base.Add(time.Duration(i+1)*40*time.Millisecond))
				emitted = append(emitted, out...)
				vcc, acc = (vcc+1)&15, (acc+1)&15
			}
			if name == "audio_slope" {
				if fault != nil || !bytes.Equal(emitted, sent) {
					t.Fatalf("repairable slope lost bytes: %d/%d error=%v", len(emitted), len(sent), fault)
				}
			} else {
				var continuity *avContinuityError
				if !errors.As(fault, &continuity) || continuity.kind != avFaultTimelineDrift {
					t.Fatalf("ineligible case did not retain drift rejection: %v", fault)
				}
			}
		})
	}
}

func TestCheckedInputRepairRetainsPrivateEpochEstablishment(t *testing.T) {
	for _, boundary := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "after_boundary"}[boundary], func(t *testing.T) {
			base := time.Unix(1700000000, 0)
			prefix, pts, vcc, acc := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
			checked := newCheckedTranscodeInput()
			checked.allowAudioClockRepair = true
			if boundary {
				if _, err := checked.push(prefix, base); err != nil || !checked.safety.repairReady {
					t.Fatalf("initial repair establishment: %v", err)
				}
				if out, err := checked.push(testAdaptationOnlyDiscontinuity(0x101, acc), base); err != nil || len(out) != 0 {
					t.Fatalf("boundary escaped: %d %v", len(out), err)
				}
				acc = (acc + 1) & 15
			} else {
				pts, vcc, acc = 0, 0, 0
				prefix = joinTSPackets(testPATPacket(0x1000), testAVPMTPacket(0x1000, 0x100, 0x101))
			}
			data := joinTSPackets(testTimestampedPESPacket(0x100, 0xe0, pts, vcc), testTimestampedPESPacket(0x101, 0xc0, pts+3*transportPTSRate, acc))
			if !boundary {
				data = append(prefix, data...)
				data = append(data, pcrTestNonPCRPacket(0x1ffe, false)...)
			}
			if out, err := checked.push(data, base); err != nil || len(out) != 0 || checked.safety.repairReady {
				t.Fatalf("unestablished skew escaped: %d %v ready=%v", len(out), err, checked.safety.repairReady)
			}
			fault := checked.safety.continuityFault(base.Add(maxPublicationAVGateHold), defaultAVContinuityPolicy())
			if fault == nil || fault.kind != avFaultTimelineSkew {
				t.Fatalf("private epoch lost hold bound: %v", fault)
			}
		})
	}
}

func TestCheckedInputRepairRetainsTransportAndGrossFences(t *testing.T) {
	prefix, pts, _, acc := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"sync", func(p []byte) { p[0] = 0 }},
		{"tei", func(p []byte) { p[1] |= 0x80 }},
		{"scramble", func(p []byte) { p[3] |= 0x80 }},
		{"continuity", func(p []byte) { p[3] = (p[3] & 0xf0) | ((acc + 2) & 15) }},
		{"pes", func(p []byte) { payload, _ := tsPayload(p); payload[9] &^= 1 }},
		{"gross_forward", func(p []byte) { copy(p, testTimestampedPESPacket(0x101, 0xc0, pts+31*transportPTSRate, acc)) }},
		{"gross_backward", func(p []byte) {
			copy(p, testTimestampedPESPacket(0x101, 0xc0, (pts+ptsModulus-4*transportPTSRate)%ptsModulus, acc))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := testTimestampedPESPacket(0x101, 0xc0, pts, acc)
			tc.mutate(bad)
			for split := 1; split < tsPacketSize; split++ {
				checked := newCheckedTranscodeInput()
				checked.allowAudioClockRepair = true
				base := time.Unix(1700000000, 0)
				if _, err := checked.push(prefix, base); err != nil {
					t.Fatal(err)
				}
				if out, err := checked.push(bad[:split], base); err != nil || len(out) != 0 {
					t.Fatalf("split %d escaped: %d %v", split, len(out), err)
				}
				out, err := checked.push(bad[split:], base)
				var fault *avContinuityError
				if len(out) != 0 || !errors.As(err, &fault) || fault.kind != avFaultTransport {
					t.Fatalf("split %d lost fence: %d %v", split, len(out), err)
				}
			}
		})
	}
}

func TestCheckedInputRepairRetainsStarvation(t *testing.T) {
	base := time.Unix(1700000000, 0)
	prefix, pts, vcc, _ := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
	checked := newCheckedTranscodeInput()
	checked.allowAudioClockRepair = true
	if _, err := checked.push(prefix, base); err != nil {
		t.Fatal(err)
	}
	for _, age := range []time.Duration{18 * time.Second, 20 * time.Second} {
		if _, err := checked.push(testTimestampedPESPacket(0x100, 0xe0, pts, vcc), base.Add(age)); err != nil {
			t.Fatal(err)
		}
		pts += transportPTSRate / 50
		vcc = (vcc + 1) & 15
		fault := checked.safety.continuityFault(base.Add(age), defaultAVContinuityPolicy())
		if age == 20*time.Second && (fault == nil || fault.kind != avFaultAudioStarvation || !inputFaultProvesUpstreamWithoutOutput(fault)) {
			t.Fatalf("starvation delegated to output: %v", fault)
		}
	}
}

func TestCheckedInputRepairDoesNotInheritClockEligibility(t *testing.T) {
	// Even a repair-mode gate cannot grant eligibility from a single sane pair
	// or borrow the previous programme's established cadence after rebinding.
	base := time.Unix(1700000000, 0)
	for _, rebound := range []bool{false, true} {
		var gate publicationAVGate
		gate.mode = publicationGateRepairableInput
		program := avProgramPIDs{videoPID: 0x100, audioPIDs: [maxAVTimelineAudioPIDs]int{0x101}, audioCount: 1}
		gate.bindSelectedProgram(program, base, true)
		if rebound {
			prefix, _, _, _ := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
			if _, _, err := gate.push(prefix, base); err != nil || !gate.repairReady {
				t.Fatalf("establish old programme: %v", err)
			}
			gate.bindSelectedProgram(program, base, true)
		}
		data := joinTSPackets(testTimestampedPESPacket(0x100, 0xe0, 0, 0), testTimestampedPESPacket(0x101, 0xc0, 0, 0))
		data = append(data, bytes.Repeat(pcrTestNonPCRPacket(0x1ffe, false), transportSyncPacketCount)...)
		if _, _, err := gate.push(data, base); err != nil || gate.repairReady {
			t.Fatalf("one sample granted repair eligibility: %v ready=%v", err, gate.repairReady)
		}
		for i := 1; i <= 2; i++ {
			data := joinTSPackets(testTimestampedPESPacket(0x100, 0xe0, 0, byte(i)),
				testTimestampedPESPacket(0x101, 0xc0, uint64(i)*transportPTSRate/2, byte(i)))
			out, _, err := gate.push(data, base)
			if err != nil || (i == 2 && len(out) != 0) {
				t.Fatalf("slope escaped before clock establishment: %d %v", len(out), err)
			}
		}
		if fault := gate.continuityFault(base.Add(maxPublicationAVGateHold), defaultAVContinuityPolicy()); fault == nil || fault.kind != avFaultTimelineDrift {
			t.Fatalf("unestablished drift lost private hold bound: %v", fault)
		}
	}
}

func TestCheckedInputRepairRetainsGrossAccumulatedClockFence(t *testing.T) {
	base := time.Unix(1700000000, 0)
	prefix, pts, vcc, acc := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
	checked := newCheckedTranscodeInput()
	checked.allowAudioClockRepair = true
	if _, err := checked.push(prefix, base); err != nil {
		t.Fatal(err)
	}
	var fault *avContinuityError
	for i := 0; i < 100; i++ {
		data := joinTSPackets(testTimestampedPESPacket(0x100, 0xe0, pts+uint64(i)*transportPTSRate/50, vcc), testTimestampedPESPacket(0x101, 0xc0, pts+uint64(i)*transportPTSRate*2/5, acc))
		_, err := checked.push(data, base.Add(time.Duration(i+1)*40*time.Millisecond))
		vcc, acc = (vcc+1)&15, (acc+1)&15
		if err != nil {
			if !errors.As(err, &fault) || (fault.kind != avFaultTimelineSkew && fault.kind != avFaultTimelineDrift) ||
				max(absoluteDuration(fault.skew), absoluteDuration(fault.drift)) < immediateGrossAVOffset {
				t.Fatalf("unexpected accumulated clock fence: %v", err)
			}
			break
		}
	}
	if fault == nil {
		t.Fatal("repair mode passed the 30-second gross-clock fence")
	}
}

func TestCheckedInputRepairRetainsSplitPESAndProgramIdentity(t *testing.T) {
	base := time.Unix(1700000000, 0)
	prefix, _, _, acc := healthyAVTranscodePrefix(0x1000, 0x100, 0x101, minPrefixDecisionBytes+2*tsPacketSize)
	for _, name := range []string{"split_pes", "program_change"} {
		t.Run(name, func(t *testing.T) {
			checked := newCheckedTranscodeInput()
			checked.allowAudioClockRepair = true
			if _, err := checked.push(prefix, base); err != nil {
				t.Fatal(err)
			}
			var bad []byte
			if name == "split_pes" {
				pes := slateTestPES(0xc0, transportPTSRate, 0, false)
				pes[9] = pes[9]&0x0f | 0x30
				if out, err := checked.push(testExactPayloadPacket(0x101, true, acc, pes[:8]), base); err != nil || len(out) != 0 {
					t.Fatalf("partial PES escaped: %d %v", len(out), err)
				}
				bad = testExactPayloadPacket(0x101, false, (acc+1)&15, pes[8:])
			} else {
				bad = testAVPMTPacket(0x1000, 0x100, 0x102)
			}
			if out, err := checked.push(bad, base); err == nil || len(out) != 0 {
				t.Fatalf("%s escaped: %d %v", name, len(out), err)
			}
		})
	}
}
