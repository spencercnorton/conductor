package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

const (
	audioOriginTestVideo  = 0x0100
	audioOriginTestFirst  = 0x0101
	audioOriginTestSecond = 0x0102
)

func newBoundAudioOriginProbe(t *testing.T, audioPIDs ...int) *audioOriginProbe {
	t.Helper()
	program := avProgramPIDs{
		videoPID:   audioOriginTestVideo,
		pcrPID:     audioOriginTestVideo,
		audioCount: len(audioPIDs),
	}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	copy(program.audioPIDs[:], audioPIDs)
	p := &audioOriginProbe{bound: true, program: program}
	p.timeline.requireFirstAudio = true
	p.timeline.bindSelectedProgram(program, time.Unix(1_700_001_000, 0))
	return p
}

func feedAudioOriginPairs(
	t *testing.T,
	p *audioOriginProbe,
	firstOffset time.Duration,
	secondOffset *time.Duration,
	perSampleDrift time.Duration,
) {
	t.Helper()
	feedAudioOriginPairRange(t, p, firstOffset, secondOffset, perSampleDrift, 0, 9)
}

func feedAudioOriginPairRange(
	t *testing.T,
	p *audioOriginProbe,
	firstOffset time.Duration,
	secondOffset *time.Duration,
	perSampleDrift time.Duration,
	start int,
	count int,
) {
	t.Helper()
	basePTS := uint64(100) * transportPTSRate
	step := 50 * time.Millisecond
	packets := make([][]byte, 0, count*3)
	for i := start; i < start+count; i++ {
		videoPTS := basePTS + durationTicks(time.Duration(i)*step, transportPTSRate)
		firstPTS := addSignedPTSTestOffset(videoPTS,
			firstOffset+time.Duration(i)*perSampleDrift)
		packets = append(packets,
			testTimestampedPESPacket(audioOriginTestVideo, 0xe0, videoPTS, byte(i)),
			testTimestampedPESPacket(audioOriginTestFirst, 0xc0, firstPTS, byte(i)))
		if secondOffset != nil {
			packets = append(packets, testTimestampedPESPacket(
				audioOriginTestSecond, 0xc1,
				addSignedPTSTestOffset(videoPTS, *secondOffset), byte(i)))
		}
	}
	p.feedTimeline(joinTSPackets(packets...), time.Unix(1_700_001_000, 0))
}

func addSignedPTSTestOffset(pts uint64, offset time.Duration) uint64 {
	ticks := int64(durationTicks(absoluteDuration(offset), transportPTSRate))
	if offset < 0 {
		return (pts + ptsModulus - uint64(ticks)) % ptsModulus
	}
	return (pts + uint64(ticks)) % ptsModulus
}

func audioOriginFaultFixture(t *testing.T, fault string, faultPID int) []byte {
	t.Helper()
	const pmtPID = 0x1000
	packets := [][]byte{
		testPATPacket(pmtPID),
		testMultiAudioPMTPacket(pmtPID, audioOriginTestVideo,
			audioOriginTestFirst, audioOriginTestSecond),
	}
	basePTS := uint64(100) * transportPTSRate
	for i := 0; i < 8; i++ {
		videoPTS := basePTS + durationTicks(time.Duration(i)*50*time.Millisecond, transportPTSRate)
		group := [][]byte{
			testTimestampedPESPacket(audioOriginTestVideo, 0xe0, videoPTS, byte(i)),
			testTimestampedPESPacket(audioOriginTestFirst, 0xc0,
				addSignedPTSTestOffset(videoPTS, -3*time.Second), byte(i)),
			testTimestampedPESPacket(audioOriginTestSecond, 0xc1, videoPTS, byte(i)),
		}
		if i == 3 {
			var target []byte
			switch faultPID {
			case audioOriginTestVideo:
				target = group[0]
			case audioOriginTestFirst:
				target = group[1]
			case audioOriginTestSecond:
				target = group[2]
			default:
				t.Fatalf("unsupported fault PID %#x", faultPID)
			}
			switch fault {
			case "tei":
				target[1] |= 0x80
			case "continuity":
				target[3] = target[3]&0xf0 | 0x0b
			case "malformed pes":
				payload, ok := tsPayload(target)
				if !ok || len(payload) < 14 {
					t.Fatalf("fault target has no complete PES timestamp: %x", target[:16])
				}
				payload[11] &^= 0x01
			case "discontinuity":
				packets = append(packets, testAdaptationOnlyDiscontinuity(faultPID, byte(i)))
			default:
				t.Fatalf("unsupported fault %q", fault)
			}
		}
		packets = append(packets, group...)
	}
	return joinTSPackets(packets...)
}

func TestAudioOriginProbeCorrectsOnlyStableBoundedFirstAudio(t *testing.T) {
	t.Run("stable negative three seconds", func(t *testing.T) {
		aligned := time.Duration(0)
		probe := newBoundAudioOriginProbe(t, audioOriginTestFirst, audioOriginTestSecond)
		feedAudioOriginPairs(t, probe, -3*time.Second, &aligned, 0)
		ready, correction, err := probe.decision()
		if err != nil || !ready || correction != 3*time.Second {
			t.Fatalf("decision=(ready=%v correction=%s err=%v), want +3s", ready, correction, err)
		}
		raw := probe.timeline.snapshotAt(time.Unix(1_700_001_000, 0))
		if raw.initialSkew != -3*time.Second || raw.finalSkew != -3*time.Second {
			t.Fatalf("raw diagnostics were rewritten: %+v", raw)
		}
		probe.timeline.setFirstAudioOriginCorrection(correction)
		if fault := probe.timeline.continuityFault(
			time.Unix(1_700_001_001, 0), defaultAVContinuityPolicy()); fault != nil {
			t.Fatalf("proved origin baseline did not satisfy the input gate: %v", fault)
		}
	})

	t.Run("aligned second language is never selected", func(t *testing.T) {
		secondaryOffset := -3 * time.Second
		probe := newBoundAudioOriginProbe(t, audioOriginTestFirst, audioOriginTestSecond)
		feedAudioOriginPairs(t, probe, 0, &secondaryOffset, 0)
		ready, correction, err := probe.decision()
		if err != nil || !ready || correction != 0 {
			t.Fatalf("secondary audio influenced 0:a:0 decision: ready=%v correction=%s err=%v",
				ready, correction, err)
		}
	})

	for _, offset := range []time.Duration{-1999 * time.Millisecond, 1999 * time.Millisecond} {
		t.Run("legitimate sub-threshold offset "+offset.String(), func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			feedAudioOriginPairs(t, probe, offset, nil, 0)
			ready, correction, err := probe.decision()
			if err != nil || !ready || correction != 0 {
				t.Fatalf("legitimate offset changed: ready=%v correction=%s err=%v",
					ready, correction, err)
			}
		})
	}

	for _, offset := range []time.Duration{-2 * time.Second, 2 * time.Second} {
		t.Run("exact safety boundary "+offset.String(), func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			feedAudioOriginPairs(t, probe, offset, nil, 0)
			ready, correction, err := probe.decision()
			if err != nil || !ready || correction != -offset {
				t.Fatalf("exact-boundary decision=(ready=%v correction=%s err=%v), want %s",
					ready, correction, err, -offset)
			}
		})
	}

	for _, offset := range []time.Duration{-5 * time.Second, 5 * time.Second} {
		t.Run("exact correction ceiling "+offset.String(), func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			feedAudioOriginPairs(t, probe, offset, nil, 0)
			ready, correction, err := probe.decision()
			if err != nil || !ready || correction != -offset {
				t.Fatalf("exact-ceiling decision=(ready=%v correction=%s err=%v), want %s",
					ready, correction, err, -offset)
			}
		})
	}
}

func TestAudioOriginPairRingWaitsForStableTrailingWindow(t *testing.T) {
	var ring audioOriginPairRing
	for _, skew := range []time.Duration{
		-4900 * time.Millisecond,
		-4800 * time.Millisecond,
		-4670 * time.Millisecond,
		-4650 * time.Millisecond,
		-4630 * time.Millisecond,
		-4660 * time.Millisecond,
		-4640 * time.Millisecond,
	} {
		ring.record(skew)
	}
	if median, stable := ring.stableMedian(-4650 * time.Millisecond); stable || median != 0 {
		t.Fatalf("seven-sample window=(median=%s stable=%v), want more evidence", median, stable)
	}
	ring.record(-4650 * time.Millisecond)
	median, stable := ring.stableMedian(-4650 * time.Millisecond)
	if !stable || absoluteDuration(median+4650*time.Millisecond) > time.Millisecond {
		t.Fatalf("stable trailing window=(median=%s stable=%v), want -4.650s", median, stable)
	}

	var recovering audioOriginPairRing
	for _, skew := range []time.Duration{
		-3 * time.Second,
		-3 * time.Second,
		-2800 * time.Millisecond,
		-3 * time.Second,
		-3 * time.Second,
		-3 * time.Second,
		-3 * time.Second,
		-3 * time.Second,
	} {
		recovering.record(skew)
	}
	if median, stable := recovering.stableMedian(-3 * time.Second); stable || median != 0 {
		t.Fatalf("transient unstable window=(median=%s stable=%v), want more evidence", median, stable)
	}
	for i := 0; i < audioOriginPairWindowSamples; i++ {
		recovering.record(-3 * time.Second)
	}
	if median, stable := recovering.stableMedian(-3 * time.Second); !stable || median != -3*time.Second {
		t.Fatalf("recovered trailing window=(median=%s stable=%v), want -3s", median, stable)
	}
}

func TestAudioOriginProbeAllowsSparseHighBitrateMuxWithinBound(t *testing.T) {
	probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
	basePTS := uint64(100) * transportPTSRate
	padding := bytes.Repeat(pcrTestNonPCRPacket(0x1fff, false), (136*1024)/tsPacketSize)
	var ready bool
	var correction time.Duration
	var err error
	for i := 0; i < int(minAudioOriginSamples); i++ {
		videoPTS := basePTS + durationTicks(time.Duration(i)*50*time.Millisecond, transportPTSRate)
		block := joinTSPackets(
			testTimestampedPESPacket(audioOriginTestVideo, 0xe0, videoPTS, byte(i)),
			testTimestampedPESPacket(audioOriginTestFirst, 0xc0,
				addSignedPTSTestOffset(videoPTS, -4650*time.Millisecond), byte(i)),
		)
		block = append(block, padding...)
		ready, correction, err = probe.push(block, time.Unix(1_700_001_000, 0))
		if err != nil {
			t.Fatalf("sparse high-bitrate proof failed after %d bytes: %v", len(probe.buffer), err)
		}
	}
	if len(probe.buffer) <= 1024*1024 || len(probe.buffer) > maxAudioOriginProbeBytes {
		t.Fatalf("fixture used %d bytes, want (1MiB, %d]", len(probe.buffer), maxAudioOriginProbeBytes)
	}
	if !ready || absoluteDuration(correction-4650*time.Millisecond) > time.Millisecond {
		t.Fatalf("sparse high-bitrate decision=(ready=%v correction=%s err=%v bytes=%d)",
			ready, correction, err, len(probe.buffer))
	}
}

func sparseAudioOriginFixture(t *testing.T, initial, final time.Duration) []byte {
	t.Helper()
	return sparseAudioOriginFixtureWithVideoStep(t, initial, final, 20*time.Millisecond)
}

func sparseAudioOriginFixtureWithVideoStep(
	t *testing.T,
	initial time.Duration,
	final time.Duration,
	videoStep time.Duration,
) []byte {
	t.Helper()
	const pmtPID = 0x1000
	basePTS := uint64(100) * transportPTSRate
	videoSpan := 51 * videoStep
	audioEnd := videoSpan + final
	audioAtVideo := []int{0, 14, 27, 40}
	packets := [][]byte{
		testPATPacket(pmtPID),
		testMultiAudioPMTPacket(pmtPID, audioOriginTestVideo,
			audioOriginTestFirst, audioOriginTestSecond),
	}
	audioIndex := 0
	for videoIndex := 0; videoIndex < 52; videoIndex++ {
		videoPTS := basePTS + durationTicks(time.Duration(videoIndex)*videoStep, transportPTSRate)
		packets = append(packets,
			testTimestampedPESPacket(audioOriginTestVideo, 0xe0, videoPTS, byte(videoIndex)))
		if audioIndex < len(audioAtVideo) && videoIndex == audioAtVideo[audioIndex] {
			audioTime := initial + (audioEnd-initial)*time.Duration(audioIndex)/
				time.Duration(len(audioAtVideo)-1)
			audioPTS := addSignedPTSTestOffset(basePTS, audioTime)
			packets = append(packets,
				testTimestampedPESPacket(audioOriginTestFirst, 0xc0, audioPTS, byte(audioIndex)),
				testTimestampedPESPacket(audioOriginTestSecond, 0xc1, videoPTS, byte(audioIndex)))
			audioIndex++
		}
	}
	if audioIndex != len(audioAtVideo) {
		t.Fatalf("inserted %d/%d sparse audio clocks", audioIndex, len(audioAtVideo))
	}
	return joinTSPackets(packets...)
}

func sparseNormalAudioOriginFixture(t *testing.T) []byte {
	t.Helper()
	return sparseAudioOriginFixture(t, -22*time.Millisecond, -249*time.Millisecond)
}

func TestAudioOriginProbePreservesSparseNormalOriginWithFourAudioSamples(t *testing.T) {
	probe := &audioOriginProbe{}
	input := sparseNormalAudioOriginFixture(t)
	ready, correction, err := probe.push(input, time.Unix(1_700_001_000, 0))
	raw := probe.timeline.snapshotAt(time.Unix(1_700_001_000, 0))
	if raw.videoSamples != 52 || raw.audioSamples != minPreservedAudioOriginSamples ||
		absoluteDuration(raw.initialSkew+22*time.Millisecond) > time.Millisecond ||
		absoluteDuration(raw.finalSkew+249*time.Millisecond) > time.Millisecond {
		t.Fatalf("sparse normal evidence=%+v", raw)
	}
	if probe.pairs.count >= int(minAudioOriginSamples) {
		t.Fatalf("fixture unexpectedly reached correction-grade evidence: %+v", probe.pairs)
	}
	if err != nil || !ready || correction != 0 {
		t.Fatalf("sparse normal decision=(ready=%v correction=%s err=%v), want unchanged replay",
			ready, correction, err)
	}
	if len(probe.buffer) != len(input) || len(probe.buffer) >= maxAudioOriginProbeBytes {
		t.Fatalf("sparse normal proof bytes=%d/%d (max=%d)",
			len(probe.buffer), len(input), maxAudioOriginProbeBytes)
	}
}

func TestSparseNormalOriginReplaysThroughZeroCorrectionInputGate(t *testing.T) {
	input := sparseNormalAudioOriginFixture(t)
	checked := newCheckedTranscodeInput()
	output, err := checked.push(input, time.Unix(1_700_001_000, 0))
	if err != nil || len(output) == 0 {
		t.Fatalf("checked sparse normal input=(bytes=%d err=%v pending=%d bound=%v snapshot=%+v)",
			len(output), err, len(checked.pending), checked.programBound,
			checked.safety.timeline.snapshotAt(time.Unix(1_700_001_000, 0)))
	}
	if !bytes.Equal(output, input) {
		t.Fatalf("checked sparse normal replay changed bytes: output=%d input=%d",
			len(output), len(input))
	}
}

func TestAudioOriginPreservedFastPathBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		initial   time.Duration
		final     time.Duration
		videoStep time.Duration
		ready     bool
	}{
		{name: "drift just under downstream bound", initial: -500 * time.Millisecond, final: -1499 * time.Millisecond, videoStep: 28 * time.Millisecond, ready: true},
		{name: "drift exact downstream bound", initial: -500 * time.Millisecond, final: -1500 * time.Millisecond, videoStep: 28 * time.Millisecond},
		{name: "endpoint exact correction boundary", initial: -1500 * time.Millisecond, final: -2 * time.Second, videoStep: 28 * time.Millisecond},
		{name: "pathological endpoint crossing", initial: 1900 * time.Millisecond, final: -1900 * time.Millisecond, videoStep: 100 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := &audioOriginProbe{}
			ready, correction, err := probe.push(
				sparseAudioOriginFixtureWithVideoStep(t, tt.initial, tt.final, tt.videoStep),
				time.Unix(1_700_001_000, 0),
			)
			if ready != tt.ready || correction != 0 {
				t.Fatalf("decision=(ready=%v correction=%s err=%v), want ready=%v correction=0",
					ready, correction, err, tt.ready)
			}
			if tt.ready && err != nil {
				t.Fatalf("safe preserved boundary rejected: %v", err)
			}
		})
	}
}

func TestAudioOriginCorrectionRangeStillRequiresEightSamples(t *testing.T) {
	for _, offset := range []time.Duration{
		-3 * time.Second,
		-2 * time.Second,
		2 * time.Second,
		3 * time.Second,
	} {
		t.Run(offset.String(), func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			feedAudioOriginPairRange(t, probe, offset, nil, 0, 0, 5)
			ready, correction, err := probe.decision()
			if err != nil || ready || correction != 0 {
				t.Fatalf("four-sample decision=(ready=%v correction=%s err=%v), want more proof",
					ready, correction, err)
			}
			feedAudioOriginPairRange(t, probe, offset, nil, 0, 5, 5)
			ready, correction, err = probe.decision()
			if err != nil || !ready || correction != -offset {
				t.Fatalf("eight-sample decision=(ready=%v correction=%s err=%v), want %s",
					ready, correction, err, -offset)
			}
		})
	}
}

func TestAudioOriginPreservedFastPathHonorsCommonSafetyGuards(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*audioOriginProbe)
		wantErr bool
	}{
		{
			name: "transport corruption",
			mutate: func(probe *audioOriginProbe) {
				probe.timeline.transportCorruptions++
			},
			wantErr: true,
		},
		{
			name: "declared discontinuity",
			mutate: func(probe *audioOriginProbe) {
				probe.timeline.sharedBoundaries++
			},
			wantErr: true,
		},
		{
			name: "selected video clock gap",
			mutate: func(probe *audioOriginProbe) {
				probe.timeline.video.maxStepTicks = int64(durationTicks(501*time.Millisecond, transportPTSRate))
			},
			wantErr: true,
		},
		{
			name: "selected audio clock fold",
			mutate: func(probe *audioOriginProbe) {
				probe.timeline.audio[0].minStepTicks = -1
			},
			wantErr: true,
		},
		{
			name: "incomplete selected audio PES",
			mutate: func(probe *audioOriginProbe) {
				probe.timeline.audioPES[0].active = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			probe.feedTimeline(sparseNormalAudioOriginFixture(t), time.Unix(1_700_001_000, 0))
			probe.timeline.mu.Lock()
			tt.mutate(probe)
			probe.timeline.mu.Unlock()
			ready, correction, err := probe.decision()
			if ready || correction != 0 || (tt.wantErr != errors.Is(err, errUncertainTranscodeInput)) {
				t.Fatalf("guarded decision=(ready=%v correction=%s err=%v), want private err=%v",
					ready, correction, err, tt.wantErr)
			}
		})
	}
}

func TestAudioOriginProbeUsesStableTransportEdgePairsWhenRawExtremaDiverge(t *testing.T) {
	probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
	basePTS := uint64(100) * transportPTSRate
	videoEdges := []time.Duration{
		267389 * time.Microsecond,
		466778 * time.Microsecond,
		668166 * time.Microsecond,
		867555 * time.Microsecond,
		1134621 * time.Microsecond,
		1334331 * time.Microsecond,
		1533720 * time.Microsecond,
		1733109 * time.Microsecond,
		1932498 * time.Microsecond,
	}
	const rawInitial = -3020711 * time.Microsecond
	const audioStep = 213333 * time.Microsecond
	packets := [][]byte{
		testTimestampedPESPacket(audioOriginTestVideo, 0xe0, basePTS, 0),
	}
	for i, videoEdge := range videoEdges {
		audioPTS := addSignedPTSTestOffset(
			basePTS+durationTicks(time.Duration(i)*audioStep, transportPTSRate), rawInitial)
		packets = append(packets,
			testTimestampedPESPacket(audioOriginTestVideo, 0xe0,
				basePTS+durationTicks(videoEdge, transportPTSRate), byte(i+1)),
			testTimestampedPESPacket(audioOriginTestFirst, 0xc0, audioPTS, byte(i)),
		)
	}
	// The first eight edges mirror a retained Fox Business capture; the final
	// cadence point exposes the eighth complete audio pairing in this synthetic
	// single-packet fixture. Sparse interleave shifts the stable pair median by
	// about 247 ms from the raw initial origin and also makes raw final extrema
	// appear to drift by more than 100 ms.
	probe.feedTimeline(joinTSPackets(packets...), time.Unix(1_700_001_000, 0))

	raw := probe.timeline.snapshotAt(time.Unix(1_700_001_000, 0))
	if absoluteDuration(raw.initialSkew-rawInitial) > time.Millisecond {
		t.Fatalf("raw initial=%s, want %s +/-1ms", raw.initialSkew, rawInitial)
	}
	if absoluteDuration(raw.finalSkew-raw.initialSkew) <= maxAudioOriginDrift {
		t.Fatalf("capture-shaped raw extrema did not diverge: %+v", raw)
	}
	median, stable := probe.pairs.stableMedian(raw.initialSkew)
	wantMedian := -3267450 * time.Microsecond
	if !stable || absoluteDuration(median-wantMedian) > time.Millisecond ||
		absoluteDuration(median-raw.initialSkew) <= maxAudioOriginDrift {
		t.Fatalf("transport-edge pairs=(median=%s stable=%v ring=%+v), raw=%+v",
			median, stable, probe.pairs, raw)
	}
	ready, correction, err := probe.decision()
	if err != nil || !ready || correction != -raw.initialSkew {
		t.Fatalf("decision=(ready=%v correction=%s err=%v), want %s from raw initial",
			ready, correction, err, -raw.initialSkew)
	}
}

func TestAudioOriginProbeIgnoresOnlyUnmappedSecondaryTransportFaults(t *testing.T) {
	faults := []string{"tei", "continuity", "malformed pes", "discontinuity"}
	targets := []struct {
		name       string
		pid        int
		wantReject bool
	}{
		{name: "secondary a1", pid: audioOriginTestSecond, wantReject: false},
		{name: "selected video", pid: audioOriginTestVideo, wantReject: true},
		{name: "primary a0", pid: audioOriginTestFirst, wantReject: true},
	}
	for _, target := range targets {
		for _, fault := range faults {
			t.Run(target.name+" "+fault, func(t *testing.T) {
				probe := &audioOriginProbe{}
				ready, correction, err := probe.push(
					audioOriginFaultFixture(t, fault, target.pid),
					time.Unix(1_700_001_050, 0),
				)
				if target.wantReject {
					if ready || correction != 0 || !errors.Is(err, errUncertainTranscodeInput) {
						t.Fatalf("selected fault decision=(ready=%v correction=%s err=%v), want fail closed",
							ready, correction, err)
					}
					return
				}
				if err != nil || !ready || correction != 3*time.Second {
					t.Fatalf("unmapped fault decision=(ready=%v correction=%s err=%v), want +3s",
						ready, correction, err)
				}
				probe.timeline.mu.Lock()
				corruptions := probe.timeline.transportCorruptions
				boundaries := probe.timeline.sharedBoundaries
				trackCount := probe.timeline.audioTrackCount
				probe.timeline.mu.Unlock()
				if corruptions != 0 || boundaries != 0 || trackCount != 1 {
					t.Fatalf("secondary fault contaminated selected proof: corruptions=%d boundaries=%d tracks=%d",
						corruptions, boundaries, trackCount)
				}
			})
		}
	}
}

func TestAudioOriginProbeBypassesOnlyClearlyNonTransportInput(t *testing.T) {
	t.Run("configured non-TS is replayed without correction", func(t *testing.T) {
		input := bytes.Repeat([]byte{0xbc}, tsPacketSize*(transportSyncPacketCount+1)+1)
		probe := &audioOriginProbe{}
		ready, correction, err := probe.push(input, time.Unix(1_700_001_075, 0))
		if err != nil || !ready || correction != 0 || !bytes.Equal(probe.buffer, input) {
			t.Fatalf("non-TS proof=(ready=%v correction=%s err=%v bytes=%d/%d equal=%v)",
				ready, correction, err, len(probe.buffer), len(input), bytes.Equal(probe.buffer, input))
		}
	})

	t.Run("ambiguous TS sync remains fail closed", func(t *testing.T) {
		packet := pcrTestNonPCRPacket(0x1fff, false)
		probe := &audioOriginProbe{}
		ready, correction, err := probe.push(
			bytes.Repeat(packet, transportSyncPacketCount+2),
			time.Unix(1_700_001_075, 0),
		)
		if err != nil || ready || correction != 0 {
			t.Fatalf("ambiguous prefix=(ready=%v correction=%s err=%v), want more fail-closed evidence",
				ready, correction, err)
		}
		ready, correction, err = probe.push(
			bytes.Repeat(packet, maxAudioOriginProbeBytes/len(packet)),
			time.Unix(1_700_001_075, 0),
		)
		if ready || correction != 0 || !errors.Is(err, errUncertainTranscodeInput) {
			t.Fatalf("ambiguous bound=(ready=%v correction=%s err=%v), want uncertain rejection",
				ready, correction, err)
		}
	})

	t.Run("malformed selected TS remains fail closed", func(t *testing.T) {
		probe := &audioOriginProbe{}
		ready, correction, err := probe.push(
			audioOriginFaultFixture(t, "malformed pes", audioOriginTestFirst),
			time.Unix(1_700_001_075, 0),
		)
		if ready || correction != 0 || !errors.Is(err, errUncertainTranscodeInput) {
			t.Fatalf("malformed TS=(ready=%v correction=%s err=%v), want uncertain rejection",
				ready, correction, err)
		}
	})
}

func TestAudioOriginProbeFailsClosedOnUnfitEvidence(t *testing.T) {
	assertUncertain := func(t *testing.T, probe *audioOriginProbe) {
		t.Helper()
		ready, correction, err := probe.decision()
		if ready || correction != 0 || !errors.Is(err, errUncertainTranscodeInput) {
			t.Fatalf("decision=(ready=%v correction=%s err=%v), want fail-closed uncertain input",
				ready, correction, err)
		}
	}

	for _, offset := range []time.Duration{-5001 * time.Millisecond, 5001 * time.Millisecond} {
		t.Run("offset exceeds bound "+offset.String(), func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			feedAudioOriginPairs(t, probe, offset, nil, 0)
			assertUncertain(t, probe)
		})
	}

	t.Run("clock gap", func(t *testing.T) {
		probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
		feedAudioOriginPairs(t, probe, -3*time.Second, nil, 0)
		probe.timeline.mu.Lock()
		probe.timeline.audio[0].maxStepTicks = int64(durationTicks(600*time.Millisecond, transportPTSRate))
		probe.timeline.mu.Unlock()
		assertUncertain(t, probe)
	})

	t.Run("clock fold", func(t *testing.T) {
		probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
		feedAudioOriginPairs(t, probe, -3*time.Second, nil, 0)
		probe.timeline.mu.Lock()
		probe.timeline.audio[0].minStepTicks = -1
		probe.timeline.mu.Unlock()
		assertUncertain(t, probe)
	})

	t.Run("transport corruption", func(t *testing.T) {
		probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
		feedAudioOriginPairs(t, probe, -3*time.Second, nil, 0)
		probe.timeline.mu.Lock()
		probe.timeline.transportCorruptions++
		probe.timeline.mu.Unlock()
		assertUncertain(t, probe)
	})

	t.Run("discontinuity", func(t *testing.T) {
		probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
		feedAudioOriginPairs(t, probe, -3*time.Second, nil, 0)
		probe.timeline.mu.Lock()
		probe.timeline.sharedBoundaries++
		probe.timeline.mu.Unlock()
		assertUncertain(t, probe)
	})

	t.Run("missing clocks at byte bound", func(t *testing.T) {
		probe := &audioOriginProbe{}
		ready, correction, err := probe.push(bytes.Repeat([]byte{0x47}, maxAudioOriginProbeBytes), time.Now())
		if ready || correction != 0 || !errors.Is(err, errUncertainTranscodeInput) {
			t.Fatalf("missing-clock decision=(ready=%v correction=%s err=%v)", ready, correction, err)
		}
	})
}

func TestAudioOriginProbeUnstableDriftEndsNeutrallyAtByteBound(t *testing.T) {
	for _, drift := range []time.Duration{
		-15 * time.Millisecond,
		15 * time.Millisecond,
		30 * time.Millisecond,
	} {
		t.Run(drift.String()+" per sample", func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			feedAudioOriginPairs(t, probe, -3*time.Second, nil, drift)
			ready, correction, err := probe.decision()
			if err != nil || ready || correction != 0 {
				t.Fatalf("unstable window=(ready=%v correction=%s err=%v), want private continuation",
					ready, correction, err)
			}
			remaining := maxAudioOriginProbeBytes - len(probe.buffer) + tsPacketSize
			padding := bytes.Repeat(pcrTestNonPCRPacket(0x1fff, false),
				(remaining+tsPacketSize-1)/tsPacketSize)
			ready, correction, err = probe.push(padding, time.Unix(1_700_001_001, 0))
			if ready || correction != 0 || !errors.Is(err, errUncertainTranscodeInput) ||
				shouldMarkSourceFailure(err) {
				t.Fatalf("bounded unstable proof=(ready=%v correction=%s err=%v source_failure=%v)",
					ready, correction, err, shouldMarkSourceFailure(err))
			}
		})
	}
}

func TestAudioOriginProbeBudgetPreservesFFmpegStartupTime(t *testing.T) {
	tests := []struct {
		name    string
		total   time.Duration
		elapsed time.Duration
		want    time.Duration
	}{
		{name: "probe wall cap", total: 4 * time.Second, want: 1500 * time.Millisecond},
		{name: "remaining partial budget", total: 4 * time.Second, elapsed: 1750 * time.Millisecond, want: 250 * time.Millisecond},
		{name: "reserve exhausted", total: 4 * time.Second, elapsed: 2 * time.Second, want: 0},
		{name: "past deadline", total: 4 * time.Second, elapsed: 5 * time.Second, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := audioOriginProbeBudget(tt.total, tt.elapsed); got != tt.want {
				t.Fatalf("audioOriginProbeBudget(%s, %s)=%s, want %s",
					tt.total, tt.elapsed, got, tt.want)
			}
		})
	}
}

func TestCheckedInputUsesProvedOriginWithoutWeakeningOutputBaseline(t *testing.T) {
	program := avProgramPIDs{
		videoPID: audioOriginTestVideo, pcrPID: audioOriginTestVideo, audioCount: 1,
	}
	for i := range program.audioPIDs {
		program.audioPIDs[i] = -1
	}
	program.audioPIDs[0] = audioOriginTestFirst
	base := time.Unix(1_700_001_100, 0)
	media := make([][]byte, 0, 16)
	for i := 0; i < 7; i++ {
		videoPTS := uint64(100)*transportPTSRate +
			durationTicks(time.Duration(i)*50*time.Millisecond, transportPTSRate)
		media = append(media,
			testTimestampedPESPacket(audioOriginTestVideo, 0xe0, videoPTS, byte(i)),
			testTimestampedPESPacket(audioOriginTestFirst, 0xc0,
				addSignedPTSTestOffset(videoPTS, -3*time.Second), byte(i)))
	}
	data := joinTSPackets(media...)

	var input publicationAVGate
	input.timeline.setFirstAudioOriginCorrection(3 * time.Second)
	input.bindSelectedProgram(program, base, true)
	if out, _, fault := input.push(data, base); fault != nil || len(out) != len(data) {
		t.Fatalf("corrected raw-input gate=(bytes=%d fault=%v), want exact release", len(out), fault)
	}

	var output publicationAVGate
	output.bindSelectedProgram(program, base, true)
	if out, _, fault := output.push(data, base); len(out) != 0 || fault != nil {
		t.Fatalf("zero-baseline output gate=(bytes=%d fault=%v), want private skew candidate", len(out), fault)
	}
	if fault := output.continuityFault(
		base.Add(maxPublicationAVGateHold+time.Millisecond),
		defaultAVContinuityPolicy()); fault == nil || fault.kind != avFaultTimelineSkew {
		t.Fatalf("zero-baseline output gate fault=%v, want authoritative skew rejection", fault)
	}
}

func TestCheckedTranscodeInputAcceptsExactProvedAudioOriginBoundary(t *testing.T) {
	const pmtPID = 0x1000
	base := time.Unix(1_700_001_150, 0)
	for _, offset := range []time.Duration{-2 * time.Second, 2 * time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			probe := newBoundAudioOriginProbe(t, audioOriginTestFirst)
			feedAudioOriginPairs(t, probe, offset, nil, 0)
			ready, correction, err := probe.decision()
			if err != nil || !ready || correction != -offset {
				t.Fatalf("origin proof=(ready=%v correction=%s err=%v), want %s",
					ready, correction, err, -offset)
			}

			packets := [][]byte{
				testPATPacket(pmtPID),
				testAVPMTPacket(pmtPID, audioOriginTestVideo, audioOriginTestFirst),
			}
			for i := 0; i < 7; i++ {
				videoPTS := uint64(100)*transportPTSRate +
					durationTicks(time.Duration(i)*50*time.Millisecond, transportPTSRate)
				packets = append(packets,
					testTimestampedPESPacket(audioOriginTestVideo, 0xe0, videoPTS, byte(i)),
					testTimestampedPESPacket(audioOriginTestFirst, 0xc0,
						addSignedPTSTestOffset(videoPTS, offset), byte(i)),
				)
			}
			input := joinTSPackets(packets...)
			checked := newCheckedTranscodeInput()
			checked.audioOriginCorrection = correction
			output, pushErr := checked.push(input, base)
			if pushErr != nil || len(output) != len(input) || !bytes.Equal(output, input) {
				t.Fatalf("proved exact-boundary input=(bytes=%d/%d err=%v equal=%v), want exact release",
					len(output), len(input), pushErr, bytes.Equal(output, input))
			}
		})
	}
}

func TestAudioOriginRefusalCarriesSlugIntoDiagnosticReason(t *testing.T) {
	err := audioOriginRefusal("byte_cap", "audio-origin evidence exceeded 1572864 bytes")
	if !errors.Is(err, errUncertainTranscodeInput) {
		t.Fatalf("refusal is not errUncertainTranscodeInput: %v", err)
	}
	if got := diagnosticReason(err); got != "transcode_input_uncertain:byte_cap" {
		t.Fatalf("diagnosticReason=%q, want the slug", got)
	}
	if got := diagnosticReason(fmt.Errorf("wrapped: %w", err)); got != "transcode_input_uncertain:byte_cap" {
		t.Fatalf("wrapped diagnosticReason=%q, want the slug", got)
	}
	if got := diagnosticReason(errUncertainTranscodeInput); got != "transcode_input_uncertain" {
		t.Fatalf("bare diagnosticReason=%q", got)
	}
	// The attempt error keeps the slug as its reason and the refusal as cause.
	attempt := &upstreamAttempt{ctx: context.Background()}
	wrapped := attempt.wrap("audio_origin", err)
	var attemptErr *sourceAttemptError
	if !errors.As(wrapped, &attemptErr) || attemptErr.reason != "transcode_input_uncertain:byte_cap" {
		t.Fatalf("wrapped reason=%v", wrapped)
	}
}
