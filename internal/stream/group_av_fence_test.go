package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// This reconstructs the observed public packet shape, not an unavailable
// private FFmpeg Read boundary: an unbounded video PES followed by six seconds
// of complete audio, then a later video clock which makes the whole read's
// endpoints agree. It exercises the actual publication/PES/pacing components.
func TestCompleteGroupCannotBorrowLaterPrivateVideoAlignment(t *testing.T) {
	const videoPID, audioPID = 0x100, 0x101
	program := avProgramPIDs{videoPID: videoPID, pcrPID: videoPID, audioCount: 1}
	program.audioPIDs[0] = audioPID
	now := time.Unix(1700000000, 0)
	var av publicationAVGate
	av.bindSelectedProgram(program, now, true)
	var pes pesPublicationGate
	pes.configure(program, true)
	var fence completeGroupAVFence
	fence.configure(program, true, false, now)
	var pacer transportPCRPacer
	pacer.Configure(videoPID, false)
	pacer.now = func() time.Time { return now }
	pacer.wait = func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }
	var wire []byte
	var sizes []int
	publish := func(groups [][]byte) error {
		for _, group := range groups {
			if fault := fence.inspect(group, now); fault != nil {
				return fault
			}
			arrival := now
			if len(group) > publicationChunkSize {
				pacer.PrepareSharedArrivalBatch(group, publicationChunkSize)
			}
			for remaining := group; len(remaining) > 0; {
				n := min(len(remaining), publicationChunkSize)
				if err := pacer.Wait(context.Background(), remaining[:n], arrival); err != nil {
					return err
				}
				remaining = remaining[n:]
			}
			wire = append(wire, group...)
			sizes = append(sizes, len(group))
		}
		return nil
	}
	feed := func(data []byte) error {
		media, _, fault := av.push(data, now)
		if fault != nil {
			return fault
		}
		groups, err := pes.push(media)
		if err != nil {
			return err
		}
		return publish(groups)
	}
	// Establish ordinary 40 ms clock progression. The final zero-length PES
	// remains private until the incident batch's next video start closes it.
	var initial []byte
	for i := 0; i < 3; i++ {
		initial = append(initial, groupFenceVideo(time.Duration(i)*40*time.Millisecond, byte(i))...)
		initial = append(initial, groupFenceAudio(time.Duration(i)*40*time.Millisecond, byte(i), 32)...)
	}
	if err := feed(initial); err != nil {
		t.Fatal(err)
	}
	before := len(wire)
	var incident []byte
	incident = append(incident, groupFenceVideo(120*time.Millisecond, 3)...)
	for i := 0; i < 281; i++ {
		incident = append(incident, groupFenceAudio(120*time.Millisecond+time.Duration(i)*time.Second*1024/48000, byte(3*i+3), 520)...)
	}
	incident = append(incident, groupFenceVideo(6120*time.Millisecond, 4)...)
	incident = append(incident, groupFenceVideo(6160*time.Millisecond, 5)...)
	err := feed(incident)
	var avErr *avContinuityError
	if !errors.As(err, &avErr) || avErr.kind != avFaultTimelineSkew {
		t.Fatalf("reconstruction error=%v, want endpoint refusal before later PCR attempt boundary", err)
	}
	snapshot := av.timeline.snapshotAt(now)
	if !snapshot.valid || absoluteDuration(snapshot.finalSkew) > 100*time.Millisecond {
		t.Fatalf("whole-read final timeline did not appear aligned: %+v", snapshot)
	}
	var published transportAVTimeline
	published.bindSelectedProgram(program, now)
	published.feedAt(wire, now)
	tail := published.snapshotAt(now)
	t.Logf("reconstruction: whole-read skew=%s, published skew=%s, incident published=%d, groups=%v, modeled pacing=%s", snapshot.finalSkew, tail.finalSkew, len(wire)-before, sizes, now.Sub(time.Unix(1700000000, 0)))
	if tail.finalSkew > 2*time.Second {
		t.Fatalf("later private video hid unsafe complete-group endpoint: published audio leads video by %s", tail.finalSkew)
	}
}

func TestCompleteGroupAVFencePreservesEstablishedContracts(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		replay                                bool
		audioCount                            int
		initialOffset, finalAudio, finalVideo time.Duration
		wantFault                             bool
	}{
		{name: "ordinary_interleaving", audioCount: 1, finalAudio: time.Second, finalVideo: 1040 * time.Millisecond},
		{name: "subthreshold_unmatched_tail", audioCount: 1, finalAudio: 2100 * time.Millisecond, finalVideo: time.Second},
		{name: "static_audio_origin", audioCount: 1, initialOffset: 1800 * time.Millisecond, finalAudio: 2800 * time.Millisecond, finalVideo: time.Second},
		{name: "unequal_initial_coverage", audioCount: 1, initialOffset: 3 * time.Second, finalAudio: 4 * time.Second, finalVideo: time.Second},
		{name: "new_audio_lead", audioCount: 1, finalAudio: 7 * time.Second, finalVideo: time.Second, wantFault: true},
		{name: "new_video_lead", audioCount: 1, finalAudio: time.Second, finalVideo: 7 * time.Second, wantFault: true},
		{name: "finite_replay_tail_unchanged", replay: true, audioCount: 1, finalAudio: 7 * time.Second, finalVideo: time.Second},
		{name: "multi_audio_policy_unchanged", audioCount: 2, finalAudio: 7 * time.Second, finalVideo: time.Second},
		// The fence deliberately makes no internal-gap claim when both
		// exposed endpoints agree. PCR and ordinary continuity still apply.
		{name: "aligned_endpoints_with_internal_gap_out_of_scope", audioCount: 1, finalAudio: 7 * time.Second, finalVideo: 7 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			program := avProgramPIDs{videoPID: 0x100, pcrPID: 0x100, audioCount: tc.audioCount}
			program.audioPIDs[0], program.audioPIDs[1] = 0x101, 0x102
			now := time.Unix(1700000000, 0)
			var f completeGroupAVFence
			f.configure(program, true, tc.replay, now)
			initial := joinTSPackets(groupFenceVideo(0, 0), groupFenceAudio(tc.initialOffset, 0, 32), pcrTestNonPCRPacket(0x1ffe, false), pcrTestNonPCRPacket(0x1ffe, false), pcrTestNonPCRPacket(0x1ffe, false))
			if err := f.inspect(initial, now); err != nil {
				t.Fatalf("initial origin rejected: %v", err)
			}
			group := joinTSPackets(groupFenceVideo(tc.finalVideo, 1), groupFenceAudio(tc.finalAudio, 1, 32))
			before := bytes.Clone(group)
			err := f.inspect(group, now.Add(40*time.Millisecond))
			if (err != nil) != tc.wantFault {
				t.Fatalf("endpoint fault=%v wantFault=%v", err, tc.wantFault)
			}
			if !bytes.Equal(before, group) {
				t.Fatal("endpoint observer changed media bytes")
			}
		})
	}
}

// Exercise the actual default runOnce publication loop, input observer,
// reservoir, transport/PES gates, pacer, error attribution and subscriber queue.
// A controlled process stands in for FFmpeg so the offending mux order is
// deterministic; this is not a decoder or production FFmpeg behavior claim.
func TestTranscodeCompleteGroupRefusesAudioTailBeforeLaterPCRFailure(t *testing.T) {
	input, _, _, _ := healthyAVTranscodeInputServer(t)
	dir := t.TempDir()
	outputPath, signalPath := filepath.Join(dir, "output.ts"), filepath.Join(dir, "published")
	initial := joinTSPackets(testPATPacket(0x1000), testAVPMTPacket(0x1000, 0x100, 0x101))
	for i := 0; i < 3; i++ {
		initial = append(initial, groupFenceVideo(time.Duration(i)*40*time.Millisecond, byte(i))...)
		initial = append(initial, groupFenceAudio(time.Duration(i)*40*time.Millisecond, byte(i), 32)...)
	}
	for len(initial) < minPrefixDecisionBytes {
		initial = append(initial, pcrTestNonPCRPacket(0x1ffe, false)...)
	}
	incident := groupFenceVideo(120*time.Millisecond, 3)
	for i := 0; i < 281; i++ {
		incident = append(incident, groupFenceAudio(120*time.Millisecond+time.Duration(i)*time.Second*1024/48000, byte(3*i+3), 520)...)
	}
	incident = append(incident, groupFenceVideo(6120*time.Millisecond, 4)...)
	incident = append(incident, groupFenceVideo(6160*time.Millisecond, 5)...)
	if err := os.WriteFile(outputPath, append(bytes.Clone(initial), incident...), 0600); err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf(`exec python3 -c 'import os,sys,time
d=open(sys.argv[1],"rb").read(); n=int(sys.argv[2])
sys.stdout.buffer.write(d[:n]); sys.stdout.buffer.flush()
deadline=time.monotonic()+5
while not os.path.exists(sys.argv[3]):
 if time.monotonic()>deadline: sys.exit(2)
 time.sleep(.005)
sys.stdout.buffer.write(d[n:]); sys.stdout.buffer.flush()
sys.stdin.buffer.read()' '%s' '%d' '%s'`, outputPath, len(initial), signalPath)
	s := NewTranscodeStreamer("complete-group-av", input.URL, stubProfile(), stubBinary(t, script), testLogger(), nil)
	s.prefixValidator = exactProgramMapTestValidator{readyAt: minPrefixDecisionBytes}
	viewer, leave := s.Subscribe("complete-group-av-viewer")
	defer leave()
	var signalOnce sync.Once
	s.OnBytes = func(int) error {
		var err error
		signalOnce.Do(func() { err = os.WriteFile(signalPath, nil, 0600) })
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	got, upstream, err := s.runOnce(ctx)
	wire := collectTranscodeSubscriber(t, viewer)
	var visible transportAVTimeline
	visible.feed(wire)
	t.Logf("actual subscriber bytes=%d final PTS skew=%s; attempt=%v", len(wire), visible.snapshotAt(time.Now()).finalSkew, err)
	var fault *avContinuityError
	if !got || upstream || !errors.Is(err, errLocalTranscodeFailure) || !errors.As(err, &fault) || fault.kind != avFaultTimelineSkew {
		t.Fatalf("actual runOnce got=%v upstream=%v err=%v, want source-neutral complete-group skew refusal", got, upstream, err)
	}
	if shouldMarkSourceFailure(err) {
		t.Fatal("local publication endpoint refusal penalizes provider health")
	}
	if !requiresImmediateContinuityFiller(err) {
		t.Fatal("typed publication refusal waits for the ordinary slate delay")
	}
	if testStreamContainsPTS(wire, 0x101, durationTicks(120*time.Millisecond+280*time.Second*1024/48000, transportPTSRate)) {
		t.Fatal("six-second audio tail reached actual subscriber")
	}
	if !testStreamContainsPTS(wire, 0x100, 0) {
		t.Fatal("test never established the original video publication")
	}
}

func TestRealFFmpegCompleteGroupFencePreservesOriginsAndBytes(t *testing.T) {
	ffmpeg, _ := requireAudioOriginTools(t)
	for _, tc := range []struct{ name, filter string }{
		{"aligned", "anull"},
		{"constant_origin", "asetpts=PTS+1.8/TB"},
		{"late_audio", "atrim=start=1.8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Generation already requires full clean A/V decode. Exact identity
			// after grouping proves this observer retained that same full media.
			input := generateJointProgressMedia(t, ffmpeg, tc.filter)
			programs := scanTSProgramMaps(input, "")
			if len(programs) != 1 {
				t.Fatalf("programmes=%d", len(programs))
			}
			selected := programs[0]
			program := avProgramPIDs{videoPID: selected.videoPID, pcrPID: selected.pcrPID,
				audioPIDs: selected.audioPIDs, audioCount: selected.audioCount, mapIdentity: selected.mapIdentity}
			now := time.Now()
			var fence completeGroupAVFence
			fence.configure(program, true, false, now)
			var pes pesPublicationGate
			pes.configure(program, true)
			groups, err := pes.push(input)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := pes.finishCleanEOF()
			if err != nil {
				t.Fatal(err)
			}
			groups = append(groups, tail...)
			for i, group := range groups {
				if fault := fence.inspect(group, now); fault != nil {
					t.Fatalf("clean %s group%d refused: %v; %+v", tc.name, i, fault, fence.timeline.snapshotAt(now))
				}
			}
			if !bytes.Equal(bytes.Join(groups, nil), input) {
				t.Fatal("clean decoded media bytes changed")
			}
			t.Logf("fully decoded input bytes=%d, complete groups=%d retained exactly", len(input), len(groups))
		})
	}
}

func groupFenceVideo(at time.Duration, cc byte) []byte {
	packet := testTimestampedPESPacket(0x100, 0xe0, durationTicks(at, transportPTSRate), cc&15)
	packet[5] |= 0x10 // exact-payload helper leaves ample adaptation stuffing
	copy(packet[6:12], slateTestPCR(durationTicks(at, transportClockRate)))
	return packet
}

func groupFenceAudio(at time.Duration, cc byte, size int) []byte {
	pes := append(slateTestPES(0xc0, durationTicks(at, transportPTSRate), 0, false), bytes.Repeat([]byte{0x55}, size)...)
	n := len(pes) - 6
	pes[4], pes[5] = byte(n>>8), byte(n)
	var packets []byte
	for first := true; len(pes) > 0; first = false {
		n := min(len(pes), 184)
		packets = append(packets, publicationTestPacket(0x101, first, pes[:n], cc&15)...)
		cc++
		pes = pes[n:]
	}
	return packets
}
