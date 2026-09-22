package stream

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

// The timestamp-only fixture PES each occupy one packet. Tests that leave the
// entity open yet require the final sample to publish must declare that end;
// zero-length PES would correctly wait for the next same-PID PUSI or clean EOF.
func declareSyntheticTimestampPESLengths(chunks ...[]byte) {
	input := bytes.Join(chunks, nil)
	for off := 0; off+tsPacketSize <= len(input); off += tsPacketSize {
		packet := input[off : off+tsPacketSize]
		pid := int(packet[1]&31)<<8 | int(packet[2])
		if (pid == 0x100 || pid == 0x101) && packet[1]&0x40 != 0 {
			if payload, ok := tsPayload(packet); ok && len(payload) >= 6 && bytes.Equal(payload[:3], []byte{0, 0, 1}) {
				n := len(payload) - 6
				payload[4], payload[5] = byte(n>>8), byte(n)
			}
		}
	}
	for _, chunk := range chunks {
		copy(chunk, input[:len(chunk)])
		input = input[len(chunk):]
	}
}

func publicationTestPacket(pid int, start bool, payload []byte, cc byte) []byte {
	p := bytes.Repeat([]byte{0xff}, tsPacketSize)
	p[0] = 0x47
	p[1] = byte(pid>>8) & 31
	if start {
		p[1] |= 64
	}
	p[2] = byte(pid)
	if len(payload) == 184 {
		p[3] = 0x10 | cc
		copy(p[4:], payload)
	} else {
		p[3] = 0x30 | cc
		p[4] = byte(183 - len(payload))
		if p[4] > 0 {
			p[5] = 0
		}
		copy(p[5+int(p[4]):], payload)
	}
	return p
}
func publicationTestHeader(total int) []byte {
	p := bytes.Repeat([]byte{0x55}, total)
	copy(p, []byte{0, 0, 1, 0xc0, 0, 0})
	if total > 6 {
		n := total - 6
		p[4] = byte(n >> 8)
		p[5] = byte(n)
	}
	return p
}
func publicationTestGate() *pesPublicationGate {
	var g pesPublicationGate
	g.configure(avProgramPIDs{videoPID: 256, audioPIDs: [maxAVTimelineAudioPIDs]int{257}, audioCount: 1}, true)
	return &g
}

func TestPESPublicationKeepsOverlappingUnitsTogether(t *testing.T) {
	g := publicationTestGate()
	v := []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}
	a := publicationTestHeader(300)
	packets := [][]byte{publicationTestPacket(256, true, v, 0), publicationTestPacket(257, true, a[:184], 0), publicationTestPacket(256, true, v, 1), publicationTestPacket(257, false, a[184:], 1), publicationTestPacket(256, true, v, 2)}
	var wire []byte
	for i, p := range packets {
		groups, err := g.push(p)
		if err != nil {
			t.Fatal(err)
		}
		if i < 4 && len(groups) > 0 {
			t.Fatalf("published before overlapping A/V interval complete at packet%d", i)
		}
		for _, b := range groups {
			wire = append(wire, b...)
		}
	}
	want := bytes.Join(packets[:4], nil)
	if !bytes.Equal(wire, want) {
		t.Fatalf("complete overlap group bytes=%d want%d", len(wire), len(want))
	}
}

func TestPESPublicationPreservesEveryHealthyByteWithSplitHeaders(t *testing.T) {
	v := []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}
	a := publicationTestHeader(586)
	input := bytes.Join([][]byte{publicationTestPacket(256, true, v, 0), publicationTestPacket(257, true, a[:2], 0), publicationTestPacket(257, false, a[2:186], 1), publicationTestPacket(257, false, a[186:370], 2), publicationTestPacket(257, false, a[370:554], 3), publicationTestPacket(257, false, a[554:], 4), publicationTestPacket(256, true, v, 1)}, nil)
	for _, size := range []int{1, 67, 188, 65524} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			g := publicationTestGate()
			var wire []byte
			for off := 0; off < len(input); {
				end := min(off+size, len(input))
				groups, err := g.push(input[off:end])
				if err != nil {
					t.Fatal(err)
				}
				for _, b := range groups {
					wire = append(wire, b...)
				}
				off = end
			}
			tail, err := g.finishCleanEOF()
			if err != nil {
				t.Fatal(err)
			}
			for _, b := range tail {
				wire = append(wire, b...)
			}
			if !bytes.Equal(wire, input) {
				t.Fatalf("byte conservation: got%d want%d", len(wire), len(input))
			}
		})
	}
}

func TestPESPublicationRejectsIncompleteDeclaredPayloadWithoutPublishing(t *testing.T) {
	g := publicationTestGate()
	v := []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}
	a := publicationTestHeader(586)
	input := bytes.Join([][]byte{publicationTestPacket(256, true, v, 0), publicationTestPacket(257, true, a[:184], 0), publicationTestPacket(257, false, a[184:366], 1), publicationTestPacket(257, true, a[:184], 2)}, nil)
	groups, err := g.push(input)
	if !errors.Is(err, errTransportAttemptBoundary) || len(groups) > 0 {
		t.Fatalf("truncated PES groups=%d err=%v", len(groups), err)
	}
}

func TestPESPublicationRetainsRetransmissionsWithoutClosingOrCountingThem(t *testing.T) {
	v := publicationTestPacket(256, true, []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}, 0)
	for _, name := range []string{"unbounded video PUSI", "bounded audio PUSI", "bounded audio continuation", "split header PUSI"} {
		t.Run(name, func(t *testing.T) {
			g := publicationTestGate()
			var packets [][]byte
			if name == "unbounded video PUSI" {
				a := publicationTestPacket(257, true, publicationTestHeader(20), 0)
				packets = [][]byte{v, a, v, publicationTestPacket(256, false, bytes.Repeat([]byte{0x55}, 184), 1)}
			} else if name == "bounded audio PUSI" {
				a := publicationTestHeader(300)
				first := publicationTestPacket(257, true, a[:184], 0)
				packets = [][]byte{v, first, first, publicationTestPacket(257, false, a[184:], 1)}
			} else if name == "bounded audio continuation" {
				a := publicationTestHeader(500)
				middle := publicationTestPacket(257, false, a[184:368], 1)
				packets = [][]byte{v, publicationTestPacket(257, true, a[:184], 0), middle, middle, publicationTestPacket(257, false, a[368:], 2)}
			} else {
				a := publicationTestHeader(120)
				first := publicationTestPacket(257, true, a[:2], 0)
				packets = [][]byte{v, first, first, publicationTestPacket(257, false, a[2:], 1)}
			}
			for i, packet := range packets {
				groups, err := g.push(packet)
				if err != nil || len(groups) > 0 {
					t.Fatalf("duplicate must not close/count PES at packet%d: groups=%d err=%v", i, len(groups), err)
				}
			}
			groups, err := g.push(publicationTestPacket(256, true, []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}, 2))
			wire := bytes.Join(groups, nil)
			if err != nil || !bytes.Equal(wire, bytes.Join(packets, nil)) {
				t.Fatalf("complete retransmitted interval bytes=%d err=%v", len(wire), err)
			}
		})
	}
}

func TestPESPublicationDuplicateExtendsCompletedPrivateOverlap(t *testing.T) {
	var g pesPublicationGate
	g.configure(avProgramPIDs{videoPID: 256, audioPIDs: [maxAVTimelineAudioPIDs]int{257, 258}, audioCount: 2}, true)
	if groups, err := g.push(joinTSPackets(publicationTestPacket(256, true, publicationTestHeader(20), 0), publicationTestPacket(257, true, publicationTestHeader(20), 0))); err != nil || len(groups) == 0 {
		t.Fatalf("initial selected A/V: groups=%d err=%v", len(groups), err)
	}
	v := publicationTestPacket(256, true, []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}, 1)
	a := publicationTestPacket(257, true, publicationTestHeader(20), 1)
	b := publicationTestHeader(300)
	for _, packet := range [][]byte{v, a, publicationTestPacket(258, true, b[:184], 0), a} {
		if groups, err := g.push(packet); err != nil || len(groups) > 0 {
			t.Fatalf("open video must retain complete audio and its duplicate: groups=%d err=%v", len(groups), err)
		}
	}
	// Closing video leaves the secondary audio open. The completed primary
	// audio spans that open edge via its duplicate and must stay private too.
	groups, err := g.push(publicationTestPacket(256, true, []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}, 2))
	if err != nil || !bytes.Equal(bytes.Join(groups, nil), v) {
		t.Fatalf("completed private audio duplicate split at another PID: published=%d want=%d err=%v", len(bytes.Join(groups, nil)), len(v), err)
	}
}

func TestPESPublicationBoundsIndefiniteOverlapWithoutTimer(t *testing.T) {
	g := publicationTestGate()
	v := []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 1}
	if groups, err := g.push(publicationTestPacket(256, true, v, 0)); err != nil || len(groups) > 0 {
		t.Fatal(err)
	}
	padding := publicationTestPacket(0x1fff, false, bytes.Repeat([]byte{0xff}, 184), 0)
	for i := 0; i < maxPESPublicationBytes/188+1; i++ {
		groups, err := g.push(padding)
		if len(groups) > 0 {
			t.Fatal("unproven unit published")
		}
		if len(g.pending) > maxPESPublicationBytes || len(g.packetTail) > 188 {
			t.Fatal("staging exceeded bound")
		}
		if err != nil {
			if !errors.Is(err, errTransportAttemptBoundary) {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("unbounded overlap accepted")
}

func TestRealSlateCompleteGroupsPreserveMediaCadenceAndDecode(t *testing.T) {
	ffmpeg, _ := requireAudioOriginTools(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sl, err := generateSlate(ctx, ffmpeg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	cursor := newSlateLoopCursor(sl)
	var wire []byte
	var duration time.Duration
	for duration < 2*sl.duration {
		b, d, err := cursor.nextComplete()
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 || len(b) > maxPESPublicationBytes {
			t.Fatalf("invalid group%d", len(b))
		}
		wire = append(wire, b...)
		duration += d
	}
	// Consume a possible audio-only final group with zero video cadence.
	for cursor.completeIndex < len(cursor.completeChunks) {
		b, d, err := cursor.nextComplete()
		if err != nil || d != 0 {
			t.Fatalf("final audio group duration=%v err=%v", d, err)
		}
		wire = append(wire, b...)
	}
	if duration != 2*sl.duration || len(wire) != 2*len(sl.data) {
		t.Fatalf("duration=%v bytes%d want%d", duration, len(wire), 2*len(sl.data))
	}
	decode := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0", "-threads", "2", "-f", "null", "-")
	decode.Stdin = bytes.NewReader(wire)
	if b, err := decode.CombinedOutput(); err != nil || len(b) > 0 {
		t.Fatalf("decode %v: %s", err, b)
	}
}

func TestSlateCompleteCadenceUsesDTSAndClipsKnownLoopEnd(t *testing.T) {
	// Presentation order crosses between B frames; decode time is the cadence.
	first := slateTestPacket(256, true, 0, nil, slateTestPES(0xe0, 18000, 9000, true))
	second := slateTestPacket(256, true, 1, nil, slateTestPES(0xe0, 14400, 12600, true))
	durations, err := slateGroupCadence([][]byte{first, second}, 256, 80*time.Millisecond)
	if err != nil || len(durations) != 2 || durations[0] != 40*time.Millisecond || durations[1] != 40*time.Millisecond {
		t.Fatalf("DTS cadence=%v err=%v", durations, err)
	}
	if _, err := slateGroupCadence([][]byte{second, first}, 256, 80*time.Millisecond); err == nil {
		t.Fatal("backward decode clock accepted")
	}
}

func TestSlateCompleteRejectsUnclockedSelectedMedia(t *testing.T) {
	input := joinTSPackets(testPATPacket(0x1000), testAVPMTPacketWithPCR(0x1000, 256, 257, 256),
		publicationTestPacket(256, true, []byte{0, 0, 1, 0xe0, 0, 4, 0x80, 0, 0, 1}, 0),
		publicationTestPacket(257, true, []byte{0, 0, 1, 0xc0, 0, 4, 0x80, 0, 0, 1}, 0))
	cursor := newSlateLoopCursor(&slate{data: input, duration: time.Second})
	if b, _, err := cursor.nextComplete(); len(b) != 0 || err == nil {
		t.Fatalf("clockless slate published=%d err=%v", len(b), err)
	}
}
