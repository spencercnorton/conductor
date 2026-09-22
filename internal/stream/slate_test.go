package stream

import (
	"bytes"
	"context"
	"os/exec"
	"testing"
	"time"
)

func TestSlateLoopCursorKeepsTransportClockAndContinuity(t *testing.T) {
	const (
		videoPID = uint16(0x100)
		audioPID = uint16(0x101)
		videoPTS = uint64(180_000)
		videoDTS = uint64(171_000)
		audioPTS = uint64(178_080)
		pcr      = uint64(42_000_123)
	)

	videoStart := slateTestPacket(videoPID, true, 14,
		append([]byte{0x90}, slateTestPCR(pcr)...),
		slateTestPES(0xe0, videoPTS, videoDTS, true))
	audioStart := slateTestPacket(audioPID, true, 4, nil,
		slateTestPES(0xc0, audioPTS, 0, false))
	videoAdaptationOnly := slateTestPacket(videoPID, false, 14, []byte{0x00}, nil)
	videoPayload := slateTestPacket(videoPID, false, 15, nil, []byte{0xaa, 0xbb})
	data := bytes.Join([][]byte{
		slateTestPacket(0, true, 7, nil, []byte{0x00, 0x00}),
		slateTestPacket(0x20, true, 9, nil, []byte{0x00, 0x02}),
		videoStart,
		audioStart,
		videoAdaptationOnly,
		videoPayload,
	}, nil)
	original := append([]byte(nil), data...)

	cursor := newSlateLoopCursor(&slate{data: data, duration: 5 * time.Second})
	loops := make([][]byte, 3)
	for i := range loops {
		loops[i] = cursor.next(len(data) + 1)
		if len(loops[i]) != len(data) {
			t.Fatalf("loop %d length = %d, want %d", i, len(loops[i]), len(data))
		}
	}
	if !bytes.Equal(data, original) {
		t.Fatal("cursor mutated the process-shared slate bytes")
	}

	ptsStep := durationTicks(5*time.Second, 90_000)
	pcrStep := durationTicks(5*time.Second, 27_000_000)
	for loop, rendered := range loops {
		gotPTS, gotDTS := slateTestPESClock(t, rendered, videoPID)
		if want := (videoPTS + uint64(loop)*ptsStep) % ptsModulus; gotPTS != want {
			t.Errorf("loop %d video PTS = %d, want %d", loop, gotPTS, want)
		}
		if want := (videoDTS + uint64(loop)*ptsStep) % ptsModulus; gotDTS != want {
			t.Errorf("loop %d video DTS = %d, want %d", loop, gotDTS, want)
		}
		gotAudioPTS, _ := slateTestPESClock(t, rendered, audioPID)
		if want := (audioPTS + uint64(loop)*ptsStep) % ptsModulus; gotAudioPTS != want {
			t.Errorf("loop %d audio PTS = %d, want %d", loop, gotAudioPTS, want)
		}
		if got := slateTestPCRClock(t, rendered, videoPID); got != (pcr+uint64(loop)*pcrStep)%pcrModulus {
			t.Errorf("loop %d PCR = %d, want %d", loop, got, (pcr+uint64(loop)*pcrStep)%pcrModulus)
		}

		videoCounters := slateTestCounters(rendered, videoPID)
		wantCounters := [][]byte{{14, 14, 15}, {0, 0, 1}, {2, 2, 3}}[loop]
		if !bytes.Equal(videoCounters, wantCounters) {
			t.Errorf("loop %d video counters = %v, want %v", loop, videoCounters, wantCounters)
		}
		if got, want := slateTestCounters(rendered, audioPID), []byte{4 + byte(loop)}; !bytes.Equal(got, want) {
			t.Errorf("loop %d audio counters = %v, want %v", loop, got, want)
		}
		if got, want := slateTestCounters(rendered, 0), []byte{7 + byte(loop)}; !bytes.Equal(got, want) {
			t.Errorf("loop %d PAT counters = %v, want %v", loop, got, want)
		}

		packet := rendered[2*slateTSPacketSize : 3*slateTSPacketSize]
		if got, want := packet[5]&0x80 != 0, loop == 0; got != want {
			t.Errorf("loop %d discontinuity flag = %v, want %v", loop, got, want)
		}
	}
}

func TestSlateLoopCursorRewritesSplitPESHeaderAcrossTimestampWrap(t *testing.T) {
	const pid = uint16(0x120)
	pts := ptsModulus - 1_000
	dts := ptsModulus - 2_000
	pes := slateTestPES(0xe0, pts, dts, true)

	// A large adaptation field leaves only eight payload bytes in the first
	// packet, splitting the optional PES timestamps across the next packet.
	first := slateTestPacket(pid, true, 1, make([]byte, 175), pes[:8])
	second := slateTestPacket(pid, false, 2, nil, pes[8:])
	data := append(first, second...)
	cursor := newSlateLoopCursor(&slate{data: data, duration: 20 * time.Millisecond})
	_ = cursor.next(len(data))
	secondLoop := cursor.next(len(data))

	gotPTS, gotDTS := slateTestPESClock(t, secondLoop, pid)
	step := durationTicks(20*time.Millisecond, 90_000)
	if want := (pts + step) % ptsModulus; gotPTS != want {
		t.Fatalf("wrapped PTS = %d, want %d", gotPTS, want)
	}
	if want := (dts + step) % ptsModulus; gotDTS != want {
		t.Fatalf("wrapped DTS = %d, want %d", gotDTS, want)
	}
	if got, want := slateTestCounters(secondLoop, pid), []byte{3, 4}; !bytes.Equal(got, want) {
		t.Fatalf("split-PES counters = %v, want %v", got, want)
	}
}

func TestSlateLoopCursorPreservesNonTransportTestSlate(t *testing.T) {
	data := []byte{0xab, 0xcd, 0xef}
	cursor := newSlateLoopCursor(&slate{data: data, duration: time.Second})
	if got := cursor.next(2); !bytes.Equal(got, data[:2]) {
		t.Fatalf("first raw chunk = %x, want %x", got, data[:2])
	}
	if got := cursor.next(2); !bytes.Equal(got, data[2:]) {
		t.Fatalf("second raw chunk = %x, want %x", got, data[2:])
	}
	if got := cursor.next(2); !bytes.Equal(got, data[:2]) {
		t.Fatalf("wrapped raw chunk = %x, want %x", got, data[:2])
	}
}

func TestSlateFeedStartsNewClockEpochAfterRealMedia(t *testing.T) {
	const pid = uint16(0x100)
	data := slateTestPacket(pid, true, 0,
		append([]byte{0x90}, slateTestPCR(1_000)...),
		slateTestPES(0xe0, 9_000, 0, false))
	sl := &slate{data: data, duration: time.Second}

	var subscribers subscriberSet
	subscribers.init("slate-epoch-test", testLogger())
	subscribers.slateFeed = newSlateLoopCursor(sl)
	_ = subscribers.slateFeed.next(len(data))
	repeated := subscribers.slateFeed.next(len(data))
	if repeated[5]&0x80 != 0 {
		t.Fatal("repeated loop retained initial discontinuity flag")
	}

	subscribers.fanout([]byte("real-media"))
	if subscribers.slateFeed != nil {
		t.Fatal("real media did not end the prior synthetic clock epoch")
	}
	subscribers.slateFeed = newSlateLoopCursor(sl)
	newEpoch := subscribers.slateFeed.next(len(data))
	if newEpoch[5]&0x80 == 0 {
		t.Fatal("new outage epoch lost its initial discontinuity flag")
	}
}

// TestGenerateSlateWithFFmpeg renders the real slate. Skipped where ffmpeg
// isn't installed (CI's go_test image); the cuda runtime image always has it.
func TestGenerateSlateWithFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not on PATH")
	}
	sl, err := generateSlate(context.Background(), "", testLogger())
	if err != nil {
		t.Fatalf("generateSlate: %v", err)
	}
	if len(sl.data) < 188*20 {
		t.Fatalf("slate suspiciously small: %d bytes", len(sl.data))
	}
	if sl.data[0] != 0x47 {
		t.Fatalf("slate does not start with TS sync byte: 0x%02x", sl.data[0])
	}
	if !isAlignedMPEGTS(sl.data) {
		t.Fatalf("slate is not packet-aligned MPEG-TS: %d bytes", len(sl.data))
	}
	if sl.duration != slateDuration {
		t.Fatalf("duration = %v, want %v", sl.duration, slateDuration)
	}
	if got := slateTestDiscontinuityCount(sl.data); got == 0 {
		t.Fatal("slate does not mark its initial transport discontinuity")
	}

	cursor := newSlateLoopCursor(sl)
	first := cursor.next(len(sl.data))
	second := cursor.next(len(sl.data))
	slateTestAssertContinuity(t, append(append([]byte(nil), first...), second...))

	firstClocks := slateTestAllPESClocks(first)
	secondClocks := slateTestAllPESClocks(second)
	if len(firstClocks) < 2 {
		t.Fatalf("slate has %d timestamped elementary streams, want video and audio", len(firstClocks))
	}
	step := durationTicks(slateDuration, 90_000)
	for pid, clocks := range firstClocks {
		if len(clocks) == 0 || len(secondClocks[pid]) == 0 {
			t.Fatalf("PID 0x%x missing PES clocks across loop boundary", pid)
		}
		firstNext := secondClocks[pid][0]
		if want := (clocks[0].pts + step) % ptsModulus; firstNext.pts != want {
			t.Errorf("PID 0x%x next-loop PTS = %d, want %d", pid, firstNext.pts, want)
		}
		maxPTS, maxDTS := clocks[0].pts, clocks[0].dts
		for _, clock := range clocks[1:] {
			maxPTS = max(maxPTS, clock.pts)
			maxDTS = max(maxDTS, clock.dts)
		}
		if maxPTS >= firstNext.pts {
			t.Errorf("PID 0x%x PTS overlaps loop boundary: max=%d next=%d", pid, maxPTS, firstNext.pts)
		}
		if clocks[0].hasDTS {
			if want := (clocks[0].dts + step) % ptsModulus; firstNext.dts != want {
				t.Errorf("PID 0x%x next-loop DTS = %d, want %d", pid, firstNext.dts, want)
			}
			if maxDTS >= firstNext.dts {
				t.Errorf("PID 0x%x DTS overlaps loop boundary: max=%d next=%d", pid, maxDTS, firstNext.dts)
			}
		}
	}
}

type slateTestPESClockValue struct {
	pts    uint64
	dts    uint64
	hasDTS bool
}

func slateTestAllPESClocks(data []byte) map[uint16][]slateTestPESClockValue {
	clocks := make(map[uint16][]slateTestPESClockValue)
	for start := 0; start+slateTSPacketSize <= len(data); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		if packet[1]&0x40 == 0 {
			continue
		}
		pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
		positions := slateTestPayloadPositions(data, start, pid, 19)
		if len(positions) < 14 || data[positions[0]] != 0 || data[positions[1]] != 0 || data[positions[2]] != 1 {
			continue
		}
		flags := data[positions[7]] >> 6
		if flags != 2 && flags != 3 {
			continue
		}
		clock := slateTestPESClockValue{pts: slateTestDecodeTimestamp(data, positions[9:14])}
		if flags == 3 && len(positions) >= 19 {
			clock.dts = slateTestDecodeTimestamp(data, positions[14:19])
			clock.hasDTS = true
		}
		clocks[pid] = append(clocks[pid], clock)
	}
	return clocks
}

func slateTestAssertContinuity(t *testing.T, data []byte) {
	t.Helper()
	type state struct {
		havePayload bool
		last        byte
	}
	states := make(map[uint16]state)
	for start := 0; start+slateTSPacketSize <= len(data); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
		if pid == 0x1fff {
			continue
		}
		adaptationControl := (packet[3] >> 4) & 0x03
		got := packet[3] & 0x0f
		current := states[pid]
		switch {
		case adaptationControl == 1 || adaptationControl == 3:
			if current.havePayload {
				if want := (current.last + 1) & 0x0f; got != want {
					t.Errorf("PID 0x%x payload counter at packet %d = %d, want %d", pid, start/slateTSPacketSize, got, want)
				}
			}
			current.havePayload = true
			current.last = got
		case adaptationControl == 2 && current.havePayload:
			if got != current.last {
				t.Errorf("PID 0x%x adaptation counter at packet %d = %d, want %d", pid, start/slateTSPacketSize, got, current.last)
			}
		}
		states[pid] = current
	}
}

func slateTestPacket(pid uint16, payloadStart bool, counter byte, adaptation, payload []byte) []byte {
	packet := bytes.Repeat([]byte{0xff}, slateTSPacketSize)
	packet[0] = 0x47
	packet[1] = byte(pid >> 8)
	if payloadStart {
		packet[1] |= 0x40
	}
	packet[2] = byte(pid)
	pos := 4
	switch {
	case adaptation != nil && payload != nil:
		packet[3] = 0x30 | counter&0x0f
		packet[4] = byte(len(adaptation))
		copy(packet[5:], adaptation)
		pos = 5 + len(adaptation)
	case adaptation != nil:
		packet[3] = 0x20 | counter&0x0f
		packet[4] = byte(len(adaptation))
		copy(packet[5:], adaptation)
		return packet
	default:
		packet[3] = 0x10 | counter&0x0f
	}
	copy(packet[pos:], payload)
	return packet
}

func slateTestPES(streamID byte, pts, dts uint64, includeDTS bool) []byte {
	headerLength := byte(5)
	flags := byte(0x80)
	if includeDTS {
		headerLength = 10
		flags = 0xc0
	}
	pes := []byte{0, 0, 1, streamID, 0, 0, 0x80, flags, headerLength}
	pes = append(pes, slateTestTimestamp(pts, map[bool]byte{true: 0x3, false: 0x2}[includeDTS])...)
	if includeDTS {
		pes = append(pes, slateTestTimestamp(dts, 0x1)...)
	}
	// A PES timestamp is useful media progress only once at least one byte of
	// elementary-stream payload follows the complete optional header.
	return append(pes, 0x00)
}

func slateTestTimestamp(value uint64, prefix byte) []byte {
	value %= ptsModulus
	return []byte{
		prefix<<4 | byte(value>>29)&0x0e | 0x01,
		byte(value >> 22),
		byte(value>>14)&0xfe | 0x01,
		byte(value >> 7),
		byte(value<<1) | 0x01,
	}
}

func slateTestPCR(total uint64) []byte {
	total %= pcrModulus
	base, extension := total/300, total%300
	return []byte{
		byte(base >> 25),
		byte(base >> 17),
		byte(base >> 9),
		byte(base >> 1),
		byte(base&1)<<7 | 0x7e | byte(extension>>8),
		byte(extension),
	}
}

func slateTestPESClock(t *testing.T, data []byte, pid uint16) (uint64, uint64) {
	t.Helper()
	for start := 0; start+slateTSPacketSize <= len(data); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		if uint16(packet[1]&0x1f)<<8|uint16(packet[2]) != pid || packet[1]&0x40 == 0 {
			continue
		}
		positions := slateTestPayloadPositions(data, start, pid, 19)
		if len(positions) < 14 {
			continue
		}
		pts := slateTestDecodeTimestamp(data, positions[9:14])
		var dts uint64
		if data[positions[7]]>>6 == 3 && len(positions) >= 19 {
			dts = slateTestDecodeTimestamp(data, positions[14:19])
		}
		return pts, dts
	}
	t.Fatalf("PID 0x%x PES timestamp not found", pid)
	return 0, 0
}

func slateTestPayloadPositions(data []byte, packetStart int, pid uint16, count int) []int {
	positions := make([]int, 0, count)
	for start := packetStart; start+slateTSPacketSize <= len(data) && len(positions) < count; start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		if uint16(packet[1]&0x1f)<<8|uint16(packet[2]) != pid {
			continue
		}
		if start != packetStart && packet[1]&0x40 != 0 {
			break
		}
		payloadStart, ok := tsPayloadStart(packet)
		if !ok {
			continue
		}
		for i := payloadStart; i < len(packet) && len(positions) < count; i++ {
			positions = append(positions, start+i)
		}
	}
	return positions
}

func slateTestDecodeTimestamp(data []byte, positions []int) uint64 {
	b0, b1 := data[positions[0]], data[positions[1]]
	b2, b3, b4 := data[positions[2]], data[positions[3]], data[positions[4]]
	return uint64(b0&0x0e)<<29 |
		uint64(b1)<<22 |
		uint64(b2&0xfe)<<14 |
		uint64(b3)<<7 |
		uint64(b4>>1)
}

func slateTestPCRClock(t *testing.T, data []byte, pid uint16) uint64 {
	t.Helper()
	for start := 0; start+slateTSPacketSize <= len(data); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		if uint16(packet[1]&0x1f)<<8|uint16(packet[2]) != pid || packet[5]&0x10 == 0 {
			continue
		}
		field := packet[6:12]
		base := uint64(field[0])<<25 |
			uint64(field[1])<<17 |
			uint64(field[2])<<9 |
			uint64(field[3])<<1 |
			uint64(field[4]>>7)
		extension := uint64(field[4]&1)<<8 | uint64(field[5])
		return base*300 + extension
	}
	t.Fatalf("PID 0x%x PCR not found", pid)
	return 0
}

func slateTestCounters(data []byte, pid uint16) []byte {
	var counters []byte
	for start := 0; start+slateTSPacketSize <= len(data); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		if uint16(packet[1]&0x1f)<<8|uint16(packet[2]) == pid {
			counters = append(counters, packet[3]&0x0f)
		}
	}
	return counters
}

func slateTestDiscontinuityCount(data []byte) int {
	count := 0
	for start := 0; start+slateTSPacketSize <= len(data); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		adaptationControl := (packet[3] >> 4) & 0x03
		if adaptationControl >= 2 && packet[4] > 0 && packet[5]&0x80 != 0 {
			count++
		}
	}
	return count
}
