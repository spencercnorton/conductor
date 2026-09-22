package stream

import (
	"fmt"
	"time"
)

// nextComplete returns one complete selected-PES overlap group and its media
// cadence. A single owner must use this API for a cursor (do not mix with next).
// Groups never cross rendered-loop boundaries; clock rewriting remains owned
// by renderLoop. The caller paces media time, not the variable encoded byte rate.
func (c *slateLoopCursor) nextComplete() ([]byte, time.Duration, error) {
	if c == nil || c.slate == nil || len(c.slate.data) == 0 || c.slate.duration <= 0 {
		return nil, 0, nil
	}
	if !c.isTS {
		// Legacy non-TS fixtures retain their old byte-exact bounded behavior.
		b := c.next(publicationChunkSize)
		return b, time.Duration(float64(len(b)) / float64(len(c.slate.data)) * float64(c.slate.duration)), nil
	}
	if c.completeIndex >= len(c.completeChunks) {
		c.renderLoop()
		selection, ready, err := selectCheckedInputProgram(c.loop)
		if err != nil {
			return nil, 0, fmt.Errorf("slate selected program unavailable: %w", err)
		}
		if !ready {
			return nil, 0, fmt.Errorf("slate selected program unavailable")
		}
		var gate pesPublicationGate
		gate.configure(selection.program, true)
		groups, err := gate.push(c.loop)
		if err != nil {
			return nil, 0, err
		}
		tail, err := gate.finishCleanEOF()
		if err != nil {
			return nil, 0, err
		}
		groups = append(groups, tail...)
		durations, err := slateGroupCadence(groups, selection.program.videoPID, c.slate.duration)
		if err != nil {
			return nil, 0, err
		}
		c.completeChunks, c.completeDurations, c.completeIndex = groups, durations, 0
		c.pos = len(c.loop)
	}
	i := c.completeIndex
	c.completeIndex++
	return c.completeChunks[i], c.completeDurations[i], nil
}

func slateGroupCadence(groups [][]byte, videoPID int, loopDuration time.Duration) ([]time.Duration, error) {
	type edge struct {
		index int
		clock uint64
	}
	var edges []edge
	for i, group := range groups {
		var parser pesPTSAssembler
		for off := 0; off+tsPacketSize <= len(group); off += tsPacketSize {
			packet := group[off : off+tsPacketSize]
			pid := int(packet[1]&31)<<8 | int(packet[2])
			if pid != videoPID {
				continue
			}
			payload, ok := tsPayload(packet)
			if !ok {
				continue
			}
			pts, status := parser.feed(payload, packet[1]&0x40 != 0)
			if status == pesPTSMalformed {
				return nil, fmt.Errorf("slate has malformed video PES clock")
			}
			if status != pesPTSPresent {
				continue
			}
			clock := pts
			if parser.count >= 19 && parser.buffer[7]&0x40 != 0 {
				clock = decodeMPEGTimestamp(parser.buffer[14:19])
			}
			if len(edges) == 0 || edges[len(edges)-1].index != i {
				edges = append(edges, edge{i, clock})
			}
		}
	}
	if len(edges) == 0 {
		return nil, fmt.Errorf("slate has no complete selected video clocks")
	}
	result := make([]time.Duration, len(groups))
	first := edges[0].clock
	var previous time.Duration
	for i, e := range edges {
		begin := ticksSignedDuration(int64((e.clock - first) % ptsModulus))
		end := loopDuration
		if i+1 < len(edges) {
			end = ticksSignedDuration(int64((edges[i+1].clock - first) % ptsModulus))
		}
		if begin < previous || begin >= loopDuration || end <= begin || end > loopDuration {
			return nil, fmt.Errorf("slate has nonmonotonic or out-of-loop video cadence")
		}
		result[e.index] = end - begin
		previous = end
	}
	return result, nil
}
