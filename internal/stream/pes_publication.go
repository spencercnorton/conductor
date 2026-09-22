package stream

import (
	"bytes"
	"fmt"
)

// A completed group is a single subscriber queue item. Its selected PES
// intervals do not cross its edges, even when the source interleaves PIDs.
// This is a PES guarantee, not a general codec access-unit/RAP guarantee.
const maxPESPublicationBytes = 2 * 1024 * 1024

type publicationPES struct {
	pid                                      int
	start, end                               int64
	header                                   [6]byte
	headerBytes, payloadBytes, declaredTotal int
	complete                                 bool
}

type publicationPayloadHistory struct{ packet [tsPacketSize]byte }

type pesPublicationGate struct {
	videoPID    int
	haveAudio   bool
	bound       bool
	selected    map[int]bool
	active      map[int]*publicationPES
	lastPES     map[int]*publicationPES
	lastPayload map[int]*publicationPayloadHistory
	spans       []*publicationPES
	pending     []byte
	packetTail  []byte
	base        int64
	firstReady  bool
}

func (g *pesPublicationGate) configure(program avProgramPIDs, ok bool) {
	*g = pesPublicationGate{}
	if !ok {
		return
	}
	g.bound = true
	g.videoPID = program.videoPID
	g.haveAudio = program.audioCount > 0
	g.selected = map[int]bool{program.videoPID: true}
	for i := 0; i < min(program.audioCount, maxAVTimelineAudioPIDs); i++ {
		g.selected[program.audioPIDs[i]] = true
	}
	g.active = make(map[int]*publicationPES)
	g.lastPES = make(map[int]*publicationPES)
	g.lastPayload = make(map[int]*publicationPayloadHistory)
}

func pesPublicationError(reason string) error {
	return fmt.Errorf("%w: incomplete selected PES publication: %s", errTransportAttemptBoundary, reason)
}

// push keeps original TS byte order. A prospective cut is moved backward
// across every overlapping completed PES, not just to the earliest open start.
// An indefinitely interleaved chain is rejected privately at the byte bound.
// No new wall timer is imposed: a zero-length video's next PUSI may legitimately
// arrive after an allowed provider pause. The caller retains its source and
// output-progress watchdogs.
func (g *pesPublicationGate) push(data []byte) ([][]byte, error) {
	if !g.bound {
		var out [][]byte
		for len(data) > 0 {
			n := min(len(data), publicationChunkSize)
			out = append(out, data[:n:n])
			data = data[n:]
		}
		return out, nil
	}
	var out [][]byte
	for len(data) > 0 {
		n := min(tsPacketSize-len(g.packetTail), len(data))
		g.packetTail = append(g.packetTail, data[:n]...)
		data = data[n:]
		if len(g.packetTail) < tsPacketSize {
			break
		}
		packet := g.packetTail[:tsPacketSize]
		if packet[0] != 0x47 {
			return nil, pesPublicationError("TS alignment")
		}
		if len(g.pending) > maxPESPublicationBytes-tsPacketSize {
			return nil, pesPublicationError("overlap group exceeds byte bound")
		}
		pos := g.base + int64(len(g.pending))
		g.pending = append(g.pending, packet...)
		pid := int(packet[1]&31)<<8 | int(packet[2])
		payload, hasPayload := tsPayloadStart(packet)
		if g.selected[pid] && hasPayload {
			if err := g.observeSelectedPayload(packet, pid, payload, pos); err != nil {
				return nil, err
			}
		}
		g.packetTail = g.packetTail[:0]
		if group := g.releaseCompletePrefix(); len(group) > 0 {
			out = append(out, group)
		} else if len(g.pending) >= publicationChunkSize {
			if group := g.releaseTrailingPackets(); len(group) > 0 {
				out = append(out, group)
			}
		}
	}
	if len(g.packetTail) == 0 {
		g.packetTail = nil
	}
	if group := g.releaseTrailingPackets(); len(group) > 0 {
		out = append(out, group)
	}
	return out, nil
}

// Upstream boundary/A/V gates already own continuity/discontinuity policy.
// An identical consecutive payload packet for a PID is a legal retransmission:
// retain its wire bytes but never count its PUSI/header/payload twice.
func (g *pesPublicationGate) observeSelectedPayload(packet []byte, pid, payload int, pos int64) error {
	prior := g.lastPayload[pid]
	if prior != nil && bytes.Equal(prior.packet[:], packet) {
		// A completed span can still be private behind another PID's open PES.
		// Include its duplicate in that overlap interval until it is released.
		if p := g.lastPES[pid]; p != nil && p.end > g.base {
			p.end = pos + tsPacketSize
		}
		return nil
	}
	if prior == nil {
		prior = &publicationPayloadHistory{}
		g.lastPayload[pid] = prior
	}
	copy(prior.packet[:], packet)
	p := g.active[pid]
	if packet[1]&0x40 != 0 {
		if p != nil {
			if p.headerBytes < 6 || p.declaredTotal > 0 && !p.complete {
				return pesPublicationError("new PUSI before declared PES completed")
			}
			p.complete = true
		}
		p = &publicationPES{pid: pid, start: pos}
		g.active[pid] = p
		g.lastPES[pid] = p
		g.spans = append(g.spans, p)
	} else if p == nil {
		return pesPublicationError("selected payload before PES start")
	}
	if p != nil {
		b := packet[payload:]
		if p.headerBytes < 6 {
			n := copy(p.header[p.headerBytes:], b)
			p.headerBytes += n
			if p.headerBytes == 6 {
				if !bytes.Equal(p.header[:3], []byte{0, 0, 1}) {
					return pesPublicationError("invalid PES header")
				}
				n := int(p.header[4])<<8 | int(p.header[5])
				if n > 0 {
					p.declaredTotal = n + 6
				}
			}
		}
		p.payloadBytes += len(b)
		p.end = pos + tsPacketSize
		if p.headerBytes == 6 && p.declaredTotal > 0 && p.payloadBytes >= p.declaredTotal {
			p.complete = true
			delete(g.active, pid)
		}
	}
	return nil
}

func (g *pesPublicationGate) releaseTrailingPackets() []byte {
	if !g.firstReady || len(g.active) > 0 || len(g.spans) > 0 || len(g.pending) == 0 {
		return nil
	}
	b := append([]byte(nil), g.pending...)
	g.base += int64(len(g.pending))
	g.pending = nil
	return b
}

func (g *pesPublicationGate) releaseCompletePrefix() []byte {
	if len(g.pending) == 0 {
		return nil
	}
	if !g.firstReady && !g.establishedBefore(g.base+int64(len(g.pending))) {
		return nil
	}
	cut := g.base + int64(len(g.pending))
	for _, p := range g.spans {
		if !p.complete && p.start < cut {
			cut = p.start
		}
	}
	for {
		prior := cut
		for _, p := range g.spans {
			if p.start < cut && p.end > cut {
				cut = p.start
			}
		}
		if cut == prior {
			break
		}
	}
	if cut <= g.base {
		return nil
	}
	if !g.firstReady && !g.establishedBefore(cut) {
		return nil
	}

	// Pure trailing PSI/PCR remains attached to the next complete media group.
	havePES := false
	for _, p := range g.spans {
		if p.complete && p.end <= cut {
			havePES = true
			break
		}
	}
	if !havePES {
		return nil
	}
	n := int(cut - g.base)
	group := append([]byte(nil), g.pending[:n]...)
	g.pending = append(g.pending[:0], g.pending[n:]...)
	g.base = cut
	g.firstReady = true
	kept := g.spans[:0]
	for _, p := range g.spans {
		if p.end > cut || !p.complete {
			kept = append(kept, p)
		}
	}
	clear(g.spans[len(kept):])
	g.spans = kept
	return group
}

func (g *pesPublicationGate) establishedBefore(cut int64) bool {
	video, audio := false, !g.haveAudio
	for _, p := range g.spans {
		if p.complete && p.end <= cut {
			if p.pid == g.videoPID {
				video = true
			} else if g.selected[p.pid] {
				audio = true
			}
		}
	}
	return video && audio
}

func (g *pesPublicationGate) waiting() bool { return g.bound && len(g.pending) > 0 }

// finishCleanEOF is only for a naturally completed mux/HTTP entity,
// never cancellation or an output/read failure. The muxer's clean EOF closes
// its final unbounded video PES. A declared nonzero PES must still be complete.
func (g *pesPublicationGate) finishCleanEOF() ([][]byte, error) {
	if !g.bound {
		return nil, nil
	}
	if len(g.packetTail) > 0 {
		return nil, pesPublicationError("partial TS at clean mux EOF")
	}
	for _, p := range g.active {
		if p.headerBytes < 6 || p.declaredTotal > 0 && !p.complete {
			return nil, pesPublicationError("declared PES truncated at EOF")
		}
		p.complete = true
	}
	clear(g.active)
	if !g.firstReady && !g.establishedBefore(g.base+int64(len(g.pending))) {
		return nil, pesPublicationError("selected A/V absent at EOF")
	}
	group := g.releaseCompletePrefix()
	if len(g.pending) > 0 {
		// Remaining bytes contain only complete nonselected packets following media.
		group = append(group, g.pending...)
		g.base += int64(len(g.pending))
		g.pending = nil
	}
	if len(group) == 0 {
		return nil, nil
	}
	return [][]byte{group}, nil
}
