package stream

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

const (
	transportClockRate       = uint64(27_000_000)
	maxTransportPCRStepSkew  = 3 * time.Second
	maxTransportAddedLatency = time.Second
	maxTransportPaceBehind   = 500 * time.Millisecond
	maxTransportRecoveryRate = 16
	// The live read-ahead handoff is byte-bounded, not duration-bounded. At a
	// lower bitrate its one queued chunk plus the producer-held chunk can be
	// more than 100ms old even after backpressure has reached the source edge.
	// Use the same fail-closed window as pacing; advancing arrival cadence still
	// has to prove this is the edge rather than a compressed catch-up burst.
	transportRecoveryEdgeLag = maxTransportAddedLatency - transportWakeReserve
	transportWakeReserve     = 25 * time.Millisecond
	transportSyncPacketCount = 5
	// Lead trim: the reservoir deliberately holds seconds of lead (see the
	// underflow re-anchor in Wait), and a source whose clock runs faster than
	// wall time grows that lead without bound. Above the high-water mark the
	// pacer drains at leadTrimRate back to the low-water mark, then returns to
	// media cadence. 2x is the gentlest rate that still trims; the downstream
	// HLS client simply gains the trimmed media as buffer.
	leadTrimHighWaterChunks = liveRelayReadAheadChunks * 3 / 4
	leadTrimLowWaterChunks  = liveRelayReadAheadChunks / 2
	leadTrimRate            = 2
	// Total garbage a live attempt may drop across mid-stream resyncs. The one
	// captured real loss burned ~16 KB; a stream burning more than this is not
	// realigning, it is dying, and deserves a fresh attempt.
	maxTransportResyncDropBytes = 256 * 1024
)

var errTransportRecoveryBacklog = errors.New("transport recovery exceeds bounded pacing window")

// errTransportSkipStale tells the read loop to DISCARD the rest of the current
// media batch and keep the attempt: the backlog is too old for any bounded
// acceleration to fit the live-latency budget, and a live viewer wants the
// live edge, not a replay of the backlog. The pacer has already armed
// live-edge re-entry (fresh discontinuity markers on every selected PID plus
// a wall/PCR re-origin at the next marked clock).
var errTransportSkipStale = errors.New("transport skips stale backlog to rejoin the live edge")
var errTransportStaleChunk = errors.New("transport chunk exceeds live relay latency bound")
var errTransportRelayBacklog = errors.New("transport relay backlog cannot fit live pacing window")
var errTransportAttemptBoundary = errors.New("transport clock boundary requires a fresh media attempt")
var errTransportProgramMapStall = errors.New("selected program map stalled beyond live relay deadline")

const (
	selectedPIDFlag       = byte(1 << 0)
	selectedPIDActiveFlag = byte(1 << 1)
)

// selectedProgramBoundaryScanner is also the final packet-bounded transform
// before fanout. The private startup gate marks every PID already present in
// its prefix, but a PMT-declared audio or distinct PCR PID may legitimately
// first appear later. This transform inserts a decoder-visible adaptation-only
// marker immediately before that first selected-PID packet without delaying
// startup. Any later explicit marker is an attempt boundary and is rejected
// before any bytes from its input slice reach fanout.
type selectedProgramBoundaryScanner struct {
	buffer []byte
	held   []byte
	synced bool
	flags  [0x2000]byte
	bound  bool
	maps   selectedProgramMapObserver
	// Bounded mid-stream resync: a capture-proven sync loss realigns
	// the transport off the old 188 grid; the pre-resync behavior failed the
	// whole attempt at the first misaligned byte even though the clock and the
	// programme continue. Garbage is dropped (never emitted), selected PIDs are
	// re-marked with decoder-visible discontinuities, and the exact PAT/PMT must
	// revalidate byte-identically before any post-resync payload is released.
	resyncing         bool
	resyncDropped     int
	resyncCompletions int
	// discardOutput runs the full alignment + PSI observation without emitting
	// packets or consuming the re-armed marker flags: skip episodes use it to
	// keep PAT/PMT continuity coherent across batches that are dropped before
	// the live edge arrives.
	discardOutput bool
}

func (s *selectedProgramBoundaryScanner) configure(program avProgramPIDs, ok bool) {
	*s = selectedProgramBoundaryScanner{}
	if !ok || program.videoPID < 0 || program.videoPID >= 0x1fff {
		return
	}
	s.bound = true
	s.maps.configure(program.mapIdentity)
	s.addSelectedPID(program.videoPID)
	for i := 0; i < min(program.audioCount, maxAVTimelineAudioPIDs); i++ {
		s.addSelectedPID(program.audioPIDs[i])
	}
	if program.pcrPID >= 0 && program.pcrPID < 0x1fff {
		s.addSelectedPID(program.pcrPID)
	}
}

func (s *selectedProgramBoundaryScanner) addSelectedPID(pid int) {
	if pid < 0 || pid >= 0x1fff || s.flags[pid]&selectedPIDFlag != 0 {
		return
	}
	s.flags[pid] |= selectedPIDFlag
}

// transform accepts arbitrary read/chunk splits and emits only complete TS
// packets. It retains at most a four-packet sync window plus one partial packet;
// ordinary live output is delayed by at most 187 bytes. Scanner state is
// attempt-local, so a boundary error discards it with the rejected attempt.
func (s *selectedProgramBoundaryScanner) transform(data []byte) ([]byte, error) {
	return s.transformAt(data, time.Now())
}

func (s *selectedProgramBoundaryScanner) transformAt(data []byte, observedAt time.Time) ([]byte, error) {
	if !s.bound || len(data) == 0 {
		return data, nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	s.buffer = append(s.buffer, data...)
	var output []byte
	for {
		if !s.synced {
			offset, ok := findTSSyncOffset(s.buffer)
			if s.resyncing {
				offset, ok = findTSResyncOffset(s.buffer)
			}
			if !ok {
				if !s.resyncing {
					if len(s.buffer) > tsPacketSize*(transportSyncPacketCount+1) {
						return nil, errTransportAttemptBoundary
					}
					return output, nil
				}
				// Keep a sync-window tail; everything before it is garbage.
				if keep := tsPacketSize*transportSyncPacketCount - 1; len(s.buffer) > keep {
					s.resyncDropped += len(s.buffer) - keep
					if s.resyncDropped > maxTransportResyncDropBytes {
						return nil, errTransportAttemptBoundary
					}
					copy(s.buffer, s.buffer[len(s.buffer)-keep:])
					s.buffer = s.buffer[:keep]
				}
				return output, nil
			}
			if offset > 0 {
				if s.resyncing {
					// Mid-stream garbage is dropped, never published.
					s.resyncDropped += offset
					if s.resyncDropped > maxTransportResyncDropBytes {
						return nil, errTransportAttemptBoundary
					}
				} else {
					output = append(output, s.buffer[:offset]...)
				}
				s.buffer = s.buffer[offset:]
			}
			if s.resyncing {
				// Completion is what the pacer must react to: the FIRST
				// post-resync PCR carries the continuation semantics, and
				// pre-loss packets emitted while garbage was still being
				// dropped must not consume the one-shots early. Counted, not
				// boolean: two realignments completing before the pacer sees
				// the output must hand the PCR scanner two continuations.
				s.resyncing = false
				s.resyncCompletions++
			}
			s.synced = true
		}

		consumed := 0
		if output == nil {
			output = make([]byte, 0, len(s.buffer)+tsPacketSize)
		}
		for consumed+tsPacketSize <= len(s.buffer) {
			packet := s.buffer[consumed : consumed+tsPacketSize]
			if packet[0] != 0x47 {
				s.beginResync(observedAt)
				break
			}
			pendingMap, changedMap := s.maps.observeAt(packet, observedAt)
			if changedMap {
				return nil, s.maps.failure()
			}
			pid := int(packet[1]&0x1f)<<8 | int(packet[2])
			if s.discardOutput {
				consumed += tsPacketSize
				continue
			}
			packetOutput := packet
			if s.flags[pid]&selectedPIDFlag != 0 {
				if tsPacketHasDiscontinuity(packet) && s.flags[pid]&selectedPIDActiveFlag != 0 {
					return nil, errTransportAttemptBoundary
				}
				if s.flags[pid]&selectedPIDActiveFlag == 0 {
					if !tsPacketHasDiscontinuity(packet) {
						packetOutput = append(newTSDiscontinuityMarker(packet), packet...)
					}
					s.flags[pid] |= selectedPIDActiveFlag
				}
			}
			if pendingMap {
				s.held = append(s.held, packetOutput...)
				if len(s.held) > maxSelectedProgramMapHoldBytes {
					return nil, errTransportProgramMapStall
				}
			} else {
				if len(s.held) > 0 {
					output = append(output, s.held...)
					s.held = nil
				}
				output = append(output, packetOutput...)
			}
			consumed += tsPacketSize
		}
		if consumed > 0 {
			copy(s.buffer, s.buffer[consumed:])
			s.buffer = s.buffer[:len(s.buffer)-consumed]
		}
		if !s.synced {
			continue
		}
		return output, nil
	}
}

const maxSelectedProgramMapHoldBytes = 2 * 1024 * 1024

// findTSResyncOffset scans the whole buffer for five consecutively strided
// sync bytes. Unlike findTSSyncOffset's 188-byte window (sized for a stream
// that is already nearly aligned), a mid-stream loss can bury kilobytes of
// garbage before the realigned grid — and valid post-loss packets may share
// the buffer with that garbage, so the scan must reach them instead of
// discarding to a blind tail.
func findTSResyncOffset(data []byte) (int, bool) {
	for offset := 0; offset+tsPacketSize*transportSyncPacketCount <= len(data); offset++ {
		matched := true
		for packet := 0; packet < transportSyncPacketCount; packet++ {
			if data[offset+packet*tsPacketSize] != 0x47 {
				matched = false
				break
			}
		}
		if matched {
			return offset, true
		}
	}
	return 0, false
}

// beginResync arms the bounded mid-stream realignment: garbage from here on is
// dropped, every selected PID gets a fresh decoder-visible discontinuity
// marker before its next payload, and the exact startup PAT/PMT must
// revalidate before held post-resync payload is released.
func (s *selectedProgramBoundaryScanner) beginResync(observedAt time.Time) {
	s.synced = false
	s.resyncing = true
	s.clearSelectedActive()
	s.maps.resyncAt(observedAt)
	metrics.RelayTransportResyncs.Inc()
}

// clearSelectedActive re-arms the discontinuity-marker machinery: the next
// payload of every selected PID gets a decoder-visible marker. Used by the
// garbage resync and by live-edge skip episodes (which lose media but not
// PSI, so they need no PAT/PMT revalidation).
func (s *selectedProgramBoundaryScanner) clearSelectedActive() {
	for pid := range s.flags {
		s.flags[pid] &^= selectedPIDActiveFlag
	}
}

// selectedProgramMapObserver assembles the exact current PAT table and
// startup-selected PMT across packet/read splits. Repeated byte-identical PSI
// is released; a complete current table that differs in version, program/PID
// binding, descriptors, or CRC is an unpublished attempt boundary.
type selectedProgramMapObserver struct {
	expected     selectedProgramMapIdentity
	enabled      bool
	patAssembler psiSectionAssembler
	pendingPAT   *patTableAccumulator
	pmtAssembler psiSectionAssembler
	patCC        tsPayloadContinuity
	pmtCC        tsPayloadContinuity
	patActive    bool
	pmtActive    bool
	pmtValidated bool
	revalidating bool
	pendingSince time.Time
	lastFailure  error
}

// resyncAt discards partially assembled PSI state after a transport resync and
// holds every selected packet until the exact startup PAT/PMT revalidates.
// The existing pending-stall bound applies: a stream that cannot re-prove its
// programme inside maxPublicationAVGateHold is an attempt boundary.
func (o *selectedProgramMapObserver) resyncAt(observedAt time.Time) {
	if !o.enabled {
		return
	}
	o.patAssembler.reset()
	o.pmtAssembler.reset()
	o.patCC.reset()
	o.pmtCC.reset()
	o.patActive = false
	o.pmtActive = false
	o.pendingPAT = nil
	o.pmtValidated = false
	o.revalidating = true
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	if o.pendingSince.IsZero() || observedAt.Before(o.pendingSince) {
		o.pendingSince = observedAt
	}
}

func (o *selectedProgramMapObserver) configure(identity selectedProgramMapIdentity) {
	*o = selectedProgramMapObserver{}
	if !identity.valid() {
		return
	}
	o.expected = identity.clone()
	o.enabled = true
	o.patAssembler.start = -1
	o.pmtAssembler.start = -1
}

func (o *selectedProgramMapObserver) observe(packet []byte) (pending, changed bool) {
	return o.observeAt(packet, time.Now())
}

func (o *selectedProgramMapObserver) observeAt(
	packet []byte,
	observedAt time.Time,
) (pending, changed bool) {
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	o.lastFailure = nil
	defer func() {
		if changed {
			if o.lastFailure == nil {
				o.lastFailure = errTransportAttemptBoundary
			}
			o.pendingSince = time.Time{}
			return
		}
		if !pending {
			o.pendingSince = time.Time{}
			return
		}
		if o.pendingSince.IsZero() || observedAt.Before(o.pendingSince) {
			o.pendingSince = observedAt
			return
		}
		if observedAt.Sub(o.pendingSince) >= maxPublicationAVGateHold {
			pending = false
			changed = true
			o.lastFailure = errTransportProgramMapStall
			o.pendingSince = time.Time{}
		}
	}()
	if !o.enabled || len(packet) != tsPacketSize || packet[0] != 0x47 {
		return false, false
	}
	pid := int(packet[1]&0x1f)<<8 | int(packet[2])
	if pid != 0 && pid != o.expected.pmtPID {
		return o.revalidating || len(o.patAssembler.buffer) > 0 || o.pendingPAT != nil ||
			len(o.pmtAssembler.buffer) > 0, false
	}
	if !tsPacketStructureValid(packet) {
		return false, true
	}
	if psiPUSIStartsMalformed(packet) {
		return false, true
	}
	active := &o.patActive
	assembler := &o.patAssembler
	continuity := &o.patCC
	if pid == o.expected.pmtPID {
		active = &o.pmtActive
		assembler = &o.pmtAssembler
		continuity = &o.pmtCC
	}
	if packet[1]&0x80 != 0 || packet[3]&0xc0 != 0 || (packet[3]>>4)&0x03 == 0 {
		return false, true
	}
	if tsPacketHasDiscontinuity(packet) {
		if *active {
			return false, true
		}
		*active = true
		assembler.reset()
		continuity.reset()
	}
	*active = true
	_, duplicate, corrupt := continuity.accept(packet)
	if corrupt {
		return false, true
	}
	if duplicate {
		return len(o.patAssembler.buffer) > 0 || o.pendingPAT != nil ||
			len(o.pmtAssembler.buffer) > 0, false
	}
	switch pid {
	case 0:
		sections := o.patAssembler.push(packet, 0)
		if o.patAssembler.lastInvalid || o.patAssembler.lastAbandoned {
			return false, true
		}
		for _, section := range sections {
			parsed, ok := parsePATSection(section.data)
			if !ok {
				return false, true
			}
			if !parsed.current {
				continue
			}
			if o.pendingPAT == nil || !o.pendingPAT.matches(parsed) {
				o.pendingPAT = newPATTableAccumulator(parsed)
			}
			if !o.pendingPAT.add(parsed, 0, section.data) {
				continue
			}
			active, ok := o.pendingPAT.activate()
			o.pendingPAT = nil
			if !ok || active.identity.transportStreamID != o.expected.transportStreamID ||
				active.identity.version != o.expected.patVersion ||
				!equalPSISections(active.sections, o.expected.patSections) {
				return false, true
			}
			program, ok := active.programs[o.expected.pmtPID]
			if !ok || program.programNumber != o.expected.programNumber {
				return false, true
			}
		}
	case o.expected.pmtPID:
		sections := o.pmtAssembler.push(packet, 0)
		if o.pmtAssembler.lastInvalid || o.pmtAssembler.lastAbandoned {
			return false, true
		}
		for _, section := range sections {
			parsed, ok := parsePMTForCodecSection(section.data, "")
			if !ok && validInactivePMTSection(section.data) {
				continue
			}
			if !ok || parsed.programNumber != o.expected.programNumber ||
				!bytes.Equal(section.data, o.expected.pmtSection) {
				return false, true
			}
			o.pmtValidated = true
			o.revalidating = false
		}
	}
	return o.revalidating || len(o.patAssembler.buffer) > 0 || o.pendingPAT != nil ||
		len(o.pmtAssembler.buffer) > 0, false
}

func (o *selectedProgramMapObserver) failure() error {
	if o.lastFailure != nil {
		return o.lastFailure
	}
	return errTransportAttemptBoundary
}

func psiPUSIStartsMalformed(packet []byte) bool {
	if len(packet) != tsPacketSize || packet[1]&0x40 == 0 {
		return false
	}
	payload, ok := tsPayload(packet)
	if !ok || len(payload) == 0 {
		return true
	}
	pointer := int(payload[0])
	if pointer > len(payload)-1 {
		return true
	}
	section := payload[1+pointer:]
	if len(section) == 0 {
		return true
	}
	if section[0] == 0xff {
		return true
	}
	if len(section) < 3 {
		return false
	}
	sectionLength := int(section[1]&0x0f)<<8 | int(section[2])
	return sectionLength < 4 || sectionLength > maxPSISectionLength
}

func validInactivePMTSection(section []byte) bool {
	if len(section) < 12 || section[0] != 0x02 || section[5]&0x01 != 0 ||
		!mpeg2PSICRCValid(section) {
		return false
	}
	// Reuse the strict structural parser after changing only current_next and
	// recomputing the test copy's CRC. The inactive table remains unpublished
	// map state; validating the copy merely distinguishes it from malformed PSI.
	current := append([]byte(nil), section...)
	current[5] |= 0x01
	rewriteMPEG2PSICRC(current)
	_, ok := parsePMTForCodecSection(current, "")
	return ok
}

func equalPSISections(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !bytes.Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

type pcrClockScanner struct {
	buffer       []byte
	synced       bool
	havePCR      bool
	havePCRPID   bool
	pcrPID       int
	lastRaw      uint64
	clock        uint64
	forceReset   bool
	pendingReset map[int]struct{}
	resetCount   uint64
	// One-shot, armed by the sibling boundary scanner after a bounded
	// transport resync: the discontinuity markers it re-inserts declare a gap
	// in the SAME clock, not a new epoch. The next PCR is judged by ordinary
	// continuity (a backward step still resets); the pacer's arrival-vs-media
	// skew check bounds how large a forward jump is plausible.
	resyncContinuations int
}

func (s *pcrClockScanner) noteResyncContinuation() {
	s.resyncContinuations++
}

func (s *pcrClockScanner) selectPID(pid int) {
	*s = pcrClockScanner{}
	if pid >= 0 && pid < 0x1fff {
		s.havePCRPID = true
		s.pcrPID = pid
	}
}

// feed accepts arbitrarily split MPEG-TS and returns the first unwrapped PCR
// clock in this batch. The boolean reset marks a declared discontinuity or an
// implausible small backward step; normal 42-bit PCR wrap remains continuous.
func (s *pcrClockScanner) feed(data []byte) (clock uint64, observed, reset bool) {
	if !s.havePCRPID {
		return 0, false, false
	}
	s.buffer = append(s.buffer, data...)
	for {
		if !s.synced {
			offset, ok := findTSSyncOffset(s.buffer)
			if !ok {
				if len(s.buffer) > tsPacketSize*transportSyncPacketCount {
					keep := tsPacketSize*transportSyncPacketCount - 1
					copy(s.buffer, s.buffer[len(s.buffer)-keep:])
					s.buffer = s.buffer[:keep]
				}
				return s.clock, false, reset
			}
			if offset > 0 {
				copy(s.buffer, s.buffer[offset:])
				s.buffer = s.buffer[:len(s.buffer)-offset]
			}
			s.synced = true
		}
		consumed := 0
		for consumed+tsPacketSize <= len(s.buffer) {
			packet := s.buffer[consumed : consumed+tsPacketSize]
			if packet[0] != 0x47 {
				consumed++
				s.synced = false
				s.forceReset = true
				break
			}
			if !tsPacketStructureValid(packet) {
				// The publication/input safety observers attribute malformed
				// selected transport. The pacing scanner must independently refuse
				// to derive a clock or boundary from the same invalid adaptation.
				consumed += tsPacketSize
				continue
			}
			adaptationControl := (packet[3] >> 4) & 0x03
			pid := int(packet[1]&0x1f)<<8 | int(packet[2])
			if (adaptationControl == 2 || adaptationControl == 3) && packet[4] >= 1 && packet[5]&0x80 != 0 {
				if s.pendingReset == nil {
					s.pendingReset = make(map[int]struct{})
				}
				s.pendingReset[pid] = struct{}{}
			}
			if (adaptationControl == 2 || adaptationControl == 3) && packet[4] >= 7 && packet[5]&0x10 != 0 {
				if pid == s.pcrPID {
					raw := decodePCR(packet[6:12])
					_, pendingReset := s.pendingReset[pid]
					declaredReset := pendingReset || s.forceReset
					continuationPCR := false
					if s.resyncContinuations > 0 && declaredReset {
						// Only a marked post-resync PCR carries an edge; a
						// pre-loss PCR emitted from the same call must not
						// consume a continuation. Without an observed clock the
						// ordinary first-PCR path establishes the origin and
						// the edge is resolved by consuming the continuation.
						declaredReset = false
						continuationPCR = true
						s.forceReset = false
						s.resyncContinuations--
					}
					delete(s.pendingReset, pid)
					thisReset := false
					if !s.havePCR || declaredReset {
						reset = s.havePCR || declaredReset
						thisReset = reset
						s.havePCR = true
						s.lastRaw = raw
						s.clock = 0
						s.forceReset = false
					} else if raw >= s.lastRaw {
						s.clock += raw - s.lastRaw
						s.lastRaw = raw
					} else if s.lastRaw-raw > pcrModulus/2 {
						s.clock += pcrModulus - s.lastRaw + raw
						s.lastRaw = raw
					} else if continuationPCR {
						// The provider's clock restarted behind the old one at
						// the resync edge. The transform already declared the
						// boundary on every selected PID, so rebase silently;
						// the pacer re-origins on its own resync one-shot
						// instead of failing the repaired attempt.
						s.clock = 0
						s.lastRaw = raw
					} else {
						reset = true
						thisReset = true
						s.clock = 0
						s.lastRaw = raw
					}
					// Fixed-size VBR chunks can span very different media
					// durations. Schedule from the first PCR in each chunk so a
					// long low-bitrate batch is available before the prior client
					// buffer runs dry; the scanner still consumes every PCR to
					// preserve wrap and discontinuity state for the next call.
					if !observed || thisReset || continuationPCR {
						// A resync edge must surface as THIS batch's clock so
						// the pacer re-origins in the same call that emits it.
						observed = true
						clock = s.clock
					}
					if thisReset {
						s.resetCount++
					}
				}
			}
			consumed += tsPacketSize
		}
		if consumed > 0 {
			copy(s.buffer, s.buffer[consumed:])
			s.buffer = s.buffer[:len(s.buffer)-consumed]
		}
		if s.synced {
			if reset {
				return clock, observed, true
			}
			return clock, observed, false
		}
	}
}

func decodePCR(field []byte) uint64 {
	base := uint64(field[0])<<25 |
		uint64(field[1])<<17 |
		uint64(field[2])<<9 |
		uint64(field[3])<<1 |
		uint64(field[4]>>7)
	extension := uint64(field[4]&0x01)<<8 | uint64(field[5])
	return base*300 + extension
}

type transportPCRPacer struct {
	scanner      pcrClockScanner
	boundaries   selectedProgramBoundaryScanner
	finiteReplay bool
	haveOrigin   bool
	originClock  uint64
	originWall   time.Time
	haveLast     bool
	lastClock    uint64
	lastArrival  time.Time
	lastEmission time.Time
	lastPCREmit  time.Time
	recoveryRate int
	pendingRate  int
	// One-shot, armed with the scanner's resync continuation: the capture-proven
	// real loss carries a large forward PCR leap (a provider encoder epoch), and
	// the resync transform has already re-marked every selected PID with
	// decoder-visible discontinuities — the same declaration the startup gate
	// makes for a fresh attempt. A post-resync forward leap therefore re-origins
	// pacing in place instead of failing the attempt; a backward clock still
	// resets and remains an attempt boundary.
	resyncEpochsPending int
	// A live-edge skip episode is open: stale batches are being discarded and
	// the marker/re-origin machinery is armed for the first post-skip chunk.
	skipEpisode bool
	// One-shot: the first chunk of a shared-arrival batch shows an advanced
	// arrival (relative to the chunk before the batch) that is not a source
	// edge; without this the epoch PrepareSharedArrivalBatch just opened exits
	// immediately and the rest of the batch runs unaccelerated into the cap.
	holdEdgeExitOnce bool
	chunksNoPCR      int
	chunkStep        time.Duration
	pendingStep      time.Duration
	// Every accelerated epoch is opened for a bounded amount of media: the
	// validated startup prefix, a shared-arrival batch, or a lead trim. Once
	// epochMedia reaches epochBudget the pacer returns to media cadence and the
	// rest of the reservoir stays queued as lead. pendingBudget carries the
	// prefix span from PrepareBufferedPrefix to the origin-establishing Wait.
	epochBudget   time.Duration
	epochMedia    time.Duration
	pendingBudget time.Duration
	trimEpoch     bool
	// backlogChunks is the reservoir depth reported by the read loop before
	// each Wait; it only decides when a lead trim opens.
	backlogChunks int
	now           func() time.Time
	wait          func(context.Context, time.Duration) error
}

// Configure binds pacing to the PCR_PID from the exact PAT/PMT program that
// ffprobe selected. A missing/invalid PCR PID disables pacing; Conductor never
// substitutes the first PCR-bearing PID from another MPTS program. Finite
// accepted responses deliberately retain full PCR pacing because their source
// arrival timestamps all describe an already-complete in-memory replay.
func (p *transportPCRPacer) Configure(pcrPID int, finiteReplay bool) {
	p.scanner.selectPID(pcrPID)
	p.boundaries.configure(avProgramPIDs{}, false)
	p.finiteReplay = finiteReplay
	p.haveOrigin = false
	p.haveLast = false
	p.lastEmission = time.Time{}
	p.lastPCREmit = time.Time{}
	p.recoveryRate = 0
	p.pendingRate = 0
	p.chunksNoPCR = 0
	p.chunkStep = 0
	p.pendingStep = 0
	p.holdEdgeExitOnce = false
	p.epochBudget = 0
	p.epochMedia = 0
	p.pendingBudget = 0
	p.trimEpoch = false
	p.backlogChunks = 0
}

// NoteBacklog records the reservoir depth (queued, not yet dequeued chunks)
// so Wait can open a lead trim above the high-water mark. Chunks, not bytes:
// the reservoir is bounded in chunks and the pacer's chunkStep already
// estimates media per chunk.
func (p *transportPCRPacer) NoteBacklog(chunks int) {
	p.backlogChunks = chunks
}

// ConfigureSelectedProgram binds the attempt-boundary observer to the exact
// startup-selected programme. Configure must be called first so all pacing and
// boundary state belongs to the same private media attempt.
func (p *transportPCRPacer) ConfigureSelectedProgram(program avProgramPIDs, ok bool) {
	p.boundaries.configure(program, ok)
}

// TransformForFanout inserts any selected PID's missing initial boundary and
// rejects later selected-program discontinuities. Callers must pace and publish
// the returned bytes, never the input slice, so the observer and decoder see
// the exact same packet sequence.
func (p *transportPCRPacer) TransformForFanout(data []byte) ([]byte, error) {
	return p.TransformForFanoutAt(data, time.Now())
}

func (p *transportPCRPacer) TransformForFanoutAt(data []byte, observedAt time.Time) ([]byte, error) {
	output, err := p.boundaries.transformAt(data, observedAt)
	if err != nil {
		metrics.RelayClockResets.Inc()
	}
	for p.boundaries.resyncCompletions > 0 {
		p.boundaries.resyncCompletions--
		p.scanner.noteResyncContinuation()
		p.resyncEpochsPending++
	}
	return output, err
}

// PrepareBufferedPrefix selects a bounded acceleration rate from the complete
// validated startup window before any of it reaches fanout. This lookahead is
// available only at the private media gate; it lets a multi-second prefix
// drain smoothly inside the live window instead of repeatedly clamping at one
// deadline or retrying an otherwise valid source forever. partSize is the
// publication partition, not the producer read size. The later boundary
// transform can still add selected-PID discontinuity packets to this prefix.
func (p *transportPCRPacer) PrepareBufferedPrefix(data []byte, partSize int) error {
	if p.finiteReplay || !p.scanner.havePCRPID {
		return nil
	}
	if p.boundaries.bound && partSize%tsPacketSize == 0 {
		if offset, aligned := findTSSyncOffset(data); aligned && offset == 0 {
			// This selected, aligned prefix has not reached the boundary
			// transform yet. Its incomplete final TS packet remains private
			// there, so it is not a separate publication part to profile.
			data = data[:len(data)-len(data)%tsPacketSize]
		}
	}
	step, span, rate, observed, bounded := bufferedPCRProfile(
		data, p.scanner.pcrPID, maxTransportAddedLatency-transportWakeReserve, partSize)
	if !bounded {
		metrics.RelayBacklogFailures.Inc()
		return errTransportRelayBacklog
	}
	if !observed {
		return nil
	}
	p.pendingStep = step
	// The accelerated startup epoch ends when the prefix's own media has been
	// emitted; media queued behind it is lead, not backlog to race through.
	p.pendingBudget = span
	if rate > 1 {
		p.pendingRate = rate
	}
	return nil
}

// PrepareSharedArrivalBatch bounds the schedule of a multi-chunk media slice
// that reaches a read loop under one shared source-arrival timestamp — a
// publication-gate release, or reservoir reads coalesced while the pacer
// waited out the previous chunk. Every chunk of such a batch shares one
// paceLimit, so pacing it at natural cadence exhausts the added-latency
// budget once the batch spans more media clock than the budget holds
// (measured: the 8th 150 ms-PCR chunk of a steady-state batch fails at
// exactly 975 ms), punishing the reservoir precisely for riding out a
// delivery pause. Profile the batch the way PrepareBufferedPrefix profiles
// the startup prefix and enter a bounded recovery epoch sized to fit the
// shared window. Batches no bounded rate can fit, and batches containing a
// clock reset, are deliberately left untouched: Wait already fails those at
// the exact chunk with the precise error, and pre-empting it here would
// relabel an attempt boundary as a rate backlog.
func (p *transportPCRPacer) PrepareSharedArrivalBatch(data []byte, partSize int) {
	if p.finiteReplay || !p.scanner.havePCRPID || !p.haveOrigin {
		// Startup prefixes keep PrepareBufferedPrefix's dedicated machinery.
		return
	}
	_, span, rate, observed, bounded := bufferedPCRProfile(
		data, p.scanner.pcrPID, maxTransportAddedLatency-transportWakeReserve, partSize)
	if !observed || !bounded || rate <= 1 {
		return
	}
	// Bound the epoch to this batch: once it is out, media cadence resumes.
	if p.recoveryRate > 0 {
		p.epochBudget += span
	} else {
		p.epochBudget = span
		p.epochMedia = 0
	}
	p.trimEpoch = false
	// The dry-run models the schedule from a fresh emission anchor while the
	// live pacer continues from lastEmission; the per-chunk paceLimit inside
	// Wait remains the enforced contract either way, so the profile only ever
	// errs toward a slightly faster drain. Never lower an active deeper
	// recovery epoch.
	if rate > p.recoveryRate {
		p.recoveryRate = rate
		metrics.RelayRecoveryEpochs.Inc()
	}
	p.holdEdgeExitOnce = true
}

// beginSkipEpisode arms live-edge re-entry once per episode: fresh
// discontinuity markers for every selected PID, and the same marked-PCR
// continuation + re-origin one-shots the resync path uses. PSI is intact in a
// skip (no bytes were corrupted, only dropped), so no PAT/PMT revalidation.
// ConsumeStaleBatch keeps the attempt's observers coherent across a batch the
// read loop discards during a skip episode: PAT/PMT continuity and the sync
// state advance through the boundary scanner in discard mode (no emission, no
// marker consumption), and the PCR scanner walks the clock so the eventual
// live-edge step stays small. Counted per 64 KiB chunk in the skip metric.
func (p *transportPCRPacer) ConsumeStaleBatch(data []byte) {
	if len(data) == 0 {
		return
	}
	p.boundaries.discardOutput = true
	_, _ = p.boundaries.transformAt(data, p.wallNow())
	p.boundaries.discardOutput = false
	p.scanner.feed(data)
	metrics.RelaySkipAheadChunks.Add(uint64((len(data) + chunkSize - 1) / chunkSize))
}

func (p *transportPCRPacer) beginSkipEpisode() {
	if p.skipEpisode {
		return
	}
	p.skipEpisode = true
	p.boundaries.clearSelectedActive()
	p.scanner.noteResyncContinuation()
	p.resyncEpochsPending++
	metrics.RelaySkipAheadEpisodes.Inc()
}

func (p *transportPCRPacer) wallNow() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *transportPCRPacer) waitFor(ctx context.Context, delay time.Duration) error {
	if p.wait != nil {
		return p.wait(ctx, delay)
	}
	return waitTransportDelay(ctx, delay)
}

func waitTransportDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Wait keeps one wall-clock/PCR mapping for the entire attempt, including the
// read-ahead tail accumulated during startup validation. For live input, a PCR
// target may add at most maxTransportAddedLatency after this chunk became
// available at the source edge. That preserves bounded burst smoothing while
// preventing a consistently fast PCR clock from accumulating minutes of relay
// backlog. Finite accepted replay is exempt and drains at its full PCR cadence.
func (p *transportPCRPacer) Wait(ctx context.Context, data []byte, arrivedAt time.Time) (retErr error) {
	clock, observed, reset := p.scanner.feed(data)
	now := p.wallNow()
	if arrivedAt.IsZero() || arrivedAt.After(now) {
		arrivedAt = now
	}
	if err := p.rejectStaleLiveChunk(arrivedAt, now); err != nil {
		return err
	}
	if !p.finiteReplay {
		// Recheck immediately before every successful return. This covers
		// scheduler pauses in non-wait branches; the wait branch also checks
		// before recording an emission so rejected media cannot advance state.
		defer func() {
			if retErr == nil {
				retErr = p.rejectStaleLiveChunk(arrivedAt, p.wallNow())
			}
		}()
	}
	if !observed {
		if !p.haveOrigin && p.pendingStep > 0 {
			p.chunkStep = p.pendingStep
			p.pendingStep = 0
			p.recoveryRate = p.pendingRate
			p.pendingRate = 0
			if p.recoveryRate > 0 {
				metrics.RelayRecoveryEpochs.Inc()
			}
		}
		if !p.haveOrigin && p.chunkStep > 0 {
			p.chunksNoPCR++
			if p.lastEmission.IsZero() {
				p.lastEmission = now
				return nil
			}
			return p.waitEstimatedChunk(ctx, arrivedAt, now)
		}
		if p.haveOrigin {
			p.chunksNoPCR++
			if p.chunkStep > 0 {
				return p.waitEstimatedChunk(ctx, arrivedAt, now)
			}
		}
		return nil
	}
	if p.resyncEpochsPending > 0 && observed && p.scanner.resyncContinuations == 0 && p.haveOrigin {
		// The marked resync edge reached the pacer: restart the wall/PCR
		// mapping here regardless of how the new clock relates to the old —
		// forward leap, rebase behind the origin, or benign continuation. The
		// transform already declared the boundary on every selected PID. All
		// pending edges collapse into this final surfaced clock — the scanner
		// already applied each intermediate edge's continuity internally.
		p.resyncEpochsPending = 0
		p.skipEpisode = false
		p.haveLast = true
		p.lastClock = clock
		p.lastArrival = arrivedAt
		p.originClock = clock
		p.originWall = now
		p.lastEmission = now
		p.lastPCREmit = now
		p.recoveryRate = 0
		p.chunksNoPCR = 0
		metrics.RelayClockResets.Inc()
		return nil
	}
	if p.haveOrigin && clock < p.originClock {
		metrics.RelayClockResets.Inc()
		return errTransportAttemptBoundary
	}
	if !p.haveOrigin {
		preOriginPaced := !p.haveOrigin && !p.lastEmission.IsZero() && p.chunkStep > 0
		if preOriginPaced {
			if err := p.waitEstimatedChunk(ctx, arrivedAt, now); err != nil {
				return err
			}
			now = p.lastEmission
		}
		p.haveOrigin = true
		p.resyncEpochsPending = 0
		p.originClock = clock
		p.originWall = now
		p.haveLast = true
		p.lastClock = clock
		p.lastArrival = arrivedAt
		p.lastEmission = now
		p.lastPCREmit = now
		if !preOriginPaced {
			p.recoveryRate = p.pendingRate
		}
		p.pendingRate = 0
		p.chunksNoPCR = 0
		if !preOriginPaced && p.pendingStep > 0 {
			p.chunkStep = p.pendingStep
		}
		p.pendingStep = 0
		p.epochBudget = p.pendingBudget
		p.epochMedia = 0
		p.trimEpoch = false
		p.pendingBudget = 0
		if p.recoveryRate > 0 && !preOriginPaced {
			metrics.RelayRecoveryEpochs.Inc()
		}
		// The startup gate has already placed decoder-visible discontinuity
		// markers before every PID's first published payload. An initial PCR
		// carrying that declaration establishes this attempt's clock origin; it
		// is not a second attempt boundary.
		if reset {
			metrics.RelayClockResets.Inc()
		}
		return nil
	}
	if reset {
		// Never publish a mixed old/new clock chunk. In particular, rewriting the
		// first packet of the whole chunk can mark A/V payload that precedes a
		// mid-chunk PCR reset while leaving the new epoch unmarked. Reopen the same
		// source through the private startup gate, which validates PAT/PMT/random
		// access and places boundaries before every selected payload.
		metrics.RelayClockResets.Inc()
		return errTransportAttemptBoundary
	}
	// Source arrival is a useful corruption cross-check only for a live body.
	// A finite accepted replay is already buffered, so sparse/VBR PCR samples
	// legitimately arrive together while still requiring their full cadence.
	mediaStep := time.Duration(0)
	callMediaStep := time.Duration(0)
	arrivalStep := time.Duration(0)
	if p.haveLast && clock >= p.lastClock {
		mediaStep = pcrTicksDuration(clock - p.lastClock)
		callMediaStep = mediaStep
		if mediaStep > 0 {
			callMediaStep = divideDurationCeil(mediaStep, p.chunksNoPCR+1)
			p.chunkStep = callMediaStep
		}
		p.chunksNoPCR = 0
		if arrivedAt.After(p.lastArrival) {
			arrivalStep = arrivedAt.Sub(p.lastArrival)
		}
		if !p.finiteReplay && mediaStep > arrivalStep+maxTransportPCRStepSkew {
			// A single multi-second PCR leap is corrupt, not clock-rate drift.
			// Reject this chunk before fanout and re-enter through the validated
			// startup gate; synthesizing a marker in-place cannot safely cover a
			// payload-only PID or a boundary in the middle of this slice.
			metrics.RelayClockResets.Inc()
			return errTransportAttemptBoundary
		}
	}
	p.haveLast = true
	p.lastClock = clock
	p.lastArrival = arrivedAt
	mediaElapsed := pcrTicksDuration(clock - p.originClock)
	target := p.originWall.Add(mediaElapsed)
	delay := target.Sub(now)
	recoveryAcceleratedBy := time.Duration(0)
	exitRecovery := false
	if !p.finiteReplay && delay < -maxTransportPaceBehind {
		// The source paused for longer than the queued lead covered and its
		// media now arrives behind the wall/PCR schedule. Re-anchor the schedule
		// here and keep pacing at media rate: whatever the source returns after
		// the pause stays queued in the reservoir as lead for the next pause,
		// and the viewer falls behind live by exactly this pause. The previous
		// policy compressed the debt at up to 16x to rejoin the live edge and
		// discarded backlogs it could not compress, which emptied the reservoir
		// at the end of every pause and left the HLS client with only the ~3s
		// it holds itself. Measured 2026-09-02 on ABC ch2: 58 output
		// hiccups in 225s under that policy; a client 20s behind the edge would
		// have starved zero times. Owner ruling 2026-09-01: margin over the
		// live edge.
		p.recoveryRate = 0
		p.epochBudget = 0
		p.epochMedia = 0
		p.trimEpoch = false
		p.originClock = clock
		p.originWall = now
		p.lastEmission = now
		p.lastPCREmit = now
		metrics.RelayClockResets.Inc()
		metrics.RelayUnderflowReanchors.Inc()
		return nil
	}
	if !p.finiteReplay && p.recoveryRate == 0 && p.chunkStep > 0 &&
		p.backlogChunks > leadTrimHighWaterChunks {
		// A source clock running ahead of wall time grows the retained lead
		// without bound; trim it before the reservoir's consumer-stall timeout
		// turns a full queue into a failed attempt. The trim ends when the
		// observed backlog reaches the low-water mark; the media budget is only
		// a hard bound, sized for a producer that keeps adding at media rate
		// while the epoch drains at leadTrimRate (review finding).
		p.recoveryRate = leadTrimRate
		p.epochBudget = time.Duration(leadTrimRate*(p.backlogChunks-leadTrimLowWaterChunks)) * p.chunkStep
		p.epochMedia = 0
		p.trimEpoch = true
		metrics.RelayLeadTrimEpochs.Inc()
	}
	if !p.finiteReplay && p.recoveryRate > 0 {
		// Exit only after advancing source arrivals prove the accelerated relay
		// has reached the source edge. A small arrival step from the original
		// catch-up burst is not enough; it must be at least half the media step.
		atSourceEdge := mediaStep > 0 && arrivalStep > 0 &&
			arrivalStep*2 >= mediaStep && now.Sub(arrivedAt) <= transportRecoveryEdgeLag
		if p.holdEdgeExitOnce || p.trimEpoch {
			// A lead trim dequeues from a deep reservoir, so its own paced
			// cadence always looks like a source edge; only its budget ends it.
			atSourceEdge = false
			p.holdEdgeExitOnce = false
		}
		// Every accelerated epoch is bounded by the media it was opened for.
		// Once that media is out, return to media cadence and keep the rest of
		// the reservoir as lead instead of draining it to the source edge.
		p.epochMedia += mediaStep
		if p.epochBudget > 0 && p.epochMedia >= p.epochBudget {
			atSourceEdge = true
		}
		if p.trimEpoch && p.backlogChunks <= leadTrimLowWaterChunks {
			atSourceEdge = true
		}
		if callMediaStep > 0 {
			pacedStep := divideDurationCeil(callMediaStep, p.recoveryRate)
			recoveryAcceleratedBy = callMediaStep - pacedStep
			target = p.lastEmission.Add(pacedStep)
		}
		if mediaStep > 0 && !p.lastPCREmit.IsZero() {
			pcrTarget := p.lastPCREmit.Add(divideDurationCeil(mediaStep, p.recoveryRate))
			if target.Before(pcrTarget) {
				target = pcrTarget
			}
		}
		if atSourceEdge {
			// The edge chunk still belongs to the recovery sequence. Give it one
			// final positive accelerated step, then re-anchor normal cadence only
			// after that emission succeeds. Returning immediately here would make
			// the edge transition a zero-interval burst.
			exitRecovery = true
		}
		if callMediaStep > 0 {
			arrivalLimit := arrivedAt.Add(maxTransportAddedLatency)
			paceLimit := arrivalLimit.Add(-transportWakeReserve)
			if target.After(paceLimit) {
				metrics.RelayPacingLatencyCaps.Inc()
				metrics.RelayPacingCappedMillis.Add(uint64(target.Sub(paceLimit) / time.Millisecond))
				p.beginSkipEpisode()
				return errTransportSkipStale
			}
		}
	} else if !p.finiteReplay {
		arrivalLimit := arrivedAt.Add(maxTransportAddedLatency)
		paceLimit := arrivalLimit.Add(-transportWakeReserve)
		minimumTarget := p.lastEmission
		if callMediaStep > 0 {
			minimumTarget = p.lastEmission.Add(divideDurationCeil(callMediaStep, maxTransportRecoveryRate))
			if target.Before(minimumTarget) {
				target = minimumTarget
			}
		}
		if mediaStep > 0 && !p.lastPCREmit.IsZero() {
			pcrTarget := p.lastPCREmit.Add(divideDurationCeil(mediaStep, maxTransportRecoveryRate))
			if minimumTarget.Before(pcrTarget) {
				minimumTarget = pcrTarget
			}
			if target.Before(minimumTarget) {
				target = minimumTarget
			}
		}
		if target.After(paceLimit) {
			cappedBy := target.Sub(paceLimit)
			metrics.RelayPacingLatencyCaps.Inc()
			metrics.RelayPacingCappedMillis.Add(uint64(cappedBy / time.Millisecond))
			if minimumTarget.After(paceLimit) {
				// Even the maximum recovery rate cannot preserve a positive
				// media-clock interval inside this chunk's live deadline.
				p.beginSkipEpisode()
				return errTransportSkipStale
			}
			target = paceLimit
			// Keep the mapping itself bounded too. Once the cap is reached,
			// subsequent normally-spaced arrivals retain their source cadence.
			p.originClock = clock
			p.originWall = target
		}
	}
	delay = target.Sub(now)
	if delay <= 0 {
		p.lastEmission = now
		p.lastPCREmit = now
		if exitRecovery {
			p.recoveryRate = 0
			p.epochBudget = 0
			p.epochMedia = 0
			p.trimEpoch = false
			p.originClock = clock
			p.originWall = now
		}
		if recoveryAcceleratedBy > 0 {
			metrics.RelayRecoveryAcceleratedMS.Add(uint64(recoveryAcceleratedBy / time.Millisecond))
		}
		return nil
	}
	if err := p.waitFor(ctx, delay); err != nil {
		return err
	}
	emittedAt := p.wallNow()
	metrics.RelayPacingMilliseconds.Add(uint64(delay / time.Millisecond))
	if err := p.rejectStaleLiveChunk(arrivedAt, emittedAt); err != nil {
		return err
	}
	p.lastEmission = emittedAt
	p.lastPCREmit = emittedAt
	if exitRecovery {
		p.recoveryRate = 0
		p.epochBudget = 0
		p.epochMedia = 0
		p.trimEpoch = false
		p.originClock = clock
		p.originWall = emittedAt
	}
	if recoveryAcceleratedBy > 0 {
		metrics.RelayRecoveryAcceleratedMS.Add(uint64(recoveryAcceleratedBy / time.Millisecond))
	}
	return nil
}

func (p *transportPCRPacer) rejectStaleLiveChunk(arrivedAt, now time.Time) error {
	if p.finiteReplay {
		return nil
	}
	// The source-arrival limit is a fanout contract, not merely a PCR target
	// clamp. Enforce it before every live emission, including the first chunk
	// of a recovery epoch, chunks without PCR, and a timer that woke late.
	arrivalLimit := arrivedAt.Add(maxTransportAddedLatency)
	if !now.After(arrivalLimit) {
		return nil
	}
	metrics.RelayStaleChunkFailures.Inc()
	metrics.RelayStaleChunkMilliseconds.Add(uint64(now.Sub(arrivalLimit) / time.Millisecond))
	return errTransportStaleChunk
}

func (p *transportPCRPacer) rejectStaleBeforeFanout(arrivedAt time.Time) error {
	now := p.wallNow()
	if arrivedAt.IsZero() || arrivedAt.After(now) {
		arrivedAt = now
	}
	return p.rejectStaleLiveChunk(arrivedAt, now)
}

func (p *transportPCRPacer) waitEstimatedChunk(
	ctx context.Context, arrivedAt, now time.Time,
) error {
	rate := 1
	if p.recoveryRate > 0 {
		rate = p.recoveryRate
	}
	pacedStep := divideDurationCeil(p.chunkStep, rate)
	minimumStep := divideDurationCeil(p.chunkStep, maxTransportRecoveryRate)
	target := p.lastEmission.Add(pacedStep)
	minimumTarget := p.lastEmission.Add(minimumStep)
	if !p.finiteReplay {
		paceLimit := arrivedAt.Add(maxTransportAddedLatency - transportWakeReserve)
		if target.After(paceLimit) {
			metrics.RelayPacingLatencyCaps.Inc()
			metrics.RelayPacingCappedMillis.Add(uint64(target.Sub(paceLimit) / time.Millisecond))
			if minimumTarget.After(paceLimit) {
				p.beginSkipEpisode()
				return errTransportSkipStale
			}
			target = paceLimit
		}
	}
	delay := target.Sub(now)
	emittedAt := now
	if delay > 0 {
		if err := p.waitFor(ctx, delay); err != nil {
			return err
		}
		emittedAt = p.wallNow()
		metrics.RelayPacingMilliseconds.Add(uint64(delay / time.Millisecond))
	}
	if err := p.rejectStaleLiveChunk(arrivedAt, emittedAt); err != nil {
		return err
	}
	p.lastEmission = emittedAt
	if p.recoveryRate > 0 && p.chunkStep > pacedStep {
		metrics.RelayRecoveryAcceleratedMS.Add(uint64((p.chunkStep - pacedStep) / time.Millisecond))
	}
	return nil
}

type bufferedPCRChunk struct {
	clock    uint64
	observed bool
	reset    bool
}

// bufferedPCRProfile returns the per-chunk media step, the PCR span of the
// buffer, the smallest bounded acceleration rate that drains it inside window,
// whether any PCR was observed, and whether such a rate exists. partSize must
// match the caller's subsequent Wait partitions, including any sparse tail.
func bufferedPCRProfile(
	data []byte, pcrPID int, window time.Duration, partSize int,
) (time.Duration, time.Duration, int, bool, bool) {
	if partSize <= 0 {
		return 0, 0, 0, false, false
	}
	var scanner pcrClockScanner
	scanner.selectPID(pcrPID)
	chunks := make([]bufferedPCRChunk, 0, (len(data)+partSize-1)/partSize)
	firstChunk, lastChunk := -1, -1
	resetAfterFirstPCR := false
	for index, offset := 0, 0; offset < len(data); index++ {
		end := min(offset+partSize, len(data))
		clock, observed, reset := scanner.feed(data[offset:end])
		chunks = append(chunks, bufferedPCRChunk{clock: clock, observed: observed, reset: reset})
		if observed {
			if firstChunk < 0 {
				firstChunk = index
			} else if reset {
				resetAfterFirstPCR = true
			}
			lastChunk = index
		}
		offset = end
	}
	if firstChunk < 0 {
		if len(chunks) > 1 {
			return 0, 0, 0, false, false
		}
		return 0, 0, 1, false, true
	}
	// The gate marks one initial discontinuity on the selected program. More
	// than one reset means the private prefix spans multiple clock epochs; one
	// lookahead rate cannot describe it safely.
	if scanner.resetCount > 1 || resetAfterFirstPCR {
		return 0, 0, 0, true, false
	}
	firstClock, lastClock := chunks[firstChunk].clock, chunks[lastChunk].clock
	if lastClock < firstClock {
		return 0, 0, 0, true, false
	}
	pcrSpan := pcrTicksDuration(lastClock - firstClock)
	clockIntervals := lastChunk - firstChunk
	if clockIntervals <= 0 || pcrSpan <= 0 {
		if len(chunks) > 1 {
			return 0, 0, 0, true, false
		}
		return 0, 0, 1, true, true
	}
	step := divideDurationCeil(pcrSpan, clockIntervals)
	// The epoch budget covers the whole buffer from its first PCR: the clocked
	// span plus the trailing no-PCR chunks at their learned step, so the epoch
	// does not end (and hand those chunks to media-rate pacing against a shared
	// release arrival) before the prefix is out.
	span := pcrSpan + time.Duration(len(chunks)-1-lastChunk)*step
	// Dry-run the exact per-chunk schedule that Wait will apply after release.
	// This includes leading/trailing no-PCR chunks, per-call duration ceilings,
	// PCR-density changes, and the hard last-PCR emission anchor. A single
	// aggregate span can underestimate all four at a rate boundary.
	for rate := 1; rate <= maxTransportRecoveryRate; rate++ {
		if bufferedPrefixScheduleFits(chunks, step, rate, window) {
			return step, span, rate, true, true
		}
	}
	return 0, span, maxTransportRecoveryRate + 1, true, false
}

func bufferedPrefixScheduleFits(
	chunks []bufferedPCRChunk, initialStep time.Duration, rate int, window time.Duration,
) bool {
	if rate <= 0 || window <= 0 || initialStep < 0 {
		return false
	}
	var (
		elapsed, originElapsed, lastPCREmit time.Duration
		originClock, lastClock              uint64
		chunkStep                           = initialStep
		chunksNoPCR                         int
		haveEmission, haveOrigin            bool
	)
	advance := func(base, step time.Duration) (time.Duration, bool) {
		if step < 0 || base < 0 || base > window || step > window-base {
			return 0, false
		}
		return base + step, true
	}
	for _, chunk := range chunks {
		if !chunk.observed {
			chunksNoPCR++
			if chunkStep <= 0 {
				continue
			}
			if !haveEmission {
				haveEmission = true
				continue
			}
			var ok bool
			elapsed, ok = advance(elapsed, divideDurationCeil(chunkStep, rate))
			if !ok {
				return false
			}
			continue
		}
		if !haveOrigin {
			if haveEmission && chunkStep > 0 {
				var ok bool
				elapsed, ok = advance(elapsed, divideDurationCeil(chunkStep, rate))
				if !ok {
					return false
				}
			}
			haveEmission = true
			haveOrigin = true
			originElapsed = elapsed
			lastPCREmit = elapsed
			originClock = chunk.clock
			lastClock = chunk.clock
			chunksNoPCR = 0
			continue
		}
		if chunk.reset || chunk.clock < lastClock || chunk.clock < originClock {
			return false
		}
		mediaStep := pcrTicksDuration(chunk.clock - lastClock)
		mediaElapsed := pcrTicksDuration(chunk.clock - originClock)
		if mediaStep < 0 || mediaElapsed < 0 {
			return false
		}
		// All private-prefix chunks share one source-arrival timestamp. Wait
		// classifies a jump over this threshold as a corrupt clock epoch and
		// clears prepared recovery, so reject it before any prefix bytes fan out
		// instead of approving a schedule Wait will not actually follow.
		if mediaStep > maxTransportPCRStepSkew {
			return false
		}
		// Wait would open a new underflow epoch immediately at this point,
		// defeating the prepared smooth schedule. A faster candidate may avoid
		// that provisional-cadence overshoot.
		sinceOrigin := elapsed - originElapsed
		if sinceOrigin > mediaElapsed && sinceOrigin-mediaElapsed > maxTransportPaceBehind {
			return false
		}
		callStep := mediaStep
		if mediaStep > 0 {
			callStep = divideDurationCeil(mediaStep, chunksNoPCR+1)
			chunkStep = callStep
		}
		chunksNoPCR = 0
		target := elapsed
		if callStep > 0 {
			var ok bool
			target, ok = advance(elapsed, divideDurationCeil(callStep, rate))
			if !ok {
				return false
			}
		}
		if mediaStep > 0 {
			pcrTarget, ok := advance(lastPCREmit, divideDurationCeil(mediaStep, rate))
			if !ok {
				return false
			}
			if target < pcrTarget {
				target = pcrTarget
			}
		}
		elapsed = target
		lastPCREmit = target
		lastClock = chunk.clock
	}
	return elapsed <= window
}

func divideDurationCeil(d time.Duration, divisor int) time.Duration {
	if d <= 0 || divisor <= 1 {
		return d
	}
	return (d + time.Duration(divisor) - 1) / time.Duration(divisor)
}

func pcrTicksDuration(ticks uint64) time.Duration {
	seconds := ticks / transportClockRate
	remainder := ticks % transportClockRate
	return time.Duration(seconds)*time.Second +
		time.Duration(remainder*uint64(time.Second)/transportClockRate)
}
