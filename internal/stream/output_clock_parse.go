package stream

import (
	"bytes"
	"fmt"
)

// The caller supplies an original-order complete selected-PES publication
// group. This parser neither reassembles arbitrary read fragments nor decides
// whether an interrupted zero-length PES is complete.
type outputClockPacket struct {
	raw, translated [tsPacketSize]byte
	have            bool
}

type outputClockNative struct {
	packets                                     map[int]outputClockPacket
	haveVideo, haveAudio, havePCR               bool
	videoDTS, videoPTS, audioPTS, audioEnd, pcr int64
	cadence                                     int64
	videoSteps                                  [64]int64
	videoStepIndex                              int
}

type outputClockPatch struct {
	positions []int
	raw       uint64
	pcr       bool
}

type outputClockDuplicate struct {
	destination, source int
	previous            [tsPacketSize]byte
}

type outputClockGroup struct {
	patches                                     []outputClockPatch
	duplicates                                  []outputClockDuplicate
	native                                      outputClockNative
	haveVideo, haveAudio, havePCR               bool
	firstDTS, firstPTS, minPTS, maxPTS, lastDTS int64
	firstAudio, audioEnd, audioStep             int64
	firstPCR, lastPCR                           int64
	lastPacketPositions                         map[int]int
}

type outputClockPES struct {
	pid             int
	data            []byte
	headerPositions []int
	payloadBytes    int
}

func outputClockParseError(reason string) error {
	return fmt.Errorf("%w: %s", errOutputClockBoundary, reason)
}

func unwrapOutputClock(raw uint64, reference, modulus int64) int64 {
	v := int64(raw)
	d := (v - reference) % modulus
	if d > modulus/2 {
		d -= modulus
	}
	if d < -modulus/2 {
		d += modulus
	}
	return reference + d
}

func copyOutputClockNative(old outputClockNative) outputClockNative {
	n := old
	n.packets = make(map[int]outputClockPacket, len(old.packets))
	for pid, p := range old.packets {
		n.packets[pid] = p
	}
	return n
}

func inspectOutputClockGroup(data []byte, program avProgramPIDs, previous outputClockNative) (outputClockGroup, error) {
	g := outputClockGroup{native: copyOutputClockNative(previous), lastPacketPositions: make(map[int]int)}
	if len(data) == 0 || len(data)%tsPacketSize != 0 || len(data) > maxPESPublicationBytes {
		return g, outputClockParseError("invalid complete group size")
	}
	active := make(map[int]*outputClockPES, 2)
	var order []*outputClockPES
	var reference int64
	haveReference := false
	if previous.haveVideo {
		reference = previous.videoDTS
		haveReference = true
	} else if previous.havePCR {
		reference = previous.pcr / 300
		haveReference = true
	} else if previous.haveAudio {
		reference = previous.audioPTS
		haveReference = true
	}
	for off := 0; off < len(data); off += tsPacketSize {
		packet := data[off : off+tsPacketSize]
		if packet[0] != 0x47 {
			return g, outputClockParseError("invalid transport alignment")
		}
		pid := int(packet[1]&31)<<8 | int(packet[2])
		selected := pid == program.videoPID || pid == program.firstAudioPID() || pid == program.pcrPID
		if !selected {
			continue
		}
		if !tsPacketStructureValid(packet) || packet[1]&0x80 != 0 || packet[3]&0xc0 != 0 {
			return g, outputClockParseError("invalid selected transport packet")
		}
		payload, hasPayload := tsPayloadStart(packet)
		prior := g.native.packets[pid]
		duplicate := hasPayload && prior.have && bytes.Equal(prior.raw[:], packet)
		if duplicate {
			d := outputClockDuplicate{destination: off, source: -1, previous: prior.translated}
			if pos, ok := g.lastPacketPositions[pid]; ok {
				d.source = pos
			}
			g.duplicates = append(g.duplicates, d)
			continue
		}
		adaptation := (packet[3] >> 4) & 3
		if adaptation == 2 || adaptation == 3 {
			if packet[4] > 0 && packet[5]&0x80 != 0 && (prior.have || pid == program.pcrPID && g.native.havePCR) {
				return g, outputClockParseError("new selected discontinuity inside output epoch")
			}
			field := 6
			for _, flag := range []byte{0x10, 0x08} {
				if packet[4] == 0 || packet[5]&flag == 0 {
					continue
				}
				if field+6 > 5+int(packet[4]) || packet[field+4]&0x7e != 0x7e || (int(packet[field+4]&1)<<8|int(packet[field+5])) >= 300 {
					return g, outputClockParseError("invalid PCR/OPCR field")
				}
				raw := decodePCR(packet[field : field+6])
				positions := []int{off + field, off + field + 1, off + field + 2, off + field + 3, off + field + 4, off + field + 5}
				g.patches = append(g.patches, outputClockPatch{positions: positions, raw: raw, pcr: true})
				if flag == 0x10 && pid == program.pcrPID {
					if !haveReference {
						reference = int64(raw / 300)
						haveReference = true
					}
					ref := reference * 300
					if g.native.havePCR {
						ref = g.native.pcr
					}
					clock := unwrapOutputClock(raw, ref, int64(pcrModulus))
					if g.native.havePCR && clock < g.native.pcr {
						return g, outputClockParseError("PCR reversed inside output epoch")
					}
					if !g.havePCR {
						g.firstPCR = clock
					}
					g.havePCR = true
					g.lastPCR = clock
					g.native.havePCR = true
					g.native.pcr = clock
				}
				field += 6
			}
		}
		if !hasPayload {
			continue
		}
		copy(prior.raw[:], packet)
		prior.have = true
		g.native.packets[pid] = prior
		g.lastPacketPositions[pid] = off
		if pid != program.videoPID && pid != program.firstAudioPID() {
			continue
		}
		pes := active[pid]
		if packet[1]&0x40 != 0 {
			pes = &outputClockPES{pid: pid}
			active[pid] = pes
			order = append(order, pes)
		} else if pes == nil {
			return g, outputClockParseError("selected payload lacks complete PES start")
		}
		pes.payloadBytes += len(packet) - payload
		keep := len(packet) - payload
		if pid == program.videoPID {
			keep = min(keep, 264-len(pes.data))
		}
		pes.data = append(pes.data, packet[payload:payload+keep]...)
		for i := payload; i < len(packet) && len(pes.headerPositions) < 19; i++ {
			pes.headerPositions = append(pes.headerPositions, off+i)
		}
	}
	for _, pes := range order {
		b := pes.data
		if len(b) < 9 || !bytes.Equal(b[:3], []byte{0, 0, 1}) || !pesHasOptionalHeader(b[3]) || b[6]&0xc0 != 0x80 {
			return g, outputClockParseError("invalid complete PES header")
		}
		if declared := int(b[4])<<8 | int(b[5]); declared > 0 {
			if pes.payloadBytes < declared+6 {
				return g, outputClockParseError("incomplete declared PES")
			}
			b = b[:min(len(b), declared+6)]
		}
		if len(b) < 9 {
			return g, outputClockParseError("short declared PES header")
		}
		header := 9 + int(b[8])
		flags := b[7] >> 6
		if header > len(b) || flags == 1 {
			return g, outputClockParseError("invalid optional PES header")
		}
		var pts, dts int64
		havePTS := flags == 2 || flags == 3
		if havePTS {
			need := 14
			if flags == 3 {
				need = 19
			}
			if header < need || len(pes.headerPositions) < need {
				return g, outputClockParseError("incomplete PES timestamps")
			}
			if b[9]>>4 != flags || b[9]&1 == 0 || b[11]&1 == 0 || b[13]&1 == 0 {
				return g, outputClockParseError("invalid PTS markers")
			}
			raw := decodeMPEGTimestamp(b[9:14])
			if !haveReference {
				reference = int64(raw)
				haveReference = true
			}
			pts = unwrapOutputClock(raw, reference, int64(ptsModulus))
			g.patches = append(g.patches, outputClockPatch{positions: append([]int(nil), pes.headerPositions[9:14]...), raw: raw})
			dts = pts
			if flags == 3 {
				if b[14]>>4 != 1 || b[14]&1 == 0 || b[16]&1 == 0 || b[18]&1 == 0 {
					return g, outputClockParseError("invalid DTS markers")
				}
				raw = decodeMPEGTimestamp(b[14:19])
				dts = unwrapOutputClock(raw, pts, int64(ptsModulus))
				g.patches = append(g.patches, outputClockPatch{positions: append([]int(nil), pes.headerPositions[14:19]...), raw: raw})
			}
		}
		if pes.pid == program.videoPID {
			if !havePTS {
				return g, outputClockParseError("video PES lacks a provable presentation clock")
			}
			if g.native.haveVideo {
				dts = unwrapOutputClock(uint64((dts%int64(ptsModulus)+int64(ptsModulus))%int64(ptsModulus)), g.native.videoDTS, int64(ptsModulus))
				pts = unwrapOutputClock(decodeMPEGTimestamp(b[9:14]), dts, int64(ptsModulus))
				step := dts - g.native.videoDTS
				if step < 0 {
					return g, outputClockParseError("video DTS reversed inside output epoch")
				}
				if step > 0 {
					g.native.videoSteps[g.native.videoStepIndex] = step
					g.native.videoStepIndex = (g.native.videoStepIndex + 1) % len(g.native.videoSteps)
					g.native.cadence = observedOutputClockCadence(g.native.videoSteps[:])
				}
			}
			if !g.haveVideo {
				g.firstDTS = dts
				g.firstPTS = pts
				g.minPTS = pts
				g.maxPTS = pts
			}
			g.haveVideo = true
			g.lastDTS = dts
			g.maxPTS = max(g.maxPTS, pts)
			g.minPTS = min(g.minPTS, pts)
			g.native.haveVideo = true
			g.native.videoDTS = dts
			g.native.videoPTS = pts
			reference = dts
		} else {
			if !havePTS {
				if !g.native.haveAudio {
					return g, outputClockParseError("AAC epoch lacks PTS")
				}
				pts = g.native.audioEnd
			} else if g.native.haveAudio {
				pts = unwrapOutputClock(decodeMPEGTimestamp(b[9:14]), g.native.audioPTS, int64(ptsModulus))
			}
			duration, step, err := completeADTSDuration(b[header:])
			if err != nil {
				return g, err
			}
			if !g.haveAudio {
				g.firstAudio = pts
				g.audioEnd = pts + duration
			}
			g.haveAudio = true
			g.audioEnd = max(g.audioEnd, pts+duration)
			g.audioStep = max(g.audioStep, step)
			g.native.haveAudio = true
			g.native.audioPTS = pts
			g.native.audioEnd = pts + duration
		}
	}
	return g, nil
}

// Infer a bounded regular-interval envelope from delivered/previewed decode
// clocks. Taking the minimum understates ordinary16/17ms TS quantization;
// taking the unconditional maximum would grant a source clock jump seconds
// of cadence credit. This is an observed interval estimate, not codec proof
// of the final picture's duration or of multiple pictures within one PES.
func observedOutputClockCadence(steps []int64) int64 {
	var minimum int64
	for _, step := range steps {
		if step > 0 && (minimum == 0 || step < minimum) {
			minimum = step
		}
	}
	var cadence int64
	for _, step := range steps {
		if step > cadence && step <= minimum*2 {
			cadence = step
		}
	}
	return cadence
}

func completeADTSDuration(data []byte) (duration, step int64, err error) {
	rates := [...]int64{96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350}
	var samples, rate int64
	for len(data) > 0 {
		if len(data) < 7 || data[0] != 0xff || data[1]&0xf6 != 0xf0 || data[2]>>6 != 1 {
			return 0, 0, outputClockParseError("AAC is not complete ADTS LC")
		}
		index := int(data[2] >> 2 & 15)
		if index >= len(rates) {
			return 0, 0, outputClockParseError("unknown ADTS sample rate")
		}
		if rate != 0 && rate != rates[index] {
			return 0, 0, outputClockParseError("ADTS rate changes within PES")
		}
		rate = rates[index]
		n := int(data[3]&3)<<11 | int(data[4])<<3 | int(data[5]>>5)
		header := 7
		if data[1]&1 == 0 {
			header = 9
		}
		if n < header || n > len(data) {
			return 0, 0, outputClockParseError("incomplete ADTS frame")
		}
		frameSamples := int64(1024) * (int64(data[6]&3) + 1)
		samples += frameSamples
		step = max(step, (frameSamples*int64(transportPTSRate)+rate-1)/rate)
		data = data[n:]
	}
	if samples == 0 {
		return 0, 0, outputClockParseError("AAC PES contains no complete frame")
	}
	return (samples*int64(transportPTSRate) + rate - 1) / rate, step, nil
}
