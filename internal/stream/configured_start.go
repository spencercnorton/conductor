package stream

// firstProgramVCLIsConfiguredRandomAccess is stricter than finding any useful
// decoded window: a joining decoder must not receive predicted or unconfigured
// slices before that window. The scanner remains bound to the exact map/fence.
func firstProgramVCLIsConfiguredRandomAccess(data []byte, codec string, pid int) bool {
	for _, program := range scanTSProgramMaps(data, codec) {
		if program.videoPID != pid {
			continue
		}
		scanner := newProgramRandomAccessScanner(program, codec)
		for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
			packet := data[pos : pos+tsPacketSize]
			if int(packet[1]&31)<<8|int(packet[2]) == pid {
				scanner.push(packet, pos, codec)
			}
		}
		return scanner.randomAccessAt >= 0 && scanner.nal.firstVCLSeen && scanner.nal.firstVCLReady
	}
	return false
}

// trimConfiguredStartupPrefix excludes complete earlier video PES while keeping
// the last complete-start audio PES crossing or immediately preceding the video
// configuration. Retained media packets remain in their original order. Exact
// current PSI is reconstructed from CRC-validated sections. Configuration that
// shares its PES with an earlier predicted picture is rejected by the subsequent
// exact-release proof; TS alignment alone is never a codec random-access proof.
func trimConfiguredStartupPrefix(data []byte, program tsVideoProgram, configurationAt int) ([]byte, bool) {
	if configurationAt < program.mediaStart || configurationAt%tsPacketSize != 0 || configurationAt+tsPacketSize > len(data) || !program.mapIdentity.valid() {
		return nil, false
	}
	packet := data[configurationAt : configurationAt+tsPacketSize]
	if int(packet[1]&31)<<8|int(packet[2]) != program.videoPID || packet[1]&0x40 == 0 {
		return nil, false
	}
	audioStarts := make(map[int]int, program.audioCount)
	for i := 0; i < min(program.audioCount, maxAVTimelineAudioPIDs); i++ {
		audioStarts[program.audioPIDs[i]] = -1
	}
	for pos := program.mediaStart; pos < configurationAt; pos += tsPacketSize {
		p := data[pos : pos+tsPacketSize]
		pid := int(p[1]&31)<<8 | int(p[2])
		if _, selected := audioStarts[pid]; selected && p[1]&0x40 != 0 {
			if _, hasPayload := tsPayload(p); !hasPayload {
				continue
			}
			audioStarts[pid] = pos
		}
	}
	start := configurationAt
	for _, pos := range audioStarts {
		if pos >= 0 && pos < start {
			start = pos
		}
	}
	patCC, patOK := lastPayloadContinuityBefore(data, 0, start)
	pmtCC, pmtOK := lastPayloadContinuityBefore(data, program.mapIdentity.pmtPID, start)
	if !patOK || !pmtOK {
		return nil, false
	}
	out := packetizePSISections(0, program.mapIdentity.patSections, patCC)
	out = append(out, packetizePSISections(program.mapIdentity.pmtPID, [][]byte{program.mapIdentity.pmtSection}, pmtCC)...)
	observedAudio := make(map[int]bool, len(audioStarts))
	for pos := start; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		p := data[pos : pos+tsPacketSize]
		pid := int(p[1]&31)<<8 | int(p[2])
		if pid == program.videoPID && pos < configurationAt {
			continue
		}
		if audioStart, selected := audioStarts[pid]; selected {
			if _, hasPayload := tsPayload(p); hasPayload {
				observedAudio[pid] = true
			}
			if audioStart < 0 {
				if _, hasPayload := tsPayload(p); p[1]&0x40 == 0 || !hasPayload {
					continue
				}
				audioStarts[pid] = pos
			} else if pos < audioStart {
				continue
			}
		}
		out = append(out, p...)
	}
	// Keep the producer's incomplete final TS fragment for the normal transform;
	// do not silently remove bytes from a PES that continues in the next read.
	tail := data[len(data)-len(data)%tsPacketSize:]
	if len(tail) > 0 && len(tail) < 4 {
		return nil, false
	}
	if len(tail) >= 4 {
		pid := int(tail[1]&31)<<8 | int(tail[2])
		if at, selected := audioStarts[pid]; selected && at < 0 {
			return nil, false
		}
	}
	out = append(out, tail...)
	if !configuredStartupHasCompleteHeads(out, program, observedAudio) {
		return nil, false
	}
	return out, true
}

// Reuse the publication parser for PES lengths, split headers, and duplicate
// packets. FFprobe's video-only release check cannot prove an audio PES head.
// The incomplete final unit remains owned by the normal publication gate; only
// the first retained unit of each observed selected track must be complete.
func configuredStartupHasCompleteHeads(data []byte, program tsVideoProgram, observedAudio map[int]bool) bool {
	var gate pesPublicationGate
	gate.configure(avProgramPIDs{videoPID: program.videoPID, audioPIDs: program.audioPIDs, audioCount: program.audioCount}, true)
	first := make(map[int]*publicationPES)
	for pos := 0; pos < len(data); pos += tsPacketSize {
		if _, err := gate.push(data[pos:min(pos+tsPacketSize, len(data))]); err != nil {
			return false
		}
		for pid, pes := range gate.lastPES {
			if first[pid] == nil {
				first[pid] = pes
			}
		}
	}
	if first[program.videoPID] == nil || !first[program.videoPID].complete {
		return false
	}
	for pid := range observedAudio {
		if first[pid] == nil || !first[pid].complete {
			return false
		}
	}
	return true
}
