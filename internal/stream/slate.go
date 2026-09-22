// slate.go — the "Source Unavailable" placeholder (spec Phase 5).
//
// When every source for a channel is down mid-stream, the reconnect loop
// feeds subscribers a short pre-rendered MPEG-TS slate on a loop instead of
// silence. Plex keeps a live, decodable stream (viewers see a slate card
// rather than a spinner or an error), and the moment a source comes back
// the pump switches back to real bytes.
//
// The slate is rendered once per process, lazily, with the same ffmpeg the
// transcode path uses. A per-pump cursor rewrites PCR, PTS/DTS, and continuity
// counters at each loop boundary so the placeholder remains one continuous
// transport stream rather than forcing decoders to recover every five
// seconds. If ffmpeg is missing or rendering fails, the feature degrades
// silently to the previous behavior (reconnect gap with no data).
package stream

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"time"
)

// slateDuration is the rendered loop length. Longer = fewer PCR wraps while
// looping; 5s keeps the render fast and the in-memory payload small.
const slateDuration = 5 * time.Second

// slate is an immutable pre-rendered MPEG-TS clip fed during outages.
type slate struct {
	data     []byte
	duration time.Duration
}

const (
	slateTSPacketSize = 188
	ptsModulus        = uint64(1) << 33
	pcrModulus        = ptsModulus * 300
)

// slateLoopCursor owns the mutable transport state for one pump. slate.data
// remains immutable and process-shared; every loop is copied before its
// timestamps/counters are rewritten because queued subscriber chunks may
// retain the prior loop's backing array.
type slateLoopCursor struct {
	slate *slate
	isTS  bool

	loop []byte
	pos  int

	ptsOffset         uint64
	pcrOffset         uint64
	looped            bool
	cc                map[uint16]slateContinuity
	completeChunks    [][]byte
	completeDurations []time.Duration
	completeIndex     int
}

type slateContinuity struct {
	havePayload bool
	last        byte
}

func newSlateLoopCursor(sl *slate) *slateLoopCursor {
	var isTS bool
	if sl != nil {
		isTS = isAlignedMPEGTS(sl.data)
	}
	return &slateLoopCursor{
		slate: sl,
		isTS:  isTS,
		cc:    make(map[uint16]slateContinuity),
	}
}

// next returns at most maxBytes without crossing a slate-loop boundary. The
// returned slice is immutable for the rest of its lifetime.
func (c *slateLoopCursor) next(maxBytes int) []byte {
	if c == nil || c.slate == nil || len(c.slate.data) == 0 || maxBytes <= 0 {
		return nil
	}
	if c.pos >= len(c.loop) {
		c.renderLoop()
	}
	end := min(c.pos+maxBytes, len(c.loop))
	chunk := c.loop[c.pos:end]
	c.pos = end
	return chunk
}

func (c *slateLoopCursor) renderLoop() {
	c.loop = append([]byte(nil), c.slate.data...)
	c.pos = 0
	if c.isTS {
		rewriteSlateLoop(c.loop, c.ptsOffset, c.pcrOffset, c.looped, c.cc)
	}
	c.looped = true
	c.ptsOffset = (c.ptsOffset + durationTicks(c.slate.duration, 90_000)) % ptsModulus
	c.pcrOffset = (c.pcrOffset + durationTicks(c.slate.duration, 27_000_000)) % pcrModulus
}

func durationTicks(d time.Duration, rate uint64) uint64 {
	if d <= 0 {
		return 0
	}
	seconds := uint64(d / time.Second)
	remainder := uint64(d % time.Second)
	return seconds*rate + remainder*rate/uint64(time.Second)
}

func isAlignedMPEGTS(data []byte) bool {
	if len(data) == 0 || len(data)%slateTSPacketSize != 0 {
		return false
	}
	for start := 0; start < len(data); start += slateTSPacketSize {
		if data[start] != 0x47 {
			return false
		}
	}
	return true
}

// rewriteSlateLoop makes a copied loop a continuation of every previously
// emitted loop. Timestamp arithmetic follows the MPEG 33-bit/PCR wrap rules.
func rewriteSlateLoop(data []byte, ptsOffset, pcrOffset uint64, repeated bool, continuity map[uint16]slateContinuity) {
	for start := 0; start+slateTSPacketSize <= len(data); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		pid := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
		adaptationControl := (packet[3] >> 4) & 0x03

		if repeated && adaptationControl >= 2 && packet[4] > 0 {
			// ffmpeg may mark the first packet of each rendered PID as a
			// discontinuity. Only the first emitted loop may retain that flag.
			packet[5] &^= 0x80
		}
		rewritePCR(packet, adaptationControl, pcrOffset)

		// Null packets do not carry a meaningful continuity sequence.
		if pid != 0x1fff {
			state := continuity[pid]
			hasPayload := adaptationControl == 1 || adaptationControl == 3
			if hasPayload {
				if state.havePayload {
					state.last = (state.last + 1) & 0x0f
				} else {
					state.havePayload = true
					state.last = packet[3] & 0x0f
				}
				packet[3] = packet[3]&0xf0 | state.last
			} else if state.havePayload {
				// Adaptation-only packets repeat the prior payload counter.
				packet[3] = packet[3]&0xf0 | state.last
			}
			continuity[pid] = state
		}

		if packet[1]&0x40 != 0 { // payload_unit_start_indicator
			rewritePESTimestamps(data, start, pid, ptsOffset)
		}
	}
}

func rewritePCR(packet []byte, adaptationControl byte, offset uint64) {
	if offset == 0 || adaptationControl < 2 || len(packet) != slateTSPacketSize || packet[4] == 0 {
		return
	}
	adaptationEnd := 5 + int(packet[4])
	if adaptationEnd > len(packet) {
		return
	}
	flags := packet[5]
	pos := 6
	if flags&0x10 != 0 { // PCR_flag
		if pos+6 > adaptationEnd {
			return
		}
		shiftPCR(packet[pos:pos+6], offset)
		pos += 6
	}
	if flags&0x08 != 0 { // OPCR_flag
		if pos+6 > adaptationEnd {
			return
		}
		shiftPCR(packet[pos:pos+6], offset)
	}
}

func shiftPCR(field []byte, offset uint64) {
	base := uint64(field[0])<<25 |
		uint64(field[1])<<17 |
		uint64(field[2])<<9 |
		uint64(field[3])<<1 |
		uint64(field[4]>>7)
	extension := uint64(field[4]&0x01)<<8 | uint64(field[5])
	total := (base*300 + extension + offset) % pcrModulus
	base, extension = total/300, total%300
	field[0] = byte(base >> 25)
	field[1] = byte(base >> 17)
	field[2] = byte(base >> 9)
	field[3] = byte(base >> 1)
	field[4] = byte(base&0x01)<<7 | field[4]&0x7e | byte(extension>>8)
	field[5] = byte(extension)
}

// rewritePESTimestamps gathers the small optional PES header across TS packet
// boundaries. ffmpeg normally fits it in the first packet, but accepting a
// split header avoids silently regressing if muxer stuffing changes.
func rewritePESTimestamps(data []byte, packetStart int, pid uint16, offset uint64) {
	if offset == 0 {
		return
	}
	var positions [19]int
	positionCount := 0
	for start := packetStart; start+slateTSPacketSize <= len(data) && positionCount < len(positions); start += slateTSPacketSize {
		packet := data[start : start+slateTSPacketSize]
		packetPID := uint16(packet[1]&0x1f)<<8 | uint16(packet[2])
		if packetPID != pid {
			continue
		}
		if start != packetStart && packet[1]&0x40 != 0 {
			break
		}
		payloadStart, ok := tsPayloadStart(packet)
		if !ok {
			continue
		}
		for i := payloadStart; i < len(packet) && positionCount < len(positions); i++ {
			positions[positionCount] = start + i
			positionCount++
		}
	}
	if positionCount < 14 || data[positions[0]] != 0 || data[positions[1]] != 0 || data[positions[2]] != 1 {
		return
	}
	if !pesHasOptionalHeader(data[positions[3]]) || data[positions[6]]&0xc0 != 0x80 {
		return
	}
	ptsDtsFlags := data[positions[7]] >> 6
	if ptsDtsFlags != 2 && ptsDtsFlags != 3 {
		return
	}
	requiredHeaderBytes := byte(5)
	if ptsDtsFlags == 3 {
		requiredHeaderBytes = 10
	}
	if data[positions[8]] < requiredHeaderBytes {
		return
	}
	shiftMPEGTimestamp(data, positions[9:14], offset)
	if ptsDtsFlags == 3 && positionCount >= 19 {
		shiftMPEGTimestamp(data, positions[14:19], offset)
	}
}

func pesHasOptionalHeader(streamID byte) bool {
	switch streamID {
	case 0xbc, 0xbe, 0xbf, 0xf0, 0xf1, 0xf2, 0xf8, 0xff:
		return false
	default:
		return true
	}
}

func tsPayloadStart(packet []byte) (int, bool) {
	if len(packet) != slateTSPacketSize || packet[0] != 0x47 {
		return 0, false
	}
	switch (packet[3] >> 4) & 0x03 {
	case 1:
		return 4, true
	case 3:
		start := 5 + int(packet[4])
		return start, start < len(packet)
	default:
		return 0, false
	}
}

func shiftMPEGTimestamp(data []byte, positions []int, offset uint64) {
	if len(positions) != 5 {
		return
	}
	b0, b1 := data[positions[0]], data[positions[1]]
	b2, b3, b4 := data[positions[2]], data[positions[3]], data[positions[4]]
	if b0&0x01 == 0 || b2&0x01 == 0 || b4&0x01 == 0 {
		return
	}
	value := uint64(b0&0x0e)<<29 |
		uint64(b1)<<22 |
		uint64(b2&0xfe)<<14 |
		uint64(b3)<<7 |
		uint64(b4>>1)
	value = (value + offset) % ptsModulus
	data[positions[0]] = b0&0xf1 | byte(value>>29)&0x0e
	data[positions[1]] = byte(value >> 22)
	data[positions[2]] = byte(value>>14)&0xfe | 0x01
	data[positions[3]] = byte(value >> 7)
	data[positions[4]] = byte(value<<1) | 0x01
}

// generateSlate renders the placeholder with ffmpeg. Tries a drawtext
// variant first (needs libfreetype, present in our cuda image); falls back
// to a plain color card if the text filter fails. ffmpegBinary == "" means
// "ffmpeg" on PATH.
func generateSlate(ctx context.Context, ffmpegBinary string, logger *slog.Logger) (*slate, error) {
	if ffmpegBinary == "" {
		ffmpegBinary = "ffmpeg"
	}
	if _, err := exec.LookPath(ffmpegBinary); err != nil {
		return nil, fmt.Errorf("ffmpeg not available: %w", err)
	}

	secs := fmt.Sprintf("%d", int(slateDuration/time.Second))
	// AAC frames are 1024 samples (21.3ms at 48kHz), and the encoder emits
	// priming/padding around a duration-exact input. Trim one 25fps video frame
	// from the silent source so the encoded audio ends before the five-second
	// loop boundary instead of overlapping the next loop's first timestamp.
	audioSecs := fmt.Sprintf("%.3f", (slateDuration - time.Second/25).Seconds())
	common := func(filters []string) []string {
		args := []string{
			"-hide_banner", "-loglevel", "error", "-nostdin",
			"-f", "lavfi", "-i", "color=c=0x10141c:s=1280x720:r=25:d=" + secs,
			"-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
			"-t", secs,
			"-af", "atrim=duration=" + audioSecs,
		}
		args = append(args, filters...)
		return append(args,
			"-c:v", "libx264", "-preset", "veryfast", "-tune", "stillimage",
			"-profile:v", "main", "-pix_fmt", "yuv420p", "-g", "50", "-bf", "0",
			"-c:a", "aac", "-b:a", "64k",
			// Match the live mux's PCR/decode phase and emit AAC promptly.
			// The default 700 ms mux delay otherwise leaves queued audio and
			// a different clock phase at each real/filler boundary.
			"-muxdelay", "0", "-muxpreload", "0",
			"-mpegts_flags", "+initial_discontinuity",
			"-f", "mpegts", "pipe:1",
		)
	}

	variants := [][]string{
		common([]string{"-vf", "drawtext=text='Source unavailable - reconnecting':fontcolor=0xc8d0dc:fontsize=44:x=(w-text_w)/2:y=(h-text_h)/2"}),
		common(nil), // plain card fallback (no libfreetype needed)
	}

	var lastErr error
	for i, args := range variants {
		renderCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		cmd := exec.CommandContext(renderCtx, ffmpegBinary, args...)
		var out, errb bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &errb
		err := cmd.Run()
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("ffmpeg variant %d: %w: %s", i, err, truncate(errb.String(), 300))
			continue
		}
		data := out.Bytes()
		if len(data) < slateTSPacketSize || !isAlignedMPEGTS(data) {
			lastErr = fmt.Errorf("ffmpeg variant %d produced invalid TS (%d bytes)", i, len(data))
			continue
		}
		if logger != nil {
			logger.Info("source-unavailable slate rendered",
				"bytes", len(data), "duration", slateDuration, "variant", i)
		}
		return &slate{data: data, duration: slateDuration}, nil
	}
	return nil, lastErr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
