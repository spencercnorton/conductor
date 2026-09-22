package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	minPrefixDecisionBytes = 64 * 1024
	minTSProbeBytes        = minPrefixDecisionBytes
	prefixProbeByteStep    = 2 * 1024 * 1024
	prefixProbeInterval    = 500 * time.Millisecond
	maxMediaPrefixBytes    = 16 * 1024 * 1024
	minUsefulMediaDuration = 750 * time.Millisecond
	minUsefulDecodedFrames = 8
	maxDecodedFrameGap     = 500 * time.Millisecond
	tsPacketSize           = 188
)

var errMediaPrefixNotReady = errors.New("media prefix did not reach a decodable random access point")
var errUncertainMediaPrefix = errors.New("local media-prefix validation was inconclusive")

type prefixDecisionKind uint8

const (
	prefixNeedMore prefixDecisionKind = iota
	prefixReady
	prefixBypass
)

type prefixDecision struct {
	kind          prefixDecisionKind
	start         int
	prepend       []byte
	reason        string
	mediaDuration time.Duration
	videoPID      int
	audioPIDs     [maxAVTimelineAudioPIDs]int
	audioCount    int
	pcrPID        int
	havePCRPID    bool
	haveProgram   bool
	mapIdentity   selectedProgramMapIdentity
}

type mediaPrefixValidator interface {
	Validate(context.Context, []byte) (prefixDecision, error)
}

func classifyMediaPrefixValidationError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(errUncertainMediaPrefix, err)
	}
	return err
}

// startupMediaGate keeps response bytes private until the prefix validator
// proves they begin at a useful MPEG-TS random-access window. Validation is
// bounded by both the source-attempt context and a hard memory cap. Unknown
// non-TS containers bypass after a small signature window; Conductor still
// supports them without claiming MPEG-TS guarantees.
type startupMediaGate struct {
	validator      mediaPrefixValidator
	buffer         []byte
	ready          bool
	started        time.Time
	completed      time.Time
	lastProbe      time.Time
	nextProbe      int
	reason         string
	validatedBytes int
	videoPID       int
	audioPIDs      [maxAVTimelineAudioPIDs]int
	audioCount     int
	pcrPID         int
	havePCRPID     bool
	haveProgram    bool
	mapIdentity    selectedProgramMapIdentity
}

func newStartupMediaGate(validator mediaPrefixValidator) *startupMediaGate {
	return &startupMediaGate{
		validator: validator,
		started:   time.Now(),
		nextProbe: minPrefixDecisionBytes,
	}
}

func (g *startupMediaGate) Push(ctx context.Context, p []byte) ([]byte, bool, error) {
	if g.ready || g.validator == nil {
		if !g.ready {
			g.completed = time.Now()
		}
		g.ready = true
		if g.reason == "" {
			g.reason = "validation_disabled"
		}
		return p, true, nil
	}
	if len(p) > maxMediaPrefixBytes-len(g.buffer) {
		// The byte-step and time probes can both fall just below the hard cap.
		// Fill the remaining bounded window and force one final decision before
		// rejecting it. If it is ready, append the unbuffered part of this read
		// to the released prefix so crossing the cap never creates a media gap.
		remaining := maxMediaPrefixBytes - len(g.buffer)
		g.buffer = append(g.buffer, p[:remaining]...)
		media, ready, err := g.validate(ctx, true)
		if err != nil {
			return nil, false, err
		}
		if ready {
			return append(media, p[remaining:]...), true, nil
		}
		return nil, false, fmt.Errorf("%w: buffered=%d cap=%d",
			errMediaPrefixNotReady, len(g.buffer)+len(p)-remaining, maxMediaPrefixBytes)
	}
	g.buffer = append(g.buffer, p...)
	now := time.Now()
	if len(g.buffer) < g.nextProbe &&
		(g.lastProbe.IsZero() || now.Sub(g.lastProbe) < prefixProbeInterval) {
		return nil, false, nil
	}
	return g.validate(ctx, false)
}

// Finalize forces one last validation when the producer reaches a terminal
// read. Finite/replayed bodies can arrive entirely inside the normal 500 ms /
// 2 MiB probe cadence; without this pass, a complete useful body whose first
// 64 KiB was too short could be discarded at EOF without ever being judged.
func (g *startupMediaGate) Finalize(ctx context.Context) ([]byte, bool, error) {
	if g.ready {
		return nil, true, nil
	}
	if len(g.buffer) == 0 {
		g.completed = time.Now()
		return nil, false, nil
	}
	return g.validate(ctx, true)
}

func (g *startupMediaGate) PushFinal(ctx context.Context, p []byte) ([]byte, bool, error) {
	media, ready, err := g.Push(ctx, p)
	if err != nil || ready {
		return media, ready, err
	}
	return g.Finalize(ctx)
}

func (g *startupMediaGate) validate(ctx context.Context, final bool) ([]byte, bool, error) {
	g.lastProbe = time.Now()
	g.nextProbe = len(g.buffer) + prefixProbeByteStep
	decision, err := g.validator.Validate(ctx, g.buffer)
	if err != nil {
		g.completed = time.Now()
		return nil, false, err
	}
	g.reason = decision.reason
	if decision.kind == prefixNeedMore {
		if final {
			g.completed = time.Now()
		}
		return nil, false, nil
	}
	if decision.start < 0 || decision.start > len(g.buffer) {
		g.completed = time.Now()
		return nil, false, fmt.Errorf("%w: validator start=%d bytes=%d",
			errMediaPrefixNotReady, decision.start, len(g.buffer))
	}
	if decision.haveProgram && decision.audioCount > maxAVTimelineAudioPIDs {
		g.completed = time.Now()
		return nil, false, fmt.Errorf("%w: selected programme declares %d audio tracks (safety limit %d)",
			errMediaPrefixNotReady, decision.audioCount, maxAVTimelineAudioPIDs)
	}
	g.ready = true
	g.completed = time.Now()
	g.pcrPID = decision.pcrPID
	g.havePCRPID = decision.havePCRPID
	g.videoPID = decision.videoPID
	g.audioPIDs = decision.audioPIDs
	g.audioCount = decision.audioCount
	g.haveProgram = decision.haveProgram
	g.mapIdentity = decision.mapIdentity.clone()
	var out []byte
	if len(decision.prepend) > 0 {
		out = make([]byte, 0, len(decision.prepend)+len(g.buffer)-decision.start)
		out = append(out, decision.prepend...)
		out = append(out, g.buffer[decision.start:]...)
	} else {
		out = g.buffer[decision.start:]
	}
	// Exact selected programmes receive per-PID markers in the final packet-
	// bounded transform, immediately before each PID's first packet (including a
	// late optional audio or distinct PCR PID). Timeline safety treats those
	// one-time markers as PID initialization rather than repeated shared epochs.
	// Validators without an exact map retain the legacy best-effort marking.
	if !decision.haveProgram || !decision.mapIdentity.valid() {
		out = markInitialTSDiscontinuity(out)
	}
	g.validatedBytes = len(out)
	g.buffer = nil
	return out, true, nil
}

func (g *startupMediaGate) BufferedBytes() int {
	if g.ready {
		return g.validatedBytes
	}
	return len(g.buffer)
}
func (g *startupMediaGate) Wait() time.Duration {
	if !g.completed.IsZero() {
		return g.completed.Sub(g.started)
	}
	return time.Since(g.started)
}
func (g *startupMediaGate) Reason() string { return g.reason }

func (g *startupMediaGate) PCRPID() (int, bool) {
	return g.pcrPID, g.havePCRPID
}

// SelectedProgram returns the exact elementary/PCR PID set selected by the
// startup validator. The relay boundary observer must use this binding rather
// than guessing PAT order in an MPTS after bytes have become public.
func (g *startupMediaGate) SelectedProgram() (avProgramPIDs, bool) {
	if !g.haveProgram {
		return avProgramPIDs{}, false
	}
	return avProgramPIDs{
		videoPID:    g.videoPID,
		pcrPID:      g.pcrPID,
		audioPIDs:   g.audioPIDs,
		audioCount:  g.audioCount,
		mapIdentity: g.mapIdentity.clone(),
	}, true
}

type ffprobeMediaPrefixValidator struct {
	ffprobeBinary string
	available     bool
	probePrefix   func(context.Context, []byte) (prefixProbeOutput, string, error)
}

func newFFprobeMediaPrefixValidator(ffmpegBinary string) *ffprobeMediaPrefixValidator {
	binary := companionFFprobe(ffmpegBinary)
	_, err := exec.LookPath(binary)
	return &ffprobeMediaPrefixValidator{ffprobeBinary: binary, available: err == nil}
}

type prefixProbeOutput struct {
	Streams []struct {
		Index         *int            `json:"index"`
		ID            json.RawMessage `json:"id"`
		CodecName     string          `json:"codec_name"`
		CodecType     string          `json:"codec_type"`
		ExtradataSize int             `json:"extradata_size"`
		Extradata     string          `json:"extradata"`
	} `json:"streams"`
	Frames []struct {
		StreamIndex             *int   `json:"stream_index"`
		KeyFrame                int    `json:"key_frame"`
		PacketPosition          string `json:"pkt_pos"`
		BestEffortTimestampTime string `json:"best_effort_timestamp_time"`
	} `json:"frames"`
}

func parseFFprobeTSPID(raw json.RawMessage) (int, bool) {
	value := bytes.TrimSpace(raw)
	if len(value) == 0 || bytes.Equal(value, []byte("null")) {
		return 0, false
	}
	var text string
	if value[0] == '"' {
		if err := json.Unmarshal(value, &text); err != nil {
			return 0, false
		}
	} else {
		text = string(value)
	}
	text = strings.TrimSpace(text)
	base := 10
	if strings.HasPrefix(text, "0x") || strings.HasPrefix(text, "0X") {
		base = 16
		text = text[2:]
	}
	if text == "" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(text, base, 13)
	if err != nil || parsed > 0x1fff {
		return 0, false
	}
	return int(parsed), true
}

func (v *ffprobeMediaPrefixValidator) probe(ctx context.Context, data []byte) (prefixProbeOutput, string, error) {
	if v.probePrefix != nil {
		return v.probePrefix(ctx, data)
	}
	cmd := exec.CommandContext(ctx, v.ffprobeBinary,
		"-hide_banner", "-loglevel", "error",
		"-select_streams", "v:0", "-show_data",
		"-show_streams", "-show_frames",
		"-show_entries", "stream=index,id,codec_name,codec_type,extradata,extradata_size:frame=stream_index,key_frame,pkt_pos,best_effort_timestamp_time",
		"-of", "json", "pipe:0")
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return prefixProbeOutput{}, "", ctxErr
		}
		// A prefix ending mid-PES commonly makes older ffprobe builds exit
		// non-zero. More bytes, not source-health attribution, is the safe
		// response while the bounded attempt remains alive.
		return prefixProbeOutput{}, "ffprobe_prefix_incomplete", nil
	}
	var probe prefixProbeOutput
	if err := json.Unmarshal(out, &probe); err != nil {
		return prefixProbeOutput{}, "ffprobe_json_incomplete", nil
	}
	return probe, "", nil
}

func (v *ffprobeMediaPrefixValidator) Validate(ctx context.Context, data []byte) (prefixDecision, error) {
	syncOffset, isTS := findTSSyncOffset(data)
	if !isTS {
		if len(data) < minPrefixDecisionBytes {
			return prefixDecision{kind: prefixNeedMore, reason: "awaiting_container_signature"}, nil
		}
		return prefixDecision{kind: prefixBypass, reason: "non_mpegts_container"}, nil
	}
	if !v.available {
		return prefixDecision{kind: prefixBypass, start: syncOffset, reason: "ffprobe_unavailable"}, nil
	}
	if len(data)-syncOffset < minTSProbeBytes {
		return prefixDecision{kind: prefixNeedMore, reason: "mpegts_prefix_buffering"}, nil
	}

	probe, incompleteReason, err := v.probe(ctx, data[syncOffset:])
	if err != nil {
		return prefixDecision{}, err
	}
	if incompleteReason != "" {
		return prefixDecision{kind: prefixNeedMore, reason: incompleteReason}, nil
	}
	if len(probe.Streams) == 0 {
		return prefixDecision{kind: prefixNeedMore, reason: "mpegts_video_not_decodable"}, nil
	}
	stream := probe.Streams[0]
	selectedVideoPID, pidOK := parseFFprobeTSPID(stream.ID)
	if !pidOK || stream.Index == nil || *stream.Index < 0 {
		return prefixDecision{kind: prefixNeedMore, reason: "mpegts_video_pid_missing"}, nil
	}
	// FFprobe 4.4 (the production runtime) exposes in-band H.264/HEVC
	// configuration through -show_data's extradata field but may omit
	// extradata_size entirely. Accept either representation; a decoded
	// keyframe is still required below.
	if (stream.CodecName == "h264" || stream.CodecName == "hevc") &&
		stream.ExtradataSize == 0 && strings.TrimSpace(stream.Extradata) == "" {
		return prefixDecision{kind: prefixNeedMore, reason: "codec_configuration_missing"}, nil
	}
	randomAccessPosition := -1
	configurationPosition := -1
	configuredVideoPID := selectedVideoPID
	if stream.CodecName == "h264" || stream.CodecName == "hevc" {
		configurationPosition, randomAccessPosition, configuredVideoPID =
			findConfiguredRandomAccessNALProgram(data[syncOffset:], stream.CodecName, selectedVideoPID)
		if randomAccessPosition < 0 {
			return prefixDecision{kind: prefixNeedMore, reason: "configured_idr_or_irap_missing"}, nil
		}
	}

	keyTimestamp := -1.0
	keyPosition := -1
	keyIndex := -1
	for i, frame := range probe.Frames {
		if frame.StreamIndex == nil || *frame.StreamIndex != *stream.Index {
			continue
		}
		pts, ptsErr := strconv.ParseFloat(frame.BestEffortTimestampTime, 64)
		if ptsErr != nil {
			continue
		}
		if keyPosition < 0 && frame.KeyFrame == 1 {
			pos, posErr := strconv.Atoi(frame.PacketPosition)
			if posErr == nil && pos >= 0 && (randomAccessPosition < 0 || pos >= randomAccessPosition) {
				keyPosition = pos
				keyTimestamp = pts
				keyIndex = i
			}
		}
	}
	if keyPosition < 0 {
		return prefixDecision{kind: prefixNeedMore, reason: "random_access_frame_missing"}, nil
	}
	if randomAccessPosition < 0 {
		randomAccessPosition = keyPosition
		configurationPosition = keyPosition
	}
	lastTimestamp := keyTimestamp
	previousTimestamp := keyTimestamp
	decodedFrames := 0
	for _, frame := range probe.Frames[keyIndex:] {
		if frame.StreamIndex == nil || *frame.StreamIndex != *stream.Index {
			continue
		}
		pts, ptsErr := strconv.ParseFloat(frame.BestEffortTimestampTime, 64)
		if ptsErr != nil {
			continue
		}
		if decodedFrames > 0 {
			if pts+0.001 < previousTimestamp {
				return prefixDecision{kind: prefixNeedMore, reason: "post_keyframe_timestamp_non_monotonic"}, nil
			}
			if time.Duration((pts-previousTimestamp)*float64(time.Second)) > maxDecodedFrameGap {
				return prefixDecision{kind: prefixNeedMore, reason: "post_keyframe_timestamp_gap"}, nil
			}
		}
		decodedFrames++
		previousTimestamp = pts
		if pts > lastTimestamp {
			lastTimestamp = pts
		}
	}
	if decodedFrames < minUsefulDecodedFrames {
		return prefixDecision{kind: prefixNeedMore, reason: "post_keyframe_frame_count_buffering"}, nil
	}
	if lastTimestamp-keyTimestamp < minUsefulMediaDuration.Seconds() {
		return prefixDecision{kind: prefixNeedMore, reason: "post_keyframe_media_buffering"}, nil
	}
	program, ok := findSafeTSProgramPrefix(
		data[syncOffset:], configurationPosition, stream.CodecName, configuredVideoPID,
	)
	if !ok {
		return prefixDecision{kind: prefixNeedMore, reason: "pat_pmt_before_keyframe_missing"}, nil
	}
	if program.safeStart < 0 || syncOffset+program.safeStart > len(data) {
		return prefixDecision{kind: prefixNeedMore, reason: "release_barrier_incomplete"}, nil
	}
	releaseStart, releasePrepend := syncOffset+program.safeStart, program.psiPrefix
	releaseCandidate := data[releaseStart:]
	if len(program.psiPrefix) > 0 {
		exact := make([]byte, 0, len(program.psiPrefix)+len(releaseCandidate))
		exact = append(exact, program.psiPrefix...)
		exact = append(exact, releaseCandidate...)
		releaseCandidate = exact
	}
	if (stream.CodecName == "h264" || stream.CodecName == "hevc") &&
		!firstProgramVCLIsConfiguredRandomAccess(releaseCandidate, stream.CodecName, configuredVideoPID) {
		trimmed, trimOK := trimConfiguredStartupPrefix(data[syncOffset:], program, configurationPosition)
		if !trimOK {
			return prefixDecision{kind: prefixNeedMore, reason: "release_complete_configured_prefix_buffering"}, nil
		}
		// The rebuilt prefix is the exact release. Future reads continue after
		// the buffered bytes; no live publication or existing viewer is changed.
		releaseCandidate = trimmed
		releaseStart = len(data)
		releasePrepend = trimmed
	}
	releaseProbe, releaseIncomplete, releaseErr := v.probe(ctx, releaseCandidate)
	if releaseErr != nil {
		return prefixDecision{}, releaseErr
	}
	if releaseIncomplete != "" {
		return prefixDecision{kind: prefixNeedMore, reason: "release_" + releaseIncomplete}, nil
	}
	releaseDuration, releaseReason := validateExactReleasedPrefix(
		releaseCandidate, releaseProbe, stream.CodecName, configuredVideoPID,
	)
	if releaseReason != "" {
		return prefixDecision{kind: prefixNeedMore, reason: releaseReason}, nil
	}
	return prefixDecision{
		kind:          prefixReady,
		start:         releaseStart,
		prepend:       releasePrepend,
		reason:        "mpegts_pat_pmt_keyframe_buffered",
		mediaDuration: releaseDuration,
		videoPID:      program.videoPID,
		audioPIDs:     program.audioPIDs,
		audioCount:    program.audioCount,
		pcrPID:        program.pcrPID,
		havePCRPID:    program.pcrPID >= 0 && program.pcrPID < 0x1fff,
		haveProgram:   true,
		mapIdentity:   program.mapIdentity.clone(),
	}, nil
}

// validateExactReleasedPrefix independently probes only the bytes that will be
// released to Plex. The wider buffered prefix may contain valid configuration
// from an obsolete PMT or transport epoch; it is never evidence that the
// post-barrier candidate is independently decodable.
func validateExactReleasedPrefix(
	data []byte,
	probe prefixProbeOutput,
	codec string,
	expectedVideoPID int,
) (time.Duration, string) {
	if len(probe.Streams) == 0 {
		return 0, "release_video_not_decodable"
	}
	stream := probe.Streams[0]
	videoPID, pidOK := parseFFprobeTSPID(stream.ID)
	if !pidOK || videoPID != expectedVideoPID || stream.Index == nil || *stream.Index < 0 {
		return 0, "release_video_pid_mismatch"
	}
	if stream.CodecName != codec {
		return 0, "release_video_codec_mismatch"
	}
	if (codec == "h264" || codec == "hevc") &&
		stream.ExtradataSize == 0 && strings.TrimSpace(stream.Extradata) == "" {
		return 0, "release_codec_configuration_missing"
	}
	_, randomAccessPosition, configuredPID :=
		findConfiguredRandomAccessNALProgram(data, codec, expectedVideoPID)
	if randomAccessPosition < 0 || configuredPID != expectedVideoPID {
		return 0, "release_configured_idr_or_irap_missing"
	}

	if (codec == "h264" || codec == "hevc") && !firstProgramVCLIsConfiguredRandomAccess(data, codec, expectedVideoPID) {
		return 0, "release_leading_video_not_random_access"
	}

	keyTimestamp := -1.0
	keyIndex := -1
	for i, frame := range probe.Frames {
		if frame.StreamIndex == nil || *frame.StreamIndex != *stream.Index {
			continue
		}
		position, positionErr := strconv.Atoi(frame.PacketPosition)
		pts, ptsErr := strconv.ParseFloat(frame.BestEffortTimestampTime, 64)
		if frame.KeyFrame != 1 || positionErr != nil || position != randomAccessPosition || ptsErr != nil {
			return 0, "release_first_decoded_frame_not_random_access"
		}
		keyTimestamp = pts
		keyIndex = i
		break
	}
	if keyIndex < 0 {
		return 0, "release_random_access_frame_missing"
	}

	lastTimestamp := keyTimestamp
	previousTimestamp := keyTimestamp
	decodedFrames := 0
	for _, frame := range probe.Frames[keyIndex:] {
		if frame.StreamIndex == nil || *frame.StreamIndex != *stream.Index {
			continue
		}
		pts, ptsErr := strconv.ParseFloat(frame.BestEffortTimestampTime, 64)
		if ptsErr != nil {
			continue
		}
		if decodedFrames > 0 {
			if pts+0.001 < previousTimestamp {
				return 0, "release_post_keyframe_timestamp_non_monotonic"
			}
			if time.Duration((pts-previousTimestamp)*float64(time.Second)) > maxDecodedFrameGap {
				return 0, "release_post_keyframe_timestamp_gap"
			}
		}
		decodedFrames++
		previousTimestamp = pts
		if pts > lastTimestamp {
			lastTimestamp = pts
		}
	}
	if decodedFrames < minUsefulDecodedFrames {
		return 0, "release_post_keyframe_frame_count_buffering"
	}
	if lastTimestamp-keyTimestamp < minUsefulMediaDuration.Seconds() {
		return 0, "release_post_keyframe_media_buffering"
	}
	return time.Duration((lastTimestamp - keyTimestamp) * float64(time.Second)), ""
}

// markInitialTSDiscontinuity makes the first published packet epoch explicit
// for every non-null PID. When that first packet already has adaptation-field
// flag space it is marked in place. A payload-only first packet cannot safely
// surrender bytes, so a full adaptation-only discontinuity packet is inserted
// immediately before it with the same continuity counter. The payload,
// PAT/PMT sections, codec configuration, and random-access frame therefore
// remain byte-for-byte intact, while a decoder sees the boundary before (not
// sometime after) the first selected A/V payload.
func markInitialTSDiscontinuity(data []byte) []byte {
	syncOffset, ok := findTSSyncOffset(data)
	if !ok {
		return data
	}
	type insertion struct {
		at     int
		packet []byte
	}
	var insertions []insertion
	var markInPlace []int
	marked := make(map[int]struct{})
	for pos := syncOffset; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] != 0x47 {
			return data
		}
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid == 0x1fff {
			continue
		}
		if _, exists := marked[pid]; exists {
			continue
		}
		adaptationControl := (packet[3] >> 4) & 0x03
		if (adaptationControl == 2 || adaptationControl == 3) && packet[4] >= 1 {
			markInPlace = append(markInPlace, pos)
			marked[pid] = struct{}{}
			continue
		}
		marker := newTSDiscontinuityMarker(packet)
		insertions = append(insertions, insertion{at: pos, packet: marker})
		marked[pid] = struct{}{}
	}
	// Apply in-place marks only after the complete TS walk succeeds. A malformed
	// tail must not return a partially rewritten prefix.
	for _, pos := range markInPlace {
		data[pos+5] |= 0x80
	}
	if len(insertions) == 0 {
		return data
	}
	out := make([]byte, 0, len(data)+len(insertions)*tsPacketSize)
	previous := 0
	for _, insertion := range insertions {
		out = append(out, data[previous:insertion.at]...)
		out = append(out, insertion.packet...)
		previous = insertion.at
	}
	out = append(out, data[previous:]...)
	return out
}

// newTSDiscontinuityMarker creates a payload-free boundary immediately before
// a selected PID's first payload. Adaptation-only packets do not advance the
// payload continuity counter, so copying the following packet's CC preserves
// both its bytes and decoder-visible continuity.
func newTSDiscontinuityMarker(nextPacket []byte) []byte {
	pid := int(nextPacket[1]&0x1f)<<8 | int(nextPacket[2])
	return newTSDiscontinuityMarkerForPID(pid, nextPacket[3]&0x0f)
}

func newTSDiscontinuityMarkerForPID(pid int, continuity byte) []byte {
	marker := make([]byte, tsPacketSize)
	for i := range marker {
		marker[i] = 0xff
	}
	marker[0] = 0x47
	marker[1] = byte(pid>>8) & 0x1f
	marker[2] = byte(pid)
	marker[3] = 0x20 | continuity&0x0f
	marker[4] = 183
	marker[5] = 0x80
	return marker
}

func findTSSyncOffset(data []byte) (int, bool) {
	if len(data) < tsPacketSize*5 {
		return 0, false
	}
	limit := min(tsPacketSize, len(data)-tsPacketSize*5+1)
	for offset := 0; offset < limit; offset++ {
		matched := true
		for packet := 0; packet < 5; packet++ {
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

func findSafeTSPrefix(data []byte, keyPosition int) (int, bool) {
	return findSafeTSPrefixForProgram(data, keyPosition, "", -1)
}

func findSafeTSPrefixForProgram(data []byte, keyPosition int, codec string, videoPID int) (int, bool) {
	program, ok := findSafeTSProgramPrefix(data, keyPosition, codec, videoPID)
	return program.safeStart, ok
}

func findSafeTSProgramPrefix(data []byte, keyPosition int, codec string, videoPID int) (tsVideoProgram, bool) {
	if keyPosition < 0 {
		return tsVideoProgram{}, false
	}
	limit := min(len(data), keyPosition+tsPacketSize)
	for _, program := range scanTSProgramMaps(data[:limit], codec) {
		if videoPID < 0 || program.videoPID == videoPID {
			return program, true
		}
	}
	return tsVideoProgram{}, false
}

func parsePATPMTPID(packet []byte) (int, bool) {
	payload, ok := tsPayload(packet)
	if !ok || len(payload) < 13 {
		return 0, false
	}
	pointer := int(payload[0])
	if 1+pointer+12 > len(payload) {
		return 0, false
	}
	return parsePATPMTPIDSection(payload[1+pointer:])
}

func parsePATPMTPIDSection(section []byte) (int, bool) {
	parsed, ok := parsePATSection(section)
	if !ok || !parsed.current || len(parsed.programs) == 0 {
		return 0, false
	}
	return parsed.programs[0].pmtPID, true
}

// mpeg2PSICRCValid applies the non-reflected MPEG-2 PSI CRC-32 (polynomial
// 0x04C11DB7, initial remainder 0xFFFFFFFF). A complete section, including its
// transmitted four-byte CRC, is valid only when the final remainder is zero.
func mpeg2PSICRCValid(section []byte) bool {
	if len(section) < 4 {
		return false
	}
	crc := uint32(0xffffffff)
	for _, value := range section {
		crc ^= uint32(value) << 24
		for bit := 0; bit < 8; bit++ {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc == 0
}

func rewriteMPEG2PSICRC(section []byte) {
	if len(section) < 4 {
		return
	}
	crc := uint32(0xffffffff)
	for _, value := range section[:len(section)-4] {
		crc ^= uint32(value) << 24
		for bit := 0; bit < 8; bit++ {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	section[len(section)-4] = byte(crc >> 24)
	section[len(section)-3] = byte(crc >> 16)
	section[len(section)-2] = byte(crc >> 8)
	section[len(section)-1] = byte(crc)
}

type patProgram struct {
	programNumber int
	pmtPID        int
}

type parsedPATSection struct {
	transportStreamID uint16
	version           byte
	current           bool
	sectionNumber     byte
	lastSectionNumber byte
	programs          []patProgram
}

func parsePATSection(section []byte) (parsedPATSection, bool) {
	if len(section) < 12 {
		return parsedPATSection{}, false
	}
	if section[0] != 0x00 {
		return parsedPATSection{}, false
	}
	sectionLength := int(section[1]&0x0f)<<8 | int(section[2])
	total := 3 + sectionLength
	end := total - 4 // exclude CRC32
	if end > len(section) || end < 8 || (end-8)%4 != 0 {
		return parsedPATSection{}, false
	}
	if total > len(section) || !mpeg2PSICRCValid(section[:total]) {
		return parsedPATSection{}, false
	}
	parsed := parsedPATSection{
		transportStreamID: uint16(section[3])<<8 | uint16(section[4]),
		version:           (section[5] >> 1) & 0x1f,
		current:           section[5]&0x01 != 0,
		sectionNumber:     section[6],
		lastSectionNumber: section[7],
		programs:          make([]patProgram, 0, (end-8)/4),
	}
	if parsed.sectionNumber > parsed.lastSectionNumber {
		return parsedPATSection{}, false
	}
	for pos := 8; pos+4 <= end; pos += 4 {
		program := int(section[pos])<<8 | int(section[pos+1])
		if program == 0 {
			continue
		}
		pid := int(section[pos+2]&0x1f)<<8 | int(section[pos+3])
		parsed.programs = append(parsed.programs, patProgram{programNumber: program, pmtPID: pid})
	}
	return parsed, true
}

func tsPayload(packet []byte) ([]byte, bool) {
	if !tsPacketStructureValid(packet) {
		return nil, false
	}
	control := (packet[3] >> 4) & 0x03
	if control == 2 {
		return nil, false
	}
	offset := 4
	if control == 3 {
		if offset >= len(packet) {
			return nil, false
		}
		offset += 1 + int(packet[offset])
	}
	if offset >= len(packet) {
		return nil, false
	}
	return packet[offset:], true
}

func tsPacketStructureValid(packet []byte) bool {
	if len(packet) != tsPacketSize || packet[0] != 0x47 {
		return false
	}
	control := (packet[3] >> 4) & 0x03
	switch control {
	case 1:
		return true
	case 2:
		// Adaptation-only consumes every byte after its length field.
		if packet[4] != tsPacketSize-5 {
			return false
		}
	case 3:
		// Adaptation plus payload must leave at least one payload byte.
		if int(packet[4]) > tsPacketSize-6 {
			return false
		}
	default:
		return false
	}
	return tsAdaptationFieldValid(packet[5 : 5+int(packet[4])])
}

// tsAdaptationFieldValid walks the bounded optional-field grammar before any
// selected packet can reach the decoder or contribute a PCR. It deliberately
// ignores stuffing byte values and the nested contents of the adaptation
// extension; only lengths and the PCR/OPCR clock encoding are safety-relevant.
func tsAdaptationFieldValid(field []byte) bool {
	if len(field) == 0 {
		return true
	}
	flags := field[0]
	position := 1
	consumeClock := func() bool {
		if position+6 > len(field) || !mpegPCRFieldValid(field[position:position+6]) {
			return false
		}
		position += 6
		return true
	}
	if flags&0x10 != 0 && !consumeClock() { // PCR_flag
		return false
	}
	if flags&0x08 != 0 { // OPCR_flag
		// An OPCR without the programme clock it references is not a usable
		// selected adaptation epoch and is rejected conservatively.
		if flags&0x10 == 0 || !consumeClock() {
			return false
		}
	}
	if flags&0x04 != 0 { // splicing_point_flag
		if position+1 > len(field) {
			return false
		}
		position++
	}
	if flags&0x02 != 0 { // transport_private_data_flag
		if position+1 > len(field) {
			return false
		}
		length := int(field[position])
		position++
		if position+length > len(field) {
			return false
		}
		position += length
	}
	if flags&0x01 != 0 { // adaptation_field_extension_flag
		if position+1 > len(field) {
			return false
		}
		length := int(field[position])
		position++
		if position+length > len(field) {
			return false
		}
	}
	return true
}

func mpegPCRFieldValid(field []byte) bool {
	if len(field) != 6 || field[4]&0x7e != 0x7e {
		return false
	}
	extension := int(field[4]&0x01)<<8 | int(field[5])
	return extension < 300
}

const maxPSISectionLength = 1021

type assembledPSISection struct {
	data  []byte
	start int
}

// psiSectionAssembler reconstructs one PID's PSI sections across arbitrary TS
// packet boundaries. It honors payload-unit pointer fields and continuity
// counters, discarding an incomplete section on gaps, transport errors,
// scrambling, or an explicit discontinuity. PAT/PMT section_length is bounded
// by MPEG-TS's section-syntax maximum before any allocation can grow.
type psiSectionAssembler struct {
	buffer         []byte
	start          int
	expected       int
	haveContinuity bool
	continuity     byte
	lastInvalid    bool
	lastAbandoned  bool
}

func (a *psiSectionAssembler) resetSection() {
	a.buffer = nil
	a.start = -1
	a.expected = 0
}

func (a *psiSectionAssembler) reset() {
	a.resetSection()
	a.haveContinuity = false
}

func (a *psiSectionAssembler) appendCurrent(data []byte) (int, *assembledPSISection, bool) {
	used := 0
	if len(a.buffer) < 3 {
		take := min(3-len(a.buffer), len(data))
		a.buffer = append(a.buffer, data[:take]...)
		data = data[take:]
		used += take
		if len(a.buffer) < 3 {
			return used, nil, false
		}
		sectionLength := int(a.buffer[1]&0x0f)<<8 | int(a.buffer[2])
		if sectionLength < 4 || sectionLength > maxPSISectionLength {
			a.lastInvalid = true
			a.resetSection()
			return used, nil, true
		}
		a.expected = 3 + sectionLength
	}
	needed := a.expected - len(a.buffer)
	take := min(needed, len(data))
	a.buffer = append(a.buffer, data[:take]...)
	used += take
	if len(a.buffer) < a.expected {
		return used, nil, false
	}
	section := &assembledPSISection{data: a.buffer, start: a.start}
	a.resetSection()
	return used, section, false
}

func (a *psiSectionAssembler) startSections(data []byte, packetPosition int) []assembledPSISection {
	var sections []assembledPSISection
	for len(data) > 0 && data[0] != 0xff {
		a.start = packetPosition
		used, section, invalid := a.appendCurrent(data)
		data = data[used:]
		if invalid {
			return sections
		}
		if section == nil {
			return sections
		}
		sections = append(sections, *section)
	}
	return sections
}

func (a *psiSectionAssembler) push(packet []byte, packetPosition int) []assembledPSISection {
	a.lastInvalid = false
	a.lastAbandoned = false
	if len(packet) != tsPacketSize || packet[0] != 0x47 ||
		packet[1]&0x80 != 0 || packet[3]&0xc0 != 0 {
		a.lastInvalid = true
		a.reset()
		return nil
	}
	control := (packet[3] >> 4) & 0x03
	if (control == 2 || control == 3) && packet[4] >= 1 && packet[5]&0x80 != 0 {
		a.reset()
	}
	if control == 0 || control == 2 {
		return nil
	}
	continuity := packet[3] & 0x0f
	continuityLost := false
	if a.haveContinuity {
		if continuity == a.continuity {
			return nil // legal duplicate payload packet
		}
		if continuity != ((a.continuity + 1) & 0x0f) {
			a.resetSection()
			continuityLost = true
		}
	}
	a.haveContinuity = true
	a.continuity = continuity
	payload, ok := tsPayload(packet)
	if !ok {
		a.lastInvalid = true
		a.resetSection()
		return nil
	}
	if packet[1]&0x40 == 0 {
		if continuityLost || len(a.buffer) == 0 {
			return nil
		}
		_, section, invalid := a.appendCurrent(payload)
		if invalid || section == nil {
			return nil
		}
		return []assembledPSISection{*section}
	}

	if len(payload) == 0 {
		a.lastInvalid = true
		a.resetSection()
		return nil
	}
	pointer := int(payload[0])
	if pointer > len(payload)-1 {
		a.lastInvalid = true
		a.resetSection()
		return nil
	}
	beforeStart := payload[1 : 1+pointer]
	var sections []assembledPSISection
	if !continuityLost && len(a.buffer) > 0 {
		_, section, invalid := a.appendCurrent(beforeStart)
		if invalid {
			a.resetSection()
		} else if section != nil {
			sections = append(sections, *section)
		} else if len(a.buffer) > 0 {
			a.lastAbandoned = true
		}
	}
	// PUSI guarantees a new section at the pointer boundary. Any previous
	// section still incomplete there is malformed and must not bleed into it.
	a.resetSection()
	return append(sections, a.startSections(payload[1+pointer:], packetPosition)...)
}

type patTableIdentity struct {
	transportStreamID uint16
	version           byte
	lastSectionNumber byte
}

type collectedPATSection struct {
	start    int
	data     []byte
	programs []patProgram
}

type patTableAccumulator struct {
	identity patTableIdentity
	sections map[byte]collectedPATSection
}

func newPATTableAccumulator(section parsedPATSection) *patTableAccumulator {
	return &patTableAccumulator{
		identity: patTableIdentity{
			transportStreamID: section.transportStreamID,
			version:           section.version,
			lastSectionNumber: section.lastSectionNumber,
		},
		sections: make(map[byte]collectedPATSection, int(section.lastSectionNumber)+1),
	}
}

func (a *patTableAccumulator) matches(section parsedPATSection) bool {
	return a != nil && a.identity == (patTableIdentity{
		transportStreamID: section.transportStreamID,
		version:           section.version,
		lastSectionNumber: section.lastSectionNumber,
	})
}

func (a *patTableAccumulator) add(section parsedPATSection, start int, data []byte) bool {
	if !a.matches(section) {
		return false
	}
	a.sections[section.sectionNumber] = collectedPATSection{
		start: start, data: append([]byte(nil), data...), programs: section.programs,
	}
	if len(a.sections) != int(a.identity.lastSectionNumber)+1 {
		return false
	}
	for sectionNumber := 0; sectionNumber <= int(a.identity.lastSectionNumber); sectionNumber++ {
		if _, exists := a.sections[byte(sectionNumber)]; !exists {
			return false
		}
	}
	return true
}

type activePATProgram struct {
	programNumber int
	programOrder  int
}

type activePATTable struct {
	identity  patTableIdentity
	safeStart int
	sections  [][]byte
	programs  map[int]activePATProgram // PMT PID -> declared program
	synthetic bool
}

func (a *patTableAccumulator) activate() (activePATTable, bool) {
	table := activePATTable{
		identity:  a.identity,
		safeStart: -1,
		sections:  make([][]byte, int(a.identity.lastSectionNumber)+1),
		programs:  make(map[int]activePATProgram),
	}
	programNumbers := make(map[int]struct{})
	order := 0
	for sectionNumber := 0; sectionNumber <= int(a.identity.lastSectionNumber); sectionNumber++ {
		section, exists := a.sections[byte(sectionNumber)]
		if !exists {
			return activePATTable{}, false
		}
		if table.safeStart < 0 || section.start < table.safeStart {
			table.safeStart = section.start
		}
		table.sections[sectionNumber] = append([]byte(nil), section.data...)
		for _, program := range section.programs {
			if _, duplicate := programNumbers[program.programNumber]; duplicate {
				return activePATTable{}, false
			}
			if _, duplicate := table.programs[program.pmtPID]; duplicate {
				return activePATTable{}, false
			}
			programNumbers[program.programNumber] = struct{}{}
			order++
			table.programs[program.pmtPID] = activePATProgram{
				programNumber: program.programNumber,
				programOrder:  order,
			}
		}
	}
	return table, table.safeStart >= 0
}

func psiVersionIsNewer(candidate, active byte) bool {
	delta := (candidate - active) & 0x1f
	return delta > 0 && delta < 16
}

func patSectionCanSupersede(section parsedPATSection, active *activePATTable) bool {
	if active == nil {
		return true
	}
	if section.transportStreamID != active.identity.transportStreamID {
		return true
	}
	return psiVersionIsNewer(section.version, active.identity.version)
}

type tsVideoProgram struct {
	safeStart    int
	pmtStart     int
	mediaStart   int
	videoPID     int
	audioPIDs    [maxAVTimelineAudioPIDs]int
	audioCount   int
	pcrPID       int
	programOrder int
	psiPrefix    []byte
	mapIdentity  selectedProgramMapIdentity
}

// selectedProgramMapIdentity is the exact current PAT table and selected PMT
// that produced the startup-validated programme. Raw section bytes include
// version, descriptors, PID bindings, and CRC, so a byte-for-byte repetition
// is harmless while any complete CRC-valid change forces a fresh private gate.
type selectedProgramMapIdentity struct {
	transportStreamID uint16
	patVersion        byte
	patSections       [][]byte
	programNumber     int
	pmtPID            int
	pmtSection        []byte
}

func (m selectedProgramMapIdentity) valid() bool {
	return len(m.patSections) > 0 && len(m.pmtSection) > 0 &&
		m.pmtPID >= 0 && m.pmtPID < 0x1fff && m.programNumber > 0
}

func (m selectedProgramMapIdentity) clone() selectedProgramMapIdentity {
	cloned := m
	cloned.patSections = make([][]byte, len(m.patSections))
	for i := range m.patSections {
		cloned.patSections[i] = append([]byte(nil), m.patSections[i]...)
	}
	cloned.pmtSection = append([]byte(nil), m.pmtSection...)
	return cloned
}

type activePMTProgram struct {
	version   byte
	section   []byte
	program   tsVideoProgram
	have      bool
	synthetic bool
}

// scanTSProgramMaps activates a PAT epoch only after every current section from
// section_number zero through last_section_number has been assembled. A newer
// complete epoch atomically retires prior PMT assemblers and candidates, then
// requires a fresh matching PMT before media can validate. Ordinary prefixes
// retain the original complete PAT/PMT and every accepted byte. If a PMT changes
// inside one PAT epoch, the exact current PAT and selected PMT sections are
// replayed ahead of only fresh media; delayed stale tables advance that barrier.
// PSI buffers remain bounded per PID by maxPSISectionLength. Each declared
// program retains only its newest complete current PMT under MPEG's 5-bit
// version ordering; rollover supersedes and stale arrivals do not. The pending
// PAT table is bounded to 256 sections and the PMT PID namespace to 13 bits.
func scanTSProgramMaps(data []byte, codec string) []tsVideoProgram {
	patAssembler := &psiSectionAssembler{start: -1}
	var pendingPAT *patTableAccumulator
	var activePAT *activePATTable
	pmtAssemblers := make(map[int]*psiSectionAssembler)
	activePMTs := make(map[int]activePMTProgram)
	for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] != 0x47 {
			return nil
		}
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if tsPacketHasDiscontinuity(packet) {
			for pmtPID, current := range activePMTs {
				if !current.have || pid != current.program.videoPID {
					continue
				}
				current.synthetic = true
				// The discontinuity packet belongs to the new epoch. Retain it so
				// payload-bearing packets can contribute fresh configuration and
				// adaptation-only packets keep their explicit clock boundary.
				current.program.safeStart = pos
				current.program.mediaStart = pos
				activePMTs[pmtPID] = current
			}
		}
		if pid == 0 {
			for _, section := range patAssembler.push(packet, pos) {
				parsed, parsedOK := parsePATSection(section.data)
				if !parsedOK || !parsed.current {
					continue
				}
				if activePAT != nil &&
					parsed.transportStreamID == activePAT.identity.transportStreamID &&
					parsed.version == activePAT.identity.version {
					continue // same-version repetition cannot erase a valid candidate
				}
				if !patSectionCanSupersede(parsed, activePAT) {
					continue
				}
				if pendingPAT == nil || !pendingPAT.matches(parsed) {
					if pendingPAT != nil &&
						parsed.transportStreamID == pendingPAT.identity.transportStreamID &&
						!psiVersionIsNewer(parsed.version, pendingPAT.identity.version) {
						continue
					}
					pendingPAT = newPATTableAccumulator(parsed)
				}
				if !pendingPAT.add(parsed, section.start, section.data) {
					continue
				}
				activated, activateOK := pendingPAT.activate()
				pendingPAT = nil
				if !activateOK {
					continue
				}
				activated.synthetic = activePAT != nil
				activePAT = &activated
				activePMTs = make(map[int]activePMTProgram, len(activePAT.programs))
				pmtAssemblers = make(map[int]*psiSectionAssembler, len(activePAT.programs))
				for pmtPID := range activePAT.programs {
					pmtAssemblers[pmtPID] = &psiSectionAssembler{start: -1}
				}
			}
			continue
		}
		pmtAssembler, tracked := pmtAssemblers[pid]
		if !tracked {
			continue
		}
		for _, section := range pmtAssembler.push(packet, pos) {
			parsedPMT, parsedOK := parsePMTForCodecSection(section.data, codec)
			if !parsedOK {
				continue
			}
			if activePAT == nil {
				continue
			}
			pat, declared := activePAT.programs[pid]
			if !declared || parsedPMT.programNumber != pat.programNumber {
				continue
			}
			prior, exists := activePMTs[pid]
			if exists {
				if parsedPMT.version == prior.version {
					if bytes.Equal(section.data, prior.section) {
						continue
					}
					// A broadcaster must not change PMT contents without changing
					// its version. Keep the first complete current table, exclude
					// the conflicting repeat, and require fresh media after it.
					if prior.have {
						prior.synthetic = true
						prior.program.safeStart = pos + tsPacketSize
						prior.program.mediaStart = pos + tsPacketSize
						activePMTs[pid] = prior
					}
					continue
				}
				if !psiVersionIsNewer(parsedPMT.version, prior.version) {
					// Delayed stale PMTs are not part of the active program map,
					// but they also cannot be released after the synthesized current
					// table. Advance the barrier and require fresh configuration and
					// random access after the stale table.
					if prior.have {
						prior.synthetic = true
						prior.program.safeStart = pos + tsPacketSize
						prior.program.mediaStart = pos + tsPacketSize
						activePMTs[pid] = prior
					}
					continue
				}
			}
			current := activePMTProgram{
				version: parsedPMT.version,
				section: append([]byte(nil), section.data...),
				synthetic: exists || activePAT.synthetic ||
					hasSelectedProgramPacketBetween(data, parsedPMT, activePAT.safeStart, pos+tsPacketSize),
			}
			if parsedPMT.videoPID >= 0 {
				current.have = true
				current.program = tsVideoProgram{
					safeStart: activePAT.safeStart, pmtStart: section.start, mediaStart: pos + tsPacketSize,
					videoPID: parsedPMT.videoPID, audioPIDs: parsedPMT.audioPIDs,
					audioCount: parsedPMT.audioCount, pcrPID: parsedPMT.pcrPID,
					programOrder: pat.programOrder,
				}
				if current.synthetic {
					current.program.safeStart = pos + tsPacketSize
				}
			}
			activePMTs[pid] = current
		}
	}
	programs := make([]tsVideoProgram, 0, len(activePMTs))
	for pmtPID, current := range activePMTs {
		if current.have {
			program := current.program
			patProgram, declared := activePAT.programs[pmtPID]
			if !declared {
				continue
			}
			program.mapIdentity = selectedProgramMapIdentity{
				transportStreamID: activePAT.identity.transportStreamID,
				patVersion:        activePAT.identity.version,
				patSections:       make([][]byte, len(activePAT.sections)),
				programNumber:     patProgram.programNumber,
				pmtPID:            pmtPID,
				pmtSection:        append([]byte(nil), current.section...),
			}
			for i := range activePAT.sections {
				program.mapIdentity.patSections[i] = append([]byte(nil), activePAT.sections[i]...)
			}
			if current.synthetic {
				patContinuity, patContinuityOK := lastPayloadContinuityBefore(data, 0, program.safeStart)
				pmtContinuity, pmtContinuityOK := lastPayloadContinuityBefore(data, pmtPID, program.safeStart)
				if !patContinuityOK || !pmtContinuityOK {
					continue
				}
				program.psiPrefix = packetizePSISections(0, activePAT.sections, patContinuity)
				program.psiPrefix = append(program.psiPrefix,
					packetizePSISections(pmtPID, [][]byte{current.section}, pmtContinuity)...)
			}
			programs = append(programs, program)
		}
	}
	sort.SliceStable(programs, func(i, j int) bool {
		if programs[i].programOrder != programs[j].programOrder {
			return programs[i].programOrder < programs[j].programOrder
		}
		return programs[i].pmtStart < programs[j].pmtStart
	})
	return programs
}

func tsPacketHasDiscontinuity(packet []byte) bool {
	if len(packet) != tsPacketSize || packet[0] != 0x47 {
		return false
	}
	control := (packet[3] >> 4) & 0x03
	return (control == 2 || control == 3) && packet[4] >= 1 && packet[5]&0x80 != 0
}

func hasTSPayloadPIDBetween(data []byte, pid, start, end int) bool {
	if pid < 0 || pid > 0x1fff {
		return false
	}
	start = max(0, start-start%tsPacketSize)
	end = min(len(data), end)
	for pos := start; pos+tsPacketSize <= end; pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] != 0x47 {
			return true
		}
		packetPID := int(packet[1]&0x1f)<<8 | int(packet[2])
		control := (packet[3] >> 4) & 0x03
		if packetPID == pid && (control == 1 || control == 3) {
			return true
		}
	}
	return false
}

func hasSelectedProgramPacketBetween(data []byte, program parsedPMTSection, start, end int) bool {
	if hasTSPIDBetween(data, program.videoPID, start, end) {
		return true
	}
	for index := 0; index < min(program.audioCount, maxAVTimelineAudioPIDs); index++ {
		if hasTSPIDBetween(data, program.audioPIDs[index], start, end) {
			return true
		}
	}
	return program.pcrPID != program.videoPID &&
		hasTSPIDBetween(data, program.pcrPID, start, end)
}

func hasTSPIDBetween(data []byte, pid, start, end int) bool {
	if pid < 0 || pid > 0x1fff {
		return false
	}
	start = max(0, start-start%tsPacketSize)
	end = min(len(data), end)
	for pos := start; pos+tsPacketSize <= end; pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] != 0x47 {
			return true
		}
		packetPID := int(packet[1]&0x1f)<<8 | int(packet[2])
		if packetPID == pid {
			return true
		}
	}
	return false
}

func lastPayloadContinuityBefore(data []byte, pid, end int) (byte, bool) {
	limit := min(len(data), end)
	var continuity byte
	found := false
	for pos := 0; pos+tsPacketSize <= limit; pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		if packet[0] != 0x47 {
			return 0, false
		}
		packetPID := int(packet[1]&0x1f)<<8 | int(packet[2])
		control := (packet[3] >> 4) & 0x03
		if packetPID == pid && (control == 1 || control == 3) {
			continuity = packet[3] & 0x0f
			found = true
		}
	}
	return continuity, found
}

func psiSectionPacketCount(sectionLength int) int {
	if sectionLength <= 0 {
		return 0
	}
	if sectionLength <= tsPacketSize-5 { // TS header plus pointer_field
		return 1
	}
	remaining := sectionLength - (tsPacketSize - 5)
	return 1 + (remaining+tsPacketSize-5)/(tsPacketSize-4)
}

// packetizePSISections creates a compact transport prefix from already
// validated complete PSI sections, retaining their original CRC bytes. Its
// final continuity counter matches the last source packet dropped before the
// release barrier, so the next retained PAT/PMT repetition stays continuous.
func packetizePSISections(pid int, sections [][]byte, endingContinuity byte) []byte {
	packetCount := 0
	for _, section := range sections {
		packetCount += psiSectionPacketCount(len(section))
	}
	if packetCount == 0 {
		return nil
	}
	continuity := byte((int(endingContinuity) - packetCount + 1) & 0x0f)
	out := make([]byte, 0, packetCount*tsPacketSize)
	for _, section := range sections {
		remaining := section
		first := true
		for len(remaining) > 0 {
			packet := bytes.Repeat([]byte{0xff}, tsPacketSize)
			packet[0] = 0x47
			packet[1] = byte(pid>>8) & 0x1f
			packet[2] = byte(pid)
			packet[3] = 0x10 | continuity
			payloadStart := 4
			if first {
				packet[1] |= 0x40
				packet[payloadStart] = 0 // pointer_field
				payloadStart++
			}
			take := min(len(remaining), tsPacketSize-payloadStart)
			copy(packet[payloadStart:], remaining[:take])
			remaining = remaining[take:]
			out = append(out, packet...)
			continuity = (continuity + 1) & 0x0f
			first = false
		}
	}
	return out
}

func scanTSProgramMap(data []byte) (safeStart, videoPID int, ok bool) {
	programs := scanTSProgramMaps(data, "")
	if len(programs) == 0 {
		return -1, -1, false
	}
	return programs[0].safeStart, programs[0].videoPID, true
}

func videoPIDFromTS(data []byte) (int, bool) {
	syncOffset, ok := findTSSyncOffset(data)
	if !ok {
		return 0, false
	}
	_, videoPID, ok := scanTSProgramMap(data[syncOffset:])
	return videoPID, ok
}

func parsePMTVideoPID(packet []byte) (int, bool) {
	payload, ok := tsPayload(packet)
	if !ok || len(payload) < 13 {
		return 0, false
	}
	pointer := int(payload[0])
	if 1+pointer+12 > len(payload) {
		return 0, false
	}
	return parsePMTVideoPIDSection(payload[1+pointer:])
}

func parsePMTVideoPIDSection(section []byte) (int, bool) {
	parsed, ok := parsePMTForCodecSection(section, "")
	return parsed.videoPID, ok && parsed.videoPID >= 0
}

type parsedPMTSection struct {
	programNumber int
	version       byte
	videoPID      int
	audioPIDs     [maxAVTimelineAudioPIDs]int
	audioCount    int
	pcrPID        int
}

func parsePMTForCodecSection(section []byte, codec string) (parsedPMTSection, bool) {
	if len(section) < 12 || section[0] != 0x02 {
		return parsedPMTSection{}, false
	}
	sectionLength := int(section[1]&0x0f)<<8 | int(section[2])
	total := 3 + sectionLength
	end := total - 4 // exclude CRC32
	if end > len(section) || end < 12 {
		return parsedPMTSection{}, false
	}
	if total > len(section) || !mpeg2PSICRCValid(section[:total]) {
		return parsedPMTSection{}, false
	}
	if section[5]&0x01 == 0 || section[6] != 0 || section[7] != 0 {
		return parsedPMTSection{}, false
	}
	programInfoLength := int(section[10]&0x0f)<<8 | int(section[11])
	if 12+programInfoLength > end {
		return parsedPMTSection{}, false
	}
	parsed := parsedPMTSection{
		programNumber: int(section[3])<<8 | int(section[4]),
		version:       (section[5] >> 1) & 0x1f,
		videoPID:      -1,
		pcrPID:        int(section[8]&0x1f)<<8 | int(section[9]),
	}
	for i := range parsed.audioPIDs {
		parsed.audioPIDs[i] = -1
	}
	for pos := 12 + programInfoLength; pos < end; {
		if pos+5 > end {
			return parsedPMTSection{}, false
		}
		streamType := section[pos]
		pid := int(section[pos+1]&0x1f)<<8 | int(section[pos+2])
		esInfoLength := int(section[pos+3]&0x0f)<<8 | int(section[pos+4])
		if pos+5+esInfoLength > end {
			return parsedPMTSection{}, false
		}
		if parsed.videoPID < 0 && streamTypeMatchesCodec(streamType, codec) {
			parsed.videoPID = pid
		}
		if isAudioStreamType(streamType, section[pos+5:pos+5+esInfoLength]) {
			if parsed.audioCount < maxAVTimelineAudioPIDs {
				parsed.audioPIDs[parsed.audioCount] = pid
			}
			parsed.audioCount++
		}
		pos += 5 + esInfoLength
	}
	return parsed, true
}

func streamTypeMatchesCodec(streamType byte, codec string) bool {
	switch codec {
	case "h264":
		return streamType == 0x1b
	case "hevc":
		return streamType == 0x24
	default:
		switch streamType {
		case 0x01, 0x02, 0x10, 0x1b, 0x24:
			return true
		default:
			return false
		}
	}
}

type annexBRandomAccessScanner struct {
	codec             string
	zeroes            int
	wantHeader        bool
	inNAL             bool
	nalType           byte
	nalBytes          int
	nalAtPES          int
	haveVPS           bool
	haveSPS           bool
	havePPS           bool
	vpsAtPES          int
	spsAtPES          int
	ppsAtPES          int
	randomAccessAtPES int
	firstVCLSeen      bool
	firstVCLReady     bool
}

func (s *annexBRandomAccessScanner) finishNAL() {
	if !s.inNAL {
		return
	}
	minimumBytes := 2 // one-byte AVC header plus at least one RBSP byte
	if s.codec == "hevc" {
		minimumBytes = 3 // two-byte HEVC header plus at least one RBSP byte
	}
	if s.nalBytes >= minimumBytes {
		switch {
		case s.codec == "hevc" && s.nalType == 32:
			s.haveVPS = true
			s.vpsAtPES = s.nalAtPES
		case s.codec == "hevc" && s.nalType == 33:
			s.haveSPS = true
			s.spsAtPES = s.nalAtPES
		case s.codec == "hevc" && s.nalType == 34:
			s.havePPS = true
			s.ppsAtPES = s.nalAtPES
		case s.codec != "hevc" && s.nalType == 7:
			s.haveSPS = true
			s.spsAtPES = s.nalAtPES
		case s.codec != "hevc" && s.nalType == 8:
			s.havePPS = true
			s.ppsAtPES = s.nalAtPES
		}
	}
	s.inNAL = false
	s.nalBytes = 0
}

func (s *annexBRandomAccessScanner) feed(data []byte, pesStart int) bool {
	for _, b := range data {
		if s.wantHeader {
			s.wantHeader = false
			s.zeroes = 0
			s.inNAL = true
			s.nalBytes = 1
			s.nalAtPES = pesStart
			if s.codec == "hevc" {
				s.nalType = (b >> 1) & 0x3f
				if s.nalType <= 31 && !s.firstVCLSeen {
					s.firstVCLSeen = true
					s.firstVCLReady = s.nalType >= 16 && s.nalType <= 23 && s.haveVPS && s.haveSPS && s.havePPS
				}
				if s.nalType >= 16 && s.nalType <= 23 && s.haveVPS && s.haveSPS && s.havePPS {
					s.randomAccessAtPES = pesStart
					return true
				}
			} else {
				s.nalType = b & 0x1f
				if s.nalType >= 1 && s.nalType <= 5 && !s.firstVCLSeen {
					s.firstVCLSeen = true
					s.firstVCLReady = s.nalType == 5 && s.haveSPS && s.havePPS
				}
				if s.nalType == 5 && s.haveSPS && s.havePPS {
					s.randomAccessAtPES = pesStart
					return true
				}
			}
			continue
		}
		if b == 0 {
			s.zeroes++
			continue
		}
		if b == 1 && s.zeroes >= 2 {
			s.finishNAL()
			s.wantHeader = true
			s.zeroes = 0
			continue
		}
		if s.inNAL {
			s.nalBytes += s.zeroes + 1
		}
		s.zeroes = 0
	}
	return false
}

type programRandomAccessScanner struct {
	program          tsVideoProgram
	nal              annexBRandomAccessScanner
	pesStart         int
	pendingPESHeader []byte
	haveContinuity   bool
	continuity       byte
	configurationAt  int
	randomAccessAt   int
}

func newProgramRandomAccessScanner(program tsVideoProgram, codec string) *programRandomAccessScanner {
	return &programRandomAccessScanner{
		program: program,
		nal: annexBRandomAccessScanner{
			codec: codec, vpsAtPES: -1, spsAtPES: -1, ppsAtPES: -1, randomAccessAtPES: -1,
		},
		pesStart: -1, configurationAt: -1, randomAccessAt: -1,
	}
}

func (s *programRandomAccessScanner) reset(codec string) {
	seen, ready := s.nal.firstVCLSeen, s.nal.firstVCLReady
	s.nal = annexBRandomAccessScanner{
		codec: codec, vpsAtPES: -1, spsAtPES: -1, ppsAtPES: -1, randomAccessAtPES: -1,
		firstVCLSeen: seen, firstVCLReady: ready,
	}
	s.pesStart = -1
	s.pendingPESHeader = nil
	s.haveContinuity = false
	s.configurationAt = -1
	s.randomAccessAt = -1
}

func (s *programRandomAccessScanner) push(packet []byte, pos int, codec string) {
	if s.randomAccessAt >= 0 || pos < s.program.mediaStart {
		return
	}
	if len(packet) != tsPacketSize || packet[0] != 0x47 ||
		packet[1]&0x80 != 0 || packet[3]&0xc0 != 0 {
		s.reset(codec)
		return
	}
	adaptationControl := (packet[3] >> 4) & 0x03
	if tsPacketHasDiscontinuity(packet) {
		s.reset(codec)
	}
	if adaptationControl == 0 || adaptationControl == 2 {
		return
	}
	continuity := packet[3] & 0x0f
	if s.haveContinuity {
		if continuity == s.continuity {
			return // legal duplicate payload packet
		}
		if continuity != (s.continuity+1)&0x0f {
			s.reset(codec)
		}
	}
	s.haveContinuity = true
	s.continuity = continuity
	payload, payloadOK := tsPayload(packet)
	if !payloadOK {
		return
	}
	if packet[1]&0x40 != 0 {
		s.pesStart = pos
		s.pendingPESHeader = append(s.pendingPESHeader[:0], payload...)
	} else if s.pendingPESHeader != nil {
		s.pendingPESHeader = append(s.pendingPESHeader, payload...)
	}
	if s.pendingPESHeader != nil {
		if len(s.pendingPESHeader) < 9 {
			return
		}
		if s.pendingPESHeader[0] != 0 || s.pendingPESHeader[1] != 0 || s.pendingPESHeader[2] != 1 {
			s.reset(codec)
			return
		}
		headerEnd := 9 + int(s.pendingPESHeader[8])
		if len(s.pendingPESHeader) < headerEnd {
			return
		}
		payload = s.pendingPESHeader[headerEnd:]
		s.pendingPESHeader = nil
	}
	if s.pesStart < 0 || !s.nal.feed(payload, s.pesStart) {
		return
	}
	s.configurationAt = min(s.nal.spsAtPES, s.nal.ppsAtPES)
	if codec == "hevc" {
		s.configurationAt = min(s.configurationAt, s.nal.vpsAtPES)
	}
	s.randomAccessAt = s.nal.randomAccessAtPES
}

// findConfiguredRandomAccessNALProgram proves transport-level random access
// rather than trusting ffprobe's broader key_frame flag (which also marks
// non-IDR open-GOP I frames). Only the exact ffprobe-selected TS PID may
// contribute configuration or random access; another same-codec program is
// never a fallback. The PID is returned to keep the subsequent safe-prefix
// proof bound to the same PAT/PMT mapping.
func findConfiguredRandomAccessNALProgram(data []byte, codec string, selectedVideoPID int) (configurationAt, randomAccessAt, videoPID int) {
	programs := scanTSProgramMaps(data, codec)
	if len(programs) == 0 || selectedVideoPID < 0 || selectedVideoPID > 0x1fff {
		return -1, -1, -1
	}
	var scanner *programRandomAccessScanner
	for _, program := range programs {
		if program.videoPID == selectedVideoPID {
			scanner = newProgramRandomAccessScanner(program, codec)
			break
		}
	}
	if scanner == nil {
		return -1, -1, -1
	}
	for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		packet := data[pos : pos+tsPacketSize]
		pid := int(packet[1]&0x1f)<<8 | int(packet[2])
		if pid == selectedVideoPID {
			scanner.push(packet, pos, codec)
		}
	}
	if scanner.randomAccessAt >= 0 {
		return scanner.configurationAt, scanner.randomAccessAt, selectedVideoPID
	}
	return -1, -1, -1
}

func findConfiguredRandomAccessNAL(data []byte, codec string) (configurationAt, randomAccessAt int) {
	videoPID, ok := videoPIDFromTS(data)
	if !ok {
		return -1, -1
	}
	configurationAt, randomAccessAt, _ = findConfiguredRandomAccessNALProgram(data, codec, videoPID)
	return configurationAt, randomAccessAt
}
