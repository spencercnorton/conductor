package dvr

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/transcode"
)

const (
	mediaGapThreshold      = time.Second
	mediaTimestampBackstep = 100 * time.Millisecond
	mediaProcessTimeout    = 30 * time.Minute
	maxToolDiagnosticBytes = 16 * 1024

	defaultArtifactPlaceholderMin = 9 * time.Minute
	defaultArtifactPlaceholderMax = 11 * time.Minute
	artifactSampleWidth           = 32
	artifactSampleHeight          = 18
	artifactFramesPerSeek         = 3
	artifactAudioRate             = 8000
	artifactAudioWindow           = 250 * time.Millisecond
	// Classifier work is bounded. An unusual topology is kept as valid media
	// because incomplete silence evidence can never justify deletion.
	maxArtifactAudioStreams  = 4
	maxArtifactAudioChannels = 8

	// artifactScanThreads bounds the full-decode bitstream scan so a
	// finalizing recording cannot starve live relay pacing on the same host.
	// Measured on the production container (ffmpeg 4.4, 8 threads): ~20-25x
	// realtime for 1080p H.264, so even a 3h recording fits the shared
	// mediaProcessTimeout alongside normalization.
	artifactScanThreads = 8
)

// artifactBitstreamCorruptionMarkers are the decoder log classes that only a
// damaged H.264 elementary stream produces — the mid-GOP splice signature that
// packet-clock validation cannot see (proven 2026-08-30: a storm capture with
// max DTS gap 17ms carried a dozen corrupt slices). Decoder-state grumbles
// ("co located POCs unavailable", "mmco: unref short failure") and null-muxer
// DTS complaints are deliberately absent: both were measured on cleanly
// decoding production artifacts and on seeks into pristine media.
var artifactBitstreamCorruptionMarkers = []string{
	"error while decoding",
	"decode_slice_header error",
	"no frame!",
	"concealing",
	"non-existing PPS",
	"non-existing SPS",
	"Invalid NAL unit",
}

var (
	errArtifactPublishRecoveryRequired = errors.New("DVR artifact publish requires recovery")
	errArtifactPublishConflict         = errors.New("DVR artifact canonical changed during publish")
)

// MediaReport is derived from packet timelines after normalization. MPEG-TS
// format duration is deliberately absent: a PTS reset can make that value span
// hours even when only minutes of media exist.
type MediaReport struct {
	Duration        time.Duration
	VideoStart      time.Duration
	VideoEnd        time.Duration
	AudioStart      time.Duration
	AudioEnd        time.Duration
	MaxGap          time.Duration
	Discontinuities int
	AVStartDelta    time.Duration
	AVEndDelta      time.Duration
	AVDrift         time.Duration
	ArtifactBytes   int64
	SHA256          string
	FillerReason    string
	// BitstreamErrors is how many decoder-corruption events the full-decode
	// scan observed in the normalized video stream (0 when the scan is
	// disabled). ffmpeg collapses repeated identical lines, so extreme damage
	// can undercount — irrelevant for the small thresholds this gate uses.
	BitstreamErrors int
}

type MediaProcessor interface {
	NormalizeAndValidate(context.Context, string, string, time.Duration) (MediaReport, error)
}

type ffmpegMediaProcessor struct {
	ffmpeg                string
	ffprobe               string
	placeholderMin        time.Duration
	placeholderMax        time.Duration
	placeholderSampling   bool
	placeholderClassifier func(context.Context, string, string, time.Duration) (bool, string, error)
	// scanMaxDecodeErrors is the bitstream-scan tolerance: more decoder
	// corruption events than this rejects the artifact. Negative disables the
	// scan entirely — the documented escape hatch if an ffmpeg upgrade ever
	// changes decoder logging and mass-rejects clean recordings.
	scanMaxDecodeErrors int
}

func newFFmpegMediaProcessor(ffmpeg string) *ffmpegMediaProcessor {
	if ffmpeg == "" {
		ffmpeg = "ffmpeg"
	}
	return &ffmpegMediaProcessor{
		ffmpeg:              ffmpeg,
		ffprobe:             companionArtifactFFprobe(ffmpeg),
		placeholderMin:      defaultArtifactPlaceholderMin,
		placeholderMax:      defaultArtifactPlaceholderMax,
		placeholderSampling: true,
	}
}

// NewFFmpegMediaProcessor is the production constructor. maxDecodeErrors sets
// the bitstream-scan tolerance (0 = any decoder corruption rejects, matching
// the standing DVR rule; negative disables the scan).
func NewFFmpegMediaProcessor(ffmpeg string, maxDecodeErrors int) MediaProcessor {
	p := newFFmpegMediaProcessor(ffmpeg)
	p.scanMaxDecodeErrors = maxDecodeErrors
	return p
}

func companionArtifactFFprobe(ffmpeg string) string {
	if ffmpeg == "" || ffmpeg == "ffmpeg" {
		return "ffprobe"
	}
	base := filepath.Base(ffmpeg)
	if strings.HasSuffix(base, "ffmpeg") {
		candidate := filepath.Join(filepath.Dir(ffmpeg),
			strings.TrimSuffix(base, "ffmpeg")+"ffprobe")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "ffprobe"
}

// NormalizeAndValidate writes a fresh candidate and proves its packet clocks
// before publication. Video remains bit-for-bit encoded; audio is decoded and
// re-encoded so its clock is rebuilt from sample count instead of perpetuating
// the input's slope or backward resets.
func (p *ffmpegMediaProcessor) NormalizeAndValidate(
	ctx context.Context,
	input, output string,
	expected time.Duration,
) (MediaReport, error) {
	if expected <= 0 {
		return MediaReport{}, errors.New("recording has a non-positive scheduled duration")
	}
	if _, err := exec.LookPath(p.ffmpeg); err != nil {
		return MediaReport{}, fmt.Errorf("ffmpeg unavailable: %w", err)
	}
	if _, err := exec.LookPath(p.ffprobe); err != nil {
		return MediaReport{}, fmt.Errorf("ffprobe unavailable: %w", err)
	}
	if _, err := os.Lstat(output); err == nil {
		return MediaReport{}, errors.New("normalized candidate already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return MediaReport{}, fmt.Errorf("inspect normalized candidate: %w", err)
	}

	processCtx, cancel := context.WithTimeout(ctx, mediaProcessTimeout)
	defer cancel()
	cmd := exec.CommandContext(processCtx, p.ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-n",
		"-fflags", "+genpts+discardcorrupt", "-err_detect", "ignore_err",
		"-dts_delta_threshold", "0.5",
		"-i", input,
		// Publish only the primary audio track: it is the track whose packet
		// coverage and repaired clock are proven below. Exposing secondary tracks
		// without independently validating each one would let Plex select media
		// that can still drift even though the artifact was marked validated.
		"-map", "0:v:0", "-map", "0:a:0", "-map", "0:s?",
		"-c:v", "copy", "-c:a", "aac", "-b:a", "256k",
		"-af", transcode.RepairAudioClockFilter(), "-c:s", "copy",
		"-avoid_negative_ts", "make_zero",
		"-mpegts_flags", "+resend_headers+initial_discontinuity",
		"-pcr_period", "20", "-muxdelay", "0", "-muxpreload", "0",
		"-f", "mpegts", output,
	)
	var diagnostic limitedDiagnostic
	cmd.Stdout = io.Discard
	cmd.Stderr = &diagnostic
	if err := cmd.Run(); err != nil {
		return MediaReport{}, fmt.Errorf("normalize recording: %w: %s", err, diagnostic.String())
	}
	info, err := os.Stat(output)
	if err != nil {
		return MediaReport{}, fmt.Errorf("inspect normalized recording: %w", err)
	}
	if info.Size() <= 0 {
		return MediaReport{}, errors.New("normalized output is empty")
	}

	video, err := p.probeTimeline(processCtx, output, "v:0")
	if err != nil {
		return MediaReport{}, fmt.Errorf("probe normalized video: %w", err)
	}
	audio, err := p.probeTimeline(processCtx, output, "a:0")
	if err != nil {
		return MediaReport{}, fmt.Errorf("probe normalized audio: %w", err)
	}
	if video.count < 2 || audio.count < 2 {
		return MediaReport{}, fmt.Errorf(
			"normalized media lacks packet coverage: video=%d audio=%d",
			video.count, audio.count)
	}

	start := max(video.first, audio.first)
	end := min(video.lastEnd, audio.lastEnd)
	duration := max(time.Duration(0), end-start)
	startOffset := audio.first - video.first
	endOffset := audio.lastEnd - video.lastEnd
	report := MediaReport{
		Duration:        duration,
		VideoStart:      video.first,
		VideoEnd:        video.lastEnd,
		AudioStart:      audio.first,
		AudioEnd:        audio.lastEnd,
		MaxGap:          max(video.maxGap, audio.maxGap),
		Discontinuities: video.discontinuities + audio.discontinuities,
		AVStartDelta:    absDuration(startOffset),
		AVEndDelta:      absDuration(endOffset),
		AVDrift:         absDuration(endOffset - startOffset),
		ArtifactBytes:   info.Size(),
	}

	coverageTolerance := min(max(10*time.Second, expected/200), 30*time.Second)
	minimumCoverage := expected - coverageTolerance
	if expected < 20*time.Second {
		minimumCoverage = expected * 4 / 5
	}
	const (
		avStartLimit = time.Second
		avEndLimit   = 2 * time.Second
		avDriftLimit = time.Second
	)
	switch {
	case duration < minimumCoverage:
		return report, fmt.Errorf("normalized media covers %s, need at least %s of %s",
			duration, minimumCoverage, expected)
	case report.Discontinuities != 0:
		return report, fmt.Errorf(
			"normalized media retains %d timestamp discontinuities (max gap %s)",
			report.Discontinuities, report.MaxGap)
	case report.AVStartDelta > avStartLimit:
		return report, fmt.Errorf("normalized A/V start offset is %s (limit %s)",
			report.AVStartDelta, avStartLimit)
	case report.AVEndDelta > avEndLimit:
		return report, fmt.Errorf("normalized A/V end offset is %s (limit %s)",
			report.AVEndDelta, avEndLimit)
	case report.AVDrift > avDriftLimit:
		return report, fmt.Errorf("normalized A/V drift is %s (limit %s)",
			report.AVDrift, avDriftLimit)
	}

	if p.placeholderSampling && duration >= p.placeholderMin && duration <= p.placeholderMax {
		classifyPlaceholder := p.uniformBlackSilent
		if p.placeholderClassifier != nil {
			classifyPlaceholder = p.placeholderClassifier
		}
		black, reason, err := classifyPlaceholder(processCtx, output, input, duration)
		if processCtx.Err() != nil {
			// Context expiry always wins the classifier return race. A callback
			// may observe cancellation and still return a normal false/true result;
			// neither outcome may publish a hash or convert the cancellation into
			// a filler verdict.
			return report, fmt.Errorf("classify normalized placeholder: %w", processCtx.Err())
		}
		if err != nil {
			// Placeholder rejection is an additional, high-confidence safety
			// decision. A decode/topology uncertainty must not discard otherwise
			// validated programme media. Cancellation still terminates the whole
			// normalization operation rather than publishing after its deadline.
		}
		if err == nil && black {
			report.FillerReason = reason
			return report, fmt.Errorf("normalized media is a high-confidence black placeholder: %s", reason)
		}
	}

	if p.scanMaxDecodeErrors >= 0 {
		// Video is published bit-for-bit (-c:v copy), so a mid-stream splice
		// that joined H.264 mid-GOP survives normalization with perfectly
		// continuous packet clocks. Only actually decoding the stream proves
		// the pictures are intact.
		corruption, sample, scanErr := p.scanVideoBitstream(processCtx, output)
		report.BitstreamErrors = corruption
		if corruption > p.scanMaxDecodeErrors {
			return report, fmt.Errorf(
				"normalized video bitstream is corrupt: %d decoder corruption events (tolerance %d): %s",
				corruption, p.scanMaxDecodeErrors, sample)
		}
		if scanErr != nil {
			// A decode that died part-way proved nothing about the tail. Within
			// tolerance is only a verdict when the whole stream was decoded.
			return report, fmt.Errorf("scan normalized video bitstream: %w", scanErr)
		}
	}

	hash, err := sha256File(output)
	if err != nil {
		return report, fmt.Errorf("hash normalized recording: %w", err)
	}
	report.SHA256 = hash
	return report, nil
}

type packetTimeline struct {
	count           int
	first           time.Duration
	lastEnd         time.Duration
	previous        time.Duration
	previousDur     time.Duration
	maxGap          time.Duration
	discontinuities int
	set             bool
}

func (p *ffmpegMediaProcessor) probeTimeline(
	ctx context.Context,
	path, selector string,
) (packetTimeline, error) {
	cmd := exec.CommandContext(ctx, p.ffprobe,
		"-v", "error", "-select_streams", selector,
		"-show_entries", "packet=pts_time,dts_time,duration_time",
		"-of", "compact=p=0:nk=1", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return packetTimeline{}, err
	}
	var diagnostic limitedDiagnostic
	cmd.Stderr = &diagnostic
	if err := cmd.Start(); err != nil {
		return packetTimeline{}, err
	}

	var timeline packetTimeline
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) < 2 {
			continue
		}
		pts, ptsOK := parseMediaSeconds(parts[0])
		dts, dtsOK := parseMediaSeconds(parts[1])
		stamp := dts
		if !dtsOK {
			stamp = pts
		}
		if !dtsOK && !ptsOK {
			continue
		}
		dur := time.Duration(0)
		if len(parts) > 2 {
			dur, _ = parseMediaSeconds(parts[2])
		}
		if dur < 0 || dur > time.Minute {
			dur = 0
		}
		if !timeline.set {
			timeline.first = stamp
			timeline.previous = stamp
			timeline.previousDur = dur
			timeline.lastEnd = stamp + dur
			timeline.set = true
			timeline.count++
			continue
		}
		if stamp < timeline.previous-mediaTimestampBackstep {
			timeline.discontinuities++
		}
		gap := stamp - (timeline.previous + timeline.previousDur)
		if gap > timeline.maxGap {
			timeline.maxGap = gap
		}
		if gap > mediaGapThreshold {
			timeline.discontinuities++
		}
		if packetEnd := stamp + dur; packetEnd > timeline.lastEnd {
			timeline.lastEnd = packetEnd
		}
		timeline.previous = stamp
		timeline.previousDur = dur
		timeline.count++
	}
	scanErr := scanner.Err()
	waitErr := cmd.Wait()
	if scanErr != nil {
		return packetTimeline{}, scanErr
	}
	if waitErr != nil {
		return packetTimeline{}, fmt.Errorf("ffprobe packets: %w: %s", waitErr, diagnostic.String())
	}
	return timeline, nil
}

// scanVideoBitstream fully decodes the normalized artifact's video stream to
// the null muxer and counts decoder log lines that only bitstream damage
// produces. Counting continues across all stderr while retained diagnostic
// text stays bounded. A non-zero ffmpeg exit means the decode did not cover
// the whole stream: the count observed so far is still returned (it may already exceed
// tolerance), together with an error so an under-tolerance partial scan can
// never pass as a verdict.
func (p *ffmpegMediaProcessor) scanVideoBitstream(
	ctx context.Context,
	path string,
) (int, string, error) {
	cmd := exec.CommandContext(ctx, p.ffmpeg,
		"-hide_banner", "-nostdin", "-v", "error",
		"-threads", strconv.Itoa(artifactScanThreads),
		"-i", path, "-map", "0:v:0", "-f", "null", "-")
	diagnostic := newBitstreamDiagnostic()
	cmd.Stdout = io.Discard
	cmd.Stderr = diagnostic
	runErr := cmd.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, "", ctxErr
	}
	corruption, sample := diagnostic.result()
	if runErr != nil {
		return corruption, sample, fmt.Errorf(
			"decode did not complete: %w: %s", runErr, diagnostic.String())
	}
	return corruption, sample, nil
}

func parseMediaSeconds(s string) (time.Duration, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "N/A" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return time.Duration(f * float64(time.Second)), true
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// uniformBlackSilent is deliberately narrow. It is only called for the known
// finite-placeholder duration band, and requires six distributed groups of
// identical near-black decoded frames plus near-silent decoded audio from
// every declared input track. Video is sampled from the normalized candidate,
// while audio is sampled from the captured input so a secondary programme
// track cannot be erased by primary-only publication before classification.
// Ordinary dark scenes, fades, credits, and black video carrying real audio
// are kept.
type artifactAudioStream struct {
	index    int
	channels int
}

type artifactMediaTopology struct {
	video    []int
	audio    []artifactAudioStream
	complete bool
}

func (p *ffmpegMediaProcessor) probeArtifactMediaTopology(
	ctx context.Context,
	path string,
) (artifactMediaTopology, error) {
	out, err := exec.CommandContext(ctx, p.ffprobe,
		"-v", "error",
		"-show_entries", "stream=index,codec_type,channels",
		"-of", "json", path,
	).Output()
	if err != nil {
		return artifactMediaTopology{}, err
	}
	return parseArtifactMediaTopology(out)
}

func parseArtifactMediaTopology(out []byte) (artifactMediaTopology, error) {
	var raw struct {
		Streams []struct {
			Index     *int   `json:"index"`
			CodecType string `json:"codec_type"`
			Channels  *int   `json:"channels"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return artifactMediaTopology{}, err
	}
	topology := artifactMediaTopology{
		video:    make([]int, 0, 1),
		audio:    make([]artifactAudioStream, 0, len(raw.Streams)),
		complete: len(raw.Streams) > 0,
	}
	seen := make(map[int]struct{}, len(raw.Streams))
	for _, stream := range raw.Streams {
		index := -1
		if stream.Index != nil {
			index = *stream.Index
		}
		if index < 0 {
			topology.complete = false
		} else if _, duplicate := seen[index]; duplicate {
			topology.complete = false
		} else {
			seen[index] = struct{}{}
		}
		switch stream.CodecType {
		case "video":
			topology.video = append(topology.video, index)
		case "audio":
			channels := 0
			if stream.Channels != nil {
				channels = *stream.Channels
			}
			topology.audio = append(topology.audio,
				artifactAudioStream{index: index, channels: channels})
		default:
			// Normalization may publish subtitles, and an input may contain
			// additional data-bearing streams. None is sampled here, so any such
			// topology makes placeholder classification indeterminate.
			topology.complete = false
		}
	}
	return topology, nil
}

func artifactMediaTopologySupported(topology artifactMediaTopology) bool {
	if !topology.complete || len(topology.video) != 1 || topology.video[0] < 0 ||
		!artifactAudioTopologySupported(topology.audio) {
		return false
	}
	for _, stream := range topology.audio {
		if stream.index == topology.video[0] {
			return false
		}
	}
	return true
}

func (p *ffmpegMediaProcessor) probeArtifactAudioStreams(
	ctx context.Context,
	path string,
) ([]artifactAudioStream, error) {
	topology, err := p.probeArtifactMediaTopology(ctx, path)
	if err != nil {
		return nil, err
	}
	if !artifactMediaTopologySupported(topology) {
		return nil, errors.New("media topology is outside the bounded placeholder classifier")
	}
	return topology.audio, nil
}

func artifactAudioTopologySupported(streams []artifactAudioStream) bool {
	if len(streams) == 0 || len(streams) > maxArtifactAudioStreams {
		return false
	}
	seen := make(map[int]struct{}, len(streams))
	for _, stream := range streams {
		if stream.index < 0 || stream.channels < 1 ||
			stream.channels > maxArtifactAudioChannels {
			return false
		}
		if _, duplicate := seen[stream.index]; duplicate {
			return false
		}
		seen[stream.index] = struct{}{}
	}
	return true
}

func artifactAudioWindowBytes(channels int) int {
	samplesPerChannel := int(artifactAudioWindow * artifactAudioRate / time.Second)
	return samplesPerChannel * channels * 2
}

func artifactAudioWindowComplete(pcm []byte, channels int) bool {
	return channels >= 1 && channels <= maxArtifactAudioChannels &&
		len(pcm) == artifactAudioWindowBytes(channels)
}

func (p *ffmpegMediaProcessor) uniformBlackSilent(
	ctx context.Context,
	videoPath, audioPath string,
	duration time.Duration,
) (bool, string, error) {
	audioStreams, err := p.probeArtifactAudioStreams(ctx, audioPath)
	if err != nil {
		return false, "", err
	}
	positions := []float64{0.05, 0.20, 0.40, 0.60, 0.80, 0.95}
	frameSize := artifactSampleWidth * artifactSampleHeight
	var firstHash [sha256.Size]byte
	haveHash := false
	for _, fraction := range positions {
		at := time.Duration(float64(duration) * fraction)
		// Leave enough decode runway on both sides of the seek. The production
		// 10-minute placeholder has ample room; the clamp also keeps short
		// executable fixtures from landing before the first TS keyframe or at EOF.
		if at < time.Second {
			at = time.Second
		}
		if latest := duration - time.Second; at > latest {
			at = max(time.Duration(0), latest)
		}
		frames, err := exec.CommandContext(ctx, p.ffmpeg,
			"-v", "error", "-ss", formatFFmpegSeconds(at), "-i", videoPath,
			"-map", "0:v:0", "-frames:v", strconv.Itoa(artifactFramesPerSeek),
			"-vf", fmt.Sprintf("scale=%d:%d:flags=area,format=gray",
				artifactSampleWidth, artifactSampleHeight),
			"-an", "-sn", "-dn", "-threads", "1",
			"-f", "rawvideo", "-pix_fmt", "gray", "pipe:1").Output()
		if err != nil {
			return false, "", err
		}
		if len(frames) < frameSize || len(frames)%frameSize != 0 ||
			len(frames) > frameSize*artifactFramesPerSeek {
			return false, "", fmt.Errorf(
				"decoded placeholder frame count mismatch: bytes=%d frame_size=%d limit=%d",
				len(frames), frameSize, artifactFramesPerSeek)
		}
		for offset := 0; offset < len(frames); offset += frameSize {
			frame := frames[offset : offset+frameSize]
			if !nearBlackArtifactFrame(frame) {
				return false, "", nil
			}
			hash := sha256.Sum256(frame)
			if !haveHash {
				firstHash = hash
				haveHash = true
			} else if hash != firstHash {
				return false, "", nil
			}
		}

		for _, stream := range audioStreams {
			pcm, audioErr := exec.CommandContext(ctx, p.ffmpeg,
				"-v", "error", "-ss", formatFFmpegSeconds(at), "-i", audioPath,
				"-map", fmt.Sprintf("0:%d", stream.index),
				"-t", formatFFmpegSeconds(artifactAudioWindow),
				"-vn", "-sn", "-dn", "-threads", "1",
				"-ar", strconv.Itoa(artifactAudioRate),
				"-f", "s16le", "pipe:1").Output()
			if audioErr != nil {
				return false, "", audioErr
			}
			if !artifactAudioWindowComplete(pcm, stream.channels) {
				return false, "", fmt.Errorf(
					"decoded audio window mismatch: stream=%d bytes=%d want=%d",
					stream.index, len(pcm), artifactAudioWindowBytes(stream.channels))
			}
			if !nearSilentPCM16(pcm) {
				return false, "", nil
			}
		}
	}
	if !haveHash {
		return false, "", errors.New("decoded placeholder frames missing")
	}
	return true, "finite_mpegts_uniform_identical_black_and_silent", nil
}

func formatFFmpegSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', 3, 64)
}

func nearBlackArtifactFrame(frame []byte) bool {
	if len(frame) == 0 {
		return false
	}
	minY, maxY := byte(255), byte(0)
	var sum uint64
	for _, y := range frame {
		minY = min(minY, y)
		maxY = max(maxY, y)
		sum += uint64(y)
	}
	mean := float64(sum) / float64(len(frame))
	return maxY <= 4 && maxY-minY <= 4 && mean <= 2
}

func nearSilentPCM16(pcm []byte) bool {
	if len(pcm) < 2 || len(pcm)%2 != 0 {
		return false
	}
	var sumSquares float64
	var peak int32
	for offset := 0; offset < len(pcm); offset += 2 {
		sample := int32(int16(binary.LittleEndian.Uint16(pcm[offset : offset+2])))
		magnitude := sample
		if magnitude < 0 {
			magnitude = -magnitude
		}
		peak = max(peak, magnitude)
		sumSquares += float64(sample * sample)
	}
	rms := math.Sqrt(sumSquares / float64(len(pcm)/2))
	return peak <= 500 && rms <= 100
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

type limitedDiagnostic struct {
	buf       bytes.Buffer
	truncated bool
}

func (w *limitedDiagnostic) Write(p []byte) (int, error) {
	n := len(p)
	remaining := maxToolDiagnosticBytes - w.buf.Len()
	if remaining > 0 {
		_, _ = w.buf.Write(p[:min(len(p), remaining)])
	}
	if len(p) > remaining {
		w.truncated = true
	}
	return n, nil
}

func (w *limitedDiagnostic) String() string {
	s := strings.TrimSpace(w.buf.String())
	if w.truncated {
		s += " [diagnostic truncated]"
	}
	return s
}

func recordingCapturePath(output string, id uuid.UUID) string {
	return output + ".capture." + id.String() + ".partial"
}

func recordingNormalizedPath(output string, id uuid.UUID) string {
	return output + ".normalized." + id.String() + ".partial"
}

// The suffix intentionally does not end in .ts, preventing Sonarr/Radarr from
// treating the retained predecessor as another import candidate.
func recordingSupersededPath(output string, id uuid.UUID) string {
	return output + ".superseded." + id.String()
}

// recordingPublishExchangePath is derived from the recording-scoped candidate,
// so it cannot collide with another attempt. It deliberately does not end in
// .ts, preventing an interrupted exchange from looking importable.
func recordingPublishExchangePath(candidate string) string {
	return candidate + ".publish-exchange"
}

// publishRecordingCandidate never removes an unverified directory entry. A
// replacement preserves both the predecessor and the validated candidate with
// hard links, atomically exchanges a private candidate alias with the canonical
// path, and verifies the displaced inode before unlinking that private alias.
// If another writer changed the canonical path, the exchange is reversed and
// every distinct inode is retained for fail-closed startup recovery. A first
// publication continues to use a hard link as its no-replace primitive.
func publishRecordingCandidate(canonical, candidate, backup string) error {
	return publishRecordingCandidateAfterBackup(canonical, candidate, backup, nil)
}

// publishRecordingCandidateAfterBackup exposes the exact preservation seam to
// deterministic tests. Production always passes a nil callback.
func publishRecordingCandidateAfterBackup(
	canonical, candidate, backup string,
	afterBackup func() error,
) error {
	candidateInfo, err := os.Stat(candidate)
	if err != nil {
		return fmt.Errorf("validated candidate missing: %w", err)
	}
	if err := syncFile(candidate); err != nil {
		return fmt.Errorf("sync validated DVR candidate: %w", err)
	}
	if backup == "" {
		if err := os.Link(candidate, canonical); err != nil {
			return fmt.Errorf("publish final recording without replacement: %w", err)
		}
		if err := syncParentDir(canonical); err != nil {
			cleanupErr := unlinkPublishedFinal(candidate, canonical)
			if cleanupErr == nil {
				cleanupErr = syncParentDir(canonical)
			}
			if cleanupErr != nil {
				return errors.Join(errArtifactPublishRecoveryRequired, err, cleanupErr)
			}
			return err
		}
		return nil
	}

	canonicalInfo, err := os.Stat(canonical)
	if err != nil {
		return fmt.Errorf("current DVR artifact missing before replacement: %w", err)
	}
	if _, err := os.Lstat(backup); err == nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			errors.New("superseded artifact backup already exists"))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Link(canonical, backup); err != nil {
		return fmt.Errorf("preserve current DVR artifact: %w", err)
	}
	backupInfo, err := os.Stat(backup)
	if err != nil || !os.SameFile(canonicalInfo, backupInfo) {
		return errors.Join(errArtifactPublishRecoveryRequired,
			errors.New("predecessor identity changed while preserving DVR artifact"), err)
	}
	if err := syncParentDir(canonical); err != nil {
		cleanupErr := removeArtifactAlias(backup, canonicalInfo)
		if cleanupErr == nil {
			cleanupErr = syncParentDir(canonical)
		}
		if cleanupErr != nil {
			return errors.Join(errArtifactPublishRecoveryRequired, err, cleanupErr)
		}
		return err
	}

	exchange := recordingPublishExchangePath(candidate)
	if _, err := os.Lstat(exchange); err == nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			errors.New("DVR artifact publish exchange path already exists"))
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.Join(errArtifactPublishRecoveryRequired, err)
	}
	if err := os.Link(candidate, exchange); err != nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			fmt.Errorf("preserve validated candidate for atomic publish: %w", err))
	}
	exchangeInfo, err := os.Stat(exchange)
	if err != nil || !os.SameFile(candidateInfo, exchangeInfo) {
		return errors.Join(errArtifactPublishRecoveryRequired,
			errors.New("validated candidate identity changed while preparing atomic publish"), err)
	}
	if err := syncParentDir(canonical); err != nil {
		return errors.Join(errArtifactPublishRecoveryRequired, err)
	}
	if afterBackup != nil {
		if err := afterBackup(); err != nil {
			return abortUnexchangedArtifactPublish(
				canonical, candidate, backup, exchange,
				canonicalInfo, candidateInfo, err)
		}
	}
	if err := atomicExchangeArtifactPaths(exchange, canonical); err != nil {
		return abortUnexchangedArtifactPublish(
			canonical, candidate, backup, exchange,
			canonicalInfo, candidateInfo,
			fmt.Errorf("atomically exchange DVR candidate: %w", err))
	}

	publishedInfo, publishedErr := os.Stat(canonical)
	displacedInfo, displacedErr := os.Stat(exchange)
	currentCandidateInfo, currentCandidateErr := os.Stat(candidate)
	if publishedErr != nil || displacedErr != nil || currentCandidateErr != nil ||
		!os.SameFile(publishedInfo, candidateInfo) ||
		!os.SameFile(currentCandidateInfo, candidateInfo) {
		return errors.Join(
			errArtifactPublishRecoveryRequired,
			errors.New("DVR artifact identities changed after atomic publish exchange"),
			publishedErr, displacedErr, currentCandidateErr,
		)
	}
	if !os.SameFile(displacedInfo, canonicalInfo) {
		return rollbackConflictingArtifactExchange(
			canonical, candidate, backup, exchange,
			canonicalInfo, candidateInfo, displacedInfo)
	}
	if err := removeArtifactAlias(exchange, canonicalInfo); err != nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			fmt.Errorf("remove verified displaced predecessor alias: %w", err))
	}
	if err := syncParentDir(canonical); err != nil {
		rollbackErr := rollbackPublishedCandidate(canonical, candidate, backup)
		if rollbackErr != nil {
			return errors.Join(errArtifactPublishRecoveryRequired, err, rollbackErr)
		}
		return err
	}
	return nil
}

func rollbackPublishedCandidate(canonical, candidate, backup string) error {
	if backup == "" {
		return errors.Join(unlinkPublishedFinal(candidate, canonical), syncParentDir(canonical))
	}

	canonicalInfo, canonicalErr := os.Stat(canonical)
	backupInfo, backupErr := os.Stat(backup)
	if canonicalErr != nil || backupErr != nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			errors.New("inspect DVR artifact rollback entries"), canonicalErr, backupErr)
	}
	if os.SameFile(canonicalInfo, backupInfo) {
		// Publication stopped before the atomic exchange.
		return errors.Join(removeArtifactAlias(backup, backupInfo), syncParentDir(canonical))
	}

	candidateInfo, candidateErr := os.Stat(candidate)
	if errors.Is(candidateErr, os.ErrNotExist) {
		// Backward-compatible recovery for artifacts published by the pre-exchange
		// implementation, which moved the sole candidate name to canonical.
		if err := os.Link(canonical, candidate); err != nil {
			return errors.Join(errArtifactPublishRecoveryRequired,
				fmt.Errorf("retain rejected DVR candidate: %w", err))
		}
		candidateInfo, candidateErr = os.Stat(candidate)
	}
	if candidateErr != nil || !os.SameFile(canonicalInfo, candidateInfo) {
		return errors.Join(errArtifactPublishRecoveryRequired,
			errors.New("canonical artifact no longer aliases the rejected candidate"),
			candidateErr)
	}

	if err := atomicExchangeArtifactPaths(canonical, backup); err != nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			fmt.Errorf("atomically restore prior DVR artifact: %w", err))
	}
	restoredInfo, restoredErr := os.Stat(canonical)
	displacedInfo, displacedErr := os.Stat(backup)
	retainedInfo, retainedErr := os.Stat(candidate)
	if restoredErr != nil || displacedErr != nil || retainedErr != nil ||
		!os.SameFile(restoredInfo, backupInfo) ||
		!os.SameFile(displacedInfo, canonicalInfo) ||
		!os.SameFile(retainedInfo, canonicalInfo) {
		reverseErr := atomicExchangeArtifactPaths(canonical, backup)
		return errors.Join(
			errArtifactPublishRecoveryRequired,
			errors.New("DVR artifact rollback exchange displaced an unexpected inode"),
			restoredErr, displacedErr, retainedErr, reverseErr,
		)
	}
	if err := removeArtifactAlias(backup, canonicalInfo); err != nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			fmt.Errorf("remove rejected candidate rollback alias: %w", err))
	}
	return syncParentDir(canonical)
}

func abortUnexchangedArtifactPublish(
	canonical, candidate, backup, exchange string,
	expectedCanonical, expectedCandidate os.FileInfo,
	cause error,
) error {
	canonicalInfo, canonicalErr := os.Stat(canonical)
	candidateInfo, candidateErr := os.Stat(candidate)
	backupInfo, backupErr := os.Stat(backup)
	exchangeInfo, exchangeErr := os.Stat(exchange)
	if canonicalErr != nil || candidateErr != nil || backupErr != nil || exchangeErr != nil ||
		!os.SameFile(canonicalInfo, expectedCanonical) ||
		!os.SameFile(backupInfo, expectedCanonical) ||
		!os.SameFile(candidateInfo, expectedCandidate) ||
		!os.SameFile(exchangeInfo, expectedCandidate) {
		return errors.Join(
			errArtifactPublishRecoveryRequired, cause,
			errors.New("artifact entries changed before atomic publish exchange"),
			canonicalErr, candidateErr, backupErr, exchangeErr,
		)
	}
	cleanupErr := removeArtifactAlias(exchange, expectedCandidate)
	if cleanupErr == nil {
		cleanupErr = removeArtifactAlias(backup, expectedCanonical)
	}
	if cleanupErr == nil {
		cleanupErr = syncParentDir(canonical)
	}
	if cleanupErr != nil {
		return errors.Join(errArtifactPublishRecoveryRequired, cause, cleanupErr)
	}
	return cause
}

func rollbackConflictingArtifactExchange(
	canonical, candidate, backup, exchange string,
	expectedPredecessor, expectedCandidate, displaced os.FileInfo,
) error {
	if err := atomicExchangeArtifactPaths(exchange, canonical); err != nil {
		return errors.Join(errArtifactPublishRecoveryRequired, errArtifactPublishConflict,
			fmt.Errorf("reverse conflicting DVR artifact exchange: %w", err))
	}
	canonicalInfo, canonicalErr := os.Stat(canonical)
	candidateInfo, candidateErr := os.Stat(candidate)
	backupInfo, backupErr := os.Stat(backup)
	exchangeInfo, exchangeErr := os.Stat(exchange)
	if canonicalErr != nil || candidateErr != nil || backupErr != nil || exchangeErr != nil ||
		!os.SameFile(canonicalInfo, displaced) ||
		!os.SameFile(candidateInfo, expectedCandidate) ||
		!os.SameFile(exchangeInfo, expectedCandidate) ||
		!os.SameFile(backupInfo, expectedPredecessor) {
		return errors.Join(
			errArtifactPublishRecoveryRequired, errArtifactPublishConflict,
			errors.New("conflicting DVR artifact exchange could not be proven reversed"),
			canonicalErr, candidateErr, backupErr, exchangeErr,
		)
	}
	if err := removeArtifactAlias(exchange, expectedCandidate); err != nil {
		return errors.Join(errArtifactPublishRecoveryRequired,
			errArtifactPublishConflict, err)
	}
	return errors.Join(errArtifactPublishRecoveryRequired,
		errArtifactPublishConflict, syncParentDir(canonical))
}

func removeArtifactAlias(path string, expected os.FileInfo) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !os.SameFile(info, expected) {
		return fmt.Errorf("refuse to remove changed DVR artifact entry %q", path)
	}
	return os.Remove(path)
}

func syncParentDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync DVR artifact directory: %w", err)
	}
	return nil
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// RecoverInterruptedArtifactPublishes rolls a filesystem-first publish back
// when startup reconciliation finds a row still carrying `publishing`
// provenance. It runs only after the runtime singleton is held, so no live
// recorder can be inside the same seam.
func RecoverInterruptedArtifactPublishes(
	ctx context.Context,
	db *store.DB,
	logger *slog.Logger,
) (int, error) {
	rows, err := db.ListInterruptedDVRPublishes(ctx)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, rec := range rows {
		candidate := rec.ArtifactPath
		if rec.OutputPath == "" || candidate == "" {
			return recovered, fmt.Errorf(
				"recording %s has incomplete interrupted-publish provenance", rec.ID)
		}
		backup := ""
		if rec.ReplacesRecordingID != nil {
			backup = recordingSupersededPath(rec.OutputPath, *rec.ReplacesRecordingID)
		}
		if err := recoverInterruptedArtifactPublish(rec.OutputPath, candidate, backup); err != nil {
			return recovered, fmt.Errorf("recover interrupted DVR publish %s: %w", rec.ID, err)
		}
		reason := "rolled back interrupted DVR artifact publish during startup"
		if err := db.FinishDVRPublishRecovery(ctx, rec.ID, reason, candidate); err != nil {
			return recovered, err
		}
		recovered++
		if logger != nil {
			logger.Warn("recovered interrupted DVR artifact publish",
				"recording_id", rec.ID, "candidate", candidate,
				"restored_predecessor", rec.ReplacesRecordingID)
		}
	}
	return recovered, nil
}

func recoverInterruptedArtifactPublish(canonical, candidate, backup string) error {
	canonicalInfo, canonicalErr := os.Stat(canonical)
	candidateInfo, candidateErr := os.Stat(candidate)
	if backup == "" {
		switch {
		case canonicalErr == nil && candidateErr == nil:
			if os.SameFile(canonicalInfo, candidateInfo) {
				// A later unlink cannot be made conditional on the directory entry
				// still naming this inode. Preserve both aliases so a concurrent
				// canonical replacement can never be removed by recovery.
				return errors.Join(errArtifactPublishRecoveryRequired,
					errors.New("ambiguous first publication retains canonical candidate alias"))
			}
			return errors.Join(errArtifactPublishRecoveryRequired,
				errors.New("untracked canonical artifact appeared during first publish recovery"))
		case canonicalErr == nil && errors.Is(candidateErr, os.ErrNotExist):
			// Current first publication uses a no-replace hard link, so a
			// successfully published candidate retains both names until database
			// promotion is durable. A lone canonical can only be legacy state or
			// an unrelated file that appeared after the candidate vanished. The
			// publishing marker does not persist candidate bytes/SHA evidence, so
			// recovery cannot distinguish those cases without risking relocation
			// of foreign media. Preserve every path and require operator recovery.
			return errors.Join(errArtifactPublishRecoveryRequired,
				errors.New("ambiguous first publication has canonical without candidate evidence"))
		case canonicalErr == nil:
			return errors.Join(errArtifactPublishRecoveryRequired,
				fmt.Errorf("inspect first-publication candidate: %w", candidateErr))
		case errors.Is(canonicalErr, os.ErrNotExist) && candidateErr == nil:
			// Publish never reached the filesystem.
		case errors.Is(canonicalErr, os.ErrNotExist) && errors.Is(candidateErr, os.ErrNotExist):
			return errors.New("both canonical and candidate artifacts are missing")
		default:
			return errors.Join(canonicalErr, candidateErr)
		}
		return syncParentDir(canonical)
	}

	backupInfo, backupErr := os.Stat(backup)
	exchange := recordingPublishExchangePath(candidate)
	exchangeInfo, exchangeErr := os.Stat(exchange)
	if exchangeErr == nil {
		switch {
		case canonicalErr == nil && candidateErr == nil && backupErr == nil &&
			os.SameFile(canonicalInfo, backupInfo) &&
			os.SameFile(candidateInfo, exchangeInfo):
			// Crash after both preservation links but before the exchange.
			if err := removeArtifactAlias(exchange, candidateInfo); err != nil {
				return err
			}
			exchangeErr = os.ErrNotExist
		case canonicalErr == nil && candidateErr == nil && backupErr == nil &&
			os.SameFile(canonicalInfo, candidateInfo) &&
			os.SameFile(exchangeInfo, backupInfo):
			// Crash after the exchange but before its verified displaced alias
			// was removed. The backup retains the same predecessor inode.
			if err := removeArtifactAlias(exchange, backupInfo); err != nil {
				return err
			}
			exchangeErr = os.ErrNotExist
		default:
			return errors.Join(errArtifactPublishRecoveryRequired,
				errors.New("ambiguous DVR artifact exchange alias during recovery"),
				canonicalErr, candidateErr, backupErr)
		}
	} else if !errors.Is(exchangeErr, os.ErrNotExist) {
		return errors.Join(errArtifactPublishRecoveryRequired, exchangeErr)
	}

	switch {
	case errors.Is(backupErr, os.ErrNotExist):
		// Publish stopped before preserving the predecessor. The predecessor and
		// candidate must both still exist and be distinct.
		if canonicalErr != nil || candidateErr != nil {
			return errors.Join(errors.New("predecessor backup is missing"), canonicalErr, candidateErr)
		}
		if os.SameFile(canonicalInfo, candidateInfo) {
			return errors.New("candidate unexpectedly aliases predecessor without a backup")
		}
	case backupErr != nil:
		return backupErr
	case canonicalErr == nil && candidateErr == nil && os.SameFile(canonicalInfo, backupInfo):
		// Crash after preserving the predecessor but before the atomic exchange.
		if err := removeArtifactAlias(backup, backupInfo); err != nil {
			return err
		}
	case canonicalErr == nil && candidateErr == nil &&
		os.SameFile(canonicalInfo, candidateInfo) &&
		!os.SameFile(canonicalInfo, backupInfo):
		// The candidate was published by the exchange protocol. The candidate
		// name remains a trusted alias, so rollback can restore the predecessor
		// without ever creating a name-less interval.
		return rollbackPublishedCandidate(canonical, candidate, backup)
	case canonicalErr == nil && errors.Is(candidateErr, os.ErrNotExist) &&
		!os.SameFile(canonicalInfo, backupInfo):
		// Backward-compatible recovery for the former rename-based publisher.
		return rollbackPublishedCandidate(canonical, candidate, backup)
	case canonicalErr == nil && candidateErr == nil &&
		os.SameFile(candidateInfo, backupInfo) &&
		!os.SameFile(canonicalInfo, candidateInfo):
		// Crash after the atomic rollback restored canonical but before its
		// rejected-candidate alias was removed from the backup path.
		if err := removeArtifactAlias(backup, candidateInfo); err != nil {
			return err
		}
	case errors.Is(canonicalErr, os.ErrNotExist) && candidateErr == nil:
		// An interrupted legacy rollback may already have retained the candidate
		// but not restored the predecessor. Link is no-replace, so a concurrent
		// foreign canonical is never overwritten.
		if err := os.Link(backup, canonical); err != nil {
			return err
		}
		if err := removeArtifactAlias(backup, backupInfo); err != nil {
			return err
		}
	default:
		return errors.Join(errArtifactPublishRecoveryRequired,
			errors.New("ambiguous replacement publish recovery state"),
			canonicalErr, candidateErr)
	}
	return syncParentDir(canonical)
}
