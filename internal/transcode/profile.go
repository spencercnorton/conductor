// Package transcode owns the FFmpeg invocation that sits between an
// upstream provider stream and Conductor's subscriber fan-out, when a
// channel/source has a transcode_profile attached.
//
// Architecture (also in docs/enhancements.md §11):
//
//	upstream HTTP body  ─┬─► (passthrough Streamer)  ─► subscribers
//	                     │
//	                     └─► transcode.Runner ─► ffmpeg ─► subscribers
//	                                               │
//	                                       (NVENC + NVDEC on the GPU)
//
// The Runner shells out to ffmpeg via os/exec, pipes upstream into stdin,
// reads transcoded MPEG-TS from stdout, and exposes that as an io.Reader
// the existing Streamer machinery can consume.
//
// Profile → argv conversion lives in this file (BuildArgs); the actual
// subprocess is in runner.go. Splitting them lets us test argv generation
// without an FFmpeg dependency in CI.
package transcode

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Kind enumerates the supported high-level shapes. Mirrors the SQL enum.
type Kind string

const (
	KindPassthrough    Kind = "passthrough"
	KindCPURemux       Kind = "cpu_remux"
	KindCPUTranscode   Kind = "cpu_transcode"
	KindNVENCTranscode Kind = "nvenc_transcode"
	KindCustom         Kind = "custom"
)

// Profile is the typed view of a transcode_profile row. Mirrors the
// columns in store/migrations/0006_transcode.sql.
type Profile struct {
	ID          uuid.UUID
	Name        string
	Description string
	Kind        Kind

	VideoCodec          string
	VideoBitrateKbps    int
	VideoMaxBitrateKbps int
	VideoHeight         int
	VideoPreset         string
	VideoTune           string
	VideoKeyint         int
	Deinterlace         bool
	HDRtoSDR            bool

	AudioCodec       string
	AudioBitrateKbps int
	AudioChannels    int
	AudioNormalize   bool

	UseNVENC      bool
	UseNVDEC      bool
	FixTimestamps bool
	LowLatency    bool

	// DropSubtitles drops the upstream subtitle PID instead of copying it.
	// Many IPTV restreams of "traditional cable" channels carry a DVB
	// bitmap-subtitle stream (codec dvb_subtitle) that Plex's Live TV
	// transcoder chokes on, causing the session to stall and restart.
	// EIA-608/708 closed captions ride inside the H.264 SEI and survive
	// `-c:v copy`, so dropping the separate DVB-sub PID loses nothing Plex
	// can actually use. When true, BuildArgs omits the `0:s?` mapping and
	// emits `-sn`.
	DropSubtitles bool

	FFmpegArgs []string // for Kind=custom or last-mile overrides

	IsBuiltin bool
	Enabled   bool
}

// IsPassthrough returns true if this profile means "no FFmpeg, copy bytes".
// Pool uses this to skip the transcode pipeline entirely.
func (p *Profile) IsPassthrough() bool {
	return p == nil || p.Kind == KindPassthrough
}

// BuildArgs returns the FFmpeg argv (excluding the leading "ffmpeg") for
// invoking this profile. Input is stdin; output is stdout MPEG-TS.
//
// For Kind=custom the FFmpegArgs slice is returned verbatim with the
// `-i pipe:0` and `-f mpegts pipe:1` boilerplate prepended/appended.
func (p *Profile) BuildArgs() []string {
	return p.BuildArgsWithAudioOriginCorrection(0)
}

// SupportsAudioOriginCorrection reports whether this profile has Conductor's
// encoded-audio clock filter in its FFmpeg graph. Copy and custom audio paths
// cannot safely shift one elementary stream without rewriting provider media
// or second-guessing operator-owned arguments.
func (p *Profile) SupportsAudioOriginCorrection() bool {
	return p != nil && !p.IsPassthrough() && p.Kind != KindCustom &&
		p.FixTimestamps && p.AudioCodec != "" &&
		!strings.EqualFold(strings.TrimSpace(p.AudioCodec), "copy")
}

// SupportsInputAudioClockRepair identifies the owned graph for which raw audio
// slope may be delegated to the sample-count filter. Video must be copied, and
// last-mile arguments must not be able to override the graph being trusted.
func (p *Profile) SupportsInputAudioClockRepair() bool {
	return p.SupportsAudioOriginCorrection() && len(p.FFmpegArgs) == 0 &&
		(p.VideoCodec == "" || p.VideoCodec == "copy")
}

// AudioClockGapThreshold is the existing gap boundary of the sample-count
// filter. Input repair eligibility uses this same bound conservatively on each
// whole PES step, without assuming how many audio frames that PES contains.
const AudioClockGapThreshold = 500 * time.Millisecond

// BuildArgsWithAudioOriginCorrection returns this profile's FFmpeg argv with
// an attempt-local correction applied only to the first output sample of the
// already selected 0:a:0 stream. The Profile is never mutated: pooled streams
// can therefore share a persisted profile while each source attempt proves its
// own clock origin independently.
func (p *Profile) BuildArgsWithAudioOriginCorrection(correction time.Duration) []string {
	if p == nil || p.IsPassthrough() {
		return nil
	}
	if correction != 0 && !p.SupportsAudioOriginCorrection() {
		correction = 0
	}

	args := []string{}

	// ── Global options (must come before -i) ──
	if p.LowLatency {
		args = append(args, "-fflags", "+nobuffer+genpts+discardcorrupt",
			"-flags", "low_delay",
			"-strict", "experimental")
	} else if p.FixTimestamps {
		args = append(args, "-fflags", "+genpts+discardcorrupt")
	}

	// Hardware decode comes BEFORE -i so ffmpeg knows to spin up the
	// CUDA decoder for the input. H.264 NVENC profiles retain NVDEC but ask
	// FFmpeg for software frames: production FFmpeg 4.4 cannot convert P010
	// CUDA frames to the 8-bit format required by Ada's H.264 encoder.
	if p.UseNVDEC {
		args = append(args, "-hwaccel", "cuda")
		if p.useCUDAFrames() {
			args = append(args, "-hwaccel_output_format", "cuda")
		}
	}

	// Recover from upstream hiccups instead of dying.
	args = append(args, "-err_detect", "ignore_err")

	args = append(args, "-i", "pipe:0")

	// ── Mapping ──
	// Map first video + first audio. Subtitles are mapped only when we're
	// keeping them; DropSubtitles omits the `0:s?` map entirely. The `?`
	// makes a missing subtitle stream non-fatal (some channels drop it).
	args = append(args, "-map", "0:v:0", "-map", "0:a:0")
	if !p.DropSubtitles {
		args = append(args, "-map", "0:s?")
	}

	// ── Video output ──
	if p.Kind == KindCustom {
		// Fall through to user-supplied args entirely.
		args = append(args, p.FFmpegArgs...)
	} else {
		args = append(args, p.videoArgs()...)
		args = append(args, p.audioArgs(correction)...)
		args = append(args, p.subtitleArgs()...)
	}

	// ── MPEG-TS output tuning (Plex-friendly defaults) ──
	// `pcr_period` is the mpegts muxer option (no `mpegts_` prefix);
	// ffmpeg 4.4 errors out with "Unrecognized option 'mpegts_pcr_period'".
	args = append(args,
		"-mpegts_flags", "+resend_headers+initial_discontinuity",
		"-pcr_period", "20",
		"-muxdelay", "0", "-muxpreload", "0",
		"-f", "mpegts", "pipe:1",
	)

	// Last-mile overrides (Kind != custom but the operator wants extras).
	if p.Kind != KindCustom && len(p.FFmpegArgs) > 0 {
		args = append(args, p.FFmpegArgs...)
	}

	return args
}

func (p *Profile) videoArgs() []string {
	if p.VideoCodec == "" || p.VideoCodec == "copy" {
		return []string{"-c:v", "copy"}
	}

	out := []string{"-c:v", p.VideoCodec}

	if p.VideoPreset != "" {
		out = append(out, "-preset", p.VideoPreset)
	}
	if p.VideoTune != "" {
		out = append(out, "-tune", p.VideoTune)
	}
	if p.VideoBitrateKbps > 0 {
		out = append(out, "-b:v", strconv.Itoa(p.VideoBitrateKbps)+"k")
	}
	if p.VideoMaxBitrateKbps > 0 {
		out = append(out, "-maxrate", strconv.Itoa(p.VideoMaxBitrateKbps)+"k",
			"-bufsize", strconv.Itoa(p.VideoMaxBitrateKbps*2)+"k")
	}
	if p.VideoBitrateKbps > 0 && p.VideoMaxBitrateKbps > 0 {
		// CBR-LD-HQ is NVENC's low-latency CBR mode.
		out = append(out, "-rc", nvencRC(p))
	}
	if p.VideoKeyint > 0 {
		out = append(out, "-g", strconv.Itoa(p.VideoKeyint),
			"-keyint_min", strconv.Itoa(p.VideoKeyint))
	}

	// Build the -vf chain. Order matters: deinterlace → tonemap → scale.
	useCUDAFrames := p.useCUDAFrames()
	var vfChain []string

	if p.Deinterlace {
		if useCUDAFrames {
			vfChain = append(vfChain, "yadif_cuda=mode=send_field:parity=auto:deint=interlaced")
		} else {
			vfChain = append(vfChain, "yadif=mode=1:parity=auto:deint=interlaced")
		}
	}

	if p.HDRtoSDR {
		// Tone-map HDR10/PQ → SDR/BT.709. CUDA tonemap requires the input
		// to be on the GPU (UseNVDEC), so we always emit the GPU path
		// when HDRtoSDR is on AND UseNVDEC is on; otherwise software path.
		if useCUDAFrames {
			vfChain = append(vfChain,
				"hwdownload", "format=p010le",
				"zscale=transfer=linear,tonemap=hable:desat=0,zscale=transfer=bt709:matrix=bt709:primaries=bt709,format=yuv420p",
				"hwupload_cuda")
		} else {
			vfChain = append(vfChain,
				"zscale=transfer=linear,tonemap=hable:desat=0,zscale=transfer=bt709:matrix=bt709:primaries=bt709,format=yuv420p")
		}
	}

	if p.VideoHeight > 0 {
		if useCUDAFrames {
			// FFmpeg 4.4's scale_cuda cannot select an output pixel format.
			// Hardware-decode paths therefore preserve the decoder's format.
			// useCUDAFrames keeps h264_nvenc out of this branch because
			// Ada NVENC cannot encode the preserved P010 input as H.264.
			vfChain = append(vfChain,
				fmt.Sprintf("scale_cuda=-2:%d", p.VideoHeight))
		} else {
			vfChain = append(vfChain,
				fmt.Sprintf("scale=-2:%d:flags=lanczos", p.VideoHeight))
		}
	}

	if p.isH264NVENC() && !p.HDRtoSDR {
		// The production FFmpeg 4.4 scale_cuda filter preserves P010 and has
		// no `format` option, while the deployed Ada encoder rejects 10-bit
		// H.264. Decode/scale in software and explicitly normalize both 8-bit
		// and 10-bit sources to yuv420p before NVENC uploads the frames. The
		// HDR software chain above already ends in this format.
		vfChain = append(vfChain, "format=yuv420p")
	}

	if len(vfChain) > 0 {
		out = append(out, "-vf", strings.Join(vfChain, ","))
	}

	if p.LowLatency && p.UseNVENC {
		// NVENC zero-latency tuning.
		out = append(out,
			"-bf", "0",
			"-temporal_aq", "0",
			"-spatial_aq", "0",
			"-rc-lookahead", "0",
			"-zerolatency", "1",
		)
	}

	return out
}

// useCUDAFrames reports whether this profile has a format-safe zero-copy path
// on the production FFmpeg/NVIDIA stack. FFmpeg 4.4's scale_cuda keeps the
// decoder's software pixel format and cannot be told to emit NV12. An H.264
// NVENC profile fed P010 CUDA frames would therefore fail at encoder startup
// with "10 bit encode not supported". H.264 still uses NVDEC, but FFmpeg
// downloads decoded frames before the software 8-bit conversion and NVENC
// upload. Custom profiles remain operator-owned and preserve their requested
// CUDA-frame contract.
func (p *Profile) useCUDAFrames() bool {
	return p.UseNVDEC && (p.Kind == KindCustom || !p.isH264NVENC())
}

func (p *Profile) isH264NVENC() bool {
	return strings.EqualFold(strings.TrimSpace(p.VideoCodec), "h264_nvenc")
}

// nvencRC returns the rate-control mode flag for h264_nvenc.
//
// The legacy `cbr_ld_hq` / `cbr_hq` modes are deprecated and ffmpeg 4.4+
// rejects them with "Presets P1 to P7 are not supported with older 2 Pass
// RC Modes". Modern h264_nvenc differentiates LL/HQ via `-tune` (already
// emitted in videoArgs) plus `-multipass`, not via the rc mode itself.
func nvencRC(p *Profile) string {
	if p.UseNVENC {
		return "cbr"
	}
	return "cbr"
}

func (p *Profile) audioArgs(originCorrection time.Duration) []string {
	if p.AudioCodec == "" || p.AudioCodec == "copy" {
		return []string{"-c:a", "copy"}
	}
	out := []string{"-c:a", p.AudioCodec}
	if p.AudioBitrateKbps > 0 {
		out = append(out, "-b:a", strconv.Itoa(p.AudioBitrateKbps)+"k")
	}
	if p.AudioChannels > 0 {
		out = append(out, "-ac", strconv.Itoa(p.AudioChannels))
	}
	// The filter supplies one stable sample clock for encoded audio while
	// preserving genuine discontinuities and any optional normalization.
	out = append(out, "-af", audioFilter(p, originCorrection))
	return out
}

func audioFilter(p *Profile, originCorrection time.Duration) string {
	// Build encoded-audio time from decoded sample count, while preserving a
	// genuine discontinuity larger than 500 ms. The previous
	// aresample=async=1000:first_pts=0 followed the audio track's own PTS and
	// therefore preserved a bad clock slope next to copied video: a 1% fast
	// audio clock became progressively worse A/V sync for the entire tune.
	//
	// For ordinary AAC/AC3 frame deltas (roughly 21–32 ms), the expression
	// advances by S/SR and ignores gradual input-clock skew. A large positive
	// jump advances by the original PTS delta so a substantive packet-loss gap
	// remains silence instead of compressing the programme. The 500 ms boundary
	// is deliberate: a 1.4x bad MPEG-TS clock is quantized by AAC demux into
	// periodic ~124 ms jumps, so the old 100 ms boundary preserved the exact
	// incident-scale drift. Sub-500 ms loss remains inherently ambiguous without
	// transport continuity evidence and is normalized. Backward resets advance
	// by sample count and stay monotonic. The first packet keeps its original
	// PTS unless this attempt independently proved a bounded origin correction.
	parts := []string{repairAudioClockFilter(originCorrection)}
	if p.AudioNormalize {
		// EBU R128 loudness normalization (-i -16 LUFS, broadcast spec).
		parts = append(parts, "loudnorm=I=-16:TP=-1.5:LRA=11")
	}
	return strings.Join(parts, ",")
}

// RepairAudioClockFilter rebuilds encoded-audio timestamps from decoded
// sample count while preserving genuine gaps larger than 500 ms. DVR artifact
// normalization uses the exact same clock policy as live transcodes so the two
// paths cannot silently disagree about what constitutes recoverable drift.
func RepairAudioClockFilter() string {
	return repairAudioClockFilter(0)
}

func repairAudioClockFilter(originCorrection time.Duration) string {
	firstPTS := "PTS"
	if originCorrection != 0 {
		seconds := strconv.FormatFloat(originCorrection.Seconds(), 'f', 6, 64)
		if originCorrection > 0 {
			seconds = "+" + seconds
		}
		firstPTS += seconds + "/TB"
	}
	gap := strconv.FormatFloat(AudioClockGapThreshold.Seconds(), 'f', -1, 64)
	return `asetpts=if(eq(N\,0)\,` + firstPTS + `\,PREV_OUTPTS+if(gt((PTS-PREV_INPTS)*TB\,S/SR+` + gap + `)\,PTS-PREV_INPTS\,S/SR/TB)),aresample=48000:async=1`
}

func (p *Profile) subtitleArgs() []string {
	// DropSubtitles: explicitly disable subtitle output (-sn). Pairs with
	// the omitted `0:s?` mapping above — belt and suspenders so a DVB
	// bitmap-subtitle PID can't leak into the MPEG-TS that Plex chokes on.
	if p.DropSubtitles {
		return []string{"-sn"}
	}
	// Otherwise copy subtitle streams when present — Plex picks up DVB-sub /
	// EIA-608 closed captions natively. Never fail the pipeline if a
	// channel has no subtitles (mapping uses `0:s?`).
	return []string{"-c:s", "copy"}
}
