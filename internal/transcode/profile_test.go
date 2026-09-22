package transcode_test

import (
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/transcode"
)

func TestPassthroughProducesNoArgs(t *testing.T) {
	p := &transcode.Profile{Kind: transcode.KindPassthrough}
	if !p.IsPassthrough() {
		t.Error("IsPassthrough should be true")
	}
	if args := p.BuildArgs(); args != nil {
		t.Errorf("passthrough should produce nil argv; got %v", args)
	}
}

func TestAudioOriginCorrectionIsAttemptLocalAndEncodedAudioOnly(t *testing.T) {
	encoded := &transcode.Profile{
		Kind: transcode.KindCPUTranscode, VideoCodec: "copy", AudioCodec: "aac",
		FixTimestamps: true,
	}
	if !encoded.SupportsAudioOriginCorrection() {
		t.Fatal("encoded timestamp-repair profile did not advertise origin correction")
	}
	corrected := strings.Join(encoded.BuildArgsWithAudioOriginCorrection(3*time.Second), " ")
	if !strings.Contains(corrected,
		`asetpts=if(eq(N\,0)\,PTS+3.000000/TB\,PREV_OUTPTS`) {
		t.Fatalf("attempt-local correction missing from first-sample branch: %s", corrected)
	}
	if got := strings.Join(encoded.BuildArgs(), " "); strings.Contains(got, "PTS+3.000000/TB") {
		t.Fatalf("attempt-local correction mutated the shared profile: %s", got)
	}

	for _, profile := range []*transcode.Profile{
		{Kind: transcode.KindCPURemux, VideoCodec: "copy", AudioCodec: "copy", FixTimestamps: true},
		{Kind: transcode.KindCustom, AudioCodec: "aac", FixTimestamps: true},
		{Kind: transcode.KindCPUTranscode, AudioCodec: "aac", FixTimestamps: false},
	} {
		if profile.SupportsAudioOriginCorrection() {
			t.Fatalf("unsafe profile unexpectedly supports correction: %+v", profile)
		}
		if got := strings.Join(profile.BuildArgsWithAudioOriginCorrection(3*time.Second), " "); strings.Contains(got, "PTS+3.000000/TB") {
			t.Fatalf("unsafe profile received a correction: %s", got)
		}
	}
}

func TestNilProfileIsPassthrough(t *testing.T) {
	var p *transcode.Profile
	if !p.IsPassthrough() {
		t.Error("nil profile should be considered passthrough")
	}
}

func TestCPURemuxArgsCopyVideoAndAudio(t *testing.T) {
	p := &transcode.Profile{
		Name: "stabilize", Kind: transcode.KindCPURemux,
		VideoCodec: "copy", AudioCodec: "copy",
		FixTimestamps: true, LowLatency: true,
	}
	got := strings.Join(p.BuildArgs(), " ")

	for _, want := range []string{
		"-fflags +nobuffer+genpts+discardcorrupt",
		"-flags low_delay",
		"-i pipe:0",
		"-map 0:v:0",
		"-map 0:a:0",
		"-c:v copy",
		"-c:a copy",
		"-f mpegts pipe:1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("cpu_remux argv missing %q\n  full: %s", want, got)
		}
	}
}

func TestH264NVENCRetainsNVDECWithEightBitCompatibilityPath(t *testing.T) {
	p := &transcode.Profile{
		Name: "nvenc-1080p", Kind: transcode.KindNVENCTranscode,
		VideoCodec: "h264_nvenc", VideoBitrateKbps: 8000, VideoMaxBitrateKbps: 10000,
		VideoHeight: 1080, VideoPreset: "p4", VideoTune: "ll", VideoKeyint: 60,
		AudioCodec: "aac", AudioBitrateKbps: 192, AudioChannels: 2,
		UseNVENC: true, UseNVDEC: true, FixTimestamps: true, LowLatency: true,
	}
	got := strings.Join(p.BuildArgs(), " ")

	for _, want := range []string{
		"-hwaccel cuda",
		"-c:v h264_nvenc",
		"-preset p4",
		"-tune ll",
		"-b:v 8000k",
		"-maxrate 10000k",
		"-bufsize 20000k",
		"-rc cbr",
		"-g 60",
		"-keyint_min 60",
		"-vf scale=-2:1080:flags=lanczos,format=yuv420p",
		"-c:a aac",
		"-b:a 192k",
		"-ac 2",
		"-af asetpts=if(eq(N\\,0)\\,PTS\\,PREV_OUTPTS+if(gt((PTS-PREV_INPTS)*TB\\,S/SR+0.5)\\,PTS-PREV_INPTS\\,S/SR/TB)),aresample=48000:async=1",
		"-bf 0",
		"-zerolatency 1",
		"-mpegts_flags +resend_headers+initial_discontinuity",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("h264 nvenc argv missing %q\n  full: %s", want, got)
		}
	}
	for _, unwanted := range []string{"-hwaccel_output_format cuda", "scale_cuda"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("h264 nvenc compatibility path contains %q\n  full: %s", unwanted, got)
		}
	}
}

func TestH264NVENCNormalizesWithoutResize(t *testing.T) {
	p := &transcode.Profile{
		Kind: transcode.KindNVENCTranscode, VideoCodec: "h264_nvenc",
		AudioCodec: "aac", UseNVENC: true, UseNVDEC: true,
	}
	got := strings.Join(p.BuildArgs(), " ")
	for _, want := range []string{"-hwaccel cuda", "-c:v h264_nvenc", "-vf format=yuv420p"} {
		if !strings.Contains(got, want) {
			t.Errorf("h264 no-resize argv missing %q\n  full: %s", want, got)
		}
	}
}

func TestH264NVENCDeinterlaceUsesSoftwareFrames(t *testing.T) {
	p := &transcode.Profile{
		Kind: transcode.KindNVENCTranscode, VideoCodec: "h264_nvenc",
		AudioCodec: "aac", UseNVENC: true, UseNVDEC: true, Deinterlace: true,
	}
	got := strings.Join(p.BuildArgs(), " ")
	if !strings.Contains(got, "-vf yadif=mode=1:parity=auto:deint=interlaced,format=yuv420p") {
		t.Fatalf("h264 deinterlace did not use normalized software frames: %s", got)
	}
	if strings.Contains(got, "yadif_cuda") || strings.Contains(got, "-hwaccel_output_format cuda") {
		t.Fatalf("h264 deinterlace leaked CUDA frames into compatibility path: %s", got)
	}
}

func TestHEVCNVENCWiresHardwareDecodeAndEncode(t *testing.T) {
	p := &transcode.Profile{
		Name: "hevc-nvenc", Kind: transcode.KindNVENCTranscode,
		VideoCodec: "hevc_nvenc", VideoHeight: 1080,
		AudioCodec: "aac", UseNVENC: true, UseNVDEC: true,
	}
	got := strings.Join(p.BuildArgs(), " ")
	for _, want := range []string{
		"-hwaccel cuda",
		"-hwaccel_output_format cuda",
		"-c:v hevc_nvenc",
		"-vf scale_cuda=-2:1080",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("hevc nvenc argv missing %q\n  full: %s", want, got)
		}
	}
}

func TestHDRtoSDRWiresTonemapChain(t *testing.T) {
	p := &transcode.Profile{
		Name: "hdr-sdr", Kind: transcode.KindNVENCTranscode,
		VideoCodec: "h264_nvenc", VideoBitrateKbps: 8000, VideoMaxBitrateKbps: 10000,
		VideoHeight: 1080,
		AudioCodec:  "aac", AudioBitrateKbps: 192,
		UseNVENC: true, UseNVDEC: true, HDRtoSDR: true,
		FixTimestamps: true, LowLatency: true,
	}
	got := strings.Join(p.BuildArgs(), " ")

	for _, want := range []string{
		"tonemap=hable:desat=0",
		"zscale=transfer=bt709:matrix=bt709:primaries=bt709",
		"format=yuv420p",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("hdr-to-sdr chain missing %q\n  full: %s", want, got)
		}
	}
	if !strings.Contains(got, "-hwaccel cuda") {
		t.Errorf("h264 hdr path must retain NVDEC: %s", got)
	}
	for _, unwanted := range []string{"-hwaccel_output_format cuda", "hwdownload", "hwupload_cuda", "scale_cuda"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("h264 hdr compatibility path contains %q\n  full: %s", unwanted, got)
		}
	}
}

func TestHEVCNVENCHDRKeepsCUDAFramePath(t *testing.T) {
	p := &transcode.Profile{
		Kind: transcode.KindNVENCTranscode, VideoCodec: "hevc_nvenc",
		AudioCodec: "aac", UseNVENC: true, UseNVDEC: true,
		HDRtoSDR: true, VideoHeight: 1080,
	}
	got := strings.Join(p.BuildArgs(), " ")
	for _, want := range []string{
		"-hwaccel cuda -hwaccel_output_format cuda",
		"hwdownload,format=p010le",
		"format=yuv420p,hwupload_cuda,scale_cuda=-2:1080",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("hevc hdr CUDA path missing %q\n  full: %s", want, got)
		}
	}
}

func TestDeinterlaceUsesNVDECVariantWhenGPUEnabled(t *testing.T) {
	p := &transcode.Profile{
		Kind:        transcode.KindNVENCTranscode,
		VideoCodec:  "hevc_nvenc",
		VideoHeight: 720,
		AudioCodec:  "aac",
		UseNVENC:    true, UseNVDEC: true, Deinterlace: true,
	}
	got := strings.Join(p.BuildArgs(), " ")
	if !strings.Contains(got, "yadif_cuda=") {
		t.Errorf("expected yadif_cuda when UseNVDEC=true; got: %s", got)
	}
}

func TestDeinterlaceUsesCPUVariantWhenGPUDisabled(t *testing.T) {
	p := &transcode.Profile{
		Kind:        transcode.KindCPUTranscode,
		VideoCodec:  "libx264",
		AudioCodec:  "aac",
		Deinterlace: true,
	}
	got := strings.Join(p.BuildArgs(), " ")
	if !strings.Contains(got, "yadif=mode=1") {
		t.Errorf("expected software yadif; got: %s", got)
	}
	if strings.Contains(got, "yadif_cuda") {
		t.Errorf("should NOT use yadif_cuda when UseNVDEC=false; got: %s", got)
	}
}

func TestAudioNormalizeAddsLoudnorm(t *testing.T) {
	p := &transcode.Profile{
		Kind:             transcode.KindCPUTranscode,
		VideoCodec:       "copy",
		AudioCodec:       "aac",
		AudioBitrateKbps: 192, AudioChannels: 2,
		AudioNormalize: true,
		FixTimestamps:  true,
	}
	got := strings.Join(p.BuildArgs(), " ")
	if !strings.Contains(got, "loudnorm=I=-16:TP=-1.5:LRA=11") {
		t.Errorf("audio_normalize should add EBU R128 loudnorm; got: %s", got)
	}
}

func TestSubtitlesAlwaysCopiedAndOptional(t *testing.T) {
	// The -map 0:s? mapping must always be present so missing subtitle
	// streams don't fail the pipeline.
	p := &transcode.Profile{
		Kind:       transcode.KindNVENCTranscode,
		VideoCodec: "h264_nvenc", AudioCodec: "aac",
		UseNVENC: true, UseNVDEC: true,
	}
	got := strings.Join(p.BuildArgs(), " ")
	if !strings.Contains(got, "-map 0:s?") {
		t.Errorf("expected -map 0:s? for optional subtitle stream; got: %s", got)
	}
	if !strings.Contains(got, "-c:s copy") {
		t.Errorf("expected -c:s copy for subtitle passthrough; got: %s", got)
	}
}

func TestDropSubtitlesOmitsSubtitleMapAndEmitsSN(t *testing.T) {
	// Models the cable-clean profile: copy video, re-encode audio to a
	// single AAC track, drop the DVB bitmap-subtitle PID that breaks Plex
	// Live TV. The 0:s? mapping and -c:s copy must be gone; -sn present.
	p := &transcode.Profile{
		Name: "cable-clean", Kind: transcode.KindCPUTranscode,
		VideoCodec: "copy",
		AudioCodec: "aac", AudioBitrateKbps: 192, AudioChannels: 2,
		FixTimestamps: true, DropSubtitles: true,
	}
	got := strings.Join(p.BuildArgs(), " ")

	for _, want := range []string{
		"-fflags +genpts+discardcorrupt",
		"-map 0:v:0",
		"-map 0:a:0",
		"-c:v copy",
		"-c:a aac",
		"-sn",
		"-f mpegts pipe:1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("cable-clean argv missing %q\n  full: %s", want, got)
		}
	}
	if strings.Contains(got, "-map 0:s?") {
		t.Errorf("DropSubtitles must omit -map 0:s?; got: %s", got)
	}
	if strings.Contains(got, "-c:s copy") {
		t.Errorf("DropSubtitles must not copy subtitles; got: %s", got)
	}
}

func TestCustomKindUsesRawArgs(t *testing.T) {
	p := &transcode.Profile{
		Kind:       transcode.KindCustom,
		VideoCodec: "h264_nvenc",
		UseNVDEC:   true,
		FFmpegArgs: []string{"-c:v", "h264_nvenc", "-preset", "p7", "-rc", "vbr_hq", "-cq", "19"},
	}
	got := strings.Join(p.BuildArgs(), " ")
	for _, want := range []string{
		"-hwaccel cuda -hwaccel_output_format cuda",
		"-c:v h264_nvenc -preset p7 -rc vbr_hq -cq 19",
		"-i pipe:0",
		"-f mpegts pipe:1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("custom argv missing %q\n  full: %s", want, got)
		}
	}
}
