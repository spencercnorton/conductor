package transcode_test

import (
	"github.com/spencercnorton/conductor/internal/transcode"
	"testing"
)

func TestInputClockRepairRequiresOwnedAudioFilterAndCopiedVideo(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*transcode.Profile)
		want   bool
	}{
		{"cable_clean", func(*transcode.Profile) {}, true},
		{"default_copy_video", func(p *transcode.Profile) { p.VideoCodec = "" }, true},
		{"audio_copy", func(p *transcode.Profile) { p.AudioCodec = "copy" }, false},
		{"audio_default", func(p *transcode.Profile) { p.AudioCodec = "" }, false},
		{"video_encode", func(p *transcode.Profile) { p.VideoCodec = "libx264" }, false},
		{"custom", func(p *transcode.Profile) { p.Kind = transcode.KindCustom }, false},
		{"passthrough", func(p *transcode.Profile) { p.Kind = transcode.KindPassthrough }, false},
		{"no_clock_filter", func(p *transcode.Profile) { p.FixTimestamps = false }, false},
		{"operator_override", func(p *transcode.Profile) { p.FFmpegArgs = []string{"-af", "anull"} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &transcode.Profile{Kind: transcode.KindCPUTranscode, VideoCodec: "copy", AudioCodec: "aac", FixTimestamps: true}
			tc.mutate(p)
			if got := p.SupportsInputAudioClockRepair(); got != tc.want {
				t.Fatalf("capability=%v want %v", got, tc.want)
			}
		})
	}
	var p *transcode.Profile
	if p.SupportsInputAudioClockRepair() {
		t.Fatal("nil profile eligible")
	}
}
