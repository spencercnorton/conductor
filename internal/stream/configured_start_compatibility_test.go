package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func TestConfiguredStartupHealthyMultiPicturePES(t *testing.T) {
	ffmpeg, ffprobe := requireLateJoinFFmpeg(t)
	for _, interlaced := range []bool{false, true} {
		t.Run(strconv.FormatBool(interlaced), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			args := []string{"-hide_banner", "-loglevel", "error", "-nostdin", "-f", "lavfi", "-i", "testsrc2=size=320x192:rate=25:duration=6", "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=6", "-c:v", "libx264", "-threads", "2", "-preset", "ultrafast", "-tune", "zerolatency", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0", "-b:v", "2M", "-c:a", "aac", "-b:a", "128k"}
			if interlaced {
				args = append(args, "-flags", "+ildct+ilme", "-x264-params", "tff=1")
			}
			args = append(args, "-f", "mpegts", "pipe:1")
			original, err := exec.CommandContext(ctx, ffmpeg, args...).Output()
			if err != nil {
				t.Fatal(err)
			}
			media := joinFirstTwoVideoPESForTest(t, original)
			decode := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-threads", "2", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-")
			decode.Stdin = bytes.NewReader(media)
			diagnostics, err := decode.CombinedOutput()
			if err != nil || len(diagnostics) > 0 {
				t.Fatalf("healthy repacked full decode: %v\n%s", err, diagnostics)
			}
			count := exec.CommandContext(ctx, ffprobe, "-v", "error", "-count_frames", "-show_entries", "stream=codec_type,nb_read_frames,field_order", "-of", "json", "pipe:0")
			count.Stdin = bytes.NewReader(media)
			output, err := count.Output()
			if err != nil {
				t.Fatal(err)
			}
			var result struct {
				Streams []struct {
					CodecType  string `json:"codec_type"`
					Frames     string `json:"nb_read_frames"`
					FieldOrder string `json:"field_order"`
				}
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatal(err)
			}
			frames, audio, field := 0, 0, ""
			for _, s := range result.Streams {
				n, _ := strconv.Atoi(s.Frames)
				if s.CodecType == "video" {
					frames = n
					field = s.FieldOrder
				}
				if s.CodecType == "audio" {
					audio = n
				}
			}
			if frames != 150 || audio < 280 {
				t.Fatalf("decoded video=%d audio=%d wantall150 video andmatching audio", frames, audio)
			}
			t.Logf("healthy repacked MPEG-TS fully decodes video=%d audio=%d field_order=%s bytes=%d", frames, audio, field, len(media))
			validator := &ffprobeMediaPrefixValidator{ffprobeBinary: ffprobe, available: true}
			got, err := validator.Validate(ctx, media)
			if err != nil || got.kind != prefixReady {
				t.Fatalf("healthy multiple-picture PES refused: kind=%v reason=%s err=%v", got.kind, got.reason, err)
			}
		})
	}
}

// MPEG-TS video PES may carry several complete access units. This losslessly
// preserves both pictures' elementary bytes and every audio byte, retaining the
// original PCR and continuity counters while removing only the second video
// PES header/PUSI. The decoder derives that CFR picture's timestamp from its
// frame cadence; later PES timestamps remain untouched.
func joinFirstTwoVideoPESForTest(t *testing.T, original []byte) []byte {
	t.Helper()
	data := append([]byte(nil), original...)
	pid, ok := videoPIDFromTS(data)
	if !ok {
		t.Fatal("video PID missing")
	}
	starts := 0
	for pos := 0; pos+tsPacketSize <= len(data); pos += tsPacketSize {
		p := data[pos : pos+tsPacketSize]
		if int(p[1]&31)<<8|int(p[2]) != pid || p[1]&0x40 == 0 {
			continue
		}
		starts++
		payload, ok := tsPayload(p)
		if !ok || len(payload) < 9 || !bytes.Equal(payload[:3], []byte{0, 0, 1}) {
			t.Fatal("fixture PES header missing")
		}
		if starts == 1 {
			if payload[4] != 0 || payload[5] != 0 {
				t.Fatal("fixture first video PES is bounded")
			}
			continue
		}
		if starts != 2 {
			continue
		}
		h := 9 + int(payload[8])
		if h > len(payload) {
			t.Fatal("fixture second PES header split")
		}
		es := append([]byte(nil), payload[h:]...)
		replacement := bytes.Repeat([]byte{0xff}, tsPacketSize)
		copy(replacement[:4], p[:4])
		replacement[1] &= ^byte(0x40)
		replacement[3] |= 0x30
		replacement[4] = byte(183 - len(es))
		replacement[5] = 0
		if p[3]&0x20 != 0 {
			copy(replacement[5:5+int(p[4])], p[5:5+int(p[4])])
		}
		copy(replacement[5+int(replacement[4]):], es)
		copy(p, replacement)
		return data
	}
	t.Fatal("fixture second video PES missing")
	return nil
}
