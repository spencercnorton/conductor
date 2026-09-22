package dvr

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBitstreamDiagnosticRecognizesMarkersAcrossWrites(t *testing.T) {
	for _, marker := range artifactBitstreamCorruptionMarkers {
		for split := 0; split <= len(marker); split++ {
			t.Run(fmt.Sprintf("%s/split-%d", marker, split), func(t *testing.T) {
				d := newBitstreamDiagnostic()
				for _, fragment := range []string{"  [h264 @ fixture] " + marker[:split], marker[split:] + " detail\r", "\n"} {
					if n, err := d.Write([]byte(fragment)); n != len(fragment) || err != nil {
						t.Fatalf("Write = %d, %v", n, err)
					}
				}
				count, sample := d.result()
				if count != 1 || sample != "[h264 @ fixture] "+marker+" detail" {
					t.Fatalf("count=%d sample=%q", count, sample)
				}
			})
		}
	}
}

func TestBitstreamDiagnosticPreservesPerLineSemantics(t *testing.T) {
	first := "[h264 @ fixture] error while decoding; non-existing PPS; error while decoding"
	input := "co located POCs unavailable\nmmco: unref short failure\n" +
		"Application provided invalid, non monotonically increasing dts\n  " + first + "  \n" +
		"no frame!; Invalid NAL unit" // EOF without newline still counts once.
	for _, size := range []int{1, 7, len(input)} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			d := newBitstreamDiagnostic()
			for remaining := input; len(remaining) > 0; {
				n := min(size, len(remaining))
				_, _ = d.Write([]byte(remaining[:n]))
				remaining = remaining[n:]
			}
			count, sample := d.result()
			if count != 2 || sample != first {
				t.Fatalf("count=%d sample=%q, want 2 and first corruption line", count, sample)
			}
			if again, _ := d.result(); again != count {
				t.Fatalf("EOF counted twice: %d then %d", count, again)
			}
		})
	}
}

func TestBitstreamDiagnosticDoesNotJoinMarkersAcrossLines(t *testing.T) {
	d := newBitstreamDiagnostic()
	_, _ = d.Write([]byte("error while\n decoding\nnon-existing P\nPS\n"))
	if count, sample := d.result(); count != 0 || sample != "" {
		t.Fatalf("separate lines fabricated a marker: count=%d sample=%q", count, sample)
	}
}

func TestBitstreamDiagnosticBoundsHugeUnterminatedLine(t *testing.T) {
	d := newBitstreamDiagnostic()
	_, _ = d.Write(bytes.Repeat([]byte{'x'}, 1024*1024))
	_, _ = d.Write([]byte("error while dec"))
	_, _ = d.Write([]byte("oding tail"))
	if d.text.buf.Len() > maxToolDiagnosticBytes || d.line.buf.Len() > maxToolDiagnosticBytes || len(d.carry) > d.overlap {
		t.Fatalf("diagnostics grew beyond their bounds: text=%d line=%d carry=%d", d.text.buf.Len(), d.line.buf.Len(), len(d.carry))
	}
	count, sample := d.result()
	if count != 1 || !strings.Contains(sample, "error while decoding") {
		t.Fatalf("long-line tail evidence lost: count=%d sample prefix=%q", count, sample[:min(100, len(sample))])
	}
	if len(sample) > maxToolDiagnosticBytes+128 || len(d.String()) > maxToolDiagnosticBytes+128 {
		t.Fatalf("saved diagnostics unbounded: sample=%d text=%d", len(sample), len(d.String()))
	}
}

func TestScanVideoBitstreamCountsBeyondDiagnosticBudget(t *testing.T) {
	benign := strings.Repeat("[null @ fixture] Application provided invalid, non monotonically increasing dts to muxer\n", 1000)
	marker := "[h264 @ fixture] error while decoding MB 33 43, bytestream -5"
	for _, tc := range []struct {
		name     string
		stderr   string
		exitCode int
		want     int
	}{
		{"benign-overflow", benign, 0, 0},
		{"corrupt-tail", benign + marker + "\n", 0, 1},
		{"incomplete-corrupt-tail", benign + marker + "\n", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := filepath.Join(t.TempDir(), "ffmpeg-stderr")
			script := fmt.Sprintf("#!/bin/sh\ncat <<'DIAGNOSTIC_EOF' >&2\n%sDIAGNOSTIC_EOF\nexit %d\n", tc.stderr, tc.exitCode)
			if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			p := &ffmpegMediaProcessor{ffmpeg: stub}
			count, sample, err := p.scanVideoBitstream(context.Background(), "unused.ts")
			if count != tc.want || (err != nil) != (tc.exitCode != 0) {
				t.Fatalf("count=%d err=%v, want count=%d exit=%d", count, err, tc.want, tc.exitCode)
			}
			if tc.want > 0 && sample != marker {
				t.Fatalf("first corruption sample=%q, want %q", sample, marker)
			}
		})
	}
}
