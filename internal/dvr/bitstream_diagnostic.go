package dvr

import (
	"bytes"
	"strings"
)

// bitstreamDiagnostic keeps bounded operator diagnostics while counting every
// corruption-bearing stderr line. Benign FFmpeg output may exhaust the text
// budget without saying anything about corruption later in the recording.
// exec.Cmd calls Write serially and joins its stderr copier before Run returns.
type bitstreamDiagnostic struct {
	text       limitedDiagnostic
	line       limitedDiagnostic
	carry      []byte
	overlap    int
	lineMarker string
	corruption int
	sample     string
}

func newBitstreamDiagnostic() *bitstreamDiagnostic {
	overlap := 0
	for _, marker := range artifactBitstreamCorruptionMarkers {
		overlap = max(overlap, len(marker)-1)
	}
	return &bitstreamDiagnostic{carry: make([]byte, 0, overlap), overlap: overlap}
}

func (d *bitstreamDiagnostic) Write(p []byte) (int, error) {
	n, _ := d.text.Write(p)
	for len(p) > 0 {
		fragment, rest, newline := bytes.Cut(p, []byte{'\n'})
		d.observe(fragment)
		if newline {
			d.finishLine()
		}
		p = rest
	}
	return n, nil
}

func (d *bitstreamDiagnostic) observe(fragment []byte) {
	if d.sample == "" {
		_, _ = d.line.Write(fragment)
	}
	if d.lineMarker != "" {
		return // The existing scan counts at most one event per stderr line.
	}

	// Only a marker-sized overlap is needed across Write boundaries. The
	// complete fragment is searched in place, even for a huge unterminated
	// line; neither the carry nor the saved line grows with that input.
	boundary := make([]byte, 0, len(d.carry)+min(len(fragment), d.overlap))
	boundary = append(boundary, d.carry...)
	boundary = append(boundary, fragment[:min(len(fragment), d.overlap)]...)
	for _, marker := range artifactBitstreamCorruptionMarkers {
		if bytes.Contains(fragment, []byte(marker)) || bytes.Contains(boundary, []byte(marker)) {
			d.lineMarker = marker
			d.corruption++
			return
		}
	}
	if len(fragment) >= d.overlap {
		d.carry = append(d.carry[:0], fragment[len(fragment)-d.overlap:]...)
	} else {
		keep := min(len(d.carry), d.overlap-len(fragment))
		copy(d.carry, d.carry[len(d.carry)-keep:])
		d.carry = append(d.carry[:keep], fragment...)
	}
}

func (d *bitstreamDiagnostic) finishLine() {
	if d.lineMarker != "" && d.sample == "" {
		d.sample = d.line.String()
		if !strings.Contains(d.sample, d.lineMarker) {
			// A very long line may put its marker beyond the saved prefix.
			// Preserve that evidence without retaining the entire line.
			d.sample = d.lineMarker + ": " + d.sample
		}
	}
	d.line.buf.Reset()
	d.line.truncated = false
	d.carry = d.carry[:0]
	d.lineMarker = ""
}

// result flushes the final stderr line when FFmpeg omits a trailing newline.
// Call it only after the command has joined its stderr writer.
func (d *bitstreamDiagnostic) result() (int, string) {
	d.finishLine()
	return d.corruption, d.sample
}

func (d *bitstreamDiagnostic) String() string { return d.text.String() }
