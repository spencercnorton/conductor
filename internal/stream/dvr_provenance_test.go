package stream

import (
	"bytes"
	"testing"
	"time"
)

func TestWriteDVRChunkUsesProvenanceNotLuminance(t *testing.T) {
	var output bytes.Buffer
	blackRealMedia := make([]byte, 188*7)

	n, err := writeDVRChunk(&output, startupChunk{
		data: blackRealMedia, realAt: time.Now(),
	})
	if err != nil || n != len(blackRealMedia) || !bytes.Equal(output.Bytes(), blackRealMedia) {
		t.Fatalf("real dark media was not retained: n=%d err=%v bytes=%d", n, err, output.Len())
	}

	slate := bytes.Repeat([]byte{0x47}, 188*7)
	n, err = writeDVRChunk(&output, startupChunk{data: slate})
	if err != nil || n != 0 {
		t.Fatalf("synthetic slate write=(%d,%v), want omitted", n, err)
	}
	if !bytes.Equal(output.Bytes(), blackRealMedia) {
		t.Fatal("synthetic slate reached the DVR artifact")
	}
}

func TestDVRMediaCoverageTracksOnlyInProgramRealMediaGaps(t *testing.T) {
	start := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	var coverage DVRMediaCoverage

	coverage.observe(start.Add(-30*time.Second), start, end)
	coverage.observe(start.Add(time.Second), start, end)
	coverage.observe(start.Add(4*time.Second), start, end)
	gapStart := start.Add(5 * time.Second)
	gapEnd := gapStart.Add(45 * time.Second)
	coverage.observe(gapStart, start, end)
	coverage.observe(gapEnd, start, end)
	coverage.observe(end.Add(-time.Second), start, end)
	coverage.observe(end.Add(5*time.Minute), start, end)

	if coverage.FirstRealChunk != start.Add(-30*time.Second) ||
		coverage.LastRealChunk != end.Add(5*time.Minute) {
		t.Fatalf("coverage endpoints=%+v", coverage)
	}
	if coverage.MaxRealGap != 45*time.Second ||
		!coverage.MaxGapStart.Equal(gapStart) || !coverage.MaxGapEnd.Equal(gapEnd) {
		t.Fatalf("maximum in-program gap=%+v, want %s..%s",
			coverage, gapStart, gapEnd)
	}
}

func TestDVRMediaCoverageClipsPaddingOnlyGaps(t *testing.T) {
	start := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	var pre, post DVRMediaCoverage
	pre.observe(start.Add(-5*time.Minute), start, end)
	pre.observe(start.Add(-time.Minute), start, end)
	post.observe(end.Add(time.Minute), start, end)
	post.observe(end.Add(5*time.Minute), start, end)
	if pre.MaxRealGap != 0 || post.MaxRealGap != 0 {
		t.Fatalf("padding-only gaps leaked into programme evidence: pre=%+v post=%+v",
			pre, post)
	}
}
