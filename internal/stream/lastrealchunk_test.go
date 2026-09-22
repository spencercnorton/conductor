package stream

import (
	"testing"
	"time"
)

// TestLastRealChunkIgnoresSyntheticFiller pins the property the DVR salvage
// rule depends on: only REAL upstream content advances lastRealChunk.
//
// During an outage the recording file keeps growing regardless — the Source
// Unavailable slate is fanned out to subscribers, and a dead upstream can
// answer with a finite black placeholder that downloads far faster than real
// time (observed 2026-08-10: ~130 minutes of black written into a recording
// during a 93-second gap). So neither file growth nor wall-clock stop time can
// tell "the program kept arriving" from "bytes kept arriving". This can.
func TestLastRealChunkIgnoresSyntheticFiller(t *testing.T) {
	var ss subscriberSet
	ss.init("test", testLogger())

	if got := ss.LastRealChunk(); !got.IsZero() {
		t.Fatalf("LastRealChunk before any delivery = %v, want zero", got)
	}

	// Real content advances it.
	ss.fanout([]byte("real program bytes"))
	afterReal := ss.LastRealChunk()
	if afterReal.IsZero() {
		t.Fatal("LastRealChunk still zero after fanout of real content")
	}

	// Synthetic filler must NOT. This is the whole point: an outage that keeps
	// the file growing must not look like continued program coverage.
	time.Sleep(2 * time.Millisecond)
	for range 50 {
		ss.fanoutSynthetic([]byte("slate filler bytes"))
	}
	if got := ss.LastRealChunk(); !got.Equal(afterReal) {
		t.Fatalf("synthetic filler advanced LastRealChunk: %v -> %v", afterReal, got)
	}

	// Real content resuming advances it again.
	time.Sleep(2 * time.Millisecond)
	ss.fanout([]byte("upstream recovered"))
	if got := ss.LastRealChunk(); !got.After(afterReal) {
		t.Fatalf("LastRealChunk did not advance when real content resumed: %v", got)
	}
}
