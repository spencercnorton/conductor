package dvr_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/store"
)

func TestSignedScheduleURLRoundtrip(t *testing.T) {
	tn := dvr.New(nil, "http://test.local:8409", "indexer-key-123", "Test")
	prog := store.EPGSearchHit{
		ProgramID: uuid.New(),
		ChannelID: uuid.New(),
		StartAt:   time.Date(2026, 5, 9, 21, 0, 0, 0, time.UTC),
		EndAt:     time.Date(2026, 5, 9, 22, 0, 0, 0, time.UTC),
		Title:     "Heat",
	}

	// Build the signed URL.
	url := exportedSignedURL(tn, prog)
	if !strings.HasPrefix(url, "http://test.local:8409/dvr/schedule.torrent?") {
		t.Fatalf("unexpected URL: %s", url)
	}
	if !strings.Contains(url, "ec=") || !strings.Contains(url, "sig=") || !strings.Contains(url, "apikey=indexer-key-123") {
		t.Errorf("URL missing required query params: %s", url)
	}

	// Extract ec + sig and round-trip.
	q := urlQuery(url)
	pid, cid, start, end, err := tn.VerifyScheduleURL(q.Get("ec"), q.Get("sig"))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if pid != prog.ProgramID {
		t.Errorf("program id roundtrip: got %v want %v", pid, prog.ProgramID)
	}
	if cid != prog.ChannelID {
		t.Errorf("channel id roundtrip: got %v want %v", cid, prog.ChannelID)
	}
	if !start.Equal(prog.StartAt) {
		t.Errorf("start roundtrip: got %v want %v", start, prog.StartAt)
	}
	if !end.Equal(prog.EndAt) {
		t.Errorf("end roundtrip: got %v want %v", end, prog.EndAt)
	}
}

func TestSignedScheduleURLTamperRejected(t *testing.T) {
	tn := dvr.New(nil, "http://test.local:8409", "indexer-key-123", "Test")
	prog := store.EPGSearchHit{
		ProgramID: uuid.New(),
		ChannelID: uuid.New(),
		StartAt:   time.Now().UTC(),
		EndAt:     time.Now().Add(time.Hour).UTC(),
	}
	url := exportedSignedURL(tn, prog)
	q := urlQuery(url)

	// Flip a bit in ec.
	tampered := q.Get("ec")
	tampered = strings.ReplaceAll(tampered, "A", "B")
	tampered = strings.ReplaceAll(tampered, "a", "b")
	if tampered == q.Get("ec") {
		// No A/a — try a different mutation.
		tampered = "X" + q.Get("ec")
	}
	if _, _, _, _, err := tn.VerifyScheduleURL(tampered, q.Get("sig")); err == nil {
		t.Error("expected tampered ec to be rejected")
	}
}

func TestSignedURLDifferentKeyRejected(t *testing.T) {
	tn1 := dvr.New(nil, "http://test.local:8409", "key-A", "Test")
	tn2 := dvr.New(nil, "http://test.local:8409", "key-B", "Test")
	prog := store.EPGSearchHit{
		ProgramID: uuid.New(),
		ChannelID: uuid.New(),
		StartAt:   time.Now().UTC(),
		EndAt:     time.Now().Add(time.Hour).UTC(),
	}
	url := exportedSignedURL(tn1, prog)
	q := urlQuery(url)
	if _, _, _, _, err := tn2.VerifyScheduleURL(q.Get("ec"), q.Get("sig")); err == nil {
		t.Error("expected URL signed with different key to be rejected")
	}
}

// exportedSignedURL exists so tests can call the now-internal-but-needed
// signing logic via the only public entrypoint that produces it: the
// HTTP search handler. We build a minimal RSS, then extract the link.
//
// Cheap shortcut: dvr.Torznab.signedScheduleURL is unexported. We exercise
// it via a test helper that builds a search response and extracts the link.
// This keeps the API surface lean.
func exportedSignedURL(tn *dvr.Torznab, h store.EPGSearchHit) string {
	// We can't call the unexported method directly, so we synthesize the
	// URL via an in-package shim added below. To keep this file
	// independent, we use the fact that Torznab encodes URLs via the
	// signedScheduleURL logic which is reached by buildAndExtract.
	return tn.SignedScheduleURLForTest(h)
}

func urlQuery(u string) urlValuesMap {
	i := strings.Index(u, "?")
	out := urlValuesMap{}
	if i < 0 {
		return out
	}
	for _, pair := range strings.Split(u[i+1:], "&") {
		eq := strings.Index(pair, "=")
		if eq < 0 {
			continue
		}
		out[pair[:eq]] = pair[eq+1:]
	}
	return out
}

type urlValuesMap map[string]string

func (u urlValuesMap) Get(k string) string {
	v, ok := u[k]
	if !ok {
		return ""
	}
	// Handle simple URL-encoding for the bits we use here.
	v = strings.ReplaceAll(v, "%3D", "=")
	v = strings.ReplaceAll(v, "+", " ")
	return v
}
