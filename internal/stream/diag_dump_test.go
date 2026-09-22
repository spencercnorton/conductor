package stream

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestSaveRefusalHead_DisabledWithoutDir(t *testing.T) {
	// Must be a no-op: nothing to assert beyond not panicking and not
	// creating anything in the working directory.
	saveRefusalHead("", "audio-origin-refusal", "abc", []byte("data"), discardLogger())
}

func TestSaveRefusalHead_WritesAndSkipsEmpty(t *testing.T) {
	dir := t.TempDir()
	saveRefusalHead(dir, "audio-origin-refusal", "0123456789abcdef", []byte("tsbytes"), discardLogger())
	saveRefusalHead(dir, "audio-origin-refusal", "0123456789abcdef", nil, discardLogger())
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 dump, got %d", len(entries))
	}
	name := entries[0].Name()
	if filepath.Ext(name) != ".ts" {
		t.Fatalf("dump name %q must end in .ts", name)
	}
	body, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "tsbytes" {
		t.Fatalf("dump content %q", body)
	}
}

func TestSaveRefusalHead_PrunesToRetention(t *testing.T) {
	dir := t.TempDir()
	// Pre-seed timestamped-looking files older than anything saveRefusalHead
	// writes; the lexicographic prune must remove the oldest first.
	for i := 0; i < diagRetainFiles+5; i++ {
		name := fmt.Sprintf("audio-origin-refusal-20200101T%06d-old.ts", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	saveRefusalHead(dir, "audio-origin-refusal", "deadbeef", []byte("fresh"), discardLogger())
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != diagRetainFiles {
		t.Fatalf("expected retention at %d files, got %d", diagRetainFiles, len(entries))
	}
	// The newest (just-written) file must have survived the prune.
	found := false
	for _, e := range entries {
		if b, _ := os.ReadFile(filepath.Join(dir, e.Name())); string(b) == "fresh" {
			found = true
		}
	}
	if !found {
		t.Fatal("freshly saved dump was pruned")
	}
}

func TestSaveRefusalHead_SweepsStalePartials(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "audio-origin-refusal-20200101T000000-dead.ts.partial")
	if err := os.WriteFile(stale, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Force the prune path to run by saving enough files to exceed retention.
	for i := 0; i <= diagRetainFiles; i++ {
		name := fmt.Sprintf("audio-origin-refusal-20200102T%06d-old.ts", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	saveRefusalHead(dir, "audio-origin-refusal", "deadbeef", []byte("fresh"), discardLogger())
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale .partial must be swept, stat err=%v", err)
	}
}

func TestSaveRefusalHead_SameSecondSameStreamDoesNotOverwrite(t *testing.T) {
	dir := t.TempDir()
	saveRefusalHead(dir, "audio-origin-refusal", "deadbeef", []byte("one"), discardLogger())
	saveRefusalHead(dir, "audio-origin-refusal", "deadbeef", []byte("two"), discardLogger())
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("same-second saves must not overwrite: got %d files", len(entries))
	}
}
