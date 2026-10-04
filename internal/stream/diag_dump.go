package stream

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Refusal-head dumps: when a startup gate refuses an upstream (for example
// the audio-origin proof), the bytes it examined are exactly the fixture the
// fixture-first rule demands before tuning that gate — and the conditions
// are transient (2026-08-29: a −16s converging join skew on WE TV at 17:42Z
// did not reproduce two hours later). Persist the gate's private buffer so
// the next occurrence collects itself.
//
// Disabled unless a directory is configured (CONDUCTOR_DIAG_DIR). Bounded:
// newest diagRetainFiles files are kept, everything older is pruned on each
// save. Best-effort by design — a dump failure must never affect the stream
// path. Known gap: prune-on-save races between concurrent refusals can leave
// one extra file until the next save; a mutex is not worth it.
const diagRetainFiles = 12

func saveRefusalHead(dir, kind, streamID string, data []byte, logger *slog.Logger) {
	if dir == "" || len(data) == 0 {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		logger.Debug("diag dump: mkdir failed", "dir", dir, "err", err)
		return
	}
	// Prune on entry as well as after a successful save: a run of failing
	// writes must not accumulate partials with no success to sweep them
	// (review follow-up).
	pruneDiagDir(dir, logger)
	short := streamID
	if len(short) > 8 {
		short = short[:8]
	}
	// Nanoseconds keep same-second refusals of the same stream from
	// overwriting each other (review finding).
	name := fmt.Sprintf("%s-%s-%s.ts", kind, time.Now().UTC().Format("20060102T150405.000000000"), short)
	tmp := filepath.Join(dir, name+".partial")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		logger.Debug("diag dump: write failed", "path", tmp, "err", err)
		return
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		logger.Debug("diag dump: rename failed", "path", tmp, "err", err)
		_ = os.Remove(tmp)
		return
	}
	logger.Info("diag dump: refusal head saved",
		"kind", kind, "path", filepath.Join(dir, name), "bytes", len(data))
	pruneDiagDir(dir, logger)
}

func pruneDiagDir(dir string, logger *slog.Logger) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// A .partial only survives a failed write or rename; it is never
		// current, so sweep any that exist rather than letting failures
		// accumulate them unbounded.
		if filepath.Ext(e.Name()) == ".partial" {
			_ = os.Remove(filepath.Join(dir, e.Name()))
			continue
		}
		if filepath.Ext(e.Name()) == ".ts" {
			files = append(files, e.Name())
		}
	}
	if len(files) <= diagRetainFiles {
		return
	}
	// The timestamp in the name sorts lexicographically; oldest first.
	sort.Strings(files)
	for _, name := range files[:len(files)-diagRetainFiles] {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			logger.Debug("diag dump: prune failed", "name", name, "err", err)
		}
	}
}
