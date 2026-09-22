package dvr

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
)

func artifactInspectionRecording(
	path string,
	state string,
	content []byte,
) store.DVRRecording {
	digest := sha256.Sum256(content)
	return store.DVRRecording{
		ID:             uuid.New(),
		OutputPath:     path,
		ArtifactState:  state,
		ArtifactBytes:  int64(len(content)),
		ArtifactSHA256: digest[:],
	}
}

func TestInspectCurrentDVRArtifactHashEvidence(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "artifact.ts")
	original := []byte("same-size-validated-media")
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("identical validated artifact is usable", func(t *testing.T) {
		rec := artifactInspectionRecording(path, "validated", original)
		inspection, err := inspectCurrentDVRArtifact(ctx, rec, osArtifactFilesystem{})
		if err != nil || !inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want usable", inspection, err)
		}
	})

	t.Run("same-size bit flip is deterministic mismatch", func(t *testing.T) {
		rec := artifactInspectionRecording(path, "validated", original)
		changed := append([]byte(nil), original...)
		changed[len(changed)/2] ^= 0x40
		if err := os.WriteFile(path, changed, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.WriteFile(path, original, 0o644); err != nil {
				t.Errorf("restore fixture: %v", err)
			}
		})

		inspection, err := inspectCurrentDVRArtifact(ctx, rec, osArtifactFilesystem{})
		if err != nil || inspection.usable ||
			!strings.Contains(inspection.demotionReason, "SHA-256 evidence changed") {
			t.Fatalf("inspection=%+v err=%v, want SHA-256 demotion", inspection, err)
		}
	})

	t.Run("legacy owner remains size-only", func(t *testing.T) {
		changed := append([]byte(nil), original...)
		changed[0] ^= 0x20
		if err := os.WriteFile(path, changed, 0o644); err != nil {
			t.Fatal(err)
		}
		rec := artifactInspectionRecording(path, "legacy", original)
		rec.ArtifactSHA256 = nil

		inspection, err := inspectCurrentDVRArtifact(ctx, rec, osArtifactFilesystem{})
		if err != nil || !inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want legacy size-only usable", inspection, err)
		}
	})

	t.Run("validated owner requires exact persisted digest", func(t *testing.T) {
		rec := artifactInspectionRecording(path, "validated", original)
		rec.ArtifactSHA256 = rec.ArtifactSHA256[:sha256.Size-1]

		inspection, err := inspectCurrentDVRArtifact(ctx, rec, osArtifactFilesystem{})
		var indeterminate *IndeterminateArtifactInspectionError
		if err == nil || errors.As(err, &indeterminate) ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want fatal persisted-evidence error", inspection, err)
		}
	})
}

type artifactTestFilesystem struct {
	stat  func(string) (os.FileInfo, error)
	lstat func(string) (os.FileInfo, error)
	open  func(string) (artifactReadFile, error)
}

func (f artifactTestFilesystem) Stat(path string) (os.FileInfo, error) {
	return f.stat(path)
}

func (f artifactTestFilesystem) Lstat(path string) (os.FileInfo, error) {
	return f.lstat(path)
}

func (f artifactTestFilesystem) Open(path string) (artifactReadFile, error) {
	return f.open(path)
}

type failingArtifactReadFile struct {
	info os.FileInfo
	err  error
}

func (f *failingArtifactReadFile) Read([]byte) (int, error) { return 0, f.err }
func (f *failingArtifactReadFile) Stat() (os.FileInfo, error) {
	return f.info, nil
}
func (f *failingArtifactReadFile) Close() error { return nil }

type mutatingArtifactReadFile struct {
	*os.File
	once   sync.Once
	mutate func()
}

func (f *mutatingArtifactReadFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if n > 0 {
		f.once.Do(f.mutate)
	}
	return n, err
}

func TestInspectCurrentDVRArtifactIndeterminateIO(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	firstPath := filepath.Join(root, "first.ts")
	secondPath := filepath.Join(root, "second.ts")
	content := []byte("validated-media-for-race")
	for _, path := range []string{firstPath, secondPath} {
		if err := os.WriteFile(path, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	firstInfo, err := os.Stat(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	secondInfo, err := os.Stat(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	rec := artifactInspectionRecording(firstPath, "validated", content)

	t.Run("existing file moved to another filesystem is indeterminate", func(t *testing.T) {
		moved := rec
		moved.ArtifactFilesystemID = "dev:not-the-fixture-filesystem"

		inspection, err := inspectCurrentDVRArtifact(
			ctx, moved, osArtifactFilesystem{})
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.As(err, &indeterminate) ||
			!strings.Contains(indeterminate.Operation, "filesystem identity") ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want typed filesystem move", inspection, err)
		}
	})

	t.Run("first-seen missing owner is typed indeterminate", func(t *testing.T) {
		unreachable := rec
		unreachable.OutputPath = filepath.Join(root, "missing-mount", "artifact.ts")

		inspection, err := inspectCurrentDVRArtifact(
			ctx, unreachable, osArtifactFilesystem{})
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.As(err, &indeterminate) ||
			!strings.Contains(indeterminate.Operation, "missing canonical artifact filesystem") ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want typed unbound-owner failure", inspection, err)
		}
	})

	t.Run("bound owner with unreachable parent is typed indeterminate", func(t *testing.T) {
		unreachable := rec
		unreachable.OutputPath = filepath.Join(root, "missing-mount", "artifact.ts")
		filesystemID, ok := artifactFilesystemID(firstInfo)
		if !ok {
			t.Fatal("fixture filesystem identity unavailable")
		}
		unreachable.ArtifactFilesystemID = filesystemID

		inspection, err := inspectCurrentDVRArtifact(
			ctx, unreachable, osArtifactFilesystem{})
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.As(err, &indeterminate) ||
			!strings.Contains(indeterminate.Operation, "parent directory") ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want typed parent failure", inspection, err)
		}
	})

	t.Run("bound missing owner demotes only on matching parent filesystem", func(t *testing.T) {
		missing := rec
		missing.OutputPath = filepath.Join(root, "missing.ts")
		filesystemID, ok := artifactFilesystemID(firstInfo)
		if !ok {
			t.Fatal("fixture filesystem identity unavailable")
		}
		missing.ArtifactFilesystemID = filesystemID

		inspection, err := inspectCurrentDVRArtifact(
			ctx, missing, osArtifactFilesystem{})
		if err != nil || inspection.usable ||
			!strings.Contains(inspection.demotionReason, "missing") ||
			inspection.filesystemID != filesystemID {
			t.Fatalf("inspection=%+v err=%v, want same-filesystem demotion", inspection, err)
		}

		missing.ArtifactFilesystemID = "dev:not-the-parent-filesystem"
		inspection, err = inspectCurrentDVRArtifact(
			ctx, missing, osArtifactFilesystem{})
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.As(err, &indeterminate) ||
			!strings.Contains(indeterminate.Operation, "parent filesystem identity") ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want mismatched-parent retention", inspection, err)
		}
	})

	t.Run("read error is typed indeterminate", func(t *testing.T) {
		readErr := errors.New("simulated media mount read failure")
		fsys := artifactTestFilesystem{
			stat:  func(string) (os.FileInfo, error) { return firstInfo, nil },
			lstat: func(path string) (os.FileInfo, error) { return os.Lstat(path) },
			open: func(string) (artifactReadFile, error) {
				return &failingArtifactReadFile{info: firstInfo, err: readErr}, nil
			},
		}

		inspection, err := inspectCurrentDVRArtifact(ctx, rec, fsys)
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.As(err, &indeterminate) || !errors.Is(err, readErr) ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want typed read failure", inspection, err)
		}
	})

	t.Run("path replacement during hash is typed indeterminate", func(t *testing.T) {
		statCalls := 0
		fsys := artifactTestFilesystem{
			stat: func(string) (os.FileInfo, error) {
				statCalls++
				if statCalls == 1 {
					return firstInfo, nil
				}
				return secondInfo, nil
			},
			lstat: func(path string) (os.FileInfo, error) { return os.Lstat(path) },
			open: func(string) (artifactReadFile, error) {
				return os.Open(firstPath)
			},
		}

		inspection, err := inspectCurrentDVRArtifact(ctx, rec, fsys)
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.As(err, &indeterminate) ||
			!strings.Contains(indeterminate.Operation, "path stability") ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want typed path race", inspection, err)
		}
	})

	t.Run("preserved mtime mutation is caught by change version", func(t *testing.T) {
		if err := os.WriteFile(firstPath, content, 0o644); err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(firstPath)
		if err != nil {
			t.Fatal(err)
		}
		rec := artifactInspectionRecording(firstPath, "validated", content)
		filesystemID, ok := artifactFilesystemID(before)
		if !ok {
			t.Fatal("fixture filesystem identity unavailable")
		}
		rec.ArtifactFilesystemID = filesystemID
		fsys := artifactTestFilesystem{
			stat:  os.Stat,
			lstat: os.Lstat,
			open: func(string) (artifactReadFile, error) {
				file, openErr := os.Open(firstPath)
				if openErr != nil {
					return nil, openErr
				}
				return &mutatingArtifactReadFile{
					File: file,
					mutate: func() {
						// Make the change time observably newer, then deliberately
						// restore mtime to prove mtime-only stability is insufficient.
						time.Sleep(5 * time.Millisecond)
						writer, writeErr := os.OpenFile(firstPath, os.O_WRONLY, 0)
						if writeErr != nil {
							t.Fatal(writeErr)
						}
						changed := []byte{content[0] ^ 0x20}
						if _, writeErr = writer.WriteAt(changed, 0); writeErr != nil {
							_ = writer.Close()
							t.Fatal(writeErr)
						}
						if writeErr = writer.Sync(); writeErr != nil {
							_ = writer.Close()
							t.Fatal(writeErr)
						}
						if writeErr = writer.Close(); writeErr != nil {
							t.Fatal(writeErr)
						}
						if writeErr = os.Chtimes(
							firstPath, before.ModTime(), before.ModTime(),
						); writeErr != nil {
							t.Fatal(writeErr)
						}
					},
				}, nil
			},
		}

		inspection, err := inspectCurrentDVRArtifact(ctx, rec, fsys)
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.As(err, &indeterminate) ||
			!strings.Contains(indeterminate.Operation, "stability after hashing") ||
			inspection.usable || inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want typed ctime race", inspection, err)
		}
		after, statErr := os.Stat(firstPath)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if !after.ModTime().Equal(before.ModTime()) {
			t.Fatalf("fixture mtime changed: before=%s after=%s",
				before.ModTime(), after.ModTime())
		}
	})
}

type startupArtifactTestStore struct {
	mu sync.Mutex

	rows        []store.DVRRecording
	listErr     error
	bindErr     error
	demoteErr   error
	bindCalls   int
	demoteCalls int
}

func (s *startupArtifactTestStore) ListCurrentDVRArtifacts(
	context.Context,
) ([]store.DVRRecording, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.DVRRecording(nil), s.rows...), nil
}

func (s *startupArtifactTestStore) BindCurrentDVRArtifactFilesystem(
	_ context.Context,
	id uuid.UUID,
	path, filesystemID string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bindCalls++
	if s.bindErr != nil {
		return s.bindErr
	}
	for index := range s.rows {
		if s.rows[index].ID == id && s.rows[index].OutputPath == path {
			s.rows[index].ArtifactFilesystemID = filesystemID
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *startupArtifactTestStore) DemoteMissingDVRArtifact(
	_ context.Context,
	id uuid.UUID,
	path, expectedFilesystemID, _ string,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.demoteCalls++
	if s.demoteErr != nil {
		return false, s.demoteErr
	}
	for index := range s.rows {
		row := &s.rows[index]
		if row.ID == id && row.OutputPath == path && row.ArtifactCurrent &&
			row.ArtifactFilesystemID == expectedFilesystemID {
			row.ArtifactCurrent = false
			row.ArtifactState = "missing"
			return true, nil
		}
	}
	return false, nil
}

func (s *startupArtifactTestStore) mutationCounts() (binds, demotions int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bindCalls, s.demoteCalls
}

func TestUsableLegacyArtifactBindsExactFilesystem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.ts")
	content := []byte("legacy-artifact")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	rec := artifactInspectionRecording(path, "legacy", content)
	rec.ArtifactCurrent = true
	rec.ArtifactPath = path
	rec.ArtifactSHA256 = nil
	inspection, err := inspectCurrentDVRArtifact(
		context.Background(), rec, osArtifactFilesystem{})
	if err != nil || !inspection.usable || inspection.filesystemID == "" {
		t.Fatalf("inspection=%+v err=%v, want usable filesystem evidence", inspection, err)
	}

	testStore := &startupArtifactTestStore{rows: []store.DVRRecording{rec}}
	check, err := applyCurrentDVRArtifactInspection(
		context.Background(), testStore, rec, inspection)
	if err != nil || !check.Usable || check.Demoted {
		t.Fatalf("check=%+v err=%v, want usable bound owner", check, err)
	}
	if binds, demotions := testStore.mutationCounts(); binds != 1 || demotions != 0 {
		t.Fatalf("database mutations binds=%d demotions=%d, want one bind",
			binds, demotions)
	}

	bindErr := errors.New("simulated filesystem binding database failure")
	failingStore := &startupArtifactTestStore{
		rows: []store.DVRRecording{rec}, bindErr: bindErr,
	}
	check, err = applyCurrentDVRArtifactInspection(
		context.Background(), failingStore, rec, inspection)
	if !errors.Is(err, bindErr) || check.Usable || check.Demoted {
		t.Fatalf("check=%+v err=%v, want fatal binding failure", check, err)
	}

	invalidStore := &startupArtifactTestStore{rows: []store.DVRRecording{rec}}
	check, err = applyCurrentDVRArtifactInspection(
		context.Background(), invalidStore, rec, currentArtifactInspection{
			demotionReason: "canonical DVR artifact is empty",
			filesystemID:   inspection.filesystemID,
		})
	var indeterminate *IndeterminateArtifactInspectionError
	if !errors.As(err, &indeterminate) || check.Usable || check.Demoted {
		t.Fatalf("check=%+v err=%v, want unbound invalid artifact retained", check, err)
	}
	if binds, demotions := invalidStore.mutationCounts(); binds != 0 || demotions != 0 {
		t.Fatalf("untrusted invalid artifact mutated database: binds=%d demotions=%d",
			binds, demotions)
	}
}

func TestStartupRetainsUnboundInvalidArtifactAndContinues(t *testing.T) {
	root := t.TempDir()
	invalidPath := filepath.Join(root, "a-invalid.ts")
	missingPath := filepath.Join(root, "b-missing.ts")
	if err := os.WriteFile(invalidPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	filesystemID, ok := artifactFilesystemID(rootInfo)
	if !ok {
		t.Fatal("fixture filesystem identity unavailable")
	}
	invalid := store.DVRRecording{
		ID: uuid.New(), ArtifactCurrent: true, ArtifactState: "legacy",
		OutputPath: invalidPath, ArtifactPath: invalidPath, ArtifactBytes: 1,
	}
	missing := store.DVRRecording{
		ID: uuid.New(), ArtifactCurrent: true, ArtifactState: "legacy",
		OutputPath: missingPath, ArtifactPath: missingPath, ArtifactBytes: 1,
		ArtifactFilesystemID: filesystemID,
	}
	testStore := &startupArtifactTestStore{
		rows: []store.DVRRecording{invalid, missing},
	}

	summary, err := reconcileCurrentDVRArtifactsWith(
		context.Background(), testStore, nil, osArtifactFilesystem{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Indeterminate != 1 || summary.Demoted != 1 ||
		summary.InspectionBudgetExhausted {
		t.Fatalf("startup summary=%+v, want one retained and one demoted", summary)
	}
	if binds, demotions := testStore.mutationCounts(); binds != 0 || demotions != 1 {
		t.Fatalf("database mutations binds=%d demotions=%d, want no bind and one demotion",
			binds, demotions)
	}
	testStore.mu.Lock()
	defer testStore.mu.Unlock()
	if !testStore.rows[0].ArtifactCurrent ||
		testStore.rows[0].ArtifactFilesystemID != "" {
		t.Fatalf("unbound invalid owner changed: %+v", testStore.rows[0])
	}
	if testStore.rows[1].ArtifactCurrent ||
		testStore.rows[1].ArtifactState != "missing" {
		t.Fatalf("later missing owner was not demoted: %+v", testStore.rows[1])
	}
}

type closeIgnoringArtifactReadFile struct {
	info       os.FileInfo
	started    chan struct{}
	release    chan struct{}
	closed     chan struct{}
	returned   chan struct{}
	startOnce  sync.Once
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func (f *closeIgnoringArtifactReadFile) Read([]byte) (int, error) {
	f.startOnce.Do(func() { close(f.started) })
	defer close(f.returned)
	<-f.release
	return 0, io.EOF
}

func (f *closeIgnoringArtifactReadFile) Stat() (os.FileInfo, error) { return f.info, nil }
func (f *closeIgnoringArtifactReadFile) Close() error {
	f.closeCalls.Add(1)
	f.closeOnce.Do(func() { close(f.closed) })
	return nil
}

type artifactInspectionTestResult struct {
	inspection currentArtifactInspection
	err        error
}

func TestInspectCurrentDVRArtifactPreservesContextAfterBlockedStat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.ts")
	content := []byte("legacy-media")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := artifactInspectionRecording(path, "legacy", content)
	rec.ArtifactSHA256 = nil
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	fsys := artifactTestFilesystem{
		stat: func(string) (os.FileInfo, error) {
			close(started)
			<-release
			return info, nil
		},
		lstat: func(string) (os.FileInfo, error) {
			return nil, errors.New("unexpected lstat after successful stat")
		},
		open: func(string) (artifactReadFile, error) {
			return nil, errors.New("unexpected open for legacy artifact")
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultC := make(chan artifactInspectionTestResult, 1)
	go func() {
		inspection, inspectErr := inspectCurrentDVRArtifact(ctx, rec, fsys)
		resultC <- artifactInspectionTestResult{inspection: inspection, err: inspectErr}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("artifact inspection never entered blocked stat")
	}
	cancel()
	releaseOnce.Do(func() { close(release) })
	select {
	case got := <-resultC:
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.Is(got.err, context.Canceled) || errors.As(got.err, &indeterminate) ||
			got.inspection.usable || got.inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want context cancellation after stat",
				got.inspection, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("artifact inspection did not return after blocked stat release")
	}
}

type lateOpenArtifactReadFile struct {
	info       os.FileInfo
	closeCalls atomic.Int32
	readCalls  atomic.Int32
	statCalls  atomic.Int32
}

func (f *lateOpenArtifactReadFile) Read([]byte) (int, error) {
	f.readCalls.Add(1)
	return 0, io.EOF
}

func (f *lateOpenArtifactReadFile) Stat() (os.FileInfo, error) {
	f.statCalls.Add(1)
	return f.info, nil
}

func (f *lateOpenArtifactReadFile) Close() error {
	f.closeCalls.Add(1)
	return nil
}

func TestInspectCurrentDVRArtifactClosesOpenCompletedAfterCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "validated.ts")
	content := []byte("validated-late-open-media")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := artifactInspectionRecording(path, "validated", content)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	opened := &lateOpenArtifactReadFile{info: info}
	fsys := artifactTestFilesystem{
		stat:  os.Stat,
		lstat: os.Lstat,
		open: func(string) (artifactReadFile, error) {
			close(started)
			<-release
			return opened, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultC := make(chan artifactInspectionTestResult, 1)
	go func() {
		inspection, inspectErr := inspectCurrentDVRArtifact(ctx, rec, fsys)
		resultC <- artifactInspectionTestResult{inspection: inspection, err: inspectErr}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("artifact inspection never entered blocked open")
	}
	cancel()
	releaseOnce.Do(func() { close(release) })
	select {
	case got := <-resultC:
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.Is(got.err, context.Canceled) || errors.As(got.err, &indeterminate) ||
			got.inspection.usable || got.inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want context cancellation after open",
				got.inspection, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("artifact inspection did not return after blocked open release")
	}
	if got := opened.closeCalls.Load(); got != 1 {
		t.Fatalf("late-open file closes=%d, want exactly one", got)
	}
	if got := opened.statCalls.Load(); got != 0 {
		t.Fatalf("late-open file stats=%d, want none after cancellation", got)
	}
	if got := opened.readCalls.Load(); got != 0 {
		t.Fatalf("late-open file reads=%d, want none after cancellation", got)
	}
}

func TestInspectCurrentDVRArtifactBlockedReadWaitsForFilesystem(t *testing.T) {
	path := filepath.Join(t.TempDir(), "validated.ts")
	content := []byte("validated-blocked-read-media")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := artifactInspectionRecording(path, "validated", content)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	opened := &closeIgnoringArtifactReadFile{
		info: info, started: make(chan struct{}), release: release,
		closed: make(chan struct{}), returned: make(chan struct{}),
	}
	fsys := artifactTestFilesystem{
		stat: os.Stat, lstat: os.Lstat,
		open: func(string) (artifactReadFile, error) { return opened, nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultC := make(chan artifactInspectionTestResult, 1)
	go func() {
		inspection, inspectErr := inspectCurrentDVRArtifact(ctx, rec, fsys)
		resultC <- artifactInspectionTestResult{inspection: inspection, err: inspectErr}
	}()
	select {
	case <-opened.started:
	case <-time.After(time.Second):
		t.Fatal("artifact inspection never entered blocked read")
	}
	cancel()
	select {
	case got := <-resultC:
		t.Fatalf("blocked read returned before filesystem release: %+v", got)
	case <-time.After(30 * time.Millisecond):
	}
	if got := opened.closeCalls.Load(); got != 0 {
		t.Fatalf("blocked-read file closes=%d before release, want zero", got)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case got := <-resultC:
		var indeterminate *IndeterminateArtifactInspectionError
		if !errors.Is(got.err, context.Canceled) || errors.As(got.err, &indeterminate) ||
			got.inspection.usable || got.inspection.demotionReason != "" {
			t.Fatalf("inspection=%+v err=%v, want context cancellation after read",
				got.inspection, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("artifact inspection did not return after blocked read release")
	}
	if got := opened.closeCalls.Load(); got != 1 {
		t.Fatalf("released blocked-read file closes=%d, want exactly one", got)
	}
}

func TestStartupArtifactInspectionDetachesCloseIgnoringRead(t *testing.T) {
	root := t.TempDir()
	blockingPath := filepath.Join(root, "a-blocking.ts")
	missingPath := filepath.Join(root, "b-missing.ts")
	content := []byte("validated-blocking-media")
	if err := os.WriteFile(blockingPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	blockingInfo, err := os.Stat(blockingPath)
	if err != nil {
		t.Fatal(err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	filesystemID, ok := artifactFilesystemID(blockingInfo)
	if !ok {
		t.Fatal("fixture filesystem identity unavailable")
	}
	rootFilesystemID, ok := artifactFilesystemID(rootInfo)
	if !ok || rootFilesystemID != filesystemID {
		t.Fatalf("fixture filesystem mismatch file=%q root=%q",
			filesystemID, rootFilesystemID)
	}
	digest := sha256.Sum256(content)
	rows := []store.DVRRecording{
		{
			ID: uuid.New(), ArtifactCurrent: true, ArtifactState: "validated",
			OutputPath: blockingPath, ArtifactPath: blockingPath,
			ArtifactBytes: int64(len(content)), ArtifactSHA256: digest[:],
			ArtifactFilesystemID: filesystemID,
		},
		{
			ID: uuid.New(), ArtifactCurrent: true, ArtifactState: "legacy",
			OutputPath: missingPath, ArtifactPath: missingPath,
			ArtifactBytes: 1234, ArtifactFilesystemID: filesystemID,
		},
	}
	testStore := &startupArtifactTestStore{rows: rows}
	started := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan struct{})
	returned := make(chan struct{})
	openCalls := 0
	var opened *closeIgnoringArtifactReadFile
	fsys := artifactTestFilesystem{
		stat:  os.Stat,
		lstat: os.Lstat,
		open: func(string) (artifactReadFile, error) {
			openCalls++
			opened = &closeIgnoringArtifactReadFile{
				info: blockingInfo, started: started, release: release,
				closed: closed, returned: returned,
			}
			return opened, nil
		},
	}

	budget := 40 * time.Millisecond
	detachmentsBefore := metrics.DVRArtifactStartupInspectionDetachments.Value()
	startedAt := time.Now()
	summary, err := reconcileCurrentDVRArtifactsWith(
		context.Background(), testStore, nil, fsys, budget)
	elapsed := time.Since(startedAt)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("startup inspection elapsed=%s, budget=%s", elapsed, budget)
	}
	select {
	case <-started:
	default:
		t.Fatal("blocking filesystem read never started")
	}
	if summary.Demoted != 0 || summary.Indeterminate != len(rows) ||
		!summary.InspectionBudgetExhausted || !summary.InspectionWorkerMayRemain {
		t.Fatalf("startup summary=%+v, want every row retained on budget", summary)
	}
	if openCalls != 1 {
		t.Fatalf("filesystem opens=%d, want only the single blocking worker", openCalls)
	}
	if opened == nil {
		t.Fatal("blocking filesystem file was not opened")
	}
	if got := opened.closeCalls.Load(); got != 0 {
		t.Fatalf("detached worker file closes=%d before release, want zero", got)
	}
	if got := metrics.DVRArtifactStartupInspectionDetachments.Value(); got != detachmentsBefore+1 {
		t.Fatalf("startup inspection detachments=%d, want %d", got, detachmentsBefore+1)
	}
	if binds, demotions := testStore.mutationCounts(); binds != 0 || demotions != 0 {
		t.Fatalf("database mutations before release: binds=%d demotions=%d",
			binds, demotions)
	}

	select {
	case <-closed:
		t.Fatal("detached worker closed its file before blocked read returned")
	default:
	}
	select {
	case <-returned:
		t.Fatal("close-ignoring read unexpectedly returned before test release")
	default:
	}
	close(release)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("timed-out filesystem worker did not exit after test release")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("released filesystem worker did not close its file")
	}
	if got := opened.closeCalls.Load(); got != 1 {
		t.Fatalf("released worker file closes=%d, want exactly one", got)
	}
	time.Sleep(20 * time.Millisecond)
	if binds, demotions := testStore.mutationCounts(); binds != 0 || demotions != 0 {
		t.Fatalf("late database mutations after timeout: binds=%d demotions=%d",
			binds, demotions)
	}
	testStore.mu.Lock()
	defer testStore.mu.Unlock()
	for _, row := range testStore.rows {
		if !row.ArtifactCurrent {
			t.Fatalf("startup detachment changed retained owner: %+v", row)
		}
	}
}

func TestStartupArtifactDatabaseFailuresRemainFatal(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "artifact.ts")
	content := []byte("startup-database-failure-media")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	filesystemID, ok := artifactFilesystemID(info)
	if !ok {
		t.Fatal("fixture filesystem identity unavailable")
	}
	digest := sha256.Sum256(content)
	base := store.DVRRecording{
		ID: uuid.New(), ArtifactCurrent: true, ArtifactState: "validated",
		OutputPath: path, ArtifactPath: path,
		ArtifactBytes: int64(len(content)), ArtifactSHA256: digest[:],
		ArtifactFilesystemID: filesystemID,
	}

	t.Run("list", func(t *testing.T) {
		listErr := errors.New("simulated artifact list database failure")
		testStore := &startupArtifactTestStore{listErr: listErr}
		summary, err := reconcileCurrentDVRArtifactsWith(
			context.Background(), testStore, nil, osArtifactFilesystem{}, time.Second)
		if !errors.Is(err, listErr) || summary != (CurrentDVRArtifactReconcileSummary{}) {
			t.Fatalf("summary=%+v err=%v, want fatal list failure", summary, err)
		}
	})

	t.Run("binding", func(t *testing.T) {
		bindErr := errors.New("simulated artifact binding database failure")
		row := base
		row.ArtifactState = "legacy"
		row.ArtifactSHA256 = nil
		row.ArtifactFilesystemID = ""
		testStore := &startupArtifactTestStore{
			rows: []store.DVRRecording{row}, bindErr: bindErr,
		}
		summary, err := reconcileCurrentDVRArtifactsWith(
			context.Background(), testStore, nil, osArtifactFilesystem{}, time.Second)
		if !errors.Is(err, bindErr) || summary != (CurrentDVRArtifactReconcileSummary{}) {
			t.Fatalf("summary=%+v err=%v, want fatal binding failure", summary, err)
		}
	})

	t.Run("demotion", func(t *testing.T) {
		demoteErr := errors.New("simulated artifact demotion database failure")
		row := base
		row.ArtifactBytes++
		testStore := &startupArtifactTestStore{
			rows: []store.DVRRecording{row}, demoteErr: demoteErr,
		}
		summary, err := reconcileCurrentDVRArtifactsWith(
			context.Background(), testStore, nil, osArtifactFilesystem{}, time.Second)
		if !errors.Is(err, demoteErr) || summary != (CurrentDVRArtifactReconcileSummary{}) {
			t.Fatalf("summary=%+v err=%v, want fatal demotion failure", summary, err)
		}
	})
}
