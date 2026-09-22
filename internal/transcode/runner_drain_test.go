package transcode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const runnerDrainHelperMarker = "--conductor-runner-drain-helper"

// Run a real, separately waited producer so tests exercise OS pipe ownership,
// including output left unread after the producer has already exited.
func TestRunnerDrainHelperProcess(t *testing.T) {
	for i, arg := range os.Args {
		if arg != runnerDrainHelperMarker || i+1 >= len(os.Args) {
			continue
		}
		switch os.Args[i+1] {
		case "small":
			_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 4096))
		case "large":
			block := bytes.Repeat([]byte("0123456789abcdef"), 4096)
			for range 64 {
				if _, err := os.Stdout.Write(block); err != nil {
					os.Exit(2)
				}
			}
			_, _ = os.Stderr.Write(bytes.Repeat([]byte("stderr-"), 32768))
			_, _ = os.Stderr.Write([]byte("final stderr marker\n"))
		case "blocked":
			block := bytes.Repeat([]byte("x"), 65536)
			_, _ = os.Stderr.Write([]byte("producer started\n"))
			for {
				if _, err := os.Stdout.Write(block); err != nil {
					os.Exit(2)
				}
			}
		case "stdin-eof":
			_, _ = os.Stdout.Write([]byte("ready"))
			_, _ = io.Copy(io.Discard, os.Stdin)
			_, _ = os.Stderr.Write([]byte("input closed\n"))
		default:
			os.Exit(3)
		}
		os.Exit(0)
	}
}

func startRunnerDrainHelper(t *testing.T, ctx context.Context, mode string) *Runner {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wrapper := filepath.Join(t.TempDir(), "producer")
	quoted := "'" + strings.ReplaceAll(binary, "'", "'\\''") + "'"
	script := "#!/bin/sh\nexport GORACE=atexit_sleep_ms=0\nexec " + quoted + " -test.run='^TestRunnerDrainHelperProcess$' -- " + runnerDrainHelperMarker + " " + mode + "\n"
	if err := os.WriteFile(wrapper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	r := New(wrapper, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := r.Start(ctx, &Profile{Name: "drain-test", Kind: Kind("test")}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func waitRunnerExited(t *testing.T, r *Runner) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !r.Exited() {
		select {
		case <-deadline.C:
			t.Fatal("producer did not exit")
		case <-tick.C:
		}
	}
}

func TestRunnerCleanExitPreservesUnreadOutput(t *testing.T) {
	r := startRunnerDrainHelper(t, context.Background(), "small")
	waitRunnerExited(t, r)
	want := bytes.Repeat([]byte("x"), 4096)
	got, err := io.ReadAll(r.Stdout())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("clean exited producer unread output = %d/%d bytes, error %v", len(got), len(want), err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Stdout().Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("explicit Close left reader open: %v", err)
	}
}

func TestRunnerDrainsLargeOutputAndCompleteStderr(t *testing.T) {
	r := startRunnerDrainHelper(t, context.Background(), "large")
	want := bytes.Repeat([]byte("0123456789abcdef"), 4096*64)
	// Small reads force the process to block and resume many times. No reader
	// delay or scheduling assumption hides its final unread pipe tail.
	var got bytes.Buffer
	buf := make([]byte, 997)
	for {
		n, err := r.Stdout().Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stdout read after %d bytes: %v", got.Len(), err)
		}
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("stdout retained %d/%d bytes", got.Len(), len(want))
	}
	waitRunnerExited(t, r)
	stderr := append(bytes.Repeat([]byte("stderr-"), 32768), []byte("final stderr marker\n")...)
	if tail := r.StderrTail(r.stderrCap); tail != string(stderr[len(stderr)-r.stderrCap:]) {
		t.Fatalf("stderr tail was not completely drained: got %d bytes", len(tail))
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerContextCancellationClosesUnreadPipe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := startRunnerDrainHelper(t, ctx, "blocked")
	cancel()
	waitRunnerExited(t, r)
	select {
	case <-r.cancelIODone:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not release caller pipes")
	}
	if _, err := r.Stdout().Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("canceled reader was not released: %v", err)
	}
	if err := r.Close(); err == nil {
		t.Fatal("canceled producer reported success")
	}
}

func TestRunnerCloseTerminatesBlockedProducer(t *testing.T) {
	r := startRunnerDrainHelper(t, context.Background(), "blocked")
	started := time.Now()
	if err := r.Close(); err == nil {
		t.Fatal("blocked producer reported clean shutdown")
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("Close exceeded bounded producer shutdown: %s", elapsed)
	}
	if !r.Exited() {
		t.Fatal("Close returned before producer exit")
	}
	if _, err := r.Stdout().Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Close left stdout open: %v", err)
	}
}

func TestRunnerConcurrentCloseAndStatus(t *testing.T) {
	r := startRunnerDrainHelper(t, context.Background(), "stdin-eof")
	if _, err := io.ReadFull(r.Stdout(), make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 20 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_ = r.Exited()
			_ = r.StderrTail(100)
			if err := r.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	workers.Wait()
	if !r.Exited() || r.StderrTail(100) != "input closed\n" {
		t.Fatalf("Close did not join exit and stderr capture: exited=%v stderr=%q", r.Exited(), r.StderrTail(100))
	}
}

func TestRunnerStartFailureReleasesPipes(t *testing.T) {
	fdDirectory := "/dev/fd"
	if _, err := os.Stat("/proc/self/fd"); err == nil {
		fdDirectory = "/proc/self/fd"
	}
	countFDs := func() int {
		entries, err := os.ReadDir(fdDirectory)
		if err != nil {
			t.Skipf("cannot inspect descriptor ownership: %v", err)
		}
		return len(entries)
	}
	before := countFDs()
	for range 20 {
		r := New(filepath.Join(t.TempDir(), "nonexistent"), slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err := r.Start(context.Background(), &Profile{Name: "failed-start", Kind: Kind("test")}); err == nil {
			t.Fatal("missing executable unexpectedly started")
		}
		if r.Stdin() != nil || r.Stdout() != nil {
			t.Fatal("failed Start exposed live pipes")
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if after := countFDs(); after > before+1 {
		t.Fatalf("failed starts leaked descriptors: before=%d after=%d", before, after)
	}
}
