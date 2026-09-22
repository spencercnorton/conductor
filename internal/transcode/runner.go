package transcode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// Runner manages one FFmpeg subprocess for the lifetime of one upstream
// stream. The flow is:
//
//	upstream HTTP body  ─►  Runner.Stdin  ─►  ffmpeg  ─►  Runner.Stdout  ─►  Streamer fan-out
//
// Caller is responsible for:
//  1. Calling Start(ctx, profile) once.
//  2. Passing stdin bytes (Streamer's upstream-pump goroutine).
//  3. Reading stdout bytes (Streamer's fan-out goroutine).
//  4. Calling Close() when the upstream tears down (or letting ctx cancel
//     the runner naturally).
//
// FFmpeg's stderr is captured into a ring buffer for the diagnostics endpoint.
type Runner struct {
	binary string // "ffmpeg" by default, or full path
	logger *slog.Logger

	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser

	stderrMu  sync.Mutex
	stderrBuf []byte // ring buffer, 64KB
	stderrCap int

	exited       atomic.Bool
	exitErr      error
	done         chan struct{}
	stopCancelIO func() bool
	cancelIODone chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

const runnerShutdownGrace = 3 * time.Second

// New constructs a Runner. binary may be empty to default to "ffmpeg" on PATH.
func New(binary string, logger *slog.Logger) *Runner {
	if binary == "" {
		binary = "ffmpeg"
	}
	return &Runner{
		binary:    binary,
		logger:    logger,
		stderrCap: 64 * 1024,
	}
}

// Start spawns ffmpeg with the profile's argv. Returns once the process is
// running and the pipes are wired. Caller may immediately Stdin().Write()
// and Stdout().Read().
func (r *Runner) Start(ctx context.Context, p *Profile) error {
	return r.StartWithAudioOriginCorrection(ctx, p, 0)
}

// StartWithAudioOriginCorrection starts FFmpeg with an attempt-local audio
// origin correction. The caller must derive correction from private input
// evidence; Runner deliberately does not inspect or mutate the shared Profile.
func (r *Runner) StartWithAudioOriginCorrection(
	ctx context.Context,
	p *Profile,
	correction time.Duration,
) error {
	if p == nil || p.IsPassthrough() {
		return errors.New("Runner.Start called with passthrough profile")
	}

	args := p.BuildArgsWithAudioOriginCorrection(correction)
	r.logger.Info("ffmpeg start",
		"profile", p.Name, "kind", p.Kind, "args_len", len(args))

	r.cmd = exec.CommandContext(ctx, r.binary, args...)
	// Bound os/exec's stderr copy if an unexpected descendant retains its
	// inherited descriptor after FFmpeg exits or the context is canceled.
	r.cmd.WaitDelay = runnerShutdownGrace

	// StdoutPipe is owned by Cmd.Wait, which closes its reader as soon as the
	// child exits. Our paced caller can still have unread media at that point.
	// Assign an OS pipe directly so only the caller's shutdown closes its reader.
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	started := false
	defer func() {
		_ = stdoutWriter.Close()
		if !started {
			_ = stdout.Close()
		}
	}()
	r.cmd.Stdout = stdoutWriter

	stdin, err := r.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}
	// A Writer makes Wait join os/exec's stderr capture before recording exit.
	// StderrPipe would have the same premature-reader-close race as stdout.
	r.cmd.Stderr = runnerStderrWriter{r}

	if err := r.cmd.Start(); err != nil {
		_ = stdin.Close()
		return fmt.Errorf("ffmpeg start: %w", err)
	}
	started = true

	r.stdin = stdin
	r.stdout = stdout
	r.done = make(chan struct{})
	r.cancelIODone = make(chan struct{})
	r.stopCancelIO = context.AfterFunc(ctx, func() {
		defer close(r.cancelIODone)
		// Cancellation abandons this attempt, unlike a natural child exit.
		// Release both caller ends even when no consumer calls Close afterwards.
		_ = stdin.Close()
		_ = stdout.Close()
	})

	// Watcher goroutine — records exit status without blocking callers.
	go func() {
		defer close(r.done)
		err := r.cmd.Wait()
		r.exitErr = err
		r.exited.Store(true)
		_ = r.stdin.Close()
		if err != nil {
			r.logger.Warn("ffmpeg exited with error",
				"profile", p.Name, "err", err,
				"stderr_tail", r.StderrTail(2048))
		} else {
			r.logger.Info("ffmpeg exited cleanly", "profile", p.Name)
		}
	}()

	return nil
}

// Stdin returns the writer ffmpeg reads from. It is nil after Start fails
// and closed after FFmpeg exits.
func (r *Runner) Stdin() io.Writer { return r.stdin }

// Stdout returns the reader ffmpeg writes to. nil after Start fails.
func (r *Runner) Stdout() io.ReadCloser { return r.stdout }

// Close terminates the subprocess if still running. Sequence:
//  1. Close stdin → ffmpeg EOFs the input, flushes, and exits cleanly.
//  2. Wait up to 3s for it to exit on its own.
//  3. If still alive: SIGKILL.
//  4. Release stdout, including any unread output the caller has abandoned.
//
// Concurrent Close calls share one shutdown. A descendant holding stderr can
// extend the wait by at most another 3s (Cmd.WaitDelay).
func (r *Runner) Close() error {
	if r.cmd == nil || r.cmd.Process == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		defer func() {
			if !r.stopCancelIO() {
				// An already-started callback owns these same pipe ends.
				// Join it so Close returns with all runner I/O work finished.
				<-r.cancelIODone
			}
		}()
		_ = r.stdin.Close()
		timer := time.NewTimer(runnerShutdownGrace)
		defer timer.Stop()
		select {
		case <-r.done:
		case <-timer.C:
			_ = r.cmd.Process.Kill()
			<-r.done
		}
		_ = r.stdout.Close()
		r.closeErr = r.exitErr
	})
	return r.closeErr
}

type runnerStderrWriter struct{ runner *Runner }

func (w runnerStderrWriter) Write(p []byte) (int, error) {
	w.runner.appendStderr(p)
	return len(p), nil
}

// Exited reports whether the subprocess has exited.
func (r *Runner) Exited() bool { return r.exited.Load() }

// StderrTail returns the most recent N bytes of ffmpeg stderr. Useful for
// surfacing in the diagnostics dashboard or in log lines on exit.
func (r *Runner) StderrTail(n int) string {
	r.stderrMu.Lock()
	defer r.stderrMu.Unlock()
	if len(r.stderrBuf) <= n {
		return string(r.stderrBuf)
	}
	return string(r.stderrBuf[len(r.stderrBuf)-n:])
}

func (r *Runner) appendStderr(p []byte) {
	r.stderrMu.Lock()
	defer r.stderrMu.Unlock()
	r.stderrBuf = append(r.stderrBuf, p...)
	if len(r.stderrBuf) > r.stderrCap {
		// Trim from the front, keep the tail (last log lines are usually
		// the interesting ones on failure).
		r.stderrBuf = r.stderrBuf[len(r.stderrBuf)-r.stderrCap:]
	}
}

// _ keeps context referenced even if Start's signature changes; the cmd
// is created with CommandContext so ctx cancellation propagates SIGKILL.
var _ context.Context = context.Background()
