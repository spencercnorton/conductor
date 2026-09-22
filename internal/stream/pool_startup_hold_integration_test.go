package stream_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

type startupLeadResponseWriter struct {
	*countingResponseWriter
	once      sync.Once
	firstByte chan time.Time
}

func (w *startupLeadResponseWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.once.Do(func() { w.firstByte <- time.Now() })
	}
	return w.countingResponseWriter.Write(p)
}

// Construct both production pump kinds through Pool. A silent first source
// spends its normal four-second attempt; the alternate takes another 1.5s to
// supply media. An unconditional four-second reservoir hold then exceeds the
// real nine-second subscriber startup deadline. The optional hold must leave
// room for publication without shortening either source's attempt budget.
// Non-TS bytes and a cat subprocess make this a deadline/plumbing regression,
// not a decoder or selected-program validation test.
func TestIntegrationStartupLeadFitsPlexDeadline(t *testing.T) {
	db := freshStreamDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var sourceOneRequests, sourceTwoRequests atomic.Int32
	sourceOne := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceOneRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer sourceOne.Close()
	sourceTwo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceTwoRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		flusher.Flush()
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		block := make([]byte, 1024)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			flusher.Flush()
		}
	}))
	defer sourceTwo.Close()
	ch, _ := seedChannel(t, db, 1, sourceOne.URL, sourceTwo.URL)
	profile, err := db.GetTranscodeProfileByName(context.Background(), "stabilize-cpu")
	if err != nil {
		t.Fatal(err)
	}
	ffmpegStub := filepath.Join(t.TempDir(), "ffmpeg-cat")
	if err := os.WriteFile(ffmpegStub, []byte("#!/bin/sh\nexec cat\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	for _, kind := range []string{"passthrough", "transcode"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "transcode" {
				if err := db.SetChannelTranscodeProfile(context.Background(), ch.ID, &profile.ID); err != nil {
					t.Fatal(err)
				}
			}
			oneBefore, twoBefore := sourceOneRequests.Load(), sourceTwoRequests.Load()
			pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
			pool.FFmpegBinary = ffmpegStub
			pool.LiveStartupLead = 4 * time.Second
			requestCtx, cancel := context.WithCancel(context.Background())
			defer func() {
				cancel()
				pool.Close()
				waitCtx, cancelWait := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancelWait()
				if err := pool.WaitForPumps(waitCtx); err != nil {
					t.Errorf("wait for %s pump cleanup: %v", kind, err)
				}
			}()
			started := time.Now()
			deadline := started.Add(stream.ClientStartupBudget)
			ctx := stream.WithStartupDeadline(requestCtx, deadline)
			req := httptest.NewRequest(http.MethodGet, "/stream/901", nil).WithContext(ctx)
			w := &startupLeadResponseWriter{countingResponseWriter: newCountingResponseWriter(), firstByte: make(chan time.Time, 1)}
			served := make(chan error, 1)
			go func() { served <- pool.Serve(ctx, w, req, ch.ID) }()

			select {
			case first := <-w.firstByte:
				elapsed := first.Sub(started)
				t.Logf("first media after %s; startup deadline reserve remaining %s", elapsed, deadline.Sub(first))
				if elapsed < 5*time.Second || !first.Before(deadline.Add(-time.Second)) {
					t.Fatalf("first media after %s, want full first-source attempt and >=1s startup reserve", elapsed)
				}
				if got := w.status.Load(); got != http.StatusOK {
					t.Fatalf("status=%d, want 200", got)
				}
			case err := <-served:
				t.Fatalf("Serve ended before media after %s: %v", time.Since(started), err)
			case <-time.After(stream.ClientStartupBudget + time.Second):
				t.Fatal("Serve neither published media nor returned by the startup deadline")
			}
			if one, two := sourceOneRequests.Load()-oneBefore, sourceTwoRequests.Load()-twoBefore; one != 1 || two != 1 {
				t.Fatalf("source requests=%d/%d, want one attempt on each source", one, two)
			}
			cancel()
			select {
			case err := <-served:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Serve error=%v, want context cancellation", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Serve did not return after cancellation")
			}
		})
	}
}
