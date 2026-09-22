package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

type cyclicResponseWriter struct {
	http.ResponseWriter
}

func (w *cyclicResponseWriter) Unwrap() http.ResponseWriter { return w }

func TestWriteStreamStartupErrorMapsAdmissionContentionTo503(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "direct", err: store.ErrAdmissionContention},
		{
			name: "deadline wrapped",
			err: fmt.Errorf("%w waiting for channel gate: %w",
				store.ErrAdmissionContention, context.DeadlineExceeded),
		},
		{
			name: "operational startup failure",
			err:  fmt.Errorf("%w: resolver: decrypt failed", store.ErrLeaseOperational),
		},
		{name: "pool closed during startup", err: stream.ErrPoolClosed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			if !writeStreamStartupError(response, tt.err) {
				t.Fatal("admission contention was not recognized as a startup error")
			}
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503", response.Code)
			}
			if !strings.Contains(response.Body.String(), "retry") {
				t.Fatalf("body=%q, want retry guidance", response.Body.String())
			}
		})
	}
}

func TestMidstreamRelocationUnknownDoesNotAppendHTTPErrorToMedia(t *testing.T) {
	response := httptest.NewRecorder()
	media := []byte{0x47, 0x40, 0x00, 0x10, 0x00, 0x00, 0x01}
	response.Header().Set("Content-Type", "video/mp2t")
	response.WriteHeader(http.StatusOK)
	if _, err := response.Write(media); err != nil {
		t.Fatal(err)
	}
	if writeStreamStartupError(response, store.ErrRelocationStateUnknown) {
		t.Fatal("midstream relocation marker was treated as a pre-byte HTTP error")
	}
	if got := response.Body.Bytes(); !bytes.Equal(got, media) {
		t.Fatalf("midstream error appended plaintext to MPEG-TS: %x", got)
	}
}

func TestPoolClosedAfterCommittedMediaDoesNotAppendHTTPError(t *testing.T) {
	response := httptest.NewRecorder()
	tracked := &statusWriter{ResponseWriter: response, code: http.StatusOK}
	media := []byte{0x47, 0x40, 0x00, 0x10, 0x00, 0x00, 0x01}
	tracked.Header().Set("Content-Type", "video/mp2t")
	tracked.WriteHeader(http.StatusOK)
	if _, err := tracked.Write(media); err != nil {
		t.Fatal(err)
	}

	if writeStreamStartupErrorIfUncommitted(tracked, stream.ErrPoolClosed) {
		t.Fatal("pool-close error was written after the media response committed")
	}
	if got := response.Body.Bytes(); !bytes.Equal(got, media) {
		t.Fatalf("pool-close error appended plaintext to MPEG-TS: %x", got)
	}
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d, want committed 200", response.Code)
	}
}

func TestPoolClosedAfterCommittedMediaTraversesGzipMiddleware(t *testing.T) {
	response := httptest.NewRecorder()
	tracked := &statusWriter{ResponseWriter: response, code: http.StatusOK}
	media := []byte{0x47, 0x40, 0x00, 0x10, 0x00, 0x00, 0x01}
	wroteStartupError := false
	handler := gzipped(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		if _, err := w.Write(media); err != nil {
			t.Fatal(err)
		}
		wroteStartupError = writeStreamStartupErrorIfUncommitted(w, stream.ErrPoolClosed)
	})
	request := httptest.NewRequest(http.MethodGet, "/auto/v22", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	handler(tracked, request)

	if wroteStartupError {
		t.Fatal("pool-close error was written through a wrapper after media committed")
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, media) {
		t.Fatalf("wrapped midstream error appended plaintext to MPEG-TS: %x", decoded)
	}
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d, want committed 200", response.Code)
	}
}

func TestResponseCommitmentCycleFailsClosed(t *testing.T) {
	wrapped := &cyclicResponseWriter{ResponseWriter: httptest.NewRecorder()}
	if !responseCommitted(wrapped) {
		t.Fatal("cyclic response wrapper was treated as safely uncommitted")
	}
	if writeStreamStartupErrorIfUncommitted(wrapped, stream.ErrPoolClosed) {
		t.Fatal("startup plaintext was written through a cyclic response wrapper")
	}
}

func TestPoolClosedBeforeCommitWritesRetryableError(t *testing.T) {
	response := httptest.NewRecorder()
	tracked := &statusWriter{ResponseWriter: response, code: http.StatusOK}
	if !writeStreamStartupErrorIfUncommitted(tracked, stream.ErrPoolClosed) {
		t.Fatal("pre-byte pool-close error was not mapped")
	}
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", response.Code)
	}
	if !strings.Contains(response.Body.String(), "retry") {
		t.Fatalf("body=%q, want retry guidance", response.Body.String())
	}
}

func TestStatusWriterPreservesFlushAndCommitTracking(t *testing.T) {
	response := httptest.NewRecorder()
	tracked := &statusWriter{ResponseWriter: response, code: http.StatusOK}
	if tracked.ResponseCommitted() {
		t.Fatal("new response was already committed")
	}
	tracked.Flush()
	if !tracked.ResponseCommitted() || !response.Flushed {
		t.Fatalf("after flush committed=%v flushed=%v, want true/true",
			tracked.ResponseCommitted(), response.Flushed)
	}
}

func TestLogStreamStartupErrorPreservesPoolClosedCause(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	logStreamStartupError(logger, "849", "127.0.0.1:1234", 25*time.Millisecond,
		fmt.Errorf("post-acquire startup: %w", stream.ErrPoolClosed))
	got := logs.String()
	for _, want := range []string{
		"stream startup unavailable during shutdown",
		stream.ErrPoolClosed.Error(),
		`"channel":"849"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("pool-closed startup log %q missing %q", got, want)
		}
	}
}

func TestLogStreamStartupErrorPreservesOperationalCause(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	err := fmt.Errorf("%w: resolver: credential decrypt failed", store.ErrLeaseOperational)
	logStreamStartupError(logger, "849", "127.0.0.1:1234", 50*time.Millisecond, err)
	got := logs.String()
	for _, want := range []string{
		"stream startup admission failed",
		"credential decrypt failed",
		`"channel":"849"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("startup diagnostic log %q missing %q", got, want)
		}
	}
}
