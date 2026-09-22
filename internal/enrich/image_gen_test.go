package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func imageGenTestPNG(t *testing.T, width, height int) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, width, height))); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestImageGenSubmitPollFetch(t *testing.T) {
	pngBody := imageGenTestPNG(t, 200, 300)
	var polls atomic.Int32
	var sawDefaults atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "secret" {
			t.Fatalf("missing X-API-Key on %s", r.URL.Path)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/generate":
			var req GenerateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatal(err)
			}
			sawDefaults.Store(req.Model == "schnell" && req.Width == 1024 && req.Height == 1536 && req.BatchSize == 1)
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"job_id":"job-1","status":"queued","poll_url":"/v1/jobs/job-1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job-1":
			w.Header().Set("Content-Type", "application/json")
			if polls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"id":"job-1","status":"queued","images":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"id":"job-1","status":"completed","images":[{"url":"/v1/images/image-gen/out.png","width":0,"height":0}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/images/image-gen/out.png":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(pngBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewImageGen(srv.URL, "secret")
	c.PollInterval = time.Millisecond
	b, err := c.Generate(context.Background(), GenerateRequest{Prompt: "a game"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, pngBody) || !sawDefaults.Load() || polls.Load() != 2 {
		t.Fatalf("unexpected generation result: bytes=%d defaults=%v polls=%d", len(b), sawDefaults.Load(), polls.Load())
	}
}

func TestImageGenRejectsForeignImageURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"job_id":"job-1","status":"queued"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"job-1","status":"completed","images":[{"url":"https://evil.example/steal.png"}]}`))
	}))
	defer srv.Close()
	c := NewImageGen(srv.URL, "secret")
	c.PollInterval = time.Millisecond
	_, err := c.Generate(context.Background(), GenerateRequest{Prompt: "a game"})
	if err == nil || !strings.Contains(err.Error(), "outside configured gateway") {
		t.Fatalf("expected foreign URL rejection, got %v", err)
	}
}

func TestImageGenSurfacesModelNotReady(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"detail":"model_not_ready"}`))
	}))
	defer srv.Close()
	c := NewImageGen(srv.URL, "secret")
	_, err := c.Generate(context.Background(), GenerateRequest{Prompt: "a game"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatalf("expected 409 error, got %v", err)
	}
}

func TestImageGenDoesNotForwardAPIKeyAcrossRedirect(t *testing.T) {
	var leaked atomic.Bool
	external := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "" {
			leaked.Store(true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer external.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, external.URL, http.StatusTemporaryRedirect)
	}))
	defer gateway.Close()

	_, err := NewImageGen(gateway.URL, "secret").Submit(context.Background(), GenerateRequest{Prompt: "a game"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("expected redirect rejection, got %v", err)
	}
	if leaked.Load() {
		t.Fatal("X-API-Key leaked across redirect")
	}
}

func TestImageGenCompletedWithoutImageIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"job-1","status":"completed","images":[]}`))
	}))
	defer srv.Close()
	c := NewImageGen(srv.URL, "secret")
	_, err := c.Wait(context.Background(), "job-1")
	if !IsTerminalGenerationError(err) {
		t.Fatalf("expected terminal completed-output error, got %v", err)
	}
}

func TestImageGenCompletedOutputHTTPClassification(t *testing.T) {
	for _, tc := range []struct {
		status   int
		terminal bool
	}{
		{http.StatusTemporaryRedirect, true},
		{http.StatusNotFound, true},
		{http.StatusGone, true},
		{http.StatusRequestTimeout, false},
		{http.StatusTooEarly, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
	} {
		if got := terminalCompletedOutputStatus(tc.status); got != tc.terminal {
			t.Errorf("status %d terminal=%v, want %v", tc.status, got, tc.terminal)
		}
	}
}

func TestImageGenExpiredJobGoneIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()
	c := NewImageGen(srv.URL, "secret")
	_, err := c.Wait(context.Background(), "expired-job")
	if !IsTerminalGenerationError(err) {
		t.Fatalf("expected terminal 410 error, got %v", err)
	}
}

func TestImageGenTruncatedCompletedPNGIsTerminal(t *testing.T) {
	pngBody := imageGenTestPNG(t, 200, 300)
	pngBody = pngBody[:len(pngBody)-8]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/jobs/job-1":
			_, _ = w.Write([]byte(`{"id":"job-1","status":"completed","images":[{"url":"/v1/images/out.png"}]}`))
		case "/v1/images/out.png":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(pngBody)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := NewImageGen(srv.URL, "secret")
	_, err := c.Wait(context.Background(), "job-1")
	if !IsTerminalGenerationError(err) {
		t.Fatalf("expected truncated completed artifact to be terminal, got %v", err)
	}
}
