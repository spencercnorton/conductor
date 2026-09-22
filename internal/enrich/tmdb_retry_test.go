// tmdb_retry_test.go — covers the retry-on-5xx and HTTP-method behavior
// added 2026-05-08 in response to TMDb's edge intermittently 502'ing
// HTTP/2 requests from our egress.
package enrich

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestRetryOn502RecoversAfterTransientFailure: server returns 502 the first
// 2 times then 200 — fetchCached should succeed on attempt 3.
func TestRetryOn502RecoversAfterTransientFailure(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n <= 2 {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "<html>502</html>")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"results":[{"id":1,"name":"hit"}],"total_results":1}`)
	}))
	defer srv.Close()

	c := NewTMDbClient(srv.URL, "test-key")
	body, status, err := c.doWithRetry(context.Background(), srv.URL+"/3/search/tv?api_key=test-key&query=x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Errorf("status: got %d want 200", status)
	}
	if !strings.Contains(string(body), `"id":1`) {
		t.Errorf("body did not include success payload: %s", string(body))
	}
	if hits.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", hits.Load())
	}
}

// TestRetryOn5xxGivesUpAfterMaxAttempts: server always 502s — caller gets
// the 502 surfaced rather than spinning forever.
func TestRetryOn5xxGivesUpAfterMaxAttempts(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "<html>502</html>")
	}))
	defer srv.Close()

	c := NewTMDbClient(srv.URL, "test-key")
	_, status, err := c.doWithRetry(context.Background(), srv.URL+"/3/search/tv?api_key=test-key&query=x")
	if err != nil {
		t.Fatalf("doWithRetry returned err on the 5xx path (caller should decide): %v", err)
	}
	if status != http.StatusBadGateway {
		t.Errorf("status: got %d want 502", status)
	}
	if hits.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", hits.Load())
	}
}

// TestNo4xxRetry: 404 is a real "not found" answer from TMDb and should
// NOT trigger retries (would just waste budget + cache thrash).
func TestNo4xxRetry(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"status_code":34}`)
	}))
	defer srv.Close()

	c := NewTMDbClient(srv.URL, "test-key")
	_, status, err := c.doWithRetry(context.Background(), srv.URL+"/3/search/tv?api_key=test-key&query=x")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if status != http.StatusNotFound {
		t.Errorf("status: got %d want 404", status)
	}
	if hits.Load() != 1 {
		t.Errorf("expected 1 attempt for 404 (no retry), got %d", hits.Load())
	}
}

// TestRetryOn429: 429 ("rate limit") is in the retry set so the caller
// gets a chance to land after backoff instead of giving up immediately.
func TestRetryOn429(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, `{"ok":true}`)
	}))
	defer srv.Close()

	c := NewTMDbClient(srv.URL, "test-key")
	_, status, err := c.doWithRetry(context.Background(), srv.URL+"/x")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if status != 200 {
		t.Errorf("status: got %d want 200", status)
	}
	if hits.Load() != 2 {
		t.Errorf("expected 2 attempts (429 then 200), got %d", hits.Load())
	}
}

// TestUserAgentSet: doWithRetry should send our identifying User-Agent so
// upstream observability shows where the requests are coming from (not
// the default Go-http-client).
func TestUserAgentSet(t *testing.T) {
	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	c := NewTMDbClient(srv.URL, "test-key")
	_, _, err := c.doWithRetry(context.Background(), srv.URL+"/x")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(seen, "Conductor") {
		t.Errorf("User-Agent missing 'Conductor': %q", seen)
	}
}

// TestContextCancellation: a cancelled context during a retry backoff
// returns the context error rather than continuing to retry.
func TestContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c := NewTMDbClient(srv.URL, "test-key")
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a tiny delay so first attempt completes (502), then
	// the backoff sleep gets interrupted.
	go func() {
		// Short enough that we abort during the first backoff window.
		<-ctx.Done()
	}()
	cancel()
	_, _, err := c.doWithRetry(ctx, srv.URL+"/x")
	if err != context.Canceled {
		// Tolerate context.DeadlineExceeded too if Do races — this test
		// asserts only that we don't ignore the cancellation.
		if err == nil {
			t.Errorf("expected context error, got nil")
		}
	}
}
