package stream

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestPlaceholderCooldownWindow(t *testing.T) {
	var nilCooldown *placeholderCooldown
	nilCooldown.mark("http://a", time.Now()) // must not panic
	if nilCooldown.active("http://a", time.Now()) {
		t.Fatal("nil cooldown must remember nothing")
	}

	c := newPlaceholderCooldown()
	t0 := time.Unix(1_000_000, 0)
	c.mark("http://a", t0)
	if !c.active("http://a", t0.Add(placeholderCooldownTTL-time.Nanosecond)) {
		t.Fatal("verdict forgotten inside the window")
	}
	if c.active("http://b", t0) {
		t.Fatal("verdict leaked to another URL")
	}
	if c.active("http://a", t0.Add(placeholderCooldownTTL)) {
		t.Fatal("verdict outlived the window")
	}
	c.mark("http://b", t0.Add(placeholderCooldownTTL))
	if _, ok := c.until["http://a"]; ok {
		t.Fatal("expired entry not pruned")
	}
}

// One off-air channel, retried by its pump and then re-tuned by a second
// pump, must cost the provider one request, not one per attempt.
func TestPlaceholderCooldownSparesProviderAcrossRetriesAndRetunes(t *testing.T) {
	var hits atomic.Int32
	data := bytes.Repeat([]byte{0xaa}, int(minFinitePreflightBytes))
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	}))
	defer provider.Close()

	black := classificationResult{kind: classificationBlackPlaceholder, reason: "test_high_confidence"}
	shared := newPlaceholderCooldown()

	// runUntilFailures drives one pump that keeps retrying the same URL, the
	// way a channel whose every source is black holds on the slate.
	runUntilFailures := func(id string, cooldown *placeholderCooldown, want int32) {
		t.Helper()
		var failures atomic.Int32
		enough := make(chan struct{})
		s := NewStreamer(id, provider.URL, testLogger(), nil)
		s.classifier = &staticFiniteClassifier{result: black}
		s.placeholders = cooldown
		s.sourceStartupTimeout = 2 * time.Second
		s.OnUpstreamDown = func(_ context.Context, cause error) (string, bool, error) {
			if !errors.Is(cause, ErrPlaceholderMedia) {
				t.Errorf("cause = %v, want ErrPlaceholderMedia", cause)
			}
			if failures.Add(1) == want {
				close(enough)
			}
			return "", true, nil
		}
		_, unsubscribe := s.Subscribe("viewer")
		defer unsubscribe()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { s.Run(ctx); close(done) }()
		select {
		case <-enough:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: only %d placeholder failures", id, failures.Load())
		}
		cancel()
		<-done
	}

	runUntilFailures("first-tune", shared, 3)
	runUntilFailures("retune", shared, 2)
	if got := hits.Load(); got != 1 {
		t.Fatalf("provider requests = %d across 5 black attempts, want 1", got)
	}

	// Control: without the shared verdict every attempt reaches the provider.
	hits.Store(0)
	runUntilFailures("no-cooldown", nil, 3)
	if got := hits.Load(); got < 3 {
		t.Fatalf("control made %d provider requests, want >= 3", got)
	}
}
