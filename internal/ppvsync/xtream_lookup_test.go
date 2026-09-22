package ppvsync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/xtream"
)

func TestStreamIDFromURL(t *testing.T) {
	cases := []struct {
		url  string
		want int64
		ok   bool
	}{
		{"http://iboostv.us/live/acc01/secret/606180.ts", 606180, true},
		{"http://iboostv.us/live/acc01/secret/606180.m3u8", 606180, true},
		{"http://iboostv.us/live/acc01/secret/606180", 606180, true},
		{"http://example.com/playlist.m3u8", 0, false},
		{"http://example.com/stream/abc.ts", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := StreamIDFromURL(c.url)
		if got != c.want || ok != c.ok {
			t.Errorf("StreamIDFromURL(%q) = (%d,%v), want (%d,%v)", c.url, got, ok, c.want, c.ok)
		}
	}
}

// catalogueServer returns an httptest server serving a fixed catalogue
// and counting fetches.
func catalogueServer(t *testing.T, fetches *atomic.Int64, fail *atomic.Bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		if fail.Load() {
			http.Error(w, "panel down", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`[
			{"stream_id": 606180, "name": "LIVE | NHL PENGUINS - BLUES | Tue 14 Apr 20:55 CEST | PPV 1"},
			{"stream_id": 700, "name": "plain channel"}
		]`))
	}))
}

func lookupForServer(srv *httptest.Server) *XtreamLookup {
	c := xtream.New(srv.URL, "u", "p", srv.Client())
	return NewXtreamLookup([]*xtream.Client{c}, nil)
}

func srcFor(url string) store.ChannelSource {
	return store.ChannelSource{UpstreamURL: url, Enabled: true}
}

func TestXtreamLookup_CachesCataloguePerTTL(t *testing.T) {
	var fetches atomic.Int64
	var fail atomic.Bool
	srv := catalogueServer(t, &fetches, &fail)
	defer srv.Close()

	l := lookupForServer(srv)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		row, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/606180.ts"))
		if err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
		if row.ID != 606180 || row.Name == "" {
			t.Fatalf("lookup %d: bad row %+v", i, row)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Errorf("want 1 catalogue fetch across 5 lookups, got %d", got)
	}

	// Unknown stream id → ErrNotFound, still no extra fetch.
	if _, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/999999.ts")); err != ErrNotFound {
		t.Errorf("want ErrNotFound for unknown id, got %v", err)
	}
	// Non-xtream URL → ErrNotFound without touching the catalogue.
	if _, err := l.LookupBySource(ctx, srcFor("http://x/playlist.m3u8")); err != ErrNotFound {
		t.Errorf("want ErrNotFound for non-xtream URL, got %v", err)
	}
	if got := fetches.Load(); got != 1 {
		t.Errorf("want still 1 fetch, got %d", got)
	}
}

func TestXtreamLookup_PinsOneGenerationAcrossPassTTLBoundary(t *testing.T) {
	var fetches atomic.Int64
	var fail atomic.Bool
	srv := catalogueServer(t, &fetches, &fail)
	defer srv.Close()

	l := lookupForServer(srv)
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return now }
	ctx := context.Background()
	if err := l.BeginPass(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/606180.ts"))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(l.TTL + time.Minute)
	second, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/700.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !second.UpdatedAt.Equal(first.UpdatedAt) || fetches.Load() != 1 {
		t.Fatalf("pass crossed generations: first=%v second=%v fetches=%d", first.UpdatedAt, second.UpdatedAt, fetches.Load())
	}
	l.EndPass()

	third, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/700.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !third.UpdatedAt.After(second.UpdatedAt) || fetches.Load() != 2 {
		t.Fatalf("post-pass refresh missing: second=%v third=%v fetches=%d", second.UpdatedAt, third.UpdatedAt, fetches.Load())
	}
}

func TestXtreamLookup_StaleServeThenError(t *testing.T) {
	var fetches atomic.Int64
	var fail atomic.Bool
	srv := catalogueServer(t, &fetches, &fail)
	defer srv.Close()

	l := lookupForServer(srv)
	now := time.Now()
	l.now = func() time.Time { return now }
	ctx := context.Background()

	if _, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/700.ts")); err != nil {
		t.Fatalf("warm fetch: %v", err)
	}

	// Panel goes down; within MaxStale the stale catalogue still answers.
	fail.Store(true)
	now = now.Add(l.TTL + time.Minute)
	if row, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/700.ts")); err != nil || row.ID != 700 {
		t.Fatalf("stale-serve window: got (%+v, %v)", row, err)
	}

	// Past MaxStale the outage must surface as an error.
	now = now.Add(l.MaxStale)
	if _, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/700.ts")); err == nil {
		t.Fatal("want error once catalogue is older than MaxStale and refresh fails")
	}

	// Panel recovers → lookups recover.
	fail.Store(false)
	now = now.Add(l.FailureBackoff)
	if row, err := l.LookupBySource(ctx, srcFor("http://x/live/u/p/700.ts")); err != nil || row.ID != 700 {
		t.Fatalf("post-recovery: got (%+v, %v)", row, err)
	}
}

func TestXtreamLookup_FailedRefreshBacksOff(t *testing.T) {
	var fetches atomic.Int64
	var fail atomic.Bool
	fail.Store(true)
	srv := catalogueServer(t, &fetches, &fail)
	defer srv.Close()

	l := lookupForServer(srv)
	now := time.Now()
	l.now = func() time.Time { return now }
	ctx := context.Background()
	src := srcFor("http://x/live/u/p/700.ts")

	for i := 0; i < 5; i++ {
		if _, err := l.LookupBySource(ctx, src); err == nil {
			t.Fatalf("lookup %d unexpectedly succeeded", i)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("failed catalogue fetched %d times inside backoff, want 1", got)
	}

	fail.Store(false)
	now = now.Add(l.FailureBackoff)
	if row, err := l.LookupBySource(ctx, src); err != nil || row.ID != 700 {
		t.Fatalf("post-backoff recovery: got (%+v, %v)", row, err)
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches after recovery = %d, want 2", got)
	}
}

func TestXtreamLookup_FailoverToSecondClient(t *testing.T) {
	var fetchesA, fetchesB atomic.Int64
	var failA, failB atomic.Bool
	failA.Store(true)
	srvA := catalogueServer(t, &fetchesA, &failA)
	defer srvA.Close()
	srvB := catalogueServer(t, &fetchesB, &failB)
	defer srvB.Close()

	l := NewXtreamLookup([]*xtream.Client{
		xtream.New(srvA.URL, "u", "p", srvA.Client()),
		xtream.New(srvB.URL, "u", "p", srvB.Client()),
	}, nil)

	row, err := l.LookupBySource(context.Background(), srcFor("http://x/live/u/p/606180.ts"))
	if err != nil || row.ID != 606180 {
		t.Fatalf("failover lookup: got (%+v, %v)", row, err)
	}
	if fetchesA.Load() == 0 || fetchesB.Load() == 0 {
		t.Errorf("expected both clients tried (A=%d, B=%d)", fetchesA.Load(), fetchesB.Load())
	}
}
