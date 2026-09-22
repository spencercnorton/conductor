package enrich_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spencercnorton/conductor/internal/enrich"
)

func newTVDbServer(t *testing.T, search any, loginCalls *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login":
			if loginCalls != nil {
				atomic.AddInt32(loginCalls, 1)
			}
			_, _ = w.Write([]byte(`{"status":"ok","data":{"token":"jwt-token-123"}}`))
		case "/search":
			if r.Header.Get("Authorization") != "Bearer jwt-token-123" {
				w.WriteHeader(401)
				return
			}
			switch v := search.(type) {
			case string:
				_, _ = w.Write([]byte(v))
			default:
				w.WriteHeader(500)
			}
		default:
			w.WriteHeader(404)
		}
	}))
}

func TestTVDbSearchSeriesAuthenticatesAndParses(t *testing.T) {
	resp := `{
		"status": "success",
		"data": [
			{
				"id": 136315,
				"name": "The Bear",
				"overview": "A young chef from the fine dining world.",
				"image": "https://artworks.thetvdb.com/banners/v4/series/136315/posters/1.jpg",
				"first_aired": "2022-06-23",
				"year": "2022",
				"imdb_id": "tt14452776"
			}
		]
	}`
	var loginCalls int32
	srv := newTVDbServer(t, resp, &loginCalls)
	defer srv.Close()

	c := enrich.NewTVDbClient(srv.URL, "test-key", "")
	results, err := c.SearchSeries(context.Background(), "The Bear", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results: got %d want 1", len(results))
	}
	r := results[0]
	if r.ID != 136315 {
		t.Errorf("ID: got %d", r.ID)
	}
	if r.Name != "The Bear" {
		t.Errorf("Name: got %q", r.Name)
	}
	if r.IMDbID != "tt14452776" {
		t.Errorf("IMDbID: got %q", r.IMDbID)
	}
	if !strings.HasPrefix(r.Image, "https://") {
		t.Errorf("Image should be absolute URL: got %q", r.Image)
	}
	if loginCalls != 1 {
		t.Errorf("expected exactly 1 login call; got %d", loginCalls)
	}
}

func TestTVDbReusesTokenAcrossCalls(t *testing.T) {
	resp := `{"status":"success","data":[]}`
	var loginCalls int32
	srv := newTVDbServer(t, resp, &loginCalls)
	defer srv.Close()

	c := enrich.NewTVDbClient(srv.URL, "k", "")
	for i := 0; i < 3; i++ {
		if _, err := c.SearchSeries(context.Background(), "x", 0); err != nil {
			t.Fatal(err)
		}
	}
	if loginCalls != 1 {
		t.Errorf("token should be reused; got %d login calls", loginCalls)
	}
}

func TestTVDbSearchYearFilterFallsBackOnEmpty(t *testing.T) {
	// Year filter shouldn't wipe results when nothing matches — fall
	// back to unfiltered list (matcher will lower confidence for the
	// year mismatch).
	resp := `{"status":"success","data":[{"id":1,"name":"X","first_aired":"2010-01-01","year":"2010"}]}`
	srv := newTVDbServer(t, resp, nil)
	defer srv.Close()

	c := enrich.NewTVDbClient(srv.URL, "k", "")
	results, err := c.SearchSeries(context.Background(), "X", 2026)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("year filter should fall back on empty; got %d results", len(results))
	}
}
