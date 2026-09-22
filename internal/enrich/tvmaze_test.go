package enrich_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spencercnorton/conductor/internal/enrich"
)

// newTVmazeServer returns an httptest server that responds to the two
// TVmaze endpoints we exercise. `searchBody` is returned verbatim from
// /search/shows; `showBody` from /shows/{id}. Either may be empty.
func newTVmazeServer(t *testing.T, searchBody, showBody string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/search/shows"):
			_, _ = w.Write([]byte(searchBody))
		case strings.HasPrefix(r.URL.Path, "/shows/"):
			_, _ = w.Write([]byte(showBody))
		default:
			w.WriteHeader(404)
		}
	}))
}

func TestTVmazeSearchShowsParses(t *testing.T) {
	resp := `[
		{
			"score": 17.2,
			"show": {
				"id": 145,
				"name": "The Bear",
				"premiered": "2022-06-23",
				"summary": "<p>A young chef from the fine dining world comes home to Chicago.</p>",
				"language": "English",
				"image": {
					"medium": "https://static.tvmaze.com/uploads/images/medium_portrait/x.jpg",
					"original": "https://static.tvmaze.com/uploads/images/original_untouched/x.jpg"
				},
				"externals": {
					"tvrage": 0,
					"thetvdb": 423104,
					"imdb": "tt14452776"
				},
				"network": {"name": "FX"}
			}
		}
	]`
	srv := newTVmazeServer(t, resp, "", 0)
	defer srv.Close()

	c := enrich.NewTVmazeClient(srv.URL)
	results, err := c.SearchShows(context.Background(), "The Bear", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results: got %d want 1", len(results))
	}
	r := results[0]
	if r.ID != 145 {
		t.Errorf("ID: got %d want 145", r.ID)
	}
	if r.Name != "The Bear" {
		t.Errorf("Name: got %q", r.Name)
	}
	if r.Externals.TheTVDB != 423104 {
		t.Errorf("TheTVDB external: got %d", r.Externals.TheTVDB)
	}
	if r.Externals.IMDb != "tt14452776" {
		t.Errorf("IMDb external: got %q", r.Externals.IMDb)
	}
	if !strings.HasPrefix(r.PosterURL(), "https://") {
		t.Errorf("PosterURL: got %q", r.PosterURL())
	}
	// PosterURL prefers original over medium when both present.
	if !strings.Contains(r.PosterURL(), "original_untouched") {
		t.Errorf("PosterURL should prefer original, got %q", r.PosterURL())
	}
}

func TestTVmazeSearchYearFilterFallsBackOnEmpty(t *testing.T) {
	// One match premiered 2010 — caller asks for year 2026.
	// Filter should wipe everything but then return the unfiltered
	// list (matcher will lower confidence for the year mismatch).
	resp := `[{"score": 5.0, "show": {"id": 1, "name": "X", "premiered": "2010-01-01"}}]`
	srv := newTVmazeServer(t, resp, "", 0)
	defer srv.Close()

	c := enrich.NewTVmazeClient(srv.URL)
	results, err := c.SearchShows(context.Background(), "X", 2026)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("year filter should fall back on empty; got %d results", len(results))
	}
}

func TestTVmazeSearchEmptyQueryRejected(t *testing.T) {
	c := enrich.NewTVmazeClient("https://api.tvmaze.com")
	_, err := c.SearchShows(context.Background(), "   ", 0)
	if err == nil {
		t.Fatal("expected error on whitespace-only query")
	}
}

func TestTVmazeGetShowParses(t *testing.T) {
	resp := `{
		"id": 145,
		"name": "The Bear",
		"premiered": "2022-06-23",
		"summary": "<p>Plain summary.</p>",
		"externals": {"imdb": "tt14452776", "thetvdb": 423104, "tvrage": 0}
	}`
	srv := newTVmazeServer(t, "", resp, 0)
	defer srv.Close()

	c := enrich.NewTVmazeClient(srv.URL)
	show, err := c.GetShow(context.Background(), 145)
	if err != nil {
		t.Fatal(err)
	}
	if show.ID != 145 {
		t.Errorf("ID: got %d", show.ID)
	}
	if show.Externals.TheTVDB != 423104 {
		t.Errorf("TheTVDB external: got %d", show.Externals.TheTVDB)
	}
}

func TestTVmazeNotFoundReturnsErrNotFound(t *testing.T) {
	srv := newTVmazeServer(t, "", "", http.StatusNotFound)
	defer srv.Close()

	c := enrich.NewTVmazeClient(srv.URL)
	_, err := c.SearchShows(context.Background(), "nonexistent", 0)
	if !errors.Is(err, enrich.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestTVmazeRateLimitDistinct(t *testing.T) {
	srv := newTVmazeServer(t, "", "", http.StatusTooManyRequests)
	defer srv.Close()

	c := enrich.NewTVmazeClient(srv.URL)
	_, err := c.SearchShows(context.Background(), "anything", 0)
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Errorf("expected rate-limit-distinct error, got %v", err)
	}
}

func TestCleanSummaryStripsTVmazeHTML(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"<p>Plain wrapped.</p>", "Plain wrapped."},
		{"<b>Bold</b> and <i>italic</i>.", "Bold and italic."},
		{"Line one<br>Line two<br/>Line three<br />done.", "Line oneLine twoLine threedone."},
		{"", ""},
		{"already clean", "already clean"},
	}
	for _, tc := range tests {
		got := enrich.CleanSummary(tc.in)
		if got != tc.want {
			t.Errorf("CleanSummary(%q): got %q want %q", tc.in, got, tc.want)
		}
	}
}
