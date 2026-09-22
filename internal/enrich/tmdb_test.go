package enrich_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/enrich"
)

func TestTMDbSearchMovieParsesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/search/movie") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("api_key"); got != "test-key" {
			t.Errorf("api_key: got %q", got)
		}
		if got := r.URL.Query().Get("query"); got != "Heat" {
			t.Errorf("query: got %q", got)
		}
		if got := r.URL.Query().Get("year"); got != "1995" {
			t.Errorf("year: got %q", got)
		}
		_, _ = w.Write([]byte(`{
			"page": 1,
			"total_results": 1,
			"results": [{
				"id": 949,
				"title": "Heat",
				"original_title": "Heat",
				"release_date": "1995-12-15",
				"overview": "A career thief plans one last big score.",
				"poster_path": "/zMyfPUelumio3tiDKPffaUpsQTD.jpg",
				"backdrop_path": "/rfEXNlql4CafRmtgp2VFQrBC4xV.jpg",
				"popularity": 30.5,
				"vote_average": 7.9
			}]
		}`))
	}))
	defer srv.Close()

	c := enrich.NewTMDbClient(srv.URL, "test-key")
	results, err := c.SearchMovie(context.Background(), "Heat", 1995)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results: got %d want 1", len(results))
	}
	r := results[0]
	if r.ID != 949 {
		t.Errorf("ID: got %d want 949", r.ID)
	}
	if r.Title != "Heat" {
		t.Errorf("Title: got %q want Heat", r.Title)
	}
	if r.PosterPath != "/zMyfPUelumio3tiDKPffaUpsQTD.jpg" {
		t.Errorf("PosterPath: got %q", r.PosterPath)
	}

	// Image URL builder should produce an absolute https URL.
	url := c.PosterURL(r.PosterPath)
	if !strings.HasPrefix(url, "https://image.tmdb.org/t/p/w500/") {
		t.Errorf("PosterURL: got %q", url)
	}
}

func TestTMDbSearchTVParsesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"results": [{
				"id": 136315,
				"name": "The Bear",
				"original_name": "The Bear",
				"first_air_date": "2022-06-23",
				"overview": "Carmy returns to Chicago.",
				"poster_path": "/zPyHtXG9aBeXgU6PjmDh9kPRBlV.jpg",
				"popularity": 80.5,
				"vote_average": 8.6
			}]
		}`))
	}))
	defer srv.Close()

	c := enrich.NewTMDbClient(srv.URL, "k")
	results, err := c.SearchTV(context.Background(), "The Bear", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != 136315 {
		t.Fatalf("unexpected results: %+v", results)
	}
}

func TestTMDbReturnsErrNotFoundOn404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()
	c := enrich.NewTMDbClient(srv.URL, "k")
	_, err := c.GetEpisode(context.Background(), 1, 1, 1)
	if err == nil {
		t.Error("expected error on 404")
	}
	if err != enrich.ErrNotFound {
		t.Errorf("expected ErrNotFound; got %v", err)
	}
}

// fakeCache implements TMDbCache in-memory for the cache-roundtrip test.
type fakeCache struct {
	store map[string]json.RawMessage
	puts  int
	hits  int
}

func (f *fakeCache) Get(_ context.Context, kind, hash string) (json.RawMessage, bool, error) {
	v, ok := f.store[kind+":"+hash]
	if ok {
		f.hits++
	}
	return v, ok, nil
}
func (f *fakeCache) Put(_ context.Context, kind, hash, _ string, body json.RawMessage) error {
	if f.store == nil {
		f.store = map[string]json.RawMessage{}
	}
	f.store[kind+":"+hash] = body
	f.puts++
	return nil
}

func TestTMDbCacheShortCircuitsSecondCall(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"results": [{"id": 1, "title": "x"}]}`))
	}))
	defer srv.Close()

	cache := &fakeCache{store: map[string]json.RawMessage{}}
	c := enrich.NewTMDbClient(srv.URL, "k")
	c.Cache = cache

	if _, err := c.SearchMovie(context.Background(), "x", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SearchMovie(context.Background(), "x", 0); err != nil {
		t.Fatal(err)
	}
	if hits != 1 {
		t.Errorf("expected 1 upstream HTTP call (cache should serve the second); got %d", hits)
	}
	if cache.puts != 1 || cache.hits != 1 {
		t.Errorf("cache stats: puts=%d hits=%d (want 1, 1)", cache.puts, cache.hits)
	}
}

// suppress unused import lint
var _ = time.Now
