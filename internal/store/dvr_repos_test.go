package store_test

import (
	"testing"

	"github.com/spencercnorton/conductor/internal/store"
)

// TestEPGIndexerSearchHasPrimaryFilter pins the anti-flood gate: only a
// free-text query or an external id counts as a primary filter. Season,
// episode and year are narrowing-only — a search carrying just those must
// not be treated as answerable, since it would match the whole catalog.
func TestEPGIndexerSearchHasPrimaryFilter(t *testing.T) {
	cases := []struct {
		name string
		s    store.EPGIndexerSearch
		want bool
	}{
		{"empty", store.EPGIndexerSearch{}, false},
		{"season+episode only", store.EPGIndexerSearch{Season: 3, Episode: 7}, false},
		{"year only", store.EPGIndexerSearch{Year: 1995}, false},
		{"season+episode+year, no title or id", store.EPGIndexerSearch{Season: 3, Episode: 7, Year: 2024}, false},
		{"movie-only flag is not a filter", store.EPGIndexerSearch{MovieOnly: true}, false},
		{"query", store.EPGIndexerSearch{Query: "The Bear"}, true},
		{"imdb id", store.EPGIndexerSearch{IMDbID: "tt0848228"}, true},
		{"tmdb id", store.EPGIndexerSearch{TMDbID: 24428}, true},
		{"tvdb id", store.EPGIndexerSearch{TVDBID: 81189}, true},
		{"query plus narrowing", store.EPGIndexerSearch{Query: "The Bear", Season: 3, Episode: 7}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.s.HasPrimaryFilter(); got != tc.want {
				t.Errorf("HasPrimaryFilter() = %v, want %v", got, tc.want)
			}
		})
	}
}
