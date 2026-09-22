// Package reconcile turns Conductor's DVR from a passive Torznab indexer
// into a proactive recorder.
//
// Background: the Torznab indexer (internal/dvr) only ever records when
// Sonarr/Radarr happen to search Conductor for an already-monitored item
// AND the EPG row carries the exact season/episode number the search asks
// for. EPG data almost never carries onscreen episode numbers, so in
// practice that path records nothing.
//
// The reconciler inverts the flow: once per cycle it pulls Sonarr's and
// Radarr's wanted/missing lists, snapshots the upcoming EPG window, matches
// them in memory, and schedules a dvr_recording row for each hit. The
// existing scheduler/recorder/postprocess pipeline (internal/dvr) takes it
// from there unchanged.
//
// Matching is keyed on what each *arr actually provides reliably:
//   - Movies: Radarr's TMDb id against epg_program_enrichment.tmdb_id
//     (~70% EPG coverage). High precision.
//   - Episodes: Sonarr's episode TITLE against the EPG sub_title, because
//     Sonarr uses TVDB ids (which the EPG lacks) and the EPG lacks onscreen
//     S/E numbers. Episode-title matching sidesteps both gaps.
package reconcile

import (
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// Program aliases the store row so callers in this package don't import
// store directly for the common case.
type Program = store.ReconcileProgram

// MovieWant is one Radarr wanted/missing movie.
type MovieWant struct {
	TMDbID int
	IMDbID string // canonical "tt0848228" form, or "" if absent
	Title  string
	Year   int
}

// EpisodeWant is one Sonarr wanted/missing episode, with its series
// context attached (Sonarr returns the series inline when asked).
type EpisodeWant struct {
	SeriesTitle  string
	SeriesYear   int
	TVDBID       int
	Season       int
	Episode      int
	EpisodeTitle string
	AirDate      *time.Time // episode air date (UTC), nil if unknown
}

// Match is one upcoming program the matcher believes satisfies a want.
type Match struct {
	Program    Program
	Kind       string  // "movie" | "episode"
	Source     string  // "wanted" (Sonarr/Radarr) | "plex-gap" (owned-but-incomplete)
	WantLabel  string  // human label of the want, for logs/recording.requested_by
	Confidence float64 // 0..1; the worker auto-records at/above its threshold
	Reason     string  // which rule fired (e.g. "tmdb-id", "episode-title")

	// Season/Episode carry the want's numbers (episodes only) so the worker
	// can build a Sonarr-style output path even when the EPG row itself has
	// no onscreen episode number. Zero for movies.
	Season  int
	Episode int

	// ArrTitle is the *arr's own title for the matched series/movie, which is
	// not always the EPG's. The matcher deliberately unions title matches with
	// TVDb/TMDb id matches so a localized or alternate EPG title still matches
	// the right want — and when the id is what matched, the two titles differ.
	//
	// The recording must be named from THIS, not from Program.Title: *arr
	// resolves the series from the path it is handed on import, so an EPG-named
	// file is rejected as "Unknown Series" and the episode stays missing, which
	// makes the reconciler re-book the next airing forever. Measured 2026-08-27:
	// EPG "90 Day" matched Sonarr's "90 Day Fiancé" by TVDb id, recorded to
	// TV/90 Day/, never imported, and was re-booked 11 times for one episode.
	//
	// Empty for plex-gap matches, where the gap is only formed when the EPG
	// title already normalizes onto the owned series key — there is no second
	// title to prefer. Callers fall back to Program.Title.
	ArrTitle string
}

// ProgramID is a convenience accessor used by dedup logic.
func (m Match) ProgramID() uuid.UUID { return m.Program.ProgramID }
