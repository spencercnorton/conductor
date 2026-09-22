package enrich

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Enrichment is the result of one enrichment pass on one program. The
// caller (worker.go) maps this onto the epg_program_enrichment columns.
type Enrichment struct {
	TMDbID        int
	TVDBID        int
	IMDbID        string
	PosterURL     string
	BackdropURL   string
	Overview      string
	Confidence    float64
	MatchedSource string // "tmdb_movie" | "tmdb_tv" | "tmdb_episode" | "manual"
}

// MinAcceptConfidence is the threshold below which we mark enrichment
// as "failed" rather than persisting a wrong match. Operator can
// override per-program via the manual_enrichment_match table.
//
// Spec §6.3 originally specced 0.85, but in practice that rejects
// most TV shows: they typically have no year info (yearScore=0.5
// "unknown"), low popularity (popularity<0.5), and no votes — so a
// perfect title match maxes around 0.70. Audit 2026-05-08 found
// 100/2528 unique programme titles matched (4%); only the popular
// movies cleared 0.85.
//
// Lowered to 0.65 + a near-exact-title short-circuit (see
// MatchScore: titleSim >= 0.95 forces accept). False-positive risk
// rises, but a wrong poster is a much smaller user cost than no
// poster at all (and TMDb's title disambiguation by year+popularity
// still wins for movies that DO have year data).
const MinAcceptConfidence = 0.65

// HighConfidenceTitleSim — when titleSim is at least this high AND the
// candidate has a secondary signal (year or non-trivial popularity),
// the match is accepted regardless of the overall blended confidence.
// Catches "show name is exactly right but TMDb has no year/popularity
// data" — common for international/news/talk programming. The
// secondary-signal guard prevents matching to TMDb stub entries that
// happen to share a generic title (e.g. "The NFL" id=235004, no air
// date, popularity ≈ 0) — those produce wrong posters.
const HighConfidenceTitleSim = 0.95

// EnrichInput is the typed view of every field Enrich actually reads from an
// epg_program row plus its immutable program/channel identity. Adding another
// mutable lookup input here must also extend store's enrichment input key and
// its guarded-write regressions so an in-flight result cannot cross identities.
type EnrichInput struct {
	ProgramID          uuid.UUID
	ChannelID          uuid.UUID
	Title              string
	OriginalAirDate    *time.Time
	IsMovie            bool
	EpisodeNumOnscreen string // "S03E07" or ""
}

// ManualLookup is the contract enricher uses to check for operator overrides
// before hitting TMDb. Implemented by the store package.
type ManualLookup interface {
	LookupManualMatch(ctx context.Context, channelID uuid.UUID, normalizedTitle string, isMovie bool) (tmdbID int, tvdbID int, imdbID string, ok bool, err error)
}

// Enricher orchestrates: manual override → TMDb → TVDb → TVmaze → matcher.
//
// Each fallback fires only when (a) the previous source is configured
// AND missed, OR (b) the previous source is not configured. Order is
// intentional:
//
//   - TVDb second because it's paid/PIN-gated but covers movies + TV;
//   - TVmaze third because it's free + no-auth but TV-only (and TVmaze's
//     `externals` field still hands us TVDB IDs, so Plex's TVDB-agent
//     match still works on the way out).
type Enricher struct {
	TMDb          *TMDbClient
	TVDb          *TVDbClient   // optional fallback
	TVmaze        *TVmazeClient // optional TV-only fallback (free, no auth)
	Manual        ManualLookup
	MinConfidence float64 // overridable for tests; defaults to MinAcceptConfidence
}

func NewEnricher(t *TMDbClient, m ManualLookup) *Enricher {
	return &Enricher{TMDb: t, Manual: m, MinConfidence: MinAcceptConfidence}
}

// WithTVDb returns the same Enricher with the TVDB fallback wired up.
// Used so callers can decide independently whether to enable each source.
func (e *Enricher) WithTVDb(t *TVDbClient) *Enricher {
	e.TVDb = t
	return e
}

// WithTVmaze returns the same Enricher with the TVmaze fallback wired
// up. TVmaze is TV-only — movie enrichment short-circuits past it.
func (e *Enricher) WithTVmaze(t *TVmazeClient) *Enricher {
	e.TVmaze = t
	return e
}

// Enrich runs the pipeline for one program. Returns nil + nil on no-match
// (caller should mark enrichment_status='failed'); returns Enrichment + nil
// on success.
func (e *Enricher) Enrich(ctx context.Context, in EnrichInput) (*Enrichment, error) {
	if (e.TMDb == nil || e.TMDb.APIKey == "") && (e.TVDb == nil || e.TVDb.APIKey == "") && e.TVmaze == nil {
		return nil, errors.New("no enrichment source configured (TMDb, TVDB, or TVmaze)")
	}

	// Manual override short-circuits everything.
	if e.Manual != nil {
		titleNorm := normalizeForMatch(in.Title)
		tmdbID, tvdbID, imdbID, ok, err := e.Manual.LookupManualMatch(ctx, in.ChannelID, titleNorm, in.IsMovie)
		if err == nil && ok {
			out, err := e.applyManual(ctx, in, tmdbID, tvdbID, imdbID)
			if err != nil {
				return nil, err
			}
			return out, nil
		}
	}

	// TMDb first (when configured).
	var primaryResult *Enrichment
	var primaryErr error
	if e.TMDb != nil && e.TMDb.APIKey != "" {
		if in.IsMovie {
			primaryResult, primaryErr = e.enrichMovie(ctx, in)
		} else {
			primaryResult, primaryErr = e.enrichTV(ctx, in)
		}
		if primaryErr == nil && primaryResult != nil {
			return primaryResult, nil
		}
	}

	// TVDB fallback when (a) TMDb missed entirely, OR (b) TMDb is not
	// configured. We don't fall back on TMDb hard errors (HTTP failure,
	// rate limit) because that's a transient — the worker will re-queue
	// the program; we want to retry TMDb next pass, not race TVDb.
	if e.TVDb != nil && e.TVDb.APIKey != "" {
		if in.IsMovie {
			r, err := e.enrichMovieTVDb(ctx, in)
			if err == nil && r != nil {
				return r, nil
			}
		} else {
			r, err := e.enrichTVTVDb(ctx, in)
			if err == nil && r != nil {
				return r, nil
			}
		}
	}

	// TVmaze fallback when the prior sources missed. TV-only — TVmaze
	// doesn't cover movies, so the movie path skips this branch.
	if e.TVmaze != nil && !in.IsMovie {
		if r, err := e.enrichTVTVmaze(ctx, in); err == nil && r != nil {
			return r, nil
		}
	}

	// Either no provider matched or TMDb errored hard. Bubble up the TMDb
	// error if there was one (so the worker logs it); otherwise return
	// (nil, nil) for "no match found" — worker marks failed without
	// flagging the row for diagnostic attention.
	if primaryErr != nil {
		return nil, primaryErr
	}
	return nil, nil
}

func (e *Enricher) enrichMovie(ctx context.Context, in EnrichInput) (*Enrichment, error) {
	year := 0
	if in.OriginalAirDate != nil {
		year = in.OriginalAirDate.Year()
	}
	results, err := e.TMDb.SearchMovie(ctx, in.Title, year)
	if err != nil {
		return nil, fmt.Errorf("search movie: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}

	best := -1
	bestScore := 0.0
	bestTitleSim := 0.0
	bestHasSecondary := false
	for i, r := range results {
		m := MatchScore(MatchInput{
			TargetTitle: in.Title, TargetYear: year,
			CandidateTitle: r.Title, CandidateOrigTitle: r.OriginalTitle,
			CandidateReleaseStr: r.ReleaseDate,
			CandidatePopularity: r.Popularity, CandidateVoteAvg: r.VoteAverage,
		})
		if m.Confidence > bestScore {
			bestScore = m.Confidence
			bestTitleSim = m.TitleSim
			bestHasSecondary = m.HasSecondarySignal
			best = i
		}
	}
	// Accept if either the blended confidence clears the threshold OR the
	// best candidate's title is a near-exact match AND has a secondary
	// signal (year/popularity) — title-alone short-circuit attracts
	// stub TMDb entries that share a generic title.
	if best < 0 || (bestScore < e.MinConfidence && (bestTitleSim < HighConfidenceTitleSim || !bestHasSecondary)) {
		return nil, nil
	}
	r := results[best]
	imdb := ""
	if ids, err := e.TMDb.GetMovieExternalIDs(ctx, r.ID); err == nil {
		imdb = ids.IMDBID
	}
	return &Enrichment{
		TMDbID:        r.ID,
		IMDbID:        imdb,
		PosterURL:     e.TMDb.PosterURL(r.PosterPath),
		BackdropURL:   e.TMDb.BackdropURL(r.BackdropPath),
		Overview:      r.Overview,
		Confidence:    bestScore,
		MatchedSource: "tmdb_movie",
	}, nil
}

func (e *Enricher) enrichTV(ctx context.Context, in EnrichInput) (*Enrichment, error) {
	results, err := e.TMDb.SearchTV(ctx, in.Title, 0)
	if err != nil {
		return nil, fmt.Errorf("search tv: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}

	best := -1
	bestScore := 0.0
	bestTitleSim := 0.0
	bestHasSecondary := false
	for i, r := range results {
		m := MatchScore(MatchInput{
			TargetTitle:    in.Title,
			CandidateTitle: r.Name, CandidateOrigTitle: r.OriginalName,
			CandidateReleaseStr: r.FirstAirDate,
			CandidatePopularity: r.Popularity, CandidateVoteAvg: r.VoteAverage,
		})
		if m.Confidence > bestScore {
			bestScore = m.Confidence
			bestTitleSim = m.TitleSim
			bestHasSecondary = m.HasSecondarySignal
			best = i
		}
	}
	if best < 0 || (bestScore < e.MinConfidence && (bestTitleSim < HighConfidenceTitleSim || !bestHasSecondary)) {
		return nil, nil
	}
	show := results[best]
	out := &Enrichment{
		TMDbID:        show.ID,
		PosterURL:     e.TMDb.PosterURL(show.PosterPath),
		BackdropURL:   e.TMDb.BackdropURL(show.BackdropPath),
		Overview:      show.Overview,
		Confidence:    bestScore,
		MatchedSource: "tmdb_tv",
	}

	// TMDb→TVDB/IMDb crosswalk: Sonarr keys on TVDB ids, which our EPG
	// otherwise lacks. Best-effort — a miss just leaves the ids unset.
	if ids, err := e.TMDb.GetTVExternalIDs(ctx, show.ID); err == nil {
		out.TVDBID = ids.TVDBID
		out.IMDbID = ids.IMDBID
	}

	// Episode lookup: SxxExx → /tv/{id}/season/{n}/episode/{m}.
	// Per-episode overview enriches the description, but we DO NOT
	// overwrite PosterURL with the episode's still_path — TMDb stills
	// are 16:9 landscape (1920×1080 native) while Plex DVR's XMLTV
	// parser renders <image type="poster"> in a 2:3 portrait frame.
	// Stretching a landscape still into portrait is the
	// "stretched posters / pictures of the show" symptom users see on
	// Plex tiles. Series poster (2:3) stays put.
	season, episode := parseSE(in.EpisodeNumOnscreen)
	if season > 0 && episode > 0 {
		ep, err := e.TMDb.GetEpisode(ctx, show.ID, season, episode)
		if err == nil && ep != nil {
			if ep.Overview != "" {
				out.Overview = ep.Overview
			}
			out.MatchedSource = "tmdb_episode"
		}
	}
	return out, nil
}

func (e *Enricher) applyManual(ctx context.Context, in EnrichInput, tmdbID, tvdbID int, imdbID string) (*Enrichment, error) {
	out := &Enrichment{
		TMDbID:        tmdbID,
		TVDBID:        tvdbID,
		IMDbID:        imdbID,
		Confidence:    1.0,
		MatchedSource: "manual",
	}
	// Hydrate from TMDb where possible.
	if in.IsMovie {
		// Movie lookup not implemented in the client (we only do search).
		// Phase 3b can add /movie/{id} GET to populate poster/overview.
		// For now the manual override gives us the IDs, which is the
		// load-bearing part for Plex matching.
	}
	return out, nil
}

// parseSE extracts (season, episode) from "S03E07". Returns (0,0) on any
// parse error so caller skips the episode-specific lookup.
func parseSE(s string) (int, int) {
	s = strings.ToUpper(s)
	if !strings.HasPrefix(s, "S") {
		return 0, 0
	}
	rest := s[1:]
	idx := strings.Index(rest, "E")
	if idx < 0 {
		return 0, 0
	}
	season, err1 := atoiSafe(rest[:idx])
	episode, err2 := atoiSafe(rest[idx+1:])
	if err1 != nil || err2 != nil {
		return 0, 0
	}
	return season, episode
}

// ─────────────────────── TVDB (fallback) ───────────────────────

func (e *Enricher) enrichMovieTVDb(ctx context.Context, in EnrichInput) (*Enrichment, error) {
	year := 0
	if in.OriginalAirDate != nil {
		year = in.OriginalAirDate.Year()
	}
	results, err := e.TVDb.SearchMovie(ctx, in.Title, year)
	if err != nil {
		return nil, fmt.Errorf("tvdb search movie: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}

	best := -1
	bestScore := 0.0
	bestTitleSim := 0.0
	bestHasSecondary := false
	for i, r := range results {
		yr := 0
		if r.Year != "" {
			yr = parseYearPrefix(r.Year + "-01-01")
		}
		_ = yr // currently unused in MatchScore (we use the candidate's release_date)
		m := MatchScore(MatchInput{
			TargetTitle: in.Title, TargetYear: year,
			CandidateTitle: r.Name, CandidateOrigTitle: r.Name,
			CandidateReleaseStr: r.Year + "-01-01",
			// TVDb doesn't expose popularity/vote — neutral defaults.
			CandidatePopularity: 5.0, CandidateVoteAvg: 7.0,
		})
		if m.Confidence > bestScore {
			bestScore = m.Confidence
			bestTitleSim = m.TitleSim
			bestHasSecondary = m.HasSecondarySignal
			best = i
		}
	}
	if best < 0 || (bestScore < e.MinConfidence && (bestTitleSim < HighConfidenceTitleSim || !bestHasSecondary)) {
		return nil, nil
	}
	r := results[best]
	return &Enrichment{
		TVDBID:        r.ID,
		IMDbID:        r.IMDbID,
		PosterURL:     r.Image,
		Overview:      r.Overview,
		Confidence:    bestScore,
		MatchedSource: "tvdb_movie",
	}, nil
}

func (e *Enricher) enrichTVTVDb(ctx context.Context, in EnrichInput) (*Enrichment, error) {
	results, err := e.TVDb.SearchSeries(ctx, in.Title, 0)
	if err != nil {
		return nil, fmt.Errorf("tvdb search series: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}

	best := -1
	bestScore := 0.0
	bestTitleSim := 0.0
	bestHasSecondary := false
	for i, r := range results {
		m := MatchScore(MatchInput{
			TargetTitle:    in.Title,
			CandidateTitle: r.Name, CandidateOrigTitle: r.Name,
			CandidateReleaseStr: r.FirstAired,
			CandidatePopularity: 5.0, CandidateVoteAvg: 7.0,
		})
		if m.Confidence > bestScore {
			bestScore = m.Confidence
			bestTitleSim = m.TitleSim
			bestHasSecondary = m.HasSecondarySignal
			best = i
		}
	}
	if best < 0 || (bestScore < e.MinConfidence && (bestTitleSim < HighConfidenceTitleSim || !bestHasSecondary)) {
		return nil, nil
	}
	r := results[best]
	return &Enrichment{
		TVDBID:        r.ID,
		IMDbID:        r.IMDbID,
		PosterURL:     r.Image,
		Overview:      r.Overview,
		Confidence:    bestScore,
		MatchedSource: "tvdb_series",
	}, nil
}

// ─────────────────────── TVmaze (free fallback, TV only) ───────────────────────

func (e *Enricher) enrichTVTVmaze(ctx context.Context, in EnrichInput) (*Enrichment, error) {
	results, err := e.TVmaze.SearchShows(ctx, in.Title, 0)
	if err != nil {
		return nil, fmt.Errorf("tvmaze search shows: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}

	best := -1
	bestScore := 0.0
	bestTitleSim := 0.0
	bestHasSecondary := false
	for i, r := range results {
		m := MatchScore(MatchInput{
			TargetTitle:         in.Title,
			CandidateTitle:      r.Name,
			CandidateOrigTitle:  r.Name,
			CandidateReleaseStr: r.Premiered,
			// TVmaze doesn't expose popularity / vote averages —
			// neutral defaults so titleSim drives the score (same
			// strategy the TVDb branch uses).
			CandidatePopularity: 5.0,
			CandidateVoteAvg:    7.0,
		})
		if m.Confidence > bestScore {
			bestScore = m.Confidence
			bestTitleSim = m.TitleSim
			bestHasSecondary = m.HasSecondarySignal
			best = i
		}
	}
	if best < 0 || (bestScore < e.MinConfidence && (bestTitleSim < HighConfidenceTitleSim || !bestHasSecondary)) {
		return nil, nil
	}
	r := results[best]
	return &Enrichment{
		// TVmaze's externals expose TVDB + IMDb IDs directly, so
		// Plex's TVDB-agent matching still works downstream.
		TVDBID:        r.Externals.TheTVDB,
		IMDbID:        r.Externals.IMDb,
		PosterURL:     r.PosterURL(),
		Overview:      CleanSummary(r.Summary),
		Confidence:    bestScore,
		MatchedSource: "tvmaze_tv",
	}, nil
}

func atoiSafe(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("non-digit: %q", c)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}
