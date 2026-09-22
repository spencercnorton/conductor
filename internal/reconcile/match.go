package reconcile

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Confidence scores per matching rule. The worker's threshold (default
// 0.80) splits auto-record from log-only/notify.
const (
	confTMDbID       = 1.00 // movie: enrichment TMDb id == Radarr tmdbId
	confIMDbID       = 0.98 // movie: enrichment IMDb id == Radarr imdbId
	confEpisodeTitle = 0.95 // episode: EPG sub_title == Sonarr episode title
	confOnscreenSE   = 0.90 // episode: EPG onscreen SxxExx == want S/E
	// SxxExx agrees but the two episode titles are both present and disagree.
	// Deliberately below the 0.80 auto-record threshold: log-only.
	confOnscreenSETitleConflict = 0.60
	confMovieTitle              = 0.60 // movie: title+year only (no id) — log-only
	confAirDate                 = 0.60 // episode: series title + is_new + air date — log-only
)

// MatchMovies returns at most one Match per Radarr want: the earliest
// upcoming airing that satisfies it, scored by the strongest rule that
// fired. Programs that aren't movies are ignored.
func MatchMovies(progs []Program, wants []MovieWant) []Match {
	// Index movie airings by the keys we match on.
	byTMDb := map[int][]Program{}
	byIMDb := map[string][]Program{}
	byTitle := map[string][]Program{}
	for _, p := range progs {
		if !p.IsMovie {
			continue
		}
		if p.TMDbID > 0 {
			byTMDb[p.TMDbID] = append(byTMDb[p.TMDbID], p)
		}
		if c := canonicalIMDb(p.IMDbID); c != "" {
			byIMDb[c] = append(byIMDb[c], p)
		}
		byTitle[titleKey(p.Title)] = append(byTitle[titleKey(p.Title)], p)
	}

	var out []Match
	for _, w := range wants {
		var best *Match
		consider := func(p Program, conf float64, reason string) {
			best = pick(best, Match{
				Program: p, Kind: "movie", Confidence: conf, Reason: reason,
				WantLabel: movieLabel(w), ArrTitle: w.Title,
			})
		}
		if w.TMDbID > 0 {
			for _, p := range byTMDb[w.TMDbID] {
				consider(p, confTMDbID, "tmdb-id")
			}
		}
		if c := canonicalIMDb(w.IMDbID); c != "" {
			for _, p := range byIMDb[c] {
				consider(p, confIMDbID, "imdb-id")
			}
		}
		if w.Title != "" {
			for _, p := range byTitle[titleKey(w.Title)] {
				if w.Year > 0 && !yearMatches(p, w.Year) {
					continue
				}
				consider(p, confMovieTitle, "title+year")
			}
		}
		if best != nil {
			out = append(out, *best)
		}
	}
	return out
}

// MatchEpisodes returns at most one Match per Sonarr want: the earliest
// upcoming airing whose series title matches and which we can tie to the
// specific episode (by episode title, by onscreen S/E, or — weakly — by
// first-run air date). Movies are ignored.
func MatchEpisodes(progs []Program, wants []EpisodeWant) []Match {
	bySeries := map[string][]Program{}
	bySeriesTVDB := map[int][]Program{}
	for _, p := range progs {
		if p.IsMovie {
			continue
		}
		bySeries[titleKey(p.Title)] = append(bySeries[titleKey(p.Title)], p)
		if p.TVDBID > 0 {
			bySeriesTVDB[p.TVDBID] = append(bySeriesTVDB[p.TVDBID], p)
		}
	}

	var out []Match
	for _, w := range wants {
		// Candidate airings = same series by title, UNION same series by
		// TVDB id (handles localized/alternate EPG titles once the
		// TMDb→TVDB crosswalk has populated tvdb_id). Dedup by program id.
		cands := seriesCandidates(bySeries[titleKey(w.SeriesTitle)], bySeriesTVDB[w.TVDBID])
		if len(cands) == 0 {
			continue
		}
		var best *Match
		consider := func(p Program, conf float64, reason string) {
			best = pick(best, Match{
				Program: p, Kind: "episode", Confidence: conf, Reason: reason,
				WantLabel: episodeLabel(w), ArrTitle: w.SeriesTitle,
				Season: w.Season, Episode: w.Episode,
			})
		}
		wantOnscreen := onscreen(w.Season, w.Episode)
		wantEpTitle := normalizeTitle(w.EpisodeTitle)
		for _, p := range cands {
			switch {
			case wantEpTitle != "" && p.SubTitle != "" &&
				normalizeTitle(p.SubTitle) == wantEpTitle:
				consider(p, confEpisodeTitle, "episode-title")
			case wantOnscreen != "" && p.EpisodeNumOnscreen != "" &&
				strings.EqualFold(p.EpisodeNumOnscreen, wantOnscreen):
				// A SxxExx that agrees while both episode TITLES are present
				// and disagree is weak evidence, not strong: sibling shows in
				// one franchise share a numbering space and the EPG tags them
				// with the parent's ids. Measured 2026-08-27 on TLC: EPG
				// "90 Day" / "The Last Resort" S03E09 matched Sonarr's
				// "90 Day Fiancé" S03E09 - "What Do You Know About Love?" at
				// 0.90 on the SxxExx alone. Different programme, same slot.
				//
				// Downgrade below the auto-record threshold rather than drop
				// it: the candidate still shows up in the preview and the
				// "candidate not recorded" log, so a real match with divergent
				// title conventions stays visible instead of vanishing.
				if wantEpTitle != "" && p.SubTitle != "" &&
					normalizeTitle(p.SubTitle) != wantEpTitle {
					consider(p, confOnscreenSETitleConflict, "onscreen-se-title-conflict")
					break
				}
				consider(p, confOnscreenSE, "onscreen-se")
			case w.AirDate != nil && p.IsNew && p.OriginalAirDate != nil &&
				sameDay(*p.OriginalAirDate, *w.AirDate):
				consider(p, confAirDate, "airdate+new")
			}
		}
		if best != nil {
			out = append(out, *best)
		}
	}
	return out
}

// confPlexGap scores an owned-but-incomplete gap match. Below the default
// auto-record threshold on purpose: these are inferred (not on a wanted
// list), so they default to preview-only unless gap-autorecord is enabled.
const confPlexGap = 0.70

// MatchPlexGaps finds upcoming episodes of series you OWN in Plex but are
// MISSING (and that Sonarr isn't already covering). For each owned series
// it emits at most one match per missing (series, episode), earliest airing
// first. excludeSeries holds series keys already handled by the Sonarr wanted
// path so the gap diff doesn't duplicate them.
//
// A gap candidate REQUIRES a parseable onscreen episode number (SxxExx). This
// is deliberate and load-bearing: unlike the Sonarr wanted path — where a
// known-real episode title anchors the EPG sub_title match — the gap path has
// no external anchor, so it can only trust an unambiguous episode number. This
// EPG's sub_title frequently carries a FRANCHISE qualifier rather than an
// episode title ("Law & Order" sub_title "Special Victims Unit", "Impractical
// Jokers" sub_title "Inside Jokes"), so subtitle-absence cannot prove an
// episode is missing — it produced ~2.5K daily false "gaps" before this guard.
// Requiring SxxExx also gives the only identity the recorder can name a file
// with for Sonarr/Radarr import. On an EPG without episode numbers this source
// is correctly near-empty until episode metadata is enriched in.
func MatchPlexGaps(progs []Program, owned *OwnedIndex, excludeSeries map[string]bool) []Match {
	if owned == nil {
		return nil
	}
	// Track the (series, episode) we've already emitted so multiple airings of
	// the same missing episode collapse to the earliest.
	type epKey struct{ series, ident string }
	best := map[epKey]*Match{}
	for _, p := range progs {
		if p.IsMovie {
			continue
		}
		sk := titleKey(p.Title)
		if !owned.OwnsSeries(sk) || excludeSeries[sk] {
			continue
		}
		season, episode := parseOnscreen(p.EpisodeNumOnscreen)
		if season == 0 && episode == 0 {
			continue // no reliable episode number — can't form an actionable gap
		}
		se := onscreen(season, episode) // canonical "SxxExx" for lookup + dedup
		// Subtitle remains a secondary ownership signal (it can only suppress a
		// candidate, never create one), so a real episode title still counts as
		// owned even when the library lacks the S/E key.
		if owned.OwnsEpisode(sk, p.SubTitle, se) {
			continue // already in the library
		}
		cand := Match{
			Program: p, Kind: "episode", Source: "plex-gap",
			Confidence: confPlexGap, Reason: "plex-owned-gap",
			WantLabel: fmt.Sprintf("%s %s (owned, missing)", p.Title, se),
			Season:    season, Episode: episode,
		}
		k := epKey{sk, se}
		best[k] = pick(best[k], cand)
	}
	out := make([]Match, 0, len(best))
	for _, m := range best {
		out = append(out, *m)
	}
	return out
}

// parseOnscreen extracts season/episode from an "SxxExx" string; returns
// 0,0 when absent or unparseable.
func parseOnscreen(se string) (season, episode int) {
	if len(se) < 6 || (se[0] != 'S' && se[0] != 's') {
		return 0, 0
	}
	var s, e int
	if _, err := fmt.Sscanf(strings.ToUpper(se), "S%dE%d", &s, &e); err != nil {
		return 0, 0
	}
	return s, e
}

// seriesCandidates merges the title-matched and TVDB-matched candidate
// lists, dropping duplicate airings (a program in both lists appears once).
func seriesCandidates(byTitle, byTVDB []Program) []Program {
	if len(byTVDB) == 0 {
		return byTitle
	}
	seen := make(map[uuid.UUID]struct{}, len(byTitle)+len(byTVDB))
	out := make([]Program, 0, len(byTitle)+len(byTVDB))
	for _, lst := range [][]Program{byTitle, byTVDB} {
		for _, p := range lst {
			if _, dup := seen[p.ProgramID]; dup {
				continue
			}
			seen[p.ProgramID] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}

// pick keeps the stronger of two candidate matches: higher confidence
// wins; on a tie the earlier airing wins (record the soonest chance).
func pick(cur *Match, cand Match) *Match {
	if cur == nil {
		return &cand
	}
	if cand.Confidence > cur.Confidence ||
		(cand.Confidence == cur.Confidence && cand.Program.StartAt.Before(cur.Program.StartAt)) {
		return &cand
	}
	return cur
}

func onscreen(season, episode int) string {
	if season <= 0 || episode <= 0 {
		return ""
	}
	return fmt.Sprintf("S%02dE%02d", season, episode)
}

func movieLabel(w MovieWant) string {
	if w.Year > 0 {
		return fmt.Sprintf("%s (%d)", w.Title, w.Year)
	}
	return w.Title
}

func episodeLabel(w EpisodeWant) string {
	se := onscreen(w.Season, w.Episode)
	if se == "" {
		return w.SeriesTitle
	}
	if w.EpisodeTitle != "" {
		return fmt.Sprintf("%s %s - %s", w.SeriesTitle, se, w.EpisodeTitle)
	}
	return fmt.Sprintf("%s %s", w.SeriesTitle, se)
}
