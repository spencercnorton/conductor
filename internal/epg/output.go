package epg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// Output is the /xmltv.xml + /epg.xml handler. Spec §7 — emit exactly the
// tags Plex parses; anything else gets dropped.
type Output struct {
	db      *store.DB
	baseURL string
	now     func() time.Time
}

func NewOutput(db *store.DB, baseURL string) *Output {
	return &Output{db: db, baseURL: baseURL, now: time.Now}
}

// ServeHTTP emits XMLTV for the next 14 days of programs.
//
// The ETag is a digest of the complete output model. Timestamp-only validators
// miss deletions, channel edits, and overlapping commits whose transaction
// timestamps are not commit ordered. Plex polls infrequently, so querying the
// output rows before deciding on 304 is a small cost for exact invalidation.
//
// Response is gzip-encoded if the client supports it; XMLTV compresses
// ~10x and Plex always advertises gzip.
func (o *Output) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := o.now()
	_, _, overlaps, err := o.db.EPGCanonicalHealth(ctx)
	if err != nil {
		http.Error(w, "epg canonical health: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if overlaps != 0 {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "epg canonical schedule contains overlapping programmes", http.StatusServiceUnavailable)
		return
	}

	// Last-Modified remains useful telemetry, but it is not a safe conditional
	// validator for this time-relative representation. Rows enter at now+14d and
	// leave at end+2h without a database mutation, so an IMS-only 304 could hide a
	// changed guide forever. The content digest below is the sole 304 authority.
	latest, err := o.db.LatestProgramUpdate(ctx)
	if err != nil {
		http.Error(w, "epg revision: "+err.Error(), http.StatusInternalServerError)
		return
	}
	from := now.Add(-2 * time.Hour) // include current program
	to := now.Add(14 * 24 * time.Hour)
	rows, err := o.db.ListProgramsForOutput(ctx, from, to)
	if err != nil {
		http.Error(w, "epg list: "+err.Error(), http.StatusInternalServerError)
		return
	}
	serveXMLTVRepresentation(w, r, rows, o.baseURL, latest, now)
}

// serveXMLTVRepresentation owns conditional semantics for both GET and HEAD.
// This guide changes as rows cross the moving output horizon, even when the
// database revision is unchanged. Therefore
// If-Modified-Since is deliberately informational only: If-None-Match is the
// sole authority for a 304 response.
func serveXMLTVRepresentation(
	w http.ResponseWriter,
	r *http.Request,
	rows []store.ProgramOutputRow,
	baseURL string,
	latest, now time.Time,
) {
	rows = projectProgramsForOutput(rows, now)
	etag := outputETag(rows, baseURL)
	w.Header().Set("ETag", etag)
	lastModified := latest.UTC().Truncate(time.Second)
	if !latest.IsZero() {
		w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	if match := r.Header.Get("If-None-Match"); match != "" {
		// RFC 9110: If-None-Match takes precedence whenever it is present.
		// Falling through to a stale timestamp after a digest mismatch would
		// hide deletion and channel-only changes from Plex.
		if ifNoneMatchMatches(match, etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	emitXMLTVProjected(w, rows, baseURL)
}

// If-None-Match uses weak entity-tag comparison for GET. Accept validator
// lists and the wildcard as well as Plex's usual single strong tag.
func ifNoneMatchMatches(headerValue, currentETag string) bool {
	current := strings.TrimSpace(currentETag)
	if len(current) >= 2 && strings.EqualFold(current[:2], "W/") {
		current = strings.TrimSpace(current[2:])
	}
	for _, raw := range strings.Split(headerValue, ",") {
		candidate := strings.TrimSpace(raw)
		if candidate == "*" {
			return true
		}
		if len(candidate) >= 2 && strings.EqualFold(candidate[:2], "W/") {
			candidate = strings.TrimSpace(candidate[2:])
		}
		if candidate != "" && candidate == current {
			return true
		}
	}
	return false
}

func outputETag(rows []store.ProgramOutputRow, baseURL string) string {
	h := sha256.New()
	_ = json.NewEncoder(h).Encode(struct {
		BaseURL string                   `json:"base_url"`
		Rows    []store.ProgramOutputRow `json:"rows"`
	}{BaseURL: baseURL, Rows: rows})
	sum := h.Sum(nil)
	return `"epg-` + hex.EncodeToString(sum[:16]) + `"`
}

// projectProgramsForOutput describes the scheduled broadcast without mutating
// the stored guide. Plex ignores <live/> and caches titles when it imports the
// guide, often before an event starts. A descriptor of the airing must therefore
// be stable across its start/stop boundaries; it is not a "live right now" badge.
// Plex shares episode titles/subtitles between airings. Recognized episodes
// remove provider live decorations consistently, retain the underlying names/IDs,
// and only expose the compatibility <live/> marker. Explicit repeat
// evidence or a proven earlier airing overrides a conflicting source live flag;
// unknown first-run state remains independent of live.
func projectProgramsForOutput(rows []store.ProgramOutputRow, _ time.Time) []store.ProgramOutputRow {
	out := make([]store.ProgramOutputRow, len(rows))
	for i, row := range rows {
		row.Title, row.SubTitle = rewriteOffSeasonTitle(row.Title, row.SubTitle)
		row.IsPlaceholder = row.IsPlaceholder || isIdleGuidePlaceholder(row.Title, row.SubTitle)
		negativeLiveCue := hasNonLiveBroadcastCue(row.Title, row.SubTitle)
		if row.HasEpisodeIdentity {
			// Normalize every airing, including an already-unflagged rerun. Stripping
			// only repeats would give Plex conflicting titles for one shared episode.
			row.Title = stripEpisodeVisibleLiveCue(row.Title)
			row.SubTitle = stripEpisodeVisibleLiveCue(row.SubTitle)
		} else if row.IsLive || negativeLiveCue {
			row.Title = stripVisibleLiveCue(row.Title)
			row.SubTitle = stripVisibleLiveCue(row.SubTitle)
			if row.Title == "" {
				if !row.HasEpisodeIdentity && row.SubTitle != "" {
					row.Title, row.SubTitle = row.SubTitle, ""
				} else {
					row.Title = "Event"
				}
			}
		}
		row.IsPlaceholder = row.IsPlaceholder || isIdleGuidePlaceholder(row.Title, row.SubTitle)
		row.IsLive = row.IsLive && !row.IsPlaceholder && !row.PreviouslyShownExplicit &&
			!row.HasEarlierAiring && !negativeLiveCue
		if row.IsLive && !row.HasEpisodeIdentity {
			if row.Title == "" {
				row.Title = "LIVE broadcast — Event"
			} else {
				row.Title = "LIVE broadcast — " + row.Title
			}
		}
		out[i] = row
	}
	return out
}

// stripEpisodeVisibleLiveCue removes explicit provider decorations without
// inventing a fallback episode/series name. Bare Live and ordinary compounds
// such as Live-Action remain titles; repeated decorations normalize once for
// consistent metadata on both live and repeat airings.
func stripEpisodeVisibleLiveCue(value string) string {
	for {
		trimmed := strings.TrimSpace(value)
		lower := strings.ToLower(trimmed)
		removedSuffix := false
		for _, suffix := range []string{" (live)", " [live]"} {
			if strings.HasSuffix(lower, suffix) {
				rest := strings.TrimSpace(trimmed[:len(trimmed)-len(suffix)])
				if rest != "" {
					value = rest
					removedSuffix = true
				}
				break
			}
		}
		if removedSuffix {
			continue
		}
		// An attached hyphen can be part of a real title, not an airing annotation.
		if strings.HasPrefix(lower, "live-") && len(lower) > len("live-") &&
			!strings.ContainsAny(lower[len("live-"):len("live-")+1], " \t\r\n") {
			return value
		}
		stripped := stripVisibleLiveCue(trimmed)
		if stripped == "" || stripped == trimmed {
			return value
		}
		value = stripped
	}
}

func stripVisibleLiveCue(value string) string {
	value = strings.TrimSpace(value)
	lower := strings.ToLower(value)
	for _, marker := range []string{"live coverage", "live broadcast"} {
		if lower == marker {
			return ""
		}
		if !strings.HasPrefix(lower, marker) {
			continue
		}
		rest := value[len(marker):]
		spaceDelimited := len(strings.TrimLeft(rest, " \t\r\n")) < len(rest)
		rest = strings.TrimSpace(rest)
		separatorDelimited := false
		for _, separator := range []string{":", "-", "–", "—"} {
			if strings.HasPrefix(rest, separator) {
				rest = strings.TrimSpace(strings.TrimPrefix(rest, separator))
				separatorDelimited = true
				break
			}
		}
		if spaceDelimited || separatorDelimited {
			return rest
		}
	}
	for _, prefix := range []string{
		"live — ", "live - ", "live: ", "live-", "live:",
	} {
		if strings.HasPrefix(lower, prefix) {
			return strings.TrimSpace(value[len(prefix):])
		}
	}
	for _, suffix := range []string{" (live)", " [live]"} {
		if strings.HasSuffix(lower, suffix) {
			return strings.TrimSpace(value[:len(value)-len(suffix)])
		}
	}
	if lower == "live" {
		return ""
	}
	return value
}

func isIdleGuidePlaceholder(title, subTitle string) bool {
	title = strings.ToLower(strings.TrimSpace(title))
	subTitle = strings.ToLower(strings.TrimSpace(subTitle))
	for _, marker := range []string{
		"no event scheduled", "no game today", "off-season", "off season",
		"channel offline", "event unavailable", "nothing scheduled",
	} {
		if title == marker || subTitle == marker ||
			strings.HasSuffix(title, " — "+marker) || strings.HasSuffix(title, " - "+marker) {
			return true
		}
	}
	return false
}

// emitXMLTV writes the XMLTV document. See spec §7 for the tag inventory.
func emitXMLTV(w http.ResponseWriter, rows []store.ProgramOutputRow, baseURL string) {
	emitXMLTVProjected(w, projectProgramsForOutput(rows, time.Now()), baseURL)
}

func emitXMLTVProjected(w http.ResponseWriter, rows []store.ProgramOutputRow, baseURL string) {
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write([]byte(`<tv generator-info-name="Conductor" source-info-name="conductor">` + "\n"))

	// Channels — emit each unique channel once. Plex hashes the id for
	// matching across runs, so it must be stable. We use the call_sign or
	// epg_channel_id as the id — whichever is present.
	seenChannel := map[string]bool{}
	for _, r := range rows {
		id := xmltvChannelID(r)
		if seenChannel[id] {
			continue
		}
		seenChannel[id] = true
		emitChannel(w, r, id, baseURL)
	}

	// Explicit PPV idle notices restore useful next-event information in the
	// grid. Plex has no supported channel-level next-event field, so these
	// notices also appear on its unsectioned "Shows On Now" shelf.
	// That tradeoff is intentional. Other heuristic filler stays hidden, and
	// notices bypass all real-programme identity, enrichment and airing flags.
	for _, r := range rows {
		if r.IsPPVPlaceholder {
			emitPPVIdleNotice(w, r)
			continue
		}
		if r.IsPlaceholder {
			continue
		}
		emitProgramme(w, r, baseURL)
	}

	_, _ = w.Write([]byte(`</tv>` + "\n"))
}

func emitPPVIdleNotice(w http.ResponseWriter, r store.ProgramOutputRow) {
	fmt.Fprintf(w, "  <programme channel=%q start=%q stop=%q>\n",
		xmltvChannelID(r), formatXMLTVTime(r.StartAt), formatXMLTVTime(r.EndAt))
	title := r.Title
	if !strings.HasPrefix(title, "Next ") {
		// Untimed provider relay labels such as "ESPN2" are announcements,
		// not evidence that this network's current programme is on this slot.
		title = "PPV idle — " + title
	}
	fmt.Fprintf(w, "    <title>%s</title>\n", xmlEscape(title))
	// Do not pass idle rows to emitProgramme: cached enrichment and stale
	// source flags must never turn a guide notice into a movie, episode or
	// live/new airing. The notice ends at the real event's existing boundary.
	desc := "PPV guide notice, not a currently airing event."
	if r.Description != "" {
		desc += " " + r.Description
	}
	fmt.Fprintf(w, "    <desc>%s</desc>\n", xmlEscape(desc))
	// Plex receives an explicit non-first-run notice. Omitting <new/> alone
	// leaves first-run interpretation to the importer and its cached identity.
	fmt.Fprintln(w, "    <previously-shown/>")
	fmt.Fprintln(w, "  </programme>")
}

func xmltvChannelID(r store.ProgramOutputRow) string {
	if r.ChannelEPGID != "" {
		return r.ChannelEPGID
	}
	if r.ChannelID != uuid.Nil {
		return "ch.conductor." + r.ChannelID.String()
	}
	return "ch.conductor.unknown"
}

func emitChannel(w http.ResponseWriter, r store.ProgramOutputRow, id, baseURL string) {
	fmt.Fprintf(w, "  <channel id=%q>\n", id)
	fmt.Fprintf(w, "    <display-name>%s</display-name>\n", xmlEscape(r.ChannelName))
	fmt.Fprintf(w, "    <display-name>%s</display-name>\n", trimZero(r.ChannelNumber))
	if r.ChannelLogoURL != "" {
		logoURL := absolutePosterURL(r.ChannelLogoURL, baseURL)
		fmt.Fprintf(w, "    <icon src=\"%s\"/>\n", xmlEscape(logoURL))
	}
	fmt.Fprintln(w, "  </channel>")
}

func emitProgramme(w http.ResponseWriter, r store.ProgramOutputRow, baseURL string) {
	chID := xmltvChannelID(r)
	fmt.Fprintf(w, "  <programme channel=%q start=%q stop=%q>\n",
		chID, formatXMLTVTime(r.StartAt), formatXMLTVTime(r.EndAt))

	// Title rewrite: dispatcharr's PPV ingest writes generic
	// "<LEAGUE>" titles with sub-title "No Event Scheduled" for slots
	// that have no game booked. The two-line render in Plex's tile
	// reads as a useless "NFL / No Event Scheduled" repeat. Rewrite
	// the title to something the user can scan ("NFL — Off-Season"
	// when no event AND the last and next games are far apart, else
	// "NFL — No Event Scheduled" — same info at title level so Plex's
	// title-only views don't lose the context).
	title, subTitle := rewriteOffSeasonTitle(r.Title, r.SubTitle)
	fmt.Fprintf(w, "    <title>%s</title>\n", xmlEscape(title))
	if subTitle != "" {
		fmt.Fprintf(w, "    <sub-title>%s</sub-title>\n", xmlEscape(subTitle))
	}

	// Description: prefer the enriched overview (TMDb/TVDB) over the
	// source XMLTV desc. Provider descriptions are often truncated to
	// 100 chars; TMDb gives us full plot summaries. One high-volume provider
	// prefixes episode-specific descriptions with "Sxx Exx". Preserve those:
	// replacing them with the series overview discards the only episode
	// synopsis and makes Plex collapse distinct episodes into one show item.
	desc := r.Description
	if r.EnrichmentOverview != "" {
		xmltvNS, onscreen := ParseEpisodeNumFromDescription(r.Description)
		descriptionMatchesEpisode := xmltvNS != "" && onscreen != ""
		if r.EpisodeNumXMLTV != "" && r.EpisodeNumXMLTV != xmltvNS {
			descriptionMatchesEpisode = false
		}
		if r.EpisodeNumOnscreen != "" && !strings.EqualFold(r.EpisodeNumOnscreen, onscreen) {
			descriptionMatchesEpisode = false
		}
		if !descriptionMatchesEpisode {
			desc = r.EnrichmentOverview
		}
	}
	if desc != "" {
		fmt.Fprintf(w, "    <desc>%s</desc>\n", xmlEscape(desc))
	}

	if validOriginalAirDate(r.OriginalAirDate, r.AiringCivilDate, r.StartAt) {
		// Movies expect a year; episodes can also benefit from the year
		// as a Plex matching hint.
		fmt.Fprintf(w, "    <date>%s</date>\n", r.OriginalAirDate.Format("20060102"))
	}
	if r.IsMovie {
		fmt.Fprintln(w, `    <category>Movie</category>`)
	} else {
		fmt.Fprintln(w, `    <category>Series</category>`)
	}
	for _, c := range r.Category {
		// Don't emit "Movie" twice.
		if strings.EqualFold(strings.TrimSpace(c), "movie") {
			continue
		}
		fmt.Fprintf(w, "    <category>%s</category>\n", xmlEscape(c))
	}

	// Posters: emit BOTH <image size="3" type="poster">URL</image> AND
	// <icon src="URL"/>. Plex DVR ignores programme-level <icon> entirely
	// (its parser uses Plex's own metadata-catalog match for programme art),
	// but it DOES read the newer XMLTV <image> element introduced by
	// SchedulesDirect/xmltv 1.61+. Tunarr emits both and gets posters in
	// Plex; we mirror that pattern. Backdrop emits as a separate <image>.
	//
	// `size="3"` = poster size 3 (largest). `type="poster"` is the
	// xmltv 1.61 attribute Plex matches on.
	// Provider matches (including operator-selected provider IDs) outrank
	// source art. A researched asset is also stored on the enrichment row,
	// but is deliberately lower precedence than artwork that later arrives
	// from the winning XMLTV feed.
	researchedPosterURL := ""
	posterURL := r.EnrichmentPosterURL
	if isResearchedPosterURL(posterURL) {
		researchedPosterURL = posterURL
		posterURL = ""
	}
	if posterURL == "" {
		posterURL = r.SourcePosterURL
	}
	if posterURL == "" {
		posterURL = researchedPosterURL
	}
	if posterURL == "" {
		posterURL = r.GeneratedPosterURL
	}
	if posterURL == "" {
		posterURL = brandedFallbackPosterURL(r, baseURL)
	}
	posterURL = absolutePosterURL(posterURL, baseURL)
	if posterURL != "" {
		fmt.Fprintf(w, "    <image size=\"3\" type=\"poster\">%s</image>\n", xmlEscape(posterURL))
		fmt.Fprintf(w, "    <icon src=\"%s\"/>\n", xmlEscape(posterURL))
	}
	if r.EnrichmentBackdropURL != "" {
		fmt.Fprintf(w, "    <image size=\"3\" orient=\"L\" type=\"backdrop\">%s</image>\n", xmlEscape(r.EnrichmentBackdropURL))
	}

	if r.EpisodeNumXMLTV != "" {
		fmt.Fprintf(w, `    <episode-num system="xmltv_ns">%s</episode-num>`+"\n", xmlEscape(r.EpisodeNumXMLTV))
	}
	if r.EpisodeNumOnscreen != "" {
		fmt.Fprintf(w, `    <episode-num system="onscreen">%s</episode-num>`+"\n", xmlEscape(r.EpisodeNumOnscreen))
	}
	// IMDb id helps Plex's "EPG match priority" rules nail the right entry
	// in its internal library (per spec §14: IMDb id > TMDb id > title+year).
	if r.EnrichmentIMDbID != "" {
		fmt.Fprintf(w, `    <episode-num system="imdb.com">title/%s</episode-num>`+"\n", xmlEscape(r.EnrichmentIMDbID))
	}
	if r.EnrichmentTMDbID > 0 {
		kind := "tv"
		if r.IsMovie {
			kind = "movie"
		}
		fmt.Fprintf(w, `    <episode-num system="themoviedb.org">%s/%d</episode-num>`+"\n", kind, r.EnrichmentTMDbID)
	}
	if r.Rating != "" {
		fmt.Fprintf(w, `    <rating system="MPAA"><value>%s</value></rating>`+"\n", xmlEscape(r.Rating))
	}
	// <live/> is a non-standard extension retained for XMLTV consumers that
	// understand it. Plex's custom XMLTV provider ignores it, so live-broadcast
	// semantics must not suppress the independent first-run/repeat state.
	if r.IsLive && !r.IsPlaceholder {
		fmt.Fprintln(w, `    <live/>`)
	}

	// Plex treats a recognized episode with no <previously-shown> marker as
	// New. Emit the repeat marker for every non-new airing, including live
	// reruns; otherwise live episode-shaped rows are mislabeled as New.
	switch {
	case r.IsNew:
		fmt.Fprintln(w, `    <new/>`)
	case validOriginalAirDate(r.OriginalAirDate, r.AiringCivilDate, r.StartAt):
		fmt.Fprintf(w, `    <previously-shown start=%q/>`+"\n",
			r.OriginalAirDate.UTC().Format("20060102")+"000000 +0000")
	default:
		fmt.Fprintln(w, `    <previously-shown/>`)
	}
	if r.IsPremiere {
		fmt.Fprintln(w, `    <premiere/>`)
	}
	if r.IsFinale {
		fmt.Fprintln(w, `    <finale/>`)
	}
	fmt.Fprintln(w, `  </programme>`)
}

func validOriginalAirDate(originalAirDate, airingCivilDate *time.Time, startAt time.Time) bool {
	if originalAirDate == nil || originalAirDate.IsZero() {
		return false
	}
	oadYear, oadMonth, oadDay := originalAirDate.Date()
	oadDate := time.Date(oadYear, oadMonth, oadDay, 0, 0, 0, 0, time.UTC)
	if airingCivilDate != nil && !airingCivilDate.IsZero() {
		airingYear, airingMonth, airingDay := airingCivilDate.Date()
		airingDate := time.Date(airingYear, airingMonth, airingDay, 0, 0, 0, 0, time.UTC)
		return !oadDate.After(airingDate)
	}

	// A legacy/manual row has no preserved source offset. An OAD strictly before
	// the UTC start date is safe in every civil timezone; an equal UTC date is
	// ambiguous for negative-offset evening airings and therefore fails closed.
	startYear, startMonth, startDay := startAt.UTC().Date()
	startUTCDate := time.Date(startYear, startMonth, startDay, 0, 0, 0, 0, time.UTC)
	return oadDate.Before(startUTCDate)
}

func absolutePosterURL(posterURL, baseURL string) string {
	if strings.HasPrefix(posterURL, "/") && baseURL != "" {
		return strings.TrimRight(baseURL, "/") + posterURL
	}
	return posterURL
}

func isResearchedPosterURL(posterURL string) bool {
	return strings.HasPrefix(posterURL, "/posters/assets/researched/")
}

// formatXMLTVTime renders to XMLTV's "20060506200000 +0000" form.
// Plex parses this strictly; trailing whitespace breaks it.
func formatXMLTVTime(t time.Time) string {
	return t.UTC().Format("20060102150405") + " +0000"
}

func xmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
	)
	return r.Replace(s)
}

func trimZero(n float64) string {
	return strconv.FormatFloat(n, 'f', -1, 64)
}

// rewriteOffSeasonTitle handles dispatcharr's no-game placeholder rows.
// Source EPG ships these as title="<LEAGUE>", sub-title="No Event
// Scheduled" — readable in the description but invisible if Plex's
// tile is showing only the title. We collapse the two into a single
// title line ("NFL — No Event Scheduled") and clear the subtitle so
// the tile reads cleanly.
//
// Off-season detection is intentionally lightweight: today the
// heuristic is "if the source title is one of our supported league
// keys and the subtitle says no event, it's a placeholder." Future
// versions can pull from the sports cache to detect off-season vs
// in-season-but-no-game-today and label accordingly.
func rewriteOffSeasonTitle(title, subTitle string) (string, string) {
	subTrim := strings.TrimSpace(subTitle)
	if !strings.EqualFold(subTrim, "No Event Scheduled") {
		return title, subTitle
	}
	// Recognised league prefixes — when the title is one of these alone,
	// the row is a dispatcharr off-state placeholder.
	t := strings.TrimSpace(title)
	upper := strings.ToUpper(t)
	switch upper {
	case "NFL", "NBA", "NHL", "MLB", "MLS",
		"NCAAF", "NCAAB",
		"EPL", "PREMIER LEAGUE", "LA LIGA", "BUNDESLIGA", "SERIE A",
		"UFC", "PGA", "F1":
		return upper + " — No Event Scheduled", ""
	}
	return title, subTitle
}

// brandedFallbackPosterURL returns the branded poster URL for a
// programme with no third-party (TMDb/TVDb) match. Rendered + cached on
// demand by /posters/branded/{channel_id} (see internal/poster).
//
// Sports programmes get a CHANNEL-INDEPENDENT card
// (/posters/branded/sport.jpg): Plex merges guide programmes into one
// identity per title, so whichever airing supplies art wins everywhere
// that title appears. With per-channel cards, a game simulcast on ABC
// and a PPV slot showed ABC's logo card on the PPV channel (reported
// 2026-06-10: "ABC 7 Denver logo for sports shows"). A neutral
// sports-tinted card is correct on every channel it propagates to.
//
// Non-sports fallbacks stay channel-branded (those programmes are
// rarely simulcast) and return "" when:
//
// - baseURL is empty (Phase 0 mode);
// - the channel has no logo (a card with NO logo is just a coloured
// square — Plex's blank tile is the smaller visual);
// - channel id is zero.
//
// Category is forwarded as ?cat=<primary> so the renderer picks the
// right tint.
func brandedFallbackPosterURL(r store.ProgramOutputRow, baseURL string) string {
	if baseURL == "" || r.ChannelID == uuid.Nil {
		return ""
	}
	cat := primaryCategory(r)
	if isSportsCategory(cat) {
		// ponytail: this card has to stay logo-less for the reason above,
		// and Render draws no text (the "category name in small ASCII
		// caps" in the package doc was never implemented) — so it can
		// only ever be a flat colour block. Plex renders it as a bare
		// green rectangle. That fails exactly the test applied to the
		// logo-less branded card below: Plex's own tile is the smaller
		// visual. Give Render a caption and this can return a URL again.
		return ""
	}
	if r.ChannelLogoURL == "" {
		return ""
	}
	url := strings.TrimRight(baseURL, "/") + "/posters/branded/" + r.ChannelID.String() + ".jpg"
	if cat != "" {
		url += "?cat=" + xmlURLQueryEscape(cat)
	}
	return url
}

// isSportsCategory: categories whose programmes routinely air on
// multiple channels at once (and therefore must not carry per-channel
// art — see brandedFallbackPosterURL).
func isSportsCategory(cat string) bool {
	c := strings.ToLower(strings.TrimSpace(cat))
	return c == "sports" || c == "sports event" || c == "sports talk" ||
		strings.HasPrefix(c, "sport")
}

// primaryCategory picks the first non-empty category in the EPG row,
// defaulting to "Movie" or "Series" based on the IsMovie flag. The
// poster renderer only uses the first match it gets.
func primaryCategory(r store.ProgramOutputRow) string {
	for _, c := range r.Category {
		c = strings.TrimSpace(c)
		if c != "" {
			return c
		}
	}
	if r.IsMovie {
		return "Movie"
	}
	return "Series"
}

// xmlURLQueryEscape: minimal URL-query encoder that handles spaces +
// XML-unsafe chars (&, <, >, ", '). Avoids pulling net/url just for
// one query param — keeps the package surface small.
func xmlURLQueryEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == ' ':
			b.WriteByte('+')
		case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		case r == '-' || r == '.' || r == '_' || r == '~':
			b.WriteRune(r)
		default:
			// Percent-encode non-ASCII / unsafe.
			for _, by := range []byte(string(r)) {
				fmt.Fprintf(&b, "%%%02X", by)
			}
		}
	}
	return b.String()
}

// _ keeps go test happy if some imports become unused as we refactor.
var _ = context.Background
