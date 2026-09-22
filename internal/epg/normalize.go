// Package epg owns the EPG ingest → normalize → enrich → output pipeline.
//
// Phase 2 scope:
//   - ingest.go    fetch XMLTV (with ETag/If-Modified-Since), parse, dedupe
//   - normalize.go strip HTML, NFC, episode-num parsing, movie + live + new detection
//   - output.go    emit Plex-correct XMLTV (spec §7) at /xmltv.xml
//   - worker.go    background goroutine running ingest on a schedule
//
// Phase 3 layers TMDb / TVDB enrichment onto epg_program_enrichment.
// Phase 4 layers TheSportsDB + image-gen composites.
package epg

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/spencercnorton/conductor/internal/store"
)

var (
	htmlTagRE      = regexp.MustCompile(`<[^>]+>`)
	wsRE           = regexp.MustCompile(`\s+`)
	yearInTitleRE  = regexp.MustCompile(`\((19|20)\d{2}\)`)
	explicitLiveRE = regexp.MustCompile(
		`(?i)(?:^\s*live(?:\s*$|\s*[:\-]|\s+(?:coverage|broadcast)\b)|[\[(]\s*live\s*[\])])`,
	)
	explicitReplayRE = regexp.MustCompile(
		`(?i)(?:^\s*(?:live\s*[:–—-]\s*)?(?:encore|replay|repeat|rebroadcast|tape[- ]delayed)(?:\s*$|\s*[:–—-])|[\[(]\s*(?:encore|replay|repeat|rebroadcast|tape[- ]delayed)\s*[\])])`,
	)
)

// CleanText runs the full normalization pass on a free-text field:
// 1. Strip HTML tags (some providers wrap descriptions in <p>/<em>).
// 2. Decode HTML entities (&amp;, &nbsp;, &#8217;, etc.).
// 3. NFC unicode normalization.
// 4. Replace fancy quotes / dashes with ASCII equivalents.
// 5. Collapse whitespace.
// 6. Trim.
func CleanText(s string) string {
	if s == "" {
		return ""
	}
	s = htmlTagRE.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	s = norm.NFC.String(s)
	s = replaceFancy(s)
	s = wsRE.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func replaceFancy(s string) string {
	r := strings.NewReplacer(
		"‘", "'", "’", "'",
		"“", `"`, "”", `"`,
		"–", "-", "—", "-",
		"…", "...",
		" ", " ",
	)
	return r.Replace(s)
}

// trailingSuperscriptRE matches a trailing run of "fake superscript"
// characters from Unicode's Phonetic Extensions / Superscripts blocks,
// optionally preceded by space. IPTV providers (iBoost, SLING-style
// resellers) use these as inline branding badges in titles and channel
// names — e.g.
//
//	"Bloomberg Surveillance ᴸᶦᵛᵉ"   → "Bloomberg Surveillance"
//	"Squawk Box ᴴᴰ"                  → "Squawk Box"
//	"NBC Sports ⁶⁰ᶠᵖˢ"              → "NBC Sports"
//	"FOX BUSINESS ᴿᴬᵂ"              → "FOX BUSINESS"
//
// Plex displays the title verbatim, so the badges show up as garbled
// fake-superscript text in the guide. Strip them at normalization time
// so all consumers (XMLTV, lineup.json, indexer search) see clean names.
//
// Character ranges covered:
//   - U+02B0..U+02FF  Spacing Modifier Letters
//   - U+2070..U+209F  Superscripts and Subscripts
//   - U+1D2C..U+1D6A  Phonetic Extensions modifier letters
//   - U+1D9B..U+1DBF  Phonetic Extensions Supplement
//
// The match is anchored at end-of-string and requires a delimiter (or
// start-of-string) before the badge run, so legitimate uses mid-title
// are preserved.
var trailingSuperscriptRE = regexp.MustCompile(
	`(?:\s+|^)[\x{02B0}-\x{02FF}\x{2070}-\x{209F}\x{1D2C}-\x{1D6A}\x{1D9B}-\x{1DBF}]+` +
		`(?:[\s/]+[\x{02B0}-\x{02FF}\x{2070}-\x{209F}\x{1D2C}-\x{1D6A}\x{1D9B}-\x{1DBF}]+)*\s*$`,
)

// StripProviderSuperscriptBadges removes IPTV-provider fake-superscript
// badges from the END of a title or name.
func StripProviderSuperscriptBadges(s string) string {
	out := trailingSuperscriptRE.ReplaceAllString(s, "")
	return strings.TrimSpace(out)
}

// SplitTitleSubTitle handles the "Show Name: Episode Title" form some
// providers ship as the title. Returns (title, sub_title). If the title
// contains no separator the input is returned with empty subtitle.
func SplitTitleSubTitle(raw string) (string, string) {
	for _, sep := range []string{": ", " - ", " — "} {
		if i := strings.Index(raw, sep); i > 0 {
			t := strings.TrimSpace(raw[:i])
			st := strings.TrimSpace(raw[i+len(sep):])
			if t != "" && st != "" {
				return t, st
			}
		}
	}
	return raw, ""
}

// ParseEpisodeNum reads any of the common XMLTV episode-num system values
// and returns (xmltv_ns, onscreen). Both may be empty if neither was
// extractable. Plex prefers onscreen for display, xmltv_ns for matching.
//
// xmltv_ns: "S.E.P" 0-indexed, e.g. "2.6." = season 3 episode 7
// onscreen: "S03E07" 1-indexed, the human-readable form
//
// Many providers ship only one; we interconvert when possible.
type EpisodeNumInput struct {
	System string // "xmltv_ns" | "onscreen" | "" (treat as onscreen if S/E shape)
	Value  string
}

var (
	onscreenRE                 = regexp.MustCompile(`(?i)^s(\d+)e(\d+)`)
	descriptionEpisodePrefixRE = regexp.MustCompile(`(?i)^s(\d+)\s+e(\d+)\b`)
)

func ParseEpisodeNum(inputs []EpisodeNumInput) (xmltvNS, onscreen string) {
	for _, in := range inputs {
		v := strings.TrimSpace(in.Value)
		if v == "" {
			continue
		}
		switch in.System {
		case "xmltv_ns":
			xmltvNS = v
		case "onscreen", "":
			if onscreenRE.MatchString(v) {
				onscreen = strings.ToUpper(v)
			}
		}
	}

	// Interconvert if we only have one form.
	if xmltvNS != "" && onscreen == "" {
		onscreen = xmltvNSToOnscreen(xmltvNS)
	} else if onscreen != "" && xmltvNS == "" {
		xmltvNS = onscreenToXMLTVNS(onscreen)
	}
	return xmltvNS, onscreen
}

// ParseEpisodeNumFromDescription recovers season/episode data from the
// provider form "S20 E02 - episode details..." when formal XMLTV episode-num
// elements are absent. The match is deliberately anchored at the beginning
// and requires whitespace between season and episode so prose containing an
// incidental S/E token is not reclassified.
func ParseEpisodeNumFromDescription(desc string) (xmltvNS, onscreen string) {
	match := descriptionEpisodePrefixRE.FindStringSubmatch(strings.TrimSpace(desc))
	if len(match) != 3 {
		return "", ""
	}
	season := atoi(match[1])
	episode := atoi(match[2])
	if season <= 0 || episode <= 0 {
		return "", ""
	}
	return formatXMLTVNS(season-1, episode-1), formatOnscreen(season, episode)
}

// xmltvNSToOnscreen turns "2.6." → "S03E07".
func xmltvNSToOnscreen(ns string) string {
	parts := strings.Split(ns, ".")
	if len(parts) < 2 {
		return ""
	}
	season, ok1 := parsePart(parts[0])
	ep, ok2 := parsePart(parts[1])
	if !ok1 || !ok2 {
		return ""
	}
	return formatOnscreen(season+1, ep+1)
}

// onscreenToXMLTVNS turns "S03E07" → "2.6.".
func onscreenToXMLTVNS(s string) string {
	m := onscreenRE.FindStringSubmatch(s)
	if len(m) != 3 {
		return ""
	}
	season := atoi(m[1])
	ep := atoi(m[2])
	if season <= 0 || ep <= 0 {
		return ""
	}
	return formatXMLTVNS(season-1, ep-1)
}

func parsePart(p string) (int, bool) {
	p = strings.TrimSpace(p)
	if p == "" {
		return 0, false
	}
	// "0" or "0/2" → take the first number.
	if i := strings.Index(p, "/"); i > 0 {
		p = p[:i]
	}
	n := atoi(p)
	if n < 0 {
		return 0, false
	}
	return n, true
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func formatOnscreen(season, ep int) string {
	return "S" + zeroPad2(season) + "E" + zeroPad2(ep)
}

func formatXMLTVNS(season, ep int) string {
	return itoa(season) + "." + itoa(ep) + "."
}

func zeroPad2(n int) string {
	if n < 10 {
		return "0" + itoa(n)
	}
	return itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(rune('0'+(n%10))) + out
		n /= 10
	}
	return out
}

// IsLikelyMovie applies the spec §6.2 heuristic: category=Movie OR
// runtime > 80min + no episode num OR year-in-title.
func IsLikelyMovie(category []string, runtimeMinutes int, title string, hasEpisodeNum bool) bool {
	for _, c := range category {
		if strings.EqualFold(strings.TrimSpace(c), "movie") {
			return true
		}
	}
	if runtimeMinutes > 80 && !hasEpisodeNum {
		return true
	}
	if !hasEpisodeNum && yearInTitleRE.MatchString(title) {
		return true
	}
	return false
}

// IsLiveBySignal applies conservative live-broadcast detection.
// An explicit upstream flag qualifies unless contradicted by an unambiguous
// replay/idle marker. Without one, only an explicit
// title marker qualifies; category=Sports/News alone is not evidence that an
// airing is live (replays, talk shows, and idle placeholders use those
// categories too).
func IsLiveBySignal(xmltvFlag bool, _ []string, title string, isMovie bool) bool {
	if hasNonLiveBroadcastCue(title, "") {
		return false
	}
	if xmltvFlag {
		return true
	}
	if isMovie {
		return false
	}
	lower := strings.ToLower(title)
	for _, marker := range []string{"encore", "replay", "no game", "no event"} {
		if strings.Contains(lower, marker) {
			return false
		}
	}
	return explicitLiveRE.MatchString(title)
}

// Match explicit annotations rather than words inside ordinary show names:
// "Replay: Final" is negative evidence; "Instant Replay" is not. Movie
// inference alone is insufficient because long sports broadcasts and titles
// containing a year can trigger that heuristic too.
func hasNonLiveBroadcastCue(title, subTitle string) bool {
	title, subTitle = stripVisibleLiveCue(title), stripVisibleLiveCue(subTitle)
	return explicitReplayRE.MatchString(title) || explicitReplayRE.MatchString(subTitle) ||
		isIdleGuidePlaceholder(title, subTitle)
}

// IsNewBySignal evaluates one source row's first-run evidence. Durable
// series+episode history is applied by the store's shared effective view.
// Original air dates are civil dates, so compare calendar days rather than
// elapsed hours across offsets or DST transitions.
func IsNewBySignal(xmltvFlag bool, originalAirDate, startAt time.Time, prevShownExplicit bool) bool {
	if prevShownExplicit {
		return false
	}
	if xmltvFlag {
		return true
	}
	if !originalAirDate.IsZero() {
		startYear, startMonth, startDay := startAt.Date()
		oadYear, oadMonth, oadDay := originalAirDate.Date()
		startDate := time.Date(startYear, startMonth, startDay, 0, 0, 0, 0, time.UTC)
		oadDate := time.Date(oadYear, oadMonth, oadDay, 0, 0, 0, 0, time.UTC)
		days := int(startDate.Sub(oadDate) / (24 * time.Hour))
		return days >= 0 && days <= 7
	}
	return false
}

// SourceCivilDate materializes the calendar date carried by a provider
// timestamp. A later timestamptz roundtrip preserves only the instant, so this
// value must be stored separately when civil-date policy matters.
func SourceCivilDate(value time.Time) time.Time {
	year, month, day := value.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// SourceHash computes a stable hash for one program entry, used by the
// upsert dedupe key. Includes the fields that differentiate one airing
// from another (channel + start + title) plus the source URL so two
// providers describing the same airing produce different rows (we'll
// merge in Phase 3 enrichment).
// ProgramContentHash fingerprints every mutable content field of a program
// row. Identity lives in the (channel_id, start_at) upsert key; this hash is
// purely the change detector — any content difference (title, metadata,
// flags, end time) yields a new hash so the priority-guarded DO UPDATE
// fires, and identical content from two sources hashes identically so rows
// don't churn when feeds overlap. SourcePriority is included so a source
// re-prioritization propagates to existing rows on the next pass.
func ProgramContentHash(p store.EPGProgram) string {
	h := sha256.New()
	field := func(parts ...string) {
		for _, s := range parts {
			h.Write([]byte(s))
			h.Write([]byte{0x1f})
		}
	}
	field(p.ChannelID.String(),
		p.StartAt.UTC().Format(time.RFC3339),
		p.EndAt.UTC().Format(time.RFC3339))
	field(p.Title, p.SubTitle, p.Description)
	field(strconv.Itoa(len(p.Category)))
	field(p.Category...)
	field(p.EpisodeNumXMLTV, p.EpisodeNumOnscreen, p.ProviderEpisodeID,
		p.Rating, p.SourcePosterURL)
	oad := ""
	if p.OriginalAirDate != nil {
		oad = p.OriginalAirDate.UTC().Format("2006-01-02")
	}
	airingCivilDate := ""
	if p.AiringCivilDate != nil && !p.AiringCivilDate.IsZero() {
		year, month, day := p.AiringCivilDate.Date()
		airingCivilDate = time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	}
	field(oad, airingCivilDate,
		fmt.Sprintf("%t|%t|%t|%t|%t|%t|%t", p.IsMovie, p.IsLive, p.IsNew,
			p.NewExplicit, p.PreviouslyShownExplicit, p.IsPremiere, p.IsFinale),
		strconv.Itoa(p.SourcePriority))
	return "xmltv:" + hex.EncodeToString(h.Sum(nil))[:32]
}
