// Package dvr implements DVR-as-Indexer: Conductor exposes a Torznab
// indexer that searches the EPG for upcoming live-TV airings, and when an
// *arr "grabs" a result, schedules a recording during the program window
// that lands as a finished file in *arr's import folder.
//
// This is intentionally NOT in the original spec (§1 marked DVR out of
// scope, deferring to Plex DVR). Added 2026-05-06 at the user's request:
// "for any true live tv ... we want to find a way to leverage the EPG as a
// sort of indexer for prowlarr so we can auto schedule dvr recordings of
// live tv episodes for something we don't already have and can't find
// online".
//
// Architecture overview (also in docs/enhancements.md §10):
//
//   1. Operator adds Conductor as a Torznab indexer in Prowlarr at LOW
//      priority. Sonarr/Radarr only fall through to Conductor when no
//      higher-priority indexer has the file — gives the user's
//      "can't find online" condition for free.
//   2. Sonarr searches → /indexer/torznab/api?t=tvsearch&q=…
//   3. Conductor returns matching upcoming EPG programs as Torznab <item>s.
//   4. Sonarr "grabs" a result → /dvr/schedule.torrent?ec=<signed>
//   5. Conductor inserts a dvr_recording row (state=scheduled) and returns a
//      tiny placeholder .torrent.
//   6. Background scheduler picks up the recording at start_time-30s,
//      opens the channel via stream.Pool.ServeWriter, captures bytes for
//      the program window, writes to <CONDUCTOR_DVR_OUTPUT_DIR>/Show/...
//   7. *arr's library import / completed-download-handling picks up the
//      finished file.
//
// Auth: indexer endpoints are gated by CONDUCTOR_DVR_INDEXER_KEY (passed
// as `apikey=…` query param, the Torznab convention). Distinct from the
// admin Bearer key — Prowlarr stores indexer keys in plaintext, so this
// key must NOT also grant admin access.
package dvr

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// Torznab is the HTTP surface mounted at /indexer/torznab/api.
type Torznab struct {
	DB         *store.DB
	BaseURL    string // e.g. http://192.0.2.12:8409
	APIKey     string // shared with Prowlarr
	SignKey    []byte // HMAC key for signed download URLs
	IndexerName string
}

func New(db *store.DB, baseURL, apiKey, name string) *Torznab {
	// Sign key derived from the APIKey + a static label so it's stable
	// across restarts but not the same as the bearer-style apikey.
	h := sha256.New()
	h.Write([]byte("conductor-dvr-sign-v1|"))
	h.Write([]byte(apiKey))
	if name == "" {
		name = "Conductor DVR"
	}
	return &Torznab{
		DB:          db,
		BaseURL:     strings.TrimRight(baseURL, "/"),
		APIKey:      apiKey,
		SignKey:     h.Sum(nil),
		IndexerName: name,
	}
}

// ServeHTTP dispatches based on the `t=` query param.
func (t *Torznab) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	if t.APIKey == "" || q.Get("apikey") != t.APIKey {
		http.Error(w, `<?xml version="1.0"?><error code="100" description="Invalid API Key"/>`,
			http.StatusUnauthorized)
		return
	}

	switch q.Get("t") {
	case "caps":
		t.caps(w)
	case "search", "tvsearch", "tv-search":
		t.search(w, r, "tvsearch")
	case "movie":
		t.search(w, r, "movie")
	default:
		t.caps(w)
	}
}

// ─────────────────────── caps ───────────────────────

// caps emits the Torznab capabilities XML Prowlarr scrapes at indexer-add time.
// The category numbers follow the Newznab convention so Prowlarr can map
// them to Sonarr/Radarr expectations.
func (t *Torznab) caps(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	body := `<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server title="Conductor DVR Indexer"/>
  <limits max="200" default="50"/>
  <retention days="90"/>
  <searching>
    <search available="yes" supportedParams="q"/>
    <tv-search available="yes" supportedParams="q,season,ep,tvdbid,imdbid"/>
    <movie-search available="yes" supportedParams="q,year,imdbid,tmdbid"/>
    <music-search available="no"/>
    <book-search available="no"/>
    <audio-search available="no"/>
  </searching>
  <categories>
    <category id="2000" name="Movies">
      <subcat id="2010" name="Movies/Foreign"/>
      <subcat id="2020" name="Movies/Other"/>
      <subcat id="2030" name="Movies/SD"/>
      <subcat id="2040" name="Movies/HD"/>
    </category>
    <category id="5000" name="TV">
      <subcat id="5030" name="TV/SD"/>
      <subcat id="5040" name="TV/HD"/>
      <subcat id="5060" name="TV/Sport"/>
      <subcat id="5080" name="TV/News"/>
    </category>
  </categories>
</caps>`
	_, _ = w.Write([]byte(body))
}

// ─────────────────────── search ───────────────────────

func (t *Torznab) search(w http.ResponseWriter, r *http.Request, kind string) {
	q := r.URL.Query()
	// Radarr/Sonarr drive movie/tvsearch by external id (imdbid/tmdbid/
	// tvdbid), often with no free-text q at all. Parsing those is what
	// keeps an id-only search from falling through to an unfiltered dump.
	params := store.EPGIndexerSearch{
		Query:     strings.TrimSpace(q.Get("q")),
		Season:    atoi(q.Get("season")),
		Episode:   atoi(q.Get("ep")),
		Year:      atoi(q.Get("year")),
		IMDbID:    strings.TrimSpace(q.Get("imdbid")),
		TMDbID:    atoi(q.Get("tmdbid")),
		TVDBID:    atoi(q.Get("tvdbid")),
		MovieOnly: kind == "movie",
		Limit:     200,
	}

	hits, err := t.DB.SearchEPGForIndexer(r.Context(), params)
	if err != nil {
		http.Error(w, `<?xml version="1.0"?><error code="500" description="`+xmlEscape(err.Error())+`"/>`,
			http.StatusInternalServerError)
		return
	}

	// Audit the search (non-fatal if it fails).
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	_ = t.DB.LogIndexerSearch(r.Context(),
		net.ParseIP(host), r.Header.Get("User-Agent"),
		params.Query, kind, params.Season, params.Episode, params.Year, len(hits))

	t.writeRSS(w, kind, hits)
}

// ─────────────────────── output ───────────────────────

type torznabRSS struct {
	XMLName xml.Name `xml:"rss"`
	Version string   `xml:"version,attr"`
	NSAtom  string   `xml:"xmlns:atom,attr"`
	NSTorz  string   `xml:"xmlns:torznab,attr"`
	Channel torznabChannel `xml:"channel"`
}

type torznabChannel struct {
	Title       string         `xml:"title"`
	Description string         `xml:"description"`
	Link        string         `xml:"link"`
	Language    string         `xml:"language"`
	Items       []torznabItem  `xml:"item"`
}

type torznabItem struct {
	Title       string         `xml:"title"`
	GUID        torznabGUID    `xml:"guid"`
	Type        string         `xml:"type"`
	PubDate     string         `xml:"pubDate"`
	Category    []int          `xml:"category"`
	Link        string         `xml:"link"`
	Comments    string         `xml:"comments,omitempty"`
	Description string         `xml:"description,omitempty"`
	Enclosure   torznabEnclosure `xml:"enclosure"`
	Attrs       []torznabAttr  `xml:"torznab:attr"`
}

type torznabGUID struct {
	IsPermaLink string `xml:"isPermaLink,attr"`
	Value       string `xml:",chardata"`
}

type torznabEnclosure struct {
	URL    string `xml:"url,attr"`
	Length string `xml:"length,attr"`
	Type   string `xml:"type,attr"`
}

type torznabAttr struct {
	XMLName xml.Name `xml:"torznab:attr"`
	Name    string   `xml:"name,attr"`
	Value   string   `xml:"value,attr"`
}

func (t *Torznab) writeRSS(w http.ResponseWriter, kind string, hits []store.EPGSearchHit) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")

	rss := torznabRSS{
		Version: "2.0",
		NSAtom:  "http://www.w3.org/2005/Atom",
		NSTorz:  "http://torznab.com/schemas/2015/feed",
		Channel: torznabChannel{
			Title:       t.IndexerName,
			Description: "Live-TV EPG via Conductor — schedules a DVR recording when grabbed",
			Link:        t.BaseURL,
			Language:    "en-US",
		},
	}

	for _, h := range hits {
		rss.Channel.Items = append(rss.Channel.Items, t.itemFromHit(h))
	}

	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(rss)
}

func (t *Torznab) itemFromHit(h store.EPGSearchHit) torznabItem {
	displayTitle := buildReleaseTitle(h)
	scheduleURL := t.signedScheduleURL(h)

	// Placeholder size so *arr quality heuristics have something plausible.
	// A future DVR recording's real size is unknown until the recorder
	// runs; estimate it from the program duration at a conservative rate.
	const indexerEstimateBitsPerSec = 1_500_000 // 1.5 Mb/s
	durationSec := int64(h.EndAt.Sub(h.StartAt).Seconds())
	bytesGuess := durationSec * indexerEstimateBitsPerSec / 8

	cats := []int{}
	switch {
	case h.IsMovie:
		cats = append(cats, 2000, 2040)
	case looksSports(h):
		cats = append(cats, 5000, 5060)
	case looksNews(h):
		cats = append(cats, 5000, 5080)
	default:
		cats = append(cats, 5000, 5040)
	}

	attrs := []torznabAttr{
		{Name: "size", Value: strconv.FormatInt(bytesGuess, 10)},
		{Name: "category", Value: strconv.Itoa(cats[0])},
	}
	if h.EpisodeNumOnscreen != "" {
		s, e := splitOnscreen(h.EpisodeNumOnscreen)
		if s > 0 {
			attrs = append(attrs, torznabAttr{Name: "season", Value: strconv.Itoa(s)})
		}
		if e > 0 {
			attrs = append(attrs, torznabAttr{Name: "episode", Value: strconv.Itoa(e)})
		}
	}
	attrs = append(attrs,
		torznabAttr{Name: "publishdate", Value: h.StartAt.UTC().Format(time.RFC1123Z)},
		torznabAttr{Name: "channel", Value: h.ChannelName},
		torznabAttr{Name: "channel_number", Value: strconv.FormatFloat(h.ChannelNumber, 'f', -1, 64)},
		torznabAttr{Name: "is_live", Value: strconv.FormatBool(h.IsLive)},
	)

	return torznabItem{
		Title: displayTitle,
		GUID: torznabGUID{
			IsPermaLink: "false",
			Value:       "conductor-" + h.ProgramID.String(),
		},
		Type:        "public",
		PubDate:     h.StartAt.UTC().Format(time.RFC1123Z),
		Category:    cats,
		Link:        scheduleURL,
		Description: h.Description,
		Enclosure: torznabEnclosure{
			URL:    scheduleURL,
			Length: strconv.FormatInt(bytesGuess, 10),
			Type:   "application/x-bittorrent",
		},
		Attrs: attrs,
	}
}

// buildReleaseTitle synthesizes a release name in the *arr-friendly form so
// Sonarr/Radarr's parsers recognize season+episode+year. Examples:
//   "The Bear S03E07 Conductor-DVR FX 1080p"
//   "Heat 1995 Conductor-DVR AMC 1080p"
//   "Denver Broncos at Kansas City Chiefs Conductor-DVR ESPN 1080p"
func buildReleaseTitle(h store.EPGSearchHit) string {
	parts := []string{cleanForTitle(h.Title)}
	if h.EpisodeNumOnscreen != "" {
		parts = append(parts, h.EpisodeNumOnscreen)
	}
	if h.SubTitle != "" {
		parts = append(parts, cleanForTitle(h.SubTitle))
	}
	parts = append(parts,
		"Conductor-DVR",
		strings.ReplaceAll(cleanForTitle(h.ChannelName), " ", "."),
		"1080p",
	)
	return strings.Join(parts, " ")
}

// cleanForTitle strips characters that confuse *arr release parsers.
func cleanForTitle(s string) string {
	s = strings.ReplaceAll(s, "/", " ")
	s = strings.ReplaceAll(s, ":", " ")
	s = strings.ReplaceAll(s, "?", "")
	s = strings.Join(strings.Fields(s), " ")
	return s
}

// signedScheduleURL builds the /dvr/schedule.torrent URL with an HMAC-signed
// payload encoding the program id + window. Prevents unauthenticated
// scheduling and makes the URL safe to log.
func (t *Torznab) signedScheduleURL(h store.EPGSearchHit) string {
	payload := strings.Join([]string{
		h.ProgramID.String(),
		h.ChannelID.String(),
		h.StartAt.UTC().Format(time.RFC3339),
		h.EndAt.UTC().Format(time.RFC3339),
	}, "|")

	sig := hmacSign(t.SignKey, payload)
	enc := base64.RawURLEncoding.EncodeToString([]byte(payload))

	v := url.Values{}
	v.Set("ec", enc)
	v.Set("sig", sig)
	v.Set("apikey", t.APIKey)
	return t.BaseURL + "/dvr/schedule.torrent?" + v.Encode()
}

// SignedScheduleURLForTest exposes signedScheduleURL to in-tree tests without
// widening the public surface. Don't call from non-test code.
func (t *Torznab) SignedScheduleURLForTest(h store.EPGSearchHit) string {
	return t.signedScheduleURL(h)
}

// VerifyScheduleURL is the inverse of signedScheduleURL — used by the
// /dvr/schedule.torrent handler to validate the request.
func (t *Torznab) VerifyScheduleURL(ec, sig string) (programID, channelID uuid.UUID, start, end time.Time, err error) {
	raw, derr := base64.RawURLEncoding.DecodeString(ec)
	if derr != nil {
		err = fmt.Errorf("decode ec: %w", derr)
		return
	}
	payload := string(raw)
	if hmacSign(t.SignKey, payload) != sig {
		err = fmt.Errorf("signature mismatch")
		return
	}
	parts := strings.SplitN(payload, "|", 4)
	if len(parts) != 4 {
		err = fmt.Errorf("unexpected payload shape")
		return
	}
	programID, err = uuid.Parse(parts[0])
	if err != nil {
		return
	}
	channelID, err = uuid.Parse(parts[1])
	if err != nil {
		return
	}
	start, err = time.Parse(time.RFC3339, parts[2])
	if err != nil {
		return
	}
	end, err = time.Parse(time.RFC3339, parts[3])
	return
}

// ─────────────────────── helpers ───────────────────────

func hmacSign(key []byte, payload string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))[:32]
}

func atoi(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

func splitOnscreen(s string) (season, ep int) {
	// "S03E07" → 3, 7
	s = strings.ToUpper(s)
	if !strings.HasPrefix(s, "S") {
		return 0, 0
	}
	rest := s[1:]
	i := strings.Index(rest, "E")
	if i < 0 {
		return 0, 0
	}
	season = atoi(rest[:i])
	ep = atoi(rest[i+1:])
	return
}

func looksSports(h store.EPGSearchHit) bool {
	return h.IsLive
}

func looksNews(h store.EPGSearchHit) bool {
	t := strings.ToLower(h.Title)
	return strings.Contains(t, "news")
}

func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}

// ensure io is used (referenced through the package's other files later).
var _ = io.Copy
