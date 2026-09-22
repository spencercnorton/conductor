package epg

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// XMLTV format reference: http://wiki.xmltv.org/index.php/XMLTVFormat
//
// We parse only the subset Plex cares about + what we use for normalization.
// Provider-specific extensions are tolerated (ignored) — Go's encoding/xml
// drops unknown elements by default.
type xmltvDoc struct {
	XMLName    xml.Name         `xml:"tv"`
	Channels   []xmltvChannel   `xml:"channel"`
	Programmes []xmltvProgramme `xml:"programme"`
}

type xmltvChannel struct {
	ID           string    `xml:"id,attr"`
	DisplayNames []string  `xml:"display-name"`
	Icon         xmltvIcon `xml:"icon"`
}

type xmltvIcon struct {
	Src string `xml:"src,attr"`
}

type xmltvProgramme struct {
	Channel         string            `xml:"channel,attr"`
	Start           string            `xml:"start,attr"`
	Stop            string            `xml:"stop,attr"`
	Title           string            `xml:"title"`
	SubTitle        string            `xml:"sub-title"`
	Desc            string            `xml:"desc"`
	Date            string            `xml:"date"`
	Categories      []string          `xml:"category"`
	EpisodeNums     []xmltvEpisodeNum `xml:"episode-num"`
	Length          xmltvLength       `xml:"length"`
	Live            *struct{}         `xml:"live"`
	New             *struct{}         `xml:"new"`
	Premiere        *struct{}         `xml:"premiere"`
	Finale          *struct{}         `xml:"finale"`
	PreviouslyShown *xmltvPrevShown   `xml:"previously-shown"`
	Rating          xmltvRating       `xml:"rating"`
	Images          []xmltvImage      `xml:"image"`
	Icon            xmltvIcon         `xml:"icon"`
}

type xmltvImage struct {
	Type   string `xml:"type,attr"`
	Orient string `xml:"orient,attr"`
	URL    string `xml:",chardata"`
}

type xmltvEpisodeNum struct {
	System string `xml:"system,attr"`
	Value  string `xml:",chardata"`
}

type xmltvLength struct {
	Units string `xml:"units,attr"`
	Value string `xml:",chardata"`
}

type xmltvPrevShown struct {
	Start string `xml:"start,attr"`
}

type xmltvRating struct {
	System string `xml:"system,attr"`
	Value  string `xml:"value"`
}

// ParseXMLTV reads an XMLTV document from r and returns a parsed doc.
// Caller is responsible for closing r if it's an io.ReadCloser.
func ParseXMLTV(r io.Reader) (*xmltvDoc, error) {
	dec := xml.NewDecoder(r)
	dec.Strict = false // tolerate broken provider XML
	dec.CharsetReader = identityCharsetReader
	var doc xmltvDoc
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("xmltv decode: %w", err)
	}
	return &doc, nil
}

// identityCharsetReader treats unknown encodings as UTF-8 — most providers
// declare iso-8859-1 then ship UTF-8 anyway. Worst case the byte stream is
// passed through and the normalizer's NFC pass cleans it up.
func identityCharsetReader(_ string, r io.Reader) (io.Reader, error) { return r, nil }

// parseXMLTVTime parses XMLTV's awkward timestamp format:
// "20260506200000 -0600" → 2026-05-06T20:00:00-06:00.
// Tolerates timestamps without TZ (treated as local).
func parseXMLTVTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty time")
	}
	for _, layout := range []string{
		"20060102150405 -0700",
		"20060102150405-0700",
		"20060102150405 Z0700",
		"20060102150405",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			// Preserve the provider's fixed offset until normalization has
			// compared calendar dates. PostgreSQL still stores the same instant.
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable time %q", s)
}

// IngestStats tallies the work done in one ingest pass.
type IngestStats struct {
	SourcesAttempted  int
	SourcesOK         int
	SourcesNoChange   int
	SourcesFailed     int
	ProgramsAdded     int
	ProgramsUpdated   int
	ProgramsUnchanged int
}

// Ingest reads from one EPG source, parses, and upserts programs.
// Returns per-source stats. Errors from one source don't stop the others.
func Ingest(ctx context.Context, db *store.DB, src store.EPGSource, hc *http.Client) (added, updated, unchanged int, status string, etag, lastMod string, err error) {
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return 0, 0, 0, "error: " + err.Error(), src.LastETag, src.LastModified, err
	}
	if src.LastETag != "" {
		req.Header.Set("If-None-Match", src.LastETag)
	}
	if src.LastModified != "" {
		req.Header.Set("If-Modified-Since", src.LastModified)
	}
	if src.AuthHeader != "" {
		// Format: "Authorization: Basic ..." — split on first colon.
		if i := strings.Index(src.AuthHeader, ":"); i > 0 {
			req.Header.Set(strings.TrimSpace(src.AuthHeader[:i]),
				strings.TrimSpace(src.AuthHeader[i+1:]))
		}
	}

	resp, err := hc.Do(req)
	if err != nil {
		return 0, 0, 0, "error: " + err.Error(), src.LastETag, src.LastModified, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		if (src.LastETag == "" && src.LastModified == "") || src.SnapshotGeneration == 0 {
			err := errors.New("upstream returned 304 to an unconditional EPG snapshot request")
			return 0, 0, 0, "error: " + err.Error(), src.LastETag, src.LastModified, err
		}
		if err := db.ValidateEPGSourceSnapshot(ctx, src); err != nil {
			return 0, 0, 0, "error: " + err.Error(), src.LastETag, src.LastModified, err
		}
		return 0, 0, 0, "no_change", src.LastETag, src.LastModified, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		msg := fmt.Sprintf("error: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		return 0, 0, 0, msg, src.LastETag, src.LastModified, fmt.Errorf("status %d", resp.StatusCode)
	}

	doc, err := ParseXMLTV(resp.Body)
	if err != nil {
		return 0, 0, 0, "error: " + err.Error(), src.LastETag, src.LastModified, err
	}

	programs, mappings, skipped, err := normalizePrograms(ctx, db, src, doc)
	if err != nil {
		// Never persist validators for a representation that did not publish.
		// Otherwise the retry may receive 304 forever while the prior snapshot
		// remains installed.
		return 0, 0, 0, "error: " + err.Error(), src.LastETag, src.LastModified, err
	}
	result, err := db.ReplaceEPGSourceSnapshot(
		ctx, src, programs, mappings,
		resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"),
	)
	if err != nil {
		return 0, 0, 0, "error: " + err.Error(), src.LastETag, src.LastModified, err
	}
	added, updated, unchanged = result.Added, result.Updated, result.Unchanged
	status = "ok"
	if summary := summarizeSkips(skipped); summary != "" {
		status = "ok (" + summary + ")"
	}
	return added, updated, unchanged, status,
		resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), nil
}

// summarizeSkips renders the per-reason malformed-row counts as a stable,
// compact status suffix, e.g. "skipped 3 malformed: empty_channel_id=1,
// non_positive_interval=2". Empty when nothing was skipped.
func summarizeSkips(skipped map[string]int) string {
	if len(skipped) == 0 {
		return ""
	}
	total := 0
	reasons := make([]string, 0, len(skipped))
	for r, n := range skipped {
		total += n
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	parts := make([]string, 0, len(reasons))
	for _, r := range reasons {
		parts = append(parts, fmt.Sprintf("%s=%d", r, skipped[r]))
	}
	return fmt.Sprintf("skipped %d malformed: %s", total, strings.Join(parts, ", "))
}

// normalizePrograms maps an XMLTV document onto conductor channels.
//
// Malformed individual rows — an empty channel-declaration id, a programme
// with a missing/undeclared channel reference, an unparseable or
// non-positive interval, an empty title — are SKIPPED and tallied by reason
// rather than failing the whole feed: real providers ship single junk rows
// (2026-08-29: iptv-epg US dark 41h over one zero-duration programme,
// the iBoost XMLTV dark over one empty channel id), and one bad row must
// not blank every channel the source covers.
//
// The fail-closed property is preserved by the terminal guard: when
// skipping left NOTHING valid but malformed rows existed, the representation
// is rejected so a garbage feed can never replace a good prior snapshot
// with an authoritative empty one. Infrastructure errors (DB lookups) and
// local mapping conflicts (two provider ids claiming one conductor channel)
// stay hard errors — those need operator action, not tolerance.
func normalizePrograms(ctx context.Context, db *store.DB, src store.EPGSource, doc *xmltvDoc) ([]store.EPGProgram, []store.EPGSnapshotMapping, map[string]int, error) {
	skipped := map[string]int{}
	skip := func(reason string) { skipped[reason]++ }

	// Build channel id → conductor channel map up-front. Programs whose
	// channel id doesn't map to a Conductor channel are silently dropped.
	chanMap := make(map[string]store.Channel)
	declaredChannels := make(map[string]struct{}, len(doc.Channels))
	mappingSet := make(map[string]store.EPGSnapshotMapping)
	claimedChannels := make(map[uuid.UUID]string)
	for _, c := range doc.Channels {
		if strings.TrimSpace(c.ID) == "" {
			skip("empty_channel_id")
			continue
		}
		declaredChannels[c.ID] = struct{}{}
		ch, lerr := db.LookupChannelByEPGID(ctx, c.ID)
		if errors.Is(lerr, store.ErrNotFound) {
			continue
		}
		if lerr != nil {
			return nil, nil, nil, fmt.Errorf("map XMLTV channel %q: %w", c.ID, lerr)
		}
		if claimedProviderID, claimed := claimedChannels[ch.ID]; claimed && claimedProviderID != c.ID {
			return nil, nil, nil, fmt.Errorf("XMLTV provider channels %q and %q map to the same Conductor channel %s",
				claimedProviderID, c.ID, ch.ID)
		}
		claimedChannels[ch.ID] = c.ID
		chanMap[c.ID] = ch
		mappingSet[c.ID] = store.EPGSnapshotMapping{ProviderChannelID: c.ID, ChannelID: ch.ID}
	}

	// A provider occasionally repeats an exact source slot. The database keeps
	// one candidate per source/start, so retain the interval with the greatest
	// coverage and use content hash only as the stable tie-breaker. Hash-first
	// selection could discard a multi-hour event for a malformed short duplicate
	// before the channel resolver ever saw it.
	type slotKey struct {
		channelID uuid.UUID
		startUnix int64
	}
	bySlot := make(map[slotKey]store.EPGProgram)
	for _, p := range doc.Programmes {
		if strings.TrimSpace(p.Channel) == "" {
			skip("empty_channel_ref")
			continue
		}
		if _, declared := declaredChannels[p.Channel]; !declared {
			skip("undeclared_channel_ref")
			continue
		}
		ch, ok := chanMap[p.Channel]
		if !ok {
			continue
		}

		start, perr := parseXMLTVTime(p.Start)
		if perr != nil {
			skip("unparseable_start")
			continue
		}
		stop, perr := parseXMLTVTime(p.Stop)
		if perr != nil {
			skip("unparseable_stop")
			continue
		}
		if !stop.After(start) {
			skip("non_positive_interval")
			continue
		}

		title := StripProviderSuperscriptBadges(CleanText(p.Title))
		subTitle := StripProviderSuperscriptBadges(CleanText(p.SubTitle))
		desc := CleanText(p.Desc)
		if title == "" {
			skip("empty_title")
			continue
		}

		// If sub-title is empty but title contains "Show: Episode", split.
		if subTitle == "" {
			if t, st := SplitTitleSubTitle(title); st != "" {
				title, subTitle = t, st
			}
		}

		var epInputs []EpisodeNumInput
		for _, e := range p.EpisodeNums {
			epInputs = append(epInputs, EpisodeNumInput{System: e.System, Value: e.Value})
		}
		xmltvNS, onscreen := ParseEpisodeNum(epInputs)
		providerEpisodeID := xmltvProviderEpisodeID(p.EpisodeNums)
		if xmltvNS == "" && onscreen == "" {
			xmltvNS, onscreen = ParseEpisodeNumFromDescription(desc)
		}

		runtime := 0
		if strings.EqualFold(p.Length.Units, "minutes") {
			runtime = atoi(strings.TrimSpace(p.Length.Value))
		}

		isMovie := IsLikelyMovie(p.Categories, runtime, title, xmltvNS != "" || onscreen != "")
		isLive := IsLiveBySignal(p.Live != nil, p.Categories, title, isMovie) &&
			!hasNonLiveBroadcastCue(title, subTitle)

		var oad *time.Time
		if p.Date != "" {
			if t, err := time.Parse("20060102", strings.TrimSpace(p.Date)); err == nil {
				oad = &t
			} else if t, err := time.Parse("2006", strings.TrimSpace(p.Date)); err == nil {
				oad = &t
			}
		}

		newExplicit := p.New != nil
		previouslyShownExplicit := p.PreviouslyShown != nil
		isNew := IsNewBySignal(newExplicit, deref(oad), start, previouslyShownExplicit)
		airingCivilDate := SourceCivilDate(start)

		prog := store.EPGProgram{
			ChannelID:               ch.ID,
			StartAt:                 start,
			EndAt:                   stop,
			Title:                   title,
			SubTitle:                subTitle,
			Description:             desc,
			Category:                cleanCategories(p.Categories),
			EpisodeNumXMLTV:         xmltvNS,
			EpisodeNumOnscreen:      onscreen,
			OriginalAirDate:         oad,
			AiringCivilDate:         &airingCivilDate,
			IsMovie:                 isMovie,
			IsLive:                  isLive,
			IsNew:                   isNew,
			IsPremiere:              p.Premiere != nil,
			IsFinale:                p.Finale != nil,
			Rating:                  CleanText(p.Rating.Value),
			SourcePosterURL:         programmePosterURL(p),
			SourcePriority:          src.Priority,
			ProviderEpisodeID:       providerEpisodeID,
			NewExplicit:             newExplicit,
			PreviouslyShownExplicit: previouslyShownExplicit,
		}
		prog.SourceHash = ProgramContentHash(prog)
		key := slotKey{channelID: ch.ID, startUnix: start.UnixNano()}
		if prior, exists := bySlot[key]; !exists || prog.EndAt.After(prior.EndAt) ||
			(prog.EndAt.Equal(prior.EndAt) && prog.SourceHash < prior.SourceHash) {
			bySlot[key] = prog
		}
	}

	// Fail-closed guard: if the feed contained malformed rows and
	// skipping them left nothing valid, this is a garbage representation,
	// not an authoritative empty snapshot — reject it so the prior good
	// snapshot (and its validators) stay installed.
	if len(bySlot) == 0 && len(skipped) > 0 {
		return nil, nil, nil, fmt.Errorf(
			"no valid mapped XMLTV programmes survived (%s)", summarizeSkips(skipped))
	}

	programs := make([]store.EPGProgram, 0, len(bySlot))
	for _, p := range bySlot {
		programs = append(programs, p)
	}
	sort.Slice(programs, func(i, j int) bool {
		if programs[i].ChannelID == programs[j].ChannelID {
			if programs[i].StartAt.Equal(programs[j].StartAt) {
				return programs[i].SourceHash < programs[j].SourceHash
			}
			return programs[i].StartAt.Before(programs[j].StartAt)
		}
		return programs[i].ChannelID.String() < programs[j].ChannelID.String()
	})
	mappings := make([]store.EPGSnapshotMapping, 0, len(mappingSet))
	for _, mapping := range mappingSet {
		mappings = append(mappings, mapping)
	}
	sort.Slice(mappings, func(i, j int) bool {
		if mappings[i].ProviderChannelID == mappings[j].ProviderChannelID {
			return mappings[i].ChannelID.String() < mappings[j].ChannelID.String()
		}
		return mappings[i].ProviderChannelID < mappings[j].ProviderChannelID
	})
	return programs, mappings, skipped, nil
}

// xmltvProviderEpisodeID retains only episode-scoped provider identities.
// Series (SH) and movie (MV) IDs are not episode identities and must never
// collapse different airings into one New/repeat history bucket.
func xmltvProviderEpisodeID(nums []xmltvEpisodeNum) string {
	for _, num := range nums {
		system := strings.ToLower(strings.TrimSpace(num.System))
		if system != "dd_progid" && system != "schedulesdirect.org" && system != "tms" {
			continue
		}
		value := strings.ToUpper(strings.TrimSpace(num.Value))
		if strings.HasPrefix(value, "EP") && len(value) > 2 {
			valid := true
			for _, r := range value[2:] {
				if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
					valid = false
					break
				}
			}
			if valid {
				return value
			}
		}
	}
	return ""
}

// programmePosterURL keeps artwork already present in the source XMLTV.
// Prefer an explicitly-labelled poster, then a non-landscape image, then the
// legacy programme <icon>.  Only HTTP(S) URLs are allowed onto the public
// XMLTV surface.
func programmePosterURL(p xmltvProgramme) string {
	for _, image := range p.Images {
		if strings.EqualFold(strings.TrimSpace(image.Type), "poster") {
			if u := httpImageURL(image.URL); u != "" {
				return u
			}
		}
	}
	for _, image := range p.Images {
		orient := strings.ToLower(strings.TrimSpace(image.Orient))
		if orient == "l" || orient == "landscape" {
			continue
		}
		if u := httpImageURL(image.URL); u != "" {
			return u
		}
	}
	return httpImageURL(p.Icon.Src)
}

func httpImageURL(raw string) string {
	u := strings.TrimSpace(raw)
	lower := strings.ToLower(u)
	if strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "http://") {
		return u
	}
	return ""
}

func cleanCategories(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, c := range in {
		c = CleanText(c)
		if c == "" {
			continue
		}
		k := strings.ToLower(c)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, c)
	}
	return out
}

func deref(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// helper to keep test file colocated with parser internals.
var _ = strconv.Itoa
