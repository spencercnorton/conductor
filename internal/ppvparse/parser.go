// Package ppvparse extracts game information from formatted IPTV
// stream names. The provider iboost (and similar) dynamically updates
// each PPV stream's name in the M3U as games are scheduled — the
// stream URL stays the same but the name encodes status, event,
// kickoff time, broadcaster, and slot identifier. Examples:
//
//	LIVE | CHAMPIONSHIP SOUTHAMPTON - BLACKBURN | Tue 14 Apr 20:55 CEST (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9
//	Live | Atlético Madrid vs. Barcelona | UEFA Champions League | 8K EXCLUSIVE | CA: DAZN PPV 5
//	End | Good Morning Football | NFL Game Pass | 2026-04-14 | 12:00 (GMT) | 8K EXCLUSIVE | BE: DAZN PPV 3
//	ENDED | RIO AMERICANO VS. OAKMONT | Tue 14 Apr 06:15 EDT (US) | 8K EXCLUSIVE | US: NFHS PPV 4
//	MLS LIVE 19: Dallas vs. St. Louis start:2025-07-20 01:25:00 stop:2025-07-20 04:07:00
//
// We're not trying to be exhaustive — only the formats we've seen
// emit useful data for Plex's guide. Names that don't match any
// pattern return ok=false and the caller skips them.
package ppvparse

import (
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// ParsedStream is the normalised output of one stream-name parse.
type ParsedStream struct {
	Status      Status        // live / upcoming / ended / unknown
	Title       string        // event/match name (e.g. "Southampton - Blackburn")
	StartAt     time.Time     // UTC kickoff, zero when unparseable
	Duration    time.Duration // explicit when source provided start+stop, otherwise default 3h
	Slot        string        // "DK: VIAPLAY PPV 9", "DAZN PPV 5", etc.
	Sport       string        // best-effort inferred from slot/title (e.g. "soccer" / "nfl"); empty if unknown
	Broadcaster string        // network from the slot prefix (e.g. "VIAPLAY", "DAZN")
	CountryCode string        // e.g. "DK", "US", "CA" — empty if not present
	RawName     string        // original input — useful for log/debug + dedup keys
	Confidence  float64       // 0..1 — how sure we are this parse is correct

	// OffIdle is true only for StatusOff parses where the slot is GENUINELY
	// idle (empty / TBA / TBD / "NO EVENT"), vs an ambiguous off — a
	// truncated event name (iboost length-caps names, dropping the trailing
	// "(…ET)" / "start:…" clause) or an untimed venue label. ppvsync sweeps
	// a slot's stored event only when the provider clearly says nothing is
	// on (OffIdle), so a still-upcoming event isn't clobbered by a truncated
	// name (2026-07-20).
	OffIdle bool

	// EndExplicit is true only when the provider itself declared the event
	// ended (for example an "ENDED | ..." pipe prefix). A StatusEnded derived
	// solely from an unchanged name's parsed wall-clock window leaves this
	// false: ppvsync may then preserve an already-active overrun row by exact
	// identity. This distinction prevents an explicit provider end-state from
	// being resurrected merely because an earlier pass extended its end_at.
	EndExplicit bool
}

// isIdleOffTail reports whether an off-state's trailing text means the slot
// is genuinely idle (nothing scheduled), vs carrying substantive content we
// must not read as "empty" — a truncated event title or a venue label.
func isIdleOffTail(s string) bool {
	switch strings.ToLower(strings.TrimSpace(strings.Trim(strings.TrimSpace(s), "-–—"))) {
	case "", "tba", "tbd", "no event", "none", "n/a":
		return true
	}
	return false
}

// Status represents the stream's airing state at parse time.
type Status string

const (
	StatusUnknown  Status = ""
	StatusLive     Status = "live"     // currently airing
	StatusUpcoming Status = "upcoming" // future broadcast
	StatusEnded    Status = "ended"    // already aired
	StatusOff      Status = "off"      // recognised PPV slot with no event scheduled
)

// Parse turns a raw stream name into a ParsedStream. Returns
// (zero, false) for unrecognised formats.
func Parse(name string) (ParsedStream, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return ParsedStream{}, false
	}
	// Try formats in priority order. iboost-shorthand is checked
	// FIRST because it's the most common pattern in our deployment
	// (~285 channel sources point at iboost) and it has the
	// distinctive "LEAGUE | SLOT - " shape.
	if p, ok := parseIboostShorthand(name); ok {
		return p, true
	}
	if p, ok := parsePipeFormat(name); ok {
		return p, true
	}
	if p, ok := parseSlotStartStop(name); ok {
		return p, true
	}
	// Off-state of the colon slot format ("UFC 00 :" with no event/start:).
	// Must come AFTER parseSlotStartStop so a live "UFC 00 : … start:…" still
	// parses as a real event.
	if p, ok := parseColonSlotOff(name); ok {
		return p, true
	}
	// Country-prefixed persistent PPV slot ("UK: EPL 1 PPV …").
	if p, ok := parseCountryPPVSlot(name); ok {
		return p, true
	}
	// Generic iboost event slots ("PPV EVENT NN: …", "LIVE EVENT NN - …").
	if p, ok := parsePPVEvent(name); ok {
		return p, true
	}
	if p, ok := parseLiveEvent(name); ok {
		return p, true
	}
	// Off-states of the generic slots — idle ("PPV EVENT 53:",
	// "LIVE EVENT 03 - NO EVENT") or untimed tails. Must come AFTER the
	// event parsers so a scheduled event never degrades to a placeholder.
	if p, ok := parsePPVEventOff(name); ok {
		return p, true
	}
	if p, ok := parseLiveEventOff(name); ok {
		return p, true
	}
	// Big Ten Network Plus PPV bank ("(US) (BTN+ 001) | … (UTC ts)").
	if p, ok := parseBTNPlus(name); ok {
		return p, true
	}
	// 8K "NO EVENT STREAMING" placeholder bound to its trailing slot.
	if p, ok := parseNoEventStreaming(name); ok {
		return p, true
	}
	if p, ok := parseFLSPFormat(name); ok {
		return p, true
	}
	return ParsedStream{}, false
}

// parseNoEventStreaming recognises iboost's 8K empty-state placeholder:
//
//	"- NO EVENT STREAMING - | 8K EXCLUSIVE | US: ESPN+ PPV 51"
//
// The event body is empty; we bind it to a StatusOff slot derived from the
// trailing "CC: BRAND PPV N" segment so the ~6,443 8K-exclusive channels keep
// guide presence once mapped. Sport is left empty (the slot is generic), so
// the placeholder title falls back to the slot label.
var noEventStreamingRE = regexp.MustCompile(`(?i)^\s*-?\s*NO EVENT STREAMING\b`)

func parseNoEventStreaming(name string) (ParsedStream, bool) {
	if !noEventStreamingRE.MatchString(strings.TrimSpace(name)) {
		return ParsedStream{}, false
	}
	parts := splitPipes(name)
	if len(parts) == 0 {
		return ParsedStream{}, false
	}
	slot, country, broadcaster, ok := parseSlot(strings.TrimSpace(parts[len(parts)-1]))
	if !ok {
		return ParsedStream{}, false
	}
	return ParsedStream{
		Status:      StatusOff,
		Slot:        slot,
		CountryCode: country,
		Broadcaster: broadcaster,
		OffIdle:     true,
		RawName:     name,
		Confidence:  0.85,
	}, true
}

// ppvLeagues is the set of league tokens we treat as recognised PPV slots.
// Keep in sync with the alternations in the format regexes.
var ppvLeagues = map[string]bool{
	"nfl": true, "nba": true, "nhl": true, "mlb": true, "mls": true,
	"ncaaf": true, "ncaab": true, "ufc": true, "epl": true, "f1": true,
	"pga": true, "box": true, "boxing": true, "wwe": true, "aew": true,
}

// parseColonSlotOff handles the OFF-STATE of iboost's colon slot format —
// the shape parseSlotStartStop covers when an event is scheduled:
//
//	UFC 00 :                       (idle — no fight scheduled)
//	UFC 03 : TBA                   (announced, no time yet)
//	BOXING 1 :
//
// With no "start:" timestamp we can't place an event, so we emit a
// recognised StatusOff slot and let OffSlotPrograms tile "No Event
// Scheduled" placeholders. Without this the nine iboost UFC slots (and any
// boxing/wwe colon slots) vanish from Plex's guide whenever no card is live
// — i.e. most of the time. Slot label matches parseSlotStartStop's
// ("UFC 00") so event rows and placeholders dedupe on the same channel.
var colonSlotOffRE = regexp.MustCompile(
	`(?i)^((?:NFL|NBA|NHL|MLB|MLS|NCAAF|NCAAB|UFC|BOX|BOXING|WWE|AEW|EPL)(?:\s+LIVE)?\s*\d{1,3})\s*:\s*(.*)$`,
)

func parseColonSlotOff(name string) (ParsedStream, bool) {
	m := colonSlotOffRE.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	// A "start:" tail is a real event — defer to parseSlotStartStop (tried
	// earlier; if we're here it failed, so don't masquerade as off).
	tail := strings.TrimSpace(m[2])
	if strings.Contains(strings.ToLower(tail), "start:") {
		return ParsedStream{}, false
	}
	slot := strings.ToUpper(strings.Join(strings.Fields(m[1]), " "))
	league := strings.ToLower(strings.Fields(slot)[0])
	if league == "box" {
		league = "boxing"
	}

	// The colon slots also carry announced fixtures with a time-of-day but
	// no "start:" timestamp, e.g. "EPL 01: 19:30 AFC Bournemouth vs Man City"
	// or "Boxing 1 : FURY vs HALL 6PM". Recover those as real events instead
	// of collapsing them to a placeholder. Tails default to ET like the
	// shorthand format, except EPL (kickoffs are UK wall-clock).
	title := cleanTitle(tail)
	if mo, d, hh, mm, evTitle, ok := parseColonSlotTail(tail); ok {
		loc := etLocation()
		if league == "epl" {
			if l, err := time.LoadLocation("Europe/London"); err == nil {
				loc = l
			}
		}
		var start time.Time
		var validWallClock bool
		if mo != 0 {
			start, validWallClock = inferYearlessDate(time.Month(mo), d, hh, mm, loc, time.Now())
		} else {
			start, validWallClock = kickoffTodayUTC(hh, mm, loc)
		}
		if !validWallClock {
			return ParsedStream{}, false
		}
		dur := defaultDuration(evTitle)
		return ParsedStream{
			Status:     statusForWindow(start, dur),
			Title:      evTitle,
			StartAt:    start,
			Duration:   dur,
			Slot:       slot,
			Sport:      league,
			RawName:    name,
			Confidence: 0.8,
		}, true
	}

	// No parseable time — recognised off-slot. Keep the announced title (if
	// any) on the struct so it isn't silently dropped; never invent a kickoff.
	return ParsedStream{
		Status:      StatusOff,
		Title:       title,
		Slot:        slot,
		Sport:       league,
		Broadcaster: strings.ToUpper(league) + " PPV",
		OffIdle:     isIdleOffTail(m[2]),
		RawName:     name,
		Confidence:  0.9,
	}, true
}

// parseColonSlotTail pulls an hour/minute + event title out of a colon-slot
// tail. Three shapes appear in the live catalogue, none of which the
// shorthand timeAndEventRE covers on its own:
//
//	"7pm Canadiens @ Sabres"           am/pm time first  (timeAndEventRE)
//	"19:30 AFC Bournemouth vs Man City" 24h time first    (colonTail24hRE)
//	"FURY vs HALL 6PM"                  am/pm time last   (colonTailPMLastRE)
//
// Returns ok=false when no time token is present (the caller keeps StatusOff).
var (
	colonTail24hRE    = regexp.MustCompile(`^\s*(\d{1,2}):(\d{2})\s+(.+?)\s*$`)
	colonTailPMLastRE = regexp.MustCompile(`(?i)^\s*(.+?)\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*$`)
)

func parseColonSlotTail(tail string) (month, day, hour, minute int, title string, ok bool) {
	if mo, d, hh, mm, event, tok := timedTailFrom(tail); tok {
		title = cleanTitle(event)
		return mo, d, hh, mm, title, title != ""
	}
	if tm := colonTail24hRE.FindStringSubmatch(tail); tm != nil {
		hour, _ = strconv.Atoi(tm[1])
		minute, _ = strconv.Atoi(tm[2])
		if hour > 23 || minute > 59 {
			return 0, 0, 0, 0, "", false
		}
		title = cleanTitle(tm[3])
		return 0, 0, hour, minute, title, title != ""
	}
	if tm := colonTailPMLastRE.FindStringSubmatch(tail); tm != nil {
		hour, minute, ok = hourMinuteFrom(tm[2], tm[3], tm[4])
		if !ok {
			return 0, 0, 0, 0, "", false
		}
		title = cleanTitle(tm[1])
		return 0, 0, hour, minute, title, title != ""
	}
	return 0, 0, 0, 0, "", false
}

// timedTailFrom extracts the optional M/D date, the am/pm time-of-day, and
// the event text from a "[M/D ]H(:MM)(am|pm) <event>" tail. month is 0 when
// the tail carries no date token; a PRESENT date token with an impossible
// month or day refuses the whole tail rather than degrading to undated —
// month 0 would otherwise collide with the no-date sentinel and silently
// anchor "0/5 7pm X" to today. Sole owner of timeAndEventRE's group
// layout — every consumer goes through here.
func timedTailFrom(tail string) (month, day, hour, minute int, event string, ok bool) {
	tm := timeAndEventRE.FindStringSubmatch(tail)
	if tm == nil {
		return 0, 0, 0, 0, "", false
	}
	hour, minute, ok = hourMinuteFrom(tm[3], tm[4], tm[5])
	if !ok {
		return 0, 0, 0, 0, "", false
	}
	if tm[1] != "" {
		month, day = atoiSafe(tm[1]), atoiSafe(tm[2])
		if month < 1 || month > 12 || day < 1 || day > 31 {
			return 0, 0, 0, 0, "", false
		}
	}
	return month, day, hour, minute, tm[6], true
}

// parseCountryPPVSlot handles persistent country-prefixed PPV slots whose
// names carry league + slot number but no event/time, e.g.:
//
//	UK: EPL 1 PPV ᵁᴴᴰ ³⁸⁴⁰ᴾ
//	US: NFL 3 PPV
//
// These iboost slots don't rename per event (the trailing glyphs are just a
// UHD/quality badge), so they're always off-state in this shape. Without a
// handler the EPL PPV channels have no guide presence. Recognised only for
// known league tokens so we don't false-positive on arbitrary
// "XX: <word> N PPV" strings.
var countryPPVRE = regexp.MustCompile(
	`(?i)^([A-Z]{2}):\s*([A-Za-z]{2,5})\s+(\d{1,3})\s+PPV\b.*$`,
)

func parseCountryPPVSlot(name string) (ParsedStream, bool) {
	m := countryPPVRE.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	league := strings.ToLower(m[2])
	if !ppvLeagues[league] {
		return ParsedStream{}, false
	}
	return ParsedStream{
		Status:      StatusOff,
		Slot:        strings.ToUpper(m[2]) + " " + m[3],
		Sport:       league,
		CountryCode: strings.ToUpper(m[1]),
		Broadcaster: strings.ToUpper(m[2]) + " PPV",
		OffIdle:     true, // persistent slot shape carries no event
		RawName:     name,
		Confidence:  0.85,
	}, true
}

// parseIboostShorthand handles the format the iboost provider's M3U
// uses for sport-PPV slots:
//
//	NHL  | 01 - 7pm Canadiens @ Sabres
//	NFL  | 02 - 1:25pm Eagles @ Cowboys
//	NHL | 02 - 9:30pm Golden Knights @ Ducks
//	NFL  | 05 - 8/28 6pm Commanders at Ravens   (dated variant, added by the
//	                                             provider ~2026-08; the M/D
//	                                             anchors the kickoff day)
//	NFL  | 01 -                            (off-state, no game scheduled)
//	NHL  |  4K - 7pm Canadiens @ Sabres    (4K bitrate variant; we still
//	                                        produce a parse so the EPG
//	                                        works on the high-bitrate slot)
//
// Time-of-day is in U.S. Eastern Time without explicit TZ — iboost is
// US-centric. We resolve to UTC using America/New_York DST rules at
// PARSE time, anchored to today's date in ET (the stream name only
// reflects the current day's slate). Future kickoffs in the next 12 h
// stay future; kickoffs more than 12 h in the past roll over to the
// next ET day.
//
// On the off-state ("LEAGUE | NN -" with nothing after the dash) we
// return ok=true with StatusOff: the slot is recognised, there is just
// no event on it. ppvsync turns that into "No Event Scheduled"
// placeholder rows so the channel keeps guide presence (the old
// dispatcharr ppv-epg.xml placeholder feed that used to cover this is
// retired).
var iboostShorthandRE = regexp.MustCompile(
	`^(?i)\s*(NFL|NBA|NHL|MLB|NCAAF|NCAAB|UFC|MLS)\s*\|\s*(0?\d+|4K)\s*-\s*(.*)$`,
)

// timeAndEventRE pulls "[M/D ]HH(:MM)?(am|pm) <event>" out of the
// trailing segment. The optional leading M/D date (a provider addition,
// ~2026-08: "8/28 6pm Commanders at Ravens") anchors the kickoff to that
// ET calendar day; without it the kickoff anchors to today with the
// forward-roll rule. <event> is everything after the time-of-day token
// (best-effort; we leave it as-is for the title).
var timeAndEventRE = regexp.MustCompile(
	`^(?i)\s*(?:(\d{1,2})/(\d{1,2})\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s+(.+?)\s*$`,
)

func parseIboostShorthand(name string) (ParsedStream, bool) {
	m := iboostShorthandRE.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	leagueRaw := strings.ToLower(strings.TrimSpace(m[1]))
	slot := strings.ToUpper(strings.TrimSpace(m[2]))
	tail := strings.TrimSpace(m[3])

	if tail == "" || tail == "-" {
		// Off-state: "NHL | 03 -". Slot recognised, no event.
		return ParsedStream{
			Status:      StatusOff,
			Slot:        leagueShorthandSlot(leagueRaw, slot),
			Sport:       leagueRaw,
			Broadcaster: strings.ToUpper(leagueRaw) + " PPV",
			OffIdle:     true, // empty tail — genuinely idle
			RawName:     name,
			Confidence:  0.9,
		}, true
	}

	mon, day, hour, minute, rawEvent, tok := timedTailFrom(tail)
	if !tok {
		// Has a tail but no parseable time-of-day. Could be a partial
		// rename mid-rotation; refuse rather than guess kickoff.
		return ParsedStream{}, false
	}
	event := cleanTitle(rawEvent)
	if event == "" {
		return ParsedStream{}, false
	}

	var startAt time.Time
	var validWallClock bool
	if mon != 0 {
		// Dated variant: the M/D names the kickoff's ET calendar day
		// explicitly, so yesterday's not-yet-renamed slate stays yesterday
		// (and reads ended) instead of forward-rolling into a ghost event.
		startAt, validWallClock = inferYearlessDate(time.Month(mon), day, hour, minute, etLocation(), time.Now())
	} else {
		startAt, validWallClock = iboostKickoffUTC(hour, minute)
	}
	if !validWallClock {
		return ParsedStream{}, false
	}
	dur := defaultDuration(event)

	return ParsedStream{
		Status:      statusForWindow(startAt, dur),
		Title:       event,
		StartAt:     startAt,
		Duration:    dur,
		Slot:        leagueShorthandSlot(leagueRaw, slot),
		Sport:       leagueRaw,
		Broadcaster: strings.ToUpper(leagueRaw) + " PPV",
		RawName:     name,
		Confidence:  0.85,
	}, true
}

// iboostKickoffUTC anchors an ET hour/minute to today's ET date (see
// kickoffTodayUTC for the forward-only day-roll rule).
func iboostKickoffUTC(hour, minute int) (time.Time, bool) {
	return kickoffTodayUTC(hour, minute, etLocation())
}

func atoiSafe(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

// kickoffTodayUTC anchors a wall-clock hour/minute to today's date in loc and
// converts to UTC, using the real clock. See kickoffAnchoredUTC for the rule.
func kickoffTodayUTC(hour, minute int, loc *time.Location) (time.Time, bool) {
	return kickoffAnchoredUTC(time.Now(), hour, minute, loc)
}

// kickoffAnchoredUTC is kickoffTodayUTC with an injectable clock (for
// deterministic tests). The name only lists a time-of-day, and iboost always
// names the UPCOMING game — the stream name flips to the next game once the
// previous one ends. So a kickoff already >12 h in the past belongs to
// TOMORROW: roll FORWARD one day.
//
// There is deliberately NO backward roll. An evening game parsed in the morning
// (e.g. a 9:30pm ET kickoff seen at 08:00 ET, ~13.5 h ahead) must stay TODAY and
// upcoming. A symmetric >12 h backward roll would anchor it to YESTERDAY, where
// statusForWindow reads StatusEnded and ToEPGProgram drops the row entirely —
// tonight's game vanishes from the guide every morning until T-12h. Keeping such
// events as today/upcoming matches iboost's own "only reflects the current day's
// slate" contract. AddDate keeps the wall clock stable across DST-transition
// days where Add(24h) would skew by an hour.
func kickoffAnchoredUTC(now time.Time, hour, minute int, loc *time.Location) (time.Time, bool) {
	n := now.In(loc)
	cand, valid := exactLocalWallTime(n.Year(), n.Month(), n.Day(), hour, minute, loc)
	if !valid {
		return time.Time{}, false
	}
	if n.Sub(cand) > 12*time.Hour {
		nextDay := cand.AddDate(0, 0, 1).In(loc)
		cand, valid = exactLocalWallTime(nextDay.Year(), nextDay.Month(), nextDay.Day(), hour, minute, loc)
		if !valid {
			return time.Time{}, false
		}
	}
	return cand.UTC(), true
}

// defaultDuration is the fallback airing length for formats that carry no
// explicit stop time. Combat-sports cards run long (main card + walkouts +
// post-fight), so ufc / boxing / fight-night / main-event names get 5 h;
// prelims and early prelims are short undercards that keep the 3 h default,
// as does everything else.
func defaultDuration(title string) time.Duration {
	t := strings.ToLower(title)
	if strings.Contains(t, "prelim") {
		return 3 * time.Hour
	}
	if strings.Contains(t, "ufc") || strings.Contains(t, "boxing") ||
		strings.Contains(t, "fight night") || strings.Contains(t, "main event") {
		return 5 * time.Hour
	}
	return 3 * time.Hour
}

// leagueShorthandSlot returns "NHL PPV 01" / "NFL PPV 4K" — the
// canonical Conductor slot label.
func leagueShorthandSlot(league, num string) string {
	return strings.ToUpper(league) + " PPV " + num
}

// etLocation returns America/New_York. tzdata missing: fall back to a
// fixed offset (EDT, the US sport-broadcast TZ March-November) — better
// to be 1h off in winter than to fail the parse entirely.
func etLocation() *time.Location {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		loc = time.FixedZone("EDT", -4*3600)
	}
	return loc
}

// ianaZone loads an IANA location by name, falling back to a fixed offset if
// tzdata is somehow unavailable. cmd/conductor imports time/tzdata so the
// fallback should never fire in the shipped binary.
func ianaZone(name, fallbackAbbrev string, fallbackOffset int) *time.Location {
	if loc, err := time.LoadLocation(name); err == nil {
		return loc
	}
	return time.FixedZone(fallbackAbbrev, fallbackOffset)
}

// hourMinuteFrom validates and converts captured "H", "MM" (may be empty),
// "am"/"pm" strings to a 24h hour+minute. time.Date normalises values such
// as 13:99 PM into a different day; rejecting them here keeps every AM/PM
// parser fail-closed instead of silently scheduling the wrong event.
func hourMinuteFrom(hourStr, minStr, ampm string) (hour, minute int, ok bool) {
	var err error
	hour, err = strconv.Atoi(hourStr)
	if err != nil || hour < 1 || hour > 12 {
		return 0, 0, false
	}
	if minStr != "" {
		minute, err = strconv.Atoi(minStr)
		if err != nil || minute < 0 || minute > 59 {
			return 0, 0, false
		}
	}
	switch {
	case strings.EqualFold(ampm, "pm") && hour != 12:
		hour += 12
	case strings.EqualFold(ampm, "am") && hour == 12:
		hour = 0
	case !strings.EqualFold(ampm, "am") && !strings.EqualFold(ampm, "pm"):
		return 0, 0, false
	}
	return hour, minute, true
}

// exactLocalWallTime constructs a local wall clock only when the requested
// calendar components identify exactly one real instant. time.Date otherwise
// normalises impossible dates and spring-forward gaps, and silently chooses
// one side of a repeated fall-back hour. PPV names provide no UTC offset with
// which to disambiguate that repeated hour, so both cases are rejected rather
// than scheduling a plausible-looking event at the wrong instant.
func exactLocalWallTime(year int, month time.Month, day, hour, minute int, loc *time.Location) (time.Time, bool) {
	return exactLocalWallDateTime(year, month, day, hour, minute, 0, loc)
}

// exactLocalWallDateTime is exactLocalWallTime with second precision. BTN+
// catalogue suffixes include seconds, and their provider boundary must not be
// rounded while converting the Eastern wall clock to UTC.
func exactLocalWallDateTime(year int, month time.Month, day, hour, minute, second int, loc *time.Location) (time.Time, bool) {
	if loc == nil || month < time.January || month > time.December || day < 1 || day > 31 ||
		hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return time.Time{}, false
	}
	candidate := time.Date(year, month, day, hour, minute, second, 0, loc)
	wall := candidate.In(loc)
	if wall.Year() != year || wall.Month() != month || wall.Day() != day ||
		wall.Hour() != hour || wall.Minute() != minute || wall.Second() != second {
		return time.Time{}, false
	}

	// Every currently-supported IANA zone changes by at most one hour; the
	// wider window also covers the half-hour transitions used by some zones.
	for delta := -2 * time.Hour; delta <= 2*time.Hour; delta += 30 * time.Minute {
		if delta == 0 {
			continue
		}
		other := candidate.Add(delta).In(loc)
		if other.Year() == year && other.Month() == month && other.Day() == day &&
			other.Hour() == hour && other.Minute() == minute && other.Second() == second {
			return time.Time{}, false
		}
	}
	return candidate, true
}

// statusForWindow: pre-kickoff is upcoming; airing window (kickoff +
// duration) is live; past is ended.
func statusForWindow(start time.Time, dur time.Duration) Status {
	now := time.Now().UTC()
	switch {
	case now.Before(start):
		return StatusUpcoming
	case now.Before(start.Add(dur)):
		return StatusLive
	default:
		return StatusEnded
	}
}

// inferYearlessDate anchors a month/day wall clock to the nearby schedule
// year while validating the final calendar value. The day is clamped only for
// choosing the year so a legitimate leap-day in the inferred adjacent year is
// accepted; the returned candidate must round-trip every original component.
func inferYearlessDate(month time.Month, day, hour, minute int, loc *time.Location, now time.Time) (time.Time, bool) {
	if month < time.January || month > time.December || day < 1 || day > 31 ||
		hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return time.Time{}, false
	}
	year := now.In(etLocation()).Year()
	safeDay := day
	if safeDay > 28 {
		safeDay = 28
	}
	anchor := time.Date(year, month, safeDay, hour, minute, 0, 0, loc)
	switch {
	case anchor.Before(now.Add(-30 * 24 * time.Hour)):
		year++
	case anchor.After(now.Add(300 * 24 * time.Hour)):
		year--
	}
	candidate, valid := exactLocalWallTime(year, month, day, hour, minute, loc)
	if !valid {
		return time.Time{}, false
	}
	return candidate.UTC(), true
}

// parsePPVEvent handles iboost's generic numbered event slots, found live
// 2026-07-11 when UFC 329 was published on them instead of the permanent
// "UFC NN :" slots (which were left empty):
//
//	PPV EVENT 05: UFC 329 McGregor vs. Holloway 2 (7.11 9:00 PM ET)
//	PPV EVENT 03: PRELIMS UFC 329 (7.11 7:00 PM ET)
//
// The parenthesised suffix is US-style month.day + time-of-day in ET.
// These slots are generic — the same stream carries boxing, wrestling,
// concerts, etc. on other nights — so Sport is inferred from the title,
// never from the slot.
var ppvEventRE = regexp.MustCompile(
	`(?i)^(PPV EVENT\s*\d{1,3})\s*:\s*(.+?)\s*\(\s*(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET\s*\)\s*$`,
)

func parsePPVEvent(name string) (ParsedStream, bool) {
	m := ppvEventRE.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	mon, _ := strconv.Atoi(m[3])
	day, _ := strconv.Atoi(m[4])
	if mon < 1 || mon > 12 || day < 1 || day > 31 {
		return ParsedStream{}, false
	}
	hour, minute, validTime := hourMinuteFrom(m[5], m[6], m[7])
	if !validTime {
		return ParsedStream{}, false
	}
	title := cleanTitle(m[2])
	if title == "" {
		return ParsedStream{}, false
	}
	start, validDate := inferYearlessDate(time.Month(mon), day, hour, minute, etLocation(), time.Now())
	if !validDate {
		return ParsedStream{}, false
	}
	dur := defaultDuration(title)
	return ParsedStream{
		Status:     statusForWindow(start, dur),
		Title:      title,
		StartAt:    start,
		Duration:   dur,
		Slot:       strings.ToUpper(strings.Join(strings.Fields(m[1]), " ")),
		Sport:      inferSport(title, ""),
		RawName:    name,
		Confidence: 0.85,
	}, true
}

// parseLiveEvent handles the dash-delimited sibling of parsePPVEvent —
// generic numbered slots with a time-of-day but no date:
//
//	LIVE EVENT 02 - 9pm UFC 329 McGregor v Holloway II
//
// The day anchor resolves like the league shorthand: today in ET, rolling
// forward when the kickoff is >12h past. Names without a leading
// time-of-day token ("LIVE EVENT 27 - OUTDOOR THEATRE Live From Coachella
// 2026") are venue/idle labels with nothing to schedule — refused, matching
// the must-not-parse fixture in TestParse_StaticChannel_NotPPV. Timed
// non-sport events (a concert with a kickoff) DO parse, with empty Sport.
var liveEventRE = regexp.MustCompile(`(?i)^(LIVE EVENT\s*\d{1,3})\s*-\s*(.+)$`)

func parseLiveEvent(name string) (ParsedStream, bool) {
	m := liveEventRE.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	mon, day, hour, minute, rawEvent, tok := timedTailFrom(m[2])
	if !tok {
		return ParsedStream{}, false
	}
	title := cleanTitle(rawEvent)
	if title == "" {
		return ParsedStream{}, false
	}
	var startAt time.Time
	var validWallClock bool
	if mon != 0 {
		startAt, validWallClock = inferYearlessDate(time.Month(mon), day, hour, minute, etLocation(), time.Now())
	} else {
		startAt, validWallClock = iboostKickoffUTC(hour, minute)
	}
	if !validWallClock {
		return ParsedStream{}, false
	}
	dur := defaultDuration(title)
	return ParsedStream{
		Status:     statusForWindow(startAt, dur),
		Title:      title,
		StartAt:    startAt,
		Duration:   dur,
		Slot:       strings.ToUpper(strings.Join(strings.Fields(m[1]), " ")),
		Sport:      inferSport(title, ""),
		RawName:    name,
		Confidence: 0.85,
	}, true
}

// parsePPVEventOff / parseLiveEventOff handle the OFF-STATES of the generic
// slots, in the same spirit as parseColonSlotOff: since 2026-07-11 the
// PPV EVENT / LIVE EVENT series have Conductor channels mapped, and a
// mapped channel with no guide rows vanishes from Plex. Live catalogue
// shapes (observed 2026-07-11):
//
//	PPV EVENT 53:                                  (idle — bare colon)
//	LIVE EVENT 03 - NO EVENT                       (idle — explicit)
//	LIVE EVENT 33 -                                (idle — empty tail)
//	LIVE EVENT 27 - OUTDOOR THEATRE Live From …    (untimed venue label)
//
// Anything that reaches these after the event parsers failed has no
// schedulable kickoff, so it becomes a StatusOff slot and OffSlotPrograms
// tiles "No Event Scheduled" placeholders. Sport stays empty — the slots
// are generic — so the placeholder title falls back to the slot label.
var (
	// Colon is OPTIONAL: iboost idles most PPV EVENT slots as bare
	// "PPV EVENT 05" (no colon) — live 2026-07-20. The strict colon-only
	// form left those unparsed, so ppvsync skipped the channel and froze
	// its guide on the last event. A real "PPV EVENT 01: … (…ET)" still
	// wins on parsePPVEvent (tried first); only untimed shapes reach here.
	ppvEventOffRE  = regexp.MustCompile(`(?i)^(PPV EVENT\s*\d{1,3})\s*(?::\s*(.*))?$`)
	liveEventOffRE = regexp.MustCompile(`(?i)^(LIVE EVENT\s*\d{1,3})\s*(?:-\s*(.*))?$`)
)

func parsePPVEventOff(name string) (ParsedStream, bool) {
	return genericSlotOff(ppvEventOffRE, name)
}

func parseLiveEventOff(name string) (ParsedStream, bool) {
	return genericSlotOff(liveEventOffRE, name)
}

func genericSlotOff(re *regexp.Regexp, name string) (ParsedStream, bool) {
	m := re.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	tail := ""
	if len(m) > 2 {
		tail = m[2]
	}
	return ParsedStream{
		Status:      StatusOff,
		Slot:        strings.ToUpper(strings.Join(strings.Fields(m[1]), " ")),
		Broadcaster: "PPV",
		// Bare/idle tail ("PPV EVENT 05", "…: TBA", "LIVE EVENT 33 -") is a
		// clean idle; a substantive tail ("…: UFC 330 Main Card", a venue
		// label) is an ambiguous off — keep guide presence but don't let
		// ppvsync sweep a possibly-truncated upcoming event.
		OffIdle:    isIdleOffTail(tail),
		RawName:    name,
		Confidence: 0.9,
	}, true
}

// parseBTNPlus handles iboost's Big Ten Network Plus PPV bank (category
// "BTN+ PPV", ~150 slots). The provider shape carries a trailing
// America/New_York wall-clock timestamp:
//
//	(US) (BTN+ 001) | Baseball: Michigan Wolverines Vs. Washington Huskies_ 20_05_2026_ 21:00 (2026-05-21 04:00:00)
//	(US) (BTN+ 002) |  (2098-12-31 08:00:01)     (idle placeholder)
//
// The timestamp is the provider's stream-availability boundary, normally
// 10-30 minutes before the advertised kickoff. Keep that pre-roll exactly;
// the title's scheduling annotation may be expressed for the home venue and
// is not an authoritative Eastern-time replacement. Idle slots carry an
// empty title and a sentinel year far in the future (2098) — those become
// StatusOff so the channel gets "No Event Scheduled" placeholders instead of
// a bogus 2098 airing. The event title commonly trails a
// "_ DD_MM_YYYY_ HH:MM" scheduling annotation; we strip it. Sport stays empty
// (BTN+ carries every college sport), so the placeholder title falls back to
// the "BIG TEN NNN" slot label.
var (
	btnPlusRE       = regexp.MustCompile(`(?i)^\(([A-Z]{2})\)\s*\(BTN\+\s*(\d{1,3})\)\s*\|\s*(.*?)\s*\((\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\)\s*$`)
	btnTitleCruftRE = regexp.MustCompile(`_\s*\d{1,2}[_ /]\d{1,2}[_ /]\d{2,4}.*$`)
)

func parseBTNPlus(name string) (ParsedStream, bool) {
	m := btnPlusRE.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	start, validWindow := parseBTNPlusEasternWindow(m[4])
	if !validWindow {
		return ParsedStream{}, false
	}
	slot := "BIG TEN " + m[2]
	title := cleanTitle(btnTitleCruftRE.ReplaceAllString(strings.TrimSpace(m[3]), ""))

	// Idle slot: no event title, or the far-future sentinel year iboost
	// parks unused BTN+ streams at. Off-state carries no kickoff.
	if title == "" || start.Year() >= 2090 {
		return ParsedStream{
			Status:      StatusOff,
			Slot:        slot,
			Broadcaster: "BTN+ PPV",
			CountryCode: strings.ToUpper(m[1]),
			OffIdle:     true, // empty title / sentinel year — genuinely idle
			RawName:     name,
			Confidence:  0.9,
		}, true
	}
	return ParsedStream{
		Status:      statusForWindow(start.UTC(), 3*time.Hour),
		Title:       title,
		StartAt:     start.UTC(),
		Duration:    3 * time.Hour,
		Slot:        slot,
		Broadcaster: "BTN+ PPV",
		CountryCode: strings.ToUpper(m[1]),
		RawName:     name,
		Confidence:  0.9,
	}, true
}

// parseBTNPlusEasternWindow converts the provider's unzoned Eastern wall
// clock to UTC. A plain time.Parse would label the wall clock as UTC, which
// exposed BTN+ events four or five hours early. Parsing the fields first and
// constructing the local instant through exactLocalWallDateTime also refuses
// impossible dates, spring-forward gaps, and ambiguous fall-back folds. The
// shipped binary embeds tzdata; if that invariant is ever lost, fail closed
// instead of silently substituting a fixed offset and being wrong in winter.
func parseBTNPlusEasternWindow(raw string) (time.Time, bool) {
	wall, err := time.Parse("2006-01-02 15:04:05", raw)
	if err != nil {
		return time.Time{}, false
	}
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		return time.Time{}, false
	}
	start, exact := exactLocalWallDateTime(
		wall.Year(), wall.Month(), wall.Day(),
		wall.Hour(), wall.Minute(), wall.Second(), loc,
	)
	if !exact {
		return time.Time{}, false
	}
	return start.UTC(), true
}

// parsePipeFormat handles the iboost-style PPV format:
//
//	STATUS | EVENT | [DATE [| TIME]] | [FLAGS] | SLOT
//
// 4-, 5-, and 6-segment variants exist; we identify each segment by
// pattern rather than by position so the parser stays robust to small
// upstream layout changes.
func parsePipeFormat(name string) (ParsedStream, bool) {
	parts := splitPipes(name)
	if len(parts) < 3 {
		return ParsedStream{}, false
	}
	status := classifyStatus(parts[0])
	if status == StatusUnknown {
		return ParsedStream{}, false
	}

	// Slot is the last segment. Validate it looks slot-shaped before
	// committing — otherwise this is probably a different format that
	// happens to start with a status word.
	slotSeg := strings.TrimSpace(parts[len(parts)-1])
	slot, country, broadcaster, slotOK := parseSlot(slotSeg)
	if !slotOK {
		return ParsedStream{}, false
	}

	// Find the date+time among the middle segments. Two layouts:
	// - one segment like "Tue 14 Apr 20:55 CEST (DK)" or "Tue 14 Apr 20:55 EDT"
	// - two adjacent segments: "2026-04-14" | "12:00 (GMT)"
	startAt, dateSeg1, dateSeg2, malformedDate := findDateInPipeSegments(parts)
	if malformedDate {
		// An explicit but impossible date/time is not equivalent to a name with
		// no date. In particular, do not let the low-confidence LIVE fallback
		// below turn "Apr 31" or "25:99" into a current-hour event.
		return ParsedStream{}, false
	}

	// Title is whatever middle segments remain after stripping date /
	// flag noise. Walk parts[1 .. len-2], skip date segments, skip
	// flags-looking segments (e.g. "8K EXCLUSIVE"), join the rest.
	titleParts := []string{}
	for i := 1; i < len(parts)-1; i++ {
		seg := strings.TrimSpace(parts[i])
		if seg == "" {
			continue
		}
		if i == dateSeg1 || i == dateSeg2 {
			continue
		}
		if isFlagSegment(seg) || isJunkSegment(seg) {
			continue
		}
		titleParts = append(titleParts, seg)
	}
	title := cleanTitle(strings.Join(titleParts, " — "))
	if title == "" {
		return ParsedStream{}, false
	}

	// A LIVE event with no parseable date would get StartAt zero and be
	// dropped by ToEPGProgram — the live tile vanishes. Anchor it to the
	// current hour so the slot exists; the time is a guess, so flag it
	// low-confidence. Non-live no-date names stay zero (we can't place them).
	lowConfNoDate := false
	if startAt.IsZero() && status == StatusLive {
		startAt = time.Now().UTC().Truncate(time.Hour)
		lowConfNoDate = true
	}

	confidence := 0.6
	if !startAt.IsZero() && !lowConfNoDate {
		confidence += 0.25
	}
	if status == StatusLive {
		confidence += 0.10
	}
	if lowConfNoDate {
		confidence = 0.4
	}
	if confidence > 1.0 {
		confidence = 1.0
	}

	return ParsedStream{
		Status:      status,
		Title:       title,
		StartAt:     startAt,
		Duration:    defaultDuration(title), // PPV format never carries a stop time
		Slot:        slot,
		Broadcaster: broadcaster,
		CountryCode: country,
		Sport:       inferSport(title, broadcaster),
		RawName:     name,
		Confidence:  confidence,
		EndExplicit: status == StatusEnded,
	}, true
}

// parseSlotStartStop handles league slots that carry explicit UTC
// start/stop timestamps in the name:
//
//	"MLS LIVE 19: Dallas vs. St. Louis start:2025-07-20 01:25:00 stop:2025-07-20 04:07:00"
//	"UFC 02: UFC 328: PRELIMS start:2026-05-10 00:55:00 stop:2026-05-10 03:00:00"
//	"UFC 00 : UWC 58 start:2026-06-08 00:55:00 stop:2026-06-08 06:00:00"
//
// (Originally MLS-only; iboost uses the same shape for its UFC slots —
// found live 2026-06-10 with all nine UFC channels unparsed.) The event
// text itself may contain colons ("UFC 328: PRELIMS"), so the title is
// captured lazily up to the " start:" marker. Both timestamps really are UTC
// — do NOT "correct" this by analogy with parseBTNPlus/parseFLSPFormat. This
// shape is a different upstream convention, verified 2026-08-27:
// "MLB 03 | Astros x Yankees start:2026-08-28 00:05:00" is an 08:05pm ET
// first pitch; read as Eastern it would be a 00:05 start. The explicit stop
// sets the real duration.
//
// The provider caps name length and truncates at the seconds field ("stop:
// 2026-05-10 08:15:" — live example on the UFC 01 slot 2026-06-10), so a
// missing stop falls back to the title-keyed default and that one documented
// trailing-colon form is accepted as minute precision. Any other explicit
// stop clause must be consumed completely and parse exactly; silently treating
// malformed stop intent as "missing" can leave a wrong event active.
//
// The separator after the slot number is ":" for most families but "|" for
// the MLB slate ("MLB 01 | Brewers x Pirates start:… stop:…") — the only
// family that publishes an explicit start AND stop in this shape.
var slotStartStopRE = regexp.MustCompile(
	`(?i)^((?:NFL|NBA|NHL|MLB|MLS|NCAAF|NCAAB|UFC|BOX|BOXING|WWE|AEW|EPL)(?:\s+LIVE)?\s*\d{1,3})\s*[:|]\s*(.+?)\s+start:(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})(?:\s+stop:(.+))?\s*$`,
)

func parseSlotStartStop(name string) (ParsedStream, bool) {
	m := slotStartStopRE.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return ParsedStream{}, false
	}
	start, err1 := time.Parse("2006-01-02 15:04:05", m[3])
	if err1 != nil {
		return ParsedStream{}, false
	}
	title := cleanTitle(m[2])
	dur := defaultDuration(title)
	if m[4] != "" {
		stopText := strings.TrimSpace(m[4])
		// The sole tolerated truncation is a missing seconds value after the
		// provider's final colon. Strip exactly one such colon, then require an
		// exact full- or minute-precision timestamp with no trailing material.
		if strings.HasSuffix(stopText, ":") && strings.Count(stopText, ":") == 2 {
			stopText = strings.TrimSuffix(stopText, ":")
		}
		stop, err2 := time.Parse("2006-01-02 15:04:05", stopText)
		if err2 != nil {
			stop, err2 = time.Parse("2006-01-02 15:04", stopText)
		}
		if err2 != nil || !stop.After(start) {
			// A missing stop clause may use the documented default, but captured
			// stop intent with invalid fields, ordering, or trailing data must not
			// be normalised or silently ignored.
			return ParsedStream{}, false
		}
		dur = stop.Sub(start)
	}
	stop := start.Add(dur)
	status := StatusUpcoming
	if time.Now().UTC().After(start) && time.Now().UTC().Before(stop) {
		status = StatusLive
	} else if time.Now().UTC().After(stop) {
		status = StatusEnded
	}
	slot := strings.ToUpper(strings.Join(strings.Fields(m[1]), " "))
	league := strings.ToLower(strings.Fields(slot)[0])
	if league == "box" {
		league = "boxing"
	}
	return ParsedStream{
		Status:     status,
		Title:      title,
		StartAt:    start.UTC(),
		Duration:   dur,
		Slot:       slot,
		Sport:      league,
		RawName:    name,
		Confidence: 0.95, // explicit start+stop = highest-quality parse
	}, true
}

// parseFLSPFormat handles flolive's college sports streams. The catalogue
// moved off the single "flolive:" prefix to per-sport prefixes
// ("flobaseball:", "flograppling:", "flowrestling:", …), so we accept any
// "flo<sport>:" — this recovers the ~998 streams the literal prefix missed:
//
//	"(FLSP 632) | flolive: 2026 Claremont M_S vs Occidental _ Tennis (Court 6) (2026-04-18 13:04:20)"
//	"(FLSP 701) | flobaseball: 2026 Team A vs Team B (2026-07-11 17:00:00)"
//
// The trailing parenthesised timestamp is NOT UTC — it is a US local wall
// clock, the same shape parseBTNPlus mis-read as UTC and shipped four to five
// hours early. Measured against this provider's catalogue on
// 2026-08-27: every FLSP entry title-matched to its human-readable "Flo
// College" twin ("… @ Aug 27 4:00 PM :Flo College 34") agrees on the wall
// clock, 94 of 94 pairs at +0h, and the day's 131 FLSP start hours peak at
// 19:00 with the mass in 12:00-20:00 — a US evening slate, not a UTC one
// (as UTC that peak is a Thursday 15:00 ET with matches kicking off at
// 08:00 ET).
//
// It is deliberately still parsed as UTC here because NO channel routes to
// this parser today (0 of the 29 stream ids carrying ppv-parse EPG rows are
// FLSP), and the evidence does not distinguish America/New_York from
// per-venue local: this bank spans Mountain, Central and Eastern venues in a
// single slate, so one fixed zone may be wrong for some rows.
//
// BEFORE PROVISIONING ANY FLSP CHANNEL: settle that question, then convert
// via exactLocalWallDateTime like parseBTNPlusEasternWindow does. Shipping
// this as-is puts a four-hour-wrong window in the guide, which reads to a
// viewer as a dead channel — the black screen is the provider's placeholder
// for a slot whose event has not started.
var flspRE = regexp.MustCompile(
	`^\((FLSP\s+\d+)\)\s+\|\s+flo[a-z]+:\s+(.+?)\s+\((\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\)\s*$`,
)

func parseFLSPFormat(name string) (ParsedStream, bool) {
	m := flspRE.FindStringSubmatch(name)
	if m == nil {
		return ParsedStream{}, false
	}
	start, err := time.Parse("2006-01-02 15:04:05", m[3])
	if err != nil {
		return ParsedStream{}, false
	}
	title := cleanTitle(m[2])
	dur := defaultDuration(title)
	status := StatusUpcoming
	if time.Now().UTC().After(start) {
		// FLSP streams don't expose explicit stop times — assume the default
		// duration elapsed = ended. Inside that window, treat as live.
		if time.Now().UTC().Before(start.Add(dur)) {
			status = StatusLive
		} else {
			status = StatusEnded
		}
	}
	return ParsedStream{
		Status:     status,
		Title:      title,
		StartAt:    start.UTC(),
		Duration:   dur,
		Slot:       m[1],
		Sport:      inferSport(m[2], "flolive"),
		RawName:    name,
		Confidence: 0.85,
	}, true
}

// classifyStatus maps "LIVE", "Live", "End", "ENDED", etc. to a
// canonical Status value. Returns StatusUnknown for non-status text.
func classifyStatus(s string) Status {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "live":
		return StatusLive
	case "end", "ended":
		return StatusEnded
	case "upcoming", "soon", "next":
		return StatusUpcoming
	}
	return StatusUnknown
}

// parseSlot parses the trailing "DK: VIAPLAY PPV 9" or "VIAPLAY PPV 9"
// segment. Returns the cleaned slot string + country code (when the
// "XX: " prefix is present) + broadcaster name.
//
// Validation: the segment must contain " PPV " somewhere (case-
// insensitive) — otherwise it's likely a different format. This is
// intentionally narrow so we don't false-positive on every pipe-
// delimited string.
var slotRE = regexp.MustCompile(
	`(?i)^([A-Z]{2}):\s+(.+?)\s+(PPV\s+\d+)\s*$`,
)
var slotNoCountryRE = regexp.MustCompile(
	`(?i)^(.+?)\s+(PPV\s+\d+)\s*$`,
)

func parseSlot(seg string) (slot, country, broadcaster string, ok bool) {
	if m := slotRE.FindStringSubmatch(seg); m != nil {
		return strings.TrimSpace(m[2] + " " + m[3]),
			strings.ToUpper(m[1]),
			strings.TrimSpace(m[2]),
			true
	}
	if m := slotNoCountryRE.FindStringSubmatch(seg); m != nil {
		return strings.TrimSpace(m[1] + " " + m[2]),
			"",
			strings.TrimSpace(m[1]),
			true
	}
	return "", "", "", false
}

// findDateInPipeSegments locates the date / date+time segments. Returns
// (kickoff_utc, idx1, idx2, malformed). idx2 is -1 when the date is in a
// single segment. malformed distinguishes an explicit impossible wall clock
// from a name that genuinely carries no date, so P10 cannot anchor malformed
// input to the current hour.
func findDateInPipeSegments(parts []string) (time.Time, int, int, bool) {
	// Single-segment patterns (most common): "Tue 14 Apr 20:55 CEST (DK)".
	for i := 1; i < len(parts)-1; i++ {
		seg := strings.TrimSpace(parts[i])
		if t, matched, malformed := parseSingleSegmentDate(seg); matched {
			return t, i, -1, malformed
		}
	}
	// Two-segment pattern: "2026-04-14" | "12:00 (GMT)" (or similar).
	for i := 1; i < len(parts)-1; i++ {
		seg1 := strings.TrimSpace(parts[i])
		seg2 := ""
		if i+1 < len(parts)-1 {
			seg2 = strings.TrimSpace(parts[i+1])
		}
		if t, matched, malformed := parseSplitDate(seg1, seg2); matched {
			return t, i, i + 1, malformed
		}
	}
	// Any remaining schedule-shaped segment is explicit scheduling intent. It
	// may use an unsupported combined layout or be malformed/incomplete, but it
	// must not be mistaken for a genuinely date-less LIVE name and anchored to
	// the current hour.
	for i := 1; i < len(parts)-1; i++ {
		seg := strings.TrimSpace(parts[i])
		if splitDateIntentRE.MatchString(seg) || splitDateTimeIntentRE.MatchString(seg) || hasPipeScheduleIntent(seg) {
			return time.Time{}, i, -1, true
		}
	}
	return time.Time{}, -1, -1, false
}

// parseSingleSegmentDate handles segments like "Tue 14 Apr 20:55 CEST (DK)"
// and "Tue 14 Apr 06:15 EDT (US)".
//
// Layout: <DOW> DD MON HH:MM <TZ> [(country)]
//
// The year isn't included in this format. We assume current year, but
// roll over to next-year when the parsed date would land in the past
// by more than 30 days (a heuristic — sports schedules don't reach
// >30d into the past for "upcoming" stream names).
var singleDateRE = regexp.MustCompile(
	`^(?:[A-Za-z]{3}\s+)?(\d{1,2})\s+([A-Za-z]{3})\s+(\d{1,2}):(\d{2})\s+([A-Z]{2,5})(?:\s+\(\w{2}\))?\s*$`,
)
var singleDateIntentRE = regexp.MustCompile(
	`^(?:[A-Za-z]{3}\s+)?\d{1,2}\s+[A-Za-z]{3}\s+\d+:\d+\s+[A-Za-z]{1,10}(?:\s+\(\w{2}\))?\s*$`,
)

func parseSingleSegmentDate(seg string) (time.Time, bool, bool) {
	if !singleDateIntentRE.MatchString(seg) {
		return time.Time{}, false, false
	}
	m := singleDateRE.FindStringSubmatch(seg)
	if m == nil {
		return time.Time{}, true, true
	}
	day, dayErr := strconv.Atoi(m[1])
	mon := monthFromAbbrev(m[2])
	hour, hourErr := strconv.Atoi(m[3])
	minute, minuteErr := strconv.Atoi(m[4])
	if dayErr != nil || hourErr != nil || minuteErr != nil || mon == 0 ||
		day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return time.Time{}, true, true
	}
	now := time.Now()
	// Validate the calendar before timezone lookup so "31 Apr ... XYZ" cannot
	// bypass calendar validation and reach the current-hour LIVE fallback.
	if _, valid := inferYearlessDate(mon, day, hour, minute, time.UTC, now); !valid {
		return time.Time{}, true, true
	}
	tz := loadKnownTZ(m[5])
	if tz == nil {
		// The stream supplied an explicit clock, but its zone is unknown. It is
		// not equivalent to a genuinely date-less LIVE name and must not reach
		// the current-hour fallback.
		return time.Time{}, true, true
	}
	candidate, valid := inferYearlessDate(mon, day, hour, minute, tz, now)
	if !valid {
		return time.Time{}, true, true
	}
	return candidate, true, false
}

// parseSplitDate handles "2026-04-14" + "12:00 (GMT)" in adjacent pipe
// segments. The date half is ISO (YYYY-MM-DD) or the European DD-MM-YYYY the
// provider also emits ("11-07-2026").
var splitDateDateRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`) // YYYY-MM-DD
var splitDateDMYRE = regexp.MustCompile(`^\d{2}-\d{2}-\d{4}$`)  // DD-MM-YYYY
var splitDateIntentRE = regexp.MustCompile(`^(?:\d{4}-\d{1,2}-\d{1,2}|\d{1,2}-\d{1,2}-\d{4})$`)
var splitDateTimeRE = regexp.MustCompile(`^(\d{1,2}):(\d{2})\s*\(([A-Z]{2,5})\)\s*$`)
var splitDateTimeIntentRE = regexp.MustCompile(`^\d+:\d+\s*(?:\([A-Za-z]{1,10}\)|[A-Za-z]{1,10})\s*$`)

// The LIVE-without-date fallback is deliberately low-confidence, but it must
// only apply when the provider genuinely supplied no scheduling information.
// These expressions recognise scheduling *intent*, not valid syntax. They are
// intentionally broader than the accepted parsers so unsupported combined
// timestamps, weekday punctuation, malformed ranges, AM/PM clocks, and IANA or
// numeric zones fail closed instead of becoming a current-hour event.
var (
	pipeNumericDateIntentRE = regexp.MustCompile(
		`(?i)(?:^|[^0-9A-Za-z])(?:(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\s*,?\s+)?\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}(?:\s|t|,|$)`,
	)
	pipeDayMonthIntentRE = regexp.MustCompile(
		`(?i)(?:^|[^0-9A-Za-z])(?:(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\s*,?\s+)?\d{1,2}(?:st|nd|rd|th)?(?:\s+|-)(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:[\s,-]+\d{2,4})?(?:\s|,|$)`,
	)
	pipeMonthDayIntentRE = regexp.MustCompile(
		`(?i)(?:^|[^0-9A-Za-z])(?:(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\s*,?\s+)?(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s+|-)\d{1,2}(?:st|nd|rd|th)?(?:[\s,-]+\d{2,4})?(?:\s|,|$)`,
	)
	pipeClockTokenIntentRE = regexp.MustCompile(`(?i)(?:^|\s|[T,(])\d{1,3}[:.]\d{1,3}(?::\d{1,3})?(?:\s*(?:am|pm))?(?:\s|$|[Z),+-])`)
	pipeClockFieldIntentRE = regexp.MustCompile(`(?i)^\s*(?:at\s+)?\d{1,3}[:.]\d{1,3}(?::\d{1,3})?(?:\s*(?:am|pm))?(?:\s+\S+)?\s*$`)
	pipeAMPMIntentRE       = regexp.MustCompile(`(?i)(?:^|\s|[,(])\d{1,2}(?::\d{1,3})?\s*(?:am|pm)\b`)
	pipeCalendarWordRE     = regexp.MustCompile(`(?i)(?:^|\s|[,(])(?:mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)(?:\s|$|[,):])|(?:^|\s|[,(])(?:today|tomorrow|tonight)(?:\s|$|[,):])`)
	pipeScheduleCueRE      = regexp.MustCompile(`(?i)(?:^|\s|[,(])(?:at|kickoff|starts?|begins?|scheduled?|time)\s*[:=-]?\s*\d{1,3}[:.]\d{1,3}`)
	pipeZoneIntentRE       = regexp.MustCompile(
		`(?i)(?:^|\s|\()(?:UTC|GMT|ET|EST|EDT|PT|PST|PDT|CT|CST|CDT|MT|MST|MDT|BST|CET|CEST|EET|EEST|WET|WEST|(?:Africa|America|Antarctica|Arctic|Asia|Atlantic|Australia|Europe|Indian|Pacific|Etc|US|Canada)/[A-Za-z0-9_+./-]+|UTC[+-]\d{1,2}(?::?\d{2})?|[+-]\d{2}:?\d{2})(?:\s|\)|$)`,
	)
)

func hasPipeScheduleIntent(seg string) bool {
	seg = strings.TrimSpace(seg)
	if seg == "" {
		return false
	}
	if pipeNumericDateIntentRE.MatchString(seg) ||
		pipeDayMonthIntentRE.MatchString(seg) ||
		pipeMonthDayIntentRE.MatchString(seg) ||
		pipeAMPMIntentRE.MatchString(seg) ||
		pipeZoneIntentRE.MatchString(seg) {
		return true
	}
	// A clock token on its own is enough when the whole field is clock-like.
	// Inside a title it needs a timezone marker; this avoids treating ordinary
	// score text such as "Team A 2:1 Team B" as a malformed schedule.
	if pipeClockTokenIntentRE.MatchString(seg) {
		return pipeClockFieldIntentRE.MatchString(seg) ||
			pipeCalendarWordRE.MatchString(seg) ||
			pipeScheduleCueRE.MatchString(seg)
	}
	return false
}

// parseDateSegment parses a bare date segment in either accepted layout.
func parseDateSegment(seg string) (time.Time, bool) {
	if splitDateDateRE.MatchString(seg) {
		d, err := time.Parse("2006-01-02", seg)
		return d, err == nil
	}
	if splitDateDMYRE.MatchString(seg) {
		d, err := time.Parse("02-01-2006", seg)
		return d, err == nil
	}
	return time.Time{}, false
}

func parseSplitDate(seg1, seg2 string) (time.Time, bool, bool) {
	if !splitDateIntentRE.MatchString(seg1) {
		return time.Time{}, false, false
	}
	dateShaped := splitDateDateRE.MatchString(seg1) || splitDateDMYRE.MatchString(seg1)
	if !dateShaped {
		return time.Time{}, true, true
	}
	d, validDate := parseDateSegment(seg1)
	if !validDate {
		return time.Time{}, true, true
	}
	tm := splitDateTimeRE.FindStringSubmatch(seg2)
	if tm == nil {
		// Once an explicit date segment is present, a missing or malformed
		// adjacent clock is invalid input rather than a date-less LIVE name.
		return time.Time{}, true, true
	}
	hour, hourErr := strconv.Atoi(tm[1])
	minute, minuteErr := strconv.Atoi(tm[2])
	if hourErr != nil || minuteErr != nil || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return time.Time{}, true, true
	}
	tz := loadKnownTZ(tm[3])
	if tz == nil {
		return time.Time{}, true, true
	}
	cand, exact := exactLocalWallTime(d.Year(), d.Month(), d.Day(), hour, minute, tz)
	if !exact {
		return time.Time{}, true, true
	}
	return cand.UTC(), true, false
}

// monthFromAbbrev: 3-letter month → time.Month. Case-insensitive.
func monthFromAbbrev(s string) time.Month {
	switch strings.ToLower(s) {
	case "jan":
		return time.January
	case "feb":
		return time.February
	case "mar":
		return time.March
	case "apr":
		return time.April
	case "may":
		return time.May
	case "jun":
		return time.June
	case "jul":
		return time.July
	case "aug":
		return time.August
	case "sep", "sept":
		return time.September
	case "oct":
		return time.October
	case "nov":
		return time.November
	case "dec":
		return time.December
	}
	return 0
}

// loadKnownTZ maps common 2-5 letter timezone abbreviations to
// *time.Location. Ambiguous abbreviations (e.g. "BST" = British Summer
// or Bangladesh Standard) get the most common interpretation; we err
// toward US/EU sports broadcast conventions because that's what these
// streams cover.
//
// For abbreviations that map to a fixed UTC offset (most non-DST
// zones) we hand back time.FixedZone — cheaper than tzdata loads.
func loadKnownTZ(abbrev string) *time.Location {
	switch strings.ToUpper(abbrev) {
	case "UTC", "GMT", "Z":
		// GMT is UTC by definition; a "GMT" label from these US-centric
		// providers means UTC, so we do NOT fold it into Europe/London —
		// that would add BST's +1 in summer and mis-time "12:00 GMT".
		return time.UTC
	// DST-pair abbreviations map to their IANA zone rather than a fixed
	// offset, so a seasonally-wrong printed abbrev ("EDT" in January) still
	// resolves to the correct wall-clock instant for the date being parsed.
	case "EST", "EDT", "ET":
		return ianaZone("America/New_York", "EST", -5*3600)
	case "PST", "PDT", "PT":
		return ianaZone("America/Los_Angeles", "PST", -8*3600)
	case "CST":
		return time.FixedZone("CST", -6*3600) // ambiguous (US Central / China / Cuba) — left fixed
	case "CDT":
		return time.FixedZone("CDT", -5*3600)
	case "MST":
		return time.FixedZone("MST", -7*3600)
	case "MDT":
		return time.FixedZone("MDT", -6*3600)
	case "AKST":
		return time.FixedZone("AKST", -9*3600)
	case "AKDT":
		return time.FixedZone("AKDT", -8*3600)
	case "HST":
		return time.FixedZone("HST", -10*3600)
	case "AST":
		return time.FixedZone("AST", -4*3600) // Atlantic
	case "ADT":
		return time.FixedZone("ADT", -3*3600)
	case "NST":
		return time.FixedZone("NST", -3*3600-1800)
	case "NDT":
		return time.FixedZone("NDT", -2*3600-1800)
	case "BST":
		return ianaZone("Europe/London", "BST", 1*3600)
	case "WET":
		return time.FixedZone("WET", 0)
	case "WEST":
		return time.FixedZone("WEST", 1*3600)
	case "CET", "CEST":
		return ianaZone("Europe/Paris", "CET", 1*3600)
	case "EET":
		return time.FixedZone("EET", 2*3600)
	case "EEST":
		return time.FixedZone("EEST", 3*3600)
	case "MSK":
		return time.FixedZone("MSK", 3*3600)
	case "JST":
		return time.FixedZone("JST", 9*3600)
	case "AEST":
		return time.FixedZone("AEST", 10*3600)
	case "AEDT":
		return time.FixedZone("AEDT", 11*3600)
	case "AWST":
		return time.FixedZone("AWST", 8*3600)
	case "ACST":
		return time.FixedZone("ACST", 9*3600+1800)
	}
	return nil
}

// isFlagSegment recognises promotional / quality flags that should be
// stripped from the title. Examples: "8K EXCLUSIVE", "4K", "RAW", "HD".
var flagSegmentRE = regexp.MustCompile(`^(?i)\s*(\d+K(\s+EXCLUSIVE)?|EXCLUSIVE|HD|FHD|UHD|RAW|VIP|FREE)\s*$`)

func isFlagSegment(s string) bool {
	return flagSegmentRE.MatchString(s)
}

// junkSegmentRE matches language/region noise the provider sometimes wedges
// between the event and the slot ("… | all | …") which otherwise leaks into
// the title.
var junkSegmentRE = regexp.MustCompile(`^(?i)all$`)

// isJunkSegment reports whether a pipe segment is throwaway noise: the literal
// "all", or a lone token with no letters or digits (stray flag glyphs /
// decorative separators). Legitimate content (matchups, "NFL Game Pass",
// country codes) always has a letter or digit and is kept.
func isJunkSegment(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return true
	}
	if junkSegmentRE.MatchString(s) {
		return true
	}
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// splitPipes splits on " | " (with surrounding spaces) — the iboost
// convention. Empty trailing segments are dropped.
func splitPipes(s string) []string {
	parts := strings.Split(s, "|")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// cleanTitle normalises whitespace and strips common decorative chars.
func cleanTitle(s string) string {
	s = strings.TrimSpace(s)
	// Collapse runs of whitespace.
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	// Strip leading/trailing punctuation we don't want.
	s = strings.Trim(s, " -|·•")
	return s
}

// inferSport applies very rough keyword matching to guess which league
// this stream covers. Used only to populate the Sport hint; the
// caller can override with explicit per-channel sport mappings (see
// internal/sports). Returns "" when nothing matches.
func inferSport(title, broadcaster string) string {
	hay := strings.ToLower(title + " " + broadcaster)
	switch {
	case strings.Contains(hay, "nfl") || strings.Contains(hay, "espn+"):
		return "nfl"
	case strings.Contains(hay, "nba"):
		return "nba"
	case strings.Contains(hay, "mlb"):
		return "mlb"
	case strings.Contains(hay, "nhl") || strings.Contains(hay, "hockey"):
		return "nhl"
	case strings.Contains(hay, "champions league") || strings.Contains(hay, "uefa"):
		return "epl" // closest enum slot for UEFA competitions
	case strings.Contains(hay, "championship") || strings.Contains(hay, "premier league"):
		return "epl"
	case strings.Contains(hay, "tennis") || strings.Contains(hay, "wta") || strings.Contains(hay, "atp"):
		return "tennis"
	case strings.Contains(hay, "lacrosse"):
		return "ncaaf" // close-enough catch-all for NCAA-tier sports
	case strings.Contains(hay, "ufc") || strings.Contains(hay, "fight"):
		return "ufc"
	}
	return ""
}
