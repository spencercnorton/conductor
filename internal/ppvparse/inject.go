package ppvparse

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// ToEPGProgram converts a parsed PPV stream name into an EPGProgram
// the existing UpsertEPGProgram path can write. Returns (zero, false)
// when the parse lacks the data we need to schedule a Plex slot —
// notably when StartAt is zero (Plex would render the programme at
// 00:00 UTC, which is useless).
//
// Conventions:
//   - Title carries the matchup ("Southampton - Blackburn").
//   - Description carries the broadcaster slot ("VIAPLAY PPV 9 (DK)")
//     so operator can tell which provider feed they're tuning into.
//   - Category list is ["Sports", <Sport-titlecased>] when we inferred
//     a sport, else just ["Sports"]. Plex uses the leading category.
//   - SourceHash is "ppv-parse:<slot-or-stream>:<unix>" so re-runs
//     upsert in place; namespaced so dispatcharr/SD ingest can't clash.
//   - IsLive=true for upcoming and currently-airing events because it
//     describes a live-origin broadcast, not whether wall-clock time is
//     presently inside the airing window. Plex currently ignores this
//     non-standard XMLTV extension, but other clients may use it.
//
// Callers that have a stable per-stream identifier (e.g. dispatcharr's
// stream id) should pass it as `streamHashKey` so the source_hash
// stays stable even when the broadcaster string is reformatted upstream.
// Empty streamHashKey falls back to the slot string.
func ToEPGProgram(channelID uuid.UUID, p ParsedStream, streamHashKey string) (store.EPGProgram, bool) {
	if p.Status == StatusEnded {
		// Already-aired games would render in Plex's "earlier today"
		// rail; leave them out — dispatcharr's existing EPG still
		// covers historical airings if the operator wants them.
		return store.EPGProgram{}, false
	}
	return EPGProgramIdentity(channelID, p, streamHashKey)
}

// EPGProgramIdentity constructs the exact row identity for a parsed event
// without rejecting StatusEnded. ppvsync uses this only to look up an already
// stored, still-active live-overrun row after the parsed nominal window has
// elapsed. Callers must never insert the returned ended candidate directly;
// ToEPGProgram remains the normal scheduling gate.
func EPGProgramIdentity(channelID uuid.UUID, p ParsedStream, streamHashKey string) (store.EPGProgram, bool) {
	if p.StartAt.IsZero() {
		return store.EPGProgram{}, false
	}
	if p.Title == "" {
		return store.EPGProgram{}, false
	}

	dur := p.Duration
	if dur <= 0 {
		dur = 3 * time.Hour
	}

	cats := []string{"Sports"}
	if p.Sport != "" {
		cats = append(cats, sportTitle(p.Sport))
	}

	desc := buildDescription(p)

	hashKey := streamHashKey
	if hashKey == "" {
		hashKey = p.Slot
	}
	// Title is part of the hash so a renamed event at the same kickoff
	// (placeholder → real matchup) registers as changed content for the
	// upsert's IS DISTINCT guard. The "ppv-parse:<key>:" prefix is
	// load-bearing — DeletePPVParsedRowsForChannelStream sweeps by it.
	srcHash := fmt.Sprintf("ppv-parse:%s:%d:%s", hashKey, p.StartAt.Unix(), p.Title)

	return store.EPGProgram{
		ChannelID:      channelID,
		StartAt:        p.StartAt,
		EndAt:          p.StartAt.Add(dur),
		Title:          p.Title,
		Description:    desc,
		Category:       cats,
		IsLive:         p.Status == StatusLive || p.Status == StatusUpcoming,
		SourceHash:     srcHash,
		SourcePriority: store.PriorityPPVSync,
	}, true
}

// buildDescription forms a one-line description with broadcaster +
// country code. Plex's tile UI shows a short blurb under the title.
func buildDescription(p ParsedStream) string {
	parts := []string{}
	if p.Broadcaster != "" {
		parts = append(parts, p.Broadcaster)
	}
	if p.Slot != "" && p.Slot != p.Broadcaster {
		parts = append(parts, p.Slot)
	}
	if p.CountryCode != "" {
		parts = append(parts, "("+p.CountryCode+")")
	}
	return strings.Join(parts, " ")
}

// sportTitle: lowercase enum-style league key → display string. NFL
// stays NFL, others get title-cased.
func sportTitle(s string) string {
	switch strings.ToLower(s) {
	case "nfl", "nba", "nhl", "mlb", "mls", "ncaaf", "ncaab",
		"epl", "f1", "pga", "ufc":
		return strings.ToUpper(s)
	}
	if s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + strings.ToLower(s[1:])
}

// OffSlotPrograms builds guide-presence rows for a recognised PPV slot.
// Returned rows tile a 24 h window in 6 h blocks aligned to the UTC
// 6-hour grid (00/06/12/18), so repeated worker passes regenerate
// identical rows and the slot-keyed upsert treats them as unchanged.
//
// When a future event is known for this exact channel, every idle block
// leading to kickoff names it ("Next Jun 10 6:15 PM MDT — Event"). The block
// containing kickoff is truncated precisely at event.StartAt; the real
// event row takes over there, and the post-event remainder of an
// overlapped block is emitted as its own row (minimum 15 minutes) so a
// game ending mid-block no longer leaves an "Unknown airing" hole until
// the next grid boundary. Off-grid tails are safe now that
// ReconcilePPVOffRows performs full-set reconciliation: any off row whose
// start is not in the freshly computed set is deleted, so a rescheduled
// or vanished event cannot orphan its old tail.
//
// SourceHash is "ppv-off:<key>:<blockUnix>" (NOT under the ppv-parse:
// prefix — the event sweep must never delete placeholders, and vice
// versa). SourcePriority is store.PriorityPPVOff: any real row from
// any source replaces a placeholder; a placeholder never replaces
// anything.
//
// Idle rows retain the collapsed "<LEAGUE> — No Event Scheduled" title
// used before cutover. Next-event hashes include the rendered event
// identity so a title, time, or display-zone correction updates the
// existing (channel,start) row instead of being rejected as unchanged.
func OffSlotPrograms(channelID uuid.UUID, p ParsedStream, streamHashKey string, ev *store.EPGProgram, now time.Time, displayLoc *time.Location) []store.EPGProgram {
	if p.Slot == "" {
		return nil
	}
	if displayLoc == nil {
		displayLoc = time.UTC
	}
	league := strings.ToUpper(strings.TrimSpace(p.Sport))
	if league == "" {
		league = p.Slot
	}
	idleTitle := league + " — No Event Scheduled"
	idleDesc := "PPV slot " + p.Slot + " — no event currently scheduled. Check back closer to game time."
	// A substantive-but-untimed off name is real information: NCAAF-style
	// banks label relay slots with the network they carry ("NCAAF 02:
	// ESPN2"). Showing that label beats a guide hole — before 2026-08-29
	// these slots injected nothing and Plex rendered all-day "Unknown
	// airing" on brand-new channels with no last-known rows.
	if p.Status == StatusOff && !p.OffIdle && strings.TrimSpace(p.Title) != "" {
		announced := strings.TrimSpace(p.Title)
		idleTitle = announced
		idleDesc = "PPV slot " + p.Slot + " is currently labeled " + strconv.Quote(announced) +
			" — no scheduled event time announced."
	}

	hashKey := streamHashKey
	if hashKey == "" {
		hashKey = p.Slot
	}

	const block = 6 * time.Hour
	// Post-event remainders shorter than this are noise in a guide row.
	const minOffSliver = 15 * time.Minute
	gridStart := now.UTC().Truncate(block)
	out := make([]store.EPGProgram, 0, 4)
	eventValid := ev != nil &&
		strings.TrimSpace(ev.Title) != "" &&
		ev.EndAt.After(ev.StartAt)
	eventUpcoming := eventValid && ev.StartAt.After(now)
	nextTitle, nextDesc := "", ""
	if eventUpcoming {
		nextTitle = formatNextEventTitle(ev.Title, ev.StartAt, displayLoc)
		nextDesc = fmt.Sprintf(
			"Next live broadcast on PPV slot %s: %s at %s.",
			p.Slot,
			strings.TrimSpace(ev.Title),
			ev.StartAt.In(displayLoc).Format("Monday, January 2, 2006 at 3:04 PM MST"),
		)
	}

	for i := 0; i < 4; i++ {
		bs := gridStart.Add(time.Duration(i) * block)
		be := bs.Add(block)
		rowEnd := be
		title, desc := idleTitle, idleDesc
		sourceHash := fmt.Sprintf("ppv-off:%s:%d", hashKey, bs.Unix())

		if eventUpcoming && bs.Before(ev.StartAt) {
			title, desc = nextTitle, nextDesc
			if be.After(ev.StartAt) {
				rowEnd = ev.StartAt
			}
			digest := sha256.Sum256([]byte(nextTitle + "\x00" + nextDesc))
			sourceHash = fmt.Sprintf(
				"ppv-off:%s:%d:next:%d:%x",
				hashKey, bs.Unix(), ev.StartAt.Unix(), digest[:8],
			)
		}

		if eventValid && bs.Before(ev.EndAt) && be.After(ev.StartAt) {
			// The event owns its own interval; emit the UNCOVERED remainder
			// of the block instead of dropping the block whole. Dropping
			// left post-event holes until the next grid boundary (observed:
			// a game ending 02:30 rendered "Unknown airing" until 06:00,
			// 2026-08-29 NCAAF report).
			if eventUpcoming && bs.Before(ev.StartAt) && rowEnd.After(bs) {
				// Pre-event portion, carrying the "Next —" banner built above.
				out = append(out, store.EPGProgram{
					ChannelID:      channelID,
					StartAt:        bs,
					EndAt:          rowEnd,
					Title:          title,
					Description:    desc,
					Category:       []string{"Sports"},
					SourceHash:     sourceHash,
					SourcePriority: store.PriorityPPVOff,
				})
			}
			if postStart := ev.EndAt; postStart.Before(be) && be.Sub(postStart) >= minOffSliver {
				out = append(out, store.EPGProgram{
					ChannelID:      channelID,
					StartAt:        postStart,
					EndAt:          be,
					Title:          idleTitle,
					Description:    idleDesc,
					Category:       []string{"Sports"},
					SourceHash:     fmt.Sprintf("ppv-off:%s:%d:post:%d", hashKey, bs.Unix(), postStart.Unix()),
					SourcePriority: store.PriorityPPVOff,
				})
			}
			continue
		}
		out = append(out, store.EPGProgram{
			ChannelID:      channelID,
			StartAt:        bs,
			EndAt:          rowEnd,
			Title:          title,
			Description:    desc,
			Category:       []string{"Sports"},
			SourceHash:     sourceHash,
			SourcePriority: store.PriorityPPVOff,
		})
	}
	return out
}

func formatNextEventTitle(eventTitle string, startAt time.Time, displayLoc *time.Location) string {
	startLocal := startAt.In(displayLoc)
	// Plex caches XMLTV between refreshes. Always include the local date,
	// even for an event happening today, so the label remains unambiguous
	// across midnight and does not change identity merely as "today" moves.
	timeLabel := startLocal.Format("Jan 2 3:04 PM MST")
	return "Next " + timeLabel + " — " + strings.TrimSpace(eventTitle)
}
