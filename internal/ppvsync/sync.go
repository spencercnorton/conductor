// Package ppvsync periodically reads the IPTV provider's live-stream
// catalogue, parses any formatted PPV stream names via internal/ppvparse,
// and injects matching events as epg_program rows on Conductor channels.
//
// # Why this exists
//
// The upstream provider (iboost et al.) dynamically renames each PPV
// stream as games are scheduled. The names look like:
//
//	"LIVE | CHAMPIONSHIP SOUTHAMPTON - BLACKBURN | Tue 14 Apr 20:55 CEST (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9"
//
// — encoding status, event, kickoff, broadcaster, slot. XMLTV guides
// never carry this; only the stream catalogue does. ppvsync polls it and
// turns parseable names into EPG programmes.
//
// v2 (2026-06-10, audit E2): the catalogue comes straight from the
// provider's Xtream player_api (see xtream_lookup.go). v1 read the same
// data out of Dispatcharr's internal Postgres — a dependency on a second
// tuner system + cross-stack docker network + credentialed DSN that
// silently killed PPV EPG whenever Dispatcharr was down (2026-05-26,
// 2026-06-07).
//
// Idempotency: each parsed event maps to an `epg_program` row whose
// source_hash is namespaced under "ppv-parse:xtream:<stream-id>:<unix>"
// (v1 rows under ppv-parse:dispatcharr:* are purged at cutover); the
// channel-level atomic replacement clears drifted/redundant starts.
package ppvsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/ppvparse"
	"github.com/spencercnorton/conductor/internal/store"
)

// StreamLookup answers "what does the provider currently call the stream
// behind this channel_source?". Implemented by XtreamLookup (provider
// player_api catalogue); stub-able for tests.
type StreamLookup interface {
	// BeginPass pins one provider catalogue generation until EndPass. A TTL
	// boundary inside a redundant-source loop must not mix old and new names.
	BeginPass(ctx context.Context) error
	EndPass()
	LookupBySource(ctx context.Context, src store.ChannelSource) (StreamRow, error)
	Close()
}

// StreamRow is the trimmed view of a provider stream record.
type StreamRow struct {
	ID        int64     // provider stream id (the integer in the source URL)
	Name      string    // free-text stream name (the parseable bit)
	UpdatedAt time.Time // pinned catalogue generation used by the DB stale-write fence
}

// ErrNotFound is the canonical "no provider stream matches that source"
// error. Worker treats it as "skip silently."
var ErrNotFound = errors.New("provider stream not found")

// Store is the subset of *store.DB the worker needs.
type Store interface {
	// AcquirePPVSyncLease orders the complete provider-fetch-to-guide-commit
	// pass across rolling workers. The provider exposes no revision token.
	AcquirePPVSyncLease(ctx context.Context) (release func() error, err error)
	ListChannelSources(ctx context.Context) ([]store.ChannelSource, error)
	// ReplacePPVParsedRowsForChannel atomically upserts the selected
	// channel-level event and removes every other ppv-parse row on that
	// channel. It prevents both redundant-source leftovers and a transient
	// empty guide window between cleanup and insertion.
	ReplacePPVParsedRowsForChannel(ctx context.Context, p store.EPGProgram, observedAt time.Time) (string, int64, bool, error)
	// DeletePPVParsedRowsForChannelStream removes existing
	// ppv-parse:<streamHashKey>:* rows for one channel when that exact
	// source explicitly reports an ended/idle state.
	DeletePPVParsedRowsForChannelStream(ctx context.Context, channelID uuid.UUID, streamHashKey string, observedAt time.Time) (int64, bool, error)
	// ReconcilePPVOffRows performs the final occupancy check, cleanup, and
	// placeholder publication atomically under the channel mutation lock.
	ReconcilePPVOffRows(ctx context.Context, channelID uuid.UUID, gridStart, windowEnd time.Time, evStart, evEnd *time.Time, rows []store.EPGProgram, observedAt time.Time) (int, bool, error)
	// ExtendPPVLiveRowEnd pushes a still-live event's end_at forward so the
	// live tile survives an overrun (S4).
	ExtendPPVLiveRowEnd(ctx context.Context, channelID uuid.UUID, startAt time.Time, sourceHash string, observedAt, triggerBefore, newEnd, hardEnd time.Time) (int64, bool, error)
	// ActivePPVLiveOverrunEnd probes an exact parsed identity after its
	// nominal window elapsed. It succeeds only while a prior extension remains
	// active, so an expired or changed event cannot be resurrected.
	ActivePPVLiveOverrunEnd(ctx context.Context, channelID uuid.UUID, startAt time.Time, sourceHash string, activeAt, hardEnd time.Time) (time.Time, bool, error)
	// PreservePPVLiveOverrunForChannel atomically retains and, when near
	// expiry, refreshes that exact active row as the channel winner. It never
	// inserts and therefore closes the probe-to-write race fail-closed.
	PreservePPVLiveOverrunForChannel(ctx context.Context, channelID uuid.UUID, startAt time.Time, sourceHash string, observedAt, activeAt, hardEnd, triggerBefore, newEnd time.Time) (time.Time, int64, bool, error)
}

const (
	liveExtensionTrigger = 30 * time.Minute
	liveExtensionWindow  = 90 * time.Minute

	// maxLiveOverrun is the finite amount of continuity an unchanged,
	// clock-elapsed provider name may borrow beyond its nominal parsed stop.
	// Three hours covers ordinary sports overtime and long combat-card delays,
	// while ensuring stale catalogue text can never renew a live row forever.
	maxLiveOverrun = 3 * time.Hour
)

// Worker syncs PPV stream names every Interval.
type Worker struct {
	Store           Store
	Lookup          StreamLookup
	Interval        time.Duration // default 15 min
	DisplayLocation *time.Location
	Logger          *slog.Logger
	now             func() time.Time

	// Monitor tracks pass success/failure for stall alerting +
	// /admin/epg/health. Optional; nil no-ops.
	Monitor *alerts.WorkerMonitor

	// lastGoodSlot remembers the last successfully-parsed slot label per
	// channel_source id. When a bound name later becomes unparseable or the
	// provider lookup misses, we keep tiling placeholders under this label so
	// the channel doesn't go blank (S3). In-memory only: runOnce is called
	// sequentially by Run, so no locking is needed; a restart just re-learns
	// each source's slot on its next successful parse.
	lastGoodSlot map[uuid.UUID]string
}

type offSlotCandidate struct {
	parsed         ppvparse.ParsedStream
	streamHashKey  string
	event          *store.EPGProgram
	overrun        bool
	overrunLimit   time.Time
	observedAt     time.Time
	sourcePriority int
	sourceID       uuid.UUID
}

type offSweepCandidate struct {
	streamHashKey string
	observedAt    time.Time
}

// preferOffSlotCandidate resolves redundant sources into one channel-level
// guide status. A schedulable event always beats an idle source, and a current
// event beats continuity from an elapsed overrun; otherwise source priority
// (then UUID for deterministic ties) decides. Without this,
// a fallback account whose catalogue rename lagged behind the primary could
// overwrite "Next — Event" with "No Event Scheduled" later in the same pass.
func preferOffSlotCandidate(candidate, current offSlotCandidate) bool {
	if (candidate.event != nil) != (current.event != nil) {
		return candidate.event != nil
	}
	if candidate.event != nil && candidate.overrun != current.overrun {
		// A current schedulable event is stronger evidence than continuity from
		// an elapsed-but-still-extended row, regardless of source priority.
		return !candidate.overrun
	}
	if candidate.sourcePriority != current.sourcePriority {
		return candidate.sourcePriority < current.sourcePriority
	}
	return candidate.sourceID.String() < current.sourceID.String()
}

// New constructs a Worker. Caller is responsible for wiring up the
// real StreamLookup (or a stub) — `New` does not perform I/O.
func New(s Store, l StreamLookup, logger *slog.Logger) *Worker {
	return &Worker{
		Store:           s,
		Lookup:          l,
		Interval:        15 * time.Minute,
		DisplayLocation: time.UTC,
		Logger:          logger,
		now:             time.Now,
		lastGoodSlot:    make(map[uuid.UUID]string),
	}
}

func (w *Worker) currentTime() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

// Run blocks until ctx is cancelled. First tick fires immediately so
// fresh containers get a sync without waiting.
func (w *Worker) Run(ctx context.Context) {
	if w.Logger != nil {
		w.Logger.Info("ppvsync worker starting", "interval", w.Interval)
	}
	if w.Monitor != nil && w.Monitor.StallAfter <= 0 {
		w.Monitor.StallAfter = 3 * w.Interval
	}
	w.Monitor.Start()
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		w.runOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runOnce does one full pass: list our channel_source rows, look up
// each stream name in the provider catalogue, parse, inject. Errors per-source
// don't abort the pass.
func (w *Worker) runOnce(ctx context.Context) {
	releasePass, err := w.Store.AcquirePPVSyncLease(ctx)
	if err != nil {
		if w.Logger != nil {
			w.Logger.Error("ppvsync acquire pass lease", "err", err)
		}
		w.Monitor.Failure(ctx, "acquire pass lease: "+err.Error())
		return
	}
	defer func() {
		if err := releasePass(); err != nil && w.Logger != nil {
			w.Logger.Error("ppvsync release pass lease", "err", err)
		}
	}()
	sources, err := w.Store.ListChannelSources(ctx)
	if err != nil {
		if w.Logger != nil {
			w.Logger.Error("ppvsync list channel_sources", "err", err)
		}
		w.Monitor.Failure(ctx, "list channel_sources: "+err.Error())
		return
	}
	hasEnabledSource := false
	for _, src := range sources {
		if src.Enabled {
			hasEnabledSource = true
			break
		}
	}
	if hasEnabledSource {
		if err := w.Lookup.BeginPass(ctx); err != nil {
			if w.Logger != nil {
				w.Logger.Error("ppvsync begin catalogue snapshot", "err", err)
			}
			w.Monitor.Failure(ctx, "begin catalogue snapshot: "+err.Error())
			return
		}
		defer w.Lookup.EndPass()
	}

	// A pass counts as successful when the provider answered at least one
	// lookup — ErrNotFound is an answer (the stream just isn't there);
	// only transport errors count against the pass. All-lookups-dead is
	// the 2026-06-07 failure shape (catalogue source gone, every lookup
	// erroring, worker logging WARN forever).
	lookupAnswered, lookupFailed := 0, 0
	var lastLookupErr error

	if w.lastGoodSlot == nil {
		w.lastGoodSlot = make(map[uuid.UUID]string)
	}

	parsed, injected, skipped, offSlots := 0, 0, 0, 0
	offCandidates := make(map[uuid.UUID]offSlotCandidate)
	offCandidateOrder := make([]uuid.UUID, 0)
	offSweepKeys := make(map[uuid.UUID][]offSweepCandidate)
	rememberedCandidates := make(map[uuid.UUID]offSlotCandidate)
	rememberedCandidateOrder := make([]uuid.UUID, 0)
	rememberLastGood := func(src store.ChannelSource, observedAt time.Time) bool {
		slot := w.lastGoodSlot[src.ID]
		if slot == "" {
			return false
		}
		candidate := offSlotCandidate{
			// StatusUnknown is deliberate: this is continuity from memory, not
			// a provider declaration that the slot is definitively idle.
			parsed:         ppvparse.ParsedStream{Status: ppvparse.StatusUnknown, Slot: slot},
			streamHashKey:  "src:" + src.ID.String(),
			sourcePriority: src.Priority,
			sourceID:       src.ID,
			observedAt:     observedAt,
		}
		current, exists := rememberedCandidates[src.ChannelID]
		if !exists {
			rememberedCandidateOrder = append(rememberedCandidateOrder, src.ChannelID)
		}
		if !exists || preferOffSlotCandidate(candidate, current) {
			rememberedCandidates[src.ChannelID] = candidate
		}
		return true
	}
	for _, src := range sources {
		if !src.Enabled {
			continue
		}
		row, err := w.Lookup.LookupBySource(ctx, src)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				lookupAnswered++
				// Preserve a channel-level fallback candidate from the last slot
				// this source parsed. It is considered only if every source for
				// the channel failed to produce a current parsed state (S3).
				if !rememberLastGood(src, row.UpdatedAt) {
					skipped++
				}
			} else {
				lookupFailed++
				lastLookupErr = err
				if w.Logger != nil && lookupFailed == 1 {
					streamID, _ := StreamIDFromURL(src.UpstreamURL)
					w.Logger.Warn("ppvsync stream lookup",
						"source", src.ID, "stream_id", streamID, "err", err)
				}
				skipped++
			}
			continue
		}
		lookupAnswered++
		ps, ok := ppvparse.Parse(row.Name)
		if !ok {
			// A provider reformat must not blank the channel. Remembered state
			// stays secondary to any successfully parsed sibling source.
			if !rememberLastGood(src, row.UpdatedAt) {
				skipped++
			}
			continue
		}
		parsed++
		if ps.Slot != "" {
			w.lastGoodSlot[src.ID] = ps.Slot
		}

		streamHashKey := fmt.Sprintf("xtream:%d", row.ID)
		prog, evOK := ppvparse.ToEPGProgram(src.ChannelID, ps, streamHashKey)
		overrun := false
		var overrunLimit time.Time
		if !evOK && ps.Status == ppvparse.StatusEnded && !ps.EndExplicit {
			// A time-derived end on an otherwise unchanged provider name is not
			// definitive while the exact stored row still carries a live extension.
			// Probe by the same source hash/start identity used for storage; an
			// explicit ENDED prefix, rename, reschedule, or expired extension stays on
			// the normal cleanup path. A probe error preserves the last committed row
			// for this pass rather than deleting liveness on an uncertain read.
			identity, identityOK := ppvparse.EPGProgramIdentity(src.ChannelID, ps, streamHashKey)
			if identityOK {
				activeAt := w.currentTime()
				overrunLimit = identity.EndAt.Add(maxLiveOverrun)
				storedEnd, active, overrunErr := w.Store.ActivePPVLiveOverrunEnd(
					ctx, src.ChannelID, identity.StartAt, identity.SourceHash, activeAt, overrunLimit,
				)
				if overrunErr != nil {
					if w.Logger != nil {
						w.Logger.Warn("ppvsync live-overrun probe", "channel", src.ChannelID, "err", overrunErr)
					}
					skipped++
					continue
				}
				if active {
					identity.EndAt = storedEnd
					prog = identity
					evOK = true
					overrun = true
				}
			}
		}
		if !evOK && (ps.Status == ppvparse.StatusEnded || ps.OffIdle) {
			// The provider clearly says nothing is on this slot: the event
			// already ended, or the name is a genuine idle shape
			// (bare/TBA/"NO EVENT"). Sweep any event row a prior pass
			// injected so the channel resets to "No Event Scheduled"
			// instead of freezing on the finished game (the "guide only
			// updates once nothing is on" report, 2026-07-20).
			//
			// An AMBIGUOUS off (ps.OffIdle == false) — a truncated event
			// name that lost its "(…ET)" suffix to iboost's length cap, or
			// an untimed venue label — is deliberately NOT swept: it may
			// still be a live/upcoming event, so the last-known event and
			// guide-status rows stay until a source clarifies the state.
			offSweepKeys[src.ChannelID] = append(offSweepKeys[src.ChannelID], offSweepCandidate{
				streamHashKey: streamHashKey,
				observedAt:    row.UpdatedAt,
			})
		}

		candidate := offSlotCandidate{
			parsed:         ps,
			streamHashKey:  streamHashKey,
			overrun:        overrun,
			sourcePriority: src.Priority,
			sourceID:       src.ID,
			observedAt:     row.UpdatedAt,
		}
		if overrun {
			candidate.overrunLimit = overrunLimit
		}
		if evOK {
			event := prog
			candidate.event = &event
		}
		current, exists := offCandidates[src.ChannelID]
		if !exists {
			offCandidateOrder = append(offCandidateOrder, src.ChannelID)
		}
		if !exists || preferOffSlotCandidate(candidate, current) {
			offCandidates[src.ChannelID] = candidate
		}
	}

	// A current provider parse is always more authoritative than remembered
	// continuity, even when the remembered source has a better priority. Only
	// channels with no current parsed candidate fall back to lastGoodSlot.
	for _, channelID := range rememberedCandidateOrder {
		if _, exists := offCandidates[channelID]; exists {
			continue
		}
		offCandidates[channelID] = rememberedCandidates[channelID]
		offCandidateOrder = append(offCandidateOrder, channelID)
	}

	// Guide presence is channel-level, not source-level. Resolve redundant
	// source states above, then write one 24 h grid per channel so a lagging
	// fallback cannot erase a primary source's exact "Next" announcement.
	for _, channelID := range offCandidateOrder {
		candidate := offCandidates[channelID]
		if candidate.observedAt.IsZero() {
			// Every destructive/publication path is bound to a concrete provider
			// catalogue generation. A transport-only or synthetic state without one
			// preserves the last committed guide rather than inventing authority.
			skipped++
			continue
		}
		if candidate.event != nil {
			action := "unchanged"
			var err error
			if candidate.overrun {
				now := w.currentTime()
				newEnd := now.Add(liveExtensionWindow)
				if newEnd.After(candidate.overrunLimit) {
					newEnd = candidate.overrunLimit
				}
				var kept bool
				candidate.event.EndAt, _, kept, err = w.Store.PreservePPVLiveOverrunForChannel(
					ctx,
					channelID,
					candidate.event.StartAt,
					candidate.event.SourceHash,
					candidate.observedAt,
					now,
					candidate.overrunLimit,
					now.Add(liveExtensionTrigger),
					newEnd,
				)
				if err == nil && !kept {
					// The exact row expired or changed after the read-only probe. Do
					// not reinsert stale state; the next pass will reconcile normally.
					skipped++
					continue
				}
			} else {
				var applied bool
				action, _, applied, err = w.Store.ReplacePPVParsedRowsForChannel(ctx, *candidate.event, candidate.observedAt)
				if err == nil && !applied {
					skipped++
					continue
				}
			}
			if err != nil {
				if w.Logger != nil {
					w.Logger.Warn("ppvsync event replace",
						"channel", channelID, "err", err)
				}
				// Do not advertise a Next row when its real event row
				// could not be stored; that would reintroduce an empty
				// guide boundary at kickoff.
				skipped++
				continue
			}
			// "updated" counts too: under the slot-keyed upsert (audit
			// E3) a re-titled or rescheduled event updates in place.
			if action == "added" || action == "updated" {
				injected++
			}
			// Keep a still-live event's tile alive when its parsed window is
			// within 30 minutes of ending. The channel-level winner has already
			// committed, so the extension cannot touch a sibling source's row.
			if !candidate.overrun && candidate.parsed.Status == ppvparse.StatusLive {
				now := w.currentTime()
				hardEnd := candidate.event.EndAt.Add(maxLiveOverrun)
				newEnd := now.Add(liveExtensionWindow)
				if newEnd.After(hardEnd) {
					newEnd = hardEnd
				}
				n, applied, err := w.Store.ExtendPPVLiveRowEnd(
					ctx, channelID, candidate.event.StartAt, candidate.event.SourceHash,
					candidate.observedAt, now.Add(liveExtensionTrigger), newEnd, hardEnd,
				)
				if err != nil {
					if w.Logger != nil {
						w.Logger.Warn("ppvsync live-extend", "channel", channelID, "err", err)
					}
				} else if !applied {
					skipped++
					continue
				} else if n > 0 && newEnd.After(candidate.event.EndAt) {
					// Keep the in-memory window aligned with the row just extended;
					// the occupancy query below also catches prior-pass extensions.
					candidate.event.EndAt = newEnd
				}
			}
		} else {
			// Only clear definitive ended/idle sources once we know no exact
			// channel-level event won. If an exact replacement later fails,
			// preserving the previous committed event is safer than leaving
			// the guide empty.
			for _, sweep := range offSweepKeys[channelID] {
				if _, applied, err := w.Store.DeletePPVParsedRowsForChannelStream(ctx, channelID, sweep.streamHashKey, sweep.observedAt); err != nil && w.Logger != nil {
					w.Logger.Warn("ppvsync off-state sweep",
						"channel", channelID, "stream", sweep.streamHashKey, "err", err)
				} else if err == nil && !applied {
					skipped++
					break
				}
			}
		}
		// A substantive but untimed off name (StatusOff, !OffIdle) is
		// ambiguous about EVENTS — it never sweeps last-known event rows
		// (it is excluded from offSweepKeys above) — but it must still
		// inject off-blocks: injection only writes ppv-off: rows, which the
		// store reconciles around higher-priority rows, so a prior event
		// cannot be clobbered. Before 2026-08-29 these slots were skipped
		// entirely and brand-new relay channels ("NCAAF 02: ESPN2") had NO
		// rows at all — all-day "Unknown airing" in Plex.
		offN := w.injectOffSlot(
			ctx,
			channelID,
			candidate.parsed,
			candidate.streamHashKey,
			candidate.event,
			candidate.observedAt,
		)
		offSlots += offN
		if candidate.event == nil && offN == 0 {
			skipped++
		}
	}

	if lookupFailed > 0 && lookupAnswered == 0 {
		detail := fmt.Sprintf("all %d stream lookups failed", lookupFailed)
		if lastLookupErr != nil {
			detail += ": " + lastLookupErr.Error()
		}
		w.Monitor.Failure(ctx, detail)
	} else {
		// Includes the zero-enabled-sources case — an idle worker isn't
		// a stalled worker.
		w.Monitor.Success(ctx)
	}

	if w.Logger != nil {
		w.Logger.Info("ppvsync pass complete",
			"sources", len(sources),
			"parsed", parsed,
			"injected", injected,
			"off_blocks", offSlots,
			"skipped", skipped)
	}
}

// injectOffSlot writes the placeholder grid for one recognised slot and
// returns how many placeholder rows were added/refreshed. Placeholders never
// overlap ev, and a future ev is advertised in every pre-kickoff row.
func (w *Worker) injectOffSlot(ctx context.Context, channelID uuid.UUID, ps ppvparse.ParsedStream, streamHashKey string, ev *store.EPGProgram, observedAt time.Time) int {
	now := w.currentTime()
	rows := ppvparse.OffSlotPrograms(channelID, ps, streamHashKey, ev, now, w.DisplayLocation)
	if len(rows) == 0 {
		return 0
	}
	gridStart := now.UTC().Truncate(6 * time.Hour)
	windowEnd := gridStart.Add(24 * time.Hour)

	var evStart, evEnd *time.Time
	if ev != nil && ev.EndAt.After(ev.StartAt) {
		evStart, evEnd = &ev.StartAt, &ev.EndAt
	}
	n, applied, err := w.Store.ReconcilePPVOffRows(
		ctx, channelID, gridStart, windowEnd, evStart, evEnd, rows, observedAt,
	)
	if err != nil {
		if w.Logger != nil {
			w.Logger.Warn("ppvsync off-slot reconcile", "channel", channelID, "err", err)
		}
		return 0
	}
	if !applied {
		return 0
	}
	return n
}
