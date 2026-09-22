package ppvsync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/ppvparse"
	"github.com/spencercnorton/conductor/internal/store"
)

// fakeStore: in-memory store for the worker. Records every upsert so
// tests can assert what got injected.
type fakeStore struct {
	mu                        sync.Mutex
	passMu                    sync.Mutex
	sources                   []store.ChannelSource
	programs                  []store.EPGProgram
	replaceErr                error
	occupiedErr               error
	deleteOffErr              error
	extendErr                 error
	overrunProbeErr           error
	overrunPreserveErr        error
	dropOverrunBeforePreserve bool
	latestObservation         map[uuid.UUID]time.Time
}

func (f *fakeStore) AcquirePPVSyncLease(ctx context.Context) (func() error, error) {
	f.passMu.Lock()
	var once sync.Once
	return func() error {
		once.Do(f.passMu.Unlock)
		return nil
	}, nil
}

func (f *fakeStore) acceptObservation(channelID uuid.UUID, observedAt time.Time) bool {
	if observedAt.IsZero() {
		return false
	}
	if f.latestObservation == nil {
		f.latestObservation = make(map[uuid.UUID]time.Time)
	}
	if observedAt.Before(f.latestObservation[channelID]) {
		return false
	}
	f.latestObservation[channelID] = observedAt
	return true
}

func (f *fakeStore) ListChannelSources(ctx context.Context) ([]store.ChannelSource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]store.ChannelSource, len(f.sources))
	copy(out, f.sources)
	return out, nil
}

func (f *fakeStore) UpsertEPGProgram(ctx context.Context, p store.EPGProgram) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.programs = append(f.programs, p)
	return "added", nil
}

func (f *fakeStore) ReplacePPVParsedRowsForChannel(ctx context.Context, p store.EPGProgram, observedAt time.Time) (string, int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.replaceErr != nil {
		return "", 0, false, f.replaceErr
	}
	if !f.acceptObservation(p.ChannelID, observedAt) {
		return "", 0, false, nil
	}
	kept := make([]store.EPGProgram, 0, len(f.programs))
	deleted := int64(0)
	for _, existing := range f.programs {
		if existing.ChannelID == p.ChannelID && strings.HasPrefix(existing.SourceHash, "ppv-parse:") {
			deleted++
			continue
		}
		kept = append(kept, existing)
	}
	f.programs = append(kept, p)
	return "added", deleted, true, nil
}

// DeletePPVParsedRowsForChannelStream simulates the day-boundary
// stale-row cleanup. Removes any prior programmes whose source_hash
// looks like ppv-parse:<key>:* on this channel — same surface the
// real Postgres helper exposes.
func (f *fakeStore) DeletePPVParsedRowsForChannelStream(ctx context.Context, channelID uuid.UUID, streamHashKey string, observedAt time.Time) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.acceptObservation(channelID, observedAt) {
		return 0, false, nil
	}
	prefix := "ppv-parse:" + streamHashKey + ":"
	kept := f.programs[:0]
	deleted := int64(0)
	for _, p := range f.programs {
		if p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, prefix) {
			deleted++
			continue
		}
		kept = append(kept, p)
	}
	f.programs = kept
	return deleted, true, nil
}

// OccupiedProgramWindows returns real (source_priority < PriorityPPVOff) row
// windows on the channel overlapping [from,to). Mirrors the store helper (S2).
func (f *fakeStore) OccupiedProgramWindows(ctx context.Context, channelID uuid.UUID, from, to time.Time) ([]store.TimeRange, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.occupiedErr != nil {
		return nil, f.occupiedErr
	}
	var out []store.TimeRange
	for _, p := range f.programs {
		if p.ChannelID == channelID && p.SourcePriority < store.PriorityPPVOff &&
			p.StartAt.Before(to) && p.EndAt.After(from) {
			out = append(out, store.TimeRange{Start: p.StartAt, End: p.EndAt})
		}
	}
	return out, nil
}

// ExtendPPVLiveRowEnd bumps end_at of the matching ppv-parse row when its
// current end_at is before triggerBefore. Mirrors the store helper (S4).
func (f *fakeStore) ExtendPPVLiveRowEnd(ctx context.Context, channelID uuid.UUID, startAt time.Time, sourceHash string, observedAt, triggerBefore, newEnd, hardEnd time.Time) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.extendErr != nil {
		return 0, false, f.extendErr
	}
	if !f.acceptObservation(channelID, observedAt) {
		return 0, false, nil
	}
	n := int64(0)
	for i := range f.programs {
		p := &f.programs[i]
		if p.ChannelID == channelID && p.StartAt.Equal(startAt) &&
			p.SourceHash == sourceHash && p.SourcePriority == store.PriorityPPVSync &&
			p.EndAt.Before(triggerBefore) && newEnd.After(p.EndAt) && hardEnd.After(p.EndAt) {
			p.EndAt = newEnd
			if p.EndAt.After(hardEnd) {
				p.EndAt = hardEnd
			}
			n++
		}
	}
	return n, true, nil
}

func (f *fakeStore) ActivePPVLiveOverrunEnd(ctx context.Context, channelID uuid.UUID, startAt time.Time, sourceHash string, activeAt, hardEnd time.Time) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.overrunProbeErr != nil {
		return time.Time{}, false, f.overrunProbeErr
	}
	if !activeAt.Before(hardEnd) {
		return time.Time{}, false, nil
	}
	for _, p := range f.programs {
		if p.ChannelID == channelID && p.StartAt.Equal(startAt) &&
			p.SourceHash == sourceHash && p.SourcePriority == store.PriorityPPVSync &&
			p.EndAt.After(activeAt) {
			end := p.EndAt
			if end.After(hardEnd) {
				end = hardEnd
			}
			return end, true, nil
		}
	}
	return time.Time{}, false, nil
}

func (f *fakeStore) PreservePPVLiveOverrunForChannel(
	ctx context.Context,
	channelID uuid.UUID,
	startAt time.Time,
	sourceHash string,
	observedAt time.Time,
	activeAt time.Time,
	hardEnd time.Time,
	triggerBefore time.Time,
	newEnd time.Time,
) (time.Time, int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.overrunPreserveErr != nil {
		return time.Time{}, 0, false, f.overrunPreserveErr
	}
	if !f.acceptObservation(channelID, observedAt) {
		return time.Time{}, 0, false, nil
	}
	if !activeAt.Before(hardEnd) {
		return time.Time{}, 0, false, nil
	}
	if f.dropOverrunBeforePreserve {
		kept := make([]store.EPGProgram, 0, len(f.programs))
		for _, p := range f.programs {
			if p.ChannelID == channelID && p.StartAt.Equal(startAt) && p.SourceHash == sourceHash {
				continue
			}
			kept = append(kept, p)
		}
		f.programs = kept
		return time.Time{}, 0, false, nil
	}
	winner := -1
	for i := range f.programs {
		p := &f.programs[i]
		if p.ChannelID == channelID && p.StartAt.Equal(startAt) &&
			p.SourceHash == sourceHash && p.SourcePriority == store.PriorityPPVSync &&
			p.EndAt.After(activeAt) {
			winner = i
			break
		}
	}
	if winner < 0 {
		return time.Time{}, 0, false, nil
	}
	if f.programs[winner].EndAt.After(hardEnd) {
		f.programs[winner].EndAt = hardEnd
	}
	if f.programs[winner].EndAt.Before(triggerBefore) && newEnd.After(f.programs[winner].EndAt) {
		f.programs[winner].EndAt = newEnd
		if f.programs[winner].EndAt.After(hardEnd) {
			f.programs[winner].EndAt = hardEnd
		}
	}
	endAt := f.programs[winner].EndAt
	kept := make([]store.EPGProgram, 0, len(f.programs))
	deleted := int64(0)
	for i, p := range f.programs {
		if i != winner && p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			deleted++
			continue
		}
		kept = append(kept, p)
	}
	f.programs = kept
	return endAt, deleted, true, nil
}

// DeletePPVOffRows mirrors the store helper: drops channel-wide ppv-off rows
// that are stale or overlap either the selected event or any stronger row.
func (f *fakeStore) DeletePPVOffRows(ctx context.Context, channelID uuid.UUID, gridStart time.Time, evStart, evEnd *time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteOffErr != nil {
		return 0, f.deleteOffErr
	}
	realRows := append([]store.EPGProgram(nil), f.programs...)
	kept := f.programs[:0]
	deleted := int64(0)
	for _, p := range f.programs {
		overlapsReal := false
		for _, realRow := range realRows {
			if realRow.ChannelID == channelID && realRow.SourcePriority < store.PriorityPPVOff &&
				p.StartAt.Before(realRow.EndAt) && p.EndAt.After(realRow.StartAt) {
				overlapsReal = true
				break
			}
		}
		match := p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-off:") &&
			(p.StartAt.Before(gridStart) ||
				(evStart != nil && p.StartAt.Before(*evEnd) && p.EndAt.After(*evStart)) ||
				overlapsReal)
		if match {
			deleted++
			continue
		}
		kept = append(kept, p)
	}
	f.programs = kept
	return deleted, nil
}

func (f *fakeStore) ReconcilePPVOffRows(
	ctx context.Context,
	channelID uuid.UUID,
	gridStart, windowEnd time.Time,
	evStart, evEnd *time.Time,
	rows []store.EPGProgram,
	observedAt time.Time,
) (int, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.occupiedErr != nil {
		return 0, false, f.occupiedErr
	}
	if f.deleteOffErr != nil {
		return 0, false, f.deleteOffErr
	}
	if !f.acceptObservation(channelID, observedAt) {
		return 0, false, nil
	}

	realRows := append([]store.EPGProgram(nil), f.programs...)
	occupied := make([]store.TimeRange, 0)
	for _, p := range realRows {
		if p.ChannelID == channelID && p.SourcePriority < store.PriorityPPVOff &&
			p.StartAt.Before(windowEnd) && p.EndAt.After(gridStart) {
			occupied = append(occupied, store.TimeRange{Start: p.StartAt, End: p.EndAt})
		}
	}
	kept := f.programs[:0]
	for _, p := range f.programs {
		overlapsReal := false
		for _, real := range occupied {
			if p.StartAt.Before(real.End) && p.EndAt.After(real.Start) {
				overlapsReal = true
				break
			}
		}
		remove := p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-off:") &&
			(p.StartAt.Before(gridStart) ||
				(evStart != nil && p.StartAt.Before(*evEnd) && p.EndAt.After(*evStart)) ||
				overlapsReal)
		if !remove {
			kept = append(kept, p)
		}
	}
	f.programs = kept

	written := 0
	for _, row := range rows {
		overlaps := false
		for _, real := range occupied {
			if row.StartAt.Before(real.End) && row.EndAt.After(real.Start) {
				overlaps = true
				break
			}
		}
		if overlaps {
			continue
		}
		found := -1
		for i := range f.programs {
			if f.programs[i].ChannelID == row.ChannelID && f.programs[i].StartAt.Equal(row.StartAt) {
				found = i
				break
			}
		}
		if found < 0 {
			f.programs = append(f.programs, row)
			written++
		} else if row.SourcePriority <= f.programs[found].SourcePriority && f.programs[found].SourceHash != row.SourceHash {
			f.programs[found] = row
			written++
		}
	}
	return written, true, nil
}

// fakeLookup: maps upstream URLs to canned provider stream rows.
type fakeLookup struct {
	rows map[string]StreamRow
	err  error
}

func (f *fakeLookup) BeginPass(ctx context.Context) error { return nil }
func (f *fakeLookup) EndPass()                            {}

func (f *fakeLookup) LookupBySource(ctx context.Context, src store.ChannelSource) (StreamRow, error) {
	if f.err != nil {
		return StreamRow{}, f.err
	}
	if r, ok := f.rows[src.UpstreamURL]; ok {
		return r, nil
	}
	return StreamRow{UpdatedAt: time.Now()}, ErrNotFound
}

func (f *fakeLookup) Close() {}

// TestRunOnce_LiveStreamInjects: a channel_source whose dispatcharr
// stream name parses to a live game with a future kickoff should be
// upserted as an epg_program row.
func TestRunOnce_LiveStreamInjects(t *testing.T) {
	chID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID:          uuid.New(),
				ChannelID:   chID,
				UpstreamURL: "http://iboost.example/stream/606180.ts",
				Enabled:     true,
			},
		},
	}
	// Use a name with a future kickoff so the parser yields status=live.
	// Use Tue 14 Apr 20:55 CEST — pinned by the parser's
	// year-rollover heuristic.
	fl := &fakeLookup{
		rows: map[string]StreamRow{
			"http://iboost.example/stream/606180.ts": {
				ID:        606180,
				Name:      "LIVE | EAGLES @ COWBOYS | Tue 14 Apr 20:55 CEST (DK) | 8K EXCLUSIVE | DK: VIAPLAY PPV 9",
				UpdatedAt: time.Now(),
			},
		},
	}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())

	// One real event row + the off-slot placeholder grid (4 blocks of
	// 6h, minus any that overlap the event window).
	var events, off []store.EPGProgram
	for _, p := range fs.programs {
		if strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			events = append(events, p)
		} else if strings.HasPrefix(p.SourceHash, "ppv-off:") {
			off = append(off, p)
		}
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 injected event, got %d (%+v)", len(events), fs.programs)
	}
	if len(off) < 2 || len(off) > 4 {
		t.Fatalf("expected 2-4 placeholder blocks, got %d", len(off))
	}
	prog := events[0]
	for _, o := range off {
		if o.StartAt.Before(prog.EndAt) && o.EndAt.After(prog.StartAt) {
			t.Errorf("placeholder %v-%v overlaps event %v-%v", o.StartAt, o.EndAt, prog.StartAt, prog.EndAt)
		}
		if o.SourcePriority != store.PriorityPPVOff {
			t.Errorf("placeholder priority got %d want %d", o.SourcePriority, store.PriorityPPVOff)
		}
	}
	if prog.ChannelID != chID {
		t.Errorf("channel id got %v want %v", prog.ChannelID, chID)
	}
	if !prog.IsLive {
		t.Error("expected IsLive=true on live stream")
	}
	if prog.Title != "EAGLES @ COWBOYS" {
		t.Errorf("title got %q", prog.Title)
	}
}

func TestRunOnce_ChannelPlaceholderPrefersKnownEventAcrossSources(t *testing.T) {
	const layout = "2006-01-02 15:04:05"
	start := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	stop := start.Add(3 * time.Hour)
	eventName := fmt.Sprintf(
		"UFC 00 : UFC 400 Main Card start:%s stop:%s",
		start.Format(layout), stop.Format(layout),
	)

	tests := []struct {
		name        string
		primaryName string
		fallback    string
		wantTitle   string
	}{
		{
			name:        "primary event beats idle fallback",
			primaryName: eventName,
			fallback:    "UFC 00 :",
			wantTitle:   "UFC 400 Main Card",
		},
		{
			name:        "fallback event beats idle primary",
			primaryName: "UFC 00 :",
			fallback:    eventName,
			wantTitle:   "UFC 400 Main Card",
		},
		{
			name:        "primary event wins divergent fallback event",
			primaryName: eventName,
			fallback: fmt.Sprintf(
				"UFC 00 : Wrong Fallback Card start:%s stop:%s",
				start.Format(layout), stop.Format(layout),
			),
			wantTitle: "UFC 400 Main Card",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chID := uuid.New()
			otherChannelID := uuid.New()
			primaryID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
			fallbackID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
			oldOverlap := store.EPGProgram{
				ChannelID: chID,
				StartAt:   start.Add(-30 * time.Minute),
				EndAt:     start.Add(30 * time.Minute),
				Title:     "Stale source placeholder",
				SourceHash: fmt.Sprintf(
					"ppv-off:xtream:999:%d",
					start.Add(-30*time.Minute).Unix(),
				),
				SourcePriority: store.PriorityPPVOff,
			}
			otherChannelRow := oldOverlap
			otherChannelRow.ChannelID = otherChannelID
			fs := &fakeStore{
				sources: []store.ChannelSource{
					{
						ID: primaryID, ChannelID: chID, Priority: 0,
						UpstreamURL: "http://iboost.example/stream/100.ts", Enabled: true,
					},
					{
						ID: fallbackID, ChannelID: chID, Priority: 1,
						UpstreamURL: "http://iboost.example/stream/101.ts", Enabled: true,
					},
				},
				programs: []store.EPGProgram{oldOverlap, otherChannelRow},
			}
			fl := &fakeLookup{rows: map[string]StreamRow{
				"http://iboost.example/stream/100.ts": {ID: 100, Name: tc.primaryName, UpdatedAt: time.Now()},
				"http://iboost.example/stream/101.ts": {ID: 101, Name: tc.fallback, UpdatedAt: time.Now()},
			}}
			w := New(fs, fl, nil)
			w.DisplayLocation = time.UTC
			w.runOnce(context.Background())

			var preRows, eventRows []store.EPGProgram
			staleOverlapFound := false
			otherChannelFound := false
			for _, p := range fs.programs {
				if p.ChannelID == chID && p.SourceHash == oldOverlap.SourceHash {
					staleOverlapFound = true
				}
				if p.ChannelID == otherChannelID && p.SourceHash == otherChannelRow.SourceHash {
					otherChannelFound = true
				}
				if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
					eventRows = append(eventRows, p)
				}
				if p.ChannelID != chID || !strings.HasPrefix(p.SourceHash, "ppv-off:") {
					continue
				}
				if !p.EndAt.After(start) {
					preRows = append(preRows, p)
				}
				if p.StartAt.Before(stop) && p.EndAt.After(start) {
					t.Errorf("placeholder %v-%v overlaps selected event %v-%v", p.StartAt, p.EndAt, start, stop)
				}
			}
			if len(preRows) == 0 {
				t.Fatalf("no pre-event status rows: %+v", fs.programs)
			}
			if len(eventRows) != 1 {
				t.Fatalf("redundant sources produced %d real event rows: %+v", len(eventRows), eventRows)
			}
			if eventRows[0].Title != tc.wantTitle {
				t.Errorf("kickoff row selected %q, pre-title expected %q", eventRows[0].Title, tc.wantTitle)
			}
			for _, p := range preRows {
				if !strings.HasPrefix(p.Title, "Next ") || !strings.Contains(p.Title, tc.wantTitle) {
					t.Errorf("pre-event row did not select exact event: %+v", p)
				}
				if strings.Contains(p.Title, "Wrong Fallback Card") {
					t.Errorf("lower-priority divergent event won: %+v", p)
				}
			}
			if staleOverlapFound {
				t.Error("channel-wide cleanup left an overlapping placeholder from another stream key")
			}
			if !otherChannelFound {
				t.Error("channel-wide cleanup escaped the selected channel")
			}
		})
	}
}

func TestRunOnce_ExactWinnerSweepsAmbiguousSourceStaleEvent(t *testing.T) {
	const layout = "2006-01-02 15:04:05"
	chID := uuid.New()
	start := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	stop := start.Add(3 * time.Hour)
	staleStart := start.Add(8 * time.Hour)
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), ChannelID: chID, Priority: 0,
				UpstreamURL: "http://iboost.example/stream/100.ts", Enabled: true,
			},
			{
				ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), ChannelID: chID, Priority: 1,
				UpstreamURL: "http://iboost.example/stream/101.ts", Enabled: true,
			},
		},
		programs: []store.EPGProgram{
			{
				ChannelID: chID, StartAt: staleStart, EndAt: staleStart.Add(3 * time.Hour),
				Title: "Stale Ambiguous Feed Event",
				SourceHash: fmt.Sprintf(
					"ppv-parse:xtream:101:%d:Stale Ambiguous Feed Event",
					staleStart.Unix(),
				),
				SourcePriority: store.PriorityPPVSync,
			},
		},
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		"http://iboost.example/stream/100.ts": {
			ID: 100,
			Name: fmt.Sprintf(
				"UFC 00 : UFC 400 Main Card start:%s stop:%s",
				start.Format(layout), stop.Format(layout),
			),
			UpdatedAt: time.Now(),
		},
		// Substantive but untimed: this source alone would preserve its
		// last-known row, but an exact channel-level winner supersedes it.
		"http://iboost.example/stream/101.ts": {
			ID: 101, Name: "PPV EVENT 05: UFC 330 Main Card", UpdatedAt: time.Now(),
		},
	}}

	w := New(fs, fl, nil)
	w.DisplayLocation = time.UTC
	w.runOnce(context.Background())

	var eventRows []store.EPGProgram
	for _, p := range fs.programs {
		if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			eventRows = append(eventRows, p)
		}
	}
	if len(eventRows) != 1 {
		t.Fatalf("exact winner left %d ppv-parse rows: %+v", len(eventRows), eventRows)
	}
	if eventRows[0].Title != "UFC 400 Main Card" || eventRows[0].StartAt != start {
		t.Fatalf("wrong channel-level winner: %+v", eventRows[0])
	}
}

func TestRunOnce_ExactReplacementFailurePreservesIdleSourcePriorEvent(t *testing.T) {
	const layout = "2006-01-02 15:04:05"
	chID := uuid.New()
	start := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	stop := start.Add(3 * time.Hour)
	priorStart := start.Add(-24 * time.Hour)
	priorHash := fmt.Sprintf(
		"ppv-parse:xtream:101:%d:Prior Known Event",
		priorStart.Unix(),
	)
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), ChannelID: chID, Priority: 0,
				UpstreamURL: "http://iboost.example/stream/100.ts", Enabled: true,
			},
			{
				ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), ChannelID: chID, Priority: 1,
				UpstreamURL: "http://iboost.example/stream/101.ts", Enabled: true,
			},
		},
		programs: []store.EPGProgram{
			{
				ChannelID: chID, StartAt: priorStart, EndAt: priorStart.Add(3 * time.Hour),
				Title: "Prior Known Event", SourceHash: priorHash,
				SourcePriority: store.PriorityPPVSync,
			},
		},
		replaceErr: errors.New("database unavailable"),
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		"http://iboost.example/stream/100.ts": {
			ID: 100,
			Name: fmt.Sprintf(
				"UFC 00 : UFC 400 Main Card start:%s stop:%s",
				start.Format(layout), stop.Format(layout),
			),
			UpdatedAt: time.Now(),
		},
		// This redundant source is definitively idle. Its prior row must
		// not be deleted unless the exact winner commits successfully.
		"http://iboost.example/stream/101.ts": {
			ID: 101, Name: "UFC 00 :", UpdatedAt: time.Now(),
		},
	}}

	w := New(fs, fl, nil)
	w.DisplayLocation = time.UTC
	w.runOnce(context.Background())

	var priorRows, nextRows, exactRows int
	for _, p := range fs.programs {
		if p.ChannelID != chID {
			continue
		}
		if p.SourceHash == priorHash {
			priorRows++
		}
		if strings.HasPrefix(p.SourceHash, "ppv-off:") {
			nextRows++
		}
		if strings.HasPrefix(p.SourceHash, "ppv-parse:xtream:100:") {
			exactRows++
		}
	}
	if priorRows != 1 || exactRows != 0 || nextRows != 0 {
		t.Fatalf(
			"failed exact replacement mutated guide state: prior=%d exact=%d off=%d programs=%+v",
			priorRows, exactRows, nextRows, fs.programs,
		)
	}
}

// TestRunOnce_ProviderNotFound_Skips: when the provider catalogue has no row
// for a URL, the worker logs+continues without injecting.
func TestRunOnce_ProviderNotFound_Skips(t *testing.T) {
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID:          uuid.New(),
				ChannelID:   uuid.New(),
				UpstreamURL: "http://unknown",
				Enabled:     true,
			},
		},
	}
	fl := &fakeLookup{rows: map[string]StreamRow{}}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())
	if len(fs.programs) != 0 {
		t.Errorf("expected 0 programs, got %d", len(fs.programs))
	}
}

// TestRunOnce_DisabledSource_Skipped: disabled channel_source rows
// don't even get a dispatcharr lookup (cheap optimisation but worth
// checking).
func TestRunOnce_DisabledSource_Skipped(t *testing.T) {
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID:          uuid.New(),
				ChannelID:   uuid.New(),
				UpstreamURL: "http://disabled",
				Enabled:     false,
			},
		},
	}
	fl := &fakeLookup{}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())
	if len(fs.programs) != 0 {
		t.Errorf("expected disabled to be skipped; got %d programs", len(fs.programs))
	}
}

// TestRunOnce_UnparseableName_Skipped: when dispatcharr's stream name
// is one of the static (non-PPV-formatted) names, parser returns
// false and the worker skips.
func TestRunOnce_UnparseableName_Skipped(t *testing.T) {
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID:          uuid.New(),
				ChannelID:   uuid.New(),
				UpstreamURL: "http://static",
				Enabled:     true,
			},
		},
	}
	fl := &fakeLookup{
		rows: map[string]StreamRow{
			"http://static": {ID: 1, Name: "VIP: CANAL+ LIVE 12 ᴿᴬᵂ"},
		},
	}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())
	if len(fs.programs) != 0 {
		t.Errorf("expected static name to be skipped; got %d", len(fs.programs))
	}
}

// TestRunOnce_ProviderErrorLogged: a non-NotFound error from
// dispatcharr (e.g. connection drop) gets logged but doesn't crash
// the pass.
func TestRunOnce_ProviderErrorLogged(t *testing.T) {
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID:          uuid.New(),
				ChannelID:   uuid.New(),
				UpstreamURL: "http://broken",
				Enabled:     true,
			},
		},
	}
	fl := &fakeLookup{err: errors.New("connection refused")}
	w := New(fs, fl, nil)
	// Should NOT panic; should NOT inject anything.
	w.runOnce(context.Background())
	if len(fs.programs) != 0 {
		t.Errorf("expected 0 programs, got %d", len(fs.programs))
	}
}

// TestRunOnce_StaleSweepRemovesYesterdayParse exercises the day-
// boundary cleanup. Pre-seed a fakeStore with a stale ppv-parse row
// from "yesterday" (same channel + same stream id but different
// kickoff). Run the worker once: the channel-level replacement atomically
// installs the fresh winner and removes the stale row.
func TestRunOnce_StaleSweepRemovesYesterdayParse(t *testing.T) {
	chID := uuid.MustParse("88888888-8888-8888-8888-888888888888")
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{
				ID:          uuid.New(),
				ChannelID:   chID,
				UpstreamURL: "http://iboost.example/stream/606171.ts",
				Enabled:     true,
			},
		},
		// Pre-seed: yesterday's parse for the same stream, kickoff in the past.
		programs: []store.EPGProgram{
			{
				ChannelID:  chID,
				StartAt:    time.Now().Add(-22 * time.Hour).Truncate(time.Second),
				EndAt:      time.Now().Add(-19 * time.Hour),
				Title:      "Penguins @ Blues",
				SourceHash: "ppv-parse:xtream:606171:1700000000:Penguins @ Blues",
			},
		},
	}
	// Fresh name with an explicit FUTURE start/stop so the parse is
	// deterministically upcoming regardless of the wall-clock the test runs at.
	start := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	freshName := fmt.Sprintf("NHL 08 : Penguins @ Blues start:%s stop:%s",
		start.Format("2006-01-02 15:04:05"),
		start.Add(3*time.Hour).Format("2006-01-02 15:04:05"))
	fl := &fakeLookup{
		rows: map[string]StreamRow{
			"http://iboost.example/stream/606171.ts": {
				ID:        606171,
				Name:      freshName,
				UpdatedAt: time.Now(),
			},
		},
	}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())

	// Exactly one row remains for this channel+stream: channel-level atomic
	// replacement installed the fresh winner and removed yesterday's row.
	count := 0
	for _, p := range fs.programs {
		if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-parse:xtream:606171:") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 ppv-parse row after replacement; got %d. programs=%+v", count, fs.programs)
	}
}

// TestRunOnce_OffStateSweepsStaleEvent: when a slot that previously
// carried an event reverts to an off-state name, the worker must sweep
// the stale ppv-parse row so the channel resets to "No Event Scheduled"
// instead of freezing on the finished game (2026-07-20 report). Pre-seed
// a ppv-parse row, feed an off-state name, assert the row is gone and
// placeholders replace it.
func TestRunOnce_OffStateSweepsStaleEvent(t *testing.T) {
	chID := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{ID: uuid.New(), ChannelID: chID, UpstreamURL: "http://iboost.example/stream/605190.ts", Enabled: true},
		},
		programs: []store.EPGProgram{
			{
				ChannelID:  chID,
				StartAt:    time.Now().Add(-2 * time.Hour),
				EndAt:      time.Now().Add(1 * time.Hour),
				Title:      "Penguins @ Blues",
				SourceHash: "ppv-parse:xtream:605190:1700000000:Penguins @ Blues",
			},
		},
	}
	fl := &fakeLookup{
		rows: map[string]StreamRow{
			// Off-state: the game ended, slot reverted to the idle shape.
			"http://iboost.example/stream/605190.ts": {ID: 605190, Name: "NHL | 08 -", UpdatedAt: time.Now()},
		},
	}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())

	var parse, off int
	for _, p := range fs.programs {
		if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			parse++
		} else if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-off:") {
			off++
		}
	}
	if parse != 0 {
		t.Errorf("stale event row not swept on off-state; %d ppv-parse rows remain: %+v", parse, fs.programs)
	}
	if off == 0 {
		t.Error("expected off-slot placeholder rows after off-state pass")
	}
}

// TestRunOnce_AmbiguousOffKeepsEvent: when a slot's name loses its time to
// iboost's length cap ("PPV EVENT 05: UFC 330 Main Card" — no "(…ET)"), it
// parses StatusOff but OffIdle=false. The worker must NOT sweep the stored
// upcoming event — the fight is still scheduled, the name is just truncated.
func TestRunOnce_AmbiguousOffKeepsEvent(t *testing.T) {
	chID := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{ID: uuid.New(), ChannelID: chID, Priority: 0, UpstreamURL: "http://iboost.example/stream/1855426.ts", Enabled: true},
			{ID: uuid.New(), ChannelID: chID, Priority: 1, UpstreamURL: "http://iboost.example/stream/2855426.ts", Enabled: true},
		},
		programs: []store.EPGProgram{
			{
				ChannelID:  chID,
				StartAt:    time.Now().Add(3 * time.Hour),
				EndAt:      time.Now().Add(6 * time.Hour),
				Title:      "UFC 330 Main Card",
				SourceHash: "ppv-parse:xtream:1855426:1800000000:UFC 330 Main Card",
			},
			{
				ChannelID:  chID,
				StartAt:    time.Now().Add(4 * time.Hour),
				EndAt:      time.Now().Add(7 * time.Hour),
				Title:      "Fallback Last Known Card",
				SourceHash: "ppv-parse:xtream:2855426:1800003600:Fallback Last Known Card",
			},
		},
	}
	fl := &fakeLookup{
		rows: map[string]StreamRow{
			"http://iboost.example/stream/1855426.ts": {ID: 1855426, Name: "PPV EVENT 05: UFC 330 Main Card", UpdatedAt: time.Now()},
			"http://iboost.example/stream/2855426.ts": {ID: 2855426, Name: "PPV EVENT 05: Fallback Last Known Card", UpdatedAt: time.Now()},
		},
	}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())

	parse := 0
	for _, p := range fs.programs {
		if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			parse++
		}
	}
	if parse != 2 {
		t.Errorf("all-ambiguous sources must preserve last-known events; got %d ppv-parse rows: %+v", parse, fs.programs)
	}
}

// TestRunOnce_SiblingFailoverSourcesSelectOneStableWinner guards the v0.36.7
// channel-level replacement architecture retained during the rebase. Redundant
// sources for the same event must yield one deterministic winner, not competing
// rows that flip-flop or lose enrichment on later passes.
func TestRunOnce_SiblingFailoverSourcesSelectOneStableWinner(t *testing.T) {
	chID := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	primaryID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	failoverID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{ID: primaryID, ChannelID: chID, Priority: 0, UpstreamURL: "http://iboost.example/primary.ts", Enabled: true},
			{ID: failoverID, ChannelID: chID, Priority: 10, UpstreamURL: "http://iboost.example/failover.ts", Enabled: true},
		},
	}
	// Both sources parse the same event at the same explicit future kickoff but
	// resolve to different provider stream IDs.
	start := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	name := fmt.Sprintf("NHL 08 : Penguins @ Blues start:%s stop:%s",
		start.Format("2006-01-02 15:04:05"),
		start.Add(3*time.Hour).Format("2006-01-02 15:04:05"))
	fl := &fakeLookup{
		rows: map[string]StreamRow{
			"http://iboost.example/primary.ts":  {ID: 606180, Name: name, UpdatedAt: time.Now()},
			"http://iboost.example/failover.ts": {ID: 999999, Name: name, UpdatedAt: time.Now()},
		},
	}
	w := New(fs, fl, nil)
	w.runOnce(context.Background())
	w.runOnce(context.Background())

	var eventRows []store.EPGProgram
	for _, p := range fs.programs {
		if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			eventRows = append(eventRows, p)
		}
	}
	if len(eventRows) != 1 {
		t.Fatalf("channel must retain one parsed winner; got %d rows: %+v", len(eventRows), eventRows)
	}
	if !strings.HasPrefix(eventRows[0].SourceHash, "ppv-parse:xtream:606180:") {
		t.Fatalf("lower-priority primary did not remain the stable winner: %+v", eventRows[0])
	}
}

func TestRunOnce_LastGoodSlotPreservesGuideOnTemporaryCatalogueLoss(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]StreamRow, string)
	}{
		{
			name: "not found",
			mutate: func(rows map[string]StreamRow, url string) {
				delete(rows, url)
			},
		},
		{
			name: "unparseable rename",
			mutate: func(rows map[string]StreamRow, url string) {
				rows[url] = StreamRow{ID: 606181, Name: "VIP: STATIC CHANNEL RAW", UpdatedAt: time.Now()}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceID := uuid.New()
			channelID := uuid.New()
			url := "http://iboost.example/stream/606181.ts"
			fs := &fakeStore{sources: []store.ChannelSource{{
				ID: sourceID, ChannelID: channelID, UpstreamURL: url, Enabled: true,
			}}}
			fl := &fakeLookup{rows: map[string]StreamRow{
				url: {ID: 606181, Name: "NHL | 08 -", UpdatedAt: time.Now()},
			}}
			w := New(fs, fl, nil)

			w.runOnce(context.Background())
			rememberedSlot := w.lastGoodSlot[sourceID]
			if rememberedSlot == "" {
				t.Fatal("successful parse did not remember its slot")
			}

			fs.mu.Lock()
			fs.programs = nil
			fs.mu.Unlock()
			tt.mutate(fl.rows, url)
			w.runOnce(context.Background())

			fs.mu.Lock()
			programs := append([]store.EPGProgram(nil), fs.programs...)
			fs.mu.Unlock()
			prefix := "ppv-off:src:" + sourceID.String() + ":"
			found := false
			for _, p := range programs {
				if strings.HasPrefix(p.SourceHash, prefix) {
					found = true
					if !strings.Contains(p.Title, rememberedSlot) {
						t.Errorf("continuity row title %q lost remembered slot %q", p.Title, rememberedSlot)
					}
				}
			}
			if !found {
				t.Fatalf("temporary catalogue loss blanked the channel; programs=%+v", programs)
			}
		})
	}
}

func TestRunOnce_CurrentParsedSiblingBeatsRememberedFallback(t *testing.T) {
	channelID := uuid.New()
	primaryID := uuid.New()
	fallbackID := uuid.New()
	primaryURL := "http://iboost.example/primary-missing.ts"
	fallbackURL := "http://iboost.example/fallback-current.ts"
	fs := &fakeStore{sources: []store.ChannelSource{
		{ID: primaryID, ChannelID: channelID, Priority: -10, UpstreamURL: primaryURL, Enabled: true},
		{ID: fallbackID, ChannelID: channelID, Priority: 10, UpstreamURL: fallbackURL, Enabled: true},
	}}
	start := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	name := fmt.Sprintf("NHL 08 : Penguins @ Blues start:%s stop:%s",
		start.Format("2006-01-02 15:04:05"),
		start.Add(3*time.Hour).Format("2006-01-02 15:04:05"))
	fl := &fakeLookup{rows: map[string]StreamRow{
		fallbackURL: {ID: 999999, Name: name, UpdatedAt: time.Now()},
	}}
	w := New(fs, fl, nil)
	w.lastGoodSlot[primaryID] = "NHL PPV 08"
	w.runOnce(context.Background())

	var events []store.EPGProgram
	for _, p := range fs.programs {
		if strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			events = append(events, p)
		}
		if strings.HasPrefix(p.SourceHash, "ppv-off:src:"+primaryID.String()+":") {
			t.Errorf("remembered primary overrode a current parsed sibling: %+v", p)
		}
	}
	if len(events) != 1 || !strings.HasPrefix(events[0].SourceHash, "ppv-parse:xtream:999999:") {
		t.Fatalf("current parsed sibling was not selected: %+v", fs.programs)
	}
}

func TestRunOnce_OffGridRespectsStoredManualOccupancy(t *testing.T) {
	channelID := uuid.New()
	gridStart := time.Now().UTC().Truncate(6 * time.Hour)
	manual := store.EPGProgram{
		ChannelID: channelID, StartAt: gridStart.Add(time.Hour), EndAt: gridStart.Add(2 * time.Hour),
		Title: "Manual UFC card", SourceHash: "manual:ufc-card", SourcePriority: -50,
	}
	oldOverlap := store.EPGProgram{
		ChannelID: channelID, StartAt: gridStart, EndAt: gridStart.Add(6 * time.Hour),
		Title: "No Event Scheduled", SourceHash: "ppv-off:old:overlap", SourcePriority: store.PriorityPPVOff,
	}
	url := "http://iboost.example/stream/606182.ts"
	fs := &fakeStore{
		sources:  []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
		programs: []store.EPGProgram{manual, oldOverlap},
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: 606182, Name: "NHL | 08 -", UpdatedAt: time.Now()},
	}}
	New(fs, fl, nil).runOnce(context.Background())

	manualFound := false
	for _, p := range fs.programs {
		if p.SourceHash == manual.SourceHash {
			manualFound = true
		}
		if strings.HasPrefix(p.SourceHash, "ppv-off:") &&
			p.StartAt.Before(manual.EndAt) && p.EndAt.After(manual.StartAt) {
			t.Errorf("placeholder still overlaps the stored manual row: %+v", p)
		}
	}
	if !manualFound {
		t.Fatal("occupancy cleanup removed the stronger manual row")
	}
}

func TestRunOnce_OccupancyReadFailureIsFailClosed(t *testing.T) {
	channelID := uuid.New()
	url := "http://iboost.example/stream/606183.ts"
	fs := &fakeStore{
		sources:     []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
		occupiedErr: errors.New("occupancy unavailable"),
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: 606183, Name: "NHL | 08 -", UpdatedAt: time.Now()},
	}}
	New(fs, fl, nil).runOnce(context.Background())

	for _, p := range fs.programs {
		if strings.HasPrefix(p.SourceHash, "ppv-off:") {
			t.Fatalf("occupancy failure wrote an unchecked placeholder: %+v", p)
		}
	}
}

func TestRunOnce_LiveWinnerExtendsBeforePlaceholderGeneration(t *testing.T) {
	channelID := uuid.New()
	url := "http://iboost.example/stream/606184.ts"
	now := time.Now().UTC()
	start := now.Add(-2 * time.Hour).Truncate(time.Second)
	stop := now.Add(10 * time.Minute).Truncate(time.Second)
	name := fmt.Sprintf("NHL 08 : Penguins @ Blues start:%s stop:%s",
		start.Format("2006-01-02 15:04:05"), stop.Format("2006-01-02 15:04:05"))
	fs := &fakeStore{sources: []store.ChannelSource{{
		ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true,
	}}}
	fl := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: 606184, Name: name, UpdatedAt: time.Now()},
	}}
	New(fs, fl, nil).runOnce(context.Background())

	var event *store.EPGProgram
	for i := range fs.programs {
		p := &fs.programs[i]
		if strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			event = p
		}
	}
	if event == nil {
		t.Fatalf("live event was not stored: %+v", fs.programs)
	}
	if event.EndAt.Before(time.Now().Add(80 * time.Minute)) {
		t.Fatalf("live event end was not extended: got %v", event.EndAt)
	}
	for _, p := range fs.programs {
		if strings.HasPrefix(p.SourceHash, "ppv-off:") &&
			p.StartAt.Before(event.EndAt) && p.EndAt.After(event.StartAt) {
			t.Errorf("placeholder overlaps extended live winner: %+v", p)
		}
	}
}

// TestRunOnce_AllLookupsDead_MarksMonitorFailure: the 2026-06-07
// failure shape — every catalogue lookup errors — must mark the
// monitor failed so the stall alert fires.
func TestRunOnce_AllLookupsDead_MarksMonitorFailure(t *testing.T) {
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{ID: uuid.New(), ChannelID: uuid.New(), UpstreamURL: "http://iboost.example/a.ts", Enabled: true},
			{ID: uuid.New(), ChannelID: uuid.New(), UpstreamURL: "http://iboost.example/b.ts", Enabled: true},
		},
	}
	fl := &fakeLookup{err: errors.New("dial tcp: dispatcharr gone")}
	w := New(fs, fl, nil)
	w.Monitor = alerts.NewWorkerMonitor("ppvsync", time.Nanosecond, nil)
	w.Monitor.Start()
	time.Sleep(time.Millisecond) // ensure the stall deadline has elapsed

	w.runOnce(context.Background())

	snap := w.Monitor.Snapshot()
	if !snap.Stalled {
		t.Fatalf("expected stalled monitor, got %+v", snap)
	}
}

// TestRunOnce_NotFoundIsAnAnswer_MarksMonitorSuccess: ErrNotFound means
// dispatcharr answered — the worker is healthy even with zero matches.
func TestRunOnce_NotFoundIsAnAnswer_MarksMonitorSuccess(t *testing.T) {
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{ID: uuid.New(), ChannelID: uuid.New(), UpstreamURL: "http://iboost.example/a.ts", Enabled: true},
		},
	}
	fl := &fakeLookup{} // empty rows → ErrNotFound for every URL
	w := New(fs, fl, nil)
	w.Monitor = alerts.NewWorkerMonitor("ppvsync", time.Nanosecond, nil)
	w.Monitor.Start()
	time.Sleep(time.Millisecond)

	w.runOnce(context.Background())

	snap := w.Monitor.Snapshot()
	if snap.Stalled {
		t.Fatalf("ErrNotFound must count as an answer: %+v", snap)
	}
}

func overrunFixture(t *testing.T, channelID uuid.UUID, streamID int64, title string, endOffset time.Duration) (string, store.EPGProgram) {
	t.Helper()
	start := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	stop := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	name := fmt.Sprintf("NHL 08 : %s start:%s stop:%s",
		title,
		start.Format("2006-01-02 15:04:05"),
		stop.Format("2006-01-02 15:04:05"),
	)
	parsed, ok := ppvparse.Parse(name)
	if !ok || parsed.Status != ppvparse.StatusEnded || parsed.EndExplicit {
		t.Fatalf("overrun fixture did not produce a clock-elapsed event: ok=%v parsed=%+v", ok, parsed)
	}
	program, ok := ppvparse.EPGProgramIdentity(channelID, parsed, fmt.Sprintf("xtream:%d", streamID))
	if !ok {
		t.Fatal("overrun fixture identity failed")
	}
	program.EndAt = time.Now().Add(endOffset)
	return name, program
}

func TestRunOnce_UnchangedElapsedNamePreservesActiveOverrun(t *testing.T) {
	channelID := uuid.New()
	const streamID int64 = 606185
	url := "http://iboost.example/stream/606185.ts"
	name, overrun := overrunFixture(t, channelID, streamID, "Penguins @ Blues", 5*time.Minute)
	sibling := overrun
	sibling.StartAt = overrun.StartAt.Add(-24 * time.Hour)
	sibling.EndAt = sibling.StartAt.Add(3 * time.Hour)
	sibling.SourceHash = "ppv-parse:xtream:999999:stale-sibling"
	fs := &fakeStore{
		sources:  []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
		programs: []store.EPGProgram{overrun, sibling},
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: streamID, Name: name, UpdatedAt: time.Now()},
	}}
	w := New(fs, fl, nil)

	// Multiple unchanged-provider passes must retain the exact overrun identity;
	// the first refreshes its nearly-expired extension, the second preserves it.
	w.runOnce(context.Background())
	w.runOnce(context.Background())

	var parsedRows []store.EPGProgram
	for _, p := range fs.programs {
		if p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			parsedRows = append(parsedRows, p)
		}
	}
	if len(parsedRows) != 1 || parsedRows[0].SourceHash != overrun.SourceHash {
		t.Fatalf("unchanged elapsed name did not retain one exact winner: %+v", parsedRows)
	}
	if parsedRows[0].EndAt.Before(time.Now().Add(80 * time.Minute)) {
		t.Fatalf("active overrun was not refreshed near expiry: end=%v", parsedRows[0].EndAt)
	}
}

func TestRunOnce_CurrentEventBeatsHigherPriorityOverrun(t *testing.T) {
	channelID := uuid.New()
	primaryURL := "http://iboost.example/primary-overrun.ts"
	fallbackURL := "http://iboost.example/fallback-current.ts"
	primaryName, overrun := overrunFixture(t, channelID, 606190, "Old Overrun", 90*time.Minute)
	futureStart := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	futureName := fmt.Sprintf("NHL 08 : Current Fresh Event start:%s stop:%s",
		futureStart.Format("2006-01-02 15:04:05"),
		futureStart.Add(3*time.Hour).Format("2006-01-02 15:04:05"),
	)
	fs := &fakeStore{
		sources: []store.ChannelSource{
			{ID: uuid.New(), ChannelID: channelID, Priority: -10, UpstreamURL: primaryURL, Enabled: true},
			{ID: uuid.New(), ChannelID: channelID, Priority: 10, UpstreamURL: fallbackURL, Enabled: true},
		},
		programs: []store.EPGProgram{overrun},
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		primaryURL:  {ID: 606190, Name: primaryName, UpdatedAt: time.Now()},
		fallbackURL: {ID: 606191, Name: futureName, UpdatedAt: time.Now()},
	}}
	New(fs, fl, nil).runOnce(context.Background())

	var parsedRows []store.EPGProgram
	for _, p := range fs.programs {
		if p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			parsedRows = append(parsedRows, p)
		}
	}
	if len(parsedRows) != 1 || parsedRows[0].Title != "Current Fresh Event" ||
		!strings.HasPrefix(parsedRows[0].SourceHash, "ppv-parse:xtream:606191:") {
		t.Fatalf("current event did not supersede higher-priority overrun: %+v", parsedRows)
	}
}

func TestRunOnce_OverrunReadWriteFailuresStayFailClosed(t *testing.T) {
	tests := []struct {
		name            string
		configure       func(*fakeStore)
		wantExistingRow bool
	}{
		{
			name: "probe error preserves committed row",
			configure: func(fs *fakeStore) {
				fs.overrunProbeErr = errors.New("probe unavailable")
			},
			wantExistingRow: true,
		},
		{
			name: "preserve transaction error preserves committed row",
			configure: func(fs *fakeStore) {
				fs.overrunPreserveErr = errors.New("preserve unavailable")
			},
			wantExistingRow: true,
		},
		{
			name: "identity lost after probe is not reinserted",
			configure: func(fs *fakeStore) {
				fs.dropOverrunBeforePreserve = true
			},
			wantExistingRow: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			channelID := uuid.New()
			const streamID int64 = 606189
			url := "http://iboost.example/stream/606189.ts"
			name, overrun := overrunFixture(t, channelID, streamID, "Penguins @ Blues", 20*time.Minute)
			fs := &fakeStore{
				sources:  []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
				programs: []store.EPGProgram{overrun},
			}
			tc.configure(fs)
			fl := &fakeLookup{rows: map[string]StreamRow{
				url: {ID: streamID, Name: name, UpdatedAt: time.Now()},
			}}
			New(fs, fl, nil).runOnce(context.Background())

			found := false
			for _, p := range fs.programs {
				if p.SourceHash == overrun.SourceHash {
					found = true
				}
				if strings.HasPrefix(p.SourceHash, "ppv-off:") {
					t.Fatalf("uncertain overrun state emitted a placeholder: %+v", p)
				}
			}
			if found != tc.wantExistingRow {
				t.Fatalf("existing-row presence=%v, want %v; programs=%+v", found, tc.wantExistingRow, fs.programs)
			}
		})
	}
}

func TestRunOnce_ProviderEndedDoesNotResurrectExtendedRow(t *testing.T) {
	channelID := uuid.New()
	const streamID int64 = 606186
	url := "http://iboost.example/stream/606186.ts"
	start := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	date := start.Format("2006-01-02")
	clock := start.Format("15:04")
	liveName := fmt.Sprintf("LIVE | Team A vs Team B | %s | %s (GMT) | US: DAZN PPV 7", date, clock)
	endedName := fmt.Sprintf("ENDED | Team A vs Team B | %s | %s (GMT) | US: DAZN PPV 7", date, clock)
	parsed, ok := ppvparse.Parse(liveName)
	if !ok {
		t.Fatal("live fixture did not parse")
	}
	program, ok := ppvparse.EPGProgramIdentity(channelID, parsed, fmt.Sprintf("xtream:%d", streamID))
	if !ok {
		t.Fatal("live fixture identity failed")
	}
	program.EndAt = time.Now().Add(90 * time.Minute)
	fs := &fakeStore{
		sources:  []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
		programs: []store.EPGProgram{program},
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: streamID, Name: endedName, UpdatedAt: time.Now()},
	}}
	New(fs, fl, nil).runOnce(context.Background())

	for _, p := range fs.programs {
		if p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			t.Fatalf("provider ENDED declaration resurrected/preserved an extended row: %+v", p)
		}
	}
}

func TestRunOnce_DefinitiveIdleClearsExtendedRow(t *testing.T) {
	channelID := uuid.New()
	const streamID int64 = 606188
	url := "http://iboost.example/stream/606188.ts"
	_, program := overrunFixture(t, channelID, streamID, "Prior Event", 90*time.Minute)
	fs := &fakeStore{
		sources:  []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
		programs: []store.EPGProgram{program},
	}
	fl := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: streamID, Name: "NHL | 08 -", UpdatedAt: time.Now()},
	}}
	New(fs, fl, nil).runOnce(context.Background())
	for _, p := range fs.programs {
		if p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			t.Fatalf("definitive idle state preserved an extended event: %+v", p)
		}
	}
}

func TestRunOnce_ChangedOrExpiredElapsedIdentityIsNotResurrected(t *testing.T) {
	tests := []struct {
		name       string
		changeName bool
		extended   time.Duration
	}{
		{name: "renamed", changeName: true, extended: 90 * time.Minute},
		{name: "extension expired", extended: -time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			channelID := uuid.New()
			const streamID int64 = 606187
			url := "http://iboost.example/stream/606187.ts"
			name, oldProgram := overrunFixture(t, channelID, streamID, "Original Event", tc.extended)
			if tc.changeName {
				name = strings.Replace(name, "Original Event", "Changed Event", 1)
			}
			fs := &fakeStore{
				sources:  []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
				programs: []store.EPGProgram{oldProgram},
			}
			fl := &fakeLookup{rows: map[string]StreamRow{
				url: {ID: streamID, Name: name, UpdatedAt: time.Now()},
			}}
			New(fs, fl, nil).runOnce(context.Background())

			for _, p := range fs.programs {
				if p.ChannelID == channelID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
					t.Fatalf("changed/expired identity was resurrected: %+v", p)
				}
			}
		})
	}
}

type replacementBeforeExtendStore struct {
	*fakeStore
	replacement store.EPGProgram
}

func (f *replacementBeforeExtendStore) ExtendPPVLiveRowEnd(
	ctx context.Context,
	channelID uuid.UUID,
	startAt time.Time,
	sourceHash string,
	observedAt, triggerBefore, newEnd, hardEnd time.Time,
) (int64, bool, error) {
	// Simulate another worker committing a same-slot rename after this worker's
	// ReplacePPVParsedRowsForChannel transaction but before its extension call.
	f.mu.Lock()
	for i := range f.programs {
		if f.programs[i].ChannelID == channelID && f.programs[i].StartAt.Equal(startAt) {
			f.programs[i] = f.replacement
			break
		}
	}
	f.mu.Unlock()
	return f.fakeStore.ExtendPPVLiveRowEnd(ctx, channelID, startAt, sourceHash, observedAt, triggerBefore, newEnd, hardEnd)
}

func TestRunOnce_StaleWinnerCannotExtendConcurrentReplacement(t *testing.T) {
	channelID := uuid.New()
	const streamID int64 = 606192
	url := "http://iboost.example/stream/606192.ts"
	start := time.Now().UTC().Add(-2*time.Hour - 50*time.Minute).Truncate(time.Second)
	stop := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
	name := fmt.Sprintf("NHL 08 : Original Event start:%s stop:%s",
		start.Format("2006-01-02 15:04:05"),
		stop.Format("2006-01-02 15:04:05"),
	)
	replacementEnd := time.Now().Add(10 * time.Minute)
	replacement := store.EPGProgram{
		ChannelID:      channelID,
		StartAt:        start,
		EndAt:          replacementEnd,
		Title:          "Concurrent Replacement",
		SourceHash:     fmt.Sprintf("ppv-parse:xtream:999999:%d:Concurrent Replacement", start.Unix()),
		SourcePriority: store.PriorityPPVSync,
	}
	fs := &replacementBeforeExtendStore{
		fakeStore: &fakeStore{sources: []store.ChannelSource{{
			ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true,
		}}},
		replacement: replacement,
	}
	lookup := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: streamID, Name: name, UpdatedAt: time.Now()},
	}}
	New(fs, lookup, nil).runOnce(context.Background())

	foundReplacement := false
	for _, p := range fs.programs {
		if p.SourceHash == replacement.SourceHash {
			foundReplacement = true
			if !p.EndAt.Equal(replacementEnd) {
				t.Fatalf("stale winner extended concurrent replacement: end=%v want %v", p.EndAt, replacementEnd)
			}
		}
	}
	if !foundReplacement {
		t.Fatal("synthetic concurrent replacement was not installed")
	}
}

type newerEventBeforeIdleDeleteStore struct {
	*fakeStore
	once          sync.Once
	replacement   store.EPGProgram
	newObservedAt time.Time
	injectedErr   error
}

func (f *newerEventBeforeIdleDeleteStore) DeletePPVParsedRowsForChannelStream(
	ctx context.Context,
	channelID uuid.UUID,
	streamHashKey string,
	observedAt time.Time,
) (int64, bool, error) {
	f.once.Do(func() {
		_, _, applied, err := f.fakeStore.ReplacePPVParsedRowsForChannel(ctx, f.replacement, f.newObservedAt)
		if err != nil {
			f.injectedErr = err
		} else if !applied {
			f.injectedErr = errors.New("newer synthetic replacement was rejected")
		}
	})
	if f.injectedErr != nil {
		return 0, false, f.injectedErr
	}
	return f.fakeStore.DeletePPVParsedRowsForChannelStream(ctx, channelID, streamHashKey, observedAt)
}

func TestRunOnce_StaleIdleCannotDeleteOrCoverNewerExactEvent(t *testing.T) {
	channelID := uuid.New()
	sourceID := uuid.New()
	const streamID int64 = 606194
	url := "http://iboost.example/stream/606194.ts"
	oldObservedAt := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newObservedAt := oldObservedAt.Add(time.Minute)
	start := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	replacement := store.EPGProgram{
		ChannelID:      channelID,
		StartAt:        start,
		EndAt:          start.Add(3 * time.Hour),
		Title:          "Newer Exact Event",
		SourceHash:     fmt.Sprintf("ppv-parse:xtream:%d:%d:Newer Exact Event", streamID, start.Unix()),
		SourcePriority: store.PriorityPPVSync,
	}
	fs := &newerEventBeforeIdleDeleteStore{
		fakeStore: &fakeStore{sources: []store.ChannelSource{{
			ID: sourceID, ChannelID: channelID, UpstreamURL: url, Enabled: true,
		}}},
		replacement:   replacement,
		newObservedAt: newObservedAt,
	}
	lookup := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: streamID, Name: "NHL | 08 -", UpdatedAt: oldObservedAt},
	}}
	New(fs, lookup, nil).runOnce(context.Background())

	if fs.injectedErr != nil {
		t.Fatal(fs.injectedErr)
	}
	found := false
	for _, p := range fs.programs {
		if p.SourceHash == replacement.SourceHash {
			found = true
		}
		if strings.HasPrefix(p.SourceHash, "ppv-off:") {
			t.Fatalf("stale idle snapshot published a placeholder over newer state: %+v", p)
		}
	}
	if !found {
		t.Fatalf("stale idle snapshot deleted newer exact event: %+v", fs.programs)
	}
}

func TestRunOnce_SelectedSourceKeepsItsOwnObservationGeneration(t *testing.T) {
	chID := uuid.MustParse("abababab-abab-abab-abab-abababababab")
	primaryID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	fallbackID := uuid.MustParse("66666666-7777-8888-9999-aaaaaaaaaaaa")
	oldObservedAt := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newObservedAt := oldObservedAt.Add(time.Minute)
	primaryURL := "http://iboost.example/stream/606210.ts"
	fallbackURL := "http://iboost.example/stream/606211.ts"
	fs := &fakeStore{sources: []store.ChannelSource{
		{ID: primaryID, ChannelID: chID, UpstreamURL: primaryURL, Priority: 0, Enabled: true},
		{ID: fallbackID, ChannelID: chID, UpstreamURL: fallbackURL, Priority: 1, Enabled: true},
	}}
	lookup := &fakeLookup{rows: map[string]StreamRow{
		primaryURL: {
			ID: 606210, Name: "NHL 08 : Primary Event start:2090-08-13 20:00:00 stop:2090-08-13 23:00:00",
			UpdatedAt: oldObservedAt,
		},
		fallbackURL: {
			ID: 606211, Name: "NHL 08 : Fallback Event start:2090-08-13 20:00:00 stop:2090-08-13 23:00:00",
			UpdatedAt: newObservedAt,
		},
	}}
	w := New(fs, lookup, nil)
	w.runOnce(context.Background())

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if got := fs.latestObservation[chID]; !got.Equal(oldObservedAt) {
		t.Fatalf("selected T1 source was relabelled as another source's T2 generation: got=%v want=%v", got, oldObservedAt)
	}
	found := false
	for _, p := range fs.programs {
		if p.ChannelID == chID && strings.HasPrefix(p.SourceHash, "ppv-parse:") {
			found = true
			if p.Title != "Primary Event" {
				t.Fatalf("selected title=%q want primary", p.Title)
			}
		}
	}
	if !found {
		t.Fatal("selected primary event was not published")
	}
}

func TestRunOnce_UnchangedOverrunExpiresAtFiniteHorizon(t *testing.T) {
	channelID := uuid.New()
	const streamID int64 = 606193
	url := "http://iboost.example/stream/606193.ts"
	clock := time.Now().UTC().Truncate(time.Second)
	start := clock.Add(-3 * time.Hour)
	nominalEnd := clock.Add(-time.Minute)
	name := fmt.Sprintf("NHL 08 : Stuck Provider Name start:%s stop:%s",
		start.Format("2006-01-02 15:04:05"),
		nominalEnd.Format("2006-01-02 15:04:05"),
	)
	parsed, ok := ppvparse.Parse(name)
	if !ok || parsed.Status != ppvparse.StatusEnded {
		t.Fatalf("elapsed fixture did not parse: ok=%v parsed=%+v", ok, parsed)
	}
	program, ok := ppvparse.EPGProgramIdentity(channelID, parsed, fmt.Sprintf("xtream:%d", streamID))
	if !ok {
		t.Fatal("elapsed fixture identity failed")
	}
	program.EndAt = clock.Add(5 * time.Minute)
	fs := &fakeStore{
		sources:  []store.ChannelSource{{ID: uuid.New(), ChannelID: channelID, UpstreamURL: url, Enabled: true}},
		programs: []store.EPGProgram{program},
	}
	lookup := &fakeLookup{rows: map[string]StreamRow{
		url: {ID: streamID, Name: name, UpdatedAt: clock},
	}}
	w := New(fs, lookup, nil)
	w.now = func() time.Time { return clock }

	// An active row inside the allowance refreshes, but never beyond the fixed
	// nominal-stop + maxLiveOverrun horizon.
	w.runOnce(context.Background())
	hardEnd := nominalEnd.Add(maxLiveOverrun)
	found := false
	for _, p := range fs.programs {
		if p.SourceHash == program.SourceHash {
			found = true
			if p.EndAt.After(hardEnd) {
				t.Fatalf("overrun exceeded hard horizon: end=%v hard=%v", p.EndAt, hardEnd)
			}
		}
	}
	if !found {
		t.Fatal("active in-horizon overrun was not preserved")
	}

	// Move the worker clock past the horizon while the provider keeps returning
	// the identical stale name. Repeated passes must delete, then never recreate,
	// the parsed row.
	clock = hardEnd.Add(time.Second)
	for i := 0; i < 2; i++ {
		w.runOnce(context.Background())
		for _, p := range fs.programs {
			if p.SourceHash == program.SourceHash {
				t.Fatalf("pass %d resurrected expired overrun: %+v", i+1, p)
			}
		}
	}
}
