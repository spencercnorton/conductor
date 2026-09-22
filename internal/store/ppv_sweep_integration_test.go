// ppv_sweep_integration_test.go — exercises the ppvsync-facing epg_program
// helpers against a real Postgres: the definitive per-stream off-state sweep,
// the occupancy cleanup that keeps placeholders off real rows (S2), and the
// live-row end extension (S4). Gated like the other integration tests
// (CONDUCTOR_INT_TEST=1).
//
//	CONDUCTOR_INT_TEST=1 go test -race -count=1 ./internal/store -run Integration
package store_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

// TestIntegrationDeletePPVParsedRows_PerStreamOffSweep covers the v0.36.7
// architecture retained during the rebase: exact events use atomic
// channel-wide replacement, while a definitive idle/ended source deletes only
// that stream's old event rows. Sibling failover and non-PPV rows survive.
func TestIntegrationDeletePPVParsedRows_PerStreamOffSweep(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 950, Name: "PPV Sweep", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	base := time.Date(2026, 7, 11, 21, 0, 0, 0, time.UTC)

	seed := func(start time.Time, hash string, prio int) {
		p := store.EPGProgram{
			ChannelID: ch.ID, StartAt: start, EndAt: start.Add(3 * time.Hour),
			Title: hash, SourceHash: hash, SourcePriority: prio,
		}
		if _, err := db.UpsertEPGProgram(ctx, p); err != nil {
			t.Fatalf("seed %q: %v", hash, err)
		}
	}
	exists := func(start time.Time) bool {
		var n int
		if err := db.Pool.QueryRow(ctx,
			`SELECT count(*) FROM epg_program WHERE channel_id=$1 AND start_at=$2`,
			ch.ID, start).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n > 0
	}

	current := base
	drift := base.Add(-24 * time.Hour)
	sibling := base.Add(48 * time.Hour)
	offBlk := base.Add(6 * time.Hour)
	xmltv := base.Add(9 * time.Hour)
	seed(current, fmt.Sprintf("ppv-parse:xtream:606180:%d:UFC 329", current.Unix()), store.PriorityPPVSync)
	seed(drift, fmt.Sprintf("ppv-parse:xtream:606180:%d:Old Card", drift.Unix()), store.PriorityPPVSync)
	seed(sibling, fmt.Sprintf("ppv-parse:xtream:999999:%d:Sibling Card", sibling.Unix()), store.PriorityPPVSync)
	seed(offBlk, fmt.Sprintf("ppv-off:xtream:606180:%d", offBlk.Unix()), store.PriorityPPVOff)
	seed(xmltv, "xmltv:somefeed:123", 5)

	n, applied, err := db.DeletePPVParsedRowsForChannelStream(ctx, ch.ID, "xtream:606180", time.Now().UTC())
	if err != nil || !applied {
		t.Fatalf("sweep: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted %d rows, want 2 (all rows for the idle stream)", n)
	}
	if exists(current) || exists(drift) {
		t.Error("definitive idle sweep left a row for its stream")
	}
	if !exists(sibling) {
		t.Error("sibling failover stream's row was evicted")
	}
	if !exists(offBlk) {
		t.Error("ppv-off placeholder must not be swept")
	}
	if !exists(xmltv) {
		t.Error("non-ppv row must not be swept")
	}
}

// TestIntegrationOccupiedProgramWindows covers S2: only real rows
// (source_priority < PriorityPPVOff) inside the window are reported, so
// placeholders can steer clear of manual/other-source rows without treating
// their own placeholder grid as occupied.
func TestIntegrationOccupiedProgramWindows(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 951, Name: "Occupancy", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	base := time.Now().UTC().Truncate(6 * time.Hour)
	seed := func(start, end time.Time, hash string, prio int) {
		p := store.EPGProgram{
			ChannelID: ch.ID, StartAt: start, EndAt: end,
			Title: hash, SourceHash: hash, SourcePriority: prio,
		}
		if _, err := db.UpsertEPGProgram(ctx, p); err != nil {
			t.Fatalf("seed %q: %v", hash, err)
		}
	}
	manualStart, manualEnd := base.Add(2*time.Hour), base.Add(5*time.Hour)
	seed(base, base.Add(6*time.Hour), "ppv-off:xtream:1:0", store.PriorityPPVOff) // placeholder — excluded
	seed(manualStart, manualEnd, "manual:ufc329", -50)                            // real, in window
	seed(base.Add(30*time.Hour), base.Add(33*time.Hour), "manual:next-day", -50)  // real, out of window

	occ, err := db.OccupiedProgramWindows(ctx, ch.ID, base, base.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("occupancy: %v", err)
	}
	if len(occ) != 1 {
		t.Fatalf("occupied windows = %d (%+v), want 1 (only the in-window real row)", len(occ), occ)
	}
	if !occ[0].Start.Equal(manualStart) || !occ[0].End.Equal(manualEnd) {
		t.Errorf("window = %v–%v, want %v–%v", occ[0].Start, occ[0].End, manualStart, manualEnd)
	}

	// The real-row writer removes an older overlapping placeholder in its own
	// channel-locked transaction; the database exclusion fence makes a stored
	// off/real overlap structurally impossible even for a legacy writer.
	n, err := db.DeletePPVOffRows(ctx, ch.ID, base, nil, nil)
	if err != nil {
		t.Fatalf("off cleanup: %v", err)
	}
	if n != 0 {
		t.Fatalf("off cleanup deleted %d rows after atomic real insert, want 0", n)
	}
	var remaining int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'ppv-off:%'`, ch.ID).Scan(&remaining); err != nil {
		t.Fatalf("count remaining placeholders: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("manual row still overlapped by %d stored placeholders", remaining)
	}

	// Outside the rolling grid, ordinary XMLTV/SD writes keep their fast path.
	// If an old placeholder nevertheless exists, the deferred exclusion fence
	// rejects that first statement and Upsert retries under channel authority.
	past := base.Add(-72 * time.Hour)
	seed(past, past.Add(6*time.Hour), "ppv-off:xtream:1:past", store.PriorityPPVOff)
	seed(past.Add(time.Hour), past.Add(2*time.Hour), "xmltv:past-real", 5)
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM epg_program
		 WHERE channel_id=$1 AND source_hash='ppv-off:xtream:1:past'`, ch.ID).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatal("serialized retry left a historical placeholder over a real row")
	}
}

// TestIntegrationExtendPPVLiveRow covers S4: a live ppv-parse row whose end is
// within the trigger window is pushed to newEnd, the extension is idempotent
// once applied, and non-ppv-parse rows are never extended.
func TestIntegrationExtendPPVLiveRow(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 952, Name: "Live Extend", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	liveStart := time.Date(2026, 7, 11, 21, 0, 0, 0, time.UTC)
	liveEnd := liveStart.Add(3 * time.Hour) // 00:00
	seed := func(start, end time.Time, hash string) {
		p := store.EPGProgram{
			ChannelID: ch.ID, StartAt: start, EndAt: end,
			Title: hash, SourceHash: hash, SourcePriority: store.PriorityPPVSync,
		}
		if _, err := db.UpsertEPGProgram(ctx, p); err != nil {
			t.Fatalf("seed %q: %v", hash, err)
		}
	}
	liveHash := fmt.Sprintf("ppv-parse:xtream:5:%d:Fight", liveStart.Unix())
	seed(liveStart, liveEnd, liveHash)

	// "now" is in the final stretch: end (00:00) < now+30m.
	now := liveEnd.Add(-15 * time.Minute) // 23:45
	trigger := now.Add(30 * time.Minute)  // 00:15
	newEnd := now.Add(90 * time.Minute)   // 01:15

	hardEnd := liveEnd.Add(3 * time.Hour)
	observedAt := time.Now().UTC()
	n, applied, err := db.ExtendPPVLiveRowEnd(ctx, ch.ID, liveStart, liveHash, observedAt, trigger, newEnd, hardEnd)
	if err != nil || !applied {
		t.Fatalf("extend: %v", err)
	}
	if n != 1 {
		t.Fatalf("extended %d rows, want 1", n)
	}
	var gotEnd time.Time
	if err := db.Pool.QueryRow(ctx,
		`SELECT end_at FROM epg_program WHERE channel_id=$1 AND start_at=$2`,
		ch.ID, liveStart).Scan(&gotEnd); err != nil {
		t.Fatalf("read end: %v", err)
	}
	if !gotEnd.Equal(newEnd) {
		t.Errorf("end_at = %v, want %v", gotEnd, newEnd)
	}

	// The next ppvsync pass presents the parser's original end again with the
	// same source hash. The slot-keyed steady-state upsert must read unchanged,
	// preserving the live extension rather than shrinking the tile back.
	action, deleted, applied, err := db.ReplacePPVParsedRowsForChannel(ctx, store.EPGProgram{
		ChannelID: ch.ID, StartAt: liveStart, EndAt: liveEnd,
		Title: liveHash, SourceHash: liveHash, SourcePriority: store.PriorityPPVSync,
	}, observedAt)
	if err != nil || !applied || action != "unchanged" || deleted != 0 {
		t.Fatalf("steady-state replacement = (%q, %d, %v, %v), want (unchanged, 0, true, nil)", action, deleted, applied, err)
	}
	if err := db.Pool.QueryRow(ctx,
		`SELECT end_at FROM epg_program WHERE channel_id=$1 AND start_at=$2`,
		ch.ID, liveStart).Scan(&gotEnd); err != nil {
		t.Fatalf("read preserved end: %v", err)
	}
	if !gotEnd.Equal(newEnd) {
		t.Fatalf("steady-state replacement shrank live extension to %v, want %v", gotEnd, newEnd)
	}

	// Idempotent: end is now past the trigger, so a re-run is a no-op.
	if n2, applied, err := db.ExtendPPVLiveRowEnd(ctx, ch.ID, liveStart, liveHash, observedAt, trigger, newEnd, hardEnd); err != nil || !applied || n2 != 0 {
		t.Fatalf("re-extend = (%d, %v, %v), want (0, true, nil)", n2, applied, err)
	}

	// Simulate a same-slot rename committed by another worker between the
	// selected winner's replacement and its extension call. The stale winner's
	// exact hash must not extend the concurrent replacement.
	replacementEnd := trigger.Add(-time.Minute)
	replacementHash := fmt.Sprintf("ppv-parse:xtream:999999:%d:Concurrent", liveStart.Unix())
	if action, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
		ChannelID: ch.ID, StartAt: liveStart, EndAt: replacementEnd,
		Title: "Concurrent", SourceHash: replacementHash, SourcePriority: store.PriorityPPVSync,
	}); err != nil || action != "updated" {
		t.Fatalf("concurrent replacement = (%q, %v), want updated", action, err)
	}
	if nRace, applied, err := db.ExtendPPVLiveRowEnd(ctx, ch.ID, liveStart, liveHash, observedAt, trigger, newEnd, hardEnd); err != nil || !applied || nRace != 0 {
		t.Fatalf("stale exact extension = (%d, %v, %v), want (0, true, nil)", nRace, applied, err)
	}
	var gotHash string
	if err := db.Pool.QueryRow(ctx,
		`SELECT end_at, source_hash FROM epg_program WHERE channel_id=$1 AND start_at=$2`,
		ch.ID, liveStart).Scan(&gotEnd, &gotHash); err != nil {
		t.Fatalf("read concurrent replacement: %v", err)
	}
	if gotHash != replacementHash || !gotEnd.Equal(replacementEnd) {
		t.Fatalf("concurrent replacement mutated: hash=%q end=%v", gotHash, gotEnd)
	}

	// A ppv-off placeholder at its own slot must never be extended.
	offStart := liveStart.Add(6 * time.Hour)
	seedOff := store.EPGProgram{
		ChannelID: ch.ID, StartAt: offStart, EndAt: offStart.Add(time.Minute),
		Title: "off", SourceHash: fmt.Sprintf("ppv-off:xtream:5:%d", offStart.Unix()),
		SourcePriority: store.PriorityPPVOff,
	}
	if _, err := db.UpsertEPGProgram(ctx, seedOff); err != nil {
		t.Fatalf("seed off: %v", err)
	}
	if _, _, err := db.ExtendPPVLiveRowEnd(ctx, ch.ID, offStart, seedOff.SourceHash, observedAt, offStart.Add(time.Hour), offStart.Add(2*time.Hour), offStart.Add(3*time.Hour)); err == nil {
		t.Fatal("non-ppv exact identity must be rejected")
	}
}

func TestIntegrationPreservePPVLiveOverrunIsExactAndNonInserting(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 953, Name: "Overrun Preserve", Enabled: true})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	start := now.Add(-3 * time.Hour)
	exactHash := fmt.Sprintf("ppv-parse:xtream:606185:%d:Fight", start.Unix())
	siblingStart := start.Add(-24 * time.Hour)
	siblingHash := fmt.Sprintf("ppv-parse:xtream:999999:%d:Sibling", siblingStart.Unix())
	for _, p := range []store.EPGProgram{
		{
			ChannelID: ch.ID, StartAt: start, EndAt: now.Add(10 * time.Minute),
			Title: "Fight", SourceHash: exactHash, SourcePriority: store.PriorityPPVSync,
		},
		{
			ChannelID: ch.ID, StartAt: siblingStart, EndAt: siblingStart.Add(3 * time.Hour),
			Title: "Sibling", SourceHash: siblingHash, SourcePriority: store.PriorityPPVSync,
		},
	} {
		if action, err := db.UpsertEPGProgram(ctx, p); err != nil || action != "added" {
			t.Fatalf("seed %q: action=%q err=%v", p.Title, action, err)
		}
	}

	hardEnd := start.Add(6 * time.Hour)
	observedAt := now
	if end, active, err := db.ActivePPVLiveOverrunEnd(ctx, ch.ID, start, exactHash, now, hardEnd); err != nil || !active || !end.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("active probe = (%v, %v, %v)", end, active, err)
	}
	newEnd := now.Add(90 * time.Minute)
	end, deleted, kept, err := db.PreservePPVLiveOverrunForChannel(
		ctx, ch.ID, start, exactHash, observedAt, now, hardEnd, now.Add(30*time.Minute), newEnd,
	)
	if err != nil || !kept || deleted != 1 || !end.Equal(newEnd) {
		t.Fatalf("preserve = (end=%v deleted=%d kept=%v err=%v)", end, deleted, kept, err)
	}
	var exactCount, siblingCount int
	if err := db.Pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE source_hash=$2),
			count(*) FILTER (WHERE source_hash=$3)
		  FROM epg_program
		 WHERE channel_id=$1`, ch.ID, exactHash, siblingHash).Scan(&exactCount, &siblingCount); err != nil {
		t.Fatal(err)
	}
	if exactCount != 1 || siblingCount != 0 {
		t.Fatalf("winner/sibling counts = %d/%d, want 1/0", exactCount, siblingCount)
	}

	// A row written by the previous unbounded implementation may extend past
	// the new finite allowance. Reads report only the cap; preservation clamps
	// the stored row, and the exact horizon is no longer active.
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	shard := int32(binary.BigEndian.Uint32(ch.ID[:4]))
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock($1, $2),
		       set_config('conductor.epg_channel_lock', $3, true)`,
		int32(0x45504731), shard, ch.ID.String()); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE epg_program SET end_at=$4
		 WHERE channel_id=$1 AND start_at=$2 AND source_hash=$3`,
		ch.ID, start, exactHash, hardEnd.Add(time.Hour)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("seed legacy over-cap row: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if end, active, err := db.ActivePPVLiveOverrunEnd(ctx, ch.ID, start, exactHash, now, hardEnd); err != nil || !active || !end.Equal(hardEnd) {
		t.Fatalf("capped active probe = (%v, %v, %v), want hard end", end, active, err)
	}
	end, _, kept, err = db.PreservePPVLiveOverrunForChannel(
		ctx, ch.ID, start, exactHash, observedAt, now, hardEnd, hardEnd.Add(time.Hour), hardEnd.Add(2*time.Hour),
	)
	if err != nil || !kept || !end.Equal(hardEnd) {
		t.Fatalf("hard-cap preserve = (end=%v kept=%v err=%v)", end, kept, err)
	}
	if end, active, err := db.ActivePPVLiveOverrunEnd(ctx, ch.ID, start, exactHash, hardEnd, hardEnd); err != nil || active || !end.IsZero() {
		t.Fatalf("at-horizon probe = (%v, %v, %v), want inactive", end, active, err)
	}
	if _, deleted, kept, err := db.PreservePPVLiveOverrunForChannel(
		ctx, ch.ID, start, exactHash, observedAt, hardEnd, hardEnd, hardEnd.Add(time.Hour), hardEnd.Add(2*time.Hour),
	); err != nil || kept || deleted != 0 {
		t.Fatalf("at-horizon preserve = deleted=%d kept=%v err=%v", deleted, kept, err)
	}

	// Expired and changed identities cannot be inserted by the preserve path.
	expiredStart := now.Add(-5 * time.Hour)
	expiredHash := fmt.Sprintf("ppv-parse:xtream:606186:%d:Expired", expiredStart.Unix())
	if action, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
		ChannelID: ch.ID, StartAt: expiredStart, EndAt: now.Add(-time.Minute),
		Title: "Expired", SourceHash: expiredHash, SourcePriority: store.PriorityPPVSync,
	}); err != nil || action != "added" {
		t.Fatalf("seed expired: action=%q err=%v", action, err)
	}
	if _, active, err := db.ActivePPVLiveOverrunEnd(ctx, ch.ID, expiredStart, expiredHash, now, expiredStart.Add(6*time.Hour)); err != nil || active {
		t.Fatalf("expired probe = active=%v err=%v", active, err)
	}
	if _, deleted, kept, err := db.PreservePPVLiveOverrunForChannel(
		ctx, ch.ID, expiredStart, expiredHash, observedAt, now, expiredStart.Add(6*time.Hour), now.Add(30*time.Minute), newEnd,
	); err != nil || kept || deleted != 0 {
		t.Fatalf("expired preserve = deleted=%d kept=%v err=%v", deleted, kept, err)
	}
	if _, deleted, kept, err := db.PreservePPVLiveOverrunForChannel(
		ctx, ch.ID, start, exactHash+":renamed", observedAt, now, hardEnd, now.Add(30*time.Minute), newEnd,
	); err != nil || kept || deleted != 0 {
		t.Fatalf("changed preserve = deleted=%d kept=%v err=%v", deleted, kept, err)
	}
	if _, _, _, err := db.PreservePPVLiveOverrunForChannel(
		ctx, ch.ID, start, "xmltv:not-ppv", observedAt, now, hardEnd, now.Add(30*time.Minute), newEnd,
	); err == nil {
		t.Fatal("non-ppv source hash must be rejected")
	}
}

func TestIntegrationPPVSnapshotOrderingPreventsStaleIdleOverlap(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	oldObservedAt := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	newObservedAt := oldObservedAt.Add(time.Minute)

	for i, exactFirst := range []bool{false, true} {
		name := "off-commits-first"
		if exactFirst {
			name = "exact-commits-first"
		}
		t.Run(name, func(t *testing.T) {
			ch, err := db.CreateChannel(ctx, store.Channel{
				Number: 960 + float64(i), Name: "PPV Snapshot " + name, Enabled: true,
			})
			if err != nil {
				t.Fatalf("create channel: %v", err)
			}
			event := store.EPGProgram{
				ChannelID: ch.ID, StartAt: base.Add(2 * time.Hour), EndAt: base.Add(5 * time.Hour),
				Title: "New Exact Event", SourceHash: "ppv-parse:xtream:606200:exact",
				SourcePriority: store.PriorityPPVSync,
			}
			off := store.EPGProgram{
				ChannelID: ch.ID, StartAt: base, EndAt: base.Add(6 * time.Hour),
				Title: "NHL — No Event Scheduled", SourceHash: "ppv-off:xtream:606200:grid",
				SourcePriority: store.PriorityPPVOff,
			}

			applyOff := func(wantApplied bool) {
				n, applied, err := db.ReconcilePPVOffRows(
					ctx, ch.ID, base, base.Add(24*time.Hour), nil, nil,
					[]store.EPGProgram{off}, oldObservedAt,
				)
				if err != nil || applied != wantApplied {
					t.Fatalf("off reconcile = (n=%d applied=%v err=%v), want applied=%v", n, applied, err, wantApplied)
				}
			}
			applyExact := func() {
				action, _, applied, err := db.ReplacePPVParsedRowsForChannel(ctx, event, newObservedAt)
				if err != nil || !applied || (action != "added" && action != "updated") {
					t.Fatalf("exact replace = (action=%q applied=%v err=%v)", action, applied, err)
				}
			}

			if exactFirst {
				applyExact()
				applyOff(false) // older observation loses after waiting for the channel lock
			} else {
				applyOff(true)
				applyExact() // newer exact commit atomically removes the old overlap
			}

			// A still-running old idle worker may next try its per-stream delete.
			// The same high-water mark rejects it before it reaches the exact row.
			if n, applied, err := db.DeletePPVParsedRowsForChannelStream(
				ctx, ch.ID, "xtream:606200", oldObservedAt,
			); err != nil || applied || n != 0 {
				t.Fatalf("stale idle delete = (n=%d applied=%v err=%v), want (0,false,nil)", n, applied, err)
			}

			var exactCount, overlappingOff int
			if err := db.Pool.QueryRow(ctx, `
				SELECT
					count(*) FILTER (WHERE source_hash=$2),
					count(*) FILTER (
						WHERE source_hash LIKE 'ppv-off:%'
						  AND start_at < $4 AND end_at > $3)
				  FROM epg_program
				 WHERE channel_id=$1`, ch.ID, event.SourceHash, event.StartAt, event.EndAt,
			).Scan(&exactCount, &overlappingOff); err != nil {
				t.Fatal(err)
			}
			if exactCount != 1 || overlappingOff != 0 {
				t.Fatalf("final exact/overlapping-off counts=%d/%d, want 1/0", exactCount, overlappingOff)
			}
		})
	}
}

func TestIntegrationPPVOffReconcileWaitsAndRechecksCommittedOccupancy(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 962, Name: "PPV Lock Race", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(6 * time.Hour)
	real := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base.Add(2 * time.Hour), EndAt: base.Add(5 * time.Hour),
		Title: "Concurrent Exact", SourceHash: "manual:concurrent-exact", SourcePriority: -20,
	}
	off := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base, EndAt: base.Add(6 * time.Hour),
		Title: "No Event Scheduled", SourceHash: "ppv-off:xtream:606201:grid",
		SourcePriority: store.PriorityPPVOff,
	}

	// Hold the same channel advisory lock and stage a real event without
	// committing it. Reconcile must block, then take a fresh READ COMMITTED
	// occupancy snapshot only after this transaction commits.
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	shard := int32(binary.BigEndian.Uint32(ch.ID[:4]))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(0x45504731), shard); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO epg_program
		    (channel_id,start_at,end_at,title,source_hash,source_priority)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		real.ChannelID, real.StartAt, real.EndAt, real.Title, real.SourceHash, real.SourcePriority); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}

	type result struct {
		n       int
		applied bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		n, applied, err := db.ReconcilePPVOffRows(
			ctx, ch.ID, base, base.Add(24*time.Hour), nil, nil,
			[]store.EPGProgram{off}, time.Now().UTC(),
		)
		done <- result{n: n, applied: applied, err: err}
	}()

	blocked := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_locks
			 WHERE locktype='advisory' AND NOT granted`).Scan(&waiting); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if waiting > 0 {
			blocked = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !blocked {
		_ = tx.Rollback(ctx)
		t.Fatal("reconcile did not wait on the channel advisory lock")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || !got.applied || got.n != 0 {
		t.Fatalf("reconcile after committed occupancy = (n=%d applied=%v err=%v), want (0,true,nil)", got.n, got.applied, got.err)
	}

	var realCount, offCount int
	if err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE source_hash=$2),
		       count(*) FILTER (WHERE source_hash LIKE 'ppv-off:%')
		  FROM epg_program WHERE channel_id=$1`, ch.ID, real.SourceHash).Scan(&realCount, &offCount); err != nil {
		t.Fatal(err)
	}
	if realCount != 1 || offCount != 0 {
		t.Fatalf("final exact/off counts=%d/%d, want 1/0", realCount, offCount)
	}
}

func TestIntegrationLegacyPPVMutationFailsClosedAfterFenceMigration(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 963, Name: "PPV Legacy Fence", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(6 * time.Hour)
	exact := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base.Add(2 * time.Hour), EndAt: base.Add(5 * time.Hour),
		Title: "New Exact", SourceHash: "ppv-parse:xtream:606202:new", SourcePriority: store.PriorityPPVSync,
	}
	if _, _, applied, err := db.ReplacePPVParsedRowsForChannel(ctx, exact, time.Now().UTC()); err != nil || !applied {
		t.Fatalf("seed exact: applied=%v err=%v", applied, err)
	}

	// This is the direct SQL shape used by the pre-migration binary. It has no
	// transaction-local channel authority and must fail rather than erase the
	// newer exact row during a rolling deployment.
	if _, err := db.Pool.Exec(ctx, `
		DELETE FROM epg_program
		 WHERE channel_id=$1 AND source_hash LIKE 'ppv-parse:' || $2 || ':%'`,
		ch.ID, "xtream:606202"); err == nil {
		t.Fatal("legacy direct PPV delete unexpectedly bypassed the database fence")
	}
	var exactCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM epg_program WHERE channel_id=$1 AND source_hash=$2`, ch.ID, exact.SourceHash).Scan(&exactCount); err != nil {
		t.Fatal(err)
	}
	if exactCount != 1 {
		t.Fatalf("new exact row count=%d after legacy delete, want 1", exactCount)
	}

	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program
		    (channel_id,start_at,end_at,title,source_hash,source_priority)
		VALUES ($1,$2,$3,'stale off','ppv-off:xtream:606202:old',$4)`,
		ch.ID, base, base.Add(6*time.Hour), store.PriorityPPVOff); err == nil {
		t.Fatal("legacy direct PPV off insert unexpectedly bypassed the database fence")
	}

	oldStart := base.Add(-20 * 24 * time.Hour)
	old := store.EPGProgram{
		ChannelID: ch.ID, StartAt: oldStart, EndAt: oldStart.Add(3 * time.Hour),
		Title: "Old PPV", SourceHash: "ppv-parse:xtream:606299:old", SourcePriority: store.PriorityPPVSync,
	}
	if action, err := db.UpsertEPGProgram(ctx, old); err != nil || action != "added" {
		t.Fatalf("seed old PPV: action=%q err=%v", action, err)
	}
	if purged, err := db.PurgeOldPrograms(ctx, 14); err != nil || purged != 1 {
		t.Fatalf("serialized PPV purge = (n=%d err=%v), want (1,nil)", purged, err)
	}
}

func TestIntegrationPPVPassLeaseSerializesCatalogueGenerations(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	releaseFirst, err := db.AcquirePPVSyncLease(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type leaseResult struct {
		release func() error
		err     error
	}
	second := make(chan leaseResult, 1)
	go func() {
		release, err := db.AcquirePPVSyncLease(ctx)
		second <- leaseResult{release: release, err: err}
	}()

	blocked := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_locks
			 WHERE locktype='advisory' AND NOT granted`).Scan(&waiting); err != nil {
			_ = releaseFirst()
			t.Fatal(err)
		}
		if waiting > 0 {
			blocked = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !blocked {
		_ = releaseFirst()
		t.Fatal("second ppvsync pass did not wait for the first pass lease")
	}
	select {
	case got := <-second:
		if got.release != nil {
			_ = got.release()
		}
		t.Fatalf("second pass acquired before first released: %v", got.err)
	default:
	}
	if err := releaseFirst(); err != nil {
		t.Fatal(err)
	}
	got := <-second
	if got.err != nil {
		t.Fatal(got.err)
	}
	if err := got.release(); err != nil {
		t.Fatal(err)
	}
}

// TestReconcilePPVOffRows_FullSetReconcileRemovesStaleTails pins the
// 2026-08-29 NCAAF guide-hole fix's safety property: trimmed post-event
// tails have off-grid starts, and when the event vanishes on a later pass
// the fresh full-block set must REPLACE the stale tail rather than coexist
// with it as overlapping placeholders.
func TestReconcilePPVOffRows_FullSetReconcileRemovesStaleTails(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 971, Name: "PPV tail reconcile", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}

	// Pass 1: an event ended 02:30; the injector emitted an off-grid
	// post-event tail alongside later full blocks.
	tail := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base.Add(150 * time.Minute), EndAt: base.Add(6 * time.Hour),
		Title: "NCAAF — No Event Scheduled", SourceHash: "ppv-off:xtream:700001:0:post:9000",
		SourcePriority: store.PriorityPPVOff,
	}
	block2 := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base.Add(6 * time.Hour), EndAt: base.Add(12 * time.Hour),
		Title: "NCAAF — No Event Scheduled", SourceHash: "ppv-off:xtream:700001:21600",
		SourcePriority: store.PriorityPPVOff,
	}
	evStart, evEnd := base, base.Add(150*time.Minute)
	if _, applied, err := db.ReconcilePPVOffRows(
		ctx, ch.ID, base, base.Add(24*time.Hour), &evStart, &evEnd,
		[]store.EPGProgram{tail, block2}, base.Add(time.Minute),
	); err != nil || !applied {
		t.Fatalf("pass 1 reconcile: applied=%v err=%v", applied, err)
	}

	// Pass 2: the event vanished; the injector now emits full grid blocks.
	full1 := store.EPGProgram{
		ChannelID: ch.ID, StartAt: base, EndAt: base.Add(6 * time.Hour),
		Title: "NCAAF — No Event Scheduled", SourceHash: "ppv-off:xtream:700001:0",
		SourcePriority: store.PriorityPPVOff,
	}
	if _, applied, err := db.ReconcilePPVOffRows(
		ctx, ch.ID, base, base.Add(24*time.Hour), nil, nil,
		[]store.EPGProgram{full1, block2}, base.Add(2*time.Minute),
	); err != nil || !applied {
		t.Fatalf("pass 2 reconcile: applied=%v err=%v", applied, err)
	}

	rows, err := db.Pool.Query(ctx,
		`SELECT start_at, end_at FROM epg_program WHERE channel_id=$1 AND source_hash LIKE 'ppv-off:%' ORDER BY start_at`, ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var spans [][2]time.Time
	for rows.Next() {
		var s, e time.Time
		if err := rows.Scan(&s, &e); err != nil {
			t.Fatal(err)
		}
		spans = append(spans, [2]time.Time{s, e})
	}
	if len(spans) != 2 {
		t.Fatalf("expected exactly the 2 fresh rows, got %d: %v (stale post tail must be reconciled away)", len(spans), spans)
	}
	for i := 1; i < len(spans); i++ {
		if spans[i][0].Before(spans[i-1][1]) {
			t.Fatalf("overlapping placeholder rows survived: %v", spans)
		}
	}
}
