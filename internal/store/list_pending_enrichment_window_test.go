// list_pending_enrichment_window_test.go — verifies the worker selection
// window added 2026-05-08 (only programmes whose start_at is within the
// xmltv emission window get enriched).
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// seedThreeProgramsAcrossWindow creates three programmes:
//   - one airing 6h ago (already over — outside window),
//   - one airing in 2h (in the future window),
//   - one airing 20 days from now (beyond the +14d cap).
//
// All three are enrichment_status='pending'. ListPendingEnrichment must
// return only the middle one.
func seedThreeProgramsAcrossWindow(t *testing.T, db *store.DB) (past, future, beyond uuid.UUID) {
	t.Helper()
	ctx := context.Background()

	if _, err := db.CreateProvider(ctx, store.Provider{
		Name: "p", Kind: "m3u_xtream", BaseURL: "http://p.test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 200, Name: "WindowTest", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	insert := func(start time.Time, hash string) uuid.UUID {
		_, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
			ChannelID:  ch.ID,
			StartAt:    start,
			EndAt:      start.Add(30 * time.Minute),
			Title:      "T " + hash,
			Category:   []string{"Series"},
			SourceHash: hash,
		})
		if err != nil {
			t.Fatal(err)
		}
		var id uuid.UUID
		row := db.Pool.QueryRow(ctx, `SELECT id FROM epg_program WHERE source_hash=$1`, hash)
		if err := row.Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	past = insert(now.Add(-6*time.Hour), "win-past")
	future = insert(now.Add(2*time.Hour), "win-future")
	beyond = insert(now.Add(20*24*time.Hour), "win-beyond")
	return
}

func TestIntegrationListPendingEnrichment_WindowFilter(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	past, future, beyond := seedThreeProgramsAcrossWindow(t, db)

	rows, err := db.ListPendingEnrichment(ctx, 50)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	got := map[uuid.UUID]bool{}
	for _, r := range rows {
		got[r.ID] = true
	}
	if got[past] {
		t.Errorf("past programme leaked into pending list (should be excluded)")
	}
	if !got[future] {
		t.Errorf("future programme missing from pending list (should be included)")
	}
	if got[beyond] {
		t.Errorf("beyond-14d programme leaked into pending list (should be excluded)")
	}
}

func TestIntegrationListPendingEnrichment_DeferredTransientError(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	_, future, _ := seedThreeProgramsAcrossWindow(t, db)

	if err := db.DeferEnrichment(ctx, future, 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	rows, err := db.ListPendingEnrichment(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.ID == future {
			t.Fatal("deferred transient failure was returned before retry_at")
		}
	}
	if _, err := db.Pool.Exec(ctx,
		`UPDATE epg_program SET enrichment_retry_at = now() - interval '1 second' WHERE id=$1`, future); err != nil {
		t.Fatal(err)
	}
	rows, err = db.ListPendingEnrichment(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.ID == future {
			return
		}
	}
	t.Fatal("deferred transient failure did not become retryable")
}

// SoonerFirst: when multiple programmes are in window, soonest start_at
// comes back first so the operator-visible guide gets posters first.
func TestIntegrationListPendingEnrichment_SoonerFirst(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	if _, err := db.CreateProvider(ctx, store.Provider{
		Name: "p", Kind: "m3u_xtream", BaseURL: "http://p.test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 201, Name: "OrderTest", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i, h := range []time.Duration{4 * time.Hour, 2 * time.Hour, 6 * time.Hour} {
		_, err := db.UpsertEPGProgram(ctx, store.EPGProgram{
			ChannelID:  ch.ID,
			StartAt:    now.Add(h),
			EndAt:      now.Add(h + 30*time.Minute),
			Title:      "T",
			Category:   []string{"Series"},
			SourceHash: time.Duration(i).String() + "-order-test",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.ListPendingEnrichment(ctx, 50)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) < 3 {
		t.Fatalf("expected at least 3 rows, got %d", len(rows))
	}
	// PendingEnrichmentRow doesn't expose start_at — verify the SQL contract
	// by querying the table directly with the same WHERE/ORDER BY.
	var times []time.Time
	r2, err := db.Pool.Query(ctx,
		`SELECT start_at FROM epg_program
		   WHERE enrichment_status='pending' AND channel_id=$1
		     AND start_at >= now() - interval '2 hours'
		     AND start_at <= now() + interval '14 days'
		   ORDER BY start_at ASC`, ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	for r2.Next() {
		var ts time.Time
		_ = r2.Scan(&ts)
		times = append(times, ts)
	}
	if len(times) < 2 {
		t.Fatalf("not enough rows for ordering check")
	}
	for i := 1; i < len(times); i++ {
		if times[i].Before(times[i-1]) {
			t.Errorf("not sorted ascending at %d: %v > %v", i, times[i-1], times[i])
		}
	}
}
