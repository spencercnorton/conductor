package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/google/uuid"
)

func TestIntegrationPPVNoticeOutputUsesExactProvenance(t *testing.T) {
	skipIfNoIntegration(t)
	// Avoid the legacy fixed-name Docker helper for this focused control.
	if os.Getenv("CONDUCTOR_TEST_DSN") == "" {
		t.Fatal("supply an explicitly isolated CONDUCTOR_TEST_DSN")
	}
	db := freshDB(t)
	ctx := context.Background()
	var number float64
	if err := db.Pool.QueryRow(ctx, `SELECT COALESCE(max(number),0)+1 FROM channel`).Scan(&number); err != nil {
		t.Fatal(err)
	}
	ch, err := db.CreateChannel(ctx, store.Channel{Number: number, Name: "notice-" + uuid.NewString(), Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for i, hash := range []string{"ppv-off:test:notice", "xmltv:test:ordinary"} {
		p := store.EPGProgram{ChannelID: ch.ID, StartAt: start.Add(time.Duration(i) * time.Hour), EndAt: start.Add(time.Duration(i+1) * time.Hour), Title: "ESPN2", SourceHash: hash, SourcePriority: store.PriorityPPVOff}
		if _, err := db.UpsertEPGProgram(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.ListProgramsForOutput(ctx, start, start.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	var found int
	for _, row := range rows {
		if row.ChannelID != ch.ID {
			continue
		}
		want := row.StartAt.Equal(start)
		if row.IsPPVPlaceholder != want || row.IsPlaceholder != want {
			t.Fatalf("same title/priority must use exact source provenance: %+v", row)
		}
		found++
	}
	if found != 2 {
		t.Fatalf("output rows=%d want 2", found)
	}
	// Output is read-only: the original source metadata/flags stay untouched.
	var live, firstRun int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE is_live), count(*) FILTER (WHERE is_new) FROM epg_program WHERE channel_id=$1`, ch.ID).Scan(&live, &firstRun); err != nil || live != 0 || firstRun != 0 {
		t.Fatalf("output changed stored airing flags: live=%d new=%d err=%v", live, firstRun, err)
	}
}
