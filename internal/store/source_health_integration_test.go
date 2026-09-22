package store_test

import (
	"context"
	"math"
	"testing"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func sourceHealth(t *testing.T, db *store.DB, id uuid.UUID) float64 {
	t.Helper()
	sources, err := db.ListChannelSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range sources {
		if s.ID == id {
			return s.HealthScore
		}
	}
	t.Fatalf("source %s not found", id)
	return 0
}

// A working source that took a boundary storm's failures must recover in a
// handful of clean sessions, not twenty: success is credited once per pump
// lifetime, so the old flat +0.05 pinned working sources at 0.0 for weeks.
func TestIntegrationSourceHealthRecoversProportionally(t *testing.T) {
	skipIfNoIntegration(t)
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 1)
	ch := seedLeaseChannel(t, db, providerID, 901, 1)
	sources, err := db.ListChannelSources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	for _, s := range sources {
		if s.ChannelID == ch.ID {
			id = s.ID
		}
	}
	if id == uuid.Nil {
		t.Fatal("seeded source not found")
	}
	approx := func(step string, want float64) {
		t.Helper()
		if got := sourceHealth(t, db, id); math.Abs(got-want) > 0.01 {
			t.Fatalf("%s: health=%.4f, want %.4f", step, got, want)
		}
	}
	for i := 0; i < 5; i++ {
		if err := db.MarkSourceFailure(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	approx("five failures floor at 0", 0)
	want := 0.0
	for i := 1; i <= 5; i++ {
		if err := db.MarkSourceSuccess(ctx, id); err != nil {
			t.Fatal(err)
		}
		want += 0.25 * (1 - want)
	}
	approx("five clean sessions", want) // 0.7627
	if want < 0.76 {
		t.Fatalf("expected the fifth clean session to reach ~0.76, got %.4f", want)
	}
	if err := db.MarkSourceFailure(ctx, id); err != nil {
		t.Fatal(err)
	}
	approx("one failure after recovery", want-0.2)
	for i := 0; i < 30; i++ {
		if err := db.MarkSourceSuccess(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if got := sourceHealth(t, db, id); got > 1.0 || got < 0.99 {
		t.Fatalf("thirty clean sessions: health=%.4f, want capped at 1.0", got)
	}
}
