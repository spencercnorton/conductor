// update_channel_source_integration_test.go — exercises UpdateChannelSource
// against a real Postgres. Same gating as lease_integration_test.go
// (CONDUCTOR_INT_TEST=1).
package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// seedOneSource creates a provider + channel + one channel_source and
// returns the source plus channel_id for the caller to use as test fixtures.
func seedOneSource(t *testing.T, db *store.DB) (uuid.UUID, store.ChannelSource) {
	t.Helper()
	ctx := context.Background()

	p, err := db.CreateProvider(ctx, store.Provider{
		Name: "test-provider", Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.CreateChannel(ctx, store.Channel{
		Number: 22, Name: "Fox Business Network", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: c.ID, ProviderID: p.ID,
		UpstreamURL: "http://upstream/old.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c.ID, s
}

func TestIntegrationUpdateChannelSource_SparseUpdate(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	chID, s := seedOneSource(t, db)

	newURL := "http://upstream/new.ts"
	got, err := db.UpdateChannelSource(ctx, chID, s.ID, store.ChannelSourceUpdate{
		UpstreamURL: &newURL,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got.UpstreamURL != newURL {
		t.Errorf("UpstreamURL: got %q want %q", got.UpstreamURL, newURL)
	}
	// Sparse: untouched fields must remain at their seeded values.
	if got.Priority != s.Priority {
		t.Errorf("Priority changed unexpectedly: got %d want %d", got.Priority, s.Priority)
	}
	if got.Enabled != s.Enabled {
		t.Errorf("Enabled changed unexpectedly: got %v want %v", got.Enabled, s.Enabled)
	}
	if got.HealthScore != s.HealthScore {
		t.Errorf("HealthScore changed unexpectedly: got %v want %v", got.HealthScore, s.HealthScore)
	}
	if got.ProviderID != s.ProviderID {
		t.Errorf("ProviderID changed unexpectedly: got %v want %v", got.ProviderID, s.ProviderID)
	}
}

func TestIntegrationUpdateChannelSource_ResetFailure(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	chID, s := seedOneSource(t, db)

	// Mark a failure so last_failure_at is non-null.
	if err := db.MarkSourceFailure(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	// Sanity check: failure stuck.
	pre, err := db.ListSourcesForChannel(ctx, chID)
	if err != nil || len(pre) != 1 || pre[0].LastFailureAt == nil {
		t.Fatalf("seed: expected last_failure_at set, got %v err=%v", pre, err)
	}

	got, err := db.UpdateChannelSource(ctx, chID, s.ID, store.ChannelSourceUpdate{
		ResetFailure: true,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got.LastFailureAt != nil {
		t.Errorf("LastFailureAt: got %v want nil", got.LastFailureAt)
	}
}

func TestIntegrationUpdateChannelSource_AllFields(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	chID, s := seedOneSource(t, db)

	// Make a second provider so we can change provider_id.
	p2, err := db.CreateProvider(ctx, store.Provider{
		Name: "test-provider-2", Kind: "m3u_xtream",
		BaseURL: "http://provider2.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	url := "http://upstream/all-new.ts"
	prio := 5
	enabled := false
	hs := 0.5
	got, err := db.UpdateChannelSource(ctx, chID, s.ID, store.ChannelSourceUpdate{
		UpstreamURL: &url,
		Priority:    &prio,
		Enabled:     &enabled,
		HealthScore: &hs,
		ProviderID:  &p2.ID,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got.UpstreamURL != url || got.Priority != prio || got.Enabled != enabled ||
		got.HealthScore != hs || got.ProviderID != p2.ID {
		t.Errorf("multi-field update mismatch: %+v", got)
	}
}

func TestIntegrationUpdateChannelSource_ScopedToChannel(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	_, s := seedOneSource(t, db)

	// Wrong channel_id but valid source_id → ErrNotFound, no mutation.
	wrongCh := uuid.New()
	url := "http://should-not-apply.ts"
	_, err := db.UpdateChannelSource(ctx, wrongCh, s.ID, store.ChannelSourceUpdate{
		UpstreamURL: &url,
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound for wrong channel scope, got %v", err)
	}
}

func TestIntegrationUpdateChannelSource_NotFound(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	chID, _ := seedOneSource(t, db)

	url := "x"
	_, err := db.UpdateChannelSource(ctx, chID, uuid.New(), store.ChannelSourceUpdate{
		UpstreamURL: &url,
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing source, got %v", err)
	}
}

// Sanity guard: the update preserves created_at-style invariants over time.
// (last_failure_at when not reset stays at its prior value.)
func TestIntegrationUpdateChannelSource_PreservesLastFailureWhenNotReset(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	chID, s := seedOneSource(t, db)

	if err := db.MarkSourceFailure(ctx, s.ID); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	url := "http://x.ts"
	got, err := db.UpdateChannelSource(ctx, chID, s.ID, store.ChannelSourceUpdate{
		UpstreamURL: &url,
		// ResetFailure NOT set
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.LastFailureAt == nil {
		t.Fatal("LastFailureAt unexpectedly nil after sparse update")
	}
	if got.LastFailureAt.After(before) {
		t.Errorf("LastFailureAt was rewritten: %v", got.LastFailureAt)
	}
}
