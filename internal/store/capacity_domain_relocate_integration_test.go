package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationLiveRelocationTakesCapacityDomainBoundary(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}
	providerA := seedCapacityDomainProvider(t, db, "live-domain-a", "relocate-shared", 1)
	providerB := seedCapacityDomainProvider(t, db, "live-domain-b", "relocate-shared", 1)
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9474.1, Name: "live-domain-relocation", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceA, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: providerA.ID,
		UpstreamURL: "http://a.test/live", Priority: 0, HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceB, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: providerB.ID,
		UpstreamURL: "http://b.test/live", Priority: 1, HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireLease(ctx, channel.ID, resolver)
	if err != nil || lease.ChannelSourceID != sourceA.ID {
		t.Fatalf("initial live lease=%+v err=%v, want source A", lease, err)
	}

	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:capacity-domain:' || $1, 0)
		)`, "domain:relocate-shared"); err != nil {
		t.Fatal(err)
	}
	moveCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, moveErr := db.RelocateStream(
		moveCtx, lease.ActiveStreamID, channel.ID, []uuid.UUID{sourceA.ID}, resolver)
	cancel()
	if !errors.Is(moveErr, store.ErrAdmissionContention) ||
		!errors.Is(moveErr, context.DeadlineExceeded) {
		t.Fatalf("live relocation across held domain err=%v, want contention deadline", moveErr)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	moved, err := db.RelocateStream(
		ctx, lease.ActiveStreamID, channel.ID, []uuid.UUID{sourceA.ID}, resolver)
	if err != nil || moved.ChannelSourceID != sourceB.ID {
		t.Fatalf("live relocation after domain release=%+v err=%v, want source B", moved, err)
	}
}

func TestIntegrationDVRRelocationIsUsageNeutralWithinCapacityDomain(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}
	providerA := seedCapacityDomainProvider(t, db, "dvr-domain-a", "dvr-relocate-shared", 1)
	providerB := seedCapacityDomainProvider(t, db, "dvr-domain-b", "dvr-relocate-shared", 1)
	target := seedCapacityDomainProvider(t, db, "dvr-domain-target", "dvr-relocate-target", 2)
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 9474.2, Name: "dvr-domain-relocation", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var sources []store.ChannelSource
	for i, candidate := range []struct {
		provider store.Provider
		url      string
	}{
		{providerA, "http://a.test/dvr"},
		{providerB, "http://b.test/dvr"},
		{target, "http://target.test/dvr"},
	} {
		source, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: candidate.provider.ID,
			UpstreamURL: candidate.url, Priority: i, HealthScore: 1, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, source)
	}
	blocker := seedCapacityDomainChannel(t, db, target.ID, 9474.3, "target-blocker")
	if _, err := db.AcquireLease(ctx, blocker.ID, resolver); err != nil {
		t.Fatalf("fill target domain to retained floor: %v", err)
	}

	lease, err := db.AcquireDVRLease(ctx, channel.ID, resolver,
		store.DVRLeasePolicy{LiveReserve: 1})
	if err != nil || lease.ChannelSourceID != sources[0].ID {
		t.Fatalf("initial DVR lease=%+v err=%v, want source A", lease, err)
	}
	moved, err := db.RelocateDVRStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sources[0].ID}, resolver, store.DVRLeasePolicy{LiveReserve: 1})
	if err != nil || moved.ChannelSourceID != sources[1].ID {
		t.Fatalf("same-domain cross-provider relocation=%+v err=%v, want usage-neutral source B", moved, err)
	}

	_, err = db.RelocateDVRStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sources[0].ID, sources[1].ID}, resolver,
		store.DVRLeasePolicy{LiveReserve: 1})
	if !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("cross-domain DVR relocation err=%v, want target live-floor ErrNoSlot", err)
	}
	liveMoved, err := db.RelocateStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sources[0].ID, sources[1].ID}, resolver)
	if err != nil || liveMoved.ChannelSourceID != sources[2].ID {
		t.Fatalf("live-priority cross-domain relocation=%+v err=%v, want target source", liveMoved, err)
	}
}
