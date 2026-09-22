// relocate_integration_test.go — RelocateStream (mid-stream failover, spec
// Phase 5) against a real Postgres. Shares the pg bootstrap helpers in
// lease_integration_test.go; same gating:
//
//	CONDUCTOR_INT_TEST=1 go test -race -count=1 ./internal/store -run Integration
package store_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// relocateChanNum hands out distinct channel numbers (unique constraint).
var relocateChanNum atomic.Int64

// seedChannelWithSources creates one channel with a source per URL, at
// ascending priority (urls[0] = priority 0), all on the given provider.
func seedChannelWithSources(t *testing.T, db *store.DB, providerID uuid.UUID, urls ...string) (store.Channel, []store.ChannelSource) {
	t.Helper()
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 900.0 + float64(relocateChanNum.Add(1)), Name: "relocate-test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]store.ChannelSource, 0, len(urls))
	for i, u := range urls {
		src, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: ch.ID, ProviderID: providerID,
			UpstreamURL: u, Priority: i, HealthScore: 1.0, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, src)
	}
	return ch, sources
}

// TestIntegrationRelocatePreservesIdentityAndMovesSource: the row keeps its
// id + client_count across a relocate, and the exclusion list advances it
// past the (dead) priority-0 source that pure priority ordering would
// otherwise pick forever.
func TestIntegrationRelocatePreservesIdentityAndMovesSource(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}

	provID, _ := seedProviderWithCredentials(t, db, 1, 1) // ONE cred, ONE slot
	ch, sources := seedChannelWithSources(t, db, provID,
		"http://src-a.test/stream", "http://src-b.test/stream")

	lease, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.ChannelSourceID != sources[0].ID {
		t.Fatalf("initial lease on source %s, want priority-0 %s", lease.ChannelSourceID, sources[0].ID)
	}
	if lease.ChannelID != ch.ID {
		t.Fatalf("lease.ChannelID = %s, want %s", lease.ChannelID, ch.ID)
	}
	// Second client attaches (client_count = 2) so we can verify the count
	// survives the hop.
	if _, err := db.AcquireLease(ctx, ch.ID, resolver); err != nil {
		t.Fatalf("attach: %v", err)
	}

	// Relocate, excluding the failed priority-0 source. The single
	// credential is fully occupied — by our own row — so this also proves
	// the capacity check excludes the slot being vacated.
	moved, err := db.RelocateStream(ctx, lease.ActiveStreamID, ch.ID,
		[]uuid.UUID{sources[0].ID}, resolver)
	if err != nil {
		t.Fatalf("relocate: %v", err)
	}
	if moved.ActiveStreamID != lease.ActiveStreamID {
		t.Fatalf("relocate changed stream id: %s → %s", lease.ActiveStreamID, moved.ActiveStreamID)
	}
	if moved.ChannelSourceID != sources[1].ID {
		t.Fatalf("relocated to %s, want excluded-advance to %s", moved.ChannelSourceID, sources[1].ID)
	}
	if moved.ClientCount != 2 {
		t.Fatalf("ClientCount = %d, want 2 preserved across relocate", moved.ClientCount)
	}
	if moved.UpstreamURL != "http://src-b.test/stream" {
		t.Fatalf("UpstreamURL = %q", moved.UpstreamURL)
	}

	// Row state must be back to 'starting' (the pump re-announces running).
	var state string
	var srcID uuid.UUID
	if err := db.Pool.QueryRow(ctx, `
		SELECT state, channel_source_id FROM active_stream WHERE id = $1`,
		lease.ActiveStreamID).Scan(&state, &srcID); err != nil {
		t.Fatal(err)
	}
	if state != "starting" || srcID != sources[1].ID {
		t.Fatalf("row state=%s src=%s after relocate", state, srcID)
	}
}

// Candidate ordering must exclude the row being moved, just like the final
// capacity check. Otherwise a reusable current credential can tie with a
// genuinely full credential and lose on priority, producing false contention.
func TestIntegrationRelocateCandidateLoadExcludesMovingStream(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}
	providerID, credentialIDs := seedProviderWithCredentials(t, db, 2, 1)
	if _, err := db.Pool.Exec(ctx,
		`UPDATE provider_credential SET priority = 0 WHERE id = $1`, credentialIDs[0]); err != nil {
		t.Fatal(err)
	}

	blocker, _ := seedChannelWithSources(t, db, providerID,
		"http://ordering-blocker.test/stream")
	blockerLease, err := db.AcquireLease(ctx, blocker.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if blockerLease.CredentialID != credentialIDs[0] {
		t.Fatalf("blocker credential=%s, want priority credential %s",
			blockerLease.CredentialID, credentialIDs[0])
	}

	channel, sources := seedChannelWithSources(t, db, providerID,
		"http://ordering-a.test/stream", "http://ordering-b.test/stream")
	lease, err := db.AcquireLease(ctx, channel.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if lease.CredentialID != credentialIDs[1] {
		t.Fatalf("moving credential=%s, want free credential %s",
			lease.CredentialID, credentialIDs[1])
	}

	moved, err := db.RelocateStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sources[0].ID}, resolver)
	if err != nil {
		t.Fatalf("reuse current credential: %v", err)
	}
	if moved.ChannelSourceID != sources[1].ID || moved.CredentialID != credentialIDs[1] {
		t.Fatalf("moved=%+v, want source %s on reusable credential %s",
			moved, sources[1].ID, credentialIDs[1])
	}
}

// TestIntegrationRelocateNoSlotWhenTargetCredentialFull: a relocate must not
// steal a slot another stream holds.
func TestIntegrationRelocateNoSlotWhenTargetCredentialFull(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}

	// One credential, 2 slots; two channels each leasing one slot.
	provID, credentialIDs := seedProviderWithCredentials(t, db, 1, 2)
	chA, sourcesA := seedChannelWithSources(t, db, provID,
		"http://a-1.test/stream", "http://a-2.test/stream")
	chB, _ := seedChannelWithSources(t, db, provID, "http://b-1.test/stream")

	leaseA, err := db.AcquireLease(ctx, chA.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireLease(ctx, chB.ID, resolver); err != nil {
		t.Fatal(err)
	}

	// Both slots in use (A + B). Relocating A within the same credential
	// must still succeed: A's own slot doesn't count against it.
	moved, err := db.RelocateStream(ctx, leaseA.ActiveStreamID, chA.ID,
		[]uuid.UUID{sourcesA[0].ID}, resolver)
	if err != nil {
		t.Fatalf("same-credential relocate should succeed (exclude-self): %v", err)
	}
	if moved.ChannelSourceID != sourcesA[1].ID {
		t.Fatalf("moved to %s, want %s", moved.ChannelSourceID, sourcesA[1].ID)
	}

	// Now drop the credential's capacity to 1 (B holds it) — the next
	// relocate cycle must report ErrNoSlot, not evict B.
	if _, err := db.Pool.Exec(ctx, `UPDATE provider_credential SET max_streams = 1`); err != nil {
		t.Fatal(err)
	}
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM provider_credential WHERE id = $1 FOR UPDATE`, credentialIDs[0]); err != nil {
		t.Fatal(err)
	}
	_, err = db.RelocateStream(ctx, leaseA.ActiveStreamID, chA.ID, nil, resolver)
	if !errors.Is(err, store.ErrNoSlot) || errors.Is(err, store.ErrAdmissionContention) {
		t.Fatalf("err = %v, want ErrNoSlot without contention when locked target credential is full", err)
	}
}

// Relocation capacity excludes the row being moved. If that row alone fills a
// locked credential, the move can reuse its slot after the lock clears and is
// therefore contention, not saturation.
func TestIntegrationRelocateLockedOwnCredentialIsContention(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}
	providerID, credentialIDs := seedProviderWithCredentials(t, db, 1, 1)
	channel, sources := seedChannelWithSources(t, db, providerID,
		"http://locked-own-a.test/stream", "http://locked-own-b.test/stream")
	lease, err := db.AcquireLease(ctx, channel.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}

	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM provider_credential WHERE id = $1 FOR UPDATE`, credentialIDs[0]); err != nil {
		t.Fatal(err)
	}

	_, err = db.RelocateStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sources[0].ID}, resolver)
	if !errors.Is(err, store.ErrAdmissionContention) || errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("locked own credential err=%v, want contention without ErrNoSlot", err)
	}
}

// TestIntegrationRelocateGoneRow: relocating a reaped stream reports
// ErrStreamGone so the pump stops instead of retrying.
func TestIntegrationRelocateGoneRow(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}

	provID, _ := seedProviderWithCredentials(t, db, 1, 1)
	ch, _ := seedChannelWithSources(t, db, provID, "http://gone.test/stream")

	lease, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `DELETE FROM active_stream WHERE id = $1`, lease.ActiveStreamID); err != nil {
		t.Fatal(err)
	}

	_, err = db.RelocateStream(ctx, lease.ActiveStreamID, ch.ID, nil, resolver)
	if !errors.Is(err, store.ErrStreamGone) {
		t.Fatalf("err = %v, want ErrStreamGone", err)
	}

	// A dead (but not yet deleted) row must behave the same.
	lease2, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkDead(ctx, lease2.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	_, err = db.RelocateStream(ctx, lease2.ActiveStreamID, ch.ID, nil, resolver)
	if !errors.Is(err, store.ErrStreamGone) {
		t.Fatalf("err = %v, want ErrStreamGone for dead row", err)
	}
}

// A DVR reservation may move within its current provider without changing
// provider usage, but cross-provider failover must preserve the target's live
// reserve. An ordinary viewer relocation can use the same final free slot.
func TestIntegrationDVRRelocatePreservesTargetLiveReserve(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}
	seedProvider := func(name string, maxStreams int) (store.Provider, store.ProviderCredential) {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{
			Name: name, Kind: "m3u_xtream", Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		credential, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
			MaxStreams: maxStreams, Priority: 100, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return provider, credential
	}
	providerA, _ := seedProvider("dvr-relocate-a", 1)
	providerB, _ := seedProvider("dvr-relocate-b", 2)

	mainChannel, err := db.CreateChannel(ctx, store.Channel{Number: 951, Name: "dvr-relocate-main", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	sourceA, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: mainChannel.ID, ProviderID: providerA.ID, UpstreamURL: "http://a.test/main",
		Priority: 0, HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceB, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: mainChannel.ID, ProviderID: providerB.ID, UpstreamURL: "http://b.test/main",
		Priority: 1, HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := db.CreateChannel(ctx, store.Channel{Number: 952, Name: "dvr-relocate-blocker", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: blocker.ID, ProviderID: providerB.ID, UpstreamURL: "http://b.test/blocker",
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireLease(ctx, blocker.ID, resolver); err != nil {
		t.Fatalf("fill provider B to its live floor: %v", err)
	}
	lease, err := db.AcquireDVRLease(ctx, mainChannel.ID, resolver,
		store.DVRLeasePolicy{LiveReserve: 1})
	if err != nil {
		t.Fatal(err)
	}
	if lease.ChannelSourceID != sourceA.ID {
		t.Fatalf("initial source=%s, want provider A %s", lease.ChannelSourceID, sourceA.ID)
	}

	if _, err := db.RelocateDVRStream(ctx, lease.ActiveStreamID, mainChannel.ID,
		[]uuid.UUID{sourceA.ID}, resolver, store.DVRLeasePolicy{LiveReserve: 1}); !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("DVR cross-provider relocate err=%v, want live-floor ErrNoSlot", err)
	}
	moved, err := db.RelocateStream(ctx, lease.ActiveStreamID, mainChannel.ID,
		[]uuid.UUID{sourceA.ID}, resolver)
	if err != nil {
		t.Fatalf("ordinary relocation into free live slot: %v", err)
	}
	if moved.ChannelSourceID != sourceB.ID {
		t.Fatalf("ordinary relocation source=%s, want provider B %s", moved.ChannelSourceID, sourceB.ID)
	}
}

// Ordinary live relocation is allowed to consume a provider's retained live
// slot, but it must take the same provider boundary as DVR admission. A target
// provider lock therefore delays the move instead of letting it race a DVR
// reserve decision on a different credential.
func TestIntegrationLiveRelocateParticipatesInProviderReserveBoundary(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}
	seedProvider := func(name string) store.Provider {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{
			Name: name + "-" + uuid.NewString(), Kind: "m3u_xtream", Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
			MaxStreams: 1, Priority: 100, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	providerA := seedProvider("live-relocate-boundary-a")
	providerB := seedProvider("live-relocate-boundary-b")
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 950 + float64(relocateChanNum.Add(1)),
		Name:   "live-relocate-provider-boundary", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceA, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: providerA.ID,
		UpstreamURL: "http://provider-a.test/live", Priority: 0,
		HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sourceB, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: providerB.ID,
		UpstreamURL: "http://provider-b.test/live", Priority: 1,
		HealthScore: 1, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireLease(ctx, channel.ID, resolver)
	if err != nil || lease.ChannelSourceID != sourceA.ID {
		t.Fatalf("initial lease=%+v err=%v, want provider A", lease, err)
	}

	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM provider WHERE id = $1 FOR UPDATE`, providerB.ID); err != nil {
		t.Fatal(err)
	}
	moveCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, moveErr := db.RelocateStream(moveCtx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sourceA.ID}, resolver)
	cancel()
	if !errors.Is(moveErr, store.ErrAdmissionContention) ||
		!errors.Is(moveErr, context.DeadlineExceeded) {
		t.Fatalf("relocate with target provider locked err=%v, want admission contention deadline", moveErr)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var durableSource uuid.UUID
	if err := db.Pool.QueryRow(ctx,
		`SELECT channel_source_id FROM active_stream WHERE id = $1`, lease.ActiveStreamID).
		Scan(&durableSource); err != nil {
		t.Fatal(err)
	}
	if durableSource != sourceA.ID {
		t.Fatalf("contended relocation changed source to %s, want %s", durableSource, sourceA.ID)
	}
	moved, err := db.RelocateStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sourceA.ID}, resolver)
	if err != nil || moved.ChannelSourceID != sourceB.ID {
		t.Fatalf("relocate after provider release=%+v err=%v, want provider B", moved, err)
	}
}

// A same-provider DVR move is not usage-neutral when the current stream sits
// on a disabled credential. Moving it onto an enabled credential adds a new
// consumer to the enabled-account budget and must retain the live floor.
func TestIntegrationDVRRelocateFromDisabledCredentialPreservesLiveReserve(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	resolver := store.PassthroughResolverForTest{}
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "dvr-relocate-disabled-current", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	disabledCurrent, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "current", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	enabledTarget, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "target", PasswordEnc: []byte{1},
		MaxStreams: 2, Priority: 100, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	mainChannel, sources := seedChannelWithSources(t, db, provider.ID,
		"http://same.test/current", "http://same.test/target")
	lease, err := db.AcquireLease(ctx, mainChannel.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if lease.CredentialID != disabledCurrent.ID {
		t.Fatalf("initial credential=%s, want priority current %s", lease.CredentialID, disabledCurrent.ID)
	}
	if _, err := db.Pool.Exec(ctx,
		`UPDATE provider_credential SET enabled = false WHERE id = $1`, disabledCurrent.ID); err != nil {
		t.Fatal(err)
	}
	blocker, _ := seedChannelWithSources(t, db, provider.ID, "http://same.test/blocker")
	blockerLease, err := db.AcquireLease(ctx, blocker.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if blockerLease.CredentialID != enabledTarget.ID {
		t.Fatalf("blocker credential=%s, want enabled target %s", blockerLease.CredentialID, enabledTarget.ID)
	}

	_, err = db.RelocateDVRStream(ctx, lease.ActiveStreamID, mainChannel.ID,
		[]uuid.UUID{sources[0].ID}, resolver, store.DVRLeasePolicy{LiveReserve: 1})
	if !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("disabled-current DVR relocation err=%v, want live-floor ErrNoSlot", err)
	}
	var currentCredential uuid.UUID
	if err := db.Pool.QueryRow(ctx,
		`SELECT credential_id FROM active_stream WHERE id = $1`, lease.ActiveStreamID).
		Scan(&currentCredential); err != nil {
		t.Fatal(err)
	}
	if currentCredential != disabledCurrent.ID {
		t.Fatalf("rejected move changed credential to %s", currentCredential)
	}

	moved, err := db.RelocateStream(ctx, lease.ActiveStreamID, mainChannel.ID,
		[]uuid.UUID{sources[0].ID}, resolver)
	if err != nil {
		t.Fatalf("ordinary live-priority move into reserved slot: %v", err)
	}
	if moved.CredentialID != enabledTarget.ID || moved.ChannelSourceID != sources[1].ID {
		t.Fatalf("ordinary move=%+v, want enabled target/source", moved)
	}
}

// Relocation uses the same strict error contract as initial acquisition: a
// later full source cannot erase an earlier resolver failure and masquerade as
// unanimous capacity exhaustion.
func TestIntegrationRelocateMixedResolverAndCapacityIsNotNoSlot(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	seedProvider := func(name string) store.Provider {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{Name: name, Kind: "m3u_xtream", Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
			MaxStreams: 1, Priority: 100, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	currentProvider := seedProvider("relocate-current")
	brokenProvider := seedProvider("relocate-broken")
	fullProvider := seedProvider("relocate-full")
	channel, err := db.CreateChannel(ctx, store.Channel{Number: 953, Name: "relocate-mixed", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	var currentSource store.ChannelSource
	for i, candidate := range []store.ChannelSource{
		{ChannelID: channel.ID, ProviderID: currentProvider.ID, UpstreamURL: "http://current.test/stream", Priority: 0, HealthScore: 1, Enabled: true},
		{ChannelID: channel.ID, ProviderID: brokenProvider.ID, UpstreamURL: "resolver-fails://relocate", Priority: 1, HealthScore: 1, Enabled: true},
		{ChannelID: channel.ID, ProviderID: fullProvider.ID, UpstreamURL: "http://full.test/relocate", Priority: 2, HealthScore: 1, Enabled: true},
	} {
		source, err := db.CreateChannelSource(ctx, candidate)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			currentSource = source
		}
	}
	fullBlocker, err := db.CreateChannel(ctx, store.Channel{Number: 954, Name: "relocate-full-blocker", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: fullBlocker.ID, ProviderID: fullProvider.ID, UpstreamURL: "http://full.test/blocker",
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	resolver := selectiveResolver{}
	lease, err := db.AcquireLease(ctx, channel.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireLease(ctx, fullBlocker.ID, resolver); err != nil {
		t.Fatal(err)
	}
	_, err = db.RelocateStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{currentSource.ID}, resolver)
	if err == nil || errors.Is(err, store.ErrNoSlot) || !errors.Is(err, errSelectiveResolver) {
		t.Fatalf("mixed relocation err=%v, want preserved resolver failure and not ErrNoSlot", err)
	}
}

func TestIntegrationRelocateMixedResolverAndDeadlineIsOperational(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	seedProvider := func(name string) store.Provider {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{Name: name, Kind: "m3u_xtream", Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
			MaxStreams: 1, Priority: 100, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	current := seedProvider("relocate-deadline-current")
	broken := seedProvider("relocate-deadline-broken")
	blocked := seedProvider("relocate-deadline-blocked")
	channel, err := db.CreateChannel(ctx, store.Channel{Number: 955, Name: "relocate-deadline", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	var currentSource store.ChannelSource
	for i, source := range []store.ChannelSource{
		{ChannelID: channel.ID, ProviderID: current.ID, UpstreamURL: "http://current.test/deadline", Priority: 0, HealthScore: 1, Enabled: true},
		{ChannelID: channel.ID, ProviderID: broken.ID, UpstreamURL: "resolver-fails://relocate-deadline", Priority: 1, HealthScore: 1, Enabled: true},
		{ChannelID: channel.ID, ProviderID: blocked.ID, UpstreamURL: "http://blocked.test/relocate-deadline", Priority: 2, HealthScore: 1, Enabled: true},
	} {
		created, err := db.CreateChannelSource(ctx, source)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			currentSource = created
		}
	}
	lease, err := db.AcquireLease(ctx, channel.ID, selectiveResolver{})
	if err != nil {
		t.Fatal(err)
	}
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM provider WHERE id = $1 FOR UPDATE`, blocked.ID); err != nil {
		t.Fatal(err)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = db.RelocateDVRStream(attemptCtx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{currentSource.ID}, selectiveResolver{}, store.DVRLeasePolicy{})
	if !errors.Is(err, store.ErrLeaseOperational) || !errors.Is(err, errSelectiveResolver) ||
		!errors.Is(err, context.DeadlineExceeded) || errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("mixed relocation resolver/deadline classification err=%v", err)
	}
}
