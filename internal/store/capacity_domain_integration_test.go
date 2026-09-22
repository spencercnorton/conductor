package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spencercnorton/conductor/internal/store"
)

func seedCapacityDomainProvider(
	t *testing.T,
	db *store.DB,
	name, domain string,
	maxStreams int,
) store.Provider {
	t.Helper()
	ctx := context.Background()
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: name + "-" + uuid.NewString(), Kind: "m3u_xtream",
		BaseURL: "http://provider.test", CapacityDomain: domain, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
		MaxStreams: maxStreams, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	return provider
}

func seedCapacityDomainChannel(
	t *testing.T,
	db *store.DB,
	providerID uuid.UUID,
	number float64,
	name string,
) store.Channel {
	t.Helper()
	ctx := context.Background()
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: number, Name: name + "-" + uuid.NewString(), Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: providerID,
		UpstreamURL: "http://provider.test/" + channel.ID.String(),
		Priority:    0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	return channel
}

func TestIntegrationCapacityDomainMigrationBackfillsIsolatedDefault(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	dsn := startTestPostgres(t)
	schema := "op473_domain_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	root, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = root.Close(ctx)
		t.Fatal(err)
	}
	_ = root.Close(ctx)
	t.Cleanup(func() {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err == nil {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			_ = conn.Close(context.Background())
		}
	})

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	migrations, err := store.LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var migration26 store.Migration
	for _, migration := range migrations {
		if migration.Name == "0026_provider_capacity_domain.sql" {
			migration26 = migration
			break
		}
		if err := applyDVRAdmissionMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply pre-0026 %s: %v", migration.Name, err)
		}
	}
	if migration26.Name == "" {
		t.Fatal("migration 0026 not found")
	}
	var legacyID uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO provider (name, kind, base_url)
		VALUES ('legacy-domain-provider', 'm3u_plain', 'http://legacy.test')
		RETURNING id`).Scan(&legacyID); err != nil {
		t.Fatal(err)
	}
	if err := applyDVRAdmissionMigration(ctx, pool, migration26); err != nil {
		t.Fatalf("apply 0026: %v", err)
	}
	var legacyDomain, newDomain string
	if err := pool.QueryRow(ctx,
		`SELECT capacity_domain FROM provider WHERE id=$1`, legacyID).Scan(&legacyDomain); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO provider (name, kind, base_url)
		VALUES ('new-domain-provider', 'm3u_plain', 'http://new.test')
		RETURNING capacity_domain`).Scan(&newDomain); err != nil {
		t.Fatal(err)
	}
	if legacyDomain != "" || newDomain != "" {
		t.Fatalf("capacity-domain defaults legacy=%q new=%q, want isolated empty", legacyDomain, newDomain)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE provider SET capacity_domain='Invalid Domain' WHERE id=$1`, legacyID); err == nil {
		t.Fatal("database accepted invalid capacity-domain slug")
	}
	var predicate string
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_expr(i.indpred, i.indrelid)
		  FROM pg_index i
		  JOIN pg_class c ON c.oid=i.indexrelid
		 WHERE c.relname='provider_capacity_domain_idx'`).Scan(&predicate); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(predicate, "capacity_domain <> ''") {
		t.Fatalf("capacity-domain index predicate=%q, want nonempty partial index", predicate)
	}
}

func TestIntegrationCapacityDomainForecastRuntimeLiveAndSameChannelSharing(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerA := seedCapacityDomainProvider(t, db, "domain-a", "iboost-shared", 1)
	providerB := seedCapacityDomainProvider(t, db, "domain-b", "iboost-shared", 1)
	channelA := seedCapacityDomainChannel(t, db, providerA.ID, 9473.1, "domain-a")
	channelB := seedCapacityDomainChannel(t, db, providerB.ID, 9473.2, "domain-b")
	policy := store.DVRAdmissionPolicy{
		LiveReserve: 1, StartSlack: time.Second, MaxOverrun: time.Second,
	}
	start := time.Now().Add(time.Hour).UTC()
	first, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channelA.ID, Title: "domain-first", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/domain-first.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channelB.ID, Title: "domain-second", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/domain-second.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if first.AdmissionState != "pending" || second.AdmissionState != "queued" ||
		!strings.Contains(second.AdmissionReason, "provider/domain DVR budget") {
		t.Fatalf("domain forecast first=%+v second=%+v", first, second)
	}

	dvrLease, err := db.AcquireDVRLease(ctx, channelA.ID,
		store.PassthroughResolverForTest{}, store.DVRLeasePolicy{LiveReserve: 1})
	if err != nil {
		t.Fatalf("first domain DVR: %v", err)
	}
	viewerLease, err := db.AcquireLease(ctx, channelA.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatalf("same-channel viewer attach: %v", err)
	}
	if viewerLease.ActiveStreamID != dvrLease.ActiveStreamID || viewerLease.Outcome != store.LeaseShared {
		t.Fatalf("viewer lease=%+v, want shared DVR stream %s", viewerLease, dvrLease.ActiveStreamID)
	}
	var streamCount, clientCount int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int, COALESCE(MAX(client_count), 0)::int
		  FROM active_stream
		 WHERE state IN ('starting','running','draining')`).Scan(&streamCount, &clientCount); err != nil {
		t.Fatal(err)
	}
	if streamCount != 1 || clientCount != 2 {
		t.Fatalf("same-channel sharing streams=%d clients=%d, want 1/2", streamCount, clientCount)
	}
	if _, err := db.AcquireDVRLease(ctx, channelB.ID,
		store.PassthroughResolverForTest{}, store.DVRLeasePolicy{LiveReserve: 1}); !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("second distinct DVR err=%v, want domain live-floor ErrNoSlot", err)
	}
	liveLease, err := db.AcquireLease(ctx, channelB.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatalf("ordinary live tune did not consume retained floor: %v", err)
	}
	if liveLease.ActiveStreamID == dvrLease.ActiveStreamID {
		t.Fatal("distinct live channel unexpectedly shared DVR stream")
	}
}

func TestIntegrationCapacityDomainConcurrentDVRAdmissionSerializes(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerA := seedCapacityDomainProvider(t, db, "concurrent-a", "concurrent-shared", 1)
	providerB := seedCapacityDomainProvider(t, db, "concurrent-b", "concurrent-shared", 1)
	channels := []store.Channel{
		seedCapacityDomainChannel(t, db, providerA.ID, 9473.3, "concurrent-a"),
		seedCapacityDomainChannel(t, db, providerB.ID, 9473.4, "concurrent-b"),
	}

	start := make(chan struct{})
	errs := make(chan error, len(channels))
	var ready sync.WaitGroup
	ready.Add(len(channels))
	for _, channel := range channels {
		channel := channel
		go func() {
			ready.Done()
			<-start
			_, err := db.AcquireDVRLease(ctx, channel.ID,
				store.PassthroughResolverForTest{}, store.DVRLeasePolicy{LiveReserve: 1})
			errs <- err
		}()
	}
	ready.Wait()
	close(start)
	var admitted, noSlot int
	for range channels {
		err := <-errs
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, store.ErrNoSlot):
			noSlot++
		default:
			t.Fatalf("unexpected concurrent admission error: %v", err)
		}
	}
	if admitted != 1 || noSlot != 1 {
		t.Fatalf("concurrent domain results admitted=%d no_slot=%d, want 1/1", admitted, noSlot)
	}
	var active int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM active_stream
		 WHERE state IN ('starting','running','draining')`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("concurrent domain admitted %d durable streams, want 1", active)
	}
}

func TestIntegrationCapacityDomainForecastAlsoEnforcesProviderLocalCapacity(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerA := seedCapacityDomainProvider(t, db, "local-a", "local-shared", 1)
	providerB := seedCapacityDomainProvider(t, db, "local-b", "local-shared", 1)
	channels := []store.Channel{
		seedCapacityDomainChannel(t, db, providerA.ID, 9473.5, "local-a-first"),
		seedCapacityDomainChannel(t, db, providerA.ID, 9473.6, "local-a-second"),
		seedCapacityDomainChannel(t, db, providerB.ID, 9473.7, "local-b"),
	}
	start := time.Now().Add(time.Hour).UTC()
	policy := store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second}
	states := make([]string, 0, len(channels))
	for i, channel := range channels {
		recording, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: fmt.Sprintf("local-%d", i), Priority: 100,
			ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
			RequestedBy: "test", OutputPath: fmt.Sprintf("/tmp/local-%d.ts", i),
		}, policy)
		if err != nil {
			t.Fatal(err)
		}
		states = append(states, recording.AdmissionState)
	}
	if got, want := fmt.Sprint(states), "[pending queued pending]"; got != want {
		t.Fatalf("provider+domain forecast states=%s, want %s", got, want)
	}
}

func TestIntegrationEmptyCapacityDomainsRemainIsolated(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerA := seedCapacityDomainProvider(t, db, "isolated-a", "", 1)
	providerB := seedCapacityDomainProvider(t, db, "isolated-b", "", 1)
	channelA := seedCapacityDomainChannel(t, db, providerA.ID, 9473.8, "isolated-a")
	channelB := seedCapacityDomainChannel(t, db, providerB.ID, 9473.9, "isolated-b")
	for _, channel := range []store.Channel{channelA, channelB} {
		if _, err := db.AcquireDVRLease(ctx, channel.ID,
			store.PassthroughResolverForTest{}, store.DVRLeasePolicy{LiveReserve: 1}); err != nil {
			t.Fatalf("isolated one-slot provider DVR rejected: %v", err)
		}
	}
}
