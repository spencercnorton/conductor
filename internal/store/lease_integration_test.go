// lease_integration_test.go — exercises the credential lease against a
// real Postgres. Gated on CONDUCTOR_INT_TEST=1 because it requires:
//   - a running Postgres reachable at CONDUCTOR_TEST_DSN, OR
//   - Docker on $PATH (the test will start a throwaway pg container).
//
// CI runs unit tests only. To run integrations locally:
//
//	CONDUCTOR_INT_TEST=1 go test -race -count=1 ./internal/store -run Integration
//
// The race-condition test is the load-bearing acceptance criterion for
// Phase 1 per spec §16: under contention, the slot count in the DB must
// equal the slot count any single sane consumer would compute, with no
// double-leasing of the same slot.
package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/spencercnorton/conductor/internal/security"
	"github.com/spencercnorton/conductor/internal/store"
)

const (
	pgImage         = "postgres:16-alpine"
	pgContainerName = "conductor-test-pg"
	pgPort          = "55432" // unlikely to collide with a dev pg
	pgPassword      = "conductor-test"
)

func skipIfNoIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
}

// startTestPostgres returns a DSN to a fresh Postgres. Prefers an existing
// CONDUCTOR_TEST_DSN; otherwise spins up a docker container.
func startTestPostgres(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("CONDUCTOR_TEST_DSN"); dsn != "" {
		return dsn
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available and CONDUCTOR_TEST_DSN not set")
	}
	// Best-effort cleanup of a prior run.
	_ = exec.Command("docker", "rm", "-f", "-v", pgContainerName).Run()

	cmd := exec.Command("docker", "run", "-d",
		"--name", pgContainerName,
		"-p", pgPort+":5432",
		"-e", "POSTGRES_PASSWORD="+pgPassword,
		"-e", "POSTGRES_DB=conductor_test",
		"--health-cmd=pg_isready -U postgres",
		"--health-interval=1s",
		"--health-timeout=2s",
		"--health-retries=10",
		pgImage)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", "-v", pgContainerName).Run()
	})

	dsn := fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%s/conductor_test?sslmode=disable", pgPassword, pgPort)

	// Wait up to 30s for the container to accept connections.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("postgres container did not become ready in time")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			conn.Close(context.Background())
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return dsn
}

func freshDB(t *testing.T) *store.DB {
	t.Helper()
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)

	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(db.Close)

	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// Conductor's pump registry and startup reconciliation are process-local.
// The dedicated session lock therefore rejects a second runtime and becomes
// available again only after the first owner explicitly releases its session.
func TestIntegrationRuntimeSingletonExcludesAndReacquires(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	first, err := db.AcquireRuntimeSingleton(ctx)
	if err != nil {
		t.Fatalf("first runtime singleton: %v", err)
	}
	if second, err := db.AcquireRuntimeSingleton(ctx); !errors.Is(err, store.ErrRuntimeAlreadyActive) {
		if second != nil {
			_ = second.Close(ctx)
		}
		t.Fatalf("second runtime singleton err=%v, want ErrRuntimeAlreadyActive", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatalf("release first runtime singleton: %v", err)
	}

	reacquired, err := db.AcquireRuntimeSingleton(ctx)
	if err != nil {
		t.Fatalf("reacquire runtime singleton: %v", err)
	}

	monitorDone := make(chan error, 1)
	go func() { monitorDone <- reacquired.Monitor(ctx, 10*time.Millisecond) }()
	var ownerPID int32
	if err := db.Pool.QueryRow(ctx, `
		SELECT pid
		  FROM pg_locks
		 WHERE locktype = 'advisory' AND granted
		   AND pid <> pg_backend_pid()
		 ORDER BY pid
		 LIMIT 1`).Scan(&ownerPID); err != nil {
		t.Fatalf("find runtime singleton backend: %v", err)
	}
	var terminated bool
	if err := db.Pool.QueryRow(ctx,
		`SELECT pg_terminate_backend($1)`, ownerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate runtime singleton backend: terminated=%v err=%v", terminated, err)
	}
	select {
	case err := <-monitorDone:
		if err == nil || !strings.Contains(err.Error(), "runtime singleton connection lost") {
			t.Fatalf("monitor loss err=%v, want connection-lost error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime singleton monitor did not promptly detect session loss")
	}
	// Close must discard the broken dedicated connection. Session termination
	// already released the advisory lock, so another owner can then acquire.
	_ = reacquired.Close(ctx)
	finalOwner, err := db.AcquireRuntimeSingleton(ctx)
	if err != nil {
		t.Fatalf("acquire after owner session loss: %v", err)
	}
	if err := finalOwner.Close(ctx); err != nil {
		t.Fatalf("release final runtime singleton: %v", err)
	}
}

// seedProviderWithCredentials creates one provider with N credentials each
// at maxStreams. Returns the provider id and credential ids.
func seedProviderWithCredentials(t *testing.T, db *store.DB, nCreds, maxStreams int) (uuid.UUID, []uuid.UUID) {
	ctx := context.Background()

	p, err := db.CreateProvider(ctx, store.Provider{
		Name: "test-provider", Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rawKey := make([]byte, 32)
	_, _ = rand.Read(rawKey)
	k, err := security.LoadCredKey(hex.EncodeToString(rawKey))
	if err != nil {
		t.Fatal(err)
	}

	credIDs := make([]uuid.UUID, 0, nCreds)
	for i := 0; i < nCreds; i++ {
		enc, _ := k.Encrypt([]byte("password" + fmt.Sprint(i)))
		c, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID:  p.ID,
			Username:    fmt.Sprintf("user%d", i),
			PasswordEnc: enc,
			MaxStreams:  maxStreams,
			Priority:    100,
			Enabled:     true,
		})
		if err != nil {
			t.Fatal(err)
		}
		credIDs = append(credIDs, c.ID)
	}
	return p.ID, credIDs
}

func seedLeaseChannel(t *testing.T, db *store.DB, providerID uuid.UUID, number float64, sourceCount int) store.Channel {
	t.Helper()
	ctx := context.Background()
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: number, Name: fmt.Sprintf("lease-channel-%.0f", number), Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < sourceCount; i++ {
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: ch.ID, ProviderID: providerID,
			UpstreamURL: fmt.Sprintf("http://provider.test/%s/%d.ts", ch.ID, i),
			Priority:    i, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return ch
}

var errSelectiveResolver = errors.New("selective resolver failure")

type selectiveResolver struct{}

func (selectiveResolver) Resolve(_ context.Context, _ store.ProviderCredential, urlTemplate string) (string, error) {
	if strings.Contains(urlTemplate, "resolver-fails") {
		return "", errSelectiveResolver
	}
	return urlTemplate, nil
}

// A later capacity-only source must not erase an earlier decrypt/resolver/DB
// failure. ErrNoSlot is reserved for unanimous, strict capacity exhaustion so
// the DVR scheduler never queues a broken source as temporary contention.
func TestIntegrationAcquireLeaseMixedResolverAndCapacityIsNotNoSlot(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	createProvider := func(name string) store.Provider {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{
			Name: name, Kind: "m3u_xtream", BaseURL: "http://provider.test", Enabled: true,
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
	brokenProvider := createProvider("resolver-error-provider")
	fullProvider := createProvider("capacity-provider")

	blocker := seedLeaseChannel(t, db, fullProvider.ID, 806, 1)
	if _, err := db.AcquireLease(ctx, blocker.ID, store.PassthroughResolverForTest{}); err != nil {
		t.Fatalf("fill capacity provider: %v", err)
	}
	candidate, err := db.CreateChannel(ctx, store.Channel{Number: 807, Name: "mixed-errors", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, src := range []store.ChannelSource{
		{ChannelID: candidate.ID, ProviderID: brokenProvider.ID, UpstreamURL: "resolver-fails://one", Priority: 0, HealthScore: 1, Enabled: true},
		{ChannelID: candidate.ID, ProviderID: fullProvider.ID, UpstreamURL: "http://provider.test/full", Priority: 1, HealthScore: 1, Enabled: true},
	} {
		if _, err := db.CreateChannelSource(ctx, src); err != nil {
			t.Fatal(err)
		}
	}

	_, err = db.AcquireLease(ctx, candidate.ID, selectiveResolver{})
	if !errors.Is(err, errSelectiveResolver) {
		t.Fatalf("mixed acquisition err=%v, want resolver cause", err)
	}
	if errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("mixed acquisition was mislabeled ErrNoSlot: %v", err)
	}
}

// A later source timing out on a provider lock must not erase an earlier
// resolver/decrypt failure. The mixed result remains operational even though
// callers can still inspect the deadline cause.
func TestIntegrationAcquireLeaseMixedResolverAndDeadlineIsOperational(t *testing.T) {
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
	broken := seedProvider("mixed-deadline-broken")
	blocked := seedProvider("mixed-deadline-blocked")
	channel, err := db.CreateChannel(ctx, store.Channel{Number: 809, Name: "mixed-deadline", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []store.ChannelSource{
		{ChannelID: channel.ID, ProviderID: broken.ID, UpstreamURL: "resolver-fails://deadline", Priority: 0, HealthScore: 1, Enabled: true},
		{ChannelID: channel.ID, ProviderID: blocked.ID, UpstreamURL: "http://blocked.test/deadline", Priority: 1, HealthScore: 1, Enabled: true},
	} {
		if _, err := db.CreateChannelSource(ctx, source); err != nil {
			t.Fatal(err)
		}
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
	_, err = db.AcquireDVRLease(attemptCtx, channel.ID, selectiveResolver{}, store.DVRLeasePolicy{})
	if !errors.Is(err, store.ErrLeaseOperational) || !errors.Is(err, errSelectiveResolver) ||
		!errors.Is(err, context.DeadlineExceeded) || errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("mixed resolver/deadline classification err=%v", err)
	}
}

func TestIntegrationAcquireLeasePreservesCancelledContext(t *testing.T) {
	db := freshDB(t)
	providerID, _ := seedProviderWithCredentials(t, db, 1, 1)
	ch := seedLeaseChannel(t, db, providerID, 808, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := db.AcquireLease(ctx, ch.ID, store.PassthroughResolverForTest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquisition err=%v, want context.Canceled", err)
	}
	if !errors.Is(err, store.ErrLeaseOperational) {
		t.Fatalf("cancelled acquisition err=%v, want operational marker", err)
	}
	if errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("cancelled acquisition was mislabeled ErrNoSlot: %v", err)
	}
}

// SKIP LOCKED returning no credential does not by itself prove contention:
// the skipped credential can already be fully occupied in committed state.
// Saturation must remain ErrNoSlot so callers enter the capacity path.
func TestIntegrationAcquireLockedFullyUtilizedCredentialIsNoSlot(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, credentialIDs := seedProviderWithCredentials(t, db, 1, 1)
	blocker := seedLeaseChannel(t, db, providerID, 808.1, 1)
	candidate := seedLeaseChannel(t, db, providerID, 808.2, 1)
	if _, err := db.AcquireLease(ctx, blocker.ID, store.PassthroughResolverForTest{}); err != nil {
		t.Fatalf("fill credential: %v", err)
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

	_, err = db.AcquireLease(ctx, candidate.ID, store.PassthroughResolverForTest{})
	if !errors.Is(err, store.ErrNoSlot) || errors.Is(err, store.ErrAdmissionContention) {
		t.Fatalf("locked full credential err=%v, want ErrNoSlot without contention", err)
	}
}

// The inverse SKIP LOCKED case must stay contention: if the free credential is
// locked and the selector falls through to an unlocked full credential, the
// provider is not saturated and must not be reported as ErrNoSlot.
func TestIntegrationAcquireLockedFreeCredentialIsContention(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, credentialIDs := seedProviderWithCredentials(t, db, 2, 1)
	if _, err := db.Pool.Exec(ctx,
		`UPDATE provider_credential SET priority = 0 WHERE id = $1`, credentialIDs[0]); err != nil {
		t.Fatal(err)
	}
	blocker := seedLeaseChannel(t, db, providerID, 808.3, 1)
	candidate := seedLeaseChannel(t, db, providerID, 808.4, 1)
	lease, err := db.AcquireLease(ctx, blocker.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatalf("fill first credential: %v", err)
	}
	if lease.CredentialID != credentialIDs[0] {
		t.Fatalf("blocker credential=%s, want priority credential %s",
			lease.CredentialID, credentialIDs[0])
	}

	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM provider_credential WHERE id = $1 FOR UPDATE`, credentialIDs[1]); err != nil {
		t.Fatal(err)
	}

	_, err = db.AcquireLease(ctx, candidate.ID, store.PassthroughResolverForTest{})
	if !errors.Is(err, store.ErrAdmissionContention) || errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("locked free credential err=%v, want contention without ErrNoSlot", err)
	}
}

// A viewer already holding the provider's only slot and a DVR request for the
// same channel must share that upstream. The DVR live-reserve policy applies
// only to a new distinct-channel capture.
func TestIntegrationDVRLeaseSharesLiveChannel(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 1)
	ch := seedLeaseChannel(t, db, providerID, 810, 1)
	resolver := store.PassthroughResolverForTest{}

	live, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatalf("live lease: %v", err)
	}
	dvrLease, err := db.AcquireDVRLease(ctx, ch.ID, resolver, store.DVRLeasePolicy{LiveReserve: 1})
	if err != nil {
		t.Fatalf("same-channel dvr lease: %v", err)
	}
	if dvrLease.Outcome != store.LeaseShared || dvrLease.ActiveStreamID != live.ActiveStreamID {
		t.Fatalf("dvr lease = %+v, want shared stream %s", dvrLease, live.ActiveStreamID)
	}

	var streams, clients int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int, COALESCE(SUM(client_count), 0)::int
		  FROM active_stream WHERE state IN ('starting','running')`).Scan(&streams, &clients); err != nil {
		t.Fatal(err)
	}
	if streams != 1 || clients != 2 {
		t.Fatalf("active streams=%d clients=%d, want one shared stream/two clients", streams, clients)
	}
}

func TestIntegrationDVRLeaseReserveClampsForOneSlotProvider(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 1)
	ch := seedLeaseChannel(t, db, providerID, 815, 1)
	lease, err := db.AcquireDVRLease(ctx, ch.ID, store.PassthroughResolverForTest{},
		store.DVRLeasePolicy{LiveReserve: 1})
	if err != nil {
		t.Fatalf("one-slot provider must still admit one DVR: %v", err)
	}
	if lease.Outcome != store.LeaseNew {
		t.Fatalf("outcome=%v, want new lease", lease.Outcome)
	}
}

// Credential slot budgets are independent. A still-live disabled account is
// attachable by the same channel, but it neither consumes nor manufactures
// capacity on the provider's remaining enabled accounts.
func TestIntegrationDVRLeaseSeparatesDisabledCredentialCapacity(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 3, 1)
	liveChannel := seedLeaseChannel(t, db, providerID, 817, 1)
	dvrZeroChannel := seedLeaseChannel(t, db, providerID, 818, 1)
	dvrReserveChannel := seedLeaseChannel(t, db, providerID, 819, 1)
	dvrBlockedChannel := seedLeaseChannel(t, db, providerID, 819.1, 1)
	resolver := store.PassthroughResolverForTest{}

	live, err := db.AcquireLease(ctx, liveChannel.ID, resolver)
	if err != nil {
		t.Fatalf("live lease: %v", err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE provider_credential SET enabled = false WHERE id = $1`, live.CredentialID); err != nil {
		t.Fatal(err)
	}
	shared, err := db.AcquireDVRLease(ctx, liveChannel.ID, resolver,
		store.DVRLeasePolicy{LiveReserve: 1})
	if err != nil || shared.Outcome != store.LeaseShared || shared.ActiveStreamID != live.ActiveStreamID {
		t.Fatalf("same-channel disabled-credential attach=%+v err=%v", shared, err)
	}
	zeroReserve, err := db.AcquireDVRLease(ctx, dvrZeroChannel.ID, resolver,
		store.DVRLeasePolicy{LiveReserve: 0})
	if err != nil {
		t.Fatalf("reserve=0 should use an enabled credential: %v", err)
	}
	if zeroReserve.CredentialID == live.CredentialID {
		t.Fatal("new DVR reused disabled credential")
	}
	if count, err := db.ReleaseClient(ctx, zeroReserve.ActiveStreamID); err != nil || count != 0 {
		t.Fatalf("release reserve=0 lease: count=%d err=%v", count, err)
	}
	if err := db.MarkDead(ctx, zeroReserve.ActiveStreamID); err != nil {
		t.Fatal(err)
	}

	if _, err := db.AcquireDVRLease(ctx, dvrReserveChannel.ID, resolver,
		store.DVRLeasePolicy{LiveReserve: 1}); err != nil {
		t.Fatalf("two enabled free slots should admit one DVR and retain one: %v", err)
	}
	if _, err := db.AcquireDVRLease(ctx, dvrBlockedChannel.ID, resolver,
		store.DVRLeasePolicy{LiveReserve: 1}); !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("last enabled slot should remain live-reserved: err=%v", err)
	}
}

// Three actual provider slots with one held for live TV admit two distinct DVR
// channels. A third stays queued by the scheduler; once either reservation is
// released, the exact same transactional request succeeds.
func TestIntegrationDVRLeaseReserveAndRetryAfterRelease(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 3)
	channels := []store.Channel{
		seedLeaseChannel(t, db, providerID, 820, 1),
		seedLeaseChannel(t, db, providerID, 821, 1),
		seedLeaseChannel(t, db, providerID, 822, 1),
	}
	resolver := store.PassthroughResolverForTest{}
	policy := store.DVRLeasePolicy{LiveReserve: 1}

	first, err := db.AcquireDVRLease(ctx, channels[0].ID, resolver, policy)
	if err != nil {
		t.Fatalf("first dvr lease: %v", err)
	}
	if _, err := db.AcquireDVRLease(ctx, channels[1].ID, resolver, policy); err != nil {
		t.Fatalf("second dvr lease: %v", err)
	}
	if _, err := db.AcquireDVRLease(ctx, channels[2].ID, resolver, policy); !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("third dvr lease err=%v, want ErrNoSlot with one live slot retained", err)
	}

	count, err := db.ReleaseClient(ctx, first.ActiveStreamID)
	if err != nil || count != 0 {
		t.Fatalf("release first lease: count=%d err=%v", count, err)
	}
	if err := db.MarkDead(ctx, first.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	retried, err := db.AcquireDVRLease(ctx, channels[2].ID, resolver, policy)
	if err != nil {
		t.Fatalf("retry after release: %v", err)
	}
	if retried.Outcome != store.LeaseNew {
		t.Fatalf("retry outcome=%v, want new lease", retried.Outcome)
	}
}

// Operator cancellation is the state-machine linearization point. Recorder
// cleanup already in flight must not overwrite it with either terminal result.
func TestIntegrationDVRCancellationWinsCompletedAndFailedCAS(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 1)
	channel := seedLeaseChannel(t, db, providerID, 823.5, 1)
	start := time.Now().Add(time.Hour)
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "cancel wins terminal CAS",
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		Priority: 100, RequestedBy: "test", OutputPath: t.TempDir() + "/cancel.ts",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, rec.ID, rec.OutputPath)
	if err != nil || !claimed {
		t.Fatalf("claim recording: claimed=%v err=%v", claimed, err)
	}
	if err := db.MarkDVRRecordingCancelled(ctx, rec.ID); err != nil {
		t.Fatal(err)
	}
	completion, err := db.MarkDVRRecordingCompleted(ctx, rec.ID, uuid.New(), 1234)
	if err != nil || completion.State != "cancelled" || completion.OperationOwned {
		t.Fatalf("completion overwrote cancellation: result=%+v err=%v", completion, err)
	}
	if updated, err := db.MarkDVRRecordingFailed(ctx, rec.ID, "late failure", 5678); err != nil || updated {
		t.Fatalf("failure overwrote cancellation: updated=%v err=%v", updated, err)
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "cancelled" || got.BytesWritten != 0 || got.Error != "" {
		t.Fatalf("terminal CAS changed cancelled row: %+v", got)
	}
}

// Schedule-time forecasting is deliberately advisory: it does not create an
// active_stream. It must make a third overlap visible immediately, preserve
// chronological non-preemption, and use priority within equal start times.
func TestIntegrationDVRForecastSurfacesThirdOverlapAndPriority(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 3)
	policy := store.DVRAdmissionPolicy{
		LiveReserve: 1,
		StartSlack:  30 * time.Second,
		MaxOverrun:  90 * time.Second,
	}

	create := func(number float64, title string, priority int, start time.Time) store.DVRRecording {
		t.Helper()
		ch := seedLeaseChannel(t, db, providerID, number, 1)
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: ch.ID, Title: title, Priority: priority,
			ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
			RequestedBy: "test", OutputPath: "/tmp/" + title + ".ts",
		}, policy)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	assertQueued := func(rec store.DVRRecording) {
		t.Helper()
		if rec.AdmissionState != "queued" || rec.AdmissionRetryAt == nil ||
			!strings.HasPrefix(rec.AdmissionReason, "capacity forecast:") || rec.AdmissionAttempts != 0 {
			t.Fatalf("forecast metadata not surfaced: %+v", rec)
		}
	}

	// Equal priority falls through to creation time, so the literal third
	// overlapping request is returned with durable queued metadata.
	firstWindow := time.Now().Add(time.Hour).UTC()
	first := create(824, "forecast-first", 100, firstWindow)
	second := create(825, "forecast-second", 100, firstWindow)
	third := create(826, "forecast-third", 100, firstWindow)
	assertQueued(third)
	// Simulate migration 0019's pending backfill, then exercise the same
	// best-effort refresh Scheduler.Run performs at startup.
	if _, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET admission_state = 'pending', admission_retry_at = NULL,
		       admission_reason = '', error = ''
		 WHERE id = ANY($1)`, []uuid.UUID{first.ID, second.ID, third.ID}); err != nil {
		t.Fatal(err)
	}
	if err := db.RefreshDVRAdmissionForecast(ctx, policy); err != nil {
		t.Fatal(err)
	}
	third, err := db.GetDVRRecording(ctx, third.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertQueued(third)
	forecastRetry := *third.AdmissionRetryAt
	if err := db.WakeQueuedDVRRecordings(ctx); err != nil {
		t.Fatal(err)
	}
	third, err = db.GetDVRRecording(ctx, third.ID)
	if err != nil {
		t.Fatal(err)
	}
	if third.AdmissionRetryAt == nil || !third.AdmissionRetryAt.Equal(forecastRetry) ||
		!strings.HasPrefix(third.AdmissionReason, "capacity forecast:") {
		t.Fatalf("runtime capacity wake rewrote future forecast metadata: %+v", third)
	}

	// For an identical start boundary, priority precedes creation time: a later
	// high-priority request displaces the earlier low-priority row.
	secondWindow := firstWindow.Add(3 * time.Hour)
	low := create(827, "forecast-low", 300, secondWindow)
	_ = create(828, "forecast-high", 10, secondWindow)
	mid := create(829, "forecast-mid", 20, secondWindow)
	if mid.AdmissionState != "pending" {
		t.Fatalf("third but higher-priority request state=%q, want pending", mid.AdmissionState)
	}
	low, err = db.GetDVRRecording(ctx, low.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertQueued(low)

	// A later start never preempts already-eligible commitments, even when the
	// later request has a much stronger priority. Two earlier starts consume
	// this provider's two-slot DVR budget before the staggered high request.
	thirdWindow := secondWindow.Add(3 * time.Hour)
	earlyLow := create(834, "forecast-staggered-early-low", 900, thirdWindow)
	earlySecond := create(835, "forecast-staggered-early-second", 800, thirdWindow.Add(time.Minute))
	laterHigh := create(836, "forecast-staggered-later-high", 1, thirdWindow.Add(2*time.Minute))
	earlyLow, err = db.GetDVRRecording(ctx, earlyLow.ID)
	if err != nil {
		t.Fatal(err)
	}
	earlySecond, err = db.GetDVRRecording(ctx, earlySecond.ID)
	if err != nil {
		t.Fatal(err)
	}
	if earlyLow.AdmissionState != "pending" || earlySecond.AdmissionState != "pending" {
		t.Fatalf("earlier commitments were preempted: low=%+v second=%+v", earlyLow, earlySecond)
	}
	assertQueued(laterHigh)

	var active int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM active_stream WHERE state IN ('starting','running')`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("schedule-time forecast held %d real provider slots, want zero", active)
	}
}

// The runtime candidate query must implement the same chronological order as
// the forecast: earlier starts first, then priority only at an equal boundary.
func TestIntegrationListPendingDVRRecordingsChronologicalOrder(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 10)
	base := time.Now().Add(-10 * time.Second).UTC()
	policy := store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second}

	create := func(number float64, title string, priority int, start time.Time) store.DVRRecording {
		t.Helper()
		ch := seedLeaseChannel(t, db, providerID, number, 1)
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: ch.ID, Title: title, Priority: priority,
			ScheduledStart: start, ScheduledEnd: time.Now().Add(time.Hour),
			RequestedBy: "test", OutputPath: "/tmp/" + title + ".ts",
		}, policy)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	earlierLow := create(837, "runtime-earlier-low", 900, base)
	laterHigh := create(838, "runtime-later-high", 1, base.Add(time.Second))
	tieLow := create(839, "runtime-tie-low", 500, base.Add(2*time.Second))
	tieHigh := create(840, "runtime-tie-high", 2, base.Add(2*time.Second))
	if err := db.QueueDVRRecordingForCapacity(ctx, earlierLow.ID,
		time.Now().Add(time.Hour), "queued for provider capacity (ordering regression)"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.ListPendingDVRRecordings(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := []uuid.UUID{earlierLow.ID, laterHigh.ID, tieHigh.ID, tieLow.ID}
	var got []uuid.UUID
	wanted := make(map[uuid.UUID]bool, len(want))
	for _, id := range want {
		wanted[id] = true
	}
	for _, row := range rows {
		if wanted[row.ID] {
			got = append(got, row.ID)
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pending order=%v, want chronological/equal-start priority %v", got, want)
	}
}

// Forecast and runtime must rank the same real source tuple. Independent
// MIN(priority)/MAX(health) aggregation would incorrectly synthesize A as
// 0/1.0 from two separate sources and rank it ahead of B's real 0/.9 tuple.
func TestIntegrationDVRForecastAndRuntimeUseSameProviderRank(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerAID := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	providerBID := uuid.MustParse("00000000-0000-0000-0000-0000000000b1")
	providerA, err := db.CreateProvider(ctx, store.Provider{
		ID: providerAID, Name: "rank-provider-a", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerB, err := db.CreateProvider(ctx, store.Provider{
		ID: providerBID, Name: "rank-provider-b", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	credA, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: providerA.ID, Username: "rank-a", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	credB, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: providerB.ID, Username: "rank-b", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.CreateChannel(ctx, store.Channel{Number: 841, Name: "rank-candidate", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []store.ChannelSource{
		{ChannelID: candidate.ID, ProviderID: providerA.ID, UpstreamURL: "http://a.test/weak", Priority: 0, HealthScore: .1, Enabled: true},
		{ChannelID: candidate.ID, ProviderID: providerA.ID, UpstreamURL: "http://a.test/healthy-backup", Priority: 100, HealthScore: 1, Enabled: true},
		{ChannelID: candidate.ID, ProviderID: providerB.ID, UpstreamURL: "http://b.test/best-real", Priority: 0, HealthScore: .9, Enabled: true},
	} {
		if _, err := db.CreateChannelSource(ctx, source); err != nil {
			t.Fatal(err)
		}
	}
	bOnly, err := db.CreateChannel(ctx, store.Channel{Number: 842, Name: "rank-b-only", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: bOnly.ID, ProviderID: providerB.ID, UpstreamURL: "http://b.test/only",
		Priority: 0, HealthScore: .9, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	start := time.Now().Add(time.Hour).UTC()
	policy := store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second}
	if _, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: candidate.ID, Title: "rank-candidate", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/rank-candidate.ts",
	}, policy); err != nil {
		t.Fatal(err)
	}
	bOnlyRec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: bOnly.ID, Title: "rank-b-only", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/rank-b-only.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if bOnlyRec.AdmissionState != "queued" {
		t.Fatalf("forecast chose a synthetic provider-A tuple; B-only state=%q", bOnlyRec.AdmissionState)
	}

	lease, err := db.AcquireDVRLease(ctx, candidate.ID, store.PassthroughResolverForTest{},
		store.DVRLeasePolicy{LiveReserve: 0})
	if err != nil {
		t.Fatal(err)
	}
	if lease.CredentialID != credB.ID || lease.CredentialID == credA.ID {
		t.Fatalf("runtime credential=%s, want provider-B credential %s", lease.CredentialID, credB.ID)
	}
}

// A row already recording is non-preemptible at schedule time. Its actual
// active_stream provider consumes the forecast budget before any newly
// scheduled priority winner is considered.
func TestIntegrationDVRForecastCountsRecordingCommitment(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 2)
	recordingChannel := seedLeaseChannel(t, db, providerID, 831, 1)
	requestChannel := seedLeaseChannel(t, db, providerID, 832, 1)
	policy := store.DVRAdmissionPolicy{
		LiveReserve: 1,
		StartSlack:  30 * time.Second,
		MaxOverrun:  90 * time.Second,
	}
	start := time.Now().Add(10 * time.Minute).UTC()

	if _, err := db.AcquireLease(ctx, recordingChannel.ID, store.PassthroughResolverForTest{}); err != nil {
		t.Fatalf("recording active stream: %v", err)
	}
	inFlight, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: recordingChannel.ID, Title: "already-recording", Priority: 300,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/already-recording.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, inFlight.ID, inFlight.OutputPath)
	if err != nil || !claimed {
		t.Fatalf("claim existing recording: claimed=%v err=%v", claimed, err)
	}

	request, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: requestChannel.ID, Title: "new-high-priority", Priority: 1,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/new-high-priority.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if request.AdmissionState != "queued" || request.AdmissionRetryAt == nil ||
		!strings.HasPrefix(request.AdmissionReason, "capacity forecast:") {
		t.Fatalf("recording commitment not surfaced on new request: %+v", request)
	}
}

// A recording pump on a disabled credential remains available to same-channel
// fan-out but does not consume another enabled credential's independent slot
// budget in the forecast.
func TestIntegrationDVRForecastSeparatesDisabledCredentialCapacity(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 3, 1)
	recordingChannel := seedLeaseChannel(t, db, providerID, 843, 1)
	sharedChannel := recordingChannel
	firstOther := seedLeaseChannel(t, db, providerID, 844, 1)
	secondOther := seedLeaseChannel(t, db, providerID, 845, 1)
	policy := store.DVRAdmissionPolicy{LiveReserve: 1, StartSlack: time.Second, MaxOverrun: time.Second}
	start := time.Now().Add(time.Hour).UTC()

	active, err := db.AcquireLease(ctx, recordingChannel.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		`UPDATE provider_credential SET enabled = false WHERE id = $1`, active.CredentialID); err != nil {
		t.Fatal(err)
	}
	inFlight, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: recordingChannel.ID, Title: "disabled-active-recording", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/disabled-active-recording.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, inFlight.ID, inFlight.OutputPath)
	if err != nil || !claimed {
		t.Fatalf("claim disabled active recording: claimed=%v err=%v", claimed, err)
	}
	create := func(channel store.Channel, title string) store.DVRRecording {
		t.Helper()
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: title, Priority: 100,
			ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
			RequestedBy: "test", OutputPath: "/tmp/" + title + ".ts",
		}, policy)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	// This recording extends beyond the already-running disabled-credential
	// commitment. Its partial overlap must carry that pump's non-counting role
	// forward, not manufacture a new enabled-account allocation after the
	// original row's forecast window ends.
	shared, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: sharedChannel.ID, Title: "disabled-active-shared", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(2 * time.Hour),
		RequestedBy: "test", OutputPath: "/tmp/disabled-active-shared.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	first := create(firstOther, "disabled-active-first-enabled")
	second := create(secondOther, "disabled-active-second-enabled")
	// Refetch after the full overlap set was recomputed.
	shared, _ = db.GetDVRRecording(ctx, shared.ID)
	first, _ = db.GetDVRRecording(ctx, first.ID)
	second, _ = db.GetDVRRecording(ctx, second.ID)
	if shared.AdmissionState != "pending" || first.AdmissionState != "pending" {
		t.Fatalf("disabled active row double-charged enabled capacity: shared=%+v first=%+v", shared, first)
	}
	if second.AdmissionState != "queued" ||
		!strings.HasPrefix(second.AdmissionReason, "capacity forecast:") {
		t.Fatalf("forecast failed to retain one enabled live slot: %+v", second)
	}
}

// A partial same-channel overlap shares the upstream while both recordings
// run, then keeps that upstream slot after the first recording ends. Forecast
// allocation must therefore carry the slot forward through the chain.
func TestIntegrationDVRForecastCarriesPartialSameChannelCapacity(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 1)
	channelX := seedLeaseChannel(t, db, providerID, 846, 1)
	channelY := seedLeaseChannel(t, db, providerID, 847, 1)
	policy := store.DVRAdmissionPolicy{
		LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second,
	}
	base := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	create := func(channel store.Channel, title string, start, end time.Time) store.DVRRecording {
		t.Helper()
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: title, Priority: 100,
			ScheduledStart: start, ScheduledEnd: end,
			RequestedBy: "test", OutputPath: "/tmp/" + title + ".ts",
		}, policy)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}

	firstX := create(channelX, "same-channel-chain-first",
		base, base.Add(time.Hour))
	secondX := create(channelX, "same-channel-chain-second",
		base.Add(30*time.Minute), base.Add(90*time.Minute))
	// Start after firstX's padded end, but while secondX is still running.
	competitorY := create(channelY, "same-channel-chain-competitor",
		base.Add(61*time.Minute), base.Add(90*time.Minute))
	firstX, _ = db.GetDVRRecording(ctx, firstX.ID)
	secondX, _ = db.GetDVRRecording(ctx, secondX.ID)
	competitorY, _ = db.GetDVRRecording(ctx, competitorY.ID)
	if firstX.AdmissionState != "pending" || secondX.AdmissionState != "pending" {
		t.Fatalf("same-channel chain was not admitted: first=%+v second=%+v", firstX, secondX)
	}
	if competitorY.AdmissionState != "queued" ||
		!strings.HasPrefix(competitorY.AdmissionReason, "capacity forecast:") {
		t.Fatalf("partial same-channel allocation lost its slot after predecessor: %+v", competitorY)
	}
}

// Under the enforced runtime singleton, the channel advisory lock makes "find
// existing or create" one serialized decision for concurrent local requests.
// Multiple sources and credentials must not create parallel upstreams.
func TestIntegrationChannelAttachOrCreateSerialized(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 4, 1)
	ch := seedLeaseChannel(t, db, providerID, 830, 2)
	resolver := store.PassthroughResolverForTest{}

	const concurrency = 20
	var wg sync.WaitGroup
	var failures atomic.Int64
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			var err error
			if i%2 == 0 {
				_, err = db.AcquireLease(ctx, ch.ID, resolver)
			} else {
				_, err = db.AcquireDVRLease(ctx, ch.ID, resolver, store.DVRLeasePolicy{})
			}
			if err != nil {
				failures.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d concurrent acquisitions failed", failures.Load())
	}

	var streams, clients int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int, COALESCE(SUM(client_count), 0)::int
		  FROM active_stream WHERE state IN ('starting','running')`).Scan(&streams, &clients); err != nil {
		t.Fatal(err)
	}
	if streams != 1 || clients != concurrency {
		t.Fatalf("streams=%d clients=%d, want one/%d", streams, clients, concurrency)
	}
}

// TestIntegrationLeaseDoesNotDoubleBook is the spec §16 Phase 1 acceptance:
// run K simultaneous lease attempts against N credentials × M slots each,
// and verify (a) no slot was double-booked, (b) every successful lease
// matches a real active_stream row.
func TestIntegrationLeaseDoesNotDoubleBook(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	const (
		nCreds      = 3
		maxPerCred  = 4
		concurrency = 24 // 2x the total capacity (12) — half should fail
	)

	provID, credIDs := seedProviderWithCredentials(t, db, nCreds, maxPerCred)

	// One channel with one source pointing at provID.
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 1, Name: "Test", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	src, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: provID,
		UpstreamURL: "http://provider.test/${USER}/${PASS}/stream.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = src

	resolver := store.PassthroughResolverForTest{}

	// Hammer N goroutines all asking for the same channel. Each one that
	// succeeds in acquiring a lease holds it (does NOT release) so the
	// slot accounting stays maxed during the run — this maximizes the
	// race-on-slot-budget pressure.
	var (
		wg        sync.WaitGroup
		newLeases atomic.Int64
		shared    atomic.Int64
		noSlot    atomic.Int64
		other     atomic.Int64
		gotIDs    sync.Map // active_stream.id → seen
	)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			l, err := db.AcquireLease(ctx, ch.ID, resolver)
			switch {
			case err == nil:
				switch l.Outcome {
				case store.LeaseNew:
					newLeases.Add(1)
				case store.LeaseShared:
					shared.Add(1)
				}
				gotIDs.Store(l.ActiveStreamID, true)
			case errors.Is(err, store.ErrNoSlot):
				noSlot.Add(1)
			default:
				other.Add(1)
				t.Logf("unexpected lease err: %v", err)
			}
		}()
	}
	wg.Wait()

	// Invariants under contention:
	//   - LeaseShared bumps refcount on an existing active_stream — no new row.
	//   - LeaseNew creates exactly one active_stream row.
	//   - So count(active_stream) == count(LeaseNew).
	//   - No credential's row count may exceed max_streams.
	//   - new + shared + no_slot + other == concurrency.
	//   - new <= total_capacity (nCreds * maxPerCred).
	t.Logf("new=%d shared=%d no_slot=%d other=%d",
		newLeases.Load(), shared.Load(), noSlot.Load(), other.Load())

	if other.Load() != 0 {
		t.Errorf("unexpected lease errors: %d", other.Load())
	}
	if total := newLeases.Load() + shared.Load() + noSlot.Load() + other.Load(); total != concurrency {
		t.Errorf("attempts unaccounted for: total=%d want=%d", total, concurrency)
	}
	if newLeases.Load() > nCreds*maxPerCred {
		t.Errorf("over-leased: new=%d > capacity=%d", newLeases.Load(), nCreds*maxPerCred)
	}

	for _, credID := range credIDs {
		var n int
		err := db.Pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM active_stream
			 WHERE credential_id = $1 AND state IN ('starting','running')`,
			credID).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n > maxPerCred {
			t.Errorf("credential %s OVER-leased: %d > %d", credID, n, maxPerCred)
		}
		t.Logf("credential %s: %d/%d slots used", credID, n, maxPerCred)
	}

	var totalActive int
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM active_stream WHERE state IN ('starting','running')`).Scan(&totalActive)
	if err != nil {
		t.Fatal(err)
	}
	if int64(totalActive) != newLeases.Load() {
		t.Errorf("active_stream count (%d) != LeaseNew count (%d)", totalActive, newLeases.Load())
	}
}

func TestIntegrationLeaseSharedAttachesNotLeases(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	provID, _ := seedProviderWithCredentials(t, db, 1, 4)
	ch, _ := db.CreateChannel(ctx, store.Channel{Number: 2, Name: "Shared", Enabled: true})
	_, _ = db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: provID,
		UpstreamURL: "http://provider.test/${USER}/${PASS}/stream.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	})

	resolver := store.PassthroughResolverForTest{}
	first, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if first.Outcome != store.LeaseNew {
		t.Fatalf("first lease should be LeaseNew, got %v", first.Outcome)
	}

	// Mark it running so the next lease finds it as a sharable upstream.
	if err := db.MarkRunning(ctx, first.ActiveStreamID); err != nil {
		t.Fatal(err)
	}

	second, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if second.Outcome != store.LeaseShared {
		t.Errorf("second lease should be LeaseShared, got %v", second.Outcome)
	}
	if second.ActiveStreamID != first.ActiveStreamID {
		t.Errorf("shared lease should re-use first stream id; got %s vs %s",
			second.ActiveStreamID, first.ActiveStreamID)
	}

	// Per-credential should still be 1 (one upstream, two clients).
	var n int
	err = db.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM active_stream WHERE state IN ('starting','running')`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 active_stream after shared attach, got %d", n)
	}

	// Refcount on the row should be 2.
	var refcount int
	err = db.Pool.QueryRow(ctx, `
		SELECT client_count FROM active_stream WHERE id = $1`, first.ActiveStreamID).Scan(&refcount)
	if err != nil {
		t.Fatal(err)
	}
	if refcount != 2 {
		t.Errorf("expected client_count=2 after shared attach, got %d", refcount)
	}
}

// FinalizeIdleStreamForDrain is the linearization point for startup abort. This exact
// interleaving models a same-channel AcquireLease landing after the aborting
// client released to zero but before cleanup tries to mark/cancel the pump.
// The attach must win and keep the shared stream alive.
func TestIntegrationFinalizeIdleStreamForDrainPreservesInterveningAttach(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	provID, _ := seedProviderWithCredentials(t, db, 1, 1)
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 2.5, Name: "Abort attach race", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: provID,
		UpstreamURL: "http://provider.test/${USER}/${PASS}/stream.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	resolver := store.PassthroughResolverForTest{}
	first, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkRunning(ctx, first.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	if count, err := db.ReleaseClient(ctx, first.ActiveStreamID); err != nil || count != 0 {
		t.Fatalf("first release count=%d err=%v, want 0/nil", count, err)
	}

	// The concurrent request commits its refcount before abort cleanup's
	// conditional state transition reaches the row.
	attached, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if attached.Outcome != store.LeaseShared || attached.ActiveStreamID != first.ActiveStreamID {
		t.Fatalf("intervening lease=%+v, want shared stream %s", attached, first.ActiveStreamID)
	}
	cancelPump, err := db.FinalizeIdleStreamForDrain(ctx, first.ActiveStreamID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelPump {
		t.Fatal("idle transition won despite intervening same-channel attach")
	}
	var state string
	var clients int
	if err := db.Pool.QueryRow(ctx,
		`SELECT state, client_count FROM active_stream WHERE id=$1`, first.ActiveStreamID).
		Scan(&state, &clients); err != nil {
		t.Fatal(err)
	}
	if state != "running" || clients != 1 {
		t.Fatalf("stream state=%s clients=%d, want running/1", state, clients)
	}

	if count, err := db.ReleaseClient(ctx, first.ActiveStreamID); err != nil || count != 0 {
		t.Fatalf("final release count=%d err=%v, want 0/nil", count, err)
	}
	cancelPump, err = db.FinalizeIdleStreamForDrain(ctx, first.ActiveStreamID)
	if err != nil || !cancelPump {
		t.Fatalf("final idle transition cancel=%v err=%v, want true/nil", cancelPump, err)
	}
}

// Pool startup cleanup is retried after timeout, so release must be exactly
// once. Replaying client A's release after client B attaches must observe B's
// count without decrementing it.
func TestIntegrationLeaseClientReleaseRetryIsIdempotent(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	provID, _ := seedProviderWithCredentials(t, db, 1, 1)
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 2.6, Name: "Release retry", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: provID,
		UpstreamURL: "http://provider.test/${USER}/${PASS}/stream.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	resolver := store.PassthroughResolverForTest{}
	clientA := uuid.New()
	first, err := db.AcquireLeaseForClient(ctx, ch.ID, resolver, clientA)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeaseClientID != clientA {
		t.Fatalf("lease client=%s, want %s", first.LeaseClientID, clientA)
	}
	if err := db.MarkRunning(ctx, first.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	if count, err := db.ReleaseLeaseClient(ctx, first.ActiveStreamID, clientA); err != nil || count != 0 {
		t.Fatalf("first exact release count=%d err=%v, want 0/nil", count, err)
	}

	clientB := uuid.New()
	attached, err := db.AcquireLeaseForClient(ctx, ch.ID, resolver, clientB)
	if err != nil {
		t.Fatal(err)
	}
	if attached.Outcome != store.LeaseShared || attached.ActiveStreamID != first.ActiveStreamID {
		t.Fatalf("intervening lease=%+v, want shared stream %s", attached, first.ActiveStreamID)
	}
	if count, err := db.ReleaseLeaseClient(ctx, first.ActiveStreamID, clientA); err != nil || count != 1 {
		t.Fatalf("retried A release count=%d err=%v, want B preserved at 1", count, err)
	}
	var clients, clientRows int
	if err := db.Pool.QueryRow(ctx, `
		SELECT a.client_count, COUNT(sc.id)::int
		  FROM active_stream a
	  LEFT JOIN stream_client sc ON sc.active_stream_id = a.id
		 WHERE a.id = $1
		 GROUP BY a.client_count`, first.ActiveStreamID).Scan(&clients, &clientRows); err != nil {
		t.Fatal(err)
	}
	if clients != 1 || clientRows != 1 {
		t.Fatalf("after retried release clients=%d rows=%d, want 1/1", clients, clientRows)
	}

	if count, err := db.ReleaseLeaseClient(ctx, first.ActiveStreamID, clientB); err != nil || count != 0 {
		t.Fatalf("final exact release count=%d err=%v, want 0/nil", count, err)
	}
	cancelPump, err := db.FinalizeIdleStreamForDrain(ctx, first.ActiveStreamID)
	if err != nil || !cancelPump {
		t.Fatalf("final idle transition cancel=%v err=%v, want true/nil", cancelPump, err)
	}
	// Model an UPDATE that committed but whose response was lost. Replaying
	// the terminal step must still say to cancel the process-local pump; false
	// would strand it behind a DB-dead/free slot.
	cancelPump, err = db.FinalizeIdleStreamForDrain(ctx, first.ActiveStreamID)
	if err != nil || !cancelPump {
		t.Fatalf("replayed idle transition cancel=%v err=%v, want true/nil", cancelPump, err)
	}
}

func TestIntegrationSweepDrainsBeforeReclaimingOrphans(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	provID, _ := seedProviderWithCredentials(t, db, 1, 4)
	ch, _ := db.CreateChannel(ctx, store.Channel{Number: 3, Name: "Sweep", Enabled: true})
	_, _ = db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: provID,
		UpstreamURL: "http://provider.test/${USER}/${PASS}/stream.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	})

	resolver := store.PassthroughResolverForTest{}
	l, err := db.AcquireLease(ctx, ch.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}

	// Release the only client and force last_heartbeat into the past.
	_, err = db.ReleaseClient(ctx, l.ActiveStreamID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Pool.Exec(ctx, `
		UPDATE active_stream SET last_heartbeat = now() - interval '1 minute'
		 WHERE id = $1`, l.ActiveStreamID)
	if err != nil {
		t.Fatal(err)
	}

	swept, err := db.SweepOrphans(ctx, 10) // 10s idle threshold
	if err != nil {
		t.Fatal(err)
	}
	if len(swept) != 1 || swept[0] != l.ActiveStreamID {
		t.Errorf("expected sweep to drain %s; got %v", l.ActiveStreamID, swept)
	}

	// Draining is non-attachable but must still occupy the provider slot until
	// the pool proves teardown completed.
	var state string
	if err := db.Pool.QueryRow(ctx,
		`SELECT state::text FROM active_stream WHERE id = $1`, l.ActiveStreamID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "draining" {
		t.Fatalf("orphan state=%q, want draining before teardown", state)
	}
	if marked, err := db.MarkDeadIfDraining(ctx, l.ActiveStreamID); err != nil || !marked {
		t.Fatalf("finish pump-less teardown: marked=%v err=%v", marked, err)
	}
	swept, err = db.SweepOrphans(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(swept) != 1 || swept[0] != l.ActiveStreamID {
		t.Fatalf("expected dead row reclaim for %s, got %v", l.ActiveStreamID, swept)
	}
	var n int
	if err := db.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM active_stream`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected 0 active_stream after completed teardown sweep, got %d", n)
	}
}

// TestIntegrationReconcileStartupFreesLeakedSlots reproduces the 2026-07-12
// incident end to end: a live active_stream row survives a restart as
// state='running'/client_count=1, which SweepOrphans can never reap, so it
// permanently consumes its credential's only slot and BLOCKS a new lease /
// failover on that credential (the actual symptom — Plex 503s, UFC PPV can't
// fail over). ReconcileStartup must mark it dead so the sweeper frees the
// slot and capacity is restored.
//
// max_streams=1 with a leak on chA's source proves the slot is genuinely
// exhausted; chB routes to a *different* source on the *same* credential so
// its lease attempt cannot share the zombie row and must find a free slot —
// exactly the failover path the leak blocked.
func TestIntegrationReconcileStartupFreesLeakedSlots(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	provID, _ := seedProviderWithCredentials(t, db, 1, 1) // one credential, one slot
	chA, _ := db.CreateChannel(ctx, store.Channel{Number: 5, Name: "LeakA", Enabled: true})
	_, _ = db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: chA.ID, ProviderID: provID,
		UpstreamURL: "http://provider.test/${USER}/${PASS}/a.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	})
	chB, _ := db.CreateChannel(ctx, store.Channel{Number: 6, Name: "LeakB", Enabled: true})
	_, _ = db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: chB.ID, ProviderID: provID,
		UpstreamURL: "http://provider.test/${USER}/${PASS}/b.ts",
		Priority:    0, HealthScore: 1.0, Enabled: true,
	})
	resolver := store.PassthroughResolverForTest{}

	l, err := db.AcquireLease(ctx, chA.ID, resolver)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the post-restart zombie: live state, one client, fresh heartbeat.
	if _, err = db.Pool.Exec(ctx, `
		UPDATE active_stream SET state='running', client_count=1, last_heartbeat=now()
		 WHERE id=$1`, l.ActiveStreamID); err != nil {
		t.Fatal(err)
	}

	// Precondition 1 — the sweeper is powerless against this row.
	if swept, err := db.SweepOrphans(ctx, 10); err != nil {
		t.Fatal(err)
	} else if len(swept) != 0 {
		t.Fatalf("expected sweep to reap nothing pre-reconcile, got %v", swept)
	}
	// Precondition 2 — the leak actually costs the slot: a lease on the other
	// channel's source (same credential) is blocked. This is the incident.
	if _, err := db.AcquireLease(ctx, chB.ID, resolver); !errors.Is(err, store.ErrNoSlot) {
		t.Fatalf("expected ErrNoSlot while zombie holds the only slot, got %v", err)
	}

	// Reconcile marks the zombie dead...
	recovered, err := db.ReconcileStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ActiveStreams != 1 || recovered.DVRRecordings != 0 {
		t.Errorf("unexpected startup recovery counts: %+v", recovered)
	}

	// ...the sweeper reclaims it...
	swept, err := db.SweepOrphans(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(swept) != 1 || swept[0] != l.ActiveStreamID {
		t.Errorf("expected sweep to reclaim %s post-reconcile, got %v", l.ActiveStreamID, swept)
	}

	// ...and capacity is genuinely restored: the previously-blocked lease now
	// succeeds with a fresh upstream (not a share of the reaped row).
	lb, err := db.AcquireLease(ctx, chB.ID, resolver)
	if err != nil {
		t.Fatalf("expected lease to succeed after reconcile freed the slot, got %v", err)
	}
	if lb.Outcome != store.LeaseNew {
		t.Errorf("expected LeaseNew after recovery, got %v", lb.Outcome)
	}
}

// A forced exit can leave both a real upstream row and dvr_recording in
// `recording`. Startup reconciliation must fail the recording explicitly while
// preserving its partial file, and the stream+recording transitions must be
// one transaction so a partial reconcile can never become allocator truth.
func TestIntegrationReconcileStartupFailsInterruptedRecordingAtomically(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	providerID, _ := seedProviderWithCredentials(t, db, 1, 1)
	channel := seedLeaseChannel(t, db, providerID, 898, 1)
	lease, err := db.AcquireLease(ctx, channel.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkRunning(ctx, lease.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	partial := t.TempDir() + "/interrupted.ts.partial"
	output := strings.TrimSuffix(partial, ".partial")
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "interrupted restart", Priority: 100,
		ScheduledStart: time.Now(), ScheduledEnd: time.Now().Add(time.Hour),
		RequestedBy: "test", OutputPath: output,
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, rec.ID, output)
	if err != nil || !claimed {
		t.Fatalf("claim interrupted recording: claimed=%v err=%v", claimed, err)
	}
	wantPartial := []byte("salvage-me")
	if err := os.WriteFile(partial, wantPartial, 0o644); err != nil {
		t.Fatal(err)
	}

	// Force the second update to fail. The first active_stream update must roll
	// back with it, proving startup never continues from half-reconciled state.
	if _, err := db.Pool.Exec(ctx, `
		CREATE FUNCTION test_fail_startup_dvr_reconcile() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'forced dvr reconcile failure';
		END
		$$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		CREATE TRIGGER test_fail_startup_dvr_reconcile
		BEFORE UPDATE ON dvr_recording
		FOR EACH ROW EXECUTE FUNCTION test_fail_startup_dvr_reconcile()`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileStartup(ctx); err == nil ||
		!strings.Contains(err.Error(), "forced dvr reconcile failure") {
		t.Fatalf("forced reconcile err=%v", err)
	}
	var streamState string
	if err := db.Pool.QueryRow(ctx,
		`SELECT state FROM active_stream WHERE id = $1`, lease.ActiveStreamID).
		Scan(&streamState); err != nil {
		t.Fatal(err)
	}
	stillRecording, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil || streamState != "running" || stillRecording.State != "recording" {
		t.Fatalf("failed reconcile partially committed: stream=%s rec=%+v err=%v",
			streamState, stillRecording, err)
	}
	if _, err := db.Pool.Exec(ctx,
		`DROP TRIGGER test_fail_startup_dvr_reconcile ON dvr_recording`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		`DROP FUNCTION test_fail_startup_dvr_reconcile()`); err != nil {
		t.Fatal(err)
	}

	recovered, err := db.ReconcileStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ActiveStreams != 1 || recovered.DVRRecordings != 1 {
		t.Fatalf("startup recovery counts=%+v, want one stream/recording", recovered)
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "failed" || got.CompletedAt == nil ||
		!strings.Contains(got.Error, "interrupted by conductor restart") ||
		got.AdmissionRetryAt != nil || got.AdmissionReason != got.Error {
		t.Fatalf("interrupted recording recovery=%+v", got)
	}
	if bytes, err := os.ReadFile(partial); err != nil || string(bytes) != string(wantPartial) {
		t.Fatalf("startup reconcile changed partial: bytes=%q err=%v", bytes, err)
	}
}

// suppress unused import in non-int-test runs
var _ = strings.HasPrefix
