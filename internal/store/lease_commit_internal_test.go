package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var commitAmbiguityChannelNumber atomic.Int64

func openCommitAmbiguityDB(t *testing.T) *DB {
	t.Helper()
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	dsn := os.Getenv("CONDUCTOR_TEST_DSN")
	if dsn == "" {
		t.Skip("CONDUCTOR_TEST_DSN is required for commit-ambiguity integration tests")
	}
	db, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedCommitAmbiguityChannel(t *testing.T, db *DB, urls ...string) (Channel, []ChannelSource) {
	t.Helper()
	ctx := context.Background()
	provider, err := db.CreateProvider(ctx, Provider{
		Name: "commit-ambiguity-" + uuid.NewString(), Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, ProviderCredential{
		ProviderID: provider.ID, Username: "commit-ambiguity",
		PasswordEnc: []byte{1}, MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, Channel{
		Number: 8700 + float64(commitAmbiguityChannelNumber.Add(1)),
		Name:   "commit-ambiguity-" + uuid.NewString(), Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]ChannelSource, 0, len(urls))
	for priority, url := range urls {
		source, err := db.CreateChannelSource(ctx, ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: url,
			Priority: priority, HealthScore: 1, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, source)
	}
	return channel, sources
}

func waitForCommitAmbiguityLock(t *testing.T, db *DB, applicationName string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var blocked bool
		if err := db.Pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE application_name = $1
				   AND state = 'active'
				   AND wait_event_type = 'Lock'
			)`, applicationName).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not reach its database lock", applicationName)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A live request may consume the configured reserve, but it must serialize
// with a DVR admission decision across the provider's complete credential
// pool. Without the provider boundary, these transactions can lock different
// one-slot credentials while their inserts are mutually invisible and admit a
// DVR that no longer leaves the promised live slot.
func TestIntegrationLiveAcquisitionSerializesProviderDVRReserve(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	ctx := context.Background()
	provider, err := db.CreateProvider(ctx, Provider{
		Name: "live-dvr-provider-boundary-" + uuid.NewString(),
		Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := db.CreateCredential(ctx, ProviderCredential{
			ProviderID: provider.ID, Username: uuid.NewString(), PasswordEnc: []byte{1},
			MaxStreams: 1, Priority: i, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	makeChannel := func(name string) Channel {
		t.Helper()
		channel, err := db.CreateChannel(ctx, Channel{
			Number: 8800 + float64(commitAmbiguityChannelNumber.Add(1)),
			Name:   name + "-" + uuid.NewString(), Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID,
			UpstreamURL: "http://provider-boundary.test/" + name,
			Priority:    0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return channel
	}
	occupiedChannel := makeChannel("occupied")
	liveChannel := makeChannel("live")
	dvrChannel := makeChannel("dvr")
	if _, err := db.AcquireLease(ctx, occupiedChannel.ID, PassthroughResolverForTest{}); err != nil {
		t.Fatalf("seed occupied slot: %v", err)
	}

	liveAtCommit := make(chan Lease, 1)
	allowLiveCommit := make(chan struct{})
	var releaseOnce sync.Once
	releaseLive := func() { releaseOnce.Do(func() { close(allowLiveCommit) }) }
	t.Cleanup(releaseLive)
	db.leaseBeforeCommitHook = func(lease Lease) error {
		if lease.ChannelID == liveChannel.ID {
			liveAtCommit <- lease
			<-allowLiveCommit
		}
		return nil
	}
	t.Cleanup(func() { db.leaseBeforeCommitHook = nil })

	type leaseResult struct {
		lease Lease
		err   error
	}
	liveDone := make(chan leaseResult, 1)
	go func() {
		lease, err := db.AcquireLease(ctx, liveChannel.ID, PassthroughResolverForTest{})
		liveDone <- leaseResult{lease: lease, err: err}
	}()
	select {
	case <-liveAtCommit:
	case <-time.After(5 * time.Second):
		t.Fatal("live acquisition did not reach its pre-COMMIT barrier")
	}

	dvrDone := make(chan leaseResult, 1)
	go func() {
		lease, err := db.AcquireDVRLease(ctx, dvrChannel.ID,
			PassthroughResolverForTest{}, DVRLeasePolicy{LiveReserve: 1})
		dvrDone <- leaseResult{lease: lease, err: err}
	}()
	waitDeadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case result := <-dvrDone:
			t.Fatalf("DVR admission escaped the uncommitted live provider boundary: result=%+v err=%v",
				result.lease, result.err)
		default:
		}
		var waiting bool
		if err := db.Pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE datname = current_database()
				   AND state = 'active'
				   AND wait_event_type = 'Lock'
				   AND query LIKE '%SELECT capacity_domain%FROM provider%FOR UPDATE%'
			)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("DVR admission did not reach the provider/domain lock")
		}
		time.Sleep(10 * time.Millisecond)
	}

	releaseLive()
	select {
	case result := <-liveDone:
		if result.err != nil || result.lease.Outcome != LeaseNew {
			t.Fatalf("live acquisition result=%+v err=%v", result.lease, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live acquisition did not commit")
	}
	select {
	case result := <-dvrDone:
		if !errors.Is(result.err, ErrNoSlot) {
			t.Fatalf("DVR admission result=%+v err=%v, want ErrNoSlot after live commit",
				result.lease, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DVR admission did not finish after live commit")
	}

	var active int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int
		  FROM active_stream a
		  JOIN provider_credential pc ON pc.id = a.credential_id
		 WHERE pc.provider_id = $1
		   AND a.state IN ('starting','running','draining')`, provider.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 2 {
		t.Fatalf("provider active streams=%d, want occupied + live only", active)
	}
}

// A lost COMMIT response must return the provisional durable identity instead
// of trying another source. Once the request deadline has gone away, cleanup
// can wait on the channel lock and release exactly the transaction that landed.
func TestIntegrationAcquireCommitAmbiguityCanBeReconciledAndReleased(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, _ := seedCommitAmbiguityChannel(t, db, "http://source-a.test/stream")
	clientID := uuid.New()
	simulated := errors.New("simulated lost COMMIT response")
	ctx, cancel := context.WithCancel(context.Background())
	db.leaseCommitResultHook = func() error {
		cancel()
		return simulated
	}

	lease, err := db.AcquireLeaseForClient(ctx, channel.ID, PassthroughResolverForTest{}, clientID)
	if !errors.Is(err, ErrLeaseCommitAmbiguous) || !errors.Is(err, ErrLeaseOperational) {
		t.Fatalf("acquire err=%v, want operational commit ambiguity", err)
	}
	if lease.ActiveStreamID == uuid.Nil || lease.LeaseClientID != clientID || lease.ChannelID != channel.ID {
		t.Fatalf("provisional lease=%+v, want exact stream/client/channel identity", lease)
	}

	current, found, err := db.ResolveAmbiguousLeaseClient(context.Background(), lease)
	if err != nil {
		t.Fatalf("resolve committed lease: %v", err)
	}
	if !found || current.ActiveStreamID != lease.ActiveStreamID || current.LeaseClientID != clientID {
		t.Fatalf("resolved found=%v lease=%+v, want exact committed token", found, current)
	}

	db.leaseCommitResultHook = nil
	remaining, err := db.ReleaseLeaseClient(context.Background(), current.ActiveStreamID, current.LeaseClientID)
	if err != nil || remaining != 0 {
		t.Fatalf("exact release remaining=%d err=%v, want 0/nil", remaining, err)
	}
	freed, err := db.FinalizeIdleStreamWithoutPump(context.Background(), current.ActiveStreamID)
	if err != nil || !freed {
		t.Fatalf("finalize committed lease freed=%v err=%v", freed, err)
	}
	var state string
	var clients, tokens int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT a.state::text, a.client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id = a.id)
		  FROM active_stream a WHERE a.id = $1`, current.ActiveStreamID).
		Scan(&state, &clients, &tokens); err != nil {
		t.Fatal(err)
	}
	if state != "dead" || clients != 0 || tokens != 0 {
		t.Fatalf("cleaned row=%s clients=%d tokens=%d, want dead/0/0", state, clients, tokens)
	}
}

// A later relocation may win the advisory lock before ambiguity reconciliation.
// Tuple C proves neither whether the original A→B transaction committed nor
// whether it rolled back, so the caller must stop rather than retry source A.
func TestIntegrationRelocationAmbiguityAdvancedStateStopsOldPump(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, sources := seedCommitAmbiguityChannel(t, db,
		"http://source-a.test/stream", "http://source-b.test/stream",
		"http://source-c.test/stream")
	lease, err := db.AcquireLeaseForClient(
		context.Background(), channel.ID, PassthroughResolverForTest{}, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	simulated := errors.New("simulated lost source-B COMMIT response")
	var advancedErr error
	db.leaseCommitResultHook = func() error {
		db.leaseCommitResultHook = nil
		_, advancedErr = db.RelocateStream(context.Background(), lease.ActiveStreamID,
			channel.ID, []uuid.UUID{sources[0].ID, sources[1].ID},
			PassthroughResolverForTest{})
		return simulated
	}

	current, err := db.RelocateStream(context.Background(), lease.ActiveStreamID,
		channel.ID, []uuid.UUID{sources[0].ID}, PassthroughResolverForTest{})
	if advancedErr != nil {
		t.Fatalf("advance ambiguous row to source C: %v", advancedErr)
	}
	if !errors.Is(err, ErrRelocationStateUnknown) {
		t.Fatalf("outer relocation err=%v, want unknown-state marker", err)
	}
	if current.ChannelSourceID != sources[2].ID || current.UpstreamURL != "http://source-c.test/stream" {
		t.Fatalf("authoritative post-race lease=%+v, want source C", current)
	}
}

// Persisted pump generation makes delayed terminalization generation-scoped.
// An old pump cannot free the row after a replacement has claimed it, even if
// the replacement's final client has already moved the row to draining/zero.
func TestIntegrationPumpGenerationPreventsOldExitFromKillingReplacement(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, _ := seedCommitAmbiguityChannel(t, db, "http://source-a.test/stream")
	clientID := uuid.New()
	lease, err := db.AcquireLeaseForClient(
		context.Background(), channel.ID, PassthroughResolverForTest{}, clientID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := db.ClaimPumpLeaseForClient(
		context.Background(), lease.ActiveStreamID, clientID)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := db.ClaimPumpLeaseForClient(
		context.Background(), lease.ActiveStreamID, clientID)
	if err != nil {
		t.Fatal(err)
	}
	if first.PumpGeneration == uuid.Nil || replacement.PumpGeneration == uuid.Nil ||
		first.PumpGeneration == replacement.PumpGeneration {
		t.Fatalf("pump generations first=%s replacement=%s, want distinct non-zero values",
			first.PumpGeneration, replacement.PumpGeneration)
	}
	// Persistence queued by the detached generation may execute after the
	// replacement claim. Both state and byte updates must be fenced out.
	if err := db.MarkRunningForPump(
		context.Background(), lease.ActiveStreamID, first.PumpGeneration); err != nil {
		t.Fatal(err)
	}
	if err := db.HeartbeatForPump(
		context.Background(), lease.ActiveStreamID, first.PumpGeneration, 999); err != nil {
		t.Fatal(err)
	}
	var state string
	var generation uuid.UUID
	var bytesOut int64
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT state::text, pump_generation, bytes_out
		  FROM active_stream WHERE id=$1`, lease.ActiveStreamID).Scan(
		&state, &generation, &bytesOut); err != nil {
		t.Fatal(err)
	}
	if state != "starting" || generation != replacement.PumpGeneration || bytesOut != 0 {
		t.Fatalf("stale persistence state=%s generation=%s bytes=%d, want starting/%s/0",
			state, generation, bytesOut, replacement.PumpGeneration)
	}
	// The bounded synchronous MarkRunning callback may time out under ordinary
	// database pressure. A successful generation-fenced heartbeat must both
	// account bytes and repair that starting -> running transition.
	if err := db.HeartbeatForPump(
		context.Background(), lease.ActiveStreamID, replacement.PumpGeneration, 17); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT state::text, pump_generation, bytes_out
		  FROM active_stream WHERE id=$1`, lease.ActiveStreamID).Scan(
		&state, &generation, &bytesOut); err != nil {
		t.Fatal(err)
	}
	if state != "running" || generation != replacement.PumpGeneration || bytesOut != 17 {
		t.Fatalf("replacement persistence state=%s generation=%s bytes=%d, want running/%s/17",
			state, generation, bytesOut, replacement.PumpGeneration)
	}
	remaining, err := db.ReleaseLeaseClient(
		context.Background(), lease.ActiveStreamID, clientID)
	if err != nil || remaining != 0 {
		t.Fatalf("release replacement token remaining=%d err=%v", remaining, err)
	}
	draining, err := db.MarkDrainingIfIdle(context.Background(), lease.ActiveStreamID)
	if err != nil || !draining {
		t.Fatalf("mark replacement draining=%v err=%v", draining, err)
	}
	freed, err := db.MarkDeadAfterPumpExit(
		context.Background(), lease.ActiveStreamID, first.PumpGeneration)
	if err != nil || freed {
		t.Fatalf("old generation freed replacement=%v err=%v", freed, err)
	}
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT state::text, pump_generation FROM active_stream WHERE id=$1`,
		lease.ActiveStreamID).Scan(&state, &generation); err != nil {
		t.Fatal(err)
	}
	if state != "draining" || generation != replacement.PumpGeneration {
		t.Fatalf("after old exit state=%s generation=%s, want draining/%s",
			state, generation, replacement.PumpGeneration)
	}
	freed, err = db.MarkDeadAfterPumpExit(
		context.Background(), lease.ActiveStreamID, replacement.PumpGeneration)
	if err != nil || !freed {
		t.Fatalf("replacement generation terminalization freed=%v err=%v", freed, err)
	}
}

// A retry that starts while an uncertain release transaction is uncommitted
// must wait before taking its READ COMMITTED data snapshot. It then observes
// the exact token gone and the post-commit count, never stale count=1.
func TestIntegrationReleaseRetryWaitsForExactClientCommitBoundary(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, _ := seedCommitAmbiguityChannel(t, db, "http://source-a.test/stream")
	clientID := uuid.New()
	lease, err := db.AcquireLeaseForClient(
		context.Background(), channel.ID, PassthroughResolverForTest{}, clientID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback(context.Background()) }()
	if _, err := first.Exec(context.Background(), `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:lease-client:' || $1::text, 0)
		)`, clientID); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Exec(context.Background(), `
		WITH removed AS (
			DELETE FROM stream_client
			 WHERE id=$2 AND active_stream_id=$1
			 RETURNING 1
		)
		UPDATE active_stream
		   SET client_count=GREATEST(0, client_count-1)
		 WHERE id=$1 AND EXISTS (SELECT 1 FROM removed)`,
		lease.ActiveStreamID, clientID); err != nil {
		t.Fatal(err)
	}
	type releaseResult struct {
		count int
		err   error
	}
	result := make(chan releaseResult, 1)
	go func() {
		count, err := db.ReleaseLeaseClient(
			context.Background(), lease.ActiveStreamID, clientID)
		result <- releaseResult{count: count, err: err}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var blocked bool
		err := db.Pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname=current_database()
				   AND wait_event_type='Lock'
				   AND query LIKE '%conductor:lease-client:%'
			)`).Scan(&blocked)
		if err == nil && blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("release retry did not wait on exact-client commit boundary")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := first.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || got.count != 0 {
			t.Fatalf("release retry count=%d err=%v, want post-commit 0/nil", got.count, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("release retry did not finish after commit")
	}
}

// When the caller is still alive, an apparently failed relocation can be
// reconciled at the same channel lock boundary and safely return source B.
func TestIntegrationRelocationCommitAmbiguityReconcilesCommittedSource(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, sources := seedCommitAmbiguityChannel(t, db,
		"http://source-a.test/stream", "http://source-b.test/stream")
	lease, err := db.AcquireLeaseForClient(
		context.Background(), channel.ID, PassthroughResolverForTest{}, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	simulated := errors.New("simulated lost relocation COMMIT response")
	db.leaseCommitResultHook = func() error { return simulated }

	moved, err := db.RelocateStream(context.Background(), lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sources[0].ID}, PassthroughResolverForTest{})
	if err != nil {
		t.Fatalf("reconciled relocation: %v", err)
	}
	if moved.ChannelSourceID != sources[1].ID || moved.UpstreamURL != "http://source-b.test/stream" {
		t.Fatalf("reconciled lease=%+v, want source B", moved)
	}
	current, err := db.CurrentLeaseForClient(
		context.Background(), lease.ActiveStreamID, lease.LeaseClientID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ChannelSourceID != sources[1].ID || current.UpstreamURL != moved.UpstreamURL {
		t.Fatalf("DB truth=%+v, want reconciled source B", current)
	}
}

// If the request deadline disappears after PostgreSQL commits source B, the
// pump must receive an explicit unknown-state marker and stop. Retrying its old
// source-A URL would make provider accounting disagree with actual media.
func TestIntegrationRelocationCommitAmbiguityNeverRetriesOldSource(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, sources := seedCommitAmbiguityChannel(t, db,
		"http://source-a.test/stream", "http://source-b.test/stream")
	lease, err := db.AcquireLeaseForClient(
		context.Background(), channel.ID, PassthroughResolverForTest{}, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	simulated := errors.New("simulated canceled relocation COMMIT response")
	ctx, cancel := context.WithCancel(context.Background())
	db.leaseCommitResultHook = func() error {
		cancel()
		return simulated
	}

	provisional, err := db.RelocateStream(ctx, lease.ActiveStreamID, channel.ID,
		[]uuid.UUID{sources[0].ID}, PassthroughResolverForTest{})
	if !errors.Is(err, ErrRelocationStateUnknown) {
		t.Fatalf("relocate err=%v, want unknown-state marker", err)
	}
	if provisional.ChannelSourceID != sources[1].ID || provisional.UpstreamURL != "http://source-b.test/stream" {
		t.Fatalf("provisional lease=%+v, want intended source B identity", provisional)
	}
	current, err := db.CurrentLeaseForClient(
		context.Background(), lease.ActiveStreamID, lease.LeaseClientID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ChannelSourceID != sources[1].ID || current.UpstreamURL != provisional.UpstreamURL {
		t.Fatalf("committed DB truth=%+v, want source B", current)
	}
}

// A retry can begin while PostgreSQL is still resolving an earlier release's
// COMMIT. The retry must linearize after that transaction instead of returning
// client_count from the statement snapshot it took before the COMMIT became
// visible. A stale positive result makes Pool believe another viewer owns the
// pump and strands an upstream whose durable client count is already zero.
func TestIntegrationReleaseLeaseClientRetryWaitsForAmbiguousCommit(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, _ := seedCommitAmbiguityChannel(t, db, "http://source-a.test/stream")
	clientID := uuid.New()
	lease, err := db.AcquireLeaseForClient(
		context.Background(), channel.ID, PassthroughResolverForTest{}, clientID)
	if err != nil {
		t.Fatal(err)
	}

	// Execute the first exact-token release but deliberately hold its
	// transaction before COMMIT. This is the server-side state when a client
	// loses the COMMIT response and starts bounded cleanup retry.
	firstTx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstTx.Rollback(context.Background()) })
	var firstRemaining int
	if err := firstTx.QueryRow(context.Background(), `
		WITH removed AS (
			DELETE FROM stream_client
			 WHERE id = $2 AND active_stream_id = $1
			 RETURNING 1
		), updated AS (
			UPDATE active_stream
			   SET client_count = GREATEST(0, client_count - 1),
			       last_heartbeat = now()
			 WHERE id = $1 AND EXISTS (SELECT 1 FROM removed)
			 RETURNING client_count
		)
		SELECT client_count FROM updated
		UNION ALL
		SELECT client_count
		  FROM active_stream
		 WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM removed)
		LIMIT 1`, lease.ActiveStreamID, clientID).Scan(&firstRemaining); err != nil {
		t.Fatal(err)
	}
	if firstRemaining != 0 {
		t.Fatalf("first release remaining=%d, want 0", firstRemaining)
	}

	// Give the retry its own identifiable pool so pg_stat_activity proves it
	// reached the conflicting database lock before the first COMMIT. This is a
	// synchronization barrier, not a timing assumption.
	applicationName := "conductor-release-retry-" + uuid.NewString()
	retryConfig := db.Pool.Config()
	retryConfig.ConnConfig.RuntimeParams["application_name"] = applicationName
	retryPool, err := pgxpool.NewWithConfig(context.Background(), retryConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(retryPool.Close)
	retryDB := &DB{Pool: retryPool}
	type releaseResult struct {
		remaining int
		err       error
	}
	retried := make(chan releaseResult, 1)
	go func() {
		remaining, releaseErr := retryDB.ReleaseLeaseClient(
			context.Background(), lease.ActiveStreamID, clientID)
		retried <- releaseResult{remaining: remaining, err: releaseErr}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var blocked bool
		if err := db.Pool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE application_name = $1
				   AND state = 'active'
				   AND wait_event_type = 'Lock'
			)`, applicationName).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retry did not overlap the first release at its database lock")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Treat this successful server COMMIT as response-ambiguous to the first
	// caller. The overlapping retry must now observe the committed count.
	if err := firstTx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-retried:
		if result.err != nil || result.remaining != 0 {
			t.Fatalf("overlapping retry remaining=%d err=%v, want 0/nil",
				result.remaining, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("overlapping retry did not finish after first release committed")
	}

	var durableClients, durableTokens int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT a.client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id = a.id)
		  FROM active_stream a
		 WHERE a.id = $1`, lease.ActiveStreamID).
		Scan(&durableClients, &durableTokens); err != nil {
		t.Fatal(err)
	}
	if durableClients != 0 || durableTokens != 0 {
		t.Fatalf("durable clients=%d tokens=%d, want 0/0",
			durableClients, durableTokens)
	}
	if capacityFree, err := db.FinalizeIdleStreamWithoutPump(
		context.Background(), lease.ActiveStreamID); err != nil || !capacityFree {
		t.Fatalf("finalize after retry capacityFree=%v err=%v, want true/nil",
			capacityFree, err)
	}
}

// An acquisition's INSERT is invisible to a row lock whose statement starts
// before COMMIT. The exact client advisory fence must make cleanup wait for the
// transaction outcome, then release the committed token from a fresh snapshot.
func TestIntegrationReleaseWaitsForUncommittedAcquisition(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, _ := seedCommitAmbiguityChannel(t, db, "http://source-a.test/stream")
	clientID := uuid.New()
	provisional := make(chan Lease, 1)
	allowCommit := make(chan struct{})
	db.leaseBeforeCommitHook = func(lease Lease) error {
		provisional <- lease
		<-allowCommit
		return nil
	}
	simulated := errors.New("simulated lost in-flight COMMIT response")
	acquireCtx, cancelAcquire := context.WithCancel(context.Background())
	db.leaseCommitResultHook = func() error {
		// Model Plex abandoning the request only after PostgreSQL has committed.
		// This keeps synchronous ambiguity reconciliation off the response path so
		// the already-waiting cleanup is the operation that must cross the fence.
		cancelAcquire()
		return simulated
	}
	t.Cleanup(func() {
		cancelAcquire()
		db.leaseBeforeCommitHook = nil
		db.leaseCommitResultHook = nil
	})

	type acquireResult struct {
		lease Lease
		err   error
	}
	acquired := make(chan acquireResult, 1)
	go func() {
		lease, err := db.AcquireLeaseForClient(
			acquireCtx, channel.ID, PassthroughResolverForTest{}, clientID)
		acquired <- acquireResult{lease: lease, err: err}
	}()
	var lease Lease
	select {
	case lease = <-provisional:
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition did not reach the pre-COMMIT barrier")
	}
	if lease.ActiveStreamID == uuid.Nil || lease.LeaseClientID != clientID {
		t.Fatalf("provisional lease=%+v, want exact stream/client identity", lease)
	}

	applicationName := "conductor-acquire-cleanup-" + uuid.NewString()
	retryConfig := db.Pool.Config()
	retryConfig.ConnConfig.RuntimeParams["application_name"] = applicationName
	retryPool, err := pgxpool.NewWithConfig(context.Background(), retryConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(retryPool.Close)
	retryDB := &DB{Pool: retryPool}
	type releaseResult struct {
		remaining int
		err       error
	}
	released := make(chan releaseResult, 1)
	go func() {
		remaining, err := retryDB.ReleaseLeaseClient(
			context.Background(), lease.ActiveStreamID, clientID)
		released <- releaseResult{remaining: remaining, err: err}
	}()
	waitForCommitAmbiguityLock(t, db, applicationName)

	close(allowCommit)
	select {
	case result := <-acquired:
		if !errors.Is(result.err, ErrLeaseCommitAmbiguous) ||
			!errors.Is(result.err, simulated) || result.lease.ActiveStreamID != lease.ActiveStreamID {
			t.Fatalf("acquire result=%+v err=%v, want exact ambiguous lease", result.lease, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition did not return after COMMIT was released")
	}
	select {
	case result := <-released:
		if result.err != nil || result.remaining != 0 {
			t.Fatalf("fenced cleanup remaining=%d err=%v, want 0/nil",
				result.remaining, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish after acquisition COMMIT")
	}

	var durableClients, durableTokens int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT a.client_count,
		       (SELECT COUNT(*)::int FROM stream_client WHERE active_stream_id = a.id)
		  FROM active_stream a WHERE a.id = $1`, lease.ActiveStreamID).
		Scan(&durableClients, &durableTokens); err != nil {
		t.Fatal(err)
	}
	if durableClients != 0 || durableTokens != 0 {
		t.Fatalf("durable clients=%d tokens=%d, want 0/0",
			durableClients, durableTokens)
	}
	if capacityFree, err := db.FinalizeIdleStreamWithoutPump(
		context.Background(), lease.ActiveStreamID); err != nil || !capacityFree {
		t.Fatalf("finalize committed acquisition capacityFree=%v err=%v, want true/nil",
			capacityFree, err)
	}
}

// A finalization replay can begin while an earlier state transition is still
// committing. It must wait and recognize the durable dead/zero result as an
// instruction that the no-pump reservation's capacity is already free.
func TestIntegrationFinalizeIdleStreamRetryWaitsForAmbiguousCommit(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	channel, _ := seedCommitAmbiguityChannel(t, db, "http://source-a.test/stream")
	clientID := uuid.New()
	lease, err := db.AcquireLeaseForClient(
		context.Background(), channel.ID, PassthroughResolverForTest{}, clientID)
	if err != nil {
		t.Fatal(err)
	}
	if remaining, err := db.ReleaseLeaseClient(
		context.Background(), lease.ActiveStreamID, clientID); err != nil || remaining != 0 {
		t.Fatalf("release remaining=%d err=%v, want 0/nil", remaining, err)
	}

	firstTx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstTx.Rollback(context.Background()) })
	if _, err := firstTx.Exec(context.Background(), `
		UPDATE active_stream SET state = 'dead', last_heartbeat = now()
		 WHERE id = $1 AND client_count = 0`, lease.ActiveStreamID); err != nil {
		t.Fatal(err)
	}

	applicationName := "conductor-finalize-retry-" + uuid.NewString()
	retryConfig := db.Pool.Config()
	retryConfig.ConnConfig.RuntimeParams["application_name"] = applicationName
	retryPool, err := pgxpool.NewWithConfig(context.Background(), retryConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(retryPool.Close)
	retryDB := &DB{Pool: retryPool}
	type finalizeResult struct {
		capacityFree bool
		err          error
	}
	finalized := make(chan finalizeResult, 1)
	go func() {
		capacityFree, err := retryDB.FinalizeIdleStreamWithoutPump(
			context.Background(), lease.ActiveStreamID)
		finalized <- finalizeResult{capacityFree: capacityFree, err: err}
	}()
	waitForCommitAmbiguityLock(t, db, applicationName)

	if err := firstTx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-finalized:
		if result.err != nil || !result.capacityFree {
			t.Fatalf("overlapping finalize capacityFree=%v err=%v, want true/nil",
				result.capacityFree, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finalization retry did not finish after first COMMIT")
	}
}
