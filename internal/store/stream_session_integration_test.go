// stream_session_integration_test.go — SweepOrphans must archive a reaped
// live session into stream_session (migration 0027). Same gating as the other
// store integration tests:
//
//	CONDUCTOR_INT_TEST=1 go test -race -count=1 ./internal/store -run Integration
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// A session that is reaped without being archived is exactly the hole that made
// the 2026-08-30 live-TV failure unexplainable, so assert the archive row and
// its values, not merely that the delete happened.
func TestIntegrationSweepOrphansArchivesDeadStreamToHistory(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	lease := seedHistoryLease(t, db, 921)
	const wrote = int64(4242)
	started := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	ended := started.Add(45 * time.Second)
	if _, err := db.Pool.Exec(ctx,
		`UPDATE active_stream SET bytes_out=$2, state='dead', started_at=$3, last_heartbeat=$4 WHERE id=$1`,
		lease.ActiveStreamID, wrote, started, ended); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := db.SweepOrphans(ctx, 60); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
	}

	var live int
	if err := db.Pool.QueryRow(ctx,
		`SELECT count(*) FROM active_stream WHERE id=$1`, lease.ActiveStreamID).
		Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 0 {
		t.Fatal("dead active_stream row survived the sweep")
	}

	var chID, sourceID, credentialID uuid.UUID
	var bytes int64
	var gotStart, gotEnd time.Time
	if err := db.Pool.QueryRow(ctx,
		`SELECT channel_id, channel_source_id, credential_id, bytes_out, started_at, ended_at
		   FROM stream_session WHERE id=$1`, lease.ActiveStreamID).
		Scan(&chID, &sourceID, &credentialID, &bytes, &gotStart, &gotEnd); err != nil {
		t.Fatalf("session was reaped without being archived: %v", err)
	}
	if chID != lease.ChannelID || sourceID != lease.ChannelSourceID || credentialID != lease.CredentialID {
		t.Fatalf("archived identities=%s/%s/%s want=%s/%s/%s", chID, sourceID, credentialID,
			lease.ChannelID, lease.ChannelSourceID, lease.CredentialID)
	}
	if bytes != wrote || !gotStart.Equal(started) || !gotEnd.Equal(ended) {
		t.Fatalf("archived values bytes=%d start=%s end=%s", bytes, gotStart, gotEnd)
	}
	var sensitiveColumns int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns
		WHERE table_schema='public' AND table_name='stream_session'
		AND column_name IN ('upstream_url', 'username', 'password_enc')`).Scan(&sensitiveColumns); err != nil {
		t.Fatal(err)
	}
	if sensitiveColumns != 0 {
		t.Fatal("history must not persist provider URLs or credentials")
	}
}

func TestIntegrationSweepOrphansArchiveFailureKeepsDeadRow(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	lease := seedHistoryLease(t, db, 922)
	if _, err := db.Pool.Exec(ctx, `UPDATE active_stream SET state='dead', bytes_out=4243 WHERE id=$1`, lease.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	// An archive write can still fail (e.g. storage or schema errors). The
	// same-statement contract must retain the source row for a later retry.
	if _, err := db.Pool.Exec(ctx, `ALTER TABLE stream_session ADD CONSTRAINT test_reject_archive CHECK (bytes_out <> 4243)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Pool.Exec(ctx, `ALTER TABLE stream_session DROP CONSTRAINT IF EXISTS test_reject_archive`)
	})
	if _, err := db.SweepOrphans(ctx, 60); err == nil {
		t.Fatal("archive refusal was ignored")
	}
	var live, archived int
	if err := db.Pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM active_stream WHERE id=$1),
		(SELECT count(*) FROM stream_session WHERE id=$1)`, lease.ActiveStreamID).Scan(&live, &archived); err != nil {
		t.Fatal(err)
	}
	if live != 1 || archived != 0 {
		t.Fatalf("archive failure lost source row: live=%d archived=%d", live, archived)
	}
	if _, err := db.Pool.Exec(ctx, `ALTER TABLE stream_session DROP CONSTRAINT test_reject_archive`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SweepOrphans(ctx, 60); err != nil {
		t.Fatalf("retry after archive recovery: %v", err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM stream_session WHERE id=$1`, lease.ActiveStreamID).Scan(&archived); err != nil || archived != 1 {
		t.Fatalf("retry archive count=%d err=%v", archived, err)
	}
}

func seedHistoryLease(t *testing.T, db *store.DB, number float64) store.Lease {
	t.Helper()
	ctx := context.Background()

	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "archive-provider-" + uuid.NewString(), Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "archive", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 0, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	channel := seedLeaseChannel(t, db, provider.ID, number, 1)
	lease, err := db.AcquireLease(ctx, channel.ID, store.PassthroughResolverForTest{})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	return lease
}
