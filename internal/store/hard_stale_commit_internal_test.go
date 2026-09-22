package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func seedCommitAmbiguityHardStaleCandidate(
	t *testing.T,
	db *DB,
) HardStaleStreamCandidate {
	t.Helper()
	ctx := context.Background()
	channel, _ := seedCommitAmbiguityChannel(t, db, "http://source-a.test/stream")
	clientID := uuid.New()
	lease, err := db.AcquireLeaseForClient(
		ctx, channel.ID, PassthroughResolverForTest{}, clientID)
	if err != nil {
		t.Fatal(err)
	}
	lease, err = db.ClaimPumpLeaseForClient(ctx, lease.ActiveStreamID, clientID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkRunning(ctx, lease.ActiveStreamID); err != nil {
		t.Fatal(err)
	}
	lastHeartbeat := time.Now().Add(-10 * time.Minute)
	if _, err := db.Pool.Exec(ctx, `
		UPDATE active_stream SET last_heartbeat = $2 WHERE id = $1`,
		lease.ActiveStreamID, lastHeartbeat); err != nil {
		t.Fatal(err)
	}
	return HardStaleStreamCandidate{
		ID: lease.ActiveStreamID, ChannelID: channel.ID,
		PumpGeneration: lease.PumpGeneration, LastHeartbeat: lastHeartbeat,
	}
}

func TestIntegrationHardStaleCommitAmbiguityReplaysExactGeneration(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	candidate := seedCommitAmbiguityHardStaleCandidate(t, db)
	simulated := errors.New("simulated lost hard-stale COMMIT response")
	var commitResponses atomic.Int32
	db.hardStaleCommitResultHook = func() error {
		if commitResponses.Add(1) == 1 {
			return simulated
		}
		return nil
	}

	finalized, err := db.FinalizeHardStaleStream(
		context.Background(), candidate, time.Now().Add(-3*time.Minute), true)
	if err != nil || !finalized {
		t.Fatalf("ambiguous hard-stale replay finalized=%v err=%v, want true/nil",
			finalized, err)
	}
	if got := commitResponses.Load(); got != 2 {
		t.Fatalf("commit-result observations=%d, want initial + exact replay", got)
	}

	var state string
	var clients, tokens int
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT a.state::text, a.client_count, COUNT(sc.id)::int
		  FROM active_stream a
	 LEFT JOIN stream_client sc ON sc.active_stream_id = a.id
		 WHERE a.id = $1
		 GROUP BY a.state, a.client_count`, candidate.ID).Scan(
		&state, &clients, &tokens); err != nil {
		t.Fatal(err)
	}
	if state != "draining" || clients != 0 || tokens != 0 {
		t.Fatalf("replayed hard-stale state=%s clients=%d tokens=%d, want draining/0/0",
			state, clients, tokens)
	}
}

func TestIntegrationHardStaleRepeatedCommitAmbiguityIsExplicit(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	candidate := seedCommitAmbiguityHardStaleCandidate(t, db)
	simulated := errors.New("simulated repeated lost hard-stale COMMIT response")
	db.hardStaleCommitResultHook = func() error { return simulated }

	finalized, err := db.FinalizeHardStaleStream(
		context.Background(), candidate, time.Now().Add(-3*time.Minute), true)
	if finalized || !errors.Is(err, ErrHardStaleFinalizeStateUnknown) ||
		!errors.Is(err, simulated) {
		t.Fatalf("repeated ambiguity finalized=%v err=%v, want false + exact unknown marker",
			finalized, err)
	}
}

func TestIntegrationHardStaleAmbiguitySurvivesPreCommitReplayFailure(t *testing.T) {
	db := openCommitAmbiguityDB(t)
	candidate := seedCommitAmbiguityHardStaleCandidate(t, db)
	simulated := errors.New("simulated first lost hard-stale COMMIT response")
	db.hardStaleCommitResultHook = func() error {
		// The first transaction has already committed when this seam runs. Make
		// replay fail in BeginTx so the original ambiguity must remain typed.
		db.Pool.Close()
		return simulated
	}

	finalized, err := db.FinalizeHardStaleStream(
		context.Background(), candidate, time.Now().Add(-3*time.Minute), true)
	if finalized || !errors.Is(err, ErrHardStaleFinalizeStateUnknown) ||
		!errors.Is(err, simulated) {
		t.Fatalf("pre-COMMIT replay failure finalized=%v err=%v, want false + preserved ambiguity",
			finalized, err)
	}
}
