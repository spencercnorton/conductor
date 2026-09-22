// lease.go implements the FOR UPDATE SKIP LOCKED credential leasing per
// spec §4.1. This is the structural fix that makes Conductor's reason to
// exist work: credentials are leased atomically with the active_stream
// insert, so two concurrent channel changes cannot double-book a slot.
//
// Algorithm (per spec §4.1 step 2):
//
//	For each candidate channel_source in priority+health order:
//	  1. Open tx.
//	  2. SELECT an existing active_stream for (channel_source_id) FOR UPDATE.
//	     If found and refcount-bump succeeds: return ShareExisting result.
//	  3. SELECT a candidate provider_credential row, ORDER BY load+priority,
//	     FOR UPDATE OF pc SKIP LOCKED, LIMIT 1.
//	  4. Holding the row lock, COUNT active_stream rows on that credential
//	     (this re-evaluates the count under the lock, since the FOR UPDATE
//	     in the SELECT only locks; the count subquery runs at SELECT time
//	     with READ COMMITTED snapshot).
//	  5. If count < max_streams: INSERT active_stream (state=starting),
//	     commit, return NewLease.
//	     Else: rollback, try next credential (or next source).
//
// Releasing a lease = transition active_stream.state to 'dead'. The
// orphan-sweeper goroutine (internal/stream/watchdog.go) does the
// physical DELETE after a grace period.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// LeaseOutcome describes how a lease attempt resolved.
type LeaseOutcome int

const (
	LeaseShared  LeaseOutcome = iota // attached to existing upstream
	LeaseNew                         // new upstream slot leased
	LeaseUnavail                     // no slot available right now
)

// Lease is the result of a successful AcquireLease.
type Lease struct {
	Outcome         LeaseOutcome
	ActiveStreamID  uuid.UUID
	LeaseClientID   uuid.UUID // non-zero for idempotent Pool release/retry
	ChannelID       uuid.UUID
	ChannelSourceID uuid.UUID
	CredentialID    uuid.UUID
	UpstreamURL     string    // resolved (creds substituted)
	ClientCount     int       // post-bump (>=1)
	PumpGeneration  uuid.UUID // non-zero after a Pool pump claims this row
}

// HardStaleStreamCandidate is a generation-bound snapshot of an attachable
// active_stream whose durable heartbeat exceeded the watchdog's hard ceiling.
// PumpGeneration is uuid.Nil when no process-local pump ever claimed the row.
// The watchdog must still cross-check this snapshot against Pool activity
// before asking FinalizeHardStaleStream to mutate it.
type HardStaleStreamCandidate struct {
	ID             uuid.UUID
	ChannelID      uuid.UUID
	PumpGeneration uuid.UUID
	LastHeartbeat  time.Time
}

// DVRLeasePolicy applies only when a DVR request would create a new upstream.
// A same-channel attach is always allowed because it consumes no upstream
// slot. LiveReserve is capacity-domain-scoped and is clamped so an isolated
// one-slot provider can still admit one DVR recording.
type DVRLeasePolicy struct {
	LiveReserve int
}

// ErrNoSlot signals no enabled credential under any candidate source has
// a free slot. Caller should 503 the client and emit an alert.
var ErrNoSlot = errors.New("all sources exhausted")

// ErrLeaseOperational marks a non-capacity source failure retained across
// fallback. The joined cause remains inspectable, but schedulers must never
// turn a mixed operational+attempt-timeout result into a capacity retry.
var ErrLeaseOperational = errors.New("lease source failed operationally")

// ErrAdmissionContention marks a retryable wait on Conductor's own
// channel/provider/credential admission serialization. A context deadline by
// itself is not capacity: resolver, pool checkout, SQL, and network deadlines
// remain operational failures unless this marker identifies the lock boundary.
var ErrAdmissionContention = errors.New("lease admission lock contended")

// ErrLeaseCommitAmbiguous means the lease transaction reached its final
// mutation but the COMMIT result could not be proven. Pool receives the
// provisional stream/client identity so it can reconcile and release exactly
// that client off the response path instead of leaking capacity.
var ErrLeaseCommitAmbiguous = errors.New("lease commit outcome ambiguous")

// ErrRelocationStateUnknown prevents a pump from retrying its old URL when a
// relocation COMMIT may have moved PostgreSQL to another source/credential.
// The safe response is to stop this pump and let its clients retry from DB
// truth, never run source A while accounting source B.
var ErrRelocationStateUnknown = errors.New("relocation commit state unknown")

// ErrHardStaleFinalizeStateUnknown means exact hard-stale finalization reached
// COMMIT but neither its response nor a bounded replay proved durable truth.
// The process-local pump must remain fail-closed; publishing more media could
// target a generation whose durable clients were already consumed.
var ErrHardStaleFinalizeStateUnknown = errors.New("hard-stale finalization state unknown")

// ErrStreamGone signals the active_stream row no longer exists (every
// client released and the sweeper reaped it). A relocating pump should
// stop, not retry.
var ErrStreamGone = errors.New("active stream gone")

// CredentialResolver decrypts the credential password and substitutes it
// into the upstream URL template. Implementing it outside `store` avoids a
// dependency on the AES-GCM key management code from the data layer.
type CredentialResolver interface {
	Resolve(ctx context.Context, c ProviderCredential, urlTemplate string) (string, error)
}

// PassthroughResolverForTest leaves the URL unchanged, ignoring credentials.
// Exported here (instead of only living in `stream`) so the lease
// integration test can use it without creating an import cycle.
type PassthroughResolverForTest struct{}

func (PassthroughResolverForTest) Resolve(_ context.Context, _ ProviderCredential, urlTemplate string) (string, error) {
	return urlTemplate, nil
}

// AcquireLease walks candidate sources for a channel and returns the first
// successful lease (either an attach to an existing upstream, or a new slot).
//
// On no-slot-anywhere returns (Lease{}, ErrNoSlot).
func (db *DB) AcquireLease(
	ctx context.Context,
	channelID uuid.UUID,
	resolver CredentialResolver,
) (Lease, error) {
	return db.acquireLease(ctx, channelID, resolver, uuid.Nil, nil)
}

// AcquireLeaseForClient registers a durable identity for one Pool client in
// the existing stream_client table. ReleaseLeaseClient can then be retried
// safely after a timeout: deleting the same identity twice never decrements a
// later viewer's refcount.
func (db *DB) AcquireLeaseForClient(
	ctx context.Context,
	channelID uuid.UUID,
	resolver CredentialResolver,
	clientID uuid.UUID,
) (Lease, error) {
	if clientID == uuid.Nil {
		return Lease{}, errors.New("lease client id is required")
	}
	return db.acquireLease(ctx, channelID, resolver, clientID, nil)
}

// AcquireDVRLease is the reservation primitive used by the DVR scheduler.
// It shares an existing channel upstream first; a new upstream is admitted
// only when the candidate provider would retain the configured live-view
// reserve. Credential row locking remains the final slot arbiter.
func (db *DB) AcquireDVRLease(
	ctx context.Context,
	channelID uuid.UUID,
	resolver CredentialResolver,
	policy DVRLeasePolicy,
) (Lease, error) {
	if policy.LiveReserve < 0 {
		policy.LiveReserve = 0
	}
	return db.acquireLease(ctx, channelID, resolver, uuid.Nil, &policy)
}

// AcquireDVRLeaseForClient combines DVR admission with a durable Pool client
// identity. The identity makes cleanup replay-safe when the first release
// attempt times out after committing in PostgreSQL.
func (db *DB) AcquireDVRLeaseForClient(
	ctx context.Context,
	channelID uuid.UUID,
	resolver CredentialResolver,
	policy DVRLeasePolicy,
	clientID uuid.UUID,
) (Lease, error) {
	if clientID == uuid.Nil {
		return Lease{}, errors.New("lease client id is required")
	}
	if policy.LiveReserve < 0 {
		policy.LiveReserve = 0
	}
	return db.acquireLease(ctx, channelID, resolver, clientID, &policy)
}

// CurrentLeaseForClient reloads the source/credential/URL currently bound to
// a durable Pool client. A shared pump may relocate after a DVR reservation is
// admitted but before that reservation calls Serve; using the original Lease
// snapshot would then start source A while PostgreSQL accounts source B.
func (db *DB) CurrentLeaseForClient(
	ctx context.Context,
	streamID, clientID uuid.UUID,
) (Lease, error) {
	return db.currentLeaseForClient(ctx, streamID, clientID, uuid.Nil)
}

// ClaimPumpLeaseForClient reloads authoritative source truth and atomically
// assigns a new process-pump generation at the channel outcome boundary. An
// old pump can then terminalize only its own generation, never a replacement
// that retained the same active_stream identity.
func (db *DB) ClaimPumpLeaseForClient(
	ctx context.Context,
	streamID, clientID uuid.UUID,
) (Lease, error) {
	return db.currentLeaseForClient(ctx, streamID, clientID, uuid.New())
}

func (db *DB) currentLeaseForClient(
	ctx context.Context,
	streamID, clientID, claimGeneration uuid.UUID,
) (Lease, error) {
	if clientID == uuid.Nil {
		return Lease{}, errors.New("lease client id is required")
	}
	lease := Lease{
		Outcome:        LeaseShared,
		ActiveStreamID: streamID,
		LeaseClientID:  clientID,
	}
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		// channel_id is immutable. Read it first, then join the same advisory
		// boundary as acquire/relocate and re-read the complete row. A reload
		// cannot slip between relocation taking its channel lock and publishing
		// source B, which would otherwise start A while PostgreSQL accounts B.
		var channelID uuid.UUID
		queryErr := tx.QueryRow(ctx, `
			SELECT a.channel_id
			  FROM active_stream a
			  JOIN stream_client sc ON sc.active_stream_id = a.id
			 WHERE a.id = $1 AND sc.id = $2`, streamID, clientID).Scan(&channelID)
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return ErrStreamGone
		}
		if queryErr != nil {
			return queryErr
		}
		if _, lockErr := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:channel-lease:' || $1::text, 0)
			)`, channelID); lockErr != nil {
			return fmt.Errorf("wait for current channel lease: %w", lockErr)
		}
		if claimGeneration != uuid.Nil {
			queryErr = tx.QueryRow(ctx, `
				UPDATE active_stream a
				   SET pump_generation = $3
				 WHERE a.id = $1
				   AND a.state IN ('starting','running')
				   AND EXISTS (
					SELECT 1 FROM stream_client sc
					 WHERE sc.active_stream_id = a.id AND sc.id = $2
				   )
				 RETURNING a.channel_id, a.channel_source_id, a.credential_id,
				           a.upstream_url, a.client_count`,
				streamID, clientID, claimGeneration).Scan(
				&lease.ChannelID, &lease.ChannelSourceID, &lease.CredentialID,
				&lease.UpstreamURL, &lease.ClientCount)
			lease.PumpGeneration = claimGeneration
		} else {
			queryErr = tx.QueryRow(ctx, `
				SELECT a.channel_id, a.channel_source_id, a.credential_id,
				       a.upstream_url, a.client_count,
				       COALESCE(a.pump_generation, '00000000-0000-0000-0000-000000000000'::uuid)
				  FROM active_stream a
				  JOIN stream_client sc ON sc.active_stream_id = a.id
				 WHERE a.id = $1
				   AND sc.id = $2
				   AND a.state IN ('starting','running')`, streamID, clientID).Scan(
				&lease.ChannelID, &lease.ChannelSourceID, &lease.CredentialID,
				&lease.UpstreamURL, &lease.ClientCount, &lease.PumpGeneration)
		}
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return ErrStreamGone
		}
		return queryErr
	})
	if errors.Is(err, ErrStreamGone) {
		return Lease{}, fmt.Errorf("%w: durable client does not own an active stream", ErrStreamGone)
	}
	if err != nil {
		return Lease{}, fmt.Errorf("reload active lease: %w", err)
	}
	return lease, nil
}

// ResolveAmbiguousLeaseClient waits on the same channel advisory lock held by
// acquire. Once it owns that lock, the uncertain transaction has definitively
// committed or rolled back: found=false can no longer race a late-visible
// commit, while found=true returns the current row bound to the exact token.
func (db *DB) ResolveAmbiguousLeaseClient(
	ctx context.Context,
	provisional Lease,
) (lease Lease, found bool, err error) {
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if _, lockErr := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:channel-lease:' || $1::text, 0)
			)`, provisional.ChannelID); lockErr != nil {
			return fmt.Errorf("wait for ambiguous channel lease: %w", lockErr)
		}
		lease = Lease{
			Outcome:        provisional.Outcome,
			ActiveStreamID: provisional.ActiveStreamID,
			LeaseClientID:  provisional.LeaseClientID,
		}
		queryErr := tx.QueryRow(ctx, `
			SELECT a.channel_id, a.channel_source_id, a.credential_id,
			       a.upstream_url, a.client_count
			  FROM active_stream a
			  JOIN stream_client sc ON sc.active_stream_id = a.id
			 WHERE a.id = $1 AND sc.id = $2`,
			provisional.ActiveStreamID, provisional.LeaseClientID).Scan(
			&lease.ChannelID, &lease.ChannelSourceID, &lease.CredentialID,
			&lease.UpstreamURL, &lease.ClientCount)
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return nil
		}
		if queryErr != nil {
			return fmt.Errorf("resolve ambiguous lease client: %w", queryErr)
		}
		found = true
		return nil
	})
	return lease, found, err
}

func (db *DB) acquireLease(
	ctx context.Context,
	channelID uuid.UUID,
	resolver CredentialResolver,
	clientID uuid.UUID,
	dvrPolicy *DVRLeasePolicy,
) (Lease, error) {
	sources, err := db.ListSourcesForChannel(ctx, channelID)
	if err != nil {
		return Lease{}, fmt.Errorf("%w: list sources: %w", ErrLeaseOperational, err)
	}
	if len(sources) == 0 {
		return Lease{}, fmt.Errorf("%w: channel %s has no enabled sources",
			ErrLeaseOperational, channelID)
	}

	var unexpected []error
	var contentions []error
	capacityFailures := 0
	for _, src := range sources {
		l, err := db.tryAcquireOnSource(ctx, channelID, src, resolver, clientID, dvrPolicy)
		if err == nil {
			return l, nil
		}
		if errors.Is(err, ErrLeaseCommitAmbiguous) {
			return l, fmt.Errorf("%w acquiring channel %s: %w",
				ErrLeaseOperational, channelID, err)
		}
		if errors.Is(err, ErrNoSlot) {
			capacityFailures++
			continue
		}
		sourceErr := fmt.Errorf("source %s: %w", src.ID, err)
		if errors.Is(err, ErrAdmissionContention) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if len(unexpected) > 0 {
					unexpected = append(unexpected, sourceErr)
					return Lease{}, fmt.Errorf(
						"%w while acquiring channel %s: %w",
						ErrLeaseOperational, channelID, errors.Join(unexpected...))
				}
				return Lease{}, fmt.Errorf("%w acquiring channel %s: %w",
					ErrAdmissionContention, channelID, sourceErr)
			}
			contentions = append(contentions, sourceErr)
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			unexpected = append(unexpected, sourceErr)
			return Lease{}, fmt.Errorf(
				"%w while acquiring channel %s: %w",
				ErrLeaseOperational, channelID, errors.Join(unexpected...))
		}
		unexpected = append(unexpected, sourceErr)
	}
	if len(unexpected) > 0 {
		unexpected = append(unexpected, contentions...)
		return Lease{}, fmt.Errorf(
			"%w acquiring channel %s (%d source(s) exhausted capacity): %w",
			ErrLeaseOperational, channelID, capacityFailures, errors.Join(unexpected...))
	}
	if len(contentions) > 0 {
		return Lease{}, fmt.Errorf(
			"%w acquiring channel %s (%d source(s) exhausted capacity): %w",
			ErrAdmissionContention, channelID, capacityFailures, errors.Join(contentions...))
	}
	// ErrNoSlot is a strict contract: every enabled source reached a real
	// credential/provider/domain capacity decision and none could admit it.
	return Lease{}, ErrNoSlot
}

// tryAcquireOnSource is one source's worth of: attach-existing-or-lease-new.
func (db *DB) tryAcquireOnSource(
	ctx context.Context,
	channelID uuid.UUID,
	src ChannelSource,
	resolver CredentialResolver,
	clientID uuid.UUID,
	dvrPolicy *DVRLeasePolicy,
) (Lease, error) {
	var out Lease
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		// A response-ambiguous COMMIT can let cleanup begin while PostgreSQL is
		// still resolving this transaction. Fence every durable client identity
		// before any active_stream read/write so ReleaseLeaseClient can wait for
		// the acquisition outcome before treating a missing row as authoritative.
		if err := lockLeaseClientOperation(ctx, tx, clientID); err != nil {
			return err
		}

		// Serialize attach-or-create for this logical channel inside the owning
		// runtime. RuntimeSingleton prevents a foreign process from treating this
		// DB row as its own process-local pump; the transaction lock still closes
		// concurrent request races through the insert.
		if _, err := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:channel-lease:' || $1::text, 0)
			)`, channelID); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: lock channel lease: %w", ErrAdmissionContention, err)
			}
			return fmt.Errorf("lock channel lease: %w", err)
		}

		// Step 1: existing upstream for this channel? Bump refcount. Search the
		// whole channel because its pump may have relocated to a fallback source.
		var existingID uuid.UUID
		var existingCount int
		var existingSource uuid.UUID
		var existingCred uuid.UUID
		var existingURL string
		err := tx.QueryRow(ctx, `
			SELECT id, client_count, channel_source_id, credential_id, upstream_url
			  FROM active_stream
			 WHERE channel_id = $1
			   AND state IN ('starting', 'running')
			 ORDER BY started_at, id
			 FOR UPDATE
			 LIMIT 1`, channelID).Scan(
			&existingID, &existingCount, &existingSource, &existingCred, &existingURL)

		switch {
		case err == nil:
			// Bump refcount + heartbeat.
			ct, bumpErr := tx.Exec(ctx, `
				UPDATE active_stream
				   SET client_count = client_count + 1,
				       last_heartbeat = now()
				 WHERE id = $1`, existingID)
			if bumpErr != nil {
				return fmt.Errorf("bump refcount: %w", bumpErr)
			}
			if ct.RowsAffected() != 1 {
				return fmt.Errorf("bump refcount: 0 rows affected (race?)")
			}
			if err := registerLeaseClient(ctx, tx, existingID, clientID); err != nil {
				return err
			}
			out = Lease{
				Outcome:         LeaseShared,
				ActiveStreamID:  existingID,
				LeaseClientID:   clientID,
				ChannelID:       channelID,
				ChannelSourceID: existingSource,
				CredentialID:    existingCred,
				UpstreamURL:     existingURL,
				ClientCount:     existingCount + 1,
			}
			if db.leaseBeforeCommitHook != nil {
				return db.leaseBeforeCommitHook(out)
			}
			return nil
		case errors.Is(err, pgx.ErrNoRows):
			// fall through to new lease
		default:
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: lock existing upstream: %w", ErrAdmissionContention, err)
			}
			return fmt.Errorf("look up existing upstream: %w", err)
		}

		// Serialize provider-local credential selection and the explicit aggregate
		// quota domain. Live requests may consume the retained floor, but they take
		// the same domain lock so a waiting DVR observes their committed insert.
		configuredDomain, _, err := lockProviderCapacityDomain(ctx, tx, src.ProviderID)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: lock provider/domain admission: %w", ErrAdmissionContention, err)
			}
			return fmt.Errorf("lock provider/domain admission: %w", err)
		}
		if dvrPolicy != nil {
			ok, err := capacityDomainHasDVRCapacity(
				ctx, tx, src.ProviderID, configuredDomain, dvrPolicy.LiveReserve)
			if err != nil {
				return err
			}
			if !ok {
				return ErrNoSlot
			}
		}

		// Step 2: pick a credential under this source's provider, ORDER BY
		// load+priority, with FOR UPDATE SKIP LOCKED so contending
		// transactions race past each other onto different rows.
		var cred ProviderCredential
		err = tx.QueryRow(ctx, `
			SELECT pc.id, pc.provider_id, pc.username, pc.password_enc,
			       pc.max_streams, pc.priority, pc.notes, pc.enabled, pc.created_at
			  FROM provider_credential pc
			 WHERE pc.provider_id = $1 AND pc.enabled
			 ORDER BY (
			       SELECT COUNT(*)::float / pc.max_streams
			         FROM active_stream a
			        WHERE a.credential_id = pc.id
			          AND a.state IN ('starting','running','draining')
			   ) ASC,
			   pc.priority ASC,
			   pc.created_at ASC
			 FOR UPDATE OF pc SKIP LOCKED
			 LIMIT 1`, src.ProviderID).Scan(
			&cred.ID, &cred.ProviderID, &cred.Username, &cred.PasswordEnc,
			&cred.MaxStreams, &cred.Priority, &cred.Notes, &cred.Enabled, &cred.CreatedAt,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return classifyCredentialAdmission(ctx, tx, src.ProviderID, nil)
		}
		if err != nil {
			return fmt.Errorf("pick credential: %w", err)
		}

		// Step 3: re-verify slot availability *under the lock*. The ORDER BY
		// subquery in step 2 uses the snapshot count, which can be stale by
		// the time we hold the lock.
		var inUse int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)::int FROM active_stream
			 WHERE credential_id = $1 AND state IN ('starting','running','draining')`,
			cred.ID).Scan(&inUse); err != nil {
			return fmt.Errorf("verify slot: %w", err)
		}
		if inUse >= cred.MaxStreams {
			// A free credential can have been skipped while this full row was
			// selected. Distinguish that transient lock from true saturation.
			return classifyCredentialAdmission(ctx, tx, src.ProviderID, nil)
		}
		if dvrPolicy != nil {
			// READ COMMITTED gives each statement a fresh snapshot. Recheck after
			// the credential lock so a live lease that committed while this DVR
			// transaction waited is included in the aggregate reserve decision.
			ok, err := capacityDomainHasDVRCapacity(
				ctx, tx, src.ProviderID, configuredDomain, dvrPolicy.LiveReserve)
			if err != nil {
				return err
			}
			if !ok {
				return ErrNoSlot
			}
		}

		// Step 4: resolve the upstream URL with the chosen credential.
		resolved, err := resolver.Resolve(ctx, cred, src.UpstreamURL)
		if err != nil {
			return fmt.Errorf("resolve upstream: %w", err)
		}

		// Step 5: insert active_stream (state=starting).
		var streamID uuid.UUID
		if err := tx.QueryRow(ctx, `
			INSERT INTO active_stream
			   (channel_id, channel_source_id, credential_id, upstream_url, client_count, state)
			VALUES ($1, $2, $3, $4, 1, 'starting')
			RETURNING id`,
			channelID, src.ID, cred.ID, resolved).Scan(&streamID); err != nil {
			return fmt.Errorf("insert active_stream: %w", err)
		}
		if err := registerLeaseClient(ctx, tx, streamID, clientID); err != nil {
			return err
		}

		out = Lease{
			Outcome:         LeaseNew,
			ActiveStreamID:  streamID,
			LeaseClientID:   clientID,
			ChannelID:       channelID,
			ChannelSourceID: src.ID,
			CredentialID:    cred.ID,
			UpstreamURL:     resolved,
			ClientCount:     1,
		}
		if db.leaseBeforeCommitHook != nil {
			return db.leaseBeforeCommitHook(out)
		}
		return nil
	})
	err = db.applyLeaseCommitResultHook(err)
	if err != nil {
		if out.ActiveStreamID == uuid.Nil {
			return Lease{}, err
		}
		// A healthy request can cheaply reconcile DB truth synchronously. Never
		// extend an already-expired Plex deadline; Pool receives the provisional
		// identity and performs bounded off-path cleanup instead.
		if ctx.Err() == nil && out.LeaseClientID != uuid.Nil {
			reconcileCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			current, committed, reconcileErr := db.ResolveAmbiguousLeaseClient(
				reconcileCtx, out)
			cancel()
			if reconcileErr == nil && committed {
				current.Outcome = out.Outcome
				return current, nil
			}
			if reconcileErr == nil {
				return Lease{}, err
			}
		}
		return out, fmt.Errorf("%w for stream %s: %w",
			ErrLeaseCommitAmbiguous, out.ActiveStreamID, err)
	}
	return out, nil
}

// lockLeaseClientOperation serializes acquisition and exact-token cleanup for
// one durable client UUID. Row locks cannot see an insertion that was
// uncommitted when their statement snapshot began; this transaction-scoped
// advisory fence waits for that acquisition to commit or roll back first. The
// next READ COMMITTED statement then observes an authoritative outcome.
func lockLeaseClientOperation(ctx context.Context, tx pgx.Tx, clientID uuid.UUID) error {
	if clientID == uuid.Nil {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:lease-client:' || $1::text, 0)
		)`, clientID); err != nil {
		return fmt.Errorf("wait for lease client operation: %w", err)
	}
	return nil
}

func registerLeaseClient(ctx context.Context, tx pgx.Tx, streamID, clientID uuid.UUID) error {
	if clientID == uuid.Nil {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO stream_client
		       (id, active_stream_id, remote_addr, user_agent)
		VALUES ($1, $2, '0.0.0.0'::inet, 'conductor-pool-lease')`,
		clientID, streamID); err != nil {
		return fmt.Errorf("register lease client: %w", err)
	}
	return nil
}

// capacityDomainHasDVRCapacity reports whether one new DVR upstream may be
// inserted while retaining the aggregate domain's live-view floor. An empty
// configured domain resolves to one provider-specific key, preserving the
// historical isolated-provider semantics. The floor is clamped to capacity-1.
func capacityDomainHasDVRCapacity(
	ctx context.Context,
	tx pgx.Tx,
	providerID uuid.UUID,
	configuredDomain string,
	requestedReserve int,
) (bool, error) {
	var capacity, inUse int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(pc.max_streams), 0)::int,
		       (SELECT COUNT(*)::int
		          FROM active_stream a
		          JOIN provider_credential active_pc ON active_pc.id = a.credential_id
		          JOIN provider active_p ON active_p.id = active_pc.provider_id
		         WHERE (($2 = '' AND active_p.id = $1)
		                OR ($2 <> '' AND active_p.capacity_domain = $2))
		           AND active_pc.enabled
		           AND a.state IN ('starting','running','draining'))
		  FROM provider_credential pc
		  JOIN provider p ON p.id = pc.provider_id
		 WHERE pc.enabled
		   AND (($2 = '' AND p.id = $1)
		        OR ($2 <> '' AND p.capacity_domain = $2))`,
		providerID, configuredDomain).Scan(&capacity, &inUse); err != nil {
		return false, fmt.Errorf("count capacity-domain dvr capacity: %w", err)
	}
	if capacity <= 0 {
		return false, nil
	}
	effectiveReserve := requestedReserve
	if effectiveReserve < 0 {
		effectiveReserve = 0
	}
	if effectiveReserve >= capacity {
		effectiveReserve = capacity - 1
	}
	return capacity-inUse > effectiveReserve, nil
}

// classifyCredentialAdmission re-reads the provider's committed capacity
// after a FOR UPDATE SKIP LOCKED selection found no usable credential. A free
// enabled credential means admission skipped a transiently locked row; no free
// credential means the provider is genuinely saturated in the current READ
// COMMITTED snapshot. Relocation excludes the stream being moved because a
// same-credential move vacates and reuses that slot atomically.
func classifyCredentialAdmission(
	ctx context.Context,
	tx pgx.Tx,
	providerID uuid.UUID,
	excludeStreamID *uuid.UUID,
) error {
	var hasFree bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM provider_credential pc
			 WHERE pc.provider_id = $1
			   AND pc.enabled
			   AND (
				SELECT COUNT(*)::int
				  FROM active_stream a
				 WHERE a.credential_id = pc.id
				   AND a.state IN ('starting','running','draining')
				   AND ($2::uuid IS NULL OR a.id <> $2)
			   ) < pc.max_streams
		)`, providerID, excludeStreamID).Scan(&hasFree); err != nil {
		return fmt.Errorf("classify credential capacity: %w", err)
	}
	if hasFree {
		return fmt.Errorf("%w: free enabled credential unavailable during row-lock selection",
			ErrAdmissionContention)
	}
	return ErrNoSlot
}

// RelocateStream moves an existing active_stream onto the next viable
// (channel_source, credential) pair — the mid-stream failover primitive
// (spec Phase 5). The row keeps its identity and client_count; only the
// source/credential/url change, so client refcounts and the in-process
// Streamer survive the hop.
//
// exclude lists channel_source IDs already tried (and failed) during the
// current reconnect gap. The exclusion matters because candidate ordering
// is priority-first: health decay alone can never demote a dead priority-0
// source below a healthy priority-1 source, so without it a pump would
// retry the same dead source forever. Pass nil to allow every source
// (including the current one — that's the plain "reconnect" case).
//
// Returns ErrStreamGone when the row vanished (sweeper reaped it) and
// ErrNoSlot when no non-excluded source has a free credential slot.
func (db *DB) RelocateStream(
	ctx context.Context,
	streamID uuid.UUID,
	channelID uuid.UUID,
	exclude []uuid.UUID,
	resolver CredentialResolver,
) (Lease, error) {
	return db.relocateStream(ctx, streamID, channelID, exclude, resolver, nil)
}

// RelocateDVRStream preserves a DVR reservation's live-view floor across
// mid-stream failover. Moving within the current provider is usage-neutral
// only while the current credential is enabled; moving from a disabled
// account into an enabled account adds an enabled-budget consumer and must be
// admitted like a cross-provider move.
func (db *DB) RelocateDVRStream(
	ctx context.Context,
	streamID uuid.UUID,
	channelID uuid.UUID,
	exclude []uuid.UUID,
	resolver CredentialResolver,
	policy DVRLeasePolicy,
) (Lease, error) {
	if policy.LiveReserve < 0 {
		policy.LiveReserve = 0
	}
	return db.relocateStream(ctx, streamID, channelID, exclude, resolver, &policy)
}

func (db *DB) relocateStream(
	ctx context.Context,
	streamID uuid.UUID,
	channelID uuid.UUID,
	exclude []uuid.UUID,
	resolver CredentialResolver,
	dvrPolicy *DVRLeasePolicy,
) (Lease, error) {
	sources, err := db.ListSourcesForChannel(ctx, channelID)
	if err != nil {
		return Lease{}, fmt.Errorf("%w: list sources: %w", ErrLeaseOperational, err)
	}
	excluded := make(map[uuid.UUID]bool, len(exclude))
	for _, id := range exclude {
		excluded[id] = true
	}

	var unexpected []error
	var contentions []error
	capacityFailures := 0
	attempted := 0
	for _, src := range sources {
		if excluded[src.ID] {
			continue
		}
		attempted++
		l, err := db.tryRelocateOnSource(ctx, streamID, src, resolver, dvrPolicy)
		if err == nil {
			return l, nil
		}
		if errors.Is(err, ErrRelocationStateUnknown) {
			return l, err
		}
		if errors.Is(err, ErrStreamGone) {
			return Lease{}, err
		}
		if errors.Is(err, ErrNoSlot) {
			capacityFailures++
			continue
		}
		sourceErr := fmt.Errorf("source %s: %w", src.ID, err)
		if errors.Is(err, ErrAdmissionContention) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				if len(unexpected) > 0 {
					unexpected = append(unexpected, sourceErr)
					return Lease{}, fmt.Errorf(
						"%w while relocating stream %s: %w",
						ErrLeaseOperational, streamID, errors.Join(unexpected...))
				}
				return Lease{}, fmt.Errorf("%w relocating stream %s: %w",
					ErrAdmissionContention, streamID, sourceErr)
			}
			contentions = append(contentions, sourceErr)
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			unexpected = append(unexpected, sourceErr)
			return Lease{}, fmt.Errorf(
				"%w while relocating stream %s: %w",
				ErrLeaseOperational, streamID, errors.Join(unexpected...))
		}
		unexpected = append(unexpected, sourceErr)
	}
	if len(unexpected) > 0 {
		unexpected = append(unexpected, contentions...)
		return Lease{}, fmt.Errorf(
			"%w relocating stream %s (%d source(s) exhausted capacity): %w",
			ErrLeaseOperational, streamID, capacityFailures, errors.Join(unexpected...))
	}
	if len(contentions) > 0 {
		return Lease{}, fmt.Errorf(
			"%w relocating stream %s (%d source(s) exhausted capacity): %w",
			ErrAdmissionContention, streamID, capacityFailures, errors.Join(contentions...))
	}
	if attempted == 0 || capacityFailures == attempted {
		return Lease{}, ErrNoSlot
	}
	return Lease{}, fmt.Errorf("relocate stream %s had no conclusive source result", streamID)
}

// tryRelocateOnSource is one source's worth of relocate: verify the target
// credential has capacity (not counting our own row — we vacate that slot
// in the same transaction) and repoint the active_stream row.
func (db *DB) tryRelocateOnSource(
	ctx context.Context,
	streamID uuid.UUID,
	src ChannelSource,
	resolver CredentialResolver,
	dvrPolicy *DVRLeasePolicy,
) (Lease, error) {
	var before, out Lease
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		// Use the same channel boundary as acquire so an ambiguous relocation
		// can be reconciled deterministically before the pump chooses a URL.
		if _, err := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:channel-lease:' || $1::text, 0)
			)`, src.ChannelID); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: lock channel relocation: %w", ErrAdmissionContention, err)
			}
			return fmt.Errorf("lock channel relocation: %w", err)
		}
		// Every relocation takes the target provider and aggregate-domain boundary.
		// This keeps ordinary live failover serialized with a DVR reserve decision
		// even when the two source rows belong to different provider records.
		targetConfiguredDomain, targetCapacityDomain, err :=
			lockProviderCapacityDomain(ctx, tx, src.ProviderID)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: lock target provider/domain for relocation: %w",
					ErrAdmissionContention, err)
			}
			return fmt.Errorf("lock target provider/domain for relocation: %w", err)
		}
		// Lock our own row next. Serializes against ReleaseClient/MarkDead
		// and the sweeper's DELETE; a vanished or dead row means everyone
		// left mid-gap and there is nothing to fail over.
		var clientCount int
		var currentProviderID uuid.UUID
		var currentCredentialEnabled bool
		var currentConfiguredDomain string
		err = tx.QueryRow(ctx, `
			SELECT a.client_count, a.channel_source_id, a.credential_id,
			       a.upstream_url, pc.provider_id, pc.enabled,
			       current_p.capacity_domain
			  FROM active_stream a
			  JOIN provider_credential pc ON pc.id = a.credential_id
			  JOIN provider current_p ON current_p.id = pc.provider_id
			 WHERE a.id = $1 AND a.state IN ('starting','running')
			 FOR UPDATE OF a`, streamID).Scan(
			&clientCount, &before.ChannelSourceID, &before.CredentialID,
			&before.UpstreamURL, &currentProviderID, &currentCredentialEnabled,
			&currentConfiguredDomain)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrStreamGone
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: lock active_stream: %w", ErrAdmissionContention, err)
			}
			return fmt.Errorf("lock active_stream: %w", err)
		}
		before.ActiveStreamID = streamID
		before.ChannelID = src.ChannelID
		before.ClientCount = clientCount

		currentCapacityDomain := effectiveCapacityDomain(
			currentProviderID, currentConfiguredDomain)
		// Moving between provider rows in one shared quota domain is usage-neutral
		// while the current credential still counts in that domain. A disabled
		// current credential or a cross-domain move adds one enabled consumer.
		dvrBudgetMove := dvrPolicy != nil &&
			(targetCapacityDomain != currentCapacityDomain || !currentCredentialEnabled)
		if dvrBudgetMove {
			// The moving row still belongs to the old domain (or a disabled account),
			// so target enabled usage naturally excludes it while the lock is held.
			ok, err := capacityDomainHasDVRCapacity(
				ctx, tx, src.ProviderID, targetConfiguredDomain, dvrPolicy.LiveReserve)
			if err != nil {
				return err
			}
			if !ok {
				return ErrNoSlot
			}
		}

		// Pick a credential under this source's provider — same
		// SKIP LOCKED dance as tryAcquireOnSource. Exclude our own row from
		// ordering just as the locked capacity recheck does below.
		var cred ProviderCredential
		err = tx.QueryRow(ctx, `
			SELECT pc.id, pc.provider_id, pc.username, pc.password_enc,
			       pc.max_streams, pc.priority, pc.notes, pc.enabled, pc.created_at
			  FROM provider_credential pc
			 WHERE pc.provider_id = $1 AND pc.enabled
			 ORDER BY (
			       SELECT COUNT(*)::float / pc.max_streams
			         FROM active_stream a
			        WHERE a.credential_id = pc.id
			          AND a.state IN ('starting','running','draining')
			          AND a.id <> $2
			   ) ASC,
			   pc.priority ASC,
			   pc.created_at ASC
			 FOR UPDATE OF pc SKIP LOCKED
			 LIMIT 1`, src.ProviderID, streamID).Scan(
			&cred.ID, &cred.ProviderID, &cred.Username, &cred.PasswordEnc,
			&cred.MaxStreams, &cred.Priority, &cred.Notes, &cred.Enabled, &cred.CreatedAt,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return classifyCredentialAdmission(ctx, tx, src.ProviderID, &streamID)
		}
		if err != nil {
			return fmt.Errorf("pick credential: %w", err)
		}

		// Capacity check under the lock, excluding our own row: moving
		// within the same credential must not count the slot being vacated.
		var inUse int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)::int FROM active_stream
			 WHERE credential_id = $1 AND state IN ('starting','running','draining')
			   AND id <> $2`,
			cred.ID, streamID).Scan(&inUse); err != nil {
			return fmt.Errorf("verify slot: %w", err)
		}
		if inUse >= cred.MaxStreams {
			// A free target credential can have been skipped while this full row
			// was selected. Apply the same strict classification as acquisition.
			return classifyCredentialAdmission(ctx, tx, src.ProviderID, &streamID)
		}
		if dvrBudgetMove {
			// Recheck after the credential lock so any live lease that committed
			// while this transaction waited is included in the target budget.
			ok, err := capacityDomainHasDVRCapacity(
				ctx, tx, src.ProviderID, targetConfiguredDomain, dvrPolicy.LiveReserve)
			if err != nil {
				return err
			}
			if !ok {
				return ErrNoSlot
			}
		}

		resolved, err := resolver.Resolve(ctx, cred, src.UpstreamURL)
		if err != nil {
			return fmt.Errorf("resolve upstream: %w", err)
		}

		ct, err := tx.Exec(ctx, `
			UPDATE active_stream
			   SET channel_source_id = $2,
			       credential_id = $3,
			       upstream_url = $4,
			       state = 'starting',
			       last_heartbeat = now()
			 WHERE id = $1`, streamID, src.ID, cred.ID, resolved)
		if err != nil {
			return fmt.Errorf("relocate active_stream: %w", err)
		}
		if ct.RowsAffected() != 1 {
			return ErrStreamGone
		}

		out = Lease{
			Outcome:         LeaseNew,
			ActiveStreamID:  streamID,
			ChannelID:       src.ChannelID,
			ChannelSourceID: src.ID,
			CredentialID:    cred.ID,
			UpstreamURL:     resolved,
			ClientCount:     clientCount,
		}
		return nil
	})
	err = db.applyLeaseCommitResultHook(err)
	if err != nil {
		if out.ActiveStreamID == uuid.Nil {
			return Lease{}, err
		}
		if ctx.Err() == nil {
			reconcileCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			current, reconcileErr := db.resolveRelocationAfterChannelBoundary(
				reconcileCtx, src.ChannelID, streamID)
			cancel()
			if reconcileErr == nil {
				if current.ChannelSourceID == out.ChannelSourceID &&
					current.CredentialID == out.CredentialID &&
					current.UpstreamURL == out.UpstreamURL {
					current.Outcome = out.Outcome
					return current, nil
				}
				if current.ChannelSourceID == before.ChannelSourceID &&
					current.CredentialID == before.CredentialID &&
					current.UpstreamURL == before.UpstreamURL {
					// Only the exact pre-transaction tuple proves rollback. The pump's
					// current URL still agrees with durable state, so another source may
					// be attempted safely.
					return Lease{}, err
				}
				// A later queued relocation won the advisory lock before this
				// reconciler. Current DB truth is authoritative but neither commit nor
				// rollback of this attempt is provable from tuple C; stop the old pump.
				return current, fmt.Errorf("%w for stream %s after state advanced: %w",
					ErrRelocationStateUnknown, streamID, err)
			}
		}
		return out, fmt.Errorf("%w for stream %s: %w",
			ErrRelocationStateUnknown, streamID, err)
	}
	return out, nil
}

func (db *DB) resolveRelocationAfterChannelBoundary(
	ctx context.Context,
	channelID, streamID uuid.UUID,
) (lease Lease, err error) {
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if _, lockErr := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:channel-lease:' || $1::text, 0)
			)`, channelID); lockErr != nil {
			return fmt.Errorf("wait for ambiguous channel relocation: %w", lockErr)
		}
		lease = Lease{Outcome: LeaseShared, ActiveStreamID: streamID}
		queryErr := tx.QueryRow(ctx, `
			SELECT channel_id, channel_source_id, credential_id,
			       upstream_url, client_count
			  FROM active_stream
			 WHERE id = $1 AND state IN ('starting','running')`, streamID).Scan(
			&lease.ChannelID, &lease.ChannelSourceID, &lease.CredentialID,
			&lease.UpstreamURL, &lease.ClientCount)
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return ErrStreamGone
		}
		if queryErr != nil {
			return fmt.Errorf("resolve ambiguous relocation: %w", queryErr)
		}
		return nil
	})
	return lease, err
}

// MarkRunning transitions a starting → running active_stream.
// Called by the proxy once upstream connection is established.
func (db *DB) MarkRunning(ctx context.Context, streamID uuid.UUID) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'running', last_heartbeat = now()
		 WHERE id = $1 AND state = 'starting'`, streamID)
	return err
}

// MarkRunningForPump is the generation-fenced pump callback variant. A
// delayed write from a detached process-local pump must not transition a
// replacement generation that claimed the same durable stream row.
func (db *DB) MarkRunningForPump(
	ctx context.Context, streamID, pumpGeneration uuid.UUID,
) error {
	if pumpGeneration == uuid.Nil {
		return errors.New("pump generation is required")
	}
	_, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'running', last_heartbeat = now()
		 WHERE id = $1
		   AND pump_generation = $2
		   AND state = 'starting'`, streamID, pumpGeneration)
	return err
}

// Heartbeat refreshes last_heartbeat. Called by the streamer pump every few
// seconds while bytes are flowing. Used by the orphan sweeper to decide
// "is this upstream alive?"
func (db *DB) Heartbeat(ctx context.Context, streamID uuid.UUID, addBytes int64) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET last_heartbeat = now(),
		       bytes_out = bytes_out + $2
		 WHERE id = $1`, streamID, addBytes)
	return err
}

// HeartbeatForPump fences byte/liveness updates to the process-local pump
// generation that actually delivered them. It also completes the idempotent
// starting -> running transition: the synchronous pre-release MarkRunning call
// is deliberately capped, so the first successful asynchronous heartbeat must
// repair a transient timeout instead of leaving a healthy pump in starting.
func (db *DB) HeartbeatForPump(
	ctx context.Context, streamID, pumpGeneration uuid.UUID, addBytes int64,
) error {
	if pumpGeneration == uuid.Nil {
		return errors.New("pump generation is required")
	}
	_, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'running',
		       last_heartbeat = now(),
		       bytes_out = bytes_out + $3
		 WHERE id = $1
		   AND pump_generation = $2
		   AND state IN ('starting','running')`, streamID, pumpGeneration, addBytes)
	return err
}

// ReleaseClient decrements client_count on an active_stream. If it hits 0
// the stream becomes a candidate for the orphan sweeper. Returns the new
// count.
func (db *DB) ReleaseClient(ctx context.Context, streamID uuid.UUID) (int, error) {
	var newCount int
	err := db.Pool.QueryRow(ctx, `
		UPDATE active_stream
		   SET client_count = GREATEST(0, client_count - 1),
		       last_heartbeat = now()
		 WHERE id = $1
		 RETURNING client_count`, streamID).Scan(&newCount)
	if errors.Is(err, pgx.ErrNoRows) {
		// Row already swept (pump OnExit or the orphan sweeper got there
		// first) — a benign teardown race, not an error. Pre-2026-06-10
		// this logged "release client: no rows in result set" on every
		// upstream-died disconnect (audit Q6).
		return 0, nil
	}
	return newCount, err
}

// ReleaseLeaseClient is the idempotent counterpart to AcquireLeaseForClient.
// The stream refcount changes only when this exact client row is deleted. A
// retry after a canceled/ambiguous response therefore observes the current
// count without decrementing a subscriber that attached in the meantime. The
// per-client transaction lock also makes the retry's data statement begin
// after an uncertain predecessor commits or rolls back; a pre-COMMIT snapshot
// can never return the predecessor's stale client_count as terminal truth.
func (db *DB) ReleaseLeaseClient(
	ctx context.Context,
	streamID, clientID uuid.UUID,
) (int, error) {
	if clientID == uuid.Nil {
		return 0, errors.New("lease client id is required")
	}
	var newCount int
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockLeaseClientOperation(ctx, tx, clientID); err != nil {
			return err
		}
		// Serialize release replays, same-channel attaches, and idle teardown
		// on the active row. In READ COMMITTED a single data-modifying CTE
		// keeps its statement-start snapshot after waiting for an overlapping
		// DELETE: it can therefore miss the now-committed token deletion yet
		// read the pre-commit positive client_count. SELECT FOR UPDATE instead
		// waits for the concurrent updater and returns its latest row version.
		if err := tx.QueryRow(ctx, `
			SELECT client_count
			  FROM active_stream
			 WHERE id = $1
			 FOR UPDATE`, streamID).Scan(&newCount); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				newCount = 0
				return nil
			}
			return fmt.Errorf("lock active stream for client release: %w", err)
		}

		removed, err := tx.Exec(ctx, `
			DELETE FROM stream_client
			 WHERE id = $2 AND active_stream_id = $1`, streamID, clientID)
		if err != nil {
			return fmt.Errorf("delete lease client: %w", err)
		}
		if removed.RowsAffected() == 0 {
			return nil
		}

		if err := tx.QueryRow(ctx, `
			UPDATE active_stream
			   SET client_count = GREATEST(0, client_count - 1),
			       last_heartbeat = now()
			 WHERE id = $1
			 RETURNING client_count`, streamID).Scan(&newCount); err != nil {
			return fmt.Errorf("decrement released lease client: %w", err)
		}
		return nil
	})
	err = db.applyLeaseCommitResultHook(err)
	return newCount, err
}

// MarkDead transitions an active_stream to 'dead'. The orphan sweeper will
// DELETE it after the grace period.
func (db *DB) MarkDead(ctx context.Context, streamID uuid.UUID) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'dead', last_heartbeat = now()
		 WHERE id = $1`, streamID)
	return err
}

// MarkDeadAfterPumpExit releases capacity only when the ended process-local
// pump has no durable clients. A DVR reservation can hold client_count > 0
// without subscribing until its recording claim succeeds; in that case the
// row must remain attachable and capacity-counted so Reservation.Serve can
// install a replacement pump without reacquiring or overbooking a slot.
func (db *DB) MarkDeadAfterPumpExit(
	ctx context.Context,
	streamID, pumpGeneration uuid.UUID,
) (bool, error) {
	if pumpGeneration == uuid.Nil {
		return false, errors.New("pump generation is required")
	}
	ct, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'dead', last_heartbeat = now()
		 WHERE id = $1
		   AND pump_generation = $2
		   AND client_count = 0
		   AND state IN ('starting','running','draining')`, streamID, pumpGeneration)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() == 1, nil
}

// MarkDrainingIfIdle atomically makes a zero-client pump non-attachable while
// keeping its credential slot counted until upstream/ffmpeg teardown actually
// returns. It closes the ReleaseClient→attach race without advertising capacity
// before the provider connection has gone away.
func (db *DB) MarkDrainingIfIdle(ctx context.Context, streamID uuid.UUID) (bool, error) {
	ct, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'draining', last_heartbeat = now()
		 WHERE id = $1 AND client_count = 0
		   AND state IN ('starting','running')`, streamID)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() == 1, nil
}

// FinalizeIdleStreamForDrain is the replay-safe tracked-pump teardown
// transition. true directs Pool to cancel its local pump when this call won
// idle→draining or a prior ambiguous attempt already left the row
// draining/dead/missing. false proves an intervening client still owns an
// attachable row, so canceling would kill a valid shared stream.
func (db *DB) FinalizeIdleStreamForDrain(ctx context.Context, streamID uuid.UUID) (bool, error) {
	var cancelPump bool
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		// A replay can overlap the uncertain COMMIT of an earlier finalization.
		// Lock in one statement, then decide from a fresh READ COMMITTED command;
		// a data-modifying CTE fallback would retain its pre-wait snapshot.
		var state string
		var clients int
		err := tx.QueryRow(ctx, `
			SELECT state::text, client_count
			  FROM active_stream
			 WHERE id = $1
			 FOR UPDATE`, streamID).Scan(&state, &clients)
		if errors.Is(err, pgx.ErrNoRows) {
			cancelPump = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock active stream for idle finalization: %w", err)
		}
		if clients != 0 {
			return nil
		}
		if state == "draining" || state == "dead" {
			cancelPump = true
			return nil
		}
		if state != "starting" && state != "running" {
			return nil
		}
		ct, err := tx.Exec(ctx, `
			UPDATE active_stream
			   SET state = 'draining', last_heartbeat = now()
			 WHERE id = $1`, streamID)
		if err != nil {
			return fmt.Errorf("finalize idle active stream for drain: %w", err)
		}
		if ct.RowsAffected() != 1 {
			return fmt.Errorf("finalize idle active stream for drain: expected one row, updated %d", ct.RowsAffected())
		}
		cancelPump = true
		return nil
	})
	return cancelPump, err
}

// MarkDeadIfIdle is for a reservation that never installed a local pump. With
// no upstream to teardown, its zero-client row can become free immediately.
func (db *DB) MarkDeadIfIdle(ctx context.Context, streamID uuid.UUID) (bool, error) {
	ct, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'dead', last_heartbeat = now()
		 WHERE id = $1 AND client_count = 0
		   AND state IN ('starting','running')`, streamID)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() == 1, nil
}

// FinalizeIdleStreamWithoutPump is the replay-safe terminal transition for a
// reservation that never installed a local pump. true means the row is now
// dead/missing and capacity may be announced; false means an intervening
// client owns the still-active row.
func (db *DB) FinalizeIdleStreamWithoutPump(ctx context.Context, streamID uuid.UUID) (bool, error) {
	var capacityFree bool
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		// As with tracked-pump teardown, decide from a fresh command after the
		// row lock. This keeps retries correct when an earlier COMMIT response was
		// ambiguous and the retry initially waited for that transaction.
		var state string
		var clients int
		err := tx.QueryRow(ctx, `
			SELECT state::text, client_count
			  FROM active_stream
			 WHERE id = $1
			 FOR UPDATE`, streamID).Scan(&state, &clients)
		if errors.Is(err, pgx.ErrNoRows) {
			capacityFree = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock active stream for no-pump finalization: %w", err)
		}
		if clients != 0 {
			return nil
		}
		if state == "dead" {
			capacityFree = true
			return nil
		}
		// A draining row still counts against provider/domain capacity until the caller
		// proves that no local pump exists and calls MarkDeadIfDraining.
		if state != "starting" && state != "running" {
			return nil
		}
		ct, err := tx.Exec(ctx, `
			UPDATE active_stream
			   SET state = 'dead', last_heartbeat = now()
			 WHERE id = $1`, streamID)
		if err != nil {
			return fmt.Errorf("finalize idle active stream without pump: %w", err)
		}
		if ct.RowsAffected() != 1 {
			return fmt.Errorf("finalize idle active stream without pump: expected one row, updated %d", ct.RowsAffected())
		}
		capacityFree = true
		return nil
	})
	return capacityFree, err
}

// MarkDeadIfDraining releases capacity after a caller has proved no local pump
// exists. A tracked pump instead reaches MarkDead from its OnExit callback.
func (db *DB) MarkDeadIfDraining(ctx context.Context, streamID uuid.UUID) (bool, error) {
	ct, err := db.Pool.Exec(ctx, `
		UPDATE active_stream
		   SET state = 'dead', last_heartbeat = now()
		 WHERE id = $1 AND client_count = 0 AND state = 'draining'`, streamID)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() == 1, nil
}

// StartupReconcileResult reports each class of process-local state recovered
// while the runtime singleton is held.
type StartupReconcileResult struct {
	ActiveStreams int64
	DVRRecordings int64
}

const startupInterruptedDVRReason = "recording interrupted by conductor restart before graceful completion; existing media files left untouched, inspect output and .partial paths"

// ReconcileStartup atomically marks process-local stream and DVR state left by
// a previous process. No in-memory pump or recorder goroutine survives a
// restart: active streams become dead for the sweeper, while recording rows
// become failed with an explicit interrupted/restart reason. The latter keeps
// UUID-qualified diagnostic artifacts untouched, removes the row from fixed
// forecast commitments, and releases the active-output uniqueness guard. Call
// only after acquiring RuntimeSingleton and before Pool/scheduler startup. Any
// error rolls back both classes so startup can fail closed against one coherent
// allocator snapshot.
func (db *DB) ReconcileStartup(ctx context.Context) (StartupReconcileResult, error) {
	var out StartupReconcileResult
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		streams, err := tx.Exec(ctx, `
			UPDATE active_stream
			   SET state = 'dead', last_heartbeat = now()
			 WHERE state <> 'dead'`)
		if err != nil {
			return fmt.Errorf("reconcile startup streams: %w", err)
		}
		recordings, err := tx.Exec(ctx, `
			UPDATE dvr_recording
			   SET state = 'failed', completed_at = now(),
			       admission_retry_at = NULL,
			       admission_reason = $1, error = $1,
			       recording_heartbeat_at = clock_timestamp(),
			       artifact_stage = CASE
			           WHEN artifact_stage = 'publishing' THEN artifact_stage
			           ELSE 'none'
			       END,
			       artifact_state = CASE
			           WHEN artifact_stage = 'publishing' THEN artifact_state
			           ELSE 'rejected'
			       END,
			       artifact_current = false,
			       artifact_filesystem_id = CASE
			           WHEN artifact_stage = 'publishing' THEN artifact_filesystem_id
			           ELSE ''
			       END,
			       artifact_error = CASE
			           WHEN artifact_stage = 'publishing' THEN artifact_error
			           ELSE $1
			       END
			 WHERE state = 'recording'`, startupInterruptedDVRReason)
		if err != nil {
			return fmt.Errorf("reconcile startup recordings: %w", err)
		}
		out.ActiveStreams = streams.RowsAffected()
		out.DVRRecordings = recordings.RowsAffected()
		return nil
	})
	if err != nil {
		return StartupReconcileResult{}, err
	}
	return out, nil
}

// SweepOrphans moves stale zero-client pumps into counted `draining` state and
// deletes rows whose teardown already completed (`dead`), archiving each
// deleted row into stream_session first so a finished live session leaves a
// durable record (migration 0027). The archive is part of the same statement:
// a session cannot be reaped without being recorded. Returned IDs tell the
// process-local pool to cancel a tracked pump; the next sweep deletes it only
// after OnExit marks it dead. Existing draining rows are retried after the idle
// timeout so a missed cancellation hint converges.
//
// Positive-client hard zombies are intentionally handled by the separate
// ListHardStaleStreams -> Pool local-activity proof ->
// FinalizeHardStaleStream path below. Folding that case into this SQL sweep
// would let a failed DB heartbeat cancel healthy local media.
func (db *DB) SweepOrphans(ctx context.Context, idleTimeoutSeconds int) ([]uuid.UUID, error) {
	rows, err := db.Pool.Query(ctx, `
		WITH to_drain AS (
			UPDATE active_stream
			   SET state = 'draining', last_heartbeat = now()
			 WHERE client_count = 0
			   AND state IN ('starting','running','draining')
			   AND last_heartbeat < now() - ($1::int * interval '1 second')
			 RETURNING id
		), deleted AS (
			DELETE FROM active_stream WHERE state = 'dead'
			RETURNING id, channel_id, channel_source_id, credential_id,
			          started_at, last_heartbeat, bytes_out
		), archived AS (
			INSERT INTO stream_session (
				id, channel_id, channel_source_id, credential_id,
				started_at, ended_at, bytes_out)
			SELECT id, channel_id, channel_source_id, credential_id,
			       started_at, last_heartbeat, bytes_out
			  FROM deleted
			ON CONFLICT (id) DO NOTHING
		)
		SELECT id FROM to_drain
		UNION ALL
		SELECT id FROM deleted`, idleTimeoutSeconds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ListHardStaleStreams returns generation-bound candidates for the mid-run
// zombie watchdog. A stale database heartbeat is only the durable half of the
// proof: callers holding the runtime singleton must also prove that the same
// local pump generation is stale (or absent) before finalizing a candidate.
func (db *DB) ListHardStaleStreams(
	ctx context.Context,
	cutoff time.Time,
) ([]HardStaleStreamCandidate, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, channel_id,
		       COALESCE(pump_generation,
		           '00000000-0000-0000-0000-000000000000'::uuid),
		       last_heartbeat
		  FROM active_stream
		 WHERE state IN ('starting','running')
		   AND last_heartbeat <= $1
		 ORDER BY last_heartbeat, id`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []HardStaleStreamCandidate
	for rows.Next() {
		var candidate HardStaleStreamCandidate
		if err := rows.Scan(
			&candidate.ID,
			&candidate.ChannelID,
			&candidate.PumpGeneration,
			&candidate.LastHeartbeat,
		); err != nil {
			return nil, err
		}
		out = append(out, candidate)
	}
	return out, rows.Err()
}

// FinalizeHardStaleStream consumes every durable client token only when the
// candidate still names the exact stale stream generation observed by the
// watchdog. The caller serializes this transaction with process-local pump
// creation by holding Pool's per-channel role lock. The matching PostgreSQL
// advisory lock extends that fence across acquire/claim/relocation.
//
// A tracked physical pump moves to draining and remains capacity-counted until
// its generation-bound OnExit callback marks it dead. A pump-less row becomes
// dead immediately because there is no provider connection left to tear down.
// The bool reports whether this exact candidate was finalized; false means a
// heartbeat, state, channel, or generation changed after candidate listing.
func (db *DB) FinalizeHardStaleStream(
	ctx context.Context,
	candidate HardStaleStreamCandidate,
	cutoff time.Time,
	hasLocalPump bool,
) (bool, error) {
	if candidate.ID == uuid.Nil || candidate.ChannelID == uuid.Nil {
		return false, errors.New("hard-stale stream and channel ids are required")
	}
	// A COMMIT response can be lost after PostgreSQL made the transition
	// durable. Replay once under the same channel advisory lock: the second
	// transaction waits for the first outcome and recognizes the exact
	// draining/dead generation as success. The caller has already stopped local
	// publication, so an unresolved second response remains safely fail-closed.
	var unresolvedCommitErr error
	for attempt := 0; attempt < 2; attempt++ {
		finalized, commitReached, err := db.finalizeHardStaleStreamOnce(
			ctx, candidate, cutoff, hasLocalPump)
		if err == nil {
			return finalized, nil
		}
		if commitReached {
			unresolvedCommitErr = errors.Join(unresolvedCommitErr, err)
		}
		if unresolvedCommitErr == nil {
			return false, err
		}
		if attempt == 0 && ctx.Err() == nil {
			continue
		}
		if !commitReached {
			// A replay can fail before reaching its own COMMIT. That does not
			// resolve the predecessor whose response was already lost; preserve
			// both causes under the typed unknown-state marker.
			unresolvedCommitErr = errors.Join(unresolvedCommitErr, err)
		}
		return false, fmt.Errorf("%w: %w",
			ErrHardStaleFinalizeStateUnknown, unresolvedCommitErr)
	}
	panic("unreachable hard-stale finalization replay")
}

func (db *DB) finalizeHardStaleStreamOnce(
	ctx context.Context,
	candidate HardStaleStreamCandidate,
	cutoff time.Time,
	hasLocalPump bool,
) (finalized bool, commitReached bool, err error) {
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:channel-lease:' || $1::text, 0)
			)`, candidate.ChannelID); err != nil {
			return fmt.Errorf("wait for hard-stale channel lease: %w", err)
		}

		var (
			channelID      uuid.UUID
			pumpGeneration uuid.UUID
			state          string
			lastHeartbeat  time.Time
			clientCount    int
			tokenCount     int
		)
		err := tx.QueryRow(ctx, `
			SELECT channel_id,
			       COALESCE(pump_generation,
			           '00000000-0000-0000-0000-000000000000'::uuid),
			       state::text, last_heartbeat, client_count,
			       (SELECT COUNT(*)::int FROM stream_client
			         WHERE active_stream_id = active_stream.id)
			  FROM active_stream
			 WHERE id = $1
			 FOR UPDATE`, candidate.ID).Scan(
			&channelID, &pumpGeneration, &state, &lastHeartbeat,
			&clientCount, &tokenCount)
		if errors.Is(err, pgx.ErrNoRows) {
			// A prior committed attempt may already have been swept. Exact row
			// identity cannot be reused, so absence is authoritative success.
			finalized = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock hard-stale active stream: %w", err)
		}
		if channelID != candidate.ChannelID || pumpGeneration != candidate.PumpGeneration {
			return nil
		}
		if (state == "dead" || (state == "draining" && hasLocalPump)) &&
			clientCount == 0 && tokenCount == 0 {
			finalized = true
			return nil
		}
		if state == "draining" && !hasLocalPump && clientCount == 0 && tokenCount == 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE active_stream
				   SET state = 'dead', last_heartbeat = clock_timestamp()
				 WHERE id = $1`, candidate.ID); err != nil {
				return fmt.Errorf("complete replayed pump-less hard-stale stream: %w", err)
			}
			finalized = true
			return nil
		}
		if (state != "starting" && state != "running") || lastHeartbeat.After(cutoff) {
			return nil
		}

		if _, err := tx.Exec(ctx, `
			DELETE FROM stream_client
			 WHERE active_stream_id = $1`, candidate.ID); err != nil {
			return fmt.Errorf("delete hard-stale stream clients: %w", err)
		}
		targetState := "dead"
		if hasLocalPump {
			targetState = "draining"
		}
		ct, err := tx.Exec(ctx, `
			UPDATE active_stream
			   SET state = $2,
			       client_count = 0,
			       last_heartbeat = clock_timestamp()
			 WHERE id = $1`, candidate.ID, targetState)
		if err != nil {
			return fmt.Errorf("finalize hard-stale active stream: %w", err)
		}
		if ct.RowsAffected() != 1 {
			return fmt.Errorf("finalize hard-stale active stream: expected one row, updated %d", ct.RowsAffected())
		}
		finalized = true
		return nil
	})
	commitReached = finalized
	err = db.applyHardStaleCommitResultHook(err)
	return finalized, commitReached, err
}
