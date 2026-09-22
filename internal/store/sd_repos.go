package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SDCredentials is the single sd_credentials row (id always 1).
type SDCredentials struct {
	Username       string
	PasswordSHA1   string
	CachedToken    string
	TokenFetchedAt *time.Time
	LastSuccessAt  *time.Time
	LastError      string
	Enabled        bool
}

// GetSDCredentials returns the singleton row. ErrNotFound when no row
// has been configured yet (operator hasn't set SD up).
func (db *DB) GetSDCredentials(ctx context.Context) (SDCredentials, error) {
	var c SDCredentials
	row := db.Pool.QueryRow(ctx, `
		SELECT username, password_sha1, cached_token,
		       token_fetched_at, last_success_at, last_error, enabled
		  FROM sd_credentials WHERE id = 1`)
	err := row.Scan(&c.Username, &c.PasswordSHA1, &c.CachedToken,
		&c.TokenFetchedAt, &c.LastSuccessAt, &c.LastError, &c.Enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return SDCredentials{}, ErrNotFound
	}
	return c, err
}

// UpsertSDCredentials writes (or updates) the singleton row. Operator
// supplies cleartext password; caller is responsible for SHA-1 hashing.
func (db *DB) UpsertSDCredentials(ctx context.Context, username, passwordSHA1 string, enabled bool) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO sd_credentials (id, username, password_sha1, enabled)
		VALUES (1, $1, $2, $3)
		ON CONFLICT (id) DO UPDATE
		   SET username = EXCLUDED.username,
		       password_sha1 = EXCLUDED.password_sha1,
		       enabled = EXCLUDED.enabled`,
		username, passwordSHA1, enabled)
	return err
}

// SDLineup is one row in sd_lineup.
type SDLineup struct {
	ID           uuid.UUID
	SDLineupID   string
	Name         string
	Location     string
	Enabled      bool
	LastPulledAt *time.Time
	LastStatus   string
	CreatedAt    time.Time
}

func (db *DB) CreateSDLineup(ctx context.Context, sdLineupID, name, location string, enabled bool) (SDLineup, error) {
	var l SDLineup
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO sd_lineup (sd_lineup_id, name, location, enabled)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (sd_lineup_id) DO UPDATE
		   SET name = EXCLUDED.name,
		       location = EXCLUDED.location,
		       enabled = EXCLUDED.enabled
		RETURNING id, sd_lineup_id, name, location, enabled,
		          last_pulled_at, last_status, created_at`,
		sdLineupID, name, location, enabled)
	if err := row.Scan(&l.ID, &l.SDLineupID, &l.Name, &l.Location, &l.Enabled,
		&l.LastPulledAt, &l.LastStatus, &l.CreatedAt); err != nil {
		return SDLineup{}, err
	}
	return l, nil
}

func (db *DB) ListEnabledSDLineups(ctx context.Context) ([]SDLineup, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, sd_lineup_id, name, location, enabled,
		       last_pulled_at, last_status, created_at
		  FROM sd_lineup WHERE enabled
		 ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SDLineup
	for rows.Next() {
		var l SDLineup
		if err := rows.Scan(&l.ID, &l.SDLineupID, &l.Name, &l.Location, &l.Enabled,
			&l.LastPulledAt, &l.LastStatus, &l.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// MarkSDLineupResult records the outcome of a pull attempt.
func (db *DB) MarkSDLineupResult(ctx context.Context, sdLineupID, status string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE sd_lineup
		   SET last_pulled_at = now(), last_status = $2
		 WHERE sd_lineup_id = $1`, sdLineupID, status)
	return err
}

// GetSDStationMD5 returns the last-known MD5 for one (lineup, station)
// pair. Used for incremental fetching — if SD's current MD5 matches
// ours, we can skip re-fetching the schedule.
func (db *DB) GetSDStationMD5(ctx context.Context, lineupID, stationID string) (string, error) {
	var md5 string
	err := db.Pool.QueryRow(ctx, `
		SELECT last_md5 FROM sd_station_state
		 WHERE sd_lineup_id = $1 AND sd_station_id = $2`,
		lineupID, stationID).Scan(&md5)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return md5, err
}

var ErrSDCandidateBlocked = errors.New("Schedules Direct candidate did not own its direct slot")

type SDStationSnapshotResult struct {
	Added     int
	Updated   int
	Unchanged int
	Deleted   int
}

// SDLineupReconcileResult describes provider stations withdrawn after the
// caller has validated and published the remaining lineup authority. Station
// state and candidate removal commit together so a failed reconciliation
// cannot expose one without the other.
type SDLineupReconcileResult struct {
	StationsWithdrawn int
	ProgramsDeleted   int
}

// ReconcileSDLineupStations applies the provider's validated current station
// set for one lineup. The caller holds AcquireSDIngestLease, which makes the
// fetched lineup authoritative against concurrent SD passes and channel
// remaps. Removed state is deleted atomically with its current/future direct SD
// candidates. A channel still referenced by another retained station state is
// left intact; that owner will either keep an identical snapshot or republish
// after the existing displacement watermark fence.
func (db *DB) ReconcileSDLineupStations(
	ctx context.Context,
	lineupID string,
	currentStationIDs []string,
) (SDLineupReconcileResult, error) {
	var result SDLineupReconcileResult
	if strings.TrimSpace(lineupID) == "" {
		return result, errors.New("SD lineup reconciliation requires a lineup identifier")
	}
	current := make([]string, 0, len(currentStationIDs))
	seen := make(map[string]struct{}, len(currentStationIDs))
	for _, stationID := range currentStationIDs {
		if strings.TrimSpace(stationID) != stationID || stationID == "" {
			return result, errors.New("SD lineup reconciliation contains an empty or whitespace-padded station identifier")
		}
		if _, duplicate := seen[stationID]; duplicate {
			return result, fmt.Errorf("SD lineup reconciliation contains duplicate station %s", stationID)
		}
		seen[stationID] = struct{}{}
		current = append(current, stationID)
	}

	err := db.InTx(ctx, func(tx pgx.Tx) error {
		// SD writers are pass-serialized, so this unlocked ownership scan is
		// stable. A nullable migration/remap-era owner is resolved through the
		// exact same symbolic-plus-numeric lookup used by publication. Ambiguity or
		// no mapping cannot prove a channel and therefore fails safe by preserving
		// its candidates while still withdrawing the obsolete station watermark.
		rows, err := tx.Query(ctx, `
			SELECT sd_lineup_id, sd_station_id, channel_id
			  FROM sd_station_state
			 ORDER BY sd_lineup_id, sd_station_id`)
		if err != nil {
			return err
		}
		type stationOwner struct {
			lineupID  string
			stationID string
			channelID *uuid.UUID
		}
		var owners []stationOwner
		for rows.Next() {
			var owner stationOwner
			if err := rows.Scan(&owner.lineupID, &owner.stationID, &owner.channelID); err != nil {
				rows.Close()
				return err
			}
			owners = append(owners, owner)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		resolveOwner := func(owner stationOwner) (uuid.UUID, bool, error) {
			if owner.channelID != nil && *owner.channelID != uuid.Nil {
				return *owner.channelID, true, nil
			}
			channel, err := lookupChannelByEPGID(ctx, tx, owner.stationID)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrAmbiguousEPGChannel) {
				return uuid.Nil, false, nil
			}
			if err != nil {
				return uuid.Nil, false, err
			}
			return channel.ID, true, nil
		}

		affectedSet := make(map[uuid.UUID]struct{})
		retainedSet := make(map[uuid.UUID]struct{})
		for _, owner := range owners {
			_, stationIsCurrent := seen[owner.stationID]
			removed := owner.lineupID == lineupID && !stationIsCurrent
			channelID, resolved, err := resolveOwner(owner)
			if err != nil {
				return err
			}
			if !resolved {
				continue
			}
			if removed {
				affectedSet[channelID] = struct{}{}
			} else {
				retainedSet[channelID] = struct{}{}
			}
		}
		affectedChannels := make([]uuid.UUID, 0, len(affectedSet))
		for channelID := range affectedSet {
			affectedChannels = append(affectedChannels, channelID)
		}
		sort.Slice(affectedChannels, func(left, right int) bool {
			return affectedChannels[left].String() < affectedChannels[right].String()
		})
		// Take every channel lock in deterministic order before mutating station
		// state, retaining the global channel -> station-state order used by direct
		// writers and preventing readers from observing a half-canonicalized channel.
		for _, channelID := range affectedChannels {
			if err := lockEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}

		deleteState, err := tx.Exec(ctx, `
			DELETE FROM sd_station_state
			 WHERE sd_lineup_id = $1
			   AND NOT (sd_station_id = ANY($2::text[]))`, lineupID, current)
		if err != nil {
			return err
		}
		result.StationsWithdrawn = int(deleteState.RowsAffected())

		outputChanged := false
		for _, channelID := range affectedChannels {
			if _, retainedOwner := retainedSet[channelID]; retainedOwner {
				continue
			}
			deleted, err := tx.Exec(ctx, `
				DELETE FROM epg_program
				 WHERE channel_id = $1
				   AND end_at > now()
				   AND epg_source_id IS NULL
				   AND (source_hash LIKE 'sd:%' OR is_legacy)`, channelID)
			if err != nil {
				return err
			}
			result.ProgramsDeleted += int(deleted.RowsAffected())
			canonicalChanged, _, err := canonicalizeEPGChannel(ctx, tx, channelID)
			if err != nil {
				return err
			}
			outputChanged = outputChanged || deleted.RowsAffected() > 0 || canonicalChanged
		}
		if outputChanged {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
	return result, err
}

// ReplaceSDStationSnapshot atomically publishes one station's complete SD
// schedule, removes future sd: rows omitted from it, resolves the canonical
// channel, and only then advances the provider MD5. Every non-empty snapshot
// must prove that its explicit content-sensitive sd: candidates are durable;
// a stronger direct writer causes the whole transaction to roll back. An
// authoritative empty snapshot advances the MD5 only after its future sd:
// rows are durably absent, with sd_station_state serving as the empty snapshot
// record. The caller holds AcquireSDIngestLease across the provider pass.
func (db *DB) ReplaceSDStationSnapshot(
	ctx context.Context,
	lineupID, stationID, md5 string,
	channelID uuid.UUID,
	programs []EPGProgram,
) (SDStationSnapshotResult, error) {
	var result SDStationSnapshotResult
	if lineupID == "" || stationID == "" || md5 == "" || channelID == uuid.Nil {
		return result, errors.New("SD station snapshot requires lineup, station, MD5, and channel identifiers")
	}
	seenStarts := make(map[int64]struct{}, len(programs))
	for _, program := range programs {
		if program.ChannelID != channelID || program.StartAt.IsZero() ||
			!program.EndAt.After(program.StartAt) || program.EPGSourceID != nil ||
			program.SourcePriority != PrioritySD || !strings.HasPrefix(program.SourceHash, "sd:") {
			return result, fmt.Errorf("invalid SD station candidate %q", program.SourceHash)
		}
		startKey := program.StartAt.UnixNano()
		if _, duplicate := seenStarts[startKey]; duplicate {
			return result, fmt.Errorf("duplicate SD station slot at %s", program.StartAt)
		}
		seenStarts[startKey] = struct{}{}
	}

	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, channelID); err != nil {
			return err
		}
		starts := make([]time.Time, 0, len(programs))
		hashes := make([]string, 0, len(programs))
		mutated := false
		for _, program := range programs {
			action, err := upsertEPGProgram(ctx, tx, program)
			if err != nil {
				return err
			}
			switch action {
			case "added":
				result.Added++
				mutated = true
			case "updated":
				result.Updated++
				mutated = true
			default:
				result.Unchanged++
			}
			var durableHash string
			var isLegacy bool
			if err := tx.QueryRow(ctx, `
				SELECT source_hash, is_legacy
				  FROM epg_program
				 WHERE channel_id=$1 AND start_at=$2 AND epg_source_id IS NULL`,
				channelID, program.StartAt).Scan(&durableHash, &isLegacy); err != nil {
				return err
			}
			if isLegacy || durableHash != program.SourceHash {
				return fmt.Errorf("%w: channel %s start %s", ErrSDCandidateBlocked, channelID, program.StartAt)
			}
			starts = append(starts, program.StartAt)
			hashes = append(hashes, program.SourceHash)
		}

		var deletedRows, deletedSDRows int
		var err error
		if len(programs) == 0 {
			err = tx.QueryRow(ctx, `
				WITH deleted AS (
				    DELETE FROM epg_program
				     WHERE channel_id=$1 AND end_at > now()
				       AND epg_source_id IS NULL
				       AND (source_hash LIKE 'sd:%' OR is_legacy)
				 RETURNING source_hash
				)
				SELECT count(*), count(*) FILTER (WHERE source_hash LIKE 'sd:%')
				  FROM deleted`, channelID).Scan(&deletedRows, &deletedSDRows)
		} else {
			err = tx.QueryRow(ctx, `
				WITH deleted AS (
				    DELETE FROM epg_program AS old
				     WHERE old.channel_id=$1 AND old.end_at > now()
				       AND old.epg_source_id IS NULL
				       AND (old.source_hash LIKE 'sd:%' OR old.is_legacy)
				       AND NOT EXISTS (
				           SELECT 1
				             FROM unnest($2::timestamptz[], $3::text[]) AS incoming(start_at, source_hash)
				            WHERE incoming.start_at=old.start_at
				              AND incoming.source_hash=old.source_hash
				       )
				 RETURNING source_hash
				)
				SELECT count(*), count(*) FILTER (WHERE source_hash LIKE 'sd:%')
				  FROM deleted`, channelID, starts, hashes).Scan(&deletedRows, &deletedSDRows)
		}
		if err != nil {
			return err
		}
		result.Deleted = deletedRows
		mutated = mutated || result.Deleted > 0
		if deletedSDRows > 0 {
			// A station snapshot owns the full channel horizon. If its cleanup
			// displaced rows from a different SD station at different starts,
			// same-slot upsert invalidation cannot see them. Clear provider
			// watermarks owned by this channel before committing this station's new
			// watermark so every displaced station is retried on its next pass
			// without forcing unrelated stations through a full refetch.
			if _, err := tx.Exec(ctx, `UPDATE sd_station_state SET last_md5 = '' WHERE channel_id = $1`, channelID); err != nil {
				return err
			}
		}

		// Real SD rows own their complete intervals over PPV-off placeholders.
		deleteTag, err := tx.Exec(ctx, `
			DELETE FROM epg_program AS off_row
			 WHERE off_row.channel_id = $1
			   AND off_row.source_hash LIKE 'ppv-off:%'
			   AND EXISTS (
			       SELECT 1 FROM epg_program AS real_row
			        WHERE real_row.channel_id = off_row.channel_id
			          AND real_row.source_priority < $2
			          AND real_row.start_at < off_row.end_at
			          AND real_row.end_at > off_row.start_at
			   )`, channelID, PriorityPPVOff)
		if err != nil {
			return err
		}
		result.Deleted += int(deleteTag.RowsAffected())
		mutated = mutated || deleteTag.RowsAffected() > 0

		canonicalChanged, _, err := canonicalizeEPGChannel(ctx, tx, channelID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO sd_station_state (sd_lineup_id, sd_station_id, channel_id, last_md5, last_pulled_at)
			VALUES ($1, $2, $4, $3, now())
			ON CONFLICT (sd_lineup_id, sd_station_id) DO UPDATE
			   SET channel_id = EXCLUDED.channel_id,
			       last_md5 = EXCLUDED.last_md5,
			       last_pulled_at = now()`, lineupID, stationID, md5, channelID); err != nil {
			return err
		}
		if mutated || canonicalChanged {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
	return result, err
}
