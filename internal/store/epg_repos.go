package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// EPGSource is one upstream XMLTV URL Conductor pulls from.
type EPGSource struct {
	ID                  uuid.UUID
	Name                string
	URL                 string
	AuthHeader          string
	Priority            int
	Enabled             bool
	LastETag            string
	LastModified        string
	LastFetchedAt       *time.Time
	LastStatus          string
	ConsecutiveFailures int
	LastOKAt            *time.Time
	SnapshotGeneration  int64
	SnapshotAppliedAt   *time.Time
	CreatedAt           time.Time
}

// ErrStaleEPGSnapshot marks an HTTP response that no longer belongs to the
// configured source generation that issued the request. Callers should drop
// the response and let the next poll fetch the current source unconditionally.
var ErrStaleEPGSnapshot = errors.New("stale EPG source snapshot")

// ErrStaleEPGPollAttempt marks diagnostic bookkeeping for a request that no
// longer represents the source's current config/snapshot, or that started
// before an already-recorded attempt. It is a neutral concurrency outcome,
// never evidence that the current upstream is failing.
var ErrStaleEPGPollAttempt = errors.New("stale EPG poll attempt")

// ErrAmbiguousEPGChannel marks a provider channel identifier that resolves to
// more than one Conductor channel. Publishing such a snapshot would route
// guide rows nondeterministically, so callers must retain the last-good data
// until the channel configuration is repaired.
var ErrAmbiguousEPGChannel = errors.New("ambiguous EPG channel mapping")

// StaleEPGSnapshotError identifies the source whose response lost a race with
// another snapshot, a source edit, or a channel remap.
type StaleEPGSnapshotError struct {
	SourceID uuid.UUID
	Reason   string
}

func (e *StaleEPGSnapshotError) Error() string {
	return fmt.Sprintf("%v for source %s: %s", ErrStaleEPGSnapshot, e.SourceID, e.Reason)
}

func (e *StaleEPGSnapshotError) Unwrap() error { return ErrStaleEPGSnapshot }

type StaleEPGPollAttemptError struct {
	SourceID uuid.UUID
	Reason   string
}

func (e *StaleEPGPollAttemptError) Error() string {
	return fmt.Sprintf("%v for source %s: %s", ErrStaleEPGPollAttempt, e.SourceID, e.Reason)
}

func (e *StaleEPGPollAttemptError) Unwrap() error { return ErrStaleEPGPollAttempt }

// EPGProgram is a thin upsert struct for one program entry. Used by the
// ingest pipeline. Distinct from `types.go` ActiveStream etc. — those are
// runtime state; this is reference data.
type EPGProgram struct {
	ChannelID          uuid.UUID
	StartAt            time.Time
	EndAt              time.Time
	Title              string
	SubTitle           string
	Description        string
	Category           []string
	EpisodeNumXMLTV    string
	EpisodeNumOnscreen string
	OriginalAirDate    *time.Time
	AiringCivilDate    *time.Time
	IsMovie            bool
	IsLive             bool
	// IsNew is the normalized source-row candidate. Read consumers use
	// epg_program_effective.effective_is_new after history/canonical policy.
	IsNew                   bool
	IsPremiere              bool
	IsFinale                bool
	Rating                  string
	SourcePosterURL         string
	SourceHash              string
	SourcePriority          int
	EPGSourceID             *uuid.UUID
	SourceGeneration        int64
	ProviderEpisodeID       string
	NewExplicit             bool
	PreviouslyShownExplicit bool
}

// Worker-injected rows outrank every XMLTV source (epg_source.priority is
// >= 0 by convention): overriding a generic feed's placeholder rows for
// PPV/sports slots is those workers' entire purpose. SD sits below the
// XMLTV range — it supplements; a curated feed row should win.
const (
	PriorityPPVSync = -10
	PrioritySports  = -5
	PrioritySD      = 10
	// PriorityPPVOff marks "No Event Scheduled" placeholder rows on PPV
	// slots: every real source (workers AND any XMLTV feed) outranks
	// them, and they can never overwrite anything but each other.
	PriorityPPVOff = 100
)

func (db *DB) CreateEPGSource(ctx context.Context, s EPGSource) (EPGSource, error) {
	if s.ID == uuid.Nil {
		s.ID = uuid.New()
	}
	releaseAuthority, err := db.acquireSDAuthority(ctx)
	if err != nil {
		return EPGSource{}, err
	}
	defer releaseAuthority()
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockSDIngestPass(ctx, tx); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			INSERT INTO epg_source (id, name, url, auth_header, priority, enabled)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING created_at`,
			s.ID, s.Name, s.URL, s.AuthHeader, s.Priority, s.Enabled).Scan(&s.CreatedAt)
	})
	if err != nil {
		return EPGSource{}, err
	}
	return s, nil
}

func (db *DB) ListEPGSources(ctx context.Context) ([]EPGSource, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, name, url, auth_header, priority, enabled,
		       last_etag, last_modified, last_fetched_at, last_status,
		       consecutive_failures, last_ok_at, snapshot_generation,
		       snapshot_applied_at, created_at
		  FROM epg_source
		 ORDER BY priority, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EPGSource
	for rows.Next() {
		var s EPGSource
		if err := rows.Scan(
			&s.ID, &s.Name, &s.URL, &s.AuthHeader, &s.Priority, &s.Enabled,
			&s.LastETag, &s.LastModified, &s.LastFetchedAt, &s.LastStatus,
			&s.ConsecutiveFailures, &s.LastOKAt, &s.SnapshotGeneration,
			&s.SnapshotAppliedAt, &s.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (db *DB) ListEnabledEPGSources(ctx context.Context) ([]EPGSource, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, name, url, auth_header, priority, enabled,
		       last_etag, last_modified, last_fetched_at, last_status,
		       consecutive_failures, last_ok_at, snapshot_generation,
		       snapshot_applied_at, created_at
		  FROM epg_source
		 WHERE enabled
		 ORDER BY priority, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EPGSource
	for rows.Next() {
		var s EPGSource
		if err := rows.Scan(
			&s.ID, &s.Name, &s.URL, &s.AuthHeader, &s.Priority, &s.Enabled,
			&s.LastETag, &s.LastModified, &s.LastFetchedAt, &s.LastStatus,
			&s.ConsecutiveFailures, &s.LastOKAt, &s.SnapshotGeneration,
			&s.SnapshotAppliedAt, &s.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// BeginEPGSourcePollAttempt obtains a database-clock start timestamp after
// confirming the worker's listed source is still current. That timestamp is
// the per-source attempt high-water used by UpdateEPGSourcePollState: when two
// requests share one source generation, the later-started request owns health
// bookkeeping even if an older network failure completes last.
func (db *DB) BeginEPGSourcePollAttempt(ctx context.Context, expected EPGSource) (time.Time, error) {
	var startedAt time.Time
	err := db.Pool.QueryRow(ctx, `
		SELECT clock_timestamp()
		  FROM epg_source
		 WHERE id = $1
		   AND url = $2
		   AND auth_header = $3
		   AND priority = $4
		   AND enabled = $5
		   AND enabled
		   AND last_etag = $6
		   AND last_modified = $7
		   AND snapshot_generation = $8`,
		expected.ID, expected.URL, expected.AuthHeader, expected.Priority,
		expected.Enabled, expected.LastETag, expected.LastModified,
		expected.SnapshotGeneration).Scan(&startedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, &StaleEPGPollAttemptError{
			SourceID: expected.ID,
			Reason:   "source changed or disappeared before request start",
		}
	}
	return startedAt, err
}

// UpdateEPGSourcePollState records status after a fetch attempt only when the
// exact config/generation/validators represented by that response are still
// current and no later-started attempt has already published health. XMLTV
// validators themselves remain owned by ReplaceEPGSourceSnapshot's candidate
// transaction; this method never changes them.
//
// ok marks the attempt successful (fresh payload or 304 no_change): it
// resets consecutive_failures and stamps last_ok_at; a failed attempt
// increments the streak. Returns the streak before and after this update so
// the caller can alert exactly when a threshold is crossed (prev < N ≤ cur)
// and on recovery (prev ≥ N, ok).
func (db *DB) UpdateEPGSourcePollState(
	ctx context.Context,
	expected EPGSource,
	attemptStartedAt time.Time,
	status string,
	ok bool,
) (prevFailures, curFailures int, err error) {
	if attemptStartedAt.IsZero() {
		return 0, 0, errors.New("EPG poll attempt start timestamp is zero")
	}
	row := db.Pool.QueryRow(ctx, `
		WITH candidate AS MATERIALIZED (
			SELECT id, consecutive_failures
			  FROM epg_source
			 WHERE id = $1
			   AND url = $2
			   AND auth_header = $3
			   AND priority = $4
			   AND enabled = $5
			   AND last_etag = $6
			   AND last_modified = $7
			   AND snapshot_generation = $8
			   AND (last_fetched_at IS NULL OR last_fetched_at < $9)
			 FOR UPDATE
		), updated AS (
			UPDATE epg_source AS source
			   SET last_status = $10,
			       last_fetched_at = $9,
			       consecutive_failures = CASE WHEN $11 THEN 0 ELSE source.consecutive_failures + 1 END,
			       last_ok_at = CASE WHEN $11 THEN clock_timestamp() ELSE source.last_ok_at END
			  FROM candidate
			 WHERE source.id = candidate.id
			 RETURNING candidate.consecutive_failures AS previous_failures,
			           source.consecutive_failures AS current_failures
		)
		SELECT previous_failures, current_failures FROM updated`,
		expected.ID, expected.URL, expected.AuthHeader, expected.Priority,
		expected.Enabled, expected.LastETag, expected.LastModified,
		expected.SnapshotGeneration, attemptStartedAt, status, ok)
	if err := row.Scan(&prevFailures, &curFailures); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, &StaleEPGPollAttemptError{
				SourceID: expected.ID,
				Reason:   "source changed, disappeared, or a later-started attempt already recorded health",
			}
		}
		return 0, 0, err
	}
	return prevFailures, curFailures, nil
}

// EPGSourceUpdate carries optional field changes for UpdateEPGSource.
// nil pointer = leave unchanged.
type EPGSourceUpdate struct {
	Name       *string
	URL        *string
	AuthHeader *string
	Priority   *int
	Enabled    *bool
}

// UpdateEPGSource applies a partial update. Any fetch-affecting edit advances
// the source generation; URL/auth/enabled changes also clear validators so the
// next enabled pass is unconditional. URL/auth changes reset the failure streak
// so diagnostics from the old endpoint/credentials do not taint the new ones.
// Disabling removes the source's candidates and promotes retained fallbacks in
// this transaction. Re-enabling also removes any candidates left by an older
// runtime, so only a fresh snapshot from the new generation can become visible.
func (db *DB) UpdateEPGSource(ctx context.Context, id uuid.UUID, u EPGSourceUpdate) (EPGSource, error) {
	var s EPGSource
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		var oldPriority int
		var oldEnabled bool
		if err := tx.QueryRow(ctx, `
			SELECT priority, enabled
			  FROM epg_source
			 WHERE id = $1
			 FOR UPDATE`, id).Scan(&oldPriority, &oldEnabled); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}

		row := tx.QueryRow(ctx, `
			UPDATE epg_source
			   SET name        = COALESCE($2, name),
			       url         = COALESCE($3, url),
			       auth_header = COALESCE($4, auth_header),
			       priority    = COALESCE($5, priority),
			       enabled     = COALESCE($6, enabled),
			       snapshot_generation  = snapshot_generation + CASE
			           WHEN ($3::text IS NOT NULL AND $3 IS DISTINCT FROM url)
			             OR ($4::text IS NOT NULL AND $4 IS DISTINCT FROM auth_header)
			             OR ($5::integer IS NOT NULL AND $5 IS DISTINCT FROM priority)
			             OR ($6::boolean IS NOT NULL AND $6 IS DISTINCT FROM enabled)
			           THEN 1 ELSE 0 END,
			       last_etag = CASE
			           WHEN ($3::text IS NOT NULL AND $3 IS DISTINCT FROM url)
			             OR ($4::text IS NOT NULL AND $4 IS DISTINCT FROM auth_header)
			             OR ($6::boolean IS NOT NULL AND $6 IS DISTINCT FROM enabled)
			           THEN '' ELSE last_etag END,
			       last_modified = CASE
			           WHEN ($3::text IS NOT NULL AND $3 IS DISTINCT FROM url)
			             OR ($4::text IS NOT NULL AND $4 IS DISTINCT FROM auth_header)
			             OR ($6::boolean IS NOT NULL AND $6 IS DISTINCT FROM enabled)
			           THEN '' ELSE last_modified END,
			       consecutive_failures = CASE
			           WHEN ($3::text IS NOT NULL AND $3 IS DISTINCT FROM url)
			             OR ($4::text IS NOT NULL AND $4 IS DISTINCT FROM auth_header)
			           THEN 0 ELSE consecutive_failures END
			 WHERE id = $1
			 RETURNING id, name, url, auth_header, priority, enabled,
			           last_etag, last_modified, last_fetched_at, last_status,
			           consecutive_failures, last_ok_at, snapshot_generation,
			           snapshot_applied_at, created_at`,
			id, u.Name, u.URL, u.AuthHeader, u.Priority, u.Enabled)
		if err := row.Scan(
			&s.ID, &s.Name, &s.URL, &s.AuthHeader, &s.Priority, &s.Enabled,
			&s.LastETag, &s.LastModified, &s.LastFetchedAt, &s.LastStatus,
			&s.ConsecutiveFailures, &s.LastOKAt, &s.SnapshotGeneration,
			&s.SnapshotAppliedAt, &s.CreatedAt,
		); err != nil {
			return err
		}
		priorityChanged := s.Priority != oldPriority
		enabledChanged := s.Enabled != oldEnabled
		disabledCleanup := !s.Enabled
		if !priorityChanged && !enabledChanged && !disabledCleanup {
			return nil
		}

		// Snapshot candidates retain the source priority used by the interval
		// resolver. Source state is already row-locked, and every affected channel
		// is locked below before candidates change, matching snapshot publication's
		// source -> channel lock order.
		rows, err := tx.Query(ctx, `
			SELECT DISTINCT channel_id
			  FROM epg_program
			 WHERE epg_source_id = $1
			 ORDER BY channel_id`, id)
		if err != nil {
			return err
		}
		var channelIDs []uuid.UUID
		for rows.Next() {
			var channelID uuid.UUID
			if err := rows.Scan(&channelID); err != nil {
				rows.Close()
				return err
			}
			channelIDs = append(channelIDs, channelID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, channelID := range channelIDs {
			if err := lockEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}
		if enabledChanged || disabledCleanup {
			// A disabled source must disappear immediately. Delete rather than merely
			// suppress so toggling it back on cannot resurrect a snapshot fetched
			// before the generation/validator fence. The re-enable branch also clears
			// candidates created by a pre-fix runtime before admitting a fresh poll;
			// any edit while already disabled performs the same repair rather than
			// accidentally re-ranking those stale candidates back into the guide.
			// Pre-0023 legacy rows have no source identity and deliberately remain:
			// priority is not provenance, so deleting them here could erase unrelated
			// enabled-source guide data. Migration forces enabled sources to refresh
			// those rows into attributable snapshots.
			if _, err := tx.Exec(ctx, `DELETE FROM epg_program WHERE epg_source_id = $1`, id); err != nil {
				return err
			}
		} else if priorityChanged {
			// Re-rank and re-canonicalize atomically so a priority edit takes effect
			// even when the next conditional fetch is a 304.
			if _, err := tx.Exec(ctx, `
				UPDATE epg_program
				   SET source_priority = $2,
				       updated_at = now()
				 WHERE epg_source_id = $1
				   AND source_priority IS DISTINCT FROM $2`, id, s.Priority); err != nil {
				return err
			}
		}
		for _, channelID := range channelIDs {
			if _, _, err := canonicalizeEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}
		if len(channelIDs) > 0 {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return EPGSource{}, ErrNotFound
		}
		return EPGSource{}, err
	}
	return s, nil
}

// DeleteEPGSource removes one XMLTV source and its durable candidates as a
// single guide mutation. The schema deliberately refuses raw source deletion
// while candidates exist; this path serializes with snapshot publication,
// explicitly deletes the candidates, promotes retained fallback winners, and
// advances the guide revision before the transaction commits.
func (db *DB) DeleteEPGSource(ctx context.Context, id uuid.UUID) error {
	return db.InTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			SELECT id
			  FROM epg_source
			 WHERE id = $1
			 FOR UPDATE`, id).Scan(&id); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}

		rows, err := tx.Query(ctx, `
			SELECT DISTINCT channel_id
			  FROM epg_program
			 WHERE epg_source_id = $1
			 ORDER BY channel_id`, id)
		if err != nil {
			return err
		}
		var channelIDs []uuid.UUID
		for rows.Next() {
			var channelID uuid.UUID
			if err := rows.Scan(&channelID); err != nil {
				rows.Close()
				return err
			}
			channelIDs = append(channelIDs, channelID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		for _, channelID := range channelIDs {
			if err := lockEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM epg_program WHERE epg_source_id = $1`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM epg_source WHERE id = $1`, id); err != nil {
			return err
		}
		for _, channelID := range channelIDs {
			if _, _, err := canonicalizeEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}
		if len(channelIDs) > 0 {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
}

func validateEPGSourceSnapshotExpectation(ctx context.Context, tx pgx.Tx, expected EPGSource) (int, error) {
	var current EPGSource
	if err := tx.QueryRow(ctx, `
		SELECT url, auth_header, priority, enabled, last_etag, last_modified,
		       snapshot_generation
		  FROM epg_source
		 WHERE id = $1
		 FOR UPDATE`, expected.ID).Scan(
		&current.URL, &current.AuthHeader, &current.Priority, &current.Enabled,
		&current.LastETag, &current.LastModified, &current.SnapshotGeneration,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	if current.SnapshotGeneration != expected.SnapshotGeneration ||
		current.URL != expected.URL ||
		current.AuthHeader != expected.AuthHeader ||
		current.Priority != expected.Priority ||
		current.Enabled != expected.Enabled ||
		current.LastETag != expected.LastETag ||
		current.LastModified != expected.LastModified {
		return 0, &StaleEPGSnapshotError{
			SourceID: expected.ID,
			Reason:   "source generation, configuration, or validators changed while fetching",
		}
	}
	if !current.Enabled {
		return 0, &StaleEPGSnapshotError{SourceID: expected.ID, Reason: "source is disabled"}
	}
	return current.Priority, nil
}

// ValidateEPGSourceSnapshot fences a 304 response against source edits and
// newer snapshot commits. A 304 contains no rows to publish, but it must not
// be accepted for validators or a configuration generation that changed while
// the request was in flight.
func (db *DB) ValidateEPGSourceSnapshot(ctx context.Context, expected EPGSource) error {
	return db.InTx(ctx, func(tx pgx.Tx) error {
		_, err := validateEPGSourceSnapshotExpectation(ctx, tx, expected)
		return err
	})
}

// EPGGuideHorizonDays measures how much future guide data remains, as the
// median per-enabled-channel horizon in days (time from now to the channel's
// last known programme end). Channels with no future programmes count as 0,
// so the median degrades as coverage decays across the lineup — a global
// max() would stay green while most channels go dark (the 2026-06 outage
// shape: 69/161 channels had zero future EPG while one source kept adding
// far-future rows for a handful).
//
// Channels ppvsync has ever populated (ppv_sync_channel_state) are left out.
// A PPV/event slot carries only the events the provider lists for today, so
// its horizon is under a day by nature, not by fault; by 2026-09 those slots
// were 150 of 250 enabled channels and the median measured ppvsync instead
// of the guide — "Guide horizon low: 0.8 days" stood for a week while every
// scheduled channel had 3–7 days. Excluding them, the 100 scheduled channels
// reported 6.2.
//
// channelCount is the number of scheduled (non-PPV) enabled channels
// considered; callers should skip horizon alerting when it is 0 (fresh
// install).
func (db *DB) EPGGuideHorizonDays(ctx context.Context) (medianDays float64, channelCount int, err error) {
	row := db.Pool.QueryRow(ctx, `
		SELECT count(*),
		       coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY h.days), 0)
		  FROM (
			SELECT extract(epoch FROM (coalesce(max(p.end_at), now()) - now())) / 86400.0 AS days
			  FROM channel c
			  LEFT JOIN epg_program p
			         ON p.channel_id = c.id AND p.is_canonical AND p.end_at > now()
			 WHERE c.enabled
			   AND NOT EXISTS (SELECT 1 FROM ppv_sync_channel_state s WHERE s.channel_id = c.id)
			 GROUP BY c.id
		  ) h`)
	if err := row.Scan(&channelCount, &medianDays); err != nil {
		return 0, 0, err
	}
	return medianDays, channelCount, nil
}

// LookupChannelByEPGID resolves an XMLTV <channel id="..."> to one of our
// channel rows. Tries channel.epg_channel_id first; on miss, falls back to
// channel.number when the XMLTV id parses as a numeric channel number. The
// fallback covers guides like Dispatcharr's that use the channel number as
// the XMLTV id rather than a tvg_id-style symbolic id. Returns ErrNotFound
// when neither lookup hits — the ingester uses this to skip programs for
// unmapped channels.
func (db *DB) LookupChannelByEPGID(ctx context.Context, epgChannelID string) (Channel, error) {
	return lookupChannelByEPGID(ctx, db.Pool, epgChannelID)
}

func lookupChannelByEPGID(ctx context.Context, q epgQueryRower, epgChannelID string) (Channel, error) {
	if strings.TrimSpace(epgChannelID) == "" {
		return Channel{}, ErrNotFound
	}
	var c Channel
	var numericID any
	if n, err := strconv.ParseFloat(epgChannelID, 64); err == nil && !math.IsNaN(n) && !math.IsInf(n, 0) {
		numericID = n
	}
	var candidateCount int
	row := q.QueryRow(ctx, `
		SELECT id, number, name, call_sign, logo_url, group_tag, enabled,
		       epg_channel_id, count(*) OVER ()
		  FROM (
			SELECT id, number, name, call_sign, logo_url, group_tag, enabled, epg_channel_id
			  FROM channel
			 WHERE epg_channel_id = $1
			UNION
			SELECT id, number, name, call_sign, logo_url, group_tag, enabled, epg_channel_id
			  FROM channel
			 WHERE $2::numeric IS NOT NULL AND number = $2::numeric
		  ) candidates
		 ORDER BY id
		 LIMIT 1`, epgChannelID, numericID)
	err := row.Scan(&c.ID, &c.Number, &c.Name, &c.CallSign, &c.LogoURL, &c.GroupTag,
		&c.Enabled, &c.EpgChannelID, &candidateCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return Channel{}, ErrNotFound
	}
	if err != nil {
		return Channel{}, err
	}
	if candidateCount > 1 {
		return Channel{}, fmt.Errorf("%w for provider channel %q", ErrAmbiguousEPGChannel, epgChannelID)
	}
	return c, nil
}

// DeletePPVParsedRowsForChannelStream removes existing ppv-parse rows
// for one (channel, stream_id) pair when that source explicitly reports
// an ended or idle state. Exact future events use the atomic channel-wide
// replacement below.
//
// The selected exact-event path does not run a pre-upsert sweep:
// ReplacePPVParsedRowsForChannel atomically updates the chosen row and removes
// redundant/drifted rows without an empty guide window or enrichment churn.
func (db *DB) DeletePPVParsedRowsForChannelStream(ctx context.Context, channelID uuid.UUID, streamHashKey string, observedAt time.Time) (deleted int64, applied bool, err error) {
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, channelID); err != nil {
			return err
		}
		var claimErr error
		applied, claimErr = claimPPVObservation(ctx, tx, channelID, observedAt)
		if claimErr != nil || !applied {
			return claimErr
		}
		tag, execErr := tx.Exec(ctx, `
			DELETE FROM epg_program
			 WHERE channel_id = $1
			   AND source_hash LIKE 'ppv-parse:' || $2 || ':%'`,
			channelID, streamHashKey)
		if execErr != nil {
			return execErr
		}
		deleted = tag.RowsAffected()
		return finalizeEPGChannelMutation(ctx, tx, channelID, deleted > 0)
	})
	return deleted, applied, err
}

// TimeRange is a half-open [Start, End) interval, used for occupancy checks.
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// EPGSnapshotResult describes one atomically-applied XMLTV source snapshot.
// Suppressed rows remain durable candidates; they are merely hidden from the
// canonical guide until a preferred overlapping row disappears.
type EPGSnapshotResult struct {
	Added      int
	Updated    int
	Unchanged  int
	Deleted    int
	Suppressed int
	Generation int64
}

// EPGSnapshotMapping binds one provider channel id to the Conductor channel it
// resolved to before parsing. Snapshot publication rechecks the binding after
// locking the source so a remap cannot race an in-flight HTTP response.
type EPGSnapshotMapping struct {
	ProviderChannelID string
	ChannelID         uuid.UUID
}

type canonicalEPGCandidate struct {
	ID         uuid.UUID
	Start      time.Time
	End        time.Time
	Priority   int
	SourceKey  string
	SourceHash string
	Canonical  bool
}

// ReplaceEPGSourceSnapshot installs one source's complete mapped snapshot in a
// single transaction. A successful empty snapshot is meaningful: it removes
// the source's old generation and may promote retained fallback candidates.
// mappings includes channels declared by the document even when they currently
// have no programmes, allowing an upstream deletion to clear them. Provider
// validators publish in this same transaction only after every binding passes
// its remap-race recheck.
func (db *DB) ReplaceEPGSourceSnapshot(
	ctx context.Context,
	expected EPGSource,
	programs []EPGProgram,
	mappings []EPGSnapshotMapping,
	etag string,
	lastModified string,
) (EPGSnapshotResult, error) {
	var result EPGSnapshotResult
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		priority, err := validateEPGSourceSnapshotExpectation(ctx, tx, expected)
		if err != nil {
			return err
		}
		sourceID := expected.ID
		result.Generation = expected.SnapshotGeneration + 1

		mappedSet := make(map[uuid.UUID]struct{}, len(mappings))
		channelSet := make(map[uuid.UUID]struct{}, len(mappings)+len(programs))
		for _, mapping := range mappings {
			mappedSet[mapping.ChannelID] = struct{}{}
			channelSet[mapping.ChannelID] = struct{}{}
		}
		rows, err := tx.Query(ctx, `
			SELECT DISTINCT channel_id
			  FROM epg_program
			 WHERE epg_source_id = $1`, sourceID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var channelID uuid.UUID
			if err := rows.Scan(&channelID); err != nil {
				rows.Close()
				return err
			}
			channelSet[channelID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for i := range programs {
			p := &programs[i]
			if p.ChannelID == uuid.Nil || !p.EndAt.After(p.StartAt) {
				return errors.New("invalid XMLTV snapshot programme interval")
			}
			if _, mapped := mappedSet[p.ChannelID]; !mapped {
				return errors.New("XMLTV snapshot programme lacks a current channel mapping")
			}
			channelSet[p.ChannelID] = struct{}{}
			p.SourcePriority = priority
			p.EPGSourceID = &sourceID
			p.SourceGeneration = result.Generation
		}

		channelIDs := make([]uuid.UUID, 0, len(channelSet))
		for id := range channelSet {
			channelIDs = append(channelIDs, id)
		}
		sort.Slice(channelIDs, func(i, j int) bool {
			return channelIDs[i].String() < channelIDs[j].String()
		})
		for _, channelID := range channelIDs {
			if err := lockEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}
		// Recheck provider bindings only after channel mutation authority is held.
		// This closes the create-source/remap race: a source inserted after the
		// remap's source-row scan cannot normalize under the old mapping, wait for
		// the remap's channel lock, and then publish stale rows after it commits.
		for _, mapping := range mappings {
			current, err := lookupChannelByEPGID(ctx, tx, mapping.ProviderChannelID)
			if err != nil || current.ID != mapping.ChannelID {
				reason := fmt.Sprintf("EPG channel mapping changed for %q", mapping.ProviderChannelID)
				if err != nil {
					reason += ": " + err.Error()
				}
				return &StaleEPGSnapshotError{SourceID: sourceID, Reason: reason}
			}
		}

		for _, p := range programs {
			action, err := upsertEPGSourceProgram(ctx, tx, p)
			if err != nil {
				return err
			}
			switch action {
			case "added":
				result.Added++
			case "updated":
				result.Updated++
			default:
				result.Unchanged++
			}
		}

		tag, err := tx.Exec(ctx, `
			DELETE FROM epg_program
			 WHERE epg_source_id = $1
			   AND source_generation <> $2`, sourceID, result.Generation)
		if err != nil {
			return err
		}
		result.Deleted += int(tag.RowsAffected())

		// Historical rows predate source provenance. Once a fresh source has
		// explicitly observed a mapped channel, retire those unattributable rows;
		// synthetic PPV/sports/SD rows were marked non-legacy by migration 0023.
		if len(mappings) > 0 {
			mappedChannelIDs := make([]uuid.UUID, 0, len(mappings))
			for _, mapping := range mappings {
				mappedChannelIDs = append(mappedChannelIDs, mapping.ChannelID)
			}
			tag, err = tx.Exec(ctx, `
				DELETE FROM epg_program
				 WHERE is_legacy
				   AND channel_id = ANY($1::uuid[])`, mappedChannelIDs)
			if err != nil {
				return err
			}
			result.Deleted += int(tag.RowsAffected())
		}

		representationChanged := result.Added > 0 || result.Updated > 0 || result.Deleted > 0
		for _, channelID := range channelIDs {
			// Migration 0018 deliberately forbids even a hidden real candidate
			// from overlapping a PPV-off placeholder. Reassert this channel's
			// trigger authority and clear such placeholders before commit.
			if err := lockEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `
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
			if tag.RowsAffected() > 0 {
				result.Deleted += int(tag.RowsAffected())
				representationChanged = true
			}
			changed, suppressed, err := canonicalizeEPGChannel(ctx, tx, channelID)
			if err != nil {
				return err
			}
			result.Suppressed += suppressed
			representationChanged = representationChanged || changed
		}
		if _, err := tx.Exec(ctx, `
			UPDATE epg_source
			   SET snapshot_generation = $2,
			       snapshot_applied_at = now(),
			       last_etag = $3,
			       last_modified = $4
			 WHERE id = $1`, sourceID, result.Generation, etag, lastModified); err != nil {
			return err
		}
		if representationChanged {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
	return result, err
}

// RetireLegacyEPGRowsAfterSnapshotBootstrap removes the last recognizable
// pre-0023 XMLTV rows only after every enabled source has published at least
// one source-owned snapshot. Until that barrier is satisfied the opaque rows
// keep the guide available while providers recover from a deploy. Afterwards,
// a surviving legacy XMLTV row belongs only to a source or channel that no
// enabled snapshot still claims, so it must not remain visible indefinitely
// merely because that source was already disabled at migration time.
//
// Historical XMLTV and Schedules Direct rows both used lowercase 32-character hex
// hashes. Priorities 10 and 100 are therefore intentionally retained: SD owns
// 10, and migration 0013 initially backfilled every then-existing row to 100.
// Explicit PPV, sports, SD, and manual producers were already marked
// non-legacy by migration 0023.
func (db *DB) RetireLegacyEPGRowsAfterSnapshotBootstrap(ctx context.Context) (int64, error) {
	var deleted int64
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		var pendingEnabledSources int
		sourceRows, err := tx.Query(ctx, `
			SELECT enabled, snapshot_applied_at IS NOT NULL
			  FROM epg_source
			 ORDER BY id
			 FOR UPDATE`)
		if err != nil {
			return err
		}
		for sourceRows.Next() {
			var enabled, snapshotApplied bool
			if err := sourceRows.Scan(&enabled, &snapshotApplied); err != nil {
				sourceRows.Close()
				return err
			}
			if enabled && !snapshotApplied {
				pendingEnabledSources++
			}
		}
		if err := sourceRows.Err(); err != nil {
			sourceRows.Close()
			return err
		}
		sourceRows.Close()
		if pendingEnabledSources > 0 {
			return nil
		}

		rows, err := tx.Query(ctx, `
			SELECT DISTINCT channel_id
			  FROM epg_program
			 WHERE is_legacy
			   AND source_hash ~ '^[0-9a-f]{32}$'
			   AND source_priority >= 0
			   AND source_priority NOT IN ($1, $2)
			 ORDER BY channel_id`, PrioritySD, PriorityPPVOff)
		if err != nil {
			return err
		}
		var channelIDs []uuid.UUID
		for rows.Next() {
			var channelID uuid.UUID
			if err := rows.Scan(&channelID); err != nil {
				rows.Close()
				return err
			}
			channelIDs = append(channelIDs, channelID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, channelID := range channelIDs {
			if err := lockEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}

		tag, err := tx.Exec(ctx, `
			DELETE FROM epg_program
			 WHERE is_legacy
			   AND source_hash ~ '^[0-9a-f]{32}$'
			   AND source_priority >= 0
			   AND source_priority NOT IN ($1, $2)`, PrioritySD, PriorityPPVOff)
		if err != nil {
			return err
		}
		deleted = tag.RowsAffected()
		for _, channelID := range channelIDs {
			if _, _, err := canonicalizeEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
		}
		if deleted > 0 {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
	return deleted, err
}

// canonicalizeEPGChannel chooses complete programme rows, never clipped
// fragments. Candidate groups are ordered by source priority and stable source
// identity. Within one source, weighted interval scheduling maximizes covered
// duration, then programme count, avoiding a malformed overlapping row from
// silently punching a large hole. Lower-priority rows are admitted only when
// their complete interval fits a gap left by all preferred groups.
func canonicalizeEPGChannel(ctx context.Context, tx pgx.Tx, channelID uuid.UUID) (changed bool, suppressed int, err error) {
	rows, err := tx.Query(ctx, `
		SELECT id, start_at, end_at, source_priority,
		       CASE
		         WHEN is_legacy THEN '2:legacy'
		         WHEN epg_source_id IS NULL THEN '0:direct'
		         ELSE '1:' || epg_source_id::text
		       END,
		       source_hash, is_canonical
		  FROM epg_program
		 WHERE channel_id = $1
		 ORDER BY source_priority,
		          CASE
		            WHEN is_legacy THEN '2:legacy'
		            WHEN epg_source_id IS NULL THEN '0:direct'
		            ELSE '1:' || epg_source_id::text
		          END,
		          start_at, end_at DESC, source_hash, id`, channelID)
	if err != nil {
		return false, 0, err
	}
	var candidates []canonicalEPGCandidate
	for rows.Next() {
		var c canonicalEPGCandidate
		if err := rows.Scan(&c.ID, &c.Start, &c.End, &c.Priority, &c.SourceKey, &c.SourceHash, &c.Canonical); err != nil {
			rows.Close()
			return false, 0, err
		}
		if c.End.After(c.Start) {
			candidates = append(candidates, c)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, 0, err
	}
	rows.Close()

	selected := selectCanonicalEPGCandidates(candidates)
	selectedSet := make(map[uuid.UUID]struct{}, len(selected))
	selectedIDs := make([]uuid.UUID, 0, len(selected))
	for _, c := range selected {
		selectedSet[c.ID] = struct{}{}
		selectedIDs = append(selectedIDs, c.ID)
	}
	for _, c := range candidates {
		_, want := selectedSet[c.ID]
		if c.Canonical != want {
			changed = true
		}
		if !want {
			suppressed++
		}
	}
	if changed {
		if _, err := tx.Exec(ctx, `
			UPDATE epg_program
			   SET is_canonical = (id = ANY($2::uuid[]))
			 WHERE channel_id = $1
			   AND is_canonical IS DISTINCT FROM (id = ANY($2::uuid[]))`,
			channelID, selectedIDs); err != nil {
			return false, 0, err
		}
	}
	supplemented, err := mergeCanonicalEPGEpisodeSupplements(ctx, tx, channelID)
	return changed || supplemented, suppressed, err
}

// mergeCanonicalEPGEpisodeSupplements preserves the established Schedules
// Direct field-level backfill after XMLTV sources gained independent candidate
// rows. A same-start, metadata-compatible fallback may fill only missing
// episode identity on the canonical winner; it never changes title, timing, or
// flags. Provider-id provenance remains explicit through the supplemented flag.
// All identity fields come from one native donor so independently sourced
// numbering schemes cannot be spliced together. A donor that can supply a
// missing validated EP id outranks formal-only donors; source priority and then
// stable source identity break ties.
func mergeCanonicalEPGEpisodeSupplements(ctx context.Context, tx pgx.Tx, channelID uuid.UUID) (bool, error) {
	var outputChanged bool
	err := tx.QueryRow(ctx, `
		WITH supplements AS (
			SELECT winner.id,
			       CASE WHEN NOT winner.provider_episode_id_supplemented
			                  AND upper(btrim(winner.provider_episode_id)) ~ '^EP[0-9A-Z]+$'
			            THEN btrim(winner.provider_episode_id) END AS own_provider_episode_id,
			       donor.provider_episode_id AS supplemental_provider_episode_id,
			       CASE WHEN NOT winner.episode_num_xmltv_supplemented
			            THEN NULLIF(winner.episode_num_xmltv, '') END AS own_episode_num_xmltv,
			       donor.episode_num_xmltv AS supplemental_episode_num_xmltv,
			       CASE WHEN NOT winner.episode_num_onscreen_supplemented
			            THEN NULLIF(winner.episode_num_onscreen, '') END AS own_episode_num_onscreen,
			       donor.episode_num_onscreen AS supplemental_episode_num_onscreen
			  FROM epg_program winner
			  LEFT JOIN LATERAL (
			      SELECT CASE WHEN NOT candidate.provider_episode_id_supplemented
			                         AND upper(btrim(candidate.provider_episode_id)) ~ '^EP[0-9A-Z]+$'
			                  THEN btrim(candidate.provider_episode_id) END AS provider_episode_id,
			             CASE WHEN NOT candidate.episode_num_xmltv_supplemented
			                  THEN NULLIF(candidate.episode_num_xmltv, '') END AS episode_num_xmltv,
			             CASE WHEN NOT candidate.episode_num_onscreen_supplemented
			                  THEN NULLIF(candidate.episode_num_onscreen, '') END AS episode_num_onscreen
			        FROM epg_program candidate
			       WHERE candidate.channel_id = winner.channel_id
			         AND candidate.start_at = winner.start_at
			         AND candidate.id <> winner.id
			         AND candidate.is_movie = winner.is_movie
			         AND lower(regexp_replace(btrim(candidate.title), '[[:space:]]+', ' ', 'g')) =
			             lower(regexp_replace(btrim(winner.title), '[[:space:]]+', ' ', 'g'))
			         AND (
			              NULLIF(btrim(candidate.sub_title), '') IS NULL
			              OR NULLIF(btrim(winner.sub_title), '') IS NULL
			              OR lower(regexp_replace(btrim(candidate.sub_title), '[[:space:]]+', ' ', 'g')) =
			                 lower(regexp_replace(btrim(winner.sub_title), '[[:space:]]+', ' ', 'g'))
			         )
			         AND (
			              candidate.original_air_date IS NULL
			              OR winner.original_air_date IS NULL
			              OR candidate.original_air_date = winner.original_air_date
			         )
			         AND (
			              winner.provider_episode_id_supplemented
			              OR upper(btrim(winner.provider_episode_id)) !~ '^EP[0-9A-Z]+$'
			              OR candidate.provider_episode_id_supplemented
			              OR upper(btrim(candidate.provider_episode_id)) !~ '^EP[0-9A-Z]+$'
			              OR upper(btrim(candidate.provider_episode_id)) =
			                 upper(btrim(winner.provider_episode_id))
			         )
			         AND (
			              winner.episode_num_xmltv = ''
			              OR winner.episode_num_xmltv_supplemented
			              OR (candidate.episode_num_xmltv <> ''
			                  AND NOT candidate.episode_num_xmltv_supplemented
			                  AND candidate.episode_num_xmltv = winner.episode_num_xmltv)
			         )
			         AND (
			              winner.episode_num_onscreen = ''
			              OR winner.episode_num_onscreen_supplemented
			              OR (candidate.episode_num_onscreen <> ''
			                  AND NOT candidate.episode_num_onscreen_supplemented
			                  AND candidate.episode_num_onscreen = winner.episode_num_onscreen)
			         )
			         AND (
			              ((winner.episode_num_xmltv = '' OR winner.episode_num_xmltv_supplemented)
			               AND candidate.episode_num_xmltv <> ''
			               AND NOT candidate.episode_num_xmltv_supplemented)
			              OR
			              ((winner.episode_num_onscreen = '' OR winner.episode_num_onscreen_supplemented)
			               AND candidate.episode_num_onscreen <> ''
			               AND NOT candidate.episode_num_onscreen_supplemented)
			              OR
			              ((winner.provider_episode_id = '' OR winner.provider_episode_id_supplemented
			                OR upper(btrim(winner.provider_episode_id)) !~ '^EP[0-9A-Z]+$')
			               AND upper(btrim(candidate.provider_episode_id)) ~ '^EP[0-9A-Z]+$'
			               AND NOT candidate.provider_episode_id_supplemented)
			         )
			       ORDER BY CASE
			                  WHEN (winner.provider_episode_id = '' OR winner.provider_episode_id_supplemented
			                        OR upper(btrim(winner.provider_episode_id)) !~ '^EP[0-9A-Z]+$')
			                   AND upper(btrim(candidate.provider_episode_id)) ~ '^EP[0-9A-Z]+$'
			                   AND NOT candidate.provider_episode_id_supplemented
			                  THEN 0 ELSE 1
			                END,
			                candidate.source_priority,
			                CASE
			                  WHEN candidate.is_legacy THEN '2:legacy'
			                  WHEN candidate.epg_source_id IS NULL THEN '0:direct'
			                  ELSE '1:' || candidate.epg_source_id::text
			                END,
			                candidate.source_hash, candidate.id
			       LIMIT 1
			  ) donor ON true
			 WHERE winner.channel_id = $1
			   AND winner.is_canonical
		), desired AS (
			SELECT id,
			       COALESCE(own_provider_episode_id, supplemental_provider_episode_id, '') AS provider_episode_id,
			       own_provider_episode_id IS NULL AND supplemental_provider_episode_id IS NOT NULL
			           AS provider_episode_id_supplemented,
			       COALESCE(own_episode_num_xmltv, supplemental_episode_num_xmltv, '') AS episode_num_xmltv,
			       own_episode_num_xmltv IS NULL AND supplemental_episode_num_xmltv IS NOT NULL
			           AS episode_num_xmltv_supplemented,
			       COALESCE(own_episode_num_onscreen, supplemental_episode_num_onscreen, '') AS episode_num_onscreen,
			       own_episode_num_onscreen IS NULL AND supplemental_episode_num_onscreen IS NOT NULL
			           AS episode_num_onscreen_supplemented
			  FROM supplements
		), changes AS (
			SELECT desired.*,
			       winner.provider_episode_id IS DISTINCT FROM desired.provider_episode_id
			        OR winner.episode_num_xmltv IS DISTINCT FROM desired.episode_num_xmltv
			        OR winner.episode_num_onscreen IS DISTINCT FROM desired.episode_num_onscreen
			           AS output_changed,
			       winner.episode_num_xmltv IS DISTINCT FROM desired.episode_num_xmltv
			        OR winner.episode_num_onscreen IS DISTINCT FROM desired.episode_num_onscreen
			           AS enrichment_input_changed
			  FROM desired
			  JOIN epg_program winner ON winner.id = desired.id
			 WHERE winner.provider_episode_id IS DISTINCT FROM desired.provider_episode_id
			    OR winner.provider_episode_id_supplemented IS DISTINCT FROM desired.provider_episode_id_supplemented
			    OR winner.episode_num_xmltv IS DISTINCT FROM desired.episode_num_xmltv
			    OR winner.episode_num_onscreen IS DISTINCT FROM desired.episode_num_onscreen
			    OR winner.episode_num_xmltv_supplemented IS DISTINCT FROM desired.episode_num_xmltv_supplemented
			    OR winner.episode_num_onscreen_supplemented IS DISTINCT FROM desired.episode_num_onscreen_supplemented
		), updated AS (
			UPDATE epg_program winner
			   SET provider_episode_id = changes.provider_episode_id,
			       provider_episode_id_supplemented = changes.provider_episode_id_supplemented,
			       episode_num_xmltv = changes.episode_num_xmltv,
			       episode_num_xmltv_supplemented = changes.episode_num_xmltv_supplemented,
			       episode_num_onscreen = changes.episode_num_onscreen,
			       episode_num_onscreen_supplemented = changes.episode_num_onscreen_supplemented,
			       enrichment_status = CASE WHEN changes.enrichment_input_changed THEN 'pending' ELSE winner.enrichment_status END,
			       enrichment_retry_at = CASE WHEN changes.enrichment_input_changed THEN '-infinity'::timestamptz ELSE winner.enrichment_retry_at END,
			       updated_at = CASE WHEN changes.output_changed THEN now() ELSE winner.updated_at END
			  FROM changes
			 WHERE winner.id = changes.id
			 RETURNING changes.output_changed
		)
		SELECT COALESCE(bool_or(output_changed), false) FROM updated`, channelID).Scan(&outputChanged)
	if err != nil {
		return false, err
	}
	return outputChanged, nil
}

func selectCanonicalEPGCandidates(in []canonicalEPGCandidate) []canonicalEPGCandidate {
	if len(in) == 0 {
		return nil
	}
	ordered := append([]canonicalEPGCandidate(nil), in...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Priority != ordered[j].Priority {
			return ordered[i].Priority < ordered[j].Priority
		}
		if ordered[i].SourceKey != ordered[j].SourceKey {
			return ordered[i].SourceKey < ordered[j].SourceKey
		}
		if !ordered[i].Start.Equal(ordered[j].Start) {
			return ordered[i].Start.Before(ordered[j].Start)
		}
		if !ordered[i].End.Equal(ordered[j].End) {
			return ordered[i].End.After(ordered[j].End)
		}
		if ordered[i].SourceHash != ordered[j].SourceHash {
			return ordered[i].SourceHash < ordered[j].SourceHash
		}
		return ordered[i].ID.String() < ordered[j].ID.String()
	})
	groups := make([][]canonicalEPGCandidate, 0)
	for i := 0; i < len(ordered); {
		j := i + 1
		for j < len(ordered) && ordered[j].Priority == ordered[i].Priority && ordered[j].SourceKey == ordered[i].SourceKey {
			j++
		}
		groups = append(groups, append([]canonicalEPGCandidate(nil), ordered[i:j]...))
		i = j
	}

	var accepted []canonicalEPGCandidate
	for _, group := range groups {
		eligible := make([]canonicalEPGCandidate, 0, len(group))
		for _, candidate := range group {
			if !overlapsCanonical(candidate, accepted) {
				eligible = append(eligible, candidate)
			}
		}
		// Resolve only among rows that fit gaps left by preferred groups. If a
		// long malformed fallback overlaps a preferred row, selecting it first
		// and rejecting it afterward could also discard several valid fallback
		// rows and silently turn their complete gap into a guide hole.
		accepted = append(accepted, maximumCoverageEPGSet(eligible)...)
		sort.Slice(accepted, func(i, j int) bool {
			if accepted[i].Start.Equal(accepted[j].Start) {
				return accepted[i].ID.String() < accepted[j].ID.String()
			}
			return accepted[i].Start.Before(accepted[j].Start)
		})
	}
	return accepted
}

type epgCoverageChoice struct {
	Duration int64
	Count    int
	Rows     []canonicalEPGCandidate
}

func maximumCoverageEPGSet(group []canonicalEPGCandidate) []canonicalEPGCandidate {
	sort.Slice(group, func(i, j int) bool {
		if group[i].End.Equal(group[j].End) {
			if group[i].Start.Equal(group[j].Start) {
				if group[i].SourceHash == group[j].SourceHash {
					return group[i].ID.String() < group[j].ID.String()
				}
				return group[i].SourceHash < group[j].SourceHash
			}
			return group[i].Start.Before(group[j].Start)
		}
		return group[i].End.Before(group[j].End)
	})
	dp := make([]epgCoverageChoice, len(group)+1)
	for i := 1; i <= len(group); i++ {
		candidate := group[i-1]
		prior := sort.Search(i-1, func(j int) bool {
			return group[j].End.After(candidate.Start)
		})
		include := epgCoverageChoice{
			Duration: dp[prior].Duration + candidate.End.Sub(candidate.Start).Nanoseconds(),
			Count:    dp[prior].Count + 1,
			Rows:     append(append([]canonicalEPGCandidate(nil), dp[prior].Rows...), candidate),
		}
		exclude := dp[i-1]
		if betterEPGCoverage(include, exclude) {
			dp[i] = include
		} else {
			dp[i] = exclude
		}
	}
	return dp[len(group)].Rows
}

func betterEPGCoverage(a, b epgCoverageChoice) bool {
	if a.Duration != b.Duration {
		return a.Duration > b.Duration
	}
	if a.Count != b.Count {
		return a.Count > b.Count
	}
	for i := 0; i < len(a.Rows) && i < len(b.Rows); i++ {
		if !a.Rows[i].Start.Equal(b.Rows[i].Start) {
			return a.Rows[i].Start.Before(b.Rows[i].Start)
		}
		if a.Rows[i].SourceHash != b.Rows[i].SourceHash {
			return a.Rows[i].SourceHash < b.Rows[i].SourceHash
		}
		if a.Rows[i].ID != b.Rows[i].ID {
			return a.Rows[i].ID.String() < b.Rows[i].ID.String()
		}
	}
	return false
}

func overlapsCanonical(candidate canonicalEPGCandidate, accepted []canonicalEPGCandidate) bool {
	for _, current := range accepted {
		if !current.Start.Before(candidate.End) {
			return false
		}
		if current.End.After(candidate.Start) {
			return true
		}
	}
	return false
}

func bumpEPGRevision(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `UPDATE epg_revision SET updated_at = clock_timestamp() WHERE singleton`)
	return err
}

func finalizeEPGChannelMutation(ctx context.Context, tx pgx.Tx, channelID uuid.UUID, mutated bool) error {
	canonicalChanged, _, err := canonicalizeEPGChannel(ctx, tx, channelID)
	if err != nil {
		return err
	}
	if mutated || canonicalChanged {
		return bumpEPGRevision(ctx, tx)
	}
	return nil
}

const (
	epgChannelLockNamespace int32 = 0x45504731 // "EPG1"
	ppvPassLockKey          int32 = 0x50505631 // "PPV1"
	sdPassLockKey           int32 = 0x53445031 // "SDP1"
)

// AcquirePPVSyncLease serialises the complete fetch-to-commit pass across
// rolling workers. The provider does not expose a catalogue revision, so this
// session lock is what gives fetch timestamps a real ordering: the next worker
// cannot fetch until the prior worker has finished publishing its snapshot.
// Per-channel locks still protect ppvsync from concurrent guide writers.
func (db *DB) AcquirePPVSyncLease(ctx context.Context) (func() error, error) {
	return db.acquireEPGPassLease(ctx, ppvPassLockKey, "ppvsync")
}

// AcquireSDIngestLease serialises the complete Schedules Direct pass,
// including provider fetches, candidate publication, and station MD5 commits.
// Source creation and UpdateChannel join the same process-local admission gate
// before taking the transaction-scoped form of this lock. Consequently local
// waiters consume no pool connection while a pass is active; the advisory lock
// retains cross-process ordering for rolling workers.
func (db *DB) AcquireSDIngestLease(ctx context.Context) (func() error, error) {
	releaseAuthority, err := db.acquireSDAuthority(ctx)
	if err != nil {
		return nil, err
	}
	releaseLease, err := db.acquireEPGPassLease(ctx, sdPassLockKey, "Schedules Direct")
	if err != nil {
		releaseAuthority()
		return nil, err
	}
	var once sync.Once
	var releaseErr error
	return func() error {
		once.Do(func() {
			releaseErr = releaseLease()
			releaseAuthority()
		})
		return releaseErr
	}, nil
}

func (db *DB) acquireEPGPassLease(ctx context.Context, key int32, owner string) (func() error, error) {
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, key); err != nil {
		conn.Release()
		return nil, err
	}

	var once sync.Once
	var releaseErr error
	release := func() error {
		once.Do(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var unlocked bool
			if err := conn.QueryRow(closeCtx, `SELECT pg_advisory_unlock($1)`, key).Scan(&unlocked); err != nil || !unlocked {
				if err == nil {
					err = fmt.Errorf("%s pass advisory lock was not held", owner)
				}
				raw := conn.Hijack()
				releaseErr = errors.Join(err, raw.Close(closeCtx))
				return
			}
			conn.Release()
		})
		return releaseErr
	}
	return release, nil
}

func lockSDIngestPass(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, sdPassLockKey)
	return err
}

// lockEPGChannel serialises every application EPG mutation for one channel.
// A collision in the 32-bit UUID shard only over-serialises unrelated
// channels; it cannot weaken correctness. PPV off-grid reconciliation relies
// on this lock so its final interval-occupancy read remains true through the
// transaction's inserts.
func lockEPGChannel(ctx context.Context, tx pgx.Tx, channelID uuid.UUID) error {
	shard := int32(binary.BigEndian.Uint32(channelID[:4]))
	_, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock($1, $2),
		       set_config('conductor.epg_channel_lock', $3, true)`,
		epgChannelLockNamespace, shard, channelID.String())
	return err
}

// claimPPVObservation advances the per-channel provider snapshot high-water
// mark. Equal timestamps are accepted so one channel reconciliation may use
// several small transactions; an older rolling-deploy worker is rejected
// before it can replace, extend, delete, or publish placeholders.
func claimPPVObservation(ctx context.Context, tx pgx.Tx, channelID uuid.UUID, observedAt time.Time) (bool, error) {
	if observedAt.IsZero() {
		return false, errors.New("ppv observation timestamp is zero")
	}
	var accepted bool
	err := tx.QueryRow(ctx, `
		INSERT INTO ppv_sync_channel_state (channel_id, observed_at)
		VALUES ($1, $2)
		ON CONFLICT (channel_id) DO UPDATE
		   SET observed_at = EXCLUDED.observed_at,
		       updated_at = now()
		 WHERE ppv_sync_channel_state.observed_at <= EXCLUDED.observed_at
		RETURNING true`, channelID, observedAt).Scan(&accepted)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return accepted, err
}

// OccupiedProgramWindows returns the [start_at, end_at) ranges of "real"
// programme rows on a channel that overlap [from, to). "Real" means
// source_priority < PriorityPPVOff — i.e. anything that isn't a "No Event
// Scheduled" placeholder: manually-inserted operator rows, sports/SD/XMLTV
// rows, and live PPV event rows all qualify. This intentionally considers
// suppressed candidates too: migration 0018's stronger database fence rejects
// a PPV-off overlap with *any* real candidate, and a rolling old binary may
// insert a row with the new is_canonical default before the resolver runs.
// ppvsync uses this to avoid tiling placeholder blocks over an existing row it
// didn't write (the 2026-07-11 incident where 8K off-tiles buried the
// operator's manual UFC 329 rows).
func (db *DB) OccupiedProgramWindows(ctx context.Context, channelID uuid.UUID, from, to time.Time) ([]TimeRange, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT start_at, end_at
		  FROM epg_program
		 WHERE channel_id = $1
		   AND source_priority < $2
		   AND start_at < $4 AND end_at > $3
		 ORDER BY start_at`,
		channelID, PriorityPPVOff, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TimeRange
	for rows.Next() {
		var r TimeRange
		if err := rows.Scan(&r.Start, &r.End); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ReconcilePPVOffRows atomically performs the final occupancy check, removes
// stale/overlapping ppv-off rows, and publishes the surviving placeholder
// grid. Every application EPG writer takes the same per-channel advisory lock,
// so a real event cannot slip between the interval read and placeholder
// inserts. observedAt also rejects an older rolling-deploy snapshot.
func (db *DB) ReconcilePPVOffRows(
	ctx context.Context,
	channelID uuid.UUID,
	gridStart, windowEnd time.Time,
	evStart, evEnd *time.Time,
	rows []EPGProgram,
	observedAt time.Time,
) (written int, applied bool, err error) {
	if !windowEnd.After(gridStart) {
		return 0, false, errors.New("ppv off-grid window is invalid")
	}
	if (evStart == nil) != (evEnd == nil) {
		return 0, false, errors.New("ppv event window is incomplete")
	}
	for _, row := range rows {
		if row.ChannelID != channelID || row.SourcePriority != PriorityPPVOff ||
			!strings.HasPrefix(row.SourceHash, "ppv-off:") ||
			!row.EndAt.After(row.StartAt) || row.StartAt.Before(gridStart) || row.EndAt.After(windowEnd) {
			return 0, false, errors.New("invalid ppv off-grid row")
		}
	}

	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, channelID); err != nil {
			return err
		}
		var claimErr error
		applied, claimErr = claimPPVObservation(ctx, tx, channelID, observedAt)
		if claimErr != nil || !applied {
			return claimErr
		}

		occupiedRows, queryErr := tx.Query(ctx, `
			SELECT start_at, end_at
			  FROM epg_program
			 WHERE channel_id = $1
			   AND source_priority < $2
			   AND start_at < $4 AND end_at > $3
			 ORDER BY start_at`,
			channelID, PriorityPPVOff, gridStart, windowEnd)
		if queryErr != nil {
			return queryErr
		}
		var occupied []TimeRange
		for occupiedRows.Next() {
			var r TimeRange
			if err := occupiedRows.Scan(&r.Start, &r.End); err != nil {
				occupiedRows.Close()
				return err
			}
			occupied = append(occupied, r)
		}
		queryErr = occupiedRows.Err()
		occupiedRows.Close()
		if queryErr != nil {
			return queryErr
		}

		// Full-set reconciliation: the computed rows are authoritative for
		// the window, so any existing off row whose start is not among them
		// is stale (an aged block, or a trimmed post-event remainder whose
		// event moved or vanished — without this, a vanished event left its
		// old off-grid tail overlapping the next pass's full block).
		incomingStarts := make([]time.Time, 0, len(rows))
		for _, row := range rows {
			incomingStarts = append(incomingStarts, row.StartAt)
		}
		if _, execErr := tx.Exec(ctx, `
			DELETE FROM epg_program AS off_row
			 WHERE off_row.channel_id = $1
			   AND off_row.source_hash LIKE 'ppv-off:%'
			   AND (off_row.start_at < $2
			        OR ($3::timestamptz IS NOT NULL
			            AND off_row.start_at < $4 AND off_row.end_at > $3)
			        OR (off_row.start_at >= $2 AND off_row.start_at < $6
			            AND off_row.start_at <> ALL($7::timestamptz[]))
			        OR EXISTS (
			            SELECT 1
			              FROM epg_program AS real_row
			             WHERE real_row.channel_id = off_row.channel_id
			               AND real_row.source_priority < $5
			               AND real_row.start_at < off_row.end_at
			               AND real_row.end_at > off_row.start_at
			        ))`,
			channelID, gridStart, evStart, evEnd, PriorityPPVOff, windowEnd, incomingStarts); execErr != nil {
			return execErr
		}

		for _, row := range rows {
			overlaps := false
			for _, real := range occupied {
				if row.StartAt.Before(real.End) && row.EndAt.After(real.Start) {
					overlaps = true
					break
				}
			}
			if overlaps {
				continue
			}
			action, upsertErr := upsertEPGProgram(ctx, tx, row)
			if upsertErr != nil {
				return upsertErr
			}
			if action == "added" || action == "updated" {
				written++
			}
		}
		return finalizeEPGChannelMutation(ctx, tx, channelID, written > 0)
	})
	return written, applied, err
}

// ExtendPPVLiveRowEnd pushes the end_at of a live ppv-parse row out to newEnd,
// but only when its current end_at is before triggerBefore — i.e. the event is
// within its last stretch and would otherwise drop off the guide while still
// airing. The complete source_hash (which includes the title) must still match
// the selected winner, so a stale worker cannot extend a same-slot rename
// committed after replacement. hardEnd is the finite nominal-stop allowance;
// neither this path nor repeated calls can extend beyond it. Returns rows
// affected (0 = nothing needed extending).
func (db *DB) ExtendPPVLiveRowEnd(ctx context.Context, channelID uuid.UUID, startAt time.Time, sourceHash string, observedAt, triggerBefore, newEnd, hardEnd time.Time) (extended int64, applied bool, err error) {
	if !strings.HasPrefix(sourceHash, "ppv-parse:") {
		return 0, false, errors.New("live-extension source hash is not ppv-parse")
	}
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, channelID); err != nil {
			return err
		}
		var claimErr error
		applied, claimErr = claimPPVObservation(ctx, tx, channelID, observedAt)
		if claimErr != nil || !applied {
			return claimErr
		}
		tag, execErr := tx.Exec(ctx, `
			UPDATE epg_program
			   SET end_at = LEAST($5, $6),
			       is_canonical = false
			 WHERE channel_id = $1
			   AND start_at = $2
			   AND source_hash = $3
			   AND epg_source_id IS NULL
			   AND source_priority = $7
			   AND end_at < $4
			   AND $5 > end_at
			   AND $6 > end_at`,
			channelID, startAt, sourceHash, triggerBefore, newEnd, hardEnd, PriorityPPVSync)
		if execErr != nil {
			return execErr
		}
		extended = tag.RowsAffected()
		return finalizeEPGChannelMutation(ctx, tx, channelID, extended > 0)
	})
	return extended, applied, err
}

// ActivePPVLiveOverrunEnd returns the stored end of an exact parsed event only
// while that row is still active. ppvsync calls this after the unchanged source
// name's nominal window has elapsed: an end later than activeAt proves a prior
// live pass extended the row. Exact source_hash matching prevents a renamed,
// rescheduled, or explicitly-ended provider state from borrowing old liveness.
// hardEnd caps both the returned window and how long unchanged provider text
// remains eligible for continuity.
func (db *DB) ActivePPVLiveOverrunEnd(ctx context.Context, channelID uuid.UUID, startAt time.Time, sourceHash string, activeAt, hardEnd time.Time) (time.Time, bool, error) {
	if !strings.HasPrefix(sourceHash, "ppv-parse:") {
		return time.Time{}, false, errors.New("live-overrun source hash is not ppv-parse")
	}
	if !hardEnd.After(startAt) || !activeAt.Before(hardEnd) {
		return time.Time{}, false, nil
	}
	var endAt time.Time
	err := db.Pool.QueryRow(ctx, `
		SELECT LEAST(end_at, $6)
		  FROM epg_program
		 WHERE channel_id = $1
		   AND start_at = $2
		   AND source_hash = $3
		   AND epg_source_id IS NULL
		   AND source_priority = $4
		   AND end_at > $5`,
		channelID, startAt, sourceHash, PriorityPPVSync, activeAt, hardEnd).Scan(&endAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return endAt, true, nil
}

// PreservePPVLiveOverrunForChannel atomically keeps an exact, still-active
// overrun row as the channel winner, refreshes it only when it nears expiry,
// and removes redundant parsed siblings. It never inserts: if the row expired,
// reached hardEnd, or its identity changed between the read-only probe and this
// transaction, kept=false prevents stale state from being resurrected. A row
// produced by the prior unbounded implementation is clamped to hardEnd here.
func (db *DB) PreservePPVLiveOverrunForChannel(
	ctx context.Context,
	channelID uuid.UUID,
	startAt time.Time,
	sourceHash string,
	observedAt time.Time,
	activeAt time.Time,
	hardEnd time.Time,
	triggerBefore time.Time,
	newEnd time.Time,
) (endAt time.Time, deleted int64, kept bool, err error) {
	if !strings.HasPrefix(sourceHash, "ppv-parse:") {
		return time.Time{}, 0, false, errors.New("live-overrun source hash is not ppv-parse")
	}
	if !hardEnd.After(startAt) || !activeAt.Before(hardEnd) {
		return time.Time{}, 0, false, nil
	}
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, channelID); err != nil {
			return err
		}
		applied, claimErr := claimPPVObservation(ctx, tx, channelID, observedAt)
		if claimErr != nil || !applied {
			return claimErr
		}
		queryErr := tx.QueryRow(ctx, `
			SELECT end_at
			  FROM epg_program
			 WHERE channel_id = $1
			   AND start_at = $2
			   AND source_hash = $3
			   AND epg_source_id IS NULL
			   AND source_priority = $4
			   AND end_at > $5
			 FOR UPDATE`,
			channelID, startAt, sourceHash, PriorityPPVSync, activeAt).Scan(&endAt)
		if errors.Is(queryErr, pgx.ErrNoRows) {
			return nil
		}
		if queryErr != nil {
			return queryErr
		}
		kept = true
		targetEnd := endAt
		if targetEnd.After(hardEnd) {
			targetEnd = hardEnd
		}
		if targetEnd.Before(triggerBefore) && newEnd.After(targetEnd) {
			targetEnd = newEnd
			if targetEnd.After(hardEnd) {
				targetEnd = hardEnd
			}
		}
		if !targetEnd.Equal(endAt) {
			if err := tx.QueryRow(ctx, `
				UPDATE epg_program
				   SET end_at = $4,
				       is_canonical = false
				 WHERE channel_id = $1
				   AND start_at = $2
				   AND source_hash = $3
				   AND epg_source_id IS NULL
				 RETURNING end_at`,
				channelID, startAt, sourceHash, targetEnd).Scan(&endAt); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx, `
			DELETE FROM epg_program
			 WHERE channel_id = $1
			   AND source_hash LIKE 'ppv-parse:%'
			   AND NOT (start_at = $2 AND source_hash = $3)`,
			channelID, startAt, sourceHash)
		if err != nil {
			return err
		}
		deleted = tag.RowsAffected()
		return finalizeEPGChannelMutation(ctx, tx, channelID, deleted > 0 || kept)
	})
	return endAt, deleted, kept, err
}

// ReplacePPVParsedRowsForChannel atomically installs the one event selected
// from a channel's redundant provider sources and removes every other
// ppv-parse row on that channel. Readers therefore see either the previous
// complete state or the new complete state, never the delete-before-insert
// gap that can otherwise make a live-TV guide tile briefly disappear.
func (db *DB) ReplacePPVParsedRowsForChannel(ctx context.Context, p EPGProgram, observedAt time.Time) (action string, deleted int64, applied bool, err error) {
	if !strings.HasPrefix(p.SourceHash, "ppv-parse:") {
		return "", 0, false, errors.New("replacement event source hash is not ppv-parse")
	}

	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, p.ChannelID); err != nil {
			return err
		}
		var claimErr error
		applied, claimErr = claimPPVObservation(ctx, tx, p.ChannelID, observedAt)
		if claimErr != nil || !applied {
			return claimErr
		}
		var upsertErr error
		action, upsertErr = upsertEPGProgram(ctx, tx, p)
		if upsertErr != nil {
			return upsertErr
		}

		// An equal-or-stronger non-PPV row may own this start time. Do not
		// delete the old PPV state or advertise a pre-event title unless the
		// selected exact event actually became the stored kickoff row.
		var selectedStored bool
		if err := tx.QueryRow(ctx, `
			SELECT source_hash = $3
			  FROM epg_program
			 WHERE channel_id = $1
			   AND start_at = $2
			   AND epg_source_id IS NULL`,
			p.ChannelID, p.StartAt, p.SourceHash).Scan(&selectedStored); err != nil {
			return err
		}
		if !selectedStored {
			return errors.New("selected PPV event did not win its guide slot")
		}

		// Publish the exact event and clear any overlapping placeholder in the
		// same channel-locked transaction. Without this, an older off-grid writer
		// that committed first could leave a six-hour placeholder spanning the
		// newly committed event until a later cleanup call.
		if _, err := tx.Exec(ctx, `
			DELETE FROM epg_program
			 WHERE channel_id = $1
			   AND source_hash LIKE 'ppv-off:%'
			   AND start_at < $3 AND end_at > $2`,
			p.ChannelID, p.StartAt, p.EndAt); err != nil {
			return err
		}

		tag, err := tx.Exec(ctx, `
			DELETE FROM epg_program
			 WHERE channel_id = $1
			   AND source_hash LIKE 'ppv-parse:%'
			   AND NOT (start_at = $2 AND source_hash = $3)`,
			p.ChannelID, p.StartAt, p.SourceHash)
		if err != nil {
			return err
		}
		deleted = tag.RowsAffected()
		return finalizeEPGChannelMutation(ctx, tx, p.ChannelID, action != "unchanged" || deleted > 0)
	})
	return action, deleted, applied, err
}

// UpsertEPGProgram inserts or updates one direct-writer program row, keyed by
// the partial unique (channel_id, start_at) constraint for rows without an EPG
// source. Content fields are replaced only when the writer's priority is at
// least as strong (numerically <=) AND the source_hash differs — equal-or-
// better priority with new content wins; a weaker source can never clobber a
// stronger one's title/metadata. The deliberate exception is an unattributed
// pre-upgrade legacy row: the first explicit direct producer claims and
// replaces that whole slot regardless of the ambiguous historical priority.
//
// Episode numbers follow the direct slot owner just like the other metadata.
// A collapsed weaker donor cannot be corrected or withdrawn safely because its
// row no longer exists. Source-backed XMLTV and the retained direct SD candidate
// receive provenance-aware exact-slot supplementation after canonical selection.
//
// Updates that change match-driving fields (title, sub-title, movie flag/year),
// or that change episode identity, reset enrichment_status so the enricher
// re-matches; other metadata-only updates keep the existing match.
// Returns: "added" | "updated" | "unchanged".
func (db *DB) UpsertEPGProgram(ctx context.Context, p EPGProgram) (string, error) {
	// Canonical interval selection is channel-wide. Every direct writer takes
	// the same authority as PPV reconciliation so candidate publication and the
	// visible winner change are atomic.
	return db.upsertEPGProgramSerialized(ctx, p)
}

func requiresSerializedEPGRetry(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "55000" ||
		(pgErr.Code == "23P01" && pgErr.ConstraintName == "epg_program_no_ppv_off_real_overlap")
}

func (db *DB) upsertEPGProgramSerialized(ctx context.Context, p EPGProgram) (string, error) {
	var action string
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, p.ChannelID); err != nil {
			return err
		}
		var err error
		action, err = upsertEPGProgram(ctx, tx, p)
		if err != nil {
			return err
		}
		// Any real guide row owns its entire interval. Remove weaker PPV
		// placeholders while the channel lock is still held so a concurrent
		// off-grid transaction cannot leave an overlap in either commit order.
		deleted := int64(0)
		if p.SourcePriority < PriorityPPVOff {
			var tag pgconn.CommandTag
			tag, err = tx.Exec(ctx, `
				DELETE FROM epg_program AS off_row
				 WHERE off_row.channel_id = $1
				   AND off_row.source_hash LIKE 'ppv-off:%'
				   AND EXISTS (
				       SELECT 1 FROM epg_program AS real_row
				        WHERE real_row.channel_id = off_row.channel_id
				          AND real_row.source_priority < $2
				          AND real_row.start_at < off_row.end_at
				          AND real_row.end_at > off_row.start_at
				   )`, p.ChannelID, PriorityPPVOff)
			deleted = tag.RowsAffected()
		}
		if err != nil {
			return err
		}
		canonicalChanged, _, err := canonicalizeEPGChannel(ctx, tx, p.ChannelID)
		if err != nil {
			return err
		}
		if action != "unchanged" || deleted > 0 || canonicalChanged {
			return bumpEPGRevision(ctx, tx)
		}
		return nil
	})
	return action, err
}

type epgQueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type epgQueryExecer interface {
	epgQueryRower
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func upsertEPGProgram(ctx context.Context, q epgQueryExecer, p EPGProgram) (string, error) {
	var previousSourceHash string
	err := q.QueryRow(ctx, `
		SELECT source_hash
		  FROM epg_program
		 WHERE channel_id = $1 AND start_at = $2 AND epg_source_id IS NULL`,
		p.ChannelID, p.StartAt).Scan(&previousSourceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		previousSourceHash = ""
	} else if err != nil {
		return "", err
	}

	var inserted bool
	var durableSourceHash string
	err = q.QueryRow(ctx, `
		INSERT INTO epg_program (
		    channel_id, start_at, end_at, title, sub_title, description,
		    category, episode_num_xmltv, episode_num_onscreen,
		    episode_num_xmltv_supplemented, episode_num_onscreen_supplemented,
		    original_air_date, is_movie, is_live, is_new, is_premiere,
		    is_finale, rating, source_poster_url, source_hash, source_priority,
		    enrichment_status, is_canonical, is_legacy
		)
		VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, '{}'::text[]), $8, $9, false, false, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, 'pending', false, false)
		ON CONFLICT (channel_id, start_at) WHERE epg_source_id IS NULL DO UPDATE
		   SET end_at               = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.end_at      ELSE epg_program.end_at      END,
		       title                = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.title       ELSE epg_program.title       END,
		       sub_title            = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.sub_title   ELSE epg_program.sub_title   END,
		       description          = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.description ELSE epg_program.description END,
		       category             = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.category    ELSE epg_program.category    END,
		       episode_num_xmltv    = CASE
		           WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN
		               CASE WHEN EXCLUDED.episode_num_xmltv <> '' THEN EXCLUDED.episode_num_xmltv
		                    WHEN epg_program.episode_num_xmltv_supplemented THEN epg_program.episode_num_xmltv
		                    ELSE '' END
		           ELSE epg_program.episode_num_xmltv
		       END,
		       episode_num_onscreen = CASE
		           WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN
		               CASE WHEN EXCLUDED.episode_num_onscreen <> '' THEN EXCLUDED.episode_num_onscreen
		                    WHEN epg_program.episode_num_onscreen_supplemented THEN epg_program.episode_num_onscreen
		                    ELSE '' END
		           ELSE epg_program.episode_num_onscreen
		       END,
		       episode_num_xmltv_supplemented = CASE
		           WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority
		           THEN EXCLUDED.episode_num_xmltv = '' AND epg_program.episode_num_xmltv_supplemented
		           ELSE epg_program.episode_num_xmltv_supplemented
		       END,
		       episode_num_onscreen_supplemented = CASE
		           WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority
		           THEN EXCLUDED.episode_num_onscreen = '' AND epg_program.episode_num_onscreen_supplemented
		           ELSE epg_program.episode_num_onscreen_supplemented
		       END,
		       original_air_date    = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.original_air_date ELSE epg_program.original_air_date END,
		       is_movie             = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.is_movie    ELSE epg_program.is_movie    END,
		       is_live              = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.is_live     ELSE epg_program.is_live     END,
		       is_new               = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.is_new      ELSE epg_program.is_new      END,
		       is_premiere          = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.is_premiere ELSE epg_program.is_premiere END,
		       is_finale            = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.is_finale   ELSE epg_program.is_finale   END,
		       rating               = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.rating      ELSE epg_program.rating      END,
		       source_poster_url    = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.source_poster_url ELSE epg_program.source_poster_url END,
		       generated_poster_url = CASE
		           WHEN epg_program.is_legacy
		                OR (EXCLUDED.source_priority <= epg_program.source_priority
		                AND (epg_program.title     IS DISTINCT FROM EXCLUDED.title
		                  OR epg_program.sub_title IS DISTINCT FROM EXCLUDED.sub_title
		                  OR epg_program.is_movie  IS DISTINCT FROM EXCLUDED.is_movie
		                  OR epg_program.original_air_date IS DISTINCT FROM EXCLUDED.original_air_date))
		           THEN '' ELSE epg_program.generated_poster_url
		       END,
		       source_hash          = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.source_hash ELSE epg_program.source_hash END,
		       source_priority      = CASE WHEN epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority THEN EXCLUDED.source_priority ELSE epg_program.source_priority END,
		       is_canonical         = false,
		       is_legacy            = false,
		       updated_at           = now(),
		       enrichment_retry_at  = CASE
		           WHEN epg_program.is_legacy
		                OR (EXCLUDED.source_priority <= epg_program.source_priority
		                AND (epg_program.title     IS DISTINCT FROM EXCLUDED.title
		                  OR epg_program.sub_title IS DISTINCT FROM EXCLUDED.sub_title
		                  OR epg_program.is_movie  IS DISTINCT FROM EXCLUDED.is_movie
		                  OR epg_program.original_air_date IS DISTINCT FROM EXCLUDED.original_air_date))
		           THEN '-infinity'::timestamptz
		           WHEN (epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority)
		            AND (epg_program.episode_num_xmltv IS DISTINCT FROM
		                    CASE WHEN EXCLUDED.episode_num_xmltv <> '' THEN EXCLUDED.episode_num_xmltv
		                         WHEN epg_program.episode_num_xmltv_supplemented THEN epg_program.episode_num_xmltv
		                         ELSE '' END
		              OR epg_program.episode_num_onscreen IS DISTINCT FROM
		                    CASE WHEN EXCLUDED.episode_num_onscreen <> '' THEN EXCLUDED.episode_num_onscreen
		                         WHEN epg_program.episode_num_onscreen_supplemented THEN epg_program.episode_num_onscreen
		                         ELSE '' END)
		           THEN '-infinity'::timestamptz
		           ELSE epg_program.enrichment_retry_at
		       END,
		       enrichment_status    = CASE
		           WHEN epg_program.is_legacy
		                OR (EXCLUDED.source_priority <= epg_program.source_priority
		                AND (epg_program.title     IS DISTINCT FROM EXCLUDED.title
		                  OR epg_program.sub_title IS DISTINCT FROM EXCLUDED.sub_title
		                  OR epg_program.is_movie  IS DISTINCT FROM EXCLUDED.is_movie
		                  OR epg_program.original_air_date IS DISTINCT FROM EXCLUDED.original_air_date))
		           THEN 'pending'::enrichment_status
		           WHEN (epg_program.is_legacy OR EXCLUDED.source_priority <= epg_program.source_priority)
		            AND (epg_program.episode_num_xmltv IS DISTINCT FROM
		                    CASE WHEN EXCLUDED.episode_num_xmltv <> '' THEN EXCLUDED.episode_num_xmltv
		                         WHEN epg_program.episode_num_xmltv_supplemented THEN epg_program.episode_num_xmltv
		                         ELSE '' END
		              OR epg_program.episode_num_onscreen IS DISTINCT FROM
		                    CASE WHEN EXCLUDED.episode_num_onscreen <> '' THEN EXCLUDED.episode_num_onscreen
		                         WHEN epg_program.episode_num_onscreen_supplemented THEN epg_program.episode_num_onscreen
		                         ELSE '' END)
		           THEN 'pending'::enrichment_status
		           ELSE epg_program.enrichment_status
		       END
		 WHERE epg_program.is_legacy
		    OR (EXCLUDED.source_priority <= epg_program.source_priority
		        AND epg_program.source_hash IS DISTINCT FROM EXCLUDED.source_hash)
		RETURNING (xmax = 0), source_hash`,
		p.ChannelID, p.StartAt, p.EndAt, p.Title, p.SubTitle, p.Description,
		p.Category, p.EpisodeNumXMLTV, p.EpisodeNumOnscreen,
		p.OriginalAirDate, p.IsMovie, p.IsLive, p.IsNew, p.IsPremiere,
		p.IsFinale, p.Rating, p.SourcePosterURL, p.SourceHash, p.SourcePriority).Scan(&inserted, &durableSourceHash)
	if errors.Is(err, pgx.ErrNoRows) {
		// Conflict row exists and the WHERE guard rejected the update —
		// either same content or a stronger source owns the slot.
		return "unchanged", nil
	}
	if err != nil {
		return "", err
	}
	if _, err := q.Exec(ctx, `
		UPDATE epg_program
		   SET provider_episode_id = CASE
		           WHEN $4 <> '' THEN $4
		           WHEN provider_episode_id_supplemented THEN provider_episode_id
		           ELSE ''
		       END,
		       provider_episode_id_supplemented = CASE
		           WHEN $4 <> '' THEN false
		           ELSE provider_episode_id_supplemented
		       END,
		       new_explicit = $5,
		       previously_shown_explicit = $6,
		       airing_civil_date = $7
		 WHERE channel_id = $1
		   AND start_at = $2
		   AND epg_source_id IS NULL
		   AND source_hash = $3`,
		p.ChannelID, p.StartAt, p.SourceHash, p.ProviderEpisodeID,
		p.NewExplicit, p.PreviouslyShownExplicit, p.AiringCivilDate); err != nil {
		return "", err
	}
	// SD only skips a station when its MD5 certifies that the corresponding
	// candidate is durable. Any different direct writer, including another SD
	// station sharing this channel/start slot, can replace that row. Invalidate
	// provider watermarks owned by this channel in the same transaction so every
	// displaced station retries and can restore itself after the replacement
	// disappears without forcing unrelated stations through a full refetch.
	if strings.HasPrefix(previousSourceHash, "sd:") && durableSourceHash != previousSourceHash {
		if _, err := q.Exec(ctx, `UPDATE sd_station_state SET last_md5 = '' WHERE channel_id = $1`, p.ChannelID); err != nil {
			return "", err
		}
	}
	if inserted {
		return "added", nil
	}
	return "updated", nil
}

func upsertEPGSourceProgram(ctx context.Context, tx pgx.Tx, p EPGProgram) (string, error) {
	if p.EPGSourceID == nil || *p.EPGSourceID == uuid.Nil || p.SourceGeneration <= 0 {
		return "", errors.New("XMLTV source programme is missing snapshot provenance")
	}
	var oldHash string
	err := tx.QueryRow(ctx, `
		SELECT source_hash
		  FROM epg_program
		 WHERE epg_source_id = $1
		   AND channel_id = $2
		   AND start_at = $3`, *p.EPGSourceID, p.ChannelID, p.StartAt).Scan(&oldHash)
	switch {
	case err == nil:
	case errors.Is(err, pgx.ErrNoRows):
		oldHash = ""
	default:
		return "", err
	}

	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO epg_program (
		    channel_id, start_at, end_at, title, sub_title, description,
		    category, episode_num_xmltv, episode_num_onscreen,
		    episode_num_xmltv_supplemented, episode_num_onscreen_supplemented,
		    original_air_date, is_movie, is_live, is_new, is_premiere,
		    is_finale, rating, source_poster_url, source_hash, source_priority,
		    enrichment_status, epg_source_id, source_generation,
		    is_canonical, is_legacy
		)
		VALUES ($1, $2, $3, $4, $5, $6, coalesce($7, '{}'::text[]), $8, $9, false, false,
		        $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
		        'pending', $20, $21, false, false)
		ON CONFLICT (epg_source_id, channel_id, start_at)
		    WHERE epg_source_id IS NOT NULL
		DO UPDATE
		   SET end_at               = EXCLUDED.end_at,
		       title                = EXCLUDED.title,
		       sub_title            = EXCLUDED.sub_title,
		       description          = EXCLUDED.description,
		       category             = EXCLUDED.category,
		       episode_num_xmltv    = CASE
		           WHEN EXCLUDED.episode_num_xmltv <> '' THEN EXCLUDED.episode_num_xmltv
		           WHEN epg_program.episode_num_xmltv_supplemented THEN epg_program.episode_num_xmltv
		           ELSE ''
		       END,
		       episode_num_onscreen = CASE
		           WHEN EXCLUDED.episode_num_onscreen <> '' THEN EXCLUDED.episode_num_onscreen
		           WHEN epg_program.episode_num_onscreen_supplemented THEN epg_program.episode_num_onscreen
		           ELSE ''
		       END,
		       episode_num_xmltv_supplemented = CASE
		           WHEN EXCLUDED.episode_num_xmltv <> '' THEN false
		           ELSE epg_program.episode_num_xmltv_supplemented
		       END,
		       episode_num_onscreen_supplemented = CASE
		           WHEN EXCLUDED.episode_num_onscreen <> '' THEN false
		           ELSE epg_program.episode_num_onscreen_supplemented
		       END,
		       original_air_date    = EXCLUDED.original_air_date,
		       is_movie             = EXCLUDED.is_movie,
		       is_live              = EXCLUDED.is_live,
		       is_new               = EXCLUDED.is_new,
		       is_premiere          = EXCLUDED.is_premiere,
		       is_finale            = EXCLUDED.is_finale,
		       rating               = EXCLUDED.rating,
		       source_poster_url    = EXCLUDED.source_poster_url,
		       generated_poster_url = CASE
		           WHEN epg_program.title IS DISTINCT FROM EXCLUDED.title
		             OR epg_program.sub_title IS DISTINCT FROM EXCLUDED.sub_title
		             OR epg_program.is_movie IS DISTINCT FROM EXCLUDED.is_movie
		             OR epg_program.original_air_date IS DISTINCT FROM EXCLUDED.original_air_date
		           THEN '' ELSE epg_program.generated_poster_url
		       END,
		       source_hash          = EXCLUDED.source_hash,
		       source_priority      = EXCLUDED.source_priority,
		       source_generation    = EXCLUDED.source_generation,
		       is_canonical         = CASE
		           WHEN epg_program.source_hash IS DISTINCT FROM EXCLUDED.source_hash
		           THEN false ELSE epg_program.is_canonical
		       END,
		       is_legacy            = false,
		       updated_at           = CASE
		           WHEN epg_program.source_hash IS DISTINCT FROM EXCLUDED.source_hash
		           THEN now() ELSE epg_program.updated_at
		       END,
		       enrichment_retry_at  = CASE
		           WHEN epg_program.title IS DISTINCT FROM EXCLUDED.title
		             OR epg_program.sub_title IS DISTINCT FROM EXCLUDED.sub_title
		             OR epg_program.is_movie IS DISTINCT FROM EXCLUDED.is_movie
		             OR epg_program.original_air_date IS DISTINCT FROM EXCLUDED.original_air_date
		             OR epg_program.episode_num_xmltv IS DISTINCT FROM
		                CASE
		                    WHEN EXCLUDED.episode_num_xmltv <> '' THEN EXCLUDED.episode_num_xmltv
		                    WHEN epg_program.episode_num_xmltv_supplemented THEN epg_program.episode_num_xmltv
		                    ELSE ''
		                END
		             OR epg_program.episode_num_onscreen IS DISTINCT FROM
		                CASE
		                    WHEN EXCLUDED.episode_num_onscreen <> '' THEN EXCLUDED.episode_num_onscreen
		                    WHEN epg_program.episode_num_onscreen_supplemented THEN epg_program.episode_num_onscreen
		                    ELSE ''
		                END
		           THEN '-infinity'::timestamptz ELSE epg_program.enrichment_retry_at
		       END,
		       enrichment_status    = CASE
		           WHEN epg_program.title IS DISTINCT FROM EXCLUDED.title
		             OR epg_program.sub_title IS DISTINCT FROM EXCLUDED.sub_title
		             OR epg_program.is_movie IS DISTINCT FROM EXCLUDED.is_movie
		             OR epg_program.original_air_date IS DISTINCT FROM EXCLUDED.original_air_date
		             OR epg_program.episode_num_xmltv IS DISTINCT FROM
		                CASE
		                    WHEN EXCLUDED.episode_num_xmltv <> '' THEN EXCLUDED.episode_num_xmltv
		                    WHEN epg_program.episode_num_xmltv_supplemented THEN epg_program.episode_num_xmltv
		                    ELSE ''
		                END
		             OR epg_program.episode_num_onscreen IS DISTINCT FROM
		                CASE
		                    WHEN EXCLUDED.episode_num_onscreen <> '' THEN EXCLUDED.episode_num_onscreen
		                    WHEN epg_program.episode_num_onscreen_supplemented THEN epg_program.episode_num_onscreen
		                    ELSE ''
		                END
		           THEN 'pending'::enrichment_status ELSE epg_program.enrichment_status
		       END
		RETURNING (xmax = 0)`,
		p.ChannelID, p.StartAt, p.EndAt, p.Title, p.SubTitle, p.Description,
		p.Category, p.EpisodeNumXMLTV, p.EpisodeNumOnscreen,
		p.OriginalAirDate, p.IsMovie, p.IsLive, p.IsNew, p.IsPremiere,
		p.IsFinale, p.Rating, p.SourcePosterURL, p.SourceHash, p.SourcePriority,
		*p.EPGSourceID, p.SourceGeneration).Scan(&inserted)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE epg_program
		   SET provider_episode_id = CASE
		           WHEN $5 <> '' THEN $5
		           WHEN provider_episode_id_supplemented THEN provider_episode_id
		           ELSE ''
		       END,
		       provider_episode_id_supplemented = CASE
		           WHEN $5 <> '' THEN false
		           ELSE provider_episode_id_supplemented
		       END,
		       new_explicit = $6,
		       previously_shown_explicit = $7,
		       airing_civil_date = $8
		 WHERE epg_source_id = $1
		   AND channel_id = $2
		   AND start_at = $3
		   AND source_hash = $4`,
		*p.EPGSourceID, p.ChannelID, p.StartAt, p.SourceHash,
		p.ProviderEpisodeID, p.NewExplicit, p.PreviouslyShownExplicit,
		p.AiringCivilDate); err != nil {
		return "", err
	}
	if inserted {
		return "added", nil
	}
	if oldHash == p.SourceHash {
		return "unchanged", nil
	}
	return "updated", nil
}

// PurgeOldPrograms deletes programs whose end_at is older than retention.
// Spec §6.3 implies we keep history for the "we've seen this before" check
// (90 days) but the EPG output only needs forward-looking programs.
// Default retention 14 days back from now.
func (db *DB) PurgeOldPrograms(ctx context.Context, retainPastDays int) (int64, error) {
	// Ordinary guide rows can retain the established bulk-delete fast path.
	// PPV rows are protected by the rolling-upgrade database fence, so reap
	// those channel-by-channel under the same serialized authority as ppvsync.
	tag, err := db.Pool.Exec(ctx, `
		DELETE FROM epg_program
		 WHERE source_hash NOT LIKE 'ppv-%'
		   AND end_at < now() - ($1::int * interval '1 day')`, retainPastDays)
	if err != nil {
		return 0, err
	}
	deleted := tag.RowsAffected()
	rows, err := db.Pool.Query(ctx, `
		SELECT DISTINCT channel_id
		  FROM epg_program
		 WHERE source_hash LIKE 'ppv-%'
		   AND end_at < now() - ($1::int * interval '1 day')
		 ORDER BY channel_id`, retainPastDays)
	if err != nil {
		return deleted, err
	}
	var channelIDs []uuid.UUID
	for rows.Next() {
		var channelID uuid.UUID
		if err := rows.Scan(&channelID); err != nil {
			rows.Close()
			return deleted, err
		}
		channelIDs = append(channelIDs, channelID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return deleted, err
	}
	for _, channelID := range channelIDs {
		var channelDeleted int64
		if err := db.InTx(ctx, func(tx pgx.Tx) error {
			if err := lockEPGChannel(ctx, tx, channelID); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `
				DELETE FROM epg_program
				 WHERE channel_id = $1
				   AND source_hash LIKE 'ppv-%'
				   AND end_at < now() - ($2::int * interval '1 day')`,
				channelID, retainPastDays)
			if err != nil {
				return err
			}
			channelDeleted = tag.RowsAffected()
			return nil
		}); err != nil {
			return deleted, err
		}
		deleted += channelDeleted
	}
	return deleted, nil
}

// ListProgramsForOutput returns programs in the [from, to] window for all
// enabled channels, ordered for emission. Used by the /xmltv.xml handler.
type ProgramOutputRow struct {
	ChannelID          uuid.UUID
	ChannelEPGID       string
	ChannelNumber      float64
	ChannelName        string
	ChannelLogoURL     string
	StartAt            time.Time
	EndAt              time.Time
	Title              string
	SubTitle           string
	Description        string
	Category           []string
	EpisodeNumXMLTV    string
	EpisodeNumOnscreen string
	OriginalAirDate    *time.Time
	AiringCivilDate    *time.Time
	IsMovie            bool
	IsLive             bool
	IsNew              bool
	IsPremiere         bool
	IsFinale           bool
	Rating             string
	SourcePosterURL    string
	GeneratedPosterURL string
	IsPlaceholder      bool
	// IsPPVPlaceholder preserves exact ppv-off provenance separately from the
	// broader title-based placeholder classification used during projection.
	IsPPVPlaceholder   bool
	HasEpisodeIdentity bool

	// Preserve explicit source repeat evidence independently of effective New.
	PreviouslyShownExplicit bool
	// A known earlier actual airing, not merely absence of first-run evidence.
	HasEarlierAiring bool

	// Enrichment fields (Phase 3) — empty when no enrichment row exists or
	// enrichment_status is neither matched nor manual. The XMLTV emitter keeps
	// provider artwork above source art and researched/manual assets below it.
	EnrichmentPosterURL   string
	EnrichmentBackdropURL string
	EnrichmentOverview    string
	EnrichmentTMDbID      int
	EnrichmentIMDbID      string
}

func (db *DB) ListProgramsForOutput(ctx context.Context, from, to time.Time) ([]ProgramOutputRow, error) {
	// LEFT JOIN epg_program_enrichment so unmatched programs still appear
	// (the alternative — INNER JOIN — would silently drop programs while
	// they wait for enrichment). Output emitter handles empty enrichment
	// fields by falling back to source values.
	rows, err := db.Pool.Query(ctx, `
		SELECT c.id, c.epg_channel_id, c.number, c.name, c.logo_url,
		       p.start_at, p.end_at, p.title, p.sub_title, p.description,
		       p.category, p.episode_num_xmltv, p.episode_num_onscreen,
		       p.original_air_date, p.airing_civil_date,
		       p.is_movie, p.is_live, p.effective_is_new,
		       p.is_premiere, p.is_finale, p.rating, p.source_poster_url,
		       p.generated_poster_url,
		       p.source_hash LIKE 'ppv-off:%',
		       p.episode_identity <> '',
		       p.previously_shown_explicit,
		       CASE WHEN p.is_live AND p.series_identity <> '' AND p.episode_identity <> '' THEN
		         EXISTS (
		           SELECT 1 FROM episode_airing_history h
		            WHERE h.series_identity = p.series_identity
		              AND h.episode_identity = ANY(epg_episode_identities(
		                  p.episode_num_onscreen, p.episode_num_xmltv, p.provider_episode_id))
		              AND h.first_aired_at < p.start_at
		              AND h.first_aired_at <= statement_timestamp()
		         ) OR EXISTS (
		           SELECT 1 FROM epg_program earlier
		            WHERE earlier.is_canonical AND NOT earlier.is_movie
		              AND epg_series_identity(earlier.title) = p.series_identity
		              AND epg_episode_identities(earlier.episode_num_onscreen,
		                  earlier.episode_num_xmltv, earlier.provider_episode_id) &&
		                  epg_episode_identities(p.episode_num_onscreen,
		                  p.episode_num_xmltv, p.provider_episode_id)
		              AND earlier.start_at < p.start_at
		              AND earlier.start_at <= statement_timestamp()
		         ) ELSE false END,
		       COALESCE(e.poster_url, ''),
		       COALESCE(e.backdrop_url, ''),
		       COALESCE(e.overview, ''),
		       COALESCE(e.tmdb_id, 0),
		       COALESCE(e.imdb_id, '')
		  FROM epg_program_effective p
		  JOIN channel c ON c.id = p.channel_id
		  LEFT JOIN epg_program_enrichment e ON e.program_id = p.id
		       AND p.enrichment_status IN ('matched', 'manual')
		 WHERE c.enabled
		   AND p.is_canonical
		   AND p.end_at >= $1 AND p.start_at <= $2
		 ORDER BY c.number, p.start_at`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProgramOutputRow
	for rows.Next() {
		var r ProgramOutputRow
		if err := rows.Scan(
			&r.ChannelID, &r.ChannelEPGID, &r.ChannelNumber, &r.ChannelName, &r.ChannelLogoURL,
			&r.StartAt, &r.EndAt, &r.Title, &r.SubTitle, &r.Description,
			&r.Category, &r.EpisodeNumXMLTV, &r.EpisodeNumOnscreen,
			&r.OriginalAirDate, &r.AiringCivilDate, &r.IsMovie, &r.IsLive, &r.IsNew,
			&r.IsPremiere, &r.IsFinale, &r.Rating, &r.SourcePosterURL,
			&r.GeneratedPosterURL, &r.IsPlaceholder, &r.HasEpisodeIdentity, &r.PreviouslyShownExplicit, &r.HasEarlierAiring,
			&r.EnrichmentPosterURL, &r.EnrichmentBackdropURL,
			&r.EnrichmentOverview, &r.EnrichmentTMDbID, &r.EnrichmentIMDbID,
		); err != nil {
			return nil, err
		}
		r.IsPPVPlaceholder = r.IsPlaceholder
		out = append(out, r)
	}
	return out, rows.Err()
}

// EPGCanonicalHealth reports the visible and retained-fallback populations and
// independently verifies the database's no-overlap invariant. The overlap
// count should be permanently zero; callers fail closed if it is not.
func (db *DB) EPGCanonicalHealth(ctx context.Context) (canonical, suppressed, overlaps int64, err error) {
	err = db.Pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE is_canonical),
		       count(*) FILTER (WHERE NOT is_canonical)
		  FROM epg_program`).Scan(&canonical, &suppressed)
	if err != nil {
		return 0, 0, 0, err
	}
	err = db.Pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM epg_program a
		  JOIN epg_program b
		    ON a.channel_id = b.channel_id
		   AND a.id < b.id
		   AND a.start_at < b.end_at
		   AND a.end_at > b.start_at
		 WHERE a.is_canonical AND b.is_canonical`).Scan(&overlaps)
	return canonical, suppressed, overlaps, err
}

// LatestProgramUpdate returns the latest content/artwork mutation in the
// current programme set.  Enrichment used to be invisible to the XMLTV ETag,
// so Plex could keep a 304-cached guide after a poster was added.
func (db *DB) LatestProgramUpdate(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := db.Pool.QueryRow(ctx, `
		SELECT GREATEST(
		    r.updated_at,
		    COALESCE(MAX(GREATEST(p.updated_at, COALESCE(e.matched_at, p.updated_at))), r.updated_at)
		)
		  FROM epg_revision r
		  LEFT JOIN epg_program p ON p.is_canonical
		  LEFT JOIN epg_program_enrichment e ON e.program_id = p.id
		 WHERE r.singleton
		 GROUP BY r.updated_at`).Scan(&t)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

// EPGIngestRun inserts an ingest_run row and returns its id; call
// FinishEPGIngestRun with totals when done.
func (db *DB) StartEPGIngestRun(ctx context.Context) (uuid.UUID, error) {
	var id uuid.UUID
	err := db.Pool.QueryRow(ctx,
		`INSERT INTO epg_ingest_run DEFAULT VALUES RETURNING id`).Scan(&id)
	return id, err
}

func (db *DB) FinishEPGIngestRun(ctx context.Context, runID uuid.UUID,
	attempted, ok, noChange, failed, added, updated, unchanged int, errMsg string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE epg_ingest_run
		   SET finished_at = now(),
		       sources_attempted = $2, sources_ok = $3,
		       sources_no_change = $4, sources_failed = $5,
		       programs_added = $6, programs_updated = $7, programs_unchanged = $8,
		       error = $9
		 WHERE id = $1`,
		runID, attempted, ok, noChange, failed, added, updated, unchanged, errMsg)
	return err
}

// DeletePPVOffRows clears stale "No Event Scheduled" placeholders for one
// channel: rows from before the current grid window, any that overlap the
// selected event window (evStart/evEnd may be nil), and any that overlap a
// stronger real programme already stored on the channel. The final predicate
// is load-bearing: filtering this pass's generated rows alone would leave an
// older placeholder in the database after an operator inserts a manual event.
// Cleanup is intentionally channel-wide because redundant source accounts can
// use different stream ids for the same channel; a placeholder from a lagging
// source must never survive over the selected event.
// Current-grid, non-overlapping placeholders are left in place so the
// worker's slot-keyed re-upserts read "unchanged" or update in place.
func (db *DB) DeletePPVOffRows(ctx context.Context, channelID uuid.UUID, gridStart time.Time, evStart, evEnd *time.Time) (deleted int64, err error) {
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockEPGChannel(ctx, tx, channelID); err != nil {
			return err
		}
		tag, execErr := tx.Exec(ctx, `
			DELETE FROM epg_program AS off_row
			 WHERE off_row.channel_id = $1
			   AND off_row.source_hash LIKE 'ppv-off:%'
			   AND (off_row.start_at < $2
			        OR ($3::timestamptz IS NOT NULL
			            AND off_row.start_at < $4 AND off_row.end_at > $3)
			        OR EXISTS (
			            SELECT 1
			              FROM epg_program AS real_row
			             WHERE real_row.channel_id = off_row.channel_id
			               AND real_row.source_priority < $5
			               AND real_row.start_at < off_row.end_at
			               AND real_row.end_at > off_row.start_at
			        ))`,
			channelID, gridStart, evStart, evEnd, PriorityPPVOff)
		if execErr != nil {
			return execErr
		}
		deleted = tag.RowsAffected()
		return finalizeEPGChannelMutation(ctx, tx, channelID, deleted > 0)
	})
	return deleted, err
}
