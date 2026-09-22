package store

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/spencercnorton/conductor/internal/metrics"
)

// pgUniqueViolation is SQLSTATE 23505.
const pgUniqueViolation = "23505"

// DefaultDVRPriority is used when an in-process caller leaves Priority zero.
// Persisted priorities are strictly positive; lower values win.
const DefaultDVRPriority = 100

// DVRRecording mirrors one row in dvr_recording.
type DVRRecording struct {
	ID                 uuid.UUID
	ChannelID          uuid.UUID
	ProgramID          *uuid.UUID
	Title              string
	SubTitle           string
	EpisodeNumXMLTV    string
	EpisodeNumOnscreen string
	IsMovie            bool
	ScheduledStart     time.Time
	ScheduledEnd       time.Time
	State              string // see dvr_recording_state enum
	// Earlier scheduled starts are non-preemptible. Among recordings becoming
	// eligible at the same start, lower Priority wins, then created_at and id.
	Priority                 int
	AdmissionState           string // pending | queued | admitted
	AdmissionAttempts        int
	AdmissionRetryAt         *time.Time
	AdmissionReason          string
	RequestedBy              string
	OutputPath               string
	BytesWritten             int64
	StartedAt                *time.Time
	CompletedAt              *time.Time
	Error                    string
	CreatedAt                time.Time
	CompletionOperationID    *uuid.UUID
	RecordingHeartbeatAt     *time.Time
	ArtifactState            string
	ArtifactStage            string
	ArtifactCurrent          bool
	ArtifactPath             string
	ArtifactBytes            int64
	ArtifactSHA256           []byte
	ArtifactFilesystemID     string
	ArtifactValidatedAt      *time.Time
	ArtifactValidatorVersion string
	ArtifactError            string
	ReplacesRecordingID      *uuid.UUID
	ExplicitRerecord         bool
	MediaDurationMS          int64
	MediaMaxGapMS            int64
	MediaDiscontinuities     int
	MediaAVStartDeltaMS      int64
	MediaAVEndDeltaMS        int64
	MediaAVDriftMS           int64
}

const dvrRecordingColumns = `
	id, channel_id, program_id, title, sub_title,
	episode_num_xmltv, episode_num_onscreen, is_movie,
	scheduled_start, scheduled_end, state::text, priority,
	admission_state, admission_attempts, admission_retry_at,
	admission_reason, requested_by, output_path, bytes_written,
	started_at, completed_at, error, created_at, completion_operation_id,
	recording_heartbeat_at, artifact_state, artifact_stage,
	artifact_current, artifact_path, artifact_bytes, artifact_sha256,
	artifact_filesystem_id,
	artifact_validated_at, artifact_validator_version, artifact_error,
	replaces_recording_id, explicit_rerecord, media_duration_ms,
	media_max_gap_ms, media_discontinuities, media_av_start_delta_ms,
	media_av_end_delta_ms, media_av_drift_ms`

type dvrRowScanner interface {
	Scan(dest ...any) error
}

func scanDVRRecording(row dvrRowScanner) (DVRRecording, error) {
	var r DVRRecording
	err := row.Scan(
		&r.ID, &r.ChannelID, &r.ProgramID, &r.Title, &r.SubTitle,
		&r.EpisodeNumXMLTV, &r.EpisodeNumOnscreen, &r.IsMovie,
		&r.ScheduledStart, &r.ScheduledEnd, &r.State, &r.Priority,
		&r.AdmissionState, &r.AdmissionAttempts, &r.AdmissionRetryAt,
		&r.AdmissionReason, &r.RequestedBy, &r.OutputPath, &r.BytesWritten,
		&r.StartedAt, &r.CompletedAt, &r.Error, &r.CreatedAt,
		&r.CompletionOperationID, &r.RecordingHeartbeatAt, &r.ArtifactState,
		&r.ArtifactStage, &r.ArtifactCurrent, &r.ArtifactPath,
		&r.ArtifactBytes, &r.ArtifactSHA256, &r.ArtifactFilesystemID,
		&r.ArtifactValidatedAt,
		&r.ArtifactValidatorVersion, &r.ArtifactError,
		&r.ReplacesRecordingID, &r.ExplicitRerecord, &r.MediaDurationMS,
		&r.MediaMaxGapMS, &r.MediaDiscontinuities, &r.MediaAVStartDeltaMS,
		&r.MediaAVEndDeltaMS, &r.MediaAVDriftMS,
	)
	return r, err
}

// EPGSearchHit is one upcoming program that matched an indexer search.
// Used by /indexer/torznab/api result rendering.
type EPGSearchHit struct {
	ProgramID          uuid.UUID
	ChannelID          uuid.UUID
	ChannelNumber      float64
	ChannelName        string
	ChannelEPGID       string
	StartAt            time.Time
	EndAt              time.Time
	Title              string
	SubTitle           string
	Description        string
	EpisodeNumXMLTV    string
	EpisodeNumOnscreen string
	IsMovie            bool
	IsLive             bool
	IsNew              bool
}

// EPGIndexerSearch holds the optional filters for one Torznab indexer
// query. A zero-valued field means "don't filter on this".
type EPGIndexerSearch struct {
	Query     string // free-text title match (case-insensitive substring)
	Season    int
	Episode   int
	Year      int
	IMDbID    string // "tt0848228", "0848228" and "848228" all accepted
	TMDbID    int
	TVDBID    int
	MovieOnly bool
	Limit     int
}

// HasPrimaryFilter reports whether the search carries a selector that
// identifies a specific title — a free-text query or an external id.
// Season, episode and year only narrow a result set; on their own they
// would still match the entire upcoming catalog. SearchEPGForIndexer
// refuses a query with no primary filter so the indexer never floods a
// requesting *arr with every airing it knows about.
func (s EPGIndexerSearch) HasPrimaryFilter() bool {
	return s.Query != "" || s.IMDbID != "" || s.TMDbID > 0 || s.TVDBID > 0
}

// SearchEPGForIndexer returns upcoming programs (end_at >= now()) matching
// the given filters, ordered by air time.
//
// The free-text query and the external ids (imdb/tmdb/tvdb) form an OR
// group: a program matches if its title contains the query OR its
// enrichment row carries a matching id. They are OR-combined so a program
// that has not been enriched yet is still found by title even when the
// *arr also supplied an id. MovieOnly, year and season/episode then narrow
// that set (case-insensitive title substring for the query; the episode
// filter applies only when both season and ep are non-zero).
//
// A search with no primary filter (see HasPrimaryFilter) returns no rows:
// the Torznab indexer is a last-resort fallback and must answer "no match"
// rather than dump every upcoming airing onto the requesting *arr.
func (db *DB) SearchEPGForIndexer(ctx context.Context, s EPGIndexerSearch) ([]EPGSearchHit, error) {
	if !s.HasPrimaryFilter() {
		return nil, nil
	}

	limit := s.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	args := []any{time.Now(), limit}
	conds := []string{"p.end_at >= $1", "c.enabled", "p.is_canonical"}

	// Primary-selector OR group. HasPrimaryFilter being true guarantees at
	// least one part, so the joined group is never empty.
	var orParts []string
	if s.Query != "" {
		args = append(args, "%"+s.Query+"%")
		orParts = append(orParts, "p.title ILIKE $"+itoa(len(args)))
	}
	if s.IMDbID != "" {
		// The enrichment row stores the canonical id ("tt0848228"); *arrs
		// send the bare number, sometimes zero-padded. Strip an optional
		// "tt" and any leading zeros from both sides so all forms compare
		// equal. NULL imdb_id (unenriched program) yields NULL → no match.
		args = append(args, s.IMDbID)
		n := itoa(len(args))
		orParts = append(orParts,
			"regexp_replace(lower(e.imdb_id), '^(tt)?0*', '') = "+
				"regexp_replace(lower($"+n+"), '^(tt)?0*', '')")
	}
	if s.TMDbID > 0 {
		args = append(args, s.TMDbID)
		orParts = append(orParts, "e.tmdb_id = $"+itoa(len(args)))
	}
	if s.TVDBID > 0 {
		args = append(args, s.TVDBID)
		orParts = append(orParts, "e.tvdb_id = $"+itoa(len(args)))
	}
	orGroup := ""
	for i, p := range orParts {
		if i == 0 {
			orGroup = p
		} else {
			orGroup += " OR " + p
		}
	}
	conds = append(conds, "("+orGroup+")")

	if s.MovieOnly {
		conds = append(conds, "p.is_movie = true")
	}
	if s.Year > 0 {
		args = append(args, s.Year)
		// Match year either via original_air_date OR a (YYYY) in the title.
		conds = append(conds,
			"(EXTRACT(YEAR FROM p.original_air_date) = $"+itoa(len(args))+
				" OR p.title ~ ('\\(' || $"+itoa(len(args))+"::text || '\\)'))")
	}
	if s.Season > 0 && s.Episode > 0 {
		args = append(args, formatOnscreen(s.Season, s.Episode))
		conds = append(conds, "p.episode_num_onscreen = $"+itoa(len(args)))
	}

	where := ""
	for i, c := range conds {
		if i == 0 {
			where = "WHERE " + c
		} else {
			where += " AND " + c
		}
	}

	sql := `
		SELECT p.id, p.channel_id, c.number, c.name, c.epg_channel_id,
		       p.start_at, p.end_at, p.title, p.sub_title, p.description,
		       p.episode_num_xmltv, p.episode_num_onscreen,
		       p.is_movie, p.is_live, p.effective_is_new
		  FROM epg_program_effective p
		  JOIN channel c ON c.id = p.channel_id
		  LEFT JOIN epg_program_enrichment e ON e.program_id = p.id
		` + where + `
		 ORDER BY p.start_at ASC
		 LIMIT $2`

	rows, err := db.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EPGSearchHit
	for rows.Next() {
		var h EPGSearchHit
		if err := rows.Scan(
			&h.ProgramID, &h.ChannelID, &h.ChannelNumber, &h.ChannelName, &h.ChannelEPGID,
			&h.StartAt, &h.EndAt, &h.Title, &h.SubTitle, &h.Description,
			&h.EpisodeNumXMLTV, &h.EpisodeNumOnscreen,
			&h.IsMovie, &h.IsLive, &h.IsNew,
		); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// itoa is duplicated here to avoid an import; the SQL builder above just
// needs to embed positional argument indices.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	out := ""
	for n > 0 {
		out = string(rune('0'+(n%10))) + out
		n /= 10
	}
	return out
}

func formatOnscreen(season, episode int) string {
	return "S" + zpad2(season) + "E" + zpad2(episode)
}
func zpad2(n int) string {
	if n < 10 {
		return "0" + itoa(n)
	}
	return itoa(n)
}

// LogIndexerSearch records a Torznab request for audit.
// Errors are non-fatal — caller should log + continue.
func (db *DB) LogIndexerSearch(ctx context.Context, remote net.IP, ua, query, kind string, season, episode, year, matches int) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO dvr_indexer_search_log
		    (remote_addr, user_agent, query, type, season, episode, year, matches)
		VALUES ($1, $2, $3, $4, NULLIF($5,0), NULLIF($6,0), NULLIF($7,0), $8)`,
		remote, ua, query, kind, season, episode, year, matches)
	return err
}

// ErrOutputPathBusy means another airing of this same episode is already
// scheduled or recording into the same output_path. Callers should treat it
// as "already handled", not as an error worth retrying.
var ErrOutputPathBusy = errors.New("another recording already owns this output_path")

// ErrValidatedArtifactExists means a validated completed attempt owns the
// canonical output path. Automated repeat-airing requests must treat this as
// already handled. Only CreateDVRReplacement may create an explicit successor.
var ErrValidatedArtifactExists = errors.New("a validated DVR artifact already owns this output_path")

// ErrArtifactNotReplaceable means the caller did not identify a current,
// validated artifact. The API deliberately has no detached/loose "arm" bit:
// replacement authorization and the target airing are persisted together.
var ErrArtifactNotReplaceable = errors.New("DVR artifact is not a current validated replacement owner")

// ErrArtifactReplacementMismatch means replacement intent does not target the
// selected owner's exact canonical path or does not carry a durable EPG target.
var ErrArtifactReplacementMismatch = errors.New("DVR replacement intent does not match its artifact owner")

// ErrArtifactReplacementTargetNotFuture means a new replacement request
// crossed its padded admission boundary before the store acquired every lock
// needed to authorize it. Exact persisted retries are returned before this
// deadline is evaluated.
var ErrArtifactReplacementTargetNotFuture = errors.New("replacement target is no longer a future airing")

// ErrDVRArtifactOwnerPreflightRequired means an automated recording intent
// encountered a current artifact owner that must be reconciled before a
// successor can become runnable. The returned recording is that owner; no
// successor row has been inserted.
var ErrDVRArtifactOwnerPreflightRequired = errors.New("DVR artifact owner requires filesystem preflight")

// ErrDVRArtifactOwnerChanged means the current artifact owner no longer
// matches the exact owner the caller reconciled. No successor row has been
// inserted; callers may restart the bounded preflight against fresh state.
var ErrDVRArtifactOwnerChanged = errors.New("DVR artifact owner changed during filesystem preflight")

// DVRArtifactOwnerProof is the persisted evidence an automated scheduler saw
// after filesystem reconciliation. The store compares every field again under
// the output-path/current-owner locks before it exposes a successor row.
type DVRArtifactOwnerProof struct {
	RecordingID          uuid.UUID
	OutputPath           string
	ArtifactPath         string
	ArtifactState        string
	ArtifactBytes        int64
	ArtifactFilesystemID string
}

// NewDVRArtifactOwnerProof captures the database half of a reconciled owner's
// evidence. Callers must reload the owner after filesystem reconciliation so a
// newly bound filesystem identity is included in the proof.
func NewDVRArtifactOwnerProof(owner DVRRecording) DVRArtifactOwnerProof {
	return DVRArtifactOwnerProof{
		RecordingID:          owner.ID,
		OutputPath:           owner.OutputPath,
		ArtifactPath:         owner.ArtifactPath,
		ArtifactState:        owner.ArtifactState,
		ArtifactBytes:        owner.ArtifactBytes,
		ArtifactFilesystemID: owner.ArtifactFilesystemID,
	}
}

func (p DVRArtifactOwnerProof) valid() bool {
	return p.RecordingID != uuid.Nil &&
		strings.TrimSpace(p.OutputPath) != "" &&
		strings.TrimSpace(p.ArtifactPath) != "" &&
		(p.ArtifactState == "legacy" || p.ArtifactState == "validated") &&
		strings.TrimSpace(p.ArtifactFilesystemID) != ""
}

func (p DVRArtifactOwnerProof) matches(owner DVRRecording) bool {
	return p.RecordingID == owner.ID &&
		p.OutputPath == owner.OutputPath &&
		p.ArtifactPath == owner.ArtifactPath &&
		p.ArtifactState == owner.ArtifactState &&
		p.ArtifactBytes == owner.ArtifactBytes &&
		p.ArtifactFilesystemID == owner.ArtifactFilesystemID
}

// CreateDVRRecording inserts an automated recording intent. Exact-airing
// re-grabs remain idempotent, including after completion. Active writers keep
// the path busy. A current validated artifact suppresses a distinct airing,
// while a legacy current artifact is retained and attached as replacement
// provenance so a newly validated candidate can safely supersede it.
func (db *DB) CreateDVRRecording(ctx context.Context, r DVRRecording, policies ...DVRAdmissionPolicy) (DVRRecording, error) {
	return db.createDVRRecordingWithArtifactOwner(
		ctx, r, false, nil, policies...)
}

// CreateDVRRecordingWithArtifactPreflight is the automated-scheduling entry
// point for filesystem-backed artifact ownership. On the first call,
// checkedOwner must be nil. If a current owner exists, the method returns it
// with ErrDVRArtifactOwnerPreflightRequired without inserting a successor.
// After the caller reconciles that exact owner, it retries with its persisted
// proof; insertion is then permitted only while the same evidence is current.
//
// Exact-airing retries are returned before the owner precondition is evaluated,
// preserving idempotency after an uncertain response. A no-owner first call
// inserts under the same output-path lock, so there is no check/insert gap.
func (db *DB) CreateDVRRecordingWithArtifactPreflight(
	ctx context.Context,
	r DVRRecording,
	checkedOwner *DVRArtifactOwnerProof,
	policies ...DVRAdmissionPolicy,
) (DVRRecording, error) {
	return db.createDVRRecordingWithArtifactOwner(
		ctx, r, true, checkedOwner, policies...)
}

func (db *DB) createDVRRecordingWithArtifactOwner(
	ctx context.Context,
	r DVRRecording,
	requireArtifactPreflight bool,
	checkedOwner *DVRArtifactOwnerProof,
	policies ...DVRAdmissionPolicy,
) (DVRRecording, error) {
	if err := normalizeDVRIntent(&r); err != nil {
		return DVRRecording{}, err
	}
	rec, err := db.createDVRRecording(
		ctx, r, requireArtifactPreflight, checkedOwner)
	if err != nil {
		return rec, err
	}
	return db.refreshAndLoadDVRIntent(ctx, rec, policies...)
}

func (db *DB) createDVRRecording(
	ctx context.Context,
	r DVRRecording,
	requireArtifactPreflight bool,
	checkedOwner *DVRArtifactOwnerProof,
) (DVRRecording, error) {
	if r.ID == uuid.Nil {
		r.ID = uuid.New()
	}
	var out DVRRecording
	var outcomeErr error
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockDVROutputPath(ctx, tx, r.OutputPath); err != nil {
			return err
		}

		// This lookup comes before every ownership check: retrying an exact
		// airing is idempotent even if that row is completed or its path now has
		// a validated owner.
		existing, err := findExactDVRRecording(ctx, tx, r)
		if err == nil {
			out = existing
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		active, err := findActiveDVRRecordingForOutputPath(ctx, tx, r.OutputPath)
		if err == nil {
			out = active
			outcomeErr = ErrOutputPathBusy
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		owner, err := findCurrentDVRArtifactForOutputPath(ctx, tx, r.OutputPath, true)
		switch {
		case err == nil && requireArtifactPreflight && checkedOwner == nil:
			// Filesystem inspection must happen before a scheduled successor is
			// visible to ListPendingDVRRecordings. Returning the locked owner
			// performs the discovery half without creating anything claimable.
			out = owner
			outcomeErr = ErrDVRArtifactOwnerPreflightRequired
			return nil
		case err == nil && requireArtifactPreflight &&
			(!checkedOwner.valid() || !checkedOwner.matches(owner)):
			out = owner
			outcomeErr = ErrDVRArtifactOwnerChanged
			return nil
		case err == nil && owner.ArtifactState == "validated":
			out = owner
			outcomeErr = ErrValidatedArtifactExists
			return nil
		case err == nil:
			// Migration 0024 permits only legacy or validated current owners.
			// Legacy is explicitly not proof of integrity, so preserve it as the
			// rollback predecessor without suppressing this new attempt.
			r.ReplacesRecordingID = &owner.ID
			r.ExplicitRerecord = false
		case errors.Is(err, pgx.ErrNoRows) && requireArtifactPreflight && checkedOwner != nil:
			outcomeErr = ErrDVRArtifactOwnerChanged
			return nil
		case errors.Is(err, pgx.ErrNoRows):
			// First artifact for this output path.
		default:
			return err
		}

		inserted, err := insertDVRRecording(ctx, tx, r)
		if errors.Is(err, pgx.ErrNoRows) {
			// A concurrent exact request can use a different caller-supplied path
			// and therefore a different advisory-lock namespace. The database key
			// remains authoritative; load its winner without mutating metadata.
			inserted, err = findExactDVRRecording(ctx, tx, r)
		}
		if err != nil {
			return translateDVRInsertError(err)
		}
		out = inserted
		return nil
	})
	if err != nil {
		return DVRRecording{}, err
	}
	return out, outcomeErr
}

// activeDVRRecordingForOutputPath returns the scheduled/recording row that
// currently owns a given output_path. Migration 0017's unique index makes at
// most one such row possible.
func (db *DB) activeDVRRecordingForOutputPath(ctx context.Context, outputPath string) (DVRRecording, error) {
	return findActiveDVRRecordingForOutputPath(ctx, db.Pool, outputPath)
}

func normalizeDVRIntent(r *DVRRecording) error {
	if r.Priority == 0 {
		r.Priority = DefaultDVRPriority
	}
	if r.Priority < 0 {
		return fmt.Errorf("dvr priority must be positive")
	}
	if strings.TrimSpace(r.OutputPath) == "" {
		return errors.New("dvr output_path is required")
	}
	return nil
}

func lockDVROutputPath(ctx context.Context, tx pgx.Tx, outputPath string) error {
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended('conductor:dvr-output:' || $1, 0)
		)`, outputPath); err != nil {
		return fmt.Errorf("lock DVR output path: %w", err)
	}
	return nil
}

type dvrQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func findExactDVRRecording(ctx context.Context, q dvrQuerier, r DVRRecording) (DVRRecording, error) {
	return scanDVRRecording(q.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
		FROM dvr_recording
		WHERE channel_id=$1 AND scheduled_start=$2 AND title=$3`,
		r.ChannelID, r.ScheduledStart, r.Title))
}

func findActiveDVRRecordingForOutputPath(ctx context.Context, q dvrQuerier, outputPath string) (DVRRecording, error) {
	return scanDVRRecording(q.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
		FROM dvr_recording
		WHERE output_path=$1 AND state IN ('scheduled','recording')`, outputPath))
}

func findCurrentDVRArtifactForOutputPath(
	ctx context.Context,
	q dvrQuerier,
	outputPath string,
	forUpdate bool,
) (DVRRecording, error) {
	lock := ""
	if forUpdate {
		lock = " FOR UPDATE"
	}
	return scanDVRRecording(q.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
		FROM dvr_recording
		WHERE output_path=$1 AND artifact_current`+lock, outputPath))
}

func insertDVRRecording(ctx context.Context, tx pgx.Tx, r DVRRecording) (DVRRecording, error) {
	return scanDVRRecording(tx.QueryRow(ctx, `
		INSERT INTO dvr_recording (
			id, channel_id, program_id, title, sub_title,
			episode_num_xmltv, episode_num_onscreen, is_movie,
			scheduled_start, scheduled_end, state, priority, requested_by,
			output_path, explicit_rerecord, replaces_recording_id
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'scheduled',$11,$12,$13,$14,$15)
		ON CONFLICT (channel_id, scheduled_start, title) DO NOTHING
		RETURNING `+dvrRecordingColumns,
		r.ID, r.ChannelID, r.ProgramID, r.Title, r.SubTitle,
		r.EpisodeNumXMLTV, r.EpisodeNumOnscreen, r.IsMovie,
		r.ScheduledStart, r.ScheduledEnd, r.Priority, r.RequestedBy,
		r.OutputPath, r.ExplicitRerecord, r.ReplacesRecordingID))
}

func translateDVRInsertError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation &&
		pgErr.ConstraintName == "dvr_recording_active_output_path_idx" {
		return ErrOutputPathBusy
	}
	return err
}

func (db *DB) refreshAndLoadDVRIntent(
	ctx context.Context,
	rec DVRRecording,
	policies ...DVRAdmissionPolicy,
) (DVRRecording, error) {
	policy := defaultDVRAdmissionPolicy()
	if len(policies) > 0 {
		policy = policies[0]
	}
	if err := db.RefreshDVRAdmissionForecast(ctx, policy); err != nil {
		// The insert/exact-airing transaction already committed. Forecast is
		// advisory and the scheduler refreshes it periodically; turning this
		// into a creation error tells the caller the request failed while a
		// runnable row exists. Return the durable intent and surface the
		// degraded observation through a process metric instead.
		metrics.DVRAdmissionPostCommitObservationFailures.Inc()
		return rec, nil
	}
	refreshed, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		// The transaction's RETURNING row is sufficient proof of durable
		// creation. A post-commit reload failure has the same false-error shape
		// as a forecast failure and converges on the next scheduler read.
		metrics.DVRAdmissionPostCommitObservationFailures.Inc()
		return rec, nil
	}
	return refreshed, nil
}

// CreateDVRReplacement atomically binds an operator-authorized replacement to
// both the current validated owner and a specific EPG program. It immediately
// creates the scheduled successor; there is no persistent loose authorization
// that a later unrelated re-airing can consume.
func (db *DB) CreateDVRReplacement(
	ctx context.Context,
	ownerID uuid.UUID,
	intent DVRRecording,
	policies ...DVRAdmissionPolicy,
) (DVRRecording, error) {
	if ownerID == uuid.Nil {
		return DVRRecording{}, ErrArtifactNotReplaceable
	}
	if intent.ProgramID == nil || *intent.ProgramID == uuid.Nil {
		return DVRRecording{}, ErrArtifactReplacementMismatch
	}
	if err := normalizeDVRIntent(&intent); err != nil {
		return DVRRecording{}, err
	}
	if intent.ID == uuid.Nil {
		intent.ID = uuid.New()
	}
	policy := defaultDVRAdmissionPolicy()
	if len(policies) > 0 {
		policy = normalizeDVRAdmissionPolicy(policies[0])
	}

	var out DVRRecording
	var outcomeErr error
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockDVROutputPath(ctx, tx, intent.OutputPath); err != nil {
			return err
		}
		// An exact retry proves its target from the already-persisted successor,
		// even after successful promotion demoted ownerID. This keeps an
		// uncertain API response replay-safe without reopening authorization.
		existing, err := findExactDVRRecording(ctx, tx, intent)
		if err == nil {
			if existing.OutputPath == intent.OutputPath && existing.ExplicitRerecord &&
				existing.ReplacesRecordingID != nil && *existing.ReplacesRecordingID == ownerID &&
				existing.ProgramID != nil && *existing.ProgramID == *intent.ProgramID {
				out = existing
				return nil
			}
			out = existing
			outcomeErr = ErrArtifactReplacementMismatch
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		matches, err := dvrReplacementProgramMatchesIntent(ctx, tx, intent)
		if err != nil {
			return err
		}
		if !matches {
			return ErrArtifactReplacementMismatch
		}

		owner, err := scanDVRRecording(tx.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
			FROM dvr_recording WHERE id=$1 FOR UPDATE`, ownerID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrArtifactNotReplaceable
		}
		if err != nil {
			return err
		}
		if owner.State != "completed" || !owner.ArtifactCurrent || owner.ArtifactState != "validated" {
			return ErrArtifactNotReplaceable
		}
		if owner.OutputPath != intent.OutputPath || !sameDVRProgramIdentity(owner, intent) {
			return ErrArtifactReplacementMismatch
		}

		// Every potentially blocking ownership check is complete. Use the
		// database clock while the output-path and owner-row locks remain held;
		// an application-side time check could pass, wait here, then persist an
		// airing whose pre-roll has already begun. The exact-retry branch above
		// intentionally remains replayable after this boundary.
		var targetStillFuture bool
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp() < $1::timestamptz`,
			intent.ScheduledStart.Add(-policy.StartSlack)).Scan(&targetStillFuture); err != nil {
			return err
		}
		if !targetStillFuture {
			return ErrArtifactReplacementTargetNotFuture
		}

		active, err := findActiveDVRRecordingForOutputPath(ctx, tx, intent.OutputPath)
		if err == nil {
			out = active
			outcomeErr = ErrOutputPathBusy
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		intent.ExplicitRerecord = true
		intent.ReplacesRecordingID = &ownerID
		inserted, err := insertDVRRecording(ctx, tx, intent)
		if errors.Is(err, pgx.ErrNoRows) {
			inserted, err = findExactDVRRecording(ctx, tx, intent)
		}
		if err != nil {
			return translateDVRInsertError(err)
		}
		if !inserted.ExplicitRerecord || inserted.ReplacesRecordingID == nil ||
			*inserted.ReplacesRecordingID != ownerID {
			return ErrArtifactReplacementMismatch
		}
		out = inserted
		return nil
	})
	if err != nil {
		return DVRRecording{}, err
	}
	if outcomeErr != nil {
		return out, outcomeErr
	}
	return db.refreshAndLoadDVRIntent(ctx, out, policies...)
}

// sameDVRProgramIdentity prevents an authenticated operator from binding an
// arbitrary future programme to an existing artifact path. Episode numbers
// are preferred because providers sometimes correct subtitles between the
// original airing and a repeat; unnumbered content falls back to title plus
// subtitle, and movies use their title.
func sameDVRProgramIdentity(owner, target DVRRecording) bool {
	if owner.IsMovie != target.IsMovie || owner.Title != target.Title {
		return false
	}
	if owner.IsMovie {
		return true
	}
	if owner.EpisodeNumOnscreen != "" && target.EpisodeNumOnscreen != "" {
		return owner.EpisodeNumOnscreen == target.EpisodeNumOnscreen
	}
	if owner.EpisodeNumXMLTV != "" && target.EpisodeNumXMLTV != "" {
		return owner.EpisodeNumXMLTV == target.EpisodeNumXMLTV
	}
	return owner.SubTitle != "" && owner.SubTitle == target.SubTitle
}

func dvrReplacementProgramMatchesIntent(ctx context.Context, tx pgx.Tx, intent DVRRecording) (bool, error) {
	if intent.ProgramID == nil {
		return false, nil
	}
	var (
		channelID          uuid.UUID
		startAt            time.Time
		endAt              time.Time
		title              string
		subTitle           string
		episodeNumXMLTV    string
		episodeNumOnscreen string
		isMovie            bool
	)
	err := tx.QueryRow(ctx, `
		SELECT channel_id, start_at, end_at, title, sub_title,
		       episode_num_xmltv, episode_num_onscreen, is_movie
		  FROM epg_program
		 WHERE id=$1 AND is_canonical
		 FOR SHARE`, *intent.ProgramID).Scan(
		&channelID, &startAt, &endAt, &title, &subTitle,
		&episodeNumXMLTV, &episodeNumOnscreen, &isMovie)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return channelID == intent.ChannelID && startAt.Equal(intent.ScheduledStart) &&
		endAt.Equal(intent.ScheduledEnd) && title == intent.Title &&
		subTitle == intent.SubTitle && episodeNumXMLTV == intent.EpisodeNumXMLTV &&
		episodeNumOnscreen == intent.EpisodeNumOnscreen && isMovie == intent.IsMovie, nil
}

// ListPendingDVRRecordings returns every scheduled recording within the next
// windowAhead duration. A future admission_retry_at is visible metadata, not a
// filter: every scheduler boundary must arbitrate all due rows chronologically
// so an earlier queued recording cannot be overtaken by a later one.
func (db *DB) ListPendingDVRRecordings(ctx context.Context, windowAhead time.Duration) ([]DVRRecording, error) {
	rows, err := db.Pool.Query(ctx, `SELECT `+dvrRecordingColumns+`
		  FROM dvr_recording
		 WHERE state = 'scheduled'
		   AND scheduled_start <= $1
		 ORDER BY scheduled_start ASC, priority ASC, created_at ASC, id ASC`,
		time.Now().Add(windowAhead))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DVRRecording
	for rows.Next() {
		r, err := scanDVRRecording(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListDVRRecordings returns recordings filtered by state (or all if state=="").
// Ordered most-recently-scheduled first.
func (db *DB) ListDVRRecordings(ctx context.Context, state string, limit int) ([]DVRRecording, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	args := []any{limit}
	where := ""
	if state != "" {
		args = append(args, state)
		where = "WHERE state = $2::dvr_recording_state"
	}
	rows, err := db.Pool.Query(ctx, `SELECT `+dvrRecordingColumns+`
		  FROM dvr_recording
		`+where+`
		 ORDER BY scheduled_start DESC
		 LIMIT $1`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DVRRecording
	for rows.Next() {
		r, err := scanDVRRecording(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (db *DB) GetDVRRecording(ctx context.Context, id uuid.UUID) (DVRRecording, error) {
	r, err := scanDVRRecording(db.Pool.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
		FROM dvr_recording WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return DVRRecording{}, ErrNotFound
	}
	return r, err
}

// GetDVRReplacementByTarget returns only an authorization that was already
// persisted for this exact predecessor/program pair. It lets an authenticated
// API retry bypass mutable filesystem/canonical-guide prechecks without ever
// creating a detached or loose replacement permission.
func (db *DB) GetDVRReplacementByTarget(
	ctx context.Context,
	ownerID, programID uuid.UUID,
) (DVRRecording, error) {
	if ownerID == uuid.Nil || programID == uuid.Nil {
		return DVRRecording{}, ErrNotFound
	}
	rec, err := scanDVRRecording(db.Pool.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
		  FROM dvr_recording
		 WHERE replaces_recording_id=$1
		   AND program_id=$2
		   AND explicit_rerecord
		 ORDER BY created_at, id
		 LIMIT 1`, ownerID, programID))
	if errors.Is(err, pgx.ErrNoRows) {
		return DVRRecording{}, ErrNotFound
	}
	return rec, err
}

// MarkDVRRecording* helpers transition the state machine + record metadata.

// TryMarkDVRRecordingStarted atomically claims a scheduled row after its
// stream reservation exists. false,nil means another scheduler/cancel path
// won; callers must release their reservation without touching the file.
func (db *DB) TryMarkDVRRecordingStarted(ctx context.Context, id uuid.UUID, outputPath string) (bool, error) {
	return db.TryMarkDVRRecordingStartedBefore(ctx, id, outputPath, time.Time{})
}

// TryMarkDVRRecordingStartedBefore is the deadline-aware scheduler claim. A
// zero deadline preserves the legacy behavior; otherwise PostgreSQL's clock
// atomically prevents a reservation returned at the deadline boundary from
// turning into a late recording.
func (db *DB) TryMarkDVRRecordingStartedBefore(
	ctx context.Context,
	id uuid.UUID,
	outputPath string,
	deadline time.Time,
) (bool, error) {
	claimed := false
	var before any
	if !deadline.IsZero() {
		before = deadline
	}
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		// Lock first, then read PostgreSQL's *current wall clock*. `now()` is
		// transaction-stable and an UPDATE qual may be evaluated before waiting
		// on this row, allowing a pre-deadline statement to commit after the
		// bound. The separate post-lock clock_timestamp check closes that seam.
		var state string
		err := tx.QueryRow(ctx, `
			SELECT state::text FROM dvr_recording
			 WHERE id = $1
			 FOR UPDATE`, id).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if state != "scheduled" {
			return nil
		}
		if !deadline.IsZero() {
			var beforeDeadline bool
			if err := tx.QueryRow(ctx,
				`SELECT clock_timestamp() < $1`, deadline).Scan(&beforeDeadline); err != nil {
				return err
			}
			if !beforeDeadline {
				return nil
			}
		}
		ct, err := tx.Exec(ctx, `
			UPDATE dvr_recording
			   SET state = 'recording', started_at = clock_timestamp(), output_path = $2,
			       recording_heartbeat_at = clock_timestamp(),
			       admission_state = 'admitted', admission_retry_at = NULL,
			       admission_reason = '', error = ''
			 WHERE id = $1 AND state = 'scheduled'
			   AND ($3::timestamptz IS NULL OR clock_timestamp() < $3)`,
			id, outputPath, before)
		if err != nil {
			return err
		}
		claimed = ct.RowsAffected() == 1
		return nil
	})
	return claimed, err
}

// MarkDVRRecordingStarted is retained for callers that do not need to
// distinguish a lost claim. New recorder code uses TryMark... directly.
func (db *DB) MarkDVRRecordingStarted(ctx context.Context, id uuid.UUID, outputPath string) error {
	_, err := db.TryMarkDVRRecordingStarted(ctx, id, outputPath)
	return err
}

// QueueDVRRecordingForCapacity records a temporary admission conflict while
// keeping the row retryable. It is idempotent with cancellation/claim races:
// only a still-scheduled row is mutated.
func (db *DB) QueueDVRRecordingForCapacity(ctx context.Context, id uuid.UUID, retryAt time.Time, reason string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET admission_state = 'queued',
		       admission_attempts = admission_attempts + 1,
		       admission_retry_at = $2,
		       admission_reason = $3,
		       error = $3
		 WHERE id = $1 AND state = 'scheduled'`, id, retryAt, reason)
	return err
}

// WakeQueuedDVRRecordings makes runtime-capacity-queued rows immediately
// eligible for the next scheduler pass. Schedule-time forecast conflicts keep
// their padded-start retry metadata until the periodic forecast recomputes
// them. A pool release is only a wakeup hint; admission still revalidates
// capacity transactionally.
func (db *DB) WakeQueuedDVRRecordings(ctx context.Context) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET admission_retry_at = now()
		 WHERE state = 'scheduled' AND admission_state = 'queued'
		   AND admission_retry_at > now()
		   AND admission_reason NOT LIKE $1`, dvrForecastReasonPrefix+"%")
	return err
}

// FailDVRRecordingAdmission terminally fails only a row that is still
// scheduled. A stale scheduler must never overwrite another process's
// successful scheduled->recording claim.
func (db *DB) FailDVRRecordingAdmission(ctx context.Context, id uuid.UUID, reason string) (bool, error) {
	ct, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET state = 'failed', completed_at = now(), error = $2,
		       admission_reason = $2, admission_retry_at = NULL
		 WHERE id = $1 AND state = 'scheduled'`, id, reason)
	return ct.RowsAffected() == 1, err
}

func (db *DB) MarkDVRRecordingProgress(ctx context.Context, id uuid.UUID, bytes int64) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET bytes_written = $2, recording_heartbeat_at = clock_timestamp()
		 WHERE id = $1 AND state = 'recording'`, id, bytes)
	return err
}

// DVRCompletionResult is the durable outcome of one idempotent completion
// operation. OperationOwned is true only when the row carries this caller's
// exact operation UUID; state="completed" alone is not ownership proof.
type DVRCompletionResult struct {
	State          string
	OperationOwned bool
}

// MarkDVRRecordingCompleted performs one replay-safe completion attempt. The
// transaction takes a recording-scoped advisory lock before its data statement
// so a retry that follows a lost COMMIT response gets a fresh READ COMMITTED
// snapshot only after the uncertain predecessor has committed or rolled back.
//
// A recording row is changed only from recording->completed. Replaying the
// same operation observes OperationOwned=true; cancellation and every other
// terminal state are reported without mutation. A completed row belonging to
// another (or legacy unowned) operation is never guessed to be this caller's.
func (db *DB) MarkDVRRecordingCompleted(
	ctx context.Context,
	id, operationID uuid.UUID,
	bytes int64,
) (DVRCompletionResult, error) {
	var result DVRCompletionResult
	if id == uuid.Nil {
		return result, errors.New("DVR recording id is required")
	}
	if operationID == uuid.Nil {
		return result, errors.New("DVR completion operation id is required")
	}
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:dvr-completion:' || $1::text, 0)
			)`, id); err != nil {
			return fmt.Errorf("wait for DVR completion operation: %w", err)
		}

		var persistedOperationID *uuid.UUID
		// Cancellation and failure do not take the completion advisory lock.
		// Lock the row in a separate READ COMMITTED statement so, if either is
		// already updating it, PostgreSQL waits and returns that transaction's
		// latest committed version. A data-modifying CTE with a fallback SELECT
		// would retain its pre-wait statement snapshot and could report stale
		// "recording" after cancellation had durably committed.
		err := tx.QueryRow(ctx, `
			SELECT state::text, completion_operation_id
			  FROM dvr_recording
			 WHERE id = $1
			 FOR UPDATE`, id).Scan(&result.State, &persistedOperationID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if result.State == "recording" {
			if err := tx.QueryRow(ctx, `
				UPDATE dvr_recording
				   SET state = 'completed', completed_at = clock_timestamp(),
				       bytes_written = $2, completion_operation_id = $3
				 WHERE id = $1 AND state = 'recording'
				 RETURNING state::text, completion_operation_id`,
				id, bytes, operationID).Scan(&result.State, &persistedOperationID); err != nil {
				return fmt.Errorf("persist DVR completion operation: %w", err)
			}
		}
		result.OperationOwned = persistedOperationID != nil && *persistedOperationID == operationID
		return nil
	})
	err = db.applyDVRCompletionCommitResultHook(err)
	if err != nil {
		// result was read before COMMIT and is only provisional when the commit
		// response is missing. Returning it beside an error would invite callers
		// to guess state from an outcome PostgreSQL has not confirmed.
		return DVRCompletionResult{}, err
	}
	return result, nil
}

// MarkDVRRecordingFailed is a compare-and-set terminal transition. false,nil
// means an operator cancellation (or another terminal path) linearized first
// and must not be overwritten by recorder cleanup that was already in flight.
func (db *DB) MarkDVRRecordingFailed(ctx context.Context, id uuid.UUID, errMsg string, bytes int64) (bool, error) {
	ct, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET state = 'failed', completed_at = clock_timestamp(), error = $2,
		       bytes_written = $3, recording_heartbeat_at = clock_timestamp(),
		       artifact_stage = CASE
		           WHEN artifact_stage = 'publishing' THEN artifact_stage
		           ELSE 'none'
		       END,
		       artifact_state = CASE
		           WHEN artifact_stage = 'publishing' THEN artifact_state
		           WHEN artifact_state IN ('none', 'validating') THEN 'rejected'
		           ELSE artifact_state
		       END,
		       artifact_error = $2
		 WHERE id = $1 AND state = 'recording'`, id, errMsg, bytes)
	return ct.RowsAffected() == 1, err
}

func (db *DB) MarkDVRRecordingCancelled(ctx context.Context, id uuid.UUID) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET state = 'cancelled', completed_at = clock_timestamp(),
		       recording_heartbeat_at = CASE
		           WHEN state = 'recording' THEN clock_timestamp()
		           ELSE recording_heartbeat_at
		       END,
		       artifact_stage = CASE
		           WHEN artifact_stage = 'publishing' THEN artifact_stage
		           ELSE 'none'
		       END,
		       artifact_state = CASE
		           WHEN artifact_stage = 'publishing' THEN artifact_state
		           WHEN state = 'recording' AND artifact_state IN ('none', 'validating') THEN 'rejected'
		           ELSE artifact_state
		       END,
		       artifact_error = CASE
		           WHEN artifact_stage = 'publishing' OR state = 'scheduled' THEN artifact_error
		           ELSE 'recording cancelled'
		       END
		 WHERE id = $1 AND state IN ('scheduled', 'recording')`, id)
	return err
}

// DVRMediaReport is timestamp-derived validation evidence for a normalized
// artifact. Values are absolute offsets in milliseconds. Format-level MPEG-TS
// duration is deliberately not used because timestamp resets can inflate it by
// hours; DurationMS is the overlapping A/V packet coverage proved by ffprobe.
type DVRMediaReport struct {
	DurationMS      int64
	MaxGapMS        int64
	Discontinuities int
	AVStartDeltaMS  int64
	AVEndDeltaMS    int64
	AVDriftMS       int64
}

var (
	ErrDVRArtifactNotReady             = errors.New("DVR artifact is not ready for this transition")
	ErrDVRArtifactPromotionConflict    = errors.New("DVR artifact promotion lost canonical ownership")
	ErrDVRArtifactCommitOutcomeUnknown = errors.New("DVR artifact promotion commit outcome is unknown")
	ErrDVRPublishRecoveryNotRequired   = errors.New("DVR artifact has no interrupted publish to recover")
	ErrDVRArtifactPublishInProgress    = errors.New("DVR artifact replacement publication is in progress")
	ErrDVRArtifactFilesystemConflict   = errors.New("DVR artifact filesystem identity binding conflict")
)

// inDVRArtifactPromotionTx distinguishes transaction-body failures, which
// PostgreSQL has definitely not committed, from a lost/failed COMMIT response.
// The recorder may roll filesystem publication back immediately for the
// former; only the latter requires preserving the publish seam and replaying
// the same operation token.
func (db *DB) inDVRArtifactPromotionTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.Join(ErrDVRArtifactCommitOutcomeUnknown,
			fmt.Errorf("commit DVR artifact promotion: %w", err))
	}
	if err := db.applyDVRCompletionCommitResultHook(nil); err != nil {
		return errors.Join(ErrDVRArtifactCommitOutcomeUnknown, err)
	}
	return nil
}

func validateDVRMediaReport(report DVRMediaReport, requireCoverage bool) error {
	if report.DurationMS < 0 || report.MaxGapMS < 0 || report.Discontinuities < 0 ||
		report.AVStartDeltaMS < 0 || report.AVEndDeltaMS < 0 || report.AVDriftMS < 0 {
		return errors.New("DVR media report values must be non-negative")
	}
	if requireCoverage && report.DurationMS == 0 {
		return errors.New("validated DVR artifact must have positive A/V coverage")
	}
	return nil
}

// MarkDVRArtifactStage records the filesystem artifact the recorder is
// currently working on. The path is retained on later rejection so operators
// can inspect or recover the exact bytes instead of receiving a generic
// "upstream dead" diagnosis.
func (db *DB) MarkDVRArtifactStage(ctx context.Context, id uuid.UUID, stage, path string) error {
	if id == uuid.Nil {
		return errors.New("DVR recording id is required")
	}
	switch stage {
	case "capturing", "normalizing", "publishing":
	default:
		return fmt.Errorf("invalid DVR artifact stage %q", stage)
	}
	if strings.TrimSpace(path) == "" {
		return errors.New("DVR artifact path is required")
	}
	ct, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET artifact_stage = $2,
		       artifact_state = CASE
		           WHEN $2 = 'normalizing' THEN 'validating'
		           ELSE artifact_state
		       END,
		       artifact_path = $3, artifact_error = '',
		       recording_heartbeat_at = clock_timestamp()
		 WHERE id = $1 AND state = 'recording'`, id, stage, path)
	if err != nil {
		return err
	}
	if ct.RowsAffected() != 1 {
		return ErrDVRArtifactNotReady
	}
	return nil
}

// RejectDVRRecordingArtifact atomically terminalizes a recording whose media
// failed normalization, coverage, A/V alignment, content, or publish checks.
// The current predecessor (if any) is untouched.
func (db *DB) RejectDVRRecordingArtifact(
	ctx context.Context,
	id uuid.UUID,
	reason, path string,
	bytes int64,
	report DVRMediaReport,
) (bool, error) {
	if id == uuid.Nil {
		return false, errors.New("DVR recording id is required")
	}
	if strings.TrimSpace(reason) == "" {
		return false, errors.New("DVR artifact rejection reason is required")
	}
	if bytes < 0 {
		return false, errors.New("DVR artifact bytes must be non-negative")
	}
	if err := validateDVRMediaReport(report, false); err != nil {
		return false, err
	}
	ct, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET state = 'failed', completed_at = clock_timestamp(), error = $2,
		       bytes_written = $4, recording_heartbeat_at = clock_timestamp(),
		       artifact_state = 'rejected', artifact_stage = 'none',
		       artifact_current = false, artifact_path = $3,
		       artifact_bytes = $4, artifact_sha256 = NULL,
		       artifact_filesystem_id = '',
		       artifact_validated_at = NULL, artifact_validator_version = '',
		       artifact_error = $2, media_duration_ms = $5,
		       media_max_gap_ms = $6, media_discontinuities = $7,
		       media_av_start_delta_ms = $8, media_av_end_delta_ms = $9,
		       media_av_drift_ms = $10
		 WHERE id = $1 AND state = 'recording'`,
		id, reason, path, bytes, report.DurationMS, report.MaxGapMS,
		report.Discontinuities, report.AVStartDeltaMS, report.AVEndDeltaMS,
		report.AVDriftMS)
	return ct.RowsAffected() == 1, err
}

// PromoteDVRRecordingArtifact is the validated completion operation. It
// preserves MarkDVRRecordingCompleted's operation-token replay contract while
// atomically transferring canonical ownership only after filesystem publish.
func (db *DB) PromoteDVRRecordingArtifact(
	ctx context.Context,
	id, operationID uuid.UUID,
	bytes int64,
	report DVRMediaReport,
	sha256 []byte,
	filesystemID, validatorVersion, canonicalPath, predecessorBackupPath string,
) (DVRCompletionResult, error) {
	var result DVRCompletionResult
	if id == uuid.Nil {
		return result, errors.New("DVR recording id is required")
	}
	if operationID == uuid.Nil {
		return result, errors.New("DVR completion operation id is required")
	}
	if bytes <= 0 {
		return result, errors.New("validated DVR artifact bytes must be positive")
	}
	if err := validateDVRMediaReport(report, true); err != nil {
		return result, err
	}
	if len(sha256) != 32 {
		return result, errors.New("validated DVR artifact SHA-256 must be exactly 32 bytes")
	}
	filesystemID = strings.TrimSpace(filesystemID)
	if filesystemID == "" {
		return result, errors.New("validated DVR artifact filesystem identity is required")
	}
	if strings.TrimSpace(validatorVersion) == "" {
		return result, errors.New("DVR artifact validator version is required")
	}
	if strings.TrimSpace(canonicalPath) == "" {
		return result, errors.New("DVR artifact canonical path is required")
	}

	err := db.inDVRArtifactPromotionTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:dvr-completion:' || $1::text, 0)
			)`, id); err != nil {
			return fmt.Errorf("wait for DVR artifact completion operation: %w", err)
		}

		// Output ownership is always locked before a DVR row lock. Create and
		// explicit replacement use the same order, avoiding a row/output lock
		// inversion while still retaining the recording-scoped replay fence.
		var storedOutputPath string
		if err := tx.QueryRow(ctx, `SELECT output_path FROM dvr_recording WHERE id=$1`, id).
			Scan(&storedOutputPath); errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		if err := lockDVROutputPath(ctx, tx, storedOutputPath); err != nil {
			return err
		}

		rec, err := scanDVRRecording(tx.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
			FROM dvr_recording WHERE id=$1 FOR UPDATE`, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		result.State = rec.State
		if rec.State != "recording" {
			result.OperationOwned = rec.CompletionOperationID != nil &&
				*rec.CompletionOperationID == operationID
			return nil
		}
		if rec.ArtifactStage != "publishing" {
			return ErrDVRArtifactNotReady
		}
		if rec.OutputPath != canonicalPath || storedOutputPath != canonicalPath {
			return errors.New("DVR artifact canonical path does not match recording output")
		}

		if rec.ReplacesRecordingID != nil {
			if strings.TrimSpace(predecessorBackupPath) == "" ||
				predecessorBackupPath == canonicalPath {
				return errors.New("replacement artifact requires a distinct predecessor backup path")
			}
			predecessor, err := scanDVRRecording(tx.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
				FROM dvr_recording WHERE id=$1 FOR UPDATE`, *rec.ReplacesRecordingID))
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrDVRArtifactPromotionConflict
			}
			if err != nil {
				return err
			}
			if predecessor.State != "completed" || !predecessor.ArtifactCurrent ||
				predecessor.OutputPath != canonicalPath {
				return ErrDVRArtifactPromotionConflict
			}
			validProvenance := (predecessor.ArtifactState == "legacy" && !rec.ExplicitRerecord) ||
				(predecessor.ArtifactState == "validated" && rec.ExplicitRerecord)
			if !validProvenance {
				return ErrDVRArtifactPromotionConflict
			}
			ct, err := tx.Exec(ctx, `
				UPDATE dvr_recording
				   SET artifact_current = false, artifact_state = 'superseded',
				       artifact_path = $2
				 WHERE id = $1 AND artifact_current AND output_path = $3`,
				predecessor.ID, predecessorBackupPath, canonicalPath)
			if err != nil {
				return err
			}
			if ct.RowsAffected() != 1 {
				return ErrDVRArtifactPromotionConflict
			}
		} else {
			if predecessorBackupPath != "" {
				return errors.New("initial artifact promotion cannot name a predecessor backup")
			}
			if _, err := findCurrentDVRArtifactForOutputPath(ctx, tx, canonicalPath, false); err == nil {
				return ErrDVRArtifactPromotionConflict
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}

		persistedOperationID := operationID
		err = tx.QueryRow(ctx, `
			UPDATE dvr_recording
			   SET state = 'completed', completed_at = clock_timestamp(),
			       bytes_written = $2, completion_operation_id = $3,
			       recording_heartbeat_at = clock_timestamp(),
			       artifact_state = 'validated', artifact_stage = 'none',
			       artifact_current = true, artifact_path = $4,
			       artifact_bytes = $2, artifact_sha256 = $5,
			       artifact_filesystem_id = $6,
			       artifact_validated_at = clock_timestamp(),
			       artifact_validator_version = $7, artifact_error = '',
			       media_duration_ms = $8, media_max_gap_ms = $9,
			       media_discontinuities = $10,
			       media_av_start_delta_ms = $11,
			       media_av_end_delta_ms = $12, media_av_drift_ms = $13
			 WHERE id = $1 AND state = 'recording' AND artifact_stage = 'publishing'
			 RETURNING state::text, completion_operation_id`,
			id, bytes, operationID, canonicalPath, sha256, filesystemID, validatorVersion,
			report.DurationMS, report.MaxGapMS, report.Discontinuities,
			report.AVStartDeltaMS, report.AVEndDeltaMS, report.AVDriftMS).
			Scan(&result.State, &persistedOperationID)
		if err != nil {
			return fmt.Errorf("persist validated DVR artifact completion: %w", err)
		}
		result.OperationOwned = persistedOperationID == operationID
		return nil
	})
	if err != nil {
		return DVRCompletionResult{}, err
	}
	return result, nil
}

// ListInterruptedDVRPublishes returns rows whose filesystem publish crossed
// the durable recovery boundary but never cleared it. Startup recovery uses
// their explicit paths/provenance rather than guessing from output existence.
func (db *DB) ListInterruptedDVRPublishes(ctx context.Context) ([]DVRRecording, error) {
	rows, err := db.Pool.Query(ctx, `SELECT `+dvrRecordingColumns+`
		FROM dvr_recording
		WHERE artifact_stage = 'publishing'
		ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DVRRecording
	for rows.Next() {
		r, err := scanDVRRecording(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListCurrentDVRArtifacts returns the small canonical ownership set for
// startup filesystem reconciliation. Database completion alone is not proof
// that an importer or operator has left the named file in place.
func (db *DB) ListCurrentDVRArtifacts(ctx context.Context) ([]DVRRecording, error) {
	rows, err := db.Pool.Query(ctx, `SELECT `+dvrRecordingColumns+`
		FROM dvr_recording
		WHERE artifact_current
		ORDER BY output_path, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DVRRecording
	for rows.Next() {
		rec, err := scanDVRRecording(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// BindCurrentDVRArtifactFilesystem durably records the filesystem that held a
// conclusively inspected canonical artifact. Missing-file reconciliation may
// later demote this owner only while the containing directory is reachable on
// that exact filesystem. The output advisory lock and row lock make the first
// binding exact with respect to replacement publication and owner transfer.
func (db *DB) BindCurrentDVRArtifactFilesystem(
	ctx context.Context,
	id uuid.UUID,
	outputPath, filesystemID string,
) error {
	if id == uuid.Nil || strings.TrimSpace(outputPath) == "" {
		return errors.New("current DVR artifact identity and output path are required")
	}
	filesystemID = strings.TrimSpace(filesystemID)
	if filesystemID == "" {
		return errors.New("current DVR artifact filesystem identity is required")
	}
	return db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockDVROutputPath(ctx, tx, outputPath); err != nil {
			return err
		}
		rec, err := scanDVRRecording(tx.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
			FROM dvr_recording WHERE id=$1 FOR UPDATE`, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !rec.ArtifactCurrent || rec.State != "completed" ||
			(rec.ArtifactState != "legacy" && rec.ArtifactState != "validated") ||
			rec.OutputPath != outputPath ||
			(rec.ArtifactPath != "" && rec.ArtifactPath != outputPath) {
			return ErrDVRArtifactFilesystemConflict
		}
		switch {
		case rec.ArtifactFilesystemID == filesystemID:
			return nil
		case rec.ArtifactFilesystemID != "":
			return ErrDVRArtifactFilesystemConflict
		}
		ct, err := tx.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_filesystem_id=$2
			 WHERE id=$1 AND artifact_current AND artifact_filesystem_id=''`,
			id, filesystemID)
		if err != nil {
			return err
		}
		if ct.RowsAffected() != 1 {
			return ErrDVRArtifactFilesystemConflict
		}
		return nil
	})
}

// DemoteMissingDVRArtifact atomically withdraws one exact current owner after
// the runtime proves its canonical file is absent or no longer matches the
// persisted byte evidence. Any in-flight successor becomes a no-replace first
// publication; if a file reappears after the filesystem check, os.Link still
// fails closed instead of overwriting it.
func (db *DB) DemoteMissingDVRArtifact(
	ctx context.Context,
	id uuid.UUID,
	outputPath, expectedFilesystemID, reason string,
) (bool, error) {
	if id == uuid.Nil || strings.TrimSpace(outputPath) == "" {
		return false, errors.New("current DVR artifact identity and output path are required")
	}
	expectedFilesystemID = strings.TrimSpace(expectedFilesystemID)
	if expectedFilesystemID == "" {
		return false, errors.New("current DVR artifact filesystem identity is required")
	}
	if strings.TrimSpace(reason) == "" {
		return false, errors.New("missing DVR artifact reason is required")
	}
	demoted := false
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if err := lockDVROutputPath(ctx, tx, outputPath); err != nil {
			return err
		}
		rec, err := scanDVRRecording(tx.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
			FROM dvr_recording WHERE id=$1 FOR UPDATE`, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !rec.ArtifactCurrent || rec.OutputPath != outputPath ||
			(rec.ArtifactState != "legacy" && rec.ArtifactState != "validated") {
			return nil
		}
		if rec.ArtifactFilesystemID != expectedFilesystemID {
			return ErrDVRArtifactFilesystemConflict
		}
		// A publishing successor has already made (or is about to make) the
		// filesystem-first canonical swap. Its bytes intentionally differ from
		// the predecessor's evidence until PromoteDVRRecordingArtifact transfers
		// ownership. Never misclassify that bounded seam as external deletion and
		// strip the successor's replacement provenance.
		var publishingSuccessor uuid.UUID
		err = tx.QueryRow(ctx, `
			SELECT id
			  FROM dvr_recording
			 WHERE replaces_recording_id=$1
			   AND artifact_stage='publishing'
			 ORDER BY created_at, id
			 LIMIT 1
			 FOR UPDATE`, id).Scan(&publishingSuccessor)
		switch {
		case err == nil:
			return ErrDVRArtifactPublishInProgress
		case errors.Is(err, pgx.ErrNoRows):
			// No filesystem-first replacement seam is active.
		default:
			return err
		}
		ct, err := tx.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_current=false, artifact_state='missing', artifact_error=$4
			 WHERE id=$1 AND output_path=$2 AND artifact_current
			   AND artifact_filesystem_id=$3`,
			id, outputPath, expectedFilesystemID, reason)
		if err != nil {
			return err
		}
		if ct.RowsAffected() != 1 {
			return nil
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dvr_recording
			   SET replaces_recording_id=NULL, explicit_rerecord=false
			 WHERE replaces_recording_id=$1 AND state IN ('scheduled','recording')`, id); err != nil {
			return err
		}
		demoted = true
		return nil
	})
	return demoted, err
}

// FinishDVRPublishRecovery records that filesystem rollback/quarantine is
// complete. It is replay-safe for an uncertain successful first call.
func (db *DB) FinishDVRPublishRecovery(
	ctx context.Context,
	id uuid.UUID,
	reason, artifactPath string,
) error {
	if id == uuid.Nil {
		return errors.New("DVR recording id is required")
	}
	if strings.TrimSpace(reason) == "" {
		return errors.New("DVR publish recovery reason is required")
	}
	return db.InTx(ctx, func(tx pgx.Tx) error {
		rec, err := scanDVRRecording(tx.QueryRow(ctx, `SELECT `+dvrRecordingColumns+`
			FROM dvr_recording WHERE id=$1 FOR UPDATE`, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if rec.ArtifactStage == "none" && rec.ArtifactState == "rejected" {
			return nil
		}
		if rec.ArtifactStage != "publishing" || rec.State == "completed" || rec.ArtifactCurrent {
			return ErrDVRPublishRecoveryNotRequired
		}
		_, err = tx.Exec(ctx, `
			UPDATE dvr_recording
			   SET state = CASE WHEN state = 'recording' THEN 'failed' ELSE state END,
			       completed_at = CASE
			           WHEN state = 'recording' THEN clock_timestamp()
			           ELSE completed_at
			       END,
			       recording_heartbeat_at = clock_timestamp(),
			       artifact_stage = 'none', artifact_state = 'rejected',
			       artifact_current = false, artifact_path = $3,
			       artifact_sha256 = NULL, artifact_filesystem_id = '',
			       artifact_validated_at = NULL,
			       artifact_validator_version = '', artifact_error = $2,
			       error = CASE
			           WHEN error = '' THEN $2
			           WHEN strpos(error, $2) > 0 THEN error
			           ELSE error || '; ' || $2
			       END
			 WHERE id = $1`, id, reason, artifactPath)
		return err
	})
}

// ListStaleDVRRecordings returns rows beyond the absolute finalization ceiling
// or past the ordinary deadline without a recent recorder heartbeat. For a
// process-local worker at the absolute ceiling, the scheduler first
// persists the distinct failed reason, then cancels the worker; recorder-side
// cancellation is compare-and-set safe and cannot overwrite that failure.
func (db *DB) ListStaleDVRRecordings(
	ctx context.Context,
	maxOverrun, hardGrace, heartbeatTimeout time.Duration,
) ([]DVRRecording, error) {
	if maxOverrun < 0 || hardGrace < 0 || heartbeatTimeout < 0 {
		return nil, errors.New("DVR stale-recording durations must be non-negative")
	}
	rows, err := db.Pool.Query(ctx, `SELECT `+dvrRecordingColumns+`
		FROM dvr_recording
		WHERE state = 'recording'
		  AND (
		      clock_timestamp() > scheduled_end
		          + ($1::bigint * interval '1 second')
		          + ($2::bigint * interval '1 second')
		      OR (
		          clock_timestamp() > scheduled_end
		              + ($1::bigint * interval '1 second')
		          AND COALESCE(recording_heartbeat_at, started_at, created_at)
		              < clock_timestamp() - ($3::bigint * interval '1 second')
		      )
		  )
		ORDER BY scheduled_end, id`,
		int64(maxOverrun/time.Second), int64(hardGrace/time.Second),
		int64(heartbeatTimeout/time.Second))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DVRRecording
	for rows.Next() {
		r, err := scanDVRRecording(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
