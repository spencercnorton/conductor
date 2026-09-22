package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PendingEnrichmentRow is what the enrich worker reads from the DB.
// Mirrors enrich.PendingProgram but kept in store to avoid a circular
// dependency (enrich would otherwise import store types).
type PendingEnrichmentRow struct {
	ID                 uuid.UUID
	ChannelID          uuid.UUID
	Title              string
	OriginalAirDate    *time.Time
	IsMovie            bool
	EpisodeNumOnscreen string
	EnrichmentInputKey string
}

// ListPendingEnrichment returns up to limit programs whose enrichment is
// not yet attempted (or whose transient-error delay elapsed). Deferred rows
// sort behind ready work so one unavailable provider cannot starve the guide.
//
// Window: only programs whose start_at is within (now - 2h, now + 14d) —
// the same window emitted by /xmltv.xml. Past airings are intentionally
// excluded: they're invisible in Plex's guide so enrichment doesn't help
// the user, and they'd otherwise hog every pass since the worker selects
// oldest-first. PurgeOldPrograms reaps them on its own schedule.
func (db *DB) ListPendingEnrichment(ctx context.Context, limit int) ([]PendingEnrichmentRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	// Keep this digest in exact lockstep with the mutable fields read by
	// enrich.Enricher. Subtitle and description deliberately are not selected:
	// the matcher does not read them, so their correction must not discard an
	// otherwise valid in-flight title/year/episode lookup.
	rows, err := db.Pool.Query(ctx, `
		SELECT id, channel_id, title, original_air_date, is_movie, episode_num_onscreen,
		       jsonb_build_array(title, original_air_date, is_movie,
		                         episode_num_onscreen)::text
		  FROM epg_program
		 WHERE enrichment_status = 'pending'
		   AND is_canonical
		   AND enrichment_retry_at <= now()
		   AND start_at >= now() - interval '2 hours'
		   AND start_at <= now() + interval '14 days'
		 ORDER BY enrichment_retry_at ASC, start_at ASC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingEnrichmentRow
	for rows.Next() {
		var r PendingEnrichmentRow
		if err := rows.Scan(&r.ID, &r.ChannelID, &r.Title, &r.OriginalAirDate,
			&r.IsMovie, &r.EpisodeNumOnscreen, &r.EnrichmentInputKey); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// EnrichmentResult is the typed view of an epg_program_enrichment upsert.
// Caller (enrich worker) builds it from the matched candidate.
type EnrichmentResult struct {
	TMDbID        int
	TVDBID        int
	IMDbID        string
	PosterURL     string
	BackdropURL   string
	Overview      string
	Confidence    float64
	MatchedSource string
}

// UpsertEnrichmentResult writes the enrichment row + flips the program's
// enrichment_status to 'matched'. Idempotent on program_id.
func (db *DB) UpsertEnrichmentResult(ctx context.Context, programID uuid.UUID, r EnrichmentResult) error {
	return db.UpsertEnrichmentResultGuarded(ctx, programID, "", r)
}

func (db *DB) UpsertEnrichmentResultGuarded(ctx context.Context, programID uuid.UUID, expectedInputKey string, r EnrichmentResult) error {
	return db.InTx(ctx, func(tx pgx.Tx) error {
		// Serialize with researched overrides. If an override won while this
		// network lookup was in flight, it must remain manual and untouched.
		var locked int
		if err := tx.QueryRow(ctx, `
			SELECT 1 FROM epg_program
			 WHERE id = $1
			   AND ($2 = '' OR (enrichment_status = 'pending'
			       AND jsonb_build_array(title, original_air_date, is_movie,
			                             episode_num_onscreen)::text = $2))
			 FOR UPDATE`, programID, expectedInputKey).Scan(&locked); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		// cast_json + sports_event_json are jsonb columns and must always
		// receive valid JSON. We don't populate cast/sports here yet
		// (Phase 4 territory), so emit the JSON null literal — the SQL
		// then NULLIF()'s it back to a SQL NULL. Without this an empty
		// string went to Postgres which rejected with SQLSTATE 22P02
		// ("invalid input syntax for type json"), blocking every match
		// from persisting.
		castJSON := []byte("null")
		sportsJSON := []byte("null")
		_, err := tx.Exec(ctx, `
			INSERT INTO epg_program_enrichment
			    (program_id, tmdb_id, tvdb_id, imdb_id, poster_url, backdrop_url,
			     overview, cast_json, sports_event_json, match_confidence)
			VALUES ($1, NULLIF($2,0), NULLIF($3,0), NULLIF($4,''), $5, $6, $7,
			        NULLIF($8::jsonb, 'null'::jsonb),
			        NULLIF($9::jsonb, 'null'::jsonb), $10)
			ON CONFLICT (program_id) DO UPDATE
			   SET tmdb_id     = EXCLUDED.tmdb_id,
			       tvdb_id     = EXCLUDED.tvdb_id,
			       imdb_id     = EXCLUDED.imdb_id,
			       poster_url  = EXCLUDED.poster_url,
			       backdrop_url= EXCLUDED.backdrop_url,
			       overview    = EXCLUDED.overview,
			       match_confidence = EXCLUDED.match_confidence,
			       matched_at  = now()`,
			programID, r.TMDbID, r.TVDBID, r.IMDbID, r.PosterURL, r.BackdropURL,
			r.Overview, string(castJSON), string(sportsJSON), r.Confidence)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			UPDATE epg_program
			   SET enrichment_status = 'matched'
			 WHERE id = $1`, programID)
		return err
	})
}

// MarkEnrichmentFailed flips enrichment_status to 'failed' so the worker
// stops retrying. Operator can re-queue via manual override or by
// updating the row directly.
func (db *DB) MarkEnrichmentFailed(ctx context.Context, programID uuid.UUID, _ string) error {
	return db.MarkEnrichmentFailedGuarded(ctx, programID, "")
}

func (db *DB) MarkEnrichmentFailedGuarded(ctx context.Context, programID uuid.UUID, expectedInputKey string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE epg_program
		   SET enrichment_status = 'failed'
		 WHERE id = $1 AND enrichment_status = 'pending'
		   AND ($2 = '' OR jsonb_build_array(title, original_air_date, is_movie,
		                                      episode_num_onscreen)::text = $2)`, programID, expectedInputKey)
	return err
}

// DeferEnrichment keeps a transient provider/transport failure retryable
// without letting the same earliest rows monopolize every worker batch.
func (db *DB) DeferEnrichment(ctx context.Context, programID uuid.UUID, delay time.Duration) error {
	return db.DeferEnrichmentGuarded(ctx, programID, "", delay)
}

func (db *DB) DeferEnrichmentGuarded(ctx context.Context, programID uuid.UUID, expectedInputKey string, delay time.Duration) error {
	if delay <= 0 {
		delay = 30 * time.Minute
	}
	_, err := db.Pool.Exec(ctx, `
		UPDATE epg_program
		   SET enrichment_status = 'pending',
		       enrichment_retry_at = now() + ($2::bigint * interval '1 millisecond')
		 WHERE id = $1 AND enrichment_status = 'pending'
		   AND ($3 = '' OR jsonb_build_array(title, original_air_date, is_movie,
		                                      episode_num_onscreen)::text = $3)`, programID, delay.Milliseconds(), expectedInputKey)
	return err
}

// ─────────────────────── TMDb response cache ───────────────────────

// GetTMDbCache reads a cache entry; ok=false on miss or expired.
func (db *DB) GetTMDbCache(ctx context.Context, kind, queryHash string) (json.RawMessage, bool, error) {
	var body []byte
	err := db.Pool.QueryRow(ctx, `
		SELECT response::text
		  FROM tmdb_response_cache
		 WHERE kind = $1::tmdb_kind
		   AND query_hash = $2
		   AND expires_at > now()`, kind, queryHash).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return json.RawMessage(body), true, nil
}

// PutTMDbCache writes a cache entry (upsert by kind+query_hash).
func (db *DB) PutTMDbCache(ctx context.Context, kind, queryHash, queryLabel string, response json.RawMessage) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO tmdb_response_cache (kind, query_hash, query_label, response)
		VALUES ($1::tmdb_kind, $2, $3, $4::jsonb)
		ON CONFLICT (kind, query_hash) DO UPDATE
		   SET response = EXCLUDED.response,
		       fetched_at = now(),
		       expires_at = now() + interval '30 days'`,
		kind, queryHash, queryLabel, string(response))
	return err
}

// ─────────────────────── Manual enrichment override ───────────────

type ManualEnrichmentMatch struct {
	ID        uuid.UUID
	ChannelID uuid.UUID
	TitleNorm string
	IsMovie   bool
	TMDbID    int
	TVDBID    int
	IMDbID    string
	Note      string
	CreatedAt time.Time
}

// LookupManualMatch checks for an operator override before TMDb runs.
// titleNorm should already be normalized (lowercase, collapsed whitespace,
// stop words removed) — callers should use enrich.NormalizeForMatch via
// the public helper exposed there.
func (db *DB) LookupManualMatch(ctx context.Context, channelID uuid.UUID, titleNorm string, isMovie bool) (tmdbID, tvdbID int, imdbID string, ok bool, err error) {
	row := db.Pool.QueryRow(ctx, `
		SELECT COALESCE(tmdb_id, 0), COALESCE(tvdb_id, 0), COALESCE(imdb_id, '')
		  FROM manual_enrichment_match
		 WHERE channel_id = $1 AND title_norm = $2 AND is_movie = $3
		 LIMIT 1`, channelID, strings.TrimSpace(titleNorm), isMovie)
	err = row.Scan(&tmdbID, &tvdbID, &imdbID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, "", false, nil
	}
	if err != nil {
		return 0, 0, "", false, err
	}
	return tmdbID, tvdbID, imdbID, true, nil
}

// CreateManualMatch inserts (or updates) an override.
func (db *DB) CreateManualMatch(ctx context.Context, m ManualEnrichmentMatch) (ManualEnrichmentMatch, error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO manual_enrichment_match
		    (id, channel_id, title_norm, is_movie, tmdb_id, tvdb_id, imdb_id, note)
		VALUES ($1, $2, $3, $4, NULLIF($5,0), NULLIF($6,0), NULLIF($7,''), $8)
		ON CONFLICT (channel_id, title_norm) DO UPDATE
		   SET is_movie = EXCLUDED.is_movie,
		       tmdb_id  = EXCLUDED.tmdb_id,
		       tvdb_id  = EXCLUDED.tvdb_id,
		       imdb_id  = EXCLUDED.imdb_id,
		       note     = EXCLUDED.note
		RETURNING created_at`,
		m.ID, m.ChannelID, strings.TrimSpace(m.TitleNorm), m.IsMovie,
		m.TMDbID, m.TVDBID, m.IMDbID, m.Note)
	if err := row.Scan(&m.CreatedAt); err != nil {
		return ManualEnrichmentMatch{}, err
	}
	return m, nil
}

// ListManualMatches returns all overrides for the dashboard.
func (db *DB) ListManualMatches(ctx context.Context) ([]ManualEnrichmentMatch, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, channel_id, title_norm, is_movie,
		       COALESCE(tmdb_id, 0), COALESCE(tvdb_id, 0), COALESCE(imdb_id, ''),
		       note, created_at
		  FROM manual_enrichment_match
		 ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ManualEnrichmentMatch
	for rows.Next() {
		var m ManualEnrichmentMatch
		if err := rows.Scan(&m.ID, &m.ChannelID, &m.TitleNorm, &m.IsMovie,
			&m.TMDbID, &m.TVDBID, &m.IMDbID, &m.Note, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PurgeExpiredTMDbCache drops cache rows past their TTL. Run periodically
// (the enrich worker can call this once per pass — cheap).
func (db *DB) PurgeExpiredTMDbCache(ctx context.Context) (int64, error) {
	tag, err := db.Pool.Exec(ctx,
		`DELETE FROM tmdb_response_cache WHERE expires_at <= now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ─────────────────────── TVDb cache (Phase 3b) ───────────────────────

// Same shape as TMDb cache but on a separate table — the kind enums are
// different (TMDb: movie/tv/episode; TVDb: series/episode/movie).

func (db *DB) GetTVDbCache(ctx context.Context, kind, queryHash string) (json.RawMessage, bool, error) {
	var body []byte
	err := db.Pool.QueryRow(ctx, `
		SELECT response::text
		  FROM tvdb_response_cache
		 WHERE kind = $1::tvdb_kind
		   AND query_hash = $2
		   AND expires_at > now()`, kind, queryHash).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return json.RawMessage(body), true, nil
}

func (db *DB) PutTVDbCache(ctx context.Context, kind, queryHash, queryLabel string, response json.RawMessage) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO tvdb_response_cache (kind, query_hash, query_label, response)
		VALUES ($1::tvdb_kind, $2, $3, $4::jsonb)
		ON CONFLICT (kind, query_hash) DO UPDATE
		   SET response = EXCLUDED.response,
		       fetched_at = now(),
		       expires_at = now() + interval '30 days'`,
		kind, queryHash, queryLabel, string(response))
	return err
}
