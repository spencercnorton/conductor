package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type PosterCandidate struct {
	ProgramID          uuid.UUID
	ChannelID          uuid.UUID
	Title              string
	SubTitle           string
	Category           []string
	IsMovie            bool
	IsLive             bool
	SourceHash         string
	EnrichmentState    string
	GeneratedPosterURL string
	StartAt            time.Time
}

// ListPosterCandidates returns guide rows that still lack usable artwork.
// Pending rows are included so a researched override can win immediately;
// the generated fallback worker itself waits for conventional enrichment to
// reach failed/matched before spending GPU time.
func (db *DB) ListPosterCandidates(ctx context.Context, limit int) ([]PosterCandidate, error) {
	if limit <= 0 || limit > 20000 {
		limit = 10000
	}
	rows, err := db.Pool.Query(ctx, `
		SELECT p.id, p.channel_id, p.title, p.sub_title, p.category,
		       p.is_movie, p.is_live, p.source_hash,
		       p.enrichment_status::text, p.generated_poster_url, p.start_at
		  FROM epg_program p
		  LEFT JOIN epg_program_enrichment e ON e.program_id = p.id
		 WHERE p.is_canonical
		   AND p.start_at >= now() - interval '2 hours'
		   AND p.start_at <= now() + interval '14 days'
		   AND (
		       (p.source_poster_url = ''
		        AND (p.enrichment_status NOT IN ('matched', 'manual')
		             OR COALESCE(e.poster_url, '') = ''))
		       OR (p.enrichment_status = 'manual'
		           AND COALESCE(e.poster_url, '') LIKE '/posters/assets/researched/%')
		   )
		 ORDER BY p.start_at ASC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]PosterCandidate, 0, limit)
	for rows.Next() {
		var c PosterCandidate
		if err := rows.Scan(&c.ProgramID, &c.ChannelID, &c.Title, &c.SubTitle,
			&c.Category, &c.IsMovie, &c.IsLive, &c.SourceHash,
			&c.EnrichmentState, &c.GeneratedPosterURL, &c.StartAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

type PosterAsset struct {
	IdentityKey   string    `json:"identity_key"`
	Kind          string    `json:"kind"`
	Status        string    `json:"status"`
	SourceURL     string    `json:"source_url,omitempty"`
	LocalPath     string    `json:"local_path,omitempty"`
	ContentSHA256 string    `json:"content_sha256,omitempty"`
	Prompt        string    `json:"prompt,omitempty"`
	Model         string    `json:"model,omitempty"`
	RemoteJobID   string    `json:"remote_job_id,omitempty"`
	Attempts      int       `json:"attempts"`
	RetryAt       time.Time `json:"retry_at"`
	LastError     string    `json:"last_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (db *DB) EnsurePosterAsset(ctx context.Context, a PosterAsset) (PosterAsset, error) {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO poster_asset
		    (identity_key, kind, source_url, prompt, model)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (identity_key) DO NOTHING`,
		strings.TrimSpace(a.IdentityKey), a.Kind, strings.TrimSpace(a.SourceURL),
		a.Prompt, a.Model)
	if err != nil {
		return PosterAsset{}, err
	}
	return db.GetPosterAsset(ctx, a.IdentityKey)
}

func (db *DB) GetPosterAsset(ctx context.Context, identityKey string) (PosterAsset, error) {
	var a PosterAsset
	err := db.Pool.QueryRow(ctx, `
		SELECT identity_key, kind, status, source_url, local_path,
		       content_sha256, prompt, model, remote_job_id, attempts, retry_at,
		       last_error, created_at, updated_at
		  FROM poster_asset
		 WHERE identity_key = $1`, strings.TrimSpace(identityKey)).Scan(
		&a.IdentityKey, &a.Kind, &a.Status, &a.SourceURL, &a.LocalPath,
		&a.ContentSHA256, &a.Prompt, &a.Model, &a.RemoteJobID, &a.Attempts, &a.RetryAt,
		&a.LastError, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return PosterAsset{}, ErrNotFound
	}
	return a, err
}

func (db *DB) MarkPosterAssetReady(ctx context.Context, identityKey, localPath, contentSHA string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE poster_asset
		   SET status = 'ready', local_path = $2, content_sha256 = $3,
		       remote_job_id = '', last_error = '', updated_at = now()
		 WHERE identity_key = $1`, identityKey, localPath, contentSHA)
	return err
}

func (db *DB) MarkPosterAssetSubmitted(ctx context.Context, identityKey, jobID string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE poster_asset
		   SET status = 'pending', remote_job_id = $2, last_error = '', updated_at = now()
		 WHERE identity_key = $1`, identityKey, strings.TrimSpace(jobID))
	return err
}

func (db *DB) ClearPosterAssetRemoteJob(ctx context.Context, identityKey string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE poster_asset SET remote_job_id = '', updated_at = now()
		 WHERE identity_key = $1`, identityKey)
	return err
}

func (db *DB) MarkPosterAssetFailed(ctx context.Context, identityKey, reason string, retryAt time.Time) error {
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	_, err := db.Pool.Exec(ctx, `
		UPDATE poster_asset
		   SET status = 'failed', attempts = attempts + 1, retry_at = $2,
		       last_error = $3, updated_at = now()
		 WHERE identity_key = $1`, identityKey, retryAt, reason)
	return err
}

type PosterOverride struct {
	ID           uuid.UUID   `json:"id"`
	ChannelID    *uuid.UUID  `json:"channel_id,omitempty"`
	TitleNorm    string      `json:"title_norm"`
	SubTitleNorm string      `json:"subtitle_norm,omitempty"`
	IsMovie      bool        `json:"is_movie"`
	AssetKey     string      `json:"asset_key"`
	Note         string      `json:"note,omitempty"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
	Asset        PosterAsset `json:"asset"`
}

// UpsertPosterOverride stores a researched source and its matching rule in a
// single transaction. The worker downloads and validates SourceURL later.
func (db *DB) UpsertPosterOverride(ctx context.Context, o PosterOverride, sourceURL string) (PosterOverride, error) {
	if o.ID == uuid.Nil {
		o.ID = uuid.New()
	}
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO poster_asset (identity_key, kind, source_url)
			VALUES ($1, 'researched', $2)
			ON CONFLICT (identity_key) DO UPDATE
			   SET source_url = EXCLUDED.source_url,
			       status = CASE
			           WHEN poster_asset.source_url IS DISTINCT FROM EXCLUDED.source_url
			           THEN 'pending' ELSE poster_asset.status END,
			       retry_at = CASE
			           WHEN poster_asset.source_url IS DISTINCT FROM EXCLUDED.source_url
			           THEN now() ELSE poster_asset.retry_at END,
			       updated_at = now()`, o.AssetKey, strings.TrimSpace(sourceURL)); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			INSERT INTO programme_poster_override
			    (id, channel_id, title_norm, subtitle_norm, is_movie, asset_key, note)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (channel_id, title_norm, subtitle_norm, is_movie) DO UPDATE
			   SET asset_key = EXCLUDED.asset_key, note = EXCLUDED.note,
			       updated_at = now()
			RETURNING id, created_at, updated_at`,
			o.ID, o.ChannelID, strings.TrimSpace(o.TitleNorm),
			strings.TrimSpace(o.SubTitleNorm), o.IsMovie, o.AssetKey,
			o.Note).Scan(&o.ID, &o.CreatedAt, &o.UpdatedAt)
	})
	if err != nil {
		return PosterOverride{}, err
	}
	o.Asset, err = db.GetPosterAsset(ctx, o.AssetKey)
	return o, err
}

func (db *DB) ListPosterOverrides(ctx context.Context) ([]PosterOverride, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT o.id, o.channel_id, o.title_norm, o.subtitle_norm, o.is_movie, o.asset_key,
		       o.note, o.created_at, o.updated_at,
		       a.identity_key, a.kind, a.status, a.source_url, a.local_path,
		       a.content_sha256, a.prompt, a.model, a.remote_job_id, a.attempts, a.retry_at,
		       a.last_error, a.created_at, a.updated_at
		  FROM programme_poster_override o
		  JOIN poster_asset a ON a.identity_key = o.asset_key
		 ORDER BY o.created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PosterOverride
	for rows.Next() {
		var o PosterOverride
		if err := rows.Scan(&o.ID, &o.ChannelID, &o.TitleNorm, &o.SubTitleNorm, &o.IsMovie,
			&o.AssetKey, &o.Note, &o.CreatedAt, &o.UpdatedAt,
			&o.Asset.IdentityKey, &o.Asset.Kind, &o.Asset.Status,
			&o.Asset.SourceURL, &o.Asset.LocalPath, &o.Asset.ContentSHA256,
			&o.Asset.Prompt, &o.Asset.Model, &o.Asset.RemoteJobID, &o.Asset.Attempts,
			&o.Asset.RetryAt, &o.Asset.LastError, &o.Asset.CreatedAt,
			&o.Asset.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// ApplyPosterToProgram attaches a durable local asset. Researched/manual art
// belongs to enrichment; the XMLTV emitter keeps it below source-feed art.
// Generated art has its own lower-precedence program column so it cannot mark
// conventional enrichment complete or outrank real artwork that appears later.
func (db *DB) ApplyPosterToProgram(ctx context.Context, programID uuid.UUID, expectedSourceHash, localURL string, manual bool) (bool, error) {
	if !manual {
		tag, err := db.Pool.Exec(ctx, `
			UPDATE epg_program p
			   SET generated_poster_url = $2, updated_at = now()
			 WHERE p.id = $1
			   AND p.source_hash = $3
			   AND p.source_poster_url = ''
			   AND NOT EXISTS (
			       SELECT 1 FROM epg_program_enrichment e
			        WHERE e.program_id = p.id
			          AND p.enrichment_status IN ('matched', 'manual')
			          AND e.poster_url <> ''
			   )`, programID, localURL, expectedSourceHash)
		return tag.RowsAffected() > 0, err
	}
	changed := false
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		var currentHash, currentPoster string
		if err := tx.QueryRow(ctx, `
			SELECT p.source_hash, COALESCE(e.poster_url, '')
			  FROM epg_program p
			  LEFT JOIN epg_program_enrichment e ON e.program_id = p.id
			 WHERE p.id = $1
			 FOR UPDATE OF p`, programID).Scan(&currentHash, &currentPoster); err != nil {
			return err
		}
		if currentHash != expectedSourceHash {
			return nil
		}
		if currentPoster != localURL {
			if _, err := tx.Exec(ctx, `
				INSERT INTO epg_program_enrichment (program_id, poster_url)
				VALUES ($1, $2)
				ON CONFLICT (program_id) DO UPDATE
				   SET poster_url = EXCLUDED.poster_url, matched_at = now()`,
				programID, localURL); err != nil {
				return err
			}
			changed = true
		}
		tag, err := tx.Exec(ctx, `
			UPDATE epg_program
			   SET enrichment_status = 'manual', generated_poster_url = '', updated_at = now()
			 WHERE id = $1
			   AND (enrichment_status <> 'manual' OR generated_poster_url <> '')`, programID)
		if err == nil && tag.RowsAffected() > 0 {
			changed = true
		}
		return err
	})
	return changed, err
}

func (db *DB) ClearGeneratedPosterForProgram(ctx context.Context, programID uuid.UUID, expectedSourceHash string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE epg_program SET generated_poster_url = '', updated_at = now()
		 WHERE id = $1 AND source_hash = $2`, programID, expectedSourceHash)
	return err
}
