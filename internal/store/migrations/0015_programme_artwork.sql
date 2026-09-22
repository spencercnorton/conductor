-- 0015_programme_artwork.sql
--
-- Preserve artwork supplied directly by XMLTV feeds.  Provider artwork is
-- deliberately kept on epg_program (rather than the enrichment table): it is
-- source metadata and must survive a TMDb/TVmaze miss without masquerading as
-- an enrichment match.

ALTER TABLE epg_program
    ADD COLUMN source_poster_url text NOT NULL DEFAULT '',
    ADD COLUMN generated_poster_url text NOT NULL DEFAULT '',
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN enrichment_retry_at timestamptz NOT NULL DEFAULT '-infinity';

-- Existing validators describe payloads Conductor parsed before programme
-- artwork existed in its model. Force one normal worker fetch after deploy;
-- otherwise a valid 304 can postpone the entire backfill indefinitely.
UPDATE epg_source SET last_etag = '', last_modified = '';

-- Durable cache index for both researched and generated posters.  The image
-- bytes live on the existing Conductor data volume; this row makes retries,
-- cache reuse, and future-airing rebinding survive process restarts.
CREATE TABLE poster_asset (
    identity_key   text PRIMARY KEY,
    kind           text NOT NULL CHECK (kind IN ('researched', 'generated')),
    status         text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'ready', 'failed')),
    source_url     text NOT NULL DEFAULT '',
    local_path     text NOT NULL DEFAULT '',
    content_sha256 text NOT NULL DEFAULT '',
    prompt         text NOT NULL DEFAULT '',
    model          text NOT NULL DEFAULT '',
    remote_job_id  text NOT NULL DEFAULT '',
    attempts       integer NOT NULL DEFAULT 0,
    retry_at       timestamptz NOT NULL DEFAULT now(),
    last_error     text NOT NULL DEFAULT '',
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX poster_asset_pending_idx
    ON poster_asset (retry_at, updated_at)
    WHERE status IN ('pending', 'failed');

-- Researched overrides are title-based and may be global or constrained to a
-- channel.  Channel scoping is important for generic names such as "News".
CREATE TABLE programme_poster_override (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id   uuid REFERENCES channel(id) ON DELETE CASCADE,
    title_norm   text NOT NULL,
    subtitle_norm text NOT NULL DEFAULT '',
    is_movie     boolean NOT NULL DEFAULT false,
    asset_key    text NOT NULL REFERENCES poster_asset(identity_key) ON DELETE RESTRICT,
    note         text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (channel_id, title_norm, subtitle_norm, is_movie)
);
