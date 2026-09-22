-- 0003_epg.sql
-- EPG ingest tables. Enrichment is a separate table so re-ingesting XMLTV
-- doesn't blow away matched TMDb/TVDB/SportsDB work.

CREATE TYPE enrichment_status AS ENUM ('pending', 'matched', 'failed', 'manual');

CREATE TABLE epg_program (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id           uuid NOT NULL REFERENCES channel(id) ON DELETE CASCADE,
    start_at             timestamptz NOT NULL,
    end_at               timestamptz NOT NULL,
    title                text NOT NULL,
    sub_title            text NOT NULL DEFAULT '',
    description          text NOT NULL DEFAULT '',
    category             text[] NOT NULL DEFAULT '{}',
    episode_num_xmltv    text NOT NULL DEFAULT '',
    episode_num_onscreen text NOT NULL DEFAULT '',
    original_air_date    date,
    is_movie             boolean NOT NULL DEFAULT false,
    is_live              boolean NOT NULL DEFAULT false,
    is_new               boolean NOT NULL DEFAULT false,
    is_premiere          boolean NOT NULL DEFAULT false,
    is_finale            boolean NOT NULL DEFAULT false,
    rating               text NOT NULL DEFAULT '',
    source_hash          text NOT NULL,                  -- content hash of source XMLTV entry
    enrichment_status    enrichment_status NOT NULL DEFAULT 'pending'
);
CREATE INDEX epg_program_window_idx ON epg_program (channel_id, start_at, end_at);
CREATE INDEX epg_program_enrichment_idx ON epg_program (enrichment_status) WHERE enrichment_status IN ('pending','failed');
CREATE UNIQUE INDEX epg_program_dedupe_idx ON epg_program (channel_id, start_at, source_hash);

CREATE TABLE epg_program_enrichment (
    program_id        uuid PRIMARY KEY REFERENCES epg_program(id) ON DELETE CASCADE,
    tmdb_id           integer,
    tvdb_id           integer,
    imdb_id           text,
    poster_url        text NOT NULL DEFAULT '',
    backdrop_url      text NOT NULL DEFAULT '',
    overview          text NOT NULL DEFAULT '',
    cast_json         jsonb,
    sports_event_json jsonb,
    match_confidence  real NOT NULL DEFAULT 0.0,
    matched_at        timestamptz NOT NULL DEFAULT now()
);

-- Episode history per channel for "we've seen this before" detection.
-- 90-day rolling window (cleanup job to be added in Phase 4).
CREATE TABLE channel_episode_seen (
    channel_id  uuid NOT NULL REFERENCES channel(id) ON DELETE CASCADE,
    episode_num text NOT NULL,
    first_seen  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, episode_num)
);
