-- 0008_enrichment.sql
-- Phase 3: TMDb / TVDB / TheSportsDB enrichment per spec §6.3.
--
-- The enrichment data itself lives in epg_program_enrichment (shipped in
-- migration 0003). This migration adds:
--   - tmdb_response_cache: 30-day TTL on raw TMDb responses, keyed by
--     (kind, query_hash). Lets the matcher re-evaluate without re-fetching
--     from TMDb (which has a rate limit + we want to be polite).
--   - manual_enrichment_match: operator overrides for programs the matcher
--     repeatedly gets wrong. (channel_id + title) → fixed (tmdb_id, kind).

CREATE TYPE tmdb_kind AS ENUM ('movie', 'tv', 'episode');

CREATE TABLE tmdb_response_cache (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind        tmdb_kind NOT NULL,
    query_hash  text NOT NULL,                -- sha256 of the normalized query input
    query_label text NOT NULL DEFAULT '',     -- human-readable for the dashboard
    response    jsonb NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL DEFAULT (now() + interval '30 days')
);
CREATE UNIQUE INDEX tmdb_cache_kind_hash_idx ON tmdb_response_cache (kind, query_hash);
CREATE INDEX tmdb_cache_expires_idx ON tmdb_response_cache (expires_at);

CREATE TABLE manual_enrichment_match (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id   uuid NOT NULL REFERENCES channel(id) ON DELETE CASCADE,
    title_norm   text NOT NULL,               -- lowercase + collapsed whitespace
    is_movie     boolean NOT NULL,            -- locked at override-time
    tmdb_id      integer,
    tvdb_id      integer,
    imdb_id      text,
    note         text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (channel_id, title_norm)
);

-- Sliding-window rate limit counter for TMDb. TMDb's free tier is ~50 req/s
-- but in practice we throttle to ~5 req/s so bursts don't spike. The
-- enrichment worker reads + decrements this row; refilled every minute.
-- Single-row table keeps the bookkeeping cheap.
CREATE TABLE enrichment_rate_state (
    id          smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    requests_made integer NOT NULL DEFAULT 0,
    window_started_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO enrichment_rate_state (id) VALUES (1) ON CONFLICT DO NOTHING;
