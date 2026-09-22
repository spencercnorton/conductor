-- 0009_tvdb_and_sd.sql
-- Phase 3b: TVDB fallback + Schedules Direct.
--
--   TVDB: same shape as TMDb cache — kind/query_hash → response, 30d TTL.
--   Schedules Direct: their own auth (username/password → daily token),
--     their own lineup model (ZIP-code-based), their own schedule format.
--     Fits awkwardly into the XMLTV ingest pipeline so we model it
--     separately: one sd_credentials row, one or more sd_lineup rows
--     (each tied to a Conductor channel via channel.epg_channel_id).

-- ─────────────────────── TVDB cache (same shape as TMDb) ───────────

CREATE TYPE tvdb_kind AS ENUM ('series', 'episode', 'movie');

CREATE TABLE tvdb_response_cache (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind        tvdb_kind NOT NULL,
    query_hash  text NOT NULL,
    query_label text NOT NULL DEFAULT '',
    response    jsonb NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL DEFAULT (now() + interval '30 days')
);
CREATE UNIQUE INDEX tvdb_cache_kind_hash_idx ON tvdb_response_cache (kind, query_hash);
CREATE INDEX tvdb_cache_expires_idx ON tvdb_response_cache (expires_at);

-- ─────────────────────── Schedules Direct ───────────────────────

-- Single-row credentials table (only one SD account ever, per home lab).
-- The token is daily-rotating; we cache it here so we don't hit
-- /token on every request.
CREATE TABLE sd_credentials (
    id              smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    username        text NOT NULL,
    password_sha1   text NOT NULL,                 -- SD wants the password as SHA-1
    cached_token    text NOT NULL DEFAULT '',
    token_fetched_at timestamptz,
    last_success_at timestamptz,
    last_error      text NOT NULL DEFAULT '',
    enabled         boolean NOT NULL DEFAULT true
);

-- One row per SD lineup the operator subscribes to. SD lineups are
-- ZIP-code-based and tied to the operator's account. Each lineup has
-- N stations; we map each station to a Conductor channel via the
-- channel.epg_channel_id field (set to the SD station_id).
CREATE TABLE sd_lineup (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sd_lineup_id text NOT NULL UNIQUE,            -- e.g. "USA-OTA-80127"
    name         text NOT NULL,
    location     text NOT NULL DEFAULT '',
    enabled      boolean NOT NULL DEFAULT true,
    last_pulled_at timestamptz,
    last_status  text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Per-station ingest state so we can incrementally fetch only stations
-- that have changed (SD provides per-station md5 hashes).
CREATE TABLE sd_station_state (
    sd_lineup_id    text NOT NULL REFERENCES sd_lineup(sd_lineup_id) ON DELETE CASCADE,
    sd_station_id   text NOT NULL,
    last_md5        text NOT NULL DEFAULT '',
    last_pulled_at  timestamptz,
    PRIMARY KEY (sd_lineup_id, sd_station_id)
);
