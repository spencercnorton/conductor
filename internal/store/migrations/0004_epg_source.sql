-- 0004_epg_source.sql
-- XMLTV ingest config + audit. Phase 2.
--
-- One epg_source row per upstream XMLTV URL. Multiple sources can fan into
-- the same channel via channel.epg_channel_id mapping; conflicts resolved
-- by source priority (lower wins, ties broken by latest source_hash).

CREATE TABLE epg_source (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text NOT NULL,
    url             text NOT NULL,
    auth_header     text NOT NULL DEFAULT '',  -- e.g., "Authorization: Basic ..."
    priority        integer NOT NULL DEFAULT 100,
    enabled         boolean NOT NULL DEFAULT true,
    last_etag       text NOT NULL DEFAULT '',  -- for If-None-Match polling
    last_modified   text NOT NULL DEFAULT '',  -- for If-Modified-Since polling
    last_fetched_at timestamptz,
    last_status     text NOT NULL DEFAULT '',  -- "ok" | "no_change" | "error: ..."
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX epg_source_enabled_priority_idx ON epg_source (enabled, priority);

CREATE TABLE epg_ingest_run (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    sources_attempted integer NOT NULL DEFAULT 0,
    sources_ok        integer NOT NULL DEFAULT 0,
    sources_no_change integer NOT NULL DEFAULT 0,
    sources_failed    integer NOT NULL DEFAULT 0,
    programs_added    integer NOT NULL DEFAULT 0,
    programs_updated  integer NOT NULL DEFAULT 0,
    programs_unchanged integer NOT NULL DEFAULT 0,
    error             text NOT NULL DEFAULT ''
);
CREATE INDEX epg_ingest_run_started_idx ON epg_ingest_run (started_at DESC);
