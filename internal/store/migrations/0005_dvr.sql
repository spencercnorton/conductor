-- 0005_dvr.sql
-- DVR-as-indexer: Conductor exposes a Torznab indexer that searches the EPG;
-- when *arr "grabs" a result, Conductor schedules a recording during the
-- program window and writes the file where *arr expects to find it.
--
-- Not in the original spec (§1 marked DVR out of scope, deferring to Plex DVR)
-- — added at user request 2026-05-06. Documented as an intentional pivot in
-- docs/enhancements.md.

CREATE TYPE dvr_recording_state AS ENUM (
    'scheduled',  -- in DB, not yet at start time
    'recording',  -- recorder goroutine actively writing bytes
    'completed',  -- recording finished cleanly, file at output_path
    'failed',     -- start hit but recording errored partway
    'cancelled'   -- operator (or *arr) explicitly cancelled
);

CREATE TABLE dvr_recording (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id      uuid NOT NULL REFERENCES channel(id) ON DELETE RESTRICT,
    program_id      uuid REFERENCES epg_program(id) ON DELETE SET NULL,
    -- Denormalize program metadata so Sonarr's import succeeds even if
    -- the source EPG row gets purged by retention.
    title           text NOT NULL,
    sub_title       text NOT NULL DEFAULT '',
    episode_num_xmltv    text NOT NULL DEFAULT '',
    episode_num_onscreen text NOT NULL DEFAULT '',
    is_movie        boolean NOT NULL DEFAULT false,
    -- Window
    scheduled_start timestamptz NOT NULL,
    scheduled_end   timestamptz NOT NULL,
    -- Lifecycle
    state           dvr_recording_state NOT NULL DEFAULT 'scheduled',
    requested_by    text NOT NULL DEFAULT '',  -- "torznab:sonarr" | "admin:spencer" | etc.
    -- Output
    output_path     text NOT NULL DEFAULT '',
    bytes_written   bigint NOT NULL DEFAULT 0,
    started_at      timestamptz,
    completed_at    timestamptz,
    error           text NOT NULL DEFAULT '',
    -- Audit
    created_at      timestamptz NOT NULL DEFAULT now(),
    -- Unique guard: don't double-schedule the same airing if Sonarr re-grabs
    UNIQUE (channel_id, scheduled_start, title)
);
CREATE INDEX dvr_recording_state_window_idx
    ON dvr_recording (state, scheduled_start)
    WHERE state IN ('scheduled', 'recording');

-- Per-search audit so the operator can see what *arr asked for and what
-- Conductor returned. Useful for diagnosing "why didn't Sonarr grab this".
-- Retention sweep handled by the same purge job as epg_program.
CREATE TABLE dvr_indexer_search_log (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    requested_at timestamptz NOT NULL DEFAULT now(),
    remote_addr  inet,
    user_agent   text NOT NULL DEFAULT '',
    query        text NOT NULL DEFAULT '',
    type         text NOT NULL DEFAULT '',     -- "search" | "tvsearch" | "movie" | "caps"
    season       integer,
    episode      integer,
    year         integer,
    matches      integer NOT NULL DEFAULT 0
);
CREATE INDEX dvr_indexer_search_log_recent_idx
    ON dvr_indexer_search_log (requested_at DESC);
