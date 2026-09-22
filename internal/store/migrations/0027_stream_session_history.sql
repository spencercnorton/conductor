-- 0027_stream_session_history.sql
--
-- active_stream is working state: SweepOrphans deletes a row once its teardown
-- completes, so a finished live-TV session leaves no trace. DVR keeps rich
-- forensics in dvr_recording; live TV kept nothing, and on 2026-08-30 a
-- user-reported failure could not be explained because the only record of the
-- session had already been deleted.
--
-- This is the archive the reap writes into. Deliberately WITHOUT foreign keys:
-- history must outlive the channel, source or credential it references. Other
-- archive failures roll back the reap so the source row remains retryable.
--
-- upstream_url is deliberately NOT copied — it embeds provider credentials
-- (http://host/live/USER/PASS/ID.ts) and history is long-lived.

CREATE TABLE IF NOT EXISTS stream_session (
    id                uuid PRIMARY KEY,
    channel_id        uuid        NOT NULL,
    channel_source_id uuid        NOT NULL,
    credential_id     uuid        NOT NULL,
    started_at        timestamptz NOT NULL,
    ended_at          timestamptz NOT NULL,
    bytes_out         bigint      NOT NULL DEFAULT 0,
    archived_at       timestamptz NOT NULL DEFAULT now()
);

-- "what ran in this window" and "what did this channel do" are the two
-- questions this table exists to answer.
CREATE INDEX IF NOT EXISTS stream_session_started_idx
    ON stream_session (started_at DESC);
CREATE INDEX IF NOT EXISTS stream_session_channel_idx
    ON stream_session (channel_id, started_at DESC);
