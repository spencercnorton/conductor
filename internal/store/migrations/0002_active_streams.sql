-- 0002_active_streams.sql
-- Active stream registry + per-client refcount.
-- Phase 1 will lease credentials and insert active_stream rows in the same tx.

CREATE TYPE stream_state AS ENUM ('starting', 'running', 'draining', 'dead');

CREATE TABLE active_stream (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id        uuid NOT NULL REFERENCES channel(id) ON DELETE RESTRICT,
    channel_source_id uuid NOT NULL REFERENCES channel_source(id) ON DELETE RESTRICT,
    credential_id     uuid NOT NULL REFERENCES provider_credential(id) ON DELETE RESTRICT,
    upstream_url      text NOT NULL,                 -- with creds substituted
    started_at        timestamptz NOT NULL DEFAULT now(),
    last_heartbeat    timestamptz NOT NULL DEFAULT now(),
    client_count      integer NOT NULL DEFAULT 0,
    state             stream_state NOT NULL DEFAULT 'starting',
    bytes_out         bigint NOT NULL DEFAULT 0,
    upstream_pid      integer
);
CREATE INDEX active_stream_credential_idx
    ON active_stream (credential_id) WHERE state IN ('starting', 'running');
CREATE INDEX active_stream_channel_idx
    ON active_stream (channel_id) WHERE state IN ('starting', 'running');

CREATE TABLE stream_client (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    active_stream_id  uuid NOT NULL REFERENCES active_stream(id) ON DELETE CASCADE,
    remote_addr       inet NOT NULL,
    user_agent        text NOT NULL DEFAULT '',
    joined_at         timestamptz NOT NULL DEFAULT now(),
    last_seen         timestamptz NOT NULL DEFAULT now(),
    bytes_sent        bigint NOT NULL DEFAULT 0
);
CREATE INDEX stream_client_active_idx ON stream_client (active_stream_id);
CREATE INDEX stream_client_last_seen_idx ON stream_client (last_seen);

-- Convenience view for the dashboard.
CREATE VIEW v_credential_load AS
SELECT
    c.id                         AS credential_id,
    c.provider_id,
    c.username,
    c.max_streams,
    COUNT(s.id) FILTER (WHERE s.state IN ('starting','running')) AS active_count,
    c.max_streams - COUNT(s.id) FILTER (WHERE s.state IN ('starting','running')) AS free_slots
FROM provider_credential c
LEFT JOIN active_stream s ON s.credential_id = c.id
WHERE c.enabled
GROUP BY c.id;
