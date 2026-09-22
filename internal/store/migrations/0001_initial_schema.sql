-- 0001_initial_schema.sql
-- Phase 1 schema. See docs/spec/dispatcharr-replacement-spec.md §3 for rationale.
-- Phase 0 doesn't run migrations; this file ships now so reviewers can
-- judge the data model up front.

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

-- Providers and credentials -------------------------------------------------
CREATE TYPE provider_kind AS ENUM ('m3u_xtream', 'm3u_plain', 'stalker');

CREATE TABLE provider (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text NOT NULL,
    kind        provider_kind NOT NULL,
    base_url    text NOT NULL,
    notes       text NOT NULL DEFAULT '',
    enabled     boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- One provider can have N credential sets, each with its own slot budget.
-- The stream allocator picks credentials at request time (NOT at boot)
-- based on current load and priority. This is the structural fix for
-- Dispatcharr's multi-instance-per-credential workaround.
CREATE TABLE provider_credential (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_id  uuid NOT NULL REFERENCES provider(id) ON DELETE CASCADE,
    username     text NOT NULL,
    password_enc bytea NOT NULL,        -- AES-GCM, key from CONDUCTOR_CRED_KEY
    max_streams  integer NOT NULL CHECK (max_streams > 0),
    priority     integer NOT NULL DEFAULT 100,
    notes        text NOT NULL DEFAULT '',
    enabled      boolean NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX provider_credential_lookup_idx
    ON provider_credential (provider_id, enabled, priority);

-- Channels and sources ------------------------------------------------------
CREATE TABLE channel (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    number          numeric(6,1) NOT NULL UNIQUE,
    name            text NOT NULL,
    call_sign       text NOT NULL DEFAULT '',
    logo_url        text NOT NULL DEFAULT '',
    group_tag       text NOT NULL DEFAULT '',
    enabled         boolean NOT NULL DEFAULT true,
    epg_channel_id  text NOT NULL DEFAULT ''   -- maps to <channel id="..."> in XMLTV
);

CREATE TABLE channel_source (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id      uuid NOT NULL REFERENCES channel(id) ON DELETE CASCADE,
    provider_id     uuid NOT NULL REFERENCES provider(id) ON DELETE RESTRICT,
    upstream_url    text NOT NULL,                  -- URL or template with $USER/$PASS placeholders
    priority        integer NOT NULL DEFAULT 100,   -- 0 = primary, higher = failover
    health_score    real NOT NULL DEFAULT 1.0,      -- maintained by stream worker
    last_failure_at timestamptz,
    enabled         boolean NOT NULL DEFAULT true
);
CREATE INDEX channel_source_health_idx
    ON channel_source (channel_id, enabled, priority, health_score DESC);

-- Lineups -------------------------------------------------------------------
CREATE TABLE lineup (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text NOT NULL UNIQUE,
    device_uuid text NOT NULL UNIQUE   -- HDHomeRun device UUID Plex sees
);

CREATE TABLE lineup_channel (
    lineup_id   uuid NOT NULL REFERENCES lineup(id) ON DELETE CASCADE,
    channel_id  uuid NOT NULL REFERENCES channel(id) ON DELETE CASCADE,
    number      numeric(6,1) NOT NULL,    -- can override channel.number per lineup
    position    integer NOT NULL,
    PRIMARY KEY (lineup_id, channel_id)
);
CREATE UNIQUE INDEX lineup_channel_number_idx
    ON lineup_channel (lineup_id, number);
