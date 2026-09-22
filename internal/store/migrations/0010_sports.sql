-- 0010_sports.sql
-- Phase 5b: sports schedule ingestion + synthetic EPG injection.
--
-- The dispatcharr/SchedulesDirect EPG sources do not cover dedicated
-- sports channels well: NFL PPV 1-16, NHL PPV 1-18, league wrap-up
-- channels, and some PPV broadcasters return generic "Sports
-- Programming" for hours of dead air. Plex's guide is therefore mostly
-- empty for these channels, even when real games are airing.
--
-- This migration adds:
--   - channel_sport_mapping: which channels carry which leagues/teams;
--     populated via admin (auto-detected for well-known channel names).
--   - sports_event_cache: deduped event records ingested from a sports
--     API (TheSportsDB by default). Lookups by (league, kickoff_at)
--     when the worker syncs a channel's schedule.
--   - the worker injects matching events as `epg_program` rows with a
--     `source_hash = 'sports:<event_id>'` so re-runs are idempotent.
--
-- Rationale for the cache table: a single event (e.g. SF @ DAL on
-- Sunday) often airs on multiple channels (NFL Network, FOX, NFL+,
-- Sunday Ticket PPV slot). Caching the event once and joining lets us
-- emit the same metadata to each channel's slot without re-fetching.

CREATE TYPE sport_league AS ENUM (
    'nfl', 'nba', 'nhl', 'mlb', 'mls',
    'ncaaf', 'ncaab',
    'epl', 'laliga', 'bundesliga', 'seriea',
    'f1', 'pga', 'tennis',
    'boxing', 'mma', 'ufc',
    'other'
);

CREATE TABLE channel_sport_mapping (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id  uuid NOT NULL REFERENCES channel(id) ON DELETE CASCADE,
    league      sport_league NOT NULL,
    -- Optional team filter. When non-null, only events involving this team
    -- get synthesised onto this channel (used for "team wrap-up" channels
    -- like Steelers Nation Radio if we ever wire those). NULL = all
    -- league events that broadcaster carries.
    team_name   text NOT NULL DEFAULT '',
    -- Optional broadcaster filter. When non-empty, only events whose
    -- TV-station field contains this string get matched. Useful when
    -- multiple of our channels carry the same league (ESPN vs FS1 vs
    -- TNT — each gets a different broadcaster filter). Case-
    -- insensitive substring match at query time.
    broadcaster_filter text NOT NULL DEFAULT '',
    -- Whether this mapping is active. Keep history rather than delete —
    -- the EPG sync worker honours `enabled = TRUE`.
    enabled     boolean NOT NULL DEFAULT TRUE,
    note        text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (channel_id, league, team_name, broadcaster_filter)
);
CREATE INDEX channel_sport_mapping_league_idx ON channel_sport_mapping (league)
    WHERE enabled;

CREATE TABLE sports_event_cache (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    league       sport_league NOT NULL,
    -- Provider's stable id (TheSportsDB idEvent, ESPN eventID, etc.)
    -- So re-syncs upsert in place rather than duplicating.
    external_id  text NOT NULL,
    home_team    text NOT NULL,
    away_team    text NOT NULL,
    kickoff_at   timestamptz NOT NULL,
    duration_min integer NOT NULL DEFAULT 180, -- 3h default; varies by sport
    venue        text NOT NULL DEFAULT '',
    -- TV-station / broadcaster string from the API. Used by the
    -- mapping match-up logic. May be empty.
    broadcaster  text NOT NULL DEFAULT '',
    -- Free-form raw json for ad-hoc fields (e.g. season/week) the
    -- emitter may want without schema migrations.
    extra        jsonb NOT NULL DEFAULT '{}'::jsonb,
    fetched_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (league, external_id)
);
CREATE INDEX sports_event_cache_kickoff_idx ON sports_event_cache (kickoff_at);
CREATE INDEX sports_event_cache_league_idx ON sports_event_cache (league, kickoff_at);

-- Provenance: when the EPG worker injects a sports event as an
-- epg_program row, it sets source_hash = 'sports:<league>:<event_id>'.
-- The dedupe index on epg_program (channel_id, start_at, source_hash)
-- already ensures idempotency without extra schema work.
