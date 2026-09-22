-- 0023_epg_canonical_snapshots.sql
--
-- XMLTV feeds are snapshots, not append-only event streams.  Keep every
-- source's current candidates (including lower-priority fallbacks), but expose
-- exactly one non-overlapping canonical schedule to Plex and DVR consumers.

ALTER TABLE epg_source
    ADD COLUMN snapshot_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN snapshot_applied_at timestamptz;

-- Symbolic XMLTV identities must be one-to-one. Numeric feed IDs also fall
-- back to channel.number; those cross-domain collisions are detected at
-- lookup time because PostgreSQL cannot express that relationship as a simple
-- unique index.
CREATE UNIQUE INDEX channel_epg_channel_id_nonempty_idx
    ON channel (epg_channel_id)
    WHERE epg_channel_id <> '';

-- A provider lineup snapshot can remove a station entirely. Preserve the
-- channel owned by every successfully published station so that removal can
-- withdraw only that station's direct candidates even though it no longer
-- appears in the provider response. Existing mapped state is backfilled; a
-- null remains valid only for pre-publication, ambiguous, or historically
-- unmapped state.
ALTER TABLE sd_station_state
    ADD COLUMN channel_id uuid REFERENCES channel(id) ON DELETE CASCADE;

-- Match lookupChannelByEPGID's exact publication contract: symbolic identity
-- plus Go-float-compatible numeric channel fallback, with UNION semantics when
-- both resolve to the same channel. A symbolic/numeric collision deliberately
-- leaves provenance null rather than guessing which historic channel owned the
-- station snapshot. The temporary parser turns invalid, infinite, NaN, and
-- out-of-range provider IDs into NULL just as the runtime resolver does.
CREATE OR REPLACE FUNCTION pg_temp.conductor_epg_numeric_id(raw_id text)
RETURNS numeric
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    parsed double precision;
BEGIN
    -- strconv.ParseFloat rejects surrounding whitespace; PostgreSQL's float
    -- input accepts it. Fail closed here so migration cannot invent numeric
    -- ownership that the live resolver would reject.
    IF raw_id <> btrim(raw_id) THEN
        RETURN NULL;
    END IF;
    BEGIN
        parsed := raw_id::double precision;
    EXCEPTION
        WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            RETURN NULL;
    END;
    IF parsed IN ('NaN'::double precision,
                  'Infinity'::double precision,
                  '-Infinity'::double precision) THEN
        RETURN NULL;
    END IF;
    RETURN parsed::numeric;
END
$$;

WITH station_candidates AS (
    SELECT state.sd_lineup_id, state.sd_station_id, channel.id AS channel_id
      FROM sd_station_state state
      JOIN channel
        ON channel.epg_channel_id <> ''
       AND channel.epg_channel_id = state.sd_station_id
    UNION
    SELECT state.sd_lineup_id, state.sd_station_id, channel.id AS channel_id
      FROM sd_station_state state
      JOIN channel
        ON channel.number = pg_temp.conductor_epg_numeric_id(state.sd_station_id)
), resolved AS (
    SELECT sd_lineup_id, sd_station_id, min(channel_id::text)::uuid AS channel_id
      FROM station_candidates
     GROUP BY sd_lineup_id, sd_station_id
    HAVING count(*) = 1
)
UPDATE sd_station_state state
   SET channel_id = resolved.channel_id
  FROM resolved
 WHERE state.sd_lineup_id = resolved.sd_lineup_id
   AND state.sd_station_id = resolved.sd_station_id;
DROP FUNCTION pg_temp.conductor_epg_numeric_id(text);
CREATE INDEX sd_station_state_channel_idx
    ON sd_station_state (channel_id)
    WHERE channel_id IS NOT NULL;

ALTER TABLE epg_program
    -- Source removal must go through the repository so affected channels can
    -- promote their retained fallback candidates before the source vanishes.
    ADD COLUMN epg_source_id uuid REFERENCES epg_source(id) ON DELETE RESTRICT,
    ADD COLUMN source_generation bigint,
    -- Unknown direct SQL writers should remain visible but fail closed on the
    -- exclusion constraint. Candidate-aware repository inserts explicitly
    -- start false and resolve the complete channel before commit.
    ADD COLUMN is_canonical boolean NOT NULL DEFAULT true,
    ADD COLUMN is_legacy boolean NOT NULL DEFAULT true,
    -- Canonical episode fields remain materialized for every existing consumer,
    -- but a copied exact-slot supplement must stay distinguishable from the
    -- winner's own metadata.  Canonicalization can then replace or withdraw a
    -- stale supplement instead of treating it as permanent source data.
    ADD COLUMN episode_num_xmltv_supplemented boolean NOT NULL DEFAULT false,
    ADD COLUMN episode_num_onscreen_supplemented boolean NOT NULL DEFAULT false;

-- Rows written by the synthetic workers have durable identities outside the
-- XMLTV poller.  Preserve them while the first post-upgrade feed snapshots
-- replace the otherwise-unattributable historical XMLTV rows.  Only explicit
-- producer namespaces prove provenance; priority alone is ambiguous because
-- old XMLTV and Schedules Direct rows both used unnamespaced hashes.
UPDATE epg_program
   SET is_legacy = false
 WHERE source_hash LIKE 'ppv-%'
    OR source_hash LIKE 'sports:%'
    OR source_hash LIKE 'manual:%'
    OR source_hash LIKE 'sd:%';

DROP INDEX epg_program_chan_start_idx;

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Independent XMLTV sources may describe the same start and must coexist so a
-- fallback can be promoted after the preferred source deletes or moves a row.
CREATE UNIQUE INDEX epg_program_xmltv_source_slot_idx
    ON epg_program (epg_source_id, channel_id, start_at)
    WHERE epg_source_id IS NOT NULL;

-- Non-XMLTV workers retain the established exact-slot arbitration semantics.
CREATE UNIQUE INDEX epg_program_direct_slot_idx
    ON epg_program (channel_id, start_at)
    WHERE epg_source_id IS NULL;

-- Establish a deterministic, overlap-free initial view before the startup
-- refresh attributes fresh XMLTV snapshots.  Higher-priority rows win; within
-- a priority, earlier starts and then longer rows win.  The runtime resolver
-- applies the same whole-program invariant with stronger source grouping.
-- Install the final partial exclusion index while the initial canonical set is
-- empty so each greedy probe is channel/range indexed even on retained-history
-- tables. The same constraint then remains the fail-closed runtime boundary.
UPDATE epg_program SET is_canonical = false;
ALTER TABLE epg_program
    ADD CONSTRAINT epg_program_canonical_no_overlap
    EXCLUDE USING gist (
        channel_id WITH =,
        tstzrange(start_at, end_at, '[)') WITH &&
    ) WHERE (is_canonical)
    DEFERRABLE INITIALLY IMMEDIATE;

DO $$
DECLARE
    candidate record;
BEGIN
    FOR candidate IN
        SELECT id, channel_id, start_at, end_at
          FROM epg_program
         WHERE end_at > start_at
         ORDER BY channel_id,
                  source_priority,
                  is_legacy,
                  start_at,
                  end_at DESC,
                  source_hash,
                  id
    LOOP
        IF NOT EXISTS (
            SELECT 1
             FROM epg_program selected
             WHERE selected.channel_id = candidate.channel_id
               AND selected.is_canonical
               AND tstzrange(selected.start_at, selected.end_at, '[)') &&
                   tstzrange(candidate.start_at, candidate.end_at, '[)')
        ) THEN
            UPDATE epg_program SET is_canonical = true WHERE id = candidate.id;
        END IF;
    END LOOP;
END
$$;

-- A monotonic output revision makes Last-Modified honest for deletions and
-- winner changes, whose surviving programme timestamps may move backwards.
CREATE TABLE epg_revision (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO epg_revision (singleton) VALUES (true);

-- Force one unconditional post-upgrade fetch.  A 304 against the pre-snapshot
-- representation would otherwise leave unowned legacy rows indefinitely.
UPDATE epg_source SET last_etag = '', last_modified = '';

-- Pre-migration Schedules Direct hashes did not identify their producer, so
-- no row can be proved to be the current SD candidate.  Force every station
-- through one post-upgrade publish using the explicit sd: namespace.
UPDATE sd_station_state SET last_md5 = '';
