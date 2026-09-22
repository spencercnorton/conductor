-- 0018_ppv_sync_channel_state.sql
--
-- A rolling deploy can briefly run two ppvsync workers. Their provider
-- catalogue snapshots may complete out of order, so transaction ordering alone
-- is not enough: an older idle snapshot that commits last could otherwise
-- delete a newer event. Persist the newest provider-observation timestamp per
-- channel so every destructive PPV mutation can reject stale snapshots.

CREATE TABLE ppv_sync_channel_state (
    channel_id  uuid PRIMARY KEY REFERENCES channel(id) ON DELETE CASCADE,
    observed_at timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Remove any overlap left by the historical split occupancy/delete/upsert
-- flow before installing a permanent backstop. The boolean inequality is
-- true only for an overlapping ppv-off/non-off pair, so ordinary real-guide
-- overlaps and adjacent placeholder blocks remain legal. Deferral lets a new
-- exact event be inserted and its old placeholder removed in one transaction.
DELETE FROM epg_program AS off_row
 WHERE off_row.source_hash LIKE 'ppv-off:%'
   AND EXISTS (
       SELECT 1
         FROM epg_program AS real_row
        WHERE real_row.channel_id = off_row.channel_id
          AND real_row.source_hash NOT LIKE 'ppv-off:%'
          AND real_row.start_at < off_row.end_at
          AND real_row.end_at > off_row.start_at
   );

CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE epg_program
    ADD CONSTRAINT epg_program_no_ppv_off_real_overlap
    EXCLUDE USING gist (
        channel_id WITH =,
        tstzrange(start_at, end_at, '[)') WITH &&,
        ((source_hash LIKE 'ppv-off:%')) WITH <>
    )
    DEFERRABLE INITIALLY DEFERRED;

-- The old binary in a rolling deployment does not know about the application
-- advisory lock or observation high-water table. Make PPV mutation authority
-- fail closed at the database boundary: new code sets this transaction-local
-- channel marker only after acquiring the matching advisory lock. A legacy
-- direct DELETE/UPSERT therefore cannot erase or replace newer PPV state after
-- this migration commits.
CREATE FUNCTION guard_serialized_ppv_epg_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_channel uuid;
    guarded_mutation boolean := false;
BEGIN
    affected_channel := CASE WHEN TG_OP = 'DELETE' THEN OLD.channel_id ELSE NEW.channel_id END;
    IF TG_OP = 'INSERT' THEN
        guarded_mutation := NEW.source_hash LIKE 'ppv-%';
    ELSIF TG_OP = 'DELETE' THEN
        guarded_mutation := OLD.source_hash LIKE 'ppv-%';
    ELSE
        guarded_mutation := (OLD.source_hash LIKE 'ppv-%' OR NEW.source_hash LIKE 'ppv-%')
            AND (OLD.channel_id IS DISTINCT FROM NEW.channel_id
              OR OLD.start_at IS DISTINCT FROM NEW.start_at
              OR OLD.end_at IS DISTINCT FROM NEW.end_at
              OR OLD.title IS DISTINCT FROM NEW.title
              OR OLD.source_hash IS DISTINCT FROM NEW.source_hash
              OR OLD.source_priority IS DISTINCT FROM NEW.source_priority);
    END IF;
    -- FK cascades originate from a separately-authorised channel deletion and
    -- run nested. Direct legacy epg_program statements run at depth one.
    IF guarded_mutation AND pg_trigger_depth() = 1 THEN
        IF current_setting('conductor.epg_channel_lock', true)
           IS DISTINCT FROM affected_channel::text THEN
            RAISE EXCEPTION 'PPV EPG mutation for channel % requires serialized authority', affected_channel
                USING ERRCODE = '55000';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER epg_program_guard_serialized_ppv_mutation
BEFORE INSERT OR UPDATE OR DELETE ON epg_program
FOR EACH ROW EXECUTE FUNCTION guard_serialized_ppv_epg_mutation();
