-- 0017_dvr_active_output_path.sql
--
-- Stop two recordings from writing the same file at the same time.
--
-- output_path is derived from series/season/episode only, but the existing
-- idempotency key is (channel_id, scheduled_start, title). A second airing of
-- the same episode therefore has a different scheduled_start, sails past that
-- key, and lands a second row pointing at the SAME file. Both recorders then
-- open <output_path>.partial with O_CREATE|O_WRONLY|O_TRUNC: the later one
-- truncates the file the earlier one is still writing, both keep writing at
-- independent offsets, the first to finish renames it away and marks itself
-- completed, and the loser's rename fails with ENOENT.
--
-- Observed 2026-08-10 on House of the Dragon S03E08: two rows 75 minutes
-- apart produced a 2.9 GB file that probes as 26.3 hours long with 190 h264
-- decode errors, and burned one of only two provider stream slots for 80
-- minutes doing it.
--
-- Scoped to the ACTIVE states only. Completed rows are deliberately excluded:
-- a re-record after a failed or cancelled attempt stays legal, and the 20
-- historical completed-row collisions need no data surgery to deploy this.

-- Preflight. CREATE UNIQUE INDEX aborts if two active rows already share an
-- output_path — precisely the state this bug produces. The reconciler mints
-- rows hourly, so "it was clean when I checked" is not a deploy guarantee, and
-- Migrate() failing means Conductor does not start at all. Resolve any
-- collision first, keeping one owner per path:
--   * an in-flight 'recording' row outranks a merely 'scheduled' one
--   * otherwise the earliest airing wins (created_at breaks exact ties)
-- Losers become 'cancelled' rather than 'failed': nothing went wrong with
-- them, they were never going to be a legal second writer on that file.
-- This whole file runs in one transaction (see store.Migrate), so the cleanup
-- and the index are applied atomically.
WITH ranked AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY output_path
               ORDER BY (state = 'recording') DESC, scheduled_start, created_at
           ) AS rn
      FROM dvr_recording
     WHERE state IN ('scheduled', 'recording')
       AND output_path <> ''
)
UPDATE dvr_recording AS r
   SET state        = 'cancelled',
       completed_at = COALESCE(r.completed_at, now()),
       error        = 'superseded by another airing already owning this output_path (migration 0017)'
  FROM ranked
 WHERE ranked.id = r.id
   AND ranked.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS dvr_recording_active_output_path_idx
    ON dvr_recording (output_path)
    WHERE state IN ('scheduled', 'recording') AND output_path <> '';
