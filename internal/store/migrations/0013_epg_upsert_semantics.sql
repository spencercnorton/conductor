-- 0013_epg_upsert_semantics.sql
-- Audit 2026-06-09 E3. The old dedupe key (channel_id, start_at, source_hash)
-- bakes the title into the conflict target (title feeds source_hash), so a
-- title change inserts a second overlapping row and a metadata-only change
-- updates nothing. Re-key to one row per (channel_id, start_at) with
-- priority-guarded DO UPDATE — the "lower priority wins, ties broken by
-- latest source_hash" rule promised in 0004's header but never implemented.

-- Which priority wrote each row. Existing rows get the weakest standing
-- (100) so the next pass from any real source refreshes them in place.
ALTER TABLE epg_program ADD COLUMN source_priority integer NOT NULL DEFAULT 100;

-- One-time dedupe: collapse rows sharing (channel_id, start_at). Keeper is
-- max(id) — arbitrary but deterministic; dropped rows shed their enrichment
-- via ON DELETE CASCADE and the keeper re-enriches on its next status pass.
DELETE FROM epg_program p
 USING epg_program q
 WHERE p.channel_id = q.channel_id
   AND p.start_at   = q.start_at
   AND p.id < q.id;

DROP INDEX epg_program_dedupe_idx;
CREATE UNIQUE INDEX epg_program_chan_start_idx ON epg_program (channel_id, start_at);
