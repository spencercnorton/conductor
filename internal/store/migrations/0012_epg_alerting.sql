-- 0012_epg_alerting.sql — EPG freshness alerting (audit 2026-06-09, item E5).
--
-- consecutive_failures: per-source failure streak, maintained by
-- UpdateEPGSourcePollState (reset to 0 on success, +1 on failure). Backs the
-- "alert after N consecutive failures" rule and survives restarts so a
-- redeploy mid-outage doesn't silence the alarm.
--
-- last_ok_at: timestamp of the last successful fetch (ok or no_change),
-- surfaced by GET /admin/epg/health.

ALTER TABLE epg_source ADD COLUMN consecutive_failures int NOT NULL DEFAULT 0;
ALTER TABLE epg_source ADD COLUMN last_ok_at timestamptz;
