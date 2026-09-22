-- 0019_dvr_capacity_admission.sql
--
-- Make DVR capacity contention a durable scheduling state instead of a
-- terminal zero-byte recording failure.  The scheduler still acquires a real
-- active_stream lease before recording; these columns expose its deterministic
-- ordering and bounded retry decisions to the admin API and survive restarts.

ALTER TABLE dvr_recording
    ADD COLUMN priority integer NOT NULL DEFAULT 100
        CHECK (priority > 0),
    ADD COLUMN admission_state text NOT NULL DEFAULT 'pending'
        CHECK (admission_state IN ('pending', 'queued', 'admitted')),
    ADD COLUMN admission_attempts integer NOT NULL DEFAULT 0
        CHECK (admission_attempts >= 0),
    ADD COLUMN admission_retry_at timestamptz,
    ADD COLUMN admission_reason text NOT NULL DEFAULT '';

-- Rows that already crossed the old scheduler/recorder boundary necessarily
-- acquired a real stream lease. Preserve that truth during the rollout.
UPDATE dvr_recording
   SET admission_state = 'admitted'
 WHERE state IN ('recording', 'completed');

-- Earlier starts are non-preemptible. Priority resolves only an equal start;
-- UUID is last so rows created in the same transaction/time stay stable.
CREATE INDEX dvr_recording_admission_order_idx
    ON dvr_recording
       (scheduled_start, priority, created_at, id)
    WHERE state = 'scheduled';

-- `draining` is a real provider connection/ffmpeg teardown phase, not free
-- capacity. Keep it non-attachable (channel lookup remains starting/running)
-- but include it in credential load and dashboard slot truth until OnExit.
DROP INDEX active_stream_credential_idx;
CREATE INDEX active_stream_credential_idx
    ON active_stream (credential_id)
    WHERE state IN ('starting', 'running', 'draining');

CREATE OR REPLACE VIEW v_credential_load AS
SELECT
    c.id AS credential_id,
    c.provider_id,
    c.username,
    c.max_streams,
    COUNT(s.id) FILTER (WHERE s.state IN ('starting','running','draining')) AS active_count,
    c.max_streams - COUNT(s.id) FILTER (WHERE s.state IN ('starting','running','draining')) AS free_slots
FROM provider_credential c
LEFT JOIN active_stream s ON s.credential_id = c.id
WHERE c.enabled
GROUP BY c.id;
