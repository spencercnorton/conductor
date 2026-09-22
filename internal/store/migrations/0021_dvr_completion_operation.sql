-- 0021_dvr_completion_operation.sql
--
-- A DVR completion can commit in PostgreSQL while the client loses the commit
-- response. Persist the caller's idempotency identity with the terminal row so
-- a fresh transaction can distinguish its own committed completion from an
-- unrelated/legacy completed state before deciding whether final media is safe.

ALTER TABLE dvr_recording
    ADD COLUMN completion_operation_id uuid;

ALTER TABLE dvr_recording
    ADD CONSTRAINT dvr_recording_completion_operation_state
    CHECK (completion_operation_id IS NULL OR state = 'completed');

CREATE UNIQUE INDEX dvr_recording_completion_operation_idx
    ON dvr_recording (completion_operation_id)
    WHERE completion_operation_id IS NOT NULL;
