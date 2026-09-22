-- 0007_postprocess.sql
-- Phase 4b: post-record processing pipeline. Runs after Recorder writes a
-- finished file. Stages execute in order; if a stage fails, downstream
-- stages still run (best-effort) but the run is marked partial.
--
-- Stage names (free-form, but built-in handlers know these):
--   commskip-auto      an external hybrid pipeline (Comskip + Whisper + LLM).
--                      Produces .edl + .srt sidecars next to the .ts.
--   webhook            Generic POST hook. Body = JSON with file path + stats.
--
-- Operator can disable per-channel via channel.postprocess_enabled.

ALTER TABLE channel
    ADD COLUMN postprocess_enabled boolean NOT NULL DEFAULT true;

CREATE TABLE postprocess_run (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    recording_id    uuid REFERENCES dvr_recording(id) ON DELETE CASCADE,
    -- Denormalize the file path so the row stays useful even if the
    -- recording row gets pruned later.
    file_path       text NOT NULL,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    success         boolean NOT NULL DEFAULT false,
    stages_run      integer NOT NULL DEFAULT 0,
    stages_ok       integer NOT NULL DEFAULT 0,
    stages_failed   integer NOT NULL DEFAULT 0,
    log             text NOT NULL DEFAULT ''   -- one-line-per-stage summary
);
CREATE INDEX postprocess_run_recording_idx ON postprocess_run (recording_id);
CREATE INDEX postprocess_run_recent_idx    ON postprocess_run (started_at DESC);
