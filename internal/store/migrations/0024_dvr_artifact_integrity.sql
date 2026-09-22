-- 0024_dvr_artifact_integrity.sql
--
-- keep the canonical DVR artifact under durable, validated ownership.
-- A completed database row is not proof that the bytes at output_path are a
-- usable recording: legacy rows pre-date normalization, coverage checks, and
-- content hashing.  They remain recoverable/current during the rollout, but
-- only a freshly validated artifact may suppress an automated repeat.

ALTER TABLE dvr_recording
    ADD COLUMN recording_heartbeat_at timestamptz,
    ADD COLUMN artifact_state text NOT NULL DEFAULT 'none'
        CHECK (artifact_state IN (
            'none', 'legacy', 'historical', 'validating', 'validated',
            'rejected', 'superseded', 'missing'
        )),
    ADD COLUMN artifact_stage text NOT NULL DEFAULT 'none'
        CHECK (artifact_stage IN ('none', 'capturing', 'normalizing', 'publishing')),
    ADD COLUMN artifact_current boolean NOT NULL DEFAULT false,
    ADD COLUMN artifact_path text NOT NULL DEFAULT '',
    ADD COLUMN artifact_bytes bigint NOT NULL DEFAULT 0
        CHECK (artifact_bytes >= 0),
    ADD COLUMN artifact_sha256 bytea,
    ADD COLUMN artifact_filesystem_id text NOT NULL DEFAULT '',
    ADD COLUMN artifact_validated_at timestamptz,
    ADD COLUMN artifact_validator_version text NOT NULL DEFAULT '',
    ADD COLUMN artifact_error text NOT NULL DEFAULT '',
    ADD COLUMN replaces_recording_id uuid
        REFERENCES dvr_recording(id) ON DELETE SET NULL,
    ADD COLUMN explicit_rerecord boolean NOT NULL DEFAULT false,
    ADD COLUMN media_duration_ms bigint NOT NULL DEFAULT 0
        CHECK (media_duration_ms >= 0),
    ADD COLUMN media_max_gap_ms bigint NOT NULL DEFAULT 0
        CHECK (media_max_gap_ms >= 0),
    ADD COLUMN media_discontinuities integer NOT NULL DEFAULT 0
        CHECK (media_discontinuities >= 0),
    ADD COLUMN media_av_start_delta_ms bigint NOT NULL DEFAULT 0
        CHECK (media_av_start_delta_ms >= 0),
    ADD COLUMN media_av_end_delta_ms bigint NOT NULL DEFAULT 0
        CHECK (media_av_end_delta_ms >= 0),
    ADD COLUMN media_av_drift_ms bigint NOT NULL DEFAULT 0
        CHECK (media_av_drift_ms >= 0);

ALTER TABLE dvr_recording
    ADD CONSTRAINT dvr_recording_artifact_sha256_length_chk
        CHECK (artifact_sha256 IS NULL OR octet_length(artifact_sha256) = 32),
    ADD CONSTRAINT dvr_recording_current_artifact_completed_chk
        CHECK (NOT artifact_current OR (
            state = 'completed' AND artifact_state IN ('legacy', 'validated')
        )),
    ADD CONSTRAINT dvr_recording_validated_artifact_evidence_chk
        CHECK (artifact_state <> 'validated' OR (
            state = 'completed'
            AND artifact_path <> ''
            AND artifact_bytes > 0
            AND artifact_sha256 IS NOT NULL
            AND artifact_filesystem_id <> ''
            AND artifact_validated_at IS NOT NULL
            AND artifact_validator_version <> ''
        )),
    ADD CONSTRAINT dvr_recording_superseded_artifact_not_current_chk
        CHECK (artifact_state <> 'superseded' OR NOT artifact_current),
    ADD CONSTRAINT dvr_recording_explicit_replacement_provenance_chk
        CHECK (NOT explicit_rerecord OR replaces_recording_id IS NOT NULL),
    ADD CONSTRAINT dvr_recording_replacement_not_self_chk
        CHECK (replaces_recording_id IS NULL OR replaces_recording_id <> id);

-- Historical duplicate completed rows all name one mutable filesystem path.
-- Only the newest row can describe the bytes currently there.  Mark it
-- current but deliberately LEGACY, never validated: SQL chronology cannot
-- establish media integrity or manufacture a trustworthy SHA-256 digest.
WITH ranked AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY output_path
               ORDER BY completed_at DESC NULLS LAST, created_at DESC, id DESC
           ) AS rn
      FROM dvr_recording
     WHERE state = 'completed' AND output_path <> ''
)
UPDATE dvr_recording AS r
   SET artifact_state = CASE WHEN ranked.rn = 1 THEN 'legacy' ELSE 'historical' END,
       artifact_current = ranked.rn = 1,
       artifact_path = r.output_path,
       artifact_bytes = GREATEST(r.bytes_written, 0)
  FROM ranked
 WHERE ranked.id = r.id;

-- If a repeat was already queued before this migration, bind it to the legacy
-- owner so successful validation can transfer ownership instead of failing at
-- publish time.  This is provenance only: it does not make the repeat an
-- operator-authorized explicit re-record and does not cancel/suppress it.
UPDATE dvr_recording AS candidate
   SET replaces_recording_id = owner.id
  FROM dvr_recording AS owner
 WHERE candidate.state IN ('scheduled', 'recording')
   AND candidate.output_path <> ''
   AND owner.output_path = candidate.output_path
   AND owner.artifact_current
   AND owner.artifact_state = 'legacy'
   AND candidate.id <> owner.id
   AND candidate.replaces_recording_id IS NULL;

-- Active capture ownership and published artifact ownership are deliberately
-- separate.  A replacement may be scheduled while its predecessor stays the
-- current canonical artifact, but only one row can be current for a path.
CREATE UNIQUE INDEX dvr_recording_current_artifact_path_idx
    ON dvr_recording (output_path)
    WHERE artifact_current AND output_path <> '';

CREATE INDEX dvr_recording_runtime_heartbeat_idx
    ON dvr_recording (recording_heartbeat_at)
    WHERE state = 'recording';

CREATE INDEX dvr_recording_interrupted_publish_idx
    ON dvr_recording (created_at, id)
    WHERE artifact_stage = 'publishing';
