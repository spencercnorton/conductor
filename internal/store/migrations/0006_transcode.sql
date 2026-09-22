-- 0006_transcode.sql
-- Phase 4: per-stream + batch transcoding profiles. Spec §4.5 covers the
-- "FFmpeg vs direct passthrough" decision; this schema makes that decision
-- DB-driven and per-source/per-channel overridable.
--
-- A transcode_profile is a named set of FFmpeg arguments + GPU usage flags.
-- Channels and channel_sources can each point at one profile; resolution
-- order at lease time is: channel_source.transcode_profile_id (per-source
-- override) > channel.transcode_profile_id (channel default) > none
-- (direct passthrough).
--
-- The 4 built-in profiles are seeded inline below. Operator-created
-- profiles get is_builtin=false and are freely editable; built-ins are
-- editable too but the operator should usually copy + customize.

CREATE TYPE transcode_kind AS ENUM (
    'passthrough',     -- no ffmpeg, direct copy (default)
    'cpu_remux',       -- ffmpeg -c copy with cleanup flags (PCR fixes, etc.)
    'cpu_transcode',   -- ffmpeg software encode
    'nvenc_transcode', -- ffmpeg NVENC + NVDEC
    'custom'           -- operator-supplied raw argv
);

CREATE TABLE transcode_profile (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text NOT NULL UNIQUE,
    description     text NOT NULL DEFAULT '',
    kind            transcode_kind NOT NULL,

    -- High-level knobs the argv builder reads when kind != 'custom'.
    -- For kind='custom' these are ignored and ffmpeg_args is used verbatim.
    video_codec     text NOT NULL DEFAULT '',          -- 'h264_nvenc' | 'libx264' | 'copy' | ''
    video_bitrate_kbps integer NOT NULL DEFAULT 0,     -- 0 = no -b:v
    video_max_bitrate_kbps integer NOT NULL DEFAULT 0, -- 0 = no -maxrate
    video_height    integer NOT NULL DEFAULT 0,        -- 0 = no scale
    video_preset    text NOT NULL DEFAULT '',          -- nvenc: p1..p7; libx264: ultrafast..slow
    video_tune      text NOT NULL DEFAULT '',          -- nvenc: ll, ull, hq; libx264: zerolatency, etc.
    video_keyint    integer NOT NULL DEFAULT 0,        -- 0 = ffmpeg default
    deinterlace     boolean NOT NULL DEFAULT false,
    hdr_to_sdr      boolean NOT NULL DEFAULT false,

    audio_codec     text NOT NULL DEFAULT '',          -- 'aac' | 'eac3' | 'copy' | ''
    audio_bitrate_kbps integer NOT NULL DEFAULT 0,
    audio_channels  integer NOT NULL DEFAULT 0,        -- 0 = preserve, 2 = stereo downmix
    audio_normalize boolean NOT NULL DEFAULT false,

    use_nvenc       boolean NOT NULL DEFAULT false,
    use_nvdec       boolean NOT NULL DEFAULT false,
    fix_timestamps  boolean NOT NULL DEFAULT true,     -- +genpts +discardcorrupt
    low_latency     boolean NOT NULL DEFAULT true,     -- +nobuffer, -fflags nobuffer

    -- Raw arg overrides for kind='custom' or last-mile tuning.
    -- Stored as JSON array: ["-fflags", "+genpts", "-c:v", "h264_nvenc", ...]
    ffmpeg_args     jsonb NOT NULL DEFAULT '[]'::jsonb,

    -- Operator-readable freeform.
    notes           text NOT NULL DEFAULT '',

    is_builtin      boolean NOT NULL DEFAULT false,
    enabled         boolean NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Add foreign keys to channel + channel_source.
ALTER TABLE channel
    ADD COLUMN transcode_profile_id uuid REFERENCES transcode_profile(id) ON DELETE SET NULL;

ALTER TABLE channel_source
    ADD COLUMN transcode_profile_id uuid REFERENCES transcode_profile(id) ON DELETE SET NULL;

CREATE INDEX channel_transcode_idx        ON channel        (transcode_profile_id) WHERE transcode_profile_id IS NOT NULL;
CREATE INDEX channel_source_transcode_idx ON channel_source (transcode_profile_id) WHERE transcode_profile_id IS NOT NULL;

-- ─────────────────────── Built-in profiles ───────────────────────
-- Seeded once at schema apply; operator can edit but probably shouldn't —
-- copy-and-customize is the recommended pattern.

INSERT INTO transcode_profile (name, description, kind, is_builtin)
VALUES ('passthrough',
        'No transcoding. Bytes from upstream are forwarded directly to clients. Use when the provider stream is clean and clients can play it natively. This is the default when a channel has no profile assigned.',
        'passthrough', true);

INSERT INTO transcode_profile (
    name, description, kind,
    video_codec, audio_codec,
    fix_timestamps, low_latency,
    is_builtin
) VALUES ('stabilize-cpu',
        'No re-encoding. ffmpeg -c copy with timestamp correction (+genpts +discardcorrupt) and PCR cleanup. Fixes most ''dirty stream'' symptoms (Plex stalls, audio drift, freezing) at near-zero CPU cost. Use as the default for any channel where direct passthrough has been flaky.',
        'cpu_remux',
        'copy', 'copy',
        true, true,
        true);

INSERT INTO transcode_profile (
    name, description, kind,
    video_codec, video_bitrate_kbps, video_max_bitrate_kbps, video_height,
    video_preset, video_tune, video_keyint,
    audio_codec, audio_bitrate_kbps, audio_channels,
    use_nvenc, use_nvdec, fix_timestamps, low_latency,
    is_builtin
) VALUES ('nvenc-1080p',
        'Hardware-accelerated NVENC encode at 1080p / 8 Mbps CBR / 192 kbps AAC stereo. NVDEC for input decode (zero-copy on the GPU). Low-latency tuning so live channel-changes stay snappy. Use for channels where you need to re-encode to fix codec compatibility (e.g. provider sends HEVC + Plex client wants H.264).',
        'nvenc_transcode',
        'h264_nvenc', 8000, 10000, 1080,
        'p4', 'll', 60,
        'aac', 192, 2,
        true, true, true, true,
        true);

INSERT INTO transcode_profile (
    name, description, kind,
    video_codec, video_bitrate_kbps, video_max_bitrate_kbps, video_height,
    video_preset, video_tune, video_keyint,
    audio_codec, audio_bitrate_kbps, audio_channels,
    use_nvenc, use_nvdec, fix_timestamps, low_latency,
    is_builtin
) VALUES ('nvenc-720p',
        'Hardware-accelerated NVENC encode at 720p / 4 Mbps CBR / 128 kbps AAC stereo. Lower bitrate for bandwidth-constrained clients (mobile data, weak WAN). Same low-latency tuning as nvenc-1080p.',
        'nvenc_transcode',
        'h264_nvenc', 4000, 5000, 720,
        'p4', 'll', 60,
        'aac', 128, 2,
        true, true, true, true,
        true);

INSERT INTO transcode_profile (
    name, description, kind,
    video_codec, video_bitrate_kbps, video_max_bitrate_kbps, video_height,
    video_preset, video_tune, video_keyint,
    audio_codec, audio_bitrate_kbps, audio_channels,
    use_nvenc, use_nvdec, fix_timestamps, low_latency, hdr_to_sdr,
    is_builtin
) VALUES ('hdr-to-sdr-nvenc',
        'NVENC encode with HDR10 → SDR (BT.709) tone mapping. Some live channels broadcast HDR; certain Plex clients (older Apple TVs in particular) refuse HDR-over-Live-TV. This profile transcodes once on Conductor''s GPU instead of forcing every Plex client to do it.',
        'nvenc_transcode',
        'h264_nvenc', 8000, 10000, 1080,
        'p4', 'hq', 60,
        'aac', 192, 2,
        true, true, true, true, true,
        true);

INSERT INTO transcode_profile (
    name, description, kind,
    audio_codec, audio_bitrate_kbps, audio_channels, audio_normalize,
    fix_timestamps, low_latency,
    is_builtin
) VALUES ('audio-fix',
        'Video copied as-is; audio re-encoded to 192 kbps AAC stereo with loudness normalization. Use when the provider ships AC3 5.1 that some clients can''t passthrough, or when audio levels swing wildly between segments.',
        'cpu_transcode',
        'aac', 192, 2, true,
        true, true,
        true);

CREATE OR REPLACE FUNCTION touch_transcode_profile_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER transcode_profile_touch
    BEFORE UPDATE ON transcode_profile
    FOR EACH ROW
    EXECUTE FUNCTION touch_transcode_profile_updated_at();
