-- 0014_drop_subtitles.sql
-- Add a drop_subtitles flag to transcode_profile and seed a "cable-clean"
-- remux profile for IPTV restreams of traditional-cable channels.
--
-- Why: providers restream channels like TLC / Discovery / Investigation ID /
-- History / A&E with the full broadcast program map intact — a DVB bitmap
-- subtitle PID (codec dvb_subtitle), a Spanish SAP audio track, and sometimes
-- AC3 audio. Conductor forwards that multi-PID program verbatim in passthrough
-- mode, and Plex's Live TV transcoder stalls/restarts on it (the "lag & break"
-- symptom). Sports/news channels arrive pre-simplified to a single AAC audio
-- program and play fine. ffprobe of the live streams confirmed the split.
--
-- The built-in remux/transcode profiles couldn't fix this on their own because
-- BuildArgs() force-copied subtitles (`-map 0:s?` + `-c:s copy`), preserving
-- the exact DVB-sub PID that breaks Plex. drop_subtitles closes that gap.

ALTER TABLE transcode_profile
    ADD COLUMN IF NOT EXISTS drop_subtitles boolean NOT NULL DEFAULT false;

-- ─────────────────────── cable-clean built-in ───────────────────────
-- Video copied untouched (no GPU, near-zero CPU). Audio re-encoded to a
-- single AAC stereo track — this also collapses AC3 / multi-language SAP
-- down to one clean track. Subtitles dropped. Timestamps + PCR cleaned.
-- The result is the same single-video / single-AAC program shape that the
-- working sports/news channels already have natively.
INSERT INTO transcode_profile (
    name, description, kind,
    video_codec,
    audio_codec, audio_bitrate_kbps, audio_channels,
    fix_timestamps, low_latency, drop_subtitles,
    is_builtin
) VALUES ('cable-clean',
        'Remux for IPTV restreams of traditional-cable channels (TLC, Discovery, ID, History, A&E, etc.). Video copied as-is; audio re-encoded to a single 192 kbps AAC stereo track (collapses AC3 / Spanish-SAP); DVB bitmap subtitles dropped; timestamps + PCR cleaned. Fixes Plex Live TV stalls/restarts on channels whose upstream program carries extra audio + subtitle PIDs. Near-zero CPU, no GPU. Assign to any channel that lags/breaks under direct passthrough.',
        'cpu_transcode',
        'copy',
        'aac', 192, 2,
        true, false, true,
        true)
ON CONFLICT (name) DO NOTHING;
