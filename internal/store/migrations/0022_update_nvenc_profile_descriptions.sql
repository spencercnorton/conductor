-- 0022_update_nvenc_profile_descriptions.sql
--
-- H.264 NVENC still uses NVDEC, but FFmpeg 4.4 must expose software frames so
-- Conductor can normalize P010 input to yuv420p before the encoder upload.
-- Update the built-in profiles' operational descriptions in a forward
-- migration rather than rewriting already-applied migration 0006. The exact
-- old-description predicates preserve any operator-customized text.

UPDATE transcode_profile
   SET description = 'Hardware-accelerated NVENC encode at 1080p / 8 Mbps CBR / 192 kbps AAC stereo. NVDEC remains enabled for input decode, but on FFmpeg 4.4 H.264 output transfers frames through system memory for software 8-bit normalization before upload to NVENC; this is not an end-to-end zero-copy path. Low-latency tuning keeps live channel changes responsive. Use for codec compatibility fixes such as HEVC input for H.264-only Plex clients.'
 WHERE name = 'nvenc-1080p'
   AND is_builtin
   AND description = 'Hardware-accelerated NVENC encode at 1080p / 8 Mbps CBR / 192 kbps AAC stereo. NVDEC for input decode (zero-copy on the GPU). Low-latency tuning so live channel-changes stay snappy. Use for channels where you need to re-encode to fix codec compatibility (e.g. provider sends HEVC + Plex client wants H.264).';

UPDATE transcode_profile
   SET description = 'Hardware-accelerated NVENC encode at 720p / 4 Mbps CBR / 128 kbps AAC stereo. NVDEC remains enabled for input decode, but on FFmpeg 4.4 H.264 output transfers frames through system memory for software 8-bit normalization before upload to NVENC; this is not an end-to-end zero-copy path. Use the lower bitrate for bandwidth-constrained clients.'
 WHERE name = 'nvenc-720p'
   AND is_builtin
   AND description = 'Hardware-accelerated NVENC encode at 720p / 4 Mbps CBR / 128 kbps AAC stereo. Lower bitrate for bandwidth-constrained clients (mobile data, weak WAN). Same low-latency tuning as nvenc-1080p.';

UPDATE transcode_profile
   SET description = 'NVENC encode with HDR10 to SDR (BT.709) tone mapping. NVDEC remains enabled for input decode, but FFmpeg 4.4 transfers frames through system memory for software tone mapping and 8-bit normalization before upload to NVENC; this is not an end-to-end zero-copy path. Use for Plex clients that cannot consume HDR over Live TV.'
 WHERE name = 'hdr-to-sdr-nvenc'
   AND is_builtin
   AND description = 'NVENC encode with HDR10 → SDR (BT.709) tone mapping. Some live channels broadcast HDR; certain Plex clients (older Apple TVs in particular) refuse HDR-over-Live-TV. This profile transcodes once on Conductor''s GPU instead of forcing every Plex client to do it.';
