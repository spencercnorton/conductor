package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TranscodeProfile is the typed view of a transcode_profile row.
// Mirrors columns in store/migrations/0006_transcode.sql.
type TranscodeProfile struct {
	ID          uuid.UUID
	Name        string
	Description string
	Kind        string

	VideoCodec          string
	VideoBitrateKbps    int
	VideoMaxBitrateKbps int
	VideoHeight         int
	VideoPreset         string
	VideoTune           string
	VideoKeyint         int
	Deinterlace         bool
	HDRtoSDR            bool

	AudioCodec       string
	AudioBitrateKbps int
	AudioChannels    int
	AudioNormalize   bool

	UseNVENC      bool
	UseNVDEC      bool
	FixTimestamps bool
	LowLatency    bool
	DropSubtitles bool

	FFmpegArgs []string

	Notes     string
	IsBuiltin bool
	Enabled   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateTranscodeProfile inserts a new operator-defined profile.
// is_builtin is forced to false for operator-created rows.
func (db *DB) CreateTranscodeProfile(ctx context.Context, p TranscodeProfile) (TranscodeProfile, error) {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	args, _ := json.Marshal(p.FFmpegArgs)
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO transcode_profile (
		    id, name, description, kind,
		    video_codec, video_bitrate_kbps, video_max_bitrate_kbps, video_height,
		    video_preset, video_tune, video_keyint,
		    deinterlace, hdr_to_sdr,
		    audio_codec, audio_bitrate_kbps, audio_channels, audio_normalize,
		    use_nvenc, use_nvdec, fix_timestamps, low_latency, drop_subtitles,
		    ffmpeg_args, notes, is_builtin, enabled
		)
		VALUES (
		    $1, $2, $3, $4::transcode_kind,
		    $5, $6, $7, $8,
		    $9, $10, $11,
		    $12, $13,
		    $14, $15, $16, $17,
		    $18, $19, $20, $21, $22,
		    $23::jsonb, $24, false, $25
		)
		RETURNING created_at, updated_at`,
		p.ID, p.Name, p.Description, p.Kind,
		p.VideoCodec, p.VideoBitrateKbps, p.VideoMaxBitrateKbps, p.VideoHeight,
		p.VideoPreset, p.VideoTune, p.VideoKeyint,
		p.Deinterlace, p.HDRtoSDR,
		p.AudioCodec, p.AudioBitrateKbps, p.AudioChannels, p.AudioNormalize,
		p.UseNVENC, p.UseNVDEC, p.FixTimestamps, p.LowLatency, p.DropSubtitles,
		string(args), p.Notes, p.Enabled,
	)
	if err := row.Scan(&p.CreatedAt, &p.UpdatedAt); err != nil {
		return TranscodeProfile{}, err
	}
	p.IsBuiltin = false
	return p, nil
}

// ListTranscodeProfiles returns every profile (builtin + operator).
func (db *DB) ListTranscodeProfiles(ctx context.Context) ([]TranscodeProfile, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, name, description, kind::text,
		       video_codec, video_bitrate_kbps, video_max_bitrate_kbps, video_height,
		       video_preset, video_tune, video_keyint,
		       deinterlace, hdr_to_sdr,
		       audio_codec, audio_bitrate_kbps, audio_channels, audio_normalize,
		       use_nvenc, use_nvdec, fix_timestamps, low_latency, drop_subtitles,
		       ffmpeg_args, notes, is_builtin, enabled, created_at, updated_at
		  FROM transcode_profile
		 ORDER BY is_builtin DESC, name ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTranscodeRows(rows)
}

// GetTranscodeProfile fetches one row by id.
func (db *DB) GetTranscodeProfile(ctx context.Context, id uuid.UUID) (TranscodeProfile, error) {
	row := db.Pool.QueryRow(ctx, `
		SELECT id, name, description, kind::text,
		       video_codec, video_bitrate_kbps, video_max_bitrate_kbps, video_height,
		       video_preset, video_tune, video_keyint,
		       deinterlace, hdr_to_sdr,
		       audio_codec, audio_bitrate_kbps, audio_channels, audio_normalize,
		       use_nvenc, use_nvdec, fix_timestamps, low_latency, drop_subtitles,
		       ffmpeg_args, notes, is_builtin, enabled, created_at, updated_at
		  FROM transcode_profile WHERE id = $1`, id)
	p, err := scanOneTranscode(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return TranscodeProfile{}, ErrNotFound
	}
	return p, err
}

// GetTranscodeProfileByName is convenient for the seed lookup ("passthrough", etc.).
func (db *DB) GetTranscodeProfileByName(ctx context.Context, name string) (TranscodeProfile, error) {
	row := db.Pool.QueryRow(ctx, `
		SELECT id, name, description, kind::text,
		       video_codec, video_bitrate_kbps, video_max_bitrate_kbps, video_height,
		       video_preset, video_tune, video_keyint,
		       deinterlace, hdr_to_sdr,
		       audio_codec, audio_bitrate_kbps, audio_channels, audio_normalize,
		       use_nvenc, use_nvdec, fix_timestamps, low_latency, drop_subtitles,
		       ffmpeg_args, notes, is_builtin, enabled, created_at, updated_at
		  FROM transcode_profile WHERE name = $1`, name)
	p, err := scanOneTranscode(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return TranscodeProfile{}, ErrNotFound
	}
	return p, err
}

// SetChannelTranscodeProfile sets (or clears, when profileID is nil) the
// channel-wide default profile.
func (db *DB) SetChannelTranscodeProfile(ctx context.Context, channelID uuid.UUID, profileID *uuid.UUID) error {
	_, err := db.Pool.Exec(ctx,
		`UPDATE channel SET transcode_profile_id = $2 WHERE id = $1`,
		channelID, profileID)
	return err
}

// SetChannelSourceTranscodeProfile sets (or clears) a per-source override.
func (db *DB) SetChannelSourceTranscodeProfile(ctx context.Context, sourceID uuid.UUID, profileID *uuid.UUID) error {
	_, err := db.Pool.Exec(ctx,
		`UPDATE channel_source SET transcode_profile_id = $2 WHERE id = $1`,
		sourceID, profileID)
	return err
}

// EffectiveTranscodeProfileForLease resolves the profile to use for a
// given (channel_id, channel_source_id) pair using:
//
//	per-source override > per-channel default > nil (passthrough)
//
// Returns nil profile + nil error when no profile is configured (caller
// should fall back to direct passthrough).
func (db *DB) EffectiveTranscodeProfileForLease(ctx context.Context, sourceID uuid.UUID) (*TranscodeProfile, error) {
	var profileID *uuid.UUID
	err := db.Pool.QueryRow(ctx, `
		SELECT COALESCE(s.transcode_profile_id, c.transcode_profile_id)
		  FROM channel_source s
		  JOIN channel c ON c.id = s.channel_id
		 WHERE s.id = $1`, sourceID).Scan(&profileID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if profileID == nil {
		return nil, nil
	}
	p, err := db.GetTranscodeProfile(ctx, *profileID)
	if err != nil {
		return nil, err
	}
	if !p.Enabled {
		return nil, nil
	}
	return &p, nil
}

func scanTranscodeRows(rows pgx.Rows) ([]TranscodeProfile, error) {
	var out []TranscodeProfile
	for rows.Next() {
		p, err := scanOneTranscode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// scanOneTranscode handles both pgx.Row and pgx.Rows because they share Scan.
type scanner interface {
	Scan(dest ...any) error
}

func scanOneTranscode(s scanner) (TranscodeProfile, error) {
	var p TranscodeProfile
	var args []byte
	if err := s.Scan(
		&p.ID, &p.Name, &p.Description, &p.Kind,
		&p.VideoCodec, &p.VideoBitrateKbps, &p.VideoMaxBitrateKbps, &p.VideoHeight,
		&p.VideoPreset, &p.VideoTune, &p.VideoKeyint,
		&p.Deinterlace, &p.HDRtoSDR,
		&p.AudioCodec, &p.AudioBitrateKbps, &p.AudioChannels, &p.AudioNormalize,
		&p.UseNVENC, &p.UseNVDEC, &p.FixTimestamps, &p.LowLatency, &p.DropSubtitles,
		&args, &p.Notes, &p.IsBuiltin, &p.Enabled, &p.CreatedAt, &p.UpdatedAt,
	); err != nil {
		return TranscodeProfile{}, err
	}
	if len(args) > 0 {
		_ = json.Unmarshal(args, &p.FFmpegArgs)
	}
	return p, nil
}
