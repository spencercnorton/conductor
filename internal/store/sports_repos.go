package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ChannelSportMapping declares "channel C carries league L's events
// (optionally filtered by team or broadcaster)." Populated via admin
// endpoints + a migration-time seed for the named sports channels we
// already know about (ESPN, NFL Network, NBA TV, etc.).
type ChannelSportMapping struct {
	ID                uuid.UUID
	ChannelID         uuid.UUID
	League            string // matches sport_league enum, lowercase
	TeamName          string // empty = match any team
	BroadcasterFilter string // empty = match any broadcaster
	Enabled           bool
	Note              string
	CreatedAt         time.Time
}

// SportsEvent is the cached row from the sports schedule API.
type SportsEvent struct {
	ID          uuid.UUID
	League      string
	ExternalID  string
	HomeTeam    string
	AwayTeam    string
	KickoffAt   time.Time
	DurationMin int
	Venue       string
	Broadcaster string
	Extra       []byte // jsonb raw bytes
	FetchedAt   time.Time
}

// ListChannelSportMappings returns all enabled (channel, league) entries.
// Used by the schedule worker.
func (db *DB) ListChannelSportMappings(ctx context.Context) ([]ChannelSportMapping, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, channel_id, league::text, team_name, broadcaster_filter,
		       enabled, note, created_at
		  FROM channel_sport_mapping
		 WHERE enabled
		 ORDER BY league, channel_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChannelSportMapping
	for rows.Next() {
		var m ChannelSportMapping
		if err := rows.Scan(&m.ID, &m.ChannelID, &m.League, &m.TeamName,
			&m.BroadcasterFilter, &m.Enabled, &m.Note, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// UpsertChannelSportMapping is the admin write path. Returns the row
// (with id assigned for new entries) — admins use it to confirm the
// mapping landed.
func (db *DB) UpsertChannelSportMapping(ctx context.Context, m ChannelSportMapping) (ChannelSportMapping, error) {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO channel_sport_mapping
		    (id, channel_id, league, team_name, broadcaster_filter, enabled, note)
		VALUES ($1, $2, $3::sport_league, $4, $5, $6, $7)
		ON CONFLICT (channel_id, league, team_name, broadcaster_filter)
		DO UPDATE SET enabled = EXCLUDED.enabled, note = EXCLUDED.note
		RETURNING created_at`,
		m.ID, m.ChannelID, m.League, m.TeamName, m.BroadcasterFilter,
		m.Enabled, m.Note)
	if err := row.Scan(&m.CreatedAt); err != nil {
		return ChannelSportMapping{}, err
	}
	return m, nil
}

// DeleteChannelSportMapping by id; the schedule worker will stop
// generating EPG entries for this mapping on its next pass.
func (db *DB) DeleteChannelSportMapping(ctx context.Context, id uuid.UUID) error {
	tag, err := db.Pool.Exec(ctx,
		`DELETE FROM channel_sport_mapping WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UpsertSportsEvent caches one event row, keyed by (league, external_id).
// Re-syncs that find the same event update the kickoff / venue / broadcaster.
func (db *DB) UpsertSportsEvent(ctx context.Context, e SportsEvent) error {
	if e.Extra == nil {
		e.Extra = []byte("{}")
	}
	if e.DurationMin == 0 {
		e.DurationMin = 180
	}
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO sports_event_cache
		    (league, external_id, home_team, away_team, kickoff_at,
		     duration_min, venue, broadcaster, extra, fetched_at)
		VALUES ($1::sport_league, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, now())
		ON CONFLICT (league, external_id)
		DO UPDATE SET
		    home_team    = EXCLUDED.home_team,
		    away_team    = EXCLUDED.away_team,
		    kickoff_at   = EXCLUDED.kickoff_at,
		    duration_min = EXCLUDED.duration_min,
		    venue        = EXCLUDED.venue,
		    broadcaster  = EXCLUDED.broadcaster,
		    extra        = EXCLUDED.extra,
		    fetched_at   = now()`,
		e.League, e.ExternalID, e.HomeTeam, e.AwayTeam, e.KickoffAt,
		e.DurationMin, e.Venue, e.Broadcaster, e.Extra)
	return err
}

// ListSportsEventsForLeague returns all cached events for a league
// whose kickoff is within the [from, to] window. Used by the EPG
// injector to generate epg_program rows for each mapped channel.
func (db *DB) ListSportsEventsForLeague(ctx context.Context, league string, from, to time.Time) ([]SportsEvent, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT id, league::text, external_id, home_team, away_team,
		       kickoff_at, duration_min, venue, broadcaster, extra, fetched_at
		  FROM sports_event_cache
		 WHERE league = $1::sport_league
		   AND kickoff_at >= $2
		   AND kickoff_at <= $3
		 ORDER BY kickoff_at ASC`, league, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SportsEvent
	for rows.Next() {
		var e SportsEvent
		if err := rows.Scan(&e.ID, &e.League, &e.ExternalID, &e.HomeTeam,
			&e.AwayTeam, &e.KickoffAt, &e.DurationMin, &e.Venue,
			&e.Broadcaster, &e.Extra, &e.FetchedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SportsEventByExternalID fetches one event for direct lookup. Returns
// ErrNotFound when the cache hasn't seen the event yet — the caller
// should kick the schedule worker for that league.
func (db *DB) SportsEventByExternalID(ctx context.Context, league, externalID string) (SportsEvent, error) {
	var e SportsEvent
	row := db.Pool.QueryRow(ctx, `
		SELECT id, league::text, external_id, home_team, away_team,
		       kickoff_at, duration_min, venue, broadcaster, extra, fetched_at
		  FROM sports_event_cache
		 WHERE league = $1::sport_league AND external_id = $2`, league, externalID)
	if err := row.Scan(&e.ID, &e.League, &e.ExternalID, &e.HomeTeam,
		&e.AwayTeam, &e.KickoffAt, &e.DurationMin, &e.Venue,
		&e.Broadcaster, &e.Extra, &e.FetchedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SportsEvent{}, ErrNotFound
		}
		return SportsEvent{}, err
	}
	return e, nil
}
