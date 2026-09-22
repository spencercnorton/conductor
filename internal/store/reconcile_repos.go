package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ReconcileProgram is one upcoming EPG airing, flattened with its
// enrichment ids, as consumed by the proactive reconciler (internal/
// reconcile). The reconciler pulls the whole upcoming window once per
// cycle and matches it in memory against Sonarr/Radarr wanted-lists, so
// this is a deliberately wide row: every field the matcher might key on.
type ReconcileProgram struct {
	ProgramID          uuid.UUID
	ChannelID          uuid.UUID
	ChannelName        string
	Title              string
	SubTitle           string
	IsMovie            bool
	IsNew              bool
	StartAt            time.Time
	EndAt              time.Time
	OriginalAirDate    *time.Time
	EpisodeNumOnscreen string
	TMDbID             int
	TVDBID             int
	IMDbID             string
}

// ListUpcomingForReconcile returns every enabled-channel program whose
// start_at is in the window (now, now+within], ordered by air time, with
// its enrichment ids LEFT JOINed (un-enriched programs still come back so
// the matcher can fall back to title/episode-title matching).
//
// One query per reconcile cycle: a few thousand rows, matched in memory.
func (db *DB) ListUpcomingForReconcile(ctx context.Context, within time.Duration) ([]ReconcileProgram, error) {
	now := time.Now()
	rows, err := db.Pool.Query(ctx, `
		SELECT p.id, p.channel_id, c.name,
		       p.title, p.sub_title, p.is_movie, p.effective_is_new,
		       p.start_at, p.end_at, p.original_air_date,
		       p.episode_num_onscreen,
		       COALESCE(e.tmdb_id, 0), COALESCE(e.tvdb_id, 0), COALESCE(e.imdb_id, '')
		  FROM epg_program_effective p
		  JOIN channel c ON c.id = p.channel_id
		  LEFT JOIN epg_program_enrichment e ON e.program_id = p.id
		 WHERE c.enabled
		   AND p.is_canonical
		   AND p.start_at > $1
		   AND p.start_at <= $2
		 ORDER BY p.start_at ASC`,
		now, now.Add(within))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ReconcileProgram
	for rows.Next() {
		var p ReconcileProgram
		if err := rows.Scan(
			&p.ProgramID, &p.ChannelID, &p.ChannelName,
			&p.Title, &p.SubTitle, &p.IsMovie, &p.IsNew,
			&p.StartAt, &p.EndAt, &p.OriginalAirDate,
			&p.EpisodeNumOnscreen,
			&p.TMDbID, &p.TVDBID, &p.IMDbID,
		); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
