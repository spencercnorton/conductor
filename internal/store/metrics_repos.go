// metrics_repos.go — cheap read-only queries backing the /metrics gauges
// (audit Q1). Each is a single SELECT; callers bound them with a short
// context (the scrape handler uses ~2s).
package store

import (
	"context"
	"time"
)

// CountActiveStreams returns the number of provider slots still in use.
// Draining pumps remain counted until upstream/ffmpeg teardown completes.
func (db *DB) CountActiveStreams(ctx context.Context) (int, error) {
	var n int
	err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM active_stream
		 WHERE state IN ('starting','running','draining')`).Scan(&n)
	return n, err
}

// EPGHorizonDays returns how many days of future guide data exist (now →
// latest future programme start). 0 when the guide has run dry — the
// "Plex guide dies on Thursday" number from the 2026-06-09 audit.
func (db *DB) EPGHorizonDays(ctx context.Context) (float64, error) {
	var days float64
	err := db.Pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM (MAX(start_at) - now())) / 86400.0, 0)
		  FROM epg_program
		 WHERE is_canonical AND start_at > now()`).Scan(&days)
	return days, err
}

// CountEPGPrograms returns rows visible to guide consumers. Suppressed source
// candidates have their own operational gauge.
func (db *DB) CountEPGPrograms(ctx context.Context) (int64, error) {
	var n int64
	err := db.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM epg_program WHERE is_canonical`).Scan(&n)
	return n, err
}

// EPGSourceFreshness is one enabled source's last-success recency.
type EPGSourceFreshness struct {
	Name     string
	LastOKAt *time.Time // nil = never succeeded
}

// ListEPGSourceFreshness returns last_ok_at per enabled EPG source.
func (db *DB) ListEPGSourceFreshness(ctx context.Context) ([]EPGSourceFreshness, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT name, last_ok_at FROM epg_source
		 WHERE enabled ORDER BY priority, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EPGSourceFreshness
	for rows.Next() {
		var f EPGSourceFreshness
		if err := rows.Scan(&f.Name, &f.LastOKAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
