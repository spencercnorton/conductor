package store

import (
	"context"

	"github.com/google/uuid"
)

// ChannelPostProcessEnabled returns the channel.postprocess_enabled flag.
// Defaults to true on missing rows so a typo in channel_id doesn't suppress
// the pipeline silently — operator will see a row in postprocess_run with a
// failed lookup.
func (db *DB) ChannelPostProcessEnabled(ctx context.Context, channelID uuid.UUID) (bool, error) {
	var enabled bool
	err := db.Pool.QueryRow(ctx,
		`SELECT postprocess_enabled FROM channel WHERE id = $1`,
		channelID).Scan(&enabled)
	return enabled, err
}

// StartPostProcessRun inserts an audit row at pipeline start. Returns the
// run id; pass to FinishPostProcessRun on completion.
func (db *DB) StartPostProcessRun(ctx context.Context, recordingID uuid.UUID, filePath string) (uuid.UUID, error) {
	var id uuid.UUID
	err := db.Pool.QueryRow(ctx, `
		INSERT INTO postprocess_run (recording_id, file_path)
		VALUES ($1, $2) RETURNING id`,
		recordingID, filePath).Scan(&id)
	return id, err
}

// FinishPostProcessRun fills in completion stats.
func (db *DB) FinishPostProcessRun(ctx context.Context, runID uuid.UUID,
	success bool, stagesRun, stagesOK, stagesFailed int, log string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE postprocess_run
		   SET finished_at = now(),
		       success = $2,
		       stages_run = $3, stages_ok = $4, stages_failed = $5,
		       log = $6
		 WHERE id = $1`,
		runID, success, stagesRun, stagesOK, stagesFailed, log)
	return err
}

// SetChannelPostProcessEnabled lets the admin toggle the per-channel gate.
func (db *DB) SetChannelPostProcessEnabled(ctx context.Context, channelID uuid.UUID, enabled bool) error {
	_, err := db.Pool.Exec(ctx,
		`UPDATE channel SET postprocess_enabled = $2 WHERE id = $1`,
		channelID, enabled)
	return err
}

// ListPostProcessRuns returns recent runs for the dashboard.
func (db *DB) ListPostProcessRuns(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := db.Pool.Query(ctx, `
		SELECT id, recording_id, file_path,
		       started_at, finished_at, success,
		       stages_run, stages_ok, stages_failed, log
		  FROM postprocess_run
		 ORDER BY started_at DESC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		row := map[string]any{}
		var (
			id, fp, log     string
			recID           *uuid.UUID
			started         interface{}
			finished        interface{}
			success         bool
			run, ok, failed int
		)
		if err := rows.Scan(&id, &recID, &fp, &started, &finished,
			&success, &run, &ok, &failed, &log); err != nil {
			return nil, err
		}
		row["id"] = id
		row["recording_id"] = recID
		row["file_path"] = fp
		row["started_at"] = started
		row["finished_at"] = finished
		row["success"] = success
		row["stages_run"] = run
		row["stages_ok"] = ok
		row["stages_failed"] = failed
		row["log"] = log
		out = append(out, row)
	}
	return out, rows.Err()
}
