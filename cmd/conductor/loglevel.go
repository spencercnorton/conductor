package main

import (
	"fmt"
	"log/slog"
	"strings"
)

// logLevelFromEnv resolves CONDUCTOR_LOG_LEVEL. Unset or empty keeps info, so
// the default deployment is unchanged.
//
// An unparseable value keeps info and returns the error rather than failing:
// the level is a diagnostic knob, and a typo in it must never be the reason
// the service does not start.
func logLevelFromEnv(raw string) (slog.Level, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return slog.LevelInfo, nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return slog.LevelInfo, fmt.Errorf("invalid CONDUCTOR_LOG_LEVEL %q: %w", raw, err)
	}
	return level, nil
}
