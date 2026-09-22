package main

import (
	"log/slog"
	"testing"
)

func TestLogLevelFromEnv(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		want    slog.Level
		wantErr bool
	}{
		{"", slog.LevelInfo, false},
		{"   ", slog.LevelInfo, false},
		{"debug", slog.LevelDebug, false},
		{"DEBUG", slog.LevelDebug, false},
		{" warn ", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"DEBUG-4", slog.LevelDebug - 4, false},
		// a typo must not change the level and must not stop the process
		{"verbose", slog.LevelInfo, true},
		{"7", slog.LevelInfo, true},
	} {
		got, err := logLevelFromEnv(tc.raw)
		if (err != nil) != tc.wantErr {
			t.Fatalf("logLevelFromEnv(%q) err=%v, wantErr=%v", tc.raw, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("logLevelFromEnv(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}
