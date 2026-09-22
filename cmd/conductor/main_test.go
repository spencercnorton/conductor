package main

import (
	"context"
	"errors"
	"testing"
)

type stubSlatePreparer struct {
	err error
}

func (s stubSlatePreparer) PrepareSlate(context.Context) error { return s.err }

func TestRequireContinuitySlateMakesPreparationFailureFatalToStartup(t *testing.T) {
	sentinel := errors.New("ffmpeg slate render failed")
	err := requireContinuitySlate(context.Background(), stubSlatePreparer{err: sentinel})
	if !errors.Is(err, sentinel) {
		t.Fatalf("required slate error=%v, want wrapped render failure", err)
	}
	if err := requireContinuitySlate(context.Background(), stubSlatePreparer{}); err != nil {
		t.Fatalf("successful required slate preparation failed: %v", err)
	}
}
