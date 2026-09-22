package postprocess_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spencercnorton/conductor/internal/postprocess"
)

func TestCommskipPostsToCorrectEndpoint(t *testing.T) {
	var captured struct {
		Path        string
		Auth        string
		ContentType string
		Body        []byte
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.Path = r.URL.Path
		captured.Auth = r.Header.Get("Authorization")
		captured.ContentType = r.Header.Get("Content-Type")
		captured.Body, _ = io.ReadAll(r.Body)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id": "abc-123",
			"status": "ok",
			"edl_path": "/dvr/Heat/Heat.edl",
			"srt_path": "/dvr/Heat/Heat.en.srt",
		})
	}))
	defer srv.Close()

	stage := postprocess.NewCommskipStage(srv.URL, "test-key", newLogger())
	r := stage.Run(context.Background(), &postprocess.Input{
		RecordingID: "rec-9", FilePath: "/dvr/Heat/Heat.ts",
		Title: "Heat", IsMovie: true,
	})

	if !r.Success {
		t.Fatalf("expected success; got: %s", r.Detail)
	}
	if captured.Path != "/api/v1/process" {
		t.Errorf("path: got %q want /api/v1/process", captured.Path)
	}
	if captured.Auth != "Bearer test-key" {
		t.Errorf("auth: got %q want Bearer test-key", captured.Auth)
	}
	if captured.ContentType != "application/json" {
		t.Errorf("content-type: got %q", captured.ContentType)
	}

	var got map[string]any
	if err := json.Unmarshal(captured.Body, &got); err != nil {
		t.Fatalf("body not JSON: %v\n  body: %s", err, captured.Body)
	}
	if got["file_path"] != "/dvr/Heat/Heat.ts" {
		t.Errorf("file_path: got %v", got["file_path"])
	}
	if got["title"] != "Heat" {
		t.Errorf("title: got %v", got["title"])
	}
	if got["is_movie"] != true {
		t.Errorf("is_movie: got %v", got["is_movie"])
	}
	tags, _ := got["tags"].(map[string]any)
	if tags["recording_id"] != "rec-9" {
		t.Errorf("tags.recording_id: got %v", tags["recording_id"])
	}
	if tags["source"] != "conductor-dvr" {
		t.Errorf("tags.source: got %v", tags["source"])
	}

	// Detail should mention the EDL + SRT paths (so they're searchable in
	// the postprocess_run.log column).
	for _, want := range []string{"edl=/dvr/Heat/Heat.edl", "srt=/dvr/Heat/Heat.en.srt", "job=abc-123"} {
		if !strings.Contains(r.Detail, want) {
			t.Errorf("detail missing %q; got %q", want, r.Detail)
		}
	}
}

func TestCommskipReturnsFailureOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	stage := postprocess.NewCommskipStage(srv.URL, "k", newLogger())
	r := stage.Run(context.Background(), &postprocess.Input{
		RecordingID: "rec-9", FilePath: "/dvr/x.ts",
	})
	if r.Success {
		t.Error("expected failure on 500")
	}
	if !strings.Contains(r.Detail, "500") {
		t.Errorf("detail should mention status; got %q", r.Detail)
	}
}

func TestCommskipReturnsFailureOnPipelineError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id": "abc",
			"status": "error",
			"error":  "ffmpeg: codec not supported",
		})
	}))
	defer srv.Close()

	stage := postprocess.NewCommskipStage(srv.URL, "k", newLogger())
	r := stage.Run(context.Background(), &postprocess.Input{
		RecordingID: "rec-9", FilePath: "/dvr/x.ts",
	})
	if r.Success {
		t.Error("expected failure when commskip returns error in body")
	}
	if !strings.Contains(r.Detail, "ffmpeg") {
		t.Errorf("detail should propagate upstream error; got %q", r.Detail)
	}
}

func TestCommskipNilWhenNoBaseURL(t *testing.T) {
	if s := postprocess.NewCommskipStage("", "k", newLogger()); s != nil {
		t.Errorf("expected nil stage when base URL empty; got %+v", s)
	}
}
