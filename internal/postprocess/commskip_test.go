package postprocess_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/postprocess"
)

// fakeCommskip is a minimal commskip-auto: POST /api/v1/batches creates
// batch "b1", each GET returns the next entry of states (the last one
// repeats), DELETE is recorded.
type fakeCommskip struct {
	mu         sync.Mutex
	createCode int
	states     []map[string]any
	polls      int
	auth       string
	created    map[string]any
	deleted    string
}

func (f *fakeCommskip) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/batches":
		f.auth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&f.created)
		if f.createCode != 0 {
			w.WriteHeader(f.createCode)
			_, _ = w.Write([]byte(`{"error":"bad target"}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true,"batch_id":"b1","episode_count":1}`))
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/batches/b1":
		i := f.polls
		if i >= len(f.states) {
			i = len(f.states) - 1
		}
		f.polls++
		_ = json.NewEncoder(w).Encode(f.states[i])
	case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/batches/b1":
		f.deleted = "b1"
		_, _ = w.Write([]byte(`{"cancelled":1}`))
	default:
		http.NotFound(w, r)
	}
}

func batch(state string, failed int, ep map[string]any) map[string]any {
	return map[string]any{"id": "b1", "state": state, "failed_count": failed,
		"episodes": []map[string]any{ep}}
}

func runStage(t *testing.T, f *fakeCommskip, dryRun bool, timeout time.Duration) postprocess.StageResult {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	stage := postprocess.NewCommskipStage(srv.URL+"/", "test-key", dryRun, newLogger())
	stage.PollInterval = time.Millisecond
	stage.PollTimeout = timeout
	return stage.Run(context.Background(), &postprocess.Input{
		RecordingID: "rec-9", FilePath: "/var/lib/conductor/dvr/Heat/Heat.ts",
		Title: "Heat", EpisodeOnscreen: "S01E02",
	})
}

func TestCommskipSubmitsBatchAndWaitsForDone(t *testing.T) {
	f := &fakeCommskip{states: []map[string]any{
		batch("queued", 0, map[string]any{"state": "pending"}),
		batch("running", 0, map[string]any{"state": "running"}),
		batch("done", 0, map[string]any{"state": "ok", "approved_cuts": 4, "total_cut_s": 671.6}),
	}}
	r := runStage(t, f, true, time.Second)
	if !r.Success {
		t.Fatalf("expected success; got %q", r.Detail)
	}
	if f.auth != "Bearer test-key" {
		t.Errorf("auth = %q", f.auth)
	}
	target, _ := f.created["target"].(map[string]any)
	if target["type"] != "file" || target["path"] != "/var/lib/conductor/dvr/Heat/Heat.ts" {
		t.Errorf("target = %v", f.created["target"])
	}
	if f.created["job"] != "commskip" || f.created["dry_run"] != true || f.created["skip_if_fresh"] != true {
		t.Errorf("create body = %v", f.created)
	}
	if f.created["label"] != "conductor-dvr Heat S01E02 rec=rec-9" {
		t.Errorf("label = %v", f.created["label"])
	}
	if f.polls != 3 {
		t.Errorf("polls = %d, want 3", f.polls)
	}
	for _, want := range []string{"batch=b1", "dry_run=true", "cuts=4", "cut_s=671.6"} {
		if !strings.Contains(r.Detail, want) {
			t.Errorf("detail missing %q; got %q", want, r.Detail)
		}
	}
}

func TestCommskipPassesDryRunFalse(t *testing.T) {
	f := &fakeCommskip{states: []map[string]any{batch("done", 0, map[string]any{"state": "skipped"})}}
	r := runStage(t, f, false, time.Second)
	if !r.Success || f.created["dry_run"] != false {
		t.Fatalf("success=%v dry_run=%v detail=%q", r.Success, f.created["dry_run"], r.Detail)
	}
}

func TestCommskipCreateErrorFails(t *testing.T) {
	for _, code := range []int{400, 401} {
		f := &fakeCommskip{createCode: code}
		r := runStage(t, f, true, time.Second)
		if r.Success || !strings.Contains(r.Detail, fmt.Sprintf("HTTP %d", code)) {
			t.Errorf("code %d: success=%v detail=%q", code, r.Success, r.Detail)
		}
	}
}

func TestCommskipFailedEpisodeFails(t *testing.T) {
	for _, b := range []map[string]any{
		batch("done", 1, map[string]any{"state": "failed", "error_tail": "ffmpeg: codec not supported"}),
		batch("failed", 1, map[string]any{"state": "failed", "error_tail": "ffmpeg: codec not supported"}),
	} {
		f := &fakeCommskip{states: []map[string]any{b}}
		r := runStage(t, f, true, time.Second)
		if r.Success || !strings.Contains(r.Detail, "ffmpeg") {
			t.Errorf("state %v: success=%v detail=%q", b["state"], r.Success, r.Detail)
		}
	}
}

func TestCommskipTimeoutCancelsBatch(t *testing.T) {
	f := &fakeCommskip{states: []map[string]any{batch("running", 0, map[string]any{"state": "running"})}}
	r := runStage(t, f, true, 20*time.Millisecond)
	if r.Success || !strings.Contains(r.Detail, "still running") {
		t.Errorf("success=%v detail=%q", r.Success, r.Detail)
	}
	if f.deleted != "b1" {
		t.Errorf("batch not cancelled on timeout")
	}
}

func TestCommskipNilWhenNoBaseURL(t *testing.T) {
	if s := postprocess.NewCommskipStage("", "k", true, newLogger()); s != nil {
		t.Errorf("expected nil stage when base URL empty; got %+v", s)
	}
}
