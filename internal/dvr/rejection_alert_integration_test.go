package dvr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/alerts"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/google/uuid"
)

type rejectionAlertProcessor func(context.Context, string, string, time.Duration) (MediaReport, error)

func (f rejectionAlertProcessor) NormalizeAndValidate(ctx context.Context, in, out string, d time.Duration) (MediaReport, error) {
	return f(ctx, in, out, d)
}

// This test exercises finalization and the real PostgreSQL recording->failed
// CAS. The processor is deliberately a controlled rejection/completion seam;
// media validation and decode quality are covered by media_artifact_test.go.
// Never start/delete a fixed-name database: the caller supplies its own DSN.
func TestIntegrationArtifactRejectionAlertsOnlyWinningTerminalizer(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("set CONDUCTOR_INT_TEST=1 and isolated CONDUCTOR_TEST_DSN")
	}
	dsn := os.Getenv("CONDUCTOR_TEST_DSN")
	if dsn == "" {
		t.Fatal("an explicitly isolated CONDUCTOR_TEST_DSN is required")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var mu sync.Mutex
	var sent []alerts.Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var ev alerts.Event
		if err := json.NewDecoder(req.Body).Decode(&ev); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		id, parseErr := uuid.Parse(fmt.Sprint(ev.Tags["recording_id"]))
		if parseErr != nil {
			t.Error(parseErr)
		} else if row, err := db.GetDVRRecording(ctx, id); err != nil || row.State != "failed" {
			t.Errorf("alert preceded durable rejection: state=%s err=%v", row.State, err)
		}
		mu.Lock()
		sent = append(sent, ev)
		mu.Unlock()
		if strings.Contains(ev.Title, "webhook_failure") {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	snapshot := func() []alerts.Event { mu.Lock(); defer mu.Unlock(); return append([]alerts.Event(nil), sent...) }
	client := alerts.New(server.URL, "task-test-secret", logger)
	const faultText = "normalized A/V start offset is 1.911122s (limit 1s)"
	fault := errors.New(faultText)
	for _, mode := range []string{"rejected_once", "concurrent_rejection", "cancelled_wins", "other_failure_wins", "validated_success", "webhook_failure", "database_failure"} {
		t.Run(mode, func(t *testing.T) {
			var channelNumber float64
			if err := db.Pool.QueryRow(ctx, `SELECT COALESCE(max(number),0)+1 FROM channel`).Scan(&channelNumber); err != nil {
				t.Fatal(err)
			}
			channel, err := db.CreateChannel(ctx, store.Channel{Number: channelNumber, Name: "alert-" + uuid.NewString(), Enabled: true})
			if err != nil {
				t.Fatal(err)
			}
			rec := store.DVRRecording{ID: uuid.New(), ChannelID: channel.ID, Title: "Alert ownership " + mode, OutputPath: filepath.Join(t.TempDir(), "episode.ts"), ScheduledStart: time.Now().Add(-time.Minute), ScheduledEnd: time.Now()}
			_, err = db.Pool.Exec(ctx, `INSERT INTO dvr_recording(id,channel_id,title,scheduled_start,scheduled_end,output_path,state) VALUES($1,$2,$3,$4,$5,$6,'recording')`, rec.ID, rec.ChannelID, rec.Title, rec.ScheduledStart, rec.ScheduledEnd, rec.OutputPath)
			if err != nil {
				t.Fatal(err)
			}
			raw := recordingCapturePath(rec.OutputPath, rec.ID)
			rawBytes := []byte("original retained capture")
			if err := os.WriteFile(raw, rawBytes, 0600); err != nil {
				t.Fatal(err)
			}
			normalized := recordingNormalizedPath(rec.OutputPath, rec.ID)
			normalizedBytes := []byte("normalized candidate")
			recorder := &Recorder{Logger: logger, DB: db, Alerts: client}
			recorder.MediaProcessor = rejectionAlertProcessor(func(_ context.Context, _, out string, d time.Duration) (MediaReport, error) {
				if err := os.WriteFile(out, normalizedBytes, 0600); err != nil {
					return MediaReport{}, err
				}
				hash := sha256.Sum256(normalizedBytes)
				report := MediaReport{Duration: d, ArtifactBytes: int64(len(normalizedBytes)), SHA256: fmt.Sprintf("%x", hash)}
				switch mode {
				case "cancelled_wins":
					if err := db.MarkDVRRecordingCancelled(ctx, rec.ID); err != nil {
						return report, err
					}
				case "other_failure_wins":
					if _, err := db.MarkDVRRecordingFailed(ctx, rec.ID, "prior capture failure", 99); err != nil {
						return report, err
					}
				case "validated_success":
					return report, nil
				case "database_failure":
					db.Close()
				}
				return report, fault
			})
			before := len(snapshot())
			if mode == "concurrent_rejection" {
				// Concurrent callers contend on the production CAS; only its winner may
				// emit. File contents are identical and all rejected candidates stay private.
				var wg sync.WaitGroup
				errs := make(chan error, 8)
				for i := 0; i < 8; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); errs <- recorder.finalizeArtifact(ctx, rec, raw, int64(len(rawBytes)), 0) }()
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					if err == nil {
						t.Fatal("rejected contender reported success")
					}
				}
			} else {
				err := recorder.finalizeArtifact(ctx, rec, raw, int64(len(rawBytes)), 0)
				switch mode {
				case "cancelled_wins", "validated_success":
					if err != nil {
						t.Fatalf("unexpected terminal error: %v", err)
					}
				case "database_failure":
					if !errors.Is(err, ErrRecordingTerminalState) {
						t.Fatalf("missing uncertain-state error: %v", err)
					}
				default:
					if !errors.Is(err, fault) {
						t.Fatalf("lost rejection cause: %v", err)
					}
				}
			}
			events := snapshot()[before:]
			want := 0
			if mode == "rejected_once" || mode == "concurrent_rejection" || mode == "webhook_failure" {
				want = 1
			}
			if len(events) != want {
				t.Fatalf("attempted %d alert requests, want%d: %+v", len(events), want, events)
			}
			if want == 1 {
				ev := events[0]
				if ev.Severity != alerts.SevError || ev.Source != "conductor.dvr" || !strings.Contains(ev.Title, rec.Title) || !strings.Contains(ev.Detail, faultText) || ev.Tags["recording_id"] != rec.ID.String() || ev.Tags["channel_id"] != rec.ChannelID.String() || ev.Tags["failure_stage"] != "artifact_rejection" {
					t.Fatalf("incomplete rejection alert: %+v", ev)
				}
				if candidate, err := os.ReadFile(normalized); err != nil || !bytes.Equal(candidate, normalizedBytes) {
					t.Fatalf("normalized diagnostic was not retained: %q %v", candidate, err)
				}
				// Retrying a completed rejection must not resend, even though the returned
				// failure is still non-nil. State==failed is not proof this caller owns it.
				if err := recorder.finalizeArtifact(ctx, rec, raw, int64(len(rawBytes)), 0); err == nil {
					t.Fatal("repeat rejection reported success")
				}
				if len(snapshot()) != before+1 {
					t.Fatal("repeat finalization duplicated alert")
				}
			}
			if mode == "database_failure" {
				return
			}
			got, err := db.GetDVRRecording(ctx, rec.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "validated_success":
				if got.State != "completed" || !got.ArtifactCurrent {
					t.Fatalf("valid artifact not published: %+v", got)
				}
			case "cancelled_wins":
				if got.State != "cancelled" {
					t.Fatalf("cancellation lost: %s", got.State)
				}
			case "other_failure_wins":
				if got.State != "failed" || got.Error != "prior capture failure" {
					t.Fatalf("prior failure overwritten: %+v", got)
				}
			default:
				if got.State != "failed" || got.ArtifactState != "rejected" || got.ArtifactPath != normalized || got.ArtifactBytes != int64(len(normalizedBytes)) {
					t.Fatalf("rejection evidence lost: %+v", got)
				}
			}
			if mode != "validated_success" {
				if rawGot, err := os.ReadFile(raw); err != nil || !bytes.Equal(rawGot, rawBytes) {
					t.Fatalf("raw capture changed: %q %v", rawGot, err)
				}
				if _, err := os.Stat(rec.OutputPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("rejected artifact published: %v", err)
				}
			}
		})
	}
}
