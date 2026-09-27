// integration_test.go — end-to-end DVR-as-Indexer roundtrip against
// real Postgres. Gated on CONDUCTOR_INT_TEST=1.
//
// Validates spec acceptance for Phase 3a:
//   - Torznab caps + tvsearch return well-formed XML with correct items
//   - signed enclosure URL roundtrips (sign → verify)
//   - the /dvr/schedule.torrent handler creates a dvr_recording row
//     with state=scheduled and the right output_path shape
//   - duplicate grabs are idempotent (Sonarr re-grab does NOT create
//     a second row, ON CONFLICT clause works)
package dvr_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/spencercnorton/conductor/internal/testdb"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/postprocess"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/stream"
)

const (
	pgImage         = "postgres:16-alpine"
	pgContainerName = "conductor-dvr-test-pg"
	pgPort          = "55436"
	pgPassword      = "conductor-test"
)

func recordingCapturePathForTest(outputPath string, recordingID uuid.UUID) string {
	return outputPath + ".capture." + recordingID.String() + ".partial"
}

// seedChannelSeq keeps each seedFutureProgram call on a distinct channel —
// channel.number is UNIQUE, so a test seeding more than one program would
// otherwise collide on the second insert.
var seedChannelSeq int

func skipIfNoIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
}

func startTestPostgres(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("CONDUCTOR_TEST_DSN"); dsn != "" {
		return testdb.New(t, dsn)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available and CONDUCTOR_TEST_DSN not set")
	}
	_ = exec.Command("docker", "rm", "-f", "-v", pgContainerName).Run()
	out, err := exec.Command("docker", "run", "-d",
		"--name", pgContainerName,
		"-p", pgPort+":5432",
		"-e", "POSTGRES_PASSWORD="+pgPassword,
		"-e", "POSTGRES_DB=conductor_dvr_test",
		pgImage).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "-v", pgContainerName).Run() })

	dsn := fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%s/conductor_dvr_test?sslmode=disable",
		pgPassword, pgPort)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("postgres container did not become ready")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, dsn)
		cancel()
		if err == nil {
			conn.Close(context.Background())
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	return dsn
}

func freshDB(t *testing.T) *store.DB {
	t.Helper()
	skipIfNoIntegration(t)
	dsn := startTestPostgres(t)
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

// seedFutureProgram inserts one upcoming program on a configured channel.
// Returns (channelID, programID). Uses a fixed source_hash so test is stable.
func seedFutureProgram(t *testing.T, db *store.DB, title, episodeOnscreen string, isMovie bool, startsIn, dur time.Duration) (chID, progID string, start, end time.Time) {
	t.Helper()
	ctx := context.Background()

	seedChannelSeq++
	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 100 + float64(seedChannelSeq), Name: "FX-TEST", CallSign: "FXT",
		EpgChannelID: "ch.fx.test." + title, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "provider-" + title + fmt.Sprint(seedChannelSeq), Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "seed-user", PasswordEnc: []byte{1},
		MaxStreams: 2, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: provider.ID,
		UpstreamURL: "http://provider.test/stream.ts",
		Priority:    0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	start = time.Now().Add(startsIn).UTC().Truncate(time.Second)
	end = start.Add(dur)

	row := db.Pool.QueryRow(ctx, `
		INSERT INTO epg_program (
		    channel_id, start_at, end_at, title, sub_title, description, category,
		    episode_num_xmltv, episode_num_onscreen, is_movie, is_live, is_new,
		    is_premiere, is_finale, source_hash, enrichment_status
		)
		VALUES ($1, $2, $3, $4, '', '', '{}', '', $5, $6, false, false, false, false, $7, 'pending')
		RETURNING id`,
		ch.ID, start, end, title, episodeOnscreen, isMovie,
		"test-hash-"+title)
	var pid string
	if err := row.Scan(&pid); err != nil {
		t.Fatalf("seed program: %v", err)
	}
	return ch.ID.String(), pid, start, end
}

func waitForRecording(t *testing.T, db *store.DB, id uuid.UUID, timeout time.Duration, accept func(store.DVRRecording) bool) store.DVRRecording {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec, err := db.GetDVRRecording(context.Background(), id)
		if err == nil && accept(rec) {
			return rec
		}
		time.Sleep(25 * time.Millisecond)
	}
	rec, err := db.GetDVRRecording(context.Background(), id)
	t.Fatalf("recording %s did not reach expected state: rec=%+v err=%v", id, rec, err)
	return store.DVRRecording{}
}

var errAdmissionResolver = errors.New("test DVR resolver failure")

type admissionFailResolver struct{}

func (admissionFailResolver) Resolve(_ context.Context, _ store.ProviderCredential, url string) (string, error) {
	if strings.Contains(url, "fails") {
		return "", errAdmissionResolver
	}
	return url, nil
}

var errAdmissionResolverDeadline = errors.New("test resolver exceeded admission attempt")

type admissionDeadlineResolver struct{}

func (admissionDeadlineResolver) Resolve(ctx context.Context, _ store.ProviderCredential, _ string) (string, error) {
	<-ctx.Done()
	return "", fmt.Errorf("%w: %w", errAdmissionResolverDeadline, ctx.Err())
}

type postprocessCallProbe struct {
	called chan<- struct{}
}

func (p postprocessCallProbe) Name() string { return "test-probe" }

func (p postprocessCallProbe) Run(_ context.Context, _ *postprocess.Input) postprocess.StageResult {
	select {
	case p.called <- struct{}{}:
	default:
	}
	return postprocess.StageResult{Stage: p.Name(), Success: true}
}

type copyingMediaProcessor struct{}

func (copyingMediaProcessor) NormalizeAndValidate(
	_ context.Context,
	input, output string,
	expectedDuration time.Duration,
) (dvr.MediaReport, error) {
	media, err := os.ReadFile(input)
	if err != nil {
		return dvr.MediaReport{}, err
	}
	if err := os.WriteFile(output, media, 0o644); err != nil {
		return dvr.MediaReport{}, err
	}
	digest := sha256.Sum256(media)
	return dvr.MediaReport{
		Duration:      expectedDuration,
		ArtifactBytes: int64(len(media)),
		SHA256:        fmt.Sprintf("%x", digest),
	}, nil
}

// Unexpected source setup/decrypt/resolver failures are not temporary slot
// contention. The scheduler must fail once with the preserved cause rather
// than queueing until a misleading capacity deadline.
func TestIntegrationSchedulerPreservesUnexpectedAdmissionFailure(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "admission-error-provider", Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "error-user", PasswordEnc: []byte{1},
		MaxStreams: 2, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	ch, err := db.CreateChannel(ctx, store.Channel{Number: 849, Name: "admission-error", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: provider.ID, UpstreamURL: "http://provider.test/fails",
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	blockedProvider, err := db.CreateProvider(ctx, store.Provider{
		Name: "admission-error-blocked-provider", Kind: "m3u_xtream",
		BaseURL: "http://provider.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: blockedProvider.ID, Username: "blocked-user", PasswordEnc: []byte{1},
		MaxStreams: 2, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: ch.ID, ProviderID: blockedProvider.ID, UpstreamURL: "http://provider.test/blocked",
		Priority: 1, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	start := time.Now().Add(500 * time.Millisecond).UTC()
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: ch.ID, Title: "resolver failure", ScheduledStart: start,
		ScheduledEnd: start.Add(time.Minute), Priority: 100,
		RequestedBy: "test", OutputPath: t.TempDir() + "/resolver-failure.ts",
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, admissionFailResolver{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = time.Second
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 30 * time.Second
	scheduler.CapacityRetryInterval = 150 * time.Millisecond
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM provider WHERE id = $1 FOR UPDATE`, blockedProvider.ID); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()

	failed := waitForRecording(t, db, rec.ID, 5*time.Second, func(r store.DVRRecording) bool {
		return r.State == "failed"
	})
	if !strings.Contains(failed.Error, errAdmissionResolver.Error()) ||
		!strings.Contains(failed.Error, "dvr admission failed") {
		t.Fatalf("unexpected admission cause not preserved: %+v", failed)
	}
	if strings.Contains(failed.Error, "capacity unavailable") ||
		strings.Contains(failed.Error, "queued for provider capacity") ||
		failed.AdmissionAttempts != 0 || failed.AdmissionRetryAt != nil {
		t.Fatalf("unexpected admission failure mislabeled as capacity: %+v", failed)
	}
}

// A deadline is retryable only when a typed Conductor admission lock caused
// it. A resolver that consumes the same per-attempt context is an operational
// media/credential failure and must retain its cause without entering the
// capacity queue.
func TestIntegrationSchedulerDoesNotQueueResolverDeadline(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "resolver-deadline", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "resolver-deadline", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 849.1, Name: "resolver-deadline", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID,
		UpstreamURL: "http://resolver-deadline.test/stream",
		Priority:    0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	start := time.Now().Add(250 * time.Millisecond).UTC()
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "resolver deadline",
		ScheduledStart: start, ScheduledEnd: start.Add(time.Minute), Priority: 100,
		RequestedBy: "test", OutputPath: t.TempDir() + "/resolver-deadline.ts",
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, admissionDeadlineResolver{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = time.Second
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 30 * time.Second
	scheduler.CapacityRetryInterval = 100 * time.Millisecond
	scheduler.CapacityRetryWindow = 3 * time.Second
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()

	failed := waitForRecording(t, db, rec.ID, 3*time.Second, func(r store.DVRRecording) bool {
		return r.State == "failed"
	})
	if !strings.Contains(failed.Error, errAdmissionResolverDeadline.Error()) ||
		!strings.Contains(failed.Error, context.DeadlineExceeded.Error()) {
		t.Fatalf("resolver deadline cause not preserved: %+v", failed)
	}
	if failed.AdmissionAttempts != 0 || failed.AdmissionRetryAt != nil ||
		strings.Contains(failed.Error, "queued for provider capacity") ||
		strings.Contains(failed.Error, "slot contention") {
		t.Fatalf("resolver deadline mislabeled as capacity contention: %+v", failed)
	}
}

// A timeout reading source configuration is an operational database failure,
// not contention on Conductor's admission locks. It must fail immediately with
// the exact ListSources cause and zero capacity attempts.
func TestIntegrationSchedulerDoesNotQueueListSourcesDeadline(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "list-sources-deadline", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "list-sources", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 849.2, Name: "list-sources-deadline", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID,
		UpstreamURL: "http://list-sources.test/stream",
		Priority:    0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Keep the row outside the scheduler window until its startup forecast has
	// completed. A sentinel forecast conflict gives the test an observable
	// barrier without relying on a sleep: startup refresh must clear it before
	// we lock channel_source and move the row into the due window.
	start := time.Now().Add(time.Hour).UTC()
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "list sources deadline",
		ScheduledStart: start, ScheduledEnd: start.Add(time.Minute), Priority: 100,
		RequestedBy: "test", OutputPath: t.TempDir() + "/list-sources-deadline.ts",
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET admission_state = 'queued', admission_retry_at = scheduled_start,
		       admission_reason = 'capacity forecast: startup barrier',
		       error = 'capacity forecast: startup barrier'
		 WHERE id = $1`, rec.ID); err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = time.Second
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 30 * time.Second
	scheduler.CapacityRetryInterval = 100 * time.Millisecond
	scheduler.CapacityRetryWindow = 3 * time.Second
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()

	waitForRecording(t, db, rec.ID, 3*time.Second, func(r store.DVRRecording) bool {
		return r.AdmissionState == "pending" && r.AdmissionReason == ""
	})
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx, `LOCK TABLE channel_source IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	dueStart := time.Now().Add(100 * time.Millisecond).UTC()
	if _, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET scheduled_start = $2, scheduled_end = $3
		 WHERE id = $1`, rec.ID, dueStart, dueStart.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	failed := waitForRecording(t, db, rec.ID, 3*time.Second, func(r store.DVRRecording) bool {
		return r.State == "failed"
	})
	if !strings.Contains(failed.Error, "list sources") ||
		!strings.Contains(failed.Error, context.DeadlineExceeded.Error()) ||
		!strings.Contains(failed.Error, store.ErrLeaseOperational.Error()) {
		t.Fatalf("ListSources deadline cause not preserved: %+v", failed)
	}
	if failed.AdmissionAttempts != 0 || failed.AdmissionRetryAt != nil ||
		strings.Contains(failed.Error, "queued for provider capacity") ||
		strings.Contains(failed.Error, "slot contention") {
		t.Fatalf("ListSources deadline mislabeled as capacity contention: %+v", failed)
	}
}

// A provider row lock can delay admission without proving capacity is full.
// Each attempt is bounded by the retry cadence so an unrelated due recording
// progresses, while the blocked row queues until its overall deadline and
// then terminalizes without ever claiming or opening a file.
func TestIntegrationSchedulerBoundsBlockedAdmissionAttempts(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	seed := func(name string, number float64) (store.Provider, store.Channel) {
		t.Helper()
		provider, err := db.CreateProvider(ctx, store.Provider{
			Name: name, Kind: "m3u_xtream", BaseURL: upstream.URL, Enabled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateCredential(ctx, store.ProviderCredential{
			ProviderID: provider.ID, Username: name, PasswordEnc: []byte{1},
			MaxStreams: 1, Priority: 100, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		channel, err := db.CreateChannel(ctx, store.Channel{Number: number, Name: name, Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
			Priority: 0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return provider, channel
	}
	blockedProvider, blockedChannel := seed("blocked-admission", 855)
	_, healthyChannel := seed("healthy-admission", 856)

	startSlack := 1100 * time.Millisecond
	retryWindow := 3 * time.Second
	base := time.Now()
	blockedStart := base.Add(time.Second)
	healthyStart := base.Add(1050 * time.Millisecond)
	policy := store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: startSlack, MaxOverrun: time.Second}
	blockedPath := t.TempDir() + "/blocked.ts"
	blocked, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: blockedChannel.ID, Title: "blocked admission", Priority: 100,
		ScheduledStart: blockedStart, ScheduledEnd: blockedStart.Add(5 * time.Second),
		RequestedBy: "test", OutputPath: blockedPath,
	}, policy)
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: healthyChannel.ID, Title: "healthy admission", Priority: 100,
		ScheduledStart: healthyStart, ScheduledEnd: healthyStart.Add(5 * time.Second),
		RequestedBy: "test", OutputPath: t.TempDir() + "/healthy.ts",
	}, policy)
	if err != nil {
		t.Fatal(err)
	}

	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM provider WHERE id = $1 FOR UPDATE`, blockedProvider.ID); err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = startSlack
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 20 * time.Millisecond
	// Keep the fallback retry deliberately slow. The independent attempt
	// budget must still let the unrelated provider progress promptly.
	scheduler.CapacityRetryInterval = 2 * time.Second
	scheduler.AdmissionAttemptTimeout = 100 * time.Millisecond
	scheduler.CapacityRetryWindow = retryWindow

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	runStarted := time.Now()
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()

	queued := waitForRecording(t, db, blocked.ID, 2*time.Second, func(r store.DVRRecording) bool {
		return r.State == "scheduled" && r.AdmissionState == "queued" && r.AdmissionAttempts > 0
	})
	if !strings.Contains(queued.AdmissionReason, "admission lock contended") ||
		!strings.Contains(queued.AdmissionReason, context.DeadlineExceeded.Error()) {
		t.Fatalf("blocked attempt reason=%q", queued.AdmissionReason)
	}
	healthyDeadline := healthyStart.Add(-startSlack).Add(retryWindow)
	healthyState := waitForRecording(t, db, healthy.ID, 3*time.Second, func(r store.DVRRecording) bool {
		return r.State == "recording"
	})
	if !time.Now().Before(healthyDeadline) {
		t.Fatalf("unrelated recording was stalled past its admission deadline %s", healthyDeadline)
	}
	if healthyState.StartedAt == nil ||
		healthyState.StartedAt.Sub(runStarted) >= time.Second {
		t.Fatalf("unrelated recording started at %v; one contended row consumed the slower retry cadence",
			healthyState.StartedAt)
	}
	failed := waitForRecording(t, db, blocked.ID, 5*time.Second, func(r store.DVRRecording) bool {
		return r.State == "failed"
	})
	if !strings.Contains(failed.Error, "admission deadline") || failed.AdmissionAttempts == 0 {
		t.Fatalf("blocked row did not queue then expire coherently: %+v", failed)
	}
	if _, err := os.Stat(recordingCapturePathForTest(blockedPath, blocked.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("blocked admission touched partial file: err=%v", err)
	}
}

// A claim that starts before the deadline can block on another transaction's
// row lock until after it. The bounded claim context and post-lock DB clock
// must refuse it promptly, release the exact lease, and touch no file.
func TestIntegrationRecorderDeadlineClaimRefusalReleasesLease(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider, err := db.CreateProvider(ctx, store.Provider{Name: "claim-deadline", Kind: "m3u_xtream", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "claim-deadline", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{Number: 857, Name: "claim-deadline", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: "http://unused.test/stream",
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/late-claim.ts"
	start := time.Now().Add(time.Minute)
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "late claim", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
		RequestedBy: "test", OutputPath: path,
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM dvr_recording WHERE id = $1 FOR UPDATE`, rec.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	result := make(chan error, 1)
	started := time.Now()
	go func() { result <- recorder.RunReservedBefore(ctx, rec, reservation, deadline) }()
	waitDeadline := time.Now().Add(2 * time.Second)
	for {
		var blocked bool
		err := db.Pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname = current_database()
				   AND wait_event_type = 'Lock'
				   AND query LIKE '%FROM dvr_recording%FOR UPDATE%'
			)`).Scan(&blocked)
		if err == nil && blocked {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("recording claim never blocked on row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-result:
		if !errors.Is(err, dvr.ErrRecordingAdmissionExpired) ||
			!errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("RunReservedBefore err=%v, want expired + context deadline", err)
		}
	case <-time.After(time.Until(deadline) + time.Second):
		t.Fatal("claim did not return at bounded admission deadline")
	}
	if elapsed := time.Since(started); elapsed > 900*time.Millisecond {
		t.Fatalf("row-lock claim exceeded bounded deadline: %v", elapsed)
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "scheduled" {
		t.Fatalf("late claim state=%q, want scheduled", got.State)
	}
	if _, err := os.Stat(recordingCapturePathForTest(path, rec.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late claim touched partial file: err=%v", err)
	}
	var clients int
	var state string
	if err := db.Pool.QueryRow(ctx, `
		SELECT client_count, state::text FROM active_stream WHERE channel_id = $1`, channel.ID).Scan(&clients, &state); err != nil {
		t.Fatal(err)
	}
	if clients != 0 || state != "dead" {
		t.Fatalf("late reservation leak: clients=%d state=%s", clients, state)
	}
}

// Periodic forecast refresh bounds stale schedule-time conflicts after an
// external cancellation; the faster capacity-retry ticker is not responsible.
func TestIntegrationSchedulerPeriodicallyClearsStaleForecast(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider, err := db.CreateProvider(ctx, store.Provider{Name: "forecast-refresh", Kind: "m3u_xtream", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "forecast-refresh", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	create := func(number float64, title string) store.DVRRecording {
		t.Helper()
		channel, err := db.CreateChannel(ctx, store.Channel{Number: number, Name: title, Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: "http://unused.test/" + title,
			Priority: 0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		start := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: title, Priority: 100,
			ScheduledStart: start, ScheduledEnd: start.Add(time.Hour),
			RequestedBy: "test", OutputPath: t.TempDir() + "/" + title + ".ts",
		}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	winner := create(858, "forecast-refresh-winner")
	loser := create(859, "forecast-refresh-loser")
	if loser.AdmissionState != "queued" || !strings.HasPrefix(loser.AdmissionReason, "capacity forecast:") {
		t.Fatalf("initial conflict not surfaced: %+v", loser)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = time.Second
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 50 * time.Millisecond
	scheduler.CapacityRetryInterval = 10 * time.Second
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()
	if err := db.MarkDVRRecordingCancelled(ctx, winner.ID); err != nil {
		t.Fatal(err)
	}
	cleared := waitForRecording(t, db, loser.ID, 2*time.Second, func(r store.DVRRecording) bool {
		return r.State == "scheduled" && r.AdmissionState == "pending" && r.AdmissionReason == ""
	})
	if cleared.AdmissionAttempts != 0 {
		t.Fatalf("forecast refresh mutated runtime attempts: %+v", cleared)
	}
}

// A future retry timestamp must not hide an earlier queued row while a later
// due row overtakes it. Every scheduler boundary wakes and re-snapshots queued
// rows chronologically; a buffered release edge makes this boundary immediate.
func TestIntegrationSchedulerQueuedEarlierRowCannotBeOvertaken(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "queued-order", Kind: "m3u_xtream", BaseURL: upstream.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "queued-order", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	seedChannel := func(number float64, title string) store.Channel {
		t.Helper()
		channel, err := db.CreateChannel(ctx, store.Channel{Number: number, Name: title, Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
			Priority: 0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		return channel
	}
	earlierChannel := seedChannel(863, "queued-earlier")
	laterChannel := seedChannel(864, "queued-later")
	blockerChannel := seedChannel(865, "queued-release-hint")
	startSlack := time.Second
	base := time.Now().Add(500 * time.Millisecond)
	policy := store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: startSlack, MaxOverrun: time.Second}
	create := func(channel store.Channel, title string, start time.Time) store.DVRRecording {
		t.Helper()
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: title, Priority: 100,
			ScheduledStart: start, ScheduledEnd: start.Add(5 * time.Second),
			RequestedBy: "test", OutputPath: t.TempDir() + "/" + title + ".ts",
		}, policy)
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	earlier := create(earlierChannel, "queued-earlier", base)
	later := create(laterChannel, "queued-later", base.Add(100*time.Millisecond))
	if err := db.QueueDVRRecordingForCapacity(ctx, earlier.ID, time.Now().Add(time.Minute),
		"queued for provider capacity (test future retry)"); err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	blocker, err := pool.ReserveWriter(ctx, blockerChannel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	blocker.Release() // buffers a capacity edge before Scheduler.Run
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = startSlack
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 30 * time.Second
	scheduler.CapacityRetryInterval = 30 * time.Second
	scheduler.CapacityRetryWindow = 2 * time.Second
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()
	waitForRecording(t, db, earlier.ID, 3*time.Second, func(r store.DVRRecording) bool {
		return r.State == "recording"
	})
	laterState, err := db.GetDVRRecording(ctx, later.ID)
	if err != nil {
		t.Fatal(err)
	}
	if laterState.State != "scheduled" || laterState.AdmissionState != "queued" {
		t.Fatalf("later row overtook earlier queued row: %+v", laterState)
	}
}

// TestIntegrationSchedulerCapacityPriorityAndReleaseRetry exercises the full
// admission path against real PostgreSQL and real Pool reservations:
// three provider slots minus one live reserve admit the two highest-priority
// recordings, surface the third as queued, then wake and claim it immediately
// when a running recording releases its slot. The long fallback interval
// proves the retry came from the release edge rather than periodic polling.
func TestIntegrationSchedulerCapacityPriorityAndReleaseRetry(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "dvr-capacity-provider", Kind: "m3u_xtream",
		BaseURL: upstream.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "capacity-user", PasswordEnc: []byte{1},
		MaxStreams: 3, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	start := time.Now().Add(500 * time.Millisecond).UTC()
	end := start.Add(20 * time.Second)
	outputDir := t.TempDir()
	makeRecording := func(number float64, title string, priority int) store.DVRRecording {
		ch, err := db.CreateChannel(ctx, store.Channel{Number: number, Name: title, Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: ch.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
			Priority: 0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: ch.ID, Title: title, ScheduledStart: start, ScheduledEnd: end,
			Priority: priority, RequestedBy: "test", OutputPath: outputDir + "/" + title + ".ts",
		})
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}

	// Create the loser first to prove created_at does not outrank priority.
	low := makeRecording(851, "low-priority", 300)
	high := makeRecording(852, "high-priority", 10)
	mid := makeRecording(853, "mid-priority", 20)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = time.Second
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.TickInterval = 30 * time.Second
	scheduler.CapacityRetryInterval = 30 * time.Second
	scheduler.CapacityRetryWindow = 15 * time.Second
	scheduler.LiveReserve = 1

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()

	waitForRecording(t, db, high.ID, 10*time.Second, func(r store.DVRRecording) bool { return r.State == "recording" })
	waitForRecording(t, db, mid.ID, 10*time.Second, func(r store.DVRRecording) bool { return r.State == "recording" })
	queued := waitForRecording(t, db, low.ID, 10*time.Second, func(r store.DVRRecording) bool {
		return r.State == "scheduled" && r.AdmissionState == "queued" && r.AdmissionAttempts > 0
	})
	if queued.AdmissionAttempts < 1 || queued.AdmissionReason == "" || queued.AdmissionRetryAt == nil {
		t.Fatalf("queued conflict not surfaced: %+v", queued)
	}
	// The admin list/detail handlers encode DVRRecording directly. Pin the
	// queued diagnostics in that JSON contract so operators can distinguish a
	// temporary capacity wait from an upstream-media failure.
	payload, err := json.Marshal(queued)
	if err != nil {
		t.Fatal(err)
	}
	var adminView map[string]any
	if err := json.Unmarshal(payload, &adminView); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"AdmissionState", "AdmissionAttempts", "AdmissionRetryAt", "AdmissionReason"} {
		if _, ok := adminView[field]; !ok {
			t.Errorf("admin DVR JSON missing %s: %s", field, payload)
		}
	}

	// Releasing one admitted recording should wake the queued loser now, not
	// after the deliberately-long 30-second fallback retry.
	releasedAt := time.Now()
	if !scheduler.Cancel(high.ID) {
		t.Fatal("high-priority recording was not in flight")
	}
	waitForRecording(t, db, high.ID, 5*time.Second, func(r store.DVRRecording) bool { return r.State == "cancelled" })
	waitForRecording(t, db, low.ID, 8*time.Second, func(r store.DVRRecording) bool { return r.State == "recording" })
	if elapsed := time.Since(releasedAt); elapsed >= scheduler.CapacityRetryInterval {
		t.Fatalf("queued recording waited %s; capacity release did not wake it", elapsed)
	}

	var active int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM active_stream WHERE state IN ('starting','running')`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 2 {
		t.Fatalf("active provider streams=%d, want two DVR streams with one of three slots reserved", active)
	}
}

// Adjacent programs overlap for StartSlack+MaxOverrun. With one provider slot,
// the second recording must remain queued during the first recording's
// post-roll padding, then start from the release edge instead of failing.
func TestIntegrationSchedulerQueuesAcrossPaddingOverlap(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "padding-provider", Kind: "m3u_xtream", BaseURL: upstream.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "padding-user", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	outputDir := t.TempDir()
	firstStart := time.Now().Add(500 * time.Millisecond).UTC()
	firstEnd := firstStart.Add(1500 * time.Millisecond)
	secondStart := firstEnd
	secondEnd := secondStart.Add(5 * time.Second)
	create := func(number float64, title string, start, end time.Time) store.DVRRecording {
		ch, err := db.CreateChannel(ctx, store.Channel{Number: number, Name: title, Enabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: ch.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
			Priority: 0, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: ch.ID, Title: title, ScheduledStart: start, ScheduledEnd: end,
			Priority: 100, RequestedBy: "test", OutputPath: outputDir + "/" + title + ".ts",
		})
		if err != nil {
			t.Fatal(err)
		}
		return rec
	}
	first := create(861, "padding-first", firstStart, firstEnd)
	second := create(862, "padding-second", secondStart, secondEnd)

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = 500 * time.Millisecond
	recorder.MaxOverrun = 1500 * time.Millisecond
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.TickInterval = 50 * time.Millisecond
	scheduler.CapacityRetryInterval = 10 * time.Second
	scheduler.CapacityRetryWindow = 5 * time.Second
	scheduler.LiveReserve = 1 // clamped to zero for this one-slot provider

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		scheduler.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
	}()

	waitForRecording(t, db, first.ID, 5*time.Second, func(r store.DVRRecording) bool { return r.State == "recording" })
	queued := waitForRecording(t, db, second.ID, 5*time.Second, func(r store.DVRRecording) bool {
		return r.State == "scheduled" && r.AdmissionState == "queued" && r.AdmissionAttempts > 0
	})
	if queued.AdmissionAttempts == 0 {
		t.Fatal("padding-overlap conflict was not counted")
	}
	if wait := time.Until(firstEnd.Add(100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	if got, err := db.GetDVRRecording(ctx, first.ID); err != nil || got.State != "recording" {
		t.Fatalf("first recording did not retain its slot in post-roll padding: state=%s err=%v", got.State, err)
	}
	if !scheduler.Cancel(first.ID) {
		t.Fatal("first padded recording was not in flight")
	}
	waitForRecording(t, db, first.ID, 3*time.Second, func(r store.DVRRecording) bool { return r.State == "cancelled" })
	waitForRecording(t, db, second.ID, 4*time.Second, func(r store.DVRRecording) bool { return r.State == "recording" })
}

// If runtime shutdown closes the Pool after the recording claim but before a
// pump starts, Recorder must durably cancel the row, release the reservation,
// and remove its zero-byte partial instead of silently leaving `recording`.
func TestIntegrationRecorderPrePumpPoolShutdownCancelsCleanly(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "pre-pump-shutdown", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "pre-pump", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 866, Name: "pre-pump-shutdown", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID,
		UpstreamURL: "http://unused.test/pre-pump", Priority: 0,
		HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	outputPath := t.TempDir() + "/pre-pump.ts"
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "pre-pump shutdown", Priority: 100,
		ScheduledStart: time.Now(), ScheduledEnd: time.Now().Add(time.Minute),
		RequestedBy: "test", OutputPath: outputPath,
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM dvr_recording WHERE id = $1 FOR UPDATE`, rec.ID); err != nil {
		t.Fatal(err)
	}
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	result := make(chan error, 1)
	go func() { result <- recorder.RunReserved(ctx, rec, reservation) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var blocked bool
		err := db.Pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				 WHERE datname = current_database()
				   AND wait_event_type = 'Lock'
				   AND query LIKE '%FROM dvr_recording%FOR UPDATE%'
			)`).Scan(&blocked)
		if err == nil && blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("recorder never blocked on atomic claim")
		}
		time.Sleep(20 * time.Millisecond)
	}
	pool.Close()
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, stream.ErrPoolClosed) {
			t.Fatalf("pre-pump shutdown err=%v, want ErrPoolClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pre-pump recorder did not finish shutdown cleanup")
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil || got.State != "cancelled" {
		t.Fatalf("pre-pump shutdown row=%+v err=%v, want cancelled", got, err)
	}
	if _, err := os.Stat(recordingCapturePathForTest(outputPath, rec.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-pump shutdown left zero-byte partial: %v", err)
	}
	var active int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM active_stream
		 WHERE state IN ('starting','running') OR client_count <> 0`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("pre-pump shutdown leaked %d active/refcounted leases", active)
	}
}

// A recording can be admitted during its bounded capacity-retry window after
// the scheduled program has already begun. Reaching the normal end deadline
// with real bytes through scheduled_end does not recover the missing start:
// keep the capture as a failed .partial instead of importing it as complete.
func TestIntegrationRecorderLateAdmissionRemainsIncomplete(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-req.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "late-admission", Kind: "m3u_xtream", BaseURL: upstream.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "late-admission", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 868, Name: "late-admission", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	outputPath := t.TempDir() + "/late-admission.ts"
	scheduledEnd := time.Now().Add(700 * time.Millisecond)
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "late admission", Priority: 100,
		ScheduledStart: scheduledEnd.Add(-10 * time.Second), ScheduledEnd: scheduledEnd,
		RequestedBy: "test", OutputPath: outputPath,
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: 0})
	if err != nil {
		t.Fatal(err)
	}
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.MaxOverrun = 0
	recordCtx, cancel := context.WithTimeout(context.Background(), 1300*time.Millisecond)
	defer cancel()
	err = recorder.RunReserved(recordCtx, rec, reservation)
	if !errors.Is(err, dvr.ErrRecordingIncompleteCoverage) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late admission err=%v, want incomplete coverage + deadline", err)
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "failed" || got.BytesWritten == 0 ||
		!strings.Contains(got.Error, dvr.ErrRecordingIncompleteCoverage.Error()) ||
		!strings.Contains(got.Error, "first_write=") {
		t.Fatalf("late admission row not durably incomplete: %+v", got)
	}
	if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late incomplete capture was finalized: %v", err)
	}
	if info, err := os.Stat(recordingCapturePathForTest(outputPath, rec.ID)); err != nil || info.Size() == 0 {
		t.Fatalf("late incomplete partial missing/empty: info=%v err=%v", info, err)
	}
}

// A validated airing can be followed by the provider's finite dead-media
// object during post-roll. The startup classifier must fence that object
// before the DVR sink, allowing the complete valid programme to finalize as
// an explicitly authorized replacement while retaining the prior artifact.
func TestIntegrationRecorderFinalizesValidProgrammeBeforeObservedBlackTail(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffmpeg missing from supported CI image")
		}
		t.Skip("ffmpeg not on PATH")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("ffprobe missing from supported CI image")
		}
		t.Skip("ffprobe not on PATH")
	}

	programme := generateDVRBlackTailTS(t, ffmpeg, "programme.ts",
		"testsrc2=size=320x180:rate=25:duration=2.400",
		"sine=frequency=1000:sample_rate=48000:duration=2.400",
		"2.400", "5M", "25")
	placeholder := generateDVRBlackTailTS(t, ffmpeg, "placeholder.ts",
		"color=c=black:size=128x72:rate=5:duration=600.046",
		"anullsrc=channel_layout=stereo:sample_rate=48000:duration=600.046",
		"600.046", "180k", "5")
	placeholder = padDVRBlackTailToObservedSize(t, placeholder)

	var programmeRequests, placeholderRequests atomic.Int64
	finiteOrigin := func(media []byte, requests *atomic.Int64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			w.Header().Set("Content-Type", "video/mp2t")
			w.Header().Set("Content-Length", fmt.Sprint(len(media)))
			_, _ = w.Write(media)
		}))
	}
	programmeOrigin := finiteOrigin(programme, &programmeRequests)
	defer programmeOrigin.Close()
	var activeChannel atomic.Value
	terminalized := make(chan error, 1)
	placeholderOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		placeholderRequests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Content-Length", fmt.Sprint(len(placeholder)))
		_, _ = w.Write(placeholder)
		channelID, ok := activeChannel.Load().(uuid.UUID)
		if !ok {
			select {
			case terminalized <- errors.New("active channel unavailable"):
			default:
			}
			return
		}
		// Production deliberately retries/slates an exhausted source set for a
		// bounded 90-second window. This focused recorder regression removes the
		// test lease after the complete placeholder response so the next
		// relocation is terminal without adding a minute to the suite; the media
		// classification and capture boundary have already run unmodified.
		_, err := db.Pool.Exec(context.Background(),
			`DELETE FROM active_stream WHERE channel_id=$1`, channelID)
		select {
		case terminalized <- err:
		default:
		}
	}))
	defer placeholderOrigin.Close()

	suffix := uuid.NewString()
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "black-tail-" + suffix, Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "black-tail-" + suffix,
		PasswordEnc: []byte{1}, MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	seedChannelSeq++
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 980 + float64(seedChannelSeq), Name: "black-tail-" + suffix, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	activeChannel.Store(channel.ID)
	// The never-requested third source makes relocation inspect the deleted
	// active row instead of short-circuiting on an all-excluded candidate set.
	for priority, upstream := range []string{
		programmeOrigin.URL, placeholderOrigin.URL, "http://127.0.0.1:1/unused",
	} {
		if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
			ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream,
			Priority: priority, HealthScore: 1, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	output := filepath.Join(t.TempDir(), "episode.ts")
	const title = "FX476 finite black-tail regression"
	ownerEnd := time.Now().Add(-time.Hour).UTC()
	owner, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: title,
		EpisodeNumXMLTV: "0.0.0/1", EpisodeNumOnscreen: "S01E01",
		ScheduledStart: ownerEnd.Add(-time.Hour), ScheduledEnd: ownerEnd,
		Priority: 100, RequestedBy: "op476-black-tail-test", OutputPath: output,
	})
	if err != nil {
		t.Fatal(err)
	}
	priorMedia := []byte("known-good validated predecessor")
	owner = promoteIntegrationArtifact(t, db, owner, priorMedia)

	targetStart := time.Now().Add(time.Second).UTC().Truncate(time.Millisecond)
	targetEnd := targetStart.Add(time.Second)
	var targetProgram uuid.UUID
	if err := db.Pool.QueryRow(ctx, `
		INSERT INTO epg_program (
			channel_id, start_at, end_at, title, episode_num_xmltv,
			episode_num_onscreen, is_canonical, source_hash
		) VALUES ($1,$2,$3,$4,'0.0.0/1','S01E01',true,$5)
		RETURNING id`, channel.ID, targetStart, targetEnd, title,
		"op476-black-tail:"+uuid.NewString()).Scan(&targetProgram); err != nil {
		t.Fatal(err)
	}
	policy := store.DVRAdmissionPolicy{
		LiveReserve: 0, StartSlack: 100 * time.Millisecond, MaxOverrun: 3 * time.Second,
	}
	replacement, err := db.CreateDVRReplacement(ctx, owner.ID, store.DVRRecording{
		ChannelID: channel.ID, ProgramID: &targetProgram, Title: title,
		EpisodeNumXMLTV: "0.0.0/1", EpisodeNumOnscreen: "S01E01",
		ScheduledStart: targetStart, ScheduledEnd: targetEnd,
		Priority: 100, RequestedBy: "op476-black-tail-test", OutputPath: output,
	}, policy)
	if err != nil {
		t.Fatal(err)
	}

	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = policy.StartSlack
	recorder.MaxOverrun = policy.MaxOverrun
	recordCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := recorder.RunReserved(recordCtx, replacement, reservation); err != nil {
		t.Fatalf("record valid programme followed by black tail: %v\n%s", err, logs.String())
	}

	if programmeRequests.Load() != 1 || placeholderRequests.Load() != 1 {
		t.Fatalf("origin requests programme=%d placeholder=%d, want 1/1",
			programmeRequests.Load(), placeholderRequests.Load())
	}
	select {
	case err := <-terminalized:
		if err != nil {
			t.Fatalf("terminalize focused test lease: %v", err)
		}
	default:
		t.Fatal("placeholder response did not reach the terminalization seam")
	}
	replacement, err = db.GetDVRRecording(ctx, replacement.ID)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = db.GetDVRRecording(ctx, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.State != "completed" || !replacement.ArtifactCurrent ||
		replacement.ArtifactState != "validated" || replacement.MediaDurationMS >= 10_000 {
		t.Fatalf("replacement did not finalize from only the valid programme: %+v", replacement)
	}
	if owner.ArtifactCurrent || owner.ArtifactState != "superseded" {
		t.Fatalf("predecessor ownership not transferred: %+v", owner)
	}
	backup := output + ".superseded." + owner.ID.String()
	if got, err := os.ReadFile(backup); err != nil || !bytes.Equal(got, priorMedia) {
		t.Fatalf("preserved predecessor=%q err=%v, want %q", got, err, priorMedia)
	}
	if info, err := os.Stat(output); err != nil || info.Size() <= 0 || info.Size() >= int64(len(placeholder)) {
		t.Fatalf("published valid artifact info=%v err=%v, placeholder bytes=%d",
			info, err, len(placeholder))
	}
	if _, err := os.Stat(recordingCapturePathForTest(output, replacement.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed capture alias still exists: %v", err)
	}
}

const observedDVRBlackPlaceholderBytes = 14_472_616

func generateDVRBlackTailTS(
	t *testing.T,
	ffmpeg, name, videoSource, audioSource, duration, muxRate, keyframe string,
) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	generateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(generateCtx, ffmpeg,
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", videoSource,
		"-f", "lavfi", "-i", audioSource,
		"-t", duration, "-shortest",
		"-c:v", "libx264", "-preset", "ultrafast",
		"-g", keyframe, "-keyint_min", keyframe, "-sc_threshold", "0",
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "64k",
		"-muxrate", muxRate, "-f", "mpegts", path,
	)
	if diagnostic, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate %s: %v: %s", name, err, diagnostic)
	}
	media, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return media
}

func padDVRBlackTailToObservedSize(t *testing.T, media []byte) []byte {
	t.Helper()
	if len(media) > observedDVRBlackPlaceholderBytes {
		t.Fatalf("generated placeholder bytes=%d exceed observed %d",
			len(media), observedDVRBlackPlaceholderBytes)
	}
	remaining := observedDVRBlackPlaceholderBytes - len(media)
	if remaining%188 != 0 {
		t.Fatalf("placeholder padding=%d is not TS-packet aligned", remaining)
	}
	nullPacket := bytes.Repeat([]byte{0xff}, 188)
	nullPacket[0], nullPacket[1], nullPacket[2], nullPacket[3] = 0x47, 0x1f, 0xff, 0x10
	for remaining > 0 {
		media = append(media, nullPacket...)
		remaining -= len(nullPacket)
	}
	return media
}

// Migration 0017 deliberately permits a later active row to reuse a terminal
// row's output_path. The filesystem handoff must nevertheless refuse to
// destroy either a known-good final or a salvageable deterministic .partial.
func TestIntegrationRecorderRefusesPreexistingTerminalArtifacts(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	seedChannelSeq++
	artifactSeq := seedChannelSeq
	artifactSuffix := uuid.NewString()
	upstreamCalled := make(chan struct{}, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case upstreamCalled <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-req.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "terminal-artifacts-" + artifactSuffix,
		Kind: "m3u_xtream", BaseURL: upstream.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "terminal-artifacts-" + artifactSuffix,
		PasswordEnc: []byte{1},
		MaxStreams:  1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 900 + float64(artifactSeq), Name: "terminal-artifacts-" + artifactSuffix,
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	postprocessCalled := make(chan struct{}, 2)
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.MaxOverrun = 0
	recorder.MediaProcessor = copyingMediaProcessor{}
	recorder.PostProcess = &postprocess.Pipeline{
		Logger: logger,
		Stages: []postprocess.Stage{postprocessCallProbe{called: postprocessCalled}},
	}

	createTerminal := func(t *testing.T, title, outputPath, terminal string, offset time.Duration) {
		t.Helper()
		priorEnd := time.Now().Add(offset)
		prior, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: title, Priority: 100,
			ScheduledStart: priorEnd.Add(-time.Hour), ScheduledEnd: priorEnd,
			RequestedBy: "test", OutputPath: outputPath,
		}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: 0})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := db.TryMarkDVRRecordingStarted(ctx, prior.ID, outputPath)
		if err != nil || !claimed {
			t.Fatalf("claim prior terminal row: claimed=%v err=%v", claimed, err)
		}
		switch terminal {
		case "completed":
			result, err := db.MarkDVRRecordingCompleted(ctx, prior.ID, uuid.New(), 1)
			if err != nil || result.State != "completed" || !result.OperationOwned {
				t.Fatalf("complete prior row: result=%+v err=%v", result, err)
			}
		case "cancelled":
			if err := db.MarkDVRRecordingCancelled(ctx, prior.ID); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unsupported terminal state %q", terminal)
		}
	}

	t.Run("known-good final is never replaced", func(t *testing.T) {
		outputPath := t.TempDir() + "/repeat.ts"
		oldMedia := []byte("known-good prior final bytes")
		createTerminal(t, "prior completed airing", outputPath, "completed", -3*time.Hour)
		if err := os.WriteFile(outputPath, oldMedia, 0o644); err != nil {
			t.Fatal(err)
		}
		scheduledEnd := time.Now().Add(700 * time.Millisecond)
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: "later completed airing", Priority: 100,
			ScheduledStart: scheduledEnd.Add(-450 * time.Millisecond), ScheduledEnd: scheduledEnd,
			RequestedBy: "test", OutputPath: outputPath,
		}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: 0})
		if err != nil {
			t.Fatalf("later terminal-path row was not legal: %v", err)
		}
		reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		var activeStreamID uuid.UUID
		if err := db.Pool.QueryRow(ctx, `
			SELECT id FROM active_stream
			 WHERE channel_id = $1
			   AND state IN ('starting', 'running', 'draining')`, channel.ID,
		).Scan(&activeStreamID); err != nil {
			t.Fatal(err)
		}
		recordCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		capturePath := recordingCapturePathForTest(outputPath, rec.ID)
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-recordCtx.Done():
					return
				case <-ticker.C:
					if info, statErr := os.Stat(capturePath); statErr == nil && info.Size() > 0 {
						pool.CancelPumps([]uuid.UUID{activeStreamID})
						return
					}
				}
			}
		}()
		err = recorder.RunReserved(recordCtx, rec, reservation)
		if !errors.Is(err, os.ErrExist) {
			t.Fatalf("existing-final recording err=%v, want no-replace error", err)
		}
		select {
		case <-upstreamCalled:
		default:
			t.Fatal("recording never reached upstream before final-path conflict")
		}
		got, err := db.GetDVRRecording(ctx, rec.ID)
		if err != nil || got.State != "failed" || got.BytesWritten == 0 {
			t.Fatalf("existing-final row=%+v err=%v, want failed capture", got, err)
		}
		var completionOperationMissing bool
		if err := db.Pool.QueryRow(ctx, `
			SELECT completion_operation_id IS NULL FROM dvr_recording WHERE id = $1`, rec.ID,
		).Scan(&completionOperationMissing); err != nil || !completionOperationMissing {
			t.Fatalf("existing-final completion operation missing=%v err=%v",
				completionOperationMissing, err)
		}
		if media, err := os.ReadFile(outputPath); err != nil || string(media) != string(oldMedia) {
			t.Fatalf("prior final=%q err=%v, want %q", media, err, oldMedia)
		}
		if info, err := os.Stat(capturePath); err != nil || info.Size() == 0 {
			t.Fatalf("new capture partial missing: info=%v err=%v", info, err)
		}
		select {
		case <-postprocessCalled:
			t.Fatal("postprocess ran without completion ownership")
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("salvageable partial is never truncated or streamed into", func(t *testing.T) {
		for {
			select {
			case <-upstreamCalled:
			default:
				goto drained
			}
		}
	drained:
		outputPath := t.TempDir() + "/repeat.ts"
		oldPartial := []byte("salvageable prior partial sentinel")
		createTerminal(t, "prior cancelled airing", outputPath, "cancelled", -2*time.Hour)
		scheduledEnd := time.Now().Add(time.Minute)
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID: channel.ID, Title: "later partial-conflict airing", Priority: 100,
			ScheduledStart: time.Now(), ScheduledEnd: scheduledEnd,
			RequestedBy: "test", OutputPath: outputPath,
		}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: 0})
		if err != nil {
			t.Fatalf("later terminal-path row was not legal: %v", err)
		}
		partialPath := recordingCapturePathForTest(outputPath, rec.ID)
		if err := os.WriteFile(partialPath, oldPartial, 0o644); err != nil {
			t.Fatal(err)
		}
		reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		err = recorder.RunReserved(ctx, rec, reservation)
		if !errors.Is(err, os.ErrExist) {
			t.Fatalf("existing-partial recording err=%v, want exclusive-create error", err)
		}
		got, err := db.GetDVRRecording(ctx, rec.ID)
		if err != nil || got.State != "failed" || got.BytesWritten != 0 {
			t.Fatalf("existing-partial row=%+v err=%v, want zero-byte failure", got, err)
		}
		if media, err := os.ReadFile(partialPath); err != nil || string(media) != string(oldPartial) {
			t.Fatalf("prior partial=%q err=%v, want exact sentinel %q", media, err, oldPartial)
		}
		if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("existing-partial attempt published a final: %v", err)
		}
		select {
		case <-upstreamCalled:
			t.Fatal("stream started after exclusive partial acquisition failed")
		case <-time.After(100 * time.Millisecond):
		}
		select {
		case <-postprocessCalled:
			t.Fatal("postprocess ran after exclusive partial acquisition failed")
		case <-time.After(100 * time.Millisecond):
		}
	})
}

// Admin cancellation updates the durable row before it asks Scheduler to
// cancel the goroutine. If completion/failure wins that tiny in-memory race,
// its compare-and-set must still observe the cancelled row, preserve partial
// media, and skip postprocess. Exercise both terminal branches deliberately.
func TestIntegrationRecorderCancellationWinsCompletionAndFailure(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-req.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "cancel-terminal-race", Kind: "m3u_xtream", BaseURL: upstream.URL, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "cancel-terminal-race", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 869, Name: "cancel-terminal-race", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	postprocessCalled := make(chan struct{}, 1)
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.MaxOverrun = 0
	recorder.PostProcess = &postprocess.Pipeline{
		Logger: logger,
		Stages: []postprocess.Stage{postprocessCallProbe{called: postprocessCalled}},
	}

	tests := []struct {
		name              string
		scheduledStartFor func(time.Time) time.Time
	}{
		{
			name: "completion",
			scheduledStartFor: func(end time.Time) time.Time {
				return end.Add(-450 * time.Millisecond)
			},
		},
		{
			name: "failure from late admission",
			scheduledStartFor: func(end time.Time) time.Time {
				return end.Add(-10 * time.Second)
			},
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outputPath := t.TempDir() + "/cancel-race.ts"
			scheduledEnd := time.Now().Add(700 * time.Millisecond)
			rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
				ChannelID: channel.ID, Title: fmt.Sprintf("cancel race %d", i), Priority: 100,
				ScheduledStart: tc.scheduledStartFor(scheduledEnd), ScheduledEnd: scheduledEnd,
				RequestedBy: "test", OutputPath: outputPath,
			}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: 0})
			if err != nil {
				t.Fatal(err)
			}
			reservation, err := pool.ReserveWriter(ctx, channel.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			recordCtx, cancel := context.WithTimeout(context.Background(), 1300*time.Millisecond)
			result := make(chan error, 1)
			go func() { result <- recorder.RunReserved(recordCtx, rec, reservation) }()
			partial := recordingCapturePathForTest(outputPath, rec.ID)
			partialDeadline := time.Now().Add(3 * time.Second)
			for {
				if info, err := os.Stat(partial); err == nil && info.Size() > 0 {
					break
				}
				if time.Now().After(partialDeadline) {
					cancel()
					t.Fatal("recording never received media before cancellation race")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if wait := time.Until(scheduledEnd.Add(50 * time.Millisecond)); wait > 0 {
				time.Sleep(wait)
			}
			if err := db.MarkDVRRecordingCancelled(ctx, rec.ID); err != nil {
				cancel()
				t.Fatal(err)
			}
			select {
			case err := <-result:
				cancel()
				if err != nil {
					t.Fatalf("cancelled terminal race returned error: %v", err)
				}
			case <-time.After(3 * time.Second):
				cancel()
				t.Fatal("recorder did not finish terminal race")
			}
			got, err := db.GetDVRRecording(ctx, rec.ID)
			if err != nil || got.State != "cancelled" {
				t.Fatalf("operator cancellation lost: row=%+v err=%v", got, err)
			}
			if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("cancelled terminal race left final output: %v", err)
			}
			if info, err := os.Stat(partial); err != nil || info.Size() == 0 {
				t.Fatalf("cancelled terminal race lost partial: info=%v err=%v", info, err)
			}
			select {
			case <-postprocessCalled:
				t.Fatal("postprocess ran after operator cancellation won")
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// Scheduler.Wait covers the recorder's durable cancellation, not merely the
// Run loop. Keep the recording row locked during shutdown to prove Wait stays
// blocked while the partial is closed and the lease is released; runtime
// singleton ownership remains held until that cleanup completes.
func TestIntegrationSchedulerWaitsForRecorderCleanupBeforeRuntimeHandoff(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	owner, err := db.AcquireRuntimeSingleton(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close(context.Background()) }()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		block := make([]byte, 188*7)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := w.Write(block); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()
	provider, err := db.CreateProvider(ctx, store.Provider{
		Name: "scheduler-shutdown", Kind: "m3u_xtream", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCredential(ctx, store.ProviderCredential{
		ProviderID: provider.ID, Username: "scheduler-shutdown", PasswordEnc: []byte{1},
		MaxStreams: 1, Priority: 100, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	channel, err := db.CreateChannel(ctx, store.Channel{
		Number: 867, Name: "scheduler-shutdown", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateChannelSource(ctx, store.ChannelSource{
		ChannelID: channel.ID, ProviderID: provider.ID, UpstreamURL: upstream.URL,
		Priority: 0, HealthScore: 1, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	outputPath := t.TempDir() + "/scheduler-shutdown.ts"
	start := time.Now().Add(250 * time.Millisecond)
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "scheduler shutdown", Priority: 100,
		ScheduledStart: start, ScheduledEnd: start.Add(time.Minute),
		RequestedBy: "test", OutputPath: outputPath,
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	pool := stream.NewPool(logger, db, store.PassthroughResolverForTest{})
	defer pool.Close()
	recorder := dvr.NewRecorder(logger, db, pool, nil)
	recorder.StartSlack = time.Second
	recorder.MaxOverrun = time.Second
	scheduler := dvr.NewScheduler(logger, db, recorder)
	scheduler.LiveReserve = 0
	scheduler.TickInterval = 30 * time.Second
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go scheduler.Run(runCtx)
	waitForRecording(t, db, rec.ID, 5*time.Second, func(r store.DVRRecording) bool {
		return r.State == "recording"
	})
	partial := recordingCapturePathForTest(outputPath, rec.ID)
	partialDeadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Stat(partial); err == nil && info.Size() > 0 {
			break
		}
		if time.Now().After(partialDeadline) {
			t.Fatal("recording partial never received bytes")
		}
		time.Sleep(20 * time.Millisecond)
	}
	lockTx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		`SELECT 1 FROM dvr_recording WHERE id = $1 FOR UPDATE`, rec.ID); err != nil {
		t.Fatal(err)
	}
	scheduler.Close()
	waitResult := make(chan error, 1)
	go func() {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer waitCancel()
		waitResult <- scheduler.Wait(waitCtx)
	}()
	select {
	case err := <-waitResult:
		t.Fatalf("Scheduler.Wait returned before durable cancellation lock released: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if second, err := db.AcquireRuntimeSingleton(ctx); !errors.Is(err, store.ErrRuntimeAlreadyActive) {
		if second != nil {
			_ = second.Close(ctx)
		}
		t.Fatalf("runtime ownership released before scheduler cleanup: %v", err)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitResult:
		if err != nil {
			t.Fatalf("Scheduler.Wait after cancellation unlock: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Scheduler.Wait did not join recorder cleanup")
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil || got.State != "cancelled" {
		t.Fatalf("scheduler shutdown row=%+v err=%v, want cancelled", got, err)
	}
	var active int
	if err := db.Pool.QueryRow(ctx, `
		SELECT COUNT(*)::int FROM active_stream
		 WHERE state IN ('starting','running') OR client_count <> 0`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("scheduler shutdown leaked %d active/refcounted leases", active)
	}

	// A terminal DB failure must make Wait fail closed rather than claiming
	// clean quiescence. Start one more recording, then reject only its
	// recording→cancelled update with a trigger.
	secondOutput := t.TempDir() + "/scheduler-terminal-failure.ts"
	secondStart := time.Now().Add(250 * time.Millisecond)
	secondRec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID: channel.ID, Title: "scheduler terminal failure", Priority: 100,
		ScheduledStart: secondStart, ScheduledEnd: secondStart.Add(time.Minute),
		RequestedBy: "test", OutputPath: secondOutput,
	}, store.DVRAdmissionPolicy{LiveReserve: 0, StartSlack: time.Second, MaxOverrun: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	secondScheduler := dvr.NewScheduler(logger, db, recorder)
	secondScheduler.LiveReserve = 0
	secondScheduler.TickInterval = 30 * time.Second
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	go secondScheduler.Run(secondCtx)
	waitForRecording(t, db, secondRec.ID, 5*time.Second, func(r store.DVRRecording) bool {
		return r.State == "recording"
	})
	if _, err := db.Pool.Exec(ctx, `
		CREATE FUNCTION test_fail_scheduler_terminal_state() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.state = 'cancelled' THEN
				RAISE EXCEPTION 'forced scheduler terminal failure';
			END IF;
			RETURN NEW;
		END
		$$`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		CREATE TRIGGER test_fail_scheduler_terminal_state
		BEFORE UPDATE ON dvr_recording
		FOR EACH ROW EXECUTE FUNCTION test_fail_scheduler_terminal_state()`); err != nil {
		t.Fatal(err)
	}
	secondScheduler.Close()
	terminalCtx, terminalCancel := context.WithTimeout(context.Background(), 5*time.Second)
	terminalErr := secondScheduler.Wait(terminalCtx)
	terminalCancel()
	if !errors.Is(terminalErr, dvr.ErrRecordingTerminalState) ||
		!strings.Contains(terminalErr.Error(), "forced scheduler terminal failure") {
		t.Fatalf("Scheduler.Wait swallowed terminal state failure: %v", terminalErr)
	}
	stuck, err := db.GetDVRRecording(ctx, secondRec.ID)
	if err != nil || stuck.State != "recording" {
		t.Fatalf("terminal update failure did not leave expected restart-recovery row: %+v err=%v", stuck, err)
	}
	if _, err := db.Pool.Exec(ctx,
		`DROP TRIGGER test_fail_scheduler_terminal_state ON dvr_recording`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx,
		`DROP FUNCTION test_fail_scheduler_terminal_state()`); err != nil {
		t.Fatal(err)
	}
	recovered, err := db.ReconcileStartup(ctx)
	if err != nil || recovered.DVRRecordings != 1 {
		t.Fatalf("startup recovery after terminal write failure=%+v err=%v", recovered, err)
	}
	recoveredRec, err := db.GetDVRRecording(ctx, secondRec.ID)
	if err != nil || recoveredRec.State != "failed" ||
		!strings.Contains(recoveredRec.Error, "interrupted by conductor restart") {
		t.Fatalf("terminal write recovery row=%+v err=%v", recoveredRec, err)
	}
	pool.Close()
	pumpCtx, pumpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.WaitForPumps(pumpCtx); err != nil {
		pumpCancel()
		t.Fatal(err)
	}
	pumpCancel()
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reacquired, err := db.AcquireRuntimeSingleton(ctx)
	if err != nil {
		t.Fatalf("runtime ownership not available after quiescence: %v", err)
	}
	if err := reacquired.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestIntegrationTorznabCaps verifies the caps response is well-formed
// XML with the categories Sonarr/Radarr expect.
func TestIntegrationTorznabCaps(t *testing.T) {
	db := freshDB(t)
	tn := dvr.New(db, "http://test.local:8409", "test-key", "Test Indexer")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/indexer/torznab/api?t=caps&apikey=test-key", nil)
	tn.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("caps: got %d want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		`<server title="Conductor DVR Indexer"/>`,
		`<tv-search available="yes"`,
		`<movie-search available="yes"`,
		`<category id="2000" name="Movies">`,
		`<category id="5000" name="TV">`,
		`<subcat id="5040" name="TV/HD"/>`,
		`<subcat id="5060" name="TV/Sport"/>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("caps missing %q\n--- body ---\n%s", want, body)
		}
	}
}

func TestIntegrationTorznabAuthRequired(t *testing.T) {
	db := freshDB(t)
	tn := dvr.New(db, "http://test.local:8409", "test-key", "Test Indexer")

	for _, q := range []string{
		"/indexer/torznab/api?t=caps",
		"/indexer/torznab/api?t=caps&apikey=wrong",
		"/indexer/torznab/api?t=tvsearch&q=heat",
	} {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, q, nil)
		tn.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("%s: got %d want 401", q, rr.Code)
		}
	}
}

// TestIntegrationTvsearchReturnsEpgHits seeds an upcoming episode, hits
// the tvsearch endpoint, and verifies the RSS contains a properly-shaped
// item with both season+episode attrs + a signed download URL.
func TestIntegrationTvsearchReturnsEpgHits(t *testing.T) {
	db := freshDB(t)
	_, _, _, _ = seedFutureProgram(t, db, "The Bear", "S03E07", false, 24*time.Hour, time.Hour)

	tn := dvr.New(db, "http://test.local:8409", "test-key", "Test Indexer")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/indexer/torznab/api?t=tvsearch&apikey=test-key&q=The+Bear&season=3&ep=7", nil)
	tn.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("tvsearch: got %d want 200", rr.Code)
	}
	body := rr.Body.String()

	for _, want := range []string{
		`<rss version="2.0"`,
		`<title>The Bear S03E07`,
		`Conductor-DVR`,
		`<guid isPermaLink="false">conductor-`,
		`/dvr/schedule.torrent?`,
		`apikey=test-key`,
		`name="season" value="3"`,
		`name="episode" value="7"`,
		`<category>5000</category>`,
		`<category>5040</category>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("tvsearch missing %q\n--- body ---\n%s", want, body)
		}
	}

	// Validate XML parses.
	if err := xml.Unmarshal(rr.Body.Bytes(), new(struct {
		XMLName xml.Name `xml:"rss"`
	})); err != nil {
		t.Errorf("rss does not parse: %v", err)
	}
}

// TestIntegrationScheduleHandlerCreatesRecording end-to-ends the grab path:
// build a signed URL → POST it → verify a dvr_recording row appears.
func TestIntegrationScheduleHandlerCreatesRecording(t *testing.T) {
	db := freshDB(t)
	chIDStr, progIDStr, start, end := seedFutureProgram(t, db, "Heat", "", true, 48*time.Hour, 2*time.Hour)
	_ = chIDStr

	tn := dvr.New(db, "http://test.local:8409", "test-key", "Test Indexer")
	sched := &dvr.ScheduleHandler{
		DB: db, Torznab: tn, OutputDir: "/tmp/conductor-dvr-test", IndexerKey: "test-key",
	}

	hits, err := db.SearchEPGForIndexer(context.Background(),
		store.EPGIndexerSearch{Query: "Heat", MovieOnly: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	scheduleURL := tn.SignedScheduleURLForTest(hits[0])

	// Hit the schedule handler with the signed URL (rewrite path to /dvr/...).
	idx := strings.Index(scheduleURL, "?")
	req := httptest.NewRequest(http.MethodGet, "/dvr/schedule.torrent"+scheduleURL[idx:], nil)
	req.Header.Set("User-Agent", "Sonarr/4.0")
	rr := httptest.NewRecorder()
	sched.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("grab: got %d want 200, body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Type"); got != "application/x-bittorrent" {
		t.Errorf("Content-Type: got %q", got)
	}
	if rr.Body.Len() == 0 {
		t.Error("expected non-empty .torrent body")
	}

	// Verify dvr_recording row exists.
	rows, err := db.ListDVRRecordings(context.Background(), "scheduled", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 scheduled recording, got %d", len(rows))
	}
	r := rows[0]
	if r.Title != "Heat" {
		t.Errorf("title: got %q want Heat", r.Title)
	}
	if !r.IsMovie {
		t.Error("IsMovie should be true")
	}
	if r.RequestedBy != "torznab:sonarr" {
		t.Errorf("requested_by: got %q want torznab:sonarr", r.RequestedBy)
	}
	if !strings.Contains(r.OutputPath, "Movies/Heat") {
		t.Errorf("output_path should be under Movies/Heat: got %q", r.OutputPath)
	}
	if !strings.HasSuffix(r.OutputPath, ".ts") {
		t.Errorf("output_path should end .ts: got %q", r.OutputPath)
	}
	if !r.ScheduledStart.Equal(start) || !r.ScheduledEnd.Equal(end) {
		t.Errorf("window: got [%v, %v] want [%v, %v]",
			r.ScheduledStart, r.ScheduledEnd, start, end)
	}
	if r.Priority != store.DefaultDVRPriority || r.AdmissionState != "pending" {
		t.Errorf("default admission metadata: priority=%d state=%q, want %d/pending",
			r.Priority, r.AdmissionState, store.DefaultDVRPriority)
	}

	// Re-grab — Sonarr might issue the same request twice.
	// Should be idempotent (same row, no duplicate).
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/dvr/schedule.torrent"+scheduleURL[idx:], nil)
	req2.Header.Set("User-Agent", "Sonarr/4.0")
	sched.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("re-grab: got %d", rr2.Code)
	}
	rows2, _ := db.ListDVRRecordings(context.Background(), "scheduled", 10)
	if len(rows2) != 1 {
		t.Errorf("re-grab should be idempotent; got %d rows", len(rows2))
	}

	// Pending list should contain it (within 50h window).
	pending, err := db.ListPendingDVRRecordings(context.Background(), 50*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Errorf("ListPendingDVRRecordings expected 1, got %d", len(pending))
	}

	_ = progIDStr // silence unused
}

// TestIntegrationScheduleHandlerRejectsTamperedURL verifies the HMAC
// guard on signed URLs — Sonarr can't construct a schedule URL itself.
func TestIntegrationScheduleHandlerRejectsTamperedURL(t *testing.T) {
	db := freshDB(t)
	_, _, _, _ = seedFutureProgram(t, db, "Tampered", "S01E01", false, time.Hour, time.Hour)

	tn := dvr.New(db, "http://test.local:8409", "test-key", "Test Indexer")
	sched := &dvr.ScheduleHandler{
		DB: db, Torznab: tn, OutputDir: "/tmp/conductor-dvr-test", IndexerKey: "test-key",
	}

	hits, _ := db.SearchEPGForIndexer(context.Background(),
		store.EPGIndexerSearch{Query: "Tampered", Limit: 10})
	url := tn.SignedScheduleURLForTest(hits[0])
	idx := strings.Index(url, "?")

	// Strip sig — should fail.
	tampered := strings.Replace(url[idx:], "sig=", "sig=zz", 1)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dvr/schedule.torrent"+tampered, nil)
	sched.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("tampered sig should 400; got %d", rr.Code)
	}
}

// TestIntegrationIndexerSearchFiltersAndGuardsFlood is the regression test
// for the DVR-indexer flooding bug: a movie/tvsearch that arrives with
// only an external id — or with nothing at all — must NOT dump the whole
// upcoming catalog. It must filter by id via the enrichment table, and an
// unfiltered search must return zero items.
func TestIntegrationIndexerSearchFiltersAndGuardsFlood(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	// Three upcoming movies, each on its own channel.
	_, avengersID, _, _ := seedFutureProgram(t, db, "The Avengers", "", true, 24*time.Hour, 2*time.Hour)
	seedFutureProgram(t, db, "Frozen", "", true, 26*time.Hour, 2*time.Hour)
	seedFutureProgram(t, db, "Heat", "", true, 28*time.Hour, 2*time.Hour)

	// Enrich only "The Avengers" with its real TMDb + IMDb ids. The other
	// two have no enrichment row, so an id search must not return them.
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO epg_program_enrichment (program_id, tmdb_id, imdb_id)
		 VALUES ($1::uuid, $2, $3)`,
		avengersID, 24428, "tt0848228"); err != nil {
		t.Fatalf("seed enrichment: %v", err)
	}

	tn := dvr.New(db, "http://test.local:8409", "test-key", "Test Indexer")

	itemCount := func(t *testing.T, rawURL string) int {
		t.Helper()
		rr := httptest.NewRecorder()
		tn.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, rawURL, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: got HTTP %d want 200", rawURL, rr.Code)
		}
		return strings.Count(rr.Body.String(), "<item>")
	}

	const base = "/indexer/torznab/api?apikey=test-key"
	cases := []struct {
		name string
		url  string
		want int
	}{
		{"movie, no params — guarded, no flood", base + "&t=movie", 0},
		{"tvsearch, no params — guarded, no flood", base + "&t=tvsearch", 0},
		{"movie, year only — year is not a primary filter", base + "&t=movie&year=2012", 0},
		{"movie by title", base + "&t=movie&q=Avengers", 1},
		{"movie by tmdbid", base + "&t=movie&tmdbid=24428", 1},
		{"movie by imdbid, tt-prefixed", base + "&t=movie&imdbid=tt0848228", 1},
		{"movie by imdbid, bare zero-padded", base + "&t=movie&imdbid=0848228", 1},
		{"movie by unknown tmdbid", base + "&t=movie&tmdbid=999999", 0},
		{"movie by title + matching imdbid", base + "&t=movie&q=Avengers&imdbid=tt0848228", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := itemCount(t, tc.url); got != tc.want {
				t.Errorf("item count = %d, want %d", got, tc.want)
			}
		})
	}
}

func promoteIntegrationArtifact(
	t *testing.T,
	db *store.DB,
	rec store.DVRRecording,
	content []byte,
) store.DVRRecording {
	t.Helper()
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Dir(rec.OutputPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rec.OutputPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	claimed, err := db.TryMarkDVRRecordingStarted(ctx, rec.ID, rec.OutputPath)
	if err != nil || !claimed {
		t.Fatalf("claim artifact owner: claimed=%v err=%v", claimed, err)
	}
	if err := db.MarkDVRArtifactStage(
		ctx, rec.ID, "publishing", rec.OutputPath+".normalized",
	); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	filesystemID, err := dvr.ArtifactFilesystemIdentity(rec.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.PromoteDVRRecordingArtifact(
		ctx,
		rec.ID,
		uuid.New(),
		int64(len(content)),
		store.DVRMediaReport{DurationMS: 3_600_000},
		digest[:],
		filesystemID,
		"op476-filesystem-test-v1",
		rec.OutputPath,
		"",
	)
	if err != nil || result.State != "completed" || !result.OperationOwned {
		t.Fatalf("promote artifact owner: result=%+v err=%v", result, err)
	}
	got, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func newIntegrationArtifactIntent(
	t *testing.T,
	db *store.DB,
	title, output string,
	startsIn time.Duration,
) store.DVRRecording {
	t.Helper()
	chID, progID, start, end := seedFutureProgram(
		t, db, title, "S01E01", false, startsIn, time.Hour)
	channelID := uuid.MustParse(chID)
	programID := uuid.MustParse(progID)
	return store.DVRRecording{
		ChannelID: channelID, ProgramID: &programID, Title: title,
		EpisodeNumOnscreen: "S01E01",
		ScheduledStart:     start, ScheduledEnd: end,
		Priority: 100, RequestedBy: "op476-filesystem-test",
		OutputPath: output,
	}
}

func TestIntegrationCurrentArtifactFilesystemReconciliation(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	root := t.TempDir()

	newIntent := func(
		title, output string,
		startsIn time.Duration,
	) store.DVRRecording {
		return newIntegrationArtifactIntent(t, db, title, output, startsIn)
	}

	t.Run("usable validated owner still suppresses", func(t *testing.T) {
		output := filepath.Join(root, "usable.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Usable owner", output, 2*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		owner = promoteIntegrationArtifact(t, db, owner, []byte("validated-media"))

		got, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Usable repeat", output, 4*time.Hour))
		if !errors.Is(err, store.ErrValidatedArtifactExists) || got.ID != owner.ID {
			t.Fatalf("usable owner suppression: got=%+v err=%v", got, err)
		}
	})

	t.Run("missing validated owner is demoted and repeat schedules", func(t *testing.T) {
		output := filepath.Join(root, "missing.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Missing owner", output, 6*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		owner = promoteIntegrationArtifact(t, db, owner, []byte("validated-media"))
		if err := os.Remove(output); err != nil {
			t.Fatal(err)
		}

		repeat, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Missing repeat", output, 8*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if repeat.State != "scheduled" || repeat.ReplacesRecordingID != nil {
			t.Fatalf("missing-owner repeat=%+v, want scheduled no-replace", repeat)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if owner.ArtifactCurrent || owner.ArtifactState != "missing" ||
			!strings.Contains(owner.ArtifactError, "missing") {
			t.Fatalf("missing owner not demoted: %+v", owner)
		}
	})

	t.Run("first-seen missing legacy owner stays current", func(t *testing.T) {
		output := filepath.Join(root, "unbound-missing.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Unbound missing owner", output, 9*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		owner = promoteIntegrationArtifact(t, db, owner, []byte("legacy-before-bootstrap"))
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_state='legacy', artifact_sha256=NULL,
			       artifact_filesystem_id='', artifact_validated_at=NULL,
			       artifact_validator_version=''
			 WHERE id=$1`, owner.ID); err != nil {
			t.Fatal(err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(output); err != nil {
			t.Fatal(err)
		}

		check, err := dvr.ReconcileCurrentDVRArtifact(ctx, db, owner)
		var inspectionErr *dvr.IndeterminateArtifactInspectionError
		if !errors.As(err, &inspectionErr) || check.Usable || check.Demoted {
			t.Fatalf("unbound missing owner check=%+v err=%v, want retained", check, err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !owner.ArtifactCurrent || owner.ArtifactState != "legacy" ||
			owner.ArtifactFilesystemID != "" {
			t.Fatalf("unbound missing owner changed: %+v", owner)
		}
	})

	t.Run("startup scan demotes changed byte evidence", func(t *testing.T) {
		output := filepath.Join(root, "changed.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Changed owner", output, 10*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		owner = promoteIntegrationArtifact(t, db, owner, []byte("validated-media"))
		if err := os.WriteFile(output, []byte("changed"), 0o644); err != nil {
			t.Fatal(err)
		}

		reconciled, err := dvr.ReconcileCurrentDVRArtifacts(ctx, db, slog.Default())
		if err != nil {
			t.Fatal(err)
		}
		if reconciled.Demoted != 1 || reconciled.Indeterminate != 1 {
			t.Fatalf("startup scan=%+v, want one demotion plus retained unbound owner",
				reconciled)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if owner.ArtifactCurrent || owner.ArtifactState != "missing" ||
			!strings.Contains(owner.ArtifactError, "byte evidence changed") {
			t.Fatalf("changed owner not demoted: %+v", owner)
		}
	})

	t.Run("same-size bit flip demotes validated owner", func(t *testing.T) {
		output := filepath.Join(root, "bitflip.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Bit-flipped owner", output, 11*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		content := []byte("same-size-validated-media")
		owner = promoteIntegrationArtifact(t, db, owner, content)
		changed := append([]byte(nil), content...)
		changed[len(changed)/2] ^= 0x20
		if err := os.WriteFile(output, changed, 0o644); err != nil {
			t.Fatal(err)
		}

		check, err := dvr.ReconcileCurrentDVRArtifact(ctx, db, owner)
		if err != nil || check.Usable || !check.Demoted {
			t.Fatalf("bit-flipped owner check=%+v err=%v, want demoted", check, err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if owner.ArtifactCurrent || owner.ArtifactState != "missing" ||
			!strings.Contains(owner.ArtifactError, "SHA-256 evidence changed") {
			t.Fatalf("bit-flipped owner not demoted with hash evidence: %+v", owner)
		}
	})

	t.Run("legacy owner remains size-only", func(t *testing.T) {
		output := filepath.Join(root, "legacy.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Legacy owner", output, 115*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		content := []byte("legacy-size-only-media")
		owner = promoteIntegrationArtifact(t, db, owner, content)
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_state='legacy', artifact_sha256=NULL,
			       artifact_filesystem_id='', artifact_validated_at=NULL,
			       artifact_validator_version=''
			 WHERE id=$1`, owner.ID); err != nil {
			t.Fatal(err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		changed := append([]byte(nil), content...)
		changed[0] ^= 0x20
		if err := os.WriteFile(output, changed, 0o644); err != nil {
			t.Fatal(err)
		}

		check, err := dvr.ReconcileCurrentDVRArtifact(ctx, db, owner)
		if err != nil || !check.Usable || check.Demoted {
			t.Fatalf("legacy owner check=%+v err=%v, want size-only usable", check, err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if owner.ArtifactFilesystemID == "" {
			t.Fatalf("legacy owner did not bind filesystem identity: %+v", owner)
		}
		repeatIntent := newIntent("Legacy usable repeat", output, 116*time.Minute)
		repeat, err := dvr.CreateDVRRecordingChecked(ctx, db, repeatIntent)
		if err != nil || repeat.State != "scheduled" || repeat.ReplacesRecordingID == nil ||
			*repeat.ReplacesRecordingID != owner.ID {
			t.Fatalf("usable legacy successor=%+v err=%v", repeat, err)
		}
	})

	t.Run("indeterminate legacy preflight leaves no runnable successor", func(t *testing.T) {
		output := filepath.Join(root, "legacy-indeterminate.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Legacy indeterminate owner", output, 117*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		owner = promoteIntegrationArtifact(t, db, owner, []byte("legacy-indeterminate-media"))
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET artifact_state='legacy', artifact_sha256=NULL,
			       artifact_validated_at=NULL, artifact_validator_version=''
			 WHERE id=$1`, owner.ID); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(output); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(root, "legacy-absent-target.ts"), output); err != nil {
			t.Fatal(err)
		}

		repeatIntent := newIntent(
			"Legacy indeterminate repeat", output, 119*time.Minute)
		returned, err := dvr.CreateDVRRecordingChecked(ctx, db, repeatIntent)
		var inspectionErr *dvr.IndeterminateArtifactInspectionError
		if !errors.As(err, &inspectionErr) || returned.ID != owner.ID {
			t.Fatalf("legacy preflight result=%+v err=%v, want retained owner", returned, err)
		}
		var successorRows int
		if err := db.Pool.QueryRow(ctx, `
			SELECT count(*) FROM dvr_recording
			 WHERE channel_id=$1 AND scheduled_start=$2 AND title=$3`,
			repeatIntent.ChannelID, repeatIntent.ScheduledStart,
			repeatIntent.Title).Scan(&successorRows); err != nil {
			t.Fatal(err)
		}
		if successorRows != 0 {
			t.Fatalf("indeterminate preflight left %d runnable successor row(s)", successorRows)
		}
	})

	t.Run("dangling canonical entry remains fail closed", func(t *testing.T) {
		output := filepath.Join(root, "dangling.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Dangling owner", output, 12*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		owner = promoteIntegrationArtifact(t, db, owner, []byte("validated-media"))
		if err := os.Remove(output); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(root, "absent-target.ts"), output); err != nil {
			t.Fatal(err)
		}

		check, err := dvr.ReconcileCurrentDVRArtifact(ctx, db, owner)
		var inspectionErr *dvr.IndeterminateArtifactInspectionError
		if !errors.As(err, &inspectionErr) || check.Usable || check.Demoted {
			t.Fatalf("dangling owner check=%+v err=%v, want fail closed", check, err)
		}
		if _, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Dangling repeat", output, 13*time.Hour),
		); !errors.As(err, &inspectionErr) {
			t.Fatalf("automated scheduling error=%v, want typed fail-closed inspection", err)
		}
		target := newIntent("Dangling replacement target", output, 135*time.Minute)
		handler := &dvr.ScheduleHandler{DB: db, Priority: 100}
		if _, err := handler.ScheduleReplacement(
			ctx, owner.ID, *target.ProgramID, "op476-dangling-test",
		); !errors.As(err, &inspectionErr) {
			t.Fatalf("explicit replacement error=%v, want typed fail-closed inspection", err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !owner.ArtifactCurrent || owner.ArtifactState != "validated" {
			t.Fatalf("dangling owner lost current ownership: %+v", owner)
		}
	})

	t.Run("publishing replacement fences predecessor demotion", func(t *testing.T) {
		output := filepath.Join(root, "publishing.ts")
		owner, err := dvr.CreateDVRRecordingChecked(
			ctx, db, newIntent("Publishing owner", output, 14*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		ownerBytes := []byte("validated-predecessor-media")
		owner = promoteIntegrationArtifact(t, db, owner, ownerBytes)

		targetStart := time.Now().Add(16 * time.Hour).UTC().Truncate(time.Second)
		var targetID uuid.UUID
		if err := db.Pool.QueryRow(ctx, `
			INSERT INTO epg_program (
				channel_id, start_at, end_at, title, episode_num_xmltv,
				episode_num_onscreen, is_canonical, source_hash
			) VALUES ($1,$2,$3,$4,'0.0.0/1','S01E01',true,$5)
			RETURNING id`, owner.ChannelID, targetStart, targetStart.Add(time.Hour),
			owner.Title, "op476-publishing:"+uuid.NewString()).Scan(&targetID); err != nil {
			t.Fatal(err)
		}
		replacement, err := db.CreateDVRReplacement(ctx, owner.ID, store.DVRRecording{
			ChannelID: owner.ChannelID, ProgramID: &targetID, Title: owner.Title,
			EpisodeNumXMLTV: "0.0.0/1", EpisodeNumOnscreen: "S01E01",
			ScheduledStart: targetStart, ScheduledEnd: targetStart.Add(time.Hour),
			Priority: 100, RequestedBy: "op476-publishing-test", OutputPath: output,
		})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := db.TryMarkDVRRecordingStarted(ctx, replacement.ID, output)
		if err != nil || !claimed {
			t.Fatalf("claim replacement: claimed=%v err=%v", claimed, err)
		}
		candidatePath := output + ".normalized.under-test"
		if err := db.MarkDVRArtifactStage(
			ctx, replacement.ID, "publishing", candidatePath,
		); err != nil {
			t.Fatal(err)
		}

		backup := output + ".predecessor.under-test"
		if err := os.Rename(output, backup); err != nil {
			t.Fatal(err)
		}
		candidateBytes := []byte("different-sized-validated-replacement-media")
		if err := os.WriteFile(output, candidateBytes, 0o644); err != nil {
			t.Fatal(err)
		}

		check, err := dvr.ReconcileCurrentDVRArtifact(ctx, db, owner)
		if !errors.Is(err, store.ErrDVRArtifactPublishInProgress) ||
			check.Usable || check.Demoted {
			t.Fatalf("publishing predecessor check=%+v err=%v", check, err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		replacement, err = db.GetDVRRecording(ctx, replacement.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !owner.ArtifactCurrent || replacement.ReplacesRecordingID == nil ||
			*replacement.ReplacesRecordingID != owner.ID ||
			replacement.ArtifactStage != "publishing" {
			t.Fatalf("publishing ownership fence lost provenance: owner=%+v successor=%+v",
				owner, replacement)
		}

		candidateSHA := sha256.Sum256(candidateBytes)
		filesystemID, err := dvr.ArtifactFilesystemIdentity(output)
		if err != nil {
			t.Fatal(err)
		}
		result, err := db.PromoteDVRRecordingArtifact(
			ctx, replacement.ID, uuid.New(), int64(len(candidateBytes)),
			store.DVRMediaReport{DurationMS: 3_600_000}, candidateSHA[:], filesystemID,
			"op476-publishing-test-v1", output, backup)
		if err != nil || result.State != "completed" || !result.OperationOwned {
			t.Fatalf("promote fenced replacement: result=%+v err=%v", result, err)
		}
		owner, err = db.GetDVRRecording(ctx, owner.ID)
		if err != nil {
			t.Fatal(err)
		}
		replacement, err = db.GetDVRRecording(ctx, replacement.ID)
		if err != nil {
			t.Fatal(err)
		}
		if owner.ArtifactCurrent || owner.ArtifactState != "superseded" ||
			!replacement.ArtifactCurrent || replacement.ArtifactState != "validated" {
			t.Fatalf("fenced promotion did not transfer ownership: owner=%+v successor=%+v",
				owner, replacement)
		}
	})
}

func TestIntegrationCurrentArtifactStartupContinuesAfterIndeterminate(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()
	root := t.TempDir()

	indeterminatePath := filepath.Join(root, "a-indeterminate.ts")
	missingPath := filepath.Join(root, "b-missing.ts")
	indeterminate, err := dvr.CreateDVRRecordingChecked(ctx, db,
		newIntegrationArtifactIntent(
			t, db, "Indeterminate startup owner", indeterminatePath, 2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	indeterminate = promoteIntegrationArtifact(
		t, db, indeterminate, []byte("indeterminate-owner-media"))
	missing, err := dvr.CreateDVRRecordingChecked(ctx, db,
		newIntegrationArtifactIntent(
			t, db, "Missing startup owner", missingPath, 4*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	missing = promoteIntegrationArtifact(t, db, missing, []byte("missing-owner-media"))

	if err := os.Remove(indeterminatePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "unmounted-target.ts"), indeterminatePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(missingPath); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciled, err := dvr.ReconcileCurrentDVRArtifacts(ctx, db, logger)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Indeterminate != 1 || reconciled.Demoted != 1 {
		t.Fatalf("startup reconcile=%+v, want one retained and one demoted", reconciled)
	}
	indeterminate, err = db.GetDVRRecording(ctx, indeterminate.ID)
	if err != nil {
		t.Fatal(err)
	}
	missing, err = db.GetDVRRecording(ctx, missing.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !indeterminate.ArtifactCurrent || indeterminate.ArtifactState != "validated" {
		t.Fatalf("indeterminate owner was not retained: %+v", indeterminate)
	}
	if missing.ArtifactCurrent || missing.ArtifactState != "missing" {
		t.Fatalf("later missing owner was not demoted: %+v", missing)
	}
}

func TestIntegrationCurrentArtifactDatabaseFailuresRemainFatal(t *testing.T) {
	t.Run("list failure", func(t *testing.T) {
		db := freshDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reconciled, err := dvr.ReconcileCurrentDVRArtifacts(ctx, db, slog.Default())
		var inspectionErr *dvr.IndeterminateArtifactInspectionError
		if err == nil || errors.As(err, &inspectionErr) ||
			reconciled.Demoted != 0 || reconciled.Indeterminate != 0 {
			t.Fatalf("reconcile=%+v err=%v, want fatal database list error", reconciled, err)
		}
	})

	t.Run("demotion failure", func(t *testing.T) {
		db := freshDB(t)
		ctx := context.Background()
		output := filepath.Join(t.TempDir(), "demotion-failure.ts")
		owner, err := dvr.CreateDVRRecordingChecked(ctx, db,
			newIntegrationArtifactIntent(
				t, db, "Demotion failure owner", output, 2*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		owner = promoteIntegrationArtifact(t, db, owner, []byte("demotion-failure-media"))
		if err := os.Remove(output); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool.Exec(ctx, `
			ALTER TABLE dvr_recording
			ADD CONSTRAINT op476_reject_missing_demotion_chk
			CHECK (artifact_state <> 'missing') NOT VALID`); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = db.Pool.Exec(context.Background(), `
				ALTER TABLE dvr_recording
				DROP CONSTRAINT IF EXISTS op476_reject_missing_demotion_chk`)
		})

		reconciled, err := dvr.ReconcileCurrentDVRArtifacts(ctx, db, slog.Default())
		var inspectionErr *dvr.IndeterminateArtifactInspectionError
		if err == nil || errors.As(err, &inspectionErr) || reconciled.Demoted != 0 {
			t.Fatalf("reconcile=%+v err=%v, want fatal demotion transaction error", reconciled, err)
		}
		owner, getErr := db.GetDVRRecording(ctx, owner.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if !owner.ArtifactCurrent || owner.ArtifactState != "validated" {
			t.Fatalf("failed demotion changed owner: %+v", owner)
		}
	})
}
