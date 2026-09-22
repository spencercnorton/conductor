package dvr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
)

// CurrentArtifactCheck distinguishes a usable owner from the exact owner this
// call demoted. Demoted is important to a recorder that must refresh its local
// replacement provenance before choosing the no-replace publish primitive;
// Reason preserves the exact deterministic evidence used for that transition.
type CurrentArtifactCheck struct {
	Usable  bool
	Demoted bool
	Reason  string
}

// IndeterminateArtifactInspectionError means filesystem state prevented a
// conclusive integrity decision. Callers must retain the current owner: the
// canonical entry may still be valid, while authorizing a replacement could
// overwrite media that merely lives on a temporarily unavailable mount.
//
// Database and ownership-transition errors deliberately use other types so
// startup can continue past only this safe, fail-closed condition.
type IndeterminateArtifactInspectionError struct {
	RecordingID uuid.UUID
	Path        string
	Operation   string
	Err         error
}

func (e *IndeterminateArtifactInspectionError) Error() string {
	return fmt.Sprintf(
		"indeterminate DVR artifact inspection recording=%s path=%q operation=%q: %v",
		e.RecordingID, e.Path, e.Operation, e.Err)
}

func (e *IndeterminateArtifactInspectionError) Unwrap() error { return e.Err }

// CurrentDVRArtifactReconcileSummary exposes both startup outcomes. Demoted
// owners no longer suppress later airings; Indeterminate owners are retained
// and surfaced for operator action while reconciliation continues.
type CurrentDVRArtifactReconcileSummary struct {
	Demoted                   int
	Indeterminate             int
	InspectionBudgetExhausted bool
	InspectionWorkerMayRemain bool
}

// DefaultStartupDVRArtifactInspectionBudget bounds aggregate startup
// filesystem inspection. Hashing is best-effort at startup; synchronous
// schedule/re-record checks remain unbounded and fail closed.
const DefaultStartupDVRArtifactInspectionBudget = 15 * time.Second

type currentDVRArtifactStore interface {
	ListCurrentDVRArtifacts(context.Context) ([]store.DVRRecording, error)
	BindCurrentDVRArtifactFilesystem(context.Context, uuid.UUID, string, string) error
	DemoteMissingDVRArtifact(context.Context, uuid.UUID, string, string, string) (bool, error)
}

type artifactReadFile interface {
	io.Reader
	Stat() (os.FileInfo, error)
	Close() error
}

type artifactFilesystem interface {
	Stat(string) (os.FileInfo, error)
	Lstat(string) (os.FileInfo, error)
	Open(string) (artifactReadFile, error)
}

type osArtifactFilesystem struct{}

func (osArtifactFilesystem) Stat(path string) (os.FileInfo, error)  { return os.Stat(path) }
func (osArtifactFilesystem) Lstat(path string) (os.FileInfo, error) { return os.Lstat(path) }
func (osArtifactFilesystem) Open(path string) (artifactReadFile, error) {
	return os.Open(path)
}

// ArtifactFilesystemIdentity returns the portable durable identity used to
// bind current DVR ownership to the filesystem containing a canonical path.
// It intentionally identifies the filesystem device, not the file inode.
func ArtifactFilesystemIdentity(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("DVR artifact path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	filesystemID, ok := artifactFilesystemID(info)
	if !ok {
		return "", errors.New("filesystem device identity is unavailable")
	}
	return filesystemID, nil
}

type currentArtifactInspection struct {
	usable         bool
	demotionReason string
	filesystemID   string
}

func indeterminateArtifactInspection(
	rec store.DVRRecording,
	operation string,
	err error,
) error {
	if err == nil {
		err = errors.New("filesystem identity or metadata changed during inspection")
	}
	return &IndeterminateArtifactInspectionError{
		RecordingID: rec.ID,
		Path:        rec.OutputPath,
		Operation:   operation,
		Err:         err,
	}
}

func stableArtifactFileInfo(before, after os.FileInfo) bool {
	if !os.SameFile(before, after) ||
		before.Mode() != after.Mode() ||
		before.Size() != after.Size() ||
		!before.ModTime().Equal(after.ModTime()) {
		return false
	}
	beforeChange, beforeOK := artifactChangeVersionOf(before)
	afterChange, afterOK := artifactChangeVersionOf(after)
	return beforeOK && afterOK && beforeChange == afterChange
}

type artifactContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r artifactContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// inspectCurrentDVRArtifact performs the filesystem-only half of ownership
// reconciliation. Deterministic absence or integrity mismatch returns a
// demotion reason. Access/read errors and any path or inode race return a
// typed indeterminate error so ownership stays fail-closed.
func inspectCurrentDVRArtifact(
	ctx context.Context,
	rec store.DVRRecording,
	fsys artifactFilesystem,
) (currentArtifactInspection, error) {
	if err := ctx.Err(); err != nil {
		return currentArtifactInspection{}, err
	}
	if rec.ArtifactState == "validated" && len(rec.ArtifactSHA256) != sha256.Size {
		return currentArtifactInspection{}, fmt.Errorf(
			"validated current DVR artifact %s has persisted SHA-256 length %d, want %d",
			rec.ID, len(rec.ArtifactSHA256), sha256.Size)
	}

	pathInfo, statErr := fsys.Stat(rec.OutputPath)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	observedFilesystemID := ""
	if statErr == nil {
		filesystemID, ok := artifactFilesystemID(pathInfo)
		if !ok {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "derive canonical artifact filesystem identity",
				errors.New("filesystem device identity is unavailable"))
		}
		if rec.ArtifactFilesystemID != "" && rec.ArtifactFilesystemID != filesystemID {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact filesystem identity",
				fmt.Errorf("filesystem=%q database=%q",
					filesystemID, rec.ArtifactFilesystemID))
		}
		observedFilesystemID = filesystemID
	}
	switch {
	case statErr == nil && !pathInfo.Mode().IsRegular():
		return currentArtifactInspection{demotionReason: fmt.Sprintf(
			"canonical DVR artifact is not a regular file: mode=%s", pathInfo.Mode()),
			filesystemID: observedFilesystemID}, nil
	case statErr == nil && pathInfo.Size() <= 0:
		return currentArtifactInspection{
			demotionReason: "canonical DVR artifact is empty",
			filesystemID:   observedFilesystemID,
		}, nil
	case statErr == nil:
		if rec.ArtifactBytes > 0 && pathInfo.Size() != rec.ArtifactBytes {
			return currentArtifactInspection{demotionReason: fmt.Sprintf(
				"canonical DVR artifact byte evidence changed: filesystem=%d database=%d",
				pathInfo.Size(), rec.ArtifactBytes),
				filesystemID: observedFilesystemID}, nil
		}
		// A legacy owner has no trustworthy digest. Preserve the migration's
		// deliberately limited size-only behavior until an explicit re-record
		// replaces it with a validated artifact.
		if rec.ArtifactState == "legacy" {
			return currentArtifactInspection{
				usable: true, filesystemID: observedFilesystemID,
			}, nil
		}
	case errors.Is(statErr, os.ErrNotExist):
		// os.Stat also returns ENOENT for a dangling symlink. Its directory entry
		// would make no-replace publication fail, so do not call it absent.
		_, lstatErr := fsys.Lstat(rec.OutputPath)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		if lstatErr == nil {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "resolve canonical filesystem entry",
				errors.New("dangling or unreadable filesystem entry"))
		} else if !errors.Is(lstatErr, os.ErrNotExist) {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "inspect canonical filesystem entry", lstatErr)
		}
		// ENOENT is conclusive only when an earlier successful inspection bound
		// this owner to a durable filesystem identity and the containing directory
		// is still reachable on that exact filesystem. Parent existence alone does
		// not prove that a NAS or removable filesystem is mounted.
		if rec.ArtifactFilesystemID == "" {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify missing canonical artifact filesystem identity",
				errors.New("owner has no durable filesystem identity evidence"))
		}
		parentInfo, parentErr := fsys.Stat(filepath.Dir(rec.OutputPath))
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		if parentErr != nil {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact parent directory", parentErr)
		}
		if !parentInfo.IsDir() {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact parent directory",
				fmt.Errorf("parent mode=%s", parentInfo.Mode()))
		}
		parentFilesystemID, ok := artifactFilesystemID(parentInfo)
		if !ok {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "derive canonical artifact parent filesystem identity",
				errors.New("filesystem device identity is unavailable"))
		}
		if parentFilesystemID != rec.ArtifactFilesystemID {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify missing canonical artifact parent filesystem identity",
				fmt.Errorf("parent=%q database=%q",
					parentFilesystemID, rec.ArtifactFilesystemID))
		}
		return currentArtifactInspection{
			demotionReason: "canonical DVR artifact is missing from the filesystem",
			filesystemID:   rec.ArtifactFilesystemID,
		}, nil
	default:
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "stat canonical artifact", statErr)
	}

	file, err := fsys.Open(rec.OutputPath)
	if ctxErr := ctx.Err(); ctxErr != nil {
		if err == nil {
			_ = file.Close()
		}
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "open canonical artifact for SHA-256", err)
	}
	defer file.Close()

	openedInfo, err := file.Stat()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "inspect opened canonical artifact", err)
	}
	if !stableArtifactFileInfo(pathInfo, openedInfo) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify opened canonical artifact identity", nil)
	}

	hash := sha256.New()
	readBytes, err := io.CopyBuffer(
		hash,
		artifactContextReader{ctx: ctx, r: file},
		make([]byte, 256*1024),
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "read canonical artifact for SHA-256", err)
	}
	if err := ctx.Err(); err != nil {
		return currentArtifactInspection{}, err
	}
	if readBytes != pathInfo.Size() {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify canonical artifact byte count after hashing",
			fmt.Errorf("read=%d stat=%d", readBytes, pathInfo.Size()))
	}

	afterReadInfo, err := file.Stat()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "reinspect opened canonical artifact after hashing", err)
	}
	if !stableArtifactFileInfo(openedInfo, afterReadInfo) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify opened canonical artifact stability after hashing", nil)
	}
	afterPathInfo, err := fsys.Stat(rec.OutputPath)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "reinspect canonical path after hashing", err)
	}
	if !stableArtifactFileInfo(pathInfo, afterPathInfo) ||
		!os.SameFile(afterReadInfo, afterPathInfo) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify canonical path stability after hashing", nil)
	}

	actualSHA := hash.Sum(nil)
	if !bytes.Equal(actualSHA, rec.ArtifactSHA256) {
		return currentArtifactInspection{demotionReason: fmt.Sprintf(
			"canonical DVR artifact SHA-256 evidence changed: filesystem=%x database=%x",
			actualSHA, rec.ArtifactSHA256),
			filesystemID: observedFilesystemID}, nil
	}
	return currentArtifactInspection{
		usable: true, filesystemID: observedFilesystemID,
	}, nil
}

// ReconcileCurrentDVRArtifact checks filesystem existence, size, and persisted
// SHA-256 evidence before trusting a validated database current-owner marker.
// Legacy owners retain their migration-era size-only treatment. It deliberately
// demotes only a proven absent/invalid artifact; permission, read, dangling-link,
// and path-race errors remain fail-closed instead of authorizing replacement.
func ReconcileCurrentDVRArtifact(
	ctx context.Context,
	db *store.DB,
	rec store.DVRRecording,
) (CurrentArtifactCheck, error) {
	if db == nil {
		return CurrentArtifactCheck{}, errors.New("DVR artifact database and owner are required")
	}
	return reconcileCurrentDVRArtifactWith(
		ctx, db, rec, osArtifactFilesystem{})
}

func validateCurrentDVRArtifactOwner(rec store.DVRRecording) (bool, error) {
	if rec.ID == uuid.Nil {
		return false, errors.New("DVR artifact owner identity is required")
	}
	if !rec.ArtifactCurrent ||
		(rec.ArtifactState != "legacy" && rec.ArtifactState != "validated") {
		return false, nil
	}
	if strings.TrimSpace(rec.OutputPath) == "" ||
		(rec.ArtifactPath != "" && rec.ArtifactPath != rec.OutputPath) {
		return false, fmt.Errorf(
			"current DVR artifact %s has inconsistent canonical paths output=%q artifact=%q",
			rec.ID, rec.OutputPath, rec.ArtifactPath)
	}
	return true, nil
}

func applyCurrentDVRArtifactInspection(
	ctx context.Context,
	db currentDVRArtifactStore,
	rec store.DVRRecording,
	inspection currentArtifactInspection,
) (CurrentArtifactCheck, error) {
	if strings.TrimSpace(inspection.filesystemID) == "" {
		return CurrentArtifactCheck{}, fmt.Errorf(
			"current DVR artifact %s inspection has no filesystem identity evidence",
			rec.ID)
	}
	if rec.ArtifactFilesystemID == "" {
		if !inspection.usable {
			return CurrentArtifactCheck{}, indeterminateArtifactInspection(
				rec, "verify invalid artifact against durable filesystem identity",
				errors.New("owner has no previously trusted filesystem identity"))
		}
		if err := db.BindCurrentDVRArtifactFilesystem(
			ctx, rec.ID, rec.OutputPath, inspection.filesystemID,
		); err != nil {
			return CurrentArtifactCheck{}, err
		}
		rec.ArtifactFilesystemID = inspection.filesystemID
	} else if rec.ArtifactFilesystemID != inspection.filesystemID {
		return CurrentArtifactCheck{}, store.ErrDVRArtifactFilesystemConflict
	}
	if inspection.usable {
		return CurrentArtifactCheck{Usable: true}, nil
	}
	if strings.TrimSpace(inspection.demotionReason) == "" {
		return CurrentArtifactCheck{}, fmt.Errorf(
			"current DVR artifact %s inspection produced no decision", rec.ID)
	}

	demoted, err := db.DemoteMissingDVRArtifact(
		ctx, rec.ID, rec.OutputPath, rec.ArtifactFilesystemID,
		inspection.demotionReason)
	if err != nil {
		return CurrentArtifactCheck{}, err
	}
	check := CurrentArtifactCheck{Demoted: demoted}
	if demoted {
		check.Reason = inspection.demotionReason
	}
	return check, nil
}

func reconcileCurrentDVRArtifactWith(
	ctx context.Context,
	db currentDVRArtifactStore,
	rec store.DVRRecording,
	fsys artifactFilesystem,
) (CurrentArtifactCheck, error) {
	eligible, err := validateCurrentDVRArtifactOwner(rec)
	if err != nil || !eligible {
		return CurrentArtifactCheck{}, err
	}

	inspection, err := inspectCurrentDVRArtifact(ctx, rec, fsys)
	if err != nil {
		return CurrentArtifactCheck{}, err
	}
	return applyCurrentDVRArtifactInspection(ctx, db, rec, inspection)
}

// ReconcileCurrentDVRArtifacts runs after runtime ownership and interrupted
// publish recovery at startup. Missing imports cannot remain durable owners
// that suppress every future airing or force replacement publication to fail.
func ReconcileCurrentDVRArtifacts(
	ctx context.Context,
	db *store.DB,
	logger *slog.Logger,
) (CurrentDVRArtifactReconcileSummary, error) {
	if db == nil {
		return CurrentDVRArtifactReconcileSummary{},
			errors.New("DVR artifact database is required")
	}
	return reconcileCurrentDVRArtifactsWith(
		ctx, db, logger, osArtifactFilesystem{},
		DefaultStartupDVRArtifactInspectionBudget)
}

type startupArtifactInspectionResult struct {
	index      int
	inspection currentArtifactInspection
	err        error
}

func retainStartupArtifactsAfterInspectionDeadline(
	ctx context.Context,
	logger *slog.Logger,
	rows []store.DVRRecording,
	processed int,
	summary CurrentDVRArtifactReconcileSummary,
	budget time.Duration,
	workerDone <-chan struct{},
) (CurrentDVRArtifactReconcileSummary, error) {
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	remaining := len(rows) - processed
	if remaining < 0 {
		remaining = 0
	}
	summary.Indeterminate += remaining
	summary.InspectionBudgetExhausted = true
	select {
	case <-workerDone:
		// The worker returned before the coordinator observed its deadline.
	default:
		// Never join here: a regular-file syscall can be uninterruptible. The
		// single FS-only worker may remain, but it owns no database capability.
		summary.InspectionWorkerMayRemain = true
		metrics.DVRArtifactStartupInspectionDetachments.Inc()
	}
	if logger != nil && remaining > 0 {
		logger.Warn("retained current DVR artifacts after startup inspection budget exhausted",
			"budget", budget, "retained_count", remaining,
			"inspection_worker_may_remain", summary.InspectionWorkerMayRemain,
			"first_unreconciled_recording_id", rows[processed].ID,
			"first_unreconciled_path", rows[processed].OutputPath)
	}
	return summary, nil
}

func reconcileCurrentDVRArtifactsWith(
	ctx context.Context,
	db currentDVRArtifactStore,
	logger *slog.Logger,
	fsys artifactFilesystem,
	budget time.Duration,
) (CurrentDVRArtifactReconcileSummary, error) {
	if budget <= 0 {
		return CurrentDVRArtifactReconcileSummary{},
			errors.New("startup DVR artifact inspection budget must be positive")
	}
	rows, err := db.ListCurrentDVRArtifacts(ctx)
	if err != nil {
		return CurrentDVRArtifactReconcileSummary{}, err
	}
	for _, rec := range rows {
		eligible, validateErr := validateCurrentDVRArtifactOwner(rec)
		if validateErr != nil {
			return CurrentDVRArtifactReconcileSummary{}, validateErr
		}
		if !eligible {
			return CurrentDVRArtifactReconcileSummary{}, fmt.Errorf(
				"database listed non-current DVR artifact %s for startup inspection", rec.ID)
		}
	}
	if len(rows) == 0 {
		return CurrentDVRArtifactReconcileSummary{}, nil
	}

	inspectionCtx, cancelInspection := context.WithTimeout(ctx, budget)
	defer cancelInspection()
	results := make(chan startupArtifactInspectionResult)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for index, rec := range rows {
			inspection, inspectErr := inspectCurrentDVRArtifact(
				inspectionCtx, rec, fsys)
			select {
			case results <- startupArtifactInspectionResult{
				index: index, inspection: inspection, err: inspectErr,
			}:
			case <-inspectionCtx.Done():
				return
			}
		}
	}()

	var summary CurrentDVRArtifactReconcileSummary
	processed := 0
	for processed < len(rows) {
		if inspectionCtx.Err() != nil {
			return retainStartupArtifactsAfterInspectionDeadline(
				ctx, logger, rows, processed, summary, budget, workerDone)
		}
		select {
		case <-inspectionCtx.Done():
			return retainStartupArtifactsAfterInspectionDeadline(
				ctx, logger, rows, processed, summary, budget, workerDone)
		case result := <-results:
			// A result racing the aggregate deadline must not authorize a late
			// database transition. Retain it and every unstarted row instead.
			if inspectionCtx.Err() != nil {
				return retainStartupArtifactsAfterInspectionDeadline(
					ctx, logger, rows, processed, summary, budget, workerDone)
			}
			if result.index != processed {
				return summary, fmt.Errorf(
					"startup DVR artifact inspection order changed: got=%d want=%d",
					result.index, processed)
			}
			rec := rows[processed]
			if result.err != nil {
				var inspectionErr *IndeterminateArtifactInspectionError
				if errors.As(result.err, &inspectionErr) {
					summary.Indeterminate++
					if logger != nil {
						logger.Warn("retained current DVR artifact after indeterminate inspection",
							"recording_id", rec.ID, "path", rec.OutputPath,
							"artifact_state", rec.ArtifactState, "err", result.err)
					}
					processed++
					continue
				}
				return summary, result.err
			}
			// Only this coordinator performs PostgreSQL ownership transitions.
			// It deliberately uses the parent context, never the expiring
			// filesystem-inspection context, so a timed-out goroutine cannot leave
			// a database mutation running after this function returns.
			check, applyErr := applyCurrentDVRArtifactInspection(
				ctx, db, rec, result.inspection)
			if applyErr != nil {
				var inspectionErr *IndeterminateArtifactInspectionError
				if errors.As(applyErr, &inspectionErr) {
					summary.Indeterminate++
					if logger != nil {
						logger.Warn("retained current DVR artifact after indeterminate inspection",
							"recording_id", rec.ID, "path", rec.OutputPath,
							"artifact_state", rec.ArtifactState, "err", applyErr)
					}
					processed++
					continue
				}
				return summary, applyErr
			}
			if check.Demoted {
				summary.Demoted++
				if logger != nil {
					logger.Warn("demoted invalid current DVR artifact",
						"recording_id", rec.ID, "path", rec.OutputPath,
						"artifact_state", rec.ArtifactState,
						"reason", check.Reason)
				}
			}
			processed++
		}
	}
	return summary, nil
}

// CreateDVRRecordingChecked wraps automated scheduling with filesystem
// reconciliation. The store first discovers any current owner under the
// output-path lock without inserting a runnable successor. After this function
// reconciles that exact owner, the store atomically requires it to remain
// current before insertion. This keeps inspection failures, process exits, and
// owner races from leaving an unverified scheduled row for the DVR scheduler to
// claim.
func CreateDVRRecordingChecked(
	ctx context.Context,
	db *store.DB,
	intent store.DVRRecording,
	policies ...store.DVRAdmissionPolicy,
) (store.DVRRecording, error) {
	var checkedOwner *store.DVRArtifactOwnerProof
	for attempt := 0; attempt < 4; attempt++ {
		rec, err := db.CreateDVRRecordingWithArtifactPreflight(
			ctx, intent, checkedOwner, policies...)
		switch {
		case errors.Is(err, store.ErrDVRArtifactOwnerChanged):
			// Ownership moved after inspection but before the transactional
			// insertion check. Restart against the fresh current owner; the
			// failed attempt inserted nothing.
			checkedOwner = nil
			continue
		case errors.Is(err, store.ErrDVRArtifactOwnerPreflightRequired):
			check, checkErr := ReconcileCurrentDVRArtifact(ctx, db, rec)
			if checkErr != nil {
				return rec, checkErr
			}
			if check.Demoted {
				checkedOwner = nil
				continue
			}
			if !check.Usable {
				return rec, store.ErrArtifactReplacementMismatch
			}
			owner, ownerErr := db.GetDVRRecording(ctx, rec.ID)
			if ownerErr != nil {
				return rec, ownerErr
			}
			proof := store.NewDVRArtifactOwnerProof(owner)
			checkedOwner = &proof
			continue
		default:
			return rec, err
		}
	}
	return store.DVRRecording{}, errors.New("DVR artifact owner changed repeatedly while scheduling")
}
