package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	runtimeSingletonLockName = "conductor:runtime-singleton"

	// DefaultRuntimeSingletonCheckInterval bounds how long a process can keep
	// serving after PostgreSQL has dropped the session that owns the runtime
	// lock. The caller must terminate the service when Monitor returns an
	// error; reconnecting would otherwise permit split-brain ownership.
	DefaultRuntimeSingletonCheckInterval = time.Second
	runtimeSingletonProbeTimeout         = time.Second
)

// ErrRuntimeAlreadyActive means another Conductor process owns this database.
// Conductor's stream pumps are process-local and startup reconciliation marks
// old active_stream rows dead, so overlapping processes are intentionally
// rejected rather than pretending their pumps can share database rows.
var ErrRuntimeAlreadyActive = errors.New("another conductor runtime is active")

// RuntimeSingleton owns a PostgreSQL session advisory lock on a dedicated
// pooled connection. It must be acquired before startup reconciliation and
// held until all process-local stream pumps and workers have stopped.
type RuntimeSingleton struct {
	mu         sync.Mutex
	conn       *pgxpool.Conn
	backendPID int32
	closed     bool
}

// AcquireRuntimeSingleton attempts to become the sole Conductor runtime for
// this database. It never waits behind another runtime: a busy lock fails
// closed so a second process cannot reconcile or attach to foreign pumps.
func (db *DB) AcquireRuntimeSingleton(ctx context.Context) (*RuntimeSingleton, error) {
	if db == nil || db.Pool == nil {
		return nil, errors.New("db not open")
	}
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire runtime singleton connection: %w", err)
	}

	var acquired bool
	var backendPID int32
	err = conn.QueryRow(ctx, `
		SELECT pg_try_advisory_lock(hashtextextended($1, 0)),
		       pg_backend_pid()`, runtimeSingletonLockName).Scan(&acquired, &backendPID)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire runtime singleton lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, ErrRuntimeAlreadyActive
	}
	return &RuntimeSingleton{conn: conn, backendPID: backendPID}, nil
}

// Monitor verifies that the exact PostgreSQL session owning the advisory lock
// remains alive. A pgxpool.Conn cannot transparently swap its underlying
// session while acquired; checking the backend PID also makes that invariant
// explicit. Any error while ctx remains live is fatal to runtime ownership.
func (s *RuntimeSingleton) Monitor(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultRuntimeSingletonCheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			probeCtx, cancel := context.WithTimeout(ctx, runtimeSingletonProbeTimeout)
			err := s.check(probeCtx)
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

func (s *RuntimeSingleton) check(ctx context.Context) error {
	if s == nil {
		return errors.New("runtime singleton is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn == nil {
		return errors.New("runtime singleton is closed")
	}
	var backendPID int32
	if err := s.conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&backendPID); err != nil {
		return fmt.Errorf("runtime singleton connection lost: %w", err)
	}
	if backendPID != s.backendPID {
		return fmt.Errorf("runtime singleton session changed from backend %d to %d", s.backendPID, backendPID)
	}
	return nil
}

// Close explicitly unlocks before returning the dedicated connection to the
// pool. If unlock cannot be proven, the session is hijacked and closed so a
// pooled idle connection can never retain the singleton accidentally.
func (s *RuntimeSingleton) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn == nil {
		return nil
	}
	s.closed = true
	conn := s.conn
	s.conn = nil

	var unlocked bool
	err := conn.QueryRow(ctx,
		`SELECT pg_advisory_unlock(hashtextextended($1, 0))`,
		runtimeSingletonLockName).Scan(&unlocked)
	if err == nil && unlocked {
		conn.Release()
		return nil
	}

	// Release would put a healthy-but-still-locked session back into the pool.
	// Remove it from the pool and close the physical connection instead.
	raw := conn.Hijack()
	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	closeErr := raw.Close(closeCtx)
	cancel()
	if err != nil {
		return errors.Join(fmt.Errorf("unlock runtime singleton: %w", err), closeErr)
	}
	if !unlocked {
		err = errors.New("runtime singleton advisory lock was not held")
	}
	return errors.Join(err, closeErr)
}
