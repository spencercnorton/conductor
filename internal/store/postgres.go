// Package store owns Postgres connectivity, schema migrations, and typed
// repository helpers used by the rest of the binary.
//
// Connection pool: pgx/v5/pgxpool, sized for "one Plex server + one operator
// dashboard" (modest). Tune CONDUCTOR_PG_MAX_CONNS if you run multiple Plex
// instances against the same Conductor.
//
// Transactions: every slot lease + active_stream insert + refcount mutation
// runs in a single explicit tx. See lease.go for the load-bearing algorithm.
package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool

	// sdAuthority is the process-local admission gate for operations that also
	// take the cross-process Schedules Direct advisory lock: a complete SD pass,
	// channel remap, or EPG source creation. Keeping waiters outside pool
	// transactions prevents a burst of remaps from exhausting the connections
	// the active SD pass needs in order to publish and release its lease.
	sdAuthorityOnce sync.Once
	sdAuthority     chan struct{}

	// leaseCommitResultHook is a deterministic integration-test seam for the
	// otherwise network-only case where PostgreSQL commits but the client loses
	// the COMMIT response. Production DBs leave it nil.
	leaseCommitResultHook func() error
	// leaseBeforeCommitHook pauses an acquisition after its durable identities
	// exist inside the transaction but before COMMIT. Integration tests use it
	// to prove cleanup fences an acquisition whose outcome is still invisible.
	// Production DBs leave it nil.
	leaseBeforeCommitHook func(Lease) error
	// dvrCompletionCommitResultHook is the equivalent seam for the DVR terminal
	// transition. Keeping it separate prevents an ambiguity regression from
	// perturbing lease setup performed by the same test.
	dvrCompletionCommitResultHook func() error
	// hardStaleCommitResultHook simulates a lost COMMIT response after exact
	// hard-stale finalization. Production DBs leave it nil.
	hardStaleCommitResultHook func() error
}

func (db *DB) acquireSDAuthority(ctx context.Context) (func(), error) {
	db.sdAuthorityOnce.Do(func() {
		db.sdAuthority = make(chan struct{}, 1)
	})
	select {
	case db.sdAuthority <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() { <-db.sdAuthority })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (db *DB) applyLeaseCommitResultHook(err error) error {
	if err != nil || db.leaseCommitResultHook == nil {
		return err
	}
	return db.leaseCommitResultHook()
}

func (db *DB) applyDVRCompletionCommitResultHook(err error) error {
	if err != nil || db.dvrCompletionCommitResultHook == nil {
		return err
	}
	return db.dvrCompletionCommitResultHook()
}

func (db *DB) applyHardStaleCommitResultHook(err error) error {
	if err != nil || db.hardStaleCommitResultHook == nil {
		return err
	}
	return db.hardStaleCommitResultHook()
}

// Open dials Postgres and returns a ready-to-use DB. The caller must Close.
//
// dsn is a libpq-style connection string, e.g.
//
//	postgres://conductor:secret@localhost:5432/conductor?sslmode=disable
func Open(ctx context.Context, dsn string) (*DB, error) {
	if dsn == "" {
		return nil, errors.New("postgres DSN is empty")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 1 * time.Minute
	if cfg.MaxConns < 8 {
		cfg.MaxConns = 8
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &DB{Pool: pool}, nil
}

func (db *DB) Close() {
	if db == nil || db.Pool == nil {
		return
	}
	db.Pool.Close()
}

// Ping exercises the pool. Used by /readyz.
func (db *DB) Ping(ctx context.Context) error {
	if db == nil || db.Pool == nil {
		return errors.New("db not open")
	}
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return db.Pool.Ping(c)
}

// InTx runs fn inside a transaction with READ COMMITTED isolation (the
// default; lease.go's correctness depends on FOR UPDATE row locks, NOT on
// snapshot isolation). Commits on nil error, rolls back otherwise.
func (db *DB) InTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // noop if already committed

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Migrate applies all embedded migrations not yet recorded in
// schema_migrations, in lexical order. Each migration runs in its own tx.
//
// Migration filenames follow `NNNN_description.sql`. The first 4 chars are
// the version key recorded in schema_migrations.
func (db *DB) Migrate(ctx context.Context) error {
	if _, err := db.Pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
		    version    text PRIMARY KEY,
		    name       text NOT NULL,
		    applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	migs, err := LoadMigrations()
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}

	applied, err := loadApplied(ctx, db.Pool)
	if err != nil {
		return err
	}

	for _, m := range migs {
		v := versionFromName(m.Name)
		if _, ok := applied[v]; ok {
			continue
		}
		err := db.InTx(ctx, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return fmt.Errorf("apply %s: %w", m.Name, err)
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
				v, m.Name)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func loadApplied(ctx context.Context, pool *pgxpool.Pool) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = struct{}{}
	}
	return out, rows.Err()
}

func versionFromName(name string) string {
	// "0001_initial_schema.sql" → "0001"
	if len(name) >= 4 {
		return name[:4]
	}
	return name
}
