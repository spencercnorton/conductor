package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spencercnorton/conductor/internal/store"
)

// Production v0.39.2 is migrated through 0018. Prove that exact historical
// schema already contains stream_state=draining, then apply 0019 unchanged and
// verify its replacement credential-load index covers a draining stream.
func TestIntegrationDVRAdmissionMigrationFrom0018Schema(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	dsn := startTestPostgres(t)
	schema := "op473_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	root, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = root.Close(ctx)
		t.Fatal(err)
	}
	_ = root.Close(ctx)
	t.Cleanup(func() {
		conn, err := pgx.Connect(context.Background(), dsn)
		if err == nil {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			_ = conn.Close(context.Background())
		}
	})

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	migrations, err := store.LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var migration19 store.Migration
	lastApplied := ""
	for _, migration := range migrations {
		if migration.Name == "0019_dvr_capacity_admission.sql" {
			migration19 = migration
			break
		}
		if err := applyDVRAdmissionMigration(ctx, pool, migration); err != nil {
			t.Fatalf("apply pre-0019 %s: %v", migration.Name, err)
		}
		lastApplied = migration.Name
	}
	if migration19.Name == "" || lastApplied != "0018_ppv_sync_channel_state.sql" {
		t.Fatalf("migration boundary missing: last=%q migration19=%q",
			lastApplied, migration19.Name)
	}

	var labels string
	if err := pool.QueryRow(ctx, `
		SELECT string_agg(e.enumlabel::text, ',' ORDER BY e.enumsortorder)
		  FROM pg_enum e
		 WHERE e.enumtypid = 'stream_state'::regtype`).Scan(&labels); err != nil {
		t.Fatal(err)
	}
	if labels != "starting,running,draining,dead" {
		t.Fatalf("pre-0019 stream_state labels=%q, want historical draining label", labels)
	}

	var streamID, credentialID uuid.UUID
	if err := pool.QueryRow(ctx, `
		WITH inserted_provider AS (
			INSERT INTO provider (name, kind, base_url)
			VALUES ('0019-upgrade-provider', 'm3u_plain', 'http://provider.test')
			RETURNING id
		), inserted_credential AS (
			INSERT INTO provider_credential
			       (provider_id, username, password_enc, max_streams)
			SELECT id, '0019-upgrade', decode('01', 'hex'), 1
			  FROM inserted_provider
			RETURNING id, provider_id
		), inserted_channel AS (
			INSERT INTO channel (number, name)
			VALUES (9473.1, '0019 upgrade channel')
			RETURNING id
		), inserted_source AS (
			INSERT INTO channel_source (channel_id, provider_id, upstream_url)
			SELECT inserted_channel.id, inserted_credential.provider_id,
			       'http://provider.test/stream'
			  FROM inserted_channel, inserted_credential
			RETURNING id, channel_id
		), inserted_stream AS (
			INSERT INTO active_stream
			       (channel_id, channel_source_id, credential_id, upstream_url,
			        client_count, state)
			SELECT inserted_source.channel_id, inserted_source.id,
			       inserted_credential.id, 'http://provider.test/stream', 0, 'draining'
			  FROM inserted_source, inserted_credential
			RETURNING id, credential_id
		)
		SELECT id, credential_id FROM inserted_stream`).Scan(&streamID, &credentialID); err != nil {
		t.Fatalf("seed pre-0019 draining stream: %v", err)
	}

	if err := applyDVRAdmissionMigration(ctx, pool, migration19); err != nil {
		t.Fatalf("apply 0019 to exact pre-0019 schema: %v", err)
	}

	var predicate string
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_expr(i.indpred, i.indrelid)
		  FROM pg_index i
		  JOIN pg_class c ON c.oid = i.indexrelid
		 WHERE c.oid = 'active_stream_credential_idx'::regclass`).Scan(&predicate); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(predicate, "draining") {
		t.Fatalf("0019 credential index predicate=%q, want draining", predicate)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	planRows, err := tx.Query(ctx, `
		EXPLAIN (ANALYZE, COSTS OFF)
		SELECT id
		  FROM active_stream
		 WHERE credential_id = $1
		   AND state IN ('starting', 'running', 'draining')`, credentialID)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for planRows.Next() {
		var line string
		if err := planRows.Scan(&line); err != nil {
			planRows.Close()
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatal(err)
	}
	planRows.Close()
	if !strings.Contains(plan.String(), "active_stream_credential_idx") {
		t.Fatalf("draining lookup did not use 0019 partial index:\n%s", plan.String())
	}
	var gotStreamID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT id
		  FROM active_stream
		 WHERE credential_id = $1
		   AND state IN ('starting', 'running', 'draining')`, credentialID).
		Scan(&gotStreamID); err != nil {
		t.Fatal(err)
	}
	if gotStreamID != streamID {
		t.Fatalf("indexed draining stream=%s, want %s", gotStreamID, streamID)
	}
}

func applyDVRAdmissionMigration(
	ctx context.Context,
	pool *pgxpool.Pool,
	migration store.Migration,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
