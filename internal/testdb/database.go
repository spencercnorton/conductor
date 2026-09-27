// Package testdb creates independent disposable databases for integration tests.
package testdb

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func isolatedDSN(base, name string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" {
		return "", fmt.Errorf("integration tests require a PostgreSQL URL")
	}
	if !strings.HasSuffix(strings.TrimPrefix(u.Path, "/"), "_test") {
		return "", fmt.Errorf("integration database name must end in _test")
	}
	u.Path = "/" + name
	return u.String(), nil
}

// New returns a fresh database URL. The supplied URL must name a disposable
// *_test database and its role must have CREATEDB. Cleanup drops only the
// randomly named database created by this call, after later test cleanups.
// It never truncates the supplied database or logs its credentials.
func New(t *testing.T, base string) string {
	t.Helper()
	name := "conductor_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	dsn, err := isolatedDSN(base, name)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal("cannot connect to disposable integration database")
	}
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		_ = admin.Close(context.Background())
		t.Fatal("cannot create isolated integration database: use a test role with CREATEDB")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP DATABASE "+quoted+" WITH (FORCE)"); err != nil {
			t.Error("cannot remove this test's isolated database")
		}
	})
	return dsn
}
