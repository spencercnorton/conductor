package testdb

import (
	"net/url"
	"testing"
)

func TestIsolatedDSNPreservesConnectionSettings(t *testing.T) {
	base := "postgres://tester:fixture@example.invalid:5432/conductor_test?sslmode=require&connect_timeout=3"
	got, err := isolatedDSN(base, "conductor_test_unique")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := url.Parse(base)
	after, _ := url.Parse(got)
	if after.Path != "/conductor_test_unique" || after.Host != before.Host || after.User.String() != before.User.String() || after.RawQuery != before.RawQuery {
		t.Fatal("isolation changed a connection setting other than database")
	}
}

func TestIsolatedDSNRefusesProductionOrNonPostgresURLs(t *testing.T) {
	for _, base := range []string{"postgres://tester@example.invalid/conductor", "https://example.invalid/conductor_test", "postgres:///conductor_test", "not a URL"} {
		if _, err := isolatedDSN(base, "conductor_test_unique"); err == nil {
			t.Fatal("unsafe integration database URL accepted")
		}
	}
}
