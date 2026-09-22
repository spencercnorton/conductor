package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/spencercnorton/conductor/internal/store"
)

const (
	legacyNVENCDescriptionFragment  = "NVDEC for input decode (zero-copy on the GPU)"
	currentNVENCDescriptionFragment = "NVDEC remains enabled for input decode"
)

func TestNVENCDescriptionChangeUsesForwardMigration(t *testing.T) {
	migrations, err := store.LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var seed, update string
	for _, migration := range migrations {
		switch migration.Name {
		case "0006_transcode.sql":
			seed = migration.SQL
		case "0022_update_nvenc_profile_descriptions.sql":
			update = migration.SQL
		}
	}
	if seed == "" || update == "" {
		t.Fatalf("required migrations missing: seed=%v update=%v", seed != "", update != "")
	}
	if !strings.Contains(seed, legacyNVENCDescriptionFragment) ||
		strings.Contains(seed, currentNVENCDescriptionFragment) {
		t.Fatal("historical migration 0006 was rewritten instead of using a forward migration")
	}
	for _, fragment := range []string{
		legacyNVENCDescriptionFragment,
		currentNVENCDescriptionFragment,
		"name = 'nvenc-1080p'",
		"name = 'nvenc-720p'",
		"name = 'hdr-to-sdr-nvenc'",
		"AND is_builtin",
		"AND description =",
	} {
		if !strings.Contains(update, fragment) {
			t.Fatalf("forward migration missing guard/content %q", fragment)
		}
	}
}

func TestIntegrationNVENCDescriptionMigrationPreservesCustomText(t *testing.T) {
	db := freshDB(t)
	ctx := context.Background()

	names := []string{"nvenc-1080p", "nvenc-720p", "hdr-to-sdr-nvenc"}
	for _, name := range names {
		profile, err := db.GetTranscodeProfileByName(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(profile.Description, currentNVENCDescriptionFragment) {
			t.Fatalf("fresh migration chain left %s description stale: %q", name, profile.Description)
		}
		if _, err := db.Pool.Exec(ctx,
			`UPDATE transcode_profile SET description = $2 WHERE id = $1`,
			profile.ID, "operator-owned "+name+" notes"); err != nil {
			t.Fatal(err)
		}
	}

	migrations, err := store.LoadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if migration.Name == "0022_update_nvenc_profile_descriptions.sql" {
			if _, err := db.Pool.Exec(ctx, migration.SQL); err != nil {
				t.Fatalf("replay guarded description migration: %v", err)
			}
			for _, name := range names {
				profile, err := db.GetTranscodeProfileByName(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				want := "operator-owned " + name + " notes"
				if profile.Description != want {
					t.Fatalf("guarded migration overwrote %s description: %q", name, profile.Description)
				}
			}
			return
		}
	}
	t.Fatal("forward description migration not found")
}
