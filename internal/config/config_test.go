package config

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestLogosDirDefault(t *testing.T) {
	t.Setenv("CONDUCTOR_LOGOS_DIR", "")
	if got := logosDirDefault("/var/lib/conductor"); got != "/var/lib/conductor/logos" {
		t.Fatalf("default logos dir = %q", got)
	}

	t.Setenv("CONDUCTOR_LOGOS_DIR", "/srv/operator-logos")
	if got := logosDirDefault("/var/lib/conductor"); got != "/srv/operator-logos" {
		t.Fatalf("operator logos dir = %q", got)
	}
}

func TestLoadPPVDisplayTimezone(t *testing.T) {
	t.Setenv("CONDUCTOR_DATA_DIR", t.TempDir())
	t.Setenv("CONDUCTOR_DEVICE_ID", "A1B2C3D4")
	t.Setenv("CONDUCTOR_PPV_DISPLAY_TIMEZONE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PPVDisplayTimezone != "America/Denver" {
		t.Fatalf("default PPV display timezone = %q", cfg.PPVDisplayTimezone)
	}

	t.Setenv("CONDUCTOR_PPV_DISPLAY_TIMEZONE", "America/New_York")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PPVDisplayTimezone != "America/New_York" {
		t.Fatalf("configured PPV display timezone = %q", cfg.PPVDisplayTimezone)
	}

	t.Setenv("CONDUCTOR_PPV_DISPLAY_TIMEZONE", "Mars/Olympus_Mons")
	if _, err := Load(); err == nil ||
		!strings.Contains(err.Error(), "CONDUCTOR_PPV_DISPLAY_TIMEZONE") {
		t.Fatalf("invalid timezone error = %v", err)
	}
}

func TestLoadDVRAdmissionConfig(t *testing.T) {
	t.Setenv("CONDUCTOR_DATA_DIR", t.TempDir())
	t.Setenv("CONDUCTOR_DEVICE_ID", "A1B2C3D4")
	t.Setenv("CONDUCTOR_DVR_LIVE_RESERVE", "")
	t.Setenv("CONDUCTOR_DVR_DEFAULT_PRIORITY", "")
	t.Setenv("CONDUCTOR_DVR_CAPACITY_RETRY_INTERVAL", "")
	t.Setenv("CONDUCTOR_DVR_CAPACITY_RETRY_WINDOW", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DVRLiveReserve != 1 || cfg.DVRDefaultPriority != 100 {
		t.Fatalf("defaults reserve=%d priority=%d, want 1/100",
			cfg.DVRLiveReserve, cfg.DVRDefaultPriority)
	}

	t.Setenv("CONDUCTOR_DVR_LIVE_RESERVE", "0")
	t.Setenv("CONDUCTOR_DVR_DEFAULT_PRIORITY", "25")
	t.Setenv("CONDUCTOR_DVR_CAPACITY_RETRY_INTERVAL", "750ms")
	t.Setenv("CONDUCTOR_DVR_CAPACITY_RETRY_WINDOW", "3m")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DVRLiveReserve != 0 || cfg.DVRDefaultPriority != 25 ||
		cfg.DVRCapacityRetryInterval != 750*time.Millisecond ||
		cfg.DVRCapacityRetryWindow != 3*time.Minute {
		t.Fatalf("configured DVR admission values: %+v", cfg)
	}

	for _, tc := range []struct {
		name, key, value string
	}{
		{"negative reserve", "CONDUCTOR_DVR_LIVE_RESERVE", "-1"},
		{"invalid reserve", "CONDUCTOR_DVR_LIVE_RESERVE", "many"},
		{"zero priority", "CONDUCTOR_DVR_DEFAULT_PRIORITY", "0"},
		{"negative retry", "CONDUCTOR_DVR_CAPACITY_RETRY_WINDOW", "-1s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Reset all knobs so only this case is invalid.
			t.Setenv("CONDUCTOR_DVR_LIVE_RESERVE", "1")
			t.Setenv("CONDUCTOR_DVR_DEFAULT_PRIORITY", "100")
			t.Setenv("CONDUCTOR_DVR_CAPACITY_RETRY_INTERVAL", "5s")
			t.Setenv("CONDUCTOR_DVR_CAPACITY_RETRY_WINDOW", "5m")
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("Load error=%v, want %s validation", err, tc.key)
			}
		})
	}
}

func TestParseUpstreamProxy(t *testing.T) {
	if u, err := parseUpstreamProxy(" "); u != nil || err != nil {
		t.Fatalf("blank = %v, %v; want direct", u, err)
	}
	withCreds := (&url.URL{Scheme: "http", User: url.UserPassword("user", "secret"), Host: "proxy.example:8888"}).String()
	u, err := parseUpstreamProxy(withCreds)
	if err != nil || u.Host != "proxy.example:8888" || u.User.Username() != "user" {
		t.Fatalf("proxy = %v, %v", u, err)
	}
	userinfo := "user:" + "secret" + "@"
	for _, bad := range []string{
		"proxy.example:8888",
		"socks5://proxy.example:1080",
		"http://" + userinfo,
		"http://" + userinfo + "proxy.example",
		"http://" + userinfo + "proxy.example:%zz",
	} {
		if _, err := parseUpstreamProxy(bad); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("%q: err = %v; want an error that does not echo the password", bad, err)
		}
	}
}
