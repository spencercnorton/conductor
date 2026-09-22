package epg_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/spencercnorton/conductor/internal/epg"
	"github.com/spencercnorton/conductor/internal/store"
)

func TestIntegrationScheduledLiveAndNewRoundtrip(t *testing.T) {
	skipIfNoIntegration(t)
	ctx := context.Background()
	dsn := os.Getenv("CONDUCTOR_TEST_DSN")
	if dsn == "" {
		t.Skip("requires an explicitly owned isolated CONDUCTOR_TEST_DSN")
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := "op477_live_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema+",public")
	u.RawQuery = query.Encode()
	db, err := store.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = db.CreateChannel(ctx, store.Channel{Number: 9477, Name: "Broadcast fixture", EpgChannelID: "fixture.live", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	body := `<tv><channel id="fixture.live"><display-name>Fixture</display-name></channel>`
	for i, repeat := range []bool{false, true} {
		marker := ""
		if repeat {
			marker = "<previously-shown/>"
		}
		body += fmt.Sprintf(`<programme channel="fixture.live" start="%s" stop="%s"><title>Recognized Series</title><sub-title>The Final</sub-title><episode-num system="xmltv_ns">2.6.</episode-num><live/><new/>%s</programme>`, fmtXMLTVTime(start.Add(time.Duration(i)*time.Hour)), fmtXMLTVTime(start.Add(time.Duration(i+1)*time.Hour)), marker)
	}
	body += fmt.Sprintf(`<programme channel="fixture.live" start="%s" stop="%s"><title>Recognized Series</title><sub-title>Another Episode</sub-title><episode-num system="xmltv_ns">2.7.</episode-num><new/></programme>`, fmtXMLTVTime(start.Add(2*time.Hour)), fmtXMLTVTime(start.Add(3*time.Hour)))
	body += fmt.Sprintf(`<programme channel="fixture.live" start="%s" stop="%s"><title>Recognized Series</title><sub-title>Past Episode</sub-title><episode-num system="xmltv_ns">2.8.</episode-num><live/></programme>`, fmtXMLTVTime(start.Add(-25*time.Hour)), fmtXMLTVTime(start.Add(-24*time.Hour)))
	body += fmt.Sprintf(`<programme channel="fixture.live" start="%s" stop="%s"><title>Recognized Series</title><sub-title>Past Episode</sub-title><episode-num system="xmltv_ns">2.8.</episode-num><live/></programme>`, fmtXMLTVTime(start.Add(3*time.Hour)), fmtXMLTVTime(start.Add(4*time.Hour)))
	body += `</tv>`
	srv := servesXMLTV(t, body)
	defer srv.Close()
	src, err := db.CreateEPGSource(ctx, store.EPGSource{Name: "Live/New fixture", URL: srv.URL, Enabled: true, Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, _, _, err := epg.Ingest(ctx, db, src, nil); err != nil {
		t.Fatal(err)
	}
	rows, err := db.ListProgramsForOutput(ctx, start.Add(-time.Minute), start.Add(5*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 || rows[0].PreviouslyShownExplicit || !rows[1].PreviouslyShownExplicit || !rows[2].IsNew ||
		rows[0].HasEarlierAiring || rows[1].HasEarlierAiring || !rows[3].HasEarlierAiring {
		t.Fatalf("explicit-repeat provenance lost in output read model: %+v", rows)
	}
	// Future repeat evidence is conservative across the episode's history.
	// Keep the positive first-run control independent so the repeat cannot
	// retroactively remove its first-run proof from this fixture.
	if rows[1].IsNew {
		t.Fatal("explicit source repeat incorrectly remained New")
	}
	out := httptest.NewRecorder()
	epg.NewOutput(db, "").ServeHTTP(out, httptest.NewRequest("GET", "/xmltv.xml", nil))
	var guide struct {
		Programmes []struct {
			Title    string    `xml:"title"`
			SubTitle string    `xml:"sub-title"`
			Live     *struct{} `xml:"live"`
			Repeat   *struct{} `xml:"previously-shown"`
			New      *struct{} `xml:"new"`
		} `xml:"programme"`
	}
	if err := xml.Unmarshal(out.Body.Bytes(), &guide); err != nil {
		t.Fatal(err)
	}
	if len(guide.Programmes) != 4 {
		t.Fatalf("unexpected guide: %s", out.Body.String())
	}
	first, second := guide.Programmes[0], guide.Programmes[1]
	if first.Title != "Recognized Series" || first.SubTitle != "The Final" || first.Live == nil ||
		second.Title != "Recognized Series" || second.SubTitle != "The Final" || second.Live != nil || second.Repeat == nil {
		t.Fatalf("future guide import misrepresents live/repeat or changes series identity: %s", out.Body.String())
	}
	if guide.Programmes[2].New == nil || guide.Programmes[2].Repeat != nil || guide.Programmes[2].Live != nil {
		t.Fatalf("recognized first-run episode lost independent New state: %s", out.Body.String())
	}
	if guide.Programmes[3].Live != nil || guide.Programmes[3].SubTitle != "Past Episode" {
		t.Fatalf("proven actual prior airing retained a contradictory live flag: %s", out.Body.String())
	}
	// A future row can start without another write to archive it. The current
	// canonical schedule still proves its earlier airing. Removing that row
	// later archives it, so neither lifecycle loses the positive repeat signal.
	if _, err := db.Pool.Exec(ctx, "DELETE FROM episode_airing_history"); err != nil {
		t.Fatal(err)
	}
	rows, err = db.ListProgramsForOutput(ctx, start.Add(-time.Minute), start.Add(5*time.Hour))
	if err != nil || len(rows) != 4 || !rows[3].HasEarlierAiring {
		t.Fatalf("started canonical evidence lost: rows=%+v err=%v", rows, err)
	}
	if _, err := db.Pool.Exec(ctx, "DELETE FROM epg_program WHERE end_at < now()"); err != nil {
		t.Fatal(err)
	}
	rows, err = db.ListProgramsForOutput(ctx, start.Add(-time.Minute), start.Add(5*time.Hour))
	if err != nil || len(rows) != 4 || !rows[3].HasEarlierAiring {
		t.Fatalf("archived earlier-airing evidence lost: rows=%+v err=%v", rows, err)
	}
}
