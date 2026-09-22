package epg

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/ppvparse"
	"github.com/spencercnorton/conductor/internal/store"
)

// The BTN+ parser feeds ppvsync's ordinary EPG identity path, and XMLTV emits
// that stored UTC instant verbatim. Keep an end-to-end regression here so a
// later output-layer timezone rewrite cannot reintroduce the four-hour shift.
func TestEmitProgramme_BTNPlusEasternBoundaryUsesCorrectUTC(t *testing.T) {
	const input = "(US) (BTN+ 001) | Soccer (W): Bellarmine at Indiana_ 21_08_2026_ 20:00 (2026-08-21 19:30:00)"
	parsed, ok := ppvparse.Parse(input)
	if !ok {
		t.Fatal("BTN+ fixture did not parse")
	}

	channelID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	program, ok := ppvparse.EPGProgramIdentity(channelID, parsed, "xtream:1954461")
	if !ok {
		t.Fatal("BTN+ parse did not produce an EPG identity")
	}
	row := store.ProgramOutputRow{
		ChannelID:     channelID,
		ChannelEPGID:  "btn-plus-001",
		ChannelName:   "BTN+ 001",
		ChannelNumber: 385,
		StartAt:       program.StartAt,
		EndAt:         program.EndAt,
		Title:         program.Title,
		Description:   program.Description,
		Category:      program.Category,
		IsLive:        program.IsLive,
	}

	w := httptest.NewRecorder()
	emitProgramme(w, row, "")
	xml := w.Body.String()
	if !strings.Contains(xml, `start="20260821233000 +0000" stop="20260822023000 +0000"`) {
		t.Fatalf("BTN+ XMLTV window did not preserve corrected UTC times:\n%s", xml)
	}
	if strings.Contains(xml, `start="20260821193000 +0000"`) {
		t.Fatalf("BTN+ XMLTV regressed to treating the Eastern suffix as UTC:\n%s", xml)
	}
}
