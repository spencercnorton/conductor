package dvr

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// TestItemFromHitSizePlausible guards the placeholder-size calculation
// against the *1024 inflation bug, where a 4-hour program was advertised
// at 2.76 TB and *arr size limits rejected the release outright.
func TestItemFromHitSizePlausible(t *testing.T) {
	tn := New(nil, "http://test.local:8409", "test-key", "Test")
	start := time.Date(2026, 6, 1, 20, 0, 0, 0, time.UTC)
	hit := store.EPGSearchHit{
		ProgramID: uuid.New(),
		ChannelID: uuid.New(),
		StartAt:   start,
		EndAt:     start.Add(4 * time.Hour),
		Title:     "Some Movie",
		IsMovie:   true,
	}
	item := tn.itemFromHit(hit)

	// 4 h at 1.5 Mb/s = 4*3600 * 1_500_000/8 bytes ≈ 2.7 GB.
	const want int64 = 4 * 3600 * 1_500_000 / 8
	got, err := strconv.ParseInt(item.Enclosure.Length, 10, 64)
	if err != nil {
		t.Fatalf("enclosure length %q is not an integer: %v", item.Enclosure.Length, err)
	}
	if got != want {
		t.Errorf("item size = %d bytes, want %d (~2.7 GB); a 1024x value means the *1024 bug is back", got, want)
	}
	if got > 50<<30 {
		t.Errorf("item size %d bytes exceeds 50 GB — implausible for a recording", got)
	}

	// The size attr must agree with the enclosure length.
	var sizeAttr string
	for _, a := range item.Attrs {
		if a.Name == "size" {
			sizeAttr = a.Value
		}
	}
	if sizeAttr != item.Enclosure.Length {
		t.Errorf("size attr %q != enclosure length %q", sizeAttr, item.Enclosure.Length)
	}
}
