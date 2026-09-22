package logo

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultChannelLogosAreValidAndPinned(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		format string
		sha256 string
	}{
		{
			name:   "epl-ppv-2c7b878f.png",
			format: "png",
			sha256: "2c7b878f8c1ceabf5395e40894b2a5c6fa9c1c3e4d2b4891deb200afdfa07065",
		},
		{
			name:   "ppv-event-6358638d.png",
			format: "png",
			sha256: "6358638dc1f90eb909a1b7b3f984e3d9370ec8ec5de0510e03ac2cc1c57f0ada",
		},
		{
			name:   "big-ten-plus-925e1063.jpg",
			format: "jpeg",
			sha256: "925e10639822bdd67c0b0367d2645a115a791a5f37a6d83a649c0e6e1066e49a",
		},
		{
			name:   "sky-sports-premier-league-75c224d8.png",
			format: "png",
			sha256: "75c224d8b6ced985756e6c3a59751cbb7772e5d6efd106408a986f2046b9d54a",
		},
		{
			name:   "mlb-league-7ad0e29a.png",
			format: "png",
			sha256: "7ad0e29a8ce809174698bab407cbb4d0a8624d0609d4af5abb48a02b4156ddf2",
		},
		{
			name:   "ncaa-707c5fa7.png",
			format: "png",
			sha256: "707c5fa7215e9abcd38a1760664eaf16b3095bb9ec4651b0e1d3f4bae1218aef",
		},
		{
			name:   "soccer-ball-b4000437.png",
			format: "png",
			sha256: "b4000437222d05542e717740693d6bd848ca0b4cae0498baa4320ef5eb06de1d",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join("..", "..", "assets", "default-logos", test.name)
			data, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				t.Skip("default logo assets are supplied by the operator; none in this tree")
			}
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			if got := hex.EncodeToString(sum[:]); got != test.sha256 {
				t.Fatalf("sha256 = %s, want %s", got, test.sha256)
			}
			config, format, err := image.DecodeConfig(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if format != test.format {
				t.Fatalf("format = %q, want %q", format, test.format)
			}
			if config.Width != 96 || config.Height != 96 {
				t.Fatalf("dimensions = %dx%d, want 96x96", config.Width, config.Height)
			}
		})
	}
}
