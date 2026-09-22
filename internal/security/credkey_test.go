package security_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/spencercnorton/conductor/internal/security"
	"github.com/spencercnorton/conductor/internal/store"
)

func newKey(t *testing.T) *security.CredKey {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	k, err := security.LoadCredKey(hex.EncodeToString(raw))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestEncryptDecryptRoundtrip(t *testing.T) {
	k := newKey(t)
	pt := []byte("hunter2-with-special-chars-!@#$%^&*()")
	ct, err := k.Encrypt(pt)
	if err != nil {
		t.Fatal(err)
	}
	got, err := k.Decrypt(ct)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(pt) {
		t.Errorf("roundtrip mismatch: got %q want %q", got, pt)
	}
}

func TestEncryptIsNonDeterministic(t *testing.T) {
	k := newKey(t)
	pt := []byte("same-plaintext")
	a, _ := k.Encrypt(pt)
	b, _ := k.Encrypt(pt)
	if string(a) == string(b) {
		t.Error("expected different ciphertexts for same plaintext (nonce should be random)")
	}
}

func TestLoadCredKeyValidation(t *testing.T) {
	cases := []struct {
		name    string
		hexKey  string
		wantNil bool
		wantErr bool
	}{
		{"empty returns nil", "", true, false},
		{"valid 32 bytes", strings.Repeat("ab", 32), false, false},
		{"invalid hex returns err+nil", "zzzz", true, true},
		{"wrong length returns err+nil", strings.Repeat("ab", 16), true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k, err := security.LoadCredKey(tc.hexKey)
			if (err != nil) != tc.wantErr {
				t.Errorf("err: got %v wantErr=%v", err, tc.wantErr)
			}
			if (k == nil) != tc.wantNil {
				t.Errorf("nil: got nil=%v want nil=%v", k == nil, tc.wantNil)
			}
		})
	}
}

func TestResolverPassthrough(t *testing.T) {
	r := security.NewResolver(nil)
	got, err := r.Resolve(context.Background(),
		store.ProviderCredential{Username: "u"},
		"http://example.com/feed.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://example.com/feed.m3u8" {
		t.Errorf("passthrough should be unchanged: got %q", got)
	}
}

func TestResolverSubstitutesUserAndPassEncoded(t *testing.T) {
	k := newKey(t)
	r := security.NewResolver(k)
	enc, err := k.Encrypt([]byte("p@ss w/special&chars="))
	if err != nil {
		t.Fatal(err)
	}
	c := store.ProviderCredential{
		Username:    "user with spaces",
		PasswordEnc: enc,
	}
	got, err := r.Resolve(context.Background(), c,
		"http://provider.com/get.php?username=${USER}&password=${PASS}&type=m3u")
	if err != nil {
		t.Fatal(err)
	}
	want := "http://provider.com/get.php?username=user+with+spaces&password=p%40ss+w%2Fspecial%26chars%3D&type=m3u"
	if got != want {
		t.Errorf("substitution: got %q want %q", got, want)
	}
}

func TestResolverErrorsWhenPassPlaceholderButNoKey(t *testing.T) {
	r := security.NewResolver(nil)
	_, err := r.Resolve(context.Background(),
		store.ProviderCredential{Username: "u"},
		"http://provider/?u=${USER}&p=${PASS}")
	if err == nil {
		t.Error("expected error when ${PASS} present and key not configured")
	}
}
