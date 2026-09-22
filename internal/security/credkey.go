// Package security houses the cryptographic primitives Conductor uses:
// AES-GCM for provider-credential password encryption, HMAC for the ops bot
// webhook signature (lives in internal/alerts).
//
// Key management: the AES-GCM key comes from CONDUCTOR_CRED_KEY (32 bytes
// hex-encoded). In production the env is injected by a secret manager at container
// start. The cleartext password lives in memory only at the moment of URL
// resolution — never logged, never returned by the admin API.
package security

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/spencercnorton/conductor/internal/store"
)

// CredKey is an AES-GCM encryption key for provider credentials at rest.
type CredKey struct {
	gcm cipher.AEAD
}

// LoadCredKey parses CONDUCTOR_CRED_KEY (32 bytes hex). If empty it returns
// nil + nil so callers can no-op (useful in dev when no providers configured).
func LoadCredKey(hexKey string) (*CredKey, error) {
	if hexKey == "" {
		return nil, nil
	}
	keyBytes, err := hex.DecodeString(strings.TrimSpace(hexKey))
	if err != nil {
		return nil, fmt.Errorf("CONDUCTOR_CRED_KEY hex decode: %w", err)
	}
	if len(keyBytes) != 32 {
		return nil, fmt.Errorf("CONDUCTOR_CRED_KEY must be 32 bytes (got %d)", len(keyBytes))
	}
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &CredKey{gcm: gcm}, nil
}

// Encrypt encrypts plaintext to ciphertext suitable for storage in
// provider_credential.password_enc.
func (k *CredKey) Encrypt(plaintext []byte) ([]byte, error) {
	if k == nil {
		return nil, errors.New("cred key not configured")
	}
	nonce := make([]byte, k.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	// Output layout: nonce || ciphertext_with_tag.
	return k.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt reverses Encrypt.
func (k *CredKey) Decrypt(ciphertext []byte) ([]byte, error) {
	if k == nil {
		return nil, errors.New("cred key not configured")
	}
	ns := k.gcm.NonceSize()
	if len(ciphertext) < ns {
		return nil, errors.New("ciphertext too short")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	return k.gcm.Open(nil, nonce, ct, nil)
}

// CredentialResolver implements store.CredentialResolver.
//
// The URL template can contain ${USER} and ${PASS} placeholders that are
// substituted with the leased credential. Both substitutions are URL-encoded
// to handle passwords containing &, =, etc.
//
// If the template has no placeholders the URL is returned as-is — useful for
// providers that bake the credential into a single permanent URL.
type CredentialResolver struct {
	Key *CredKey
}

func NewResolver(key *CredKey) *CredentialResolver {
	return &CredentialResolver{Key: key}
}

func (r *CredentialResolver) Resolve(ctx context.Context, c store.ProviderCredential, urlTemplate string) (string, error) {
	if urlTemplate == "" {
		return "", errors.New("upstream URL template is empty")
	}
	hasUser := strings.Contains(urlTemplate, "${USER}")
	hasPass := strings.Contains(urlTemplate, "${PASS}")
	if !hasUser && !hasPass {
		return urlTemplate, nil
	}

	out := urlTemplate
	if hasUser {
		out = strings.ReplaceAll(out, "${USER}", url.QueryEscape(c.Username))
	}
	if hasPass {
		if r.Key == nil {
			return "", errors.New("upstream URL needs ${PASS} but cred key is not configured")
		}
		pw, err := r.Key.Decrypt(c.PasswordEnc)
		if err != nil {
			return "", fmt.Errorf("decrypt credential: %w", err)
		}
		out = strings.ReplaceAll(out, "${PASS}", url.QueryEscape(string(pw)))
	}
	return out, nil
}
