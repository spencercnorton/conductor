package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestValidateProviderCapacityDomain(t *testing.T) {
	valid := []string{"", "a", "iboost", "iboost-shared-2", strings.Repeat("a", 63)}
	for _, domain := range valid {
		if err := ValidateProviderCapacityDomain(domain); err != nil {
			t.Errorf("valid domain %q rejected: %v", domain, err)
		}
	}
	invalid := []string{
		"Iboost", " has-space", "has_space", "-leading", "trailing-",
		"two--okay?", strings.Repeat("a", 64),
	}
	for _, domain := range invalid {
		err := ValidateProviderCapacityDomain(domain)
		if !errors.Is(err, ErrInvalidProviderCapacityDomain) {
			t.Errorf("invalid domain %q err=%v, want typed invalid-domain error", domain, err)
		}
	}
}

func TestEffectiveCapacityDomainKeepsEmptyProvidersIsolated(t *testing.T) {
	providerA := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	providerB := uuid.MustParse("00000000-0000-0000-0000-0000000000b1")
	if gotA, gotB := effectiveCapacityDomain(providerA, ""), effectiveCapacityDomain(providerB, ""); gotA == gotB {
		t.Fatalf("empty domains collapsed providers into %q", gotA)
	}
	if gotA, gotB := effectiveCapacityDomain(providerA, "shared"), effectiveCapacityDomain(providerB, "shared"); gotA != gotB {
		t.Fatalf("named domain keys differ: %q != %q", gotA, gotB)
	}
}
