package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const capacityDomainMaxLength = 63

var (
	providerCapacityDomainPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

	// ErrInvalidProviderCapacityDomain identifies an operator-supplied domain
	// that is not the stable lowercase slug used by admission locks.
	ErrInvalidProviderCapacityDomain = errors.New("invalid provider capacity domain")
	// ErrProviderCapacityDomainBusy prevents quota-boundary edits while an
	// affected upstream still owns capacity. The operator can retry once every
	// starting/running/draining row in both boundaries is gone.
	ErrProviderCapacityDomainBusy = errors.New("provider capacity domain is busy")
)

// ValidateProviderCapacityDomain accepts an empty isolated boundary or a
// lowercase DNS-label-shaped slug. It deliberately does not trim or lowercase:
// silently changing an operator's quota identity could group the wrong account.
func ValidateProviderCapacityDomain(domain string) error {
	if domain == "" {
		return nil
	}
	if len(domain) > capacityDomainMaxLength || !providerCapacityDomainPattern.MatchString(domain) {
		return fmt.Errorf("%w: use 1-%d lowercase letters, digits, or interior hyphens",
			ErrInvalidProviderCapacityDomain, capacityDomainMaxLength)
	}
	return nil
}

func effectiveCapacityDomain(providerID uuid.UUID, domain string) string {
	if domain == "" {
		return "provider:" + providerID.String()
	}
	return "domain:" + domain
}

func lockCapacityDomains(ctx context.Context, tx pgx.Tx, domains ...string) error {
	unique := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		unique[domain] = struct{}{}
	}
	ordered := make([]string, 0, len(unique))
	for domain := range unique {
		ordered = append(ordered, domain)
	}
	sort.Strings(ordered)
	for _, domain := range ordered {
		if _, err := tx.Exec(ctx, `
			SELECT pg_advisory_xact_lock(
				hashtextextended('conductor:capacity-domain:' || $1, 0)
			)`, domain); err != nil {
			return err
		}
	}
	return nil
}

// lockProviderCapacityDomain takes the provider-local boundary first, then the
// aggregate quota boundary. Every live/DVR new-upstream consumer uses this
// order, keeping credential selection local while serializing shared quotas.
func lockProviderCapacityDomain(
	ctx context.Context,
	tx pgx.Tx,
	providerID uuid.UUID,
) (string, string, error) {
	var configured string
	if err := tx.QueryRow(ctx, `
		SELECT capacity_domain
		  FROM provider
		 WHERE id = $1
		 FOR UPDATE`, providerID).Scan(&configured); err != nil {
		return "", "", err
	}
	effective := effectiveCapacityDomain(providerID, configured)
	if err := lockCapacityDomains(ctx, tx, effective); err != nil {
		return "", "", err
	}
	return configured, effective, nil
}

func capacityDomainBusy(
	ctx context.Context,
	tx pgx.Tx,
	providerID uuid.UUID,
	configuredDomain string,
) (bool, error) {
	var busy bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM active_stream a
			  JOIN provider_credential pc ON pc.id = a.credential_id
			  JOIN provider p ON p.id = pc.provider_id
			 WHERE a.state IN ('starting','running','draining')
			   AND (($2 = '' AND p.id = $1)
			        OR ($2 <> '' AND p.capacity_domain = $2))
		)`, providerID, configuredDomain).Scan(&busy); err != nil {
		return false, err
	}
	return busy, nil
}

// UpdateProviderCapacityDomain changes only the quota boundary. Idempotent
// retries are allowed while busy; a real boundary move locks and proves both
// the old and new domains idle before publishing the new membership.
func (db *DB) UpdateProviderCapacityDomain(
	ctx context.Context,
	providerID uuid.UUID,
	domain string,
) (Provider, error) {
	if err := ValidateProviderCapacityDomain(domain); err != nil {
		return Provider{}, err
	}
	var out Provider
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			SELECT id, name, kind::text, base_url, capacity_domain,
			       notes, enabled, created_at
			  FROM provider
			 WHERE id = $1
			 FOR UPDATE`, providerID).Scan(
			&out.ID, &out.Name, &out.Kind, &out.BaseURL, &out.CapacityDomain,
			&out.Notes, &out.Enabled, &out.CreatedAt,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if out.CapacityDomain == domain {
			return nil
		}

		oldEffective := effectiveCapacityDomain(providerID, out.CapacityDomain)
		newEffective := effectiveCapacityDomain(providerID, domain)
		if err := lockCapacityDomains(ctx, tx, oldEffective, newEffective); err != nil {
			return fmt.Errorf("lock affected provider capacity domains: %w", err)
		}
		for _, affected := range []struct {
			effective  string
			configured string
		}{
			{effective: oldEffective, configured: out.CapacityDomain},
			{effective: newEffective, configured: domain},
		} {
			busy, err := capacityDomainBusy(ctx, tx, providerID, affected.configured)
			if err != nil {
				return fmt.Errorf("check affected provider capacity domain: %w", err)
			}
			if busy {
				return fmt.Errorf("%w: %s", ErrProviderCapacityDomainBusy, affected.effective)
			}
		}
		if err := tx.QueryRow(ctx, `
			UPDATE provider
			   SET capacity_domain = $2
			 WHERE id = $1
			 RETURNING capacity_domain`, providerID, domain).Scan(&out.CapacityDomain); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return Provider{}, fmt.Errorf("update provider capacity domain: %w", err)
	}
	return out, nil
}
