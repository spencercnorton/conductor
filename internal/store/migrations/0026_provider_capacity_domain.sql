-- 0026_provider_capacity_domain.sql
--
-- Some upstream accounts expose one shared concurrency quota through multiple
-- provider rows. Keep source ownership and credential selection provider-local,
-- but let operators explicitly group rows for aggregate admission accounting.
-- Empty preserves the historical isolated-provider behavior.

ALTER TABLE provider
    ADD COLUMN capacity_domain text NOT NULL DEFAULT '',
    ADD CONSTRAINT provider_capacity_domain_slug_ck CHECK (
        capacity_domain = '' OR
        capacity_domain ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'
    );

CREATE INDEX provider_capacity_domain_idx
    ON provider (capacity_domain, id)
    WHERE capacity_domain <> '';
