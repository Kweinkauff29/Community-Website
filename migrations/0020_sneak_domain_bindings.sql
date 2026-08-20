-- Migration: 0020_sneak_domain_bindings.sql
-- Cloudflare for SaaS Custom Hostname & SSL Domain Binding Infrastructure

CREATE TABLE IF NOT EXISTS sneak_domain_bindings (
    id TEXT PRIMARY KEY,
    domain_id TEXT NOT NULL REFERENCES sneak_domains(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    provider TEXT NOT NULL DEFAULT 'cloudflare_saas',
    hostname TEXT NOT NULL UNIQUE,
    provider_hostname_id TEXT,
    status TEXT NOT NULL DEFAULT 'requested' CHECK(status IN ('requested', 'pending_dns', 'pending_validation', 'pending_ssl', 'active', 'error', 'removing', 'removed')),
    ssl_status TEXT CHECK(ssl_status IN ('initializing', 'pending_validation', 'pending_deployment', 'active', 'cancelled', 'error')),
    validation_method TEXT DEFAULT 'http',
    cname_target TEXT,
    ownership_txt_name TEXT,
    ownership_txt_value TEXT,
    last_checked_at DATETIME,
    activated_at DATETIME,
    removed_at DATETIME,
    error_code TEXT,
    error_summary TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_domain_bindings_site ON sneak_domain_bindings(site_id);
CREATE INDEX IF NOT EXISTS idx_sneak_domain_bindings_hostname ON sneak_domain_bindings(hostname);
CREATE INDEX IF NOT EXISTS idx_sneak_domain_bindings_status ON sneak_domain_bindings(status);
