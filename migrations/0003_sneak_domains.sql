-- Migration: 0003_sneak_domains.sql
-- Description: Creates the sneak_domains table for domain whitelist / CORS authorization per site.

CREATE TABLE IF NOT EXISTS sneak_domains (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    domain TEXT NOT NULL, -- e.g. 'localhost', 'realtorjohn.com', '*.realtorjohn.com'
    verified INTEGER NOT NULL DEFAULT 1, -- 1 = verified, 0 = pending verification
    status TEXT NOT NULL DEFAULT 'active', -- 'active', 'disabled'
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
