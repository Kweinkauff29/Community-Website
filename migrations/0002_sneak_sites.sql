-- Migration: 0002_sneak_sites.sql
-- Description: Creates the sneak_sites table for managing individual tenant site instances and scopes.

CREATE TABLE IF NOT EXISTS sneak_sites (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES sneak_accounts(id) ON DELETE CASCADE,
    site_key TEXT UNIQUE NOT NULL,
    site_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active', -- 'active', 'suspended', 'inactive'
    scope_type TEXT NOT NULL DEFAULT 'market', -- 'market', 'agent', 'office'
    scope_value TEXT, -- MLS Agent ID or MLS Office ID when scoped
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
