-- Migration: 0027_sneak_consumer_saved_searches.sql
-- Description: Saved searches and criteria state for authenticated property buyers/consumers.
-- Limit: Max 25 saved searches per consumer per site.
-- Isolation: Strictly site-scoped (tenant isolated) and consumer-owned.

CREATE TABLE IF NOT EXISTS sneak_consumer_saved_searches (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    state_version INTEGER NOT NULL DEFAULT 1,
    state_json TEXT NOT NULL,
    state_hash TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_saved_searches_user_site ON sneak_consumer_saved_searches(user_id, site_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_saved_searches_hash ON sneak_consumer_saved_searches(user_id, site_id, state_hash);
