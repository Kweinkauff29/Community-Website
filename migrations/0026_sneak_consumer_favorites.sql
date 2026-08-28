-- Migration: 0026_sneak_consumer_favorites.sql
-- Description: Server-side saved properties (favorites) for authenticated property buyers/consumers.
-- Constraint: Unique per consumer per site per listing key. Limit enforced application-side (200 items max).

CREATE TABLE IF NOT EXISTS sneak_consumer_favorites (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    listing_key TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, site_id, listing_key)
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_favorites_user_site ON sneak_consumer_favorites(user_id, site_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_favorites_listing ON sneak_consumer_favorites(listing_key);
