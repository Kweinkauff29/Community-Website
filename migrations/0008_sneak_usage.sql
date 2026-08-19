-- Migration: 0008_sneak_usage.sql
-- Description: Creates the sneak_usage table for tracking daily usage metrics per site.

CREATE TABLE IF NOT EXISTS sneak_usage (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    usage_date TEXT NOT NULL DEFAULT (DATE('now')),
    searches INTEGER NOT NULL DEFAULT 0,
    listing_views INTEGER NOT NULL DEFAULT 0,
    leads INTEGER NOT NULL DEFAULT 0,
    UNIQUE(site_id, usage_date)
);
