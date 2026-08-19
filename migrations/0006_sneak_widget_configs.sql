-- Migration: 0006_sneak_widget_configs.sql
-- Description: Creates the sneak_widget_configs table for configuring widgets (search, search_bar, listing_grid, open_houses, featured_listings).

CREATE TABLE IF NOT EXISTS sneak_widget_configs (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    widget_type TEXT NOT NULL, -- 'search', 'search_bar', 'listing_grid', 'open_houses', 'featured_listings'
    enabled INTEGER NOT NULL DEFAULT 1,
    config_json TEXT, -- widget-specific settings (e.g. default zoom, default cities, layout)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
