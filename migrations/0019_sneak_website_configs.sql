-- Migration: 0019_sneak_website_configs.sql
-- Multi-Tenant Low-Cost IDX Website Template System Configuration Table

CREATE TABLE IF NOT EXISTS sneak_website_configs (
    site_id TEXT PRIMARY KEY REFERENCES sneak_sites(id) ON DELETE CASCADE,
    enabled INTEGER NOT NULL DEFAULT 0,
    template_key TEXT NOT NULL DEFAULT 'essential' CHECK(template_key IN ('essential', 'coastal', 'brokerage')),
    site_title TEXT,
    tagline TEXT,
    hero_heading TEXT,
    hero_subheading TEXT,
    hero_image_url TEXT,
    about_heading TEXT,
    about_body TEXT,
    about_image_url TEXT,
    featured_areas_json TEXT,
    navigation_json TEXT,
    social_links_json TEXT,
    seo_title TEXT,
    seo_description TEXT,
    footer_text TEXT,
    contact_cta_text TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_website_configs_enabled ON sneak_website_configs(enabled);
