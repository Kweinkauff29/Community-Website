-- Migration: 0005_sneak_branding.sql
-- Description: Creates the sneak_branding table for tenant branding and UI customization.

CREATE TABLE IF NOT EXISTS sneak_branding (
    site_id TEXT PRIMARY KEY REFERENCES sneak_sites(id) ON DELETE CASCADE,
    display_name TEXT,
    brokerage TEXT,
    logo_url TEXT,
    agent_photo_url TEXT,
    primary_color TEXT DEFAULT '#1a2a3a',
    secondary_color TEXT DEFAULT '#2596be',
    phone TEXT,
    email TEXT,
    website_url TEXT,
    config_json TEXT -- JSON for custom header links, disclosures, footer info, social links
);
