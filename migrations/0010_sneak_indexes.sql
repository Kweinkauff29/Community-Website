-- Migration: 0010_sneak_indexes.sql
-- Description: Adds indexes to optimize multi-tenant lookups and search filtering for SNEAK tables.

-- Tenant & Site indexes
CREATE INDEX IF NOT EXISTS idx_sneak_sites_key ON sneak_sites(site_key);
CREATE INDEX IF NOT EXISTS idx_sneak_sites_account ON sneak_sites(account_id);
CREATE INDEX IF NOT EXISTS idx_sneak_domains_lookup ON sneak_domains(site_id, domain);
CREATE INDEX IF NOT EXISTS idx_sneak_branding_site ON sneak_branding(site_id);
CREATE INDEX IF NOT EXISTS idx_sneak_widget_site ON sneak_widget_configs(site_id, widget_type);
CREATE INDEX IF NOT EXISTS idx_sneak_leads_site ON sneak_leads(site_id, created_at);
CREATE INDEX IF NOT EXISTS idx_sneak_usage_site_date ON sneak_usage(site_id, usage_date);
CREATE INDEX IF NOT EXISTS idx_sneak_oh_date ON sneak_open_houses(OpenHouseDate);
CREATE INDEX IF NOT EXISTS idx_sneak_oh_listing ON sneak_open_houses(ListingKey);
