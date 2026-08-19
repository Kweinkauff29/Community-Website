-- Migration: 0010_sneak_indexes.sql
-- Description: Adds indexes to optimize multi-tenant lookups and search filtering.

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

-- Listings table search indexes (non-destructive)
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(StandardStatus);
CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(ListPrice);
CREATE INDEX IF NOT EXISTS idx_listings_city ON listings(City);
CREATE INDEX IF NOT EXISTS idx_listings_prop_type ON listings(PropertyType, PropertySubType);
CREATE INDEX IF NOT EXISTS idx_listings_beds_baths ON listings(BedroomsTotal, BathroomsTotalInteger);
CREATE INDEX IF NOT EXISTS idx_listings_agent ON listings(ListAgentMlsId);
CREATE INDEX IF NOT EXISTS idx_listings_contract_date ON listings(ListingContractDate);
