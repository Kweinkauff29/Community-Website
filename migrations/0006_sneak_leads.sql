-- Migration: 0006_sneak_leads.sql
-- Description: Creates the sneak_leads table for capturing inbound leads per site.

CREATE TABLE IF NOT EXISTS sneak_leads (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    listing_key TEXT,
    lead_type TEXT NOT NULL DEFAULT 'property_inquiry', -- 'property_inquiry', 'schedule_tour', 'contact_agent', 'general'
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    message TEXT,
    source_url TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
