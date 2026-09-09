-- Migration: 0001_initial_schema.sql
-- Database: ccor-global-buyers (D1)
-- Description: Creates gb_leads and gb_profile_daily tables with privacy-preserving schema and indexes

-- 1. Contact / Lead Profiles Table
CREATE TABLE IF NOT EXISTS gb_leads (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT,
    email TEXT NOT NULL,
    market TEXT,
    country_code TEXT,
    intent TEXT,
    marketing_opt_in INTEGER NOT NULL DEFAULT 0,
    consent INTEGER NOT NULL DEFAULT 0,
    consent_version TEXT NOT NULL,
    source_path TEXT,
    referrer_host TEXT,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    utm_content TEXT
);

-- Indexes for gb_leads
CREATE INDEX IF NOT EXISTS idx_gb_leads_created_at ON gb_leads(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_gb_leads_market ON gb_leads(market);
CREATE INDEX IF NOT EXISTS idx_gb_leads_country_code ON gb_leads(country_code);
CREATE INDEX IF NOT EXISTS idx_gb_leads_intent ON gb_leads(intent);
CREATE INDEX IF NOT EXISTS idx_gb_leads_email ON gb_leads(email);

-- 2. Daily Profile Selection Aggregates Table (No PII, no IP, strictly aggregate event counts)
CREATE TABLE IF NOT EXISTS gb_profile_daily (
    day TEXT NOT NULL,
    market TEXT NOT NULL,
    country_code TEXT,
    intent TEXT NOT NULL,
    selection_count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (day, market, intent)
);

-- Indexes for gb_profile_daily
CREATE INDEX IF NOT EXISTS idx_gb_profile_daily_day ON gb_profile_daily(day DESC);
CREATE INDEX IF NOT EXISTS idx_gb_profile_daily_market ON gb_profile_daily(market);
CREATE INDEX IF NOT EXISTS idx_gb_profile_daily_intent ON gb_profile_daily(intent);
