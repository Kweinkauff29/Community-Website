-- Migration: 0002_market_and_fare_data.sql
-- Database: ccor-global-buyers (D1)
-- Description: Adds lead_source to gb_leads, creates gb_market_daily for anonymous country selections, and creates gb_fare_samples for flight fare intelligence

-- 1. Add lead_source to gb_leads
ALTER TABLE gb_leads ADD COLUMN lead_source TEXT DEFAULT 'brief_manual';

-- 2. Daily Country / Market Selection Aggregates Table (No PII, strictly country click counts)
CREATE TABLE IF NOT EXISTS gb_market_daily (
    day TEXT NOT NULL,
    market TEXT NOT NULL,
    country_code TEXT,
    selection_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, market)
);

CREATE INDEX IF NOT EXISTS idx_gb_market_daily_day ON gb_market_daily(day DESC);
CREATE INDEX IF NOT EXISTS idx_gb_market_daily_market ON gb_market_daily(market);

-- 3. Flight Fare Samples & Benchmarks Table
CREATE TABLE IF NOT EXISTS gb_fare_samples (
    id TEXT PRIMARY KEY,
    sampled_at TEXT NOT NULL,
    market TEXT,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL DEFAULT 'RSW',
    departure_date TEXT NOT NULL,
    return_date TEXT NOT NULL,
    fare_usd REAL NOT NULL,
    local_currency TEXT,
    fare_local REAL,
    carrier TEXT,
    stops INTEGER,
    duration_minutes INTEGER
);

CREATE INDEX IF NOT EXISTS idx_gb_fare_samples_sampled_at ON gb_fare_samples(sampled_at DESC);
CREATE INDEX IF NOT EXISTS idx_gb_fare_samples_origin ON gb_fare_samples(origin);
CREATE INDEX IF NOT EXISTS idx_gb_fare_samples_market ON gb_fare_samples(market);
