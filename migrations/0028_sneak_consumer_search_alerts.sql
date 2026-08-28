-- Migration: 0028_sneak_consumer_search_alerts.sql
-- Description: Saved Search Email Alerts & Delivery Infrastructure (Phase 7.3C2A)
-- Schema: Alert preferences, match ledger, delivery tracking, and cron run locks.

-- 1. Alert Preferences Table
CREATE TABLE IF NOT EXISTS sneak_consumer_search_alerts (
    id TEXT PRIMARY KEY,
    saved_search_id TEXT NOT NULL UNIQUE REFERENCES sneak_consumer_saved_searches(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,

    frequency TEXT NOT NULL DEFAULT 'off', -- 'off', 'asap', 'daily'
    enabled INTEGER NOT NULL DEFAULT 0,
    enabled_at DATETIME,

    timezone TEXT NOT NULL DEFAULT 'America/New_York',
    return_url TEXT,

    last_checked_at DATETIME,
    last_sent_at DATETIME,
    last_daily_local_date TEXT,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_search_alerts_due ON sneak_consumer_search_alerts(enabled, frequency, site_id);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_search_alerts_user ON sneak_consumer_search_alerts(user_id, site_id);

-- 2. Alert Match Ledger Table (Idempotency & Anti-Spam)
CREATE TABLE IF NOT EXISTS sneak_consumer_alert_matches (
    id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL REFERENCES sneak_consumer_search_alerts(id) ON DELETE CASCADE,
    saved_search_id TEXT NOT NULL REFERENCES sneak_consumer_saved_searches(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    listing_key TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT 'new_listing',
    first_matched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notified_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(alert_id, listing_key, event_type)
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_alert_matches_pending ON sneak_consumer_alert_matches(alert_id, notified_at);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_alert_matches_search ON sneak_consumer_alert_matches(saved_search_id, listing_key);

-- 3. Delivery Log Table
CREATE TABLE IF NOT EXISTS sneak_consumer_alert_deliveries (
    id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL REFERENCES sneak_consumer_search_alerts(id) ON DELETE CASCADE,
    saved_search_id TEXT NOT NULL REFERENCES sneak_consumer_saved_searches(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    frequency TEXT NOT NULL,
    match_count INTEGER NOT NULL DEFAULT 0,
    listing_keys_json TEXT NOT NULL,
    status TEXT NOT NULL, -- 'sent', 'failed', 'provider_unconfigured'
    provider TEXT,
    provider_message_id TEXT,
    error_code TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_alert_deliveries_alert ON sneak_consumer_alert_deliveries(alert_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_alert_deliveries_user ON sneak_consumer_alert_deliveries(user_id, site_id);

-- 4. Alert Processing Run / Overlap Lock Table
CREATE TABLE IF NOT EXISTS sneak_alert_runs (
    id TEXT PRIMARY KEY,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    status TEXT NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed'
    searches_evaluated INTEGER NOT NULL DEFAULT 0,
    matches_found INTEGER NOT NULL DEFAULT 0,
    emails_attempted INTEGER NOT NULL DEFAULT 0,
    emails_sent INTEGER NOT NULL DEFAULT 0,
    emails_failed INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
