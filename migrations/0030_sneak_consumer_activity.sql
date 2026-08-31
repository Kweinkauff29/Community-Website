-- 0030_sneak_consumer_activity.sql
-- SNEAK IDX Phase 7.3C2B: Authenticated Consumer Activity Ledger & Last Activity Tracking

CREATE TABLE IF NOT EXISTS sneak_consumer_activity_events (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    listing_key TEXT,
    saved_search_id TEXT,
    lead_id TEXT,
    metadata_json TEXT,
    dedupe_key TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_consumer_activity_site_created ON sneak_consumer_activity_events(site_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_consumer_activity_user_site_created ON sneak_consumer_activity_events(user_id, site_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_consumer_activity_event_type ON sneak_consumer_activity_events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_consumer_activity_listing_key ON sneak_consumer_activity_events(listing_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_consumer_activity_dedupe ON sneak_consumer_activity_events(dedupe_key) WHERE dedupe_key IS NOT NULL;

-- Add last_activity_at timestamp to sneak_consumer_users
ALTER TABLE sneak_consumer_users ADD COLUMN last_activity_at DATETIME;
