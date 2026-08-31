-- 0031_sneak_consumer_compare.sql
-- SNEAK IDX Phase 7.3C3A: site-scoped authenticated property compare keys.
-- Stores references only; listing data is always resolved from current sneak_listings state.

CREATE TABLE IF NOT EXISTS sneak_consumer_compare (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    listing_key TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, site_id, listing_key)
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_compare_user_site
    ON sneak_consumer_compare(user_id, site_id, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_compare_listing
    ON sneak_consumer_compare(listing_key);

-- Enforce the four-property ceiling even when concurrent requests race.
CREATE TRIGGER IF NOT EXISTS trg_sneak_consumer_compare_max_four
BEFORE INSERT ON sneak_consumer_compare
WHEN (
    SELECT COUNT(*)
    FROM sneak_consumer_compare
    WHERE user_id = NEW.user_id AND site_id = NEW.site_id
) >= 4
AND NOT EXISTS (
    SELECT 1
    FROM sneak_consumer_compare
    WHERE user_id = NEW.user_id AND site_id = NEW.site_id AND listing_key = NEW.listing_key
)
BEGIN
    SELECT RAISE(ABORT, 'compare_limit_exceeded');
END;
