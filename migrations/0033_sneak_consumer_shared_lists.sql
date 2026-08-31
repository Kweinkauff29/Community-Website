-- 0033_sneak_consumer_shared_lists.sql
-- SNEAK IDX Phase 7.3C3B: private-by-default consumer property lists with
-- revocable, unlisted public capability links.
--
-- These tables store listing references only. Public and owner reads must
-- resolve current eligible MLS state from sneak_listings.

CREATE TABLE IF NOT EXISTS sneak_consumer_shared_lists (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 80),
    share_enabled INTEGER NOT NULL DEFAULT 0 CHECK (share_enabled IN (0, 1)),
    public_slug TEXT UNIQUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_shared_lists_owner
    ON sneak_consumer_shared_lists(user_id, site_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_shared_lists_public
    ON sneak_consumer_shared_lists(site_id, public_slug, share_enabled);

CREATE TABLE IF NOT EXISTS sneak_consumer_shared_list_items (
    id TEXT PRIMARY KEY,
    list_id TEXT NOT NULL REFERENCES sneak_consumer_shared_lists(id) ON DELETE CASCADE,
    listing_key TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0 CHECK (sort_order >= 0),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(list_id, listing_key)
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_shared_list_items_list
    ON sneak_consumer_shared_list_items(list_id, sort_order ASC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_shared_list_items_listing
    ON sneak_consumer_shared_list_items(listing_key);

-- Server handlers provide friendly errors; triggers preserve limits under
-- concurrent requests and direct administrative writes.
CREATE TRIGGER IF NOT EXISTS trg_sneak_consumer_shared_lists_max_ten
BEFORE INSERT ON sneak_consumer_shared_lists
WHEN (
    SELECT COUNT(*)
    FROM sneak_consumer_shared_lists
    WHERE user_id = NEW.user_id AND site_id = NEW.site_id
) >= 10
BEGIN
    SELECT RAISE(ABORT, 'shared_list_limit_exceeded');
END;

CREATE TRIGGER IF NOT EXISTS trg_sneak_consumer_shared_list_items_max_twenty_five
BEFORE INSERT ON sneak_consumer_shared_list_items
WHEN (
    SELECT COUNT(*)
    FROM sneak_consumer_shared_list_items
    WHERE list_id = NEW.list_id
) >= 25
AND NOT EXISTS (
    SELECT 1
    FROM sneak_consumer_shared_list_items
    WHERE list_id = NEW.list_id AND listing_key = NEW.listing_key
)
BEGIN
    SELECT RAISE(ABORT, 'shared_list_item_limit_exceeded');
END;
