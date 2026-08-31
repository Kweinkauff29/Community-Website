-- MANUAL LEGACY ENVIRONMENT REPAIR — NOT AN AUTOMATIC MIGRATION
--
-- Use only after scripts/check-sneak-site-fk-compatibility.mjs reports:
--   LEGACY EMPTY — MANUAL EMPTY-SCHEMA REPAIR AVAILABLE
--
-- This script intentionally refuses to run when any consumer-related row
-- exists. A populated legacy database requires a reviewed, data-preserving
-- migration; do not weaken or bypass the guard.

PRAGMA defer_foreign_keys = ON;

CREATE TABLE _sneak_consumer_fk_repair_guard (
    row_count INTEGER NOT NULL CHECK (row_count = 0)
);

INSERT INTO _sneak_consumer_fk_repair_guard (row_count)
SELECT
    (SELECT COUNT(*) FROM sneak_consumer_users) +
    (SELECT COUNT(*) FROM sneak_consumer_magic_links) +
    (SELECT COUNT(*) FROM sneak_consumer_sessions) +
    (SELECT COUNT(*) FROM sneak_consumer_auth_exchanges) +
    (SELECT COUNT(*) FROM sneak_consumer_favorites) +
    (SELECT COUNT(*) FROM sneak_consumer_saved_searches) +
    (SELECT COUNT(*) FROM sneak_consumer_search_alerts) +
    (SELECT COUNT(*) FROM sneak_consumer_alert_deliveries) +
    (SELECT COUNT(*) FROM sneak_consumer_alert_matches) +
    (SELECT COUNT(*) FROM sneak_consumer_activity_events) +
    (SELECT COUNT(*) FROM sneak_consumer_compare);

DROP TABLE sneak_consumer_magic_links;
DROP TABLE sneak_consumer_sessions;
DROP TABLE sneak_consumer_auth_exchanges;
DROP TABLE sneak_consumer_favorites;
DROP TABLE sneak_consumer_users;

CREATE TABLE sneak_consumer_users (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    activated_at DATETIME,
    last_login_at DATETIME,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_activity_at DATETIME,
    UNIQUE(site_id, email)
);

CREATE INDEX idx_sneak_consumer_users_site_email
    ON sneak_consumer_users(site_id, email);
CREATE INDEX idx_sneak_consumer_users_email
    ON sneak_consumer_users(email);

CREATE TABLE sneak_consumer_magic_links (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    purpose TEXT NOT NULL DEFAULT 'login',
    return_url TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    used_at DATETIME
);

CREATE INDEX idx_sneak_consumer_magic_links_token
    ON sneak_consumer_magic_links(token_hash);
CREATE INDEX idx_sneak_consumer_magic_links_user
    ON sneak_consumer_magic_links(user_id, site_id);

CREATE TABLE sneak_consumer_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    last_seen_at DATETIME,
    revoked_at DATETIME
);

CREATE INDEX idx_sneak_consumer_sessions_token
    ON sneak_consumer_sessions(token_hash);
CREATE INDEX idx_sneak_consumer_sessions_user
    ON sneak_consumer_sessions(user_id, site_id, revoked_at);

CREATE TABLE sneak_consumer_auth_exchanges (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    code_hash TEXT UNIQUE NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    used_at DATETIME
);

CREATE INDEX idx_sneak_consumer_auth_exchanges_code
    ON sneak_consumer_auth_exchanges(code_hash);
CREATE INDEX idx_sneak_consumer_auth_exchanges_user
    ON sneak_consumer_auth_exchanges(user_id);

CREATE TABLE sneak_consumer_favorites (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    listing_key TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, site_id, listing_key)
);

CREATE INDEX idx_sneak_consumer_favorites_user_site
    ON sneak_consumer_favorites(user_id, site_id, created_at DESC);
CREATE INDEX idx_sneak_consumer_favorites_listing
    ON sneak_consumer_favorites(listing_key);

DROP TABLE _sneak_consumer_fk_repair_guard;
