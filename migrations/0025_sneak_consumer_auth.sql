-- Migration: 0025_sneak_consumer_auth.sql
-- Description: Consumer (property buyer) identity, passwordless magic links, site-scoped sessions, and one-time auth exchange codes.
-- Realm Isolation: Consumer identity is strictly separated from REALTOR® / member accounts (sneak_member_users).

CREATE TABLE IF NOT EXISTS sneak_consumer_users (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'active', 'disabled'
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    activated_at DATETIME,
    last_login_at DATETIME,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(site_id, email)
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_users_site_email ON sneak_consumer_users(site_id, email);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_users_email ON sneak_consumer_users(email);

CREATE TABLE IF NOT EXISTS sneak_consumer_magic_links (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    purpose TEXT NOT NULL DEFAULT 'login', -- 'login'
    return_url TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    used_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_magic_links_token ON sneak_consumer_magic_links(token_hash);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_magic_links_user ON sneak_consumer_magic_links(user_id, site_id);

CREATE TABLE IF NOT EXISTS sneak_consumer_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    last_seen_at DATETIME,
    revoked_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_sessions_token ON sneak_consumer_sessions(token_hash);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_sessions_user ON sneak_consumer_sessions(user_id, site_id, revoked_at);

CREATE TABLE IF NOT EXISTS sneak_consumer_auth_exchanges (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_consumer_users(id) ON DELETE CASCADE,
    site_id TEXT NOT NULL REFERENCES sneak_sites(id) ON DELETE CASCADE,
    code_hash TEXT UNIQUE NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    used_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_auth_exchanges_code ON sneak_consumer_auth_exchanges(code_hash);
CREATE INDEX IF NOT EXISTS idx_sneak_consumer_auth_exchanges_user ON sneak_consumer_auth_exchanges(user_id);

CREATE TABLE IF NOT EXISTS sneak_consumer_login_attempts (
    id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    identifier_hash TEXT NOT NULL,
    attempt_type TEXT NOT NULL, -- 'email' or 'ip'
    created_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_login_attempts ON sneak_consumer_login_attempts(identifier_hash, attempt_type, created_at);
