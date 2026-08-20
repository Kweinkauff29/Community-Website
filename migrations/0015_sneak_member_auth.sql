-- Migration: 0015_sneak_member_auth.sql
-- Description: Passwordless member authentication, magic links, member sessions, and member audit logging.

CREATE TABLE IF NOT EXISTS sneak_member_users (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES sneak_accounts(id) ON DELETE CASCADE,
    email TEXT UNIQUE NOT NULL,
    role TEXT NOT NULL DEFAULT 'owner', -- 'owner', 'admin', 'viewer'
    status TEXT NOT NULL DEFAULT 'invited', -- 'invited', 'active', 'disabled'
    invited_at DATETIME,
    activated_at DATETIME,
    last_login_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_member_users_account ON sneak_member_users(account_id);
CREATE INDEX IF NOT EXISTS idx_sneak_member_users_email ON sneak_member_users(email);

CREATE TABLE IF NOT EXISTS sneak_member_magic_links (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_member_users(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    purpose TEXT NOT NULL DEFAULT 'login', -- 'login', 'invite'
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    used_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_sneak_member_magic_links_token ON sneak_member_magic_links(token_hash);
CREATE INDEX IF NOT EXISTS idx_sneak_member_magic_links_user ON sneak_member_magic_links(user_id);

CREATE TABLE IF NOT EXISTS sneak_member_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES sneak_member_users(id) ON DELETE CASCADE,
    account_id TEXT NOT NULL REFERENCES sneak_accounts(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    last_seen_at DATETIME,
    revoked_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_sneak_member_sessions_token ON sneak_member_sessions(token_hash);
CREATE INDEX IF NOT EXISTS idx_sneak_member_sessions_user ON sneak_member_sessions(user_id, revoked_at);

CREATE TABLE IF NOT EXISTS sneak_member_audit (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    account_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT,
    summary TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_member_audit_created ON sneak_member_audit(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sneak_member_audit_account ON sneak_member_audit(account_id);
