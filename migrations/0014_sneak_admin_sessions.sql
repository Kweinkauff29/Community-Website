-- Migration: 0014_sneak_admin_sessions.sql
-- Description: Server-side revocable admin sessions and rate limiting tables.

CREATE TABLE IF NOT EXISTS sneak_admin_sessions (
    id TEXT PRIMARY KEY,
    token_hash TEXT UNIQUE NOT NULL,
    admin_actor TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL,
    last_seen_at DATETIME,
    revoked_at DATETIME,
    user_agent_hash TEXT,
    created_ip_hash TEXT
);

CREATE INDEX IF NOT EXISTS idx_sneak_admin_sessions_token ON sneak_admin_sessions(token_hash);
CREATE INDEX IF NOT EXISTS idx_sneak_admin_sessions_actor ON sneak_admin_sessions(admin_actor, revoked_at);

CREATE TABLE IF NOT EXISTS sneak_admin_login_attempts (
    id TEXT PRIMARY KEY,
    ip_hash TEXT NOT NULL,
    attempted_at DATETIME NOT NULL,
    success INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sneak_admin_login_attempts_ip ON sneak_admin_login_attempts(ip_hash, attempted_at);
