-- Migration: 0017_sneak_member_rate_limits.sql
-- Description: Pseudonymized IP and email hash rate limiting for public member authentication endpoints.

CREATE TABLE IF NOT EXISTS sneak_member_login_attempts (
    id TEXT PRIMARY KEY,
    ip_hash TEXT NOT NULL,
    email_hash TEXT NOT NULL,
    attempted_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sneak_member_login_ip ON sneak_member_login_attempts(ip_hash, attempted_at DESC);
CREATE INDEX IF NOT EXISTS idx_sneak_member_login_email ON sneak_member_login_attempts(email_hash, attempted_at DESC);
