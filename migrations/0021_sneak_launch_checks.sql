-- Migration: 0021_sneak_launch_checks.sql
-- Description: Creates sneak_launch_checks table to store authoritative evidence for SNEAK pilot readiness.

CREATE TABLE IF NOT EXISTS sneak_launch_checks (
    check_key TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('pass', 'pending', 'fail')),
    source TEXT NOT NULL,
    checked_at DATETIME NOT NULL,
    detail_json TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Seed initial pending checks
INSERT OR IGNORE INTO sneak_launch_checks (check_key, status, source, checked_at, detail_json) VALUES
('cloudflare_saas_enabled', 'pending', 'system', datetime('now'), '{"description":"Cloudflare for SaaS enabled on provider zone"}'),
('cloudflare_fallback_active', 'pending', 'system', datetime('now'), '{"description":"Cloudflare for SaaS fallback origin active"}'),
('cloudflare_real_custom_hostname', 'pending', 'system', datetime('now'), '{"description":"Real Cloudflare custom hostname created"}'),
('cloudflare_real_ssl', 'pending', 'system', datetime('now'), '{"description":"Real Cloudflare SSL certificate active"}'),
('cloudflare_real_https', 'pending', 'system', datetime('now'), '{"description":"Real HTTPS request to custom hostname succeeds without headers"}'),
('cloudflare_real_idx', 'pending', 'system', datetime('now'), '{"description":"Real SNEAK IDX bootstrap and search active from customer origin"}'),
('cloudflare_real_removal', 'pending', 'system', datetime('now'), '{"description":"Real custom hostname deletion and deauthorization verified"}'),
('email_provider_configured', 'pending', 'system', datetime('now'), '{"description":"Live transactional email provider credential configured"}'),
('email_domain_verified', 'pending', 'system', datetime('now'), '{"description":"Sender domain verified with DKIM and SPF"}'),
('email_real_invitation', 'pending', 'system', datetime('now'), '{"description":"Real member invitation email received and consumed"}'),
('email_real_login', 'pending', 'system', datetime('now'), '{"description":"Real passwordless magic link email received and consumed"}'),
('email_replay_protection', 'pending', 'system', datetime('now'), '{"description":"Single-use token consumption and replay protection verified"}');
