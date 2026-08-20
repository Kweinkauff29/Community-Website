-- Migration: 0013_sneak_admin_audit.sql
-- Description: Audit trail for administrative actions (account, site, domain, branding mutations).

CREATE TABLE IF NOT EXISTS sneak_admin_audit (
    id TEXT PRIMARY KEY,
    admin_actor TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT,
    summary TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_admin_audit_created ON sneak_admin_audit(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sneak_admin_audit_entity ON sneak_admin_audit(entity_type, entity_id);
