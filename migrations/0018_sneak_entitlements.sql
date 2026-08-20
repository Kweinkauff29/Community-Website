-- Migration: 0018_sneak_entitlements.sql
-- Description: Generic SNEAK Account Entitlement Model (GrowthZone / Manual Billing)

CREATE TABLE IF NOT EXISTS sneak_account_entitlements (
    account_id TEXT PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'manual', -- 'manual', 'growthzone'
    status TEXT NOT NULL DEFAULT 'active', -- 'active', 'grace', 'delinquent', 'suspended', 'canceled'
    plan TEXT,
    effective_at DATETIME,
    expires_at DATETIME,
    grace_until DATETIME,
    external_reference TEXT, -- GrowthZone Contact / Member / Billing Reference ID
    notes TEXT,
    last_verified_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_entitlements_status ON sneak_account_entitlements(status);
CREATE INDEX IF NOT EXISTS idx_sneak_entitlements_ext_ref ON sneak_account_entitlements(external_reference);
