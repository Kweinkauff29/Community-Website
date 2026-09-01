-- Migration: 0034_sneak_growthzone_reconciliation.sql
-- Description: Fail-closed GrowthZone entitlement reconciliation state and Phase 7.4B1 capability evidence.

CREATE TABLE IF NOT EXISTS sneak_growthzone_reconciliation (
    account_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'never' CHECK(status IN (
        'never', 'verified_no_change', 'entitlement_changed', 'mapping_ambiguous',
        'manual_override', 'remote_unavailable', 'invalid_response', 'not_configured'
    )),
    external_reference TEXT,
    remote_contact_id TEXT,
    remote_membership_id TEXT,
    remote_status TEXT,
    normalized_status TEXT,
    difference TEXT,
    error_code TEXT,
    snapshot_json TEXT,
    last_attempt_at DATETIME,
    last_success_at DATETIME,
    last_changed_at DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_growthzone_reconciliation_status
    ON sneak_growthzone_reconciliation(status, last_attempt_at);

INSERT OR IGNORE INTO sneak_launch_checks (check_key, status, source, checked_at, detail_json) VALUES
('member_magic_link_e2e', 'pending', 'system', datetime('now'), '{"description":"Controlled inbox member magic link delivered, consumed, and replay rejected"}'),
('consumer_magic_link_e2e', 'pending', 'system', datetime('now'), '{"description":"Controlled inbox consumer magic link delivered, consumed, and replay rejected"}'),
('alerts_asap_e2e', 'pending', 'system', datetime('now'), '{"description":"Controlled inbox ASAP saved-search alert delivered without duplicate send"}'),
('alerts_daily_e2e', 'pending', 'system', datetime('now'), '{"description":"Controlled inbox Daily saved-search alert delivered once for the local day"}'),
('alerts_unsubscribe_e2e', 'pending', 'system', datetime('now'), '{"description":"Signed unsubscribe link consumed and alert disabled"}'),
('growthzone_reconciliation_e2e', 'pending', 'system', datetime('now'), '{"description":"Authenticated GrowthZone response reconciled through an explicit durable mapping"}');
