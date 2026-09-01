-- Migration: 0035_sneak_growthzone_reconciliation_fk.sql
-- Description: Rebuild GrowthZone reconciliation state with canonical account FK integrity.

-- Abort instead of silently dropping any orphaned reconciliation state.
CREATE TABLE sneak_growthzone_reconciliation_fk_guard (
    orphan_count INTEGER NOT NULL CHECK(orphan_count = 0)
);

INSERT INTO sneak_growthzone_reconciliation_fk_guard (orphan_count)
SELECT COUNT(*)
FROM sneak_growthzone_reconciliation reconciliation
LEFT JOIN sneak_accounts account_record ON account_record.id = reconciliation.account_id
WHERE account_record.id IS NULL;

DROP TABLE sneak_growthzone_reconciliation_fk_guard;

CREATE TABLE sneak_growthzone_reconciliation_next (
    account_id TEXT PRIMARY KEY
        REFERENCES sneak_accounts(id)
        ON DELETE CASCADE,
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

INSERT INTO sneak_growthzone_reconciliation_next (
    account_id, status, external_reference, remote_contact_id, remote_membership_id,
    remote_status, normalized_status, difference, error_code, snapshot_json,
    last_attempt_at, last_success_at, last_changed_at, created_at, updated_at
)
SELECT
    account_id, status, external_reference, remote_contact_id, remote_membership_id,
    remote_status, normalized_status, difference, error_code, snapshot_json,
    last_attempt_at, last_success_at, last_changed_at, created_at, updated_at
FROM sneak_growthzone_reconciliation;

DROP TABLE sneak_growthzone_reconciliation;
ALTER TABLE sneak_growthzone_reconciliation_next RENAME TO sneak_growthzone_reconciliation;

CREATE INDEX idx_sneak_growthzone_reconciliation_status
    ON sneak_growthzone_reconciliation(status, last_attempt_at);

PRAGMA foreign_key_check;
