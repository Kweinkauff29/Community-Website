/**
 * sneak-member/billing.js
 * 
 * SNEAK Generic Account Entitlement Engine (GrowthZone System of Record):
 * - Recurring billing administered on the 1st of each month via GrowthZone.
 * - SNEAK stores service entitlements (active, grace, delinquent, suspended, canceled).
 * - Zero cardholder data or payment tokens stored in SNEAK.
 */

import { isAccountEntitled, humanizeEntitlementStatus } from '../sneak-shared/entitlement.js';

// Compatibility re-export for existing callers. The implementation is centralized.
export { isAccountEntitled };

/**
 * Retrieves full billing & entitlement overview for an account.
 */
export async function getAccountEntitlement(db, accountId) {
    if (!db || !accountId) return null;

    const query = `
        SELECT 
            e.account_id, e.source, e.status AS entitlement_status, e.plan,
            e.effective_at, e.expires_at, e.grace_until, e.external_reference,
            e.notes, e.last_verified_at, e.created_at, e.updated_at,
            a.account_name, a.plan AS account_plan, a.status AS account_status
        FROM sneak_accounts a
        LEFT JOIN sneak_account_entitlements e ON a.id = e.account_id
        WHERE a.id = ?
    `;

    const row = await db.prepare(query).bind(accountId).first();
    if (!row) return null;

    const isEntitled = isAccountEntitled(
        row.account_status,
        row.entitlement_status,
        row.grace_until,
        new Date(),
        row.expires_at
    );

    return {
        accountId: row.account_id,
        accountName: row.account_name,
        plan: row.plan || row.account_plan || 'pro',
        provider: 'GrowthZone',
        billingCycle: 'Monthly (1st of each month)',
        status: row.entitlement_status || 'missing',
        statusLabel: humanizeEntitlementStatus(row.entitlement_status),
        isEntitled,
        graceUntil: row.grace_until || null,
        externalReference: row.external_reference || null,
        lastVerifiedAt: row.last_verified_at || row.updated_at || null,
        notes: row.notes || null
    };
}

/**
 * Updates or creates account entitlement record (Admin / GrowthZone support sync).
 */
export async function setAccountEntitlement(db, accountId, {
    source = 'manual',
    status = 'active',
    plan = null,
    effectiveAt = null,
    expiresAt = null,
    graceUntil = null,
    externalReference = null,
    notes = null
}) {
    if (!db || !accountId) throw new Error('Missing account ID');

    const now = new Date().toISOString();

    await db.prepare(`
        INSERT INTO sneak_account_entitlements (
            account_id, source, status, plan, effective_at, expires_at, grace_until,
            external_reference, notes, last_verified_at, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(account_id) DO UPDATE SET
            source = excluded.source,
            status = excluded.status,
            plan = COALESCE(excluded.plan, sneak_account_entitlements.plan),
            effective_at = COALESCE(excluded.effective_at, sneak_account_entitlements.effective_at),
            expires_at = excluded.expires_at,
            grace_until = excluded.grace_until,
            external_reference = COALESCE(excluded.external_reference, sneak_account_entitlements.external_reference),
            notes = COALESCE(excluded.notes, sneak_account_entitlements.notes),
            last_verified_at = excluded.last_verified_at,
            updated_at = excluded.updated_at
    `).bind(
        accountId, source, status, plan, effectiveAt || now, expiresAt, graceUntil,
        externalReference, notes, now, now, now
    ).run();

    return getAccountEntitlement(db, accountId);
}
