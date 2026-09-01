/**
 * Canonical CCOR IDX service-entitlement and tenant-serving rules.
 *
 * sneak_account_entitlements is the only MVP subscription authority.
 * Legacy sneak_account_billing / Stripe rows are intentionally ignored.
 */

export const ENTITLEMENT_STATUSES = new Set([
    'active',
    'grace',
    'delinquent',
    'suspended',
    'canceled'
]);

export const TENANT_SCOPE_TYPES = new Set(['market', 'agent', 'office']);

export const SERVING_BLOCKERS = Object.freeze({
    ACCOUNT_INACTIVE: 'The account is not operational.',
    SITE_INACTIVE: 'The IDX site is not active.',
    ENTITLEMENT_MISSING: 'No authoritative service entitlement exists.',
    ENTITLEMENT_INACTIVE: 'The service entitlement does not allow IDX serving.',
    ENTITLEMENT_EXPIRED: 'The service entitlement has expired.',
    GRACE_EXPIRED: 'The entitlement grace period has expired.',
    DOMAIN_UNAUTHORIZED: 'The request domain is not active and verified for this site.',
    SCOPE_INVALID: 'The tenant listing scope is incomplete or invalid.'
});

function blocker(code) {
    return { code, message: SERVING_BLOCKERS[code] };
}

function isAfter(value, now) {
    if (!value) return false;
    const timestamp = new Date(value).getTime();
    return Number.isFinite(timestamp) && now.getTime() > timestamp;
}

/**
 * Pure evaluation of the generic entitlement record.
 */
export function evaluateEntitlement({ accountStatus, entitlementStatus, graceUntil, expiresAt, now = new Date() } = {}) {
    if (accountStatus !== 'active') {
        return { allowed: false, blocker: blocker('ACCOUNT_INACTIVE') };
    }

    const status = String(entitlementStatus || '').trim().toLowerCase();
    if (!status) {
        return { allowed: false, blocker: blocker('ENTITLEMENT_MISSING') };
    }
    if (!ENTITLEMENT_STATUSES.has(status)) {
        return { allowed: false, blocker: blocker('ENTITLEMENT_INACTIVE') };
    }
    if (expiresAt && isAfter(expiresAt, now)) {
        return { allowed: false, blocker: blocker('ENTITLEMENT_EXPIRED') };
    }
    if (status === 'active') {
        return { allowed: true, blocker: null };
    }
    if (status === 'grace' || status === 'delinquent') {
        const validGrace = Boolean(graceUntil) && !isAfter(graceUntil, now);
        return validGrace
            ? { allowed: true, blocker: null }
            : { allowed: false, blocker: blocker('GRACE_EXPIRED') };
    }
    return { allowed: false, blocker: blocker('ENTITLEMENT_INACTIVE') };
}

export function isAccountEntitled(accountStatus, entitlementStatus, graceUntil, now = new Date(), expiresAt = null) {
    return evaluateEntitlement({ accountStatus, entitlementStatus, graceUntil, expiresAt, now }).allowed;
}

export function isTenantScopeValid(scopeType, scopeValue) {
    const normalizedType = String(scopeType || '').trim().toLowerCase();
    if (!TENANT_SCOPE_TYPES.has(normalizedType)) return false;
    if (normalizedType === 'market') return true;
    return Boolean(String(scopeValue || '').trim());
}

/**
 * Pure, normalized answer to: "May this site currently serve IDX?"
 */
export function evaluateServingDecision({
    accountStatus,
    siteStatus,
    entitlementStatus,
    graceUntil = null,
    expiresAt = null,
    domainAuthorized,
    scopeType,
    scopeValue = null,
    now = new Date()
} = {}) {
    const blockers = [];

    const entitlement = evaluateEntitlement({
        accountStatus,
        entitlementStatus,
        graceUntil,
        expiresAt,
        now
    });
    if (entitlement.blocker) blockers.push(entitlement.blocker);
    if (siteStatus !== 'active') blockers.push(blocker('SITE_INACTIVE'));
    if (domainAuthorized !== true) blockers.push(blocker('DOMAIN_UNAUTHORIZED'));
    if (!isTenantScopeValid(scopeType, scopeValue)) blockers.push(blocker('SCOPE_INVALID'));

    return { canServe: blockers.length === 0, blockers };
}

export function humanizeEntitlementStatus(status) {
    switch (String(status || '').toLowerCase()) {
        case 'active': return 'Active';
        case 'grace': return 'Grace Period';
        case 'delinquent': return 'Payment Attention Required';
        case 'suspended': return 'Suspended — Contact CCOR';
        case 'canceled': return 'Canceled';
        default: return 'Not Configured — Contact CCOR';
    }
}
