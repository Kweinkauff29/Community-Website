/**
 * GrowthZone -> canonical SNEAK entitlement reconciliation.
 *
 * Security boundaries:
 * - GrowthZone is never consulted by a public serving path.
 * - Automatic mutation requires source=growthzone and an explicit durable
 *   external_reference: person:<contactId>[:membership:<membershipId>] or
 *   org:<contactId>[:membership:<membershipId>].
 * - Unknown/ambiguous responses and transport failures never change service.
 * - Manual entitlements are never overwritten.
 */

const VALID_CANONICAL_STATUSES = new Set(['active', 'grace', 'delinquent', 'suspended', 'canceled']);
const VALID_CANONICAL_PLANS = new Set(['trial', 'standard', 'pro', 'brokerage']);
const MAX_RESPONSE_BYTES = 256 * 1024;
const DEFAULT_TIMEOUT_MS = 10000;
const MAX_BATCH_SIZE = 50;

function cleanText(value, max = 200) {
    if (value === null || value === undefined) return null;
    const text = String(value).trim();
    return text ? text.slice(0, max) : null;
}

function toIso(value) {
    if (!value) return null;
    const parsed = new Date(value);
    return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null;
}

function flag(value) {
    return value === true || value === 1 || ['true', 'yes', '1', 'y'].includes(String(value || '').trim().toLowerCase());
}

function membershipIdOf(value) {
    return cleanText(value?.MembershipId ?? value?.membershipId ?? value?.Id ?? value?.id, 80);
}

export function parseGrowthZoneReference(value) {
    const reference = cleanText(value, 200);
    if (!reference) return null;
    const match = reference.match(/^(person|org):(\d+)(?::membership:(\d+))?$/i);
    if (!match) return null;
    return {
        reference,
        contactType: match[1].toLowerCase(),
        contactId: match[2],
        membershipId: match[3] || null
    };
}

function statusParts(value) {
    const raw = cleanText(value, 80);
    if (!raw) return { code: null, name: '' };
    const match = raw.match(/^\s*(\d+)\s*(?:-\s*(.*))?$/);
    if (match) return { code: Number(match[1]), name: String(match[2] || '').trim().toLowerCase() };
    return { code: Number.isFinite(Number(raw)) ? Number(raw) : null, name: raw.toLowerCase() };
}

function mapMembershipStatus(membership) {
    if (flag(membership?.IsInGracePeriod)) return 'grace';
    if (flag(membership?.IsInactive) || flag(membership?.InActive)) return 'canceled';

    const numeric = statusParts(membership?.MembershipStatusTypeId ?? membership?.membershipStatusTypeId);
    const named = cleanText(membership?.Status ?? membership?.status ?? numeric.name, 80)?.toLowerCase() || '';
    if ([2, 3, 4].includes(numeric.code) || ['active', 'trial', 'courtesy'].includes(named)) return 'active';
    if ([1, 7, 8, 9].includes(numeric.code) || ['pending approval', 'pendingapproval', 'lead', 'prospect', 'suspended'].includes(named)) return 'suspended';
    if ([0, 5, 6].includes(numeric.code) || ['nonmember', 'non member', 'dropped', 'expired', 'canceled', 'cancelled'].includes(named)) return 'canceled';
    if (named === 'grace' || named === 'in grace period') return 'grace';
    if (named === 'delinquent') return 'delinquent';
    return null;
}

function mappedPlan(env, membership, currentPlan) {
    let map = {};
    try {
        map = JSON.parse(env?.GROWTHZONE_PLAN_MAP_JSON || '{}');
    } catch {
        return currentPlan;
    }
    if (!map || typeof map !== 'object' || Array.isArray(map)) return currentPlan;
    const typeId = cleanText(membership?.MembershipTypeId ?? membership?.membershipTypeId, 80);
    const name = cleanText(membership?.Name ?? membership?.name, 120);
    const candidate = (typeId && map[typeId]) || (name && map[name]);
    return typeof candidate === 'string' && VALID_CANONICAL_PLANS.has(candidate.trim().toLowerCase())
        ? candidate.trim().toLowerCase()
        : currentPlan;
}

export function normalizeGrowthZoneMembership(membership, { env = {}, currentPlan = null } = {}) {
    if (!membership || typeof membership !== 'object' || Array.isArray(membership)) {
        return { ok: false, code: 'InvalidMembershipPayload' };
    }
    const status = mapMembershipStatus(membership);
    const membershipId = membershipIdOf(membership);
    if (!status || !VALID_CANONICAL_STATUSES.has(status) || !membershipId) {
        return { ok: false, code: status ? 'MembershipIdMissing' : 'UnknownMembershipStatus' };
    }
    const statusDescriptor = cleanText(membership?.Status ?? membership?.MembershipStatusTypeId, 80);
    return {
        ok: true,
        membershipId,
        status,
        plan: mappedPlan(env, membership, currentPlan),
        effectiveAt: toIso(membership?.StartDate ?? membership?.startDate),
        expiresAt: toIso(membership?.ExpirationDate ?? membership?.expirationDate),
        graceUntil: toIso(membership?.GracePeriodExpirationDate ?? membership?.gracePeriodExpirationDate),
        remoteStatus: statusDescriptor,
        membershipTypeId: cleanText(membership?.MembershipTypeId ?? membership?.membershipTypeId, 80),
        membershipName: cleanText(membership?.Name ?? membership?.name, 120)
    };
}

async function readBoundedJson(response) {
    const declared = Number(response.headers.get('Content-Length') || 0);
    if (declared > MAX_RESPONSE_BYTES) throw new Error('GrowthZoneResponseTooLarge');
    if (!response.body || typeof response.body.getReader !== 'function') return response.json();
    const reader = response.body.getReader();
    const chunks = [];
    let size = 0;
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > MAX_RESPONSE_BYTES) {
            await reader.cancel();
            throw new Error('GrowthZoneResponseTooLarge');
        }
        chunks.push(value);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return JSON.parse(new TextDecoder().decode(bytes));
}

export async function fetchGrowthZoneMemberships(env, reference, fetchImpl = fetch) {
    const baseUrl = cleanText(env?.GROWTHZONE_BASE_URL, 500);
    const apiKey = env?.GROWTHZONE_API_KEY;
    if (!baseUrl || !apiKey) return { ok: false, code: 'GrowthZoneNotConfigured', configured: false };

    let endpoint;
    try {
        const base = new URL(baseUrl);
        if (base.protocol !== 'https:') return { ok: false, code: 'GrowthZoneBaseUrlInvalid', configured: false };
        endpoint = new URL(`/api/contacts/${reference.contactType}/${reference.contactId}/memberships`, base);
    } catch {
        return { ok: false, code: 'GrowthZoneBaseUrlInvalid', configured: false };
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), Number(env?.GROWTHZONE_TIMEOUT_MS || DEFAULT_TIMEOUT_MS));
    try {
        const response = await fetchImpl(endpoint.toString(), {
            method: 'GET',
            headers: { Accept: 'application/json', Authorization: `ApiKey ${apiKey}` },
            signal: controller.signal
        });
        if (!response.ok) {
            return { ok: false, configured: true, code: `GrowthZoneHttp${response.status}`, retryable: response.status === 429 || response.status >= 500 };
        }
        const data = await readBoundedJson(response);
        if (!Array.isArray(data)) return { ok: false, configured: true, code: 'GrowthZoneResponseInvalid', retryable: false };
        return { ok: true, configured: true, memberships: data.slice(0, 100) };
    } catch (err) {
        return { ok: false, configured: true, code: err?.name === 'AbortError' ? 'GrowthZoneTimeout' : cleanText(err?.message, 100) || 'GrowthZoneUnavailable', retryable: true };
    } finally {
        clearTimeout(timeout);
    }
}

async function audit(db, actor, action, accountId, summary) {
    await db.prepare(`
        INSERT INTO sneak_admin_audit (id, admin_actor, action, entity_type, entity_id, summary, created_at)
        VALUES (?, ?, ?, 'account', ?, ?, datetime('now'))
    `).bind(`audit_${crypto.randomUUID()}`, actor, action, accountId, cleanText(summary, 500)).run();
}

async function recordState(db, accountId, values) {
    const now = values.now || new Date().toISOString();
    const snapshot = values.snapshot ? JSON.stringify(values.snapshot) : null;
    await db.prepare(`
        INSERT INTO sneak_growthzone_reconciliation (
            account_id, status, external_reference, remote_contact_id, remote_membership_id,
            remote_status, normalized_status, difference, error_code, snapshot_json,
            last_attempt_at, last_success_at, last_changed_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(account_id) DO UPDATE SET
            status = excluded.status,
            external_reference = excluded.external_reference,
            remote_contact_id = excluded.remote_contact_id,
            remote_membership_id = excluded.remote_membership_id,
            remote_status = excluded.remote_status,
            normalized_status = excluded.normalized_status,
            difference = excluded.difference,
            error_code = excluded.error_code,
            snapshot_json = excluded.snapshot_json,
            last_attempt_at = excluded.last_attempt_at,
            last_success_at = COALESCE(excluded.last_success_at, sneak_growthzone_reconciliation.last_success_at),
            last_changed_at = COALESCE(excluded.last_changed_at, sneak_growthzone_reconciliation.last_changed_at),
            updated_at = excluded.updated_at
    `).bind(
        accountId, values.status, values.reference || null, values.contactId || null,
        values.membershipId || null, values.remoteStatus || null, values.normalizedStatus || null,
        values.difference || null, values.errorCode || null, snapshot,
        now, values.success ? now : null, values.changed ? now : null, now, now
    ).run();
}

function sameNullable(a, b) {
    return (a || null) === (b || null);
}

export async function reconcileGrowthZoneAccount(db, env, accountId, { actor = 'system:growthzone', now = new Date(), fetchImpl = fetch } = {}) {
    const attemptedAt = (now instanceof Date ? now : new Date(now)).toISOString();
    const row = await db.prepare(`
        SELECT a.id AS account_id, a.plan AS account_plan, e.*
        FROM sneak_accounts a
        LEFT JOIN sneak_account_entitlements e ON e.account_id = a.id
        WHERE a.id = ?
    `).bind(accountId).first();
    if (!row) return { ok: false, code: 'AccountNotFound', status: 404 };

    if (!row.source) {
        await recordState(db, accountId, { status: 'mapping_ambiguous', errorCode: 'CanonicalEntitlementMissing', now: attemptedAt });
        await audit(db, actor, 'GROWTHZONE_MAPPING_AMBIGUOUS', accountId, 'No canonical entitlement exists; GrowthZone reconciliation made no change.');
        return { ok: false, changed: false, reconciliationStatus: 'mapping_ambiguous', code: 'CanonicalEntitlementMissing' };
    }

    if (row.source !== 'growthzone') {
        await recordState(db, accountId, { status: 'manual_override', reference: row.external_reference, errorCode: 'ManualSourceProtected', now: attemptedAt });
        await audit(db, actor, 'GROWTHZONE_MANUAL_OVERRIDE', accountId, 'Skipped GrowthZone reconciliation because the canonical entitlement source is manual.');
        return { ok: true, changed: false, reconciliationStatus: 'manual_override' };
    }

    const reference = parseGrowthZoneReference(row.external_reference);
    if (!reference) {
        await recordState(db, accountId, { status: 'mapping_ambiguous', reference: row.external_reference, errorCode: 'ExplicitReferenceRequired', now: attemptedAt });
        await audit(db, actor, 'GROWTHZONE_MAPPING_AMBIGUOUS', accountId, 'GrowthZone reconciliation requires an explicit person/org contact reference; no entitlement change was made.');
        return { ok: false, changed: false, reconciliationStatus: 'mapping_ambiguous', code: 'ExplicitReferenceRequired' };
    }

    const remote = await fetchGrowthZoneMemberships(env, reference, fetchImpl);
    if (!remote.ok) {
        const status = remote.configured === false ? 'not_configured' : (remote.code === 'GrowthZoneResponseInvalid' ? 'invalid_response' : 'remote_unavailable');
        await recordState(db, accountId, { status, reference: reference.reference, contactId: reference.contactId, errorCode: remote.code, now: attemptedAt });
        await audit(db, actor, status === 'not_configured' ? 'GROWTHZONE_NOT_CONFIGURED' : 'GROWTHZONE_REMOTE_UNAVAILABLE', accountId, `GrowthZone verification failed (${remote.code}); the canonical entitlement was preserved.`);
        return { ok: false, changed: false, reconciliationStatus: status, code: remote.code };
    }

    let selected = null;
    if (reference.membershipId) {
        selected = remote.memberships.find(item => membershipIdOf(item) === reference.membershipId) || null;
    } else if (remote.memberships.length === 1) {
        selected = remote.memberships[0];
    }
    if (!selected) {
        const code = remote.memberships.length === 0 ? 'MembershipNotFound' : 'MembershipSelectionAmbiguous';
        await recordState(db, accountId, { status: 'mapping_ambiguous', reference: reference.reference, contactId: reference.contactId, errorCode: code, now: attemptedAt });
        await audit(db, actor, 'GROWTHZONE_MAPPING_AMBIGUOUS', accountId, `GrowthZone returned no unique membership match (${code}); no entitlement change was made.`);
        return { ok: false, changed: false, reconciliationStatus: 'mapping_ambiguous', code };
    }

    const normalized = normalizeGrowthZoneMembership(selected, { env, currentPlan: row.plan || row.account_plan });
    if (!normalized.ok) {
        await recordState(db, accountId, { status: 'invalid_response', reference: reference.reference, contactId: reference.contactId, membershipId: membershipIdOf(selected), errorCode: normalized.code, now: attemptedAt });
        await audit(db, actor, 'GROWTHZONE_MAPPING_AMBIGUOUS', accountId, `GrowthZone membership could not be normalized (${normalized.code}); no entitlement change was made.`);
        return { ok: false, changed: false, reconciliationStatus: 'invalid_response', code: normalized.code };
    }

    const difference = {};
    if (row.status !== normalized.status) difference.status = { from: row.status || null, to: normalized.status };
    if ((row.plan || null) !== (normalized.plan || null)) difference.plan = { from: row.plan || null, to: normalized.plan || null };
    if (!sameNullable(toIso(row.effective_at), normalized.effectiveAt)) difference.effective_at = { from: toIso(row.effective_at), to: normalized.effectiveAt };
    if (!sameNullable(toIso(row.expires_at), normalized.expiresAt)) difference.expires_at = { from: toIso(row.expires_at), to: normalized.expiresAt };
    if (!sameNullable(toIso(row.grace_until), normalized.graceUntil)) difference.grace_until = { from: toIso(row.grace_until), to: normalized.graceUntil };
    const changed = Object.keys(difference).length > 0;

    await db.prepare(`
        UPDATE sneak_account_entitlements
        SET status = ?, plan = ?, effective_at = ?, expires_at = ?, grace_until = ?,
            last_verified_at = ?, updated_at = CASE WHEN ? THEN ? ELSE updated_at END
        WHERE account_id = ? AND source = 'growthzone'
    `).bind(
        normalized.status, normalized.plan || row.plan || row.account_plan,
        normalized.effectiveAt, normalized.expiresAt, normalized.graceUntil,
        attemptedAt, changed ? 1 : 0, attemptedAt, accountId
    ).run();

    const reconciliationStatus = changed ? 'entitlement_changed' : 'verified_no_change';
    await recordState(db, accountId, {
        status: reconciliationStatus,
        reference: reference.reference,
        contactId: reference.contactId,
        membershipId: normalized.membershipId,
        remoteStatus: normalized.remoteStatus,
        normalizedStatus: normalized.status,
        difference: changed ? JSON.stringify(difference) : null,
        snapshot: {
            membershipId: normalized.membershipId,
            membershipTypeId: normalized.membershipTypeId,
            membershipName: normalized.membershipName,
            status: normalized.remoteStatus,
            effectiveAt: normalized.effectiveAt,
            expiresAt: normalized.expiresAt,
            graceUntil: normalized.graceUntil
        },
        success: true,
        changed,
        now: attemptedAt
    });
    await audit(db, actor, changed ? 'GROWTHZONE_ENTITLEMENT_CHANGED' : 'GROWTHZONE_NO_CHANGE', accountId,
        changed ? `GrowthZone verification updated canonical entitlement fields: ${Object.keys(difference).join(', ')}.` : 'GrowthZone verification completed with no canonical entitlement change.');

    return { ok: true, changed, reconciliationStatus, normalizedStatus: normalized.status, difference };
}

export async function reconcileGrowthZoneBatch(db, env, { actor = 'system:growthzone', limit = 25, now = new Date(), fetchImpl = fetch } = {}) {
    const safeLimit = Math.max(1, Math.min(MAX_BATCH_SIZE, Number(limit) || 25));
    const rows = await db.prepare(`
        SELECT account_id FROM sneak_account_entitlements
        WHERE source = 'growthzone'
        ORDER BY COALESCE(last_verified_at, '1970-01-01') ASC, account_id ASC
        LIMIT ?
    `).bind(safeLimit).all();
    const results = [];
    for (const row of (rows.results || [])) {
        results.push({ accountId: row.account_id, ...(await reconcileGrowthZoneAccount(db, env, row.account_id, { actor, now, fetchImpl })) });
    }
    return {
        attempted: results.length,
        changed: results.filter(item => item.changed).length,
        succeeded: results.filter(item => item.ok).length,
        failed: results.filter(item => !item.ok).length,
        results
    };
}
