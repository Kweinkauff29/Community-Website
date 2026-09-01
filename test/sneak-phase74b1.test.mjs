import test, { describe } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

import { sendTransactionalEmail as sendSharedEmail } from '../sneak-shared/email-provider.js';
import { sendTransactionalEmail as sendMemberEmail } from '../sneak-member/email.js';
import { sendConsumerTransactionalEmail } from '../sneak-consumer/email.js';
import { handleRecordLaunchCheck } from '../sneak-admin/api.js';
import {
    fetchGrowthZoneMemberships,
    normalizeGrowthZoneMembership,
    parseGrowthZoneReference,
    reconcileGrowthZoneAccount,
    reconcileGrowthZoneBatch
} from '../sneak-admin/growthzone.js';

function mailjetSuccess(id = 12345) {
    return new Response(JSON.stringify({
        Messages: [{ Status: 'success', To: [{ MessageID: id, MessageUUID: `uuid-${id}` }] }]
    }), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

function createGrowthZoneDb({ source = 'growthzone', status = 'suspended', reference = 'person:100:membership:200' } = {}) {
    const tables = {
        account: { id: 'acc_1', account_name: 'QA Account', plan: 'standard' },
        entitlement: {
            account_id: 'acc_1', source, status, plan: 'standard', effective_at: null,
            expires_at: null, grace_until: null, external_reference: reference,
            last_verified_at: null, updated_at: '2026-09-01T00:00:00.000Z'
        },
        reconciliation: null,
        audits: []
    };

    return {
        tables,
        prepare(sql) {
            const normalized = sql.replace(/\s+/g, ' ').trim();
            let params = [];
            const statement = {
                bind(...values) { params = values; return statement; },
                async first() {
                    if (normalized.includes('FROM sneak_accounts a') && normalized.includes('LEFT JOIN sneak_account_entitlements')) {
                        if (params[0] !== tables.account.id) return null;
                        return { account_id: tables.account.id, account_plan: tables.account.plan, ...tables.entitlement };
                    }
                    if (normalized.includes('FROM sneak_growthzone_reconciliation')) return tables.reconciliation;
                    if (normalized.includes('FROM sneak_account_entitlements')) return tables.entitlement;
                    if (normalized.includes('FROM sneak_accounts')) return params[0] === tables.account.id ? tables.account : null;
                    return null;
                },
                async all() {
                    if (normalized.includes('FROM sneak_account_entitlements') && normalized.includes("source = 'growthzone'")) {
                        return { results: tables.entitlement.source === 'growthzone' ? [{ account_id: tables.account.id }] : [] };
                    }
                    return { results: [] };
                },
                async run() {
                    if (normalized.startsWith('INSERT INTO sneak_growthzone_reconciliation')) {
                        const [accountId, reconciliationStatus, externalReference, contactId, membershipId,
                            remoteStatus, normalizedStatus, difference, errorCode, snapshotJson,
                            lastAttemptAt, lastSuccessAt, lastChangedAt] = params;
                        const previous = tables.reconciliation || {};
                        tables.reconciliation = {
                            account_id: accountId,
                            status: reconciliationStatus,
                            external_reference: externalReference,
                            remote_contact_id: contactId,
                            remote_membership_id: membershipId,
                            remote_status: remoteStatus,
                            normalized_status: normalizedStatus,
                            difference,
                            error_code: errorCode,
                            snapshot_json: snapshotJson,
                            last_attempt_at: lastAttemptAt,
                            last_success_at: lastSuccessAt || previous.last_success_at || null,
                            last_changed_at: lastChangedAt || previous.last_changed_at || null
                        };
                    } else if (normalized.startsWith('INSERT INTO sneak_admin_audit')) {
                        tables.audits.push({ id: params[0], actor: params[1], action: params[2], account_id: params[3], summary: params[4] });
                    } else if (normalized.startsWith('UPDATE sneak_account_entitlements')) {
                        const [nextStatus, plan, effectiveAt, expiresAt, graceUntil, verifiedAt, changed, updatedAt, accountId] = params;
                        if (accountId === tables.account.id && tables.entitlement.source === 'growthzone') {
                            Object.assign(tables.entitlement, {
                                status: nextStatus,
                                plan,
                                effective_at: effectiveAt,
                                expires_at: expiresAt,
                                grace_until: graceUntil,
                                last_verified_at: verifiedAt,
                                updated_at: changed ? updatedAt : tables.entitlement.updated_at
                            });
                        }
                    }
                    return { success: true, meta: { changes: 1 } };
                }
            };
            return statement;
        }
    };
}

const configuredEnv = {
    GROWTHZONE_BASE_URL: 'https://tenant.growthzoneapp.com',
    GROWTHZONE_API_KEY: 'test-only-api-key'
};

const activeMembership = {
    MembershipId: 200,
    MembershipStatusTypeId: 2,
    Status: 'Active',
    MembershipTypeId: 77,
    Name: 'REALTOR Membership',
    StartDate: '2026-01-01T00:00:00Z',
    ExpirationDate: '2026-12-31T23:59:59Z',
    IsInactive: false,
    IsInGracePeriod: false
};

describe('Phase 7.4B1 transactional email and GrowthZone reconciliation', () => {
    test('shared Mailjet adapter accepts only an explicit provider success with message identity', async () => {
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async () => mailjetSuccess(9988);
        try {
            const result = await sendSharedEmail({ MAILJET_API_KEY: 'key', MAILJET_SECRET_KEY: 'secret' }, {
                to: 'qa@example.com', subject: 'QA', html: '<p>QA</p>', text: 'QA'
            });
            assert.deepEqual({ success: result.success, status: result.status, provider: result.provider, id: result.id },
                { success: true, status: 'sent', provider: 'mailjet', id: '9988' });
        } finally { globalThis.fetch = originalFetch; }
    });

    test('shared provider rejects a Mailjet message-level error even when HTTP is 2xx', async () => {
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async () => new Response(JSON.stringify({
            Messages: [{ Status: 'error', Errors: [{ ErrorCode: 'send-0003', StatusCode: 400 }] }]
        }), { status: 200 });
        try {
            const result = await sendSharedEmail({ MAILJET_API_KEY: 'key', MAILJET_SECRET_KEY: 'secret' }, {
                to: 'qa@example.com', subject: 'QA', html: '<p>QA</p>', text: 'QA'
            });
            assert.equal(result.success, false);
            assert.equal(result.status, 'failed');
            assert.equal(result.retryable, false);
            assert.equal(result.errorCode, 'send-0003');
        } finally { globalThis.fetch = originalFetch; }
    });

    test('shared provider distinguishes retryable transport responses and fails closed when unconfigured', async () => {
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async () => new Response(JSON.stringify({ ErrorCode: 'rate-limit' }), { status: 429 });
        try {
            const retry = await sendSharedEmail({ MAILJET_API_KEY: 'key', MAILJET_SECRET_KEY: 'secret' }, { to: 'qa@example.com', subject: 'QA', html: 'QA', text: 'QA' });
            assert.equal(retry.success, false);
            assert.equal(retry.retryable, true);
            const missing = await sendSharedEmail({}, { to: 'qa@example.com', subject: 'QA', html: 'QA', text: 'QA' });
            assert.equal(missing.status, 'provider_unconfigured');
            assert.equal(missing.success, false);
        } finally { globalThis.fetch = originalFetch; }
    });

    test('Member and Consumer wrappers both route through the shared real-provider contract', async () => {
        const originalFetch = globalThis.fetch;
        const customIds = [];
        globalThis.fetch = async (_url, options) => {
            customIds.push(JSON.parse(options.body).Messages[0].CustomID);
            return mailjetSuccess(customIds.length);
        };
        try {
            const env = { MAILJET_API_KEY: 'key', MAILJET_SECRET_KEY: 'secret' };
            const member = await sendMemberEmail(env, { to: 'member@example.com', subject: 'Member', html: 'Member', text: 'Member' });
            const consumer = await sendConsumerTransactionalEmail(env, { to: 'consumer@example.com', subject: 'Consumer', html: 'Consumer', text: 'Consumer' });
            assert.equal(member.status, 'sent');
            assert.equal(consumer.status, 'sent');
            assert.deepEqual(customIds, ['SNEAK-IDX-MEMBER', 'SNEAK-CONSUMER-AUTH']);
        } finally { globalThis.fetch = originalFetch; }
    });

    test('Member and Consumer runtime adapters contain no simulated-success delivery path', () => {
        for (const file of ['sneak-member/email.js', 'sneak-consumer/email.js']) {
            const source = fs.readFileSync(file, 'utf8');
            assert.doesNotMatch(source, /provider:\s*['"]simulated['"]/);
            assert.match(source, /sneak-shared\/email-provider\.js/);
        }
    });

    test('readiness evidence cannot pass without controlled inbox or real GrowthZone sources', async () => {
        const noDb = { prepare() { throw new Error('should reject before database access'); } };
        for (const check_key of ['member_magic_link_e2e', 'consumer_magic_link_e2e', 'alerts_asap_e2e', 'alerts_daily_e2e', 'alerts_unsubscribe_e2e']) {
            const response = await handleRecordLaunchCheck(noDb, { check_key, status: 'pass', source: 'system' });
            assert.equal(response.status, 400);
        }
        const growthZone = await handleRecordLaunchCheck(noDb, {
            check_key: 'growthzone_reconciliation_e2e', status: 'pass', source: 'system'
        });
        assert.equal(growthZone.status, 400);
    });

    test('explicit GrowthZone references parse durable contact and membership identifiers only', () => {
        assert.deepEqual(parseGrowthZoneReference('person:123:membership:456'), {
            reference: 'person:123:membership:456', contactType: 'person', contactId: '123', membershipId: '456'
        });
        assert.deepEqual(parseGrowthZoneReference('org:987'), {
            reference: 'org:987', contactType: 'org', contactId: '987', membershipId: null
        });
        assert.equal(parseGrowthZoneReference('Jane Smith'), null);
        assert.equal(parseGrowthZoneReference('email:member@example.com'), null);
    });

    test('official GrowthZone membership states normalize without inferring delinquency from balance', () => {
        assert.equal(normalizeGrowthZoneMembership(activeMembership).status, 'active');
        assert.equal(normalizeGrowthZoneMembership({ ...activeMembership, MembershipStatusTypeId: 9, Status: 'Suspended' }).status, 'suspended');
        assert.equal(normalizeGrowthZoneMembership({ ...activeMembership, MembershipStatusTypeId: 6, Status: 'Expired' }).status, 'canceled');
        assert.equal(normalizeGrowthZoneMembership({ ...activeMembership, IsInGracePeriod: true }).status, 'grace');
        assert.equal(normalizeGrowthZoneMembership({ ...activeMembership, Balance: 500 }).status, 'active');
        assert.equal(normalizeGrowthZoneMembership({ ...activeMembership, MembershipStatusTypeId: 88, Status: 'Unknown' }).ok, false);
    });

    test('GrowthZone fetch uses server-side ApiKey authentication and a bounded official contact endpoint', async () => {
        let observed = null;
        const result = await fetchGrowthZoneMemberships(configuredEnv, parseGrowthZoneReference('person:100:membership:200'), async (url, options) => {
            observed = { url, authorization: options.headers.Authorization };
            return new Response(JSON.stringify([activeMembership]), { status: 200 });
        });
        assert.equal(result.ok, true);
        assert.equal(observed.url, 'https://tenant.growthzoneapp.com/api/contacts/person/100/memberships');
        assert.equal(observed.authorization, 'ApiKey test-only-api-key');
    });

    test('actual returned status transition updates the canonical entitlement and emits a sanitized audit', async () => {
        const db = createGrowthZoneDb();
        const result = await reconcileGrowthZoneAccount(db, configuredEnv, 'acc_1', {
            actor: 'qa-admin',
            now: new Date('2026-09-01T12:00:00Z'),
            fetchImpl: async () => new Response(JSON.stringify([activeMembership]), { status: 200 })
        });
        assert.equal(result.ok, true);
        assert.equal(result.changed, true);
        assert.equal(db.tables.entitlement.status, 'active');
        assert.equal(db.tables.entitlement.last_verified_at, '2026-09-01T12:00:00.000Z');
        assert.equal(db.tables.reconciliation.status, 'entitlement_changed');
        assert.equal(db.tables.audits.at(-1).action, 'GROWTHZONE_ENTITLEMENT_CHANGED');
        assert.doesNotMatch(db.tables.audits.at(-1).summary, /test-only-api-key/);
    });

    test('reconciliation is idempotent after the canonical state matches', async () => {
        const db = createGrowthZoneDb({ status: 'active' });
        db.tables.entitlement.effective_at = '2026-01-01T00:00:00.000Z';
        db.tables.entitlement.expires_at = '2026-12-31T23:59:59.000Z';
        const result = await reconcileGrowthZoneAccount(db, configuredEnv, 'acc_1', {
            now: new Date('2026-09-01T12:00:00Z'),
            fetchImpl: async () => new Response(JSON.stringify([activeMembership]), { status: 200 })
        });
        assert.equal(result.changed, false);
        assert.equal(db.tables.reconciliation.status, 'verified_no_change');
        assert.equal(db.tables.audits.at(-1).action, 'GROWTHZONE_NO_CHANGE');
    });

    test('ambiguous identity fails closed without modifying entitlement', async () => {
        const db = createGrowthZoneDb({ reference: 'person:100', status: 'active' });
        const result = await reconcileGrowthZoneAccount(db, configuredEnv, 'acc_1', {
            fetchImpl: async () => new Response(JSON.stringify([activeMembership, { ...activeMembership, MembershipId: 201 }]), { status: 200 })
        });
        assert.equal(result.ok, false);
        assert.equal(result.reconciliationStatus, 'mapping_ambiguous');
        assert.equal(db.tables.entitlement.status, 'active');
        assert.equal(db.tables.entitlement.last_verified_at, null);
    });

    test('manual source is never overwritten and does not call GrowthZone', async () => {
        const db = createGrowthZoneDb({ source: 'manual', status: 'active' });
        let called = false;
        const result = await reconcileGrowthZoneAccount(db, configuredEnv, 'acc_1', { fetchImpl: async () => { called = true; return mailjetSuccess(); } });
        assert.equal(result.reconciliationStatus, 'manual_override');
        assert.equal(called, false);
        assert.equal(db.tables.entitlement.status, 'active');
    });

    test('API outage and missing configuration preserve an otherwise-active entitlement and last_verified_at', async () => {
        for (const [env, response] of [
            [configuredEnv, async () => new Response('unavailable', { status: 503 })],
            [{}, async () => { throw new Error('must not call'); }]
        ]) {
            const db = createGrowthZoneDb({ status: 'active' });
            db.tables.entitlement.last_verified_at = '2026-08-31T12:00:00.000Z';
            const result = await reconcileGrowthZoneAccount(db, env, 'acc_1', { fetchImpl: response });
            assert.equal(result.ok, false);
            assert.equal(db.tables.entitlement.status, 'active');
            assert.equal(db.tables.entitlement.last_verified_at, '2026-08-31T12:00:00.000Z');
        }
    });

    test('bounded bulk reconciliation processes only canonical growthzone-source accounts', async () => {
        const db = createGrowthZoneDb({ status: 'active' });
        db.tables.entitlement.effective_at = '2026-01-01T00:00:00.000Z';
        db.tables.entitlement.expires_at = '2026-12-31T23:59:59.000Z';
        const result = await reconcileGrowthZoneBatch(db, configuredEnv, {
            limit: 500,
            fetchImpl: async () => new Response(JSON.stringify([activeMembership]), { status: 200 })
        });
        assert.equal(result.attempted, 1);
        assert.equal(result.succeeded, 1);
        assert.equal(result.changed, 0);
    });

    test('public serving code remains dependent only on canonical entitlement state, never GrowthZone', () => {
        const serving = fs.readFileSync('SneakIDXWorker.js', 'utf8');
        const authority = fs.readFileSync('sneak-shared/entitlement.js', 'utf8');
        assert.doesNotMatch(serving, /growthzoneapp|GROWTHZONE_API_KEY|api\/contacts\/.*memberships/i);
        assert.doesNotMatch(authority, /growthzoneapp|GROWTHZONE_API_KEY|fetch\s*\(/i);
        assert.match(authority, /sneak_account_entitlements is the only MVP subscription authority/i);
    });
});
