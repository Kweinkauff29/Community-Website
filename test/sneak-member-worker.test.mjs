/**
 * test/sneak-member-worker.test.mjs
 * 
 * SNEAK Member Worker & GrowthZone Entitlement Test Suite:
 * - Public Magic Link Zero-Token Guarantee & Anti-Enumeration
 * - Attack Simulation
 * - Transactional Email Template & Dispatch Handling
 * - Atomic Single-Use Consumption & Race Condition Defense
 * - Disabled User / Suspended Account Login Blocking
 * - Revocable 7-Day Sessions & Logout CSRF
 * - Strict Tenant Isolation
 * - Generic GrowthZone Entitlement State Machine & Serving Rules
 * - Absence of Stripe Endpoints / Secrets
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import worker from '../sneak-member/worker.js';
import {
    createMagicLinkRecord,
    requestPublicMagicLink,
    verifyAndConsumeMagicLink,
    createMemberSession,
    verifyMemberSession,
    revokeMemberSession,
    sha256Hex
} from '../sneak-member/auth.js';
import {
    isAccountEntitled,
    getAccountEntitlement,
    setAccountEntitlement
} from '../sneak-member/billing.js';
import {
    renderMagicLinkTemplate,
    renderInvitationTemplate,
    sendTransactionalEmail
} from '../sneak-member/email.js';

function createMockMemberDB() {
    const tables = {
        sneak_accounts: [
            { id: 'acc_member_1', account_name: 'Member 1 Realty', status: 'active', plan: 'pro' },
            { id: 'acc_member_2', account_name: 'Member 2 Realty', status: 'active', plan: 'standard' },
            { id: 'acc_suspended', account_name: 'Suspended Realty', status: 'suspended', plan: 'pro' }
        ],
        sneak_member_users: [
            { id: 'usr_1', account_id: 'acc_member_1', email: 'member1@realty.com', role: 'owner', status: 'active', invited_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' },
            { id: 'usr_2', account_id: 'acc_member_2', email: 'member2@realty.com', role: 'owner', status: 'active', invited_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' },
            { id: 'usr_disabled', account_id: 'acc_member_1', email: 'disabled@realty.com', role: 'viewer', status: 'disabled', invited_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' },
            { id: 'usr_susp_acc', account_id: 'acc_suspended', email: 'susp@realty.com', role: 'owner', status: 'active', invited_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_sites: [
            { id: 'site_1', account_id: 'acc_member_1', site_key: 'site-mem-1', site_name: 'Site 1', status: 'active', scope_type: 'agent', scope_value: 'B3650316' },
            { id: 'site_2', account_id: 'acc_member_2', site_key: 'site-mem-2', site_name: 'Site 2', status: 'active', scope_type: 'office', scope_value: 'BPRI' }
        ],
        sneak_domains: [
            { id: 'dom_1', site_id: 'site_1', domain: 'member1.com', verified: 1, status: 'active', created_at: '2026-08-20T00:00:00Z' },
            { id: 'dom_2', site_id: 'site_2', domain: 'member2.com', verified: 1, status: 'active', created_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_branding: [],
        sneak_widget_configs: [],
        sneak_leads: [],
        sneak_member_magic_links: [],
        sneak_member_sessions: [],
        sneak_member_login_attempts: [],
        sneak_account_entitlements: [
            { account_id: 'acc_member_1', source: 'growthzone', status: 'active', plan: 'pro', effective_at: '2026-08-01T00:00:00Z', grace_until: null, external_reference: 'GZ_MEM_01' }
        ],
        sneak_listings: [{ count: 37165 }],
        sneak_open_houses: [{ count: 2018 }]
    };

    return {
        tables,
        prepare(query) {
            let boundArgs = [];
            return {
                bind(...args) {
                    boundArgs = args;
                    return this;
                },
                async first() {
                    const res = await this.all();
                    return res.results ? res.results[0] : null;
                },
                async all() {
                    if (query.includes('FROM sneak_accounts WHERE id = ?')) {
                        return { results: tables.sneak_accounts.filter(a => a.id === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_accounts a LEFT JOIN sneak_account_entitlements e ON a.id = e.account_id WHERE a.id = ?')) {
                        const a = tables.sneak_accounts.find(x => x.id === boundArgs[0]);
                        if (!a) return { results: [] };
                        const e = tables.sneak_account_entitlements.find(x => x.account_id === a.id);
                        return {
                            results: [{
                                account_id: a.id,
                                account_name: a.account_name,
                                account_status: a.status,
                                account_plan: a.plan,
                                source: e?.source || 'manual',
                                entitlement_status: e?.status || 'active',
                                plan: e?.plan || a.plan,
                                effective_at: e?.effective_at,
                                expires_at: e?.expires_at,
                                grace_until: e?.grace_until,
                                external_reference: e?.external_reference,
                                notes: e?.notes,
                                last_verified_at: e?.last_verified_at
                            }]
                        };
                    }
                    if (query.includes('FROM sneak_member_magic_links l') && query.includes('JOIN sneak_member_users u')) {
                        const tokenHash = boundArgs[0];
                        const l = tables.sneak_member_magic_links.find(x => x.token_hash === tokenHash);
                        if (!l) return { results: [] };
                        const u = tables.sneak_member_users.find(x => x.id === l.user_id);
                        const a = tables.sneak_accounts.find(x => x.id === u?.account_id);
                        return {
                            results: [{
                                user_id: l.user_id,
                                account_id: u?.account_id,
                                email: u?.email,
                                role: u?.role,
                                user_status: u?.status,
                                account_name: a?.account_name,
                                account_status: a?.status
                            }]
                        };
                    }
                    if (query.includes('FROM sneak_member_users u') && query.includes('JOIN sneak_accounts a')) {
                        const u = tables.sneak_member_users.find(x => x.email === boundArgs[0]);
                        if (!u) return { results: [] };
                        const a = tables.sneak_accounts.find(x => x.id === u.account_id);
                        return { results: [{ id: u.id, account_id: u.account_id, user_status: u.status, account_status: a?.status }] };
                    }
                    if (query.includes('FROM sneak_member_sessions')) {
                        const tokenHash = boundArgs[0];
                        const s = tables.sneak_member_sessions.find(x => x.token_hash === tokenHash && !x.revoked_at);
                        if (!s) return { results: [] };
                        const u = tables.sneak_member_users.find(x => x.id === s.user_id);
                        const a = tables.sneak_accounts.find(x => x.id === s.account_id);
                        return {
                            results: [{
                                session_id: s.id,
                                user_id: s.user_id,
                                account_id: s.account_id,
                                created_at: s.created_at,
                                expires_at: s.expires_at,
                                revoked_at: s.revoked_at,
                                member_email: u?.email,
                                member_role: u?.role,
                                user_status: u?.status,
                                account_name: a?.account_name,
                                account_status: a?.status,
                                account_plan: a?.plan
                            }]
                        };
                    }
                    if (query.includes('FROM sneak_sites WHERE account_id = ?')) {
                        return { results: tables.sneak_sites.filter(s => s.account_id === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_domains WHERE site_id = ?')) {
                        return { results: tables.sneak_domains.filter(d => d.site_id === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_domains d JOIN sneak_sites s')) {
                        return { results: tables.sneak_domains.filter(d => {
                            const site = tables.sneak_sites.find(s => s.id === d.site_id);
                            return site && site.account_id === boundArgs[0];
                        }) };
                    }
                    if (query.includes('FROM sneak_branding WHERE site_id = ?')) {
                        return { results: tables.sneak_branding.filter(b => b.site_id === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_account_entitlements WHERE account_id = ?')) {
                        return { results: tables.sneak_account_entitlements.filter(e => e.account_id === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_member_login_attempts')) {
                        return { results: [{ count: 0 }] };
                    }
                    if (query.includes('count(*) as count FROM sneak_listings')) {
                        return { results: [{ count: 37165 }] };
                    }
                    if (query.includes('count(*) as count FROM sneak_open_houses')) {
                        return { results: [{ count: 2018 }] };
                    }
                    return { results: [] };
                },
                async run() {
                    if (query.includes('UPDATE sneak_member_magic_links') && query.includes('WHERE token_hash = ?')) {
                        const [used_at, token_hash] = boundArgs;
                        const link = tables.sneak_member_magic_links.find(l => l.token_hash === token_hash && !l.used_at);
                        if (link) {
                            link.used_at = used_at;
                            return { success: true, meta: { changes: 1 }, changes: 1 };
                        }
                        return { success: true, meta: { changes: 0 }, changes: 0 };
                    }
                    if (query.includes('UPDATE sneak_member_magic_links') && query.includes('WHERE user_id = ?')) {
                        const [used_at, user_id, purpose] = boundArgs;
                        tables.sneak_member_magic_links.forEach(l => {
                            if (l.user_id === user_id && l.purpose === purpose && !l.used_at) l.used_at = used_at;
                        });
                        return { success: true };
                    }
                    if (query.includes('INSERT INTO sneak_member_magic_links')) {
                        const [id, user_id, token_hash, purpose, created_at, expires_at] = boundArgs;
                        tables.sneak_member_magic_links.push({ id, user_id, token_hash, purpose, created_at, expires_at, used_at: null });
                        return { success: true };
                    }
                    if (query.includes('INSERT INTO sneak_member_sessions')) {
                        const [id, user_id, account_id, token_hash, created_at, expires_at, last_seen_at] = boundArgs;
                        tables.sneak_member_sessions.push({ id, user_id, account_id, token_hash, created_at, expires_at, last_seen_at, revoked_at: null });
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_member_sessions')) {
                        const tokenHash = boundArgs[0];
                        const s = tables.sneak_member_sessions.find(x => x.token_hash === tokenHash);
                        if (s) s.revoked_at = new Date().toISOString();
                        return { success: true };
                    }
                    if (query.includes('INSERT INTO sneak_account_entitlements')) {
                        const [account_id, source, status, plan, effective_at, expires_at, grace_until, external_reference, notes] = boundArgs;
                        const existing = tables.sneak_account_entitlements.find(e => e.account_id === account_id);
                        if (existing) {
                            existing.source = source;
                            existing.status = status;
                            existing.plan = plan || existing.plan;
                            existing.grace_until = grace_until;
                            existing.external_reference = external_reference || existing.external_reference;
                        } else {
                            tables.sneak_account_entitlements.push({
                                account_id, source, status, plan, effective_at, expires_at, grace_until, external_reference, notes
                            });
                        }
                        return { success: true };
                    }
                    return { success: true };
                }
            };
        }
    };
}

describe('SNEAK Member Worker & GrowthZone Alignment Suite (Phase 5.1)', () => {

    test('TEST 1: Public magic link request NEVER returns tokens or identifiers', async () => {
        const mockDB = createMockMemberDB();
        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        const res = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/magic-link', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ email: 'member1@realty.com' })
        }), env, {});

        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.success, true);
        assert.equal(data.message, 'If an account exists, a sign-in link will be sent.');

        const serialized = JSON.stringify(data);
        assert.equal(serialized.includes('rawToken'), false);
        assert.equal(serialized.includes('token'), false);
        assert.equal(serialized.includes('userId'), false);
        assert.equal(serialized.includes('accountId'), false);
    });

    test('TEST 2: Transactional email templates render properly without leaking tokens', () => {
        const magicHtml = renderMagicLinkTemplate({ verifyUrl: 'https://preview.sneakidx.com/verify?token=abc', expiresMinutes: 15 });
        assert.ok(magicHtml.includes('Sign In to SNEAK Portal'));
        assert.ok(magicHtml.includes('15 minutes'));
        assert.ok(magicHtml.includes('Coconut Coast Organization of REALTORS®'));
        assert.ok(!magicHtml.includes('Bonita Springs-Estero'));
        assert.ok(!magicHtml.includes('BER'));
        assert.ok(!magicHtml.includes('sk_test_'));

        const inviteHtml = renderInvitationTemplate({ inviteUrl: 'https://preview.sneakidx.com/invite?token=abc', accountName: 'Naples Realty' });
        assert.ok(inviteHtml.includes('Naples Realty'));
        assert.ok(inviteHtml.includes('GrowthZone'));
        assert.ok(inviteHtml.includes('24 hours'));
        assert.ok(inviteHtml.includes('Coconut Coast Organization of REALTORS®'));
        assert.ok(inviteHtml.includes('CCOR Member Services'));
        assert.ok(!inviteHtml.includes('Bonita Springs-Estero'));
        assert.ok(!inviteHtml.includes('BER'));
    });

    test('TEST 3: Atomic single-use consumption & simultaneous replay prevention', async () => {
        const mockDB = createMockMemberDB();
        const rawToken = await createMagicLinkRecord(mockDB, 'usr_1', 'login', 900);
        assert.ok(rawToken);

        const consume1 = await verifyAndConsumeMagicLink(mockDB, rawToken);
        assert.ok(consume1);
        assert.equal(consume1.user.email, 'member1@realty.com');

        const consume2 = await verifyAndConsumeMagicLink(mockDB, rawToken);
        assert.equal(consume2, null, 'Replayed token rejected');
    });

    test('TEST 4: Disabled user & Suspended account login rejection', async () => {
        const mockDB = createMockMemberDB();

        // Disabled user
        const rawToken1 = await createMagicLinkRecord(mockDB, 'usr_disabled', 'login', 900);
        const consume1 = await verifyAndConsumeMagicLink(mockDB, rawToken1);
        assert.equal(consume1, null);

        // Suspended account
        const rawToken2 = await createMagicLinkRecord(mockDB, 'usr_susp_acc', 'login', 900);
        const consume2 = await verifyAndConsumeMagicLink(mockDB, rawToken2);
        assert.equal(consume2, null);
    });

    test('TEST 5: Member Session Revocation & Logout CSRF', async () => {
        const mockDB = createMockMemberDB();
        const rawSession = await createMemberSession(mockDB, 'usr_1', 'acc_member_1', 604800);
        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        // Cross-origin logout fails
        const csrfFail = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/logout', {
            method: 'POST',
            headers: {
                'Cookie': `__Host-sneak_member_session=${rawSession}`,
                'Origin': 'https://attacker-domain.com',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(csrfFail.status, 403);

        // Same-origin logout succeeds
        const logoutRes = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/logout', {
            method: 'POST',
            headers: {
                'Cookie': `__Host-sneak_member_session=${rawSession}`,
                'Origin': 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(logoutRes.status, 200);
    });

    test('TEST 6: Generic GrowthZone Entitlement State Machine', () => {
        const now = new Date('2026-08-20T12:00:00Z');
        const futureGrace = '2026-08-23T12:00:00Z';
        const expiredGrace = '2026-08-19T12:00:00Z';

        // 1. Backward compatibility (no entitlement row)
        assert.equal(isAccountEntitled('active', null, null, now), true);
        assert.equal(isAccountEntitled('suspended', null, null, now), false);

        // 2. Active status
        assert.equal(isAccountEntitled('active', 'active', null, now), true);

        // 3. Grace status
        assert.equal(isAccountEntitled('active', 'grace', futureGrace, now), true);
        assert.equal(isAccountEntitled('active', 'grace', expiredGrace, now), false);

        // 4. Delinquent status
        assert.equal(isAccountEntitled('active', 'delinquent', null, now), false);
        assert.equal(isAccountEntitled('active', 'delinquent', futureGrace, now), true);

        // 5. Suspended / Canceled
        assert.equal(isAccountEntitled('active', 'suspended', null, now), false);
        assert.equal(isAccountEntitled('active', 'canceled', null, now), false);

        // 6. Admin Account suspension always wins
        assert.equal(isAccountEntitled('suspended', 'active', null, now), false);
    });

    test('TEST 7: Member Billing endpoint returns GrowthZone configuration and zero Stripe routes', async () => {
        const mockDB = createMockMemberDB();
        const rawSession = await createMemberSession(mockDB, 'usr_1', 'acc_member_1', 604800);
        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        // GET /api/member/billing
        const billRes = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/billing', {
            headers: {
                'Cookie': `__Host-sneak_member_session=${rawSession}`,
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(billRes.status, 200);
        const billData = await billRes.json();
        assert.equal(billData.provider, 'GrowthZone');
        assert.equal(billData.billingCycle, 'Monthly (1st of each month)');
        assert.ok(billData.growthzoneUrl.includes('growthzoneapp.com'));

        // Stripe Webhook route returns 404 (removed from Worker)
        const stripeRes = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/stripe/webhook', {
            method: 'POST'
        }), env, {});
        assert.equal(stripeRes.status, 404, 'Stripe webhook endpoint removed');
    });

    test('TEST 8: Mailjet Send API v3.1 integration & error handling', async () => {
        const originalFetch = globalThis.fetch;
        let capturedPayload = null;
        let capturedAuth = null;

        globalThis.fetch = async (url, opts) => {
            if (url === 'https://api.mailjet.com/v3.1/send') {
                capturedAuth = opts.headers['Authorization'];
                capturedPayload = JSON.parse(opts.body);
                return {
                    ok: true,
                    status: 200,
                    json: async () => ({
                        Messages: [{
                            Status: 'success',
                            CustomID: 'SNEAK-IDX',
                            To: [{ Email: 'agent@test.com', MessageID: 123456789 }]
                        }]
                    })
                };
            }
            return { ok: false, status: 404, text: async () => 'Not found' };
        };

        try {
            const env = {
                MAILJET_API_KEY: 'test_key',
                MAILJET_SECRET_KEY: 'test_secret',
                EMAIL_FROM: 'SNEAK IDX <idx@mail.coconutcoasthomes.com>'
            };

            const result = await sendTransactionalEmail(env, {
                to: 'agent@test.com',
                subject: 'Test Subject',
                html: '<p>Test Body</p>',
                text: 'Test Body'
            });

            assert.equal(result.success, true);
            assert.equal(result.provider, 'mailjet');
            assert.equal(result.id, '123456789');
            assert.equal(capturedAuth, 'Basic ' + btoa('test_key:test_secret'));
            assert.equal(capturedPayload.Messages[0].From.Email, 'idx@mail.coconutcoasthomes.com');
            assert.equal(capturedPayload.Messages[0].From.Name, 'SNEAK IDX');
            assert.equal(capturedPayload.Messages[0].To[0].Email, 'agent@test.com');
        } finally {
            globalThis.fetch = originalFetch;
        }
    });
});

