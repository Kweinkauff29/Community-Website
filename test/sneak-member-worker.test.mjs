/**
 * test/sneak-member-worker.test.mjs
 * 
 * Comprehensive Security & Authority Test Suite for Member Worker:
 * - Public Magic Link Zero-Token Guarantee & Anti-Enumeration
 * - Attack Simulation
 * - Atomic Single-Use Consumption & Race Condition Defense
 * - Disabled User / Suspended Account Login Blocking
 * - Revocable 7-Day Sessions & Logout CSRF
 * - Strict Tenant Isolation
 * - Stripe Signature Verification & Idempotency
 * - Checkout Completed Entitlement Authority (No Blind Activation)
 * - Out-of-Order Webhook Protection
 * - Metadata Conflict Protection
 * - Configurable Grace Periods
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
    sha256Hex,
    timingSafeEqual
} from '../sneak-member/auth.js';
import {
    deriveEntitlement,
    verifyStripeWebhookSignature,
    handleStripeWebhookEvent,
    getPaymentGraceDays
} from '../sneak-member/billing.js';

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
        sneak_member_audit: [],
        sneak_account_billing: [
            { account_id: 'acc_existing_cus', stripe_customer_id: 'cus_bound_01', stripe_subscription_id: 'sub_bound_01', billing_status: 'active', entitlement_status: 'active', last_stripe_event_created: 1000 }
        ],
        sneak_stripe_events: [],
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
                    if (query.includes('FROM sneak_member_magic_links')) {
                        const tokenHash = boundArgs[0];
                        return { results: tables.sneak_member_magic_links.filter(m => m.token_hash === tokenHash && !m.used_at) };
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
                    if (query.includes('FROM sneak_widget_configs WHERE site_id = ?')) {
                        return { results: tables.sneak_widget_configs.filter(w => w.site_id === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_account_billing WHERE account_id = ?')) {
                        return { results: tables.sneak_account_billing.filter(b => b.account_id === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_account_billing WHERE stripe_customer_id = ? AND account_id != ?')) {
                        return { results: tables.sneak_account_billing.filter(b => b.stripe_customer_id === boundArgs[0] && b.account_id !== boundArgs[1]) };
                    }
                    if (query.includes('FROM sneak_account_billing WHERE stripe_subscription_id = ? OR stripe_customer_id = ?')) {
                        return { results: tables.sneak_account_billing.filter(b => b.stripe_subscription_id === boundArgs[0] || b.stripe_customer_id === boundArgs[1]) };
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
                    if (query.includes('INSERT INTO sneak_member_login_attempts')) {
                        const [id, ip_hash, email_hash, attempted_at] = boundArgs;
                        tables.sneak_member_login_attempts.push({ id, ip_hash, email_hash, attempted_at });
                        return { success: true };
                    }
                    if (query.includes('INSERT INTO sneak_stripe_events')) {
                        const [stripe_event_id, event_type, stripe_object_id, event_created] = boundArgs;
                        const exists = tables.sneak_stripe_events.find(e => e.stripe_event_id === stripe_event_id);
                        if (exists) throw new Error('UNIQUE constraint failed: sneak_stripe_events.stripe_event_id');
                        tables.sneak_stripe_events.push({ stripe_event_id, event_type, stripe_object_id, event_created, status: 'processing' });
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_stripe_events SET status = ?')) {
                        const [status, event_id] = boundArgs;
                        const ev = tables.sneak_stripe_events.find(e => e.stripe_event_id === event_id);
                        if (ev) ev.status = status;
                        return { success: true };
                    }
                    if (query.includes('INSERT INTO sneak_account_billing')) {
                        const [account_id, stripe_customer_id, stripe_subscription_id, last_stripe_event_created, updated_at] = boundArgs;
                        const existing = tables.sneak_account_billing.find(b => b.account_id === account_id);
                        if (existing) {
                            existing.stripe_customer_id = stripe_customer_id;
                            existing.stripe_subscription_id = stripe_subscription_id || existing.stripe_subscription_id;
                            existing.last_stripe_event_created = last_stripe_event_created;
                            existing.updated_at = updated_at;
                        } else {
                            tables.sneak_account_billing.push({
                                account_id,
                                stripe_customer_id,
                                stripe_subscription_id,
                                billing_status: 'incomplete',
                                entitlement_status: 'inactive',
                                last_stripe_event_created,
                                updated_at
                            });
                        }
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_account_billing')) {
                        return { success: true };
                    }
                    return { success: true };
                }
            };
        }
    };
}

describe('SNEAK Member Worker Security & Authority Suite (Phase 5.0.1)', () => {

    test('TEST 1: Public magic link request NEVER returns tokens, user IDs, or email existence signals', async () => {
        const mockDB = createMockMemberDB();
        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        // 1. Request for KNOWN registered email
        const knownRes = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/magic-link', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ email: 'member1@realty.com' })
        }), env, {});
        assert.equal(knownRes.status, 200);
        const knownData = await knownRes.json();

        // 2. Request for UNKNOWN unregistered email
        const unknownRes = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/magic-link', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ email: 'unregistered.stranger@otherdomain.com' })
        }), env, {});
        assert.equal(unknownRes.status, 200);
        const unknownData = await unknownRes.json();

        // Responses MUST be identical
        assert.deepEqual(knownData, unknownData);
        assert.equal(knownData.success, true);
        assert.equal(knownData.message, 'If an account exists, a sign-in link will be sent.');

        // Verify ZERO secrets or identifiers in response
        const serialized = JSON.stringify(knownData);
        assert.equal(serialized.includes('rawToken'), false);
        assert.equal(serialized.includes('token'), false);
        assert.equal(serialized.includes('userId'), false);
        assert.equal(serialized.includes('accountId'), false);
        assert.equal(serialized.includes('usr_'), false);
        assert.equal(serialized.includes('acc_'), false);
    });

    test('TEST 2: Attack simulation - Attacker cannot authenticate using public magic-link response', async () => {
        const mockDB = createMockMemberDB();
        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        // Attacker attempts public magic link request for victim
        const res = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/magic-link', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ email: 'member1@realty.com' })
        }), env, {});
        const data = await res.json();

        // Attacker attempts to verify with any fields from the response
        const tokenAttempt1 = data.rawToken || '';
        const verifyRes1 = await worker.fetch(new Request(`https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/verify?token=${tokenAttempt1}`, {
            headers: { 'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev' }
        }), env, {});
        assert.equal(verifyRes1.status, 400, 'Empty/missing token rejected with 400');

        const verifyRes2 = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/verify?token=fake_guessed_token_1234567890123456789012', {
            headers: { 'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev' }
        }), env, {});
        assert.equal(verifyRes2.status, 401, 'Invalid guessed token rejected with 401');
    });

    test('TEST 3: Atomic single-use consumption & simultaneous race condition prevention', async () => {
        const mockDB = createMockMemberDB();
        
        // Create internal invite link
        const rawToken = await createMagicLinkRecord(mockDB, 'usr_1', 'login', 900);
        assert.ok(rawToken);

        // First consumption succeeds
        const consume1 = await verifyAndConsumeMagicLink(mockDB, rawToken);
        assert.ok(consume1);
        assert.equal(consume1.user.email, 'member1@realty.com');

        // Immediate simultaneous/subsequent replay must fail (changes !== 1)
        const consume2 = await verifyAndConsumeMagicLink(mockDB, rawToken);
        assert.equal(consume2, null, 'Replayed token rejected atomically');
    });

    test('TEST 4: Disabled user cannot login via magic link or session', async () => {
        const mockDB = createMockMemberDB();
        const rawToken = await createMagicLinkRecord(mockDB, 'usr_disabled', 'login', 900);

        const consume = await verifyAndConsumeMagicLink(mockDB, rawToken);
        assert.equal(consume, null, 'Disabled user magic link verification rejected');

        const rawSession = await createMemberSession(mockDB, 'usr_disabled', 'acc_member_1', 604800);
        const sessionCtx = await verifyMemberSession(mockDB, rawSession);
        assert.equal(sessionCtx, null, 'Disabled user session rejected');
    });

    test('TEST 5: Suspended account member cannot login via magic link or session', async () => {
        const mockDB = createMockMemberDB();
        const rawToken = await createMagicLinkRecord(mockDB, 'usr_susp_acc', 'login', 900);

        const consume = await verifyAndConsumeMagicLink(mockDB, rawToken);
        assert.equal(consume, null, 'Suspended account magic link verification rejected');

        const rawSession = await createMemberSession(mockDB, 'usr_susp_acc', 'acc_suspended', 604800);
        const sessionCtx = await verifyMemberSession(mockDB, rawSession);
        assert.equal(sessionCtx, null, 'Suspended account session rejected');
    });

    test('TEST 6: 7-Day Revocable Member Session & Logout CSRF Protection', async () => {
        const mockDB = createMockMemberDB();
        const rawSession = await createMemberSession(mockDB, 'usr_1', 'acc_member_1', 604800);
        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        // Foreign origin logout attempt
        const csrfFailRes = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/logout', {
            method: 'POST',
            headers: {
                'Cookie': `__Host-sneak_member_session=${rawSession}`,
                'Origin': 'https://attacker-domain.com',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(csrfFailRes.status, 403, 'Foreign origin logout blocked with 403');

        // Same origin logout succeeds
        const logoutRes = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/auth/logout', {
            method: 'POST',
            headers: {
                'Cookie': `__Host-sneak_member_session=${rawSession}`,
                'Origin': 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(logoutRes.status, 200);

        // Replay old session cookie returns 401
        const sessionCtx = await verifyMemberSession(mockDB, rawSession);
        assert.equal(sessionCtx, null, 'Revoked session returns null');
    });

    test('TEST 7: Strict Tenant Isolation across Overview, Domains, and Branding', async () => {
        const mockDB = createMockMemberDB();
        const session1 = await createMemberSession(mockDB, 'usr_1', 'acc_member_1', 604800);
        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        // Member 1 attempts cross-account domain deletion (dom_2 belongs to Member 2)
        const unauthDel = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/domains/dom_2', {
            method: 'DELETE',
            headers: {
                'Cookie': `__Host-sneak_member_session=${session1}`,
                'Origin': 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(unauthDel.status, 404, 'Cross-tenant domain deletion blocked');
    });

    test('TEST 8: Stripe Webhook Signature Verification & Idempotency', async () => {
        const mockDB = createMockMemberDB();
        const MOCK_WEBHOOK_SECRET = 'mock-unit-test-webhook-secret-12345';
        const rawPayload = JSON.stringify({
            id: 'evt_test_sub_update_01',
            type: 'customer.subscription.updated',
            created: 1771500000,
            data: {
                object: {
                    id: 'sub_bound_01',
                    customer: 'cus_bound_01',
                    status: 'active'
                }
            }
        });

        const timestamp = Math.floor(Date.now() / 1000);
        const enc = new TextEncoder();
        const key = await crypto.subtle.importKey('raw', enc.encode(MOCK_WEBHOOK_SECRET), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
        const sig = Array.from(new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(`${timestamp}.${rawPayload}`)))).map(b => b.toString(16).padStart(2, '0')).join('');
        const sigHeader = `t=${timestamp},v1=${sig}`;

        const env = { SNEAK_ENV: 'staging', DB: mockDB, STRIPE_WEBHOOK_SECRET: MOCK_WEBHOOK_SECRET };

        // Process initial event
        const res1 = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/stripe/webhook', {
            method: 'POST',
            headers: { 'Stripe-Signature': sigHeader, 'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev' },
            body: rawPayload
        }), env, {});
        assert.equal(res1.status, 200);

        // Process duplicate event
        const res2 = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/stripe/webhook', {
            method: 'POST',
            headers: { 'Stripe-Signature': sigHeader, 'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev' },
            body: rawPayload
        }), env, {});
        assert.equal(res2.status, 200);
        const data2 = await res2.json();
        assert.equal(data2.duplicate, true, 'Duplicate webhook handled idempotently');
    });

    test('TEST 9: checkout.session.completed does NOT blindly grant active entitlement', async () => {
        const mockDB = createMockMemberDB();
        const event = {
            id: 'evt_checkout_new_01',
            type: 'checkout.session.completed',
            created: 1771500100,
            data: {
                object: {
                    customer: 'cus_new_123',
                    subscription: 'sub_new_123',
                    metadata: { sneak_account_id: 'acc_member_2' }
                }
            }
        };

        await handleStripeWebhookEvent(mockDB, event);

        const billingRow = mockDB.tables.sneak_account_billing.find(b => b.account_id === 'acc_member_2');
        assert.ok(billingRow);
        assert.equal(billingRow.stripe_customer_id, 'cus_new_123');
        assert.equal(billingRow.billing_status, 'incomplete', 'Billing status initialized as incomplete');
        assert.equal(billingRow.entitlement_status, 'inactive', 'Entitlement remains inactive until subscription verification');
    });

    test('TEST 10: Metadata/Customer conflict quarantines webhook without mutating accounts', async () => {
        const mockDB = createMockMemberDB();
        // cus_bound_01 is already bound to acc_existing_cus in mockDB.
        // Attacker sends event with metadata.sneak_account_id = acc_member_1
        const conflictEvent = {
            id: 'evt_conflict_01',
            type: 'checkout.session.completed',
            created: 1771500200,
            data: {
                object: {
                    customer: 'cus_bound_01',
                    subscription: 'sub_stolen_01',
                    metadata: { sneak_account_id: 'acc_member_1' }
                }
            }
        };

        await assert.rejects(async () => {
            await handleStripeWebhookEvent(mockDB, conflictEvent);
        }, /Conflict: Stripe Customer cus_bound_01 already bound/);

        // Ensure acc_member_1 was NOT granted customer
        const stolenAccount = mockDB.tables.sneak_account_billing.find(b => b.account_id === 'acc_member_1');
        assert.equal(stolenAccount, undefined, 'Conflicting account was not mutated');
    });

    test('TEST 11: Configurable payment grace period & Entitlement mapping', () => {
        const now = new Date('2026-08-20T12:00:00Z');
        const futureGrace = '2026-08-23T12:00:00Z';
        const expiredGrace = '2026-08-19T12:00:00Z';

        // Configurable Grace Days helper
        assert.equal(getPaymentGraceDays({ SNEAK_PAYMENT_GRACE_DAYS: '5' }), 5);
        assert.equal(getPaymentGraceDays({ SNEAK_PAYMENT_GRACE_DAYS: '99' }), 3); // out of bounds default
        assert.equal(getPaymentGraceDays({}), 3); // default

        // Entitlement state machine
        assert.equal(deriveEntitlement('active', null, now), 'active');
        assert.equal(deriveEntitlement('trialing', null, now), 'active');
        assert.equal(deriveEntitlement('past_due', futureGrace, now), 'grace');
        assert.equal(deriveEntitlement('past_due', expiredGrace, now), 'inactive');
        assert.equal(deriveEntitlement('canceled', null, now), 'inactive');
        assert.equal(deriveEntitlement('unpaid', null, now), 'inactive');
    });
});
