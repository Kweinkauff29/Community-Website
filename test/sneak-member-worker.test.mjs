/**
 * test/sneak-member-worker.test.mjs
 * 
 * Unit Test Suite for Member Worker: Passwordless Auth, 7-Day Revocable Sessions,
 * Strict Tenant Isolation, Self-Service, and Stripe Billing Webhooks / Entitlements.
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import worker from '../sneak-member/worker.js';
import {
    createMagicLink,
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
    handleStripeWebhookEvent
} from '../sneak-member/billing.js';

function createMockMemberDB() {
    const tables = {
        sneak_accounts: [
            { id: 'acc_member_1', account_name: 'Member 1 Realty', status: 'active', plan: 'pro' },
            { id: 'acc_member_2', account_name: 'Member 2 Realty', status: 'active', plan: 'standard' }
        ],
        sneak_member_users: [
            { id: 'usr_1', account_id: 'acc_member_1', email: 'member1@realty.com', role: 'owner', status: 'active', invited_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' },
            { id: 'usr_2', account_id: 'acc_member_2', email: 'member2@realty.com', role: 'owner', status: 'active', invited_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' }
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
        sneak_member_audit: [],
        sneak_account_billing: [],
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
                    if (query.includes('FROM sneak_member_users WHERE email = ?')) {
                        return { results: tables.sneak_member_users.filter(u => u.email === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_member_users WHERE id = ?')) {
                        return { results: tables.sneak_member_users.filter(u => u.id === boundArgs[0]) };
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
                    if (query.includes('FROM sneak_account_billing WHERE stripe_subscription_id = ? OR stripe_customer_id = ?')) {
                        return { results: tables.sneak_account_billing.filter(b => b.stripe_subscription_id === boundArgs[0] || b.stripe_customer_id === boundArgs[1]) };
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
                    if (query.includes('INSERT INTO sneak_member_magic_links')) {
                        const [id, user_id, token_hash, purpose, created_at, expires_at] = boundArgs;
                        tables.sneak_member_magic_links.push({ id, user_id, token_hash, purpose, created_at, expires_at, used_at: null });
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_member_magic_links')) {
                        const [used_at, id] = boundArgs;
                        const link = tables.sneak_member_magic_links.find(l => l.id === id);
                        if (link) link.used_at = used_at;
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
                    if (query.includes('INSERT INTO sneak_domains')) {
                        const [id, site_id, domain] = boundArgs;
                        tables.sneak_domains.push({ id, site_id, domain, verified: 0, status: 'active', created_at: new Date().toISOString() });
                        return { success: true };
                    }
                    if (query.includes('DELETE FROM sneak_domains WHERE id = ?')) {
                        const id = boundArgs[0];
                        const idx = tables.sneak_domains.findIndex(d => d.id === id);
                        if (idx !== -1) tables.sneak_domains.splice(idx, 1);
                        return { success: true };
                    }
                    if (query.includes('INSERT OR REPLACE INTO sneak_branding')) {
                        const [site_id, display_name, brokerage, logo_url, agent_photo_url, primary_color, secondary_color, phone, email, website_url] = boundArgs;
                        const existing = tables.sneak_branding.find(b => b.site_id === site_id);
                        if (existing) {
                            Object.assign(existing, { display_name, brokerage, logo_url, agent_photo_url, primary_color, secondary_color, phone, email, website_url });
                        } else {
                            tables.sneak_branding.push({ site_id, display_name, brokerage, logo_url, agent_photo_url, primary_color, secondary_color, phone, email, website_url });
                        }
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
                        tables.sneak_account_billing.push({
                            account_id,
                            stripe_customer_id,
                            stripe_subscription_id,
                            billing_status: 'active',
                            entitlement_status: 'active',
                            last_stripe_event_created,
                            updated_at
                        });
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

describe('SNEAK Member Worker & Passwordless Auth Suite', () => {

    test('TEST 1: Passwordless Magic Link generation & single-use consumption', async () => {
        const mockDB = createMockMemberDB();
        
        // 1. Create magic link
        const mlResult = await createMagicLink(mockDB, 'member1@realty.com', 'login', 900);
        assert.equal(mlResult.success, true);
        assert.ok(mlResult.rawToken);
        assert.equal(mlResult.rawToken.length, 64); // 32 bytes hex

        // Verify raw token is NOT in D1, only SHA-256 hash
        const storedHash = mockDB.tables.sneak_member_magic_links[0].token_hash;
        assert.notEqual(storedHash, mlResult.rawToken);
        assert.equal(storedHash, await sha256Hex(mlResult.rawToken));

        // 2. Consume magic link
        const consume1 = await verifyAndConsumeMagicLink(mockDB, mlResult.rawToken);
        assert.ok(consume1);
        assert.equal(consume1.user.email, 'member1@realty.com');
        assert.ok(consume1.sessionToken);

        // 3. Replay magic link -> Must return null (single use!)
        const consume2 = await verifyAndConsumeMagicLink(mockDB, mlResult.rawToken);
        assert.equal(consume2, null, 'Replayed magic link is rejected');
    });

    test('TEST 2: 7-Day Revocable Member Session & Logout', async () => {
        const mockDB = createMockMemberDB();
        const rawSession = await createMemberSession(mockDB, 'usr_1', 'acc_member_1', 604800);
        
        // Verify active session
        const sessionCtx1 = await verifyMemberSession(mockDB, rawSession);
        assert.ok(sessionCtx1);
        assert.equal(sessionCtx1.member_email, 'member1@realty.com');
        assert.equal(sessionCtx1.account_name, 'Member 1 Realty');

        // Revoke session on logout
        await revokeMemberSession(mockDB, rawSession);

        // Replay revoked session
        const sessionCtx2 = await verifyMemberSession(mockDB, rawSession);
        assert.equal(sessionCtx2, null, 'Revoked member session returns null');
    });

    test('TEST 3: Strict Tenant Isolation on Overview, Domains & Branding', async () => {
        const mockDB = createMockMemberDB();
        const session1 = await createMemberSession(mockDB, 'usr_1', 'acc_member_1', 604800);
        const session2 = await createMemberSession(mockDB, 'usr_2', 'acc_member_2', 604800);

        const env = { SNEAK_ENV: 'staging', DB: mockDB };

        // 1. Member 1 requests overview
        const res1 = await worker.fetch(new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/overview', {
            headers: {
                'Cookie': `__Host-sneak_member_session=${session1}`,
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        const data1 = await res1.json();
        assert.equal(data1.account.id, 'acc_member_1');
        assert.equal(data1.domains[0].domain, 'member1.com');

        // 2. Member 1 attempts to delete Member 2 domain (dom_2)
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

    test('TEST 4: Stripe Webhook Signature Verification & Idempotency', async () => {
        const mockDB = createMockMemberDB();
        const MOCK_WEBHOOK_SECRET = 'mock-unit-test-webhook-secret-12345';
        const rawPayload = JSON.stringify({
            id: 'evt_test_checkout_01',
            type: 'checkout.session.completed',
            created: 1771500000,
            data: {
                object: {
                    customer: 'cus_test_123',
                    subscription: 'sub_test_123',
                    metadata: { sneak_account_id: 'acc_member_1' }
                }
            }
        });

        // Generate HMAC signature
        const timestamp = Math.floor(Date.now() / 1000);
        const enc = new TextEncoder();
        const key = await crypto.subtle.importKey('raw', enc.encode(MOCK_WEBHOOK_SECRET), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
        const sig = Array.from(new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(`${timestamp}.${rawPayload}`)))).map(b => b.toString(16).padStart(2, '0')).join('');
        const sigHeader = `t=${timestamp},v1=${sig}`;

        // 1. Valid Signature Test
        const isValid = await verifyStripeWebhookSignature(rawPayload, sigHeader, MOCK_WEBHOOK_SECRET);
        assert.equal(isValid, true, 'Valid Stripe webhook signature accepted');

        // 2. Invalid Signature Test
        const isInvalid = await verifyStripeWebhookSignature(rawPayload, `t=${timestamp},v1=bad_signature`, MOCK_WEBHOOK_SECRET);
        assert.equal(isInvalid, false, 'Invalid signature rejected');

        // 3. Process Webhook Event
        const env = { SNEAK_ENV: 'staging', DB: mockDB, STRIPE_WEBHOOK_SECRET: MOCK_WEBHOOK_SECRET };
        const req1 = new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/stripe/webhook', {
            method: 'POST',
            headers: {
                'Stripe-Signature': sigHeader,
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            },
            body: rawPayload
        });
        const res1 = await worker.fetch(req1, env, {});
        assert.equal(res1.status, 200);

        // 4. Duplicate Event Idempotency Test -> Must return 200 with duplicate flag without re-inserting
        const req2 = new Request('https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/stripe/webhook', {
            method: 'POST',
            headers: {
                'Stripe-Signature': sigHeader,
                'Host': 'sneak-idx-member-staging.bonitaspringsrealtors.workers.dev'
            },
            body: rawPayload
        });
        const res2 = await worker.fetch(req2, env, {});
        assert.equal(res2.status, 200);
        const data2 = await res2.json();
        assert.equal(data2.duplicate, true, 'Duplicate webhook handled idempotently');
    });

    test('TEST 5: Billing Entitlement State Machine Transitions', () => {
        const now = new Date('2026-08-20T12:00:00Z');
        const futureGrace = '2026-08-23T12:00:00Z';
        const expiredGrace = '2026-08-19T12:00:00Z';

        assert.equal(deriveEntitlement('active', null, now), 'active');
        assert.equal(deriveEntitlement('trialing', null, now), 'active');
        assert.equal(deriveEntitlement('past_due', futureGrace, now), 'grace');
        assert.equal(deriveEntitlement('past_due', expiredGrace, now), 'inactive');
        assert.equal(deriveEntitlement('canceled', null, now), 'inactive');
        assert.equal(deriveEntitlement('unpaid', null, now), 'inactive');
        assert.equal(deriveEntitlement('incomplete', null, now), 'inactive');
    });
});
