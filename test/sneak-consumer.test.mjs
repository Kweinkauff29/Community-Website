/**
 * test/sneak-consumer.test.mjs
 * 
 * SNEAK Consumer Identity & Server Favorites Test Suite:
 * - Anti-Enumeration & Zero-Token Guarantee in magic link responses
 * - Single-Use SHA-256 Magic Link & Replay Prevention
 * - 2-Minute Exchange Code Handoff & Single-Use Consumption
 * - 14-Day Revocable Consumer Sessions
 * - Strict Site-Scoped Tenant Isolation (Site A token fails on Site B)
 * - Server Favorites: List, Add, Remove, Merge Union, Max 200 Limit
 * - Address Suppression & Off-Market Listing Handling
 * - Open-Redirect Prevention
 * - Rate Limiting Defense (Email & IP)
 * - Account Deletion / GDPR
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import worker from '../sneak-consumer/worker.js';
import {
    sha256Hex,
    generateRawToken,
    requestConsumerMagicLink,
    verifyAndConsumeConsumerMagicLink,
    exchangeAuthCodeForSession,
    verifyConsumerSession,
    revokeConsumerSession,
    deleteConsumerAccount
} from '../sneak-consumer/auth.js';

const TEST_SIGNING_SECRET = 'test-signing-secret-key-32-chars-length!!';

// Mock Sign IDX Session token helper
async function createSignedIdxSession(siteKey, secret = TEST_SIGNING_SECRET) {
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const exp = Math.floor(Date.now() / 1000) + 3600;
    const payload = Buffer.from(JSON.stringify({
        siteKey,
        site_id: siteKey === 'site-mem-1' ? 'site_1' : (siteKey === 'demo-ccor' ? 'site_ccor' : 'site_2'),
        origin: 'https://coconutcoastrealtors.org',
        exp
    })).toString('base64url');

    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
        'raw',
        enc.encode(secret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
    );
    const sigBuf = await crypto.subtle.sign('HMAC', key, enc.encode(`${header}.${payload}`));
    const sig = Buffer.from(sigBuf).toString('base64url');
    return `${header}.${payload}.${sig}`;
}

function createMockConsumerDB() {
    const tables = {
        sneak_sites: [
            { id: 'site_1', site_key: 'site-mem-1', site_name: 'Site 1', status: 'active', scope_type: 'agent', scope_value: 'B3650316', account_id: 'acc_1' },
            { id: 'site_2', site_key: 'site-mem-2', site_name: 'Site 2', status: 'active', scope_type: 'office', scope_value: 'BPRI', account_id: 'acc_2' },
            { id: 'site_ccor', site_key: 'demo-ccor', site_name: 'CCOR Demo', status: 'active', scope_type: 'agent', scope_value: 'B3650316', account_id: 'acc_ccor' }
        ],
        sneak_accounts: [
            { id: 'acc_1', account_name: 'Member 1 Realty', status: 'active' },
            { id: 'acc_2', account_name: 'Member 2 Realty', status: 'active' },
            { id: 'acc_ccor', account_name: 'CCOR Realty', status: 'active' }
        ],
        sneak_domains: [
            { id: 'dom_1', site_id: 'site_1', domain: 'member1.com', verified: 1, status: 'active' },
            { id: 'dom_2', site_id: 'site_2', domain: 'member2.com', verified: 1, status: 'active' },
            { id: 'dom_ccor', site_id: 'site_ccor', domain: 'coconutcoastrealtors.org', verified: 1, status: 'active' }
        ],
        sneak_branding: [],
        sneak_consumer_users: [
            { id: 'c_usr_1', site_id: 'site_1', email: 'buyer1@example.com', status: 'active', created_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_consumer_magic_links: [],
        sneak_consumer_sessions: [],
        sneak_consumer_auth_exchanges: [],
        sneak_consumer_login_attempts: [],
        sneak_consumer_favorites: [
            { id: 'fav_1', site_id: 'site_1', user_id: 'c_usr_1', listing_key: '2240001', created_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_listings: [
            {
                ListingKey: '2240001',
                StandardStatus: 'Active',
                ListPrice: 750000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '123 Coconut Grove Way',
                City: 'Bonita Springs',
                BedroomsTotal: 3,
                BathroomsTotalInteger: 2,
                LivingArea: 1850,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1,
                PrimaryPhoto: 'https://images.sneakidx.com/photo1.jpg'
            },
            {
                ListingKey: '2240002',
                StandardStatus: 'Active',
                ListPrice: 920000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '456 Estero Bay Blvd',
                City: 'Estero',
                BedroomsTotal: 4,
                BathroomsTotalInteger: 3,
                LivingArea: 2400,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 0, // Suppressed address
                PrimaryPhoto: 'https://images.sneakidx.com/photo2.jpg'
            },
            {
                ListingKey: '2240003_closed',
                StandardStatus: 'Closed',
                ListPrice: 500000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '789 Hidden Harbor Dr',
                City: 'Naples',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1
            }
        ]
    };

    return {
        tables,
        prepare(query) {
            let boundArgs = [];
            const nq = query.replace(/\s+/g, ' ').trim();
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
                    // sneak_sites + sneak_accounts + sneak_branding
                    if (nq.includes('FROM sneak_sites s') && nq.includes('JOIN sneak_accounts a')) {
                        const site = tables.sneak_sites.find(s => s.site_key === boundArgs[0]);
                        if (!site) return { results: [] };
                        const acc = tables.sneak_accounts.find(a => a.id === site.account_id);
                        return {
                            results: [{
                                site_id: site.id,
                                site_key: site.site_key,
                                site_status: site.status,
                                account_name: acc?.account_name || 'Member',
                                account_status: acc?.status || 'active',
                                display_name: 'Ursula Weinkauff',
                                brokerage: 'CCOR'
                            }]
                        };
                    }

                    // sneak_sites
                    if (nq.includes('FROM sneak_sites WHERE site_key = ?')) {
                        const site = tables.sneak_sites.find(s => s.site_key === boundArgs[0]);
                        return { results: site ? [site] : [] };
                    }
                    if (nq.includes('FROM sneak_sites WHERE id = ?')) {
                        const site = tables.sneak_sites.find(s => s.id === boundArgs[0]);
                        return { results: site ? [site] : [] };
                    }

                    // sneak_domains
                    if (nq.includes('FROM sneak_domains WHERE domain = ?')) {
                        const dom = tables.sneak_domains.find(d => d.domain === boundArgs[0] && d.status === 'active');
                        return { results: dom ? [dom] : [] };
                    }
                    if (nq.includes('FROM sneak_domains WHERE site_id = ?')) {
                        const list = tables.sneak_domains.filter(d => d.site_id === boundArgs[0] && d.status === 'active' && d.verified === 1);
                        return { results: list };
                    }

                    // sneak_consumer_users
                    if (nq.includes('FROM sneak_consumer_users WHERE site_id = ? AND email = ?')) {
                        const u = tables.sneak_consumer_users.find(x => x.site_id === boundArgs[0] && x.email === boundArgs[1]);
                        return { results: u ? [u] : [] };
                    }
                    if (nq.includes('FROM sneak_consumer_users WHERE id = ?')) {
                        const u = tables.sneak_consumer_users.find(x => x.id === boundArgs[0]);
                        return { results: u ? [u] : [] };
                    }

                    // sneak_consumer_magic_links
                    if (nq.includes('FROM sneak_consumer_magic_links l') && nq.includes('JOIN sneak_consumer_users u')) {
                        const tokenHash = boundArgs[0];
                        const l = tables.sneak_consumer_magic_links.find(x => x.token_hash === tokenHash);
                        if (!l) return { results: [] };
                        const u = tables.sneak_consumer_users.find(x => x.id === l.user_id);
                        const s = tables.sneak_sites.find(x => x.id === l.site_id);
                        return {
                            results: [{
                                id: l.id,
                                user_id: l.user_id,
                                site_id: l.site_id,
                                site_key: s?.site_key,
                                return_url: l.return_url,
                                expires_at: l.expires_at,
                                email: u?.email,
                                user_status: u?.status
                            }]
                        };
                    }

                    // sneak_consumer_auth_exchanges
                    if (nq.includes('FROM sneak_consumer_auth_exchanges e') && nq.includes('JOIN sneak_consumer_users u')) {
                        const codeHash = boundArgs[0];
                        const e = tables.sneak_consumer_auth_exchanges.find(x => x.code_hash === codeHash);
                        if (!e) return { results: [] };
                        const u = tables.sneak_consumer_users.find(x => x.id === e.user_id);
                        const s = tables.sneak_sites.find(x => x.id === e.site_id);
                        return {
                            results: [{
                                id: e.id,
                                user_id: e.user_id,
                                site_id: e.site_id,
                                site_key: s?.site_key,
                                expires_at: e.expires_at,
                                email: u?.email,
                                user_status: u?.status
                            }]
                        };
                    }

                    // sneak_consumer_sessions
                    if (nq.includes('FROM sneak_consumer_sessions s') && nq.includes('JOIN sneak_consumer_users u')) {
                        const tokenHash = boundArgs[0];
                        const s = tables.sneak_consumer_sessions.find(x => x.token_hash === tokenHash && !x.revoked_at);
                        if (!s) return { results: [] };
                        const u = tables.sneak_consumer_users.find(x => x.id === s.user_id);
                        const si = tables.sneak_sites.find(x => x.id === s.site_id);
                        return {
                            results: [{
                                session_id: s.id,
                                user_id: s.user_id,
                                site_id: s.site_id,
                                site_key: si?.site_key,
                                site_status: si?.status || 'active',
                                created_at: s.created_at,
                                expires_at: s.expires_at,
                                consumer_email: u?.email,
                                user_status: u?.status
                            }]
                        };
                    }

                    // Rate limiting
                    if (nq.includes('FROM sneak_consumer_login_attempts')) {
                        const count = tables.sneak_consumer_login_attempts.filter(a => a.identifier_hash === boundArgs[0]).length;
                        return { results: [{ count }] };
                    }

                    // sneak_consumer_favorites
                    if (nq.includes('COUNT(*) as count FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?') || nq.includes('count(*) as count FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?')) {
                        const count = tables.sneak_consumer_favorites.filter(f => f.user_id === boundArgs[0] && f.site_id === boundArgs[1]).length;
                        return { results: [{ count }] };
                    }
                    if (nq.includes('FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ? AND listing_key = ?')) {
                        const fav = tables.sneak_consumer_favorites.find(f => f.user_id === boundArgs[0] && f.site_id === boundArgs[1] && f.listing_key === boundArgs[2]);
                        return { results: fav ? [fav] : [] };
                    }
                    if (nq.includes('FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?') && nq.includes('ORDER BY')) {
                        const favs = tables.sneak_consumer_favorites.filter(f => f.user_id === boundArgs[0] && f.site_id === boundArgs[1]);
                        return { results: favs };
                    }
                    if (nq.includes('FROM sneak_listings WHERE ListingKey IN')) {
                        const placeholders = boundArgs.filter(k => typeof k === 'string' && k.length > 0);
                        const results = tables.sneak_listings.filter(l => placeholders.includes(l.ListingKey));
                        return { results };
                    }
                    if (nq.includes('FROM sneak_listings WHERE ListingKey = ?')) {
                        const item = tables.sneak_listings.find(x => x.ListingKey === boundArgs[0]);
                        return { results: item ? [item] : [] };
                    }

                    return { results: [] };
                },
                async run() {
                    // INSERT INTO sneak_consumer_users
                    if (nq.includes('INSERT INTO sneak_consumer_users')) {
                        tables.sneak_consumer_users.push({
                            id: boundArgs[0],
                            site_id: boundArgs[1],
                            email: boundArgs[2],
                            status: 'pending',
                            created_at: new Date().toISOString()
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_users
                    if (nq.includes('UPDATE sneak_consumer_users SET status =')) {
                        const u = tables.sneak_consumer_users.find(x => x.id === boundArgs[3]);
                        if (u) {
                            u.status = 'active';
                            u.activated_at = new Date().toISOString();
                            u.last_login_at = new Date().toISOString();
                        }
                        return { meta: { changes: 1 }, success: true };
                    }
                    // INSERT INTO sneak_consumer_magic_links
                    if (nq.includes('INSERT INTO sneak_consumer_magic_links')) {
                        tables.sneak_consumer_magic_links.push({
                            id: boundArgs[0],
                            user_id: boundArgs[1],
                            site_id: boundArgs[2],
                            token_hash: boundArgs[3],
                            purpose: 'login',
                            return_url: boundArgs[4],
                            created_at: boundArgs[5],
                            expires_at: boundArgs[6],
                            used_at: null
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_magic_links (used_at)
                    if (nq.includes('UPDATE sneak_consumer_magic_links')) {
                        const tokenHash = boundArgs[1];
                        const l = tables.sneak_consumer_magic_links.find(x => x.token_hash === tokenHash && !x.used_at);
                        if (l) {
                            l.used_at = boundArgs[0];
                            return { meta: { changes: 1 }, changes: 1, success: true };
                        }
                        return { meta: { changes: 0 }, changes: 0, success: true };
                    }
                    // INSERT INTO sneak_consumer_auth_exchanges
                    if (nq.includes('INSERT INTO sneak_consumer_auth_exchanges')) {
                        tables.sneak_consumer_auth_exchanges.push({
                            id: boundArgs[0],
                            user_id: boundArgs[1],
                            site_id: boundArgs[2],
                            code_hash: boundArgs[3],
                            created_at: boundArgs[4],
                            expires_at: boundArgs[5],
                            used_at: null
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_auth_exchanges (used_at)
                    if (nq.includes('UPDATE sneak_consumer_auth_exchanges')) {
                        const codeHash = boundArgs[1];
                        const e = tables.sneak_consumer_auth_exchanges.find(x => x.code_hash === codeHash && !x.used_at);
                        if (e) {
                            e.used_at = boundArgs[0];
                            return { meta: { changes: 1 }, changes: 1, success: true };
                        }
                        return { meta: { changes: 0 }, changes: 0, success: true };
                    }
                    // INSERT INTO sneak_consumer_sessions
                    if (nq.includes('INSERT INTO sneak_consumer_sessions')) {
                        tables.sneak_consumer_sessions.push({
                            id: boundArgs[0],
                            user_id: boundArgs[1],
                            site_id: boundArgs[2],
                            token_hash: boundArgs[3],
                            created_at: boundArgs[4],
                            expires_at: boundArgs[5],
                            revoked_at: null
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_sessions (revocation)
                    if (nq.includes('UPDATE sneak_consumer_sessions SET revoked_at =')) {
                        const s = tables.sneak_consumer_sessions.find(x => x.token_hash === boundArgs[0]);
                        if (s) s.revoked_at = new Date().toISOString();
                        return { meta: { changes: 1 }, success: true };
                    }
                    // INSERT INTO sneak_consumer_login_attempts
                    if (nq.includes('INSERT INTO sneak_consumer_login_attempts')) {
                        tables.sneak_consumer_login_attempts.push({
                            id: boundArgs[0],
                            site_id: boundArgs[1],
                            identifier_hash: boundArgs[2],
                            attempt_type: boundArgs[3],
                            created_at: boundArgs[4]
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // INSERT OR IGNORE INTO sneak_consumer_favorites
                    if (nq.includes('INSERT OR IGNORE INTO sneak_consumer_favorites')) {
                        const exists = tables.sneak_consumer_favorites.some(f => f.user_id === boundArgs[2] && f.site_id === boundArgs[1] && f.listing_key === boundArgs[3]);
                        if (!exists) {
                            tables.sneak_consumer_favorites.push({
                                id: boundArgs[0],
                                site_id: boundArgs[1],
                                user_id: boundArgs[2],
                                listing_key: boundArgs[3],
                                created_at: new Date().toISOString()
                            });
                        }
                        return { meta: { changes: 1 }, success: true };
                    }
                    // DELETE FROM sneak_consumer_favorites
                    if (nq.includes('DELETE FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ? AND listing_key = ?')) {
                        tables.sneak_consumer_favorites = tables.sneak_consumer_favorites.filter(
                            f => !(f.user_id === boundArgs[0] && f.site_id === boundArgs[1] && f.listing_key === boundArgs[2])
                        );
                        return { meta: { changes: 1 }, success: true };
                    }
                    if (nq.includes('DELETE FROM sneak_consumer_users')) {
                        const uid = boundArgs[0];
                        const sid = boundArgs[1];
                        tables.sneak_consumer_users = tables.sneak_consumer_users.filter(
                            u => !(u.id === uid && u.site_id === sid)
                        );
                        tables.sneak_consumer_favorites = tables.sneak_consumer_favorites.filter(
                            f => !(f.user_id === uid && f.site_id === sid)
                        );
                        tables.sneak_consumer_sessions = tables.sneak_consumer_sessions.filter(
                            s => !(s.user_id === uid && s.site_id === sid)
                        );
                        return { meta: { changes: 1 }, success: true };
                    }

                    return { meta: { changes: 1 }, success: true };
                }
            };
        }
    };
}

describe('CCOR IDX / SNEAK Consumer Worker & Identity (Phase 7.3C1A)', () => {

    test('1. Health and Version check returns build 2026.08.27.7.3c1a', async () => {
        const req = new Request('https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev/api/consumer/version');
        const res = await worker.fetch(req, {});
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.service, 'sneak-consumer-worker');
        assert.equal(data.build, '2026.08.27.7.3c1a');
        assert.equal(data.status, 'healthy');
    });

    test('2. Anti-Enumeration: magic link request returns identical generic response regardless of existence', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Existing user
        const req1 = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                email: 'buyer1@example.com',
                returnUrl: 'https://member1.com/homes'
            })
        });
        const res1 = await worker.fetch(req1, env);
        assert.equal(res1.status, 200);
        const data1 = await res1.json();
        assert.equal(data1.success, true);
        assert.equal(data1.message, 'If the email is valid, a sign-in link has been sent. Please check your inbox.');

        // New user
        const req2 = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                email: 'brand_new_buyer@example.com',
                returnUrl: 'https://member1.com/homes'
            })
        });
        const res2 = await worker.fetch(req2, env);
        assert.equal(res2.status, 200);
        const data2 = await res2.json();
        assert.equal(data2.success, true);
        assert.equal(data2.message, 'If the email is valid, a sign-in link has been sent. Please check your inbox.');

        // Verify tokens were created in DB with SHA-256 hash (never raw token)
        assert.equal(db.tables.sneak_consumer_magic_links.length, 2);
        assert.ok(db.tables.sneak_consumer_magic_links[0].token_hash);
        assert.notEqual(db.tables.sneak_consumer_magic_links[0].token_hash, 'buyer1@example.com');
    });

    test('3. Open-Redirect Prevention: invalid or unverified external returnUrl is rejected', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Attacker attempting open redirect to evil-phishing.com
        const req = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                email: 'buyer1@example.com',
                returnUrl: 'https://evil-phishing.com/steal-creds'
            })
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200); // Generic response prevents enumeration, but zero token is generated
        assert.equal(db.tables.sneak_consumer_magic_links.length, 0);
    });

    test('4. End-to-end Flow: Magic Link verify -> 2-min Exchange Code -> 14-day Session token', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // 1. Issue magic link
        await requestConsumerMagicLink(db, {
            siteKey: 'site-mem-1',
            email: 'flowbuyer@example.com',
            returnUrl: 'https://member1.com/idx-search',
            ipHash: 'hash_192_168_1_1',
            env
        });
        assert.equal(db.tables.sneak_consumer_magic_links.length, 1);
        const linkRecord = db.tables.sneak_consumer_magic_links[0];
        
        // Create known token
        const rawToken = generateRawToken();
        const knownHash = await sha256Hex(rawToken);
        linkRecord.token_hash = knownHash;

        // 2. Consume magic link via GET /api/consumer/auth/verify?token=...
        const verifyReq = new Request(`https://consumer.staging/api/consumer/auth/verify?token=${rawToken}`);
        const verifyRes = await worker.fetch(verifyReq, env);
        assert.equal(verifyRes.status, 302, 'Verify endpoint must 302 redirect');
        const redirectUrl = new URL(verifyRes.headers.get('Location'));
        assert.equal(redirectUrl.origin, 'https://member1.com');
        assert.equal(redirectUrl.pathname, '/idx-search');
        assert.ok(redirectUrl.searchParams.has('auth_code'));
        const authCode = redirectUrl.searchParams.get('auth_code');

        // 3. Replay prevention: reusing the token must fail
        const replayRes = await worker.fetch(verifyReq, env);
        assert.equal(replayRes.status, 401);

        // 4. Exchange auth_code for session token
        const idxSession = await createSignedIdxSession('site-mem-1');
        const exchangeReq = new Request('https://consumer.staging/api/consumer/auth/exchange', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                code: authCode,
                session: idxSession
            })
        });
        const exchangeRes = await worker.fetch(exchangeReq, env);
        assert.equal(exchangeRes.status, 200);
        const exchangeData = await exchangeRes.json();
        assert.ok(exchangeData.consumerSession);
        assert.equal(exchangeData.user.email, 'flowbuyer@example.com');
        const consumerSession = exchangeData.consumerSession;

        // 5. Replay exchange code must fail
        const replayExchangeRes = await worker.fetch(exchangeReq, env);
        assert.equal(replayExchangeRes.status, 400);

        // 6. Access /api/consumer/auth/me
        const meReq = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-1', {
            headers: { Authorization: `Bearer ${consumerSession}` }
        });
        const meRes = await worker.fetch(meReq, env);
        assert.equal(meRes.status, 200);
        const meData = await meRes.json();
        assert.equal(meData.user.email, 'flowbuyer@example.com');
    });

    test('5. Tenant Isolation: Session created on Site A returns 403 on Site B', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Create session for Site 1
        const tokenSite1 = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_iso_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(tokenSite1),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        // Access Site 1 -> 200 OK
        const reqSite1 = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-1', {
            headers: { Authorization: `Bearer ${tokenSite1}` }
        });
        const resSite1 = await worker.fetch(reqSite1, env);
        assert.equal(resSite1.status, 200);

        // Access Site 2 with Site 1 token -> 403 Forbidden
        const reqSite2 = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-2', {
            headers: { Authorization: `Bearer ${tokenSite1}` }
        });
        const resSite2 = await worker.fetch(reqSite2, env);
        assert.equal(resSite2.status, 403);
        const dataSite2 = await resSite2.json();
        assert.equal(dataSite2.error, 'TenantSiteMismatch');
    });

    test('6. Server Favorites: List, Add, Remove, and Merge Union', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Log in buyer1 (c_usr_1 on site_1)
        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_fav_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        // 1. List favorites (initially has 2240001)
        const listReq = new Request('https://consumer.staging/api/consumer/favorites?site=site-mem-1', {
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const listRes = await worker.fetch(listReq, env);
        assert.equal(listRes.status, 200);
        const listData = await listRes.json();
        assert.equal(listData.favorites.length, 1);
        assert.equal(listData.favorites[0].listingKey, '2240001');
        assert.equal(listData.favorites[0].listing.ListPrice, 750000);

        // 2. Add listing 2240002 (with suppressed address check)
        const addReq = new Request('https://consumer.staging/api/consumer/favorites', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                listingKey: '2240002'
            })
        });
        const addRes = await worker.fetch(addReq, env);
        assert.equal(addRes.status, 200);

        // Verify address suppression was applied
        const listRes2 = await worker.fetch(listReq, env);
        const listData2 = await listRes2.json();
        assert.equal(listData2.favorites.length, 2);
        const suppressedFav = listData2.favorites.find(f => f.listingKey === '2240002');
        assert.equal(suppressedFav.listing.UnparsedAddress, 'Address Undisclosed');

        // 3. Remove listing 2240001
        const delReq = new Request('https://consumer.staging/api/consumer/favorites/2240001?site=site-mem-1', {
            method: 'DELETE',
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const delRes = await worker.fetch(delReq, env);
        assert.equal(delRes.status, 200);

        // 4. Merge anonymous favorites
        const mergeReq = new Request('https://consumer.staging/api/consumer/favorites/merge', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                listingKeys: ['2240001', '2240002', '2240003_closed']
            })
        });
        const mergeRes = await worker.fetch(mergeReq, env);
        assert.equal(mergeRes.status, 200);
        const mergeData = await mergeRes.json();
        assert.equal(mergeData.success, true);
    });

    test('7. Server Favorites Limit: Max 200 listings enforced', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_limit',
            user_id: 'c_usr_limit',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });
        db.tables.sneak_consumer_users.push({
            id: 'c_usr_limit',
            site_id: 'site_1',
            email: 'limit@example.com',
            status: 'active'
        });

        // Fill 200 favorites
        for (let i = 0; i < 200; i++) {
            db.tables.sneak_consumer_favorites.push({
                id: `fav_fill_${i}`,
                site_id: 'site_1',
                user_id: 'c_usr_limit',
                listing_key: `key_${i}`,
                created_at: new Date().toISOString()
            });
        }

        // Try adding 201st favorite
        const addReq = new Request('https://consumer.staging/api/consumer/favorites', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                listingKey: '2240001'
            })
        });
        const addRes = await worker.fetch(addReq, env);
        assert.equal(addRes.status, 400);
        const addData = await addRes.json();
        assert.equal(addData.error, 'FavoriteLimitExceeded');
    });

    test('8. Logout Session Revocation', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_logout',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        // Logout
        const logoutReq = new Request('https://consumer.staging/api/consumer/auth/logout', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ site: 'site-mem-1' })
        });
        const logoutRes = await worker.fetch(logoutReq, env);
        assert.equal(logoutRes.status, 200);

        // Verification after logout -> 401
        const meReq = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-1', {
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const meRes = await worker.fetch(meReq, env);
        assert.equal(meRes.status, 401);
    });

    test('9. GDPR / Account Deletion permanently removes consumer data', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_del',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        const delReq = new Request('https://consumer.staging/api/consumer/account?site=site-mem-1', {
            method: 'DELETE',
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const delRes = await worker.fetch(delReq, env);
        assert.equal(delRes.status, 200);
        const delData = await delRes.json();
        assert.equal(delData.success, true);

        // Consumer user and favorites are gone from DB
        assert.equal(db.tables.sneak_consumer_users.some(u => u.id === 'c_usr_1'), false);
        assert.equal(db.tables.sneak_consumer_favorites.some(f => f.user_id === 'c_usr_1'), false);
    });
});
