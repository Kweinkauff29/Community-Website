/**
 * test/sneak-admin-worker.test.mjs
 * 
 * Unit and Security Test Suite for SNEAK Admin Worker (sneak-idx-admin-staging).
 */

import { test, describe, before } from 'node:test';
import assert from 'node:assert/strict';
import worker from '../sneak-admin/worker.js';
import {
    hashPasswordPbkdf2,
    verifyAdminPassword,
    createAdminSession,
    verifyAdminSession,
    revokeAdminSession,
    sha256Hex,
    timingSafeEqual
} from '../sneak-admin/auth.js';
import { normalizeDomain, validateDomainSafety } from '../sneak-admin/validation.js';

let TEST_PBKDF2_HASH = '';
const TEST_MOCK_PASSWORD = 'mock-unit-test-password-12345';

function createMockAdminDB() {
    const tables = {
        sneak_accounts: [
            { id: 'acc_demo', member_id: 'M1001', account_name: 'Demo Brokerage', status: 'active', plan: 'brokerage', created_at: '2026-08-20' }
        ],
        sneak_sites: [
            { id: 'site_demo', account_id: 'acc_demo', site_key: 'demo-ccor', site_name: 'Demo Site', status: 'active', scope_type: 'market', scope_value: null, created_at: '2026-08-20' }
        ],
        sneak_domains: [
            { id: 'dom_demo', site_id: 'site_demo', domain: 'localhost', verified: 1, status: 'active' }
        ],
        sneak_branding: [
            { site_id: 'site_demo', display_name: 'Demo Site', brokerage: 'Demo Brokerage', primary_color: '#1a365d', secondary_color: '#2596be' }
        ],
        sneak_widget_configs: [
            { id: 'w_demo_search', site_id: 'site_demo', widget_type: 'search', enabled: 1, config_json: '{}' }
        ],
        sneak_listings: [
            { ListingKey: 'L1', ListAgentMlsId: 'A123', ListOfficeMlsId: 'OFF99' }
        ],
        sneak_open_houses: [
            { id: 'oh1', OpenHouseKey: 'OH1', ListingKey: 'L1' }
        ],
        sneak_sync_state: [
            { sync_name: 'listings', status: 'success', last_cursor: '2026-08-20T12:00:00.000Z' }
        ],
        sneak_sync_runs: [],
        sneak_leads: [],
        sneak_admin_audit: [],
        sneak_admin_sessions: [],
        sneak_admin_login_attempts: []
    };

    function createStatement(query, boundArgs = []) {
        return {
            bind(...args) {
                return createStatement(query, args);
            },
            async first() {
                if (query.includes('FROM sneak_admin_sessions') && query.includes('token_hash')) {
                    const session = tables.sneak_admin_sessions.find(s => s.token_hash === boundArgs[0]);
                    if (!session || session.revoked_at) return null;
                    if (new Date(session.expires_at) < new Date()) return null;
                    return session;
                }
                if (query.includes('FROM sneak_admin_login_attempts')) {
                    const attempts = tables.sneak_admin_login_attempts.filter(a => a.ip_hash === boundArgs[0] && a.success === 0);
                    return { count: attempts.length };
                }
                if (query.includes('FROM sneak_accounts WHERE id =')) {
                    return tables.sneak_accounts.find(a => a.id === boundArgs[0]) || null;
                }
                if (query.includes('FROM sneak_sites WHERE site_key =')) {
                    return tables.sneak_sites.find(s => s.site_key === boundArgs[0]) || null;
                }
                if (query.includes('FROM sneak_sites WHERE id =')) {
                    return tables.sneak_sites.find(s => s.id === boundArgs[0]) || null;
                }
                if (query.includes('FROM sneak_domains WHERE id =')) {
                    return tables.sneak_domains.find(d => d.id === boundArgs[0]) || null;
                }
                if (query.includes('count(*) as count FROM sneak_listings WHERE ListAgentMlsId =')) {
                    const count = tables.sneak_listings.filter(l => l.ListAgentMlsId === boundArgs[0]).length;
                    return { count };
                }
                if (query.includes('count(*) as count FROM sneak_listings WHERE ListOfficeMlsId =')) {
                    const count = tables.sneak_listings.filter(l => l.ListOfficeMlsId === boundArgs[0]).length;
                    return { count };
                }
                if (query.includes('count(*) as count FROM sneak_listings')) {
                    return { count: tables.sneak_listings.length };
                }
                if (query.includes('count(*) as count FROM sneak_open_houses')) {
                    return { count: tables.sneak_open_houses.length };
                }
                if (query.includes('count(*) as total FROM sneak_domains')) {
                    return { total: tables.sneak_domains.length };
                }
                if (query.includes('SELECT count(*) as total, sum(case when status')) {
                    return { total: tables.sneak_accounts.length, active: 1, suspended: 0 };
                }
                if (query.includes('SELECT count(*) as total, sum(case when scope_type')) {
                    return { total: tables.sneak_sites.length, agent_sites: 0, office_sites: 0, market_sites: 1 };
                }
                return null;
            },
            async all() {
                if (query.includes('FROM sneak_accounts')) {
                    return { results: tables.sneak_accounts };
                }
                if (query.includes('FROM sneak_sites WHERE account_id =')) {
                    return { results: tables.sneak_sites.filter(s => s.account_id === boundArgs[0]) };
                }
                if (query.includes('FROM sneak_domains WHERE site_id =')) {
                    return { results: tables.sneak_domains.filter(d => d.site_id === boundArgs[0]) };
                }
                if (query.includes('FROM sneak_widget_configs WHERE site_id =')) {
                    return { results: tables.sneak_widget_configs.filter(w => w.site_id === boundArgs[0]) };
                }
                if (query.includes('FROM sneak_sync_state')) {
                    return { results: tables.sneak_sync_state };
                }
                if (query.includes('FROM sneak_sync_runs')) {
                    return { results: tables.sneak_sync_runs };
                }
                if (query.includes('FROM sneak_leads')) {
                    return { results: tables.sneak_leads };
                }
                if (query.includes('FROM sneak_admin_audit')) {
                    return { results: tables.sneak_admin_audit };
                }
                return { results: [] };
            },
            async run() {
                if (query.includes('INSERT INTO sneak_admin_sessions')) {
                    const [id, token_hash, admin_actor, created_at, expires_at, last_seen_at, user_agent_hash, created_ip_hash] = boundArgs;
                    tables.sneak_admin_sessions.push({
                        id,
                        token_hash,
                        admin_actor,
                        created_at,
                        expires_at,
                        last_seen_at,
                        revoked_at: null,
                        user_agent_hash,
                        created_ip_hash
                    });
                    return { success: true };
                }
                if (query.includes('UPDATE sneak_admin_sessions')) {
                    if (query.includes('admin_actor = ?')) {
                        const actor = boundArgs[0];
                        tables.sneak_admin_sessions.forEach(s => {
                            if (s.admin_actor === actor) s.revoked_at = new Date().toISOString();
                        });
                    } else if (query.includes('token_hash = ?')) {
                        const hash = boundArgs[0];
                        tables.sneak_admin_sessions.forEach(s => {
                            if (s.token_hash === hash) s.revoked_at = new Date().toISOString();
                        });
                    }
                    return { success: true };
                }
                if (query.includes('INSERT INTO sneak_admin_login_attempts')) {
                    tables.sneak_admin_login_attempts.push({
                        id: boundArgs[0],
                        ip_hash: boundArgs[1],
                        attempted_at: new Date().toISOString(),
                        success: boundArgs[2]
                    });
                    return { success: true };
                }
                if (query.includes('INSERT INTO sneak_accounts')) {
                    tables.sneak_accounts.push({ id: boundArgs[0], member_id: boundArgs[1], account_name: boundArgs[2], status: boundArgs[5], plan: boundArgs[6] });
                    return { success: true };
                }
                if (query.includes('INSERT INTO sneak_sites')) {
                    tables.sneak_sites.push({ id: boundArgs[0], account_id: boundArgs[1], site_key: boundArgs[2], site_name: boundArgs[3], status: boundArgs[4], scope_type: boundArgs[5], scope_value: boundArgs[6] });
                    return { success: true };
                }
                if (query.includes('INSERT INTO sneak_domains')) {
                    tables.sneak_domains.push({ id: boundArgs[0], site_id: boundArgs[1], domain: boundArgs[2], verified: boundArgs[3], status: boundArgs[4] });
                    return { success: true };
                }
                if (query.includes('INSERT INTO sneak_admin_audit')) {
                    tables.sneak_admin_audit.push({ id: boundArgs[0], action: boundArgs[2], entity_type: boundArgs[3], summary: boundArgs[5] });
                    return { success: true };
                }
                if (query.includes('UPDATE sneak_accounts SET')) {
                    const acc = tables.sneak_accounts.find(a => a.id === boundArgs[boundArgs.length - 1]);
                    if (acc && boundArgs[0]) acc.status = boundArgs[0];
                    return { success: true };
                }
                if (query.includes('DELETE FROM sneak_domains')) {
                    tables.sneak_domains = tables.sneak_domains.filter(d => d.id !== boundArgs[0]);
                    return { success: true };
                }
                return { success: true };
            }
        };
    }

    return {
        tables,
        prepare(query) {
            return createStatement(query);
        },
        async batch(statements) {
            for (const s of statements) {
                await s.run();
            }
            return statements.map(() => ({ success: true }));
        }
    };
}

describe('SNEAK Hardened Admin Worker Test Suite', () => {

    before(async () => {
        // Generate PBKDF2 hash (using 50,000 iterations in unit test suite for fast testing)
        TEST_PBKDF2_HASH = await hashPasswordPbkdf2(TEST_MOCK_PASSWORD, 50000);
    });

    test('TEST 1: PBKDF2 password hashing & constant-time verification', async () => {
        assert.ok(TEST_PBKDF2_HASH.startsWith('pbkdf2-sha256$50000$'));
        const valid = await verifyAdminPassword(TEST_MOCK_PASSWORD, TEST_PBKDF2_HASH);
        assert.equal(valid, true, 'Valid password matches PBKDF2 hash');

        const invalid = await verifyAdminPassword('WrongPassword!', TEST_PBKDF2_HASH);
        assert.equal(invalid, false, 'Invalid password rejected');

        // Timing-safe equal tests
        assert.equal(timingSafeEqual('abcdef', 'abcdef'), true);
        assert.equal(timingSafeEqual('abcdef', 'abcdeg'), false);
        assert.equal(timingSafeEqual('abcdef', 'abcde'), false);
    });

    test('TEST 2: Login flow creates revocable D1 session & sets __Host- cookie', async () => {
        const mockDB = createMockAdminDB();
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_ADMIN_PASSWORD_HASH: TEST_PBKDF2_HASH,
            DB: mockDB
        };

        const loginRes = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ password: TEST_MOCK_PASSWORD })
        }), env, {});

        assert.equal(loginRes.status, 200);
        const setCookie = loginRes.headers.get('Set-Cookie');
        assert.ok(setCookie.includes('__Host-sneak_admin_session='));
        assert.ok(setCookie.includes('HttpOnly'));
        assert.ok(setCookie.includes('SameSite=Strict'));

        // Verify session stored in D1
        assert.equal(mockDB.tables.sneak_admin_sessions.length, 1);
        assert.equal(mockDB.tables.sneak_admin_sessions[0].admin_actor, 'admin');
    });

    test('TEST 3: Session revocation on logout & replay prevention', async () => {
        const mockDB = createMockAdminDB();
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_ADMIN_PASSWORD_HASH: TEST_PBKDF2_HASH,
            DB: mockDB
        };

        // 1. Create session
        const rawToken = await createAdminSession(mockDB, 'admin', 'ip1', 'ua1', 3600);
        const cookie = `__Host-sneak_admin_session=${rawToken}`;

        // 2. Access dashboard with valid session
        const dashRes1 = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/dashboard', {
            headers: {
                'Cookie': cookie,
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(dashRes1.status, 200, 'Valid session accesses dashboard');

        // 3. Logout
        const logoutRes = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/logout', {
            method: 'POST',
            headers: {
                'Cookie': cookie,
                'Origin': 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(logoutRes.status, 200);

        // 4. Replay old session cookie -> must return 401 Unauthorized
        const dashRes2 = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/dashboard', {
            headers: {
                'Cookie': cookie,
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            }
        }), env, {});
        assert.equal(dashRes2.status, 401, 'Replayed revoked session returns 401');
    });

    test('TEST 4: Strict Same-Origin CSRF enforcement blocks foreign & missing origin on mutations', async () => {
        const mockDB = createMockAdminDB();
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_ADMIN_PASSWORD_HASH: TEST_PBKDF2_HASH,
            DB: mockDB
        };
        const rawToken = await createAdminSession(mockDB, 'admin', 'ip1', 'ua1', 3600);
        const cookie = `__Host-sneak_admin_session=${rawToken}`;

        // 1. Foreign Origin
        const foreignRes = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/accounts/acc_demo', {
            method: 'PATCH',
            headers: {
                'Cookie': cookie,
                'Content-Type': 'application/json',
                'Origin': 'https://attacker-site.com',
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ status: 'suspended' })
        }), env, {});
        assert.equal(foreignRes.status, 403, 'Foreign origin rejected with 403');

        // 2. Missing Origin on mutation
        const missingOriginRes = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/accounts/acc_demo', {
            method: 'PATCH',
            headers: {
                'Cookie': cookie,
                'Content-Type': 'application/json',
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ status: 'suspended' })
        }), env, {});
        assert.equal(missingOriginRes.status, 403, 'Missing origin rejected with 403');

        // 3. Same Origin mutation succeeds
        const sameOriginRes = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/accounts/acc_demo', {
            method: 'PATCH',
            headers: {
                'Cookie': cookie,
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ status: 'suspended' })
        }), env, {});
        assert.equal(sameOriginRes.status, 200, 'Same-origin mutation succeeds');
    });

    test('TEST 5: Rate limiting blocks after 10 failed login attempts', async () => {
        const mockDB = createMockAdminDB();
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_ADMIN_PASSWORD_HASH: TEST_PBKDF2_HASH,
            DB: mockDB
        };

        // Trigger 10 failed logins
        for (let i = 0; i < 10; i++) {
            await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/login', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Origin': 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                    'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                    'CF-Connecting-IP': '198.51.100.55'
                },
                body: JSON.stringify({ password: 'WrongPassword!' })
            }), env, {});
        }

        // 11th attempt should be rate limited
        const rateLimitRes = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                'CF-Connecting-IP': '198.51.100.55'
            },
            body: JSON.stringify({ password: TEST_MOCK_PASSWORD })
        }), env, {});

        assert.equal(rateLimitRes.status, 429, 'Rate limited with 429 Too Many Requests');
    });

    test('TEST 6: Missing SNEAK_ADMIN_PASSWORD_HASH fails closed with 500 error', async () => {
        const mockDB = createMockAdminDB();
        const env = {
            SNEAK_ENV: 'staging',
            DB: mockDB // SNEAK_ADMIN_PASSWORD_HASH missing!
        };

        const res = await worker.fetch(new Request('https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev/api/admin/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev',
                'Host': 'sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev'
            },
            body: JSON.stringify({ password: TEST_MOCK_PASSWORD })
        }), env, {});

        assert.equal(res.status, 500);
        const data = await res.json();
        assert.equal(data.error, 'ConfigurationError');
    });
});
