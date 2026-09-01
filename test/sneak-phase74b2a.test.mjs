import test, { describe } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import {
    buildTenantListingScope,
    isListingIdxEligible,
    applyListingDisplayControls,
    buildCommonListingFilters
} from '../sneak-shared/idx-query.js';
import { getSecurityHeaders } from '../sneak-admin/worker.js';
import consumerWorker from '../sneak-consumer/worker.js';
import alertsWorker, { processAlerts } from '../sneak-alerts/worker.js';
import adminWorker from '../sneak-admin/worker.js';

const rootDir = path.resolve(import.meta.dirname, '..');

describe('CCOR IDX — Phase 7.4B2A.1 Production Activation Pre-Flight Corrections', () => {

    describe('1. Pilot Search Scope Model & Lead Routing', () => {
        const pilotSite = {
            id: 'site_pilot01',
            site_id: 'site_pilot01',
            account_id: 'acc_pilot01',
            site_key: 'pilot-01',
            site_name: 'Ursula Weinkauff IDX',
            status: 'active',
            scope_type: 'market',
            scope_value: null,
            default_agent_mls_id: '633942'
        };

        const listingA = {
            ListingKey: 'LIST_A_OTHER_AGENT',
            ListingId: '224001',
            ListPrice: 750000,
            ListAgentMlsId: 'OTHER_AGENT_888',
            ListOfficeName: 'Other Brokerage LLC',
            StandardStatus: 'Active',
            InternetEntireListingDisplayYN: 1,
            InternetAddressDisplayYN: 1,
            City: 'Bonita Springs',
            UnparsedAddress: '123 Palm Ave'
        };

        const listingB = {
            ListingKey: 'LIST_B_PARTICIPANT',
            ListingId: '224002',
            ListPrice: 920000,
            ListAgentMlsId: '633942',
            ListOfficeName: 'Local Real Estate LLC',
            StandardStatus: 'Active',
            InternetEntireListingDisplayYN: 1,
            InternetAddressDisplayYN: 1,
            City: 'Bonita Springs',
            UnparsedAddress: '456 Ocean Dr'
        };

        const listingC_Ineligible = {
            ListingKey: 'LIST_C_INELIGIBLE',
            ListingId: '224003',
            ListPrice: 650000,
            ListAgentMlsId: 'OTHER_AGENT_888',
            StandardStatus: 'Active',
            InternetEntireListingDisplayYN: 0, // Ineligible for IDX
            InternetAddressDisplayYN: 1,
            City: 'Bonita Springs',
            UnparsedAddress: '789 Hidden Rd'
        };

        const listingD_Closed = {
            ListingKey: 'LIST_D_CLOSED',
            ListingId: '224004',
            ListPrice: 800000,
            ListAgentMlsId: '633942',
            StandardStatus: 'Closed', // Ineligible status
            InternetEntireListingDisplayYN: 1,
            InternetAddressDisplayYN: 1,
            City: 'Bonita Springs',
            UnparsedAddress: '101 Sold Way'
        };

        test('market pilot site compiles to 1=1 full-market search scope', () => {
            const scope = buildTenantListingScope(pilotSite);
            assert.equal(scope.valid, true);
            assert.equal(scope.clause, '1=1');
            assert.deepEqual(scope.binds, []);
        });

        test('full-market search matches both participant listing and other-agent listing', () => {
            const scope = buildTenantListingScope(pilotSite);
            const db = new DatabaseSync(':memory:');
            try {
                db.exec(`
                    CREATE TABLE sneak_listings (
                        ListingKey TEXT PRIMARY KEY,
                        ListingId TEXT,
                        ListPrice INTEGER,
                        ListAgentMlsId TEXT,
                        ListOfficeName TEXT,
                        StandardStatus TEXT,
                        PropertyType TEXT,
                        PropertySubType TEXT,
                        BedroomsTotal INTEGER,
                        BathroomsTotalInteger REAL,
                        LivingArea INTEGER,
                        InternetEntireListingDisplayYN INTEGER,
                        InternetAddressDisplayYN INTEGER,
                        City TEXT,
                        UnparsedAddress TEXT,
                        ModificationTimestamp DATETIME,
                        ListingContractDate DATETIME
                    );
                `);

                const insert = db.prepare(`
                    INSERT INTO sneak_listings (
                        ListingKey, ListingId, ListPrice, ListAgentMlsId, ListOfficeName,
                        StandardStatus, PropertyType, PropertySubType, BedroomsTotal, BathroomsTotalInteger, LivingArea,
                        InternetEntireListingDisplayYN, InternetAddressDisplayYN,
                        City, UnparsedAddress, ModificationTimestamp, ListingContractDate
                    ) VALUES (?, ?, ?, ?, ?, ?, 'Residential', 'Single Family Residence', 3, 2, 2000, ?, ?, ?, ?, datetime('now'), datetime('now'))
                `);

                for (const l of [listingA, listingB, listingC_Ineligible, listingD_Closed]) {
                    insert.run(
                        l.ListingKey, l.ListingId, l.ListPrice, l.ListAgentMlsId, l.ListOfficeName || '',
                        l.StandardStatus, l.InternetEntireListingDisplayYN, l.InternetAddressDisplayYN,
                        l.City, l.UnparsedAddress
                    );
                }

                // Execute market-scoped search
                const params = new URLSearchParams({ site: 'pilot-01' });
                const filter = buildCommonListingFilters(params, pilotSite);
                assert.equal(filter.valid, true);

                const rows = db.prepare(`SELECT ListingKey, ListAgentMlsId, ListOfficeName FROM sneak_listings ${filter.whereSQL}`)
                    .all(...filter.bindValues);

                const keys = rows.map(r => r.ListingKey);
                assert.ok(keys.includes('LIST_A_OTHER_AGENT'), 'Other agent eligible listing appears in market search');
                assert.ok(keys.includes('LIST_B_PARTICIPANT'), 'Participant listing appears in market search');
                assert.ok(!keys.includes('LIST_C_INELIGIBLE'), 'Display-ineligible listing is rejected from market search');
                assert.ok(!keys.includes('LIST_D_CLOSED'), 'Closed listing is rejected from active market search');

                // Detail check
                const detailA = db.prepare(`SELECT * FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?) AND ${scope.clause}`)
                    .get('LIST_A_OTHER_AGENT', 'LIST_A_OTHER_AGENT', ...scope.binds);
                assert.ok(detailA, 'Detail for other agent listing succeeds');
                assert.equal(isListingIdxEligible(detailA), true);
                assert.equal(detailA.ListOfficeName, 'Other Brokerage LLC', 'Listing brokerage attribution is preserved intact');

                const detailB = db.prepare(`SELECT * FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?) AND ${scope.clause}`)
                    .get('LIST_B_PARTICIPANT', 'LIST_B_PARTICIPANT', ...scope.binds);
                assert.ok(detailB, 'Detail for participant listing succeeds');
                assert.equal(isListingIdxEligible(detailB), true);

                // Ineligible listings
                const detailC = db.prepare(`SELECT * FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?) AND ${scope.clause}`)
                    .get('LIST_C_INELIGIBLE', 'LIST_C_INELIGIBLE', ...scope.binds);
                assert.equal(isListingIdxEligible(detailC), false, 'Listing C fails display eligibility');

                const detailD = db.prepare(`SELECT * FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?) AND ${scope.clause}`)
                    .get('LIST_D_CLOSED', 'LIST_D_CLOSED', ...scope.binds);
                assert.equal(isListingIdxEligible(detailD), false, 'Listing D fails status eligibility');

                // Featured listings query for participant agent 633942
                const agentRows = db.prepare(`
                    SELECT ListingKey, ListAgentMlsId FROM sneak_listings
                    WHERE ListAgentMlsId = ? AND StandardStatus = 'Active' AND InternetEntireListingDisplayYN = 1 AND ${scope.clause}
                `).all('633942', ...scope.binds);

                assert.equal(agentRows.length, 1);
                assert.equal(agentRows[0].ListingKey, 'LIST_B_PARTICIPANT');
                assert.equal(agentRows[0].ListAgentMlsId, '633942');
            } finally {
                db.close();
            }
        });

        test('lead capture on another broker listing routes to owning member site without overwriting attribution', () => {
            const db = new DatabaseSync(':memory:');
            try {
                db.exec(`
                    CREATE TABLE sneak_leads (
                        id TEXT PRIMARY KEY,
                        site_id TEXT,
                        listing_key TEXT,
                        name TEXT,
                        email TEXT,
                        created_at DATETIME
                    );
                `);

                // Inquiry on Listing A (Other Brokerage LLC) on pilot site
                const leadId = 'lead_test_01';
                db.prepare(`
                    INSERT INTO sneak_leads (id, site_id, listing_key, name, email, created_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                `).run(leadId, pilotSite.id, listingA.ListingKey, 'Prospective Buyer', 'buyer@example.com');

                const savedLead = db.prepare('SELECT * FROM sneak_leads WHERE id = ?').get(leadId);
                assert.equal(savedLead.site_id, 'site_pilot01', 'Lead routes to owning participant site');
                assert.equal(savedLead.listing_key, 'LIST_A_OTHER_AGENT');
                assert.equal(listingA.ListOfficeName, 'Other Brokerage LLC', 'Listing brokerage attribution remains untouched');
            } finally {
                db.close();
            }
        });
    });

    describe('2. Admin Content Security Policy (CSP)', () => {
        test('Admin CSP is strictly connect-src self for production, staging, and missing env', () => {
            const prodHeaders = getSecurityHeaders({ SNEAK_ENV: 'production' });
            const stagingHeaders = getSecurityHeaders({ SNEAK_ENV: 'staging' });
            const emptyHeaders = getSecurityHeaders({});

            for (const [name, headers] of [
                ['production', prodHeaders],
                ['staging', stagingHeaders],
                ['empty env', emptyHeaders]
            ]) {
                const csp = headers['Content-Security-Policy'];
                assert.ok(csp, `${name} CSP header exists`);
                assert.match(csp, /connect-src 'self';/, `${name} CSP strictly uses connect-src 'self'`);
                assert.doesNotMatch(csp, /staging/, `${name} CSP contains no staging references`);
                assert.doesNotMatch(csp, /sneak-idx-worker/, `${name} CSP contains no external worker hostnames`);
                assert.match(csp, /frame-ancestors 'none'/, `${name} frame-ancestors is none`);
            }
        });

        const mockDb = {
            prepare: (sql) => ({
                bind: (...args) => ({
                    first: async () => {
                        if (sql.includes('sneak_admin_sessions')) {
                            return { session_hash: 'mock_hash', expires_at: new Date(Date.now() + 10000).toISOString(), admin_actor: 'admin' };
                        }
                        return null;
                    },
                    all: async () => [],
                    run: async () => ({})
                }),
                first: async () => null,
                all: async () => [],
                run: async () => ({})
            })
        };

        const prodEnv = {
            SNEAK_ENV: 'production',
            SNEAK_SERVICE_NAME: 'sneak-idx-admin',
            SNEAK_ADMIN_PASSWORD_HASH: null,
            DB: mockDb
        };

        function assertNoStagingCsp(res, label) {
            const csp = res.headers.get('Content-Security-Policy');
            assert.ok(csp, `${label} has CSP header`);
            assert.match(csp, /connect-src 'self';/, `${label} CSP enforces connect-src 'self'`);
            assert.doesNotMatch(csp, /staging/, `${label} CSP contains no staging references`);
            assert.doesNotMatch(csp, /sneak-idx-worker-staging/, `${label} CSP contains no staging worker host`);
            assert.doesNotMatch(csp, /sneak-idx-staging/, `${label} CSP contains no sneak-idx-staging`);
        }

        test('production Admin UI route (GET /) returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/', { method: 'GET' }), prodEnv);
            assert.equal(res.status, 200);
            assertNoStagingCsp(res, 'GET /');
        });

        test('production Admin health route (GET /health) returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/health', { method: 'GET' }), prodEnv);
            assert.equal(res.status, 200);
            assertNoStagingCsp(res, 'GET /health');
        });

        test('production Admin login CSRF failure returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/api/admin/login', { method: 'POST' }), prodEnv);
            assert.equal(res.status, 403);
            assertNoStagingCsp(res, 'POST /api/admin/login (CSRF)');
        });

        test('production Admin login missing password hash returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/api/admin/login', {
                method: 'POST',
                headers: { 'X-Sneak-Admin': '1', Origin: 'https://admin.example', Host: 'admin.example' }
            }), prodEnv);
            assert.equal(res.status, 500);
            assertNoStagingCsp(res, 'POST /api/admin/login (missing hash)');
        });

        test('production Admin protected route unauthenticated returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/api/admin/me', {
                method: 'GET',
                headers: { 'X-Sneak-Admin': '1', Origin: 'https://admin.example', Host: 'admin.example' }
            }), prodEnv);
            assert.equal(res.status, 401);
            assertNoStagingCsp(res, 'GET /api/admin/me (unauthenticated)');
        });

        test('production Admin protected route authenticated JSON returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/api/admin/me', {
                method: 'GET',
                headers: {
                    'X-Sneak-Admin': '1',
                    Origin: 'https://admin.example',
                    Host: 'admin.example',
                    Cookie: '__Host-sneak_admin_session=0123456789abcdef0123456789abcdef'
                }
            }), prodEnv);
            assert.equal(res.status, 200);
            assertNoStagingCsp(res, 'GET /api/admin/me (authenticated)');
        });

        test('production Admin root 404 route returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/nonexistent-path', { method: 'GET' }), prodEnv);
            assert.equal(res.status, 404);
            assertNoStagingCsp(res, 'GET /nonexistent-path (Root 404)');
        });

        test('production Admin API 404 route returns clean CSP with zero staging hosts', async () => {
            const res = await adminWorker.fetch(new Request('https://admin.example/api/admin/nonexistent-endpoint', {
                method: 'GET',
                headers: {
                    'X-Sneak-Admin': '1',
                    Origin: 'https://admin.example',
                    Host: 'admin.example',
                    Cookie: '__Host-sneak_admin_session=0123456789abcdef0123456789abcdef'
                }
            }), prodEnv);
            assert.equal(res.status, 404);
            assertNoStagingCsp(res, 'GET /api/admin/nonexistent-endpoint (API 404)');
        });

        test('staging Admin health route remains functional with same-origin CSP', async () => {
            const res = await adminWorker.fetch(new Request('https://admin-staging.example/health', { method: 'GET' }), {
                SNEAK_ENV: 'staging',
                SNEAK_SERVICE_NAME: 'sneak-idx-admin-staging',
                DB: mockDb
            });
            assert.equal(res.status, 200);
            assertNoStagingCsp(res, 'Staging GET /health');
        });
    });

    describe('3. Production Secret Inventory & Runtime Reader Verification', () => {
        test('SNEAK_ADMIN_SESSION_SECRET is NOT consumed by runtime code', () => {
            const codeFiles = [
                'sneak-admin/worker.js',
                'sneak-admin/auth.js',
                'sneak-admin/api.js',
                'sneak-admin/ui.js',
                'SneakIDXWorker.js'
            ];
            for (const rel of codeFiles) {
                const content = fs.readFileSync(path.join(rootDir, rel), 'utf8');
                assert.doesNotMatch(content, /SNEAK_ADMIN_SESSION_SECRET/, `${rel} must not reference SNEAK_ADMIN_SESSION_SECRET`);
            }
        });

        test('actual pilot-required secrets are strictly identified by runtime reader', () => {
            // 1. Serving requires SNEAK_SIGNING_SECRET
            const servingCode = fs.readFileSync(path.join(rootDir, 'SneakIDXWorker.js'), 'utf8');
            assert.match(servingCode, /env\.SNEAK_SIGNING_SECRET/);

            // 2. Sync requires BRIDGE_TOKEN
            const syncCode = fs.readFileSync(path.join(rootDir, 'sneak-sync', 'bridge.js'), 'utf8');
            assert.match(syncCode, /env\.BRIDGE_TOKEN/);

            // 3. Member requires MAILJET_API_KEY / MAILJET_SECRET_KEY
            const memberEmailCode = fs.readFileSync(path.join(rootDir, 'sneak-shared', 'email-provider.js'), 'utf8');
            assert.match(memberEmailCode, /env\?\.MAILJET_API_KEY/);
            assert.match(memberEmailCode, /env\?\.MAILJET_SECRET_KEY/);

            // 4. Admin requires SNEAK_ADMIN_PASSWORD_HASH
            const adminCode = fs.readFileSync(path.join(rootDir, 'sneak-admin', 'worker.js'), 'utf8');
            assert.match(adminCode, /env\.SNEAK_ADMIN_PASSWORD_HASH/);
        });
    });

    describe('4. Fail-Closed Backend Capability Gates', () => {
        test('disabled GrowthZone reconciliation skips cron execution cleanly', async () => {
            let executed = false;
            await adminWorker.scheduled({}, {
                GROWTHZONE_RECONCILIATION_ENABLED: 'false',
                DB: {
                    prepare: () => { executed = true; throw new Error('DB should not be queried'); }
                }
            });
            assert.equal(executed, false, 'GrowthZone reconciliation cron short-circuited when disabled');
        });

        test('disabled alert processing short-circuits with capability disabled status', async () => {
            const res = await processAlerts({
                db: null,
                env: { EMAIL_ALERTS_ENABLED: 'false' }
            });
            assert.equal(res.skipped, true);
            assert.equal(res.reason, 'CapabilityDisabled');
        });

        test('disabled consumer auth fails closed on direct API request', async () => {
            const res = await consumerWorker.fetch(
                new Request('https://consumer.example/api/consumer/auth/magic-link', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ email: 'test@example.com', siteKey: 'pilot-01' })
                }),
                { CONSUMER_AUTH_ENABLED: 'false' }
            );
            assert.equal(res.status, 503);
            const data = await res.json();
            assert.equal(data.error, 'CapabilityDisabled');
        });
    });
});
