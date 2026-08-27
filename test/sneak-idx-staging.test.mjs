/**
 * test/sneak-idx-staging.test.mjs
 * 
 * Comprehensive Test Suite for SNEAK IDX Phase 2.2:
 * Pre-Staging Isolation, Auth Hardening, Session Enforcement & Clean Migrations.
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';
import worker from '../SneakIDXWorker.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

// Helper to create mock D1 database
function createMockDB() {
    const sites = [
        {
            id: 'site_demo_ccor',
            account_id: 'acc_demo_ccor',
            site_key: 'demo-ccor',
            site_name: 'Premier Coast Realty',
            status: 'active',
            scope_type: 'market',
            scope_value: null,
            account_name: 'Premier Coast Realty Demo',
            account_status: 'active',
            plan: 'pro',
            default_agent_mls_id: 'DEMO_AGENT_01',
            default_office_mls_id: 'DEMO_OFFICE_01',
            display_name: 'Premier Coast Realty',
            brokerage: 'Premier Coast Realty LLC',
            logo_url: '',
            agent_photo_url: '',
            primary_color: '#1a365d',
            secondary_color: '#0284c7',
            phone: '(239) 555-0199',
            email: 'team@premiercoastrealty.demo',
            website_url: 'https://premiercoastrealty.demo',
            branding_config: '{}'
        },
        {
            id: 'site_other',
            account_id: 'acc_other',
            site_key: 'other-site',
            site_name: 'Other Realty',
            status: 'active',
            scope_type: 'agent',
            scope_value: 'AGENT_99',
            account_name: 'Other Realty Demo',
            account_status: 'active',
            plan: 'standard',
            default_agent_mls_id: 'AGENT_99',
            default_office_mls_id: null,
            display_name: 'Other Agent',
            brokerage: 'Other Brokerage',
            logo_url: '',
            agent_photo_url: '',
            primary_color: '#000000',
            secondary_color: '#ffffff',
            phone: '(239) 555-9999',
            email: 'agent99@other.demo',
            website_url: 'https://other.demo',
            branding_config: '{}'
        }
    ];

    const domains = [
        { id: 'd1', site_id: 'site_demo_ccor', domain: 'localhost', verified: 1, status: 'active' },
        { id: 'd2', site_id: 'site_demo_ccor', domain: '127.0.0.1', verified: 1, status: 'active' },
        { id: 'd3', site_id: 'site_demo_ccor', domain: 'preview.sneakidx.com', verified: 1, status: 'active' },
        { id: 'd4', site_id: 'site_demo_ccor', domain: '*.realtorjohn.com', verified: 1, status: 'active' }
    ];

    const listings = [
        {
            ListingKey: '22400101',
            ListingId: '22400101',
            ListPrice: 1250000,
            OriginalListPrice: 1295000,
            UnparsedAddress: '26744 Hickory Blvd',
            City: 'Bonita Springs',
            StateOrProvince: 'FL',
            PostalCode: '34134',
            CountyOrParish: 'Lee',
            BedroomsTotal: 4,
            BathroomsTotalInteger: 3,
            LivingArea: 2850,
            StandardStatus: 'Active',
            PropertyType: 'Residential',
            PropertySubType: 'Single Family Residence',
            PrimaryPhoto: 'https://example.com/photo1.jpg',
            ListingContractDate: '2026-05-01',
            ModificationTimestamp: '2026-05-10T12:00:00Z',
            Latitude: 26.3382,
            Longitude: -81.8214,
            YearBuilt: 2021,
            LotSizeAcres: 0.35,
            ListAgentFullName: 'Sarah Jenkins',
            ListOfficeName: 'Premier Coast Realty LLC',
            ListOfficePhone: '(239) 555-0199',
            ListAgentMlsId: 'DEMO_AGENT_01',
            ListOfficeMlsId: 'DEMO_OFFICE_01',
            SubdivisionName: 'Bonita Beach',
            Zoning: 'RS-1',
            InternetAddressDisplayYN: 1,
            InternetEntireListingDisplayYN: 1
        }
    ];

    return {
        prepare(sql) {
            return {
                bind(...args) {
                    return {
                        async first() {
                            if (sql.includes('FROM sneak_sites') && (sql.includes('site_key = ?') || sql.includes('WHERE s.site_key = ?'))) {
                                const key = args[0];
                                const s = sites.find(x => x.site_key === key);
                                if (s) {
                                    return {
                                        id: s.id,
                                        site_id: s.id,
                                        account_id: s.account_id,
                                        site_key: s.site_key,
                                        site_name: s.site_name,
                                        site_status: s.status,
                                        scope_type: s.scope_type,
                                        scope_value: s.scope_value,
                                        account_name: s.account_name,
                                        account_status: s.account_status,
                                        plan: s.plan,
                                        default_agent_mls_id: s.default_agent_mls_id,
                                        default_office_mls_id: s.default_office_mls_id,
                                        display_name: s.display_name,
                                        brokerage: s.brokerage,
                                        logo_url: s.logo_url,
                                        agent_photo_url: s.agent_photo_url,
                                        primary_color: s.primary_color,
                                        secondary_color: s.secondary_color,
                                        phone: s.phone,
                                        email: s.email,
                                        website_url: s.website_url,
                                        branding_config: s.branding_config
                                    };
                                }
                                return null;
                            }
                            if (sql.includes('SELECT id FROM sneak_sites WHERE site_key = ?')) {
                                const key = args[0];
                                const found = sites.find(s => s.site_key === key);
                                return found ? { id: found.id } : null;
                            }
                            if (sql.includes('SELECT COUNT(*) AS total FROM sneak_listings')) {
                                return { total: listings.length };
                            }
                            if (sql.includes('FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?)')) {
                                const key = args[0];
                                return listings.find(l => l.ListingKey === key || l.ListingId === key) || null;
                            }
                            return null;
                        },
                        async all() {
                            if (sql.includes('FROM sneak_domains WHERE site_id = ?')) {
                                const siteId = args[0];
                                const results = domains.filter(d => d.site_id === siteId && d.status === 'active' && d.verified === 1);
                                return { results };
                            }
                            if (sql.includes('FROM sneak_listings')) {
                                return { results: listings };
                            }
                            if (sql.includes('FROM sneak_widget_configs WHERE site_id = ?')) {
                                return { results: [] };
                            }
                            return { results: [] };
                        },
                        async run() {
                            return { success: true };
                        }
                    };
                }
            };
        },
        async batch() {
            return [];
        }
    };
}

describe('SNEAK IDX Phase 2.2 Test Suite', () => {
    const TEST_SIGNING_SECRET = 'test_random_signing_secret_key_1234567890';

    // TEST A: SNEAK_ENV=staging API request without session -> 401 SessionRequired
    test('TEST A: SNEAK_ENV=staging request without session returns 401 SessionRequired', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.staging/idx/v1/search?site=demo-ccor', {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 401);
        const data = await res.json();
        assert.equal(data.error, 'SessionRequired');
    });

    // TEST B: SNEAK_ENV=production API request without session -> 401 SessionRequired
    test('TEST B: SNEAK_ENV=production request without session returns 401 SessionRequired', async () => {
        const env = {
            SNEAK_ENV: 'production',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.prod/idx/v1/search?site=demo-ccor', {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 401);
        const data = await res.json();
        assert.equal(data.error, 'SessionRequired');
    });

    // TEST C: SNEAK_ENV=development API request without session -> 401 SessionRequired
    test('TEST C: SNEAK_ENV=development request without session returns 401 SessionRequired', async () => {
        const env = {
            SNEAK_ENV: 'development',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('http://localhost:8787/idx/v1/search?site=demo-ccor', {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 401);
        const data = await res.json();
        assert.equal(data.error, 'SessionRequired');
    });

    // TEST D: staging bootstrap with NO Origin -> 403 DomainNotAuthorized
    test('TEST D: staging bootstrap with NO Origin returns 403 DomainNotAuthorized', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 403);
        const data = await res.json();
        assert.equal(data.error, 'DomainNotAuthorized');
    });

    // TEST E: staging bootstrap with unauthorized Origin -> 403 DomainNotAuthorized
    test('TEST E: staging bootstrap with unauthorized Origin returns 403 DomainNotAuthorized', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://unauthorized-broker.com' }
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 403);
        const data = await res.json();
        assert.equal(data.error, 'DomainNotAuthorized');
    });

    // TEST F: staging bootstrap with verified Origin -> 200 + valid session token
    test('TEST F: staging bootstrap with verified Origin returns 200 and signed session token', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.success, true);
        assert.ok(data.session && typeof data.session === 'string');
        assert.equal(data.siteKey, 'demo-ccor');

        // Test wildcard subdomain authorization (*.realtorjohn.com)
        const reqSub = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://agent.realtorjohn.com' }
        });
        const resSub = await worker.fetch(reqSub, env);
        assert.equal(resSub.status, 200);
    });

    // TEST G: SNEAK_SIGNING_SECRET missing in staging -> controlled 500 ConfigurationError, no default secret
    test('TEST G: SNEAK_SIGNING_SECRET missing returns 500 ConfigurationError', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            DB: createMockDB()
            // SNEAK_SIGNING_SECRET omitted
        };
        const req = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 500);
        const data = await res.json();
        assert.equal(data.error, 'ConfigurationError');
    });

    // TEST H: tampered session token -> 401 InvalidSession
    test('TEST H: tampered session token returns 401 InvalidSession', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        // Bootstrap to get a valid token
        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        // Tamper with the session token
        const tamperedSession = session.slice(0, -5) + 'XXXXX';
        const searchReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${tamperedSession}`, {
            method: 'GET'
        });
        const searchRes = await worker.fetch(searchReq, env);
        assert.equal(searchRes.status, 401);
        const data = await searchRes.json();
        assert.equal(data.error, 'InvalidSession');
    });

    // TEST I: expired session token -> 401 InvalidSession
    test('TEST I: expired session token returns 401 InvalidSession', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        // Create an expired token manually using HMAC
        const header = { alg: 'HS256', typ: 'SNEAK-SESSION' };
        const payload = {
            siteKey: 'demo-ccor',
            siteId: 'site_demo_ccor',
            origin: 'preview.sneakidx.com',
            iat: Math.floor(Date.now() / 1000) - 3600,
            exp: Math.floor(Date.now() / 1000) - 1800 // Expired 30 mins ago
        };

        const enc = new TextEncoder();
        const base64UrlEncode = (str) => btoa(str).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
        const encodedHeader = base64UrlEncode(JSON.stringify(header));
        const encodedPayload = base64UrlEncode(JSON.stringify(payload));
        const dataToSign = `${encodedHeader}.${encodedPayload}`;

        const key = await crypto.subtle.importKey(
            'raw',
            enc.encode(TEST_SIGNING_SECRET),
            { name: 'HMAC', hash: 'SHA-256' },
            false,
            ['sign']
        );
        const sigBuffer = await crypto.subtle.sign('HMAC', key, enc.encode(dataToSign));
        const sigBytes = new Uint8Array(sigBuffer);
        let binary = '';
        for (let i = 0; i < sigBytes.byteLength; i++) binary += String.fromCharCode(sigBytes[i]);
        const encodedSig = base64UrlEncode(binary);
        const expiredToken = `${dataToSign}.${encodedSig}`;

        const req = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${expiredToken}`, {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 401);
        const data = await res.json();
        assert.equal(data.error, 'InvalidSession');
    });

    // TEST J: Site A token used on Site B -> 403 SessionMismatch
    test('TEST J: Site A session token used on Site B returns 403 SessionMismatch', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        // Bootstrap for demo-ccor
        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        // Use demo-ccor token on other-site
        const searchReq = new Request(`https://sneak.staging/idx/v1/search?site=other-site&session=${session}`, {
            method: 'GET'
        });
        const searchRes = await worker.fetch(searchReq, env);
        assert.equal(searchRes.status, 403);
        const data = await searchRes.json();
        assert.equal(data.error, 'SessionMismatch');
    });

    // TEST K: Migration files contain no legacy tables/indexes
    test('TEST K: Migration files contain no CREATE TABLE listings, open_houses, or idx_listings_*', () => {
        const migrationsDir = path.join(rootDir, 'migrations');
        const files = fs.readdirSync(migrationsDir).filter(f => f.endsWith('.sql'));

        for (const file of files) {
            const content = fs.readFileSync(path.join(migrationsDir, file), 'utf8');
            assert.equal(content.includes('CREATE TABLE listings'), false, `File ${file} creates legacy listings table`);
            assert.equal(content.includes('CREATE TABLE IF NOT EXISTS listings ('), false, `File ${file} creates legacy listings table`);
            assert.equal(content.includes('CREATE TABLE open_houses'), false, `File ${file} creates legacy open_houses table`);
            assert.equal(content.includes('CREATE TABLE IF NOT EXISTS open_houses ('), false, `File ${file} creates legacy open_houses table`);
            assert.equal(content.includes('idx_listings_'), false, `File ${file} creates legacy listings index`);
        }
    });

    // TEST L: Legacy project files remain zero-diff
    test('TEST L: Legacy project files (home-search/index.html, ListingsWorker.js, wrangler.toml) have zero diff', () => {
        const diff = execSync('git diff origin/main -- home-search/index.html ListingsWorker.js wrangler.toml', {
            cwd: rootDir,
            encoding: 'utf8'
        });
        assert.equal(diff.trim(), '', 'Legacy files must have zero diff against main');
    });

    // TEST M: Bridge probe script with missing token reports TOKEN UNAVAILABLE
    test('TEST M: Bridge probe utility with missing token exits cleanly and reports TOKEN UNAVAILABLE', () => {
        const output = execSync('node scripts/probe-bridge.mjs', {
            cwd: rootDir,
            env: { ...process.env, BRIDGE_TOKEN: '' },
            encoding: 'utf8'
        });
        assert.ok(output.includes('BRIDGE PROBE NOT RUN — TOKEN UNAVAILABLE'), 'Probe must report token unavailable');
    });

    // CORS TESTS
    test('CORS: OPTIONS /idx/v1/bootstrap with authorized origin returns exact origin', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'OPTIONS',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 204);
        assert.equal(res.headers.get('Access-Control-Allow-Origin'), 'https://preview.sneakidx.com');
    });

    test('CORS: OPTIONS /idx/v1/bootstrap with unauthorized origin returns 403', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'OPTIONS',
            headers: { Origin: 'https://evil-unauthorized.com' }
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 403);
    });

    test('CORS: OPTIONS /idx/v1/search does not return wildcard * for unauthorized cross-origin', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };
        const req = new Request('https://sneak.staging/idx/v1/search?site=demo-ccor', {
            method: 'OPTIONS',
            headers: { Origin: 'https://random-attacker.com' }
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 403);
        assert.notEqual(res.headers.get('Access-Control-Allow-Origin'), '*');
    });

    test('Authenticated Search and Map queries with valid session succeed', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        // Bootstrap
        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        // Search
        const sReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}`, {
            method: 'GET'
        });
        const sRes = await worker.fetch(sReq, env);
        assert.equal(sRes.status, 200);
        const sData = await sRes.json();
        assert.ok(Array.isArray(sData.data));
        assert.equal(sData.data.length, 1);

        // Map
        const mReq = new Request(`https://sneak.staging/idx/v1/map?site=demo-ccor&session=${session}&north=27&south=26&east=-81&west=-82`, {
            method: 'GET'
        });
        const mRes = await worker.fetch(mReq, env);
        assert.equal(mRes.status, 200);
        const mData = await mRes.json();
        assert.ok(Array.isArray(mData.data));
    });

    // PHASE 7.3A SEARCH PARITY TESTS
    test('PHASE 7.3A: Commercial and Land property category queries execute successfully', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        // Commercial search
        const commReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&propertyType=commercial`, {
            method: 'GET'
        });
        const commRes = await worker.fetch(commReq, env);
        assert.equal(commRes.status, 200);

        // Land search
        const landReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&propertyType=land`, {
            method: 'GET'
        });
        const landRes = await worker.fetch(landReq, env);
        assert.equal(landRes.status, 200);

        // Rental search
        const rentalReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&propertyType=rental`, {
            method: 'GET'
        });
        const rentalRes = await worker.fetch(rentalReq, env);
        assert.equal(rentalRes.status, 200);
    });

    test('PHASE 7.3A: Advanced Wave-1 and More Filters parameters parse cleanly', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const advUrl = `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}` +
            `&minSqft=2000&maxSqft=5000&minAcres=0.5&maxAcres=5&minYear=2015&maxYear=2025` +
            `&waterfront=1&pool=1&garage=2&newConstruction=1&openHouse=1&priceReduced=1` +
            `&subdivision=Pelican+Landing&county=Lee&postalCode=34134&sort=sqftDesc`;

        const advReq = new Request(advUrl, { method: 'GET' });
        const advRes = await worker.fetch(advReq, env);
        assert.equal(advRes.status, 200);
        const advData = await advRes.json();
        assert.ok(advData.data);
    });

    test('PHASE 7.3A: Sort parameters (priceAsc, priceDesc, sqftDesc, acresDesc, yearDesc) are accepted', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const sorts = ['dateDesc', 'priceAsc', 'priceDesc', 'sqftDesc', 'acresDesc', 'yearDesc'];
        for (const s of sorts) {
            const req = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&sort=${s}`, {
                method: 'GET'
            });
            const res = await worker.fetch(req, env);
            assert.equal(res.status, 200, `Sort ${s} failed`);
        }
    });

    // PHASE 7.3B1 INTERACTIVE MAP & VIEWPORT SYNCHRONIZATION TESTS
    test('PHASE 7.3B1: /idx/v1/map returns full context-aware marker payload with truncation metadata', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const mapReq = new Request(`https://sneak.staging/idx/v1/map?site=demo-ccor&session=${session}&limit=500`, {
            method: 'GET'
        });
        const mapRes = await worker.fetch(mapReq, env);
        assert.equal(mapRes.status, 200);
        const mapData = await mapRes.json();

        assert.equal(typeof mapData.truncated, 'boolean');
        assert.equal(typeof mapData.count, 'number');
        assert.equal(typeof mapData.limit, 'number');
        assert.ok(Array.isArray(mapData.data));

        if (mapData.data.length > 0) {
            const m = mapData.data[0];
            assert.ok('ListingKey' in m);
            assert.ok('ListingId' in m);
            assert.ok('ListPrice' in m);
            assert.ok('UnparsedAddress' in m);
            assert.ok('City' in m);
            assert.ok('PropertyType' in m);
            assert.ok('Latitude' in m);
            assert.ok('Longitude' in m);
            assert.ok('BedroomsTotal' in m);
            assert.ok('BathroomsTotalInteger' in m);
            assert.ok('LivingArea' in m);
            assert.ok('LotSizeAcres' in m);
            assert.ok('SubdivisionName' in m);
            assert.ok('YearBuilt' in m);
            assert.ok('Zoning' in m);
            assert.ok('ListOfficeName' in m);
        }
    });

    test('PHASE 7.3B1: Viewport bounding box parameters (north, south, east, west) are accepted on /search and /map', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        // Test bounded /search
        const boundedSearchReq = new Request(
            `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&north=26.40&south=26.30&east=-81.70&west=-81.90`,
            { method: 'GET' }
        );
        const boundedSearchRes = await worker.fetch(boundedSearchReq, env);
        assert.equal(boundedSearchRes.status, 200);
        const searchData = await boundedSearchRes.json();
        assert.ok(Array.isArray(searchData.data));

        // Test bounded /map
        const boundedMapReq = new Request(
            `https://sneak.staging/idx/v1/map?site=demo-ccor&session=${session}&north=26.40&south=26.30&east=-81.70&west=-81.90`,
            { method: 'GET' }
        );
        const boundedMapRes = await worker.fetch(boundedMapReq, env);
        assert.equal(boundedMapRes.status, 200);
        const mapData = await boundedMapRes.json();
        assert.ok(Array.isArray(mapData.data));
    });

    test('PHASE 7.3B1: Category-specific /map queries succeed for sale, rental, commercial, and land', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const categories = ['sale', 'rental', 'commercial', 'land'];
        for (const cat of categories) {
            const req = new Request(`https://sneak.staging/idx/v1/map?site=demo-ccor&session=${session}&propertyType=${cat}`, {
                method: 'GET'
            });
            const res = await worker.fetch(req, env);
            assert.equal(res.status, 200, `Map query for ${cat} failed`);
        }
    });

    test('PHASE 7.3B1: Out-of-bounds coordinates fail safely without crashing SQL execution', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        // Invalid latitudes > 90 and longitudes > 180
        const req = new Request(`https://sneak.staging/idx/v1/map?site=demo-ccor&session=${session}&north=105&south=-95&east=200&west=-200`, {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
    });

    // PHASE 7.3B2A TESTS: Radius Search + Near Me + Spatial State + Build Version
    test('PHASE 7.3B2A: Valid radius query on /idx/v1/search succeeds with centerLat, centerLng, radiusMiles', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const req = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&centerLat=26.34&centerLng=-81.78&radiusMiles=5`, {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.ok(Array.isArray(data.data));
    });

    test('PHASE 7.3B2A: Valid radius query on /idx/v1/map succeeds with centerLat, centerLng, radiusMiles', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const req = new Request(`https://sneak.staging/idx/v1/map?site=demo-ccor&session=${session}&centerLat=26.34&centerLng=-81.78&radiusMiles=10`, {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.ok(Array.isArray(data.data));
    });

    test('PHASE 7.3B2A: Incomplete or out-of-range radius queries return HTTP 400 InvalidSpatialFilter', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const testUrls = [
            `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&centerLat=26.34&centerLng=-81.78&radiusMiles=100`, // radius > 50
            `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&centerLat=26.34&centerLng=-81.78&radiusMiles=-5`,  // negative radius
            `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&centerLat=95.0&centerLng=-81.78&radiusMiles=5`,   // invalid lat
            `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&centerLat=26.34&centerLng=200.0&radiusMiles=5`,   // invalid lng
            `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&centerLat=26.34`,                                 // missing lng and radius
            `https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&centerLat=26.34&centerLng=-81.78`                  // missing radius
        ];

        for (const url of testUrls) {
            const req = new Request(url, { method: 'GET' });
            const res = await worker.fetch(req, env);
            assert.equal(res.status, 400, `Expected HTTP 400 for ${url}, got ${res.status}`);
            const data = await res.json();
            assert.equal(data.error, 'InvalidSpatialFilter');
        }
    });

    test('PHASE 7.3B2A: Radius search functions across all property categories (sale, rental, commercial, land)', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        for (const cat of ['sale', 'rental', 'commercial', 'land']) {
            const req = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&propertyType=${cat}&centerLat=26.34&centerLng=-81.78&radiusMiles=5`, {
                method: 'GET'
            });
            const res = await worker.fetch(req, env);
            assert.equal(res.status, 200, `Radius search failed for category ${cat}`);
            const data = await res.json();
            assert.ok(Array.isArray(data.data));
        }
    });

    // PHASE 7.3B2B TESTS: Server-Authoritative Polygon Search + Drawing State
    const validTestPolygon = {
        type: "Polygon",
        coordinates: [
            [
                [-81.82, 26.35],
                [-81.78, 26.36],
                [-81.76, 26.32],
                [-81.81, 26.30],
                [-81.82, 26.35]
            ]
        ]
    };

    test('PHASE 7.3B2B: Valid polygon query on /idx/v1/search succeeds with GeoJSON polygon', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const polygonParam = encodeURIComponent(JSON.stringify(validTestPolygon));
        const req = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&polygon=${polygonParam}`, {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.ok(Array.isArray(data.data));
        assert.ok(data.pagination);
    });

    test('PHASE 7.3B2B: Valid polygon query on /idx/v1/map succeeds with GeoJSON polygon', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const polygonParam = encodeURIComponent(JSON.stringify(validTestPolygon));
        const req = new Request(`https://sneak.staging/idx/v1/map?site=demo-ccor&session=${session}&polygon=${polygonParam}`, {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.ok(Array.isArray(data.data));
    });

    test('PHASE 7.3B2B: Malformed or invalid polygon queries return HTTP 400 InvalidSpatialFilter', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        // 1. Invalid JSON
        const invJsonReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&polygon=not-json`, { method: 'GET' });
        const invJsonRes = await worker.fetch(invJsonReq, env);
        assert.equal(invJsonRes.status, 400);

        // 2. Wrong GeoJSON type
        const wrongType = encodeURIComponent(JSON.stringify({ type: 'Point', coordinates: [-81.8, 26.3] }));
        const wrongTypeReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&polygon=${wrongType}`, { method: 'GET' });
        const wrongTypeRes = await worker.fetch(wrongTypeReq, env);
        assert.equal(wrongTypeRes.status, 400);

        // 3. Fewer than 3 unique vertices
        const fewPoints = encodeURIComponent(JSON.stringify({ type: 'Polygon', coordinates: [[[-81.8, 26.3], [-81.7, 26.4], [-81.8, 26.3]]] }));
        const fewPointsReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&polygon=${fewPoints}`, { method: 'GET' });
        const fewPointsRes = await worker.fetch(fewPointsReq, env);
        assert.equal(fewPointsRes.status, 400);

        // 4. More than 40 vertices limit
        const excessCoords = [];
        for (let i = 0; i < 45; i++) {
            excessCoords.push([-81.8 + i * 0.001, 26.3 + (i % 2) * 0.001]);
        }
        excessCoords.push(excessCoords[0]);
        const excessPolygon = encodeURIComponent(JSON.stringify({ type: 'Polygon', coordinates: [excessCoords] }));
        const excessReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&polygon=${excessPolygon}`, { method: 'GET' });
        const excessRes = await worker.fetch(excessReq, env);
        assert.equal(excessRes.status, 400);

        // 5. Out of bounds latitude / longitude
        const outOfBounds = encodeURIComponent(JSON.stringify({
            type: 'Polygon',
            coordinates: [[[-81.8, 95.0], [-81.7, 26.3], [-81.9, 26.2], [-81.8, 95.0]]]
        }));
        const oobReq = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&polygon=${outOfBounds}`, { method: 'GET' });
        const oobRes = await worker.fetch(oobReq, env);
        assert.equal(oobRes.status, 400);
    });

    test('PHASE 7.3B2B: Polygon search functions across all property categories (sale, rental, commercial, land)', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const polygonParam = encodeURIComponent(JSON.stringify(validTestPolygon));
        for (const cat of ['sale', 'rental', 'commercial', 'land']) {
            const req = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&propertyType=${cat}&polygon=${polygonParam}`, {
                method: 'GET'
            });
            const res = await worker.fetch(req, env);
            assert.equal(res.status, 200, `Polygon search failed for category ${cat}`);
            const data = await res.json();
            assert.ok(Array.isArray(data.data));
        }
    });

    test('PHASE 7.3B2B: Polygon search combined with advanced filters (price, waterfront, beds)', async () => {
        const env = {
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET,
            DB: createMockDB()
        };

        const bReq = new Request('https://sneak.staging/idx/v1/bootstrap?site=demo-ccor', {
            method: 'GET',
            headers: { Origin: 'https://preview.sneakidx.com' }
        });
        const bRes = await worker.fetch(bReq, env);
        const { session } = await bRes.json();

        const polygonParam = encodeURIComponent(JSON.stringify(validTestPolygon));
        const req = new Request(`https://sneak.staging/idx/v1/search?site=demo-ccor&session=${session}&polygon=${polygonParam}&minPrice=300000&maxPrice=1000000&waterfront=1&beds=3`, {
            method: 'GET'
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.ok(Array.isArray(data.data));
    });

    test('PHASE 7.3B2B: Frontend build versions are uniformly bumped to 2026.08.27.7.3b2b', () => {
        const searchHtml = fs.readFileSync(path.join(rootDir, 'sneak-idx/search/index.html'), 'utf8');
        const embedJs = fs.readFileSync(path.join(rootDir, 'sneak-idx/embed.js'), 'utf8');

        assert.ok(searchHtml.includes('data-ui-build="2026.08.27.7.3b2b"'), 'search/index.html must have data-ui-build="2026.08.27.7.3b2b"');
        assert.ok(searchHtml.includes("CCOR_IDX_UI_BUILD = '2026.08.27.7.3b2b'"), "search/index.html must have CCOR_IDX_UI_BUILD = '2026.08.27.7.3b2b'");
        assert.ok(embedJs.includes("const buildVersion = '2026.08.27.7.3b2b'"), "embed.js must have buildVersion = '2026.08.27.7.3b2b'");
        assert.ok(searchHtml.includes('id="drawToolBtn"'), 'search/index.html must have #drawToolBtn');
        assert.ok(searchHtml.includes('id="drawControlsBanner"'), 'search/index.html must have #drawControlsBanner');
    });
});
