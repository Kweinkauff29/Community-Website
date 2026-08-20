/**
 * test/sneak-sites-worker.test.mjs
 * 
 * SNEAK Website Engine & Template System Test Suite (Phase 6):
 * - Preview Token Cryptographic Verification & Tamper Resistance
 * - 3 Multi-Tenant Templates (Essential, Coastal, Brokerage)
 * - Common Page Routes & Sub-Page Dispatching
 * - Scoped MLS Ingestion & Deterministic Ordering
 * - Lead Submission & Spam / Honeypot Defense
 * - Service Entitlement & Suspension Enforcement
 * - SEO Metadata & Preview Noindex Enforcement
 * - Cross-Tenant Isolation
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import worker from '../sneak-sites/worker.js';
import { createPreviewToken, verifyPreviewToken } from '../sneak-sites/preview.js';
import { resolveTenantSite, getFeaturedListings, getUpcomingOpenHouses } from '../sneak-sites/data.js';

const TEST_PREVIEW_SECRET = 'test_secret_for_sneak_website_preview_2026';

function createMockSitesDB() {
    const tables = {
        sneak_accounts: [
            { id: 'acc_agent_a', account_name: 'Agent A Realty', status: 'active', plan: 'pro' },
            { id: 'acc_agent_b', account_name: 'Agent B Coastal', status: 'active', plan: 'pro' },
            { id: 'acc_office_c', account_name: 'Premier Brokerage C', status: 'active', plan: 'pro' },
            { id: 'acc_suspended', account_name: 'Suspended Realty', status: 'suspended', plan: 'pro' }
        ],
        sneak_sites: [
            { id: 'site_a', account_id: 'acc_agent_a', site_key: 'site-agent-a', site_name: 'Agent A Site', scope_type: 'agent', scope_value: 'B3650316', status: 'active' },
            { id: 'site_b', account_id: 'acc_agent_b', site_key: 'site-agent-b', site_name: 'Agent B Site', scope_type: 'agent', scope_value: 'B3650316', status: 'active' },
            { id: 'site_c', account_id: 'acc_office_c', site_key: 'site-office-c', site_name: 'Office C Site', scope_type: 'office', scope_value: 'BPRI', status: 'active' },
            { id: 'site_susp', account_id: 'acc_suspended', site_key: 'site-suspended', site_name: 'Suspended Site', scope_type: 'market', scope_value: null, status: 'active' }
        ],
        sneak_branding: [
            { site_id: 'site_a', display_name: 'Sarah Agent', brokerage: 'Bonita Springs Realty', primary_color: '#1e3a8a', secondary_color: '#0284c7', phone: '(239) 555-0111', email: 'sarah@realty.com' },
            { site_id: 'site_b', display_name: 'David Coastal', brokerage: 'Naples Luxury Realty', primary_color: '#0f172a', secondary_color: '#38bdf8', phone: '(239) 555-0222', email: 'david@luxury.com' },
            { site_id: 'site_c', display_name: 'Premier Brokerage Team', brokerage: 'Gulf Premier Properties', primary_color: '#064e3b', secondary_color: '#10b981', phone: '(239) 555-0333', email: 'info@gulfpremier.com' }
        ],
        sneak_website_configs: [
            {
                site_id: 'site_a',
                enabled: 1,
                template_key: 'essential',
                site_title: 'Sarah Agent | Southwest Florida Homes',
                tagline: 'Your Trusted Real Estate Advisor',
                hero_heading: 'Find Your Place in Southwest Florida',
                hero_subheading: 'Explore luxury communities and golf estates.',
                hero_image_url: 'https://preview.sneakidx.com/hero1.jpg',
                about_heading: 'About Sarah Agent',
                about_body: 'Serving home buyers and sellers for over a decade.',
                featured_areas_json: JSON.stringify([{ name: 'Bonita Springs', description: 'Beaches & Golf', filter: 'Bonita Springs', image_url: 'https://preview.sneakidx.com/bonita.jpg' }]),
                seo_title: 'Sarah Agent - SWFL Real Estate',
                seo_description: 'Active SWFL MLS properties and open houses.',
                footer_text: '© 2026 Sarah Agent. All rights reserved.'
            },
            {
                site_id: 'site_b',
                enabled: 1,
                template_key: 'coastal',
                site_title: 'David Coastal | Luxury Residences',
                tagline: 'Curated Naples & Bonita Coastal Living',
                hero_heading: 'Southwest Florida Coastal Residences',
                hero_subheading: 'Waterfront estates and luxury enclaves.',
                hero_image_url: 'https://preview.sneakidx.com/hero2.jpg',
                about_heading: 'David Coastal Story',
                about_body: 'Specializing in private beachfront and golf access properties.',
                featured_areas_json: JSON.stringify([{ name: 'Naples', description: 'World-class dining & beaches', filter: 'Naples', image_url: 'https://preview.sneakidx.com/naples.jpg' }]),
                seo_title: 'David Coastal - Luxury Residences',
                seo_description: 'Luxury residences and open houses in Naples.',
                footer_text: '© 2026 David Coastal.'
            },
            {
                site_id: 'site_c',
                enabled: 1,
                template_key: 'brokerage',
                site_title: 'Gulf Premier Properties | Office MLS Search',
                tagline: 'Leading Southwest Florida Real Estate Team',
                hero_heading: 'Comprehensive Brokerage Representation',
                hero_subheading: 'Serving all Southwest Florida markets.',
                hero_image_url: 'https://preview.sneakidx.com/hero3.jpg',
                about_heading: 'Our Brokerage Team',
                about_body: 'Full-service real estate brokerage representing premier Southwest Florida properties.',
                featured_areas_json: JSON.stringify([{ name: 'Estero', description: 'Estero Bay living', filter: 'Estero', image_url: 'https://preview.sneakidx.com/estero.jpg' }]),
                seo_title: 'Gulf Premier Properties - Brokerage Search',
                seo_description: 'Search office listings and agent open houses.',
                footer_text: '© 2026 Gulf Premier Properties.'
            }
        ],
        sneak_domains: [
            { site_id: 'site_a', domain: 'sarahagent.com', verified: 1, status: 'active' },
            { site_id: 'site_b', domain: 'davidcoastal.com', verified: 1, status: 'active' },
            { site_id: 'site_c', domain: 'gulfpremier.com', verified: 1, status: 'active' }
        ],
        sneak_account_entitlements: [
            { account_id: 'acc_agent_a', status: 'active', plan: 'pro', grace_until: null },
            { account_id: 'acc_agent_b', status: 'grace', plan: 'pro', grace_until: '2026-12-31T23:59:59Z' },
            { account_id: 'acc_office_c', status: 'active', plan: 'pro', grace_until: null }
        ],
        sneak_listings: [
            { ListingKey: 'list_1', ListAgentMlsId: 'B3650316', ListOfficeMlsId: 'BPRI', ListPrice: 850000, StandardStatus: 'Active', UnparsedAddress: '101 Bay View DR', City: 'Bonita Springs', StateOrProvince: 'FL', ModificationTimestamp: '2026-08-20T12:00:00Z' },
            { ListingKey: 'list_2', ListAgentMlsId: 'B3650316', ListOfficeMlsId: 'BPRI', ListPrice: 1250000, StandardStatus: 'Active', UnparsedAddress: '202 Gulf Coast WAY', City: 'Naples', StateOrProvince: 'FL', ModificationTimestamp: '2026-08-20T11:00:00Z' },
            { ListingKey: 'list_3', ListAgentMlsId: 'OTHER_AGENT', ListOfficeMlsId: 'BPRI', ListPrice: 650000, StandardStatus: 'Active', UnparsedAddress: '303 River OAKS', City: 'Estero', StateOrProvince: 'FL', ModificationTimestamp: '2026-08-20T10:00:00Z' }
        ],
        sneak_open_houses: [
            { OpenHouseKey: 'oh_1', ListingKey: 'list_1', OpenHouseDate: '2026-08-23', OpenHouseStartTime: '13:00', OpenHouseEndTime: '16:00' }
        ],
        sneak_leads: []
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
                    if (query.includes('FROM sneak_sites s') && query.includes('WHERE s.site_key = ?')) {
                        const s = tables.sneak_sites.find(x => x.site_key === boundArgs[0]);
                        if (!s) return { results: [] };
                        const a = tables.sneak_accounts.find(x => x.id === s.account_id);
                        const b = tables.sneak_branding.find(x => x.site_id === s.id);
                        const w = tables.sneak_website_configs.find(x => x.site_id === s.id);
                        const e = tables.sneak_account_entitlements.find(x => x.account_id === s.account_id);
                        return {
                            results: [{
                                site_id: s.id,
                                account_id: s.account_id,
                                site_key: s.site_key,
                                site_name: s.site_name,
                                scope_type: s.scope_type,
                                scope_value: s.scope_value,
                                site_status: s.status,
                                account_name: a?.account_name,
                                account_plan: a?.plan,
                                account_status: a?.status,
                                entitlement_status: e?.status || 'active',
                                grace_until: e?.grace_until,
                                entitlement_plan: e?.plan,
                                display_name: b?.display_name,
                                brokerage: b?.brokerage,
                                logo_url: b?.logo_url,
                                agent_photo_url: b?.agent_photo_url,
                                primary_color: b?.primary_color,
                                secondary_color: b?.secondary_color,
                                phone: b?.phone,
                                email: b?.email,
                                website_enabled: w?.enabled,
                                template_key: w?.template_key,
                                site_title: w?.site_title,
                                tagline: w?.tagline,
                                hero_heading: w?.hero_heading,
                                hero_subheading: w?.hero_subheading,
                                hero_image_url: w?.hero_image_url,
                                about_heading: w?.about_heading,
                                about_body: w?.about_body,
                                featured_areas_json: w?.featured_areas_json,
                                seo_title: w?.seo_title,
                                seo_description: w?.seo_description,
                                footer_text: w?.footer_text
                            }]
                        };
                    }
                    if (query.includes('FROM sneak_listings') && query.includes('ListAgentMlsId = ?')) {
                        return { results: tables.sneak_listings.filter(l => l.ListAgentMlsId === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_listings') && query.includes('ListOfficeMlsId = ?')) {
                        return { results: tables.sneak_listings.filter(l => l.ListOfficeMlsId === boundArgs[0]) };
                    }
                    if (query.includes('FROM sneak_open_houses o')) {
                        return { results: tables.sneak_open_houses.map(oh => {
                            const l = tables.sneak_listings.find(x => x.ListingKey === oh.ListingKey);
                            return { ...oh, ...l };
                        }) };
                    }
                    return { results: [] };
                },
                async run() {
                    if (query.includes('INSERT INTO sneak_leads')) {
                        const [id, site_id, client_name, email, phone, message, listing_key] = boundArgs;
                        tables.sneak_leads.push({ id, site_id, client_name, email, phone, message, listing_key, created_at: new Date().toISOString() });
                        return { success: true };
                    }
                    return { success: true };
                }
            };
        }
    };
}

describe('SNEAK Website Engine & Multi-Tenant Templates Suite (Phase 6)', () => {

    test('TEST 1: Preview Token Cryptographic Verification & Tamper Resistance', async () => {
        // 1. Valid Token
        const token = await createPreviewToken('site-agent-a', 'site_a', TEST_PREVIEW_SECRET, 1800);
        const validRes = await verifyPreviewToken(token, 'site-agent-a', TEST_PREVIEW_SECRET);
        assert.equal(validRes.valid, true);
        assert.equal(validRes.payload.siteKey, 'site-agent-a');

        // 2. Expired Token
        const expiredToken = await createPreviewToken('site-agent-a', 'site_a', TEST_PREVIEW_SECRET, -10);
        const expiredRes = await verifyPreviewToken(expiredToken, 'site-agent-a', TEST_PREVIEW_SECRET);
        assert.equal(expiredRes.valid, false);
        assert.ok(expiredRes.error.includes('expired'));

        // 3. Tampered Token Signature
        const tampered = token.slice(0, -5) + 'xxxxx';
        const tamperedRes = await verifyPreviewToken(tampered, 'site-agent-a', TEST_PREVIEW_SECRET);
        assert.equal(tamperedRes.valid, false);

        // 4. Site Key Mismatch (Cannot preview Tenant B using Tenant A token)
        const mismatchRes = await verifyPreviewToken(token, 'site-agent-b', TEST_PREVIEW_SECRET);
        assert.equal(mismatchRes.valid, false);
        assert.ok(mismatchRes.error.includes('mismatch'));
    });

    test('TEST 2: SNEAK Essential Template Rendering & Components', async () => {
        const mockDB = createMockSitesDB();
        const env = { SNEAK_ENV: 'staging', SNEAK_WEBSITE_PREVIEW_SECRET: TEST_PREVIEW_SECRET, DB: mockDB };
        const token = await createPreviewToken('site-agent-a', 'site_a', TEST_PREVIEW_SECRET, 1800);

        const res = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(res.status, 200);
        const html = await res.text();

        assert.ok(html.includes('PREVIEW MODE'));
        assert.ok(html.includes('Sarah Agent'));
        assert.ok(html.includes('Find Your Place in Southwest Florida'));
        assert.ok(html.includes('ess-hero-search'));
        assert.ok(html.includes('Featured Properties'));
        assert.ok(html.includes('101 Bay View DR'));
        assert.ok(html.includes('<meta name="robots" content="noindex, nofollow" />'));
        assert.ok(html.includes('IDX technology powered by <strong>SNEAK</strong>'));
    });

    test('TEST 3: SNEAK Coastal Template Rendering & Editorial Aesthetics', async () => {
        const mockDB = createMockSitesDB();
        const env = { SNEAK_ENV: 'staging', SNEAK_WEBSITE_PREVIEW_SECRET: TEST_PREVIEW_SECRET, DB: mockDB };
        const token = await createPreviewToken('site-agent-b', 'site_b', TEST_PREVIEW_SECRET, 1800);

        const res = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-b/?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(res.status, 200);
        const html = await res.text();

        assert.ok(html.includes('David Coastal'));
        assert.ok(html.includes('Southwest Florida Coastal Residences'));
        assert.ok(html.includes('cst-header-trans'));
        assert.ok(html.includes('Selected Coastal Residences'));
        assert.ok(html.includes('Naples'));
    });

    test('TEST 4: SNEAK Brokerage Template Rendering & Office Scope', async () => {
        const mockDB = createMockSitesDB();
        const env = { SNEAK_ENV: 'staging', SNEAK_WEBSITE_PREVIEW_SECRET: TEST_PREVIEW_SECRET, DB: mockDB };
        const token = await createPreviewToken('site-office-c', 'site_c', TEST_PREVIEW_SECRET, 1800);

        const res = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-office-c/?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(res.status, 200);
        const html = await res.text();

        assert.ok(html.includes('Gulf Premier Properties'));
        assert.ok(html.includes('brk-topbar'));
        assert.ok(html.includes('Comprehensive Brokerage Representation'));
        assert.ok(html.includes('Brokerage Exclusive &amp; Office Listings'));
        assert.ok(html.includes('Client Services'));
    });

    test('TEST 5: Common Page Routes (/search, /open-houses, /about, /contact)', async () => {
        const mockDB = createMockSitesDB();
        const env = { SNEAK_ENV: 'staging', SNEAK_WEBSITE_PREVIEW_SECRET: TEST_PREVIEW_SECRET, DB: mockDB };
        const token = await createPreviewToken('site-agent-a', 'site_a', TEST_PREVIEW_SECRET, 1800);

        // /search
        const searchRes = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/search?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(searchRes.status, 200);
        const searchHtml = await searchRes.text();
        assert.ok(searchHtml.includes('sneak-idx-root'));
        assert.ok(searchHtml.includes('embed.js'));

        // /open-houses
        const ohRes = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/open-houses?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(ohRes.status, 200);

        // /about
        const aboutRes = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/about?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(aboutRes.status, 200);
        const aboutHtml = await aboutRes.text();
        assert.ok(aboutHtml.includes('About Sarah Agent'));

        // /contact
        const contactRes = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/contact?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(contactRes.status, 200);
        const contactHtml = await contactRes.text();
        assert.ok(contactHtml.includes('submitContactForm'));
    });

    test('TEST 6: Public Lead Submission & Honeypot Spam Defense', async () => {
        const mockDB = createMockSitesDB();
        const env = { SNEAK_ENV: 'staging', SNEAK_WEBSITE_PREVIEW_SECRET: TEST_PREVIEW_SECRET, DB: mockDB };
        const token = await createPreviewToken('site-agent-a', 'site_a', TEST_PREVIEW_SECRET, 1800);

        // 1. Legitimate Contact Lead
        const leadRes = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/api/contact?token=${encodeURIComponent(token)}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: 'Alice Buyer',
                email: 'alice@buyer.com',
                phone: '(239) 555-9999',
                message: 'I would like to tour 101 Bay View DR.'
            })
        }), env, {});
        assert.equal(leadRes.status, 201);
        const leadData = await leadRes.json();
        assert.equal(leadData.success, true);
        assert.equal(mockDB.tables.sneak_leads.length, 1);
        assert.equal(mockDB.tables.sneak_leads[0].site_id, 'site_a');
        assert.equal(mockDB.tables.sneak_leads[0].client_name, 'Alice Buyer');

        // 2. Honeypot Spam Bot
        const botRes = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/api/contact?token=${encodeURIComponent(token)}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: 'Bot User',
                email: 'bot@spam.com',
                website_hp: 'http://spam-link.com'
            })
        }), env, {});
        assert.equal(botRes.status, 200); // Silent drop
        assert.equal(mockDB.tables.sneak_leads.length, 1, 'Honeypot submission dropped from D1');
    });

    test('TEST 7: Service Entitlement & Suspension Enforcement', async () => {
        const mockDB = createMockSitesDB();
        const env = { SNEAK_ENV: 'staging', SNEAK_WEBSITE_PREVIEW_SECRET: TEST_PREVIEW_SECRET, DB: mockDB };

        // Suspended Account Website Access
        const token = await createPreviewToken('site-suspended', 'site_susp', TEST_PREVIEW_SECRET, 1800);
        const res = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-suspended/?token=${encodeURIComponent(token)}`), env, {});
        assert.equal(res.status, 503);
        const html = await res.text();
        assert.ok(html.includes('Website Temporarily Unavailable'));
        assert.ok(!html.includes('GrowthZone'));
        assert.ok(!html.includes('delinquent'));
    });

    test('TEST 8: Cross-Tenant Isolation Across 3 Sites', async () => {
        const mockDB = createMockSitesDB();
        const env = { SNEAK_ENV: 'staging', SNEAK_WEBSITE_PREVIEW_SECRET: TEST_PREVIEW_SECRET, DB: mockDB };

        const tokenA = await createPreviewToken('site-agent-a', 'site_a', TEST_PREVIEW_SECRET, 1800);
        const tokenB = await createPreviewToken('site-agent-b', 'site_b', TEST_PREVIEW_SECRET, 1800);

        const resA = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-a/?token=${encodeURIComponent(tokenA)}`), env, {});
        const htmlA = await resA.text();

        const resB = await worker.fetch(new Request(`https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/site-agent-b/?token=${encodeURIComponent(tokenB)}`), env, {});
        const htmlB = await resB.text();

        assert.ok(htmlA.includes('Sarah Agent'));
        assert.ok(!htmlA.includes('David Coastal'));

        assert.ok(htmlB.includes('David Coastal'));
        assert.ok(!htmlB.includes('Sarah Agent'));
    });
});
