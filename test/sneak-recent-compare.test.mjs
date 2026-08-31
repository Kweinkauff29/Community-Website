/**
 * Phase 7.3C3A — Recently Viewed + Property Compare security and behavior.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import consumerWorker from '../sneak-consumer/worker.js';
import {
    sanitizeActivityMetadata,
    recordConsumerActivity,
    fetchCurrentListingSummaries,
    handleListRecentlyViewed,
    handleListCompare,
    handleAddCompare,
    handleMergeCompare
} from '../sneak-consumer/api.js';
import { handleListingSummaries } from '../SneakIDXWorker.js';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const RAW_TOKEN = 'phase73c3a-test-consumer-session-token-123456789';

function listing(key, overrides = {}) {
    return {
        ListingKey: key,
        ListingId: `MLS-${key}`,
        ListPrice: 750000,
        OriginalListPrice: 775000,
        StandardStatus: 'Active',
        PropertyType: 'Residential',
        PropertySubType: 'Single Family Residence',
        BedroomsTotal: 3,
        BathroomsTotalInteger: 2,
        LivingArea: 1900,
        LotSizeAcres: 0.25,
        YearBuilt: 2020,
        City: 'Bonita Springs',
        StateOrProvince: 'FL',
        PostalCode: '34135',
        CountyOrParish: 'Lee',
        UnparsedAddress: `${key} Gulf Way`,
        PrimaryPhoto: `https://example.test/${key}.jpg`,
        ListOfficeName: 'Coconut Coast Realty',
        ListAgentMlsId: 'AGENT_A',
        ListAgentKey: 'AGENT_A',
        ListOfficeMlsId: 'OFFICE_A',
        ListOfficeKey: 'OFFICE_A',
        SubdivisionName: 'Coconut Shores',
        InternetEntireListingDisplayYN: 1,
        InternetAddressDisplayYN: 1,
        ...overrides
    };
}

function createDb({ compare = [] } = {}) {
    const tables = {
        sites: [
            { site_id: 'site_a', id: 'site_a', site_key: 'site-a', status: 'active', scope_type: 'agent', scope_value: 'AGENT_A' },
            { site_id: 'site_b', id: 'site_b', site_key: 'site-b', status: 'active', scope_type: 'agent', scope_value: 'AGENT_B' }
        ],
        listings: [
            listing('res-1'),
            listing('rent-2', { PropertyType: 'Residential Lease', ListPrice: 4200 }),
            listing('land-3', { PropertyType: 'Land', BedroomsTotal: null, BathroomsTotalInteger: null, LivingArea: null, LotSizeAcres: 2.5, InternetAddressDisplayYN: 0 }),
            listing('comm-4', { PropertyType: 'Commercial Sale', BedroomsTotal: null, BathroomsTotalInteger: null, LivingArea: 6400, Zoning: 'C-2' }),
            listing('res-5'),
            listing('hidden-6', { InternetEntireListingDisplayYN: 0 }),
            listing('closed-7', { StandardStatus: 'Closed' }),
            listing('foreign-8', { ListAgentMlsId: 'AGENT_B', ListAgentKey: 'AGENT_B' })
        ],
        events: [
            { listing_key: 'res-1', viewed_at: '2026-08-31T12:00:00Z' },
            { listing_key: 'hidden-6', viewed_at: '2026-08-31T11:00:00Z' },
            { listing_key: 'land-3', viewed_at: '2026-08-31T10:00:00Z' }
        ],
        compare: compare.map((key, index) => ({ id: `cmp_${index}`, site_id: 'site_a', user_id: 'user_a', listing_key: key, created_at: `2026-08-31T0${index}:00:00Z` })),
        activityWrites: []
    };

    function queryExecutor(sql, args) {
        const normalized = sql.replace(/\s+/g, ' ').trim();
        return {
            async first() {
                if (normalized.includes('FROM sneak_consumer_sessions s') && normalized.includes('JOIN sneak_consumer_users u')) {
                    return {
                        session_id: 'session_a', user_id: 'user_a', site_id: 'site_a',
                        created_at: '2026-08-01T00:00:00Z', expires_at: '2099-01-01T00:00:00Z', revoked_at: null,
                        consumer_email: 'buyer@example.test', user_status: 'active', site_key: 'site-a', site_status: 'active'
                    };
                }
                if (normalized.includes('FROM sneak_sites') && normalized.includes('WHERE site_key = ?')) {
                    return tables.sites.find(site => site.site_key === args[0]) || null;
                }
                const result = await this.all();
                return result.results[0] || null;
            },
            async all() {
                if (normalized.includes('FROM sneak_consumer_activity_events') && normalized.includes("event_type = 'listing_view'")) {
                    return { results: tables.events };
                }
                if (normalized.includes('FROM sneak_consumer_compare') && normalized.includes('ORDER BY created_at')) {
                    return { results: tables.compare.filter(row => row.user_id === args[0] && row.site_id === args[1]) };
                }
                if (normalized.includes('FROM sneak_listings') && normalized.includes('ListingKey IN')) {
                    const requested = new Set(args.filter(arg => tables.listings.some(item => item.ListingKey === arg)));
                    const agentScope = normalized.includes('ListAgentMlsId = ?');
                    return {
                        results: tables.listings.filter(item => requested.has(item.ListingKey)
                            && item.InternetEntireListingDisplayYN === 1
                            && ['Active', 'Active Under Contract', 'Pending'].includes(item.StandardStatus)
                            && (!agentScope || item.ListAgentMlsId === 'AGENT_A'))
                    };
                }
                return { results: [] };
            },
            async run() {
                if (normalized.startsWith('INSERT OR IGNORE INTO sneak_consumer_compare')) {
                    const [id, siteId, userId, key] = args;
                    const exists = tables.compare.some(row => row.site_id === siteId && row.user_id === userId && row.listing_key === key);
                    if (!exists && tables.compare.filter(row => row.site_id === siteId && row.user_id === userId).length >= 4) {
                        throw new Error('compare_limit_exceeded');
                    }
                    if (!exists) tables.compare.push({ id, site_id: siteId, user_id: userId, listing_key: key, created_at: new Date().toISOString() });
                    return { meta: { changes: exists ? 0 : 1 } };
                }
                if (normalized.startsWith('DELETE FROM sneak_consumer_compare')) {
                    const [userId, siteId, ...keys] = args;
                    const before = tables.compare.length;
                    tables.compare = tables.compare.filter(row => !(row.user_id === userId && row.site_id === siteId && keys.includes(row.listing_key)));
                    return { meta: { changes: before - tables.compare.length } };
                }
                if (normalized.startsWith('INSERT OR IGNORE INTO sneak_consumer_activity_events')) {
                    tables.activityWrites.push({
                        id: args[0], site_id: args[1], user_id: args[2], event_type: args[3],
                        listing_key: args[4], saved_search_id: args[5], lead_id: args[6],
                        metadata_json: args[7], dedupe_key: args[8], created_at: args[9]
                    });
                    return { meta: { changes: 1 } };
                }
                return { meta: { changes: 1 } };
            }
        };
    }

    return {
        tables,
        prepare(sql) {
            return {
                bind(...args) { return queryExecutor(sql, args); },
                ...queryExecutor(sql, [])
            };
        }
    };
}

function authRequest(url, method = 'GET', body = null) {
    return new Request(url, {
        method,
        headers: { 'Authorization': `Bearer ${RAW_TOKEN}`, 'Content-Type': 'application/json' },
        body: body === null ? undefined : JSON.stringify(body)
    });
}

describe('SNEAK IDX Phase 7.3C3A — Recently Viewed + Property Compare', () => {
    test('1. Activity metadata is event-sanitized and unknown fields are discarded', () => {
        assert.deepEqual(sanitizeActivityMetadata('saved_search_created', { name: ' Gulf Homes ', secret: 'discard' }), { name: 'Gulf Homes' });
        assert.deepEqual(sanitizeActivityMetadata('alert_frequency_changed', { frequency: 'weekly', email: 'discard' }), { frequency: 'weekly' });
        assert.equal(sanitizeActivityMetadata('listing_view', { arbitrary: 'discard' }), null);
        assert.equal(sanitizeActivityMetadata('alert_enabled', { frequency: 'hourly' }), null);
    });

    test('2. Every non-null metadata_json value is complete valid JSON below the 2 KB ceiling', async () => {
        const db = createDb();
        await recordConsumerActivity(db, {
            siteId: 'site_a', userId: 'user_a', eventType: 'saved_search_updated',
            metadata: { name: 'x'.repeat(5000), unknown: 'y'.repeat(5000) }
        });
        const stored = db.tables.activityWrites[0].metadata_json;
        assert.ok(stored);
        assert.doesNotThrow(() => JSON.parse(stored));
        assert.ok(new TextEncoder().encode(stored).byteLength <= 2048);
        assert.deepEqual(Object.keys(JSON.parse(stored)), ['name']);
    });

    test('3. Current summary resolution enforces tenant, status, display, address controls, and caller order', async () => {
        const db = createDb();
        const rows = await fetchCurrentListingSummaries(db, db.tables.sites[0], ['land-3', 'hidden-6', 'foreign-8', 'closed-7', 'res-1']);
        assert.deepEqual(rows.map(row => row.ListingKey), ['land-3', 'res-1']);
        assert.equal(rows[0].UnparsedAddress, 'Address Undisclosed');
    });

    test('4. Authenticated Recently Viewed is derived from listing_view events and omits stale/private results', async () => {
        const db = createDb();
        const res = await handleListRecentlyViewed(authRequest('https://consumer.test/api/consumer/recently-viewed?site=site-a'), new URL('https://consumer.test/api/consumer/recently-viewed?site=site-a'), { DB: db }, 'https://site.test');
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.deepEqual(data.recentlyViewed.map(entry => entry.listingKey), ['res-1', 'land-3']);
        assert.equal(data.recentlyViewed.some(entry => entry.listingKey === 'hidden-6'), false);
    });

    test('5. Compare reads expose only current scoped listings and prune invalid keys', async () => {
        const db = createDb({ compare: ['res-1', 'hidden-6', 'foreign-8'] });
        const url = new URL('https://consumer.test/api/consumer/compare?site=site-a');
        const res = await handleListCompare(authRequest(url.href), url, { DB: db }, 'https://site.test');
        const data = await res.json();
        assert.deepEqual(data.compare.map(entry => entry.listingKey), ['res-1']);
        assert.deepEqual(db.tables.compare.map(entry => entry.listing_key), ['res-1']);
    });

    test('6. Compare writes reject unavailable listings and enforce a maximum of four', async () => {
        const invalidDb = createDb();
        const invalidRes = await handleAddCompare(authRequest('https://consumer.test/api/consumer/compare', 'POST', { site: 'site-a', listingKey: 'hidden-6' }), { DB: invalidDb }, 'https://site.test');
        assert.equal(invalidRes.status, 404);

        const fullDb = createDb({ compare: ['res-1', 'rent-2', 'land-3', 'comm-4'] });
        const fullRes = await handleAddCompare(authRequest('https://consumer.test/api/consumer/compare', 'POST', { site: 'site-a', listingKey: 'res-5' }), { DB: fullDb }, 'https://site.test');
        assert.equal(fullRes.status, 400);
        assert.equal((await fullRes.json()).error, 'CompareLimitExceeded');
    });

    test('7. Local-to-server Compare merge validates keys, preserves server entries, and stays bounded', async () => {
        const db = createDb({ compare: ['res-1'] });
        const req = authRequest('https://consumer.test/api/consumer/compare/merge', 'POST', {
            site: 'site-a', listingKeys: ['hidden-6', 'rent-2', 'foreign-8', 'land-3', 'comm-4']
        });
        const res = await handleMergeCompare(req, { DB: db }, 'https://site.test');
        const data = await res.json();
        assert.equal(data.count, 3);
        assert.deepEqual(data.compare.map(entry => entry.listingKey), ['res-1', 'rent-2', 'land-3']);
    });

    test('8. Recently Viewed and Compare consumer APIs reject anonymous requests', async () => {
        const env = { DB: createDb() };
        const recent = await consumerWorker.fetch(new Request('https://consumer.test/api/consumer/recently-viewed?site=site-a'), env);
        const compare = await consumerWorker.fetch(new Request('https://consumer.test/api/consumer/compare?site=site-a'), env);
        assert.equal(recent.status, 401);
        assert.equal(compare.status, 401);
    });

    test('9. Serving summary endpoint is bounded and returns only current scoped summaries', async () => {
        const db = createDb();
        const url = new URL('https://idx.test/idx/v1/listings/summary?site=site-a&keys=res-1,hidden-6,foreign-8,land-3');
        const res = await handleListingSummaries(url, db.tables.sites[0], { DB: db }, 'https://site.test');
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.deepEqual(data.data.map(row => row.ListingKey), ['res-1', 'land-3']);

        const tooMany = new URL(`https://idx.test/idx/v1/listings/summary?site=site-a&keys=${Array.from({ length: 21 }, (_, index) => `k${index}`).join(',')}`);
        assert.equal((await handleListingSummaries(tooMany, db.tables.sites[0], { DB: db }, 'https://site.test')).status, 400);
    });

    test('10. Migration 0031 stores references only and enforces four via a database trigger', () => {
        const sql = fs.readFileSync(path.join(rootDir, 'migrations', '0031_sneak_consumer_compare.sql'), 'utf8');
        assert.match(sql, /CREATE TABLE IF NOT EXISTS sneak_consumer_compare/);
        assert.match(sql, /listing_key TEXT NOT NULL/);
        assert.match(sql, /trg_sneak_consumer_compare_max_four/);
        assert.match(sql, />= 4/);
        assert.doesNotMatch(sql, /ListPrice|UnparsedAddress|MediaJSON|PrimaryPhoto/);

        const compatibilitySql = fs.readFileSync(path.join(rootDir, 'migrations', '0032_sneak_site_fk_compatibility.sql'), 'utf8');
        assert.match(compatibilitySql, /CHECK \(row_count = 0\)/);
        assert.match(compatibilitySql, /REFERENCES sneak_sites\(id\)/);
        assert.doesNotMatch(compatibilitySql, /REFERENCES sneak_sites\(site_id\)/);
    });

    test('11. Anonymous privacy uses only site-scoped key storage and has no Recently Viewed upload path', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        assert.match(html, /ccor_recently_viewed_' \+ SITE_KEY/);
        assert.match(html, /JSON\.stringify\(recentlyViewedEntries\.map\(entry => \(\{ key: entry\.key, viewedAt: entry\.viewedAt \}\)\)\)/);
        assert.doesNotMatch(html, /recently-viewed\/merge/);
        assert.match(html, /if \(consumerUser && consumerSessionToken && item\?\.ListingKey\)/);
        assert.match(html, /recordAnonymousRecentlyViewed\(item\.ListingKey\)/);
    });

    test('12. Compare UI covers card/detail/tray and Residential, Rental, Land, Commercial contexts responsively', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        for (const marker of ['card-compare-btn', 'detailCompareBtn', 'compareTray', 'compareModal', "return 'rental'", "return 'land'", "return 'commercial'", "return 'residential'"]) {
            assert.ok(html.includes(marker), `Missing UI marker: ${marker}`);
        }
        assert.match(html, /@media \(max-width: 768px\)[\s\S]*\.compare-tray/);
        assert.match(html, /const COMPARE_LIMIT = 4/);
    });

    test('13. C3A application script parses and serving/consumer paths contain no Bridge calls', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(match => match[1]);
        const applicationScript = scripts.find(script => script.includes('CCOR_IDX_UI_BUILD'));
        assert.ok(applicationScript);
        assert.doesNotThrow(() => new Function(applicationScript));
        const serving = fs.readFileSync(path.join(rootDir, 'SneakIDXWorker.js'), 'utf8');
        const consumer = fs.readFileSync(path.join(rootDir, 'sneak-consumer', 'api.js'), 'utf8');
        assert.doesNotMatch(serving.slice(serving.indexOf('handleListingSummaries'), serving.indexOf('handleListingMedia')), /api\.bridgeinteractive|BRIDGE_TOKEN/);
        assert.doesNotMatch(consumer, /api\.bridgeinteractive|BRIDGE_TOKEN/);
    });
});
