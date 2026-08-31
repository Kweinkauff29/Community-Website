import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
    generatePublicSharedListSlug,
    handleAddSharedListItem,
    handleCreateSharedList,
    handleDeleteAccount,
    handleDeleteSharedList,
    handleDisableSharedListShare,
    handleEnableSharedListShare,
    handleGetSharedList,
    handleListSharedLists,
    handleRemoveSharedListItem,
    handleUpdateSharedList,
    normalizeSharedListName
} from '../sneak-consumer/api.js';
import { sha256Hex } from '../sneak-consumer/auth.js';
import {
    handlePublicSharedList,
    sanitizeMemberShareBaseUrl
} from '../SneakIDXWorker.js';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const TOKEN_A = 'sharing-test-session-a-abcdefghijklmnopqrstuvwxyz';
const TOKEN_A_OTHER = 'sharing-test-session-a-other-abcdefghijklmnop';
const TOKEN_B = 'sharing-test-session-b-abcdefghijklmnopqrstuvwxyz';
const ORIGIN = 'https://member-a.test';

function request(url, method = 'GET', body = null, token = TOKEN_A) {
    return new Request(url, {
        method,
        headers: {
            Origin: ORIGIN,
            Authorization: `Bearer ${token}`,
            ...(body ? { 'Content-Type': 'application/json' } : {})
        },
        body: body ? JSON.stringify(body) : undefined
    });
}

function listing(key, overrides = {}) {
    return {
        ListingKey: key,
        ListingId: `MLS-${key}`,
        ListPrice: 725000,
        OriginalListPrice: 750000,
        StandardStatus: 'Active',
        PropertyType: 'Residential',
        PropertySubType: 'Single Family Residence',
        BedroomsTotal: 3,
        BathroomsTotalInteger: 2,
        LivingArea: 1800,
        LotSizeAcres: 0.25,
        YearBuilt: 2020,
        City: 'Bonita Springs',
        StateOrProvince: 'FL',
        PostalCode: '34135',
        CountyOrParish: 'Lee',
        UnparsedAddress: `${key} Main St`,
        PrimaryPhoto: 'https://images.example.test/property.jpg',
        ListOfficeName: 'Current Listing Brokerage',
        ListAgentMlsId: 'AGENT_A',
        ListAgentKey: 'AGENT_A',
        ListOfficeMlsId: 'OFFICE_A',
        ListOfficeKey: 'OFFICE_A',
        SubdivisionName: 'Current Community',
        WaterfrontYN: 0,
        PoolPrivateYN: 0,
        GarageSpaces: 2,
        NewConstructionYN: 0,
        Zoning: null,
        ListingContractDate: '2026-08-01',
        InternetEntireListingDisplayYN: 1,
        InternetAddressDisplayYN: 1,
        ...overrides
    };
}

async function createSharingDb() {
    const sessionHashes = new Map([
        [await sha256Hex(TOKEN_A), { userId: 'user-a', siteId: 'site-a', siteKey: 'site-a', email: 'same@example.test' }],
        [await sha256Hex(TOKEN_A_OTHER), { userId: 'user-a-other', siteId: 'site-a', siteKey: 'site-a', email: 'other@example.test' }],
        [await sha256Hex(TOKEN_B), { userId: 'user-b', siteId: 'site-b', siteKey: 'site-b', email: 'same@example.test' }]
    ]);
    const tables = {
        sites: [
            { site_id: 'site-a', id: 'site-a', site_key: 'site-a', status: 'active', scope_type: 'agent', scope_value: 'AGENT_A', display_name: 'Agent A', site_name: 'Agent A Search', brokerage: 'Brokerage A', logo_url: '' },
            { site_id: 'site-b', id: 'site-b', site_key: 'site-b', status: 'active', scope_type: 'agent', scope_value: 'AGENT_B', display_name: 'Agent B', site_name: 'Agent B Search', brokerage: 'Brokerage B', logo_url: '' }
        ],
        domains: [
            { site_id: 'site-a', domain: 'member-a.test', status: 'active', verified: 1 },
            { site_id: 'site-b', domain: 'member-b.test', status: 'active', verified: 1 }
        ],
        listings: [
            listing('active-1'),
            listing('suppressed-2', { UnparsedAddress: '999 Secret Lane', InternetAddressDisplayYN: 0, ListPrice: 410000 }),
            listing('hidden-3', { InternetEntireListingDisplayYN: 0 }),
            listing('closed-4', { StandardStatus: 'Closed' }),
            listing('foreign-5', { ListAgentMlsId: 'AGENT_B', ListAgentKey: 'AGENT_B' }),
            listing('land-6', { PropertyType: 'Land', PropertySubType: 'Residential Lot', BedroomsTotal: null, BathroomsTotalInteger: null, LivingArea: null, LotSizeAcres: 1.4 }),
            listing('commercial-7', { PropertyType: 'Commercial Sale', PropertySubType: 'Retail', BedroomsTotal: null, BathroomsTotalInteger: null, LivingArea: 4200, LotSizeAcres: 0.8, Zoning: 'C-1' })
        ],
        lists: [],
        items: [],
        users: new Set(['user-a', 'user-a-other', 'user-b']),
        activityWrites: 0
    };

    const publicRow = row => row && ({ ...row, item_count: tables.items.filter(item => item.list_id === row.id).length });
    const eligibleListings = (keys, site) => keys.map(key => tables.listings.find(item => item.ListingKey === key)).filter(item =>
        item && item.InternetEntireListingDisplayYN === 1 && ['Active', 'Active Under Contract', 'Pending'].includes(item.StandardStatus) &&
        (site.scope_type === 'market' || item.ListAgentMlsId === site.scope_value)
    );

    function execute(sql, args, mode) {
        const normalized = sql.replace(/\s+/g, ' ').trim();
        if (normalized.includes('FROM sneak_consumer_sessions s')) {
            const session = sessionHashes.get(args[0]);
            if (!session || !tables.users.has(session.userId)) return null;
            return { session_id: `session-${session.userId}`, user_id: session.userId, site_id: session.siteId, consumer_email: session.email, user_status: 'active', site_key: session.siteKey, site_status: 'active' };
        }
        if (normalized.startsWith('UPDATE sneak_consumer_sessions SET last_seen_at')) return { meta: { changes: 1 } };
        if (normalized.includes('SELECT id AS site_id, site_key, scope_type')) return tables.sites.find(site => site.site_key === args[0] && site.status === 'active') || null;
        if (normalized.includes('SELECT domain FROM sneak_domains')) return { results: tables.domains.filter(domain => domain.site_id === args[0] && domain.status === 'active' && domain.verified === 1) };

        if (normalized.includes('SELECT COUNT(*) AS count FROM sneak_consumer_shared_lists')) {
            return { count: tables.lists.filter(list => list.user_id === args[0] && list.site_id === args[1]).length };
        }
        if (normalized.startsWith('INSERT INTO sneak_consumer_shared_lists')) {
            const [id, site_id, user_id, name, created_at, updated_at] = args;
            if (tables.lists.filter(list => list.user_id === user_id && list.site_id === site_id).length >= 10) throw new Error('shared_list_limit_exceeded');
            tables.lists.push({ id, site_id, user_id, name, share_enabled: 0, public_slug: null, created_at, updated_at });
            return { meta: { changes: 1 } };
        }
        if (normalized.includes('FROM sneak_consumer_shared_lists l WHERE l.id = ?')) {
            return publicRow(tables.lists.find(list => list.id === args[0] && list.user_id === args[1] && list.site_id === args[2])) || null;
        }
        if (normalized.includes('FROM sneak_consumer_shared_lists l WHERE l.user_id = ?')) {
            return { results: tables.lists.filter(list => list.user_id === args[0] && list.site_id === args[1]).map(publicRow).sort((a, b) => b.updated_at.localeCompare(a.updated_at)) };
        }
        if (normalized.startsWith('UPDATE sneak_consumer_shared_lists SET name =')) {
            const row = tables.lists.find(list => list.id === args[1] && list.user_id === args[2] && list.site_id === args[3]);
            if (!row) return { meta: { changes: 0 } };
            row.name = args[0]; row.updated_at = new Date().toISOString();
            return { meta: { changes: 1 } };
        }
        if (normalized.startsWith('DELETE FROM sneak_consumer_shared_lists')) {
            const index = tables.lists.findIndex(list => list.id === args[0] && list.user_id === args[1] && list.site_id === args[2]);
            if (index < 0) return { meta: { changes: 0 } };
            const [removed] = tables.lists.splice(index, 1);
            tables.items = tables.items.filter(item => item.list_id !== removed.id);
            return { meta: { changes: 1 } };
        }
        if (normalized.includes('SET share_enabled = 1, public_slug = ?')) {
            const row = tables.lists.find(list => list.id === args[1] && list.user_id === args[2] && list.site_id === args[3] && list.share_enabled === 0);
            if (!row) return { meta: { changes: 0 } };
            if (tables.lists.some(list => list.public_slug === args[0])) throw new Error('UNIQUE constraint failed');
            row.share_enabled = 1; row.public_slug = args[0]; row.updated_at = new Date().toISOString();
            return { meta: { changes: 1 } };
        }
        if (normalized.includes('SET share_enabled = 0, public_slug = NULL')) {
            const row = tables.lists.find(list => list.id === args[0] && list.user_id === args[1] && list.site_id === args[2]);
            if (!row) return { meta: { changes: 0 } };
            row.share_enabled = 0; row.public_slug = null; row.updated_at = new Date().toISOString();
            return { meta: { changes: 1 } };
        }
        if (normalized.includes('FROM sneak_consumer_shared_lists WHERE public_slug = ?')) {
            const row = tables.lists.find(list => list.public_slug === args[0] && list.site_id === args[1] && list.share_enabled === 1);
            return row ? { id: row.id, name: row.name } : null;
        }

        if (normalized.startsWith('INSERT INTO sneak_consumer_shared_list_items') || normalized.startsWith('INSERT OR IGNORE INTO sneak_consumer_shared_list_items')) {
            const [id, list_id, listing_key, sort_order, suppliedCreatedAt] = args;
            if (tables.items.some(item => item.list_id === list_id && item.listing_key === listing_key)) return { meta: { changes: 0 } };
            if (tables.items.filter(item => item.list_id === list_id).length >= 25) throw new Error('shared_list_item_limit_exceeded');
            tables.items.push({ id, list_id, listing_key, sort_order, created_at: suppliedCreatedAt || new Date().toISOString() });
            return { meta: { changes: 1 } };
        }
        if (normalized.includes('SELECT id FROM sneak_consumer_shared_list_items WHERE list_id = ?')) {
            return tables.items.find(item => item.list_id === args[0] && item.listing_key === args[1]) || null;
        }
        if (normalized.includes('SELECT COALESCE(MAX(sort_order), -1) + 1 AS next_order')) {
            const orders = tables.items.filter(item => item.list_id === args[0]).map(item => Number(item.sort_order));
            return { next_order: orders.length ? Math.max(...orders) + 1 : 0 };
        }
        if (normalized.startsWith('DELETE FROM sneak_consumer_shared_list_items')) {
            const before = tables.items.length;
            tables.items = tables.items.filter(item => !(item.list_id === args[0] && item.listing_key === args[1]));
            return { meta: { changes: before - tables.items.length } };
        }
        if (normalized.includes('SELECT id, listing_key, sort_order, created_at FROM sneak_consumer_shared_list_items')) {
            return { results: tables.items.filter(item => item.list_id === args[0]).sort((a, b) => a.sort_order - b.sort_order) };
        }
        if (normalized.includes('SELECT listing_key, sort_order FROM sneak_consumer_shared_list_items')) {
            return { results: tables.items.filter(item => item.list_id === args[0]).sort((a, b) => a.sort_order - b.sort_order) };
        }

        if (normalized.includes('FROM sneak_listings') && normalized.includes('ListingKey IN (')) {
            const placeholderSection = normalized.match(/ListingKey IN \(([^)]*)\)/)?.[1] || '';
            const keyCount = (placeholderSection.match(/\?/g) || []).length;
            const keys = args.slice(0, keyCount);
            const scopeValue = args[keyCount];
            const site = tables.sites.find(candidate => candidate.scope_value === scopeValue) || tables.sites[0];
            return { results: eligibleListings(keys, site) };
        }
        if (normalized.startsWith('DELETE FROM sneak_consumer_users')) {
            const [userId, siteId] = args;
            tables.users.delete(userId);
            const removedIds = new Set(tables.lists.filter(list => list.user_id === userId && list.site_id === siteId).map(list => list.id));
            tables.lists = tables.lists.filter(list => !removedIds.has(list.id));
            tables.items = tables.items.filter(item => !removedIds.has(item.list_id));
            return { meta: { changes: 1 } };
        }
        if (normalized.includes('INSERT') && normalized.includes('sneak_consumer_activity_events')) {
            tables.activityWrites += 1;
            return { meta: { changes: 1 } };
        }
        throw new Error(`Unhandled sharing mock SQL (${mode}): ${normalized}`);
    }

    const db = {
        tables,
        prepare(sql) {
            let args = [];
            return {
                bind(...values) { args = values; return this; },
                async first() { return execute(sql, args, 'first'); },
                async all() { return execute(sql, args, 'all'); },
                async run() { return execute(sql, args, 'run'); }
            };
        },
        async batch(statements) {
            const results = [];
            for (const statement of statements) results.push(await statement.run());
            return results;
        }
    };
    return db;
}

async function createList(db, name = 'Private Favorites', listingKeys = []) {
    const res = await handleCreateSharedList(
        request('https://consumer.test/api/consumer/shared-lists', 'POST', { site: 'site-a', name, listingKeys }),
        { DB: db },
        ORIGIN
    );
    return { res, data: await res.json() };
}

describe('SNEAK IDX Phase 7.3C3B — Safe Property Sharing + Shared Lists', () => {
    test('1. Migration 0033 stores listing references only and enforces cascade plus limits', () => {
        const sql = fs.readFileSync(path.join(rootDir, 'migrations', '0033_sneak_consumer_shared_lists.sql'), 'utf8');
        assert.match(sql, /CREATE TABLE IF NOT EXISTS sneak_consumer_shared_lists/);
        assert.match(sql, /CREATE TABLE IF NOT EXISTS sneak_consumer_shared_list_items/);
        assert.match(sql, /REFERENCES sneak_sites\(id\) ON DELETE CASCADE/);
        assert.match(sql, /REFERENCES sneak_consumer_users\(id\) ON DELETE CASCADE/);
        assert.match(sql, /REFERENCES sneak_consumer_shared_lists\(id\) ON DELETE CASCADE/);
        assert.match(sql, /UNIQUE\(list_id, listing_key\)/);
        assert.match(sql, /shared_list_limit_exceeded/);
        assert.match(sql, /shared_list_item_limit_exceeded/);
        assert.doesNotMatch(sql, /ListPrice|UnparsedAddress|MediaJSON|PublicRemarks/);
    });

    test('2. New lists are private by default and may include validated current references', async () => {
        const db = await createSharingDb();
        const { res, data } = await createList(db, 'Weekend Homes', ['active-1', 'suppressed-2']);
        assert.equal(res.status, 201);
        assert.equal(data.sharedList.name, 'Weekend Homes');
        assert.equal(data.sharedList.shareEnabled, false);
        assert.equal(data.sharedList.publicSlug, null);
        assert.equal(data.sharedList.itemCount, 2);
    });

    test('3. List names require 1–80 trimmed characters', async () => {
        assert.equal(normalizeSharedListName('  My List  '), 'My List');
        assert.equal(normalizeSharedListName(''), null);
        assert.equal(normalizeSharedListName('x'.repeat(81)), null);
        const db = await createSharingDb();
        const { res } = await createList(db, 'x'.repeat(81));
        assert.equal(res.status, 400);
    });

    test('4. Maximum 10 lists is enforced server-side', async () => {
        const db = await createSharingDb();
        for (let index = 0; index < 10; index++) (await createList(db, `List ${index}`));
        const { res, data } = await createList(db, 'List 11');
        assert.equal(res.status, 400);
        assert.equal(data.error, 'SharedListLimitExceeded');
    });

    test('5. Owner can list and rename lists', async () => {
        const db = await createSharingDb();
        const created = await createList(db);
        const id = created.data.sharedList.id;
        const patchUrl = new URL(`https://consumer.test/api/consumer/shared-lists/${id}?site=site-a`);
        const renamed = await handleUpdateSharedList(request(patchUrl, 'PATCH', { name: 'Renamed List' }), id, patchUrl, { DB: db }, ORIGIN);
        assert.equal(renamed.status, 200);
        assert.equal((await renamed.json()).sharedList.name, 'Renamed List');
        const listUrl = new URL('https://consumer.test/api/consumer/shared-lists?site=site-a');
        const listed = await handleListSharedLists(request(listUrl), listUrl, { DB: db }, ORIGIN);
        assert.equal((await listed.json()).sharedLists[0].name, 'Renamed List');
    });

    test('6. Adding a valid property is idempotent and removal works', async () => {
        const db = await createSharingDb();
        const id = (await createList(db)).data.sharedList.id;
        const addReq = () => request(`https://consumer.test/api/consumer/shared-lists/${id}/items`, 'POST', { site: 'site-a', listingKey: 'active-1' });
        assert.equal((await handleAddSharedListItem(addReq(), id, { DB: db }, ORIGIN)).status, 200);
        const duplicate = await handleAddSharedListItem(addReq(), id, { DB: db }, ORIGIN);
        assert.equal((await duplicate.json()).idempotent, true);
        assert.equal(db.tables.items.length, 1);
        const removeUrl = new URL(`https://consumer.test/api/consumer/shared-lists/${id}/items/active-1?site=site-a`);
        assert.equal((await handleRemoveSharedListItem(request(removeUrl, 'DELETE'), id, 'active-1', removeUrl, { DB: db }, ORIGIN)).status, 200);
        assert.equal(db.tables.items.length, 0);
    });

    test('7. Maximum 25 properties is enforced server-side', async () => {
        const db = await createSharingDb();
        for (let index = 0; index < 25; index++) db.tables.listings.push(listing(`bulk-${index}`));
        const id = (await createList(db, 'Full List', Array.from({ length: 25 }, (_, index) => `bulk-${index}`))).data.sharedList.id;
        const res = await handleAddSharedListItem(request('https://consumer.test/add', 'POST', { site: 'site-a', listingKey: 'active-1' }), id, { DB: db }, ORIGIN);
        assert.equal(res.status, 400);
        assert.equal((await res.json()).error, 'SharedListItemLimitExceeded');
    });

    test('8. Adding hidden, closed, or out-of-scope properties is rejected', async () => {
        const db = await createSharingDb();
        const id = (await createList(db)).data.sharedList.id;
        for (const key of ['hidden-3', 'closed-4', 'foreign-5']) {
            const res = await handleAddSharedListItem(request('https://consumer.test/add', 'POST', { site: 'site-a', listingKey: key }), id, { DB: db }, ORIGIN);
            assert.equal(res.status, 404);
        }
    });

    test('9. Cross-site consumer and another consumer cannot manage the owner list', async () => {
        const db = await createSharingDb();
        const id = (await createList(db)).data.sharedList.id;
        const crossUrl = new URL(`https://consumer.test/api/consumer/shared-lists/${id}?site=site-a`);
        const crossSite = await handleGetSharedList(request(crossUrl, 'GET', null, TOKEN_B), id, crossUrl, { DB: db }, ORIGIN);
        assert.equal(crossSite.status, 403);
        const other = await handleGetSharedList(request(crossUrl, 'GET', null, TOKEN_A_OTHER), id, crossUrl, { DB: db }, ORIGIN);
        assert.equal(other.status, 404);
        assert.doesNotMatch(JSON.stringify(await other.json()), /user-a|same@example/);
    });

    test('10. Same email on two sites remains two isolated identities', async () => {
        const db = await createSharingDb();
        await createList(db, 'Site A List');
        const urlB = new URL('https://consumer.test/api/consumer/shared-lists?site=site-b');
        const response = await handleListSharedLists(request(urlB, 'GET', null, TOKEN_B), urlB, { DB: db }, ORIGIN);
        assert.deepEqual((await response.json()).sharedLists, []);
    });

    test('11. Public slugs use 192 bits of cryptographic randomness and are unique', () => {
        const slugs = Array.from({ length: 64 }, generatePublicSharedListSlug);
        assert.ok(slugs.every(slug => /^[a-f0-9]{48}$/.test(slug)));
        assert.equal(new Set(slugs).size, slugs.length);
    });

    test('12. Enabling sharing creates an unlisted public read with no owner identity', async () => {
        const db = await createSharingDb();
        const id = (await createList(db, 'Public Picks', ['active-1'])).data.sharedList.id;
        const enabled = await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN);
        const slug = (await enabled.json()).sharedList.publicSlug;
        const publicResponse = await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN);
        const payload = await publicResponse.json();
        assert.equal(publicResponse.status, 200);
        assert.equal(payload.name, 'Public Picks');
        assert.equal(payload.properties[0].ListingKey, 'active-1');
        const serialized = JSON.stringify(payload);
        assert.doesNotMatch(serialized, /same@example\.test|user-a|consumerSession|savedSearch|favorite|activity/i);
    });

    test('13. Public reads are tenant-bound and wrong-site reads fail generically', async () => {
        const db = await createSharingDb();
        const id = (await createList(db, 'Site A', ['active-1'])).data.sharedList.id;
        const slug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        const wrongSite = await handlePublicSharedList(slug, db.tables.sites[1], { DB: db }, ORIGIN);
        assert.equal(wrongSite.status, 404);
        assert.deepEqual(await wrongSite.json(), { error: 'SharedListUnavailable', message: 'This shared list is no longer available.' });
    });

    test('14. Malformed, unknown, disabled, deleted, and wrong-site public links share one generic response', async () => {
        const db = await createSharingDb();
        for (const slug of ['bad', '0'.repeat(48)]) {
            const res = await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN);
            assert.equal(res.status, 404);
            assert.equal((await res.json()).error, 'SharedListUnavailable');
        }
    });

    test('15. Disabling revokes the old slug and re-enabling generates a new slug', async () => {
        const db = await createSharingDb();
        const id = (await createList(db, 'Revocable', ['active-1'])).data.sharedList.id;
        const oldSlug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        const disableUrl = new URL(`https://consumer.test/api/consumer/shared-lists/${id}/share?site=site-a`);
        await handleDisableSharedListShare(request(disableUrl, 'DELETE'), id, disableUrl, { DB: db }, ORIGIN);
        assert.equal((await handlePublicSharedList(oldSlug, db.tables.sites[0], { DB: db }, ORIGIN)).status, 404);
        const newSlug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        assert.notEqual(newSlug, oldSlug);
        assert.equal((await handlePublicSharedList(newSlug, db.tables.sites[0], { DB: db }, ORIGIN)).status, 200);
    });

    test('16. Deleting a list invalidates its public link without touching unrelated consumer state', async () => {
        const db = await createSharingDb();
        db.tables.unrelated = { favorites: 2, compare: 3, activity: 4 };
        const id = (await createList(db, 'Delete Me', ['active-1'])).data.sharedList.id;
        const slug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        const deleteUrl = new URL(`https://consumer.test/api/consumer/shared-lists/${id}?site=site-a`);
        await handleDeleteSharedList(request(deleteUrl, 'DELETE'), id, deleteUrl, { DB: db }, ORIGIN);
        assert.equal((await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN)).status, 404);
        assert.deepEqual(db.tables.unrelated, { favorites: 2, compare: 3, activity: 4 });
    });

    test('17. Consumer account deletion cascades lists/items and invalidates the public link', async () => {
        const db = await createSharingDb();
        const id = (await createList(db, 'Account Cascade', ['active-1'])).data.sharedList.id;
        const slug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        const deleteUrl = new URL('https://consumer.test/api/consumer/account?site=site-a');
        assert.equal((await handleDeleteAccount(request(deleteUrl, 'DELETE'), deleteUrl, { DB: db }, ORIGIN)).status, 200);
        assert.equal((await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN)).status, 404);
    });

    test('18. Public read omits hidden, closed, and out-of-scope historical references', async () => {
        const db = await createSharingDb();
        const id = (await createList(db, 'Current Only', ['active-1'])).data.sharedList.id;
        for (const [index, key] of ['hidden-3', 'closed-4', 'foreign-5'].entries()) db.tables.items.push({ id: `stale-${index}`, list_id: id, listing_key: key, sort_order: index + 1, created_at: new Date().toISOString() });
        const slug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        const payload = await (await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN)).json();
        assert.deepEqual(payload.properties.map(item => item.ListingKey), ['active-1']);
    });

    test('19. Public read enforces address suppression without hidden address leakage', async () => {
        const db = await createSharingDb();
        const id = (await createList(db, 'Suppressed', ['suppressed-2'])).data.sharedList.id;
        const slug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        const serialized = JSON.stringify(await (await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN)).json());
        assert.match(serialized, /Address Undisclosed/);
        assert.doesNotMatch(serialized, /999 Secret Lane/);
    });

    test('20. Public read returns current price/status and listing-office attribution', async () => {
        const db = await createSharingDb();
        const id = (await createList(db, 'Current State', ['active-1'])).data.sharedList.id;
        const current = db.tables.listings.find(item => item.ListingKey === 'active-1');
        current.ListPrice = 699000;
        current.StandardStatus = 'Pending';
        const slug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        const property = (await (await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN)).json()).properties[0];
        assert.equal(property.ListPrice, 699000);
        assert.equal(property.StandardStatus, 'Pending');
        assert.equal(property.ListOfficeName, 'Current Listing Brokerage');
    });

    test('21. Anonymous public reads create no Consumer activity or account state', async () => {
        const db = await createSharingDb();
        const userCount = db.tables.users.size;
        const id = (await createList(db, 'No Tracking', ['active-1'])).data.sharedList.id;
        const slug = (await (await handleEnableSharedListShare(request('https://consumer.test/share', 'POST', { site: 'site-a' }), id, { DB: db }, ORIGIN)).json()).sharedList.publicSlug;
        await handlePublicSharedList(slug, db.tables.sites[0], { DB: db }, ORIGIN);
        assert.equal(db.tables.activityWrites, 0);
        assert.equal(db.tables.users.size, userCount);
    });

    test('22. Safe member host URL accepts only authorized HTTPS and strips credentials/deep links', async () => {
        const db = await createSharingDb();
        const safe = await sanitizeMemberShareBaseUrl(db, 'site-a', 'https://member-a.test/idx/?auth_code=a&session=b&consumer_session=c&member_session=d&token=e&bearer=f&bootstrap_token=g&ccor_listing=old&ccor_list=old#secret', false);
        assert.equal(safe, 'https://member-a.test/idx/');
        assert.equal(await sanitizeMemberShareBaseUrl(db, 'site-a', 'https://evil.test/idx/', false), null);
        assert.equal(await sanitizeMemberShareBaseUrl(db, 'site-a', 'http://member-a.test/idx/', false), null);
        assert.equal(await sanitizeMemberShareBaseUrl(db, 'site-a', 'https://consumer:secret@member-a.test/idx/', false), null);
    });

    test('23. Frontend deep-link construction uses member domain, replaces stale links, and strips auth values', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        const start = html.indexOf('const SHARE_SENSITIVE_PARAMS');
        const end = html.indexOf('function openManualShareFallback', start);
        const source = html.slice(start, end);
        const build = new Function('tenantConfig', `${source}; return buildMemberDeepLink;`)({ shareBaseUrl: 'https://member-a.test/idx/?auth_code=a&session=b&token=c&ccor_listing=old&ccor_list=old' });
        const property = new URL(build('ccor_listing', 'active-1'));
        assert.equal(property.origin + property.pathname, 'https://member-a.test/idx/');
        assert.equal(property.search, '?ccor_listing=active-1');
        assert.equal(build('ccor_list', 'f'.repeat(48)), `https://member-a.test/idx/?ccor_list=${'f'.repeat(48)}`);
        const unsafeBuild = new Function('tenantConfig', `${source}; return buildMemberDeepLink;`)({ shareBaseUrl: 'https://consumer:secret@member-a.test/idx/' });
        assert.equal(unsafeBuild('ccor_listing', 'active-1'), null);
    });

    test('24. Native Web Share path receives human title, short text, and member-domain URL', async () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        assert.match(html, /typeof navigator\.share === 'function'/);
        assert.match(html, /text: 'View this property'/);
        assert.match(html, /title: `\$\{address\} — \$\{fmt\.format/);
        assert.match(html, /ccor_listing/);
    });

    test('25. Copy fallback uses Clipboard API and offers a selectable manual field', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        assert.match(html, /navigator\.clipboard\?\.writeText/);
        assert.match(html, /Link copied/);
        assert.match(html, /id="manualShareInput"[^>]*readonly/);
        assert.match(html, /input\.select\(\)/);
    });

    test('26. UI includes owner management and all required integration entry points', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        for (const marker of ['Shared Lists', 'Create List', 'Enable Sharing', 'Copy Share Link', 'Disable Sharing', 'detailAddToListBtn', 'cart-add-list-btn', 'recent-add-list-btn', 'compareSaveListBtn', 'Save as List']) {
            assert.ok(html.includes(marker), `Missing UI marker: ${marker}`);
        }
        assert.match(html, /private list/i);
        assert.match(html, /\.consumer-modal-overlay\s*\{[\s\S]*?z-index:\s*12000;/, 'Consumer list/share/auth modals must render above listing detail');
    });

    test('27. Public-list view is noindex, contextual, escaped through textContent, and view-only', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        assert.match(html, /robots\.content = 'noindex, nofollow'/);
        assert.match(html, /title\.textContent = data\.name/);
        assert.match(html, /publicListingContext/);
        assert.doesNotMatch(html, /shared_list_viewed|share_link_copied|shared_list_created/);
        assert.doesNotMatch(html, /recipient editing|shared ownership|public directory/i);
    });

    test('28. Embed safely forwards ccor_list and server-validated host_page without changing resize security', () => {
        const embed = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'embed.js'), 'utf8');
        assert.match(embed, /hostPageUrl=/);
        assert.match(embed, /data\.hostPageUrl/);
        assert.match(embed, /ccor_list/);
        assert.match(embed, /e\.source && e\.source !== iframe\.contentWindow/);
        assert.match(embed, /newHeight < 400 \|\| newHeight > 3000/);
    });

    test('29. Unavailable property deep links remain generic and serving paths contain no Bridge call', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        assert.match(html, /This property is no longer available\./);
        const serving = fs.readFileSync(path.join(rootDir, 'SneakIDXWorker.js'), 'utf8');
        const publicRoute = serving.slice(serving.indexOf('export async function handlePublicSharedList'), serving.indexOf('async function handleListingDetail'));
        assert.doesNotMatch(publicRoute, /api\.bridgeinteractive|BRIDGE_TOKEN/);
    });

    test('30. Shared lists do not widen REALTOR activity vocabulary or Member Portal administration', () => {
        const consumer = fs.readFileSync(path.join(rootDir, 'sneak-consumer', 'api.js'), 'utf8');
        const member = fs.readFileSync(path.join(rootDir, 'sneak-member', 'ui.js'), 'utf8');
        const activitySection = consumer.slice(consumer.indexOf('export const ALLOWED_ACTIVITY_TYPES'), consumer.indexOf('const ACTIVITY_METADATA_MAX_BYTES'));
        assert.doesNotMatch(activitySection, /shared_list|share_link/);
        assert.doesNotMatch(member, /Shared Lists|shared-lists/);
    });

    test('31. C3B application script parses and affected build markers are exact', () => {
        const html = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
        const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(match => match[1]);
        const app = scripts.find(script => script.includes('CCOR_IDX_UI_BUILD'));
        assert.doesNotThrow(() => new Function(app));
        assert.match(html, /data-ui-build="2026\.08\.31\.7\.3c3b"/);
        assert.match(fs.readFileSync(path.join(rootDir, 'sneak-idx', 'embed.js'), 'utf8'), /2026\.08\.31\.7\.3c3b/);
        assert.match(fs.readFileSync(path.join(rootDir, 'SneakIDXWorker.js'), 'utf8'), /2026\.08\.31\.7\.3c3b/);
        assert.match(fs.readFileSync(path.join(rootDir, 'sneak-consumer', 'worker.js'), 'utf8'), /2026\.08\.31\.7\.3c3b/);
    });
});
