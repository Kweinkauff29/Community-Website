/**
 * test/sneak-sync-bootstrap.test.mjs
 * 
 * SNEAK IDX Phase 7.4B2A.4 — Production Sync Operational Hardening
 * State Truthfulness + Renewable Lock + Bounded-Memory Bootstrap
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import worker, {
    runListingDelta,
    runFullInventoryBootstrap,
    runListingReconciliation,
    runOpenHouseSync
} from '../sneak-sync/worker.js';
import { acquireLock, renewLock, releaseLock } from '../sneak-sync/lock.js';
import { calculateAccountReadiness } from '../sneak-admin/readiness.js';

function createTestDatabase() {
    const sqlite = new DatabaseSync(':memory:');
    sqlite.exec(`
        CREATE TABLE sneak_sync_locks (
            job_name TEXT PRIMARY KEY,
            lock_id TEXT NOT NULL,
            acquired_at DATETIME NOT NULL,
            expires_at DATETIME NOT NULL
        );

        CREATE TABLE sneak_sync_runs (
            id TEXT PRIMARY KEY,
            job_name TEXT NOT NULL,
            started_at DATETIME NOT NULL,
            finished_at DATETIME,
            status TEXT NOT NULL,
            records_fetched INTEGER DEFAULT 0,
            records_upserted INTEGER DEFAULT 0,
            records_removed INTEGER DEFAULT 0,
            bridge_pages INTEGER DEFAULT 0,
            d1_operations INTEGER DEFAULT 0,
            duration_seconds REAL DEFAULT 0,
            error_code TEXT,
            error_summary TEXT,
            created_at DATETIME
        );

        CREATE TABLE sneak_sync_state (
            sync_name TEXT PRIMARY KEY,
            last_successful_sync DATETIME,
            last_cursor TEXT,
            last_record_count INTEGER DEFAULT 0,
            last_full_reconciliation DATETIME,
            status TEXT DEFAULT 'pending',
            updated_at DATETIME
        );

        CREATE TABLE sneak_listings (
            ListingKey TEXT PRIMARY KEY,
            ListingId TEXT,
            ListPrice REAL,
            OriginalListPrice REAL,
            UnparsedAddress TEXT,
            StreetNumber TEXT,
            StreetName TEXT,
            UnitNumber TEXT,
            City TEXT,
            StateOrProvince TEXT,
            PostalCode TEXT,
            CountyOrParish TEXT,
            BedroomsTotal INTEGER,
            BathroomsTotalInteger INTEGER,
            BathroomsFull INTEGER,
            BathroomsHalf INTEGER,
            LivingArea REAL,
            StandardStatus TEXT,
            PropertyType TEXT,
            PropertySubType TEXT,
            PrimaryPhoto TEXT,
            MediaJSON TEXT,
            ListingContractDate TEXT,
            ModificationTimestamp TEXT,
            StatusChangeTimestamp TEXT,
            Latitude REAL,
            Longitude REAL,
            YearBuilt INTEGER,
            LotSizeAcres REAL,
            SubdivisionName TEXT,
            PublicRemarks TEXT,
            ListAgentKey TEXT,
            ListAgentFullName TEXT,
            ListAgentEmail TEXT,
            ListAgentDirectPhone TEXT,
            ListAgentMlsId TEXT,
            ListOfficeKey TEXT,
            ListOfficeName TEXT,
            ListOfficePhone TEXT,
            ListOfficeMlsId TEXT,
            InternetEntireListingDisplayYN INTEGER DEFAULT 1,
            InternetAddressDisplayYN INTEGER DEFAULT 1,
            OriginatingSystemKey TEXT,
            OriginatingSystemName TEXT,
            WaterfrontYN INTEGER DEFAULT 0,
            PoolPrivateYN INTEGER DEFAULT 0,
            GarageSpaces REAL DEFAULT 0,
            NewConstructionYN INTEGER DEFAULT 0,
            Zoning TEXT
        );

        CREATE TABLE sneak_open_houses (
            id TEXT PRIMARY KEY,
            OpenHouseKey TEXT UNIQUE,
            ListingKey TEXT,
            OpenHouseDate TEXT,
            OpenHouseStartTime TEXT,
            OpenHouseEndTime TEXT,
            OpenHouseType TEXT,
            Status TEXT,
            OpenHouseRemarks TEXT,
            ModificationTimestamp TEXT,
            created_at DATETIME,
            updated_at DATETIME
        );
    `);

    return {
        sqlite,
        d1: {
            prepare(sql) {
                return {
                    bind(...args) {
                        return {
                            async first() {
                                const stmt = sqlite.prepare(sql);
                                return stmt.get(...args) || null;
                            },
                            async all() {
                                const stmt = sqlite.prepare(sql);
                                return { results: stmt.all(...args) };
                            },
                            async run() {
                                const stmt = sqlite.prepare(sql);
                                const res = stmt.run(...args);
                                return { success: true, ...res };
                            }
                        };
                    },
                    async first() {
                        const stmt = sqlite.prepare(sql);
                        return stmt.get() || null;
                    },
                    async all() {
                        const stmt = sqlite.prepare(sql);
                        return { results: stmt.all() };
                    },
                    async run() {
                        const stmt = sqlite.prepare(sql);
                        const res = stmt.run();
                        return { success: true, ...res };
                    }
                };
            },
            async batch(statements) {
                sqlite.exec('BEGIN TRANSACTION;');
                try {
                    for (const s of statements) {
                        await s.run();
                    }
                    sqlite.exec('COMMIT;');
                } catch (err) {
                    sqlite.exec('ROLLBACK;');
                    throw err;
                }
            }
        }
    };
}

function mockBridgeListing(key, overrides = {}) {
    return {
        ListingKey: key,
        ListingId: `MLS_${key}`,
        ListPrice: 750000,
        OriginalListPrice: 775000,
        StandardStatus: 'Active',
        PropertyType: 'Residential',
        PropertySubType: 'SingleFamilyResidence',
        UnparsedAddress: `123 ${key} Way`,
        StreetNumber: '123',
        StreetName: `${key} Way`,
        City: 'Bonita Springs',
        StateOrProvince: 'FL',
        PostalCode: '34135',
        CountyOrParish: 'Lee',
        BedroomsTotal: 3,
        BathroomsTotalInteger: 2,
        LivingArea: 2100,
        ModificationTimestamp: '2026-07-15T10:00:00.000Z',
        Media: [
            { MediaURL: `https://images.example.com/${key}/1.jpg`, MediaCategory: 'Photo', Order: 1 },
            { MediaURL: `https://images.example.com/${key}/2.jpg`, MediaCategory: 'Photo', Order: 2 }
        ],
        InternetEntireListingDisplayYN: true,
        InternetAddressDisplayYN: true,
        OriginatingSystemKey: 'bsaor',
        ...overrides
    };
}

describe('SNEAK IDX Phase 7.4B2A.4 — Production Sync Operational Hardening', () => {

    test('1. Empty D1 + No Cursor executes Full Inventory Bootstrap (running state persisted, cursor committed, MediaJSON populated)', async () => {
        const { d1, sqlite } = createTestDatabase();
        const listings = [
            mockBridgeListing('LST_BOOT_1', { ModificationTimestamp: '2026-06-01T12:00:00.000Z' }),
            mockBridgeListing('LST_BOOT_2', { ModificationTimestamp: '2026-07-20T15:30:00.000Z' }),
            mockBridgeListing('LST_BOOT_3', { ModificationTimestamp: '2026-08-30T09:00:00.000Z' })
        ];

        let checkedRunningState = false;
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            // Verify that sneak_sync_state is persisted as 'running' with null cursor during fetch
            const currentSyncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            if (currentSyncState && currentSyncState.status === 'running') {
                checkedRunningState = true;
                assert.equal(currentSyncState.last_cursor, null, 'last_cursor must remain unset while running');
            }

            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': listings.length, value: [] }));
            }
            return new Response(JSON.stringify({ '@odata.count': listings.length, value: listings }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            const result = await runListingDelta(env);

            assert.equal(result.status, 'success');
            assert.equal(result.mode, 'bootstrap');
            assert.equal(result.recordsFetched, 3);
            assert.equal(result.recordsUpserted, 3);
            assert.equal(result.finalListingCount, 3);
            assert.equal(checkedRunningState, true, 'status=running was persisted during bootstrap execution');

            // Verify D1 records
            const rows = sqlite.prepare("SELECT * FROM sneak_listings ORDER BY ListingKey ASC").all();
            assert.equal(rows.length, 3);
            assert.equal(rows[0].PrimaryPhoto, 'https://images.example.com/LST_BOOT_1/1.jpg');
            assert.equal(JSON.parse(rows[0].MediaJSON).length, 2);

            // Verify final sync_state
            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.ok(syncState.last_cursor);
            assert.equal(syncState.status, 'success');
            assert.equal(syncState.last_record_count, 3);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('2. Partial D1 + No Cursor enters Bootstrap Mode, fills missing inventory, prunes stale rows idempotently', async () => {
        const { d1, sqlite } = createTestDatabase();
        sqlite.prepare(`
            INSERT INTO sneak_listings (ListingKey, StandardStatus, ListPrice)
            VALUES ('LST_A', 'Active', 500000), ('LST_OLD_STALE', 'Active', 400000);
        `).run();

        const currentBridgeListings = [
            mockBridgeListing('LST_A', { ListPrice: 525000 }),
            mockBridgeListing('LST_B'),
            mockBridgeListing('LST_C')
        ];

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': currentBridgeListings.length, value: [] }));
            }
            return new Response(JSON.stringify({ '@odata.count': currentBridgeListings.length, value: currentBridgeListings }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            const result = await runListingDelta(env);

            assert.equal(result.status, 'success');
            assert.equal(result.mode, 'bootstrap');
            assert.equal(result.recordsUpserted, 3);
            assert.equal(result.recordsRemoved, 1);

            const rows = sqlite.prepare("SELECT ListingKey, ListPrice FROM sneak_listings ORDER BY ListingKey ASC").all();
            assert.deepEqual(rows.map(r => r.ListingKey), ['LST_A', 'LST_B', 'LST_C']);
            assert.equal(rows[0].ListPrice, 525000);

            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.ok(syncState.last_cursor);
            assert.equal(syncState.status, 'success');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('3. Empty Bridge inventory fails closed on initial bootstrap, persists status=failure, cursor remains unset', async () => {
        const { d1, sqlite } = createTestDatabase();

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async () => {
            return new Response(JSON.stringify({ '@odata.count': 0, value: [] }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            await assert.rejects(
                async () => await runListingDelta(env),
                /EmptyBootstrapInventory/
            );

            // Verify status=failure is persisted and cursor remains unset
            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.ok(syncState, 'sync_state row exists');
            assert.equal(syncState.status, 'failure');
            assert.equal(syncState.last_cursor, null, 'cursor must remain unset on failed bootstrap');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('4. Bootstrap failure on page fetch persists status=failure, leaves cursor unset, allows safe retry', async () => {
        const { d1, sqlite } = createTestDatabase();

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 2, value: [] }));
            }
            return new Response(JSON.stringify({ message: 'Bridge Gateway Timeout' }), { status: 504 });
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            await assert.rejects(
                async () => await runListingDelta(env),
                /Bridge API error HTTP 504/
            );

            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState.status, 'failure');
            assert.equal(syncState.last_cursor, null);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('5. Delta failure preserves existing successful cursor and status=failure; next execution enters DELTA mode', async () => {
        const { d1, sqlite } = createTestDatabase();
        const initialCursor = '2026-08-31T12:00:00.000Z';

        // 1. Seed successful bootstrap
        sqlite.prepare(`
            INSERT INTO sneak_sync_state (sync_name, last_cursor, last_successful_sync, last_record_count, status)
            VALUES ('listings', ?, ?, 2, 'success');
        `).run(initialCursor, initialCursor);

        sqlite.prepare(`
            INSERT INTO sneak_listings (ListingKey, StandardStatus, ListPrice)
            VALUES ('LST_1', 'Active', 500000), ('LST_2', 'Active', 600000);
        `).run();

        // 2. Simulate failed delta (e.g. Bridge 500)
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async () => {
            return new Response(JSON.stringify({ message: 'Internal Bridge Error' }), { status: 500 });
        };

        const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
        await assert.rejects(
            async () => await runListingDelta(env),
            /Bridge API error HTTP 500/
        );

        // Verify: cursor remains initialCursor, status becomes failure
        let syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
        assert.equal(syncState.status, 'failure');
        assert.equal(syncState.last_cursor, initialCursor, 'existing cursor must be preserved');
        assert.equal(syncState.last_successful_sync, initialCursor, 'last_successful_sync must be preserved');

        // 3. Next execution: MUST enter DELTA MODE (using initialCursor - 5 min), NOT full bootstrap
        let capturedDeltaFilter = null;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            capturedDeltaFilter = u.searchParams.get('$filter');
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 1, value: [] }));
            }
            return new Response(JSON.stringify({
                '@odata.count': 1,
                value: [mockBridgeListing('LST_1', { ListPrice: 510000 })]
            }));
        };

        try {
            const retryResult = await runListingDelta(env);
            assert.equal(retryResult.status, 'success');
            assert.equal(retryResult.mode, 'delta', 'Must enter DELTA mode, NOT bootstrap');
            assert.match(capturedDeltaFilter, /ModificationTimestamp ge 2026-08-31T11:55:00\.000Z/);

            // On success: status returns to success and cursor advances
            syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState.status, 'success');
            assert.notEqual(syncState.last_cursor, initialCursor, 'cursor advanced on successful retry');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('6. Lock renewal extends lease for owner, fails for non-owner, and protects from cron overlap', async () => {
        const { d1, sqlite } = createTestDatabase();

        // 1. Acquire lock L1
        const lock1 = await acquireLock(d1, 'test_job', 300);
        assert.ok(lock1);

        // 2. Owner renews lock L1 -> succeeds
        const renewed = await renewLock(d1, 'test_job', lock1, 600);
        assert.equal(renewed, true);

        // 3. Different lock ID tries to renew -> returns false
        const fakeRenew = await renewLock(d1, 'test_job', 'fake_lock_id', 600);
        assert.equal(fakeRenew, false);

        // 4. Concurrency protection: second process cannot acquire lock while active
        const lock2 = await acquireLock(d1, 'test_job', 300);
        assert.equal(lock2, null, 'Second process cannot acquire active lock');

        // 5. Release lock
        await releaseLock(d1, 'test_job', lock1);
        const lock3 = await acquireLock(d1, 'test_job', 300);
        assert.ok(lock3, 'Can acquire lock after release');

        sqlite.close();
    });

    test('7. Lock ownership loss during bootstrap aborts immediately (SyncLockLost), does NOT prune, does NOT advance cursor', async () => {
        const { d1, sqlite } = createTestDatabase();
        // Seed preexisting D1 listings
        sqlite.prepare("INSERT INTO sneak_listings (ListingKey, StandardStatus) VALUES ('LST_PREEXISTING', 'Active');").run();

        const originalFetch = globalThis.fetch;
        let pageCount = 0;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 2, value: [] }));
            }
            pageCount++;
            // On page 1: simulate external lock expiration / theft by another process
            sqlite.prepare("DELETE FROM sneak_sync_locks WHERE job_name = 'listing_delta';").run();
            return new Response(JSON.stringify({
                '@odata.count': 2,
                value: [mockBridgeListing('LST_BOOT_1')]
            }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            await assert.rejects(
                async () => await runListingDelta(env),
                /SyncLockLost/
            );

            // Invariants on lock loss:
            // 1. Cursor must NOT be committed
            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState.last_cursor, null);
            assert.equal(syncState.status, 'failure');

            // 2. Preexisting rows must NOT have been pruned
            const preexisting = sqlite.prepare("SELECT * FROM sneak_listings WHERE ListingKey = 'LST_PREEXISTING'").get();
            assert.ok(preexisting, 'Preexisting listing was NOT pruned on lock loss');

            // 3. Failure run logged with SYNC_LOCK_LOST
            const run = sqlite.prepare("SELECT * FROM sneak_sync_runs WHERE status = 'failure'").get();
            assert.equal(run.error_code, 'SYNC_LOCK_LOST');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('8. Bounded-memory bootstrap: batches D1 writes during pagination and discards statements per page', async () => {
        const { d1, sqlite } = createTestDatabase();
        const batchCalls = [];

        // Wrap D1 batch to track invocation points relative to pages
        const trackingD1 = {
            ...d1,
            async batch(statements) {
                batchCalls.push(statements.length);
                return await d1.batch(statements);
            }
        };

        // Create 3 pages of 5 listings each
        const page1 = [mockBridgeListing('P1_1'), mockBridgeListing('P1_2')];
        const page2 = [mockBridgeListing('P2_1'), mockBridgeListing('P2_2')];
        const page3 = [mockBridgeListing('P3_1'), mockBridgeListing('P3_2')];

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 6, value: [] }));
            }
            const skip = Number(u.searchParams.get('$skip') || 0);
            if (skip === 0) {
                return new Response(JSON.stringify({
                    '@odata.count': 6,
                    '@odata.nextLink': 'https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$skip=2',
                    value: page1
                }));
            } else if (skip === 2) {
                return new Response(JSON.stringify({
                    '@odata.count': 6,
                    '@odata.nextLink': 'https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$skip=4',
                    value: page2
                }));
            } else {
                return new Response(JSON.stringify({
                    '@odata.count': 6,
                    value: page3
                }));
            }
        };

        try {
            const env = { DB: trackingD1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            const result = await runListingDelta(env);

            assert.equal(result.status, 'success');
            assert.equal(result.recordsFetched, 6);
            assert.equal(result.finalListingCount, 6);

            // Verify D1 batch was called at least once per page (streaming), not just once at the end
            assert.ok(batchCalls.length >= 3, `D1 batch was called ${batchCalls.length} times during pagination`);
            for (const callSize of batchCalls) {
                assert.ok(callSize <= 50, `Each batch size ${callSize} <= 50`);
            }
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('9. Failure after partial streamed writes leaves partial rows safely replaceable, cursor unset, no stale pruning', async () => {
        const { d1, sqlite } = createTestDatabase();
        sqlite.prepare("INSERT INTO sneak_listings (ListingKey, StandardStatus) VALUES ('LST_OLD_KEEP', 'Active');").run();

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 4, value: [] }));
            }
            const skip = Number(u.searchParams.get('$skip') || 0);
            if (skip === 0) {
                return new Response(JSON.stringify({
                    '@odata.count': 4,
                    '@odata.nextLink': 'https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$skip=2',
                    value: [mockBridgeListing('LST_STREAM_1'), mockBridgeListing('LST_STREAM_2')]
                }));
            }
            // Page 2 fails!
            return new Response(JSON.stringify({ message: 'Network Failure' }), { status: 502 });
        };

        const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
        await assert.rejects(
            async () => await runListingDelta(env),
            /Bridge API error HTTP 502/
        );

        // Invariants on mid-stream failure:
        // 1. Partial rows may exist in D1
        const streamRow1 = sqlite.prepare("SELECT * FROM sneak_listings WHERE ListingKey = 'LST_STREAM_1'").get();
        assert.ok(streamRow1, 'Partial streamed row was written to D1');

        // 2. Old rows must NOT be pruned (stale pruning happens only after complete scan)
        const oldRow = sqlite.prepare("SELECT * FROM sneak_listings WHERE ListingKey = 'LST_OLD_KEEP'").get();
        assert.ok(oldRow, 'Stale pruning did not occur');

        // 3. Cursor remains unset and status=failure
        const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
        assert.equal(syncState.last_cursor, null);
        assert.equal(syncState.status, 'failure');

        // 4. Retry succeeds and produces exact final state
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 2, value: [] }));
            }
            return new Response(JSON.stringify({
                '@odata.count': 2,
                value: [mockBridgeListing('LST_STREAM_1', { ListPrice: 800000 }), mockBridgeListing('LST_STREAM_2')]
            }));
        };

        try {
            const retryResult = await runListingDelta(env);
            assert.equal(retryResult.status, 'success');
            assert.equal(retryResult.mode, 'bootstrap', 'Enters bootstrap again because cursor was unset');
            assert.equal(retryResult.recordsRemoved, 1, 'LST_OLD_KEEP pruned after complete successful scan');

            const allRows = sqlite.prepare("SELECT ListingKey, ListPrice FROM sneak_listings ORDER BY ListingKey ASC").all();
            assert.deepEqual(allRows.map(r => r.ListingKey), ['LST_STREAM_1', 'LST_STREAM_2']);
            assert.equal(allRows[0].ListPrice, 800000);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('10. Large reconciliation streaming fallback streams repairs in bounded batches and verifies exact inventory', async () => {
        const { d1, sqlite } = createTestDatabase();

        // Seed D1 with 1 listing
        sqlite.prepare("INSERT INTO sneak_listings (ListingKey, StandardStatus) VALUES ('EXISTING_1', 'Active');").run();

        // Create 505 missing keys (exceeds LARGE_MISSING_THRESHOLD = 500)
        const missingKeys = [];
        for (let i = 1; i <= 505; i++) {
            missingKeys.push(`MISSING_${i}`);
        }
        const allBridgeKeys = ['EXISTING_1', ...missingKeys];

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            const select = u.searchParams.get('$select');
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': allBridgeKeys.length, value: [] }));
            }
            if (select === 'ListingKey') {
                return new Response(JSON.stringify({
                    '@odata.count': allBridgeKeys.length,
                    value: allBridgeKeys.map(k => ({ ListingKey: k }))
                }));
            }
            // Full current scan for large fallback
            return new Response(JSON.stringify({
                '@odata.count': allBridgeKeys.length,
                value: allBridgeKeys.map(k => mockBridgeListing(k))
            }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            const result = await runListingReconciliation(env);

            assert.equal(result.status, 'success');
            assert.equal(result.missingKeysFound, 505);
            assert.equal(result.missingKeysRepaired, 505);
            assert.equal(result.finalD1Count, 506);

            const countRow = sqlite.prepare("SELECT count(*) as count FROM sneak_listings").get();
            assert.equal(countRow.count, 506);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('11. Reconciliation lock ownership loss fails closed, does not prune, does not update checkpoint', async () => {
        const { d1, sqlite } = createTestDatabase();
        sqlite.prepare("INSERT INTO sneak_listings (ListingKey, StandardStatus) VALUES ('LST_A', 'Active'), ('LST_STALE', 'Active');").run();

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            const select = u.searchParams.get('$select');
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 2, value: [] }));
            }
            if (select === 'ListingKey') {
                // Steal/expire lock before key enumeration completes
                sqlite.prepare("DELETE FROM sneak_sync_locks WHERE job_name = 'listing_reconciliation';").run();
                return new Response(JSON.stringify({
                    '@odata.count': 2,
                    value: [{ ListingKey: 'LST_A' }, { ListingKey: 'LST_B' }]
                }));
            }
            return new Response(JSON.stringify({ value: [] }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            await assert.rejects(
                async () => await runListingReconciliation(env),
                /SyncLockLost/
            );

            // Verify: stale row was NOT pruned
            const staleRow = sqlite.prepare("SELECT * FROM sneak_listings WHERE ListingKey = 'LST_STALE'").get();
            assert.ok(staleRow, 'Stale row was preserved on lock loss');

            // Verify: reconciliation checkpoint was NOT updated
            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState, undefined);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('12. Worker /health truthful state progression through lifecycle', async () => {
        const { d1, sqlite } = createTestDatabase();
        const env = { DB: d1, SNEAK_ENV: 'production', SNEAK_SERVICE_NAME: 'sneak-idx-sync' };

        // 1. Fresh state: NOT_INITIALIZED
        let res = await worker.fetch(new Request('https://sync.internal/health'), env);
        let data = await res.json();
        assert.equal(data.syncReadiness, 'NOT_INITIALIZED');

        // 2. Active bootstrap: BOOTSTRAPPING
        sqlite.prepare("INSERT INTO sneak_sync_state (sync_name, status) VALUES ('listings', 'running');").run();
        res = await worker.fetch(new Request('https://sync.internal/health'), env);
        data = await res.json();
        assert.equal(data.syncReadiness, 'BOOTSTRAPPING');

        // 3. Failed bootstrap: FAILED
        sqlite.prepare("UPDATE sneak_sync_state SET status = 'failure' WHERE sync_name = 'listings';").run();
        res = await worker.fetch(new Request('https://sync.internal/health'), env);
        data = await res.json();
        assert.equal(data.syncReadiness, 'FAILED');

        // 4. Successful bootstrap: HEALTHY
        sqlite.prepare(`
            UPDATE sneak_sync_state
            SET status = 'success', last_cursor = '2026-09-01T12:00:00.000Z', last_successful_sync = '2026-09-01T12:00:00.000Z', last_record_count = 100
            WHERE sync_name = 'listings';
        `).run();
        res = await worker.fetch(new Request('https://sync.internal/health'), env);
        data = await res.json();
        assert.equal(data.syncReadiness, 'HEALTHY');

        // 5. Delta failure with preserved cursor: FAILED
        sqlite.prepare("UPDATE sneak_sync_state SET status = 'failure' WHERE sync_name = 'listings';").run();
        res = await worker.fetch(new Request('https://sync.internal/health'), env);
        data = await res.json();
        assert.equal(data.syncReadiness, 'FAILED');
        assert.equal(data.syncState.last_cursor, '2026-09-01T12:00:00.000Z', 'Cursor remains preserved');

        // 6. Delta recovery: HEALTHY
        sqlite.prepare("UPDATE sneak_sync_state SET status = 'success', last_cursor = '2026-09-01T12:15:00.000Z' WHERE sync_name = 'listings';").run();
        res = await worker.fetch(new Request('https://sync.internal/health'), env);
        data = await res.json();
        assert.equal(data.syncReadiness, 'HEALTHY');

        sqlite.close();
    });

    test('13. Admin readiness blocks launch on sync failure with SYNC_FAILED blocker', async () => {
        const { d1, sqlite } = createTestDatabase();

        // Seed account, site, domain, branding, widget, member, entitlement
        const tables = {
            accounts: [{ id: 'acc_1', status: 'active' }],
            sites: [{ id: 'site_1', account_id: 'acc_1', site_key: 'site_1', status: 'active', scope_type: 'market', scope_value: null }],
            domains: [{ id: 'dom_1', site_id: 'site_1', domain: 'example.com', verified: 1, status: 'active' }],
            branding: [{ site_id: 'site_1', display_name: 'Test', brokerage: 'Brokerage', email: 'test@example.com' }],
            widgets: [{ site_id: 'site_1', widget_type: 'search', enabled: 1 }],
            members: [{ id: 'mem_1', account_id: 'acc_1', email: 'member@example.com', status: 'active' }],
            entitlements: [{ account_id: 'acc_1', status: 'active' }],
            sync: { sync_name: 'listings', last_cursor: '2026-09-01T12:00:00.000Z', last_successful_sync: new Date().toISOString(), status: 'failure' },
            listings: [{ ListingKey: 'L1' }]
        };

        const mockAdminDb = {
            prepare(sql) {
                const q = sql.replace(/\s+/g, ' ').trim();
                function makeResult(args = []) {
                    return {
                        async first() {
                            if (q.includes("FROM sneak_sync_state WHERE sync_name = 'listings'")) return tables.sync;
                            if (q.includes('FROM sneak_accounts WHERE id = ?')) return tables.accounts[0];
                            if (q.includes('FROM sneak_sites WHERE account_id = ?')) return tables.sites[0];
                            if (q.includes('FROM sneak_sites WHERE site_key = ?')) return tables.sites[0];
                            if (q.includes('COUNT(*) AS count FROM sneak_listings')) return { count: 1 };
                            if (q.includes('SELECT count(*) as count FROM sneak_listings')) return { count: 1 };
                            if (q.includes('SELECT display_name')) return tables.branding[0];
                            return null;
                        },
                        async all() {
                            if (q.includes('FROM sneak_sites WHERE account_id = ?')) return { results: tables.sites };
                            if (q.includes('FROM sneak_member_users WHERE account_id = ?')) return { results: tables.members };
                            if (q.includes('FROM sneak_domains WHERE site_id = ?')) return { results: tables.domains };
                            if (q.includes('FROM sneak_widget_configs WHERE site_id = ?')) return { results: tables.widgets };
                            return { results: [] };
                        }
                    };
                }
                return {
                    bind(...args) {
                        return makeResult(args);
                    },
                    ...makeResult([])
                };
            }
        };

        const healthyBindings = {
            servingWorker: { async fetch() { return new Response(JSON.stringify({ status: 'ok' })); } },
            memberWorker: { async fetch() { return new Response(JSON.stringify({ status: 'ok' })); } },
            alertWorker: { async fetch() { return new Response(JSON.stringify({ enabled: false, deliveryReady: false })); } }
        };

        const readiness = await calculateAccountReadiness(mockAdminDb, 'acc_1', healthyBindings);
        assert.equal(readiness.launchReady, false, 'Launch is blocked when sync status is failure');
        const blockerCodes = readiness.launchBlockers.map(b => b.code);
        assert.ok(blockerCodes.includes('SYNC_FAILED'), 'SYNC_FAILED blocker is present');

        sqlite.close();
    });

    test('14. Open house sync cleanly integrates with bootstrapped listings', async () => {
        const { d1, sqlite } = createTestDatabase();
        sqlite.prepare("INSERT INTO sneak_listings (ListingKey, StandardStatus) VALUES ('LST_OH_READY', 'Active');").run();

        const tomorrow = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 1, value: [] }));
            }
            return new Response(JSON.stringify({
                '@odata.count': 1,
                value: [{
                    OpenHouseKey: 'OH_OP_1',
                    ListingKey: 'LST_OH_READY',
                    OpenHouseDate: tomorrow,
                    OpenHouseStartTime: '13:00:00',
                    OpenHouseEndTime: '16:00:00',
                    OpenHouseType: 'Public',
                    Status: 'Active'
                }]
            }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            const result = await runOpenHouseSync(env);

            assert.equal(result.status, 'success');
            assert.equal(result.validEvents, 1);

            const ohRows = sqlite.prepare("SELECT * FROM sneak_open_houses").all();
            assert.equal(ohRows.length, 1);
            assert.equal(ohRows[0].OpenHouseKey, 'OH_OP_1');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });
});
