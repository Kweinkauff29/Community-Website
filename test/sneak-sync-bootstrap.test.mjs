/**
 * test/sneak-sync-bootstrap.test.mjs
 * 
 * SNEAK IDX Phase 7.4B2A.3 — Production Full-Inventory Bootstrap + Self-Healing Reconciliation
 */

import { test, describe, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import worker, {
    runListingDelta,
    runFullInventoryBootstrap,
    runListingReconciliation,
    runOpenHouseSync
} from '../sneak-sync/worker.js';

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
        ModificationTimestamp: '2026-07-15T10:00:00.000Z', // 45+ days old to prove no 24-hr limit
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

describe('SNEAK IDX Phase 7.4B2A.3 — Production Full-Inventory Bootstrap + Self-Healing Reconciliation', () => {

    test('1. Empty D1 + No Cursor executes Full Inventory Bootstrap (no 24-hr limit, MediaJSON populated, cursor committed)', async () => {
        const { d1, sqlite } = createTestDatabase();
        const listings = [
            mockBridgeListing('LST_BOOT_1', { ModificationTimestamp: '2026-06-01T12:00:00.000Z' }),
            mockBridgeListing('LST_BOOT_2', { ModificationTimestamp: '2026-07-20T15:30:00.000Z' }),
            mockBridgeListing('LST_BOOT_3', { ModificationTimestamp: '2026-08-30T09:00:00.000Z' })
        ];

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
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

            // Verify sneak_listings populated in D1
            const rows = sqlite.prepare("SELECT * FROM sneak_listings ORDER BY ListingKey ASC").all();
            assert.equal(rows.length, 3);
            assert.equal(rows[0].ListingKey, 'LST_BOOT_1');
            assert.equal(rows[0].PrimaryPhoto, 'https://images.example.com/LST_BOOT_1/1.jpg');
            const mediaObj = JSON.parse(rows[0].MediaJSON);
            assert.equal(mediaObj.length, 2);

            // Verify sneak_sync_state has cursor committed
            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.ok(syncState.last_cursor, 'last_cursor is committed');
            assert.equal(syncState.last_record_count, 3);
            assert.equal(syncState.status, 'success');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('2. Partial D1 + No Cursor enters Bootstrap Mode, fills missing inventory, prunes stale rows idempotently', async () => {
        const { d1, sqlite } = createTestDatabase();
        // Pre-insert valid item A and stale item OLD_STALE
        sqlite.prepare(`
            INSERT INTO sneak_listings (ListingKey, StandardStatus, ListPrice)
            VALUES ('LST_A', 'Active', 500000), ('LST_OLD_STALE', 'Active', 400000);
        `).run();

        const currentBridgeListings = [
            mockBridgeListing('LST_A', { ListPrice: 525000 }), // price updated
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
            assert.equal(result.recordsRemoved, 1, 'Preexisting stale row LST_OLD_STALE was pruned');

            const rows = sqlite.prepare("SELECT ListingKey, ListPrice FROM sneak_listings ORDER BY ListingKey ASC").all();
            assert.deepEqual(rows.map(r => r.ListingKey), ['LST_A', 'LST_B', 'LST_C']);
            assert.equal(rows[0].ListPrice, 525000, 'LST_A updated idempotently');

            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.ok(syncState.last_cursor);
            assert.equal(syncState.last_record_count, 3);
            assert.equal(syncState.status, 'success');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('3. Empty Bridge inventory fails closed on initial bootstrap without test override', async () => {
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

            // Cursor must remain unset
            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState, undefined);

            const run = sqlite.prepare("SELECT * FROM sneak_sync_runs WHERE status = 'failure'").get();
            assert.ok(run, 'Sync failure was recorded');
            assert.equal(run.error_code, 'BOOTSTRAP_SYNC_ERROR');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('4. Bootstrap failure on page fetch does not commit cursor and allows safe restart', async () => {
        const { d1, sqlite } = createTestDatabase();

        const originalFetch = globalThis.fetch;
        let callCount = 0;
        globalThis.fetch = async (urlStr) => {
            callCount++;
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

            // Verify cursor is NOT committed
            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState, undefined);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('5. Bootstrap count mismatch throws and fails closed', async () => {
        const { d1, sqlite } = createTestDatabase();

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 3, value: [] }));
            }
            // Returns only 2 records instead of expected 3
            return new Response(JSON.stringify({
                '@odata.count': 3,
                value: [mockBridgeListing('LST_1'), mockBridgeListing('LST_2')]
            }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            await assert.rejects(
                async () => await runListingDelta(env),
                /Bootstrap completeness shortfall/
            );

            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState, undefined);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('6. Duplicate ListingKey anomaly throws and fails closed', async () => {
        const { d1, sqlite } = createTestDatabase();

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 2, value: [] }));
            }
            // Duplicate ListingKey
            return new Response(JSON.stringify({
                '@odata.count': 2,
                value: [mockBridgeListing('LST_DUP'), mockBridgeListing('LST_DUP')]
            }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            await assert.rejects(
                async () => await runListingDelta(env),
                /Bootstrap duplicate anomaly/
            );

            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.equal(syncState, undefined);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('7. Delta mode after successful bootstrap queries only changes with 5-minute overlap', async () => {
        const { d1, sqlite } = createTestDatabase();
        // Seed completed bootstrap state
        const bootstrapCursor = '2026-08-31T12:00:00.000Z';
        sqlite.prepare(`
            INSERT INTO sneak_sync_state (sync_name, last_cursor, last_successful_sync, last_record_count, status)
            VALUES ('listings', ?, ?, 2, 'success');
        `).run(bootstrapCursor, bootstrapCursor);

        sqlite.prepare(`
            INSERT INTO sneak_listings (ListingKey, StandardStatus, ListPrice)
            VALUES ('LST_EXISTING_1', 'Active', 500000), ('LST_EXISTING_2', 'Active', 600000);
        `).run();

        let capturedDeltaFilter = null;
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            capturedDeltaFilter = u.searchParams.get('$filter');
            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 2, value: [] }));
            }
            return new Response(JSON.stringify({
                '@odata.count': 2,
                value: [
                    mockBridgeListing('LST_EXISTING_1', { ListPrice: 510000 }), // updated
                    mockBridgeListing('LST_NEW_3') // newly added
                ]
            }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            const result = await runListingDelta(env);

            assert.equal(result.status, 'success');
            assert.equal(result.mode, 'delta');
            assert.match(capturedDeltaFilter, /ModificationTimestamp ge 2026-08-31T11:55:00\.000Z/);

            const rows = sqlite.prepare("SELECT ListingKey, ListPrice FROM sneak_listings ORDER BY ListingKey ASC").all();
            assert.equal(rows.length, 3);
            assert.equal(rows.find(r => r.ListingKey === 'LST_EXISTING_1').ListPrice, 510000);
            assert.ok(rows.find(r => r.ListingKey === 'LST_NEW_3'));
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('8. Self-healing reconciliation repairs missing listings via targeted hydration and prunes stale rows', async () => {
        const { d1, sqlite } = createTestDatabase();
        // D1 has LST_A, LST_C, and LST_STALE.
        // Bridge has LST_A, LST_B, LST_C.
        // Missing in D1: LST_B. Stale in D1: LST_STALE.
        sqlite.prepare(`
            INSERT INTO sneak_listings (ListingKey, StandardStatus)
            VALUES ('LST_A', 'Active'), ('LST_C', 'Active'), ('LST_STALE', 'Active');
        `).run();

        const bridgeKeyList = ['LST_A', 'LST_B', 'LST_C'];
        let targetedFetchUrl = null;

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            const select = u.searchParams.get('$select');
            const filter = u.searchParams.get('$filter');

            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 3, value: [] }));
            }

            // Key enumeration query ($select=ListingKey)
            if (select === 'ListingKey') {
                return new Response(JSON.stringify({
                    '@odata.count': 3,
                    value: bridgeKeyList.map(k => ({ ListingKey: k }))
                }));
            }

            // Targeted repair query
            targetedFetchUrl = urlStr;
            if (filter.includes("ListingKey eq 'LST_B'")) {
                return new Response(JSON.stringify({
                    value: [mockBridgeListing('LST_B')]
                }));
            }

            return new Response(JSON.stringify({ value: [] }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            const result = await runListingReconciliation(env);

            assert.equal(result.status, 'success');
            assert.equal(result.missingKeysFound, 1);
            assert.equal(result.missingKeysRepaired, 1);
            assert.equal(result.staleKeysFound, 1);
            assert.equal(result.staleKeysPruned, 1);
            assert.equal(result.finalD1Count, 3);

            assert.match(decodeURIComponent(targetedFetchUrl), /ListingKey(\+| )eq(\+| )'LST_B'/);

            // Verify final D1 inventory is exactly LST_A, LST_B, LST_C
            const rows = sqlite.prepare("SELECT ListingKey FROM sneak_listings ORDER BY ListingKey ASC").all();
            assert.deepEqual(rows.map(r => r.ListingKey), ['LST_A', 'LST_B', 'LST_C']);

            const syncState = sqlite.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").get();
            assert.ok(syncState.last_full_reconciliation);
            assert.equal(syncState.last_record_count, 3);
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('9. Reconciliation repair failure fails closed when missing key cannot be retrieved', async () => {
        const { d1, sqlite } = createTestDatabase();
        sqlite.prepare("INSERT INTO sneak_listings (ListingKey, StandardStatus) VALUES ('LST_A', 'Active');").run();

        const originalFetch = globalThis.fetch;
        globalThis.fetch = async (urlStr) => {
            const u = new URL(urlStr);
            const select = u.searchParams.get('$select');

            if (u.searchParams.get('$count') === 'true') {
                return new Response(JSON.stringify({ '@odata.count': 2, value: [] }));
            }
            if (select === 'ListingKey') {
                return new Response(JSON.stringify({
                    '@odata.count': 2,
                    value: [{ ListingKey: 'LST_A' }, { ListingKey: 'LST_MISSING' }]
                }));
            }
            // Simulates failure to return missing listing
            return new Response(JSON.stringify({ value: [] }));
        };

        try {
            const env = { DB: d1, BRIDGE_TOKEN: 'mock_token', SNEAK_ENV: 'production' };
            await assert.rejects(
                async () => await runListingReconciliation(env),
                /Reconciliation repair shortfall/
            );
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('10. Open house sync populates forward horizon for bootstrapped listing', async () => {
        const { d1, sqlite } = createTestDatabase();
        // Seed bootstrapped listing
        sqlite.prepare(`
            INSERT INTO sneak_listings (ListingKey, StandardStatus)
            VALUES ('LST_WITH_OH', 'Active');
        `).run();

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
                    OpenHouseKey: 'OH_BOOTSTRAP_1',
                    ListingKey: 'LST_WITH_OH',
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
            assert.equal(ohRows[0].OpenHouseKey, 'OH_BOOTSTRAP_1');
            assert.equal(ohRows[0].ListingKey, 'LST_WITH_OH');
        } finally {
            globalThis.fetch = originalFetch;
            sqlite.close();
        }
    });

    test('11. Sync Worker health endpoint reports accurate syncReadiness and metadata', async () => {
        const { d1, sqlite } = createTestDatabase();
        const env = { DB: d1, SNEAK_ENV: 'production', SNEAK_SERVICE_NAME: 'sneak-idx-sync' };

        // 1. Initial / Uninitialized state
        let res = await worker.fetch(new Request('https://sync.internal/health'), env);
        let data = await res.json();
        assert.equal(data.syncReadiness, 'NOT_INITIALIZED');
        assert.equal(data.syncState.last_cursor, null);
        assert.equal(data.syncState.last_record_count, 0);

        // 2. Post-Bootstrap Healthy state
        sqlite.prepare(`
            INSERT INTO sneak_sync_state (sync_name, last_cursor, last_successful_sync, last_record_count, status)
            VALUES ('listings', '2026-09-01T12:00:00.000Z', '2026-09-01T12:00:00.000Z', 1500, 'success');
        `).run();

        res = await worker.fetch(new Request('https://sync.internal/health'), env);
        data = await res.json();
        assert.equal(data.syncReadiness, 'HEALTHY');
        assert.equal(data.syncState.last_cursor, '2026-09-01T12:00:00.000Z');
        assert.equal(data.syncState.last_record_count, 1500);
        assert.equal(data.syncState.status, 'success');

        sqlite.close();
    });
});
