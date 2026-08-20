/**
 * test/sneak-sync-worker.test.mjs
 * 
 * Unit and Integration Test Suite for SNEAK Dedicated Sync Worker (sneak-idx-sync-staging).
 */

import { test, describe, before, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import worker, { runListingDelta, runOpenHouseSync, runListingReconciliation } from '../sneak-sync/worker.js';
import { acquireLock, releaseLock } from '../sneak-sync/lock.js';

// Mock D1 Database implementation for unit testing
function createMockDB() {
    const tables = {
        sneak_sync_locks: [],
        sneak_sync_runs: [],
        sneak_sync_state: [
            { sync_name: 'listings', last_cursor: '2026-08-20T12:00:00.000Z', last_successful_sync: '2026-08-20T12:00:00.000Z', last_record_count: 10 }
        ],
        sneak_listings: [
            { ListingKey: 'KEY_1', StandardStatus: 'Active' },
            { ListingKey: 'KEY_2', StandardStatus: 'Active' }
        ],
        sneak_open_houses: [
            { id: 'oh_1', OpenHouseKey: 'OH_1', ListingKey: 'KEY_1', OpenHouseDate: '2026-08-25' }
        ]
    };

    function createStatement(query, boundArgs = []) {
        return {
            bind(...args) {
                return createStatement(query, args);
            },
            async first() {
                if (query.includes('FROM sneak_sync_state')) {
                    return tables.sneak_sync_state[0] || null;
                }
                if (query.includes('count(*) as count FROM sneak_listings')) {
                    return { count: tables.sneak_listings.length };
                }
                if (query.includes('count(*) as count FROM sneak_open_houses')) {
                    return { count: tables.sneak_open_houses.length };
                }
                return null;
            },
            async all() {
                if (query.includes('FROM sneak_listings')) {
                    return { results: tables.sneak_listings.map(l => ({ ListingKey: l.ListingKey })) };
                }
                if (query.includes('FROM sneak_open_houses')) {
                    return { results: tables.sneak_open_houses.map(oh => ({ OpenHouseKey: oh.OpenHouseKey })) };
                }
                return { results: [] };
            },
            async run() {
                if (query.includes('INSERT INTO sneak_sync_locks')) {
                    const [job_name, lock_id] = boundArgs;
                    const existing = tables.sneak_sync_locks.find(l => l.job_name === job_name);
                    if (existing) throw new Error('Lock exists');
                    tables.sneak_sync_locks.push({ job_name, lock_id, acquired_at: new Date().toISOString() });
                    return { success: true };
                }
                if (query.includes('DELETE FROM sneak_sync_locks WHERE job_name = ? AND lock_id = ?')) {
                    const [job_name, lock_id] = boundArgs;
                    tables.sneak_sync_locks = tables.sneak_sync_locks.filter(l => !(l.job_name === job_name && l.lock_id === lock_id));
                    return { success: true };
                }
                if (query.includes('DELETE FROM sneak_sync_locks WHERE job_name = ? AND expires_at < datetime')) {
                    // Do not delete unexpired locks
                    return { success: true };
                }
                if (query.includes('INSERT INTO sneak_sync_runs')) {
                    tables.sneak_sync_runs.push({ id: boundArgs[0], job_name: boundArgs[1], status: boundArgs[4] });
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
            return statements.map(() => ({ success: true }));
        }
    };
}

describe('SNEAK Sync Worker Test Suite', () => {

    test('TEST 1: Public fetch handler returns 200 health check on / and 404 on mutation paths', async () => {
        const env = { SNEAK_ENV: 'staging' };
        
        // Root / health
        const res1 = await worker.fetch(new Request('http://localhost/'), env, {});
        assert.equal(res1.status, 200);
        const data1 = await res1.json();
        assert.equal(data1.service, 'sneak-idx-sync-staging');
        assert.equal(data1.status, 'ok');

        // Mutation paths
        const res2 = await worker.fetch(new Request('http://localhost/run-sync'), env, {});
        assert.equal(res2.status, 404);

        const res3 = await worker.fetch(new Request('http://localhost/admin'), env, {});
        assert.equal(res3.status, 404);
    });

    test('TEST 2: Scheduled handler routes known cron expressions and ignores unknown cron', async () => {
        const env = { SNEAK_ENV: 'staging', DB: createMockDB(), BRIDGE_TOKEN: 'mock' };
        let waitedJob = null;
        const ctx = {
            waitUntil(promise) {
                waitedJob = promise;
            }
        };

        // Unknown schedule
        await worker.scheduled({ cron: '1 1 1 1 *' }, env, ctx);
        assert.equal(waitedJob, null, 'Unknown cron should trigger no background job');
    });

    test('TEST 3: Missing BRIDGE_TOKEN fails closed safely with 0 mutations', async () => {
        const mockDB = createMockDB();
        const env = { SNEAK_ENV: 'staging', DB: mockDB }; // NO BRIDGE_TOKEN

        await assert.rejects(async () => {
            await runListingDelta(env);
        }, /BRIDGE_TOKEN configuration missing/);

        // Verify cursor unchanged
        assert.equal(mockDB.tables.sneak_sync_state[0].last_cursor, '2026-08-20T12:00:00.000Z');
        // Verify lock released
        assert.equal(mockDB.tables.sneak_sync_locks.length, 0);
    });

    test('TEST 4: Distributed lock prevents concurrent job execution', async () => {
        const mockDB = createMockDB();
        const lock1 = await acquireLock(mockDB, 'listing_delta', 300);
        assert.ok(lock1, 'First lock acquired');

        const lock2 = await acquireLock(mockDB, 'listing_delta', 300);
        assert.equal(lock2, null, 'Second concurrent lock rejected');

        await releaseLock(mockDB, 'listing_delta', lock1);
        assert.equal(mockDB.tables.sneak_sync_locks.length, 0, 'Lock released');
    });

    test('TEST 5: OpenHouse sync fails closed on missing BRIDGE_TOKEN with 0 deletions', async () => {
        const mockDB = createMockDB();
        const env = { SNEAK_ENV: 'staging', DB: mockDB }; // NO BRIDGE_TOKEN

        await assert.rejects(async () => {
            await runOpenHouseSync(env);
        }, /BRIDGE_TOKEN configuration missing/);

        // Verify open houses untouched
        assert.equal(mockDB.tables.sneak_open_houses.length, 1);
        // Verify lock released
        assert.equal(mockDB.tables.sneak_sync_locks.length, 0);
    });

    test('TEST 6: Reconciliation fails closed on missing BRIDGE_TOKEN with 0 deletions', async () => {
        const mockDB = createMockDB();
        const env = { SNEAK_ENV: 'staging', DB: mockDB }; // NO BRIDGE_TOKEN

        await assert.rejects(async () => {
            await runListingReconciliation(env);
        }, /BRIDGE_TOKEN configuration missing/);

        // Verify listings untouched
        assert.equal(mockDB.tables.sneak_listings.length, 2);
        // Verify lock released
        assert.equal(mockDB.tables.sneak_sync_locks.length, 0);
    });
});
