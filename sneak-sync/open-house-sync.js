/**
 * sneak-sync/open-house-sync.js
 * 
 * Worker-native Real Open House Synchronizer & Reconciliation.
 */

import {
    BRIDGE_OPENHOUSE_ENDPOINT,
    MAX_PAGE_SIZE,
    fetchBridgeOData
} from './bridge.js';
import { transformOpenHouseRecord } from './transforms.js';
import { acquireLock, releaseLock, recordSyncRun } from './lock.js';

const BATCH_CHUNK_SIZE = 50;

export async function runOpenHouseSync(env) {
    const jobName = 'open_house_sync';
    const startedAt = new Date().toISOString();
    const startTime = Date.now();

    // 1. Acquire Distributed Lock
    const lockId = await acquireLock(env.DB, jobName, 300);
    if (!lockId) {
        console.log(JSON.stringify({ job: jobName, status: 'skipped', reason: 'concurrency_lock_active', startedAt }));
        return { status: 'skipped', reason: 'locked' };
    }

    let recordsFetched = 0;
    let validEvents = 0;
    let offMarketSkipped = 0;
    let cancelledSkipped = 0;
    let stalePruned = 0;
    let bridgePages = 0;
    let d1Operations = 0;

    try {
        // 2. Query Active ListingKeys from sneak_listings in D1
        const activeKeys = new Set();
        let offset = 0;
        const pageSize = 10000;
        let hasMore = true;

        while (hasMore) {
            const rows = await env.DB.prepare(`SELECT ListingKey FROM sneak_listings LIMIT ? OFFSET ?`).bind(pageSize, offset).all();
            const list = rows.results || [];
            for (const r of list) {
                if (r.ListingKey) activeKeys.add(r.ListingKey);
            }
            if (list.length < pageSize) {
                hasMore = false;
            } else {
                offset += pageSize;
            }
        }

        // 3. Query Bridge OpenHouse (60-day horizon)
        const today = new Date().toISOString().slice(0, 10);
        const maxFutureDate = new Date(Date.now() + 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
        const ohFilter = `OriginatingSystemKey eq 'bsaor' and OpenHouseDate ge ${today} and OpenHouseDate le ${maxFutureDate}`;

        const countData = await fetchBridgeOData(BRIDGE_OPENHOUSE_ENDPOINT, {
            '$top': 1,
            '$filter': ohFilter,
            '$count': 'true'
        }, env);
        const expectedCount = countData['@odata.count'] || 0;

        let currentUrl = new URL(BRIDGE_OPENHOUSE_ENDPOINT);
        currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
        currentUrl.searchParams.set('$filter', ohFilter);
        currentUrl.searchParams.set('$orderby', 'OpenHouseDate asc');

        let nextUrlStr = currentUrl.toString();
        const seenAllOHKeys = new Set();
        const validOHKeys = new Set();
        const validOpenHouses = [];
        let duplicateCount = 0;
        let missingOpenHouseKey = 0;
        let missingListingKey = 0;

        while (nextUrlStr) {
            bridgePages++;
            const u = new URL(nextUrlStr);
            const params = {};
            for (const [k, v] of u.searchParams.entries()) {
                if (k !== 'access_token') params[k] = v;
            }

            const data = await fetchBridgeOData(u.origin + u.pathname, params, env);
            const records = data.value || [];
            recordsFetched += records.length;

            for (const r of records) {
                if (!r.OpenHouseKey) { missingOpenHouseKey++; continue; }
                if (!r.ListingKey) { missingListingKey++; continue; }

                if (seenAllOHKeys.has(r.OpenHouseKey)) {
                    duplicateCount++;
                } else {
                    seenAllOHKeys.add(r.OpenHouseKey);
                }

                if (r.OpenHouseStatus === 'Canceled' || r.OpenHouseStatus === 'Cancelled') {
                    cancelledSkipped++;
                    continue;
                }

                if (activeKeys.has(r.ListingKey)) {
                    validEvents++;
                    validOHKeys.add(r.OpenHouseKey);
                    validOpenHouses.push(transformOpenHouseRecord(r));
                } else {
                    offMarketSkipped++;
                }
            }

            nextUrlStr = data['@odata.nextLink'] || null;
        }

        // 4. Strict Completeness Guards
        if (recordsFetched !== expectedCount) {
            throw new Error(`OpenHouse count shortfall: fetched ${recordsFetched} != expected ${expectedCount}`);
        }
        if (seenAllOHKeys.size !== expectedCount) {
            throw new Error(`OpenHouse unique key mismatch: unique ${seenAllOHKeys.size} != expected ${expectedCount}`);
        }
        if (duplicateCount > 0) {
            throw new Error(`OpenHouse duplicate anomaly: ${duplicateCount} duplicate OpenHouseKeys`);
        }
        if (missingOpenHouseKey > 0 || missingListingKey > 0) {
            throw new Error(`OpenHouse missing keys: OHKeys=${missingOpenHouseKey}, ListingKeys=${missingListingKey}`);
        }

        // 5. Batch Upsert Valid Events to D1
        const upsertSql = `
            INSERT OR REPLACE INTO sneak_open_houses (
                id, OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'));
        `;

        const upsertStatements = validOpenHouses.map(oh => {
            return env.DB.prepare(upsertSql).bind(
                oh.id, oh.OpenHouseKey, oh.ListingKey, oh.OpenHouseStartTime, oh.OpenHouseEndTime, oh.OpenHouseDate, oh.OpenHouseRemarks
            );
        });

        for (let i = 0; i < upsertStatements.length; i += BATCH_CHUNK_SIZE) {
            const chunk = upsertStatements.slice(i, i + BATCH_CHUNK_SIZE);
            await env.DB.batch(chunk);
            d1Operations += chunk.length;
        }

        // 6. Set-Difference Reconciliation & Past Date Pruning
        const existingOHRows = await env.DB.prepare("SELECT OpenHouseKey FROM sneak_open_houses").all();
        const existingOHList = existingOHRows.results || [];
        const staleKeys = [];
        for (const row of existingOHList) {
            if (row.OpenHouseKey && !validOHKeys.has(row.OpenHouseKey)) {
                staleKeys.push(row.OpenHouseKey);
            }
        }

        if (staleKeys.length > 0) {
            const deleteSql = "DELETE FROM sneak_open_houses WHERE OpenHouseKey = ?";
            const deleteStatements = staleKeys.map(k => env.DB.prepare(deleteSql).bind(k));
            for (let i = 0; i < deleteStatements.length; i += BATCH_CHUNK_SIZE) {
                const chunk = deleteStatements.slice(i, i + BATCH_CHUNK_SIZE);
                await env.DB.batch(chunk);
                d1Operations += chunk.length;
            }
            stalePruned = staleKeys.length;
        }

        // Delete past events
        await env.DB.prepare("DELETE FROM sneak_open_houses WHERE OpenHouseDate < date('now')").run();
        d1Operations++;

        // 7. Update Sync State
        const finalCountRow = await env.DB.prepare("SELECT count(*) as count FROM sneak_open_houses").first();
        const finalOHCount = finalCountRow?.count || 0;

        await env.DB.prepare(`
            INSERT OR REPLACE INTO sneak_sync_state (
                sync_name, last_successful_sync, last_full_reconciliation, last_record_count, status, updated_at
            ) VALUES ('open_houses', datetime('now'), datetime('now'), ?, 'success', datetime('now'))
        `).bind(finalOHCount).run();
        d1Operations++;

        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;

        await recordSyncRun(env.DB, {
            jobName,
            startedAt,
            finishedAt,
            status: 'success',
            recordsFetched,
            recordsUpserted: validEvents,
            recordsRemoved: stalePruned,
            bridgePages,
            d1Operations,
            durationSeconds
        });

        await releaseLock(env.DB, jobName, lockId);

        const summary = {
            job: jobName,
            status: 'success',
            startedAt,
            finishedAt,
            durationSeconds,
            expectedCount,
            recordsFetched,
            uniqueOHKeys: seenAllOHKeys.size,
            validEvents,
            offMarketSkipped,
            cancelledSkipped,
            stalePruned,
            finalOHCount,
            bridgePages,
            d1Operations
        };
        console.log(JSON.stringify(summary));
        return summary;

    } catch (err) {
        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;
        console.error(JSON.stringify({
            job: jobName,
            status: 'failure',
            startedAt,
            finishedAt,
            durationSeconds,
            error: err.message
        }));

        await recordSyncRun(env.DB, {
            jobName,
            startedAt,
            finishedAt,
            status: 'failure',
            recordsFetched,
            recordsUpserted: 0,
            recordsRemoved: 0,
            bridgePages,
            d1Operations,
            durationSeconds,
            errorCode: 'OPEN_HOUSE_SYNC_ERROR',
            errorSummary: err.message
        });

        await releaseLock(env.DB, jobName, lockId);
        throw err;
    }
}
