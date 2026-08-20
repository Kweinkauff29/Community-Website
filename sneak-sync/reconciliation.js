/**
 * sneak-sync/reconciliation.js
 * 
 * Worker-native Full Listing Inventory Reconciliation.
 */

import {
    BRIDGE_PROPERTY_ENDPOINT,
    FINAL_SNEAK_LISTING_FILTER,
    DETERMINISTIC_ORDERBY,
    MAX_PAGE_SIZE,
    fetchBridgeOData
} from './bridge.js';
import { acquireLock, releaseLock, recordSyncRun } from './lock.js';

const BATCH_CHUNK_SIZE = 50;

export async function runListingReconciliation(env) {
    const jobName = 'listing_reconciliation';
    const startedAt = new Date().toISOString();
    const startTime = Date.now();

    // 1. Acquire Distributed Concurrency Lock
    const lockId = await acquireLock(env.DB, jobName, 600);
    if (!lockId) {
        console.log(JSON.stringify({ job: jobName, status: 'skipped', reason: 'concurrency_lock_active', startedAt }));
        return { status: 'skipped', reason: 'locked' };
    }

    let recordsFetched = 0;
    let stalePruned = 0;
    let missingFound = 0;
    let bridgePages = 0;
    let d1Operations = 0;

    try {
        // 2. Query All Eligible ListingKeys from Bridge
        const countData = await fetchBridgeOData(BRIDGE_PROPERTY_ENDPOINT, {
            '$top': 1,
            '$filter': FINAL_SNEAK_LISTING_FILTER,
            '$count': 'true'
        }, env);
        const expectedCount = countData['@odata.count'] || 0;

        let currentUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
        currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
        currentUrl.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
        currentUrl.searchParams.set('$select', 'ListingKey');
        currentUrl.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);

        let nextUrlStr = currentUrl.toString();
        const bridgeKeys = new Set();
        let duplicateCount = 0;

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
                if (!r.ListingKey) continue;
                if (bridgeKeys.has(r.ListingKey)) {
                    duplicateCount++;
                } else {
                    bridgeKeys.add(r.ListingKey);
                }
            }

            nextUrlStr = data['@odata.nextLink'] || null;
        }

        // 3. Strict Completeness Guards
        if (recordsFetched !== expectedCount) {
            throw new Error(`Reconciliation shortfall: fetched ${recordsFetched} != expected ${expectedCount}`);
        }
        if (bridgeKeys.size !== expectedCount) {
            throw new Error(`Reconciliation unique key mismatch: unique ${bridgeKeys.size} != expected ${expectedCount}`);
        }
        if (duplicateCount > 0) {
            throw new Error(`Reconciliation duplicate anomaly: ${duplicateCount} duplicate ListingKeys`);
        }

        // 4. Query All Serving ListingKeys from D1
        const d1Keys = new Set();
        let offset = 0;
        const pageSize = 10000;
        let hasMore = true;

        while (hasMore) {
            const rows = await env.DB.prepare(`SELECT ListingKey FROM sneak_listings LIMIT ? OFFSET ?`).bind(pageSize, offset).all();
            const list = rows.results || [];
            for (const r of list) {
                if (r.ListingKey) d1Keys.add(r.ListingKey);
            }
            if (list.length < pageSize) {
                hasMore = false;
            } else {
                offset += pageSize;
            }
        }

        // 5. Compute Inventory Diff
        const staleKeys = [];
        const missingKeys = [];

        for (const key of d1Keys) {
            if (!bridgeKeys.has(key)) staleKeys.push(key);
        }
        for (const key of bridgeKeys) {
            if (!d1Keys.has(key)) missingKeys.push(key);
        }

        missingFound = missingKeys.length;

        // 6. Prune Stale Rows
        if (staleKeys.length > 0) {
            const deleteSql = "DELETE FROM sneak_listings WHERE ListingKey = ?";
            const deleteStatements = staleKeys.map(k => env.DB.prepare(deleteSql).bind(k));
            for (let i = 0; i < deleteStatements.length; i += BATCH_CHUNK_SIZE) {
                const chunk = deleteStatements.slice(i, i + BATCH_CHUNK_SIZE);
                await env.DB.batch(chunk);
                d1Operations += chunk.length;
            }
            stalePruned = staleKeys.length;
        }

        // 7. Update Reconciliation Checkpoint
        const countRow = await env.DB.prepare("SELECT count(*) as count FROM sneak_listings").first();
        const postReconcileCount = countRow?.count || 0;

        await env.DB.prepare(`
            UPDATE sneak_sync_state
            SET last_full_reconciliation = datetime('now'), last_record_count = ?, updated_at = datetime('now')
            WHERE sync_name = 'listings'
        `).bind(postReconcileCount).run();
        d1Operations++;

        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;

        await recordSyncRun(env.DB, {
            jobName,
            startedAt,
            finishedAt,
            status: 'success',
            recordsFetched,
            recordsUpserted: 0,
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
            bridgeEligibleKeys: bridgeKeys.size,
            d1ServingKeys: d1Keys.size,
            staleKeysPruned: stalePruned,
            missingKeysFound: missingFound,
            postReconcileCount,
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
            errorCode: 'RECONCILIATION_ERROR',
            errorSummary: err.message
        });

        await releaseLock(env.DB, jobName, lockId);
        throw err;
    }
}
