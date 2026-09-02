/**
 * sneak-sync/reconciliation.js
 * 
 * Worker-native Self-Healing Listing Inventory Reconciliation.
 * Hardened with:
 * - Bounded-memory streamed writes during repair
 * - Lock lease renewals during long scan/repair operations
 * - Immediate abort if lock ownership is lost (fails closed, no stale pruning, no checkpoint)
 */

import {
    BRIDGE_PROPERTY_ENDPOINT,
    FINAL_SNEAK_LISTING_FILTER,
    SELECT_PARAM,
    DETERMINISTIC_ORDERBY,
    MAX_PAGE_SIZE,
    fetchBridgeOData
} from './bridge.js';
import { transformListingRecord } from './transforms.js';
import { acquireLock, renewLock, releaseLock, recordSyncRun } from './lock.js';

const BATCH_CHUNK_SIZE = 50;
const MISSING_CHUNK_SIZE = 25;
const LARGE_MISSING_THRESHOLD = 500;

const upsertSql = `
    INSERT OR REPLACE INTO sneak_listings (
        ListingKey, ListingId, ListPrice, OriginalListPrice,
        UnparsedAddress, StreetNumber, StreetName, UnitNumber,
        City, StateOrProvince, PostalCode, CountyOrParish,
        BedroomsTotal, BathroomsTotalInteger, BathroomsFull, BathroomsHalf,
        LivingArea, StandardStatus, PropertyType, PropertySubType,
        PrimaryPhoto, MediaJSON, ListingContractDate, ModificationTimestamp, StatusChangeTimestamp,
        Latitude, Longitude, YearBuilt, LotSizeAcres, SubdivisionName, PublicRemarks,
        ListAgentKey, ListAgentFullName, ListAgentEmail, ListAgentDirectPhone, ListAgentMlsId,
        ListOfficeKey, ListOfficeName, ListOfficePhone, ListOfficeMlsId,
        InternetEntireListingDisplayYN, InternetAddressDisplayYN,
        OriginatingSystemKey, OriginatingSystemName,
        WaterfrontYN, PoolPrivateYN, GarageSpaces, NewConstructionYN, Zoning
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`;

const deleteSql = `DELETE FROM sneak_listings WHERE ListingKey = ?;`;

function createUpsertStatement(db, row) {
    return db.prepare(upsertSql).bind(
        row.ListingKey, row.ListingId, row.ListPrice, row.OriginalListPrice,
        row.UnparsedAddress, row.StreetNumber, row.StreetName, row.UnitNumber,
        row.City, row.StateOrProvince, row.PostalCode, row.CountyOrParish,
        row.BedroomsTotal, row.BathroomsTotalInteger, row.BathroomsFull, row.BathroomsHalf,
        row.LivingArea, row.StandardStatus, row.PropertyType, row.PropertySubType,
        row.PrimaryPhoto, row.MediaJSON, row.ListingContractDate, row.ModificationTimestamp, row.StatusChangeTimestamp,
        row.Latitude, row.Longitude, row.YearBuilt, row.LotSizeAcres, row.SubdivisionName, row.PublicRemarks,
        row.ListAgentKey, row.ListAgentFullName, row.ListAgentEmail, row.ListAgentDirectPhone, row.ListAgentMlsId,
        row.ListOfficeKey, row.ListOfficeName, row.ListOfficePhone, row.ListOfficeMlsId,
        row.InternetEntireListingDisplayYN, row.InternetAddressDisplayYN,
        row.OriginatingSystemKey, row.OriginatingSystemName,
        row.WaterfrontYN, row.PoolPrivateYN, row.GarageSpaces, row.NewConstructionYN, row.Zoning
    );
}

export async function runListingReconciliation(env) {
    const jobName = 'listing_reconciliation';
    const startedAt = new Date().toISOString();
    const startTime = Date.now();

    // 1. Acquire Distributed Concurrency Lock (600s with renewal)
    const lockId = await acquireLock(env.DB, jobName, 600);
    if (!lockId) {
        console.log(JSON.stringify({ job: jobName, status: 'skipped', reason: 'concurrency_lock_active', startedAt }));
        return { status: 'skipped', reason: 'locked' };
    }

    let recordsFetched = 0;
    let stalePruned = 0;
    let missingFound = 0;
    let missingRepaired = 0;
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

            // Renew lock lease during key scan
            const renewed = await renewLock(env.DB, jobName, lockId, 600);
            if (!renewed) {
                throw new Error('SyncLockLost: Lock lease expired or stolen during key scan');
            }

            nextUrlStr = data['@odata.nextLink'] || null;
        }

        // 3. Strict Completeness Guards
        if (duplicateCount > 0) {
            throw new Error(`Reconciliation duplicate anomaly: ${duplicateCount} duplicate ListingKeys`);
        }
        if (bridgeKeys.size !== recordsFetched) {
            throw new Error(`Reconciliation unique key mismatch: unique ${bridgeKeys.size} != fetched ${recordsFetched}`);
        }
        const shortfall = Math.abs(recordsFetched - expectedCount);
        const shortfallPct = expectedCount > 0 ? (shortfall / expectedCount) : 0;
        if ((expectedCount <= 50 && shortfall > 0) || (shortfallPct > 0.01)) {
            throw new Error(`Reconciliation shortfall: fetched ${recordsFetched} != expected ${expectedCount}`);
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

        // 6. Self-Healing Repair in Bounded Memory (Sections 18, 19)
        const repairedKeys = new Set();

        if (missingKeys.length > 0) {
            if (missingKeys.length <= LARGE_MISSING_THRESHOLD) {
                // Targeted chunk retrieval: fetch chunk, write immediately, discard statements
                for (let i = 0; i < missingKeys.length; i += MISSING_CHUNK_SIZE) {
                    const chunk = missingKeys.slice(i, i + MISSING_CHUNK_SIZE);
                    const keyDisjunction = chunk.map(k => `ListingKey eq '${String(k).replace(/'/g, "''")}'`).join(' or ');
                    const chunkFilter = `${FINAL_SNEAK_LISTING_FILTER} and (${keyDisjunction})`;

                    bridgePages++;
                    const data = await fetchBridgeOData(BRIDGE_PROPERTY_ENDPOINT, {
                        '$top': chunk.length,
                        '$filter': chunkFilter,
                        '$select': SELECT_PARAM
                    }, env);

                    const records = data.value || [];
                    recordsFetched += records.length;

                    const chunkStatements = [];
                    for (const r of records) {
                        if (!r.ListingKey) continue;
                        repairedKeys.add(r.ListingKey);
                        const row = transformListingRecord(r);
                        chunkStatements.push(createUpsertStatement(env.DB, row));
                    }

                    // Bounded batch write
                    for (let j = 0; j < chunkStatements.length; j += BATCH_CHUNK_SIZE) {
                        const batchChunk = chunkStatements.slice(j, j + BATCH_CHUNK_SIZE);
                        await env.DB.batch(batchChunk);
                        d1Operations += batchChunk.length;
                    }

                    // Renew lock after chunk write
                    const renewed = await renewLock(env.DB, jobName, lockId, 600);
                    if (!renewed) {
                        throw new Error('SyncLockLost: Lock lease expired or stolen during targeted missing key repair');
                    }
                }
            } else {
                // Large-missing fallback: stream repaired pages in bounded memory
                console.log(JSON.stringify({
                    job: jobName,
                    action: 'large_missing_fallback',
                    threshold: LARGE_MISSING_THRESHOLD,
                    missingCount: missingKeys.length
                }));
                const missingSet = new Set(missingKeys);
                let currentUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
                currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
                currentUrl.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
                currentUrl.searchParams.set('$select', SELECT_PARAM);
                currentUrl.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);

                let nextUrlStr = currentUrl.toString();
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

                    const pageStatements = [];
                    for (const r of records) {
                        if (r.ListingKey && missingSet.has(r.ListingKey)) {
                            repairedKeys.add(r.ListingKey);
                            const row = transformListingRecord(r);
                            pageStatements.push(createUpsertStatement(env.DB, row));
                        }
                    }

                    // Bounded batch write for this page's repaired listings
                    for (let j = 0; j < pageStatements.length; j += BATCH_CHUNK_SIZE) {
                        const batchChunk = pageStatements.slice(j, j + BATCH_CHUNK_SIZE);
                        await env.DB.batch(batchChunk);
                        d1Operations += batchChunk.length;
                    }

                    const renewed = await renewLock(env.DB, jobName, lockId, 600);
                    if (!renewed) {
                        throw new Error('SyncLockLost: Lock lease expired or stolen during large missing key repair');
                    }

                    nextUrlStr = data['@odata.nextLink'] || null;
                }
            }

            // Verify all missing keys were retrieved
            if (repairedKeys.size !== missingKeys.length) {
                const stillMissing = missingKeys.filter(k => !repairedKeys.has(k));
                throw new Error(`Reconciliation repair shortfall: repaired ${repairedKeys.size} != expected ${missingKeys.length} missing keys. Still missing: ${stillMissing.slice(0, 5).join(', ')}`);
            }

            missingRepaired = repairedKeys.size;
        }

        // 7. Verify Lock Ownership Before Stale Pruning (Section 19)
        const lockCheckPrePrune = await renewLock(env.DB, jobName, lockId, 600);
        if (!lockCheckPrePrune) {
            throw new Error('SyncLockLost: Lock lease lost before reconciliation stale pruning');
        }

        // 8. Prune Stale Rows
        if (staleKeys.length > 0) {
            const deleteStatements = staleKeys.map(k => env.DB.prepare(deleteSql).bind(k));
            for (let i = 0; i < deleteStatements.length; i += BATCH_CHUNK_SIZE) {
                const chunk = deleteStatements.slice(i, i + BATCH_CHUNK_SIZE);
                await env.DB.batch(chunk);
                d1Operations += chunk.length;
            }
            stalePruned = staleKeys.length;
        }

        // 9. Re-check Final Inventory Consistency
        const finalD1Keys = new Set();
        offset = 0;
        hasMore = true;
        while (hasMore) {
            const rows = await env.DB.prepare(`SELECT ListingKey FROM sneak_listings LIMIT ? OFFSET ?`).bind(pageSize, offset).all();
            const list = rows.results || [];
            for (const r of list) {
                if (r.ListingKey) finalD1Keys.add(r.ListingKey);
            }
            if (list.length < pageSize) {
                hasMore = false;
            } else {
                offset += pageSize;
            }
        }

        if (finalD1Keys.size !== bridgeKeys.size) {
            throw new Error(`Reconciliation inventory mismatch: final D1 count ${finalD1Keys.size} != Bridge eligible count ${bridgeKeys.size}`);
        }

        for (const k of bridgeKeys) {
            if (!finalD1Keys.has(k)) {
                throw new Error(`Reconciliation consistency check failed: key ${k} still missing in D1 after repair`);
            }
        }

        // 10. Verify Lock Ownership Before Checkpoint (Section 19)
        const lockCheckPreCheckpoint = await renewLock(env.DB, jobName, lockId, 600);
        if (!lockCheckPreCheckpoint) {
            throw new Error('SyncLockLost: Lock lease lost before reconciliation checkpoint');
        }

        // 11. Update Reconciliation Checkpoint
        await env.DB.prepare(`
            INSERT INTO sneak_sync_state (sync_name, last_full_reconciliation, last_record_count, updated_at)
            VALUES ('listings', datetime('now'), ?, datetime('now'))
            ON CONFLICT(sync_name) DO UPDATE SET
                last_full_reconciliation = excluded.last_full_reconciliation,
                last_record_count = excluded.last_record_count,
                updated_at = excluded.updated_at
        `).bind(finalD1Keys.size).run();
        d1Operations++;

        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;

        await recordSyncRun(env.DB, {
            jobName,
            startedAt,
            finishedAt,
            status: 'success',
            recordsFetched,
            recordsUpserted: missingRepaired,
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
            missingKeysFound: missingFound,
            missingKeysRepaired: missingRepaired,
            staleKeysFound: staleKeys.length,
            staleKeysPruned: stalePruned,
            finalD1Count: finalD1Keys.size,
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
            recordsUpserted: missingRepaired,
            recordsRemoved: 0,
            bridgePages,
            d1Operations,
            durationSeconds,
            errorCode: err.message.startsWith('SyncLockLost') ? 'SYNC_LOCK_LOST' : 'RECONCILIATION_ERROR',
            errorSummary: err.message
        });

        await releaseLock(env.DB, jobName, lockId);
        throw err;
    }
}
