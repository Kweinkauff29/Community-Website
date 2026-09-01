/**
 * sneak-sync/listing-sync.js
 * 
 * Worker-native Operational Listing Synchronizer: Full Inventory Bootstrap + 15-Min Delta.
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
import { acquireLock, releaseLock, recordSyncRun } from './lock.js';

const ELIGIBLE_STATUSES = new Set(['Active', 'Active Under Contract', 'Pending']);
const BATCH_CHUNK_SIZE = 50;

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

/**
 * Primary listing synchronization dispatcher.
 * Automatically performs full inventory bootstrap if no valid cursor exists;
 * otherwise performs an incremental delta sync from stored cursor with 5-min overlap.
 */
export async function runListingDelta(env) {
    const jobName = 'listing_delta';
    const startedAt = new Date().toISOString();
    const startTime = Date.now();

    // 1. Acquire Distributed Concurrency Lock (600s to allow full bootstrap)
    const lockId = await acquireLock(env.DB, jobName, 600);
    if (!lockId) {
        console.log(JSON.stringify({ job: jobName, status: 'skipped', reason: 'concurrency_lock_active', startedAt }));
        return { status: 'skipped', reason: 'locked' };
    }

    try {
        const stateRes = await env.DB.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").first();
        const cursor = stateRes?.last_cursor || stateRes?.last_successful_sync;
        const hasCompletedBootstrap = Boolean(cursor && stateRes?.status === 'success');

        if (!hasCompletedBootstrap) {
            return await runFullInventoryBootstrap(env, lockId, startedAt, startTime);
        } else {
            return await runListingDeltaFromCursor(env, lockId, startedAt, startTime, stateRes);
        }
    } catch (err) {
        throw err;
    }
}

/**
 * Authoritative Full Current Inventory Bootstrap.
 * Fetches the entire eligible inventory bounded by syncUpperBound, validates completeness,
 * hydrates all fields including MediaJSON, prunes any stale D1 rows, and commits cursor.
 */
export async function runFullInventoryBootstrap(env, existingLockId = null, initialStartedAt = null, initialStartTime = null) {
    const jobName = 'listing_delta';
    const startedAt = initialStartedAt || new Date().toISOString();
    const startTime = initialStartTime || Date.now();

    let lockId = existingLockId;
    if (!lockId) {
        lockId = await acquireLock(env.DB, jobName, 600);
        if (!lockId) {
            console.log(JSON.stringify({ job: jobName, mode: 'bootstrap', status: 'skipped', reason: 'concurrency_lock_active', startedAt }));
            return { status: 'skipped', reason: 'locked' };
        }
    }

    let recordsFetched = 0;
    let recordsUpserted = 0;
    let recordsRemoved = 0;
    let bridgePages = 0;
    let d1Operations = 0;

    try {
        const syncUpperBound = new Date().toISOString();
        const bootstrapFilter = `${FINAL_SNEAK_LISTING_FILTER} and ModificationTimestamp lt ${syncUpperBound}`;

        // 1. Expected Count
        const countData = await fetchBridgeOData(BRIDGE_PROPERTY_ENDPOINT, {
            '$top': 1,
            '$filter': bootstrapFilter,
            '$count': 'true'
        }, env);

        const expectedCount = countData['@odata.count'] || 0;

        // Fail-safe for empty inventory (Section 13)
        if (expectedCount === 0 && !env?.ALLOW_EMPTY_BOOTSTRAP) {
            throw new Error('EmptyBootstrapInventory: Bridge returned 0 eligible listings for initial bootstrap');
        }

        // 2. Fetch All Eligible Records
        let currentUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
        currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
        currentUrl.searchParams.set('$filter', bootstrapFilter);
        currentUrl.searchParams.set('$select', SELECT_PARAM);
        currentUrl.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);

        let nextUrlStr = currentUrl.toString();
        const seenListingKeys = new Set();
        let duplicateCount = 0;
        const upsertStatements = [];

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
                if (seenListingKeys.has(r.ListingKey)) {
                    duplicateCount++;
                } else {
                    seenListingKeys.add(r.ListingKey);
                }

                if (ELIGIBLE_STATUSES.has(r.StandardStatus)) {
                    recordsUpserted++;
                    const row = transformListingRecord(r);
                    upsertStatements.push(createUpsertStatement(env.DB, row));
                }
            }

            nextUrlStr = data['@odata.nextLink'] || null;
        }

        // 3. Strict Completeness Guards (Section 8)
        if (duplicateCount > 0) {
            throw new Error(`Bootstrap duplicate anomaly: encountered ${duplicateCount} duplicate ListingKeys`);
        }
        if (recordsFetched !== expectedCount) {
            throw new Error(`Bootstrap completeness shortfall: fetched ${recordsFetched} != expected ${expectedCount}`);
        }
        if (seenListingKeys.size !== expectedCount) {
            throw new Error(`Bootstrap unique key mismatch: unique ${seenListingKeys.size} != expected ${expectedCount}`);
        }

        // 4. Batch Upsert D1 Statements
        for (let i = 0; i < upsertStatements.length; i += BATCH_CHUNK_SIZE) {
            const chunk = upsertStatements.slice(i, i + BATCH_CHUNK_SIZE);
            await env.DB.batch(chunk);
            d1Operations += chunk.length;
        }

        // 5. Prune Preexisting Stale D1 Rows Not In This Bootstrap Set (Section 11)
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

        const staleKeys = [];
        for (const k of d1Keys) {
            if (!seenListingKeys.has(k)) staleKeys.push(k);
        }

        if (staleKeys.length > 0) {
            const deleteStatements = staleKeys.map(k => env.DB.prepare(deleteSql).bind(k));
            for (let i = 0; i < deleteStatements.length; i += BATCH_CHUNK_SIZE) {
                const chunk = deleteStatements.slice(i, i + BATCH_CHUNK_SIZE);
                await env.DB.batch(chunk);
                d1Operations += chunk.length;
            }
            recordsRemoved = staleKeys.length;
        }

        // 6. Verify Exact Final State
        const countRow = await env.DB.prepare("SELECT count(*) as count FROM sneak_listings").first();
        const finalCount = countRow?.count || 0;

        if (finalCount !== seenListingKeys.size) {
            throw new Error(`Bootstrap final inventory mismatch: final D1 count ${finalCount} != expected ${seenListingKeys.size}`);
        }

        // 7. Commit Cursor (Section 12)
        await env.DB.prepare(`
            INSERT OR REPLACE INTO sneak_sync_state (
                sync_name, last_successful_sync, last_cursor, last_record_count, status, updated_at
            ) VALUES ('listings', datetime('now'), ?, ?, 'success', datetime('now'))
        `).bind(syncUpperBound, finalCount).run();
        d1Operations++;

        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;

        await recordSyncRun(env.DB, {
            jobName,
            startedAt,
            finishedAt,
            status: 'success',
            recordsFetched,
            recordsUpserted,
            recordsRemoved,
            bridgePages,
            d1Operations,
            durationSeconds
        });

        await releaseLock(env.DB, jobName, lockId);

        const summary = {
            job: jobName,
            mode: 'bootstrap',
            status: 'success',
            startedAt,
            finishedAt,
            durationSeconds,
            syncUpperBound,
            expectedCount,
            recordsFetched,
            recordsUpserted,
            recordsRemoved,
            bridgePages,
            d1Operations,
            finalListingCount: finalCount
        };
        console.log(JSON.stringify(summary));
        return summary;

    } catch (err) {
        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;
        console.error(JSON.stringify({
            job: jobName,
            mode: 'bootstrap',
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
            recordsUpserted,
            recordsRemoved,
            bridgePages,
            d1Operations,
            durationSeconds,
            errorCode: 'BOOTSTRAP_SYNC_ERROR',
            errorSummary: err.message
        });

        await releaseLock(env.DB, jobName, lockId);
        throw err;
    }
}

/**
 * 15-Minute Incremental Delta Sync from Stored Cursor with 5-Minute Overlap.
 */
export async function runListingDeltaFromCursor(env, lockId, startedAt, startTime, stateRes) {
    const jobName = 'listing_delta';
    let recordsFetched = 0;
    let recordsUpserted = 0;
    let recordsRemoved = 0;
    let bridgePages = 0;
    let d1Operations = 0;

    try {
        const syncUpperBound = new Date().toISOString();
        let cursor = stateRes?.last_cursor || stateRes?.last_successful_sync;
        cursor = cursor.replace(' ', 'T');
        if (!cursor.endsWith('Z')) cursor += 'Z';
        const cursorMs = new Date(cursor).getTime();
        const syncLowerBound = new Date(cursorMs - 5 * 60 * 1000).toISOString();

        const deltaFilter = `OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and ModificationTimestamp ge ${syncLowerBound} and ModificationTimestamp lt ${syncUpperBound}`;

        // 1. Expected Changes Count
        const countData = await fetchBridgeOData(BRIDGE_PROPERTY_ENDPOINT, {
            '$top': 1,
            '$filter': deltaFilter,
            '$count': 'true'
        }, env);

        const expectedCount = countData['@odata.count'] || 0;

        if (expectedCount === 0) {
            const finishedAt = new Date().toISOString();
            await env.DB.prepare(`
                UPDATE sneak_sync_state
                SET last_cursor = ?, last_successful_sync = datetime('now'), status = 'success', updated_at = datetime('now')
                WHERE sync_name = 'listings'
            `).bind(syncUpperBound).run();

            await recordSyncRun(env.DB, {
                jobName,
                startedAt,
                finishedAt,
                status: 'success',
                recordsFetched: 0,
                recordsUpserted: 0,
                recordsRemoved: 0,
                bridgePages: 0,
                d1Operations: 1,
                durationSeconds: (Date.now() - startTime) / 1000
            });

            await releaseLock(env.DB, jobName, lockId);

            const result = {
                job: jobName,
                mode: 'delta',
                status: 'success',
                startedAt,
                finishedAt,
                syncLowerBound,
                syncUpperBound,
                expectedCount: 0,
                recordsFetched: 0,
                recordsUpserted: 0,
                recordsRemoved: 0,
                cursor: syncUpperBound
            };
            console.log(JSON.stringify(result));
            return result;
        }

        // 2. Fetch All Changed Records
        let currentUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
        currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
        currentUrl.searchParams.set('$filter', deltaFilter);
        currentUrl.searchParams.set('$select', SELECT_PARAM);
        currentUrl.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);

        let nextUrlStr = currentUrl.toString();
        const seenListingKeys = new Set();
        let duplicateCount = 0;
        const upsertStatements = [];
        const deleteStatements = [];

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
                if (seenListingKeys.has(r.ListingKey)) {
                    duplicateCount++;
                } else {
                    seenListingKeys.add(r.ListingKey);
                }

                if (ELIGIBLE_STATUSES.has(r.StandardStatus)) {
                    recordsUpserted++;
                    const row = transformListingRecord(r);
                    upsertStatements.push(createUpsertStatement(env.DB, row));
                } else {
                    recordsRemoved++;
                    deleteStatements.push(env.DB.prepare(deleteSql).bind(r.ListingKey));
                }
            }

            nextUrlStr = data['@odata.nextLink'] || null;
        }

        // 3. Strict Completeness Guards
        if (duplicateCount > 0) {
            throw new Error(`Duplicate anomaly: encountered ${duplicateCount} duplicate ListingKeys`);
        }
        if (recordsFetched !== expectedCount) {
            throw new Error(`Completeness short-fall: fetched ${recordsFetched} != expected ${expectedCount}`);
        }
        if (seenListingKeys.size !== expectedCount) {
            throw new Error(`Unique key mismatch: unique ${seenListingKeys.size} != expected ${expectedCount}`);
        }

        // 4. Execute D1 Operations in Batches
        const allStatements = [...upsertStatements, ...deleteStatements];
        for (let i = 0; i < allStatements.length; i += BATCH_CHUNK_SIZE) {
            const chunk = allStatements.slice(i, i + BATCH_CHUNK_SIZE);
            await env.DB.batch(chunk);
            d1Operations += chunk.length;
        }

        // 5. Update Sync State and Commit Cursor
        const countRow = await env.DB.prepare("SELECT count(*) as count FROM sneak_listings").first();
        const finalCount = countRow?.count || 0;

        await env.DB.prepare(`
            UPDATE sneak_sync_state
            SET last_cursor = ?, last_successful_sync = datetime('now'), last_record_count = ?, status = 'success', updated_at = datetime('now')
            WHERE sync_name = 'listings'
        `).bind(syncUpperBound, finalCount).run();
        d1Operations++;

        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;

        await recordSyncRun(env.DB, {
            jobName,
            startedAt,
            finishedAt,
            status: 'success',
            recordsFetched,
            recordsUpserted,
            recordsRemoved,
            bridgePages,
            d1Operations,
            durationSeconds
        });

        await releaseLock(env.DB, jobName, lockId);

        const summary = {
            job: jobName,
            mode: 'delta',
            status: 'success',
            startedAt,
            finishedAt,
            durationSeconds,
            syncLowerBound,
            syncUpperBound,
            expectedCount,
            recordsFetched,
            recordsUpserted,
            recordsRemoved,
            bridgePages,
            d1Operations,
            finalListingCount: finalCount
        };
        console.log(JSON.stringify(summary));
        return summary;

    } catch (err) {
        const finishedAt = new Date().toISOString();
        const durationSeconds = (Date.now() - startTime) / 1000;
        console.error(JSON.stringify({
            job: jobName,
            mode: 'delta',
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
            recordsUpserted,
            recordsRemoved,
            bridgePages,
            d1Operations,
            durationSeconds,
            errorCode: 'DELTA_SYNC_ERROR',
            errorSummary: err.message
        });

        await releaseLock(env.DB, jobName, lockId);
        throw err;
    }
}
