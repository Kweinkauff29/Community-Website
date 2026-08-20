/**
 * sneak-sync/listing-sync.js
 * 
 * Worker-native Operational Listing Delta Synchronizer.
 */

import {
    BRIDGE_PROPERTY_ENDPOINT,
    SELECT_PARAM,
    DETERMINISTIC_ORDERBY,
    MAX_PAGE_SIZE,
    fetchBridgeOData
} from './bridge.js';
import { transformListingRecord } from './transforms.js';
import { acquireLock, releaseLock, recordSyncRun } from './lock.js';

const ELIGIBLE_STATUSES = new Set(['Active', 'Active Under Contract', 'Pending']);
const BATCH_CHUNK_SIZE = 50;

export async function runListingDelta(env) {
    const jobName = 'listing_delta';
    const startedAt = new Date().toISOString();
    const startTime = Date.now();

    // 1. Acquire Distributed Concurrency Lock
    const lockId = await acquireLock(env.DB, jobName, 300);
    if (!lockId) {
        console.log(JSON.stringify({ job: jobName, status: 'skipped', reason: 'concurrency_lock_active', startedAt }));
        return { status: 'skipped', reason: 'locked' };
    }

    let recordsFetched = 0;
    let recordsUpserted = 0;
    let recordsRemoved = 0;
    let bridgePages = 0;
    let d1Operations = 0;

    try {
        // 2. Fixed Delta Window
        const syncUpperBound = new Date().toISOString();
        let syncLowerBound = null;

        const stateRes = await env.DB.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").first();
        let cursor = stateRes?.last_cursor || stateRes?.last_successful_sync;

        if (cursor) {
            cursor = cursor.replace(' ', 'T');
            if (!cursor.endsWith('Z')) cursor += 'Z';
            const cursorMs = new Date(cursor).getTime();
            syncLowerBound = new Date(cursorMs - 5 * 60 * 1000).toISOString();
        } else {
            syncLowerBound = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        }

        const deltaFilter = `OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and ModificationTimestamp ge ${syncLowerBound} and ModificationTimestamp lt ${syncUpperBound}`;

        // 3. Expected Changes Count
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
                status: 'success',
                startedAt,
                finishedAt,
                expectedCount: 0,
                recordsFetched: 0,
                recordsUpserted: 0,
                recordsRemoved: 0,
                cursor: syncUpperBound
            };
            console.log(JSON.stringify(result));
            return result;
        }

        // 4. Fetch All Changed Records
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

        const upsertSql = `
            INSERT OR REPLACE INTO sneak_listings (
                ListingKey, ListingId, ListPrice, OriginalListPrice,
                UnparsedAddress, StreetNumber, StreetName, UnitNumber,
                City, StateOrProvince, PostalCode, CountyOrParish,
                BedroomsTotal, BathroomsTotalInteger, BathroomsFull, BathroomsHalf,
                LivingArea, StandardStatus, PropertyType, PropertySubType,
                PrimaryPhoto, ListingContractDate, ModificationTimestamp, StatusChangeTimestamp,
                Latitude, Longitude, YearBuilt, LotSizeAcres, SubdivisionName, PublicRemarks,
                ListAgentKey, ListAgentFullName, ListAgentEmail, ListAgentDirectPhone, ListAgentMlsId,
                ListOfficeKey, ListOfficeName, ListOfficePhone, ListOfficeMlsId,
                OriginatingSystemKey, OriginatingSystemName
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        `;

        const deleteSql = `DELETE FROM sneak_listings WHERE ListingKey = ?;`;

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
                    upsertStatements.push(env.DB.prepare(upsertSql).bind(
                        row.ListingKey, row.ListingId, row.ListPrice, row.OriginalListPrice,
                        row.UnparsedAddress, row.StreetNumber, row.StreetName, row.UnitNumber,
                        row.City, row.StateOrProvince, row.PostalCode, row.CountyOrParish,
                        row.BedroomsTotal, row.BathroomsTotalInteger, row.BathroomsFull, row.BathroomsHalf,
                        row.LivingArea, row.StandardStatus, row.PropertyType, row.PropertySubType,
                        row.PrimaryPhoto, row.ListingContractDate, row.ModificationTimestamp, row.StatusChangeTimestamp,
                        row.Latitude, row.Longitude, row.YearBuilt, row.LotSizeAcres, row.SubdivisionName, row.PublicRemarks,
                        row.ListAgentKey, row.ListAgentFullName, row.ListAgentEmail, row.ListAgentDirectPhone, row.ListAgentMlsId,
                        row.ListOfficeKey, row.ListOfficeName, row.ListOfficePhone, row.ListOfficeMlsId,
                        row.OriginatingSystemKey, row.OriginatingSystemName
                    ));
                } else {
                    recordsRemoved++;
                    deleteStatements.push(env.DB.prepare(deleteSql).bind(r.ListingKey));
                }
            }

            nextUrlStr = data['@odata.nextLink'] || null;
        }

        // 5. Strict Completeness Guards
        if (recordsFetched !== expectedCount) {
            throw new Error(`Completeness short-fall: fetched ${recordsFetched} != expected ${expectedCount}`);
        }
        if (seenListingKeys.size !== expectedCount) {
            throw new Error(`Unique key mismatch: unique ${seenListingKeys.size} != expected ${expectedCount}`);
        }
        if (duplicateCount > 0) {
            throw new Error(`Duplicate anomaly: encountered ${duplicateCount} duplicate ListingKeys`);
        }

        // 6. Execute D1 Operations in Batches
        const allStatements = [...upsertStatements, ...deleteStatements];
        for (let i = 0; i < allStatements.length; i += BATCH_CHUNK_SIZE) {
            const chunk = allStatements.slice(i, i + BATCH_CHUNK_SIZE);
            await env.DB.batch(chunk);
            d1Operations += chunk.length;
        }

        // 7. Update Sync State and Commit Cursor
        const countRow = await env.DB.prepare("SELECT count(*) as count FROM sneak_listings").first();
        const finalCount = countRow?.count || 0;

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
