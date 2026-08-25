#!/usr/bin/env node

/**
 * scripts/sync-bridge-staging.mjs
 * 
 * Operational Incremental Listing Delta Synchronizer for SNEAK IDX Staging.
 * 
 * MECHANISM:
 * 1. Captures fixed UTC timestamp at start (syncUpperBound).
 * 2. Queries previous sync cursor from sneak_sync_state, setting lower bound = cursor - 5 minutes (overlap window).
 * 3. Queries Bridge with:
 *      OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL'
 *      and ModificationTimestamp ge LOWER_BOUND and ModificationTimestamp lt UPPER_BOUND
 *    (All statuses fetched so off-market status transitions can be processed).
 * 4. For each record:
 *      - Active / Active Under Contract / Pending -> Upsert into sneak_listings
 *      - Closed / Cancelled / Expired / Withdrawn / Other -> Delete from sneak_listings (serving inventory eviction)
 * 5. Deterministic cursor-based @odata.nextLink pagination.
 * 6. Updates sneak_sync_state only after complete success.
 * 
 * ZERO-SECRET PRINCIPLE:
 * - Reads BRIDGE_TOKEN strictly from environment or .dev.vars (local only).
 * - Never prints or logs token.
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
    BRIDGE_PROPERTY_ENDPOINT,
    SELECT_PARAM,
    DETERMINISTIC_ORDERBY,
    MAX_PAGE_SIZE
} from './bridge-config.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const TARGET_DB_NAME = 'sneak-idx-staging';
const TARGET_DB_ID = '6b91eeca-d65f-434c-a49f-419dff98285f';
const WRANGLER_CONFIG = 'wrangler.sneak.toml';

function verifyDatabaseTarget() {
    const configPath = path.join(rootDir, WRANGLER_CONFIG);
    if (!fs.existsSync(configPath)) throw new Error(`Configuration file ${WRANGLER_CONFIG} not found.`);
    const content = fs.readFileSync(configPath, 'utf8');
    if (!content.includes(TARGET_DB_ID) || !content.includes(TARGET_DB_NAME)) {
        throw new Error(`SAFETY REJECTION: ${WRANGLER_CONFIG} does not match ${TARGET_DB_NAME} (${TARGET_DB_ID}).`);
    }
    if (content.includes('community-idx')) {
        throw new Error('FATAL SAFETY VIOLATION: community-idx detected in SNEAK configuration.');
    }
}

// Resolve BRIDGE_TOKEN
let bridgeToken = process.env.BRIDGE_TOKEN;
if (bridgeToken === undefined) {
    const devVarsPath = path.join(rootDir, '.dev.vars');
    if (fs.existsSync(devVarsPath)) {
        try {
            const content = fs.readFileSync(devVarsPath, 'utf8');
            for (const line of content.split('\n')) {
                const trimmed = line.trim();
                if (trimmed.startsWith('BRIDGE_TOKEN=')) {
                    bridgeToken = trimmed.substring('BRIDGE_TOKEN='.length).trim().replace(/^["']|["']$/g, '');
                    break;
                }
            }
        } catch {}
    }
}

const isExecute = process.argv.includes('--execute');
const isDryRun = !isExecute || process.argv.includes('--dry-run');

function extractPrimaryPhoto(media) {
    if (!Array.isArray(media) || media.length === 0) return null;
    const sorted = [...media].sort((a, b) => (a.Order ?? 999) - (b.Order ?? 999));
    const first = sorted.find(m => m && m.MediaURL);
    return first ? first.MediaURL : null;
}

function escapeSql(val) {
    if (val === null || val === undefined) return 'NULL';
    if (typeof val === 'number') return isNaN(val) ? 'NULL' : String(val);
    if (typeof val === 'boolean') return val ? '1' : '0';
    return "'" + String(val).replace(/'/g, "''") + "'";
}

function recordToUpsertSql(r) {
    const primaryPhoto = extractPrimaryPhoto(r.Media);
    let lat = r.Latitude ?? null;
    let lon = r.Longitude ?? null;
    if (lat == null && lon == null && Array.isArray(r.Coordinates) && r.Coordinates.length >= 2) {
        lon = r.Coordinates[0];
        lat = r.Coordinates[1];
    }

    const media = r.Media || [];
    const sorted = [...media].sort((a, b) => (a.Order ?? 999) - (b.Order ?? 999));
    const urls = sorted.map(m => m && m.MediaURL).filter(Boolean);
    const uniqueUrls = Array.from(new Set(urls));
    const mediaJson = uniqueUrls.length > 0 ? JSON.stringify(uniqueUrls) : null;

    const values = [
        escapeSql(r.ListingKey),
        escapeSql(r.ListingId ?? r.ListingKey),
        escapeSql(r.ListPrice ?? null),
        escapeSql(r.OriginalListPrice ?? null),
        escapeSql(r.UnparsedAddress ?? null),
        escapeSql(r.StreetNumber ?? null),
        escapeSql(r.StreetName ?? null),
        escapeSql(r.UnitNumber ?? null),
        escapeSql(r.City ?? null),
        escapeSql(r.StateOrProvince ?? null),
        escapeSql(r.PostalCode ?? null),
        escapeSql(r.CountyOrParish ?? null),
        escapeSql(r.BedroomsTotal ?? null),
        escapeSql(r.BathroomsTotalInteger ?? null),
        escapeSql(r.BathroomsFull ?? null),
        escapeSql(r.BathroomsHalf ?? null),
        escapeSql(r.LivingArea ?? null),
        escapeSql(r.StandardStatus ?? null),
        escapeSql(r.PropertyType ?? null),
        escapeSql(r.PropertySubType ?? null),
        escapeSql(primaryPhoto),
        escapeSql(mediaJson),
        escapeSql(r.ListingContractDate ?? null),
        escapeSql(r.ModificationTimestamp ?? null),
        escapeSql(r.StatusChangeTimestamp ?? null),
        escapeSql(lat),
        escapeSql(lon),
        escapeSql(r.YearBuilt ?? null),
        escapeSql(r.LotSizeAcres ?? null),
        escapeSql(r.SubdivisionName ?? null),
        escapeSql(r.PublicRemarks ?? null),
        escapeSql(r.ListAgentKey ?? null),
        escapeSql(r.ListAgentFullName ?? null),
        escapeSql(r.ListAgentEmail ?? null),
        escapeSql(r.ListAgentDirectPhone ?? null),
        escapeSql(r.ListAgentMlsId ?? null),
        escapeSql(r.ListOfficeKey ?? null),
        escapeSql(r.ListOfficeName ?? null),
        escapeSql(r.ListOfficePhone ?? null),
        escapeSql(r.ListOfficeMlsId ?? null),
        (r.InternetEntireListingDisplayYN === true || r.InternetEntireListingDisplayYN === 1) ? 1 : 0,
        (r.InternetAddressDisplayYN === true || r.InternetAddressDisplayYN === 1) ? 1 : 0,
        escapeSql(r.OriginatingSystemKey ?? 'bsaor'),
        escapeSql(r.OriginatingSystemName ?? 'Bonita Springs')
    ];

    return `INSERT OR REPLACE INTO sneak_listings (
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
        OriginatingSystemKey, OriginatingSystemName
    ) VALUES (${values.join(', ')});`;
}

function recordToDeleteSql(r) {
    return `DELETE FROM sneak_listings WHERE ListingKey = ${escapeSql(r.ListingKey)};`;
}

async function getSyncState() {
    const raw = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="SELECT * FROM sneak_sync_state WHERE sync_name = 'listings';" --json -c ${WRANGLER_CONFIG}`, {
        cwd: rootDir,
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'ignore']
    });
    try {
        const parsed = JSON.parse(raw);
        const results = parsed[0]?.results || [];
        return results[0] || null;
    } catch {
        return null;
    }
}

async function runSync() {
    verifyDatabaseTarget();

    console.log('====================================================');
    console.log('SNEAK IDX — OPERATIONAL LISTING DELTA SYNC');
    console.log('====================================================');
    console.log(`Target Database: ${TARGET_DB_NAME} (${TARGET_DB_ID})`);
    console.log(`Execution Mode:  ${isExecute ? 'EXECUTE (WRITING TO REMOTE D1)' : 'DRY RUN (READ ONLY)'}`);
    console.log('====================================================');

    if (!bridgeToken) {
        console.log('\nBRIDGE TOKEN REQUIRED LOCALLY');
        console.log('Notice: BRIDGE_TOKEN is not set in environment or .dev.vars.');
        process.exit(0);
    }

    // 1. Determine Fixed Sync Window
    let syncUpperBound = null;
    const untilArg = process.argv.find(a => a.startsWith('--until='));
    if (untilArg) {
        syncUpperBound = untilArg.split('=')[1];
        if (!syncUpperBound.endsWith('Z')) syncUpperBound += 'Z';
    } else {
        syncUpperBound = new Date().toISOString();
    }

    let syncLowerBound = null;
    const sinceArg = process.argv.find(a => a.startsWith('--since='));
    if (sinceArg) {
        syncLowerBound = sinceArg.split('=')[1];
        if (!syncLowerBound.endsWith('Z')) syncLowerBound += 'Z';
    } else {
        const state = await getSyncState();
        let cursor = state?.last_cursor || state?.last_successful_sync;
        if (cursor) {
            cursor = cursor.replace(' ', 'T');
            if (!cursor.endsWith('Z')) cursor += 'Z';
            // Apply 5-minute overlap window
            const cursorMs = new Date(cursor).getTime();
            syncLowerBound = new Date(cursorMs - 5 * 60 * 1000).toISOString();
        } else {
            // Default to 24 hours prior if no cursor recorded
            syncLowerBound = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        }
    }

    // Strict window validation
    const lowerMs = new Date(syncLowerBound).getTime();
    const upperMs = new Date(syncUpperBound).getTime();
    if (isNaN(lowerMs) || isNaN(upperMs)) {
        throw new Error(`Invalid sync window timestamp(s): lower=${syncLowerBound}, upper=${syncUpperBound}`);
    }
    if (lowerMs >= upperMs) {
        throw new Error(`FATAL WINDOW ERROR: syncLowerBound (${syncLowerBound}) must be strictly less than syncUpperBound (${syncUpperBound}).`);
    }

    console.log(`\n[Fixed Sync Window]`);
    console.log(`  Lower Bound (>=): ${syncLowerBound} (includes 5-min overlap)`);
    console.log(`  Upper Bound (<):  ${syncUpperBound}`);

    const deltaFilter = `OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and ModificationTimestamp ge ${syncLowerBound} and ModificationTimestamp lt ${syncUpperBound}`;
    console.log(`  Delta Query Filter: ${deltaFilter}`);

    // 2. Query Expected Delta Count
    const countUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
    countUrl.searchParams.set('$top', '1');
    countUrl.searchParams.set('$filter', deltaFilter);
    countUrl.searchParams.set('$count', 'true');
    countUrl.searchParams.set('access_token', bridgeToken);

    const countRes = await fetch(countUrl.toString(), { headers: { Accept: 'application/json' } });
    if (!countRes.ok) throw new Error(`Bridge Delta Count HTTP ${countRes.status}: ${countRes.statusText}`);
    const countData = await countRes.json();
    const expectedCount = countData['@odata.count'] || 0;
    console.log(`  Expected Changed Records in Window: ${expectedCount.toLocaleString()}`);

    if (expectedCount === 0) {
        console.log('\nNo listing modifications recorded in this sync window.');
        if (isExecute) {
            // Commit cursor
            const syncSql = `UPDATE sneak_sync_state SET last_cursor = '${syncUpperBound}', last_successful_sync = datetime('now'), status = 'success', updated_at = datetime('now') WHERE sync_name = 'listings';`;
            execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${syncSql}" -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });
            console.log(`Updated sync cursor to ${syncUpperBound}.`);
        }
        return;
    }

    // 3. Fetch & Process Delta Records
    const scratchDir = path.join(rootDir, 'scratch');
    if (isExecute && !fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });

    let currentUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
    currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
    currentUrl.searchParams.set('$filter', deltaFilter);
    currentUrl.searchParams.set('$select', SELECT_PARAM);
    currentUrl.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);
    currentUrl.searchParams.set('access_token', bridgeToken);

    let nextUrlStr = currentUrl.toString();
    let pageCount = 0;
    let totalFetched = 0;
    let eligibleUpserts = 0;
    let ineligibleRemovals = 0;
    const seenListingKeys = new Set();
    let duplicateCount = 0;

    const ELIGIBLE_STATUSES = new Set(['Active', 'Active Under Contract', 'Pending']);
    const sqlStatements = [];
    const startTime = Date.now();

    while (nextUrlStr) {
        pageCount++;
        const res = await fetch(nextUrlStr, { headers: { Accept: 'application/json' } });
        if (!res.ok) throw new Error(`Bridge API Delta Fetch Error HTTP ${res.status} on page ${pageCount}`);
        const data = await res.json();
        const records = data.value || [];
        totalFetched += records.length;

        for (const r of records) {
            if (!r.ListingKey) continue;
            if (seenListingKeys.has(r.ListingKey)) {
                duplicateCount++;
            } else {
                seenListingKeys.add(r.ListingKey);
            }

            if (ELIGIBLE_STATUSES.has(r.StandardStatus)) {
                eligibleUpserts++;
                if (isExecute) sqlStatements.push(recordToUpsertSql(r));
            } else {
                ineligibleRemovals++;
                if (isExecute) sqlStatements.push(recordToDeleteSql(r));
            }
        }

        if (data['@odata.nextLink']) {
            let nLink = data['@odata.nextLink'];
            if (!nLink.includes('access_token=')) nLink += `&access_token=${bridgeToken}`;
            nextUrlStr = nLink;
        } else {
            nextUrlStr = null;
        }
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log('\n====================================================');
    console.log(`DELTA SYNC AUDIT (${duration}s)`);
    console.log('====================================================');
    console.log(`- Expected Changes:           ${expectedCount.toLocaleString()}`);
    console.log(`- Total Records Fetched:      ${totalFetched.toLocaleString()}`);
    console.log(`- Unique ListingKeys:         ${seenListingKeys.size.toLocaleString()}`);
    console.log(`- Duplicate ListingKeys:      ${duplicateCount}`);
    console.log(`- Total Pages:                ${pageCount}`);
    console.log(`- Eligible Upserts (Active):  ${eligibleUpserts.toLocaleString()}`);
    console.log(`- Ineligible Removals (Off):  ${ineligibleRemovals.toLocaleString()}`);

    if (totalFetched !== expectedCount) {
        console.error(`FAIL: Records fetched (${totalFetched}) does not match expected count (${expectedCount})!`);
        process.exit(1);
    }

    if (isDryRun) {
        console.log('\nDRY RUN COMPLETE: Zero database writes performed.');
        console.log('To execute delta synchronization, run: node scripts/sync-bridge-staging.mjs --execute');
        return;
    }

    // 4. In Execute Mode, Write Batches to D1
    console.log(`\n[Writing ${sqlStatements.length} Delta Operations to D1 in Chunks of 200...]`);
    let written = 0;
    let batchIndex = 0;
    const chunkSize = 200;

    for (let i = 0; i < sqlStatements.length; i += chunkSize) {
        batchIndex++;
        const chunk = sqlStatements.slice(i, i + chunkSize);
        const batchFile = path.join(scratchDir, `sync-chunk-${batchIndex}.sql`);
        fs.writeFileSync(batchFile, chunk.join('\n'), 'utf8');

        try {
            execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${batchFile} -c ${WRANGLER_CONFIG}`, {
                cwd: rootDir,
                stdio: 'pipe'
            });
            written += chunk.length;
        } catch (err) {
            console.error(`FATAL D1 DELTA FAILURE on batch ${batchIndex} (operations executed: ${written})`);
            if (fs.existsSync(batchFile)) { try { fs.unlinkSync(batchFile); } catch {} }
            process.exit(1);
        }

        if (fs.existsSync(batchFile)) { try { fs.unlinkSync(batchFile); } catch {} }
    }

    // 5. Commit Updated Cursor
    const isHistoricalBounded = Boolean(untilArg);
    const countQuery = `SELECT count(*) as count FROM sneak_listings;`;
    const countOut = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${countQuery}" --json -c ${WRANGLER_CONFIG}`, {
        cwd: rootDir,
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'ignore']
    });
    let finalListingCount = 0;
    try {
        finalListingCount = JSON.parse(countOut)[0]?.results[0]?.count || 0;
    } catch {}

    if (isHistoricalBounded) {
        console.log(`\n[Historical Test Run]: Cursor in sneak_sync_state was NOT updated for bounded test interval (${syncLowerBound} -> ${syncUpperBound}).`);
    } else {
        console.log('\n[Committing Sync Checkpoint]');
        const updateStateSql = `INSERT OR REPLACE INTO sneak_sync_state (
            sync_name, last_successful_sync, last_cursor, last_record_count, status, updated_at
        ) VALUES (
            'listings', datetime('now'), '${syncUpperBound}', ${finalListingCount}, 'success', datetime('now')
        );`;
        const stateFile = path.join(scratchDir, 'sync-state.sql');
        fs.writeFileSync(stateFile, updateStateSql, 'utf8');
        execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${stateFile} -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });
        if (fs.existsSync(stateFile)) { try { fs.unlinkSync(stateFile); } catch {} }
        console.log(`New Committed Cursor: ${syncUpperBound}`);
    }

    console.log('====================================================');
    console.log(`DELTA SYNC COMPLETED SUCCESSFULLY (${duration}s)`);
    console.log(`Operations Written: ${written.toLocaleString()}`);
    console.log(`New Staging Listing Count: ${finalListingCount.toLocaleString()}`);
    console.log('====================================================');
}

runSync().catch(err => {
    console.error('Delta Sync Error:', err.message);
    process.exit(1);
});
