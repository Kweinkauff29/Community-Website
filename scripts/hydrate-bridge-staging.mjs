#!/usr/bin/env node

/**
 * scripts/hydrate-bridge-staging.mjs
 * 
 * Controlled local importer to hydrate sneak-idx-staging D1 with real authorized Bridge MLS listings.
 * 
 * SAFETY CONSTRAINTS:
 * - Strictly targets sneak-idx-staging (database ID: 6b91eeca-d65f-434c-a49f-419dff98285f).
 * - Refuses execution against community-idx or any legacy/production database.
 * - Defaults to --dry-run. Explicit --execute flag is required to perform writes.
 * - Uses shared locked filter from scripts/bridge-config.mjs.
 * - Streams and chunks data efficiently (no giant in-memory arrays).
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
    BRIDGE_BASE_URL,
    BRIDGE_PROPERTY_ENDPOINT,
    FINAL_SNEAK_LISTING_FILTER,
    SELECT_PARAM,
    DETERMINISTIC_ORDERBY,
    MAX_PAGE_SIZE
} from './bridge-config.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

// 1. Target Database Safety Guardrails
const TARGET_DB_NAME = 'sneak-idx-staging';
const TARGET_DB_ID = '6b91eeca-d65f-434c-a49f-419dff98285f';
const WRANGLER_CONFIG = 'wrangler.sneak.toml';

function verifyDatabaseTarget() {
    const configPath = path.join(rootDir, WRANGLER_CONFIG);
    if (!fs.existsSync(configPath)) {
        throw new Error(`Configuration file ${WRANGLER_CONFIG} not found.`);
    }
    const configContent = fs.readFileSync(configPath, 'utf8');
    if (!configContent.includes(TARGET_DB_ID) || !configContent.includes(TARGET_DB_NAME)) {
        throw new Error(`SAFETY REJECTION: ${WRANGLER_CONFIG} does not match expected staging database ${TARGET_DB_NAME} (${TARGET_DB_ID}).`);
    }
    if (configContent.includes('community-idx')) {
        throw new Error('FATAL SAFETY VIOLATION: community-idx detected in SNEAK configuration. Execution aborted.');
    }
}

// 2. Resolve BRIDGE_TOKEN
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
    const sorted = [...media].sort((a, b) => {
        const orderA = a.Order ?? 999;
        const orderB = b.Order ?? 999;
        return orderA - orderB;
    });
    const first = sorted.find(m => m && m.MediaURL);
    return first ? first.MediaURL : null;
}

function escapeSql(val) {
    if (val === null || val === undefined) return 'NULL';
    if (typeof val === 'number') return isNaN(val) ? 'NULL' : String(val);
    if (typeof val === 'boolean') return val ? '1' : '0';
    return "'" + String(val).replace(/'/g, "''") + "'";
}

function recordToSql(r) {
    const primaryPhoto = extractPrimaryPhoto(r.Media);
    
    // Map Coordinates GeoJSON array [lon, lat] -> Latitude = Coordinates[1], Longitude = Coordinates[0]
    let lat = r.Latitude ?? null;
    let lon = r.Longitude ?? null;
    if (lat == null && lon == null && Array.isArray(r.Coordinates) && r.Coordinates.length >= 2) {
        lon = r.Coordinates[0];
        lat = r.Coordinates[1];
    }

    // Use value ?? null to protect legitimate numeric zeros (e.g. BathroomsHalf = 0, LotSizeAcres = 0)
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
        r.InternetEntireListingDisplayYN === false ? 0 : 1,
        r.InternetAddressDisplayYN === false ? 0 : 1,
        escapeSql(r.OriginatingSystemKey ?? 'bsaor'),
        escapeSql(r.OriginatingSystemName ?? 'Bonita Springs')
    ];

    return `INSERT OR REPLACE INTO sneak_listings (
        ListingKey, ListingId, ListPrice, OriginalListPrice,
        UnparsedAddress, StreetNumber, StreetName, UnitNumber,
        City, StateOrProvince, PostalCode, CountyOrParish,
        BedroomsTotal, BathroomsTotalInteger, BathroomsFull, BathroomsHalf,
        LivingArea, StandardStatus, PropertyType, PropertySubType,
        PrimaryPhoto, ListingContractDate, ModificationTimestamp, StatusChangeTimestamp,
        Latitude, Longitude, YearBuilt, LotSizeAcres, SubdivisionName, PublicRemarks,
        ListAgentKey, ListAgentFullName, ListAgentEmail, ListAgentDirectPhone, ListAgentMlsId,
        ListOfficeKey, ListOfficeName, ListOfficePhone, ListOfficeMlsId,
        InternetEntireListingDisplayYN, InternetAddressDisplayYN,
        OriginatingSystemKey, OriginatingSystemName
    ) VALUES (${values.join(', ')});`;
}

async function runHydration() {
    verifyDatabaseTarget();

    console.log('====================================================');
    console.log('SNEAK IDX — CONTROLLED STAGING DATA HYDRATOR');
    console.log('====================================================');
    console.log(`Target Database: ${TARGET_DB_NAME} (${TARGET_DB_ID})`);
    console.log(`Execution Mode:  ${isExecute ? 'EXECUTE (WRITING TO REMOTE D1)' : 'DRY RUN (READ ONLY)'}`);
    console.log(`Locked Filter:   ${FINAL_SNEAK_LISTING_FILTER}`);
    console.log('====================================================');

    if (!bridgeToken) {
        console.log('\nBRIDGE TOKEN REQUIRED LOCALLY');
        console.log('Notice: BRIDGE_TOKEN is not set in environment or .dev.vars.');
        console.log('Cannot proceed with live Bridge data fetch without valid credentials.');
        process.exit(0);
    }

    // Step A: Fetch Expected @odata.count
    console.log('\n[Step 1] Querying Expected Listing Count from Bridge...');
    const countUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
    countUrl.searchParams.set('$top', '1');
    countUrl.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
    countUrl.searchParams.set('$count', 'true');
    countUrl.searchParams.set('access_token', bridgeToken);

    const countRes = await fetch(countUrl.toString(), { headers: { Accept: 'application/json' } });
    if (!countRes.ok) throw new Error(`Bridge Count Query HTTP ${countRes.status}: ${countRes.statusText}`);
    const countData = await countRes.json();
    const expectedCount = countData['@odata.count'] || 0;
    console.log(`  Expected Bridge Records (@odata.count): ${expectedCount.toLocaleString()}`);

    const maxPagesArg = process.argv.find(a => a.startsWith('--pages='));
    const isPagesAll = maxPagesArg ? maxPagesArg.includes('all') : false;
    const maxPages = isPagesAll ? Infinity : (maxPagesArg ? parseInt(maxPagesArg.split('=')[1], 10) : (isDryRun ? Infinity : Infinity));

    const top = MAX_PAGE_SIZE;
    let pageCount = 0;
    let totalFetched = 0;
    let totalTransformed = 0;
    let totalWithCoords = 0;
    let totalWithPhotos = 0;
    let totalWithAgentMlsId = 0;
    let totalWithOfficeMlsId = 0;
    let totalWithOfficeKey = 0;

    const seenListingKeys = new Set();
    let duplicateCount = 0;

    const scratchDir = path.join(rootDir, 'scratch');
    if (isExecute && !fs.existsSync(scratchDir)) {
        fs.mkdirSync(scratchDir, { recursive: true });
    }

    const startTime = Date.now();
    let batchIndex = 0;
    let writtenCount = 0;

    let nextUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
    nextUrl.searchParams.set('$top', String(top));
    nextUrl.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
    nextUrl.searchParams.set('$select', SELECT_PARAM);
    nextUrl.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);
    nextUrl.searchParams.set('access_token', bridgeToken);

    let currentUrl = nextUrl.toString();

    while (currentUrl && pageCount < maxPages) {
        pageCount++;

        const res = await fetch(currentUrl, { headers: { Accept: 'application/json' } });
        if (!res.ok) {
            let errText = '';
            try { errText = JSON.stringify(await res.json()); } catch { errText = await res.text(); }
            throw new Error(`Bridge API Fetch Error HTTP ${res.status} on page ${pageCount}:\n${errText}`);
        }

        const data = await res.json();
        const records = data.value || [];
        totalFetched += records.length;

        const currentBatchSql = [];

        for (const r of records) {
            if (!r.ListingKey) {
                console.error(`  Warning: Record on page ${pageCount} missing ListingKey!`);
                continue;
            }

            if (seenListingKeys.has(r.ListingKey)) {
                duplicateCount++;
            } else {
                seenListingKeys.add(r.ListingKey);
            }

            totalTransformed++;
            const lat = r.Latitude ?? (Array.isArray(r.Coordinates) ? r.Coordinates[1] : null);
            const lon = r.Longitude ?? (Array.isArray(r.Coordinates) ? r.Coordinates[0] : null);
            if (lat != null && lon != null) totalWithCoords++;
            if (extractPrimaryPhoto(r.Media)) totalWithPhotos++;
            if (r.ListAgentMlsId) totalWithAgentMlsId++;
            if (r.ListOfficeMlsId) totalWithOfficeMlsId++;
            if (r.ListOfficeKey) totalWithOfficeKey++;

            if (isExecute) {
                currentBatchSql.push(recordToSql(r));
            }
        }

        if (pageCount % 10 === 0 || !data['@odata.nextLink'] || pageCount === maxPages) {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log(`  Page ${pageCount}: fetched ${totalFetched.toLocaleString()}/${expectedCount.toLocaleString()} records (${elapsed}s elapsed, ${writtenCount.toLocaleString()} written)`);
        }

        // In EXECUTE mode, write batch immediately to D1 in chunks of 500
        if (isExecute && currentBatchSql.length > 0) {
            const chunkSize = 500;
            for (let c = 0; c < currentBatchSql.length; c += chunkSize) {
                batchIndex++;
                const chunkRows = currentBatchSql.slice(c, c + chunkSize);
                const chunkSql = chunkRows.join('\n');
                const batchFile = path.join(scratchDir, `hydrate-chunk-${batchIndex}.sql`);
                fs.writeFileSync(batchFile, chunkSql, 'utf8');

                try {
                    execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${batchFile} -c ${WRANGLER_CONFIG}`, {
                        cwd: rootDir,
                        stdio: 'pipe'
                    });
                    writtenCount += chunkRows.length;
                } catch (batchErr) {
                    console.error(`\nFATAL D1 BATCH FAILURE on batch ${batchIndex} (rows written before failure: ${writtenCount})`);
                    console.error('Batch error detail:', batchErr.message);
                    if (fs.existsSync(batchFile)) { try { fs.unlinkSync(batchFile); } catch {} }
                    process.exit(1);
                }

                if (fs.existsSync(batchFile)) {
                    try { fs.unlinkSync(batchFile); } catch {}
                }
            }
        }

        // Advance to nextLink
        if (data['@odata.nextLink']) {
            let nLink = data['@odata.nextLink'];
            if (!nLink.includes('access_token=')) {
                nLink += (nLink.includes('?') ? '&' : '?') + `access_token=${bridgeToken}`;
            }
            currentUrl = nLink;
        } else {
            currentUrl = null;
        }
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log('\n====================================================');
    console.log(`HYDRATION AUDIT SUMMARY (${duration}s total)`);
    console.log('====================================================');
    console.log(`- Expected Count (@odata.count):   ${expectedCount.toLocaleString()}`);
    console.log(`- Total Records Fetched:           ${totalFetched.toLocaleString()}`);
    console.log(`- Total Unique ListingKeys:        ${seenListingKeys.size.toLocaleString()}`);
    console.log(`- Duplicate ListingKeys:           ${duplicateCount}`);
    console.log(`- Total Pages Fetched:             ${pageCount}`);
    console.log(`- Records with Coordinates:        ${totalWithCoords.toLocaleString()} (${Math.round((totalWithCoords/totalTransformed)*100)}%)`);
    console.log(`- Records with Primary Photo:      ${totalWithPhotos.toLocaleString()} (${Math.round((totalWithPhotos/totalTransformed)*100)}%)`);
    console.log(`- Records with ListAgentMlsId:     ${totalWithAgentMlsId.toLocaleString()} (${Math.round((totalWithAgentMlsId/totalTransformed)*100)}%)`);
    console.log(`- Records with ListOfficeMlsId:    ${totalWithOfficeMlsId.toLocaleString()} (${Math.round((totalWithOfficeMlsId/totalTransformed)*100)}%)`);
    console.log(`- Records with ListOfficeKey:      ${totalWithOfficeKey.toLocaleString()} (${Math.round((totalWithOfficeKey/totalTransformed)*100)}%)`);

    if (isDryRun) {
        console.log('\n[COMPLETENESS GUARD VERIFICATION]');
        if (totalFetched !== expectedCount) {
            console.error(`FAIL: Records fetched (${totalFetched}) does not match expected count (${expectedCount})!`);
            process.exit(1);
        }
        if (duplicateCount > 0) {
            console.warn(`WARNING: ${duplicateCount} duplicate ListingKeys encountered.`);
        }
        console.log('PASS: All completeness guards satisfied. DRY RUN performed 0 database writes.');
        console.log('To execute remote staging ingestion, run: node scripts/hydrate-bridge-staging.mjs --execute');
        return;
    }

    // Update sneak_sync_state only after all batches succeed
    console.log('\n[Step 3] Updating sneak_sync_state Checkpoint...');
    const syncSql = `INSERT OR REPLACE INTO sneak_sync_state (
        sync_name, last_successful_sync, last_full_reconciliation, last_record_count, status, updated_at
    ) VALUES (
        'listings', datetime('now'), datetime('now'), ${writtenCount}, 'success', datetime('now')
    );`;
    const syncFile = path.join(scratchDir, 'sync-checkpoint.sql');
    fs.writeFileSync(syncFile, syncSql, 'utf8');
    execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${syncFile} -c ${WRANGLER_CONFIG}`, {
        cwd: rootDir,
        stdio: 'pipe'
    });
    fs.unlinkSync(syncFile);

    console.log('\n====================================================');
    console.log(`STAGING HYDRATION COMPLETED SUCCESSFULLY (${duration}s)`);
    console.log(`Total Rows Written to ${TARGET_DB_NAME}: ${writtenCount.toLocaleString()}`);
    console.log(`Total D1 Batches: ${batchIndex}`);
    console.log('====================================================');
}

runHydration().catch(err => {
    console.error('Hydration error:', err.message);
    process.exit(1);
});
