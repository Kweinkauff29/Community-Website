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
 * - Safely handles zero values (value ?? null).
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

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

// 3. Schema Fields
const INTENDED_FIELDS = [
    'ListingKey', 'ListingId', 'ListPrice', 'OriginalListPrice',
    'UnparsedAddress', 'StreetNumber', 'StreetName', 'UnitNumber',
    'City', 'StateOrProvince', 'PostalCode', 'CountyOrParish',
    'BedroomsTotal', 'BathroomsTotalInteger', 'BathroomsFull', 'BathroomsHalf',
    'LivingArea', 'StandardStatus', 'PropertyType', 'PropertySubType',
    'ListingContractDate', 'ModificationTimestamp', 'StatusChangeTimestamp',
    'YearBuilt', 'LotSizeAcres', 'Latitude', 'Longitude', 'Coordinates',
    'Media', 'PublicRemarks', 'SubdivisionName',
    'ListAgentKey', 'ListAgentMlsId', 'ListAgentFullName', 'ListAgentEmail', 'ListAgentDirectPhone',
    'ListOfficeKey', 'ListOfficeMlsId', 'ListOfficeName', 'ListOfficePhone',
    'OriginatingSystemKey', 'OriginatingSystemName'
];

function extractPrimaryPhoto(media) {
    if (!Array.isArray(media) || media.length === 0) return null;
    // Sort by Order if present
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
    console.log('====================================================');

    if (!bridgeToken) {
        console.log('\nBRIDGE TOKEN REQUIRED LOCALLY');
        console.log('Notice: BRIDGE_TOKEN is not set in environment or .dev.vars.');
        console.log('Cannot proceed with live Bridge data fetch without valid credentials.');
        process.exit(0);
    }

    const selectFields = INTENDED_FIELDS.filter(f => f !== 'Latitude' && f !== 'Longitude').join(',');
    const filter = "StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";
    
    let skip = 0;
    const top = 200;
    let hasMore = true;
    let totalFetched = 0;
    let totalTransformed = 0;
    let totalWithCoords = 0;
    let totalWithPhotos = 0;
    const sqlStatements = [];

    let pageCount = 0;
    const maxPagesArg = process.argv.find(a => a.startsWith('--pages='));
    const maxPages = maxPagesArg ? (maxPagesArg.includes('all') ? Infinity : parseInt(maxPagesArg.split('=')[1], 10)) : (isDryRun ? 5 : Infinity);

    const startTime = Date.now();

    while (hasMore && pageCount < maxPages) {
        pageCount++;
        const url = new URL('https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property');
        url.searchParams.set('$top', String(top));
        url.searchParams.set('$skip', String(skip));
        url.searchParams.set('$filter', filter);
        url.searchParams.set('$select', selectFields);
        url.searchParams.set('access_token', bridgeToken);

        const res = await fetch(url.toString(), { headers: { Accept: 'application/json' } });
        if (!res.ok) {
            throw new Error(`Bridge API Error HTTP ${res.status}: ${res.statusText}`);
        }

        const data = await res.json();
        const records = data.value || [];
        totalFetched += records.length;

        for (const r of records) {
            if (!r.ListingKey) continue;
            totalTransformed++;
            const lat = r.Latitude ?? (Array.isArray(r.Coordinates) ? r.Coordinates[1] : null);
            const lon = r.Longitude ?? (Array.isArray(r.Coordinates) ? r.Coordinates[0] : null);
            if (lat != null && lon != null) totalWithCoords++;
            if (extractPrimaryPhoto(r.Media)) totalWithPhotos++;
            sqlStatements.push(recordToSql(r));
        }

        console.log(`  Fetched page ${pageCount}: ${records.length} records (Total so far: ${totalFetched})`);

        if (records.length < top) {
            hasMore = false;
        } else {
            skip += top;
        }
    }

    const fetchDuration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\nFetch Summary (${fetchDuration}s):`);
    console.log(`- Total Records Fetched:      ${totalFetched}`);
    console.log(`- Total Transformed SQL rows: ${totalTransformed}`);
    console.log(`- Records with Coordinates:   ${totalWithCoords} (${Math.round((totalWithCoords/totalTransformed)*100)}%)`);
    console.log(`- Records with Primary Photo: ${totalWithPhotos} (${Math.round((totalWithPhotos/totalTransformed)*100)}%)`);

    if (isDryRun) {
        console.log('\nDRY RUN COMPLETE: Zero database writes performed.');
        console.log('To execute remote staging ingestion, run: node scripts/hydrate-bridge-staging.mjs --execute');
        return;
    }

    // EXECUTE MODE: Write to sneak-idx-staging in batches
    console.log('\n[EXECUTE MODE] Writing to sneak-idx-staging in batches of 100...');
    const scratchDir = path.join(rootDir, 'scratch');
    if (!fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });

    const batchSize = 100;
    let writtenCount = 0;
    for (let i = 0; i < sqlStatements.length; i += batchSize) {
        const batchSql = sqlStatements.slice(i, i + batchSize).join('\n');
        const batchFile = path.join(scratchDir, `hydrate-batch-${Math.floor(i/batchSize)}.sql`);
        fs.writeFileSync(batchFile, batchSql, 'utf8');

        console.log(`  Applying batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(sqlStatements.length/batchSize)} (${Math.min(batchSize, sqlStatements.length - i)} rows)...`);
        execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${batchFile} -c ${WRANGLER_CONFIG}`, {
            cwd: rootDir,
            stdio: 'pipe'
        });

        writtenCount += Math.min(batchSize, sqlStatements.length - i);
        fs.unlinkSync(batchFile);
    }

    // Checkpoint in sneak_sync_state
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

    const totalDuration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\n====================================================');
    console.log(`STAGING HYDRATION COMPLETED SUCCESSFULLY (${totalDuration}s)`);
    console.log(`Rows written to ${TARGET_DB_NAME}: ${writtenCount}`);
    console.log('====================================================');
}

runHydration().catch(err => {
    console.error('Hydration error:', err.message);
    process.exit(1);
});
