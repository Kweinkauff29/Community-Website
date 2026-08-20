#!/usr/bin/env node

/**
 * scripts/sync-open-houses-staging.mjs
 * 
 * Ingestion and synchronization utility for real Bridge MLS OpenHouse data.
 * 
 * LOGIC:
 * 1. Queries Bridge for future open houses (OpenHouseDate >= today).
 * 2. Queries active ListingKeys currently in sneak_listings.
 * 3. Filters to only open houses whose ListingKey exists in sneak_listings (preventing off-market/unauthorized OH).
 * 4. Persists into sneak_open_houses.
 * 5. Reconciles stale/past open houses, leaving only valid future events.
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
    BRIDGE_OPENHOUSE_ENDPOINT,
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

function escapeSql(val) {
    if (val === null || val === undefined) return 'NULL';
    if (typeof val === 'number') return isNaN(val) ? 'NULL' : String(val);
    if (typeof val === 'boolean') return val ? '1' : '0';
    return "'" + String(val).replace(/'/g, "''") + "'";
}

async function getActiveListingKeys() {
    console.log('\n[Step 1] Loading Active ListingKeys from sneak_listings in D1...');
    const keys = new Set();
    let offset = 0;
    const limit = 10000;
    let hasMore = true;

    while (hasMore) {
        const query = `SELECT ListingKey FROM sneak_listings LIMIT ${limit} OFFSET ${offset};`;
        const raw = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${query}" --json -c ${WRANGLER_CONFIG}`, {
            cwd: rootDir,
            encoding: 'utf8',
            maxBuffer: 50 * 1024 * 1024,
            stdio: ['pipe', 'pipe', 'ignore']
        });
        const parsed = JSON.parse(raw);
        const rows = parsed[0]?.results || [];
        for (const row of rows) {
            if (row.ListingKey) keys.add(row.ListingKey);
        }
        if (rows.length < limit) {
            hasMore = false;
        } else {
            offset += limit;
        }
    }
    console.log(`  Loaded ${keys.size.toLocaleString()} active listing keys for cross-referencing.`);
    return keys;
}

async function runOpenHouseSync() {
    verifyDatabaseTarget();

    console.log('====================================================');
    console.log('SNEAK IDX — REAL OPEN HOUSE SYNCHRONIZER');
    console.log('====================================================');
    console.log(`Target Database: ${TARGET_DB_NAME} (${TARGET_DB_ID})`);
    console.log(`Execution Mode:  ${isExecute ? 'EXECUTE (WRITING TO D1)' : 'DRY RUN (READ ONLY)'}`);
    console.log('====================================================');

    if (!bridgeToken) {
        console.log('\nBRIDGE TOKEN REQUIRED LOCALLY');
        console.log('Notice: BRIDGE_TOKEN is not set in environment or .dev.vars.');
        process.exit(0);
    }

    const activeListingKeys = await getActiveListingKeys();

    // 2. Query Bridge OpenHouse for future dates
    const today = new Date().toISOString().slice(0, 10);
    // Horizon: next 60 days
    const maxFutureDate = new Date(Date.now() + 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const ohFilter = `OriginatingSystemKey eq 'bsaor' and OpenHouseDate ge ${today} and OpenHouseDate le ${maxFutureDate}`;
    console.log(`\n[Step 2] Fetching Future Open Houses from Bridge (Horizon: ${today} to ${maxFutureDate})...`);
    console.log(`  Filter: ${ohFilter}`);

    const countUrl = new URL(BRIDGE_OPENHOUSE_ENDPOINT);
    countUrl.searchParams.set('$top', '1');
    countUrl.searchParams.set('$filter', ohFilter);
    countUrl.searchParams.set('$count', 'true');
    countUrl.searchParams.set('access_token', bridgeToken);

    const countRes = await fetch(countUrl.toString(), { headers: { Accept: 'application/json' } });
    if (!countRes.ok) throw new Error(`Bridge OpenHouse Count HTTP ${countRes.status}: ${countRes.statusText}`);
    const countData = await countRes.json();
    const expectedCount = countData['@odata.count'] || 0;
    console.log(`  Expected Future Open Houses in Horizon: ${expectedCount.toLocaleString()}`);

    let currentUrl = new URL(BRIDGE_OPENHOUSE_ENDPOINT);
    currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
    currentUrl.searchParams.set('$filter', ohFilter);
    currentUrl.searchParams.set('$orderby', 'OpenHouseDate asc');
    currentUrl.searchParams.set('access_token', bridgeToken);

    let nextUrlStr = currentUrl.toString();
    let pageCount = 0;
    let totalFetched = 0;
    let duplicateCount = 0;
    let missingOpenHouseKey = 0;
    let missingListingKey = 0;
    let validListingMatches = 0;
    let missingListingMatches = 0;
    let cancelledCount = 0;

    const validOpenHouses = [];
    const validOHKeys = new Set();
    const seenAllOHKeys = new Set();
    const statusCounts = {};
    const typeCounts = {};

    while (nextUrlStr) {
        pageCount++;
        const res = await fetch(nextUrlStr, { headers: { Accept: 'application/json' } });
        if (!res.ok) throw new Error(`Bridge OpenHouse Fetch HTTP ${res.status} on page ${pageCount}`);
        const data = await res.json();
        const records = data.value || [];
        totalFetched += records.length;

        for (const r of records) {
            if (!r.OpenHouseKey) { missingOpenHouseKey++; continue; }
            if (!r.ListingKey) { missingListingKey++; continue; }

            if (seenAllOHKeys.has(r.OpenHouseKey)) {
                duplicateCount++;
            } else {
                seenAllOHKeys.add(r.OpenHouseKey);
            }

            const st = r.OpenHouseStatus || 'Active';
            statusCounts[st] = (statusCounts[st] || 0) + 1;
            const tp = r.OpenHouseType || 'Public';
            typeCounts[tp] = (typeCounts[tp] || 0) + 1;

            if (r.OpenHouseStatus === 'Canceled' || r.OpenHouseStatus === 'Cancelled') {
                cancelledCount++;
                continue;
            }

            // Check if listing is active in sneak_listings
            if (activeListingKeys.has(r.ListingKey)) {
                validListingMatches++;
                validOHKeys.add(r.OpenHouseKey);
                validOpenHouses.push(r);
            } else {
                missingListingMatches++;
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

    console.log('\n====================================================');
    console.log('OPEN HOUSE INGESTION AUDIT');
    console.log('====================================================');
    console.log(`- Expected Future Open Houses:     ${expectedCount.toLocaleString()}`);
    console.log(`- Total Open Houses Fetched:       ${totalFetched.toLocaleString()}`);
    console.log(`- Unique OpenHouseKeys:            ${seenAllOHKeys.size.toLocaleString()}`);
    console.log(`- Duplicates:                      ${duplicateCount}`);
    console.log(`- Missing OpenHouseKey:            ${missingOpenHouseKey}`);
    console.log(`- Missing ListingKey:              ${missingListingKey}`);
    console.log(`- Status Distribution:             ${JSON.stringify(statusCounts)}`);
    console.log(`- Type Distribution:               ${JSON.stringify(typeCounts)}`);
    console.log(`- Matched Active sneak_listings:   ${validListingMatches.toLocaleString()}`);
    console.log(`- Unmatched / Off-Market Listings: ${missingListingMatches.toLocaleString()}`);
    console.log(`- Cancelled Events Skipped:        ${cancelledCount.toLocaleString()}`);

    // STRICT FAIL-CLOSED COMPLETENESS GUARDS
    if (totalFetched !== expectedCount) {
        throw new Error(`FATAL OPEN HOUSE SHORTFALL: Fetched (${totalFetched}) !== expected count (${expectedCount}). Aborted with ZERO D1 modifications.`);
    }
    if (seenAllOHKeys.size !== expectedCount) {
        throw new Error(`FATAL OPEN HOUSE KEY MISMATCH: Unique keys (${seenAllOHKeys.size}) !== expected count (${expectedCount}). Aborted with ZERO D1 modifications.`);
    }
    if (duplicateCount > 0) {
        throw new Error(`FATAL OPEN HOUSE DUPLICATES: Encountered ${duplicateCount} duplicate OpenHouseKeys. Aborted with ZERO D1 modifications.`);
    }
    if (missingOpenHouseKey > 0 || missingListingKey > 0) {
        throw new Error(`FATAL OPEN HOUSE MISSING IDENTIFIERS: OHKeys=${missingOpenHouseKey}, ListingKeys=${missingListingKey}. Aborted.`);
    }

    if (isDryRun) {
        console.log('\nDRY RUN COMPLETE: Zero database writes performed.');
        console.log(`To ingest ${validOpenHouses.length} valid open houses, run: node scripts/sync-open-houses-staging.mjs --execute`);
        return;
    }

    // 3. Write Valid Open Houses to D1
    const scratchDir = path.join(rootDir, 'scratch');
    if (!fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });

    console.log(`\n[Writing ${validOpenHouses.length} Valid Open Houses to sneak_open_houses...]`);
    const sqlStatements = validOpenHouses.map(r => {
        const id = `oh_${r.OpenHouseKey}`;
        return `INSERT OR REPLACE INTO sneak_open_houses (
            id, OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, created_at, updated_at
        ) VALUES (
            ${escapeSql(id)}, ${escapeSql(r.OpenHouseKey)}, ${escapeSql(r.ListingKey)},
            ${escapeSql(r.OpenHouseStartTime)}, ${escapeSql(r.OpenHouseEndTime)}, ${escapeSql(r.OpenHouseDate)},
            ${escapeSql(r.OpenHouseRemarks)}, datetime('now'), datetime('now')
        );`;
    });

    const chunkSize = 200;
    let batchIdx = 0;
    for (let i = 0; i < sqlStatements.length; i += chunkSize) {
        batchIdx++;
        const chunk = sqlStatements.slice(i, i + chunkSize);
        const file = path.join(scratchDir, `oh-chunk-${batchIdx}.sql`);
        fs.writeFileSync(file, chunk.join('\n'), 'utf8');

        try {
            execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${file} -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });
        } catch (err) {
            console.error(`FATAL OPEN HOUSE BATCH FAILURE on batch ${batchIdx}`);
            if (fs.existsSync(file)) { try { fs.unlinkSync(file); } catch {} }
            process.exit(1);
        }

        if (fs.existsSync(file)) { try { fs.unlinkSync(file); } catch {} }
    }

    // 4. Stale / Past / Cancelled / Off-Market Open House Reconciliation
    console.log('\n[Reconciling Stale, Cancelled, and Past Open Houses in D1...]');
    // Fetch all currently existing OH keys in D1
    const existingRaw = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="SELECT OpenHouseKey FROM sneak_open_houses;" --json -c ${WRANGLER_CONFIG}`, {
        cwd: rootDir,
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'ignore']
    });
    const existingRows = JSON.parse(existingRaw)[0]?.results || [];
    const staleOHKeys = [];
    for (const row of existingRows) {
        if (row.OpenHouseKey && !validOHKeys.has(row.OpenHouseKey)) {
            staleOHKeys.push(row.OpenHouseKey);
        }
    }

    let stalePruned = 0;
    if (staleOHKeys.length > 0) {
        console.log(`  Found ${staleOHKeys.length} stale/cancelled/off-market open houses in D1 to prune...`);
        for (let i = 0; i < staleOHKeys.length; i += chunkSize) {
            const chunk = staleOHKeys.slice(i, i + chunkSize);
            const pruneSql = `DELETE FROM sneak_open_houses WHERE OpenHouseKey IN (${chunk.map(k => `'${k}'`).join(', ')});`;
            const pruneFile = path.join(scratchDir, `oh-prune-${i}.sql`);
            fs.writeFileSync(pruneFile, pruneSql, 'utf8');
            try {
                execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${pruneFile} -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });
                stalePruned += chunk.length;
            } finally {
                if (fs.existsSync(pruneFile)) { try { fs.unlinkSync(pruneFile); } catch {} }
            }
        }
    }

    // Also remove any past date records
    const cleanupPastSql = `DELETE FROM sneak_open_houses WHERE OpenHouseDate < date('now');`;
    execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${cleanupPastSql}" -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });

    // Update sneak_sync_state for open houses
    const updateOHStateSql = `INSERT OR REPLACE INTO sneak_sync_state (
        sync_name, last_successful_sync, last_full_reconciliation, last_record_count, status, updated_at
    ) VALUES (
        'open_houses', datetime('now'), datetime('now'), ${validOpenHouses.length}, 'success', datetime('now')
    );`;
    execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${updateOHStateSql}" -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });

    const countQuery = `SELECT count(*) as count FROM sneak_open_houses;`;
    const countOut = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${countQuery}" --json -c ${WRANGLER_CONFIG}`, {
        cwd: rootDir,
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'ignore']
    });
    let finalOHCount = 0;
    try {
        finalOHCount = JSON.parse(countOut)[0]?.results[0]?.count || 0;
    } catch {}

    console.log('====================================================');
    console.log('OPEN HOUSE SYNCHRONIZATION COMPLETED');
    console.log(`- Valid Open Houses Ingested:    ${validOpenHouses.length.toLocaleString()}`);
    console.log(`- Stale/Off-Market Rows Pruned:  ${stalePruned.toLocaleString()}`);
    console.log(`- Final sneak_open_houses Count: ${finalOHCount.toLocaleString()}`);
    console.log('====================================================');
}

runOpenHouseSync().catch(err => {
    console.error('OpenHouse Sync Error:', err.message);
    process.exit(1);
});
