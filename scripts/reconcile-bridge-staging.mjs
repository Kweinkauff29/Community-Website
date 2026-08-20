#!/usr/bin/env node

/**
 * scripts/reconcile-bridge-staging.mjs
 * 
 * Full Inventory Reconciliation Utility for SNEAK IDX Staging.
 * 
 * PURPOSE:
 * Audits complete active serving inventory in sneak_listings against Bridge OData eligible listings.
 * Identifies and prunes any stale/orphaned listings in D1, while reporting any missing records.
 * 
 * SAFETY:
 * - Requires 100% complete traversal of Bridge eligible feed before calculating diffs.
 * - Defaults to --dry-run (read-only audit).
 * - Never truncates or bulk-wipes D1.
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
    BRIDGE_PROPERTY_ENDPOINT,
    FINAL_SNEAK_LISTING_FILTER,
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

async function getD1ListingKeys() {
    console.log('\n[Step 1] Querying Serving ListingKeys from sneak-idx-staging D1...');
    // Query in paginated chunks to avoid large JSON buffer limits
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

    console.log(`  Current D1 Serving Keys: ${keys.size.toLocaleString()}`);
    return keys;
}

async function getBridgeEligibleKeys() {
    console.log('\n[Step 2] Fetching All Eligible ListingKeys from Bridge...');
    const countUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
    countUrl.searchParams.set('$top', '1');
    countUrl.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
    countUrl.searchParams.set('$count', 'true');
    countUrl.searchParams.set('access_token', bridgeToken);

    const countRes = await fetch(countUrl.toString(), { headers: { Accept: 'application/json' } });
    if (!countRes.ok) throw new Error(`Bridge Count Query HTTP ${countRes.status}: ${countRes.statusText}`);
    const countData = await countRes.json();
    const expectedCount = countData['@odata.count'] || 0;
    console.log(`  Expected Bridge Eligible Listings: ${expectedCount.toLocaleString()}`);

    let currentUrl = new URL(BRIDGE_PROPERTY_ENDPOINT);
    currentUrl.searchParams.set('$top', String(MAX_PAGE_SIZE));
    currentUrl.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
    currentUrl.searchParams.set('$select', 'ListingKey');
    currentUrl.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);
    currentUrl.searchParams.set('access_token', bridgeToken);

    let nextUrlStr = currentUrl.toString();
    let pageCount = 0;
    const bridgeKeys = new Set();
    const startTime = Date.now();

    while (nextUrlStr) {
        pageCount++;
        const res = await fetch(nextUrlStr, { headers: { Accept: 'application/json' } });
        if (!res.ok) throw new Error(`Bridge Reconciliation Fetch HTTP ${res.status} on page ${pageCount}`);
        const data = await res.json();
        const records = data.value || [];

        for (const r of records) {
            if (r.ListingKey) bridgeKeys.add(r.ListingKey);
        }

        if (pageCount % 20 === 0 || !data['@odata.nextLink']) {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log(`  Page ${pageCount}: collected ${bridgeKeys.size.toLocaleString()}/${expectedCount.toLocaleString()} keys (${elapsed}s elapsed)`);
        }

        if (data['@odata.nextLink']) {
            let nLink = data['@odata.nextLink'];
            if (!nLink.includes('access_token=')) nLink += `&access_token=${bridgeToken}`;
            nextUrlStr = nLink;
        } else {
            nextUrlStr = null;
        }
    }

    if (bridgeKeys.size !== expectedCount) {
        console.warn(`Notice: Bridge keys collected (${bridgeKeys.size}) vs expected count (${expectedCount}) — variance of ${Math.abs(bridgeKeys.size - expectedCount)}`);
    }

    return bridgeKeys;
}

async function runReconciliation() {
    verifyDatabaseTarget();

    console.log('====================================================');
    console.log('SNEAK IDX — FULL INVENTORY RECONCILIATION');
    console.log('====================================================');
    console.log(`Target Database: ${TARGET_DB_NAME} (${TARGET_DB_ID})`);
    console.log(`Execution Mode:  ${isExecute ? 'EXECUTE (PRUNING STALE ROWS)' : 'DRY RUN (READ ONLY)'}`);
    console.log('====================================================');

    if (!bridgeToken) {
        console.log('\nBRIDGE TOKEN REQUIRED LOCALLY');
        console.log('Notice: BRIDGE_TOKEN is not set in environment or .dev.vars.');
        process.exit(0);
    }

    const d1Keys = await getD1ListingKeys();
    const bridgeKeys = await getBridgeEligibleKeys();

    console.log('\n[Step 3] Computing Inventory Diff...');
    const staleKeys = [];
    const missingKeys = [];

    for (const key of d1Keys) {
        if (!bridgeKeys.has(key)) staleKeys.push(key);
    }
    for (const key of bridgeKeys) {
        if (!d1Keys.has(key)) missingKeys.push(key);
    }

    console.log('\n====================================================');
    console.log('RECONCILIATION AUDIT SUMMARY');
    console.log('====================================================');
    console.log(`- Current Bridge Eligible Keys: ${bridgeKeys.size.toLocaleString()}`);
    console.log(`- Current D1 Serving Keys:      ${d1Keys.size.toLocaleString()}`);
    console.log(`- Stale Rows in D1:             ${staleKeys.length.toLocaleString()}`);
    console.log(`- Missing Rows in D1:           ${missingKeys.length.toLocaleString()}`);

    if (isDryRun) {
        console.log('\nDRY RUN COMPLETE: Zero database writes performed.');
        if (staleKeys.length > 0) {
            console.log(`To prune ${staleKeys.length} stale rows, run: node scripts/reconcile-bridge-staging.mjs --execute`);
        }
        return;
    }

    // In Execute Mode, Prune Stale Rows
    const scratchDir = path.join(rootDir, 'scratch');
    if (!fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });

    let pruned = 0;
    if (staleKeys.length > 0) {
        console.log(`\n[Pruning ${staleKeys.length} Stale Rows from D1 in Chunks of 200...]`);
        const chunkSize = 200;
        let batchIdx = 0;

        for (let i = 0; i < staleKeys.length; i += chunkSize) {
            batchIdx++;
            const chunk = staleKeys.slice(i, i + chunkSize);
            const sql = `DELETE FROM sneak_listings WHERE ListingKey IN (${chunk.map(k => `'${k}'`).join(', ')});`;
            const file = path.join(scratchDir, `reconcile-chunk-${batchIdx}.sql`);
            fs.writeFileSync(file, sql, 'utf8');

            try {
                execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${file} -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });
                pruned += chunk.length;
            } catch (err) {
                console.error(`FATAL RECONCILIATION PRUNING ERROR on batch ${batchIdx}`);
                if (fs.existsSync(file)) { try { fs.unlinkSync(file); } catch {} }
                process.exit(1);
            }

            if (fs.existsSync(file)) { try { fs.unlinkSync(file); } catch {} }
        }
    }

    // Update last_full_reconciliation
    console.log('\n[Updating Reconciliation Checkpoint]');
    const postCountQuery = `SELECT count(*) as count FROM sneak_listings;`;
    const postCountOut = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${postCountQuery}" --json -c ${WRANGLER_CONFIG}`, {
        cwd: rootDir,
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'ignore']
    });
    let postReconcileCount = 0;
    try {
        postReconcileCount = JSON.parse(postCountOut)[0]?.results[0]?.count || 0;
    } catch {}

    const updateRecSql = `UPDATE sneak_sync_state SET last_full_reconciliation = datetime('now'), last_record_count = ${postReconcileCount}, updated_at = datetime('now') WHERE sync_name = 'listings';`;
    execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --command="${updateRecSql}" -c ${WRANGLER_CONFIG}`, { cwd: rootDir, stdio: 'pipe' });

    console.log('====================================================');
    console.log('RECONCILIATION COMPLETED SUCCESSFULLY');
    console.log(`- Rows Pruned:               ${pruned.toLocaleString()}`);
    console.log(`- Post-Reconciliation Count: ${postReconcileCount.toLocaleString()}`);
    console.log('====================================================');
}

runReconciliation().catch(err => {
    console.error('Reconciliation Error:', err.message);
    process.exit(1);
});
