// scripts/populate-media-json.mjs
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

let bridgeToken = process.env.BRIDGE_TOKEN;
if (!bridgeToken) {
    try {
        const devVars = fs.readFileSync(path.join(rootDir, '.dev.vars'), 'utf8');
        for (const line of devVars.split('\n')) {
            const trimmed = line.trim();
            if (trimmed.startsWith('BRIDGE_TOKEN=')) {
                bridgeToken = trimmed.substring('BRIDGE_TOKEN='.length).trim().replace(/^["']|["']$/g, '');
                break;
            }
        }
    } catch {}
}

if (!bridgeToken) {
    console.error('BRIDGE_TOKEN missing from environment and .dev.vars');
    process.exit(1);
}

function escapeSql(val) {
    if (val === null || val === undefined) return 'NULL';
    if (typeof val === 'number') return Number.isFinite(val) ? String(val) : 'NULL';
    if (typeof val === 'boolean') return val ? '1' : '0';
    return `'${String(val).replace(/'/g, "''")}'`;
}

async function run() {
    console.log('====================================================');
    console.log('SNEAK IDX — MEDIAJSON POPULATION FROM BRIDGE');
    console.log('====================================================');

    const url = new URL(BRIDGE_PROPERTY_ENDPOINT);
    url.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
    url.searchParams.set('$select', 'ListingKey,Media,InternetEntireListingDisplayYN,InternetAddressDisplayYN');
    url.searchParams.set('$orderby', DETERMINISTIC_ORDERBY);
    url.searchParams.set('$top', String(MAX_PAGE_SIZE));
    url.searchParams.set('access_token', bridgeToken);

    let nextLink = url.toString();
    let totalProcessed = 0;
    let page = 0;
    const CHUNK_SIZE = 1000;
    let statements = [];
    const startTime = Date.now();

    while (nextLink) {
        page++;
        const res = await fetch(nextLink, { headers: { Accept: 'application/json' } });
        if (!res.ok) {
            throw new Error(`Bridge API HTTP ${res.status}: ${await res.text()}`);
        }
        const data = await res.json();
        const records = data.value || [];
        if (records.length === 0) break;

        for (const r of records) {
            if (!r.ListingKey) continue;
            totalProcessed++;

            const media = r.Media || [];
            const sorted = [...media].sort((a, b) => (a.Order ?? 999) - (b.Order ?? 999));
            const urls = sorted.map(m => m && m.MediaURL).filter(Boolean);
            const uniqueUrls = Array.from(new Set(urls));
            const mediaJson = uniqueUrls.length > 0 ? JSON.stringify(uniqueUrls) : null;
            const primaryPhoto = uniqueUrls.length > 0 ? uniqueUrls[0] : null;

            const entireDisplay = (r.InternetEntireListingDisplayYN === true || r.InternetEntireListingDisplayYN === 1) ? 1 : 0;
            const addrDisplay = (r.InternetAddressDisplayYN === true || r.InternetAddressDisplayYN === 1) ? 1 : 0;

            const updateSql = `UPDATE sneak_listings SET InternetEntireListingDisplayYN = ${entireDisplay}, InternetAddressDisplayYN = ${addrDisplay}, MediaJSON = ${escapeSql(mediaJson)}, PrimaryPhoto = COALESCE(${escapeSql(primaryPhoto)}, PrimaryPhoto) WHERE ListingKey = ${escapeSql(r.ListingKey)};`;
            statements.push(updateSql);

            if (statements.length >= CHUNK_SIZE) {
                flushBatch(statements, totalProcessed);
                statements = [];
            }
        }

        nextLink = data['@odata.nextLink'] || null;
        if (nextLink && !nextLink.includes('access_token=')) {
            const u = new URL(nextLink);
            u.searchParams.set('access_token', bridgeToken);
            nextLink = u.toString();
        }
    }

    if (statements.length > 0) {
        flushBatch(statements, totalProcessed);
        statements = [];
    }

    const durationSec = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n\nSuccessfully backfilled MediaJSON for ${totalProcessed} records in ${durationSec}s.`);
}

function flushBatch(stmts, processedCount) {
    const tmpFile = path.join(rootDir, '.temp_media_batch.sql');
    fs.writeFileSync(tmpFile, stmts.join('\n'), 'utf8');
    try {
        execSync(
            `npx wrangler d1 execute sneak-idx-staging --remote -c wrangler.sneak.toml --file=${tmpFile}`,
            { stdio: 'ignore' }
        );
        process.stdout.write(`\rBackfilled: ${processedCount} / ~37,145 listings...`);
    } finally {
        if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
    }
}

run().catch(err => {
    console.error('\nPopulation error:', err);
    process.exit(1);
});
