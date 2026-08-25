// scripts/sample-media-stats.mjs
import { readFileSync } from 'fs';
import { BRIDGE_PROPERTY_ENDPOINT, FINAL_SNEAK_LISTING_FILTER } from './bridge-config.mjs';

let bridgeToken = process.env.BRIDGE_TOKEN;
if (!bridgeToken) {
    try {
        const vars = readFileSync('.dev.vars', 'utf8');
        for (const line of vars.split('\n')) {
            const trimmed = line.trim();
            if (trimmed.startsWith('BRIDGE_TOKEN=')) {
                bridgeToken = trimmed.substring('BRIDGE_TOKEN='.length).trim().replace(/^["']|["']$/g, '');
            }
        }
    } catch {}
}

if (!bridgeToken) {
    console.error('BRIDGE_TOKEN not found in environment or .dev.vars');
    process.exit(1);
}

async function run() {
    const url = new URL(BRIDGE_PROPERTY_ENDPOINT);
    url.searchParams.set('$top', '200');
    url.searchParams.set('$select', 'ListingKey,Media');
    url.searchParams.set('$filter', FINAL_SNEAK_LISTING_FILTER);
    url.searchParams.set('access_token', bridgeToken);

    console.log('Querying 200 eligible records from Bridge for Media statistics...');
    const res = await fetch(url.toString(), { headers: { Accept: 'application/json' } });
    if (!res.ok) {
        throw new Error(`Bridge API HTTP ${res.status}: ${await res.text()}`);
    }

    const data = await res.json();
    const records = data.value || [];
    console.log(`Successfully fetched ${records.length} records.`);

    const mediaCounts = [];
    const payloadBytes = [];

    for (const r of records) {
        const media = r.Media || [];
        const sorted = [...media].sort((a, b) => (a.Order ?? 999) - (b.Order ?? 999));
        const urls = sorted.map(m => m?.MediaURL).filter(Boolean);
        const uniqueUrls = Array.from(new Set(urls));
        mediaCounts.push(uniqueUrls.length);
        const jsonStr = JSON.stringify(uniqueUrls);
        payloadBytes.push(Buffer.byteLength(jsonStr, 'utf8'));
    }

    mediaCounts.sort((a, b) => a - b);
    payloadBytes.sort((a, b) => a - b);

    const sum = mediaCounts.reduce((a, b) => a + b, 0);
    const avg = sum / mediaCounts.length;
    const median = mediaCounts[Math.floor(mediaCounts.length / 2)];
    const p95 = mediaCounts[Math.floor(mediaCounts.length * 0.95)];
    const max = mediaCounts[mediaCounts.length - 1];

    const byteSum = payloadBytes.reduce((a, b) => a + b, 0);
    const avgBytes = byteSum / payloadBytes.length;
    const medianBytes = payloadBytes[Math.floor(payloadBytes.length / 2)];
    const p95Bytes = payloadBytes[Math.floor(payloadBytes.length * 0.95)];
    const maxBytes = payloadBytes[payloadBytes.length - 1];

    const estimatedTotalMB = ((avgBytes * 37145) / (1024 * 1024)).toFixed(2);

    console.log('\n====================================================');
    console.log('BRIDGE MEDIA SAMPLING RESULTS (200 Eligible Records)');
    console.log('====================================================');
    console.log(`Average Media Count:         ${avg.toFixed(2)}`);
    console.log(`Median Media Count:          ${median}`);
    console.log(`95th Percentile Media Count: ${p95}`);
    console.log(`Maximum Media Count:         ${max}`);
    console.log(`Average MediaJSON Bytes:     ${avgBytes.toFixed(2)} bytes (~${(avgBytes / 1024).toFixed(2)} KB)`);
    console.log(`Median MediaJSON Bytes:      ${medianBytes} bytes`);
    console.log(`95th Percentile Bytes:       ${p95Bytes} bytes`);
    console.log(`Max MediaJSON Bytes:         ${maxBytes} bytes`);
    console.log(`Estimated 37,145 Listings:   ${estimatedTotalMB} MB total D1 storage`);
    console.log('====================================================\n');
}

run().catch(console.error);
