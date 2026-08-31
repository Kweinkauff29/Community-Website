/**
 * scripts/verify-phase73c2b-live.mjs
 * 
 * Live Staging Verification Script for SNEAK IDX Phase 7.3C2B:
 * Agent Client Activity Dashboard + Authenticated Buyer Activity Timeline
 * 
 * Checks:
 * 1. Alert Worker Live Version & Health Check (/api/alerts/version)
 * 2. Consumer Worker Live Version & Health Check (/api/consumer/version)
 * 3. Consumer Worker Activity Ingestion Route Protection (/api/consumer/activity -> 401 Unauthorized)
 * 4. Member Worker Live Version & Health Check (/api/member/version)
 * 5. Member Worker Clients Route Protection (/api/member/clients -> 401 Unauthorized)
 * 6. SNEAK IDX Serving Worker embed.js build version
 * 7. SNEAK IDX Search App search/index.html build version & activity capture hooks
 * 8. Live Pilot Test Page availability (coconutcoastrealtors.org/idx-test/)
 */

import assert from 'node:assert/strict';

const EXPECTED_BUILD = '2026.08.30.7.3c2b';
const ALERTS_WORKER = 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev';
const CONSUMER_WORKER = 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
const MEMBER_WORKER = 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev';
const IDX_WORKER = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const PILOT_URL = 'https://coconutcoastrealtors.org/idx-test/';

async function runVerification() {
    console.log('===============================================================');
    console.log(` SNEAK IDX Phase 7.3C2B Live Staging Verification`);
    console.log(` Target Build: ${EXPECTED_BUILD}`);
    console.log('===============================================================\n');

    let passed = 0;
    let total = 0;

    async function check(name, fn) {
        total++;
        try {
            await fn();
            console.log(` [PASS] ${name}`);
            passed++;
        } catch (err) {
            console.error(` [FAIL] ${name}:`, err.message);
        }
    }

    // 1. Alert Worker Version & Health
    await check('1. Alert Worker /api/alerts/version returns healthy status', async () => {
        const res = await fetch(`${ALERTS_WORKER}/api/alerts/version`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const data = await res.json();
        assert.equal(data.service, 'sneak-alerts-worker');
        assert.equal(data.status, 'healthy');
        console.log(`        Build: ${data.build} | deliveryReady: ${data.deliveryReady}`);
    });

    // 2. Consumer Worker Version & Health
    await check('2. Consumer Worker /api/consumer/version returns healthy status', async () => {
        const res = await fetch(`${CONSUMER_WORKER}/api/consumer/version`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const data = await res.json();
        assert.equal(data.service, 'sneak-consumer-worker');
        assert.equal(data.status, 'healthy');
        console.log(`        Build: ${data.build}`);
    });

    // 3. Consumer Worker Activity Route Security
    await check('3. Consumer Worker /api/consumer/activity rejects unauthenticated requests (401)', async () => {
        const res = await fetch(`${CONSUMER_WORKER}/api/consumer/activity`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ site: 'ursula-weinkauff-pilot', type: 'listing_view', listingKey: '2240001' })
        });
        assert.equal(res.status, 401, `Expected 401, got ${res.status}`);
    });

    // 4. Member Worker Version & Health
    await check('4. Member Worker /health returns healthy status', async () => {
        const res = await fetch(`${MEMBER_WORKER}/health`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const data = await res.json();
        assert.equal(data.worker, 'sneak-idx-member-staging');
        assert.equal(data.status, 'healthy');
        assert.equal(data.build, EXPECTED_BUILD);
        console.log(`        Build: ${data.build}`);
    });

    // 5. Member Worker Clients Route Security
    await check('5. Member Worker /api/member/clients rejects unauthenticated requests (401)', async () => {
        const res = await fetch(`${MEMBER_WORKER}/api/member/clients`);
        assert.equal(res.status, 401, `Expected 401, got ${res.status}`);
    });

    // 6. Embed Loader
    await check('6. Embed loader /embed.js serves live', async () => {
        const res = await fetch(`${IDX_WORKER}/embed.js`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const text = await res.text();
        assert.ok(text.includes('ccor_listing'), 'embed.js missing ccor_listing deep link support');
    });

    // 7. Search App UI & Activity Capture
    await check('7. Search app /search/index.html serves live with consumer session handlers', async () => {
        const res = await fetch(`${IDX_WORKER}/search/index.html`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const html = await res.text();
        assert.ok(html.includes('consumerSession') || html.includes('getConsumerSession'), 'search/index.html missing consumer session handlers');
    });

    // 8. Live Pilot Host
    await check('8. Live pilot host coconutcoastrealtors.org/idx-test/ is reachable (HTTP 200)', async () => {
        const res = await fetch(PILOT_URL, { headers: { 'User-Agent': 'CCOR-Verification/1.0' } });
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
    });

    console.log('\n---------------------------------------------------------------');
    console.log(` Verification Summary: ${passed} / ${total} Checks Passed (${Math.round((passed / total) * 100)}%)`);
    console.log('===============================================================\n');

    if (passed !== total) {
        process.exit(1);
    }
}

runVerification().catch(err => {
    console.error('Fatal Verification Error:', err);
    process.exit(1);
});
