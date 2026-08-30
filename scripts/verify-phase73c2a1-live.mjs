/**
 * scripts/verify-phase73c2a1-live.mjs
 * 
 * Live Staging Verification Script for SNEAK IDX Phase 7.3C2A.1 Hotfix.
 * 
 * Checks:
 * 1. Alert Worker Live Version & Health Check (/api/alerts/version)
 * 2. Alert Worker Unsubscribe Route Error Handling (/api/alerts/unsubscribe)
 * 3. Consumer Worker Live Version & Health Check (/api/consumer/version)
 * 4. SNEAK IDX Search App Build & Script (/embed.js, /search/index.html)
 * 5. Live Pilot Page availability (coconutcoastrealtors.org/idx-test/)
 */

import assert from 'node:assert/strict';

const EXPECTED_BUILD = '2026.08.30.7.3c2a1';
const ALERTS_WORKER = 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev';
const CONSUMER_WORKER = 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
const IDX_WORKER = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const PILOT_URL = 'https://coconutcoastrealtors.org/idx-test/';

async function runVerification() {
    console.log('===============================================================');
    console.log(` SNEAK IDX Phase 7.3C2A.1 Live Staging Verification`);
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
    await check('1. Alert Worker /api/alerts/version returns expected build', async () => {
        const res = await fetch(`${ALERTS_WORKER}/api/alerts/version`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const data = await res.json();
        assert.equal(data.service, 'sneak-alerts-worker');
        assert.equal(data.build, EXPECTED_BUILD, `Expected ${EXPECTED_BUILD}, got ${data.build}`);
        assert.equal(data.status, 'healthy');
        console.log(`        Build: ${data.build} | emailProviderConfigured: ${data.emailProviderConfigured} | signingSecretConfigured: ${data.signingSecretConfigured} | deliveryReady: ${data.deliveryReady}`);
    });

    // 2. Alert Worker Unsubscribe Endpoint
    await check('2. Alert Worker /api/alerts/unsubscribe missing token returns 400 Bad Request', async () => {
        const res = await fetch(`${ALERTS_WORKER}/api/alerts/unsubscribe`);
        assert.equal(res.status, 400);
        const html = await res.text();
        assert.ok(html.includes('This unsubscribe link is invalid or incomplete.'));
    });

    await check('3. Alert Worker /api/alerts/unsubscribe malformed token returns 400 or 503 if secret unconfigured', async () => {
        const res = await fetch(`${ALERTS_WORKER}/api/alerts/unsubscribe?token=malformed-token`);
        assert.ok([400, 503].includes(res.status), `Expected 400 or 503, got ${res.status}`);
        const html = await res.text();
        assert.ok(html.includes('This unsubscribe link is invalid or incomplete.') || html.includes('Service temporarily unavailable.'));
    });

    // 3. Consumer Worker Version & Health
    await check('4. Consumer Worker /api/consumer/version returns expected build', async () => {
        const res = await fetch(`${CONSUMER_WORKER}/api/consumer/version`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const data = await res.json();
        assert.equal(data.service, 'sneak-consumer-worker');
        assert.equal(data.build, EXPECTED_BUILD, `Expected ${EXPECTED_BUILD}, got ${data.build}`);
        assert.equal(data.status, 'healthy');
    });

    // 4. Embed Loader & Search HTML
    await check('5. Embed loader /embed.js serves live and contains expected build version', async () => {
        const res = await fetch(`${IDX_WORKER}/embed.js`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const text = await res.text();
        assert.ok(text.includes(`const buildVersion = '${EXPECTED_BUILD}'`), `embed.js missing build ${EXPECTED_BUILD}`);
        assert.ok(text.includes('ccor_listing'), 'embed.js missing ccor_listing deep link support');
    });

    await check('6. Search app /search/index.html serves live and has data-ui-build attribute', async () => {
        const res = await fetch(`${IDX_WORKER}/search/index.html`);
        assert.equal(res.status, 200, `Expected 200, got ${res.status}`);
        const html = await res.text();
        assert.ok(html.includes(`data-ui-build="${EXPECTED_BUILD}"`), `search/index.html missing data-ui-build="${EXPECTED_BUILD}"`);
        assert.ok(html.includes(`CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`), `search/index.html missing CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`);
    });

    // 5. Live Pilot Host
    await check('7. Live pilot host coconutcoastrealtors.org/idx-test/ is reachable (HTTP 200)', async () => {
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
