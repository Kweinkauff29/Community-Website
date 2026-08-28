/**
 * scripts/verify-phase73c1b-live.mjs
 * 
 * Live Verification Script for Phase 7.3C1B:
 * - Consumer Worker health & version (2026.08.28.7.3c1b)
 * - Saved Searches CRUD endpoints (PATCH, OPTIONS, unauthenticated rejection)
 * - Serving Worker remote embed.js build version (2026.08.28.7.3c1b) & clamp(780px, 85vh, 960px)
 * - Serving Worker search/index.html app-shell flexbox rules & Saved Searches modals
 * - Live pilot responsiveness (https://coconutcoastrealtors.org/idx-test/)
 */

const CONSUMER_WORKER_URL = 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
const SERVING_WORKER_URL = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const PILOT_URL = 'https://coconutcoastrealtors.org/idx-test/';
const EXPECTED_BUILD = '2026.08.28.7.3c1b';

async function runLiveVerification() {
    console.log(`\n======================================================`);
    console.log(`  SNEAK IDX PHASE 7.3C1B LIVE STAGING VERIFICATION`);
    console.log(`======================================================\n`);

    let passed = 0;
    let failed = 0;

    function assert(desc, condition, details = '') {
        if (condition) {
            console.log(`  ✅ PASS: ${desc}`);
            passed++;
        } else {
            console.error(`  ❌ FAIL: ${desc} ${details ? `(${details})` : ''}`);
            failed++;
        }
    }

    // 1. Check Consumer Worker Version
    try {
        const res = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/version`);
        assert('Consumer Worker /api/consumer/version responds 200', res.status === 200, `status: ${res.status}`);
        const data = await res.json();
        assert('Consumer Worker service is sneak-consumer-worker', data.service === 'sneak-consumer-worker', `service: ${data.service}`);
        assert(`Consumer Worker build is ${EXPECTED_BUILD}`, data.build === EXPECTED_BUILD, `build: ${data.build}`);
        assert('Consumer Worker status is healthy', data.status === 'healthy', `status: ${data.status}`);
    } catch (err) {
        assert('Consumer Worker /api/consumer/version reachable', false, err.message);
    }

    // 2. Check Magic Link Anti-Enumeration & CORS
    try {
        const res = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/auth/magic-link`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Origin': 'https://coconutcoastrealtors.org'
            },
            body: JSON.stringify({
                site: 'demo-ccor',
                email: 'test_verify_phase73c1b@example.com',
                returnUrl: 'https://coconutcoastrealtors.org/idx-test/'
            })
        });
        assert('Consumer Worker magic-link responds 200', res.status === 200, `status: ${res.status}`);
        const data = await res.json();
        assert('Consumer Worker magic-link returns success: true', data.success === true, JSON.stringify(data));
        assert('Consumer Worker magic-link returns anti-enumeration message', typeof data.message === 'string' && data.message.includes('sign-in link has been sent'), `msg: ${data.message}`);
        assert('Consumer Worker returns CORS header Access-Control-Allow-Origin', res.headers.get('Access-Control-Allow-Origin') === 'https://coconutcoastrealtors.org', `CORS: ${res.headers.get('Access-Control-Allow-Origin')}`);
    } catch (err) {
        assert('Consumer Worker magic-link endpoint reachable', false, err.message);
    }

    // 3. Check Saved Searches OPTIONS & Authentication Gate
    try {
        const optRes = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/saved-searches`, {
            method: 'OPTIONS',
            headers: { 'Origin': 'https://coconutcoastrealtors.org' }
        });
        assert('Saved searches OPTIONS responds 204 or 200', optRes.status === 204 || optRes.status === 200, `status: ${optRes.status}`);
        const allowMethods = optRes.headers.get('Access-Control-Allow-Methods') || '';
        assert('Access-Control-Allow-Methods includes PATCH, POST, GET, DELETE', allowMethods.includes('PATCH') && allowMethods.includes('DELETE') && allowMethods.includes('POST'), `methods: ${allowMethods}`);

        // Unauthenticated request must return 401 Unauthorized
        const unauthRes = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/saved-searches?site=demo-ccor`);
        assert('Unauthenticated saved searches GET returns 401', unauthRes.status === 401, `status: ${unauthRes.status}`);
    } catch (err) {
        assert('Consumer Worker saved-searches reachable', false, err.message);
    }

    // 4. Check Serving Worker embed.js
    try {
        const res = await fetch(`${SERVING_WORKER_URL}/embed.js`);
        assert('Serving Worker /embed.js responds 200', res.status === 200, `status: ${res.status}`);
        const text = await res.text();
        assert(`embed.js contains build ${EXPECTED_BUILD}`, text.includes(EXPECTED_BUILD));
        assert('embed.js contains responsive desktop clamp(780px, 85vh, 960px)', text.includes('clamp(780px, 85vh, 960px)'));
        assert('embed.js validates e.source === iframe.contentWindow', text.includes('e.source !== iframe.contentWindow'));
        assert('embed.js contains auth_code URL extraction', text.includes('auth_code'));
    } catch (err) {
        assert('Serving Worker /embed.js reachable', false, err.message);
    }

    // 5. Check Serving Worker search/index.html
    try {
        const res = await fetch(`${SERVING_WORKER_URL}/search/`);
        assert('Serving Worker /search/ responds 200', res.status === 200, `status: ${res.status}`);
        const html = await res.text();
        assert(`search/index.html contains CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`, html.includes(EXPECTED_BUILD));
        assert('search/index.html contains body.is-embedded flex shell', html.includes('body.is-embedded'));
        assert('search/index.html contains #saveSearchBtn', html.includes('id="saveSearchBtn"'));
        assert('search/index.html contains #saveSearchModal', html.includes('id="saveSearchModal"'));
        assert('search/index.html contains #savedSearchesModal', html.includes('id="savedSearchesModal"'));
        assert('search/index.html contains serializeSearchState function', html.includes('serializeSearchState'));
        assert('search/index.html contains applySearchState function', html.includes('applySearchState'));
    } catch (err) {
        assert('Serving Worker /search/ reachable', false, err.message);
    }

    // 6. Check Live Pilot Page
    try {
        const res = await fetch(PILOT_URL);
        assert('Live pilot page https://coconutcoastrealtors.org/idx-test/ responds 200', res.status === 200, `status: ${res.status}`);
        const html = await res.text();
        assert('Live pilot embeds sneak-idx embed loader script', html.includes('sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/embed.js') || html.includes('data-sneak-idx'));
    } catch (err) {
        assert('Live pilot reachable', false, err.message);
    }

    console.log(`\n======================================================`);
    console.log(`  VERIFICATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log(`======================================================\n`);

    if (failed > 0) {
        process.exit(1);
    }
}

runLiveVerification();
