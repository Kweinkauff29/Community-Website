/**
 * scripts/verify-phase73c1b1-live.mjs
 * 
 * AUTOMATED HTTP/API VERIFICATION for Phase 7.3C1B.1 HOTFIX:
 * - Serving Worker health & version (2026.08.28.7.3c1b1)
 * - Serving Worker remote embed.js build version (2026.08.28.7.3c1b1) & getRecommendedSearchHeight
 * - Serving Worker search/index.html app-shell flexbox rules, internal listings scrolling, and nested footer
 * - Consumer Worker health & Saved Searches endpoints
 * - Parameterized Search and Spatial API routes
 * - Live pilot responsiveness (https://coconutcoastrealtors.org/idx-test/)
 * 
 * NOTE: This script validates HTTP/API endpoints only.
 * Full visual acceptance requires browser verification across Desktop, Tablet, and Mobile.
 */

const CONSUMER_WORKER_URL = 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
const SERVING_WORKER_URL = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const PILOT_URL = 'https://coconutcoastrealtors.org/idx-test/';
const EXPECTED_BUILD = '2026.08.28.7.3c1b1';

async function runLiveVerification() {
    console.log(`\n======================================================`);
    console.log(`  SNEAK IDX PHASE 7.3C1B.1 AUTOMATED HTTP/API VERIFICATION`);
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

    // 1. Check Serving Worker Health
    try {
        const res = await fetch(`${SERVING_WORKER_URL}/idx/v1/health`);
        assert('Serving Worker /idx/v1/health responds 200', res.status === 200, `status: ${res.status}`);
        const data = await res.json();
        assert('Serving Worker service is sneak-idx-worker', data.service === 'sneak-idx-worker', `service: ${data.service}`);
        assert('Serving Worker status is ok', data.status === 'ok', `status: ${data.status}`);
    } catch (err) {
        assert('Serving Worker health endpoint reachable', false, err.message);
    }

    // 2. Check Serving Worker embed.js
    try {
        const res = await fetch(`${SERVING_WORKER_URL}/embed.js`);
        assert('Serving Worker /embed.js responds 200', res.status === 200, `status: ${res.status}`);
        const text = await res.text();
        assert(`embed.js contains build ${EXPECTED_BUILD}`, text.includes(EXPECTED_BUILD));
        assert('embed.js contains getRecommendedSearchHeight function', text.includes('getRecommendedSearchHeight'));
        assert('embed.js contains parent resize listener', text.includes("addEventListener('resize'"));
        assert('embed.js validates e.source === iframe.contentWindow', text.includes('e.source !== iframe.contentWindow'));
        assert('embed.js contains auth_code URL extraction', text.includes('auth_code'));
    } catch (err) {
        assert('Serving Worker /embed.js reachable', false, err.message);
    }

    // 3. Check Serving Worker search/index.html
    try {
        const res = await fetch(`${SERVING_WORKER_URL}/search/`);
        assert('Serving Worker /search/ responds 200', res.status === 200, `status: ${res.status}`);
        const html = await res.text();
        assert(`search/index.html contains CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`, html.includes(EXPECTED_BUILD));
        assert('search/index.html contains data-ui-build="2026.08.28.7.3c1b1"', html.includes(`data-ui-build="${EXPECTED_BUILD}"`));
        assert('search/index.html contains body.is-embedded flex shell', html.includes('body.is-embedded'));
        assert('search/index.html has footer nested inside listings-section', html.indexOf('class="listings-section"') < html.indexOf('class="footer-disclaimer"'));
        assert('search/index.html contains #saveSearchBtn', html.includes('id="saveSearchBtn"'));
        assert('search/index.html contains #saveSearchModal', html.includes('id="saveSearchModal"'));
        assert('search/index.html contains #savedSearchesModal', html.includes('id="savedSearchesModal"'));
        assert('search/index.html contains serializeSearchState function', html.includes('serializeSearchState'));
        assert('search/index.html contains applySearchState function', html.includes('applySearchState'));
    } catch (err) {
        assert('Serving Worker /search/ reachable', false, err.message);
    }

    // 4. Check Consumer Worker Version & CORS
    try {
        const res = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/version`);
        assert('Consumer Worker /api/consumer/version responds 200', res.status === 200, `status: ${res.status}`);
        const data = await res.json();
        assert('Consumer Worker service is sneak-consumer-worker', data.service === 'sneak-consumer-worker', `service: ${data.service}`);
        assert('Consumer Worker status is healthy', data.status === 'healthy', `status: ${data.status}`);
    } catch (err) {
        assert('Consumer Worker /api/consumer/version reachable', false, err.message);
    }

    // 5. Check Saved Searches OPTIONS & Authentication Gate
    try {
        const optRes = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/saved-searches`, {
            method: 'OPTIONS',
            headers: { 'Origin': 'https://coconutcoastrealtors.org' }
        });
        assert('Saved searches OPTIONS responds 204 or 200', optRes.status === 204 || optRes.status === 200, `status: ${optRes.status}`);
        const allowMethods = optRes.headers.get('Access-Control-Allow-Methods') || '';
        assert('Access-Control-Allow-Methods includes PATCH, POST, GET, DELETE', allowMethods.includes('PATCH') && allowMethods.includes('DELETE') && allowMethods.includes('POST'), `methods: ${allowMethods}`);

        // Unauthenticated request must return 401 Unauthorized
        const unauthRes = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/saved-searches?site=ursula-weinkauff-pilot`);
        assert('Unauthenticated saved searches GET returns 401', unauthRes.status === 401, `status: ${unauthRes.status}`);
    } catch (err) {
        assert('Consumer Worker saved-searches reachable', false, err.message);
    }

    // 6. Check Tenant Bootstrap & Search API
    try {
        const bRes = await fetch(`${SERVING_WORKER_URL}/idx/v1/bootstrap?site=ursula-weinkauff-pilot`, {
            headers: { 'Origin': 'https://coconutcoastrealtors.org' }
        });
        assert('Bootstrap API responds 200 for ursula-weinkauff-pilot', bRes.status === 200, `status: ${bRes.status}`);
        const bData = await bRes.json();
        assert('Bootstrap returns session token', Boolean(bData.session), 'session present');

        if (bData.session) {
            const sRes = await fetch(`${SERVING_WORKER_URL}/idx/v1/search?site=ursula-weinkauff-pilot&session=${encodeURIComponent(bData.session)}&limit=10`, {
                headers: { 'Origin': 'https://coconutcoastrealtors.org' }
            });
            assert('Search API responds 200 with session', sRes.status === 200, `status: ${sRes.status}`);
            const sData = await sRes.json();
            assert('Search API returns listings array', Array.isArray(sData.data), `listings: ${sData.data?.length}`);
        }
    } catch (err) {
        assert('Serving Worker Bootstrap & Search API reachable', false, err.message);
    }

    // 7. Check Live Pilot Page
    try {
        const res = await fetch(PILOT_URL);
        assert('Live pilot page https://coconutcoastrealtors.org/idx-test/ responds 200', res.status === 200, `status: ${res.status}`);
        const html = await res.text();
        assert('Live pilot embeds sneak-idx embed loader script', html.includes('sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/embed.js'));
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
