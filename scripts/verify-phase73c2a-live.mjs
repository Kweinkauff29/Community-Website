/**
 * scripts/verify-phase73c2a-live.mjs
 * 
 * Live Verification Script for CCOR IDX / SNEAK IDX Phase 7.3C2A:
 * - Alert Worker health, service name, build version, and provider status
 * - Consumer Worker health, build version, and alert route security
 * - Serving Worker embed.js & search/ UI build version and deep-link logic
 * - Live public search, radius search, and polygon search
 * - Unsubscribe endpoint malformed token security
 * - Live pilot test page responsiveness
 */

const ALERTS_WORKER_URL = 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev';
const CONSUMER_WORKER_URL = 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
const SERVING_WORKER_URL = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const PILOT_URL = 'https://coconutcoastrealtors.org/idx-test/';
const EXPECTED_BUILD = '2026.08.28.7.3c2a';

async function runVerification() {
    console.log('======================================================');
    console.log('  SNEAK IDX PHASE 7.3C2A LIVE STAGING VERIFICATION');
    console.log('======================================================\n');

    let passed = 0;
    let failed = 0;

    function assertCheck(name, condition, details = '') {
        if (condition) {
            console.log(`  ✅ PASS: ${name}`);
            passed++;
        } else {
            console.error(`  ❌ FAIL: ${name} ${details ? '(' + details + ')' : ''}`);
            failed++;
        }
    }

    try {
        // 1. Alert Worker Health & Version
        const alertVerRes = await fetch(`${ALERTS_WORKER_URL}/api/alerts/version`);
        assertCheck('Alert Worker /api/alerts/version responds 200', alertVerRes.status === 200);
        if (alertVerRes.ok) {
            const data = await alertVerRes.json();
            assertCheck('Alert Worker service is sneak-alerts-worker', data.service === 'sneak-alerts-worker');
            assertCheck(`Alert Worker build is ${EXPECTED_BUILD}`, data.build === EXPECTED_BUILD, `got: ${data.build}`);
            assertCheck('Alert Worker status is healthy', data.status === 'healthy');
        }

        // 2. Alert Worker Unsubscribe with Malformed Token returns 400
        const unsubRes = await fetch(`${ALERTS_WORKER_URL}/api/alerts/unsubscribe?token=malformed_token_123`);
        assertCheck('Alert Worker unsubscribe with malformed token responds 400 Bad Request', unsubRes.status === 400);
        const unsubHtml = await unsubRes.text();
        assertCheck('Alert Worker unsubscribe renders clean HTML error page', unsubHtml.includes('Invalid Link'));

        // 3. Consumer Worker Health & Version
        const consumerVerRes = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/version`);
        assertCheck('Consumer Worker /api/consumer/version responds 200', consumerVerRes.status === 200);
        if (consumerVerRes.ok) {
            const data = await consumerVerRes.json();
            assertCheck('Consumer Worker service is sneak-consumer-worker', data.service === 'sneak-consumer-worker');
            assertCheck(`Consumer Worker build is ${EXPECTED_BUILD}`, data.build === EXPECTED_BUILD, `got: ${data.build}`);
            assertCheck('Consumer Worker status is healthy', data.status === 'healthy');
        }

        // 4. Consumer Worker Alert Preferences Endpoint Security (Unauthenticated 401)
        const alertPrefRes = await fetch(`${CONSUMER_WORKER_URL}/api/consumer/saved-searches/test_id/alert?site=ursula-weinkauff-pilot`);
        assertCheck('Consumer Worker alert preferences route is protected (401 Unauthorized)', alertPrefRes.status === 401);

        // 5. Serving Worker embed.js
        const embedRes = await fetch(`${SERVING_WORKER_URL}/embed.js`);
        assertCheck('Serving Worker /embed.js responds 200', embedRes.status === 200);
        const embedJs = await embedRes.text();
        assertCheck(`embed.js contains build ${EXPECTED_BUILD}`, embedJs.includes(`const buildVersion = '${EXPECTED_BUILD}'`));
        assertCheck('embed.js contains ccor_listing deep link forwarding', embedJs.includes('ccor_listing'));

        // 6. Serving Worker search/index.html
        const searchRes = await fetch(`${SERVING_WORKER_URL}/search/`);
        assertCheck('Serving Worker /search/ responds 200', searchRes.status === 200);
        const searchHtml = await searchRes.text();
        assertCheck(`search/index.html contains CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`, searchHtml.includes(`CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`));
        assertCheck(`search/index.html contains data-ui-build="${EXPECTED_BUILD}"`, searchHtml.includes(`data-ui-build="${EXPECTED_BUILD}"`));
        assertCheck('search/index.html contains updateSavedSearchAlert function', searchHtml.includes('updateSavedSearchAlert'));
        assertCheck('search/index.html contains initialDeepListingKey parser', searchHtml.includes('initialDeepListingKey'));

        // 7. Live Pilot Page
        const pilotRes = await fetch(PILOT_URL, {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
        });
        assertCheck(`Live pilot page ${PILOT_URL} responds 200`, pilotRes.status === 200);

        // 8. Live Bootstrap & Spatial Search
        const bootRes = await fetch(`${SERVING_WORKER_URL}/idx/v1/bootstrap?site=ursula-weinkauff-pilot`, {
            headers: { 'Origin': 'https://coconutcoastrealtors.org' }
        });
        assertCheck('Bootstrap for pilot site returns 200 and signed session token', bootRes.status === 200);
        if (bootRes.ok) {
            const bootData = await bootRes.json();
            const session = bootData.session;

            // Live Radius Search
            const radRes = await fetch(`${SERVING_WORKER_URL}/idx/v1/search?site=ursula-weinkauff-pilot&centerLat=26.34&centerLng=-81.78&radiusMiles=5&limit=5`, {
                headers: { 'Authorization': `Bearer ${session}`, 'X-SNEAK-Session': session }
            });
            assertCheck('Live Radius Search responds 200 OK', radRes.status === 200);
            if (radRes.ok) {
                const radData = await radRes.json();
                assertCheck('Live Radius Search returns results array', Array.isArray(radData.data) && radData.data.length > 0, `count: ${radData.total}`);
            }

            // Live Polygon Search
            const polygon = {
                type: 'Polygon',
                coordinates: [[
                    [-81.85, 26.30],
                    [-81.70, 26.30],
                    [-81.70, 26.40],
                    [-81.85, 26.40],
                    [-81.85, 26.30]
                ]]
            };
            const polyRes = await fetch(`${SERVING_WORKER_URL}/idx/v1/search?site=ursula-weinkauff-pilot&polygon=${encodeURIComponent(JSON.stringify(polygon))}&limit=5`, {
                headers: { 'Authorization': `Bearer ${session}`, 'X-SNEAK-Session': session }
            });
            assertCheck('Live Polygon Search responds 200 OK', polyRes.status === 200);
            if (polyRes.ok) {
                const polyData = await polyRes.json();
                assertCheck('Live Polygon Search returns results array', Array.isArray(polyData.data) && polyData.data.length > 0, `count: ${polyData.total}`);
            }
        }

    } catch (err) {
        console.error('  ❌ EXCEPTION during verification:', err);
        failed++;
    }

    console.log('\n------------------------------------------------------');
    console.log(`  TOTAL: ${passed + failed} | PASSED: ${passed} | FAILED: ${failed}`);
    console.log('======================================================\n');

    if (failed > 0) {
        process.exit(1);
    }
}

runVerification();
