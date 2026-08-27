/**
 * SNEAK IDX Phase 7.3B2B Live Staging Verification Script
 * Validates deployed staging worker and static assets for Server-Authoritative Polygon Search, Drawing State, and Hardened Spatial Validation.
 */

const STAGING_ORIGIN = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const SITE_KEY = 'ursula-weinkauff-pilot';
const EXPECTED_BUILD = '2026.08.27.7.3b2b';

// Known polygon in SW Florida (around Bonita Springs / Estero)
const SW_FL_POLYGON = {
    type: "Polygon",
    coordinates: [
        [
            [-81.84, 26.37],
            [-81.76, 26.38],
            [-81.74, 26.31],
            [-81.82, 26.29],
            [-81.84, 26.37]
        ]
    ]
};

async function runLiveVerification() {
    console.log(`\n======================================================`);
    console.log(`CCOR IDX PLUG-IN — PHASE 7.3B2B LIVE STAGING VERIFICATION`);
    console.log(`Target: ${STAGING_ORIGIN}`);
    console.log(`Expected UI Build: ${EXPECTED_BUILD}`);
    console.log(`======================================================\n`);

    let passed = 0;
    let failed = 0;

    function assert(condition, message) {
        if (condition) {
            console.log(`  [PASS] ${message}`);
            passed++;
        } else {
            console.error(`  [FAIL] ${message}`);
            failed++;
        }
    }

    try {
        // 1. Static Asset Validation: embed.js
        console.log(`\n--- 1. Validating embed.js build version ---`);
        const embedRes = await fetch(`${STAGING_ORIGIN}/embed.js`);
        assert(embedRes.ok, `embed.js returned HTTP ${embedRes.status}`);
        const embedText = await embedRes.text();
        assert(embedText.includes(`const buildVersion = '${EXPECTED_BUILD}'`), `embed.js contains buildVersion = '${EXPECTED_BUILD}'`);

        // 2. Static Asset Validation: search/index.html
        console.log(`\n--- 2. Validating search/index.html build version and UI markup ---`);
        const searchRes = await fetch(`${STAGING_ORIGIN}/search/index.html`);
        assert(searchRes.ok, `search/index.html returned HTTP ${searchRes.status}`);
        const searchText = await searchRes.text();
        assert(searchText.includes(`data-ui-build="${EXPECTED_BUILD}"`), `search/index.html contains data-ui-build="${EXPECTED_BUILD}"`);
        assert(searchText.includes(`CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`), `search/index.html contains CCOR_IDX_UI_BUILD = '${EXPECTED_BUILD}'`);
        assert(searchText.includes(`id="drawToolBtn"`), `search/index.html contains #drawToolBtn`);
        assert(searchText.includes(`id="drawControlsBanner"`), `search/index.html contains #drawControlsBanner`);
        assert(searchText.includes(`id="radiusToolBtn"`), `search/index.html contains #radiusToolBtn`);
        assert(searchText.includes(`id="nearMeToolBtn"`), `search/index.html contains #nearMeToolBtn`);
        assert(searchText.includes(`id="activeSpatialPill"`), `search/index.html contains #activeSpatialPill`);

        // 3. Live API Bootstrap
        console.log(`\n--- 3. Testing live /idx/v1/bootstrap ---`);
        const bRes = await fetch(`${STAGING_ORIGIN}/idx/v1/bootstrap?site=${SITE_KEY}`, {
            headers: { 'Origin': 'https://coconutcoastrealtors.org' }
        });
        assert(bRes.ok, `/idx/v1/bootstrap returned HTTP ${bRes.status}`);
        const bData = await bRes.json();
        assert(bData.success === true && Boolean(bData.session), `Bootstrap returned valid session token`);
        const session = bData.session;

        // 4. Live API Polygon Search
        console.log(`\n--- 4. Testing live /idx/v1/search with Polygon Mode ---`);
        const polyParam = encodeURIComponent(JSON.stringify(SW_FL_POLYGON));
        const sRes = await fetch(`${STAGING_ORIGIN}/idx/v1/search?site=${SITE_KEY}&session=${session}&polygon=${polyParam}`);
        assert(sRes.ok, `/idx/v1/search with polygon returned HTTP ${sRes.status}`);
        const sData = await sRes.json();
        assert(Array.isArray(sData.data), `/idx/v1/search returned data array (${sData.data.length} listings)`);
        assert(Boolean(sData.pagination && typeof sData.pagination.total === 'number'), `/idx/v1/search returned exact pagination total (${sData.pagination?.total})`);

        // 5. Live API Polygon Map
        console.log(`\n--- 5. Testing live /idx/v1/map with Polygon Mode ---`);
        const mRes = await fetch(`${STAGING_ORIGIN}/idx/v1/map?site=${SITE_KEY}&session=${session}&polygon=${polyParam}`);
        assert(mRes.ok, `/idx/v1/map with polygon returned HTTP ${mRes.status}`);
        const mData = await mRes.json();
        assert(Array.isArray(mData.data), `/idx/v1/map returned data array (${mData.data.length} markers)`);

        // 6. Live API Categories with Polygon
        console.log(`\n--- 6. Testing live /idx/v1/search across categories with Polygon ---`);
        for (const cat of ['sale', 'rental', 'commercial', 'land']) {
            const catRes = await fetch(`${STAGING_ORIGIN}/idx/v1/search?site=${SITE_KEY}&session=${session}&propertyType=${cat}&polygon=${polyParam}`);
            assert(catRes.ok, `/idx/v1/search (${cat}, polygon) returned HTTP ${catRes.status}`);
            const catData = await catRes.json();
            assert(Array.isArray(catData.data), `Category ${cat} returned valid data array`);
        }

        // 7. Live Malformed / Invalid Polygon Validation
        console.log(`\n--- 7. Testing live malformed polygon validation (HTTP 400 InvalidSpatialFilter) ---`);
        const invPolyRes = await fetch(`${STAGING_ORIGIN}/idx/v1/search?site=${SITE_KEY}&session=${session}&polygon=not-json`);
        assert(invPolyRes.status === 400, `/idx/v1/search with malformed JSON returned HTTP 400`);
        const invPolyData = await invPolyRes.json();
        assert(invPolyData.error === 'InvalidSpatialFilter', `Returned error code InvalidSpatialFilter`);

        // 8. Live Hardened Radius Validation (Incomplete Radius parameters return HTTP 400)
        console.log(`\n--- 8. Testing live hardened radius validation (HTTP 400 InvalidSpatialFilter) ---`);
        const incRadRes = await fetch(`${STAGING_ORIGIN}/idx/v1/search?site=${SITE_KEY}&session=${session}&centerLat=26.34`);
        assert(incRadRes.status === 400, `/idx/v1/search with incomplete radius returned HTTP 400`);
        const incRadData = await incRadRes.json();
        assert(incRadData.error === 'InvalidSpatialFilter', `Returned error code InvalidSpatialFilter`);

        // 9. Live Regression: Radius Mode
        console.log(`\n--- 9. Testing live Radius Search regression ---`);
        const radRes = await fetch(`${STAGING_ORIGIN}/idx/v1/search?site=${SITE_KEY}&session=${session}&centerLat=26.34&centerLng=-81.78&radiusMiles=5`);
        assert(radRes.ok, `/idx/v1/search (5 mi radius) returned HTTP ${radRes.status}`);

        // 10. Live Regression: Viewport Bounding Box Mode
        console.log(`\n--- 10. Testing live Viewport Search regression ---`);
        const vpRes = await fetch(`${STAGING_ORIGIN}/idx/v1/search?site=${SITE_KEY}&session=${session}&north=26.4&south=26.3&east=-81.7&west=-81.8`);
        assert(vpRes.ok, `/idx/v1/search (viewport) returned HTTP ${vpRes.status}`);

    } catch (err) {
        console.error(`\nUnexpected error during live verification:`, err);
        failed++;
    }

    console.log(`\n======================================================`);
    console.log(`PHASE 7.3B2B LIVE VERIFICATION SUMMARY: ${passed} PASSED, ${failed} FAILED`);
    console.log(`======================================================\n`);

    if (failed > 0) {
        process.exit(1);
    }
}

runLiveVerification();
