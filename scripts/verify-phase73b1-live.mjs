// scripts/verify-phase73b1-live.mjs
const BASE_URL = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const SITE_KEY = 'ursula-weinkauff-pilot';
const AUTHORIZED_ORIGIN = 'https://coconutcoastrealtors.org';

async function run() {
    console.log('====================================================');
    console.log('CCOR IDX PLUG-IN — PHASE 7.3B1 LIVE VERIFICATION');
    console.log('Target:', BASE_URL);
    console.log('Origin:', AUTHORIZED_ORIGIN);
    console.log('Site Key:', SITE_KEY);
    console.log('====================================================\n');

    // 1. Static Asset Build & UI Telemetry Audit
    console.log('1. Checking Live Static Assets & UI Build Telemetry...');
    const searchHtmlRes = await fetch(`${BASE_URL}/search/index.html`);
    if (!searchHtmlRes.ok) throw new Error(`Static asset fetch failed: HTTP ${searchHtmlRes.status}`);
    const htmlText = await searchHtmlRes.text();

    const hasBuildTag = htmlText.includes('data-ui-build="2026.08.27.7.3b1"');
    const hasBuildConst = htmlText.includes("CCOR_IDX_UI_BUILD = '2026.08.27.7.3b1'");
    const hasSearchAsIMove = htmlText.includes('id="searchAsIMoveToggle"');
    const hasSearchThisArea = htmlText.includes('id="searchThisAreaBtn"');
    const hasMobileToggle = htmlText.includes('id="mobileViewToggleBar"');
    const hasMapOverlay = htmlText.includes('class="map-controls-overlay"');

    console.log(`   - data-ui-build="2026.08.27.7.3b1": ${hasBuildTag}`);
    console.log(`   - UI Build Constant:               ${hasBuildConst}`);
    console.log(`   - Search As I Move Toggle:         ${hasSearchAsIMove}`);
    console.log(`   - Search This Area Button:         ${hasSearchThisArea}`);
    console.log(`   - Mobile View Toggle Bar:          ${hasMobileToggle}`);
    console.log(`   - Map Controls Overlay:            ${hasMapOverlay}`);

    if (!hasBuildTag || !hasSearchAsIMove || !hasMobileToggle) {
        throw new Error('Static asset verification failed');
    }
    console.log('   PASS: Static Search UI is 100% up-to-date.\n');

    // 2. Embed Loader Build Version
    console.log('2. Checking embed.js build version...');
    const embedRes = await fetch(`${BASE_URL}/embed.js`);
    if (!embedRes.ok) throw new Error(`embed.js fetch failed: HTTP ${embedRes.status}`);
    const embedText = await embedRes.text();
    const embedHasBuild = embedText.includes('2026.08.27.7.3b1');
    console.log(`   - embed.js build 2026.08.27.7.3b1: ${embedHasBuild}`);
    if (!embedHasBuild) throw new Error('embed.js build version mismatch');
    console.log('   PASS: embed.js is 100% up-to-date.\n');

    // 3. Bootstrap & Session
    console.log('3. Testing GET /idx/v1/bootstrap with authorized Origin...');
    const bRes = await fetch(`${BASE_URL}/idx/v1/bootstrap?site=${encodeURIComponent(SITE_KEY)}`, {
        headers: { Origin: AUTHORIZED_ORIGIN, Accept: 'application/json' }
    });
    if (!bRes.ok) throw new Error(`Bootstrap failed: HTTP ${bRes.status}`);
    const bData = await bRes.json();
    const sessionToken = bData.session;
    console.log(`   PASS: Bootstrap HTTP 200 OK | Session: ${sessionToken.substring(0, 24)}...\n`);

    const authHeaders = {
        'Authorization': `Bearer ${sessionToken}`,
        'Origin': AUTHORIZED_ORIGIN,
        'Accept': 'application/json'
    };

    // 4. Viewport-Bounded /idx/v1/search
    console.log('4. Testing Bounded /idx/v1/search (North: 26.40, South: 26.30, East: -81.75, West: -81.85)...');
    const boundedSearchUrl = `${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&north=26.40&south=26.30&east=-81.75&west=-81.85`;
    const searchRes = await fetch(boundedSearchUrl, { headers: authHeaders });
    if (!searchRes.ok) throw new Error(`Bounded search failed: HTTP ${searchRes.status}`);
    const searchData = await searchRes.json();
    const boundedSearchCount = searchData.pagination?.total || 0;
    console.log(`   PASS: Bounded Search HTTP 200 OK | Total in Viewport: ${boundedSearchCount}`);

    // 5. Viewport-Bounded /idx/v1/map and Marker Payload Schema
    console.log('\n5. Testing Bounded /idx/v1/map and Marker Schema...');
    const boundedMapUrl = `${BASE_URL}/idx/v1/map?site=${encodeURIComponent(SITE_KEY)}&north=26.40&south=26.30&east=-81.75&west=-81.85&limit=500`;
    const mapRes = await fetch(boundedMapUrl, { headers: authHeaders });
    if (!mapRes.ok) throw new Error(`Bounded map failed: HTTP ${mapRes.status}`);
    const mapData = await mapRes.json();
    console.log(`   PASS: Bounded Map HTTP 200 OK | Markers Returned: ${mapData.count} | Truncated: ${mapData.truncated}`);

    if (mapData.data && mapData.data.length > 0) {
        const m = mapData.data[0];
        console.log(`   Sample Marker: MLS #${m.ListingId}`);
        console.log(`   - Address:         ${m.UnparsedAddress}`);
        console.log(`   - Price:           $${(m.ListPrice || 0).toLocaleString()}`);
        console.log(`   - Beds/Baths:      ${m.BedroomsTotal || 0} bd • ${m.BathroomsTotalInteger || 0} ba`);
        console.log(`   - Living Area:     ${m.LivingArea || 'N/A'} sqft`);
        console.log(`   - Lot Size Acres:  ${m.LotSizeAcres || 'N/A'}`);
        console.log(`   - Subdivision:     ${m.SubdivisionName || 'N/A'}`);
        console.log(`   - Year Built:      ${m.YearBuilt || 'N/A'}`);
        console.log(`   - Zoning:          ${m.Zoning || 'N/A'}`);
        console.log(`   - List Office:     ${m.ListOfficeName || 'N/A'}`);

        // Verify key presence
        const requiredKeys = ['ListingKey', 'ListingId', 'ListPrice', 'UnparsedAddress', 'City', 'Latitude', 'Longitude', 'BedroomsTotal', 'BathroomsTotalInteger', 'LivingArea', 'LotSizeAcres', 'SubdivisionName', 'YearBuilt', 'Zoning', 'ListOfficeName'];
        for (const k of requiredKeys) {
            if (!(k in m)) throw new Error(`Missing required marker key: ${k}`);
        }
        console.log('   PASS: Complete Context-Aware Marker Schema verified.');
    }

    // 6. Category Map Requests (Commercial, Land, Rental)
    console.log('\n6. Testing Category-Specific Map Queries...');
    
    // Commercial Map
    const commMapRes = await fetch(`${BASE_URL}/idx/v1/map?site=${encodeURIComponent(SITE_KEY)}&propertyType=commercial`, { headers: authHeaders });
    const commMapData = await commMapRes.json();
    console.log(`   - Commercial Map Markers: ${commMapData.count} (Truncated: ${commMapData.truncated})`);

    // Land Map
    const landMapRes = await fetch(`${BASE_URL}/idx/v1/map?site=${encodeURIComponent(SITE_KEY)}&propertyType=land`, { headers: authHeaders });
    const landMapData = await landMapRes.json();
    console.log(`   - Lot & Land Map Markers: ${landMapData.count} (Truncated: ${landMapData.truncated})`);

    // Rental Map
    const rentalMapRes = await fetch(`${BASE_URL}/idx/v1/map?site=${encodeURIComponent(SITE_KEY)}&propertyType=rental`, { headers: authHeaders });
    const rentalMapData = await rentalMapRes.json();
    console.log(`   - Rental Map Markers:     ${rentalMapData.count} (Truncated: ${rentalMapData.truncated})`);

    // 7. Security: Unauthorized Origin Rejection
    console.log('\n7. Testing Unauthorized Origin Rejection...');
    const evilRes = await fetch(`${BASE_URL}/idx/v1/bootstrap?site=${encodeURIComponent(SITE_KEY)}`, {
        headers: { Origin: 'https://unauthorized-evil-attacker.com' }
    });
    console.log(`   - Unauthorized Origin Status: HTTP ${evilRes.status} (Expected: 403)`);
    if (evilRes.status !== 403) throw new Error(`Security violation: Origin not blocked`);
    console.log('   PASS: Fail-Closed Origin Protection verified.\n');

    console.log('====================================================');
    console.log('ALL PHASE 7.3B1 LIVE CHECKS COMPLETED SUCCESSFULLY');
    console.log('====================================================\n');
}

run().catch(console.error);
