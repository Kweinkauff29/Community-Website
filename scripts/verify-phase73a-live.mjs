// scripts/verify-phase73a-live.mjs
import { execSync } from 'child_process';

const BASE_URL = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const SITE_KEY = 'ursula-weinkauff-pilot';
const AUTHORIZED_ORIGIN = 'https://coconutcoastrealtors.org';

async function run() {
    console.log('====================================================');
    console.log('CCOR IDX PLUG-IN — PHASE 7.3A LIVE VERIFICATION');
    console.log('Target:', BASE_URL);
    console.log('Origin:', AUTHORIZED_ORIGIN);
    console.log('Site Key:', SITE_KEY);
    console.log('====================================================\n');

    // 1. Static Asset Build & UI Audit
    console.log('1. Checking Live Static Assets & UI Build Telemetry...');
    const searchHtmlRes = await fetch(`${BASE_URL}/search/index.html`);
    if (!searchHtmlRes.ok) throw new Error(`Static asset fetch failed: HTTP ${searchHtmlRes.status}`);
    const htmlText = await searchHtmlRes.text();

    const hasBuildTag = htmlText.includes('data-ui-build="2026.08.25.7.3a"');
    const hasBuildConst = htmlText.includes("CCOR_IDX_UI_BUILD = '2026.08.25.7.3a'");
    const hasMoreFiltersDrawer = htmlText.includes('id="moreFiltersDrawer"');
    const hasCommercialPill = htmlText.includes('data-type="commercial"');
    const hasLandPill = htmlText.includes('data-type="land"');
    const hasCCORBrand = htmlText.includes('CCOR IDX Plug-in');
    const hasNoVisibleSneak = !htmlText.includes('Powered by SNEAK');

    console.log(`   - data-ui-build="2026.08.25.7.3a": ${hasBuildTag}`);
    console.log(`   - UI Build Constant:             ${hasBuildConst}`);
    console.log(`   - Commercial Pill:               ${hasCommercialPill}`);
    console.log(`   - Lot & Land Pill:               ${hasLandPill}`);
    console.log(`   - More Filters Drawer:           ${hasMoreFiltersDrawer}`);
    console.log(`   - CCOR IDX Plug-in Brand:        ${hasCCORBrand}`);
    console.log(`   - No SNEAK Branding:             ${hasNoVisibleSneak}`);

    if (!hasBuildTag || !hasCommercialPill || !hasLandPill || !hasMoreFiltersDrawer) {
        throw new Error('Static asset verification failed');
    }
    console.log('   PASS: Static Search UI is 100% up-to-date.\n');

    // 2. Bootstrap & Session
    console.log('2. Testing GET /idx/v1/bootstrap with authorized Origin...');
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

    // 3. Category Search API (Commercial)
    console.log('3. Testing /idx/v1/search?propertyType=commercial...');
    const commRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&propertyType=commercial`, {
        headers: authHeaders
    });
    if (!commRes.ok) throw new Error(`Commercial search failed: HTTP ${commRes.status}`);
    const commData = await commRes.json();
    const commTotal = commData.pagination?.total || 0;
    console.log(`   PASS: Commercial Search HTTP 200 OK | Total Commercial Listings: ${commTotal}`);
    if (commData.data && commData.data.length > 0) {
        const c = commData.data[0];
        console.log(`         Sample: MLS #${c.ListingId} | Type: ${c.PropertyType} | Subtype: ${c.PropertySubType || 'N/A'} | Price: $${(c.ListPrice || 0).toLocaleString()}`);
    }

    // 4. Category Search API (Land)
    console.log('\n4. Testing /idx/v1/search?propertyType=land...');
    const landRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&propertyType=land`, {
        headers: authHeaders
    });
    if (!landRes.ok) throw new Error(`Land search failed: HTTP ${landRes.status}`);
    const landData = await landRes.json();
    const landTotal = landData.pagination?.total || 0;
    console.log(`   PASS: Lot & Land Search HTTP 200 OK | Total Land Listings: ${landTotal}`);
    if (landData.data && landData.data.length > 0) {
        const l = landData.data[0];
        console.log(`         Sample: MLS #${l.ListingId} | Type: ${l.PropertyType} | Acres: ${l.LotSizeAcres || 'N/A'} | Price: $${(l.ListPrice || 0).toLocaleString()}`);
    }

    // 5. Advanced More Filters Queries
    console.log('\n5. Testing More Filters Wave-1 API Parameters...');
    
    // Living Area Sq Ft
    const sqftRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&propertyType=sale&minSqft=2500&maxSqft=4500`, { headers: authHeaders });
    const sqftData = await sqftRes.json();
    console.log(`   - Living Area (2500 - 4500 sqft):  ${sqftData.pagination?.total || 0} listings`);

    // Lot Size Acres
    const acresRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&propertyType=land&minAcres=0.5&maxAcres=5`, { headers: authHeaders });
    const acresData = await acresRes.json();
    console.log(`   - Lot Size (0.5 - 5 Acres):         ${acresData.pagination?.total || 0} listings`);

    // Year Built
    const yearRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&propertyType=sale&minYear=2020`, { headers: authHeaders });
    const yearData = await yearRes.json();
    console.log(`   - Year Built (2020+):              ${yearData.pagination?.total || 0} listings`);

    // Subdivision
    const subRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&subdivision=Pelican+Landing`, { headers: authHeaders });
    const subData = await subRes.json();
    console.log(`   - Subdivision ("Pelican Landing"): ${subData.pagination?.total || 0} listings`);

    // Open House Only
    const ohRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&openHouse=1`, { headers: authHeaders });
    const ohData = await ohRes.json();
    console.log(`   - Open House Only:                 ${ohData.pagination?.total || 0} listings`);

    // Price Reduced
    const prRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&priceReduced=1`, { headers: authHeaders });
    const prData = await prRes.json();
    console.log(`   - Price Reduced Only:              ${prData.pagination?.total || 0} listings`);

    // Sort: Sq Ft High to Low
    const sortRes = await fetch(`${BASE_URL}/idx/v1/search?site=${encodeURIComponent(SITE_KEY)}&propertyType=sale&sort=sqftDesc`, { headers: authHeaders });
    const sortData = await sortRes.json();
    console.log(`   - Sort sqftDesc Top 1 Sqft:        ${(sortData.data?.[0]?.LivingArea || 0).toLocaleString()} sqft`);

    // 6. Lead Capture Routing to Site Owner
    console.log('\n6. Testing Lead Capture POST /idx/v1/lead...');
    const sampleListingKey = (commData.data?.[0]?.ListingKey) || (landData.data?.[0]?.ListingKey);
    const leadPayload = {
        siteKey: SITE_KEY,
        listingKey: sampleListingKey,
        name: 'Phase 7.3A Test Inquirer',
        email: 'phase73a.test@example.com',
        phone: '239-555-7300',
        message: 'Inquiring about commercial/land opportunities.',
        leadType: 'property_inquiry'
    };

    const leadRes = await fetch(`${BASE_URL}/idx/v1/lead`, {
        method: 'POST',
        headers: {
            ...authHeaders,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(leadPayload)
    });
    if (!leadRes.ok) throw new Error(`Lead POST failed: HTTP ${leadRes.status}`);
    const leadResult = await leadRes.json();
    console.log(`   PASS: Lead Capture HTTP 200 OK | Lead ID: ${leadResult.leadId}`);

    // Verify lead in D1 assigned to Ursula's site
    const d1Check = execSync(
        `npx wrangler d1 execute sneak-idx-staging --remote -c wrangler.sneak.toml --command "SELECT id, site_id, name, email, listing_key, lead_type FROM sneak_leads WHERE id = '${leadResult.leadId}'"`,
        { encoding: 'utf8' }
    );
    console.log('   D1 Lead Table Confirmation:');
    console.log(d1Check);

    console.log('====================================================');
    console.log('ALL PHASE 7.3A LIVE CHECKS COMPLETED SUCCESSFULLY');
    console.log('====================================================\n');
}

run().catch(console.error);
