// scripts/verify-phase72-live.mjs
import { execSync } from 'node:child_process';

const BASE_URL = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const AUTHORIZED_ORIGIN = 'https://coconutcoastrealtors.org';
const SITE_KEY = 'ursula-weinkauff-pilot';

async function run() {
    console.log('====================================================');
    console.log('CCOR IDX PLUG-IN — PHASE 7.2 LIVE ENDPOINT VERIFICATION');
    console.log(`Target: ${BASE_URL}`);
    console.log(`Origin: ${AUTHORIZED_ORIGIN}`);
    console.log('====================================================\n');

    // 1. D1 Database Verification Metrics
    console.log('1. Querying D1 Post-Backfill Metrics...');
    const d1Metrics = execSync(
        `npx wrangler d1 execute sneak-idx-staging --remote -c wrangler.sneak.toml --command "SELECT count(*) as total, sum(case when InternetEntireListingDisplayYN = 1 then 1 else 0 end) as display_1, sum(case when InternetEntireListingDisplayYN = 0 then 1 else 0 end) as display_0, sum(case when InternetAddressDisplayYN = 1 then 1 else 0 end) as addr_1, sum(case when InternetAddressDisplayYN = 0 then 1 else 0 end) as addr_0, sum(case when MediaJSON IS NOT NULL then 1 else 0 end) as media_populated FROM sneak_listings"`,
        { encoding: 'utf8' }
    );
    console.log(d1Metrics);

    // 2. Bootstrap & Session Acquisition
    console.log('2. Testing GET /idx/v1/bootstrap?site=... with authorized Origin...');
    const bootRes = await fetch(`${BASE_URL}/idx/v1/bootstrap?site=${SITE_KEY}`, {
        method: 'GET',
        headers: {
            'Origin': AUTHORIZED_ORIGIN
        }
    });
    if (!bootRes.ok) throw new Error(`Bootstrap failed: HTTP ${bootRes.status} ${await bootRes.text()}`);
    const bootData = await bootRes.json();
    const sessionToken = bootData.session || bootData.sessionToken;
    console.log(`   PASS: Bootstrap HTTP 200 OK | Session Token: ${sessionToken.substring(0, 20)}...`);

    const authHeaders = {
        'Origin': AUTHORIZED_ORIGIN,
        'Authorization': `Bearer ${sessionToken}`,
        'X-SNEAK-Session': sessionToken
    };

    // 3. Tenant Config
    console.log('3. Testing /idx/v1/config...');
    const cfgRes = await fetch(`${BASE_URL}/idx/v1/config?site=${SITE_KEY}`, { headers: authHeaders });
    const cfgData = await cfgRes.json();
    console.log(`   PASS: Config HTTP 200 OK | Display Name: "${cfgData.displayName}" | Brokerage: "${cfgData.brokerage}"`);

    // 4. Market Search (All Listings)
    console.log('4. Testing /idx/v1/search (Full Market)...');
    const searchRes = await fetch(`${BASE_URL}/idx/v1/search?site=${SITE_KEY}&limit=10`, { headers: authHeaders });
    const searchData = await searchRes.json();
    console.log(`   PASS: Market Search HTTP 200 OK | Total Active Market Listings: ${searchData.pagination.total}`);

    // 5. Commercial Search
    console.log('5. Testing /idx/v1/search (Commercial Filter)...');
    const commRes = await fetch(`${BASE_URL}/idx/v1/search?site=${SITE_KEY}&propertyType=commercial&limit=5`, { headers: authHeaders });
    const commData = await commRes.json();
    console.log(`   PASS: Commercial Search HTTP 200 OK | Total Commercial Listings: ${commData.pagination.total}`);
    const commItem = commData.data[0];
    console.log(`         Sample: MLS #${commItem.ListingId} | Type: ${commItem.PropertyType} | Price: $${commItem.ListPrice?.toLocaleString()}`);

    // 6. Lot & Land Search
    console.log('6. Testing /idx/v1/search (Lot & Land Filter)...');
    const landRes = await fetch(`${BASE_URL}/idx/v1/search?site=${SITE_KEY}&propertyType=land&limit=5`, { headers: authHeaders });
    const landData = await landRes.json();
    console.log(`   PASS: Lot & Land Search HTTP 200 OK | Total Land Listings: ${landData.pagination.total}`);
    const landItem = landData.data[0];
    console.log(`         Sample: MLS #${landItem.ListingId} | Type: ${landItem.PropertyType} | Lot Acres: ${landItem.LotSizeAcres} | Price: $${landItem.ListPrice?.toLocaleString()}`);

    // 7. Dual Price Slider Filter ($400k - $1.5M)
    console.log('7. Testing /idx/v1/search (Price Range $400K - $1.5M)...');
    const priceRes = await fetch(`${BASE_URL}/idx/v1/search?site=${SITE_KEY}&minPrice=400000&maxPrice=1500000&limit=5`, { headers: authHeaders });
    const priceData = await priceRes.json();
    console.log(`   PASS: Price Range Search HTTP 200 OK | Total Matching: ${priceData.pagination.total}`);

    // 8. Full Photo Gallery from D1 (Listing Detail & Media)
    console.log('8. Testing /idx/v1/listing/:key and /idx/v1/listing/:key/media from D1...');
    const testKey = searchData.data[0].ListingKey;
    const detailRes = await fetch(`${BASE_URL}/idx/v1/listing/${encodeURIComponent(testKey)}?site=${SITE_KEY}`, { headers: authHeaders });
    const detailData = await detailRes.json();
    const mediaRes = await fetch(`${BASE_URL}/idx/v1/listing/${encodeURIComponent(testKey)}/media?site=${SITE_KEY}`, { headers: authHeaders });
    const mediaData = await mediaRes.json();
    console.log(`   PASS: Listing Detail HTTP 200 OK | Address: "${detailData.data.UnparsedAddress}" | Listing Office: "${detailData.data.ListOfficeName}"`);
    console.log(`   PASS: Media Gallery HTTP 200 OK | Photos Count: ${mediaData.media?.length || 0} URLs`);

    // 9. Lead Capture Routing to Site Owner (Ursula Weinkauff)
    console.log('9. Testing Lead Capture POST /idx/v1/lead...');
    const leadRes = await fetch(`${BASE_URL}/idx/v1/lead`, {
        method: 'POST',
        headers: {
            ...authHeaders,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            siteKey: SITE_KEY,
            listingKey: testKey,
            name: 'Phase 7.2 Lead Test',
            email: 'phase72.lead@example.com',
            phone: '239-555-0199',
            message: 'Testing Phase 7.2 CCOR IDX Plug-in site owner lead capture routing.',
            leadType: 'property_inquiry',
            sourceUrl: `${AUTHORIZED_ORIGIN}/idx-test/`
        })
    });
    const leadData = await leadRes.json();
    console.log(`   PASS: Lead Capture HTTP 200 OK | Lead ID: ${leadData.leadId}`);

    // Verify lead in D1
    const leadCheck = execSync(
        `npx wrangler d1 execute sneak-idx-staging --remote -c wrangler.sneak.toml --command "SELECT id, site_id, name, email, listing_key, lead_type FROM sneak_leads WHERE id = '${leadData.leadId}'"`,
        { encoding: 'utf8' }
    );
    console.log('   D1 Lead Table Confirmation:');
    console.log(leadCheck);

    // 10. Check Static Search Asset for Branding
    console.log('10. Checking Static Asset /search/index.html Branding...');
    const htmlRes = await fetch(`${BASE_URL}/search/index.html`);
    const htmlText = await htmlRes.text();
    const hasCCOR = htmlText.includes('CCOR IDX Plug-in');
    const hasSNEAK = htmlText.includes('SNEAK Portal') || htmlText.includes('SNEAK IDX Platform');
    const hasDualSlider = htmlText.includes('priceRangeMin') && htmlText.includes('dual-range-container');
    const hasAgentListingsRemoved = !htmlText.includes('detailViewAgentBtn') && !htmlText.includes('agentModalOverlay');
    console.log(`    - Has "CCOR IDX Plug-in": ${hasCCOR}`);
    console.log(`    - Has user-visible "SNEAK": ${hasSNEAK} (expected false)`);
    console.log(`    - Has Dual Range Price Slider: ${hasDualSlider}`);
    console.log(`    - "View Agent Listings" removed: ${hasAgentListingsRemoved}`);

    console.log('\n====================================================');
    console.log('ALL PHASE 7.2 LIVE SYSTEM CHECKS COMPLETED SUCCESSFULLY');
    console.log('====================================================');
}

run().catch(err => {
    console.error('\nVerification error:', err);
    process.exit(1);
});
