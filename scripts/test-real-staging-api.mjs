/**
 * scripts/test-real-staging-api.mjs
 * 
 * Comprehensive automated verification of real MLS data on sneak-idx-worker-staging.
 * Tests bootstrap, session tokens, search, map, detail, media, property types,
 * agent scoping, office scoping, and scope violation rejection.
 */

const STAGING_WORKER_URL = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const ORIGIN = 'http://localhost:8090';

async function request(path, options = {}) {
    const url = `${STAGING_WORKER_URL}${path}`;
    const headers = {
        'Origin': ORIGIN,
        'Accept': 'application/json',
        ...(options.headers || {})
    };
    const res = await fetch(url, { ...options, headers });
    let body = null;
    try { body = await res.json(); } catch { body = await res.text(); }
    return { status: res.status, headers: res.headers, body };
}

async function runTests() {
    console.log('====================================================');
    console.log('SNEAK IDX — REAL MLS STAGING API & SCOPE VERIFICATION');
    console.log(`Worker: ${STAGING_WORKER_URL}`);
    console.log('====================================================\n');

    let passed = 0;
    let failed = 0;

    function assert(cond, name, details = '') {
        if (cond) {
            console.log(`  [PASS] ${name}`);
            passed++;
        } else {
            console.error(`  [FAIL] ${name} — ${details}`);
            failed++;
        }
    }

    // 1. MARKET SCOPE BOOTSTRAP (demo-ccor)
    console.log('[1] Testing Market Scope Bootstrap (demo-ccor)...');
    const bootMarket = await request('/idx/v1/bootstrap?site=demo-ccor');
    assert(bootMarket.status === 200, 'Market bootstrap returns 200 OK');
    const marketSession = typeof bootMarket.body?.session === 'string' ? bootMarket.body.session : bootMarket.body?.session?.token;
    assert(Boolean(marketSession), 'Market session token returned');

    // 2. REAL SEARCH (Market Scope)
    console.log('\n[2] Testing Real Search (Market Scope)...');
    const searchMarket = await request('/idx/v1/search?site=demo-ccor&limit=10', {
        headers: { 'X-Sneak-Session': marketSession }
    });
    assert(searchMarket.status === 200, 'Market search returns 200 OK');
    const marketListings = searchMarket.body?.data || [];
    assert(marketListings.length > 0, `Search returned real listings (count: ${marketListings.length}, total: ${searchMarket.body?.pagination?.total})`);
    
    const sampleRealListing = marketListings[0];
    assert(Boolean(sampleRealListing?.ListingKey), `Sample listing has ListingKey: ${sampleRealListing?.ListingKey}`);
    assert(Boolean(sampleRealListing?.City), `Sample listing has City: ${sampleRealListing?.City}`);
    assert(Boolean(sampleRealListing?.ListPrice), `Sample listing has ListPrice: $${sampleRealListing?.ListPrice?.toLocaleString()}`);

    // 3. REAL MAP (Market Scope)
    console.log('\n[3] Testing Real Map (Market Scope)...');
    const mapMarket = await request('/idx/v1/map?site=demo-ccor&bounds=25.8,-82.2,26.8,-81.4', {
        headers: { 'X-Sneak-Session': marketSession }
    });
    assert(mapMarket.status === 200, 'Map query returns 200 OK');
    const mapPins = mapMarket.body?.data || [];
    assert(mapPins.length > 0, `Map returned real spatial pins (count: ${mapPins.length})`);
    if (mapPins.length > 0) {
        assert(typeof mapPins[0].Latitude === 'number' && typeof mapPins[0].Longitude === 'number', 'Map pins have valid numeric coordinates');
    }

    // 4. SEARCH PROPERTY TYPES
    console.log('\n[4] Testing Search Property Types Filtering...');
    // Sale (Residential)
    const saleRes = await request('/idx/v1/search?site=demo-ccor&propertyType=sale&limit=10', { headers: { 'X-Sneak-Session': marketSession } });
    const saleListings = saleRes.body?.data || [];
    const nonSaleTypes = saleListings.filter(l => l.PropertyType === 'Land' || l.PropertyType === 'Residential Lease');
    assert(saleRes.status === 200 && saleListings.length > 0 && nonSaleTypes.length === 0, `propertyType=sale returns Sale listings (count: ${saleListings.length}, total: ${saleRes.body?.pagination?.total})`);

    // Rental (Residential Lease)
    const rentalRes = await request('/idx/v1/search?site=demo-ccor&propertyType=rental&limit=10', { headers: { 'X-Sneak-Session': marketSession } });
    const rentalListings = rentalRes.body?.data || [];
    const onlyLease = rentalListings.every(l => l.PropertyType === 'Residential Lease');
    assert(rentalRes.status === 200 && rentalListings.length > 0 && onlyLease, `propertyType=rental returns Residential Lease (count: ${rentalListings.length}, total: ${rentalRes.body?.pagination?.total})`);

    // Land
    const landRes = await request('/idx/v1/search?site=demo-ccor&propertyType=land&limit=10', { headers: { 'X-Sneak-Session': marketSession } });
    const landListings = landRes.body?.data || [];
    const onlyLand = landListings.every(l => l.PropertyType === 'Land');
    assert(landRes.status === 200 && landListings.length > 0 && onlyLand, `propertyType=land returns Land (count: ${landListings.length}, total: ${landRes.body?.pagination?.total})`);

    // Commercial
    const commRes = await request('/idx/v1/search?site=demo-ccor&propertyType=commercial&limit=10', { headers: { 'X-Sneak-Session': marketSession } });
    const commListings = commRes.body?.data || [];
    const onlyComm = commListings.every(l => l.PropertyType === 'Commercial Sale' || l.PropertyType?.includes('Commercial'));
    assert(commRes.status === 200 && commListings.length > 0 && onlyComm, `propertyType=commercial returns Commercial Sale (count: ${commListings.length}, total: ${commRes.body?.pagination?.total})`);

    // 5. REAL DETAIL ENDPOINT (D1 alone without remote BRIDGE_TOKEN)
    console.log('\n[5] Testing Real Listing Detail Endpoint (/idx/v1/listing/:key)...');
    const detailRes = await request(`/idx/v1/listing/${sampleRealListing.ListingKey}?site=demo-ccor`, {
        headers: { 'X-Sneak-Session': marketSession }
    });
    assert(detailRes.status === 200, 'Listing detail returns 200 OK from D1');
    const d = detailRes.body?.data;
    assert(Boolean(d?.UnparsedAddress || (d?.StreetNumber && d?.StreetName)), 'Detail provides address');
    assert(Boolean(d?.ListPrice), 'Detail provides price');
    assert(Boolean(d?.StandardStatus), 'Detail provides StandardStatus');
    assert(Boolean(d?.ListAgentFullName || d?.ListAgentKey), 'Detail provides listing agent');
    assert(Boolean(d?.ListOfficeName || d?.ListOfficeKey), 'Detail provides listing office');
    assert(Boolean(d?.PrimaryPhoto), 'Detail provides primary photo from D1');

    // 6. REAL MEDIA ENDPOINT
    console.log('\n[6] Testing Real Listing Media Endpoint (/idx/v1/listing/:key/media)...');
    const mediaRes = await request(`/idx/v1/listing/${sampleRealListing.ListingKey}/media?site=demo-ccor`, {
        headers: { 'X-Sneak-Session': marketSession }
    });
    assert(mediaRes.status === 200, 'Listing media endpoint returns 200 OK');
    const mediaList = mediaRes.body?.media || mediaRes.body?.data || [];
    assert(mediaList.length > 0, `Media list returns ${mediaList.length} items (D1 primary photo fallback)`);

    // 7. AGENT SCOPE VERIFICATION (scope-test-agent -> ListAgentMlsId = 'B3650316')
    console.log('\n[7] Testing Agent Scope (scope-test-agent -> B3650316)...');
    const bootAgent = await request('/idx/v1/bootstrap?site=scope-test-agent');
    assert(bootAgent.status === 200, 'Agent site bootstrap returns 200 OK');
    const agentSession = typeof bootAgent.body?.session === 'string' ? bootAgent.body.session : bootAgent.body?.session?.token;

    const agentSearch = await request('/idx/v1/search?site=scope-test-agent&limit=20', {
        headers: { 'X-Sneak-Session': agentSession }
    });
    assert(agentSearch.status === 200, 'Agent scoped search returns 200 OK');
    const agentListings = agentSearch.body?.data || [];
    const allAgentMatch = agentListings.length > 0 && agentListings.every(l => l.ListAgentMlsId === 'B3650316');
    assert(allAgentMatch, `Agent search returns ONLY agent's listings (count: ${agentListings.length}/${agentSearch.body?.pagination?.total})`);

    const agentListingKey = agentListings[0]?.ListingKey;
    const agentDetail = await request(`/idx/v1/listing/${agentListingKey}?site=scope-test-agent`, {
        headers: { 'X-Sneak-Session': agentSession }
    });
    assert(agentDetail.status === 200, 'Detail for in-scope agent listing returns 200 OK');

    // Agent Scope Escape Test (requesting another agent's listing with agent session)
    const otherListingKey = sampleRealListing.ListingKey !== agentListingKey ? sampleRealListing.ListingKey : marketListings[1]?.ListingKey;
    const agentEscapeDetail = await request(`/idx/v1/listing/${otherListingKey}?site=scope-test-agent`, {
        headers: { 'X-Sneak-Session': agentSession }
    });
    assert(agentEscapeDetail.status === 404 || agentEscapeDetail.status === 403, `Agent scope escape blocked on detail (HTTP ${agentEscapeDetail.status})`);

    const agentEscapeMedia = await request(`/idx/v1/listing/${otherListingKey}/media?site=scope-test-agent`, {
        headers: { 'X-Sneak-Session': agentSession }
    });
    assert(agentEscapeMedia.status === 404 || agentEscapeMedia.status === 403, `Agent scope escape blocked on media (HTTP ${agentEscapeMedia.status})`);

    // Agent endpoint requesting outside agent MLS ID
    const agentEndpointEscape = await request(`/idx/v1/agent/ANOTHER_AGENT/listings?site=scope-test-agent`, {
        headers: { 'X-Sneak-Session': agentSession }
    });
    assert(agentEndpointEscape.status === 403, `Agent endpoint requesting outside MLS ID blocked (HTTP ${agentEndpointEscape.status})`);

    // 8. OFFICE SCOPE VERIFICATION (scope-test-office -> ListOfficeMlsId = 'BPRI')
    console.log('\n[8] Testing Office Scope (scope-test-office -> BPRI)...');
    const bootOffice = await request('/idx/v1/bootstrap?site=scope-test-office');
    assert(bootOffice.status === 200, 'Office site bootstrap returns 200 OK');
    const officeSession = typeof bootOffice.body?.session === 'string' ? bootOffice.body.session : bootOffice.body?.session?.token;

    const officeSearch = await request('/idx/v1/search?site=scope-test-office&limit=20', {
        headers: { 'X-Sneak-Session': officeSession }
    });
    assert(officeSearch.status === 200, 'Office scoped search returns 200 OK');
    const officeListings = officeSearch.body?.data || [];
    const allOfficeMatch = officeListings.length > 0 && officeListings.every(l => l.ListOfficeMlsId === 'BPRI');
    assert(allOfficeMatch, `Office search returns ONLY office's listings (count: ${officeListings.length}/${officeSearch.body?.pagination?.total})`);

    const officeListingKey = officeListings[0]?.ListingKey;
    const officeDetail = await request(`/idx/v1/listing/${officeListingKey}?site=scope-test-office`, {
        headers: { 'X-Sneak-Session': officeSession }
    });
    assert(officeDetail.status === 200, 'Detail for in-scope office listing returns 200 OK');

    // Office Scope Escape Test (requesting listing from another office)
    const nonOfficeListing = marketListings.find(l => l.ListOfficeMlsId !== 'BPRI');
    if (nonOfficeListing) {
        const officeEscapeDetail = await request(`/idx/v1/listing/${nonOfficeListing.ListingKey}?site=scope-test-office`, {
            headers: { 'X-Sneak-Session': officeSession }
        });
        assert(officeEscapeDetail.status === 404 || officeEscapeDetail.status === 403, `Office scope escape blocked on detail (HTTP ${officeEscapeDetail.status})`);

        const officeEscapeMedia = await request(`/idx/v1/listing/${nonOfficeListing.ListingKey}/media?site=scope-test-office`, {
            headers: { 'X-Sneak-Session': officeSession }
        });
        assert(officeEscapeMedia.status === 404 || officeEscapeMedia.status === 403, `Office scope escape blocked on media (HTTP ${officeEscapeMedia.status})`);
    }

    // 9. OPEN HOUSE API & SCOPE VERIFICATION
    console.log('\n[9] Testing Open Houses API & Tenant Scoping (/idx/v1/open-houses)...');
    // Market Scope Open Houses
    const marketOH = await request('/idx/v1/open-houses?site=demo-ccor', {
        headers: { 'X-Sneak-Session': marketSession }
    });
    assert(marketOH.status === 200, 'Market open houses returns 200 OK');
    const marketOHList = marketOH.body?.data || [];
    assert(marketOHList.length > 0, `Market open houses returned real events (count: ${marketOHList.length})`);
    if (marketOHList.length > 0) {
        assert(Boolean(marketOHList[0].openHouse?.openHouseKey), 'Open house payload contains openHouseKey');
        assert(Boolean(marketOHList[0].property?.City), 'Open house payload contains joined property details');
    }

    // Agent Scope Open Houses
    const agentOH = await request('/idx/v1/open-houses?site=scope-test-agent', {
        headers: { 'X-Sneak-Session': agentSession }
    });
    assert(agentOH.status === 200, 'Agent open houses returns 200 OK');
    const agentOHList = agentOH.body?.data || [];
    const allAgentOHMatch = agentOHList.every(item => item.property?.ListAgentMlsId === 'B3650316');
    assert(allAgentOHMatch, `Agent open houses returned only in-scope events (current count: ${agentOHList.length})`);

    // Office Scope Open Houses
    const officeOH = await request('/idx/v1/open-houses?site=scope-test-office', {
        headers: { 'X-Sneak-Session': officeSession }
    });
    assert(officeOH.status === 200, 'Office open houses returns 200 OK');
    const officeOHList = officeOH.body?.data || [];
    const allOfficeOHMatch = officeOHList.every(item => item.property?.ListOfficeMlsId === 'BPRI');
    assert(allOfficeOHMatch, `Office open houses returned only in-scope events (current count: ${officeOHList.length})`);

    console.log('\n====================================================');
    console.log(`REAL MLS STAGING ACCEPTANCE RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log('====================================================');

    if (failed > 0) process.exit(1);
}

runTests().catch(err => {
    console.error('Test execution error:', err);
    process.exit(1);
});
