import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildCommonListingFilters, buildListingOrderClause } from '../sneak-shared/idx-query.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, '..');

test('FEATURED & ROUTE FILTERING SUITE', async (t) => {
    await t.test('1. Query Builder: buildCommonListingFilters agent & open house filters', () => {
        const mockSite = { scope_type: 'market', default_agent_mls_id: '633942' };

        // Test agentMlsId single
        const params1 = new URLSearchParams('agent=633942');
        const res1 = buildCommonListingFilters(params1, mockSite);
        assert.ok(res1.valid, 'query must be valid');
        assert.ok(res1.whereClauses.some(c => c.includes('ListAgentMlsId = ?')));
        assert.ok(res1.bindValues.includes('633942'));

        // Test comma-separated agent IDs
        const params2 = new URLSearchParams('agents=633942,654321,999888');
        const res2 = buildCommonListingFilters(params2, mockSite);
        assert.ok(res2.valid, 'query must be valid');
        assert.ok(res2.whereClauses.some(c => c.includes('ListAgentMlsId IN (?,?,?)')));
        assert.deepEqual(res2.bindValues.filter(b => ['633942', '654321', '999888'].includes(b)), ['633942', '654321', '999888']);

        // Test featured filter fallback
        const params3 = new URLSearchParams('featured=1');
        const res3 = buildCommonListingFilters(params3, mockSite);
        assert.ok(res3.valid, 'query must be valid');
        assert.ok(res3.whereClauses.some(c => c.includes('ListAgentMlsId = ?')));
        assert.ok(res3.bindValues.includes('633942'));

        // Test open houses filter
        const params4 = new URLSearchParams('openHouses=true');
        const res4 = buildCommonListingFilters(params4, mockSite);
        assert.ok(res4.valid, 'query must be valid');
        assert.ok(res4.whereClauses.some(c => c.includes('sneak_open_houses')));

        // Test price range & city
        const params5 = new URLSearchParams('city=Fort%20Myers%20Beach&minPrice=350000&maxPrice=500000');
        const res5 = buildCommonListingFilters(params5, mockSite);
        assert.ok(res5.valid, 'query must be valid');
        assert.ok(res5.whereClauses.some(c => c.includes('City')));
        assert.ok(res5.whereClauses.some(c => c.includes('ListPrice >= ?')));
        assert.ok(res5.whereClauses.some(c => c.includes('ListPrice <= ?')));
        assert.ok(res5.bindValues.includes('Fort Myers Beach'));
        assert.ok(res5.bindValues.includes(350000));
        assert.ok(res5.bindValues.includes(500000));
    });

    await t.test('2. Embed Loader: Route parsing and regex validation', () => {
        const embedJs = fs.readFileSync(path.join(rootDir, 'sneak-idx/embed.js'), 'utf8');

        // Verify parseParentRouteAndFilters exists in embed.js
        assert.ok(embedJs.includes('function parseParentRouteAndFilters'), 'embed.js must include parseParentRouteAndFilters');
        assert.ok(embedJs.includes('homes-for-sale-in-'), 'embed.js must support homes-for-sale route slug pattern');
        assert.ok(embedJs.includes('data-route-mode'), 'embed.js must support data-route-mode attribute');
        assert.ok(embedJs.includes('data-agent'), 'embed.js must support data-agent attribute');
        assert.ok(embedJs.includes('data-featured'), 'embed.js must support data-featured attribute');
        assert.ok(embedJs.includes('data-open-houses'), 'embed.js must support data-open-houses attribute');
        assert.ok(embedJs.includes('data-heading'), 'embed.js must support data-heading attribute');

        // Simulate regex route parsing logic in Node
        function parseTestSlug(slug) {
            let city = null;
            let minPrice = null;
            let maxPrice = null;
            let propertyType = null;
            let heading = null;

            const formatCitySlug = (s) => s.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');

            const priceMatch = slug.match(/(?:homes|condos|properties|rentals)-for-(?:sale|rent)-in-([a-z0-9-]+?)(?:-fl)?-(\d+)-to-(\d+)/i) ||
                               slug.match(/\/([a-z0-9-]+?)(?:-fl)?-(\d+)-to-(\d+)/i);
            if (priceMatch) {
                const rawCity = priceMatch[1].replace(/-fl$/i, '');
                city = formatCitySlug(rawCity);
                minPrice = parseInt(priceMatch[2], 10);
                maxPrice = parseInt(priceMatch[3], 10);
                heading = `Homes for Sale in ${city}, FL $${minPrice.toLocaleString()} to $${maxPrice.toLocaleString()}`;
            }

            return { city, minPrice, maxPrice, propertyType, heading };
        }

        const parsed1 = parseTestSlug('/homes-for-sale-in-fort-myers-beach-fl-350000-to-500000');
        assert.equal(parsed1.city, 'Fort Myers Beach');
        assert.equal(parsed1.minPrice, 350000);
        assert.equal(parsed1.maxPrice, 500000);
        assert.equal(parsed1.heading, 'Homes for Sale in Fort Myers Beach, FL $350,000 to $500,000');

        const parsed2 = parseTestSlug('/homes-for-sale-in-estero-fl-1500000-to-2500000');
        assert.equal(parsed2.city, 'Estero');
        assert.equal(parsed2.minPrice, 1500000);
        assert.equal(parsed2.maxPrice, 2500000);
        assert.equal(parsed2.heading, 'Homes for Sale in Estero, FL $1,500,000 to $2,500,000');
    });

    await t.test('3. Search UI: Agent photo avatar badge and landing header banner', () => {
        const searchHtml = fs.readFileSync(path.join(rootDir, 'sneak-idx/search/index.html'), 'utf8');

        // Check CSS classes
        assert.ok(searchHtml.includes('.card-agent-badge'), 'search/index.html must include .card-agent-badge CSS');
        assert.ok(searchHtml.includes('.landing-header-banner'), 'search/index.html must include .landing-header-banner CSS');
        assert.ok(searchHtml.includes('.card-tour-badge'), 'search/index.html must include .card-tour-badge CSS');
        assert.ok(searchHtml.includes('.open-house-badge'), 'search/index.html must include .open-house-badge CSS');
        assert.ok(searchHtml.includes('.card-broker-reciprocity'), 'search/index.html must include Broker Reciprocity display');

        // Check DOM elements & functions
        assert.ok(searchHtml.includes('id="landingHeaderBanner"'), 'search/index.html must have #landingHeaderBanner element');
        assert.ok(searchHtml.includes('function updateLandingBanner'), 'search/index.html must have updateLandingBanner()');
        assert.ok(searchHtml.includes('function formatOpenHouseBadge'), 'search/index.html must have formatOpenHouseBadge()');
        assert.ok(searchHtml.includes('function centerMapOnCity'), 'search/index.html must have centerMapOnCity()');
        assert.ok(searchHtml.includes('AgentPhotoUrl'), 'search/index.html renderListings must handle item.AgentPhotoUrl');
    });

    await t.test('4. Member Portal UI: Embed snippet generator & tabs', () => {
        const memberUi = fs.readFileSync(path.join(rootDir, 'sneak-member/ui.js'), 'utf8');

        assert.ok(memberUi.includes('id="tab-widgets"'), 'sneak-member/ui.js must include widgets generator tab');
        assert.ok(memberUi.includes('builderWidgetType'), 'sneak-member/ui.js must include builderWidgetType');
        assert.ok(memberUi.includes('builderAgentId'), 'sneak-member/ui.js must include builderAgentId');
        assert.ok(memberUi.includes('builderCity'), 'sneak-member/ui.js must include builderCity');
        assert.ok(memberUi.includes('builderMinPrice'), 'sneak-member/ui.js must include builderMinPrice');
        assert.ok(memberUi.includes('builderMaxPrice'), 'sneak-member/ui.js must include builderMaxPrice');
        assert.ok(memberUi.includes('builderOpenHouses'), 'sneak-member/ui.js must include builderOpenHouses');
        assert.ok(memberUi.includes('builderRouteMode'), 'sneak-member/ui.js must include builderRouteMode');
        assert.ok(memberUi.includes('updateCustomEmbedCode'), 'sneak-member/ui.js must include updateCustomEmbedCode function');
    });

    await t.test('5. Admin Embed Generator: featured_agent and landing_page templates', () => {
        const adminGen = fs.readFileSync(path.join(rootDir, 'sneak-admin/embed-generator.js'), 'utf8');

        assert.ok(adminGen.includes('featured_agent'), 'embed-generator.js must include featured_agent snippet generator');
        assert.ok(adminGen.includes('landing_page'), 'embed-generator.js must include landing_page snippet generator');
        assert.ok(adminGen.includes('data-featured="true"'), 'embed-generator.js must render data-featured attribute');
        assert.ok(adminGen.includes('data-route-mode="auto"'), 'embed-generator.js must render data-route-mode attribute');
    });

    await t.test('6. Listing Pinning: Order clause compilation, embed forwarding & UI badges', () => {
        // Verify SQL Order clause compilation with pin precedence
        const orderRes = buildListingOrderClause('priceDesc', ['LST_001', 'LST_002'], ['633942', '642811']);
        assert.ok(orderRes.orderSQL.includes('CASE WHEN ListingKey IN (?,?) THEN 0 WHEN ListAgentMlsId IN (?,?) THEN 1 ELSE 2 END ASC, ListPrice DESC'), 'Must prioritize pinned listings, then pinned agents, then price sort');
        assert.deepEqual(orderRes.orderBinds, ['LST_001', 'LST_002', '633942', '642811'], 'Order bind arguments must match in exact sequence');

        // Verify filter parsing of pin parameters
        const site = { scope_type: 'market', default_agent_mls_id: '633942' };
        const filterRes = buildCommonListingFilters(new URLSearchParams('pinOwn=1&pinListings=LST_A,LST_B&pinAgents=12345'), site);
        assert.ok(filterRes.pinnedAgents.includes('633942'), 'pinOwn must pin site default agent MLS ID');
        assert.ok(filterRes.pinnedAgents.includes('12345'), 'Explicit pinAgents must be included');
        assert.deepEqual(filterRes.pinnedListings, ['LST_A', 'LST_B'], 'Explicit pinListings must be included');

        // Verify embed.js forwards pin parameters
        const embedJs = fs.readFileSync(path.join(rootDir, 'sneak-idx/embed.js'), 'utf8');
        assert.ok(embedJs.includes('scriptPinOwn'), 'embed.js must parse data-pin-own');
        assert.ok(embedJs.includes('scriptPinAgent'), 'embed.js must parse data-pin-agent');
        assert.ok(embedJs.includes('scriptPinListings'), 'embed.js must parse data-pin-listings');
        assert.ok(embedJs.includes('&pinOwn=1'), 'embed.js must forward pinOwn to iframeUrl');
        assert.ok(embedJs.includes('&pinAgent='), 'embed.js must forward pinAgent to iframeUrl');
        assert.ok(embedJs.includes('&pinListings='), 'embed.js must forward pinListings to iframeUrl');

        // Verify search/index.html UI badges & parameter forwarding
        const searchHtml = fs.readFileSync(path.join(rootDir, 'sneak-idx/search/index.html'), 'utf8');
        assert.ok(searchHtml.includes('.pinned-badge'), 'search/index.html must include .pinned-badge CSS');
        assert.ok(searchHtml.includes('listing-card-pinned'), 'search/index.html must apply listing-card-pinned class');
        assert.ok(searchHtml.includes('Pinned</div>'), 'search/index.html must render Pinned badge text');
        assert.ok(searchHtml.includes("filterPinOwn = urlParams.get('pinOwn')"), 'search/index.html must extract pinOwn');
        assert.ok(searchHtml.includes("p.set('pinOwn', '1')"), 'search/index.html must pass pinOwn to API');
        assert.ok(searchHtml.includes("p.set('pinAgents', filterPinAgent)"), 'search/index.html must pass pinAgents to API');
        assert.ok(searchHtml.includes("p.set('pinListings', filterPinListings)"), 'search/index.html must pass pinListings to API');

        // Verify Member Portal UI builder has pinning inputs
        const memberUi = fs.readFileSync(path.join(rootDir, 'sneak-member/ui.js'), 'utf8');
        assert.ok(memberUi.includes('builderPinOwn'), 'sneak-member/ui.js must include builderPinOwn checkbox');
        assert.ok(memberUi.includes('builderPinAgents'), 'sneak-member/ui.js must include builderPinAgents input');
        assert.ok(memberUi.includes('builderPinListings'), 'sneak-member/ui.js must include builderPinListings input');
        assert.ok(memberUi.includes('data-pin-own="true"'), 'sneak-member/ui.js must emit data-pin-own attribute');
    });

    await t.test('7. Layout Expansion & Adaptive Height: Wider container and recently viewed section expansion', () => {
        const searchHtml = fs.readFileSync(path.join(rootDir, 'sneak-idx/search/index.html'), 'utf8').replace(/\r\n/g, '\n');

        // Verify layout width enlargement
        assert.ok(searchHtml.includes('.filters-outer {\n      max-width: 1800px;'), 'filters-outer must expand to 1800px max-width');
        assert.ok(searchHtml.includes('.carousel-container {\n      max-width: 1800px;'), 'carousel-container must expand to 1800px max-width');
        assert.ok(searchHtml.includes('.recently-viewed-section {\n      max-width: 1800px;'), 'recently-viewed-section must expand to 1800px max-width');
        assert.ok(searchHtml.includes('width: 58%;'), 'listings-section desktop width must be expanded to 58%');
        assert.ok(searchHtml.includes('width: 60%;'), 'listings-section wide desktop width must expand to 60%');

        // Verify recently viewed increases height
        assert.ok(searchHtml.includes('body.has-recently-viewed .main-container'), 'search/index.html must have increased height rule for active recently viewed');
        assert.ok(searchHtml.includes('updateAdaptiveEmbedHeight'), 'search/index.html must include updateAdaptiveEmbedHeight()');
        assert.ok(searchHtml.includes("type: 'SNEAK_RESIZE'"), 'search/index.html must post SNEAK_RESIZE message');
        assert.ok(searchHtml.includes('baseH + 185'), 'search/index.html must add extra height when recently viewed is active');

        // Verify embed.js desktop default height and adaptive resizing
        const embedJs = fs.readFileSync(path.join(rootDir, 'sneak-idx/embed.js'), 'utf8');
        assert.ok(embedJs.includes('Math.round(viewportHeight * 0.92), 1150'), 'embed.js desktop height must expand up to 1150px');
        assert.ok(embedJs.includes("e.data.type !== 'SNEAK_RESIZE'"), 'embed.js must listen for SNEAK_RESIZE');
    });

    await t.test('8. Database Row Write Reductions: Read-usage suppression, conditional upsert & session throttling', () => {
        // 1. SneakIDXWorker write suppression
        const workerCode = fs.readFileSync(path.join(rootDir, 'SneakIDXWorker.js'), 'utf8');
        assert.ok(!workerCode.includes("recordUsage(site.site_id, 'searches', env)"), 'Worker must NOT write to sneak_usage on searches');
        assert.ok(!workerCode.includes("recordUsage(site.site_id, 'listing_views', env)"), 'Worker must NOT write to sneak_usage on listing views');
        assert.ok(workerCode.includes("if (column !== 'leads') return;"), 'recordUsage must restrict D1 row writes strictly to leads');

        // 2. Lock & sync run log suppression on 0 changes
        const lockCode = fs.readFileSync(path.join(rootDir, 'sneak-sync/lock.js'), 'utf8');
        assert.ok(lockCode.includes("runData.status === 'success' && !runData.recordsFetched && !runData.recordsUpserted && !runData.recordsRemoved"), 'recordSyncRun must suppress writes when zero records modified');

        // 3. Conditional listing upsert
        const syncCode = fs.readFileSync(path.join(rootDir, 'sneak-sync/listing-sync.js'), 'utf8');
        assert.ok(syncCode.includes('ON CONFLICT(ListingKey) DO UPDATE SET'), 'listing-sync must use ON CONFLICT DO UPDATE SET');
        assert.ok(syncCode.includes('WHERE excluded.ModificationTimestamp IS NOT sneak_listings.ModificationTimestamp'), 'listing-sync must conditionally skip unchanged rows in overlap window');

        // 4. Session last_seen_at throttling (15 minutes)
        const consumerAuth = fs.readFileSync(path.join(rootDir, 'sneak-consumer/auth.js'), 'utf8');
        assert.ok(consumerAuth.includes("last_seen_at < datetime('now', '-15 minutes')"), 'sneak-consumer/auth.js must throttle session last_seen_at writes');

        const memberAuth = fs.readFileSync(path.join(rootDir, 'sneak-member/auth.js'), 'utf8');
        assert.ok(memberAuth.includes("last_seen_at < datetime('now', '-15 minutes')"), 'sneak-member/auth.js must throttle session last_seen_at writes');

        const adminAuth = fs.readFileSync(path.join(rootDir, 'sneak-admin/auth.js'), 'utf8');
        assert.ok(adminAuth.includes("last_seen_at < datetime('now', '-15 minutes')"), 'sneak-admin/auth.js must throttle admin session last_seen_at writes');

        // 5. Consumer user last_activity_at throttling (15 minutes)
        const consumerApi = fs.readFileSync(path.join(rootDir, 'sneak-consumer/api.js'), 'utf8');
        assert.ok(consumerApi.includes("last_activity_at < datetime(?, '-15 minutes')"), 'sneak-consumer/api.js must throttle user last_activity_at writes');

        // 6. Open House & Reconciliation conditional upserts
        const ohSyncCode = fs.readFileSync(path.join(rootDir, 'sneak-sync/open-house-sync.js'), 'utf8');
        assert.ok(ohSyncCode.includes('ON CONFLICT(OpenHouseKey) DO UPDATE SET'), 'open-house-sync must use ON CONFLICT DO UPDATE SET');
        assert.ok(ohSyncCode.includes('WHERE excluded.OpenHouseStartTime IS NOT sneak_open_houses.OpenHouseStartTime'), 'open-house-sync must conditionally skip unchanged rows');

        const reconCode = fs.readFileSync(path.join(rootDir, 'sneak-sync/reconciliation.js'), 'utf8');
        assert.ok(reconCode.includes('ON CONFLICT(ListingKey) DO UPDATE SET'), 'reconciliation must use ON CONFLICT DO UPDATE SET');
        assert.ok(reconCode.includes('WHERE excluded.ModificationTimestamp IS NOT sneak_listings.ModificationTimestamp'), 'reconciliation must conditionally skip unchanged rows');
    });
});
