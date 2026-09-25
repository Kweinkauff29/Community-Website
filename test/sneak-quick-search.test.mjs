import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildCommonListingFilters } from '../sneak-shared/idx-query.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, '..');

test('SNEAK QUICK SEARCH & LOCATION ROUTING SUITE', async (t) => {
    const mockSite = { scope_type: 'market', default_agent_mls_id: '633942' };

    await t.test('1. Query Engine: Flexible Location, Subdivision, Price, Bed and Bath filters', () => {
        // Test location=estero
        const params1 = new URLSearchParams('location=estero');
        const res1 = buildCommonListingFilters(params1, mockSite);
        assert.ok(res1.valid, 'query must be valid');
        assert.ok(res1.whereClauses.some(c => c.includes('LOWER(City) = LOWER(?) OR LOWER(SubdivisionName) LIKE ?')), 'must filter city or subdivision');
        assert.ok(res1.bindValues.includes('estero'));
        assert.ok(res1.bindValues.includes('%estero%'));

        // Test explicit subdivision=Pelican Landing
        const params2 = new URLSearchParams('subdivision=Pelican%20Landing');
        const res2 = buildCommonListingFilters(params2, mockSite);
        assert.ok(res2.valid, 'query must be valid');
        assert.ok(res2.whereClauses.some(c => c.includes('LOWER(SubdivisionName) LIKE ?')));
        assert.ok(res2.bindValues.includes('%pelican landing%'));

        // Test price range string price=500000-1000000
        const params3 = new URLSearchParams('price=500000-1000000');
        const res3 = buildCommonListingFilters(params3, mockSite);
        assert.ok(res3.valid, 'query must be valid');
        assert.ok(res3.whereClauses.some(c => c.includes('ListPrice >= ?')));
        assert.ok(res3.whereClauses.some(c => c.includes('ListPrice <= ?')));
        assert.ok(res3.bindValues.includes(500000));
        assert.ok(res3.bindValues.includes(1000000));

        // Test beds and baths parameters
        const params4 = new URLSearchParams('beds=3&baths=2');
        const res4 = buildCommonListingFilters(params4, mockSite);
        assert.ok(res4.valid, 'query must be valid');
        assert.ok(res4.whereClauses.some(c => c.includes('BedroomsTotal >= ?')));
        assert.ok(res4.whereClauses.some(c => c.includes('BathroomsTotalInteger >= ?')));
        assert.ok(res4.bindValues.includes(3));
        assert.ok(res4.bindValues.includes(2));

        // Test combined quick-search query: location=estero&price=400000-800000&beds=3&baths=2
        const params5 = new URLSearchParams('location=estero&price=400000-800000&beds=3&baths=2');
        const res5 = buildCommonListingFilters(params5, mockSite);
        assert.ok(res5.valid, 'query must be valid');
        assert.ok(res5.bindValues.includes('estero'));
        assert.ok(res5.bindValues.includes(400000));
        assert.ok(res5.bindValues.includes(800000));
        assert.ok(res5.bindValues.includes(3));
        assert.ok(res5.bindValues.includes(2));
    });

    await t.test('2. Embed Loader: Quick Search widget type and query forwarding', () => {
        const embedJs = fs.readFileSync(path.join(rootDir, 'sneak-idx/embed.js'), 'utf8');

        // Verify quick-search widget path
        assert.ok(embedJs.includes("widgetType === 'quick-search'"), 'embed.js must recognize quick-search widget');
        assert.ok(embedJs.includes('/quick-search/'), 'embed.js must route to /quick-search/ path');

        // Verify parameter parsing
        assert.ok(embedJs.includes("searchParams.has('location')"), 'embed.js must parse location parameter');
        assert.ok(embedJs.includes("searchParams.has('subdivision')"), 'embed.js must parse subdivision parameter');
        assert.ok(embedJs.includes("searchParams.has('price')"), 'embed.js must parse price parameter');
        assert.ok(embedJs.includes("searchParams.has('beds')"), 'embed.js must parse beds parameter');
        assert.ok(embedJs.includes("searchParams.has('baths')"), 'embed.js must parse baths parameter');

        // Verify iframe URL forwarding
        assert.ok(embedJs.includes('&location='), 'embed.js must forward location to iframe');
        assert.ok(embedJs.includes('&subdivision='), 'embed.js must forward subdivision to iframe');
        assert.ok(embedJs.includes('&beds='), 'embed.js must forward beds to iframe');
        assert.ok(embedJs.includes('&baths='), 'embed.js must forward baths to iframe');
        assert.ok(embedJs.includes('&price='), 'embed.js must forward price to iframe');
    });

    await t.test('3. Serving Worker: /idx/v1/locations endpoint and Recommended Searches', () => {
        const workerJs = fs.readFileSync(path.join(rootDir, 'SneakIDXWorker.js'), 'utf8');

        assert.ok(workerJs.includes("url.pathname === '/idx/v1/locations'"), 'SneakIDXWorker must route /idx/v1/locations');
        assert.ok(workerJs.includes('async function handleLocations'), 'SneakIDXWorker must define handleLocations');
        assert.ok(workerJs.includes('Pelican Landing, Bonita Springs, FL'), 'handleLocations must include Pelican Landing in recommended');
        assert.ok(workerJs.includes('Bonita Bay, Bonita Springs, FL'), 'handleLocations must include Bonita Bay in recommended');
        assert.ok(workerJs.includes('Shadow Wood, Estero, FL'), 'handleLocations must include Shadow Wood in recommended');
        assert.ok(workerJs.includes('West Bay Club, Estero, FL'), 'handleLocations must include West Bay Club in recommended');
        assert.ok(workerJs.includes('The Colony, Bonita Springs, FL'), 'handleLocations must include The Colony in recommended');
    });

    await t.test('4. Quick Search UI Component: UI structure, autocomplete and redirect', () => {
        const quickSearchHtml = fs.readFileSync(path.join(rootDir, 'sneak-idx/quick-search/index.html'), 'utf8');

        // Heading verification
        assert.ok(quickSearchHtml.includes('Find Your Southwest Florida Dream Home'), 'Quick search must feature default heading');
        assert.ok(quickSearchHtml.includes('urlParams.get(\'heading\')'), 'Quick search must allow agent custom heading');

        // Input placeholder and caret
        assert.ok(quickSearchHtml.includes('Type a city, subdivision, zip, address, or listing #'), 'Must have correct placeholder');
        assert.ok(quickSearchHtml.includes('RECOMMENDED SEARCHES'), 'Must render RECOMMENDED SEARCHES section');

        // Filter buttons
        assert.ok(quickSearchHtml.includes('id="priceBtn"'), 'Must have Price filter button');
        assert.ok(quickSearchHtml.includes('id="bedsBtn"'), 'Must have Beds filter button');
        assert.ok(quickSearchHtml.includes('id="bathsBtn"'), 'Must have Baths filter button');
        assert.ok(quickSearchHtml.includes('id="searchSubmitBtn"'), 'Must have Search submit button');

        // Redirect logic
        assert.ok(quickSearchHtml.includes('http://ursulaweinkauff.com/quick-search'), 'Must default redirect to http://ursulaweinkauff.com/quick-search');
        assert.ok(quickSearchHtml.includes('params.set(\'location\''), 'Redirect must serialize location');
        assert.ok(quickSearchHtml.includes('executeRedirect'), 'Must have executeRedirect logic');
    });

    await t.test('5. Search Application UI: Pre-Filter Hydration from Quick Search Redirect', () => {
        const searchHtml = fs.readFileSync(path.join(rootDir, 'sneak-idx/search/index.html'), 'utf8');

        // Check search UI reads location, subdivision, beds, baths, price from URL
        assert.ok(searchHtml.includes("urlParams.get('location')"), 'search/index.html must read location param');
        assert.ok(searchHtml.includes("urlParams.get('subdivision')"), 'search/index.html must read subdivision param');
        assert.ok(searchHtml.includes("urlParams.get('price')"), 'search/index.html must read price param');
        assert.ok(searchHtml.includes("urlParams.get('beds')"), 'search/index.html must read beds param');
        assert.ok(searchHtml.includes("urlParams.get('baths')"), 'search/index.html must read baths param');

        // Check subdivision in landing banner
        assert.ok(searchHtml.includes('drawerState?.subdivision'), 'Landing banner must support subdivision');
    });

    await t.test('6. Embed Loader & Search Application: Listing Grid (4-Across Showcase)', () => {
        const embedJs = fs.readFileSync(path.join(rootDir, 'sneak-idx/embed.js'), 'utf8');
        const searchHtml = fs.readFileSync(path.join(rootDir, 'sneak-idx/search/index.html'), 'utf8');

        // embed.js parses data-layout and passes layout to iframe
        assert.ok(embedJs.includes("currentScript.getAttribute('data-layout')"), 'embed.js must read data-layout from script');
        assert.ok(embedJs.includes("containerEl.getAttribute('data-layout')"), 'embed.js must read data-layout from container');
        assert.ok(embedJs.includes("widgetType === 'listing_grid' || widgetType === 'grid'"), 'embed.js must map listing_grid widget');
        assert.ok(embedJs.includes("&layout="), 'embed.js must append layout parameter to iframe URL');

        // search/index.html layout-grid-showcase CSS rules
        assert.ok(searchHtml.includes('body.layout-grid-showcase .map-section') || searchHtml.includes('.main-container.layout-grid-showcase .map-section'), 'Must hide map in showcase mode');
        assert.ok(searchHtml.includes('body.layout-grid-showcase .carousels-wrapper') || searchHtml.includes('.main-container.layout-grid-showcase .carousels-wrapper'), 'Must hide Just Listed carousel in showcase mode');
        assert.ok(searchHtml.includes('repeat(4, 1fr)'), 'Must configure 4-column across grid for desktop');
        assert.ok(searchHtml.includes('body.layout-grid-showcase .listings-section'), 'Listings section must expand full width in showcase mode');

        // search/index.html script initialization
        assert.ok(searchHtml.includes("urlParams.get('layout')"), 'search/index.html must read layout parameter');
        assert.ok(searchHtml.includes("classList.add('layout-grid-showcase')"), 'search/index.html must activate layout-grid-showcase class');
    });

    await t.test('7. Admin Embed Generator: Quick Search & 4-Across Listing Grid Presets', () => {
        const adminUiJs = fs.readFileSync(path.join(rootDir, 'sneak-admin/ui.js'), 'utf8');
        const embedGenJs = fs.readFileSync(path.join(rootDir, 'sneak-admin/embed-generator.js'), 'utf8');

        // Check Quick Search in Admin UI
        assert.ok(adminUiJs.includes('adminCustomHeading'), 'Admin UI must have custom heading input for quick search');
        assert.ok(adminUiJs.includes('adminRedirectUrl'), 'Admin UI must have redirect URL input for quick search');
        assert.ok(adminUiJs.includes('data-widget="quick-search"'), 'Admin UI must configure data-widget="quick-search"');
        assert.ok(adminUiJs.includes('data-heading="'), 'Admin UI must serialize data-heading');
        assert.ok(adminUiJs.includes('data-redirect-url="'), 'Admin UI must serialize data-redirect-url');

        // Check Listing Grid in Admin UI
        assert.ok(adminUiJs.includes('data-widget="search" data-layout="grid"'), 'Admin UI must configure data-widget="search" data-layout="grid"');
        assert.ok(adminUiJs.includes('Listing Grid (4-Across Showcase)'), 'Admin UI dropdown must feature 4-Across Showcase option');

        // Check embed-generator.js library
        assert.ok(embedGenJs.includes('data-widget="quick-search"'), 'embed-generator.js must include quick-search widget snippet');
        assert.ok(embedGenJs.includes('data-layout="grid"'), 'embed-generator.js must include data-layout="grid" snippet');
    });
});
