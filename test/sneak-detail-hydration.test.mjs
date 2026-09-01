import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

import { getListingMediaUrls } from '../SneakIDXWorker.js';
import { applyListingDisplayControls, buildTenantListingScope } from '../sneak-shared/idx-query.js';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const uiSource = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'search', 'index.html'), 'utf8');
const workerSource = fs.readFileSync(path.join(rootDir, 'SneakIDXWorker.js'), 'utf8');
const helperSource = fs.readFileSync(path.join(rootDir, 'sneak-idx', 'detail-hydration.js'), 'utf8');

function loadHydrationHelpers() {
    const sandbox = { window: {}, AbortController };
    vm.runInNewContext(helperSource, sandbox, { filename: 'detail-hydration.js' });
    return sandbox.window.CCORDetailHydration;
}

const hydration = loadHydrationHelpers();

describe('SNEAK IDX listing detail and multi-photo regression', () => {
    test('1. media endpoint URL-string contract is consumed canonically', () => {
        assert.deepEqual([...hydration.mediaFromEndpoint({ media: ['https://img/1.jpg', 'https://img/2.jpg'] })],
            ['https://img/1.jpg', 'https://img/2.jpg']);
        assert.deepEqual([...hydration.mediaFromEndpoint({ data: [{ MediaURL: 'https://old-contract.jpg' }] })], []);
        assert.doesNotMatch(uiSource, /mediaData\.data/);
    });

    test('2. multi-photo MediaJSON becomes the complete ordered detail gallery', () => {
        const listing = {
            PrimaryPhoto: 'https://img/1.jpg',
            MediaJSON: JSON.stringify(['https://img/1.jpg', 'https://img/2.jpg', 'https://img/3.jpg'])
        };
        assert.deepEqual(getListingMediaUrls(listing), ['https://img/1.jpg', 'https://img/2.jpg', 'https://img/3.jpg']);
        assert.match(workerSource, /fullDetails\.Media = getListingMediaUrls\(fullDetails\)/);
    });

    test('3. full detail Media is the primary gallery source when the media fallback fails', () => {
        const detail = { PrimaryPhoto: 'https://img/1.jpg', Media: [
            { MediaURL: 'https://img/1.jpg', Order: 0 },
            { MediaURL: 'https://img/2.jpg', Order: 1 }
        ] };
        const detailMedia = hydration.mediaFromDetail(detail);
        assert.deepEqual([...hydration.chooseGallery(detailMedia, [], ['https://optimistic.jpg'])],
            ['https://img/1.jpg', 'https://img/2.jpg']);
    });

    test('4. search-card clicks always enter the keyed authoritative hydration path', () => {
        assert.match(uiSource, /card\.onclick = \(\) => openDetailByKey\(item\.ListingKey, item\)/);
        assert.doesNotMatch(uiSource, /card\.onclick = \(\) => openDetail\(item\)/);
    });

    test('5. allListings cache supplies optimism but never bypasses the detail fetch', () => {
        assert.match(uiSource, /optimisticSummary \|\| allListings\.find/);
        assert.match(uiSource, /const detailPromise = fetch\(API_BASE \+ '\/idx\/v1\/listing\/'/);
        assert.doesNotMatch(uiSource, /if \(item\) \{\s*openDetail\(item\);\s*return;/);
    });

    test('6. authoritative PublicRemarks replaces the intentional loading state', () => {
        assert.equal(hydration.descriptionText({}, true), 'Loading full property details…');
        assert.equal(hydration.descriptionText({ PublicRemarks: 'Authoritative remarks.' }, false), 'Authoritative remarks.');
        assert.equal(hydration.descriptionText({}, false), 'No public remarks available.');
        assert.doesNotMatch(uiSource, /Loading description\.\.\./);
    });

    test('7. authoritative listing brokerage and agent attribution render truthfully', () => {
        const attribution = hydration.listingAttribution({
            ListOfficeName: 'Example Brokerage',
            ListAgentFullName: 'Alex Agent'
        });
        assert.equal(attribution.brokerage, 'Listed by: Example Brokerage');
        assert.equal(attribution.agent, 'Listing Agent: Alex Agent');
        assert.equal(hydration.listingAttribution({}).brokerage, 'Listing brokerage not provided.');
    });

    test('8. rapid A to B selection aborts A and prevents stale response application', () => {
        const coordinator = hydration.createRequestCoordinator();
        const requestA = coordinator.begin('A');
        const requestB = coordinator.begin('B');
        assert.equal(requestA.signal.aborted, true);
        assert.equal(coordinator.isCurrent(requestA), false);
        assert.equal(coordinator.isCurrent(requestB), true);
        assert.equal(coordinator.matchesListing(requestA, { ListingKey: 'A' }), false);
        assert.equal(coordinator.matchesListing(requestB, { ListingKey: 'B' }), true);
    });

    test('9. inaccessible detail removes optimistic summary content', () => {
        assert.match(uiSource, /if \(!response\.ok\) \{\s*detailFailed = true;\s*renderDetailUnavailable\(\);\s*return;/);
        assert.match(uiSource, /function renderDetailUnavailable\(\) \{[\s\S]*currentDetail = null;[\s\S]*setGalleryMedia\(\[\]\);/);
        assert.match(uiSource, /This property is no longer available\./);
    });

    test('10. media failure does not block authoritative textual detail rendering', () => {
        const detailStart = uiSource.indexOf('async function openDetailByKey');
        const detailEnd = uiSource.indexOf('window.openDetailByKey = openDetailByKey', detailStart);
        const detailBody = uiSource.slice(detailStart, detailEnd);
        assert.match(detailBody, /const mediaPromise = fetchDetailMedia\(request\);/);
        assert.match(detailBody, /const response = await detailPromise;/);
        assert.doesNotMatch(detailBody, /await mediaPromise/);
        assert.match(detailBody, /renderDetailRecord\(fullDetail\);/);
    });

    test('11. Land and Commercial categories do not receive fake residential facts', () => {
        assert.equal(hydration.classifyListing({ PropertyType: 'Land' }), 'land');
        assert.equal(hydration.classifyListing({ PropertyType: 'Commercial Sale' }), 'commercial');
        assert.equal(hydration.classifyListing({ PropertyType: 'Residential Lease' }), 'rental');
        const landLabels = hydration.buildDetailFacts({ PropertyType: 'Land', BedroomsTotal: 4 }).overview.map(fact => fact.label);
        const commercialLabels = hydration.buildDetailFacts({ PropertyType: 'Commercial Sale', BedroomsTotal: 4, LivingArea: 1200 }).overview.map(fact => fact.label);
        assert.equal(landLabels.includes('Bedrooms'), false);
        assert.equal(commercialLabels.includes('Bedrooms'), false);
        assert.equal(commercialLabels.includes('Building Area'), true);
    });

    test('12. address suppression remains fail-closed in authoritative detail', () => {
        const hidden = applyListingDisplayControls({
            InternetAddressDisplayYN: 0,
            UnparsedAddress: '123 Private Street',
            StreetNumber: '123',
            StreetName: 'Private Street',
            UnitNumber: '4'
        });
        assert.equal(hidden.UnparsedAddress, 'Address Undisclosed');
        assert.equal(hidden.StreetNumber, '');
        assert.equal(hidden.StreetName, '');
        assert.equal(hidden.UnitNumber, '');
    });

    test('13. tenant scope remains parameterized and fail-closed', () => {
        assert.deepEqual(buildTenantListingScope({ scope_type: 'agent', scope_value: '' }), {
            valid: false, clause: '1=0', binds: [], error: 'MissingAgentScopeValue'
        });
        const office = buildTenantListingScope({ scope_type: 'office', scope_value: 'OFFICE-1' });
        assert.equal(office.valid, true);
        assert.deepEqual(office.binds, ['OFFICE-1', 'OFFICE-1']);
        assert.match(office.clause, /ListOfficeMlsId = \?/);
    });

    test('14. detail and media serving handlers remain D1-backed with no Bridge call', () => {
        const start = workerSource.indexOf('async function handleListingDetail');
        const end = workerSource.indexOf('async function handleAgentListings', start);
        const servingHandlers = workerSource.slice(start, end);
        assert.match(servingHandlers, /env\.DB\.prepare/);
        assert.doesNotMatch(servingHandlers, /bridgedataoutput|BRIDGE_TOKEN|api\.bridgedata/);
    });

    test('15. media URLs are scheme-checked, deduplicated, and order-preserving', () => {
        const urls = getListingMediaUrls({
            MediaJSON: JSON.stringify([
                'https://img/first.jpg',
                'javascript:alert(1)',
                'https://img/first.jpg',
                'http://img/second.jpg',
                '/local/fallback.jpg'
            ])
        });
        assert.deepEqual(urls, ['https://img/first.jpg', 'http://img/second.jpg', '/local/fallback.jpg']);
        assert.deepEqual([...hydration.uniqueStrings(['a', 'a', 'b'])], ['a', 'b']);
    });

    test('16. gallery prioritizes the selected image and lazy-loads other thumbnails', () => {
        assert.match(uiSource, /image\.loading = index === detailIdx \? 'eager' : 'lazy'/);
        assert.match(uiSource, /mainImage\.fetchPriority = 'high'/);
        assert.match(uiSource, /count\.textContent = String\(detailIdx \+ 1\) \+ ' \/ ' \+ String\(currentMediaUrls\.length\)/);
        assert.match(uiSource, /previous\.disabled = detailIdx === 0/);
        assert.match(uiSource, /next\.disabled = detailIdx === currentMediaUrls\.length - 1/);
    });

    test('17. every current listing surface uses the single authoritative entry point', () => {
        assert.doesNotMatch(uiSource, /function openDetail\(/);
        assert.doesNotMatch(uiSource, /\bopenDetail\(item\)/);
        assert.match(uiSource, /window\.openDetailByKey = openDetailByKey/);
    });

    test('18. activity and Recently Viewed work begins only after primary fetches are started', () => {
        const detailStart = uiSource.indexOf('async function openDetailByKey');
        const detailEnd = uiSource.indexOf('window.openDetailByKey = openDetailByKey', detailStart);
        const body = uiSource.slice(detailStart, detailEnd);
        assert.ok(body.indexOf('const detailPromise = fetch') < body.indexOf('recordDetailViewInBackground(listingKey)'));
        assert.ok(body.indexOf('const mediaPromise = fetchDetailMedia') < body.indexOf('recordDetailViewInBackground(listingKey)'));
        assert.match(uiSource, /setTimeout\(\(\) => \{[\s\S]*listing_view/);
    });

    test('19. zero and invalid quantities are omitted instead of displayed as facts', () => {
        const facts = hydration.buildDetailFacts({
            PropertyType: 'Residential',
            ListPrice: 0,
            BedroomsTotal: 0,
            BathroomsTotalInteger: '0',
            LivingArea: 0,
            LotSizeAcres: '0',
            YearBuilt: 0,
            GarageSpaces: 0
        });
        assert.deepEqual([...facts.overview].map(fact => ({ ...fact })), [{ label: 'Property Type', value: 'Residential', kind: 'text' }]);
        assert.deepEqual([...facts.features], []);
        assert.equal(hydration.positiveNumber(-1), null);
        assert.equal(hydration.positiveNumber('1250'), 1250);
    });

    test('20. residential and rental facts include only meaningful residential dimensions', () => {
        const residential = hydration.buildDetailFacts({
            PropertyType: 'Residential', BedroomsTotal: 3, BathroomsTotalInteger: 2, LivingArea: 1840
        });
        const rental = hydration.buildDetailFacts({
            PropertyType: 'Residential Lease', BedroomsTotal: 2, BathroomsTotalInteger: 1, LivingArea: 900
        });
        for (const result of [residential, rental]) {
            const labels = result.overview.map(fact => fact.label);
            assert.equal(labels.includes('Bedrooms'), true);
            assert.equal(labels.includes('Bathrooms'), true);
            assert.equal(labels.includes('Living Area'), true);
        }
    });

    test('21. affirmative amenities render while false and zero features remain absent', () => {
        const facts = hydration.buildDetailFacts({
            PropertyType: 'Residential', WaterfrontYN: true, PoolPrivateYN: 1,
            NewConstructionYN: false, GarageSpaces: 2
        });
        assert.deepEqual([...facts.features].map(fact => fact.label), ['Waterfront', 'Private Pool', 'Garage Spaces']);
        assert.equal(hydration.affirmative('true'), true);
        assert.equal(hydration.affirmative('false'), false);
    });

    test('22. location and community facts expose available current fields', () => {
        const facts = hydration.buildDetailFacts({
            City: 'Naples', CountyOrParish: 'Collier', PostalCode: '34102',
            SubdivisionName: 'Old Naples', Zoning: 'R1'
        });
        assert.deepEqual([...facts.location].map(fact => fact.label), ['City', 'County', 'ZIP Code', 'Subdivision', 'Zoning']);
    });

    test('23. listing information includes identifiers, contract date, price history, office, and agent', () => {
        const facts = hydration.buildDetailFacts({
            ListingKey: 'key-1', ListingId: 'MLS-123', ListingContractDate: '2026-08-20',
            OriginalListPrice: 600000, ListPrice: 570000,
            ListOfficeName: 'Example Brokerage', ListAgentFullName: 'Alex Agent'
        });
        const byLabel = new Map(facts.listingInformation.map(fact => [fact.label, fact]));
        assert.equal(byLabel.get('MLS / List ID').value, 'MLS-123');
        assert.equal(byLabel.get('Listing Contract Date').kind, 'date');
        assert.equal(byLabel.get('Original List Price').value, 600000);
        assert.equal(byLabel.get('Current List Price').value, 570000);
        assert.deepEqual({ ...byLabel.get('Price Reduction').value }, { amount: 30000, percent: 5 });
        assert.equal(byLabel.get('Listing Office').value, 'Example Brokerage');
        assert.equal(byLabel.get('Listing Agent').value, 'Alex Agent');
    });

    test('24. price reduction is never invented without a lower current price', () => {
        const equal = hydration.buildDetailFacts({ OriginalListPrice: 500000, ListPrice: 500000 });
        const higher = hydration.buildDetailFacts({ OriginalListPrice: 500000, ListPrice: 510000 });
        assert.equal(equal.listingInformation.some(fact => fact.label === 'Price Reduction'), false);
        assert.equal(higher.listingInformation.some(fact => fact.label === 'Price Reduction'), false);
    });

    test('25. detail modal uses a contained neutral image stage and intentional inner scrolling', () => {
        assert.match(uiSource, /\.detail-gallery-img \{[\s\S]*object-fit: contain;[\s\S]*object-position: center;/);
        assert.match(uiSource, /\.detail-modal \{[\s\S]*overflow: hidden;/);
        assert.match(uiSource, /\.detail-scroll-region \{[\s\S]*overflow-y: auto;/);
        assert.match(uiSource, /height: 100dvh/);
    });

    test('26. thumbnail rendering is bounded and remains keyboard accessible', () => {
        assert.match(uiSource, /const DETAIL_THUMBNAIL_WINDOW = 12/);
        assert.match(uiSource, /currentMediaUrls\.slice\(windowStart, windowEnd\)/);
        assert.match(uiSource, /button\.setAttribute\('aria-label', 'Show property photo '/);
        assert.doesNotMatch(uiSource, /currentMediaUrls\.forEach\(\(url, index\) =>/);
    });

    test('27. richer detail hierarchy contains every required section', () => {
        for (const heading of [
            'Property Overview', 'Features & Amenities', 'Location & Community',
            'Listing Information', 'Description', 'Attribution & Contact'
        ]) {
            assert.match(uiSource, new RegExp(heading.replace('&', '&amp;')));
        }
        assert.match(uiSource, /white-space: pre-line/);
    });
});
