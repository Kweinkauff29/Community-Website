#!/usr/bin/env node
/** Read-only real-staging verification for authoritative listing detail and synchronized galleries. */

const BUILD = '2026.08.31.7.4a';
const API = process.env.SNEAK_DETAIL_API_URL || 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const SITE = process.env.SNEAK_DETAIL_SITE_KEY || 'mem-gz-2mmc';
const ORIGIN = process.env.SNEAK_DETAIL_ORIGIN || 'http://localhost';
const SAMPLE_SIZE = Math.min(20, Math.max(10, Number(process.env.SNEAK_DETAIL_SAMPLE_SIZE) || 10));
let passed = 0;
let failed = 0;

function check(condition, label, detail = '') {
    if (condition) {
        passed += 1;
        console.log('PASS ' + label + (detail ? ' — ' + detail : ''));
    } else {
        failed += 1;
        console.error('FAIL ' + label + (detail ? ' — ' + detail : ''));
    }
}

async function jsonRequest(path, headers = {}) {
    const response = await fetch(API + path, { headers: { Accept: 'application/json', Origin: ORIGIN, ...headers } });
    const data = await response.json().catch(() => ({}));
    return { response, data };
}

const health = await jsonRequest('/idx/v1/health');
check(health.response.ok && health.data.build === BUILD, 'Serving health/build', health.data.build || String(health.response.status));

const bootstrap = await jsonRequest('/idx/v1/bootstrap?site=' + encodeURIComponent(SITE));
check(bootstrap.response.ok && typeof bootstrap.data.session === 'string', 'authorized tenant bootstrap', String(bootstrap.response.status));
const sessionHeaders = { 'X-SNEAK-Session': bootstrap.data.session || '' };

const search = await jsonRequest('/idx/v1/search?site=' + encodeURIComponent(SITE) + '&limit=100&status=All&sort=newest', sessionHeaders);
const cards = Array.isArray(search.data?.data) ? search.data.data : [];
check(search.response.ok && cards.length > 0, 'search endpoint returns current cards', String(cards.length));

const candidates = cards.filter(card => card?.ListingKey).slice(0, SAMPLE_SIZE);
check(candidates.length === SAMPLE_SIZE, 'real listing sample is bounded and complete', String(candidates.length));

let multiPhotoListings = 0;
for (const card of candidates) {
    const key = String(card.ListingKey);
    const encodedKey = encodeURIComponent(key);
    const [detailResult, mediaResult] = await Promise.all([
        jsonRequest('/idx/v1/listing/' + encodedKey + '?site=' + encodeURIComponent(SITE), sessionHeaders),
        jsonRequest('/idx/v1/listing/' + encodedKey + '/media?site=' + encodeURIComponent(SITE), sessionHeaders)
    ]);

    const detail = detailResult.data?.data || {};
    let mediaJson = [];
    try {
        const parsed = JSON.parse(detail.MediaJSON || '[]');
        if (Array.isArray(parsed)) mediaJson = parsed.filter(Boolean);
    } catch {}
    const detailMedia = Array.isArray(detail.Media) ? detail.Media.map(item => item?.MediaURL).filter(Boolean) : [];
    const endpointMedia = Array.isArray(mediaResult.data?.media) ? mediaResult.data.media.filter(Boolean) : [];
    if (mediaJson.length > 1) multiPhotoListings += 1;

    check(detailResult.response.ok && detail.ListingKey === key, key + ' authoritative detail response');
    check(mediaResult.response.ok && mediaResult.data?.listingKey === key, key + ' media response contract');
    check(Boolean(detail.PrimaryPhoto) === Boolean(card.PrimaryPhoto), key + ' PrimaryPhoto agreement');
    check(detailMedia.length === mediaJson.length, key + ' detail Media count', String(detailMedia.length));
    check(endpointMedia.length === mediaJson.length, key + ' media endpoint count', String(endpointMedia.length));
    check(Boolean(detail.PublicRemarks) && Boolean(detail.ListOfficeName) && Boolean(detail.ListAgentFullName),
        key + ' remarks and attribution populated');

    console.log('SAMPLE ' + JSON.stringify({
        ListingKey: key,
        propertyType: detail.PropertyType || card.PropertyType || null,
        primaryPhotoPresent: detail.PrimaryPhoto ? 'YES' : 'NO',
        mediaJsonCount: mediaJson.length,
        detailMediaCount: detailMedia.length,
        mediaEndpointCount: endpointMedia.length
    }));
}

check(multiPhotoListings >= Math.min(3, candidates.length), 'several sampled listings contain multiple synchronized photos', String(multiPhotoListings));
console.log('RESULT ' + passed + '/' + (passed + failed) + ' PASS');
process.exitCode = failed ? 1 : 0;
