/** Live staging verification for Phase 7.3C3A. */
import assert from 'node:assert/strict';

const BUILD = '2026.08.31.7.3c3a1';
const ALERT_BUILD = '2026.08.31.7.3c3a';
const SITE = 'ursula-weinkauff-pilot';
const ORIGIN = 'https://coconutcoastrealtors.org';
const IDX = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const CONSUMER = 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
const MEMBER = 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev';
const ALERTS = 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev';
const PILOT = 'https://coconutcoastrealtors.org/idx-test/';

let passed = 0;
let total = 0;

async function check(name, fn) {
    total++;
    try {
        await fn();
        passed++;
        console.log(`[PASS] ${name}`);
    } catch (error) {
        console.error(`[FAIL] ${name}: ${error.message}`);
    }
}

await check('Alert Worker remains healthy and reports explicit delivery configuration state', async () => {
    const res = await fetch(`${ALERTS}/api/alerts/version`);
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.build, ALERT_BUILD);
    assert.equal(typeof data.emailProviderConfigured, 'boolean');
    assert.equal(typeof data.signingSecretConfigured, 'boolean');
    assert.equal(data.deliveryReady, data.emailProviderConfigured && data.signingSecretConfigured);
    console.log(`       Mailjet=${data.emailProviderConfigured} signingSecret=${data.signingSecretConfigured} deliveryReady=${data.deliveryReady}`);
});

await check('Consumer Worker reports C3A build', async () => {
    const res = await fetch(`${CONSUMER}/api/consumer/version`);
    assert.equal(res.status, 200);
    assert.equal((await res.json()).build, BUILD);
});

await check('Member Worker reports the corrective C3A.1 browser-QA build', async () => {
    const res = await fetch(`${MEMBER}/health`, { headers: { Accept: 'application/json' } });
    assert.equal(res.status, 200);
    assert.equal((await res.json()).build, BUILD);
});

await check('Recently Viewed rejects anonymous consumer requests', async () => {
    const res = await fetch(`${CONSUMER}/api/consumer/recently-viewed?site=${SITE}`);
    assert.equal(res.status, 401);
});

await check('Compare list rejects anonymous consumer requests', async () => {
    const res = await fetch(`${CONSUMER}/api/consumer/compare?site=${SITE}`);
    assert.equal(res.status, 401);
});

await check('Compare writes reject anonymous consumer requests', async () => {
    const res = await fetch(`${CONSUMER}/api/consumer/compare`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ site: SITE, listingKey: 'unauthorized-test' })
    });
    assert.equal(res.status, 401);
});

let session = '';
let sampleKeys = [];
await check('Serving Worker reports C3A build and pilot search returns current listing keys', async () => {
    const health = await fetch(`${IDX}/idx/v1/health`);
    assert.equal(health.status, 200);
    assert.equal((await health.json()).build, BUILD);
    const boot = await fetch(`${IDX}/idx/v1/bootstrap?site=${SITE}`, { headers: { Origin: ORIGIN } });
    assert.equal(boot.status, 200);
    session = (await boot.json()).session;
    assert.ok(session);
    const search = await fetch(`${IDX}/idx/v1/search?site=${SITE}&limit=4`, {
        headers: { Authorization: `Bearer ${session}`, 'X-SNEAK-Session': session, Origin: ORIGIN }
    });
    assert.equal(search.status, 200);
    sampleKeys = (await search.json()).data.map(item => item.ListingKey).filter(Boolean);
    assert.ok(sampleKeys.length > 0);
});

await check('Bounded listing-summary endpoint returns current tenant-scoped rows', async () => {
    assert.ok(session && sampleKeys.length);
    const params = new URLSearchParams({ site: SITE, keys: sampleKeys.join(',') });
    const res = await fetch(`${IDX}/idx/v1/listings/summary?${params}`, {
        headers: { Authorization: `Bearer ${session}`, 'X-SNEAK-Session': session, Origin: ORIGIN }
    });
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.deepEqual(data.data.map(item => item.ListingKey), sampleKeys);
    assert.ok(data.data.every(item => item.InternetEntireListingDisplayYN === 1));
});

await check('Listing-summary endpoint rejects more than 20 keys', async () => {
    const keys = Array.from({ length: 21 }, (_, index) => `bounded-${index}`).join(',');
    const res = await fetch(`${IDX}/idx/v1/listings/summary?site=${SITE}&keys=${keys}`, {
        headers: { Authorization: `Bearer ${session}`, 'X-SNEAK-Session': session, Origin: ORIGIN }
    });
    assert.equal(res.status, 400);
});

await check('Live search UI contains C3A controls and anonymous privacy boundary', async () => {
    const res = await fetch(`${IDX}/search/index.html`);
    assert.equal(res.status, 200);
    const html = await res.text();
    assert.ok(html.includes(`data-ui-build="${BUILD}"`));
    assert.ok(html.includes('recentlyViewedSection'));
    assert.ok(html.includes('compareTray'));
    assert.ok(html.includes('compareModal'));
    assert.ok(html.includes("'ccor_recently_viewed_' + SITE_KEY"));
    assert.ok(!html.includes('recently-viewed/merge'));
});

await check('Live embed loader carries the C3A deterministic build', async () => {
    const res = await fetch(`${IDX}/embed.js`);
    assert.equal(res.status, 200);
    assert.ok((await res.text()).includes(`const buildVersion = '${BUILD}'`));
});

await check('Live WordPress pilot remains reachable with the staging embed', async () => {
    const res = await fetch(PILOT, { headers: { 'User-Agent': 'CCOR-C3A-Verification/1.0' } });
    assert.equal(res.status, 200);
    assert.ok((await res.text()).includes('sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/embed.js'));
});

console.log(`\nPhase 7.3C3A live verification: ${passed}/${total} passed`);
if (passed !== total) process.exit(1);
