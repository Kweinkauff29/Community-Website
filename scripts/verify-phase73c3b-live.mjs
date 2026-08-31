/** Live staging verification for Phase 7.3C3B. */
import assert from 'node:assert/strict';

const BUILD = '2026.08.31.7.3c3b';
const MEMBER_BUILD = '2026.08.31.7.3c3a1';
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

await check('Alert Worker remains healthy and reports explicit delivery state', async () => {
    const res = await fetch(`${ALERTS}/api/alerts/version`);
    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.build, ALERT_BUILD);
    assert.equal(typeof data.emailProviderConfigured, 'boolean');
    assert.equal(typeof data.signingSecretConfigured, 'boolean');
    assert.equal(data.deliveryReady, data.emailProviderConfigured && data.signingSecretConfigured);
    console.log(`       Mailjet=${data.emailProviderConfigured} signingSecret=${data.signingSecretConfigured} deliveryReady=${data.deliveryReady}`);
});

await check('Consumer Worker reports C3B build', async () => {
    const res = await fetch(`${CONSUMER}/api/consumer/version`);
    assert.equal(res.status, 200);
    assert.equal((await res.json()).build, BUILD);
});

await check('Member Worker remains on its verified C3A.1 build', async () => {
    const res = await fetch(`${MEMBER}/health`, { headers: { Accept: 'application/json' } });
    assert.equal(res.status, 200);
    assert.equal((await res.json()).build, MEMBER_BUILD);
});

await check('Shared-list owner collection routes reject anonymous access', async () => {
    const read = await fetch(`${CONSUMER}/api/consumer/shared-lists?site=${SITE}`);
    assert.equal(read.status, 401);
    const create = await fetch(`${CONSUMER}/api/consumer/shared-lists`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ site: SITE, name: 'Unauthorized' })
    });
    assert.equal(create.status, 401);
});

await check('Shared-list owner item/share routes reject anonymous access', async () => {
    const detail = await fetch(`${CONSUMER}/api/consumer/shared-lists/not-a-list?site=${SITE}`);
    assert.equal(detail.status, 401);
    const share = await fetch(`${CONSUMER}/api/consumer/shared-lists/not-a-list/share`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ site: SITE })
    });
    assert.equal(share.status, 401);
});

await check('C3A Recently Viewed and Compare protections remain active', async () => {
    const recent = await fetch(`${CONSUMER}/api/consumer/recently-viewed?site=${SITE}`);
    const compare = await fetch(`${CONSUMER}/api/consumer/compare?site=${SITE}`);
    assert.equal(recent.status, 401);
    assert.equal(compare.status, 401);
});

let session = '';
let sampleKeys = [];
await check('Serving Worker reports C3B and bootstraps the live pilot', async () => {
    const health = await fetch(`${IDX}/idx/v1/health`);
    assert.equal(health.status, 200);
    assert.equal((await health.json()).build, BUILD);
    const candidate = `${PILOT}?auth_code=secret&session=secret&ccor_listing=old&ccor_list=old&keep=ok#secret`;
    const bootUrl = `${IDX}/idx/v1/bootstrap?site=${SITE}&hostPageUrl=${encodeURIComponent(candidate)}`;
    const boot = await fetch(bootUrl, { headers: { Origin: ORIGIN } });
    assert.equal(boot.status, 200);
    const bootData = await boot.json();
    session = bootData.session;
    assert.ok(session);
    const sanitized = new URL(bootData.hostPageUrl);
    assert.equal(sanitized.origin, ORIGIN);
    assert.equal(sanitized.searchParams.get('keep'), 'ok');
    for (const key of ['auth_code', 'session', 'ccor_listing', 'ccor_list']) {
        assert.equal(sanitized.searchParams.has(key), false);
    }
    assert.equal(sanitized.hash, '');
});

await check('Pilot search and bounded current listing summaries remain available', async () => {
    const headers = { Authorization: `Bearer ${session}`, 'X-SNEAK-Session': session, Origin: ORIGIN };
    const search = await fetch(`${IDX}/idx/v1/search?site=${SITE}&limit=4`, { headers });
    assert.equal(search.status, 200);
    sampleKeys = (await search.json()).data.map(item => item.ListingKey).filter(Boolean);
    assert.ok(sampleKeys.length > 0);
    const params = new URLSearchParams({ site: SITE, keys: sampleKeys.join(',') });
    const summary = await fetch(`${IDX}/idx/v1/listings/summary?${params}`, { headers });
    assert.equal(summary.status, 200);
    const data = await summary.json();
    assert.deepEqual(data.data.map(item => item.ListingKey), sampleKeys);
});

await check('Malformed and unknown public slugs fail with the same generic response', async () => {
    const headers = { Authorization: `Bearer ${session}`, 'X-SNEAK-Session': session, Origin: ORIGIN };
    const malformed = await fetch(`${IDX}/idx/v1/shared-list/malformed?site=${SITE}`, { headers });
    const unknown = await fetch(`${IDX}/idx/v1/shared-list/${'a'.repeat(48)}?site=${SITE}`, { headers });
    assert.equal(malformed.status, 404);
    assert.equal(unknown.status, 404);
    assert.deepEqual(await malformed.json(), await unknown.json());
});

await check('Live search UI contains complete C3B controls and privacy markers', async () => {
    const res = await fetch(`${IDX}/search/index.html`);
    assert.equal(res.status, 200);
    const html = await res.text();
    assert.ok(html.includes(`data-ui-build="${BUILD}"`));
    for (const marker of [
        'sharedListsModal', 'addToListModal', 'publicSharedListModal',
        'Save as List', 'Enable Sharing', 'Disable Sharing',
        'navigator.share', 'noindex, nofollow'
    ]) assert.ok(html.includes(marker), `Missing UI marker: ${marker}`);
    assert.ok(!html.includes('recently-viewed/merge'));
});

await check('Live embed forwards ccor_list on the deterministic C3B build', async () => {
    const res = await fetch(`${IDX}/embed.js`);
    assert.equal(res.status, 200);
    const js = await res.text();
    assert.ok(js.includes(`const buildVersion = '${BUILD}'`));
    assert.ok(js.includes("currentUrl.searchParams.has('ccor_list')"));
    assert.ok(js.includes('&ccor_list='));
});

await check('Live WordPress pilot remains reachable with the staging embed', async () => {
    const res = await fetch(PILOT, { headers: { 'User-Agent': 'CCOR-C3B-Verification/1.0' } });
    assert.equal(res.status, 200);
    assert.ok((await res.text()).includes('sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/embed.js'));
});

console.log(`\nPhase 7.3C3B live verification: ${passed}/${total} passed`);
if (passed !== total) process.exit(1);
