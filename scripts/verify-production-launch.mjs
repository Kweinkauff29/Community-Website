import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const BUILD = '2026.09.01.7.4b2';
const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const urls = {
    serving: process.env.SNEAK_PRODUCTION_SERVING_URL || 'https://sneak-idx-worker.bonitaspringsrealtors.workers.dev',
    sync: process.env.SNEAK_PRODUCTION_SYNC_URL || 'https://sneak-idx-sync.bonitaspringsrealtors.workers.dev',
    consumer: process.env.SNEAK_PRODUCTION_CONSUMER_URL || 'https://sneak-idx-consumer.bonitaspringsrealtors.workers.dev',
    member: process.env.SNEAK_PRODUCTION_MEMBER_URL || 'https://sneak-idx-member.bonitaspringsrealtors.workers.dev',
    admin: process.env.SNEAK_PRODUCTION_ADMIN_URL || 'https://sneak-idx-admin.bonitaspringsrealtors.workers.dev',
    sites: process.env.SNEAK_PRODUCTION_SITES_URL || 'https://sneak-idx-sites.bonitaspringsrealtors.workers.dev',
    alerts: process.env.SNEAK_PRODUCTION_ALERTS_URL || 'https://sneak-idx-alerts.bonitaspringsrealtors.workers.dev'
};

const siteKey = (process.env.SNEAK_PRODUCTION_SITE_KEY || '').trim();
const memberOrigin = (process.env.SNEAK_PRODUCTION_MEMBER_ORIGIN || '').trim().replace(/\/$/, '');
const memberPage = (process.env.SNEAK_PRODUCTION_MEMBER_PAGE_URL || '').trim();
let passed = 0;
let failed = 0;

function check(condition, label, detail = '') {
    if (condition) {
        passed++;
        console.log(`PASS ${label}${detail ? ` — ${detail}` : ''}`);
    } else {
        failed++;
        console.error(`FAIL ${label}${detail ? ` — ${detail}` : ''}`);
    }
}

async function readJson(url, options = {}) {
    try {
        const response = await fetch(url, { redirect: 'manual', ...options });
        const data = await response.json().catch(() => ({}));
        return { response, data };
    } catch (error) {
        return { response: null, data: {}, error: error instanceof Error ? error.message : String(error) };
    }
}

for (const [name, endpoint] of Object.entries({
    serving: `${urls.serving}/idx/v1/health`,
    sync: `${urls.sync}/health`,
    consumer: `${urls.consumer}/api/consumer/version`,
    member: `${urls.member}/health`,
    admin: `${urls.admin}/health`,
    sites: `${urls.sites}/health`,
    alerts: `${urls.alerts}/health`
})) {
    const result = await readJson(endpoint, { headers: { Accept: 'application/json' } });
    check(result.response?.ok === true, `${name} health`, result.error || String(result.response?.status || 'unreachable'));
    check(result.data?.build === BUILD, `${name} build`, String(result.data?.build || 'missing'));
}

const syncHealth = await readJson(`${urls.sync}/health`);
const syncData = syncHealth.data;
const syncState = syncData?.syncState;
const syncRecordCount = Number(syncState?.last_record_count || 0);
const syncCursor = syncState?.last_cursor;
const syncStatus = syncState?.status;
const syncDate = syncState?.last_successful_sync ? new Date(syncState.last_successful_sync) : null;
const syncAgeMinutes = syncDate && Number.isFinite(syncDate.getTime()) ? Math.max(0, Math.round((Date.now() - syncDate.getTime()) / 60000)) : null;

check(Boolean(syncCursor), 'Production sync cursor committed', syncCursor || 'absent');
check(syncRecordCount > 0, 'Production sync listing inventory positive', `${syncRecordCount} listings`);
check(syncStatus === 'success', 'Production sync status successful', syncStatus || 'uninitialized');
check(Number.isFinite(syncAgeMinutes) && syncAgeMinutes <= 180, 'Production sync inventory fresh within policy', syncAgeMinutes !== null ? `${syncAgeMinutes}m ago` : 'no timestamp');
check(syncData?.syncReadiness === 'HEALTHY', 'Production sync readiness is HEALTHY', syncData?.syncReadiness || 'NOT_INITIALIZED');

const consumerHealth = await readJson(`${urls.consumer}/api/consumer/version`);
check(consumerHealth.data?.authEnabled === false, 'Consumer accounts disabled for pilot');
check(consumerHealth.data?.emailAlertsEnabled === false, 'Consumer alert controls disabled for pilot');

const alertHealth = await readJson(`${urls.alerts}/health`);
check(alertHealth.data?.enabled === false && alertHealth.data?.deliveryReady === false, 'Alert delivery disabled and fail closed');

const adminHealth = await readJson(`${urls.admin}/health`);
check(adminHealth.data?.growthZoneEnabled === false && adminHealth.data?.growthZoneConfigured === false,
    'GrowthZone automation disabled for manual pilot mode');

const memberProtected = await readJson(`${urls.member}/api/member/overview`);
check(memberProtected.response?.status === 401, 'Member protected route rejects anonymous access');
const adminProtected = await readJson(`${urls.admin}/api/admin/accounts`);
check(adminProtected.response?.status === 401, 'Admin protected route rejects anonymous access');

for (const configFile of [
    'wrangler.sneak.production.toml', 'wrangler.sneak-sync.production.toml',
    'wrangler.sneak-member.production.toml', 'wrangler.sneak-consumer.production.toml',
    'wrangler.sneak-alerts.production.toml', 'wrangler.sneak-admin.production.toml',
    'wrangler.sneak-sites.production.toml'
]) {
    const source = fs.readFileSync(path.join(rootDir, configFile), 'utf8');
    check(!/^\s*BRIDGE_TOKEN\s*=/m.test(source), `${configFile} contains no Bridge secret value`);
}

if (!siteKey || !memberOrigin) {
    check(false, 'Tenant smoke inputs supplied', 'set SNEAK_PRODUCTION_SITE_KEY and SNEAK_PRODUCTION_MEMBER_ORIGIN');
} else {
    const bootstrap = await readJson(`${urls.serving}/idx/v1/bootstrap?site=${encodeURIComponent(siteKey)}&hostPageUrl=${encodeURIComponent(memberPage || `${memberOrigin}/`)}`, {
        headers: { Origin: memberOrigin, Referer: `${memberOrigin}/` }
    });
    check(bootstrap.response?.ok === true && typeof bootstrap.data?.session === 'string', 'Authorized-domain bootstrap');

    if (bootstrap.response?.ok && bootstrap.data?.session) {
        const headers = { Origin: memberOrigin, Referer: `${memberOrigin}/`, Authorization: `Bearer ${bootstrap.data.session}` };
        const search = await readJson(`${urls.serving}/idx/v1/search?site=${encodeURIComponent(siteKey)}&limit=1`, { headers });
        const first = search.data?.data?.[0];
        check(search.response?.ok === true && first?.ListingKey, 'Production search returns current listing');

        if (first?.ListingKey) {
            const key = encodeURIComponent(first.ListingKey);
            const detail = await readJson(`${urls.serving}/idx/v1/listing/${key}?site=${encodeURIComponent(siteKey)}`, { headers });
            check(detail.response?.ok === true && detail.data?.ListingKey === first.ListingKey, 'Production listing detail');
            const media = await readJson(`${urls.serving}/idx/v1/listing/${key}/media?site=${encodeURIComponent(siteKey)}`, { headers });
            const mediaItems = Array.isArray(media.data) ? media.data : media.data?.data;
            check(media.response?.ok === true && Array.isArray(mediaItems) && mediaItems.length > 0, 'Production listing media');
        }

        const config = await readJson(`${urls.serving}/idx/v1/config?site=${encodeURIComponent(siteKey)}`, { headers });
        check(config.response?.ok === true && config.data?.features?.consumerAuth === false, 'Serving capability profile');
    }

    if (memberPage) {
        try {
            const pageResponse = await fetch(memberPage, { redirect: 'follow' });
            check(pageResponse.ok && new URL(pageResponse.url).protocol === 'https:', 'Member website HTTPS page');
        } catch (error) {
            check(false, 'Member website HTTPS page', error instanceof Error ? error.message : String(error));
        }
    }
}

console.log(`Production launch smoke: ${passed}/${passed + failed} passed, ${failed} failed.`);
if (failed) process.exitCode = 1;
