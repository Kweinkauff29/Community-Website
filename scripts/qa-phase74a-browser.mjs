#!/usr/bin/env node
/**
 * Phase 7.4A final browser sign-off.
 *
 * Uses a locally installed Playwright/Chromium engine against live staging. It creates
 * short-lived hashed Admin/Member sessions plus one isolated account/client fixture,
 * then deletes only those exact records during cleanup.
 */

import crypto from 'node:crypto';
import { createRequire } from 'node:module';
import { existsSync, mkdirSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import process from 'node:process';

const ADMIN = 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev';
const MEMBER = 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev';
const SERVING = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';
const SITE = process.env.SNEAK_DETAIL_SITE_KEY || 'ursula-weinkauff-pilot';
const SITE_ORIGIN = process.env.SNEAK_DETAIL_ORIGIN || 'https://coconutcoastrealtors.org';
const BUILD = '2026.08.31.7.4a';
const SKIP_LISTING_BROWSER = process.env.PHASE74A_BROWSER_SKIP_LISTING === '1';
const suffix = `${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 6)}`;
const screenshotDir = join(tmpdir(), `phase74a-browser-${suffix}`);
mkdirSync(screenshotDir, { recursive: true });

const viewports = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'tablet', width: 1024, height: 768 },
  { name: 'mobile', width: 390, height: 844 }
];

let passed = 0;
let failed = 0;
let browser = null;
let adminSessionId = null;
let memberSessionId = null;
let fixture = null;

function check(condition, label, detail = '') {
  if (condition) {
    passed += 1;
    console.log(`PASS ${label}${detail ? ` — ${detail}` : ''}`);
  } else {
    failed += 1;
    console.error(`FAIL ${label}${detail ? ` — ${detail}` : ''}`);
  }
  return Boolean(condition);
}

function requireCheck(condition, label, detail = '') {
  check(condition, label, detail);
  if (!condition) throw new Error(`${label}${detail ? `: ${detail}` : ''}`);
}

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function runD1(sql, config = 'wrangler.sneak-admin.toml') {
  const npxCli = process.platform === 'win32'
    ? join(dirname(process.execPath), 'node_modules', 'npm', 'bin', 'npx-cli.js')
    : null;
  const command = npxCli && existsSync(npxCli) ? process.execPath : 'npx';
  const prefix = npxCli && existsSync(npxCli) ? [npxCli] : [];
  const result = spawnSync(command, [...prefix,
    'wrangler', 'd1', 'execute', 'sneak-idx-staging',
    '--config', config, '--remote', '--command', sql, '--json'
  ], { cwd: process.cwd(), encoding: 'utf8', windowsHide: true, shell: false });
  if (result.status !== 0) throw new Error((result.stderr || result.stdout || 'D1 command failed').trim());
  const start = result.stdout.indexOf('[');
  const payload = JSON.parse(result.stdout.slice(start));
  if (!payload.every(entry => entry.success)) throw new Error('D1 command returned an unsuccessful result.');
  return payload.flatMap(entry => entry.results || []);
}

function insertTemporarySession(table, prefix, fields) {
  const rawToken = crypto.randomBytes(32).toString('hex');
  const tokenHash = crypto.createHash('sha256').update(rawToken).digest('hex');
  const id = `${prefix}_${suffix}_${crypto.randomUUID().slice(0, 8)}`;
  const now = new Date().toISOString();
  const expires = new Date(Date.now() + 2 * 60 * 60 * 1000).toISOString();
  if (table === 'sneak_admin_sessions') {
    runD1(`INSERT INTO sneak_admin_sessions (id,token_hash,admin_actor,created_at,expires_at,last_seen_at,revoked_at,user_agent_hash,created_ip_hash) VALUES (${sqlString(id)},${sqlString(tokenHash)},'phase74a-browser-qa',${sqlString(now)},${sqlString(expires)},${sqlString(now)},NULL,'browser-qa','browser-qa')`);
  } else {
    runD1(`INSERT INTO sneak_member_sessions (id,user_id,account_id,token_hash,created_at,expires_at,last_seen_at,revoked_at) VALUES (${sqlString(id)},${sqlString(fields.userId)},${sqlString(fields.accountId)},${sqlString(tokenHash)},${sqlString(now)},${sqlString(expires)},${sqlString(now)},NULL)`, 'wrangler.sneak-member.toml');
  }
  return { id, rawToken };
}

function loadPlaywright() {
  const require = createRequire(import.meta.url);
  try { return require('playwright'); } catch {}
  const local = process.env.LOCALAPPDATA;
  const links = local ? join(local, 'ms-playwright', '.links') : '';
  if (links && existsSync(links)) {
    for (const name of readdirSync(links)) {
      const corePath = readFileSync(join(links, name), 'utf8').trim();
      const modulesRoot = dirname(corePath);
      const packagePath = join(modulesRoot, 'playwright');
      if (existsSync(join(packagePath, 'package.json'))) return require(packagePath);
    }
  }
  throw new Error('Playwright package was not found in the repo or local Playwright cache links.');
}

function chromeExecutable() {
  const candidates = [
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe'
  ];
  return candidates.find(existsSync) || null;
}

async function apiJson(path, session, options = {}) {
  const response = await fetch(SERVING + path, {
    ...options,
    headers: {
      Accept: 'application/json',
      Origin: SITE_ORIGIN,
      ...(session ? { 'X-SNEAK-Session': session } : {}),
      ...(options.headers || {})
    }
  });
  const data = await response.json().catch(() => ({}));
  return { response, data };
}

async function selectListingMatrix() {
  const bootstrap = await apiJson(`/idx/v1/bootstrap?site=${encodeURIComponent(SITE)}`);
  requireCheck(bootstrap.response.ok && bootstrap.data.session, 'browser tenant bootstrap', String(bootstrap.response.status));
  const matrix = [];
  const targets = { sale: 5, rental: 2, land: 2, commercial: 2 };

  for (const [propertyType, count] of Object.entries(targets)) {
    const search = await apiJson(`/idx/v1/search?site=${encodeURIComponent(SITE)}&propertyType=${propertyType}&limit=100&status=All&sort=newest`, bootstrap.data.session);
    const cards = Array.isArray(search.data?.data) ? search.data.data : [];
    if (propertyType === 'rental' && cards.length === 0) {
      check(true, 'rental browser sample unavailable in current tenant inventory', '0 current listings');
      continue;
    }
    requireCheck(search.response.ok && cards.length >= count, `${propertyType} browser sample available`, String(cards.length));
    const candidates = [];
    for (const card of cards.slice(0, propertyType === 'sale' ? 24 : 12)) {
      if (!card.ListingKey) continue;
      const key = String(card.ListingKey);
      const [detailResult, mediaResult] = await Promise.all([
        apiJson(`/idx/v1/listing/${encodeURIComponent(key)}?site=${encodeURIComponent(SITE)}`, bootstrap.data.session),
        apiJson(`/idx/v1/listing/${encodeURIComponent(key)}/media?site=${encodeURIComponent(SITE)}`, bootstrap.data.session)
      ]);
      const detail = detailResult.data?.data;
      const media = Array.isArray(mediaResult.data?.media) ? mediaResult.data.media : [];
      if (detailResult.response.ok && mediaResult.response.ok && detail?.ListingKey && media.length) {
        candidates.push({ propertyType, key, detail, media });
      }
      if (propertyType !== 'sale' && candidates.length >= count) break;
    }
    candidates.sort((a, b) => b.media.length - a.media.length);
    requireCheck(candidates.length >= count, `${propertyType} authoritative detail sample complete`, String(candidates.length));
    matrix.push(...candidates.slice(0, count));
  }
  const richCount = matrix.filter(item => item.media.length > 20).length;
  check(richCount >= 5, 'at least five browser samples have more than 20 photos', String(richCount));
  return { session: bootstrap.data.session, matrix };
}

async function documentFits(page) {
  return page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1);
}

async function runListingBrowserQa(playwright, executablePath, servingSession, matrix) {
  const context = await browser.newContext({ viewport: viewports[0], ignoreHTTPSErrors: false });
  await context.addInitScript(site => {
    localStorage.removeItem('ccor_compare_' + site);
    localStorage.removeItem('ccor_recently_viewed_' + site);
  }, SITE);
  const page = await context.newPage();
  const requests = [];
  const badImages = [];
  const pageErrors = [];
  page.on('request', request => requests.push({ url: request.url(), method: request.method(), type: request.resourceType(), at: Date.now() }));
  page.on('response', response => {
    if (response.request().resourceType() === 'image' && response.status() >= 400) badImages.push(`${response.status()} ${response.url()}`);
  });
  page.on('pageerror', error => pageErrors.push(error.message));

  const baseUrl = `${SERVING}/search/?site=${encodeURIComponent(SITE)}&propertyType=sale&session=${encodeURIComponent(servingSession)}`;
  await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForSelector('.listing-card', { timeout: 45000 });
  check(await page.getAttribute('body', 'data-ui-build') === BUILD, 'live consumer browser build marker', BUILD);

  const beforeCardRequests = requests.length;
  await page.locator('.listing-card').first().click();
  await page.waitForSelector('#detailOverlay', { state: 'visible' });
  await page.waitForFunction(() => document.querySelector('#detailListingInfoFacts')?.childElementCount > 0);
  check(true, 'search card enters authoritative detail modal');
  await page.click('#detailClose');
  const anonymousActivityPosts = requests.slice(beforeCardRequests).filter(item => item.method === 'POST' && item.url.includes('/api/consumer/activity'));
  check(anonymousActivityPosts.length === 0, 'anonymous detail view creates no server activity request');
  await page.waitForTimeout(500);
  const recentShape = await page.evaluate(site => {
    const value = JSON.parse(localStorage.getItem('ccor_recently_viewed_' + site) || '[]');
    return value.length > 0 && value.every(entry => entry && Object.keys(entry).sort().join(',') === 'key,viewedAt');
  }, SITE);
  check(recentShape, 'anonymous Recently Viewed stores only site-scoped key/timestamp records');

  for (const viewport of viewports) {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    for (let index = 0; index < matrix.length; index += 1) {
      const sample = matrix[index];
      const start = requests.length;
      await page.evaluate(key => window.openDetailByKey(key), sample.key);
      await page.waitForFunction(expected => {
        const facts = [...document.querySelectorAll('#detailListingInfoFacts dd')].map(node => node.textContent);
        return facts.some(value => value === expected);
      }, sample.detail.ListingId || sample.key, { timeout: 30000 });
      await page.waitForFunction(() => {
        const image = document.getElementById('detailMainImg');
        return image?.complete && image.naturalWidth > 0 && image.naturalHeight > 0;
      }, null, { timeout: 30000 });
      await page.waitForTimeout(250);

      const metrics = await page.evaluate(() => {
        const image = document.getElementById('detailMainImg');
        const stage = document.getElementById('detailGallery');
        const modal = document.querySelector('.detail-modal');
        const close = document.getElementById('detailClose');
        const thumbButtons = [...document.querySelectorAll('#detailThumbs .detail-thumb-button')];
        const tops = thumbButtons.map(button => Math.round(button.getBoundingClientRect().top));
        const facts = [...document.querySelectorAll('.detail-fact dd')].map(node => node.textContent.trim());
        return {
          objectFit: getComputedStyle(image).objectFit,
          imageComplete: image.complete && image.naturalWidth > 0,
          stageWidth: stage.getBoundingClientRect().width,
          stageHeight: stage.getBoundingClientRect().height,
          modal: modal.getBoundingClientRect().toJSON(),
          close: close.getBoundingClientRect().toJSON(),
          thumbs: thumbButtons.length,
          oneThumbRow: tops.every(top => Math.abs(top - tops[0]) <= 1),
          activeThumbs: document.querySelectorAll('#detailThumbs .detail-thumb.active').length,
          count: document.getElementById('detailImgCount').textContent.trim(),
          headings: [...document.querySelectorAll('.detail-section h3')].filter(node => !node.closest('section').hidden).map(node => node.textContent.trim()),
          facts,
          description: document.getElementById('detailDesc').textContent,
          scrollable: document.getElementById('detailScrollRegion').scrollHeight >= document.getElementById('detailScrollRegion').clientHeight,
          bodyWidth: document.documentElement.scrollWidth,
          viewportWidth: window.innerWidth
        };
      });

      const label = `${viewport.name} ${sample.propertyType} ${index + 1}`;
      check(metrics.objectFit === 'contain' && metrics.imageComplete, `${label} hero contained and loaded`);
      check(metrics.stageWidth > 0 && metrics.stageHeight > 0 && metrics.stageHeight < viewport.height, `${label} responsive neutral stage`);
      check(metrics.modal.x >= -1 && metrics.modal.x + metrics.modal.width <= viewport.width + 1 && metrics.modal.y >= -1 && metrics.modal.y + metrics.modal.height <= viewport.height + 1, `${label} modal fits viewport`);
      check(metrics.close.x >= 0 && metrics.close.y >= 0 && metrics.close.x + metrics.close.width <= viewport.width, `${label} close action remains accessible`);
      check(metrics.thumbs > 0 && metrics.thumbs <= 12 && metrics.oneThumbRow && metrics.activeThumbs === 1, `${label} bounded single-row thumbnail strip`, String(metrics.thumbs));
      check(metrics.count.endsWith(`/ ${sample.media.length}`), `${label} gallery count agrees with authoritative media`, metrics.count);
      check(['Property Overview', 'Location & Community', 'Listing Information', 'Description'].every(heading => metrics.headings.includes(heading)), `${label} rich detail hierarchy`);
      check(!metrics.facts.some(value => /(^|\s)0(?:\.0+)?\s*(acres?|sq\s*ft|garage)/i.test(value)), `${label} zero-value facts omitted`);
      if (sample.propertyType === 'land' || sample.propertyType === 'commercial') {
        check(!metrics.facts.some(value => /bed(room)?s?|bath(room)?s?/i.test(value)), `${label} no fake residential facts`);
      }
      check(metrics.description === String(sample.detail.PublicRemarks || 'No public remarks available.'), `${label} full authoritative remarks`);
      check(metrics.bodyWidth <= metrics.viewportWidth + 1, `${label} no page clipping or horizontal overflow`);

      const openRequests = requests.slice(start);
      const mediaSet = new Set(sample.media);
      const uniquePhotoRequests = new Set(openRequests.filter(item => mediaSet.has(item.url)).map(item => item.url));
      check(uniquePhotoRequests.size <= 13, `${label} initial photo downloads bounded`, String(uniquePhotoRequests.size));
      const detailAt = openRequests.find(item => item.url.includes(`/idx/v1/listing/${encodeURIComponent(sample.key)}?`))?.at;
      const mediaAt = openRequests.find(item => item.url.includes(`/idx/v1/listing/${encodeURIComponent(sample.key)}/media?`))?.at;
      const activityAt = openRequests.find(item => item.url.includes('/api/consumer/activity'))?.at;
      check(Boolean(detailAt && mediaAt) && (!activityAt || (detailAt <= activityAt && mediaAt <= activityAt)), `${label} primary detail/media requests start before activity`);

      if (sample.media.length > 1) {
        const before = metrics.count;
        await page.click('#detailNextBtn');
        await page.waitForFunction(previous => document.getElementById('detailImgCount').textContent.trim() !== previous, before);
        check(true, `${label} next-photo navigation`);
      }
      if (index === 0) {
        await page.screenshot({ path: join(screenshotDir, `listing-${viewport.name}.png`), fullPage: false });
      }
      await page.click('#detailClose');
    }
  }

  const [first, second] = matrix;
  await page.evaluate(({ a, b }) => { window.openDetailByKey(a); window.openDetailByKey(b); }, { a: first.key, b: second.key });
  await page.waitForFunction(expected => [...document.querySelectorAll('#detailListingInfoFacts dd')].some(node => node.textContent === expected), second.detail.ListingId || second.key);
  await page.waitForTimeout(750);
  check(await page.locator('#detailListingInfoFacts').innerText().then(text => text.includes(second.detail.ListingId || second.key)), 'rapid A-to-B selection leaves B authoritative');

  await page.click('#detailCompareBtn');
  await page.click('#detailClose');
  await page.evaluate(key => window.openDetailByKey(key), matrix[2].key);
  await page.waitForFunction(expected => [...document.querySelectorAll('#detailListingInfoFacts dd')].some(node => node.textContent === expected), matrix[2].detail.ListingId || matrix[2].key);
  await page.click('#detailCompareBtn');
  await page.click('#detailClose');
  check(await page.locator('#compareTray').isVisible(), 'anonymous Compare tray is visible after two selections');
  await page.click('#compareTrayOpen');
  check(await page.locator('#compareModal').isVisible() && await page.locator('.compare-table').count() === 1, 'anonymous contextual Compare modal renders current summaries');
  const compareShape = await page.evaluate(site => {
    const value = JSON.parse(localStorage.getItem('ccor_compare_' + site) || '[]');
    return value.length === 2 && value.every(entry => typeof entry === 'string');
  }, SITE);
  check(compareShape, 'anonymous Compare stores site-scoped listing keys only');

  check(!requests.some(item => /api\.bridgedata|bridgedataoutput/i.test(item.url)), 'consumer browser traffic makes no Bridge call');
  check(badImages.length === 0, 'consumer browser has no failed image responses', badImages.slice(0, 2).join('; '));
  check(pageErrors.length === 0, 'consumer browser has no uncaught page errors', pageErrors.slice(0, 2).join('; '));
  await context.close();
}

async function runAdminBrowserQa(adminToken) {
  const context = await browser.newContext({ viewport: viewports[0] });
  await context.addCookies([{ name: '__Host-sneak_admin_session', value: adminToken, url: ADMIN, httpOnly: true, secure: true, sameSite: 'Strict' }]);
  const storedCookie = (await context.cookies(ADMIN)).find(cookie => cookie.name === '__Host-sneak_admin_session');
  requireCheck(Boolean(storedCookie?.value && storedCookie.secure && storedCookie.httpOnly), 'Admin browser cookie stored as secure HttpOnly host cookie');
  await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: ADMIN });
  const page = await context.newPage();
  const apiFailures = [];
  page.on('response', response => {
    if (response.url().includes('/api/admin/') && response.status() >= 400) apiFailures.push(`${response.status()} ${response.url()}`);
  });
  await page.goto(ADMIN, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForFunction(() => document.querySelector('.grid-stats') || document.getElementById('adminPassword'), null, { timeout: 30000 });
  if (await page.locator('#adminPassword').count()) {
    await page.screenshot({ path: join(screenshotDir, 'admin-auth-failure.png'), fullPage: false });
  }
  requireCheck(await page.locator('#adminPassword').count() === 0, 'Admin browser session accepted by UI');
  await page.waitForSelector('.grid-stats');
  check(await page.getByText('Total Accounts').isVisible(), 'authenticated Admin dashboard browser');
  await page.getByText('+ Provision Member').click();
  await page.waitForSelector('#onboardModal');

  const accountName = `Phase 74A Browser QA ${suffix}`;
  const memberEmail = `phase74a-${suffix}@example.com`;
  const siteKey = `phase74a-browser-${suffix}`.replace(/[^a-z0-9-]/g, '').slice(0, 50);
  await page.fill('#obAccountName', accountName);
  await page.fill('#obMemberEmail', memberEmail);
  await page.fill('#obMemberId', `QA-${suffix}`);
  await page.fill('#obAgentMls', 'B3650316');
  await page.selectOption('#obPlan', 'pro');
  await page.selectOption('#obEntSource', 'growthzone');
  await page.fill('#obReference', `GZ-BROWSER-${suffix}`);
  await page.fill('#obSiteName', accountName);
  await page.fill('#obSiteKey', siteKey);
  await page.selectOption('#obScopeType', 'market');
  await page.fill('#obDomain', `qa-${suffix}.example.com`);
  await page.fill('#obBrokerage', 'CCOR Browser QA Realty');
  await page.fill('#obPhone', '239-555-0100');
  await page.fill('#obBrandEmail', memberEmail);
  await page.getByRole('button', { name: 'Create & Continue', exact: true }).click();
  await page.waitForFunction(name => document.querySelector('#app h2')?.textContent === name, accountName, { timeout: 45000 });

  const accountText = await page.locator('#app').innerText();
  const accountId = accountText.match(/Account\s+(acc_[A-Za-z0-9_-]+)/)?.[1];
  requireCheck(/^acc_[A-Za-z0-9_-]+$/.test(accountId || ''), 'guided Admin fixture account created');
  const accountRows = runD1(`SELECT id AS account_id FROM sneak_accounts WHERE id=${sqlString(accountId)}; SELECT id AS site_id FROM sneak_sites WHERE account_id=${sqlString(accountId)} LIMIT 1; SELECT id AS member_user_id FROM sneak_member_users WHERE account_id=${sqlString(accountId)} LIMIT 1`);
  fixture = {
    accountId,
    siteId: accountRows.find(row => row.site_id)?.site_id,
    memberUserId: accountRows.find(row => row.member_user_id)?.member_user_id,
    accountName,
    memberEmail,
    siteKey
  };
  requireCheck(Boolean(fixture.siteId && fixture.memberUserId), 'guided Admin fixture persisted site and member');

  for (const heading of ['Account', 'Entitlement', 'Member Users', 'Client / Lead Summary', 'IDX Site', 'Domains', 'Branding', 'Responsive Embed', 'Readiness Checklist', 'Audit History']) {
    check(await page.getByRole('heading', { name: heading, exact: true }).isVisible(), `Admin account detail ${heading}`);
  }
  const domainsSection = page.locator('section').filter({ has: page.getByRole('heading', { name: 'Domains', exact: true }) });
  await domainsSection.getByRole('button', { name: 'Authorize' }).click();
  await page.waitForFunction(() => document.querySelector('#adminToast')?.textContent.includes('active and verified'));
  await page.getByRole('button', { name: 'Refresh checks' }).click();
  await page.waitForTimeout(500);
  check(await page.locator('#app').innerText().then(text => text.includes('READY TO LAUNCH')), 'Admin readiness reaches READY TO LAUNCH after domain authorization');

  await page.fill('#entReference', `GZ-BROWSER-VERIFIED-${suffix}`);
  await page.getByRole('button', { name: 'Save Entitlement' }).click();
  await page.waitForFunction(() => document.querySelector('#adminToast')?.textContent.includes('Entitlement saved'));
  await page.fill('#brandPhone', '239-555-0199');
  await page.getByRole('button', { name: 'Save Branding' }).click();
  await page.waitForFunction(() => document.querySelector('#adminToast')?.textContent.includes('Branding saved'));
  await page.getByRole('button', { name: 'Copy Embed Code' }).click();
  check(await page.locator('#embedSnippet').innerText().then(text => text.includes(siteKey)), 'Admin responsive embed contains fixture site key');

  await page.getByText('Accounts', { exact: true }).first().click();
  await page.waitForSelector('#accountSearch');
  await page.fill('#accountSearch', accountName);
  const filteredResponse = page.waitForResponse(response => {
    const url = new URL(response.url());
    return response.request().method() === 'GET' && url.pathname === '/api/admin/accounts' && url.searchParams.get('q') === accountName;
  });
  await page.locator('form.filters').first().getByRole('button', { name: 'Search' }).click();
  await filteredResponse;
  await page.waitForFunction(name => document.querySelector('tbody')?.textContent.includes(name) && document.querySelectorAll('tbody tr').length === 1, accountName);
  const fixtureRow = page.getByRole('row').filter({ hasText: accountName });
  check(await fixtureRow.count() === 1, 'Admin account search isolates fixture row');
  await fixtureRow.getByRole('button', { name: 'Open' }).click();
  await page.waitForFunction(name => document.querySelector('#app h2')?.textContent === name, accountName);

  await page.getByRole('button', { name: 'Suspend', exact: true }).click();
  const suspendLifecycle = page.waitForResponse(response => response.request().method() === 'POST' && new URL(response.url()).pathname === `/api/admin/accounts/${accountId}/lifecycle`);
  const suspendRefresh = page.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname === `/api/admin/accounts/${accountId}`);
  await page.click('#impactConfirm');
  await Promise.all([suspendLifecycle, suspendRefresh]);
  await page.waitForFunction(() => document.querySelector('#app')?.textContent.includes('ACCOUNT_INACTIVE'));
  check(true, 'Admin suspend lifecycle blocks serving without deleting fixture');
  await page.getByRole('button', { name: 'Reactivate', exact: true }).click();
  const reactivateLifecycle = page.waitForResponse(response => response.request().method() === 'POST' && new URL(response.url()).pathname === `/api/admin/accounts/${accountId}/lifecycle`);
  const reactivateRefresh = page.waitForResponse(response => response.request().method() === 'GET' && new URL(response.url()).pathname === `/api/admin/accounts/${accountId}`);
  await page.click('#impactConfirm');
  await Promise.all([reactivateLifecycle, reactivateRefresh]);
  await page.waitForFunction(() => document.querySelector('#app')?.textContent.includes('READY TO LAUNCH') && document.querySelector('#app .badge-active')?.textContent.toLowerCase().includes('active'));
  check(true, 'Admin reactivate lifecycle restores same fixture');

  await page.getByText('Launch Readiness', { exact: true }).click();
  await page.waitForFunction(() => document.querySelector('#app')?.textContent.includes('Launch Readiness Control Plane'));
  check(await page.locator('#app').innerText().then(text => text.includes('Core capability') && text.includes('Optional capability')), 'Admin global readiness separates core and optional capabilities');

  for (const viewport of viewports) {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    check(await documentFits(page), `Admin ${viewport.name} no clipping or page overflow`);
    check(await page.getByText('Launch Readiness Control Plane').isVisible(), `Admin ${viewport.name} readiness content visible`);
    await page.screenshot({ path: join(screenshotDir, `admin-${viewport.name}.png`), fullPage: false });
  }
  check(apiFailures.length === 0, 'Admin browser protected APIs return no errors', apiFailures.slice(0, 2).join('; '));
  await context.close();
}

function seedMemberClientFixture(listingSamples) {
  const now = new Date().toISOString();
  const consumerId = `cuser_browser_${suffix}`.replaceAll('-', '_');
  const favoriteId = `cfav_browser_${suffix}`.replaceAll('-', '_');
  const searchId = `css_browser_${suffix}`.replaceAll('-', '_');
  const alertId = `calert_browser_${suffix}`.replaceAll('-', '_');
  const leadId = `lead_browser_${suffix}`.replaceAll('-', '_');
  const email = `buyer-${suffix}@example.com`;
  const listingA = listingSamples[0].key;
  const listingB = listingSamples[1].key;
  const state = JSON.stringify({ version: 1, propertyType: 'sale', filters: { city: 'Bonita Springs', waterfront: true } });
  runD1(`UPDATE sneak_member_users SET status='active',activated_at=${sqlString(now)},updated_at=${sqlString(now)} WHERE id=${sqlString(fixture.memberUserId)}`, 'wrangler.sneak-member.toml');
  runD1(`INSERT INTO sneak_consumer_users (id,site_id,email,status,created_at,activated_at,last_login_at,updated_at,last_activity_at) VALUES (${sqlString(consumerId)},${sqlString(fixture.siteId)},${sqlString(email)},'active',${sqlString(now)},${sqlString(now)},${sqlString(now)},${sqlString(now)},${sqlString(now)})`, 'wrangler.sneak-member.toml');
  runD1(`INSERT INTO sneak_consumer_favorites (id,site_id,user_id,listing_key,created_at) VALUES (${sqlString(favoriteId)},${sqlString(fixture.siteId)},${sqlString(consumerId)},${sqlString(listingA)},${sqlString(now)}); INSERT INTO sneak_consumer_favorites (id,site_id,user_id,listing_key,created_at) VALUES (${sqlString(favoriteId + '_2')},${sqlString(fixture.siteId)},${sqlString(consumerId)},${sqlString(listingB)},${sqlString(now)})`, 'wrangler.sneak-member.toml');
  runD1(`INSERT INTO sneak_consumer_saved_searches (id,site_id,user_id,name,state_version,state_json,state_hash,created_at,updated_at) VALUES (${sqlString(searchId)},${sqlString(fixture.siteId)},${sqlString(consumerId)},'Browser QA Waterfront Homes',1,${sqlString(state)},${sqlString(crypto.createHash('sha256').update(state).digest('hex'))},${sqlString(now)},${sqlString(now)})`, 'wrangler.sneak-member.toml');
  runD1(`INSERT INTO sneak_consumer_search_alerts (id,saved_search_id,site_id,user_id,frequency,enabled,enabled_at,timezone,created_at,updated_at) VALUES (${sqlString(alertId)},${sqlString(searchId)},${sqlString(fixture.siteId)},${sqlString(consumerId)},'daily',1,${sqlString(now)},'America/New_York',${sqlString(now)},${sqlString(now)})`, 'wrangler.sneak-member.toml');
  runD1(`INSERT INTO sneak_leads (id,site_id,listing_key,lead_type,name,email,phone,message,source_url,created_at) VALUES (${sqlString(leadId)},${sqlString(fixture.siteId)},${sqlString(listingA)},'property_inquiry','Browser QA Buyer',${sqlString(email)},'239-555-0111','Please schedule a browser QA showing.','https://example.com/qa',${sqlString(now)})`, 'wrangler.sneak-member.toml');
  const activities = [
    ['listing_view', listingA, null, null, null],
    ['favorite_added', listingA, null, null, null],
    ['saved_search_created', null, searchId, null, JSON.stringify({ name: 'Browser QA Waterfront Homes' })],
    ['alert_enabled', null, searchId, null, JSON.stringify({ frequency: 'daily' })],
    ['inquiry_submitted', listingA, null, leadId, null]
  ];
  for (const [index, activity] of activities.entries()) {
    const [type, listingKey, savedSearchId, activityLeadId, metadata] = activity;
    runD1(`INSERT INTO sneak_consumer_activity_events (id,site_id,user_id,event_type,listing_key,saved_search_id,lead_id,metadata_json,dedupe_key,created_at) VALUES (${sqlString(`cact_browser_${suffix}_${index}`)},${sqlString(fixture.siteId)},${sqlString(consumerId)},${sqlString(type)},${listingKey ? sqlString(listingKey) : 'NULL'},${savedSearchId ? sqlString(savedSearchId) : 'NULL'},${activityLeadId ? sqlString(activityLeadId) : 'NULL'},${metadata ? sqlString(metadata) : 'NULL'},NULL,${sqlString(new Date(Date.now() - index * 60000).toISOString())})`, 'wrangler.sneak-member.toml');
  }
  fixture.consumerId = consumerId;
  fixture.consumerEmail = email;
}

async function runMemberBrowserQa(memberToken) {
  const context = await browser.newContext({ viewport: viewports[0] });
  await context.addCookies([{ name: '__Host-sneak_member_session', value: memberToken, url: MEMBER, httpOnly: true, secure: true, sameSite: 'Lax' }]);
  const page = await context.newPage();
  const apiFailures = [];
  page.on('response', response => {
    if (response.url().includes('/api/member/') && response.status() >= 400) apiFailures.push(`${response.status()} ${response.url()}`);
  });
  await page.goto(MEMBER, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForFunction(() => document.getElementById('appContainer')?.style.display !== 'none' && document.getElementById('statClients')?.textContent !== '-');
  check(await page.getByText('Registered Clients').isVisible(), 'authenticated Member overview browser');

  for (const viewport of viewports) {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.getByText('Clients', { exact: true }).click();
    await page.waitForFunction(email => document.getElementById('clientsTableBody')?.textContent.includes(email), fixture.consumerEmail);
    const kpiVisibility = await Promise.all(['clientKpiTotal', 'clientKpiActive7d', 'clientKpiSavedHomes', 'clientKpiSavedSearches'].map(id => page.locator('#' + id).isVisible()));
    check(kpiVisibility.every(Boolean), `Member ${viewport.name} KPI cards present`);
    await page.fill('#clientSearchInput', fixture.consumerEmail);
    await page.waitForTimeout(500);
    await page.waitForFunction(email => document.getElementById('clientsTableBody')?.textContent.includes(email), fixture.consumerEmail);
    check(await page.locator('#clientsTableBody tr').count() === 1, `Member ${viewport.name} client email search`);
    await page.selectOption('#clientSortSelect', 'saved_homes');
    await page.waitForFunction(email => document.getElementById('clientsTableBody')?.textContent.includes(email), fixture.consumerEmail);
    check(await page.locator('#clientSortSelect').inputValue() === 'saved_homes', `Member ${viewport.name} client sort`);
    await page.getByRole('button', { name: 'View Activity' }).click();
    await page.waitForFunction(email => document.getElementById('clientDetailEmail')?.textContent === email, fixture.consumerEmail);
    check(await page.locator('#clientActivityTimeline .timeline-item').count() >= 5, `Member ${viewport.name} Activity Timeline`);
    await page.getByRole('button', { name: 'Saved Homes', exact: true }).click();
    check(await page.locator('#clientSavedHomesGrid .property-grid-card').count() === 2, `Member ${viewport.name} Saved Homes`);
    await page.getByRole('button', { name: 'Saved Searches & Alerts', exact: true }).click();
    check(await page.locator('#clientSavedSearchesList').innerText().then(text => text.includes('Browser QA Waterfront Homes') && text.includes('DAILY')), `Member ${viewport.name} Saved Searches`);
    await page.getByRole('button', { name: 'Inquiries & Leads', exact: true }).click();
    check(await page.locator('#clientInquiriesList').innerText().then(text => text.includes('Please schedule a browser QA showing.')), `Member ${viewport.name} Inquiries`);
    check(await documentFits(page), `Member ${viewport.name} no page clipping or horizontal overflow`);
    const modalFits = await page.evaluate(() => {
      const rect = document.querySelector('#clientDetailModal .modal-card').getBoundingClientRect();
      return rect.left >= -1 && rect.right <= innerWidth + 1 && rect.top >= -1 && rect.bottom <= innerHeight + 1;
    });
    check(modalFits, `Member ${viewport.name} Client Detail fits viewport`);
    const modalContentFits = await page.evaluate(() => {
      const body = document.querySelector('#clientDetailModal .modal-body').getBoundingClientRect();
      const visible = [
        ...document.querySelectorAll('#clientDetailModal .stat-card'),
        document.querySelector('#clientDetailModal .subtab-nav'),
        ...document.querySelectorAll('#clientDetailModal .client-subtab-pane')
      ].filter(node => node && getComputedStyle(node).display !== 'none');
      return visible.every(node => {
        const rect = node.getBoundingClientRect();
        return rect.left >= body.left - 1 && rect.right <= body.right + 1;
      });
    });
    check(modalContentFits, `Member ${viewport.name} Client Detail content is not horizontally clipped`);
    await page.screenshot({ path: join(screenshotDir, `member-${viewport.name}.png`), fullPage: false });
    await page.locator('.modal-close-btn').click();
    await page.fill('#clientSearchInput', '');
    await page.waitForTimeout(350);
  }
  check(apiFailures.length === 0, 'Member browser protected APIs return no errors', apiFailures.slice(0, 2).join('; '));
  await context.close();
}

function cleanup() {
  if (memberSessionId) {
    try { runD1(`DELETE FROM sneak_member_sessions WHERE id=${sqlString(memberSessionId)}`, 'wrangler.sneak-member.toml'); } catch (error) { check(false, 'temporary Member session cleanup', error.message); }
  }
  if (fixture?.accountId && /^acc_[A-Za-z0-9_-]+$/.test(fixture.accountId)) {
    const id = sqlString(fixture.accountId);
    const statements = [
      `DELETE FROM sneak_member_sessions WHERE account_id=${id}`,
      `DELETE FROM sneak_member_audit WHERE account_id=${id}`,
      `DELETE FROM sneak_admin_audit WHERE entity_id=${id} OR entity_id IN (SELECT id FROM sneak_sites WHERE account_id=${id}) OR entity_id IN (SELECT id FROM sneak_member_users WHERE account_id=${id}) OR entity_id IN (SELECT d.id FROM sneak_domains d JOIN sneak_sites s ON s.id=d.site_id WHERE s.account_id=${id})`,
      `DELETE FROM sneak_account_entitlements WHERE account_id=${id}`,
      `DELETE FROM sneak_accounts WHERE id=${id}`
    ];
    try {
      for (const statement of statements) runD1(statement);
      check(true, 'isolated Admin/Member browser fixture cleanup');
    } catch (error) { check(false, 'isolated Admin/Member browser fixture cleanup', error.message); }
  }
  if (adminSessionId) {
    try { runD1(`DELETE FROM sneak_admin_sessions WHERE id=${sqlString(adminSessionId)}`); check(true, 'temporary Admin session cleanup'); }
    catch (error) { check(false, 'temporary Admin session cleanup', error.message); }
  }
}

try {
  const playwright = loadPlaywright();
  const executablePath = chromeExecutable();
  requireCheck(Boolean(executablePath), 'local Chrome or Edge executable available', executablePath || 'missing');
  browser = await playwright.chromium.launch({ headless: true, executablePath });
  check(true, 'actual local Chromium browser launched', executablePath);

  const { session: servingSession, matrix } = await selectListingMatrix();
  console.log(`SAMPLE ${JSON.stringify(matrix.map(item => ({ category: item.propertyType, listingKey: item.key, photos: item.media.length })))}`);
  if (!SKIP_LISTING_BROWSER) await runListingBrowserQa(playwright, executablePath, servingSession, matrix);

  const adminSession = insertTemporarySession('sneak_admin_sessions', 'sess_browser', {});
  adminSessionId = adminSession.id;
  const directAdmin = await fetch(`${ADMIN}/api/admin/dashboard`, { headers: { Accept: 'application/json', Cookie: `__Host-sneak_admin_session=${adminSession.rawToken}` } });
  requireCheck(directAdmin.status === 200, 'temporary Admin session accepted by protected API', String(directAdmin.status));
  await runAdminBrowserQa(adminSession.rawToken);

  seedMemberClientFixture(matrix);
  const memberSession = insertTemporarySession('sneak_member_sessions', 'msess_browser', { userId: fixture.memberUserId, accountId: fixture.accountId });
  memberSessionId = memberSession.id;
  await runMemberBrowserQa(memberSession.rawToken);
} catch (error) {
  check(false, 'browser QA harness completed', error.stack || error.message);
} finally {
  if (browser) await browser.close().catch(() => {});
  cleanup();
}

console.log(`SCREENSHOTS ${screenshotDir}`);
console.log(`RESULT ${passed}/${passed + failed} PASS`);
process.exitCode = failed ? 1 : 0;
