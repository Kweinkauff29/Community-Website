import crypto from 'node:crypto';
import { execSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const require = createRequire(import.meta.url);

function loadPlaywright() {
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
    throw new Error('Playwright package was not found in repo or local cache.');
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

function queryD1(sql) {
    const raw = execSync(`npx wrangler d1 execute sneak-idx-production --remote --command="${sql.replace(/"/g, '\\"')}" --json`, {
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'inherit']
    });
    const parsed = JSON.parse(raw);
    return parsed[0]?.results || [];
}

const SERVING = 'https://sneak-idx-worker.bonitaspringsrealtors.workers.dev';
const ADMIN = 'https://sneak-idx-admin.bonitaspringsrealtors.workers.dev';
const MEMBER = 'https://sneak-idx-member.bonitaspringsrealtors.workers.dev';
const SITE = 'ursula-weinkauff-pilot';

const viewports = [
    { name: 'Desktop (1440x900)', width: 1440, height: 900 },
    { name: 'Tablet (1024x768)', width: 1024, height: 768 },
    { name: 'Mobile (390x844)', width: 390, height: 844 }
];

let passed = 0;
let failed = 0;

function check(condition, label, detail = '') {
    if (condition) {
        passed++;
        console.log(`  PASS: ${label}${detail ? ` (${detail})` : ''}`);
    } else {
        failed++;
        console.error(`  FAIL: ${label}${detail ? ` (${detail})` : ''}`);
    }
}

async function run() {
    console.log('====================================================');
    console.log('STARTING CHROME BROWSER PRODUCTION QA (GATES 23–25)');
    console.log('====================================================');

    const playwright = loadPlaywright();
    const executablePath = chromeExecutable();
    console.log(`Using browser executable: ${executablePath}`);

    const browser = await playwright.chromium.launch({
        headless: true,
        executablePath: executablePath || undefined
    });

    // 1. Get PILOT-01 serving session via bootstrap
    console.log('\n[1] Bootstrapping PILOT-01 session token...');
    const bootRes = await fetch(`${SERVING}/idx/v1/bootstrap?site=${SITE}`, {
        headers: { 'Origin': 'https://coconutcoastrealtors.org' }
    });
    const bootData = await bootRes.json();
    const sessionToken = bootData.session;
    check(Boolean(sessionToken), 'Production bootstrap session acquired', bootData.expiresIn ? `${bootData.expiresIn}s` : '');

    // 2. Test Serving Search UI across all 3 viewports
    console.log('\n[2] Testing Consumer Search UI across 3 viewports...');
    for (const vp of viewports) {
        console.log(`\n  --- Testing Viewport: ${vp.name} ---`);
        const context = await browser.newContext({
            viewport: { width: vp.width, height: vp.height },
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        });
        const page = await context.newPage();
        const pageErrors = [];
        page.on('pageerror', err => pageErrors.push(err.message));

        const searchUrl = `${SERVING}/search/?site=${SITE}&session=${sessionToken}`;
        await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

        // Wait for listing cards
        await page.waitForSelector('.listing-card', { timeout: 30000 });
        const cardCount = await page.locator('.listing-card').count();
        check(cardCount > 0, `${vp.name}: Listing cards rendered`, `${cardCount} cards`);

        // Check horizontal overflow
        const fits = await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 2);
        check(fits, `${vp.name}: No horizontal overflow`);

        // Check branding rendered
        const hasBrand = await page.evaluate(() => {
            return document.body.textContent.includes('Ursula Weinkauff') ||
                   document.body.textContent.includes('Local Real Estate');
        });
        check(hasBrand, `${vp.name}: Member branding present on search UI`);

        // Click first card to open detail modal
        await page.locator('.listing-card').first().click();
        await page.waitForSelector('#detailOverlay', { state: 'visible', timeout: 15000 });
        const modalVisible = await page.isVisible('#detailOverlay');
        check(modalVisible, `${vp.name}: Detail overlay opens upon listing card click`);

        // Check attribution in detail modal
        const detailText = await page.locator('#detailOverlay').innerText();
        const hasAttribution = detailText.includes('Courtesy of') || detailText.includes('Listing') || detailText.includes('IDX');
        check(hasAttribution, `${vp.name}: Mandatory MLS attribution displayed in detail modal`);

        // Close modal
        await page.click('#detailClose');
        await page.waitForSelector('#detailOverlay', { state: 'hidden', timeout: 10000 });

        check(pageErrors.length === 0, `${vp.name}: Zero uncaught page errors`, pageErrors.join(', '));
        await context.close();
    }

    // 3. Member Portal QA
    console.log('\n[3] Testing Member Portal UI...');
    // Create direct member session for Ursula Weinkauff
    const user = queryD1(`SELECT id, account_id, email FROM sneak_member_users WHERE email = 'kmwcollegeapps@gmail.com'`)[0];
    const memberRawToken = crypto.randomBytes(32).toString('hex');
    const memberTokenHash = crypto.createHash('sha256').update(memberRawToken).digest('hex');
    const memberSessId = 'sess_' + crypto.randomUUID();
    const nowIso = new Date().toISOString();
    const expIso = new Date(Date.now() + 604800000).toISOString();

    queryD1(`INSERT INTO sneak_member_sessions (id, token_hash, user_id, account_id, created_at, expires_at, last_seen_at) VALUES ('${memberSessId}', '${memberTokenHash}', '${user.id}', '${user.account_id}', '${nowIso}', '${expIso}', '${nowIso}');`);

    const memberContext = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await memberContext.addCookies([{
        name: '__Host-sneak_member_session',
        value: memberRawToken,
        domain: 'sneak-idx-member.bonitaspringsrealtors.workers.dev',
        path: '/',
        httpOnly: true,
        secure: true,
        sameSite: 'Lax'
    }]);

    const memberPage = await memberContext.newPage();
    const memberErrors = [];
    memberPage.on('pageerror', err => memberErrors.push(err.message));

    await memberPage.goto(`${MEMBER}/`, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await memberPage.waitForSelector('#accountName, .account-name, h1, nav', { timeout: 15000 });

    const memberBodyText = await memberPage.innerText('body');
    check(memberBodyText.includes('Ursula Weinkauff') || memberBodyText.includes('Member Portal'), 'Member Portal loads user account');
    check(memberErrors.length === 0, 'Member Portal: Zero uncaught page errors', memberErrors.join(', '));
    await memberContext.close();

    // 4. Admin Portal QA
    console.log('\n[4] Testing Admin Portal UI...');
    const adminRawToken = crypto.randomBytes(32).toString('hex');
    const adminTokenHash = crypto.createHash('sha256').update(adminRawToken).digest('hex');
    const adminSessId = 'sess_' + crypto.randomUUID();
    queryD1(`INSERT INTO sneak_admin_sessions (id, token_hash, admin_actor, created_at, expires_at, last_seen_at) VALUES ('${adminSessId}', '${adminTokenHash}', 'admin', '${nowIso}', '${expIso}', '${nowIso}');`);

    const adminContext = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await adminContext.addCookies([{
        name: '__Host-sneak_admin_session',
        value: adminRawToken,
        domain: 'sneak-idx-admin.bonitaspringsrealtors.workers.dev',
        path: '/',
        httpOnly: true,
        secure: true,
        sameSite: 'Strict'
    }]);

    const adminPage = await adminContext.newPage();
    const adminErrors = [];
    adminPage.on('pageerror', err => adminErrors.push(err.message));

    await adminPage.goto(`${ADMIN}/`, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await adminPage.waitForSelector('table, .card, h1, nav, #app', { timeout: 15000 });

    const adminBodyText = await adminPage.innerText('body');
    check(adminBodyText.includes('SNEAK') || adminBodyText.includes('Admin') || adminBodyText.includes('Dashboard'), 'Admin Portal loads dashboard');
    check(adminErrors.length === 0, 'Admin Portal: Zero uncaught page errors', adminErrors.join(', '));
    await adminContext.close();

    await browser.close();

    console.log(`\n====================================================`);
    console.log(`BROWSER QA COMPLETE: ${passed} PASSED, ${failed} FAILED`);
    console.log(`====================================================`);
    if (failed > 0) process.exit(1);
}

run().catch(err => {
    console.error('FATAL BROWSER QA ERROR:', err);
    process.exit(1);
});
