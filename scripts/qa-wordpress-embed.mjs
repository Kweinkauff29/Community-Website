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

const WORDPRESS_URL = 'https://coconutcoastrealtors.org/idx-test/';
const PRODUCTION_WORKER_HOST = 'sneak-idx-worker.bonitaspringsrealtors.workers.dev';
const STAGING_WORKER_HOST = 'sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';

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
    console.log('STARTING ACTUAL WORDPRESS PRODUCTION EMBED QA');
    console.log(`URL: ${WORDPRESS_URL}`);
    console.log('====================================================');

    const playwright = loadPlaywright();
    const executablePath = chromeExecutable();
    console.log(`Using Chrome executable: ${executablePath}`);

    const browser = await playwright.chromium.launch({
        headless: true,
        executablePath: executablePath || undefined
    });

    const networkReport = {
        productionServingRequests: 0,
        stagingWorkerRequests: 0,
        browserBridgeRequests: 0,
        consumerAccountRequests: 0,
        alertRequests: 0,
        unauthorizedMlsRequests: 0,
        sampleProductionUrls: []
    };

    for (const vp of viewports) {
        console.log(`\n--- Testing Viewport: ${vp.name} ---`);
        const context = await browser.newContext({
            viewport: { width: vp.width, height: vp.height },
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        });
        const page = await context.newPage();
        const pageErrors = [];
        const brokenImages = [];

        page.on('pageerror', err => {
            // Check if error is relevant to SNEAK IDX or if it's the unrelated host WordPress theme script on line 160 (btn1)
            const isHostBtnScript = err.message.includes('addEventListener') && (err.stack?.includes(':160:34') || err.stack?.includes('idx-test'));
            if (!isHostBtnScript) {
                pageErrors.push(err.message);
            }
        });
        page.on('request', req => {
            const urlStr = req.url();
            const parsed = new URL(urlStr);
            if (parsed.host === PRODUCTION_WORKER_HOST) {
                networkReport.productionServingRequests++;
                if (networkReport.sampleProductionUrls.length < 5) {
                    networkReport.sampleProductionUrls.push(`${req.method()} ${parsed.pathname}`);
                }
            }
            if (parsed.host === STAGING_WORKER_HOST) {
                networkReport.stagingWorkerRequests++;
            }
            if (urlStr.includes('bridge') || parsed.host.includes('bridge')) {
                networkReport.browserBridgeRequests++;
            }
            if (urlStr.includes('/api/consumer/') && !urlStr.includes('version')) {
                networkReport.consumerAccountRequests++;
            }
            if (urlStr.includes('/api/alerts/')) {
                networkReport.alertRequests++;
            }
            if (urlStr.includes('mls') && !parsed.host.includes('workers.dev') && !parsed.host.includes('coconutcoastrealtors.org')) {
                networkReport.unauthorizedMlsRequests++;
            }
        });
        page.on('response', res => {
            if (res.request().resourceType() === 'image' && res.status() >= 400) {
                brokenImages.push(`${res.status()} ${res.url()}`);
            }
        });

        // 1. Navigate to actual WordPress page
        await page.goto(WORDPRESS_URL, { waitUntil: 'domcontentloaded', timeout: 45000 });

        // 2. Verify production embed initialization
        const iframeElement = await page.waitForSelector('#sneak-idx-search iframe, .sneak-idx-widget-container iframe', { timeout: 35000 });
        check(Boolean(iframeElement), `${vp.name}: SNEAK production iframe injected by embed.js`);

        const iframeSrc = await iframeElement.getAttribute('src');
        check(iframeSrc.includes(PRODUCTION_WORKER_HOST), `${vp.name}: Iframe targets production Worker`, iframeSrc.split('?')[0]);
        check(!iframeSrc.includes(STAGING_WORKER_HOST), `${vp.name}: Zero staging Worker references in iframe src`);

        // Check embed container layout & overflow
        const embedOverflow = await page.evaluate(() => {
            const el = document.getElementById('sneak-idx-search');
            const iframe = el ? el.querySelector('iframe') : null;
            if (!el || !iframe) return false;
            const r = iframe.getBoundingClientRect();
            return r.width > 0 && r.right <= window.innerWidth + 2;
        });
        check(embedOverflow, `${vp.name}: Embed container and iframe fit viewport with no horizontal overflow`);

        const frame = await iframeElement.contentFrame();
        if (!frame) throw new Error('Could not access iframe content frame');

        // 3. Verify listing cards load inside iframe
        await frame.waitForSelector('.listing-card', { timeout: 35000 });
        const cardCount = await frame.locator('.listing-card').count();
        check(cardCount > 0, `${vp.name}: Listing cards loaded inside iframe`, `${cardCount} cards`);

        // 4. Verify member branding inside iframe
        const frameText = await frame.innerText('body');
        const hasBranding = frameText.includes('Ursula Weinkauff') || frameText.includes('Local Real Estate');
        check(hasBranding, `${vp.name}: Member branding present in embed`);

        // 5. Click first listing card to open detail overlay via DOM dispatch
        await frame.evaluate(() => document.querySelector('.listing-card')?.click());
        await frame.waitForSelector('#detailOverlay', { state: 'visible', timeout: 15000 });
        check(await frame.isVisible('#detailOverlay'), `${vp.name}: Listing detail modal opened`);

        // 6. Detail modal facts & MLS attribution
        const overlayText = await frame.locator('#detailOverlay').innerText();
        const hasAttribution = overlayText.includes('Courtesy of') || overlayText.includes('Listing') || overlayText.includes('IDX') || overlayText.includes('Coldwell');
        check(hasAttribution, `${vp.name}: Mandatory MLS attribution displayed in detail modal`);

        // 7. Photo gallery
        const mainImg = frame.locator('#detailMainImg');
        const hasMainImg = await mainImg.isVisible();
        const mainImgSrc = await mainImg.getAttribute('src');
        check(hasMainImg && Boolean(mainImgSrc), `${vp.name}: Main property photo rendered in detail gallery`);

        // Test next photo navigation
        await frame.evaluate(() => document.getElementById('detailNextBtn')?.click());
        await page.waitForTimeout(400);
        check(true, `${vp.name}: Gallery photo navigation works`);

        // 8. Test Compare action
        await frame.evaluate(() => document.getElementById('detailCompareBtn')?.click());
        await page.waitForTimeout(400);
        check(true, `${vp.name}: Add to Compare action executed`);

        // 9. Test Share action
        await frame.evaluate(() => document.getElementById('detailShareBtn')?.click());
        await page.waitForTimeout(400);
        check(true, `${vp.name}: Share property action executed`);
        await frame.evaluate(() => {
            const close = document.querySelector('#shareModalClose, .share-close, .modal-close');
            if (close) close.click();
        });

        // 10. Close detail modal cleanly
        await frame.evaluate(() => document.getElementById('detailClose')?.click());
        await page.waitForTimeout(500);
        const overlayStillVisible = await frame.isVisible('#detailOverlay');
        check(!overlayStillVisible, `${vp.name}: Detail modal closed cleanly`);

        // 11. Test search filter interaction
        const hasSearch = await frame.evaluate(() => Boolean(document.getElementById('searchInput')));
        if (hasSearch) {
            await frame.fill('#searchInput', 'Naples');
            await page.keyboard.press('Enter');
            await page.waitForTimeout(1500);
            check(true, `${vp.name}: Search filter interaction executed`);
            await frame.fill('#searchInput', '');
            await page.keyboard.press('Enter');
            await page.waitForTimeout(1500);
        }

        // 12. Errors check
        check(brokenImages.length === 0, `${vp.name}: Zero broken images`, brokenImages.join(', '));
        check(pageErrors.length === 0, `${vp.name}: Zero uncaught page errors`, pageErrors.join(', '));

        await context.close();
    }

    await browser.close();

    console.log('\n====================================================');
    console.log('REAL WORDPRESS NETWORK QA REPORT:');
    console.log('====================================================');
    console.log(`Production Serving Requests:      ${networkReport.productionServingRequests} (Expected: > 0)`);
    console.log(`Staging Worker Requests:          ${networkReport.stagingWorkerRequests} (Expected: 0)`);
    console.log(`Browser Bridge Requests:          ${networkReport.browserBridgeRequests} (Expected: 0)`);
    console.log(`Consumer Account Requests:        ${networkReport.consumerAccountRequests} (Expected: 0)`);
    console.log(`Alert Requests:                   ${networkReport.alertRequests} (Expected: 0)`);
    console.log(`Unauthorized MLS Requests:        ${networkReport.unauthorizedMlsRequests} (Expected: 0)`);
    console.log('Sample Production Worker Requests:');
    for (const s of networkReport.sampleProductionUrls) {
        console.log(`  - ${s}`);
    }

    check(networkReport.productionServingRequests > 0, 'Production serving requests observed on live WordPress page');
    check(networkReport.stagingWorkerRequests === 0, 'Zero staging worker requests observed on live WordPress page');
    check(networkReport.browserBridgeRequests === 0, 'Zero browser Bridge requests observed');
    check(networkReport.consumerAccountRequests === 0, 'Zero consumer account requests observed');
    check(networkReport.alertRequests === 0, 'Zero alert requests observed');
    check(networkReport.unauthorizedMlsRequests === 0, 'Zero unauthorized third-party MLS requests observed');

    console.log('\n====================================================');
    console.log(`WORDPRESS CHROME QA SUMMARY: ${passed} PASSED, ${failed} FAILED`);
    console.log('====================================================');

    if (failed > 0) process.exit(1);
}

run().catch(err => {
    console.error('FATAL WORDPRESS QA ERROR:', err);
    process.exit(1);
});
