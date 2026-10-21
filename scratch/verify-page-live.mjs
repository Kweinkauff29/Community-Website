import { chromium } from 'playwright';
import path from 'path';
import fs from 'fs';

async function main() {
  const browser = await chromium.launch();
  const outDir = path.resolve('scratch/screenshots');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const url = 'https://coconutcoastopenhouses.com/open-house/';
  console.log('Navigating to', url);

  // Desktop test
  const desktopPage = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const failedRequests = [];
  desktopPage.on('requestfailed', req => {
    failedRequests.push({ url: req.url(), failure: req.failure()?.errorText });
  });

  await desktopPage.goto(url, { waitUntil: 'networkidle' });
  await desktopPage.waitForTimeout(2000);

  const topText = await desktopPage.textContent('#top-bar');
  console.log('Top bar text:', topText?.replace(/\s+/g, ' ').trim());

  const dateTiles = await desktopPage.$$eval('.date-tile', els => els.map(e => e.textContent.replace(/\s+/g, ' ').trim()));
  console.log('Date tiles:', dateTiles);

  const sponsorLinks = await desktopPage.$$eval('a[href*="lawfirmnaples"]', els => els.map(e => ({ href: e.href, text: e.textContent.replace(/\s+/g, ' ').trim() })));
  console.log('Sponsor links count:', sponsorLinks.length);

  const imgStatus = await desktopPage.$$eval('img', imgs => imgs.map(img => ({
    src: img.src,
    alt: img.alt,
    complete: img.complete,
    naturalWidth: img.naturalWidth,
    naturalHeight: img.naturalHeight,
    visible: img.offsetWidth > 0 && img.offsetHeight > 0
  })));

  const wwmrImgs = imgStatus.filter(i => i.src.includes('wwmr') || i.alt?.includes('Woods') || i.alt?.includes('WWMR'));
  console.log('WWMR Images status:', wwmrImgs);

  await desktopPage.screenshot({ path: path.join(outDir, 'desktop-header.png'), clip: { x: 0, y: 0, width: 1440, height: 800 } });
  await desktopPage.screenshot({ path: path.join(outDir, 'desktop-footer.png'), fullPage: true });

  // Mobile test
  const mobilePage = await browser.newPage({ viewport: { width: 390, height: 844 } });
  await mobilePage.goto(url, { waitUntil: 'networkidle' });
  await mobilePage.waitForTimeout(2000);

  await mobilePage.screenshot({ path: path.join(outDir, 'mobile-header.png'), clip: { x: 0, y: 0, width: 390, height: 800 } });

  console.log('Failed network requests:', failedRequests);
  await browser.close();
  console.log('QA completed successfully!');
}

main().catch(err => {
  console.error('QA script error:', err);
  process.exit(1);
});
