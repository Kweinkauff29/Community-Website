// Requires Playwright with Chromium and WebKit installed.
// Run with --local-assets before deployment; omit it to verify live assets.
const { chromium, webkit } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const base = process.env.QA_SITE_URL || 'http://ursulaweinkauff.com/';
const screenshots = process.env.QA_SCREENSHOTS;
const local = process.argv.includes('--local-assets');

async function assertMenuFits(menu) {
  // Clickability alone misses WebKit's overflow clipping: inspect every clipping ancestor.
  for (let attempt = 0; attempt < 50; attempt++) {
    const geometry = await menu.evaluate(el => {
      const r = el.getBoundingClientRect();
      const clipped = [];
      for (let n = el.parentElement; n; n = n.parentElement) {
        const s = getComputedStyle(n), b = n.getBoundingClientRect();
        if (['hidden','clip','scroll','auto'].includes(s.overflowY) && r.bottom > b.bottom + 1) clipped.push(n.tagName);
      }
      return {left:r.left,right:r.right,bottom:r.bottom,width:innerWidth,height:innerHeight,clipped};
    });
    if (geometry.left >= 0 && geometry.right <= geometry.width + 1 && geometry.bottom <= geometry.height + 1 && !geometry.clipped.length) return;
    if (attempt === 49) assert.fail('Dropdown clipped: '+JSON.stringify(geometry));
    await new Promise(resolve => setTimeout(resolve,100));
  }
}

(async () => {
  if (screenshots) await fs.mkdir(screenshots,{recursive:true});
  for (const [engine, browserType] of Object.entries({chromium,webkit})) {
    const browser = await browserType.launch({headless:true});
    try {
      for (const width of [1440,390,320]) {
        const page = await browser.newPage({viewport:{width,height:1000}});
        page.setDefaultTimeout(20000);
        if (local) await page.route('https://sneak-idx-worker.bonitaspringsrealtors.workers.dev/**', async route => {
          const pathname = new URL(route.request().url()).pathname;
          if (!['/embed.js','/quick-search/'].includes(pathname)) return route.continue();
          await route.fulfill({contentType: pathname.endsWith('.js')?'text/javascript':'text/html',
            body: await fs.readFile(path.join(__dirname,'../sneak-idx',pathname.endsWith('/')?pathname+'index.html':pathname))});
        });
        const url=new URL(base);url.searchParams.set('dropdown-qa',Date.now());
        await page.goto(url.href,{waitUntil:'domcontentloaded'});
        const frame=page.frameLocator('.idx-quick-search-shell iframe');
        await frame.locator('#qsCaretBtn').click();
        await frame.locator('#qsDropdown.show').waitFor();
        await assertMenuFits(frame.locator('#qsDropdown'));
        await frame.locator('.qs-item').first().click();
        await frame.locator('#qsInput').fill('Estero');
        await frame.locator('#qsDropdown.show').waitFor();
        await frame.locator('#qsInput').press('Escape');
        await frame.locator('#priceBtn').click();
        await assertMenuFits(frame.locator('#pricePopover'));
        if (screenshots && engine==='webkit') await page.screenshot({path:path.join(screenshots,`${engine}-${width}-price.png`)});
        await frame.locator('#qsMinPrice').selectOption('400000');
        await frame.locator('#qsMaxPrice').selectOption('800000');
        await frame.locator('#priceApply').click();
        for (const [name,value] of [['beds','3'],['baths','2']]) {
          await frame.locator(`#${name}Btn`).click();
          await assertMenuFits(frame.locator(`#${name}Popover`));
          if (screenshots && engine==='webkit') await page.screenshot({path:path.join(screenshots,`${engine}-${width}-${name}.png`)});
          await frame.locator(`#${name}PillGroup [data-val="${value}"]`).click();
        }
        await frame.locator('#searchSubmitBtn').click();
        await page.waitForURL(u=>u.pathname==='/quick-search' && u.searchParams.get('beds')==='3');
        const destination=new URL(page.url());
        assert.equal(destination.origin,new URL(base).origin);
        for (const [key,value] of Object.entries({location:'Estero',minPrice:'400000',maxPrice:'800000',beds:'3',baths:'2'})) assert.equal(destination.searchParams.get(key),value);
        const result=page.locator('.idx-widget-shell iframe');await result.waitFor();
        const resultUrl=new URL(await result.getAttribute('src'));
        for (const key of ['location','minPrice','maxPrice','beds','baths']) assert.equal(resultUrl.searchParams.get(key),destination.searchParams.get(key));
        console.log(`${engine} ${width}px: all dropdowns fit; search redirect and result filters passed`);
        await page.close();
      }
    } finally {await browser.close();}
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
