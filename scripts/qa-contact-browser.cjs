const {chromium,webkit}=require('playwright');
const fs=require('node:fs/promises');const path=require('node:path');const assert=require('node:assert/strict');
const local=process.argv.includes('--local-assets');
const host='https://sneak-idx-worker.bonitaspringsrealtors.workers.dev';
(async()=>{
 for(const [name,type] of Object.entries({chromium,webkit})){
  const browser=await type.launch();try{
   for(const width of [1440,390]){
    const page=await browser.newPage({viewport:{width,height:1000}});page.setDefaultTimeout(30000);let authRequest;
    if(local)await page.route(host+'/**',async route=>{
     const url=new URL(route.request().url());
     if(['/embed.js','/search/','/capture/'].includes(url.pathname))return route.fulfill({contentType:url.pathname.endsWith('.js')?'text/javascript':'text/html',body:await fs.readFile(path.join(__dirname,'../sneak-idx',url.pathname.endsWith('/')?url.pathname+'index.html':url.pathname))});
     if(url.pathname==='/idx/v1/config'){const response=await route.fetch();const cfg=await response.json();cfg.consumerEnabled=true;cfg.consumerAuthEnabled=true;cfg.features={...cfg.features,consumerAuth:true};cfg.leadCaptureSettings={popup_mode:'optional',popup_after_views:3};return route.fulfill({response,json:cfg});}
     return route.continue();
    });
    await page.route('**/api/consumer/auth/magic-link',async route=>{authRequest=route.request().postDataJSON();return route.fulfill({json:{success:true,message:'Check your inbox.'}});});
    await page.route('**/idx/v1/lead?*',async route=>{if(route.request().method()!=='POST')return route.continue();assert.equal(route.request().postDataJSON().email,'browser-qa@example.com');return route.fulfill({status:201,json:{success:true}});});
    await page.goto('http://ursulaweinkauff.com/quick-search',{waitUntil:'domcontentloaded'});
    const frame=page.frameLocator('.idx-widget-shell iframe');
    await frame.locator('.listing-card').first().waitFor();
    await frame.locator('.listing-card .card-price').first().click();
    await frame.locator('#detailContactBtn:not([disabled])').waitFor();
    await frame.locator('#detailClose').click();
    await frame.locator('#recentlyViewedSection').waitFor();
    const rects=await frame.locator('#recentlyViewedSection').evaluate(el=>({recent:el.getBoundingClientRect().top,recentBottom:el.getBoundingClientRect().bottom,main:document.getElementById('mainContainer').getBoundingClientRect().bottom,height:innerHeight,mainHeight:document.getElementById('mainContainer').getBoundingClientRect().height}));
    assert.ok(rects.recent>=rects.main-1,JSON.stringify(rects));assert.ok(rects.mainHeight>=350&&rects.mainHeight<1500,JSON.stringify(rects));
    assert.ok(rects.recentBottom<=rects.height+1,JSON.stringify(rects));
    const h1=await page.locator('.idx-widget-shell iframe').evaluate(e=>e.offsetHeight);await page.waitForTimeout(1200);const h2=await page.locator('.idx-widget-shell iframe').evaluate(e=>e.offsetHeight);assert.ok(Math.abs(h1-h2)<5,`Unstable embed ${h1}->${h2}`);
    // Embedded mode exposes sign-in through a save action rather than a hidden header.
    await frame.locator('#consumerEmbeddedAccountBtn').click();await frame.locator('#consumerAuthModal.open').waitFor();
    await frame.locator('#consumerEmailInput').fill('browser-qa@example.com');await frame.locator('#consumerSubmitBtn').click();
    await page.waitForFunction(()=>true);for(let i=0;i<30&&!authRequest;i++)await page.waitForTimeout(100);
    assert.ok(authRequest);assert.equal(new URL(authRequest.returnUrl).protocol,'https:');assert.equal(new URL(authRequest.returnUrl).pathname,'/portal');
    await frame.locator('#consumerAuthClose').click();
    await page.evaluate(src=>{const target=document.createElement('div');target.id='qa-contact';document.body.append(target);const script=document.createElement('script');script.src=src;script.dataset.site='ursula-weinkauff';script.dataset.widget='lead-capture';script.dataset.target='#qa-contact';document.body.append(script);},host+'/embed.js');
    await page.locator('#qa-contact > .sneak-idx-widget-container > button').click();
    const popup=page.frameLocator('#qa-contact dialog iframe');await popup.locator('#name').fill('Browser QA');await popup.locator('#email').fill('browser-qa@example.com');await popup.locator('#message').fill('Test request');await popup.locator('#submit').click();await popup.locator('#status').filter({hasText:'Thank you'}).waitFor();
    await page.locator('#qa-contact dialog > button').click();assert.equal(await page.locator('#qa-contact dialog').isVisible(),false);
    console.log(name,width,'recent placement, stable height, secure sign-in, contact popup PASS',rects);await page.unrouteAll({behavior:"wait"});await page.close();
   }
  }finally{await browser.close();}
 }
})().catch(e=>{console.error(e);process.exitCode=1});
