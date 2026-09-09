import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1400, 'height': 900})
        page = await context.new_page()
        
        # Navigate
        await page.goto('http://localhost:8088/open-house/', wait_until='networkidle')
        await page.wait_for_timeout(1000)
        
        # 1. Desktop Top Header & Ad Banner
        await page.screenshot(path='/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/openhouse_desktop_top.png')
        
        # 2. Open listing modal and screenshot 970x250 banner
        first_card = await page.query_selector('.listing-card')
        if first_card:
            await first_card.click()
            await page.wait_for_selector('#detailOverlay[style*="display: block"]', timeout=5000)
            await page.wait_for_timeout(1000)
            # scroll to bottom of modal
            await page.evaluate("document.querySelector('.detail-modal').scrollTop = document.querySelector('.detail-modal').scrollHeight")
            await page.wait_for_timeout(500)
            await page.screenshot(path='/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/openhouse_modal_ad.png')
            # close modal
            await page.click('#detailClose')
            await page.wait_for_timeout(500)
            
        # 3. Desktop Footer
        footer = await page.query_selector('.event-footer')
        if footer:
            await footer.scroll_into_view_if_needed()
            await page.wait_for_timeout(1000)
            await page.screenshot(path='/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/openhouse_desktop_footer.png')
            
        # 4. Mobile Viewport (390x844)
        await page.set_viewport_size({'width': 390, 'height': 844})
        await page.goto('http://localhost:8088/open-house/', wait_until='networkidle')
        await page.wait_for_selector('#loadingOverlay', state='hidden', timeout=15000)
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(1000)
        await page.screenshot(path='/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/openhouse_mobile_top.png')
        
        # Scroll to mobile footer
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)
        await page.screenshot(path='/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/openhouse_mobile_footer.png')
            
        await browser.close()
        print("Screenshots captured successfully!")

asyncio.run(run())
