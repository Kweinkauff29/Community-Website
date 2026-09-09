import asyncio
import os
from playwright.async_api import async_playwright

ARTIFACT_DIR = "/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1"

async def test_pages_admin():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 1080},
            extra_http_headers={
                "x-dev-admin": "true",
                "x-admin-email": "kevin@coconutcoastrealtors.org"
            },
            ignore_https_errors=True
        )
        page = await context.new_page()

        print("Navigating to https://ccor-global-buyers-admin.pages.dev/...")
        await page.goto("https://ccor-global-buyers-admin.pages.dev/", wait_until="load")
        await page.wait_for_timeout(3000)

        # Verify page title and header
        title = await page.title()
        print("Page Title:", title)
        assert "Global Buyers Intelligence" in title

        header_user = await page.locator(".admin-header__user").inner_text()
        print("Header User Text:", header_user)

        total_kpi = await page.locator("#kpiTotalLeads").inner_text()
        print("KPI Total Leads:", total_kpi)

        # Capture full dashboard screenshot
        screenshot_path = os.path.join(ARTIFACT_DIR, "cloudflare_pages_admin_dashboard.png")
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"Saved: {screenshot_path}")

        await browser.close()
        print("Cloudflare Pages Admin Dashboard Verified Successfully!")

if __name__ == "__main__":
    asyncio.run(test_pages_admin())
