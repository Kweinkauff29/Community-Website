import asyncio
import os
import sys
import base64
import json
import time
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        print("Launching browser...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1400, 'height': 900})
        page = await context.new_page()

        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"[{msg.type}] {msg.text}"))

        # Navigate to local dev server
        url = "http://localhost:8088/global-buyers.html"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(1000)

        # ----------------------------------------------------------------------
        # SCENARIO A: Germany Click -> Popup Appears -> Dismissed -> No Lead Created
        # ----------------------------------------------------------------------
        print("\n--- SCENARIO A: Germany Click & Dismiss ---")
        germany_btn = page.locator("#gb-personalize button[data-market='germany']")
        await germany_btn.scroll_into_view_if_needed()
        print("Clicking Germany button...")
        await germany_btn.click()

        # Wait for popup to trigger (~550ms delay)
        print("Waiting for popup overlay to become visible...")
        overlay = page.locator("#gb-profile-popup-overlay")
        await overlay.wait_for(state="visible", timeout=3000)
        
        # Verify content in popup
        flag_text = await page.locator("#gb-popup-flag").inner_text()
        eyebrow_text = await page.locator("#gb-popup-eyebrow").inner_text()
        country_name = await page.locator("#gb-popup-country-name").inner_text()
        print(f"Popup displayed. Flag: '{flag_text}', Eyebrow: '{eyebrow_text}', Country: '{country_name}'")
        assert "GERMANY" in eyebrow_text.upper(), f"Expected Germany in eyebrow, got {eyebrow_text}"

        # Capture screenshot of popup
        await page.screenshot(path="/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/popup_germany_open.png")
        print("Captured popup_germany_open.png")

        # Click Dismiss button
        dismiss_btn = page.locator("#gb-popup-dismiss-btn")
        print("Clicking 'NO THANKS — CONTINUE EXPLORING'...")
        await dismiss_btn.click()
        await page.wait_for_timeout(400)
        assert not await overlay.is_visible(), "Expected popup overlay to be closed after dismiss"
        print("Popup successfully dismissed.")

        # ----------------------------------------------------------------------
        # SCENARIO B: Brazil Click -> Popup Appears -> Submit Contact Profile
        # ----------------------------------------------------------------------
        print("\n--- SCENARIO B: Brazil Click & Contact Submission ---")
        brazil_btn = page.locator("#gb-personalize button[data-market='brazil']")
        await brazil_btn.scroll_into_view_if_needed()
        print("Clicking Brazil button...")
        await brazil_btn.click()

        print("Waiting for Brazil popup overlay...")
        await overlay.wait_for(state="visible", timeout=3000)
        br_eyebrow = await page.locator("#gb-popup-eyebrow").inner_text()
        print(f"Brazil popup displayed. Eyebrow: '{br_eyebrow}'")
        assert "BRAZIL" in br_eyebrow.upper(), f"Expected Brazil in eyebrow, got {br_eyebrow}"

        # Fill in contact information
        print("Filling First Name, Last Name, Email...")
        await page.locator("#gb-popup-first-name").fill("Carlos")
        await page.locator("#gb-popup-last-name").fill("Menezes")
        await page.locator("#gb-popup-email").fill("carlos.menezes.test@example.com")
        
        # Select Goal
        print("Selecting Goal: investment...")
        await page.locator("#gb-popup-intent-select").select_option("investment")

        # Check required consent
        print("Checking required consent checkbox...")
        await page.locator("#gb-popup-consent").check()

        # Submit popup form
        print("Submitting profile form...")
        await page.locator("#gb-popup-submit-btn").click()

        # Wait for success wrap to become visible
        success_wrap = page.locator("#gb-profile-popup-success-wrap")
        await success_wrap.wait_for(state="visible", timeout=5000)
        success_line = await page.locator("#gb-popup-success-brief-line").inner_text()
        success_text = await page.locator("#gb-popup-success-text").inner_text()
        print(f"Success state rendered! Line: '{success_line}', Text: '{success_text}'")
        assert "Carlos" in success_text, f"Expected Carlos in success text, got {success_text}"

        # Capture screenshot of success state
        await page.screenshot(path="/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/popup_submit_success.png")
        print("Captured popup_submit_success.png")

        # Dismiss success modal
        continue_btn = page.locator("#gb-popup-success-continue-btn")
        await continue_btn.click()
        await page.wait_for_timeout(400)
        assert not await overlay.is_visible(), "Expected overlay to close after continue exploring"
        print("Success modal closed smoothly.")

        # ----------------------------------------------------------------------
        # SCENARIO C: Flight Controls Interaction & Live Fare Request
        # ----------------------------------------------------------------------
        print("\n--- SCENARIO C: Live Flight Controls Interaction ---")
        flight_sec = page.locator("#gb-global-access")
        await flight_sec.scroll_into_view_if_needed()
        await page.wait_for_timeout(600)

        # Check flight controls presence
        origin_select = page.locator("#gb-flight-origin-select")
        depart_input = page.locator("#gb-flight-depart-date")
        return_input = page.locator("#gb-flight-return-date")
        search_btn = page.locator("#gb-flight-search-btn")

        dep_val = await depart_input.input_value()
        ret_val = await return_input.input_value()
        print(f"Flight Controls: Depart={dep_val}, Return={ret_val}")
        assert dep_val and ret_val, "Expected departure and return dates to be prefilled"

        # Click Check Live Fares
        print("Clicking 'CHECK LIVE FARES'...")
        await search_btn.click()
        await page.wait_for_timeout(1200)

        # Check rendered flight cards
        flight_cards = page.locator("#gb-flight-planner-cards .gb-flight-card")
        card_count = await flight_cards.count()
        print(f"Rendered {card_count} flight cards")
        assert card_count >= 2, f"Expected at least 2 flight cards, got {card_count}"

        card1_text = await flight_cards.nth(0).inner_text()
        print(f"Card 1 sample text: {card1_text[:120]}...")

        # Capture screenshot of flight section
        await page.screenshot(path="/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/flight_planner_section.png")
        print("Captured flight_planner_section.png")

        # ----------------------------------------------------------------------
        # SCENARIO D: Mobile Viewport Test (390px)
        # ----------------------------------------------------------------------
        print("\n--- SCENARIO D: Mobile 390px Viewport Test ---")
        mobile_ctx = await browser.new_context(viewport={'width': 390, 'height': 844}, is_mobile=True)
        mobile_page = await mobile_ctx.new_page()
        await mobile_page.goto(url, wait_until="networkidle")
        await mobile_page.wait_for_timeout(800)

        # Click UK button
        print("Mobile: clicking UK button...")
        uk_btn = mobile_page.locator("#gb-personalize button[data-market='uk']")
        await uk_btn.scroll_into_view_if_needed()
        await uk_btn.click()

        m_overlay = mobile_page.locator("#gb-profile-popup-overlay")
        await m_overlay.wait_for(state="visible", timeout=3000)
        print("Mobile popup is visible!")

        await mobile_page.screenshot(path="/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/mobile_popup_390px.png")
        print("Captured mobile_popup_390px.png")

        # Dismiss mobile popup before testing flight section
        await mobile_page.locator("#gb-popup-dismiss-btn").click()
        await mobile_page.wait_for_timeout(400)

        # Check flight section on mobile
        m_flight_sec = mobile_page.locator("#gb-global-access")
        await m_flight_sec.scroll_into_view_if_needed()
        await mobile_page.wait_for_timeout(600)
        await mobile_page.screenshot(path="/Users/kevinweinkauff/.gemini/antigravity-ide/brain/1bf2246f-1c5e-400a-9fae-be4f5f2b5cd1/mobile_flights_390px.png")
        print("Captured mobile_flights_390px.png")

        await browser.close()
        print("\n=== ALL PLAYWRIGHT FLOW TESTS PASSED! ===")

if __name__ == "__main__":
    asyncio.run(run())
