/**
 * scripts/test-website-flow.mjs
 * 
 * End-to-End Live Validation of Multi-Tenant Website Engine, 3 Templates (Essential, Coastal, Brokerage),
 * Preview Security, Lead Capture, Content Preservation, and Entitlement Enforcement on Staging.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const ADMIN_URL = "https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev";
const MEMBER_URL = "https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev";
const SITES_URL = "https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev";

// Load Admin Password from environment or .dev.vars
let adminPassword = process.env.SNEAK_ADMIN_TEST_PASSWORD;
if (!adminPassword) {
    const devVarsPath = path.join(rootDir, '.dev.vars');
    if (fs.existsSync(devVarsPath)) {
        try {
            const content = fs.readFileSync(devVarsPath, 'utf8');
            for (const line of content.split('\n')) {
                const trimmed = line.trim();
                if (trimmed.startsWith('SNEAK_ADMIN_TEST_PASSWORD=')) {
                    adminPassword = trimmed.substring('SNEAK_ADMIN_TEST_PASSWORD='.length).trim().replace(/^["']|["']$/g, '');
                    break;
                }
            }
        } catch {}
    }
}

if (!adminPassword) {
    console.error("FATAL: SNEAK_ADMIN_TEST_PASSWORD is required for integration tests.");
    process.exit(1);
}

let passed = 0;
let failed = 0;

function assert(condition, message) {
    if (condition) {
        console.log(`  [PASS] ${message}`);
        passed++;
    } else {
        console.error(`  [FAIL] ${message}`);
        failed++;
    }
}

async function runWebsiteFlowTests() {
    console.log("====================================================");
    console.log("SNEAK IDX — END-TO-END WEBSITE TEMPLATE ENGINE VALIDATION");
    console.log(`Admin Worker:   ${ADMIN_URL}`);
    console.log(`Sites Worker:   ${SITES_URL}`);
    console.log(`Member Worker:  ${MEMBER_URL}`);
    console.log("====================================================");

    // 1. Authenticate as Admin
    console.log("\n[1] Authenticating as Admin on Staging...");
    const adminLoginRes = await fetch(`${ADMIN_URL}/api/admin/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ password: adminPassword })
    });
    assert(adminLoginRes.status === 200, "Admin login returned HTTP 200 OK");
    const adminCookie = (adminLoginRes.headers.get("Set-Cookie") || "").split(";")[0];

    // 2. Provision Tenant Account & Site
    console.log("\n[2] Provisioning Agent Tenant Account for Website Engine...");
    const siteKey = `siteflow-${Date.now().toString(36).slice(-4)}`;
    const memberEmail = `agent.${Date.now()}@bonitaspringsrealtors.org`;

    const provRes = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            account_name: "SNEAK Template Test Agent",
            member_id: "NAR_AGENT_SITE_01",
            plan: "pro",
            scope_type: "agent",
            scope_value: "B3650316",
            site_key: siteKey,
            domain: "localhost",
            branding: {
                display_name: "Victoria Hamilton",
                brokerage: "Bonita Springs Luxury Real Estate",
                phone: "(239) 555-8899",
                email: memberEmail
            }
        })
    });
    assert(provRes.status === 201, "Tenant account & site provisioned");
    const provData = await provRes.json();
    const accountId = provData.account?.id;
    const siteId = provData.site?.id;

    // 3. Set Active GrowthZone Entitlement
    console.log("\n[3] Setting Active GrowthZone Entitlement...");
    const entRes = await fetch(`${ADMIN_URL}/api/admin/accounts/${accountId}/entitlement`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            source: "growthzone",
            status: "active",
            external_reference: "GZ_SITEFLOW_01"
        })
    });
    assert(entRes.status === 200, "GrowthZone entitlement configured");

    // 4. Configure Website Template 1 (Essential)
    console.log("\n[4] Configuring Template 1: SNEAK Essential...");
    const webConfigRes = await fetch(`${ADMIN_URL}/api/admin/sites/${siteId}/website`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            enabled: 1,
            template_key: "essential",
            site_title: "Victoria Hamilton | Bonita Springs Real Estate",
            tagline: "Exclusive Coastal & Golf Properties",
            hero_heading: "Discover Luxury in Southwest Florida",
            hero_subheading: "Your premier guide to Bonita Springs, Estero, and Naples real estate.",
            about_heading: "About Victoria Hamilton",
            about_body: "Dedicated to providing white-glove MLS representation and property advisory across Southwest Florida.",
            contact_cta_text: "Ready to tour properties or list your home? Get in touch today."
        })
    });
    assert(webConfigRes.status === 200, "SNEAK Essential website configured via Admin API");
    const webConfigData = await webConfigRes.json();
    const previewUrl = webConfigData.previewUrl;
    assert(Boolean(previewUrl), "Received signed preview URL from Admin API");

    // 5. Fetch & Verify Live SNEAK Essential Preview Homepage
    console.log("\n[5] Fetching Live SNEAK Essential Website Preview...");
    const siteHomeRes = await fetch(previewUrl);
    assert(siteHomeRes.status === 200, "Essential preview returned HTTP 200 OK");
    const siteHomeHtml = await siteHomeRes.text();
    assert(siteHomeHtml.includes("PREVIEW MODE"), "Preview banner displayed");
    assert(siteHomeHtml.includes("Victoria Hamilton"), "Branded display name rendered");
    assert(siteHomeHtml.includes("Discover Luxury in Southwest Florida"), "Hero heading rendered");
    assert(siteHomeHtml.includes("ess-hero-search"), "Essential hero search bar present");
    assert(siteHomeHtml.includes("Featured Properties"), "Featured properties section present");
    assert(siteHomeHtml.includes('<meta name="robots" content="noindex, nofollow" />'), "Preview page has noindex meta tag");
    assert(siteHomeHtml.includes("IDX technology powered by <strong>SNEAK</strong>"), "MLS compliance & platform footer present");

    // Extract Preview Token from URL
    const urlObj = new URL(previewUrl);
    const previewToken = urlObj.searchParams.get("token");

    // 6. Test Website Sub-Pages on Essential Template
    console.log("\n[6] Testing Sub-Pages on SNEAK Essential Template...");
    
    // /search
    const searchRes = await fetch(`${SITES_URL}/preview/${siteKey}/search?token=${encodeURIComponent(previewToken)}`);
    assert(searchRes.status === 200, "Search page returned HTTP 200 OK");
    const searchHtml = await searchRes.text();
    assert(searchHtml.includes("sneak-idx-root"), "Search page embeds SNEAK IDX container");
    assert(searchHtml.includes("embed.js"), "Search page embeds embed.js");

    // /open-houses
    const ohRes = await fetch(`${SITES_URL}/preview/${siteKey}/open-houses?token=${encodeURIComponent(previewToken)}`);
    assert(ohRes.status === 200, "Open Houses page returned HTTP 200 OK");

    // /about
    const aboutRes = await fetch(`${SITES_URL}/preview/${siteKey}/about?token=${encodeURIComponent(previewToken)}`);
    assert(aboutRes.status === 200, "About page returned HTTP 200 OK");
    const aboutHtml = await aboutRes.text();
    assert(aboutHtml.includes("Victoria Hamilton"), "About page renders agent identity");

    // /contact
    const contactRes = await fetch(`${SITES_URL}/preview/${siteKey}/contact?token=${encodeURIComponent(previewToken)}`);
    assert(contactRes.status === 200, "Contact page returned HTTP 200 OK");

    // 7. Submit Public Lead on Contact Page
    console.log("\n[7] Testing Public Lead Form Submission & Member Visibility...");
    const leadSubmitRes = await fetch(`${SITES_URL}/preview/${siteKey}/api/contact?token=${encodeURIComponent(previewToken)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            name: "Jonathan Test Buyer",
            email: "j.buyer.test@example.com",
            phone: "(239) 555-4433",
            message: "I would like to schedule a private showing for a waterfront condo."
        })
    });
    assert(leadSubmitRes.status === 201, "Lead submitted with HTTP 201 Created");
    const leadSubmitData = await leadSubmitRes.json();
    assert(Boolean(leadSubmitData.leadId), "Received generated lead ID");

    // 8. Template Switching to Template 2 (Coastal) with Content Preservation
    console.log("\n[8] Switching Template to SNEAK Coastal & Verifying Content Preservation...");
    const coastalUpdateRes = await fetch(`${ADMIN_URL}/api/admin/sites/${siteId}/website`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            enabled: 1,
            template_key: "coastal"
        })
    });
    assert(coastalUpdateRes.status === 200, "Template updated to 'coastal'");

    const coastalRes = await fetch(previewUrl);
    assert(coastalRes.status === 200, "Coastal preview returned HTTP 200 OK");
    const coastalHtml = await coastalRes.text();
    assert(coastalHtml.includes("cst-header-trans"), "Coastal editorial header rendered");
    assert(coastalHtml.includes("Selected Coastal Residences"), "Coastal portfolio section rendered");
    assert(coastalHtml.includes("Discover Luxury in Southwest Florida"), "Original hero heading preserved after switch");

    // 9. Template Switching to Template 3 (Brokerage)
    console.log("\n[9] Switching Template to SNEAK Brokerage...");
    const brokerageUpdateRes = await fetch(`${ADMIN_URL}/api/admin/sites/${siteId}/website`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            enabled: 1,
            template_key: "brokerage"
        })
    });
    assert(brokerageUpdateRes.status === 200, "Template updated to 'brokerage'");

    const brkRes = await fetch(previewUrl);
    assert(brkRes.status === 200, "Brokerage preview returned HTTP 200 OK");
    const brkHtml = await brkRes.text();
    assert(brkHtml.includes("brk-topbar"), "Brokerage topbar rendered");
    assert(brkHtml.includes("Brokerage Exclusive &amp; Office Listings"), "Brokerage inventory section rendered");
    assert(brkHtml.includes("Comprehensive Real Estate Representation"), "Brokerage client services section rendered");

    // 10. Service Entitlement Enforcement on Website Engine
    console.log("\n[10] Testing Entitlement Enforcement on Website Engine...");
    
    // Suspend Entitlement
    await fetch(`${ADMIN_URL}/api/admin/accounts/${accountId}/entitlement`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ status: "suspended" })
    });
    const suspRes = await fetch(previewUrl);
    assert(suspRes.status === 503, "Suspended entitlement blocked with HTTP 503 Service Unavailable");
    const suspHtml = await suspRes.text();
    assert(suspHtml.includes("Website Temporarily Unavailable"), "Generic maintenance message returned");
    assert(!suspHtml.includes("GrowthZone"), "Zero GrowthZone leakage in unavailable page");
    assert(!suspHtml.includes("suspended"), "Zero internal reason leakage in unavailable page");

    // Restore Entitlement
    await fetch(`${ADMIN_URL}/api/admin/accounts/${accountId}/entitlement`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ status: "active" })
    });
    const restoredRes = await fetch(previewUrl);
    assert(restoredRes.status === 200, "Active entitlement restored website with HTTP 200 OK");

    // 11. Preview Security & Tamper Rejection
    console.log("\n[11] Testing Preview Token Security & Tamper Resistance...");
    
    // Missing Token
    const noTokenRes = await fetch(`${SITES_URL}/preview/${siteKey}/`);
    assert(noTokenRes.status === 403, "Missing preview token rejected with HTTP 403");

    // Tampered Token
    const tamperedToken = previewToken.slice(0, -6) + "xxxxxx";
    const tamperedRes = await fetch(`${SITES_URL}/preview/${siteKey}/?token=${encodeURIComponent(tamperedToken)}`);
    assert(tamperedRes.status === 403, "Tampered preview token rejected with HTTP 403");

    // Token for Site A used on different site
    const foreignSiteRes = await fetch(`${SITES_URL}/preview/demo-ccor/?token=${encodeURIComponent(previewToken)}`);
    assert(foreignSiteRes.status === 403, "Token for wrong site rejected with HTTP 403");

    // 12. Sitemap & Robots.txt
    console.log("\n[12] Testing Sitemap and Robots.txt...");
    const robotsRes = await fetch(`${SITES_URL}/robots.txt`);
    assert(robotsRes.status === 200, "robots.txt returned HTTP 200 OK");
    const robotsTxt = await robotsRes.text();
    assert(robotsTxt.includes("Disallow: /"), "Staging robots.txt disallows indexing");

    const sitemapRes = await fetch(`${SITES_URL}/preview/${siteKey}/sitemap.xml?token=${encodeURIComponent(previewToken)}`);
    assert(sitemapRes.status === 200, "sitemap.xml returned HTTP 200 OK");
    const sitemapXml = await sitemapRes.text();
    assert(sitemapXml.includes("<loc>"), "sitemap.xml contains url entries");

    console.log("\n====================================================");
    console.log(`WEBSITE TEMPLATE ENGINE VALIDATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log("====================================================");

    if (failed > 0) process.exit(1);
}

runWebsiteFlowTests().catch(err => {
    console.error("Test Execution Error:", err);
    process.exit(1);
});
