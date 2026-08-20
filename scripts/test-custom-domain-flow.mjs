/**
 * scripts/test-custom-domain-flow.mjs
 * 
 * End-to-End Live Validation of Cloudflare for SaaS Custom Domain Provisioning,
 * Status Transition (pending_dns -> active), Automatic SNEAK Verification, Host-Header
 * Routing, IDX Embedded Bootstrap, Canonical SEO, and Deauthorization on Staging.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const ADMIN_URL = "https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev";
const MEMBER_URL = "https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev";
const SITES_URL = "https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev";
const SERVING_URL = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";

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

async function runCustomDomainFlowTests() {
    console.log("====================================================");
    console.log("SNEAK IDX — SIMULATED CLOUDFLARE DOMAIN FLOW VALIDATION");
    console.log("Provider Mode:  SIMULATED CLOUDFLARE ADAPTER");
    console.log(`Admin Worker:   ${ADMIN_URL}`);
    console.log(`Sites Worker:   ${SITES_URL}`);
    console.log(`Member Worker:  ${MEMBER_URL}`);
    console.log(`Serving Worker: ${SERVING_URL}`);
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
    console.log("\n[2] Provisioning Tenant Account for Custom Domain Pipeline...");
    const siteKey = `domsite-${Date.now().toString(36).slice(-4)}`;
    const testHostname = `www.agent-${Date.now().toString(36).slice(-5)}.bonitaspringsrealtors.org`;

    const provRes = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            account_name: "SNEAK Custom Domain Agent",
            member_id: "NAR_DOM_AGENT_01",
            plan: "pro",
            scope_type: "agent",
            scope_value: "B3650316",
            site_key: siteKey,
            branding: {
                display_name: "Alexander Sterling",
                brokerage: "Bonita Springs Coastal Properties",
                phone: "(239) 555-7722",
                email: "alex@sterlingcoastal.com"
            }
        })
    });
    assert(provRes.status === 201, "Tenant account & site provisioned");
    const provData = await provRes.json();
    const accountId = provData.account?.id;
    const siteId = provData.site?.id;

    // Provision Site B for duplicate ownership collision testing
    const provRes2 = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            account_name: "SNEAK Collision Test Agent",
            member_id: "NAR_DOM_AGENT_02",
            plan: "pro",
            scope_type: "agent",
            scope_value: "B3650316",
            site_key: `domsite2-${Date.now().toString(36).slice(-4)}`
        })
    });
    const provData2 = await provRes2.json();
    const siteId2 = provData2.site?.id;

    // 3. Set Active GrowthZone Entitlement & Enable Website
    console.log("\n[3] Setting Active Entitlement & Enabling Website Config...");
    await fetch(`${ADMIN_URL}/api/admin/accounts/${accountId}/entitlement`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ source: "growthzone", status: "active", external_reference: "GZ_DOM_01" })
    });

    const webConfigRes = await fetch(`${ADMIN_URL}/api/admin/sites/${siteId}/website`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            enabled: 1,
            template_key: "coastal",
            site_title: "Alexander Sterling | Luxury Coastal Residences",
            tagline: "Curated Waterfront & Golf Estates",
            hero_heading: "Extraordinary Southwest Florida Living",
            hero_subheading: "Explore exclusive waterfront estates across Bonita Springs and Naples."
        })
    });
    assert(webConfigRes.status === 200, "Website configuration saved and enabled");

    // 4. Prepare Custom Hostname via Admin API
    console.log("\n[4] Preparing Custom Hostname via Admin API...");
    const prepRes = await fetch(`${ADMIN_URL}/api/admin/sites/${siteId}/domains/prepare`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ hostname: testHostname })
    });
    assert(prepRes.status === 201, "Custom hostname prepared with HTTP 201 Created");
    const prepData = await prepRes.json();
    assert(prepData.success === true, "Preparation returned success");
    assert(prepData.hostname === testHostname, "Hostname normalized accurately");
    assert(prepData.binding.status === "pending_dns", "Initial status is 'pending_dns'");
    assert(prepData.dnsInstructions.type === "CNAME", "DNS instruction specifies CNAME");
    assert(Boolean(prepData.dnsInstructions.target), "DNS instruction contains SaaS CNAME target");
    const bindingId = prepData.binding.id;

    // 5. Verify Unverified Host Fails Closed (404)
    console.log("\n[5] Verifying Unverified Host Returns HTTP 404...");
    const unvRes = await fetch(SITES_URL, {
        headers: { "Host": testHostname, "X-Forwarded-Host": testHostname }
    });
    assert(unvRes.status === 404, "Unverified custom hostname returns HTTP 404 Site Not Found");

    // 6. Refresh Custom Hostname Status (Simulates Cloudflare SaaS DNS & SSL verification)
    console.log("\n[6] Refreshing Custom Hostname Status (Cloudflare SaaS Verification)...");
    const refRes = await fetch(`${ADMIN_URL}/api/admin/domain-bindings/${bindingId}/refresh`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL }
    });
    assert(refRes.status === 200, "Refresh returned HTTP 200 OK");
    const refData = await refRes.json();
    assert(refData.isFullyActive === true, "Domain status reached fully active");
    assert(refData.binding.status === "active", "Binding status is 'active'");
    assert(refData.binding.ssl_status === "active", "SSL status is 'active'");

    // 7. Verify Live Custom Host Header Routing on Website Engine
    console.log("\n[7] Verifying Live Custom Host-Header Resolution on Website Engine...");
    const liveSiteRes = await fetch(SITES_URL, {
        headers: { "Host": testHostname, "X-Forwarded-Host": testHostname }
    });
    assert(liveSiteRes.status === 200, "Active custom domain returned HTTP 200 OK");
    const liveHtml = await liveSiteRes.text();
    assert(liveHtml.includes("Alexander Sterling"), "Branded display name rendered on custom host");
    assert(liveHtml.includes("Extraordinary Southwest Florida Living"), "Hero heading rendered on custom host");
    assert(liveHtml.includes(`https://${testHostname}`), "Canonical link uses custom customer hostname");
    assert(liveHtml.includes('<meta name="robots" content="index, follow" />'), "Robots tag allows indexing on active custom host");
    assert(!liveHtml.includes("PREVIEW MODE"), "Preview banner absent on live custom domain");

    // 8. Verify Sub-Pages and IDX Bootstrap on Custom Host
    console.log("\n[8] Testing Sub-Pages & SNEAK IDX Bootstrap on Custom Host...");
    const searchRes = await fetch(`${SITES_URL}/search`, {
        headers: { "Host": testHostname, "X-Forwarded-Host": testHostname }
    });
    assert(searchRes.status === 200, "Search sub-page returned HTTP 200 OK");
    const searchHtml = await searchRes.text();
    assert(searchHtml.includes("sneak-idx-root"), "Search page embeds SNEAK IDX container");

    // 9. Verify Public Lead Submission via Custom Host
    console.log("\n[9] Testing Lead Ingestion via Custom Host...");
    const leadRes = await fetch(`${SITES_URL}/api/contact`, {
        method: "POST",
        headers: { "Host": testHostname, "X-Forwarded-Host": testHostname, "Content-Type": "application/json" },
        body: JSON.stringify({
            name: "Eleanor Vance",
            email: "eleanor.vance@example.com",
            phone: "(239) 555-1234",
            message: "Inquiry on luxury beachfront property."
        })
    });
    assert(leadRes.status === 201, "Lead submitted via custom host with HTTP 201 Created");

    // 10. Verify Robots.txt and Sitemap.xml on Custom Host
    console.log("\n[10] Verifying Robots.txt and Sitemap.xml on Custom Host...");
    const robotsRes = await fetch(`${SITES_URL}/robots.txt`, {
        headers: { "Host": testHostname, "X-Forwarded-Host": testHostname }
    });
    assert(robotsRes.status === 200, "robots.txt returned HTTP 200 OK");
    const robotsText = await robotsRes.text();
    assert(robotsText.includes("Allow: /"), "robots.txt allows indexing on live custom host");

    const sitemapRes = await fetch(`${SITES_URL}/sitemap.xml`, {
        headers: { "Host": testHostname, "X-Forwarded-Host": testHostname }
    });
    assert(sitemapRes.status === 200, "sitemap.xml returned HTTP 200 OK");
    const sitemapXml = await sitemapRes.text();
    assert(sitemapXml.includes(`https://${testHostname}/`), "sitemap.xml emits customer hostname URLs");

    // 11. Test Security: Duplicate Domain Claim Blocked
    console.log("\n[11] Testing Duplicate Domain Claim Across Tenants...");
    const dupRes = await fetch(`${ADMIN_URL}/api/admin/sites/${siteId2}/domains/prepare`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ hostname: testHostname })
    });
    assert(dupRes.status === 400, "Duplicate domain claim blocked with HTTP 400");
    const dupData = await dupRes.json();
    assert((dupData.message || dupData.error).includes("already connected to another SNEAK site"), "Error reports domain ownership conflict");

    // 12. Test Domain Removal & Deauthorization
    console.log("\n[12] Testing Domain Removal & Immediate Deauthorization...");
    const delRes = await fetch(`${ADMIN_URL}/api/admin/domain-bindings/${bindingId}`, {
        method: "DELETE",
        headers: { "Cookie": adminCookie, "Origin": ADMIN_URL }
    });
    assert(delRes.status === 200, "Domain binding deleted with HTTP 200 OK");

    const afterDelRes = await fetch(SITES_URL, {
        headers: { "Host": testHostname, "X-Forwarded-Host": testHostname }
    });
    assert(afterDelRes.status === 404, "Custom host immediately returns HTTP 404 after deletion");

    console.log("\n====================================================");
    console.log(`CUSTOM DOMAIN FLOW VALIDATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log("====================================================");

    if (failed > 0) process.exit(1);
}

runCustomDomainFlowTests().catch(err => {
    console.error("Test Execution Error:", err);
    process.exit(1);
});
