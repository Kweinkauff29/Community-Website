/**
 * scripts/test-provisioning-flow.mjs
 * 
 * End-to-End Live Validation of Member Provisioning, Hardened Admin Auth, and Scoped Serving.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const ADMIN_URL = "https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev";
const SERVING_URL = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";

// 1. Resolve SNEAK_ADMIN_TEST_PASSWORD from environment or .dev.vars (NO FALLBACK)
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
    console.error("FATAL: SNEAK_ADMIN_TEST_PASSWORD is required for provisioning integration tests.");
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

async function runProvisioningTests() {
    console.log("====================================================");
    console.log("SNEAK IDX — END-TO-END PROVISIONING & ADMIN VALIDATION");
    console.log(`Admin Worker:   ${ADMIN_URL}`);
    console.log(`Serving Worker: ${SERVING_URL}`);
    console.log("====================================================");

    // 1. Hardened Admin Authentication with PBKDF2
    console.log("\n[1] Testing PBKDF2 Admin Authentication (/api/admin/login)...");
    const loginRes = await fetch(`${ADMIN_URL}/api/admin/login`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Origin": ADMIN_URL
        },
        body: JSON.stringify({ password: adminPassword })
    });
    assert(loginRes.status === 200, "PBKDF2 admin login returned HTTP 200 OK");
    const cookieHeader = loginRes.headers.get("Set-Cookie") || "";
    assert(cookieHeader.includes("__Host-sneak_admin_session="), "Received __Host-sneak_admin_session cookie");
    assert(cookieHeader.includes("SameSite=Strict"), "Cookie contains SameSite=Strict");
    assert(cookieHeader.includes("HttpOnly"), "Cookie contains HttpOnly");

    // Extract cookie value for subsequent requests
    const cookieVal = cookieHeader.split(";")[0];

    // 2. Admin Dashboard
    console.log("\n[2] Testing Admin Dashboard Metrics (/api/admin/dashboard)...");
    const dashRes = await fetch(`${ADMIN_URL}/api/admin/dashboard`, {
        headers: { "Cookie": cookieVal }
    });
    assert(dashRes.status === 200, "Dashboard endpoint returned HTTP 200 OK");
    const dashData = await dashRes.json();
    assert(dashData.inventory?.activeListings > 30000, `Dashboard reports active listings (${dashData.inventory?.activeListings?.toLocaleString()})`);
    assert(dashData.inventory?.futureOpenHouses > 1000, `Dashboard reports future open houses (${dashData.inventory?.futureOpenHouses?.toLocaleString()})`);

    // 3. MLS ID Validation Endpoint
    console.log("\n[3] Testing MLS ID Validation Endpoint (/api/admin/validate-mls)...");
    const validAgentRes = await fetch(`${ADMIN_URL}/api/admin/validate-mls?type=agent&mlsId=B3650316`, {
        headers: { "Cookie": cookieVal }
    });
    const validAgentData = await validAgentRes.json();
    assert(validAgentData.valid === true && validAgentData.count > 0, `Agent B3650316 validated with ${validAgentData.count} active listings`);

    const invalidAgentRes = await fetch(`${ADMIN_URL}/api/admin/validate-mls?type=agent&mlsId=NON_EXISTENT_999`, {
        headers: { "Cookie": cookieVal }
    });
    const invalidAgentData = await invalidAgentRes.json();
    assert(invalidAgentData.valid === false && invalidAgentData.count === 0, "Non-existent MLS ID returned valid: false");

    // 4. Provision New Agent Member Site (demo-prov-agent)
    console.log("\n[4] Provisioning New Agent Tenant Account & Site...");
    const agentSiteKey = `prov-agent-${Date.now().toString(36).slice(-4)}`;
    const provAgentRes = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
        method: "POST",
        headers: {
            "Cookie": cookieVal,
            "Content-Type": "application/json",
            "Origin": ADMIN_URL
        },
        body: JSON.stringify({
            account_name: "SNEAK Demo Agent Member",
            member_id: "NAR_TEST_01",
            plan: "pro",
            scope_type: "agent",
            scope_value: "B3650316",
            site_key: agentSiteKey,
            domain: "localhost",
            branding: {
                display_name: "SNEAK Demo Agent Site",
                brokerage: "Premier Staging Realty",
                primary_color: "#1e3a8a",
                secondary_color: "#3b82f6"
            }
        })
    });
    assert(provAgentRes.status === 201, "Agent tenant account provisioned with HTTP 201 Created");
    const provAgentData = await provAgentRes.json();
    const createdAgentAccountId = provAgentData.account?.id;
    const createdAgentSiteId = provAgentData.site?.id;
    assert(Boolean(provAgentData.embed?.snippets?.search?.htmlSnippet), "Generated full search embed snippet");

    // 5. Verify Serving Worker Live Bootstrap for Provisioned Agent Site
    console.log("\n[5] Testing Serving Worker Bootstrap for Newly Provisioned Site...");
    const bootRes = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${agentSiteKey}`, {
        headers: { Origin: "http://localhost:8090" }
    });
    assert(bootRes.status === 200, "Newly provisioned site bootstrap returns HTTP 200 OK on authorized origin");
    const bootData = await bootRes.json();
    const sessionToken = bootData.session;
    assert(Boolean(sessionToken), "Received valid signed HMAC session token for new site");

    // 6. Verify Scoped Search on Newly Provisioned Agent Site
    console.log("\n[6] Verifying Live Scope Isolation on Serving Worker...");
    const searchRes = await fetch(`${SERVING_URL}/idx/v1/search?site=${agentSiteKey}&limit=20`, {
        headers: { "X-Sneak-Session": sessionToken }
    });
    assert(searchRes.status === 200, "Search returns HTTP 200 OK with session token");
    const searchData = await searchRes.json();
    const listings = searchData.data || [];
    const allMatch = listings.length > 0 && listings.every(l => l.ListAgentMlsId === "B3650316");
    assert(allMatch, `Search returned ONLY agent B3650316 listings (count: ${listings.length}/${searchData.pagination?.total})`);

    // 7. Verify Scope Escape Rejection
    console.log("\n[7] Testing Scope Escape Protection on Newly Provisioned Tenant...");
    const escapeDetail = await fetch(`${SERVING_URL}/idx/v1/listing/OUTSIDE_LISTING_KEY?site=${agentSiteKey}`, {
        headers: { "X-Sneak-Session": sessionToken }
    });
    assert(escapeDetail.status === 404, "Outside listing detail request rejected with HTTP 404");

    // 8. Test Account Suspension Enforcement
    console.log("\n[8] Testing Account Suspension Enforcement...");
    const suspendRes = await fetch(`${ADMIN_URL}/api/admin/accounts/${createdAgentAccountId}`, {
        method: "PATCH",
        headers: {
            "Cookie": cookieVal,
            "Content-Type": "application/json",
            "Origin": ADMIN_URL
        },
        body: JSON.stringify({ status: "suspended" })
    });
    assert(suspendRes.status === 200, "Account suspended via admin API");

    // Verify Serving Worker denies bootstrap when account is suspended
    const bootSuspended = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${agentSiteKey}`, {
        headers: { Origin: "http://localhost:8090" }
    });
    assert(bootSuspended.status === 403, "Serving Worker blocked bootstrap for suspended account with HTTP 403 SiteInactive");

    // Reactivate Account
    const reactivateRes = await fetch(`${ADMIN_URL}/api/admin/accounts/${createdAgentAccountId}`, {
        method: "PATCH",
        headers: {
            "Cookie": cookieVal,
            "Content-Type": "application/json",
            "Origin": ADMIN_URL
        },
        body: JSON.stringify({ status: "active" })
    });
    assert(reactivateRes.status === 200, "Account reactivated via admin API");

    const bootReactivated = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${agentSiteKey}`, {
        headers: { Origin: "http://localhost:8090" }
    });
    assert(bootReactivated.status === 200, "Serving Worker restored bootstrap service upon reactivation");

    // 9. Test Domain Whitelist Enforcement
    console.log("\n[9] Testing Domain Security on Provisioned Site...");
    const unauthBoot = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${agentSiteKey}`, {
        headers: { Origin: "https://unauthorized-broker.com" }
    });
    assert(unauthBoot.status === 403, "Unauthorized origin rejected with HTTP 403 DomainNotAuthorized");

    // 10. Provision Office Member Site (demo-prov-office)
    console.log("\n[10] Provisioning New Office Tenant Account & Site...");
    const officeSiteKey = `prov-off-${Date.now().toString(36).slice(-4)}`;
    const provOffRes = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
        method: "POST",
        headers: {
            "Cookie": cookieVal,
            "Content-Type": "application/json",
            "Origin": ADMIN_URL
        },
        body: JSON.stringify({
            account_name: "SNEAK Demo Office Member",
            member_id: "OFF_TEST_02",
            plan: "brokerage",
            scope_type: "office",
            scope_value: "BPRI",
            site_key: officeSiteKey,
            domain: "localhost",
            branding: {
                display_name: "SNEAK Demo Office Site",
                brokerage: "Premier Brokerage Associates"
            }
        })
    });
    assert(provOffRes.status === 201, "Office tenant account provisioned with HTTP 201 Created");
    const provOffData = await provOffRes.json();
    const createdOfficeSiteId = provOffData.site?.id;

    // Bootstrap Office Site
    const offBootRes = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${officeSiteKey}`, {
        headers: { Origin: "http://localhost:8090" }
    });
    assert(offBootRes.status === 200, "Office site bootstrap returns HTTP 200 OK");
    const offBootData = await offBootRes.json();
    const offSession = offBootData.session;

    // Verify Scoped Search on Office Site
    const offSearchRes = await fetch(`${SERVING_URL}/idx/v1/search?site=${officeSiteKey}&limit=20`, {
        headers: { "X-Sneak-Session": offSession }
    });
    assert(offSearchRes.status === 200, "Office search returns HTTP 200 OK with session token");
    const offSearchData = await offSearchRes.json();
    const offListings = offSearchData.data || [];
    const allOffMatch = offListings.length > 0 && offListings.every(l => l.ListOfficeMlsId === "BPRI" || l.ListOfficeKey === "BPRI");
    assert(allOffMatch, `Office search returned ONLY BPRI listings (count: ${offListings.length}/${offSearchData.pagination?.total})`);

    // 11. Test Dynamic Domain Management (Add, Disable, Delete)
    console.log("\n[11] Testing Dynamic Domain Management...");
    const addDomRes = await fetch(`${ADMIN_URL}/api/admin/sites/${createdOfficeSiteId}/domains`, {
        method: "POST",
        headers: {
            "Cookie": cookieVal,
            "Content-Type": "application/json",
            "Origin": ADMIN_URL
        },
        body: JSON.stringify({ domain: "https://testbrokerage.com/search" })
    });
    assert(addDomRes.status === 201, "Added domain 'testbrokerage.com' to office site");
    const addDomData = await addDomRes.json();
    const newDomId = addDomData.domain?.id;

    // Test bootstrap from testbrokerage.com origin
    const testDomBoot = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${officeSiteKey}`, {
        headers: { Origin: "https://testbrokerage.com" }
    });
    assert(testDomBoot.status === 200, "Bootstrap succeeds on newly added domain 'testbrokerage.com'");

    // Disable domain
    const disableDomRes = await fetch(`${ADMIN_URL}/api/admin/domains/${newDomId}`, {
        method: "PATCH",
        headers: {
            "Cookie": cookieVal,
            "Content-Type": "application/json",
            "Origin": ADMIN_URL
        },
        body: JSON.stringify({ status: "disabled" })
    });
    assert(disableDomRes.status === 200, "Disabled domain via admin API");

    const disabledBoot = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${officeSiteKey}`, {
        headers: { Origin: "https://testbrokerage.com" }
    });
    assert(disabledBoot.status === 403, "Serving Worker blocked bootstrap on disabled domain with HTTP 403");

    // Delete domain
    const delDomRes = await fetch(`${ADMIN_URL}/api/admin/domains/${newDomId}`, {
        method: "DELETE",
        headers: {
            "Cookie": cookieVal,
            "Origin": ADMIN_URL
        }
    });
    assert(delDomRes.status === 200, "Deleted domain via admin API");

    // 12. Test CSRF Protection
    console.log("\n[12] Testing CSRF Safety Protection on Admin Mutations...");
    const csrfFailRes = await fetch(`${ADMIN_URL}/api/admin/accounts/${createdAgentAccountId}`, {
        method: "PATCH",
        headers: {
            "Cookie": cookieVal,
            "Content-Type": "application/json",
            "Origin": "https://malicious-attacker-site.com"
        },
        body: JSON.stringify({ plan: "trial" })
    });
    assert(csrfFailRes.status === 403, "Cross-origin untrusted mutation rejected with HTTP 403 CSRF failure");

    // 13. Test Generated Embed Snippets
    console.log("\n[13] Verifying Generated Embed Snippets Completeness...");
    const embedRes = await fetch(`${ADMIN_URL}/api/admin/sites/${createdOfficeSiteId}/embed`, {
        headers: { "Cookie": cookieVal }
    });
    assert(embedRes.status === 200, "Embed endpoint returned HTTP 200 OK");
    const embedData = await embedRes.json();
    assert(embedData.snippets?.search?.operational === true, "Search snippet is marked operational");
    assert(embedData.snippets?.search_bar?.operational === true, "Search bar snippet is marked operational");
    assert(embedData.snippets?.listing_grid?.operational === true, "Listing grid snippet is marked operational");
    assert(embedData.snippets?.open_houses?.operational === true, "Open houses snippet is marked operational");
    assert(embedData.snippets?.search?.htmlSnippet.includes(officeSiteKey), "Snippet contains correct siteKey");

    // 14. Test Session Revocation & Replay Prevention on Live Worker
    console.log("\n[14] Testing Live Logout & Session Revocation...");
    const logoutRes = await fetch(`${ADMIN_URL}/api/admin/logout`, {
        method: "POST",
        headers: {
            "Cookie": cookieVal,
            "Origin": ADMIN_URL
        }
    });
    assert(logoutRes.status === 200, "Logout returned HTTP 200 OK");

    const replayRes = await fetch(`${ADMIN_URL}/api/admin/dashboard`, {
        headers: { "Cookie": cookieVal }
    });
    assert(replayRes.status === 401, "Replaying revoked session cookie returns HTTP 401 Unauthorized");

    console.log("\n====================================================");
    console.log(`PROVISIONING VALIDATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log("====================================================");

    if (failed > 0) process.exit(1);
}

runProvisioningTests().catch(err => {
    console.error("Test Execution Error:", err);
    process.exit(1);
});
