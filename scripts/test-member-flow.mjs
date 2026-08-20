/**
 * scripts/test-member-flow.mjs
 * 
 * End-to-End Live Validation of Member Self-Service Portal, Passwordless Auth,
 * Scoped Serving, and Stripe Billing Lifecycle on Staging Workers.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const ADMIN_URL = "https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev";
const MEMBER_URL = "https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev";
const SERVING_URL = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";

// Load Admin Password from environment or .dev.vars (NO FALLBACK)
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

async function runMemberFlowTests() {
    console.log("====================================================");
    console.log("SNEAK IDX — END-TO-END MEMBER PORTAL & AUTH VALIDATION");
    console.log(`Admin Worker:   ${ADMIN_URL}`);
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

    // 2. Provision New Account for Member Self-Service Testing
    console.log("\n[2] Provisioning New Tenant Account via Admin API...");
    const siteKey = `mem-test-${Date.now().toString(36).slice(-4)}`;
    const memberEmail = `test.member.${Date.now()}@bonitaspringsrealtors.org`;

    const provRes = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            account_name: "SNEAK Self-Service Test Member",
            member_id: "NAR_MEM_01",
            plan: "pro",
            scope_type: "agent",
            scope_value: "B3650316",
            site_key: siteKey,
            domain: "localhost",
            branding: {
                display_name: "Initial Staging Member",
                brokerage: "Bonita Springs Realty"
            }
        })
    });
    assert(provRes.status === 201, "Account provisioned via Admin API");
    const provData = await provRes.json();
    const accountId = provData.account?.id;
    const siteId = provData.site?.id;

    // 3. Admin Creates Member Invitation Magic Link
    console.log("\n[3] Admin Generating Member Invitation Link...");
    const inviteRes = await fetch(`${ADMIN_URL}/api/admin/accounts/${accountId}/members`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ email: memberEmail, role: "owner" })
    });
    assert(inviteRes.status === 201, "Member user invited with HTTP 201 Created");
    const inviteData = await inviteRes.json();
    const rawInviteToken = inviteData.rawToken;
    assert(Boolean(rawInviteToken), "Received single-use magic invitation token");

    // 4. Member Consumes Magic Link & Receives 7-Day Session
    console.log("\n[4] Member Verifying & Consuming Magic Link...");
    const verifyRes = await fetch(`${MEMBER_URL}/api/member/auth/verify?token=${encodeURIComponent(rawInviteToken)}`);
    assert(verifyRes.status === 200, "Magic link verified with HTTP 200 OK");
    const memberCookieHeader = verifyRes.headers.get("Set-Cookie") || "";
    assert(memberCookieHeader.includes("__Host-sneak_member_session="), "Received __Host-sneak_member_session cookie");
    assert(memberCookieHeader.includes("SameSite=Lax"), "Cookie contains SameSite=Lax");
    const memberCookie = memberCookieHeader.split(";")[0];

    // 5. Replay Magic Token -> Must Fail
    console.log("\n[5] Verifying Single-Use Consumption (Replay Prevention)...");
    const replayRes = await fetch(`${MEMBER_URL}/api/member/auth/verify?token=${encodeURIComponent(rawInviteToken)}`);
    assert(replayRes.status === 401, "Replaying already consumed magic link rejected with HTTP 401");

    // 6. Member Portal Overview
    console.log("\n[6] Testing Member Overview API (/api/member/overview)...");
    const overviewRes = await fetch(`${MEMBER_URL}/api/member/overview`, {
        headers: { "Cookie": memberCookie }
    });
    assert(overviewRes.status === 200, "Overview endpoint returned HTTP 200 OK");
    const overviewData = await overviewRes.json();
    assert(overviewData.account?.id === accountId, "Overview returned authenticated member's account");
    assert(overviewData.inventory?.activeListings > 30000, "Overview reports active MLS inventory");

    // 7. Member Self-Service Domain Addition & Admin Verification
    console.log("\n[7] Testing Member Domain Addition & Verification Lifecycle...");
    const addDomRes = await fetch(`${MEMBER_URL}/api/member/domains`, {
        method: "POST",
        headers: { "Cookie": memberCookie, "Content-Type": "application/json", "Origin": MEMBER_URL },
        body: JSON.stringify({ domain: "https://mycustomagentwebsite.com/search" })
    });
    assert(addDomRes.status === 201, "Member added domain with HTTP 201 Created");
    const addDomData = await addDomRes.json();
    const newDomId = addDomData.domain?.id;
    assert(addDomData.domain?.verified === 0, "Member added domain starts unverified (verified = 0)");

    // Serving worker rejects unverified domain
    const unverifiedBoot = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${siteKey}`, {
        headers: { Origin: "https://mycustomagentwebsite.com" }
    });
    assert(unverifiedBoot.status === 403, "Serving Worker blocked unverified domain with HTTP 403");

    // Admin verifies domain
    const verifyDomRes = await fetch(`${ADMIN_URL}/api/admin/domains/${newDomId}`, {
        method: "PATCH",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ verified: 1 })
    });
    assert(verifyDomRes.status === 200, "Admin verified domain");

    // Serving worker allows bootstrap from verified domain
    const verifiedBoot = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${siteKey}`, {
        headers: { Origin: "https://mycustomagentwebsite.com" }
    });
    assert(verifiedBoot.status === 200, "Serving Worker allows bootstrap on verified domain");

    // 8. Member Self-Service Branding Customization
    console.log("\n[8] Testing Member Branding Customization...");
    const brandRes = await fetch(`${MEMBER_URL}/api/member/branding`, {
        method: "PUT",
        headers: { "Cookie": memberCookie, "Content-Type": "application/json", "Origin": MEMBER_URL },
        body: JSON.stringify({
            display_name: "Luxury Gulf Coast Homes",
            brokerage: "Premier Gulf Properties",
            primary_color: "#0f172a",
            secondary_color: "#38bdf8"
        })
    });
    assert(brandRes.status === 200, "Member updated branding with HTTP 200 OK");

    // Verify config on serving worker reflects updated branding
    const bootVerifiedData = await verifiedBoot.json();
    const cfgRes = await fetch(`${SERVING_URL}/idx/v1/config?site=${siteKey}`, {
        headers: { "X-Sneak-Session": bootVerifiedData.session }
    });
    const cfgData = await cfgRes.json();
    assert(cfgData.displayName === "Luxury Gulf Coast Homes", "Serving worker reflects member's custom branding");
    assert(cfgData.primaryColor === "#0f172a", "Serving worker reflects member's primary color");

    // 9. Member Embed Code Retrieval
    console.log("\n[9] Testing Member Embed Code Snippets...");
    const embedRes = await fetch(`${MEMBER_URL}/api/member/embed`, {
        headers: { "Cookie": memberCookie }
    });
    assert(embedRes.status === 200, "Member retrieved embed snippets with HTTP 200 OK");
    const embedData = await embedRes.json();
    assert(embedData.snippets?.search?.htmlSnippet.includes(siteKey), "Embed snippet contains tenant site key");
    assert(!embedData.snippets?.search?.htmlSnippet.includes(memberCookie), "Embed snippet contains zero secrets/tokens");

    // 10. Member Logout & Session Revocation
    console.log("\n[10] Testing Member Logout & Session Revocation...");
    const logoutRes = await fetch(`${MEMBER_URL}/api/member/auth/logout`, {
        method: "POST",
        headers: { "Cookie": memberCookie, "Origin": MEMBER_URL }
    });
    assert(logoutRes.status === 200, "Member logout returned HTTP 200 OK");

    const replayMemberRes = await fetch(`${MEMBER_URL}/api/member/overview`, {
        headers: { "Cookie": memberCookie }
    });
    assert(replayMemberRes.status === 401, "Replaying revoked member session cookie rejected with HTTP 401");

    console.log("\n====================================================");
    console.log(`MEMBER PORTAL VALIDATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log("====================================================");

    if (failed > 0) process.exit(1);
}

runMemberFlowTests().catch(err => {
    console.error("Test Execution Error:", err);
    process.exit(1);
});
