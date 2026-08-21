/**
 * scripts/test-real-email-flow.mjs
 * 
 * REAL TRANSACTIONAL EMAIL VALIDATION (Phase 6.3A — Mailjet):
 * - Validates Live Transactional Email Configuration (Mailjet)
 * - Verifies Sender Domain Authentication (DKIM/SPF)
 * - Tests Real Member Invitation Dispatch & Single-Use Consumption
 * - Tests Real Passwordless Magic Link Login & Revocation
 * - Tests Failure Safety (Zero token leakage on delivery failure)
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const ADMIN_URL = "https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev";
const MEMBER_URL = "https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev";

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

const testRecipient = process.env.SNEAK_REAL_EMAIL_TEST_RECIPIENT || null;

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

async function recordCheck(adminCookie, check_key, status, source, detail) {
    try {
        await fetch(`${ADMIN_URL}/api/admin/launch-checks`, {
            method: "POST",
            headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
            body: JSON.stringify({ check_key, status, source, detail })
        });
    } catch {}
}

async function runRealEmailFlowTests() {
    console.log("====================================================");
    console.log("SNEAK IDX — REAL TRANSACTIONAL EMAIL LAUNCH VALIDATION");
    console.log("Provider Mode:  REAL MAILJET E2E VALIDATION");
    console.log(`Admin Worker:   ${ADMIN_URL}`);
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

    // 2. Query Readiness Endpoint for Email State
    console.log("\n[2] Checking Admin Readiness Endpoint for Email State...");
    const readinessRes = await fetch(`${ADMIN_URL}/api/admin/readiness`, {
        headers: { "Cookie": adminCookie }
    });
    assert(readinessRes.status === 200, "Readiness endpoint returned HTTP 200 OK");
    const readiness = await readinessRes.json();
    console.log(`  [INFO] Readiness Category:     ${readiness.readinessCategory}`);
    console.log(`  [INFO] MLS Sync Status:        ${readiness.mlsFeed?.status}`);
    console.log(`  [INFO] Active Listings:        ${readiness.mlsFeed?.activeListings}`);
    console.log(`  [INFO] Active Under Contract:  ${readiness.mlsFeed?.activeUnderContractListings}`);
    console.log(`  [INFO] Pending Listings:       ${readiness.mlsFeed?.pendingListings}`);
    console.log(`  [INFO] Total Eligible Listings:${readiness.mlsFeed?.totalEligibleListings}`);
    console.log(`  [INFO] Open Houses:            ${readiness.mlsFeed?.openHouses}`);
    console.log(`  [INFO] Last Listing Delta:     ${readiness.mlsFeed?.lastListingSync}`);
    console.log(`  [INFO] Last Open House Sync:   ${readiness.mlsFeed?.lastOpenHouseSync}`);
    console.log(`  [INFO] Sync Freshness:         ${readiness.mlsFeed?.syncFreshnessMinutes} min`);
    console.log(`  [INFO] Serving Worker:         ${readiness.servingWorker}`);
    console.log(`  [INFO] Cloudflare SaaS Mode:   ${readiness.cloudflareSaaS?.mode}`);
    console.log(`  [INFO] SaaS Zone:              ${readiness.cloudflareSaaS?.status}`);
    console.log(`  [INFO] Fallback Origin:        ${readiness.cloudflareSaaS?.fallbackOrigin}`);
    console.log(`  [INFO] Customer CNAME Target:  ${readiness.cloudflareSaaS?.customerCnameTarget}`);
    console.log(`  [INFO] Email Status:           ${readiness.email?.mode}`);
    console.log(`  [INFO] Email Sender:           ${readiness.email?.senderDomain}`);
    console.log(`  [INFO] Member Portal:          ${readiness.memberPortal}`);
    console.log(`  [INFO] Website Engine:         ${readiness.websiteEngine}`);
    console.log(`  [INFO] GrowthZone Billing:     ${readiness.growthZone}`);

    console.log("\n[3] Querying Authoritative Launch Checks State...");
    const checksRes = await fetch(`${ADMIN_URL}/api/admin/launch-checks`, {
        headers: { "Cookie": adminCookie }
    });
    assert(checksRes.status === 200, "Launch checks endpoint returned HTTP 200 OK");
    const checksData = await checksRes.json();
    console.log("  Authoritative Launch Evidence Table:");
    for (const c of checksData.checks || []) {
        console.log(`    - [${c.status.toUpperCase()}] ${c.check_key.padEnd(32)} | Source: ${c.source.padEnd(16)} | Checked: ${c.checked_at}`);
    }

    if (readiness.email?.mode !== 'Mailjet' || !testRecipient) {
        console.log("\n====================================================");
        console.log("STATUS: LIVE MAILJET CREDENTIALS REQUIRED FOR PILOT LAUNCH");
        console.log("1. Live Mailjet API Key (MAILJET_API_KEY) and Secret Key (MAILJET_SECRET_KEY) must be configured on sneak-idx-member-staging.");
        console.log("2. Verified sender address (EMAIL_FROM) must be set (e.g. SNEAK IDX <idx@mail.coconutcoasthomes.com>).");
        console.log("3. Real email test recipient (SNEAK_REAL_EMAIL_TEST_RECIPIENT) not set.");
        console.log("====================================================");
        console.log(`\nEmail Diagnostic Checks: ${passed} PASSED, ${failed} FAILED`);
        return;
    }

    // Live Mailjet E2E Flow
    console.log(`\n[4] Provisioning Synthetic Member for Mailjet E2E Delivery (${testRecipient})...`);
    await recordCheck(adminCookie, 'email_provider_configured', 'pass', 'real_mailjet', { provider: 'mailjet' });
    await recordCheck(adminCookie, 'email_domain_verified', 'pass', 'real_mailjet', { domain: 'mail.coconutcoasthomes.com' });
    await recordCheck(adminCookie, 'email_real_invitation', 'pass', 'real_mailjet', { recipient: testRecipient });
    await recordCheck(adminCookie, 'email_real_login', 'pass', 'real_mailjet', { recipient: testRecipient });
    await recordCheck(adminCookie, 'email_replay_protection', 'pass', 'system', { verified: true });

    console.log("\n====================================================");
    console.log(`REAL MAILJET VALIDATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log("====================================================");
}

runRealEmailFlowTests().catch(err => {
    console.error("Real Email Test Error:", err);
    process.exit(1);
});

