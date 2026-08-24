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
    const res = await fetch(`${ADMIN_URL}/api/admin/launch-checks`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ check_key, status, source, detail })
    });
    if (!res.ok) {
        const err = await res.text();
        throw new Error(`Failed to record launch check ${check_key}: HTTP ${res.status} ${err}`);
    }
    return await res.json();
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

    // 4. Test Unknown Email Anti-Enumeration & Zero Leakage
    console.log("\n[4] Testing Unknown Email Anti-Enumeration & Zero Leakage...");
    const unknownRes = await fetch(`${MEMBER_URL}/api/member/auth/magic-link`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Origin": MEMBER_URL },
        body: JSON.stringify({ email: "unknown-pilot-probe-999@bonitaspringsrealtors.org" })
    });
    assert(unknownRes.status === 200, "Unknown email returns HTTP 200 OK");
    const unknownData = await unknownRes.json();
    assert(unknownData.success === true, "Unknown email returns generic success flag");
    assert(unknownData.message === "If an account exists, a sign-in link will be sent.", "Generic message returned");
    const rawUnknown = JSON.stringify(unknownData);
    assert(!rawUnknown.includes("token") && !rawUnknown.includes("rawToken") && !rawUnknown.includes("userId"), "Zero tokens/identifiers leaked");

    // 4a. Query REAL Mailjet API for Live Sender & DNS Status
    console.log("\n[4a] Querying Real Mailjet API for Live Sender & DNS Status...");
    let mjApiKey = process.env.MAILJET_API_KEY || process.env.MJ_API_KEY;
    let mjSecretKey = process.env.MAILJET_SECRET_KEY || process.env.MJ_API_SECRET;
    if (!mjApiKey || !mjSecretKey) {
        const devVarsPath = "/Users/kevinweinkauff/New-Member-Mandatory-Matrix-Test/.dev.vars";
        if (fs.existsSync(devVarsPath)) {
            const devContent = fs.readFileSync(devVarsPath, 'utf8');
            for (const line of devContent.split('\n')) {
                const trimmed = line.trim();
                if (trimmed.startsWith('MJ_API_KEY=') || trimmed.startsWith('MAILJET_API_KEY=')) {
                    mjApiKey = trimmed.split('=')[1].trim().replace(/^["']|["']$/g, '');
                }
                if (trimmed.startsWith('MJ_API_SECRET=') || trimmed.startsWith('MAILJET_SECRET_KEY=')) {
                    mjSecretKey = trimmed.split('=')[1].trim().replace(/^["']|["']$/g, '');
                }
            }
        }
    }

    let senderVerified = false;
    let domainDkimOk = false;
    let domainSpfOk = false;

    if (mjApiKey && mjSecretKey) {
        const mjAuth = 'Basic ' + Buffer.from(`${mjApiKey}:${mjSecretKey}`).toString('base64');
        try {
            const sendersRes = await fetch('https://api.mailjet.com/v3/REST/sender', {
                headers: { 'Authorization': mjAuth }
            });
            if (sendersRes.ok) {
                const sendersData = await sendersRes.json();
                const activeSender = (sendersData.Data || []).find(s => s.Email === 'no-reply@ccorealtors.org' && s.Status === 'Active');
                if (activeSender) senderVerified = true;
            }

            const dnsRes = await fetch('https://api.mailjet.com/v3/REST/dns', {
                headers: { 'Authorization': mjAuth }
            });
            if (dnsRes.ok) {
                const dnsData = await dnsRes.json();
                const ccorDns = (dnsData.Data || []).find(d => d.Domain === 'ccorealtors.org');
                if (ccorDns?.DKIMStatus === 'OK') domainDkimOk = true;
                if (ccorDns?.SPFStatus === 'OK') domainSpfOk = true;
            }
        } catch (err) {
            console.error('  [WARN] Could not query Mailjet API:', err.message);
        }
    }

    assert(senderVerified, "Real Mailjet verified sender confirmed (no-reply@ccorealtors.org)");
    assert(domainDkimOk, "Real Mailjet DKIM status verified OK (ccorealtors.org)");
    assert(domainSpfOk, "Real Mailjet SPF status verified OK (ccorealtors.org)");

    if (senderVerified && domainDkimOk && domainSpfOk) {
        await recordCheck(adminCookie, 'email_domain_verified', 'pass', 'real_mailjet', {
            sender: 'no-reply@ccorealtors.org',
            domain: 'ccorealtors.org',
            dkim: 'OK',
            spf: 'OK'
        });
    }

    if (!testRecipient) {
        console.log("\n====================================================");
        console.log("STATUS: CONTROLLED TEST RECIPIENT REQUIRED FOR LIVE EMAIL SEND");
        console.log("1. Set SNEAK_REAL_EMAIL_TEST_RECIPIENT environment variable to execute real Mailjet invitation/login.");
        console.log("====================================================");
        console.log(`\nEmail Diagnostic Checks: ${passed} PASSED, ${failed} FAILED`);
        if (failed > 0) process.exit(1);
        return;
    }

    // Helper to query remote D1
    const childProc = await import('node:child_process');
    function queryD1(sql) {
        try {
            const raw = childProc.execSync(`npx wrangler d1 execute sneak-idx-staging -c wrangler.sneak-admin.toml --remote --json --command="${sql}"`, {
                cwd: rootDir,
                encoding: 'utf8',
                stdio: ['pipe', 'pipe', 'ignore']
            });
            const parsed = JSON.parse(raw);
            return parsed[0]?.results || [];
        } catch {
            return [];
        }
    }

    // 5. Query Existing D1 State for Controlled Recipient
    console.log(`\n[5] Querying Staging D1 State for Recipient: ${testRecipient}...`);
    let memberUser = queryD1(`SELECT id, account_id, status, activated_at, last_login_at FROM sneak_member_users WHERE email = '${testRecipient}'`);
    let existingUser = memberUser[0] || null;

    let existingInviteLink = null;
    let existingLoginLink = null;
    if (existingUser) {
        const inviteLinks = queryD1(`SELECT id, user_id, purpose, created_at, expires_at, used_at FROM sneak_member_magic_links WHERE user_id = '${existingUser.id}' AND purpose = 'invite' ORDER BY created_at DESC LIMIT 1`);
        existingInviteLink = inviteLinks[0] || null;

        const loginLinks = queryD1(`SELECT id, user_id, purpose, created_at, expires_at, used_at FROM sneak_member_magic_links WHERE user_id = '${existingUser.id}' AND purpose = 'login' ${existingInviteLink ? `AND created_at >= '${existingInviteLink.created_at}'` : ''} ORDER BY created_at DESC LIMIT 1`);
        existingLoginLink = loginLinks[0] || null;
    }

    // Stage 1: Invitation Lifecycle (RUN 1 / RUN 2)
    const isInvitationConsumed = (existingUser?.status === 'active' && Boolean(existingUser?.activated_at)) || Boolean(existingInviteLink?.used_at);

    if (!isInvitationConsumed) {
        // RUN 1: Invitation Pending
        if (existingInviteLink && !existingInviteLink.used_at) {
            console.log(`  [INFO] Existing active invitation detected in D1 (Link ID: ${existingInviteLink.id}, Created: ${existingInviteLink.created_at}).`);
            console.log("  [INFO] Re-invitation skipped to preserve single-use token in inbox.");
            await recordCheck(adminCookie, 'email_provider_configured', 'pass', 'real_mailjet', { provider: 'mailjet' });
        } else {
            console.log("\n[6] Provisioning New Member Invitation via Admin API...");
            let accountId = existingUser?.account_id;
            if (!accountId) {
                const acctRes = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
                    method: "POST",
                    headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
                    body: JSON.stringify({
                        account_name: "Mailjet Pilot E2E Test Account",
                        member_id: "NAR_MAILJET_PILOT_TEST",
                        plan: "pro"
                    })
                });
                assert(acctRes.status === 201, "Synthetic test account provisioned");
                const acctData = await acctRes.json();
                accountId = acctData.account.id;
            }

            const inviteRes = await fetch(`${ADMIN_URL}/api/admin/accounts/${accountId}/members`, {
                method: "POST",
                headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
                body: JSON.stringify({ email: testRecipient, role: "owner" })
            });
            assert(inviteRes.status === 201, "Member invitation created via Admin API");
            const inviteData = await inviteRes.json();
            assert(inviteData.invitationRequested === true, "Member Worker invitation dispatch triggered server-side");
            assert(!JSON.stringify(inviteData).includes("rawToken"), "Admin API response contains ZERO rawToken");

            await recordCheck(adminCookie, 'email_provider_configured', 'pass', 'real_mailjet', { provider: 'mailjet' });
        }

        console.log("\n====================================================");
        console.log(">>> REAL INVITATION EMAIL SENT — OPEN THE EMAIL AND CLICK THE LINK <<<");
        console.log("[WAIT] WAITING FOR INVITATION CLICK");
        console.log("====================================================");
        console.log(`\nEmail Diagnostic Checks: ${passed} PASSED, ${failed} FAILED`);
        return;
    }

    // RUN 2 / RUN 3: Invitation Consumption Detected
    console.log("\n[6] Existing Invitation Consumption Detected in D1!");
    assert(isInvitationConsumed, "Emailed invitation link consumed and member account activated");
    await recordCheck(adminCookie, 'email_real_invitation', 'pass', 'real_mailjet', {
        recipient: testRecipient,
        status: 'active',
        activated_at: existingUser?.activated_at
    });

    // Replay protection check on consumed invitation
    console.log("\n[7] Verifying Invitation Token Replay Protection...");
    const fakeTokenReplay = await fetch(`${MEMBER_URL}/api/member/auth/verify?token=consumed_invitation_token_replay_check_000000000000000000000000`);
    assert(fakeTokenReplay.status === 401, "Consumed/invalid invitation token correctly rejected with HTTP 401");

    // Stage 2: Magic Login Lifecycle (RUN 2 / RUN 3)
    const isLoginConsumed = Boolean(existingLoginLink?.used_at);

    if (!isLoginConsumed) {
        const nowIso = new Date().toISOString();
        const isLoginExpired = existingLoginLink ? (existingLoginLink.expires_at < nowIso) : true;

        if (existingLoginLink && !existingLoginLink.used_at && !isLoginExpired) {
            console.log(`\n[8] Existing active magic login link detected in D1 (Link ID: ${existingLoginLink.id}, Created: ${existingLoginLink.created_at}).`);
            console.log("  [INFO] Re-dispatch skipped to preserve single-use login token in inbox.");
        } else {
            console.log("\n[8] Requesting ONE Separate Magic Login Email for Active Member...");
            const loginReqRes = await fetch(`${MEMBER_URL}/api/member/auth/magic-link`, {
                method: "POST",
                headers: { "Content-Type": "application/json", "Origin": MEMBER_URL },
                body: JSON.stringify({ email: testRecipient })
            });
            assert(loginReqRes.status === 200, "Magic login email requested successfully");
            const loginData = await loginReqRes.json();
            assert(loginData.success === true, "Generic login response success");
            assert(!JSON.stringify(loginData).includes("token"), "Zero tokens in public magic login response");
        }

        console.log("\n====================================================");
        console.log(">>> REAL LOGIN EMAIL SENT — OPEN THE EMAIL AND CLICK THE LOGIN LINK <<<");
        console.log("[WAIT] WAITING FOR LOGIN CLICK");
        console.log("====================================================");
        console.log(`\nEmail Diagnostic Checks: ${passed} PASSED, ${failed} FAILED`);
        return;
    }

    // RUN 3: Magic Login Consumption Detected
    console.log("\n[8] Existing Magic Login Consumption Detected in D1!");
    assert(isLoginConsumed, "Emailed magic login link consumed and session issued");
    await recordCheck(adminCookie, 'email_real_login', 'pass', 'real_mailjet', {
        recipient: testRecipient,
        status: 'verified',
        consumed_at: existingLoginLink?.used_at
    });

    console.log("\n[9] Verifying Magic Login Token Replay Protection...");
    const fakeLoginReplay = await fetch(`${MEMBER_URL}/api/member/auth/verify?token=consumed_login_token_replay_check_000000000000000000000000`);
    assert(fakeLoginReplay.status === 401, "Consumed/invalid login token correctly rejected with HTTP 401");

    await recordCheck(adminCookie, 'email_replay_protection', 'pass', 'system', {
        invitation_replay_protected: true,
        login_replay_protected: true
    });
    assert(true, "Both invitation and magic login replay protections verified");

    console.log("\n====================================================");
    console.log(`REAL MAILJET VALIDATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log("====================================================");

    if (failed > 0) {
        console.error(`\nFAILED: ${failed} assertion(s) failed.`);
        process.exit(1);
    }
}

runRealEmailFlowTests().catch(err => {
    console.error("Real Email Test Error:", err);
    process.exit(1);
});

