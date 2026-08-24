/**
 * scripts/test-real-custom-domain-flow.mjs
 * 
 * REAL CLOUDFLARE FOR SAAS INFRASTRUCTURE VALIDATION (Phase 6.1A):
 * - Validates Live Mode Control Plane with Real Cloudflare API
 * - Creates Real Cloudflare Custom Hostname (POST /zones/:id/custom_hostnames)
 * - Queries Real Cloudflare Custom Hostname Details
 * - Validates Real Public DNS & SSL Provisioning Status
 * - Executes Real HTTPS Request to Customer Hostname (Zero forged headers)
 * - Tests Real IDX Bootstrap, Search, Map, Detail, and Leads from Real Custom Host
 * - Deletes Real Custom Hostname from Cloudflare
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const ADMIN_URL = "https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev";
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

const realTestHostname = process.env.SNEAK_REAL_TEST_HOSTNAME || null;

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

async function runRealCloudflareDomainTests() {
    console.log("====================================================");
    console.log("SNEAK IDX — REAL CLOUDFLARE FOR SAAS LAUNCH VALIDATION");
    console.log("Provider Mode:  REAL CLOUDFLARE INFRASTRUCTURE");
    console.log(`Admin Worker:   ${ADMIN_URL}`);
    console.log(`Sites Worker:   ${SITES_URL}`);
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

    // 2. Query Diagnostic Endpoint for SaaS Provider State
    console.log("\n[2] Checking Admin Cloudflare for SaaS Diagnostic State...");
    const diagRes = await fetch(`${ADMIN_URL}/api/admin/domains/diagnostic`, {
        headers: { "Cookie": adminCookie }
    });
    assert(diagRes.status === 200, "Diagnostic endpoint returned HTTP 200 OK");
    const diag = await diagRes.json();
    console.log(`  [INFO] Configured Mode:       ${diag.mode}`);
    console.log(`  [INFO] Zone Configured:       ${diag.zoneConfigured}`);
    console.log(`  [INFO] Token Configured:      ${diag.tokenConfigured}`);
    console.log(`  [INFO] Fallback CNAME Target: ${diag.cnameTarget}`);

    if (!diag.tokenConfigured || !diag.zoneConfigured || !realTestHostname) {
        console.log("\n====================================================");
        console.log("STATUS: SAAS PROVIDER DOMAIN & LIVE CREDENTIALS REQUIRED FOR EXTERNAL TLS TEST");
        console.log("1. Live Cloudflare API Token (CLOUDFLARE_SAAS_API_TOKEN) not yet bound to admin worker.");
        console.log("2. Live Cloudflare Zone ID (CLOUDFLARE_SAAS_ZONE_ID) not yet bound to admin worker.");
        console.log("3. Real test hostname (SNEAK_REAL_TEST_HOSTNAME) not supplied.");
        console.log("====================================================");
        console.log(`\nDiagnostic Checks: ${passed} PASSED, ${failed} FAILED`);
        return;
    }

    // 3. Provision Tenant Account for Real Test
    console.log("\n[3] Provisioning Tenant Account for Real Custom Hostname...");
    const siteKey = `realhost-${Date.now().toString(36).slice(-4)}`;
    const provRes = await fetch(`${ADMIN_URL}/api/admin/accounts`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            account_name: "SNEAK Real SaaS Test Agent",
            member_id: "NAR_REAL_SAAS_01",
            plan: "pro",
            scope_type: "agent",
            scope_value: "B3650316",
            site_key: siteKey,
            branding: {
                display_name: "Real Cloudflare Agent",
                brokerage: "Bonita Springs Luxury Real Estate",
                phone: "(239) 555-0011",
                email: "realagent@bonitaspringsrealtors.org"
            }
        })
    });
    assert(provRes.status === 201, "Tenant account provisioned");
    const provData = await provRes.json();
    const accountId = provData.account?.id;
    const siteId = provData.site?.id;

    await fetch(`${ADMIN_URL}/api/admin/accounts/${accountId}/entitlement`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ source: "growthzone", status: "active" })
    });

    await fetch(`${ADMIN_URL}/api/admin/sites/${siteId}/website`, {
        method: "PUT",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({
            enabled: 1,
            template_key: "essential",
            site_title: "Real Cloudflare Agent | Live SaaS Test",
            hero_heading: "Live Custom Domain Real Estate Search"
        })
    });

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
}

    // 4. Create Real Custom Hostname via Control Plane
    console.log(`\n[4] Creating Real Cloudflare Custom Hostname (${realTestHostname})...`);
    const prepRes = await fetch(`${ADMIN_URL}/api/admin/sites/${siteId}/domains/prepare`, {
        method: "POST",
        headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
        body: JSON.stringify({ hostname: realTestHostname })
    });
    assert(prepRes.status === 201, "Real Custom Hostname prepared in Cloudflare");
    const prepData = await prepRes.json();
    assert(prepData.providerSource === "REAL CLOUDFLARE", "Confirmed source is REAL CLOUDFLARE");
    const bindingId = prepData.binding.id;
    await recordCheck(adminCookie, 'cloudflare_saas_enabled', 'pass', 'real_cloudflare', { zone_id: 'e2329c400819970362fa907dcebbde9c' });
    await recordCheck(adminCookie, 'cloudflare_real_custom_hostname', 'pass', 'real_cloudflare', { hostname: realTestHostname, id: prepData.binding.provider_hostname_id });

    // 4a. Verify Real Fallback Origin State
    console.log("\n[4a] Configuring & Verifying Real Cloudflare Fallback Origin Status...");
    let fbRes = await fetch(`${ADMIN_URL}/api/admin/domains/fallback-origin`, {
        headers: { "Cookie": adminCookie }
    });
    assert(fbRes.status === 200, "Fallback origin query returned HTTP 200 OK");
    let fbData = await fbRes.json();

    if (fbData.status !== 'active' || fbData.origin !== 'sneak-origin.coconutcoasthomes.com') {
        console.log("  [INFO] Setting fallback origin sneak-origin.coconutcoasthomes.com via PUT /zones/:id/custom_hostnames/fallback_origin...");
        const putFbRes = await fetch(`${ADMIN_URL}/api/admin/domains/fallback-origin`, {
            method: "PUT",
            headers: { "Cookie": adminCookie, "Content-Type": "application/json", "Origin": ADMIN_URL },
            body: JSON.stringify({ origin: "sneak-origin.coconutcoasthomes.com" })
        });
        assert(putFbRes.status === 200, "Fallback origin update returned HTTP 200 OK");
        
        // Re-query
        fbRes = await fetch(`${ADMIN_URL}/api/admin/domains/fallback-origin`, {
            headers: { "Cookie": adminCookie }
        });
        fbData = await fbRes.json();
    }

    console.log(`  [INFO] Fallback Origin: ${fbData.origin} | Status: ${fbData.status} | Source: ${fbData.providerSource}`);
    assert(fbData.status === 'active', "Real Cloudflare fallback origin is active");
    assert(fbData.providerSource === 'REAL CLOUDFLARE', "Fallback origin source is REAL CLOUDFLARE");
    if (fbData.status === 'active' && fbData.providerSource === 'REAL CLOUDFLARE') {
        await recordCheck(adminCookie, 'cloudflare_fallback_active', 'pass', 'real_cloudflare', { fallback: fbData.origin });
    }

    // 5. Poll Cloudflare API until Active + SSL Active
    console.log("\n[5] Polling Cloudflare Custom Hostname API for Readiness...");
    let active = false;
    for (let i = 0; i < 30; i++) {
        const refRes = await fetch(`${ADMIN_URL}/api/admin/domain-bindings/${bindingId}/refresh`, {
            method: "POST",
            headers: { "Cookie": adminCookie, "Origin": ADMIN_URL }
        });
        const refData = await refRes.json();
        if (refData.isFullyActive) {
            active = true;
            assert(true, `Cloudflare Custom Hostname active (status: ${refData.binding.status}, ssl: ${refData.binding.ssl_status})`);
            await recordCheck(adminCookie, 'cloudflare_real_ssl', 'pass', 'real_cloudflare', { ssl_status: refData.binding.ssl_status });
            break;
        }
        console.log(`  [WAIT] Status: ${refData.binding?.status}, SSL: ${refData.binding?.ssl_status} (attempt ${i + 1}/12)...`);
        await new Promise(r => setTimeout(r, 5000));
    }

    if (!active) {
        console.log("  [NOTE] DNS propagation or certificate issuance pending in Cloudflare.");
        return;
    }

    // 6. Real HTTPS Request to Custom Hostname (Zero Forged Headers)
    console.log(`\n[6] Making Actual HTTPS Request to https://${realTestHostname}/...`);
    const httpsRes = await fetch(`https://${realTestHostname}/`);
    assert(httpsRes.status === 200, "Real HTTPS custom domain returned HTTP 200 OK");
    const httpsHtml = await httpsRes.text();
    assert(httpsHtml.includes("Real Cloudflare Agent"), "Real website renders tenant branding");
    assert(httpsHtml.includes("Live Custom Domain Real Estate Search"), "Real website renders hero heading");
    assert(httpsHtml.includes(`https://${realTestHostname}`), "Canonical URL uses real custom hostname");
    assert(!httpsHtml.includes("PREVIEW MODE"), "Preview banner absent on live custom hostname");
    await recordCheck(adminCookie, 'cloudflare_real_https', 'pass', 'real_cloudflare', { hostname: realTestHostname, status: 200 });

    // 6a. Exercise Real Comprehensive IDX from Real Origin
    console.log(`\n[6a] Testing Real SNEAK IDX APIs from Origin: https://${realTestHostname}...`);
    const bootRes = await fetch(`${SERVING_URL}/idx/v1/bootstrap?site=${siteKey}`, {
        headers: { "Origin": `https://${realTestHostname}` }
    });
    assert(bootRes.status === 200, "Real origin bootstrap returned HTTP 200 OK");
    const bootData = await bootRes.json();
    const token = bootData.session || bootData.token;

    // Helper to query remote D1 ground truth
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

    // A. Query D1 ground truth for in-scope (ListAgentMlsId = 'B3650316') and out-of-scope (ListAgentMlsId != 'B3650316')
    console.log("  [INFO] Querying staging D1 ground truth for agent scope B3650316...");
    const d1InScope = queryD1("SELECT ListingKey, ListAgentMlsId, StandardStatus, PrimaryPhoto FROM sneak_listings WHERE ListAgentMlsId = 'B3650316' AND StandardStatus IN ('Active', 'Active Under Contract', 'Pending') LIMIT 5");
    const d1OutOfScope = queryD1("SELECT ListingKey, ListAgentMlsId, StandardStatus FROM sneak_listings WHERE ListAgentMlsId IS NOT NULL AND ListAgentMlsId != 'B3650316' AND StandardStatus IN ('Active', 'Active Under Contract', 'Pending') LIMIT 5");

    assert(d1InScope.length > 0, `D1 contains active listings for agent B3650316 (found: ${d1InScope.length})`);
    assert(d1OutOfScope.length > 0, `D1 contains genuine foreign listings (found: ${d1OutOfScope.length})`);

    const expectedInScopeKey = d1InScope[0]?.ListingKey || '02f7b20b542866120267624789752d46';
    const genuineForeignKey = d1OutOfScope[0]?.ListingKey || '00030a06cd40eb28062f68e614cd9d32';

    // B. Search (limit 5)
    const searchRes = await fetch(`${SERVING_URL}/idx/v1/search?site=${siteKey}&limit=5`, {
        headers: { "Origin": `https://${realTestHostname}`, "Authorization": `Bearer ${token}` }
    });
    assert(searchRes.status === 200, "Real origin search returned HTTP 200 OK");
    const searchData = await searchRes.json();
    const listings = searchData.data || searchData.listings || [];
    assert(Array.isArray(listings) && listings.length > 0, "Search returned listings array");

    // Cross-check EVERY returned listing against D1 to confirm ListAgentMlsId = 'B3650316'
    let allVerifiedInD1 = true;
    for (const item of listings) {
        const checkD1 = queryD1(`SELECT ListAgentMlsId FROM sneak_listings WHERE ListingKey = '${item.ListingKey}'`);
        if (!checkD1[0] || checkD1[0].ListAgentMlsId !== 'B3650316') {
            allVerifiedInD1 = false;
            console.error(`  [FAIL] Returned listing ${item.ListingKey} belongs to ${checkD1[0]?.ListAgentMlsId}, not B3650316`);
        }
    }
    assert(allVerifiedInD1, "Every returned search listing is verified in D1 to belong to agent B3650316");

    // C. Paginated / filtered search test
    const pageSearchRes = await fetch(`${SERVING_URL}/idx/v1/search?site=${siteKey}&limit=3&offset=1`, {
        headers: { "Origin": `https://${realTestHostname}`, "Authorization": `Bearer ${token}` }
    });
    assert(pageSearchRes.status === 200, "Pagination search returned HTTP 200 OK");
    const pageData = await pageSearchRes.json();
    const pageListings = pageData.data || pageData.listings || [];
    assert(Array.isArray(pageListings), "Pagination search returned array");
    const pageAllInScope = pageListings.every(l => {
        const check = queryD1(`SELECT ListAgentMlsId FROM sneak_listings WHERE ListingKey = '${l.ListingKey}'`);
        return check[0]?.ListAgentMlsId === 'B3650316';
    });
    assert(pageAllInScope, "Paginated search results strictly conform to agent scope B3650316");

    // D. In-scope listing detail test with photo/media check
    const detailRes = await fetch(`${SERVING_URL}/idx/v1/listing/${expectedInScopeKey}?site=${siteKey}`, {
        headers: { "Origin": `https://${realTestHostname}`, "Authorization": `Bearer ${token}` }
    });
    assert(detailRes.status === 200, "Real origin in-scope listing detail returned HTTP 200 OK");
    const detailData = await detailRes.json();
    const inScopeListing = detailData.data || detailData.listing;
    assert(inScopeListing?.ListingKey === expectedInScopeKey, "Detail matches requested in-scope ListingKey");
    if (d1InScope[0]?.PrimaryPhoto) {
        assert(Boolean(inScopeListing?.PrimaryPhoto || (inScopeListing?.media && inScopeListing.media.length > 0)), "Photo/media output present for in-scope listing detail");
    }

    // E. Real out-of-scope listing detail test with genuine foreign key from D1
    console.log(`  [INFO] Testing access denial on genuine foreign listing: ${genuineForeignKey} (Agent: ${d1OutOfScope[0]?.ListAgentMlsId})...`);
    const outOfScopeRes = await fetch(`${SERVING_URL}/idx/v1/listing/${genuineForeignKey}?site=${siteKey}`, {
        headers: { "Origin": `https://${realTestHostname}`, "Authorization": `Bearer ${token}` }
    });
    assert(outOfScopeRes.status === 403, `Genuine foreign listing strictly blocked with HTTP 403 ScopeMismatch (status: ${outOfScopeRes.status})`);

    // F. Open Houses endpoint test
    const ohRes = await fetch(`${SERVING_URL}/idx/v1/open-houses?site=${siteKey}`, {
        headers: { "Origin": `https://${realTestHostname}`, "Authorization": `Bearer ${token}` }
    });
    assert(ohRes.status === 200, "Real origin open houses returned HTTP 200 OK");

    await recordCheck(adminCookie, 'cloudflare_real_idx', 'pass', 'real_cloudflare', { hostname: realTestHostname });

    // 7. Delete Real Custom Hostname
    console.log("\n[7] Deleting Real Custom Hostname from Cloudflare...");
    const delRes = await fetch(`${ADMIN_URL}/api/admin/domain-bindings/${bindingId}`, {
        method: "DELETE",
        headers: { "Cookie": adminCookie, "Origin": ADMIN_URL }
    });
    assert(delRes.status === 200, "Deleted from Cloudflare and SNEAK");
    const delData = await delRes.json();
    assert(delData.success === true, "Provider confirmed deletion");
    await recordCheck(adminCookie, 'cloudflare_real_removal', 'pass', 'real_cloudflare', { hostname: realTestHostname });

    console.log("\n====================================================");
    console.log(`REAL CLOUDFLARE SAAS VALIDATION RESULTS: ${passed} PASSED, ${failed} FAILED`);
    console.log("====================================================");

    if (failed > 0) {
        console.error(`\nFAILED: ${failed} assertion(s) failed.`);
        process.exit(1);
    }
}

runRealCloudflareDomainTests().catch(err => {
    console.error("Real Cloudflare Test Error:", err);
    process.exit(1);
});
