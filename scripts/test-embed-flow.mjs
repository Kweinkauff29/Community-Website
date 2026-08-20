/**
 * scripts/test-embed-flow.mjs
 * 
 * End-to-End simulation and validation of embed.js lifecycle on external parent origins.
 */

const STAGING = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";

async function testEmbedFlow() {
    console.log("====================================================");
    console.log("SNEAK IDX — END-TO-END EMBED LIFECYCLE VALIDATION");
    console.log("====================================================");

    // 1. Fetch live embed.js from staging
    console.log("\n[1] Fetching live embed.js from staging...");
    const embedRes = await fetch(`${STAGING}/embed.js`);
    const embedCode = await embedRes.text();
    console.log(`  embed.js: HTTP ${embedRes.status} (${embedCode.length} bytes)`);
    if (!embedCode.includes("handleBootstrap") && !embedCode.includes("/idx/v1/bootstrap")) {
        throw new Error("embed.js does not contain expected bootstrap logic");
    }

    // 2. Simulate Authorized Parent Bootstrap (http://localhost:8090)
    console.log("\n[2] Executing Bootstrap from Parent Origin 'http://localhost:8090'...");
    const bootRes = await fetch(`${STAGING}/idx/v1/bootstrap?site=demo-ccor`, {
        headers: { Origin: "http://localhost:8090" }
    });
    const bootData = await bootRes.json();
    console.log(`  Bootstrap HTTP ${bootRes.status}:`, {
        success: bootData.success,
        expiresIn: bootData.expiresIn,
        siteKey: bootData.siteKey,
        hasSession: Boolean(bootData.session)
    });

    if (bootRes.status !== 200 || !bootData.session) {
        throw new Error("Authorized bootstrap failed");
    }

    // 3. Verify Iframe Construction
    console.log("\n[3] Verifying Iframe URL Construction...");
    const sessionToken = bootData.session;
    const iframeSrc = `${STAGING}/search/?site=demo-ccor&session=${encodeURIComponent(sessionToken)}&embed=true`;
    console.log(`  Constructed Iframe URL: ${iframeSrc.substring(0, 100)}...`);

    // 4. Fetch Search HTML & Verify CSP Headers
    console.log("\n[4] Fetching Iframe Search UI HTML & Checking CSP...");
    const iframeRes = await fetch(iframeSrc);
    const iframeHtml = await iframeRes.text();
    const csp = iframeRes.headers.get("content-security-policy");
    console.log(`  Search HTML: HTTP ${iframeRes.status} (${iframeHtml.length} bytes)`);
    console.log(`  CSP Header: ${csp}`);
    if (!csp || !csp.includes("http://localhost")) {
        throw new Error("CSP header does not permit http://localhost framing");
    }

    // 5. Verify Session URL Scrubbing Mechanism
    console.log("\n[5] Verifying Session URL Scrubbing Mechanism in Search UI...");
    if (!iframeHtml.includes("window.history.replaceState") || !iframeHtml.includes("urlParams.delete('session')")) {
        throw new Error("Search UI does not contain session scrubbing history.replaceState logic");
    }
    console.log("  PASS: Search UI contains window.history.replaceState() session scrubber.");

    // 6. Simulate Search UI In-Iframe Data Retrieval using Session Token
    console.log("\n[6] Simulating Search UI In-Iframe API Requests with Session Token...");
    const configRes = await fetch(`${STAGING}/idx/v1/config?site=demo-ccor`, {
        headers: { "X-SNEAK-Session": sessionToken }
    });
    const config = await configRes.json();
    console.log(`  GET /idx/v1/config: HTTP ${configRes.status} | Display: "${config.displayName}" | Color: ${config.primaryColor}`);

    const searchRes = await fetch(`${STAGING}/idx/v1/search?site=demo-ccor`, {
        headers: { "X-SNEAK-Session": sessionToken }
    });
    const searchData = await searchRes.json();
    console.log(`  GET /idx/v1/search: HTTP ${searchRes.status} | Total: ${searchData.pagination.total} listings`);

    const mapRes = await fetch(`${STAGING}/idx/v1/map?site=demo-ccor&north=27&south=26&east=-81&west=-82`, {
        headers: { "X-SNEAK-Session": sessionToken }
    });
    const mapData = await mapRes.json();
    console.log(`  GET /idx/v1/map: HTTP ${mapRes.status} | Markers: ${mapData.count}`);

    // 7. Verify Unauthorized Parent Behavior (Rejection & No Iframe)
    console.log("\n[7] Testing Unauthorized Domain Rejection (https://unauthorized-broker.com)...");
    const unauthBoot = await fetch(`${STAGING}/idx/v1/bootstrap?site=demo-ccor`, {
        headers: { Origin: "https://unauthorized-broker.com" }
    });
    console.log(`  Unauthorized Bootstrap: HTTP ${unauthBoot.status}`);
    const unauthData = await unauthBoot.json();
    console.log(`  Error: ${unauthData.error} - ${unauthData.message}`);
    if (unauthBoot.status !== 403) {
        throw new Error("Expected 403 on unauthorized domain");
    }
    console.log("  PASS: Unauthorized origin was blocked with HTTP 403.");

    console.log("\n====================================================");
    console.log("EMBED LIFECYCLE & SECURITY PROOF COMPLETE: SUCCESS!");
    console.log("====================================================");
}

testEmbedFlow().catch(err => {
    console.error("Embed proof failure:", err);
    process.exit(1);
});
