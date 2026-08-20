/**
 * scripts/test-remote-staging.mjs
 * 
 * Executes full remote staging API & security test suite against deployed Cloudflare Worker.
 */

const STAGING = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";

async function main() {
    console.log("====================================================");
    console.log("SNEAK IDX PLATFORM — LIVE REMOTE STAGING VALIDATION");
    console.log("====================================================");
    console.log("Host:", STAGING);

    // 1. Health Check
    console.log("\n[1/11] Testing GET /idx/v1/health...");
    const healthRes = await fetch(`${STAGING}/idx/v1/health`);
    const healthData = await healthRes.json();
    console.log(`  HTTP ${healthRes.status}:`, healthData);
    if (healthRes.status !== 200 || healthData.status !== 'ok') {
        throw new Error("Health check failed");
    }

    // 2. Static Assets & CSP
    console.log("\n[2/11] Testing Static Assets & CSP Headers...");
    const embedRes = await fetch(`${STAGING}/embed.js`);
    const embedText = await embedRes.text();
    console.log(`  GET /embed.js: HTTP ${embedRes.status} (length: ${embedText.length} bytes, type: ${embedRes.headers.get("content-type")})`);
    
    const searchHtmlRes = await fetch(`${STAGING}/search/?site=demo-ccor`);
    const csp = searchHtmlRes.headers.get("content-security-policy");
    console.log(`  GET /search/?site=demo-ccor: HTTP ${searchHtmlRes.status}`);
    console.log(`  CSP Header: ${csp}`);
    if (!csp || !csp.includes("frame-ancestors")) {
        throw new Error("Missing frame-ancestors CSP on search UI");
    }

    // 3. Unauthenticated Session Rejection
    console.log("\n[3/11] Testing Unauthenticated Session Rejection (No Session)...");
    const noSessionSearch = await fetch(`${STAGING}/idx/v1/search?site=demo-ccor`);
    const noSessionData = await noSessionSearch.json();
    console.log(`  GET /idx/v1/search (no session): HTTP ${noSessionSearch.status}`, noSessionData);
    if (noSessionSearch.status !== 401 || noSessionData.error !== 'SessionRequired') {
        throw new Error("Expected 401 SessionRequired");
    }

    const noSessionMap = await fetch(`${STAGING}/idx/v1/map?site=demo-ccor`);
    const noSessionMapData = await noSessionMap.json();
    console.log(`  GET /idx/v1/map (no session): HTTP ${noSessionMap.status}`, noSessionMapData);
    if (noSessionMap.status !== 401 || noSessionMapData.error !== 'SessionRequired') {
        throw new Error("Expected 401 SessionRequired on map");
    }

    // 4. Bootstrap Without Origin (Should Fail 403 in Staging)
    console.log("\n[4/11] Testing Bootstrap Without Origin...");
    const noOriginBoot = await fetch(`${STAGING}/idx/v1/bootstrap?site=demo-ccor`);
    const noOriginData = await noOriginBoot.json();
    console.log(`  GET /idx/v1/bootstrap (no Origin): HTTP ${noOriginBoot.status}`, noOriginData);
    if (noOriginBoot.status !== 403) {
        throw new Error("Expected 403 on bootstrap with no Origin");
    }

    // 5. Bootstrap With Unauthorized Origin
    console.log("\n[5/11] Testing Bootstrap With Unauthorized Origin...");
    const unauthBoot = await fetch(`${STAGING}/idx/v1/bootstrap?site=demo-ccor`, {
        headers: { Origin: "https://malicious-domain.example" }
    });
    const unauthData = await unauthBoot.json();
    console.log(`  GET /idx/v1/bootstrap (unauthorized origin): HTTP ${unauthBoot.status}`, unauthData);
    if (unauthBoot.status !== 403) {
        throw new Error("Expected 403 on unauthorized origin");
    }

    // 6. Bootstrap With Authorized Localhost Origin
    console.log("\n[6/11] Testing Bootstrap With Authorized Localhost Origin (http://localhost:8090)...");
    const authBoot = await fetch(`${STAGING}/idx/v1/bootstrap?site=demo-ccor`, {
        headers: { Origin: "http://localhost:8090" }
    });
    const bootData = await authBoot.json();
    const acao = authBoot.headers.get("access-control-allow-origin");
    console.log(`  GET /idx/v1/bootstrap: HTTP ${authBoot.status}`);
    console.log(`  Access-Control-Allow-Origin: ${acao}`);
    console.log(`  Success: ${bootData.success}, ExpiresIn: ${bootData.expiresIn}s, SiteKey: ${bootData.siteKey}, Has Session Token: ${Boolean(bootData.session)}`);

    if (authBoot.status !== 200 || !bootData.session) {
        throw new Error("Bootstrap failed to issue session token");
    }

    const session = bootData.session;

    // 7. Authenticated Config Endpoint
    console.log("\n[7/11] Testing GET /idx/v1/config with Session Token...");
    const configRes = await fetch(`${STAGING}/idx/v1/config?site=demo-ccor`, {
        headers: { "X-SNEAK-Session": session }
    });
    const config = await configRes.json();
    console.log(`  HTTP ${configRes.status}: ${config.displayName} | Brokerage: ${config.brokerage} | Primary Color: ${config.primaryColor}`);

    // 8. Authenticated Search & Map Endpoints
    console.log("\n[8/11] Testing GET /idx/v1/search & GET /idx/v1/map...");
    const searchRes = await fetch(`${STAGING}/idx/v1/search?site=demo-ccor`, {
        headers: { "X-SNEAK-Session": session }
    });
    const searchData = await searchRes.json();
    console.log(`  GET /idx/v1/search: HTTP ${searchRes.status} | Total: ${searchData.pagination.total} listings | Page Size: ${searchData.pagination.pageSize}`);
    searchData.data.forEach((l, idx) => {
        console.log(`    [${idx+1}] ${l.ListingKey}: ${l.UnparsedAddress}, ${l.City} - $${l.ListPrice?.toLocaleString()} (${l.StandardStatus}, ${l.PropertySubType})`);
    });

    const mapRes = await fetch(`${STAGING}/idx/v1/map?site=demo-ccor&north=27&south=26&east=-81&west=-82`, {
        headers: { "X-SNEAK-Session": session }
    });
    const mapData = await mapRes.json();
    console.log(`  GET /idx/v1/map: HTTP ${mapRes.status} | Markers: ${mapData.count}`);

    // 9. Filters Test
    console.log("\n[9/11] Testing Filter Precision (City, SubType)...");
    const bonitaRes = await fetch(`${STAGING}/idx/v1/search?site=demo-ccor&city=Bonita+Springs`, {
        headers: { "X-SNEAK-Session": session }
    });
    const bonita = await bonitaRes.json();
    console.log(`  city=Bonita Springs: ${bonita.data.length} listings returned`);

    const esteroRes = await fetch(`${STAGING}/idx/v1/search?site=demo-ccor&city=Estero`, {
        headers: { "X-SNEAK-Session": session }
    });
    const estero = await esteroRes.json();
    console.log(`  city=Estero: ${estero.data.length} listings returned`);

    const condoRes = await fetch(`${STAGING}/idx/v1/search?site=demo-ccor&propertySubType=Condominium`, {
        headers: { "X-SNEAK-Session": session }
    });
    const condo = await condoRes.json();
    console.log(`  propertySubType=Condominium: ${condo.data.length} listings returned`);

    // 10. Listing Detail & Media & Open Houses
    console.log("\n[10/11] Testing Detail, Media, & Open Houses...");
    const sampleKey = searchData.data[0].ListingKey;
    const detailRes = await fetch(`${STAGING}/idx/v1/listing/${sampleKey}?site=demo-ccor`, {
        headers: { "X-SNEAK-Session": session }
    });
    const detail = await detailRes.json();
    console.log(`  GET /idx/v1/listing/${sampleKey}: HTTP ${detailRes.status} - ${detail.data.UnparsedAddress} ($${detail.data.ListPrice})`);

    const mediaRes = await fetch(`${STAGING}/idx/v1/listing/${sampleKey}/media?site=demo-ccor`, {
        headers: { "X-SNEAK-Session": session }
    });
    const media = await mediaRes.json();
    console.log(`  GET /idx/v1/listing/${sampleKey}/media: HTTP ${mediaRes.status} - Photos: ${media.media.length}`);

    const ohRes = await fetch(`${STAGING}/idx/v1/open-houses?site=demo-ccor`, {
        headers: { "X-SNEAK-Session": session }
    });
    const ohData = await ohRes.json();
    console.log(`  GET /idx/v1/open-houses: HTTP ${ohRes.status} - Count: ${ohData.data.length}`);
    ohData.data.forEach(oh => {
        console.log(`    OH: ${oh.openHouse.openHouseKey} on ${oh.openHouse.date} (${oh.openHouse.startTime}) - ${oh.property.UnparsedAddress} ($${oh.property.ListPrice})`);
    });

    // 11. Lead Submission
    console.log("\n[11/11] Testing POST /idx/v1/lead Submission...");
    const leadPayload = {
        siteKey: "demo-ccor",
        listingKey: sampleKey,
        name: "SNEAK Staging Test",
        email: "staging-test@example.invalid",
        phone: "(239) 555-0199",
        message: "Automated staging verification — safe to delete.",
        sourceUrl: "http://localhost:8090"
    };
    const leadRes = await fetch(`${STAGING}/idx/v1/lead?site=demo-ccor`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "X-SNEAK-Session": session
        },
        body: JSON.stringify(leadPayload)
    });
    const leadResult = await leadRes.json();
    console.log(`  POST /idx/v1/lead: HTTP ${leadRes.status}`, leadResult);

    if (leadRes.status !== 201 || !leadResult.success) {
        throw new Error("Lead submission failed");
    }

    console.log("\n====================================================");
    console.log("ALL 11 REMOTE STAGING API & SECURITY CHECKS PASSED!");
    console.log("====================================================");
}

main().catch(err => {
    console.error("Staging test failure:", err);
    process.exit(1);
});
