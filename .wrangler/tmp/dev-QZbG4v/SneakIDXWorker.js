var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// .wrangler/tmp/bundle-i4L0Nr/checked-fetch.js
var urls = /* @__PURE__ */ new Set();
function checkURL(request, init) {
  const url = request instanceof URL ? request : new URL(
    (typeof request === "string" ? new Request(request, init) : request).url
  );
  if (url.port && url.port !== "443" && url.protocol === "https:") {
    if (!urls.has(url.toString())) {
      urls.add(url.toString());
      console.warn(
        `WARNING: known issue with \`fetch()\` requests to custom HTTPS ports in published Workers:
 - ${url.toString()} - the custom port will be ignored when the Worker is published using the \`wrangler deploy\` command.
`
      );
    }
  }
}
__name(checkURL, "checkURL");
globalThis.fetch = new Proxy(globalThis.fetch, {
  apply(target, thisArg, argArray) {
    const [request, init] = argArray;
    checkURL(request, init);
    return Reflect.apply(target, thisArg, argArray);
  }
});

// SneakIDXWorker.js
var SneakIDXWorker_default = {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const origin = req.headers.get("Origin") || "";
    const referer = req.headers.get("Referer") || "";
    if (req.method === "OPTIONS") {
      return handleCorsPreflight(req, env);
    }
    if (url.pathname === "/idx/v1/health") {
      return jsonResponse({
        status: "ok",
        service: "sneak-idx-worker",
        version: "1.0.0",
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      }, 200, "*");
    }
    if (!url.pathname.startsWith("/idx/v1/")) {
      return jsonResponse({ error: "Not Found", message: "SNEAK IDX API endpoints are under /idx/v1/" }, 404, "*");
    }
    const siteKey = url.searchParams.get("site") || (req.method === "POST" ? await peekSiteKeyFromPost(req.clone()) : null);
    if (!siteKey) {
      return jsonResponse({
        error: "MissingSiteKey",
        message: "A valid SNEAK site key (?site=...) is required."
      }, 400, "*");
    }
    const authResult = await resolveAndAuthorizeSite(siteKey, origin, referer, env);
    if (!authResult.authorized) {
      return jsonResponse({
        error: authResult.error || "Unauthorized",
        message: authResult.message || "Access denied for this site key or origin."
      }, authResult.status || 403, "*");
    }
    const { site, branding, allowedOrigin } = authResult;
    try {
      if (url.pathname === "/idx/v1/config" && req.method === "GET") {
        return await handleGetConfig(site, branding, env, allowedOrigin);
      }
      if (url.pathname === "/idx/v1/search" && req.method === "GET") {
        return await handleSearch(url, site, env, ctx, allowedOrigin);
      }
      const mediaMatch = url.pathname.match(/^\/idx\/v1\/listing\/([^/]+)\/media$/);
      if (mediaMatch && req.method === "GET") {
        const listingKey = decodeURIComponent(mediaMatch[1]);
        return await handleListingMedia(listingKey, req, env, ctx, allowedOrigin);
      }
      const listingMatch = url.pathname.match(/^\/idx\/v1\/listing\/([^/]+)$/);
      if (listingMatch && req.method === "GET") {
        const listingKey = decodeURIComponent(listingMatch[1]);
        return await handleListingDetail(listingKey, site, req, env, ctx, allowedOrigin);
      }
      const agentMatch = url.pathname.match(/^\/idx\/v1\/agent\/([^/]+)\/listings$/);
      if (agentMatch && req.method === "GET") {
        const agentMlsId = decodeURIComponent(agentMatch[1]);
        return await handleAgentListings(agentMlsId, url, site, env, allowedOrigin);
      }
      if (url.pathname === "/idx/v1/open-houses" && req.method === "GET") {
        return await handleOpenHouses(url, site, env, allowedOrigin);
      }
      if (url.pathname === "/idx/v1/lead" && req.method === "POST") {
        return await handleLeadSubmission(req, site, env, ctx, allowedOrigin);
      }
      return jsonResponse({ error: "NotFound", message: "Route not found" }, 404, allowedOrigin);
    } catch (err) {
      console.error("Unhandled SNEAK Worker Error:", err);
      return jsonResponse({
        error: "InternalServerError",
        message: "An error occurred while processing your request."
      }, 500, allowedOrigin);
    }
  },
  /**
   * Scheduled cron handler for generalized SNEAK Open House synchronization
   * Synchronizes today - 1 day through today + 30 days without modifying legacy tables
   */
  async scheduled(event, env, ctx) {
    console.log("Starting SNEAK Generalized Open House Sync...");
    if (!env.BRIDGE_TOKEN || !env.DB) {
      console.warn("BRIDGE_TOKEN or DB missing, skipping scheduled sync.");
      return;
    }
    await syncSneakOpenHouses(env);
  }
};
async function resolveAndAuthorizeSite(siteKey, origin, referer, env) {
  if (!env.DB) {
    return { authorized: false, error: "DatabaseError", message: "Database binding unavailable.", status: 500 };
  }
  const query = `
        SELECT 
            s.id AS site_id, s.account_id, s.site_key, s.site_name, s.status AS site_status,
            s.scope_type, s.scope_value,
            a.account_name, a.status AS account_status, a.plan, a.agent_mls_id AS default_agent_mls_id, a.office_mls_id AS default_office_mls_id,
            b.display_name, b.brokerage, b.logo_url, b.agent_photo_url, b.primary_color, b.secondary_color,
            b.phone, b.email, b.website_url, b.config_json AS branding_config
        FROM sneak_sites s
        JOIN sneak_accounts a ON s.account_id = a.id
        LEFT JOIN sneak_branding b ON s.id = b.site_id
        WHERE s.site_key = ?
    `;
  const siteRecord = await env.DB.prepare(query).bind(siteKey).first();
  if (!siteRecord) {
    return { authorized: false, error: "SiteNotFound", message: "Site key does not exist.", status: 404 };
  }
  if (siteRecord.site_status !== "active" || siteRecord.account_status !== "active") {
    return { authorized: false, error: "SiteInactive", message: "This SNEAK site is currently inactive or suspended.", status: 403 };
  }
  const domainsResult = await env.DB.prepare(
    "SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active'"
  ).bind(siteRecord.site_id).all();
  const allowedDomains = (domainsResult.results || []).map((r) => r.domain.toLowerCase().trim());
  let requestHost = "";
  let effectiveOrigin = origin;
  if (origin) {
    try {
      requestHost = new URL(origin).hostname.toLowerCase();
    } catch {
    }
  } else if (referer) {
    try {
      const refUrl = new URL(referer);
      requestHost = refUrl.hostname.toLowerCase();
      if (!effectiveOrigin) effectiveOrigin = refUrl.origin;
    } catch {
    }
  }
  const isDevHost = requestHost === "localhost" || requestHost === "127.0.0.1" || requestHost === "::1" || requestHost.endsWith(".local") || requestHost.endsWith(".internal");
  let isDomainAuthorized = false;
  if (!requestHost) {
    isDomainAuthorized = true;
    effectiveOrigin = "*";
  } else if (isDevHost) {
    isDomainAuthorized = true;
  } else {
    isDomainAuthorized = allowedDomains.some((d) => {
      if (d === "*" || d === requestHost) return true;
      if (d.startsWith("*.")) {
        const rootDomain = d.slice(2);
        return requestHost === rootDomain || requestHost.endsWith("." + rootDomain);
      }
      return false;
    });
  }
  if (!isDomainAuthorized) {
    return {
      authorized: false,
      error: "DomainNotAuthorized",
      message: `Domain '${requestHost}' is not authorized to use this SNEAK site key.`,
      status: 403
    };
  }
  return {
    authorized: true,
    site: siteRecord,
    branding: siteRecord,
    allowedOrigin: effectiveOrigin || "*"
  };
}
__name(resolveAndAuthorizeSite, "resolveAndAuthorizeSite");
async function peekSiteKeyFromPost(req) {
  try {
    const body = await req.json();
    return body.siteKey || body.site || null;
  } catch {
    return null;
  }
}
__name(peekSiteKeyFromPost, "peekSiteKeyFromPost");
function handleCorsPreflight(req, env) {
  const origin = req.headers.get("Origin") || "*";
  const headers = new Headers();
  headers.set("Access-Control-Allow-Origin", origin);
  headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, X-Site-Key");
  headers.set("Access-Control-Max-Age", "86400");
  headers.set("Vary", "Origin");
  return new Response(null, { status: 204, headers });
}
__name(handleCorsPreflight, "handleCorsPreflight");
async function handleGetConfig(site, branding, env, allowedOrigin) {
  const widgetRows = await env.DB.prepare(
    "SELECT widget_type, enabled, config_json FROM sneak_widget_configs WHERE site_id = ?"
  ).bind(site.site_id).all();
  const widgets = {};
  (widgetRows.results || []).forEach((w) => {
    let parsed = {};
    try {
      parsed = JSON.parse(w.config_json);
    } catch {
    }
    widgets[w.widget_type] = {
      enabled: Boolean(w.enabled),
      config: parsed
    };
  });
  let customBranding = {};
  if (branding.branding_config) {
    try {
      customBranding = JSON.parse(branding.branding_config);
    } catch {
    }
  }
  const configPayload = {
    siteKey: site.site_key,
    siteName: site.site_name,
    displayName: branding.display_name || site.account_name,
    brokerage: branding.brokerage || "",
    logoUrl: branding.logo_url || "",
    agentPhotoUrl: branding.agent_photo_url || "",
    primaryColor: branding.primary_color || "#1a2a3a",
    secondaryColor: branding.secondary_color || "#2596be",
    phone: branding.phone || "",
    email: branding.email || "",
    websiteUrl: branding.website_url || "",
    scope: {
      type: site.scope_type || "market",
      value: site.scope_value || null
    },
    brandingConfig: customBranding,
    features: {
      search: true,
      map: true,
      savedListings: true,
      openHouses: true,
      leadCapture: true
    },
    widgets
  };
  return jsonResponse(configPayload, 200, allowedOrigin, "public, max-age=300, s-maxage=600");
}
__name(handleGetConfig, "handleGetConfig");
async function handleSearch(url, site, env, ctx, allowedOrigin) {
  const params = url.searchParams;
  const city = params.get("city");
  const minPrice = parseFloat(params.get("minPrice")) || null;
  const maxPrice = parseFloat(params.get("maxPrice")) || null;
  const beds = parseInt(params.get("beds"), 10) || null;
  const baths = parseInt(params.get("baths"), 10) || null;
  const propertyType = params.get("propertyType") || "sale";
  const propertySubType = params.get("propertySubType");
  const status = params.get("status") || "Active";
  const q = (params.get("q") || params.get("search") || "").trim();
  const sort = params.get("sort") || "dateDesc";
  const page = Math.max(1, parseInt(params.get("page"), 10) || 1);
  const limit = Math.min(100, Math.max(1, parseInt(params.get("limit") || params.get("pageSize"), 10) || 24));
  const offset = (page - 1) * limit;
  const whereClauses = [];
  const bindValues = [];
  if (site.scope_type === "agent" && site.scope_value) {
    whereClauses.push("ListAgentMlsId = ?");
    bindValues.push(site.scope_value);
  } else if (site.scope_type === "office" && site.scope_value) {
    whereClauses.push("(ListOfficeName = ? OR ListOfficePhone = ?)");
    bindValues.push(site.scope_value, site.scope_value);
  } else {
    const agentMlsId = params.get("agentMlsId");
    if (agentMlsId) {
      whereClauses.push("ListAgentMlsId = ?");
      bindValues.push(agentMlsId);
    }
  }
  if (status === "Pending") {
    whereClauses.push("(StandardStatus = 'Pending' OR StandardStatus = 'Active Under Contract')");
  } else if (status === "Active Under Contract") {
    whereClauses.push("StandardStatus = 'Active Under Contract'");
  } else if (status === "Closed") {
    whereClauses.push("StandardStatus = 'Closed'");
  } else if (status === "All") {
    whereClauses.push("(StandardStatus = 'Active' OR StandardStatus = 'Active Under Contract' OR StandardStatus = 'Pending')");
  } else {
    whereClauses.push("StandardStatus = 'Active'");
  }
  if (propertyType === "sale") {
    whereClauses.push("(PropertyType = 'Residential' OR PropertyType = 'Residential Income')");
  } else if (propertyType === "rental") {
    whereClauses.push("PropertyType = 'Residential Lease'");
  } else if (propertyType === "commercial") {
    whereClauses.push("(PropertyType = 'Commercial Sale' OR PropertyType = 'Commercial' OR PropertyType = 'Commercial Lease' OR PropertyType = 'Business Opportunity')");
  } else if (propertyType === "land") {
    whereClauses.push("PropertyType = 'Land'");
  } else if (propertyType && propertyType !== "all") {
    whereClauses.push("PropertyType = ?");
    bindValues.push(propertyType);
  }
  if (propertySubType) {
    const subTypes = propertySubType.split(",").map((s) => s.trim()).filter(Boolean);
    if (subTypes.length > 0) {
      const expanded = [];
      subTypes.forEach((st) => {
        if (st === "Single Family Residence") {
          expanded.push("Single Family Residence", "Manufactured Home");
        } else if (st === "Condominium") {
          expanded.push("Condominium", "High Rise (8+)", "Mid Rise (4-7)", "Low Rise (1-3)");
        } else if (st === "Townhouse") {
          expanded.push("Townhouse");
        } else if (st === "Multi Family") {
          expanded.push("Multi Family", "Duplex", "Triplex", "Quadruplex");
        } else if (st === "Villa") {
          expanded.push("Villa Attached", "Villa Detached");
        } else {
          expanded.push(st);
        }
      });
      const placeholders = expanded.map(() => "?").join(",");
      whereClauses.push(`PropertySubType IN (${placeholders})`);
      bindValues.push(...expanded);
    }
  }
  if (city) {
    const cities = city.split(",").map((c) => c.trim()).filter(Boolean);
    if (cities.length === 1) {
      whereClauses.push("LOWER(City) = LOWER(?)");
      bindValues.push(cities[0]);
    } else if (cities.length > 1) {
      const placeholders = cities.map(() => "LOWER(?)").join(",");
      whereClauses.push(`LOWER(City) IN (${placeholders})`);
      bindValues.push(...cities);
    }
  }
  if (minPrice !== null && minPrice > 0) {
    whereClauses.push("ListPrice >= ?");
    bindValues.push(minPrice);
  }
  if (maxPrice !== null && maxPrice > 0) {
    whereClauses.push("ListPrice <= ?");
    bindValues.push(maxPrice);
  }
  if (beds !== null && beds > 0) {
    whereClauses.push("BedroomsTotal >= ?");
    bindValues.push(beds);
  }
  if (baths !== null && baths > 0) {
    whereClauses.push("BathroomsTotalInteger >= ?");
    bindValues.push(baths);
  }
  if (q) {
    whereClauses.push("(ListingKey = ? OR ListingId = ? OR LOWER(UnparsedAddress) LIKE ? OR LOWER(City) LIKE ? OR LOWER(ListAgentFullName) LIKE ? OR LOWER(ListAgentMlsId) = ?)");
    const likeQ = `%${q.toLowerCase()}%`;
    bindValues.push(q, q, likeQ, likeQ, likeQ, q.toLowerCase());
  }
  const whereSQL = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
  let orderSQL = "ORDER BY ListingContractDate DESC, ModificationTimestamp DESC";
  if (sort === "priceDesc") {
    orderSQL = "ORDER BY ListPrice DESC, ListingContractDate DESC";
  } else if (sort === "priceAsc") {
    orderSQL = "ORDER BY ListPrice ASC, ListingContractDate DESC";
  } else if (sort === "dateAsc") {
    orderSQL = "ORDER BY ListingContractDate ASC";
  }
  const countSQL = `SELECT COUNT(*) AS total FROM listings ${whereSQL}`;
  const countStmt = env.DB.prepare(countSQL).bind(...bindValues);
  const countRes = await countStmt.first();
  const total = countRes ? countRes.total : 0;
  const selectCols = `
        ListingKey, ListingId, ListPrice, UnparsedAddress, City, CountyOrParish,
        BedroomsTotal, BathroomsTotalInteger, LivingArea, StandardStatus,
        PropertyType, PropertySubType, PrimaryPhoto, ListingContractDate,
        Latitude, Longitude, ModificationTimestamp, YearBuilt, LotSizeAcres,
        ListAgentFullName, ListOfficeName, ListOfficePhone, ListAgentMlsId
    `;
  const searchSQL = `SELECT ${selectCols} FROM listings ${whereSQL} ${orderSQL} LIMIT ? OFFSET ?`;
  const searchStmt = env.DB.prepare(searchSQL).bind(...bindValues, limit, offset);
  const results = await searchStmt.all();
  const formattedListings = (results.results || []).map((row) => ({
    ...row,
    Coordinates: row.Longitude && row.Latitude ? [row.Longitude, row.Latitude] : null,
    Media: row.PrimaryPhoto ? [{ MediaURL: row.PrimaryPhoto, Order: 0 }] : []
  }));
  if (ctx && ctx.waitUntil) {
    ctx.waitUntil(recordUsage(site.site_id, "searches", env));
  }
  const totalPages = Math.ceil(total / limit);
  return jsonResponse({
    data: formattedListings,
    pagination: {
      total,
      page,
      pageSize: limit,
      totalPages,
      hasMore: page < totalPages
    }
  }, 200, allowedOrigin, "public, max-age=60, s-maxage=120");
}
__name(handleSearch, "handleSearch");
async function handleListingDetail(listingKey, site, req, env, ctx, allowedOrigin) {
  const d1Listing = await env.DB.prepare(
    "SELECT * FROM listings WHERE ListingKey = ? OR ListingId = ?"
  ).bind(listingKey, listingKey).first();
  if (!d1Listing && !env.BRIDGE_TOKEN) {
    return jsonResponse({ error: "ListingNotFound", message: "Property not found." }, 404, allowedOrigin);
  }
  let fullDetails = { ...d1Listing };
  if (env.BRIDGE_TOKEN) {
    const cache = caches.default;
    const cacheUrl = new URL(req.url);
    cacheUrl.searchParams.delete("site");
    const cacheKey = new Request(cacheUrl.toString(), { method: "GET" });
    let cached = await cache.match(cacheKey);
    if (cached) {
      try {
        const cachedData = await cached.json();
        fullDetails = { ...fullDetails, ...cachedData };
      } catch {
      }
    } else {
      try {
        const sel = "ListingKey,ListingId,UnparsedAddress,City,PostalCode,CountyOrParish,ListPrice,PropertyType,PropertySubType,BedroomsTotal,BathroomsTotalInteger,LivingArea,LotSizeAcres,YearBuilt,StandardStatus,SubdivisionName,ListAgentFullName,ListAgentEmail,ListAgentDirectPhone,ListAgentKey,ListOfficeName,ListOfficePhone,PublicRemarks,Coordinates,Media,ModificationTimestamp";
        const bridgeUrl = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$filter=ListingKey eq '${encodeURIComponent(listingKey)}'&$select=${sel}&access_token=${env.BRIDGE_TOKEN}`;
        const bridgeRes = await fetch(bridgeUrl, { headers: { Accept: "application/json" } });
        if (bridgeRes.ok) {
          const bridgeJson = await bridgeRes.json();
          const p = bridgeJson.value && bridgeJson.value[0];
          if (p) {
            fullDetails = { ...fullDetails, ...p };
            const cacheHeaders = new Headers({
              "Content-Type": "application/json",
              "Cache-Control": "public, s-maxage=600"
            });
            const responseToCache = new Response(JSON.stringify(p), { headers: cacheHeaders });
            if (ctx && ctx.waitUntil) {
              ctx.waitUntil(cache.put(cacheKey, responseToCache));
            }
          }
        }
      } catch (err) {
        console.warn("Bridge detail fetch fallback failed:", err);
      }
    }
  }
  if (fullDetails.Longitude && fullDetails.Latitude && !fullDetails.Coordinates) {
    fullDetails.Coordinates = [fullDetails.Longitude, fullDetails.Latitude];
  }
  if (fullDetails.Media && Array.isArray(fullDetails.Media)) {
    fullDetails.Media = fullDetails.Media.sort((a, b) => (a.Order || 0) - (b.Order || 0));
  } else if (fullDetails.PrimaryPhoto) {
    fullDetails.Media = [{ MediaURL: fullDetails.PrimaryPhoto, Order: 0 }];
  }
  if (ctx && ctx.waitUntil) {
    ctx.waitUntil(recordUsage(site.site_id, "listing_views", env));
  }
  return jsonResponse({ data: fullDetails }, 200, allowedOrigin, "public, max-age=120, s-maxage=600");
}
__name(handleListingDetail, "handleListingDetail");
async function handleListingMedia(listingKey, req, env, ctx, allowedOrigin) {
  const cache = caches.default;
  const cacheUrl = new URL(req.url);
  cacheUrl.searchParams.delete("site");
  const cacheKey = new Request(cacheUrl.toString(), { method: "GET" });
  const cached = await cache.match(cacheKey);
  if (cached) {
    const h = new Headers(cached.headers);
    h.set("Access-Control-Allow-Origin", allowedOrigin);
    h.set("Vary", "Origin");
    return new Response(await cached.text(), { status: cached.status, headers: h });
  }
  let mediaUrls = [];
  if (env.BRIDGE_TOKEN) {
    try {
      const bridgeUrl = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$filter=ListingKey eq '${encodeURIComponent(listingKey)}'&$select=ListingKey,Media&access_token=${env.BRIDGE_TOKEN}`;
      const res = await fetch(bridgeUrl, { headers: { Accept: "application/json" } });
      if (res.ok) {
        const data = await res.json();
        const p = data.value && data.value[0];
        if (p && p.Media && Array.isArray(p.Media)) {
          mediaUrls = p.Media.sort((a, b) => (a.Order || 0) - (b.Order || 0)).map((m) => m.MediaURL || m.MediaUrl || m.MediaURLLarge).filter(Boolean);
        }
      }
    } catch (err) {
      console.warn("Error fetching media upstream:", err);
    }
  }
  if (!mediaUrls.length) {
    const row = await env.DB.prepare("SELECT PrimaryPhoto FROM listings WHERE ListingKey = ?").bind(listingKey).first();
    if (row && row.PrimaryPhoto) {
      mediaUrls = [row.PrimaryPhoto];
    }
  }
  const payload = {
    listingKey,
    media: mediaUrls
  };
  const outResponse = jsonResponse(payload, 200, allowedOrigin, "public, max-age=600, s-maxage=3600");
  if (ctx && ctx.waitUntil && mediaUrls.length > 0) {
    ctx.waitUntil(cache.put(cacheKey, outResponse.clone()));
  }
  return outResponse;
}
__name(handleListingMedia, "handleListingMedia");
async function handleAgentListings(agentMlsId, url, site, env, allowedOrigin) {
  const limit = Math.min(50, parseInt(url.searchParams.get("limit"), 10) || 20);
  const results = await env.DB.prepare(`
        SELECT ListingKey, ListingId, ListPrice, UnparsedAddress, City, BedroomsTotal, BathroomsTotalInteger, LivingArea, PrimaryPhoto, StandardStatus, PropertyType, PropertySubType
        FROM listings
        WHERE ListAgentMlsId = ? AND StandardStatus = 'Active'
        ORDER BY ListingContractDate DESC LIMIT ?
    `).bind(agentMlsId, limit).all();
  return jsonResponse({ data: results.results || [] }, 200, allowedOrigin, "public, max-age=300, s-maxage=600");
}
__name(handleAgentListings, "handleAgentListings");
async function handleOpenHouses(url, site, env, allowedOrigin) {
  const todayStr = (/* @__PURE__ */ new Date()).toISOString().split("T")[0];
  const city = url.searchParams.get("city");
  let query = `
        SELECT OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, PropertyData
        FROM sneak_open_houses
        WHERE OpenHouseDate >= ?
        ORDER BY OpenHouseDate ASC, OpenHouseStartTime ASC
        LIMIT 100
    `;
  const results = await env.DB.prepare(query).bind(todayStr).all();
  const data = (results.results || []).map((row) => {
    let property = null;
    try {
      property = JSON.parse(row.PropertyData);
    } catch {
    }
    return {
      openHouse: {
        openHouseKey: row.OpenHouseKey,
        listingKey: row.ListingKey,
        startTime: row.OpenHouseStartTime,
        endTime: row.OpenHouseEndTime,
        date: row.OpenHouseDate,
        remarks: row.OpenHouseRemarks
      },
      property
    };
  }).filter((x) => x.property);
  return jsonResponse({ data }, 200, allowedOrigin, "public, max-age=300, s-maxage=300");
}
__name(handleOpenHouses, "handleOpenHouses");
async function handleLeadSubmission(req, site, env, ctx, allowedOrigin) {
  let body;
  try {
    body = await req.json();
  } catch {
    return jsonResponse({ error: "InvalidJSON", message: "Malformed JSON payload." }, 400, allowedOrigin);
  }
  const name = (body.name || "").trim();
  const email = (body.email || "").trim();
  const phone = (body.phone || "").trim();
  const message = (body.message || "").trim();
  const listingKey = body.listingKey || null;
  const leadType = body.leadType || "property_inquiry";
  const sourceUrl = body.sourceUrl || req.headers.get("Referer") || "";
  if (!name || !email) {
    return jsonResponse({ error: "ValidationError", message: "Name and Email are required fields." }, 400, allowedOrigin);
  }
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    return jsonResponse({ error: "ValidationError", message: "Please provide a valid email address." }, 400, allowedOrigin);
  }
  const leadId = "lead_" + Date.now().toString(36) + "_" + Math.random().toString(36).substring(2, 7);
  await env.DB.prepare(`
        INSERT INTO sneak_leads (
            id, site_id, listing_key, lead_type, name, email, phone, message, source_url, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    `).bind(
    leadId,
    site.site_id,
    listingKey,
    leadType,
    name,
    email,
    phone,
    message,
    sourceUrl
  ).run();
  if (ctx && ctx.waitUntil) {
    ctx.waitUntil(recordUsage(site.site_id, "leads", env));
  }
  return jsonResponse({
    success: true,
    leadId,
    message: "Inquiry received successfully. Our team will contact you soon."
  }, 201, allowedOrigin);
}
__name(handleLeadSubmission, "handleLeadSubmission");
async function recordUsage(siteId, column, env) {
  try {
    const today = (/* @__PURE__ */ new Date()).toISOString().split("T")[0];
    const id = `${siteId}_${today}`;
    await env.DB.prepare(`
            INSERT INTO sneak_usage (id, site_id, usage_date, ${column})
            VALUES (?, ?, ?, 1)
            ON CONFLICT(site_id, usage_date) DO UPDATE SET
                ${column} = ${column} + 1
        `).bind(id, siteId, today).run();
  } catch (err) {
    console.warn("Failed to record usage metric:", err);
  }
}
__name(recordUsage, "recordUsage");
async function syncSneakOpenHouses(env) {
  const today = /* @__PURE__ */ new Date();
  const startDate = new Date(today);
  startDate.setDate(today.getDate() - 1);
  const endDate = new Date(today);
  endDate.setDate(today.getDate() + 30);
  const startStr = startDate.toISOString().split("T")[0];
  const endStr = endDate.toISOString().split("T")[0];
  const ohFilter = `OpenHouseStatus eq 'Active' and OpenHouseDate ge ${startStr} and OpenHouseDate le ${endStr}`;
  const ohURL = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(ohFilter)}&$top=200&$orderby=OpenHouseStartTime asc&access_token=${env.BRIDGE_TOKEN}`;
  let ohRec = [];
  let next = ohURL;
  while (next) {
    const res = await fetch(next);
    if (!res.ok) break;
    const d = await res.json();
    ohRec.push(...d.value || []);
    next = d["@odata.nextLink"] || null;
    if (next && !next.includes("access_token")) {
      next += (next.includes("?") ? "&" : "?") + "access_token=" + env.BRIDGE_TOKEN;
    }
  }
  if (!ohRec.length) {
    console.log("No upcoming open houses found.");
    return;
  }
  const listingKeys = [...new Set(ohRec.map((r) => r.ListingKey))];
  let properties = [];
  const PROP_SEL = "ListingKey,ListingId,UnparsedAddress,City,PostalCode,ListPrice,PropertyType,PropertySubType,BedroomsTotal,BathroomsTotalInteger,LivingArea,LotSizeAcres,YearBuilt,StandardStatus,SubdivisionName,ListAgentFullName,ListAgentEmail,ListAgentDirectPhone,ListAgentKey,ListOfficeName,ListOfficePhone,PublicRemarks,Coordinates,Media";
  for (let i = 0; i < listingKeys.length; i += 25) {
    const chunk = listingKeys.slice(i, i + 25);
    const batchFilter = chunk.map((k) => `ListingKey eq '${k}'`).join(" or ");
    const pURL = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$filter=${encodeURIComponent(`(${batchFilter})`)}&$top=100&$select=${PROP_SEL}&access_token=${env.BRIDGE_TOKEN}`;
    const pres = await fetch(pURL);
    if (pres.ok) {
      const pd = await pres.json();
      properties.push(...pd.value || []);
    }
  }
  const propMap = new Map(properties.map((p) => [p.ListingKey, p]));
  const statements = ohRec.map((oh) => {
    const p = propMap.get(oh.ListingKey) || null;
    const id = "oh_" + (oh.OpenHouseKey || oh.ListingKey);
    return env.DB.prepare(`
            INSERT OR REPLACE INTO sneak_open_houses (
                id, OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, PropertyData, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        `).bind(
      id,
      oh.OpenHouseKey || oh.ListingKey,
      oh.ListingKey,
      oh.OpenHouseStartTime,
      oh.OpenHouseEndTime,
      oh.OpenHouseDate,
      oh.OpenHouseRemarks || "",
      JSON.stringify(p)
    );
  });
  for (let i = 0; i < statements.length; i += 50) {
    await env.DB.batch(statements.slice(i, i + 50));
  }
  console.log(`Successfully synced ${ohRec.length} SNEAK open houses.`);
}
__name(syncSneakOpenHouses, "syncSneakOpenHouses");
function jsonResponse(data, status = 200, allowedOrigin = "*", cacheControl = null) {
  const headers = new Headers();
  headers.set("Content-Type", "application/json");
  headers.set("Access-Control-Allow-Origin", allowedOrigin);
  headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, X-Site-Key");
  headers.set("Vary", "Origin");
  if (cacheControl) {
    headers.set("Cache-Control", cacheControl);
  } else {
    headers.set("Cache-Control", "no-store");
  }
  return new Response(JSON.stringify(data), { status, headers });
}
__name(jsonResponse, "jsonResponse");

// ../.nvm/versions/node/v20.19.4/lib/node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// ../.nvm/versions/node/v20.19.4/lib/node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    return Response.json(error, {
      status: 500,
      headers: { "MF-Experimental-Error-Stack": "true" }
    });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-i4L0Nr/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = SneakIDXWorker_default;

// ../.nvm/versions/node/v20.19.4/lib/node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-i4L0Nr/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=SneakIDXWorker.js.map
