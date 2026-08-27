/**
 * SNEAK IDX Worker (sneak-idx-worker)
 * Multi-tenant IDX API & Service — Phase 2.1 Production Hardened Foundation
 * 
 * Provides versioned /idx/v1/... endpoints for multi-tenant real estate search,
 * lightweight map viewport queries, tenant configuration, dynamic branding,
 * parameterized D1 SQL search, listing detail & media retrieval, lead capture,
 * embed session bootstrap, and event-routed cron synchronization.
 * 
 * Isolated & Production-Safe: Connects to sneak_listings and sneak_* tables in
 * community-idx D1 without modifying legacy ListingsWorker or existing production routes.
 */

export default {
    async fetch(req, env, ctx) {
        const url = new URL(req.url);
        const origin = req.headers.get('Origin') || '';
        const referer = req.headers.get('Referer') || '';

        // 1. Handle CORS Preflight Requests
        if (req.method === 'OPTIONS') {
            return await handleCorsPreflight(req, url, env);
        }

        // 2. Public Health Check (Does not require tenant auth or session)
        if (url.pathname === '/idx/v1/health') {
            return jsonResponse({
                status: 'ok',
                service: 'sneak-idx-worker',
                version: '2.1.0',
                dataset: 'sneak_listings',
                environment: env.SNEAK_ENV || 'staging',
                timestamp: new Date().toISOString()
            }, 200, '*');
        }

        // 3. Static Assets & Dynamic CSP Worker-First Handling
        if (!url.pathname.startsWith('/idx/v1/')) {
            if (env.ASSETS) {
                // Intercept search UI HTML to dynamically attach strict frame-ancestors CSP
                const isSearchHtml = url.pathname === '/' || url.pathname === '/search' || url.pathname === '/search/' || url.pathname.endsWith('/search/index.html') || url.pathname.endsWith('.html');
                const siteKey = url.searchParams.get('site');

                if (isSearchHtml && siteKey) {
                    return await handleStaticWithCSP(req, siteKey, env);
                }
                return await env.ASSETS.fetch(req);
            }
            return jsonResponse({ error: 'NotFound', message: 'SNEAK IDX API endpoints are under /idx/v1/' }, 404);
        }

        // 4. Route: GET /idx/v1/bootstrap?site=SITE_KEY
        // Special endpoint called directly from member host via embed.js to authenticate domain and issue session
        if (url.pathname === '/idx/v1/bootstrap' && req.method === 'GET') {
            return await handleBootstrap(req, url, env);
        }

        // 5. Extract Site Key
        let siteKey = url.searchParams.get('site');
        if (!siteKey && req.method === 'POST') {
            siteKey = await peekSiteKeyFromPost(req.clone());
        }

        if (!siteKey) {
            return jsonResponse({
                error: 'MissingSiteKey',
                message: 'A valid SNEAK site key (?site=...) is required.'
            }, 400);
        }

        // 6. Tenant Resolution & Session Authorization
        const authResult = await resolveAndAuthorizeRequest(req, siteKey, origin, referer, env);
        if (!authResult.authorized) {
            return jsonResponse({
                error: authResult.error || 'Unauthorized',
                message: authResult.message || 'Access denied for this site key or session.'
            }, authResult.status || 403);
        }

        const { site, branding, allowedOrigin } = authResult;

        try {
            // --- ROUTE: GET /idx/v1/config ---
            if (url.pathname === '/idx/v1/config' && req.method === 'GET') {
                return await handleGetConfig(site, branding, env, allowedOrigin);
            }

            // --- ROUTE: GET /idx/v1/search ---
            if (url.pathname === '/idx/v1/search' && req.method === 'GET') {
                return await handleSearch(url, site, env, ctx, allowedOrigin);
            }

            // --- ROUTE: GET /idx/v1/map ---
            if (url.pathname === '/idx/v1/map' && req.method === 'GET') {
                return await handleMap(url, site, env, ctx, allowedOrigin);
            }

            // --- ROUTE: GET /idx/v1/listing/:listingKey/media ---
            const mediaMatch = url.pathname.match(/^\/idx\/v1\/listing\/([^/]+)\/media$/);
            if (mediaMatch && req.method === 'GET') {
                const listingKey = decodeURIComponent(mediaMatch[1]);
                return await handleListingMedia(listingKey, site, req, env, ctx, allowedOrigin);
            }

            // --- ROUTE: GET /idx/v1/listing/:listingKey ---
            const listingMatch = url.pathname.match(/^\/idx\/v1\/listing\/([^/]+)$/);
            if (listingMatch && req.method === 'GET') {
                const listingKey = decodeURIComponent(listingMatch[1]);
                return await handleListingDetail(listingKey, site, req, env, ctx, allowedOrigin);
            }

            // --- ROUTE: GET /idx/v1/agent/:mlsId/listings ---
            const agentMatch = url.pathname.match(/^\/idx\/v1\/agent\/([^/]+)\/listings$/);
            if (agentMatch && req.method === 'GET') {
                const agentMlsId = decodeURIComponent(agentMatch[1]);
                return await handleAgentListings(agentMlsId, url, site, env, allowedOrigin);
            }

            // --- ROUTE: GET /idx/v1/open-houses ---
            if (url.pathname === '/idx/v1/open-houses' && req.method === 'GET') {
                return await handleOpenHouses(url, site, env, allowedOrigin);
            }

            // --- ROUTE: POST /idx/v1/lead ---
            if (url.pathname === '/idx/v1/lead' && req.method === 'POST') {
                return await handleLeadSubmission(req, site, env, ctx, allowedOrigin);
            }

            return jsonResponse({ error: 'NotFound', message: 'Endpoint not found.' }, 404, allowedOrigin);
        } catch (err) {
            console.error('Unhandled SNEAK Worker Error:', err);
            return jsonResponse({
                error: 'InternalServerError',
                message: 'An unexpected error occurred while processing your request.'
            }, 500, allowedOrigin);
        }
    },

    /**
     * Event-routed Scheduled Cron Handler
     * Dispatches tasks strictly according to event.cron trigger.
     */
    async scheduled(event, env, ctx) {
        console.log(`Starting SNEAK Scheduled Event for cron trigger: ${event.cron}`);
        if (!env.DB) {
            console.warn("DB binding missing, skipping scheduled sync.");
            return;
        }

        switch (event.cron) {
            case "0 */2 * * *":
                try {
                    await syncSneakListings(env);
                } catch (err) {
                    console.error("SNEAK Listing Sync Cron Error:", err);
                }
                break;

            case "*/15 * * * *":
                try {
                    await syncSneakOpenHouses(env);
                } catch (err) {
                    console.error("SNEAK Open House Sync Cron Error:", err);
                }
                break;

            default:
                console.warn(`Unrecognized cron schedule: ${event.cron}. No jobs executed.`);
        }
    }
};

/* ==========================================================================
   WEB CRYPTO HMAC-SHA256 SIGNING & SESSION TOKENS
   ========================================================================== */

function base64UrlEncode(str) {
    const base64 = btoa(str);
    return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function base64UrlDecode(str) {
    let base64 = str.replace(/-/g, '+').replace(/_/g, '/');
    while (base64.length % 4) base64 += '=';
    return atob(base64);
}

function arrayBufferToBase64Url(buffer) {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    for (let i = 0; i < bytes.byteLength; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return base64UrlEncode(binary);
}

async function getSigningKey(secret) {
    const enc = new TextEncoder();
    return await crypto.subtle.importKey(
        'raw',
        enc.encode(secret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign', 'verify']
    );
}

async function signSessionToken(payload, secret) {
    const header = { alg: 'HS256', typ: 'SNEAK-SESSION' };
    const encodedHeader = base64UrlEncode(JSON.stringify(header));
    const encodedPayload = base64UrlEncode(JSON.stringify(payload));
    const dataToSign = `${encodedHeader}.${encodedPayload}`;

    const key = await getSigningKey(secret);
    const enc = new TextEncoder();
    const signatureBuffer = await crypto.subtle.sign('HMAC', key, enc.encode(dataToSign));
    const encodedSignature = arrayBufferToBase64Url(signatureBuffer);

    return `${dataToSign}.${encodedSignature}`;
}

async function verifySessionToken(token, secret) {
    if (!token || typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length !== 3) return null;

    const [encodedHeader, encodedPayload, encodedSignature] = parts;
    const dataToSign = `${encodedHeader}.${encodedPayload}`;

    try {
        const key = await getSigningKey(secret);
        const enc = new TextEncoder();

        const sigBinary = base64UrlDecode(encodedSignature);
        const sigBytes = new Uint8Array(sigBinary.length);
        for (let i = 0; i < sigBinary.length; i++) {
            sigBytes[i] = sigBinary.charCodeAt(i);
        }

        const isValid = await crypto.subtle.verify('HMAC', key, sigBytes, enc.encode(dataToSign));
        if (!isValid) return null;

        const payload = JSON.parse(base64UrlDecode(encodedPayload));
        const now = Math.floor(Date.now() / 1000);
        if (payload.exp && payload.exp < now) {
            return null; // Expired
        }

        return payload;
    } catch {
        return null;
    }
}

/* ==========================================================================
   TENANT RESOLUTION, BOOTSTRAP & DOMAIN VERIFICATION
   ========================================================================== */

function getSigningSecret(env) {
    if (!env || !env.SNEAK_SIGNING_SECRET || typeof env.SNEAK_SIGNING_SECRET !== 'string' || !env.SNEAK_SIGNING_SECRET.trim()) {
        throw new Error('SNEAK_SIGNING_SECRET is not configured.');
    }
    return env.SNEAK_SIGNING_SECRET.trim();
}

/**
 * Generic entitlement helper: Evaluates SNEAK service entitlement status.
 * Backward compatible: allows service if no entitlement row exists (staging/demo accounts).
 */
function isAccountEntitled(accountStatus, entitlementStatus, graceUntil, now = new Date()) {
    if (accountStatus !== 'active') return false;
    if (!entitlementStatus) return true; // Backward compatibility

    const status = (entitlementStatus || '').toLowerCase().trim();
    if (status === 'active') return true;
    if (status === 'grace' || status === 'delinquent') {
        return Boolean(graceUntil && new Date(graceUntil) > now);
    }
    return false; // suspended, canceled
}

/**
 * GET /idx/v1/bootstrap?site=SITE_KEY
 * Called directly by embed.js on the embedding member webpage.
 * Validates member Origin against sneak_domains and issues a signed session token.
 */
async function handleBootstrap(req, url, env) {
    const siteKey = url.searchParams.get('site');
    if (!siteKey) {
        return jsonResponse({ error: 'MissingSiteKey', message: 'Site key is required for bootstrap.' }, 400);
    }

    const origin = req.headers.get('Origin') || '';
    const referer = req.headers.get('Referer') || '';
    const envName = (env.SNEAK_ENV || 'staging').toLowerCase();
    const isDev = envName === 'development';

    let requestHost = '';
    let effectiveOrigin = origin;

    if (origin) {
        try {
            requestHost = new URL(origin).hostname.toLowerCase();
        } catch {
            return jsonResponse({ error: 'InvalidOrigin', message: 'Origin header could not be parsed.' }, 400);
        }
    } else if (referer) {
        try {
            const parsedReferer = new URL(referer);
            requestHost = parsedReferer.hostname.toLowerCase();
            effectiveOrigin = `${parsedReferer.protocol}//${parsedReferer.host}`;
        } catch {
            requestHost = '';
        }
    }

    if (!requestHost) {
        if (!isDev) {
            return jsonResponse({
                error: 'DomainNotAuthorized',
                message: 'Origin header is required for SNEAK bootstrap in staging and production.'
            }, 403);
        }
    }

    const isDevHost = requestHost === 'localhost' || requestHost === '127.0.0.1' || requestHost === '::1';

    // 1. Fetch site, account, and entitlement
    if (!env.DB) {
        return jsonResponse({ error: 'DatabaseError', message: 'Database binding unavailable.' }, 500);
    }

    const query = `
        SELECT 
            s.id AS site_id, s.account_id, s.site_key, s.status AS site_status,
            a.account_name, a.status AS account_status,
            ent.status AS entitlement_status, ent.grace_until
        FROM sneak_sites s
        JOIN sneak_accounts a ON s.account_id = a.id
        LEFT JOIN sneak_account_entitlements ent ON a.id = ent.account_id
        WHERE s.site_key = ?
    `;
    const siteRecord = await env.DB.prepare(query).bind(siteKey).first();
    if (!siteRecord) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site key does not exist.' }, 404);
    }

    if (siteRecord.site_status !== 'active' || siteRecord.account_status !== 'active') {
        return jsonResponse({ error: 'SiteInactive', message: 'This SNEAK site is currently inactive or suspended.' }, 403);
    }

    // Generic entitlement check (backward compatible if no entitlement row exists)
    if (!isAccountEntitled(siteRecord.account_status, siteRecord.entitlement_status, siteRecord.grace_until)) {
        return jsonResponse({ error: 'EntitlementInactive', message: 'This SNEAK site service entitlement is currently inactive or expired.' }, 403);
    }

    // 2. Fetch verified domains (status = 'active' AND verified = 1)
    const domainsResult = await env.DB.prepare(
        "SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1"
    ).bind(siteRecord.site_id).all();
    const allowedDomains = (domainsResult.results || []).map(r => r.domain.toLowerCase().trim());

    let isAuthorized = false;

    if (isDevHost && isDev) {
        isAuthorized = true;
    } else {
        isAuthorized = allowedDomains.some(d => {
            if (d === '*') return false; // Global wildcard disallowed for tenants
            if (d === requestHost) return true;
            if (d.startsWith('*.')) {
                const rootDomain = d.slice(2);
                return requestHost === rootDomain || requestHost.endsWith('.' + rootDomain);
            }
            return false;
        });
    }

    if (!isAuthorized) {
        return jsonResponse({
            error: 'DomainNotAuthorized',
            message: `Domain '${requestHost || 'unknown'}' is not authorized or verified for this SNEAK site.`
        }, 403);
    }

    // 3. Issue short-lived session token (20 minutes = 1200 seconds)
    let secret;
    try {
        secret = getSigningSecret(env);
    } catch (err) {
        console.error('Session signing secret error:', err.message);
        return jsonResponse({
            error: 'ConfigurationError',
            message: 'SNEAK session signing secret is not configured.'
        }, 500);
    }

    const now = Math.floor(Date.now() / 1000);
    const exp = now + 1200;
    const tokenPayload = {
        siteKey: siteRecord.site_key,
        siteId: siteRecord.site_id,
        origin: requestHost,
        iat: now,
        exp: exp
    };

    const sessionToken = await signSessionToken(tokenPayload, secret);

    return jsonResponse({
        success: true,
        session: sessionToken,
        expiresIn: 1200,
        siteKey: siteRecord.site_key,
        searchUrl: `/search/?site=${encodeURIComponent(siteRecord.site_key)}`
    }, 200, effectiveOrigin);
}

/**
 * Resolves site and authenticates request using signed SNEAK session token
 */
async function resolveAndAuthorizeRequest(req, siteKey, origin, referer, env) {
    if (!env.DB) {
        return { authorized: false, error: 'DatabaseError', message: 'Database binding unavailable.', status: 500 };
    }

    // Lookup site, account, branding, and generic entitlement
    const query = `
        SELECT 
            s.id AS site_id, s.account_id, s.site_key, s.site_name, s.status AS site_status,
            s.scope_type, s.scope_value,
            a.account_name, a.status AS account_status, a.plan, a.agent_mls_id AS default_agent_mls_id, a.office_mls_id AS default_office_mls_id,
            b.display_name, b.brokerage, b.logo_url, b.agent_photo_url, b.primary_color, b.secondary_color,
            b.phone, b.email, b.website_url, b.config_json AS branding_config,
            ent.status AS entitlement_status, ent.grace_until
        FROM sneak_sites s
        JOIN sneak_accounts a ON s.account_id = a.id
        LEFT JOIN sneak_branding b ON s.id = b.site_id
        LEFT JOIN sneak_account_entitlements ent ON a.id = ent.account_id
        WHERE s.site_key = ?
    `;

    const siteRecord = await env.DB.prepare(query).bind(siteKey).first();
    if (!siteRecord) {
        return { authorized: false, error: 'SiteNotFound', message: 'Site key does not exist.', status: 404 };
    }

    if (siteRecord.site_status !== 'active' || siteRecord.account_status !== 'active') {
        return { authorized: false, error: 'SiteInactive', message: 'This SNEAK site is currently inactive or suspended.', status: 403 };
    }

    // Generic entitlement check (backward compatible if no entitlement row exists)
    if (!isAccountEntitled(siteRecord.account_status, siteRecord.entitlement_status, siteRecord.grace_until)) {
        return { authorized: false, error: 'EntitlementInactive', message: 'This SNEAK site service entitlement is currently inactive or expired.', status: 403 };
    }

    // Extract Session Token from Header (Authorization: Bearer <token> or X-SNEAK-Session) or query string
    let token = '';
    const authHeader = req.headers.get('Authorization') || '';
    if (authHeader.startsWith('Bearer ')) {
        token = authHeader.slice(7).trim();
    }
    if (!token) {
        token = req.headers.get('X-SNEAK-Session') || '';
    }
    if (!token) {
        try {
            const url = new URL(req.url);
            token = url.searchParams.get('session') || '';
        } catch {}
    }

    // Tenant data endpoints strictly require a session across all environments
    if (!token) {
        return {
            authorized: false,
            error: 'SessionRequired',
            message: 'A valid SNEAK session token is required to access tenant data.',
            status: 401
        };
    }

    let secret;
    try {
        secret = getSigningSecret(env);
    } catch (err) {
        return {
            authorized: false,
            error: 'ConfigurationError',
            message: 'Signing secret is not configured.',
            status: 500
        };
    }

    const sessionPayload = await verifySessionToken(token, secret);
    if (!sessionPayload) {
        return {
            authorized: false,
            error: 'InvalidSession',
            message: 'The SNEAK session token is invalid or expired. Please re-authenticate.',
            status: 401
        };
    }

    // Verify token site matches requested site
    if (sessionPayload.siteKey !== siteKey) {
        return {
            authorized: false,
            error: 'SessionMismatch',
            message: 'Session token does not match the requested site.',
            status: 403
        };
    }

    return {
        authorized: true,
        site: siteRecord,
        branding: siteRecord,
        session: sessionPayload,
        allowedOrigin: origin || null
    };
}

async function peekSiteKeyFromPost(req) {
    try {
        const body = await req.json();
        return body.siteKey || body.site || null;
    } catch {
        return null;
    }
}

async function handleCorsPreflight(req, url, env) {
    const origin = req.headers.get('Origin') || '';
    const siteKey = url.searchParams.get('site');

    // 1. Cross-origin access is specifically intended for /idx/v1/bootstrap
    if (url.pathname === '/idx/v1/bootstrap') {
        if (!siteKey || !origin || !env.DB) {
            return new Response(null, { status: 403 });
        }

        try {
            const site = await env.DB.prepare("SELECT id FROM sneak_sites WHERE site_key = ?").bind(siteKey).first();
            if (site) {
                const doms = await env.DB.prepare(
                    "SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1"
                ).bind(site.id).all();
                const host = new URL(origin).hostname.toLowerCase();
                const isDevHost = host === 'localhost' || host === '127.0.0.1' || host === '::1';
                const envName = (env.SNEAK_ENV || 'staging').toLowerCase();

                let matched = false;
                if (isDevHost && envName === 'development') {
                    matched = true;
                } else {
                    matched = (doms.results || []).some(r => {
                        const d = r.domain.toLowerCase().trim();
                        if (d === '*') return false; // Disallow global '*'
                        if (d === host) return true;
                        if (d.startsWith('*.')) {
                            const root = d.slice(2);
                            return host === root || host.endsWith('.' + root);
                        }
                        return false;
                    });
                }

                if (matched) {
                    const headers = new Headers();
                    headers.set('Access-Control-Allow-Origin', origin);
                    headers.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
                    headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, X-Site-Key, X-SNEAK-Session');
                    headers.set('Access-Control-Max-Age', '86400');
                    headers.set('Vary', 'Origin');
                    return new Response(null, { status: 204, headers });
                }
            }
        } catch {}

        return new Response(null, { status: 403 });
    }

    // 2. For other tenant endpoints (/idx/v1/*): only permit verified domains or same-origin
    if (siteKey && origin && env.DB) {
        try {
            const site = await env.DB.prepare("SELECT id FROM sneak_sites WHERE site_key = ?").bind(siteKey).first();
            if (site) {
                const doms = await env.DB.prepare(
                    "SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1"
                ).bind(site.id).all();
                const host = new URL(origin).hostname.toLowerCase();
                const matched = (doms.results || []).some(r => {
                    const d = r.domain.toLowerCase().trim();
                    if (d === '*') return false;
                    if (d === host) return true;
                    if (d.startsWith('*.')) {
                        const root = d.slice(2);
                        return host === root || host.endsWith('.' + root);
                    }
                    return false;
                });
                if (matched) {
                    const headers = new Headers();
                    headers.set('Access-Control-Allow-Origin', origin);
                    headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
                    headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, X-Site-Key, X-SNEAK-Session');
                    headers.set('Access-Control-Max-Age', '86400');
                    headers.set('Vary', 'Origin');
                    return new Response(null, { status: 204, headers });
                }
            }
        } catch {}
    }

    return new Response(null, { status: 403 });
}

async function handleStaticWithCSP(req, siteKey, env) {
    const res = await env.ASSETS.fetch(req);
    if (!res.ok) {
        return res;
    }

    // Retrieve verified domains to construct strict frame-ancestors
    let frameAncestors = "'self'";
    try {
        const site = await env.DB.prepare("SELECT id FROM sneak_sites WHERE site_key = ?").bind(siteKey).first();
        if (site) {
            const doms = await env.DB.prepare(
                "SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1"
            ).bind(site.id).all();
            
            const allowed = [];
            (doms.results || []).forEach(r => {
                const d = r.domain.trim();
                if (d === 'localhost' || d === '127.0.0.1') {
                    allowed.push(`http://${d}:*`, `http://${d}`);
                } else if (d.startsWith('*.')) {
                    allowed.push(`https://${d}`, `https://${d.slice(2)}`);
                } else if (d !== '*') {
                    // Strictly HTTPS without auto-expanding www
                    allowed.push(`https://${d}`);
                }
            });

            if (allowed.length > 0) {
                frameAncestors += ' ' + [...new Set(allowed)].join(' ');
            }
        }
    } catch (err) {
        console.warn('CSP resolution error:', err);
    }

    const newHeaders = new Headers(res.headers);
    newHeaders.set('Content-Security-Policy', `frame-ancestors ${frameAncestors};`);
    return new Response(res.body, { status: res.status, headers: newHeaders });
}

/* ==========================================================================
   FAIL-CLOSED TENANT SCOPE ENGINE
   ========================================================================== */

/**
 * Builds parameterized SQL WHERE clause and bindings for tenant listing scope.
 * Fails closed on missing or invalid scope configuration.
 */
function buildTenantListingScope(site, tableAlias = '') {
    const prefix = tableAlias ? `${tableAlias}.` : '';

    if (site.scope_type === 'market') {
        return {
            valid: true,
            clause: '1=1',
            binds: []
        };
    }

    if (site.scope_type === 'agent') {
        if (site.scope_value && typeof site.scope_value === 'string' && site.scope_value.trim()) {
            return {
                valid: true,
                clause: `${prefix}ListAgentMlsId = ?`,
                binds: [site.scope_value.trim()]
            };
        }
        // Missing agent scope value -> Fail closed
        return {
            valid: false,
            clause: '1=0',
            binds: [],
            error: 'MissingAgentScopeValue'
        };
    }

    if (site.scope_type === 'office') {
        if (site.scope_value && typeof site.scope_value === 'string' && site.scope_value.trim()) {
            const val = site.scope_value.trim();
            return {
                valid: true,
                clause: `(${prefix}ListOfficeMlsId = ? OR ${prefix}ListOfficeKey = ?)`,
                binds: [val, val]
            };
        }
        // Missing office scope value -> Fail closed
        return {
            valid: false,
            clause: '1=0',
            binds: [],
            error: 'MissingOfficeScopeValue'
        };
    }

    // Unknown or unconfigured scope_type -> Fail closed
    return {
        valid: false,
        clause: '1=0',
        binds: [],
        error: 'InvalidScopeType'
    };
}

/* ==========================================================================
   SHARED LISTING FILTER ENGINE (Search & Map Consistency)
   ========================================================================== */

/**
 * Validates whether a listing is eligible for IDX / Internet display.
 * Strictly fail-closed: requires InternetEntireListingDisplayYN = 1.
 */
function isListingIdxEligible(listing) {
    if (!listing) return false;
    if (listing.InternetEntireListingDisplayYN !== 1) {
        return false;
    }
    const eligibleStatuses = ['Active', 'Active Under Contract', 'Pending'];
    if (listing.StandardStatus && !eligibleStatuses.includes(listing.StandardStatus)) {
        return false;
    }
    return true;
}

/**
 * Suppresses address fields if InternetAddressDisplayYN is not 1.
 * Strictly fail-closed: suppresses address on false or null/unknown.
 */
function applyListingDisplayControls(item) {
    if (!item) return item;
    const transformed = { ...item };
    if (transformed.InternetAddressDisplayYN !== 1) {
        transformed.UnparsedAddress = "Address Undisclosed";
        transformed.StreetNumber = "";
        transformed.StreetName = "";
        transformed.UnitNumber = "";
    }
    return transformed;
}

/**
 * Shared filter helper that parses and standardizes SQL query filters
 * for BOTH /idx/v1/search and /idx/v1/map endpoints.
 */
function buildCommonListingFilters(params, site) {
    const city = (params.get('city') || '').substring(0, 200);
    const county = (params.get('county') || params.get('CountyOrParish') || '').substring(0, 200).trim();
    const postalCode = (params.get('postalCode') || params.get('zip') || params.get('PostalCode') || '').substring(0, 20).trim();
    const subdivision = (params.get('subdivision') || params.get('SubdivisionName') || '').substring(0, 200).trim();
    const minPrice = parseFloat(params.get('minPrice')) || null;
    const maxPrice = parseFloat(params.get('maxPrice')) || null;
    const beds = parseInt(params.get('beds'), 10) || null;
    const baths = parseInt(params.get('baths'), 10) || null;
    const minSqft = parseFloat(params.get('minSqft') || params.get('minLivingArea')) || null;
    const maxSqft = parseFloat(params.get('maxSqft') || params.get('maxLivingArea')) || null;
    const minAcres = parseFloat(params.get('minAcres') || params.get('minLotSizeAcres')) || null;
    const maxAcres = parseFloat(params.get('maxAcres') || params.get('maxLotSizeAcres')) || null;
    const minYear = parseInt(params.get('minYear') || params.get('minYearBuilt'), 10) || null;
    const maxYear = parseInt(params.get('maxYear') || params.get('maxYearBuilt'), 10) || null;
    const waterfront = params.get('waterfront');
    const pool = params.get('pool');
    const garage = parseInt(params.get('garage') || params.get('minGarage'), 10) || null;
    const newConstruction = params.get('newConstruction');
    const openHouseOnly = params.get('openHouse') === '1' || params.get('openHouseOnly') === 'true';
    const priceReduced = params.get('priceReduced') === '1' || params.get('priceReduced') === 'true';
    const newListingDays = parseInt(params.get('newListingDays'), 10) || null;
    const zoning = (params.get('zoning') || '').trim();
    const propertyType = (params.get('propertyType') || 'sale').toLowerCase();
    const propertySubType = params.get('propertySubType');
    const status = params.get('status') || 'Active';
    const q = (params.get('q') || params.get('search') || '').substring(0, 200).trim();

    const whereClauses = [];
    const bindValues = [];

    // 1. Enforce Fail-Closed Tenant Scope
    const scope = buildTenantListingScope(site);
    if (!scope.valid) {
        return { valid: false, error: scope.error };
    }
    whereClauses.push(scope.clause);
    bindValues.push(...scope.binds);

    // 2. Internet Entire Listing Display Compliance (Fail Closed)
    whereClauses.push("InternetEntireListingDisplayYN = 1");

    // Optional agent or office filter narrowing for market-scoped sites
    if (site.scope_type === 'market') {
        const agentMlsId = params.get('agentMlsId');
        if (agentMlsId) {
            whereClauses.push("ListAgentMlsId = ?");
            bindValues.push(agentMlsId);
        }
        const officeMlsId = params.get('officeMlsId');
        if (officeMlsId) {
            whereClauses.push("(ListOfficeMlsId = ? OR ListOfficeKey = ?)");
            bindValues.push(officeMlsId, officeMlsId);
        }
    }

    // 3. Standard Status Filtering
    if (status === 'Pending') {
        whereClauses.push("(StandardStatus = 'Pending' OR StandardStatus = 'Active Under Contract')");
    } else if (status === 'Active Under Contract') {
        whereClauses.push("StandardStatus = 'Active Under Contract'");
    } else if (status === 'Closed') {
        whereClauses.push("StandardStatus = 'Closed'");
    } else if (status === 'All') {
        whereClauses.push("(StandardStatus = 'Active' OR StandardStatus = 'Active Under Contract' OR StandardStatus = 'Pending')");
    } else {
        whereClauses.push("StandardStatus = 'Active'");
    }

    // 4. Property Type Filtering
    if (propertyType === 'sale') {
        whereClauses.push("(PropertyType = 'Residential' OR PropertyType = 'Residential Income' OR PropertyType = 'Boat Dock')");
    } else if (propertyType === 'rental') {
        whereClauses.push("PropertyType = 'Residential Lease'");
    } else if (propertyType === 'commercial') {
        whereClauses.push("(PropertyType = 'Commercial Sale' OR PropertyType = 'Commercial' OR PropertyType = 'Commercial Lease' OR PropertyType = 'Business Opportunity')");
    } else if (propertyType === 'land') {
        whereClauses.push("PropertyType = 'Land'");
    } else if (propertyType && propertyType !== 'all') {
        whereClauses.push("PropertyType = ?");
        bindValues.push(propertyType);
    }

    // 5. SubType Filtering (Only apply residential expansion if propertyType is sale or rental)
    if (propertySubType && (propertyType === 'sale' || propertyType === 'rental' || propertyType === 'all')) {
        const subTypes = propertySubType.split(',').map(s => s.trim()).filter(Boolean);
        if (subTypes.length > 0) {
            const expanded = [];
            subTypes.forEach(st => {
                if (st === 'Single Family Residence') { expanded.push('Single Family Residence', 'Manufactured Home'); }
                else if (st === 'Condominium') { expanded.push('Condominium', 'High Rise (8+)', 'Mid Rise (4-7)', 'Low Rise (1-3)'); }
                else if (st === 'Townhouse') { expanded.push('Townhouse'); }
                else if (st === 'Multi Family') { expanded.push('Multi Family', 'Duplex', 'Triplex', 'Quadruplex'); }
                else if (st === 'Villa') { expanded.push('Villa Attached', 'Villa Detached'); }
                else { expanded.push(st); }
            });
            const placeholders = expanded.map(() => '?').join(',');
            whereClauses.push(`PropertySubType IN (${placeholders})`);
            bindValues.push(...expanded);
        }
    }

    // 6. City Filtering
    if (city) {
        const cities = city.split(',').map(c => c.trim()).filter(Boolean);
        if (cities.length === 1) {
            whereClauses.push("LOWER(City) = LOWER(?)");
            bindValues.push(cities[0]);
        } else if (cities.length > 1) {
            const placeholders = cities.map(() => 'LOWER(?)').join(',');
            whereClauses.push(`LOWER(City) IN (${placeholders})`);
            bindValues.push(...cities);
        }
    }

    // 7. County Filtering
    if (county) {
        whereClauses.push("LOWER(CountyOrParish) = LOWER(?)");
        bindValues.push(county);
    }

    // 8. Postal Code Filtering
    if (postalCode) {
        whereClauses.push("PostalCode = ?");
        bindValues.push(postalCode);
    }

    // 9. Subdivision Filtering
    if (subdivision) {
        whereClauses.push("LOWER(SubdivisionName) LIKE ?");
        bindValues.push(`%${subdivision.toLowerCase()}%`);
    }

    // 10. Price Range
    if (minPrice !== null && minPrice > 0) {
        whereClauses.push("ListPrice >= ?");
        bindValues.push(minPrice);
    }
    if (maxPrice !== null && maxPrice > 0) {
        whereClauses.push("ListPrice <= ?");
        bindValues.push(maxPrice);
    }

    // 11. Bedrooms / Bathrooms (Only apply to residential search)
    if (propertyType === 'sale' || propertyType === 'rental' || propertyType === 'all') {
        if (beds !== null && beds > 0) {
            whereClauses.push("BedroomsTotal >= ?");
            bindValues.push(beds);
        }
        if (baths !== null && baths > 0) {
            whereClauses.push("BathroomsTotalInteger >= ?");
            bindValues.push(baths);
        }
    }

    // 12. Size: Living Area (Sqft) min/max
    if (minSqft !== null && minSqft > 0) {
        whereClauses.push("LivingArea >= ?");
        bindValues.push(minSqft);
    }
    if (maxSqft !== null && maxSqft > 0) {
        whereClauses.push("LivingArea <= ?");
        bindValues.push(maxSqft);
    }

    // 13. Size: Lot Size (Acres) min/max
    if (minAcres !== null && minAcres > 0) {
        whereClauses.push("LotSizeAcres >= ?");
        bindValues.push(minAcres);
    }
    if (maxAcres !== null && maxAcres > 0) {
        whereClauses.push("LotSizeAcres <= ?");
        bindValues.push(maxAcres);
    }

    // 14. Year Built min/max
    if (minYear !== null && minYear > 0) {
        whereClauses.push("YearBuilt >= ?");
        bindValues.push(minYear);
    }
    if (maxYear !== null && maxYear > 0) {
        whereClauses.push("YearBuilt <= ?");
        bindValues.push(maxYear);
    }

    // 15. Wave-1 Amenities
    if (waterfront === '1' || waterfront === 'true') {
        whereClauses.push("WaterfrontYN = 1");
    }
    if (pool === '1' || pool === 'true') {
        whereClauses.push("PoolPrivateYN = 1");
    }
    if (garage !== null && garage > 0) {
        whereClauses.push("GarageSpaces >= ?");
        bindValues.push(garage);
    }
    if (newConstruction === '1' || newConstruction === 'true') {
        whereClauses.push("NewConstructionYN = 1");
    }
    if (priceReduced) {
        whereClauses.push("(OriginalListPrice IS NOT NULL AND ListPrice < OriginalListPrice)");
    }
    if (newListingDays !== null && newListingDays > 0) {
        whereClauses.push("ListingContractDate >= date('now', ?)");
        bindValues.push(`-${newListingDays} days`);
    }
    if (openHouseOnly) {
        whereClauses.push("ListingKey IN (SELECT DISTINCT ListingKey FROM sneak_open_houses WHERE OpenHouseStatus = 'Active' AND (OpenHouseDate IS NULL OR OpenHouseDate >= date('now')))");
    }
    if (zoning) {
        whereClauses.push("LOWER(Zoning) = LOWER(?)");
        bindValues.push(zoning);
    }

    // 16. Unified Text Search
    if (q) {
        whereClauses.push("(ListingKey = ? OR ListingId = ? OR PostalCode = ? OR LOWER(UnparsedAddress) LIKE ? OR LOWER(City) LIKE ? OR LOWER(SubdivisionName) LIKE ? OR LOWER(ListAgentFullName) LIKE ? OR LOWER(ListAgentMlsId) = ?)");
        const likeQ = `%${q.toLowerCase()}%`;
        bindValues.push(q, q, q, likeQ, likeQ, likeQ, likeQ, q.toLowerCase());
    }

    // 17. Geographic Viewport Bounding Box (north, south, east, west)
    const north = parseFloat(params.get('north'));
    const south = parseFloat(params.get('south'));
    const east = parseFloat(params.get('east'));
    const west = parseFloat(params.get('west'));

    if (!isNaN(north) && !isNaN(south) && north >= -90 && north <= 90 && south >= -90 && south <= 90) {
        const minLat = Math.min(south, north);
        const maxLat = Math.max(south, north);
        whereClauses.push("Latitude IS NOT NULL AND Latitude >= ? AND Latitude <= ?");
        bindValues.push(minLat, maxLat);
    }

    if (!isNaN(east) && !isNaN(west) && east >= -180 && east <= 180 && west >= -180 && west <= 180) {
        if (west <= east) {
            whereClauses.push("Longitude IS NOT NULL AND Longitude >= ? AND Longitude <= ?");
            bindValues.push(west, east);
        } else {
            // Anti-meridian boundary
            whereClauses.push("Longitude IS NOT NULL AND (Longitude >= ? OR Longitude <= ?)");
            bindValues.push(west, east);
        }
    }

    return {
        valid: true,
        whereSQL: `WHERE ${whereClauses.join(' AND ')}`,
        whereClauses,
        bindValues
    };
}

/* ==========================================================================
   ENDPOINT HANDLERS
   ========================================================================== */

/**
 * GET /idx/v1/config?site=abc123
 */
async function handleGetConfig(site, branding, env, allowedOrigin) {
    const rawWidgets = await env.DB.prepare(`
        SELECT widget_type, config_json, enabled 
        FROM sneak_widget_configs 
        WHERE site_id = ?
    `).bind(site.site_id).all();

    const widgets = {};
    (rawWidgets.results || []).forEach(w => {
        let conf = {};
        if (w.config_json) {
            try { conf = JSON.parse(w.config_json); } catch {}
        }
        widgets[w.widget_type] = {
            enabled: Boolean(w.enabled),
            config: conf
        };
    });

    let customBranding = {};
    if (branding.branding_config) {
        try { customBranding = JSON.parse(branding.branding_config); } catch {}
    }

    const configPayload = {
        siteKey: site.site_key,
        siteName: site.site_name,
        displayName: branding.display_name || site.site_name || 'Real Estate Search',
        brokerage: branding.brokerage || '',
        logoUrl: branding.logo_url || '',
        agentPhotoUrl: branding.agent_photo_url || '',
        primaryColor: branding.primary_color || '#1a2a3a',
        secondaryColor: branding.secondary_color || '#2596be',
        phone: branding.phone || '',
        email: branding.email || '',
        websiteUrl: branding.website_url || '',
        displayScope: site.scope_type || 'market',
        participantAgentMlsId: site.default_agent_mls_id || site.scope_value || null,
        featuredListingsScope: 'agent',
        tenantScope: {
            type: site.scope_type || 'market',
            value: site.scope_value || null
        },
        scope: {
            type: site.scope_type || 'market',
            value: site.scope_value || null
        },
        brandingConfig: customBranding,
        features: {
            search: true,
            map: true,
            savedListings: true,
            openHouses: true,
            featuredListings: true,
            leadCapture: true
        },
        widgets
    };

    return jsonResponse(configPayload, 200, allowedOrigin, 'public, max-age=300, s-maxage=600');
}

/**
 * GET /idx/v1/search?site=abc123&page=1&limit=20&sort=newest&city=...
 */
async function handleSearch(url, site, env, ctx, allowedOrigin) {
    const params = url.searchParams;
    const filter = buildCommonListingFilters(params, site);

    if (!filter.valid) {
        return jsonResponse({
            error: 'InvalidTenantScope',
            message: 'Tenant scope is invalid or incomplete. Access denied.'
        }, 403, allowedOrigin);
    }

    const page = Math.max(1, parseInt(params.get('page'), 10) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(params.get('limit'), 10) || 20));
    const offset = (page - 1) * limit;

    // Sorting
    const sort = params.get('sort') || 'newest';
    let orderSQL = "ORDER BY ModificationTimestamp DESC, ListingContractDate DESC";
    if (sort === 'priceDesc') {
        orderSQL = "ORDER BY ListPrice DESC, ListingContractDate DESC";
    } else if (sort === 'priceAsc') {
        orderSQL = "ORDER BY ListPrice ASC, ListingContractDate DESC";
    } else if (sort === 'sqftDesc') {
        orderSQL = "ORDER BY LivingArea DESC NULLS LAST, ListPrice DESC";
    } else if (sort === 'acresDesc') {
        orderSQL = "ORDER BY LotSizeAcres DESC NULLS LAST, ListPrice DESC";
    } else if (sort === 'yearDesc') {
        orderSQL = "ORDER BY YearBuilt DESC NULLS LAST, ListPrice DESC";
    } else if (sort === 'dateAsc') {
        orderSQL = "ORDER BY ListingContractDate ASC";
    }

    // Count Total
    const countSQL = `SELECT COUNT(*) AS total FROM sneak_listings ${filter.whereSQL}`;
    const countRes = await env.DB.prepare(countSQL).bind(...filter.bindValues).first();
    const total = countRes ? countRes.total : 0;

    // Fetch Results
    const selectCols = `
        ListingKey, ListingId, ListPrice, OriginalListPrice, UnparsedAddress, City, StateOrProvince, PostalCode, CountyOrParish,
        BedroomsTotal, BathroomsTotalInteger, LivingArea, StandardStatus,
        PropertyType, PropertySubType, PrimaryPhoto, ListingContractDate,
        Latitude, Longitude, ModificationTimestamp, YearBuilt, LotSizeAcres,
        ListAgentFullName, ListOfficeName, ListOfficePhone, ListAgentMlsId, ListOfficeMlsId, SubdivisionName,
        WaterfrontYN, PoolPrivateYN, GarageSpaces, NewConstructionYN, Zoning,
        InternetEntireListingDisplayYN, InternetAddressDisplayYN
    `;
    const searchSQL = `SELECT ${selectCols} FROM sneak_listings ${filter.whereSQL} ${orderSQL} LIMIT ? OFFSET ?`;
    const results = await env.DB.prepare(searchSQL).bind(...filter.bindValues, limit, offset).all();

    const formattedListings = (results.results || []).map(row => {
        const item = applyListingDisplayControls(row);
        return {
            ...item,
            Coordinates: (item.Longitude && item.Latitude) ? [item.Longitude, item.Latitude] : null,
            Media: item.PrimaryPhoto ? [{ MediaURL: item.PrimaryPhoto, Order: 0 }] : []
        };
    });

    if (ctx && ctx.waitUntil) {
        ctx.waitUntil(recordUsage(site.site_id, 'searches', env));
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
    }, 200, allowedOrigin, 'public, max-age=60, s-maxage=120');
}

/**
 * GET /idx/v1/map?site=abc123&city=...&minPrice=...&north=...&south=...&east=...&west=...
 */
async function handleMap(url, site, env, ctx, allowedOrigin) {
    const params = url.searchParams;
    const filter = buildCommonListingFilters(params, site);

    if (!filter.valid) {
        return jsonResponse({
            error: 'InvalidTenantScope',
            message: 'Tenant scope is invalid or incomplete. Access denied.'
        }, 403, allowedOrigin);
    }

    const markerLimit = Math.min(2000, Math.max(1, parseInt(params.get('limit'), 10) || 500));
    const mapSQL = `
        SELECT 
            ListingKey, ListingId, ListPrice, UnparsedAddress, City, PostalCode, StandardStatus, 
            PropertyType, PropertySubType, PrimaryPhoto, Latitude, Longitude, 
            BedroomsTotal, BathroomsTotalInteger, LivingArea, LotSizeAcres, SubdivisionName, 
            YearBuilt, Zoning, ListOfficeName, InternetAddressDisplayYN, InternetEntireListingDisplayYN
        FROM sneak_listings
        ${filter.whereSQL}
        LIMIT ?
    `;

    const results = await env.DB.prepare(mapSQL).bind(...filter.bindValues, markerLimit + 1).all();
    const rawListings = results.results || [];
    const truncated = rawListings.length > markerLimit;
    const slicedListings = truncated ? rawListings.slice(0, markerLimit) : rawListings;

    const markers = slicedListings.map(row => {
        const item = applyListingDisplayControls(row);
        return {
            ListingKey: item.ListingKey,
            ListingId: item.ListingId,
            ListPrice: item.ListPrice,
            UnparsedAddress: item.UnparsedAddress,
            City: item.City,
            PostalCode: item.PostalCode,
            StandardStatus: item.StandardStatus,
            PropertyType: item.PropertyType,
            PropertySubType: item.PropertySubType,
            PrimaryPhoto: item.PrimaryPhoto,
            Latitude: item.Latitude,
            Longitude: item.Longitude,
            BedroomsTotal: item.BedroomsTotal,
            BathroomsTotalInteger: item.BathroomsTotalInteger,
            LivingArea: item.LivingArea,
            LotSizeAcres: item.LotSizeAcres,
            SubdivisionName: item.SubdivisionName,
            YearBuilt: item.YearBuilt,
            Zoning: item.Zoning,
            ListOfficeName: item.ListOfficeName
        };
    });

    return jsonResponse({
        data: markers,
        count: markers.length,
        limit: markerLimit,
        truncated
    }, 200, allowedOrigin, 'public, max-age=60, s-maxage=120');
}

/**
 * GET /idx/v1/listing/:listingKey?site=abc123
 */
async function handleListingDetail(listingKey, site, req, env, ctx, allowedOrigin) {
    const scope = buildTenantListingScope(site);
    if (!scope.valid) {
        return jsonResponse({ error: 'InvalidTenantScope', message: 'Tenant scope is invalid.' }, 403, allowedOrigin);
    }

    const query = `
        SELECT * FROM sneak_listings 
        WHERE (ListingKey = ? OR ListingId = ?) AND ${scope.clause}
    `;
    const d1Listing = await env.DB.prepare(query).bind(listingKey, listingKey, ...scope.binds).first();

    if (!d1Listing) {
        const globalExists = await env.DB.prepare("SELECT ListingKey, InternetEntireListingDisplayYN, StandardStatus FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?)").bind(listingKey, listingKey).first();
        if (globalExists && isListingIdxEligible(globalExists)) {
            return jsonResponse({ error: 'ScopeMismatch', message: 'Property is outside this tenant authorized scope.' }, 403, allowedOrigin);
        }
        return jsonResponse({ error: 'ListingNotFound', message: 'Property not found or not accessible within this scope.' }, 404, allowedOrigin);
    }

    if (!isListingIdxEligible(d1Listing)) {
        return jsonResponse({ error: 'ListingNotFound', message: 'Property is not accessible for online display.' }, 404, allowedOrigin);
    }

    let fullDetails = applyListingDisplayControls({ ...d1Listing });

    if (fullDetails.Longitude && fullDetails.Latitude && !fullDetails.Coordinates) {
        fullDetails.Coordinates = [fullDetails.Longitude, fullDetails.Latitude];
    }

    // Parse synchronized full media gallery from D1 MediaJSON
    let mediaArray = [];
    if (fullDetails.MediaJSON) {
        try {
            const parsed = JSON.parse(fullDetails.MediaJSON);
            if (Array.isArray(parsed) && parsed.length > 0) {
                mediaArray = parsed.map((url, idx) => ({ MediaURL: url, Order: idx }));
            }
        } catch {}
    }
    if (mediaArray.length === 0 && fullDetails.PrimaryPhoto) {
        mediaArray = [{ MediaURL: fullDetails.PrimaryPhoto, Order: 0 }];
    }
    fullDetails.Media = mediaArray;

    if (ctx && ctx.waitUntil) {
        ctx.waitUntil(recordUsage(site.site_id, 'listing_views', env));
    }

    return jsonResponse({ data: fullDetails }, 200, allowedOrigin, 'public, max-age=120, s-maxage=600');
}

/**
 * GET /idx/v1/listing/:listingKey/media?site=abc123
 */
async function handleListingMedia(listingKey, site, req, env, ctx, allowedOrigin) {
    const scope = buildTenantListingScope(site);
    if (!scope.valid) {
        return jsonResponse({ error: 'InvalidTenantScope', message: 'Tenant scope is invalid.' }, 403, allowedOrigin);
    }

    const row = await env.DB.prepare(
        `SELECT ListingKey, PrimaryPhoto, MediaJSON, InternetEntireListingDisplayYN, StandardStatus FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?) AND ${scope.clause}`
    ).bind(listingKey, listingKey, ...scope.binds).first();

    if (!row || !isListingIdxEligible(row)) {
        return jsonResponse({ error: 'ListingNotFound', message: 'Property media not accessible.' }, 404, allowedOrigin);
    }

    let mediaUrls = [];
    if (row.MediaJSON) {
        try {
            const parsed = JSON.parse(row.MediaJSON);
            if (Array.isArray(parsed)) {
                mediaUrls = parsed.filter(Boolean);
            }
        } catch {}
    }
    if (mediaUrls.length === 0 && row.PrimaryPhoto) {
        mediaUrls = [row.PrimaryPhoto];
    }

    const payload = {
        listingKey: row.ListingKey,
        media: mediaUrls
    };

    return jsonResponse(payload, 200, allowedOrigin, 'public, max-age=300, s-maxage=600');
}

/**
 * GET /idx/v1/agent/:mlsId/listings?site=abc123
 */
async function handleAgentListings(agentMlsId, url, site, env, allowedOrigin) {
    if (site.scope_type === 'agent' && site.scope_value && site.scope_value !== agentMlsId) {
        return jsonResponse({
            error: 'Forbidden',
            message: 'Agent-scoped sites cannot query listings for other MLS agents.'
        }, 403, allowedOrigin);
    }

    const scope = buildTenantListingScope(site);
    if (!scope.valid) {
        return jsonResponse({ error: 'InvalidTenantScope', message: 'Tenant scope is invalid.' }, 403, allowedOrigin);
    }

    const limit = Math.min(50, Math.max(1, parseInt(url.searchParams.get('limit'), 10) || 20));

    const results = await env.DB.prepare(`
        SELECT ListingKey, ListingId, ListPrice, UnparsedAddress, City, BedroomsTotal, BathroomsTotalInteger, LivingArea, PrimaryPhoto, StandardStatus, PropertyType, PropertySubType
        FROM sneak_listings
        WHERE ListAgentMlsId = ? AND StandardStatus = 'Active' AND ${scope.clause}
        ORDER BY ListingContractDate DESC LIMIT ?
    `).bind(agentMlsId, ...scope.binds, limit).all();

    return jsonResponse({ data: results.results || [] }, 200, allowedOrigin, 'public, max-age=300, s-maxage=600');
}

/**
 * GET /idx/v1/open-houses?site=abc123
 */
async function handleOpenHouses(url, site, env, allowedOrigin) {
    const scope = buildTenantListingScope(site, 'l');
    if (!scope.valid) {
        return jsonResponse({ error: 'InvalidTenantScope', message: 'Tenant scope is invalid.' }, 403, allowedOrigin);
    }

    const todayStr = new Date().toISOString().split('T')[0];
    const dateFrom = url.searchParams.get('dateFrom') || todayStr;
    const dateTo = url.searchParams.get('dateTo');
    const city = (url.searchParams.get('city') || '').trim();
    const agentMlsId = url.searchParams.get('agent');
    const officeMlsId = url.searchParams.get('office');

    const whereClauses = ["oh.OpenHouseDate >= ?"];
    const bindValues = [dateFrom];

    if (dateTo) {
        whereClauses.push("oh.OpenHouseDate <= ?");
        bindValues.push(dateTo);
    }

    whereClauses.push(scope.clause);
    bindValues.push(...scope.binds);

    if (city) {
        whereClauses.push("LOWER(l.City) = LOWER(?)");
        bindValues.push(city);
    }

    if (site.scope_type === 'market') {
        if (agentMlsId) {
            whereClauses.push("l.ListAgentMlsId = ?");
            bindValues.push(agentMlsId);
        }
        if (officeMlsId) {
            whereClauses.push("(l.ListOfficeMlsId = ? OR l.ListOfficeKey = ?)");
            bindValues.push(officeMlsId, officeMlsId);
        }
    }

    const query = `
        SELECT 
            oh.OpenHouseKey, oh.ListingKey, oh.OpenHouseStartTime, oh.OpenHouseEndTime, oh.OpenHouseDate, oh.OpenHouseRemarks, oh.PropertyData,
            l.UnparsedAddress, l.City, l.ListPrice, l.BedroomsTotal, l.BathroomsTotalInteger, l.LivingArea, l.PrimaryPhoto, l.ListAgentFullName, l.ListOfficeName
        FROM sneak_open_houses oh
        JOIN sneak_listings l ON oh.ListingKey = l.ListingKey
        WHERE ${whereClauses.join(' AND ')}
        ORDER BY oh.OpenHouseDate ASC, oh.OpenHouseStartTime ASC
        LIMIT 100
    `;

    const results = await env.DB.prepare(query).bind(...bindValues).all();

    const data = (results.results || []).map(row => {
        let property = null;
        try { property = JSON.parse(row.PropertyData); } catch {}
        if (!property) {
            property = {
                ListingKey: row.ListingKey,
                UnparsedAddress: row.UnparsedAddress,
                City: row.City,
                ListPrice: row.ListPrice,
                BedroomsTotal: row.BedroomsTotal,
                BathroomsTotalInteger: row.BathroomsTotalInteger,
                LivingArea: row.LivingArea,
                PrimaryPhoto: row.PrimaryPhoto,
                ListAgentFullName: row.ListAgentFullName,
                ListOfficeName: row.ListOfficeName
            };
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
    });

    return jsonResponse({ data }, 200, allowedOrigin, 'public, max-age=300, s-maxage=300');
}

/**
 * POST /idx/v1/lead
 */
async function handleLeadSubmission(req, site, env, ctx, allowedOrigin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Malformed JSON payload.' }, 400, allowedOrigin);
    }

    const name = (body.name || '').trim().substring(0, 100);
    const email = (body.email || '').trim().substring(0, 150);
    const phone = (body.phone || '').trim().substring(0, 30);
    const message = (body.message || '').trim().substring(0, 2000);
    const listingKey = (body.listingKey || '').trim().substring(0, 50) || null;
    const leadType = (body.leadType || 'property_inquiry').substring(0, 50);
    const sourceUrl = (body.sourceUrl || req.headers.get('Referer') || '').substring(0, 500);

    // Validation
    if (!name || !email) {
        return jsonResponse({ error: 'ValidationError', message: 'Name and Email are required fields.' }, 400, allowedOrigin);
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
        return jsonResponse({ error: 'ValidationError', message: 'Please provide a valid email address.' }, 400, allowedOrigin);
    }

    // If listingKey is provided, strictly validate that it exists inside the tenant's authorized scope
    if (listingKey) {
        const scope = buildTenantListingScope(site);
        if (!scope.valid) {
            return jsonResponse({ error: 'InvalidTenantScope', message: 'Tenant scope is invalid.' }, 403, allowedOrigin);
        }
        const exists = await env.DB.prepare(
            `SELECT ListingKey FROM sneak_listings WHERE (ListingKey = ? OR ListingId = ?) AND ${scope.clause}`
        ).bind(listingKey, listingKey, ...scope.binds).first();
        
        if (!exists) {
            return jsonResponse({
                error: 'InvalidListingKey',
                message: 'The requested listing does not exist or is outside this site scope.'
            }, 400, allowedOrigin);
        }
    }

    const leadId = 'lead_' + Date.now().toString(36) + '_' + Math.random().toString(36).substring(2, 7);

    // Save lead into sneak_leads
    await env.DB.prepare(`
        INSERT INTO sneak_leads (
            id, site_id, listing_key, lead_type, name, email, phone, message, source_url, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    `).bind(
        leadId, site.site_id, listingKey, leadType, name, email, phone, message, sourceUrl
    ).run();

    if (ctx && ctx.waitUntil) {
        ctx.waitUntil(recordUsage(site.site_id, 'leads', env));
    }

    return jsonResponse({
        success: true,
        leadId,
        message: 'Inquiry received successfully. Our team will contact you soon.'
    }, 201, allowedOrigin);
}

/* ==========================================================================
   METRICS & SYNCHRONIZATION ENGINE
   ========================================================================== */

async function recordUsage(siteId, column, env) {
    try {
        const today = new Date().toISOString().split('T')[0];
        const id = `${siteId}_${today}`;
        await env.DB.prepare(`
            INSERT INTO sneak_usage (id, site_id, usage_date, ${column})
            VALUES (?, ?, ?, 1)
            ON CONFLICT(site_id, usage_date) DO UPDATE SET
                ${column} = ${column} + 1
        `).bind(id, siteId, today).run();
    } catch (err) {
        console.warn('Failed to record usage metric:', err);
    }
}

/**
 * Synchronizes full statewide / regional IDX listings from Bridge into sneak_listings.
 */
async function syncSneakListings(env) {
    console.log("Starting SNEAK Listing Dataset Sync...");

    const SEL = [
        'ListingKey', 'ListingId', 'ListPrice', 'OriginalListPrice',
        'UnparsedAddress', 'StreetNumber', 'StreetName', 'UnitNumber',
        'City', 'StateOrProvince', 'PostalCode', 'CountyOrParish',
        'BedroomsTotal', 'BathroomsTotalInteger', 'BathroomsFull', 'BathroomsHalf',
        'LivingArea', 'StandardStatus', 'PropertyType', 'PropertySubType',
        'ListingContractDate', 'ModificationTimestamp', 'StatusChangeTimestamp',
        'YearBuilt', 'LotSizeAcres', 'Latitude', 'Longitude', 'Coordinates',
        'Media', 'PublicRemarks', 'SubdivisionName',
        'ListAgentKey', 'ListAgentMlsId', 'ListAgentFullName', 'ListAgentEmail', 'ListAgentDirectPhone',
        'ListOfficeKey', 'ListOfficeMlsId', 'ListOfficeName', 'ListOfficePhone',
        'OriginatingSystemKey', 'OriginatingSystemName'
    ].join(',');

    const baseFilter = "OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";
    const batchSize = 200;
    const allFetchedKeys = new Set();

    const searchParams = new URLSearchParams({
        '$filter': baseFilter,
        '$select': SEL,
        '$top': batchSize.toString()
    });

    let next = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?${searchParams.toString()}&access_token=${env.BRIDGE_TOKEN}`;
    let syncSuccess = true;
    let totalProcessed = 0;

    while (next) {
        let res;
        try {
            res = await fetch(next, { headers: { Accept: 'application/json' } });
        } catch (fetchErr) {
            console.error("Bridge network error during listing sync:", fetchErr);
            syncSuccess = false;
            break;
        }

        if (!res.ok) {
            console.error(`Bridge API returned HTTP ${res.status} during listing sync.`);
            syncSuccess = false;
            break;
        }

        let data;
        try {
            data = await res.json();
        } catch (jsonErr) {
            console.error("Bridge response JSON parse error:", jsonErr);
            syncSuccess = false;
            break;
        }

        const items = data.value || [];
        if (!items.length) break;

        const statements = items.map(i => {
            allFetchedKeys.add(i.ListingKey);

            let photo = '';
            if (i.Media && Array.isArray(i.Media) && i.Media.length > 0) {
                const sorted = i.Media.sort((a, b) => (a.Order || 0) - (b.Order || 0));
                photo = sorted[0].MediaURL || sorted[0].MediaUrl || sorted[0].MediaURLLarge || '';
            }

            const lat = i.Latitude || i.Coordinates?.[1] || null;
            const lng = i.Longitude || i.Coordinates?.[0] || null;

            return env.DB.prepare(`
                INSERT OR REPLACE INTO sneak_listings (
                    ListingKey, ListingId, ListPrice, OriginalListPrice,
                    UnparsedAddress, StreetNumber, StreetName, UnitNumber,
                    City, StateOrProvince, PostalCode, CountyOrParish,
                    BedroomsTotal, BathroomsTotalInteger, BathroomsFull, BathroomsHalf,
                    LivingArea, StandardStatus, PropertyType, PropertySubType,
                    ListingContractDate, ModificationTimestamp, StatusChangeTimestamp,
                    YearBuilt, LotSizeAcres, Latitude, Longitude,
                    PrimaryPhoto, PublicRemarks, SubdivisionName,
                    ListAgentKey, ListAgentMlsId, ListAgentFullName, ListAgentEmail, ListAgentDirectPhone,
                    ListOfficeKey, ListOfficeMlsId, ListOfficeName, ListOfficePhone,
                    OriginatingSystemKey, OriginatingSystemName, updated_at
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, CURRENT_TIMESTAMP
                )
            `).bind(
                i.ListingKey, i.ListingId || null, i.ListPrice || null, i.OriginalListPrice || null,
                i.UnparsedAddress || null, i.StreetNumber || null, i.StreetName || null, i.UnitNumber || null,
                i.City || null, i.StateOrProvince || null, i.PostalCode || null, i.CountyOrParish || null,
                i.BedroomsTotal || null, i.BathroomsTotalInteger || null, i.BathroomsFull || null, i.BathroomsHalf || null,
                i.LivingArea || null, i.StandardStatus || null, i.PropertyType || null, i.PropertySubType || null,
                i.ListingContractDate || null, i.ModificationTimestamp || null, i.StatusChangeTimestamp || null,
                i.YearBuilt || null, i.LotSizeAcres || null, lat, lng,
                photo, i.PublicRemarks || null, i.SubdivisionName || null,
                i.ListAgentKey || null, i.ListAgentMlsId || null, i.ListAgentFullName || null, i.ListAgentEmail || null, i.ListAgentDirectPhone || null,
                i.ListOfficeKey || null, i.ListOfficeMlsId || null, i.ListOfficeName || null, i.ListOfficePhone || null,
                i.OriginatingSystemKey || null, i.OriginatingSystemName || null
            );
        });

        for (let i = 0; i < statements.length; i += 50) {
            await env.DB.batch(statements.slice(i, i + 50));
        }

        totalProcessed += items.length;

        next = data['@odata.nextLink'] || null;
        if (next && !next.includes('access_token')) {
            next += (next.includes('?') ? '&' : '?') + 'access_token=' + env.BRIDGE_TOKEN;
        }
    }

    // Safe Stale Cleanup: Only prune listings if the entire sync succeeded without errors
    if (syncSuccess && allFetchedKeys.size > 0) {
        try {
            const existing = await env.DB.prepare("SELECT ListingKey FROM sneak_listings").all();
            const staleKeys = (existing.results || []).filter(r => !allFetchedKeys.has(r.ListingKey)).map(r => r.ListingKey);

            if (staleKeys.length > 0) {
                console.log(`Pruning ${staleKeys.length} stale listings from sneak_listings...`);
                for (let i = 0; i < staleKeys.length; i += 50) {
                    const chunk = staleKeys.slice(i, i + 50);
                    const placeholders = chunk.map(() => '?').join(',');
                    await env.DB.prepare(`DELETE FROM sneak_listings WHERE ListingKey IN (${placeholders})`).bind(...chunk).run();
                }
            }
        } catch (cleanupErr) {
            console.error("Stale listing cleanup error:", cleanupErr);
        }
    } else if (!syncSuccess) {
        console.warn("Listing sync encountered errors. Stale cleanup skipped to preserve existing data.");
    }

    console.log(`SNEAK Listing Sync Complete. Processed: ${totalProcessed}, Active Keys: ${allFetchedKeys.size}`);
}

/**
 * Generalized Open House Sync (today - 1 day through today + 30 days)
 */
async function syncSneakOpenHouses(env) {
    console.log("Starting SNEAK Open House Sync...");

    const today = new Date();
    const startDate = new Date(today);
    startDate.setDate(today.getDate() - 1);
    const endDate = new Date(today);
    endDate.setDate(today.getDate() + 30);

    const startStr = startDate.toISOString().split('T')[0];
    const endStr = endDate.toISOString().split('T')[0];

    const ohFilter = `OpenHouseStatus eq 'Active' and OpenHouseDate ge ${startStr} and OpenHouseDate le ${endStr}`;
    const ohURL = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(ohFilter)}&$top=200&$orderby=OpenHouseStartTime asc&access_token=${env.BRIDGE_TOKEN}`;

    let ohRec = [];
    let next = ohURL;
    let syncSuccess = true;

    while (next) {
        let res;
        try {
            res = await fetch(next);
        } catch (err) {
            console.error("Open house sync fetch error:", err);
            syncSuccess = false;
            break;
        }

        if (!res.ok) {
            console.error("Open house sync Bridge error:", res.status);
            syncSuccess = false;
            break;
        }

        const d = await res.json();
        ohRec.push(...(d.value || []));
        next = d['@odata.nextLink'] || null;
        if (next && !next.includes('access_token')) {
            next += (next.includes('?') ? '&' : '?') + 'access_token=' + env.BRIDGE_TOKEN;
        }
    }

    if (!syncSuccess || !ohRec.length) {
        console.log(`Open house sync finished. Found: ${ohRec.length} records. (Success: ${syncSuccess})`);
        return;
    }

    const listingKeys = [...new Set(ohRec.map(r => r.ListingKey))];
    let properties = [];
    const PROP_SEL = 'ListingKey,ListingId,UnparsedAddress,City,PostalCode,ListPrice,PropertyType,PropertySubType,BedroomsTotal,BathroomsTotalInteger,LivingArea,LotSizeAcres,YearBuilt,StandardStatus,SubdivisionName,ListAgentFullName,ListAgentEmail,ListAgentDirectPhone,ListAgentKey,ListOfficeName,ListOfficePhone,ListOfficeMlsId,PublicRemarks,Coordinates,Media';

    for (let i = 0; i < listingKeys.length; i += 25) {
        const chunk = listingKeys.slice(i, i + 25);
        const batchFilter = chunk.map(k => `ListingKey eq '${escapeODataString(k)}'`).join(' or ');
        const pURL = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$filter=${encodeURIComponent(`(${batchFilter})`)}&$top=100&$select=${PROP_SEL}&access_token=${env.BRIDGE_TOKEN}`;
        try {
            const pres = await fetch(pURL);
            if (pres.ok) {
                const pd = await pres.json();
                properties.push(...(pd.value || []));
            }
        } catch (propErr) {
            console.warn("Error fetching property batch for open houses:", propErr);
        }
    }

    const propMap = new Map(properties.map(p => [p.ListingKey, p]));

    const statements = ohRec.map(oh => {
        const p = propMap.get(oh.ListingKey) || null;
        const id = 'oh_' + (oh.OpenHouseKey || oh.ListingKey);
        return env.DB.prepare(`
            INSERT OR REPLACE INTO sneak_open_houses (
                id, OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, PropertyData, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        `).bind(
            id, oh.OpenHouseKey || oh.ListingKey, oh.ListingKey, oh.OpenHouseStartTime, oh.OpenHouseEndTime, oh.OpenHouseDate, oh.OpenHouseRemarks || '', JSON.stringify(p)
        );
    });

    for (let i = 0; i < statements.length; i += 50) {
        await env.DB.batch(statements.slice(i, i + 50));
    }

    // Safe Stale Cleanup for sneak_open_houses
    const validOhKeys = new Set(ohRec.map(oh => oh.OpenHouseKey || oh.ListingKey));
    if (syncSuccess && validOhKeys.size > 0) {
        try {
            const existing = await env.DB.prepare("SELECT OpenHouseKey FROM sneak_open_houses").all();
            const staleKeys = (existing.results || []).filter(row => !validOhKeys.has(row.OpenHouseKey)).map(r => r.OpenHouseKey);
            for (let i = 0; i < staleKeys.length; i += 50) {
                const chunk = staleKeys.slice(i, i + 50);
                const placeholders = chunk.map(() => "?").join(",");
                await env.DB.prepare(`DELETE FROM sneak_open_houses WHERE OpenHouseKey IN (${placeholders})`).bind(...chunk).run();
            }
        } catch (cleanupErr) {
            console.error("Open house stale cleanup error:", cleanupErr);
        }
    }

    console.log(`Successfully synced ${ohRec.length} SNEAK open houses.`);
}

/* ==========================================================================
   UTILITY HELPERS
   ========================================================================== */

function escapeODataString(str) {
    if (typeof str !== 'string') return '';
    return str.replace(/'/g, "''");
}

function jsonResponse(data, status = 200, allowedOrigin = null, cacheControl = null) {
    const headers = new Headers();
    headers.set('Content-Type', 'application/json');
    if (allowedOrigin) {
        headers.set('Access-Control-Allow-Origin', allowedOrigin);
        headers.set('Vary', 'Origin');
    }
    headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, X-Site-Key, X-SNEAK-Session');

    if (cacheControl) {
        headers.set('Cache-Control', cacheControl);
    } else {
        headers.set('Cache-Control', 'no-store');
    }

    return new Response(JSON.stringify(data), { status, headers });
}
