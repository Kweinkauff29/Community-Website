/**
 * sneak-consumer/api.js
 * 
 * Consumer API Route Handlers for SNEAK IDX Platform:
 * - Authentication (Magic Link, Verify, Exchange, Me, Logout, Delete Account)
 * - Server Favorites (List, Add, Remove, Merge)
 * 
 * Security Invariants:
 * - Strictly site-scoped data isolation.
 * - Cache-Control: no-store on all authenticated responses.
 * - Strict CORS with Vary: Origin.
 * - Enforces Internet Entire Listing Display & Address Display controls.
 * - Max 200 favorites per consumer per site limit.
 */

import {
    sha256Hex,
    requestConsumerMagicLink,
    verifyAndConsumeConsumerMagicLink,
    exchangeAuthCodeForSession,
    verifyConsumerSession,
    revokeConsumerSession,
    deleteConsumerAccount
} from './auth.js';

const FAVORITES_LIMIT = 200;

export function jsonResponse(data, status = 200, origin = null) {
    const headers = new Headers();
    headers.set('Content-Type', 'application/json');
    headers.set('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
    headers.set('Pragma', 'no-cache');
    
    if (origin) {
        headers.set('Access-Control-Allow-Origin', origin);
        headers.set('Vary', 'Origin');
        headers.set('Access-Control-Allow-Credentials', 'true');
    }
    headers.set('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, X-Site-Key, X-SNEAK-Session');

    return new Response(JSON.stringify(data), { status, headers });
}

function extractBearerToken(req) {
    const authHeader = req.headers.get('Authorization') || '';
    if (authHeader.startsWith('Bearer ')) {
        return authHeader.slice(7).trim();
    }
    return req.headers.get('X-Consumer-Session') || '';
}

function getIpHash(req) {
    const ip = req.headers.get('CF-Connecting-IP') || req.headers.get('X-Forwarded-For') || '127.0.0.1';
    // Sync hash representation
    return ip.split(',')[0].trim();
}

/**
 * POST /api/consumer/auth/magic-link
 */
export async function handleRequestMagicLink(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site, email, returnUrl } = body || {};
    const ip = getIpHash(req);
    const ipHash = await sha256Hex(ip);

    const result = await requestConsumerMagicLink(env.DB, {
        siteKey: site,
        email,
        returnUrl,
        ipHash,
        env
    });

    return jsonResponse(result, 200, origin);
}

/**
 * GET /api/consumer/auth/verify?token=...
 * Consumes magic link and redirects to validated member site with ?auth_code=...
 */
export async function handleVerifyMagicLink(req, url, env) {
    const token = url.searchParams.get('token');
    if (!token) {
        return new Response('Invalid or missing authentication token.', { status: 400 });
    }

    const result = await verifyAndConsumeConsumerMagicLink(env.DB, token);
    if (!result) {
        return new Response(`
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Link Expired</title><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="background:#0b1329;color:#f8fafc;font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;">
<div style="background:#111e38;padding:32px;border-radius:12px;border:1px solid rgba(255,255,255,0.1);max-width:400px;text-align:center;">
<h2 style="color:#f87171;margin-top:0;">Sign-In Link Expired</h2>
<p style="color:#94a3b8;font-size:14px;line-height:1.5;">This sign-in link is invalid or has already been used. Please return to the property search and request a new link.</p>
</div>
</body>
</html>
        `, {
            status: 401,
            headers: { 'Content-Type': 'text/html; charset=utf-8' }
        });
    }

    // Build return redirect URL with short-lived auth exchange code
    const returnUrl = new URL(result.returnUrl);
    returnUrl.searchParams.set('auth_code', result.exchangeCode);

    return Response.redirect(returnUrl.toString(), 302);
}

/**
 * POST /api/consumer/auth/exchange
 */
export async function handleExchangeAuthCode(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site, code, session } = body || {};
    const signingSecret = env.SNEAK_SIGNING_SECRET || '';

    const exchangeResult = await exchangeAuthCodeForSession(env.DB, {
        code,
        siteKey: site,
        sessionToken: session,
        signingSecret
    });

    if (!exchangeResult) {
        return jsonResponse({
            error: 'InvalidExchangeCode',
            message: 'Authentication exchange code is invalid, expired, or already consumed.'
        }, 401, origin);
    }

    return jsonResponse({
        success: true,
        consumerSession: exchangeResult.consumerSession,
        user: exchangeResult.user
    }, 200, origin);
}

function sessionErrorResponse(session, origin) {
    if (session?.error === 'SiteMismatch') {
        return jsonResponse({ error: 'TenantSiteMismatch', message: 'Consumer session does not match requested site.' }, 403, origin);
    }
    return jsonResponse({ error: 'Unauthorized', message: 'Valid consumer session is required.' }, 401, origin);
}

/**
 * GET /api/consumer/auth/me
 */
export async function handleGetMe(req, url, env, origin) {
    const rawToken = extractBearerToken(req);
    const siteKey = url.searchParams.get('site');

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    return jsonResponse({
        success: true,
        user: {
            id: session.userId,
            email: session.email,
            siteKey: session.siteKey
        }
    }, 200, origin);
}

/**
 * POST /api/consumer/auth/logout
 */
export async function handleLogout(req, env, origin) {
    const rawToken = extractBearerToken(req);
    if (rawToken) {
        await revokeConsumerSession(env.DB, rawToken);
    }
    return jsonResponse({ success: true, message: 'Logged out successfully.' }, 200, origin);
}

/**
 * DELETE /api/consumer/account
 */
export async function handleDeleteAccount(req, url, env, origin) {
    const rawToken = extractBearerToken(req);
    const siteKey = url.searchParams.get('site');

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    await deleteConsumerAccount(env.DB, session.userId, session.siteId);
    return jsonResponse({ success: true, message: 'Your account and saved homes have been deleted.' }, 200, origin);
}

/**
 * Helper to get site tenant scope and verify listing eligibility
 */
async function resolveSiteScope(db, siteKey) {
    return await db.prepare(`
        SELECT id AS site_id, site_key, scope_type, scope_value, status
        FROM sneak_sites
        WHERE site_key = ? AND status = 'active'
    `).bind(siteKey).first();
}

/**
 * GET /api/consumer/favorites?site=...
 */
export async function handleListFavorites(req, url, env, origin) {
    const rawToken = extractBearerToken(req);
    const siteKey = url.searchParams.get('site');

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    // 1. Fetch user's saved listing keys
    const favRows = await env.DB.prepare(`
        SELECT listing_key, created_at
        FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ?
        ORDER BY created_at DESC
        LIMIT 200
    `).bind(session.userId, session.siteId).all();

    const keys = (favRows.results || []).map(r => r.listing_key);
    if (!keys.length) {
        return jsonResponse({ success: true, count: 0, favorites: [] }, 200, origin);
    }

    // 2. Fetch current listings from D1 within tenant scope and Internet display eligibility
    // Never bypass InternetEntireListingDisplayYN = 1 or active tenant scope
    let scopeClause = '';
    const bindings = [];

    if (site.scope_type === 'agent' && site.scope_value) {
        scopeClause = ' AND (ListAgentMlsId = ? OR ListAgentKey = ?)';
        bindings.push(site.scope_value, site.scope_value);
    } else if (site.scope_type === 'office' && site.scope_value) {
        scopeClause = ' AND (ListOfficeMlsId = ? OR ListOfficeKey = ?)';
        bindings.push(site.scope_value, site.scope_value);
    }

    // Batch lookup in chunks of 50
    const listingMap = new Map();
    for (let i = 0; i < keys.length; i += 50) {
        const chunk = keys.slice(i, i + 50);
        const placeholders = chunk.map(() => '?').join(',');
        const query = `
            SELECT 
                ListingKey, ListingId, ListPrice, StandardStatus, PropertyType, PropertySubType,
                BedroomsTotal, BathroomsTotalInteger, LivingArea, LotSizeAcres, YearBuilt,
                City, PostalCode, UnparsedAddress, StreetNumber, StreetName, UnitNumber,
                PrimaryPhoto, MediaJSON, InternetEntireListingDisplayYN, InternetAddressDisplayYN,
                ListOfficeName, WaterfrontYN, PoolPrivateYN, SubdivisionName
            FROM sneak_listings
            WHERE ListingKey IN (${placeholders})
              AND (InternetEntireListingDisplayYN = 1 OR InternetEntireListingDisplayYN IS NULL)
              ${scopeClause}
        `;
        const res = await env.DB.prepare(query).bind(...chunk, ...bindings).all();
        for (const row of res.results || []) {
            listingMap.set(row.ListingKey, row);
        }
    }

    // 3. Assemble response preserving user save order and enforcing address suppression
    const favorites = (favRows.results || []).map(f => {
        const listing = listingMap.get(f.listing_key);
        if (!listing) {
            return {
                listingKey: f.listing_key,
                createdAt: f.created_at,
                unavailable: true
            };
        }

        const isAddressSuppressed = (listing.InternetAddressDisplayYN === 0);
        return {
            listingKey: listing.ListingKey,
            createdAt: f.created_at,
            unavailable: false,
            listing: {
                ListingKey: listing.ListingKey,
                ListingId: listing.ListingId,
                ListPrice: listing.ListPrice,
                StandardStatus: listing.StandardStatus,
                PropertyType: listing.PropertyType,
                PropertySubType: listing.PropertySubType,
                BedroomsTotal: listing.BedroomsTotal,
                BathroomsTotalInteger: listing.BathroomsTotalInteger,
                LivingArea: listing.LivingArea,
                LotSizeAcres: listing.LotSizeAcres,
                YearBuilt: listing.YearBuilt,
                City: listing.City,
                PostalCode: listing.PostalCode,
                UnparsedAddress: isAddressSuppressed ? 'Address Undisclosed' : listing.UnparsedAddress,
                PrimaryPhoto: listing.PrimaryPhoto,
                ListOfficeName: listing.ListOfficeName,
                WaterfrontYN: listing.WaterfrontYN,
                PoolPrivateYN: listing.PoolPrivateYN,
                SubdivisionName: listing.SubdivisionName
            }
        };
    });

    return jsonResponse({
        success: true,
        count: favorites.length,
        favorites
    }, 200, origin);
}

/**
 * POST /api/consumer/favorites
 */
export async function handleAddFavorite(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site: siteKey, listingKey } = body || {};
    if (!listingKey || typeof listingKey !== 'string') {
        return jsonResponse({ error: 'MissingListingKey', message: 'listingKey is required.' }, 400, origin);
    }

    const rawToken = extractBearerToken(req);
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    // 1. Check current favorite count limit
    const countRow = await env.DB.prepare(`
        SELECT count(*) as count FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ?
    `).bind(session.userId, session.siteId).first();

    if ((countRow?.count || 0) >= FAVORITES_LIMIT) {
        return jsonResponse({
            error: 'FavoriteLimitExceeded',
            message: `You have reached the maximum limit of ${FAVORITES_LIMIT} saved homes.`
        }, 400, origin);
    }

    // 2. Validate listing exists in tenant scope and is Internet display eligible
    let scopeClause = '';
    const bindings = [listingKey];

    if (site.scope_type === 'agent' && site.scope_value) {
        scopeClause = ' AND (ListAgentMlsId = ? OR ListAgentKey = ?)';
        bindings.push(site.scope_value, site.scope_value);
    } else if (site.scope_type === 'office' && site.scope_value) {
        scopeClause = ' AND (ListOfficeMlsId = ? OR ListOfficeKey = ?)';
        bindings.push(site.scope_value, site.scope_value);
    }

    const listing = await env.DB.prepare(`
        SELECT ListingKey FROM sneak_listings
        WHERE ListingKey = ?
          AND (InternetEntireListingDisplayYN = 1 OR InternetEntireListingDisplayYN IS NULL)
          ${scopeClause}
    `).bind(...bindings).first();

    if (!listing) {
        return jsonResponse({ error: 'ListingNotFound', message: 'Listing is not available or outside site scope.' }, 404, origin);
    }

    // 3. Insert favorite (idempotent via INSERT OR IGNORE)
    const favId = `cfav_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    await env.DB.prepare(`
        INSERT OR IGNORE INTO sneak_consumer_favorites (id, site_id, user_id, listing_key, created_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    `).bind(favId, session.siteId, session.userId, listingKey).run();

    const updatedCount = await env.DB.prepare(`
        SELECT count(*) as count FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ?
    `).bind(session.userId, session.siteId).first();

    return jsonResponse({
        success: true,
        listingKey,
        count: updatedCount?.count || 1
    }, 200, origin);
}

/**
 * DELETE /api/consumer/favorites/:listingKey?site=...
 */
export async function handleRemoveFavorite(req, listingKey, url, env, origin) {
    if (!listingKey) {
        return jsonResponse({ error: 'MissingListingKey', message: 'listingKey is required.' }, 400, origin);
    }

    const siteKey = url.searchParams.get('site');
    const rawToken = extractBearerToken(req);

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    await env.DB.prepare(`
        DELETE FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ? AND listing_key = ?
    `).bind(session.userId, session.siteId, listingKey).run();

    const updatedCount = await env.DB.prepare(`
        SELECT count(*) as count FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ?
    `).bind(session.userId, session.siteId).first();

    return jsonResponse({
        success: true,
        listingKey,
        count: updatedCount?.count || 0
    }, 200, origin);
}

/**
 * POST /api/consumer/favorites/merge
 * Merges local anonymous favorites into canonical server favorites.
 */
export async function handleMergeFavorites(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site: siteKey, listingKeys } = body || {};
    if (!Array.isArray(listingKeys)) {
        return jsonResponse({ error: 'InvalidListingKeys', message: 'listingKeys must be an array.' }, 400, origin);
    }

    const rawToken = extractBearerToken(req);
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    // Clean & unique keys
    const cleanKeys = [...new Set(listingKeys.filter(k => typeof k === 'string' && k.trim().length > 0))].slice(0, FAVORITES_LIMIT);

    // Fetch existing favorites
    const existing = await env.DB.prepare(`
        SELECT listing_key FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ?
    `).bind(session.userId, session.siteId).all();

    const existingKeys = new Set((existing.results || []).map(r => r.listing_key));
    const keysToAdd = cleanKeys.filter(k => !existingKeys.has(k));

    // Batch insert new keys respecting limit
    const remainingSlots = Math.max(0, FAVORITES_LIMIT - existingKeys.size);
    const toInsert = keysToAdd.slice(0, remainingSlots);

    for (const key of toInsert) {
        const favId = `cfav_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await env.DB.prepare(`
            INSERT OR IGNORE INTO sneak_consumer_favorites (id, site_id, user_id, listing_key, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        `).bind(favId, session.siteId, session.userId, key).run();
    }

    const finalCount = await env.DB.prepare(`
        SELECT count(*) as count FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ?
    `).bind(session.userId, session.siteId).first();

    return jsonResponse({
        success: true,
        count: finalCount?.count || 0,
        merged: toInsert.length
    }, 200, origin);
}
