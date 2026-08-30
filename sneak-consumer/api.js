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
    deleteConsumerAccount,
    validateReturnUrl
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
    headers.set('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, X-Site-Key, X-SNEAK-Session, X-Consumer-Session');

    return new Response(JSON.stringify(data), { status, headers });
}

const SAVED_SEARCHES_LIMIT = 25;
const MAX_STATE_BYTES = 16384; // 16 KB

/**
 * Normalizes and validates serialized search state.
 */
export function normalizeSearchState(rawState) {
    if (!rawState || typeof rawState !== 'object') return null;

    const version = Number(rawState.version) || 1;
    if (version !== 1) return null; // Reject unsupported versions

    const search = rawState.search || rawState;
    if (!search || typeof search !== 'object') return null;

    const normalized = {
        propertyType: ['sale', 'rental', 'commercial', 'land'].includes(search.propertyType) ? search.propertyType : 'sale',
        q: (typeof search.q === 'string' && search.q.trim().length <= 150) ? search.q.trim() : null,
        minPrice: (typeof search.minPrice === 'number' && search.minPrice > 0) ? search.minPrice : null,
        maxPrice: (typeof search.maxPrice === 'number' && search.maxPrice > 0) ? search.maxPrice : null,
        beds: (typeof search.beds === 'number' || (typeof search.beds === 'string' && !isNaN(Number(search.beds)))) ? Number(search.beds) : null,
        baths: (typeof search.baths === 'number' || (typeof search.baths === 'string' && !isNaN(Number(search.baths)))) ? Number(search.baths) : null,
        propertySubType: Array.isArray(search.propertySubType) 
            ? search.propertySubType.filter(s => typeof s === 'string' && s.length <= 60).slice(0, 20)
            : (typeof search.propertySubType === 'string' && search.propertySubType ? search.propertySubType.split(',').map(s => s.trim()).filter(Boolean).slice(0, 20) : []),
        sort: ['dateDesc', 'priceDesc', 'priceAsc', 'sqftDesc', 'yearDesc', 'acresDesc'].includes(search.sort) ? search.sort : 'dateDesc',
        drawerState: {},
        spatialState: { mode: null }
    };

    // Normalize Drawer State
    if (search.drawerState && typeof search.drawerState === 'object') {
        const d = search.drawerState;
        normalized.drawerState = {
            subtypes: Array.isArray(d.subtypes) ? d.subtypes.filter(s => typeof s === 'string').slice(0, 20) : [],
            minSqft: (typeof d.minSqft === 'number' && d.minSqft > 0) ? d.minSqft : null,
            maxSqft: (typeof d.maxSqft === 'number' && d.maxSqft > 0) ? d.maxSqft : null,
            minAcres: (typeof d.minAcres === 'number' && d.minAcres > 0) ? d.minAcres : null,
            maxAcres: (typeof d.maxAcres === 'number' && d.maxAcres > 0) ? d.maxAcres : null,
            minYear: (typeof d.minYear === 'number' && d.minYear > 1800) ? d.minYear : null,
            maxYear: (typeof d.maxYear === 'number' && d.maxYear > 1800) ? d.maxYear : null,
            county: (typeof d.county === 'string' && d.county.length <= 50) ? d.county.trim() : '',
            zip: (typeof d.zip === 'string' && d.zip.length <= 10) ? d.zip.trim() : '',
            subdivision: (typeof d.subdivision === 'string' && d.subdivision.length <= 80) ? d.subdivision.trim() : '',
            cities: Array.isArray(d.cities) ? d.cities.filter(c => typeof c === 'string').slice(0, 20) : [],
            waterfront: Boolean(d.waterfront),
            pool: Boolean(d.pool),
            garage: (typeof d.garage === 'number' && d.garage >= 0) ? d.garage : 0,
            newConstruction: Boolean(d.newConstruction),
            openHouse: Boolean(d.openHouse),
            newListing: Boolean(d.newListing),
            priceReduced: Boolean(d.priceReduced)
        };
    }

    // Normalize Spatial State
    if (search.spatialState && typeof search.spatialState === 'object') {
        const sp = search.spatialState;
        if (sp.mode === 'radius' && typeof sp.centerLat === 'number' && typeof sp.centerLng === 'number') {
            normalized.spatialState = {
                mode: 'radius',
                centerLat: Number(sp.centerLat.toFixed(6)),
                centerLng: Number(sp.centerLng.toFixed(6)),
                radiusMiles: (typeof sp.radiusMiles === 'number' && sp.radiusMiles > 0 && sp.radiusMiles <= 100) ? sp.radiusMiles : 5
            };
        } else if (sp.mode === 'polygon' && sp.polygon && Array.isArray(sp.polygon.coordinates) && Array.isArray(sp.polygon.coordinates[0])) {
            const rawRing = sp.polygon.coordinates[0];
            if (rawRing.length >= 4 && rawRing.length <= 40) {
                const cleanCoords = rawRing.map(pt => [Number(Number(pt[0]).toFixed(6)), Number(Number(pt[1]).toFixed(6))]);
                normalized.spatialState = {
                    mode: 'polygon',
                    polygon: {
                        type: 'Polygon',
                        coordinates: [cleanCoords]
                    }
                };
            }
        } else if (sp.mode === 'viewport' && sp.viewportBounds) {
            normalized.spatialState = {
                mode: 'viewport',
                viewportBounds: {
                    north: Number(Number(sp.viewportBounds.north).toFixed(6)),
                    south: Number(Number(sp.viewportBounds.south).toFixed(6)),
                    east: Number(Number(sp.viewportBounds.east).toFixed(6)),
                    west: Number(Number(sp.viewportBounds.west).toFixed(6))
                }
            };
        }
    }

    return {
        version: 1,
        search: normalized
    };
}

/**
 * GET /api/consumer/saved-searches?site=...
 */
export async function handleListSavedSearches(req, url, env, origin) {
    const rawToken = extractBearerToken(req);
    const siteKey = url.searchParams.get('site');

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const rows = await env.DB.prepare(`
        SELECT 
            s.id, s.name, s.state_version, s.state_json, s.created_at, s.updated_at,
            a.id AS alert_id, a.frequency, a.enabled, a.enabled_at, a.timezone, a.return_url
        FROM sneak_consumer_saved_searches s
        LEFT JOIN sneak_consumer_search_alerts a ON s.id = a.saved_search_id
        WHERE s.user_id = ? AND s.site_id = ?
        ORDER BY s.updated_at DESC
        LIMIT ${SAVED_SEARCHES_LIMIT}
    `).bind(session.userId, session.siteId).all();

    const savedSearches = (rows.results || []).map(r => {
        let state = null;
        try {
            state = JSON.parse(r.state_json);
        } catch {}
        return {
            id: r.id,
            name: r.name,
            stateVersion: r.state_version,
            state: state,
            alert: {
                id: r.alert_id || null,
                frequency: r.frequency || 'off',
                enabled: Boolean(r.enabled),
                enabledAt: r.enabled_at || null,
                timezone: r.timezone || 'America/New_York',
                returnUrl: r.return_url || null
            },
            createdAt: r.created_at,
            updatedAt: r.updated_at
        };
    });

    return jsonResponse({
        success: true,
        count: savedSearches.length,
        savedSearches
    }, 200, origin);
}

/**
 * POST /api/consumer/saved-searches
 */
export async function handleCreateSavedSearch(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site: siteKey, name, state } = body || {};
    if (!name || typeof name !== 'string' || !name.trim()) {
        return jsonResponse({ error: 'MissingName', message: 'Saved search name is required.' }, 400, origin);
    }

    const cleanName = name.trim().slice(0, 80);

    const normalizedState = normalizeSearchState(state);
    if (!normalizedState) {
        return jsonResponse({ error: 'InvalidSearchState', message: 'Invalid or unsupported search state provided.' }, 400, origin);
    }

    const stateJson = JSON.stringify(normalizedState);
    if (stateJson.length > MAX_STATE_BYTES) {
        return jsonResponse({ error: 'PayloadTooLarge', message: 'Search state exceeds maximum allowed size (16 KB).' }, 400, origin);
    }

    const rawToken = extractBearerToken(req);
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const stateHash = await sha256Hex(stateJson);

    // Check if exact same search state already exists for this consumer/site
    const existing = await env.DB.prepare(`
        SELECT id, name, state_version, state_json, created_at, updated_at
        FROM sneak_consumer_saved_searches
        WHERE user_id = ? AND site_id = ? AND state_hash = ?
    `).bind(session.userId, session.siteId, stateHash).first();

    if (existing) {
        // Update name if changed and bump updated_at
        await env.DB.prepare(`
            UPDATE sneak_consumer_saved_searches
            SET name = ?, updated_at = datetime('now')
            WHERE id = ?
        `).bind(cleanName, existing.id).run();

        return jsonResponse({
            success: true,
            savedSearch: {
                id: existing.id,
                name: cleanName,
                stateVersion: existing.state_version,
                state: normalizedState,
                createdAt: existing.created_at,
                updatedAt: new Date().toISOString()
            }
        }, 200, origin);
    }

    // Check count limit
    const countRow = await env.DB.prepare(`
        SELECT count(*) as count FROM sneak_consumer_saved_searches
        WHERE user_id = ? AND site_id = ?
    `).bind(session.userId, session.siteId).first();

    if ((countRow?.count || 0) >= SAVED_SEARCHES_LIMIT) {
        return jsonResponse({
            error: 'SavedSearchLimitExceeded',
            message: `You have reached the maximum limit of ${SAVED_SEARCHES_LIMIT} saved searches.`
        }, 400, origin);
    }

    const searchId = `css_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const now = new Date().toISOString();

    await env.DB.prepare(`
        INSERT INTO sneak_consumer_saved_searches (id, site_id, user_id, name, state_version, state_json, state_hash, created_at, updated_at)
        VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)
    `).bind(searchId, session.siteId, session.userId, cleanName, stateJson, stateHash, now, now).run();

    return jsonResponse({
        success: true,
        savedSearch: {
            id: searchId,
            name: cleanName,
            stateVersion: 1,
            state: normalizedState,
            createdAt: now,
            updatedAt: now
        }
    }, 200, origin);
}

/**
 * PATCH /api/consumer/saved-searches/:id?site=...
 */
export async function handleUpdateSavedSearch(req, searchId, url, env, origin) {
    if (!searchId) {
        return jsonResponse({ error: 'MissingSearchId', message: 'searchId is required.' }, 400, origin);
    }

    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { name } = body || {};
    if (!name || typeof name !== 'string' || !name.trim()) {
        return jsonResponse({ error: 'MissingName', message: 'Saved search name is required.' }, 400, origin);
    }

    const cleanName = name.trim().slice(0, 80);
    const siteKey = url.searchParams.get('site');
    const rawToken = extractBearerToken(req);

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const updateRes = await env.DB.prepare(`
        UPDATE sneak_consumer_saved_searches
        SET name = ?, updated_at = datetime('now')
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(cleanName, searchId, session.userId, session.siteId).run();

    const changes = updateRes?.meta?.changes ?? updateRes?.changes ?? 0;
    if (changes === 0) {
        return jsonResponse({ error: 'NotFound', message: 'Saved search not found or unauthorized.' }, 404, origin);
    }

    return jsonResponse({
        success: true,
        id: searchId,
        name: cleanName
    }, 200, origin);
}

/**
 * DELETE /api/consumer/saved-searches/:id?site=...
 */
export async function handleDeleteSavedSearch(req, searchId, url, env, origin) {
    if (!searchId) {
        return jsonResponse({ error: 'MissingSearchId', message: 'searchId is required.' }, 400, origin);
    }

    const siteKey = url.searchParams.get('site');
    const rawToken = extractBearerToken(req);

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const delRes = await env.DB.prepare(`
        DELETE FROM sneak_consumer_saved_searches
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).run();

    const changes = delRes?.meta?.changes ?? delRes?.changes ?? 0;
    if (changes === 0) {
        return jsonResponse({ error: 'NotFound', message: 'Saved search not found or unauthorized.' }, 404, origin);
    }

    return jsonResponse({
        success: true,
        id: searchId,
        message: 'Saved search deleted successfully.'
    }, 200, origin);
}

/**
 * Sanitizes return URL for alert preferences, stripping sensitive auth tokens.
 */
export async function sanitizeAlertReturnUrl(db, siteId, returnUrlStr, isDev = false) {
    const validated = await validateReturnUrl(db, siteId, returnUrlStr, isDev);
    if (!validated) return null;
    try {
        const u = new URL(validated);
        u.searchParams.delete('auth_code');
        u.searchParams.delete('session');
        u.searchParams.delete('token');
        u.searchParams.delete('bearer');
        u.searchParams.delete('csess');
        return u.toString();
    } catch {
        return validated;
    }
}

/**
 * GET /api/consumer/saved-searches/:id/alert?site=...
 */
export async function handleGetSavedSearchAlert(req, searchId, url, env, origin) {
    if (!searchId) {
        return jsonResponse({ error: 'MissingSearchId', message: 'searchId is required.' }, 400, origin);
    }
    const siteKey = url.searchParams.get('site');
    const rawToken = extractBearerToken(req);

    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    // Verify saved search exists and belongs to user
    const search = await env.DB.prepare(`
        SELECT id FROM sneak_consumer_saved_searches
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).first();

    if (!search) {
        return jsonResponse({ error: 'NotFound', message: 'Saved search not found or unauthorized.' }, 404, origin);
    }

    const alert = await env.DB.prepare(`
        SELECT id, saved_search_id, frequency, enabled, enabled_at, timezone, return_url, last_checked_at, last_sent_at
        FROM sneak_consumer_search_alerts
        WHERE saved_search_id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).first();

    return jsonResponse({
        success: true,
        alert: alert ? {
            id: alert.id,
            savedSearchId: alert.saved_search_id,
            frequency: alert.frequency,
            enabled: Boolean(alert.enabled),
            enabledAt: alert.enabled_at,
            timezone: alert.timezone,
            returnUrl: alert.return_url,
            lastCheckedAt: alert.last_checked_at,
            lastSentAt: alert.last_sent_at
        } : {
            savedSearchId: searchId,
            frequency: 'off',
            enabled: false,
            enabledAt: null,
            timezone: 'America/New_York',
            returnUrl: null
        }
    }, 200, origin);
}

/**
 * PUT or POST /api/consumer/saved-searches/:id/alert
 */
export async function handleUpdateSavedSearchAlert(req, searchId, url, env, origin) {
    if (!searchId) {
        return jsonResponse({ error: 'MissingSearchId', message: 'searchId is required.' }, 400, origin);
    }

    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site: siteKey, frequency, timezone = 'America/New_York', returnUrl } = body || {};
    const siteKeyParam = siteKey || url.searchParams.get('site');
    const rawToken = extractBearerToken(req);

    const session = await verifyConsumerSession(env.DB, rawToken, siteKeyParam);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    const validFrequencies = ['off', 'asap', 'daily'];
    const cleanFreq = (frequency || '').toLowerCase().trim();
    if (!validFrequencies.includes(cleanFreq)) {
        return jsonResponse({
            error: 'InvalidFrequency',
            message: "Frequency must be one of: 'off', 'asap', 'daily'."
        }, 400, origin);
    }

    // Verify saved search exists and belongs to user
    const search = await env.DB.prepare(`
        SELECT id, name FROM sneak_consumer_saved_searches
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).first();

    if (!search) {
        return jsonResponse({ error: 'NotFound', message: 'Saved search not found or unauthorized.' }, 404, origin);
    }

    // Validate returnUrl
    const isDev = (env?.SNEAK_ENV || 'staging') === 'development';
    let cleanReturnUrl = null;
    if (returnUrl) {
        cleanReturnUrl = await sanitizeAlertReturnUrl(env.DB, session.siteId, returnUrl, isDev);
        if (!cleanReturnUrl && cleanFreq !== 'off') {
            return jsonResponse({
                error: 'InvalidReturnUrl',
                message: 'returnUrl must be a valid HTTPS URL matching an authorized site domain.'
            }, 400, origin);
        }
    }

    const isEnabled = (cleanFreq !== 'off') ? 1 : 0;
    const nowIso = new Date().toISOString();
    const alertId = `calert_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;

    // Upsert into sneak_consumer_search_alerts
    await env.DB.prepare(`
        INSERT INTO sneak_consumer_search_alerts
        (id, saved_search_id, site_id, user_id, frequency, enabled, enabled_at, timezone, return_url, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CASE WHEN ? = 1 THEN ? ELSE NULL END, ?, ?, ?, ?)
        ON CONFLICT(saved_search_id) DO UPDATE SET
            frequency = excluded.frequency,
            enabled = excluded.enabled,
            enabled_at = CASE 
                WHEN excluded.enabled = 1 AND sneak_consumer_search_alerts.enabled = 0 THEN excluded.enabled_at
                WHEN excluded.enabled = 1 AND sneak_consumer_search_alerts.enabled = 1 THEN sneak_consumer_search_alerts.enabled_at
                ELSE NULL 
            END,
            timezone = excluded.timezone,
            return_url = COALESCE(excluded.return_url, sneak_consumer_search_alerts.return_url),
            updated_at = excluded.updated_at
    `).bind(
        alertId, searchId, session.siteId, session.userId, cleanFreq, isEnabled, isEnabled, nowIso,
        timezone, cleanReturnUrl, nowIso, nowIso
    ).run();

    const updated = await env.DB.prepare(`
        SELECT id, saved_search_id, frequency, enabled, enabled_at, timezone, return_url, last_checked_at, last_sent_at
        FROM sneak_consumer_search_alerts
        WHERE saved_search_id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).first();

    return jsonResponse({
        success: true,
        alert: {
            id: updated.id,
            savedSearchId: updated.saved_search_id,
            frequency: updated.frequency,
            enabled: Boolean(updated.enabled),
            enabledAt: updated.enabled_at,
            timezone: updated.timezone,
            returnUrl: updated.return_url,
            lastCheckedAt: updated.last_checked_at,
            lastSentAt: updated.last_sent_at
        }
    }, 200, origin);
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
