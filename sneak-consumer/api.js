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
const COMPARE_LIMIT = 4;
const RECENTLY_VIEWED_LIMIT = 20;
const RECENTLY_VIEWED_CANDIDATE_LIMIT = 40;
const SHARED_LISTS_LIMIT = 10;
const SHARED_LIST_ITEMS_LIMIT = 25;
const SHARED_LIST_NAME_LIMIT = 80;

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

export const ALLOWED_ACTIVITY_TYPES = [
    'listing_view',
    'favorite_added',
    'favorite_removed',
    'saved_search_created',
    'saved_search_updated',
    'saved_search_deleted',
    'alert_enabled',
    'alert_frequency_changed',
    'alert_disabled',
    'inquiry_submitted'
];

const ACTIVITY_METADATA_MAX_BYTES = 2048;
const ALERT_FREQUENCIES = new Set(['asap', 'daily', 'off']);

/**
 * Reduces activity metadata to the fields explicitly allowed for each event.
 * Unknown keys are discarded before serialization so the activity ledger never
 * becomes an accidental storage channel for arbitrary browser or request data.
 */
export function sanitizeActivityMetadata(eventType, metadata) {
    if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) return null;

    let sanitized = null;
    if (eventType === 'saved_search_created' || eventType === 'saved_search_updated' || eventType === 'saved_search_deleted') {
        if (typeof metadata.name === 'string' && metadata.name.trim()) {
            sanitized = { name: metadata.name.trim().slice(0, 80) };
        }
    } else if (eventType === 'alert_enabled' || eventType === 'alert_frequency_changed') {
        if (typeof metadata.frequency === 'string' && ALERT_FREQUENCIES.has(metadata.frequency)) {
            sanitized = { frequency: metadata.frequency };
        }
    }

    if (!sanitized) return null;

    const serialized = JSON.stringify(sanitized);
    if (new TextEncoder().encode(serialized).byteLength > ACTIVITY_METADATA_MAX_BYTES) return null;
    return sanitized;
}

/**
 * Logs an authenticated buyer activity event into sneak_consumer_activity_events
 * and updates sneak_consumer_users.last_activity_at.
 * 
 * Invariants:
 * - Fails safely without disrupting the primary business mutation.
 * - Max 2 KB metadata JSON payload.
 * - Deduplication support via dedupe_key.
 */
export async function recordConsumerActivity(db, {
    siteId,
    userId,
    eventType,
    listingKey = null,
    savedSearchId = null,
    leadId = null,
    metadata = null,
    dedupeKey = null,
    now = new Date()
}) {
    if (!siteId || !userId || !eventType || !ALLOWED_ACTIVITY_TYPES.includes(eventType)) {
        return { success: false, error: 'InvalidActivityEvent' };
    }

    const eventId = `cact_${crypto.randomUUID()}`;
    const nowIso = (now instanceof Date ? now : new Date(now)).toISOString();
    const sanitizedMetadata = sanitizeActivityMetadata(eventType, metadata);
    const metadataJson = sanitizedMetadata ? JSON.stringify(sanitizedMetadata) : null;

    try {
        await db.prepare(`
            INSERT OR IGNORE INTO sneak_consumer_activity_events
            (id, site_id, user_id, event_type, listing_key, saved_search_id, lead_id, metadata_json, dedupe_key, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).bind(
            eventId, siteId, userId, eventType,
            listingKey || null, savedSearchId || null, leadId || null,
            metadataJson, dedupeKey || null, nowIso
        ).run();

        // Bump last_activity_at on sneak_consumer_users
        await db.prepare(`
            UPDATE sneak_consumer_users
            SET last_activity_at = ?, updated_at = ?
            WHERE id = ?
        `).bind(nowIso, nowIso, userId).run();

        return { success: true, eventId };
    } catch (err) {
        console.warn('[CONSUMER ACTIVITY RECORD ERROR]', err.message);
        return { success: false, error: err.message };
    }
}

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

    // Server Activity Log: saved_search_created
    await recordConsumerActivity(env.DB, {
        siteId: session.siteId,
        userId: session.userId,
        eventType: 'saved_search_created',
        savedSearchId: searchId,
        metadata: { name: cleanName }
    });

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

    // Server Activity Log: saved_search_updated
    await recordConsumerActivity(env.DB, {
        siteId: session.siteId,
        userId: session.userId,
        eventType: 'saved_search_updated',
        savedSearchId: searchId,
        metadata: { name: cleanName }
    });

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

    // Fetch existing search name before deletion for historical timeline label
    const existingSearch = await env.DB.prepare(`
        SELECT name FROM sneak_consumer_saved_searches
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).first();

    const delRes = await env.DB.prepare(`
        DELETE FROM sneak_consumer_saved_searches
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).run();

    const changes = delRes?.meta?.changes ?? delRes?.changes ?? 0;
    if (changes === 0) {
        return jsonResponse({ error: 'NotFound', message: 'Saved search not found or unauthorized.' }, 404, origin);
    }

    // Server Activity Log: saved_search_deleted
    await recordConsumerActivity(env.DB, {
        siteId: session.siteId,
        userId: session.userId,
        eventType: 'saved_search_deleted',
        savedSearchId: searchId,
        metadata: { name: existingSearch?.name || 'Saved Search' }
    });

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

    // Check previous alert frequency before upsert to determine meaningful change
    const oldAlert = await env.DB.prepare(`
        SELECT frequency, enabled FROM sneak_consumer_search_alerts
        WHERE saved_search_id = ? AND user_id = ? AND site_id = ?
    `).bind(searchId, session.userId, session.siteId).first();
    const oldFreq = oldAlert ? (oldAlert.enabled ? oldAlert.frequency : 'off') : 'off';

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

    // Server Activity Log for Alert Preference Transition
    const newFreq = cleanFreq;
    if (oldFreq === 'off' && (newFreq === 'asap' || newFreq === 'daily')) {
        await recordConsumerActivity(env.DB, {
            siteId: session.siteId,
            userId: session.userId,
            eventType: 'alert_enabled',
            savedSearchId: searchId,
            metadata: { frequency: newFreq }
        });
    } else if ((oldFreq === 'asap' || oldFreq === 'daily') && (newFreq === 'asap' || newFreq === 'daily') && oldFreq !== newFreq) {
        await recordConsumerActivity(env.DB, {
            siteId: session.siteId,
            userId: session.userId,
            eventType: 'alert_frequency_changed',
            savedSearchId: searchId,
            metadata: { frequency: newFreq }
        });
    } else if ((oldFreq === 'asap' || oldFreq === 'daily') && newFreq === 'off') {
        await recordConsumerActivity(env.DB, {
            siteId: session.siteId,
            userId: session.userId,
            eventType: 'alert_disabled',
            savedSearchId: searchId
        });
    }

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

function buildCurrentListingScope(site) {
    if (!site) return { valid: false, clause: '1=0', bindings: [] };
    if (site.scope_type === 'market') return { valid: true, clause: '1=1', bindings: [] };
    if (site.scope_type === 'agent' && site.scope_value) {
        return {
            valid: true,
            clause: '(ListAgentMlsId = ? OR ListAgentKey = ?)',
            bindings: [site.scope_value, site.scope_value]
        };
    }
    if (site.scope_type === 'office' && site.scope_value) {
        return {
            valid: true,
            clause: '(ListOfficeMlsId = ? OR ListOfficeKey = ?)',
            bindings: [site.scope_value, site.scope_value]
        };
    }
    return { valid: false, clause: '1=0', bindings: [] };
}

function toListingSummary(listing) {
    const addressAllowed = listing.InternetAddressDisplayYN === 1;
    return {
        ListingKey: listing.ListingKey,
        ListingId: listing.ListingId,
        ListPrice: listing.ListPrice,
        OriginalListPrice: listing.OriginalListPrice,
        StandardStatus: listing.StandardStatus,
        PropertyType: listing.PropertyType,
        PropertySubType: listing.PropertySubType,
        BedroomsTotal: listing.BedroomsTotal,
        BathroomsTotalInteger: listing.BathroomsTotalInteger,
        LivingArea: listing.LivingArea,
        LotSizeAcres: listing.LotSizeAcres,
        YearBuilt: listing.YearBuilt,
        City: listing.City,
        StateOrProvince: listing.StateOrProvince,
        PostalCode: listing.PostalCode,
        CountyOrParish: listing.CountyOrParish,
        UnparsedAddress: addressAllowed ? listing.UnparsedAddress : 'Address Undisclosed',
        PrimaryPhoto: listing.PrimaryPhoto,
        ListOfficeName: listing.ListOfficeName,
        SubdivisionName: listing.SubdivisionName,
        WaterfrontYN: listing.WaterfrontYN,
        PoolPrivateYN: listing.PoolPrivateYN,
        GarageSpaces: listing.GarageSpaces,
        NewConstructionYN: listing.NewConstructionYN,
        Zoning: listing.Zoning,
        ListingContractDate: listing.ListingContractDate
    };
}

/**
 * Resolves only current, tenant-scoped, Internet-display-eligible summaries.
 * Historical listing snapshots are never read from consumer persistence.
 */
export async function fetchCurrentListingSummaries(db, site, listingKeys, maxKeys = 40) {
    const cleanKeys = [...new Set((Array.isArray(listingKeys) ? listingKeys : [])
        .filter(key => typeof key === 'string' && key.trim())
        .map(key => key.trim().slice(0, 50)))]
        .slice(0, maxKeys);
    if (!cleanKeys.length) return [];

    const scope = buildCurrentListingScope(site);
    if (!scope.valid) return [];

    const placeholders = cleanKeys.map(() => '?').join(',');
    const rows = await db.prepare(`
        SELECT
            ListingKey, ListingId, ListPrice, OriginalListPrice, StandardStatus,
            PropertyType, PropertySubType, BedroomsTotal, BathroomsTotalInteger,
            LivingArea, LotSizeAcres, YearBuilt, City, StateOrProvince, PostalCode,
            CountyOrParish, UnparsedAddress, PrimaryPhoto, ListOfficeName,
            SubdivisionName, WaterfrontYN, PoolPrivateYN, GarageSpaces,
            NewConstructionYN, Zoning, ListingContractDate,
            InternetEntireListingDisplayYN, InternetAddressDisplayYN
        FROM sneak_listings
        WHERE ListingKey IN (${placeholders})
          AND InternetEntireListingDisplayYN = 1
          AND StandardStatus IN ('Active', 'Active Under Contract', 'Pending')
          AND ${scope.clause}
    `).bind(...cleanKeys, ...scope.bindings).all();

    const byKey = new Map((rows.results || []).map(row => [row.ListingKey, toListingSummary(row)]));
    return cleanKeys.map(key => byKey.get(key)).filter(Boolean);
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
    const insertRes = await env.DB.prepare(`
        INSERT OR IGNORE INTO sneak_consumer_favorites (id, site_id, user_id, listing_key, created_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    `).bind(favId, session.siteId, session.userId, listingKey).run();

    const changes = insertRes?.meta?.changes ?? insertRes?.changes ?? 0;
    if (changes > 0) {
        // Server Activity Log: favorite_added
        await recordConsumerActivity(env.DB, {
            siteId: session.siteId,
            userId: session.userId,
            eventType: 'favorite_added',
            listingKey
        });
    }

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

    const delRes = await env.DB.prepare(`
        DELETE FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ? AND listing_key = ?
    `).bind(session.userId, session.siteId, listingKey).run();

    const changes = delRes?.meta?.changes ?? delRes?.changes ?? 0;
    if (changes > 0) {
        // Server Activity Log: favorite_removed
        await recordConsumerActivity(env.DB, {
            siteId: session.siteId,
            userId: session.userId,
            eventType: 'favorite_removed',
            listingKey
        });
    }

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
        const res = await env.DB.prepare(`
            INSERT OR IGNORE INTO sneak_consumer_favorites (id, site_id, user_id, listing_key, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        `).bind(favId, session.siteId, session.userId, key).run();

        const changes = res?.meta?.changes ?? res?.changes ?? 0;
        if (changes > 0) {
            await recordConsumerActivity(env.DB, {
                siteId: session.siteId,
                userId: session.userId,
                eventType: 'favorite_added',
                listingKey: key
            });
        }
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

/**
 * GET /api/consumer/recently-viewed?site=...
 * Builds authenticated history exclusively from existing listing_view events.
 */
export async function handleListRecentlyViewed(req, url, env, origin) {
    const rawToken = extractBearerToken(req);
    const siteKey = url.searchParams.get('site');
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) return sessionErrorResponse(session, origin);

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site || site.site_id !== session.siteId) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    const eventRows = await env.DB.prepare(`
        SELECT listing_key, MAX(created_at) AS viewed_at
        FROM sneak_consumer_activity_events
        WHERE user_id = ? AND site_id = ? AND event_type = 'listing_view' AND listing_key IS NOT NULL
          AND created_at >= datetime('now', '-90 days')
        GROUP BY listing_key
        ORDER BY viewed_at DESC
        LIMIT ${RECENTLY_VIEWED_CANDIDATE_LIMIT}
    `).bind(session.userId, session.siteId).all();

    const events = eventRows.results || [];
    const summaries = await fetchCurrentListingSummaries(
        env.DB,
        site,
        events.map(event => event.listing_key),
        RECENTLY_VIEWED_CANDIDATE_LIMIT
    );
    const summaryByKey = new Map(summaries.map(listing => [listing.ListingKey, listing]));
    const recentlyViewed = events
        .map(event => {
            const listing = summaryByKey.get(event.listing_key);
            return listing ? { listingKey: event.listing_key, viewedAt: event.viewed_at, listing } : null;
        })
        .filter(Boolean)
        .slice(0, RECENTLY_VIEWED_LIMIT);

    return jsonResponse({ success: true, count: recentlyViewed.length, recentlyViewed }, 200, origin);
}

async function loadValidCompareEntries(db, site, session) {
    const rows = await db.prepare(`
        SELECT listing_key, created_at
        FROM sneak_consumer_compare
        WHERE user_id = ? AND site_id = ?
        ORDER BY created_at ASC, id ASC
        LIMIT ${COMPARE_LIMIT + 20}
    `).bind(session.userId, session.siteId).all();

    const entries = rows.results || [];
    const summaries = await fetchCurrentListingSummaries(db, site, entries.map(entry => entry.listing_key), COMPARE_LIMIT + 20);
    const summaryByKey = new Map(summaries.map(listing => [listing.ListingKey, listing]));
    const staleKeys = entries.filter(entry => !summaryByKey.has(entry.listing_key)).map(entry => entry.listing_key);
    if (staleKeys.length) {
        const placeholders = staleKeys.map(() => '?').join(',');
        await db.prepare(`
            DELETE FROM sneak_consumer_compare
            WHERE user_id = ? AND site_id = ? AND listing_key IN (${placeholders})
        `).bind(session.userId, session.siteId, ...staleKeys).run();
    }
    return entries
        .map(entry => {
            const listing = summaryByKey.get(entry.listing_key);
            return listing ? { listingKey: entry.listing_key, addedAt: entry.created_at, listing } : null;
        })
        .filter(Boolean)
        .slice(0, COMPARE_LIMIT);
}

/** GET /api/consumer/compare?site=... */
export async function handleListCompare(req, url, env, origin) {
    const rawToken = extractBearerToken(req);
    const siteKey = url.searchParams.get('site');
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) return sessionErrorResponse(session, origin);

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site || site.site_id !== session.siteId) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    const compare = await loadValidCompareEntries(env.DB, site, session);
    return jsonResponse({ success: true, count: compare.length, limit: COMPARE_LIMIT, compare }, 200, origin);
}

/** POST /api/consumer/compare */
export async function handleAddCompare(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site: siteKey, listingKey } = body || {};
    if (typeof listingKey !== 'string' || !listingKey.trim()) {
        return jsonResponse({ error: 'MissingListingKey', message: 'listingKey is required.' }, 400, origin);
    }

    const rawToken = extractBearerToken(req);
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) return sessionErrorResponse(session, origin);

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site || site.site_id !== session.siteId) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    const cleanKey = listingKey.trim().slice(0, 50);
    const eligible = await fetchCurrentListingSummaries(env.DB, site, [cleanKey], 1);
    if (!eligible.length) {
        return jsonResponse({ error: 'ListingNotFound', message: 'Listing is not currently available for comparison.' }, 404, origin);
    }

    const existing = await loadValidCompareEntries(env.DB, site, session);
    if (existing.some(item => item.listingKey === cleanKey)) {
        return jsonResponse({ success: true, count: existing.length, limit: COMPARE_LIMIT, compare: existing }, 200, origin);
    }
    if (existing.length >= COMPARE_LIMIT) {
        return jsonResponse({ error: 'CompareLimitExceeded', message: `You can compare up to ${COMPARE_LIMIT} properties.` }, 400, origin);
    }

    const compareId = `ccmp_${crypto.randomUUID()}`;
    try {
        await env.DB.prepare(`
            INSERT OR IGNORE INTO sneak_consumer_compare (id, site_id, user_id, listing_key, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        `).bind(compareId, session.siteId, session.userId, cleanKey).run();
    } catch (error) {
        if (String(error?.message || '').includes('compare_limit_exceeded')) {
            return jsonResponse({ error: 'CompareLimitExceeded', message: `You can compare up to ${COMPARE_LIMIT} properties.` }, 400, origin);
        }
        throw error;
    }

    const compare = await loadValidCompareEntries(env.DB, site, session);
    return jsonResponse({ success: true, count: compare.length, limit: COMPARE_LIMIT, compare }, 200, origin);
}

/** DELETE /api/consumer/compare/:listingKey?site=... */
export async function handleRemoveCompare(req, listingKey, url, env, origin) {
    const siteKey = url.searchParams.get('site');
    const rawToken = extractBearerToken(req);
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) return sessionErrorResponse(session, origin);

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site || site.site_id !== session.siteId) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    const cleanKey = String(listingKey || '').trim().slice(0, 50);
    await env.DB.prepare(`
        DELETE FROM sneak_consumer_compare
        WHERE user_id = ? AND site_id = ? AND listing_key = ?
    `).bind(session.userId, session.siteId, cleanKey).run();

    const compare = await loadValidCompareEntries(env.DB, site, session);
    return jsonResponse({ success: true, count: compare.length, limit: COMPARE_LIMIT, compare }, 200, origin);
}

/** POST /api/consumer/compare/merge */
export async function handleMergeCompare(req, env, origin) {
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
    if (!session || session.error) return sessionErrorResponse(session, origin);

    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site || site.site_id !== session.siteId) {
        return jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404, origin);
    }

    const localKeys = [...new Set(listingKeys
        .filter(key => typeof key === 'string' && key.trim())
        .map(key => key.trim().slice(0, 50)))]
        .slice(0, COMPARE_LIMIT);
    const eligibleLocal = await fetchCurrentListingSummaries(env.DB, site, localKeys, COMPARE_LIMIT);
    const eligibleSet = new Set(eligibleLocal.map(listing => listing.ListingKey));
    const existing = await loadValidCompareEntries(env.DB, site, session);
    const existingSet = new Set(existing.map(item => item.listingKey));
    const toInsert = localKeys
        .filter(key => eligibleSet.has(key) && !existingSet.has(key))
        .slice(0, Math.max(0, COMPARE_LIMIT - existing.length));

    let mergedCount = 0;
    for (const key of toInsert) {
        const compareId = `ccmp_${crypto.randomUUID()}`;
        try {
            const insertResult = await env.DB.prepare(`
                INSERT OR IGNORE INTO sneak_consumer_compare (id, site_id, user_id, listing_key, created_at)
                VALUES (?, ?, ?, ?, datetime('now'))
            `).bind(compareId, session.siteId, session.userId, key).run();
            if (Number(insertResult?.meta?.changes || 0) > 0) mergedCount += 1;
        } catch (error) {
            if (!String(error?.message || '').includes('compare_limit_exceeded')) throw error;
            break;
        }
    }

    const compare = await loadValidCompareEntries(env.DB, site, session);
    return jsonResponse({
        success: true,
        count: compare.length,
        limit: COMPARE_LIMIT,
        merged: mergedCount,
        compare
    }, 200, origin);
}

/* ===========================================================================
   Consumer Shared Property Lists (Phase 7.3C3B)
   ========================================================================== */

export function normalizeSharedListName(name) {
    if (typeof name !== 'string') return null;
    const cleanName = name.trim();
    if (!cleanName || cleanName.length > SHARED_LIST_NAME_LIMIT) return null;
    return cleanName;
}

/** Generates a 192-bit unlisted public capability identifier. */
export function generatePublicSharedListSlug() {
    const bytes = new Uint8Array(24);
    crypto.getRandomValues(bytes);
    return Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
}

function toSharedListOwnerSummary(row) {
    return {
        id: row.id,
        name: row.name,
        shareEnabled: Boolean(row.share_enabled),
        publicSlug: row.share_enabled ? (row.public_slug || null) : null,
        itemCount: Number(row.item_count || 0),
        createdAt: row.created_at,
        updatedAt: row.updated_at
    };
}

async function resolveSharedListOwnerContext(req, siteKey, env) {
    const rawToken = extractBearerToken(req);
    const session = await verifyConsumerSession(env.DB, rawToken, siteKey);
    if (!session || session.error) return { errorResponse: sessionErrorResponse(session, null) };
    const site = await resolveSiteScope(env.DB, siteKey);
    if (!site || site.site_id !== session.siteId) {
        return { errorResponse: jsonResponse({ error: 'SiteNotFound', message: 'Site is inactive or not found.' }, 404) };
    }
    return { session, site };
}

async function getOwnedSharedList(db, listId, session) {
    return await db.prepare(`
        SELECT l.id, l.name, l.share_enabled, l.public_slug, l.created_at, l.updated_at,
               (SELECT COUNT(*) FROM sneak_consumer_shared_list_items i WHERE i.list_id = l.id) AS item_count
        FROM sneak_consumer_shared_lists l
        WHERE l.id = ? AND l.user_id = ? AND l.site_id = ?
    `).bind(listId, session.userId, session.siteId).first();
}

function sharedListNotFound(origin) {
    return jsonResponse({ error: 'NotFound', message: 'Property list not found or unavailable.' }, 404, origin);
}

/** GET /api/consumer/shared-lists?site=... */
export async function handleListSharedLists(req, url, env, origin) {
    const siteKey = url.searchParams.get('site');
    const context = await resolveSharedListOwnerContext(req, siteKey, env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }

    const rows = await env.DB.prepare(`
        SELECT l.id, l.name, l.share_enabled, l.public_slug, l.created_at, l.updated_at,
               (SELECT COUNT(*) FROM sneak_consumer_shared_list_items i WHERE i.list_id = l.id) AS item_count
        FROM sneak_consumer_shared_lists l
        WHERE l.user_id = ? AND l.site_id = ?
        ORDER BY l.updated_at DESC, l.id DESC
        LIMIT ${SHARED_LISTS_LIMIT}
    `).bind(context.session.userId, context.session.siteId).all();

    const sharedLists = (rows.results || []).map(toSharedListOwnerSummary);
    return jsonResponse({ success: true, count: sharedLists.length, limit: SHARED_LISTS_LIMIT, sharedLists }, 200, origin);
}

/** POST /api/consumer/shared-lists */
export async function handleCreateSharedList(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const siteKey = body?.site;
    const cleanName = normalizeSharedListName(body?.name);
    if (!cleanName) {
        return jsonResponse({ error: 'InvalidName', message: `List name must be between 1 and ${SHARED_LIST_NAME_LIMIT} characters.` }, 400, origin);
    }
    if (body?.listingKeys !== undefined && !Array.isArray(body.listingKeys)) {
        return jsonResponse({ error: 'InvalidListingKeys', message: 'listingKeys must be an array.' }, 400, origin);
    }

    const context = await resolveSharedListOwnerContext(req, siteKey, env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }

    const countRow = await env.DB.prepare(`
        SELECT COUNT(*) AS count FROM sneak_consumer_shared_lists
        WHERE user_id = ? AND site_id = ?
    `).bind(context.session.userId, context.session.siteId).first();
    if (Number(countRow?.count || 0) >= SHARED_LISTS_LIMIT) {
        return jsonResponse({ error: 'SharedListLimitExceeded', message: `You can create up to ${SHARED_LISTS_LIMIT} property lists.` }, 400, origin);
    }

    const listingKeys = [...new Set((body.listingKeys || [])
        .filter(key => typeof key === 'string' && key.trim())
        .map(key => key.trim().slice(0, 50)))];
    if (listingKeys.length > SHARED_LIST_ITEMS_LIMIT) {
        return jsonResponse({ error: 'SharedListItemLimitExceeded', message: `A property list can contain up to ${SHARED_LIST_ITEMS_LIMIT} properties.` }, 400, origin);
    }
    if (listingKeys.length) {
        const eligible = await fetchCurrentListingSummaries(env.DB, context.site, listingKeys, SHARED_LIST_ITEMS_LIMIT);
        if (eligible.length !== listingKeys.length) {
            return jsonResponse({ error: 'ListingNotFound', message: 'One or more properties are unavailable or outside this site scope.' }, 404, origin);
        }
    }

    const listId = `clist_${crypto.randomUUID()}`;
    const now = new Date().toISOString();
    const statements = [env.DB.prepare(`
        INSERT INTO sneak_consumer_shared_lists
        (id, site_id, user_id, name, share_enabled, public_slug, created_at, updated_at)
        VALUES (?, ?, ?, ?, 0, NULL, ?, ?)
    `).bind(listId, context.session.siteId, context.session.userId, cleanName, now, now)];
    listingKeys.forEach((listingKey, index) => {
        statements.push(env.DB.prepare(`
            INSERT INTO sneak_consumer_shared_list_items
            (id, list_id, listing_key, sort_order, created_at)
            VALUES (?, ?, ?, ?, ?)
        `).bind(`clitem_${crypto.randomUUID()}`, listId, listingKey, index, now));
    });

    try {
        if (typeof env.DB.batch === 'function') await env.DB.batch(statements);
        else for (const statement of statements) await statement.run();
    } catch (error) {
        if (String(error?.message || '').includes('shared_list_limit_exceeded')) {
            return jsonResponse({ error: 'SharedListLimitExceeded', message: `You can create up to ${SHARED_LISTS_LIMIT} property lists.` }, 400, origin);
        }
        throw error;
    }

    const created = await getOwnedSharedList(env.DB, listId, context.session);
    return jsonResponse({ success: true, sharedList: toSharedListOwnerSummary(created) }, 201, origin);
}

/** GET /api/consumer/shared-lists/:id?site=... */
export async function handleGetSharedList(req, listId, url, env, origin) {
    const context = await resolveSharedListOwnerContext(req, url.searchParams.get('site'), env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }
    const list = await getOwnedSharedList(env.DB, listId, context.session);
    if (!list) return sharedListNotFound(origin);

    const itemRows = await env.DB.prepare(`
        SELECT id, listing_key, sort_order, created_at
        FROM sneak_consumer_shared_list_items
        WHERE list_id = ?
        ORDER BY sort_order ASC, created_at ASC, id ASC
        LIMIT ${SHARED_LIST_ITEMS_LIMIT}
    `).bind(listId).all();
    const itemReferences = itemRows.results || [];
    const summaries = await fetchCurrentListingSummaries(
        env.DB,
        context.site,
        itemReferences.map(item => item.listing_key),
        SHARED_LIST_ITEMS_LIMIT
    );
    const summaryByKey = new Map(summaries.map(listing => [listing.ListingKey, listing]));
    const items = itemReferences.map(item => ({
        id: item.id,
        listingKey: item.listing_key,
        sortOrder: item.sort_order,
        addedAt: item.created_at,
        unavailable: !summaryByKey.has(item.listing_key),
        listing: summaryByKey.get(item.listing_key) || null
    }));

    return jsonResponse({ success: true, sharedList: { ...toSharedListOwnerSummary(list), items } }, 200, origin);
}

/** PATCH /api/consumer/shared-lists/:id?site=... */
export async function handleUpdateSharedList(req, listId, url, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }
    const cleanName = normalizeSharedListName(body?.name);
    if (!cleanName) {
        return jsonResponse({ error: 'InvalidName', message: `List name must be between 1 and ${SHARED_LIST_NAME_LIMIT} characters.` }, 400, origin);
    }
    const context = await resolveSharedListOwnerContext(req, url.searchParams.get('site'), env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }
    const result = await env.DB.prepare(`
        UPDATE sneak_consumer_shared_lists SET name = ?, updated_at = datetime('now')
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(cleanName, listId, context.session.userId, context.session.siteId).run();
    if (Number(result?.meta?.changes ?? result?.changes ?? 0) === 0) return sharedListNotFound(origin);
    const updated = await getOwnedSharedList(env.DB, listId, context.session);
    return jsonResponse({ success: true, sharedList: toSharedListOwnerSummary(updated) }, 200, origin);
}

/** DELETE /api/consumer/shared-lists/:id?site=... */
export async function handleDeleteSharedList(req, listId, url, env, origin) {
    const context = await resolveSharedListOwnerContext(req, url.searchParams.get('site'), env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }
    const result = await env.DB.prepare(`
        DELETE FROM sneak_consumer_shared_lists
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(listId, context.session.userId, context.session.siteId).run();
    if (Number(result?.meta?.changes ?? result?.changes ?? 0) === 0) return sharedListNotFound(origin);
    return jsonResponse({ success: true, id: listId }, 200, origin);
}

/** POST /api/consumer/shared-lists/:id/items */
export async function handleAddSharedListItem(req, listId, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }
    const siteKey = body?.site;
    const cleanKey = typeof body?.listingKey === 'string' ? body.listingKey.trim().slice(0, 50) : '';
    if (!cleanKey) return jsonResponse({ error: 'MissingListingKey', message: 'listingKey is required.' }, 400, origin);

    const context = await resolveSharedListOwnerContext(req, siteKey, env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }
    const list = await getOwnedSharedList(env.DB, listId, context.session);
    if (!list) return sharedListNotFound(origin);

    const eligible = await fetchCurrentListingSummaries(env.DB, context.site, [cleanKey], 1);
    if (!eligible.length) {
        return jsonResponse({ error: 'ListingNotFound', message: 'Property is unavailable or outside this site scope.' }, 404, origin);
    }

    const existing = await env.DB.prepare(`
        SELECT id FROM sneak_consumer_shared_list_items WHERE list_id = ? AND listing_key = ?
    `).bind(listId, cleanKey).first();
    if (existing) {
        return jsonResponse({ success: true, idempotent: true, listingKey: cleanKey, count: Number(list.item_count || 0) }, 200, origin);
    }
    if (Number(list.item_count || 0) >= SHARED_LIST_ITEMS_LIMIT) {
        return jsonResponse({ error: 'SharedListItemLimitExceeded', message: `A property list can contain up to ${SHARED_LIST_ITEMS_LIMIT} properties.` }, 400, origin);
    }

    const orderRow = await env.DB.prepare(`
        SELECT COALESCE(MAX(sort_order), -1) + 1 AS next_order
        FROM sneak_consumer_shared_list_items WHERE list_id = ?
    `).bind(listId).first();
    try {
        await env.DB.prepare(`
            INSERT OR IGNORE INTO sneak_consumer_shared_list_items
            (id, list_id, listing_key, sort_order, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        `).bind(`clitem_${crypto.randomUUID()}`, listId, cleanKey, Number(orderRow?.next_order || 0)).run();
    } catch (error) {
        if (String(error?.message || '').includes('shared_list_item_limit_exceeded')) {
            return jsonResponse({ error: 'SharedListItemLimitExceeded', message: `A property list can contain up to ${SHARED_LIST_ITEMS_LIMIT} properties.` }, 400, origin);
        }
        throw error;
    }
    return jsonResponse({ success: true, idempotent: false, listingKey: cleanKey, count: Number(list.item_count || 0) + 1 }, 200, origin);
}

/** DELETE /api/consumer/shared-lists/:id/items/:listingKey?site=... */
export async function handleRemoveSharedListItem(req, listId, listingKey, url, env, origin) {
    const context = await resolveSharedListOwnerContext(req, url.searchParams.get('site'), env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }
    const list = await getOwnedSharedList(env.DB, listId, context.session);
    if (!list) return sharedListNotFound(origin);
    const cleanKey = String(listingKey || '').trim().slice(0, 50);
    await env.DB.prepare(`
        DELETE FROM sneak_consumer_shared_list_items WHERE list_id = ? AND listing_key = ?
    `).bind(listId, cleanKey).run();
    return jsonResponse({ success: true, listingKey: cleanKey }, 200, origin);
}

/** POST /api/consumer/shared-lists/:id/share */
export async function handleEnableSharedListShare(req, listId, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }
    const context = await resolveSharedListOwnerContext(req, body?.site, env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }
    const list = await getOwnedSharedList(env.DB, listId, context.session);
    if (!list) return sharedListNotFound(origin);
    if (list.share_enabled && list.public_slug) {
        return jsonResponse({ success: true, sharedList: toSharedListOwnerSummary(list) }, 200, origin);
    }

    let enabled = null;
    for (let attempt = 0; attempt < 3 && !enabled; attempt++) {
        const slug = generatePublicSharedListSlug();
        try {
            const result = await env.DB.prepare(`
                UPDATE sneak_consumer_shared_lists
                SET share_enabled = 1, public_slug = ?, updated_at = datetime('now')
                WHERE id = ? AND user_id = ? AND site_id = ? AND share_enabled = 0
            `).bind(slug, listId, context.session.userId, context.session.siteId).run();
            if (Number(result?.meta?.changes ?? result?.changes ?? 0) > 0) enabled = slug;
        } catch (error) {
            if (!String(error?.message || '').toLowerCase().includes('unique')) throw error;
        }
    }
    const updated = await getOwnedSharedList(env.DB, listId, context.session);
    if (!updated?.share_enabled || !updated?.public_slug) {
        return jsonResponse({ error: 'ShareEnableFailed', message: 'Unable to enable sharing. Please try again.' }, 500, origin);
    }
    return jsonResponse({ success: true, sharedList: toSharedListOwnerSummary(updated) }, 200, origin);
}

/** DELETE /api/consumer/shared-lists/:id/share?site=... */
export async function handleDisableSharedListShare(req, listId, url, env, origin) {
    const context = await resolveSharedListOwnerContext(req, url.searchParams.get('site'), env);
    if (context.errorResponse) {
        const data = await context.errorResponse.json();
        return jsonResponse(data, context.errorResponse.status, origin);
    }
    const result = await env.DB.prepare(`
        UPDATE sneak_consumer_shared_lists
        SET share_enabled = 0, public_slug = NULL, updated_at = datetime('now')
        WHERE id = ? AND user_id = ? AND site_id = ?
    `).bind(listId, context.session.userId, context.session.siteId).run();
    if (Number(result?.meta?.changes ?? result?.changes ?? 0) === 0) return sharedListNotFound(origin);
    const updated = await getOwnedSharedList(env.DB, listId, context.session);
    return jsonResponse({ success: true, sharedList: toSharedListOwnerSummary(updated) }, 200, origin);
}

/**
 * POST /api/consumer/activity
 * Browser-reported activity endpoint for authenticated buyers (listing_view, inquiry_submitted).
 * 
 * Invariants:
 * - Session authentication required.
 * - Rate limit: 120 browser events / consumer / site / hour.
 * - Allowed types: strictly 'listing_view' and 'inquiry_submitted'.
 * - 30-minute deduplication window for listing views.
 * - Server verification of listing scope and display compliance.
 * - Server verification of inquiry ownership by matching site and normalized email.
 */
export async function handleReportActivity(req, env, origin) {
    let body;
    try {
        body = await req.json();
    } catch {
        return jsonResponse({ error: 'InvalidJSON', message: 'Request body must be valid JSON.' }, 400, origin);
    }

    const { site: siteKey, type, listingKey, leadId } = body || {};
    const siteKeyParam = siteKey || new URL(req.url).searchParams.get('site');
    const rawToken = extractBearerToken(req);

    const session = await verifyConsumerSession(env.DB, rawToken, siteKeyParam);
    if (!session || session.error) {
        return sessionErrorResponse(session, origin);
    }

    // Rate Limit: 120 browser-originated events per consumer per site per hour
    try {
        const rateRow = await env.DB.prepare(`
            SELECT count(*) as count FROM sneak_consumer_activity_events
            WHERE user_id = ? AND site_id = ? AND event_type IN ('listing_view', 'inquiry_submitted')
              AND created_at > datetime('now', '-1 hour')
        `).bind(session.userId, session.siteId).first();

        if ((rateRow?.count || 0) >= 120) {
            return jsonResponse({
                error: 'RateLimitExceeded',
                message: 'Activity reporting rate limit exceeded (max 120 events/hour).'
            }, 429, origin);
        }
    } catch {}

    // Allowlist check: Only listing_view and inquiry_submitted allowed from browser
    if (type !== 'listing_view' && type !== 'inquiry_submitted') {
        return jsonResponse({
            error: 'InvalidEventType',
            message: 'Browser reporting is only permitted for listing_view and inquiry_submitted.'
        }, 400, origin);
    }

    // Handle listing_view
    if (type === 'listing_view') {
        if (!listingKey || typeof listingKey !== 'string') {
            return jsonResponse({ error: 'MissingListingKey', message: 'listingKey is required for listing_view.' }, 400, origin);
        }

        const cleanListingKey = listingKey.trim().slice(0, 50);

        const site = await resolveSiteScope(env.DB, siteKeyParam || session.siteKey);
        if (!site) {
            return jsonResponse({ error: 'SiteNotFound', message: 'Site not found.' }, 404, origin);
        }

        let scopeClause = '';
        const bindings = [cleanListingKey];
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
            return jsonResponse({
                error: 'InvalidListingKey',
                message: 'Listing does not exist, is display-disabled, or is outside site scope.'
            }, 404, origin);
        }

        // Server-derived 30-minute dedupe bucket
        const bucket = Math.floor(Date.now() / (30 * 60 * 1000));
        const dedupeKey = `listing_view:${session.userId}:${session.siteId}:${cleanListingKey}:${bucket}`;

        const result = await recordConsumerActivity(env.DB, {
            siteId: session.siteId,
            userId: session.userId,
            eventType: 'listing_view',
            listingKey: cleanListingKey,
            dedupeKey
        });

        return jsonResponse({ success: true, recorded: result.success }, 200, origin);
    }

    // Handle inquiry_submitted
    if (type === 'inquiry_submitted') {
        if (!leadId || typeof leadId !== 'string') {
            return jsonResponse({ error: 'MissingLeadId', message: 'leadId is required for inquiry_submitted.' }, 400, origin);
        }

        const cleanLeadId = leadId.trim();
        const lead = await env.DB.prepare(`
            SELECT id, site_id, email, listing_key FROM sneak_leads WHERE id = ?
        `).bind(cleanLeadId).first();

        if (!lead) {
            return jsonResponse({ error: 'LeadNotFound', message: 'Inquiry record not found.' }, 404, origin);
        }

        if (lead.site_id !== session.siteId) {
            return jsonResponse({ error: 'UnauthorizedLead', message: 'Inquiry belongs to another site.' }, 403, origin);
        }

        const consumerUser = await env.DB.prepare(`
            SELECT email FROM sneak_consumer_users WHERE id = ?
        `).bind(session.userId).first();

        const consumerEmail = (consumerUser?.email || '').toLowerCase().trim();
        const leadEmail = (lead.email || '').toLowerCase().trim();

        if (!consumerEmail || consumerEmail !== leadEmail) {
            return jsonResponse({
                error: 'EmailMismatch',
                message: 'Inquiry email does not match authenticated consumer email.'
            }, 403, origin);
        }

        const dedupeKey = `inquiry_submitted:${session.userId}:${session.siteId}:${cleanLeadId}`;
        const result = await recordConsumerActivity(env.DB, {
            siteId: session.siteId,
            userId: session.userId,
            eventType: 'inquiry_submitted',
            listingKey: listingKey ? String(listingKey).trim() : lead.listing_key,
            leadId: lead.id,
            dedupeKey
        });

        return jsonResponse({ success: true, recorded: result.success }, 200, origin);
    }

    return jsonResponse({ error: 'BadRequest', message: 'Invalid activity request.' }, 400, origin);
}
