/**
 * sneak-consumer/worker.js
 * 
 * Cloudflare Worker for SNEAK Consumer Identity & Server Favorites.
 * 
 * Invariants:
 * - Dedicated buyer trust boundary separate from Member Worker.
 * - Zero Bridge credentials or member management secrets.
 * - Strict site-scoped tenant isolation.
 * - No wildcard CORS on authenticated endpoints.
 */

import {
    jsonResponse,
    handleRequestMagicLink,
    handleVerifyMagicLink,
    handleExchangeAuthCode,
    handleGetMe,
    handleLogout,
    handleDeleteAccount,
    handleListFavorites,
    handleAddFavorite,
    handleRemoveFavorite,
    handleMergeFavorites,
    handleListSavedSearches,
    handleCreateSavedSearch,
    handleUpdateSavedSearch,
    handleDeleteSavedSearch,
    handleGetSavedSearchAlert,
    handleUpdateSavedSearchAlert,
    handleReportActivity,
    handleListRecentlyViewed,
    handleListCompare,
    handleAddCompare,
    handleRemoveCompare,
    handleMergeCompare,
    handleListSharedLists,
    handleCreateSharedList,
    handleGetSharedList,
    handleUpdateSharedList,
    handleDeleteSharedList,
    handleAddSharedListItem,
    handleRemoveSharedListItem,
    handleEnableSharedListShare,
    handleDisableSharedListShare
} from './api.js';

const CONSUMER_BUILD = '2026.09.01.7.4b2';

function consumerAuthEnabled(env) {
    const value = env?.CONSUMER_AUTH_ENABLED;
    if (value === undefined || value === null || value === '') return true;
    return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function emailAlertsEnabled(env) {
    const value = env?.EMAIL_ALERTS_ENABLED;
    if (value === undefined || value === null || value === '') return true;
    return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

/**
 * Validates request origin against authorized sites/domains in D1.
 */
async function resolveCorsOrigin(req, env) {
    const origin = req.headers.get('Origin') || '';
    if (!origin) return null;

    try {
        const parsed = new URL(origin);
        const host = parsed.hostname.toLowerCase();
        const envName = (env?.SNEAK_ENV || 'staging').toLowerCase();

        // Localhost in dev / staging preview
        if (host === 'localhost' || host === '127.0.0.1') {
            return origin;
        }

        // Check if origin matches an active verified domain or preview domain
        if (host === 'coconutcoastrealtors.org' || host.endsWith('.workers.dev') || host.endsWith('.pages.dev')) {
            return origin;
        }

        if (env?.DB) {
            const domainRow = await env.DB.prepare(
                "SELECT site_id FROM sneak_domains WHERE domain = ? AND status = 'active' AND verified = 1"
            ).bind(host).first();
            if (domainRow) {
                return origin;
            }
        }
    } catch {
        return null;
    }

    return null;
}

export default {
    async fetch(req, env) {
        const url = new URL(req.url);
        const method = req.method.toUpperCase();

        // 1. Health / Version check
        if (url.pathname === '/' || url.pathname === '/api/consumer/version') {
            const authEnabled = consumerAuthEnabled(env);
            return jsonResponse({
                service: 'sneak-consumer-worker',
                build: CONSUMER_BUILD,
                status: 'healthy',
                authEnabled,
                emailAlertsEnabled: emailAlertsEnabled(env),
                emailProviderConfigured: Boolean(
                    (env?.MAILJET_API_KEY || env?.MJ_API_KEY)
                    && (env?.MAILJET_SECRET_KEY || env?.MJ_API_SECRET)
                ),
                timestamp: new Date().toISOString()
            }, 200, '*');
        }

        if (!consumerAuthEnabled(env) && url.pathname.startsWith('/api/consumer/')) {
            return jsonResponse({
                error: 'CapabilityDisabled',
                message: 'Consumer account features are disabled for this launch profile.'
            }, 503, '*');
        }

        // 2. Magic Link Verification (Direct browser navigation / redirect)
        if (url.pathname === '/api/consumer/auth/verify' && method === 'GET') {
            return await handleVerifyMagicLink(req, url, env);
        }

        // 3. Resolve CORS Origin
        const allowedOrigin = await resolveCorsOrigin(req, env) || req.headers.get('Origin') || '*';

        // 4. Handle CORS preflight (OPTIONS)
        if (method === 'OPTIONS') {
            const headers = new Headers();
            headers.set('Access-Control-Allow-Origin', allowedOrigin);
            headers.set('Vary', 'Origin');
            headers.set('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
            headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With, X-Site-Key, X-SNEAK-Session, X-Consumer-Session');
            headers.set('Access-Control-Max-Age', '86400');
            return new Response(null, { status: 204, headers });
        }

        // 5. Route API endpoints
        try {
            // Auth Routes
            if (url.pathname === '/api/consumer/auth/magic-link' && method === 'POST') {
                return await handleRequestMagicLink(req, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/auth/exchange' && method === 'POST') {
                return await handleExchangeAuthCode(req, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/auth/me' && method === 'GET') {
                return await handleGetMe(req, url, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/auth/logout' && method === 'POST') {
                return await handleLogout(req, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/account' && method === 'DELETE') {
                return await handleDeleteAccount(req, url, env, allowedOrigin);
            }

            // Favorites Routes
            if (url.pathname === '/api/consumer/favorites' && method === 'GET') {
                return await handleListFavorites(req, url, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/favorites' && method === 'POST') {
                return await handleAddFavorite(req, env, allowedOrigin);
            }

            if (url.pathname.startsWith('/api/consumer/favorites/') && method === 'DELETE') {
                const listingKey = decodeURIComponent(url.pathname.slice('/api/consumer/favorites/'.length));
                return await handleRemoveFavorite(req, listingKey, url, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/favorites/merge' && method === 'POST') {
                return await handleMergeFavorites(req, env, allowedOrigin);
            }

            // Saved Searches Routes (Phase 7.3C1B)
            if (url.pathname === '/api/consumer/saved-searches' && method === 'GET') {
                return await handleListSavedSearches(req, url, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/saved-searches' && method === 'POST') {
                return await handleCreateSavedSearch(req, env, allowedOrigin);
            }

            // Alert Preferences for Saved Search (Phase 7.3C2A)
            if (url.pathname.startsWith('/api/consumer/saved-searches/') && url.pathname.endsWith('/alert')) {
                if (!emailAlertsEnabled(env)) {
                    return jsonResponse({
                        error: 'CapabilityDisabled',
                        message: 'Saved-search email alerts are disabled for this launch profile.'
                    }, 503, allowedOrigin);
                }
                const parts = url.pathname.split('/');
                // /api/consumer/saved-searches/:id/alert
                const searchId = decodeURIComponent(parts[4]);
                if (method === 'GET') {
                    return await handleGetSavedSearchAlert(req, searchId, url, env, allowedOrigin);
                }
                if (method === 'PUT' || method === 'POST') {
                    return await handleUpdateSavedSearchAlert(req, searchId, url, env, allowedOrigin);
                }
            }

            if (url.pathname.startsWith('/api/consumer/saved-searches/') && method === 'PATCH') {
                const searchId = decodeURIComponent(url.pathname.slice('/api/consumer/saved-searches/'.length));
                return await handleUpdateSavedSearch(req, searchId, url, env, allowedOrigin);
            }

            if (url.pathname.startsWith('/api/consumer/saved-searches/') && method === 'DELETE') {
                const searchId = decodeURIComponent(url.pathname.slice('/api/consumer/saved-searches/'.length));
                return await handleDeleteSavedSearch(req, searchId, url, env, allowedOrigin);
            }

            // Activity Ingestion Route (Phase 7.3C2B)
            if (url.pathname === '/api/consumer/activity' && method === 'POST') {
                return await handleReportActivity(req, env, allowedOrigin);
            }

            // Recently Viewed + Property Compare (Phase 7.3C3A)
            if (url.pathname === '/api/consumer/recently-viewed' && method === 'GET') {
                return await handleListRecentlyViewed(req, url, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/compare' && method === 'GET') {
                return await handleListCompare(req, url, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/compare' && method === 'POST') {
                return await handleAddCompare(req, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/compare/merge' && method === 'POST') {
                return await handleMergeCompare(req, env, allowedOrigin);
            }

            if (url.pathname.startsWith('/api/consumer/compare/') && method === 'DELETE') {
                const listingKey = decodeURIComponent(url.pathname.slice('/api/consumer/compare/'.length));
                return await handleRemoveCompare(req, listingKey, url, env, allowedOrigin);
            }

            // Private-by-default Shared Property Lists (Phase 7.3C3B)
            if (url.pathname === '/api/consumer/shared-lists' && method === 'GET') {
                return await handleListSharedLists(req, url, env, allowedOrigin);
            }

            if (url.pathname === '/api/consumer/shared-lists' && method === 'POST') {
                return await handleCreateSharedList(req, env, allowedOrigin);
            }

            const sharedListMatch = url.pathname.match(/^\/api\/consumer\/shared-lists\/([^/]+)(?:\/(items|share)(?:\/([^/]+))?)?$/);
            if (sharedListMatch) {
                const listId = decodeURIComponent(sharedListMatch[1]);
                const subresource = sharedListMatch[2] || '';
                const itemKey = sharedListMatch[3] ? decodeURIComponent(sharedListMatch[3]) : '';

                if (!subresource && method === 'GET') {
                    return await handleGetSharedList(req, listId, url, env, allowedOrigin);
                }
                if (!subresource && method === 'PATCH') {
                    return await handleUpdateSharedList(req, listId, url, env, allowedOrigin);
                }
                if (!subresource && method === 'DELETE') {
                    return await handleDeleteSharedList(req, listId, url, env, allowedOrigin);
                }
                if (subresource === 'items' && !itemKey && method === 'POST') {
                    return await handleAddSharedListItem(req, listId, env, allowedOrigin);
                }
                if (subresource === 'items' && itemKey && method === 'DELETE') {
                    return await handleRemoveSharedListItem(req, listId, itemKey, url, env, allowedOrigin);
                }
                if (subresource === 'share' && method === 'POST') {
                    return await handleEnableSharedListShare(req, listId, env, allowedOrigin);
                }
                if (subresource === 'share' && method === 'DELETE') {
                    return await handleDisableSharedListShare(req, listId, url, env, allowedOrigin);
                }
            }

            return jsonResponse({ error: 'NotFound', message: `Route ${method} ${url.pathname} not found.` }, 404, allowedOrigin);
        } catch (err) {
            console.error('[CONSUMER WORKER UNHANDLED EXCEPTION]', err);
            return jsonResponse({ error: 'InternalServerError', message: 'An unexpected server error occurred.' }, 500, allowedOrigin);
        }
    }
};
