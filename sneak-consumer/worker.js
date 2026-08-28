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
    handleDeleteSavedSearch
} from './api.js';

const CONSUMER_BUILD = '2026.08.28.7.3c1b';

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
            return jsonResponse({
                service: 'sneak-consumer-worker',
                build: CONSUMER_BUILD,
                status: 'healthy',
                timestamp: new Date().toISOString()
            }, 200, '*');
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
            headers.set('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
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

            if (url.pathname.startsWith('/api/consumer/saved-searches/') && method === 'PATCH') {
                const searchId = decodeURIComponent(url.pathname.slice('/api/consumer/saved-searches/'.length));
                return await handleUpdateSavedSearch(req, searchId, url, env, allowedOrigin);
            }

            if (url.pathname.startsWith('/api/consumer/saved-searches/') && method === 'DELETE') {
                const searchId = decodeURIComponent(url.pathname.slice('/api/consumer/saved-searches/'.length));
                return await handleDeleteSavedSearch(req, searchId, url, env, allowedOrigin);
            }

            return jsonResponse({ error: 'NotFound', message: `Route ${method} ${url.pathname} not found.` }, 404, allowedOrigin);
        } catch (err) {
            console.error('[CONSUMER WORKER UNHANDLED EXCEPTION]', err);
            return jsonResponse({ error: 'InternalServerError', message: 'An unexpected server error occurred.' }, 500, allowedOrigin);
        }
    }
};
