/**
 * sneak-admin/worker.js
 * 
 * SNEAK IDX Platform — Hardened Private Administration Worker (sneak-idx-admin-staging)
 */

import { renderAdminHtml } from './ui.js';
import {
    verifyAdminPassword,
    createAdminSession,
    verifyAdminSession,
    revokeAdminSession,
    getSessionTokenFromRequest,
    validateAdminCsrf,
    checkLoginRateLimit,
    recordLoginAttempt,
    sha256Hex
} from './auth.js';
import * as api from './api.js';

const SECURITY_HEADERS = {
    'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline' https://fonts.googleapis.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; connect-src 'self' https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev; frame-ancestors 'none';",
    'X-Frame-Options': 'DENY',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Permissions-Policy': 'accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()'
};

function json(data, status = 200, headers = {}) {
    return new Response(JSON.stringify(data), {
        status,
        headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-store',
            ...SECURITY_HEADERS,
            ...headers
        }
    });
}

function error(message, status = 400, code = 'BadRequest') {
    return json({ error: code, message }, status);
}

async function logAuthAudit(db, actor, action, summary) {
    try {
        const id = `audit_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_admin_audit (id, admin_actor, action, entity_type, entity_id, summary, created_at)
            VALUES (?, ?, ?, 'auth', ?, ?, datetime('now'))
        `).bind(id, actor, action, actor, summary).run();
    } catch (err) {
        console.error('[AUTH AUDIT LOG ERROR]', err.message);
    }
}

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;
        const method = request.method.toUpperCase();

        // Client IP & UA pseudonymization
        const clientIp = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || '127.0.0.1';
        const userAgent = request.headers.get('User-Agent') || 'unknown';
        const ipHash = await sha256Hex(clientIp);
        const uaHash = await sha256Hex(userAgent);

        // 1. Static UI Route
        if (method === 'GET' && (path === '/' || path === '/admin' || path === '/index.html')) {
            return new Response(renderAdminHtml(), {
                status: 200,
                headers: {
                    'Content-Type': 'text/html; charset=utf-8',
                    'Cache-Control': 'no-store',
                    ...SECURITY_HEADERS
                }
            });
        }

        // 2. Health check
        if (method === 'GET' && path === '/health') {
            return json({
                service: 'sneak-idx-admin-staging',
                status: 'ok',
                env: env.SNEAK_ENV || 'staging',
                timestamp: new Date().toISOString()
            });
        }

        // 3. Admin Login (POST /api/admin/login)
        if (path === '/api/admin/login' && method === 'POST') {
            // CSRF / Same-Origin Check
            if (!validateAdminCsrf(request)) {
                return error('CSRF verification failed', 403, 'Forbidden');
            }

            const storedHash = env.SNEAK_ADMIN_PASSWORD_HASH;
            if (!storedHash) {
                // Fail closed: Never authenticate without explicit password hash configuration
                return error('Admin authentication configuration missing.', 500, 'ConfigurationError');
            }

            // Rate Limit Check
            const rateLimit = await checkLoginRateLimit(env.DB, ipHash);
            if (!rateLimit.allowed) {
                await logAuthAudit(env.DB, 'anonymous', 'LOGIN_RATE_LIMITED', `Rate limited after ${rateLimit.failedAttempts} failed attempts`);
                return error('Too many failed login attempts. Please try again in 15 minutes.', 429, 'TooManyRequests');
            }

            try {
                const body = await request.json();
                const password = body?.password || '';

                const valid = await verifyAdminPassword(password, storedHash);
                if (!valid) {
                    await recordLoginAttempt(env.DB, ipHash, false);
                    await logAuthAudit(env.DB, 'anonymous', 'LOGIN_FAILURE', 'Invalid password attempt');
                    return error('Invalid credentials.', 401, 'InvalidCredentials');
                }

                // Successful login
                await recordLoginAttempt(env.DB, ipHash, true);
                const actor = 'admin';
                const rawToken = await createAdminSession(env.DB, actor, ipHash, uaHash, 14400); // 4 hour session

                await logAuthAudit(env.DB, actor, 'LOGIN_SUCCESS', 'Admin session created');

                const cookie = `__Host-sneak_admin_session=${encodeURIComponent(rawToken)}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=14400`;

                return json({
                    success: true,
                    message: 'Authenticated successfully'
                }, 200, { 'Set-Cookie': cookie });
            } catch (err) {
                console.error('[LOGIN ERROR]', err);
                return error('Malformed login request: ' + err.message, 400);
            }
        }

        // 4. Admin Logout (POST /api/admin/logout)
        if (path === '/api/admin/logout' && method === 'POST') {
            const rawToken = getSessionTokenFromRequest(request);
            if (rawToken && env.DB) {
                await revokeAdminSession(env.DB, rawToken);
                await logAuthAudit(env.DB, 'admin', 'LOGOUT', 'Admin session revoked in database');
            }

            const cookie = `__Host-sneak_admin_session=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0`;
            return json({ success: true, message: 'Logged out' }, 200, { 'Set-Cookie': cookie });
        }

        // 5. Protected Admin API Routes Guard
        if (path.startsWith('/api/admin/')) {
            // Strict Same-Origin CSRF Verification for all state-changing calls
            if (!validateAdminCsrf(request)) {
                return error('CSRF verification failed', 403, 'Forbidden');
            }

            // Verify Server-Side Session from D1
            const rawToken = getSessionTokenFromRequest(request);
            if (!rawToken || !env.DB) {
                return error('Authentication required to access admin resources.', 401, 'Unauthorized');
            }

            const session = await verifyAdminSession(env.DB, rawToken);
            if (!session) {
                return error('Invalid or expired session. Please log in again.', 401, 'Unauthorized');
            }

            const actor = session.admin_actor || 'admin';
            const db = env.DB;

            // Route matching
            if (path === '/api/admin/me' && method === 'GET') {
                return json({ authenticated: true, sub: actor, expiresAt: session.expires_at });
            }

            if (path === '/api/admin/dashboard' && method === 'GET') {
                return await api.handleDashboard(db);
            }

            if (path === '/api/admin/validate-mls' && method === 'GET') {
                return await api.handleValidateMls(db, url);
            }

            if (path === '/api/admin/accounts' && method === 'GET') {
                return await api.handleListAccounts(db, url);
            }

            if (path === '/api/admin/accounts' && method === 'POST') {
                const body = await request.json();
                return await api.handleCreateAccount(db, body, actor);
            }

            // /api/admin/accounts/:id
            const accMatch = path.match(/^\/api\/admin\/accounts\/([^\/]+)$/);
            if (accMatch) {
                const accountId = accMatch[1];
                if (method === 'GET') return await api.handleGetAccount(db, accountId);
                if (method === 'PATCH') {
                    const body = await request.json();
                    return await api.handleUpdateAccount(db, accountId, body, actor);
                }
            }

            // /api/admin/accounts/:id/sites
            const accSitesMatch = path.match(/^\/api\/admin\/accounts\/([^\/]+)\/sites$/);
            if (accSitesMatch && method === 'POST') {
                const accountId = accSitesMatch[1];
                const body = await request.json();
                return await api.handleCreateSite(db, accountId, body, actor);
            }

            // /api/admin/accounts/:id/members
            const accMembersMatch = path.match(/^\/api\/admin\/accounts\/([^\/]+)\/members$/);
            if (accMembersMatch) {
                const accountId = accMembersMatch[1];
                if (method === 'GET') return await api.handleListAccountMembers(db, accountId);
                if (method === 'POST') {
                    const body = await request.json();
                    return await api.handleCreateAccountMemberInvite(db, accountId, body, actor, env);
                }
            }

            // /api/admin/sites/:id
            const siteMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)$/);
            if (siteMatch) {
                const siteId = siteMatch[1];
                if (method === 'PATCH') {
                    const body = await request.json();
                    return await api.handleUpdateSite(db, siteId, body, actor);
                }
            }

            // /api/admin/sites/:id/domains
            const siteDomMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/domains$/);
            if (siteDomMatch) {
                const siteId = siteDomMatch[1];
                if (method === 'GET') return await api.handleListDomains(db, siteId);
                if (method === 'POST') {
                    const body = await request.json();
                    return await api.handleAddDomain(db, siteId, body, actor);
                }
            }

            // /api/admin/launch-checks
            if (path === '/api/admin/launch-checks') {
                if (method === 'GET') return await api.handleListLaunchChecks(db);
                if (method === 'POST') {
                    const body = await request.json();
                    return await api.handleRecordLaunchCheck(db, body, actor);
                }
            }

            // /api/admin/readiness
            if (path === '/api/admin/readiness' && method === 'GET') {
                return await api.handleGetReadiness(db, env);
            }

            // /api/admin/domains/diagnostic
            if (path === '/api/admin/domains/diagnostic' && method === 'GET') {
                return await api.handleGetDomainDiagnostic(env);
            }

            // /api/admin/domains/fallback-origin
            if (path === '/api/admin/domains/fallback-origin' && method === 'GET') {
                return await api.handleGetFallbackOrigin(env);
            }
            if (path === '/api/admin/domains/fallback-origin' && method === 'PUT') {
                const body = await request.json().catch(() => ({}));
                return await api.handleUpdateFallbackOrigin(env, body);
            }

            // /api/admin/domains/:id
            const domMatch = path.match(/^\/api\/admin\/domains\/([^\/]+)$/);
            if (domMatch) {
                const domainId = domMatch[1];
                if (method === 'PATCH') {
                    const body = await request.json();
                    return await api.handleUpdateDomain(db, domainId, body, actor);
                }
                if (method === 'DELETE') {
                    return await api.handleDeleteDomain(db, domainId, actor);
                }
            }

            // /api/admin/sites/:id/branding
            const brandMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/branding$/);
            if (brandMatch) {
                const siteId = brandMatch[1];
                if (method === 'GET') return await api.handleGetBranding(db, siteId);
                if (method === 'PUT') {
                    const body = await request.json();
                    return await api.handleUpdateBranding(db, siteId, body, actor);
                }
            }

            // /api/admin/sites/:id/widgets
            const widgetListMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/widgets$/);
            if (widgetListMatch && method === 'GET') {
                const siteId = widgetListMatch[1];
                return await api.handleGetWidgets(db, siteId);
            }

            // /api/admin/sites/:id/widgets/:type
            const widgetMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/widgets\/([^\/]+)$/);
            if (widgetMatch && method === 'PUT') {
                const siteId = widgetMatch[1];
                const widgetType = widgetMatch[2];
                const body = await request.json();
                return await api.handleUpdateWidget(db, siteId, widgetType, body, actor);
            }

            // /api/admin/sites/:id/embed
            const embedMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/embed$/);
            if (embedMatch && method === 'GET') {
                const siteId = embedMatch[1];
                return await api.handleGetEmbed(db, siteId);
            }

            // /api/admin/accounts/:id/entitlement
            const entMatch = path.match(/^\/api\/admin\/accounts\/([^\/]+)\/entitlement$/);
            if (entMatch) {
                const accountId = entMatch[1];
                if (method === 'GET') return await api.handleGetAccountEntitlement(db, accountId);
                if (method === 'PUT') {
                    const body = await request.json();
                    return await api.handleUpdateAccountEntitlement(db, accountId, body, actor);
                }
            }

            // /api/admin/sites/:id/website
            const webMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/website$/);
            if (webMatch) {
                const siteId = webMatch[1];
                if (method === 'GET') return await api.handleGetWebsiteConfig(db, siteId, env);
                if (method === 'PUT') {
                    const body = await request.json();
                    return await api.handleUpdateWebsiteConfig(db, siteId, body, actor, env);
                }
            }

            // /api/admin/sites/:id/website/preview-token
            const prevMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/website\/preview-token$/);
            if (prevMatch && method === 'POST') {
                const siteId = prevMatch[1];
                return await api.handleCreateWebsitePreviewToken(db, siteId, env);
            }

            // /api/admin/sites/:id/domains/bindings
            const domBindMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/domains\/bindings$/);
            if (domBindMatch && method === 'GET') {
                const siteId = domBindMatch[1];
                return await api.handleListDomainBindings(db, siteId);
            }

            // /api/admin/sites/:id/domains/prepare
            const prepMatch = path.match(/^\/api\/admin\/sites\/([^\/]+)\/domains\/prepare$/);
            if (prepMatch && method === 'POST') {
                const siteId = prepMatch[1];
                const body = await request.json();
                return await api.handlePrepareDomainBinding(db, siteId, body, actor, env);
            }

            // /api/admin/domain-bindings/:id/refresh
            const refMatch = path.match(/^\/api\/admin\/domain-bindings\/([^\/]+)\/refresh$/);
            if (refMatch && method === 'POST') {
                const bindingId = refMatch[1];
                return await api.handleRefreshDomainBinding(db, bindingId, actor, env);
            }

            // /api/admin/domain-bindings/:id
            const delMatch = path.match(/^\/api\/admin\/domain-bindings\/([^\/]+)$/);
            if (delMatch && method === 'DELETE') {
                const bindingId = delMatch[1];
                return await api.handleRemoveDomainBinding(db, bindingId, actor, env);
            }

            // /api/admin/domains/diagnostic
            if (path === '/api/admin/domains/diagnostic' && method === 'GET') {
                return await api.handleGetDomainDiagnostic(env);
            }

            return error('API endpoint not found', 404);
        }

        return new Response('Not Found', { status: 404, headers: SECURITY_HEADERS });
    }
};
