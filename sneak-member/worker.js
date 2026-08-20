/**
 * sneak-member/worker.js
 * 
 * Dedicated Cloudflare Worker for SNEAK Member Self-Service Portal,
 * Passwordless Magic Link Auth, and Stripe Test-Mode Billing.
 */

import { renderMemberUI } from './ui.js';
import {
    createMagicLink,
    verifyAndConsumeMagicLink,
    verifyMemberSession,
    revokeMemberSession,
    getSessionTokenFromRequest,
    validateMemberCsrf
} from './auth.js';
import {
    handleMemberOverview,
    handleListMemberDomains,
    handleAddMemberDomain,
    handleDeleteMemberDomain,
    handleGetMemberBranding,
    handleUpdateMemberBranding,
    handleGetMemberWidgets,
    handleUpdateMemberWidget,
    handleGetMemberEmbed,
    handleGetMemberLeads,
    handleGetMemberBilling
} from './api.js';
import {
    verifyStripeWebhookSignature,
    handleStripeWebhookEvent
} from './billing.js';

const SECURITY_HEADERS = {
    'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline' https://fonts.googleapis.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; connect-src 'self'; frame-ancestors 'none';",
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

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;
        const method = request.method.toUpperCase();

        // 1. Health Check
        if (path === '/health' || (path === '/' && request.headers.get('Accept') === 'application/json')) {
            return json({
                status: 'healthy',
                worker: 'sneak-idx-member-staging',
                timestamp: new Date().toISOString()
            });
        }

        // 2. Stripe Webhook Endpoint (No CSRF / No Member Cookie)
        if (path === '/api/stripe/webhook' && method === 'POST') {
            const signatureHeader = request.headers.get('Stripe-Signature');
            const webhookSecret = env.STRIPE_WEBHOOK_SECRET;

            if (!webhookSecret) {
                return error('STRIPE_WEBHOOK_SECRET is not configured on Member Worker.', 500, 'ConfigurationError');
            }

            const rawBody = await request.text();
            const valid = await verifyStripeWebhookSignature(rawBody, signatureHeader, webhookSecret);
            if (!valid) {
                return error('Invalid Stripe signature or timestamp expired', 400, 'InvalidSignature');
            }

            try {
                const event = JSON.parse(rawBody);
                const result = await handleStripeWebhookEvent(env.DB, event);
                return json({ received: true, ...result });
            } catch (err) {
                console.error('[STRIPE WEBHOOK ERROR]', err);
                return error('Webhook processing error: ' + err.message, 500);
            }
        }

        // 3. Passwordless Magic Link Request (POST /api/member/auth/magic-link)
        if (path === '/api/member/auth/magic-link' && method === 'POST') {
            if (!validateMemberCsrf(request)) {
                return error('CSRF verification failed', 403, 'Forbidden');
            }

            try {
                const body = await request.json();
                const result = await createMagicLink(env.DB, body?.email, 'login', 900);
                return json(result);
            } catch (err) {
                return error('Malformed request', 400);
            }
        }

        // 4. Magic Link Consumption & Verification (GET /api/member/auth/verify)
        if (path === '/api/member/auth/verify' && method === 'GET') {
            const token = url.searchParams.get('token');
            if (!token) return error('Missing verification token', 400);

            const result = await verifyAndConsumeMagicLink(env.DB, token);
            if (!result) {
                return error('Invalid, expired, or already used magic link.', 401, 'InvalidToken');
            }

            const cookie = `__Host-sneak_member_session=${encodeURIComponent(result.sessionToken)}; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=604800`;

            return json({
                success: true,
                message: 'Authenticated successfully',
                user: result.user
            }, 200, { 'Set-Cookie': cookie });
        }

        // 5. Member Logout (POST /api/member/auth/logout)
        if (path === '/api/member/auth/logout' && method === 'POST') {
            const rawToken = getSessionTokenFromRequest(request);
            if (rawToken && env.DB) {
                await revokeMemberSession(env.DB, rawToken);
            }

            const cookie = `__Host-sneak_member_session=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0`;
            return json({ success: true, message: 'Logged out' }, 200, { 'Set-Cookie': cookie });
        }

        // 6. Member Portal UI (GET /)
        if (path === '/' || path === '/index.html' || path === '/domains' || path === '/branding' || path === '/billing') {
            return new Response(renderMemberUI(), {
                headers: {
                    'Content-Type': 'text/html; charset=UTF-8',
                    'Cache-Control': 'no-store',
                    ...SECURITY_HEADERS
                }
            });
        }

        // 7. Protected Member API Routes (/api/member/*)
        if (path.startsWith('/api/member/')) {
            // Member Session Verification
            const rawToken = getSessionTokenFromRequest(request);
            if (!rawToken) {
                return error('Authentication required. Please sign in.', 401, 'Unauthorized');
            }

            const memberContext = await verifyMemberSession(env.DB, rawToken);
            if (!memberContext) {
                return error('Invalid or expired member session.', 401, 'Unauthorized');
            }

            // Same-Origin CSRF Check for Mutations
            if (!validateMemberCsrf(request)) {
                return error('CSRF verification failed', 403, 'Forbidden');
            }

            // Route protected requests
            if (path === '/api/member/overview' && method === 'GET') {
                return handleMemberOverview(env.DB, memberContext);
            }

            if (path === '/api/member/domains' && method === 'GET') {
                return handleListMemberDomains(env.DB, memberContext);
            }

            if (path === '/api/member/domains' && method === 'POST') {
                const body = await request.json();
                return handleAddMemberDomain(env.DB, memberContext, body);
            }

            const domMatch = path.match(/^\/api\/member\/domains\/([^/]+)$/);
            if (domMatch && method === 'DELETE') {
                return handleDeleteMemberDomain(env.DB, memberContext, domMatch[1]);
            }

            if (path === '/api/member/branding' && method === 'GET') {
                return handleGetMemberBranding(env.DB, memberContext);
            }

            if (path === '/api/member/branding' && method === 'PUT') {
                const body = await request.json();
                return handleUpdateMemberBranding(env.DB, memberContext, body);
            }

            if (path === '/api/member/widgets' && method === 'GET') {
                return handleGetMemberWidgets(env.DB, memberContext);
            }

            const widMatch = path.match(/^\/api\/member\/widgets\/([^/]+)$/);
            if (widMatch && method === 'PUT') {
                const body = await request.json();
                return handleUpdateMemberWidget(env.DB, memberContext, widMatch[1], body);
            }

            if (path === '/api/member/embed' && method === 'GET') {
                return handleGetMemberEmbed(env.DB, memberContext);
            }

            if (path === '/api/member/leads' && method === 'GET') {
                return handleGetMemberLeads(env.DB, memberContext);
            }

            if (path === '/api/member/billing' && method === 'GET') {
                return handleGetMemberBilling(env.DB, memberContext);
            }

            if (path === '/api/member/billing/checkout' && method === 'POST') {
                if (!env.STRIPE_SECRET_KEY) {
                    return error('STRIPE TEST CONFIGURATION REQUIRED (STRIPE_SECRET_KEY missing)', 503, 'StripeNotConfigured');
                }
                return error('Stripe test checkout not initialized', 501);
            }

            if (path === '/api/member/billing/portal' && method === 'POST') {
                if (!env.STRIPE_SECRET_KEY) {
                    return error('STRIPE TEST CONFIGURATION REQUIRED (STRIPE_SECRET_KEY missing)', 503, 'StripeNotConfigured');
                }
                return error('Stripe customer portal not initialized', 501);
            }

            return error('Member API endpoint not found', 404);
        }

        return new Response('Not Found', { status: 404, headers: SECURITY_HEADERS });
    }
};
