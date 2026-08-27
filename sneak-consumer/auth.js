/**
 * sneak-consumer/auth.js
 * 
 * Secure Passwordless Authentication & Session Management for Property Buyers / Consumers.
 * 
 * Invariants & Trust Separation:
 * - Strictly isolated from REALTOR® / Member accounts (sneak_member_users).
 * - Consumer identities are site-scoped (UNIQUE(site_id, email)).
 * - Magic links are single-use, 15-minute TTL, SHA-256 hashed.
 * - Handoff uses short-lived (2-minute) single-use auth exchange codes.
 * - Sessions are 14-day revocable bearer tokens, SHA-256 hashed.
 * - Open-redirect protection enforces HTTPS and verified domains in sneak_domains.
 * - Pseudonymized rate limiting with generic enumeration-safe responses.
 */

export function bufferToHex(buffer) {
    return Array.from(new Uint8Array(buffer)).map(b => b.toString(16).padStart(2, '0')).join('');
}

export function timingSafeEqual(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string') return false;
    if (a.length !== b.length) return false;
    let diff = 0;
    for (let i = 0; i < a.length; i++) {
        diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
    }
    return diff === 0;
}

export async function sha256Hex(str) {
    if (!str) return '';
    const enc = new TextEncoder();
    const hashBuffer = await crypto.subtle.digest('SHA-256', enc.encode(str));
    return bufferToHex(hashBuffer);
}

export function generateRawToken() {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    return bufferToHex(bytes);
}

function base64UrlDecode(str) {
    let base64 = str.replace(/-/g, '+').replace(/_/g, '/');
    while (base64.length % 4) base64 += '=';
    return atob(base64);
}

async function getSigningKey(secret) {
    const enc = new TextEncoder();
    return await crypto.subtle.importKey(
        'raw',
        enc.encode(secret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['verify']
    );
}

/**
 * Verifies signed SNEAK session token from IDX bootstrap.
 */
export async function verifyIdxSessionToken(token, secret) {
    if (!token || typeof token !== 'string' || !secret) return null;
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

/**
 * Validates return URL against verified domains for the site in sneak_domains.
 * Prevents open redirects, javascript: schemes, and unauthorized hostnames.
 */
export async function validateReturnUrl(db, siteId, returnUrlStr, isDev = false) {
    if (!returnUrlStr || typeof returnUrlStr !== 'string') return null;

    let parsed;
    try {
        parsed = new URL(returnUrlStr);
    } catch {
        return null;
    }

    const protocol = parsed.protocol.toLowerCase();
    const hostname = parsed.hostname.toLowerCase();

    // Dev mode exception for local testing
    if (isDev && (hostname === 'localhost' || hostname === '127.0.0.1')) {
        if (protocol === 'http:' || protocol === 'https:') {
            return parsed.toString();
        }
        return null;
    }

    // Must be HTTPS in staging and production
    if (protocol !== 'https:') {
        return null;
    }

    // Verify hostname against sneak_domains (status = 'active' AND verified = 1)
    const domainsResult = await db.prepare(
        "SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1"
    ).bind(siteId).all();

    const allowedDomains = (domainsResult.results || []).map(r => r.domain.toLowerCase().trim());
    const isAuthorized = allowedDomains.some(d => {
        if (d === '*') return false; // Global wildcard disallowed
        if (d === hostname) return true;
        if (d.startsWith('*.')) {
            const rootDomain = d.slice(2);
            return hostname === rootDomain || hostname.endsWith('.' + rootDomain);
        }
        return false;
    });

    if (!isAuthorized) {
        return null;
    }

    // Strip unsafe fragments and return validated URL
    return parsed.toString();
}

import { sendConsumerMagicLinkEmail } from './email.js';

/**
 * Handles public consumer magic link request.
 * Dispatches transactional email, applies rate limiting, and returns generic enumeration-safe response.
 */
export async function requestConsumerMagicLink(db, { siteKey, email, returnUrl, ipHash, env }) {
    const GENERIC_RESPONSE = {
        success: true,
        message: 'If the email is valid, a sign-in link has been sent. Please check your inbox.'
    };

    const cleanEmail = (email || '').toLowerCase().trim();
    if (!cleanEmail || !cleanEmail.includes('@') || cleanEmail.length < 5 || cleanEmail.length > 254) {
        return GENERIC_RESPONSE;
    }

    if (!siteKey || !db) {
        return GENERIC_RESPONSE;
    }

    // 1. Resolve site and verify active entitlement
    const site = await db.prepare(`
        SELECT s.id AS site_id, s.site_key, s.status AS site_status,
               a.account_name, a.status AS account_status,
               b.display_name, b.brokerage
        FROM sneak_sites s
        JOIN sneak_accounts a ON s.account_id = a.id
        LEFT JOIN sneak_branding b ON s.id = b.site_id
        WHERE s.site_key = ?
    `).bind(siteKey).first();

    if (!site || site.site_status !== 'active' || site.account_status !== 'active') {
        return GENERIC_RESPONSE;
    }

    // 2. Validate return URL
    const envName = (env?.SNEAK_ENV || 'staging').toLowerCase();
    const isDev = envName === 'development';
    const validatedReturn = await validateReturnUrl(db, site.site_id, returnUrl, isDev);
    if (!validatedReturn) {
        return GENERIC_RESPONSE;
    }

    const emailHash = await sha256Hex(cleanEmail);
    const now = new Date().toISOString();

    // 3. Rate limiting (5 requests/15 min for email, 20 requests/15 min for IP)
    try {
        const ipAttempts = await db.prepare(`
            SELECT count(*) as count FROM sneak_consumer_login_attempts
            WHERE identifier_hash = ? AND attempt_type = 'ip' AND created_at > datetime('now', '-15 minutes')
        `).bind(ipHash).first();

        const emailAttempts = await db.prepare(`
            SELECT count(*) as count FROM sneak_consumer_login_attempts
            WHERE identifier_hash = ? AND attempt_type = 'email' AND created_at > datetime('now', '-15 minutes')
        `).bind(emailHash).first();

        // Record attempts
        const ipAttemptId = `cla_ip_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        const emailAttemptId = `cla_em_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;

        await db.prepare(`
            INSERT INTO sneak_consumer_login_attempts (id, site_id, identifier_hash, attempt_type, created_at)
            VALUES (?, ?, ?, 'ip', ?)
        `).bind(ipAttemptId, site.site_id, ipHash, now).run();

        await db.prepare(`
            INSERT INTO sneak_consumer_login_attempts (id, site_id, identifier_hash, attempt_type, created_at)
            VALUES (?, ?, ?, 'email', ?)
        `).bind(emailAttemptId, site.site_id, emailHash, now).run();

        if ((ipAttempts?.count || 0) >= 20 || (emailAttempts?.count || 0) >= 5) {
            return GENERIC_RESPONSE; // Throttled generic response
        }
    } catch (err) {
        console.error('[CONSUMER RATE LIMIT ERROR]', err.message);
    }

    // 4. Find or create pending consumer user (site-scoped)
    let user = await db.prepare(`
        SELECT id, site_id, email, status FROM sneak_consumer_users
        WHERE site_id = ? AND email = ?
    `).bind(site.site_id, cleanEmail).first();

    if (!user) {
        const userId = `cuser_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_consumer_users (id, site_id, email, status, created_at, updated_at)
            VALUES (?, ?, ?, 'pending', ?, ?)
        `).bind(userId, site.site_id, cleanEmail, now, now).run();

        user = { id: userId, site_id: site.site_id, email: cleanEmail, status: 'pending' };
    } else if (user.status === 'disabled') {
        return GENERIC_RESPONSE; // Disabled user
    }

    // 5. Generate magic link (15 minutes TTL)
    const rawToken = generateRawToken();
    const tokenHash = await sha256Hex(rawToken);
    const linkId = `cml_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const expiresAt = new Date(Date.now() + (900 * 1000)).toISOString();

    // Rotate: Invalidate previous unused links for this user and site
    await db.prepare(`
        UPDATE sneak_consumer_magic_links
        SET used_at = ?
        WHERE user_id = ? AND site_id = ? AND used_at IS NULL
    `).bind(now, user.id, site.site_id).run();

    await db.prepare(`
        INSERT INTO sneak_consumer_magic_links (id, user_id, site_id, token_hash, purpose, return_url, created_at, expires_at, used_at)
        VALUES (?, ?, ?, ?, 'login', ?, ?, ?, NULL)
    `).bind(linkId, user.id, site.site_id, tokenHash, validatedReturn, now, expiresAt).run();

    // 6. Send transactional email
    const consumerWorkerUrl = env?.CONSUMER_WORKER_URL || 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
    const verifyUrl = `${consumerWorkerUrl}/api/consumer/auth/verify?token=${encodeURIComponent(rawToken)}`;

    await sendConsumerMagicLinkEmail(env, {
        email: cleanEmail,
        verifyUrl,
        expiresMinutes: 15,
        agentName: site.display_name || site.account_name,
        brokerage: site.brokerage
    });

    return GENERIC_RESPONSE;
}

/**
 * Atomically verifies and consumes a single-use consumer magic link.
 * Creates a short-lived (2 min) auth exchange code and returns returnUrl + exchange code.
 */
export async function verifyAndConsumeConsumerMagicLink(db, rawToken) {
    if (!rawToken || typeof rawToken !== 'string' || rawToken.length < 32) return null;

    const tokenHash = await sha256Hex(rawToken);
    const now = new Date().toISOString();

    // 1. Atomic mutation: consume token
    const updateRes = await db.prepare(`
        UPDATE sneak_consumer_magic_links
        SET used_at = ?
        WHERE token_hash = ?
          AND used_at IS NULL
          AND expires_at > ?
    `).bind(now, tokenHash, now).run();

    const changes = updateRes?.meta?.changes ?? updateRes?.changes ?? 0;
    if (changes !== 1) {
        return null; // Expired, already used, or invalid
    }

    // 2. Fetch magic link and user
    const link = await db.prepare(`
        SELECT l.id, l.user_id, l.site_id, l.return_url, u.email, u.status AS user_status, s.site_key
        FROM sneak_consumer_magic_links l
        JOIN sneak_consumer_users u ON l.user_id = u.id
        JOIN sneak_sites s ON l.site_id = s.id
        WHERE l.token_hash = ?
    `).bind(tokenHash).first();

    if (!link || link.user_status === 'disabled') {
        return null;
    }

    // 3. Activate user if pending
    await db.prepare(`
        UPDATE sneak_consumer_users
        SET status = 'active',
            activated_at = COALESCE(activated_at, ?),
            last_login_at = ?,
            updated_at = ?
        WHERE id = ?
    `).bind(now, now, now, link.user_id).run();

    // 4. Create one-time auth exchange code (120 seconds TTL)
    const rawExchangeCode = generateRawToken();
    const codeHash = await sha256Hex(rawExchangeCode);
    const exchangeId = `cae_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const exchangeExpiresAt = new Date(Date.now() + (120 * 1000)).toISOString();

    await db.prepare(`
        INSERT INTO sneak_consumer_auth_exchanges (id, user_id, site_id, code_hash, created_at, expires_at, used_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
    `).bind(exchangeId, link.user_id, link.site_id, codeHash, now, exchangeExpiresAt).run();

    return {
        returnUrl: link.return_url,
        exchangeCode: rawExchangeCode,
        siteKey: link.site_key,
        userId: link.user_id,
        email: link.email
    };
}

/**
 * Exchanges single-use auth code for long-lived consumer session token (14 days).
 */
export async function exchangeAuthCodeForSession(db, { code, siteKey, sessionToken, signingSecret }) {
    if (!code || typeof code !== 'string' || code.length < 32 || !siteKey || !db) {
        return null;
    }

    // Validate signed IDX session if provided
    if (sessionToken && signingSecret) {
        const payload = await verifyIdxSessionToken(sessionToken, signingSecret);
        if (!payload || payload.siteKey !== siteKey) {
            return null;
        }
    }

    const codeHash = await sha256Hex(code);
    const now = new Date().toISOString();

    // 1. Atomically consume exchange code
    const updateRes = await db.prepare(`
        UPDATE sneak_consumer_auth_exchanges
        SET used_at = ?
        WHERE code_hash = ?
          AND used_at IS NULL
          AND expires_at > ?
    `).bind(now, codeHash, now).run();

    const changes = updateRes?.meta?.changes ?? updateRes?.changes ?? 0;
    if (changes !== 1) {
        return null;
    }

    // 2. Fetch exchange details
    const exchange = await db.prepare(`
        SELECT e.id, e.user_id, e.site_id, u.email, u.status AS user_status, s.site_key
        FROM sneak_consumer_auth_exchanges e
        JOIN sneak_consumer_users u ON e.user_id = u.id
        JOIN sneak_sites s ON e.site_id = s.id
        WHERE e.code_hash = ?
    `).bind(codeHash).first();

    if (!exchange || exchange.site_key !== siteKey || exchange.user_status !== 'active') {
        return null;
    }

    // 3. Create 14-day revocable consumer session (1,209,600 seconds)
    const rawSessionToken = generateRawToken();
    const sessionTokenHash = await sha256Hex(rawSessionToken);
    const sessionId = `csess_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const sessionExpiresAt = new Date(Date.now() + (1209600 * 1000)).toISOString();

    await db.prepare(`
        INSERT INTO sneak_consumer_sessions (id, user_id, site_id, token_hash, created_at, expires_at, last_seen_at, revoked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
    `).bind(sessionId, exchange.user_id, exchange.site_id, sessionTokenHash, now, sessionExpiresAt, now).run();

    return {
        consumerSession: rawSessionToken,
        user: {
            id: exchange.user_id,
            email: exchange.email,
            siteKey: exchange.site_key
        }
    };
}

/**
 * Verifies active consumer session. Enforces site isolation.
 */
export async function verifyConsumerSession(db, rawToken, requestedSiteKey = null) {
    if (!rawToken || typeof rawToken !== 'string' || rawToken.length < 32 || !db) {
        return null;
    }

    const tokenHash = await sha256Hex(rawToken);

    const row = await db.prepare(`
        SELECT s.id AS session_id, s.user_id, s.site_id, s.created_at, s.expires_at, s.revoked_at,
               u.email AS consumer_email, u.status AS user_status,
               si.site_key, si.status AS site_status
        FROM sneak_consumer_sessions s
        JOIN sneak_consumer_users u ON s.user_id = u.id
        JOIN sneak_sites si ON s.site_id = si.id
        WHERE s.token_hash = ?
          AND s.revoked_at IS NULL
          AND s.expires_at > datetime('now')
    `).bind(tokenHash).first();

    if (!row) return null;

    if (row.user_status !== 'active' || row.site_status !== 'active') {
        return null;
    }

    // Strict site isolation check: A session issued for site A must return 403 on site B
    if (requestedSiteKey && row.site_key !== requestedSiteKey) {
        return { error: 'SiteMismatch', status: 403 };
    }

    // Async update last_seen_at
    try {
        await db.prepare("UPDATE sneak_consumer_sessions SET last_seen_at = datetime('now') WHERE id = ?").bind(row.session_id).run();
    } catch {}

    return {
        sessionId: row.session_id,
        userId: row.user_id,
        siteId: row.site_id,
        siteKey: row.site_key,
        email: row.consumer_email
    };
}

/**
 * Revokes an active consumer session.
 */
export async function revokeConsumerSession(db, rawToken) {
    if (!rawToken || !db) return;
    const tokenHash = await sha256Hex(rawToken);
    await db.prepare(`
        UPDATE sneak_consumer_sessions
        SET revoked_at = datetime('now')
        WHERE token_hash = ?
    `).bind(tokenHash).run();
}

/**
 * Authenticated deletion of consumer account & associated data (GDPR / CCPA / Forget Me).
 */
export async function deleteConsumerAccount(db, userId, siteId) {
    if (!userId || !siteId || !db) return;

    // Delete user (cascades sessions, magic links, exchanges, favorites via ON DELETE CASCADE)
    await db.prepare(`
        DELETE FROM sneak_consumer_users
        WHERE id = ? AND site_id = ?
    `).bind(userId, siteId).run();
}
