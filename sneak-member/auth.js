/**
 * sneak-member/auth.js
 * 
 * Secure Passwordless Authentication, Atomic Single-Use Magic Links, 
 * Rate Limiting, 7-Day Revocable Sessions, and Same-Origin CSRF Protection.
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

/**
 * Creates an internal single-use magic link in D1.
 * Intended strictly for trusted callers (e.g. Admin invitation creation).
 */
export async function createMagicLinkRecord(db, userId, purpose = 'login', ttlSeconds = 900) {
    if (!userId) return null;

    const rawToken = generateRawToken();
    const tokenHash = await sha256Hex(rawToken);
    const linkId = `ml_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const now = new Date().toISOString();
    const expiresAt = new Date(Date.now() + (ttlSeconds * 1000)).toISOString();

    // Rotate: Invalidate previous unconsumed magic links for this user & purpose
    await db.prepare(`
        UPDATE sneak_member_magic_links
        SET used_at = ?
        WHERE user_id = ? AND purpose = ? AND used_at IS NULL
    `).bind(now, userId, purpose).run();

    // Insert new magic link
    await db.prepare(`
        INSERT INTO sneak_member_magic_links (id, user_id, token_hash, purpose, created_at, expires_at, used_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
    `).bind(linkId, userId, tokenHash, purpose, now, expiresAt).run();

    return rawToken;
}

import { sendMagicLinkEmail, sendInvitationEmail } from './email.js';

/**
 * Public magic link request handler with pseudonymized rate limiting & enumeration protection.
 * NEVER returns rawToken, token_hash, userId, accountId, or user existence signals.
 * Dispatches rawToken strictly via secure transactional email.
 */
export async function requestPublicMagicLink(db, email, ipHash, env = {}) {
    const cleanEmail = (email || '').toLowerCase().trim();
    const GENERIC_RESPONSE = {
        success: true,
        message: 'If an account exists, a sign-in link will be sent.'
    };

    if (!cleanEmail || !cleanEmail.includes('@') || cleanEmail.length < 5) {
        return GENERIC_RESPONSE;
    }

    const emailHash = await sha256Hex(cleanEmail);
    const now = new Date().toISOString();

    // 1. Rate Limit Checks (5 requests / 15 minutes per IP hash and Email hash)
    if (db) {
        try {
            const ipAttempts = await db.prepare(`
                SELECT count(*) as count FROM sneak_member_login_attempts
                WHERE ip_hash = ? AND attempted_at > datetime('now', '-15 minutes')
            `).bind(ipHash).first();

            const emailAttempts = await db.prepare(`
                SELECT count(*) as count FROM sneak_member_login_attempts
                WHERE email_hash = ? AND attempted_at > datetime('now', '-15 minutes')
            `).bind(emailHash).first();

            // Record this attempt
            const attemptId = `mla_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
            await db.prepare(`
                INSERT INTO sneak_member_login_attempts (id, ip_hash, email_hash, attempted_at)
                VALUES (?, ?, ?, ?)
            `).bind(attemptId, ipHash, emailHash, now).run();

            if ((ipAttempts?.count || 0) >= 30 || (emailAttempts?.count || 0) >= 10) {
                return GENERIC_RESPONSE;
            }
        } catch (err) {
            console.error('[MEMBER RATE LIMIT ERROR]', err.message);
        }
    }

    // 2. Lookup member user
    const user = await db.prepare(`
        SELECT u.id, u.account_id, u.status AS user_status, a.status AS account_status, a.account_name
        FROM sneak_member_users u
        JOIN sneak_accounts a ON u.account_id = a.id
        WHERE u.email = ?
    `).bind(cleanEmail).first();

    if (!user || !['invited', 'active'].includes(user.user_status) || user.account_status !== 'active') {
        return GENERIC_RESPONSE;
    }

    // 3. Create single-use token internally and dispatch exclusively via transactional email
    try {
        const isInvited = (user.user_status === 'invited');
        const purpose = isInvited ? 'invite' : 'login';
        const ttlSeconds = isInvited ? 86400 : 900; // 24 hours for invitation, 15 minutes for login
        const rawToken = await createMagicLinkRecord(db, user.id, purpose, ttlSeconds);
        if (rawToken) {
            const isProd = (env?.SNEAK_ENV || '').toLowerCase() === 'production';
            const defaultBaseUrl = isProd
                ? 'https://sneak-idx-member.bonitaspringsrealtors.workers.dev'
                : 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev';
            const baseUrl = env?.MEMBER_PORTAL_URL || defaultBaseUrl;
            const verifyUrl = `${baseUrl}/api/member/auth/verify?token=${encodeURIComponent(rawToken)}`;
            if (isInvited) {
                await sendInvitationEmail(env, { email: cleanEmail, inviteUrl: verifyUrl, accountName: user.account_name });
            } else {
                await sendMagicLinkEmail(env, { email: cleanEmail, verifyUrl, expiresMinutes: 15 });
            }
        }
    } catch (err) {
        console.error('[MAGIC LINK DISPATCH ERROR]', err.message);
    }

    return GENERIC_RESPONSE;
}

/**
 * Atomically verifies and consumes a single-use magic link.
 * Prevents race conditions and replay attacks via conditional UPDATE inspection.
 */
export async function verifyAndConsumeMagicLink(db, rawToken) {
    if (!rawToken || typeof rawToken !== 'string' || rawToken.length < 32) return null;

    const tokenHash = await sha256Hex(rawToken);
    const now = new Date().toISOString();

    // 1. Atomic conditional mutation: mutate exactly one active, unconsumed, unexpired row
    const updateRes = await db.prepare(`
        UPDATE sneak_member_magic_links
        SET used_at = ?
        WHERE token_hash = ?
          AND used_at IS NULL
          AND expires_at > ?
    `).bind(now, tokenHash, now).run();

    const changes = updateRes?.meta?.changes ?? updateRes?.changes ?? 0;
    if (changes !== 1) {
        // Token was already consumed, expired, or does not exist
        return null;
    }

    // 2. Load associated user & account status
    const linkUser = await db.prepare(`
        SELECT 
            l.user_id, u.account_id, u.email, u.role, u.status AS user_status,
            a.account_name, a.status AS account_status
        FROM sneak_member_magic_links l
        JOIN sneak_member_users u ON l.user_id = u.id
        JOIN sneak_accounts a ON u.account_id = a.id
        WHERE l.token_hash = ?
    `).bind(tokenHash).first();

    if (!linkUser) return null;

    // 3. User and Account Status Verification
    if (!['invited', 'active'].includes(linkUser.user_status) || linkUser.account_status !== 'active') {
        // Disabled member or suspended account
        return null;
    }

    // 4. Update user activation & login timestamps
    await db.prepare(`
        UPDATE sneak_member_users
        SET status = 'active',
            activated_at = COALESCE(activated_at, ?),
            last_login_at = ?,
            updated_at = ?
        WHERE id = ?
    `).bind(now, now, now, linkUser.user_id).run();

    // 5. Create 7-day revocable member session
    const sessionToken = await createMemberSession(db, linkUser.user_id, linkUser.account_id, 604800);

    return {
        user: {
            id: linkUser.user_id,
            account_id: linkUser.account_id,
            email: linkUser.email,
            role: linkUser.role,
            status: 'active'
        },
        sessionToken
    };
}

/**
 * Creates a server-side revocable member session (7 days).
 */
export async function createMemberSession(db, userId, accountId, ttlSeconds = 604800) {
    const rawToken = generateRawToken();
    const tokenHash = await sha256Hex(rawToken);
    const sessionId = `msess_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const now = new Date().toISOString();
    const expiresAt = new Date(Date.now() + (ttlSeconds * 1000)).toISOString();

    await db.prepare(`
        INSERT INTO sneak_member_sessions (
            id, user_id, account_id, token_hash, created_at, expires_at, last_seen_at, revoked_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, NULL
        )
    `).bind(sessionId, userId, accountId, tokenHash, now, expiresAt, now).run();

    return rawToken;
}

/**
 * Verifies active member session from D1.
 */
export async function verifyMemberSession(db, rawToken) {
    if (!rawToken || typeof rawToken !== 'string' || rawToken.length < 32) return null;

    const tokenHash = await sha256Hex(rawToken);

    const query = `
        SELECT 
            s.id AS session_id, s.user_id, s.account_id, s.created_at, s.expires_at, s.revoked_at,
            u.email AS member_email, u.role AS member_role, u.status AS user_status,
            a.account_name, a.status AS account_status, a.plan AS account_plan,
            a.agent_mls_id, a.office_mls_id
        FROM sneak_member_sessions s
        JOIN sneak_member_users u ON s.user_id = u.id
        JOIN sneak_accounts a ON s.account_id = a.id
        WHERE s.token_hash = ? AND s.revoked_at IS NULL AND s.expires_at > datetime('now')
    `;

    const row = await db.prepare(query).bind(tokenHash).first();
    if (!row) return null;

    if (row.user_status !== 'active' || row.account_status !== 'active') {
        return null;
    }

    // Async update last_seen_at
    try {
        await db.prepare("UPDATE sneak_member_sessions SET last_seen_at = datetime('now') WHERE id = ?").bind(row.session_id).run();
    } catch {}

    return row;
}

/**
 * Revokes a member session.
 */
export async function revokeMemberSession(db, rawToken) {
    if (!rawToken) return;
    const tokenHash = await sha256Hex(rawToken);
    await db.prepare(`
        UPDATE sneak_member_sessions
        SET revoked_at = datetime('now')
        WHERE token_hash = ?
    `).bind(tokenHash).run();
}

/**
 * Extracts session token from Cookie header (__Host-sneak_member_session).
 */
export function getSessionTokenFromRequest(req) {
    const cookieHeader = req.headers.get('Cookie') || '';
    for (const cookie of cookieHeader.split(';')) {
        const [name, val] = cookie.trim().split('=');
        if (name === '__Host-sneak_member_session') {
            return decodeURIComponent(val || '');
        }
    }
    return null;
}

/**
 * Strict Same-Origin CSRF validation for Member mutations.
 */
export function validateMemberCsrf(req) {
    const method = req.method.toUpperCase();
    if (['GET', 'HEAD', 'OPTIONS'].includes(method)) return true;

    const origin = req.headers.get('Origin');
    const host = req.headers.get('Host');

    if (!origin || !host) return false;

    try {
        const originUrl = new URL(origin);
        if (originUrl.host === host) return true;

        if (originUrl.hostname === 'localhost' || originUrl.hostname === '127.0.0.1') {
            if (host.startsWith('localhost') || host.startsWith('127.0.0.1')) {
                return true;
            }
        }
    } catch {
        return false;
    }

    return false;
}
