/**
 * sneak-member/auth.js
 * 
 * Passwordless Authentication, Single-Use Magic Links, 7-Day Revocable Sessions,
 * and Same-Origin CSRF Protection for SNEAK Member Portal.
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
 * Creates a single-use magic link in D1.
 */
export async function createMagicLink(db, email, purpose = 'login', ttlSeconds = 900) {
    const cleanEmail = (email || '').toLowerCase().trim();
    if (!cleanEmail) return { success: false, error: 'Email is required' };

    const user = await db.prepare("SELECT * FROM sneak_member_users WHERE email = ?").bind(cleanEmail).first();
    if (!user) {
        // Generic message: Do not reveal whether email exists
        return { success: true, message: 'If an account exists, a sign-in link has been created/sent.' };
    }

    const rawToken = generateRawToken();
    const tokenHash = await sha256Hex(rawToken);
    const linkId = `ml_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const now = new Date().toISOString();
    const expiresAt = new Date(Date.now() + (ttlSeconds * 1000)).toISOString();

    await db.prepare(`
        INSERT INTO sneak_member_magic_links (id, user_id, token_hash, purpose, created_at, expires_at, used_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
    `).bind(linkId, user.id, tokenHash, purpose, now, expiresAt).run();

    return {
        success: true,
        message: 'If an account exists, a sign-in link has been created/sent.',
        rawToken, // Staging immediate retrieval
        userId: user.id,
        accountId: user.account_id
    };
}

/**
 * Verifies and atomically consumes a single-use magic link.
 */
export async function verifyAndConsumeMagicLink(db, rawToken) {
    if (!rawToken || typeof rawToken !== 'string') return null;

    const tokenHash = await sha256Hex(rawToken);
    const now = new Date().toISOString();

    const link = await db.prepare(`
        SELECT id, user_id, purpose, expires_at, used_at
        FROM sneak_member_magic_links
        WHERE token_hash = ? AND used_at IS NULL AND expires_at > datetime('now')
    `).bind(tokenHash).first();

    if (!link) return null;

    // Atomically consume magic link
    await db.prepare(`
        UPDATE sneak_member_magic_links
        SET used_at = ?
        WHERE id = ? AND used_at IS NULL
    `).bind(now, link.id).run();

    // Activate/update user
    await db.prepare(`
        UPDATE sneak_member_users
        SET status = 'active',
            activated_at = COALESCE(activated_at, ?),
            last_login_at = ?,
            updated_at = ?
        WHERE id = ?
    `).bind(now, now, now, link.user_id).run();

    const user = await db.prepare("SELECT * FROM sneak_member_users WHERE id = ?").bind(link.user_id).first();
    if (!user) return null;

    // Create 7-day member session
    const sessionToken = await createMemberSession(db, user.id, user.account_id, 604800);

    return {
        user,
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
