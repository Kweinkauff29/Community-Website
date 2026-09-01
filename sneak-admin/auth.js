/**
 * sneak-admin/auth.js
 * 
 * Hardened Admin Authentication, PBKDF2 Password Hashing, Server-Side Revocable Sessions,
 * Strict Same-Origin CSRF Defense, and Rate Limiting.
 */

// Helper to convert ArrayBuffer to hex string
export function bufferToHex(buffer) {
    return Array.from(new Uint8Array(buffer)).map(b => b.toString(16).padStart(2, '0')).join('');
}

/**
 * Constant-time string comparison to prevent timing side-channel attacks.
 */
export function timingSafeEqual(a, b) {
    if (typeof a !== 'string' || typeof b !== 'string') return false;
    const maxLength = Math.max(a.length, b.length);
    let result = a.length ^ b.length;
    for (let i = 0; i < maxLength; i++) {
        result |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
    }
    return result === 0;
}

/**
 * Computes SHA-256 hash of a string (used for token hashing and IP/UA pseudonymization).
 */
export async function sha256Hex(str) {
    if (!str) return '';
    const enc = new TextEncoder();
    const hashBuffer = await crypto.subtle.digest('SHA-256', enc.encode(str));
    return bufferToHex(hashBuffer);
}

/**
 * Derives a PBKDF2-HMAC-SHA256 key from a password and hex salt.
 * Note: Cloudflare Workers WebCrypto enforces a hard maximum of 100,000 iterations for PBKDF2.
 */
export async function derivePbkdf2Hash(password, saltHex, iterations = 100000) {
    const enc = new TextEncoder();
    const passwordKey = await crypto.subtle.importKey(
        'raw',
        enc.encode(password),
        { name: 'PBKDF2' },
        false,
        ['deriveBits']
    );

    const saltBytes = Uint8Array.from(saltHex.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
    const derivedBits = await crypto.subtle.deriveBits(
        {
            name: 'PBKDF2',
            salt: saltBytes,
            iterations: iterations,
            hash: 'SHA-256'
        },
        passwordKey,
        256 // 32 bytes (256 bits)
    );

    return bufferToHex(derivedBits);
}

/**
 * Creates a formatted PBKDF2 password hash string.
 */
export async function hashPasswordPbkdf2(password, iterations = 100000) {
    const saltBytes = crypto.getRandomValues(new Uint8Array(16)); // 128-bit salt
    const saltHex = bufferToHex(saltBytes);
    const derivedHex = await derivePbkdf2Hash(password, saltHex, iterations);
    return `pbkdf2-sha256$${iterations}$${saltHex}$${derivedHex}`;
}

/**
 * Verifies a password against a stored PBKDF2 hash using constant-time comparison.
 */
export async function verifyAdminPassword(password, storedHash) {
    if (!password || !storedHash || typeof storedHash !== 'string') return false;

    const cleanHash = storedHash.trim();
    const parts = cleanHash.split('$');
    if (parts.length !== 4 || parts[0] !== 'pbkdf2-sha256') {
        // Reject legacy / fast SHA-256 hashes
        return false;
    }

    const iterations = parseInt(parts[1], 10);
    const saltHex = parts[2];
    const expectedDerivedHex = parts[3];

    if (isNaN(iterations) || iterations < 10000 || !saltHex || !expectedDerivedHex) {
        return false;
    }

    const computedDerivedHex = await derivePbkdf2Hash(password, saltHex, iterations);
    return timingSafeEqual(computedDerivedHex, expectedDerivedHex);
}

/**
 * Generates a 32-byte cryptographically random session token.
 */
export function generateRawSessionToken() {
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    return bufferToHex(bytes);
}

/**
 * Creates a new server-side revocable session in D1.
 */
export async function createAdminSession(db, actor, ipHash, userAgentHash, ttlSeconds = 14400) {
    const rawToken = generateRawSessionToken();
    const tokenHash = await sha256Hex(rawToken);
    const sessionId = `sess_${crypto.randomUUID()}`;
    const now = new Date().toISOString();
    const expiresAt = new Date(Date.now() + (ttlSeconds * 1000)).toISOString();

    // Rotate: Revoke previous active sessions for this actor
    await db.prepare(`
        UPDATE sneak_admin_sessions
        SET revoked_at = ?
        WHERE admin_actor = ? AND revoked_at IS NULL
    `).bind(now, actor).run();

    // Insert new active session
    await db.prepare(`
        INSERT INTO sneak_admin_sessions (
            id, token_hash, admin_actor, created_at, expires_at, last_seen_at, revoked_at, user_agent_hash, created_ip_hash
        ) VALUES (
            ?, ?, ?, ?, ?, ?, NULL, ?, ?
        )
    `).bind(sessionId, tokenHash, actor, now, expiresAt, now, userAgentHash, ipHash).run();

    return rawToken;
}

/**
 * Verifies a presented raw session token against active D1 sessions.
 */
export async function verifyAdminSession(db, rawToken) {
    if (!rawToken || typeof rawToken !== 'string' || rawToken.length < 32) return null;

    const tokenHash = await sha256Hex(rawToken);

    const session = await db.prepare(`
        SELECT id, token_hash, admin_actor, created_at, expires_at, revoked_at
        FROM sneak_admin_sessions
        WHERE token_hash = ? AND revoked_at IS NULL AND expires_at > datetime('now')
    `).bind(tokenHash).first();

    if (!session) return null;

    // Async update last_seen_at
    try {
        await db.prepare("UPDATE sneak_admin_sessions SET last_seen_at = datetime('now') WHERE id = ?").bind(session.id).run();
    } catch {}

    return session;
}

/**
 * Revokes a session in D1.
 */
export async function revokeAdminSession(db, rawToken) {
    if (!rawToken) return;
    const tokenHash = await sha256Hex(rawToken);
    await db.prepare(`
        UPDATE sneak_admin_sessions
        SET revoked_at = datetime('now')
        WHERE token_hash = ?
    `).bind(tokenHash).run();
}

/**
 * Extracts session token from Cookie header (__Host-sneak_admin_session).
 */
export function getSessionTokenFromRequest(req) {
    const cookieHeader = req.headers.get('Cookie') || '';
    for (const cookie of cookieHeader.split(';')) {
        const [name, val] = cookie.trim().split('=');
        if (name === '__Host-sneak_admin_session') {
            return decodeURIComponent(val || '');
        }
    }
    return null;
}

/**
 * Checks login rate limiting for IP hash (max 10 failed attempts / 15 min).
 */
export async function checkLoginRateLimit(db, ipHash) {
    const res = await db.prepare(`
        SELECT count(*) as count
        FROM sneak_admin_login_attempts
        WHERE ip_hash = ? AND attempted_at > datetime('now', '-15 minutes') AND success = 0
    `).bind(ipHash).first();

    const count = res?.count || 0;
    return {
        allowed: count < 10,
        failedAttempts: count
    };
}

/**
 * Records a login attempt in D1.
 */
export async function recordLoginAttempt(db, ipHash, success) {
    const id = `att_${crypto.randomUUID()}`;
    try {
        await db.prepare(`
            INSERT INTO sneak_admin_login_attempts (id, ip_hash, attempted_at, success)
            VALUES (?, ?, datetime('now'), ?)
        `).bind(id, ipHash, success ? 1 : 0).run();
    } catch (err) {
        console.error('[LOGIN ATTEMPT LOG ERROR]', err.message);
    }
}

/**
 * Strict Same-Origin CSRF validation.
 */
export function validateAdminCsrf(req) {
    const method = req.method.toUpperCase();
    if (['GET', 'HEAD', 'OPTIONS'].includes(method)) return true;

    const origin = req.headers.get('Origin');
    const host = req.headers.get('Host');

    if (!origin || !host) return false;

    try {
        const originUrl = new URL(origin);
        // Staging/Production exact match
        if (originUrl.host === host) return true;

        // Local dev support
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
