/**
 * sneak-sites/preview.js
 * 
 * Secure Short-Lived Preview Token Issuance & Verification for SNEAK Websites.
 * Uses dedicated SNEAK_WEBSITE_PREVIEW_SECRET (isolated from serving and admin session secrets).
 */

function base64UrlEncode(str) {
    return btoa(str).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function base64UrlDecode(str) {
    str = str.replace(/-/g, '+').replace(/_/g, '/');
    while (str.length % 4) str += '=';
    return atob(str);
}

async function getHmacKey(secretStr) {
    const enc = new TextEncoder();
    return await crypto.subtle.importKey(
        'raw',
        enc.encode(secretStr),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign', 'verify']
    );
}

/**
 * Creates a signed short-lived preview token.
 * 
 * @param {string} siteKey - The site key being previewed
 * @param {string} siteId - The site ID
 * @param {string} secret - SNEAK_WEBSITE_PREVIEW_SECRET
 * @param {number} ttlSeconds - Token TTL (default 1800s = 30 minutes)
 * @returns {Promise<string>} Signed token string
 */
export async function createPreviewToken(siteKey, siteId, secret, ttlSeconds = 1800) {
    if (!secret || typeof secret !== 'string') {
        throw new Error('SNEAK_WEBSITE_PREVIEW_SECRET is required to create preview token');
    }

    const now = Math.floor(Date.now() / 1000);
    const payload = {
        siteKey,
        siteId,
        purpose: 'website_preview',
        iat: now,
        exp: now + ttlSeconds
    };

    const header = { alg: 'HS256', typ: 'SNEAK-PREVIEW' };
    const encHeader = base64UrlEncode(JSON.stringify(header));
    const encPayload = base64UrlEncode(JSON.stringify(payload));
    const dataToSign = `${encHeader}.${encPayload}`;

    const key = await getHmacKey(secret);
    const signature = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(dataToSign));
    const encSignature = base64UrlEncode(String.fromCharCode(...new Uint8Array(signature)));

    return `${dataToSign}.${encSignature}`;
}

/**
 * Verifies and parses a preview token.
 * 
 * @param {string} token - The raw preview token string
 * @param {string} expectedSiteKey - The site key the request is attempting to preview
 * @param {string} secret - SNEAK_WEBSITE_PREVIEW_SECRET
 * @returns {Promise<{ valid: boolean, payload?: object, error?: string }>}
 */
export async function verifyPreviewToken(token, expectedSiteKey, secret) {
    if (!token || typeof token !== 'string') {
        return { valid: false, error: 'Missing preview token' };
    }
    if (!secret || typeof secret !== 'string') {
        return { valid: false, error: 'Preview secret not configured' };
    }

    const parts = token.split('.');
    if (parts.length !== 3) {
        return { valid: false, error: 'Malformed preview token format' };
    }

    const [encHeader, encPayload, encSignature] = parts;
    const dataToVerify = `${encHeader}.${encPayload}`;

    try {
        const key = await getHmacKey(secret);
        
        // Decode signature
        const sigBin = base64UrlDecode(encSignature);
        const sigArr = new Uint8Array(sigBin.length);
        for (let i = 0; i < sigBin.length; i++) sigArr[i] = sigBin.charCodeAt(i);

        const isValidSig = await crypto.subtle.verify('HMAC', key, sigArr, new TextEncoder().encode(dataToVerify));
        if (!isValidSig) {
            return { valid: false, error: 'Invalid preview token signature' };
        }

        const payload = JSON.parse(base64UrlDecode(encPayload));
        const now = Math.floor(Date.now() / 1000);

        if (!payload.exp || payload.exp < now) {
            return { valid: false, error: 'Preview token has expired' };
        }

        if (payload.purpose !== 'website_preview') {
            return { valid: false, error: 'Invalid token purpose' };
        }

        if (expectedSiteKey && payload.siteKey !== expectedSiteKey) {
            return { valid: false, error: 'Token site key mismatch' };
        }

        return { valid: true, payload };
    } catch (err) {
        return { valid: false, error: 'Token verification failed: ' + err.message };
    }
}
