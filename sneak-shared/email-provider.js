/**
 * sneak-shared/email-provider.js
 * 
 * Centralized Transactional Email Adapter and Tamper-Proof Token Signer
 * for Consumer Authentication and Saved Search Email Alerts.
 * 
 * Invariants:
 * - Never log or leak raw Mailjet credentials or bearer tokens.
 * - Fails safely with status 'provider_unconfigured' when secrets are absent in staging/dev.
 * - provider_unconfigured must NEVER be treated as successful email delivery.
 * - Unsubscribe tokens require an explicit, cryptographically sufficient signing secret.
 * - Zero default fallback keys in runtime code.
 * - Unsubscribe tokens grant exclusively alert-disabling capabilities (no auth escalation).
 */

const DEFAULT_FROM = 'CCOR Property Search <no-reply@ccorealtors.org>';
const MIN_SECRET_LENGTH = 16;

export function bufferToHex(buffer) {
    return Array.from(new Uint8Array(buffer)).map(b => b.toString(16).padStart(2, '0')).join('');
}

export function base64UrlEncode(str) {
    const b64 = typeof btoa === 'function' ? btoa(str) : Buffer.from(str).toString('base64');
    return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

export function base64UrlDecode(str) {
    let base64 = str.replace(/-/g, '+').replace(/_/g, '/');
    while (base64.length % 4) base64 += '=';
    return typeof atob === 'function' ? atob(base64) : Buffer.from(base64, 'base64').toString('utf-8');
}

/**
 * Creates a cryptographically tamper-proof unsubscribe token.
 * Payload: { alertId, userId, siteId, t: timestamp }
 */
export async function createUnsubscribeToken(alertId, userId, siteId, secret) {
    if (!secret || typeof secret !== 'string' || secret.length < MIN_SECRET_LENGTH) {
        throw new Error('SigningSecretUnavailable');
    }
    if (!alertId || !userId || !siteId) return '';
    const payload = JSON.stringify({
        a: alertId,
        u: userId,
        s: siteId,
        t: Date.now()
    });
    const encodedPayload = base64UrlEncode(payload);

    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
        'raw',
        enc.encode(secret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
    );

    const sigBuffer = await crypto.subtle.sign('HMAC', key, enc.encode(encodedPayload));
    const encodedSig = base64UrlEncode(String.fromCharCode(...new Uint8Array(sigBuffer)));

    return `${encodedPayload}.${encodedSig}`;
}

/**
 * Verifies and decodes an unsubscribe token.
 */
export async function verifyUnsubscribeToken(token, secret) {
    if (!secret || typeof secret !== 'string' || secret.length < MIN_SECRET_LENGTH) {
        throw new Error('SigningSecretUnavailable');
    }
    if (!token || typeof token !== 'string' || !token.includes('.')) return null;

    const [encodedPayload, encodedSig] = token.split('.');
    if (!encodedPayload || !encodedSig) return null;

    try {
        const enc = new TextEncoder();
        const key = await crypto.subtle.importKey(
            'raw',
            enc.encode(secret),
            { name: 'HMAC', hash: 'SHA-256' },
            false,
            ['verify']
        );

        const sigBinary = base64UrlDecode(encodedSig);
        const sigBytes = new Uint8Array(sigBinary.length);
        for (let i = 0; i < sigBinary.length; i++) {
            sigBytes[i] = sigBinary.charCodeAt(i);
        }

        const isValid = await crypto.subtle.verify('HMAC', key, sigBytes, enc.encode(encodedPayload));
        if (!isValid) return null;

        const payload = JSON.parse(base64UrlDecode(encodedPayload));
        if (!payload || !payload.a || !payload.u || !payload.s) return null;

        return {
            alertId: payload.a,
            userId: payload.u,
            siteId: payload.s,
            timestamp: payload.t
        };
    } catch {
        return null;
    }
}

function parseFromAddress(value, defaultName) {
    const input = String(value || '').trim();
    const bracketed = input.match(/^\s*(.*?)\s*<([^<>]+)>\s*$/);
    if (bracketed) {
        return { name: bracketed[1].trim() || defaultName, email: bracketed[2].trim() };
    }
    return input.includes('@')
        ? { name: defaultName, email: input }
        : { name: defaultName, email: 'no-reply@ccorealtors.org' };
}

async function readJsonResponse(response, maxBytes = 64 * 1024) {
    const declaredLength = Number(response.headers?.get?.('Content-Length') || 0);
    if (declaredLength > maxBytes) throw new Error('ProviderResponseTooLarge');

    if (!response.body || typeof response.body.getReader !== 'function') {
        if (typeof response.text !== 'function' && typeof response.json === 'function') {
            return await response.json();
        }
        const text = await response.text();
        if (new TextEncoder().encode(text).byteLength > maxBytes) throw new Error('ProviderResponseTooLarge');
        return text ? JSON.parse(text) : {};
    }

    const reader = response.body.getReader();
    const chunks = [];
    let size = 0;
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > maxBytes) {
            await reader.cancel();
            throw new Error('ProviderResponseTooLarge');
        }
        chunks.push(value);
    }
    const combined = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
        combined.set(chunk, offset);
        offset += chunk.byteLength;
    }
    const text = new TextDecoder().decode(combined);
    return text ? JSON.parse(text) : {};
}

function providerFailure({ status = 'failed', errorCode = 'DeliveryFailed', retryable = true } = {}) {
    return {
        success: false,
        retryable,
        status,
        provider: status === 'provider_unconfigured' ? null : 'mailjet',
        providerMessageId: null,
        id: null,
        errorCode: String(errorCode || 'DeliveryFailed').slice(0, 100)
    };
}

/**
 * Dispatches transactional email through Mailjet API v3.1.
 * Provider configuration and every provider response are fail closed; there is
 * no simulated-success adapter in a runtime path.
 */
export async function sendTransactionalEmail(env, { to, subject, html, text, from, customId }) {
    const fromStr = env?.EMAIL_FROM || env?.FROM_EMAIL || DEFAULT_FROM;
    const sender = parseFromAddress(from || fromStr, 'CCOR Property Search');

    const apiKey = env?.MAILJET_API_KEY || env?.MJ_API_KEY;
    const secretKey = env?.MAILJET_SECRET_KEY || env?.MJ_API_SECRET;

    if (!apiKey || !secretKey) {
        return providerFailure({ status: 'provider_unconfigured', errorCode: 'EmailProviderNotConfigured' });
    }

    try {
        const auth = 'Basic ' + (typeof btoa === 'function' ? btoa(`${apiKey}:${secretKey}`) : Buffer.from(`${apiKey}:${secretKey}`).toString('base64'));
        const message = {
            From: { Email: sender.email, Name: sender.name },
            To: [{ Email: to }],
            Subject: subject,
            HTMLPart: html,
            TextPart: text || ''
        };
        if (customId) message.CustomID = String(customId).slice(0, 255);

        const res = await fetch('https://api.mailjet.com/v3.1/send', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': auth
            },
            body: JSON.stringify({ Messages: [message] })
        });

        let data = {};
        try {
            data = await readJsonResponse(res);
        } catch (err) {
            return providerFailure({ errorCode: err?.message || 'InvalidProviderResponse', retryable: res.status === 429 || res.status >= 500 });
        }

        if (!res.ok) {
            const code = data?.ErrorCode || data?.ErrorMessage || data?.StatusCode || `HTTP_${res.status}`;
            return providerFailure({ errorCode: code, retryable: res.status === 429 || res.status >= 500 });
        }

        const firstMessage = data?.Messages?.[0];
        if (String(firstMessage?.Status || '').toLowerCase() !== 'success') {
            const firstError = firstMessage?.Errors?.[0];
            return providerFailure({
                errorCode: firstError?.ErrorCode || firstError?.ErrorMessage || 'ProviderMessageRejected',
                retryable: Number(firstError?.StatusCode || 0) === 429 || Number(firstError?.StatusCode || 0) >= 500
            });
        }

        const msgId = firstMessage?.To?.[0]?.MessageID || firstMessage?.To?.[0]?.MessageUUID;
        if (!msgId) return providerFailure({ errorCode: 'ProviderMessageIdMissing', retryable: true });

        return {
            success: true,
            retryable: false,
            status: 'sent',
            provider: 'mailjet',
            providerMessageId: String(msgId),
            id: String(msgId),
            errorCode: null
        };
    } catch (err) {
        return providerFailure({ errorCode: err?.message || 'NetworkError', retryable: true });
    }
}
