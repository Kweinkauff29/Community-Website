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

/**
 * Dispatches transactional email via Mailjet API v3.1 or fallback simulated staging adapter.
 */
export async function sendTransactionalEmail(env, { to, subject, html, text }) {
    const fromStr = env?.EMAIL_FROM || env?.FROM_EMAIL || DEFAULT_FROM;
    let fromEmail = 'no-reply@ccorealtors.org';
    let fromName = 'CCOR Property Search';

    const fromMatch = fromStr.match(/^(?:(.*)<)?([^>]+)>?$/);
    if (fromMatch) {
        fromName = (fromMatch[1] || 'CCOR Property Search').trim();
        fromEmail = fromMatch[2].trim();
    }

    const apiKey = env?.MAILJET_API_KEY || env?.MJ_API_KEY;
    const secretKey = env?.MAILJET_SECRET_KEY || env?.MJ_API_SECRET;

    if (apiKey && secretKey) {
        try {
            const auth = 'Basic ' + (typeof btoa === 'function' ? btoa(`${apiKey}:${secretKey}`) : Buffer.from(`${apiKey}:${secretKey}`).toString('base64'));
            const payload = {
                Messages: [
                    {
                        From: {
                            Email: fromEmail,
                            Name: fromName
                        },
                        To: [
                            {
                                Email: to
                            }
                        ],
                        Subject: subject,
                        HTMLPart: html,
                        TextPart: text || ''
                    }
                ]
            };

            const res = await fetch('https://api.mailjet.com/v3.1/send', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': auth
                },
                body: JSON.stringify(payload)
            });

            if (res.ok) {
                const data = await res.json();
                const msgId = data?.Messages?.[0]?.To?.[0]?.MessageID || `mj_${Date.now()}`;
                return {
                    success: true,
                    status: 'sent',
                    provider: 'mailjet',
                    providerMessageId: String(msgId)
                };
            }

            const errorText = await res.text();
            let errorCode = `HTTP_${res.status}`;
            try {
                const errObj = JSON.parse(errorText);
                errorCode = errObj?.ErrorMessage || errObj?.StatusCode || errorCode;
            } catch {}

            return {
                success: false,
                retryable: true,
                status: 'failed',
                provider: 'mailjet',
                errorCode: String(errorCode).slice(0, 100)
            };
        } catch (err) {
            return {
                success: false,
                retryable: true,
                status: 'failed',
                provider: 'mailjet',
                errorCode: (err.message || 'NetworkError').slice(0, 100)
            };
        }
    }

    // Fallback: Provider unconfigured when Mailjet credentials are not set
    return {
        success: false,
        retryable: true,
        status: 'provider_unconfigured',
        provider: null,
        providerMessageId: null,
        error: 'EmailProviderNotConfigured'
    };
}
