/**
 * sneak-member/email.js
 * 
 * Modular Transactional Email Delivery Adapter for SNEAK IDX:
 * - Magic Link Login Delivery
 * - Member Onboarding Invitation Delivery
 * - Lead Notification Delivery
 * - Supports Resend / Postmark / Simulated Staging Adapters
 */

const DEFAULT_FROM = 'CCOR IDX Plug-in <no-reply@ccorealtors.org>';

/**
 * Builds HTML template for Member Magic Link Sign-In.
 */
export function renderMagicLinkTemplate({ verifyUrl, expiresMinutes = 15 }) {
    return `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Your CCOR IDX Plug-in Sign-In Link</title>
</head>
<body style="margin: 0; padding: 0; background-color: #0b1329; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #f8fafc;">
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #0b1329; padding: 40px 20px;">
        <tr>
            <td align="center">
                <table width="100%" max-width="540px" border="0" cellspacing="0" cellpadding="0" style="max-width: 540px; background-color: #111e38; border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; overflow: hidden; padding: 36px 32px; box-shadow: 0 10px 25px rgba(0,0,0,0.5);">
                    <tr>
                        <td align="left" style="padding-bottom: 24px; border-bottom: 1px solid rgba(255,255,255,0.08);">
                            <div style="font-size: 20px; font-weight: 700; color: #38bdf8; letter-spacing: 0.5px;">CCOR IDX Plug-in</div>
                            <div style="font-size: 13px; color: #94a3b8; margin-top: 2px;">Coconut Coast Organization of REALTORS®</div>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 28px 0 20px 0;">
                            <h2 style="font-size: 22px; font-weight: 600; color: #ffffff; margin: 0 0 12px 0;">Sign in to your CCOR IDX Plug-in Portal</h2>
                            <p style="font-size: 15px; line-height: 1.5; color: #cbd5e1; margin: 0 0 24px 0;">
                                Click the button below to securely access your CCOR IDX Plug-in account. This single-use sign-in link is valid for the next <strong>${expiresMinutes} minutes</strong>.
                            </p>
                            <table border="0" cellspacing="0" cellpadding="0" style="margin: 28px 0;">
                                <tr>
                                    <td align="center" style="border-radius: 8px; background: linear-gradient(135deg, #0284c7, #0369a1);">
                                        <a href="${verifyUrl}" target="_blank" style="font-size: 15px; font-weight: 600; color: #ffffff; text-decoration: none; padding: 14px 28px; display: inline-block; border-radius: 8px;">
                                            Sign In to CCOR IDX Plug-in Portal
                                        </a>
                                    </td>
                                </tr>
                            </table>
                            <p style="font-size: 13px; line-height: 1.5; color: #64748b; margin: 24px 0 0 0;">
                                Or copy and paste this URL into your browser:<br>
                                <a href="${verifyUrl}" style="color: #38bdf8; word-break: break-all; font-size: 12px;">${verifyUrl}</a>
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding-top: 24px; border-top: 1px solid rgba(255,255,255,0.08);">
                            <p style="font-size: 12px; line-height: 1.4; color: #64748b; margin: 0;">
                                If you did not request this sign-in link, no action is needed. Your account remains secure.<br>
                                © ${new Date().getFullYear()} Coconut Coast Organization of REALTORS®. All rights reserved.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    `.trim();
}

/**
 * Builds HTML template for Member Onboarding Invitation.
 */
export function renderInvitationTemplate({ inviteUrl, accountName }) {
    return `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Welcome to CCOR IDX Plug-in</title>
</head>
<body style="margin: 0; padding: 0; background-color: #0b1329; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #f8fafc;">
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #0b1329; padding: 40px 20px;">
        <tr>
            <td align="center">
                <table width="100%" max-width="540px" border="0" cellspacing="0" cellpadding="0" style="max-width: 540px; background-color: #111e38; border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; overflow: hidden; padding: 36px 32px; box-shadow: 0 10px 25px rgba(0,0,0,0.5);">
                    <tr>
                        <td align="left" style="padding-bottom: 24px; border-bottom: 1px solid rgba(255,255,255,0.08);">
                            <div style="font-size: 20px; font-weight: 700; color: #38bdf8; letter-spacing: 0.5px;">CCOR IDX Plug-in</div>
                            <div style="font-size: 13px; color: #94a3b8; margin-top: 2px;">Coconut Coast Organization of REALTORS®</div>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 28px 0 20px 0;">
                            <h2 style="font-size: 22px; font-weight: 600; color: #ffffff; margin: 0 0 12px 0;">Your CCOR IDX Plug-in Account is Ready</h2>
                            <p style="font-size: 15px; line-height: 1.5; color: #cbd5e1; margin: 0 0 16px 0;">
                                You have been invited to manage your MLS search, domain settings, custom branding, and embed codes for <strong>${accountName || 'your real estate business'}</strong>.
                            </p>
                            <p style="font-size: 15px; line-height: 1.5; color: #cbd5e1; margin: 0 0 24px 0;">
                                This invitation link is valid for 24 hours. Billing is administered through GrowthZone on the first of each month. Click below to activate your account and configure your website integration:
                            </p>
                            <table border="0" cellspacing="0" cellpadding="0" style="margin: 28px 0;">
                                <tr>
                                    <td align="center" style="border-radius: 8px; background: linear-gradient(135deg, #0284c7, #0369a1);">
                                        <a href="${inviteUrl}" target="_blank" style="font-size: 15px; font-weight: 600; color: #ffffff; text-decoration: none; padding: 14px 28px; display: inline-block; border-radius: 8px;">
                                            Accept Invitation & Sign In
                                        </a>
                                    </td>
                                </tr>
                            </table>
                            <p style="font-size: 13px; line-height: 1.5; color: #64748b; margin: 24px 0 0 0;">
                                Or paste this link in your browser:<br>
                                <a href="${inviteUrl}" style="color: #38bdf8; word-break: break-all; font-size: 12px;">${inviteUrl}</a>
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding-top: 24px; border-top: 1px solid rgba(255,255,255,0.08);">
                            <p style="font-size: 12px; line-height: 1.4; color: #64748b; margin: 0;">
                                If you did not request or apply for CCOR IDX Plug-in, please contact CCOR Member Services.<br>
                                © ${new Date().getFullYear()} Coconut Coast Organization of REALTORS®. All rights reserved.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    `.trim();
}

/**
 * Generic email delivery function supporting Resend, Postmark, or Simulated Staging Adapter.
 */
export async function sendTransactionalEmail(env, { to, subject, html, text }) {
    const from = env?.EMAIL_FROM || DEFAULT_FROM;

    // 1. Mailjet Adapter (Primary for CCOR IDX Plug-in)
    const mailjetApiKey = env?.MAILJET_API_KEY || env?.MJ_API_KEY;
    const mailjetSecretKey = env?.MAILJET_SECRET_KEY || env?.MJ_API_SECRET;
    if (mailjetApiKey && mailjetSecretKey) {
        try {
            let fromEmail = 'no-reply@ccorealtors.org';
            let fromName = 'CCOR IDX Plug-in';
            const fromStr = env?.EMAIL_FROM || env?.FROM_EMAIL || DEFAULT_FROM;
            const fromMatch = fromStr.match(/^(?:(.*)<)?([^>]+)>?$/);
            if (fromMatch) {
                fromName = (fromMatch[1] || 'CCOR IDX Plug-in').trim();
                fromEmail = fromMatch[2].trim();
            }

            const auth = 'Basic ' + btoa(`${mailjetApiKey}:${mailjetSecretKey}`);
            const payload = {
                Messages: [
                    {
                        From: {
                            Email: fromEmail,
                            Name: fromName
                        },
                        To: [
                            {
                                Email: to,
                                Name: ''
                            }
                        ],
                        Subject: subject,
                        TextPart: text || '',
                        HTMLPart: html,
                        CustomID: 'SNEAK-IDX'
                    }
                ]
            };

            const res = await fetch('https://api.mailjet.com/v3.1/send', {
                method: 'POST',
                headers: {
                    'Authorization': auth,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!res.ok) {
                const errText = await res.text();
                console.error('[EMAIL MAILJET ERROR]', res.status, errText);
                return { success: false, error: `Mailjet HTTP ${res.status}` };
            }

            const data = await res.json();
            const messageId = data?.Messages?.[0]?.To?.[0]?.MessageID || data?.Messages?.[0]?.CustomID || 'mj_sent';
            return { success: true, id: String(messageId), provider: 'mailjet' };
        } catch (err) {
            console.error('[EMAIL MAILJET EXCEPTION]', err.message);
            return { success: false, error: err.message };
        }
    }

    // 2. Resend Adapter (Legacy fallback)
    if (env?.RESEND_API_KEY) {
        try {
            const res = await fetch('https://api.resend.com/emails', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${env.RESEND_API_KEY}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    from,
                    to: [to],
                    subject,
                    html,
                    text: text || ''
                })
            });

            if (!res.ok) {
                const errText = await res.text();
                console.error('[EMAIL RESEND ERROR]', res.status, errText);
                return { success: false, error: `Resend HTTP ${res.status}` };
            }

            const data = await res.json();
            return { success: true, id: data.id, provider: 'resend' };
        } catch (err) {
            console.error('[EMAIL RESEND EXCEPTION]', err.message);
            return { success: false, error: err.message };
        }
    }

    // 2. Postmark Adapter (if POSTMARK_SERVER_TOKEN is configured)
    if (env?.POSTMARK_SERVER_TOKEN) {
        try {
            const res = await fetch('https://api.postmarkapp.com/email', {
                method: 'POST',
                headers: {
                    'X-Postmark-Server-Token': env.POSTMARK_SERVER_TOKEN,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify({
                    From: from,
                    To: to,
                    Subject: subject,
                    HtmlBody: html,
                    TextBody: text || ''
                })
            });

            if (!res.ok) {
                const errText = await res.text();
                console.error('[EMAIL POSTMARK ERROR]', res.status, errText);
                return { success: false, error: `Postmark HTTP ${res.status}` };
            }

            const data = await res.json();
            return { success: true, id: data.MessageID, provider: 'postmark' };
        } catch (err) {
            console.error('[EMAIL POSTMARK EXCEPTION]', err.message);
            return { success: false, error: err.message };
        }
    }

    // 3. Staging Simulation Adapter (when no live email secret is configured)
    // Logs delivery without exposing secret tokens publicly
    return {
        success: true,
        provider: 'simulated',
        to,
        subject,
        timestamp: new Date().toISOString()
    };
}

/**
 * High-level helper to send magic link login email.
 */
export async function sendMagicLinkEmail(env, { email, verifyUrl, expiresMinutes = 15 }) {
    const subject = 'Your CCOR IDX Plug-in Sign-In Link';
    const html = renderMagicLinkTemplate({ verifyUrl, expiresMinutes });
    const text = `Sign in to your CCOR IDX Plug-in Portal: ${verifyUrl} (Valid for ${expiresMinutes} minutes). If you did not request this, please ignore.`;
    return sendTransactionalEmail(env, { to: email, subject, html, text });
}

/**
 * High-level helper to send member invitation email.
 */
export async function sendInvitationEmail(env, { email, inviteUrl, accountName }) {
    const subject = 'Welcome to CCOR IDX Plug-in — Your Account is Ready';
    const html = renderInvitationTemplate({ inviteUrl, accountName });
    const text = `Welcome to CCOR IDX Plug-in. Click here to activate your account for ${accountName || 'your business'} (Valid for 24 hours): ${inviteUrl}. If you did not request or apply for CCOR IDX Plug-in, please contact CCOR Member Services.`;
    return sendTransactionalEmail(env, { to: email, subject, html, text });
}
