/**
 * sneak-consumer/email.js
 * 
 * Transactional Email Adapter for Consumer Authentication (Property Buyers).
 * Uses the shared fail-closed Mailjet API v3.1 adapter.
 */

import { sendTransactionalEmail } from '../sneak-shared/email-provider.js';

const DEFAULT_FROM = 'CCOR Property Search <no-reply@ccorealtors.org>';

export function renderConsumerMagicLinkTemplate({ verifyUrl, expiresMinutes = 15, agentName, brokerage }) {
    const headerTitle = agentName ? `${agentName} | Property Search` : 'CCOR Property Search';
    const subTitle = brokerage ? brokerage : 'Coconut Coast Organization of REALTORS®';

    return `
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Your Property Search Sign-In Link</title>
</head>
<body style="margin: 0; padding: 0; background-color: #0b1329; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; color: #f8fafc;">
    <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #0b1329; padding: 40px 20px;">
        <tr>
            <td align="center">
                <table width="100%" max-width="540px" border="0" cellspacing="0" cellpadding="0" style="max-width: 540px; background-color: #111e38; border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; overflow: hidden; padding: 36px 32px; box-shadow: 0 10px 25px rgba(0,0,0,0.5);">
                    <tr>
                        <td align="left" style="padding-bottom: 24px; border-bottom: 1px solid rgba(255,255,255,0.08);">
                            <div style="font-size: 20px; font-weight: 700; color: #38bdf8; letter-spacing: 0.5px;">${headerTitle}</div>
                            <div style="font-size: 13px; color: #94a3b8; margin-top: 2px;">${subTitle}</div>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 28px 0 20px 0;">
                            <h2 style="font-size: 22px; font-weight: 600; color: #ffffff; margin: 0 0 12px 0;">Sign in to your Saved Homes</h2>
                            <p style="font-size: 15px; line-height: 1.5; color: #cbd5e1; margin: 0 0 24px 0;">
                                Click the button below to sign in and access your saved properties across all your devices. This secure single-use sign-in link is valid for the next <strong>${expiresMinutes} minutes</strong>.
                            </p>
                            <table border="0" cellspacing="0" cellpadding="0" style="margin: 28px 0;">
                                <tr>
                                    <td align="center" style="border-radius: 8px; background: linear-gradient(135deg, #0284c7, #0369a1);">
                                        <a href="${verifyUrl}" target="_blank" style="font-size: 15px; font-weight: 600; color: #ffffff; text-decoration: none; padding: 14px 28px; display: inline-block; border-radius: 8px;">
                                            Sign In to View Saved Homes
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
                                If you did not request this sign-in link, you can safely ignore this email.<br>
                                © ${new Date().getFullYear()} ${subTitle}. Powered by SNEAK IDX Platform.
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
 * Dispatches consumer transactional email.
 */
export async function sendConsumerTransactionalEmail(env, { to, subject, html, text }) {
    return sendTransactionalEmail(env, {
        to,
        subject,
        html,
        text,
        from: env?.EMAIL_FROM || env?.FROM_EMAIL || DEFAULT_FROM,
        customId: 'SNEAK-CONSUMER-AUTH'
    });
}

export async function sendConsumerMagicLinkEmail(env, { email, verifyUrl, expiresMinutes = 15, agentName, brokerage }) {
    const subject = 'Your property search sign-in link';
    const html = renderConsumerMagicLinkTemplate({ verifyUrl, expiresMinutes, agentName, brokerage });
    const text = `Sign in to your property search saved homes: ${verifyUrl} (Valid for ${expiresMinutes} minutes). If you did not request this, please ignore.`;
    return sendConsumerTransactionalEmail(env, { to: email, subject, html, text });
}
