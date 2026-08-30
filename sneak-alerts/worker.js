/**
 * sneak-alerts/worker.js
 * 
 * SNEAK IDX Saved Search Email Alert Engine Worker.
 * 
 * Scheduled Process & Delivery Infrastructure:
 * - Discovers due alerts across ASAP and Daily digest frequencies.
 * - Evaluates candidate inventory against saved search state and tenant scope.
 * - Idempotent delivery tracking with match ledger and delivery logs.
 * - Fail-safe transactional email delivery via Mailjet / staging simulator.
 * - Cryptographically signed one-click unsubscribe endpoint.
 * - Strict zero-Bridge token, zero-GrowthZone, zero-Stripe security boundary.
 */

import { findDueAlerts, matchNewListingsForAlert } from './matcher.js';
import { renderSavedSearchAlertEmail, escapeHtml } from './email.js';
import { sendTransactionalEmail, createUnsubscribeToken, verifyUnsubscribeToken } from '../sneak-shared/email-provider.js';

export const ALERT_BUILD = '2026.08.28.7.3c2a';

export function jsonResponse(data, status = 200, origin = null) {
    const headers = new Headers();
    headers.set('Content-Type', 'application/json');
    headers.set('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0');
    headers.set('Pragma', 'no-cache');

    if (origin) {
        headers.set('Access-Control-Allow-Origin', origin);
        headers.set('Vary', 'Origin');
    }
    return new Response(JSON.stringify(data), { status, headers });
}

/**
 * Core alert processing engine executed by scheduled cron or test dry-runs.
 */
export async function processAlerts({ db, env, dryRun = false, now = new Date() }) {
    const runId = `arun_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const nowIso = (now instanceof Date ? now : new Date(now)).toISOString();

    const stats = {
        runId,
        startedAt: nowIso,
        status: 'running',
        searchesEvaluated: 0,
        matchesFound: 0,
        emailsAttempted: 0,
        emailsSent: 0,
        emailsFailed: 0,
        dryRun
    };

    // 1. Run Lock / Overlap Protection
    try {
        const activeRun = await db.prepare(`
            SELECT id, started_at FROM sneak_alert_runs
            WHERE status = 'running' AND started_at > datetime('now', '-5 minutes')
            LIMIT 1
        `).first();

        if (activeRun && !dryRun) {
            console.log(`[ALERT WORKER] Overlapping run detected (${activeRun.id}). Skipping invocation.`);
            return { skipped: true, reason: 'OverlapLocked', activeRunId: activeRun.id };
        }

        if (!dryRun) {
            await db.prepare(`
                INSERT INTO sneak_alert_runs (id, started_at, status)
                VALUES (?, ?, 'running')
            `).bind(runId, nowIso).run();
        }
    } catch (err) {
        console.error('[ALERT WORKER RUN LOCK ERROR]', err.message);
    }

    try {
        // 2. Discover Due Alerts
        const dueAlerts = await findDueAlerts(db, { limit: 150, now });
        stats.searchesEvaluated = dueAlerts.length;

        const alertsWorkerUrl = env?.ALERTS_WORKER_URL || 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev';
        const signingSecret = env?.SNEAK_SIGNING_SECRET || 'sneak-default-token-secret-fallback-key';

        for (const alert of dueAlerts) {
            // 3. Match candidate listings
            const matchResult = await matchNewListingsForAlert(db, alert);
            if (!matchResult.valid || !matchResult.listings || matchResult.listings.length === 0) {
                // No new matches -> update last_checked_at and localDate for daily
                if (!dryRun) {
                    await db.prepare(`
                        UPDATE sneak_consumer_search_alerts
                        SET last_checked_at = ?,
                            last_daily_local_date = COALESCE(?, last_daily_local_date),
                            updated_at = ?
                        WHERE id = ?
                    `).bind(nowIso, alert.currentLocalDate || null, nowIso, alert.alert_id).run();
                }
                continue;
            }

            const newListings = matchResult.listings;
            stats.matchesFound += newListings.length;

            // 4. Record matches into ledger (idempotent via INSERT OR IGNORE)
            if (!dryRun) {
                for (const listing of newListings) {
                    const matchId = `cmatch_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
                    await db.prepare(`
                        INSERT OR IGNORE INTO sneak_consumer_alert_matches 
                        (id, alert_id, saved_search_id, site_id, user_id, listing_key, event_type, first_matched_at, notified_at, created_at)
                        VALUES (?, ?, ?, ?, ?, 'new_listing', ?, NULL, ?)
                    `).bind(matchId, alert.alert_id, alert.saved_search_id, alert.site_id, alert.user_id, listing.ListingKey, nowIso, nowIso).run();
                }
            }

            // 5. Send Alert Email
            stats.emailsAttempted++;

            if (dryRun) {
                stats.emailsSent++;
                continue;
            }

            const unsubToken = await createUnsubscribeToken(alert.alert_id, alert.user_id, alert.site_id, signingSecret);
            const unsubscribeUrl = `${alertsWorkerUrl}/api/alerts/unsubscribe?token=${encodeURIComponent(unsubToken)}`;
            const returnUrl = alert.return_url || 'https://coconutcoastrealtors.org/idx-test/';

            const html = renderSavedSearchAlertEmail({
                alert,
                searchName: alert.search_name,
                site: { id: alert.site_id, site_key: alert.site_key },
                branding: { display_name: alert.display_name, brokerage: alert.brokerage, primary_color: alert.primary_color },
                account: { account_name: alert.account_name },
                listings: newListings,
                totalMatches: newListings.length,
                returnUrl,
                unsubscribeUrl
            });

            const isDaily = alert.frequency === 'daily';
            const subject = isDaily
                ? `${newListings.length} new match${newListings.length === 1 ? '' : 'es'} for "${alert.search_name || 'Saved Search'}"`
                : `${newListings.length} new home${newListings.length === 1 ? '' : 's'} match your search: "${alert.search_name || 'Saved Search'}"`;

            const deliveryRes = await sendTransactionalEmail(env, {
                to: alert.consumer_email,
                subject,
                html,
                text: `${newListings.length} new matching properties for your saved search "${alert.search_name}". View them at: ${returnUrl}`
            });

            const deliveryId = `cdel_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
            const listingKeysJson = JSON.stringify(newListings.map(l => l.ListingKey));

            if (deliveryRes.status === 'sent' || deliveryRes.status === 'provider_unconfigured') {
                stats.emailsSent++;

                // Mark matches as notified
                const listingKeys = newListings.map(l => l.ListingKey);
                for (const key of listingKeys) {
                    await db.prepare(`
                        UPDATE sneak_consumer_alert_matches
                        SET notified_at = ?
                        WHERE alert_id = ? AND listing_key = ? AND event_type = 'new_listing'
                    `).bind(nowIso, alert.alert_id, key).run();
                }

                // Log delivery
                await db.prepare(`
                    INSERT INTO sneak_consumer_alert_deliveries
                    (id, alert_id, saved_search_id, site_id, user_id, frequency, match_count, listing_keys_json, status, provider, provider_message_id, created_at, sent_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `).bind(
                    deliveryId, alert.alert_id, alert.saved_search_id, alert.site_id, alert.user_id,
                    alert.frequency, newListings.length, listingKeysJson, deliveryRes.status,
                    deliveryRes.provider, deliveryRes.providerMessageId || null, nowIso, nowIso
                ).run();

                // Update alert record
                await db.prepare(`
                    UPDATE sneak_consumer_search_alerts
                    SET last_checked_at = ?,
                        last_sent_at = ?,
                        last_daily_local_date = COALESCE(?, last_daily_local_date),
                        updated_at = ?
                    WHERE id = ?
                `).bind(nowIso, nowIso, alert.currentLocalDate || null, nowIso, alert.alert_id).run();

            } else {
                stats.emailsFailed++;

                // Record failed delivery for retry
                await db.prepare(`
                    INSERT INTO sneak_consumer_alert_deliveries
                    (id, alert_id, saved_search_id, site_id, user_id, frequency, match_count, listing_keys_json, status, provider, error_code, created_at, sent_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'failed', ?, ?, ?, NULL)
                `).bind(
                    deliveryId, alert.alert_id, alert.saved_search_id, alert.site_id, alert.user_id,
                    alert.frequency, newListings.length, listingKeysJson, deliveryRes.provider,
                    deliveryRes.errorCode || 'DeliveryFailed', nowIso
                ).run();

                await db.prepare(`
                    UPDATE sneak_consumer_search_alerts
                    SET last_checked_at = ?,
                        updated_at = ?
                    WHERE id = ?
                `).bind(nowIso, nowIso, alert.alert_id).run();
            }
        }

        stats.status = 'completed';
    } catch (err) {
        stats.status = 'failed';
        stats.error = err.message;
        console.error('[ALERT WORKER PROCESSING ERROR]', err);
    } finally {
        if (!dryRun) {
            try {
                await db.prepare(`
                    UPDATE sneak_alert_runs
                    SET completed_at = datetime('now'),
                        status = ?,
                        searches_evaluated = ?,
                        matches_found = ?,
                        emails_attempted = ?,
                        emails_sent = ?,
                        emails_failed = ?,
                        error_message = ?
                    WHERE id = ?
                `).bind(
                    stats.status, stats.searchesEvaluated, stats.matchesFound,
                    stats.emailsAttempted, stats.emailsSent, stats.emailsFailed,
                    stats.error || null, runId
                ).run();
            } catch {}
        }
    }

    return stats;
}

/**
 * HTML Response helper for Unsubscribe Page
 */
function renderUnsubscribePage({ success, searchName = 'Saved Search', message }) {
    return new Response(`
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email Alert Preferences</title>
</head>
<body style="margin:0;padding:0;background-color:#0b1329;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#f8fafc;display:flex;align-items:center;justify-content:center;min-height:100vh;">
    <div style="background-color:#111e38;border:1px solid rgba(255,255,255,0.1);border-radius:12px;padding:36px 32px;max-width:440px;text-align:center;box-shadow:0 10px 30px rgba(0,0,0,0.5);margin:20px;">
        <div style="font-size:36px;margin-bottom:16px;">${success ? '🔕' : '⚠️'}</div>
        <h2 style="font-size:22px;font-weight:700;color:#ffffff;margin:0 0 12px 0;">${success ? 'Alerts Turned Off' : 'Invalid Link'}</h2>
        <p style="font-size:15px;line-height:1.5;color:#cbd5e1;margin:0 0 24px 0;">
            ${escapeHtml(message)}
        </p>
        <div style="font-size:12px;color:#64748b;border-top:1px solid rgba(255,255,255,0.08);padding-top:16px;">
            You can re-enable alerts anytime from your Saved Searches in the property search.
        </div>
    </div>
</body>
</html>
    `.trim(), {
        status: success ? 200 : 400,
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
}

export default {
    async fetch(req, env) {
        const url = new URL(req.url);
        const method = req.method.toUpperCase();

        // 1. Health & Version Check
        if (url.pathname === '/' || url.pathname === '/health' || url.pathname === '/api/alerts/version') {
            return jsonResponse({
                service: 'sneak-alerts-worker',
                build: ALERT_BUILD,
                status: 'healthy',
                emailProviderConfigured: Boolean(env?.MAILJET_API_KEY && env?.MAILJET_SECRET_KEY),
                timestamp: new Date().toISOString()
            }, 200, '*');
        }

        // 2. Unsubscribe Route (Public GET with signed token)
        if ((url.pathname === '/unsubscribe' || url.pathname === '/api/alerts/unsubscribe') && method === 'GET') {
            const token = url.searchParams.get('token');
            if (!token) {
                return renderUnsubscribePage({
                    success: false,
                    message: 'Missing or invalid unsubscribe token.'
                });
            }

            const signingSecret = env?.SNEAK_SIGNING_SECRET || 'sneak-default-token-secret-fallback-key';
            const decoded = await verifyUnsubscribeToken(token, signingSecret);

            if (!decoded || !decoded.alertId) {
                return renderUnsubscribePage({
                    success: false,
                    message: 'This unsubscribe link is invalid or has expired.'
                });
            }

            // Set alert to off
            await env.DB.prepare(`
                UPDATE sneak_consumer_search_alerts
                SET enabled = 0,
                    frequency = 'off',
                    enabled_at = NULL,
                    updated_at = datetime('now')
                WHERE id = ? AND user_id = ? AND site_id = ?
            `).bind(decoded.alertId, decoded.userId, decoded.siteId).run();

            return renderUnsubscribePage({
                success: true,
                message: 'Email alerts for this saved search have been turned off.'
            });
        }

        // 404 on unknown routes
        return jsonResponse({ error: 'NotFound', message: `Route ${method} ${url.pathname} not found.` }, 404, '*');
    },

    async scheduled(event, env, ctx) {
        console.log(`[ALERT CRON TRIGGERED] Cron: ${event?.cron || 'scheduled'}`);
        const result = await processAlerts({ db: env.DB, env, dryRun: false });
        console.log(`[ALERT CRON FINISHED] Evaluated: ${result.searchesEvaluated}, Sent: ${result.emailsSent}, Failed: ${result.emailsFailed}`);
    }
};
