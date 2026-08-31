/**
 * sneak-alerts/worker.js
 * 
 * SNEAK IDX Saved Search Email Alert Engine Worker.
 * 
 * Scheduled Process & Delivery Infrastructure (Phase 7.3C2A.1 Hardened):
 * - Discovers due alerts across ASAP and Daily digest frequencies.
 * - Evaluates candidate inventory against saved search state and tenant scope.
 * - Claim-before-send atomic idempotency prevents duplicate email deliveries.
 * - Delivery status semantics: Only 'sent' marks notified_at and updates last_sent_at.
 * - 'provider_unconfigured' leaves listings retryable with delivery_status 'provider_unconfigured'.
 * - Strict secret verification: Fails closed when SNEAK_SIGNING_SECRET is absent (no fallback keys).
 * - Cryptographically signed one-click unsubscribe endpoint with clean error states.
 * - Strict zero-Bridge token, zero-GrowthZone, zero-Stripe security boundary.
 */

import { findDueAlerts, matchNewListingsForAlert } from './matcher.js';
import { renderSavedSearchAlertEmail, escapeHtml } from './email.js';
import { sendTransactionalEmail, createUnsubscribeToken, verifyUnsubscribeToken } from '../sneak-shared/email-provider.js';

export const ALERT_BUILD = '2026.08.30.7.3c2b';

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
    const signingSecret = env?.SNEAK_ALERT_UNSUBSCRIBE_SECRET || env?.SNEAK_SIGNING_SECRET;
    const providerReady = Boolean(env?.MAILJET_API_KEY && env?.MAILJET_SECRET_KEY);
    const signingReady = Boolean(signingSecret && typeof signingSecret === 'string' && signingSecret.length >= 16);
    const deliveryReady = providerReady && signingReady;

    // Delivery-Not-Ready Short Circuit (Phase 7.3C2A.1 Stabilization):
    // If delivery credentials are not ready and this is not an explicit dry-run,
    // exit safely before discovering due alerts or matching to avoid unnecessary D1 churn.
    if (!deliveryReady && !dryRun) {
        console.log('[ALERT WORKER] Delivery infrastructure not configured. Short-circuiting scheduled run.');
        return {
            skipped: true,
            reason: 'DeliveryNotConfigured',
            deliveryReady: false
        };
    }

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
        emailsWouldSend: 0,
        emailsFailed: 0,
        emailsDeferred: 0,
        dryRun
    };

    // 1. Atomic Run Lock / Overlap Protection
    try {
        if (!dryRun) {
            const lockRes = await db.prepare(`
                INSERT INTO sneak_alert_runs (id, started_at, status)
                SELECT ?, ?, 'running'
                WHERE NOT EXISTS (
                    SELECT 1 FROM sneak_alert_runs
                    WHERE status = 'running' AND started_at > datetime('now', '-5 minutes')
                )
            `).bind(runId, nowIso).run();

            if (lockRes.meta && lockRes.meta.changes === 0) {
                console.log(`[ALERT WORKER] Overlapping run detected. Skipping invocation.`);
                return { skipped: true, reason: 'OverlapLocked' };
            }
        }
    } catch (err) {
        console.error('[ALERT WORKER RUN LOCK ERROR]', err.message);
    }

    try {
        // 2. Discover Due Alerts
        const dueAlerts = await findDueAlerts(db, { limit: 150, now });
        stats.searchesEvaluated = dueAlerts.length;

        const alertsWorkerUrl = env?.ALERTS_WORKER_URL || 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev';

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

            const candidateListings = matchResult.listings;
            stats.matchesFound += candidateListings.length;

            if (dryRun) {
                stats.emailsAttempted++;
                stats.emailsWouldSend++;
                continue;
            }

            // 4. Claim-Before-Send Atomic Idempotency
            // Attempt to claim candidate listings for this run
            const claimExpiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();
            for (const listing of candidateListings) {
                const matchId = `cmatch_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
                await db.prepare(`
                    INSERT INTO sneak_consumer_alert_matches 
                    (id, alert_id, saved_search_id, site_id, user_id, listing_key, event_type, claim_id, claimed_at, claim_expires_at, delivery_status, attempt_count, first_matched_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'new_listing', ?, ?, ?, 'claimed', 1, ?, ?)
                    ON CONFLICT(alert_id, listing_key, event_type) DO UPDATE SET
                        claim_id = CASE
                            WHEN notified_at IS NULL AND (claim_expires_at IS NULL OR claim_expires_at < datetime('now') OR delivery_status IN ('failed', 'provider_unconfigured', 'secret_unconfigured', 'pending') OR claim_id = excluded.claim_id)
                            THEN excluded.claim_id
                            ELSE claim_id
                        END,
                        claimed_at = CASE
                            WHEN notified_at IS NULL AND (claim_expires_at IS NULL OR claim_expires_at < datetime('now') OR delivery_status IN ('failed', 'provider_unconfigured', 'secret_unconfigured', 'pending') OR claim_id = excluded.claim_id)
                            THEN excluded.claimed_at
                            ELSE claimed_at
                        END,
                        claim_expires_at = CASE
                            WHEN notified_at IS NULL AND (claim_expires_at IS NULL OR claim_expires_at < datetime('now') OR delivery_status IN ('failed', 'provider_unconfigured', 'secret_unconfigured', 'pending') OR claim_id = excluded.claim_id)
                            THEN excluded.claim_expires_at
                            ELSE claim_expires_at
                        END,
                        delivery_status = CASE
                            WHEN notified_at IS NULL AND (claim_expires_at IS NULL OR claim_expires_at < datetime('now') OR delivery_status IN ('failed', 'provider_unconfigured', 'secret_unconfigured', 'pending') OR claim_id = excluded.claim_id)
                            THEN 'claimed'
                            ELSE delivery_status
                        END,
                        attempt_count = CASE
                            WHEN notified_at IS NULL AND (claim_expires_at IS NULL OR claim_expires_at < datetime('now') OR delivery_status IN ('failed', 'provider_unconfigured', 'secret_unconfigured', 'pending') OR claim_id = excluded.claim_id)
                            THEN attempt_count + 1
                            ELSE attempt_count
                        END
                `).bind(
                    matchId, alert.alert_id, alert.saved_search_id, alert.site_id, alert.user_id,
                    listing.ListingKey, runId, nowIso, claimExpiresAt, nowIso, nowIso
                ).run();
            }

            // Verify which listings were successfully claimed by this execution
            const claimedRows = await db.prepare(`
                SELECT listing_key FROM sneak_consumer_alert_matches
                WHERE alert_id = ? AND claim_id = ? AND delivery_status = 'claimed' AND notified_at IS NULL
            `).bind(alert.alert_id, runId).all();

            const claimedKeySet = new Set((claimedRows.results || []).map(r => r.listing_key));
            const claimedListings = candidateListings.filter(l => claimedKeySet.has(l.ListingKey));

            if (claimedListings.length === 0) {
                // All candidates were claimed or already notified by another concurrent run
                console.log(`[ALERT WORKER] Alert ${alert.alert_id} candidate matches already claimed. Skipping duplicate email.`);
                continue;
            }

            // 5. Verify Signing Secret (Fail Closed)
            if (!signingSecret || typeof signingSecret !== 'string' || signingSecret.length < 16) {
                console.log('[ALERT WORKER] Alert signing secret not configured. Deferring delivery.');
                stats.emailsDeferred++;

                // Release claim so it can be retried once secret is provided
                await db.prepare(`
                    UPDATE sneak_consumer_alert_matches
                    SET delivery_status = 'secret_unconfigured',
                        claim_expires_at = datetime('now')
                    WHERE alert_id = ? AND claim_id = ?
                `).bind(alert.alert_id, runId).run();

                await db.prepare(`
                    UPDATE sneak_consumer_search_alerts
                    SET last_checked_at = ?,
                        updated_at = ?
                    WHERE id = ?
                `).bind(nowIso, nowIso, alert.alert_id).run();
                continue;
            }

            // 6. Send Alert Email
            stats.emailsAttempted++;

            const unsubToken = await createUnsubscribeToken(alert.alert_id, alert.user_id, alert.site_id, signingSecret);
            const unsubscribeUrl = `${alertsWorkerUrl}/api/alerts/unsubscribe?token=${encodeURIComponent(unsubToken)}`;
            const returnUrl = alert.return_url || 'https://coconutcoastrealtors.org/idx-test/';

            const html = renderSavedSearchAlertEmail({
                alert,
                searchName: alert.search_name,
                site: { id: alert.site_id, site_key: alert.site_key },
                branding: { display_name: alert.display_name, brokerage: alert.brokerage, primary_color: alert.primary_color },
                account: { account_name: alert.account_name },
                listings: claimedListings,
                totalMatches: claimedListings.length,
                returnUrl,
                unsubscribeUrl
            });

            const isDaily = alert.frequency === 'daily';
            const subject = isDaily
                ? `${claimedListings.length} new match${claimedListings.length === 1 ? '' : 'es'} for "${alert.search_name || 'Saved Search'}"`
                : `${claimedListings.length} new home${claimedListings.length === 1 ? '' : 's'} match your search: "${alert.search_name || 'Saved Search'}"`;

            const deliveryRes = await sendTransactionalEmail(env, {
                to: alert.consumer_email,
                subject,
                html,
                text: `${claimedListings.length} new matching properties for your saved search "${alert.search_name}". View them at: ${returnUrl}`
            });

            const deliveryId = `cdel_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
            const listingKeysJson = JSON.stringify(claimedListings.map(l => l.ListingKey));

            if (deliveryRes.status === 'sent') {
                stats.emailsSent++;

                // Mark matches as delivered and notified
                await db.prepare(`
                    UPDATE sneak_consumer_alert_matches
                    SET notified_at = ?,
                        delivery_status = 'sent',
                        claim_expires_at = NULL
                    WHERE alert_id = ? AND claim_id = ?
                `).bind(nowIso, alert.alert_id, runId).run();

                // Log delivery
                await db.prepare(`
                    INSERT INTO sneak_consumer_alert_deliveries
                    (id, alert_id, saved_search_id, site_id, user_id, frequency, match_count, listing_keys_json, status, provider, provider_message_id, error_code, created_at, sent_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'sent', ?, ?, NULL, ?, ?)
                `).bind(
                    deliveryId, alert.alert_id, alert.saved_search_id, alert.site_id, alert.user_id,
                    alert.frequency, claimedListings.length, listingKeysJson,
                    deliveryRes.provider, deliveryRes.providerMessageId || null, nowIso, nowIso
                ).run();

                // Update alert record (advances last_sent_at)
                await db.prepare(`
                    UPDATE sneak_consumer_search_alerts
                    SET last_checked_at = ?,
                        last_sent_at = ?,
                        last_daily_local_date = COALESCE(?, last_daily_local_date),
                        updated_at = ?
                    WHERE id = ?
                `).bind(nowIso, nowIso, alert.currentLocalDate || null, nowIso, alert.alert_id).run();

            } else if (deliveryRes.status === 'provider_unconfigured') {
                stats.emailsDeferred++;

                // Release claim so it can be retried once provider is configured
                // DO NOT set notified_at or last_sent_at!
                await db.prepare(`
                    UPDATE sneak_consumer_alert_matches
                    SET delivery_status = 'provider_unconfigured',
                        claim_expires_at = datetime('now')
                    WHERE alert_id = ? AND claim_id = ?
                `).bind(alert.alert_id, runId).run();

                // Log unconfigured delivery attempt (sent_at is NULL)
                await db.prepare(`
                    INSERT INTO sneak_consumer_alert_deliveries
                    (id, alert_id, saved_search_id, site_id, user_id, frequency, match_count, listing_keys_json, status, provider, provider_message_id, error_code, created_at, sent_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `).bind(
                    deliveryId, alert.alert_id, alert.saved_search_id, alert.site_id, alert.user_id,
                    alert.frequency, claimedListings.length, listingKeysJson, 'provider_unconfigured',
                    null, null, 'ProviderUnconfigured', nowIso, null
                ).run();

                // Update checked timestamp only (last_sent_at remains unchanged)
                await db.prepare(`
                    UPDATE sneak_consumer_search_alerts
                    SET last_checked_at = ?,
                        updated_at = ?
                    WHERE id = ?
                `).bind(nowIso, nowIso, alert.alert_id).run();

            } else {
                stats.emailsFailed++;

                // Record failed delivery and release claim for retry
                // DO NOT set notified_at or last_sent_at!
                await db.prepare(`
                    UPDATE sneak_consumer_alert_matches
                    SET delivery_status = 'failed',
                        claim_expires_at = datetime('now')
                    WHERE alert_id = ? AND claim_id = ?
                `).bind(alert.alert_id, runId).run();

                await db.prepare(`
                    INSERT INTO sneak_consumer_alert_deliveries
                    (id, alert_id, saved_search_id, site_id, user_id, frequency, match_count, listing_keys_json, status, provider, provider_message_id, error_code, created_at, sent_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `).bind(
                    deliveryId, alert.alert_id, alert.saved_search_id, alert.site_id, alert.user_id,
                    alert.frequency, claimedListings.length, listingKeysJson, 'failed',
                    deliveryRes.provider, null, deliveryRes.errorCode || 'DeliveryFailed', nowIso, null
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
                        emails_deferred = ?,
                        error_message = ?
                    WHERE id = ?
                `).bind(
                    stats.status, stats.searchesEvaluated, stats.matchesFound,
                    stats.emailsAttempted, stats.emailsSent, stats.emailsFailed,
                    stats.emailsDeferred, stats.error || null, runId
                ).run();
            } catch {}
        }
    }

    return stats;
}

/**
 * HTML Response helper for Unsubscribe Page
 */
function renderUnsubscribePage({ success, status = 200, title = 'Email Alerts', message }) {
    const icon = success ? '🔕' : (status >= 500 ? '🔧' : '⚠️');
    const headerTitle = title || (success ? 'Alerts Turned Off' : 'Invalid Link');

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
        <div style="font-size:36px;margin-bottom:16px;">${icon}</div>
        <h2 style="font-size:22px;font-weight:700;color:#ffffff;margin:0 0 12px 0;">${headerTitle}</h2>
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
        status,
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
    });
}

export default {
    async fetch(req, env) {
        const url = new URL(req.url);
        const method = req.method.toUpperCase();

        const signingSecret = env?.SNEAK_ALERT_UNSUBSCRIBE_SECRET || env?.SNEAK_SIGNING_SECRET;
        const emailConfigured = Boolean(env?.MAILJET_API_KEY && env?.MAILJET_SECRET_KEY);
        const secretConfigured = Boolean(signingSecret && typeof signingSecret === 'string' && signingSecret.length >= 16);

        // 1. Health & Version Check
        if (url.pathname === '/' || url.pathname === '/health' || url.pathname === '/api/alerts/version') {
            return jsonResponse({
                service: 'sneak-alerts-worker',
                build: ALERT_BUILD,
                status: 'healthy',
                emailProviderConfigured: emailConfigured,
                signingSecretConfigured: secretConfigured,
                deliveryReady: Boolean(emailConfigured && secretConfigured),
                timestamp: new Date().toISOString()
            }, 200, '*');
        }

        // 2. Unsubscribe Route (Public GET with signed token)
        if ((url.pathname === '/unsubscribe' || url.pathname === '/api/alerts/unsubscribe') && method === 'GET') {
            const token = url.searchParams.get('token');
            if (!token) {
                return renderUnsubscribePage({
                    success: false,
                    status: 400,
                    title: 'Invalid Link',
                    message: 'This unsubscribe link is invalid or incomplete.'
                });
            }

            if (!secretConfigured) {
                console.log('[ALERT WORKER] Alert signing secret not configured for unsubscribe verification.');
                return renderUnsubscribePage({
                    success: false,
                    status: 503,
                    title: 'Service Unavailable',
                    message: 'Service temporarily unavailable. Please try again later.'
                });
            }

            const decoded = await verifyUnsubscribeToken(token, signingSecret);

            if (!decoded || !decoded.alertId) {
                return renderUnsubscribePage({
                    success: false,
                    status: 400,
                    title: 'Invalid Link',
                    message: 'This unsubscribe link is invalid or incomplete.'
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
                status: 200,
                title: 'Alerts Turned Off',
                message: 'Email alerts for this saved search have been turned off.'
            });
        }

        // 404 on unknown routes
        return jsonResponse({ error: 'NotFound', message: `Route ${method} ${url.pathname} not found.` }, 404, '*');
    },

    async scheduled(event, env, ctx) {
        console.log(`[ALERT CRON TRIGGERED] Cron: ${event?.cron || 'scheduled'}`);
        const result = await processAlerts({ db: env.DB, env, dryRun: false });
        console.log(`[ALERT CRON FINISHED] Evaluated: ${result.searchesEvaluated}, Sent: ${result.emailsSent}, Deferred: ${result.emailsDeferred}, Failed: ${result.emailsFailed}`);
    }
};
