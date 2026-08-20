/**
 * sneak-member/billing.js
 * 
 * Central Stripe Billing Entitlement Logic, Webhook Processing with Idempotency,
 * and Checkout/Customer Portal Integrations.
 */

import { timingSafeEqual, bufferToHex } from './auth.js';

/**
 * Central function to derive billing entitlement from Stripe subscription state.
 */
export function deriveEntitlement(subscriptionStatus, graceUntil, now = new Date()) {
    if (!subscriptionStatus || typeof subscriptionStatus !== 'string') {
        return 'inactive';
    }

    const status = subscriptionStatus.toLowerCase().trim();

    switch (status) {
        case 'active':
        case 'trialing':
            return 'active';

        case 'past_due':
            if (graceUntil) {
                const graceDate = new Date(graceUntil);
                if (now <= graceDate) {
                    return 'grace';
                }
            }
            return 'inactive';

        case 'unpaid':
        case 'canceled':
        case 'incomplete':
        case 'incomplete_expired':
        case 'paused':
        default:
            return 'inactive';
    }
}

/**
 * Verifies Stripe Webhook HMAC-SHA256 signature against raw request body.
 */
export async function verifyStripeWebhookSignature(rawBody, signatureHeader, secret, toleranceSeconds = 300) {
    if (!rawBody || !signatureHeader || !secret) return false;

    const pairs = signatureHeader.split(',').map(p => p.trim().split('='));
    let timestamp = null;
    const signatures = [];

    for (const [k, v] of pairs) {
        if (k === 't') timestamp = parseInt(v, 10);
        if (k === 'v1') signatures.push(v);
    }

    if (!timestamp || signatures.length === 0) return false;

    const now = Math.floor(Date.now() / 1000);
    if (Math.abs(now - timestamp) > toleranceSeconds) {
        return false; // Timestamp outside tolerance window
    }

    const payload = `${timestamp}.${rawBody}`;
    const enc = new TextEncoder();

    const key = await crypto.subtle.importKey(
        'raw',
        enc.encode(secret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
    );

    const sigBuffer = await crypto.subtle.sign('HMAC', key, enc.encode(payload));
    const computedHex = bufferToHex(sigBuffer);

    return signatures.some(sig => timingSafeEqual(sig, computedHex));
}

/**
 * Processes incoming Stripe Webhook event idempotently.
 */
export async function handleStripeWebhookEvent(db, event) {
    if (!event || !event.id) return { success: false, error: 'Malformed event' };

    const eventId = event.id;
    const eventType = event.type;
    const eventCreated = event.created || Math.floor(Date.now() / 1000);
    const objectId = event.data?.object?.id || null;

    // 1. Idempotency Check: Atomically reserve event in sneak_stripe_events
    try {
        await db.prepare(`
            INSERT INTO sneak_stripe_events (stripe_event_id, event_type, stripe_object_id, event_created, status, received_at)
            VALUES (?, ?, ?, ?, 'processing', datetime('now'))
        `).bind(eventId, eventType, objectId, eventCreated).run();
    } catch (err) {
        if (err.message?.includes('UNIQUE constraint') || err.message?.includes('PRIMARY KEY')) {
            // Duplicate event delivered: Acknowledge with 200 without mutating state
            return { success: true, duplicate: true, message: 'Duplicate event ignored.' };
        }
        throw err;
    }

    const now = new Date();
    const nowIso = now.toISOString();

    try {
        const obj = event.data?.object || {};

        switch (eventType) {
            case 'checkout.session.completed': {
                const customerId = obj.customer;
                const subscriptionId = obj.subscription;
                const accountId = obj.metadata?.sneak_account_id;

                if (accountId && customerId) {
                    await db.prepare(`
                        INSERT INTO sneak_account_billing (
                            account_id, stripe_customer_id, stripe_subscription_id,
                            billing_status, entitlement_status, last_stripe_event_created, updated_at
                        ) VALUES (
                            ?, ?, ?, 'active', 'active', ?, ?
                        )
                        ON CONFLICT(account_id) DO UPDATE SET
                            stripe_customer_id = excluded.stripe_customer_id,
                            stripe_subscription_id = COALESCE(excluded.stripe_subscription_id, sneak_account_billing.stripe_subscription_id),
                            billing_status = 'active',
                            entitlement_status = 'active',
                            last_stripe_event_created = excluded.last_stripe_event_created,
                            updated_at = excluded.updated_at
                    `).bind(accountId, customerId, subscriptionId || null, eventCreated, nowIso).run();
                }
                break;
            }

            case 'customer.subscription.created':
            case 'customer.subscription.updated': {
                const subId = obj.id;
                const customerId = obj.customer;
                const status = obj.status || 'active';
                const priceId = obj.items?.data?.[0]?.price?.id || null;
                const productId = obj.items?.data?.[0]?.price?.product || null;
                const cancelAtPeriodEnd = obj.cancel_at_period_end ? 1 : 0;
                const currentPeriodStart = obj.current_period_start ? new Date(obj.current_period_start * 1000).toISOString() : null;
                const currentPeriodEnd = obj.current_period_end ? new Date(obj.current_period_end * 1000).toISOString() : null;
                const trialStart = obj.trial_start ? new Date(obj.trial_start * 1000).toISOString() : null;
                const trialEnd = obj.trial_end ? new Date(obj.trial_end * 1000).toISOString() : null;

                // Load existing billing row to check grace
                const existing = await db.prepare("SELECT * FROM sneak_account_billing WHERE stripe_subscription_id = ? OR stripe_customer_id = ?").bind(subId, customerId).first();

                // Out of order safeguard: If existing event is strictly newer, do not overwrite
                if (existing?.last_stripe_event_created && existing.last_stripe_event_created > eventCreated) {
                    break;
                }

                let graceUntil = existing?.grace_until || null;
                if (status === 'past_due' && !graceUntil) {
                    graceUntil = new Date(Date.now() + (3 * 86400 * 1000)).toISOString(); // 3 day grace default
                } else if (status === 'active' || status === 'trialing') {
                    graceUntil = null; // Cleared on recovery
                }

                const entitlement = deriveEntitlement(status, graceUntil, now);

                if (existing) {
                    await db.prepare(`
                        UPDATE sneak_account_billing
                        SET stripe_subscription_id = ?,
                            stripe_price_id = ?,
                            stripe_product_id = ?,
                            billing_status = ?,
                            entitlement_status = ?,
                            trial_start = ?,
                            trial_end = ?,
                            current_period_start = ?,
                            current_period_end = ?,
                            cancel_at_period_end = ?,
                            grace_until = ?,
                            last_stripe_event_created = ?,
                            updated_at = ?
                        WHERE account_id = ?
                    `).bind(
                        subId, priceId, productId, status, entitlement,
                        trialStart, trialEnd, currentPeriodStart, currentPeriodEnd,
                        cancelAtPeriodEnd, graceUntil, eventCreated, nowIso, existing.account_id
                    ).run();
                }
                break;
            }

            case 'customer.subscription.deleted': {
                const subId = obj.id;
                const customerId = obj.customer;

                await db.prepare(`
                    UPDATE sneak_account_billing
                    SET billing_status = 'canceled',
                        entitlement_status = 'inactive',
                        cancel_at_period_end = 0,
                        grace_until = NULL,
                        last_stripe_event_created = ?,
                        updated_at = ?
                    WHERE stripe_subscription_id = ? OR stripe_customer_id = ?
                `).bind(eventCreated, nowIso, subId, customerId).run();
                break;
            }

            case 'invoice.payment_failed': {
                const customerId = obj.customer;
                const subId = obj.subscription;
                const graceUntil = new Date(Date.now() + (3 * 86400 * 1000)).toISOString(); // 3-day grace

                await db.prepare(`
                    UPDATE sneak_account_billing
                    SET billing_status = 'past_due',
                        entitlement_status = 'grace',
                        grace_until = COALESCE(grace_until, ?),
                        last_stripe_event_created = ?,
                        updated_at = ?
                    WHERE stripe_customer_id = ? OR stripe_subscription_id = ?
                `).bind(graceUntil, eventCreated, nowIso, customerId, subId).run();
                break;
            }

            case 'invoice.paid': {
                const customerId = obj.customer;
                const subId = obj.subscription;

                await db.prepare(`
                    UPDATE sneak_account_billing
                    SET billing_status = 'active',
                        entitlement_status = 'active',
                        grace_until = NULL,
                        last_stripe_event_created = ?,
                        updated_at = ?
                    WHERE stripe_customer_id = ? OR stripe_subscription_id = ?
                `).bind(eventCreated, nowIso, customerId, subId).run();
                break;
            }

            default:
                break;
        }

        // Mark processed
        await db.prepare(`
            UPDATE sneak_stripe_events
            SET status = 'processed', processed_at = datetime('now')
            WHERE stripe_event_id = ?
        `).bind(eventId).run();

        return { success: true };
    } catch (err) {
        await db.prepare(`
            UPDATE sneak_stripe_events
            SET status = 'error', error_summary = ?
            WHERE stripe_event_id = ?
        `).bind(err.message, eventId).run();
        throw err;
    }
}
