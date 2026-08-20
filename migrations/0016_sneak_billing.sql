-- Migration: 0016_sneak_billing.sql
-- Description: Stripe billing model, customer/subscription tracking, entitlement states, and webhook event idempotency.

CREATE TABLE IF NOT EXISTS sneak_account_billing (
    account_id TEXT PRIMARY KEY REFERENCES sneak_accounts(id) ON DELETE CASCADE,
    stripe_customer_id TEXT UNIQUE,
    stripe_subscription_id TEXT UNIQUE,
    stripe_price_id TEXT,
    stripe_product_id TEXT,
    billing_status TEXT NOT NULL DEFAULT 'none', -- 'none', 'trialing', 'active', 'past_due', 'unpaid', 'canceled', 'incomplete', 'incomplete_expired', 'paused'
    entitlement_status TEXT NOT NULL DEFAULT 'inactive', -- 'active', 'grace', 'inactive'
    trial_start DATETIME,
    trial_end DATETIME,
    current_period_start DATETIME,
    current_period_end DATETIME,
    cancel_at_period_end INTEGER DEFAULT 0,
    grace_until DATETIME,
    last_stripe_event_created INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_billing_customer ON sneak_account_billing(stripe_customer_id);
CREATE INDEX IF NOT EXISTS idx_sneak_billing_subscription ON sneak_account_billing(stripe_subscription_id);
CREATE INDEX IF NOT EXISTS idx_sneak_billing_entitlement ON sneak_account_billing(entitlement_status);

CREATE TABLE IF NOT EXISTS sneak_stripe_events (
    stripe_event_id TEXT PRIMARY KEY,
    event_type TEXT,
    stripe_object_id TEXT,
    event_created INTEGER,
    received_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    processed_at DATETIME,
    status TEXT, -- 'processed', 'ignored', 'duplicate', 'error'
    error_summary TEXT
);

CREATE INDEX IF NOT EXISTS idx_sneak_stripe_events_created ON sneak_stripe_events(event_created DESC);
