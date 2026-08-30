-- Migration: 0029_sneak_alert_delivery_hardening.sql
-- Description: Alert Delivery Hardening, Claim-Before-Send Idempotency & Run Concurrency (Phase 7.3C2A.1)

ALTER TABLE sneak_consumer_alert_matches ADD COLUMN claim_id TEXT;
ALTER TABLE sneak_consumer_alert_matches ADD COLUMN claimed_at DATETIME;
ALTER TABLE sneak_consumer_alert_matches ADD COLUMN claim_expires_at DATETIME;
ALTER TABLE sneak_consumer_alert_matches ADD COLUMN delivery_status TEXT DEFAULT 'pending';
ALTER TABLE sneak_consumer_alert_matches ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_sneak_consumer_alert_matches_claim 
    ON sneak_consumer_alert_matches(alert_id, claim_id, delivery_status);

ALTER TABLE sneak_alert_runs ADD COLUMN emails_deferred INTEGER NOT NULL DEFAULT 0;
