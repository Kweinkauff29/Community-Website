-- Migration: 0001_sneak_accounts.sql
-- Description: Creates the sneak_accounts table for managing tenant accounts.

CREATE TABLE IF NOT EXISTS sneak_accounts (
    id TEXT PRIMARY KEY,
    member_id TEXT,
    account_name TEXT NOT NULL,
    agent_mls_id TEXT,
    office_mls_id TEXT,
    status TEXT NOT NULL DEFAULT 'active', -- 'active', 'suspended', 'inactive'
    plan TEXT NOT NULL DEFAULT 'standard',  -- 'trial', 'standard', 'pro', 'brokerage'
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
