-- Migration: 0011_sneak_sync_state.sql
-- Description: Creates sneak_sync_state table to track sync cursors, hydration, incremental timestamps, and reconciliation.

CREATE TABLE IF NOT EXISTS sneak_sync_state (
    sync_name TEXT PRIMARY KEY,
    last_successful_sync TEXT,
    last_cursor TEXT,
    last_full_reconciliation TEXT,
    last_record_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'idle',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
