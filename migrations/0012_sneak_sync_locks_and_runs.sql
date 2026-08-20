-- Migration: 0012_sneak_sync_locks_and_runs.sql
-- Description: Distributed job concurrency locks and operational sync execution history.

CREATE TABLE IF NOT EXISTS sneak_sync_locks (
    job_name TEXT PRIMARY KEY,
    lock_id TEXT NOT NULL,
    acquired_at DATETIME NOT NULL,
    expires_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS sneak_sync_runs (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME,
    status TEXT NOT NULL,
    records_fetched INTEGER DEFAULT 0,
    records_upserted INTEGER DEFAULT 0,
    records_removed INTEGER DEFAULT 0,
    bridge_pages INTEGER DEFAULT 0,
    d1_operations INTEGER DEFAULT 0,
    duration_seconds REAL DEFAULT 0.0,
    error_code TEXT,
    error_summary TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sneak_sync_runs_job ON sneak_sync_runs(job_name, started_at DESC);
