-- Migration: 0008_sneak_open_houses.sql
-- Description: Creates the generalized sneak_open_houses table (today - 1 to today + 30) without impacting legacy open_houses.

CREATE TABLE IF NOT EXISTS sneak_open_houses (
    id TEXT PRIMARY KEY,
    OpenHouseKey TEXT UNIQUE NOT NULL,
    ListingKey TEXT NOT NULL,
    OpenHouseStartTime TEXT,
    OpenHouseEndTime TEXT,
    OpenHouseDate TEXT,
    OpenHouseRemarks TEXT,
    PropertyData TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
