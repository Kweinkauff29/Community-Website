-- migrations/0023_sneak_listing_media_cache.sql
-- SNEAK IDX Listing Media Array Cache Migration
-- Adds MediaJSON column to store ordered public photo URLs directly in D1

ALTER TABLE sneak_listings ADD COLUMN MediaJSON TEXT;
