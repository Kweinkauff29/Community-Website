-- migrations/0022_sneak_idx_display_controls.sql
-- SNEAK IDX Internet & Address Display Controls Migration
-- Authoritative Bridge OData BSAOR fields: InternetEntireListingDisplayYN, InternetAddressDisplayYN

ALTER TABLE sneak_listings ADD COLUMN InternetEntireListingDisplayYN INTEGER DEFAULT 1;
ALTER TABLE sneak_listings ADD COLUMN InternetAddressDisplayYN INTEGER DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_sneak_listings_idx_display 
ON sneak_listings (InternetEntireListingDisplayYN, StandardStatus);
