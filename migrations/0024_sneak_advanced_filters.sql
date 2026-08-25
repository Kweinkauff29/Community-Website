-- migrations/0024_sneak_advanced_filters.sql
-- Phase 7.3A: Advanced Search Parity Fields and Index Optimization

ALTER TABLE sneak_listings ADD COLUMN WaterfrontYN INTEGER DEFAULT 0;
ALTER TABLE sneak_listings ADD COLUMN PoolPrivateYN INTEGER DEFAULT 0;
ALTER TABLE sneak_listings ADD COLUMN GarageSpaces REAL DEFAULT 0;
ALTER TABLE sneak_listings ADD COLUMN NewConstructionYN INTEGER DEFAULT 0;
ALTER TABLE sneak_listings ADD COLUMN Zoning TEXT;

CREATE INDEX IF NOT EXISTS idx_sneak_listings_subdivision ON sneak_listings(SubdivisionName);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_postal ON sneak_listings(PostalCode);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_sqft ON sneak_listings(LivingArea);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_acres ON sneak_listings(LotSizeAcres);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_year ON sneak_listings(YearBuilt);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_waterfront ON sneak_listings(WaterfrontYN);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_pool ON sneak_listings(PoolPrivateYN);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_garage ON sneak_listings(GarageSpaces);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_newconst ON sneak_listings(NewConstructionYN);
