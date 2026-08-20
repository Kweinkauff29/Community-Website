-- Migration: 0011_sneak_listings.sql
-- Description: Creates dedicated sneak_listings table and indexes for SNEAK multi-tenant IDX platform.
-- Completely isolated from legacy listings table used by public Home Search.

CREATE TABLE IF NOT EXISTS sneak_listings (
    ListingKey TEXT PRIMARY KEY,
    ListingId TEXT,
    ListPrice REAL,
    OriginalListPrice REAL,
    UnparsedAddress TEXT,
    StreetNumber TEXT,
    StreetName TEXT,
    UnitNumber TEXT,
    City TEXT,
    StateOrProvince TEXT,
    PostalCode TEXT,
    CountyOrParish TEXT,
    BedroomsTotal INTEGER,
    BathroomsTotalInteger INTEGER,
    BathroomsFull INTEGER,
    BathroomsHalf INTEGER,
    LivingArea REAL,
    StandardStatus TEXT,
    PropertyType TEXT,
    PropertySubType TEXT,
    ListingContractDate TEXT,
    ModificationTimestamp TEXT,
    StatusChangeTimestamp TEXT,
    YearBuilt INTEGER,
    LotSizeAcres REAL,
    Latitude REAL,
    Longitude REAL,
    PrimaryPhoto TEXT,
    PublicRemarks TEXT,
    SubdivisionName TEXT,
    ListAgentKey TEXT,
    ListAgentMlsId TEXT,
    ListAgentFullName TEXT,
    ListAgentEmail TEXT,
    ListAgentDirectPhone TEXT,
    ListOfficeKey TEXT,
    ListOfficeMlsId TEXT,
    ListOfficeName TEXT,
    ListOfficePhone TEXT,
    OriginatingSystemKey TEXT,
    OriginatingSystemName TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Search and Scoping Performance Indexes
CREATE INDEX IF NOT EXISTS idx_sneak_listings_status ON sneak_listings(StandardStatus);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_price ON sneak_listings(ListPrice);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_city ON sneak_listings(City);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_prop_type ON sneak_listings(PropertyType, PropertySubType);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_beds_baths ON sneak_listings(BedroomsTotal, BathroomsTotalInteger);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_agent ON sneak_listings(ListAgentMlsId);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_office ON sneak_listings(ListOfficeMlsId);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_office_key ON sneak_listings(ListOfficeKey);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_lat_lng ON sneak_listings(Latitude, Longitude);
CREATE INDEX IF NOT EXISTS idx_sneak_listings_contract_date ON sneak_listings(ListingContractDate);
