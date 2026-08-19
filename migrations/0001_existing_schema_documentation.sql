-- Migration: 0001_existing_schema_documentation.sql
-- Description: Documents existing tables used by ListingsWorker and OpenhouseWorker.
-- Uses CREATE TABLE IF NOT EXISTS to prevent modifying existing production tables.

CREATE TABLE IF NOT EXISTS listings (
    ListingKey TEXT PRIMARY KEY,
    ListingId TEXT,
    ListPrice REAL,
    UnparsedAddress TEXT,
    City TEXT,
    CountyOrParish TEXT,
    BedroomsTotal INTEGER,
    BathroomsTotalInteger INTEGER,
    LivingArea REAL,
    StandardStatus TEXT,
    PropertyType TEXT,
    PropertySubType TEXT,
    PrimaryPhoto TEXT,
    ListingContractDate TEXT,
    Latitude REAL,
    Longitude REAL,
    ModificationTimestamp TEXT,
    YearBuilt INTEGER,
    LotSizeAcres REAL,
    ListAgentFullName TEXT,
    ListOfficeName TEXT,
    ListOfficePhone TEXT,
    ListAgentMlsId TEXT
);

CREATE TABLE IF NOT EXISTS open_houses (
    OpenHouseKey TEXT PRIMARY KEY,
    ListingKey TEXT,
    OpenHouseStartTime TEXT,
    OpenHouseEndTime TEXT,
    OpenHouseDate TEXT,
    OpenHouseRemarks TEXT,
    PropertyData TEXT
);
