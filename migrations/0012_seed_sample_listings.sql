-- Migration: 0012_seed_sample_listings.sql
-- Description: Seeds sample listings for local development testing (INSERT OR IGNORE).

INSERT OR IGNORE INTO listings (
    ListingKey, ListingId, ListPrice, UnparsedAddress, City, CountyOrParish,
    BedroomsTotal, BathroomsTotalInteger, LivingArea, StandardStatus,
    PropertyType, PropertySubType, PrimaryPhoto, ListingContractDate,
    Latitude, Longitude, ModificationTimestamp, YearBuilt, LotSizeAcres,
    ListAgentFullName, ListOfficeName, ListOfficePhone, ListAgentMlsId
) VALUES 
(
    '22400101', '22400101', 1250000, '26744 Hickory Blvd', 'Bonita Springs', 'Lee',
    4, 3, 2850, 'Active',
    'Residential', 'Single Family Residence', 'https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=800&auto=format&fit=crop&q=80', '2026-05-01',
    26.3382, -81.8214, '2026-05-10T12:00:00Z', 2021, 0.35,
    'Sarah Jenkins', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_AGENT_01'
),
(
    '22400102', '22400102', 675000, '10432 Pelican Sound Pkwy #102', 'Estero', 'Lee',
    2, 2, 1450, 'Active',
    'Residential', 'Condominium', 'https://images.unsplash.com/photo-1545324418-cc1a3fa10c00?w=800&auto=format&fit=crop&q=80', '2026-05-02',
    26.4312, -81.8152, '2026-05-11T14:30:00Z', 2018, 0.0,
    'Michael Torres', 'Gulf Coast Realty Group', '(239) 555-0144', 'AGT_TORRES_02'
),
(
    '22400103', '22400103', 2495000, '4820 Pelican Colony Blvd #1401', 'Bonita Springs', 'Lee',
    3, 4, 3400, 'Active',
    'Residential', 'High Rise (8+)', 'https://images.unsplash.com/photo-1512917774080-9991f1c4c750?w=800&auto=format&fit=crop&q=80', '2026-05-03',
    26.3315, -81.8289, '2026-05-12T09:15:00Z', 2022, 0.0,
    'Sarah Jenkins', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_AGENT_01'
),
(
    '22400104', '22400104', 450000, '8921 Coconut Rd', 'Estero', 'Lee',
    3, 2, 1780, 'Active Under Contract',
    'Residential', 'Single Family Residence', 'https://images.unsplash.com/photo-1580587771525-78b9dba3b914?w=800&auto=format&fit=crop&q=80', '2026-04-28',
    26.4189, -81.8023, '2026-05-08T16:00:00Z', 2015, 0.22,
    'David Miller', 'Sunshine State Realty', '(239) 555-0177', 'AGT_MILLER_03'
),
(
    '22400105', '22400105', 890000, '1540 Gulf Shore Blvd N', 'Naples', 'Collier',
    3, 3, 2200, 'Active',
    'Residential', 'Villa Detached', 'https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=800&auto=format&fit=crop&q=80', '2026-05-04',
    26.1550, -81.8040, '2026-05-12T11:00:00Z', 2019, 0.18,
    'Elena Rostova', 'Naples Luxury Homes', '(239) 555-0188', 'AGT_ROSTOVA_04'
),
(
    '22400106', '22400106', 525000, '2105 W First St #504', 'Fort Myers', 'Lee',
    2, 2, 1320, 'Pending',
    'Residential', 'Condominium', 'https://images.unsplash.com/photo-1502672260266-1c1ef2d93688?w=800&auto=format&fit=crop&q=80', '2026-04-20',
    26.6430, -81.8740, '2026-05-05T10:00:00Z', 2020, 0.0,
    'Sarah Jenkins', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_AGENT_01'
),
(
    '22400107', '22400107', 315000, '12040 Metro Pkwy', 'Fort Myers', 'Lee',
    0, 0, 4500, 'Active',
    'Commercial', 'Commercial', 'https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?w=800&auto=format&fit=crop&q=80', '2026-05-01',
    26.5720, -81.8550, '2026-05-10T08:00:00Z', 2012, 0.85,
    'Robert Vance', 'Commercial Florida Group', '(239) 555-0122', 'AGT_VANCE_05'
),
(
    '22400108', '22400108', 275000, 'Lot 14 Estero River Heights', 'Estero', 'Lee',
    0, 0, 0, 'Active',
    'Land', 'Residential Lot', 'https://images.unsplash.com/photo-1500382017468-9049fed747ef?w=800&auto=format&fit=crop&q=80', '2026-05-02',
    26.4350, -81.8250, '2026-05-11T09:00:00Z', NULL, 0.52,
    'Sarah Jenkins', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_AGENT_01'
);

-- Seed sample open house
INSERT OR IGNORE INTO sneak_open_houses (
    id, OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, PropertyData
) VALUES (
    'oh_sample_01', 'OH_22400101', '22400101', '2026-05-23T13:00:00Z', '2026-05-23T16:00:00Z', '2026-05-23',
    'Join us for refreshments and tour this luxury coastal home!',
    '{"ListingKey":"22400101","UnparsedAddress":"26744 Hickory Blvd","City":"Bonita Springs","ListPrice":1250000,"BedroomsTotal":4,"BathroomsTotalInteger":3,"LivingArea":2850,"PrimaryPhoto":"https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=800&auto=format&fit=crop&q=80"}'
);
