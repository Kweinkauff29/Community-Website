-- Seed: local-demo-listings.sql
-- Description: Seeds sample listings into sneak_listings for local offline development and UI testing.
-- WARNING: DO NOT RUN THIS IN PRODUCTION.
-- Run locally via: wrangler d1 execute community-idx --local --file=seeds/local-demo-listings.sql

INSERT OR IGNORE INTO sneak_listings (
    ListingKey, ListingId, ListPrice, OriginalListPrice, UnparsedAddress, StreetNumber, StreetName, UnitNumber, City, StateOrProvince, PostalCode, CountyOrParish,
    BedroomsTotal, BathroomsTotalInteger, BathroomsFull, BathroomsHalf, LivingArea, StandardStatus,
    PropertyType, PropertySubType, PrimaryPhoto, ListingContractDate, ModificationTimestamp, StatusChangeTimestamp,
    Latitude, Longitude, YearBuilt, LotSizeAcres, SubdivisionName, PublicRemarks,
    ListAgentKey, ListAgentFullName, ListAgentEmail, ListAgentDirectPhone, ListAgentMlsId,
    ListOfficeKey, ListOfficeName, ListOfficePhone, ListOfficeMlsId,
    OriginatingSystemKey, OriginatingSystemName
) VALUES 
(
    '22400101', '22400101', 1250000, 1295000, '26744 Hickory Blvd', '26744', 'Hickory Blvd', NULL, 'Bonita Springs', 'FL', '34134', 'Lee',
    4, 3, 3, 0, 2850, 'Active',
    'Residential', 'Single Family Residence', 'https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=800&auto=format&fit=crop&q=80', '2026-05-01', '2026-05-10T12:00:00Z', '2026-05-01T08:00:00Z',
    26.3382, -81.8214, 2021, 0.35, 'Bonita Beach', 'Stunning coastal luxury home just steps from the Gulf with open concept layout, chef kitchen, and private heated pool.',
    'AGT_KEY_01', 'Sarah Jenkins', 'sarah@premiercoastrealty.demo', '(239) 555-0199', 'DEMO_AGENT_01',
    'OFF_KEY_01', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_OFFICE_01',
    'bsaor', 'Bonita Springs'
),
(
    '22400102', '22400102', 675000, 695000, '10432 Pelican Sound Pkwy #102', '10432', 'Pelican Sound Pkwy', '102', 'Estero', 'FL', '33928', 'Lee',
    2, 2, 2, 0, 1450, 'Active',
    'Residential', 'Condominium', 'https://images.unsplash.com/photo-1545324418-cc1a3fa10c00?w=800&auto=format&fit=crop&q=80', '2026-05-02', '2026-05-11T14:30:00Z', '2026-05-02T09:00:00Z',
    26.4312, -81.8152, 2018, 0.0, 'Pelican Sound', 'Turnkey bundled golf condo with serene water views, screened lanai, and access to private river club boat shuttle.',
    'AGT_KEY_02', 'Michael Torres', 'michael@gulfcoastrealty.demo', '(239) 555-0144', 'AGT_TORRES_02',
    'OFF_KEY_02', 'Gulf Coast Realty Group', '(239) 555-0144', 'OFF_GULF_02',
    'bsaor', 'Bonita Springs'
),
(
    '22400103', '22400103', 2495000, 2595000, '4820 Pelican Colony Blvd #1401', '4820', 'Pelican Colony Blvd', '1401', 'Bonita Springs', 'FL', '34134', 'Lee',
    3, 4, 3, 1, 3400, 'Active',
    'Residential', 'High Rise (8+)', 'https://images.unsplash.com/photo-1512917774080-9991f1c4c750?w=800&auto=format&fit=crop&q=80', '2026-05-03', '2026-05-12T09:15:00Z', '2026-05-03T10:00:00Z',
    26.3315, -81.8289, 2022, 0.0, 'Pelican Landing', 'Exquisite high-rise penthouse featuring panoramic views of Estero Bay and the Gulf of Mexico. Private elevator entry.',
    'AGT_KEY_01', 'Sarah Jenkins', 'sarah@premiercoastrealty.demo', '(239) 555-0199', 'DEMO_AGENT_01',
    'OFF_KEY_01', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_OFFICE_01',
    'bsaor', 'Bonita Springs'
),
(
    '22400104', '22400104', 450000, 465000, '8921 Coconut Rd', '8921', 'Coconut Rd', NULL, 'Estero', 'FL', '33928', 'Lee',
    3, 2, 2, 0, 1780, 'Active Under Contract',
    'Residential', 'Single Family Residence', 'https://images.unsplash.com/photo-1580587771525-78b9dba3b914?w=800&auto=format&fit=crop&q=80', '2026-04-28', '2026-05-08T16:00:00Z', '2026-05-08T16:00:00Z',
    26.4189, -81.8023, 2015, 0.22, 'Coconut Point Estates', 'Charming single-family home minutes from shopping and dining with fenced yard and screened patio.',
    'AGT_KEY_03', 'David Miller', 'david@sunshinerealty.demo', '(239) 555-0177', 'AGT_MILLER_03',
    'OFF_KEY_03', 'Sunshine State Realty', '(239) 555-0177', 'OFF_SUNSHINE_03',
    'bsaor', 'Bonita Springs'
),
(
    '22400105', '22400105', 890000, 920000, '1540 Gulf Shore Blvd N', '1540', 'Gulf Shore Blvd N', NULL, 'Naples', 'FL', '34102', 'Collier',
    3, 3, 3, 0, 2200, 'Active',
    'Residential', 'Villa Detached', 'https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=800&auto=format&fit=crop&q=80', '2026-05-04', '2026-05-12T11:00:00Z', '2026-05-04T12:00:00Z',
    26.1550, -81.8040, 2019, 0.18, 'Olde Naples', 'Luxurious detached villa in Olde Naples within walking distance to the beach and 5th Avenue South dining.',
    'AGT_KEY_04', 'Elena Rostova', 'elena@naplesluxury.demo', '(239) 555-0188', 'AGT_ROSTOVA_04',
    'OFF_KEY_04', 'Naples Luxury Homes', '(239) 555-0188', 'OFF_NAPLES_04',
    'bsaor', 'Bonita Springs'
),
(
    '22400106', '22400106', 525000, 540000, '2105 W First St #504', '2105', 'W First St', '504', 'Fort Myers', 'FL', '33901', 'Lee',
    2, 2, 2, 0, 1320, 'Pending',
    'Residential', 'Condominium', 'https://images.unsplash.com/photo-1502672260266-1c1ef2d93688?w=800&auto=format&fit=crop&q=80', '2026-04-20', '2026-05-05T10:00:00Z', '2026-05-05T10:00:00Z',
    26.6430, -81.8740, 2020, 0.0, 'River District Oasis', 'Downtown Fort Myers riverfront condo with resort-style amenities, infinity pool, and marina access.',
    'AGT_KEY_01', 'Sarah Jenkins', 'sarah@premiercoastrealty.demo', '(239) 555-0199', 'DEMO_AGENT_01',
    'OFF_KEY_01', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_OFFICE_01',
    'bsaor', 'Bonita Springs'
),
(
    '22400107', '22400107', 315000, 315000, '12040 Metro Pkwy', '12040', 'Metro Pkwy', NULL, 'Fort Myers', 'FL', '33966', 'Lee',
    0, 0, 0, 0, 4500, 'Active',
    'Commercial', 'Commercial', 'https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?w=800&auto=format&fit=crop&q=80', '2026-05-01', '2026-05-10T08:00:00Z', '2026-05-01T08:00:00Z',
    26.5720, -81.8550, 2012, 0.85, 'Metro Industrial Park', 'Prime flex office/warehouse building with easy access to I-75 and Page Field Airport.',
    'AGT_KEY_05', 'Robert Vance', 'robert@commfl.demo', '(239) 555-0122', 'AGT_VANCE_05',
    'OFF_KEY_05', 'Commercial Florida Group', '(239) 555-0122', 'OFF_COMM_05',
    'bsaor', 'Bonita Springs'
),
(
    '22400108', '22400108', 275000, 290000, 'Lot 14 Estero River Heights', 'Lot 14', 'Estero River Heights', NULL, 'Estero', 'FL', '33928', 'Lee',
    0, 0, 0, 0, 0, 'Active',
    'Land', 'Residential Lot', 'https://images.unsplash.com/photo-1500382017468-9049fed747ef?w=800&auto=format&fit=crop&q=80', '2026-05-02', '2026-05-11T09:00:00Z', '2026-05-02T09:00:00Z',
    26.4350, -81.8250, NULL, 0.52, 'Estero River Heights', 'Oversized half-acre residential building lot with direct gulf access canal frontage and mature oak trees.',
    'AGT_KEY_01', 'Sarah Jenkins', 'sarah@premiercoastrealty.demo', '(239) 555-0199', 'DEMO_AGENT_01',
    'OFF_KEY_01', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_OFFICE_01',
    'bsaor', 'Bonita Springs'
);

-- Seed sample open house
INSERT OR IGNORE INTO sneak_open_houses (
    id, OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, PropertyData
) VALUES (
    'oh_sample_01', 'OH_22400101', '22400101', '2026-05-23T13:00:00Z', '2026-05-23T16:00:00Z', '2026-05-23',
    'Join us for refreshments and tour this luxury coastal home!',
    '{"ListingKey":"22400101","UnparsedAddress":"26744 Hickory Blvd","City":"Bonita Springs","ListPrice":1250000,"BedroomsTotal":4,"BathroomsTotalInteger":3,"LivingArea":2850,"PrimaryPhoto":"https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=800&auto=format&fit=crop&q=80"}'
);
