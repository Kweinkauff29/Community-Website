-- Seed: local-demo-listings.sql
-- Description: Seeds sample listings into sneak_listings for local offline development and UI testing.
-- DYNAMIC DATES: Generates relative dates on execution.
-- WARNING: DO NOT RUN THIS IN PRODUCTION.
-- Run locally via: wrangler d1 execute sneak-idx-staging --local --file=seeds/local-demo-listings.sql -c wrangler.sneak.toml

INSERT OR REPLACE INTO sneak_listings (
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
    'Residential', 'Single Family Residence', 'https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=800&auto=format&fit=crop&q=80',
    date('now', '-1 day'), datetime('now', '-1 hour'), datetime('now', '-2 hour'),
    26.3382, -81.8214, 2021, 0.35, 'Bonita Beach', 'Stunning coastal luxury home just steps from the Gulf with open concept layout, chef kitchen, and private heated pool.',
    'AGT_KEY_01', 'Sarah Jenkins', 'sarah@premiercoastrealty.demo', '(239) 555-0199', 'DEMO_AGENT_01',
    'OFF_KEY_01', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_OFFICE_01',
    'bsaor', 'Bonita Springs'
),
(
    '22400102', '22400102', 675000, 695000, '10432 Pelican Sound Pkwy #102', '10432', 'Pelican Sound Pkwy', '102', 'Estero', 'FL', '33928', 'Lee',
    2, 2, 2, 0, 1450, 'Active',
    'Residential', 'Condominium', 'https://images.unsplash.com/photo-1545324418-cc1a3fa10c00?w=800&auto=format&fit=crop&q=80',
    date('now', '-2 days'), datetime('now', '-2 hours'), datetime('now', '-3 hours'),
    26.4312, -81.8152, 2018, 0.0, 'Pelican Sound', 'Turnkey bundled golf condo with serene water views, screened lanai, and access to private river club boat shuttle.',
    'AGT_KEY_02', 'Michael Torres', 'michael@gulfcoastrealty.demo', '(239) 555-0144', 'AGT_TORRES_02',
    'OFF_KEY_02', 'Gulf Coast Realty Group', '(239) 555-0144', 'OFF_GULF_02',
    'bsaor', 'Bonita Springs'
),
(
    '22400103', '22400103', 2495000, 2595000, '4820 Pelican Colony Blvd #1401', '4820', 'Pelican Colony Blvd', '1401', 'Bonita Springs', 'FL', '34134', 'Lee',
    3, 4, 3, 1, 3400, 'Active',
    'Residential', 'High Rise (8+)', 'https://images.unsplash.com/photo-1512917774080-9991f1c4c750?w=800&auto=format&fit=crop&q=80',
    date('now', '-3 days'), datetime('now', '-3 hours'), datetime('now', '-4 hours'),
    26.3315, -81.8289, 2022, 0.0, 'Pelican Landing', 'Exquisite high-rise penthouse featuring panoramic views of Estero Bay and the Gulf of Mexico. Private elevator entry.',
    'AGT_KEY_01', 'Sarah Jenkins', 'sarah@premiercoastrealty.demo', '(239) 555-0199', 'DEMO_AGENT_01',
    'OFF_KEY_01', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_OFFICE_01',
    'bsaor', 'Bonita Springs'
),
(
    '22400104', '22400104', 450000, 465000, '8921 Coconut Rd', '8921', 'Coconut Rd', NULL, 'Estero', 'FL', '33928', 'Lee',
    3, 2, 2, 0, 1780, 'Active Under Contract',
    'Residential', 'Single Family Residence', 'https://images.unsplash.com/photo-1580587771525-78b9dba3b914?w=800&auto=format&fit=crop&q=80',
    date('now', '-4 days'), datetime('now', '-4 hours'), datetime('now', '-5 hours'),
    26.4189, -81.8023, 2015, 0.22, 'Coconut Point Estates', 'Charming single-family home minutes from shopping and dining with fenced yard and screened patio.',
    'AGT_KEY_03', 'David Miller', 'david@sunshinerealty.demo', '(239) 555-0177', 'AGT_MILLER_03',
    'OFF_KEY_03', 'Sunshine State Realty', '(239) 555-0177', 'OFF_SUNSHINE_03',
    'bsaor', 'Bonita Springs'
),
(
    '22400105', '22400105', 890000, 920000, '1540 Gulf Shore Blvd N', '1540', 'Gulf Shore Blvd N', NULL, 'Naples', 'FL', '34102', 'Collier',
    3, 3, 3, 0, 2200, 'Active',
    'Residential', 'Villa Detached', 'https://images.unsplash.com/photo-1600585154340-be6161a56a0c?w=800&auto=format&fit=crop&q=80',
    date('now', '-5 days'), datetime('now', '-5 hours'), datetime('now', '-6 hours'),
    26.1550, -81.8040, 2019, 0.18, 'Olde Naples', 'Luxurious detached villa in Olde Naples within walking distance to the beach and 5th Avenue South dining.',
    'AGT_KEY_04', 'Elena Rostova', 'elena@naplesluxury.demo', '(239) 555-0188', 'AGT_ROSTOVA_04',
    'OFF_KEY_04', 'Naples Luxury Homes', '(239) 555-0188', 'OFF_NAPLES_04',
    'bsaor', 'Bonita Springs'
),
(
    '22400106', '22400106', 525000, 525000, '23101 Fashion Dr #304', '23101', 'Fashion Dr', '304', 'Estero', 'FL', '33928', 'Lee',
    2, 2, 2, 0, 1320, 'Active',
    'Residential', 'Condominium', 'https://images.unsplash.com/photo-1502672260266-1c1ef2d93688?w=800&auto=format&fit=crop&q=80',
    date('now', '-6 days'), datetime('now', '-6 hours'), datetime('now', '-7 hours'),
    26.4380, -81.7980, 2017, 0.0, 'Villagio', 'Modern condo with upgraded quartz counters, stainless appliances, and attached garage in gated community.',
    'AGT_KEY_01', 'Sarah Jenkins', 'sarah@premiercoastrealty.demo', '(239) 555-0199', 'DEMO_AGENT_01',
    'OFF_KEY_01', 'Premier Coast Realty LLC', '(239) 555-0199', 'DEMO_OFFICE_01',
    'bsaor', 'Bonita Springs'
);
