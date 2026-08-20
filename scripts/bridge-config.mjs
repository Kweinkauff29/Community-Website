/**
 * scripts/bridge-config.mjs
 * 
 * Shared configuration and constants for Bridge Interactive MLS OData feed operations.
 * NO SECRETS ARE STORED IN THIS FILE.
 */

export const BRIDGE_BASE_URL = 'https://api.bridgedataoutput.com/api/v2/OData/bsaor';
export const BRIDGE_PROPERTY_ENDPOINT = `${BRIDGE_BASE_URL}/Property`;
export const BRIDGE_OPENHOUSE_ENDPOINT = `${BRIDGE_BASE_URL}/OpenHouse`;

// Three explicit filter definitions
export const FILTER_FLORIDA_ELIGIBLE = "StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";
export const FILTER_BSAOR_ANY_STATE = "OriginatingSystemKey eq 'bsaor' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";

// LOCKED FINAL SNEAK STAGING FILTER
export const FINAL_SNEAK_LISTING_FILTER = "OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";

// 40 OData select fields (Coordinates used in place of scalar Latitude/Longitude)
export const SELECT_FIELDS = [
    'ListingKey', 'ListingId', 'ListPrice', 'OriginalListPrice',
    'UnparsedAddress', 'StreetNumber', 'StreetName', 'UnitNumber',
    'City', 'StateOrProvince', 'PostalCode', 'CountyOrParish',
    'BedroomsTotal', 'BathroomsTotalInteger', 'BathroomsFull', 'BathroomsHalf',
    'LivingArea', 'StandardStatus', 'PropertyType', 'PropertySubType',
    'ListingContractDate', 'ModificationTimestamp', 'StatusChangeTimestamp',
    'YearBuilt', 'LotSizeAcres', 'Coordinates',
    'Media', 'PublicRemarks', 'SubdivisionName',
    'ListAgentKey', 'ListAgentMlsId', 'ListAgentFullName', 'ListAgentEmail', 'ListAgentDirectPhone',
    'ListOfficeKey', 'ListOfficeMlsId', 'ListOfficeName', 'ListOfficePhone',
    'OriginatingSystemKey', 'OriginatingSystemName'
];

export const SELECT_PARAM = SELECT_FIELDS.join(',');
export const DETERMINISTIC_ORDERBY = 'ListingKey asc';
export const MAX_PAGE_SIZE = 200;
