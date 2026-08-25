/**
 * sneak-sync/bridge.js
 * 
 * Worker-native Bridge MLS OData Client.
 * 
 * SECURITY PRINCIPLES:
 * - Uses env.BRIDGE_TOKEN directly.
 * - Throws configuration error if token is missing.
 * - Never prints or logs access tokens or token-bearing nextLinks.
 */

export const BRIDGE_PROPERTY_ENDPOINT = "https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property";
export const BRIDGE_OPENHOUSE_ENDPOINT = "https://api.bridgedataoutput.com/api/v2/OData/bsaor/OpenHouse";

export const FINAL_SNEAK_LISTING_FILTER = "OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";

export const SELECT_PARAM = [
    "ListingKey",
    "ListingId",
    "ListPrice",
    "OriginalListPrice",
    "StandardStatus",
    "PropertyType",
    "PropertySubType",
    "UnparsedAddress",
    "StreetNumber",
    "StreetName",
    "UnitNumber",
    "City",
    "StateOrProvince",
    "PostalCode",
    "CountyOrParish",
    "SubdivisionName",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "BathroomsFull",
    "BathroomsHalf",
    "LivingArea",
    "LotSizeAcres",
    "YearBuilt",
    "PublicRemarks",
    "ListingContractDate",
    "ModificationTimestamp",
    "StatusChangeTimestamp",
    "Media",
    "Coordinates",
    "ListAgentKey",
    "ListAgentMlsId",
    "ListAgentFullName",
    "ListAgentEmail",
    "ListAgentDirectPhone",
    "ListOfficeKey",
    "ListOfficeMlsId",
    "ListOfficeName",
    "ListOfficePhone",
    "InternetEntireListingDisplayYN",
    "InternetAddressDisplayYN",
    "OriginatingSystemKey",
    "OriginatingSystemName"
].join(",");

export const DETERMINISTIC_ORDERBY = "ListingKey asc";
export const MAX_PAGE_SIZE = 200;

export function sanitizeUrl(urlStr) {
    try {
        const u = new URL(urlStr);
        if (u.searchParams.has("access_token")) {
            u.searchParams.set("access_token", "[REDACTED]");
        }
        return u.toString();
    } catch {
        return "[INVALID_URL]";
    }
}

/**
 * Executes an authenticated Bridge OData request.
 */
export async function fetchBridgeOData(endpoint, params, env) {
    if (!env || !env.BRIDGE_TOKEN) {
        throw new Error("BRIDGE_TOKEN configuration missing in Worker environment");
    }

    const url = new URL(endpoint);
    for (const [k, v] of Object.entries(params || {})) {
        if (v !== undefined && v !== null) {
            url.searchParams.set(k, String(v));
        }
    }
    url.searchParams.set("access_token", env.BRIDGE_TOKEN);

    const res = await fetch(url.toString(), {
        headers: {
            "Accept": "application/json",
            "User-Agent": "SNEAK-IDX-SyncWorker/1.0"
        }
    });

    if (!res.ok) {
        const status = res.status;
        let errMsg = `Bridge API error HTTP ${status}`;
        try {
            const errJson = await res.json();
            if (errJson.message) errMsg += `: ${errJson.message}`;
        } catch {}
        throw new Error(errMsg);
    }

    return await res.json();
}
