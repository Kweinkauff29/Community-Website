/**
 * sneak-sync/transforms.js
 * 
 * Worker-native transformations for MLS Listings and Open House records.
 */

export function extractPrimaryPhoto(media) {
    if (!Array.isArray(media) || media.length === 0) return null;
    const sorted = [...media].sort((a, b) => (a.Order ?? 999) - (b.Order ?? 999));
    const first = sorted.find(m => m && m.MediaURL);
    return first ? first.MediaURL : null;
}

export function transformListingRecord(r) {
    const primaryPhoto = extractPrimaryPhoto(r.Media);
    let lat = r.Latitude ?? null;
    let lon = r.Longitude ?? null;
    if (lat == null && lon == null && Array.isArray(r.Coordinates) && r.Coordinates.length >= 2) {
        lon = r.Coordinates[0];
        lat = r.Coordinates[1];
    }

    return {
        ListingKey: r.ListingKey,
        ListingId: r.ListingId ?? r.ListingKey,
        ListPrice: r.ListPrice ?? null,
        OriginalListPrice: r.OriginalListPrice ?? null,
        UnparsedAddress: r.UnparsedAddress ?? null,
        StreetNumber: r.StreetNumber ?? null,
        StreetName: r.StreetName ?? null,
        UnitNumber: r.UnitNumber ?? null,
        City: r.City ?? null,
        StateOrProvince: r.StateOrProvince ?? null,
        PostalCode: r.PostalCode ?? null,
        CountyOrParish: r.CountyOrParish ?? null,
        BedroomsTotal: r.BedroomsTotal ?? null,
        BathroomsTotalInteger: r.BathroomsTotalInteger ?? null,
        BathroomsFull: r.BathroomsFull ?? null,
        BathroomsHalf: r.BathroomsHalf ?? null,
        LivingArea: r.LivingArea ?? null,
        StandardStatus: r.StandardStatus ?? null,
        PropertyType: r.PropertyType ?? null,
        PropertySubType: r.PropertySubType ?? null,
        PrimaryPhoto: primaryPhoto,
        ListingContractDate: r.ListingContractDate ?? null,
        ModificationTimestamp: r.ModificationTimestamp ?? null,
        StatusChangeTimestamp: r.StatusChangeTimestamp ?? null,
        Latitude: lat,
        Longitude: lon,
        YearBuilt: r.YearBuilt ?? null,
        LotSizeAcres: r.LotSizeAcres ?? null,
        SubdivisionName: r.SubdivisionName ?? null,
        PublicRemarks: r.PublicRemarks ?? null,
        ListAgentKey: r.ListAgentKey ?? null,
        ListAgentFullName: r.ListAgentFullName ?? null,
        ListAgentEmail: r.ListAgentEmail ?? null,
        ListAgentDirectPhone: r.ListAgentDirectPhone ?? null,
        ListAgentMlsId: r.ListAgentMlsId ?? null,
        ListOfficeKey: r.ListOfficeKey ?? null,
        ListOfficeName: r.ListOfficeName ?? null,
        ListOfficePhone: r.ListOfficePhone ?? null,
        ListOfficeMlsId: r.ListOfficeMlsId ?? null,
        InternetEntireListingDisplayYN: r.InternetEntireListingDisplayYN === false ? 0 : 1,
        InternetAddressDisplayYN: r.InternetAddressDisplayYN === false ? 0 : 1,
        OriginatingSystemKey: r.OriginatingSystemKey ?? null,
        OriginatingSystemName: r.OriginatingSystemName ?? null
    };
}

export function transformOpenHouseRecord(r) {
    return {
        id: `oh_${r.OpenHouseKey}`,
        OpenHouseKey: r.OpenHouseKey,
        ListingKey: r.ListingKey,
        OpenHouseStartTime: r.OpenHouseStartTime ?? null,
        OpenHouseEndTime: r.OpenHouseEndTime ?? null,
        OpenHouseDate: r.OpenHouseDate ?? null,
        OpenHouseRemarks: r.OpenHouseRemarks ?? null,
        OpenHouseStatus: r.OpenHouseStatus ?? 'Active',
        OpenHouseType: r.OpenHouseType ?? 'Public'
    };
}
