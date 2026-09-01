(function initCCORDetailHydration(global) {
  'use strict';

  function uniqueStrings(values) {
    const seen = new Set();
    const result = [];
    for (const value of values || []) {
      if (typeof value !== 'string') continue;
      const clean = value.trim();
      if (!clean || seen.has(clean)) continue;
      seen.add(clean);
      result.push(clean);
    }
    return result;
  }

  function mediaFromDetail(detail) {
    const media = Array.isArray(detail?.Media)
      ? detail.Media.map(item => typeof item === 'string' ? item : item?.MediaURL)
      : [];
    const ordered = uniqueStrings(media);
    return ordered.length ? ordered : uniqueStrings([detail?.PrimaryPhoto]);
  }

  function mediaFromEndpoint(payload) {
    return uniqueStrings(Array.isArray(payload?.media) ? payload.media : []);
  }

  function chooseGallery(detailMedia, endpointMedia, optimisticMedia) {
    const detail = uniqueStrings(detailMedia);
    const endpoint = uniqueStrings(endpointMedia);
    const optimistic = uniqueStrings(optimisticMedia);
    if (detail.length >= endpoint.length && detail.length) return detail;
    if (endpoint.length) return endpoint;
    if (detail.length) return detail;
    return optimistic;
  }

  function classifyListing(listing) {
    const type = String(listing?.PropertyType || '').toLowerCase();
    const subtype = String(listing?.PropertySubType || '').toLowerCase();
    if (type.includes('land') || subtype.includes('land') || subtype.includes('lot')) return 'land';
    if (type.includes('commercial') || type.includes('business') || subtype.includes('commercial')) return 'commercial';
    if (type.includes('lease') || type.includes('rental') || subtype.includes('rental') || subtype.includes('lease')) return 'rental';
    return 'residential';
  }

  function positiveNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) && number > 0 ? number : null;
  }

  function affirmative(value) {
    return value === true || value === 1 || String(value || '').toLowerCase() === 'true';
  }

  function meaningfulText(value) {
    if (value === null || value === undefined) return '';
    return String(value).trim();
  }

  function buildDetailFacts(listing) {
    const source = listing || {};
    const category = classifyListing(source);
    const sections = {
      category,
      overview: [],
      features: [],
      location: [],
      listingInformation: []
    };
    const add = (section, label, value, kind = 'text') => {
      if (value === null || value === undefined || value === '') return;
      sections[section].push({ label, value, kind });
    };
    const addPositive = (section, label, value, kind = 'number') => {
      const number = positiveNumber(value);
      if (number !== null) add(section, label, number, kind);
    };

    addPositive('overview', 'Current Price', source.ListPrice, 'currency');
    add('overview', 'Status', meaningfulText(source.StandardStatus));
    add('overview', 'Property Type', meaningfulText(source.PropertyType));
    add('overview', 'Property Subtype', meaningfulText(source.PropertySubType));

    if (category === 'residential' || category === 'rental') {
      addPositive('overview', 'Bedrooms', source.BedroomsTotal, 'integer');
      addPositive('overview', 'Bathrooms', source.BathroomsTotalInteger, 'number');
      addPositive('overview', 'Living Area', source.LivingArea, 'squareFeet');
    } else if (category === 'commercial') {
      addPositive('overview', 'Building Area', source.LivingArea, 'squareFeet');
    }
    addPositive('overview', 'Lot Size', source.LotSizeAcres, 'acres');
    if (category !== 'land') addPositive('overview', 'Year Built', source.YearBuilt, 'integer');

    if (affirmative(source.WaterfrontYN)) add('features', 'Waterfront', 'Yes');
    if ((category === 'residential' || category === 'rental') && affirmative(source.PoolPrivateYN)) {
      add('features', 'Private Pool', 'Yes');
    }
    if (category !== 'land') addPositive('features', 'Garage Spaces', source.GarageSpaces, 'number');
    if (category !== 'land' && affirmative(source.NewConstructionYN)) add('features', 'New Construction', 'Yes');

    add('location', 'City', meaningfulText(source.City));
    add('location', 'County', meaningfulText(source.CountyOrParish));
    add('location', 'ZIP Code', meaningfulText(source.PostalCode));
    add('location', 'Subdivision', meaningfulText(source.SubdivisionName));
    add('location', 'Zoning', meaningfulText(source.Zoning));

    add('listingInformation', 'MLS / List ID', meaningfulText(source.ListingId || source.ListingKey));
    add('listingInformation', 'Listing Contract Date', meaningfulText(source.ListingContractDate), 'date');
    const originalPrice = positiveNumber(source.OriginalListPrice);
    const currentPrice = positiveNumber(source.ListPrice);
    if (originalPrice !== null) add('listingInformation', 'Original List Price', originalPrice, 'currency');
    if (currentPrice !== null) add('listingInformation', 'Current List Price', currentPrice, 'currency');
    if (originalPrice !== null && currentPrice !== null && originalPrice > currentPrice) {
      add('listingInformation', 'Price Reduction', {
        amount: originalPrice - currentPrice,
        percent: ((originalPrice - currentPrice) / originalPrice) * 100
      }, 'priceReduction');
    }
    add('listingInformation', 'Listing Office', meaningfulText(source.ListOfficeName));
    add('listingInformation', 'Listing Agent', meaningfulText(source.ListAgentFullName));

    return sections;
  }

  function descriptionText(listing, loading = false) {
    if (listing?.PublicRemarks) return String(listing.PublicRemarks);
    return loading ? 'Loading full property details…' : 'No public remarks available.';
  }

  function listingAttribution(listing) {
    return {
      brokerage: listing?.ListOfficeName ? 'Listed by: ' + String(listing.ListOfficeName) : 'Listing brokerage not provided.',
      agent: listing?.ListAgentFullName ? 'Listing Agent: ' + String(listing.ListAgentFullName) : 'Listing agent not provided.'
    };
  }

  function createRequestCoordinator() {
    let activeListingKey = null;
    let requestId = 0;
    let controller = null;
    return {
      begin(listingKey) {
        if (controller) controller.abort();
        controller = new AbortController();
        activeListingKey = String(listingKey || '');
        requestId += 1;
        return { listingKey: activeListingKey, requestId, signal: controller.signal };
      },
      isCurrent(request) {
        return Boolean(request)
          && !request.signal.aborted
          && request.requestId === requestId
          && request.listingKey === activeListingKey;
      },
      matchesListing(request, listing) {
        if (!this.isCurrent(request) || !listing) return false;
        return String(listing.ListingKey || '') === request.listingKey
          || String(listing.ListingId || '') === request.listingKey;
      },
      cancel() {
        if (controller) controller.abort();
        controller = null;
        activeListingKey = null;
        requestId += 1;
      },
      activeKey() {
        return activeListingKey;
      }
    };
  }

  global.CCORDetailHydration = Object.freeze({
    uniqueStrings,
    mediaFromDetail,
    mediaFromEndpoint,
    chooseGallery,
    classifyListing,
    positiveNumber,
    affirmative,
    buildDetailFacts,
    descriptionText,
    listingAttribution,
    createRequestCoordinator
  });
})(window);
