/**
 * sneak-shared/idx-query.js
 * 
 * Shared SNEAK IDX Query & Filter Engine
 * Centralized repository for SQL query compilation, tenant scope enforcement,
 * spatial filters (radius, polygon, bounding box), display compliance, and
 * saved-search matching parity across Serving Worker and Alert Worker.
 */

/**
 * Builds parameterized SQL WHERE clause and bindings for tenant listing scope.
 * Fails closed on missing or invalid scope configuration.
 */
export function buildTenantListingScope(site, tableAlias = '') {
    const prefix = tableAlias ? `${tableAlias}.` : '';

    if (site.scope_type === 'market') {
        return {
            valid: true,
            clause: '1=1',
            binds: []
        };
    }

    if (site.scope_type === 'agent') {
        if (site.scope_value && typeof site.scope_value === 'string' && site.scope_value.trim()) {
            return {
                valid: true,
                clause: `${prefix}ListAgentMlsId = ?`,
                binds: [site.scope_value.trim()]
            };
        }
        // Missing agent scope value -> Fail closed
        return {
            valid: false,
            clause: '1=0',
            binds: [],
            error: 'MissingAgentScopeValue'
        };
    }

    if (site.scope_type === 'office') {
        if (site.scope_value && typeof site.scope_value === 'string' && site.scope_value.trim()) {
            const val = site.scope_value.trim();
            return {
                valid: true,
                clause: `(${prefix}ListOfficeMlsId = ? OR ${prefix}ListOfficeKey = ?)`,
                binds: [val, val]
            };
        }
        // Missing office scope value -> Fail closed
        return {
            valid: false,
            clause: '1=0',
            binds: [],
            error: 'MissingOfficeScopeValue'
        };
    }

    // Unknown or unconfigured scope_type -> Fail closed
    return {
        valid: false,
        clause: '1=0',
        binds: [],
        error: 'InvalidScopeType'
    };
}

/**
 * Validates whether a listing is eligible for IDX / Internet display.
 * Strictly fail-closed: requires InternetEntireListingDisplayYN = 1.
 */
export function isListingIdxEligible(listing) {
    if (!listing) return false;
    if (listing.InternetEntireListingDisplayYN !== 1) {
        return false;
    }
    const eligibleStatuses = ['Active', 'Active Under Contract', 'Pending'];
    if (listing.StandardStatus && !eligibleStatuses.includes(listing.StandardStatus)) {
        return false;
    }
    return true;
}

/**
 * Suppresses address fields if InternetAddressDisplayYN is not 1.
 * Strictly fail-closed: suppresses address on false or null/unknown.
 */
export function applyListingDisplayControls(item) {
    if (!item) return item;
    const transformed = { ...item };
    if (transformed.InternetAddressDisplayYN !== 1) {
        transformed.UnparsedAddress = "Address Undisclosed";
        transformed.StreetNumber = "";
        transformed.StreetName = "";
        transformed.UnitNumber = "";
    }
    return transformed;
}

/**
 * Shared filter helper that compiles parameterized SQL query filters
 * for /idx/v1/search, /idx/v1/map, and Alert Worker saved search matching.
 */
export function buildCommonListingFilters(params, site) {
    const get = (key) => (typeof params.get === 'function' ? params.get(key) : params[key]);
    const has = (key) => (typeof params.has === 'function' ? params.has(key) : (key in params && params[key] !== undefined && params[key] !== null));

    const city = (get('city') || '').substring(0, 200);
    const county = (get('county') || get('CountyOrParish') || '').substring(0, 200).trim();
    const postalCode = (get('postalCode') || get('zip') || get('PostalCode') || '').substring(0, 20).trim();
    const subdivision = (get('subdivision') || get('SubdivisionName') || '').substring(0, 200).trim();
    const minPrice = parseFloat(get('minPrice')) || null;
    const maxPrice = parseFloat(get('maxPrice')) || null;
    const beds = parseInt(get('beds'), 10) || null;
    const baths = parseInt(get('baths'), 10) || null;
    const minSqft = parseFloat(get('minSqft') || get('minLivingArea')) || null;
    const maxSqft = parseFloat(get('maxSqft') || get('maxLivingArea')) || null;
    const minAcres = parseFloat(get('minAcres') || get('minLotSizeAcres')) || null;
    const maxAcres = parseFloat(get('maxAcres') || get('maxLotSizeAcres')) || null;
    const minYear = parseInt(get('minYear') || get('minYearBuilt'), 10) || null;
    const maxYear = parseInt(get('maxYear') || get('maxYearBuilt'), 10) || null;
    const waterfront = get('waterfront');
    const pool = get('pool');
    const garage = parseInt(get('garage') || get('minGarage'), 10) || null;
    const newConstruction = get('newConstruction');
    const openHouseOnly = get('openHouse') === '1' || get('openHouseOnly') === 'true' || get('openHouse') === true;
    const priceReduced = get('priceReduced') === '1' || get('priceReduced') === 'true' || get('priceReduced') === true;
    const newListingDays = parseInt(get('newListingDays'), 10) || null;
    const zoning = (get('zoning') || '').trim();
    const propertyType = (get('propertyType') || 'sale').toLowerCase();
    const propertySubType = get('propertySubType');
    const status = get('status') || 'Active';
    const q = (get('q') || get('search') || '').substring(0, 200).trim();

    const whereClauses = [];
    const bindValues = [];

    // 1. Enforce Fail-Closed Tenant Scope
    const scope = buildTenantListingScope(site);
    if (!scope.valid) {
        return { valid: false, error: scope.error };
    }
    whereClauses.push(scope.clause);
    bindValues.push(...scope.binds);

    // 2. Internet Entire Listing Display Compliance (Fail Closed)
    whereClauses.push("InternetEntireListingDisplayYN = 1");

    // Optional agent or office filter narrowing for market-scoped sites
    if (site.scope_type === 'market') {
        const agentMlsId = get('agentMlsId');
        if (agentMlsId) {
            whereClauses.push("ListAgentMlsId = ?");
            bindValues.push(agentMlsId);
        }
        const officeMlsId = get('officeMlsId');
        if (officeMlsId) {
            whereClauses.push("(ListOfficeMlsId = ? OR ListOfficeKey = ?)");
            bindValues.push(officeMlsId, officeMlsId);
        }
    }

    // 3. Standard Status Filtering
    if (status === 'Pending') {
        whereClauses.push("(StandardStatus = 'Pending' OR StandardStatus = 'Active Under Contract')");
    } else if (status === 'Active Under Contract') {
        whereClauses.push("StandardStatus = 'Active Under Contract'");
    } else if (status === 'Closed') {
        whereClauses.push("StandardStatus = 'Closed'");
    } else if (status === 'All') {
        whereClauses.push("(StandardStatus = 'Active' OR StandardStatus = 'Active Under Contract' OR StandardStatus = 'Pending')");
    } else {
        whereClauses.push("StandardStatus = 'Active'");
    }

    // 4. Property Type Filtering
    if (propertyType === 'sale') {
        whereClauses.push("(PropertyType = 'Residential' OR PropertyType = 'Residential Income' OR PropertyType = 'Boat Dock')");
    } else if (propertyType === 'rental') {
        whereClauses.push("PropertyType = 'Residential Lease'");
    } else if (propertyType === 'commercial') {
        whereClauses.push("(PropertyType = 'Commercial Sale' OR PropertyType = 'Commercial' OR PropertyType = 'Commercial Lease' OR PropertyType = 'Business Opportunity')");
    } else if (propertyType === 'land') {
        whereClauses.push("PropertyType = 'Land'");
    } else if (propertyType && propertyType !== 'all') {
        whereClauses.push("PropertyType = ?");
        bindValues.push(propertyType);
    }

    // 5. SubType Filtering (Only apply residential expansion if propertyType is sale or rental or all)
    if (propertySubType && (propertyType === 'sale' || propertyType === 'rental' || propertyType === 'all')) {
        const subTypes = (Array.isArray(propertySubType) ? propertySubType : String(propertySubType).split(','))
            .map(s => s.trim())
            .filter(Boolean);

        if (subTypes.length > 0) {
            const expanded = [];
            subTypes.forEach(st => {
                if (st === 'Single Family Residence') { expanded.push('Single Family Residence', 'Manufactured Home'); }
                else if (st === 'Condominium') { expanded.push('Condominium', 'High Rise (8+)', 'Mid Rise (4-7)', 'Low Rise (1-3)'); }
                else if (st === 'Townhouse') { expanded.push('Townhouse'); }
                else if (st === 'Multi Family') { expanded.push('Multi Family', 'Duplex', 'Triplex', 'Quadruplex'); }
                else if (st === 'Villa') { expanded.push('Villa Attached', 'Villa Detached'); }
                else { expanded.push(st); }
            });
            const placeholders = expanded.map(() => '?').join(',');
            whereClauses.push(`PropertySubType IN (${placeholders})`);
            bindValues.push(...expanded);
        }
    }

    // 6. City Filtering
    if (city) {
        const cities = (Array.isArray(city) ? city : String(city).split(',')).map(c => c.trim()).filter(Boolean);
        if (cities.length === 1) {
            whereClauses.push("LOWER(City) = LOWER(?)");
            bindValues.push(cities[0]);
        } else if (cities.length > 1) {
            const placeholders = cities.map(() => 'LOWER(?)').join(',');
            whereClauses.push(`LOWER(City) IN (${placeholders})`);
            bindValues.push(...cities);
        }
    }

    // 7. County Filtering
    if (county) {
        whereClauses.push("LOWER(CountyOrParish) = LOWER(?)");
        bindValues.push(county);
    }

    // 8. Postal Code Filtering
    if (postalCode) {
        whereClauses.push("PostalCode = ?");
        bindValues.push(postalCode);
    }

    // 9. Subdivision Filtering
    if (subdivision) {
        whereClauses.push("LOWER(SubdivisionName) LIKE ?");
        bindValues.push(`%${subdivision.toLowerCase()}%`);
    }

    // 10. Price Range
    if (minPrice !== null && minPrice > 0) {
        whereClauses.push("ListPrice >= ?");
        bindValues.push(minPrice);
    }
    if (maxPrice !== null && maxPrice > 0) {
        whereClauses.push("ListPrice <= ?");
        bindValues.push(maxPrice);
    }

    // 11. Bedrooms / Bathrooms (Only apply to residential search)
    if (propertyType === 'sale' || propertyType === 'rental' || propertyType === 'all') {
        if (beds !== null && beds > 0) {
            whereClauses.push("BedroomsTotal >= ?");
            bindValues.push(beds);
        }
        if (baths !== null && baths > 0) {
            whereClauses.push("BathroomsTotalInteger >= ?");
            bindValues.push(baths);
        }
    }

    // 12. Size: Living Area (Sqft) min/max
    if (minSqft !== null && minSqft > 0) {
        whereClauses.push("LivingArea >= ?");
        bindValues.push(minSqft);
    }
    if (maxSqft !== null && maxSqft > 0) {
        whereClauses.push("LivingArea <= ?");
        bindValues.push(maxSqft);
    }

    // 13. Size: Lot Size (Acres) min/max
    if (minAcres !== null && minAcres > 0) {
        whereClauses.push("LotSizeAcres >= ?");
        bindValues.push(minAcres);
    }
    if (maxAcres !== null && maxAcres > 0) {
        whereClauses.push("LotSizeAcres <= ?");
        bindValues.push(maxAcres);
    }

    // 14. Year Built min/max
    if (minYear !== null && minYear > 0) {
        whereClauses.push("YearBuilt >= ?");
        bindValues.push(minYear);
    }
    if (maxYear !== null && maxYear > 0) {
        whereClauses.push("YearBuilt <= ?");
        bindValues.push(maxYear);
    }

    // 15. Wave-1 Amenities
    if (waterfront === '1' || waterfront === 'true' || waterfront === true) {
        whereClauses.push("WaterfrontYN = 1");
    }
    if (pool === '1' || pool === 'true' || pool === true) {
        whereClauses.push("PoolPrivateYN = 1");
    }
    if (garage !== null && garage > 0) {
        whereClauses.push("GarageSpaces >= ?");
        bindValues.push(garage);
    }
    if (newConstruction === '1' || newConstruction === 'true' || newConstruction === true) {
        whereClauses.push("NewConstructionYN = 1");
    }
    if (priceReduced) {
        whereClauses.push("(OriginalListPrice IS NOT NULL AND ListPrice < OriginalListPrice)");
    }
    if (newListingDays !== null && newListingDays > 0) {
        whereClauses.push("ListingContractDate >= date('now', ?)");
        bindValues.push(`-${newListingDays} days`);
    }
    if (openHouseOnly) {
        whereClauses.push("ListingKey IN (SELECT DISTINCT ListingKey FROM sneak_open_houses WHERE OpenHouseStatus = 'Active' AND (OpenHouseDate IS NULL OR OpenHouseDate >= date('now')))");
    }
    if (zoning) {
        whereClauses.push("LOWER(Zoning) = LOWER(?)");
        bindValues.push(zoning);
    }

    // 16. Unified Text Search
    if (q) {
        whereClauses.push("(ListingKey = ? OR ListingId = ? OR PostalCode = ? OR LOWER(UnparsedAddress) LIKE ? OR LOWER(City) LIKE ? OR LOWER(SubdivisionName) LIKE ? OR LOWER(ListAgentFullName) LIKE ? OR LOWER(ListAgentMlsId) = ?)");
        const likeQ = `%${q.toLowerCase()}%`;
        bindValues.push(q, q, q, likeQ, likeQ, likeQ, likeQ, q.toLowerCase());
    }

    // 17. Spatial Filtering: Polygon Mode vs. Radius Mode vs. Viewport Bounding Box Mode
    const polygonParam = get('polygon');
    if (polygonParam) {
        let parsedGeo;
        try {
            parsedGeo = typeof polygonParam === 'object' ? polygonParam : JSON.parse(polygonParam);
        } catch (e) {
            return {
                valid: false,
                status: 400,
                error: 'InvalidSpatialFilter',
                message: 'Polygon search geometry is malformed JSON.'
            };
        }

        if (!parsedGeo || parsedGeo.type !== 'Polygon' || !Array.isArray(parsedGeo.coordinates) || parsedGeo.coordinates.length !== 1) {
            return {
                valid: false,
                status: 400,
                error: 'InvalidSpatialFilter',
                message: 'Polygon search geometry must be a valid GeoJSON Polygon with a single exterior ring.'
            };
        }

        const ring = parsedGeo.coordinates[0];
        if (!Array.isArray(ring) || ring.length < 3) {
            return {
                valid: false,
                status: 400,
                error: 'InvalidSpatialFilter',
                message: 'Polygon exterior ring must contain at least 3 vertices.'
            };
        }

        const first = ring[0];
        const last = ring[ring.length - 1];
        if (!Array.isArray(first) || !Array.isArray(last) || first.length < 2 || last.length < 2) {
            return {
                valid: false,
                status: 400,
                error: 'InvalidSpatialFilter',
                message: 'Polygon coordinates are malformed.'
            };
        }

        const isClosed = (first[0] === last[0] && first[1] === last[1]);
        const uniqueVertices = isClosed ? ring.slice(0, -1) : ring;

        if (uniqueVertices.length < 3) {
            return {
                valid: false,
                status: 400,
                error: 'InvalidSpatialFilter',
                message: 'Polygon must have at least 3 unique vertices.'
            };
        }

        if (uniqueVertices.length > 40) {
            return {
                valid: false,
                status: 400,
                error: 'InvalidSpatialFilter',
                message: 'Polygon vertex count exceeds the maximum limit of 40 vertices.'
            };
        }

        let latMin = 90, latMax = -90, lngMin = 180, lngMax = -180;
        const validatedRing = [];

        for (const pt of uniqueVertices) {
            if (!Array.isArray(pt) || pt.length < 2) {
                return {
                    valid: false,
                    status: 400,
                    error: 'InvalidSpatialFilter',
                    message: 'Polygon point must be [longitude, latitude].'
                };
            }
            const lng = Number(pt[0]);
            const lat = Number(pt[1]);

            if (isNaN(lng) || isNaN(lat) || !isFinite(lng) || !isFinite(lat) ||
                lat < -90 || lat > 90 || lng < -180 || lng > 180) {
                return {
                    valid: false,
                    status: 400,
                    error: 'InvalidSpatialFilter',
                    message: 'Polygon coordinates must be valid numbers with latitude in [-90, 90] and longitude in [-180, 180].'
                };
            }

            latMin = Math.min(latMin, lat);
            latMax = Math.max(latMax, lat);
            lngMin = Math.min(lngMin, lng);
            lngMax = Math.max(lngMax, lng);
            validatedRing.push([lng, lat]);
        }

        // Close ring for edge traversal
        validatedRing.push([validatedRing[0][0], validatedRing[0][1]]);

        // Bounding box prefilter
        whereClauses.push("Latitude IS NOT NULL AND Longitude IS NOT NULL");
        whereClauses.push("Latitude >= ? AND Latitude <= ? AND Longitude >= ? AND Longitude <= ?");
        bindValues.push(latMin, latMax, lngMin, lngMax);

        // Build exact Point-in-Polygon (Ray Casting) SQL expression
        const edgeClauses = [];
        for (let i = 0; i < validatedRing.length - 1; i++) {
            const p1 = validatedRing[i];
            const p2 = validatedRing[i + 1];
            const lng1 = p1[0], lat1 = p1[1];
            const lng2 = p2[0], lat2 = p2[1];

            if (lat1 === lat2) {
                // Horizontal edge: ray along latitude will not cross
                continue;
            }

            const edgeLatMin = Math.min(lat1, lat2);
            const edgeLatMax = Math.max(lat1, lat2);
            const slope = (lng2 - lng1) / (lat2 - lat1);

            edgeClauses.push(`CASE WHEN (Latitude >= ? AND Latitude < ? AND Longitude <= (? + (Latitude - ?) * ?)) THEN 1 ELSE 0 END`);
            bindValues.push(edgeLatMin, edgeLatMax, lng1, lat1, slope);
        }

        if (edgeClauses.length > 0) {
            whereClauses.push(`((${edgeClauses.join(' + ')}) % 2) = 1`);
        } else {
            return {
                valid: false,
                status: 400,
                error: 'InvalidSpatialFilter',
                message: 'Polygon is degenerate.'
            };
        }

    } else {
        // Radius Mode vs. Viewport Mode
        const hasAnyRadius = (
            has('centerLat') ||
            has('centerLng') ||
            has('radiusMiles')
        );

        if (hasAnyRadius) {
            const centerLat = parseFloat(get('centerLat'));
            const centerLng = parseFloat(get('centerLng'));
            const radiusMiles = parseFloat(get('radiusMiles'));

            const isValidRadius = (
                !isNaN(centerLat) && !isNaN(centerLng) && !isNaN(radiusMiles) &&
                centerLat >= -90 && centerLat <= 90 &&
                centerLng >= -180 && centerLng <= 180 &&
                radiusMiles > 0 && radiusMiles <= 50
            );

            if (!isValidRadius) {
                return {
                    valid: false,
                    status: 400,
                    error: 'InvalidSpatialFilter',
                    message: 'Radius search parameters are incomplete or invalid.'
                };
            }

            // Radius Mode: Compute geographic bounding box prefilter + parameterized equirectangular distance math
            const deltaLat = radiusMiles / 69.0;
            const cosLat = Math.cos(centerLat * Math.PI / 180.0);
            const cosFactor = 69.0 * cosLat;
            const deltaLng = cosLat !== 0 ? radiusMiles / Math.abs(cosFactor) : 180.0;

            const latMin = Math.max(-90, centerLat - deltaLat);
            const latMax = Math.min(90, centerLat + deltaLat);
            const lngMin = centerLng - deltaLng;
            const lngMax = centerLng + deltaLng;
            const radiusSq = radiusMiles * radiusMiles;

            whereClauses.push("Latitude IS NOT NULL AND Longitude IS NOT NULL");
            whereClauses.push("Latitude >= ? AND Latitude <= ?");
            bindValues.push(latMin, latMax);

            if (lngMin >= -180 && lngMax <= 180) {
                whereClauses.push("Longitude >= ? AND Longitude <= ?");
                bindValues.push(lngMin, lngMax);
            } else {
                // Normalize anti-meridian
                const normLngMin = ((lngMin + 180) % 360 + 360) % 360 - 180;
                const normLngMax = ((lngMax + 180) % 360 + 360) % 360 - 180;
                whereClauses.push("(Longitude >= ? OR Longitude <= ?)");
                bindValues.push(normLngMin, normLngMax);
            }

            // Exact distance constraint
            whereClauses.push("(((Latitude - ?) * 69.0) * ((Latitude - ?) * 69.0) + ((Longitude - ?) * ?) * ((Longitude - ?) * ?)) <= ?");
            bindValues.push(centerLat, centerLat, centerLng, cosFactor, centerLng, cosFactor, radiusSq);

        } else {
            // Viewport Bounding Box Mode (north, south, east, west)
            const north = parseFloat(get('north'));
            const south = parseFloat(get('south'));
            const east = parseFloat(get('east'));
            const west = parseFloat(get('west'));

            if (!isNaN(north) && !isNaN(south) && north >= -90 && north <= 90 && south >= -90 && south <= 90) {
                const minLat = Math.min(south, north);
                const maxLat = Math.max(south, north);
                whereClauses.push("Latitude IS NOT NULL AND Latitude >= ? AND Latitude <= ?");
                bindValues.push(minLat, maxLat);
            }

            if (!isNaN(east) && !isNaN(west) && east >= -180 && east <= 180 && west >= -180 && west <= 180) {
                if (west <= east) {
                    whereClauses.push("Longitude IS NOT NULL AND Longitude >= ? AND Longitude <= ?");
                    bindValues.push(west, east);
                } else {
                    // Anti-meridian boundary
                    whereClauses.push("Longitude IS NOT NULL AND (Longitude >= ? OR Longitude <= ?)");
                    bindValues.push(west, east);
                }
            }
        }
    }

    return {
        valid: true,
        whereSQL: `WHERE ${whereClauses.join(' AND ')}`,
        whereClauses,
        bindValues
    };
}

/**
 * Converts saved search criteria state (from sneak_consumer_saved_searches)
 * into standard parameters map consumable by buildCommonListingFilters.
 */
export function buildSavedSearchListingParams(state) {
    const s = typeof state === 'string' ? JSON.parse(state) : (state || {});
    const params = new Map();

    if (s.propertyType) params.set('propertyType', s.propertyType);
    if (s.q) params.set('q', s.q);
    if (s.minPrice !== null && s.minPrice !== undefined) params.set('minPrice', String(s.minPrice));
    if (s.maxPrice !== null && s.maxPrice !== undefined) params.set('maxPrice', String(s.maxPrice));
    if (s.beds !== null && s.beds !== undefined) params.set('beds', String(s.beds));
    if (s.baths !== null && s.baths !== undefined) params.set('baths', String(s.baths));

    if (Array.isArray(s.propertySubType) && s.propertySubType.length > 0) {
        params.set('propertySubType', s.propertySubType.join(','));
    } else if (typeof s.propertySubType === 'string' && s.propertySubType.trim()) {
        params.set('propertySubType', s.propertySubType.trim());
    }

    if (s.drawerState && typeof s.drawerState === 'object') {
        const ds = s.drawerState;
        if (Array.isArray(ds.subtypes) && ds.subtypes.length > 0) {
            params.set('propertySubType', ds.subtypes.join(','));
        }
        if (ds.minSqft) params.set('minSqft', String(ds.minSqft));
        if (ds.maxSqft) params.set('maxSqft', String(ds.maxSqft));
        if (ds.minAcres) params.set('minAcres', String(ds.minAcres));
        if (ds.maxAcres) params.set('maxAcres', String(ds.maxAcres));
        if (ds.minYear) params.set('minYear', String(ds.minYear));
        if (ds.maxYear) params.set('maxYear', String(ds.maxYear));
        if (ds.county) params.set('county', ds.county);
        if (ds.zip) params.set('postalCode', ds.zip);
        if (ds.subdivision) params.set('subdivision', ds.subdivision);
        if (Array.isArray(ds.cities) && ds.cities.length > 0) params.set('city', ds.cities.join(','));
        if (ds.waterfront) params.set('waterfront', '1');
        if (ds.pool) params.set('pool', '1');
        if (ds.garage) params.set('garage', String(ds.garage));
        if (ds.newConstruction) params.set('newConstruction', '1');
        if (ds.openHouse) params.set('openHouse', '1');
        if (ds.priceReduced) params.set('priceReduced', '1');
        if (ds.newListing) params.set('newListingDays', '7');
    }

    if (s.spatialState && typeof s.spatialState === 'object') {
        const sp = s.spatialState;
        if (sp.mode === 'radius' && sp.centerLat && sp.centerLng) {
            params.set('centerLat', String(sp.centerLat));
            params.set('centerLng', String(sp.centerLng));
            params.set('radiusMiles', String(sp.radiusMiles || 5));
        } else if (sp.mode === 'polygon' && sp.polygon) {
            params.set('polygon', typeof sp.polygon === 'string' ? sp.polygon : JSON.stringify(sp.polygon));
        } else if (sp.mode === 'viewport' && sp.viewportBounds) {
            const vb = sp.viewportBounds;
            if (vb.north !== undefined) params.set('north', String(vb.north));
            if (vb.south !== undefined) params.set('south', String(vb.south));
            if (vb.east !== undefined) params.set('east', String(vb.east));
            if (vb.west !== undefined) params.set('west', String(vb.west));
        }
    }

    return params;
}

/**
 * Builds the complete WHERE SQL clause and bindings for matching listings
 * against a saved search, with strict tenant isolation.
 */
export function buildSavedSearchWhereQuery(site, stateJson) {
    const params = buildSavedSearchListingParams(stateJson);
    return buildCommonListingFilters(params, site);
}
