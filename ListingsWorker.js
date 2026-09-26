export default {
    async fetch(req, env, ctx) {
        const url = new URL(req.url);

        // New cached endpoint
        if (url.pathname === '/api/cached-listings') {
            const results = await env.DB.prepare("SELECT * FROM listings").all();
            const headers = new Headers();
            headers.set('Access-Control-Allow-Origin', '*');
            headers.set('Content-Type', 'application/json');
            // Cache for 1 min on client, 3 min on edge so 15-min cron updates propagate promptly
            headers.set('Cache-Control', 'public, max-age=60, s-maxage=180');
            return new Response(JSON.stringify(results.results || []), { headers });
        }

        if (url.pathname === '/api/cached-openhouses') {
            const results = await env.DB.prepare("SELECT * FROM open_houses").all();
            const headers = new Headers();
            headers.set('Access-Control-Allow-Origin', '*');
            headers.set('Content-Type', 'application/json');
            headers.set('Cache-Control', 'public, max-age=60, s-maxage=60'); // Fresh cache
            
            const CCOR_CITIES = ['BONITA SPRINGS', 'ESTERO'];

            // Format to match what frontend expects and filter to Bonita/Estero (BER/CCOR)
            const data = (results.results || []).map(row => {
                let property = null;
                try { property = JSON.parse(row.PropertyData); } catch(e) {}
                return {
                    oh: {
                        OpenHouseKey: row.OpenHouseKey,
                        ListingKey: row.ListingKey,
                        OpenHouseStartTime: row.OpenHouseStartTime,
                        OpenHouseEndTime: row.OpenHouseEndTime,
                        OpenHouseDate: row.OpenHouseDate,
                        OpenHouseRemarks: row.OpenHouseRemarks
                    },
                    property
                };
            }).filter(x => {
                if (!x.property || !x.property.City) return false;
                const city = x.property.City.toUpperCase().trim();
                const orig = (x.property.OriginatingSystemName || '').toLowerCase();
                const agentId = (x.property.ListAgentMlsId || '').toUpperCase();
                const officeId = (x.property.ListOfficeMlsId || '').toUpperCase();
                return CCOR_CITIES.includes(city) || orig.includes('bonita') || agentId.startsWith('B') || officeId.startsWith('B');
            });

            return new Response(JSON.stringify(data), { headers });
        }

        // Temporary endpoints to trigger sync manually
        if (url.pathname === '/api/manual-sync') {
            await this.syncListings(env);
            await this.syncOpenHouses(env);
            return new Response('Sync Triggered (Listings + Open Houses)', { status: 200 });
        }

        if (url.pathname === '/api/sync-listings') {
            await this.syncListings(env);
            return new Response('Listings synced successfully', { status: 200 });
        }

        if (url.pathname === '/api/sync-openhouses') {
            await this.syncOpenHouses(env);
            return new Response('Open houses synced successfully', { status: 200 });
        }

        // Allow basic API access
        if (!url.pathname.startsWith('/api/v2/') && url.pathname !== '/api/debug-keys') {
            return new Response('Not allowed', { status: 403 });
        }

        if (url.pathname === '/api/debug-keys') {
            const testUrl = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$top=1&access_token=${env.BRIDGE_TOKEN}`;
            const res = await fetch(testUrl);
            const data = await res.json();
            const keys = data.value && data.value[0] ? Object.keys(data.value[0]) : [];
            return new Response(JSON.stringify(keys), { headers: { 'Content-Type': 'application/json' } });
        }

        // Check edge cache first for OData requests
        const cache = caches.default;
        const cacheKey = new Request(req.url, { method: 'GET' });
        const cached = await cache.match(cacheKey);
        if (cached) {
            const h = new Headers(cached.headers);
            h.set('CF-Cache-Status', 'HIT');
            return new Response(await cached.text(), { status: cached.status, headers: h });
        }

        // Build upstream URL
        const upstream = new URL('https://api.bridgedataoutput.com' + url.pathname);
        const EXCLUDE = ['access_token', '_limitmedia'];
        url.searchParams.forEach((v, k) => {
            if (!EXCLUDE.includes(k.toLowerCase())) upstream.searchParams.set(k, v);
        });

        // Use the same token
        upstream.searchParams.set('access_token', env.BRIDGE_TOKEN);

        const res = await fetch(upstream, { headers: { Accept: 'application/json' } });
        const headers = new Headers(res.headers);
        headers.set('Access-Control-Allow-Origin', '*');

        let body = await res.text();
        const ct = headers.get('content-type') || '';

        if (ct.includes('application/json')) {
            try {
                const j = JSON.parse(body);
                const rewrite = (u) => {
                    if (!u) return u;
                    const p = new URL(u);
                    p.searchParams.delete('access_token');
                    return `${url.origin}${p.pathname}?${p.searchParams.toString()}`;
                };
                if (j['@odata.nextLink']) j['@odata.nextLink'] = rewrite(j['@odata.nextLink']);
                if (j.next) j.next = rewrite(j.next);

                if (url.searchParams.get('_limitMedia') === '1' && Array.isArray(j.value)) {
                    j.value.forEach(p => {
                        if (p.Media && Array.isArray(p.Media) && p.Media.length > 1) {
                            p.Media.sort((a, b) => (a.Order || 0) - (b.Order || 0));
                            p.Media = [p.Media[0]];
                        }
                    });
                }
                body = JSON.stringify(j);
            } catch { }
        }

        headers.set('Cache-Control', 'public, s-maxage=300');
        const out = new Response(body, { status: res.status, headers });
        if (res.ok && ct.includes('application/json')) {
            ctx.waitUntil(cache.put(cacheKey, out.clone()));
        }
        return out;
    },

    async scheduled(event, env, ctx) {
        // Determine current hour in Eastern Time (America/New_York)
        const estFormatter = new Intl.DateTimeFormat('en-US', {
            timeZone: 'America/New_York',
            hour: 'numeric',
            hourCycle: 'h23'
        });
        const currentHour = parseInt(estFormatter.format(new Date(event?.scheduledTime || Date.now())), 10);
        console.log(`[Scheduled Sync] Triggered at ${new Date().toISOString()} (Eastern Hour: ${currentHour}:00 EST)`);

        // Time window: 9 AM to 9 PM EST (9 to 21 inclusive). No sync at night.
        if (currentHour < 9 || currentHour > 21) {
            console.log(`[Scheduled Sync] Hour ${currentHour}:00 EST is outside 9 AM - 9 PM window. Skipping night sync (0 writes).`);
            return;
        }

        // Open houses: Every 2 hours within 9am-9pm EST (9, 11, 13, 15, 17, 19, 21 -> 7 runs/day)
        const OPEN_HOUSE_HOURS = [9, 11, 13, 15, 17, 19, 21];

        // Listings: 3 times during 9am-9pm EST (9:00 AM, 3:00 PM, 9:00 PM EST -> 9, 15, 21 -> 3 runs/day)
        const LISTING_HOURS = [9, 15, 21];

        const shouldSyncListings = LISTING_HOURS.includes(currentHour);
        const shouldSyncOpenHouses = OPEN_HOUSE_HOURS.includes(currentHour);

        if (shouldSyncListings) {
            console.log(`[Scheduled Sync] Starting listings sync for ${currentHour}:00 EST...`);
            await this.syncListings(env);
        }

        if (shouldSyncOpenHouses) {
            console.log(`[Scheduled Sync] Starting open houses sync for ${currentHour}:00 EST...`);
            await this.syncOpenHouses(env);
        }
    },

    async syncListings(env) {
        console.log("Starting Listing Sync...");
        const SEL = "ListingKey,ListingId,ListPrice,UnparsedAddress,City,CountyOrParish,BedroomsTotal,BathroomsTotalInteger,LivingArea,StandardStatus,PropertyType,PropertySubType,Media,ListingContractDate,Coordinates,ModificationTimestamp,YearBuilt,LotSizeAcres,ListAgentFullName,ListOfficeName,ListOfficePhone,ListAgentMlsId";
        const baseF = "OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending') and (CountyOrParish eq 'Lee' or CountyOrParish eq 'Collier') and (toupper(City) eq 'BONITA SPRINGS' or toupper(City) eq 'ESTERO' or toupper(City) eq 'NAPLES' or toupper(City) eq 'FORT MYERS' or toupper(City) eq 'FT MYERS' or toupper(City) eq 'FT. MYERS')";

        // 1. Fetch existing listing keys and modification timestamps from D1 for smart diffing
        const existingMap = new Map();
        try {
            const existingRecords = await env.DB.prepare("SELECT ListingKey, ModificationTimestamp FROM listings").all();
            for (const row of (existingRecords.results || [])) {
                existingMap.set(row.ListingKey, row.ModificationTimestamp || '');
            }
            console.log(`Loaded ${existingMap.size} existing listings from D1 for diffing.`);
        } catch (e) {
            console.error("Failed to load existing listings for diffing, falling back to full insert:", e);
        }

        const BATCH = 200;
        const allFetchedKeys = new Set();
        let updatedCount = 0;
        let skippedCount = 0;
        
        const p = new URLSearchParams({
            '$filter': baseF,
            '$select': SEL,
            '$top': BATCH
        });
        
        let next = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?${p}&access_token=${env.BRIDGE_TOKEN}`;

        while (next) {
            const res = await fetch(next);
            if (!res.ok) {
                console.error("Bridge API error during sync:", res.status);
                break;
            }
            const data = await res.json();
            const items = data.value || [];
            if (!items.length) break;

            const statements = [];
            for (const i of items) {
                allFetchedKeys.add(i.ListingKey);

                // Diff check: if listing already exists with identical ModificationTimestamp, skip D1 write
                if (existingMap.has(i.ListingKey) && existingMap.get(i.ListingKey) === (i.ModificationTimestamp || '')) {
                    skippedCount++;
                    continue;
                }

                let photo = "";
                if (i.Media && i.Media.length) {
                    const sorted = i.Media.sort((a, b) => (a.Order || 0) - (b.Order || 0));
                    photo = sorted[0].MediaURL || sorted[0].MediaUrl || sorted[0].MediaURLLarge || "";
                }
                statements.push(env.DB.prepare(`
                    INSERT OR REPLACE INTO listings (
                        ListingKey, ListingId, ListPrice, UnparsedAddress, City, CountyOrParish, 
                        BedroomsTotal, BathroomsTotalInteger, LivingArea, StandardStatus, 
                        PropertyType, PropertySubType, PrimaryPhoto, ListingContractDate, 
                        Latitude, Longitude, ModificationTimestamp, YearBuilt, LotSizeAcres,
                        ListAgentFullName, ListOfficeName, ListOfficePhone, ListAgentMlsId
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `).bind(
                    i.ListingKey, i.ListingId, i.ListPrice, i.UnparsedAddress, i.City, i.CountyOrParish,
                    i.BedroomsTotal, i.BathroomsTotalInteger, i.LivingArea, i.StandardStatus,
                    i.PropertyType, i.PropertySubType, photo, i.ListingContractDate,
                    i.Coordinates?.[1] || null, i.Coordinates?.[0] || null, i.ModificationTimestamp,
                    i.YearBuilt || null, i.LotSizeAcres || null,
                    i.ListAgentFullName || null, i.ListOfficeName || null, i.ListOfficePhone || null,
                    i.ListAgentMlsId || null
                ));
                updatedCount++;
            }

            if (statements.length > 0) {
                await env.DB.batch(statements);
            }
            
            next = data['@odata.nextLink'] || null;
            if (next && !next.includes('access_token')) {
                next += (next.includes('?') ? '&' : '?') + 'access_token=' + env.BRIDGE_TOKEN;
            }
        }

        // Cleanup: Remove listings in D1 that are no longer in the active OData set
        if (allFetchedKeys.size > 0 && existingMap.size > 0) {
            const staleKeys = [];
            for (const key of existingMap.keys()) {
                if (!allFetchedKeys.has(key)) {
                    staleKeys.push(key);
                }
            }
            if (staleKeys.length > 0) {
                for (let i = 0; i < staleKeys.length; i += 50) {
                    const chunk = staleKeys.slice(i, i + 50);
                    const placeholders = chunk.map(() => "?").join(",");
                    await env.DB.prepare(`DELETE FROM listings WHERE ListingKey IN (${placeholders})`).bind(...chunk).run();
                }
                console.log(`Removed ${staleKeys.length} stale listings from D1.`);
            }
        }

        console.log(`Sync Complete. Fetched: ${allFetchedKeys.size} | Updated: ${updatedCount} | Skipped unchanged: ${skippedCount}`);
    },

    async syncOpenHouses(env) {
        const EVENT_START = '2026-09-25';
        const EVENT_END = '2026-09-27';
        const ohFilter = `(OpenHouseStatus eq 'Active' or OpenHouseStatus eq null) and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge ${EVENT_START} and OpenHouseDate le ${EVENT_END}`;
        const ohURL = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(ohFilter)}&$top=200&$orderby=OpenHouseStartTime asc&access_token=${env.BRIDGE_TOKEN}`;
        
        let ohRec = [];
        let next = ohURL;
        while (next) {
            const res = await fetch(next);
            if (!res.ok) break;
            const d = await res.json();
            ohRec.push(...(d.value || []));
            next = d['@odata.nextLink'] || null;
            if (next && !next.includes('access_token')) {
                next += (next.includes('?') ? '&' : '?') + 'access_token=' + env.BRIDGE_TOKEN;
            }
        }
        
        const listingKeys = [...new Set(ohRec.map(r => r.ListingKey))];
        let properties = [];
        const PROP_SEL = 'ListingKey,ListingId,UnparsedAddress,City,PostalCode,ListPrice,PropertyType,PropertySubType,BedroomsTotal,BathroomsTotalInteger,LivingArea,LotSizeAcres,YearBuilt,StandardStatus,SubdivisionName,ListAgentFullName,ListAgentEmail,ListAgentDirectPhone,ListAgentKey,ListAgentMlsId,ListOfficeName,ListOfficePhone,ListOfficeMlsId,OriginatingSystemName,PublicRemarks,Coordinates,Media';
        
        for (let i = 0; i < listingKeys.length; i += 25) {
            const chunk = listingKeys.slice(i, i + 25);
            const batchFilter = chunk.map(k => `ListingKey eq '${k}'`).join(' or ');
            const pURL = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$filter=${encodeURIComponent(`(${batchFilter})`)}&$top=100&$select=${PROP_SEL}&access_token=${env.BRIDGE_TOKEN}`;
            const pres = await fetch(pURL);
            if (pres.ok) {
                const pd = await pres.json();
                properties.push(...(pd.value || []));
            }
        }

        const propMap = new Map(properties.map(p => [p.ListingKey, p]));
        const CCOR_CITIES = ['BONITA SPRINGS', 'ESTERO'];

        // Filter Open House records to only Bonita / Estero / Bonita agents
        const filteredOhRec = ohRec.filter(oh => {
            const p = propMap.get(oh.ListingKey);
            if (!p || !p.City) return false;
            const city = p.City.toUpperCase().trim();
            const orig = (p.OriginatingSystemName || '').toLowerCase();
            const agentId = (p.ListAgentMlsId || '').toUpperCase();
            const officeId = (p.ListOfficeMlsId || '').toUpperCase();
            return CCOR_CITIES.includes(city) || orig.includes('bonita') || agentId.startsWith('B') || officeId.startsWith('B');
        });

        // 1. Fetch existing open houses for diffing
        const existingMap = new Map();
        try {
            const existing = await env.DB.prepare("SELECT OpenHouseKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, length(PropertyData) as len FROM open_houses").all();
            for (const row of (existing.results || [])) {
                existingMap.set(row.OpenHouseKey, `${row.OpenHouseStartTime}|${row.OpenHouseEndTime}|${row.OpenHouseDate}|${row.OpenHouseRemarks}|${row.len}`);
            }
        } catch (e) {
            console.error("Failed to load existing open houses for diffing:", e);
        }

        const statements = [];
        let ohSkipped = 0;
        let ohUpdated = 0;
        for (const oh of filteredOhRec) {
            const key = oh.OpenHouseKey || oh.ListingKey;
            const p = propMap.get(oh.ListingKey) || null;
            const pStr = JSON.stringify(p);
            const sig = `${oh.OpenHouseStartTime}|${oh.OpenHouseEndTime}|${oh.OpenHouseDate}|${oh.OpenHouseRemarks}|${pStr.length}`;
            if (existingMap.has(key) && existingMap.get(key) === sig) {
                ohSkipped++;
                continue;
            }
            statements.push(env.DB.prepare(`
                INSERT OR REPLACE INTO open_houses (
                    OpenHouseKey, ListingKey, OpenHouseStartTime, OpenHouseEndTime, OpenHouseDate, OpenHouseRemarks, PropertyData
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            `).bind(
                key, oh.ListingKey, oh.OpenHouseStartTime, oh.OpenHouseEndTime, oh.OpenHouseDate, oh.OpenHouseRemarks, pStr
            ));
            ohUpdated++;
        }

        // Batch execution in chunks of 50
        for (let i = 0; i < statements.length; i += 50) {
            await env.DB.batch(statements.slice(i, i + 50));
        }

        // Cleanup: remove all open houses in DB that are not in filteredOhRec
        const validOhKeys = new Set(filteredOhRec.map(oh => oh.OpenHouseKey || oh.ListingKey));
        const staleKeys = [];
        for (const key of existingMap.keys()) {
            if (!validOhKeys.has(key)) {
                staleKeys.push(key);
            }
        }
        for (let i = 0; i < staleKeys.length; i += 50) {
            const chunk = staleKeys.slice(i, i + 50);
            const placeholders = chunk.map(() => "?").join(",");
            await env.DB.prepare(`DELETE FROM open_houses WHERE OpenHouseKey IN (${placeholders})`).bind(...chunk).run();
        }
        console.log(`Synced Open Houses. Total: ${filteredOhRec.length} | Updated: ${ohUpdated} | Skipped unchanged: ${ohSkipped} | Stale removed: ${staleKeys.length}`);
    }
};
