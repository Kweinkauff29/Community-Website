export default {
    async fetch(req, env, ctx) {
        const url = new URL(req.url);

        // New cached endpoint
        if (url.pathname === '/api/cached-listings') {
            const results = await env.DB.prepare("SELECT * FROM listings").all();
            const headers = new Headers();
            headers.set('Access-Control-Allow-Origin', '*');
            headers.set('Content-Type', 'application/json');
            // Cache for 1 hour on the client, 1 day on the edge
            headers.set('Cache-Control', 'public, max-age=3600, s-maxage=86400');
            return new Response(JSON.stringify(results.results || []), { headers });
        }

        // Temporary endpoint to trigger sync manually
        if (url.pathname === '/api/manual-sync') {
            await this.scheduled(null, env, ctx);
            return new Response('Sync Triggered', { status: 200 });
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
        // Daily Sync Logic
        console.log("Starting Daily Listing Sync...");
        const SEL = "ListingKey,ListingId,ListPrice,UnparsedAddress,City,CountyOrParish,BedroomsTotal,BathroomsTotalInteger,LivingArea,StandardStatus,PropertyType,PropertySubType,Media,ListingContractDate,Coordinates,ModificationTimestamp,YearBuilt,LotSizeAcres,ListAgentFullName,ListOfficeName,ListOfficePhone,ListAgentMlsId";
        const baseF = "OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending') and (CountyOrParish eq 'Lee' or CountyOrParish eq 'Collier' or CountyOrParish eq 'Hendry')";

        let hasMore = true;
        let skip = 0;
        const BATCH = 200;
        const allFetchedKeys = new Set();

        while (hasMore) {
            const p = new URLSearchParams({
                '$filter': baseF,
                '$select': SEL,
                '$top': BATCH,
                '$skip': skip,
                '$orderby': 'ListingKey'
            });
            const url = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?${p}&access_token=${env.BRIDGE_TOKEN}`;
            const res = await fetch(url);
            if (!res.ok) break;
            const data = await res.json();
            const items = data.value || [];
            if (!items.length) { hasMore = false; break; }

            const statements = items.map(i => {
                allFetchedKeys.add(i.ListingKey);
                let photo = "";
                if (i.Media && i.Media.length) {
                    const sorted = i.Media.sort((a, b) => (a.Order || 0) - (b.Order || 0));
                    photo = sorted[0].MediaURL || sorted[0].MediaUrl || sorted[0].MediaURLLarge || "";
                }
                return env.DB.prepare(`
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
                );
            });

            await env.DB.batch(statements);
            skip += items.length;
            if (items.length < BATCH) hasMore = false;
        }

        // Cleanup: Remove listings in D1 that are no longer in the active OData set
        if (allFetchedKeys.size > 0) {
            const registeredKeys = await env.DB.prepare("SELECT ListingKey FROM listings").all();
            const staleKeys = registeredKeys.results.filter(row => !allFetchedKeys.has(row.ListingKey));
            if (staleKeys.length > 0) {
                const chunks = [];
                for (let i = 0; i < staleKeys.length; i += 50) {
                    chunks.push(staleKeys.slice(i, i + 50));
                }
                for (const chunk of chunks) {
                    const placeholders = chunk.map(() => "?").join(",");
                    await env.DB.prepare(`DELETE FROM listings WHERE ListingKey IN (${placeholders})`).bind(...chunk.map(r => r.ListingKey)).run();
                }
            }
        }

        console.log(`Sync Complete. Total listings synced: ${allFetchedKeys.size}`);
    }
};
