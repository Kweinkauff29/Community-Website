export default {
    async fetch(req, env, ctx) {
        const url = new URL(req.url);

        if (!url.pathname.startsWith('/api/v2/')) {
            return new Response('Not allowed', { status: 403 });
        }

        // Check edge cache first
        const cache = caches.default;
        const cacheKey = new Request(req.url, { method: 'GET' });
        const cached = await cache.match(cacheKey);
        if (cached) {
            // Optional: add a fast header so you can see hits in DevTools
            const h = new Headers(cached.headers);
            h.set('CF-Cache-Status', 'HIT');
            return new Response(await cached.text(), { status: cached.status, headers: h });
        }

        // Build upstream
        const upstream = new URL('https://api.bridgedataoutput.com' + url.pathname);
        url.searchParams.forEach((v, k) => {
            if (k.toLowerCase() !== 'access_token') upstream.searchParams.set(k, v);
        });
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
                body = JSON.stringify(j);
            } catch { }
        }

        // Make it cacheable for a short time (e.g., 300s)
        headers.set('Cache-Control', 'public, s-maxage=300');
        const out = new Response(body, { status: res.status, headers });

        // Only cache successful JSON responses
        if (res.ok && ct.includes('application/json')) {
            ctx.waitUntil(cache.put(cacheKey, out.clone()));
        }

        return out;
    }
}
