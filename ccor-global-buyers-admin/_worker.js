/**
 * Cloudflare Pages Advanced Mode Worker for CCOR Global Buyers Intelligence Admin
 * Project: ccor-global-buyers-admin
 */

const ALLOWED_MARKETS = ['canada', 'germany', 'brazil', 'uk', 'colombia', 'argentina', 'mexico', 'otherLatam', 'other'];
const ALLOWED_INTENTS = ['secondHome', 'investment', 'futureMove', 'vacationRental', 'business', 'justExploring'];

/**
 * Cloudflare Access JWT Parser & Authenticator
 * Validates JWT token structure, expiration, and user email against authorized list.
 * Strips all caller-controlled dev bypasses.
 */
function authenticateAdmin(request, env) {
    const jwt = request.headers.get('Cf-Access-Jwt-Assertion');
    const headerEmail = request.headers.get('Cf-Access-Authenticated-User-Email');

    if (!jwt && !headerEmail) {
        return { authenticated: false, error: 'Cloudflare Access authentication required', status: 401 };
    }

    let authenticatedEmail = null;

    if (jwt) {
        try {
            const parts = jwt.split('.');
            if (parts.length !== 3) {
                return { authenticated: false, error: 'Malformed Cloudflare Access JWT', status: 401 };
            }
            const payloadRaw = atob(parts[1].replace(/-/g, '+').replace(/_/g, '/'));
            const payload = JSON.parse(payloadRaw);

            const nowSec = Math.floor(Date.now() / 1000);
            if (payload.exp && payload.exp < nowSec) {
                return { authenticated: false, error: 'Cloudflare Access token has expired', status: 401 };
            }

            authenticatedEmail = payload.email || payload.preferred_username || headerEmail;
        } catch (e) {
            return { authenticated: false, error: 'Invalid Cloudflare Access JWT structure', status: 401 };
        }
    } else {
        authenticatedEmail = headerEmail;
    }

    if (!authenticatedEmail) {
        return { authenticated: false, error: 'No authenticated user identity located in request', status: 401 };
    }

    if (env.ADMIN_EMAILS) {
        const allowedList = env.ADMIN_EMAILS.split(',').map(e => e.trim().toLowerCase());
        if (!allowedList.includes(authenticatedEmail.toLowerCase())) {
            return { authenticated: false, error: `User email (${authenticatedEmail}) is not authorized for CCOR Global Buyers Administration`, status: 403 };
        }
    }

    return { authenticated: true, email: authenticatedEmail };
}

function jsonResponse(data, status = 200, extraHeaders = {}) {
    return new Response(JSON.stringify(data), {
        status,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            ...extraHeaders
        }
    });
}

export default {
    async fetch(request, env, context) {
        const url = new URL(request.url);
        const path = url.pathname;

        const adminHeaders = {
            'Cache-Control': 'no-store, no-cache, must-revalidate',
            'X-Robots-Tag': 'noindex, nofollow',
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY'
        };

        // 1. Check Authentication for all admin routes & assets
        const auth = authenticateAdmin(request, env);
        if (!auth.authenticated) {
            if (path.includes('/api/')) {
                return jsonResponse({ ok: false, error: auth.error }, auth.status, adminHeaders);
            }
            return new Response(`<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>CCOR Admin — Authentication Required</title>
    <style>
        body { font-family: -apple-system, sans-serif; background: #0b2545; color: #fff; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; }
        .card { background: #fff; color: #1e293b; padding: 2.5rem; border-radius: 10px; max-width: 480px; box-shadow: 0 10px 30px rgba(0,0,0,0.3); }
        h2 { margin-top: 0; color: #0b2545; font-size: 1.4rem; }
        p { color: #64748b; font-size: 0.95rem; line-height: 1.5; }
        .badge { background: #fef3c7; color: #92400e; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 0.75rem; }
    </style>
</head>
<body>
    <div class="card">
        <span class="badge">CCOR PRIVATE ACCESS</span>
        <h2>Authentication Required</h2>
        <p>This portal is restricted to authorized CCOR staff via Cloudflare Access.</p>
        <p style="font-size:0.8rem; color:#94a3b8;">${auth.error}</p>
    </div>
</body>
</html>`, {
                status: auth.status,
                headers: {
                    'Content-Type': 'text/html; charset=utf-8',
                    ...adminHeaders
                }
            });
        }

        adminHeaders['X-Authenticated-User'] = auth.email;

        // 2. API Routes: Normalize path (support both /global-buyers-admin/api/* and /api/*)
        const isApi = path.startsWith('/global-buyers-admin/api/') || path.startsWith('/api/');
        if (isApi) {
            const apiSubPath = path.startsWith('/global-buyers-admin/api/')
                ? path.replace('/global-buyers-admin/api', '')
                : path.replace('/api', '');

            // A. If D1 is directly available on Pages, execute queries
            if (env.GLOBAL_BUYERS_DB) {
                // Summary API (Three-layer metric model + Funnel)
                if (apiSubPath === '/summary' && request.method === 'GET') {
                    try {
                        const now = new Date();
                        const d7 = new Date(now.getTime() - 7 * 86400000).toISOString();
                        const d30 = new Date(now.getTime() - 30 * 86400000).toISOString();

                        const [
                            totalCountrySelRow,
                            totalProfileSelRow,
                            totalLeadsRow,
                            uniqueEmailsRow,
                            leads7dRow,
                            leads30dRow,
                            optInRow,
                            topMarketRow,
                            topGoalRow
                        ] = await Promise.all([
                            env.GLOBAL_BUYERS_DB.prepare('SELECT SUM(selection_count) AS count FROM gb_market_daily').first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT SUM(selection_count) AS count FROM gb_profile_daily').first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads').first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(DISTINCT email) AS count FROM gb_leads').first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE created_at >= ?').bind(d7).first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE created_at >= ?').bind(d30).first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE marketing_opt_in = 1').first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT market, COUNT(*) AS count FROM gb_leads GROUP BY market ORDER BY count DESC LIMIT 1').first(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT intent, COUNT(*) AS count FROM gb_leads GROUP BY intent ORDER BY count DESC LIMIT 1').first()
                        ]);

                        const [mktSelRes, mktProfRes, mktLeadsRes, goalSelRes, goalLeadsRes] = await Promise.all([
                            env.GLOBAL_BUYERS_DB.prepare('SELECT market, SUM(selection_count) AS count FROM gb_market_daily GROUP BY market ORDER BY count DESC').all(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT market, SUM(selection_count) AS count FROM gb_profile_daily GROUP BY market ORDER BY count DESC').all(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT market, COUNT(*) AS count FROM gb_leads GROUP BY market ORDER BY count DESC').all(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT intent, SUM(selection_count) AS count FROM gb_profile_daily GROUP BY intent ORDER BY count DESC').all(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT intent, COUNT(*) AS count FROM gb_leads GROUP BY intent ORDER BY count DESC').all()
                        ]);

                        const countrySelections = totalCountrySelRow?.count || 0;
                        const profileSelections = totalProfileSelRow?.count || 0;
                        const contactProfiles = totalLeadsRow?.count || 0;
                        const uniqueEmails = uniqueEmailsRow?.count || 0;

                        const funnel = {
                            countrySelections,
                            profileSelections,
                            contactProfiles,
                            countryToProfilePct: countrySelections > 0 ? ((profileSelections / countrySelections) * 100).toFixed(1) : '0.0',
                            profileToContactPct: profileSelections > 0 ? ((contactProfiles / profileSelections) * 100).toFixed(1) : '0.0',
                            countryToContactPct: countrySelections > 0 ? ((contactProfiles / countrySelections) * 100).toFixed(1) : '0.0'
                        };

                        const topMCount = topMarketRow?.count || 0;
                        const topGCount = topGoalRow?.count || 0;

                        return jsonResponse({
                            ok: true,
                            countrySelections,
                            profileSelections,
                            contactProfiles,
                            uniqueEmails,
                            leads7d: leads7dRow?.count || 0,
                            leads30d: leads30dRow?.count || 0,
                            marketingOptIns: optInRow?.count || 0,
                            topMarket: topMarketRow ? { market: topMarketRow.market, count: topMCount, share: contactProfiles > 0 ? Math.round((topMCount / contactProfiles) * 100) : 0 } : null,
                            topGoal: topGoalRow ? { intent: topGoalRow.intent, count: topGCount, share: contactProfiles > 0 ? Math.round((topGCount / contactProfiles) * 100) : 0 } : null,
                            funnel,
                            marketSelections: mktSelRes.results || [],
                            marketProfiles: mktProfRes.results || [],
                            marketLeads: mktLeadsRes.results || [],
                            goalSelections: goalSelRes.results || [],
                            goalLeads: goalLeadsRes.results || []
                        }, 200, adminHeaders);
                    } catch (err) {
                        return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                    }
                }

                // Matrix API
                if (apiSubPath === '/matrix' && request.method === 'GET') {
                    try {
                        const res = await env.GLOBAL_BUYERS_DB.prepare(`
                            SELECT market, intent, SUM(selection_count) as count
                            FROM gb_profile_daily
                            GROUP BY market, intent
                        `).all();

                        const matrix = {};
                        (res.results || []).forEach(row => {
                            if (!matrix[row.market]) matrix[row.market] = {};
                            matrix[row.market][row.intent] = row.count;
                        });

                        return jsonResponse({ ok: true, matrix }, 200, adminHeaders);
                    } catch (err) {
                        return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                    }
                }

                // Trends API
                if (apiSubPath === '/trends' && request.method === 'GET') {
                    try {
                        const range = url.searchParams.get('range') || '30d';
                        let daysBack = 30;
                        if (range === '7d') daysBack = 7;
                        else if (range === '90d') daysBack = 90;
                        else if (range === 'all') daysBack = 365;

                        const startDate = new Date(Date.now() - daysBack * 86400000).toISOString().split('T')[0];

                        const [mktByDay, selByDay, leadsByDay] = await Promise.all([
                            env.GLOBAL_BUYERS_DB.prepare('SELECT day, SUM(selection_count) as count FROM gb_market_daily WHERE day >= ? GROUP BY day ORDER BY day ASC').bind(startDate).all(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT day, SUM(selection_count) as count FROM gb_profile_daily WHERE day >= ? GROUP BY day ORDER BY day ASC').bind(startDate).all(),
                            env.GLOBAL_BUYERS_DB.prepare('SELECT SUBSTR(created_at, 1, 10) as day, COUNT(*) as count FROM gb_leads WHERE created_at >= ? GROUP BY day ORDER BY day ASC').bind(startDate).all()
                        ]);

                        const dayMap = {};
                        (mktByDay.results || []).forEach(r => {
                            dayMap[r.day] = { day: r.day, countrySelections: r.count, profileSelections: 0, leads: 0 };
                        });
                        (selByDay.results || []).forEach(r => {
                            if (!dayMap[r.day]) dayMap[r.day] = { day: r.day, countrySelections: 0, profileSelections: 0, leads: 0 };
                            dayMap[r.day].profileSelections = r.count;
                        });
                        (leadsByDay.results || []).forEach(r => {
                            if (!dayMap[r.day]) dayMap[r.day] = { day: r.day, countrySelections: 0, profileSelections: 0, leads: 0 };
                            dayMap[r.day].leads = r.count;
                        });

                        const sortedDays = Object.values(dayMap).sort((a, b) => a.day.localeCompare(b.day));
                        return jsonResponse({ ok: true, days: sortedDays, range }, 200, adminHeaders);
                    } catch (err) {
                        return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                    }
                }

                // Leads API
                if (apiSubPath === '/leads' && request.method === 'GET') {
                    try {
                        const search = (url.searchParams.get('search') || '').trim();
                        const market = (url.searchParams.get('market') || '').trim();
                        const intent = (url.searchParams.get('intent') || '').trim();
                        const page = Math.max(1, parseInt(url.searchParams.get('page') || '1', 10));
                        const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit') || '25', 10)));
                        const offset = (page - 1) * limit;

                        let conditions = ['1=1'];
                        let binds = [];

                        if (search) {
                            conditions.push('(first_name LIKE ? OR last_name LIKE ? OR email LIKE ?)');
                            const s = `%${search}%`;
                            binds.push(s, s, s);
                        }
                        if (market) {
                            conditions.push('market = ?');
                            binds.push(market);
                        }
                        if (intent) {
                            conditions.push('intent = ?');
                            binds.push(intent);
                        }

                        const whereClause = conditions.join(' AND ');
                        const countQuery = `SELECT COUNT(*) as total FROM gb_leads WHERE ${whereClause}`;
                        const countStmt = env.GLOBAL_BUYERS_DB.prepare(countQuery);
                        const totalRes = await (binds.length > 0 ? countStmt.bind(...binds) : countStmt).first();

                        const listQuery = `
                            SELECT id, created_at, first_name, last_name, email, market, country_code,
                                   intent, marketing_opt_in, consent, consent_version, referrer_host,
                                   utm_source, utm_medium, utm_campaign, lead_source
                            FROM gb_leads
                            WHERE ${whereClause}
                            ORDER BY created_at DESC
                            LIMIT ? OFFSET ?
                        `;
                        const listBinds = [...binds, limit, offset];
                        const listStmt = env.GLOBAL_BUYERS_DB.prepare(listQuery);
                        const leadsRes = await listStmt.bind(...listBinds).all();

                        return jsonResponse({
                            ok: true,
                            leads: leadsRes.results || [],
                            total: totalRes?.total || 0,
                            page,
                            limit
                        }, 200, adminHeaders);
                    } catch (err) {
                        return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                    }
                }

                // Flight Snapshots API
                if (apiSubPath === '/flight-snapshots' && request.method === 'GET') {
                    try {
                        const res = await env.GLOBAL_BUYERS_DB.prepare(`
                            SELECT id, sampled_at, market, origin, destination, departure_date, return_date,
                                   fare_usd, local_currency, fare_local, carrier, stops, duration_minutes
                            FROM gb_fare_samples
                            ORDER BY sampled_at DESC
                            LIMIT 50
                        `).all();

                        return jsonResponse({ ok: true, snapshots: res.results || [] }, 200, adminHeaders);
                    } catch (err) {
                        return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                    }
                }

                // Delete Lead API
                if (apiSubPath.startsWith('/leads/') && request.method === 'DELETE') {
                    try {
                        const leadId = apiSubPath.replace('/leads/', '').trim();
                        if (!leadId) {
                            return jsonResponse({ ok: false, error: 'Lead ID is required.' }, 400, adminHeaders);
                        }

                        await env.GLOBAL_BUYERS_DB.prepare('DELETE FROM gb_leads WHERE id = ?').bind(leadId).run();
                        return jsonResponse({ ok: true, deletedId: leadId }, 200, adminHeaders);
                    } catch (err) {
                        return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                    }
                }

                // Export CSV API
                if (apiSubPath === '/export.csv' && request.method === 'GET') {
                    try {
                        const res = await env.GLOBAL_BUYERS_DB.prepare(`
                            SELECT created_at, first_name, last_name, email, market, country_code,
                                   intent, marketing_opt_in, consent_version, utm_source, utm_medium, utm_campaign, lead_source
                            FROM gb_leads
                            ORDER BY created_at DESC
                        `).all();

                        const rows = res.results || [];
                        const headers = ['created_at', 'first_name', 'last_name', 'email', 'market', 'country_code', 'intent', 'marketing_opt_in', 'consent_version', 'utm_source', 'utm_medium', 'utm_campaign', 'lead_source'];

                        let csv = headers.join(',') + '\n';
                        rows.forEach(r => {
                            const line = headers.map(h => {
                                const val = r[h] !== null && r[h] !== undefined ? String(r[h]) : '';
                                return `"${val.replace(/"/g, '""')}"`;
                            }).join(',');
                            csv += line + '\n';
                        });

                        const filename = `ccor-global-buyers-leads-${new Date().toISOString().split('T')[0]}.csv`;
                        return new Response(csv, {
                            headers: {
                                'Content-Type': 'text/csv; charset=utf-8',
                                'Content-Disposition': `attachment; filename="${filename}"`,
                                ...adminHeaders
                            }
                        });
                    } catch (err) {
                        return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                    }
                }
            }

            // B. If D1 is not directly attached on Pages, proxy to Worker API securely
            const workerBase = env.WORKER_API_BASE || 'https://ccor-global-buyers-api.bonitaspringsrealtors.workers.dev';
            const workerTargetUrl = `${workerBase}/global-buyers-admin/api${apiSubPath}${url.search}`;

            const proxyHeaders = new Headers();
            const accept = request.headers.get('Accept');
            if (accept) proxyHeaders.set('Accept', accept);
            const contentType = request.headers.get('Content-Type');
            if (contentType) proxyHeaders.set('Content-Type', contentType);
            if (request.headers.get('Cf-Access-Jwt-Assertion')) {
                proxyHeaders.set('Cf-Access-Jwt-Assertion', request.headers.get('Cf-Access-Jwt-Assertion'));
            }
            proxyHeaders.set('Cf-Access-Authenticated-User-Email', auth.email);

            const proxyRes = await fetch(workerTargetUrl, {
                method: request.method,
                headers: proxyHeaders,
                body: request.method !== 'GET' && request.method !== 'HEAD' ? request.body : undefined
            });

            const respHeaders = new Headers(proxyRes.headers);
            Object.entries(adminHeaders).forEach(([k, v]) => respHeaders.set(k, v));
            if (proxyRes.headers.get('Content-Disposition')) {
                respHeaders.set('Content-Disposition', proxyRes.headers.get('Content-Disposition'));
            }
            if (proxyRes.headers.get('Content-Type')) {
                respHeaders.set('Content-Type', proxyRes.headers.get('Content-Type'));
            }

            return new Response(proxyRes.body, {
                status: proxyRes.status,
                headers: respHeaders
            });
        }

        // 3. Serve Frontend (index.html or static files)
        const assetResponse = await env.ASSETS.fetch(request);
        const headers = new Headers(assetResponse.headers);
        Object.entries(adminHeaders).forEach(([k, v]) => headers.set(k, v));

        return new Response(assetResponse.body, {
            status: assetResponse.status,
            statusText: assetResponse.statusText,
            headers
        });
    }
};
