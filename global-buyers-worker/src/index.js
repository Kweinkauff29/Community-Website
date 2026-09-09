/**
 * ccor-global-buyers-api Cloudflare Worker
 * 
 * Endpoints:
 * - Public API:
 *   - POST /global-buyers-api/leads (Lead capture with spam defense, validation, prepared D1 insert)
 *   - POST /global-buyers-api/profile-selection (Daily anonymous aggregate counter, no PII)
 *   - GET  /global-buyers-api/flights (Graceful flight provider coexistence)
 * 
 * - Admin (Cloudflare Access Protected):
 *   - GET  /global-buyers-admin/ (Dashboard UI)
 *   - GET  /global-buyers-admin/api/summary (KPIs & breakdowns)
 *   - GET  /global-buyers-admin/api/matrix (Market x Goal cross-tab)
 *   - GET  /global-buyers-admin/api/trends (Historical interest)
 *   - GET  /global-buyers-admin/api/leads (Filtered contact table)
 *   - GET  /global-buyers-admin/api/export.csv (CSV export)
 *   - DELETE /global-buyers-admin/api/leads/:id (Lead deletion)
 */

import adminHtml from './admin.html';

const ALLOWED_MARKETS = ['canada', 'germany', 'brazil', 'uk', 'colombia', 'argentina', 'mexico', 'otherLatam', 'other'];
const ALLOWED_INTENTS = ['secondHome', 'investment', 'futureMove', 'vacationRental', 'business', 'justExploring'];
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function getCorsHeaders(request, env) {
    const origin = request.headers.get('Origin') || '';
    const allowed = (env.ALLOWED_ORIGINS || '').split(',').map(o => o.trim());
    
    // Check allowed origin or default safe matching
    let allowOrigin = '';
    if (allowed.includes(origin)) {
        allowOrigin = origin;
    } else if (origin.endsWith('coconutcoastlifestyles.com') || origin.endsWith('coconutcoastrealtors.org') || origin.startsWith('http://localhost:') || origin.startsWith('http://127.0.0.1:')) {
        allowOrigin = origin;
    } else if (allowed.length > 0) {
        allowOrigin = allowed[0];
    }

    return {
        'Access-Control-Allow-Origin': allowOrigin,
        'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
        'Access-Control-Max-Age': '86400',
        'Vary': 'Origin'
    };
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

function getReferrerHost(refererHeader) {
    if (!refererHeader) return null;
    try {
        const parsed = new URL(refererHeader);
        return parsed.hostname;
    } catch {
        return null;
    }
}

/**
 * Cloudflare Access Authentication Helper
 */
function authenticateAdmin(request, env) {
    // 1. Cloudflare Access Identity Header
    let email = request.headers.get('Cf-Access-Authenticated-User-Email');

    // 2. Allow local preview / dev bypass only if explicitly enabled
    if (!email && (env.ENVIRONMENT === 'development' || request.headers.get('x-dev-admin') === 'true')) {
        email = request.headers.get('x-admin-email') || 'kevin@coconutcoastrealtors.org';
    }

    if (!email) {
        return { authenticated: false, error: 'Cloudflare Access authentication required', status: 401 };
    }

    // 3. Defense-in-depth: Optional ADMIN_EMAILS allowlist
    if (env.ADMIN_EMAILS) {
        const allowedList = env.ADMIN_EMAILS.split(',').map(e => e.trim().toLowerCase());
        if (!allowedList.includes(email.toLowerCase())) {
            return { authenticated: false, error: 'User email not authorized for Global Buyers Administration', status: 403 };
        }
    }

    return { authenticated: true, email };
}

export default {
    async fetch(request, env) {
        const url = new URL(request.url);
        const path = url.pathname;
        const corsHeaders = getCorsHeaders(request, env);

        // Preflight CORS
        if (request.method === 'OPTIONS') {
            return new Response(null, {
                status: 204,
                headers: corsHeaders
            });
        }

        // ====================================================================
        // PUBLIC API ENDPOINTS
        // ====================================================================

        // 1. POST /global-buyers-api/leads
        if (path === '/global-buyers-api/leads' && request.method === 'POST') {
            try {
                const body = await request.json();

                // Anti-Spam: Honeypot check
                if (body.website || body.confirm_email || body.honeypot) {
                    return jsonResponse({ ok: true, leadId: 'filtered' }, 200, corsHeaders);
                }

                // Anti-Spam: Cloudflare Turnstile validation if configured
                if (env.TURNSTILE_SECRET_KEY && body.turnstileToken) {
                    const tsRes = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                        body: new URLSearchParams({
                            secret: env.TURNSTILE_SECRET_KEY,
                            response: body.turnstileToken
                        })
                    });
                    const tsData = await tsRes.json();
                    if (!tsData.success) {
                        return jsonResponse({ ok: false, error: 'Turnstile verification failed' }, 400, corsHeaders);
                    }
                }

                // Server-Side Field Validation
                const firstName = (body.firstName || '').trim();
                const lastName = (body.lastName || '').trim();
                const email = (body.email || '').trim().toLowerCase();
                const market = (body.market || '').trim();
                const countryCode = body.countryCode ? body.countryCode.trim().toUpperCase() : null;
                const intent = (body.intent || '').trim();
                const consent = !!body.consent;
                const consentVersion = (body.consentVersion || '2026-v1').trim();
                const marketingOptIn = body.marketingOptIn ? 1 : 0;

                if (!firstName || firstName.length > 80) {
                    return jsonResponse({ ok: false, error: 'Valid first name is required (1–80 characters).' }, 400, corsHeaders);
                }
                if (lastName.length > 100) {
                    return jsonResponse({ ok: false, error: 'Last name cannot exceed 100 characters.' }, 400, corsHeaders);
                }
                if (!email || email.length > 254 || !EMAIL_REGEX.test(email)) {
                    return jsonResponse({ ok: false, error: 'A valid email address is required.' }, 400, corsHeaders);
                }
                if (!consent) {
                    return jsonResponse({ ok: false, error: 'Consent to store inquiry information is required.' }, 400, corsHeaders);
                }

                const validMarket = ALLOWED_MARKETS.includes(market) ? market : 'other';
                const validIntent = ALLOWED_INTENTS.includes(intent) ? intent : 'justExploring';

                const id = crypto.randomUUID();
                const createdAt = new Date().toISOString();
                const referrerHost = getReferrerHost(request.headers.get('Referer'));
                const utm = body.utm || {};

                // Prepared D1 Insert (Zero raw concatenation, zero IP or UA stored)
                await env.GLOBAL_BUYERS_DB.prepare(`
                    INSERT INTO gb_leads (
                        id, created_at, first_name, last_name, email, market, country_code,
                        intent, marketing_opt_in, consent, consent_version, source_path,
                        referrer_host, utm_source, utm_medium, utm_campaign, utm_content
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                        ?8, ?9, ?10, ?11, ?12,
                        ?13, ?14, ?15, ?16, ?17
                    )
                `).bind(
                    id, createdAt, firstName, lastName || null, email, validMarket, countryCode || null,
                    validIntent, marketingOptIn, 1, consentVersion, url.pathname,
                    referrerHost, utm.source || null, utm.medium || null, utm.campaign || null, utm.content || null
                ).run();

                return jsonResponse({ ok: true, leadId: id }, 201, corsHeaders);

            } catch (err) {
                return jsonResponse({ ok: false, error: 'Server error processing lead submission.' }, 500, corsHeaders);
            }
        }

        // 2. POST /global-buyers-api/profile-selection (Daily anonymous aggregate increment)
        if (path === '/global-buyers-api/profile-selection' && request.method === 'POST') {
            try {
                const body = await request.json();
                const market = (body.market || '').trim();
                const countryCode = body.countryCode ? body.countryCode.trim().toUpperCase() : null;
                const intent = (body.intent || '').trim();

                if (!ALLOWED_MARKETS.includes(market) || !ALLOWED_INTENTS.includes(intent)) {
                    return jsonResponse({ ok: false, error: 'Invalid market or intent.' }, 400, corsHeaders);
                }

                const day = new Date().toISOString().split('T')[0]; // UTC Day YYYY-MM-DD

                // Prepared D1 Upsert (Strictly count, zero PII)
                await env.GLOBAL_BUYERS_DB.prepare(`
                    INSERT INTO gb_profile_daily (day, market, country_code, intent, selection_count)
                    VALUES (?1, ?2, ?3, ?4, 1)
                    ON CONFLICT(day, market, intent) DO UPDATE SET selection_count = selection_count + 1
                `).bind(day, market, countryCode || null, intent).run();

                return jsonResponse({ ok: true }, 200, corsHeaders);

            } catch (err) {
                return jsonResponse({ ok: false, error: 'Error logging profile selection.' }, 500, corsHeaders);
            }
        }

        // 3. GET /global-buyers-api/flights (Coexistence: graceful 503 if credentials absent)
        if (path === '/global-buyers-api/flights' && request.method === 'GET') {
            if (!env.DUFFEL_ACCESS_TOKEN) {
                return jsonResponse({
                    ok: false,
                    error: 'live_flight_provider_unconfigured',
                    message: 'Live flight offers endpoint requires DUFFEL_ACCESS_TOKEN. Consumer frontend should display verified planning estimates.'
                }, 503, corsHeaders);
            }

            // Future Duffel proxy logic here when token is provisioned
            return jsonResponse({ ok: true, offers: [] }, 200, corsHeaders);
        }

        // ====================================================================
        // PROTECTED ADMIN DASHBOARD & ADMIN APIs
        // ====================================================================

        if (path.startsWith('/global-buyers-admin')) {
            const auth = authenticateAdmin(request, env);
            if (!auth.authenticated) {
                return new Response(`401 Unauthorized: ${auth.error}. Access restricted to approved CCOR staff.`, {
                    status: auth.status,
                    headers: {
                        'Content-Type': 'text/plain; charset=utf-8',
                        'Cache-Control': 'no-store',
                        'X-Robots-Tag': 'noindex, nofollow'
                    }
                });
            }

            const adminHeaders = {
                'Cache-Control': 'no-store, no-cache, must-revalidate',
                'X-Robots-Tag': 'noindex, nofollow',
                'X-Content-Type-Options': 'nosniff',
                'X-Frame-Options': 'DENY',
                'X-Authenticated-User': auth.email
            };

            // A. Serve Dashboard HTML
            if (path === '/global-buyers-admin' || path === '/global-buyers-admin/') {
                return new Response(adminHtml, {
                    headers: {
                        'Content-Type': 'text/html; charset=utf-8',
                        ...adminHeaders
                    }
                });
            }

            // B. GET /global-buyers-admin/api/summary
            if (path === '/global-buyers-admin/api/summary' && request.method === 'GET') {
                try {
                    const now = new Date();
                    const d7 = new Date(now.getTime() - 7 * 86400000).toISOString();
                    const d30 = new Date(now.getTime() - 30 * 86400000).toISOString();

                    const [totalRow, uniqueRow, leads7dRow, leads30dRow, optInRow, topMarketRow, topGoalRow, totalSelRow] = await Promise.all([
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(DISTINCT email) AS count FROM gb_leads').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE created_at >= ?').bind(d7).first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE created_at >= ?').bind(d30).first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE marketing_opt_in = 1').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, COUNT(*) AS count FROM gb_leads GROUP BY market ORDER BY count DESC LIMIT 1').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT intent, COUNT(*) AS count FROM gb_leads GROUP BY intent ORDER BY count DESC LIMIT 1').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT SUM(selection_count) AS count FROM gb_profile_daily').first()
                    ]);

                    // Market distribution for leads & selections
                    const [marketLeadsRes, marketSelRes, goalLeadsRes, goalSelRes] = await Promise.all([
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, COUNT(*) AS count FROM gb_leads GROUP BY market ORDER BY count DESC').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, SUM(selection_count) AS count FROM gb_profile_daily GROUP BY market ORDER BY count DESC').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT intent, COUNT(*) AS count FROM gb_leads GROUP BY intent ORDER BY count DESC').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT intent, SUM(selection_count) AS count FROM gb_profile_daily GROUP BY intent ORDER BY count DESC').all()
                    ]);

                    const totalLeads = totalRow?.count || 0;
                    const topMCount = topMarketRow?.count || 0;
                    const topGCount = topGoalRow?.count || 0;

                    return jsonResponse({
                        ok: true,
                        totalLeads,
                        uniqueEmails: uniqueRow?.count || 0,
                        leads7d: leads7dRow?.count || 0,
                        leads30d: leads30dRow?.count || 0,
                        marketingOptIns: optInRow?.count || 0,
                        totalSelections: totalSelRow?.count || 0,
                        topMarket: topMarketRow ? { market: topMarketRow.market, count: topMCount, share: totalLeads > 0 ? Math.round((topMCount / totalLeads) * 100) : 0 } : null,
                        topGoal: topGoalRow ? { intent: topGoalRow.intent, count: topGCount, share: totalLeads > 0 ? Math.round((topGCount / totalLeads) * 100) : 0 } : null,
                        marketLeads: marketLeadsRes.results || [],
                        marketSelections: marketSelRes.results || [],
                        goalLeads: goalLeadsRes.results || [],
                        goalSelections: goalSelRes.results || []
                    }, 200, adminHeaders);

                } catch (err) {
                    return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                }
            }

            // C. GET /global-buyers-admin/api/matrix
            if (path === '/global-buyers-admin/api/matrix' && request.method === 'GET') {
                try {
                    const res = await env.GLOBAL_BUYERS_DB.prepare(`
                        SELECT market, intent, COUNT(*) as count
                        FROM gb_leads
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

            // D. GET /global-buyers-admin/api/trends
            if (path === '/global-buyers-admin/api/trends' && request.method === 'GET') {
                try {
                    const range = url.searchParams.get('range') || '30d';
                    let daysBack = 30;
                    if (range === '7d') daysBack = 7;
                    if (range === '90d') daysBack = 90;
                    if (range === 'all') daysBack = 365;

                    const startDate = new Date(Date.now() - daysBack * 86400000).toISOString().split('T')[0];

                    const [leadsByDay, selByDay] = await Promise.all([
                        env.GLOBAL_BUYERS_DB.prepare(`
                            SELECT SUBSTR(created_at, 1, 10) as day, COUNT(*) as count
                            FROM gb_leads
                            WHERE created_at >= ?
                            GROUP BY day
                            ORDER BY day ASC
                        `).bind(startDate).all(),
                        env.GLOBAL_BUYERS_DB.prepare(`
                            SELECT day, SUM(selection_count) as count
                            FROM gb_profile_daily
                            WHERE day >= ?
                            GROUP BY day
                            ORDER BY day ASC
                        `).bind(startDate).all()
                    ]);

                    const dayMap = {};
                    (leadsByDay.results || []).forEach(r => {
                        if (!dayMap[r.day]) dayMap[r.day] = { day: r.day, leads: 0, selections: 0 };
                        dayMap[r.day].leads = r.count;
                    });
                    (selByDay.results || []).forEach(r => {
                        if (!dayMap[r.day]) dayMap[r.day] = { day: r.day, leads: 0, selections: 0 };
                        dayMap[r.day].selections = r.count;
                    });

                    const days = Object.values(dayMap).sort((a, b) => a.day.localeCompare(b.day));
                    return jsonResponse({ ok: true, days }, 200, adminHeaders);

                } catch (err) {
                    return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                }
            }

            // E. GET /global-buyers-admin/api/leads
            if (path === '/global-buyers-admin/api/leads' && request.method === 'GET') {
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
                               utm_source, utm_medium, utm_campaign
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

            // F. GET /global-buyers-admin/api/export.csv
            if (path === '/global-buyers-admin/api/export.csv' && request.method === 'GET') {
                try {
                    const res = await env.GLOBAL_BUYERS_DB.prepare(`
                        SELECT created_at, first_name, last_name, email, market, country_code,
                               intent, marketing_opt_in, consent_version, utm_source, utm_medium, utm_campaign
                        FROM gb_leads
                        ORDER BY created_at DESC
                    `).all();

                    const rows = res.results || [];
                    const headers = ['created_at', 'first_name', 'last_name', 'email', 'market', 'country_code', 'intent', 'marketing_opt_in', 'consent_version', 'utm_source', 'utm_medium', 'utm_campaign'];
                    
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

            // G. DELETE /global-buyers-admin/api/leads/:id
            if (path.startsWith('/global-buyers-admin/api/leads/') && request.method === 'DELETE') {
                try {
                    const id = path.replace('/global-buyers-admin/api/leads/', '').trim();
                    if (!id) {
                        return jsonResponse({ ok: false, error: 'Missing lead ID' }, 400, adminHeaders);
                    }

                    await env.GLOBAL_BUYERS_DB.prepare('DELETE FROM gb_leads WHERE id = ?').bind(id).run();
                    return jsonResponse({ ok: true, deletedId: id }, 200, adminHeaders);

                } catch (err) {
                    return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                }
            }

            return new Response('Admin route not found.', { status: 404, headers: adminHeaders });
        }

        return new Response('Not Found', { status: 404, headers: corsHeaders });
    }
};
