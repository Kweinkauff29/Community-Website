import adminHtml from './admin.html';

const ALLOWED_MARKETS = ['canada', 'germany', 'brazil', 'uk', 'colombia', 'argentina', 'mexico', 'otherLatam', 'other'];
const ALLOWED_INTENTS = ['secondHome', 'investment', 'futureMove', 'vacationRental', 'business', 'justExploring'];
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

// In-memory cache for live flight searches (TTL: 2 hours)
const flightCache = new Map();

function getCorsHeaders(request, env) {
    const origin = request.headers.get('Origin') || '';
    const allowed = (env.ALLOWED_ORIGINS || '').split(',').map(o => o.trim());
    
    let allowOrigin = '';
    if (allowed.includes(origin)) {
        allowOrigin = origin;
    } else if (origin.endsWith('coconutcoastlifestyles.com') || origin.endsWith('coconutcoastrealtors.org') || origin.endsWith('.pages.dev') || origin.startsWith('http://localhost:') || origin.startsWith('http://127.0.0.1:')) {
        allowOrigin = origin;
    } else if (allowed.length > 0) {
        allowOrigin = allowed[0];
    }

    return {
        'Access-Control-Allow-Origin': allowOrigin,
        'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Cf-Access-Jwt-Assertion, Cf-Access-Authenticated-User-Email',
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
 * Cloudflare Access JWT Parser & Authenticator
 * Validates signature format, expiration, and user email allowlist
 */
function authenticateAdmin(request, env) {
    const jwt = request.headers.get('Cf-Access-Jwt-Assertion');
    const headerEmail = request.headers.get('Cf-Access-Authenticated-User-Email');

    // Reject immediately if no credentials present
    if (!jwt && !headerEmail) {
        return { authenticated: false, error: 'Cloudflare Access authentication required', status: 401 };
    }

    let authenticatedEmail = null;

    // Validate JWT assertion if present
    if (jwt) {
        try {
            const parts = jwt.split('.');
            if (parts.length !== 3) {
                return { authenticated: false, error: 'Malformed Cloudflare Access JWT', status: 401 };
            }

            // Decode payload (base64url)
            const payloadRaw = atob(parts[1].replace(/-/g, '+').replace(/_/g, '/'));
            const payload = JSON.parse(payloadRaw);

            // Check expiration
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

    // Verify against ADMIN_EMAILS allowlist
    if (env.ADMIN_EMAILS) {
        const allowedList = env.ADMIN_EMAILS.split(',').map(e => e.trim().toLowerCase());
        if (!allowedList.includes(authenticatedEmail.toLowerCase())) {
            return { authenticated: false, error: `User email (${authenticatedEmail}) is not authorized for CCOR Global Buyers Administration`, status: 403 };
        }
    }

    return { authenticated: true, email: authenticatedEmail };
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

        // 1. POST /global-buyers-api/market-selection (Anonymous Country Selection Event)
        if (path === '/global-buyers-api/market-selection' && request.method === 'POST') {
            try {
                const body = await request.json();
                const market = (body.market || '').trim();
                const countryCode = body.countryCode ? body.countryCode.trim().toUpperCase() : null;

                if (!ALLOWED_MARKETS.includes(market)) {
                    return jsonResponse({ ok: false, error: 'Invalid market specified.' }, 400, corsHeaders);
                }

                const day = new Date().toISOString().split('T')[0]; // UTC Day YYYY-MM-DD

                // Increment daily aggregate record for market (Zero PII)
                await env.GLOBAL_BUYERS_DB.prepare(`
                    INSERT INTO gb_market_daily (day, market, country_code, selection_count)
                    VALUES (?1, ?2, ?3, 1)
                    ON CONFLICT(day, market) DO UPDATE SET selection_count = selection_count + 1
                `).bind(day, market, countryCode).run();

                return jsonResponse({ ok: true }, 200, corsHeaders);
            } catch (err) {
                return jsonResponse({ ok: false, error: 'Error logging country selection.' }, 500, corsHeaders);
            }
        }

        // 2. POST /global-buyers-api/profile-selection (Anonymous Country + Goal Event)
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
                `).bind(day, market, countryCode, intent).run();

                return jsonResponse({ ok: true }, 200, corsHeaders);
            } catch (err) {
                return jsonResponse({ ok: false, error: 'Error logging profile selection.' }, 500, corsHeaders);
            }
        }

        // 3. POST /global-buyers-api/leads (Contact Profiles)
        if (path === '/global-buyers-api/leads' && request.method === 'POST') {
            try {
                const body = await request.json();

                // Anti-Spam: Honeypot check
                if (body.website || body.confirm_email || body.honeypot || body.organization_url) {
                    return jsonResponse({ ok: true, leadId: 'filtered' }, 200, corsHeaders);
                }

                // Anti-Spam: Cloudflare Turnstile validation
                if (env.TURNSTILE_SECRET_KEY) {
                    const token = body.turnstileToken || body['cf-turnstile-response'] || body.cfTurnstileResponse;
                    if (!token) {
                        return jsonResponse({ ok: false, error: 'Turnstile verification token required.' }, 400, corsHeaders);
                    }

                    // Cloudflare testing dummy tokens support
                    if (token !== '1x00000000000000000000AA') {
                        const tsRes = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                            body: new URLSearchParams({
                                secret: env.TURNSTILE_SECRET_KEY,
                                response: token
                            })
                        });
                        const tsData = await tsRes.json();
                        if (!tsData.success) {
                            return jsonResponse({ ok: false, error: 'Turnstile verification failed.' }, 400, corsHeaders);
                        }
                    }
                }

                // Field Validation
                const firstName = (body.firstName || '').trim();
                const lastName = (body.lastName || '').trim();
                const email = (body.email || '').trim().toLowerCase();
                const market = (body.market || '').trim();
                const countryCode = body.countryCode ? body.countryCode.trim().toUpperCase() : null;
                const intent = (body.intent || '').trim();
                const consent = !!body.consent || !!body.consentGiven;
                const consentVersion = (body.consentVersion || '2026.1').trim();
                const marketingOptIn = body.marketingOptIn ? 1 : 0;
                const leadSource = (body.leadSource || body.lead_source || 'brief_manual').trim();

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
                        referrer_host, utm_source, utm_medium, utm_campaign, utm_content, lead_source
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                        ?8, ?9, ?10, ?11, ?12,
                        ?13, ?14, ?15, ?16, ?17, ?18
                    )
                `).bind(
                    id, createdAt, firstName, lastName || null, email, validMarket, countryCode,
                    validIntent, marketingOptIn, 1, consentVersion, url.pathname,
                    referrerHost, utm.source || null, utm.medium || null, utm.campaign || null, utm.content || null, leadSource
                ).run();

                return jsonResponse({ ok: true, leadId: id }, 201, corsHeaders);
            } catch (err) {
                return jsonResponse({ ok: false, error: 'Server error processing lead submission.' }, 500, corsHeaders);
            }
        }

        // 4. GET /global-buyers-api/flights (Live Duffel Integration + Cache)
        if (path === '/global-buyers-api/flights' && request.method === 'GET') {
            const origin = (url.searchParams.get('origin') || 'FRA').trim().toUpperCase();
            const destination = 'RSW';
            const market = (url.searchParams.get('market') || 'germany').trim().toLowerCase();

            // Default dates (+45 days departure, 7 days later return)
            const dNow = new Date();
            const defaultDepart = new Date(dNow.getTime() + 45 * 86400000).toISOString().split('T')[0];
            const defaultReturn = new Date(dNow.getTime() + 52 * 86400000).toISOString().split('T')[0];

            const departureDate = url.searchParams.get('departureDate') || defaultDepart;
            const returnDate = url.searchParams.get('returnDate') || defaultReturn;
            const adults = parseInt(url.searchParams.get('adults') || '1', 10);

            const cacheKey = `${origin}:${destination}:${departureDate}:${returnDate}:${adults}`;
            const cached = flightCache.get(cacheKey);
            if (cached && (Date.now() - cached.timestamp < 7200000)) { // 2 hour cache
                return jsonResponse(cached.data, 200, corsHeaders);
            }

            // Check if live Duffel token exists
            if (!env.DUFFEL_ACCESS_TOKEN) {
                return jsonResponse({
                    ok: false,
                    live: false,
                    error: 'DUFFEL_ACCESS_TOKEN_REQUIRED',
                    message: 'Live airline fare search requires DUFFEL_ACCESS_TOKEN. Verified planning benchmark displayed.',
                    origin,
                    destination
                }, 503, corsHeaders);
            }

            // Call Duffel API
            try {
                const duffelReqBody = {
                    data: {
                        slices: [
                            { origin, destination, departure_date: departureDate },
                            { origin: destination, destination: origin, departure_date: returnDate }
                        ],
                        passengers: [{ type: 'adult' }],
                        cabin_class: 'economy'
                    }
                };

                const duffelRes = await fetch('https://api.duffel.com/air/offer_requests', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${env.DUFFEL_ACCESS_TOKEN}`,
                        'Duffel-Version': 'v2',
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    body: JSON.stringify(duffelReqBody)
                });

                if (!duffelRes.ok) {
                    const errText = await duffelRes.text();
                    return jsonResponse({ ok: false, error: 'Duffel API returned error', details: errText }, 502, corsHeaders);
                }

                const duffelJson = await duffelRes.json();
                const rawOffers = (duffelJson.data && duffelJson.data.offers) || [];

                // Sort by total_amount ascending and pick top 3
                rawOffers.sort((a, b) => parseFloat(a.total_amount) - parseFloat(b.total_amount));
                const topOffers = rawOffers.slice(0, 3).map(offer => {
                    const outSlice = offer.slices && offer.slices[0];
                    const inSlice = offer.slices && offer.slices[1];

                    const outSegments = (outSlice && outSlice.segments) || [];
                    const inSegments = (inSlice && inSlice.segments) || [];

                    const airlineNames = Array.from(new Set([
                        ...(outSegments.map(s => s.operating_carrier?.name || s.marketing_carrier?.name).filter(Boolean)),
                        ...(inSegments.map(s => s.operating_carrier?.name || s.marketing_carrier?.name).filter(Boolean))
                    ]));

                    return {
                        id: offer.id,
                        totalUsd: parseFloat(offer.total_amount),
                        currency: offer.total_currency,
                        carrier: offer.owner?.name || (airlineNames[0] || 'Airline Partner'),
                        airlineNames,
                        durationMinutesOut: outSlice?.duration ? parseIsoDuration(outSlice.duration) : null,
                        durationMinutesIn: inSlice?.duration ? parseIsoDuration(inSlice.duration) : null,
                        stopsOutbound: Math.max(0, outSegments.length - 1),
                        stopsReturn: Math.max(0, inSegments.length - 1),
                        outbound: outSegments.map(s => ({
                            origin: s.origin.iata_code,
                            destination: s.destination.iata_code,
                            carrierName: s.marketing_carrier?.name || s.operating_carrier?.name,
                            carrierCode: s.marketing_carrier?.iata_code,
                            flightNumber: s.marketing_carrier_flight_number,
                            departingAt: s.departing_at,
                            arrivingAt: s.arriving_at
                        })),
                        inbound: inSegments.map(s => ({
                            origin: s.origin.iata_code,
                            destination: s.destination.iata_code,
                            carrierName: s.marketing_carrier?.name || s.operating_carrier?.name,
                            carrierCode: s.marketing_carrier?.iata_code,
                            flightNumber: s.marketing_carrier_flight_number,
                            departingAt: s.departing_at,
                            arrivingAt: s.arriving_at
                        }))
                    };
                });

                const responseData = {
                    ok: true,
                    live: true,
                    checkedAt: new Date().toISOString(),
                    origin,
                    destination,
                    departureDate,
                    returnDate,
                    offers: topOffers
                };

                // Cache response
                flightCache.set(cacheKey, { timestamp: Date.now(), data: responseData });

                // Record lowest fare sample to gb_fare_samples in D1
                if (topOffers.length > 0) {
                    const lowest = topOffers[0];
                    const sampleId = crypto.randomUUID();
                    await env.GLOBAL_BUYERS_DB.prepare(`
                        INSERT INTO gb_fare_samples (
                            id, sampled_at, market, origin, destination, departure_date, return_date,
                            fare_usd, local_currency, fare_local, carrier, stops, duration_minutes
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                    `).bind(
                        sampleId, new Date().toISOString(), market, origin, destination, departureDate, returnDate,
                        lowest.totalUsd, lowest.currency, lowest.totalUsd, lowest.carrier, lowest.stopsOutbound, lowest.durationMinutesOut
                    ).run().catch(() => {});
                }

                return jsonResponse(responseData, 200, corsHeaders);
            } catch (err) {
                return jsonResponse({ ok: false, error: err.message }, 500, corsHeaders);
            }
        }

        // ====================================================================
        // PROTECTED ADMIN DASHBOARD & ADMIN APIs
        // ====================================================================

        if (path.startsWith('/global-buyers-admin')) {
            const auth = authenticateAdmin(request, env);
            if (!auth.authenticated) {
                return new Response(`401 Unauthorized: ${auth.error}. Access restricted to approved CCOR staff via Cloudflare Access.`, {
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

            // B. GET /global-buyers-admin/api/summary (Three Distinct Layers + Funnel)
            if (path === '/global-buyers-admin/api/summary' && request.method === 'GET') {
                try {
                    const now = new Date();
                    const d7 = new Date(now.getTime() - 7 * 86400000).toISOString();
                    const d30 = new Date(now.getTime() - 30 * 86400000).toISOString();

                    const [
                        totalLeadsRow,
                        uniqueRow,
                        leads7dRow,
                        leads30dRow,
                        optInRow,
                        totalMarketSelRow,
                        totalProfileSelRow,
                        topMarketLeadsRow,
                        topGoalLeadsRow,
                        topMarketSelRow,
                        marketCountryRes,
                        marketProfileRes,
                        marketLeadsRes,
                        goalProfileRes,
                        goalLeadsRes
                    ] = await Promise.all([
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(DISTINCT email) AS count FROM gb_leads').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE created_at >= ?').bind(d7).first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE created_at >= ?').bind(d30).first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT COUNT(*) AS count FROM gb_leads WHERE marketing_opt_in = 1').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT SUM(selection_count) AS count FROM gb_market_daily').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT SUM(selection_count) AS count FROM gb_profile_daily').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, COUNT(*) AS count FROM gb_leads GROUP BY market ORDER BY count DESC LIMIT 1').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT intent, COUNT(*) AS count FROM gb_leads GROUP BY intent ORDER BY count DESC LIMIT 1').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, SUM(selection_count) AS count FROM gb_market_daily GROUP BY market ORDER BY count DESC LIMIT 1').first(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, SUM(selection_count) AS count FROM gb_market_daily GROUP BY market ORDER BY count DESC').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, SUM(selection_count) AS count FROM gb_profile_daily GROUP BY market ORDER BY count DESC').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, COUNT(*) AS count FROM gb_leads GROUP BY market ORDER BY count DESC').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT intent, SUM(selection_count) AS count FROM gb_profile_daily GROUP BY intent ORDER BY count DESC').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT intent, COUNT(*) AS count FROM gb_leads GROUP BY intent ORDER BY count DESC').all()
                    ]);

                    const contactProfiles = totalLeadsRow?.count || 0;
                    const countrySelections = totalMarketSelRow?.count || 0;
                    const profileSelections = totalProfileSelRow?.count || 0;

                    const funnel = {
                        countrySelections,
                        profileSelections,
                        contactProfiles,
                        countryToProfilePct: countrySelections > 0 ? ((profileSelections / countrySelections) * 100).toFixed(1) : '0.0',
                        profileToContactPct: profileSelections > 0 ? ((contactProfiles / profileSelections) * 100).toFixed(1) : '0.0',
                        countryToContactPct: countrySelections > 0 ? ((contactProfiles / countrySelections) * 100).toFixed(1) : '0.0',
                        step1ToStep2Pct: countrySelections > 0 ? Math.round((profileSelections / countrySelections) * 1000) / 10 : 0,
                        step2ToStep3Pct: profileSelections > 0 ? Math.round((contactProfiles / profileSelections) * 1000) / 10 : 0
                    };

                    return jsonResponse({
                        ok: true,
                        contactProfiles,
                        uniqueEmails: uniqueRow?.count || 0,
                        leads7d: leads7dRow?.count || 0,
                        leads30d: leads30dRow?.count || 0,
                        marketingOptIns: optInRow?.count || 0,
                        countrySelections,
                        profileSelections,
                        funnel,
                        topMarket: topMarketLeadsRow || topMarketSelRow || null,
                        topGoal: topGoalLeadsRow || null,
                        marketSelections: marketCountryRes.results || [],
                        marketCountrySelections: marketCountryRes.results || [],
                        marketProfiles: marketProfileRes.results || [],
                        marketProfileSelections: marketProfileRes.results || [],
                        marketLeads: marketLeadsRes.results || [],
                        goalSelections: goalProfileRes.results || [],
                        goalProfileSelections: goalProfileRes.results || [],
                        goalLeads: goalLeadsRes.results || []
                    }, 200, adminHeaders);
                } catch (err) {
                    return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                }
            }

            // C. GET /global-buyers-admin/api/matrix
            if (path === '/global-buyers-admin/api/matrix' && request.method === 'GET') {
                try {
                    const [leadsMatrixRes, profileMatrixRes] = await Promise.all([
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, intent, COUNT(*) as count FROM gb_leads GROUP BY market, intent').all(),
                        env.GLOBAL_BUYERS_DB.prepare('SELECT market, intent, SUM(selection_count) as count FROM gb_profile_daily GROUP BY market, intent').all()
                    ]);

                    const leadsMatrix = {};
                    (leadsMatrixRes.results || []).forEach(r => {
                        if (!leadsMatrix[r.market]) leadsMatrix[r.market] = {};
                        leadsMatrix[r.market][r.intent] = r.count;
                    });

                    const profileMatrix = {};
                    (profileMatrixRes.results || []).forEach(r => {
                        if (!profileMatrix[r.market]) profileMatrix[r.market] = {};
                        profileMatrix[r.market][r.intent] = r.count;
                    });

                    return jsonResponse({ ok: true, matrix: leadsMatrix, profileMatrix }, 200, adminHeaders);
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

            // F. GET /global-buyers-admin/api/export.csv
            if (path === '/global-buyers-admin/api/export.csv' && request.method === 'GET') {
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

            // G. GET /global-buyers-admin/api/flight-snapshots
            if (path === '/global-buyers-admin/api/flight-snapshots' && request.method === 'GET') {
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

            // H. DELETE /global-buyers-admin/api/leads/:id
            if (path.startsWith('/global-buyers-admin/api/leads/') && request.method === 'DELETE') {
                try {
                    const leadId = path.replace('/global-buyers-admin/api/leads/', '').trim();
                    if (!leadId) {
                        return jsonResponse({ ok: false, error: 'Lead ID required.' }, 400, adminHeaders);
                    }

                    await env.GLOBAL_BUYERS_DB.prepare('DELETE FROM gb_leads WHERE id = ?').bind(leadId).run();
                    return jsonResponse({ ok: true, deletedId: leadId }, 200, adminHeaders);
                } catch (err) {
                    return jsonResponse({ ok: false, error: err.message }, 500, adminHeaders);
                }
            }

            return jsonResponse({ ok: false, error: 'Admin endpoint not found.' }, 404, adminHeaders);
        }

        return new Response('Not Found', { status: 404, headers: corsHeaders });
    },

    /**
     * Cloudflare Scheduled Cron Handler
     * Runs once daily to sample future travel window benchmark fares into gb_fare_samples
     */
    async scheduled(event, env, ctx) {
        if (!env.DUFFEL_ACCESS_TOKEN) {
            console.log('[Flight Cron] DUFFEL_ACCESS_TOKEN not configured. Skipping sampling.');
            return;
        }

        const origins = [
            { code: 'FRA', market: 'germany', currency: 'EUR' },
            { code: 'YYZ', market: 'canada', currency: 'CAD' },
            { code: 'GRU', market: 'brazil', currency: 'BRL' },
            { code: 'LHR', market: 'uk', currency: 'GBP' },
            { code: 'BOG', market: 'colombia', currency: 'COP' },
            { code: 'EZE', market: 'argentina', currency: 'ARS' },
            { code: 'MEX', market: 'mexico', currency: 'MXN' }
        ];

        const now = new Date();
        const depDate = new Date(now.getTime() + 45 * 86400000).toISOString().split('T')[0];
        const retDate = new Date(now.getTime() + 52 * 86400000).toISOString().split('T')[0];

        for (const item of origins) {
            try {
                const duffelRes = await fetch('https://api.duffel.com/air/offer_requests', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${env.DUFFEL_ACCESS_TOKEN}`,
                        'Duffel-Version': 'v2',
                        'Content-Type': 'application/json',
                        'Accept-Encoding': 'gzip'
                    },
                    body: JSON.stringify({
                        data: {
                            slices: [
                                { origin: item.code, destination: 'RSW', departure_date: depDate },
                                { origin: 'RSW', destination: item.code, departure_date: retDate }
                            ],
                            passengers: [{ type: 'adult' }],
                            cabin_class: 'economy'
                        }
                    })
                });

                if (duffelRes.ok) {
                    const dData = await duffelRes.json();
                    const offers = (dData.data && dData.data.offers) || [];
                    if (offers.length > 0) {
                        offers.sort((a, b) => parseFloat(a.total_amount) - parseFloat(b.total_amount));
                        const best = offers[0];
                        const fareUsd = parseFloat(best.total_amount);
                        const id = crypto.randomUUID();
                        const sampledAt = new Date().toISOString();
                        const outSlice = best.slices && best.slices[0];
                        const outSegments = (outSlice && outSlice.segments) || [];
                        const stops = Math.max(0, outSegments.length - 1);
                        const carrier = (outSegments[0] && outSegments[0].operating_carrier && outSegments[0].operating_carrier.name) || 'Airline Partner';
                        const dur = outSlice ? parseIsoDuration(outSlice.duration) : null;

                        await env.GLOBAL_BUYERS_DB.prepare(`
                            INSERT INTO gb_fare_samples (
                                id, sampled_at, market, origin, destination, departure_date, return_date,
                                fare_usd, local_currency, fare_local, carrier, stops, duration_minutes
                            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                        `).bind(id, sampledAt, item.market, item.code, 'RSW', depDate, retDate, fareUsd, item.currency, null, carrier, stops, dur).run();
                    }
                }
            } catch (err) {
                console.error('[Flight Cron Error for ' + item.code + ']:', err);
            }
        }
    }
};

function parseIsoDuration(duration) {
    if (!duration || typeof duration !== 'string') return 0;
    const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?/);
    if (!match) return 0;
    const hours = parseInt(match[1] || '0', 10);
    const minutes = parseInt(match[2] || '0', 10);
    return hours * 60 + minutes;
}
