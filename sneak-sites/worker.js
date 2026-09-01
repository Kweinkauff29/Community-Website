/**
 * sneak-sites/worker.js
 * 
 * Dedicated Cloudflare Worker for Multi-Tenant Low-Cost IDX Website Template Engine.
 * Serves SNEAK Essential, SNEAK Coastal, and SNEAK Brokerage templates.
 */

import { verifyPreviewToken } from './preview.js';
import {
    resolveTenantSite,
    getFeaturedListings,
    getUpcomingOpenHouses
} from './data.js';
import { handleContactSubmission } from './contact.js';

export const SNEAK_SITES_BUILD = '2026.09.01.7.4b2';
import {
    renderEssentialHome,
    renderEssentialSearch,
    renderEssentialOpenHouses,
    renderEssentialAbout,
    renderEssentialContact
} from './templates/essential.js';
import {
    renderCoastalHome,
    renderCoastalSearch,
    renderCoastalOpenHouses,
    renderCoastalAbout,
    renderCoastalContact
} from './templates/coastal.js';
import {
    renderBrokerageHome,
    renderBrokerageSearch,
    renderBrokerageOpenHouses,
    renderBrokerageAbout,
    renderBrokerageContact
} from './templates/brokerage.js';

const SECURITY_HEADERS = {
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'SAMEORIGIN',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Permissions-Policy': 'accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()'
};

function htmlResponse(html, status = 200, headers = {}) {
    return new Response(html, {
        status,
        headers: {
            'Content-Type': 'text/html; charset=UTF-8',
            ...SECURITY_HEADERS,
            ...headers
        }
    });
}

function serviceUnavailablePage() {
    return htmlResponse(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Website Unavailable | SNEAK Real Estate</title>
    <meta name="robots" content="noindex, nofollow">
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; background: #f8fafc; color: #1e293b; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 24px; text-align: center; }
        .box { max-width: 480px; background: #fff; border: 1px solid #e2e8f0; padding: 40px; border-radius: 16px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); }
        h1 { font-size: 1.5rem; margin-bottom: 12px; color: #0f172a; }
        p { color: #64748b; font-size: 0.95rem; line-height: 1.6; }
    </style>
</head>
<body>
    <div class="box">
        <h1>Website Temporarily Unavailable</h1>
        <p>This real estate website is currently undergoing scheduled maintenance or is temporarily inactive. Please check back shortly.</p>
    </div>
</body>
</html>`, 503);
}

function unauthorizedPreviewPage(message) {
    return htmlResponse(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Preview Unauthorized | SNEAK</title>
    <meta name="robots" content="noindex, nofollow">
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #f8fafc; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 24px; text-align: center; }
        .box { max-width: 460px; background: #1e293b; border: 1px solid #334155; padding: 40px; border-radius: 16px; }
        h1 { font-size: 1.4rem; margin-bottom: 12px; color: #f59e0b; }
        p { color: #94a3b8; font-size: 0.95rem; line-height: 1.6; margin-bottom: 24px; }
        code { background: #0f172a; padding: 4px 8px; border-radius: 4px; color: #38bdf8; font-size: 0.85rem; }
    </style>
</head>
<body>
    <div class="box">
        <h1>Preview Access Required</h1>
        <p>${message || 'A valid signed preview token is required to view this website in staging preview mode.'}</p>
        <p style="font-size: 0.8rem; color: #64748b;">Generate a fresh preview link from the SNEAK Admin or Member Portal.</p>
    </div>
</body>
</html>`, 403);
}

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;
        const method = request.method.toUpperCase();

        // 1. Health check
        if (path === '/health') {
            return new Response(JSON.stringify({
                status: 'healthy',
                worker: env?.SNEAK_SERVICE_NAME || 'sneak-idx-sites-staging',
                build: SNEAK_SITES_BUILD,
                timestamp: new Date().toISOString()
            }), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        const rawHost = request.headers.get('X-Forwarded-Host') || request.headers.get('Host') || url.hostname;
        const host = rawHost.split(',')[0].split(':')[0].trim().toLowerCase();

        // 2. Robots.txt
        if (path === '/robots.txt') {
            const isStagingWorkerHost = host.includes('workers.dev') || host.includes('localhost') || host === '127.0.0.1';
            const isPreviewRoute = path.startsWith('/preview/');
            const body = isPreviewRoute || isStagingWorkerHost
                ? "User-agent: *\nDisallow: /\n"
                : "User-agent: *\nAllow: /\nSitemap: https://" + host + "/sitemap.xml\n";
            return new Response(body, {
                status: 200,
                headers: { 'Content-Type': 'text/plain', 'Cache-Control': 'public, max-age=86400' }
            });
        }

        // 3. Match Route Type (Preview Route vs Custom Host Route)
        let siteKey = null;
        let subPath = path;
        let isPreview = false;
        let previewToken = url.searchParams.get('token');
        let basePath = '';

        const previewMatch = path.match(/^\/preview\/([^/]+)(\/.*)?$/);
        if (previewMatch) {
            isPreview = true;
            siteKey = previewMatch[1];
            subPath = previewMatch[2] || '/';
            basePath = `/preview/${siteKey}`;

            // Check preview cookie if token not in URL
            if (!previewToken) {
                const cookieHeader = request.headers.get('Cookie') || '';
                const matchCookie = cookieHeader.match(/sneak_preview_token=([^;]+)/);
                if (matchCookie) previewToken = decodeURIComponent(matchCookie[1]);
            }

            // Verify Preview Token
            const secret = env.SNEAK_WEBSITE_PREVIEW_SECRET;
            if (!secret) return unauthorizedPreviewPage('Website preview signing is not configured.');
            const verification = await verifyPreviewToken(previewToken, siteKey, secret);
            if (!verification.valid) {
                return unauthorizedPreviewPage(verification.error);
            }
        } else {
            // Custom Domain Resolution
            const isStagingWorkerHost = host.includes('workers.dev') || host.includes('localhost') || host === '127.0.0.1';
            
            if (isStagingWorkerHost && path === '/') {
                return htmlResponse(`
                    <div style="font-family: sans-serif; text-align: center; padding: 80px 20px;">
                        <h1>SNEAK IDX Website Engine (Staging)</h1>
                        <p style="color: #64748b; margin-top: 12px;">Use a signed preview URL (<code>/preview/:siteKey?token=...</code>) or custom tenant domain.</p>
                    </div>
                `);
            }
        }

        // 4. Resolve Tenant Site
        const siteBundle = await resolveTenantSite(env.DB, {
            siteKey: siteKey || undefined,
            domain: !siteKey ? host : undefined
        });

        if (!siteBundle) {
            return htmlResponse('<h1>404 — Site Not Found</h1><p>No real estate website is configured for this domain or key.</p>', 404);
        }

        // 5. Entitlement & Operational Guard
        if (!siteBundle.isOperational) {
            return serviceUnavailablePage();
        }

        // 6. Handle Contact Lead API
        if ((subPath === '/api/contact' || subPath === '/contact') && method === 'POST') {
            return await handleContactSubmission(request, env, siteBundle);
        }

        // 7. Sitemap.xml
        if (subPath === '/sitemap.xml') {
            const rootUrl = isPreview ? `${url.origin}${basePath}` : `https://${host}`;
            const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url><loc>${rootUrl}/</loc><priority>1.0</priority></url>
    <url><loc>${rootUrl}/search</loc><priority>0.9</priority></url>
    <url><loc>${rootUrl}/open-houses</loc><priority>0.8</priority></url>
    <url><loc>${rootUrl}/about</loc><priority>0.7</priority></url>
    <url><loc>${rootUrl}/contact</loc><priority>0.8</priority></url>
</urlset>`;
            return new Response(xml, {
                status: 200,
                headers: { 'Content-Type': 'application/xml', 'Cache-Control': 'public, max-age=3600' }
            });
        }

        // 8. Load Scoped MLS Data for Home / Open Houses
        const [featuredListings, openHouses] = await Promise.all([
            (subPath === '/' || subPath === '') ? getFeaturedListings(env.DB, siteBundle.site, 6) : Promise.resolve([]),
            (subPath === '/' || subPath === '' || subPath === '/open-houses') ? getUpcomingOpenHouses(env.DB, siteBundle.site, 6) : Promise.resolve([])
        ]);

        // 9. Template Dispatcher
        const template = (siteBundle.websiteConfig.template_key || 'essential').toLowerCase();

        // Optional cookie header to persist preview session across navigation clicks
        const cookieHeader = isPreview && previewToken ? {
            'Set-Cookie': `sneak_preview_token=${encodeURIComponent(previewToken)}; Path=/preview/${siteKey}; SameSite=Lax; Max-Age=1800`
        } : {};

        let responseHtml = '';

        if (template === 'coastal') {
            switch (subPath) {
                case '/':
                case '':
                    responseHtml = renderCoastalHome({ siteBundle, featuredListings, openHouses, basePath, previewToken });
                    break;
                case '/search':
                    responseHtml = renderCoastalSearch({ siteBundle, basePath, previewToken });
                    break;
                case '/open-houses':
                    responseHtml = renderCoastalOpenHouses({ siteBundle, openHouses, basePath, previewToken });
                    break;
                case '/about':
                    responseHtml = renderCoastalAbout({ siteBundle, basePath, previewToken });
                    break;
                case '/contact':
                    responseHtml = renderCoastalContact({ siteBundle, basePath, previewToken });
                    break;
                default:
                    return htmlResponse('<h1>Page Not Found</h1>', 404);
            }
        } else if (template === 'brokerage') {
            switch (subPath) {
                case '/':
                case '':
                    responseHtml = renderBrokerageHome({ siteBundle, featuredListings, openHouses, basePath, previewToken });
                    break;
                case '/search':
                    responseHtml = renderBrokerageSearch({ siteBundle, basePath, previewToken });
                    break;
                case '/open-houses':
                    responseHtml = renderBrokerageOpenHouses({ siteBundle, openHouses, basePath, previewToken });
                    break;
                case '/about':
                    responseHtml = renderBrokerageAbout({ siteBundle, basePath, previewToken });
                    break;
                case '/contact':
                    responseHtml = renderBrokerageContact({ siteBundle, basePath, previewToken });
                    break;
                default:
                    return htmlResponse('<h1>Page Not Found</h1>', 404);
            }
        } else {
            // Default: SNEAK Essential
            switch (subPath) {
                case '/':
                case '':
                    responseHtml = renderEssentialHome({ siteBundle, featuredListings, openHouses, basePath, previewToken });
                    break;
                case '/search':
                    responseHtml = renderEssentialSearch({ siteBundle, basePath, previewToken });
                    break;
                case '/open-houses':
                    responseHtml = renderEssentialOpenHouses({ siteBundle, openHouses, basePath, previewToken });
                    break;
                case '/about':
                    responseHtml = renderEssentialAbout({ siteBundle, basePath, previewToken });
                    break;
                case '/contact':
                    responseHtml = renderEssentialContact({ siteBundle, basePath, previewToken });
                    break;
                default:
                    return htmlResponse('<h1>Page Not Found</h1>', 404);
            }
        }

        return htmlResponse(responseHtml, 200, cookieHeader);
    }
};
