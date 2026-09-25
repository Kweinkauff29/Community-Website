/**
 * sneak-admin/embed-generator.js
 * 
 * Embed snippet generator for tenant sites.
 */

const STAGING_SERVING_URL = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";
const PRODUCTION_SERVING_URL = "https://sneak-idx-worker.bonitaspringsrealtors.workers.dev";
const EMBED_BUILD = '2026.09.01.7.4b2';

function resolveServingUrl(env = {}) {
    const isProd = (env?.SNEAK_ENV || '').toLowerCase() === 'production';
    const defaultServing = isProd ? PRODUCTION_SERVING_URL : STAGING_SERVING_URL;
    const candidate = env?.SNEAK_SERVING_URL || defaultServing;
    try {
        const parsed = new URL(candidate);
        if (parsed.protocol !== 'https:') throw new Error('Serving URL must use HTTPS.');
        if (isProd && parsed.origin.includes('staging')) {
            return PRODUCTION_SERVING_URL;
        }
        return parsed.origin;
    } catch {
        return defaultServing;
    }
}

export function generateEmbedSnippets(siteKey, allowedDomains = [], branding = {}, env = {}) {
    const primaryColor = branding.primary_color || '#1a365d';
    const servingUrl = resolveServingUrl(env);
    const scriptUrl = `${servingUrl}/embed.js?v=${EMBED_BUILD}`;

    const definitions = [
        {
            widgetType: "search",
            targetId: 'sneak-idx-search',
            name: "Full Search & Map",
            description: "Interactive MLS search with grid, filters, and dynamic map markers.",
            htmlSnippet: `<!-- CCOR IDX Full Search Widget -->
<div id="sneak-idx-search" data-site="${siteKey}" data-widget="search" style="width:100%;max-width:100%;"></div>
<script src="${scriptUrl}" data-site="${siteKey}" data-widget="search" data-target="#sneak-idx-search" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "search_bar",
            targetId: 'sneak-idx-search-bar',
            name: "Quick Search Bar",
            description: "Compact single-line MLS property search bar suitable for hero headers.",
            htmlSnippet: `<!-- CCOR IDX Quick Search Bar -->
<div id="sneak-idx-search-bar" data-site="${siteKey}" data-widget="search_bar" style="width:100%;max-width:100%;"></div>
<script src="${scriptUrl}" data-site="${siteKey}" data-widget="search_bar" data-target="#sneak-idx-search-bar" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "listing_grid",
            targetId: 'sneak-idx-grid',
            name: "Listing Grid",
            description: "Responsive grid showcasing active and pending properties.",
            htmlSnippet: `<!-- CCOR IDX Listing Grid -->
<div id="sneak-idx-grid" data-site="${siteKey}" data-widget="listing_grid" style="width:100%;max-width:100%;"></div>
<script src="${scriptUrl}" data-site="${siteKey}" data-widget="listing_grid" data-target="#sneak-idx-grid" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "open_houses",
            targetId: 'sneak-idx-open-houses',
            name: "Open Houses Showcase",
            description: "Scheduled upcoming open house events with calendar tags.",
            htmlSnippet: `<!-- CCOR IDX Open Houses Widget -->
<div id="sneak-idx-open-houses" data-site="${siteKey}" data-widget="open_houses" style="width:100%;max-width:100%;"></div>
<script src="${scriptUrl}" data-site="${siteKey}" data-widget="open_houses" data-target="#sneak-idx-open-houses" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "featured_agent",
            targetId: 'sneak-idx-featured',
            name: "Agent Featured Listings",
            description: "Showcases agent's active and pending listings with headshot branding badges.",
            htmlSnippet: `<!-- CCOR IDX Agent Featured Listings Widget -->
<div id="sneak-idx-featured" data-site="${siteKey}" data-widget="search" data-featured="true" style="width:100%;max-width:100%;"></div>
<script src="${scriptUrl}" data-site="${siteKey}" data-widget="search" data-featured="true" data-target="#sneak-idx-featured" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "landing_page",
            targetId: 'sneak-idx-landing',
            name: "Pre-Filtered City & Price Route Landing Page",
            description: "Dedicated property landing page with auto-detected route parameters and contextual title banner.",
            htmlSnippet: `<!-- CCOR IDX Route Landing Page Widget (Detects city and price range from URL slug or data attributes) -->
<div id="sneak-idx-landing" data-site="${siteKey}" data-widget="search" data-route-mode="auto" style="width:100%;max-width:100%;"></div>
<script src="${scriptUrl}" data-site="${siteKey}" data-widget="search" data-route-mode="auto" data-target="#sneak-idx-landing" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "pinned_listings",
            targetId: 'sneak-idx-pinned',
            name: "Pinned Agent Listings",
            description: "Displays listings with agent properties pinned as the first displayed results.",
            htmlSnippet: `<!-- CCOR IDX Pinned Listings Widget -->
<div id="sneak-idx-pinned" data-site="${siteKey}" data-widget="search" data-pin-own="true" style="width:100%;max-width:100%;"></div>
<script src="${scriptUrl}" data-site="${siteKey}" data-widget="search" data-pin-own="true" data-target="#sneak-idx-pinned" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        }
    ].map(item => ({ ...item, operational: true, responsiveMode: 'adaptive' }));

    const snippets = Object.fromEntries(definitions.map(item => [item.widgetType, item]));

    return {
        siteKey,
        servingHost: servingUrl,
        embedBuild: EMBED_BUILD,
        allowedDomains,
        snippets,
        installationNotes: [
            'Paste one complete snippet into the authorized page where the IDX should appear.',
            'Keep the container ID and data-site value unchanged.',
            'The iframe height adapts automatically; do not add a fixed height unless intentionally overriding responsive behavior.',
            'After installation, verify the page from an active, verified domain.'
        ]
    };
}
