/**
 * sneak-admin/embed-generator.js
 * 
 * Embed snippet generator for tenant sites.
 */

const DEFAULT_SERVING_URL = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";
const EMBED_BUILD = '2026.09.01.7.4b2';

function resolveServingUrl(env = {}) {
    const candidate = env?.SNEAK_SERVING_URL || DEFAULT_SERVING_URL;
    try {
        const parsed = new URL(candidate);
        if (parsed.protocol !== 'https:') throw new Error('Serving URL must use HTTPS.');
        return parsed.origin;
    } catch {
        return DEFAULT_SERVING_URL;
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
