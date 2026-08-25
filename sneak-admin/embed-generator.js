/**
 * sneak-admin/embed-generator.js
 * 
 * Embed snippet generator for tenant sites.
 */

const STAGING_SERVING_URL = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";

export function generateEmbedSnippets(siteKey, allowedDomains = [], branding = {}) {
    const primaryColor = branding.primary_color || '#1a365d';
    const scriptUrl = `${STAGING_SERVING_URL}/embed.js`;

    const snippets = [
        {
            widgetType: "search",
            name: "Full Search & Map",
            description: "Interactive MLS search with grid, filters, and dynamic map markers.",
            htmlSnippet: `<!-- CCOR IDX Full Search Widget -->
<div id="sneak-idx-search" data-site="${siteKey}" data-widget="search" style="width: 100%; min-height: 800px;"></div>
<script src="${scriptUrl}" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "search_bar",
            name: "Quick Search Bar",
            description: "Compact single-line MLS property search bar suitable for hero headers.",
            htmlSnippet: `<!-- CCOR IDX Quick Search Bar -->
<div id="sneak-idx-search-bar" data-site="${siteKey}" data-widget="search_bar" style="width: 100%;"></div>
<script src="${scriptUrl}" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "listing_grid",
            name: "Listing Grid",
            description: "Responsive grid showcasing active and pending properties.",
            htmlSnippet: `<!-- CCOR IDX Listing Grid -->
<div id="sneak-idx-grid" data-site="${siteKey}" data-widget="listing_grid" style="width: 100%; min-height: 600px;"></div>
<script src="${scriptUrl}" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        },
        {
            widgetType: "open_houses",
            name: "Open Houses Showcase",
            description: "Scheduled upcoming open house events with calendar tags.",
            htmlSnippet: `<!-- CCOR IDX Open Houses Widget -->
<div id="sneak-idx-open-houses" data-site="${siteKey}" data-widget="open_houses" style="width: 100%; min-height: 500px;"></div>
<script src="${scriptUrl}" async defer></script>`,
            recommendedWidth: "100%",
            responsive: true
        }
    ];

    return {
        siteKey,
        servingHost: STAGING_SERVING_URL,
        allowedDomains,
        snippets
    };
}
