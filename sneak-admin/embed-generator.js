/**
 * sneak-admin/embed-generator.js
 * 
 * Embed snippet generator for tenant sites.
 */

const STAGING_SERVING_URL = "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev";

export function generateEmbedSnippets(siteKey, allowedDomains = [], branding = {}) {
    const primaryColor = branding.primary_color || '#1a365d';

    const snippets = {
        search: {
            name: "Full Search & Interactive Map",
            description: "Complete searchable MLS map and listings explorer.",
            operational: true,
            htmlSnippet: `<!-- SNEAK IDX Full Search Widget -->
<div id="sneak-idx-search" data-site="${siteKey}" data-widget="search" style="width: 100%; min-height: 800px;"></div>
<script src="${STAGING_SERVING_URL}/embed.js" data-site="${siteKey}" data-widget="search" async defer></script>`,
            previewUrl: `${STAGING_SERVING_URL}/search/?site=${siteKey}`
        },
        search_bar: {
            name: "Quick Search Bar",
            description: "Compact search bar for homepages redirecting to the full search experience.",
            operational: true,
            htmlSnippet: `<!-- SNEAK IDX Quick Search Bar -->
<div id="sneak-idx-search-bar" data-site="${siteKey}" data-widget="search_bar" style="width: 100%;"></div>
<script src="${STAGING_SERVING_URL}/embed.js" data-site="${siteKey}" data-widget="search_bar" async defer></script>`,
            previewUrl: `${STAGING_SERVING_URL}/search/?site=${siteKey}&view=compact`
        },
        listing_grid: {
            name: "Featured Listings Grid",
            description: "Responsive showcase grid of in-scope active inventory.",
            operational: true,
            htmlSnippet: `<!-- SNEAK IDX Listing Grid -->
<div id="sneak-idx-grid" data-site="${siteKey}" data-widget="listing_grid" style="width: 100%; min-height: 600px;"></div>
<script src="${STAGING_SERVING_URL}/embed.js" data-site="${siteKey}" data-widget="listing_grid" async defer></script>`,
            previewUrl: `${STAGING_SERVING_URL}/search/?site=${siteKey}&layout=grid`
        },
        open_houses: {
            name: "Upcoming Open Houses",
            description: "Live schedule of upcoming open houses for in-scope properties.",
            operational: true,
            htmlSnippet: `<!-- SNEAK IDX Open Houses Widget -->
<div id="sneak-idx-open-houses" data-site="${siteKey}" data-widget="open_houses" style="width: 100%; min-height: 500px;"></div>
<script src="${STAGING_SERVING_URL}/embed.js" data-site="${siteKey}" data-widget="open_houses" async defer></script>`,
            previewUrl: `${STAGING_SERVING_URL}/search/?site=${siteKey}&openhouses=1`
        }
    };

    return {
        siteKey,
        servingHost: STAGING_SERVING_URL,
        allowedDomains,
        snippets
    };
}
