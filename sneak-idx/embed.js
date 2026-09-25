/**
 * SNEAK IDX Embed Loader (embed.js)
 * 
 * Lightweight, zero-dependency widget embed loader for the SNEAK IDX Platform.
 * Bootstraps authorization directly from member webpage and embeds isolated IDX widgets.
 * 
 * Usage:
 * <script 
 *   src="https://your-sneak-host.com/embed.js" 
 *   data-site="demo-ccor" 
 *   data-widget="search" 
 *   data-height="850px">
 * </script>
 */

(function () {
    'use strict';

    // Find the currently executing script element
    const currentScript = document.currentScript || (function () {
        const scripts = document.getElementsByTagName('script');
        return scripts[scripts.length - 1];
    })();

    if (!currentScript) {
        console.error('[SNEAK IDX] Unable to locate embed script element.');
        return;
    }

    // Read configuration attributes from script tag or fallback to container element
    let siteKey = currentScript.getAttribute('data-site') || currentScript.getAttribute('data-site-key');
    let widgetType = currentScript.getAttribute('data-widget');
    let customHeight = currentScript.getAttribute('data-height');
    let targetSelector = currentScript.getAttribute('data-target');
    let customParams = currentScript.getAttribute('data-params') || '';

    let scriptCity = currentScript.getAttribute('data-city');
    let scriptMinPrice = currentScript.getAttribute('data-min-price');
    let scriptMaxPrice = currentScript.getAttribute('data-max-price');
    let scriptPropType = currentScript.getAttribute('data-property-type');
    let scriptAgent = currentScript.getAttribute('data-agent') || currentScript.getAttribute('data-agent-mls-id');
    let scriptOpenHouses = currentScript.getAttribute('data-open-houses') || currentScript.getAttribute('data-open-house');
    let scriptFeatured = currentScript.getAttribute('data-featured');
    let scriptHeading = currentScript.getAttribute('data-heading');
    let routeMode = currentScript.getAttribute('data-route-mode'); // 'auto', 'true', 'false'
    let scriptPinOwn = currentScript.getAttribute('data-pin-own');
    let scriptPinAgent = currentScript.getAttribute('data-pin-agent') || currentScript.getAttribute('data-pin-agents');
    let scriptPinListings = currentScript.getAttribute('data-pin-listings') || currentScript.getAttribute('data-pin-listing');
    let scriptLocation = currentScript.getAttribute('data-location');
    let scriptSubdivision = currentScript.getAttribute('data-subdivision');
    let scriptBeds = currentScript.getAttribute('data-beds') || currentScript.getAttribute('data-bed');
    let scriptBaths = currentScript.getAttribute('data-baths') || currentScript.getAttribute('data-bath');
    let scriptPrice = currentScript.getAttribute('data-price');
    let scriptRedirectUrl = currentScript.getAttribute('data-redirect-url') || currentScript.getAttribute('data-redirect');
    let scriptLayout = currentScript.getAttribute('data-layout');
    let scriptHideFilters = currentScript.getAttribute('data-hide-filters');
    let scriptMode = currentScript.getAttribute('data-mode');

    // Resolve container element: prefer explicit targetSelector, then preceding sibling, then standard IDs
    let containerEl = targetSelector ? document.querySelector(targetSelector) : null;
    if (!containerEl) {
        const prev = currentScript.previousElementSibling;
        if (prev && (prev.hasAttribute('data-site') || prev.hasAttribute('data-site-key') || (prev.id && prev.id.startsWith('sneak-idx-')))) {
            containerEl = prev;
        } else {
            containerEl = document.getElementById('sneak-idx-quick-search') ||
                          document.getElementById('sneak-idx-search-bar') ||
                          document.getElementById('sneak-idx-bar') ||
                          document.getElementById('sneak-idx-grid') ||
                          document.getElementById('sneak-idx-landing') ||
                          document.getElementById('sneak-idx-featured') ||
                          document.getElementById('sneak-idx-search') ||
                          document.getElementById('sneak-idx-open-houses');
        }
    }
    if (containerEl) {
        if (!siteKey) siteKey = containerEl.getAttribute('data-site') || containerEl.getAttribute('data-site-key');
        if (!widgetType) widgetType = containerEl.getAttribute('data-widget');
        if (!customHeight) customHeight = containerEl.getAttribute('data-height');
        if (!scriptMode) scriptMode = containerEl.getAttribute('data-mode');
        if (!targetSelector && containerEl.id) targetSelector = '#' + containerEl.id;
        if (!scriptCity) scriptCity = containerEl.getAttribute('data-city');
        if (!scriptMinPrice) scriptMinPrice = containerEl.getAttribute('data-min-price');
        if (!scriptMaxPrice) scriptMaxPrice = containerEl.getAttribute('data-max-price');
        if (!scriptPropType) scriptPropType = containerEl.getAttribute('data-property-type');
        if (!scriptAgent) scriptAgent = containerEl.getAttribute('data-agent') || containerEl.getAttribute('data-agent-mls-id');
        if (!scriptOpenHouses) scriptOpenHouses = containerEl.getAttribute('data-open-houses') || containerEl.getAttribute('data-open-house');
        if (!scriptFeatured) scriptFeatured = containerEl.getAttribute('data-featured');
        if (!scriptHeading) scriptHeading = containerEl.getAttribute('data-heading');
        if (!routeMode) routeMode = containerEl.getAttribute('data-route-mode');
        if (!scriptPinOwn) scriptPinOwn = containerEl.getAttribute('data-pin-own');
        if (!scriptPinAgent) scriptPinAgent = containerEl.getAttribute('data-pin-agent') || containerEl.getAttribute('data-pin-agents');
        if (!scriptPinListings) scriptPinListings = containerEl.getAttribute('data-pin-listings') || containerEl.getAttribute('data-pin-listing');
        if (!scriptLocation) scriptLocation = containerEl.getAttribute('data-location');
        if (!scriptSubdivision) scriptSubdivision = containerEl.getAttribute('data-subdivision');
        if (!scriptBeds) scriptBeds = containerEl.getAttribute('data-beds') || containerEl.getAttribute('data-bed');
        if (!scriptBaths) scriptBaths = containerEl.getAttribute('data-baths') || containerEl.getAttribute('data-bath');
        if (!scriptPrice) scriptPrice = containerEl.getAttribute('data-price');
        if (!scriptRedirectUrl) scriptRedirectUrl = containerEl.getAttribute('data-redirect-url') || containerEl.getAttribute('data-redirect');
        if (!scriptLayout) scriptLayout = containerEl.getAttribute('data-layout');
        if (!scriptHideFilters) scriptHideFilters = containerEl.getAttribute('data-hide-filters');
    }

    const isSearchBarWidget = scriptMode === 'bar' ||
        widgetType === 'quick-search' || widgetType === 'quick_search' ||
        widgetType === 'search-bar' || widgetType === 'search_bar' || widgetType === 'bar' ||
        (containerEl && (containerEl.id === 'sneak-idx-search-bar' || containerEl.id === 'sneak-idx-quick-search' || containerEl.id === 'sneak-idx-bar'));

    if (isSearchBarWidget) {
        widgetType = 'quick-search';
    } else if (widgetType === 'listing_grid' || widgetType === 'grid') {
        scriptLayout = scriptLayout || 'grid';
        widgetType = 'search';
    }

    widgetType = widgetType || 'search';
    let isFixedHeight = currentScript.getAttribute('data-fixed-height') === 'true';

    if (!siteKey) {
        console.error('[SNEAK IDX] Missing required "data-site" attribute on embed script or container.');
        return;
    }

    // Resolve Base Host URL & Widget Root
    let baseUrl = currentScript.getAttribute('data-base-url');
    let isSubdirectory = false;

    if (!baseUrl) {
        try {
            const scriptSrc = currentScript.src;
            if (scriptSrc && scriptSrc.startsWith('http')) {
                const parsed = new URL(scriptSrc);
                baseUrl = parsed.origin;
                if (parsed.pathname.includes('/sneak-idx/')) {
                    isSubdirectory = true;
                }
            } else {
                baseUrl = window.location.origin;
            }
        } catch {
            baseUrl = window.location.origin;
        }
    }

    // Helper to insert container into DOM
    function mountContainer(element) {
        if (targetSelector) {
            const targetEl = document.querySelector(targetSelector);
            if (targetEl) {
                targetEl.appendChild(element);
                return;
            }
        }
        currentScript.parentNode.insertBefore(element, currentScript.nextSibling);
    }

    // Helper to display neutral authorization error
    function renderAuthError(msg) {
        const errContainer = document.createElement('div');
        errContainer.className = 'sneak-idx-error';
        errContainer.style.cssText = 'padding:24px 16px;text-align:center;color:#64748b;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;font-size:14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;margin:12px 0;';
        errContainer.textContent = msg || 'SNEAK IDX is not authorized for this website.';
        mountContainer(errContainer);
    }

    // Bootstrap authorization from the member host. The current page is
    // supplied as an untrusted candidate and revalidated by the Serving Worker
    // before it can ever become a share-link base.
    function scrubHostPageCandidate(rawUrl) {
        try {
            const parsed = new URL(rawUrl);
            const sensitive = new Set([
                'auth_code', 'session', 'consumer_session', 'consumersession',
                'member_session', 'token', 'access_token', 'bearer',
                'authorization', 'bootstrap', 'bootstrap_token', 'magic_token',
                'magic_link_token', 'csess', 'ccor_listing', 'ccor_list'
            ]);
            Array.from(parsed.searchParams.keys()).forEach(function (key) {
                if (sensitive.has(key.toLowerCase())) parsed.searchParams.delete(key);
            });
            parsed.hash = '';
            return parsed.toString();
        } catch {
            return '';
        }
    }

    const safeHostCandidate = scrubHostPageCandidate(window.location.href);
    let bootstrapUrl = `${baseUrl}/idx/v1/bootstrap?site=${encodeURIComponent(siteKey)}`;
    if (safeHostCandidate) {
        bootstrapUrl += `&hostPageUrl=${encodeURIComponent(safeHostCandidate)}`;
    }

    fetch(bootstrapUrl, {
        method: 'GET',
        headers: { 'Accept': 'application/json' }
    })
    .then(function (res) {
        if (res.status === 403 || res.status === 401) {
            renderAuthError('SNEAK IDX is not authorized for this website.');
            return null;
        }
        if (!res.ok) {
            renderAuthError('Unable to load property search at this time.');
            return null;
        }
        return res.json();
    })
    .then(function (data) {
        if (!data || !data.success || !data.session) {
            return;
        }

        // Determine target widget path
        let widgetRoot = isSubdirectory ? '/sneak-idx/search/' : '/search/';
        let widgetPath = widgetRoot;
        if (widgetType === 'open_houses' || widgetType === 'open-houses') {
            widgetPath = `${widgetRoot}?type=open-houses&`;
        } else if (widgetType === 'quick-search' || widgetType === 'quick_search' || widgetType === 'search-bar' || widgetType === 'search_bar' || widgetType === 'bar') {
            widgetPath = isSubdirectory ? '/sneak-idx/quick-search/' : '/quick-search/';
            if (!customHeight) customHeight = '195px';
        }

        // Check for parent page auth exchange code (Phase 7.3C1A)
        let parentAuthCode = null;
        let deepListingKey = null;
        let deepListSlug = null;
        try {
            const currentUrl = new URL(window.location.href);
            if (currentUrl.searchParams.has('auth_code')) {
                parentAuthCode = currentUrl.searchParams.get('auth_code');
                currentUrl.searchParams.delete('auth_code');
                window.history.replaceState({}, document.title, currentUrl.pathname + (currentUrl.search ? currentUrl.search : '') + currentUrl.hash);
            }
            if (currentUrl.searchParams.has('ccor_listing')) {
                deepListingKey = currentUrl.searchParams.get('ccor_listing');
            }
            if (currentUrl.searchParams.has('ccor_list')) {
                deepListSlug = currentUrl.searchParams.get('ccor_list');
            }
        } catch {}

        // Parse contextual route slugs & parameters (Phase 7.4B3)
        function parseParentRouteAndFilters() {
            const filters = {};
            if (routeMode !== 'false') {
                try {
                    const pathname = window.location.pathname.toLowerCase();
                    const formatCitySlug = (slug) => {
                        return slug.split('-')
                            .map(w => w.charAt(0).toUpperCase() + w.slice(1))
                            .join(' ');
                    };

                    // Pattern 1: /homes-for-sale-in-{city}-fl-{min}-to-{max} or /condos-for-sale-in-...
                    const priceRouteMatch = pathname.match(/(?:homes|condos|properties|rentals)-for-(?:sale|rent)-in-([a-z0-9-]+?)(?:-fl)?-(\d+)-to-(\d+)/i) ||
                                            pathname.match(/\/([a-z0-9-]+?)(?:-fl)?-(\d+)-to-(\d+)/i);
                    if (priceRouteMatch) {
                        const rawCitySlug = priceRouteMatch[1].replace(/-fl$/i, '');
                        filters.city = formatCitySlug(rawCitySlug);
                        filters.minPrice = priceRouteMatch[2];
                        filters.maxPrice = priceRouteMatch[3];
                        if (pathname.includes('rental') || pathname.includes('for-rent')) {
                            filters.propertyType = 'rental';
                        } else if (pathname.includes('condo')) {
                            filters.propertyType = 'sale';
                            filters.propertySubType = 'Condominium';
                        } else {
                            filters.propertyType = 'sale';
                        }
                    } else {
                        // Pattern 2: /homes-for-sale-in-{city}(?:-fl)? or /open-houses-in-{city}
                        const cityRouteMatch = pathname.match(/(?:homes|properties|condos)-for-sale-in-([a-z0-9-]+?)(?:-fl)?(?:\/|$)/i) ||
                                               pathname.match(/open-houses-in-([a-z0-9-]+?)(?:-fl)?(?:\/|$)/i) ||
                                               pathname.match(/real-estate-in-([a-z0-9-]+?)(?:-fl)?(?:\/|$)/i);
                        if (cityRouteMatch) {
                            const rawCitySlug = cityRouteMatch[1].replace(/-fl$/i, '');
                            filters.city = formatCitySlug(rawCitySlug);
                            filters.propertyType = 'sale';
                            if (pathname.includes('open-house')) {
                                filters.openHouses = '1';
                            }
                        } else if (pathname.includes('open-houses') || pathname.includes('open-house')) {
                            filters.openHouses = '1';
                        } else if (pathname.includes('featured-listings') || pathname.includes('agent-listings') || pathname.includes('our-listings')) {
                            filters.featured = '1';
                        }
                    }
                } catch (err) {
                    console.warn('[SNEAK IDX] Route parse error:', err);
                }
            }

            // Parent URL query parameters
            try {
                const currentUrl = new URL(window.location.href);
                if (currentUrl.searchParams.has('location')) filters.location = currentUrl.searchParams.get('location');
                if (currentUrl.searchParams.has('subdivision')) filters.subdivision = currentUrl.searchParams.get('subdivision');
                if (currentUrl.searchParams.has('city')) filters.city = currentUrl.searchParams.get('city');
                if (currentUrl.searchParams.has('price')) filters.price = currentUrl.searchParams.get('price');
                if (currentUrl.searchParams.has('minPrice') || currentUrl.searchParams.has('min_price')) {
                    filters.minPrice = currentUrl.searchParams.get('minPrice') || currentUrl.searchParams.get('min_price');
                }
                if (currentUrl.searchParams.has('maxPrice') || currentUrl.searchParams.has('max_price')) {
                    filters.maxPrice = currentUrl.searchParams.get('maxPrice') || currentUrl.searchParams.get('max_price');
                }
                if (currentUrl.searchParams.has('beds') || currentUrl.searchParams.has('bed')) {
                    filters.beds = currentUrl.searchParams.get('beds') || currentUrl.searchParams.get('bed');
                }
                if (currentUrl.searchParams.has('baths') || currentUrl.searchParams.has('bath')) {
                    filters.baths = currentUrl.searchParams.get('baths') || currentUrl.searchParams.get('bath');
                }
                if (currentUrl.searchParams.has('propertyType')) filters.propertyType = currentUrl.searchParams.get('propertyType');
                if (currentUrl.searchParams.has('agent')) filters.agent = currentUrl.searchParams.get('agent');
                if (currentUrl.searchParams.has('agentMlsId')) filters.agent = currentUrl.searchParams.get('agentMlsId');
                if (currentUrl.searchParams.has('openHouses') || currentUrl.searchParams.has('open_houses')) filters.openHouses = '1';
                if (currentUrl.searchParams.has('featured')) filters.featured = '1';
                if (currentUrl.searchParams.has('heading')) filters.heading = currentUrl.searchParams.get('heading');
                if (currentUrl.searchParams.has('pinOwn')) filters.pinOwn = currentUrl.searchParams.get('pinOwn');
                if (currentUrl.searchParams.has('pinAgent')) filters.pinAgent = currentUrl.searchParams.get('pinAgent');
                if (currentUrl.searchParams.has('pinAgents')) filters.pinAgent = currentUrl.searchParams.get('pinAgents');
                if (currentUrl.searchParams.has('pinListings')) filters.pinListings = currentUrl.searchParams.get('pinListings');
                if (currentUrl.searchParams.has('layout')) filters.layout = currentUrl.searchParams.get('layout');
                if (currentUrl.searchParams.has('hideFilters') || currentUrl.searchParams.has('hide_filters')) filters.hideFilters = '1';
            } catch {}

            // Script/container data attributes override
            if (scriptLocation) filters.location = scriptLocation;
            if (scriptSubdivision) filters.subdivision = scriptSubdivision;
            if (scriptCity) filters.city = scriptCity;
            if (scriptPrice) filters.price = scriptPrice;
            if (scriptMinPrice) filters.minPrice = scriptMinPrice;
            if (scriptMaxPrice) filters.maxPrice = scriptMaxPrice;
            if (scriptBeds) filters.beds = scriptBeds;
            if (scriptBaths) filters.baths = scriptBaths;
            if (scriptPropType) filters.propertyType = scriptPropType;
            if (scriptAgent) filters.agent = scriptAgent;
            if (scriptOpenHouses === 'true' || scriptOpenHouses === '1') filters.openHouses = '1';
            if (scriptFeatured === 'true' || scriptFeatured === '1') filters.featured = '1';
            if (scriptHeading) filters.heading = scriptHeading;
            if (scriptPinOwn === 'true' || scriptPinOwn === '1') filters.pinOwn = '1';
            if (scriptPinAgent) filters.pinAgent = scriptPinAgent;
            if (scriptPinListings) filters.pinListings = scriptPinListings;
            if (scriptLayout) filters.layout = scriptLayout;
            if (scriptHideFilters === 'true' || scriptHideFilters === '1') filters.hideFilters = '1';

            return filters;
        }

        const routeFilters = parseParentRouteAndFilters();

        // Construct iframe URL with signed session token and deterministic build version
        const separator = widgetPath.includes('?') ? '&' : '?';
        const buildVersion = '2026.09.25.2';
        let iframeUrl = `${baseUrl}${widgetPath}${separator}site=${encodeURIComponent(siteKey)}&session=${encodeURIComponent(data.session)}&embed=true&v=${encodeURIComponent(buildVersion)}`;
        if (data.hostPageUrl) {
            iframeUrl += `&host_page=${encodeURIComponent(data.hostPageUrl)}`;
        }
        if (parentAuthCode) {
            iframeUrl += `&auth_code=${encodeURIComponent(parentAuthCode)}`;
        }
        if (deepListingKey) {
            iframeUrl += `&ccor_listing=${encodeURIComponent(deepListingKey)}`;
        }
        if (deepListSlug) {
            iframeUrl += `&ccor_list=${encodeURIComponent(deepListSlug)}`;
        }
        if (routeFilters.location) {
            iframeUrl += `&location=${encodeURIComponent(routeFilters.location)}`;
        }
        if (routeFilters.subdivision) {
            iframeUrl += `&subdivision=${encodeURIComponent(routeFilters.subdivision)}`;
        }
        if (routeFilters.city) {
            iframeUrl += `&city=${encodeURIComponent(routeFilters.city)}`;
        }
        if (routeFilters.price) {
            iframeUrl += `&price=${encodeURIComponent(routeFilters.price)}`;
        }
        if (routeFilters.minPrice) {
            iframeUrl += `&minPrice=${encodeURIComponent(routeFilters.minPrice)}`;
        }
        if (routeFilters.maxPrice) {
            iframeUrl += `&maxPrice=${encodeURIComponent(routeFilters.maxPrice)}`;
        }
        if (routeFilters.beds) {
            iframeUrl += `&beds=${encodeURIComponent(routeFilters.beds)}`;
        }
        if (routeFilters.baths) {
            iframeUrl += `&baths=${encodeURIComponent(routeFilters.baths)}`;
        }
        if (scriptRedirectUrl) {
            iframeUrl += `&redirect_url=${encodeURIComponent(scriptRedirectUrl)}`;
        }
        if (routeFilters.propertyType) {
            iframeUrl += `&propertyType=${encodeURIComponent(routeFilters.propertyType)}`;
        }
        if (routeFilters.propertySubType) {
            iframeUrl += `&propertySubType=${encodeURIComponent(routeFilters.propertySubType)}`;
        }
        if (routeFilters.agent) {
            iframeUrl += `&agent=${encodeURIComponent(routeFilters.agent)}`;
        }
        if (routeFilters.openHouses) {
            iframeUrl += `&openHouses=1`;
        }
        if (routeFilters.featured) {
            iframeUrl += `&featured=1`;
        }
        if (routeFilters.heading) {
            iframeUrl += `&heading=${encodeURIComponent(routeFilters.heading)}`;
        }
        if (routeFilters.pinOwn) {
            iframeUrl += `&pinOwn=1`;
        }
        if (routeFilters.pinAgent) {
            iframeUrl += `&pinAgent=${encodeURIComponent(routeFilters.pinAgent)}`;
        }
        if (routeFilters.pinListings) {
            iframeUrl += `&pinListings=${encodeURIComponent(routeFilters.pinListings)}`;
        }
        if (routeFilters.layout) {
            iframeUrl += `&layout=${encodeURIComponent(routeFilters.layout)}`;
        }
        if (routeFilters.hideFilters) {
            iframeUrl += `&hideFilters=1`;
        }
        if (customParams) {
            iframeUrl += `&${customParams}`;
        }

        // Create wrapper container (WordPress / Beaver Builder safe)
        const container = document.createElement('div');
        container.className = 'sneak-idx-widget-container';
        container.style.width = '100%';
        container.style.maxWidth = '100%';
        container.style.position = 'relative';
        container.style.boxSizing = 'border-box';

        // Deterministic responsive height computation for search application
        function getRecommendedSearchHeight(viewportWidth, viewportHeight) {
            if (viewportWidth <= 600) {
                // Mobile Viewport (e.g. 390x844)
                return Math.max(680, Math.min(Math.round(viewportHeight * 0.88), 850));
            }
            if (viewportWidth <= 1024) {
                // Tablet Viewport (e.g. 1024x768)
                return Math.max(760, Math.min(Math.round(viewportHeight * 0.88), 920));
            }
            // Desktop Viewport (e.g. 1440x900, 1920x1080) - Expanded layout
            return Math.max(900, Math.min(Math.round(viewportHeight * 0.92), 1150));
        }

        const isQuickSearch = widgetType === 'quick-search';
        const isShowcase = ['grid', 'showcase', 'listing_grid'].includes(routeFilters.layout);
        let computedHeight = '900px';
        if (isQuickSearch) {
            computedHeight = customHeight || '185px';
            container.style.height = computedHeight;
            container.style.position = 'relative';
        } else if (isFixedHeight && customHeight) {
            computedHeight = customHeight;
        } else {
            const initialNumeric = getRecommendedSearchHeight(window.innerWidth || 1440, window.innerHeight || 900);
            computedHeight = `${initialNumeric}px`;
        }

        // Create responsive iframe
        const iframe = document.createElement('iframe');
        iframe.src = iframeUrl;
        iframe.title = `CCOR IDX Real Estate Search (${siteKey})`;
        iframe.style.width = '100%';
        iframe.style.height = computedHeight;
        iframe.style.minHeight = isQuickSearch ? '160px' : isShowcase ? '0' : '550px';
        iframe.style.border = 'none';
        iframe.style.display = 'block';
        iframe.style.overflow = 'hidden';
        iframe.setAttribute('loading', 'lazy');
        iframe.setAttribute('allow', 'geolocation');

        container.appendChild(iframe);
        mountContainer(container);

        // Parent window resize listener for non-fixed responsive embed mode
        if (!isFixedHeight && !isQuickSearch && !isShowcase) {
            let resizeDebounceTimer = null;
            window.addEventListener('resize', function () {
                if (resizeDebounceTimer) clearTimeout(resizeDebounceTimer);
                resizeDebounceTimer = setTimeout(function () {
                    const w = window.innerWidth || 1440;
                    const h = window.innerHeight || 900;
                    const nextH = getRecommendedSearchHeight(w, h);
                    iframe.style.height = `${nextH}px`;
                }, 150);
            });
        }

        // Optional Secured postMessage listener for adaptive resizing
        let lastResizeHeight = 0;
        window.addEventListener('message', function (e) {
            if (!e.data || e.data.type !== 'SNEAK_RESIZE') return;
            if (e.origin !== new URL(baseUrl).origin) return;
            if (isFixedHeight) return;
            if (e.source && e.source !== iframe.contentWindow) return;
            if (e.data.siteKey && e.data.siteKey !== siteKey && e.data.siteKey !== 'ursulaweinkauff-com') return;

            const newHeight = Number(e.data.height);
            if (!Number.isFinite(newHeight) || newHeight < 140 || newHeight > 3500) return;

            // Debounce small jitter <= 3px
            if (!isQuickSearch && Math.abs(newHeight - lastResizeHeight) <= 3) return;
            lastResizeHeight = newHeight;

            if (isQuickSearch) {
                const baseH = Number(e.data.baseHeight) || newHeight;
                container.style.height = `${baseH}px`;
                container.style.zIndex = e.data.isOverlay ? '1000' : '';
                iframe.style.position = 'absolute';
                iframe.style.top = '0';
                iframe.style.left = '0';
                iframe.style.height = `${newHeight}px`;
                return;
            }
            iframe.style.height = `${newHeight}px`;
            if (container) {
                container.style.height = `${newHeight}px`;
            }
        });

    })
    .catch(function (err) {
        console.warn('[CCOR IDX Plug-in] Bootstrap failed:', err);
        renderAuthError('Unable to load property search at this time.');
    });

})();
