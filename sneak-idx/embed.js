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

    // If missing on script tag, check container elements in DOM
    if (!siteKey) {
        const container = document.getElementById('sneak-idx-search') ||
                          document.getElementById('sneak-idx-search-bar') ||
                          document.getElementById('sneak-idx-grid') ||
                          document.getElementById('sneak-idx-open-houses') ||
                          document.querySelector('[data-site]') ||
                          document.querySelector('[data-site-key]');
        if (container) {
            siteKey = container.getAttribute('data-site') || container.getAttribute('data-site-key');
            if (!widgetType) widgetType = container.getAttribute('data-widget');
            if (!customHeight) customHeight = container.getAttribute('data-height');
            if (!targetSelector && container.id) targetSelector = '#' + container.id;
        }
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

    // Bootstrap authorization from member host
    const bootstrapUrl = `${baseUrl}/idx/v1/bootstrap?site=${encodeURIComponent(siteKey)}`;

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
        }

        // Check for parent page auth exchange code (Phase 7.3C1A)
        let parentAuthCode = null;
        let deepListingKey = null;
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
        } catch {}

        // Construct iframe URL with signed session token and deterministic build version
        const separator = widgetPath.includes('?') ? '&' : '?';
        const buildVersion = '2026.08.31.7.3c3a1';
        let iframeUrl = `${baseUrl}${widgetPath}${separator}site=${encodeURIComponent(siteKey)}&session=${encodeURIComponent(data.session)}&embed=true&v=${encodeURIComponent(buildVersion)}`;
        if (parentAuthCode) {
            iframeUrl += `&auth_code=${encodeURIComponent(parentAuthCode)}`;
        }
        if (deepListingKey) {
            iframeUrl += `&ccor_listing=${encodeURIComponent(deepListingKey)}`;
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
            // Desktop Viewport (e.g. 1440x900, 1920x1080)
            return Math.max(860, Math.min(Math.round(viewportHeight * 0.90), 1050));
        }

        let computedHeight = '850px';
        if (isFixedHeight && customHeight) {
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
        iframe.style.minHeight = '550px';
        iframe.style.border = 'none';
        iframe.style.display = 'block';
        iframe.style.overflow = 'hidden';
        iframe.setAttribute('loading', 'lazy');
        iframe.setAttribute('allow', 'geolocation');

        container.appendChild(iframe);
        mountContainer(container);

        // Parent window resize listener for non-fixed responsive embed mode
        if (!isFixedHeight) {
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
            if (isFixedHeight) return;
            if (e.data.siteKey !== siteKey) return;
            if (e.source && e.source !== iframe.contentWindow) return;

            const newHeight = Number(e.data.height);
            if (!Number.isFinite(newHeight) || newHeight < 400 || newHeight > 3000) return;

            // Debounce small jitter <= 3px
            if (Math.abs(newHeight - lastResizeHeight) <= 3) return;
            lastResizeHeight = newHeight;

            iframe.style.height = `${newHeight}px`;
        });

    })
    .catch(function (err) {
        console.warn('[CCOR IDX Plug-in] Bootstrap failed:', err);
        renderAuthError('Unable to load property search at this time.');
    });

})();
