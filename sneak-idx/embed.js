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
    customHeight = customHeight || '850px';

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

        // Construct iframe URL with signed session token and deterministic build version
        const separator = widgetPath.includes('?') ? '&' : '?';
        const buildVersion = '2026.08.27.7.3b1';
        let iframeUrl = `${baseUrl}${widgetPath}${separator}site=${encodeURIComponent(siteKey)}&session=${encodeURIComponent(data.session)}&embed=true&v=${encodeURIComponent(buildVersion)}`;
        if (customParams) {
            iframeUrl += `&${customParams}`;
        }

        // Create wrapper container
        const container = document.createElement('div');
        container.className = 'sneak-idx-widget-container';
        container.style.width = '100%';
        container.style.maxWidth = '100%';
        container.style.position = 'relative';
        container.style.overflow = 'hidden';
        container.style.boxSizing = 'border-box';

        // Create responsive iframe
        const iframe = document.createElement('iframe');
        iframe.src = iframeUrl;
        iframe.title = `CCOR IDX Real Estate Search (${siteKey})`;
        iframe.style.width = '100%';
        iframe.style.height = customHeight;
        iframe.style.minHeight = '500px';
        iframe.style.border = 'none';
        iframe.style.display = 'block';
        iframe.style.overflow = 'hidden';
        iframe.setAttribute('loading', 'lazy');
        iframe.setAttribute('allow', 'geolocation');

        container.appendChild(iframe);
        mountContainer(container);
    })
    .catch(function (err) {
        console.warn('[CCOR IDX Plug-in] Bootstrap failed:', err);
        renderAuthError('Unable to load property search at this time.');
    });

    // Optional postMessage listener for auto-resizing
    window.addEventListener('message', function (e) {
        if (!e.data || e.data.type !== 'SNEAK_RESIZE') return;
        if (e.data.siteKey === siteKey && e.data.height) {
            const iframes = document.querySelectorAll(`iframe[src*="site=${siteKey}"]`);
            iframes.forEach(function (f) {
                f.style.height = `${e.data.height}px`;
            });
        }
    });

})();
