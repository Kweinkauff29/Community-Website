/**
 * SNEAK IDX Embed Loader (embed.js)
 * 
 * Lightweight, zero-dependency widget embed loader for the SNEAK IDX Platform.
 * Embeds configurable, isolated IDX widgets (search, listing grid, open houses) into any 3rd-party website.
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

    // Read configuration attributes
    const siteKey = currentScript.getAttribute('data-site') || currentScript.getAttribute('data-site-key');
    const widgetType = currentScript.getAttribute('data-widget') || 'search';
    const customHeight = currentScript.getAttribute('data-height') || '850px';
    const targetSelector = currentScript.getAttribute('data-target');
    const customParams = currentScript.getAttribute('data-params') || '';

    if (!siteKey) {
        console.error('[SNEAK IDX] Missing required "data-site" attribute on embed script.');
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

    // Determine target widget path
    let widgetRoot = isSubdirectory ? '/sneak-idx/search/' : '/search/';
    let widgetPath = widgetRoot;
    if (widgetType === 'open_houses' || widgetType === 'open-houses') {
        widgetPath = `${widgetRoot}?type=open-houses&`;
    }

    // Construct safe iframe URL
    const separator = widgetPath.includes('?') ? '&' : '?';
    let iframeUrl = `${baseUrl}${widgetPath}${separator}site=${encodeURIComponent(siteKey)}&embed=true`;
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
    iframe.title = `SNEAK IDX Real Estate Search (${siteKey})`;
    iframe.style.width = '100%';
    iframe.style.height = customHeight;
    iframe.style.minHeight = '500px';
    iframe.style.border = 'none';
    iframe.style.display = 'block';
    iframe.style.overflow = 'hidden';
    iframe.setAttribute('loading', 'lazy');
    iframe.setAttribute('allow', 'geolocation');

    container.appendChild(iframe);

    // Insert into DOM
    if (targetSelector) {
        const targetEl = document.querySelector(targetSelector);
        if (targetEl) {
            targetEl.appendChild(container);
        } else {
            currentScript.parentNode.insertBefore(container, currentScript.nextSibling);
        }
    } else {
        currentScript.parentNode.insertBefore(container, currentScript.nextSibling);
    }

    // Optional postMessage listener for future auto-resizing
    window.addEventListener('message', function (e) {
        if (!e.data || e.data.type !== 'SNEAK_RESIZE') return;
        if (e.data.siteKey === siteKey && e.data.height) {
            iframe.style.height = `${e.data.height}px`;
        }
    });

})();
