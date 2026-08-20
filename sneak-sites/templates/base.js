/**
 * sneak-sites/templates/base.js
 * 
 * Shared Base Layout, SEO Meta, Design Tokens & Common Components for SNEAK Websites.
 */

function escapeHtml(str) {
    if (typeof str !== 'string') return '';
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

export function formatPrice(price) {
    if (!price && price !== 0) return 'Price on Request';
    return '$' + Number(price).toLocaleString('en-US');
}

export function renderPropertyCard(p, basePath = '') {
    const photo = p.MediaURL || 'https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?auto=format&fit=crop&w=800&q=80';
    const address = p.UnparsedAddress || 'Address Available Upon Request';
    const cityState = `${p.City || 'Bonita Springs'}, ${p.StateOrProvince || 'FL'} ${p.PostalCode || ''}`.trim();
    const beds = p.BedroomsTotal != null ? `${p.BedroomsTotal} Beds` : '';
    const baths = p.BathroomsTotalInteger != null ? `${p.BathroomsTotalInteger} Baths` : '';
    const sqft = p.LivingArea ? `${Number(p.LivingArea).toLocaleString()} SqFt` : '';
    const specs = [beds, baths, sqft].filter(Boolean).join(' • ');

    const detailUrl = `${basePath}/search?key=${encodeURIComponent(p.ListingKey)}`;

    return `
        <article class="sneak-property-card">
            <div class="sneak-card-media">
                <img src="${escapeHtml(photo)}" alt="${escapeHtml(address)}" loading="lazy" />
                <span class="sneak-card-badge">${escapeHtml(p.PropertySubType || p.PropertyType || 'Residential')}</span>
            </div>
            <div class="sneak-card-content">
                <div class="sneak-card-price">${formatPrice(p.ListPrice)}</div>
                <h3 class="sneak-card-title">${escapeHtml(address)}</h3>
                <p class="sneak-card-city">${escapeHtml(cityState)}</p>
                <div class="sneak-card-specs">${escapeHtml(specs)}</div>
                <a href="${detailUrl}" class="sneak-card-link">View Property Details &rarr;</a>
            </div>
        </article>
    `;
}

export function renderOpenHouseCard(oh, basePath = '') {
    const photo = oh.MediaURL || 'https://images.unsplash.com/photo-1600585154340-be6161a56a0c?auto=format&fit=crop&w=800&q=80';
    const address = oh.UnparsedAddress || 'Southwest Florida Home';
    const cityState = `${oh.City || 'Bonita Springs'}, ${oh.StateOrProvince || 'FL'}`;
    const dateStr = oh.OpenHouseDate ? new Date(oh.OpenHouseDate + 'T12:00:00Z').toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }) : 'Upcoming';
    const detailUrl = `${basePath}/search?key=${encodeURIComponent(oh.ListingKey)}`;

    return `
        <article class="sneak-oh-card">
            <div class="sneak-oh-media">
                <img src="${escapeHtml(photo)}" alt="${escapeHtml(address)}" loading="lazy" />
                <div class="sneak-oh-badge">
                    <span class="sneak-oh-date">${escapeHtml(dateStr)}</span>
                </div>
            </div>
            <div class="sneak-oh-content">
                <div class="sneak-card-price">${formatPrice(oh.ListPrice)}</div>
                <h3 class="sneak-card-title">${escapeHtml(address)}</h3>
                <p class="sneak-card-city">${escapeHtml(cityState)}</p>
                <a href="${detailUrl}" class="sneak-card-link">View Open House &rarr;</a>
            </div>
        </article>
    `;
}

export function renderBaseLayout({
    siteBundle,
    pageTitle,
    pagePath = '/',
    basePath = '',
    contentHtml = '',
    previewToken = null,
    extraHead = '',
    customStyles = ''
}) {
    const { branding, websiteConfig, site } = siteBundle;
    const isPreview = Boolean(previewToken);
    const siteTitle = pageTitle ? `${pageTitle} | ${websiteConfig.site_title}` : websiteConfig.site_title;
    const seoDesc = websiteConfig.seo_description;
    const primaryColor = branding.primary_color || '#1e3a8a';
    const secondaryColor = branding.secondary_color || '#0284c7';

    const robotsMeta = isPreview
        ? '<meta name="robots" content="noindex, nofollow" />'
        : '<meta name="robots" content="index, follow" />';

    const previewBanner = isPreview ? `
        <div class="sneak-preview-banner">
            <span><strong>PREVIEW MODE</strong> — Template: <em>${escapeHtml(websiteConfig.template_key.toUpperCase())}</em> | Site: <code>${escapeHtml(site.site_key)}</code></span>
        </div>
    ` : '';

    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${escapeHtml(siteTitle)}</title>
    <meta name="description" content="${escapeHtml(seoDesc)}" />
    ${robotsMeta}
    
    <!-- Open Graph -->
    <meta property="og:title" content="${escapeHtml(siteTitle)}" />
    <meta property="og:description" content="${escapeHtml(seoDesc)}" />
    <meta property="og:type" content="website" />
    ${branding.agent_photo_url || websiteConfig.hero_image_url ? `<meta property="og:image" content="${escapeHtml(branding.agent_photo_url || websiteConfig.hero_image_url)}" />` : ''}

    <!-- Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Playfair+Display:ital,wght@0,500;0,700;1,400&family=Plus+Jakarta+Sans:wght@500;600;700;800&display=swap" rel="stylesheet" />

    <!-- Core Design Tokens -->
    <style>
        :root {
            --brand-primary: ${escapeHtml(primaryColor)};
            --brand-secondary: ${escapeHtml(secondaryColor)};
            --brand-bg: #0f172a;
            --surface-bg: #ffffff;
            --surface-subtle: #f8fafc;
            --surface-elevated: #ffffff;
            --text-main: #0f172a;
            --text-muted: #64748b;
            --text-light: #ffffff;
            --border-color: #e2e8f0;
            --radius-sm: 6px;
            --radius-md: 10px;
            --radius-lg: 16px;
            --radius-full: 9999px;
            --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
            --shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -4px rgba(0, 0, 0, 0.1);
            --container-max: 1240px;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', system-ui, -apple-system, sans-serif;
            background: var(--surface-bg);
            color: var(--text-main);
            line-height: 1.6;
            -webkit-font-smoothing: antialiased;
        }

        a { color: inherit; text-decoration: none; }
        img { max-width: 100%; height: auto; display: block; }
        button, input, textarea, select { font: inherit; }

        .sneak-container {
            max-width: var(--container-max);
            margin: 0 auto;
            padding: 0 24px;
        }

        .sneak-preview-banner {
            background: #f59e0b;
            color: #78350f;
            text-align: center;
            padding: 8px 16px;
            font-size: 0.85rem;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 1000;
            border-bottom: 1px solid #d97706;
        }

        /* Buttons */
        .sneak-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            padding: 12px 24px;
            border-radius: var(--radius-md);
            font-weight: 600;
            font-size: 0.95rem;
            cursor: pointer;
            transition: all 0.2s ease;
            border: 1px solid transparent;
        }
        .sneak-btn-primary { background: var(--brand-primary); color: #fff; }
        .sneak-btn-primary:hover { opacity: 0.92; transform: translateY(-1px); }
        .sneak-btn-secondary { background: #fff; color: var(--text-main); border-color: var(--border-color); }
        .sneak-btn-secondary:hover { background: var(--surface-subtle); }

        /* Property Card Components */
        .sneak-grid-3 {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
            gap: 28px;
        }
        .sneak-property-card, .sneak-oh-card {
            background: var(--surface-bg);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-lg);
            overflow: hidden;
            box-shadow: var(--shadow-sm);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            display: flex;
            flex-direction: column;
        }
        .sneak-property-card:hover, .sneak-oh-card:hover {
            transform: translateY(-4px);
            box-shadow: var(--shadow-lg);
        }
        .sneak-card-media, .sneak-oh-media {
            height: 220px;
            position: relative;
            background: #e2e8f0;
        }
        .sneak-card-media img, .sneak-oh-media img {
            width: 100%;
            height: 100%;
            object-fit: cover;
        }
        .sneak-card-badge {
            position: absolute;
            top: 12px;
            left: 12px;
            background: rgba(15, 23, 42, 0.75);
            color: #fff;
            padding: 4px 10px;
            border-radius: var(--radius-full);
            font-size: 0.75rem;
            font-weight: 600;
            backdrop-filter: blur(4px);
        }
        .sneak-oh-badge {
            position: absolute;
            bottom: 12px;
            left: 12px;
            background: var(--brand-primary);
            color: #fff;
            padding: 6px 12px;
            border-radius: var(--radius-md);
            font-size: 0.8rem;
            font-weight: 700;
        }
        .sneak-card-content, .sneak-oh-content {
            padding: 20px;
            display: flex;
            flex-direction: column;
            flex: 1;
        }
        .sneak-card-price {
            font-size: 1.35rem;
            font-weight: 800;
            color: var(--brand-primary);
            margin-bottom: 6px;
        }
        .sneak-card-title {
            font-size: 1.05rem;
            font-weight: 600;
            line-height: 1.3;
            margin-bottom: 4px;
        }
        .sneak-card-city {
            color: var(--text-muted);
            font-size: 0.875rem;
            margin-bottom: 12px;
        }
        .sneak-card-specs {
            color: var(--text-main);
            font-size: 0.875rem;
            font-weight: 500;
            padding-top: 12px;
            margin-top: auto;
            border-top: 1px solid var(--border-color);
            margin-bottom: 12px;
        }
        .sneak-card-link {
            color: var(--brand-primary);
            font-weight: 600;
            font-size: 0.875rem;
            display: inline-flex;
            align-items: center;
        }

        /* Lead Contact Form */
        .sneak-form-card {
            background: var(--surface-bg);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-lg);
            padding: 32px;
            box-shadow: var(--shadow-md);
        }
        .sneak-form-group {
            margin-bottom: 18px;
            text-align: left;
        }
        .sneak-form-label {
            display: block;
            font-weight: 600;
            font-size: 0.875rem;
            margin-bottom: 6px;
            color: var(--text-main);
        }
        .sneak-form-input, .sneak-form-textarea {
            width: 100%;
            padding: 12px 14px;
            border: 1px solid var(--border-color);
            border-radius: var(--radius-md);
            font-size: 0.95rem;
            transition: border-color 0.15s ease;
        }
        .sneak-form-input:focus, .sneak-form-textarea:focus {
            outline: none;
            border-color: var(--brand-primary);
        }

        /* Footer & Compliance */
        .sneak-footer {
            background: #090d16;
            color: #94a3b8;
            padding: 60px 0 30px 0;
            border-top: 1px solid #1e293b;
            font-size: 0.9rem;
        }
        .sneak-footer-grid {
            display: grid;
            grid-template-columns: 2fr 1fr 1fr;
            gap: 40px;
            margin-bottom: 40px;
        }
        .sneak-footer h4 {
            color: #fff;
            margin-bottom: 16px;
            font-size: 1.05rem;
        }
        .sneak-footer-nav {
            list-style: none;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        .sneak-footer-nav a:hover { color: #fff; }
        .sneak-compliance-box {
            border-top: 1px solid #1e293b;
            padding-top: 24px;
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            font-size: 0.8rem;
            color: #64748b;
        }

        @media (max-width: 768px) {
            .sneak-grid-3 { grid-template-columns: 1fr; }
            .sneak-footer-grid { grid-template-columns: 1fr; gap: 30px; }
        }
    </style>
    ${customStyles ? `<style>${customStyles}</style>` : ''}
    ${extraHead}
</head>
<body>
    ${previewBanner}
    <div id="sneak-site-root">
        ${contentHtml}
    </div>

    <script>
        async function submitContactForm(formElem) {
            event.preventDefault();
            const btn = formElem.querySelector('button[type="submit"]');
            const originalText = btn.innerText;
            btn.disabled = true;
            btn.innerText = 'Sending...';

            const formData = new FormData(formElem);
            const body = {};
            formData.forEach((v, k) => { body[k] = v; });

            try {
                const res = await fetch('${basePath}/api/contact', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                const data = await res.json();
                if (res.ok) {
                    formElem.innerHTML = '<div style="padding: 24px; background: rgba(16, 185, 129, 0.1); color: #059669; border-radius: 8px; text-align: center;"><h3>Thank You!</h3><p style="margin-top: 8px;">' + (data.message || 'Your inquiry has been received.') + '</p></div>';
                } else {
                    alert(data.message || 'Could not submit inquiry. Please check fields and try again.');
                    btn.disabled = false;
                    btn.innerText = originalText;
                }
            } catch {
                alert('Network error. Please try again.');
                btn.disabled = false;
                btn.innerText = originalText;
            }
        }
    </script>
</body>
</html>`;
}
