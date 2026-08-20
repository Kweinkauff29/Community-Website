/**
 * sneak-sites/templates/coastal.js
 * 
 * SNEAK Coastal Template: Editorial, Image-Forward, Luxury Lifestyle SWFL Real Estate Website.
 */

import {
    renderBaseLayout,
    renderPropertyCard,
    renderOpenHouseCard,
    formatPrice
} from './base.js';

function escapeHtml(str) {
    if (typeof str !== 'string') return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
}

function renderHeader(siteBundle, basePath, activeTab = 'home', isTransparent = false) {
    const { branding } = siteBundle;

    return `
        <header class="cst-header ${isTransparent ? 'cst-header-trans' : 'cst-header-solid'}">
            <div class="sneak-container cst-header-inner">
                <a href="${basePath}/" class="cst-brand">
                    ${branding.logo_url ? `<img src="${escapeHtml(branding.logo_url)}" alt="${escapeHtml(branding.display_name)}" class="cst-logo" />` : ''}
                    <div class="cst-brand-text">
                        <span class="cst-brand-name">${escapeHtml(branding.display_name)}</span>
                        <span class="cst-brand-sub">${escapeHtml(branding.brokerage)}</span>
                    </div>
                </a>
                <nav class="cst-nav">
                    <a href="${basePath}/" class="cst-nav-link ${activeTab === 'home' ? 'active' : ''}">Home</a>
                    <a href="${basePath}/search" class="cst-nav-link ${activeTab === 'search' ? 'active' : ''}">Properties</a>
                    <a href="${basePath}/open-houses" class="cst-nav-link ${activeTab === 'open-houses' ? 'active' : ''}">Open Houses</a>
                    <a href="${basePath}/about" class="cst-nav-link ${activeTab === 'about' ? 'active' : ''}">Story</a>
                    <a href="${basePath}/contact" class="cst-nav-link ${activeTab === 'contact' ? 'active' : ''}">Inquiries</a>
                </nav>
                <div>
                    <a href="${basePath}/contact" class="cst-cta-btn">Private Consultation</a>
                </div>
            </div>
        </header>
    `;
}

function renderFooter(siteBundle, basePath) {
    const { branding, websiteConfig } = siteBundle;

    return `
        <footer class="cst-footer">
            <div class="sneak-container">
                <div class="cst-footer-grid">
                    <div>
                        <div class="cst-footer-brand">${escapeHtml(branding.display_name)}</div>
                        <div style="font-size: 0.85rem; color: #a1a1aa; letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 16px;">${escapeHtml(branding.brokerage)}</div>
                        <p style="color: #71717a; font-size: 0.9rem; max-width: 360px;">${escapeHtml(websiteConfig.tagline)}</p>
                    </div>
                    <div>
                        <h4 class="cst-footer-heading">Southwest Florida Portfolio</h4>
                        <ul class="cst-footer-list">
                            <li><a href="${basePath}/search?type=sale">Waterfront &amp; Luxury Estates</a></li>
                            <li><a href="${basePath}/search?type=rental">Executive Leases</a></li>
                            <li><a href="${basePath}/open-houses">Curated Open Houses</a></li>
                        </ul>
                    </div>
                    <div>
                        <h4 class="cst-footer-heading">Advisory &amp; Contact</h4>
                        <ul class="cst-footer-list">
                            <li>${escapeHtml(branding.phone)}</li>
                            <li>${escapeHtml(branding.email)}</li>
                            <li><a href="${basePath}/contact" style="color: #e4e4e7;">Schedule a Showing &rarr;</a></li>
                        </ul>
                    </div>
                </div>
                <div class="cst-footer-bottom">
                    <div>${escapeHtml(websiteConfig.footer_text)}</div>
                    <div>IDX Powered by <strong>SNEAK</strong> • Equal Housing Opportunity</div>
                </div>
            </div>
        </footer>
    `;
}

const COASTAL_STYLES = `
    .cst-header {
        position: sticky;
        top: 0;
        z-index: 100;
        transition: background 0.3s ease;
    }
    .cst-header-solid {
        background: #09090b;
        color: #fff;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .cst-header-trans {
        background: #09090b;
        color: #fff;
        border-bottom: 1px solid rgba(255,255,255,0.08);
    }
    .cst-header-inner {
        display: flex;
        align-items: center;
        justify-content: space-between;
        height: 84px;
    }
    .cst-brand { display: flex; align-items: center; gap: 14px; }
    .cst-logo { height: 48px; width: auto; object-fit: contain; }
    .cst-brand-name { font-family: 'Playfair Display', serif; font-size: 1.4rem; font-weight: 700; letter-spacing: 0.02em; }
    .cst-brand-sub { font-size: 0.7rem; letter-spacing: 0.15em; text-transform: uppercase; color: #a1a1aa; }
    .cst-nav { display: flex; align-items: center; gap: 32px; }
    .cst-nav-link { font-size: 0.85rem; font-weight: 500; letter-spacing: 0.08em; text-transform: uppercase; color: #a1a1aa; transition: color 0.2s ease; }
    .cst-nav-link:hover, .cst-nav-link.active { color: #ffffff; border-bottom: 1px solid #ffffff; }
    .cst-cta-btn {
        padding: 10px 20px;
        border: 1px solid rgba(255,255,255,0.3);
        border-radius: var(--radius-sm);
        font-size: 0.8rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #fff;
        transition: all 0.2s ease;
    }
    .cst-cta-btn:hover { background: #fff; color: #09090b; }

    /* Hero */
    .cst-hero {
        min-height: 80vh;
        display: flex;
        align-items: center;
        position: relative;
        background-size: cover;
        background-position: center;
        color: #fff;
        padding: 100px 0;
    }
    .cst-hero::before {
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(180deg, rgba(9,9,11,0.5) 0%, rgba(9,9,11,0.85) 100%);
    }
    .cst-hero-content {
        position: relative;
        z-index: 1;
        max-width: 900px;
        margin: 0 auto;
        text-align: center;
    }
    .cst-hero-tag {
        font-size: 0.85rem;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        color: #93c5fd;
        margin-bottom: 18px;
    }
    .cst-hero-heading {
        font-family: 'Playfair Display', serif;
        font-size: 3.5rem;
        font-weight: 700;
        line-height: 1.15;
        margin-bottom: 20px;
    }
    .cst-hero-sub {
        font-size: 1.2rem;
        color: #d4d4d8;
        max-width: 680px;
        margin: 0 auto 40px auto;
        font-weight: 300;
    }

    /* Luxury Search Glass Box */
    .cst-search-box {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(12px);
        border-radius: var(--radius-md);
        padding: 24px;
        box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
        display: flex;
        flex-wrap: wrap;
        gap: 16px;
        color: #09090b;
    }
    .cst-search-field { flex: 1; min-width: 170px; text-align: left; }
    .cst-search-field label { display: block; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; color: #71717a; margin-bottom: 6px; }
    .cst-search-field input, .cst-search-field select {
        width: 100%;
        padding: 12px 14px;
        border: 1px solid #e4e4e7;
        border-radius: var(--radius-sm);
        background: #fafafa;
    }

    /* Editorial Sections */
    .cst-section { padding: 90px 0; }
    .cst-section-dark { background: #09090b; color: #fff; }
    .cst-section-header {
        text-align: center;
        max-width: 640px;
        margin: 0 auto 56px auto;
    }
    .cst-section-tag {
        font-size: 0.8rem;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        color: var(--brand-secondary);
        margin-bottom: 10px;
    }
    .cst-section-title {
        font-family: 'Playfair Display', serif;
        font-size: 2.6rem;
        font-weight: 700;
    }

    /* Editorial Area Cards */
    .cst-area-card {
        height: 380px;
        border-radius: var(--radius-md);
        position: relative;
        overflow: hidden;
        display: flex;
        flex-direction: column;
        justify-content: flex-end;
        padding: 32px;
        color: #fff;
        background-size: cover;
        background-position: center;
        transition: transform 0.3s ease;
    }
    .cst-area-card:hover { transform: scale(1.02); }
    .cst-area-card::before {
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(180deg, transparent 40%, rgba(9,9,11,0.9) 100%);
    }
    .cst-area-inner { position: relative; z-index: 1; }
    .cst-area-title { font-family: 'Playfair Display', serif; font-size: 1.85rem; font-weight: 700; margin-bottom: 6px; }

    /* Footer */
    .cst-footer { background: #09090b; color: #a1a1aa; padding: 80px 0 30px 0; border-top: 1px solid #27272a; }
    .cst-footer-grid { display: grid; grid-template-columns: 2fr 1fr 1fr; gap: 48px; margin-bottom: 48px; }
    .cst-footer-brand { font-family: 'Playfair Display', serif; font-size: 1.5rem; color: #fff; margin-bottom: 4px; }
    .cst-footer-heading { color: #fff; font-size: 0.85rem; letter-spacing: 0.15em; text-transform: uppercase; margin-bottom: 18px; }
    .cst-footer-list { list-style: none; display: flex; flex-direction: column; gap: 12px; font-size: 0.9rem; }
    .cst-footer-list a:hover { color: #fff; }
    .cst-footer-bottom { border-top: 1px solid #27272a; padding-top: 24px; display: flex; justify-content: space-between; font-size: 0.8rem; }

    @media (max-width: 768px) {
        .cst-hero-heading { font-size: 2.3rem; }
        .cst-header-inner { flex-direction: column; height: auto; padding: 16px 0; gap: 14px; }
        .cst-nav { flex-wrap: wrap; justify-content: center; gap: 16px; }
        .cst-footer-grid { grid-template-columns: 1fr; }
    }
`;

export function renderCoastalHome({ siteBundle, featuredListings = [], openHouses = [], basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;

    const listingsHtml = featuredListings.length > 0
        ? `<div class="sneak-grid-3">${featuredListings.map(p => renderPropertyCard(p, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: #71717a;">Curated private portfolio updating shortly.</p>`;

    const openHousesHtml = openHouses.length > 0
        ? `<div class="sneak-grid-3">${openHouses.map(oh => renderOpenHouseCard(oh, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: #71717a;">No private open houses scheduled at this time.</p>`;

    const areasHtml = (websiteConfig.featured_areas || []).map(a => `
        <a href="${basePath}/search?q=${encodeURIComponent(a.filter || a.name)}" class="cst-area-card" style="background-image: url('${escapeHtml(a.image_url)}');">
            <div class="cst-area-inner">
                <h3 class="cst-area-title">${escapeHtml(a.name)}</h3>
                <p style="font-size: 0.9rem; color: #d4d4d8;">${escapeHtml(a.description)}</p>
            </div>
        </a>
    `).join('');

    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'home', true)}

        <section class="cst-hero" style="background-image: url('${escapeHtml(websiteConfig.hero_image_url)}');">
            <div class="sneak-container cst-hero-content">
                <div class="cst-hero-tag">Southwest Florida Coastal Living</div>
                <h1 class="cst-hero-heading">${escapeHtml(websiteConfig.hero_heading)}</h1>
                <p class="cst-hero-sub">${escapeHtml(websiteConfig.hero_subheading)}</p>

                <form class="cst-search-box" action="${basePath}/search" method="GET">
                    <div class="cst-search-field" style="flex: 2;">
                        <label>Location / Enclave</label>
                        <input type="text" name="q" placeholder="Naples, Bonita Bay, Pelican Landing..." />
                    </div>
                    <div class="cst-search-field">
                        <label>Price Range</label>
                        <select name="minPrice">
                            <option value="">Any Minimum</option>
                            <option value="750000">$750,000</option>
                            <option value="1500000">$1,500,000</option>
                            <option value="3000000">$3,000,000</option>
                            <option value="5000000">$5,000,000+</option>
                        </select>
                    </div>
                    <div class="cst-search-field">
                        <label>Bedrooms</label>
                        <select name="beds">
                            <option value="">Any Beds</option>
                            <option value="3">3+ Bedrooms</option>
                            <option value="4">4+ Bedrooms</option>
                            <option value="5">5+ Bedrooms</option>
                        </select>
                    </div>
                    <div style="display: flex; align-items: flex-end;">
                        <button type="submit" class="sneak-btn sneak-btn-primary" style="height: 44px; padding: 0 28px;">Search Portfolio</button>
                    </div>
                </form>
            </div>
        </section>

        <!-- Curated Properties -->
        <section class="cst-section">
            <div class="sneak-container">
                <div class="cst-section-header">
                    <div class="cst-section-tag">Featured Portfolio</div>
                    <h2 class="cst-section-title">Selected Coastal Residences</h2>
                </div>
                ${listingsHtml}
                <div style="text-align: center; margin-top: 50px;">
                    <a href="${basePath}/search" class="cst-cta-btn" style="color: #09090b; border-color: #09090b; padding: 14px 28px;">Explore All MLS Properties &rarr;</a>
                </div>
            </div>
        </section>

        <!-- Coastal Communities -->
        <section class="cst-section cst-section-dark">
            <div class="sneak-container">
                <div class="cst-section-header">
                    <div class="cst-section-tag">Enclaves &amp; Communities</div>
                    <h2 class="cst-section-title" style="color: #fff;">Featured Destinations</h2>
                </div>
                <div class="sneak-grid-3">
                    ${areasHtml}
                </div>
            </div>
        </section>

        <!-- Agent Story -->
        <section class="cst-section">
            <div class="sneak-container" style="max-width: 1080px;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 60px; align-items: center;">
                    <img src="${escapeHtml(websiteConfig.about_image_url || branding.agent_photo_url || 'https://images.unsplash.com/photo-1573496359142-b8d87734a5a2?auto=format&fit=crop&w=800&q=80')}" alt="${escapeHtml(branding.display_name)}" style="width: 100%; height: 480px; object-fit: cover; border-radius: var(--radius-md);" />
                    <div>
                        <div class="cst-section-tag">Private Advisory</div>
                        <h2 style="font-family: 'Playfair Display', serif; font-size: 2.6rem; font-weight: 700; margin-bottom: 20px;">${escapeHtml(branding.display_name)}</h2>
                        <p style="color: #71717a; font-size: 1.1rem; line-height: 1.8; margin-bottom: 28px;">${escapeHtml(websiteConfig.about_body)}</p>
                        <a href="${basePath}/about" class="cst-cta-btn" style="color: #09090b; border-color: #09090b;">Learn More About Advisory &rarr;</a>
                    </div>
                </div>
            </div>
        </section>

        <!-- Open Houses -->
        <section class="cst-section" style="background: #fafafa; border-top: 1px solid #e4e4e7;">
            <div class="sneak-container">
                <div class="cst-section-header">
                    <div class="cst-section-tag">Private Viewings</div>
                    <h2 class="cst-section-title">Upcoming Open Houses</h2>
                </div>
                ${openHousesHtml}
            </div>
        </section>

        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Luxury Coastal Real Estate',
        pagePath: '/',
        basePath,
        contentHtml,
        previewToken,
        customStyles: COASTAL_STYLES
    });
}

export function renderCoastalSearch({ siteBundle, basePath = '', previewToken = null }) {
    const { site } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'search', false)}
        <div style="min-height: calc(100vh - 84px); background: #09090b; padding: 32px 0;">
            <div class="sneak-container">
                <h1 style="font-family: 'Playfair Display', serif; font-size: 2.2rem; color: #fff; margin-bottom: 20px;">Southwest Florida Real Estate Portfolio</h1>
                <div id="sneak-idx-root" data-site="${escapeHtml(site.site_key)}"></div>
                <script src="https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/embed.js" async></script>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Properties Portfolio',
        pagePath: '/search',
        basePath,
        contentHtml,
        previewToken,
        customStyles: COASTAL_STYLES
    });
}

export function renderCoastalOpenHouses({ siteBundle, openHouses = [], basePath = '', previewToken = null }) {
    const openHousesHtml = openHouses.length > 0
        ? `<div class="sneak-grid-3">${openHouses.map(oh => renderOpenHouseCard(oh, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: #71717a; padding: 60px 0;">No private open houses currently scheduled.</p>`;

    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'open-houses', false)}
        <div class="cst-section">
            <div class="sneak-container">
                <div class="cst-section-header">
                    <div class="cst-section-tag">Private Viewings</div>
                    <h1 class="cst-section-title">Upcoming Open Houses</h1>
                </div>
                ${openHousesHtml}
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Upcoming Open Houses',
        pagePath: '/open-houses',
        basePath,
        contentHtml,
        previewToken,
        customStyles: COASTAL_STYLES
    });
}

export function renderCoastalAbout({ siteBundle, basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'about', false)}
        <div class="cst-section">
            <div class="sneak-container" style="max-width: 960px;">
                <div style="text-align: center; margin-bottom: 48px;">
                    <img src="${escapeHtml(websiteConfig.about_image_url || branding.agent_photo_url || 'https://images.unsplash.com/photo-1573496359142-b8d87734a5a2?auto=format&fit=crop&w=800&q=80')}" alt="${escapeHtml(branding.display_name)}" style="width: 220px; height: 220px; border-radius: var(--radius-full); object-fit: cover; margin: 0 auto 24px auto;" />
                    <div class="cst-section-tag">Private Advisory</div>
                    <h1 style="font-family: 'Playfair Display', serif; font-size: 2.8rem; font-weight: 700;">${escapeHtml(branding.display_name)}</h1>
                    <p style="color: #71717a; font-size: 1.1rem; margin-top: 6px;">${escapeHtml(branding.brokerage)}</p>
                </div>
                <div style="font-size: 1.15rem; line-height: 1.9; color: #27272a; max-width: 780px; margin: 0 auto 48px auto;">
                    <p>${escapeHtml(websiteConfig.about_body)}</p>
                </div>
                <div style="text-align: center;">
                    <a href="${basePath}/contact" class="cst-cta-btn" style="color: #09090b; border-color: #09090b; padding: 14px 32px;">Inquire for Representation &rarr;</a>
                </div>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: `Story & Advisory - ${branding.display_name}`,
        pagePath: '/about',
        basePath,
        contentHtml,
        previewToken,
        customStyles: COASTAL_STYLES
    });
}

export function renderCoastalContact({ siteBundle, basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'contact', false)}
        <div class="cst-section" style="background: #fafafa;">
            <div class="sneak-container" style="max-width: 640px;">
                <div class="cst-section-header">
                    <div class="cst-section-tag">Inquiries</div>
                    <h1 class="cst-section-title">Contact Advisory</h1>
                    <p style="color: #71717a; margin-top: 10px;">${escapeHtml(websiteConfig.contact_cta_text)}</p>
                </div>
                <div class="sneak-form-card" style="box-shadow: 0 20px 25px -5px rgba(0,0,0,0.05);">
                    <form onsubmit="submitContactForm(this)">
                        <div style="display: none;">
                            <input type="text" name="website_hp" tabindex="-1" autocomplete="off" />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Full Name *</label>
                            <input type="text" name="name" class="sneak-form-input" placeholder="Alexander Hamilton" required />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Email Address *</label>
                            <input type="email" name="email" class="sneak-form-input" placeholder="alexander@example.com" required />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Telephone</label>
                            <input type="tel" name="phone" class="sneak-form-input" placeholder="(239) 555-0100" />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Message / Property of Interest *</label>
                            <textarea name="message" rows="4" class="sneak-form-textarea" placeholder="I am inquiring regarding waterfront residences in Bonita Springs..." required></textarea>
                        </div>
                        <button type="submit" class="sneak-btn sneak-btn-primary" style="width: 100%; border-radius: var(--radius-sm);">Submit Confidential Inquiry</button>
                    </form>
                </div>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: `Contact ${branding.display_name}`,
        pagePath: '/contact',
        basePath,
        contentHtml,
        previewToken,
        customStyles: COASTAL_STYLES
    });
}
