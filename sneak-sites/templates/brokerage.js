/**
 * sneak-sites/templates/brokerage.js
 * 
 * SNEAK Brokerage Template: Structured, Company-Forward, Multi-Agent & Office-Scoped Real Estate Website.
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

function renderHeader(siteBundle, basePath, activeTab = 'home') {
    const { branding } = siteBundle;

    return `
        <div class="brk-topbar">
            <div class="sneak-container brk-topbar-inner">
                <div>Serving Southwest Florida • Bonita Springs • Estero • Naples • Fort Myers</div>
                <div>Call Direct: <strong>${escapeHtml(branding.phone)}</strong></div>
            </div>
        </div>
        <header class="brk-header">
            <div class="sneak-container brk-header-inner">
                <a href="${basePath}/" class="brk-brand">
                    ${branding.logo_url ? `<img src="${escapeHtml(branding.logo_url)}" alt="${escapeHtml(branding.brokerage || branding.display_name)}" class="brk-logo" />` : ''}
                    <div class="brk-brand-text">
                        <span class="brk-brand-name">${escapeHtml(branding.brokerage || branding.display_name)}</span>
                        <span class="brk-brand-sub">Southwest Florida Real Estate Advisory</span>
                    </div>
                </a>
                <nav class="brk-nav">
                    <a href="${basePath}/" class="brk-nav-link ${activeTab === 'home' ? 'active' : ''}">Home</a>
                    <a href="${basePath}/search" class="brk-nav-link ${activeTab === 'search' ? 'active' : ''}">Search MLS</a>
                    <a href="${basePath}/open-houses" class="brk-nav-link ${activeTab === 'open-houses' ? 'active' : ''}">Open Houses</a>
                    <a href="${basePath}/about" class="brk-nav-link ${activeTab === 'about' ? 'active' : ''}">Company Profile</a>
                    <a href="${basePath}/contact" class="brk-nav-link ${activeTab === 'contact' ? 'active' : ''}">Contact Office</a>
                </nav>
                <div class="brk-cta-box">
                    <a href="${basePath}/contact" class="sneak-btn sneak-btn-primary">Office Inquiries</a>
                </div>
            </div>
        </header>
    `;
}

function renderFooter(siteBundle, basePath) {
    const { branding, websiteConfig } = siteBundle;

    return `
        <footer class="sneak-footer">
            <div class="sneak-container">
                <div class="sneak-footer-grid">
                    <div>
                        <h4 style="font-size: 1.3rem;">${escapeHtml(branding.brokerage || branding.display_name)}</h4>
                        <p style="margin-bottom: 12px; color: #94a3b8;">${escapeHtml(websiteConfig.tagline)}</p>
                        <p style="font-size: 0.85rem; color: #64748b;">Member of Bonita Springs-Estero REALTORS® &amp; Florida REALTORS®.</p>
                    </div>
                    <div>
                        <h4>Brokerage Navigation</h4>
                        <ul class="sneak-footer-nav">
                            <li><a href="${basePath}/search?type=sale">Active Properties for Sale</a></li>
                            <li><a href="${basePath}/search?type=rental">Residential Rentals</a></li>
                            <li><a href="${basePath}/search?type=commercial">Commercial Real Estate</a></li>
                            <li><a href="${basePath}/open-houses">Brokerage Open Houses</a></li>
                        </ul>
                    </div>
                    <div>
                        <h4>Headquarters &amp; Direct</h4>
                        <ul class="sneak-footer-nav">
                            <li><strong>Office Phone:</strong> ${escapeHtml(branding.phone)}</li>
                            <li><strong>Inquiries:</strong> ${escapeHtml(branding.email)}</li>
                            <li><a href="${basePath}/contact">Request Representation &rarr;</a></li>
                        </ul>
                    </div>
                </div>
                <div class="sneak-compliance-box">
                    <div>${escapeHtml(websiteConfig.footer_text)}</div>
                    <div>IDX technology powered by <strong>SNEAK</strong> • Equal Housing Opportunity</div>
                </div>
            </div>
        </footer>
    `;
}

const BROKERAGE_STYLES = `
    .brk-topbar {
        background: #0f172a;
        color: #94a3b8;
        font-size: 0.8rem;
        padding: 6px 0;
        border-bottom: 1px solid #1e293b;
    }
    .brk-topbar-inner {
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .brk-header {
        background: #ffffff;
        border-bottom: 2px solid #e2e8f0;
        position: sticky;
        top: 0;
        z-index: 100;
    }
    .brk-header-inner {
        display: flex;
        align-items: center;
        justify-content: space-between;
        height: 80px;
    }
    .brk-brand { display: flex; align-items: center; gap: 14px; }
    .brk-logo { height: 48px; width: auto; object-fit: contain; }
    .brk-brand-text { display: flex; flex-direction: column; }
    .brk-brand-name { font-size: 1.25rem; font-weight: 800; color: #0f172a; line-height: 1.2; text-transform: uppercase; letter-spacing: -0.01em; }
    .brk-brand-sub { font-size: 0.75rem; color: #64748b; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }
    .brk-nav { display: flex; align-items: center; gap: 28px; }
    .brk-nav-link { font-size: 0.9rem; font-weight: 700; color: #475569; text-transform: uppercase; letter-spacing: 0.03em; transition: color 0.15s ease; }
    .brk-nav-link:hover, .brk-nav-link.active { color: var(--brand-primary); }

    /* Hero */
    .brk-hero {
        padding: 90px 0;
        background-size: cover;
        background-position: center;
        color: #fff;
        position: relative;
    }
    .brk-hero::before {
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(135deg, rgba(15,23,42,0.92) 0%, rgba(15,23,42,0.75) 100%);
    }
    .brk-hero-content {
        position: relative;
        z-index: 1;
        max-width: 960px;
    }
    .brk-hero-tag {
        display: inline-block;
        background: rgba(255,255,255,0.1);
        border: 1px solid rgba(255,255,255,0.2);
        padding: 6px 14px;
        border-radius: var(--radius-full);
        font-size: 0.8rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 18px;
    }
    .brk-hero-heading {
        font-size: 3rem;
        font-weight: 800;
        line-height: 1.15;
        margin-bottom: 16px;
    }
    .brk-hero-sub {
        font-size: 1.2rem;
        color: #94a3b8;
        max-width: 720px;
        margin-bottom: 36px;
    }

    /* Structured Search Grid */
    .brk-search-card {
        background: #ffffff;
        border-radius: var(--radius-lg);
        padding: 24px;
        box-shadow: var(--shadow-lg);
        display: grid;
        grid-template-columns: 2fr 1fr 1fr 1fr auto;
        gap: 16px;
        align-items: flex-end;
    }
    .brk-field label { display: block; font-size: 0.75rem; font-weight: 800; text-transform: uppercase; color: #475569; margin-bottom: 6px; }
    .brk-field input, .brk-field select {
        width: 100%;
        padding: 11px 12px;
        border: 1px solid #cbd5e1;
        border-radius: var(--radius-md);
        color: #0f172a;
    }

    /* Services Grid */
    .brk-services-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 24px;
        margin-top: 40px;
    }
    .brk-service-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: var(--radius-lg);
        padding: 32px;
        box-shadow: var(--shadow-sm);
        transition: transform 0.2s ease;
    }
    .brk-service-card:hover { transform: translateY(-4px); }
    .brk-service-title { font-size: 1.25rem; font-weight: 700; color: #0f172a; margin-bottom: 10px; }
    .brk-service-desc { color: #64748b; font-size: 0.95rem; line-height: 1.6; }

    @media (max-width: 992px) {
        .brk-search-card { grid-template-columns: 1fr 1fr; }
    }
    @media (max-width: 768px) {
        .brk-topbar { display: none; }
        .brk-header-inner { flex-direction: column; height: auto; padding: 16px 0; gap: 14px; }
        .brk-nav { flex-wrap: wrap; justify-content: center; gap: 14px; }
        .brk-hero-heading { font-size: 2.2rem; }
        .brk-search-card { grid-template-columns: 1fr; }
    }
`;

export function renderBrokerageHome({ siteBundle, featuredListings = [], openHouses = [], basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;

    const listingsHtml = featuredListings.length > 0
        ? `<div class="sneak-grid-3">${featuredListings.map(p => renderPropertyCard(p, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: #64748b;">No active office inventory currently displayed.</p>`;

    const openHousesHtml = openHouses.length > 0
        ? `<div class="sneak-grid-3">${openHouses.map(oh => renderOpenHouseCard(oh, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: #64748b;">No brokerage open houses scheduled at this time.</p>`;

    const areasHtml = (websiteConfig.featured_areas || []).map(a => `
        <div class="brk-service-card" style="background-image: linear-gradient(rgba(15,23,42,0.7), rgba(15,23,42,0.85)), url('${escapeHtml(a.image_url)}'); background-size: cover; color: #fff;">
            <h3 style="font-size: 1.4rem; font-weight: 700; margin-bottom: 6px;">${escapeHtml(a.name)}</h3>
            <p style="color: #cbd5e1; font-size: 0.9rem; margin-bottom: 16px;">${escapeHtml(a.description)}</p>
            <a href="${basePath}/search?q=${encodeURIComponent(a.filter || a.name)}" class="sneak-btn sneak-btn-secondary" style="padding: 6px 14px; font-size: 0.8rem;">Explore Area &rarr;</a>
        </div>
    `).join('');

    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'home')}

        <section class="brk-hero" style="background-image: url('${escapeHtml(websiteConfig.hero_image_url)}');">
            <div class="sneak-container brk-hero-content">
                <span class="brk-hero-tag">Premier Southwest Florida Brokerage</span>
                <h1 class="brk-hero-heading">${escapeHtml(websiteConfig.hero_heading)}</h1>
                <p class="brk-hero-sub">${escapeHtml(websiteConfig.hero_subheading)}</p>

                <form class="brk-search-card" action="${basePath}/search" method="GET">
                    <div class="brk-field">
                        <label>Location / Enclave</label>
                        <input type="text" name="q" placeholder="Bonita Springs, Estero, Naples..." />
                    </div>
                    <div class="brk-field">
                        <label>Min Price</label>
                        <select name="minPrice">
                            <option value="">Any</option>
                            <option value="400000">$400,000</option>
                            <option value="750000">$750,000</option>
                            <option value="1250000">$1,250,000</option>
                            <option value="2500000">$2,500,000+</option>
                        </select>
                    </div>
                    <div class="brk-field">
                        <label>Max Price</label>
                        <select name="maxPrice">
                            <option value="">Any</option>
                            <option value="750000">$750,000</option>
                            <option value="1500000">$1,500,000</option>
                            <option value="3000000">$3,000,000</option>
                            <option value="10000000">$10,000,000+</option>
                        </select>
                    </div>
                    <div class="brk-field">
                        <label>Beds</label>
                        <select name="beds">
                            <option value="">Any</option>
                            <option value="2">2+</option>
                            <option value="3">3+</option>
                            <option value="4">4+</option>
                        </select>
                    </div>
                    <div>
                        <button type="submit" class="sneak-btn sneak-btn-primary" style="height: 44px; padding: 0 24px;">Search MLS</button>
                    </div>
                </form>
            </div>
        </section>

        <!-- Office Featured Inventory -->
        <section class="ess-section">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Featured Inventory</div>
                    <h2 class="ess-section-title">Brokerage Exclusive &amp; Office Listings</h2>
                </div>
                ${listingsHtml}
                <div style="text-align: center; margin-top: 40px;">
                    <a href="${basePath}/search" class="sneak-btn sneak-btn-secondary">Search All Active Southwest Florida MLS Listings &rarr;</a>
                </div>
            </div>
        </section>

        <!-- Markets Served -->
        <section class="ess-section ess-section-alt">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Markets Served</div>
                    <h2 class="ess-section-title">Southwest Florida Coverage</h2>
                </div>
                <div class="brk-services-grid">
                    ${areasHtml}
                </div>
            </div>
        </section>

        <!-- Brokerage Services -->
        <section class="ess-section">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Client Services</div>
                    <h2 class="ess-section-title">Comprehensive Real Estate Representation</h2>
                </div>
                <div class="brk-services-grid">
                    <div class="brk-service-card">
                        <h3 class="brk-service-title">Residential Sales &amp; Acquisition</h3>
                        <p class="brk-service-desc">Strategic buyer and seller representation across luxury golf communities, coastal condos, and single-family estates.</p>
                    </div>
                    <div class="brk-service-card">
                        <h3 class="brk-service-title">Luxury Marketing Platform</h3>
                        <p class="brk-service-desc">Targeted digital marketing, high-definition architectural media, and syndicated MLS distribution for premier properties.</p>
                    </div>
                    <div class="brk-service-card">
                        <h3 class="brk-service-title">Relocation &amp; Advisory</h3>
                        <p class="brk-service-desc">Concierge relocation services helping out-of-state and international buyers transition smoothly to Southwest Florida.</p>
                    </div>
                </div>
            </div>
        </section>

        <!-- Upcoming Open Houses -->
        <section class="ess-section ess-section-alt">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Tour Opportunities</div>
                    <h2 class="ess-section-title">Brokerage Open Houses</h2>
                </div>
                ${openHousesHtml}
            </div>
        </section>

        <!-- Office Contact CTA -->
        <section class="ess-section" style="background: #0f172a; color: #fff; text-align: center;">
            <div class="sneak-container" style="max-width: 700px;">
                <h2 style="font-size: 2.25rem; font-weight: 800; margin-bottom: 16px;">Connect with Our Brokerage Team</h2>
                <p style="color: #94a3b8; font-size: 1.1rem; margin-bottom: 32px;">${escapeHtml(websiteConfig.contact_cta_text)}</p>
                <a href="${basePath}/contact" class="sneak-btn sneak-btn-primary" style="font-size: 1rem; padding: 14px 32px;">Contact Our Office</a>
            </div>
        </section>

        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Brokerage Home',
        pagePath: '/',
        basePath,
        contentHtml,
        previewToken,
        customStyles: BROKERAGE_STYLES
    });
}

export function renderBrokerageSearch({ siteBundle, basePath = '', previewToken = null }) {
    const { site } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'search')}
        <div style="min-height: calc(100vh - 80px); background: #f8fafc; padding: 24px 0;">
            <div class="sneak-container">
                <h1 style="font-size: 1.85rem; font-weight: 800; color: #0f172a; margin-bottom: 16px;">Southwest Florida Brokerage MLS Search</h1>
                <div id="sneak-idx-root" data-site="${escapeHtml(site.site_key)}"></div>
                <script src="https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/embed.js" async></script>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'MLS Search',
        pagePath: '/search',
        basePath,
        contentHtml,
        previewToken,
        customStyles: BROKERAGE_STYLES
    });
}

export function renderBrokerageOpenHouses({ siteBundle, openHouses = [], basePath = '', previewToken = null }) {
    const openHousesHtml = openHouses.length > 0
        ? `<div class="sneak-grid-3">${openHouses.map(oh => renderOpenHouseCard(oh, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: #64748b; padding: 40px 0;">No upcoming open houses scheduled at this time.</p>`;

    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'open-houses')}
        <div class="ess-section">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Tour Opportunities</div>
                    <h1 class="ess-section-title">Brokerage Open Houses</h1>
                </div>
                ${openHousesHtml}
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Open Houses',
        pagePath: '/open-houses',
        basePath,
        contentHtml,
        previewToken,
        customStyles: BROKERAGE_STYLES
    });
}

export function renderBrokerageAbout({ siteBundle, basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'about')}
        <div class="ess-section">
            <div class="sneak-container" style="max-width: 920px;">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Company Profile</div>
                    <h1 class="ess-section-title">${escapeHtml(branding.brokerage || branding.display_name)}</h1>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 40px; align-items: center; margin-bottom: 40px;">
                    <div>
                        <p style="font-size: 1.1rem; line-height: 1.8; color: #334155; margin-bottom: 24px;">${escapeHtml(websiteConfig.about_body)}</p>
                        <div style="background: #f1f5f9; padding: 20px; border-radius: var(--radius-md);">
                            <strong>Headquarters:</strong> Southwest Florida<br />
                            <strong>Phone:</strong> ${escapeHtml(branding.phone)}<br />
                            <strong>Direct:</strong> ${escapeHtml(branding.email)}
                        </div>
                    </div>
                    <div>
                        <img src="${escapeHtml(websiteConfig.about_image_url || branding.logo_url || 'https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?auto=format&fit=crop&w=800&q=80')}" alt="${escapeHtml(branding.brokerage || branding.display_name)}" style="width: 100%; border-radius: var(--radius-lg); box-shadow: var(--shadow-md);" />
                    </div>
                </div>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Company Profile',
        pagePath: '/about',
        basePath,
        contentHtml,
        previewToken,
        customStyles: BROKERAGE_STYLES
    });
}

export function renderBrokerageContact({ siteBundle, basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'contact')}
        <div class="ess-section ess-section-alt">
            <div class="sneak-container" style="max-width: 700px;">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Contact Headquarters</div>
                    <h1 class="ess-section-title">Brokerage Inquiries</h1>
                    <p style="color: #64748b; margin-top: 8px;">${escapeHtml(websiteConfig.contact_cta_text)}</p>
                </div>
                <div class="sneak-form-card">
                    <form onsubmit="submitContactForm(this)">
                        <div style="display: none;">
                            <input type="text" name="website_hp" tabindex="-1" autocomplete="off" />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Full Name *</label>
                            <input type="text" name="name" class="sneak-form-input" placeholder="John Smith" required />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Email Address *</label>
                            <input type="email" name="email" class="sneak-form-input" placeholder="john@company.com" required />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Phone Number</label>
                            <input type="tel" name="phone" class="sneak-form-input" placeholder="(239) 555-0100" />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Inquiry Details *</label>
                            <textarea name="message" rows="4" class="sneak-form-textarea" placeholder="Describe your real estate needs or questions..." required></textarea>
                        </div>
                        <button type="submit" class="sneak-btn sneak-btn-primary" style="width: 100%;">Submit Inquiry</button>
                    </form>
                </div>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Contact Brokerage',
        pagePath: '/contact',
        basePath,
        contentHtml,
        previewToken,
        customStyles: BROKERAGE_STYLES
    });
}
