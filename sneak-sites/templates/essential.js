/**
 * sneak-sites/templates/essential.js
 * 
 * SNEAK Essential Template: Bright, Clean, Modern, Conversion-Focused Agent Website.
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
    const { branding, websiteConfig } = siteBundle;
    const displayName = branding.display_name;
    const brokerage = branding.brokerage;

    return `
        <header class="ess-header">
            <div class="sneak-container ess-header-inner">
                <a href="${basePath}/" class="ess-brand">
                    ${branding.logo_url ? `<img src="${escapeHtml(branding.logo_url)}" alt="${escapeHtml(displayName)}" class="ess-logo" />` : ''}
                    <div class="ess-brand-text">
                        <span class="ess-brand-name">${escapeHtml(displayName)}</span>
                        <span class="ess-brand-sub">${escapeHtml(brokerage)}</span>
                    </div>
                </a>
                <nav class="ess-nav">
                    <a href="${basePath}/" class="ess-nav-link ${activeTab === 'home' ? 'active' : ''}">Home</a>
                    <a href="${basePath}/search" class="ess-nav-link ${activeTab === 'search' ? 'active' : ''}">Property Search</a>
                    <a href="${basePath}/open-houses" class="ess-nav-link ${activeTab === 'open-houses' ? 'active' : ''}">Open Houses</a>
                    <a href="${basePath}/about" class="ess-nav-link ${activeTab === 'about' ? 'active' : ''}">About</a>
                    <a href="${basePath}/contact" class="ess-nav-link ${activeTab === 'contact' ? 'active' : ''}">Contact</a>
                </nav>
                <div class="ess-header-cta">
                    <a href="${basePath}/contact" class="sneak-btn sneak-btn-primary">Schedule Showing</a>
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
                        <h4 style="font-size: 1.25rem;">${escapeHtml(branding.display_name)}</h4>
                        <p style="margin-bottom: 12px;">${escapeHtml(branding.brokerage)}</p>
                        <p style="color: #64748b; font-size: 0.85rem; max-width: 380px;">${escapeHtml(websiteConfig.tagline)}</p>
                    </div>
                    <div>
                        <h4>Explore Properties</h4>
                        <ul class="sneak-footer-nav">
                            <li><a href="${basePath}/search?type=sale">Homes for Sale</a></li>
                            <li><a href="${basePath}/search?type=rental">Annual Rentals</a></li>
                            <li><a href="${basePath}/search?type=commercial">Commercial Properties</a></li>
                            <li><a href="${basePath}/open-houses">Upcoming Open Houses</a></li>
                        </ul>
                    </div>
                    <div>
                        <h4>Get In Touch</h4>
                        <ul class="sneak-footer-nav">
                            <li><strong>Phone:</strong> ${escapeHtml(branding.phone)}</li>
                            <li><strong>Email:</strong> ${escapeHtml(branding.email)}</li>
                            <li><a href="${basePath}/contact">Send a Message &rarr;</a></li>
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

const ESSENTIAL_STYLES = `
    .ess-header {
        background: #ffffff;
        border-bottom: 1px solid var(--border-color);
        position: sticky;
        top: 0;
        z-index: 100;
    }
    .ess-header-inner {
        display: flex;
        align-items: center;
        justify-content: space-between;
        height: 76px;
    }
    .ess-brand {
        display: flex;
        align-items: center;
        gap: 12px;
    }
    .ess-logo { height: 44px; width: auto; object-fit: contain; }
    .ess-brand-text { display: flex; flex-direction: column; }
    .ess-brand-name { font-weight: 700; font-size: 1.15rem; color: var(--text-main); line-height: 1.2; }
    .ess-brand-sub { font-size: 0.75rem; color: var(--text-muted); font-weight: 500; }
    .ess-nav { display: flex; align-items: center; gap: 24px; }
    .ess-nav-link { font-weight: 600; font-size: 0.9rem; color: var(--text-muted); transition: color 0.15s ease; }
    .ess-nav-link:hover, .ess-nav-link.active { color: var(--brand-primary); }

    /* Hero Section */
    .ess-hero {
        position: relative;
        padding: 90px 0;
        background-size: cover;
        background-position: center;
        color: #ffffff;
        text-align: center;
    }
    .ess-hero::before {
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(rgba(15, 23, 42, 0.65), rgba(15, 23, 42, 0.75));
    }
    .ess-hero-content {
        position: relative;
        z-index: 1;
        max-width: 840px;
        margin: 0 auto;
    }
    .ess-hero-heading {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 2.75rem;
        font-weight: 800;
        line-height: 1.2;
        margin-bottom: 14px;
        letter-spacing: -0.02em;
    }
    .ess-hero-sub {
        font-size: 1.15rem;
        color: #cbd5e1;
        margin-bottom: 36px;
    }

    /* Hero Search Bar */
    .ess-hero-search {
        background: #ffffff;
        padding: 16px;
        border-radius: var(--radius-lg);
        box-shadow: 0 20px 25px -5px rgba(0,0,0,0.3);
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: center;
    }
    .ess-search-field {
        flex: 1;
        min-width: 160px;
        text-align: left;
    }
    .ess-search-field label {
        display: block;
        font-size: 0.75rem;
        font-weight: 700;
        color: var(--text-muted);
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .ess-search-field input, .ess-search-field select {
        width: 100%;
        padding: 10px 12px;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-sm);
        color: var(--text-main);
    }

    /* Sections */
    .ess-section { padding: 70px 0; }
    .ess-section-alt { background: var(--surface-subtle); }
    .ess-section-header {
        text-align: center;
        max-width: 600px;
        margin: 0 auto 48px auto;
    }
    .ess-section-tag {
        font-size: 0.8rem;
        font-weight: 700;
        text-transform: uppercase;
        color: var(--brand-primary);
        letter-spacing: 0.05em;
        margin-bottom: 8px;
    }
    .ess-section-title {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-size: 2rem;
        font-weight: 800;
        color: var(--text-main);
    }

    /* Area Cards */
    .ess-areas-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: 24px;
    }
    .ess-area-card {
        height: 260px;
        border-radius: var(--radius-lg);
        position: relative;
        overflow: hidden;
        display: flex;
        flex-direction: column;
        justify-content: flex-end;
        padding: 24px;
        color: #fff;
        background-size: cover;
        background-position: center;
        box-shadow: var(--shadow-md);
        transition: transform 0.2s ease;
    }
    .ess-area-card:hover { transform: translateY(-4px); }
    .ess-area-card::before {
        content: '';
        position: absolute;
        inset: 0;
        background: linear-gradient(transparent, rgba(15, 23, 42, 0.85));
    }
    .ess-area-content { position: relative; z-index: 1; }
    .ess-area-title { font-size: 1.35rem; font-weight: 700; margin-bottom: 4px; }
    .ess-area-desc { font-size: 0.85rem; color: #cbd5e1; }

    /* About Preview */
    .ess-about-box {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 48px;
        align-items: center;
        background: #fff;
        border: 1px solid var(--border-color);
        border-radius: var(--radius-lg);
        padding: 40px;
    }
    .ess-about-img {
        width: 100%;
        height: 380px;
        object-fit: cover;
        border-radius: var(--radius-md);
    }

    @media (max-width: 768px) {
        .ess-header-inner { height: auto; padding: 16px 0; flex-direction: column; gap: 12px; }
        .ess-nav { flex-wrap: wrap; justify-content: center; gap: 16px; }
        .ess-hero-heading { font-size: 2rem; }
        .ess-hero-search { flex-direction: column; }
        .ess-about-box { grid-template-columns: 1fr; }
    }
`;

export function renderEssentialHome({ siteBundle, featuredListings = [], openHouses = [], basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;

    const listingsHtml = featuredListings.length > 0
        ? `<div class="sneak-grid-3">${featuredListings.map(p => renderPropertyCard(p, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: var(--text-muted);">No active featured listings currently in scope.</p>`;

    const openHousesHtml = openHouses.length > 0
        ? `<div class="sneak-grid-3">${openHouses.map(oh => renderOpenHouseCard(oh, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: var(--text-muted);">No upcoming open houses scheduled at this time.</p>`;

    const areasHtml = (websiteConfig.featured_areas || []).map(a => `
        <a href="${basePath}/search?q=${encodeURIComponent(a.filter || a.name)}" class="ess-area-card" style="background-image: url('${escapeHtml(a.image_url)}');">
            <div class="ess-area-content">
                <h3 class="ess-area-title">${escapeHtml(a.name)}</h3>
                <p class="ess-area-desc">${escapeHtml(a.description)}</p>
            </div>
        </a>
    `).join('');

    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'home')}

        <section class="ess-hero" style="background-image: url('${escapeHtml(websiteConfig.hero_image_url)}');">
            <div class="sneak-container ess-hero-content">
                <h1 class="ess-hero-heading">${escapeHtml(websiteConfig.hero_heading)}</h1>
                <p class="ess-hero-sub">${escapeHtml(websiteConfig.hero_subheading)}</p>

                <form class="ess-hero-search" action="${basePath}/search" method="GET">
                    <div class="ess-search-field" style="flex: 2;">
                        <label>Location / City</label>
                        <input type="text" name="q" placeholder="Bonita Springs, Naples, Estero..." />
                    </div>
                    <div class="ess-search-field">
                        <label>Min Price</label>
                        <select name="minPrice">
                            <option value="">Any Min</option>
                            <option value="300000">$300k</option>
                            <option value="500000">$500k</option>
                            <option value="750000">$750k</option>
                            <option value="1000000">$1M</option>
                            <option value="2000000">$2M</option>
                        </select>
                    </div>
                    <div class="ess-search-field">
                        <label>Max Price</label>
                        <select name="maxPrice">
                            <option value="">Any Max</option>
                            <option value="500000">$500k</option>
                            <option value="750000">$750k</option>
                            <option value="1000000">$1M</option>
                            <option value="2000000">$2M</option>
                            <option value="5000000">$5M+</option>
                        </select>
                    </div>
                    <div class="ess-search-field">
                        <label>Beds</label>
                        <select name="beds">
                            <option value="">Any</option>
                            <option value="2">2+</option>
                            <option value="3">3+</option>
                            <option value="4">4+</option>
                        </select>
                    </div>
                    <div style="display: flex; align-items: flex-end;">
                        <button type="submit" class="sneak-btn sneak-btn-primary" style="height: 42px;">Search Homes</button>
                    </div>
                </form>
            </div>
        </section>

        <!-- Featured Listings -->
        <section class="ess-section">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Curated Portfolio</div>
                    <h2 class="ess-section-title">Featured Properties</h2>
                </div>
                ${listingsHtml}
                <div style="text-align: center; margin-top: 40px;">
                    <a href="${basePath}/search" class="sneak-btn sneak-btn-secondary">View All Active Listings &rarr;</a>
                </div>
            </div>
        </section>

        <!-- Featured Areas -->
        <section class="ess-section ess-section-alt">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Communities</div>
                    <h2 class="ess-section-title">Featured Southwest Florida Markets</h2>
                </div>
                <div class="ess-areas-grid">
                    ${areasHtml}
                </div>
            </div>
        </section>

        <!-- About Intro -->
        <section class="ess-section">
            <div class="sneak-container">
                <div class="ess-about-box">
                    <div>
                        <div class="ess-section-tag">About The Advisor</div>
                        <h2 style="font-size: 2.25rem; font-weight: 800; margin-bottom: 16px;">${escapeHtml(branding.display_name)}</h2>
                        <p style="color: var(--text-muted); font-size: 1.05rem; line-height: 1.7; margin-bottom: 24px;">${escapeHtml(websiteConfig.about_body)}</p>
                        <a href="${basePath}/about" class="sneak-btn sneak-btn-primary">Read Full Bio &amp; Story</a>
                    </div>
                    <div>
                        <img src="${escapeHtml(websiteConfig.about_image_url || branding.agent_photo_url || 'https://images.unsplash.com/photo-1573496359142-b8d87734a5a2?auto=format&fit=crop&w=800&q=80')}" alt="${escapeHtml(branding.display_name)}" class="ess-about-img" />
                    </div>
                </div>
            </div>
        </section>

        <!-- Open Houses Section -->
        <section class="ess-section ess-section-alt">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Schedule &amp; Tour</div>
                    <h2 class="ess-section-title">Upcoming Open Houses</h2>
                </div>
                ${openHousesHtml}
            </div>
        </section>

        <!-- Contact CTA -->
        <section class="ess-section" style="background: var(--brand-primary); color: #fff; text-align: center;">
            <div class="sneak-container" style="max-width: 680px;">
                <h2 style="font-size: 2.25rem; font-weight: 800; margin-bottom: 16px;">Connect with ${escapeHtml(branding.display_name)}</h2>
                <p style="color: #e2e8f0; font-size: 1.1rem; margin-bottom: 32px;">${escapeHtml(websiteConfig.contact_cta_text)}</p>
                <a href="${basePath}/contact" class="sneak-btn sneak-btn-secondary" style="font-size: 1.05rem; padding: 14px 32px;">Start the Conversation</a>
            </div>
        </section>

        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Home',
        pagePath: '/',
        basePath,
        contentHtml,
        previewToken,
        customStyles: ESSENTIAL_STYLES
    });
}

export function renderEssentialSearch({ siteBundle, basePath = '', previewToken = null }) {
    const { site } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'search')}
        <div style="min-height: calc(100vh - 76px); background: #f8fafc; padding: 24px 0;">
            <div class="sneak-container">
                <h1 style="font-size: 1.75rem; font-weight: 800; margin-bottom: 16px;">Southwest Florida MLS Property Search</h1>
                <div id="sneak-idx-root" data-site="${escapeHtml(site.site_key)}"></div>
                <script src="https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/embed.js" async></script>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: 'Property Search',
        pagePath: '/search',
        basePath,
        contentHtml,
        previewToken,
        customStyles: ESSENTIAL_STYLES
    });
}

export function renderEssentialOpenHouses({ siteBundle, openHouses = [], basePath = '', previewToken = null }) {
    const openHousesHtml = openHouses.length > 0
        ? `<div class="sneak-grid-3">${openHouses.map(oh => renderOpenHouseCard(oh, basePath)).join('')}</div>`
        : `<p style="text-align: center; color: var(--text-muted); padding: 40px 0;">No upcoming open houses scheduled at this time. Check back soon!</p>`;

    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'open-houses')}
        <div class="ess-section">
            <div class="sneak-container">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Tour Opportunities</div>
                    <h1 class="ess-section-title">Upcoming Open Houses</h1>
                    <p style="color: var(--text-muted); margin-top: 8px;">Explore Southwest Florida properties hosting public and private viewings this week.</p>
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
        customStyles: ESSENTIAL_STYLES
    });
}

export function renderEssentialAbout({ siteBundle, basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'about')}
        <div class="ess-section">
            <div class="sneak-container" style="max-width: 900px;">
                <div style="display: flex; gap: 40px; align-items: center; margin-bottom: 40px; flex-wrap: wrap;">
                    <img src="${escapeHtml(websiteConfig.about_image_url || branding.agent_photo_url || 'https://images.unsplash.com/photo-1573496359142-b8d87734a5a2?auto=format&fit=crop&w=800&q=80')}" alt="${escapeHtml(branding.display_name)}" style="width: 240px; height: 240px; border-radius: var(--radius-full); object-fit: cover; box-shadow: var(--shadow-md);" />
                    <div>
                        <div class="ess-section-tag">Professional Profile</div>
                        <h1 style="font-size: 2.5rem; font-weight: 800;">${escapeHtml(branding.display_name)}</h1>
                        <p style="color: var(--text-muted); font-size: 1.1rem; margin-top: 4px;">${escapeHtml(branding.brokerage)}</p>
                    </div>
                </div>
                <div style="font-size: 1.1rem; line-height: 1.8; color: var(--text-main); margin-bottom: 40px;">
                    <p>${escapeHtml(websiteConfig.about_body)}</p>
                </div>
                <div style="background: var(--surface-subtle); padding: 32px; border-radius: var(--radius-lg); border: 1px solid var(--border-color); text-align: center;">
                    <h3 style="font-size: 1.35rem; margin-bottom: 8px;">Ready to work together?</h3>
                    <p style="color: var(--text-muted); margin-bottom: 20px;">Contact me directly for representation or MLS market inquiries.</p>
                    <a href="${basePath}/contact" class="sneak-btn sneak-btn-primary">Send a Direct Message</a>
                </div>
            </div>
        </div>
        ${renderFooter(siteBundle, basePath)}
    `;

    return renderBaseLayout({
        siteBundle,
        pageTitle: `About ${branding.display_name}`,
        pagePath: '/about',
        basePath,
        contentHtml,
        previewToken,
        customStyles: ESSENTIAL_STYLES
    });
}

export function renderEssentialContact({ siteBundle, basePath = '', previewToken = null }) {
    const { branding, websiteConfig } = siteBundle;
    const contentHtml = `
        ${renderHeader(siteBundle, basePath, 'contact')}
        <div class="ess-section ess-section-alt">
            <div class="sneak-container" style="max-width: 680px;">
                <div class="ess-section-header">
                    <div class="ess-section-tag">Get In Touch</div>
                    <h1 class="ess-section-title">Contact ${escapeHtml(branding.display_name)}</h1>
                    <p style="color: var(--text-muted); margin-top: 8px;">${escapeHtml(websiteConfig.contact_cta_text)}</p>
                </div>
                <div class="sneak-form-card">
                    <form onsubmit="submitContactForm(this)">
                        <!-- Honeypot -->
                        <div style="display: none;">
                            <input type="text" name="website_hp" tabindex="-1" autocomplete="off" />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Your Full Name *</label>
                            <input type="text" name="name" class="sneak-form-input" placeholder="Jane Doe" required />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Email Address *</label>
                            <input type="email" name="email" class="sneak-form-input" placeholder="jane@example.com" required />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">Phone Number (Optional)</label>
                            <input type="tel" name="phone" class="sneak-form-input" placeholder="(239) 555-0123" />
                        </div>
                        <div class="sneak-form-group">
                            <label class="sneak-form-label">How can I help you? *</label>
                            <textarea name="message" rows="4" class="sneak-form-textarea" placeholder="I'm interested in viewing properties in Bonita Springs..." required></textarea>
                        </div>
                        <button type="submit" class="sneak-btn sneak-btn-primary" style="width: 100%;">Send Inquiry</button>
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
        customStyles: ESSENTIAL_STYLES
    });
}
