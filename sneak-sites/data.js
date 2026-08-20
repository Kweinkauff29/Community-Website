/**
 * sneak-sites/data.js
 * 
 * Tenant Resolution, Branding, Website Configuration & MLS Data Access for SNEAK Websites.
 */

import { isAccountEntitled } from '../sneak-member/billing.js';

/**
 * Resolves a full tenant site bundle (Site, Account, Entitlement, Branding, Website Config).
 * Supports lookup by siteKey or custom domain.
 */
export async function resolveTenantSite(db, { siteKey, domain }) {
    if (!db) return null;

    let query = `
        SELECT 
            s.id AS site_id, s.account_id, s.site_key, s.site_name, s.scope_type, s.scope_value, s.status AS site_status,
            a.account_name, a.plan AS account_plan, a.status AS account_status,
            e.status AS entitlement_status, e.grace_until, e.plan AS entitlement_plan,
            b.display_name, b.brokerage, b.logo_url, b.agent_photo_url, b.primary_color, b.secondary_color,
            b.phone, b.email, b.website_url,
            w.enabled AS website_enabled, w.template_key, w.site_title, w.tagline,
            w.hero_heading, w.hero_subheading, w.hero_image_url,
            w.about_heading, w.about_body, w.about_image_url,
            w.featured_areas_json, w.navigation_json, w.social_links_json,
            w.seo_title, w.seo_description, w.footer_text, w.contact_cta_text
        FROM sneak_sites s
        JOIN sneak_accounts a ON s.account_id = a.id
        LEFT JOIN sneak_account_entitlements e ON a.id = e.account_id
        LEFT JOIN sneak_branding b ON s.id = b.site_id
        LEFT JOIN sneak_website_configs w ON s.id = w.site_id
    `;

    let row = null;

    if (siteKey) {
        query += ` WHERE s.site_key = ?`;
        row = await db.prepare(query).bind(siteKey).first();
    } else if (domain) {
        query += `
            JOIN sneak_domains d ON s.id = d.site_id
            WHERE d.domain = ? AND d.status = 'active' AND d.verified = 1
        `;
        row = await db.prepare(query).bind(domain.toLowerCase().trim()).first();
    }

    if (!row) return null;

    // Check Administrative Status
    const isAccountActive = row.account_status === 'active';
    const isSiteActive = row.site_status === 'active';

    // Check Service Entitlement
    const isEntitled = isAccountEntitled(row.account_status, row.entitlement_status, row.grace_until);

    let featuredAreas = [];
    try {
        if (row.featured_areas_json) featuredAreas = JSON.parse(row.featured_areas_json);
    } catch {}

    let socialLinks = {};
    try {
        if (row.social_links_json) socialLinks = JSON.parse(row.social_links_json);
    } catch {}

    return {
        site: {
            id: row.site_id,
            account_id: row.account_id,
            site_key: row.site_key,
            site_name: row.site_name,
            scope_type: row.scope_type || 'market',
            scope_value: row.scope_value || null,
            status: row.site_status
        },
        account: {
            id: row.account_id,
            account_name: row.account_name,
            plan: row.entitlement_plan || row.account_plan || 'pro',
            status: row.account_status
        },
        entitlement: {
            status: row.entitlement_status || 'active',
            isEntitled,
            graceUntil: row.grace_until
        },
        isOperational: isAccountActive && isSiteActive && isEntitled,
        branding: {
            display_name: row.display_name || row.account_name || 'Premier Agent',
            brokerage: row.brokerage || 'Bonita Springs-Estero REALTORS®',
            logo_url: row.logo_url || null,
            agent_photo_url: row.agent_photo_url || null,
            primary_color: row.primary_color || '#1e3a8a',
            secondary_color: row.secondary_color || '#0284c7',
            phone: row.phone || '(239) 555-0100',
            email: row.email || 'info@southwestfloridarealty.com',
            website_url: row.website_url || null
        },
        websiteConfig: {
            enabled: row.website_enabled === 1,
            template_key: row.template_key || 'essential',
            site_title: row.site_title || `${row.display_name || row.account_name} | Southwest Florida Real Estate`,
            tagline: row.tagline || 'Your trusted guide to Southwest Florida living',
            hero_heading: row.hero_heading || 'Find Your Place in Southwest Florida',
            hero_subheading: row.hero_subheading || 'Explore luxury waterfront estates, golf communities, and coastal properties.',
            hero_image_url: row.hero_image_url || 'https://images.unsplash.com/photo-1512917774080-9991f1c4c750?auto=format&fit=crop&w=1600&q=80',
            about_heading: row.about_heading || `About ${row.display_name || row.account_name}`,
            about_body: row.about_body || 'Dedicated to providing exceptional real estate advisory and MLS representation across Bonita Springs, Estero, Naples, and Southwest Florida.',
            about_image_url: row.about_image_url || row.agent_photo_url || null,
            featured_areas: featuredAreas.length > 0 ? featuredAreas : [
                { name: 'Bonita Springs', description: 'Gulf beaches, boating, and golf clubs', filter: 'Bonita Springs', image_url: 'https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&w=600&q=80' },
                { name: 'Estero', description: 'Vibrant master-planned communities and Estero Bay', filter: 'Estero', image_url: 'https://images.unsplash.com/photo-1544644181-1484b3fdfc62?auto=format&fit=crop&w=600&q=80' },
                { name: 'Naples', description: 'World-class dining, luxury estates, and white sand beaches', filter: 'Naples', image_url: 'https://images.unsplash.com/photo-1580587771525-78b9dba3b914?auto=format&fit=crop&w=600&q=80' }
            ],
            social_links: socialLinks,
            seo_title: row.seo_title || `${row.display_name || row.account_name} - Southwest Florida Real Estate`,
            seo_description: row.seo_description || 'Search all active MLS listings, open houses, and luxury properties in Southwest Florida.',
            footer_text: row.footer_text || `© ${new Date().getFullYear()} ${row.display_name || row.account_name}. All rights reserved.`,
            contact_cta_text: row.contact_cta_text || 'Ready to start your property search or schedule a private showing? Get in touch today.'
        }
    };
}

/**
 * Fetches deterministic featured listings strictly within tenant scope.
 */
export async function getFeaturedListings(db, site, limit = 6) {
    if (!db || !site) return [];

    let query = `
        SELECT 
            ListingKey, ListPrice, StandardStatus, PropertyType, PropertySubType,
            UnparsedAddress, City, StateOrProvince, PostalCode,
            BedroomsTotal, BathroomsTotalInteger, LivingArea,
            MediaURL, ModificationTimestamp
        FROM sneak_listings
        WHERE StandardStatus = 'Active'
    `;
    const params = [];

    if (site.scope_type === 'agent' && site.scope_value) {
        query += ` AND ListAgentMlsId = ?`;
        params.push(site.scope_value);
    } else if (site.scope_type === 'office' && site.scope_value) {
        query += ` AND ListOfficeMlsId = ?`;
        params.push(site.scope_value);
    }

    query += ` ORDER BY ModificationTimestamp DESC LIMIT ?`;
    params.push(limit);

    try {
        const stmt = db.prepare(query);
        const res = await stmt.bind(...params).all();
        return res.results || [];
    } catch (err) {
        console.error('[FEATURED LISTINGS ERROR]', err.message);
        return [];
    }
}

/**
 * Fetches upcoming open houses strictly within tenant scope.
 */
export async function getUpcomingOpenHouses(db, site, limit = 6) {
    if (!db || !site) return [];

    const nowIso = new Date().toISOString();
    let query = `
        SELECT 
            o.OpenHouseKey, o.ListingKey, o.OpenHouseDate, o.OpenHouseStartTime, o.OpenHouseEndTime,
            l.UnparsedAddress, l.City, l.StateOrProvince, l.PostalCode,
            l.ListPrice, l.BedroomsTotal, l.BathroomsTotalInteger, l.LivingArea,
            l.MediaURL, l.PropertySubType
        FROM sneak_open_houses o
        JOIN sneak_listings l ON o.ListingKey = l.ListingKey
        WHERE l.StandardStatus = 'Active'
          AND (o.OpenHouseEndTime >= ? OR o.OpenHouseDate >= date('now'))
    `;
    const params = [nowIso];

    if (site.scope_type === 'agent' && site.scope_value) {
        query += ` AND l.ListAgentMlsId = ?`;
        params.push(site.scope_value);
    } else if (site.scope_type === 'office' && site.scope_value) {
        query += ` AND l.ListOfficeMlsId = ?`;
        params.push(site.scope_value);
    }

    query += ` ORDER BY o.OpenHouseDate ASC, o.OpenHouseStartTime ASC LIMIT ?`;
    params.push(limit);

    try {
        const stmt = db.prepare(query);
        const res = await stmt.bind(...params).all();
        return res.results || [];
    } catch (err) {
        console.error('[OPEN HOUSES ERROR]', err.message);
        return [];
    }
}
