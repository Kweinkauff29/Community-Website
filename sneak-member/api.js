/**
 * sneak-member/api.js
 * 
 * Member Portal API Handlers with Strict Tenant Isolation.
 */

import {
    normalizeDomain,
    validateDomainSafety,
    validateHexColor,
    validateUrl,
    VALID_WIDGET_TYPES
} from '../sneak-admin/validation.js';
import { generateEmbedSnippets } from '../sneak-admin/embed-generator.js';
import { getAccountEntitlement } from './billing.js';

function json(data, status = 200) {
    return new Response(JSON.stringify(data), {
        status,
        headers: { 'Content-Type': 'application/json' }
    });
}

function error(message, status = 400, code = 'BadRequest') {
    return json({ error: code, message }, status);
}

async function logMemberAudit(db, userId, accountId, action, entityType, entityId, summary) {
    try {
        const id = `maudit_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_member_audit (id, user_id, account_id, action, entity_type, entity_id, summary, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        `).bind(id, userId, accountId, action, entityType, entityId, summary).run();
    } catch (err) {
        console.error('[MEMBER AUDIT LOG ERROR]', err.message);
    }
}

/**
 * GET /api/member/overview
 */
export async function handleMemberOverview(db, memberContext, env = {}) {
    const { account_id, user_id, member_email, account_name, account_plan } = memberContext;

    const sites = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).all();
    const siteList = sites.results || [];
    const primarySite = siteList[0] || null;

    let domains = [];
    let branding = null;
    let widgetConfigs = [];
    let embed = null;

    if (primarySite) {
        const domRes = await db.prepare("SELECT * FROM sneak_domains WHERE site_id = ?").bind(primarySite.id).all();
        domains = domRes.results || [];
        branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(primarySite.id).first();
        const widRes = await db.prepare("SELECT * FROM sneak_widget_configs WHERE site_id = ?").bind(primarySite.id).all();
        widgetConfigs = widRes.results || [];

        const allowed = domains.filter(d => d.status === 'active' && d.verified === 1).map(d => d.domain);
        embed = generateEmbedSnippets(primarySite.site_key, allowed, branding || {}, env);
    }

    const entitlement = await getAccountEntitlement(db, account_id);
    const listingsCount = await db.prepare("SELECT count(*) as count FROM sneak_listings").first();
    const openHousesCount = await db.prepare("SELECT count(*) as count FROM sneak_open_houses").first();

    const leadsCount = await db.prepare(`
        SELECT count(*) as count
        FROM sneak_leads l
        JOIN sneak_sites s ON l.site_id = s.id
        WHERE s.account_id = ?
    `).bind(account_id).first();

    const clientsCountRow = await db.prepare(`
        SELECT count(*) as count
        FROM sneak_consumer_users u
        JOIN sneak_sites s ON u.site_id = s.id
        WHERE s.account_id = ?
    `).bind(account_id).first();

    const activeClients7dRow = await db.prepare(`
        SELECT count(*) as count
        FROM sneak_consumer_users u
        JOIN sneak_sites s ON u.site_id = s.id
        WHERE s.account_id = ?
          AND (u.last_activity_at > datetime('now', '-7 days') OR u.last_login_at > datetime('now', '-7 days'))
    `).bind(account_id).first();

    const savedHomesCountRow = await db.prepare(`
        SELECT count(*) as count
        FROM sneak_consumer_favorites f
        JOIN sneak_sites s ON f.site_id = s.id
        WHERE s.account_id = ?
    `).bind(account_id).first();

    const savedSearchesCountRow = await db.prepare(`
        SELECT count(*) as count
        FROM sneak_consumer_saved_searches ss
        JOIN sneak_sites s ON ss.site_id = s.id
        WHERE s.account_id = ?
    `).bind(account_id).first();

    return json({
        account: {
            id: account_id,
            account_name,
            plan: account_plan,
            member_email
        },
        site: primarySite,
        domains,
        branding,
        widgets: widgetConfigs,
        embed,
        billing: {
            authority: 'sneak_account_entitlements',
            provider: entitlement?.provider || 'CCOR / GrowthZone',
            plan: entitlement?.plan || account_plan,
            status: entitlement?.status || 'missing',
            statusLabel: entitlement?.statusLabel || 'Not Configured — Contact CCOR',
            entitlement_status: entitlement?.status || 'missing',
            isEntitled: entitlement?.isEntitled === true,
            graceUntil: entitlement?.graceUntil || null,
            lastVerifiedAt: entitlement?.lastVerifiedAt || null
        },
        inventory: {
            activeListings: listingsCount?.count || 0,
            futureOpenHouses: openHousesCount?.count || 0
        },
        leadsCount: leadsCount?.count || 0,
        clientsCount: clientsCountRow?.count || 0,
        activeClients7dCount: activeClients7dRow?.count || 0,
        savedHomesCount: savedHomesCountRow?.count || 0,
        savedSearchesCount: savedSearchesCountRow?.count || 0
    });
}

/**
 * GET /api/member/domains
 */
export async function handleListMemberDomains(db, memberContext) {
    const { account_id } = memberContext;
    const res = await db.prepare(`
        SELECT d.*
        FROM sneak_domains d
        JOIN sneak_sites s ON d.site_id = s.id
        WHERE s.account_id = ?
        ORDER BY d.created_at ASC
    `).bind(account_id).all();

    return json({ domains: res.results || [] });
}

/**
 * POST /api/member/domains
 */
export async function handleAddMemberDomain(db, memberContext, body) {
    const { account_id, user_id } = memberContext;
    const { domain } = body;

    const domainVal = validateDomainSafety(domain);
    if (!domainVal.valid) return error(domainVal.error);

    const primarySite = await db.prepare("SELECT id FROM sneak_sites WHERE account_id = ? LIMIT 1").bind(account_id).first();
    if (!primarySite) return error('No site found for this account', 404);

    const domainId = `dom_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;

    // Member-added domains start with verified = 0 pending admin confirmation
    await db.prepare(`
        INSERT INTO sneak_domains (id, site_id, domain, verified, status, created_at)
        VALUES (?, ?, ?, 0, 'active', datetime('now'))
    `).bind(domainId, primarySite.id, domainVal.domain).run();

    await logMemberAudit(db, user_id, account_id, 'ADD_DOMAIN', 'domain', domainId, `Member added domain '${domainVal.domain}' (pending verification)`);

    const newDomain = await db.prepare("SELECT * FROM sneak_domains WHERE id = ?").bind(domainId).first();
    return json({ success: true, domain: newDomain }, 201);
}

/**
 * DELETE /api/member/domains/:id
 */
export async function handleDeleteMemberDomain(db, memberContext, domainId) {
    const { account_id, user_id } = memberContext;

    // Strict Tenant Isolation: Check domain belongs to member's account
    const existing = await db.prepare(`
        SELECT d.id, d.domain
        FROM sneak_domains d
        JOIN sneak_sites s ON d.site_id = s.id
        WHERE d.id = ? AND s.account_id = ?
    `).bind(domainId, account_id).first();

    if (!existing) return error('Domain not found or unauthorized', 404);

    await db.prepare("DELETE FROM sneak_domains WHERE id = ?").bind(domainId).run();
    await logMemberAudit(db, user_id, account_id, 'DELETE_DOMAIN', 'domain', domainId, `Member removed domain '${existing.domain}'`);

    return json({ success: true });
}

/**
 * GET /api/member/branding
 */
export async function handleGetMemberBranding(db, memberContext) {
    const { account_id } = memberContext;
    const site = await db.prepare("SELECT id FROM sneak_sites WHERE account_id = ? LIMIT 1").bind(account_id).first();
    if (!site) return error('No site configured', 404);

    const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(site.id).first();
    return json({ branding: branding || null });
}

/**
 * PUT /api/member/branding
 */
export async function handleUpdateMemberBranding(db, memberContext, body) {
    const { account_id, user_id } = memberContext;
    const site = await db.prepare("SELECT id, site_name, site_key FROM sneak_sites WHERE account_id = ? LIMIT 1").bind(account_id).first();
    if (!site) return error('No site configured', 404);

    const {
        display_name,
        brokerage,
        logo_url,
        agent_photo_url,
        primary_color = '#1a365d',
        secondary_color = '#2596be',
        phone,
        email,
        website_url,
        config_json
    } = body;

    if (primary_color && !validateHexColor(primary_color)) return error('Invalid primary_color hex format.');
    if (secondary_color && !validateHexColor(secondary_color)) return error('Invalid secondary_color hex format.');
    if (logo_url && !validateUrl(logo_url)) return error('Invalid logo_url format.');
    if (agent_photo_url && !validateUrl(agent_photo_url)) return error('Invalid agent_photo_url format.');
    if (website_url && !validateUrl(website_url)) return error('Invalid website_url format.');

    await db.prepare(`
        INSERT OR REPLACE INTO sneak_branding (
            site_id, display_name, brokerage, logo_url, agent_photo_url,
            primary_color, secondary_color, phone, email, website_url, config_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
        site.id,
        display_name || site.site_name,
        brokerage || null,
        logo_url || null,
        agent_photo_url || null,
        primary_color,
        secondary_color,
        phone || null,
        email || null,
        website_url || null,
        typeof config_json === 'object' ? JSON.stringify(config_json) : (config_json || null)
    ).run();

    await logMemberAudit(db, user_id, account_id, 'UPDATE_BRANDING', 'branding', site.id, `Updated branding for site ${site.site_key}`);

    const updated = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(site.id).first();
    return json({ success: true, branding: updated });
}

/**
 * GET /api/member/widgets
 */
export async function handleGetMemberWidgets(db, memberContext) {
    const { account_id } = memberContext;
    const site = await db.prepare("SELECT id FROM sneak_sites WHERE account_id = ? LIMIT 1").bind(account_id).first();
    if (!site) return error('No site configured', 404);

    const res = await db.prepare("SELECT * FROM sneak_widget_configs WHERE site_id = ?").bind(site.id).all();
    return json({ widgets: res.results || [] });
}

/**
 * PUT /api/member/widgets/:type
 */
export async function handleUpdateMemberWidget(db, memberContext, widgetType, body) {
    const { account_id, user_id } = memberContext;
    if (!VALID_WIDGET_TYPES.has(widgetType)) return error(`Invalid widget type: ${widgetType}`);

    const site = await db.prepare("SELECT id FROM sneak_sites WHERE account_id = ? LIMIT 1").bind(account_id).first();
    if (!site) return error('No site configured', 404);

    const { enabled = 1, config_json = {} } = body;
    const configStr = typeof config_json === 'object' ? JSON.stringify(config_json) : String(config_json || '{}');
    const id = `w_${site.id}_${widgetType}`;

    await db.prepare(`
        INSERT OR REPLACE INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json, created_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    `).bind(id, site.id, widgetType, enabled ? 1 : 0, configStr).run();

    await logMemberAudit(db, user_id, account_id, 'UPDATE_WIDGET', 'widget', id, `Updated widget ${widgetType}`);

    const updated = await db.prepare("SELECT * FROM sneak_widget_configs WHERE id = ?").bind(id).first();
    return json({ success: true, widget: updated });
}

/**
 * GET /api/member/embed
 */
export async function handleGetMemberEmbed(db, memberContext, env = {}) {
    const { account_id } = memberContext;
    const site = await db.prepare("SELECT id, site_key FROM sneak_sites WHERE account_id = ? LIMIT 1").bind(account_id).first();
    if (!site) return error('No site configured', 404);

    const domains = await db.prepare("SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1").bind(site.id).all();
    const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(site.id).first();

    const allowed = (domains.results || []).map(d => d.domain);
    const embed = generateEmbedSnippets(site.site_key, allowed, branding || {}, env);

    return json(embed);
}

/**
 * GET /api/member/leads
 */
export async function handleGetMemberLeads(db, memberContext) {
    const { account_id } = memberContext;
    const res = await db.prepare(`
        SELECT l.*, s.site_name
        FROM sneak_leads l
        JOIN sneak_sites s ON l.site_id = s.id
        WHERE s.account_id = ?
        ORDER BY l.created_at DESC
        LIMIT 50
    `).bind(account_id).all();

    return json({ leads: res.results || [] });
}

/**
 * GET /api/member/billing
 */
export async function handleGetMemberBilling(db, memberContext) {
    const { account_id } = memberContext;
    const entitlement = await getAccountEntitlement(db, account_id);

    return json({
        plan: entitlement?.plan || memberContext.account_plan || 'pro',
        provider: 'GrowthZone',
        billingCycle: 'Monthly (1st of each month)',
        status: entitlement?.status || 'missing',
        statusLabel: entitlement?.statusLabel || 'Not Configured — Contact CCOR',
        isEntitled: entitlement?.isEntitled === true,
        graceUntil: entitlement?.graceUntil || null,
        growthzoneUrl: 'https://bonitaspringsesterorealtorsfl.growthzoneapp.com/',
        instructions: 'Your SNEAK IDX subscription is managed by the Coconut Coast Organization of REALTORS® through GrowthZone. Billing occurs automatically on the first of each month.'
    });
}

import { createPreviewToken } from '../sneak-sites/preview.js';

/**
 * GET /api/member/website
 */
export async function handleGetMemberWebsiteConfig(db, memberContext, env) {
    const { account_id } = memberContext;
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).first();
    if (!site) return error('No site found for this account', 404);

    const config = await db.prepare("SELECT * FROM sneak_website_configs WHERE site_id = ?").bind(site.id).first();
    const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(site.id).first();

    const secret = env?.SNEAK_WEBSITE_PREVIEW_SECRET || 'dev_preview_secret_ccor_2026';
    let previewToken = null;
    let previewUrl = null;

    try {
        previewToken = await createPreviewToken(site.site_key, site.id, secret, 1800);
        previewUrl = `https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/${site.site_key}?token=${encodeURIComponent(previewToken)}`;
    } catch {}

    return json({
        site_id: site.id,
        site_key: site.site_key,
        website: config || {
            site_id: site.id,
            enabled: 0,
            template_key: 'essential',
            site_title: `${branding?.display_name || memberContext.account_name} | Southwest Florida Real Estate`,
            tagline: 'Your trusted guide to Southwest Florida living',
            hero_heading: 'Find Your Place in Southwest Florida',
            hero_subheading: 'Explore luxury waterfront estates, golf communities, and coastal properties.',
            hero_image_url: 'https://images.unsplash.com/photo-1512917774080-9991f1c4c750?auto=format&fit=crop&w=1600&q=80',
            about_heading: `About ${branding?.display_name || memberContext.account_name}`,
            about_body: 'Dedicated to providing exceptional real estate advisory and MLS representation across Southwest Florida.',
            about_image_url: branding?.agent_photo_url || null,
            featured_areas_json: JSON.stringify([
                { name: 'Bonita Springs', description: 'Gulf beaches, boating, and golf clubs', filter: 'Bonita Springs', image_url: 'https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&w=600&q=80' },
                { name: 'Estero', description: 'Vibrant master-planned communities and Estero Bay', filter: 'Estero', image_url: 'https://images.unsplash.com/photo-1544644181-1484b3fdfc62?auto=format&fit=crop&w=600&q=80' },
                { name: 'Naples', description: 'World-class dining, luxury estates, and white sand beaches', filter: 'Naples', image_url: 'https://images.unsplash.com/photo-1580587771525-78b9dba3b914?auto=format&fit=crop&w=600&q=80' }
            ]),
            seo_title: `${branding?.display_name || memberContext.account_name} - Southwest Florida Real Estate`,
            seo_description: 'Search all active MLS listings, open houses, and luxury properties in Southwest Florida.',
            footer_text: `© ${new Date().getFullYear()} ${branding?.display_name || memberContext.account_name}. All rights reserved.`,
            contact_cta_text: 'Ready to start your property search or schedule a private showing? Get in touch today.'
        },
        previewUrl,
        previewToken
    });
}

/**
 * PUT /api/member/website
 */
export async function handleUpdateMemberWebsiteConfig(db, memberContext, body, env) {
    const { account_id, user_id } = memberContext;
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).first();
    if (!site) return error('No site found for this account', 404);

    const {
        enabled = 0,
        template_key = 'essential',
        site_title,
        tagline,
        hero_heading,
        hero_subheading,
        hero_image_url,
        about_heading,
        about_body,
        about_image_url,
        featured_areas_json,
        social_links_json,
        seo_title,
        seo_description,
        footer_text,
        contact_cta_text
    } = body;

    const validTemplates = ['essential', 'coastal', 'brokerage'];
    if (!validTemplates.includes(template_key)) {
        return error(`Invalid template_key. Must be one of: ${validTemplates.join(', ')}`);
    }

    const now = new Date().toISOString();

    await db.prepare(`
        INSERT INTO sneak_website_configs (
            site_id, enabled, template_key, site_title, tagline,
            hero_heading, hero_subheading, hero_image_url,
            about_heading, about_body, about_image_url,
            featured_areas_json, social_links_json,
            seo_title, seo_description, footer_text, contact_cta_text,
            created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?
        )
        ON CONFLICT(site_id) DO UPDATE SET
            enabled = COALESCE(excluded.enabled, sneak_website_configs.enabled),
            template_key = COALESCE(excluded.template_key, sneak_website_configs.template_key),
            site_title = COALESCE(excluded.site_title, sneak_website_configs.site_title),
            tagline = COALESCE(excluded.tagline, sneak_website_configs.tagline),
            hero_heading = COALESCE(excluded.hero_heading, sneak_website_configs.hero_heading),
            hero_subheading = COALESCE(excluded.hero_subheading, sneak_website_configs.hero_subheading),
            hero_image_url = COALESCE(excluded.hero_image_url, sneak_website_configs.hero_image_url),
            about_heading = COALESCE(excluded.about_heading, sneak_website_configs.about_heading),
            about_body = COALESCE(excluded.about_body, sneak_website_configs.about_body),
            about_image_url = COALESCE(excluded.about_image_url, sneak_website_configs.about_image_url),
            featured_areas_json = COALESCE(excluded.featured_areas_json, sneak_website_configs.featured_areas_json),
            social_links_json = COALESCE(excluded.social_links_json, sneak_website_configs.social_links_json),
            seo_title = COALESCE(excluded.seo_title, sneak_website_configs.seo_title),
            seo_description = COALESCE(excluded.seo_description, sneak_website_configs.seo_description),
            footer_text = COALESCE(excluded.footer_text, sneak_website_configs.footer_text),
            contact_cta_text = COALESCE(excluded.contact_cta_text, sneak_website_configs.contact_cta_text),
            updated_at = excluded.updated_at
    `).bind(
        site.id, enabled !== undefined ? (enabled ? 1 : 0) : null, template_key || null, site_title || null, tagline || null,
        hero_heading || null, hero_subheading || null, hero_image_url || null,
        about_heading || null, about_body || null, about_image_url || null,
        typeof featured_areas_json === 'object' ? JSON.stringify(featured_areas_json) : (featured_areas_json || null),
        typeof social_links_json === 'object' ? JSON.stringify(social_links_json) : (social_links_json || null),
        seo_title || null, seo_description || null, footer_text || null, contact_cta_text || null,
        now, now
    ).run();

    await logMemberAudit(db, user_id, account_id, 'UPDATE_WEBSITE_CONFIG', 'website_config', site.id, `Member updated website template to '${template_key}'`);

    const updated = await db.prepare("SELECT * FROM sneak_website_configs WHERE site_id = ?").bind(site.id).first();

    const secret = env?.SNEAK_WEBSITE_PREVIEW_SECRET || 'dev_preview_secret_ccor_2026';
    let previewUrl = null;
    try {
        const previewToken = await createPreviewToken(site.site_key, site.id, secret, 1800);
        previewUrl = `https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/${site.site_key}?token=${encodeURIComponent(previewToken)}`;
    } catch {}

    return json({ success: true, website: updated, previewUrl });
}

/**
 * POST /api/member/website/preview-token
 */
export async function handleCreateMemberWebsitePreviewToken(db, memberContext, env) {
    const { account_id } = memberContext;
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).first();
    if (!site) return error('No site found for this account', 404);

    const secret = env?.SNEAK_WEBSITE_PREVIEW_SECRET || 'dev_preview_secret_ccor_2026';
    const previewToken = await createPreviewToken(site.site_key, site.id, secret, 1800);
    const previewUrl = `https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/${site.site_key}?token=${encodeURIComponent(previewToken)}`;

    return json({ success: true, siteKey: site.site_key, previewToken, previewUrl, expiresIn: 1800 });
}

import {
    prepareCustomHostname,
    refreshCustomHostnameStatus,
    removeCustomHostname
} from '../sneak-admin/cloudflare-saas.js';

/**
 * GET /api/member/domain-status
 */
export async function handleGetMemberDomainStatus(db, memberContext, env) {
    const { account_id } = memberContext;
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).first();
    if (!site) return error('Site not found', 404);

    const binding = await db.prepare(`
        SELECT b.*, d.verified AS sneak_verified, d.status AS domain_status
        FROM sneak_domain_bindings b
        JOIN sneak_domains d ON b.domain_id = d.id
        WHERE b.site_id = ? AND b.status != 'removed'
        ORDER BY b.created_at DESC
        LIMIT 1
    `).bind(site.id).first();

    if (!binding) {
        return json({
            connected: false,
            status: 'setup_needed',
            statusLabel: 'Setup Needed',
            domain: null,
            dnsInstructions: null
        });
    }

    let statusLabel = 'Setup Needed';
    if (binding.status === 'active' && binding.ssl_status === 'active') {
        statusLabel = 'Connected';
    } else if (binding.status === 'pending_dns') {
        statusLabel = 'Waiting for DNS';
    } else if (binding.status === 'pending_ssl' || binding.ssl_status === 'pending_validation') {
        statusLabel = 'Securing Website';
    } else if (binding.status === 'error') {
        statusLabel = 'Problem';
    }

    return json({
        connected: binding.status === 'active' && binding.ssl_status === 'active',
        bindingId: binding.id,
        domain: binding.hostname,
        status: binding.status,
        sslStatus: binding.ssl_status,
        statusLabel,
        dnsInstructions: {
            type: 'CNAME',
            name: binding.hostname.startsWith('www.') ? 'www' : binding.hostname,
            target: binding.cname_target || 'customers.sneakidx.com',
            txtVerification: binding.ownership_txt_name ? {
                name: binding.ownership_txt_name,
                value: binding.ownership_txt_value
            } : null
        },
        lastCheckedAt: binding.last_checked_at
    });
}

/**
 * POST /api/member/domains/request
 */
export async function handleRequestMemberDomain(db, memberContext, body, env) {
    const { account_id, user_id } = memberContext;
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).first();
    if (!site) return error('Site not found', 404);

    const { hostname } = body;
    if (!hostname) return error('Hostname is required', 400);

    const result = await prepareCustomHostname(db, site.id, hostname, env, `member:${user_id}`);
    if (!result.success) return error(result.error, 400);

    await logMemberAudit(db, user_id, account_id, 'REQUEST_CUSTOM_DOMAIN', 'domain_binding', result.binding.id, `Requested custom domain ${result.hostname}`);

    return json(result, 201);
}

/**
 * POST /api/member/domains/check-connection
 */
export async function handleRefreshMemberDomainStatus(db, memberContext, env) {
    const { account_id, user_id } = memberContext;
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).first();
    if (!site) return error('Site not found', 404);

    const binding = await db.prepare(`
        SELECT * FROM sneak_domain_bindings
        WHERE site_id = ? AND status != 'removed'
        ORDER BY created_at DESC
        LIMIT 1
    `).bind(site.id).first();

    if (!binding) return error('No active domain binding found to refresh', 404);

    const result = await refreshCustomHostnameStatus(db, binding.id, env, `member:${user_id}`);
    if (!result.success) return error(result.error, 400);

    return json(result);
}

/**
 * DELETE /api/member/domain-binding
 */
export async function handleRemoveMemberDomain(db, memberContext, env) {
    const { account_id, user_id } = memberContext;
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(account_id).first();
    if (!site) return error('Site not found', 404);

    const binding = await db.prepare(`
        SELECT * FROM sneak_domain_bindings
        WHERE site_id = ? AND status != 'removed'
        ORDER BY created_at DESC
        LIMIT 1
    `).bind(site.id).first();

    if (!binding) return error('No active domain binding found', 404);

    const result = await removeCustomHostname(db, binding.id, env, `member:${user_id}`);
    if (!result.success) return error(result.error, 400);

    await logMemberAudit(db, user_id, account_id, 'REMOVE_CUSTOM_DOMAIN', 'domain_binding', binding.id, `Removed custom domain ${binding.hostname}`);

    return json(result);
}

/**
 * ============================================================================
 * SNEAK MEMBER CLIENTS & ENGAGEMENT ACTIVITY API (Phase 7.3C2B)
 * ============================================================================
 */

/**
 * GET /api/member/clients?search=...&sort=...&page=...&limit=...
 * Returns paginated list of authenticated buyers across all sites owned by member's account.
 */
export async function handleListMemberClients(db, memberContext, url) {
    const { account_id } = memberContext;

    // 1. Resolve all sites belonging to authenticated account
    const sitesRes = await db.prepare("SELECT id, site_name, site_key, scope_type, scope_value FROM sneak_sites WHERE account_id = ?").bind(account_id).all();
    const sites = sitesRes.results || [];
    if (sites.length === 0) {
        return json({ success: true, clients: [], total: 0, page: 1, limit: 25, totalPages: 0 });
    }

    const siteMap = new Map(sites.map(s => [s.id, s]));
    const siteIds = sites.map(s => s.id);
    const placeholders = siteIds.map(() => '?').join(',');

    // 2. Parse query parameters
    const search = (url.searchParams.get('search') || '').trim().slice(0, 100);
    const sort = url.searchParams.get('sort') || 'recently_active';
    const page = Math.max(1, parseInt(url.searchParams.get('page'), 10) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit'), 10) || 25));
    const offset = (page - 1) * limit;

    // 3. Sorting SQL expression
    let orderBySql = 'ORDER BY COALESCE(u.last_activity_at, u.last_login_at, u.updated_at, u.created_at) DESC';
    if (sort === 'newest') {
        orderBySql = 'ORDER BY u.created_at DESC';
    } else if (sort === 'saved_homes') {
        orderBySql = 'ORDER BY favorite_count DESC, u.created_at DESC';
    } else if (sort === 'saved_searches') {
        orderBySql = 'ORDER BY saved_search_count DESC, u.created_at DESC';
    }

    // 4. Count query
    let countSql = `SELECT count(*) as total FROM sneak_consumer_users u WHERE u.site_id IN (${placeholders})`;
    const countBinds = [...siteIds];
    if (search) {
        countSql += ` AND LOWER(u.email) LIKE ?`;
        countBinds.push(`%${search.toLowerCase()}%`);
    }

    const countRow = await db.prepare(countSql).bind(...countBinds).first();
    const total = countRow?.total || 0;
    const totalPages = Math.ceil(total / limit);

    if (total === 0) {
        return json({ success: true, clients: [], total: 0, page, limit, totalPages: 0 });
    }

    // 5. Select clients with aggregate engagement counts
    let querySql = `
        SELECT 
            u.id, u.site_id, u.email, u.status, u.created_at, u.last_login_at, u.last_activity_at,
            (SELECT count(*) FROM sneak_consumer_favorites f WHERE f.user_id = u.id AND f.site_id = u.site_id) as favorite_count,
            (SELECT count(*) FROM sneak_consumer_saved_searches s WHERE s.user_id = u.id AND s.site_id = u.site_id) as saved_search_count,
            (SELECT count(*) FROM sneak_consumer_search_alerts a WHERE a.user_id = u.id AND a.site_id = u.site_id AND a.enabled = 1) as alert_count,
            (SELECT count(*) FROM sneak_leads l WHERE l.site_id = u.site_id AND LOWER(l.email) = LOWER(u.email)) as inquiry_count
        FROM sneak_consumer_users u
        WHERE u.site_id IN (${placeholders})
    `;
    const queryBinds = [...siteIds];

    if (search) {
        querySql += ` AND LOWER(u.email) LIKE ?`;
        queryBinds.push(`%${search.toLowerCase()}%`);
    }

    querySql += ` ${orderBySql} LIMIT ? OFFSET ?`;
    queryBinds.push(limit, offset);

    const rowsRes = await db.prepare(querySql).bind(...queryBinds).all();
    const clients = (rowsRes.results || []).map(r => {
        const site = siteMap.get(r.site_id);
        return {
            id: r.id,
            siteId: r.site_id,
            siteName: site?.site_name || 'Website',
            email: r.email,
            status: r.status,
            createdAt: r.created_at,
            lastLoginAt: r.last_login_at || null,
            lastActivityAt: r.last_activity_at || r.last_login_at || r.created_at,
            savedHomesCount: Number(r.favorite_count) || 0,
            savedSearchesCount: Number(r.saved_search_count) || 0,
            alertsCount: Number(r.alert_count) || 0,
            inquiriesCount: Number(r.inquiry_count) || 0
        };
    });

    return json({
        success: true,
        clients,
        total,
        page,
        limit,
        totalPages
    });
}

/**
 * GET /api/member/clients/:consumerId
 * Returns rich client profile, saved homes, saved searches, and recent inquiries.
 */
export async function handleGetMemberClientDetail(db, memberContext, consumerId) {
    const { account_id } = memberContext;

    // Strict Tenant Isolation: Verify client belongs to a site owned by member's account
    const userRow = await db.prepare(`
        SELECT u.id, u.site_id, u.email, u.status, u.created_at, u.activated_at, u.last_login_at, u.last_activity_at,
               s.site_name, s.site_key, s.scope_type, s.scope_value
        FROM sneak_consumer_users u
        JOIN sneak_sites s ON u.site_id = s.id
        WHERE u.id = ? AND s.account_id = ?
    `).bind(consumerId, account_id).first();

    if (!userRow) {
        return error('Client not found or unauthorized', 404, 'NotFound');
    }

    // 1. Current Engagement Counts
    const favCountRow = await db.prepare(`SELECT count(*) as c FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?`).bind(userRow.id, userRow.site_id).first();
    const searchCountRow = await db.prepare(`SELECT count(*) as c FROM sneak_consumer_saved_searches WHERE user_id = ? AND site_id = ?`).bind(userRow.id, userRow.site_id).first();
    const alertCountRow = await db.prepare(`SELECT count(*) as c FROM sneak_consumer_search_alerts WHERE user_id = ? AND site_id = ? AND enabled = 1`).bind(userRow.id, userRow.site_id).first();
    const inquiryCountRow = await db.prepare(`SELECT count(*) as c FROM sneak_leads WHERE site_id = ? AND LOWER(email) = LOWER(?)`).bind(userRow.site_id, userRow.email).first();

    // 2. Fetch Saved Homes (Favorites) with Display Control Enforcement
    const favRows = await db.prepare(`
        SELECT listing_key, created_at FROM sneak_consumer_favorites
        WHERE user_id = ? AND site_id = ?
        ORDER BY created_at DESC
        LIMIT 50
    `).bind(userRow.id, userRow.site_id).all();

    const keys = (favRows.results || []).map(f => f.listing_key);
    const listingMap = new Map();

    if (keys.length > 0) {
        let scopeClause = '';
        const bindings = [];

        if (userRow.scope_type === 'agent' && userRow.scope_value) {
            scopeClause = ' AND (ListAgentMlsId = ? OR ListAgentKey = ?)';
            bindings.push(userRow.scope_value, userRow.scope_value);
        } else if (userRow.scope_type === 'office' && userRow.scope_value) {
            scopeClause = ' AND (ListOfficeMlsId = ? OR ListOfficeKey = ?)';
            bindings.push(userRow.scope_value, userRow.scope_value);
        }

        for (let i = 0; i < keys.length; i += 50) {
            const chunk = keys.slice(i, i + 50);
            const placeholders = chunk.map(() => '?').join(',');
            const res = await db.prepare(`
                SELECT 
                    ListingKey, ListPrice, StandardStatus, PropertyType, PropertySubType,
                    BedroomsTotal, BathroomsTotalInteger, LivingArea, LotSizeAcres,
                    City, PostalCode, UnparsedAddress, PrimaryPhoto,
                    InternetEntireListingDisplayYN, InternetAddressDisplayYN
                FROM sneak_listings
                WHERE ListingKey IN (${placeholders})
                  AND (InternetEntireListingDisplayYN = 1 OR InternetEntireListingDisplayYN IS NULL)
                  ${scopeClause}
            `).bind(...chunk, ...bindings).all();

            for (const row of res.results || []) {
                listingMap.set(row.ListingKey, row);
            }
        }
    }

    const savedHomes = (favRows.results || []).map(f => {
        const listing = listingMap.get(f.listing_key);
        if (!listing) {
            return {
                listingKey: f.listing_key,
                createdAt: f.created_at,
                unavailable: true
            };
        }

        const isAddressSuppressed = (listing.InternetAddressDisplayYN === 0);
        return {
            listingKey: listing.ListingKey,
            createdAt: f.created_at,
            unavailable: false,
            price: listing.ListPrice,
            address: isAddressSuppressed ? 'Address Undisclosed' : (listing.UnparsedAddress || 'Address Undisclosed'),
            city: listing.City || '',
            postalCode: listing.PostalCode || '',
            propertyType: listing.PropertyType || 'Residential',
            propertySubType: listing.PropertySubType || '',
            bedrooms: listing.BedroomsTotal || null,
            bathrooms: listing.BathroomsTotalInteger || null,
            livingArea: listing.LivingArea || null,
            status: listing.StandardStatus || 'Active',
            primaryPhoto: listing.PrimaryPhoto || null
        };
    });

    // 3. Fetch Saved Searches & Alerts
    const searchRows = await db.prepare(`
        SELECT 
            s.id, s.name, s.created_at, s.updated_at,
            a.frequency, a.enabled, a.enabled_at
        FROM sneak_consumer_saved_searches s
        LEFT JOIN sneak_consumer_search_alerts a ON s.id = a.saved_search_id
        WHERE s.user_id = ? AND s.site_id = ?
        ORDER BY s.updated_at DESC
        LIMIT 25
    `).bind(userRow.id, userRow.site_id).all();

    const savedSearches = (searchRows.results || []).map(s => ({
        id: s.id,
        name: s.name,
        alertFrequency: s.frequency || 'off',
        alertEnabled: Boolean(s.enabled),
        enabledAt: s.enabled_at || null,
        createdAt: s.created_at,
        updatedAt: s.updated_at
    }));

    // 4. Fetch Associated Inquiries
    const leadRows = await db.prepare(`
        SELECT id, listing_key, lead_type, name, email, phone, message, created_at
        FROM sneak_leads
        WHERE site_id = ? AND LOWER(email) = LOWER(?)
        ORDER BY created_at DESC
        LIMIT 20
    `).bind(userRow.site_id, userRow.email).all();

    const inquiries = (leadRows.results || []).map(l => ({
        id: l.id,
        listingKey: l.listing_key || null,
        leadType: l.lead_type || 'property_inquiry',
        name: l.name || '',
        email: l.email || '',
        phone: l.phone || null,
        message: l.message || '',
        createdAt: l.created_at
    }));

    return json({
        success: true,
        client: {
            id: userRow.id,
            siteId: userRow.site_id,
            siteName: userRow.site_name,
            email: userRow.email,
            status: userRow.status,
            createdAt: userRow.created_at,
            activatedAt: userRow.activated_at || null,
            lastLoginAt: userRow.last_login_at || null,
            lastActivityAt: userRow.last_activity_at || userRow.last_login_at || userRow.created_at
        },
        engagement: {
            savedHomesCount: favCountRow?.c || 0,
            savedSearchesCount: searchCountRow?.c || 0,
            alertsCount: alertCountRow?.c || 0,
            inquiriesCount: inquiryCountRow?.c || 0
        },
        savedHomes,
        savedSearches,
        inquiries
    });
}

/**
 * GET /api/member/clients/:consumerId/activity?page=...&limit=...
 * Returns verified longitudinal buyer activity timeline.
 */
export async function handleGetMemberClientActivity(db, memberContext, consumerId, url) {
    const { account_id } = memberContext;

    // Strict Tenant Isolation
    const userRow = await db.prepare(`
        SELECT u.id, u.site_id, u.email, s.scope_type, s.scope_value
        FROM sneak_consumer_users u
        JOIN sneak_sites s ON u.site_id = s.id
        WHERE u.id = ? AND s.account_id = ?
    `).bind(consumerId, account_id).first();

    if (!userRow) {
        return error('Client not found or unauthorized', 404, 'NotFound');
    }

    const page = Math.max(1, parseInt(url.searchParams.get('page'), 10) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get('limit'), 10) || 50));
    const offset = (page - 1) * limit;

    const countRow = await db.prepare(`
        SELECT count(*) as total FROM sneak_consumer_activity_events
        WHERE user_id = ? AND site_id = ?
    `).bind(userRow.id, userRow.site_id).first();

    const total = countRow?.total || 0;
    const totalPages = Math.ceil(total / limit);

    if (total === 0) {
        return json({ success: true, events: [], total: 0, page, limit, totalPages: 0 });
    }

    const eventsRes = await db.prepare(`
        SELECT id, event_type, listing_key, saved_search_id, lead_id, metadata_json, created_at
        FROM sneak_consumer_activity_events
        WHERE user_id = ? AND site_id = ?
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    `).bind(userRow.id, userRow.site_id, limit, offset).all();

    const rawEvents = eventsRes.results || [];

    // Collect listing keys to resolve safe display data
    const listingKeys = [...new Set(rawEvents.map(e => e.listing_key).filter(Boolean))];
    const listingMap = new Map();

    if (listingKeys.length > 0) {
        let scopeClause = '';
        const bindings = [];

        if (userRow.scope_type === 'agent' && userRow.scope_value) {
            scopeClause = ' AND (ListAgentMlsId = ? OR ListAgentKey = ?)';
            bindings.push(userRow.scope_value, userRow.scope_value);
        } else if (userRow.scope_type === 'office' && userRow.scope_value) {
            scopeClause = ' AND (ListOfficeMlsId = ? OR ListOfficeKey = ?)';
            bindings.push(userRow.scope_value, userRow.scope_value);
        }

        for (let i = 0; i < listingKeys.length; i += 50) {
            const chunk = listingKeys.slice(i, i + 50);
            const placeholders = chunk.map(() => '?').join(',');
            const res = await db.prepare(`
                SELECT 
                    ListingKey, ListPrice, City, UnparsedAddress, PrimaryPhoto,
                    InternetEntireListingDisplayYN, InternetAddressDisplayYN
                FROM sneak_listings
                WHERE ListingKey IN (${placeholders})
                  AND (InternetEntireListingDisplayYN = 1 OR InternetEntireListingDisplayYN IS NULL)
                  ${scopeClause}
            `).bind(...chunk, ...bindings).all();

            for (const row of res.results || []) {
                listingMap.set(row.ListingKey, row);
            }
        }
    }

    const events = rawEvents.map(e => {
        let metadata = null;
        try {
            metadata = e.metadata_json ? JSON.parse(e.metadata_json) : null;
        } catch {}

        let listing = null;
        if (e.listing_key) {
            const l = listingMap.get(e.listing_key);
            if (l) {
                const isAddressSuppressed = (l.InternetAddressDisplayYN === 0);
                listing = {
                    listingKey: l.ListingKey,
                    price: l.ListPrice,
                    address: isAddressSuppressed ? 'Address Undisclosed' : (l.UnparsedAddress || 'Address Undisclosed'),
                    city: l.City || '',
                    primaryPhoto: l.PrimaryPhoto || null
                };
            }
        }

        return {
            id: e.id,
            type: e.event_type,
            createdAt: e.created_at,
            listingKey: e.listing_key || null,
            listing,
            savedSearchId: e.saved_search_id || null,
            searchName: metadata?.name || null,
            leadId: e.lead_id || null,
            metadata: metadata ? { frequency: metadata.frequency, name: metadata.name } : null
        };
    });

    return json({
        success: true,
        events,
        total,
        page,
        limit,
        totalPages
    });
}


