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
export async function handleMemberOverview(db, memberContext) {
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
        embed = generateEmbedSnippets(primarySite.site_key, allowed, branding || {});
    }

    const billing = await db.prepare("SELECT * FROM sneak_account_billing WHERE account_id = ?").bind(account_id).first();
    const listingsCount = await db.prepare("SELECT count(*) as count FROM sneak_listings").first();
    const openHousesCount = await db.prepare("SELECT count(*) as count FROM sneak_open_houses").first();

    const leadsCount = await db.prepare(`
        SELECT count(*) as count
        FROM sneak_leads l
        JOIN sneak_sites s ON l.site_id = s.id
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
        billing: billing || {
            billing_status: 'none',
            entitlement_status: 'inactive'
        },
        inventory: {
            activeListings: listingsCount?.count || 0,
            futureOpenHouses: openHousesCount?.count || 0
        },
        leadsCount: leadsCount?.count || 0
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
export async function handleGetMemberEmbed(db, memberContext) {
    const { account_id } = memberContext;
    const site = await db.prepare("SELECT id, site_key FROM sneak_sites WHERE account_id = ? LIMIT 1").bind(account_id).first();
    if (!site) return error('No site configured', 404);

    const domains = await db.prepare("SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1").bind(site.id).all();
    const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(site.id).first();

    const allowed = (domains.results || []).map(d => d.domain);
    const embed = generateEmbedSnippets(site.site_key, allowed, branding || {});

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
        status: entitlement?.status || 'active',
        isEntitled: entitlement ? entitlement.isEntitled : true,
        graceUntil: entitlement?.graceUntil || null,
        growthzoneUrl: 'https://bonitaspringsesterorealtorsfl.growthzoneapp.com/',
        instructions: 'Your SNEAK IDX subscription is managed by Bonita Springs-Estero REALTORS® through GrowthZone. Billing occurs automatically on the first of each month.'
    });
}
