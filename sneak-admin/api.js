/**
 * sneak-admin/api.js
 * 
 * Admin API Handlers for Member Provisioning, Site Management, and Embeds.
 */

import {
    normalizeSiteKey,
    validateDomainSafety,
    validateHexColor,
    validateUrl,
    checkMlsInventory,
    VALID_PLANS,
    VALID_STATUSES,
    VALID_SCOPE_TYPES,
    VALID_WIDGET_TYPES
} from './validation.js';
import { generateEmbedSnippets } from './embed-generator.js';

function json(data, status = 200) {
    return new Response(JSON.stringify(data), {
        status,
        headers: { 'Content-Type': 'application/json' }
    });
}

function error(message, status = 400, code = 'BadRequest') {
    return json({ error: code, message }, status);
}

async function logAudit(db, actor, action, entityType, entityId, summary) {
    try {
        const id = `audit_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_admin_audit (id, admin_actor, action, entity_type, entity_id, summary, created_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        `).bind(id, actor, action, entityType, entityId, summary).run();
    } catch (err) {
        console.error('[AUDIT LOG ERROR]', err.message);
    }
}

/**
 * GET /api/admin/dashboard
 */
export async function handleDashboard(db) {
    const accountCounts = await db.prepare(`
        SELECT 
            count(*) as total,
            sum(case when status = 'active' then 1 else 0 end) as active,
            sum(case when status = 'suspended' then 1 else 0 end) as suspended
        FROM sneak_accounts
    `).first();

    const siteCounts = await db.prepare(`
        SELECT 
            count(*) as total,
            sum(case when scope_type = 'agent' then 1 else 0 end) as agent_sites,
            sum(case when scope_type = 'office' then 1 else 0 end) as office_sites,
            sum(case when scope_type = 'market' then 1 else 0 end) as market_sites
        FROM sneak_sites
    `).first();

    const domainCount = await db.prepare(
        "SELECT count(*) as total FROM sneak_domains WHERE status = 'active' AND verified = 1"
    ).first();

    const listingsCount = await db.prepare("SELECT count(*) as count FROM sneak_listings").first();
    const openHousesCount = await db.prepare("SELECT count(*) as count FROM sneak_open_houses").first();

    const syncStates = await db.prepare("SELECT * FROM sneak_sync_state").all();
    const recentSyncRuns = await db.prepare(
        "SELECT * FROM sneak_sync_runs ORDER BY started_at DESC LIMIT 5"
    ).all();

    const recentLeads = await db.prepare(`
        SELECT l.*, s.site_name, a.account_name
        FROM sneak_leads l
        JOIN sneak_sites s ON l.site_id = s.id
        JOIN sneak_accounts a ON s.account_id = a.id
        ORDER BY l.created_at DESC LIMIT 10
    `).all();

    const recentAudit = await db.prepare(
        "SELECT * FROM sneak_admin_audit ORDER BY created_at DESC LIMIT 10"
    ).all();

    return json({
        accounts: {
            total: accountCounts?.total || 0,
            active: accountCounts?.active || 0,
            suspended: accountCounts?.suspended || 0
        },
        sites: {
            total: siteCounts?.total || 0,
            agentSites: siteCounts?.agent_sites || 0,
            officeSites: siteCounts?.office_sites || 0,
            marketSites: siteCounts?.market_sites || 0
        },
        authorizedDomains: domainCount?.total || 0,
        inventory: {
            activeListings: listingsCount?.count || 0,
            futureOpenHouses: openHousesCount?.count || 0
        },
        sync: {
            states: syncStates.results || [],
            recentRuns: recentSyncRuns.results || []
        },
        recentLeads: recentLeads.results || [],
        recentAudit: recentAudit.results || []
    });
}

/**
 * GET /api/admin/accounts
 */
export async function handleListAccounts(db, url) {
    const q = url.searchParams.get('q') || '';
    const status = url.searchParams.get('status') || '';
    const plan = url.searchParams.get('plan') || '';

    let where = "WHERE 1=1";
    const binds = [];

    if (q) {
        where += " AND (a.account_name LIKE ? OR a.member_id LIKE ? OR a.agent_mls_id LIKE ? OR a.office_mls_id LIKE ?)";
        const pattern = `%${q}%`;
        binds.push(pattern, pattern, pattern, pattern);
    }
    if (status && VALID_STATUSES.has(status)) {
        where += " AND a.status = ?";
        binds.push(status);
    }
    if (plan && VALID_PLANS.has(plan)) {
        where += " AND a.plan = ?";
        binds.push(plan);
    }

    const query = `
        SELECT 
            a.*,
            count(s.id) as site_count
        FROM sneak_accounts a
        LEFT JOIN sneak_sites s ON a.id = s.account_id
        ${where}
        GROUP BY a.id
        ORDER BY a.created_at DESC
    `;

    const res = await db.prepare(query).bind(...binds).all();
    return json({ accounts: res.results || [] });
}

/**
 * POST /api/admin/accounts (Atomic Provisioning)
 */
export async function handleCreateAccount(db, body, actor) {
    const {
        account_name,
        member_id,
        plan = 'standard',
        status = 'active',
        scope_type = 'market',
        scope_value = '',
        site_name,
        site_key,
        domain = '',
        branding = {},
        override_mls_warning = false
    } = body;

    if (!account_name || typeof account_name !== 'string' || account_name.trim().length === 0) {
        return error('Account name is required.');
    }
    if (!VALID_PLANS.has(plan)) return error(`Invalid plan. Must be one of: ${[...VALID_PLANS].join(', ')}`);
    if (!VALID_STATUSES.has(status)) return error(`Invalid status. Must be one of: ${[...VALID_STATUSES].join(', ')}`);
    if (!VALID_SCOPE_TYPES.has(scope_type)) return error(`Invalid scope_type.`);

    if ((scope_type === 'agent' || scope_type === 'office') && !scope_value) {
        return error(`MLS ID is required for ${scope_type} scope.`);
    }

    // MLS Inventory Validation
    if (scope_type !== 'market' && scope_value) {
        const mlsCheck = await checkMlsInventory(db, scope_type, scope_value);
        if (!mlsCheck.valid && !override_mls_warning) {
            return json({
                error: 'MlsIdNotFound',
                message: `No active listings found in current MLS feed for ${scope_type} ID '${scope_value}'.`,
                mlsCheck,
                canOverride: true
            }, 422);
        }
    }

    // Determine normalized site_key
    let finalSiteKey = normalizeSiteKey(site_key || site_name || account_name);
    if (!finalSiteKey) finalSiteKey = `site-${Date.now().toString(36)}`;

    // Check collision and make unique if needed
    const existingKey = await db.prepare("SELECT id FROM sneak_sites WHERE site_key = ?").bind(finalSiteKey).first();
    if (existingKey) {
        finalSiteKey = `${finalSiteKey}-${Math.random().toString(36).slice(2, 6)}`;
    }

    // Domain validation if provided
    let cleanDomain = null;
    if (domain) {
        const domainVal = validateDomainSafety(domain);
        if (!domainVal.valid) {
            return error(domainVal.error);
        }
        cleanDomain = domainVal.domain;
    }

    // Generate IDs
    const accountId = `acc_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const siteId = `site_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const finalSiteName = site_name || account_name;

    const agentMlsId = scope_type === 'agent' ? scope_value : (body.agent_mls_id || null);
    const officeMlsId = scope_type === 'office' ? scope_value : (body.office_mls_id || null);

    const statements = [
        // 1. Account
        db.prepare(`
            INSERT INTO sneak_accounts (id, member_id, account_name, agent_mls_id, office_mls_id, status, plan, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        `).bind(accountId, member_id || null, account_name.trim(), agentMlsId, officeMlsId, status, plan),

        // 2. Site
        db.prepare(`
            INSERT INTO sneak_sites (id, account_id, site_key, site_name, status, scope_type, scope_value, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        `).bind(siteId, accountId, finalSiteKey, finalSiteName, status, scope_type, scope_value || null),

        // 3. Branding
        db.prepare(`
            INSERT INTO sneak_branding (site_id, display_name, brokerage, logo_url, agent_photo_url, primary_color, secondary_color, phone, email, website_url, config_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).bind(
            siteId,
            branding.display_name || finalSiteName,
            branding.brokerage || 'Premier Realty',
            branding.logo_url || null,
            branding.agent_photo_url || null,
            branding.primary_color || '#1a365d',
            branding.secondary_color || '#2596be',
            branding.phone || null,
            branding.email || null,
            branding.website_url || null,
            branding.config_json || null
        ),

        // 4. Default Widget Configs
        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'search', 1, '{}')").bind(`w_${siteId}_search`, siteId),
        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'search_bar', 1, '{}')").bind(`w_${siteId}_sb`, siteId),
        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'listing_grid', 1, '{}')").bind(`w_${siteId}_grid`, siteId),
        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'open_houses', 1, '{}')").bind(`w_${siteId}_oh`, siteId)
    ];

    // Optional Domain
    if (cleanDomain) {
        const domainId = `dom_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        statements.push(
            db.prepare(`
                INSERT INTO sneak_domains (id, site_id, domain, verified, status, created_at)
                VALUES (?, ?, ?, 1, 'active', datetime('now'))
            `).bind(domainId, siteId, cleanDomain)
        );
    }

    // Execute atomic batch
    await db.batch(statements);

    await logAudit(db, actor, 'CREATE_ACCOUNT', 'account', accountId, `Provisioned account '${account_name}' with site '${finalSiteKey}' (${scope_type})`);

    const embed = generateEmbedSnippets(finalSiteKey, cleanDomain ? [cleanDomain] : [], branding);

    return json({
        success: true,
        account: {
            id: accountId,
            member_id,
            account_name,
            status,
            plan
        },
        site: {
            id: siteId,
            site_key: finalSiteKey,
            site_name: finalSiteName,
            scope_type,
            scope_value,
            status
        },
        domain: cleanDomain,
        embed
    }, 201);
}

/**
 * GET /api/admin/accounts/:id
 */
export async function handleGetAccount(db, accountId) {
    const account = await db.prepare("SELECT * FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    if (!account) return error('Account not found', 404);

    const sites = await db.prepare("SELECT * FROM sneak_sites WHERE account_id = ?").bind(accountId).all();
    const siteList = [];

    for (const site of (sites.results || [])) {
        const domains = await db.prepare("SELECT * FROM sneak_domains WHERE site_id = ?").bind(site.id).all();
        const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(site.id).first();
        const widgets = await db.prepare("SELECT * FROM sneak_widget_configs WHERE site_id = ?").bind(site.id).all();
        const leads = await db.prepare("SELECT * FROM sneak_leads WHERE site_id = ? ORDER BY created_at DESC LIMIT 10").bind(site.id).all();

        const allowedDomains = (domains.results || []).filter(d => d.status === 'active' && d.verified === 1).map(d => d.domain);
        const embed = generateEmbedSnippets(site.site_key, allowedDomains, branding || {});

        siteList.push({
            ...site,
            domains: domains.results || [],
            branding: branding || null,
            widgets: widgets.results || [],
            leads: leads.results || [],
            embed
        });
    }

    return json({
        account,
        sites: siteList
    });
}

/**
 * PATCH /api/admin/accounts/:id
 */
export async function handleUpdateAccount(db, accountId, body, actor) {
    const account = await db.prepare("SELECT * FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    if (!account) return error('Account not found', 404);

    const { account_name, member_id, plan, status } = body;
    const updates = [];
    const binds = [];

    if (account_name !== undefined) {
        if (!account_name.trim()) return error('Account name cannot be empty');
        updates.push("account_name = ?");
        binds.push(account_name.trim());
    }
    if (member_id !== undefined) {
        updates.push("member_id = ?");
        binds.push(member_id || null);
    }
    if (plan !== undefined) {
        if (!VALID_PLANS.has(plan)) return error('Invalid plan');
        updates.push("plan = ?");
        binds.push(plan);
    }
    if (status !== undefined) {
        if (!VALID_STATUSES.has(status)) return error('Invalid status');
        updates.push("status = ?");
        binds.push(status);
    }

    if (updates.length === 0) return json({ success: true, account });

    updates.push("updated_at = datetime('now')");
    binds.push(accountId);

    await db.prepare(`UPDATE sneak_accounts SET ${updates.join(', ')} WHERE id = ?`).bind(...binds).run();

    await logAudit(db, actor, 'UPDATE_ACCOUNT', 'account', accountId, `Updated account: ${JSON.stringify(body)}`);

    const updated = await db.prepare("SELECT * FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    return json({ success: true, account: updated });
}

/**
 * POST /api/admin/accounts/:id/sites
 */
export async function handleCreateSite(db, accountId, body, actor) {
    const account = await db.prepare("SELECT * FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    if (!account) return error('Account not found', 404);

    const {
        site_name,
        site_key,
        scope_type = 'market',
        scope_value = '',
        status = 'active',
        override_mls_warning = false
    } = body;

    if (!site_name || !site_name.trim()) return error('Site name is required.');
    if (!VALID_SCOPE_TYPES.has(scope_type)) return error('Invalid scope type.');
    if ((scope_type === 'agent' || scope_type === 'office') && !scope_value) {
        return error(`MLS ID required for ${scope_type} scope.`);
    }

    // MLS ID Validation
    if (scope_type !== 'market' && scope_value) {
        const mlsCheck = await checkMlsInventory(db, scope_type, scope_value);
        if (!mlsCheck.valid && !override_mls_warning) {
            return json({
                error: 'MlsIdNotFound',
                message: `No active listings found for ${scope_type} ID '${scope_value}'.`,
                mlsCheck,
                canOverride: true
            }, 422);
        }
    }

    let finalSiteKey = normalizeSiteKey(site_key || site_name);
    if (!finalSiteKey) finalSiteKey = `site-${Date.now().toString(36)}`;

    const existingKey = await db.prepare("SELECT id FROM sneak_sites WHERE site_key = ?").bind(finalSiteKey).first();
    if (existingKey) {
        finalSiteKey = `${finalSiteKey}-${Math.random().toString(36).slice(2, 6)}`;
    }

    const siteId = `site_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;

    const statements = [
        db.prepare(`
            INSERT INTO sneak_sites (id, account_id, site_key, site_name, status, scope_type, scope_value, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        `).bind(siteId, accountId, finalSiteKey, site_name.trim(), status, scope_type, scope_value || null),

        db.prepare(`
            INSERT INTO sneak_branding (site_id, display_name, brokerage, primary_color, secondary_color)
            VALUES (?, ?, 'Premier Realty', '#1a365d', '#2596be')
        `).bind(siteId, site_name.trim()),

        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'search', 1, '{}')").bind(`w_${siteId}_search`, siteId),
        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'search_bar', 1, '{}')").bind(`w_${siteId}_sb`, siteId),
        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'listing_grid', 1, '{}')").bind(`w_${siteId}_grid`, siteId),
        db.prepare("INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json) VALUES (?, ?, 'open_houses', 1, '{}')").bind(`w_${siteId}_oh`, siteId)
    ];

    await db.batch(statements);

    await logAudit(db, actor, 'CREATE_SITE', 'site', siteId, `Created site '${finalSiteKey}' on account ${accountId}`);

    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    return json({ success: true, site }, 201);
}

/**
 * PATCH /api/admin/sites/:id
 */
export async function handleUpdateSite(db, siteId, body, actor) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

    const { site_name, status, scope_type, scope_value, override_mls_warning } = body;
    const updates = [];
    const binds = [];

    if (site_name !== undefined) {
        if (!site_name.trim()) return error('Site name cannot be empty');
        updates.push("site_name = ?");
        binds.push(site_name.trim());
    }
    if (status !== undefined) {
        if (!VALID_STATUSES.has(status)) return error('Invalid status');
        updates.push("status = ?");
        binds.push(status);
    }
    if (scope_type !== undefined) {
        if (!VALID_SCOPE_TYPES.has(scope_type)) return error('Invalid scope type');
        if ((scope_type === 'agent' || scope_type === 'office') && !scope_value) {
            return error(`MLS ID required for ${scope_type} scope.`);
        }
        if (scope_type !== 'market' && scope_value) {
            const mlsCheck = await checkMlsInventory(db, scope_type, scope_value);
            if (!mlsCheck.valid && !override_mls_warning) {
                return json({
                    error: 'MlsIdNotFound',
                    message: `No active listings found for ${scope_type} ID '${scope_value}'.`,
                    mlsCheck,
                    canOverride: true
                }, 422);
            }
        }
        updates.push("scope_type = ?");
        binds.push(scope_type);
        updates.push("scope_value = ?");
        binds.push(scope_value || null);
    }

    if (updates.length === 0) return json({ success: true, site });

    updates.push("updated_at = datetime('now')");
    binds.push(siteId);

    await db.prepare(`UPDATE sneak_sites SET ${updates.join(', ')} WHERE id = ?`).bind(...binds).run();

    await logAudit(db, actor, 'UPDATE_SITE', 'site', siteId, `Updated site: ${JSON.stringify(body)}`);

    const updated = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    return json({ success: true, site: updated });
}

/**
 * GET /api/admin/sites/:id/domains
 */
export async function handleListDomains(db, siteId) {
    const res = await db.prepare("SELECT * FROM sneak_domains WHERE site_id = ? ORDER BY created_at ASC").bind(siteId).all();
    return json({ domains: res.results || [] });
}

/**
 * POST /api/admin/sites/:id/domains
 */
export async function handleAddDomain(db, siteId, body, actor) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

    const { domain, verified = 1, status = 'active' } = body;
    const domainVal = validateDomainSafety(domain);
    if (!domainVal.valid) return error(domainVal.error);

    const domainId = `dom_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;

    await db.prepare(`
        INSERT INTO sneak_domains (id, site_id, domain, verified, status, created_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    `).bind(domainId, siteId, domainVal.domain, verified ? 1 : 0, status).run();

    await logAudit(db, actor, 'ADD_DOMAIN', 'domain', domainId, `Added domain '${domainVal.domain}' to site ${siteId}`);

    const newDomain = await db.prepare("SELECT * FROM sneak_domains WHERE id = ?").bind(domainId).first();
    return json({ success: true, domain: newDomain }, 201);
}

/**
 * PATCH /api/admin/domains/:id
 */
export async function handleUpdateDomain(db, domainId, body, actor) {
    const existing = await db.prepare("SELECT * FROM sneak_domains WHERE id = ?").bind(domainId).first();
    if (!existing) return error('Domain not found', 404);

    const { verified, status } = body;
    const updates = [];
    const binds = [];

    if (verified !== undefined) {
        updates.push("verified = ?");
        binds.push(verified ? 1 : 0);
    }
    if (status !== undefined) {
        if (!['active', 'disabled'].includes(status)) return error('Invalid domain status');
        updates.push("status = ?");
        binds.push(status);
    }

    if (updates.length === 0) return json({ success: true, domain: existing });

    binds.push(domainId);
    await db.prepare(`UPDATE sneak_domains SET ${updates.join(', ')} WHERE id = ?`).bind(...binds).run();

    await logAudit(db, actor, 'UPDATE_DOMAIN', 'domain', domainId, `Updated domain status: ${JSON.stringify(body)}`);

    const updated = await db.prepare("SELECT * FROM sneak_domains WHERE id = ?").bind(domainId).first();
    return json({ success: true, domain: updated });
}

/**
 * DELETE /api/admin/domains/:id
 */
export async function handleDeleteDomain(db, domainId, actor) {
    const existing = await db.prepare("SELECT * FROM sneak_domains WHERE id = ?").bind(domainId).first();
    if (!existing) return error('Domain not found', 404);

    await db.prepare("DELETE FROM sneak_domains WHERE id = ?").bind(domainId).run();
    await logAudit(db, actor, 'DELETE_DOMAIN', 'domain', domainId, `Deleted domain '${existing.domain}'`);

    return json({ success: true });
}

/**
 * GET /api/admin/sites/:id/branding
 */
export async function handleGetBranding(db, siteId) {
    const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(siteId).first();
    return json({ branding: branding || null });
}

/**
 * PUT /api/admin/sites/:id/branding
 */
export async function handleUpdateBranding(db, siteId, body, actor) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

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
        siteId,
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

    await logAudit(db, actor, 'UPDATE_BRANDING', 'site', siteId, `Updated branding for site ${site.site_key}`);

    const updated = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(siteId).first();
    return json({ success: true, branding: updated });
}

/**
 * GET /api/admin/sites/:id/widgets
 */
export async function handleGetWidgets(db, siteId) {
    const res = await db.prepare("SELECT * FROM sneak_widget_configs WHERE site_id = ?").bind(siteId).all();
    return json({ widgets: res.results || [] });
}

/**
 * PUT /api/admin/sites/:id/widgets/:type
 */
export async function handleUpdateWidget(db, siteId, widgetType, body, actor) {
    if (!VALID_WIDGET_TYPES.has(widgetType)) return error(`Invalid widget type: ${widgetType}`);

    const { enabled = 1, config_json = {} } = body;
    const configStr = typeof config_json === 'object' ? JSON.stringify(config_json) : String(config_json || '{}');
    const id = `w_${siteId}_${widgetType}`;

    await db.prepare(`
        INSERT OR REPLACE INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json, created_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    `).bind(id, siteId, widgetType, enabled ? 1 : 0, configStr).run();

    await logAudit(db, actor, 'UPDATE_WIDGET', 'site', siteId, `Updated widget ${widgetType} (enabled: ${Boolean(enabled)})`);

    const updated = await db.prepare("SELECT * FROM sneak_widget_configs WHERE id = ?").bind(id).first();
    return json({ success: true, widget: updated });
}

/**
 * GET /api/admin/sites/:id/embed
 */
export async function handleGetEmbed(db, siteId) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

    const domains = await db.prepare("SELECT domain FROM sneak_domains WHERE site_id = ? AND status = 'active' AND verified = 1").bind(siteId).all();
    const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(siteId).first();

    const allowed = (domains.results || []).map(d => d.domain);
    const embed = generateEmbedSnippets(site.site_key, allowed, branding || {});

    return json(embed);
}

/**
 * GET /api/admin/validate-mls
 */
export async function handleValidateMls(db, url) {
    const type = url.searchParams.get('type') || 'agent';
    const mlsId = url.searchParams.get('mlsId') || '';

    if (!mlsId) return error('mlsId query param is required');
    if (!['agent', 'office'].includes(type)) return error('type must be agent or office');

    const result = await checkMlsInventory(db, type, mlsId);
    return json(result);
}
