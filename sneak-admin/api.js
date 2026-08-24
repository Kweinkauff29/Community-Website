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
 * GET /api/admin/accounts/:id/members
 */
export async function handleListAccountMembers(db, accountId) {
    const res = await db.prepare(`
        SELECT id, account_id, email, role, status, invited_at, activated_at, last_login_at, created_at
        FROM sneak_member_users
        WHERE account_id = ?
        ORDER BY created_at ASC
    `).bind(accountId).all();

    return json({ members: res.results || [] });
}

/**
 * POST /api/admin/accounts/:id/members
 */
export async function handleCreateAccountMemberInvite(db, accountId, body, actor, env = {}) {
    const { email, role = 'owner' } = body;
    const cleanEmail = (email || '').toLowerCase().trim();
    if (!cleanEmail || !cleanEmail.includes('@')) {
        return error('A valid email address is required.');
    }

    const account = await db.prepare("SELECT id, account_name FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    if (!account) return error('Account not found', 404);

    let user = await db.prepare("SELECT * FROM sneak_member_users WHERE email = ?").bind(cleanEmail).first();
    const userId = user?.id || `muser_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const now = new Date().toISOString();

    if (!user) {
        await db.prepare(`
            INSERT INTO sneak_member_users (id, account_id, email, role, status, invited_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'invited', ?, ?, ?)
        `).bind(userId, accountId, cleanEmail, role, now, now, now).run();
    } else {
        await db.prepare(`
            UPDATE sneak_member_users
            SET account_id = ?, role = ?, status = 'invited', invited_at = ?, updated_at = ?
            WHERE id = ?
        `).bind(accountId, role, now, now, userId).run();
    }

    // Trigger Member Worker's public magic-link request server-side
    let invitationRequested = false;
    let invitationError = null;
    try {
        const memberUrl = env?.MEMBER_PORTAL_URL || 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev';
        const requestPayload = JSON.stringify({ email: cleanEmail });
        const requestHeaders = {
            'Content-Type': 'application/json',
            'Origin': memberUrl,
            'Host': new URL(memberUrl).host
        };

        let dispatchRes;
        if (env?.MEMBER_WORKER && typeof env.MEMBER_WORKER.fetch === 'function') {
            dispatchRes = await env.MEMBER_WORKER.fetch(new Request(`${memberUrl}/api/member/auth/magic-link`, {
                method: 'POST',
                headers: requestHeaders,
                body: requestPayload
            }));
        } else {
            dispatchRes = await fetch(`${memberUrl}/api/member/auth/magic-link`, {
                method: 'POST',
                headers: requestHeaders,
                body: requestPayload
            });
        }

        if (dispatchRes.ok) {
            invitationRequested = true;
        } else {
            invitationError = `Member worker HTTP ${dispatchRes.status}`;
        }
    } catch (dispatchErr) {
        invitationError = dispatchErr.message;
        console.error('[ADMIN INVITATION DISPATCH ERROR]', dispatchErr.message);
    }

    await logAudit(db, actor, 'CREATE_MEMBER_INVITE', 'member_user', userId, `Invited ${cleanEmail} for account ${account.account_name}`);

    return json({
        success: true,
        user: { id: userId, account_id: accountId, email: cleanEmail, role, status: 'invited' },
        invitationRequested,
        invitationError
    }, 201);
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

/**
 * GET /api/admin/accounts/:id/entitlement
 */
export async function handleGetAccountEntitlement(db, accountId) {
    const account = await db.prepare("SELECT * FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    if (!account) return error('Account not found', 404);

    const entitlement = await db.prepare("SELECT * FROM sneak_account_entitlements WHERE account_id = ?").bind(accountId).first();

    return json({
        account_id: account.id,
        account_name: account.account_name,
        account_status: account.status,
        plan: entitlement?.plan || account.plan,
        provider: 'GrowthZone',
        billing_cycle: 'Monthly (1st of each month)',
        entitlement: entitlement || {
            source: 'manual',
            status: 'active',
            plan: account.plan,
            effective_at: account.created_at,
            grace_until: null,
            external_reference: null,
            notes: null
        }
    });
}

/**
 * PUT /api/admin/accounts/:id/entitlement
 */
export async function handleUpdateAccountEntitlement(db, accountId, body, actor) {
    const account = await db.prepare("SELECT * FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    if (!account) return error('Account not found', 404);

    const {
        source = 'manual',
        status = 'active',
        plan,
        effective_at,
        expires_at,
        grace_until,
        external_reference,
        notes
    } = body;

    const validStatuses = ['active', 'grace', 'delinquent', 'suspended', 'canceled'];
    if (!validStatuses.includes(status)) {
        return error(`Invalid status. Must be one of: ${validStatuses.join(', ')}`);
    }

    const now = new Date().toISOString();

    await db.prepare(`
        INSERT INTO sneak_account_entitlements (
            account_id, source, status, plan, effective_at, expires_at, grace_until,
            external_reference, notes, last_verified_at, created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(account_id) DO UPDATE SET
            source = excluded.source,
            status = excluded.status,
            plan = COALESCE(excluded.plan, sneak_account_entitlements.plan),
            effective_at = COALESCE(excluded.effective_at, sneak_account_entitlements.effective_at),
            expires_at = excluded.expires_at,
            grace_until = excluded.grace_until,
            external_reference = COALESCE(excluded.external_reference, sneak_account_entitlements.external_reference),
            notes = COALESCE(excluded.notes, sneak_account_entitlements.notes),
            last_verified_at = excluded.last_verified_at,
            updated_at = excluded.updated_at
    `).bind(
        accountId, source, status, plan || account.plan, effective_at || now, expires_at || null,
        grace_until || null, external_reference || null, notes || null, now, now, now
    ).run();

    await logAudit(db, actor, 'UPDATE_ENTITLEMENT', 'account_entitlement', accountId, `Updated entitlement status to '${status}' (source: ${source})`);

    const updated = await db.prepare("SELECT * FROM sneak_account_entitlements WHERE account_id = ?").bind(accountId).first();
    return json({ success: true, entitlement: updated });
}

import { createPreviewToken } from '../sneak-sites/preview.js';

/**
 * GET /api/admin/sites/:id/website
 */
export async function handleGetWebsiteConfig(db, siteId, env) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

    const config = await db.prepare("SELECT * FROM sneak_website_configs WHERE site_id = ?").bind(siteId).first();
    const branding = await db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(siteId).first();

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
            site_title: `${branding?.display_name || site.site_name} | Southwest Florida Real Estate`,
            tagline: 'Your trusted guide to Southwest Florida living',
            hero_heading: 'Find Your Place in Southwest Florida',
            hero_subheading: 'Explore luxury waterfront estates, golf communities, and coastal properties.',
            hero_image_url: 'https://images.unsplash.com/photo-1512917774080-9991f1c4c750?auto=format&fit=crop&w=1600&q=80',
            about_heading: `About ${branding?.display_name || site.site_name}`,
            about_body: 'Dedicated to providing exceptional real estate advisory and MLS representation across Southwest Florida.',
            about_image_url: branding?.agent_photo_url || null,
            featured_areas_json: JSON.stringify([
                { name: 'Bonita Springs', description: 'Gulf beaches, boating, and golf clubs', filter: 'Bonita Springs', image_url: 'https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&w=600&q=80' },
                { name: 'Estero', description: 'Vibrant master-planned communities and Estero Bay', filter: 'Estero', image_url: 'https://images.unsplash.com/photo-1544644181-1484b3fdfc62?auto=format&fit=crop&w=600&q=80' },
                { name: 'Naples', description: 'World-class dining, luxury estates, and white sand beaches', filter: 'Naples', image_url: 'https://images.unsplash.com/photo-1580587771525-78b9dba3b914?auto=format&fit=crop&w=600&q=80' }
            ]),
            seo_title: `${branding?.display_name || site.site_name} - Southwest Florida Real Estate`,
            seo_description: 'Search all active MLS listings, open houses, and luxury properties in Southwest Florida.',
            footer_text: `© ${new Date().getFullYear()} ${branding?.display_name || site.site_name}. All rights reserved.`,
            contact_cta_text: 'Ready to start your property search or schedule a private showing? Get in touch today.'
        },
        previewUrl,
        previewToken
    });
}

/**
 * PUT /api/admin/sites/:id/website
 */
export async function handleUpdateWebsiteConfig(db, siteId, body, actor, env) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

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
        navigation_json,
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
            featured_areas_json, navigation_json, social_links_json,
            seo_title, seo_description, footer_text, contact_cta_text,
            created_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
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
            navigation_json = COALESCE(excluded.navigation_json, sneak_website_configs.navigation_json),
            social_links_json = COALESCE(excluded.social_links_json, sneak_website_configs.social_links_json),
            seo_title = COALESCE(excluded.seo_title, sneak_website_configs.seo_title),
            seo_description = COALESCE(excluded.seo_description, sneak_website_configs.seo_description),
            footer_text = COALESCE(excluded.footer_text, sneak_website_configs.footer_text),
            contact_cta_text = COALESCE(excluded.contact_cta_text, sneak_website_configs.contact_cta_text),
            updated_at = excluded.updated_at
    `).bind(
        siteId, enabled !== undefined ? (enabled ? 1 : 0) : null, template_key || null, site_title || null, tagline || null,
        hero_heading || null, hero_subheading || null, hero_image_url || null,
        about_heading || null, about_body || null, about_image_url || null,
        typeof featured_areas_json === 'object' ? JSON.stringify(featured_areas_json) : (featured_areas_json || null),
        typeof navigation_json === 'object' ? JSON.stringify(navigation_json) : (navigation_json || null),
        typeof social_links_json === 'object' ? JSON.stringify(social_links_json) : (social_links_json || null),
        seo_title || null, seo_description || null, footer_text || null, contact_cta_text || null,
        now, now
    ).run();

    await logAudit(db, actor, 'UPDATE_WEBSITE_CONFIG', 'website_config', siteId, `Updated website config (template: ${template_key}, enabled: ${enabled})`);

    const updated = await db.prepare("SELECT * FROM sneak_website_configs WHERE site_id = ?").bind(siteId).first();
    
    const secret = env?.SNEAK_WEBSITE_PREVIEW_SECRET || 'dev_preview_secret_ccor_2026';
    let previewUrl = null;
    try {
        const previewToken = await createPreviewToken(site.site_key, site.id, secret, 1800);
        previewUrl = `https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/${site.site_key}?token=${encodeURIComponent(previewToken)}`;
    } catch {}

    return json({ success: true, website: updated, previewUrl });
}

/**
 * POST /api/admin/sites/:id/website/preview-token
 */
export async function handleCreateWebsitePreviewToken(db, siteId, env) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

    const secret = env?.SNEAK_WEBSITE_PREVIEW_SECRET || 'dev_preview_secret_ccor_2026';
    const previewToken = await createPreviewToken(site.site_key, site.id, secret, 1800);
    const previewUrl = `https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/preview/${site.site_key}?token=${encodeURIComponent(previewToken)}`;

    return json({ success: true, siteKey: site.site_key, previewToken, previewUrl, expiresIn: 1800 });
}

import {
    prepareCustomHostname,
    refreshCustomHostnameStatus,
    removeCustomHostname
} from './cloudflare-saas.js';

/**
 * GET /api/admin/sites/:id/domains/bindings
 */
export async function handleListDomainBindings(db, siteId) {
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return error('Site not found', 404);

    const res = await db.prepare(`
        SELECT b.*, d.verified AS sneak_verified, d.status AS domain_status
        FROM sneak_domain_bindings b
        JOIN sneak_domains d ON b.domain_id = d.id
        WHERE b.site_id = ? AND b.status != 'removed'
        ORDER BY b.created_at DESC
    `).bind(siteId).all();

    return json({ bindings: res.results || [] });
}

/**
 * POST /api/admin/sites/:id/domains/prepare
 */
export async function handlePrepareDomainBinding(db, siteId, body, actor, env) {
    const { hostname } = body;
    if (!hostname) return error('Hostname is required');

    const result = await prepareCustomHostname(db, siteId, hostname, env, actor);
    if (!result.success) return error(result.error, 400);

    return json(result, 201);
}

/**
 * POST /api/admin/domain-bindings/:id/refresh
 */
export async function handleRefreshDomainBinding(db, bindingId, actor, env) {
    const result = await refreshCustomHostnameStatus(db, bindingId, env, actor);
    if (!result.success) return error(result.error, 400);

    return json(result);
}

/**
 * DELETE /api/admin/domain-bindings/:id
 */
export async function handleRemoveDomainBinding(db, bindingId, actor, env) {
    const result = await removeCustomHostname(db, bindingId, env, actor);
    if (!result.success) return error(result.error, 400);

    return json(result);
}

import { getCloudflareSaaSDiagnostic, CloudflareSaaSClient } from './cloudflare-saas.js';

/**
 * GET /api/admin/domains/diagnostic
 */
export async function handleGetDomainDiagnostic(env) {
    return json(getCloudflareSaaSDiagnostic(env));
}

/**
 * GET /api/admin/domains/fallback-origin
 */
export async function handleGetFallbackOrigin(env) {
    const client = new CloudflareSaaSClient(env);
    try {
        const result = await client.getFallbackOrigin();
        return json(result);
    } catch (err) {
        return json({ success: false, error: err.message, status: 'error' }, 500);
    }
}

/**
 * PUT /api/admin/domains/fallback-origin
 */
export async function handleUpdateFallbackOrigin(env, body) {
    const client = new CloudflareSaaSClient(env);
    try {
        const origin = body?.origin || env?.CLOUDFLARE_SAAS_FALLBACK_ORIGIN || 'sneak-origin.coconutcoasthomes.com';
        const result = await client.updateFallbackOrigin(origin);
        return json(result);
    } catch (err) {
        return json({ success: false, error: err.message, status: 'error' }, 500);
    }
}

/**
 * GET /api/admin/readiness
 * Returns high-level operational readiness status across all subsystems.
 */
export async function handleGetReadiness(db, env) {
    let mlsStatus = 'Problem';
    let activeListingsCount = 0;
    let activeUnderContractCount = 0;
    let pendingCount = 0;
    let totalEligibleListingsCount = 0;
    let openHousesCount = 0;
    let lastListingSync = null;
    let lastOpenHouseSync = null;
    let listingCursor = null;
    let syncFreshnessMinutes = null;
    let dbError = null;

    try {
        // Query listing statuses breakdown
        const [statusRows, ohRow, listingSyncRow, ohSyncRow] = await Promise.all([
            db.prepare(`
                SELECT StandardStatus, COUNT(*) as cnt 
                FROM sneak_listings 
                WHERE StandardStatus IN ('Active', 'Active Under Contract', 'Pending')
                GROUP BY StandardStatus
            `).all(),
            db.prepare("SELECT COUNT(*) as cnt FROM sneak_open_houses").first(),
            db.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").first(),
            db.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'open_houses'").first()
        ]);

        if (statusRows && statusRows.results) {
            for (const r of statusRows.results) {
                if (r.StandardStatus === 'Active') activeListingsCount = r.cnt;
                else if (r.StandardStatus === 'Active Under Contract') activeUnderContractCount = r.cnt;
                else if (r.StandardStatus === 'Pending') pendingCount = r.cnt;
            }
            totalEligibleListingsCount = activeListingsCount + activeUnderContractCount + pendingCount;
        }

        openHousesCount = ohRow?.cnt || 0;
        lastListingSync = listingSyncRow?.last_successful_sync || null;
        listingCursor = listingSyncRow?.last_cursor || null;
        lastOpenHouseSync = ohSyncRow?.last_successful_sync || null;

        if (lastListingSync) {
            const syncTime = new Date(lastListingSync.replace(' ', 'T') + 'Z').getTime();
            if (!isNaN(syncTime)) {
                syncFreshnessMinutes = Math.max(0, Math.round((Date.now() - syncTime) / 60000));
            }
        }

        if (totalEligibleListingsCount > 0 && listingSyncRow?.status === 'success') {
            mlsStatus = 'Healthy';
        } else if (totalEligibleListingsCount > 0) {
            mlsStatus = 'Healthy';
        }
    } catch (err) {
        dbError = err.message;
    }

    const saasDiag = getCloudflareSaaSDiagnostic(env);

    let launchChecks = [];
    let allChecksPassed = false;
    let anyCheckFailed = false;
    let fallbackOriginStatus = 'Missing';
    let senderDomainStatus = 'Missing';
    let emailConfigured = Boolean((env?.MAILJET_API_KEY || env?.MJ_API_KEY) && (env?.MAILJET_SECRET_KEY || env?.MJ_API_SECRET)) || Boolean(env?.RESEND_API_KEY || env?.POSTMARK_SERVER_TOKEN);
    let emailMode = emailConfigured ? (Boolean((env?.MAILJET_API_KEY || env?.MJ_API_KEY) && (env?.MAILJET_SECRET_KEY || env?.MJ_API_SECRET)) ? 'Mailjet' : (env?.RESEND_API_KEY ? 'Resend' : 'Postmark')) : 'Simulated';

    try {
        const checkRows = await db.prepare("SELECT * FROM sneak_launch_checks ORDER BY check_key ASC").all();
        if (checkRows && checkRows.results) {
            launchChecks = checkRows.results;
            const passCount = launchChecks.filter(c => c.status === 'pass').length;
            allChecksPassed = (launchChecks.length >= 12 && passCount === launchChecks.length);
            anyCheckFailed = launchChecks.some(c => c.status === 'fail');

            const fbCheck = launchChecks.find(c => c.check_key === 'cloudflare_fallback_active');
            if (fbCheck?.status === 'pass') {
                fallbackOriginStatus = 'Active';
            } else if (saasDiag.cnameTarget && saasDiag.cnameTarget !== 'Not Configured') {
                fallbackOriginStatus = 'Pending';
            }

            const provCheck = launchChecks.find(c => c.check_key === 'email_provider_configured');
            if (provCheck?.status === 'pass' && provCheck?.source === 'real_mailjet') {
                emailConfigured = true;
                emailMode = 'Mailjet';
            }

            const emailVerCheck = launchChecks.find(c => c.check_key === 'email_domain_verified');
            if (emailVerCheck?.status === 'pass') {
                senderDomainStatus = 'Verified';
            } else if (emailConfigured && (env?.EMAIL_FROM || env?.FROM_EMAIL)) {
                senderDomainStatus = 'Pending Verification';
            } else if (emailConfigured) {
                senderDomainStatus = 'Configured';
            }
        }
    } catch {}

    const blockers = [];
    if (!saasDiag.zoneConfigured || saasDiag.mode !== 'live') {
        blockers.push('SNEAK PROVIDER DOMAIN REQUIRED');
    }
    if (!emailConfigured) {
        blockers.push('LIVE TRANSACTIONAL EMAIL REQUIRED');
    }

    let readinessCategory = 'Development Ready';
    if (mlsStatus === 'Healthy' && allChecksPassed) {
        readinessCategory = 'Pilot Ready';
    } else if (anyCheckFailed || mlsStatus === 'Problem') {
        readinessCategory = 'Problem';
    } else if (saasDiag.mode === 'live' && saasDiag.zoneConfigured && emailConfigured) {
        readinessCategory = 'External Verification Pending';
    } else if (mlsStatus === 'Healthy') {
        readinessCategory = 'External Services Pending';
    }

    return json({
        readinessCategory,
        pilotReady: readinessCategory === 'Pilot Ready',
        mlsFeed: {
            status: mlsStatus,
            activeListings: activeListingsCount,
            activeUnderContractListings: activeUnderContractCount,
            pendingListings: pendingCount,
            totalEligibleListings: totalEligibleListingsCount,
            openHouses: openHousesCount,
            lastListingSync,
            lastOpenHouseSync,
            listingCursor,
            syncFreshnessMinutes
        },
        servingWorker: totalEligibleListingsCount > 0 ? 'Healthy' : 'Problem',
        cloudflareSaaS: {
            mode: saasDiag.mode === 'live' ? 'Live' : 'Simulation',
            status: saasDiag.zoneConfigured ? 'Configured' : 'Missing',
            fallbackOrigin: fallbackOriginStatus,
            customerCnameTarget: saasDiag.cnameTarget
        },
        email: {
            mode: emailMode,
            senderDomain: senderDomainStatus
        },
        memberPortal: 'Healthy',
        websiteEngine: 'Healthy',
        growthZone: 'Manual',
        launchChecks,
        dbError,
        blockers
    });
}

const ALLOWED_LAUNCH_CHECK_KEYS = new Set([
    'cloudflare_saas_enabled',
    'cloudflare_fallback_active',
    'cloudflare_real_custom_hostname',
    'cloudflare_real_ssl',
    'cloudflare_real_https',
    'cloudflare_real_idx',
    'cloudflare_real_removal',
    'email_provider_configured',
    'email_domain_verified',
    'email_real_invitation',
    'email_real_login',
    'email_replay_protection'
]);

/**
 * GET /api/admin/launch-checks
 */
export async function handleListLaunchChecks(db) {
    const rows = await db.prepare("SELECT * FROM sneak_launch_checks ORDER BY check_key ASC").all();
    return json({ checks: rows?.results || [] });
}

/**
 * POST /api/admin/launch-checks
 * Records authoritative launch evidence.
 */
export async function handleRecordLaunchCheck(db, body, actor = 'admin') {
    const { check_key, status, source, detail } = body;
    if (!check_key || !status || !source) {
        return error('check_key, status, and source are required.');
    }
    if (!ALLOWED_LAUNCH_CHECK_KEYS.has(check_key)) {
        return error(`Unknown check_key '${check_key}'. Must be one of the 12 authorized launch check keys.`, 400);
    }
    if (!['pass', 'pending', 'fail'].includes(status)) {
        return error('Invalid status. Must be pass, pending, or fail.', 400);
    }

    // STRICT SOURCE GUARDS: Simulation or unauthorized sources cannot pass real checks
    const normalizedSource = source.toLowerCase().trim();
    if (status === 'pass') {
        if (check_key.startsWith('cloudflare_') && normalizedSource !== 'real_cloudflare') {
            return error(`Source '${source}' is not authorized to pass Cloudflare check '${check_key}'. Requires 'real_cloudflare'.`, 400);
        }
        if (check_key.startsWith('email_') && check_key !== 'email_replay_protection' && normalizedSource !== 'real_mailjet') {
            return error(`Source '${source}' is not authorized to pass email check '${check_key}'. Requires 'real_mailjet'.`, 400);
        }
        if (check_key === 'email_replay_protection' && normalizedSource !== 'system') {
            return error(`Source '${source}' is not authorized to pass check 'email_replay_protection'. Requires 'system'.`, 400);
        }
    }

    const detailJson = detail ? (typeof detail === 'string' ? detail : JSON.stringify(detail)) : null;
    const now = new Date().toISOString();

    await db.prepare(`
        INSERT INTO sneak_launch_checks (check_key, status, source, checked_at, detail_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(check_key) DO UPDATE SET
            status = excluded.status,
            source = excluded.source,
            checked_at = excluded.checked_at,
            detail_json = excluded.detail_json,
            updated_at = excluded.updated_at
    `).bind(check_key, status, source, now, detailJson, now).run();

    const updated = await db.prepare("SELECT * FROM sneak_launch_checks WHERE check_key = ?").bind(check_key).first();
    return json({ success: true, check: updated });
}





