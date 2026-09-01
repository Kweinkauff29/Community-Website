/**
 * Per-account launch readiness projection. Uses D1/service bindings only; never Bridge.
 */

import { evaluateServingDecision, isTenantScopeValid } from '../sneak-shared/entitlement.js';
import { checkMlsInventory } from './validation.js';
import { generateEmbedSnippets } from './embed-generator.js';

async function readWorkerHealth(binding, path = '/health') {
    if (!binding || typeof binding.fetch !== 'function') return { reachable: false, status: 'not_configured', data: {} };
    try {
        const response = await binding.fetch(new Request(`https://service.internal${path}`, { headers: { Accept: 'application/json' } }));
        const data = await response.json().catch(() => ({}));
        return { reachable: response.ok, status: response.ok ? 'healthy' : 'unhealthy', data };
    } catch {
        return { reachable: false, status: 'unreachable', data: {} };
    }
}

function launchBlocker(code, message, capability = 'core_idx') {
    return { code, message, capability };
}

function parseD1Date(value) {
    if (!value) return null;
    const text = String(value);
    const parsed = new Date(text.includes('T') ? text : `${text.replace(' ', 'T')}Z`);
    return Number.isFinite(parsed.getTime()) ? parsed : null;
}

export async function calculateAccountReadiness(db, accountId, env = {}) {
    const account = await db.prepare("SELECT * FROM sneak_accounts WHERE id = ?").bind(accountId).first();
    if (!account) return null;

    const [entitlement, sitesResult, membersResult, syncState, listingsRow, servingHealth, memberHealth, consumerHealth, alertHealth, launchChecksResult, reconciliation] = await Promise.all([
        db.prepare("SELECT * FROM sneak_account_entitlements WHERE account_id = ?").bind(accountId).first(),
        db.prepare("SELECT * FROM sneak_sites WHERE account_id = ? ORDER BY created_at ASC").bind(accountId).all(),
        db.prepare("SELECT id, email, role, status, invited_at, activated_at FROM sneak_member_users WHERE account_id = ? ORDER BY created_at ASC").bind(accountId).all(),
        db.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").first(),
        db.prepare("SELECT COUNT(*) AS count FROM sneak_listings").first(),
        readWorkerHealth(env.SERVING_WORKER, '/idx/v1/health'),
        readWorkerHealth(env.MEMBER_WORKER, '/health'),
        readWorkerHealth(env.CONSUMER_WORKER, '/api/consumer/version'),
        readWorkerHealth(env.ALERT_WORKER, '/health'),
        db.prepare("SELECT check_key, status, checked_at FROM sneak_launch_checks").all(),
        db.prepare("SELECT * FROM sneak_growthzone_reconciliation WHERE account_id = ?").bind(accountId).first()
    ]);

    const site = (sitesResult.results || [])[0] || null;
    let domains = [];
    let branding = null;
    let widgets = [];
    let scopeInventory = { valid: false, count: 0 };
    let bootstrap = { reachable: false, status: 'not_checked' };

    if (site) {
        const [domainRows, brandRow, widgetRows] = await Promise.all([
            db.prepare("SELECT * FROM sneak_domains WHERE site_id = ? ORDER BY created_at ASC").bind(site.id).all(),
            db.prepare("SELECT * FROM sneak_branding WHERE site_id = ?").bind(site.id).first(),
            db.prepare("SELECT * FROM sneak_widget_configs WHERE site_id = ?").bind(site.id).all()
        ]);
        domains = domainRows.results || [];
        branding = brandRow;
        widgets = widgetRows.results || [];
        scopeInventory = await checkMlsInventory(db, site.scope_type, site.scope_value);

        const authorizedDomain = domains.find(item => item.status === 'active' && item.verified === 1);
        if (authorizedDomain && env.SERVING_WORKER && typeof env.SERVING_WORKER.fetch === 'function') {
            try {
                const origin = `https://${authorizedDomain.domain}`;
                const response = await env.SERVING_WORKER.fetch(new Request(
                    `https://service.internal/idx/v1/bootstrap?site=${encodeURIComponent(site.site_key)}`,
                    { headers: { Origin: origin, Referer: `${origin}/` } }
                ));
                bootstrap = { reachable: response.ok, status: response.ok ? 'healthy' : `http_${response.status}` };
            } catch {
                bootstrap = { reachable: false, status: 'unreachable' };
            }
        }
    }

    const domainAuthorized = domains.some(item => item.status === 'active' && item.verified === 1);
    const serving = evaluateServingDecision({
        accountStatus: account.status,
        siteStatus: site?.status,
        entitlementStatus: entitlement?.status,
        graceUntil: entitlement?.grace_until,
        expiresAt: entitlement?.expires_at,
        domainAuthorized,
        scopeType: site?.scope_type,
        scopeValue: site?.scope_value
    });

    const members = membersResult.results || [];
    const activeMemberExists = members.some(member => member.status === 'active');
    const memberExists = members.some(member => member.status === 'active' || member.status === 'invited');
    const searchWidgetReady = widgets.some(widget => widget.widget_type === 'search' && widget.enabled === 1);
    const brandingReady = Boolean(branding?.display_name && branding?.brokerage && branding?.email);
    const embedReady = Boolean(site?.site_key && generateEmbedSnippets(
        site.site_key,
        domains.filter(item => item.status === 'active' && item.verified === 1).map(item => item.domain),
        branding || {}
    ).snippets.search?.htmlSnippet);
    const syncDate = parseD1Date(syncState?.last_successful_sync || syncState?.updated_at);
    const syncAgeMinutes = syncDate ? Math.max(0, Math.round((Date.now() - syncDate.getTime()) / 60000)) : null;
    const syncFresh = syncState?.status === 'success' && Number.isFinite(syncAgeMinutes) && syncAgeMinutes <= 180;

    const launchBlockers = serving.blockers.map(item => ({ ...item, capability: 'core_idx' }));
    if (!memberExists) launchBlockers.push(launchBlocker('MEMBER_USER_MISSING', 'Create an invited or active Member Portal user.', 'member_portal'));
    if (!scopeInventory.valid) launchBlockers.push(launchBlocker('MLS_IDENTITY_UNVERIFIED', 'The tenant MLS identity has no matching current inventory.'));
    if (!brandingReady) launchBlockers.push(launchBlocker('BRANDING_INCOMPLETE', 'Display name, brokerage, and contact email are required.'));
    if (!searchWidgetReady) launchBlockers.push(launchBlocker('SEARCH_WIDGET_DISABLED', 'The primary search widget is not enabled.'));
    if (!embedReady) launchBlockers.push(launchBlocker('EMBED_UNAVAILABLE', 'A current embed snippet could not be generated.'));
    if (!syncFresh) launchBlockers.push(launchBlocker('SYNC_STALE', 'The latest successful listing sync is missing or older than three hours.'));
    if (Number(listingsRow?.count || 0) < 1) launchBlockers.push(launchBlocker('MLS_INVENTORY_EMPTY', 'No listing inventory is available.'));
    if (!servingHealth.reachable) launchBlockers.push(launchBlocker('SERVING_WORKER_UNHEALTHY', 'The Serving Worker health check did not succeed.'));
    if (!bootstrap.reachable) launchBlockers.push(launchBlocker('BOOTSTRAP_UNVERIFIED', 'The authorized-domain bootstrap check did not succeed.', 'custom_domain'));

    const checklist = [
        ['ACCOUNT_VALID', account.status === 'active', 'Account is active'],
        ['ENTITLEMENT_VALID', !serving.blockers.some(item => item.code.startsWith('ENTITLEMENT_') || item.code === 'GRACE_EXPIRED'), 'Generic entitlement permits service'],
        ['SITE_ACTIVE', site?.status === 'active', 'Primary IDX site is active'],
        ['TENANT_SCOPE_VALID', Boolean(site && isTenantScopeValid(site.scope_type, site.scope_value) && scopeInventory.valid), 'Tenant scope and MLS identity are valid'],
        ['MEMBER_USER_EXISTS', memberExists, 'Member Portal user exists and is invited or active'],
        ['DOMAIN_AUTHORIZED', domainAuthorized, 'A domain is active and verified'],
        ['BRANDING_COMPLETE', brandingReady, 'Required branding and contact identity are complete'],
        ['SEARCH_WIDGET_ENABLED', searchWidgetReady, 'Search widget is enabled'],
        ['EMBED_GENERATED', embedReady, 'Responsive embed snippet is available'],
        ['SERVING_HEALTHY', servingHealth.reachable, 'Serving Worker is healthy'],
        ['BOOTSTRAP_HEALTHY', bootstrap.reachable, 'Authorized-domain bootstrap succeeds'],
        ['SYNC_FRESH', syncFresh, 'Listing sync is fresh'],
        ['LISTINGS_AVAILABLE', Number(listingsRow?.count || 0) > 0, 'MLS listing inventory is available']
    ].map(([code, passed, label]) => ({ code, label, type: 'automated', status: passed ? 'pass' : 'fail' }));

    const uniqueBlockers = [...new Map(launchBlockers.map(item => [item.code, item])).values()];
    const launchReady = uniqueBlockers.length === 0;
    const alertReady = alertHealth.data?.deliveryReady === true;
    const launchChecksByKey = new Map((launchChecksResult.results || []).map(item => [item.check_key, item]));
    const passed = key => launchChecksByKey.get(key)?.status === 'pass';
    const memberEmailReady = memberHealth.data?.emailProviderConfigured === true && passed('member_magic_link_e2e');
    const consumerEmailReady = consumerHealth.data?.emailProviderConfigured === true && passed('consumer_magic_link_e2e');
    const savedSearchAlertsReady = alertReady
        && passed('alerts_asap_e2e')
        && passed('alerts_daily_e2e')
        && passed('alerts_unsubscribe_e2e');
    const reconciliationSuccess = ['verified_no_change', 'entitlement_changed'].includes(reconciliation?.status);
    const reconciliationAge = parseD1Date(reconciliation?.last_success_at);
    const reconciliationStale = Boolean(reconciliationSuccess && (!reconciliationAge || Date.now() - reconciliationAge.getTime() > 36 * 60 * 60 * 1000));
    const growthZoneStatus = entitlement?.source === 'manual'
        ? 'MANUAL_OVERRIDE'
        : (!env?.GROWTHZONE_BASE_URL || !env?.GROWTHZONE_API_KEY
            ? 'NOT_READY'
            : (reconciliationStale ? 'VERIFICATION_STALE' : (reconciliationSuccess ? 'READY' : 'NOT_VERIFIED')));

    return {
        accountId,
        siteId: site?.id || null,
        canServe: serving.canServe,
        blockers: serving.blockers,
        launchReady,
        launchStatus: launchReady ? 'READY_TO_LAUNCH' : 'NOT_READY',
        launchBlockers: uniqueBlockers,
        checklist,
        setupProgress: {
            memberIdentity: members.length ? (activeMemberExists ? 'complete' : 'needs_attention') : 'missing',
            subscription: entitlement ? (serving.blockers.some(item => item.code.startsWith('ENTITLEMENT_') || item.code === 'GRACE_EXPIRED') ? 'needs_attention' : 'complete') : 'missing',
            idxSite: site ? 'complete' : 'missing',
            domain: domainAuthorized ? 'complete' : 'needs_attention',
            branding: brandingReady ? 'complete' : 'needs_attention',
            embed: embedReady ? 'ready' : 'missing',
            launch: launchReady ? 'ready' : 'blocked'
        },
        capabilities: {
            coreSearch: { status: launchReady ? 'READY' : 'NOT_READY', core: true },
            memberMagicLinkEmail: { status: memberEmailReady ? 'READY' : (memberHealth.data?.emailProviderConfigured ? 'NOT_VERIFIED' : 'NOT_READY'), core: false },
            consumerLogin: { status: consumerEmailReady ? 'READY' : (consumerHealth.data?.emailProviderConfigured ? 'NOT_VERIFIED' : 'NOT_READY'), core: false },
            consumerMagicLinkEmail: { status: consumerEmailReady ? 'READY' : (consumerHealth.data?.emailProviderConfigured ? 'NOT_VERIFIED' : 'NOT_READY'), core: false },
            savedSearchEmailAlerts: { status: savedSearchAlertsReady ? 'READY' : (alertReady ? 'NOT_VERIFIED' : 'NOT_READY'), core: false },
            growthZoneReconciliation: { status: growthZoneStatus, core: false },
            customDomain: { status: domainAuthorized && bootstrap.reachable ? 'READY' : 'NOT_READY', core: true },
            memberPortal: { status: memberHealth.reachable && memberExists ? 'READY' : 'NOT_READY', core: true },
            adminPortal: { status: 'READY', core: true }
        },
        diagnostics: {
            syncAgeMinutes,
            listingCount: Number(listingsRow?.count || 0),
            servingWorker: servingHealth.status,
            memberWorker: memberHealth.status,
            consumerWorker: consumerHealth.status,
            alertsDeliveryConfigured: alertReady,
            reconciliationStatus: reconciliation?.status || 'never',
            reconciliationLastAttempt: reconciliation?.last_attempt_at || null,
            reconciliationLastVerified: entitlement?.last_verified_at || null,
            bootstrap: bootstrap.status
        }
    };
}
