/**
 * sneak-admin/cloudflare-saas.js
 * 
 * Cloudflare for SaaS Custom Hostname & SSL Management for SNEAK Websites.
 * Supports explicit 'live' and 'simulation' modes, fail-closed security,
 * hardened provider delete recovery, orphan prevention, and atomic D1 synchronization.
 */

const FORBIDDEN_HOST_SUFFIXES = [
    'workers.dev',
    'pages.dev',
    'localhost',
    '127.0.0.1',
    '::1',
    'sneakidx.com',
    'bonitaspringsrealtors.workers.dev'
];

/**
 * Normalizes and validates a requested customer hostname.
 */
export function normalizeHostname(raw) {
    if (!raw || typeof raw !== 'string') {
        return { valid: false, error: 'Hostname is required.' };
    }

    let cleaned = raw.trim().toLowerCase();

    // Strip protocols
    cleaned = cleaned.replace(/^https?:\/\//i, '');
    
    // Strip trailing slashes, paths, queries, fragments, ports
    cleaned = cleaned.split('/')[0].split('?')[0].split('#')[0].split(':')[0].trim();

    if (!cleaned) {
        return { valid: false, error: 'Invalid hostname format.' };
    }

    // Reject wildcards
    if (cleaned.includes('*')) {
        return { valid: false, error: 'Wildcard hostnames are not supported for custom websites.' };
    }

    // Reject IP addresses (IPv4 & IPv6)
    if (/^(\d{1,3}\.){3}\d{1,3}$/.test(cleaned) || cleaned.includes(':')) {
        return { valid: false, error: 'IP addresses cannot be used as custom domains.' };
    }

    // Reject forbidden internal / infrastructure hostnames
    for (const suffix of FORBIDDEN_HOST_SUFFIXES) {
        if (cleaned === suffix || cleaned.endsWith('.' + suffix)) {
            return { valid: false, error: `Domain cannot belong to reserved infrastructure (${suffix}).` };
        }
    }

    // Validate standard FQDN structure (must have at least one dot, valid labels)
    const fqdnRegex = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/;
    if (!fqdnRegex.test(cleaned)) {
        return { valid: false, error: 'Hostname must be a valid fully qualified domain name (e.g. www.yourdomain.com).' };
    }

    // Check TLD length
    const parts = cleaned.split('.');
    const tld = parts[parts.length - 1];
    if (tld.length < 2 || /^\d+$/.test(tld)) {
        return { valid: false, error: 'Invalid top-level domain.' };
    }

    return { valid: true, hostname: cleaned };
}

/**
 * Cloudflare for SaaS API Adapter.
 */
export class CloudflareSaaSClient {
    constructor(env) {
        this.mode = (env?.CLOUDFLARE_SAAS_MODE || 'simulation').toLowerCase();
        this.apiToken = env?.CLOUDFLARE_SAAS_API_TOKEN || null;
        this.zoneId = env?.CLOUDFLARE_SAAS_ZONE_ID || null;
        this.fallbackTarget = env?.CLOUDFLARE_SAAS_CNAME_TARGET || (this.mode === 'live' ? null : 'customers.sneakidx.com');
    }

    get isLive() {
        return this.mode === 'live';
    }

    validateLiveConfig() {
        if (this.isLive) {
            if (!this.apiToken) {
                throw new Error('CLOUDFLARE_SAAS_API_TOKEN is required in live mode.');
            }
            if (!this.zoneId) {
                throw new Error('CLOUDFLARE_SAAS_ZONE_ID is required in live mode.');
            }
            if (!this.fallbackTarget) {
                throw new Error('CLOUDFLARE_SAAS_CNAME_TARGET is required in live mode.');
            }
        }
    }

    async createCustomHostname(hostname) {
        if (this.isLive) {
            this.validateLiveConfig();

            const res = await fetch(`https://api.cloudflare.com/client/v4/zones/${this.zoneId}/custom_hostnames`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.apiToken}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    hostname,
                    ssl: {
                        method: 'http',
                        type: 'dv',
                        settings: { http2: 'on', min_tls_version: '1.2' }
                    }
                })
            });

            const data = await res.json();
            if (!data.success) {
                const err = data.errors?.[0] || { message: 'Cloudflare API error' };
                // Handle already exists in Cloudflare
                if (err.code === 1434 || (err.message && err.message.toLowerCase().includes('already exists'))) {
                    // Try to look up existing
                    const listRes = await fetch(`https://api.cloudflare.com/client/v4/zones/${this.zoneId}/custom_hostnames?hostname=${encodeURIComponent(hostname)}`, {
                        headers: { 'Authorization': `Bearer ${this.apiToken}` }
                    });
                    const listData = await listRes.json();
                    if (listData.success && listData.result?.[0]) {
                        const existing = listData.result[0];
                        return {
                            providerHostnameId: existing.id,
                            status: existing.status,
                            sslStatus: existing.ssl?.status || 'pending_validation',
                            cnameTarget: this.fallbackTarget,
                            ownershipTxtName: existing.ownership_verification?.name || null,
                            ownershipTxtValue: existing.ownership_verification?.value || null,
                            providerSource: 'REAL CLOUDFLARE'
                        };
                    }
                }
                throw new Error(`[Cloudflare Error ${err.code || 'API'}]: ${err.message || 'Failed to create Custom Hostname'}`);
            }

            const r = data.result;
            return {
                providerHostnameId: r.id,
                status: r.status || 'pending_validation',
                sslStatus: r.ssl?.status || 'pending_validation',
                cnameTarget: this.fallbackTarget,
                ownershipTxtName: r.ownership_verification?.name || null,
                ownershipTxtValue: r.ownership_verification?.value || null,
                providerSource: 'REAL CLOUDFLARE'
            };
        }

        // SIMULATED PROVIDER ADAPTER
        return {
            providerHostnameId: `cf_cust_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`,
            status: 'pending_dns',
            sslStatus: 'pending_validation',
            cnameTarget: this.fallbackTarget || 'customers.sneakidx.com',
            ownershipTxtName: `_cf-custom-hostname.${hostname}`,
            ownershipTxtValue: `sneak-verify-${Date.now().toString(36)}`,
            providerSource: 'SIMULATED PROVIDER'
        };
    }

    async getCustomHostname(providerHostnameId, hostname) {
        if (this.isLive) {
            this.validateLiveConfig();

            const res = await fetch(`https://api.cloudflare.com/client/v4/zones/${this.zoneId}/custom_hostnames/${providerHostnameId}`, {
                headers: {
                    'Authorization': `Bearer ${this.apiToken}`,
                    'Content-Type': 'application/json'
                }
            });

            const data = await res.json();
            if (!data.success) {
                const err = data.errors?.[0] || { message: 'Cloudflare API error' };
                throw new Error(`[Cloudflare Error ${err.code || 'API'}]: ${err.message || 'Failed to fetch Custom Hostname details'}`);
            }

            const r = data.result;
            return {
                status: r.status,
                sslStatus: r.ssl?.status,
                cnameTarget: this.fallbackTarget,
                ownershipTxtName: r.ownership_verification?.name || null,
                ownershipTxtValue: r.ownership_verification?.value || null,
                providerSource: 'REAL CLOUDFLARE'
            };
        }

        // SIMULATED PROVIDER ADAPTER
        return {
            status: 'active',
            sslStatus: 'active',
            cnameTarget: this.fallbackTarget || 'customers.sneakidx.com',
            ownershipTxtName: `_cf-custom-hostname.${hostname}`,
            ownershipTxtValue: 'verified',
            providerSource: 'SIMULATED PROVIDER'
        };
    }

    async deleteCustomHostname(providerHostnameId) {
        if (this.isLive) {
            this.validateLiveConfig();

            const res = await fetch(`https://api.cloudflare.com/client/v4/zones/${this.zoneId}/custom_hostnames/${providerHostnameId}`, {
                method: 'DELETE',
                headers: { 'Authorization': `Bearer ${this.apiToken}` }
            });

            const data = await res.json();
            if (!data.success) {
                const err = data.errors?.[0] || { message: 'Cloudflare API error' };
                throw new Error(`[Cloudflare Error ${err.code || 'API'}]: ${err.message || 'Failed to delete Custom Hostname'}`);
            }
            return true;
        }

        return true;
    }
}

/**
 * Prepares and registers a custom hostname for a SNEAK site.
 */
export async function prepareCustomHostname(db, siteId, rawHostname, env, actor = 'admin') {
    const norm = normalizeHostname(rawHostname);
    if (!norm.valid) {
        return { success: false, error: norm.error };
    }
    const hostname = norm.hostname;

    // 1. Verify site exists
    const site = await db.prepare("SELECT * FROM sneak_sites WHERE id = ?").bind(siteId).first();
    if (!site) return { success: false, error: 'Site not found.' };

    // 2. Check duplicate ownership across all tenants
    const existingBinding = await db.prepare(`
        SELECT b.*, s.site_name, a.account_name 
        FROM sneak_domain_bindings b
        JOIN sneak_sites s ON b.site_id = s.id
        JOIN sneak_accounts a ON s.account_id = a.id
        WHERE b.hostname = ? AND b.status != 'removed'
    `).bind(hostname).first();

    if (existingBinding && existingBinding.site_id !== siteId) {
        return {
            success: false,
            error: `Domain '${hostname}' is already connected to another SNEAK site (${existingBinding.account_name}).`
        };
    }

    // 3. Ensure sneak_domains record exists
    let domainRecord = await db.prepare("SELECT * FROM sneak_domains WHERE site_id = ? AND domain = ?").bind(siteId, hostname).first();
    if (!domainRecord) {
        const domainId = `dom_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_domains (id, site_id, domain, verified, status, created_at)
            VALUES (?, ?, ?, 0, 'active', datetime('now'))
        `).bind(domainId, siteId, hostname).run();
        domainRecord = await db.prepare("SELECT * FROM sneak_domains WHERE id = ?").bind(domainId).first();
    }

    // 4. Cloudflare API Custom Hostname Creation
    const client = new CloudflareSaaSClient(env);
    let cfResult;
    try {
        cfResult = await client.createCustomHostname(hostname);
    } catch (err) {
        console.error('[CLOUDFLARE SAAS ERROR]', err.message);
        return { success: false, error: err.message };
    }

    // 5. Atomic D1 Binding Record Upsert
    const bindingId = existingBinding?.id || `bind_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
    const now = new Date().toISOString();

    try {
        await db.prepare(`
            INSERT INTO sneak_domain_bindings (
                id, domain_id, site_id, provider, hostname, provider_hostname_id,
                status, ssl_status, validation_method, cname_target,
                ownership_txt_name, ownership_txt_value, last_checked_at, created_at, updated_at
            ) VALUES (
                ?, ?, ?, 'cloudflare_saas', ?, ?,
                ?, ?, 'http', ?,
                ?, ?, ?, ?, ?
            )
            ON CONFLICT(hostname) DO UPDATE SET
                provider_hostname_id = excluded.provider_hostname_id,
                status = excluded.status,
                ssl_status = excluded.ssl_status,
                cname_target = excluded.cname_target,
                ownership_txt_name = excluded.ownership_txt_name,
                ownership_txt_value = excluded.ownership_txt_value,
                last_checked_at = excluded.last_checked_at,
                updated_at = excluded.updated_at
        `).bind(
            bindingId, domainRecord.id, siteId, hostname, cfResult.providerHostnameId,
            cfResult.status, cfResult.sslStatus, cfResult.cnameTarget,
            cfResult.ownershipTxtName, cfResult.ownershipTxtValue, now, now, now
        ).run();
    } catch (d1Err) {
        // D1 Write Failure: Attempt Cloudflare cleanup to avoid leaving orphan Custom Hostnames
        if (cfResult.providerHostnameId && client.isLive) {
            try { await client.deleteCustomHostname(cfResult.providerHostnameId); } catch {}
        }
        return { success: false, error: 'Database error saving domain binding: ' + d1Err.message };
    }

    // 6. Record Audit Log
    try {
        const auditId = `audit_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_admin_audit (id, admin_actor, action, entity_type, entity_id, summary, created_at)
            VALUES (?, ?, 'PREPARE_CUSTOM_HOSTNAME', 'domain_binding', ?, ?, datetime('now'))
        `).bind(auditId, actor, bindingId, `Prepared custom hostname ${hostname} for site ${site.site_key} [${cfResult.providerSource}]`).run();
    } catch {}

    const binding = await db.prepare("SELECT * FROM sneak_domain_bindings WHERE id = ?").bind(bindingId).first();

    return {
        success: true,
        providerSource: cfResult.providerSource,
        hostname,
        binding,
        dnsInstructions: {
            type: 'CNAME',
            name: hostname.startsWith('www.') ? 'www' : hostname,
            target: cfResult.cnameTarget,
            txtVerification: cfResult.ownershipTxtName ? {
                name: cfResult.ownershipTxtName,
                value: cfResult.ownershipTxtValue
            } : null
        }
    };
}

/**
 * Checks and updates status from Cloudflare for SaaS.
 * Activates sneak_domains when status === 'active' && ssl_status === 'active'.
 */
export async function refreshCustomHostnameStatus(db, bindingId, env, actor = 'system') {
    const binding = await db.prepare("SELECT * FROM sneak_domain_bindings WHERE id = ?").bind(bindingId).first();
    if (!binding) return { success: false, error: 'Domain binding not found.' };

    const client = new CloudflareSaaSClient(env);
    let cfResult;
    try {
        cfResult = await client.getCustomHostname(binding.provider_hostname_id, binding.hostname);
    } catch (err) {
        const now = new Date().toISOString();
        await db.prepare(`
            UPDATE sneak_domain_bindings
            SET error_code = 'REFRESH_ERROR',
                error_summary = ?,
                last_checked_at = ?,
                updated_at = ?
            WHERE id = ?
        `).bind(err.message.slice(0, 255), now, now, bindingId).run();
        return { success: false, error: 'Could not refresh status: ' + err.message };
    }

    const now = new Date().toISOString();
    const isFullyActive = cfResult.status === 'active' && cfResult.sslStatus === 'active';

    await db.prepare(`
        UPDATE sneak_domain_bindings
        SET status = ?,
            ssl_status = ?,
            error_code = NULL,
            error_summary = NULL,
            last_checked_at = ?,
            activated_at = CASE WHEN ? = 1 THEN COALESCE(activated_at, ?) ELSE activated_at END,
            updated_at = ?
        WHERE id = ?
    `).bind(
        cfResult.status, cfResult.sslStatus, now,
        isFullyActive ? 1 : 0, now, now, bindingId
    ).run();

    // If fully active, activate domain authorization in sneak_domains
    if (isFullyActive) {
        await db.prepare(`
            UPDATE sneak_domains
            SET verified = 1, status = 'active'
            WHERE id = ?
        `).bind(binding.domain_id).run();
    }

    const updated = await db.prepare("SELECT * FROM sneak_domain_bindings WHERE id = ?").bind(bindingId).first();

    return {
        success: true,
        providerSource: cfResult.providerSource,
        isFullyActive,
        binding: updated
    };
}

/**
 * Safely removes a custom hostname binding.
 * Hardened to prevent marking removed if Cloudflare deletion fails in live mode.
 */
export async function removeCustomHostname(db, bindingId, env, actor = 'admin') {
    const binding = await db.prepare("SELECT * FROM sneak_domain_bindings WHERE id = ?").bind(bindingId).first();
    if (!binding) return { success: false, error: 'Binding not found.' };

    // 1. Disable authorization in SNEAK immediately
    await db.prepare("UPDATE sneak_domains SET verified = 0, status = 'disabled' WHERE id = ?").bind(binding.domain_id).run();

    // 2. Delete from Cloudflare
    const client = new CloudflareSaaSClient(env);
    let deleteSuccess = false;
    let deleteError = null;

    try {
        if (binding.provider_hostname_id) {
            deleteSuccess = await client.deleteCustomHostname(binding.provider_hostname_id);
        } else {
            deleteSuccess = true;
        }
    } catch (err) {
        deleteError = err.message;
        console.error('[CF DELETE ERROR]', err.message);
    }

    const now = new Date().toISOString();

    if (!deleteSuccess && client.isLive) {
        // In live mode, if Cloudflare delete failed, mark removal_error and preserve provider_hostname_id
        await db.prepare(`
            UPDATE sneak_domain_bindings
            SET status = 'removal_error',
                error_code = 'PROVIDER_DELETE_ERROR',
                error_summary = ?,
                updated_at = ?
            WHERE id = ?
        `).bind(deleteError ? deleteError.slice(0, 255) : 'Cloudflare delete failed', now, bindingId).run();

        return {
            success: false,
            error: `Domain disabled in SNEAK, but Cloudflare Custom Hostname deletion failed: ${deleteError || 'Unknown error'}. Preserved binding ID for retry.`
        };
    }

    // 3. Mark binding removed in D1
    await db.prepare(`
        UPDATE sneak_domain_bindings
        SET status = 'removed', removed_at = ?, error_code = NULL, error_summary = NULL, updated_at = ?
        WHERE id = ?
    `).bind(now, now, bindingId).run();

    // 4. Record Audit Log
    try {
        const auditId = `audit_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_admin_audit (id, admin_actor, action, entity_type, entity_id, summary, created_at)
            VALUES (?, ?, 'REMOVE_CUSTOM_HOSTNAME', 'domain_binding', ?, ?, datetime('now'))
        `).bind(auditId, actor, bindingId, `Removed custom hostname ${binding.hostname}`).run();
    } catch {}

    return { success: true, message: `Domain ${binding.hostname} removed successfully.` };
}

/**
 * Returns sanitized diagnostic information for Cloudflare for SaaS configuration.
 */
export function getCloudflareSaaSDiagnostic(env) {
    const client = new CloudflareSaaSClient(env);
    return {
        mode: client.mode,
        isLive: client.isLive,
        zoneConfigured: Boolean(env?.CLOUDFLARE_SAAS_ZONE_ID),
        tokenConfigured: Boolean(env?.CLOUDFLARE_SAAS_API_TOKEN),
        cnameTarget: client.fallbackTarget || 'Not Configured',
        provider: 'cloudflare_saas'
    };
}
