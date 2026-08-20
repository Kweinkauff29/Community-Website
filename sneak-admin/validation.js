/**
 * sneak-admin/validation.js
 * 
 * Server-side input validation and MLS inventory checks.
 */

export function normalizeSiteKey(input) {
    if (!input || typeof input !== 'string') return '';
    return input
        .toLowerCase()
        .trim()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}

export function normalizeDomain(input) {
    if (!input || typeof input !== 'string') return '';
    let d = input.trim().toLowerCase();
    // Strip protocol
    d = d.replace(/^https?:\/\//, '');
    // Strip paths and query strings
    d = d.split('/')[0].split('?')[0].split('#')[0];
    // Strip port to match URL.hostname
    d = d.split(':')[0];
    return d.trim();
}

export function validateDomainSafety(domain) {
    if (!domain || typeof domain !== 'string') {
        return { valid: false, error: 'Domain cannot be empty.' };
    }
    const d = normalizeDomain(domain);
    if (!d) return { valid: false, error: 'Domain cannot be empty.' };

    // Insecure wildcard checks
    if (d === '*' || d === '*.com' || d === '*.net' || d === '*.org' || d === '*.workers.dev') {
        return { valid: false, error: 'Broad wildcard domain values are disallowed.' };
    }

    // Localhost / test domains
    if (d === 'localhost' || d === '127.0.0.1' || d.startsWith('localhost:')) {
        return { valid: true, domain: d };
    }

    // Domain regex
    const domainRegex = /^(\*\.)?([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,}$/i;
    if (!domainRegex.test(d)) {
        return { valid: false, error: `Invalid domain format: '${domain}'. Must be a valid hostname (e.g. 'realtorjohn.com').` };
    }

    return { valid: true, domain: d };
}

export function validateHexColor(color) {
    if (!color) return true; // Optional
    return /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(color.trim());
}

export function validateUrl(url) {
    if (!url) return true; // Optional
    try {
        const u = new URL(url);
        return u.protocol === 'http:' || u.protocol === 'https:';
    } catch {
        return false;
    }
}

export const VALID_PLANS = new Set(['trial', 'standard', 'pro', 'brokerage']);
export const VALID_STATUSES = new Set(['active', 'suspended', 'inactive']);
export const VALID_SCOPE_TYPES = new Set(['market', 'agent', 'office']);
export const VALID_WIDGET_TYPES = new Set(['search', 'search_bar', 'listing_grid', 'open_houses', 'featured_listings']);

/**
 * Validates MLS ID against current D1 inventory.
 */
export async function checkMlsInventory(db, scopeType, scopeValue) {
    if (scopeType === 'market' || !scopeValue) {
        return { valid: true, count: null };
    }

    let count = 0;
    let sample = null;

    if (scopeType === 'agent') {
        const res = await db.prepare(
            "SELECT count(*) as count FROM sneak_listings WHERE ListAgentMlsId = ? OR ListAgentKey = ?"
        ).bind(scopeValue, scopeValue).first();
        count = res?.count || 0;

        if (count > 0) {
            sample = await db.prepare(
                "SELECT ListAgentFullName, ListOfficeName, City, ListPrice FROM sneak_listings WHERE ListAgentMlsId = ? OR ListAgentKey = ? LIMIT 1"
            ).bind(scopeValue, scopeValue).first();
        }
    } else if (scopeType === 'office') {
        const res = await db.prepare(
            "SELECT count(*) as count FROM sneak_listings WHERE ListOfficeMlsId = ? OR ListOfficeKey = ?"
        ).bind(scopeValue, scopeValue).first();
        count = res?.count || 0;

        if (count > 0) {
            sample = await db.prepare(
                "SELECT ListOfficeName, City, ListPrice FROM sneak_listings WHERE ListOfficeMlsId = ? OR ListOfficeKey = ? LIMIT 1"
            ).bind(scopeValue, scopeValue).first();
        }
    }

    return {
        valid: count > 0,
        count,
        sample
    };
}
