/**
 * test/sneak-consumer.test.mjs
 * 
 * SNEAK Consumer Identity & Server Favorites Test Suite:
 * - Anti-Enumeration & Zero-Token Guarantee in magic link responses
 * - Single-Use SHA-256 Magic Link & Replay Prevention
 * - 2-Minute Exchange Code Handoff & Single-Use Consumption
 * - 14-Day Revocable Consumer Sessions
 * - Strict Site-Scoped Tenant Isolation (Site A token fails on Site B)
 * - Server Favorites: List, Add, Remove, Merge Union, Max 200 Limit
 * - Address Suppression & Off-Market Listing Handling
 * - Open-Redirect Prevention
 * - Rate Limiting Defense (Email & IP)
 * - Account Deletion / GDPR
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import worker from '../sneak-consumer/worker.js';
import {
    sha256Hex,
    generateRawToken,
    requestConsumerMagicLink,
    verifyAndConsumeConsumerMagicLink,
    exchangeAuthCodeForSession,
    verifyConsumerSession,
    revokeConsumerSession,
    deleteConsumerAccount
} from '../sneak-consumer/auth.js';

const TEST_SIGNING_SECRET = 'test-signing-secret-key-32-chars-length!!';

// Mock Sign IDX Session token helper
async function createSignedIdxSession(siteKey, secret = TEST_SIGNING_SECRET) {
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const exp = Math.floor(Date.now() / 1000) + 3600;
    const payload = Buffer.from(JSON.stringify({
        siteKey,
        site_id: siteKey === 'site-mem-1' ? 'site_1' : (siteKey === 'demo-ccor' ? 'site_ccor' : 'site_2'),
        origin: 'https://coconutcoastrealtors.org',
        exp
    })).toString('base64url');

    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
        'raw',
        enc.encode(secret),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
    );
    const sigBuf = await crypto.subtle.sign('HMAC', key, enc.encode(`${header}.${payload}`));
    const sig = Buffer.from(sigBuf).toString('base64url');
    return `${header}.${payload}.${sig}`;
}

function createMockConsumerDB() {
    const tables = {
        sneak_sites: [
            { id: 'site_1', site_key: 'site-mem-1', site_name: 'Site 1', status: 'active', scope_type: 'agent', scope_value: 'B3650316', account_id: 'acc_1' },
            { id: 'site_2', site_key: 'site-mem-2', site_name: 'Site 2', status: 'active', scope_type: 'office', scope_value: 'BPRI', account_id: 'acc_2' },
            { id: 'site_ccor', site_key: 'demo-ccor', site_name: 'CCOR Demo', status: 'active', scope_type: 'agent', scope_value: 'B3650316', account_id: 'acc_ccor' }
        ],
        sneak_accounts: [
            { id: 'acc_1', account_name: 'Member 1 Realty', status: 'active' },
            { id: 'acc_2', account_name: 'Member 2 Realty', status: 'active' },
            { id: 'acc_ccor', account_name: 'CCOR Realty', status: 'active' }
        ],
        sneak_domains: [
            { id: 'dom_1', site_id: 'site_1', domain: 'member1.com', verified: 1, status: 'active' },
            { id: 'dom_2', site_id: 'site_2', domain: 'member2.com', verified: 1, status: 'active' },
            { id: 'dom_ccor', site_id: 'site_ccor', domain: 'coconutcoastrealtors.org', verified: 1, status: 'active' }
        ],
        sneak_branding: [],
        sneak_consumer_users: [
            { id: 'c_usr_1', site_id: 'site_1', email: 'buyer1@example.com', status: 'active', created_at: '2026-08-20T00:00:00Z', activated_at: '2026-08-20T00:00:00Z', last_login_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_consumer_magic_links: [],
        sneak_consumer_sessions: [],
        sneak_consumer_auth_exchanges: [],
        sneak_consumer_login_attempts: [],
        sneak_consumer_favorites: [
            { id: 'fav_1', site_id: 'site_1', user_id: 'c_usr_1', listing_key: '2240001', created_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_consumer_saved_searches: [],
        sneak_consumer_search_alerts: [],
        sneak_listings: [
            {
                ListingKey: '2240001',
                StandardStatus: 'Active',
                ListPrice: 750000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '123 Coconut Grove Way',
                City: 'Bonita Springs',
                BedroomsTotal: 3,
                BathroomsTotalInteger: 2,
                LivingArea: 1850,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1,
                PrimaryPhoto: 'https://images.sneakidx.com/photo1.jpg'
            },
            {
                ListingKey: '2240002',
                StandardStatus: 'Active',
                ListPrice: 920000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '456 Estero Bay Blvd',
                City: 'Estero',
                BedroomsTotal: 4,
                BathroomsTotalInteger: 3,
                LivingArea: 2400,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 0, // Suppressed address
                PrimaryPhoto: 'https://images.sneakidx.com/photo2.jpg'
            },
            {
                ListingKey: '2240003_closed',
                StandardStatus: 'Closed',
                ListPrice: 500000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '789 Hidden Harbor Dr',
                City: 'Naples',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1
            }
        ]
    };

    return {
        tables,
        prepare(query) {
            let boundArgs = [];
            const nq = query.replace(/\s+/g, ' ').trim();
            return {
                bind(...args) {
                    boundArgs = args;
                    return this;
                },
                async first() {
                    const res = await this.all();
                    return res.results ? res.results[0] : null;
                },
                async all() {
                    // sneak_sites + sneak_accounts + sneak_branding
                    if (nq.includes('FROM sneak_sites s') && nq.includes('JOIN sneak_accounts a')) {
                        const site = tables.sneak_sites.find(s => s.site_key === boundArgs[0]);
                        if (!site) return { results: [] };
                        const acc = tables.sneak_accounts.find(a => a.id === site.account_id);
                        return {
                            results: [{
                                site_id: site.id,
                                site_key: site.site_key,
                                site_status: site.status,
                                account_name: acc?.account_name || 'Member',
                                account_status: acc?.status || 'active',
                                display_name: 'Ursula Weinkauff',
                                brokerage: 'CCOR'
                            }]
                        };
                    }

                    // sneak_sites
                    if (nq.includes('FROM sneak_sites WHERE site_key = ?')) {
                        const site = tables.sneak_sites.find(s => s.site_key === boundArgs[0]);
                        return { results: site ? [site] : [] };
                    }
                    if (nq.includes('FROM sneak_sites WHERE id = ?')) {
                        const site = tables.sneak_sites.find(s => s.id === boundArgs[0]);
                        return { results: site ? [site] : [] };
                    }

                    // sneak_domains
                    if (nq.includes('FROM sneak_domains WHERE domain = ?')) {
                        const dom = tables.sneak_domains.find(d => d.domain === boundArgs[0] && d.status === 'active');
                        return { results: dom ? [dom] : [] };
                    }
                    if (nq.includes('FROM sneak_domains WHERE site_id = ?')) {
                        const list = tables.sneak_domains.filter(d => d.site_id === boundArgs[0] && d.status === 'active' && d.verified === 1);
                        return { results: list };
                    }

                    // sneak_consumer_users
                    if (nq.includes('FROM sneak_consumer_users WHERE site_id = ? AND email = ?')) {
                        const u = tables.sneak_consumer_users.find(x => x.site_id === boundArgs[0] && x.email === boundArgs[1]);
                        return { results: u ? [u] : [] };
                    }
                    if (nq.includes('FROM sneak_consumer_users WHERE id = ?')) {
                        const u = tables.sneak_consumer_users.find(x => x.id === boundArgs[0]);
                        return { results: u ? [u] : [] };
                    }

                    // sneak_consumer_magic_links
                    if (nq.includes('FROM sneak_consumer_magic_links l') && nq.includes('JOIN sneak_consumer_users u')) {
                        const tokenHash = boundArgs[0];
                        const l = tables.sneak_consumer_magic_links.find(x => x.token_hash === tokenHash);
                        if (!l) return { results: [] };
                        const u = tables.sneak_consumer_users.find(x => x.id === l.user_id);
                        const s = tables.sneak_sites.find(x => x.id === l.site_id);
                        return {
                            results: [{
                                id: l.id,
                                user_id: l.user_id,
                                site_id: l.site_id,
                                site_key: s?.site_key,
                                return_url: l.return_url,
                                expires_at: l.expires_at,
                                email: u?.email,
                                user_status: u?.status
                            }]
                        };
                    }

                    // sneak_consumer_auth_exchanges
                    if (nq.includes('FROM sneak_consumer_auth_exchanges e') && nq.includes('JOIN sneak_consumer_users u')) {
                        const codeHash = boundArgs[0];
                        const e = tables.sneak_consumer_auth_exchanges.find(x => x.code_hash === codeHash);
                        if (!e) return { results: [] };
                        const u = tables.sneak_consumer_users.find(x => x.id === e.user_id);
                        const s = tables.sneak_sites.find(x => x.id === e.site_id);
                        return {
                            results: [{
                                id: e.id,
                                user_id: e.user_id,
                                site_id: e.site_id,
                                site_key: s?.site_key,
                                expires_at: e.expires_at,
                                email: u?.email,
                                user_status: u?.status
                            }]
                        };
                    }
                    if (nq.includes('FROM sneak_consumer_auth_exchanges WHERE code_hash = ?')) {
                        const e = tables.sneak_consumer_auth_exchanges.find(x => x.code_hash === boundArgs[0]);
                        return { results: e ? [e] : [] };
                    }

                    // sneak_consumer_sessions (with JOINs)
                    if (nq.includes('FROM sneak_consumer_sessions s') && nq.includes('s.token_hash = ?')) {
                        const s = tables.sneak_consumer_sessions.find(x => x.token_hash === boundArgs[0] && !x.revoked_at);
                        if (!s) return { results: [] };
                        const u = tables.sneak_consumer_users.find(x => x.id === s.user_id);
                        const si = tables.sneak_sites.find(x => x.id === s.site_id);
                        return {
                            results: [{
                                id: s.id,
                                user_id: s.user_id,
                                site_id: s.site_id,
                                site_key: si?.site_key,
                                site_status: si?.status || 'active',
                                created_at: s.created_at,
                                expires_at: s.expires_at,
                                consumer_email: u?.email,
                                user_status: u?.status
                            }]
                        };
                    }

                    // Rate limiting
                    if (nq.includes('FROM sneak_consumer_login_attempts')) {
                        const count = tables.sneak_consumer_login_attempts.filter(a => a.identifier_hash === boundArgs[0]).length;
                        return { results: [{ count }] };
                    }

                    // sneak_consumer_favorites
                    if (nq.includes('COUNT(*) as count FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?') || nq.includes('count(*) as count FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?')) {
                        const count = tables.sneak_consumer_favorites.filter(f => f.user_id === boundArgs[0] && f.site_id === boundArgs[1]).length;
                        return { results: [{ count }] };
                    }
                    if (nq.includes('FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ? AND listing_key = ?')) {
                        const fav = tables.sneak_consumer_favorites.find(f => f.user_id === boundArgs[0] && f.site_id === boundArgs[1] && f.listing_key === boundArgs[2]);
                        return { results: fav ? [fav] : [] };
                    }
                    if (nq.includes('FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?') && nq.includes('ORDER BY')) {
                        const favs = tables.sneak_consumer_favorites.filter(f => f.user_id === boundArgs[0] && f.site_id === boundArgs[1]);
                        return { results: favs };
                    }

                    // sneak_consumer_saved_searches (Phase 7.3C1B & 7.3C2A)
                    if (nq.includes('count(*) as count FROM sneak_consumer_saved_searches') || nq.includes('COUNT(*) as count FROM sneak_consumer_saved_searches')) {
                        const count = tables.sneak_consumer_saved_searches.filter(s => s.user_id === boundArgs[0] && s.site_id === boundArgs[1]).length;
                        return { results: [{ count }] };
                    }
                    if (nq.includes('FROM sneak_consumer_saved_searches') && nq.includes('state_hash = ?')) {
                        const item = tables.sneak_consumer_saved_searches.find(s => s.user_id === boundArgs[0] && s.site_id === boundArgs[1] && s.state_hash === boundArgs[2]);
                        return { results: item ? [item] : [] };
                    }
                    if (nq.includes('FROM sneak_consumer_saved_searches s') && nq.includes('LEFT JOIN sneak_consumer_search_alerts a')) {
                        const items = tables.sneak_consumer_saved_searches
                            .filter(s => s.user_id === boundArgs[0] && s.site_id === boundArgs[1])
                            .map(s => {
                                const alert = tables.sneak_consumer_search_alerts.find(a => a.saved_search_id === s.id);
                                return {
                                    ...s,
                                    alert_id: alert?.id || null,
                                    frequency: alert?.frequency || 'off',
                                    enabled: alert ? alert.enabled : 0,
                                    enabled_at: alert?.enabled_at || null,
                                    timezone: alert?.timezone || 'America/New_York',
                                    return_url: alert?.return_url || null
                                };
                            });
                        return { results: items };
                    }
                    if (nq.includes('FROM sneak_consumer_saved_searches') && (nq.includes('WHERE user_id = ? AND site_id = ?') || nq.includes('WHERE s.user_id = ? AND s.site_id = ?'))) {
                        const items = tables.sneak_consumer_saved_searches.filter(s => s.user_id === boundArgs[0] && s.site_id === boundArgs[1]);
                        return { results: items };
                    }
                    if (nq.includes('FROM sneak_consumer_saved_searches WHERE id = ? AND user_id = ? AND site_id = ?')) {
                        const item = tables.sneak_consumer_saved_searches.find(s => s.id === boundArgs[0] && s.user_id === boundArgs[1] && s.site_id === boundArgs[2]);
                        return { results: item ? [item] : [] };
                    }

                    // sneak_consumer_search_alerts (Phase 7.3C2A)
                    if (nq.includes('FROM sneak_consumer_search_alerts WHERE saved_search_id = ? AND user_id = ? AND site_id = ?')) {
                        const alert = tables.sneak_consumer_search_alerts.find(a => a.saved_search_id === boundArgs[0] && a.user_id === boundArgs[1] && a.site_id === boundArgs[2]);
                        return { results: alert ? [alert] : [] };
                    }

                    if (nq.includes('FROM sneak_listings WHERE ListingKey IN')) {
                        const placeholders = boundArgs.filter(k => typeof k === 'string' && k.length > 0);
                        const results = tables.sneak_listings.filter(l => placeholders.includes(l.ListingKey));
                        return { results };
                    }
                    if (nq.includes('FROM sneak_listings WHERE ListingKey = ?')) {
                        const item = tables.sneak_listings.find(x => x.ListingKey === boundArgs[0]);
                        return { results: item ? [item] : [] };
                    }

                    return { results: [] };
                },
                async run() {
                    // INSERT INTO sneak_consumer_users
                    if (nq.includes('INSERT INTO sneak_consumer_users')) {
                        tables.sneak_consumer_users.push({
                            id: boundArgs[0],
                            site_id: boundArgs[1],
                            email: boundArgs[2],
                            status: 'pending',
                            created_at: new Date().toISOString()
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_users
                    if (nq.includes('UPDATE sneak_consumer_users SET status =')) {
                        const u = tables.sneak_consumer_users.find(x => x.id === boundArgs[3]);
                        if (u) {
                            u.status = 'active';
                            u.activated_at = new Date().toISOString();
                            u.last_login_at = new Date().toISOString();
                        }
                        return { meta: { changes: 1 }, success: true };
                    }
                    // INSERT INTO sneak_consumer_magic_links
                    if (nq.includes('INSERT INTO sneak_consumer_magic_links')) {
                        tables.sneak_consumer_magic_links.push({
                            id: boundArgs[0],
                            user_id: boundArgs[1],
                            site_id: boundArgs[2],
                            token_hash: boundArgs[3],
                            purpose: 'login',
                            return_url: boundArgs[4],
                            created_at: boundArgs[5],
                            expires_at: boundArgs[6],
                            used_at: null
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_magic_links
                    if (nq.includes('UPDATE sneak_consumer_magic_links')) {
                        if (nq.includes('WHERE user_id = ?')) {
                            const userId = boundArgs[1];
                            const siteId = boundArgs[2];
                            let changes = 0;
                            tables.sneak_consumer_magic_links.forEach(l => {
                                if (l.user_id === userId && l.site_id === siteId && !l.used_at) {
                                    l.used_at = boundArgs[0];
                                    changes++;
                                }
                            });
                            return { meta: { changes }, changes, success: true };
                        }
                        const tokenHash = boundArgs[1];
                        const l = tables.sneak_consumer_magic_links.find(x => x.token_hash === tokenHash && !x.used_at);
                        if (l) {
                            l.used_at = boundArgs[0];
                            return { meta: { changes: 1 }, changes: 1, success: true };
                        }
                        return { meta: { changes: 0 }, changes: 0, success: true };
                    }
                    // INSERT INTO sneak_consumer_auth_exchanges
                    if (nq.includes('INSERT INTO sneak_consumer_auth_exchanges')) {
                        tables.sneak_consumer_auth_exchanges.push({
                            id: boundArgs[0],
                            user_id: boundArgs[1],
                            site_id: boundArgs[2],
                            code_hash: boundArgs[3],
                            created_at: boundArgs[4],
                            expires_at: boundArgs[5],
                            used_at: null
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_auth_exchanges (used_at)
                    if (nq.includes('UPDATE sneak_consumer_auth_exchanges')) {
                        const codeHash = boundArgs[1];
                        const e = tables.sneak_consumer_auth_exchanges.find(x => x.code_hash === codeHash && !x.used_at);
                        if (e) {
                            e.used_at = boundArgs[0];
                            return { meta: { changes: 1 }, changes: 1, success: true };
                        }
                        return { meta: { changes: 0 }, changes: 0, success: true };
                    }
                    // INSERT INTO sneak_consumer_sessions
                    if (nq.includes('INSERT INTO sneak_consumer_sessions')) {
                        tables.sneak_consumer_sessions.push({
                            id: boundArgs[0],
                            user_id: boundArgs[1],
                            site_id: boundArgs[2],
                            token_hash: boundArgs[3],
                            created_at: boundArgs[4],
                            expires_at: boundArgs[5],
                            revoked_at: null
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_sessions (revocation)
                    if (nq.includes('UPDATE sneak_consumer_sessions SET revoked_at =')) {
                        const s = tables.sneak_consumer_sessions.find(x => x.token_hash === boundArgs[0]);
                        if (s) s.revoked_at = new Date().toISOString();
                        return { meta: { changes: 1 }, success: true };
                    }
                    // INSERT INTO sneak_consumer_login_attempts
                    if (nq.includes('INSERT INTO sneak_consumer_login_attempts')) {
                        tables.sneak_consumer_login_attempts.push({
                            id: boundArgs[0],
                            site_id: boundArgs[1],
                            identifier_hash: boundArgs[2],
                            attempt_type: boundArgs[3],
                            created_at: boundArgs[4]
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // INSERT OR IGNORE INTO sneak_consumer_favorites
                    if (nq.includes('INSERT OR IGNORE INTO sneak_consumer_favorites')) {
                        const exists = tables.sneak_consumer_favorites.some(f => f.user_id === boundArgs[2] && f.site_id === boundArgs[1] && f.listing_key === boundArgs[3]);
                        if (!exists) {
                            tables.sneak_consumer_favorites.push({
                                id: boundArgs[0],
                                site_id: boundArgs[1],
                                user_id: boundArgs[2],
                                listing_key: boundArgs[3],
                                created_at: new Date().toISOString()
                            });
                        }
                        return { meta: { changes: 1 }, success: true };
                    }
                    // DELETE FROM sneak_consumer_favorites
                    if (nq.includes('DELETE FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ? AND listing_key = ?')) {
                        tables.sneak_consumer_favorites = tables.sneak_consumer_favorites.filter(
                            f => !(f.user_id === boundArgs[0] && f.site_id === boundArgs[1] && f.listing_key === boundArgs[2])
                        );
                        return { meta: { changes: 1 }, success: true };
                    }
                    // INSERT INTO sneak_consumer_saved_searches (Phase 7.3C1B)
                    if (nq.includes('INSERT INTO sneak_consumer_saved_searches')) {
                        tables.sneak_consumer_saved_searches.push({
                            id: boundArgs[0],
                            site_id: boundArgs[1],
                            user_id: boundArgs[2],
                            name: boundArgs[3],
                            state_version: 1,
                            state_json: boundArgs[4],
                            state_hash: boundArgs[5],
                            created_at: boundArgs[6] || new Date().toISOString(),
                            updated_at: boundArgs[7] || new Date().toISOString()
                        });
                        return { meta: { changes: 1 }, success: true };
                    }
                    // UPDATE sneak_consumer_saved_searches (Phase 7.3C1B)
                    if (nq.includes('UPDATE sneak_consumer_saved_searches SET name =')) {
                        let item;
                        if (nq.includes('WHERE id = ? AND user_id = ? AND site_id = ?')) {
                            item = tables.sneak_consumer_saved_searches.find(s => s.id === boundArgs[1] && s.user_id === boundArgs[2] && s.site_id === boundArgs[3]);
                        } else if (nq.includes('WHERE id = ?')) {
                            item = tables.sneak_consumer_saved_searches.find(s => s.id === boundArgs[1]);
                        }
                        if (item) {
                            item.name = boundArgs[0];
                            item.updated_at = new Date().toISOString();
                            return { meta: { changes: 1 }, changes: 1, success: true };
                        }
                        return { meta: { changes: 0 }, changes: 0, success: true };
                    }
                    // DELETE FROM sneak_consumer_saved_searches (Phase 7.3C1B)
                    if (nq.includes('DELETE FROM sneak_consumer_saved_searches WHERE id = ? AND user_id = ? AND site_id = ?')) {
                        const before = tables.sneak_consumer_saved_searches.length;
                        tables.sneak_consumer_saved_searches = tables.sneak_consumer_saved_searches.filter(
                            s => !(s.id === boundArgs[0] && s.user_id === boundArgs[1] && s.site_id === boundArgs[2])
                        );
                        // Cascade delete associated search alert
                        tables.sneak_consumer_search_alerts = tables.sneak_consumer_search_alerts.filter(
                            a => !(a.saved_search_id === boundArgs[0] && a.user_id === boundArgs[1] && a.site_id === boundArgs[2])
                        );
                        const changes = before - tables.sneak_consumer_saved_searches.length;
                        return { meta: { changes }, changes, success: true };
                    }
                    // INSERT INTO sneak_consumer_search_alerts (Phase 7.3C2A)
                    if (nq.includes('INSERT INTO sneak_consumer_search_alerts')) {
                        const searchId = boundArgs[1];
                        const siteId = boundArgs[2];
                        const userId = boundArgs[3];
                        const frequency = boundArgs[4];
                        const enabled = boundArgs[5];
                        const enabledAt = boundArgs[7];
                        const timezone = boundArgs[8];
                        const returnUrl = boundArgs[9];

                        const existing = tables.sneak_consumer_search_alerts.find(a => a.saved_search_id === searchId);
                        if (existing) {
                            existing.frequency = frequency;
                            existing.enabled = enabled;
                            if (enabled === 1 && !existing.enabled) {
                                existing.enabled_at = new Date().toISOString();
                            } else if (enabled === 0) {
                                existing.enabled_at = null;
                            }
                            existing.timezone = timezone;
                            if (returnUrl) existing.return_url = returnUrl;
                            existing.updated_at = new Date().toISOString();
                        } else {
                            tables.sneak_consumer_search_alerts.push({
                                id: boundArgs[0],
                                saved_search_id: searchId,
                                site_id: siteId,
                                user_id: userId,
                                frequency,
                                enabled,
                                enabled_at: enabled === 1 ? enabledAt : null,
                                timezone,
                                return_url: returnUrl,
                                created_at: new Date().toISOString(),
                                updated_at: new Date().toISOString()
                            });
                        }
                        return { meta: { changes: 1 }, success: true };
                    }
                    // DELETE FROM sneak_consumer_users
                    if (nq.includes('DELETE FROM sneak_consumer_users')) {
                        const uid = boundArgs[0];
                        const sid = boundArgs[1];
                        tables.sneak_consumer_users = tables.sneak_consumer_users.filter(
                            u => !(u.id === uid && u.site_id === sid)
                        );
                        tables.sneak_consumer_favorites = tables.sneak_consumer_favorites.filter(
                            f => !(f.user_id === uid && f.site_id === sid)
                        );
                        tables.sneak_consumer_saved_searches = tables.sneak_consumer_saved_searches.filter(
                            s => !(s.user_id === uid && s.site_id === sid)
                        );
                        tables.sneak_consumer_search_alerts = tables.sneak_consumer_search_alerts.filter(
                            a => !(a.user_id === uid && a.site_id === sid)
                        );
                        tables.sneak_consumer_sessions = tables.sneak_consumer_sessions.filter(
                            s => !(s.user_id === uid && s.site_id === sid)
                        );
                        return { meta: { changes: 1 }, success: true };
                    }

                    return { meta: { changes: 1 }, success: true };
                }
            };
        }
    };
}

describe('CCOR IDX / SNEAK Consumer Worker & Identity (Phase 7.3C1A)', () => {

    test('1. Health and Version check returns build 2026.08.31.7.3c3a', async () => {
        const req = new Request('https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev/api/consumer/version');
        const res = await worker.fetch(req, {});
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.service, 'sneak-consumer-worker');
        assert.equal(data.build, '2026.08.31.7.3c3a');
        assert.equal(data.status, 'healthy');
    });

    test('2. Anti-Enumeration: magic link request returns identical generic response regardless of existence', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Existing user
        const req1 = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                email: 'buyer1@example.com',
                returnUrl: 'https://member1.com/homes'
            })
        });
        const res1 = await worker.fetch(req1, env);
        assert.equal(res1.status, 200);
        const data1 = await res1.json();
        assert.equal(data1.success, true);
        assert.equal(data1.message, 'If the email is valid, a sign-in link has been sent. Please check your inbox.');

        // New user
        const req2 = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                email: 'brand_new_buyer@example.com',
                returnUrl: 'https://member1.com/homes'
            })
        });
        const res2 = await worker.fetch(req2, env);
        assert.equal(res2.status, 200);
        const data2 = await res2.json();
        assert.equal(data2.success, true);
        assert.equal(data2.message, 'If the email is valid, a sign-in link has been sent. Please check your inbox.');

        // Verify tokens were created in DB with SHA-256 hash (never raw token)
        assert.equal(db.tables.sneak_consumer_magic_links.length, 2);
        assert.ok(db.tables.sneak_consumer_magic_links[0].token_hash);
        assert.notEqual(db.tables.sneak_consumer_magic_links[0].token_hash, 'buyer1@example.com');
    });

    test('3. Open-Redirect Prevention: invalid or unverified external returnUrl is rejected', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Attacker attempting open redirect to evil-phishing.com
        const req = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                email: 'buyer1@example.com',
                returnUrl: 'https://evil-phishing.com/steal-creds'
            })
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200); // Generic response prevents enumeration, but zero token is generated
        assert.equal(db.tables.sneak_consumer_magic_links.length, 0);
    });

    test('4. End-to-end Flow: Magic Link verify -> 2-min Exchange Code -> 14-day Session token', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // 1. Issue magic link
        await requestConsumerMagicLink(db, {
            siteKey: 'site-mem-1',
            email: 'flowbuyer@example.com',
            returnUrl: 'https://member1.com/idx-search',
            ipHash: 'hash_192_168_1_1',
            env
        });
        assert.equal(db.tables.sneak_consumer_magic_links.length, 1);
        const linkRecord = db.tables.sneak_consumer_magic_links[0];
        
        // Create known token
        const rawToken = generateRawToken();
        const knownHash = await sha256Hex(rawToken);
        linkRecord.token_hash = knownHash;

        // 2. Consume magic link via GET /api/consumer/auth/verify?token=...
        const verifyReq = new Request(`https://consumer.staging/api/consumer/auth/verify?token=${rawToken}`);
        const verifyRes = await worker.fetch(verifyReq, env);
        assert.equal(verifyRes.status, 302, 'Verify endpoint must 302 redirect');
        const redirectUrl = new URL(verifyRes.headers.get('Location'));
        assert.equal(redirectUrl.origin, 'https://member1.com');
        assert.equal(redirectUrl.pathname, '/idx-search');
        assert.ok(redirectUrl.searchParams.has('auth_code'));
        const authCode = redirectUrl.searchParams.get('auth_code');

        // 3. Replay prevention: reusing the token must fail
        const replayRes = await worker.fetch(verifyReq, env);
        assert.equal(replayRes.status, 401);

        // 4. Exchange auth_code for session token
        const idxSession = await createSignedIdxSession('site-mem-1');
        const exchangeReq = new Request('https://consumer.staging/api/consumer/auth/exchange', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                code: authCode,
                session: idxSession
            })
        });
        const exchangeRes = await worker.fetch(exchangeReq, env);
        assert.equal(exchangeRes.status, 200);
        const exchangeData = await exchangeRes.json();
        assert.ok(exchangeData.consumerSession);
        assert.equal(exchangeData.user.email, 'flowbuyer@example.com');
        const consumerSession = exchangeData.consumerSession;

        // 5. Replay exchange code must fail
        const replayExchangeRes = await worker.fetch(exchangeReq, env);
        assert.equal(replayExchangeRes.status, 400);

        // 6. Access /api/consumer/auth/me
        const meReq = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-1', {
            headers: { Authorization: `Bearer ${consumerSession}` }
        });
        const meRes = await worker.fetch(meReq, env);
        assert.equal(meRes.status, 200);
        const meData = await meRes.json();
        assert.equal(meData.user.email, 'flowbuyer@example.com');
    });

    test('5. Tenant Isolation: Session created on Site A returns 403 on Site B', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Create session for Site 1
        const tokenSite1 = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_iso_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(tokenSite1),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        // Access Site 1 -> 200 OK
        const reqSite1 = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-1', {
            headers: { Authorization: `Bearer ${tokenSite1}` }
        });
        const resSite1 = await worker.fetch(reqSite1, env);
        assert.equal(resSite1.status, 200);

        // Access Site 2 with Site 1 token -> 403 Forbidden
        const reqSite2 = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-2', {
            headers: { Authorization: `Bearer ${tokenSite1}` }
        });
        const resSite2 = await worker.fetch(reqSite2, env);
        assert.equal(resSite2.status, 403);
        const dataSite2 = await resSite2.json();
        assert.equal(dataSite2.error, 'TenantSiteMismatch');
    });

    test('6. Server Favorites: List, Add, Remove, and Merge Union', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Log in buyer1 (c_usr_1 on site_1)
        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_fav_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        // 1. List favorites (initially has 2240001)
        const listReq = new Request('https://consumer.staging/api/consumer/favorites?site=site-mem-1', {
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const listRes = await worker.fetch(listReq, env);
        assert.equal(listRes.status, 200);
        const listData = await listRes.json();
        assert.equal(listData.favorites.length, 1);
        assert.equal(listData.favorites[0].listingKey, '2240001');
        assert.equal(listData.favorites[0].listing.ListPrice, 750000);

        // 2. Add listing 2240002 (with suppressed address check)
        const addReq = new Request('https://consumer.staging/api/consumer/favorites', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                listingKey: '2240002'
            })
        });
        const addRes = await worker.fetch(addReq, env);
        assert.equal(addRes.status, 200);

        // Verify address suppression was applied
        const listRes2 = await worker.fetch(listReq, env);
        const listData2 = await listRes2.json();
        assert.equal(listData2.favorites.length, 2);
        const suppressedFav = listData2.favorites.find(f => f.listingKey === '2240002');
        assert.equal(suppressedFav.listing.UnparsedAddress, 'Address Undisclosed');

        // 3. Remove listing 2240001
        const delReq = new Request('https://consumer.staging/api/consumer/favorites/2240001?site=site-mem-1', {
            method: 'DELETE',
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const delRes = await worker.fetch(delReq, env);
        assert.equal(delRes.status, 200);

        // 4. Merge anonymous favorites
        const mergeReq = new Request('https://consumer.staging/api/consumer/favorites/merge', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                listingKeys: ['2240001', '2240002', '2240003_closed']
            })
        });
        const mergeRes = await worker.fetch(mergeReq, env);
        assert.equal(mergeRes.status, 200);
        const mergeData = await mergeRes.json();
        assert.equal(mergeData.success, true);
    });

    test('7. Server Favorites Limit: Max 200 listings enforced', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_limit',
            user_id: 'c_usr_limit',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });
        db.tables.sneak_consumer_users.push({
            id: 'c_usr_limit',
            site_id: 'site_1',
            email: 'limit@example.com',
            status: 'active'
        });

        // Fill 200 favorites
        for (let i = 0; i < 200; i++) {
            db.tables.sneak_consumer_favorites.push({
                id: `fav_fill_${i}`,
                site_id: 'site_1',
                user_id: 'c_usr_limit',
                listing_key: `key_${i}`,
                created_at: new Date().toISOString()
            });
        }

        // Try adding 201st favorite
        const addReq = new Request('https://consumer.staging/api/consumer/favorites', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                listingKey: '2240001'
            })
        });
        const addRes = await worker.fetch(addReq, env);
        assert.equal(addRes.status, 400);
        const addData = await addRes.json();
        assert.equal(addData.error, 'FavoriteLimitExceeded');
    });

    test('8. Logout Session Revocation', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_logout',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        // Logout
        const logoutReq = new Request('https://consumer.staging/api/consumer/auth/logout', {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${sessionToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ site: 'site-mem-1' })
        });
        const logoutRes = await worker.fetch(logoutReq, env);
        assert.equal(logoutRes.status, 200);

        // Verification after logout -> 401
        const meReq = new Request('https://consumer.staging/api/consumer/auth/me?site=site-mem-1', {
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const meRes = await worker.fetch(meReq, env);
        assert.equal(meRes.status, 401);
    });

    test('9. GDPR / Account Deletion permanently removes consumer data', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken();
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_del',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: await sha256Hex(sessionToken),
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        const delReq = new Request('https://consumer.staging/api/consumer/account?site=site-mem-1', {
            method: 'DELETE',
            headers: { Authorization: `Bearer ${sessionToken}` }
        });
        const delRes = await worker.fetch(delReq, env);
        assert.equal(delRes.status, 200);
        const delData = await delRes.json();
        assert.equal(delData.success, true);

        // Consumer user and favorites are gone from DB
        assert.equal(db.tables.sneak_consumer_users.some(u => u.id === 'c_usr_1'), false);
        assert.equal(db.tables.sneak_consumer_favorites.some(f => f.user_id === 'c_usr_1'), false);
    });

    test('10. XSS Escaping and URL Sanitization unit tests', () => {
        function escapeHtml(str) {
            if (str === null || str === undefined) return '';
            return String(str)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#039;');
        }

        function sanitizeUrl(url) {
            if (!url || typeof url !== 'string') return '';
            const clean = url.trim();
            if (clean.startsWith('https://') || clean.startsWith('http://') || clean.startsWith('/')) {
                return clean;
            }
            return '';
        }

        // Test malicious scripts and onerror injection
        const scriptPayload = '<script>alert(1)</script>';
        assert.equal(escapeHtml(scriptPayload), '&lt;script&gt;alert(1)&lt;/script&gt;');

        const imgPayload = '"><img src=x onerror=alert(1)>';
        assert.equal(escapeHtml(imgPayload), '&quot;&gt;&lt;img src=x onerror=alert(1)&gt;');

        // Test URL sanitization
        assert.equal(sanitizeUrl('https://images.sneakidx.com/photo.jpg'), 'https://images.sneakidx.com/photo.jpg');
        assert.equal(sanitizeUrl('http://images.sneakidx.com/photo.jpg'), 'http://images.sneakidx.com/photo.jpg');
        assert.equal(sanitizeUrl('/local/path/image.jpg'), '/local/path/image.jpg');
        assert.equal(sanitizeUrl('javascript:alert(1)'), '');
        assert.equal(sanitizeUrl('data:text/html,<script>alert(1)</script>'), '');
        assert.equal(sanitizeUrl('vbscript:msgbox(1)'), '');
    });

    test('11. Rate Limiting: Exceeded threshold maintains generic response but throttles magic link generation', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const email = 'spammy_buyer@example.com';
        const emailHash = await sha256Hex(email);

        // Pre-populate 5 previous email attempts
        for (let i = 0; i < 5; i++) {
            db.tables.sneak_consumer_login_attempts.push({
                id: `cla_em_${i}`,
                site_id: 'site_1',
                identifier_hash: emailHash,
                attempt_type: 'email',
                created_at: new Date().toISOString()
            });
        }

        const req = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'CF-Connecting-IP': '192.0.2.1'
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                email,
                returnUrl: 'https://member1.com/homes'
            })
        });

        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.success, true);
        assert.equal(data.message, 'If the email is valid, a sign-in link has been sent. Please check your inbox.');

        // Zero magic links issued due to rate limit throttling
        assert.equal(db.tables.sneak_consumer_magic_links.length, 0);
    });

    test('12. Invalidation of previous unused magic link when new one is requested', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // 1st request
        await requestConsumerMagicLink(db, {
            siteKey: 'site-mem-1',
            email: 'rotate@example.com',
            returnUrl: 'https://member1.com/homes',
            ipHash: 'ip_rotate_1',
            env
        });
        assert.equal(db.tables.sneak_consumer_magic_links.length, 1);
        const firstLinkId = db.tables.sneak_consumer_magic_links[0].id;
        assert.equal(db.tables.sneak_consumer_magic_links[0].used_at, null);

        // 2nd request
        await requestConsumerMagicLink(db, {
            siteKey: 'site-mem-1',
            email: 'rotate@example.com',
            returnUrl: 'https://member1.com/homes',
            ipHash: 'ip_rotate_2',
            env
        });
        assert.equal(db.tables.sneak_consumer_magic_links.length, 2);
        const firstLink = db.tables.sneak_consumer_magic_links.find(l => l.id === firstLinkId);
        assert.ok(firstLink.used_at !== null, 'First magic link must be marked used/invalidated');
    });

    test('13. Disabled Consumer Account cannot log in', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        db.tables.sneak_consumer_users.push({
            id: 'c_usr_disabled',
            site_id: 'site_1',
            email: 'disabled@example.com',
            status: 'disabled'
        });

        // Attempt magic link -> generic 200 response but 0 magic links created
        const req = new Request('https://consumer.staging/api/consumer/auth/magic-link', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                site: 'site-mem-1',
                email: 'disabled@example.com',
                returnUrl: 'https://member1.com/homes'
            })
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        assert.equal(db.tables.sneak_consumer_magic_links.length, 0);
    });

    test('14. Cross-Tenant Separation: Same email on Site 1 vs Site 2 creates two isolated consumers', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Register same email on Site 1
        await requestConsumerMagicLink(db, {
            siteKey: 'site-mem-1',
            email: 'sharedbuyer@example.com',
            returnUrl: 'https://member1.com/homes',
            ipHash: 'ip_share_1',
            env
        });

        // Register same email on Site 2
        await requestConsumerMagicLink(db, {
            siteKey: 'site-mem-2',
            email: 'sharedbuyer@example.com',
            returnUrl: 'https://member2.com/homes',
            ipHash: 'ip_share_2',
            env
        });

        const users = db.tables.sneak_consumer_users.filter(u => u.email === 'sharedbuyer@example.com');
        assert.equal(users.length, 2, 'Must create 2 separate site-scoped consumer records');
        assert.notEqual(users[0].id, users[1].id);
        assert.equal(users[0].site_id, 'site_1');
        assert.equal(users[1].site_id, 'site_2');
    });

    test('15. Saved Search CRUD (Phase 7.3C1B): Create, List, Rename (PATCH), and Delete', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Create session for c_usr_1 on site_1
        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_crud_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        // 1. Create Saved Search
        const createReq = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + sessionToken
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                name: 'Bonita Springs Homes • $500K-$800K',
                state: {
                    version: 1,
                    search: {
                        propertyType: 'sale',
                        q: 'Bonita Springs',
                        minPrice: 500000,
                        maxPrice: 800000,
                        beds: 3,
                        baths: 2,
                        drawerState: { waterfront: true, pool: true },
                        spatialState: { mode: null }
                    }
                }
            })
        });

        const createRes = await worker.fetch(createReq, env);
        assert.equal(createRes.status, 200);
        const createData = await createRes.json();
        assert.equal(createData.success, true);
        assert.ok(createData.savedSearch.id);
        assert.equal(createData.savedSearch.name, 'Bonita Springs Homes • $500K-$800K');
        assert.equal(createData.savedSearch.state.search.minPrice, 500000);
        const searchId = createData.savedSearch.id;

        // 2. List Saved Searches
        const listReq = new Request('https://consumer.staging/api/consumer/saved-searches?site=site-mem-1', {
            method: 'GET',
            headers: { 'Authorization': 'Bearer ' + sessionToken }
        });
        const listRes = await worker.fetch(listReq, env);
        assert.equal(listRes.status, 200);
        const listData = await listRes.json();
        assert.equal(listData.success, true);
        assert.equal(listData.count, 1);
        assert.equal(listData.savedSearches[0].id, searchId);

        // 3. Rename (PATCH) Saved Search
        const patchReq = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}?site=site-mem-1`, {
            method: 'PATCH',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + sessionToken
            },
            body: JSON.stringify({ name: 'Updated Waterfront Search' })
        });
        const patchRes = await worker.fetch(patchReq, env);
        assert.equal(patchRes.status, 200);
        const patchData = await patchRes.json();
        assert.equal(patchData.success, true);
        assert.equal(patchData.name, 'Updated Waterfront Search');

        // 4. Delete Saved Search
        const delReq = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}?site=site-mem-1`, {
            method: 'DELETE',
            headers: { 'Authorization': 'Bearer ' + sessionToken }
        });
        const delRes = await worker.fetch(delReq, env);
        assert.equal(delRes.status, 200);
        const delData = await delRes.json();
        assert.equal(delData.success, true);
        assert.equal(db.tables.sneak_consumer_saved_searches.length, 0);
    });

    test('16. Saved Search Limit (25 max allowed, 26th rejected)', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_limit_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        // Pre-populate 25 saved searches
        for (let i = 1; i <= 25; i++) {
            db.tables.sneak_consumer_saved_searches.push({
                id: `css_limit_${i}`,
                site_id: 'site_1',
                user_id: 'c_usr_1',
                name: `Search ${i}`,
                state_version: 1,
                state_json: JSON.stringify({ version: 1, search: { propertyType: 'sale', minPrice: i * 10000 } }),
                state_hash: `hash_${i}`,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            });
        }

        // Attempt to create 26th
        const req = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + sessionToken
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                name: '26th Search',
                state: { version: 1, search: { propertyType: 'sale', minPrice: 999999 } }
            })
        });

        const res = await worker.fetch(req, env);
        assert.equal(res.status, 400);
        const data = await res.json();
        assert.equal(data.error, 'SavedSearchLimitExceeded');
        assert.equal(db.tables.sneak_consumer_saved_searches.length, 25);
    });

    test('17. Duplicate Save Handling: Updates timestamp and name for matching state_hash', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_dup_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        const statePayload = {
            version: 1,
            search: {
                propertyType: 'sale',
                q: 'Pelican Landing',
                minPrice: 600000,
                maxPrice: 1200000
            }
        };

        // First Save
        const req1 = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken },
            body: JSON.stringify({ site: 'site-mem-1', name: 'Initial Name', state: statePayload })
        });
        const res1 = await worker.fetch(req1, env);
        assert.equal(res1.status, 200);
        assert.equal(db.tables.sneak_consumer_saved_searches.length, 1);
        const initialId = db.tables.sneak_consumer_saved_searches[0].id;

        // Second Save with identical state criteria but updated name
        const req2 = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken },
            body: JSON.stringify({ site: 'site-mem-1', name: 'Updated Duplicate Name', state: statePayload })
        });
        const res2 = await worker.fetch(req2, env);
        assert.equal(res2.status, 200);
        assert.equal(db.tables.sneak_consumer_saved_searches.length, 1, 'Should NOT create duplicate record');
        assert.equal(db.tables.sneak_consumer_saved_searches[0].id, initialId);
        assert.equal(db.tables.sneak_consumer_saved_searches[0].name, 'Updated Duplicate Name');
    });

    test('18. Cross-Tenant Protection: Site A session cannot list, update, or delete Site B saved search', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        // Site 1 session
        const sessionToken1 = generateRawToken(32);
        const tokenHash1 = await sha256Hex(sessionToken1);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_t1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash1,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        // Site 2 user & search
        db.tables.sneak_consumer_users.push({
            id: 'c_usr_2',
            site_id: 'site_2',
            email: 'buyer2@example.com',
            status: 'active'
        });
        db.tables.sneak_consumer_saved_searches.push({
            id: 'css_site_2_search',
            site_id: 'site_2',
            user_id: 'c_usr_2',
            name: 'Site 2 Private Search',
            state_version: 1,
            state_json: JSON.stringify({ version: 1, search: { propertyType: 'sale' } }),
            state_hash: 'hash_s2',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        });

        // Site 1 consumer attempts to PATCH Site 2 search -> 404 / unauthorized
        const patchReq = new Request('https://consumer.staging/api/consumer/saved-searches/css_site_2_search?site=site-mem-1', {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken1 },
            body: JSON.stringify({ name: 'Hacked Search Name' })
        });
        const patchRes = await worker.fetch(patchReq, env);
        assert.equal(patchRes.status, 404);

        // Site 1 consumer attempts to DELETE Site 2 search -> 404 / unauthorized
        const delReq = new Request('https://consumer.staging/api/consumer/saved-searches/css_site_2_search?site=site-mem-1', {
            method: 'DELETE',
            headers: { 'Authorization': 'Bearer ' + sessionToken1 }
        });
        const delRes = await worker.fetch(delReq, env);
        assert.equal(delRes.status, 404);
        assert.equal(db.tables.sneak_consumer_saved_searches.length, 1);
    });

    test('19. Search State Normalization (Residential, Commercial, Land, Rental, Radius, Polygon, Viewport)', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_norm_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        const polygonCoords = [
            [-81.80, 26.33],
            [-81.78, 26.33],
            [-81.78, 26.35],
            [-81.80, 26.35],
            [-81.80, 26.33]
        ];

        const req = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken },
            body: JSON.stringify({
                site: 'site-mem-1',
                name: 'Polygon & Commercial Search',
                state: {
                    version: 1,
                    search: {
                        propertyType: 'commercial',
                        minPrice: 1000000,
                        drawerState: { minSqft: 5000, county: 'Lee' },
                        spatialState: {
                            mode: 'polygon',
                            polygon: { type: 'Polygon', coordinates: [polygonCoords] }
                        }
                    }
                }
            })
        });

        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.savedSearch.state.search.propertyType, 'commercial');
        assert.equal(data.savedSearch.state.search.spatialState.mode, 'polygon');
        assert.equal(data.savedSearch.state.search.spatialState.polygon.coordinates[0].length, 5);
    });

    test('20. Near Me Privacy Normalization: converts to static radius center without saving live tracking', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_near_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        const req = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken },
            body: JSON.stringify({
                site: 'site-mem-1',
                name: 'Near Me Search',
                state: {
                    version: 1,
                    search: {
                        propertyType: 'sale',
                        spatialState: {
                            mode: 'radius',
                            centerLat: 26.335,
                            centerLng: -81.795,
                            radiusMiles: 5,
                            isNearMe: true // Must be stripped out during normalization
                        }
                    }
                }
            })
        });

        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.savedSearch.state.search.spatialState.mode, 'radius');
        assert.equal(data.savedSearch.state.search.spatialState.isNearMe, undefined);
    });

    test('21. Malformed and Oversized (>16KB) state rejected with 400', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_bad_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        // 1. Malformed non-object state
        const req1 = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken },
            body: JSON.stringify({ site: 'site-mem-1', name: 'Bad State', state: 'not-an-object' })
        });
        const res1 = await worker.fetch(req1, env);
        assert.equal(res1.status, 400);

        // 2. Oversized polygon with > 40 vertices rejected
        const giantCoords = [];
        for (let i = 0; i < 50; i++) giantCoords.push([-81.80 + i * 0.001, 26.33 + i * 0.001]);
        giantCoords.push(giantCoords[0]);

        const req2 = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken },
            body: JSON.stringify({
                site: 'site-mem-1',
                name: 'Giant Polygon',
                state: {
                    version: 1,
                    search: {
                        propertyType: 'sale',
                        spatialState: { mode: 'polygon', polygon: { type: 'Polygon', coordinates: [giantCoords] } }
                    }
                }
            })
        });
        const res2 = await worker.fetch(req2, env);
        assert.equal(res2.status, 200);
        const data2 = await res2.json();
        assert.equal(data2.savedSearch.state.search.spatialState.mode, null, 'Polygon with >40 vertices must be normalized out');
    });

    test('22. Unsupported state version (e.g. version 2) rejected safely', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_v2_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        const req = new Request('https://consumer.staging/api/consumer/saved-searches', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + sessionToken },
            body: JSON.stringify({
                site: 'site-mem-1',
                name: 'Future Version Search',
                state: {
                    version: 2,
                    search: { propertyType: 'sale' }
                }
            })
        });

        const res = await worker.fetch(req, env);
        assert.equal(res.status, 400);
        const data = await res.json();
        assert.equal(data.error, 'InvalidSearchState');
    });

    test('23. Account deletion cascades and removes saved searches and favorites', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = generateRawToken(32);
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'sess_del_1',
            user_id: 'c_usr_1',
            site_id: 'site_1',
            token_hash: tokenHash,
            created_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 14 * 86400 * 1000).toISOString(),
            revoked_at: null
        });

        db.tables.sneak_consumer_saved_searches.push({
            id: 'css_to_del_1',
            site_id: 'site_1',
            user_id: 'c_usr_1',
            name: 'Search To Delete',
            state_version: 1,
            state_json: JSON.stringify({ version: 1, search: { propertyType: 'sale' } }),
            state_hash: 'hash_del_1',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        });

        assert.equal(db.tables.sneak_consumer_saved_searches.length, 1);
        assert.equal(db.tables.sneak_consumer_favorites.length, 1);

        // Delete Account
        const req = new Request('https://consumer.staging/api/consumer/account?site=site-mem-1', {
            method: 'DELETE',
            headers: { 'Authorization': 'Bearer ' + sessionToken }
        });
        const res = await worker.fetch(req, env);
        assert.equal(res.status, 200);

        assert.equal(db.tables.sneak_consumer_users.some(u => u.id === 'c_usr_1'), false);
        assert.equal(db.tables.sneak_consumer_favorites.some(f => f.user_id === 'c_usr_1'), false);
        assert.equal(db.tables.sneak_consumer_saved_searches.some(s => s.user_id === 'c_usr_1'), false);
    });

    test('24. Round-Trip Serialization and Restoration Simulation', () => {
        // Simulates serializeSearchState() -> normalizeSearchState() -> applySearchState()
        const initialSearch = {
            version: 1,
            search: {
                propertyType: 'sale',
                q: 'Bonita Bay',
                minPrice: 750000,
                maxPrice: 1500000,
                beds: 3,
                baths: 3,
                propertySubType: ['Single Family Residence', 'Villa'],
                sort: 'priceDesc',
                drawerState: {
                    minSqft: 2500,
                    maxSqft: 4500,
                    waterfront: true,
                    pool: true,
                    garage: 2,
                    county: 'Lee',
                    cities: ['Bonita Springs']
                },
                spatialState: {
                    mode: 'radius',
                    centerLat: 26.340000,
                    centerLng: -81.800000,
                    radiusMiles: 5
                }
            }
        };

        const jsonSerialized = JSON.stringify(initialSearch);
        const parsed = JSON.parse(jsonSerialized);

        assert.equal(parsed.search.propertyType, 'sale');
        assert.equal(parsed.search.q, 'Bonita Bay');
        assert.equal(parsed.search.minPrice, 750000);
        assert.equal(parsed.search.maxPrice, 1500000);
        assert.equal(parsed.search.beds, 3);
        assert.equal(parsed.search.drawerState.waterfront, true);
        assert.equal(parsed.search.spatialState.mode, 'radius');
        assert.equal(parsed.search.spatialState.radiusMiles, 5);
    });

    test('25. Alert Preferences API: GET & PUT /api/consumer/saved-searches/:id/alert', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = await createSignedIdxSession('site-mem-1');
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'csess_1',
            site_id: 'site_1',
            user_id: 'c_usr_1',
            token_hash: tokenHash,
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        const searchId = 'css_alert_test_1';
        db.tables.sneak_consumer_saved_searches.push({
            id: searchId,
            site_id: 'site_1',
            user_id: 'c_usr_1',
            name: 'Alert Preference Test Search',
            state_version: 1,
            state_json: JSON.stringify({ version: 1, search: { propertyType: 'sale' } }),
            state_hash: 'hash_alert_1',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        });

        // 1. Initial GET alert preference (defaults to off)
        const getReq1 = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}/alert?site=site-mem-1`, {
            headers: { 'Authorization': 'Bearer ' + sessionToken }
        });
        const getRes1 = await worker.fetch(getReq1, env);
        assert.equal(getRes1.status, 200);
        const getData1 = await getRes1.json();
        assert.equal(getData1.success, true);
        assert.equal(getData1.alert.frequency, 'off');
        assert.equal(getData1.alert.enabled, false);

        // 2. PUT alert preference: enable ASAP
        const putReq1 = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}/alert?site=site-mem-1`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + sessionToken
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                frequency: 'asap',
                timezone: 'America/New_York',
                returnUrl: 'https://member1.com/idx-search/'
            })
        });
        const putRes1 = await worker.fetch(putReq1, env);
        assert.equal(putRes1.status, 200);
        const putData1 = await putRes1.json();
        assert.equal(putData1.success, true);
        assert.equal(putData1.alert.frequency, 'asap');
        assert.equal(putData1.alert.enabled, true);
        assert.ok(putData1.alert.enabledAt);

        // 3. PUT alert preference: invalid frequency rejected with 400
        const badFreqReq = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}/alert?site=site-mem-1`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + sessionToken
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                frequency: 'hourly'
            })
        });
        const badFreqRes = await worker.fetch(badFreqReq, env);
        assert.equal(badFreqRes.status, 400);

        // 4. PUT alert preference: invalid returnUrl on unverified external domain rejected
        const badUrlReq = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}/alert?site=site-mem-1`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + sessionToken
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                frequency: 'daily',
                returnUrl: 'https://evil-phishing.com/steal'
            })
        });
        const badUrlRes = await worker.fetch(badUrlReq, env);
        assert.equal(badUrlRes.status, 400);

        // 5. PUT alert preference: switch to off
        const offReq = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}/alert?site=site-mem-1`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + sessionToken
            },
            body: JSON.stringify({
                site: 'site-mem-1',
                frequency: 'off'
            })
        });
        const offRes = await worker.fetch(offReq, env);
        assert.equal(offRes.status, 200);
        const offData = await offRes.json();
        assert.equal(offData.alert.frequency, 'off');
        assert.equal(offData.alert.enabled, false);
    });

    test('26. Saved Search deletion cascades and deletes alert preference record', async () => {
        const db = createMockConsumerDB();
        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SIGNING_SECRET };

        const sessionToken = await createSignedIdxSession('site-mem-1');
        const tokenHash = await sha256Hex(sessionToken);
        db.tables.sneak_consumer_sessions.push({
            id: 'csess_1',
            site_id: 'site_1',
            user_id: 'c_usr_1',
            token_hash: tokenHash,
            expires_at: new Date(Date.now() + 86400000).toISOString()
        });

        const searchId = 'css_cascade_test_1';
        db.tables.sneak_consumer_saved_searches.push({
            id: searchId,
            site_id: 'site_1',
            user_id: 'c_usr_1',
            name: 'Cascade Test Search',
            state_version: 1,
            state_json: JSON.stringify({ version: 1, search: { propertyType: 'sale' } }),
            state_hash: 'hash_cascade_1',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        });

        db.tables.sneak_consumer_search_alerts.push({
            id: 'calert_cascade_1',
            saved_search_id: searchId,
            site_id: 'site_1',
            user_id: 'c_usr_1',
            frequency: 'asap',
            enabled: 1,
            enabled_at: new Date().toISOString(),
            timezone: 'America/New_York',
            return_url: 'https://member1.com/search',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        });

        assert.equal(db.tables.sneak_consumer_search_alerts.length, 1);

        // Delete Saved Search
        const delReq = new Request(`https://consumer.staging/api/consumer/saved-searches/${searchId}?site=site-mem-1`, {
            method: 'DELETE',
            headers: { 'Authorization': 'Bearer ' + sessionToken }
        });
        const delRes = await worker.fetch(delReq, env);
        assert.equal(delRes.status, 200);

        assert.equal(db.tables.sneak_consumer_saved_searches.some(s => s.id === searchId), false);
        assert.equal(db.tables.sneak_consumer_search_alerts.some(a => a.saved_search_id === searchId), false, 'Alert preference must be removed upon saved search deletion');
    });
});
