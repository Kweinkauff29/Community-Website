/**
 * test/sneak-domain-provisioning.test.mjs
 * 
 * SNEAK Domain Provisioning, Cloudflare for SaaS & Host Resolution Test Suite (Phase 6.1):
 * - Domain Normalization & Strict Validation
 * - Duplicate Ownership Prevention
 * - Cloudflare for SaaS Lifecycle & Status Mapping (requested -> active)
 * - Automatic SNEAK Verification Activation
 * - Domain Removal & Deauthorization
 * - Host-Header Exact Tenant Resolution on Website Engine
 * - Unknown / Unverified Host 404 Guard
 * - Cross-Tenant Host Isolation
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import {
    normalizeHostname,
    prepareCustomHostname,
    refreshCustomHostnameStatus,
    removeCustomHostname
} from '../sneak-admin/cloudflare-saas.js';
import { handleRecordLaunchCheck, handleGetReadiness } from '../sneak-admin/api.js';
import worker from '../sneak-sites/worker.js';

export function createMockDomainDB() {
    const tables = {
        sneak_launch_checks: [
            { check_key: 'cloudflare_saas_enabled', status: 'pending', source: 'system' },
            { check_key: 'cloudflare_fallback_active', status: 'pending', source: 'system' },
            { check_key: 'cloudflare_real_custom_hostname', status: 'pending', source: 'system' },
            { check_key: 'cloudflare_real_ssl', status: 'pending', source: 'system' },
            { check_key: 'cloudflare_real_https', status: 'pending', source: 'system' },
            { check_key: 'cloudflare_real_idx', status: 'pending', source: 'system' },
            { check_key: 'cloudflare_real_removal', status: 'pending', source: 'system' },
            { check_key: 'email_provider_configured', status: 'pending', source: 'system' },
            { check_key: 'email_domain_verified', status: 'pending', source: 'system' },
            { check_key: 'email_real_invitation', status: 'pending', source: 'system' },
            { check_key: 'email_real_login', status: 'pending', source: 'system' },
            { check_key: 'email_replay_protection', status: 'pending', source: 'system' }
        ],
        sneak_sync_state: [
            { sync_name: 'listings', status: 'success', last_successful_sync: '2026-08-21 14:00:00', last_cursor: '2026-08-21T14:00:00Z' },
            { sync_name: 'open_houses', status: 'success', last_successful_sync: '2026-08-21 14:00:00', last_cursor: null }
        ],
        sneak_listings: [
            { ListingKey: 'lst_1', StandardStatus: 'Active', ListAgentMlsId: 'B3650316' }
        ],
        sneak_open_houses: [
            { OpenHouseKey: 'oh_1', ListingKey: 'lst_1' }
        ],
        sneak_accounts: [
            { id: 'acc_a', account_name: 'Agent A Realty', status: 'active', plan: 'pro' },
            { id: 'acc_b', account_name: 'Agent B Realty', status: 'active', plan: 'pro' }
        ],
        sneak_sites: [
            { id: 'site_a', account_id: 'acc_a', site_key: 'site-a', site_name: 'Site A', scope_type: 'agent', scope_value: 'B3650316', status: 'active' },
            { id: 'site_b', account_id: 'acc_b', site_key: 'site-b', site_name: 'Site B', scope_type: 'agent', scope_value: 'B9999999', status: 'active' }
        ],
        sneak_branding: [
            { site_id: 'site_a', display_name: 'Sarah Agent A', brokerage: 'Bonita Realty', primary_color: '#1e3a8a' },
            { site_id: 'site_b', display_name: 'David Agent B', brokerage: 'Naples Realty', primary_color: '#064e3b' }
        ],
        sneak_website_configs: [
            { site_id: 'site_a', enabled: 1, template_key: 'essential', site_title: 'Sarah Agent A Official', hero_heading: 'Welcome to Sarah Site A' },
            { site_id: 'site_b', enabled: 1, template_key: 'coastal', site_title: 'David Agent B Official', hero_heading: 'Welcome to David Site B' }
        ],
        sneak_domains: [
            { id: 'dom_a', site_id: 'site_a', domain: 'www.sarahhomesfl.com', verified: 1, status: 'active' }
        ],
        sneak_domain_bindings: [
            {
                id: 'bind_a',
                domain_id: 'dom_a',
                site_id: 'site_a',
                provider: 'cloudflare_saas',
                hostname: 'www.sarahhomesfl.com',
                provider_hostname_id: 'cf_host_123',
                status: 'active',
                ssl_status: 'active',
                cname_target: 'customers.sneakidx.com'
            }
        ],
        sneak_account_entitlements: [
            { account_id: 'acc_a', status: 'active', plan: 'pro', grace_until: null },
            { account_id: 'acc_b', status: 'active', plan: 'pro', grace_until: null }
        ],
        sneak_leads: [],
        sneak_admin_audit: []
    };

    return {
        tables,
        prepare(query) {
            let boundArgs = [];
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
                    if (query.includes('FROM sneak_sites') && query.includes('WHERE id = ?')) {
                        const site = tables.sneak_sites.find(s => s.id === boundArgs[0]);
                        return { results: site ? [site] : [] };
                    }
                    if (query.includes('FROM sneak_domain_bindings b') && query.includes('WHERE b.hostname = ?')) {
                        const b = tables.sneak_domain_bindings.find(x => x.hostname === boundArgs[0] && x.status !== 'removed');
                        if (!b) return { results: [] };
                        const s = tables.sneak_sites.find(x => x.id === b.site_id);
                        const a = tables.sneak_accounts.find(x => x.id === s?.account_id);
                        return { results: [{ ...b, site_name: s?.site_name, account_name: a?.account_name }] };
                    }
                    if (query.includes('FROM sneak_domains') && query.includes('WHERE site_id = ? AND domain = ?')) {
                        const d = tables.sneak_domains.find(x => x.site_id === boundArgs[0] && x.domain === boundArgs[1]);
                        return { results: d ? [d] : [] };
                    }
                    if (query.includes('FROM sneak_domains') && query.includes('WHERE id = ?')) {
                        const d = tables.sneak_domains.find(x => x.id === boundArgs[0]);
                        return { results: d ? [d] : [] };
                    }
                    if (query.includes('FROM sneak_domain_bindings') && query.includes('WHERE id = ?')) {
                        const b = tables.sneak_domain_bindings.find(x => x.id === boundArgs[0]);
                        return { results: b ? [b] : [] };
                    }
                    if (query.includes('FROM sneak_sites s') && query.includes('WHERE d.domain = ?')) {
                        const d = tables.sneak_domains.find(x => x.domain === boundArgs[0] && x.status === 'active' && x.verified === 1);
                        if (!d) return { results: [] };
                        const s = tables.sneak_sites.find(x => x.id === d.site_id);
                        const a = tables.sneak_accounts.find(x => x.id === s?.account_id);
                        const b = tables.sneak_branding.find(x => x.site_id === s?.id);
                        const w = tables.sneak_website_configs.find(x => x.site_id === s?.id);
                        const e = tables.sneak_account_entitlements.find(x => x.account_id === s?.account_id);
                        return {
                            results: [{
                                site_id: s.id,
                                account_id: s.account_id,
                                site_key: s.site_key,
                                site_name: s.site_name,
                                site_status: s.status,
                                account_name: a?.account_name,
                                account_plan: a?.plan,
                                account_status: a?.status,
                                entitlement_status: e?.status || 'active',
                                grace_until: e?.grace_until,
                                entitlement_plan: e?.plan,
                                display_name: b?.display_name,
                                brokerage: b?.brokerage,
                                primary_color: b?.primary_color,
                                website_enabled: w?.enabled,
                                template_key: w?.template_key,
                                site_title: w?.site_title,
                                hero_heading: w?.hero_heading
                            }]
                        };
                    }
                    if (query.includes('FROM sneak_launch_checks') && query.includes('WHERE check_key = ?')) {
                        const c = tables.sneak_launch_checks.find(x => x.check_key === boundArgs[0]);
                        return { results: c ? [c] : [] };
                    }
                    if (query.includes('FROM sneak_launch_checks')) {
                        return { results: tables.sneak_launch_checks };
                    }
                    if (query.includes('FROM sneak_sync_state') && (query.includes("'listings'") || boundArgs[0] === 'listings')) {
                        const s = tables.sneak_sync_state.find(x => x.sync_name === 'listings');
                        return { results: s ? [s] : [] };
                    }
                    if (query.includes('FROM sneak_sync_state') && (query.includes("'open_houses'") || boundArgs[0] === 'open_houses')) {
                        const s = tables.sneak_sync_state.find(x => x.sync_name === 'open_houses');
                        return { results: s ? [s] : [] };
                    }
                    if (query.includes('FROM sneak_sync_state')) {
                        return { results: tables.sneak_sync_state };
                    }
                    if (query.includes('GROUP BY StandardStatus')) {
                        return { results: [{ StandardStatus: 'Active', cnt: tables.sneak_listings.length }] };
                    }
                    if (query.includes('COUNT(*) as cnt FROM sneak_listings')) {
                        return { results: [{ cnt: tables.sneak_listings.length }] };
                    }
                    if (query.includes('COUNT(*) as cnt FROM sneak_open_houses')) {
                        return { results: [{ cnt: tables.sneak_open_houses.length }] };
                    }
                    return { results: [] };
                },
                async run() {
                    if (query.includes('INSERT INTO sneak_launch_checks')) {
                        const [check_key, status, source, checked_at, detail_json] = boundArgs;
                        tables.sneak_launch_checks = tables.sneak_launch_checks.filter(x => x.check_key !== check_key);
                        tables.sneak_launch_checks.push({ check_key, status, source, checked_at, detail_json });
                        return { success: true };
                    }
                    if (query.includes('INSERT INTO sneak_domains')) {
                        const [id, site_id, domain] = boundArgs;
                        tables.sneak_domains.push({ id, site_id, domain, verified: 0, status: 'active', created_at: new Date().toISOString() });
                        return { success: true };
                    }
                    if (query.includes('INSERT INTO sneak_domain_bindings')) {
                        const [id, domain_id, site_id, hostname, provider_hostname_id, status, ssl_status, cname_target, ownership_txt_name, ownership_txt_value, last_checked_at] = boundArgs;
                        tables.sneak_domain_bindings = tables.sneak_domain_bindings.filter(x => x.hostname !== hostname);
                        tables.sneak_domain_bindings.push({
                            id, domain_id, site_id, provider: 'cloudflare_saas', hostname, provider_hostname_id,
                            status, ssl_status, cname_target, ownership_txt_name, ownership_txt_value, last_checked_at,
                            created_at: new Date().toISOString(), updated_at: new Date().toISOString()
                        });
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_domain_bindings') && query.includes('status = ?')) {
                        const [status, ssl_status, last_checked_at, isFullyActive, activated_at, updated_at, bindingId] = boundArgs;
                        const b = tables.sneak_domain_bindings.find(x => x.id === bindingId);
                        if (b) {
                            b.status = status;
                            b.ssl_status = ssl_status;
                            b.last_checked_at = last_checked_at;
                            if (isFullyActive) b.activated_at = activated_at;
                            b.updated_at = updated_at;
                        }
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_domains') && query.includes('verified = 1')) {
                        const domainId = boundArgs[0];
                        const d = tables.sneak_domains.find(x => x.id === domainId);
                        if (d) { d.verified = 1; d.status = 'active'; }
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_domains') && query.includes('verified = 0')) {
                        const domainId = boundArgs[0];
                        const d = tables.sneak_domains.find(x => x.id === domainId);
                        if (d) { d.verified = 0; d.status = 'disabled'; }
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_domain_bindings') && query.includes('status = \'removed\'')) {
                        const removed_at = boundArgs[0];
                        const updated_at = boundArgs[1];
                        const bindingId = boundArgs[2];
                        const b = tables.sneak_domain_bindings.find(x => x.id === bindingId);
                        if (b) { b.status = 'removed'; b.removed_at = removed_at; b.updated_at = updated_at; }
                        return { success: true };
                    }
                    if (query.includes('UPDATE sneak_domain_bindings') && query.includes('status = \'removal_error\'')) {
                        const error_summary = boundArgs[0];
                        const updated_at = boundArgs[1];
                        const bindingId = boundArgs[2];
                        const b = tables.sneak_domain_bindings.find(x => x.id === bindingId);
                        if (b) {
                            b.status = 'removal_error';
                            b.error_code = 'PROVIDER_DELETE_ERROR';
                            b.error_summary = error_summary;
                            b.updated_at = updated_at;
                        }
                        return { success: true };
                    }
                    return { success: true };
                }
            };
        }
    };
}

describe('SNEAK Custom Domain Provisioning & Cloudflare for SaaS Suite (Phase 6.1)', () => {

    test('TEST 1: Domain Normalization & Strict Validation', () => {
        // Valid inputs
        assert.equal(normalizeHostname('https://www.smithrealty.com/').hostname, 'www.smithrealty.com');
        assert.equal(normalizeHostname('HTTP://JOHN.HOMES.FL.US:8080/search?q=1').hostname, 'john.homes.fl.us');
        assert.equal(normalizeHostname('www.naplesluxury.com').hostname, 'www.naplesluxury.com');

        // Invalid / Malformed / Forbidden inputs
        assert.equal(normalizeHostname('').valid, false);
        assert.equal(normalizeHostname('*.smithrealty.com').valid, false, 'Rejects wildcards');
        assert.equal(normalizeHostname('192.168.1.1').valid, false, 'Rejects IPv4');
        assert.equal(normalizeHostname('::1').valid, false, 'Rejects IPv6');
        assert.equal(normalizeHostname('myworker.workers.dev').valid, false, 'Rejects workers.dev');
        assert.equal(normalizeHostname('test.pages.dev').valid, false, 'Rejects pages.dev');
        assert.equal(normalizeHostname('app.sneakidx.com').valid, false, 'Rejects SaaS zone itself');
        assert.equal(normalizeHostname('localhost').valid, false, 'Rejects localhost');
        assert.equal(normalizeHostname('invalid_domain_name').valid, false, 'Rejects invalid FQDN');
    });

    test('TEST 2: Duplicate Domain Ownership Across Tenants is Blocked', async () => {
        const mockDB = createMockDomainDB();
        const env = {};

        // Tenant B attempts to claim Tenant A's active domain
        const res = await prepareCustomHostname(mockDB, 'site_b', 'www.sarahhomesfl.com', env, 'admin');
        assert.equal(res.success, false);
        assert.ok(res.error.includes('already connected to another SNEAK site'));
    });

    test('TEST 3: Custom Hostname Preparation & DNS Instructions', async () => {
        const mockDB = createMockDomainDB();
        const env = { CLOUDFLARE_SAAS_CNAME_TARGET: 'customers.sneak-provider.com' };

        const res = await prepareCustomHostname(mockDB, 'site_b', 'www.davidnaples.com', env, 'admin');
        assert.equal(res.success, true);
        assert.equal(res.hostname, 'www.davidnaples.com');
        assert.equal(res.binding.status, 'pending_dns');
        assert.equal(res.binding.ssl_status, 'pending_validation');
        assert.equal(res.dnsInstructions.type, 'CNAME');
        assert.equal(res.dnsInstructions.name, 'www');
        assert.equal(res.dnsInstructions.target, 'customers.sneak-provider.com');

        // Verify initial verification in sneak_domains is 0 (not auto-verified)
        const dom = mockDB.tables.sneak_domains.find(x => x.domain === 'www.davidnaples.com');
        assert.equal(dom.verified, 0);
    });

    test('TEST 4: Status Refresh & Automatic SNEAK Verification Activation', async () => {
        const mockDB = createMockDomainDB();
        const env = { CLOUDFLARE_SAAS_CNAME_TARGET: 'customers.sneak-provider.com' };

        // 1. Prepare
        const prep = await prepareCustomHostname(mockDB, 'site_b', 'www.davidnaples.com', env, 'admin');
        const bindingId = prep.binding.id;

        // 2. Refresh
        const refresh = await refreshCustomHostnameStatus(mockDB, bindingId, env, 'admin');
        assert.equal(refresh.success, true);
        assert.equal(refresh.isFullyActive, true);
        assert.equal(refresh.binding.status, 'active');
        assert.equal(refresh.binding.ssl_status, 'active');

        // 3. Verify sneak_domains was automatically verified
        const dom = mockDB.tables.sneak_domains.find(x => x.domain === 'www.davidnaples.com');
        assert.equal(dom.verified, 1);
        assert.equal(dom.status, 'active');
    });

    test('TEST 5: Domain Removal & Safe Deauthorization', async () => {
        const mockDB = createMockDomainDB();
        const env = {};

        const res = await removeCustomHostname(mockDB, 'bind_a', env, 'admin');
        assert.equal(res.success, true);

        // Verify domain authorization is revoked immediately
        const dom = mockDB.tables.sneak_domains.find(x => x.id === 'dom_a');
        assert.equal(dom.verified, 0);
        assert.equal(dom.status, 'disabled');

        // Verify binding is marked removed
        const bind = mockDB.tables.sneak_domain_bindings.find(x => x.id === 'bind_a');
        assert.equal(bind.status, 'removed');
    });

    test('TEST 6: Host-Header Tenant Resolution on Website Engine', async () => {
        const mockDB = createMockDomainDB();
        const env = { DB: mockDB, SNEAK_ENV: 'staging' };

        // 1. Request on verified custom domain www.sarahhomesfl.com
        const req = new Request('https://www.sarahhomesfl.com/', {
            headers: { 'Host': 'www.sarahhomesfl.com' }
        });
        const res = await worker.fetch(req, env, {});
        assert.equal(res.status, 200);
        const html = await res.text();

        assert.ok(html.includes('Sarah Agent A Official'));
        assert.ok(html.includes('Welcome to Sarah Site A'));
        assert.ok(html.includes('<link rel="canonical" href="https://www.sarahhomesfl.com" />'));
        assert.ok(html.includes('<meta property="og:url" content="https://www.sarahhomesfl.com" />'));
        assert.ok(html.includes('<meta name="robots" content="index, follow" />'));
    });

    test('TEST 7: Unknown Host & Unverified Host 404 Guard', async () => {
        const mockDB = createMockDomainDB();
        const env = { DB: mockDB, SNEAK_ENV: 'staging' };

        // Unknown Host
        const unkRes = await worker.fetch(new Request('https://www.randomunknownsite.com/'), env, {});
        assert.equal(unkRes.status, 404);
        const unkHtml = await unkRes.text();
        assert.ok(unkHtml.includes('404 — Site Not Found'));
        assert.ok(!unkHtml.includes('Sarah Agent A'));

        // Unverified Host
        mockDB.tables.sneak_domains.push({ id: 'dom_unv', site_id: 'site_b', domain: 'www.unverifiedsite.com', verified: 0, status: 'active' });
        const unvRes = await worker.fetch(new Request('https://www.unverifiedsite.com/'), env, {});
        assert.equal(unvRes.status, 404);
    });

    test('TEST 8: Cross-Tenant Host Isolation', async () => {
        const mockDB = createMockDomainDB();
        // Activate David's domain
        mockDB.tables.sneak_domains.push({ id: 'dom_b', site_id: 'site_b', domain: 'www.davidnaples.com', verified: 1, status: 'active' });
        const env = { DB: mockDB, SNEAK_ENV: 'staging' };

        const resA = await worker.fetch(new Request('https://www.sarahhomesfl.com/'), env, {});
        const htmlA = await resA.text();

        const resB = await worker.fetch(new Request('https://www.davidnaples.com/'), env, {});
        const htmlB = await resB.text();

        assert.ok(htmlA.includes('Sarah Agent A'));
        assert.ok(!htmlA.includes('David Agent B'));

        assert.ok(htmlB.includes('David Agent B'));
        assert.ok(!htmlB.includes('Sarah Agent A'));
    });

    test('TEST 9: Live Mode Fails Closed on Missing Credentials', async () => {
        const mockDB = createMockDomainDB();
        const liveEnvNoToken = { CLOUDFLARE_SAAS_MODE: 'live' };

        const res = await prepareCustomHostname(mockDB, 'site_b', 'www.live-fail-test.com', liveEnvNoToken, 'admin');
        assert.equal(res.success, false);
        assert.ok(res.error.includes('CLOUDFLARE_SAAS_API_TOKEN is required in live mode'));
    });

    test('TEST 10: Explicit Provider Source Identification', async () => {
        const mockDB = createMockDomainDB();
        const simEnv = { CLOUDFLARE_SAAS_MODE: 'simulation' };

        const res = await prepareCustomHostname(mockDB, 'site_b', 'www.sim-label-test.com', simEnv, 'admin');
        assert.equal(res.success, true);
        assert.equal(res.providerSource, 'SIMULATED PROVIDER');
    });

    test('TEST 11: Provider Delete Failure Hardening in Live Mode', async () => {
        const mockDB = createMockDomainDB();
        // Live mode without tokens will fail delete in Cloudflare
        const liveEnv = { CLOUDFLARE_SAAS_MODE: 'live', CLOUDFLARE_SAAS_API_TOKEN: 'invalid_token', CLOUDFLARE_SAAS_ZONE_ID: 'invalid_zone', CLOUDFLARE_SAAS_CNAME_TARGET: 'customers.sneakidx.com' };

        // mock fetch failure for live delete
        const originalFetch = globalThis.fetch;
        globalThis.fetch = async () => ({
            json: async () => ({ success: false, errors: [{ code: 1000, message: 'Invalid API token' }] })
        });

        try {
            const res = await removeCustomHostname(mockDB, 'bind_a', liveEnv, 'admin');
            assert.equal(res.success, false);
            assert.ok(res.error.includes('Cloudflare Custom Hostname deletion failed'));

            // SNEAK authorization was still safely revoked
            const dom = mockDB.tables.sneak_domains.find(x => x.id === 'dom_a');
            assert.equal(dom.verified, 0);
            assert.equal(dom.status, 'disabled');

            // Binding preserved in removal_error state for retry
            const bind = mockDB.tables.sneak_domain_bindings.find(x => x.id === 'bind_a');
            assert.equal(bind.status, 'removal_error');
            assert.equal(bind.error_code, 'PROVIDER_DELETE_ERROR');
        } finally {
            globalThis.fetch = originalFetch;
        }
    });

    test('TEST 12: Launch Checks Model & Simulation Guard', async () => {
        const mockDB = createMockDomainDB();

        // 1. Simulation source cannot mark real cloudflare check as pass
        const blockedSimCF = await handleRecordLaunchCheck(mockDB, {
            check_key: 'cloudflare_real_custom_hostname',
            status: 'pass',
            source: 'simulated_provider'
        }, 'admin');
        assert.equal(blockedSimCF.status, 400);

        // 2. Simulation source cannot mark real email check as pass
        const blockedSimEmail = await handleRecordLaunchCheck(mockDB, {
            check_key: 'email_real_invitation',
            status: 'pass',
            source: 'simulated_email'
        }, 'admin');
        assert.equal(blockedSimEmail.status, 400);

        // 3. Real source can record pass
        const validReal = await handleRecordLaunchCheck(mockDB, {
            check_key: 'cloudflare_real_custom_hostname',
            status: 'pass',
            source: 'real_cloudflare',
            detail: { hostname: 'sneak-test.coconutcoasthomes.com' }
        }, 'admin');
        assert.equal(validReal.status, 200);
        const check = mockDB.tables.sneak_launch_checks.find(x => x.check_key === 'cloudflare_real_custom_hostname');
        assert.equal(check.status, 'pass');
    });

    test('TEST 13: Evidence-Based Readiness Category Calculation', async () => {
        const mockDB = createMockDomainDB();
        const simEnv = { CLOUDFLARE_SAAS_MODE: 'simulation' };

        // 1. Initial State: External Services Pending
        const r1Res = await handleGetReadiness(mockDB, simEnv);
        const r1 = await r1Res.json();
        assert.equal(r1.readinessCategory, 'External Services Pending');
        assert.equal(r1.pilotReady, false);
        assert.equal(r1.cloudflareSaaS.fallbackOrigin, 'Pending');
        assert.equal(r1.email.senderDomain, 'Missing');

        // 2. Mark all 12 checks passed with real sources
        const checkKeys = [
            'cloudflare_saas_enabled', 'cloudflare_fallback_active', 'cloudflare_real_custom_hostname',
            'cloudflare_real_ssl', 'cloudflare_real_https', 'cloudflare_real_idx', 'cloudflare_real_removal',
            'email_provider_configured', 'email_domain_verified', 'email_real_invitation',
            'email_real_login', 'email_replay_protection'
        ];
        for (const k of checkKeys) {
            const c = mockDB.tables.sneak_launch_checks.find(x => x.check_key === k);
            if (c) {
                c.status = 'pass';
                if (k.startsWith('cloudflare_')) c.source = 'real_cloudflare';
                else if (k.startsWith('email_') && k !== 'email_replay_protection') c.source = 'real_mailjet';
                else if (k === 'email_replay_protection') c.source = 'system';
            }
        }

        // 3. Verified State with all evidence passed: Pilot Ready
        const liveEnv = {
            CLOUDFLARE_SAAS_MODE: 'live',
            CLOUDFLARE_SAAS_API_TOKEN: 'valid_token',
            CLOUDFLARE_SAAS_ZONE_ID: 'valid_zone',
            CLOUDFLARE_SAAS_CNAME_TARGET: 'sneak-customers.coconutcoasthomes.com',
            MAILJET_API_KEY: 'mj_valid_key',
            MAILJET_SECRET_KEY: 'mj_valid_secret',
            EMAIL_FROM: 'SNEAK IDX <idx@mail.coconutcoasthomes.com>'
        };

        const r2Res = await handleGetReadiness(mockDB, liveEnv);
        const r2 = await r2Res.json();
        assert.equal(r2.readinessCategory, 'Pilot Ready');
        assert.equal(r2.pilotReady, true);
        assert.equal(r2.cloudflareSaaS.fallbackOrigin, 'Active');
        assert.equal(r2.email.mode, 'Mailjet');
        assert.equal(r2.email.senderDomain, 'Verified');
    });
});


