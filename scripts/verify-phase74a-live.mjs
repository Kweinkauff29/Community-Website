#!/usr/bin/env node
/** Live Phase 7.4A health verification and optional isolated Admin API lifecycle fixture. */

import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const BUILD = '2026.08.31.7.4a';
const ADMIN = process.env.PHASE74A_ADMIN_URL || 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev';
const services = [
    ['Admin', `${ADMIN}/health`, BUILD],
    ['Member', 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/health', BUILD],
    ['Serving', 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev/idx/v1/health', BUILD],
    ['Consumer', 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev/', null],
    ['Sites', 'https://sneak-idx-sites-staging.bonitaspringsrealtors.workers.dev/health', BUILD],
    ['Alerts', 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev/health', null]
];
const runFixture = process.argv.includes('--fixture');
const keepFixture = process.argv.includes('--keep-fixture');
const session = process.env.SNEAK_ADMIN_SESSION || '';
const origin = new URL(ADMIN).origin;
let passed = 0;
let failed = 0;
let fixture = null;

function check(condition, label, detail = '') {
    if (condition) { passed++; console.log(`PASS ${label}${detail ? ` — ${detail}` : ''}`); }
    else { failed++; console.error(`FAIL ${label}${detail ? ` — ${detail}` : ''}`); }
}

async function request(url, options = {}) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    return { response, data };
}

async function admin(path, options = {}) {
    const headers = { Accept: 'application/json', Origin: origin, Cookie: `__Host-sneak_admin_session=${session}`, ...(options.headers || {}) };
    if (options.body && typeof options.body !== 'string') { headers['Content-Type'] = 'application/json'; options.body = JSON.stringify(options.body); }
    const result = await request(`${ADMIN}/api/admin${path}`, { ...options, headers });
    if (!result.response.ok) throw new Error(`${path} returned ${result.response.status}: ${result.data.message || result.data.error || 'unknown error'}`);
    return result.data;
}

function assertNoSecretShape(value, label) {
    const serialized = JSON.stringify(value);
    check(!/(password_hash|token_hash|api[_-]?key|secret[_-]?key|stripe_customer_id)/i.test(serialized), label);
}

async function cleanupFixture(accountId) {
    if (!/^acc_[0-9a-f-]{36}$/i.test(accountId)) throw new Error('Refusing cleanup for an unrecognized fixture ID.');
    const sql = `DELETE FROM sneak_admin_audit WHERE entity_id = '${accountId}' OR entity_id IN (SELECT id FROM sneak_sites WHERE account_id = '${accountId}') OR entity_id IN (SELECT id FROM sneak_member_users WHERE account_id = '${accountId}') OR entity_id IN (SELECT d.id FROM sneak_domains d JOIN sneak_sites s ON s.id = d.site_id WHERE s.account_id = '${accountId}'); DELETE FROM sneak_account_entitlements WHERE account_id = '${accountId}'; DELETE FROM sneak_accounts WHERE id = '${accountId}';`;
    const npx = process.platform === 'win32' ? 'npx.cmd' : 'npx';
    const tempDir = mkdtempSync(join(tmpdir(), 'phase74a-cleanup-'));
    const sqlFile = join(tempDir, 'cleanup.sql');
    writeFileSync(sqlFile, sql, { encoding: 'utf8', mode: 0o600 });
    let result;
    try {
        result = spawnSync(npx, ['--yes', 'wrangler@latest', 'd1', 'execute', 'sneak-idx-staging', '--config', 'wrangler.sneak-admin.toml', '--remote', '--file', sqlFile], {
            cwd: fileURLToPath(new URL('..', import.meta.url)),
            encoding: 'utf8',
            windowsHide: true,
            shell: process.platform === 'win32'
        });
    } finally {
        rmSync(tempDir, { recursive: true, force: true });
    }
    if (result.status !== 0) throw new Error((result.error?.message || result.stderr || result.stdout || 'Fixture cleanup failed').trim());
    check(true, 'isolated QA fixture cleanup');
}

for (const [name, url, expectedBuild] of services) {
    try {
        const { response, data } = await request(url, { headers: { Accept: 'application/json' } });
        check(response.ok, `${name} health`, String(response.status));
        if (expectedBuild) check((data.build || data.version) === expectedBuild, `${name} build`, data.build || data.version || 'missing');
        assertNoSecretShape(data, `${name} health secret boundary`);
    } catch (error) { check(false, `${name} health`, error.message); }
}

try {
    const { response } = await request(`${ADMIN}/api/admin/dashboard`, { headers: { Accept: 'application/json' } });
    check(response.status === 401, 'unauthenticated Admin API protection', String(response.status));
} catch (error) { check(false, 'unauthenticated Admin API protection', error.message); }

if (runFixture) {
    if (!session) {
        check(false, 'isolated Admin fixture', 'SNEAK_ADMIN_SESSION is required');
    } else {
        const suffix = `${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`;
        const mlsId = process.env.PHASE74A_MLS_ID || 'B3650316';
        try {
            const identity = await admin(`/validate-mls?type=agent&mlsId=${encodeURIComponent(mlsId)}`);
            check(typeof identity.valid === 'boolean', 'MLS identity validation API');

            const created = await admin('/accounts', { method: 'POST', body: {
                account_name: `Phase 74A QA ${suffix}`, member_id: `QA-${suffix}`, plan: 'pro',
                scope_type: 'agent', scope_value: mlsId, site_name: `Phase 74A QA ${suffix}`,
                site_key: `phase-74a-${suffix}`, override_mls_warning: true,
                branding: { display_name: `Phase 74A QA ${suffix}`, brokerage: 'CCOR QA Realty', email: `qa-${suffix}@example.com` }
            } });
            fixture = { accountId: created.account.id, siteId: created.site.id, siteKey: created.site.site_key };
            check(Boolean(fixture.accountId && fixture.siteId), 'account/site provisioning APIs');

            const initial = await admin(`/accounts/${fixture.accountId}/readiness`);
            check(initial.launchStatus === 'NOT_READY' && !initial.canServe, 'initial provisioning is fail-closed');

            await admin(`/accounts/${fixture.accountId}/entitlement`, { method: 'PUT', body: { source: 'growthzone', status: 'active', plan: 'pro', external_reference: `GZ-${suffix}`, notes: 'Phase 7.4A isolated QA fixture' } });
            check(true, 'generic entitlement API');

            const invitation = await admin(`/accounts/${fixture.accountId}/members`, { method: 'POST', body: { email: `qa-${suffix}@example.com`, role: 'owner' } });
            check(invitation.user?.account_id === fixture.accountId, 'member invite association API');

            const domain = await admin(`/sites/${fixture.siteId}/domains`, { method: 'POST', body: { domain: `qa-${suffix}.example.com`, verified: false, status: 'disabled' } });
            fixture.domainId = domain.domain.id;
            await admin(`/domains/${fixture.domainId}`, { method: 'PATCH', body: { verified: true, status: 'active' } });
            check(true, 'domain association/authorization APIs');

            await admin(`/sites/${fixture.siteId}/branding`, { method: 'PUT', body: { display_name: `Phase 74A QA ${suffix}`, brokerage: 'CCOR QA Realty', email: `qa-${suffix}@example.com`, phone: '239-555-0100', primary_color: '#1a365d' } });
            check(true, 'branding API');
            await admin(`/sites/${fixture.siteId}/widgets/search`, { method: 'PUT', body: { enabled: true, config_json: { responsive: true } } });
            check(true, 'widget API');

            const embed = await admin(`/sites/${fixture.siteId}/embed`);
            check(embed.snippets?.search?.htmlSnippet?.includes(fixture.siteKey), 'embed generation API');
            assertNoSecretShape(embed, 'embed secret boundary');

            const ready = await admin(`/accounts/${fixture.accountId}/readiness`);
            check(ready.launchStatus === 'READY_TO_LAUNCH' && ready.canServe, 'READY TO LAUNCH lifecycle state', (ready.launchBlockers || []).map(item => item.code).join(','));
            check(ready.capabilities?.savedSearchEmailAlerts?.status === 'NOT_READY' && ready.capabilities.savedSearchEmailAlerts.core === false, 'optional alert readiness separation');

            await admin(`/accounts/${fixture.accountId}/lifecycle`, { method: 'POST', body: { action: 'suspend' } });
            const suspended = await admin(`/accounts/${fixture.accountId}/readiness`);
            check(!suspended.canServe && suspended.blockers.some(item => item.code === 'ACCOUNT_INACTIVE'), 'suspend denies serving');

            await admin(`/accounts/${fixture.accountId}/lifecycle`, { method: 'POST', body: { action: 'reactivate' } });
            const reactivated = await admin(`/accounts/${fixture.accountId}/readiness`);
            check(reactivated.canServe && reactivated.siteId === fixture.siteId, 'reactivate restores same site');

            const detail = await admin(`/accounts/${fixture.accountId}`);
            check(detail.audit?.some(item => item.action === 'SUSPEND_ACCOUNT') && detail.audit?.some(item => item.action === 'REACTIVATE_ACCOUNT'), 'bounded account audit history');
            assertNoSecretShape(detail, 'account detail secret boundary');

            if (!keepFixture) await cleanupFixture(fixture.accountId);
            else console.log(`FIXTURE ${JSON.stringify(fixture)}`);
        } catch (error) {
            check(false, 'isolated Admin fixture lifecycle', error.message);
            if (fixture?.accountId && !keepFixture) {
                try { await cleanupFixture(fixture.accountId); } catch (cleanupError) { check(false, 'isolated QA fixture cleanup', cleanupError.message); }
            }
        }
    }
}

console.log(`RESULT ${passed}/${passed + failed} PASS`);
process.exitCode = failed ? 1 : 0;
