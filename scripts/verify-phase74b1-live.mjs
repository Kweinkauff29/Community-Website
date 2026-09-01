#!/usr/bin/env node
/** Phase 7.4B1 staging health, capability, and auth-boundary verification. */

const BUILD = '2026.09.01.7.4b1';
const ADMIN = 'https://sneak-idx-admin-staging.bonitaspringsrealtors.workers.dev';
const MEMBER = 'https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev';
const CONSUMER = 'https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev';
const ALERTS = 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev';
const SERVING = 'https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev';

let passed = 0;
let failed = 0;

function check(condition, label, detail = '') {
    if (condition) {
        passed += 1;
        console.log(`PASS ${label}${detail ? ` — ${detail}` : ''}`);
    } else {
        failed += 1;
        console.error(`FAIL ${label}${detail ? ` — ${detail}` : ''}`);
    }
}

async function request(url, options = {}) {
    const response = await fetch(url, options);
    const text = await response.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch {}
    return { response, data, text };
}

function noSecretMaterial(data) {
    const serialized = JSON.stringify(data);
    return !/(api[_-]?key|secret[_-]?key|token_hash|password_hash|authorization)/i.test(serialized);
}

const member = await request(`${MEMBER}/health`, { headers: { Accept: 'application/json' } });
check(member.response.status === 200, 'Member health', String(member.response.status));
check(member.data.build === BUILD, 'Member build', member.data.build);
check(member.data.emailProviderConfigured === true, 'Member provider configuration is observable without secret values');
check(noSecretMaterial(member.data), 'Member health secret boundary');

const consumer = await request(`${CONSUMER}/api/consumer/version`, { headers: { Accept: 'application/json' } });
check(consumer.response.status === 200, 'Consumer health', String(consumer.response.status));
check(consumer.data.build === BUILD, 'Consumer build', consumer.data.build);
check(consumer.data.emailProviderConfigured === false, 'Consumer provider remains fail-closed while secrets are absent');
check(noSecretMaterial(consumer.data), 'Consumer health secret boundary');

const alerts = await request(`${ALERTS}/health`, { headers: { Accept: 'application/json' } });
check(alerts.response.status === 200, 'Alerts health', String(alerts.response.status));
check(alerts.data.build === BUILD, 'Alerts build', alerts.data.build);
check(alerts.data.emailProviderConfigured === false && alerts.data.signingSecretConfigured === false && alerts.data.deliveryReady === false,
    'Alerts provider/signing/delivery readiness fails closed');
check(noSecretMaterial(alerts.data), 'Alerts health secret boundary');

const admin = await request(`${ADMIN}/health`, { headers: { Accept: 'application/json' } });
check(admin.response.status === 200, 'Admin health', String(admin.response.status));
check(admin.data.build === BUILD, 'Admin build', admin.data.build);
check(admin.data.growthZoneConfigured === false, 'GrowthZone readiness fails closed while API key is absent');
check(noSecretMaterial(admin.data), 'Admin health secret boundary');

const dashboard = await request(`${ADMIN}/api/admin/dashboard`, { headers: { Accept: 'application/json' } });
check(dashboard.response.status === 401, 'Unauthenticated Admin API protection', String(dashboard.response.status));

const reconciliation = await request(`${ADMIN}/api/admin/accounts/not-a-real-account/reconciliation`, { headers: { Accept: 'application/json' } });
check(reconciliation.response.status === 401, 'Unauthenticated reconciliation API protection', String(reconciliation.response.status));

const memberToken = await request(`${MEMBER}/api/member/auth/verify?token=${'x'.repeat(40)}`, { headers: { Accept: 'application/json' } });
check(memberToken.response.status === 401, 'Member invalid/replayed/expired-token boundary', String(memberToken.response.status));

const consumerToken = await request(`${CONSUMER}/api/consumer/auth/verify?token=${'x'.repeat(40)}`);
check(consumerToken.response.status === 401, 'Consumer invalid/replayed/expired-token boundary', String(consumerToken.response.status));

const serving = await request(`${SERVING}/idx/v1/health`, { headers: { Accept: 'application/json' } });
check(serving.response.status === 200, 'Unmodified Serving Worker health', String(serving.response.status));

const pilot = await request('https://coconutcoastrealtors.org/idx-test/');
check(pilot.response.status === 200, 'Live staging pilot page', String(pilot.response.status));

console.log(`RESULT ${passed}/${passed + failed} PASS`);
process.exitCode = failed ? 1 : 0;
