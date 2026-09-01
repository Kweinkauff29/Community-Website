import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

import adminWorker from '../sneak-admin/worker.js';
import {
    evaluateEntitlement,
    evaluateServingDecision,
    isAccountEntitled,
    isTenantScopeValid
} from '../sneak-shared/entitlement.js';
import {
    handleCreateAccount,
    handleUpdateAccountEntitlement,
    handleCreateAccountMemberInvite,
    handleAddDomain,
    handleUpdateDomain,
    handleUpdateBranding,
    handleAccountLifecycle
} from '../sneak-admin/api.js';
import { calculateAccountReadiness } from '../sneak-admin/readiness.js';
import { generateEmbedSnippets } from '../sneak-admin/embed-generator.js';
import { renderAdminHtml } from '../sneak-admin/ui.js';

function memoryDb() {
    const tables = {
        accounts: [], sites: [], domains: [], branding: [], widgets: [], members: [],
        entitlements: [], audit: [], consumers: [{ id: 'preserved-consumer' }], leads: [{ id: 'preserved-lead' }],
        sync: { sync_name: 'listings', status: 'success', last_successful_sync: new Date().toISOString() },
        listings: [{ ListingKey: 'L-READY' }]
    };

    function statement(sql, args = []) {
        const q = sql.replace(/\s+/g, ' ').trim();
        return {
            bind(...next) { return statement(sql, next); },
            async first() {
                if (q.includes('FROM sneak_accounts WHERE id = ?')) return tables.accounts.find(row => row.id === args[0]) || null;
                if (q.includes('SELECT id, account_name FROM sneak_accounts')) {
                    const row = tables.accounts.find(item => item.id === args[0]);
                    return row ? { id: row.id, account_name: row.account_name } : null;
                }
                if (q.includes('FROM sneak_sites WHERE site_key = ?')) return tables.sites.find(row => row.site_key === args[0]) || null;
                if (q.includes('FROM sneak_sites WHERE id = ?')) return tables.sites.find(row => row.id === args[0]) || null;
                if (q.includes('SELECT id FROM sneak_domains WHERE site_id = ? AND domain = ?')) return tables.domains.find(row => row.site_id === args[0] && row.domain === args[1]) || null;
                if (q.includes('FROM sneak_domains WHERE id = ?')) return tables.domains.find(row => row.id === args[0]) || null;
                if (q.includes('FROM sneak_branding WHERE site_id = ?')) return tables.branding.find(row => row.site_id === args[0]) || null;
                if (q.includes('FROM sneak_member_users WHERE email = ?')) return tables.members.find(row => row.email === args[0]) || null;
                if (q.includes('FROM sneak_account_entitlements WHERE account_id = ?')) return tables.entitlements.find(row => row.account_id === args[0]) || null;
                if (q.includes("FROM sneak_sync_state WHERE sync_name = 'listings'")) return tables.sync;
                if (q.includes('COUNT(*) AS count FROM sneak_listings')) return { count: tables.listings.length };
                if (q.includes('SELECT count(*) as count FROM sneak_listings')) return { count: tables.listings.length };
                if (q.includes('SELECT display_name')) return tables.branding.find(row => row.site_id === args[0]) || null;
                return null;
            },
            async all() {
                if (q.includes('FROM sneak_sites WHERE account_id = ?')) return { results: tables.sites.filter(row => row.account_id === args[0]) };
                if (q.includes('FROM sneak_member_users WHERE account_id = ?')) return { results: tables.members.filter(row => row.account_id === args[0]) };
                if (q.includes('FROM sneak_domains WHERE site_id = ?')) return { results: tables.domains.filter(row => row.site_id === args[0]) };
                if (q.includes('FROM sneak_widget_configs WHERE site_id = ?')) return { results: tables.widgets.filter(row => row.site_id === args[0]) };
                return { results: [] };
            },
            async run() {
                if (q.startsWith('INSERT INTO sneak_accounts')) {
                    tables.accounts.push({ id: args[0], member_id: args[1], account_name: args[2], agent_mls_id: args[3], office_mls_id: args[4], status: args[5], plan: args[6], created_at: new Date().toISOString(), updated_at: new Date().toISOString() });
                } else if (q.startsWith('INSERT INTO sneak_sites')) {
                    tables.sites.push({ id: args[0], account_id: args[1], site_key: args[2], site_name: args[3], status: args[4], scope_type: args[5], scope_value: args[6], created_at: new Date().toISOString() });
                } else if (q.includes('sneak_branding (')) {
                    tables.branding = tables.branding.filter(row => row.site_id !== args[0]);
                    tables.branding.push({ site_id: args[0], display_name: args[1], brokerage: args[2], logo_url: args[3], agent_photo_url: args[4], primary_color: args[5], secondary_color: args[6], phone: args[7], email: args[8], website_url: args[9] });
                } else if (q.includes('INSERT INTO sneak_widget_configs')) {
                    tables.widgets.push({ id: args[0], site_id: args[1], widget_type: q.match(/'(search|search_bar|listing_grid|open_houses)'/)?.[1] || args[2], enabled: 1, config_json: '{}' });
                } else if (q.startsWith('INSERT INTO sneak_domains')) {
                    tables.domains.push({ id: args[0], site_id: args[1], domain: args[2], verified: args[3] ?? 0, status: args[4] || 'disabled', created_at: new Date().toISOString() });
                } else if (q.startsWith('INSERT INTO sneak_member_users')) {
                    tables.members.push({ id: args[0], account_id: args[1], email: args[2], role: args[3], status: 'invited', invited_at: args[4] });
                } else if (q.startsWith('UPDATE sneak_member_users')) {
                    const row = tables.members.find(item => item.id === args[4]);
                    if (row) Object.assign(row, { account_id: args[0], role: args[1], status: 'invited' });
                } else if (q.startsWith('INSERT INTO sneak_account_entitlements')) {
                    let row = tables.entitlements.find(item => item.account_id === args[0]);
                    const values = { account_id: args[0], source: args[1], status: args[2], plan: args[3], effective_at: args[4], expires_at: args[5], grace_until: args[6], external_reference: args[7], notes: args[8], last_verified_at: args[9], updated_at: args[11] };
                    if (row) Object.assign(row, values); else tables.entitlements.push(values);
                } else if (q.startsWith('UPDATE sneak_accounts SET status')) {
                    const row = tables.accounts.find(item => item.id === args.at(-1));
                    if (row) { row.status = q.includes("'suspended'") ? 'suspended' : q.includes("'inactive'") ? 'inactive' : 'active'; row.updated_at = args[0]; }
                } else if (q.startsWith('UPDATE sneak_account_entitlements SET status = ?')) {
                    const row = tables.entitlements.find(item => item.account_id === args.at(-1)); if (row) row.status = args[0];
                } else if (q.includes("SET status = 'canceled'")) {
                    const row = tables.entitlements.find(item => item.account_id === args.at(-1)); if (row) row.status = 'canceled';
                } else if (q.startsWith('UPDATE sneak_domains SET')) {
                    const row = tables.domains.find(item => item.id === args.at(-1));
                    if (row) { row.verified = args[0]; row.status = args[1]; }
                } else if (q.startsWith('INSERT INTO sneak_admin_audit')) {
                    tables.audit.push({ id: args[0], actor: args[1], action: args[2], entity_type: args[3], entity_id: args[4], summary: args[5] });
                }
                return { success: true };
            }
        };
    }
    return { tables, prepare: sql => statement(sql), async batch(items) { for (const item of items) await item.run(); return items.map(() => ({ success: true })); } };
}

function healthyBindings() {
    const health = data => ({ fetch: async request => new Response(JSON.stringify(new URL(request.url).pathname.includes('bootstrap') ? { success: true, session: 'bounded-qa-session' } : data), { status: 200, headers: { 'Content-Type': 'application/json' } }) });
    return {
        SERVING_WORKER: health({ status: 'ok', build: '2026.08.31.7.4a' }),
        MEMBER_WORKER: health({ status: 'healthy', build: '2026.08.31.7.4a' }),
        ALERT_WORKER: health({ status: 'healthy', deliveryReady: false })
    };
}

describe('Phase 7.4A — Launch Readiness and Staff Provisioning', () => {
    const now = new Date('2026-08-31T12:00:00Z');

    test('1. generic entitlement is authoritative and a Stripe row alone cannot grant service', () => {
        assert.equal(isAccountEntitled('active', null, null, now), false);
        assert.equal(evaluateServingDecision({ accountStatus:'active', siteStatus:'active', entitlementStatus:null, domainAuthorized:true, scopeType:'market', legacyStripeBilling:{ entitlement_status:'active' }, now }).canServe, false);
    });
    test('2. active entitlement permits serving', () => assert.equal(evaluateEntitlement({ accountStatus:'active', entitlementStatus:'active', now }).allowed, true));
    test('3. unexpired grace permits serving', () => assert.equal(evaluateEntitlement({ accountStatus:'active', entitlementStatus:'grace', graceUntil:'2026-09-02T00:00:00Z', now }).allowed, true));
    test('4. expired grace is blocked', () => assert.equal(evaluateEntitlement({ accountStatus:'active', entitlementStatus:'grace', graceUntil:'2026-08-01T00:00:00Z', now }).blocker.code, 'GRACE_EXPIRED'));
    test('5. delinquent without grace is blocked', () => assert.equal(evaluateEntitlement({ accountStatus:'active', entitlementStatus:'delinquent', now }).allowed, false));
    test('6. suspended entitlement is blocked', () => assert.equal(isAccountEntitled('active','suspended',null,now), false));
    test('7. canceled entitlement is blocked', () => assert.equal(isAccountEntitled('active','canceled',null,now), false));
    test('8. inactive account blocks active entitlement', () => assert.equal(evaluateEntitlement({ accountStatus:'inactive', entitlementStatus:'active', now }).blocker.code, 'ACCOUNT_INACTIVE'));
    test('9. inactive site returns stable blocker', () => assert.ok(evaluateServingDecision({accountStatus:'active',siteStatus:'inactive',entitlementStatus:'active',domainAuthorized:true,scopeType:'market',now}).blockers.some(item=>item.code==='SITE_INACTIVE')));
    test('10. unauthorized domain returns stable blocker', () => assert.ok(evaluateServingDecision({accountStatus:'active',siteStatus:'active',entitlementStatus:'active',domainAuthorized:false,scopeType:'market',now}).blockers.some(item=>item.code==='DOMAIN_UNAUTHORIZED')));
    test('11. agent and office scopes require a value', () => { assert.equal(isTenantScopeValid('agent',''),false); assert.equal(isTenantScopeValid('office','OFF1'),true); });
    test('12. blocker object contract is normalized', () => assert.deepEqual(Object.keys(evaluateServingDecision({}).blockers[0]).sort(), ['code','message']));

    const db = memoryDb();
    let accountId;
    let siteId;

    test('13. account and site provisioning persist canonical records', async () => {
        const response = await handleCreateAccount(db, { account_name:'Phase 74A REALTOR', member_id:'M74A', plan:'pro', scope_type:'market', site_name:'Phase 74A Site', branding:{ email:'qa@example.com', brokerage:'QA Realty' } }, 'qa-admin');
        assert.equal(response.status, 201); const body = await response.json(); accountId=body.account.id; siteId=body.site.id;
        assert.equal(db.tables.accounts.length,1); assert.equal(db.tables.sites[0].account_id,accountId);
    });
    test('14. provisioning is initially not ready without entitlement/domain/member', async () => {
        const result=await calculateAccountReadiness(db,accountId,healthyBindings());
        assert.equal(result.launchReady,false); assert.ok(result.launchBlockers.some(item=>item.code==='ENTITLEMENT_MISSING'));
    });
    test('15. generic entitlement provisioning records manual/GrowthZone reference fields', async () => {
        const response=await handleUpdateAccountEntitlement(db,accountId,{source:'growthzone',status:'active',plan:'pro',external_reference:'GZ-QA-74A'},'qa-admin');
        assert.equal(response.status,200); assert.equal(db.tables.entitlements[0].external_reference,'GZ-QA-74A');
    });
    test('16. member invite associates only to the requested account', async () => {
        const response=await handleCreateAccountMemberInvite(db,accountId,{email:'member74a@example.com',role:'owner'},'qa-admin',{MEMBER_WORKER:{fetch:async()=>new Response('{}',{status:200})}});
        assert.equal(response.status,201); assert.equal(db.tables.members[0].account_id,accountId); db.tables.members[0].status='active';
    });
    test('17. cross-account member mutation target is rejected', async () => {
        db.tables.accounts.push({id:'acc-other',account_name:'Other',status:'active',plan:'pro'});
        const response=await handleCreateAccountMemberInvite(db,'acc-other',{email:'member74a@example.com'},'qa-admin',{});
        assert.equal(response.status,409); assert.equal(db.tables.members[0].account_id,accountId);
    });
    test('18. domain association starts pending and can be deliberately authorized', async () => {
        const add=await handleAddDomain(db,siteId,{domain:'qa74a.example.com',verified:0,status:'disabled'},'qa-admin'); assert.equal(add.status,201);
        const domain=(await add.json()).domain; assert.equal(domain.verified,0);
        const update=await handleUpdateDomain(db,domain.id,{verified:true,status:'active'},'qa-admin'); assert.equal(update.status,200);
    });
    test('19. branding configuration persists required contact identity', async () => {
        const response=await handleUpdateBranding(db,siteId,{display_name:'Phase 74A REALTOR',brokerage:'QA Realty',email:'member74a@example.com',primary_color:'#1a365d'},'qa-admin');
        assert.equal(response.status,200); assert.equal(db.tables.branding[0].email,'member74a@example.com');
    });
    test('20. embed generator is current, responsive, site-scoped, and secret-free', () => {
        const embed=generateEmbedSnippets('phase-74a',['qa74a.example.com'],{}); const html=embed.snippets.search.htmlSnippet;
        assert.match(html,/2026\.09\.01\.7\.4b2/); assert.match(html,/data-site="phase-74a"/); assert.doesNotMatch(html,/session=|token=|min-height/i);
    });
    test('21. completed fixture distinguishes ready core from optional alerts', async () => {
        const result=await calculateAccountReadiness(db,accountId,healthyBindings());
        assert.equal(result.launchReady,true); assert.equal(result.canServe,true); assert.equal(result.capabilities.coreSearch.status,'READY'); assert.equal(result.capabilities.savedSearchEmailAlerts.status,'NOT_READY'); assert.equal(result.capabilities.savedSearchEmailAlerts.core,false);
    });
    test('22. launch checklist contains automated operational checks', async () => {
        const result=await calculateAccountReadiness(db,accountId,healthyBindings()); assert.ok(result.checklist.length>=12); assert.ok(result.checklist.every(item=>item.type==='automated'));
    });
    test('23. suspend denies service without deleting account data', async () => {
        const before={site:db.tables.sites[0].site_key,domain:db.tables.domains[0].domain,consumers:db.tables.consumers.length,leads:db.tables.leads.length};
        const response=await handleAccountLifecycle(db,accountId,{action:'suspend'},'qa-admin'); assert.equal(response.status,200);
        const result=await calculateAccountReadiness(db,accountId,healthyBindings()); assert.equal(result.canServe,false); assert.deepEqual(before,{site:db.tables.sites[0].site_key,domain:db.tables.domains[0].domain,consumers:db.tables.consumers.length,leads:db.tables.leads.length});
    });
    test('24. reactivate restores serving with the same site and domain', async () => {
        const response=await handleAccountLifecycle(db,accountId,{action:'reactivate'},'qa-admin'); assert.equal(response.status,200);
        const result=await calculateAccountReadiness(db,accountId,healthyBindings()); assert.equal(result.canServe,true); assert.equal(db.tables.sites[0].site_key,'phase-74a-site'); assert.equal(db.tables.domains[0].domain,'qa74a.example.com');
    });
    test('25. important lifecycle and provisioning mutations create audit records', () => {
        const actions=db.tables.audit.map(row=>row.action); assert.ok(actions.includes('CREATE_ACCOUNT')); assert.ok(actions.includes('UPDATE_ENTITLEMENT')); assert.ok(actions.includes('SUSPEND_ACCOUNT')); assert.ok(actions.includes('REACTIVATE_ACCOUNT'));
    });
    test('26. Admin UI uses deliberate impact modal and contains all operational sections', () => {
        const html=renderAdminHtml(); for(const label of ['Account','Entitlement','Member Users','IDX Site','Domains','Branding','Responsive Embed','Readiness Checklist','Audit History']) assert.ok(html.includes(label));
        assert.ok(html.includes('openImpactModal')); assert.ok(!html.includes('confirm('));
    });
    test('27. rendered Admin application script compiles in a browser runtime', () => {
        const html=renderAdminHtml(); const start=html.indexOf('<script>')+8; const end=html.lastIndexOf('</script>');
        assert.ok(start>7 && end>start); assert.doesNotThrow(()=>new Function(html.slice(start,end)));
    });
    test('28. Admin mutation handlers await account refreshes to prevent stale navigation races', () => {
        const html=renderAdminHtml();
        for(const refresh of ['await viewAccount(id,true)','await viewAccount(accountId,true)']) assert.ok(html.includes(refresh));
        for(const guard of ['enterView(\'readiness\')','canRender(revision, \'readiness\')','refreshOnly && (currentView !== \'account\'']) assert.ok(html.includes(guard));
        assert.doesNotMatch(html,/notify\('Lifecycle updated\.'\);viewAccount\(id\)/);
    });
    test('29. Admin health is deterministic and secret-free', async () => {
        const response=await adminWorker.fetch(new Request('https://admin.example/health'),{SNEAK_ENV:'staging'},{}); const body=await response.json();
        assert.deepEqual(body,{service:'sneak-idx-admin-staging',status:'ok',build:'2026.09.01.7.4b2',environment:'staging',growthZoneEnabled:true,growthZoneConfigured:false}); assert.doesNotMatch(JSON.stringify(body),/secret|password|token_hash/i);
    });
    test('30. runtime source has no preview fallback secret or Stripe authority query', () => {
        const sources=['sneak-admin/api.js','sneak-admin/readiness.js','sneak-sites/worker.js','SneakIDXWorker.js'].map(file=>fs.readFileSync(new URL('../'+file,import.meta.url),'utf8')).join('\n');
        assert.doesNotMatch(sources,/dev_preview_secret_ccor_2026/); assert.doesNotMatch(fs.readFileSync(new URL('../sneak-admin/readiness.js',import.meta.url),'utf8'),/sneak_account_billing|stripe_customer/i);
    });
});
