# CCOR IDX production cutover runbook

Build: `2026.09.01.7.4b2`

Decision recorded 2026-09-01: **PHASE 7.4B2A — PRODUCTION FOUNDATION: COMPLETE | PHASE 7.4B2B — FIRST MEMBER ACTIVATION: BLOCKED / NOT STARTED.** Do not deploy production Workers, activate routes or schedules, provision the member, or replace the current staging embed until every required core gate below has current evidence.

## Isolated production resources

| Resource | Production name | Current state |
| --- | --- | --- |
| D1 | `sneak-idx-production` (`9d3c529d-a769-4ab3-9385-d9c8bc5e937d`) | Created in ENAM; migrations 0001–0035 current; clean |
| Serving | `sneak-idx-worker` | Config prepared; not deployed |
| Sync | `sneak-idx-sync` | Config prepared; not deployed; no cron |
| Consumer | `sneak-idx-consumer` | Config prepared; not deployed; account features disabled |
| Member | `sneak-idx-member` | Config prepared; not deployed |
| Admin | `sneak-idx-admin` | Config prepared; not deployed |
| Sites | `sneak-idx-sites` | Config prepared; not deployed; no custom-host route |
| Alerts | `sneak-idx-alerts` | Config prepared; not deployed; delivery disabled; no cron |

All production configs bind only `sneak-idx-production`. Staging D1, test consumers, sessions, saved state, QA members, alert history, and reconciliation fixtures were not copied. The production counts at the foundation gate were 0 accounts, 0 consumers, 0 listings, and 0 reconciliation rows.

## First-member launch profile

Approved participant: PILOT-01, MLS agent identity `633942`. Controlled operator and mailbox details remain in `docs/sneak-pilot-tracker.md` and must not be copied into logs or public reports.

Member-facing site: `https://coconutcoastrealtors.org`, target page `/idx-test/`. The existing website embed is the pilot delivery surface; a SNEAK-hosted custom website/hostname is not used.

No production account, site, site key, member user, entitlement, or domain row has been created. Provision those only after the required gates pass.

### Authoritative pilot search & scope model
- **Site Search Scope:** `market` (full authorized MLS market inventory; `scope_value` is null/empty)
- **Participant Agent Identity:** `account.agent_mls_id = '633942'` (used for participant-scoped "My Listings" and lead routing)
- **Participant Brokerage / Office:** `Local Real Estate LLC`
- **Featured / Participant Listings:** Agent MLS ID `633942` via `/idx/v1/agent/633942/listings`
- **Lead Ownership:** The pilot member account/site (`site_id = site.site_id`)
- **Main Consumer Search:** Full eligible MLS market inventory (`scope_type = 'market'`, SQL clause `1=1`)
- **Display Compliance:** Strictly enforced (`InternetEntireListingDisplayYN = 1`, address suppression if `InternetAddressDisplayYN = 0`, eligible status rules)
- **Critical Architecture Rule:** Do NOT configure site scope as `agent` merely because the participant is an individual agent. Participant identity and IDX inventory/search scope are distinct concepts. An `agent` site scope would restrict the consumer's entire MLS search to only that agent's own listings.

| Capability | Classification | Current status |
| --- | --- | --- |
| Core IDX search/embed | Required | BLOCKED — no production sync, inventory, tenant, signing, or browser proof |
| Admin Portal | Required | BLOCKED — production auth/session secrets unavailable; Worker not deployed |
| Member Portal | Required | BLOCKED — production member and email proof absent; Worker not deployed |
| Member magic-link email | Required | BLOCKED — production Mailjet/sender/inbox evidence absent |
| Consumer accounts, server favorites, saved searches, shared lists | Disabled optional | DISABLED FOR PILOT via `CONSUMER_AUTH_ENABLED=false` |
| Saved-search email alerts | Disabled optional | DISABLED FOR PILOT via `EMAIL_ALERTS_ENABLED=false`; no schedule |
| GrowthZone automation | Disabled optional | MANUAL ENTITLEMENT MODE via `GROWTHZONE_RECONCILIATION_ENABLED=false`; no schedule |
| Custom hosted website/hostname | Disabled optional | NOT USED via `CUSTOM_HOST_ENABLED=false` |

Anonymous search, map, detail, Compare, Recently Viewed, and direct property share remain part of the intended core experience. Disabled account/email controls must not appear or accept direct API requests.

### Admin Portal browser network model & CSP
- **Admin Browser API Calls:** Strict same-origin `/api/admin/*` (`const API_BASE = '/api/admin'`).
- **Inter-Worker Service Integration:** Server-side Cloudflare Worker service bindings (`env.SERVING_WORKER`, `env.MEMBER_WORKER`, `env.CONSUMER_WORKER`, `env.SITES_WORKER`, `env.ALERT_WORKER`). Zero browser-to-worker cross-origin requests.
- **Content Security Policy (CSP):** `connect-src 'self'`. Guarantees zero external/staging Worker origin leakage in production or staging response headers. Missing environment information strictly defaults to `connect-src 'self'`.

## Production secret-name inventory

Store values only as Cloudflare Worker secrets. Never put values in Wrangler config, D1, source, screenshots, smoke output, audit text, or this document.

- Serving: `SNEAK_SIGNING_SECRET`
- Sync: `BRIDGE_TOKEN`
- Member: `MAILJET_API_KEY`, `MAILJET_SECRET_KEY`, `SNEAK_WEBSITE_PREVIEW_SECRET`
- Admin: `SNEAK_ADMIN_PASSWORD_HASH`, `SNEAK_WEBSITE_PREVIEW_SECRET` *(Note: `SNEAK_ADMIN_SESSION_SECRET` is NOT consumed by runtime; session tokens are generated via WebCrypto, hashed with SHA-256, and verified directly against D1)*
- Sites: `SNEAK_WEBSITE_PREVIEW_SECRET`
- Consumer, only when enabled later: `MAILJET_API_KEY`, `MAILJET_SECRET_KEY`
- Alerts, only when enabled later: `MAILJET_API_KEY`, `MAILJET_SECRET_KEY`, `SNEAK_ALERT_UNSUBSCRIBE_SECRET`
- Custom hosting, only when enabled later: `CLOUDFLARE_SAAS_API_TOKEN`
- GrowthZone automation, only when enabled later: `GROWTHZONE_API_KEY`

`EMAIL_FROM` and service URLs are non-secret configuration. The proposed sender name is `no-reply@ccorealtors.org`, but it is not approved for production until Mailjet sender/domain authorization, CCOR ownership, SPF/DKIM as applicable, provider acceptance, and controlled-inbox delivery all pass.

## Required cutover sequence

1. Restore Chrome control by installing/enabling the ChatGPT/Codex Browser extension and native host. Confirm the connected Chrome session before any required browser claim.
2. Connect the approved controlled mailbox. Do not substitute the currently connected unrelated Gmail account.
3. Configure production Serving, Sync, Member, and Admin secrets by name. Inventory with `wrangler secret list`; never print values.
4. Verify the exact Mailjet sender/domain, then prove Member request → provider success → controlled inbox → one-time consume → session. Prove replay rejection and expiry rejection without exposing tokens.
5. Deploy production Serving, Sync, Consumer, Member, Sites, Alerts, then Admin. Record immutable version IDs. Optional Workers remain fail closed.
6. Run the initial Bridge sync from Sync only. Verify inventory, `MediaJSON`, display controls, sync state, and a subsequent incremental sync. Then enable only the verified Sync schedules.
7. Provision one manual-source account/site/member/domain for PILOT-01 through Admin. Do not copy staging rows. Associate the real member domain and generate a production-serving embed.
8. Run Admin readiness. Required capabilities must be READY; optional capabilities must read DISABLED, MANUAL MODE, or NOT USED.
9. Install the production embed on the member page. Run the production smoke and the complete Chrome matrix before activation.
10. Test suspend/reactivate at a coordinated time, record rollback evidence, reactivate, then begin the monitoring window. Do not onboard another member.

Production deployment commands use the corresponding `wrangler.sneak-*.production.toml` files. Do not add production Sync cron until initial and incremental sync evidence passes. Do not add Alert or GrowthZone cron in this launch profile.

## Verification gates

Run the safe smoke script with explicit non-secret tenant inputs:

```powershell
$env:SNEAK_PRODUCTION_SITE_KEY = '<production-site-key>'
$env:SNEAK_PRODUCTION_MEMBER_ORIGIN = 'https://coconutcoastrealtors.org'
$env:SNEAK_PRODUCTION_MEMBER_PAGE_URL = 'https://coconutcoastrealtors.org/idx-test/'
node scripts/verify-production-launch.mjs
```

It verifies all seven health/build endpoints, the disabled capability profile, protected Member/Admin routes, authorized bootstrap, search, current detail/media, HTTPS member page, and that no Bridge secret is present in serving-path config. Auth secrets are supplied only through normal operator sessions, never through the script.

Required actual-Chrome QA against the real member page:

- 1440×900, 1024×768, and 390×844.
- Initial search, filters, map, listing cards, current detail and full fields, a 20+ photo gallery, address controls, Compare, property share, Recently Viewed, attribution, member branding, iframe height, and no horizontal overflow.
- No console/page errors and no serving-path Bridge requests.
- Member Portal sign-in, correct account/service/domain/branding/widgets/embed/leads/clients, and no cross-account leakage.
- Admin login, search/readiness/entitlement/domain/branding/audit, plus coordinated suspend/reactivate.
- One authorized listing and at least one known out-of-scope ListingKey must be tested against the configured tenant model; denial must be generic.

## Rollback

Rollback is non-destructive and begins by stopping exposure:

1. Suspend the account entitlement or site in Admin. Confirm public bootstrap is denied.
2. Disable the member-page production embed/route. Keep the prior staging embed available for the controlled test page if the operator chooses to restore it.
3. Disable Sync schedules and stop manual sync calls. Alert and GrowthZone schedules are already absent.
4. List deployments with `npx wrangler deployments list --config <production-config>` and restore a known version with `npx wrangler rollback <VERSION_ID> --config <production-config> --message "pilot rollback"`.
5. Confirm health and protected routes, then reactivate only after the failure is understood.

No production Worker deployment exists yet, so Worker/route rollback has not been executed and must not be reported as tested. Migration 0035 is forward-only; do not use a destructive down migration.

## D1 recovery

D1 Time Travel is available and a current bookmark was successfully retrieved on 2026-09-01. This confirms the mechanism, not a restore test.

1. Suspend the entitlement/site, remove public exposure, stop Sync, and hold all Worker mutations.
2. Identify a restore point without changing data:
   `npx wrangler d1 time-travel info sneak-idx-production --timestamp="<RFC3339>" --config wrangler.sneak.production.toml`
3. Have the authorized Cloudflare operator review the bookmark and impact. A restore overwrites the database in place and cancels in-flight queries.
4. Only with explicit recovery authorization, restore by bookmark:
   `npx wrangler d1 time-travel restore sneak-idx-production --bookmark=<BOOKMARK> --config wrangler.sneak.production.toml`
5. Retain the pre-restore bookmark returned by Cloudflare so the restore can itself be undone.
6. Re-run migration status, `PRAGMA foreign_key_check`, FK compatibility, smoke, tenant scope, and Chrome QA before reactivation.

Recovery owner: the authorized CCOR Cloudflare operator. A production restore was not performed during B2 foundation work.

## Monitoring and expansion gate

After activation, observe one controlled member for a defined 24-hour window. At activation, +1 hour, +4 hours, and +24 hours, record bootstrap/search/detail/media success, Sync freshness and listing count, Worker errors, HTTPS/domain state, Member Portal access, and lead delivery. Review Cloudflare observability without logging links, sessions, API keys, or tokens.

Expansion requires: zero unresolved severity-1/2 issues, stable Sync freshness, complete required email evidence, passing production smoke and three-viewport Chrome QA, passing tenant negative test, tested suspend/reactivate and Worker rollback, and operator review of the monitoring record. SEO and additional members remain deferred.
