# CCOR IDX Phase 7.4B1 Launch Readiness

Current 7.4B1 implementation gate: **PASS**. Shared provider hardening, GrowthZone reconciliation, migration, staging deployment, automated regression, live checks, and actual-browser Admin/Member/Consumer acceptance pass.

Operational launch gate: **NOT READY**. Member has provider credentials but lacks current controlled-inbox proof. Consumer, Alerts, alert signing, and GrowthZone credentials are absent. Readiness remains fail closed until an authorized operator supplies those values and controlled-inbox/API evidence is recorded.

Build: `2026.09.01.7.4b1` for Admin, Member, Consumer, and Alerts. The untouched Serving/IDX build remains `2026.08.31.7.4a`.

Environment assessed: staging

Production cutover: not started

## Control-plane audit

The existing `sneak-admin/` Worker remains the staff control plane. Phase 7.4A reuses its hardened session authentication, account/site/domain/branding/widget APIs, embed generator, Cloudflare for SaaS operations, launch-check table, and bounded audit log. It adds a resumable guided UI, an operational account detail, normalized account readiness, capability-level status, and non-destructive lifecycle actions.

Existing canonical records naturally preserve workflow progress; no wizard-state table was added. Staff can stop after any persisted step and resume from Account Detail.

| Area | Before 7.4A | 7.4A result | Remaining manual/external work |
| --- | --- | --- | --- |
| Admin auth | PBKDF2, revocable D1 session, CSRF, rate limit | Preserved and reviewed; deterministic health/build | Rotate/manage password hash through Wrangler secret operations |
| Accounts/sites | CRUD and atomic starter provisioning | Guided, resumable workflow and operational detail | Final business approval |
| Entitlements | Generic table existed, but duplicated fail-open logic | Central fail-closed authority plus bounded GrowthZone reconciliation | Configure authenticated GrowthZone access and record real reconciliation evidence |
| Member users | Invite association and Member Worker dispatch | Included in setup progress; cross-account reassignment rejected | Member must consume invitation |
| Domains | Allowlist plus Cloudflare for SaaS prepare/refresh/remove | Pending-by-default association, human status, deliberate removal | DNS/ownership/certificate steps |
| Branding/widgets/embed | APIs existed; snippet collection shape was inconsistent | Current responsive snippets, safe defaults, copy/install notes | Member installs snippet and staff visually confirms |
| Readiness | Global launch checks/readiness existed | Per-account serving and launch results reuse the same infrastructure | External/manual launch evidence |
| Audit | Bounded global recent history | Bounded latest-50 account/site/member/domain history | Retention policy is future work |

No D1 migration was required. Migrations `0001`–`0005`, `0013`–`0016`, and `0018`–`0021` already contain the canonical lifecycle state.

## Billing authority

MVP billing authority is `sneak_account_entitlements`.

- Sources: `manual`, `growthzone`
- Statuses: `active`, `grace`, `delinquent`, `suspended`, `canceled`
- Operational fields: plan, effective/expiration/grace dates, external reference, notes, last verified date
- GrowthZone in 7.4B1: explicit durable reference, bounded Admin reconciliation, and protected manual override
- Stripe checkout: not implemented
- Stripe authority: no

Migration `0016_sneak_billing.sql` and its Stripe-shaped tables are retained only for compatibility with already-migrated environments. They are legacy/future-unused for the CCOR MVP. Neither runtime serving readiness nor the Admin UI reads an old Stripe row to grant access.

## Authoritative lifecycle

| Entity | States | Operational meaning |
| --- | --- | --- |
| Account | `active`, `suspended`, `inactive` | `active` is required. Suspended is a temporary stop; inactive is used for canceled service. |
| Entitlement | `active`, `grace`, `delinquent`, `suspended`, `canceled` | Active serves. Grace/delinquent serve only through a valid `grace_until`. Suspended/canceled never serve. Missing rows fail closed. |
| Site | `active`, `suspended`, `inactive` | Only active sites serve. Disabling does not delete the site key or settings. |
| Domain | `verified` 0/1 plus `active`/`disabled`; binding requested/pending/active/error/removing/removed | Serving requires a verified, active tenant domain. Cloudflare binding state explains DNS/SSL progress. |
| Member user | `invited`, `active`, `disabled` | Invited/active establish association. Only active sessions receive operational Member access. |

The public serving decision is:

`account active AND site active AND generic entitlement permits service AND domain active/verified AND tenant scope valid`.

The shared implementation is `sneak-shared/entitlement.js`. Stable runtime blocker codes are `ACCOUNT_INACTIVE`, `SITE_INACTIVE`, `ENTITLEMENT_MISSING`, `ENTITLEMENT_INACTIVE`, `ENTITLEMENT_EXPIRED`, `GRACE_EXPIRED`, `DOMAIN_UNAUTHORIZED`, and `SCOPE_INVALID`.

Suspend and cancel never delete sites, domains, branding, widgets, consumers, favorites, saved searches, leads, shared lists, or audit history. Reactivation corrects canonical account/entitlement/site state and reuses the same records.

## Staff provisioning workflow

1. Validate member/MLS identity and record member, agent, and office identifiers.
2. Create the account and record an authoritative manual/GrowthZone entitlement.
3. Create the IDX site, immutable site key, and agent/office/market scope.
4. Associate/invite the Member Portal owner.
5. Add the member hostname pending; complete ownership, Cloudflare, and authorization steps.
6. Complete branding/contact identity.
7. Enable the responsive search widget and copy the current embed.
8. Run account readiness; resolve each explicit blocker before launch approval.

Creating an account is not launch activation. Missing entitlement/domain/scope records fail closed.

## Readiness model

Per-account API: `GET /api/admin/accounts/:id/readiness`.

`canServe` reports only the canonical runtime serving decision. `launchReady` adds member association, MLS identity/inventory, branding, widget, embed, Serving Worker, bootstrap, sync freshness, and inventory checks. Account Detail labels checks as automated. Existing `/api/admin/launch-checks` evidence is labeled automated versus manual instead of creating a competing checklist table.

Core and optional capabilities are separate:

- Core search: requires account launch readiness.
- Custom domain: requires active/verified domain and successful bootstrap.
- Member Portal: requires reachable Worker and member association.
- Admin Portal: core operational control plane.
- Member magic-link email: provider configuration plus controlled-inbox delivery/consumption evidence.
- Consumer magic-link email: provider configuration plus controlled-inbox delivery/consumption evidence.
- Saved-search email alerts: provider/signing configuration plus ASAP, Daily, duplicate, and unsubscribe evidence.
- GrowthZone reconciliation: API configuration plus authenticated explicit-mapping evidence; stale verification is reported separately.

Installation detection does not crawl arbitrary websites. Bootstrap validates a supplied authorized origin; final embed installation and visual/mobile checks remain manual launch confirmations.

## Consumer filter/detail parity

The consumer detail modal renders only current, display-authorized D1 listing fields. It does not reconstruct stale fields from cards, local storage, activity history, shared lists, or Compare state.

| Search/filter concept | Current detail treatment | Category/context rule |
| --- | --- | --- |
| Property category and subtype | Property Type and Property Subtype | Residential, Rental, Land, and Commercial headings/facts are selected from authoritative type fields |
| Current price | Header plus Current Price and Current List Price | Positive values only |
| Price reduced | Original List Price, Current List Price, and computed reduction | Shown only when both positive prices exist and original is greater than current |
| Beds and baths | Bedrooms and Bathrooms | Residential/Rental only; zero or missing values omitted |
| Living/building area | Living Area or Building Area | Residential/Rental or Commercial only; zero or missing values omitted |
| Lot size | Lot Size | All categories when positive; zero acres omitted |
| Year built | Year Built | Non-Land only when positive |
| Waterfront | Affirmative Waterfront feature | Shown only for an affirmative source value |
| Private pool | Affirmative Private Pool feature | Residential/Rental only and affirmative source value |
| Garage | Garage Spaces | Non-Land only when positive |
| New construction | Affirmative New Construction feature | Non-Land only and affirmative source value |
| City, County, ZIP, Subdivision, Zoning | Location & Community facts | Only available current fields; street/unit suppression remains controlled by `InternetAddressDisplayYN` |
| Listing date and MLS/List ID | Listing Information | Current Listing Contract Date and Listing ID/Key only |
| Listing office and agent | Listing Information plus attribution | Current source values only; no brokerage/agent inference |
| Open house | Not duplicated into the listing-detail record | Remains a separate current-state open-house query/surface; no historical DOM reconstruction |
| Public remarks | Full Description section | Authoritative untruncated `PublicRemarks`; readable pre-line wrapping |

Unknown or unavailable fields are omitted. The UI never invents bedrooms for Land/Commercial, never displays zero acreage/area/garage as a meaningful feature, and never calls Bridge from the consumer serving path.

## Acceptance evidence

Phase 7.4B1 evidence:

- Automated regression: `262/262` tests passing across 14 suites.
- Focused affected regression: `101/101` tests passing across five suites.
- Live staging verification: `22/22`; exact affected builds, secret-free health, fail-closed provider/signing/GrowthZone state, auth boundaries, unchanged Serving health, and pilot availability.
- Focused actual-Chrome acceptance: `123/123`; live Admin reconciliation/manual override/capabilities, authenticated Member/C2B and login failure states, authenticated Consumer saved-search/Daily preference, generic no-token Consumer login response, and desktop/tablet/mobile overflow checks.
- Full actual-Chrome regression: `748/748` at 1440Ã—900, 1024Ã—768, and 390Ã—844, including the 11-listing Residential/Rental/Land/Commercial matrix and complete authenticated Member Portal.
- Migration `0034_sneak_growthzone_reconciliation.sql` is applied locally and remotely; staging reports no pending migrations.
- Staging deployments: Admin `ea6c1aa8-2468-4875-aed7-fd97fed7db8b`, Member `c005a50e-3964-4554-aa40-d022e570cb72`, Consumer `3921e019-6120-4358-8d78-ad3c460ecb64`, Alerts `5a3c22d6-ce1f-48f8-80db-c4c58ee5d1c9`.

Phase 7.4A retained evidence:

- Automated regression: `246/246` tests passing across 13 suites.
- Live authoritative detail/media verification: `65/65` checks across 10 real multi-photo listings; D1 `MediaJSON`, detail `Media`, and `/media` counts agree.
- Live Phase 7.4A health/auth verification: `17/17` checks; Admin, Member, Serving, Consumer, Sites, and Alerts are reachable, affected builds are current, health output is secret-free, and unauthenticated Admin API access returns 401.
- Actual-browser acceptance: `748/748` checks using local Google Chrome through Playwright against staging at 1440×900, 1024×768, and 390×844.
- Consumer browser matrix: five Residential, two Rental, two Land, and two Commercial listings at every viewport, including eight samples with more than 20 photos and 10 multi-photo listings (approximately 9–50 photos). Checks covered current price, safe address, status/type/subtype, city, listing office/agent attribution, meaningful filter-to-detail field parity, contained imagery, counts, bounded 12-thumbnail window/downloads, thumbnail selection, previous/next traversal, responsive detail hierarchy, full remarks, category facts, zero-value suppression, rapid A→B race safety, no image errors, no page errors, and no Bridge traffic. Search card, map popup, current Just Listed carousel, Recently Viewed, Saved Homes, Compare, Shared List, and direct `ccor_listing` all opened through the same authoritative detail/media request path.
- Admin browser workflow: authenticated dashboard; isolated guided provisioning; all Account Detail sections; domain authorization; entitlement/branding/embed operations; search; suspend/reactivate; core-versus-optional readiness; protected API checks; and responsive/no-overflow checks at all three viewports. Render ownership prevents completed background mutations from overwriting later operator navigation.
- Member/C2B browser regression: authenticated Overview/service state, Website & Domains, Branding, Widgets, Embed Code, Leads, Subscription & Billing, Clients navigation, four KPI cards, email search, sorting, Client Detail, Saved Homes, Saved Searches & Alerts, Activity Timeline, Inquiries, protected APIs, modal fit, and no horizontal clipping at all three viewports.
- Privacy: anonymous listing views created no Consumer activity request; Recently Viewed retained only site-scoped listing keys/timestamps; Compare retained only site-scoped listing keys; isolated authenticated fixtures and temporary Admin sessions were removed after QA.
- Remote D1: no pending migrations and `PRAGMA foreign_key_check` returned zero rows.

Repeat the browser sign-off with `node scripts/qa-phase74a-browser.mjs`. The acceptance run must not set `PHASE74A_BROWSER_SKIP_LISTING`.

## External dependency readiness matrix

Only secret names are listed. No values belong in this document or any readiness response.

| Dependency | Basic launch | Staging status | Secret/config names | Health / evidence | Failure effect | Operator action |
| --- | --- | --- | --- | --- | --- | --- |
| Serving Worker | Yes | Configured | `SNEAK_SIGNING_SECRET` | `/idx/v1/health`, authorized bootstrap | Public IDX denied | Verify health, signing secret, D1, entitlement/domain |
| Sync Worker | Yes | Configured; schedules remain staging config | `BRIDGE_TOKEN` | `/health`, `sneak_sync_state` freshness | Inventory becomes stale | Restore token/run; never call Bridge from serving/Admin |
| Consumer Worker | Optional for base search | Worker healthy; shared real-provider path active; no provider secrets, so fail closed | `MAILJET_API_KEY`, `MAILJET_SECRET_KEY`, `EMAIL_FROM` | `/api/consumer/version`, controlled-inbox test | Buyer magic links do not arrive | Configure provider and verify sender/inbox |
| Member Worker | Yes for self-service | Worker healthy; both Mailjet secret names configured; current real inbox delivery not verified | `MAILJET_API_KEY`, `MAILJET_SECRET_KEY`, `EMAIL_FROM` (or legacy provider names) | `/health`, real invitation/login test | Member cannot receive sign-in link if provider/sender fails | Run current invitation/login inbox verification |
| Admin Worker | Yes | Configured | `SNEAK_ADMIN_PASSWORD_HASH`, `SNEAK_WEBSITE_PREVIEW_SECRET`, Cloudflare SaaS names | `/health`, authenticated API | Staff control plane unavailable | Restore secret/bindings; revoke sessions if needed |
| Sites Worker | Only for hosted sites | Configured | `SNEAK_WEBSITE_PREVIEW_SECRET` | `/health`, signed preview/custom host | Hosted template unavailable | Restore secret/binding/domain |
| Alert Worker | Optional | Worker healthy; delivery not ready | `MAILJET_API_KEY`, `MAILJET_SECRET_KEY`, `SNEAK_ALERT_UNSUBSCRIBE_SECRET` | `/health` `deliveryReady` | Saved-search emails defer/retry | Configure all three and run real delivery/replay QA |
| D1 | Yes | Configured; canonical consumer FKs; migrations current | Binding `DB` | FK preflight, migrations list | Platform data APIs fail closed | Stop launch; repair/migrate safely |
| Cloudflare SaaS | Required for custom host | Live configuration present; per-host DNS/SSL remains operational | `CLOUDFLARE_SAAS_API_TOKEN` plus zone/config vars | Admin diagnostic, binding refresh, HTTPS/bootstrap | Custom hostname unavailable | Resolve DNS, ownership, certificate, API availability |
| GrowthZone process | Yes for paid entitlement operations | Admin reconciliation deployed; API key absent, so fail closed | `GROWTHZONE_API_KEY`; non-secret base URL/map/limit vars | Admin health, single/bulk reconciliation, launch evidence | Verification becomes stale; canonical state is preserved | Configure API key and verify one explicit mapping |

Domain UI translates provider state into operator language: DNS/ownership missing, certificate pending, hostname active, or Cloudflare API unavailable. Credentials never enter D1 audit text or API output.

## Email operational audit

- Member magic link: `MAILJET_API_KEY` and `MAILJET_SECRET_KEY` names are present on the staging Member Worker, so requests select the strict shared Mailjet adapter. Public responses never return the raw token. Current controlled-inbox delivery/consumption is not verified, so readiness is `NOT_VERIFIED`.
- Consumer magic link: the shared real-provider path is deployed and simulation is removed. Staging has no provider secrets, so dispatch returns `provider_unconfigured`; controlled-inbox delivery is not attempted and readiness is `NOT_READY`.
- Saved-search alerts: the strict shared Mailjet provider and signed unsubscribe path are deployed. Mailjet and unsubscribe-signing secrets remain absent, delivery is fail closed, and ASAP/Daily/unsubscribe inbox evidence is not available.
- Full operator procedure and current matrix: `docs/operations/transactional-email.md`.

## Admin security review

- Four-hour server-side revocable Admin sessions; login rotates the actor’s prior active session.
- `__Host-sneak_admin_session` is HttpOnly, Secure, SameSite Strict, and path `/`.
- Same-origin CSRF is mandatory for every mutation; foreign or missing origins fail.
- PBKDF2-SHA256 hash configuration fails closed; 10 failed attempts per IP hash in 15 minutes triggers rate limiting.
- CSP, frame denial, nosniff, no-store API/UI responses, restricted permissions, and pseudonymized IP/UA audit inputs remain.
- Account detail audit is bounded to 50 newest matching rows.
- No raw password, token, session hash, Cloudflare credential, or email credential appears in UI/readiness output.
- The hard-coded website preview fallback secret was removed; preview signing now fails closed when unconfigured.

## Production plan for Phase 7.4B2 (document only)

1. Create distinct production Worker names and a new production D1 database; do not copy staging consumer/member data by default.
2. Apply the full ordered migration set after the FK compatibility preflight; record migration output.
3. Provision secret names per the matrix through Wrangler/Cloudflare secret storage and verify no values are committed.
4. Configure production Cloudflare for SaaS zone, fallback origin, customer CNAME, routes, certificates, and DNS.
5. Configure production sync and alert cron schedules only after Bridge/email/signing verification.
6. Configure and verify GrowthZone-to-generic-entitlement reconciliation, retaining manual override/audit procedures.
7. Run isolated production fixtures, authenticated Admin/Member QA, responsive consumer regression, real inbox tests, custom-host TLS/bootstrap tests, and legacy zero-diff.
8. Cut traffic gradually. Roll back by restoring prior Worker deployments/routes; do not roll back D1 destructively. Suspend affected entitlements if serving safety is uncertain.

SEO Phase 7.3D remains deferred/post-launch.
