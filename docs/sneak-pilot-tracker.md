# SNEAK IDX — Controlled 3-Member Pilot Tracker
**Phase 7 Operational System & Feedback Log**

* **Repository:** `Kweinkauff29/Community-Website`
* **Branch:** `feature/sneak-idx-platform`
* **Pilot Target:** 3 Real Association Members (1 Embed, 1 Essential Website, 1 Coastal Website, optional 1 Brokerage)
* **Status of Truth:** GrowthZone (financial/member system of record)
* **Custom Domain Target:** `sneak-customers.coconutcoasthomes.com`
* **Security Protocol:** Strictly zero secrets, passwords, magic link tokens, or card data stored in this file.

---

## 1. Pilot Member Master Registry

| Pilot ID | Member Name | MLS ID | Email | Website / Custom Domain | Account ID | Site ID | Plan | Product Type | Template | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **PILOT-01** | Ursula Weinkauff | `633942` | `kmwcollegeapps@gmail.com` | `https://coconutcoastrealtors.org` | `acc_1787583729221_cv3ma` | `site_1787583729221_rzxfa` | `standard` | IDX Embed Only | N/A / embed | Launched |
| **PILOT-02** | *Pending Operator Input* | *Pending* | *Pending* | *Real Custom Domain* | *Pending* | *Pending* | `pro` | Turnkey Website | `essential` | Planned |
| **PILOT-03** | *Pending Operator Input* | *Pending* | *Pending* | *Real Custom Domain* | *Pending* | *Pending* | `pro` | Turnkey Website | `coastal` | Planned |
| **PILOT-04** *(Opt)* | *Pending Operator Input* | *Pending Office ID* | *Pending* | *Real Custom Domain* | *Pending* | *Pending* | `brokerage` | Brokerage Website | `brokerage` | Deferred |

---

## 2. Universal Launch Checklist (Per Participant)

Every participant must complete all verification gates before being marked **Launched**:

- [x] **GrowthZone Gate:** OPERATOR-CONFIRMED INTERNAL PILOT (Internal staff/operator participant).
- [x] **SNEAK Entitlement Gate:** Entitlement active (`source = 'growthzone'`, `plan = 'standard'`, `external_reference = 'PILOT-01-INTERNAL'`).
- [x] **MLS Scope Validation:** Verified Full-Market IDX scope (`scope_type = 'market'`, 32,059+ active inventory items across 1,668 listing offices); participant identity mapped to `633942` (Ursula S Weinkauff) for featured listings and lead routing; foreign listings return 200 OK with mandatory listing brokerage attribution.
- [x] **Invitation Gate:** Verified live Mailjet authentication previously satisfied & account normalized.
- [x] **Member Portal Gate:** Member successfully authenticated and accessed portal dashboard for `Ursula Weinkauff — SNEAK Pilot`.
- [x] **Branding & Config Gate:** Display name (`Ursula Weinkauff`), brokerage (`Local Real Estate LLC`), colors (`#0f2942`, `#2b6cb0`), and email configured.
- [x] **Domain / Embed Gate:** PASS — Installed and executing on `https://coconutcoastrealtors.org/idx-test/` with authorized Origin `https://coconutcoastrealtors.org`.
- [x] **Live HTTPS Gate:** PASS — Loads over HTTPS 200 OK with zero mixed content errors.
- [x] **Live IDX Functionality Gate:** PASS — Full-market search (32,000+ listings), interactive map, featured agent listings (`/idx/v1/agent/633942/listings`), address suppression controls (`InternetAddressDisplayYN`), and internet display compliance (`InternetEntireListingDisplayYN`) verified live.
- [x] **Lead Ingestion Gate:** PASS — Live frontend lead ingestion routes seamlessly to participant's tenant account (`acc_1787583729221_cv3ma`).
- [x] **Cross-Device Gate:** PASS — Responsive desktop and mobile layout verified with zero horizontal overflow.
- [x] **Staff Observation Gate:** Onboarding duration, live verification metrics, and intervention count logged.

---

## 3. Detailed Participant Records & Logs

### PILOT #1: IDX Embed Only (Existing Member Website)
* **Goal:** Prove SNEAK IDX container & embed script operate cleanly inside a third-party host (WordPress Elementor/Beaver Builder) with full-market IDX consumer search and participant-scoped lead capture & featured listings.
* **Participant Details:**
  * MLS Participant / REALTOR: Ursula Weinkauff
  * MLS Agent ID / Participant Identity: `633942`
  * Brokerage: Local Real Estate LLC
  * Controlled Pilot Operator: Kevin Weinkauff
  * Controlled Pilot Email / Login: `kmwcollegeapps@gmail.com`
  * Pilot Type: Internal Controlled IDX Embed Validation
  * Existing Member Website URL: `https://coconutcoastrealtors.org`
  * Authorized Embed Origin: `https://coconutcoastrealtors.org`
  * Target Test Page: `https://coconutcoastrealtors.org/idx-test/`
  * Account ID: `acc_1787583729221_cv3ma`
  * Site ID: `site_1787583729221_rzxfa`
  * Site Key: `ursula-weinkauff-pilot`
  * Display Scope: `market` (Full-market MLS consumer search)
  * Featured Scope: `agent` (`633942` — Ursula Weinkauff)
  * Plan: `standard`
* **Operator Authorization Note:**
  * "Operator explicitly approved use of Ursula Weinkauff's verified agent scope 633942 for PILOT-01 while Kevin Weinkauff operates the controlled pilot using kmwcollegeapps@gmail.com."
* **Phase 7.1 Full-Market Model Correction:**
  * *Context:* SNEAK originally restricted ordinary consumer search queries to the participant's `ListAgentMlsId`. In Phase 7.1, tenant display scope was decoupled from participant identity: ordinary consumer search, map, open houses, and listing details now query full MLS market inventory (`37,145` active listings across SWFL), while `agent_mls_id = 633942` powers featured listings (`/idx/v1/agent/633942/listings`) and lead routing.
  * *Internet Display Rules:* Applied migration `0022_sneak_idx_display_controls.sql` enforcing Bridge OData `InternetEntireListingDisplayYN` (suppress non-IDX listings) and `InternetAddressDisplayYN` (mask address as "Address Undisclosed" when 0).
* **Operational Milestones:**
  * Account & Site Provisioned Date: 2026-08-24 20:33 UTC
  * Invitation Gate Satisfied Date: 2026-08-24 16:03 UTC (Pre-verified real Mailjet auth normalized)
  * Embed Snippet Installed on Page: 2026-08-25 12:58 UTC
  * Phase 7.0 Initial Verification Date: 2026-08-25 13:00 UTC
  * Phase 7.1 Full-Market Model Deployment & Live Verification: 2026-08-25 14:45 UTC (100% PASS)
  * Phase 7.2 Product Corrections & Live Verification: 2026-08-25 16:25 UTC (100% PASS)
  * Phase 7.3B1 Interactive Map/List Synchronization & Mobile UX: 2026-08-27 17:15 UTC (100% PASS)
  * Phase 7.3B2A Radius & Spatial State Search: 2026-08-27 18:30 UTC (100% PASS)
  * Phase 7.3B2B Server-Authoritative Polygon Search: 2026-08-27 19:45 UTC (100% PASS)
  * Phase 7.3C1A Buyer Identity, Magic Link Auth & Server Favorites: 2026-08-27 20:25 UTC (100% PASS)
  * Phase 7.3C1B Embed Layout Stabilization & Saved Searches: 2026-08-28 15:10 UTC (100% PASS)
  * Phase 7.3C1B.1 Live WordPress Embed Layout Hotfix & Browser QA: 2026-08-28 16:00 UTC (100% PASS)
  * Phase 7.3C2A Saved Search Email Alert Engine & Delivery Infrastructure: 2026-08-30 21:50 UTC (100% PASS)
  * Phase 7.3C2A.1 Alert Delivery Correctness, Concurrency & Secret Hardening Hotfix: 2026-08-30 23:00 UTC (100% PASS)
  * Phase 7.3C2B Agent Client Activity Dashboard & Buyer Timeline: 2026-08-31 (COMPLETE; authenticated desktop/tablet/mobile browser QA pass)
  * Phase 7.3C3A.1 Recently Viewed Retention, Migration Safety & Browser Sign-Off: 2026-08-31 (COMPLETE; deployed and all acceptance gates pass)
  * Phase 7.3C3B Safe Property Sharing + Consumer Shareable Property Lists: 2026-08-31 (COMPLETE; deployed, independent anonymous recipient and revocation browser QA pass)
* **Phase 7.3C2B Agent Client Activity Dashboard & Authenticated Buyer Timeline:**
  * *Privacy-Preserving Activity Ledger:* D1 table `sneak_consumer_activity_events` (migration 0030) records authenticated buyer events (`listing_view`, `favorite_*`, `saved_search_*`, `alert_*`, `inquiry_submitted`). Zero anonymous tracking, zero fingerprinting, zero geolocation tracking.
  * *Consumer Ingestion Protection:* `POST /api/consumer/activity` enforces authentication, strict browser allowlist (`listing_view`, `inquiry_submitted`), 30-minute deduplication on listing views, lead email match verification, and 120 events/hr rate limiting.
  * *Member Portal Clients Roster & Timeline:* REALTOR® Member Portal features full Clients navigation tab, 4 live aggregate KPI summary cards, email search filter, multi-criteria sorting, and responsive Client Detail modal with interactive longitudinal activity timeline, saved homes (with display controls), saved searches, and inquiries.
  * *Strict Tenant Isolation:* All member client and timeline queries strictly scoped by authenticated `account_id`; cross-account lookups return 404 with zero enumeration.
* **Live Validation Evidence:**
  * Live Public URL: `https://coconutcoastrealtors.org/idx-test/` (HTTP 200 OK)
  * Live Static UI Build: PASS — `data-ui-build="2026.08.30.7.3c2b"`, `CCOR_IDX_UI_BUILD = '2026.08.30.7.3c2b'`, `embed.js &v=2026.08.30.7.3c2b`
  * Live Alert Worker Health: PASS — `https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev/api/alerts/version` (200 OK, `sneak-alerts-worker`, `2026.08.30.7.3c2b`, `healthy`, `deliveryReady: false`)
  * Live Consumer Worker Health: PASS — `https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev/api/consumer/version` (200 OK, `sneak-consumer-worker`, `2026.08.30.7.3c2b`, `healthy`)
  * Live Member Worker Health: PASS — `https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/health` (200 OK, `sneak-idx-member-staging`, `2026.08.30.7.3c2b`, `healthy`)
  * Live Member Worker Clients Route Security: PASS — `https://sneak-idx-member-staging.bonitaspringsrealtors.workers.dev/api/member/clients` (401 Unauthorized for unauthenticated requests)
  * Automated Regression Suite: PASS — 133/133 tests passing across 9 test suites (`node --test test/*.test.mjs`)
  * Live Automated Verification: PASS — 8/8 live verification assertions passing (`node scripts/verify-phase73c2b-live.mjs`)
  * Required C2B Authenticated Browser QA: **PASS** — actual authenticated Chromium verification at 1440×900, 1024×768, and 390×844 covered Clients navigation, KPIs, search/sort, roster and detail, Saved Homes, Saved Searches/Alerts, Activity Timeline, Inquiries, modal scrolling, responsive behavior, and zero clipping/horizontal overflow. The saved-home image fallback defect found during QA was fixed and the three viewports reran with zero page errors.
* **Phase 7.3C3A.1 Recently Viewed + Property Compare Corrective Sign-Off:**
  * *Recently Viewed Privacy & Retention:* Authenticated history is derived only from existing `listing_view` events, limited to 20 unique current listings within 90 days. Anonymous history stays in site-scoped local storage as at most 20 `{ key, viewedAt }` entries for 30 days, is cleaned in storage, survives ordinary logout, and is never uploaded after login.
  * *Compare Persistence:* Anonymous compare stores site-scoped listing keys only. Authenticated compare uses `sneak_consumer_compare`, merges eligible local keys after login, and enforces a four-property maximum in both API logic and D1.
  * *Current-State Safety:* All recent/compare reads and writes validate current site scope, display permission, and active status; stored references never expose historical listing snapshots.
  * *Contextual UI:* Search cards and listing detail include compare actions, with a responsive tray and Residential/Rental/Land/Commercial comparison modal.
  * *D1 Migrations:* `0031_sneak_consumer_compare.sql` remains applied. Wrangler was verified to track migration name and applied timestamp without checksums; `0032_sneak_site_fk_compatibility.sql` is now a safe marker while the staging-specific empty-schema repair is retained as an explicit manual operations artifact. Canonical/legacy preflight fails closed where data-preserving assessment is required; no remote migrations are pending.
  * *Authenticated Cross-Device Browser QA:* PASS — independent browser sessions A and B showed the same two authenticated Compare listings and newly viewed authenticated listing. Zero Compare activity events and zero retroactive events for anonymous pre-login history were recorded; isolated QA data was removed and verified absent.
  * *Automated Regression:* PASS — 157/157 tests across 10 suites.
  * *Live Endpoint Verification:* PASS — 12/12 checks for build health, auth boundaries, pilot bootstrap/search, bounded current-state summaries, assets/embed, and the live WordPress pilot.
  * *Anonymous Privacy Browser QA:* PASS — no anonymous consumer request or activity POST, site-scoped recent keys/timestamps only, compare listing keys/order only, no full listing records, and no history upload after login.
  * *Required C3A Browser QA:* **PASS** — actual Chromium QA at 1440×900, 1024×768, and 390×844 covered card/detail Compare, two/three/four columns, fifth rejection, tray/remove/clear, all four property contexts, Recently Viewed 20-item cleanup, listing detail, favorites, Saved Search prompt, map, responsive readability, and unobstructed controls. The mobile flex-basis issue found during QA was fixed and rerun without app/host overflow or page errors.
  * *Build:* `2026.08.31.7.3c3a1`.
* **Phase 7.3C3B Safe Property Sharing + Consumer Shareable Property Lists:**
  * *Property Share:* Listing detail and public-list cards support native Web Share with a member-domain `ccor_listing` URL. The fallback uses Clipboard API when permitted and a selectable manual-copy field when iframe permissions block clipboard writes.
  * *URL Safety:* The Serving Worker validates the host page against active verified tenant domains, requires HTTPS outside development, removes fragments, auth/session/bootstrap/magic/bearer values, and stale deep links, then returns the only base URL the iframe may use for sharing.
  * *Private Lists & Limits:* Remote migration `0033_sneak_consumer_shared_lists.sql` is applied. D1 stores listing references only and enforces 10 lists per consumer/site, 25 items per list, 80-character names, uniqueness, and cascading deletion. Lists remain private until the owner explicitly enables sharing.
  * *Capability & Revocation:* Public slugs contain 192 bits of cryptographic randomness, are unlisted and tenant-bound, and expose no owner identity. Disabling sharing immediately revokes and clears the old slug; re-enabling rotates to a new slug.
  * *Current MLS & Privacy:* Public reads use only current `sneak_listings` state with site scope, available status, Internet display and address suppression, plus current listing-office attribution. Anonymous reads create no Consumer account, activity event, fingerprint, share analytics, or third-party tracking and are marked `noindex, nofollow`.
  * *Consumer Integrations:* Buyer account Shared Lists supports create, rename, view, remove, delete, enable, copy, and disable. Add to List works from detail, Saved Homes, and Recently Viewed; Compare saves to a private list.
  * *Automated Regression:* PASS — 188/188 tests across 11 suites.
  * *Live Endpoint Verification:* PASS — 12/12 covering affected builds, Consumer owner-route protection, C3A protections, signed pilot bootstrap, host URL sanitization, current listing summaries, generic public capability failure, assets/embed, Member health, Alert state, and pilot availability.
  * *Required Browser QA:* PASS — actual Chromium at 1440×900, 1024×768, and 390×844 covered property sharing, manual-copy fallback, owner management, Saved Homes/Recently Viewed/Compare integration, contextual public list, listing detail, responsive controls, and zero IDX modal clipping/overflow. A modal stack-order defect found during live QA was fixed, regression-tested, redeployed, and rerun.
  * *Independent Recipient & Revocation:* PASS — authenticated Browser A enabled sharing; independent anonymous Browser B loaded current properties without login, identity disclosure, or any Consumer Worker POST; after Browser A disabled sharing, Browser B refreshed the original URL and received the generic unavailable state.
  * *Alert Delivery State:* Mailjet configured: NO; signing secret configured: NO; delivery ready: NO; real email delivery: NOT VERIFIED / NOT ATTEMPTED. C3B has no email-service dependency and did not modify alert secrets.
  * *Build:* `2026.08.31.7.3c3b` for affected Serving/Consumer/embed/search surfaces.
* **Observability & Timing:**
  * Staff Setup Time: ~9 minutes
  * Member Implementation Time: ~2 minutes (HTML snippet paste in WordPress)
  * Staff Interventions Required: 15 (Identity reconciliation, Phase 7.1 model correction, Phase 7.2 product corrections, Phase 7.3A search parity, Phase 7.3B1 interactive map synchronization, Phase 7.3B2A radius search, Phase 7.3B2B polygon search, Phase 7.3C1A buyer auth, Phase 7.3C1B saved searches, Phase 7.3C1B.1 embed layout hotfix, Phase 7.3C2A email alert engine, Phase 7.3C2A.1 delivery hardening, Phase 7.3C2B client activity dashboard, Phase 7.3C3A.1 corrective sign-off, Phase 7.3C3B safe sharing/public lists)
  * Support Questions Logged: 0
* **Final Status:** **ACTIVE PILOT (Phase 7.3C2B, 7.3C3A.1, and 7.3C3B complete; automated, live API, independent-recipient revocation, and mandatory browser QA pass)**

---

### PILOT #2: Essential Website (Turnkey Agent Site)
* **Goal:** Validate self-service agent onboarding, Essential template presentation, and Cloudflare SaaS custom domain connection.
* **Participant Details:**
  * Member Name:
  * MLS Agent ID:
  * Email Address:
  * Custom Domain:
  * Brand Colors:
* **Operational Milestones:**
  * Account & Site Provisioned Date:
  * Invitation Sent / Accepted Date:
  * DNS CNAME Configured Date:
  * Cloudflare Hostname Active Date:
  * Live HTTPS Handshake Date:
  * Lead Ingestion Test Passed Date:
* **Observability & Timing:**
  * Staff Setup Time:
  * Member Onboarding Time:
  * DNS Configuration Friction Points:
  * Support Questions Logged:
* **Scope Verification Evidence:**
  * Verified In-Scope ListingKey:
  * Verified Foreign ListingKey (HTTP 403 Confirmed):
* **Issues / Observations:**
  * *(None recorded yet)*

---

### PILOT #3: Coastal Website (Editorial Luxury Agent Site)
* **Goal:** Evaluate Coastal high-aesthetic layout, visual branding, featured neighborhoods, and mobile editorial feel.
* **Participant Details:**
  * Member Name:
  * MLS Agent ID:
  * Email Address:
  * Custom Domain:
  * Brand Palette & Hero Assets:
* **Operational Milestones:**
  * Account & Site Provisioned Date:
  * Invitation Sent / Accepted Date:
  * DNS CNAME Configured Date:
  * Cloudflare Hostname Active Date:
  * Live HTTPS Handshake Date:
  * Lead Ingestion Test Passed Date:
* **Observability & Timing:**
  * Staff Setup Time:
  * Member Onboarding Time:
  * Template Customization Time:
  * Support Questions Logged:
* **Scope Verification Evidence:**
  * Verified In-Scope ListingKey:
  * Verified Foreign ListingKey (HTTP 403 Confirmed):
* **Issues / Observations:**
  * *(None recorded yet)*

---

### PILOT #4 (Optional): Brokerage Website (Multi-Agent Office Site)
* **Goal:** Validate office MLS scope, multi-agent presentation, and brokerage-level branding inventory.
* **Participant Details:**
  * Brokerage Name:
  * Office MLS ID:
  * Managing Broker Email:
  * Custom Domain:
* **Status:** Deferred until Pilots 1–3 are fully operational.

---

## 4. Member Feedback Questionnaire & Classification

### Standard Post-Onboarding Interview
1. **Setup Clarity:** Was the invitation and initial setup clear and straightforward?
2. **Branding Self-Service:** Did you understand where and how to adjust your branding, photos, and colors?
3. **Domain Guidance:** Were the instructions for connecting your domain or embedding the widget easy to follow?
4. **Search Experience:** Was property search intuitive and responsive?
5. **Data Correctness:** Did your active listings, open houses, and property details appear accurate?
6. **Lead Workflow:** Did you understand where incoming buyer leads appear?
7. **Autonomy:** Would you feel comfortable managing this ongoing without staff assistance?
8. **Unmet Expectations:** What did you expect to configure or customize that was not available?
9. **Friction:** What part of the process caused confusion or delay?
10. **Pricing Sensitivity:** At what monthly price point would this feel:
    * *Cheap / No-brainer:* $_____ / mo
    * *Fair & Reasonable:* $_____ / mo
    * *Expensive / Hesitant:* $_____ / mo

### Feedback Classification Matrix
* `BUG`: Immediate defect affecting security, auth, domain, data correctness, or lead ingestion. *(Fix immediately)*
* `CONFUSION`: Member misunderstanding existing UI or workflow instructions. *(Address in docs/copy)*
* `ONBOARDING`: Friction or delay during account invitation or DNS record setup.
* `CONFIGURATION GAP`: Member desired an available setting that was hard to locate.
* `FEATURE REQUEST`: Net new functionality requested by member. *(Log only; apply 3-request threshold)*
* `NICE-TO-HAVE`: Minor cosmetic or non-critical enhancement.

---

## 5. Daily Platform Health Verification Log

| Date | Time (UTC) | MLS Sync Freshness | Active Listings | Open Houses | Serving Worker | Cloudflare SaaS | Mailjet Status | Launch Blockers | Auditor |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **2026-08-24** | 17:25 | 10 min | 32,069 | 942 | Healthy | Live / Active | Verified | None (0) | System Preflight |
