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
  * Phase 7.3C2B Agent Client Activity Dashboard & Buyer Timeline: 2026-08-31 09:30 UTC (deployed; required authenticated desktop/tablet/mobile browser sign-off pending)
  * Phase 7.3C3A Recently Viewed + Property Compare: 2026-08-31 (deployed; automated and live API QA pass; required browser sign-off pending)
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
  * Required C2B Authenticated Browser QA: **PENDING** — the in-app browser runtime reported no connected browser. Clients navigation, KPI cards, search/sort, Client Detail, Saved Homes, Saved Searches, Activity Timeline, Inquiries, responsive behavior, and clipping/overflow still require actual verification at 1440×900, 1024×768, and 390×844.
* **Phase 7.3C3A Recently Viewed + Property Compare:**
  * *Recently Viewed Privacy:* Authenticated history is derived only from existing `listing_view` events. Anonymous history stays in site-scoped local storage as `{ key, viewedAt }` and is never uploaded after login.
  * *Compare Persistence:* Anonymous compare stores site-scoped listing keys only. Authenticated compare uses `sneak_consumer_compare`, merges eligible local keys after login, and enforces a four-property maximum in both API logic and D1.
  * *Current-State Safety:* All recent/compare reads and writes validate current site scope, display permission, and active status; stored references never expose historical listing snapshots.
  * *Contextual UI:* Search cards and listing detail include compare actions, with a responsive tray and Residential/Rental/Land/Commercial comparison modal.
  * *D1 Migrations:* `0031_sneak_consumer_compare.sql` and guarded legacy staging FK repair `0032_sneak_site_fk_compatibility.sql` applied remotely; `PRAGMA foreign_key_check` returned zero violations and no migrations remain pending.
  * *Authenticated Cross-Session API QA:* PASS — compare written with session A and recently viewed recorded with session A were both visible from independent session B; zero Compare activity events; isolated QA account/session data removed and verified absent.
  * *Automated Regression:* PASS — 149/149 tests across 10 suites.
  * *Live Endpoint Verification:* PASS — 12/12 checks for build health, auth boundaries, pilot bootstrap/search, bounded current-state summaries, assets/embed, and the live WordPress pilot.
  * *Anonymous Privacy Automation:* PASS — no anonymous recent upload route, no anonymous consumer request, site-scoped local keys/timestamps only, listing keys only for compare, and no full listing records in browser storage.
  * *Required C3A Browser QA:* **PENDING** — actual anonymous/authenticated desktop, tablet, mobile, cross-device, local-to-server merge, modal, tray, privacy/storage, and overflow verification could not run because the in-app browser runtime reported no connected browser.
  * *Build:* `2026.08.31.7.3c3a`.
* **Observability & Timing:**
  * Staff Setup Time: ~9 minutes
  * Member Implementation Time: ~2 minutes (HTML snippet paste in WordPress)
  * Staff Interventions Required: 13 (Identity reconciliation, Phase 7.1 model correction, Phase 7.2 product corrections, Phase 7.3A search parity, Phase 7.3B1 interactive map synchronization, Phase 7.3B2A radius search, Phase 7.3B2B polygon search, Phase 7.3C1A buyer auth, Phase 7.3C1B saved searches, Phase 7.3C1B.1 embed layout hotfix, Phase 7.3C2A email alert engine, Phase 7.3C2A.1 delivery hardening, Phase 7.3C2B client activity dashboard)
  * Support Questions Logged: 0
* **Final Status:** **ACTIVE PILOT (Phase 7.3C3A deployed; C2B/C3A required browser sign-off pending)**

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
