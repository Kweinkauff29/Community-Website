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
* **Phase 7.3C1B.1 Embed Sizing & Layout Hotfix:**
  * *Deterministic Parent-Controlled Responsive Sizing:* Replaced static clamp / legacy `data-height` locking with `getRecommendedSearchHeight()` in `embed.js` and debounced parent window resize listener, dynamically adapting iframe height across Desktop (~860px–960px), Tablet (~760px), and Mobile (~680px–740px).
  * *Nested Disclaimer Architecture:* Moved `.footer-disclaimer` inside `.listings-section` beneath `#paginationBar`, freeing 180px of vertical space and ensuring a 620px+ usable Map/List workspace.
  * *CSS Syntax & Mobile Repair:* Resolved unclosed media query syntax error at line 1979 and ensured mobile list view uses internal container scrolling.
* **Live Validation Evidence:**
  * Live Public URL: `https://coconutcoastrealtors.org/idx-test/` (HTTP 200 OK)
  * Live Static UI Build: PASS — `data-ui-build="2026.08.28.7.3c1b1"`, `CCOR_IDX_UI_BUILD = '2026.08.28.7.3c1b1'`, `embed.js &v=2026.08.28.7.3c1b1`
  * Live Consumer Worker Health: PASS — `https://sneak-idx-consumer-staging.bonitaspringsrealtors.workers.dev/api/consumer/version` (200 OK, `sneak-consumer-worker`, `2026.08.28.7.3c1b`, `healthy`)
  * Live Saved Searches REST API: PASS — OPTIONS, GET (401 unauthenticated), POST, PATCH, DELETE verified
  * Live Polygon & Radius Search: PASS — Polygon Mode and Radius Mode filter accurately across residential, commercial, and land
  * Live Marker Schema: PASS — Complete context-aware fields returned from D1 with address suppression enforced
  * Automated Regression Suite: PASS — 103/103 tests passing (`node --test test/*.test.mjs`)
  * Live Automated Verification: PASS — 31/31 live verification assertions passing (`node scripts/verify-phase73c1b1-live.mjs`)
  * Live Browser Visual QA: PASS — Verified across 1440×900, 1920×1080, 1024×768, and 390×844 with zero footer overlap, internal listings scrolling, no outer scrollbars, and seamless map/list responsiveness.
* **Observability & Timing:**
  * Staff Setup Time: ~9 minutes
  * Member Implementation Time: ~2 minutes (HTML snippet paste in WordPress)
  * Staff Interventions Required: 10 (Identity reconciliation, Phase 7.1 model correction, Phase 7.2 product corrections, Phase 7.3A search parity, Phase 7.3B1 interactive map synchronization, Phase 7.3B2A radius search, Phase 7.3B2B polygon search, Phase 7.3C1A buyer auth, Phase 7.3C1B saved searches, Phase 7.3C1B.1 embed layout hotfix)
  * Support Questions Logged: 0
* **Final Status:** **ACTIVE PILOT (CCOR IDX Plug-in Phase 7.3C1B.1 Deployed & Verified)**


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
