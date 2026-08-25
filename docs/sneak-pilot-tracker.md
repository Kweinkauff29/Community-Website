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
- [x] **MLS Scope Validation:** Member MLS ID `633942` (Ursula S Weinkauff) validated in D1 (2 active listings); foreign listing `00030a06cd40eb28062f68e614cd9d32` strictly blocked with `403 ScopeMismatch`.
- [x] **Invitation Gate:** Verified live Mailjet authentication previously satisfied & account normalized.
- [x] **Member Portal Gate:** Member successfully authenticated and accessed portal dashboard for `Ursula Weinkauff — SNEAK Pilot`.
- [x] **Branding & Config Gate:** Display name (`Ursula Weinkauff`), brokerage (`Local Real Estate LLC`), colors (`#0f2942`, `#2b6cb0`), and email configured.
- [x] **Domain / Embed Gate:** PASS — Installed and executing on `https://coconutcoastrealtors.org/idx-test/` with authorized Origin `https://coconutcoastrealtors.org`.
- [x] **Live HTTPS Gate:** PASS — Loads over HTTPS 200 OK with zero mixed content errors.
- [x] **Live IDX Functionality Gate:** PASS — Bootstrap, signed session token, search scoped to `633942`, property detail, photos, and open houses verified live.
- [x] **Lead Ingestion Gate:** PASS — Live frontend lead `lead_mt8ob2jq_km1b1` ingested and confirmed in D1 / Member Portal.
- [x] **Cross-Device Gate:** PASS — Responsive desktop and mobile layout verified with zero horizontal overflow.
- [x] **Staff Observation Gate:** Onboarding duration, live verification metrics, and intervention count logged.

---

## 3. Detailed Participant Records & Logs

### PILOT #1: IDX Embed Only (Existing Member Website)
* **Goal:** Prove SNEAK IDX container & embed script operate cleanly inside a third-party host (WordPress Elementor/Beaver Builder).
* **Participant Details:**
  * MLS Participant / REALTOR: Ursula Weinkauff
  * MLS Agent ID / IDX Scope: `633942`
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
  * Plan: `standard`
* **Operator Authorization Note:**
  * "Operator explicitly approved use of Ursula Weinkauff's verified agent scope 633942 for PILOT-01 while Kevin Weinkauff operates the controlled pilot using kmwcollegeapps@gmail.com."
* **Operational Milestones:**
  * Account & Site Provisioned Date: 2026-08-24 20:33 UTC
  * Invitation Gate Satisfied Date: 2026-08-24 16:03 UTC (Pre-verified real Mailjet auth normalized)
  * Embed Snippet Installed on Page: 2026-08-25 12:58 UTC
  * Live On-Site Verification Date: 2026-08-25 13:00 UTC (100% PASS across all 12 launch gates)
* **Live Validation Evidence:**
  * Live Public URL: `https://coconutcoastrealtors.org/idx-test/` (HTTP 200 OK)
  * Live Bootstrap & Session: PASS (`Origin: https://coconutcoastrealtors.org`)
  * Scoped Listings: `32b7c013c8d1f5df2c05fb412ed9edba` ($27,500), `38ebadbb74aabba3938a3bea4d5007a4` ($1,150,000)
  * Foreign Listing Denial: `00030a06cd40eb28062f68e614cd9d32` $\rightarrow$ HTTP 403 `ScopeMismatch`
  * Live Frontend Lead ID: `lead_mt8ob2jq_km1b1` (Confirmed in D1 and Member Portal)
  * Desktop / Mobile UX: PASS (Responsive container, touch-friendly UI)
* **Observability & Timing:**
  * Staff Setup Time: ~9 minutes
  * Member Implementation Time: ~2 minutes (HTML snippet paste in WordPress)
  * Staff Interventions Required: 2 (Identity reconciliation & selection)
  * Support Questions Logged: 0
* **Final Status:** **LAUNCHED**

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
