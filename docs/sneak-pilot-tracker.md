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
| **PILOT-01** | Kevin Weinkauff | `B3360322` / `633942` | `kmwcollegeapps@gmail.com` | `https://coconutcoastrealtors.org` | `acc_1787583729221_cv3ma` | `site_1787583729221_rzxfa` | `standard` | IDX Embed Only | N/A / embed | BLOCKED — MLS IDENTITY MISMATCH |
| **PILOT-02** | *Pending Operator Input* | *Pending* | *Pending* | *Real Custom Domain* | *Pending* | *Pending* | `pro` | Turnkey Website | `essential` | Planned |
| **PILOT-03** | *Pending Operator Input* | *Pending* | *Pending* | *Real Custom Domain* | *Pending* | *Pending* | `pro` | Turnkey Website | `coastal` | Planned |
| **PILOT-04** *(Opt)* | *Pending Operator Input* | *Pending Office ID* | *Pending* | *Real Custom Domain* | *Pending* | *Pending* | `brokerage` | Brokerage Website | `brokerage` | Deferred |

---

## 2. Universal Launch Checklist (Per Participant)

Every participant must complete all verification gates before being marked **Launched**:

- [x] **GrowthZone Gate:** OPERATOR-CONFIRMED INTERNAL PILOT (Internal staff/operator participant).
- [x] **SNEAK Entitlement Gate:** Entitlement active (`source = 'growthzone'`, `plan = 'standard'`, `external_reference = 'PILOT-01-INTERNAL'`).
- [ ] **MLS Scope Validation:** IDENTITY MISMATCH — Operator provided `B3360322` (0 listings in D1); `633942` belongs to `Ursula S Weinkauff` in D1 ground truth.
- [x] **Invitation Gate:** Verified live Mailjet authentication previously satisfied & account normalized.
- [x] **Member Portal Gate:** Member successfully authenticated and accessed portal dashboard for `Kevin Weinkauff — SNEAK Pilot`.
- [x] **Branding & Config Gate:** Display name (`Kevin Weinkauff`), brokerage (`Coconut Coast Realtors`), colors (`#0f2942`, `#2b6cb0`), and email configured.
- [ ] **Domain / Embed Gate:** PENDING LIVE EMBED — Authorized Origin `https://coconutcoastrealtors.org` verified in D1.
- [ ] **Live HTTPS Gate:** PENDING LIVE EMBED.
- [ ] **Live IDX Functionality Gate:** PENDING LIVE EMBED — (Backend/API Prevalidation: `PASS`).
- [ ] **Lead Ingestion Gate:** PENDING LIVE FRONTEND + MEMBER PORTAL VERIFICATION — (Backend Ingestion: `PASS`).
- [ ] **Cross-Device Gate:** PENDING LIVE EMBED.
- [x] **Staff Observation Gate:** Onboarding duration, identity reconciliation findings, and intervention count logged.

---

## 3. Detailed Participant Records & Logs

### PILOT #1: IDX Embed Only (Existing Member Website)
* **Goal:** Prove SNEAK IDX container & embed script operate cleanly inside a third-party host (WordPress Elementor/Beaver Builder).
* **Participant Details:**
  * Member Name: Kevin Weinkauff (Internal Operator Pilot)
  * Operator-Supplied MLS ID: `B3360322`
  * Bridge/D1 ListAgentMlsId for 633942: `633942` (`Ursula S Weinkauff`, Local Real Estate LLC)
  * Email Address: `kmwcollegeapps@gmail.com`
  * Existing Member Website URL: `https://coconutcoastrealtors.org`
  * Authorized Embed Origin: `https://coconutcoastrealtors.org`
  * Target Test Page: `https://coconutcoastrealtors.org/idx-test/`
  * Site Key: `kevin-weinkauff-pilot`
* **Operational Milestones:**
  * Account & Site Provisioned Date: 2026-08-24 20:09 UTC
  * Invitation Gate Satisfied Date: 2026-08-24 16:03 UTC (Pre-verified real Mailjet auth normalized)
  * Backend API Prevalidation Date: 2026-08-24 20:09 UTC (All endpoints 100% PASS)
  * Embed Snippet Generated Date: 2026-08-24 20:10 UTC
  * Live Origin Verification Date: 2026-08-24 20:09 UTC
* **Identity Reconciliation Findings:**
  * Query for `ListAgentMlsId = 'B3360322'`: 0 eligible listings in D1.
  * Query for `ListAgentMlsId = '633942'`: 2 active listings in D1, but associated with `Ursula S Weinkauff`.
  * Search by Name (`Kevin Weinkauff`): 0 listings in D1 across all 37,160 listing records.
  * Decision: Per pilot governance rules (Case B), installation is blocked until Kevin's actual MLS ID or authorized representative scope is confirmed.
* **Observability & Timing:**
  * Staff Setup Time: ~8 minutes
  * Member Implementation Time: Blocked on MLS identity clarification
  * Staff Interventions Required: 2 (Identity reconciliation audit)
  * Support Questions Logged: 0
* **Scope Verification Evidence:**
  * Backend Verified Listings (Scope 633942): `32b7c013c8d1f5df2c05fb412ed9edba` ($27,500, Lehigh Acres), `38ebadbb74aabba3938a3bea4d5007a4` ($1,150,000, Naples)
  * Backend Verified Foreign Listing (HTTP 403 Confirmed): `00030a06cd40eb28062f68e614cd9d32` (Agent: `P3401594`)
* **Issues / Observations:**
  * Operator provided `B3360322` which has 0 listings in D1 ground truth.
  * `633942` belongs to `Ursula S Weinkauff` in D1 ground truth, not `Kevin Weinkauff`.
  * Status set to `BLOCKED — MLS IDENTITY MISMATCH` pending operator direction.

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
