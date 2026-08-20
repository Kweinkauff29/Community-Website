# SNEAK IDX Platform — Phase 2.2: Pre-Staging Isolation & Auth Hardening

> High-performance, secure, multi-tenant real estate search, mapping, and lead capture engine powered by Cloudflare Workers & Cloudflare D1.

---

## 1. Overview & Architecture

**SNEAK** transforms MLS property data into a white-label, multi-tenant IDX software service:

- **Dedicated Isolated D1 Database:** SNEAK staging operates on `sneak-idx-staging` (and future production on `sneak-idx-production`), completely isolated from the legacy `community-idx` database used by the public Home Search.
- **Dedicated SNEAK Worker:** `sneak-idx-worker-staging` providing a versioned REST API (`/idx/v1/...`) with strict signed session tokens, fail-closed tenant scoping, and origin domain verification.
- **Unified Hosting Topology:** Static assets (`/search/`, `/embed.js`) and API endpoints served with `run_worker_first = true` to guarantee dynamic CSP `frame-ancestors` enforcement.
- **Tenant Configurations:** Distinct branding, scoping (market/agent/office), verified domains, and lead destinations controlled strictly via database records.

```
                           Bridge Interactive (MLS OData)
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
      Legacy Sync (County/City Filter)              SNEAK Sync (Future 3-Part Hydration)
                   │                                           │
                   ▼                                           ▼
             listings (D1)                              sneak_listings (D1)
             (community-idx)                            (sneak-idx-staging)
                   │                                           │
                   ▼                                           ▼
           ListingsWorker.js                       SneakIDXWorker.js (Staging)
         (/api/v2, /cached-*)                             (/idx/v1/*)
                   │                                           │
         ┌─────────┴─────────┐                       ┌─────────┴─────────┐
         ▼                   ▼                       ▼                   ▼
    Home Search          Open House            SNEAK Search UI       embed.js Loader
(home-search/index)     (Live Page)          (/search/?site=...)   (3rd-party Sites)
```

---

## 2. Embed Bootstrap & Signed Session Architecture

### The Origin Dilemma & The Solution
When an iframe (`idx.sneak.../search/?site=...`) executes inside a member's webpage (`exampleagent.com`), subsequent API requests originating from JavaScript inside the iframe will have the SNEAK frontend's origin (`idx.sneak...`), **not** `exampleagent.com`.

To securely validate the embedding member's domain against `sneak_domains`, SNEAK uses a **Bootstrap Session Architecture**:

```
MEMBER WEBPAGE (exampleagent.com)
      │
      ▼
1. embed.js executes on member DOM
      │
      ▼
2. GET /idx/v1/bootstrap?site=SITE_KEY
   (Browser sends Origin: https://exampleagent.com)
      │
      ▼
3. Worker validates Origin against sneak_domains (verified = 1 AND status = 'active')
   - Requires Origin header in staging and production (no missing-origin bypass)
   - Checks exact domain or configured wildcard subdomain (*.example.com)
   - Generates HMAC-SHA256 signed session token (lifetime: 20 min)
   - Returns { session: "<signed_token>", expiresIn: 1200 }
      │
      ▼
4. embed.js constructs iframe:
   https://SNEAK_HOST/search/?site=SITE_KEY&session=<signed_token>&embed=true
      │
      ▼
5. Search UI extracts session from URL, scrubs URL via history.replaceState(),
   and attaches Authorization: Bearer <session> to all tenant API requests.
      │
      ▼
6. API verifies HMAC-SHA256 signature, expiration, and matching siteKey.
```

If bootstrap fails (HTTP 403), `embed.js` does **not** create the iframe and instead renders a neutral message: *"SNEAK IDX is not authorized for this website."*

---

## 3. Web Crypto HMAC-SHA256 Session Signing & Security Model

- **Secret Variable:** `SNEAK_SIGNING_SECRET` (strictly declared as a required secret in `wrangler.sneak.toml`).
- **No Hardcoded Fallback Secret:** If `SNEAK_SIGNING_SECRET` is missing in the environment, the worker fails closed and returns HTTP 500 (`ConfigurationError`).
- **Token Format:** `base64url(header).base64url(payload).base64url(signature)`
- **Mandatory Sessions Across All Environments:** All tenant data endpoints (`/search`, `/map`, `/listing/*`, `/agent/*`, `/open-houses`, `/lead`) require a signed session token in `staging`, `production`, and `development`. Unauthenticated calls receive HTTP `401 SessionRequired`.
- **CORS Policies:**
  - `OPTIONS /idx/v1/bootstrap`: Cross-origin preflight allowed only for verified domains (`Access-Control-Allow-Origin: <origin>`).
  - Tenant data APIs: Do not return permissive `Access-Control-Allow-Origin: *` to arbitrary cross-origin callers.

---

## 4. Standalone SNEAK Database & Clean Migrations

SNEAK migrations in `migrations/` are standalone and contain **only** SNEAK tables and indexes:

1. `0001_sneak_accounts.sql` — Tenant accounts
2. `0002_sneak_sites.sql` — Tenant site instances and scoping (market/agent/office)
3. `0003_sneak_domains.sql` — Domain whitelist and verification
4. `0004_sneak_branding.sql` — Custom branding, logos, colors, contact details
5. `0005_sneak_widget_configs.sql` — Per-widget settings
6. `0006_sneak_leads.sql` — Inbound lead capture
7. `0007_sneak_usage.sql` — Daily usage tracking
8. `0008_sneak_open_houses.sql` — SNEAK-specific open houses
9. `0009_sneak_listings.sql` — Full statewide/regional SNEAK listing dataset
10. `0010_sneak_indexes.sql` — Performance indexes for tenant and scoping lookups
11. `0011_sneak_sync_state.sql` — Sync cursors, hydration, and reconciliation state

**Zero Legacy Impact:** Legacy tables (`listings`, `open_houses`) and `community-idx` are completely untouched.

---

## 5. Fail-Closed Tenant Scoping Engine

The tenant scoping engine (`buildTenantListingScope`) guarantees that malformed or incomplete tenant configurations **fail closed** (evaluating to `1=0` and rejecting with HTTP 403), never falling back to market-wide access:

| Scope Type | Condition | Result |
| :--- | :--- | :--- |
| `market` | Always | `1=1` (Market-wide inventory) |
| `agent` | `scope_value` present | `ListAgentMlsId = ?` |
| `agent` | `scope_value` missing | **DENY** (`1=0`, HTTP 403) |
| `office` | `scope_value` present | `(ListOfficeMlsId = ? OR ListOfficeKey = ?)` |
| `office` | `scope_value` missing | **DENY** (`1=0`, HTTP 403) |
| Unknown | Any | **DENY** (`1=0`, HTTP 403) |

---

## 6. Bridge Feed Status & Target Sync Architecture

> [!IMPORTANT]
> **UNVERIFIED UNTIL AUTHENTICATED BRIDGE PROBE:** Exact record counts, regional coverage, feed field availability, and sync durations remain unverified until an authenticated probe is run with a live `BRIDGE_TOKEN`.

### Bridge Probe Utility (`scripts/probe-bridge.mjs`)
A local-only script is available to test Bridge feed compatibility safely:
```bash
# Provide BRIDGE_TOKEN in .dev.vars (gitignored) or environment, then run:
node scripts/probe-bridge.mjs
```
If `BRIDGE_TOKEN` is missing, it cleanly outputs `BRIDGE PROBE NOT RUN — TOKEN UNAVAILABLE` without erroring or faking results.

### Target Data Sync Strategy
For real MLS ingestion, the production sync strategy consists of:
1. **Initial Hydration:** One-time controlled batch import of eligible listings via an administrative/chunked script.
2. **Incremental Sync:** Regular query using `ModificationTimestamp gt <last_successful_sync>` to upsert only changed records.
3. **Periodic Reconciliation:** Daily/weekly sync fetching active `ListingKey` values to identify and prune/deactivate stale listings.

*Note: For the initial staging embed verification, scheduled crons are disabled in `wrangler.sneak.toml` and staging operates on demo listings (`seeds/staging-demo-listings.sql`).*

---

## 7. Known Limitations & Roadmap

- **Session Lifetime (20 Minutes):** Current session tokens expire after 20 minutes (1200 seconds). If expired, requests receive HTTP `401 InvalidSession`. In the next phase, the iframe will emit `SNEAK_SESSION_EXPIRED` via `window.postMessage` to `embed.js` to trigger a seamless background re-bootstrap without page reload.
- **Staging D1 Provisioning:** Remote staging requires creating `sneak-idx-staging` D1 database and inserting its generated UUID into `wrangler.sneak.toml`.

---

## 8. SNEAK API Reference (`/idx/v1/...`)

| Endpoint | Method | Auth Required | Description |
| :--- | :--- | :--- | :--- |
| `/idx/v1/health` | GET | No | Public worker health status and environment. |
| `/idx/v1/bootstrap` | GET | Origin Check | Validates member Origin from embed.js and returns signed session. |
| `/idx/v1/config` | GET | Session Token | Tenant branding, colors, scope, and widget settings. |
| `/idx/v1/search` | GET | Session Token | Paginated search (24/page) from `sneak_listings`. |
| `/idx/v1/map` | GET | Session Token | Lightweight markers (up to 1,000) with viewport bbox. |
| `/idx/v1/listing/:key` | GET | Session Token | Full listing details. Scoped to tenant. Edge-cached. |
| `/idx/v1/listing/:key/media` | GET | Session Token | Photo gallery. Scoped to tenant. Edge-cached. |
| `/idx/v1/agent/:mlsId/listings` | GET | Session Token | Active listings for an agent within authorized tenant scope. |
| `/idx/v1/open-houses` | GET | Session Token | Upcoming open houses with filters (`city`, `dateFrom`, `dateTo`). |
| `/idx/v1/lead` | POST | Session Token | Submits lead inquiry. Validates listing existence within scope. |

---

## 9. Local & Staging Development Guide

### 1. Apply D1 Migrations Locally
```bash
npx wrangler d1 migrations apply sneak-idx-staging --local -c wrangler.sneak.toml
```

### 2. Seed Staging Demo Tenant & Local Test Listings
```bash
# Provision demo-ccor tenant
npx wrangler d1 execute sneak-idx-staging --local --file=seeds/staging-demo-tenant.sql -c wrangler.sneak.toml

# Seed sample demo listings
npx wrangler d1 execute sneak-idx-staging --local --file=seeds/staging-demo-listings.sql -c wrangler.sneak.toml
```

### 3. Run the SNEAK Worker Locally
```bash
npx wrangler dev -c wrangler.sneak.toml --port 8788
```

### 4. Run Automated Unit Tests
```bash
node --test test/sneak-idx-staging.test.mjs
```
