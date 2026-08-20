# SNEAK IDX Platform — Phase 2.1: Hardened Pre-Staging Foundation

> High-performance, secure, multi-tenant real estate search, mapping, and lead capture engine powered by Cloudflare Workers & Cloudflare D1.

---

## 1. Overview & Architecture

**SNEAK** transforms MLS property data into a white-label, multi-tenant IDX software service:

- **ONE MLS dataset:** Synchronized statewide/regional feed stored in `sneak_listings` in Cloudflare D1 (`community-idx`).
- **ONE backend engine:** `sneak-idx-worker` providing a versioned REST API (`/idx/v1/...`) with signed session tokens, fail-closed tenant scoping, and origin domain verification.
- **ONE unified hosting topology:** Static assets (`/search/`, `/embed.js`) and API endpoints served with `run_worker_first = true` to guarantee dynamic CSP enforcement.
- **MANY customer configurations:** Distinct branding, scoping (market/agent/office), verified domains, and lead destinations controlled strictly by database records.

```
                           Bridge Interactive (MLS OData)
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
      Legacy Sync (County/City Filter)              SNEAK Sync (Full IDX Feed)
                   │                                           │
                   ▼                                           ▼
             listings (D1)                              sneak_listings (D1)
                   │                                           │
                   ▼                                           ▼
           ListingsWorker.js                           SneakIDXWorker.js
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

## 3. Web Crypto HMAC-SHA256 Session Signing

- **Secret Variable:** `SNEAK_SIGNING_SECRET` (distinct from `BRIDGE_TOKEN`).
- **Token Format:** `base64url(header).base64url(payload).base64url(signature)`
- **Payload Schema:**
  ```json
  {
    "siteKey": "demo-ccor",
    "siteId": "site_demo_01",
    "origin": "exampleagent.com",
    "iat": 1724112000,
    "exp": 1724113200
  }
  ```
- **Configuration (Remote):**
  ```bash
  npx wrangler secret put SNEAK_SIGNING_SECRET -c wrangler.sneak.toml
  ```

---

## 4. Fail-Closed Tenant Scoping Engine

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

## 5. Event-Driven Scheduled Cron Architecture

`wrangler.sneak.toml` configures two independent cron triggers:
```toml
[triggers]
crons = ["0 */2 * * *", "*/15 * * * *"]
```

The Worker `scheduled(event, env, ctx)` dispatches tasks using `event.cron`:
- `"0 */2 * * *"` → Executes `syncSneakListings(env)` (2-hour full listing sync)
- `"*/15 * * * *"` → Executes `syncSneakOpenHouses(env)` (15-minute open house sync)
- Independent `try/catch` error boundaries prevent failure of one sync from affecting another.

---

## 6. Static Asset Routing & Strict Dynamic CSP

With `run_worker_first = true`, the SNEAK Worker intercepts requests to `/search`, `/search/`, and `/search/index.html`:
- Dynamically queries `sneak_domains` (`status = 'active'` AND `verified = 1`).
- Injects a strict `Content-Security-Policy: frame-ancestors 'self' <allowed_origins>;` header.
- **Strict HTTPS enforcement:** Verified domains use `https://` (HTTP allowed only for `localhost`/`127.0.0.1`).
- **No auto-expansion:** Does not automatically add `www.` unless explicitly configured in `sneak_domains` or matching a wildcard (`*.domain.com`).

---

## 7. Search & Map Filter Consistency

Both `/idx/v1/search` and `/idx/v1/map` use the unified `buildCommonListingFilters(params, site)` helper. Subtype expansions (e.g. `Condominium` → `Condominium`, `High Rise (8+)`, `Mid Rise (4-7)`, `Low Rise (1-3)`) and city filtering produce identical query results across both the paginated card grid and the lightweight map pins.

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
```powershell
npx wrangler d1 migrations apply community-idx --local -c wrangler.sneak.toml
```

### 2. Seed Staging Demo Tenant & Local Test Listings
```powershell
# Provision demo-ccor tenant
npx wrangler d1 execute community-idx --local --file=seeds/staging-demo-tenant.sql -c wrangler.sneak.toml

# Seed sample listings (for offline UI testing only)
npx wrangler d1 execute community-idx --local --file=seeds/local-demo-listings.sql -c wrangler.sneak.toml
```

### 3. Run the SNEAK Worker Locally
```powershell
npx wrangler dev -c wrangler.sneak.toml --port 8788
```

### 4. Test SNEAK Search Application Locally
Open in browser:
```
http://127.0.0.1:8788/search/?site=demo-ccor
```
Or test the full bootstrap embed flow:
```
http://127.0.0.1:8788/test-embed.html
```
