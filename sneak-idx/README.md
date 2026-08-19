# SNEAK IDX Platform

> Multi-Tenant Real Estate Search, Mapping, and Lead Capture Service powered by Cloudflare Workers & Cloudflare D1.

---

## 1. Overview & Architecture

**SNEAK** transforms existing single-tenant real estate search technology into a configurable, multi-tenant IDX software service. Instead of managing separate codebases or duplicating MLS data for every broker, agent, or office, SNEAK provides:

- **ONE MLS dataset:** Synchronized from Bridge Interactive into Cloudflare D1 (`community-idx`).
- **ONE backend engine:** `sneak-idx-worker` providing a versioned, secure REST API (`/idx/v1/...`).
- **ONE shared search client:** Configurable frontend (`sneak-idx/search/index.html`) and responsive embed loader (`sneak-idx/embed.js`).
- **MANY customer/site configurations:** Distinct branding, scopes, domains, widget settings, and lead destinations controlled by database records.

```
                          Bridge Interactive (MLS OData)
                                        │
                                        ▼
                            MLS Ingestion Sync (D1)
                                        │
                       ┌────────────────┴────────────────┐
                       ▼                                 ▼
             ListingsWorker.js                   SneakIDXWorker.js
           (/api/v2, /cached-*)                     (/idx/v1/*)
                       │                                 │
           ┌───────────┴───────────┐         ┌───────────┴───────────┐
           ▼                       ▼         ▼                       ▼
      Home Search             Open House  SNEAK Search UI        embed.js Loader
  (home-search/index.html)    (Live site) (sneak-idx/search)     (3rd-party Sites)
```

---

## 2. Existing Systems Reused

To ensure zero downtime and maximum performance, SNEAK builds upon the established infrastructure:

1. **Cloudflare D1 Database (`community-idx`):** Database ID `9940be0f-a4be-4835-81f7-09a52d42f7a9`.
2. **`listings` Table:** Reused directly as the primary search data source without duplicating listing rows.
3. **Bridge MLS Sync:** The existing scheduled ingestion in `ListingsWorker.js` continues syncing Active/Pending inventory.
4. **Untouched Legacy Production Routes:** `/api/v2/...`, `/api/cached-listings`, `/api/cached-openhouses`, and `home-search/index.html` remain completely intact.

---

## 3. New Files & Directory Structure

```
├── migrations/
│   ├── 0001_existing_schema_documentation.sql  # Documents legacy listings & open_houses tables
│   ├── 0002_sneak_accounts.sql                 # Tenant accounts, billing status, MLS IDs
│   ├── 0003_sneak_sites.sql                    # Site keys, names, scoping (market/agent/office)
│   ├── 0004_sneak_domains.sql                  # Domain whitelisting & CORS authorization
│   ├── 0005_sneak_branding.sql                 # Colors, logos, agent photos, contact info, links
│   ├── 0006_sneak_widget_configs.sql           # Config for search, grid, open houses
│   ├── 0007_sneak_leads.sql                    # Lead capture storage for property inquiries
│   ├── 0008_sneak_usage.sql                    # Daily tracking for searches, views, leads
│   ├── 0009_sneak_open_houses.sql              # Generalized rolling open house dataset
│   ├── 0010_sneak_indexes.sql                  # Search & relational performance indexes
│   ├── 0011_seed_demo_tenant.sql               # Default demo-ccor tenant seed
│   └── 0012_seed_sample_listings.sql           # Local test listings fixture
├── sneak-idx/
│   ├── search/
│   │   └── index.html                          # Multi-tenant search frontend application
│   ├── embed.js                                # Zero-dependency responsive widget embed loader
│   ├── test-embed.html                         # Interactive embed demonstration
│   └── README.md                               # This documentation
├── SneakIDXWorker.js                           # SNEAK IDX Cloudflare Worker API implementation
└── wrangler.sneak.toml                         # Dedicated Cloudflare Worker configuration
```

---

## 4. D1 Multi-Tenant Tables

| Table | Purpose |
| :--- | :--- |
| `sneak_accounts` | Manages tenant accounts, member IDs, plan types (`trial`, `standard`, `pro`), and status. |
| `sneak_sites` | Defines individual site instances, unique `site_key`, and MLS scoping (`market`, `agent`, `office`). |
| `sneak_domains` | Whitelists authorized domains per site for dynamic CORS and embed authorization. |
| `sneak_branding` | Stores display names, brokerages, logos, agent photos, primary/secondary colors, and custom nav links. |
| `sneak_widget_configs` | Stores settings for specific widgets (`search`, `listing_grid`, `open_houses`, `featured_listings`). |
| `sneak_leads` | Stores customer inquiries and lead submissions. |
| `sneak_usage` | Tracks daily metrics (`searches`, `listing_views`, `leads`) per site. |
| `sneak_open_houses` | Stores generalized rolling open houses (`today - 1 day` to `today + 30 days`). |

---

## 5. Security & Authorization Model

### A. Bridge Token Protection
- `BRIDGE_TOKEN` is **NEVER** exposed to client-side code, query strings, or GitHub repositories.
- Upstream requests to Bridge Interactive are strictly proxied and filtered server-side inside `SneakIDXWorker.js`.

### B. Site Key Resolution
- Every request to `/idx/v1/*` must supply a `?site={siteKey}` parameter.
- The server resolves account status, permissions, and scoping directly from D1. Frontend parameters cannot elevate MLS scope.

### C. Domain Whitelist & Dynamic CORS
- SNEAK does not use wildcard `Access-Control-Allow-Origin: *` in production for tenant endpoints.
- The server verifies the `Origin` / `Referer` against `sneak_domains` and dynamically returns the authorized origin.
- Localhost (`localhost`, `127.0.0.1`, `::1`) is automatically supported for local development.

### D. Server-Side Scoping
- If `scope_type = 'agent'`, the query automatically injects `WHERE ListAgentMlsId = ?`.
- If `scope_type = 'office'`, the query automatically injects `WHERE ListOfficeName = ?`.

---

## 6. SNEAK API Reference (`/idx/v1/...`)

### `GET /idx/v1/health`
Health check endpoint.
- **Response:** `{ "status": "ok", "service": "sneak-idx-worker", "version": "1.0.0", "timestamp": "..." }`

### `GET /idx/v1/config?site={siteKey}`
Retrieves tenant configuration, branding tokens, feature flags, and widget settings.

### `GET /idx/v1/search?site={siteKey}&...`
Parameterized D1 search with SQL filtering, indexing, and pagination.
- **Query Parameters:**
  - `city` (string or comma-separated list)
  - `minPrice` / `maxPrice` (numbers)
  - `beds` / `baths` (integers)
  - `propertyType` (`sale`, `rental`, `commercial`, `land`)
  - `propertySubType` (comma-separated subtypes)
  - `status` (`Active`, `Pending`, `Active Under Contract`, `Closed`)
  - `q` / `search` (searches address, city, MLS #, agent name)
  - `sort` (`dateDesc`, `dateAsc`, `priceDesc`, `priceAsc`)
  - `page` (integer, default 1)
  - `limit` (integer, default 24, max 100)
- **Response:**
  ```json
  {
    "data": [ ... ],
    "pagination": {
      "total": 142,
      "page": 1,
      "pageSize": 24,
      "totalPages": 6,
      "hasMore": true
    }
  }
  ```

### `GET /idx/v1/listing/:listingKey?site={siteKey}`
Fetches full property details. Reads D1 first and falls back to server-side Bridge lookup with edge caching (`s-maxage=600`).

### `GET /idx/v1/listing/:listingKey/media?site={siteKey}`
Fetches photo gallery for a listing with edge caching.

### `GET /idx/v1/agent/:mlsId/listings?site={siteKey}`
Returns active listings for a specific agent MLS ID.

### `GET /idx/v1/open-houses?site={siteKey}`
Returns upcoming open houses from `sneak_open_houses`.

### `POST /idx/v1/lead`
Submits a lead/inquiry into `sneak_leads`.
- **Payload:**
  ```json
  {
    "siteKey": "demo-ccor",
    "listingKey": "22400101",
    "name": "Jane Doe",
    "email": "jane@example.com",
    "phone": "(239) 555-1234",
    "message": "I would like to schedule a showing."
  }
  ```

---

## 7. Local Development

### 1. Apply D1 Migrations Locally
```bash
npx wrangler d1 migrations apply community-idx --local
```

### 2. Run the SNEAK Worker Locally
```bash
npx wrangler dev -c wrangler.sneak.toml --port 8788
```

### 3. Open the Search Application
Serve the workspace and open:
```
http://127.0.0.1:8788/sneak-idx/search/?site=demo-ccor
```
Or open [`sneak-idx/search/index.html?site=demo-ccor&api=http://127.0.0.1:8788`](http://127.0.0.1:8788/sneak-idx/search/?site=demo-ccor) in your browser.

---

## 8. Deployment to Cloudflare Staging / Production

### 1. Apply D1 Migrations to Remote Database
```bash
npx wrangler d1 migrations apply community-idx --remote
```

### 2. Set BRIDGE_TOKEN Secret on Worker (if detail lookup is desired)
```bash
npx wrangler secret put BRIDGE_TOKEN -c wrangler.sneak.toml
```

### 3. Deploy the SNEAK Worker
```bash
npx wrangler deploy -c wrangler.sneak.toml
```

---

## 9. Embedding SNEAK on 3rd-Party Websites

Place the following script tag on any client website:

```html
<!-- SNEAK IDX Search Widget -->
<script 
  src="https://your-sneak-domain.com/sneak-idx/embed.js" 
  data-site="YOUR_SITE_KEY" 
  data-widget="search" 
  data-height="850px">
</script>
```

### Supported Embed Attributes:
- `data-site`: (Required) Tenant site key (e.g. `demo-ccor`).
- `data-widget`: Widget type (`search`, `open_houses`).
- `data-height`: Container height (default `850px`).
- `data-target`: (Optional) CSS selector of target container element.
- `data-base-url`: (Optional) Host URL override.

---

## 10. Provisioning a New Tenant

To onboard a new agent or office, execute SQL against the D1 database:

```sql
-- 1. Create Account
INSERT INTO sneak_accounts (id, account_name, agent_mls_id, plan, status)
VALUES ('acc_smith_01', 'John Smith Realty', 'AGENT_MLS_123', 'pro', 'active');

-- 2. Create Site
INSERT INTO sneak_sites (id, account_id, site_key, site_name, scope_type, scope_value, status)
VALUES ('site_smith_01', 'acc_smith_01', 'john-smith-realty', 'John Smith - Coastal Homes', 'market', NULL, 'active');

-- 3. Whitelist Domains
INSERT INTO sneak_domains (id, site_id, domain, verified, status)
VALUES 
    ('dom_smith_1', 'site_smith_01', 'johnsmithrealty.com', 1, 'active'),
    ('dom_smith_2', 'site_smith_01', '*.johnsmithrealty.com', 1, 'active');

-- 4. Configure Branding & Colors
INSERT INTO sneak_branding (site_id, display_name, brokerage, primary_color, secondary_color, phone, email, website_url)
VALUES (
    'site_smith_01',
    'John Smith',
    'Premier Coastal Properties',
    '#0f172a',
    '#0284c7',
    '(239) 555-0188',
    'john@johnsmithrealty.com',
    'https://johnsmithrealty.com'
);

-- 5. Enable Widgets
INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json)
VALUES ('wc_smith_search', 'site_smith_01', 'search', 1, '{"pageSize":24}');
```

---

## 11. Known Limitations & Future Work

1. **Email / Webhook Lead Notifications:** Leads are reliably stored in `sneak_leads`. The next iteration should add an email notification dispatcher (e.g. Resend / Postmark / SendGrid Worker bindings) or CRM webhooks.
2. **User Accounts / Saved Searches:** Saved listings currently utilize tenant-scoped `localStorage`. Server-side buyer account management can be added.
3. **Advanced Polygon / Boundary Map Search:** Server-side bounding box / geo-distance queries can be added in future iterations.
