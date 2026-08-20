# SNEAK IDX Platform — Phase 2: Production Hardened Foundation

> High-performance, secure, multi-tenant real estate search, mapping, and lead capture software foundation powered by Cloudflare Workers & Cloudflare D1.

---

## 1. Overview & Architecture

**SNEAK** transforms MLS property data into a white-label, multi-tenant IDX software service. Rather than deploying bespoke applications or duplicating MLS databases per customer, SNEAK operates as a single, multi-tenant engine:

- **ONE MLS dataset:** Synchronized statewide/regional feed stored in `sneak_listings` in Cloudflare D1 (`community-idx`).
- **ONE backend engine:** `sneak-idx-worker` providing a versioned, secure REST API (`/idx/v1/...`) with tenant scoping, rate limiting, and CORS security.
- **ONE unified hosting topology:** Static frontend assets (`/search/`, `/embed.js`) and API endpoints served from a single SNEAK host (e.g. `idx.sneakidx.com` or `sneak-idx-worker.<subdomain>.workers.dev`).
- **MANY customer/site configurations:** Distinct branding, scoping (market/agent/office), verified domains, widget settings, and lead destinations controlled strictly by database records.

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

## 2. Legacy System Isolation

To guarantee zero regression for existing live production apps:
- **Legacy `listings` and `open_houses` tables:** Remain completely intact and continue serving `home-search/index.html` and `ListingsWorker.js`.
- **Dedicated `sneak_listings` and `sneak_open_houses` tables:** Dedicated exclusively to SNEAK multi-tenant queries.
- **No Shared Sync Code:** The SNEAK worker sync engine runs independently on its own scheduled triggers.

---

## 3. Database Schema & Migration Architecture

Production D1 migrations in `migrations/` contain **schema definitions and indexes ONLY**. Seed fixtures with fake data or demo users are strictly decoupled and stored in `seeds/`.

```
migrations/
├── 0001_existing_schema_documentation.sql  # Documents legacy listings & open_houses tables
├── 0002_sneak_accounts.sql                 # Tenant accounts, member IDs, status, plans
├── 0003_sneak_sites.sql                    # Site instances, site keys, scope types & values
├── 0004_sneak_domains.sql                  # Domain whitelists, verification status
├── 0005_sneak_branding.sql                 # Colors, logos, agent photos, contact info
├── 0006_sneak_widget_configs.sql           # Config for search, grid, open houses
├── 0007_sneak_leads.sql                    # Lead capture storage for property inquiries
├── 0008_sneak_usage.sql                    # Daily tracking for searches, views, leads
├── 0009_sneak_open_houses.sql              # Generalized rolling open house dataset
├── 0010_sneak_indexes.sql                  # Relational and lookup performance indexes
└── 0011_sneak_listings.sql                 # Dedicated SNEAK MLS listing dataset & indexes

seeds/
├── staging-demo-tenant.sql                 # Provisions demo-ccor account & site for staging
└── local-demo-listings.sql                 # Sample listing fixtures for local offline UI testing
```

### Key D1 Tables:
| Table | Description |
| :--- | :--- |
| `sneak_listings` | Complete statewide/regional MLS listings with durable agent & office identifiers. |
| `sneak_open_houses` | Generalized rolling open house schedule (today - 1 day to today + 30 days). |
| `sneak_accounts` | Tenant accounts, membership, plan tiers (`trial`, `standard`, `pro`, `brokerage`). |
| `sneak_sites` | Individual site instances, unique `site_key`, scoping (`market`, `agent`, `office`). |
| `sneak_domains` | Authorized domains per site; requires `status = 'active'` AND `verified = 1`. |
| `sneak_branding` | Display names, brokerages, logos, photos, colors (`primary_color`, `secondary_color`). |
| `sneak_widget_configs`| Configurable widget properties (`search`, `listing_grid`, `open_houses`). |
| `sneak_leads` | Inquiries submitted via `/idx/v1/lead` mapped to `site_id` and optional `listing_key`. |
| `sneak_usage` | Daily request metrics per site (`searches`, `listing_views`, `leads`). |

---

## 4. `sneak_listings` Schema & Bridge Synchronization

### Durable MLS Identifiers
- **Agent Scope:** `ListAgentMlsId` (RESO standard agent MLS ID) and `ListAgentKey`.
- **Office Scope:** `ListOfficeMlsId` (RESO standard listing office identifier) and `ListOfficeKey`. Office names and phone numbers are preserved for display but are **never** used as scope authorization keys.

### Synchronization Logic (`syncSneakListings`)
1. Ingests all active inventory:
   ```odata
   OriginatingSystemKey eq 'bsaor' and StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')
   ```
2. Ingests all standard RESO fields including: `ListingKey`, `ListingId`, `ListPrice`, `OriginalListPrice`, `UnparsedAddress`, `StreetNumber`, `StreetName`, `UnitNumber`, `City`, `StateOrProvince`, `PostalCode`, `CountyOrParish`, `BedroomsTotal`, `BathroomsTotalInteger`, `BathroomsFull`, `BathroomsHalf`, `LivingArea`, `StandardStatus`, `PropertyType`, `PropertySubType`, `ListingContractDate`, `ModificationTimestamp`, `StatusChangeTimestamp`, `YearBuilt`, `LotSizeAcres`, `Latitude`, `Longitude`, `PrimaryPhoto`, `PublicRemarks`, `SubdivisionName`, `ListAgentMlsId`, `ListAgentFullName`, `ListAgentEmail`, `ListAgentDirectPhone`, `ListOfficeMlsId`, `ListOfficeName`, `ListOfficePhone`, `OriginatingSystemKey`, `OriginatingSystemName`.
3. Follows OData pagination (`@odata.nextLink`) using server-side batch D1 statements.
4. **Safe Stale Cleanup:** Collects all fetched `ListingKey` values in memory. Only after the **entire pagination succeeds without error** does it prune records no longer active. If Bridge returns an error or network drop occurs mid-sync, all existing records are safely retained.

---

## 5. Security & Authorization Model

### Unified Tenant Scoping Engine
Every endpoint that queries or retrieves listing information enforces `buildTenantListingScope(site)`:
- **Market Scope (`scope_type = 'market'`):** Full authorized regional inventory. Query parameters (`agentMlsId`, `officeMlsId`) can narrow results, but cannot exceed MLS eligibility.
- **Agent Scope (`scope_type = 'agent'`):** Automatically injects `ListAgentMlsId = ?` using `site.scope_value`. Client-side query parameters cannot override or broaden this scope.
- **Office Scope (`scope_type = 'office'`):** Automatically injects `(ListOfficeMlsId = ? OR ListOfficeKey = ?)` using `site.scope_value`.

### Route-Level Enforcement
- `GET /idx/v1/search`: Filtered by tenant scope.
- `GET /idx/v1/map`: Filtered by tenant scope.
- `GET /idx/v1/listing/:listingKey`: Checks `sneak_listings` against tenant scope **before** returning data or querying Bridge. If outside tenant scope, returns `404 Not Found`.
- `GET /idx/v1/listing/:listingKey/media`: Verifies listing scope **before** requesting upstream Bridge photos.
- `GET /idx/v1/agent/:mlsId/listings`: Rejects mismatched agent requests with `403 Forbidden` for agent-scoped sites.
- `GET /idx/v1/open-houses`: Joins `sneak_open_houses` with scoped `sneak_listings`.

### Domain Verification & No-Origin Policy
1. `sneak_domains` requires `status = 'active'` AND `verified = 1`.
2. Browser/widget requests with missing `Origin` and `Referer` headers are rejected with `403 Forbidden` (`OriginRequired`) in production. Localhost (`localhost`, `127.0.0.1`, `::1`, `*.local`) is permitted for development.
3. **CORS Preflight (`OPTIONS`):** Preflight requests evaluate `?site=` to verify the requesting Origin against verified domains before returning `Access-Control-Allow-Origin`.

---

## 6. SNEAK API Reference (`/idx/v1/...`)

All tenant API endpoints require `?site={siteKey}`.

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/idx/v1/health` | GET | Public service health status & version (no site required). |
| `/idx/v1/config` | GET | Tenant branding, primary/secondary colors, scope, and widget settings. |
| `/idx/v1/search` | GET | Parameterized, paginated listing search (default 24/page, max 100). |
| `/idx/v1/map` | GET | Lightweight marker endpoint (up to 1,000 markers) with bounding box support (`north`, `south`, `east`, `west`). |
| `/idx/v1/listing/:key` | GET | Full listing details. Scoped to tenant. Edge-cached. |
| `/idx/v1/listing/:key/media` | GET | High-resolution photo gallery. Scoped to tenant. Edge-cached. |
| `/idx/v1/agent/:mlsId/listings` | GET | Active listings for an agent within authorized tenant scope. |
| `/idx/v1/open-houses` | GET | Upcoming open houses with filters (`city`, `dateFrom`, `dateTo`, `agent`, `office`). |
| `/idx/v1/lead` | POST | Submits lead inquiry to `sneak_leads`. Validates payload. |

---

## 7. Static Hosting & Embed Architecture

### Normalized SNEAK Public URLs
- Embed loader: `https://SNEAK_HOST/embed.js`
- Search application: `https://SNEAK_HOST/search/?site=SITE_KEY`
- API base: `https://SNEAK_HOST/idx/v1/...`

### Zero-Dependency Embed Loader (`embed.js`)
Place on any client website:

```html
<!-- SNEAK IDX Responsive Search Widget -->
<script 
  src="https://your-sneak-host.com/embed.js" 
  data-site="demo-ccor" 
  data-widget="search" 
  data-height="850px">
</script>
```

### Frame-Ancestors CSP
When `/search/` is requested, `SneakIDXWorker.js` dynamically checks the site's verified domains and attaches:
```http
Content-Security-Policy: frame-ancestors 'self' https://authorized-domain.com https://www.authorized-domain.com;
```

---

## 8. Local & Staging Development Guide

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
Or test embed:
```
http://127.0.0.1:8788/test-embed.html
```

---

## 9. Safe Remote Deployment Procedure

> [!CAUTION]
> Inspect all migrations before applying to remote D1. Verify migrations contain **schema changes only**.

```powershell
# 1. Apply schema migrations to remote D1
npx wrangler d1 migrations apply community-idx --remote -c wrangler.sneak.toml

# 2. Provision staging tenant (if testing staging)
npx wrangler d1 execute community-idx --remote --file=seeds/staging-demo-tenant.sql -c wrangler.sneak.toml

# 3. Set Bridge API secret on worker
npx wrangler secret put BRIDGE_TOKEN -c wrangler.sneak.toml

# 4. Deploy SNEAK worker to staging
npx wrangler deploy -c wrangler.sneak.toml
```

---

## 10. Provisioning a New Customer / Tenant

Execute SQL against D1:

```sql
-- 1. Create Tenant Account
INSERT INTO sneak_accounts (id, account_name, agent_mls_id, office_mls_id, plan, status)
VALUES ('acc_realtor_smith', 'Smith Luxury Homes', 'SMITH_MLS_01', 'OFFICE_BS_01', 'pro', 'active');

-- 2. Create Site Instance (e.g. agent-scoped)
INSERT INTO sneak_sites (id, account_id, site_key, site_name, scope_type, scope_value, status)
VALUES ('site_smith_01', 'acc_realtor_smith', 'smith-luxury', 'Smith Luxury Homes Search', 'agent', 'SMITH_MLS_01', 'active');

-- 3. Whitelist & Verify Customer Domains
INSERT INTO sneak_domains (id, site_id, domain, verified, status)
VALUES 
    ('dom_smith_1', 'site_smith_01', 'smithluxuryhomes.com', 1, 'active'),
    ('dom_smith_2', 'site_smith_01', '*.smithluxuryhomes.com', 1, 'active');

-- 4. Configure Dynamic Branding
INSERT INTO sneak_branding (site_id, display_name, brokerage, primary_color, secondary_color, phone, email, website_url)
VALUES (
    'site_smith_01',
    'Smith Luxury Homes',
    'Premier Coastal Realty',
    '#0f172a',
    '#0284c7',
    '(239) 555-0188',
    'john@smithluxuryhomes.com',
    'https://smithluxuryhomes.com'
);

-- 5. Enable Search Widget
INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json)
VALUES ('wc_smith_search', 'site_smith_01', 'search', 1, '{"pageSize":24}');
```
