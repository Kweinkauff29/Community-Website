# CCOR IDX Plug-in: Competitor Parity & Feature Roadmap

This document outlines the product architecture, feed capability matrix, and strategic feature roadmap for the **CCOR IDX Plug-in** (formerly SNEAK IDX Platform) operated by the Bonita Springs-Estero REALTORS® (CCOR).

---

## 1. Executive Product Architecture

```mermaid
graph TD
    MLS["Bonita Springs-Estero MLS Feed (Bridge OData)"] -->|Secure Ingestion Cron| SYNC["sneak-idx-sync-staging (BRIDGE_TOKEN Isolated)"]
    SYNC -->|Batch Upsert + Transforms| D1[("Cloudflare D1 Storage (sneak_listings, MediaJSON)")]
    D1 -->|Zero-Token Cached Query Engine| SERVE["sneak-idx-worker-staging (Serving Worker)"]
    SERVE -->|Signed Session Bootstrap| EMBED["CCOR IDX Plug-in Embed (embed.js)"]
    EMBED -->|Responsive Search Widget| SITE["Member Website (e.g. WordPress, Custom Domain)"]
```

---

## 2. Feature Roadmap by Phase

### Phase 7.3A — Search Parity Foundation
* **Goal:** Deliver consumer-grade search filtering across residential, commercial, and land properties with full mobile responsiveness and deterministic build telemetry.
* **Status:** `COMPLETE`
* **Features Included:**
  * Responsive 4-category navigation (`For Sale`, `Rental`, `Commercial`, `Lot & Land`).
  * Discrete dual-handle price range slider (`$0 — $20M+`).
  * Context-aware filter controls (automatic suppression of beds/baths/residential subtypes for commercial and land).
  * Comprehensive **More Filters** drawer (20+ consumer-facing filters).
  * **Wave-1 Advanced Filters:** Waterfront, Pool, Garage, New Construction, Sqft, Acres, Year Built, County, ZIP, Subdivision, Activity.
  * Multi-photo gallery served directly from D1 `MediaJSON` cache.
  * Site-owner lead capture routing with complete MLS broker attribution separation.
  * Deterministic UI build identification (`2026.08.25.7.3a`).

---

### Phase 7.3B1 — Interactive Map/List Synchronization & Mobile Map UX (CURRENT)
* **Goal:** Deliver a unified, debounced viewport-synchronized map/list search experience with context-aware popups, card/marker cross-highlighting, and responsive tablet/mobile segmented toggle.
* **Status:** `COMPLETE / DEPLOYED`
* **Features Included:**
  * **Search As I Move Map:** 400ms debounced viewport bounding box search (`north, south, east, west`) applied simultaneously across `/idx/v1/search` and `/idx/v1/map`.
  * **Map Search Loop Prevention:** Map auto-fits initial results once on page load; user-initiated pan/zoom searches strictly maintain user viewport without resetting or jumping.
  * **Search As I Move Control:** Unobtrusive `Search as I move` checkbox overlay with dynamic `Search this area` button when toggle is unchecked.
  * **Context-Aware Map Popups:**
    * *Residential & Rental:* Price, address, `X bd • Y ba • Z sqft`, photo preview, View Details CTA. Zero fake `0 bd • 0 ba`.
    * *Lot & Land:* Price, address, `X Acres • Subdivision/City`, photo preview, View Details CTA (beds/baths suppressed).
    * *Commercial:* Price, address, `X sqft • Y Acres • Subtype • Zoning`, photo preview, View Details CTA (beds/baths suppressed).
  * **Bidirectional Map ↔ Card Synchronization:**
    * Hovering/focusing a listing card raises and glows the corresponding map marker pin (`pin-highlighted`).
    * Clicking a map marker pin highlights the listing card and smoothly scrolls it into view on desktop.
  * **Mobile / Tablet Segmented View Toggle (<= 1024px):** Replaced vertical stacking with `List | Map` segmented control (defaults to List mode with active listing count; Map mode automatically resizes Leaflet tiles via `map.invalidateSize()`).
  * **Race-Safe Fetching & Lightweight Loaders:** `AbortController` request cancellation prevents stale query overwrites; non-blocking spinner/overlay keeps the map draggable during movement.
  * **Truncation Indicator:** Map query selects `limit + 1` to gracefully flag and display `Too many properties to display. Zoom in to see more.`
  * **Deterministic UI Build:** `2026.08.27.7.3b1` telemetry verified across search app, embed loader, and console.

---

### Phase 7.3B2A — Radius Search, Near Me, Spatial State & Desktop Hide Map
* **Goal:** Deliver centralized frontend spatial search state, server-side equirectangular distance filtering in D1 SQLite, responsive Leaflet radius visualization, Near Me geolocation, and desktop map toggle.
* **Status:** `COMPLETE / DEPLOYED`
* **Features Included:**
  * **Centralized Spatial State Engine:** Mode precedence (`viewport` vs `radius`), center point coordinates, distance radius in miles, `isNearMe` origin tracking, search state serialization (`serializeSearchState` / `applySearchState` for Phase 7.3C preparation).
  * **D1 SQL Equirectangular Distance Filtering:** Bounding box prefilter + parameterized distance math executed server-side across `/idx/v1/search` and `/idx/v1/map`.
  * **Radius Map UI & Visualization:** Quick distance selector chips (1, 2, 5, 10, 25 mi), interactive map center placement, dynamic Leaflet circle rendering, fit-to-radius bounds once.
  * **Near Me Geolocation:** Compact GPS location search with HTML5 `navigator.geolocation`, fallback friendly messaging, ephemeral coordinates without telemetry storage.
  * **Desktop Hide Map (> 1024px):** Toggle button collapses map and expands listings grid to 100% full width; persisted in `localStorage` (`ccor_idx_map_visible_${SITE_KEY}`).
  * **Active Spatial Badges:** Responsive `Within X mi ✕` pill in secondary filter toolbar and on-map badge overlay with one-click area clearing.
  * **Deterministic UI Build:** `2026.08.27.7.3b2a` across search UI, embed loader, and tests.

---

### Phase 7.3B2B — Draw Area / Server-Authoritative Polygon Search (CURRENT)
* **Goal:** Deliver custom interactive polygon search area drawing, draggable vertex editing, hardened spatial validation with HTTP 400 rejection on malformed spatial parameters, and server-authoritative ray-casting point-in-polygon SQL execution in Cloudflare D1 SQLite.
* **Status:** `COMPLETE / DEPLOYED`
* **Features Included:**
  * **Custom Draw Area Tool & UI:** Interactive map drawing with crosshair cursor, dynamic dashed guide lines, vertex placement with auto-close snapping to start vertex, floating control banner with one-click Complete / Cancel.
  * **Draggable Vertex Editing:** One-click `Edit Area` turns polygon vertices into draggable handles with live geometry updates, saving to state on completion without premature intermediate queries.
  * **Server-Authoritative D1 SQL Ray-Casting:** Bounding box prefilter (`Latitude BETWEEN ? AND ? AND Longitude BETWEEN ? AND ?`) + parameterized ray-crossing SQLite modulo expression (`((edge1 + ... + edgeN) % 2) = 1`). Exact search counts, pagination totals, and map markers agree 100%.
  * **Hardened Spatial Validation:** Explicit HTTP 400 `InvalidSpatialFilter` returned on incomplete radius parameters or malformed GeoJSON geometry; prevents silent search broadening.
  * **GeoJSON Polygon Serialization:** 5-6 decimal place normalized coordinate storage in `spatialState.polygon`, fully serializable and restorable via `serializeSearchState()` / `applySearchState()`.
  * **Mutual Exclusivity & Clean Mode Switching:** Drawing a polygon automatically clears radius; selecting radius clears polygon; clearing area returns to standard viewport mode.
  * **Deterministic UI Build:** `2026.08.27.7.3b2b` across search UI, embed loader, and verification scripts.

---

### Phase 7.3C1A — Buyer Identity, Passwordless Authentication & Server Favorites (COMPLETED)
* **Goal:** Deliver site-scoped consumer identity, passwordless magic-link authentication with 2-minute exchange codes, 14-day revocable sessions, server favorites with anonymous union merge, address suppression compliance, and XSS sanitization.
* **Status:** `COMPLETE / DEPLOYED`
* **Features Included:**
  * **Isolated Consumer Worker & Trust Realm (`sneak-consumer/`):** Dedicated worker (`sneak-idx-consumer-staging`) strictly isolated from member/admin realms (`sneak-member/`).
  * **Site-Scoped Consumer Identity:** `sneak_consumer_users` with `UNIQUE(site_id, email)` ensures buyer accounts on Agent A vs Agent B sites are isolated tenants.
  * **Passwordless Magic Links & Rate Limiting:** Single-use SHA-256 hashed magic links with 15-minute TTL; generic anti-enumeration responses protect user privacy; IP & email rate limiting defense.
  * **Open-Redirect & Protocol Protection:** Return URLs strictly verified against active verified hostnames in `sneak_domains` with mandatory HTTPS.
  * **2-Minute Exchange Code Handoff:** Atomic single-use exchange code in `sneak_consumer_auth_exchanges` prevents exposure of long-lived session tokens in redirect URLs.
  * **Embed Parent Handoff:** `embed.js` extracts `auth_code` from parent page query, forwards into search iframe, and cleans parent browser URL with `history.replaceState`.
  * **Server Favorites & Local Anonymous Merge:** `sneak_consumer_favorites` (max 200 items); optimistic heart icon toggles; POST `/api/consumer/favorites/merge` automatically merges anonymous local favorites on sign-in.
  * **XSS Sanitization & MLS Security:** `escapeHtml` and `sanitizeUrl` guard all dynamic HTML rendering across listing cards, popups, and carousels.
  * **Deterministic UI Build:** `2026.08.27.7.3c1a` across search UI, embed loader, consumer worker, and test suites.

### Phase 7.3C1B — Embed Layout Stabilization & Saved Searches / State Restore (COMPLETED)
* **Goal:** Stabilize the embedded search layout into a true app shell, eliminate host website bottom cut-offs, provide responsive parent iframe height scaling, and deliver server-persisted saved searches with duplicate detection and round-trip state restoration.
* **Status:** `COMPLETE / DEPLOYED`
* **Features Included:**
  * **Embed App-Shell Architecture:** Refactored embedded search (`body.is-embedded`) to a CSS flex container (`flex-direction: column; height: 100vh; overflow: hidden;`), consuming all remaining iframe height for map and listings without pushing content beyond iframe boundary into host website footers.
  * **Parent Iframe Height Stabilization:** Responsive desktop default `clamp(780px, 85vh, 960px)` in `embed.js`, respecting optional `data-height` and `data-fixed-height` attributes.
  * **Secured PostMessage Resize Channel:** Window source validation (`e.source === iframe.contentWindow`), site key match, height bounds enforcement (400px–3000px), and anti-jitter debouncing (<= 3px filter).
  * **Saved Searches Schema (`sneak_consumer_saved_searches`):** Cloudflare D1 table with foreign key isolation to `sneak_sites(id)` and `sneak_consumer_users(id)`, compound indexes for rapid user listing and duplicate detection.
  * **Search State Normalization & Privacy:** Strict validation engine (max 16KB, version 1); converts dynamic GPS Near Me tracking to static coordinate radius centers; validates max 40 polygon vertices.
  * **Duplicate Detection via SHA-256 (`state_hash`):** Matching search criteria automatically updates search name and timestamp instead of creating duplicate records; enforced 25 saved searches limit per consumer/site.
  * **Consumer Saved Searches API:** Full REST CRUD endpoints (`GET`, `POST`, `PATCH /:id`, `DELETE /:id`) under `/api/consumer/saved-searches` with tenant isolation.
  * **UI Integration & Search Restore:** Save Search trigger button in toolbar, modal naming dialog with criteria summary, Saved Searches list modal with View/Rename/Delete, and pending anonymous save intent restoration across magic-link sign-in.
  * **Deterministic UI Build:** `2026.08.28.7.3c1b` across search UI, embed loader, consumer worker, and test suites.

#### 7.3C1B.1 Layout Hotfix (COMPLETED)
* **Exact Regression:** Live WordPress pilot on `/idx-test/` rendered with crushed Map/List workspace, clipped listings, and footer placement anomalies.
* **Root Cause:**
  1. *Stale `data-height="850px"` in WordPress snippet:* Overrode modern responsive height because `embed.js` lacked parent window resize listeners and treated legacy `data-height` as an immutable fixed height rather than a compatibility hint.
  2. *Footer disclaimer consuming app flex space:* `.footer-disclaimer` was placed outside `.main-container` directly under `body`, consuming ~180px of flexbox height inside `body.is-embedded` (which was constrained to 100vh).
  3. *Unclosed CSS media query syntax error:* Unclosed selector and media query around line 1979 prevented modal and responsive rules from applying properly on desktop.
  4. *Mobile listings height collapse:* `.main-container.mobile-list-view .listings-section` used `height: auto !important` instead of bounded internal scrolling.
* **Final Embed Sizing Model:**
  * **Deterministic Parent-Controlled Responsive Sizing:** `getRecommendedSearchHeight()` dynamically calculates optimal height based on parent viewport:
    * Desktop (`> 1024px`): `Math.max(860, Math.min(Math.round(viewportHeight * 0.90), 1050))` (~860px–960px).
    * Tablet (`601px - 1024px`): `Math.max(760, Math.min(Math.round(viewportHeight * 0.88), 920))` (~760px).
    * Mobile (`<= 600px`): `Math.max(680, Math.min(Math.round(viewportHeight * 0.88), 850))` (~680px–740px).
  * **Debounced Parent Window Resize:** Window resize listener (150ms debounce) recalculates iframe height continuously across orientation changes and desktop resizing (bypassed only when `data-fixed-height="true"` is explicitly set).
  * **Nested Disclaimer Stream:** Moved `.footer-disclaimer` inside `.listings-section` beneath `#paginationBar`, freeing 180px of permanent vertical space for a 620px+ usable Map/List workspace.
  * **Compact Carousel & Syntax Fix:** Refined carousel image height to 100px and repaired all CSS media query blocks.
* **Tested Viewports:** Desktop (1440×900, 1920×1080), Tablet (1024×768), Mobile (390×844).
* **Deterministic UI Build:** `2026.08.28.7.3c1b1`.

---


### Phase 7.3C2A — Saved Search Email Alert Engine & Delivery Infrastructure (COMPLETED)
* **Goal:** Deliver scheduled automated email alerts (ASAP and Daily digest) matching saved search criteria with exact query parity, anti-spam baseline isolation, delivery idempotency, context-aware specs, address suppression, deep linking, and one-click tamper-proof unsubscribe.
* **Status:** `COMPLETE / DEPLOYED`
* **Features Included:**
  * **Dedicated Alert Worker (`sneak-alerts/`):** Cloudflare Worker (`sneak-idx-alerts-staging`) running scheduled cron jobs (`10,25,40,55 * * * *`) offset from MLS sync to prevent race conditions.
  * **Strict Security Boundary:** Zero Bridge token, zero GrowthZone credentials, zero Stripe secrets. Queries inventory directly from Cloudflare D1.
  * **Shared Query Parity Engine (`sneak-shared/idx-query.js`):** Unified `buildSavedSearchWhereQuery` ensures 100% criteria agreement between public search and alert matching across residential, rental, land, commercial, radius, and polygon modes.
  * **Baseline Anti-Spam Guarantee:** Alert activation sets `enabled_at = now`; historical inventory matching the search before enablement is strictly excluded to prevent inbox flooding.
  * **Idempotent Match Ledger (`sneak_consumer_alert_matches`):** `UNIQUE(alert_id, listing_key, event_type)` ensures each new property is alerted at most once per saved search.
  * **Daily Digest Scheduling:** Timezone-aware date calculations (`getLocalTimeInZone`) send daily digests at/after 8:00 AM local time, strictly once per local calendar day (`last_daily_local_date`).
  * **Context-Aware Email Cards & Attribution:** Up to 10 property cards per email with beds/baths/sqft for residential/rental, acres/subdivision for land, sqft/acres/subtype/zoning for commercial, mandatory listing brokerage attribution, and `InternetAddressDisplayYN` address suppression.
  * **Deep Linking (`?ccor_listing=<ListingKey>`):** Embed loader and search app parse listing key, validate tenant session/scope, and open property detail modal with graceful fallback notice if unavailable.
  * **One-Click Signed Unsubscribe:** Tamper-proof HMAC-SHA256 tokens disable alerts without requiring user login or passwords.
  * **Delivery Logging & Bounded Retries:** `sneak_consumer_alert_deliveries` tracks provider message IDs, delivery statuses (`sent`, `failed`, `provider_unconfigured`), and error codes.
  * **Deterministic UI Build:** `2026.08.28.7.3c2a` across alert worker, consumer worker, serving worker, embed loader, and test suites.

#### 7.3C2A.1 Alert Delivery Correctness, Concurrency & Secret Hardening Hotfix (COMPLETED)
* **Goal:** Enforce fail-closed secret handling, eliminate all hardcoded fallback signing secrets, make alert delivery concurrency-safe via atomic claim-before-send semantics, and fix delivery status accounting so `provider_unconfigured` never marks listings as permanently notified.
* **Status:** `COMPLETE / DEPLOYED`
* **Features & Correctness Hardening:**
  * **HOTFIX 1 — provider_unconfigured Semantics:** When Mailjet credentials are absent or unconfigured in staging/dev, transactional email provider returns `{ success: false, retryable: true, status: 'provider_unconfigured' }`. Worker records delivery row with `status = 'provider_unconfigured'` and `sent_at = NULL`, leaves candidate listings retryable without setting `notified_at` or advancing `last_sent_at`, and tallies `emailsDeferred += 1` (`emailsSent = 0`).
  * **HOTFIX 2 — Removal of Fallback Signing Secrets:** Completely removed `'sneak-default-token-secret-fallback-key'` from all runtime code. Tokens require `SNEAK_ALERT_UNSUBSCRIBE_SECRET` or `SNEAK_SIGNING_SECRET` (>= 16 chars). Worker fails closed with delivery deferral and returns HTTP 503 on unsubscribe route when secrets are missing.
  * **HOTFIX 3 — Atomic Claim-Before-Send Idempotency:** Implemented atomic listing claiming in `sneak_consumer_alert_matches` with `claim_id`, `claimed_at`, `claim_expires_at` (10 mins), and `delivery_status = 'claimed'`. Only candidate listings claimed by the current execution run are included in alert delivery. Concurrent executions competing for the same listings produce exactly 1 delivered email with zero duplicate sends.
  * **D1 Migration 0029:** Added `claim_id`, `claimed_at`, `claim_expires_at`, `delivery_status`, `attempt_count` columns and index `idx_sneak_consumer_alert_matches_claim` to `sneak_consumer_alert_matches`, plus `emails_deferred` to `sneak_alert_runs`.
  * **Deterministic UI & Worker Build:** `2026.08.30.7.3c2a1` across alert worker, consumer worker, embed loader, search UI, and test suites.

---

### Phase 7.3C2B — Agent Client Activity Dashboard & Authenticated Buyer Activity Timeline (COMPLETE)
* **Goal:** Deliver a dedicated Clients area in the REALTOR® Member Portal allowing the REALTOR® to view authenticated buyers using their CCOR IDX site, inspect longitudinal buyer activity timelines, and review saved homes, searches, and inquiries with strict tenant isolation and MLS display controls.
* **Status:** `COMPLETE / DEPLOYED / AUTHENTICATED BROWSER QA PASS`
* **Features Included:**
  * **Privacy & Controlled Telemetry Ledger (`sneak_consumer_activity_events`):**
    * D1 Migration `0030_sneak_consumer_activity.sql` creates high-throughput activity ledger with compound indexes and adds `last_activity_at` to `sneak_consumer_users`.
    * **Zero Anonymous Telemetry Invariant:** Anonymous browsing is never tracked. Strictly captures authenticated buyer actions under controlled vocabulary (`listing_view`, `favorite_added`, `favorite_removed`, `saved_search_created`, `saved_search_updated`, `saved_search_deleted`, `alert_enabled`, `alert_frequency_changed`, `alert_disabled`, `inquiry_submitted`).
    * Zero visitor fingerprinting, zero IP profiles, zero scroll/pan tracking, zero GPS coordinates. Known event metadata is allowlisted before serialization, unknown fields are discarded, and only complete valid JSON values up to 2KB are stored.
  * **Consumer Activity Ingestion API (`POST /api/consumer/activity`):**
    * Authenticated session required (HTTP 401 on missing/invalid token).
    * Restrictive browser allowlist: Only `listing_view` and `inquiry_submitted` accepted from frontend.
    * 30-minute deterministic deduplication window on `listing_view` via `dedupe_key`.
    * Listing validation: Verifies property exists, is display eligible (`InternetEntireListingDisplayYN = 1`), and matches site tenant scope.
    * Inquiry validation: Verifies `leadId` belongs to site and matches normalized consumer email.
    * Rate limiting: 120 browser-reported events per hour per consumer session.
  * **Server-Side Mutation Logging:**
    * Favorites CRUD automatically records `favorite_added` and `favorite_removed`.
    * Saved searches CRUD records `saved_search_created`, `saved_search_updated`, `saved_search_deleted` (preserving search name in metadata).
    * Search alert preference transitions record `alert_enabled`, `alert_frequency_changed`, `alert_disabled`.
  * **Serving Worker & Frontend IDX Activity Capture:**
    * `sneak-idx/search/index.html` fires non-blocking `keepalive: true` `listing_view` on detail modal open when consumer is signed in.
    * Authenticated lead form submission automatically logs `inquiry_submitted` with `leadId`.
  * **Member Worker APIs (`sneak-member/api.js`):**
    * `GET /api/member/clients`: Paginated buyer roster with search by email, sorting (`recently_active`, `newest`, `saved_homes`, `saved_searches`), and aggregated counters.
    * `GET /api/member/clients/:id`: Full client profile, saved homes (with address suppression and off-market unavailability flags), saved searches with alert frequencies, and inquiries.
    * `GET /api/member/clients/:id/activity`: Paginated chronological activity timeline with resolved safe listing metadata.
    * `GET /api/member/overview`: Updated with `clientsCount`, `activeClients7dCount`, `savedHomesCount`, `savedSearchesCount`.
    * **Strict Tenant Isolation:** All queries strictly join `sneak_sites` filtered by `memberContext.account_id`. Foreign client lookups return HTTP 404 with zero enumeration. Same email across two member sites represents two distinct, isolated client records.
  * **Member Portal UI (`sneak-member/ui.js`):**
    * Modern dark-mode dashboard with Clients navigation tab, 4 live KPI summary cards, responsive client list table, search bar, sort dropdown, and empty states.
    * Modal Client Detail view with quick summary, Saved Homes grid, Saved Searches/Alerts list, Inquiries list, and interactive Activity Timeline with human-friendly event badges and timestamps.
    * Authenticated live browser verification passed at desktop (1440x900), tablet (1024x768), and mobile (390x844), covering Clients navigation, KPIs, search/sort, detail data, modal scrolling, responsive layout, and zero clipping/overflow. A saved-home fallback-handler defect found during QA was corrected.
  * **Deterministic Corrective Build:** Member Portal is deployed at `2026.08.31.7.3c3a1`; the C2B acceptance scope is complete.

---

### Phase 7.3C3A — Recently Viewed + Property Compare (COMPLETE — C3A.1 CORRECTIVE SIGN-OFF)
* **Goal:** Private recently viewed history and contextual side-by-side listing comparison for anonymous and authenticated consumers without adding anonymous server tracking.
* **Status:** `COMPLETE / DEPLOYED / AUTOMATED, LIVE API, AND BROWSER QA PASS`
* **Features Included:**
  * **Authenticated Recently Viewed:** `GET /api/consumer/recently-viewed` derives at most 20 unique listings exclusively from authenticated `listing_view` events in the last 90 days. It fetches extra candidates before resolving current active, display-eligible, in-scope D1 state.
  * **Anonymous Recently Viewed:** Site-scoped local storage retains at most 20 unique `{ key, viewedAt }` entries for 30 days, rewrites cleaned storage, and preserves device history through ordinary logout. Anonymous history is never sent to the consumer worker or uploaded after login.
  * **Property Compare:** Card controls, listing-detail action, responsive four-property tray, clear/remove controls, and a contextual comparison modal for Residential, Rental, Land, and Commercial listings.
  * **Authenticated Persistence:** `0031_sneak_consumer_compare.sql` stores listing references only, enforces a unique user/site/listing tuple, and provides a database-level maximum-four trigger. API reads and writes resolve current listing state and prune stale, private, off-market, or out-of-scope keys.
  * **Anonymous Persistence & Merge:** Site-scoped local storage contains listing keys only. Login merges eligible local keys into the server list, preserves existing server entries, respects the four-property cap, and clears local compare keys only after a successful merge. Anonymous Recently Viewed is not part of that merge.
  * **Bounded Summary Endpoint:** `GET /idx/v1/listings/summary` accepts at most 20 keys and uses only the serving D1 cache; it does not call Bridge or emit consumer activity.
  * **Activity Isolation:** Compare mutations do not create REALTOR® Client Activity events.
  * **Migration Safety:** `0032_sneak_site_fk_compatibility.sql` is a safe compatibility marker because canonical migrations already target `sneak_sites(id)`. Historical empty-staging repair SQL is retained as an explicitly manual operations artifact, and a read-only preflight fails closed for populated legacy schemas.
  * **Metadata Safety:** Event-specific allowlists discard unknown fields; complete valid JSON is serialized within 2KB without string slicing. Alert frequency metadata supports only Off, ASAP, and Daily.
  * **Browser Acceptance:** Actual Chromium QA passed at 1440x900, 1024x768, and 390x844, plus independent authenticated browser contexts for server-persisted Compare and Recently Viewed and anonymous network/storage privacy inspection.
  * **Deterministic Corrective Build:** `2026.08.31.7.3c3a1` across affected consumer, member, and serving workers, embed loader, search UI, verification, and tests. Alert runtime remains unchanged.

### Phase 7.3C3B — Safe Property Sharing + Consumer Shareable Property Lists (COMPLETE)
* **Goal:** Add explicit consumer-controlled sharing without weakening privacy, tenant isolation, or IDX display rules.
* **Status:** `COMPLETE / DEPLOYED / AUTOMATED, LIVE API, AND INDEPENDENT-RECIPIENT BROWSER QA PASS`
* **Features Included:**
  * **Individual Property Sharing:** Listing detail and public-list cards use the native Web Share API when supported. Desktop/browser fallback first attempts the Clipboard API and then exposes a selectable manual-copy field. Payloads contain a human-readable address/price title, short description, and member-domain `ccor_listing` URL.
  * **Safe Member URLs:** `embed.js` forwards an untrusted host-page candidate for Serving Worker validation. Only active verified tenant domains are accepted, HTTPS is mandatory outside development, fragments are removed, and auth/session/bootstrap/magic/bearer tokens plus stale `ccor_listing` and `ccor_list` values are stripped before one intended deep-link value is appended. Normal sharing never exposes a `workers.dev` iframe URL.
  * **Private Shared Lists:** Migration `0033_sneak_consumer_shared_lists.sql` stores list and listing-reference rows only, with cascading site/user/list foreign keys, unique items, indexes, and database-backed concurrency guards. Limits are 10 lists per consumer/site, 25 properties per list, and 80 characters per list name.
  * **Owner Management:** Authenticated Consumer Worker APIs support create, list, rename, delete, add/remove item, enable sharing, and disable sharing. Adding is idempotent and validates current listing existence, tenant scope, Internet display permission, and available status. Lists are private by default.
  * **Unlisted Capability Model:** Enabling sharing creates a 48-hex-character slug from 192 bits of Web Crypto randomness. The capability is tenant-bound and discloses no consumer identity. Disabling sharing immediately sets the slug to `NULL`; re-enabling creates a new slug, so previously distributed links remain revoked.
  * **Current-State Public Read:** `GET /idx/v1/shared-list/:slug` runs through the signed Serving Worker bootstrap, resolves only current D1 MLS rows, enforces tenant/status/display/address controls, and returns current price, status, contextual summary fields, and listing-office attribution. Missing, malformed, wrong-site, disabled, and deleted capabilities share the same generic unavailable response. No Bridge call occurs in the serving path.
  * **Consumer UI Integration:** Shared Lists appears after Saved Homes, Saved Searches, and Recently Viewed in the buyer account. Add to List is available from detail, Saved Homes, and Recently Viewed; Compare can be saved as a private list. Public lists use the existing card/detail design across Residential, Rental, Land, and Commercial and remain closeable within the REALTOR® member site.
  * **Privacy:** Anonymous recipients need no consumer account. Public reads create no consumer event, client record, fingerprint, share analytics, or third-party tracking. Public payloads contain no consumer email/ID/session, saved state, activity, or other lists. Views are marked `noindex, nofollow`, and no shared-list events were added to the REALTOR® activity timeline.
  * **Acceptance:** Migration 0033 is applied remotely; Serving and Consumer Workers are deployed. Automated regression passes 188/188 across 11 suites, live verification passes 12/12, and actual Chromium acceptance passes 14/14 across 1440×900, 1024×768, and 390×844. Browser A created and shared the list; an independent anonymous Browser B opened current properties without login or Consumer POSTs; Browser A revoked it; Browser B refreshed the old URL and received the generic unavailable view.
  * **Deterministic Build:** `2026.08.31.7.3c3b` for affected Serving Worker, Consumer Worker, embed loader, and search UI. Member and Alert runtimes remain intentionally unchanged.

---

### Phase 7.4A — Launch Readiness Control Plane + Staff Provisioning (COMPLETE)
* **Goal:** Let CCOR staff safely provision, understand, activate, troubleshoot, suspend, cancel, and reactivate member service without ad-hoc D1 edits.
* **Status:** `COMPLETE / STAGING DEPLOYED / ALL ACCEPTANCE GATES PASS`
* **Features Included:**
  * Central fail-closed serving helper backed only by `sneak_account_entitlements`; legacy Stripe rows cannot authorize service.
  * Resumable eight-step Admin workflow using canonical account, entitlement, site, member, domain, branding, widget, and readiness records.
  * Searchable account operations list and complete Account Detail with setup progress, serving/launch blockers, audit, lifecycle, and current responsive embed.
  * Stable per-account readiness blocker codes, automated checklist, and core-versus-optional capability status.
  * Deliberate non-browser modal confirmations for suspend, cancel, reactivate, site disable, and domain removal; no routine lifecycle action deletes data.
  * Human Member subscription states and removal of legacy Stripe presentation from current MVP controls.
  * External dependency, security, email, lifecycle, provisioning, and production-next-phase documentation in `docs/ccor-idx-launch-readiness.md`.
  * Corrective consumer detail hydration always fetches the authoritative current D1 listing, consumes the canonical URL-string media contract, rejects stale A→B responses, and keeps activity/history work non-blocking. Ten real multi-photo listings passed 65/65 live detail/media assertions after deployment.
  * Actual Chrome acceptance passed 748/748 checks at 1440×900, 1024×768, and 390×844 across the 11-listing category/gallery matrix, explicit current price/address/status/type/subtype/city/attribution and meaningful filter-field parity, every current detail origin, authenticated Admin workflow, complete authenticated Member/C2B navigation and client workflow, anonymous privacy, request ordering, overflow, and no-Bridge gates.
  * Automated regression passed 246/246 tests across 13 suites; live Phase 7.4A health/auth verification passed 17/17; remote D1 has no pending migrations and no foreign-key violations.
  * Deterministic Admin/Serving/Member/Sites/embed/search build `2026.08.31.7.4a`.

### Phase 7.4B1 — Transactional Email Readiness + GrowthZone Reconciliation
* **Goal:** Converge transactional email on one strict provider contract and add safe GrowthZone-to-canonical-entitlement reconciliation before production infrastructure exists.
* **Status:** `IMPLEMENTED / STAGING DEPLOYED / OPERATOR EVIDENCE PENDING`
* **Features Included:**
  * Member, Consumer, and Alerts use `sneak-shared/email-provider.js`; simulated success is removed. Mailjet HTTP success alone is insufficient: message-level success and a provider message ID are required.
  * Migration `0034_sneak_growthzone_reconciliation.sql` records bounded reconciliation state and separate controlled-inbox/API launch evidence.
  * GrowthZone mutation requires an explicit person/org contact reference, normalizes documented membership states, preserves `sneak_account_entitlements` as the only serving authority, protects manual overrides, and never suspends on timeout/outage.
  * Admin Account Detail shows reconciliation source/reference/status/attempt/difference and a single-account action; Launch Readiness provides separate capability cards and a bounded batch control.
  * Daily reconciliation runs at 11:15 UTC, sequentially, with a default limit of 25 and hard limit of 50 because a documented upstream request allowance was not established.
  * Automated regression passes 262/262 across 14 suites; live staging verification passes 22/22; full Chrome regression remains 748/748; focused 7.4B1 Chrome acceptance passes 123/123.
  * Affected build: `2026.09.01.7.4b1`. Migration 0034 and Admin/Member/Consumer/Alerts staging deployments are complete.
* **Remaining operator evidence:** Member controlled-inbox delivery/consumption has not been re-verified. Consumer/Alerts Mailjet credentials, Alerts signing secret, GrowthZone API key, controlled QA mailbox access, and authenticated GrowthZone proof are unavailable. Those capabilities remain `NOT_VERIFIED` or `NOT_READY`; no mock/config-only state is labeled ready.

### Phase 7.4B2 — Production Environment + First Member Cutover (NEXT)
* **Goal:** Supply and verify remaining operational credentials/evidence, create isolated production Workers and D1, and perform the first controlled member cutover with rollback readiness.
* **Status:** `NEXT / NOT STARTED`
* **Boundary:** No production Worker, production D1, route, DNS cutover, or SEO work started in 7.4B1.

### Phase 7.3D — SEO & Market Content Engine (DEFERRED / POST-LAUNCH)
* **Goal:** Drive organic Google search traffic and local community authority through static/edge-rendered SEO landing pages.
* **Status:** `DEFERRED / POST-LAUNCH`
* **Feature Candidates:**
  * **SEO-Friendly Listing URLs:** Slugged paths (e.g. `/homes/bonita-springs/pelican-landing/3694-pleasant-springs-dr-224017488`).
  * **Community & Neighborhood Landing Pages:** Pre-filtered landing pages for major subdivisions (Pelican Landing, Bonita Bay, Miromar Lakes, Mediterra).
  * **Open House Landing Pages:** Weekend Open House directory with automated Friday updates.
  * **Dynamic XML Sitemaps:** Automated sitemap index generation containing all active MLS listing URLs.
  * **Local Market Statistics Widget:** Median price, days on market, and inventory trends for local communities.

---

## 3. BSAOR MLS Feed Capability & Feature Status Matrix

| Feature / Field Concept | RESO / BSAOR Standard Field | Data Type | Feed Coverage | Feature Status | Notes / Plan |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Property Categories** | `PropertyType` | `Edm.String` | 100.0% | `AVAILABLE` | Residential, Land, Lease, Commercial Sale |
| **Residential Subtypes** | `PropertySubType` | `Edm.String` | 36.2% | `AVAILABLE` | Single Family, Condo, Townhouse, Villa, Duplex |
| **Price Filter & Range** | `ListPrice` | `Edm.Double` | 100.0% | `AVAILABLE` | Dual-handle discrete price ladder slider |
| **Beds / Baths** | `BedroomsTotal`, `BathroomsTotalInteger` | `Edm.Int64` | 99.2% | `AVAILABLE` | Filter & card display (hidden on Land/Comm) |
| **Living Area / Sq Ft** | `LivingArea` | `Edm.Double` | 98.4% | `AVAILABLE` | Min/Max filter & sorting (`sqftDesc`) |
| **Lot Size / Acres** | `LotSizeAcres` | `Edm.Double` | 99.1% | `AVAILABLE` | Min/Max filter & sorting (`acresDesc`) |
| **Year Built** | `YearBuilt` | `Edm.Int64` | 97.6% | `AVAILABLE` | Min/Max filter & sorting (`yearDesc`) |
| **Waterfront** | `WaterfrontYN`, `WaterfrontFeatures` | `Edm.Boolean` | 31.3% / 99.8% | `AVAILABLE` | Wave-1 filter (`WaterfrontYN = 1`) |
| **Private Pool** | `PoolPrivateYN`, `PoolFeatures` | `Edm.Boolean` | 8.3% / 9.2% | `AVAILABLE` | Wave-1 filter (`PoolPrivateYN = 1`) |
| **Garage Spaces** | `GarageSpaces`, `AttachedGarageYN` | `Edm.Double` | 25.7% | `AVAILABLE` | Wave-1 filter (`GarageSpaces >= N`) |
| **New Construction** | `NewConstructionYN` | `Edm.Boolean` | 10.7% | `AVAILABLE` | Wave-1 filter (`NewConstructionYN = 1`) |
| **Open Houses** | `sneak_open_houses` (Bridge OpenHouse) | Table Join | Active Only | `AVAILABLE` | Filter (`openHouse=1`) & badge |
| **Price Reduced** | `OriginalListPrice`, `ListPrice` | `Edm.Double` | 96.0% | `AVAILABLE` | Filter (`ListPrice < OriginalListPrice`) |
| **New Listings (7 Days)** | `ListingContractDate` | `Edm.Date` | 100.0% | `AVAILABLE` | Filter (`newListingDays=7`) & Carousel |
| **Subdivision / Neighborhood** | `SubdivisionName` | `Edm.String` | 88.5% | `AVAILABLE` | Text search & filter matching |
| **County / Postal Code** | `CountyOrParish`, `PostalCode` | `Edm.String` | 100.0% | `AVAILABLE` | Dropdown & ZIP filter |
| **Zoning** | `Zoning` | `Edm.String` | 55.5% | `AVAILABLE` | Filter & card display for Commercial & Land |
| **Photo Gallery** | `MediaJSON` (Bridge Media array) | `Edm.String` | 99.4% | `AVAILABLE` | Full multi-photo carousel & thumbnails |
| **Interactive Map** | `Coordinates` / `Latitude`, `Longitude` | `REAL` | 99.1% | `AVAILABLE` | Clustered Leaflet markers with popups |
| **Spatial Polygon Search** | `Coordinates` Ray-Casting SQLite SQL | `GeoJSON` | 99.1% | `AVAILABLE` | Server-authoritative ray-casting polygon filter (Phase 7.3B2B) |
| **Radius Distance Search** | `Coordinates` Equirectangular D1 SQL | Math | 99.1% | `AVAILABLE` | Server-authoritative distance radius filter (Phase 7.3B2A) |
| **View Type (Canal/Golf/Water)** | `ViewYN`, `View` | `Collection` | 49.0% / 95.7% | `FEED AUDIT REQUIRED` | Planned for Wave-2 Amenity Filters |
| **55+ / Senior Community** | `SeniorCommunityYN` | `Edm.Boolean` | 0.2% | `NOT RECOMMENDED` | Unreliable feed coverage (< 1%) |
| **Virtual Tour Link** | `VirtualTourURLUnbranded` | `Edm.String` | 6.2% | `FUTURE` | Planned for Property Detail modal |
| **Furnished Status** | `Furnished` | `Edm.String` | 35.3% | `FUTURE` | Turnkey / Furnished / Unfurnished for Rentals |
| **Pets Policy** | `PetsAllowed` | `Collection` | 36.2% | `FUTURE` | Pets allowed filters for Rentals / Condos |
| **HOA Fee Amount** | `AssociationFee`, `AssociationFeeFrequency` | `Edm.Double` | 3.7% | `FEED AUDIT REQUIRED` | Low standard field coverage; check custom fields |
| **School District** | `ElementarySchool`, `HighSchool` | `Edm.String` | 15.2% | `FEED AUDIT REQUIRED` | Low coverage; check Lee/Collier mapping |
