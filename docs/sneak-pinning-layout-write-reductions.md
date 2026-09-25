# SNEAK IDX: Listing Pinning, Adaptive Layout & Database Write Reduction

## Executive Summary
This update fulfills three core enhancements across the **SNEAK IDX Platform**:
1. **Listing Pinning Across All Integration Types**: Provides the ability for members and site administrators to pin their own listings, listings of chosen agents, or specific MLS listing keys to appear first across all IDX integration types (search, map, quick search bar, listing grid, featured widgets, open houses, and agent showcases).
2. **Main Layout Expansion & Adaptive Height**:
   - Expanded container width from 1440/1600px up to **1800px** on desktop, with the listings grid widened to **58%** (and **60%** on screens >= 1600px).
   - Dynamically increases embed height via `SNEAK_RESIZE` postMessage by **+185px** whenever the Recently Viewed bar is active, preventing recently viewed cards from crowding the listings or map.
3. **Cloudflare D1 Database Row Write Reduction**: Implemented systematic write mitigations eliminating tens of thousands of unnecessary D1 row writes per day to protect database quotas.

---

## 1. Listing Pinning Across All Integration Types

### Supported Parameters & HTML Attributes
Members and administrators can specify pinning in any embed snippet or URL:
- **`data-pin-own="true"`** or `?pinOwn=1`: Automatically pins the member site's own listings first using their `default_agent_mls_id`.
- **`data-pin-agent="MLS_ID"`** / **`data-pin-agents="MLS1,MLS2"`** or `?pinAgents=...`: Pins listings of one or more chosen agents first.
- **`data-pin-listings="KEY1,KEY2"`** or `?pinListings=...`: Pins specific listing keys first.

### Backend Order Precedence Engine (`sneak-shared/idx-query.js`)
Sort queries compile with prioritized SQL `CASE` clauses while preserving user-selected secondary sorts:
```sql
ORDER BY 
  CASE 
    WHEN ListingKey IN (?, ?) THEN 0 
    WHEN ListAgentMlsId IN (?, ?) THEN 1 
    ELSE 2 
  END ASC, 
  ListPrice DESC
```
- Bound parameters are safely sequenced to protect against SQL injection.
- Pinned items return with an explicit `isPinned: true` metadata flag.

### Endpoints & Integration Types Supported
1. **Full Search & Map** (`SneakIDXWorker.js` `handleSearch`): Evaluates pinned listings, pinned agents, and site own agent.
2. **Open Houses Showcase** (`SneakIDXWorker.js` `handleOpenHouses`): Sorts pinned open house events first.
3. **Agent Listing Feed** (`SneakIDXWorker.js` `handleAgentListings`): Prioritizes specific pinned listing keys within the agent's inventory.
4. **Embed Loader** (`sneak-idx/embed.js`): Automatically detects `data-pin-own`, `data-pin-agent(s)`, `data-pin-listing(s)` on the script tag, container tag, or parent URL query strings.
5. **Search Application UI** (`sneak-idx/search/index.html`): Renders a blue **Pinned** chip with thumbtack icon and `.listing-card-pinned` styling on cards.
6. **Member Portal UI** (`sneak-member/ui.js`): Interactive builder inputs for Pin My Listings, Pin Agent MLS IDs, and Pin Specific Listing Keys.
7. **Admin Generator** (`sneak-admin/embed-generator.js`): Native `pinned_listings` snippet template.

---

## 2. Main Layout Width & Adaptive Recently Viewed Height

### Wider Desktop Layout
- `.filters-outer`, `.carousel-container`, and `.recently-viewed-section` maximum width expanded to **1800px; width: 100%**.
- Desktop listings section (`.listings-section`) expanded from 55% to **58%**, expanding to **60%** on wide viewports (>= 1600px).
- Embed loader desktop default height increased to **900px – 1150px**.

### Adaptive Recently Viewed Height
When a user views properties, they are logged to Recently Viewed:
1. `renderRecentlyViewed()` calls `updateAdaptiveEmbedHeight()`.
2. Toggles `body.has-recently-viewed` and `.main-container.has-recently-viewed` with `min-height: 690px`.
3. Dispatches a cross-origin `SNEAK_RESIZE` postMessage:
   ```javascript
   window.parent.postMessage({
     type: 'SNEAK_RESIZE',
     siteKey: SITE_KEY,
     height: targetH, // baseH + 185px
     recentlyViewedActive: isRecentActive
   }, '*');
   ```
4. `embed.js` receives the message and expands `iframe.style.height = `${newHeight}px`` up to 3500px.
5. The listings grid and map maintain their full vertical canvas without being squeezed or cropped.

---

## 3. Cloudflare D1 Database Row Write Reductions

| Area | Previous Behavior | Optimized Behavior | Estimated Write Reduction |
| :--- | :--- | :--- | :--- |
| **`SneakIDXWorker.js`** | `recordUsage(site_id, 'searches')` and `recordUsage(site_id, 'listing_views')` ran on every search query, keystroke, map pan, and listing view. | Suppressed search and view writes. `recordUsage` row writes strictly gated to `leads`. | **~99% reduction** (~10,000–50,000 writes/day saved) |
| **`sneak-sync/lock.js`** | Recorded a `sneak_sync_runs` row every 15 minutes even when no listings were fetched or updated. | `recordSyncRun` skips write when `status === 'success'` and zero records were fetched, upserted, or removed. | **~96 writes/day saved** per sync job |
| **`sneak-sync/listing-sync.js`** | Unconditional `INSERT OR REPLACE` deleted and re-inserted rows during Bridge delta overlap windows. | `INSERT INTO sneak_listings (...) ON CONFLICT(ListingKey) DO UPDATE SET ... WHERE excluded.ModificationTimestamp IS NOT sneak_listings.ModificationTimestamp OR excluded.StandardStatus IS NOT sneak_listings.StandardStatus OR excluded.ListPrice IS NOT sneak_listings.ListPrice`. | **~80–90% reduction** during recurring delta runs |
| **`sneak-sync/open-house-sync.js`** | Unconditional `INSERT OR REPLACE` rewrote all active open houses on every run. | `INSERT INTO sneak_open_houses (...) ON CONFLICT(OpenHouseKey) DO UPDATE SET ... WHERE excluded.OpenHouseStartTime IS NOT sneak_open_houses.OpenHouseStartTime OR ...`. | **~95% reduction** in recurring open house sync writes |
| **`sneak-sync/reconciliation.js`** | Periodic full reconciliation used `INSERT OR REPLACE` for all scanned rows. | Converted to conditional `ON CONFLICT(ListingKey) DO UPDATE SET ... WHERE ...`. | **~90% reduction** on clean inventory runs |
| **`sneak-admin/auth.js`** | Updated `last_seen_at` on every authenticated admin HTTP call. | Throttled `UPDATE sneak_admin_sessions SET last_seen_at = datetime('now') WHERE id = ? AND (last_seen_at IS NULL OR last_seen_at < datetime('now', '-15 minutes'))`. | **~95% reduction** during active admin sessions |
| **`sneak-member/auth.js`** | Updated `last_seen_at` on every authenticated member request. | Throttled `last_seen_at` updates to 15-minute intervals. | **~95% reduction** during active member sessions |
| **`sneak-consumer/auth.js`** | Updated `last_seen_at` on every authenticated consumer request. | Throttled `last_seen_at` updates to 15-minute intervals. | **~95% reduction** during active consumer sessions |
| **`sneak-consumer/api.js`** | Updated `last_activity_at` on consumer users on every action. | Throttled `last_activity_at` updates to 15-minute intervals. | **~90% reduction** during active consumer usage |
