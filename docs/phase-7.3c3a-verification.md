# Phase 7.3C3A Verification Report

Date: 2026-08-31

Build: `2026.08.31.7.3c3a`

Status: **Deployed; automated/live API verification passed; required C2B and C3A browser sign-off pending**

## Delivered scope

- Authenticated Recently Viewed derived only from existing `listing_view` events.
- Anonymous Recently Viewed stored only in site-scoped local storage as listing key and timestamp; no server tracking and no login upload.
- Anonymous and authenticated Compare persistence, local-to-server Compare merge, and a four-property maximum.
- Current tenant, display eligibility, active status, and address-display validation on reads and writes.
- Bounded current-state listing-summary endpoint with a 20-key limit and no Bridge call in the serving path.
- Compare controls on cards and listing detail, responsive Compare tray, and contextual Residential/Rental/Land/Commercial comparison modal.
- Compare changes excluded from the REALTOR® Client Activity timeline.
- Activity metadata allowlisted by event type before serialization. Unknown fields are discarded; every stored non-null value is complete valid JSON within the 2KB ceiling.

## Database and deployment

- `0031_sneak_consumer_compare.sql`: applied remotely; reference-only compare table, unique user/site/key constraint, indexes, and maximum-four trigger.
- `0032_sneak_site_fk_compatibility.sql`: applied remotely after detecting an empty legacy staging schema with consumer foreign keys targeting `sneak_sites(site_id)` instead of `sneak_sites(id)`. The migration is guarded to abort when consumer data exists.
- Remote migration list: no pending migrations.
- Remote `PRAGMA foreign_key_check`: zero violations.
- Remote invalid `metadata_json` rows: zero.
- Staging Worker versions:
  - Serving: `4b716682-16eb-4f68-845e-a8052932e37a`
  - Consumer: `c118ac48-c864-4f9a-bf4f-7351357bf80a`
  - Member: `77674e20-75fb-47dd-b1ce-f0ff3d29ed29`
  - Alerts: `07059051-aa45-461a-847d-e669c365df17`

## Verification evidence

- Automated regression: **149/149 passed** across 10 suites.
- C3A live endpoint verification: **12/12 passed**.
- Real MLS staging/IDX acceptance: **44/44 passed** across bootstrap, session enforcement, search, map, category filters, listing detail/media, agent/office scoping, scope escape prevention, and open houses.
- Authenticated cross-session API verification: PASS. Compare written from isolated session A was visible from session B; a `listing_view` recorded from session A appeared in Recently Viewed from session B.
- Activity isolation: zero Compare activity events for the authenticated QA user.
- QA data cleanup: isolated user and both sessions removed; zero synthetic QA users remain.
- Anonymous privacy automation: PASS. There is no anonymous Recently Viewed upload route or anonymous consumer tracking call; only site-scoped keys/timestamps are retained, and no full listing records are stored.
- Protected legacy zero-diff: PASS for `home-search/index.html`, `ListingsWorker.js`, and `wrangler.toml`.
- Mailjet/signing state: `mailjetConfigured=false`, `signingSecretConfigured=false`, `deliveryReady=false`; alert delivery remains fail-closed.

## Required browser sign-off still pending

The in-app browser runtime reported that no browser was connected. Static tests, HTTP checks, and API probes are not recorded as browser verification.

### Phase 7.3C2B Member Portal

Run authenticated QA at 1440×900, 1024×768, and 390×844 for Clients navigation, KPI cards, search/sort, Client Detail, Saved Homes, Saved Searches, Activity Timeline, Inquiries, responsive behavior, and clipping/overflow.

### Phase 7.3C3A consumer experience

Run anonymous and authenticated QA at the same three viewports for Recently Viewed, card/detail Compare actions, maximum-four behavior, tray, removal/clear, all four contextual comparison modes, local-to-server Compare merge, cross-device authenticated persistence, anonymous storage privacy, and clipping/overflow.

C3A must not be marked complete until both browser passes are recorded with an actual connected browser.
