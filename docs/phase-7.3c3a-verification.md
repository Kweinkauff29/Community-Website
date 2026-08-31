# Phase 7.3C3A.1 Verification Report

Date: 2026-08-31

Build: `2026.08.31.7.3c3a1`

Status: **COMPLETE / DEPLOYED / AUTOMATED, LIVE API, AND BROWSER QA PASS**

Phase 7.3C3B remains **PLANNED / NOT STARTED**.

## Corrective scope

- Authenticated Recently Viewed returns at most 20 unique eligible listings from `listing_view` events in the last 90 days. It fetches up to 40 candidates before current-state filtering, keeps the latest duplicate view, and sorts newest first.
- Anonymous Recently Viewed retains at most 20 unique entries for 30 days in site-scoped local storage. Malformed and expired entries are pruned and the cleaned list is rewritten. Each item contains only `{ key, viewedAt }`.
- Logging out preserves local device history. Signing in never uploads anonymous Recently Viewed history.
- Activity metadata is sanitized by event type before serialization. Unknown fields are discarded, `weekly` is not an accepted alert frequency, every non-null value is complete valid JSON, and the UTF-8 payload remains at or below 2KB without truncating serialized JSON.
- Existing Compare behavior remains intact: anonymous local persistence, authenticated D1 persistence, local-to-server merge, cross-device persistence, a four-property maximum, reference-only storage, current tenant/display/active-state validation, and no Compare events in the REALTOR® activity timeline.
- Listing summaries remain bounded to 20 keys, contain only current eligible listing state, and make no Bridge request in the consumer-serving path.

## Migration 0032 safety

The original `0032_sneak_site_fk_compatibility.sql` was an empty-staging repair for a historical schema whose consumer foreign keys targeted `sneak_sites(site_id)`. Its zero-row guard made it unsuitable as an automatic migration in a populated environment, including a healthy canonical environment.

- Wrangler/D1 tracking was verified against the live `d1_migrations` table and Wrangler 4.127.1 source. Applied migrations are matched by migration filename/name and record an applied timestamp; there is no checksum or content hash.
- The `0032_sneak_site_fk_compatibility.sql` filename is retained as a safe no-op compatibility marker. Staging therefore does not rerun the historical repair, and fresh/current canonical environments do not rebuild consumer tables.
- The original repair is preserved at `scripts/sql/repair-legacy-sneak-site-fks-empty.sql`, clearly labeled as a manual legacy empty-environment operation.
- `scripts/check-sneak-site-fk-compatibility.mjs` performs read-only schema and row-count inspection:
  - canonical: exits 0 with `PASS — NO REPAIR REQUIRED`;
  - legacy and empty: exits 2 and points to the manual empty-schema repair;
  - legacy and populated: exits 3 with `STOP — DATA-PRESERVING MANUAL MIGRATION REQUIRED`;
  - unrecognized schema: exits 4 and fails closed.
- The live staging preflight classified the schema as canonical with zero consumer rows.
- A fresh temporary D1 applied the complete 0001–0032 chain. A populated canonical fixture then executed the 0032 marker without losing data, and its foreign key still targeted `sneak_sites(id)`.
- The remote migration list has no pending migrations. Remote invalid activity metadata JSON rows and QA fixture rows are zero.

The operational procedure is documented in `docs/operations/sneak-site-fk-compatibility.md`.

## Automated and live verification

- Full automated regression: **157/157 passed**, 0 failed, across 10 suites.
- C3A live endpoint verification: **12/12 passed**.
- Real MLS/IDX staging regression: **44/44 passed**.
- Authenticated API/session regression: PASS.
- Anonymous privacy automation: PASS.
- Protected legacy comparison to `origin/main`: ZERO DIFF for `ListingsWorker.js`, `home-search/index.html`, `open-house/index.html`, and `wrangler.toml`.

## Actual browser QA

Browser verification used installed Playwright 1.59.1 with Chromium 1200 after the integrated browser reported no connected runtime. It used isolated authenticated staging fixtures and did not weaken authentication. All fixtures and sessions were removed after QA and an exact-ID cleanup query returned zero remaining rows.

### Phase 7.3C2B Member Portal

PASS at 1440×900, 1024×768, and 390×844 with an actual authenticated session:

- Clients navigation, four KPI cards, client search, saved-search/saved-home sorting, client roster, and Client Detail all worked.
- The detail view rendered two Saved Homes, one Saved Search with a Daily alert, five Activity Timeline events, and an Inquiry with phone and message.
- Modal scrolling and mobile navigation/layout worked. Page scroll width equaled viewport width at all three sizes; no clipping or horizontal overflow occurred.
- Browser QA exposed a malformed saved-home image fallback handler. It was corrected and all three viewports reran with zero page errors.

### Phase 7.3C3A public IDX

PASS on the live pilot and signed app at desktop 1440×900, tablet 1024×768, and mobile 390×844:

- Desktop covered footer position, search, listing detail, card/detail Compare controls, tray, two/three/four-property comparisons, fifth-property rejection, removal, clear, Recently Viewed cleanup to 20, favorites, Save Search authentication prompt, and map behavior.
- Residential, Rental, Land, and Commercial comparison modes rendered their contextual specifications.
- Tablet covered detail, Compare tray/modal, Recently Viewed, and List/Map behavior with no app overflow or page errors.
- Mobile covered physically clickable Compare controls, tray, horizontally scrollable comparison content, readable Recently Viewed, unobstructed List/Map controls, and map switching. A mobile flex-basis layout issue found during QA was corrected; the rerun had no app or host-page horizontal overflow and no page errors.
- The pilot embed width matched its container at all three sizes. The desktop WordPress host itself retained an unrelated 18px page overflow outside the iframe; the IDX embed and app did not overflow.

### Authenticated cross-device browser QA

PASS with independent browser contexts/sessions A and B for the same isolated buyer and site:

- Session A signed in, merged an eligible local Compare key, selected a second property, and viewed another listing.
- Session B displayed the same two server-persisted Compare properties and the authenticated Recently Viewed result.
- Session B had no anonymous Compare storage. No Compare timeline events were created.
- The database contained zero retroactive `listing_view` events for A/B/C from session A's pre-login anonymous local history.

### Anonymous privacy browser QA

PASS:

- Recently Viewed storage contained only arrays of `{ key, viewedAt }`; Compare storage contained listing keys/order only.
- No full listing snapshots were stored.
- Anonymous browsing made no request to the Consumer Worker and posted zero activity events.
- No anonymous Recently Viewed history was uploaded on authentication.
- Ordinary logout preserves site-scoped device history; account deletion still clears it.

## Deployment

- Serving Worker: `b4d6fc90-ee1e-4900-9ddd-3208cb959df2`
- Consumer Worker: `6fe150ab-e5cb-43ad-826d-e088abd499e4`
- Member Worker: `969f3d8d-3f2a-4739-90fa-0ababd33854f`
- Alert Worker: unchanged at the prior C3A build because alert runtime code did not change.
- Current C3A.1 build is live in Serving/search UI, embed, Consumer, and Member.

## Alert delivery status

- Mailjet configured: **NO**
- Signing secret configured: **NO**
- Delivery ready: **NO**
- Real email delivery: **NO**

The Alert Worker remains fail-closed. No secret values are recorded here.
