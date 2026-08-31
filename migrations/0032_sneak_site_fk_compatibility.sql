-- SNEAK IDX Phase 7.3C3A.1: compatibility marker only.
--
-- Historical staging required a one-time repair because five early consumer
-- tables referenced sneak_sites(site_id) instead of the canonical
-- sneak_sites(id). That empty-environment repair was applied to staging before
-- this migration was converted to a marker. Wrangler records applied D1
-- migrations by name, so staging will not rerun this file.
--
-- The canonical migrations already reference sneak_sites(id), so fresh and
-- populated canonical environments require no schema mutation here. Operators
-- must run scripts/check-sneak-site-fk-compatibility.mjs before advancing a
-- legacy environment. The historical empty-schema repair is preserved at
-- scripts/sql/repair-legacy-sneak-site-fks-empty.sql and must never be run on a
-- populated database without an environment-specific, data-preserving plan.

SELECT 1 AS sneak_site_fk_compatibility_marker;
