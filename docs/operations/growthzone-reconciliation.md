# GrowthZone Entitlement Reconciliation

Phase 7.4B1 adds a bounded, idempotent GrowthZone reconciliation process to the Admin Worker. GrowthZone is an upstream business-system input; `sneak_account_entitlements` remains the sole MVP serving authority. The public Serving Worker remains D1-only and never calls GrowthZone or Bridge.

## Audited integration surface

GrowthZone's curated API uses `Authorization: ApiKey <key>`. Phase 7.4B1 reads one of these contact-membership endpoints:

- `/api/contacts/person/{ContactId}/memberships`
- `/api/contacts/org/{ContactId}/memberships`

The available membership representation includes membership ID/type, status, start date, expiration date, grace-period expiration, inactive state, and in-grace state. No repository integration or authenticated GrowthZone secret existed before this phase. The public documentation reviewed for this work did not establish a safe request-rate allowance, so reconciliation is sequential, daily, and bounded to 25 accounts by default (hard maximum 50).

## Durable identity

Automatic mutation requires `source = growthzone` and one explicit canonical `external_reference`:

- `person:<contactId>:membership:<membershipId>`
- `org:<contactId>:membership:<membershipId>`

The membership segment may be omitted only when the remote contact has exactly one membership. Display name and email are never automatic mutation keys. Missing, malformed, zero-match, or multi-match references become `mapping_ambiguous` and preserve the canonical entitlement.

## Status mapping

| GrowthZone membership state | Canonical status |
| --- | --- |
| `IsInGracePeriod = true` | `grace` |
| `IsInactive = true` | `canceled` |
| Active (2), Trial (3), Courtesy (4) | `active` |
| Pending Approval (1), Lead (7), Prospect (8), Suspended (9) | `suspended` |
| NonMember (0), Dropped (5), Expired (6) | `canceled` |
| Explicit Grace / In Grace Period | `grace` |
| Explicit Delinquent | `delinquent` |
| Unknown or contradictory value | invalid response; no mutation |

Balance is not interpreted as delinquency. Membership type changes the plan only through an explicit non-secret `GROWTHZONE_PLAN_MAP_JSON`; absent a mapping, the current plan is preserved.

## Reconciliation behavior

For each eligible account the process reads the current canonical entitlement, fetches the explicitly mapped membership, normalizes only known fields, compares the result, and updates `sneak_account_entitlements` only after a valid response. A successful verification advances `last_verified_at`, including an idempotent no-change check. `updated_at` and `last_changed_at` advance only for a real canonical difference.

State is recorded in `sneak_growthzone_reconciliation` as `verified_no_change`, `entitlement_changed`, `mapping_ambiguous`, `manual_override`, `remote_unavailable`, `invalid_response`, `not_configured`, or `never`. The bounded `sneak_admin_audit` record contains an outcome summary, never a credential or full remote payload.

Network failure, timeout, rate limiting, provider outage, malformed response, or missing configuration never suspends an account and never advances `last_verified_at`. Staff see the failure/staleness and retain the last valid canonical state. A GrowthZone-sourced record older than 36 hours after its last successful verification is `VERIFICATION_STALE`.

## Manual override

`source = manual` is a protected operator override. Scheduled, bulk, and single-account reconciliation skip it, record `manual_override`, and make no entitlement change. Staff must explicitly change the source back to `growthzone` before automation can resume.

## Operations

- Single account: Admin Account Detail → GrowthZone Reconciliation → Reconcile Now.
- Bounded batch: Admin Launch Readiness → Reconcile GrowthZone.
- Schedule: daily at 11:15 UTC (`15 11 * * *`), maximum 25 by current staging configuration.
- Required secret: `GROWTHZONE_API_KEY` on the Admin Worker.
- Non-secret configuration: `GROWTHZONE_BASE_URL`, `GROWTHZONE_PLAN_MAP_JSON`, `GROWTHZONE_BATCH_LIMIT`.
- Evidence gate: `growthzone_reconciliation_e2e` requires source `real_growthzone` and a successful authenticated response through an explicit mapping.

Current staging health reports `growthZoneConfigured: false` because the API key has not been supplied. Reconcile Now was browser-verified to return a fail-closed not-configured state while preserving the active canonical entitlement.
