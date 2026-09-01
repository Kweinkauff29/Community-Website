# Transactional Email Operations

Phase 7.4B1 uses one fail-closed Mailjet adapter in `sneak-shared/email-provider.js` for Member sign-in/invitation, Consumer sign-in, and Saved Search alerts. There is no simulated-success runtime path.

## Provider contract

A send is successful only when all of the following are true:

1. both Mailjet credentials are configured;
2. the Mailjet request returns a successful HTTP status;
3. the first Mailjet message reports `Status: success`; and
4. Mailjet supplies a message ID or UUID.

A 2xx response containing a message-level error is a failed delivery. Missing configuration returns `provider_unconfigured`; HTTP 429 and 5xx/network failures are retryable; other provider rejections are non-retryable. Provider response bodies are bounded to 64 KB. Logs, API responses, readiness records, and tests must never contain secret values or raw magic-link tokens.

## Staging secret names

Use the normal Cloudflare secret process separately for each Worker. Never copy a value into a command history, source file, test, ticket, or this document.

| Worker | Required secrets | Non-secret sender configuration |
| --- | --- | --- |
| Member | `MAILJET_API_KEY`, `MAILJET_SECRET_KEY` | `EMAIL_FROM` |
| Consumer | `MAILJET_API_KEY`, `MAILJET_SECRET_KEY` | `EMAIL_FROM` |
| Alerts | `MAILJET_API_KEY`, `MAILJET_SECRET_KEY`, `SNEAK_ALERT_UNSUBSCRIBE_SECRET` | `EMAIL_FROM` |

After an authorized operator supplies values interactively, deploy only the affected staging Worker and confirm its health endpoint. Configuration is not delivery proof.

## Readiness evidence

Email capability status is intentionally separate:

- Member magic-link email requires a controlled-inbox receipt, successful link consumption, established session, replay rejection, and expired-token rejection.
- Consumer magic-link email requires controlled-inbox receipt, successful site-scoped exchange/session, replay rejection, and expired-token rejection.
- Saved Search alerts require controlled-inbox ASAP and Daily delivery, claim-before-send behavior, no duplicate send, a valid signed unsubscribe, and confirmation that unsubscribe disables the alert.

Record only timestamps, check source, provider message identifier where operationally necessary, and a short outcome. Do not store message bodies, magic links, tokens, recipient credentials, or provider secrets. Launch-check keys are `member_magic_link_e2e`, `consumer_magic_link_e2e`, `alerts_asap_e2e`, `alerts_daily_e2e`, and `alerts_unsubscribe_e2e`; a pass requires source `controlled_inbox`.

## Failure and retry behavior

- Member and Consumer public request responses remain generic and never reveal account existence, tokens, or provider outcome.
- Alert rows are marked sent only after the strict provider success contract passes.
- Missing provider/signing configuration, provider rejection, or retryable failure leaves alert candidates unsent and retryable.
- Claims are bounded and recover after expiry; concurrent cron runs cannot double-claim the same candidate.
- Unsubscribe signing fails closed when the signing secret is missing or too short.

## Current staging state — 2026-09-01

| Capability | Provider configured | Real controlled inbox | Action consumed | Readiness |
| --- | --- | --- | --- | --- |
| Member magic link | Yes | Not verified in this phase | Not verified | `NOT_VERIFIED` |
| Consumer magic link | No | Not attempted | Not attempted | `NOT_READY` |
| ASAP alert | No; signing secret also absent | Not attempted | N/A | `NOT_READY` |
| Daily alert | No; signing secret also absent | Not attempted | N/A | `NOT_READY` |
| Unsubscribe | Signing secret absent | N/A | Not attempted | `NOT_READY` |

These statuses must remain fail closed until an authorized operator supplies the missing secrets and a controlled mailbox is available. Do not send QA messages to real members.
