# IDX contacts and notifications

Member dashboard: open **Contacts & Email**. Administrators open an account and select **Contacts & Email**. The list combines inquiries and verified buyer accounts by email within each website. Contact details include inquiries, sign-ins, viewed properties, saved-home/search activity, and recent email delivery status. Owner/admin roles may edit notification settings; viewers may read contacts.

The Embed Code builder includes **Contact & Sign-In Popup**, in addition to quick search, full map/search, featured listings, and listing grids. Copy its snippet into an authorized website. A visitor can inquire without creating an account, or request a passwordless sign-in link. Signup prompts can be disabled, opened by the visitor, or offered after a configurable number of property views. Automatic prompts are dismissible and shown once per browser session.

Notifications go to active or invited account owner/admin email addresses. First verified sign-in produces one signup notice. Each inquiry produces an inquiry notice. Weekly summaries default to Friday at 5 p.m. America/New_York; each website can change the day, hour, timezone, and notification toggles. Summaries identify only signed-in browsing and submitted inquiries. Anonymous views are stored locally, not attributed to people.

## Deployment

Apply migration `0036_sneak_contacts_notifications.sql` before deploying the serving/member workers. Existing contacts are backfilled without historical signup/inquiry emails. Weekly delivery starts with the first scheduled period ending after the settings row was created.

Production email workers currently set `EMAIL_BCC = "tech@berealtors.org"` for temporary delivery monitoring, including sign-in links, inquiries, and summaries. Remove this variable to stop the copies. BCC is added only at the final Mailjet send, including relayed consumer messages; a message already addressed to that mailbox is not duplicated.

The member worker reuses its configured Mailjet credentials and verified sender. Consumer authentication calls it through the `MAILER` service binding. Both workers need the same random `SNEAK_MAILER_SECRET` (a Cloudflare secret, never an embed attribute). The member worker runs the owner queue every five minutes. Failed temporary sends retry up to five times; failed or canceled delivery status is shown in the dashboard. Account/site suspension and expired entitlements stop notification delivery. Current recipient roles and notification toggles are checked again before each send.

The internal `/internal/email` endpoint requires that shared secret. An internal request with `sandbox: true` performs Mailjet validation without delivery and returns `validated`, never `sent`. Production sends require a provider message ID. Do not put the shared secret or Mailjet keys in client code.

`/portal?site=SITE_KEY&signin=1` provides a secure hosted account/search destination. It enforces site status and service entitlement. Email return URLs allow only verified HTTPS domains or the exact hosted portal route for the same site. Ursula's HTTP website uses this secure portal for sign-in until its custom-domain HTTPS certificate is ready.

Recently Viewed is a separate section below the map/listings. Embedded results preserve at least 420 pixels for the map/listings when that section is shown, including on phones.

## Verification

- `node --test test/sneak-contacts.test.mjs` exercises the full SQLite migration chain, tenant isolation, actual member-session write authorization, magic-link/exchange lifecycle, email outbox, retry cancellation, timezone/DST scheduling, lead validation/rate limits, and withheld-address handling.
- `node scripts/qa-contact-browser.cjs --local-assets` checks Chromium and WebKit at desktop/phone widths; form submissions and sign-in email requests are intercepted. Omit the flag after deployment to verify live assets.
- `node scripts/qa-quick-search.cjs` verifies live search dropdowns and redirects.

### Inquiry details and member sign-in

Property inquiry emails resolve the listing within the site’s IDX scope and include the permitted address, MLS number, price, beds, baths, area, status and a secure property link. Suppressed or unavailable listings use a reference fallback. Address display restrictions still apply. The hosted portal opens the linked property directly.

Member magic-link GET requests only display a confirmation page; a same-origin POST consumes the token and redirects to the dashboard. Login resends preserve earlier unexpired links, while invitations retain rotation. Rate limits use normalized SQLite timestamps for the 15-minute window. Mailjet dispatch is immediate, reports failure to the UI and records dispatch duration without tokens or recipient addresses. Authentication messages disable click/open tracking; production BCC settings remain applied. Inbox arrival time depends on the email providers.

Inquiry submissions and verified buyer signups dispatch their durable outbox event immediately through the private `OwnerNotifier` service binding. The member worker cron remains the fallback for failed dispatch and weekly scheduling; public requests cannot invoke the notification entrypoint. Per-event processing preserves entitlement, tenant settings, recipient checks and atomic delivery claims, and does not enqueue weekly digests.
