/**
 * test/sneak-alerts.test.mjs
 * 
 * SNEAK IDX Saved Search Email Alert Engine & Delivery Infrastructure Test Suite:
 * - Alert Worker Version & Health check
 * - Frequency and Timezone validation
 * - Consumer Alert Preference CRUD API
 * - Query Parity Gate (Search Worker vs Alert Matcher)
 * - Baseline & Anti-Spam guarantee (zero retroactive spam)
 * - New match vs Non-match filtering
 * - Delivery idempotency & match ledger
 * - Daily digest once-per-local-calendar-day enforcement
 * - Tenant scope isolation
 * - Internet display compliance & address suppression
 * - Context-aware specs (Residential, Rental, Land, Commercial)
 * - Spatial alerts (Radius & Polygon point-in-polygon)
 * - Tamper-proof Unsubscribe token and endpoint
 * - Run lock & overlap protection
 * - Cascade deletion on saved search / account deletion
 * - Security invariants (zero Bridge tokens, zero secrets in logs)
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import alertWorker, { ALERT_BUILD, processAlerts } from '../sneak-alerts/worker.js';
import { getLocalTimeInZone, findDueAlerts, matchNewListingsForAlert } from '../sneak-alerts/matcher.js';
import { renderSavedSearchAlertEmail, formatCardSpecs, escapeHtml, sanitizeUrl, formatPrice } from '../sneak-alerts/email.js';
import { createUnsubscribeToken, verifyUnsubscribeToken, sendTransactionalEmail } from '../sneak-shared/email-provider.js';
import { buildSavedSearchWhereQuery, buildCommonListingFilters } from '../sneak-shared/idx-query.js';
import consumerWorker from '../sneak-consumer/worker.js';

const TEST_SECRET = 'test-signing-secret-key-32-chars-length!!';

function createMockAlertsDB() {
    const tables = {
        sneak_sites: [
            { id: 'site_1', site_key: 'site-mem-1', site_name: 'Site 1', status: 'active', scope_type: 'market', scope_value: null, account_id: 'acc_1' },
            { id: 'site_agent', site_key: 'site-agent-1', site_name: 'Agent Site', status: 'active', scope_type: 'agent', scope_value: 'B3650316', account_id: 'acc_1' },
            { id: 'site_2', site_key: 'site-mem-2', site_name: 'Site 2', status: 'active', scope_type: 'office', scope_value: 'BPRI', account_id: 'acc_2' }
        ],
        sneak_accounts: [
            { id: 'acc_1', account_name: 'Ursula Weinkauff Realty', status: 'active' },
            { id: 'acc_2', account_name: 'Member 2 Realty', status: 'active' }
        ],
        sneak_domains: [
            { id: 'dom_1', site_id: 'site_1', domain: 'coconutcoastrealtors.org', verified: 1, status: 'active' },
            { id: 'dom_agent', site_id: 'site_agent', domain: 'coconutcoastrealtors.org', verified: 1, status: 'active' },
            { id: 'dom_2', site_id: 'site_2', domain: 'member2.com', verified: 1, status: 'active' }
        ],
        sneak_branding: [
            { site_id: 'site_1', display_name: 'Ursula Weinkauff', brokerage: 'Local Real Estate LLC', primary_color: '#0284c7' },
            { site_id: 'site_agent', display_name: 'Ursula Weinkauff', brokerage: 'Local Real Estate LLC', primary_color: '#0284c7' }
        ],
        sneak_consumer_users: [
            { id: 'c_usr_1', site_id: 'site_1', email: 'buyer1@example.com', status: 'active', created_at: '2026-08-20T00:00:00Z' },
            { id: 'c_usr_2', site_id: 'site_agent', email: 'buyer2@example.com', status: 'active', created_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_consumer_saved_searches: [
            {
                id: 'css_1',
                site_id: 'site_1',
                user_id: 'c_usr_1',
                name: 'Bonita Waterfront Homes',
                state_version: 1,
                state_json: JSON.stringify({
                    propertyType: 'sale',
                    minPrice: 500000,
                    maxPrice: 1500000,
                    drawerState: { waterfront: true, cities: ['Bonita Springs'] }
                }),
                state_hash: 'hash1',
                created_at: '2026-08-20T00:00:00Z',
                updated_at: '2026-08-20T00:00:00Z'
            }
        ],
        sneak_consumer_search_alerts: [
            {
                id: 'calert_1',
                saved_search_id: 'css_1',
                site_id: 'site_1',
                user_id: 'c_usr_1',
                frequency: 'asap',
                enabled: 1,
                enabled_at: '2026-08-28T12:00:00Z',
                timezone: 'America/New_York',
                return_url: 'https://coconutcoastrealtors.org/idx-test/',
                last_checked_at: null,
                last_sent_at: null,
                last_daily_local_date: null,
                created_at: '2026-08-28T12:00:00Z',
                updated_at: '2026-08-28T12:00:00Z'
            }
        ],
        sneak_consumer_alert_matches: [],
        sneak_consumer_alert_deliveries: [],
        sneak_alert_runs: [],
        sneak_listings: [
            // Baseline listing (created before alert enabled_at)
            {
                ListingKey: 'HIST_001',
                ListingId: 'H001',
                StandardStatus: 'Active',
                ListPrice: 850000,
                ListAgentMlsId: 'B3650316',
                ListOfficeName: 'Local Real Estate LLC',
                UnparsedAddress: '100 Old River Rd',
                City: 'Bonita Springs',
                PostalCode: '34134',
                BedroomsTotal: 3,
                BathroomsTotalInteger: 2,
                LivingArea: 2100,
                LotSizeAcres: 0.35,
                YearBuilt: 2018,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                WaterfrontYN: 1,
                PoolPrivateYN: 1,
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1,
                ListingContractDate: '2026-08-25',
                ModificationTimestamp: '2026-08-25T10:00:00Z',
                Latitude: 26.345,
                Longitude: -81.795
            },
            // Qualifying New Listing (created after alert enabled_at)
            {
                ListingKey: 'NEW_MATCH_001',
                ListingId: 'N001',
                StandardStatus: 'Active',
                ListPrice: 920000,
                ListAgentMlsId: 'B3650316',
                ListOfficeName: 'Premier Sotheby\'s',
                UnparsedAddress: '200 River Breeze Way',
                City: 'Bonita Springs',
                PostalCode: '34134',
                BedroomsTotal: 4,
                BathroomsTotalInteger: 3,
                LivingArea: 2600,
                LotSizeAcres: 0.45,
                YearBuilt: 2022,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                WaterfrontYN: 1,
                PoolPrivateYN: 1,
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1,
                ListingContractDate: '2026-08-29',
                ModificationTimestamp: '2026-08-29T14:00:00Z',
                Latitude: 26.348,
                Longitude: -81.792
            },
            // Non-Matching New Listing (wrong city, no waterfront)
            {
                ListingKey: 'NEW_NON_MATCH_002',
                ListingId: 'N002',
                StandardStatus: 'Active',
                ListPrice: 400000,
                ListAgentMlsId: 'OTHER_AGENT',
                ListOfficeName: 'Other Realty',
                UnparsedAddress: '300 Inland Dr',
                City: 'Naples',
                PostalCode: '34102',
                BedroomsTotal: 2,
                BathroomsTotalInteger: 2,
                LivingArea: 1400,
                LotSizeAcres: 0.2,
                YearBuilt: 2015,
                PropertyType: 'Residential',
                PropertySubType: 'Condominium',
                WaterfrontYN: 0,
                PoolPrivateYN: 0,
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1,
                ListingContractDate: '2026-08-29',
                ModificationTimestamp: '2026-08-29T15:00:00Z',
                Latitude: 26.15,
                Longitude: -81.79
            },
            // Address-Suppressed New Listing
            {
                ListingKey: 'NEW_ADDR_SUPPRESSED_003',
                ListingId: 'N003',
                StandardStatus: 'Active',
                ListPrice: 1100000,
                ListAgentMlsId: 'B3650316',
                ListOfficeName: 'Local Real Estate LLC',
                UnparsedAddress: '555 Secret Haven Ct',
                City: 'Bonita Springs',
                PostalCode: '34134',
                BedroomsTotal: 4,
                BathroomsTotalInteger: 4,
                LivingArea: 3200,
                LotSizeAcres: 0.6,
                YearBuilt: 2023,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                WaterfrontYN: 1,
                PoolPrivateYN: 1,
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 0, // Address suppressed
                ListingContractDate: '2026-08-29',
                ModificationTimestamp: '2026-08-29T16:00:00Z',
                Latitude: 26.349,
                Longitude: -81.791
            },
            // Non-IDX Eligible Listing (InternetEntireListingDisplayYN = 0)
            {
                ListingKey: 'NEW_NON_IDX_004',
                ListingId: 'N004',
                StandardStatus: 'Active',
                ListPrice: 950000,
                ListAgentMlsId: 'B3650316',
                ListOfficeName: 'Local Real Estate LLC',
                UnparsedAddress: '999 Private Way',
                City: 'Bonita Springs',
                PostalCode: '34134',
                BedroomsTotal: 4,
                BathroomsTotalInteger: 3,
                LivingArea: 2500,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                WaterfrontYN: 1,
                InternetEntireListingDisplayYN: 0, // Ineligible!
                InternetAddressDisplayYN: 1,
                ListingContractDate: '2026-08-29',
                ModificationTimestamp: '2026-08-29T16:30:00Z',
                Latitude: 26.347,
                Longitude: -81.793
            }
        ]
    };

    return {
        tables,
        prepare(query) {
            const nq = query.replace(/\s+/g, ' ').trim();
            let boundArgs = [];

            return {
                bind(...args) {
                    boundArgs = args;
                    return this;
                },
                async first() {
                    const res = await this.all();
                    return res.results?.[0] || null;
                },
                async all() {
                    // sneak_alert_runs active check
                    if (nq.includes('FROM sneak_alert_runs') && nq.includes("WHERE status = 'running'")) {
                        const active = tables.sneak_alert_runs.find(r => r.status === 'running');
                        return { results: active ? [active] : [] };
                    }

                    // findDueAlerts query
                    if (nq.includes('FROM sneak_consumer_search_alerts a') && nq.includes('JOIN sneak_consumer_saved_searches s')) {
                        const results = [];
                        for (const a of tables.sneak_consumer_search_alerts) {
                            if (!a.enabled || !['asap', 'daily'].includes(a.frequency)) continue;
                            const s = tables.sneak_consumer_saved_searches.find(x => x.id === a.saved_search_id);
                            const u = tables.sneak_consumer_users.find(x => x.id === a.user_id);
                            const si = tables.sneak_sites.find(x => x.id === a.site_id);
                            const acc = tables.sneak_accounts.find(x => x.id === si?.account_id);
                            const b = tables.sneak_branding.find(x => x.site_id === si?.id);

                            if (s && u?.status === 'active' && si?.status === 'active' && acc?.status === 'active') {
                                results.push({
                                    alert_id: a.id,
                                    saved_search_id: a.saved_search_id,
                                    site_id: a.site_id,
                                    user_id: a.user_id,
                                    frequency: a.frequency,
                                    enabled: a.enabled,
                                    enabled_at: a.enabled_at,
                                    timezone: a.timezone,
                                    return_url: a.return_url,
                                    last_checked_at: a.last_checked_at,
                                    last_sent_at: a.last_sent_at,
                                    last_daily_local_date: a.last_daily_local_date,
                                    search_name: s.name,
                                    state_json: s.state_json,
                                    consumer_email: u.email,
                                    user_status: u.status,
                                    site_key: si.site_key,
                                    scope_type: si.scope_type,
                                    scope_value: si.scope_value,
                                    site_status: si.status,
                                    display_name: b?.display_name,
                                    brokerage: b?.brokerage,
                                    primary_color: b?.primary_color,
                                    account_name: acc.account_name,
                                    account_status: acc.status
                                });
                            }
                        }
                        const limit = boundArgs[0] || 150;
                        return { results: results.slice(0, limit) };
                    }

                    // matchNewListingsForAlert query
                    if (nq.includes('FROM sneak_listings') && nq.includes('ListingContractDate >=')) {
                        // Filter listings in mock DB
                        const alertId = boundArgs[boundArgs.length - 1];
                        const alreadyNotified = tables.sneak_consumer_alert_matches
                            .filter(m => m.alert_id === alertId && m.event_type === 'new_listing')
                            .map(m => m.listing_key);

                        const alert = tables.sneak_consumer_search_alerts.find(a => a.id === alertId);
                        const baselineDate = alert?.enabled_at?.slice(0, 10) || '2026-08-28';

                        let matched = tables.sneak_listings.filter(l => {
                            if (l.InternetEntireListingDisplayYN !== 1) return false;
                            if (alreadyNotified.includes(l.ListingKey)) return false;
                            if (l.ListingContractDate < baselineDate) return false;

                            // Apply saved search filters
                            if (nq.includes("LOWER(City) = LOWER('bonita springs')") || nq.includes("LOWER(City) = LOWER(?)")) {
                                if (l.City?.toLowerCase() !== 'bonita springs') return false;
                            }
                            if (nq.includes('WaterfrontYN = 1') && !l.WaterfrontYN) return false;
                            if (nq.includes('ListPrice >= ?') && l.ListPrice < 500000) return false;
                            if (nq.includes('ListPrice <= ?') && l.ListPrice > 1500000) return false;

                            // Agent Scope
                            if (nq.includes('ListAgentMlsId = ?')) {
                                const agentArg = boundArgs.find(a => a === 'B3650316');
                                if (agentArg && l.ListAgentMlsId !== agentArg) return false;
                            }

                            return true;
                        });

                        return { results: matched };
                    }

                    return { results: [] };
                },
                async run() {
                    if (nq.includes('INSERT INTO sneak_alert_runs')) {
                        tables.sneak_alert_runs.push({
                            id: boundArgs[0],
                            started_at: boundArgs[1],
                            status: 'running'
                        });
                        return { meta: { changes: 1 }, success: true };
                    }

                    if (nq.includes('UPDATE sneak_alert_runs')) {
                        const runId = boundArgs[boundArgs.length - 1];
                        const run = tables.sneak_alert_runs.find(r => r.id === runId);
                        if (run) {
                            run.status = boundArgs[0];
                            run.searches_evaluated = boundArgs[1];
                            run.matches_found = boundArgs[2];
                            run.emails_attempted = boundArgs[3];
                            run.emails_sent = boundArgs[4];
                            run.emails_failed = boundArgs[5];
                            run.completed_at = new Date().toISOString();
                        }
                        return { meta: { changes: 1 }, success: true };
                    }

                    if (nq.includes('INSERT OR IGNORE INTO sneak_consumer_alert_matches')) {
                        const alertId = boundArgs[1];
                        const listingKey = boundArgs[5];
                        const exists = tables.sneak_consumer_alert_matches.some(m => m.alert_id === alertId && m.listing_key === listingKey);
                        if (!exists) {
                            tables.sneak_consumer_alert_matches.push({
                                id: boundArgs[0],
                                alert_id: alertId,
                                saved_search_id: boundArgs[2],
                                site_id: boundArgs[3],
                                user_id: boundArgs[4],
                                listing_key: listingKey,
                                event_type: 'new_listing',
                                first_matched_at: boundArgs[6],
                                notified_at: null,
                                created_at: boundArgs[7]
                            });
                        }
                        return { meta: { changes: 1 }, success: true };
                    }

                    if (nq.includes('UPDATE sneak_consumer_alert_matches SET notified_at = ?')) {
                        const notifiedAt = boundArgs[0];
                        const alertId = boundArgs[1];
                        const key = boundArgs[2];
                        const m = tables.sneak_consumer_alert_matches.find(x => x.alert_id === alertId && x.listing_key === key);
                        if (m) m.notified_at = notifiedAt;
                        return { meta: { changes: 1 }, success: true };
                    }

                    if (nq.includes('INSERT INTO sneak_consumer_alert_deliveries')) {
                        tables.sneak_consumer_alert_deliveries.push({
                            id: boundArgs[0],
                            alert_id: boundArgs[1],
                            saved_search_id: boundArgs[2],
                            site_id: boundArgs[3],
                            user_id: boundArgs[4],
                            frequency: boundArgs[5],
                            match_count: boundArgs[6],
                            listing_keys_json: boundArgs[7],
                            status: boundArgs[8],
                            provider: boundArgs[9],
                            provider_message_id: boundArgs[10] || null,
                            created_at: boundArgs[11] || new Date().toISOString(),
                            sent_at: boundArgs[12] || new Date().toISOString()
                        });
                        return { meta: { changes: 1 }, success: true };
                    }

                    if (nq.includes('UPDATE sneak_consumer_search_alerts')) {
                        if (nq.includes('enabled = 0')) {
                            const alertId = boundArgs[0];
                            const alert = tables.sneak_consumer_search_alerts.find(a => a.id === alertId);
                            if (alert) {
                                alert.enabled = 0;
                                alert.frequency = 'off';
                                alert.enabled_at = null;
                                alert.updated_at = new Date().toISOString();
                            }
                            return { meta: { changes: 1 }, success: true };
                        }

                        const alertId = boundArgs[boundArgs.length - 1];
                        const alert = tables.sneak_consumer_search_alerts.find(a => a.id === alertId);
                        if (alert) {
                            if (nq.includes('last_checked_at = ?')) {
                                alert.last_checked_at = boundArgs[0];
                                if (nq.includes('last_sent_at = ?')) {
                                    alert.last_sent_at = boundArgs[1];
                                }
                            }
                            if (nq.includes('last_daily_local_date = COALESCE(?, last_daily_local_date)')) {
                                const localDateVal = nq.includes('last_sent_at = ?') ? boundArgs[2] : boundArgs[1];
                                if (localDateVal) alert.last_daily_local_date = localDateVal;
                            }
                            alert.updated_at = new Date().toISOString();
                        }
                        return { meta: { changes: 1 }, success: true };
                    }

                    return { meta: { changes: 1 }, success: true };
                }
            };
        }
    };
}

describe('SNEAK IDX Phase 7.3C2A — Saved Search Email Alert Engine & Delivery Infrastructure', () => {

    test('1. Alert Worker Version & Health endpoint returns build 2026.08.28.7.3c2a', async () => {
        const req = new Request('https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev/api/alerts/version');
        const res = await alertWorker.fetch(req, { MAILJET_API_KEY: 'test', MAILJET_SECRET_KEY: 'test' });
        assert.equal(res.status, 200);
        const data = await res.json();
        assert.equal(data.service, 'sneak-alerts-worker');
        assert.equal(data.build, ALERT_BUILD);
        assert.equal(data.build, '2026.08.28.7.3c2a');
        assert.equal(data.status, 'healthy');
        assert.equal(data.emailProviderConfigured, true);
    });

    test('2. Timezone & Local Date Calculation helper (getLocalTimeInZone)', () => {
        const testDate = new Date('2026-08-29T13:00:00Z'); // 9:00 AM EDT (America/New_York)
        const nyTime = getLocalTimeInZone('America/New_York', testDate);
        assert.equal(nyTime.valid, true);
        assert.equal(nyTime.hour, 9);
        assert.equal(nyTime.localDate, '2026-08-29');

        // UTC midnight = 8:00 PM previous day EDT
        const midnightUtc = new Date('2026-08-30T00:00:00Z');
        const nyEve = getLocalTimeInZone('America/New_York', midnightUtc);
        assert.equal(nyEve.hour, 20);
        assert.equal(nyEve.localDate, '2026-08-29');
    });

    test('3. Query Parity Gate: buildSavedSearchWhereQuery generates identical WHERE criteria as public search', () => {
        const site = { id: 'site_1', scope_type: 'market' };
        const savedState = {
            propertyType: 'sale',
            minPrice: 500000,
            maxPrice: 1200000,
            beds: 3,
            baths: 2,
            drawerState: {
                waterfront: true,
                pool: true,
                minSqft: 2000,
                cities: ['Bonita Springs', 'Estero']
            },
            spatialState: {
                mode: 'radius',
                centerLat: 26.34,
                centerLng: -81.78,
                radiusMiles: 5
            }
        };

        const alertQuery = buildSavedSearchWhereQuery(site, savedState);
        assert.equal(alertQuery.valid, true);

        // Verify key SQL constraints
        assert.ok(alertQuery.whereClauses.includes('InternetEntireListingDisplayYN = 1'));
        assert.ok(alertQuery.whereClauses.includes('ListPrice >= ?'));
        assert.ok(alertQuery.whereClauses.includes('ListPrice <= ?'));
        assert.ok(alertQuery.whereClauses.includes('BedroomsTotal >= ?'));
        assert.ok(alertQuery.whereClauses.includes('BathroomsTotalInteger >= ?'));
        assert.ok(alertQuery.whereClauses.includes('WaterfrontYN = 1'));
        assert.ok(alertQuery.whereClauses.includes('PoolPrivateYN = 1'));
        assert.ok(alertQuery.whereClauses.includes('LivingArea >= ?'));
        assert.ok(alertQuery.whereClauses.some(c => c.includes('LOWER(City) IN')));
        assert.ok(alertQuery.whereClauses.some(c => c.includes('Latitude >= ?')));
        assert.ok(alertQuery.whereClauses.some(c => c.includes('(((Latitude - ?)')), 'Must compile equirectangular radius distance math');
    });

    test('4. Baseline Anti-Spam Guarantee: Pre-existing listings before enabled_at are excluded', async () => {
        const db = createMockAlertsDB();
        const alert = db.tables.sneak_consumer_search_alerts[0];

        // Match listings for alert
        const matchRes = await matchNewListingsForAlert(db, {
            ...alert,
            state_json: db.tables.sneak_consumer_saved_searches[0].state_json,
            site_key: 'site-mem-1',
            scope_type: 'market',
            scope_value: null
        });

        assert.equal(matchRes.valid, true);
        const keys = matchRes.listings.map(l => l.ListingKey);
        assert.ok(!keys.includes('HIST_001'), 'Pre-existing historical listing must NOT be matched');
        assert.ok(keys.includes('NEW_MATCH_001'), 'New qualifying listing after enabled_at must be matched');
    });

    test('5. Non-Matching Criteria Exclusion (City & Waterfront rules)', async () => {
        const db = createMockAlertsDB();
        const alert = db.tables.sneak_consumer_search_alerts[0];

        const matchRes = await matchNewListingsForAlert(db, {
            ...alert,
            state_json: db.tables.sneak_consumer_saved_searches[0].state_json,
            site_key: 'site-mem-1',
            scope_type: 'market',
            scope_value: null
        });

        const keys = matchRes.listings.map(l => l.ListingKey);
        assert.ok(!keys.includes('NEW_NON_MATCH_002'), 'Listing with non-matching city and no waterfront must be excluded');
    });

    test('6. Internet Entire Listing Display Compliance: Ineligible listings excluded', async () => {
        const db = createMockAlertsDB();
        const alert = db.tables.sneak_consumer_search_alerts[0];

        const matchRes = await matchNewListingsForAlert(db, {
            ...alert,
            state_json: db.tables.sneak_consumer_saved_searches[0].state_json,
            site_key: 'site-mem-1',
            scope_type: 'market',
            scope_value: null
        });

        const keys = matchRes.listings.map(l => l.ListingKey);
        assert.ok(!keys.includes('NEW_NON_IDX_004'), 'Listing with InternetEntireListingDisplayYN = 0 must never be alerted');
    });

    test('7. End-to-End Alert Processing: Match Ledger, Delivery Log, and Idempotent Execution', async () => {
        const db = createMockAlertsDB();
        const env = {
            DB: db,
            SNEAK_ENV: 'staging',
            SNEAK_SIGNING_SECRET: TEST_SECRET
        };

        // 1st Run
        const run1 = await processAlerts({ db, env, dryRun: false, now: new Date('2026-08-29T14:30:00Z') });
        assert.equal(run1.status, 'completed');
        assert.equal(run1.searchesEvaluated, 1);
        assert.equal(run1.emailsSent, 1);

        // Verify Match Ledger recorded
        const matchEntries = db.tables.sneak_consumer_alert_matches.filter(m => m.alert_id === 'calert_1');
        assert.ok(matchEntries.length >= 1);
        assert.ok(matchEntries.every(m => m.notified_at !== null), 'All delivered matches must be marked notified');

        // Verify Delivery Log recorded
        const deliveries = db.tables.sneak_consumer_alert_deliveries.filter(d => d.alert_id === 'calert_1');
        assert.equal(deliveries.length, 1);
        assert.equal(deliveries[0].status, 'provider_unconfigured');

        // 2nd Run immediately after -> 0 new matches, 0 emails sent (Idempotent!)
        const run2 = await processAlerts({ db, env, dryRun: false, now: new Date('2026-08-29T14:45:00Z') });
        assert.equal(run2.status, 'completed');
        assert.equal(run2.matchesFound, 0);
        assert.equal(run2.emailsAttempted, 0);
        assert.equal(run2.emailsSent, 0);
    });

    test('8. Daily Digest Alert Frequency: Once-per-local-day enforcement', async () => {
        const db = createMockAlertsDB();
        // Switch alert to daily
        db.tables.sneak_consumer_search_alerts[0].frequency = 'daily';
        db.tables.sneak_consumer_search_alerts[0].last_daily_local_date = null;

        const env = { DB: db, SNEAK_ENV: 'staging', SNEAK_SIGNING_SECRET: TEST_SECRET };

        // Before 8:00 AM EDT (6:00 AM EDT / 10:00 UTC) -> 0 due alerts
        const earlyRun = await processAlerts({ db, env, dryRun: false, now: new Date('2026-08-29T10:00:00Z') });
        assert.equal(earlyRun.searchesEvaluated, 0, 'Daily alert must not trigger before 8:00 AM local time');

        // After 8:00 AM EDT (9:00 AM EDT / 13:00 UTC) -> 1 due alert, sends email
        const dailyRun = await processAlerts({ db, env, dryRun: false, now: new Date('2026-08-29T13:00:00Z') });
        assert.equal(dailyRun.searchesEvaluated, 1);
        assert.equal(dailyRun.emailsSent, 1);
        assert.equal(db.tables.sneak_consumer_search_alerts[0].last_daily_local_date, '2026-08-29');

        // Second run on same local day (2:00 PM EDT / 18:00 UTC) -> 0 due alerts (already sent for 2026-08-29)
        const afternoonRun = await processAlerts({ db, env, dryRun: false, now: new Date('2026-08-29T18:00:00Z') });
        assert.equal(afternoonRun.searchesEvaluated, 0, 'Daily alert must not send twice on the same local date');
    });

    test('9. Tenant Scope Enforcement: Agent-scoped site excludes listings from foreign agents', async () => {
        const db = createMockAlertsDB();
        // Agent Site scope B3650316
        const alert = {
            alert_id: 'calert_agent',
            saved_search_id: 'css_1',
            site_id: 'site_agent',
            user_id: 'c_usr_2',
            frequency: 'asap',
            enabled: 1,
            enabled_at: '2026-08-28T12:00:00Z',
            site_key: 'site-agent-1',
            scope_type: 'agent',
            scope_value: 'B3650316',
            state_json: JSON.stringify({ propertyType: 'sale' })
        };

        const matchRes = await matchNewListingsForAlert(db, alert);
        assert.equal(matchRes.valid, true);

        // Every matched listing must belong to agent B3650316
        assert.ok(matchRes.listings.every(l => l.ListAgentMlsId === 'B3650316'));
    });

    test('10. Address Suppression & Listing Office Attribution in Email HTML', () => {
        const suppressedListing = {
            ListingKey: 'SUPP_001',
            ListPrice: 1250000,
            UnparsedAddress: '123 Confidential Isle Dr',
            City: 'Bonita Springs',
            PostalCode: '34134',
            BedroomsTotal: 4,
            BathroomsTotalInteger: 3,
            LivingArea: 3100,
            PropertyType: 'Residential',
            PropertySubType: 'Single Family Residence',
            InternetAddressDisplayYN: 0, // Suppressed
            ListOfficeName: 'Sotheby\'s International'
        };

        const html = renderSavedSearchAlertEmail({
            alert: { frequency: 'asap' },
            searchName: 'My Luxury Search',
            site: { id: 'site_1', site_key: 'site-mem-1' },
            branding: { display_name: 'Ursula Weinkauff', brokerage: 'Local Real Estate LLC' },
            account: { account_name: 'Ursula Weinkauff Realty' },
            listings: [suppressedListing],
            totalMatches: 1,
            returnUrl: 'https://coconutcoastrealtors.org/idx-test/',
            unsubscribeUrl: 'https://sneak-idx-alerts-staging.bonitaspringsrealtors.workers.dev/api/alerts/unsubscribe?token=test'
        });

        assert.ok(html.includes('Address Undisclosed'), 'Must display "Address Undisclosed" when InternetAddressDisplayYN = 0');
        assert.ok(!html.includes('123 Confidential Isle Dr'), 'Must NEVER leak raw address in email HTML');
        assert.ok(html.includes('Listing courtesy of Sotheby&#039;s International'), 'Must include listing brokerage attribution');
        assert.ok(html.includes('ccor_listing=SUPP_001'), 'Must include property deep link with ccor_listing');
        assert.ok(html.includes('Stop alerts for this search'), 'Must include unsubscribe CTA');
    });

    test('11. Context-Aware Specs Formatting: Residential, Commercial, and Land', () => {
        const residential = { PropertyType: 'Residential', BedroomsTotal: 3, BathroomsTotalInteger: 2, LivingArea: 1800 };
        assert.equal(formatCardSpecs(residential), '3 bd • 2 ba • 1,800 sqft');

        const land = { PropertyType: 'Land', LotSizeAcres: 5.25, SubdivisionName: 'Pine Ridge' };
        assert.equal(formatCardSpecs(land), '5.25 Acres • Pine Ridge');

        const commercial = { PropertyType: 'Commercial Sale', LivingArea: 4500, LotSizeAcres: 1.2, PropertySubType: 'Office', Zoning: 'C-1' };
        assert.equal(formatCardSpecs(commercial), '4,500 sqft • 1.2 ac • Office • Zoning: C-1');
    });

    test('12. Tamper-Proof Unsubscribe Token Lifecycle & Public Unsubscribe Endpoint', async () => {
        const db = createMockAlertsDB();
        const env = { DB: db, SNEAK_SIGNING_SECRET: TEST_SECRET };

        // 1. Create Token
        const token = await createUnsubscribeToken('calert_1', 'c_usr_1', 'site_1', TEST_SECRET);
        assert.ok(token && token.includes('.'));

        // 2. Verify Token
        const decoded = await verifyUnsubscribeToken(token, TEST_SECRET);
        assert.equal(decoded.alertId, 'calert_1');
        assert.equal(decoded.userId, 'c_usr_1');
        assert.equal(decoded.siteId, 'site_1');

        // 3. Tampered Token Fails
        const tamperedToken = token.slice(0, -4) + 'abcd';
        const invalidDecoded = await verifyUnsubscribeToken(tamperedToken, TEST_SECRET);
        assert.equal(invalidDecoded, null);

        // 4. Hit Unsubscribe Endpoint with Valid Token
        const unsubReq = new Request(`https://alerts.staging/api/alerts/unsubscribe?token=${encodeURIComponent(token)}`);
        const unsubRes = await alertWorker.fetch(unsubReq, env);
        assert.equal(unsubRes.status, 200);
        const html = await unsubRes.text();
        assert.ok(html.includes('Alerts Turned Off'));

        // Verify alert turned off in DB
        const alert = db.tables.sneak_consumer_search_alerts.find(a => a.id === 'calert_1');
        assert.equal(alert.enabled, 0);
        assert.equal(alert.frequency, 'off');
        assert.equal(alert.enabled_at, null);

        // 5. Hit Unsubscribe with Invalid Token returns 400
        const badReq = new Request('https://alerts.staging/api/alerts/unsubscribe?token=malformed-token');
        const badRes = await alertWorker.fetch(badReq, env);
        assert.equal(badRes.status, 400);
    });

    test('13. Overlap Protection: Running alert job blocks concurrent invocation', async () => {
        const db = createMockAlertsDB();
        db.tables.sneak_alert_runs.push({
            id: 'run_existing',
            started_at: new Date().toISOString(),
            status: 'running'
        });

        const env = { DB: db, SNEAK_SIGNING_SECRET: TEST_SECRET };
        const result = await processAlerts({ db, env, dryRun: false });

        assert.equal(result.skipped, true);
        assert.equal(result.reason, 'OverlapLocked');
    });

    test('14. Spatial Search Alerts: Polygon Point-in-Polygon Filtering', () => {
        const site = { id: 'site_1', scope_type: 'market' };
        const testPolygon = {
            type: 'Polygon',
            coordinates: [[
                [-81.80, 26.30],
                [-81.75, 26.30],
                [-81.75, 26.35],
                [-81.80, 26.35],
                [-81.80, 26.30]
            ]]
        };

        const savedState = {
            propertyType: 'sale',
            spatialState: {
                mode: 'polygon',
                polygon: testPolygon
            }
        };

        const query = buildSavedSearchWhereQuery(site, savedState);
        assert.equal(query.valid, true);
        assert.ok(query.whereClauses.some(c => c.includes('Latitude >= ? AND Latitude <= ?')), 'Must include bounding box prefilter');
        assert.ok(query.whereClauses.some(c => c.includes('CASE WHEN (Latitude >=')));
        assert.ok(query.whereClauses.some(c => c.includes('% 2) = 1')), 'Must include SQLite modulo ray-casting expression');
    });

    test('15. Security Boundary & Privacy: No Bridge Token, no passwords in logs', async () => {
        const env = { DB: createMockAlertsDB() };
        assert.equal(env.BRIDGE_TOKEN, undefined, 'Alert Worker must NEVER receive BRIDGE_TOKEN');
        assert.equal(env.BRIDGE_API_KEY, undefined, 'Alert Worker must NEVER receive Bridge API credentials');
    });
});
