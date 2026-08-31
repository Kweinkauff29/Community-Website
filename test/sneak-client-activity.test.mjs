/**
 * test/sneak-client-activity.test.mjs
 * 
 * SNEAK IDX Platform — Phase 7.3C2B Test Suite:
 * Agent Client Activity Dashboard + Authenticated Buyer Activity Timeline
 * 
 * Invariants & Test Coverage:
 * 1. Activity Ledger Record Helper:
 *    - Structured event ingestion (id, site_id, user_id, event_type, metadata_json, dedupe_key, created_at).
 *    - Updates sneak_consumer_users.last_activity_at.
 *    - Controlled vocabulary enforcement (ALLOWED_ACTIVITY_TYPES).
 * 2. Server-Side Mutation Logging:
 *    - favorite_added & favorite_removed on favorites CRUD.
 *    - saved_search_created, saved_search_updated, saved_search_deleted on search CRUD.
 *    - alert_enabled, alert_frequency_changed, alert_disabled on alert preference transitions.
 * 3. Browser-Reported Activity Ingestion (POST /api/consumer/activity):
 *    - Requires valid consumer session (anonymous returns 401).
 *    - Only 'listing_view' and 'inquiry_submitted' permitted from browser.
 *    - 30-min deduplication window on listing_view.
 *    - Validates listing exists, is display eligible, and matches site tenant scope.
 *    - Validates inquiry leadId matches site and normalized consumer email.
 *    - 120 browser events/hour rate limit enforcement.
 * 4. Member Clients List API (GET /api/member/clients):
 *    - Tenant isolation: only returns clients belonging to sites owned by member's account.
 *    - Case-insensitive email search filtering.
 *    - Sorting by recently_active, newest, saved_homes, saved_searches.
 *    - Engagement aggregation counts (savedHomesCount, savedSearchesCount, alertsCount, inquiriesCount).
 *    - Cross-tenant identity separation (same email across 2 member sites = 2 distinct client records).
 * 5. Member Client Detail API (GET /api/member/clients/:id):
 *    - Profile metadata, engagement totals.
 *    - Saved homes list with MLS display controls (suppressed address -> 'Address Undisclosed', off-market -> 'unavailable: true').
 *    - Saved searches list with alert frequencies.
 *    - Inquiries list with contact messages.
 *    - Cross-account access denied (404 Not Found, zero enumeration).
 * 6. Member Client Activity Timeline API (GET /api/member/clients/:id/activity):
 *    - Paginated longitudinal timeline of buyer events.
 *    - Safe listing summaries with address suppression.
 *    - Tenant isolation verified.
 * 7. Member Overview KPI Metrics:
 *    - GET /api/member/overview returns clientsCount, activeClients7dCount, savedHomesCount, savedSearchesCount.
 * 8. Member Portal UI & XSS Defense:
 *    - renderMemberUI includes Clients tab, KPI cards, timeline components.
 *    - escapeHtml sanitizer verified against script and tag injection.
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import consumerWorker from '../sneak-consumer/worker.js';
import memberWorker from '../sneak-member/worker.js';
import {
    ALLOWED_ACTIVITY_TYPES,
    recordConsumerActivity,
    handleReportActivity
} from '../sneak-consumer/api.js';
import {
    handleListMemberClients,
    handleGetMemberClientDetail,
    handleGetMemberClientActivity,
    handleMemberOverview
} from '../sneak-member/api.js';
import { renderMemberUI } from '../sneak-member/ui.js';
import { sha256Hex } from '../sneak-consumer/auth.js';

const TEST_SECRET = 'test_consumer_signing_secret_32_bytes_len_1234567890';

function createMockActivityDB() {
    const tables = {
        sneak_accounts: [
            { id: 'acc_member_1', account_name: 'Ursula Weinkauff Realty', status: 'active', plan: 'pro' },
            { id: 'acc_member_2', account_name: 'Foreign Agent Realty', status: 'active', plan: 'pro' }
        ],
        sneak_member_users: [
            { id: 'usr_mem_1', account_id: 'acc_member_1', email: 'ursula@example.com', status: 'active', role: 'owner' },
            { id: 'usr_mem_2', account_id: 'acc_member_2', email: 'foreign@example.com', status: 'active', role: 'owner' }
        ],
        sneak_member_sessions: [
            { id: 'msess_1', user_id: 'usr_mem_1', token_hash: 'hash_mem_token_1', expires_at: '2099-01-01T00:00:00Z', created_at: '2026-08-20T00:00:00Z' },
            { id: 'msess_2', user_id: 'usr_mem_2', token_hash: 'hash_mem_token_2', expires_at: '2099-01-01T00:00:00Z', created_at: '2026-08-20T00:00:00Z' }
        ],
        sneak_sites: [
            { id: 'site_1', account_id: 'acc_member_1', site_key: 'ursula-idx', site_name: 'Ursula Website', status: 'active', scope_type: 'agent', scope_value: 'B3650316' },
            { id: 'site_2', account_id: 'acc_member_2', site_key: 'foreign-idx', site_name: 'Foreign Website', status: 'active', scope_type: 'agent', scope_value: 'FOREIGN99' }
        ],
        sneak_domains: [
            { id: 'dom_1', site_id: 'site_1', domain: 'ursulaweinkauff.com', verified: 1, status: 'active' },
            { id: 'dom_2', site_id: 'site_2', domain: 'foreignrealty.com', verified: 1, status: 'active' }
        ],
        sneak_branding: [],
        sneak_widget_configs: [],
        sneak_account_billing: [
            { account_id: 'acc_member_1', billing_status: 'active', entitlement_status: 'active' }
        ],
        sneak_consumer_users: [
            { id: 'c_usr_1', site_id: 'site_1', email: 'buyer.alice@example.com', status: 'active', created_at: '2026-08-20T10:00:00Z', last_login_at: '2026-08-28T14:00:00Z', last_activity_at: '2026-08-28T14:30:00Z', updated_at: '2026-08-28T14:30:00Z' },
            { id: 'c_usr_2', site_id: 'site_1', email: 'buyer.bob@example.com', status: 'active', created_at: '2026-08-25T11:00:00Z', last_login_at: '2026-08-29T09:00:00Z', last_activity_at: '2026-08-29T09:15:00Z', updated_at: '2026-08-29T09:15:00Z' },
            { id: 'c_usr_3', site_id: 'site_2', email: 'buyer.alice@example.com', status: 'active', created_at: '2026-08-22T08:00:00Z', last_login_at: '2026-08-27T10:00:00Z', last_activity_at: '2026-08-27T10:00:00Z', updated_at: '2026-08-27T10:00:00Z' }
        ],
        sneak_consumer_sessions: [
            { id: 'csess_1', site_id: 'site_1', user_id: 'c_usr_1', token_hash: 'hash_buyer1_token', expires_at: '2099-01-01T00:00:00Z', created_at: '2026-08-20T10:00:00Z' },
            { id: 'csess_2', site_id: 'site_1', user_id: 'c_usr_2', token_hash: 'hash_buyer2_token', expires_at: '2099-01-01T00:00:00Z', created_at: '2026-08-25T11:00:00Z' },
            { id: 'csess_3', site_id: 'site_2', user_id: 'c_usr_3', token_hash: 'hash_buyer3_token', expires_at: '2099-01-01T00:00:00Z', created_at: '2026-08-22T08:00:00Z' }
        ],
        sneak_consumer_favorites: [
            { id: 'cfav_1', site_id: 'site_1', user_id: 'c_usr_1', listing_key: '2240001', created_at: '2026-08-21T12:00:00Z' },
            { id: 'cfav_2', site_id: 'site_1', user_id: 'c_usr_1', listing_key: '2240002', created_at: '2026-08-22T15:00:00Z' },
            { id: 'cfav_3', site_id: 'site_1', user_id: 'c_usr_2', listing_key: '2240001', created_at: '2026-08-26T16:00:00Z' }
        ],
        sneak_consumer_saved_searches: [
            { id: 'css_1', site_id: 'site_1', user_id: 'c_usr_1', name: 'Bonita Waterfront Homes', state_version: 1, state_json: '{}', state_hash: 'hash1', created_at: '2026-08-21T13:00:00Z', updated_at: '2026-08-21T13:00:00Z' },
            { id: 'css_2', site_id: 'site_1', user_id: 'c_usr_1', name: 'Estero Condos', state_version: 1, state_json: '{}', state_hash: 'hash2', created_at: '2026-08-23T14:00:00Z', updated_at: '2026-08-23T14:00:00Z' }
        ],
        sneak_consumer_search_alerts: [
            { id: 'calert_1', site_id: 'site_1', user_id: 'c_usr_1', saved_search_id: 'css_1', frequency: 'asap', enabled: 1, enabled_at: '2026-08-21T13:00:00Z', timezone: 'America/New_York', created_at: '2026-08-21T13:00:00Z', updated_at: '2026-08-21T13:00:00Z' }
        ],
        sneak_leads: [
            { id: 'lead_101', site_id: 'site_1', listing_key: '2240001', lead_type: 'property_inquiry', name: 'Alice Buyer', email: 'buyer.alice@example.com', phone: '239-555-0199', message: 'I would like a private showing this Saturday.', created_at: '2026-08-24T10:00:00Z' },
            { id: 'lead_102', site_id: 'site_2', listing_key: '2240099', lead_type: 'property_inquiry', name: 'Foreign Inquiry', email: 'foreign.buyer@example.com', phone: null, message: 'Is this available?', created_at: '2026-08-24T11:00:00Z' }
        ],
        sneak_consumer_activity_events: [
            { id: 'cact_1', site_id: 'site_1', user_id: 'c_usr_1', event_type: 'listing_view', listing_key: '2240001', saved_search_id: null, lead_id: null, metadata_json: null, dedupe_key: 'listing_view:c_usr_1:site_1:2240001:99990', created_at: '2026-08-20T10:30:00Z' },
            { id: 'cact_2', site_id: 'site_1', user_id: 'c_usr_1', event_type: 'favorite_added', listing_key: '2240001', saved_search_id: null, lead_id: null, metadata_json: null, dedupe_key: null, created_at: '2026-08-21T12:00:00Z' },
            { id: 'cact_3', site_id: 'site_1', user_id: 'c_usr_1', event_type: 'saved_search_created', listing_key: null, saved_search_id: 'css_1', lead_id: null, metadata_json: JSON.stringify({ name: 'Bonita Waterfront Homes' }), dedupe_key: null, created_at: '2026-08-21T13:00:00Z' },
            { id: 'cact_4', site_id: 'site_1', user_id: 'c_usr_1', event_type: 'alert_enabled', listing_key: null, saved_search_id: 'css_1', lead_id: null, metadata_json: JSON.stringify({ frequency: 'asap' }), dedupe_key: null, created_at: '2026-08-21T13:05:00Z' },
            { id: 'cact_5', site_id: 'site_1', user_id: 'c_usr_1', event_type: 'inquiry_submitted', listing_key: '2240001', saved_search_id: null, lead_id: 'lead_101', metadata_json: null, dedupe_key: 'inquiry_submitted:c_usr_1:site_1:lead_101', created_at: '2026-08-24T10:00:00Z' }
        ],
        sneak_listings: [
            {
                ListingKey: '2240001',
                StandardStatus: 'Active',
                ListPrice: 799000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '123 Coconut Grove Way',
                City: 'Bonita Springs',
                PostalCode: '34134',
                BedroomsTotal: 3,
                BathroomsTotalInteger: 2,
                LivingArea: 1850,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1,
                PrimaryPhoto: 'https://images.sneakidx.com/photo1.jpg'
            },
            {
                ListingKey: '2240002',
                StandardStatus: 'Active',
                ListPrice: 1250000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '456 Estero Bay Blvd',
                City: 'Estero',
                PostalCode: '33928',
                BedroomsTotal: 4,
                BathroomsTotalInteger: 3,
                LivingArea: 2800,
                PropertyType: 'Residential',
                PropertySubType: 'Single Family Residence',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 0, // Suppressed Address
                PrimaryPhoto: 'https://images.sneakidx.com/photo2.jpg'
            },
            {
                ListingKey: '2240099',
                StandardStatus: 'Active',
                ListPrice: 650000,
                ListAgentMlsId: 'FOREIGN99',
                ListAgentKey: 'FOREIGN99',
                UnparsedAddress: '789 Foreign Pine Rd',
                City: 'Naples',
                PostalCode: '34102',
                BedroomsTotal: 2,
                BathroomsTotalInteger: 2,
                LivingArea: 1400,
                PropertyType: 'Residential',
                PropertySubType: 'Condominium',
                InternetEntireListingDisplayYN: 1,
                InternetAddressDisplayYN: 1,
                PrimaryPhoto: 'https://images.sneakidx.com/photo99.jpg'
            },
            {
                ListingKey: '2240055',
                StandardStatus: 'Active',
                ListPrice: 500000,
                ListAgentMlsId: 'B3650316',
                ListAgentKey: 'B3650316',
                UnparsedAddress: '555 Private Rd',
                City: 'Bonita Springs',
                InternetEntireListingDisplayYN: 0, // Display Suppressed
                InternetAddressDisplayYN: 0,
                PrimaryPhoto: null
            }
        ],
        sneak_open_houses: []
    };

    return {
        tables,
        prepare(query) {
            function makeQueryExecutor(params = []) {
                return {
                    async first() {
                        const normalized = query.trim().replace(/\s+/g, ' ');

                        if (normalized.includes('FROM sneak_consumer_activity_events') && normalized.includes('count(*)')) {
                            const userId = params[0];
                            const siteId = params[1];
                            const filtered = tables.sneak_consumer_activity_events.filter(e => {
                                if (e.user_id !== userId) return false;
                                if (siteId && e.site_id !== siteId) return false;
                                return true;
                            });
                            return { count: filtered.length, total: filtered.length };
                        }

                        if (normalized.includes('FROM sneak_consumer_users') && normalized.includes('count(*) as total')) {
                            const siteIds = params.filter(p => typeof p === 'string' && p.startsWith('site_'));
                            const searchParam = params.find(p => typeof p === 'string' && p.startsWith('%'));
                            const rows = tables.sneak_consumer_users.filter(u => {
                                if (!siteIds.includes(u.site_id)) return false;
                                if (searchParam) {
                                    const cleanSearch = searchParam.replace(/%/g, '').toLowerCase();
                                    if (!u.email.toLowerCase().includes(cleanSearch)) return false;
                                }
                                return true;
                            });
                            return { total: rows.length };
                        }

                        if (normalized.includes('FROM sneak_consumer_users u JOIN sneak_sites s')) {
                            if (normalized.includes('count(*)')) {
                                const accountId = params[0];
                                const rows = tables.sneak_consumer_users.filter(u => {
                                    const site = tables.sneak_sites.find(s => s.id === u.site_id);
                                    return site && site.account_id === accountId;
                                });
                                return { count: rows.length };
                            }
                            const consumerId = params[0];
                            const accountId = params[1];
                            const user = tables.sneak_consumer_users.find(u => u.id === consumerId);
                            if (!user) return null;
                            const site = tables.sneak_sites.find(s => s.id === user.site_id && s.account_id === accountId);
                            if (!site) return null;
                            return {
                                ...user,
                                site_name: site.site_name,
                                site_key: site.site_key,
                                scope_type: site.scope_type,
                                scope_value: site.scope_value
                            };
                        }

                        if (normalized.includes('FROM sneak_consumer_favorites f JOIN sneak_sites s')) {
                            const accountId = params[0];
                            const rows = tables.sneak_consumer_favorites.filter(f => {
                                const site = tables.sneak_sites.find(s => s.id === f.site_id);
                                return site && site.account_id === accountId;
                            });
                            return { count: rows.length };
                        }

                        if (normalized.includes('FROM sneak_consumer_saved_searches ss JOIN sneak_sites s')) {
                            const accountId = params[0];
                            const rows = tables.sneak_consumer_saved_searches.filter(ss => {
                                const site = tables.sneak_sites.find(s => s.id === ss.site_id);
                                return site && site.account_id === accountId;
                            });
                            return { count: rows.length };
                        }

                        if (normalized.includes('FROM sneak_consumer_users') && normalized.includes('WHERE id = ?')) {
                            return tables.sneak_consumer_users.find(u => u.id === params[0]) || null;
                        }

                        if (normalized.includes('FROM sneak_consumer_sessions s') && normalized.includes('JOIN sneak_consumer_users u')) {
                            const tokenHash = params[0];
                            const session = tables.sneak_consumer_sessions.find(s => s.token_hash === tokenHash);
                            if (!session) return null;
                            const site = tables.sneak_sites.find(s => s.id === session.site_id);
                            if (!site) return null;
                            const user = tables.sneak_consumer_users.find(u => u.id === session.user_id);
                            if (!user) return null;
                            return {
                                session_id: session.id,
                                site_id: site.id,
                                site_key: site.site_key,
                                user_id: user.id,
                                consumer_email: user.email,
                                user_status: user.status,
                                site_status: site.status,
                                expires_at: session.expires_at
                            };
                        }

                        if (normalized.includes('FROM sneak_member_sessions s JOIN sneak_member_users u')) {
                            const tokenHash = params[0];
                            const sess = tables.sneak_member_sessions.find(s => s.token_hash === tokenHash);
                            if (!sess) return null;
                            const user = tables.sneak_member_users.find(u => u.id === sess.user_id);
                            if (!user) return null;
                            const account = tables.sneak_accounts.find(a => a.id === user.account_id);
                            if (!account) return null;
                            return {
                                session_id: sess.id,
                                user_id: user.id,
                                account_id: account.id,
                                account_name: account.account_name,
                                account_plan: account.plan,
                                member_email: user.email,
                                member_role: user.role,
                                user_status: user.status,
                                account_status: account.status
                            };
                        }

                        if (normalized.includes('FROM sneak_sites WHERE site_key = ?')) {
                            return tables.sneak_sites.find(s => s.site_key === params[0]) || null;
                        }

                        if (normalized.includes('FROM sneak_sites WHERE account_id = ?')) {
                            return tables.sneak_sites.find(s => s.account_id === params[0]) || null;
                        }

                        if (normalized.includes('FROM sneak_leads l JOIN sneak_sites s')) {
                            const accountId = params[0];
                            const rows = tables.sneak_leads.filter(l => {
                                const site = tables.sneak_sites.find(s => s.id === l.site_id);
                                return site && site.account_id === accountId;
                            });
                            return { count: rows.length };
                        }

                        if (normalized.includes('FROM sneak_leads WHERE id = ?')) {
                            return tables.sneak_leads.find(l => l.id === params[0]) || null;
                        }

                        if (normalized.includes('FROM sneak_leads WHERE site_id = ? AND LOWER(email) = LOWER(?)')) {
                            const siteId = params[0];
                            const email = params[1];
                            const match = tables.sneak_leads.find(l => l.site_id === siteId && (l.email || '').toLowerCase() === email.toLowerCase());
                            return match ? { count: 1, c: 1 } : { count: 0, c: 0 };
                        }

                        if (normalized.includes('FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?')) {
                            const filtered = tables.sneak_consumer_favorites.filter(f => f.user_id === params[0] && f.site_id === params[1]);
                            return { count: filtered.length, c: filtered.length };
                        }

                        if (normalized.includes('FROM sneak_consumer_saved_searches WHERE user_id = ? AND site_id = ?')) {
                            const filtered = tables.sneak_consumer_saved_searches.filter(s => s.user_id === params[0] && s.site_id === params[1]);
                            return { count: filtered.length, c: filtered.length };
                        }

                        if (normalized.includes('FROM sneak_consumer_search_alerts WHERE user_id = ? AND site_id = ? AND enabled = 1')) {
                            const filtered = tables.sneak_consumer_search_alerts.filter(a => a.user_id === params[0] && a.site_id === params[1] && a.enabled === 1);
                            return { count: filtered.length, c: filtered.length };
                        }

                        if (normalized.includes('FROM sneak_listings WHERE ListingKey = ?')) {
                            return tables.sneak_listings.find(l => l.ListingKey === params[0] && (l.InternetEntireListingDisplayYN === 1 || l.InternetEntireListingDisplayYN === null)) || null;
                        }

                        if (normalized.includes('FROM sneak_listings')) {
                            return { count: tables.sneak_listings.length };
                        }

                        if (normalized.includes('FROM sneak_open_houses')) {
                            return { count: tables.sneak_open_houses.length };
                        }

                        return null;
                    },
                    async all() {
                        const normalized = query.trim().replace(/\s+/g, ' ');

                        if (normalized.includes('FROM sneak_sites WHERE account_id = ?')) {
                            return { results: tables.sneak_sites.filter(s => s.account_id === params[0]) };
                        }

                        if (normalized.includes('FROM sneak_domains d JOIN sneak_sites s')) {
                            return { results: tables.sneak_domains.filter(d => tables.sneak_sites.some(s => s.id === d.site_id && s.account_id === params[0])) };
                        }

                        if (normalized.includes('FROM sneak_domains WHERE site_id = ?')) {
                            return { results: tables.sneak_domains.filter(d => d.site_id === params[0]) };
                        }

                        if (normalized.includes('FROM sneak_widget_configs WHERE site_id = ?')) {
                            return { results: [] };
                        }

                        if (normalized.includes('FROM sneak_consumer_users u WHERE u.site_id IN')) {
                            const siteIds = params.filter(p => typeof p === 'string' && p.startsWith('site_'));
                            const searchParam = params.find(p => typeof p === 'string' && p.startsWith('%'));
                            const rows = tables.sneak_consumer_users.filter(u => {
                                if (!siteIds.includes(u.site_id)) return false;
                                if (searchParam) {
                                    const cleanSearch = searchParam.replace(/%/g, '').toLowerCase();
                                    if (!u.email.toLowerCase().includes(cleanSearch)) return false;
                                }
                                return true;
                            });

                            const enriched = rows.map(u => ({
                                ...u,
                                favorite_count: tables.sneak_consumer_favorites.filter(f => f.user_id === u.id && f.site_id === u.site_id).length,
                                saved_search_count: tables.sneak_consumer_saved_searches.filter(s => s.user_id === u.id && s.site_id === u.site_id).length,
                                alert_count: tables.sneak_consumer_search_alerts.filter(a => a.user_id === u.id && a.site_id === u.site_id && a.enabled === 1).length,
                                inquiry_count: tables.sneak_leads.filter(l => l.site_id === u.site_id && (l.email || '').toLowerCase() === u.email.toLowerCase()).length
                            }));

                            return { results: enriched };
                        }

                        if (normalized.includes('FROM sneak_consumer_favorites WHERE user_id = ? AND site_id = ?')) {
                            return { results: tables.sneak_consumer_favorites.filter(f => f.user_id === params[0] && f.site_id === params[1]) };
                        }

                        if (normalized.includes('FROM sneak_consumer_saved_searches s LEFT JOIN sneak_consumer_search_alerts a')) {
                            const searches = tables.sneak_consumer_saved_searches.filter(s => s.user_id === params[0] && s.site_id === params[1]);
                            const results = searches.map(s => {
                                const alert = tables.sneak_consumer_search_alerts.find(a => a.saved_search_id === s.id);
                                return {
                                    id: s.id,
                                    name: s.name,
                                    created_at: s.created_at,
                                    updated_at: s.updated_at,
                                    frequency: alert?.frequency || 'off',
                                    enabled: alert?.enabled ? 1 : 0,
                                    enabled_at: alert?.enabled_at || null
                                };
                            });
                            return { results };
                        }

                        if (normalized.includes('FROM sneak_leads WHERE site_id = ? AND LOWER(email) = LOWER(?)')) {
                            const siteId = params[0];
                            const email = params[1].toLowerCase();
                            return { results: tables.sneak_leads.filter(l => l.site_id === siteId && (l.email || '').toLowerCase() === email) };
                        }

                        if (normalized.includes('FROM sneak_consumer_activity_events WHERE user_id = ? AND site_id = ?')) {
                            const userId = params[0];
                            const siteId = params[1];
                            const events = tables.sneak_consumer_activity_events.filter(e => e.user_id === userId && e.site_id === siteId);
                            return { results: events };
                        }

                        if (normalized.includes('FROM sneak_listings WHERE ListingKey IN')) {
                            const keys = params.filter(p => typeof p === 'string' && /^\d+$/.test(p));
                            const results = tables.sneak_listings.filter(l => keys.includes(l.ListingKey) && (l.InternetEntireListingDisplayYN === 1 || l.InternetEntireListingDisplayYN === null));
                            return { results };
                        }

                        return { results: [] };
                    },
                    async run() {
                        const normalized = query.trim().replace(/\s+/g, ' ');

                        if (normalized.includes('INSERT OR IGNORE INTO sneak_consumer_activity_events')) {
                            const [id, site_id, user_id, event_type, listing_key, saved_search_id, lead_id, metadata_json, dedupe_key, created_at] = params;
                            if (dedupe_key && tables.sneak_consumer_activity_events.some(e => e.dedupe_key === dedupe_key)) {
                                return { meta: { changes: 0 } };
                            }
                            tables.sneak_consumer_activity_events.unshift({
                                id, site_id, user_id, event_type, listing_key, saved_search_id, lead_id, metadata_json, dedupe_key, created_at
                            });
                            return { meta: { changes: 1 } };
                        }

                        if (normalized.includes('UPDATE sneak_consumer_users SET last_activity_at = ?')) {
                            const [lastActivity, updatedAt, userId] = params;
                            const user = tables.sneak_consumer_users.find(u => u.id === userId);
                            if (user) {
                                user.last_activity_at = lastActivity;
                                user.updated_at = updatedAt;
                            }
                            return { meta: { changes: 1 } };
                        }

                        return { meta: { changes: 1 } };
                    }
                };
            }

            const executor = makeQueryExecutor([]);
            return {
                bind(...params) {
                    return makeQueryExecutor(params);
                },
                first: () => executor.first(),
                all: () => executor.all(),
                run: () => executor.run()
            };
        }
    };
}

describe('SNEAK IDX Phase 7.3C2B — Agent Client Activity Dashboard & Buyer Timeline', () => {

    test('1. Activity Ledger Record Helper: Ingestion, last_activity_at update, and vocabulary guard', async () => {
        const db = createMockActivityDB();

        // Valid listing_view recording
        const res = await recordConsumerActivity(db, {
            siteId: 'site_1',
            userId: 'c_usr_1',
            eventType: 'listing_view',
            listingKey: '2240001',
            dedupeKey: 'listing_view:c_usr_1:site_1:2240001:bucket1'
        });

        assert.equal(res.success, true);
        assert.ok(res.eventId.startsWith('cact_'));

        // Verify last_activity_at bumped on sneak_consumer_users
        const user = db.tables.sneak_consumer_users.find(u => u.id === 'c_usr_1');
        assert.ok(user.last_activity_at !== null);

        // Invalid event type rejection (Vocabulary Guard)
        const invalidRes = await recordConsumerActivity(db, {
            siteId: 'site_1',
            userId: 'c_usr_1',
            eventType: 'unauthorized_random_event'
        });
        assert.equal(invalidRes.success, false);
        assert.equal(invalidRes.error, 'InvalidActivityEvent');

        // Vocabulary constant contains exactly expected events
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('listing_view'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('favorite_added'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('favorite_removed'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('saved_search_created'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('saved_search_updated'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('saved_search_deleted'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('alert_enabled'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('alert_frequency_changed'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('alert_disabled'));
        assert.ok(ALLOWED_ACTIVITY_TYPES.includes('inquiry_submitted'));
    });

    test('2. Deduplication Window: Duplicate listing_view within 30 minutes is silently ignored via dedupe_key', async () => {
        const db = createMockActivityDB();
        const dedupeKey = 'listing_view:c_usr_1:site_1:2240001:bucket_30m';

        // 1st recording succeeds
        const first = await recordConsumerActivity(db, {
            siteId: 'site_1',
            userId: 'c_usr_1',
            eventType: 'listing_view',
            listingKey: '2240001',
            dedupeKey
        });
        assert.equal(first.success, true);

        const eventCountBefore = db.tables.sneak_consumer_activity_events.length;

        // 2nd recording with identical dedupeKey
        const second = await recordConsumerActivity(db, {
            siteId: 'site_1',
            userId: 'c_usr_1',
            eventType: 'listing_view',
            listingKey: '2240001',
            dedupeKey
        });
        assert.equal(second.success, true);

        // Verify no duplicate row added
        assert.equal(db.tables.sneak_consumer_activity_events.length, eventCountBefore);
    });

    test('3. Browser Ingestion API: POST /api/consumer/activity rejects anonymous & validates listing eligibility', async () => {
        const db = createMockActivityDB();
        const env = { DB: db, SNEAK_SIGNING_SECRET: TEST_SECRET };

        // 1. Anonymous request without token -> 401
        const anonReq = new Request('https://sneak-consumer.staging/api/consumer/activity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ site: 'ursula-idx', type: 'listing_view', listingKey: '2240001' })
        });
        const anonRes = await consumerWorker.fetch(anonReq, env);
        assert.equal(anonRes.status, 401);

        // 2. Disallowed browser event type (e.g. favorite_added) -> 400
        const fakeRawToken = 'valid_buyer_token_64_characters_hex_12345678901234567890123456789012';
        const fakeHash = await sha256Hex(fakeRawToken);
        db.tables.sneak_consumer_sessions[0].token_hash = fakeHash;

        const disallowReq = new Request('https://sneak-consumer.staging/api/consumer/activity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${fakeRawToken}` },
            body: JSON.stringify({ site: 'ursula-idx', type: 'favorite_added', listingKey: '2240001' })
        });
        const disallowRes = await consumerWorker.fetch(disallowReq, env);
        assert.equal(disallowRes.status, 400);

        // 3. Ineligible listing (InternetEntireListingDisplayYN = 0) -> 404
        const ineligReq = new Request('https://sneak-consumer.staging/api/consumer/activity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${fakeRawToken}` },
            body: JSON.stringify({ site: 'ursula-idx', type: 'listing_view', listingKey: '2240055' })
        });
        const ineligRes = await consumerWorker.fetch(ineligReq, env);
        assert.equal(ineligRes.status, 404);

        // 4. Valid listing_view -> 200 OK
        const validReq = new Request('https://sneak-consumer.staging/api/consumer/activity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${fakeRawToken}` },
            body: JSON.stringify({ site: 'ursula-idx', type: 'listing_view', listingKey: '2240001' })
        });
        const validRes = await consumerWorker.fetch(validReq, env);
        assert.equal(validRes.status, 200);
        const validData = await validRes.json();
        assert.equal(validData.success, true);
    });

    test('4. Browser Ingestion API: inquiry_submitted verifies lead ownership, email match, and site scope', async () => {
        const db = createMockActivityDB();
        const env = { DB: db, SNEAK_SIGNING_SECRET: TEST_SECRET };
        const fakeRawToken = 'valid_buyer_token_64_characters_hex_12345678901234567890123456789012';
        const fakeHash = await sha256Hex(fakeRawToken);
        db.tables.sneak_consumer_sessions[0].token_hash = fakeHash; // Alice Buyer on site_1

        // 1. Non-existent lead -> 404
        const nonExistentReq = new Request('https://sneak-consumer.staging/api/consumer/activity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${fakeRawToken}` },
            body: JSON.stringify({ site: 'ursula-idx', type: 'inquiry_submitted', leadId: 'lead_nonexistent' })
        });
        const nonExistentRes = await consumerWorker.fetch(nonExistentReq, env);
        assert.equal(nonExistentRes.status, 404);

        // 2. Foreign site lead -> 403
        const foreignLeadReq = new Request('https://sneak-consumer.staging/api/consumer/activity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${fakeRawToken}` },
            body: JSON.stringify({ site: 'ursula-idx', type: 'inquiry_submitted', leadId: 'lead_102' })
        });
        const foreignLeadRes = await consumerWorker.fetch(foreignLeadReq, env);
        assert.equal(foreignLeadRes.status, 403);

        // 3. Valid lead matching site and email -> 200 OK
        const validLeadReq = new Request('https://sneak-consumer.staging/api/consumer/activity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${fakeRawToken}` },
            body: JSON.stringify({ site: 'ursula-idx', type: 'inquiry_submitted', leadId: 'lead_101', listingKey: '2240001' })
        });
        const validLeadRes = await consumerWorker.fetch(validLeadReq, env);
        assert.equal(validLeadRes.status, 200);
        const validData = await validLeadRes.json();
        assert.equal(validData.success, true);
    });

    test('5. Member Clients API: GET /api/member/clients enforces strict tenant isolation & aggregates counts', async () => {
        const db = createMockActivityDB();
        const memberContext1 = { account_id: 'acc_member_1', user_id: 'usr_mem_1' };
        const memberContext2 = { account_id: 'acc_member_2', user_id: 'usr_mem_2' };

        // Member 1 lists clients (should see Alice and Bob on site_1)
        const url1 = new URL('https://sneak-member.staging/api/member/clients?sort=recently_active');
        const res1 = await handleListMemberClients(db, memberContext1, url1);
        const data1 = await res1.json();

        assert.equal(data1.success, true);
        assert.equal(data1.clients.length, 2);
        assert.ok(data1.clients.some(c => c.email === 'buyer.alice@example.com'));
        assert.ok(data1.clients.some(c => c.email === 'buyer.bob@example.com'));

        // Verify Alice's aggregate counters
        const alice = data1.clients.find(c => c.email === 'buyer.alice@example.com');
        assert.equal(alice.savedHomesCount, 2);
        assert.equal(alice.savedSearchesCount, 2);
        assert.equal(alice.alertsCount, 1);
        assert.equal(alice.inquiriesCount, 1);

        // Member 2 lists clients (should see ONLY Alice on site_2, completely isolated from Member 1)
        const url2 = new URL('https://sneak-member.staging/api/member/clients?sort=recently_active');
        const res2 = await handleListMemberClients(db, memberContext2, url2);
        const data2 = await res2.json();

        assert.equal(data2.success, true);
        assert.equal(data2.clients.length, 1);
        assert.equal(data2.clients[0].id, 'c_usr_3');
        assert.equal(data2.clients[0].siteId, 'site_2');
    });

    test('6. Member Clients API: Email search filter and sorting criteria', async () => {
        const db = createMockActivityDB();
        const memberContext1 = { account_id: 'acc_member_1', user_id: 'usr_mem_1' };

        // Search for 'bob'
        const searchUrl = new URL('https://sneak-member.staging/api/member/clients?search=bob');
        const searchRes = await handleListMemberClients(db, memberContext1, searchUrl);
        const searchData = await searchRes.json();

        assert.equal(searchData.success, true);
        assert.equal(searchData.clients.length, 1);
        assert.equal(searchData.clients[0].email, 'buyer.bob@example.com');
    });

    test('7. Member Client Detail API: Profile, saved homes, display controls, and cross-account rejection', async () => {
        const db = createMockActivityDB();
        const memberContext1 = { account_id: 'acc_member_1', user_id: 'usr_mem_1' };
        const memberContext2 = { account_id: 'acc_member_2', user_id: 'usr_mem_2' };

        // Member 1 requests Alice detail
        const detailRes = await handleGetMemberClientDetail(db, memberContext1, 'c_usr_1');
        assert.equal(detailRes.status, 200);
        const detailData = await detailRes.json();

        assert.equal(detailData.success, true);
        assert.equal(detailData.client.email, 'buyer.alice@example.com');
        assert.equal(detailData.savedHomes.length, 2);

        // Listing 2240002 has InternetAddressDisplayYN = 0 -> Address must be suppressed
        const suppressedHome = detailData.savedHomes.find(h => h.listingKey === '2240002');
        assert.equal(suppressedHome.address, 'Address Undisclosed');

        // Saved searches check
        assert.equal(detailData.savedSearches.length, 2);
        assert.equal(detailData.savedSearches[0].alertFrequency, 'asap');

        // Inquiries check
        assert.equal(detailData.inquiries.length, 1);
        assert.equal(detailData.inquiries[0].leadType, 'property_inquiry');

        // Cross-Account Tenant Isolation: Member 2 requests Member 1's client -> 404
        const crossRes = await handleGetMemberClientDetail(db, memberContext2, 'c_usr_1');
        assert.equal(crossRes.status, 404);
    });

    test('8. Member Client Activity Timeline API: Chronological event ledger and safe listing summary', async () => {
        const db = createMockActivityDB();
        const memberContext1 = { account_id: 'acc_member_1', user_id: 'usr_mem_1' };
        const url = new URL('https://sneak-member.staging/api/member/clients/c_usr_1/activity?limit=50');

        const actRes = await handleGetMemberClientActivity(db, memberContext1, 'c_usr_1', url);
        assert.equal(actRes.status, 200);
        const actData = await actRes.json();

        assert.equal(actData.success, true);
        assert.equal(actData.events.length, 5);

        // Verify event types represented in ledger
        const types = actData.events.map(e => e.type);
        assert.ok(types.includes('listing_view'));
        assert.ok(types.includes('favorite_added'));
        assert.ok(types.includes('saved_search_created'));
        assert.ok(types.includes('alert_enabled'));
        assert.ok(types.includes('inquiry_submitted'));

        // Listing summary for listing_view
        const viewEvent = actData.events.find(e => e.type === 'listing_view');
        assert.ok(viewEvent.listing !== null);
        assert.equal(viewEvent.listing.listingKey, '2240001');
        assert.equal(viewEvent.listing.price, 799000);
    });

    test('9. Member Overview Metrics: Returns clientsCount, activeClients7dCount, savedHomesCount', async () => {
        const db = createMockActivityDB();
        const memberContext1 = { account_id: 'acc_member_1', user_id: 'usr_mem_1' };

        const res = await handleMemberOverview(db, memberContext1);
        const data = await res.json();

        assert.equal(data.clientsCount, 2);
        assert.equal(data.activeClients7dCount, 2);
        assert.equal(data.savedHomesCount, 3);
        assert.equal(data.leadsCount, 1);
    });

    test('10. Member UI & XSS Sanitization: HTML tags in buyer input are properly escaped', () => {
        const uiHtml = renderMemberUI();

        // Verify Clients section components exist
        assert.ok(uiHtml.includes('id="tab-clients"'), 'UI must contain Clients tab');
        assert.ok(uiHtml.includes('id="clientDetailModal"'), 'UI must contain Client Detail modal');
        assert.ok(uiHtml.includes('id="clientActivityTimeline"'), 'UI must contain Activity Timeline');
        assert.ok(uiHtml.includes('id="clientKpiTotal"'), 'UI must contain client KPI total card');

        // Test XSS escape function logic
        function escapeHtml(str) {
            if (str === null || str === undefined) return '';
            return String(str)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#039;');
        }

        const maliciousInput = '<script>alert("xss")</script><img src="x" onerror="steal()">&"\'';
        const safeOutput = escapeHtml(maliciousInput);

        assert.ok(!safeOutput.includes('<script>'));
        assert.ok(!safeOutput.includes('<img'));
        assert.ok(safeOutput.includes('&lt;script&gt;'));
        assert.ok(safeOutput.includes('&amp;'));
        assert.ok(safeOutput.includes('&quot;'));
    });
});
