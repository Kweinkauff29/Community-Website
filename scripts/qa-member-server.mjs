/**
 * scripts/qa-member-server.mjs
 * 
 * Local test server serving SNEAK Member Portal with full authenticated mock session
 * and rich client data for browser verification at 1440x900, 1024x768, and 390x844.
 */

import http from 'node:http';
import { renderMemberUI } from '../sneak-member/ui.js';

const PORT = 8788;

const MOCK_OVERVIEW = {
    account: {
        id: "acc_ursula_pilot",
        account_name: "Ursula Weinkauff — SNEAK Pilot",
        plan: "standard",
        member_email: "ursula@bonitaspringsrealtors.org"
    },
    site: {
        id: "site_ursula_1",
        account_id: "acc_ursula_pilot",
        site_key: "ursula-weinkauff-pilot",
        site_name: "Ursula Weinkauff IDX Pilot",
        status: "active",
        scope_type: "market",
        scope_value: null,
        created_at: "2026-08-24 15:02:09"
    },
    domains: [{ id: "dom_1", site_id: "site_ursula_1", domain: "coconutcoastrealtors.org", verified: 1, status: "active" }],
    branding: {
        display_name: "Ursula Weinkauff",
        brokerage: "Local Real Estate LLC",
        primary_color: "#0f2942",
        secondary_color: "#2b6cb0",
        email: "ursula@bonitaspringsrealtors.org",
        website_url: "https://coconutcoastrealtors.org"
    },
    widgets: [],
    embed: {
        siteKey: "ursula-weinkauff-pilot",
        servingHost: "https://sneak-idx-worker-staging.bonitaspringsrealtors.workers.dev",
        allowedDomains: ["coconutcoastrealtors.org"],
        snippets: []
    },
    billing: { billing_status: "active", entitlement_status: "active" },
    inventory: { activeListings: 32059, futureOpenHouses: 580 },
    leadsCount: 12,
    clientsCount: 4,
    activeClients7dCount: 3,
    savedHomesCount: 9,
    savedSearchesCount: 6
};

const MOCK_CLIENTS = [
    {
        id: "cuser_alice_1",
        email: "alice.buyer@gmail.com",
        status: "active",
        siteId: "site_ursula_1",
        siteName: "Ursula Weinkauff IDX Pilot",
        siteKey: "ursula-weinkauff-pilot",
        createdAt: "2026-08-20T10:00:00Z",
        lastLoginAt: "2026-08-30T14:30:00Z",
        lastActivityAt: "2026-08-30T14:45:00Z",
        savedHomesCount: 4,
        savedSearchesCount: 2,
        alertsCount: 2,
        inquiriesCount: 1
    },
    {
        id: "cuser_bob_2",
        email: "bob.investor@floridahomes.com",
        status: "active",
        siteId: "site_ursula_1",
        siteName: "Ursula Weinkauff IDX Pilot",
        siteKey: "ursula-weinkauff-pilot",
        createdAt: "2026-08-22T11:00:00Z",
        lastLoginAt: "2026-08-29T16:00:00Z",
        lastActivityAt: "2026-08-29T16:20:00Z",
        savedHomesCount: 3,
        savedSearchesCount: 3,
        alertsCount: 1,
        inquiriesCount: 2
    },
    {
        id: "cuser_carol_3",
        email: "carol.smith@yahoo.com",
        status: "active",
        siteId: "site_ursula_1",
        siteName: "Ursula Weinkauff IDX Pilot",
        siteKey: "ursula-weinkauff-pilot",
        createdAt: "2026-08-25T09:00:00Z",
        lastLoginAt: "2026-08-28T12:00:00Z",
        lastActivityAt: "2026-08-28T12:15:00Z",
        savedHomesCount: 2,
        savedSearchesCount: 1,
        alertsCount: 0,
        inquiriesCount: 0
    },
    {
        id: "cuser_david_4",
        email: "david.lee@outlook.com",
        status: "active",
        siteId: "site_ursula_1",
        siteName: "Ursula Weinkauff IDX Pilot",
        siteKey: "ursula-weinkauff-pilot",
        createdAt: "2026-08-28T15:00:00Z",
        lastLoginAt: "2026-08-28T15:30:00Z",
        lastActivityAt: "2026-08-28T15:30:00Z",
        savedHomesCount: 0,
        savedSearchesCount: 0,
        alertsCount: 0,
        inquiriesCount: 0
    }
];

const MOCK_ALICE_DETAIL = {
    success: true,
    client: {
        id: "cuser_alice_1",
        email: "alice.buyer@gmail.com",
        status: "active",
        siteId: "site_ursula_1",
        siteName: "Ursula Weinkauff IDX Pilot",
        siteKey: "ursula-weinkauff-pilot",
        scopeType: "market",
        createdAt: "2026-08-20T10:00:00Z",
        activatedAt: "2026-08-20T10:05:00Z",
        lastLoginAt: "2026-08-30T14:30:00Z",
        lastActivityAt: "2026-08-30T14:45:00Z"
    },
    savedHomes: [
        {
            listingKey: "224017488",
            savedAt: "2026-08-21T12:00:00Z",
            unavailable: false,
            price: 1450000,
            address: "3694 Pleasant Springs Dr",
            city: "Bonita Springs",
            postalCode: "34134",
            propertyType: "Residential",
            propertySubType: "Single Family Residence",
            bedrooms: 4,
            bathrooms: 4,
            livingArea: 3200,
            status: "Active",
            primaryPhoto: "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w=600"
        },
        {
            listingKey: "224019921",
            savedAt: "2026-08-22T15:30:00Z",
            unavailable: false,
            price: 875000,
            address: "Address Undisclosed",
            city: "Estero",
            postalCode: "33928",
            propertyType: "Residential",
            propertySubType: "Condominium",
            bedrooms: 3,
            bathrooms: 2,
            livingArea: 1850,
            status: "Active",
            primaryPhoto: "https://images.unsplash.com/photo-1512917774080-9991f1c4c750?w=600"
        }
    ],
    savedSearches: [
        {
            id: "css_alice_1",
            name: "Bonita Springs Waterfront 3+ Beds",
            createdAt: "2026-08-21T11:00:00Z",
            updatedAt: "2026-08-21T11:00:00Z",
            alertFrequency: "asap",
            alertEnabled: true,
            alertEnabledAt: "2026-08-21T11:05:00Z"
        },
        {
            id: "css_alice_2",
            name: "Estero Luxury Condos",
            createdAt: "2026-08-24T14:00:00Z",
            updatedAt: "2026-08-24T14:00:00Z",
            alertFrequency: "daily",
            alertEnabled: true,
            alertEnabledAt: "2026-08-24T14:05:00Z"
        }
    ],
    inquiries: [
        {
            id: "lead_alice_1",
            listingKey: "224017488",
            leadType: "property_inquiry",
            name: "Alice Buyer",
            email: "alice.buyer@gmail.com",
            phone: "(239) 555-0144",
            message: "Hello Ursula, I would love to schedule a private tour of 3694 Pleasant Springs Dr this Saturday at 2 PM if possible.",
            createdAt: "2026-08-24T16:00:00Z"
        }
    ]
};

const MOCK_ALICE_ACTIVITY = {
    success: true,
    events: [
        {
            id: "cact_5",
            type: "inquiry_submitted",
            leadId: "lead_alice_1",
            listingKey: "224017488",
            createdAt: "2026-08-24T16:00:00Z",
            listing: {
                listingKey: "224017488",
                price: 1450000,
                address: "3694 Pleasant Springs Dr",
                city: "Bonita Springs",
                status: "Active"
            }
        },
        {
            id: "cact_4",
            type: "alert_enabled",
            savedSearchId: "css_alice_1",
            metadata: { frequency: "asap" },
            createdAt: "2026-08-21T11:05:00Z"
        },
        {
            id: "cact_3",
            type: "saved_search_created",
            savedSearchId: "css_alice_1",
            metadata: { name: "Bonita Springs Waterfront 3+ Beds" },
            createdAt: "2026-08-21T11:00:00Z"
        },
        {
            id: "cact_2",
            type: "favorite_added",
            listingKey: "224017488",
            createdAt: "2026-08-21T12:00:00Z",
            listing: {
                listingKey: "224017488",
                price: 1450000,
                address: "3694 Pleasant Springs Dr",
                city: "Bonita Springs",
                status: "Active"
            }
        },
        {
            id: "cact_1",
            type: "listing_view",
            listingKey: "224017488",
            createdAt: "2026-08-21T11:45:00Z",
            listing: {
                listingKey: "224017488",
                price: 1450000,
                address: "3694 Pleasant Springs Dr",
                city: "Bonita Springs",
                status: "Active"
            }
        }
    ],
    total: 5,
    page: 1,
    limit: 50
};

const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://localhost:${PORT}`);
    const pathname = url.pathname;

    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    if (req.method === 'OPTIONS') {
        res.writeHead(200);
        res.end();
        return;
    }

    if (pathname === '/' || pathname === '/index.html') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderMemberUI());
        return;
    }

    if (pathname === '/api/member/overview') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(MOCK_OVERVIEW));
        return;
    }

    if (pathname === '/api/member/clients') {
        const search = (url.searchParams.get('search') || '').toLowerCase();
        const sort = url.searchParams.get('sort') || 'recently_active';
        
        let filtered = [...MOCK_CLIENTS];
        if (search) {
            filtered = filtered.filter(c => c.email.toLowerCase().includes(search));
        }

        if (sort === 'newest') {
            filtered.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
        } else if (sort === 'saved_homes') {
            filtered.sort((a, b) => b.savedHomesCount - a.savedHomesCount);
        } else if (sort === 'saved_searches') {
            filtered.sort((a, b) => b.savedSearchesCount - a.savedSearchesCount);
        } else {
            filtered.sort((a, b) => new Date(b.lastActivityAt || b.lastLoginAt) - new Date(a.lastActivityAt || a.lastLoginAt));
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            success: true,
            clients: filtered,
            total: filtered.length,
            page: 1,
            limit: 20
        }));
        return;
    }

    if (pathname.startsWith('/api/member/clients/') && pathname.endsWith('/activity')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(MOCK_ALICE_ACTIVITY));
        return;
    }

    if (pathname.startsWith('/api/member/clients/')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(MOCK_ALICE_DETAIL));
        return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'NotFound' }));
});

server.listen(PORT, () => {
    console.log(`QA Member Server running on http://localhost:${PORT}`);
});
