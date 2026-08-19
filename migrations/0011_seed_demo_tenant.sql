-- Migration: 0011_seed_demo_tenant.sql
-- Description: Seeds the initial demo tenant (demo-ccor) for development and staging testing.

INSERT OR IGNORE INTO sneak_accounts (
    id, member_id, account_name, agent_mls_id, office_mls_id, status, plan
) VALUES (
    'acc_demo_ccor',
    'MEM-1001',
    'Premier Coast Realty Demo',
    'DEMO_AGENT_01',
    'DEMO_OFFICE_01',
    'active',
    'pro'
);

INSERT OR IGNORE INTO sneak_sites (
    id, account_id, site_key, site_name, status, scope_type, scope_value
) VALUES (
    'site_demo_ccor',
    'acc_demo_ccor',
    'demo-ccor',
    'Premier Coast Realty - Full Market Search',
    'active',
    'market',
    NULL
);

-- Authorized Domains for Demo Site (Development & Preview)
INSERT OR IGNORE INTO sneak_domains (id, site_id, domain, verified, status)
VALUES 
    ('dom_demo_local1', 'site_demo_ccor', 'localhost', 1, 'active'),
    ('dom_demo_local2', 'site_demo_ccor', '127.0.0.1', 1, 'active'),
    ('dom_demo_preview', 'site_demo_ccor', 'preview.sneakidx.com', 1, 'active'),
    ('dom_demo_workers', 'site_demo_ccor', '*.workers.dev', 1, 'active');

-- Branding Configuration for Demo Site
INSERT OR REPLACE INTO sneak_branding (
    site_id, display_name, brokerage, logo_url, agent_photo_url,
    primary_color, secondary_color, phone, email, website_url, config_json
) VALUES (
    'site_demo_ccor',
    'Premier Coast Realty',
    'Premier Coast Realty LLC',
    '',
    '',
    '#1a365d',
    '#0284c7',
    '(239) 555-0199',
    'team@premiercoastrealty.demo',
    'https://premiercoastrealty.demo',
    '{"headerLinks":[{"title":"Homes for Sale","url":"#"},{"title":"Open Houses","url":"#"},{"title":"About","url":"#"}],"disclaimerOrg":"Premier Coast Realty LLC","showPoweredBy":true}'
);

-- Default Widget Configurations
INSERT OR IGNORE INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json)
VALUES
    ('wc_demo_search', 'site_demo_ccor', 'search', 1, '{"defaultCity":"","defaultType":"sale","defaultSort":"dateDesc","pageSize":24}'),
    ('wc_demo_grid', 'site_demo_ccor', 'listing_grid', 1, '{"limit":12,"sort":"dateDesc"}'),
    ('wc_demo_openhouses', 'site_demo_ccor', 'open_houses', 1, '{"daysAhead":30}');
