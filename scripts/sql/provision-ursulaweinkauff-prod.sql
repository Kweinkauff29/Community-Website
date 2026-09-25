-- Provision dedicated site: ursulaweinkauff-com in sneak-idx-production
-- Account: acc_6023a5d9-d6fb-446f-bdbe-7924999f003c (Ursula Weinkauff — SNEAK Pilot)

INSERT INTO sneak_sites (
    id, account_id, site_key, site_name, status, scope_type, scope_value, created_at, updated_at
) VALUES (
    'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50',
    'acc_6023a5d9-d6fb-446f-bdbe-7924999f003c',
    'ursulaweinkauff-com',
    'Ursula Weinkauff — Ursulaweinkauff.com',
    'active',
    'market',
    NULL,
    datetime('now'),
    datetime('now')
);

INSERT INTO sneak_domains (id, site_id, domain, verified, status, created_at) VALUES
    ('dom_prod_uw_1', 'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50', 'ursulaweinkauff.com', 1, 'active', datetime('now')),
    ('dom_prod_uw_2', 'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50', 'www.ursulaweinkauff.com', 1, 'active', datetime('now'));

INSERT INTO sneak_branding (
    site_id, display_name, brokerage, logo_url, agent_photo_url,
    primary_color, secondary_color, phone, email, website_url, config_json
) VALUES (
    'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50',
    'Ursula Weinkauff',
    'Local Real Estate LLC',
    '',
    '',
    '#0f2942',
    '#2b6cb0',
    '',
    'kmwcollegeapps@gmail.com',
    'https://ursulaweinkauff.com',
    '{"showPoweredBy":true}'
);

INSERT INTO sneak_widget_configs (id, site_id, widget_type, enabled, config_json, created_at) VALUES
    ('w_site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50_search', 'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50', 'search', 1, '{}', datetime('now')),
    ('w_site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50_sb', 'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50', 'search_bar', 1, '{}', datetime('now')),
    ('w_site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50_grid', 'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50', 'listing_grid', 1, '{}', datetime('now')),
    ('w_site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50_oh', 'site_7b8a1c9e-2e4a-4f51-a968-3d8b2d3e4f50', 'open_houses', 1, '{}', datetime('now'));
