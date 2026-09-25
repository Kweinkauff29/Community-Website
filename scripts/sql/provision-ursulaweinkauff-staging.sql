-- Provision dedicated site: ursulaweinkauff-com in sneak-idx-staging
-- Account: acc_1787583729221_cv3ma (Ursula Weinkauff — SNEAK Pilot)

INSERT INTO sneak_sites (
    id, account_id, site_key, site_name, status, scope_type, scope_value, created_at, updated_at
) VALUES (
    'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061',
    'acc_1787583729221_cv3ma',
    'ursulaweinkauff-com',
    'Ursula Weinkauff — Ursulaweinkauff.com',
    'active',
    'market',
    NULL,
    datetime('now'),
    datetime('now')
);

INSERT INTO sneak_domains (id, site_id, domain, verified, status, created_at) VALUES
    ('dom_stg_uw_1', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', 'ursulaweinkauff.com', 1, 'active', datetime('now')),
    ('dom_stg_uw_2', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', 'www.ursulaweinkauff.com', 1, 'active', datetime('now')),
    ('dom_stg_uw_local1', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', 'localhost', 1, 'active', datetime('now')),
    ('dom_stg_uw_local2', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', '127.0.0.1', 1, 'active', datetime('now'));

INSERT INTO sneak_branding (
    site_id, display_name, brokerage, logo_url, agent_photo_url,
    primary_color, secondary_color, phone, email, website_url, config_json
) VALUES (
    'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061',
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
    ('w_site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061_search', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', 'search', 1, '{}', datetime('now')),
    ('w_site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061_sb', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', 'search_bar', 1, '{}', datetime('now')),
    ('w_site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061_grid', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', 'listing_grid', 1, '{}', datetime('now')),
    ('w_site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061_oh', 'site_8c9b2d0f-3f5b-4e62-ba79-4e9c3e4f5061', 'open_houses', 1, '{}', datetime('now'));
