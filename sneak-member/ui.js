/**
 * sneak-member/ui.js
 * 
 * Single-Page Application for SNEAK Member Self-Service Portal:
 * - Overview KPI Dashboard
 * - Clients & Authenticated Buyer Activity Timeline (Phase 7.3C2B)
 * - Website & Custom Domain Provisioning
 * - Custom Branding & Color Palette
 * - Widget Config & HTML Embed Snippets
 * - Captured Inquiries & Leads
 * - GrowthZone Billing Alignment
 */

export function renderMemberUI() {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SNEAK IDX — Member Portal</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-base: #0a0e17;
            --bg-surface: #111827;
            --bg-surface-elevated: #1f2937;
            --bg-surface-highlight: #374151;
            --border-subtle: #1f2937;
            --border-strong: #374151;
            --text-primary: #f9fafb;
            --text-secondary: #9ca3af;
            --text-muted: #6b7280;
            --accent-primary: #3b82f6;
            --accent-hover: #2563eb;
            --accent-glow: rgba(59, 130, 246, 0.15);
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --purple: #8b5cf6;
            --radius-sm: 6px;
            --radius-md: 8px;
            --radius-lg: 12px;
            --radius-full: 9999px;
            --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.2), 0 4px 6px -2px rgba(0, 0, 0, 0.1);
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-base);
            color: var(--text-primary);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            line-height: 1.5;
        }

        /* Typography */
        h1, h2, h3, h4 { font-weight: 700; color: var(--text-primary); }
        code, pre { font-family: 'JetBrains Mono', monospace; font-size: 0.875rem; }

        /* App Layout */
        .app-container { display: flex; flex: 1; min-height: 100vh; }
        .sidebar {
            width: 260px;
            background: var(--bg-surface);
            border-right: 1px solid var(--border-subtle);
            display: flex;
            flex-direction: column;
            flex-shrink: 0;
        }
        .sidebar-header {
            padding: 24px 20px;
            border-bottom: 1px solid var(--border-subtle);
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .brand-badge {
            background: linear-gradient(135deg, #2563eb, #1d4ed8);
            color: #fff;
            padding: 4px 8px;
            border-radius: var(--radius-md);
            font-weight: 800;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
        }
        .sidebar-nav { padding: 16px 12px; display: flex; flex-direction: column; gap: 4px; flex: 1; overflow-y: auto; }
        .nav-item {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 10px 14px;
            color: var(--text-secondary);
            text-decoration: none;
            border-radius: var(--radius-md);
            font-weight: 500;
            font-size: 0.9rem;
            transition: all 0.15s ease;
            cursor: pointer;
            user-select: none;
        }
        .nav-item:hover { background: var(--bg-surface-elevated); color: var(--text-primary); }
        .nav-item.active { background: var(--accent-glow); color: var(--accent-primary); font-weight: 600; }

        .main-content { flex: 1; display: flex; flex-direction: column; background: var(--bg-base); min-width: 0; }
        .topbar {
            height: 64px;
            border-bottom: 1px solid var(--border-subtle);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 32px;
            background: var(--bg-surface);
            flex-shrink: 0;
        }
        .page-content { padding: 32px; flex: 1; overflow-y: auto; }

        /* Cards & Metrics */
        .grid-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 28px; }
        .stat-card {
            background: var(--bg-surface);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 20px;
        }
        .stat-label { color: var(--text-muted); font-size: 0.78rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
        .stat-val { font-size: 1.65rem; font-weight: 800; color: var(--text-primary); }

        .panel {
            background: var(--bg-surface);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 24px;
            margin-bottom: 24px;
        }
        .panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap; gap: 12px; }

        /* Forms & Buttons */
        .form-group { margin-bottom: 16px; }
        .form-label { display: block; font-size: 0.85rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 6px; }
        .form-control {
            width: 100%;
            padding: 10px 14px;
            background: var(--bg-base);
            border: 1px solid var(--border-strong);
            border-radius: var(--radius-md);
            color: var(--text-primary);
            font-size: 0.9rem;
            outline: none;
            transition: border-color 0.15s ease;
            font-family: inherit;
        }
        .form-control:focus { border-color: var(--accent-primary); }
        .form-select {
            padding: 8px 12px;
            background: var(--bg-base);
            border: 1px solid var(--border-strong);
            border-radius: var(--radius-md);
            color: var(--text-primary);
            font-size: 0.875rem;
            outline: none;
        }

        .btn {
            padding: 9px 16px;
            border-radius: var(--radius-md);
            font-weight: 600;
            font-size: 0.875rem;
            cursor: pointer;
            border: none;
            display: inline-flex;
            align-items: center;
            gap: 8px;
            font-family: inherit;
            transition: all 0.15s ease;
            text-decoration: none;
        }
        .btn-primary { background: var(--accent-primary); color: #fff; }
        .btn-primary:hover { background: var(--accent-hover); }
        .btn-secondary { background: var(--bg-surface-elevated); color: var(--text-primary); border: 1px solid var(--border-strong); }
        .btn-secondary:hover { background: var(--bg-surface-highlight); }
        .btn-danger { background: rgba(239, 68, 68, 0.1); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.2); }
        .btn-danger:hover { background: var(--danger); color: #fff; }
        .btn-sm { padding: 6px 12px; font-size: 0.8rem; }

        .badge { display: inline-flex; align-items: center; padding: 4px 10px; border-radius: 9999px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; }
        .badge-success { background: rgba(16, 185, 129, 0.15); color: var(--success); border: 1px solid rgba(16, 185, 129, 0.25); }
        .badge-warning { background: rgba(245, 158, 11, 0.15); color: var(--warning); border: 1px solid rgba(245, 158, 11, 0.25); }
        .badge-danger { background: rgba(239, 68, 68, 0.15); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.25); }
        .badge-purple { background: rgba(139, 92, 246, 0.15); color: var(--purple); border: 1px solid rgba(139, 92, 246, 0.25); }
        .badge-info { background: rgba(59, 130, 246, 0.15); color: var(--accent-primary); border: 1px solid rgba(59, 130, 246, 0.25); }

        .table-responsive { width: 100%; overflow-x: auto; margin-top: 16px; }
        .table { width: 100%; border-collapse: collapse; text-align: left; font-size: 0.875rem; }
        .table th { padding: 12px 16px; border-bottom: 1px solid var(--border-strong); color: var(--text-muted); font-weight: 600; text-transform: uppercase; font-size: 0.75rem; letter-spacing: 0.05em; white-space: nowrap; }
        .table td { padding: 14px 16px; border-bottom: 1px solid var(--border-subtle); color: var(--text-primary); vertical-align: middle; }
        .table tbody tr:hover { background: var(--bg-surface-elevated); }

        .code-box {
            background: var(--bg-base);
            border: 1px solid var(--border-strong);
            border-radius: var(--radius-md);
            padding: 14px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.825rem;
            color: #38bdf8;
            margin: 12px 0 16px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-break: break-all;
        }

        /* Modal & Drawer */
        .modal-overlay {
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0, 0, 0, 0.7);
            backdrop-filter: blur(4px);
            display: none;
            justify-content: center;
            align-items: center;
            z-index: 1000;
            padding: 20px;
        }
        .modal-overlay.open { display: flex; }
        .modal-card {
            background: var(--bg-surface);
            border: 1px solid var(--border-strong);
            border-radius: var(--radius-lg);
            width: 100%;
            max-width: 900px;
            max-height: 90vh;
            display: flex;
            flex-direction: column;
            box-shadow: var(--shadow-lg);
        }
        .modal-header {
            padding: 20px 24px;
            border-bottom: 1px solid var(--border-subtle);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .modal-body { padding: 24px; overflow-y: auto; flex: 1; }
        .modal-close-btn { background: transparent; border: none; color: var(--text-secondary); font-size: 1.5rem; cursor: pointer; padding: 4px; line-height: 1; }
        .modal-close-btn:hover { color: var(--text-primary); }

        /* Timeline Components */
        .timeline { position: relative; padding-left: 28px; margin-top: 16px; }
        .timeline::before { content: ''; position: absolute; left: 10px; top: 8px; bottom: 8px; width: 2px; background: var(--border-strong); }
        .timeline-item { position: relative; margin-bottom: 24px; }
        .timeline-item:last-child { margin-bottom: 0; }
        .timeline-marker {
            position: absolute;
            left: -28px;
            top: 2px;
            width: 22px;
            height: 22px;
            border-radius: 50%;
            background: var(--bg-surface-elevated);
            border: 2px solid var(--accent-primary);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 10px;
        }
        .timeline-content {
            background: var(--bg-base);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-md);
            padding: 14px 16px;
        }
        .timeline-meta { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; font-size: 0.8rem; color: var(--text-muted); }
        .timeline-title { font-size: 0.95rem; font-weight: 700; color: var(--text-primary); margin-bottom: 4px; }

        /* Property Mini-Card in Detail/Timeline */
        .mini-listing-card {
            display: flex;
            gap: 12px;
            background: var(--bg-surface);
            border: 1px solid var(--border-strong);
            border-radius: var(--radius-sm);
            padding: 10px;
            margin-top: 8px;
            align-items: center;
        }
        .mini-listing-img { width: 64px; height: 48px; border-radius: 4px; object-fit: cover; background: #1e293b; flex-shrink: 0; }
        .mini-listing-info { flex: 1; min-width: 0; }
        .mini-listing-price { font-weight: 700; color: var(--text-primary); font-size: 0.9rem; }
        .mini-listing-addr { color: var(--text-secondary); font-size: 0.8rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

        .properties-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 14px; margin-top: 14px; }
        .property-grid-card {
            background: var(--bg-base);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-md);
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }
        .property-grid-card img { width: 100%; height: 130px; object-fit: cover; background: #1e293b; }
        .property-grid-body { padding: 10px 12px; flex: 1; }

        /* Tabs inside Client Detail Modal */
        .subtab-nav { display: flex; gap: 8px; border-bottom: 1px solid var(--border-subtle); margin-bottom: 20px; padding-bottom: 8px; }
        .subtab-btn { background: transparent; border: none; color: var(--text-secondary); padding: 8px 14px; font-weight: 600; font-size: 0.875rem; border-radius: var(--radius-md); cursor: pointer; }
        .subtab-btn.active { background: var(--bg-surface-elevated); color: var(--accent-primary); }

        /* Responsive Breakpoints */
        @media (max-width: 1024px) {
            .sidebar { width: 220px; }
            .page-content { padding: 20px; }
            .topbar { padding: 0 20px; }
        }
        @media (max-width: 768px) {
            .app-container { flex-direction: column; }
            .sidebar { width: 100%; border-right: none; border-bottom: 1px solid var(--border-subtle); }
            .sidebar-nav { flex-direction: row; overflow-x: auto; padding: 10px; }
            .grid-cards { grid-template-columns: 1fr; }
            .modal-card { max-height: 95vh; padding: 12px; }
        }
    </style>
</head>
<body>
    <!-- Auth / Login Screen -->
    <div id="authScreen" style="display: flex; align-items: center; justify-content: center; min-height: 100vh; padding: 20px;">
        <div style="background: var(--bg-surface); border: 1px solid var(--border-subtle); border-radius: var(--radius-lg); padding: 40px; width: 100%; max-width: 440px; box-shadow: var(--shadow-lg);">
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 24px;">
                <span class="brand-badge">CCOR IDX</span>
                <h2 style="font-size: 1.3rem;">Member Portal</h2>
            </div>
            <p style="color: var(--text-secondary); font-size: 0.9rem; margin-bottom: 24px;">
                Sign in to manage your MLS IDX sites, client buyer activity, custom domains, widgets, and leads.
            </p>
            <div id="loginMsg" style="display: none; padding: 12px; border-radius: var(--radius-md); font-size: 0.875rem; margin-bottom: 20px;"></div>
            <form id="magicLoginForm">
                <div class="form-group">
                    <label class="form-label" for="memberEmail">REALTOR® Email Address</label>
                    <input type="email" id="memberEmail" class="form-control" placeholder="agent@example.com" required autofocus>
                </div>
                <button type="submit" id="loginBtn" class="btn btn-primary" style="width: 100%; justify-content: center; padding: 12px;">
                    Send Magic Sign-In Link
                </button>
            </form>
        </div>
    </div>

    <!-- App Container -->
    <div id="appContainer" class="app-container" style="display: none;">
        <aside class="sidebar">
            <div class="sidebar-header">
                <span class="brand-badge">CCOR IDX</span>
                <span style="font-weight: 700; font-size: 0.95rem;">Member Portal</span>
            </div>
            <nav class="sidebar-nav">
                <div class="nav-item active" onclick="switchTab('overview')">Overview</div>
                <div class="nav-item" onclick="switchTab('clients')">Clients</div>
                <div class="nav-item" onclick="switchTab('domains')">Website & Domains</div>
                <div class="nav-item" onclick="switchTab('branding')">Branding</div>
                <div class="nav-item" onclick="switchTab('widgets')">Widgets</div>
                <div class="nav-item" onclick="switchTab('embed')">Embed Code</div>
                <div class="nav-item" onclick="switchTab('leads')">Leads</div>
                <div class="nav-item" onclick="switchTab('billing')">Subscription & Billing</div>
            </nav>
            <div style="padding: 16px; border-top: 1px solid var(--border-subtle);">
                <button class="btn btn-secondary" style="width: 100%; justify-content: center;" onclick="logout()">Sign Out</button>
            </div>
        </aside>

        <main class="main-content">
            <header class="topbar">
                <div id="accountTitle" style="font-weight: 700;">Loading account...</div>
                <div id="entitlementBadge"></div>
            </header>

            <div class="page-content">
                <!-- Overview Tab -->
                <div id="tab-overview" class="tab-pane">
                    <div class="grid-cards">
                        <div class="stat-card">
                            <div class="stat-label">Registered Clients</div>
                            <div class="stat-val" id="statClients">-</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Active Buyers (7d)</div>
                            <div class="stat-val" id="statActiveClients">-</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Saved Homes</div>
                            <div class="stat-val" id="statSavedHomes">-</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Total Leads Captured</div>
                            <div class="stat-val" id="statLeads">-</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Active MLS Listings</div>
                            <div class="stat-val" id="statListings">-</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Subscription Status</div>
                            <div class="stat-val" style="font-size: 1.25rem;" id="statBilling">-</div>
                        </div>
                    </div>

                    <div class="panel">
                        <div class="panel-header">
                            <h3>IDX Site Configuration</h3>
                            <button class="btn btn-secondary" onclick="switchTab('embed')">Get Embed Snippets</button>
                        </div>
                        <div id="siteDetails"></div>
                    </div>
                </div>

                <!-- Clients Tab (Phase 7.3C2B) -->
                <div id="tab-clients" class="tab-pane" style="display: none;">
                    <div class="grid-cards">
                        <div class="stat-card">
                            <div class="stat-label">Total Registered Buyers</div>
                            <div class="stat-val" id="clientKpiTotal">0</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Active in Last 7 Days</div>
                            <div class="stat-val" id="clientKpiActive7d">0</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Total Saved Homes</div>
                            <div class="stat-val" id="clientKpiSavedHomes">0</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Active Saved Searches</div>
                            <div class="stat-val" id="clientKpiSavedSearches">0</div>
                        </div>
                    </div>

                    <div class="panel">
                        <div class="panel-header">
                            <div>
                                <h3>Authenticated Buyer Clients</h3>
                                <p style="color: var(--text-secondary); font-size: 0.85rem; margin-top: 4px;">
                                    Track authenticated buyers using your CCOR IDX website, their saved homes, and real-time engagement activity.
                                </p>
                            </div>
                        </div>

                        <!-- Search & Filter Controls -->
                        <div style="display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; align-items: center; justify-content: space-between;">
                            <div style="display: flex; gap: 12px; flex: 1; min-width: 260px; max-width: 500px;">
                                <input type="text" id="clientSearchInput" class="form-control" placeholder="Search by buyer email..." oninput="debounceClientSearch()">
                            </div>
                            <div style="display: flex; gap: 12px; align-items: center;">
                                <label style="font-size: 0.85rem; color: var(--text-secondary); white-space: nowrap;">Sort by:</label>
                                <select id="clientSortSelect" class="form-select" onchange="loadClients(1)">
                                    <option value="recently_active">Recently Active</option>
                                    <option value="newest">Newest Registered</option>
                                    <option value="saved_homes">Most Saved Homes</option>
                                    <option value="saved_searches">Most Saved Searches</option>
                                </select>
                            </div>
                        </div>

                        <div class="table-responsive">
                            <table class="table">
                                <thead>
                                    <tr>
                                        <th>Buyer Email</th>
                                        <th>Site</th>
                                        <th>Status</th>
                                        <th>Last Active</th>
                                        <th>Saved Homes</th>
                                        <th>Saved Searches</th>
                                        <th>Inquiries</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody id="clientsTableBody">
                                    <tr><td colspan="8" style="text-align: center; color: var(--text-muted); padding: 32px;">Loading clients...</td></tr>
                                </tbody>
                            </table>
                        </div>

                        <!-- Pagination Controls -->
                        <div id="clientPaginationBar" style="display: flex; justify-content: space-between; align-items: center; margin-top: 20px; padding-top: 16px; border-top: 1px solid var(--border-subtle);">
                            <div id="clientPageInfo" style="font-size: 0.85rem; color: var(--text-muted);">Page 1 of 1</div>
                            <div style="display: flex; gap: 8px;">
                                <button id="clientPrevBtn" class="btn btn-secondary btn-sm" onclick="changeClientPage(-1)" disabled>Previous</button>
                                <button id="clientNextBtn" class="btn btn-secondary btn-sm" onclick="changeClientPage(1)" disabled>Next</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Domains Tab -->
                <div id="tab-domains" class="tab-pane" style="display: none;">
                    <div class="panel">
                        <div class="panel-header">
                            <h3>Authorized Website Domains</h3>
                        </div>
                        <p style="color: var(--text-secondary); font-size: 0.875rem; margin-bottom: 20px;">
                            Add the hostname of your website where your IDX widget will be embedded. Newly added domains require administrator verification before live serving.
                        </p>
                        <form id="addDomainForm" style="display: flex; gap: 12px; margin-bottom: 24px;">
                            <input type="text" id="newDomainInput" class="form-control" placeholder="myrealtywebsite.com" required style="max-width: 360px;">
                            <button type="submit" class="btn btn-primary">Add Domain</button>
                        </form>
                        <div class="table-responsive">
                            <table class="table">
                                <thead>
                                    <tr>
                                        <th>Domain</th>
                                        <th>Verification Status</th>
                                        <th>Added Date</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody id="domainsTableBody"></tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- Branding Tab -->
                <div id="tab-branding" class="tab-pane" style="display: none;">
                    <div class="panel" style="max-width: 600px;">
                        <div class="panel-header">
                            <h3>Custom Brand Styling</h3>
                        </div>
                        <form id="brandingForm">
                            <div class="form-group">
                                <label class="form-label">Display Name / Title</label>
                                <input type="text" id="brandDisplayName" class="form-control">
                            </div>
                            <div class="form-group">
                                <label class="form-label">Brokerage Name</label>
                                <input type="text" id="brandBrokerage" class="form-control">
                            </div>
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                                <div class="form-group">
                                    <label class="form-label">Primary Color (Hex)</label>
                                    <input type="text" id="brandPrimaryColor" class="form-control" placeholder="#1a365d">
                                </div>
                                <div class="form-group">
                                    <label class="form-label">Secondary Accent Color</label>
                                    <input type="text" id="brandSecondaryColor" class="form-control" placeholder="#2596be">
                                </div>
                            </div>
                            <div class="form-group">
                                <label class="form-label">Logo URL</label>
                                <input type="url" id="brandLogoUrl" class="form-control" placeholder="https://example.com/logo.png">
                            </div>
                            <div class="form-group">
                                <label class="form-label">Agent Headshot URL</label>
                                <input type="url" id="brandPhotoUrl" class="form-control" placeholder="https://example.com/photo.jpg">
                            </div>
                            <button type="submit" class="btn btn-primary">Save Branding Changes</button>
                        </form>
                    </div>
                </div>

                <!-- Widgets Tab -->
                <div id="tab-widgets" class="tab-pane" style="display: none;">
                    <div class="panel">
                        <div class="panel-header">
                            <h3>IDX Widget Management</h3>
                        </div>
                        <p style="color: var(--text-secondary); font-size: 0.875rem;">
                            Customize widget settings and configurations for your embedded property search components.
                        </p>
                    </div>
                </div>

                <!-- Embed Tab -->
                <div id="tab-embed" class="tab-pane" style="display: none;">
                    <div class="panel">
                        <div class="panel-header">
                            <h3>Full Search Embed</h3>
                        </div>
                        <p style="color: var(--text-secondary); font-size: 0.875rem;">Copy and paste this HTML snippet into any page on your authorized website.</p>
                        <div class="code-box" id="embedSearchCode">Loading snippet...</div>
                        <button class="btn btn-secondary" onclick="copySnippet('embedSearchCode')">Copy HTML Code</button>
                    </div>

                    <div class="panel">
                        <div class="panel-header">
                            <h3>Quick Search Bar Widget</h3>
                        </div>
                        <div class="code-box" id="embedBarCode">Loading snippet...</div>
                        <button class="btn btn-secondary" onclick="copySnippet('embedBarCode')">Copy HTML Code</button>
                    </div>

                    <div class="panel">
                        <div class="panel-header">
                            <h3>Open Houses Widget</h3>
                        </div>
                        <div class="code-box" id="embedOhCode">Loading snippet...</div>
                        <button class="btn btn-secondary" onclick="copySnippet('embedOhCode')">Copy HTML Code</button>
                    </div>
                </div>

                <!-- Leads Tab -->
                <div id="tab-leads" class="tab-pane" style="display: none;">
                    <div class="panel">
                        <div class="panel-header">
                            <h3>Captured Client Inquiries & Leads</h3>
                        </div>
                        <div class="table-responsive">
                            <table class="table">
                                <thead>
                                    <tr>
                                        <th>Client Name</th>
                                        <th>Email</th>
                                        <th>Phone</th>
                                        <th>Listing Key</th>
                                        <th>Date Captured</th>
                                    </tr>
                                </thead>
                                <tbody id="leadsTableBody"></tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- Billing Tab -->
                <div id="tab-billing" class="tab-pane" style="display: none;">
                    <div class="panel" style="max-width: 600px;">
                        <div class="panel-header">
                            <h3>Subscription & Billing</h3>
                        </div>
                        <div id="billingDetails" style="margin-bottom: 24px;"></div>
                        <div style="display: flex; gap: 12px;">
                            <a id="growthzoneLink" href="https://bonitaspringsesterorealtorsfl.growthzoneapp.com/" target="_blank" rel="noopener noreferrer" class="btn btn-primary">
                                Manage Billing in GrowthZone
                            </a>
                        </div>
                    </div>
                </div>
            </div>
        </main>
    </div>

    <!-- Client Detail & Activity Modal / Drawer (Phase 7.3C2B) -->
    <div id="clientDetailModal" class="modal-overlay">
        <div class="modal-card">
            <div class="modal-header">
                <div>
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <h3 id="clientDetailEmail" style="font-size: 1.2rem;">buyer@example.com</h3>
                        <span id="clientDetailStatusBadge" class="badge badge-success">Active</span>
                    </div>
                    <div id="clientDetailMeta" style="color: var(--text-muted); font-size: 0.8rem; margin-top: 4px;">Registered on Site Name</div>
                </div>
                <button class="modal-close-btn" onclick="closeClientDetail()">&times;</button>
            </div>
            
            <div class="modal-body">
                <!-- Engagement Metric Cards -->
                <div class="grid-cards" style="margin-bottom: 20px;">
                    <div class="stat-card" style="padding: 14px;">
                        <div class="stat-label">Saved Homes</div>
                        <div class="stat-val" id="clientDetailSavedHomesCount" style="font-size: 1.3rem;">0</div>
                    </div>
                    <div class="stat-card" style="padding: 14px;">
                        <div class="stat-label">Saved Searches</div>
                        <div class="stat-val" id="clientDetailSavedSearchesCount" style="font-size: 1.3rem;">0</div>
                    </div>
                    <div class="stat-card" style="padding: 14px;">
                        <div class="stat-label">Active Alerts</div>
                        <div class="stat-val" id="clientDetailAlertsCount" style="font-size: 1.3rem;">0</div>
                    </div>
                    <div class="stat-card" style="padding: 14px;">
                        <div class="stat-label">Inquiries</div>
                        <div class="stat-val" id="clientDetailInquiriesCount" style="font-size: 1.3rem;">0</div>
                    </div>
                </div>

                <!-- Subtab Navigation -->
                <div class="subtab-nav">
                    <button class="subtab-btn active" onclick="switchClientSubtab('activity')">Activity Timeline</button>
                    <button class="subtab-btn" onclick="switchClientSubtab('homes')">Saved Homes</button>
                    <button class="subtab-btn" onclick="switchClientSubtab('searches')">Saved Searches & Alerts</button>
                    <button class="subtab-btn" onclick="switchClientSubtab('inquiries')">Inquiries & Leads</button>
                </div>

                <!-- Subtab 1: Activity Timeline -->
                <div id="clientSubtab-activity" class="client-subtab-pane">
                    <div id="clientActivityTimeline" class="timeline">
                        <div style="color: var(--text-muted); font-size: 0.875rem;">Loading activity ledger...</div>
                    </div>
                </div>

                <!-- Subtab 2: Saved Homes -->
                <div id="clientSubtab-homes" class="client-subtab-pane" style="display: none;">
                    <div id="clientSavedHomesGrid" class="properties-grid">
                        <div style="color: var(--text-muted); font-size: 0.875rem;">No saved homes.</div>
                    </div>
                </div>

                <!-- Subtab 3: Saved Searches -->
                <div id="clientSubtab-searches" class="client-subtab-pane" style="display: none;">
                    <div id="clientSavedSearchesList">
                        <div style="color: var(--text-muted); font-size: 0.875rem;">No saved searches.</div>
                    </div>
                </div>

                <!-- Subtab 4: Inquiries -->
                <div id="clientSubtab-inquiries" class="client-subtab-pane" style="display: none;">
                    <div id="clientInquiriesList">
                        <div style="color: var(--text-muted); font-size: 0.875rem;">No inquiries recorded.</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let currentAccount = null;
        let clientCurrentPage = 1;
        let clientTotalPages = 1;
        let clientSearchTimer = null;
        let activeClientId = null;

        function escapeHtml(str) {
            if (str === null || str === undefined) return '';
            return String(str)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#039;');
        }

        function formatRelativeDate(isoStr) {
            if (!isoStr) return 'Never';
            const date = new Date(isoStr);
            if (isNaN(date.getTime())) return 'Never';
            return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit' });
        }

        async function init() {
            const urlParams = new URLSearchParams(window.location.search);
            const token = urlParams.get('token');
            if (token) {
                await verifyMagicToken(token);
                return;
            }

            try {
                const res = await fetch('/api/member/overview');
                if (res.status === 200) {
                    const data = await res.json();
                    renderPortal(data);
                } else {
                    showAuth();
                }
            } catch {
                showAuth();
            }
        }

        function showAuth() {
            document.getElementById('authScreen').style.display = 'flex';
            document.getElementById('appContainer').style.display = 'none';
        }

        async function verifyMagicToken(token) {
            try {
                const res = await fetch('/api/member/auth/verify?token=' + encodeURIComponent(token));
                if (res.status === 200) {
                    window.history.replaceState({}, document.title, window.location.pathname);
                    window.location.reload();
                } else {
                    showAuth();
                    const msg = document.getElementById('loginMsg');
                    msg.style.display = 'block';
                    msg.style.background = 'rgba(239, 68, 68, 0.15)';
                    msg.style.color = '#ef4444';
                    msg.innerText = 'Magic link expired or already used. Please request a new one.';
                }
            } catch {
                showAuth();
            }
        }

        document.getElementById('magicLoginForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const email = document.getElementById('memberEmail').value;
            const btn = document.getElementById('loginBtn');
            btn.disabled = true;
            btn.innerText = 'Sending Link...';

            try {
                const res = await fetch('/api/member/auth/magic-link', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'Origin': window.location.origin },
                    body: JSON.stringify({ email })
                });
                const data = await res.json();
                const msg = document.getElementById('loginMsg');
                msg.style.display = 'block';
                msg.style.background = 'rgba(16, 185, 129, 0.15)';
                msg.style.color = '#10b981';
                msg.innerText = data.message || 'Magic link generated! Check your email for the link.';
            } catch {
                alert('Failed to send magic link.');
            } finally {
                btn.disabled = false;
                btn.innerText = 'Send Magic Sign-In Link';
            }
        });

        function renderPortal(data) {
            currentAccount = data;
            document.getElementById('authScreen').style.display = 'none';
            document.getElementById('appContainer').style.display = 'flex';

            document.getElementById('accountTitle').innerText = data.account.account_name + ' (' + data.account.plan.toUpperCase() + ')';
            document.getElementById('statClients').innerText = (data.clientsCount || 0).toLocaleString();
            document.getElementById('statActiveClients').innerText = (data.activeClients7dCount || 0).toLocaleString();
            document.getElementById('statSavedHomes').innerText = (data.savedHomesCount || 0).toLocaleString();
            document.getElementById('statListings').innerText = data.inventory?.activeListings?.toLocaleString() || '0';
            document.getElementById('statLeads').innerText = (data.leadsCount || 0).toLocaleString();
            
            // Client KPI cards
            document.getElementById('clientKpiTotal').innerText = (data.clientsCount || 0).toLocaleString();
            document.getElementById('clientKpiActive7d').innerText = (data.activeClients7dCount || 0).toLocaleString();
            document.getElementById('clientKpiSavedHomes').innerText = (data.savedHomesCount || 0).toLocaleString();
            document.getElementById('clientKpiSavedSearches').innerText = (data.savedSearchesCount || 0).toLocaleString();

            const ent = data.billing?.entitlement_status || 'inactive';
            const badge = document.getElementById('entitlementBadge');
            if (ent === 'active') {
                badge.innerHTML = '<span class="badge badge-success">Live IDX Active</span>';
                document.getElementById('statBilling').innerText = 'Active';
            } else if (ent === 'grace') {
                badge.innerHTML = '<span class="badge badge-warning">Grace Period</span>';
                document.getElementById('statBilling').innerText = 'Past Due (Grace)';
            } else {
                badge.innerHTML = '<span class="badge badge-danger">Subscription Required</span>';
                document.getElementById('statBilling').innerText = 'Inactive';
            }

            if (data.site) {
                document.getElementById('siteDetails').innerHTML = \`
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-top: 12px;">
                        <div><strong style="color: var(--text-muted); font-size: 0.8rem;">SITE KEY:</strong><br><code>\${escapeHtml(data.site.site_key)}</code></div>
                        <div><strong style="color: var(--text-muted); font-size: 0.8rem;">SCOPE TYPE:</strong><br>\${escapeHtml(data.site.scope_type.toUpperCase())}</div>
                        <div><strong style="color: var(--text-muted); font-size: 0.8rem;">MLS IDENTIFIER:</strong><br><code>\${escapeHtml(data.site.scope_value || 'Market')}</code></div>
                    </div>
                \`;
            }

            renderDomains(data.domains || []);
            if (data.branding) {
                document.getElementById('brandDisplayName').value = data.branding.display_name || '';
                document.getElementById('brandBrokerage').value = data.branding.brokerage || '';
                document.getElementById('brandPrimaryColor').value = data.branding.primary_color || '';
                document.getElementById('brandSecondaryColor').value = data.branding.secondary_color || '';
                document.getElementById('brandLogoUrl').value = data.branding.logo_url || '';
                document.getElementById('brandPhotoUrl').value = data.branding.agent_photo_url || '';
            }

            if (data.embed?.snippets) {
                document.getElementById('embedSearchCode').innerText = data.embed.snippets.search?.htmlSnippet || '';
                document.getElementById('embedBarCode').innerText = data.embed.snippets.search_bar?.htmlSnippet || '';
                document.getElementById('embedOhCode').innerText = data.embed.snippets.open_houses?.htmlSnippet || '';
            }

            renderBilling(data.billing);
        }

        function switchTab(tabId) {
            document.querySelectorAll('.tab-pane').forEach(p => p.style.display = 'none');
            document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
            const targetPane = document.getElementById('tab-' + tabId);
            if (targetPane) targetPane.style.display = 'block';
            if (event && event.currentTarget) event.currentTarget.classList.add('active');

            if (tabId === 'clients') {
                loadClients(1);
            } else if (tabId === 'leads') {
                loadLeads();
            }
        }

        // --- Clients Management (Phase 7.3C2B) ---
        function debounceClientSearch() {
            clearTimeout(clientSearchTimer);
            clientSearchTimer = setTimeout(() => {
                loadClients(1);
            }, 300);
        }

        async function loadClients(page = 1) {
            clientCurrentPage = page;
            const search = document.getElementById('clientSearchInput')?.value || '';
            const sort = document.getElementById('clientSortSelect')?.value || 'recently_active';
            const tbody = document.getElementById('clientsTableBody');

            tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-muted); padding: 24px;">Loading clients...</td></tr>';

            try {
                const res = await fetch(\`/api/member/clients?search=\${encodeURIComponent(search)}&sort=\${encodeURIComponent(sort)}&page=\${page}&limit=20\`);
                if (!res.ok) throw new Error('Failed to fetch clients');

                const data = await res.json();
                clientTotalPages = data.totalPages || 1;

                if (!data.clients || data.clients.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--text-muted); padding: 32px;">No authenticated clients found.</td></tr>';
                } else {
                    tbody.innerHTML = data.clients.map(c => \`
                        <tr>
                            <td>
                                <strong>\${escapeHtml(c.email)}</strong>
                                <div style="font-size: 0.75rem; color: var(--text-muted);">ID: \${escapeHtml(c.id)}</div>
                            </td>
                            <td>\${escapeHtml(c.siteName)}</td>
                            <td><span class="badge \${c.status === 'active' ? 'badge-success' : 'badge-danger'}">\${escapeHtml(c.status)}</span></td>
                            <td>\${formatRelativeDate(c.lastActivityAt)}</td>
                            <td><strong>\${c.savedHomesCount}</strong></td>
                            <td><strong>\${c.savedSearchesCount}</strong> \${c.alertsCount > 0 ? '<span class="badge badge-info" style="font-size: 0.65rem; padding: 2px 6px;">' + c.alertsCount + ' Alert</span>' : ''}</td>
                            <td><strong>\${c.inquiriesCount}</strong></td>
                            <td>
                                <button class="btn btn-secondary btn-sm" onclick="openClientDetail('\${escapeHtml(c.id)}')">
                                    View Activity
                                </button>
                            </td>
                        </tr>
                    \`).join('');
                }

                // Update pagination controls
                document.getElementById('clientPageInfo').textContent = \`Page \${data.page} of \${clientTotalPages} (\${data.total} clients)\`;
                document.getElementById('clientPrevBtn').disabled = (data.page <= 1);
                document.getElementById('clientNextBtn').disabled = (data.page >= clientTotalPages);

            } catch (err) {
                tbody.innerHTML = '<tr><td colspan="8" style="text-align: center; color: var(--danger); padding: 24px;">Error loading clients.</td></tr>';
            }
        }

        function changeClientPage(delta) {
            const newPage = clientCurrentPage + delta;
            if (newPage >= 1 && newPage <= clientTotalPages) {
                loadClients(newPage);
            }
        }

        // Client Detail Modal
        async function openClientDetail(consumerId) {
            activeClientId = consumerId;
            const modal = document.getElementById('clientDetailModal');
            modal.classList.add('open');

            document.getElementById('clientDetailEmail').textContent = 'Loading client...';
            document.getElementById('clientActivityTimeline').innerHTML = '<div style="color: var(--text-muted);">Loading activity ledger...</div>';
            document.getElementById('clientSavedHomesGrid').innerHTML = '<div style="color: var(--text-muted);">Loading saved homes...</div>';
            document.getElementById('clientSavedSearchesList').innerHTML = '<div style="color: var(--text-muted);">Loading saved searches...</div>';
            document.getElementById('clientInquiriesList').innerHTML = '<div style="color: var(--text-muted);">Loading inquiries...</div>';

            switchClientSubtab('activity');

            try {
                const [detailRes, activityRes] = await Promise.all([
                    fetch(\`/api/member/clients/\${encodeURIComponent(consumerId)}\`),
                    fetch(\`/api/member/clients/\${encodeURIComponent(consumerId)}/activity?limit=50\`)
                ]);

                if (!detailRes.ok) throw new Error('Client not found');
                const detailData = await detailRes.json();
                const activityData = activityRes.ok ? await activityRes.json() : { events: [] };

                // Populate Header & Engagement Counters
                const c = detailData.client;
                document.getElementById('clientDetailEmail').textContent = c.email;
                document.getElementById('clientDetailStatusBadge').textContent = c.status.toUpperCase();
                document.getElementById('clientDetailStatusBadge').className = 'badge ' + (c.status === 'active' ? 'badge-success' : 'badge-danger');
                document.getElementById('clientDetailMeta').textContent = \`Registered \${formatRelativeDate(c.createdAt)} on \${c.siteName} • Last Active: \${formatRelativeDate(c.lastActivityAt)}\`;

                document.getElementById('clientDetailSavedHomesCount').textContent = detailData.engagement.savedHomesCount || 0;
                document.getElementById('clientDetailSavedSearchesCount').textContent = detailData.engagement.savedSearchesCount || 0;
                document.getElementById('clientDetailAlertsCount').textContent = detailData.engagement.alertsCount || 0;
                document.getElementById('clientDetailInquiriesCount').textContent = detailData.engagement.inquiriesCount || 0;

                // Render Timeline
                renderClientTimeline(activityData.events || []);

                // Render Saved Homes
                renderClientSavedHomes(detailData.savedHomes || []);

                // Render Saved Searches
                renderClientSavedSearches(detailData.savedSearches || []);

                // Render Inquiries
                renderClientInquiries(detailData.inquiries || []);

            } catch (err) {
                alert('Failed to load client details.');
                closeClientDetail();
            }
        }

        function closeClientDetail() {
            document.getElementById('clientDetailModal').classList.remove('open');
            activeClientId = null;
        }

        function switchClientSubtab(tab) {
            document.querySelectorAll('.client-subtab-pane').forEach(p => p.style.display = 'none');
            document.querySelectorAll('.subtab-btn').forEach(b => b.classList.remove('active'));
            const target = document.getElementById('clientSubtab-' + tab);
            if (target) target.style.display = 'block';
            if (event && event.currentTarget) event.currentTarget.classList.add('active');
        }

        function renderClientTimeline(events) {
            const container = document.getElementById('clientActivityTimeline');
            if (!events.length) {
                container.innerHTML = '<div style="color: var(--text-muted); padding: 16px 0;">No activity events recorded for this buyer yet.</div>';
                return;
            }

            const eventLabels = {
                listing_view: { label: 'Viewed Property', icon: '👁️', color: 'var(--accent-primary)' },
                favorite_added: { label: 'Saved Home to Favorites', icon: '❤️', color: 'var(--danger)' },
                favorite_removed: { label: 'Removed Home from Favorites', icon: '💔', color: 'var(--text-muted)' },
                saved_search_created: { label: 'Created Saved Search', icon: '🔍', color: 'var(--purple)' },
                saved_search_updated: { label: 'Updated Saved Search', icon: '✏️', color: 'var(--purple)' },
                saved_search_deleted: { label: 'Deleted Saved Search', icon: '🗑️', color: 'var(--text-muted)' },
                alert_enabled: { label: 'Enabled Email Alerts', icon: '🔔', color: 'var(--success)' },
                alert_frequency_changed: { label: 'Changed Alert Frequency', icon: '🎛️', color: 'var(--warning)' },
                alert_disabled: { label: 'Disabled Email Alerts', icon: '🔕', color: 'var(--text-muted)' },
                inquiry_submitted: { label: 'Submitted Property Inquiry', icon: '✉️', color: 'var(--success)' }
            };

            container.innerHTML = events.map(e => {
                const conf = eventLabels[e.type] || { label: e.type, icon: '•', color: 'var(--accent-primary)' };
                let extraHtml = '';

                if (e.listing) {
                    const priceFmt = e.listing.price ? '$' + Number(e.listing.price).toLocaleString() : '';
                    extraHtml = \`
                        <div class="mini-listing-card">
                            <img src="\${escapeHtml(e.listing.primaryPhoto || '')}" class="mini-listing-img" alt="Listing" onerror="this.style.display='none'">
                            <div class="mini-listing-info">
                                <div class="mini-listing-price">\${escapeHtml(priceFmt)}</div>
                                <div class="mini-listing-addr">\${escapeHtml(e.listing.address || 'Address Undisclosed')}\${e.listing.city ? ', ' + escapeHtml(e.listing.city) : ''}</div>
                            </div>
                        </div>
                    \`;
                } else if (e.listingKey) {
                    extraHtml = \`
                        <div style="font-size: 0.8rem; color: var(--text-muted); margin-top: 4px;">
                            Listing Key: <code>\${escapeHtml(e.listingKey)}</code> (Property no longer available or off-market)
                        </div>
                    \`;
                }

                if (e.searchName) {
                    extraHtml += \`
                        <div style="font-size: 0.85rem; color: var(--text-secondary); margin-top: 4px;">
                            Search: <strong>\${escapeHtml(e.searchName)}</strong>
                        </div>
                    \`;
                }

                if (e.metadata?.frequency) {
                    extraHtml += \`
                        <div style="margin-top: 4px;">
                            <span class="badge badge-info">Frequency: \${escapeHtml(e.metadata.frequency.toUpperCase())}</span>
                        </div>
                    \`;
                }

                return \`
                    <div class="timeline-item">
                        <div class="timeline-marker" style="border-color: \${conf.color};">\${conf.icon}</div>
                        <div class="timeline-content">
                            <div class="timeline-meta">
                                <span>\${formatRelativeDate(e.createdAt)}</span>
                                <span class="badge badge-info" style="font-size: 0.65rem;">\${escapeHtml(e.type)}</span>
                            </div>
                            <div class="timeline-title">\${escapeHtml(conf.label)}</div>
                            \${extraHtml}
                        </div>
                    </div>
                \`;
            }).join('');
        }

        function renderClientSavedHomes(homes) {
            const grid = document.getElementById('clientSavedHomesGrid');
            if (!homes.length) {
                grid.innerHTML = '<div style="color: var(--text-muted); grid-column: 1 / -1; padding: 16px 0;">No saved homes.</div>';
                return;
            }

            grid.innerHTML = homes.map(h => {
                if (h.unavailable) {
                    return \`
                        <div class="property-grid-card">
                            <div style="height: 130px; background: #1e293b; display: flex; align-items: center; justify-content: center; color: var(--text-muted); font-size: 0.8rem; padding: 12px; text-align: center;">
                                Property no longer available
                            </div>
                            <div class="property-grid-body">
                                <div style="font-size: 0.75rem; color: var(--text-muted);">MLS: \${escapeHtml(h.listingKey)}</div>
                                <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 4px;">Saved: \${formatRelativeDate(h.createdAt)}</div>
                            </div>
                        </div>
                    \`;
                }

                const price = h.price ? '$' + Number(h.price).toLocaleString() : '$0';
                return \`
                    <div class="property-grid-card">
                        <img src="\${escapeHtml(h.primaryPhoto || '')}" alt="Property" onerror="this.src='data:image/svg+xml;utf8,<svg xmlns=\\'http://www.w3.org/2000/svg\\' width=\\'100\\' height=\\'100\\'><rect fill=\\'%231e293b\\' width=\\'100\\' height=\\'100\\'/></svg>'">
                        <div class="property-grid-body">
                            <div style="font-weight: 800; font-size: 1rem; color: var(--text-primary);">\${escapeHtml(price)}</div>
                            <div style="font-size: 0.8rem; color: var(--text-secondary); margin-top: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">
                                \${escapeHtml(h.address)}\${h.city ? ', ' + escapeHtml(h.city) : ''}
                            </div>
                            <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 6px;">
                                \${h.bedrooms ? h.bedrooms + ' Beds • ' : ''}\${h.bathrooms ? h.bathrooms + ' Baths • ' : ''}\${h.livingArea ? Number(h.livingArea).toLocaleString() + ' sqft' : ''}
                            </div>
                            <div style="font-size: 0.7rem; color: var(--text-muted); margin-top: 6px; border-top: 1px solid var(--border-subtle); padding-top: 6px;">
                                Saved \${formatRelativeDate(h.createdAt)}
                            </div>
                        </div>
                    </div>
                \`;
            }).join('');
        }

        function renderClientSavedSearches(searches) {
            const list = document.getElementById('clientSavedSearchesList');
            if (!searches.length) {
                list.innerHTML = '<div style="color: var(--text-muted); padding: 16px 0;">No saved searches.</div>';
                return;
            }

            list.innerHTML = searches.map(s => \`
                <div style="background: var(--bg-base); border: 1px solid var(--border-subtle); border-radius: var(--radius-md); padding: 14px 16px; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
                    <div>
                        <div style="font-weight: 700; font-size: 0.95rem;">\${escapeHtml(s.name)}</div>
                        <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 4px;">Created: \${formatRelativeDate(s.createdAt)} • Updated: \${formatRelativeDate(s.updatedAt)}</div>
                    </div>
                    <div>
                        <span class="badge \${s.alertEnabled ? 'badge-success' : 'badge-warning'}">
                            Alert: \${escapeHtml(s.alertFrequency.toUpperCase())}
                        </span>
                    </div>
                </div>
            \`).join('');
        }

        function renderClientInquiries(inquiries) {
            const list = document.getElementById('clientInquiriesList');
            if (!inquiries.length) {
                list.innerHTML = '<div style="color: var(--text-muted); padding: 16px 0;">No inquiries recorded.</div>';
                return;
            }

            list.innerHTML = inquiries.map(inq => \`
                <div style="background: var(--bg-base); border: 1px solid var(--border-subtle); border-radius: var(--radius-md); padding: 14px 16px; margin-bottom: 12px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; font-size: 0.8rem; color: var(--text-muted);">
                        <span>\${formatRelativeDate(inq.createdAt)}</span>
                        \${inq.listingKey ? '<span>Listing: <code>' + escapeHtml(inq.listingKey) + '</code></span>' : ''}
                    </div>
                    <div style="font-weight: 600; font-size: 0.9rem; margin-bottom: 4px;">\${escapeHtml(inq.name || 'Client')}\${inq.phone ? ' • ' + escapeHtml(inq.phone) : ''}</div>
                    <div style="font-size: 0.85rem; color: var(--text-secondary); background: var(--bg-surface); padding: 10px; border-radius: var(--radius-sm); margin-top: 8px; line-height: 1.4;">
                        \${escapeHtml(inq.message || 'No message provided.')}
                    </div>
                </div>
            \`).join('');
        }

        // --- Other Portal Sections ---
        async function loadLeads() {
            const tbody = document.getElementById('leadsTableBody');
            tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted); padding: 24px;">Loading leads...</td></tr>';
            try {
                const res = await fetch('/api/member/leads');
                const data = await res.json();
                if (!data.leads || !data.leads.length) {
                    tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted); padding: 32px;">No leads captured yet.</td></tr>';
                    return;
                }
                tbody.innerHTML = data.leads.map(l => \`
                    <tr>
                        <td><strong>\${escapeHtml(l.name || 'Anonymous')}</strong></td>
                        <td>\${escapeHtml(l.email || '-')}</td>
                        <td>\${escapeHtml(l.phone || '-')}</td>
                        <td><code>\${escapeHtml(l.listing_key || 'General')}</code></td>
                        <td>\${formatRelativeDate(l.created_at)}</td>
                    </tr>
                \`).join('');
            } catch {
                tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--danger); padding: 24px;">Failed to load leads.</td></tr>';
            }
        }

        function renderDomains(domains) {
            const tbody = document.getElementById('domainsTableBody');
            if (!domains.length) {
                tbody.innerHTML = '<tr><td colspan="4" style="color: var(--text-muted);">No domains configured yet.</td></tr>';
                return;
            }
            tbody.innerHTML = domains.map(d => \`
                <tr>
                    <td><strong>\${escapeHtml(d.domain)}</strong></td>
                    <td>\${d.verified ? '<span class="badge badge-success">Verified</span>' : '<span class="badge badge-warning">Pending Admin Verification</span>'}</td>
                    <td>\${d.created_at || '-'}</td>
                    <td><button class="btn btn-danger btn-sm" onclick="deleteDomain('\${escapeHtml(d.id)}')">Remove</button></td>
                </tr>
            \`).join('');
        }

        function renderBilling(billing) {
            const div = document.getElementById('billingDetails');
            const status = (billing?.status || 'active').toUpperCase();
            const plan = (billing?.plan || currentAccount?.account?.plan || 'pro').toUpperCase();
            
            div.innerHTML = \`
                <p style="margin-bottom: 8px;"><strong>Billing Provider:</strong> GrowthZone</p>
                <p style="margin-bottom: 8px;"><strong>Current Plan:</strong> \${escapeHtml(plan)}</p>
                <p style="margin-bottom: 8px;"><strong>Service Status:</strong> <span class="badge \${status === 'ACTIVE' ? 'badge-success' : 'badge-warning'}">\${escapeHtml(status)}</span></p>
                <p style="margin-bottom: 16px;"><strong>Billing Schedule:</strong> Recurring on the 1st of each month.</p>
                <p style="color: var(--text-muted); font-size: 0.875rem; line-height: 1.5;">
                    Your CCOR IDX Plug-in subscription is administered directly by Coconut Coast Organization of REALTORS® through GrowthZone. To view past statements or update your payment method, click below to open GrowthZone.
                </p>
            \`;
        }

        document.getElementById('addDomainForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const domain = document.getElementById('newDomainInput').value;
            const res = await fetch('/api/member/domains', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Origin': window.location.origin },
                body: JSON.stringify({ domain })
            });
            if (res.status === 201) {
                window.location.reload();
            } else {
                const err = await res.json();
                alert(err.message || 'Failed to add domain');
            }
        });

        async function deleteDomain(id) {
            if (!confirm('Remove this domain?')) return;
            const res = await fetch('/api/member/domains/' + id, {
                method: 'DELETE',
                headers: { 'Origin': window.location.origin }
            });
            if (res.ok) window.location.reload();
        }

        document.getElementById('brandingForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const body = {
                display_name: document.getElementById('brandDisplayName').value,
                brokerage: document.getElementById('brandBrokerage').value,
                primary_color: document.getElementById('brandPrimaryColor').value,
                secondary_color: document.getElementById('brandSecondaryColor').value,
                logo_url: document.getElementById('brandLogoUrl').value,
                agent_photo_url: document.getElementById('brandPhotoUrl').value
            };
            const res = await fetch('/api/member/branding', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json', 'Origin': window.location.origin },
                body: JSON.stringify(body)
            });
            if (res.ok) alert('Branding saved successfully!');
        });

        function copySnippet(elemId) {
            const text = document.getElementById(elemId).innerText;
            navigator.clipboard.writeText(text);
            alert('HTML embed snippet copied to clipboard!');
        }

        async function logout() {
            await fetch('/api/member/auth/logout', {
                method: 'POST',
                headers: { 'Origin': window.location.origin }
            });
            window.location.reload();
        }

        init();
    </script>
</body>
</html>`;
}
