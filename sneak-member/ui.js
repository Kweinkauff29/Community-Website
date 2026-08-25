/**
 * sneak-member/ui.js
 * 
 * Single-Page Application for SNEAK Member Self-Service Portal.
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
            --radius-md: 8px;
            --radius-lg: 12px;
            --radius-full: 9999px;
            --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
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
        .sidebar-nav { padding: 16px 12px; display: flex; flex-direction: column; gap: 4px; flex: 1; }
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
        }
        .nav-item:hover { background: var(--bg-surface-elevated); color: var(--text-primary); }
        .nav-item.active { background: var(--accent-glow); color: var(--accent-primary); font-weight: 600; }

        .main-content { flex: 1; display: flex; flex-direction: column; background: var(--bg-base); }
        .topbar {
            height: 64px;
            border-bottom: 1px solid var(--border-subtle);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 32px;
            background: var(--bg-surface);
        }
        .page-content { padding: 32px; flex: 1; overflow-y: auto; }

        /* Cards & Metrics */
        .grid-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 20px; margin-bottom: 32px; }
        .stat-card {
            background: var(--bg-surface);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 20px;
        }
        .stat-label { color: var(--text-muted); font-size: 0.8rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
        .stat-val { font-size: 1.75rem; font-weight: 800; color: var(--text-primary); }

        .panel {
            background: var(--bg-surface);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 24px;
            margin-bottom: 24px;
        }
        .panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }

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
        }
        .form-control:focus { border-color: var(--accent-primary); }
        .btn {
        .nav-item.active { background: var(--accent-primary); color: #fff; font-weight: 600; }
        
        .main-content { flex: 1; padding: 40px; overflow-y: auto; max-width: 1100px; }
        
        /* Typography & Components */
        h1 { font-size: 1.8rem; font-weight: 800; letter-spacing: -0.02em; margin-bottom: 8px; }
        .page-desc { color: var(--text-secondary); font-size: 0.95rem; margin-bottom: 32px; }
        .card { background: var(--bg-surface); border: 1px solid var(--border-subtle); border-radius: var(--radius-lg); padding: 24px; margin-bottom: 24px; }
        .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        .card-title { font-size: 1.15rem; font-weight: 700; }
        
        .btn { padding: 10px 18px; border-radius: var(--radius-md); font-weight: 600; font-size: 0.875rem; cursor: pointer; border: none; display: inline-flex; align-items: center; gap: 8px; font-family: var(--font-main); transition: all 0.15s ease; }
        .btn-primary { background: var(--accent-primary); color: #fff; }
        .btn-primary:hover { background: var(--accent-hover); }
        .btn-secondary { background: var(--bg-surface-elevated); color: var(--text-primary); border: 1px solid var(--border-strong); }
        .btn-secondary:hover { background: var(--bg-surface-highlight); }
        .btn-danger { background: rgba(239, 68, 68, 0.1); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.2); }
        .btn-danger:hover { background: var(--danger); color: #fff; }
        
        .form-group { margin-bottom: 16px; }
        .form-label { display: block; font-size: 0.875rem; font-weight: 600; color: var(--text-secondary); margin-bottom: 6px; }
        .form-control { width: 100%; padding: 10px 14px; background: var(--bg-base); border: 1px solid var(--border-strong); border-radius: var(--radius-md); color: var(--text-primary); font-family: var(--font-main); font-size: 0.9rem; }
        .form-control:focus { outline: none; border-color: var(--accent-primary); box-shadow: 0 0 0 3px var(--accent-glow); }
        
        .badge { display: inline-flex; align-items: center; padding: 4px 10px; border-radius: 9999px; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; }
        .badge-success { background: rgba(16, 185, 129, 0.1); color: var(--success); border: 1px solid rgba(16, 185, 129, 0.2); }
        .badge-warning { background: rgba(245, 158, 11, 0.1); color: var(--warning); border: 1px solid rgba(245, 158, 11, 0.2); }
        .badge-danger { background: rgba(239, 68, 68, 0.1); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.2); }
        
        .brand-badge { background: rgba(59, 130, 246, 0.1); color: var(--accent-primary); border: 1px solid rgba(59, 130, 246, 0.2); padding: 4px 8px; border-radius: 6px; font-weight: 700; font-size: 0.75rem; }
        
        .table { width: 100%; border-collapse: collapse; text-align: left; }
        .table th { padding: 12px 16px; border-bottom: 1px solid var(--border-subtle); color: var(--text-muted); font-size: 0.8rem; text-transform: uppercase; font-weight: 600; }
        .table td { padding: 16px; border-bottom: 1px solid var(--border-subtle); font-size: 0.9rem; }
        
        /* Auth Screen */
        .auth-container { max-width: 400px; margin: 100px auto; padding: 32px; background: var(--bg-surface); border: 1px solid var(--border-subtle); border-radius: var(--radius-lg); text-align: center; }
        .code-box { background: var(--bg-base); padding: 14px; border-radius: var(--radius-md); border: 1px solid var(--border-strong); overflow-x: auto; color: #60a5fa; font-size: 0.8rem; margin: 12px 0; }
    </style>
</head>
<body>
    <div id="authScreen" class="auth-container" style="display: none;">
        <div style="margin-bottom: 24px;">
            <span class="brand-badge">CCOR IDX Plug-in</span>
            <h2 style="margin-top: 12px;">Member Sign In</h2>
            <p style="color: var(--text-muted); font-size: 0.875rem; margin-top: 6px;">Sign in securely with a single-use magic link</p>
        </div>
        <div id="loginMsg" style="display: none; padding: 12px; border-radius: var(--radius-md); font-size: 0.875rem; margin-bottom: 16px;"></div>
        <form id="magicLoginForm">
            <div class="form-group" style="text-align: left;">
                <label class="form-label">Account Email Address</label>
                <input type="email" id="memberEmail" class="form-control" placeholder="name@brokerage.com" required autofocus>
            </div>
            <button type="submit" id="loginBtn" class="btn btn-primary" style="width: 100%; justify-content: center;">Send Magic Sign-In Link</button>
        </form>
    </div>

    <div id="appContainer" class="app-container" style="display: none;">
        <aside class="sidebar">
            <div class="sidebar-header">
                <span class="brand-badge">CCOR IDX Plug-in</span>
                <span style="font-weight: 700; font-size: 0.95rem;">Member Portal</span>
            </div>
            <nav class="sidebar-nav">
                <div class="nav-item active" onclick="switchTab('overview')">Overview</div>
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
                            <div class="stat-label">Active MLS Listings</div>
                            <div class="stat-val" id="statListings">-</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Upcoming Open Houses</div>
                            <div class="stat-val" id="statOpenHouses">-</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-label">Total Leads Captured</div>
                            <div class="stat-val" id="statLeads">-</div>
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

    <script>
        let currentAccount = null;

        async function init() {
            // Check if magic link verification token in URL
            const urlParams = new URLSearchParams(window.location.search);
            const token = urlParams.get('token');
            if (token) {
                await verifyMagicToken(token);
                return;
            }

            // Load Overview
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
            document.getElementById('authScreen').style.display = 'block';
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
                msg.innerText = data.message || 'Magic link generated! Check your email or admin portal for the link.';
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
            document.getElementById('statListings').innerText = data.inventory?.activeListings?.toLocaleString() || '0';
            document.getElementById('statOpenHouses').innerText = data.inventory?.futureOpenHouses?.toLocaleString() || '0';
            document.getElementById('statLeads').innerText = data.leadsCount || '0';
            
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
                        <div><strong style="color: var(--text-muted); font-size: 0.8rem;">SITE KEY:</strong><br><code>\${data.site.site_key}</code></div>
                        <div><strong style="color: var(--text-muted); font-size: 0.8rem;">SCOPE TYPE:</strong><br>\${data.site.scope_type.toUpperCase()}</div>
                        <div><strong style="color: var(--text-muted); font-size: 0.8rem;">MLS IDENTIFIER:</strong><br><code>\${data.site.scope_value}</code></div>
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

        function renderDomains(domains) {
            const tbody = document.getElementById('domainsTableBody');
            if (!domains.length) {
                tbody.innerHTML = '<tr><td colspan="4" style="color: var(--text-muted);">No domains configured yet.</td></tr>';
                return;
            }
            tbody.innerHTML = domains.map(d => \`
                <tr>
                    <td><strong>\${d.domain}</strong></td>
                    <td>\${d.verified ? '<span class="badge badge-success">Verified</span>' : '<span class="badge badge-warning">Pending Admin Verification</span>'}</td>
                    <td>\${d.created_at || '-'}</td>
                    <td><button class="btn btn-danger" onclick="deleteDomain('\${d.id}')">Remove</button></td>
                </tr>
            \`).join('');
        }

        function renderBilling(billing) {
            const div = document.getElementById('billingDetails');
            const status = (billing?.status || 'active').toUpperCase();
            const plan = (billing?.plan || currentAccount?.account?.plan || 'pro').toUpperCase();
            
            div.innerHTML = \`
                <p style="margin-bottom: 8px;"><strong>Billing Provider:</strong> GrowthZone</p>
                <p style="margin-bottom: 8px;"><strong>Current Plan:</strong> \${plan}</p>
                <p style="margin-bottom: 8px;"><strong>Service Status:</strong> <span class="badge \${status === 'ACTIVE' ? 'badge-success' : 'badge-warning'}">\${status}</span></p>
                <p style="margin-bottom: 16px;"><strong>Billing Schedule:</strong> Recurring on the 1st of each month.</p>
                <p style="color: var(--text-muted); font-size: 0.875rem; line-height: 1.5;">
                    Your CCOR IDX Plug-in subscription is administered directly by Coconut Coast Organization of REALTORS® through GrowthZone. To view past statements or update your payment method, click below to open GrowthZone.
                </p>
            \`;
        }

        function switchTab(tabId) {
            document.querySelectorAll('.tab-pane').forEach(p => p.style.display = 'none');
            document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
            document.getElementById('tab-' + tabId).style.display = 'block';
            event.currentTarget.classList.add('active');
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
