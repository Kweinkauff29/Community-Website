/**
 * sneak-admin/ui.js
 * 
 * Embedded Administrative Portal Frontend for SNEAK IDX Platform.
 */

export function renderAdminHtml(env = {}) {
    const isProd = (env?.SNEAK_ENV || '').toLowerCase() === 'production';
    const envBadge = isProd ? 'Admin Portal (Production)' : 'Admin Portal (Staging)';
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CCOR IDX Plug-in — Administration Portal</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-base: #0f172a;
            --bg-surface: #1e293b;
            --bg-card: #1e293b;
            --bg-input: #0f172a;
            --border: #334155;
            --border-focus: #3b82f6;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --primary: #2563eb;
            --primary-hover: #1d4ed8;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --font: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-mono: 'JetBrains Mono', monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: var(--font);
            background-color: var(--bg-base);
            color: var(--text-main);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
        }

        header {
            background-color: var(--bg-surface);
            border-bottom: 1px solid var(--border);
            padding: 0.875rem 1.5rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .brand-logo {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            font-weight: 700;
            font-size: 1.125rem;
            color: #fff;
        }

        .badge-env {
            font-size: 0.6875rem;
            font-weight: 600;
            text-transform: uppercase;
            padding: 0.2rem 0.5rem;
            background: #1e3a8a;
            color: #93c5fd;
            border-radius: 4px;
        }

        .nav-links {
            display: flex;
            gap: 1.5rem;
            align-items: center;
        }

        .nav-link {
            color: var(--text-muted);
            text-decoration: none;
            font-size: 0.875rem;
            font-weight: 500;
            cursor: pointer;
            transition: color 0.15s;
        }
        .nav-link.active, .nav-link:hover { color: #fff; }

        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            font-size: 0.875rem;
            font-weight: 500;
            border-radius: 6px;
            cursor: pointer;
            border: 1px solid transparent;
            transition: all 0.15s;
        }
        .btn-primary { background: var(--primary); color: #fff; }
        .btn-primary:hover { background: var(--primary-hover); }
        .btn-secondary { background: #334155; color: #fff; }
        .btn-secondary:hover { background: #475569; }
        .btn-danger { background: rgba(239, 68, 68, 0.2); color: #fca5a5; border-color: rgba(239, 68, 68, 0.3); }
        .btn-danger:hover { background: var(--danger); color: #fff; }
        .btn-sm { padding: 0.25rem 0.625rem; font-size: 0.75rem; }

        main {
            flex: 1;
            padding: 1.5rem;
            max-width: 1300px;
            width: 100%;
            margin: 0 auto;
        }

        .grid-stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }

        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1.25rem;
        }

        .card-stat h4 {
            font-size: 0.75rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }
        .card-stat .val {
            font-size: 1.75rem;
            font-weight: 700;
            color: #fff;
        }
        .card-stat .sub {
            font-size: 0.75rem;
            color: var(--text-muted);
            margin-top: 0.25rem;
        }

        .table-responsive {
            width: 100%;
            overflow-x: auto;
            margin-top: 1rem;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 0.875rem;
        }

        th {
            background: rgba(15, 23, 42, 0.6);
            color: var(--text-muted);
            font-weight: 600;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border);
        }

        td {
            padding: 0.875rem 1rem;
            border-bottom: 1px solid var(--border);
            color: #e2e8f0;
        }

        tr:hover td {
            background: rgba(51, 65, 85, 0.3);
        }

        .badge {
            display: inline-block;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        .badge-active { background: rgba(16, 185, 129, 0.2); color: #6ee7b7; border: 1px solid rgba(16, 185, 129, 0.3); }
        .badge-suspended { background: rgba(239, 68, 68, 0.2); color: #fca5a5; border: 1px solid rgba(239, 68, 68, 0.3); }
        .badge-market { background: rgba(59, 130, 246, 0.2); color: #93c5fd; }
        .badge-agent { background: rgba(168, 85, 247, 0.2); color: #d8b4fe; }
        .badge-office { background: rgba(245, 158, 11, 0.2); color: #fde68a; }

        .modal-backdrop {
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0, 0, 0, 0.75);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 999;
            backdrop-filter: blur(4px);
        }
        .modal {
            background: var(--bg-surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            width: 90%;
            max-width: 650px;
            max-height: 90vh;
            overflow-y: auto;
            padding: 1.5rem;
        }

        .form-group {
            margin-bottom: 1rem;
        }
        .form-group label {
            display: block;
            font-size: 0.8125rem;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 0.375rem;
        }
        .form-control {
            width: 100%;
            background: var(--bg-input);
            border: 1px solid var(--border);
            color: #fff;
            padding: 0.5rem 0.75rem;
            font-size: 0.875rem;
            border-radius: 6px;
            outline: none;
            font-family: inherit;
        }
        .form-control:focus { border-color: var(--border-focus); }

        .code-block {
            background: #090d16;
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 0.875rem;
            font-family: var(--font-mono);
            font-size: 0.8125rem;
            color: #38bdf8;
            overflow-x: auto;
            white-space: pre-wrap;
            position: relative;
            margin: 0.75rem 0;
        }

        .color-preview {
            width: 32px; height: 32px; border-radius: 4px; border: 1px solid var(--border);
        }

        .auth-container {
            max-width: 400px;
            margin: 6rem auto;
            background: var(--bg-surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 2rem;
        }

        .toolbar { display:flex; gap:.75rem; align-items:center; justify-content:space-between; flex-wrap:wrap; margin-bottom:1rem; }
        .filters { display:flex; gap:.6rem; flex:1; flex-wrap:wrap; }
        .filters .form-control { min-width:150px; width:auto; flex:1; }
        .detail-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:1rem; }
        .section-title { display:flex; align-items:center; justify-content:space-between; gap:.75rem; margin-bottom:.8rem; }
        .muted { color:var(--text-muted); font-size:.82rem; line-height:1.5; }
        .progress-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr)); gap:.6rem; }
        .progress-step { border:1px solid var(--border); border-radius:7px; padding:.7rem; }
        .progress-step strong { display:block; font-size:.72rem; text-transform:uppercase; color:var(--text-muted); margin-bottom:.25rem; }
        .status-ready { color:#6ee7b7; }
        .status-blocked { color:#fca5a5; }
        .blocker-list { display:grid; gap:.5rem; margin-top:.75rem; }
        .blocker { border-left:3px solid var(--danger); background:rgba(239,68,68,.08); padding:.65rem .75rem; font-size:.82rem; }
        .field-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:.8rem; }
        .wizard-step { border-top:1px solid var(--border); padding-top:1rem; margin-top:1rem; }
        .wizard-step h4 { margin-bottom:.7rem; }
        .toast { position:fixed; right:1rem; bottom:1rem; z-index:1200; background:#0f172a; border:1px solid var(--border); padding:.8rem 1rem; border-radius:7px; max-width:360px; }
        .audit-list { display:grid; gap:.5rem; max-height:360px; overflow:auto; }
        .audit-item { border-bottom:1px solid var(--border); padding:.55rem 0; font-size:.8rem; }
        .actions { display:flex; gap:.5rem; flex-wrap:wrap; }
        @media (max-width: 760px) {
            header { align-items:flex-start; gap:.8rem; flex-direction:column; padding:.75rem; }
            .nav-links { gap:.7rem; flex-wrap:wrap; width:100%; }
            main { padding:.75rem; overflow-x:hidden; }
            .detail-grid, .field-grid { grid-template-columns:1fr; }
            .card { padding:.9rem; }
            .table-responsive { margin-left:0; max-width:100%; }
            th, td { padding:.65rem .55rem; }
            .modal { width:96%; max-height:94vh; padding:1rem; }
            .brand-logo { flex-wrap:wrap; }
            .filters .form-control { min-width:100%; width:100%; }
        }
    </style>
</head>
<body>
    <header>
        <div class="brand-logo">
            <span>⚡ CCOR IDX Plug-in</span>
            <span class="badge-env">${envBadge}</span>
        </div>
        <nav class="nav-links" id="mainNav" style="display: none;">
            <a class="nav-link active" onclick="showView('dashboard')">Dashboard</a>
            <a class="nav-link" onclick="showView('accounts')">Accounts</a>
            <a class="nav-link" onclick="showView('readiness')">Launch Readiness</a>
            <a class="nav-link" onclick="openOnboardModal()">+ Provision Member</a>
            <button class="btn btn-secondary btn-sm" onclick="logout()">Sign Out</button>
        </nav>
    </header>

    <main id="app">
        <!-- Views rendered by JS -->
    </main>

    <script>
        const API_BASE = '/api/admin';
        let currentAuth = false;
        let currentView = 'dashboard';
        let currentAccountId = null;
        let renderRevision = 0;

        function enterView(viewName, accountId = null) {
            currentView = viewName;
            currentAccountId = accountId;
            renderRevision += 1;
            return renderRevision;
        }

        function canRender(revision, viewName, accountId = null) {
            return revision === renderRevision && currentView === viewName && currentAccountId === accountId;
        }

        async function api(path, options = {}) {
            options.headers = options.headers || {};
            options.headers['X-Sneak-Admin'] = '1';
            if (options.body && typeof options.body === 'object') {
                options.headers['Content-Type'] = 'application/json';
                options.body = JSON.stringify(options.body);
            }
            const res = await fetch(API_BASE + path, options);
            if (res.status === 401) {
                currentAuth = false;
                renderLogin();
                throw new Error('Authentication required');
            }
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data.message || data.error || ('Request failed: ' + res.status));
            return data;
        }

        async function checkAuth() {
            try {
                const data = await api('/dashboard');
                currentAuth = true;
                document.getElementById('mainNav').style.display = 'flex';
                renderDashboard(data);
            } catch (err) {
                renderLogin();
            }
        }

        function renderLogin() {
            document.getElementById('mainNav').style.display = 'none';
            document.getElementById('app').innerHTML = \`
                <div class="auth-container">
                    <h2 style="margin-bottom: 0.5rem; font-size: 1.25rem;">Admin Authentication</h2>
                    <p style="color: var(--text-muted); font-size: 0.875rem; margin-bottom: 1.5rem;">Enter password to access CCOR IDX Plug-in administration.</p>
                    <form onsubmit="handleLogin(event)">
                        <div class="form-group">
                            <label>Password</label>
                            <input type="password" id="adminPassword" class="form-control" placeholder="••••••••••••" required autofocus>
                        </div>
                        <button type="submit" class="btn btn-primary" style="width: 100%;">Authenticate</button>
                        <div id="loginError" style="color: var(--danger); font-size: 0.8125rem; margin-top: 0.75rem; display: none;"></div>
                    </form>
                </div>
            \`;
        }

        async function handleLogin(e) {
            e.preventDefault();
            const pw = document.getElementById('adminPassword').value;
            const errDiv = document.getElementById('loginError');
            errDiv.style.display = 'none';

            try {
                const res = await fetch('/api/admin/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'X-Sneak-Admin': '1' },
                    body: JSON.stringify({ password: pw })
                });
                const data = await res.json();
                if (!res.ok) throw new Error(data.message || 'Login failed');
                checkAuth();
            } catch (err) {
                errDiv.innerText = err.message;
                errDiv.style.display = 'block';
            }
        }

        async function logout() {
            await fetch('/api/admin/logout', { method: 'POST', headers: { 'X-Sneak-Admin': '1' } });
            currentAuth = false;
            renderLogin();
        }

        async function showView(viewName) {
            document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
            if (viewName === 'dashboard') {
                const revision = enterView('dashboard');
                const data = await api('/dashboard');
                if (canRender(revision, 'dashboard')) renderDashboard(data);
            } else if (viewName === 'accounts') {
                await loadAccounts();
            } else if (viewName === 'readiness') {
                const revision = enterView('readiness');
                const data = await api('/readiness');
                if (canRender(revision, 'readiness')) renderGlobalReadiness(data);
            }
        }

        function notify(message, isError = false) {
            document.getElementById('adminToast')?.remove();
            const node = document.createElement('div');
            node.id = 'adminToast';
            node.className = 'toast';
            node.style.borderColor = isError ? 'var(--danger)' : 'var(--success)';
            node.textContent = message;
            document.body.appendChild(node);
            setTimeout(() => node.remove(), 3500);
        }

        function renderDashboard(data) {
            document.getElementById('app').innerHTML = \`
                <div class="grid-stats">
                    <div class="card card-stat">
                        <h4>Total Accounts</h4>
                        <div class="val">\${data.accounts.total}</div>
                        <div class="sub">\${data.accounts.active} active • \${data.accounts.suspended} suspended</div>
                    </div>
                    <div class="card card-stat">
                        <h4>Tenant Sites</h4>
                        <div class="val">\${data.sites.total}</div>
                        <div class="sub">\${data.sites.agentSites} Agent • \${data.sites.officeSites} Office • \${data.sites.marketSites} Market</div>
                    </div>
                    <div class="card card-stat">
                        <h4>Active MLS Inventory</h4>
                        <div class="val">\${Number(data.inventory.activeListings).toLocaleString()}</div>
                        <div class="sub">\${Number(data.inventory.futureOpenHouses).toLocaleString()} Future Open Houses</div>
                    </div>
                    <div class="card card-stat">
                        <h4>Authorized Domains</h4>
                        <div class="val">\${data.authorizedDomains}</div>
                        <div class="sub">Verified tenant origins</div>
                    </div>
                </div>

                <div class="card" style="margin-bottom: 1.5rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                        <h3 style="font-size: 1rem;">Recent Sync Status</h3>
                        <span class="badge badge-active">Automated</span>
                    </div>
                    <div style="font-size: 0.875rem; color: var(--text-muted);">
                        Listing Delta Sync: Every 15 min • Open House Sync: Hourly • Remote D1: Connected
                    </div>
                </div>

                <div class="card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                        <h3 style="font-size: 1rem;">Recent Audit History</h3>
                    </div>
                    <div class="table-responsive">
                        <table>
                            <thead>
                                <tr>
                                    <th>Action</th>
                                    <th>Entity</th>
                                    <th>Summary</th>
                                    <th>Timestamp</th>
                                </tr>
                            </thead>
                            <tbody>
                                \${(data.recentAudit || []).map(a => \`
                                    <tr>
                                        <td><strong>\${a.action}</strong></td>
                                        <td>\${a.entity_type} (\${a.entity_id})</td>
                                        <td>\${a.summary}</td>
                                        <td style="color: var(--text-muted); font-size: 0.75rem;">\${a.created_at}</td>
                                    </tr>
                                \`).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            \`;
        }

        function renderAccounts(accounts) {
            document.getElementById('app').innerHTML = \`
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                    <h2 style="font-size: 1.25rem;">Member Accounts</h2>
                    <button class="btn btn-primary btn-sm" onclick="openOnboardModal()">+ Provision New Member</button>
                </div>
                <div class="card">
                    <div class="table-responsive">
                        <table>
                            <thead>
                                <tr>
                                    <th>Account Name</th>
                                    <th>Member ID</th>
                                    <th>Plan</th>
                                    <th>Status</th>
                                    <th>Sites</th>
                                    <th>Created</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                \${accounts.map(a => \`
                                    <tr>
                                        <td><strong>\${a.account_name}</strong></td>
                                        <td>\${a.member_id || '—'}</td>
                                        <td><span class="badge" style="background: #334155;">\${a.plan}</span></td>
                                        <td><span class="badge badge-\${a.status}">\${a.status}</span></td>
                                        <td>\${a.site_count}</td>
                                        <td style="color: var(--text-muted); font-size: 0.75rem;">\${a.created_at?.slice(0, 10)}</td>
                                        <td>
                                            <button class="btn btn-secondary btn-sm" onclick="viewAccount('\${a.id}')">Manage</button>
                                            <button class="btn \${a.status === 'active' ? 'btn-danger' : 'btn-primary'} btn-sm" onclick="toggleAccountStatus('\${a.id}', '\${a.status}')">
                                                \${a.status === 'active' ? 'Suspend' : 'Activate'}
                                            </button>
                                        </td>
                                    </tr>
                                \`).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            \`;
        }

        async function toggleAccountStatus(id, currentStatus) {
            requestLifecycle(id, currentStatus === 'active' ? 'suspend' : 'reactivate', 'account');
        }

        async function viewAccount(id) {
            const data = await api(\`/accounts/\${id}\`);
            const acc = data.account;
            const site = data.sites[0];

            document.getElementById('app').innerHTML = \`
                <div style="margin-bottom: 1rem;">
                    <a class="nav-link" onclick="showView('accounts')" style="margin-bottom: 0.5rem; display: inline-block;">← Back to Accounts</a>
                    <h2 style="font-size: 1.25rem;">\${acc.account_name}</h2>
                    <p style="color: var(--text-muted); font-size: 0.875rem;">Status: <span class="badge badge-\${acc.status}">\${acc.status}</span> • Plan: \${acc.plan}</p>
                </div>

                \${site ? \`
                    <div class="card" style="margin-bottom: 1.5rem;">
                        <h3 style="font-size: 1rem; margin-bottom: 0.75rem;">Site: \${site.site_name} (<code>\${site.site_key}</code>)</h3>
                        <p style="font-size: 0.875rem; margin-bottom: 1rem;">
                            Scope: <span class="badge badge-\${site.scope_type}">\${site.scope_type}</span> \${site.scope_value ? 'MLS ID: <strong>' + site.scope_value + '</strong>' : ''}
                        </p>

                        <h4 style="font-size: 0.875rem; margin: 1rem 0 0.5rem 0;">Authorized Domains</h4>
                        <div style="display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 1rem;">
                            \${(site.domains || []).map(d => \`
                                <span class="badge" style="background: #1e293b; border: 1px solid var(--border); font-size: 0.8125rem;">
                                    \${d.domain} (\${d.status})
                                    <span style="color: var(--danger); cursor: pointer; margin-left: 0.5rem;" onclick="deleteDomain('\${d.id}', '\${acc.id}')">×</span>
                                </span>
                            \`).join('')}
                        </div>
                        <form onsubmit="addDomain(event, '\${site.id}', '\${acc.id}')" style="display: flex; gap: 0.5rem; max-width: 400px;">
                            <input type="text" id="newDomain" class="form-control" placeholder="example.com" required>
                            <button type="submit" class="btn btn-secondary btn-sm">Add Domain</button>
                        </form>

                        <h4 style="font-size: 0.875rem; margin: 1.5rem 0 0.5rem 0;">Embed Code Snippet (Full Search)</h4>
                        <div class="code-block" id="embedSnippet">\${escapeHtml(site.embed?.snippets?.search?.htmlSnippet || '')}</div>
                        <button class="btn btn-primary btn-sm" onclick="copyEmbedCode()">📋 Copy Embed Code</button>
                        <a href="\${site.embed?.snippets?.search?.previewUrl}" target="_blank" class="btn btn-secondary btn-sm" style="margin-left: 0.5rem;">↗ Preview IDX</a>
                    </div>
                \` : '<p>No site configured.</p>'}
            \`;
        }

        function escapeHtml(str) {
            return String(str ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
        }

        function copyEmbedCode() {
            const text = document.getElementById('embedSnippet').innerText;
            navigator.clipboard.writeText(text).then(() => notify('Embed code copied.')).catch(() => notify('Select and copy the code manually.', true));
        }

        async function addDomain(e, siteId, accId) {
            e.preventDefault();
            const d = document.getElementById('newDomain').value;
            await api(\`/sites/\${siteId}/domains\`, { method: 'POST', body: { domain: d } });
            viewAccount(accId);
        }

        async function deleteDomain(domId, accId) {
            requestDomainDelete(domId, accId, 'this domain');
        }

        function openOnboardModal() {
            const modal = document.createElement('div');
            modal.className = 'modal-backdrop';
            modal.id = 'onboardModal';
            modal.innerHTML = \`
                <div class="modal">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                        <h3 style="font-size: 1.125rem;">Provision New Member</h3>
                        <span style="cursor: pointer; font-size: 1.25rem;" onclick="closeModal()">×</span>
                    </div>
                    <form onsubmit="handleOnboard(event)">
                        <div class="form-group">
                            <label>Account / Member Name</label>
                            <input type="text" id="obAccountName" class="form-control" placeholder="e.g. John Smith Realty" required>
                        </div>
                        <div class="form-group">
                            <label>Member ID (NAR / MLS User ID)</label>
                            <input type="text" id="obMemberId" class="form-control" placeholder="e.g. M12345">
                        </div>
                        <div class="form-group">
                            <label>IDX Scope</label>
                            <select id="obScopeType" class="form-control" onchange="toggleScopeInput()">
                                <option value="market">Market Wide (All MLS Listings)</option>
                                <option value="agent">Agent Listings Only</option>
                                <option value="office">Office Listings Only</option>
                            </select>
                        </div>
                        <div class="form-group" id="scopeValueGroup" style="display: none;">
                            <label>MLS Identifier (Agent or Office MLS ID)</label>
                            <div style="display: flex; gap: 0.5rem;">
                                <input type="text" id="obScopeValue" class="form-control" placeholder="e.g. B3650316 or BPRI">
                                <button type="button" class="btn btn-secondary btn-sm" onclick="validateMlsId()">Validate</button>
                            </div>
                            <div id="mlsFeedback" style="font-size: 0.75rem; margin-top: 0.25rem;"></div>
                        </div>
                        <div class="form-group">
                            <label>Authorized Website Domain</label>
                            <input type="text" id="obDomain" class="form-control" placeholder="e.g. johnsmithrealtor.com or localhost">
                        </div>
                        <div class="form-group">
                            <label>Plan</label>
                            <select id="obPlan" class="form-control">
                                <option value="standard">Standard</option>
                                <option value="trial">Trial</option>
                                <option value="pro">Pro</option>
                                <option value="brokerage">Brokerage</option>
                            </select>
                        </div>
                        <div style="display: flex; justify-content: flex-end; gap: 0.5rem; margin-top: 1.5rem;">
                            <button type="button" class="btn btn-secondary" onclick="closeModal()">Cancel</button>
                            <button type="submit" class="btn btn-primary">Provision Member & Generate Embed</button>
                        </div>
                    </form>
                </div>
            \`;
            document.body.appendChild(modal);
        }

        function closeModal() {
            const m = document.getElementById('onboardModal');
            if (m) m.remove();
        }

        function toggleScopeInput() {
            const type = document.getElementById('obScopeType').value;
            const grp = document.getElementById('scopeValueGroup');
            grp.style.display = type === 'market' ? 'none' : 'block';
        }

        async function validateMlsId() {
            const type = document.getElementById('obScopeType').value;
            const id = document.getElementById('obScopeValue').value.trim();
            const fb = document.getElementById('mlsFeedback');
            if (!id) {
                fb.innerHTML = '<span style="color: var(--danger)">Please enter an MLS ID.</span>';
                return;
            }
            fb.innerHTML = '<span style="color: var(--text-muted)">Checking current inventory...</span>';
            const data = await api(\`/validate-mls?type=\${type}&mlsId=\${encodeURIComponent(id)}\`);
            if (data.valid) {
                fb.innerHTML = \`<span style="color: var(--success)">✓ Valid: Found \${data.count} active listings for this \${type}.</span>\`;
            } else {
                fb.innerHTML = \`<span style="color: var(--warning)">⚠️ Warning: 0 active listings found in feed.</span>\`;
            }
        }

        async function handleOnboard(e) {
            e.preventDefault();
            const payload = {
                account_name: document.getElementById('obAccountName').value,
                member_id: document.getElementById('obMemberId').value,
                scope_type: document.getElementById('obScopeType').value,
                scope_value: document.getElementById('obScopeValue')?.value?.trim() || '',
                domain: document.getElementById('obDomain').value,
                plan: document.getElementById('obPlan').value,
                override_mls_warning: true
            };

            const res = await api('/accounts', { method: 'POST', body: payload });
            closeModal();
            viewAccount(res.account.id);
        }

        // Phase 7.4A staff control plane overrides (canonical API-backed views)
        async function loadAccounts() {
            const revision = enterView('accounts');
            const q = document.getElementById('accountSearch')?.value || '';
            const status = document.getElementById('accountStatusFilter')?.value || '';
            const entitlement = document.getElementById('entitlementFilter')?.value || '';
            const params = new URLSearchParams({ q, status, entitlement });
            const data = await api('/accounts?' + params.toString());
            if (canRender(revision, 'accounts')) renderAccounts(data.accounts, { q, status, entitlement });
        }

        function renderAccounts(accounts, filters = {}) {
            document.getElementById('app').innerHTML = \`
                <div class="toolbar">
                    <div><h2 style="font-size:1.25rem;">Member Accounts</h2><p class="muted">Search identity, email, MLS ID, domain, or entitlement reference.</p></div>
                    <button class="btn btn-primary" onclick="openOnboardModal()">+ Guided Provisioning</button>
                </div>
                <div class="card" style="margin-bottom:1rem;"><form class="filters" onsubmit="event.preventDefault();loadAccounts()">
                    <input id="accountSearch" class="form-control" value="\${escapeHtml(filters.q || '')}" placeholder="Name, email, MLS ID, domain, reference">
                    <select id="accountStatusFilter" class="form-control"><option value="">All account states</option>\${['active','suspended','inactive'].map(v => '<option '+(filters.status===v?'selected':'')+' value="'+v+'">'+v+'</option>').join('')}</select>
                    <select id="entitlementFilter" class="form-control"><option value="">All entitlements</option>\${['active','grace','delinquent','suspended','canceled','missing'].map(v => '<option '+(filters.entitlement===v?'selected':'')+' value="'+v+'">'+v+'</option>').join('')}</select>
                    <button class="btn btn-secondary">Search</button>
                </form></div>
                <div class="card"><div class="table-responsive"><table>
                    <thead><tr><th>Account / Member</th><th>MLS Identity</th><th>Plan / Entitlement</th><th>Domain</th><th>Serving</th><th>Updated</th><th></th></tr></thead>
                    <tbody>\${accounts.length ? accounts.map(a => \`<tr>
                        <td><strong>\${escapeHtml(a.account_name)}</strong><div class="muted">\${escapeHtml(a.member_email || a.member_id || 'No member user')}</div></td>
                        <td>Agent: \${escapeHtml(a.agent_mls_id || '—')}<br>Office: \${escapeHtml(a.office_mls_id || '—')}</td>
                        <td>\${escapeHtml(a.plan)}<br><span class="badge \${a.entitlement_status === 'active' ? 'badge-active' : 'badge-suspended'}">\${escapeHtml(a.entitlement_label)}</span></td>
                        <td>\${escapeHtml(a.primary_domain || 'Not configured')}<div class="muted">\${a.domain_verified === 1 && a.domain_status === 'active' ? 'Verified' : 'Needs attention'}</div></td>
                        <td><span class="\${a.canServe ? 'status-ready' : 'status-blocked'}">\${a.canServe ? 'SERVING' : 'BLOCKED'}</span><div class="muted">\${Number(a.site_count || 0)} site(s)</div></td>
                        <td class="muted">\${escapeHtml((a.updated_at || a.created_at || '').slice(0,16))}</td>
                        <td><button class="btn btn-secondary btn-sm" onclick="viewAccount('\${escapeHtml(a.id)}')">Open</button></td>
                    </tr>\`).join('') : '<tr><td colspan="7" class="muted">No accounts match these filters.</td></tr>'}</tbody>
                </table></div></div>\`;
        }

        function renderGlobalReadiness(data) {
            const capabilities = Object.entries(data.capabilities || {});
            document.getElementById('app').innerHTML = \`<div class="toolbar"><div><h2>Launch Readiness Control Plane</h2><p class="muted">Core IDX and optional capabilities are reported separately.</p></div><div class="actions"><button class="btn btn-secondary btn-sm" onclick="reconcileGrowthZoneBulk()">Reconcile GrowthZone</button><span class="badge \${data.pilotReady ? 'badge-active' : 'badge-suspended'}">\${escapeHtml(data.readinessCategory || 'Unknown')}</span></div></div>
                <div class="grid-stats">\${capabilities.map(([name,value]) => '<div class="card card-stat"><h4>'+escapeHtml(name.replace(/([A-Z])/g,' $1'))+'</h4><div class="val" style="font-size:1.15rem">'+escapeHtml(value.status)+'</div><div class="sub">'+(value.core?'Core capability':'Optional capability')+'</div></div>').join('')}</div>
                <div class="card"><div class="section-title"><h3>Environment blockers</h3><span class="badge">\${(data.blockers || []).length}</span></div><div class="blocker-list">\${(data.blockers || []).length ? data.blockers.map(b => '<div class="blocker"><strong>'+escapeHtml(b.code)+'</strong><br>'+escapeHtml(b.message)+'</div>').join('') : '<p class="status-ready">No environment blockers recorded.</p>'}</div></div>\`;
        }

        let pendingImpactAction = null;

        function setupLabel(value) {
            return String(value || 'missing').replaceAll('_', ' ').replace(/\b\w/g, c => c.toUpperCase());
        }

        async function viewAccount(id, refreshOnly = false) {
            if (refreshOnly && (currentView !== 'account' || currentAccountId !== id)) return;
            const revision = enterView('account', id);
            const data = await api('/accounts/' + encodeURIComponent(id));
            if (!canRender(revision, 'account', id)) return;
            const acc = data.account;
            const site = data.sites?.[0] || null;
            const ent = data.entitlement || {};
            const rec = data.reconciliation || {};
            const readiness = data.readiness || { setupProgress:{}, launchBlockers:[], checklist:[] };
            const progress = Object.entries(readiness.setupProgress || {});
            document.getElementById('app').innerHTML = \`
                <div class="toolbar"><div><a class="nav-link" onclick="showView('accounts')">← Accounts</a><h2 style="margin-top:.45rem;">\${escapeHtml(acc.account_name)}</h2><p class="muted">Account \${escapeHtml(acc.id)} · Updated \${escapeHtml(acc.updated_at || acc.created_at || '')}</p></div>
                    <div class="actions"><button class="btn btn-danger" onclick="requestLifecycle('\${escapeHtml(acc.id)}','suspend','\${escapeHtml(acc.account_name)}')">Suspend</button><button class="btn btn-danger" onclick="requestLifecycle('\${escapeHtml(acc.id)}','cancel','\${escapeHtml(acc.account_name)}')">Cancel</button><button class="btn btn-primary" onclick="requestLifecycle('\${escapeHtml(acc.id)}','reactivate','\${escapeHtml(acc.account_name)}')">Reactivate</button></div></div>
                <div class="card" style="margin-bottom:1rem;border-color:\${readiness.launchReady ? 'var(--success)' : 'var(--danger)'}"><div class="section-title"><div><h3 class="\${readiness.launchReady ? 'status-ready':'status-blocked'}">\${readiness.launchReady ? 'READY TO LAUNCH':'NOT READY'}</h3><p class="muted">Effective serving: \${readiness.canServe ? 'allowed':'denied'} · generic entitlement authority</p></div><button class="btn btn-secondary btn-sm" onclick="viewAccount('\${escapeHtml(acc.id)}')">Refresh checks</button></div>
                    <div class="progress-grid">\${progress.map(([key,value]) => '<div class="progress-step"><strong>'+escapeHtml(key.replace(/([A-Z])/g,' $1'))+'</strong><span class="'+(value==='complete'||value==='ready'?'status-ready':'status-blocked')+'">'+escapeHtml(setupLabel(value))+'</span></div>').join('')}</div>
                    <div class="blocker-list">\${(readiness.launchBlockers || []).map(b => '<div class="blocker"><strong>'+escapeHtml(b.code)+'</strong> — '+escapeHtml(b.message)+'</div>').join('')}</div>
                </div>
                <div class="detail-grid">
                    <section class="card"><div class="section-title"><h3>Account</h3><span class="badge badge-\${escapeHtml(acc.status)}">\${escapeHtml(acc.status)}</span></div><p><strong>Member ID:</strong> \${escapeHtml(acc.member_id || '—')}</p><p><strong>Agent MLS:</strong> \${escapeHtml(acc.agent_mls_id || '—')}</p><p><strong>Office MLS:</strong> \${escapeHtml(acc.office_mls_id || '—')}</p><p><strong>Plan:</strong> \${escapeHtml(acc.plan)}</p></section>
                    <section class="card"><div class="section-title"><h3>Entitlement</h3><span class="badge \${ent.status==='active'?'badge-active':'badge-suspended'}">\${escapeHtml(data.entitlementLabel)}</span></div>
                        <form onsubmit="saveEntitlement(event,'\${escapeHtml(acc.id)}')"><div class="field-grid">
                            <div class="form-group"><label>Source</label><select id="entSource" class="form-control"><option value="manual" \${ent.source==='manual'?'selected':''}>Manual</option><option value="growthzone" \${ent.source==='growthzone'?'selected':''}>GrowthZone</option></select></div>
                            <div class="form-group"><label>Status</label><select id="entStatus" class="form-control">\${['active','grace','delinquent','suspended','canceled'].map(v=>'<option '+(ent.status===v?'selected':'')+' value="'+v+'">'+setupLabel(v)+'</option>').join('')}</select></div>
                            <div class="form-group"><label>Plan</label><input id="entPlan" class="form-control" value="\${escapeHtml(ent.plan || acc.plan || '')}"></div>
                            <div class="form-group"><label>External Reference</label><input id="entReference" class="form-control" value="\${escapeHtml(ent.external_reference || '')}"></div>
                            <div class="form-group"><label>Effective Date</label><input id="entEffective" type="datetime-local" class="form-control" value="\${escapeHtml((ent.effective_at || '').slice(0,16))}"></div>
                            <div class="form-group"><label>Expires</label><input id="entExpires" type="datetime-local" class="form-control" value="\${escapeHtml((ent.expires_at || '').slice(0,16))}"></div>
                            <div class="form-group"><label>Grace Until</label><input id="entGrace" type="datetime-local" class="form-control" value="\${escapeHtml((ent.grace_until || '').slice(0,16))}"></div>
                            <div class="form-group"><label>Last Verified</label><input class="form-control" disabled value="\${escapeHtml(ent.last_verified_at || 'Not verified')}"></div>
                        </div><div class="form-group"><label>Notes</label><textarea id="entNotes" class="form-control" rows="2">\${escapeHtml(ent.notes || '')}</textarea></div><button class="btn btn-primary">Save Entitlement</button></form>
                    </section>
                    <section class="card"><div class="section-title"><h3>GrowthZone Reconciliation</h3><span class="badge \${['verified_no_change','entitlement_changed'].includes(rec.status)?'badge-active':'badge-suspended'}">\${escapeHtml(setupLabel(rec.status || (ent.source === 'growthzone' ? 'never' : 'manual_override')))}</span></div>
                        <p><strong>Entitlement Source:</strong> \${escapeHtml(ent.source || 'Not configured')}</p>
                        <p><strong>GrowthZone Reference:</strong> \${escapeHtml(ent.external_reference || 'Not mapped')}</p>
                        <p><strong>Canonical Status:</strong> \${escapeHtml(ent.status || 'Missing')}</p>
                        <p><strong>Last Verified:</strong> \${escapeHtml(ent.last_verified_at || 'Never')}</p>
                        <p><strong>Reconciliation Status:</strong> \${escapeHtml(setupLabel(rec.status || 'never'))}</p>
                        <p><strong>Last Attempt:</strong> \${escapeHtml(rec.last_attempt_at || 'Never')}</p>
                        <p><strong>Difference / Action:</strong> \${escapeHtml(rec.difference || rec.error_code || 'No difference')}</p>
                        <p class="muted">Automatic reconciliation requires person:&lt;contactId&gt;:membership:&lt;membershipId&gt; or org:&lt;contactId&gt;:membership:&lt;membershipId&gt;. Manual source is protected.</p>
                        <button class="btn btn-secondary" onclick="reconcileAccount('\${escapeHtml(acc.id)}')" \${ent.source === 'growthzone' ? '' : 'disabled'}>Reconcile Now</button>
                    </section>
                    <section class="card"><div class="section-title"><h3>Member Users</h3><span class="badge">\${data.members.length}</span></div>\${data.members.map(m=>'<p><strong>'+escapeHtml(m.email)+'</strong> · '+escapeHtml(setupLabel(m.status))+' · '+escapeHtml(m.role)+'</p>').join('') || '<p class="muted">No member user associated.</p>'}<form class="filters" onsubmit="inviteMember(event,'\${escapeHtml(acc.id)}')" style="margin-top:.8rem"><input id="inviteEmail" type="email" class="form-control" placeholder="member@example.com" required><button class="btn btn-secondary">Create / Resend Invite</button></form></section>
                    <section class="card"><div class="section-title"><h3>Client / Lead Summary</h3></div><div class="grid-stats" style="margin:0"><div class="card-stat"><h4>Authenticated Clients</h4><div class="val">\${Number(data.clientLeadSummary?.clients||0)}</div></div><div class="card-stat"><h4>Leads</h4><div class="val">\${Number(data.clientLeadSummary?.leads||0)}</div></div></div></section>
                    \${site ? \`<section class="card"><div class="section-title"><h3>IDX Site</h3><span class="badge badge-\${escapeHtml(site.status)}">\${escapeHtml(site.status)}</span></div><p><strong>\${escapeHtml(site.site_name)}</strong> · <code>\${escapeHtml(site.site_key)}</code></p><p class="muted">IDX Search Scope: <strong>\${escapeHtml(site.scope_type)}</strong> \${escapeHtml(site.scope_value || '(full market inventory)')} · Participant Agent MLS: <code>\${escapeHtml(acc.agent_mls_id || '—')}</code></p><div class="actions" style="margin-top:.7rem"><button class="btn btn-danger btn-sm" onclick="requestSiteDisable('\${escapeHtml(site.id)}','\${escapeHtml(acc.id)}')">Disable Site</button><button class="btn btn-primary btn-sm" onclick="enableSite('\${escapeHtml(site.id)}','\${escapeHtml(acc.id)}')">Enable Site</button></div></section>
                    <section class="card"><div class="section-title"><h3>Domains</h3><span class="badge">\${site.domains.length}</span></div><div>\${site.domains.map(d=>'<p><strong>'+escapeHtml(d.domain)+'</strong> · '+(d.verified===1?'Verified':'Ownership not verified')+' · '+escapeHtml(d.status)+' <button class="btn btn-secondary btn-sm" data-domain-id="'+escapeHtml(d.id)+'" data-account-id="'+escapeHtml(acc.id)+'" onclick="authorizeDomain(this.dataset.domainId,this.dataset.accountId)">Authorize</button> <button class="btn btn-danger btn-sm" data-domain-id="'+escapeHtml(d.id)+'" data-account-id="'+escapeHtml(acc.id)+'" data-domain="'+escapeHtml(d.domain)+'" onclick="requestDomainDelete(this.dataset.domainId,this.dataset.accountId,this.dataset.domain)">Remove</button></p>').join('') || '<p class="muted">No domain configured.</p>'}</div><form class="filters" onsubmit="addAdminDomain(event,'\${escapeHtml(site.id)}','\${escapeHtml(acc.id)}')" style="margin-top:.8rem"><input id="detailNewDomain" class="form-control" placeholder="www.member-site.com" required><button class="btn btn-secondary">Add Pending Domain</button></form></section>
                    <section class="card"><div class="section-title"><h3>Branding</h3></div><form onsubmit="saveBranding(event,'\${escapeHtml(site.id)}','\${escapeHtml(acc.id)}')"><div class="field-grid"><div class="form-group"><label>Display Name</label><input id="brandName" class="form-control" value="\${escapeHtml(site.branding?.display_name||'')}"></div><div class="form-group"><label>Brokerage</label><input id="brandBrokerage" class="form-control" value="\${escapeHtml(site.branding?.brokerage||'')}"></div><div class="form-group"><label>Phone</label><input id="brandPhone" class="form-control" value="\${escapeHtml(site.branding?.phone||'')}"></div><div class="form-group"><label>Email</label><input id="brandEmail" type="email" class="form-control" value="\${escapeHtml(site.branding?.email||'')}"></div><div class="form-group"><label>Logo URL</label><input id="brandLogo" class="form-control" value="\${escapeHtml(site.branding?.logo_url||'')}"></div><div class="form-group"><label>Primary Color</label><input id="brandPrimary" class="form-control" value="\${escapeHtml(site.branding?.primary_color||'#1a365d')}"></div></div><button class="btn btn-primary">Save Branding</button></form></section>
                    <section class="card"><div class="section-title"><h3>Responsive Embed</h3><span class="badge badge-active">\${escapeHtml(site.embed?.embedBuild || '')}</span></div><p class="muted">\${(site.embed?.installationNotes||[]).map(escapeHtml).join(' ')}</p><div class="code-block" id="embedSnippet">\${escapeHtml(site.embed?.snippets?.search?.htmlSnippet || '')}</div><button class="btn btn-primary btn-sm" onclick="copyEmbedCode()">Copy Embed Code</button></section>\` : '<section class="card"><h3>IDX Sites</h3><p class="status-blocked">No site configured.</p></section>'}
                    <section class="card"><div class="section-title"><h3>Readiness Checklist</h3><span class="badge">Automated</span></div>\${(readiness.checklist||[]).map(c=>'<p class="'+(c.status==='pass'?'status-ready':'status-blocked')+'">'+(c.status==='pass'?'✓':'×')+' '+escapeHtml(c.label)+'</p>').join('')}</section>
                    <section class="card"><div class="section-title"><h3>Audit History</h3><span class="badge">Latest 50</span></div><div class="audit-list">\${(data.audit||[]).map(a=>'<div class="audit-item"><strong>'+escapeHtml(a.action)+'</strong> · '+escapeHtml(a.created_at||'')+'<br><span class="muted">'+escapeHtml(a.summary||'')+'</span></div>').join('') || '<p class="muted">No matching audit records.</p>'}</div></section>
                </div>\`;
        }

        function openImpactModal(title, message, confirmLabel, action) {
            pendingImpactAction = action;
            const modal = document.createElement('div'); modal.className='modal-backdrop'; modal.id='impactModal';
            modal.innerHTML='<div class="modal" style="max-width:520px"><h3>'+escapeHtml(title)+'</h3><p class="muted" style="margin:1rem 0">'+escapeHtml(message)+'</p><div class="actions" style="justify-content:flex-end"><button class="btn btn-secondary" onclick="closeImpactModal()">Go Back</button><button id="impactConfirm" class="btn btn-danger">'+escapeHtml(confirmLabel)+'</button></div></div>';
            document.body.appendChild(modal);
            document.getElementById('impactConfirm').onclick = async () => { const fn=pendingImpactAction; closeImpactModal(); try { await fn(); } catch(err) { notify(err.message,true); } };
        }
        function closeImpactModal(){ document.getElementById('impactModal')?.remove(); pendingImpactAction=null; }
        function requestLifecycle(id, action, name) {
            const messages={suspend:'Public IDX and member operational access will stop. All configuration and buyer data will be preserved.',cancel:'The entitlement will be canceled and service will stop. No data will be deleted.',reactivate:'The account and suspended/canceled entitlement will return to active. Existing data and settings remain.'};
            openImpactModal(setupLabel(action)+' account', messages[action], setupLabel(action)+' '+name, async()=>{await api('/accounts/'+encodeURIComponent(id)+'/lifecycle',{method:'POST',body:{action}});notify('Lifecycle updated.');await viewAccount(id,true);});
        }
        function requestSiteDisable(siteId,accountId){openImpactModal('Disable IDX site','Serving stops for this site. The site key, domain, configuration, and buyer data remain.','Disable Site',async()=>{await api('/sites/'+encodeURIComponent(siteId),{method:'PATCH',body:{status:'inactive'}});await viewAccount(accountId,true);});}
        async function enableSite(siteId,accountId){await api('/sites/'+encodeURIComponent(siteId),{method:'PATCH',body:{status:'active'}});notify('Site enabled.');await viewAccount(accountId,true);}
        function requestDomainDelete(domainId,accountId,domain){openImpactModal('Remove domain authorization','The domain '+domain+' will immediately stop authorizing IDX bootstrap. Other account data is preserved.','Remove Domain',async()=>{await api('/domains/'+encodeURIComponent(domainId),{method:'DELETE'});await viewAccount(accountId,true);});}
        async function authorizeDomain(domainId,accountId){await api('/domains/'+encodeURIComponent(domainId),{method:'PATCH',body:{verified:true,status:'active'}});notify('Domain marked active and verified.');await viewAccount(accountId,true);}
        async function saveEntitlement(e,accountId){e.preventDefault();await api('/accounts/'+encodeURIComponent(accountId)+'/entitlement',{method:'PUT',body:{source:entSource.value,status:entStatus.value,plan:entPlan.value,external_reference:entReference.value,effective_at:entEffective.value||null,expires_at:entExpires.value||null,grace_until:entGrace.value||null,notes:entNotes.value}});notify('Entitlement saved.');await viewAccount(accountId,true);}
        async function reconcileAccount(accountId){try{await api('/accounts/'+encodeURIComponent(accountId)+'/reconcile',{method:'POST',body:{}});notify('GrowthZone reconciliation completed.');}catch(err){notify(err.message,true);}await viewAccount(accountId,true);}
        async function reconcileGrowthZoneBulk(){try{const result=await api('/growthzone/reconcile',{method:'POST',body:{limit:25}});notify('GrowthZone reconciliation: '+Number(result.succeeded||0)+' succeeded, '+Number(result.failed||0)+' need attention.');}catch(err){notify(err.message,true);}await showView('readiness');}
        async function inviteMember(e,accountId){e.preventDefault();const result=await api('/accounts/'+encodeURIComponent(accountId)+'/members',{method:'POST',body:{email:inviteEmail.value,role:'owner'}});notify(result.invitationRequested?'Invitation requested.':'Member associated; live email delivery was not confirmed.',!result.invitationRequested);await viewAccount(accountId,true);}
        async function addAdminDomain(e,siteId,accountId){e.preventDefault();await api('/sites/'+encodeURIComponent(siteId)+'/domains',{method:'POST',body:{domain:detailNewDomain.value,verified:false,status:'disabled'}});notify('Pending domain added.');await viewAccount(accountId,true);}
        async function saveBranding(e,siteId,accountId){e.preventDefault();await api('/sites/'+encodeURIComponent(siteId)+'/branding',{method:'PUT',body:{display_name:brandName.value,brokerage:brandBrokerage.value,phone:brandPhone.value,email:brandEmail.value,logo_url:brandLogo.value,primary_color:brandPrimary.value}});notify('Branding saved.');await viewAccount(accountId,true);}
        function copyEmbedCode(){const text=document.getElementById('embedSnippet')?.innerText||'';navigator.clipboard.writeText(text).then(()=>notify('Embed code copied.')).catch(()=>notify('Select and copy the code manually.',true));}

        function openOnboardModal() {
            const modal=document.createElement('div');modal.className='modal-backdrop';modal.id='onboardModal';modal.innerHTML=\`<div class="modal">
                <div class="section-title"><div><h3>Guided Staff Provisioning</h3><p class="muted">Each completed API step persists to the canonical account records.</p></div><button class="btn btn-secondary btn-sm" onclick="closeModal()">Close</button></div>
                <form onsubmit="handleOnboard(event)">
                    <div class="wizard-step"><h4>1 — Member / MLS Identity</h4><div class="field-grid"><div class="form-group"><label>Account Display Name</label><input id="obAccountName" class="form-control" required></div><div class="form-group"><label>Member Email</label><input id="obMemberEmail" type="email" class="form-control" required></div><div class="form-group"><label>Member ID</label><input id="obMemberId" class="form-control"></div><div class="form-group"><label>Agent MLS ID</label><input id="obAgentMls" class="form-control"></div><div class="form-group"><label>Office MLS ID</label><input id="obOfficeMls" class="form-control"></div></div></div>
                    <div class="wizard-step"><h4>2 — Account & Entitlement</h4><div class="field-grid"><div class="form-group"><label>Plan</label><select id="obPlan" class="form-control"><option>standard</option><option>trial</option><option>pro</option><option>brokerage</option></select></div><div class="form-group"><label>Source</label><select id="obEntSource" class="form-control"><option value="manual">Manual</option><option value="growthzone">GrowthZone</option></select></div><div class="form-group"><label>External Reference</label><input id="obReference" class="form-control" placeholder="GrowthZone/member reference"></div></div></div>
                    <div class="wizard-step"><h4>3 — IDX Site / Scope</h4><div class="field-grid"><div class="form-group"><label>Site Name</label><input id="obSiteName" class="form-control"></div><div class="form-group"><label>Site Key (optional)</label><input id="obSiteKey" class="form-control"></div><div class="form-group"><label>IDX Search Scope (Consumer Inventory)</label><select id="obScopeType" class="form-control" onchange="toggleScopeInput()"><option value="market">Market (Full IDX Search — Standard)</option><option value="agent">Agent (Restricted to Agent's Own Listings Only)</option><option value="office">Office (Restricted to Office Listings Only)</option></select><p class="muted" style="margin-top:0.35rem;font-size:0.8rem;">Use <strong>Market</strong> for standard member IDX search with full market inventory. Participant Agent MLS ID (configured in Step 1) powers "My Listings" and lead capture.</p></div><div class="form-group" id="scopeValueGroup" style="display:none"><label>Scope MLS Identifier</label><div class="actions"><input id="obScopeValue" class="form-control"><button type="button" class="btn btn-secondary btn-sm" onclick="validateMlsId()">Validate</button></div><div id="mlsFeedback" class="muted"></div></div></div></div>
                    <div class="wizard-step"><h4>4 — Member Portal User</h4><p class="muted">An owner user is associated to this account and a magic-link request is dispatched. Delivery readiness is reported separately.</p></div>
                    <div class="wizard-step"><h4>5 — Domain</h4><div class="form-group"><label>Member Website Hostname</label><input id="obDomain" class="form-control" placeholder="www.member-site.com"><p class="muted">New hostnames remain disabled and unverified until staff completes domain operations.</p></div></div>
                    <div class="wizard-step"><h4>6 — Branding</h4><div class="field-grid"><div class="form-group"><label>Brokerage</label><input id="obBrokerage" class="form-control" required></div><div class="form-group"><label>Phone</label><input id="obPhone" class="form-control"></div><div class="form-group"><label>Contact Email</label><input id="obBrandEmail" type="email" class="form-control" required></div><div class="form-group"><label>Primary Color</label><input id="obPrimaryColor" class="form-control" value="#1a365d"></div></div></div>
                    <div class="wizard-step"><h4>7 — Widget</h4><p class="muted">The responsive search widget is enabled using safe defaults. Step 8 readiness runs on Account Detail.</p></div>
                    <div class="actions" style="justify-content:flex-end;margin-top:1.2rem"><button type="button" class="btn btn-secondary" onclick="closeModal()">Save for Later</button><button class="btn btn-primary">Create & Continue</button></div>
                </form></div>\`;document.body.appendChild(modal);
        }

        async function handleOnboard(e) {
            e.preventDefault();
            let accountId=null;
            try {
                const scopeType=obScopeType.value; const scopeValue=obScopeValue.value.trim();
                const created=await api('/accounts',{method:'POST',body:{account_name:obAccountName.value,member_id:obMemberId.value,agent_mls_id:obAgentMls.value,office_mls_id:obOfficeMls.value,plan:obPlan.value,site_name:obSiteName.value||obAccountName.value,site_key:obSiteKey.value,scope_type:scopeType,scope_value:scopeValue,domain:obDomain.value,override_mls_warning:true,branding:{display_name:obAccountName.value,brokerage:obBrokerage.value,phone:obPhone.value,email:obBrandEmail.value,primary_color:obPrimaryColor.value}}});
                accountId=created.account.id;
                await api('/accounts/'+encodeURIComponent(accountId)+'/entitlement',{method:'PUT',body:{source:obEntSource.value,status:'active',plan:obPlan.value,external_reference:obReference.value,notes:'Created through Phase 7.4A guided provisioning'}});
                await api('/accounts/'+encodeURIComponent(accountId)+'/members',{method:'POST',body:{email:obMemberEmail.value,role:'owner'}});
                await api('/sites/'+encodeURIComponent(created.site.id)+'/branding',{method:'PUT',body:{display_name:obAccountName.value,brokerage:obBrokerage.value,phone:obPhone.value,email:obBrandEmail.value,primary_color:obPrimaryColor.value}});
                await api('/sites/'+encodeURIComponent(created.site.id)+'/widgets/search',{method:'PUT',body:{enabled:true,config_json:{responsive:true}}});
                closeModal();notify('Canonical provisioning steps saved. Complete domain verification and activation in Account Detail.');await viewAccount(accountId);
            } catch(err) {
                closeModal();notify(accountId?'Provisioning paused safely. Existing steps are saved; resume from Account Detail.':'Provisioning failed before account creation: '+err.message,true);
                if(accountId)await viewAccount(accountId);
            }
        }

        // Initialize
        checkAuth();
    </script>
</body>
</html>`;
}
