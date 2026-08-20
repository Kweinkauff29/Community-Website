/**
 * sneak-admin/ui.js
 * 
 * Embedded Administrative Portal Frontend for SNEAK IDX Platform.
 */

export function renderAdminHtml() {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SNEAK IDX — Administration Portal</title>
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
    </style>
</head>
<body>
    <header>
        <div class="brand-logo">
            <span>⚡ SNEAK IDX</span>
            <span class="badge-env">Admin Portal (Staging)</span>
        </div>
        <nav class="nav-links" id="mainNav" style="display: none;">
            <a class="nav-link active" onclick="showView('dashboard')">Dashboard</a>
            <a class="nav-link" onclick="showView('accounts')">Accounts</a>
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
            return res.json();
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
                    <p style="color: var(--text-muted); font-size: 0.875rem; margin-bottom: 1.5rem;">Enter password to access SNEAK IDX administration.</p>
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
                const data = await api('/dashboard');
                renderDashboard(data);
            } else if (viewName === 'accounts') {
                const data = await api('/accounts');
                renderAccounts(data.accounts);
            }
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
            const newStatus = currentStatus === 'active' ? 'suspended' : 'active';
            if (!confirm(\`Are you sure you want to \${newStatus.toUpperCase()} this account?\`)) return;
            await api(\`/accounts/\${id}\`, { method: 'PATCH', body: { status: newStatus } });
            showView('accounts');
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
            return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }

        function copyEmbedCode() {
            const text = document.getElementById('embedSnippet').innerText;
            navigator.clipboard.writeText(text);
            alert('Embed snippet copied to clipboard!');
        }

        async function addDomain(e, siteId, accId) {
            e.preventDefault();
            const d = document.getElementById('newDomain').value;
            await api(\`/sites/\${siteId}/domains\`, { method: 'POST', body: { domain: d } });
            viewAccount(accId);
        }

        async function deleteDomain(domId, accId) {
            if (!confirm('Remove this domain?')) return;
            await api(\`/domains/\${domId}\`, { method: 'DELETE' });
            viewAccount(accId);
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

        // Initialize
        checkAuth();
    </script>
</body>
</html>`;
}
