import test, { describe } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { execFileSync } from 'node:child_process';

import consumerWorker from '../sneak-consumer/worker.js';
import alertsWorker, { processAlerts } from '../sneak-alerts/worker.js';
import { generateEmbedSnippets } from '../sneak-admin/embed-generator.js';

const rootDir = path.resolve(import.meta.dirname, '..');
const migrationPath = path.join(rootDir, 'migrations', '0035_sneak_growthzone_reconciliation_fk.sql');
const migrationSql = fs.readFileSync(migrationPath, 'utf8');
const productionConfigs = [
    'wrangler.sneak.production.toml',
    'wrangler.sneak-sync.production.toml',
    'wrangler.sneak-member.production.toml',
    'wrangler.sneak-consumer.production.toml',
    'wrangler.sneak-alerts.production.toml',
    'wrangler.sneak-admin.production.toml',
    'wrangler.sneak-sites.production.toml'
];

function createPre35Database() {
    const db = new DatabaseSync(':memory:');
    db.exec(`
        PRAGMA foreign_keys = ON;
        CREATE TABLE sneak_accounts (id TEXT PRIMARY KEY);
        CREATE TABLE sneak_launch_checks (
            check_key TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            source TEXT,
            checked_at DATETIME,
            detail_json TEXT
        );
    `);
    db.exec(fs.readFileSync(path.join(rootDir, 'migrations', '0034_sneak_growthzone_reconciliation.sql'), 'utf8'));
    return db;
}

describe('Phase 7.4B2 production isolation and cutover safety', () => {
    test('migration 0035 declares the canonical account cascade and refuses silent orphan filtering', () => {
        assert.match(migrationSql, /REFERENCES\s+sneak_accounts\s*\(id\)\s*ON DELETE CASCADE/i);
        assert.match(migrationSql, /CHECK\s*\(orphan_count\s*=\s*0\)/i);
        assert.doesNotMatch(migrationSql, /INSERT[\s\S]*sneak_growthzone_reconciliation_next[\s\S]*SELECT[\s\S]*INNER JOIN/i);
        assert.match(migrationSql, /CREATE INDEX idx_sneak_growthzone_reconciliation_status/i);
    });

    test('populated migration preserves valid reconciliation state and account deletion cascades', () => {
        const db = createPre35Database();
        try {
            db.prepare('INSERT INTO sneak_accounts (id) VALUES (?)').run('acc_valid');
            db.prepare(`INSERT INTO sneak_growthzone_reconciliation (
                account_id, status, external_reference, remote_contact_id, last_attempt_at, last_success_at, difference
            ) VALUES (?, ?, ?, ?, ?, ?, ?)`)
                .run('acc_valid', 'verified_no_change', 'person:100:membership:200', '100',
                    '2026-09-01T12:00:00.000Z', '2026-09-01T12:00:00.000Z', 'No change');

            db.exec(migrationSql);

            const preserved = db.prepare('SELECT * FROM sneak_growthzone_reconciliation WHERE account_id = ?').get('acc_valid');
            assert.equal(preserved.status, 'verified_no_change');
            assert.equal(preserved.external_reference, 'person:100:membership:200');
            assert.equal(preserved.last_success_at, '2026-09-01T12:00:00.000Z');
            assert.deepEqual(db.prepare("PRAGMA foreign_key_list('sneak_growthzone_reconciliation')").all().map(row => ({
                table: row.table, from: row.from, to: row.to, onDelete: row.on_delete
            })), [{ table: 'sneak_accounts', from: 'account_id', to: 'id', onDelete: 'CASCADE' }]);
            assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(), []);

            db.prepare('DELETE FROM sneak_accounts WHERE id = ?').run('acc_valid');
            assert.equal(db.prepare('SELECT COUNT(*) AS count FROM sneak_growthzone_reconciliation').get().count, 0);
        } finally {
            db.close();
        }
    });

    test('fresh canonical migration chain applies through 0035 with zero FK violations', () => {
        const db = new DatabaseSync(':memory:');
        try {
            db.exec('PRAGMA foreign_keys = ON;');
            const files = fs.readdirSync(path.join(rootDir, 'migrations')).filter(name => /^\d{4}_.+\.sql$/.test(name)).sort();
            for (const file of files) db.exec(fs.readFileSync(path.join(rootDir, 'migrations', file), 'utf8'));
            assert.equal(files.at(-1), '0035_sneak_growthzone_reconciliation_fk.sql');
            assert.deepEqual(db.prepare('PRAGMA foreign_key_check').all(), []);
        } finally {
            db.close();
        }
    });

    test('production configs are D1-isolated, secret-free, and keep unverified optional schedules disabled', () => {
        for (const file of productionConfigs) {
            const source = fs.readFileSync(path.join(rootDir, file), 'utf8');
            assert.match(source, /database_name\s*=\s*"sneak-idx-production"/);
            assert.match(source, /database_id\s*=\s*"9d3c529d-a769-4ab3-9385-d9c8bc5e937d"/);
            assert.doesNotMatch(source, /sneak-idx-staging|6b91eeca-d65f-434c-a49f-419dff98285f/);
            assert.doesNotMatch(source, /^\s*(?:BRIDGE_TOKEN|MAILJET_API_KEY|MAILJET_SECRET_KEY|GROWTHZONE_API_KEY|SNEAK_SIGNING_SECRET|SNEAK_ADMIN_PASSWORD_HASH)\s*=/m);
        }
        const consumer = fs.readFileSync(path.join(rootDir, 'wrangler.sneak-consumer.production.toml'), 'utf8');
        const alerts = fs.readFileSync(path.join(rootDir, 'wrangler.sneak-alerts.production.toml'), 'utf8');
        const admin = fs.readFileSync(path.join(rootDir, 'wrangler.sneak-admin.production.toml'), 'utf8');
        assert.match(consumer, /CONSUMER_AUTH_ENABLED\s*=\s*"false"/);
        assert.match(alerts, /EMAIL_ALERTS_ENABLED\s*=\s*"false"/);
        assert.match(admin, /GROWTHZONE_RECONCILIATION_ENABLED\s*=\s*"false"/);
        for (const source of [alerts, admin]) {
            const active = source.split(/\r?\n/).filter(line => !line.trim().startsWith('#')).join('\n');
            assert.doesNotMatch(active, /\[triggers\]|crons\s*=/);
        }
    });

    test('disabled production account and alert capabilities fail closed at runtime', async () => {
        const consumerHealth = await consumerWorker.fetch(new Request('https://consumer.example/api/consumer/version'), {
            CONSUMER_AUTH_ENABLED: 'false', EMAIL_ALERTS_ENABLED: 'false'
        });
        assert.deepEqual(await consumerHealth.json().then(data => ({ auth: data.authEnabled, alerts: data.emailAlertsEnabled })), {
            auth: false, alerts: false
        });
        const login = await consumerWorker.fetch(new Request('https://consumer.example/api/consumer/auth/magic-link', { method: 'POST' }), {
            CONSUMER_AUTH_ENABLED: 'false'
        });
        assert.equal(login.status, 503);

        const alertHealth = await alertsWorker.fetch(new Request('https://alerts.example/health'), { EMAIL_ALERTS_ENABLED: 'false' });
        assert.equal((await alertHealth.json()).deliveryReady, false);
        const scheduled = await processAlerts({ db: null, env: { EMAIL_ALERTS_ENABLED: 'false' } });
        assert.deepEqual(scheduled, { skipped: true, reason: 'CapabilityDisabled', deliveryReady: false });
    });

    test('production embed generation uses the production serving host and current build', () => {
        const embed = generateEmbedSnippets('pilot-site', ['member.example'], {}, {
            SNEAK_SERVING_URL: 'https://sneak-idx-worker.bonitaspringsrealtors.workers.dev'
        });
        assert.equal(embed.servingHost, 'https://sneak-idx-worker.bonitaspringsrealtors.workers.dev');
        assert.equal(embed.embedBuild, '2026.09.01.7.4b2');
        assert.match(embed.snippets.search.htmlSnippet, /sneak-idx-worker\.bonitaspringsrealtors\.workers\.dev\/embed\.js\?v=2026\.09\.01\.7\.4b2/);
    });

    test('all four protected legacy files remain zero-diff from origin/main', () => {
        execFileSync('git', ['diff', '--exit-code', 'origin/main', '--',
            'ListingsWorker.js', 'home-search/index.html', 'open-house/index.html', 'wrangler.toml'
        ], { cwd: rootDir, stdio: 'pipe' });
    });
});
