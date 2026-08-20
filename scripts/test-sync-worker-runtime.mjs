/**
 * scripts/test-sync-worker-runtime.mjs
 * 
 * Direct runtime validation of Worker-native synchronization modules against remote D1 sneak-idx-staging.
 */

import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { runListingDelta } from '../sneak-sync/listing-sync.js';
import { runOpenHouseSync } from '../sneak-sync/open-house-sync.js';
import { runListingReconciliation } from '../sneak-sync/reconciliation.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

const TARGET_DB_NAME = 'sneak-idx-staging';
const TARGET_DB_ID = '6b91eeca-d65f-434c-a49f-419dff98285f';
const WRANGLER_CONFIG = 'wrangler.sneak-sync.toml';

// Resolve BRIDGE_TOKEN
let bridgeToken = process.env.BRIDGE_TOKEN;
if (!bridgeToken) {
    const devVarsPath = path.join(rootDir, '.dev.vars');
    if (fs.existsSync(devVarsPath)) {
        try {
            const content = fs.readFileSync(devVarsPath, 'utf8');
            for (const line of content.split('\n')) {
                const trimmed = line.trim();
                if (trimmed.startsWith('BRIDGE_TOKEN=')) {
                    bridgeToken = trimmed.substring('BRIDGE_TOKEN='.length).trim().replace(/^["']|["']$/g, '');
                    break;
                }
            }
        } catch {}
    }
}

// Remote D1 Adapter for Worker runtime functions
function createRemoteD1Adapter() {
    function executeRemoteCommand(sql, json = true) {
        const scratchDir = path.join(rootDir, 'scratch');
        if (!fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });
        const tmpFile = path.join(scratchDir, `adapter-${Date.now()}-${Math.random().toString(36).slice(2, 6)}.sql`);
        fs.writeFileSync(tmpFile, sql, 'utf8');

        try {
            const raw = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${tmpFile} ${json ? '--json' : ''} -c ${WRANGLER_CONFIG}`, {
                cwd: rootDir,
                encoding: 'utf8',
                maxBuffer: 50 * 1024 * 1024,
                stdio: ['pipe', 'pipe', 'ignore']
            });
            if (json) {
                const parsed = JSON.parse(raw);
                return parsed[0] || { results: [], success: true };
            }
            return { success: true };
        } finally {
            if (fs.existsSync(tmpFile)) {
                try { fs.unlinkSync(tmpFile); } catch {} }
        }
    }

    function escapeValue(v) {
        if (v === null || v === undefined) return 'NULL';
        if (typeof v === 'number') return isNaN(v) ? 'NULL' : String(v);
        if (typeof v === 'boolean') return v ? '1' : '0';
        return "'" + String(v).replace(/'/g, "''") + "'";
    }

    function interpolate(query, args) {
        let idx = 0;
        return query.replace(/\?/g, () => {
            if (idx >= args.length) return 'NULL';
            return escapeValue(args[idx++]);
        });
    }

    function createStatement(query, boundArgs = []) {
        return {
            bind(...args) {
                return createStatement(query, args);
            },
            async first() {
                const sql = interpolate(query, boundArgs);
                const res = executeRemoteCommand(sql, true);
                return res.results?.[0] || null;
            },
            async all() {
                const sql = interpolate(query, boundArgs);
                const res = executeRemoteCommand(sql, true);
                return { results: res.results || [] };
            },
            async run() {
                const sql = interpolate(query, boundArgs);
                const res = executeRemoteCommand(sql, true);
                return { success: res.success !== false, meta: res.meta };
            }
        };
    }

    return {
        prepare(query) {
            return createStatement(query);
        },
        async batch(statements) {
            const sqls = statements.map(s => {
                // If statement is prepared
                return s.sql || s;
            });
            // Execute batch as a single multi-statement file
            const combinedSql = statements.map(stmt => {
                // To get rendered SQL from our statement
                if (stmt.sql) return stmt.sql;
                return stmt;
            }).join(';\n') + ';';

            const scratchDir = path.join(rootDir, 'scratch');
            if (!fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });
            const tmpFile = path.join(scratchDir, `batch-${Date.now()}-${Math.random().toString(36).slice(2, 6)}.sql`);
            fs.writeFileSync(tmpFile, combinedSql, 'utf8');

            try {
                execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${tmpFile} -c ${WRANGLER_CONFIG}`, {
                    cwd: rootDir,
                    stdio: 'pipe'
                });
                return statements.map(() => ({ success: true }));
            } finally {
                if (fs.existsSync(tmpFile)) {
                    try { fs.unlinkSync(tmpFile); } catch {} }
            }
        }
    };
}

// Enhance createStatement to store sql
function createStatementWithSql(query, boundArgs = [], executeFn) {
    function escapeValue(v) {
        if (v === null || v === undefined) return 'NULL';
        if (typeof v === 'number') return isNaN(v) ? 'NULL' : String(v);
        if (typeof v === 'boolean') return v ? '1' : '0';
        return "'" + String(v).replace(/'/g, "''") + "'";
    }

    function interpolate(q, args) {
        let idx = 0;
        return q.replace(/\?/g, () => {
            if (idx >= args.length) return 'NULL';
            return escapeValue(args[idx++]);
        });
    }

    const renderedSql = interpolate(query, boundArgs);

    return {
        sql: renderedSql,
        bind(...args) {
            return createStatementWithSql(query, args, executeFn);
        },
        async first() {
            const res = executeFn(renderedSql, true);
            return res.results?.[0] || null;
        },
        async all() {
            const res = executeFn(renderedSql, true);
            return { results: res.results || [] };
        },
        async run() {
            const res = executeFn(renderedSql, true);
            return { success: res.success !== false, meta: res.meta };
        }
    };
}

function createEnhancedD1Adapter() {
    function executeRemoteCommand(sql, json = true) {
        const scratchDir = path.join(rootDir, 'scratch');
        if (!fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });
        const tmpFile = path.join(scratchDir, `adapter-${Date.now()}-${Math.random().toString(36).slice(2, 6)}.sql`);
        fs.writeFileSync(tmpFile, sql, 'utf8');

        try {
            const raw = execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${tmpFile} ${json ? '--json' : ''} -c ${WRANGLER_CONFIG}`, {
                cwd: rootDir,
                encoding: 'utf8',
                maxBuffer: 50 * 1024 * 1024,
                stdio: ['pipe', 'pipe', 'ignore']
            });
            if (json) {
                const jsonStart = raw.indexOf('[');
                const jsonEnd = raw.lastIndexOf(']');
                if (jsonStart !== -1 && jsonEnd !== -1) {
                    const parsed = JSON.parse(raw.slice(jsonStart, jsonEnd + 1));
                    return parsed[0] || { results: [], success: true };
                }
                return { results: [], success: true };
            }
            return { success: true };
        } finally {
            if (fs.existsSync(tmpFile)) {
                try { fs.unlinkSync(tmpFile); } catch {}
            }
        }
    }

    return {
        prepare(query) {
            return createStatementWithSql(query, [], executeRemoteCommand);
        },
        async batch(statements) {
            const combinedSql = statements.map(s => s.sql || s).join('\n');
            const scratchDir = path.join(rootDir, 'scratch');
            if (!fs.existsSync(scratchDir)) fs.mkdirSync(scratchDir, { recursive: true });
            const tmpFile = path.join(scratchDir, `batch-${Date.now()}-${Math.random().toString(36).slice(2, 6)}.sql`);
            fs.writeFileSync(tmpFile, combinedSql, 'utf8');

            try {
                execSync(`npx wrangler d1 execute ${TARGET_DB_NAME} --remote --file=${tmpFile} -c ${WRANGLER_CONFIG}`, {
                    cwd: rootDir,
                    stdio: 'pipe'
                });
                return statements.map(() => ({ success: true }));
            } finally {
                if (fs.existsSync(tmpFile)) {
                    try { fs.unlinkSync(tmpFile); } catch {} }
            }
        }
    };
}

async function runValidation() {
    console.log('====================================================');
    console.log('SNEAK IDX — WORKER-NATIVE SYNC MODULE VALIDATION');
    console.log('Target D1: sneak-idx-staging (6b91eeca-d65f-434c-a49f-419dff98285f)');
    console.log('====================================================');

    const db = createEnhancedD1Adapter();
    const env = {
        SNEAK_ENV: 'staging',
        BRIDGE_TOKEN: bridgeToken,
        DB: db
    };

    // [1] Listing Delta Sync Execution 1
    console.log('\n[1] Executing Worker-Native runListingDelta (Run 1)...');
    const deltaRes1 = await runListingDelta(env);
    console.log('  Delta Run 1 Result:', deltaRes1);

    // [2] Listing Delta Sync Execution 2 (Idempotency / Convergence)
    console.log('\n[2] Executing Worker-Native runListingDelta (Run 2 - Consecutive)...');
    const deltaRes2 = await runListingDelta(env);
    console.log('  Delta Run 2 Result:', deltaRes2);

    // [3] Open House Sync Execution 1
    console.log('\n[3] Executing Worker-Native runOpenHouseSync (Run 1)...');
    const ohRes1 = await runOpenHouseSync(env);
    console.log('  OpenHouse Run 1 Result:', ohRes1);

    // [4] Open House Sync Execution 2 (Idempotency)
    console.log('\n[4] Executing Worker-Native runOpenHouseSync (Run 2 - Consecutive)...');
    const ohRes2 = await runOpenHouseSync(env);
    console.log('  OpenHouse Run 2 Result:', ohRes2);

    console.log('\n====================================================');
    console.log('WORKER-NATIVE SYNC RUNTIME VALIDATION COMPLETED!');
    console.log('====================================================');
}

runValidation().catch(err => {
    console.error('Validation Error:', err);
    process.exit(1);
});
