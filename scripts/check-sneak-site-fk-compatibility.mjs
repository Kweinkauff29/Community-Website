/**
 * Read-only D1 preflight for the historical sneak_sites(site_id) consumer FK drift.
 *
 * Examples:
 *   node scripts/check-sneak-site-fk-compatibility.mjs --remote
 *   node scripts/check-sneak-site-fk-compatibility.mjs --local --persist-to .wrangler/state
 */
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

export const CONSUMER_FK_TABLES = [
    'sneak_consumer_users',
    'sneak_consumer_magic_links',
    'sneak_consumer_sessions',
    'sneak_consumer_auth_exchanges',
    'sneak_consumer_favorites'
];

const FK_QUERIES = CONSUMER_FK_TABLES.map(table => `
    SELECT '${table}' AS table_name,
           fk.[table] AS parent_table,
           fk.[from] AS from_column,
           fk.[to] AS target_column
    FROM pragma_foreign_key_list('${table}') AS fk
    WHERE fk.[table] = 'sneak_sites';`
);

export const FK_QUERY = FK_QUERIES.join('\n');

export const ROW_COUNT_QUERY = `
    SELECT
        (SELECT COUNT(*) FROM sneak_consumer_users) +
        (SELECT COUNT(*) FROM sneak_consumer_magic_links) +
        (SELECT COUNT(*) FROM sneak_consumer_sessions) +
        (SELECT COUNT(*) FROM sneak_consumer_auth_exchanges) +
        (SELECT COUNT(*) FROM sneak_consumer_favorites) +
        (SELECT COUNT(*) FROM sneak_consumer_saved_searches) +
        (SELECT COUNT(*) FROM sneak_consumer_search_alerts) +
        (SELECT COUNT(*) FROM sneak_consumer_alert_deliveries) +
        (SELECT COUNT(*) FROM sneak_consumer_alert_matches) +
        (SELECT COUNT(*) FROM sneak_consumer_activity_events) +
        (SELECT COUNT(*) FROM sneak_consumer_compare)
        AS consumer_row_count;
`;

export function classifySneakSiteFkCompatibility(foreignKeys, consumerRowCount) {
    const rows = Array.isArray(foreignKeys) ? foreignKeys : [];
    const rowCount = Number(consumerRowCount);
    const byTable = new Map(rows.map(row => [row.table_name, row]));
    const complete = CONSUMER_FK_TABLES.every(table => byTable.has(table));
    const canonical = complete && CONSUMER_FK_TABLES.every(table => {
        const row = byTable.get(table);
        return row.parent_table === 'sneak_sites' && row.from_column === 'site_id' && row.target_column === 'id';
    });
    const legacy = complete && CONSUMER_FK_TABLES.every(table => {
        const row = byTable.get(table);
        return row.parent_table === 'sneak_sites' && row.from_column === 'site_id' && row.target_column === 'site_id';
    });

    if (canonical) {
        return {
            classification: 'CANONICAL',
            message: 'PASS — NO REPAIR REQUIRED',
            exitCode: 0,
            consumerRowCount: Number.isFinite(rowCount) ? rowCount : null
        };
    }
    if (legacy && rowCount === 0) {
        return {
            classification: 'LEGACY_EMPTY',
            message: 'LEGACY EMPTY — MANUAL EMPTY-SCHEMA REPAIR AVAILABLE',
            exitCode: 2,
            consumerRowCount: 0
        };
    }
    if (legacy && Number.isFinite(rowCount) && rowCount > 0) {
        return {
            classification: 'LEGACY_POPULATED',
            message: 'STOP — DATA-PRESERVING MANUAL MIGRATION REQUIRED',
            exitCode: 3,
            consumerRowCount: rowCount
        };
    }
    return {
        classification: 'UNRECOGNIZED',
        message: 'STOP — UNRECOGNIZED CONSUMER FK SCHEMA; MANUAL ASSESSMENT REQUIRED',
        exitCode: 4,
        consumerRowCount: Number.isFinite(rowCount) ? rowCount : null
    };
}

function parseArguments(argv) {
    const options = {
        database: 'sneak-idx-staging',
        config: 'wrangler.sneak-consumer.toml',
        mode: null,
        persistTo: null,
        json: false
    };
    for (let index = 0; index < argv.length; index++) {
        const arg = argv[index];
        if (arg === '--remote' || arg === '--local') options.mode = arg;
        else if (arg === '--database') options.database = argv[++index];
        else if (arg === '--config') options.config = argv[++index];
        else if (arg === '--persist-to') options.persistTo = argv[++index];
        else if (arg === '--json') options.json = true;
        else throw new Error(`Unknown argument: ${arg}`);
    }
    if (!options.mode) throw new Error('Specify exactly one target mode: --local or --remote.');
    if (options.persistTo && options.mode !== '--local') throw new Error('--persist-to requires --local.');
    return options;
}

function executeReadOnlyPreflight(options) {
    const bundledNpxCli = path.join(path.dirname(process.execPath), 'node_modules', 'npm', 'bin', 'npx-cli.js');
    const command = fs.existsSync(bundledNpxCli) ? process.execPath : 'npx';
    const cwd = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
    const executeQuery = sql => {
        const commandSql = sql.replace(/\s+/g, ' ').trim();
        const wranglerArgs = [
            '--yes', 'wrangler@latest', 'd1', 'execute', options.database,
            '--config', options.config,
            options.mode,
            '--json',
            `--command=${commandSql}`
        ];
        if (options.persistTo) wranglerArgs.push('--persist-to', options.persistTo);
        const args = fs.existsSync(bundledNpxCli) ? [bundledNpxCli, ...wranglerArgs] : wranglerArgs;
        const result = spawnSync(command, args, {
            cwd,
            encoding: 'utf8',
            maxBuffer: 10 * 1024 * 1024,
            windowsHide: true
        });
        if (result.status !== 0) {
            throw new Error((result.stderr || result.stdout || result.error?.message || 'Wrangler preflight failed.').trim());
        }
        try {
            return JSON.parse(result.stdout)?.[0]?.results || [];
        } catch {
            throw new Error('Wrangler returned non-JSON output; schema was not classified.');
        }
    };
    const foreignKeys = FK_QUERIES.flatMap(executeQuery);
    const consumerRowCount = executeQuery(ROW_COUNT_QUERY)?.[0]?.consumer_row_count;
    return classifySneakSiteFkCompatibility(foreignKeys, consumerRowCount);
}

export function main(argv = process.argv.slice(2)) {
    const options = parseArguments(argv);
    const result = executeReadOnlyPreflight(options);
    if (options.json) console.log(JSON.stringify(result));
    else {
        console.log(result.message);
        console.log(`classification=${result.classification}`);
        console.log(`consumer_rows=${result.consumerRowCount ?? 'unknown'}`);
    }
    return result.exitCode;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        process.exitCode = main();
    } catch (error) {
        console.error(`STOP — ${error instanceof Error ? error.message : String(error)}`);
        process.exitCode = 1;
    }
}
