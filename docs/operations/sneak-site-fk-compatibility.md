# SNEAK Consumer Site-FK Compatibility

## Why this exists

An early staging schema created five consumer tables whose `site_id` foreign keys referenced `sneak_sites(site_id)`. The canonical parent key is `sneak_sites(id)`. Staging was empty, so the affected tables were safely rebuilt once when migration `0032_sneak_site_fk_compatibility.sql` was originally applied.

Canonical migrations `0025_sneak_consumer_auth.sql` and `0026_sneak_consumer_favorites.sql` now reference `sneak_sites(id)`. A fresh database does not require the historical repair.

## Wrangler migration tracking

Wrangler 4.127.1 creates `d1_migrations` with three columns:

- `id` — autoincrementing sequence
- `name` — unique migration path/name
- `applied_at` — application timestamp

There is no checksum or content-hash column. Wrangler reads the migration names from this table and treats a local file as pending only when its relative name is absent. The staging row for `0032_sneak_site_fk_compatibility.sql` therefore prevents the historical repair from running again even though the repository file is now a compatibility-marker no-op.

## Required preflight

Run the read-only classifier before advancing any environment:

```powershell
node scripts/check-sneak-site-fk-compatibility.mjs --remote
```

Use `--database` and `--config` when targeting a different D1 database/configuration. Local persistent state is supported with `--local --persist-to <directory>`.

Results:

- `PASS — NO REPAIR REQUIRED`: all five consumer foreign keys reference `sneak_sites(id)`. Migration `0032` is a no-op and can advance on either empty or populated canonical data.
- `LEGACY EMPTY — MANUAL EMPTY-SCHEMA REPAIR AVAILABLE`: the legacy target is present and all consumer tables are empty. Review and explicitly apply `scripts/sql/repair-legacy-sneak-site-fks-empty.sql`, then rerun the preflight.
- `STOP — DATA-PRESERVING MANUAL MIGRATION REQUIRED`: the legacy target is present with consumer data. Do not run the empty-schema repair. Export/back up the database, inventory every dependent table, and prepare an environment-specific table-copy/rebuild migration that preserves and reconciles every row.
- `STOP — UNRECOGNIZED CONSUMER FK SCHEMA; MANUAL ASSESSMENT REQUIRED`: the schema is incomplete or mixed and must be reviewed manually.

The preflight performs only `PRAGMA`/`SELECT` queries. It never changes schema or data.

## Migration-chain safety

- Fresh canonical database: canonical earlier migrations create correct foreign keys; `0032` executes only `SELECT 1`.
- Populated canonical database: `0032` does not inspect, drop, rebuild, or block consumer rows.
- Legacy empty database: the preflight stops automatic advancement and points to the guarded manual repair.
- Legacy populated database: the preflight fails closed; no destructive repair is automatic.

Do not copy the manual repair back into the automatic migration chain.
