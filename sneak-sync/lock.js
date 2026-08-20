/**
 * sneak-sync/lock.js
 * 
 * D1-backed distributed locking and operational sync run history.
 */

export async function acquireLock(db, jobName, ttlSeconds = 300) {
    const lockId = `lock_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;

    // 1. Purge expired locks for this job
    await db.prepare("DELETE FROM sneak_sync_locks WHERE job_name = ? AND expires_at < datetime('now')").bind(jobName).run();

    // 2. Attempt to acquire lock atomically
    try {
        const res = await db.prepare(`
            INSERT INTO sneak_sync_locks (job_name, lock_id, acquired_at, expires_at)
            VALUES (?, ?, datetime('now'), datetime('now', '+' || ? || ' seconds'))
        `).bind(jobName, lockId, ttlSeconds).run();

        if (res.success) {
            return lockId;
        }
        return null;
    } catch {
        // Primary key conflict means job is already locked
        return null;
    }
}

export async function releaseLock(db, jobName, lockId) {
    if (!lockId) return;
    try {
        await db.prepare("DELETE FROM sneak_sync_locks WHERE job_name = ? AND lock_id = ?").bind(jobName, lockId).run();
    } catch (err) {
        console.error(`[LOCK RELEASE ERROR] Failed to release lock ${jobName}:`, err.message);
    }
}

export async function recordSyncRun(db, runData) {
    try {
        const id = `run_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
        await db.prepare(`
            INSERT INTO sneak_sync_runs (
                id, job_name, started_at, finished_at, status,
                records_fetched, records_upserted, records_removed,
                bridge_pages, d1_operations, duration_seconds,
                error_code, error_summary, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        `).bind(
            id,
            runData.jobName,
            runData.startedAt,
            runData.finishedAt,
            runData.status,
            runData.recordsFetched || 0,
            runData.recordsUpserted || 0,
            runData.recordsRemoved || 0,
            runData.bridgePages || 0,
            runData.d1Operations || 0,
            runData.durationSeconds || 0.0,
            runData.errorCode || null,
            runData.errorSummary || null
        ).run();
    } catch (err) {
        console.error(`[SYNC RUN LOG ERROR]:`, err.message);
    }
}
