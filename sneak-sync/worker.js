/**
 * sneak-sync/worker.js
 * 
 * SNEAK IDX Platform — Dedicated Background Synchronization Worker
 * 
 * ROLE:
 * - Executes automated scheduled sync tasks (listing deltas, open house deltas, reconciliation).
 * - Holds BRIDGE_TOKEN secret securely.
 * - Writes directly to D1 database.
 * - Does NOT expose public mutation or trigger routes.
 */

import { runListingDelta, runFullInventoryBootstrap } from './listing-sync.js';
import { runOpenHouseSync } from './open-house-sync.js';
import { runListingReconciliation } from './reconciliation.js';

export const SNEAK_SYNC_BUILD = '2026.09.01.7.4b2';

export default {
    /**
     * Public HTTP fetch handler (Read-Only Health Check).
     */
    async fetch(request, env, ctx) {
        const url = new URL(request.url);

        if (url.pathname === '/' || url.pathname === '/health') {
            let syncState = null;
            if (env?.DB) {
                try {
                    syncState = await env.DB.prepare("SELECT * FROM sneak_sync_state WHERE sync_name = 'listings'").first();
                } catch {}
            }

            const hasCursor = Boolean(syncState?.last_cursor);
            const status = syncState?.status;
            const recordCount = Number(syncState?.last_record_count || 0);

            let syncReadiness = 'NOT_INITIALIZED';
            if (!hasCursor) {
                if (status === 'running') {
                    syncReadiness = 'BOOTSTRAPPING';
                } else if (status === 'failure') {
                    syncReadiness = 'FAILED';
                } else {
                    syncReadiness = 'NOT_INITIALIZED';
                }
            } else {
                if (status === 'failure') {
                    syncReadiness = 'FAILED';
                } else if (status === 'success' && recordCount > 0) {
                    syncReadiness = 'HEALTHY';
                } else if (status === 'running') {
                    syncReadiness = recordCount > 0 ? 'HEALTHY' : 'BOOTSTRAPPING';
                } else {
                    syncReadiness = recordCount > 0 ? 'HEALTHY' : 'NOT_INITIALIZED';
                }
            }

            return new Response(JSON.stringify({
                service: env?.SNEAK_SERVICE_NAME || (env?.SNEAK_ENV === 'production' ? 'sneak-idx-sync' : 'sneak-idx-sync-staging'),
                build: SNEAK_SYNC_BUILD,
                status: 'ok',
                env: env?.SNEAK_ENV || 'staging',
                timestamp: new Date().toISOString(),
                syncReadiness,
                syncState: {
                    last_cursor: syncState?.last_cursor || null,
                    last_successful_sync: syncState?.last_successful_sync || null,
                    last_full_reconciliation: syncState?.last_full_reconciliation || null,
                    last_record_count: recordCount,
                    status: syncState?.status || 'uninitialized'
                }
            }, null, 2), {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-store'
                }
            });
        }

        return new Response(JSON.stringify({
            error: 'NotFound',
            message: 'SNEAK background sync worker does not expose public endpoints.'
        }), {
            status: 404,
            headers: { 'Content-Type': 'application/json' }
        });
    },

    /**
     * Scheduled cron handler.
     */
    async scheduled(controller, env, ctx) {
        const cron = controller.cron;
        console.log(`[SCHEDULED EVENT TRIGGERED] Cron expression: ${cron}`);

        switch (cron) {
            case "*/15 * * * *":
                ctx.waitUntil(runListingDelta(env));
                break;

            case "7 * * * *":
                ctx.waitUntil(runOpenHouseSync(env));
                break;

            case "23 7 * * *":
                ctx.waitUntil(runListingReconciliation(env));
                break;

            default:
                console.warn(`[UNKNOWN CRON SCHEDULE] '${cron}' triggered. Zero database mutations executed.`);
                break;
        }
    }
};

export {
    runListingDelta,
    runFullInventoryBootstrap,
    runOpenHouseSync,
    runListingReconciliation
};
