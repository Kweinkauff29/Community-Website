/**
 * sneak-sync/worker.js
 * 
 * SNEAK IDX Platform — Dedicated Background Synchronization Worker
 * 
 * ROLE:
 * - Executes automated scheduled sync tasks (listing deltas, open house deltas, reconciliation).
 * - Holds BRIDGE_TOKEN secret securely.
 * - Writes directly to sneak-idx-staging D1.
 * - Does NOT expose public mutation or trigger routes.
 */

import { runListingDelta } from './listing-sync.js';
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
            return new Response(JSON.stringify({
                service: env?.SNEAK_SERVICE_NAME || 'sneak-idx-sync-staging',
                build: SNEAK_SYNC_BUILD,
                status: 'ok',
                env: env.SNEAK_ENV || 'staging',
                timestamp: new Date().toISOString()
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
    runOpenHouseSync,
    runListingReconciliation
};
