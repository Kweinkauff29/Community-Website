/**
 * sneak-alerts/matcher.js
 * 
 * Saved Search Matching Engine & Due Alert Discovery.
 * 
 * Reuses buildSavedSearchWhereQuery from sneak-shared/idx-query.js to guarantee
 * 100% filter matching parity with public search engine.
 */

import { buildSavedSearchWhereQuery } from '../sneak-shared/idx-query.js';

const BATCH_ALERT_LIMIT = 150;

/**
 * Calculates current local hour (0-23) and local date ('YYYY-MM-DD') for a given IANA timezone.
 */
export function getLocalTimeInZone(timeZone = 'America/New_York', date = new Date()) {
    try {
        const formatter = new Intl.DateTimeFormat('en-US', {
            timeZone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: 'numeric',
            hourCycle: 'h23'
        });

        const parts = formatter.formatToParts(date);
        let year = '', month = '', day = '', hour = 0;

        for (const p of parts) {
            if (p.type === 'year') year = p.value;
            if (p.type === 'month') month = p.value;
            if (p.type === 'day') day = p.value;
            if (p.type === 'hour') hour = parseInt(p.value, 10);
        }

        const localDate = `${year}-${month}-${day}`;
        return { hour, localDate, valid: true };
    } catch {
        // Fallback to UTC / New York on invalid timezone
        const d = new Date(date);
        const localDate = d.toISOString().slice(0, 10);
        return { hour: d.getUTCHours(), localDate, valid: false };
    }
}

/**
 * Discovers due search alerts for the current cron execution.
 */
export async function findDueAlerts(db, { limit = BATCH_ALERT_LIMIT, now = new Date() } = {}) {
    const rows = await db.prepare(`
        SELECT 
            a.id AS alert_id,
            a.saved_search_id,
            a.site_id,
            a.user_id,
            a.frequency,
            a.enabled,
            a.enabled_at,
            a.timezone,
            a.return_url,
            a.last_checked_at,
            a.last_sent_at,
            a.last_daily_local_date,
            s.name AS search_name,
            s.state_json,
            u.email AS consumer_email,
            u.status AS user_status,
            si.site_key,
            si.scope_type,
            si.scope_value,
            si.status AS site_status,
            b.display_name,
            b.brokerage,
            b.primary_color,
            acc.account_name,
            acc.status AS account_status
        FROM sneak_consumer_search_alerts a
        JOIN sneak_consumer_saved_searches s ON a.saved_search_id = s.id
        JOIN sneak_consumer_users u ON a.user_id = u.id
        JOIN sneak_sites si ON a.site_id = si.id
        JOIN sneak_accounts acc ON si.account_id = acc.id
        LEFT JOIN sneak_branding b ON si.id = b.site_id
        WHERE a.enabled = 1
          AND a.frequency IN ('asap', 'daily')
          AND u.status = 'active'
          AND si.status = 'active'
          AND acc.status = 'active'
        ORDER BY a.last_checked_at ASC NULLS FIRST, a.created_at ASC
        LIMIT ?
    `).bind(limit).all();

    const candidates = rows.results || [];
    const dueAlerts = [];

    for (const candidate of candidates) {
        if (candidate.frequency === 'asap') {
            dueAlerts.push(candidate);
        } else if (candidate.frequency === 'daily') {
            const tz = candidate.timezone || 'America/New_York';
            const { hour, localDate } = getLocalTimeInZone(tz, now);

            // Daily digest sends after 8:00 AM local time, once per local calendar day
            if (hour >= 8 && candidate.last_daily_local_date !== localDate) {
                candidate.currentLocalDate = localDate;
                dueAlerts.push(candidate);
            }
        }
    }

    return dueAlerts;
}

/**
 * Evaluates candidate listings for a saved search alert using shared query compiler.
 */
export async function matchNewListingsForAlert(db, alertRecord) {
    const site = {
        id: alertRecord.site_id,
        site_key: alertRecord.site_key,
        scope_type: alertRecord.scope_type,
        scope_value: alertRecord.scope_value
    };

    let stateObj;
    try {
        stateObj = JSON.parse(alertRecord.state_json);
    } catch {
        return { valid: false, error: 'MalformedSavedSearchState', listings: [] };
    }

    const queryResult = buildSavedSearchWhereQuery(site, stateObj);
    if (!queryResult.valid) {
        return { valid: false, error: queryResult.error, listings: [] };
    }

    const whereClauses = [...queryResult.whereClauses];
    const bindValues = [...queryResult.bindValues];

    // Baseline: only listings at or after alert.enabled_at (anti-spam historical isolation)
    const baselineDate = alertRecord.enabled_at ? alertRecord.enabled_at.slice(0, 10) : new Date().toISOString().slice(0, 10);
    const baselineTimestamp = alertRecord.enabled_at || new Date().toISOString();

    whereClauses.push(`(
        ListingContractDate >= ? 
        OR (ListingContractDate IS NULL AND ModificationTimestamp >= ?) 
        OR (ListingContractDate IS NULL AND ModificationTimestamp IS NULL AND created_at >= ?)
    )`);
    bindValues.push(baselineDate, baselineTimestamp, baselineTimestamp);

    // Anti-replay: exclude listings already notified, delivered, or actively claimed by an ongoing unexpired run
    whereClauses.push(`ListingKey NOT IN (
        SELECT listing_key FROM sneak_consumer_alert_matches 
        WHERE alert_id = ? AND event_type = 'new_listing'
          AND (
              notified_at IS NOT NULL 
              OR delivery_status = 'sent'
              OR (delivery_status = 'claimed' AND claim_expires_at > datetime('now'))
          )
    )`);
    bindValues.push(alertRecord.alert_id);

    const query = `
        SELECT 
            ListingKey, ListingId, ListPrice, StandardStatus, PropertyType, PropertySubType,
            BedroomsTotal, BathroomsTotalInteger, LivingArea, LotSizeAcres, YearBuilt,
            City, PostalCode, UnparsedAddress, StreetNumber, StreetName, UnitNumber,
            PrimaryPhoto, MediaJSON, InternetEntireListingDisplayYN, InternetAddressDisplayYN,
            ListOfficeName, WaterfrontYN, PoolPrivateYN, SubdivisionName,
            ListingContractDate, ModificationTimestamp
        FROM sneak_listings
        WHERE ${whereClauses.join(' AND ')}
        ORDER BY ListingContractDate DESC, ModificationTimestamp DESC
        LIMIT 50
    `;

    try {
        const res = await db.prepare(query).bind(...bindValues).all();
        const listings = res.results || [];
        return { valid: true, listings };
    } catch (err) {
        return { valid: false, error: err.message, listings: [] };
    }
}
