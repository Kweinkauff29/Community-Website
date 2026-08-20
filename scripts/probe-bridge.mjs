#!/usr/bin/env node

/**
 * scripts/probe-bridge.mjs
 * 
 * Local-only utility to safely probe and audit the Bridge Interactive OData BSAOR Property feed.
 * Validates field selection, schema availability, feed connectivity, coverage statistics,
 * office/agent identifier population rates, media structures, and open house resource.
 * 
 * SENSITIVITY NOTICE:
 * - Reads BRIDGE_TOKEN strictly from environment variables or .dev.vars.
 * - NEVER logs, prints, or exports the token value.
 * - Fails cleanly with 'BRIDGE TOKEN REQUIRED LOCALLY' if token is absent.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

// 1. Resolve BRIDGE_TOKEN from process env or .dev.vars
let bridgeToken = process.env.BRIDGE_TOKEN;

if (bridgeToken === undefined) {
    const devVarsPath = path.join(rootDir, '.dev.vars');
    if (fs.existsSync(devVarsPath)) {
        try {
            const content = fs.readFileSync(devVarsPath, 'utf8');
            const lines = content.split('\n');
            for (const line of lines) {
                const trimmed = line.trim();
                if (trimmed.startsWith('BRIDGE_TOKEN=')) {
                    bridgeToken = trimmed.substring('BRIDGE_TOKEN='.length).trim().replace(/^["']|["']$/g, '');
                    break;
                }
            }
        } catch {}
    }
}

if (!bridgeToken) {
    console.log('====================================================');
    console.log('BRIDGE TOKEN REQUIRED LOCALLY');
    console.log('BRIDGE PROBE NOT RUN — TOKEN UNAVAILABLE');
    console.log('====================================================');
    console.log('Notice: BRIDGE_TOKEN is not set in environment or .dev.vars.');
    console.log('To run this authenticated probe locally:');
    console.log('  1. Add BRIDGE_TOKEN="your-token" to .dev.vars (gitignored)');
    console.log('  2. Run: node scripts/probe-bridge.mjs');
    console.log('====================================================');
    process.exit(0);
}

// 2. Exact 42 field selection intended for sneak_listings
const INTENDED_FIELDS = [
    'ListingKey', 'ListingId', 'ListPrice', 'OriginalListPrice',
    'UnparsedAddress', 'StreetNumber', 'StreetName', 'UnitNumber',
    'City', 'StateOrProvince', 'PostalCode', 'CountyOrParish',
    'BedroomsTotal', 'BathroomsTotalInteger', 'BathroomsFull', 'BathroomsHalf',
    'LivingArea', 'StandardStatus', 'PropertyType', 'PropertySubType',
    'ListingContractDate', 'ModificationTimestamp', 'StatusChangeTimestamp',
    'YearBuilt', 'LotSizeAcres', 'Latitude', 'Longitude', 'Coordinates',
    'Media', 'PublicRemarks', 'SubdivisionName',
    'ListAgentKey', 'ListAgentMlsId', 'ListAgentFullName', 'ListAgentEmail', 'ListAgentDirectPhone',
    'ListOfficeKey', 'ListOfficeMlsId', 'ListOfficeName', 'ListOfficePhone',
    'OriginatingSystemKey', 'OriginatingSystemName'
];

async function bridgeFetch(endpointPath, queryParams = {}) {
    const url = new URL(`https://api.bridgedataoutput.com/api/v2/OData/bsaor/${endpointPath}`);
    for (const [k, v] of Object.entries(queryParams)) {
        url.searchParams.set(k, v);
    }
    url.searchParams.set('access_token', bridgeToken);

    const response = await fetch(url.toString(), {
        headers: { Accept: 'application/json' }
    });

    if (!response.ok) {
        let errText = '';
        try {
            const errJson = await response.json();
            errText = JSON.stringify(errJson, null, 2);
        } catch {
            errText = await response.text();
        }
        throw new Error(`Bridge API HTTP ${response.status} ${response.statusText} on ${endpointPath}:\n${errText}`);
    }

    return await response.json();
}

async function runProbe() {
    console.log('====================================================');
    console.log('SNEAK IDX — AUTHENTICATED BRIDGE FEED AUDIT & PROBE');
    console.log('====================================================');
    console.log('Target Base: https://api.bridgedataoutput.com/api/v2/OData/bsaor');

    try {
        // --- 1. FIELD VALIDATION ($top=1 without select to inspect native fields) ---
        console.log('\n[1/5] Probing Property Feed Schema with native record inspect ($top=1)...');
        const rawSampleData = await bridgeFetch('Property', { '$top': '1' });
        const rawRecords = rawSampleData.value || [];
        if (rawRecords.length === 0) {
            console.log('  Notice: 0 records returned for $top=1 probe.');
            return;
        }

        const sample = rawRecords[0];
        const nativeKeys = new Set(Object.keys(sample));
        console.log(`  HTTP 200 OK — Successfully retrieved native record (ListingKey: ${sample.ListingKey})`);
        console.log(`  Total native fields present on record: ${nativeKeys.size}`);
        console.log('  Sample Coordinates value:', JSON.stringify(sample.Coordinates));
        console.log('  Sample Latitude/Longitude fields present?:', 'Latitude' in sample, 'Longitude' in sample);

        // Check which intended fields are present natively
        const missing = INTENDED_FIELDS.filter(f => !nativeKeys.has(f));
        const present = INTENDED_FIELDS.filter(f => nativeKeys.has(f));
        console.log(`  Intended fields natively present: ${present.length}/${INTENDED_FIELDS.length}`);
        if (missing.length > 0) {
            console.log(`  Intended fields NOT in native keys: ${missing.join(', ')}`);
        }

        // Adjust SELECT_FIELDS for OData queries (excluding Latitude/Longitude if rejected by Bridge)
        const validSelectFields = INTENDED_FIELDS.filter(f => f !== 'Latitude' && f !== 'Longitude' && nativeKeys.has(f));
        const fieldSelect = validSelectFields.join(',');
        console.log(`  Valid OData $select list (${validSelectFields.length} fields)`);

        // --- 2. ACTIVE/PENDING COUNTS & ORIGIN SYSTEMS ($top=200 sample audit) ---
        console.log('\n[2/5] Auditing Dataset Counts & Identifier Coverage ($top=200 audit sample)...');
        const filter = "StateOrProvince eq 'FL' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";
        const auditData = await bridgeFetch('Property', {
            '$top': '200',
            '$filter': filter,
            '$select': fieldSelect,
            '$count': 'true'
        });

        const totalCount = auditData['@odata.count'] || auditData.value?.length || 0;
        const auditRecords = auditData.value || [];
        console.log(`  Total Florida Active/Pending/Under Contract count (@odata.count): ${totalCount}`);

        // Statistics aggregates
        const originSystems = {};
        const counties = {};
        const cities = {};
        const propTypes = {};
        const statuses = {};
        let withListOfficeMlsId = 0;
        let withListOfficeKey = 0;
        let withListAgentMlsId = 0;
        let withCoords = 0;
        let withMedia = 0;

        for (const r of auditRecords) {
            const osKey = r.OriginatingSystemKey || 'UNKNOWN';
            originSystems[osKey] = (originSystems[osKey] || 0) + 1;

            const cty = r.CountyOrParish || 'UNKNOWN';
            counties[cty] = (counties[cty] || 0) + 1;

            const city = r.City || 'UNKNOWN';
            cities[city] = (cities[city] || 0) + 1;

            const pt = r.PropertyType || 'UNKNOWN';
            propTypes[pt] = (propTypes[pt] || 0) + 1;

            const st = r.StandardStatus || 'UNKNOWN';
            statuses[st] = (statuses[st] || 0) + 1;

            if (r.ListOfficeMlsId) withListOfficeMlsId++;
            if (r.ListOfficeKey) withListOfficeKey++;
            if (r.ListAgentMlsId) withListAgentMlsId++;
            if ((r.Latitude != null && r.Longitude != null) || (Array.isArray(r.Coordinates) && r.Coordinates.length >= 2)) withCoords++;
            if (Array.isArray(r.Media) && r.Media.length > 0) withMedia++;
        }

        console.log('\n  [A] Status Distribution (in sample):', statuses);
        console.log('  [B] Originating Systems (in sample):', originSystems);
        console.log('  [C] County Distribution (top sample):', counties);
        console.log('  [D] Property Types (in sample):', propTypes);

        console.log('\n  [E] Scoping Identifier Population Rates (in sample):');
        console.log(`    - ListOfficeMlsId: ${withListOfficeMlsId}/${auditRecords.length} (${Math.round((withListOfficeMlsId/auditRecords.length)*100)}%)`);
        console.log(`    - ListOfficeKey:   ${withListOfficeKey}/${auditRecords.length} (${Math.round((withListOfficeKey/auditRecords.length)*100)}%)`);
        console.log(`    - ListAgentMlsId:  ${withListAgentMlsId}/${auditRecords.length} (${Math.round((withListAgentMlsId/auditRecords.length)*100)}%)`);

        console.log('\n  [F] Asset & Spatial Coverage (in sample):');
        console.log(`    - Usable Map Coordinates: ${withCoords}/${auditRecords.length} (${Math.round((withCoords/auditRecords.length)*100)}%)`);
        console.log(`    - Media / Photos:         ${withMedia}/${auditRecords.length} (${Math.round((withMedia/auditRecords.length)*100)}%)`);

        // --- 3. BSAOR-SPECIFIC FILTER COUNT ---
        console.log('\n[3/5] Checking BSAOR Origin-Filtered Count...');
        const bsaorFilter = "OriginatingSystemKey eq 'bsaor' and (StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract' or StandardStatus eq 'Pending')";
        const bsaorData = await bridgeFetch('Property', {
            '$top': '1',
            '$filter': bsaorFilter,
            '$count': 'true'
        });
        const bsaorCount = bsaorData['@odata.count'] || 0;
        console.log(`  BSAOR Specific Count (@odata.count): ${bsaorCount}`);

        // --- 4. MEDIA STRUCTURE AUDIT ---
        console.log('\n[4/5] Auditing Media Array Structure & Field Naming...');
        const mediaSample = auditRecords.find(r => Array.isArray(r.Media) && r.Media.length > 0);
        if (mediaSample) {
            const firstPhoto = mediaSample.Media[0];
            console.log('  Sample Media Item keys:', Object.keys(firstPhoto).join(', '));
            console.log('  MediaURL present:', Boolean(firstPhoto.MediaURL));
            console.log('  Order present:', firstPhoto.Order ?? firstPhoto.MediaCategory ?? '[N/A]');
        }

        // --- 5. OPEN HOUSE RESOURCE AUDIT ---
        console.log('\n[5/5] Auditing OpenHouse Resource Feed...');
        try {
            const ohData = await bridgeFetch('OpenHouse', {
                '$top': '5',
                '$count': 'true'
            });
            const ohCount = ohData['@odata.count'] || ohData.value?.length || 0;
            console.log(`  OpenHouse Endpoint: HTTP 200 OK | Count: ${ohCount}`);
            if (ohData.value?.length > 0) {
                const sampleOH = ohData.value[0];
                console.log('  Sample OpenHouse keys:', Object.keys(sampleOH).join(', '));
                console.log(`  Sample OH: ListingKey=${sampleOH.ListingKey}, Date=${sampleOH.OpenHouseDate || sampleOH.OpenHouseStartTime}`);
            }
        } catch (ohErr) {
            console.log('  OpenHouse Resource Notice:', ohErr.message);
        }

        console.log('\n====================================================');
        console.log('AUTHENTICATED BRIDGE AUDIT PROBE COMPLETED');
        console.log('====================================================');

    } catch (err) {
        console.error('Probe execution error:', err.message);
        process.exit(1);
    }
}

runProbe();
