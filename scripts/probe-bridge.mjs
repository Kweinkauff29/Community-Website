#!/usr/bin/env node

/**
 * scripts/probe-bridge.mjs
 * 
 * Local-only utility to safely probe the Bridge Interactive OData BSAOR Property feed.
 * Validates field selection, schema availability, and feed connectivity.
 * 
 * SENSITIVITY NOTICE:
 * - Reads BRIDGE_TOKEN strictly from environment variables or .dev.vars.
 * - NEVER logs or prints the token value.
 * - Fails cleanly with 'BRIDGE PROBE NOT RUN — TOKEN UNAVAILABLE' if token is absent.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

// 1. Resolve BRIDGE_TOKEN from process env or .dev.vars
let bridgeToken = process.env.BRIDGE_TOKEN || '';

if (!bridgeToken) {
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
    console.log('BRIDGE PROBE NOT RUN — TOKEN UNAVAILABLE');
    console.log('====================================================');
    console.log('Notice: BRIDGE_TOKEN is not set in environment or .dev.vars.');
    console.log('To run this authenticated probe locally:');
    console.log('  1. Add BRIDGE_TOKEN="your-token" to .dev.vars (gitignored)');
    console.log('  2. Run: node scripts/probe-bridge.mjs');
    console.log('====================================================');
    process.exit(0);
}

// 2. Exact field selection intended for sneak_listings
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

async function runProbe() {
    console.log('====================================================');
    console.log('SNEAK IDX — AUTHENTICATED BRIDGE FEED PROBE');
    console.log('====================================================');
    console.log('Endpoint: https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property');
    console.log(`Testing field selection for ${INTENDED_FIELDS.length} intended sneak_listings fields...`);

    const selectParam = INTENDED_FIELDS.join(',');
    const url = `https://api.bridgedataoutput.com/api/v2/OData/bsaor/Property?$top=1&$select=${selectParam}&access_token=${bridgeToken}`;

    try {
        const response = await fetch(url, { headers: { Accept: 'application/json' } });

        if (!response.ok) {
            console.error(`\nProbe failed: Bridge API returned HTTP ${response.status} ${response.statusText}`);
            try {
                const errBody = await response.json();
                console.error('Error detail:', JSON.stringify(errBody, null, 2));
            } catch {}
            process.exit(1);
        }

        const data = await response.json();
        const records = data.value || [];

        if (records.length === 0) {
            console.log('\nField Selection: SUCCESS (HTTP 200)');
            console.log('Notice: 0 records returned for $top=1 probe.');
            return;
        }

        const sample = records[0];
        console.log('\nField Selection: SUCCESS (HTTP 200)');
        console.log('Sample Record Property Inspection:');
        console.log('----------------------------------------------------');
        console.log(`- ListingKey:           ${sample.ListingKey || '[N/A]'}`);
        console.log(`- City:                 ${sample.City || '[N/A]'}`);
        console.log(`- CountyOrParish:       ${sample.CountyOrParish || '[N/A]'}`);
        console.log(`- StateOrProvince:      ${sample.StateOrProvince || '[N/A]'}`);
        console.log(`- StandardStatus:       ${sample.StandardStatus || '[N/A]'}`);
        console.log(`- PropertyType:         ${sample.PropertyType || '[N/A]'}`);
        console.log(`- ListOfficeMlsId:      ${sample.ListOfficeMlsId || '[N/A]'}`);
        console.log(`- ListOfficeKey:        ${sample.ListOfficeKey || '[N/A]'}`);
        console.log(`- ListAgentMlsId:       ${sample.ListAgentMlsId || '[N/A]'}`);
        console.log(`- OriginatingSystemKey: ${sample.OriginatingSystemKey || '[N/A]'}`);
        console.log(`- OriginatingSystemName:${sample.OriginatingSystemName || '[N/A]'}`);
        console.log('----------------------------------------------------');

        // Check which requested fields are present in the response
        const returnedKeys = new Set(Object.keys(sample));
        const missingFields = INTENDED_FIELDS.filter(f => !returnedKeys.has(f));

        if (missingFields.length === 0) {
            console.log('All 42 intended fields recognized and returned by Bridge feed.');
        } else {
            console.log(`Fields omitted / null in sample record (${missingFields.length}):`);
            console.log('  ' + missingFields.join(', '));
        }

        console.log('\n====================================================');
        console.log('AUTHENTICATED BRIDGE PROBE COMPLETED SUCCESSFULLY');
        console.log('====================================================');
    } catch (err) {
        console.error('Probe execution error:', err.message);
        process.exit(1);
    }
}

runProbe();
