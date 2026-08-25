// scripts/audit-bsaor-fields.mjs
import { readFileSync, writeFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { BRIDGE_BASE_URL, BRIDGE_PROPERTY_ENDPOINT, FINAL_SNEAK_LISTING_FILTER } from './bridge-config.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

let bridgeToken = process.env.BRIDGE_TOKEN;
if (!bridgeToken) {
    try {
        const vars = readFileSync(path.join(rootDir, '.dev.vars'), 'utf8');
        for (const line of vars.split('\n')) {
            const trimmed = line.trim();
            if (trimmed.startsWith('BRIDGE_TOKEN=')) {
                bridgeToken = trimmed.substring('BRIDGE_TOKEN='.length).trim().replace(/^["']|["']$/g, '');
            }
        }
    } catch {}
}

if (!bridgeToken) {
    console.error('BRIDGE_TOKEN not found in environment or .dev.vars');
    process.exit(1);
}

async function run() {
    console.log('====================================================');
    console.log('BSAOR ODATA METADATA & FIELD COVERAGE AUDIT');
    console.log('====================================================\n');

    // 1. Fetch $metadata
    console.log('1. Fetching BSAOR OData $metadata XML...');
    const metaUrl = `${BRIDGE_BASE_URL}/$metadata?access_token=${bridgeToken}`;
    const metaRes = await fetch(metaUrl);
    if (!metaRes.ok) {
        throw new Error(`Failed to fetch $metadata: HTTP ${metaRes.status}`);
    }
    const metaXml = await metaRes.text();
    console.log(`   Fetched $metadata (${(metaXml.length / 1024).toFixed(1)} KB)`);

    // Parse Property EntityType Property elements
    const propRegex = /<Property\s+Name="([^"]+)"\s+Type="([^"]+)"/g;
    let match;
    const metadataFields = new Map();
    while ((match = propRegex.exec(metaXml)) !== null) {
        metadataFields.set(match[1], match[2]);
    }
    console.log(`   Discovered ${metadataFields.size} total fields defined on Property entity in $metadata.`);

    // Candidate Search Terms
    const candidateTerms = [
        'Waterfront', 'Pool', 'Garage', 'Parking', 'Fireplace', 'Association', 'HOA',
        'Senior', '55', 'NewConstruction', 'VirtualTour', 'PriceChange', 'DaysOnMarket',
        'DaysOnline', 'Stories', 'View', 'School', 'Zoning', 'RoadFrontage', 'Utilities',
        'Sewer', 'WaterSource', 'Furnished', 'Pets', 'ArchitecturalStyle', 'CommunityFeatures',
        'LotFeatures', 'InteriorFeatures', 'ExteriorFeatures', 'WaterfrontFeatures'
    ];

    const discoveredCandidates = [];
    for (const [field, type] of metadataFields.entries()) {
        for (const term of candidateTerms) {
            if (field.toLowerCase().includes(term.toLowerCase())) {
                discoveredCandidates.push({ field, type, matchedTerm: term });
                break;
            }
        }
    }

    console.log(`\n2. Discovered ${discoveredCandidates.length} candidate fields matching semantic amenity concepts in $metadata:\n`);
    for (const c of discoveredCandidates) {
        console.log(`   - ${c.field} (${c.type}) [Concept: ${c.matchedTerm}]`);
    }

    // Select primary candidate fields to sample for coverage
    const targetFields = [
        'ListingKey', 'PropertyType', 'PropertySubType',
        'WaterfrontYN', 'WaterfrontFeatures',
        'PoolPrivateYN', 'PoolFeatures',
        'GarageSpaces', 'AttachedGarageYN', 'ParkingTotal',
        'FireplaceYN', 'FireplacesTotal',
        'AssociationFee', 'AssociationFeeFrequency', 'AssociationYN',
        'SeniorCommunityYN', 'CommunityFeatures',
        'NewConstructionYN',
        'VirtualTourURLUnbranded',
        'DaysOnMarket', 'ListingContractDate', 'StatusChangeTimestamp', 'ModificationTimestamp', 'OriginalListPrice',
        'Stories', 'StoriesTotal',
        'View', 'ViewYN',
        'Zoning', 'ZoningDescription',
        'RoadFrontageType', 'Utilities', 'Sewer', 'WaterSource',
        'Furnished', 'PetsAllowed', 'ArchitecturalStyle'
    ].filter(f => metadataFields.has(f) || f === 'ListingKey');

    console.log(`\n3. Sampling 600 live eligible records for ${targetFields.length} target fields (3 pages of 200)...`);

    let nextLink = `${BRIDGE_PROPERTY_ENDPOINT}?$top=200&$select=${targetFields.join(',')}&$filter=${encodeURIComponent(FINAL_SNEAK_LISTING_FILTER)}&access_token=${bridgeToken}`;
    const records = [];

    while (nextLink && records.length < 600) {
        const sampleRes = await fetch(nextLink, { headers: { Accept: 'application/json' } });
        if (!sampleRes.ok) {
            throw new Error(`Sample fetch failed: HTTP ${sampleRes.status} ${await sampleRes.text()}`);
        }
        const sampleData = await sampleRes.json();
        const batch = sampleData.value || [];
        records.push(...batch);
        nextLink = sampleData['@odata.nextLink'] || null;
        if (nextLink && !nextLink.includes('access_token=')) {
            const u = new URL(nextLink);
            u.searchParams.set('access_token', bridgeToken);
            nextLink = u.toString();
        }
    }
    console.log(`   Successfully fetched ${records.length} sample records.\n`);

    // Calculate coverage stats
    const stats = {};
    for (const f of targetFields) {
        if (f === 'ListingKey') continue;
        stats[f] = {
            field: f,
            type: metadataFields.get(f) || 'Unknown',
            total: records.length,
            nonNull: 0,
            values: new Set(),
            resNonZero: 0,
            commNonZero: 0,
            landNonZero: 0
        };
    }

    for (const r of records) {
        const isComm = (r.PropertyType || '').toLowerCase().includes('commercial') || (r.PropertyType || '').toLowerCase().includes('business');
        const isLand = (r.PropertyType || '').toLowerCase() === 'land';
        const isRes = !isComm && !isLand;

        for (const f of targetFields) {
            if (f === 'ListingKey') continue;
            const val = r[f];
            if (val !== null && val !== undefined && val !== '' && val !== false && val !== 0 && !(Array.isArray(val) && val.length === 0)) {
                stats[f].nonNull++;
                if (isRes) stats[f].resNonZero++;
                if (isComm) stats[f].commNonZero++;
                if (isLand) stats[f].landNonZero++;
            }
            if (val !== null && val !== undefined && val !== '') {
                if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') {
                    if (stats[f].values.size < 6) stats[f].values.add(String(val));
                } else if (Array.isArray(val)) {
                    val.forEach(v => { if (stats[f].values.size < 6) stats[f].values.add(String(v)); });
                }
            }
        }
    }

    console.log('======================================================================================================');
    console.log('FIELD COVERAGE AUDIT RESULTS (500 Sample Records)');
    console.log('======================================================================================================');
    console.log(
        'Field Name'.padEnd(26) +
        '| Type'.padEnd(16) +
        '| Non-Null %'.padEnd(14) +
        '| Res %'.padEnd(10) +
        '| Land %'.padEnd(10) +
        '| Sample / Distinct Values'
    );
    console.log('-'.repeat(102));

    const auditSummary = [];

    for (const [f, s] of Object.entries(stats)) {
        const pct = ((s.nonNull / s.total) * 100).toFixed(1) + '%';
        const resPct = ((s.resNonZero / s.total) * 100).toFixed(1) + '%';
        const landPct = ((s.landNonZero / s.total) * 100).toFixed(1) + '%';
        const valPreview = Array.from(s.values).slice(0, 3).join(', ');

        console.log(
            s.field.padEnd(26) +
            `| ${s.type}`.padEnd(16) +
            `| ${pct}`.padEnd(14) +
            `| ${resPct}`.padEnd(10) +
            `| ${landPct}`.padEnd(10) +
            `| ${valPreview.substring(0, 30)}`
        );

        auditSummary.push({
            field: s.field,
            type: s.type,
            coverage: pct,
            sampleValues: valPreview,
            resRelevance: s.resNonZero > 0 ? 'High' : 'Low',
            landRelevance: s.landNonZero > 0 ? 'High' : 'Low'
        });
    }
    console.log('======================================================================================================\n');
}

run().catch(console.error);
