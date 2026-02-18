const https = require('https');

// Check counts for each type with Active status and Bonita Springs source
const types = [
    { name: 'Commercial', filter: "(PropertyType eq 'Commercial Sale' or PropertyType eq 'Commercial')" },
    { name: 'Land', filter: "PropertyType eq 'Land'" },
    { name: 'Rental', filter: "PropertyType eq 'Residential Lease'" },
    { name: 'Sale', filter: "(PropertyType eq 'Residential' or PropertyType eq 'Residential Income')" }
];

const checkType = (type) => {
    const filter = `OriginatingSystemName eq 'Bonita Springs' and StandardStatus eq 'Active' and ${type.filter}`;
    const url = `https://listingsworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/Property?$top=1&$count=true&$filter=${encodeURIComponent(filter)}`;

    https.get(url, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
            try {
                const json = JSON.parse(data);
                console.log(`${type.name}: ${json['@odata.count']} listings found`);
            } catch (e) {
                console.error(`${type.name}: Error parsing JSON`, e.message);
            }
        });
    });
};

types.forEach(checkType);
