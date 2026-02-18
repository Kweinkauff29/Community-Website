const https = require('https');

const url = "https://listingsworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/Property?$select=PropertyType,PropertySubType&$top=50&$filter=OriginatingSystemName eq 'Bonita Springs' and contains(PropertyType, 'Commercial')";

https.get(url, (res) => {
    let data = '';
    res.on('data', (chunk) => data += chunk);
    res.on('end', () => {
        try {
            const json = JSON.parse(data);
            if (!json.value) {
                console.log('No value property. Raw Data:');
                console.log(data.substring(0, 500));
                return;
            }

            const types = new Set();
            const subTypes = new Set();
            const pairs = new Set();

            json.value.forEach(item => {
                types.add(item.PropertyType);
                subTypes.add(item.PropertySubType);
                pairs.add(`${item.PropertyType} -> ${item.PropertySubType}`);
            });

            console.log('--- Property Types ---');
            console.log([...types].sort().join('\n'));
            console.log('\n--- Property SubTypes ---');
            console.log([...subTypes].sort().join('\n'));
            console.log('\n--- Pairs ---');
            console.log([...pairs].sort().join('\n'));

        } catch (e) {
            console.error(e.message);
        }
    });
}).on('error', (e) => {
    console.error(e);
});
