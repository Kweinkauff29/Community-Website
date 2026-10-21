async function testSync() {
  const EVENT_START = '2026-10-30';
  const EVENT_END = '2026-11-01';
  const ohFilter = `(OpenHouseStatus eq 'Active' or OpenHouseStatus eq null) and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge ${EVENT_START} and OpenHouseDate le ${EVENT_END}`;
  const ohURL = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(ohFilter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  
  console.log('Fetching OH records...');
  const res = await fetch(ohURL);
  const d = await res.json();
  const ohRec = d.value || [];
  console.log(`Found ${ohRec.length} raw open houses.`);

  const listingKeys = [...new Set(ohRec.map(r => r.ListingKey))];
  console.log(`Found ${listingKeys.length} unique listing keys.`);

  const PROP_SEL = 'ListingKey,ListingId,UnparsedAddress,City,PostalCode,ListPrice,PropertyType,PropertySubType,BedroomsTotal,BathroomsTotalInteger,LivingArea,LotSizeAcres,YearBuilt,StandardStatus,SubdivisionName,ListAgentFullName,ListAgentEmail,ListAgentDirectPhone,ListAgentKey,ListAgentMlsId,ListOfficeName,ListOfficePhone,ListOfficeMlsId,OriginatingSystemName,PublicRemarks,Coordinates,Media';
  let properties = [];

  for (let i = 0; i < listingKeys.length; i += 25) {
    const chunk = listingKeys.slice(i, i + 25);
    const batchFilter = chunk.map(k => `ListingKey eq '${k}'`).join(' or ');
    const pURL = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/Property?$filter=${encodeURIComponent(`(${batchFilter})`)}&$top=100&$select=${PROP_SEL}`;
    const pres = await fetch(pURL);
    if (pres.ok) {
      const pd = await pres.json();
      properties.push(...(pd.value || []));
    }
  }
  console.log(`Fetched ${properties.length} properties.`);

  const propMap = new Map(properties.map(p => [p.ListingKey, p]));
  const CCOR_CITIES = ['BONITA SPRINGS', 'ESTERO'];

  const filtered = ohRec.filter(oh => {
    const p = propMap.get(oh.ListingKey);
    if (!p || !p.City) return false;
    const city = p.City.toUpperCase().trim();
    const orig = (p.OriginatingSystemName || '').toLowerCase();
    const agentId = (p.ListAgentMlsId || '').toUpperCase();
    const officeId = (p.ListOfficeMlsId || '').toUpperCase();
    return CCOR_CITIES.includes(city) || orig.includes('bonita') || agentId.startsWith('B') || officeId.startsWith('B');
  });

  console.log(`Filtered open houses count: ${filtered.length}`);
  const cityCounts = {};
  filtered.forEach(oh => {
    const p = propMap.get(oh.ListingKey);
    const c = p?.City || 'Unknown';
    cityCounts[c] = (cityCounts[c] || 0) + 1;
  });
  console.log('City breakdown:', cityCounts);
  const dateCounts = {};
  filtered.forEach(oh => {
    const d = (oh.OpenHouseDate || oh.OpenHouseStartTime || '').slice(0, 10);
    dateCounts[d] = (dateCounts[d] || 0) + 1;
  });
  console.log('Date breakdown:', dateCounts);
}

testSync();
