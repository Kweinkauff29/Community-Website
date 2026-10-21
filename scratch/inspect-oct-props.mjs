async function inspectProps() {
  const EVENT_START = '2026-10-30';
  const EVENT_END = '2026-11-01';
  const ohFilter = `(OpenHouseStatus eq 'Active' or OpenHouseStatus eq null) and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge ${EVENT_START} and OpenHouseDate le ${EVENT_END}`;
  const ohURL = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(ohFilter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  
  const res = await fetch(ohURL);
  const d = await res.json();
  const ohRec = d.value || [];
  const listingKeys = [...new Set(ohRec.map(r => r.ListingKey))];

  const PROP_SEL = 'ListingKey,ListingId,UnparsedAddress,City,PostalCode,ListPrice,PropertyType,PropertySubType,OriginatingSystemName,ListAgentMlsId,ListOfficeMlsId';
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

  console.log('All properties returned:');
  properties.forEach(p => {
    console.log({
      address: p.UnparsedAddress,
      city: p.City,
      orig: p.OriginatingSystemName,
      agentId: p.ListAgentMlsId,
      officeId: p.ListOfficeMlsId
    });
  });
}

inspectProps();
