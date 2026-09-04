async function testAllProps() {
  const filter = "OpenHouseStatus eq 'Active' and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge 2026-09-25 and OpenHouseDate le 2026-09-27";
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  const r = await fetch(url);
  const d = await r.json();
  const ohRec = d.value || [];
  const listingKeys = [...new Set(ohRec.map(r => r.ListingKey))];

  const PROP_SEL = 'ListingKey,ListingId,UnparsedAddress,City,PostalCode,ListPrice,PropertyType,PropertySubType,BedroomsTotal,BathroomsTotalInteger,LivingArea,LotSizeAcres,YearBuilt,StandardStatus,SubdivisionName,ListAgentFullName,ListAgentEmail,ListAgentDirectPhone,ListAgentKey,ListOfficeName,ListOfficePhone,PublicRemarks,Coordinates,Media';
  let properties = [];
  for (let i = 0; i < listingKeys.length; i += 25) {
    const chunk = listingKeys.slice(i, i + 25);
    const batchFilter = chunk.map(k => `ListingKey eq '${k}'`).join(' or ');
    const pURL = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/Property?$filter=${encodeURIComponent(batchFilter)}&$top=100&$select=${PROP_SEL}`;
    const pres = await fetch(pURL);
    const pd = await pres.json();
    properties.push(...(pd.value || []));
  }
  console.log(`Listing keys: ${listingKeys.length}, Properties found: ${properties.length}`);
  if (properties.length > 0) {
    console.log('Sample address:', properties[0].UnparsedAddress, properties[0].City, properties[0].ListPrice);
  }
}
testAllProps();
