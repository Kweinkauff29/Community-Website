async function testProps() {
  const filter = "OpenHouseStatus eq 'Active' and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge 2026-09-25 and OpenHouseDate le 2026-09-27";
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  const r = await fetch(url);
  const d = await r.json();
  const ohRec = d.value || [];
  console.log('Open houses found:', ohRec.length);
  const listingKeys = [...new Set(ohRec.map(r => r.ListingKey))];
  console.log('Unique listing keys:', listingKeys.length);

  const chunk = listingKeys.slice(0, 25);
  const batchFilter = chunk.map(k => `ListingKey eq '${k}'`).join(' or ');
  const PROP_SEL = 'ListingKey,ListingId,UnparsedAddress,City,PostalCode,ListPrice,PropertyType,PropertySubType,BedroomsTotal,BathroomsTotalInteger,LivingArea,LotSizeAcres,YearBuilt,StandardStatus,SubdivisionName,ListAgentFullName,ListAgentEmail,ListAgentDirectPhone,ListAgentKey,ListOfficeName,ListOfficePhone,PublicRemarks,Coordinates,Media';
  const pURL = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/Property?$filter=${encodeURIComponent(`(${batchFilter}) and OriginatingSystemName eq 'Bonita Springs'`)}&$top=100&$select=${PROP_SEL}`;
  const pres = await fetch(pURL);
  console.log('Property status:', pres.status);
  const pd = await pres.json();
  console.log('Properties returned for chunk of 25:', pd.value?.length);
  if (pd.value && pd.value[0]) {
    console.log('Sample property:', pd.value[0].UnparsedAddress, pd.value[0].City, pd.value[0].ListPrice);
  }
}
testProps();
