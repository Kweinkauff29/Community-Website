async function testProps2() {
  const filter = "OpenHouseStatus eq 'Active' and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge 2026-09-25 and OpenHouseDate le 2026-09-27";
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=5&$orderby=OpenHouseStartTime asc`;
  const r = await fetch(url);
  const d = await r.json();
  const oh = d.value[0];
  console.log('ListingKey:', oh.ListingKey);
  
  // Try querying Property by ListingKey without any other filter
  const pURL1 = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/Property?$filter=ListingKey eq '${oh.ListingKey}'`;
  const pres1 = await fetch(pURL1);
  const pd1 = await pres1.json();
  console.log('Query without OriginatingSystemName:', pd1.value?.length);
  if (pd1.value && pd1.value[0]) {
    console.log('OriginatingSystemName in Property:', pd1.value[0].OriginatingSystemName);
    console.log('OriginatingSystemKey in Property:', pd1.value[0].OriginatingSystemKey);
    console.log('Address:', pd1.value[0].UnparsedAddress);
  }
}
testProps2();
