async function test() {
  const filter = "OpenHouseStatus eq 'Active' and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge 2026-09-25 and OpenHouseDate le 2026-09-27";
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  console.log('Fetching:', url);
  const r = await fetch(url);
  console.log('Status:', r.status);
  const d = await r.json();
  console.log('Value count:', d.value?.length);
  if (d.value && d.value.length > 0) {
    console.log('Sample:', d.value[0]);
  } else {
    console.log('Response body:', JSON.stringify(d).slice(0, 500));
  }
}
test();
