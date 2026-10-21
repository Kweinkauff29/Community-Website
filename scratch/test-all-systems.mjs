async function testAllSystems() {
  const EVENT_START = '2026-10-30';
  const EVENT_END = '2026-11-01';
  // Check without OriginatingSystemName filter
  const filter = `(OpenHouseStatus eq 'Active' or OpenHouseStatus eq null) and OpenHouseDate ge ${EVENT_START} and OpenHouseDate le ${EVENT_END}`;
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  
  const res = await fetch(url);
  const d = await res.json();
  const ohRec = d.value || [];
  console.log(`Total OH without OriginatingSystemName filter: ${ohRec.length}`);

  const origCounts = {};
  ohRec.forEach(oh => {
    const o = oh.OriginatingSystemName || 'null';
    origCounts[o] = (origCounts[o] || 0) + 1;
  });
  console.log('OriginatingSystemName breakdown:', origCounts);
}

testAllSystems();
