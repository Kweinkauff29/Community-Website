async function checkDates() {
  const filter = "(OpenHouseStatus eq 'Active' or OpenHouseStatus eq null) and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge 2026-10-30 and OpenHouseDate le 2026-11-01";
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  console.log('Fetching:', url);
  const r = await fetch(url);
  const d = await r.json();
  const counts = {};
  (d.value || []).forEach(oh => {
    const dt = (oh.OpenHouseDate || oh.OpenHouseStartTime || '').slice(0, 10);
    counts[dt] = (counts[dt] || 0) + 1;
  });
  console.log('Result total:', (d.value || []).length, 'Date breakdown:', counts);
  if (d.value && d.value[0]) console.log('Sample:', d.value[0]);
}
checkDates();
