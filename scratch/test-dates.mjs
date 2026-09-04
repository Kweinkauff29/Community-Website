async function checkDates() {
  const filter = "OpenHouseStatus eq 'Active' and OriginatingSystemName eq 'Bonita Springs' and OpenHouseDate ge 2026-09-25 and OpenHouseDate le 2026-09-27";
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  const r = await fetch(url);
  const d = await r.json();
  const counts = {};
  (d.value || []).forEach(oh => {
    const dt = (oh.OpenHouseDate || oh.OpenHouseStartTime || '').slice(0, 10);
    counts[dt] = (counts[dt] || 0) + 1;
  });
  console.log('Date breakdown:', counts);
}
checkDates();
