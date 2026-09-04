async function checkOHSystems() {
  const filter = "OpenHouseStatus eq 'Active' and OpenHouseDate ge 2026-09-25 and OpenHouseDate le 2026-09-27";
  const url = `https://openhouseworker.bonitaspringsrealtors.workers.dev/api/v2/OData/bsaor/OpenHouse?$filter=${encodeURIComponent(filter)}&$top=200&$orderby=OpenHouseStartTime asc`;
  const r = await fetch(url);
  const d = await r.json();
  const systems = new Map();
  (d.value || []).forEach(oh => {
    systems.set(oh.OriginatingSystemName, (systems.get(oh.OriginatingSystemName) || 0) + 1);
  });
  console.log('OpenHouse systems:', Object.fromEntries(systems));
}
checkOHSystems();
