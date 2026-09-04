async function verifyLive() {
  const url = 'https://coconutcoastopenhouses.com/open-house/?t=' + Date.now();
  console.log('Fetching:', url);
  const r = await fetch(url, { headers: { 'Cache-Control': 'no-cache' } });
  const html = await r.text();
  console.log('Includes Sept 25th - 27th:', html.includes('Open House Weekend - Sept 25th - 27th'));
  console.log('Includes 2026-09-25:', html.includes('2026-09-25'));
  console.log('Includes September Open House Weekend:', html.includes('September Open House Weekend'));
  console.log('Still includes May 1st:', html.includes('May 1st'));
}
verifyLive();
