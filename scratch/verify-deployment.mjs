async function verify() {
  const urls = [
    'https://acd6ef34.coconut-coast-openhouses.pages.dev/open-house/',
    'https://coconutcoastopenhouses.com/open-house/?t=' + Date.now(),
    'https://coconut-coast-openhouses.pages.dev/open-house/?t=' + Date.now()
  ];

  for (const u of urls) {
    console.log(`\n--- Checking: ${u} ---`);
    try {
      const res = await fetch(u, { headers: { 'Cache-Control': 'no-cache' } });
      console.log('Status:', res.status);
      const html = await res.text();
      console.log('Includes "Oct 30th - Nov 1st":', html.includes('Oct 30th - Nov 1st'));
      console.log('Includes "2026-10-30":', html.includes('2026-10-30'));
      console.log('Includes "Woods, Weidenmiller, Michetti & Rudnick":', html.includes('Woods, Weidenmiller, Michetti & Rudnick'));
      console.log('Includes "lawfirmnaples.com":', html.includes('lawfirmnaples.com'));
      console.log('Includes "239-325.4070":', html.includes('239-325.4070'));
      console.log('Has old "Sept 25th":', html.includes('Sept 25th'));
      console.log('Has old "Chapman":', html.includes('Chapman'));
      console.log('Has old "cigflorida":', html.includes('cigflorida'));
    } catch (e) {
      console.error('Error fetching', u, e);
    }
  }
}

verify();
