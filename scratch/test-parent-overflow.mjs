async function run() {
  const r = await fetch('http://ursulaweinkauff.com/');
  const html = await r.text();
  const cssMatches = [...html.matchAll(/href=["']([^"']+\.css[^"']*)["']/g)].map(m => m[1]);
  for (const c of cssMatches) {
    const url = c.startsWith('http') ? c : 'http://ursulaweinkauff.com/' + c.replace(/^\//, '');
    const res = await fetch(url);
    const css = await res.text();
    // Search for any overflow rules
    const matches = css.match(/[^{}]*\{[^}]*overflow[^}]*\}/gi) || [];
    matches.forEach(m => {
      if (m.includes('search') || m.includes('weather') || m.includes('hero') || m.includes('idx') || m.includes('site-width')) {
        console.log('Match in', c, ':\n', m.trim());
      }
    });
  }
}
run();
