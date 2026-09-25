async function run() {
  const r = await fetch('http://ursulaweinkauff.com/');
  const t = await r.text();
  const cssMatches = [...t.matchAll(/href=["']([^"']+\.css[^"']*)["']/g)].map(m => m[1]);
  for (const c of cssMatches) {
    const url = c.startsWith('http') ? c : 'http://ursulaweinkauff.com/' + c.replace(/^\//, '');
    const res = await fetch(url);
    const css = await res.text();
    const rules = css.match(/[^{}]*(?:idx-quick-search-shell|home-search|search-weather)[^{}]*\{[^}]*\}/g) || [];
    if (rules.length) {
      console.log('--- From', c);
      rules.forEach(rule => console.log('  ', rule.trim()));
    }
  }
}
run();
