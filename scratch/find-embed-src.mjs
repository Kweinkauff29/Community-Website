async function run() {
  const res = await fetch('http://ursulaweinkauff.com/');
  const html = await res.text();
  const matches = [...html.matchAll(/(https?:\/\/[^"'\s]+\/embed\.js)/g)].map(m => m[1]);
  console.log('embed.js full URLs found:', matches);
  if (!matches.length) {
    const scripts = [...html.matchAll(/<script[^>]*src=["']([^"']+)["']/gi)].map(m => m[1]);
    for (const s of scripts) {
      const sUrl = s.startsWith('http') ? s : 'http://ursulaweinkauff.com/' + s.replace(/^\//, '');
      const sRes = await fetch(sUrl);
      const text = await sRes.text();
      if (text.includes('embed.js') || text.includes('sneak-idx-search-bar')) {
        console.log('Found in script:', sUrl);
        const hit = text.match(/https?:\/\/[^"'\s]+\/embed\.js/);
        if (hit) console.log('   hit:', hit[0]);
      }
    }
  }
}
run();
