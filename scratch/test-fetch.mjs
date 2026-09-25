import fs from 'fs';

async function main() {
  const res = await fetch('http://ursulaweinkauff.com/');
  const html = await res.text();
  const cssUrls = [...html.matchAll(/href=["']([^"']+\.css[^"']*)["']/g)].map(m => m[1]);
  console.log('CSS URLs:', cssUrls);

  for (const url of cssUrls) {
    const fullUrl = url.startsWith('http') ? url : 'http://ursulaweinkauff.com/' + url.replace(/^\//, '');
    try {
      const cRes = await fetch(fullUrl);
      const css = await cRes.text();
      if (css.includes('home-search') || css.includes('search-weather') || css.includes('idx-quick-search')) {
        console.log('Found in', url);
        const lines = css.split('\n');
        lines.forEach(l => {
          if (l.includes('home-search') || l.includes('search-weather')) {
            console.log('   ', l);
          }
        });
      }
    } catch (e) {
      console.error(e);
    }
  }
}
main();
