async function run() {
  const res = await fetch('http://ursulaweinkauff.com/quick-search');
  const html = await res.text();
  const scripts = [...html.matchAll(/<script[^>]*src=["']([^"']+)["'][^>]*>/gi)].map(m => m[1]);
  console.log('Scripts:', scripts);
  const iframes = [...html.matchAll(/<iframe[^>]*src=["']([^"']+)["'][^>]*>/gi)].map(m => m[1]);
  console.log('Iframes:', iframes);
  const sneakDivs = [...html.matchAll(/<div[^>]*id=["']sneak-[^"']+["'][^>]*>/gi)].map(m => m[0]);
  console.log('Sneak divs:', sneakDivs);
}
run();
