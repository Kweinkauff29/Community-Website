async function run() {
  const r = await fetch('http://ursulaweinkauff.com/');
  const html = await r.text();
  const allUrls = html.match(/https?:\/\/[^\s"'<>]+\.js[^\s"'<>]*/g) || [];
  console.log('All JS URLs in HTML:');
  allUrls.forEach(u => console.log('  ', u));
}
run();
