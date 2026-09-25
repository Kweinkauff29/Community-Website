async function run() {
  const res = await fetch('http://ursulaweinkauff.com/');
  const html = await res.text();
  const scriptTags = [...html.matchAll(/<script[\s\S]*?<\/script>/gi)].map(m => m[0]);
  console.log('Script tags count:', scriptTags.length);
  scriptTags.forEach((s, i) => {
    console.log(`Script ${i}:`, s.substring(0, 150));
    if (s.includes('sneak') || s.includes('embed') || s.includes('idx')) {
      console.log('   FULL MATCH:', s);
    }
  });
}
run();
