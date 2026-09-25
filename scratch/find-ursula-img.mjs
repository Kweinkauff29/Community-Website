async function main() {
  try {
    const res = await fetch('https://www.focus-realestate.com/about-ursula-her-team', { redirect: 'follow', headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } });
    const html = await res.text();
    const urls = [...html.matchAll(/https?:\/\/[^\s"'<>]+\.(?:jpg|jpeg|png|webp)/gi)].map(m => m[0]);
    console.log('About page images:', [...new Set(urls)]);
  } catch (err) {
    console.error('Error:', err.message);
  }
}
main();
