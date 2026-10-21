import fs from 'fs';
import path from 'path';

const urls = [
  { name: 'graphic1_re_pic', url: 'https://coconutcoastrealtors.org/wp-content/uploads/2026/10/Woods-Weidenmiller-Michetti-Rudnick-RE-pic_page-0001-scaled-e1790867979596.jpg' },
  { name: 'graphic2_re', url: 'https://coconutcoastrealtors.org/wp-content/uploads/2026/10/Woods-Weidenmiller-Michetti-Rudnick-RE_page-0001-scaled-e1790868008376.jpg' },
  { name: 'logo1_full_cmyk', url: 'https://coconutcoastrealtors.org/wp-content/uploads/2026/10/WWMR-Logo-Full-CMYK-scaled.jpg' },
  { name: 'logo2_letters_only', url: 'https://coconutcoastrealtors.org/wp-content/uploads/2026/10/WWMR-Letters-Only-Logo-JPG.jpg' },
];

function getImageDimensions(buffer) {
  // Simple JPEG / PNG dimension reader
  if (buffer[0] === 0xff && buffer[1] === 0xd8) {
    let offset = 2;
    while (offset < buffer.length) {
      if (buffer[offset] !== 0xff) break;
      const marker = buffer[offset + 1];
      if (marker === 0xc0 || marker === 0xc2) {
        const height = buffer.readUInt16BE(offset + 5);
        const width = buffer.readUInt16BE(offset + 7);
        return { width, height, type: 'jpeg' };
      }
      const length = buffer.readUInt16BE(offset + 2);
      offset += 2 + length;
    }
  } else if (buffer[0] === 0x89 && buffer.toString('ascii', 1, 4) === 'PNG') {
    const width = buffer.readUInt32BE(16);
    const height = buffer.readUInt32BE(20);
    return { width, height, type: 'png' };
  }
  return { width: null, height: null, type: 'unknown' };
}

async function main() {
  const outDir = path.resolve('scratch/wwmr_art');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  for (const item of urls) {
    console.log(`Fetching ${item.name} from ${item.url}...`);
    try {
      const res = await fetch(item.url);
      if (!res.ok) {
        console.error(`Failed ${item.name}: ${res.status} ${res.statusText}`);
        continue;
      }
      const buf = Buffer.from(await res.arrayBuffer());
      const ext = item.url.endsWith('.png') ? '.png' : '.jpg';
      const outPath = path.join(outDir, item.name + ext);
      fs.writeFileSync(outPath, buf);
      const dims = getImageDimensions(buf);
      console.log(`Saved: ${outPath} | Size: ${buf.length} bytes | Dims: ${dims.width}x${dims.height} (${dims.type})`);
    } catch (e) {
      console.error(`Error ${item.name}:`, e);
    }
  }
}

main();
