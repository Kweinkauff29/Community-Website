import fs from 'fs';
import path from 'path';

const src = path.resolve('open-house/index.html');
const targets = [
  'cf-deploy/index.html',
  'cf-deploy/open-house/index.html',
  'cf-deploy/open-house/open-house/index.html'
];

for (const t of targets) {
  const dest = path.resolve(t);
  fs.copyFileSync(src, dest);
  console.log(`Copied ${src} -> ${dest}`);
}
