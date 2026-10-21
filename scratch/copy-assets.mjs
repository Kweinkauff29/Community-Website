import fs from 'fs';
import path from 'path';

const srcDir = path.resolve('scratch/wwmr_art');
const targetDirs = [
  'open-house/assets',
  'cf-deploy/assets',
  'cf-deploy/open-house/assets',
  'cf-deploy/open-house/open-house/assets'
];

const fileMap = {
  'graphic1_re_pic.jpg': 'wwmr-re-pic.jpg',
  'graphic2_re.jpg': 'wwmr-re-banner.jpg',
  'logo1_full_cmyk.jpg': 'wwmr-logo-full.jpg',
  'logo2_letters_only.jpg': 'wwmr-logo-letters.jpg',
};

for (const tDir of targetDirs) {
  const fullTDir = path.resolve(tDir);
  if (!fs.existsSync(fullTDir)) {
    fs.mkdirSync(fullTDir, { recursive: true });
  }
  for (const [srcName, destName] of Object.entries(fileMap)) {
    const srcPath = path.join(srcDir, srcName);
    const destPath = path.join(fullTDir, destName);
    if (fs.existsSync(srcPath)) {
      fs.copyFileSync(srcPath, destPath);
      console.log(`Copied ${srcName} -> ${destPath}`);
    } else {
      console.error(`Source missing: ${srcPath}`);
    }
  }
}
