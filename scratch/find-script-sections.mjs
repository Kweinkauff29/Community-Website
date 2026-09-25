import { readFileSync } from 'node:fs';
const content = readFileSync('sneak-idx/search/index.html', 'utf8');
const lines = content.split('\n');
console.log('Total lines:', lines.length);

const keywords = ['<script', 'urlParams', 'URLSearchParams', 'fetchListings', 'renderCard', 'function renderListingCard', 'agentPhoto', 'openHouseBadge', 'function init'];
lines.forEach((line, idx) => {
  for (const kw of keywords) {
    if (line.includes(kw)) {
      console.log(`Line ${idx + 1} [${kw}]: ${line.trim().slice(0, 100)}`);
      break;
    }
  }
});
