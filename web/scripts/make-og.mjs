// Generates web/public/og.png (1200x630) — the social/Open Graph card.
// Run from web/: node scripts/make-og.mjs
import sharp from 'sharp';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const out = join(here, '..', 'public', 'og.png');

const svg = `<svg width="1200" height="630" viewBox="0 0 1200 630" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#0b1016"/>
      <stop offset="1" stop-color="#131c27"/>
    </linearGradient>
    <radialGradient id="glow" cx="0.8" cy="0.15" r="0.6">
      <stop offset="0" stop-color="#336791" stop-opacity="0.35"/>
      <stop offset="1" stop-color="#336791" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <rect width="1200" height="630" fill="url(#bg)"/>
  <rect width="1200" height="630" fill="url(#glow)"/>

  <!-- three-bar rotation mark -->
  <g transform="translate(96,150)">
    <rect x="0" y="0"  width="150" height="26" rx="13" fill="#5b9bd5"/>
    <rect x="0" y="42" width="150" height="26" rx="13" fill="#336791"/>
    <rect x="0" y="84" width="150" height="26" rx="13" fill="#f0883e"/>
  </g>

  <text x="96" y="360" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="132" font-weight="800" fill="#e6edf3" letter-spacing="-4">PgQue</text>

  <text x="100" y="438" font-family="-apple-system, Segoe UI, Roboto, sans-serif" font-size="44" font-weight="700" fill="#ffffff">Zero-bloat Postgres queue.</text>
  <text x="100" y="498" font-family="-apple-system, Segoe UI, Roboto, sans-serif" font-size="38" font-weight="400" fill="#9aa7b4">One SQL file to install. PgQ, universal edition.</text>

  <text x="100" y="566" font-family="ui-monospace, monospace" font-size="30" font-weight="600" fill="#5b9bd5">pgque.dev</text>
</svg>`;

await sharp(Buffer.from(svg)).png().toFile(out);
console.log('wrote', out);
