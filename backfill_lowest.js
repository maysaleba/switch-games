#!/usr/bin/env node
// One-off: replays git history of the prices file (oldest first) to seed lowest_prices.json.
const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const REL_PATH = 'output/merged_enriched_with_prices.json';
const LOWEST_FILE = path.resolve('output/lowest_prices.json');

const git = (...args) =>
  execFileSync('git', args, { encoding: 'utf8', maxBuffer: 512 * 1024 * 1024 });

const commits = git('log', '--reverse', '--format=%H %cs', '--', REL_PATH)
  .split('\n')
  .filter(Boolean)
  .map(line => line.split(' '));

const lowest = {};
let skipped = 0;

commits.forEach(([hash, date], i) => {
  let entries;
  try {
    entries = JSON.parse(git('show', `${hash}:${REL_PATH}`));
  } catch {
    skipped++;
    return;
  }

  for (const { key, prices } of entries) {
    for (const [country, p] of Object.entries(prices || {})) {
      const sale = Number(p.sale);
      if (p.sale == null || p.sale === '' || !Number.isFinite(sale)) continue;

      const currency = p.sale_currency || p.regular_currency;
      const prev = lowest[key]?.[country];
      if (prev && (prev.currency !== currency || sale >= prev.price)) continue;

      lowest[key] = lowest[key] || {};
      lowest[key][country] = { price: sale, currency, date };
    }
  }

  console.log(`[${i + 1}/${commits.length}] ${hash.slice(0, 7)} ${date}`);
});

fs.writeFileSync(LOWEST_FILE, JSON.stringify(lowest, null, 2), 'utf8');

const games = Object.keys(lowest).length;
const records = Object.values(lowest).reduce((n, c) => n + Object.keys(c).length, 0);
console.log(`Done: ${games} games, ${records} records (${skipped} commits unreadable)`);
