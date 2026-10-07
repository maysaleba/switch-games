#!/usr/bin/env node
// Records the lowest sale price seen per game key and country. Run after getPrices.js.
const fs = require('fs');
const path = require('path');

const PRICES_FILE = path.resolve('output/merged_enriched_with_prices.json');
const LOWEST_FILE = path.resolve('output/lowest_prices.json');

const load = (file, fallback) => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
};

const entries = load(PRICES_FILE, []);
const lowest = load(LOWEST_FILE, {});
const today = new Date().toISOString().slice(0, 10);

let added = 0, lowered = 0, currencyMismatch = 0;

for (const { key, prices } of entries) {
  for (const [country, p] of Object.entries(prices || {})) {
    const sale = Number(p.sale);
    if (p.sale == null || p.sale === '' || !Number.isFinite(sale)) continue;

    const currency = p.sale_currency || p.regular_currency;
    const prev = lowest[key]?.[country];

    if (prev && prev.currency !== currency) {
      currencyMismatch++;
      continue;
    }

    if (prev && sale >= prev.price) continue;

    lowest[key] = lowest[key] || {};
    lowest[key][country] = { price: sale, currency, date: today };
    prev ? lowered++ : added++;
  }
}

fs.writeFileSync(LOWEST_FILE, JSON.stringify(lowest, null, 2), 'utf8');
console.log(`lowest_prices: +${added} new, ${lowered} lowered, ${currencyMismatch} currency mismatches skipped`);
