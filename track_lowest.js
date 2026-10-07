#!/usr/bin/env node
// Records the lowest sale price seen per game key and country. Run after getPrices.js.
const fs = require('fs');
const path = require('path');

const PRICES_FILE = path.resolve('output/merged_enriched_with_prices.json');
const LOWEST_FILE = path.resolve('output/lowest_prices.json');
const STATUS_FILE = path.resolve('output/current_sale_status.json');
const CSVJSON_FILE = path.resolve('csvjson.json');

const load = (file, fallback) => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
};

const entries = load(PRICES_FILE, []);
const lowest = load(LOWEST_FILE, {});
const csvRows = load(CSVJSON_FILE, []);
const today = new Date().toISOString().slice(0, 10);
const now = Date.now();

if (!Array.isArray(entries) || !Array.isArray(csvRows) || !lowest || Array.isArray(lowest) || typeof lowest !== 'object') {
  throw new Error('Expected price-cache and csvjson arrays, plus a lowest-price object');
}

let added = 0, lowered = 0, matched = 0, aboveLow = 0, unknown = 0, currencyMismatch = 0;
const observedSales = new Set();
const csvSlugs = new Set(csvRows.map(row => row.Slug).filter(Boolean));

for (const { key, prices } of entries) {
  if (!key) continue;

  for (const [country, p] of Object.entries(prices || {})) {
    const sale = Number(p.sale);
    if (p.sale == null || p.sale === '' || !Number.isFinite(sale)) continue;

    const currency = p.sale_currency || p.regular_currency;
    const saleStart = p.sale_start || null;
    const saleEnd = p.sale_end || null;
    const saleStartTime = saleStart ? Date.parse(saleStart) : null;
    const saleEndTime = saleEnd ? Date.parse(saleEnd) : NaN;
    if (saleStart && (!Number.isFinite(saleStartTime) || saleStartTime > now)) continue;
    const activeSale = Number.isFinite(saleEndTime) && saleEndTime > now;

    lowest[key] = lowest[key] || {};
    const prev = lowest[key][country];
    const saleKey = `${key}\0${country}`;
    const sameSale = activeSale && prev?.currentSale &&
      (saleStart || saleEnd) &&
      prev.currentSale.saleStart === saleStart &&
      prev.currentSale.saleEnd === saleEnd &&
      prev.currentSale.price === sale &&
      prev.currentSale.currency === currency;

    let status = null;

    if (!prev) {
      added++;
      lowest[key][country] = { price: sale, currency, date: today };
      if (activeSale) {
        status = 'unknown';
        unknown++;
      }
    } else if (prev.currency !== currency) {
      if (activeSale) {
        currencyMismatch++;
        status = 'unknown';
      }
    } else if (sameSale) {
      status = prev.currentSale.status;
    } else if (sale < prev.price) {
      if (activeSale) {
        status = 'new_low';
        lowered++;
      }
      prev.price = sale;
      prev.currency = currency;
      prev.date = today;
    } else if (activeSale && sale === prev.price) {
      status = 'matches_low';
      matched++;
    } else if (activeSale) {
      status = 'not_low';
      aboveLow++;
    }

    if (!activeSale) continue;

    const countryRecord = lowest[key][country];
    countryRecord.currentSale = {
      price: sale,
      currency,
      status,
      saleStart,
      saleEnd,
      observedAt: today
    };
    observedSales.add(saleKey);
  }
}

for (const [key, record] of Object.entries(lowest)) {
  for (const [country, countryRecord] of Object.entries(record || {})) {
    if (countryRecord?.currentSale && !observedSales.has(`${key}\0${country}`)) {
      delete countryRecord.currentSale;
    }
  }
}

const statusFeed = Object.fromEntries(
  [...csvSlugs].map(slug => [slug, {}])
);
for (const [key, record] of Object.entries(lowest)) {
  if (!csvSlugs.has(key)) continue;

  for (const [country, countryRecord] of Object.entries(record || {})) {
    const status = countryRecord?.currentSale?.status;
    if (!status) continue;
    statusFeed[key] = statusFeed[key] || {};
    statusFeed[key][country] = status;
  }
}

fs.writeFileSync(LOWEST_FILE, JSON.stringify(lowest, null, 2), 'utf8');
fs.writeFileSync(STATUS_FILE, JSON.stringify(statusFeed), 'utf8');
console.log(`lowest_prices: +${added} new, ${lowered} lowered, ${currencyMismatch} currency mismatches skipped`);
console.log(`Sale comparison: ${lowered} new lows, ${matched} matches, ${aboveLow} not lows, ${unknown} unknown`);
console.log(`Wrote ${Object.keys(statusFeed).length} games to ${STATUS_FILE}`);
