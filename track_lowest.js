#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const PRICES_FILE = path.resolve('output/merged_enriched_with_prices.json');
const LOWEST_FILE = path.resolve('output/lowest_prices.json');
const STATUS_FILE = path.resolve('output/current_sale_status.json');
const CSVJSON_FILE = path.resolve('csvjson.json');
const FX_URL = 'https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json';

const load = (file, fallback) => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
};

function activeSaleFor(price, now) {
  const sale = Number(price?.sale);
  if (price?.sale == null || price.sale === '' || !Number.isFinite(sale)) return null;

  const start = price.sale_start || null;
  const end = price.sale_end || null;
  const startTime = start ? Date.parse(start) : null;
  const endTime = end ? Date.parse(end) : NaN;
  if (start && (!Number.isFinite(startTime) || startTime > now)) return null;
  if (!Number.isFinite(endTime) || endTime <= now) return null;

  return { sale, start, end };
}

async function fetchUsdRates() {
  const response = await fetch(FX_URL, { signal: AbortSignal.timeout(15000) });
  if (!response.ok) throw new Error(`Exchange-rate request failed: HTTP ${response.status}`);
  const data = await response.json();
  const rates = data?.usd;
  if (!rates || !Number.isFinite(Number(rates.php)) || Number(rates.php) <= 0) {
    throw new Error('Exchange-rate response has no valid USD→PHP rate');
  }
  return { rates, date: data.date || 'unknown' };
}

function makePhpConverter(rates) {
  const phpPerUsd = Number(rates.php);
  return (amount, currency) => {
    const unitsPerUsd = Number(rates[String(currency || '').toLowerCase()]);
    if (!Number.isFinite(amount) || !Number.isFinite(unitsPerUsd) || unitsPerUsd <= 0) return null;
    return (amount / unitsPerUsd) * phpPerUsd;
  };
}

function convertRankPriceToPhp(amount, currency, country, rates, toPhp) {
  if (country !== 'AR' || currency !== 'ARS') return toPhp(amount, currency);

  const ars = Number(amount);
  const phpPerARS = Number(rates.php) / Number(rates.ars);
  if (!Number.isFinite(ars) || ars <= 0 || !Number.isFinite(phpPerARS) || phpPerARS <= 0) return null;

  const basePhp = ars * phpPerARS;
  const regionalityTax = basePhp * 1.21 * ((1500 - 12100 * phpPerARS) / (12100 * phpPerARS));
  const argentinaVat = basePhp * 0.21;
  return basePhp + argentinaVat + regionalityTax;
}

function findGlobalCheapestPhp(lowest, entries, toPhp, rates, now) {
  const result = {};
  for (const [key, countries] of Object.entries(lowest)) {
    let minimum = Infinity;

    for (const [country, record] of Object.entries(countries || {})) {
      const converted = convertRankPriceToPhp(Number(record?.price), record?.currency, country, rates, toPhp);
      if (converted == null) continue;
      minimum = Math.min(minimum, converted);
    }

    if (Number.isFinite(minimum)) result[key] = minimum;
  }

  for (const { key, prices } of entries) {
    if (!key) continue;
    for (const [country, price] of Object.entries(prices || {})) {
      const activeSale = activeSaleFor(price, now);
      if (!activeSale) continue;
      const currency = price.sale_currency || price.regular_currency;
      const converted = convertRankPriceToPhp(activeSale.sale, currency, country, rates, toPhp);
      if (converted == null) continue;
      result[key] = Math.min(result[key] ?? Infinity, converted);
    }
  }

  return result;
}

async function main() {
  const entries = load(PRICES_FILE, []);
  const lowest = load(LOWEST_FILE, {});
  const csvRows = load(CSVJSON_FILE, []);
  const today = new Date().toISOString().slice(0, 10);
  const now = Date.now();

  if (!Array.isArray(entries) || !Array.isArray(csvRows) || !lowest || Array.isArray(lowest) || typeof lowest !== 'object') {
    throw new Error('Expected price-cache and csvjson arrays, plus a lowest-price object');
  }

  const rateData = await fetchUsdRates();
  const rates = rateData.rates;
  const toPhp = makePhpConverter(rates);
  const globalCheapestPhp = findGlobalCheapestPhp(lowest, entries, toPhp, rates, now);
  const csvSlugs = new Set(csvRows.map(row => row.Slug).filter(Boolean));
  const observedSales = new Set();
  let added = 0, lowered = 0, matched = 0, aboveLow = 0, unknown = 0, currencyMismatch = 0;

  for (const { key, prices } of entries) {
    if (!key) continue;

    for (const [country, price] of Object.entries(prices || {})) {
      const sale = Number(price?.sale);
      if (price?.sale == null || price.sale === '' || !Number.isFinite(sale)) continue;

      const currency = price.sale_currency || price.regular_currency;
      const activeSale = activeSaleFor(price, now);
      lowest[key] = lowest[key] || {};
      let record = lowest[key][country];
      const hadComparableLow = Boolean(record && record.currency === currency && Number.isFinite(Number(record.price)));
      const sameSale = activeSale && record?.currentSale &&
        record.currentSale.saleStart === activeSale.start &&
        record.currentSale.saleEnd === activeSale.end &&
        Number(record.currentSale.price) === activeSale.sale &&
        record.currentSale.currency === currency;
      const historicReferencePrice = sameSale && record.currentSale.statusRule === 'global_php_v5'
        ? record.currentSale.historicPrice
        : hadComparableLow ? Number(record.price) : null;
      let status = null;

      if (!record) {
        record = lowest[key][country] = { price: sale, currency, date: today };
        added++;
      }

      if (activeSale) {
        const salePhp = convertRankPriceToPhp(sale, currency, country, rates, toPhp);
        const cheapestPhp = globalCheapestPhp[key];
        if (salePhp == null || cheapestPhp == null) {
          status = 'unknown';
          unknown++;
        } else if (salePhp > cheapestPhp + 0.01) {
          status = 'not_low';
          aboveLow++;
        } else if (record.currency !== currency || historicReferencePrice == null) {
          status = 'unknown';
          if (record.currency !== currency) currencyMismatch++;
          else unknown++;
        } else if (sale < Number(historicReferencePrice)) {
          status = 'new_low';
          lowered++;
        } else if (sale === Number(historicReferencePrice)) {
          status = 'matches_low';
          matched++;
        } else {
          status = 'not_low';
          aboveLow++;
        }
      }

      if (activeSale && record.currency === currency && sale < Number(record.price)) {
        record.price = sale;
        record.currency = currency;
        record.date = today;
      }

      if (!activeSale) continue;

      record.currentSale = {
        price: activeSale.sale,
        currency,
        status: status || 'unknown',
        statusRule: 'global_php_v5',
        historicPrice: historicReferencePrice,
        saleStart: activeSale.start,
        saleEnd: activeSale.end
      };
      observedSales.add(`${key}\0${country}`);
    }
  }

  for (const [key, countries] of Object.entries(lowest)) {
    for (const [country, record] of Object.entries(countries || {})) {
      if (record?.currentSale && !observedSales.has(`${key}\0${country}`)) delete record.currentSale;
    }
  }

  const statusFeed = Object.fromEntries([...csvSlugs].map(slug => [slug, {}]));
  for (const [key, countries] of Object.entries(lowest)) {
    if (!csvSlugs.has(key)) continue;
    for (const [country, record] of Object.entries(countries || {})) {
      if (record?.currentSale?.status) {
        statusFeed[key][country] = record.currentSale.status;
      }
    }
  }

  fs.writeFileSync(LOWEST_FILE, JSON.stringify(lowest, null, 2), 'utf8');
  fs.writeFileSync(STATUS_FILE, JSON.stringify(statusFeed), 'utf8');
  console.log(`lowest_prices: +${added} new, ${lowered} global new lows, ${currencyMismatch} currency mismatches skipped`);
  console.log(`Sale comparison: ${lowered} new lows, ${matched} matches, ${aboveLow} not lows, ${unknown} unknown`);
  console.log(`Global minimum countries determined using USD→PHP rates dated ${rateData.date}`);
  console.log(`Wrote ${Object.keys(statusFeed).length} games to ${STATUS_FILE}`);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
