#!/usr/bin/env node
/**
 * Pre-price the freshly fetched Philippines catalog before regional merge.
 * Saves PH price snapshots to the PH master and keeps only discounted items
 * in ph_games_enriched_current.json for merge_enriched.js.
 */
const fs = require('fs');
const path = require('path');

const API = 'https://api.ec.nintendo.com/v1/price';
const DATA_DIR = path.join(__dirname, 'data');
const MASTER = path.join(DATA_DIR, 'ph_games_enriched.json');
const CURRENT = path.join(DATA_DIR, 'ph_games_enriched_current.json');
const LIMIT = 50;
const COUNTRY = 'PH';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function formatPriceRow(row) {
  const regular = row?.regular_price || {};
  const discount = row?.discount_price || {};
  const raw = value => value == null ? null : value;

  return {
    regular: raw(regular.raw_value),
    regular_currency: regular.currency || null,
    sale: raw(discount.raw_value),
    sale_currency: discount.currency || null,
    sale_start: discount.start_datetime || null,
    sale_end: discount.end_datetime || null,
  };
}

function isCurrentlyDiscounted(price, now = new Date()) {
  if (!price || price.sale == null || price.sale === '' || price.regular == null || price.regular === '') {
    return false;
  }

  const regular = Number(price.regular);
  const sale = Number(price.sale);
  if (!Number.isFinite(regular) || !Number.isFinite(sale) || sale >= regular) return false;

  const start = price.sale_start ? new Date(price.sale_start) : null;
  const end = price.sale_end ? new Date(price.sale_end) : null;
  if (start && !Number.isNaN(start.getTime()) && start > now) return false;
  if (end && !Number.isNaN(end.getTime()) && end <= now) return false;

  return true;
}

function loadArray(file) {
  const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (!Array.isArray(parsed)) throw new Error(`Expected an array in ${file}`);
  return parsed;
}

async function fetchChunk(ids, attempt = 0) {
  const params = new URLSearchParams({ country: COUNTRY, limit: String(LIMIT), lang: 'en' });
  for (const id of ids) params.append('ids', id);

  try {
    const response = await fetch(`${API}?${params}`);
    if (!response.ok) throw new Error(`Price API returned ${response.status}: ${await response.text()}`);
    const body = await response.json();
    if (!Array.isArray(body?.prices)) throw new Error('Unexpected price API response: prices is not an array');
    return body.prices;
  } catch (error) {
    if (attempt >= 3) throw error;
    await sleep(800 * (2 ** attempt));
    return fetchChunk(ids, attempt + 1);
  }
}

async function priceCatalog() {
  if (!fs.existsSync(CURRENT)) throw new Error(`Missing PH catalog snapshot: ${CURRENT}`);
  const catalog = loadArray(CURRENT);
  const ids = [...new Set(catalog.map(item => String(item?.nsuid_as || '').trim()).filter(Boolean))];
  const pricesById = new Map();

  for (let offset = 0; offset < ids.length; offset += LIMIT) {
    const chunk = ids.slice(offset, offset + LIMIT);
    const rows = await fetchChunk(chunk);
    for (const row of rows) {
      if (row?.title_id != null) pricesById.set(String(row.title_id), formatPriceRow(row));
    }
    console.log(`PH price check: ${Math.min(offset + chunk.length, ids.length)}/${ids.length} IDs`);
    if (offset + LIMIT < ids.length) await sleep(150);
  }

  const now = new Date();
  const checkedAt = now.toISOString();
  const checkedCatalog = catalog.map(item => {
    const id = String(item.nsuid_as);
    const price = pricesById.get(id) || formatPriceRow(null);
    const onSale = isCurrentlyDiscounted(price, now);
    return {
      ...item,
      prices: { ...(item.prices || {}), PH: price },
      ph_price_checked_at: checkedAt,
      ph_on_sale: onSale,
      active_in_base: onSale,
    };
  });

  const priorMaster = loadArray(MASTER);
  const checkedById = new Map(checkedCatalog.map(item => [String(item.nsuid_as), item]));
  const master = priorMaster.map(item => {
    const checked = checkedById.get(String(item?.nsuid_as || ''));
    return checked ? { ...item, ...checked } : item;
  });
  const knownIds = new Set(master.map(item => String(item?.nsuid_as || '')));
  for (const item of checkedCatalog) {
    if (!knownIds.has(String(item.nsuid_as))) master.push(item);
  }

  const onSale = checkedCatalog.filter(item => item.ph_on_sale);
  fs.writeFileSync(MASTER, JSON.stringify(master, null, 2), 'utf8');
  // This is the merger's PH input: only positively discounted listings.
  fs.writeFileSync(CURRENT, JSON.stringify(onSale, null, 2), 'utf8');
  console.log(`PH checked ${ids.length} IDs; ${onSale.length} currently discounted; ${ids.length - onSale.length} not on sale/price unavailable.`);
}

if (require.main === module) {
  priceCatalog().catch(error => {
    console.error(`PH price check failed: ${error.message || error}`);
    process.exitCode = 1;
  });
}

module.exports = { formatPriceRow, isCurrentlyDiscounted, priceCatalog };
