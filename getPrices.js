#!/usr/bin/env node
/**
 * Inline merged_enriched → prices enricher (with progress + safe concurrency)
 * - Reads:  ./merged_enriched.json
 * - Writes: ./merged_enriched_with_prices.json
 *
 * Logic:
 *   For entries where active_in_base === true, fetch prices for region sets based on available nsuid_* fields.
 *   Region sets:
 *     US: ['US','MX','BR','CA','CO','AR','PE']   (requires nsuid_us)
 *     EU: ['ZA','AU','NZ','NO','PL']             (requires nsuid_eu)
 *     JP: ['JP']                                 (requires nsuid_jp)
 *     KR: ['KR']                                 (requires nsuid_kr)
 *     HK: ['HK']                                 (requires nsuid_hk)
 *
 * Endpoint: https://api.ec.nintendo.com/v1/price?country=XX&ids=...&limit=50&lang=en
 */

const fs = require('fs');
const path = require('path');

// ---------- config ----------
const INPUT_FILE  = path.resolve('output/merged_enriched.json');
const OUTPUT_FILE = path.resolve('output/merged_enriched_with_prices.json');

const PRICE_GET_URL     = 'https://api.ec.nintendo.com/v1/price';
const PRICE_LIST_LIMIT  = 50;   // API page size
const PRICE_GET_LANG    = 'en';

// Concurrency settings
const COUNTRY_POOL_SIZE = 3;    // safe starting point: 2–4
const BACKOFF_BASE_MS   = 800;  // base for exponential backoff (adds jitter)

// You provided these region sets:
const regionSets = {
  US: ['US', 'MX', 'BR', 'CA', 'CO', 'AR', 'PE'],
  EU: ['ZA', 'AU', 'NZ', 'NO', 'PL'],
  JP: ['JP'],
  KR: ['KR'],
  HK: ['HK'],
};

// ---------- helpers ----------
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

/** Simple promise pool for running tasks with limited concurrency */
function runWithPool(tasks, { concurrency = 3 } = {}) {
  let i = 0, active = 0, done = 0;
  const total = tasks.length;
  return new Promise((resolve, reject) => {
    const next = () => {
      if (i >= total && active === 0) return resolve();
      while (active < concurrency && i < total) {
        const idx = i++;
        const startNow = Date.now();
        active++;
        Promise.resolve()
          .then(tasks[idx])
          .then(() => {
            done++;
            const elapsed = Math.round((Date.now() - startNow) / 100) / 10;
            console.log(`   • task ${done}/${total} finished in ${elapsed}s`);
          })
          .catch(reject)
          .finally(() => { active--; next(); });
      }
    };
    next();
  });
}

// Global backoff gate — slows all requests briefly after a 429
let globalBackoffUntil = 0;
async function globalBackoffGate() {
  const now = Date.now();
  if (now < globalBackoffUntil) {
    const wait = globalBackoffUntil - now;
    console.log(`⏳ Global backoff ${wait}ms`);
    await sleep(wait);
  }
}
function setGlobalBackoff(ms) {
  globalBackoffUntil = Math.max(globalBackoffUntil, Date.now() + ms);
}

/** One price page (≤50 ids), with retries/backoff and Retry-After handling */
async function fetchPricesPageWithRetry(country, idsChunk, { retries = 3, backoffBase = BACKOFF_BASE_MS } = {}) {
  const params = new URLSearchParams();
  params.set('country', country);
  params.set('limit', String(PRICE_LIST_LIMIT));
  params.set('lang', PRICE_GET_LANG);
  idsChunk.forEach(id => params.append('ids', id));

  for (let attempt = 0; attempt <= retries; attempt++) {
    await globalBackoffGate();
    try {
      const url = `${PRICE_GET_URL}?${params.toString()}`;
      const res = await fetch(url);
      if (!res.ok) {
        const bodyText = await res.text().catch(() => '');
        // Handle 429 with Retry-After
        if (res.status === 429) {
          let waitMs = 0;
          const retryAfter = res.headers.get('retry-after');
          if (retryAfter) {
            // seconds or HTTP-date; assume seconds when numeric
            const n = Number(retryAfter);
            waitMs = Number.isFinite(n) ? n * 1000 : backoffBase * Math.pow(2, attempt);
          } else {
            waitMs = backoffBase * Math.pow(2, attempt);
          }
          waitMs += Math.floor(Math.random() * 250); // jitter
          console.log(`   ⚠️  ${country} 429; waiting ${waitMs}ms`);
          setGlobalBackoff(waitMs);
          if (attempt === retries) throw new Error(`PRICE_get_request_failed 429 ${bodyText}`);
          await sleep(waitMs);
          continue;
        }
        throw new Error(`PRICE_get_request_failed ${res.status} ${bodyText}`);
      }
      return await res.json();
    } catch (err) {
      if (attempt === retries) throw err;
      const wait = backoffBase * Math.pow(2, attempt) + Math.floor(Math.random() * 250);
      console.log(`   ⚠️  ${country} retry ${attempt + 1} in ${wait}ms (${err.message || err})`);
      await sleep(wait);
    }
  }
}

/** Fetch all pages sequentially for a single country; log per-page progress */
async function getPricesForCountry(country, ids) {
  let acc = [];
  console.log(`→ Fetching ${ids.length} IDs for ${country}…`);
  for (let offset = 0; offset < ids.length; offset += PRICE_LIST_LIMIT) {
    const chunk = ids.slice(offset, offset + PRICE_LIST_LIMIT);
    const page = await fetchPricesPageWithRetry(country, chunk);
    if (Array.isArray(page?.prices)) acc = acc.concat(page.prices);
    console.log(`   ▸ ${country} fetched ${Math.min(offset + PRICE_LIST_LIMIT, ids.length)}/${ids.length} (acc=${acc.length})`);
  }
  console.log(`✓ Completed ${country} (${acc.length} rows)`);
  return { country, prices: acc };
}

/** Normalize an API row to your output shape */
function formatPriceRow(row) {
  const reg = row?.regular_price || {};
  const disc = row?.discount_price || {};
  // always prefer raw_value now
  const pickRaw = (p) => p ? (p.raw_value ?? null) : null;
  return {
    regular: pickRaw(reg),
    regular_currency: reg.currency || null,
    sale: pickRaw(disc),
    sale_currency: disc.currency || null,
    sale_start: disc.start_datetime || null,
    sale_end: disc.end_datetime || null
  };
}

/** Merge fetched country prices back into entries by NSUID */
function mergeBack(entries, country, priceRows, countryToIdToIndexes) {
  const idMap = countryToIdToIndexes[country] || {};
  let applied = 0;
  for (const row of priceRows) {
    const id = String(row?.title_id || '');
    const targets = idMap[id];
    if (!targets) continue;
    const formatted = formatPriceRow(row);
    for (const idx of targets) {
      if (!entries[idx].prices) entries[idx].prices = {};
      entries[idx].prices[country] = formatted;
      applied++;
    }
  }
  console.log(`   ↳ merged ${applied} price mappings for ${country}`);
}

/** Build batches: which countries to fetch which IDs, and where to merge them back */
function buildCountryBatches(entries) {
  const countryToIds = {};
  const countryToIdToIndexes = {};
  const upsert = (country, id, idx) => {
    if (!countryToIds[country]) countryToIds[country] = new Set();
    if (!countryToIdToIndexes[country]) countryToIdToIndexes[country] = {};
    if (!countryToIdToIndexes[country][id]) countryToIdToIndexes[country][id] = [];
    countryToIds[country].add(id);
    countryToIdToIndexes[country][id].push(idx);
  };

  entries.forEach((e, idx) => {
    if (!e || e.active_in_base !== true) return;
    if (e.nsuid_us) regionSets.US.forEach(c => upsert(c, String(e.nsuid_us), idx));
    if (e.nsuid_eu) regionSets.EU.forEach(c => upsert(c, String(e.nsuid_eu), idx));
    if (e.nsuid_jp) regionSets.JP.forEach(c => upsert(c, String(e.nsuid_jp), idx));
    if (e.nsuid_kr) regionSets.KR.forEach(c => upsert(c, String(e.nsuid_kr), idx));
    if (e.nsuid_hk) regionSets.HK.forEach(c => upsert(c, String(e.nsuid_hk), idx));
  });

  const countryToIdsArr = {};
  for (const [country, set] of Object.entries(countryToIds)) {
    countryToIdsArr[country] = Array.from(set);
  }

  return { countryToIds: countryToIdsArr, countryToIdToIndexes };
}

// ---------- main ----------
(async () => {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`Missing input file: ${INPUT_FILE}`);
    process.exit(1);
  }
  const raw = fs.readFileSync(INPUT_FILE, 'utf8');
  let entries;
  try {
    entries = JSON.parse(raw);
  } catch (e) {
    console.error('Input is not valid JSON.');
    throw e;
  }
  if (!Array.isArray(entries)) {
    console.error('Expected top-level array in merged_enriched.json');
    process.exit(1);
  }

  console.log(`Loaded ${entries.length} entries`);
  const { countryToIds, countryToIdToIndexes } = buildCountryBatches(entries);

  const countries = Object.keys(countryToIds).filter(c => (countryToIds[c]?.length ?? 0) > 0);
  console.log(`Countries to fetch (${countries.length}): ${countries.join(', ')}`);
  countries.forEach(c => console.log(`  - ${c}: ${countryToIds[c].length} IDs`));

  // Prepare tasks: one per country
  const tasks = countries.map(country => async () => {
    const ids = countryToIds[country];
    try {
      const { prices } = await getPricesForCountry(country, ids);
      mergeBack(entries, country, prices, countryToIdToIndexes);
      await sleep(150); // tiny gap before next task
    } catch (err) {
      console.warn(`⚠️  Skipping ${country}: ${err.message || err}`);
    }
  });

  // Run with limited concurrency
  console.log(`Starting country fetches with concurrency=${COUNTRY_POOL_SIZE}…`);
  await runWithPool(tasks, { concurrency: COUNTRY_POOL_SIZE });

  // Write output
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(entries, null, 2), 'utf8');
  console.log(`✅ Wrote ${OUTPUT_FILE} with ${entries.length} entries`);
})().catch(err => {
  console.error(err);
  process.exit(1);
});
