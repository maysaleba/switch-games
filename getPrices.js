#!/usr/bin/env node
/**
 * Inline merged_enriched → prices enricher (with progress + safe concurrency)
 * - Reads:  ./merged_enriched.json
 * - Writes: ./merged_enriched_with_prices.json
 *
 * Logic:
 *   - Loads previous merged_enriched_with_prices.json as price cache.
 *   - Reuses previous prices per game.
 *   - Skips fetching a country if that country's sale_end is still active.
 *   - Still fetches missing/expired country prices.
 */

const fs = require('fs');
const path = require('path');

// ---------- config ----------
const INPUT_FILE  = path.resolve('output/merged_enriched.json');
const OUTPUT_FILE = path.resolve('output/merged_enriched_with_prices.json');
const PREVIOUS_PRICE_FILE = OUTPUT_FILE;

const PRICE_GET_URL     = 'https://api.ec.nintendo.com/v1/price';
const PRICE_LIST_LIMIT  = 50;
const PRICE_GET_LANG    = 'en';

const COUNTRY_POOL_SIZE = 2;
const BACKOFF_BASE_MS   = 800;

const regionSets = {
  US: ['US', 'MX', 'BR', 'CA', 'CO', 'AR', 'PE'],
  EU: ['ZA', 'AU', 'NZ', 'NO', 'PL'],
  JP: ['JP'],
  KR: ['KR'],
  HK: ['HK'],
  AS: ['TH', 'SG', 'MY'],
};

// ---------- helpers ----------
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

function toPriceCache(entries) {
  return entries
    .map(entry => ({
      key: getEntryKey(entry),
      prices: entry.prices || {}
    }))
    .filter(x => x.key && Object.keys(x.prices).length > 0);
}

function loadJsonArraySafe(filePath) {
  try {
    if (!fs.existsSync(filePath)) return [];
    const raw = fs.readFileSync(filePath, 'utf8').trim();
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function getEntryKey(e) {
  return String(
    e.urlKey ||
    e.nsuid_us ||
    e.nsuid_eu ||
    e.nsuid_jp ||
    e.nsuid_hk ||
    e.nsuid_as ||
    e.nsuid_kr ||
    ''
  );
}

function hydratePreviousPrices(entries) {
  const previous = loadJsonArraySafe(PREVIOUS_PRICE_FILE);
  const previousByKey = new Map();

  for (const old of previous) {
    const key = old.key || getEntryKey(old);
    if (key) previousByKey.set(key, old);
  }

  let copied = 0;

  for (const entry of entries) {
    const key = getEntryKey(entry);
    const old = previousByKey.get(key);

    if (old?.prices) {
      entry.prices = old.prices;
      copied++;
    }
  }

  console.log(`♻️ Reused previous prices for ${copied}/${entries.length} entries`);
}

function saleStillActive(entry, country) {
  const p = entry?.prices?.[country];

  if (!p) return false;
  if (p.sale == null || p.sale === '') return false;
  if (!p.sale_end) return false;

  const end = new Date(p.sale_end);
  if (Number.isNaN(end.getTime())) return false;

  return end > new Date();
}

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
          .finally(() => {
            active--;
            next();
          });
      }
    };

    next();
  });
}

// Global backoff gate
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

/** One price page with retries/backoff */
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

        if (res.status === 429) {
          let waitMs = 0;
          const retryAfter = res.headers.get('retry-after');

          if (retryAfter) {
            const n = Number(retryAfter);
            waitMs = Number.isFinite(n)
              ? n * 1000
              : backoffBase * Math.pow(2, attempt);
          } else {
            waitMs = backoffBase * Math.pow(2, attempt);
          }

          waitMs += Math.floor(Math.random() * 250);

          console.log(`   ⚠️  ${country} 429; waiting ${waitMs}ms`);
          setGlobalBackoff(waitMs);

          if (attempt === retries) {
            throw new Error(`PRICE_get_request_failed 429 ${bodyText}`);
          }

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

/** Fetch all pages sequentially for a single country */
async function getPricesForCountry(country, ids) {
  let acc = [];

  console.log(`→ Fetching ${ids.length} IDs for ${country}…`);

  for (let offset = 0; offset < ids.length; offset += PRICE_LIST_LIMIT) {
    const chunk = ids.slice(offset, offset + PRICE_LIST_LIMIT);
    const page = await fetchPricesPageWithRetry(country, chunk);

    if (Array.isArray(page?.prices)) {
      acc = acc.concat(page.prices);
    }

    console.log(`   ▸ ${country} fetched ${Math.min(offset + PRICE_LIST_LIMIT, ids.length)}/${ids.length} (acc=${acc.length})`);
  }

  console.log(`✓ Completed ${country} (${acc.length} rows)`);
  return { country, prices: acc };
}

/** Normalize API row */
function formatPriceRow(row) {
  const reg = row?.regular_price || {};
  const disc = row?.discount_price || {};

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

/** Merge fetched prices back into entries */
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

function hasEnglish(value) {
  if (!value) return false;

  let tokens = [];

  if (Array.isArray(value)) {
    tokens = value;
  } else if (typeof value === 'string') {
    tokens = value.split(/[\s,;|/]+/);
  } else {
    return false;
  }

  return tokens.some(tok => {
    if (!tok) return false;

    const t = String(tok).trim().toLowerCase();

    return t === 'en' || t === 'english' || /^en([-_][a-z]+)?$/.test(t);
  });
}

function supportsEnglishForRegion(entry, region) {
  switch (region) {
    case 'JP': return hasEnglish(entry.supportLanguage_jp);
    case 'KR': return hasEnglish(entry.supportLanguage_kr);
    case 'HK': return hasEnglish(entry.supportLanguage_hk);
    default:   return true;
  }
}

/** Build fetch batches */
function buildCountryBatches(entries) {
  const countryToIds = {};
  const countryToIdToIndexes = {};
  let skippedActiveKnownSales = 0;

  const upsert = (country, id, idx) => {
    if (saleStillActive(entries[idx], country)) {
      skippedActiveKnownSales++;
      return;
    }

    if (!countryToIds[country]) countryToIds[country] = new Set();
    if (!countryToIdToIndexes[country]) countryToIdToIndexes[country] = {};
    if (!countryToIdToIndexes[country][id]) countryToIdToIndexes[country][id] = [];

    countryToIds[country].add(id);
    countryToIdToIndexes[country][id].push(idx);
  };

  entries.forEach((e, idx) => {
    if (!e || e.active_in_base !== true) return;

    if (e.nsuid_us) {
      regionSets.US.forEach(c => upsert(c, String(e.nsuid_us), idx));
    }

    if (e.nsuid_eu) {
      regionSets.EU.forEach(c => upsert(c, String(e.nsuid_eu), idx));
    }

    if (e.nsuid_as) {
      regionSets.AS.forEach(c => upsert(c, String(e.nsuid_as), idx));
    }

    if (e.nsuid_jp && supportsEnglishForRegion(e, 'JP')) {
      regionSets.JP.forEach(c => upsert(c, String(e.nsuid_jp), idx));
    } else if (e.nsuid_jp && !supportsEnglishForRegion(e, 'JP')) {
      console.log(`   ⤷ skip JP (no EN) for idx=${idx} title="${e.title || ''}"`);
    }

    if (e.nsuid_kr && supportsEnglishForRegion(e, 'KR')) {
      regionSets.KR.forEach(c => upsert(c, String(e.nsuid_kr), idx));
    } else if (e.nsuid_kr && !supportsEnglishForRegion(e, 'KR')) {
      console.log(`   ⤷ skip KR (no EN) for idx=${idx} title="${e.title || ''}"`);
    }

    if (e.nsuid_hk && supportsEnglishForRegion(e, 'HK')) {
      regionSets.HK.forEach(c => upsert(c, String(e.nsuid_hk), idx));
    } else if (e.nsuid_hk && !supportsEnglishForRegion(e, 'HK')) {
      console.log(`   ⤷ skip HK (no EN) for idx=${idx} title="${e.title || ''}"`);
    }
  });

  const countryToIdsArr = {};

  for (const [country, set] of Object.entries(countryToIds)) {
    countryToIdsArr[country] = Array.from(set);
  }

  console.log(`⏭️ Skipped ${skippedActiveKnownSales} country-price checks because sale_end is still active`);

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

  hydratePreviousPrices(entries);

  const { countryToIds, countryToIdToIndexes } = buildCountryBatches(entries);

  const countries = Object.keys(countryToIds).filter(c => (countryToIds[c]?.length ?? 0) > 0);

  console.log(`Countries to fetch (${countries.length}): ${countries.join(', ') || '(none)'}`);
  countries.forEach(c => console.log(`  - ${c}: ${countryToIds[c].length} IDs`));

  let failedCountries = [];

  const tasks = countries.map(country => async () => {
    const ids = countryToIds[country];

    try {
      const { prices } = await getPricesForCountry(country, ids);
      mergeBack(entries, country, prices, countryToIdToIndexes);
      await sleep(150);
    } catch (err) {
      console.error(`❌ FAILED ${country}: ${err.message || err}`);
      failedCountries.push(country);
    }
  });

  console.log(`Starting country fetches with concurrency=${COUNTRY_POOL_SIZE}…`);

  await runWithPool(tasks, { concurrency: COUNTRY_POOL_SIZE });

  if (failedCountries.length > 0) {
    console.error(`\n🚨 Price fetching failed for: ${failedCountries.join(', ')}`);
    process.exit(1);
  }

  const priceCache = toPriceCache(entries);
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(priceCache, null, 2), 'utf8');

  console.log(`✅ Wrote ${OUTPUT_FILE} with ${entries.length} entries`);
})().catch(err => {
  console.error(err);
  process.exit(1);
});