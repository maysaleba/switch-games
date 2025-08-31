#!/usr/bin/env node
/**
 * Inline merged_enriched → prices enricher (polite version)
 * - Reads:  ./output/merged_enriched.json
 * - Writes: ./output/merged_enriched_with_prices.json
 *
 * Politeness features:
 *  - Global rate limiter (minimum gap between ANY two HTTP requests + jitter)
 *  - Adaptive exponential backoff on 429 and 403 (treat 403 as soft-rate-limit on CI IPs)
 *  - Honors Retry-After when present (seconds or HTTP-date)
 *  - Adaptive concurrency (gentler defaults on CI)
 *  - Identifying User-Agent to avoid generic bot fingerprint
 */

const fs = require('fs');
const path = require('path');

// ---------- config ----------
const INPUT_FILE  = path.resolve('output/merged_enriched.json');
const OUTPUT_FILE = path.resolve('output/merged_enriched_with_prices.json');

const PRICE_GET_URL     = 'https://api.ec.nintendo.com/v1/price';
const PRICE_LIST_LIMIT  = 50;
const PRICE_GET_LANG    = 'en';

// Gentler defaults on CI (GitHub Actions sets CI=true)
const ON_CI = String(process.env.CI || '').toLowerCase() === 'true';

// Global pacing between any two requests
let MIN_REQUEST_GAP_MS = ON_CI ? 1200 : 500;  // ensure ~<1 rps on CI
const JITTER_MS = [75, 200];                  // extra random jitter each request

// Concurrency across countries
let COUNTRY_POOL_SIZE = ON_CI ? 1 : 3;

// Backoff base
let BACKOFF_BASE_MS = ON_CI ? 1500 : 800;

// Optional environment overrides (if you want to tweak per workflow)
if (process.env.MIN_REQUEST_GAP_MS) MIN_REQUEST_GAP_MS = +process.env.MIN_REQUEST_GAP_MS || MIN_REQUEST_GAP_MS;
if (process.env.COUNTRY_POOL_SIZE) COUNTRY_POOL_SIZE = +process.env.COUNTRY_POOL_SIZE || COUNTRY_POOL_SIZE;
if (process.env.BACKOFF_BASE_MS)   BACKOFF_BASE_MS   = +process.env.BACKOFF_BASE_MS   || BACKOFF_BASE_MS;

// Region sets (same as yours)
const regionSets = {
  US: ['US', 'MX', 'BR', 'CA', 'CO', 'AR', 'PE'],
  EU: ['ZA', 'AU', 'NZ', 'NO', 'PL'],
  JP: ['JP'],
  KR: ['KR'],
  HK: ['HK'],
};

// ---------- helpers ----------
const sleep = (ms) => new Promise(res => setTimeout(res, ms));
const rand = (a, b) => Math.floor(a + Math.random() * (b - a + 1));

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

// Global backoff gate — slows all requests after rate-limit events
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

// Global pacing gate — enforces min gap between *any* two requests
let lastRequestAt = 0;
async function politeGate() {
  const now = Date.now();
  const since = now - lastRequestAt;
  const need = MIN_REQUEST_GAP_MS - since;
  const jitter = rand(JITTER_MS[0], JITTER_MS[1]);
  if (need + jitter > 0) {
    await sleep(need + jitter);
  }
  lastRequestAt = Date.now();
}

function parseRetryAfter(val) {
  if (!val) return null;
  const n = Number(val);
  if (Number.isFinite(n)) return n * 1000;
  const d = Date.parse(val);
  return Number.isFinite(d) ? Math.max(0, d - Date.now()) : null;
}

const DEFAULT_HEADERS = {
  'User-Agent': `maysaleba-prices-bot/1.0 (${ON_CI ? 'github-actions' : 'local'})`,
  'Accept': 'application/json',
};

// Treat 403 like a soft rate-limit (CI IPs often receive this)
function isSoftRateLimited(status) {
  return status === 429 || status === 403;
}

/** One price page (≤50 ids), with retries/backoff and Retry-After handling */
async function fetchPricesPageWithRetry(country, idsChunk, { retries = 4, backoffBase = BACKOFF_BASE_MS } = {}) {
  const params = new URLSearchParams();
  params.set('country', country);
  params.set('limit', String(PRICE_LIST_LIMIT));
  params.set('lang', PRICE_GET_LANG);
  idsChunk.forEach(id => params.append('ids', id));

  for (let attempt = 0; attempt <= retries; attempt++) {
    await globalBackoffGate();
    await politeGate(); // ensure global pacing

    try {
      const url = `${PRICE_GET_URL}?${params.toString()}`;
      const res = await fetch(url, { headers: DEFAULT_HEADERS });

      if (!res.ok) {
        const bodyText = await res.text().catch(() => '');

        if (isSoftRateLimited(res.status)) {
          let waitMs = parseRetryAfter(res.headers.get('retry-after'));
          if (waitMs == null) waitMs = backoffBase * Math.pow(2, attempt);
          waitMs += rand(200, 600); // jitter
          console.log(`   ⚠️  ${country} ${res.status}; waiting ${waitMs}ms (attempt ${attempt + 1}/${retries + 1})`);
          setGlobalBackoff(waitMs);

          // On repeated soft rate-limits, get even more polite
          if (ON_CI && attempt >= 1) {
            MIN_REQUEST_GAP_MS = Math.min(5000, Math.max(MIN_REQUEST_GAP_MS, backoffBase * 2));
          }
          if (attempt === retries) throw new Error(`PRICE_get_request_failed ${res.status} ${bodyText}`);
          await sleep(waitMs);
          continue;
        }

        // Other HTTP errors: let retry with exponential backoff
        if (attempt === retries) throw new Error(`PRICE_get_request_failed ${res.status} ${bodyText}`);
        const wait = backoffBase * Math.pow(2, attempt) + rand(150, 450);
        console.log(`   ⚠️  ${country} HTTP ${res.status}; retry in ${wait}ms`);
        await sleep(wait);
        continue;
      }

      // Success
      const json = await res.json();
      return json;

    } catch (err) {
      if (attempt === retries) throw err;
      const wait = backoffBase * Math.pow(2, attempt) + rand(150, 450);
      console.log(`   ⚠️  ${country} network error; retry in ${wait}ms (${err.message || err})`);
      await sleep(wait);
    }
  }
}

/** Fetch all pages sequentially for a single country; log per-page progress */
async function getPricesForCountry(country, ids) {
  let acc = [];
  console.log(`→ Fetching ${ids.length} IDs for ${country}…`);
  // Gentle per-country initial delay to desynchronize
  await sleep(rand(200, 1000));

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

  // Randomized small startup delay (herd control on CI)
  if (ON_CI) await sleep(rand(400, 1400));

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
      // small pause between countries
      await sleep(rand(200, 600));
    } catch (err) {
      console.warn(`⚠️  Skipping ${country}: ${err.message || err}`);
      // If we got repeatedly 403/429, slow down globally for the rest
      MIN_REQUEST_GAP_MS = Math.min(6000, Math.max(MIN_REQUEST_GAP_MS, BACKOFF_BASE_MS * 2));
      setGlobalBackoff(BACKOFF_BASE_MS * 2 + rand(300, 900));
    }
  });

  console.log(`Starting country fetches with concurrency=${COUNTRY_POOL_SIZE}… (CI=${ON_CI})`);
  await runWithPool(tasks, { concurrency: COUNTRY_POOL_SIZE });

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(entries, null, 2), 'utf8');
  console.log(`✅ Wrote ${OUTPUT_FILE} with ${entries.length} entries`);
})().catch(err => {
  console.error(err);
  process.exit(1);
});
