#!/usr/bin/env node
/**
 * Faster EU eShop fetcher with backoff safety + MASTER/SNAPSHOT bookkeeping.
 *
 * MASTER  (data/eu_games_enriched.json):
 *   - Grow-forever union of everything ever seen.
 *   - Adds: active_in_base, first_seen_at, last_seen_at, last_checked_at.
 *   - Non-destructive merge: fetched blanks don't overwrite existing values.
 *
 * SNAPSHOT (data/eu_games_enriched_current.json):
 *   - Only items present in today's fetch (active_in_base=true).
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios');

// ---------- helpers ----------
const delay = (ms) => new Promise(r => setTimeout(r, ms));

async function safeGet(url, { params = {}, headers = {} } = {}, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await axios.get(url, { params, headers, timeout: 20000 });
    } catch (err) {
      const isLast = attempt === retries - 1;
      if (isLast) throw err;
      const backoff = 500 * Math.pow(2, attempt); // 500, 1000, 2000…
      console.warn(`⚠️  EU request failed (attempt ${attempt + 1}). Retrying in ${backoff}ms`);
      await delay(backoff);
    }
  }
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

function euKey(doc) {
  const v = doc?.nsuid_txt;
  return Array.isArray(v) ? v[0] : v || null;
}

// prefer non-empty `a`; else non-empty `b`; else `a` (keep types)
function pick(a, b) {
  const sa = (a ?? '').toString().trim();
  const sb = (b ?? '').toString().trim();
  return sa ? a : (sb ? b : a);
}

function nowIso() { return new Date().toISOString(); }

function mapDoc(doc) {
  const nsuid = euKey(doc) || '';

  // Normalize platform
  const platformName = Array.isArray(doc?.system_names_txt) && doc.system_names_txt.length > 0
    ? doc.system_names_txt[0]
    : 'Nintendo Switch';

  // Slugify title + platform
  const slugBase = (doc?.title || '').trim().toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')   // replace non-alphanumeric with hyphen
    .replace(/^-+|-+$/g, '');      // trim leading/trailing hyphens

  // Normalize platform name for URL key
  let platformKey = '';
  if (platformName === 'Nintendo Switch') {
    platformKey = 'switch';
  } else if (platformName === 'Nintendo Switch 2') {
    platformKey = 'switch-2';
  } else {
    platformKey = platformName.toLowerCase().replace(/\s+/g, '-');
  }

  const urlKey = slugBase ? `${slugBase}-${platformKey}` : '';

  return {
    title: doc?.title || '',
    nsuid_eu: nsuid,
    url: doc?.url || '',
    urlKey,
    platform: platformName,
    genres: Array.isArray(doc?.pretty_game_categories_txt) ? doc.pretty_game_categories_txt : [],
    releaseDate: Array.isArray(doc?.dates_released_dts) && doc.dates_released_dts.length > 0
      ? doc.dates_released_dts[0]
      : '',
    imageSquare: doc?.image_url_sq_s || '',
    imageKey: doc?.image_url || '',
    publisher: doc?.publisher || '',
    dlcType: doc?.type || '',
    playerCount: doc?.players_to || '',
    productCode_eu: Array.isArray(doc?.product_code_txt) && doc.product_code_txt.length > 0
      ? doc.product_code_txt[0]
      : ''
  };
}

// ---------- EU fetch (unchanged behavior) ----------
async function fetchEUGamesOnSale({ locale = 'en', limit = 30000 } = {}) {
  console.log('▶️ Starting EU games fetch...');

  const BASE = `http://search.nintendo-europe.com/${locale}/select`;
  const PAGE_SIZE = 1000;
  const RATE_DELAY_MS = 600; // faster, but still safe

  const BASE_PARAMS = {
    fq: '(type:GAME OR type:DLC) AND price_has_discount_b:* AND nsuid_txt:*',
    q: '*',
    sort: 'sorting_title asc',
    wt: 'json'
  };

  const all = new Map();
  let start = 0;
  let numFound = 0;

  do {
    const params = { ...BASE_PARAMS, rows: PAGE_SIZE, start, _t: Date.now() };
    const { data } = await safeGet(BASE, { params });
    const docs = data?.response?.docs || [];
    numFound = data?.response?.numFound ?? numFound;

    for (const doc of docs) {
      const k = euKey(doc);
      if (k && !all.has(k)) all.set(k, doc);
    }

    start += PAGE_SIZE;
    await delay(RATE_DELAY_MS);
  } while (start < numFound && all.size < limit);

  const unique = Array.from(all.values()).slice(0, limit).map(mapDoc);
  console.log(`▶️ Completed EU games fetch. Total unique: ${unique.length}`);
  return unique;
}

// ---------- main (MASTER/SNAPSHOT) ----------
async function main() {
  const outDir = path.join(__dirname, 'data');
  const outPathMaster  = path.join(outDir, 'eu_games_enriched.json');          // MASTER (union)
  const outPathCurrent = path.join(outDir, 'eu_games_enriched_current.json');  // SNAPSHOT (active only)

  // Fresh fetch (today's "base")
  const fetched = await fetchEUGamesOnSale(); // sale-only view by default

  // Index today's items by NSUID
  const fetchedById = new Map();
  for (const e of fetched) {
    if (e.nsuid_eu) fetchedById.set(e.nsuid_eu, e);
  }

  // Load existing MASTER
  const existing = loadJsonArraySafe(outPathMaster);
  const existingById = new Map();
  for (const e of existing) {
    if (e && e.nsuid_eu) existingById.set(e.nsuid_eu, e);
  }

  // UNION: fetched ∪ existing  (non-destructive)
  const unionIds = new Set([...fetchedById.keys(), ...existingById.keys()]);
  const master = [];
  const now = nowIso();

  for (const id of unionIds) {
    const base  = fetchedById.get(id)   || {};
    const prior = existingById.get(id)  || {};

    // start from prior; overlay safe fresh bits from today's fetch
    const merged = { ...prior };

    // Fresh metadata from fetch (only if non-empty)
    merged.title       = pick(base.title,       merged.title);
    merged.url         = pick(base.url,         merged.url);
    merged.urlKey      = pick(base.urlKey,      merged.urlKey);
    merged.platform    = pick(base.platform,    merged.platform);
    merged.genres      = (Array.isArray(base.genres) && base.genres.length) ? base.genres
                     : (Array.isArray(merged.genres) ? merged.genres : []);
    merged.releaseDate = pick(base.releaseDate, merged.releaseDate);
    merged.imageSquare = pick(base.imageSquare, merged.imageSquare);
    merged.imageKey    = pick(base.imageKey,    merged.imageKey);
    merged.publisher   = pick(base.publisher,   merged.publisher);
    merged.dlcType     = pick(base.dlcType,     merged.dlcType);
    merged.playerCount = pick(base.playerCount, merged.playerCount);

    // IDs (ensure continuity)
    merged.nsuid_eu    = pick(base.nsuid_eu,    merged.nsuid_eu);

    // productCode_eu comes straight from the API (often empty string when absent).
    // Keep prior non-empty if fetch is empty; otherwise accept new non-empty.
    merged.productCode_eu = pick(base.productCode_eu, merged.productCode_eu);

    // Bookkeeping
    merged.active_in_base = fetchedById.has(id);
    merged.first_seen_at  = merged.first_seen_at || prior.first_seen_at || now;
    if (merged.active_in_base) {
      merged.last_seen_at  = now;           // seen in today's fetch
      merged.last_checked_at = now;         // we "checked" it via the API today
    } else {
      merged.last_seen_at  = merged.last_seen_at || now; // keep previous
      // last_checked_at: leave as-is; not touched this run
    }

    master.push(merged);
  }

  // Save MASTER (union)
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outPathMaster, JSON.stringify(master, null, 2), 'utf8');

  // Save SNAPSHOT (only active entries from today's fetch)
  const current = master.filter(e => e.active_in_base);
  fs.writeFileSync(outPathCurrent, JSON.stringify(current, null, 2), 'utf8');

  console.log(`✅ Saved EU master to ${outPathMaster}`);
  console.log(`✅ Saved EU snapshot to ${outPathCurrent}`);
  console.log(`ℹ️ Existing: ${existing.length} | Fetched today: ${fetched.length} | Master total: ${master.length} | Active today: ${current.length}`);
}

if (require.main === module) {
  main().catch(err => { console.error(err); process.exit(1); });
}

module.exports = { fetchEUGamesOnSale };
