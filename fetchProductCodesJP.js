#!/usr/bin/env node
/**
 * JP enrichment (MASTER + SNAPSHOT)
 *
 * MASTER:   data/jp_games_enriched.json      (grow-forever union)
 * SNAPSHOT: data/jp_games_enriched_current.json (only active_in_base=true)
 *
 * Adds: active_in_base, first_seen_at, last_seen_at, last_checked_at
 * Preserves original behavior:
 *  - Fetch per-item HTML, extract c_groupCode -> productCode_jp
 *  - Detect English support (supportLanguage='en') when found in product node
 *  - Fill platform from c_labelPlatform: "BEE"->"Nintendo Switch 2", "HAC"->"Nintendo Switch"
 *  - Only fetch network for ACTIVE items (present in today's base) with D-prefixed nsuid
 *  - Periodic saves + debug HTML dump on misses
 *
 * Input  (default): data/jp_games.json
 * Output (master) : data/jp_games_enriched.json
 * Output (current): data/jp_games_enriched_current.json
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');

// -------- CLI --------
const args = process.argv.slice(2);
function getArg(name, def = undefined) {
  const i = args.findIndex(a => a === `--${name}`);
  if (i === -1) return def;
  const val = args[i + 1];
  if (!val || val.startsWith('--')) return true;
  return val;
}

const INPUT_PATH   = getArg('in',  'data/jp_games.json');
const OUT_MASTER   = getArg('out', 'data/jp_games_enriched.json');
const OUT_CURRENT  = 'data/jp_games_enriched_current.json';
const CONCURRENCY  = Number(getArg('concurrency', 4));
const FORCE        = !!getArg('force', false);

const REQUEST_DELAY_MS = 200;
const RETRIES = 3;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

function ensureDir(p) { fs.mkdirSync(path.dirname(p), { recursive: true }); }

// -------- HTTP --------
async function safeGet(url, retries = RETRIES) {
  const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ja,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Referer': 'https://store-jp.nintendo.com/',
    'Connection': 'keep-alive',
  };
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await axios.get(url, { headers: HEADERS, timeout: 20000, maxRedirects: 5, decompress: true });
      return res.data;
    } catch (err) {
      if (attempt === retries - 1) throw err;
      const jitter = 400 + Math.floor(Math.random() * 600);
      console.warn(`⚠️ GET failed (${attempt + 1}/${retries}) for ${url}: ${err.message}. Backing off…`);
      await sleep(800 + attempt * 800 + jitter);
    }
  }
}

// -------- JSON utils --------
function readJsonSafe(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}
function writePrettyJson(filePath, data) {
  ensureDir(filePath);
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
}

// -------- Content helpers (from your original) --------
function hasEnglishSupport(product) {
  const langs = product?.c_original_specification?.supportLanguages || [];
  const norm = new Set(langs.map(x => String(x).trim()));
  return norm.has('en') || norm.has('en_US');
}

function dfsFindGroupCodeAndProduct(node) {
  if (node == null || typeof node !== 'object') return { code: null, productNode: null };
  if (Array.isArray(node)) {
    for (const v of node) { const hit = dfsFindGroupCodeAndProduct(v); if (hit.code) return hit; }
    return { code: null, productNode: null };
  }
  for (const [k, v] of Object.entries(node)) {
    if (k === 'c_groupCode' && typeof v === 'string' && v.trim()) {
      return { code: v.trim(), productNode: node };
    }
  }
  for (const v of Object.values(node)) {
    const hit = dfsFindGroupCodeAndProduct(v);
    if (hit.code) return hit;
  }
  return { code: null, productNode: null };
}

function dfsFindLabelPlatform(node) {
  if (!node || typeof node !== 'object') return null;
  if (Array.isArray(node)) {
    for (const v of node) { const found = dfsFindLabelPlatform(v); if (found) return found; }
    return null;
  }
  if (typeof node.c_labelPlatform === 'string' && node.c_labelPlatform.trim()) {
    return node.c_labelPlatform.trim();
  }
  for (const v of Object.values(node)) {
    const found = dfsFindLabelPlatform(v);
    if (found) return found;
  }
  return null;
}

// Parse JSON-bearing <script> blocks; extract productCode, english flag, and platform via c_labelPlatform
function extractJPFromHtml(html) {
  const $ = cheerio.load(html);
  const candidates = [];

  $('script[type="application/json"], script[id], script').each((_, el) => {
    const s = ($(el).contents().text() || '').trim();
    if (!s) return;
    try {
      const looksJson = (s.startsWith('{') && s.endsWith('}')) || (s.startsWith('[') && s.endsWith(']'));
      if (looksJson) candidates.push(JSON.parse(s));
    } catch {}
  });

  let productCode = null;
  let english = false;
  let platformFromLabel = '';

  for (const j of candidates) {
    const { code, productNode } = dfsFindGroupCodeAndProduct(j);
    if (code && !productCode) {
      productCode = code.replace(/_/g, '').toUpperCase();
      english = hasEnglishSupport(productNode);
    }
    if (!platformFromLabel) {
      const label = dfsFindLabelPlatform(j); // "BEE" or "HAC"
      if (label === 'BEE') platformFromLabel = 'Nintendo Switch 2';
      else if (label === 'HAC') platformFromLabel = 'Nintendo Switch';
    }
    if (productCode && platformFromLabel) break;
  }

  if (!productCode) {
    const m = html.match(/"c_groupCode"\s*:\s*"([A-Za-z0-9_\-]+)"/);
    if (m && m[1]) productCode = m[1].replace(/_/g, '').toUpperCase();
  }

  return { productCode: productCode || null, english: !!english, platform: platformFromLabel };
}

function buildItemUrl(nsuid) {
  return `https://store-jp.nintendo.com/item/software/${nsuid}`;
}

// Simple concurrency pool
function pool(items, limit, worker) {
  let i = 0, active = 0;
  const results = [];
  return new Promise((resolve) => {
    const next = () => {
      if (i >= items.length && active === 0) return resolve(Promise.all(results));
      while (active < limit && i < items.length) {
        const idx = i++;
        active++;
        const p = Promise.resolve()
          .then(() => worker(items[idx], idx))
          .then(r => { active--; next(); return r; })
          .catch(_ => { active--; next(); });
        results.push(p);
      }
    };
    next();
  });
}

// ---------- Non-destructive helpers ----------
const isNonEmpty = (v) => (typeof v === 'string' ? v.trim() !== '' : v != null);
// prefer non-empty a; else non-empty b; else a (keep types)
function pick(a, b) {
  const sa = (a ?? '').toString().trim();
  const sb = (b ?? '').toString().trim();
  return sa ? a : (sb ? b : a);
}

// -------- Main (MASTER + SNAPSHOT) --------
(async function main() {
  const baseInput = readJsonSafe(INPUT_PATH, []);
  if (!Array.isArray(baseInput) || baseInput.length === 0) {
    console.error(`No data in ${INPUT_PATH}. Make sure it exists and is a JSON array.`);
    process.exit(1);
  }

  // Today's "base view": index by nsuid_jp (string), and keep only D-prefixed for fetch candidates
  const inBase = new Map();
  const activeDPref = new Set();
  for (const g of baseInput) {
    const nsuid = String(g?.nsuid_jp || g?.nsuid || '');
    if (!nsuid) continue;
    inBase.set(nsuid, g);
    if (/^D/i.test(nsuid)) activeDPref.add(nsuid);
  }

  // Load existing MASTER
  const existing = readJsonSafe(OUT_MASTER, []);
  const existingById = new Map();
  for (const row of Array.isArray(existing) ? existing : []) {
    const nsuid = row?.nsuid_jp ? String(row.nsuid_jp) : (row?.nsuid ? String(row.nsuid) : null);
    if (nsuid) existingById.set(nsuid, row);
  }

  // UNION: base ∪ existing (no drops)
  const unionIds = new Set([...inBase.keys(), ...existingById.keys()]);
  const working = [];
  const now = nowIso();

  for (const id of unionIds) {
    const base = inBase.get(id) || {};
    const prior = existingById.get(id) || {};

    const merged = { ...prior };

    // Fresh fields from base (non-destructive)
    if (isNonEmpty(base.title))       merged.title = base.title;
    if (isNonEmpty(base.url))         merged.url   = base.url;
    if (isNonEmpty(base.urlKey))      merged.urlKey = base.urlKey;
    merged.platform    = pick(base.platform,    merged.platform);
    merged.genres      = (Array.isArray(base.genres) && base.genres.length) ? base.genres : (Array.isArray(merged.genres) ? merged.genres : []);
    merged.releaseDate = pick(base.releaseDate, merged.releaseDate);
    merged.imageSquare = pick(base.imageSquare, merged.imageSquare);
    merged.imageKey    = pick(base.imageKey,    merged.imageKey);
    merged.publisher   = pick(base.publisher,   merged.publisher);
    merged.dlcType     = pick(base.dlcType,     merged.dlcType);
    merged.playerCount = pick(base.playerCount, merged.playerCount);

    // IDs
    merged.nsuid_jp = pick(base.nsuid_jp, merged.nsuid_jp) || pick(base.nsuid, merged.nsuid);

    // Keep existing productCode_jp unless base provides a non-empty (base typically doesn't)
    merged.productCode_jp = pick(base.productCode_jp, merged.productCode_jp);

    // If supportLanguage already set, keep it; else leave for fetch to possibly set
    if (isNonEmpty(base.supportLanguage) && !merged.supportLanguage) merged.supportLanguage = base.supportLanguage;

    // Bookkeeping
    merged.active_in_base = inBase.has(id);
    merged.first_seen_at  = merged.first_seen_at || prior.first_seen_at || now;
    if (merged.active_in_base) {
      merged.last_seen_at = now;             // seen in today's base
    } else {
      merged.last_seen_at = merged.last_seen_at || now; // keep previous if any
    }

    working.push(merged);
  }

  // Decide what to fetch (ONLY for active items that are D-prefixed)
  const toProcess = working.filter(row => {
    if (!row.active_in_base) return false;
    const nsuid = String(row.nsuid_jp || row.nsuid || '');
    if (!/^D/i.test(nsuid)) return false;  // JP pages of interest
    if (!FORCE && typeof row.productCode_jp === 'string' && row.productCode_jp.trim() !== '') {
      // already have code; we might still do platform fill if blank
      if (String(row.platform || '') !== '') return false; // platform already present -> skip altogether
      return true; // allow platform-only detection
    }
    return true; // need code and/or platform
  });

  console.log(`JP union=${working.length} active=${inBase.size} fetchCandidates=${toProcess.length}`);

  // Index for in-place updates
  const idxById = new Map();
  for (let i = 0; i < working.length; i++) {
    const id = String(working[i].nsuid_jp || working[i].nsuid || '');
    if (id) idxById.set(id, i);
  }

  // Fetch loop
  let processed = 0, updated = 0, skipped = 0, failed = 0;
  await pool(toProcess, CONCURRENCY, async (row) => {
    const nsuid = String(row.nsuid_jp || row.nsuid || '');
    const needPlatform = String(row.platform || '') === '';
    const haveCode = typeof row.productCode_jp === 'string' && row.productCode_jp.trim() !== '';

    if (!FORCE && haveCode && !needPlatform) {
      skipped++; processed++;
      return;
    }

    const url = buildItemUrl(nsuid);
    try {
      const jitter = Math.floor(Math.random() * 600);
      await sleep(REQUEST_DELAY_MS + jitter);

      const html = await safeGet(url);
      const { productCode, english, platform } = extractJPFromHtml(html);

      const i = idxById.get(nsuid);
      if (i != null) {
        if (productCode) {
          working[i].productCode_jp = productCode;
          if (english && !working[i].supportLanguage) working[i].supportLanguage = 'en';
          updated++;
          console.log(`✅ ${nsuid} → ${productCode}${english ? ' (en)' : ''}${platform ? ` [${platform}]` : ''}`);
        } else {
          // platform-only success?
          if (needPlatform && platform) {
            console.log(`ℹ️ Platform-only filled: ${nsuid} [${platform}]`);
          } else {
            ensureDir(`debug_html_jp/${nsuid}.html`);
            fs.writeFileSync(`debug_html_jp/${nsuid}.html`, html);
            console.warn(`⚠️ No productCode_jp found for ${nsuid} (${url}). Saved HTML.`);
            failed++;
          }
        }
        // platform fill only if currently empty and we detected it
        if (String(working[i].platform || '') === '' && platform) {
          working[i].platform = platform;
        }
        working[i].last_checked_at = nowIso();
      }
    } catch (err) {
      failed++;
      console.warn(`❌ Failed ${nsuid} → ${err.message}`);
    } finally {
      processed++;
      if (processed % 50 === 0 || processed === toProcess.length) {
        // Periodic save of MASTER + CURRENT
        writePrettyJson(OUT_MASTER, working);
        const current = working.filter(e => e.active_in_base);
        writePrettyJson(OUT_CURRENT, current);
        console.log(`💾 Progress saved (${processed}/${toProcess.length})`);
      }
    }
  });

  // Final save
  writePrettyJson(OUT_MASTER, working);
  const current = working.filter(e => e.active_in_base);
  writePrettyJson(OUT_CURRENT, current);

  console.log('\n=== Summary (JP MASTER) ===');
  console.log(`Processed: ${processed}`);
  console.log(`Updated:   ${updated}`);
  console.log(`Skipped:   ${skipped}`);
  console.log(`Failed:    ${failed}`);
  console.log(`Master:    ${path.resolve(OUT_MASTER)} (${working.length})`);
  console.log(`Current:   ${path.resolve(OUT_CURRENT)} (${current.length})`);
})().catch(err => {
  console.error(err);
  process.exit(1);
});
