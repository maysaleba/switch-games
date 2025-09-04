#!/usr/bin/env node
/**
 * csvjson.json <-> merged_enriched_with_prices.json sync + build (with progress logs)
 * - No XLSX output.
 * - Rebuilds csvjson.json + csvjson.csv.
 * - Drops entries with no sale_end in ANY region.
 * - Metacritic match/URL use a slug normalized by removing a trailing "-switch" or "-switch-2".
 * - Slug replacements are loaded from external slug_replacements.txt (lines like: old=new; "#" comments allowed).
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const dayjs = require('dayjs');

// -------------------- CONFIG --------------------
const REMOTE_CSVJSON_URL     = 'https://raw.githubusercontent.com/maysaleba/maysaleba.github.io/main/src/csvjson.json';
const LOCAL_FALLBACK_CSVJSON = path.resolve('output/csvjson.json');                 // sample fallback if remote fetch fails
const MERGED_WITH_PRICES     = path.resolve('output/merged_enriched_with_prices.json'); // source file you uploaded
const HLTB_JSON              = path.resolve('hltb.json');                                // optional
const METACRITIC_CSV         = path.resolve('metacritic_switch.csv');                    // local CSV you maintain
const SLUG_REPLACEMENTS_TXT  = path.resolve('slug_replacements.txt');                    // external replacements

const OUT_JSON = path.resolve('csvjson.json');
const OUT_CSV  = path.resolve('csvjson.csv');

// human-readable output keys per region for sale prices
const REGION_KEY_MAP = {
  US: 'SalePrice',           // explicit for US
  MX: 'MexicoPrice',
  BR: 'BrazilPrice',
  CA: 'CanadaPrice',
  CO: 'ColombiaPrice',
  AR: 'ArgentinaPrice',
  PE: 'PeruPrice',
  AU: 'AustraliaPrice',
  ZA: 'SouthafricaPrice',
  NZ: 'NewZealandPrice',
  PL: 'PolandPrice',
  NO: 'NorwayPrice',
  JP: 'JapanPrice',
  KR: 'KoreaPrice',
  HK: 'HongKongPrice'
};

// fallback chain for "Price" (regular) column
const REGULAR_FALLBACK = ['US', 'AU', 'JP', 'KR', 'HK'];

// -------------------- HELPERS --------------------
// Put this near your other helpers
function buildNintendoUrl(u) {
  if (!u) return '';
  const s = String(u).trim();
  if (!s) return '';
  // If it already starts with https, return as-is
  if (s.startsWith('https')) return s;
  // Otherwise, prefix the Nintendo domain (normalize leading slash)
  const path = s.startsWith('/') ? s : `/${s}`;
  return `https://www.nintendo.com${path}`;
}

function getJSON(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`GET ${url} failed with ${res.statusCode}`));
          return;
        }
        let data = '';
        res.on('data', (d) => (data += d));
        res.on('end', () => {
          try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
        });
      })
      .on('error', reject);
  });
}

function readJsonSafe(p) {
  try {
    if (!fs.existsSync(p)) return null;
    const raw = fs.readFileSync(p, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}
function readTextSafe(p) {
  try {
    if (!fs.existsSync(p)) return null;
    return fs.readFileSync(p, 'utf8');
  } catch {
    return null;
  }
}

// === Load replacements from external file ===
function loadSlugReplacements(filePath) {
  const map = new Map();
  const raw = readTextSafe(filePath);
  if (!raw) return map;
  const lines = raw
    .split(/\r?\n/)
    .map(l => l.trim())
    .filter(Boolean)
    .filter(l => !l.startsWith('#')); // allow comments
  for (const line of lines) {
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    const from = line.slice(0, eq).trim();
    const to   = line.slice(eq + 1).trim();
    if (from && to) map.set(from, to);
  }
  return map;
}
const SLUG_REPLACEMENTS = loadSlugReplacements(SLUG_REPLACEMENTS_TXT);

function toNumber(val) {
  if (val == null) return NaN;
  if (typeof val === 'number') return val;
  const s = String(val).trim();
  if (!s) return NaN;
  const num = s.replace(/[^\d.,-]/g, '').replace(',', '.');
  return Number(num);
}

function computeMaxPercentOffAcrossRegions(pricesObj) {
  let best = 0;
  for (const [, p] of Object.entries(pricesObj || {})) {
    if (!p) continue;
    const sale = toNumber(p.sale);
    const regular = toNumber(p.regular);
    if (isFinite(sale) && isFinite(regular) && regular > 0 && sale >= 0 && sale < regular) {
      const pct = Math.round((1 - sale / regular) * 100);
      if (pct > best) best = pct;
    }
  }
  return best > 0 ? `${best}%` : '';
}

// ---------- slug sanitize (external map + "-s" rule) ----------
function applySlugReplacements(slug) {
  if (!slug) return slug;
  if (SLUG_REPLACEMENTS.has(slug)) {
    slug = SLUG_REPLACEMENTS.get(slug);
  }
  if (slug !== 'dragon-quest-xi-s-echoes-of-an-elusive-age') {
    // replace "-s" at the end of a token with "s" (your historical rule)
    slug = slug.replace(/-s(?![a-zA-Z])/g, 's');
  }
  return slug;
}

// ✅ Metacritic normalization: strip trailing "-switch" or "-switch-2"
function normalizeForMetacritic(slug) {
  if (!slug) return slug;
  return slug.replace(/-switch(?:-2)?$/, '');
}

// HLTB fuzzy (simple + dependency-free)
function safeHLTB(hltbData, title) {
  if (!hltbData || !Array.isArray(hltbData) || !title) {
    return { MainStory: '', MainExtra: '', Completionist: '', LowestPrice: '' };
  }
  const clean = (t) => String(t).replace(/[^a-zA-Z0-9:+ ]/g, '').trim().toLowerCase();
  const target = clean(
    title
      .replace('CRISIS CORE –FINAL FANTASY VII– REUNION', 'CRISIS CORE: FINAL FANTASY VII REUNION')
      .replace('Prince of Persia The Lost Crown', 'Prince of Persia: The Lost Crown')
  );
  let best = null;
  let bestScore = 0;
  for (const row of hltbData) {
    const t = clean(row.game_name || '');
    const sim = jaccardSimilarity(target, t);
    if (sim > bestScore) { bestScore = sim; best = row; if (sim === 1) break; }
  }
  if (!best || bestScore < 0.9) return { MainStory: '', MainExtra: '', Completionist: '', LowestPrice: '' };
  return {
    MainStory: Math.round((best.comp_main || 0) / 3600) || '',
    MainExtra: Math.round((best.comp_plus || 0) / 3600) || '',
    Completionist: Math.round((best.comp_100 || 0) / 3600) || '',
    LowestPrice: best.game_id ? `/game/${best.game_id}` : ''
  };
}
function jaccardSimilarity(a, b) {
  const A = new Set(a.split(' ').filter(Boolean));
  const B = new Set(b.split(' ').filter(Boolean));
  const inter = [...A].filter((x) => B.has(x)).length;
  const uni = new Set([...A, ...B]).size || 1;
  return inter / uni;
}

function buildImageUrl(g) {
  const imgEU = g.imageSquare_eu && g.imageSquare_eu.trim();
  const img   = g.imageSquare && g.imageSquare.trim();

  if (imgEU) {
    return `https://images.weserv.nl/?url=${imgEU}&w=240`;
  }
  if (img) {
    return `https://images.weserv.nl/?url=${img}&w=240`;
  }
  if (g.imageKey) {
    if (g.imageKey.startsWith("https")) {
      return `https://images.weserv.nl/?url=${g.imageKey}&w=240`;
    }
    return `https://images.weserv.nl/?url=https://assets.nintendo.com/image/upload/${g.imageKey}&w=240`;
  }
  return '';
}

function buildMexPrice(g) {
  // NOTE: This preserves your existing logic for MexPrice.
  const imgEU = g.imageSquare_eu && g.imageSquare_eu.trim();
  const img   = g.imageSquare && g.imageSquare.trim();
  if (imgEU) return imgEU;
  if (img)   return img;
  if (g.imageKey) return `https://assets.nintendo.com/image/upload/${g.imageKey}`;
  return '';
}
function pickRegular(prices) {
  for (const code of REGULAR_FALLBACK) {
    const p = prices?.[code]?.regular;
    if (p != null && String(p) !== '') return p;
  }
  return '';
}

function toCsvValue(v) {
  if (v == null) return '';
  const s = String(v);
  if (s.includes('"') || s.includes(',') || s.includes('\n')) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function writeCsv(rows) {
  const headerSet = new Set();
  rows.forEach((r) => Object.keys(r).forEach((k) => headerSet.add(k)));
  const headers = Array.from(headerSet);
  const lines = [];
  lines.push(headers.map(toCsvValue).join(','));
  for (const r of rows) lines.push(headers.map((h) => toCsvValue(r[h])).join(','));
  fs.writeFileSync(OUT_CSV, lines.join('\n'), 'utf8');
}

// Load Metacritic CSV → map by sanitized+normalized Slug
function loadMetacriticMap() {
  const raw = readTextSafe(METACRITIC_CSV);
  if (!raw) return new Map();
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const header = lines.shift();
  if (!header) return new Map();

  const cols = header.split(',');
  const idxSlug = cols.indexOf('Slug');
  const idxScore = cols.indexOf('Critic Score');
  if (idxSlug === -1) return new Map();

  const map = new Map();
  for (const line of lines) {
    const parts = parseCsvLine(line, cols.length);
    const rawSlug = (parts[idxSlug] || '').trim();
    if (!rawSlug) continue;
    const sanitized = applySlugReplacements(rawSlug);
    const norm = normalizeForMetacritic(sanitized); // << key part
    const score = (idxScore >= 0 ? (parts[idxScore] || '').trim() : '');
    map.set(norm, { score });
  }
  return map;
}

// tiny CSV parser with quotes support
function parseCsvLine(line, expectCols) {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }
        else { inQ = false; }
      } else cur += c;
    } else {
      if (c === ',') { out.push(cur); cur = ''; }
      else if (c === '"') { inQ = true; }
      else cur += c;
    }
  }
  out.push(cur);
  while (expectCols && out.length < expectCols) out.push('');
  return out;
}

// Ensure all REGION_KEY_MAP labels exist on a row as strings
function ensureAllRegionKeys(row) {
  for (const label of Object.values(REGION_KEY_MAP)) {
    if (row[label] == null) row[label] = '';
  }
}

// -------------------- MAIN --------------------
(async () => {
  const t0 = Date.now();
  console.log('🚀 Start sync/build');

  // 0) quick info about external replacements
  console.log(`🔁 Loaded slug replacements: ${SLUG_REPLACEMENTS.size} rules from ${SLUG_REPLACEMENTS_TXT}`);

  // 1) load source (only active_in_base = true, and must have at least one sale_end)
  const merged = readJsonSafe(MERGED_WITH_PRICES) || [];
  const active = merged.filter(g =>
    g.active_in_base === true &&
    Object.values(g.prices || {}).some(p => p?.sale_end)
  );
  const droppedNoSaleEnd = merged.filter(g =>
    g.active_in_base === true &&
    !Object.values(g.prices || {}).some(p => p?.sale_end)
  ).length;

  console.log(`📦 merged_enriched_with_prices.json total: ${merged.length}`);
  console.log(`✅ active_in_base=true & has sale_end: ${active.length}`);
  console.log(`🚫 dropped for missing sale_end:      ${droppedNoSaleEnd}`);

  // Build source index by sanitized urlKey
  const sourceBySlug = new Map();
  for (const g of active) {
    const slug = applySlugReplacements((g.urlKey || '').trim());
    if (slug) sourceBySlug.set(slug, g);
  }
  console.log(`🧭 Source index prepared: ${sourceBySlug.size} unique slugs`);

  // 2) load csvjson.json (remote → fallback)
  let csvjson;
  try {
    process.stdout.write(`🌐 Fetching remote csvjson.json ... `);
    csvjson = await getJSON(REMOTE_CSVJSON_URL);
    console.log(`OK (${csvjson.length} rows)`);
  } catch (e) {
    console.log(`failed (${e.message}). Using local fallback.`);
    csvjson = readJsonSafe(LOCAL_FALLBACK_CSVJSON) || [];
    console.log(`📄 Loaded fallback csvjson.json: ${csvjson.length} rows`);
  }

  // 3) optional HLTB + Metacritic
  const hltbData = readJsonSafe(HLTB_JSON);
  console.log(`📘 HLTB present: ${!!hltbData} ${hltbData ? `(${hltbData.length} rows)` : ''}`);
  const metaMap = loadMetacriticMap();
  console.log(`🟣 Metacritic rows loaded: ${metaMap.size}`);

  // helpers to build rows / compare
  function buildRowFromSource(g) {
    const prices = g.prices || {};
    const saleEnds = [];
    const saleStarts = [];
    for (const [, p] of Object.entries(prices)) {
      if (p?.sale_end)   saleEnds.push(dayjs(p.sale_end));
      if (p?.sale_start) saleStarts.push(dayjs(p.sale_start));
    }
    const SaleEnds = saleEnds.length ? saleEnds.sort((a, b) => a - b)[0].format('YYYY-MM-DD') : '';
    const SaleStarted = saleStarts.length ? saleStarts.sort((a, b) => a - b)[0].format('YYYY-MM-DD') : '';
    const salePriceUS = prices?.US?.sale ?? ''; // guard unusual keying
    const PercentOff  = computeMaxPercentOffAcrossRegions(prices);
    const hltb        = safeHLTB(hltbData, g.title || '');
    const Image       = buildImageUrl(g);
    const MexPrice    = buildMexPrice(g);
    const Price       = pickRegular(prices);

    const row = {
      CanadaPrice: prices?.CA?.sale ?? '',
      SCORE: '',
      SaleEnds,
      LowestPrice: hltb.LowestPrice,
      SaleStarted,
      description: '',
      Image,
      NewZealandPrice: prices?.NZ?.sale ?? '',
      URL: buildNintendoUrl(g.url),   // ⬅️ was: g.url || '
      platform: g.platform || '',
      MainStory: hltb.MainStory,
      Trailer: '',
      PeruPrice: prices?.PE?.sale ?? '',
      PercentOff,
      Completionist: hltb.Completionist,
      genre: Array.isArray(g.genres) ? g.genres.join(', ') : (g.genre || ''),
      ArgentinaPrice: prices?.AR?.sale ?? '',
      AustraliaPrice: prices?.AU?.sale ?? '',
      MexPrice,
      ColombiaPrice: prices?.CO?.sale ?? '',
      SouthafricaPrice: prices?.ZA?.sale ?? '',
      BrazilPrice: prices?.BR?.sale ?? '',
      OpenCriticURL: '',
      Title: g.title || '',
      ESRBRating: g.dlcType || '',
      Publisher: g.publisher || '',
      ReleaseDate: g.releaseDate || '',
      Slug: applySlugReplacements((g.urlKey || '').trim()),
      PolandPrice: prices?.PL?.sale ?? '',
      NumberofPlayers: 'https://shope.ee/5ALD8alAHo',
      NorwayPrice: prices?.NO?.sale ?? '',
      Price,
      MexicoPrice: prices?.MX?.sale ?? '',
      MainExtra: hltb.MainExtra,
      SalePrice: salePriceUS,
      Popularity: null
    };

    // include any known region sale keys (from REGION_KEY_MAP)
    for (const [code, label] of Object.entries(REGION_KEY_MAP)) {
      if (label === 'SalePrice') continue; // US handled separately
      row[label] = prices?.[code]?.sale ?? row[label] ?? '';
    }

    // guarantee ALL region keys exist even if empty
    ensureAllRegionKeys(row);

    // Metacritic enrichment (normalize slug for compare + URL)
    const metaSlug = normalizeForMetacritic(row.Slug);
    const meta = metaMap.get(metaSlug);
    if (meta) {
      row.SCORE = meta.score || '';
    }
    row.OpenCriticURL = metaSlug
      ? `https://www.metacritic.com/game/${metaSlug}/critic-reviews/?platform=nintendo-switch`
      : '';

    return row;
  }

  function pricesDiffer(existing, fresh) {
    // ⬇️ Added 'Image' and 'MexPrice' to the comparison set
    const keysToCheck = new Set(Object.values(REGION_KEY_MAP).concat([
      'PercentOff','SaleEnds','SaleStarted','Price','Image','MexPrice'
    ]));
    for (const k of keysToCheck) {
      if ((existing?.[k] ?? '') !== (fresh?.[k] ?? '')) return true;
    }
    return false;
  }

  const nextRows = [];
  const seen = new Set();

  let kept = 0, updated = 0, deleted = 0, appended = 0;
  let i = 0;
  const totalExisting = csvjson.length;

  // Walk existing → keep/patch/delete
  for (const row of csvjson) {
    i++;
    const slug = applySlugReplacements((row.Slug || '').trim());
    if (!sourceBySlug.has(slug)) {
      deleted++;
      console.log(`[${i}/${totalExisting}] 🗑️  delete: ${slug}`);
      continue;
    }
    const g = sourceBySlug.get(slug);
    const rebuilt = buildRowFromSource(g);
    seen.add(slug);

    if (pricesDiffer(row, rebuilt)) {
      updated++;
      nextRows.push(rebuilt);
      console.log(`[${i}/${totalExisting}] 🔁 update: ${slug}`);
    } else {
      kept++;
      // keep row, but ensure SCORE/URL/platform/genre fresh & region keys present
      const keptRow = { ...row };
      const metaSlug = normalizeForMetacritic(rebuilt.Slug);
      keptRow.SCORE = rebuilt.SCORE || keptRow.SCORE || '';
      keptRow.OpenCriticURL = metaSlug
        ? `https://www.metacritic.com/game/${metaSlug}/critic-reviews/?platform=nintendo-switch`
        : (keptRow.OpenCriticURL || '');
      keptRow.platform = rebuilt.platform || keptRow.platform || '';
      keptRow.genre = rebuilt.genre || keptRow.genre || '';

      // ensure ALL region labels exist
      ensureAllRegionKeys(keptRow);

      nextRows.push(keptRow);

      if (i % 50 === 0 || i === totalExisting) {
        console.log(`[${i}/${totalExisting}] ✅ keep (running): ${kept} kept, ${updated} updated, ${deleted} deleted`);
      }
    }
  }

  // Append new rows
  const toAppend = [];
  for (const [slug] of sourceBySlug.entries()) {
    if (seen.has(slug)) continue;
    toAppend.push(slug);
  }
  const appendTotal = toAppend.length;
  console.log(`➕ Appending ${appendTotal} new rows...`);
  let a = 0;
  for (const slug of toAppend) {
    a++;
    const g = sourceBySlug.get(slug);
    const built = buildRowFromSource(g);
    // make sure all regions present (redundant safety)
    ensureAllRegionKeys(built);
    nextRows.push(built);
    appended++;
    if (a <= 5 || a % 25 === 0 || a === appendTotal) {
      console.log(`   [${a}/${appendTotal}] append: ${slug}`);
    }
  }

  // Write outputs
  fs.writeFileSync(OUT_JSON, JSON.stringify(nextRows, null, 2), 'utf8');
  writeCsv(nextRows);

  const dt = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('—'.repeat(60));
  console.log(`🏁 Done in ${dt}s`);
  console.log(`   kept:     ${kept}`);
  console.log(`   updated:  ${updated}`);
  console.log(`   deleted:  ${deleted}`);
  console.log(`   appended: ${appended}`);
  console.log(`📤 Wrote:
   - ${OUT_JSON}
   - ${OUT_CSV}`);
})();
