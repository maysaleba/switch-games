#!/usr/bin/env node
/**
 * Data-driven merge for US base with N regions (e.g., jp/hk/eu[/kr]).
 * Matching rules:
 *   - k1/k2/k3 (NSUID first4 + productCode variants) → require SAME platform
 *   - title fallback                                → require SAME platform
 *   - urlKey (k4)                                   → IGNORES platform; uses *loose* key (hyphens removed)
 *       + Safety: prefer candidates with the same NSUID first4 when multiple items share the same loose key.
 *
 * active_in_base OR-in logic:
 *   - If ANY matched source has active_in_base === true → merged.active_in_base = true.
 *   - We also track WHICH regions have active_in_base === true (activeRegions set).
 *   - At the end of each row, we PRUNE nsuid_* fields to ONLY those regions present in activeRegions.
 *
 * NEW IN THIS VERSION:
 *   - Prune nsuid_* so ONLY regions where active_in_base === true remain.
 *   - Strict urlKey mode driven by external file: config/strict_urlkeys.txt (one urlKey per line; '#' comments allowed)
 *     If a US row’s urlKey is listed there, we ONLY match by urlKey (loose) and skip k1/k2/k3/title rules for that row.
 */

const fs = require('fs');
const path = require('path');

// ====== CONFIG ======
const INPUTS = {
  us: 'data/us_games_enriched.json',
  jp: 'data/jp_games_enriched.json',
  hk: 'data/hk_games_enriched.json',
  eu: 'data/eu_games_enriched.json',
//  kr: 'data/kr_games_enriched.json', // <- add when ready
};

const REGIONS = ['jp', 'hk', 'eu'];                // non-US regions we try to match
const REGION_CODES_ALL = ['us', 'jp', 'hk', 'eu']; // used for pruning nsuid_*
const OUT_DIR = 'output';
const LOG_DIR = 'logs';
const OUT_FILE = path.join(OUT_DIR, 'merged_enriched.json');
const LOG_FILE = path.join(LOG_DIR, 'merge_unmatched.log');

const ENABLE_TITLE_FALLBACK = true;

// NEW: optional external file for strict urlKey-only matching
const STRICT_URLKEY_FILE = 'strict_urlkeys.txt';

// ====== utils ======
function readJson(p) { return JSON.parse(fs.readFileSync(p, 'utf8')); }
function ensureDir(d) { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); }
function uc(s) { return (s ?? '').toString().trim().toUpperCase(); }
function isNonEmpty(s) { return s != null && (typeof s !== 'string' || s.trim().length > 0); }

function normalizeNsuid(region, nsuidRaw) {
  if (!nsuidRaw) return null;
  let s = String(nsuidRaw).trim();
  if (region === 'jp' && s.startsWith('D')) s = s.slice(1); // strip 'D' for JP
  return s;
}
function first4DigitsFromNsuid(nsuidRaw, region) {
  const nsuid = normalizeNsuid(region, nsuidRaw);
  if (!nsuid) return null;
  const digits = nsuid.replace(/\D+/g, '');
  return digits.length >= 4 ? digits.slice(0, 4) : null;
}

function pcFirst8(pc) { const s = uc(pc); return s.length >= 8 ? s.slice(0, 8) : null; }
function pcPos4to8(pc) { const s = uc(pc); return s.length >= 8 ? s.slice(3, 8) : null; }

function getUrlKeyRaw(item) { return item?.urlKey ?? item?.slug ?? item?.url_key ?? null; }
function normalizeSlug(s) {
  if (!s) return null;
  let t = String(s).trim().toLowerCase()
    .replace(/^https?:\/\/[^/]+/g, '')
    .replace(/^\/+|\/+$/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  return t || null;
}
function getUrlKey(item) {
  const raw = getUrlKeyRaw(item);
  if (raw) return normalizeSlug(raw);
  const url = item?.url;
  if (url) {
    const m = String(url).match(/\/([^/?#]+?)(?:\.html)?(?:\?|#|$)/i);
    if (m && m[1]) return normalizeSlug(m[1]);
  }
  return null;
}

// ---- loose URL key (remove hyphens) ----
function urlKeyLooseFromItem(item) {
  const k = getUrlKey(item);
  return k ? k.replace(/-/g, '') : null;
}

function normalizeTitle(s) {
  if (!s) return null;
  return String(s).toLowerCase().normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[’']/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// ---- platform helpers ----
function normalizePlatform(s) {
  if (!s) return null;
  return String(s).trim().toLowerCase(); // strict equality
}
function getPlatform(item) {
  return normalizePlatform(item?.platform);
}

function supportLangField(region) { return `supportLanguage_${region}`; }
function pickSupportLanguage(item) {
  if (Array.isArray(item?.supportLanguages)) {
    const norm = new Set(item.supportLanguages.map(x => String(x).trim()));
    if (norm.has('en') || norm.has('en_US')) return 'en';
  }
  if (isNonEmpty(item?.supportLanguage)) return String(item.supportLanguage).trim();
  if (Array.isArray(item?.c_original_specification?.supportLanguages)) {
    const norm = new Set(item.c_original_specification.supportLanguages.map(x => String(x).trim()));
    if (norm.has('en') || norm.has('en_US')) return 'en';
  }
  return null;
}

function sortKeysPretty(item) {
  const preferred = [
    'title','url','urlKey','platform','genres','releaseDate','publisher','dlcType','playerCount',
    'imageSquare','imageSquare_us','imageSquare_eu','imageSquare_jp','imageSquare_hk','imageSquare_kr','imageKey',
    'nsuid_us','nsuid_eu','nsuid_jp','nsuid_hk','nsuid_kr',
    'productCode_us','productCode_eu','productCode_jp','productCode_hk','productCode_kr',
    'supportLanguage_us','supportLanguage_eu','supportLanguage_jp','supportLanguage_hk','supportLanguage_kr',
    'active_in_base',
  ];
  const out = {};
  for (const k of preferred) if (Object.prototype.hasOwnProperty.call(item, k)) out[k] = item[k];
  for (const k of Object.keys(item)) if (!Object.prototype.hasOwnProperty.call(out, k)) out[k] = item[k];
  return out;
}

// ====== indexing per region ======
function buildRegionIndexes(arr, region) {
  const k1 = new Map();
  const k2 = new Map();
  const k3 = new Map();
  const k4 = new Map(); // loose urlKey → array of items
  const kt = new Map();

  for (const item of arr) {
    const nsuid = item[`nsuid_${region}`] ?? item.nsuid ?? null;
    const f4 = first4DigitsFromNsuid(nsuid, region);
    const pc = item[`productCode_${region}`] ?? item.productCode ?? null;
    const urlKeyL = urlKeyLooseFromItem(item);
    const titleN = ENABLE_TITLE_FALLBACK ? normalizeTitle(item?.title) : null;
    const plat = getPlatform(item); // may be null/empty

    if (f4 && isNonEmpty(pc) && plat) {
      const pcU = uc(pc);
      const key1 = `${f4}|${pcU}|${plat}`; if (!k1.has(key1)) k1.set(key1, item);
      const pc8 = pcFirst8(pcU);   if (pc8) { const key2 = `${f4}|${pc8}|${plat}`; if (!k2.has(key2)) k2.set(key2, item); }
      const pc48 = pcPos4to8(pcU); if (pc48){ const key3 = `${f4}|${pc48}|${plat}`; if (!k3.has(key3)) k3.set(key3, item); }
    }

    if (urlKeyL) {
      const arrL = k4.get(urlKeyL);
      if (arrL) arrL.push(item);
      else k4.set(urlKeyL, [item]);
    }

    if (titleN && plat) {
      const keyt = `${titleN}|${plat}`;
      if (!kt.has(keyt)) kt.set(keyt, item);
    }
  }
  return { k1, k2, k3, k4, kt };
}

// ====== strict urlKey list ======
function loadStrictUrlKeys(filePath) {
  if (!fs.existsSync(filePath)) {
    console.log(`ℹ️ No strict urlKey file found at ${filePath}. Running without strict list.`);
    return new Set();
  }
  const raw = fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(x => x && !x.startsWith('#'));
  const normalized = raw.map(normalizeSlug).filter(Boolean);
  const set = new Set(normalized);
  console.log(`🔑 Loaded ${set.size} strict urlKeys from ${filePath}`);
  return set;
}

let STRICT_URLKEYS = loadStrictUrlKeys(STRICT_URLKEY_FILE);

// ====== matching ======
function tryMatch(regionIdx, usItem, { strictUrlKeyHit }) {
  const urlKeyUS = getUrlKey(usItem);
  const urlKeyUSL = urlKeyUS ? urlKeyUS.replace(/-/g, '') : null;

  // --- STRICT URLKEY-ONLY MODE (per-row) ---
  if (strictUrlKeyHit && urlKeyUS && urlKeyUSL) {
    if (regionIdx.k4.has(urlKeyUSL)) {
      const candidates = regionIdx.k4.get(urlKeyUSL) || [];
      if (candidates.length === 1) return { item: candidates[0], rule: 'strict-urlKey' };
      // Prefer a candidate sharing NSUID first4 with the US item (if any)
      const f4US =
        first4DigitsFromNsuid(usItem.nsuid_us ?? usItem.nsuid, 'us') ||
        null;
      if (f4US) {
        for (const cand of candidates) {
          const candF4 =
            first4DigitsFromNsuid(cand.nsuid_us ?? cand.nsuid, 'us') ||
            first4DigitsFromNsuid(cand.nsuid_eu ?? cand.nsuid, 'eu') ||
            first4DigitsFromNsuid(cand.nsuid_jp ?? cand.nsuid, 'jp') ||
            first4DigitsFromNsuid(cand.nsuid_hk ?? cand.nsuid, 'hk') ||
            first4DigitsFromNsuid(cand.nsuid_kr ?? cand.nsuid, 'kr') ||
            (String(cand.nsuid ?? '').replace(/\D+/g, '').slice(0,4) || null);
          if (candF4 && candF4 === f4US) return { item: cand, rule: 'strict-urlKey' };
        }
      }
      // Fallback to first candidate deterministically
      return { item: candidates[0], rule: 'strict-urlKey' };
    }
    // Strict list says urlKey-only, but no regional candidate → no match (do NOT fall back)
    return null;
  }

  // --- NORMAL LOGIC (k1/k2/k3 → urlKey → title) ---
  const usNsuid = usItem.nsuid_us ?? usItem.nsuid;
  const f4 = first4DigitsFromNsuid(usNsuid, 'us');
  const pcUS = usItem.productCode_us ?? usItem.productCode ?? null;
  const titleUS = ENABLE_TITLE_FALLBACK ? normalizeTitle(usItem?.title) : null;
  const platUS = getPlatform(usItem); // may be null

  if (f4 && isNonEmpty(pcUS) && platUS) {
    const pcU = uc(pcUS);
    const key1 = `${f4}|${pcU}|${platUS}`; if (regionIdx.k1.has(key1)) return { item: regionIdx.k1.get(key1), rule: 'k1' };
    const pc8 = pcFirst8(pcU);             if (pc8) { const key2 = `${f4}|${pc8}|${platUS}`; if (regionIdx.k2.has(key2)) return { item: regionIdx.k2.get(key2), rule: 'k2' }; }
    const pc48 = pcPos4to8(pcU);           if (pc48){ const key3 = `${f4}|${pc48}|${platUS}`; if (regionIdx.k3.has(key3)) return { item: regionIdx.k3.get(key3), rule: 'k3' }; }
  }

  if (urlKeyUSL && regionIdx.k4.has(urlKeyUSL)) {
    const candidates = regionIdx.k4.get(urlKeyUSL) || [];
    if (candidates.length === 1) return { item: candidates[0], rule: 'k4' };
    if (f4) {
      for (const cand of candidates) {
        const candF4 =
          first4DigitsFromNsuid(cand.nsuid_us ?? cand.nsuid, 'us') ||
          first4DigitsFromNsuid(cand.nsuid_eu ?? cand.nsuid, 'eu') ||
          first4DigitsFromNsuid(cand.nsuid_jp ?? cand.nsuid, 'jp') ||
          first4DigitsFromNsuid(cand.nsuid_hk ?? cand.nsuid, 'hk') ||
          first4DigitsFromNsuid(cand.nsuid_kr ?? cand.nsuid, 'kr') ||
          (String(cand.nsuid ?? '').replace(/\D+/g, '').slice(0,4) || null);
        if (candF4 && candF4 === f4) return { item: cand, rule: 'k4' };
      }
    }
    return { item: candidates[0], rule: 'k4' };
  }

  if (titleUS && platUS) {
    const keyt = `${titleUS}|${platUS}`;
    if (regionIdx.kt?.has(keyt)) return { item: regionIdx.kt.get(keyt), rule: 'title' };
  }

  return null;
}

// ---- append + active tracking
function appendRegionFields(base, region, matched, { onRaise } = {}) {
  const nsuidKey = `nsuid_${region}`;
  const pcodeKey = `productCode_${region}`;
  const slKey = supportLangField(region);

  let nsuidVal = matched?.[nsuidKey] ?? matched?.nsuid ?? null;
  const pcodeVal = matched?.[pcodeKey] ?? matched?.productCode ?? null;
  const supLang = pickSupportLanguage(matched);
  const platformVal = matched?.platform ?? null;

  if (nsuidVal) nsuidVal = normalizeNsuid(region, nsuidVal);

  if (isNonEmpty(nsuidVal)) base[nsuidKey] = String(nsuidVal).trim();
  if (isNonEmpty(pcodeVal)) base[pcodeKey] = String(pcodeVal).trim().toUpperCase();
  if (isNonEmpty(supLang)) base[slKey] = String(supLang).trim();

  if (!isNonEmpty(base.platform) && isNonEmpty(platformVal)) {
    base.platform = String(platformVal).trim();
  }

  // also copy region-specific imageSquare if present
  const imgSqKey = `imageSquare_${region}`;
  const imgSqVal = matched?.imageSquare ?? matched?.image_square ?? null;
  if (isNonEmpty(imgSqVal) && !isNonEmpty(base[imgSqKey])) {
    base[imgSqKey] = String(imgSqVal).trim();
  }

  // Flag active + track region that is active
  if (matched && matched.active_in_base === true) {
    if (base.active_in_base !== true && typeof onRaise === 'function') onRaise();
    base.active_in_base = true;
    // track region activity for pruning
    base.__activeRegions?.add(region);
  }

  // Keep note if this region contributed (debugging)
  if (!base.__matchedRegions) base.__matchedRegions = new Set();
  base.__matchedRegions.add(region);
}

// ====== main ======
(function main() {
  // Load sources
  const us = readJson(INPUTS.us);
  const US_BASE_COUNT = Array.isArray(us) ? us.length : 0;

  const regionData = {};
  for (const r of REGIONS) regionData[r] = readJson(INPUTS[r]);

  // Build indexes
  const regionIdx = {};
  const seen = {};
  const ruleCounts = {};
  for (const r of REGIONS) {
    regionIdx[r] = buildRegionIndexes(regionData[r], r);
    seen[r] = new Set();
    ruleCounts[r] = { 'strict-urlKey':0, k1:0, k2:0, k3:0, k4:0, title:0 };
  }

  // RAISE LOGGING ACCUMULATORS
  const activeRaiseEvents = [];
  const activeRaiseCounts = Object.fromEntries(REGIONS.map(r => [r, 0]));

  // Merge pass over US
  const merged = us.map((orig, rowId) => {
    const item = { ...orig, __rowId: rowId };
    const wasActiveInitially = orig.active_in_base === true;

    // per-row set of regions that are "active"
    item.__activeRegions = new Set();
    if (orig.active_in_base === true) item.__activeRegions.add('us');

    // Determine if this US row is in strict urlKey list
    const uk = getUrlKey(item);
    const strictHit = !!(uk && STRICT_URLKEYS.has(uk));

    for (const r of REGIONS) {
      const res = tryMatch(regionIdx[r], item, { strictUrlKeyHit: strictHit });
      if (res?.item) {
        appendRegionFields(item, r, res.item, {
          onRaise: () => {
            if (!wasActiveInitially) {
              activeRaiseCounts[r] += 1;
              const title = item.title ?  `"${item.title}"` : '';
              const usNsuid = item.nsuid_us ?? item.nsuid ?? 'N/A';
              activeRaiseEvents.push(
                `raised active_in_base by ${r.toUpperCase()} for rowId ${rowId} ${title} (US nsuid: ${usNsuid})`
              );
            }
          }
        });

        const idN = normalizeNsuid(r, (res.item[`nsuid_${r}`] ?? res.item.nsuid ?? null));
        if (idN) seen[r].add(idN);
        if (ruleCounts[r][res.rule] != null) ruleCounts[r][res.rule] += 1;
      }
    }

    // === PRUNE nsuid_* to ONLY regions in __activeRegions
    for (const rc of REGION_CODES_ALL) {
      const key = `nsuid_${rc}`;
      if (Object.prototype.hasOwnProperty.call(item, key)) {
        if (!item.__activeRegions.has(rc)) {
          delete item[key];
        }
      }
    }

    // housekeeping
    delete item.__matchedRegions;
    delete item.__activeRegions;

    // ensure urlKey is present (normalize if needed)
    if (!isNonEmpty(item.urlKey)) {
      const ukNow = getUrlKey(item);
      if (ukNow) item.urlKey = ukNow;
    }

    return sortKeysPretty(item);
  });

  // Count unmatched to US (no nsuid_/productCode_ present in merged rows)
  const usToUnmatched = {};
  for (const r of REGIONS) {
    usToUnmatched[r] = merged.filter(x => !x[`nsuid_${r}`] && !x[`productCode_${r}`]).length;
  }

  // Compute leftovers (region entries not matched to any US)
  const leftovers = {};
  for (const r of REGIONS) {
    leftovers[r] = regionData[r].filter(g => {
      const idN = normalizeNsuid(r, (g[`nsuid_${r}`] ?? g.nsuid ?? null));
      return idN && !seen[r].has(idN);
    });
  }

  // ---- Append unmatched EU entries verbatim
  const euLeftovers = leftovers['eu'] || [];
  for (const g of euLeftovers) {
    const clone = { ...g };
    if (!isNonEmpty(clone.urlKey)) {
      const uk = getUrlKey(g);
      if (isNonEmpty(uk)) clone.urlKey = uk;
    }
    // keep as-is; imageSquare_eu is inherently present via clone.imageSquare if consumer needs it
    merged.push(sortKeysPretty(clone));
  }
  console.log(`➕ Appended EU-only entries: ${euLeftovers.length}`);

  // write outputs
  ensureDir(OUT_DIR); ensureDir(LOG_DIR);
  const clean = merged.map(({ __rowId, __nomatch_debug, ...rest }) => rest);
  fs.writeFileSync(OUT_FILE, JSON.stringify(clean, null, 2), 'utf8');

  // log
  const totalAfterAppend = clean.length;
  const logLines = [];
  logLines.push(`EU-only entries appended to final output: ${euLeftovers.length}`);
  logLines.push(`Merge run @ ${new Date().toISOString()}`);
  logLines.push(`US base entries: ${US_BASE_COUNT}`);
  logLines.push(`Final output total (after EU append): ${totalAfterAppend}`);
  for (const r of REGIONS) {
    const matchedCount = US_BASE_COUNT - usToUnmatched[r];
    logLines.push(`Matched ${r.toUpperCase()}: ${matchedCount} / ${US_BASE_COUNT}`);
  }
  logLines.push('');

  for (const r of REGIONS) {
    logLines.push(`${r.toUpperCase()} entries not matched to any US: ${leftovers[r].length}`);
  }
  logLines.push('');

  logLines.push('Rule usage (how matches were made):');
  for (const r of REGIONS) logLines.push(`  ${r.toUpperCase()}: ${JSON.stringify(ruleCounts[r])}`);
  logLines.push('');

  logLines.push('active_in_base raises (region flipped merged value from false→true):');
  logLines.push(`  by region counts: ${JSON.stringify(activeRaiseCounts)}`);
  if (activeRaiseEvents.length) {
    for (const line of activeRaiseEvents) logLines.push(`  - ${line}`);
  } else {
    logLines.push('  (none)');
  }
  logLines.push('');

  for (const r of REGIONS) {
    logLines.push(`--- ${r.toUpperCase()} entries not matched to any US ---`);
    for (const g of leftovers[r]) {
      const row = {
        [`nsuid_${r}`]: g[`nsuid_${r}`] ?? g.nsuid ?? null,
        [`productCode_${r}`]: g[`productCode_${r}`] ?? g.productCode ?? null,
        urlKey: getUrlKey(g),
        title: g.title ?? null,
        platform: g.platform ?? null,
        active_in_base: g.active_in_base === true ? true : undefined,
      };
      logLines.push(JSON.stringify(row));
    }
    logLines.push('');
  }

  fs.writeFileSync(LOG_FILE, logLines.join('\n'), 'utf8');

  // concise console summary
  console.log(`✅ Merged to ${OUT_FILE}`);
  console.log(`ℹ️ US base: ${US_BASE_COUNT} | EU-only appended: ${euLeftovers.length} | Final total: ${totalAfterAppend}`);
  for (const r of REGIONS) {
    const matchedCount = US_BASE_COUNT - usToUnmatched[r];
    console.log(`✅ Matched ${r.toUpperCase()}: ${matchedCount} / ${US_BASE_COUNT} | 🚫 ${r.toUpperCase()}→US unmatched: ${leftovers[r].length}`);
  }
  console.log(`⬆️ active_in_base raises by region: ${JSON.stringify(activeRaiseCounts)}`);
  console.log(`📝 nsuid_* pruning: kept only regions with active_in_base === true`);
  console.log(`📝 Log written to ${LOG_FILE}`);
})();
