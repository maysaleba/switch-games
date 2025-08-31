#!/usr/bin/env node
/**
 * Fetch JP product group codes (c_groupCode) for NSUIDs from jp_games.json
 * and write jp_games_enriched.json with appended:
 *   - productCode_jp: <c_groupCode without underscores, e.g., "HACPAEUC">
 *   - supportLanguage: "en"  (iff supportLanguages contains "en" or "en_US")
 *
 * Requirements:
 *   - token.txt  (contains the Bearer token on a single line)
 *   - jp_games.json (array of objects with at least { nsuid_jp, ... })
 *
 * Resumable:
 *   - If jp_games_enriched.json exists, preserves existing data and skips
 *     entries that already have productCode_jp (non-null).
 *   - Re-reads token.txt before each request, so you can replace the token
 *     if rate-limited or expired; the script will continue automatically.
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios');

const INPUT_PATH  = path.resolve(__dirname, 'jp_games.json');
const OUTPUT_PATH = path.resolve(__dirname, 'jp_games_enriched.json');
const TOKEN_PATH  = path.resolve(__dirname, 'token.txt');

// API constants
const BASE_URL = 'https://store-jp.nintendo.com/mobify/proxy/api/product/shopper-products/v1/organizations/f_ecom_bfgj_prd/products';
const DEFAULT_SITE_ID = 'MNS'; // primary JP site
const FALLBACK_SITE_ID = 'JPS'; // fallback if batch empty
const FIXED_PARAMS = {
  currency: 'JPY',
  locale: 'ja-JP',
  siteId: DEFAULT_SITE_ID,
};
const BATCH_SIZE = 20;

// Backoff settings
const MAX_RETRIES = 5;
const BASE_DELAY_MS = 1000; // 1s base
const JITTER_MS = 250;

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

function readToken() {
  try {
    const raw = fs.readFileSync(TOKEN_PATH, 'utf8');
    // strip BOM + trim
    return raw.replace(/^\uFEFF/, '').trim();
  } catch (e) {
    throw new Error(`Missing or unreadable token file at ${TOKEN_PATH}`);
  }
}

function safeLoadJson(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    console.warn(`⚠️ Could not parse ${filePath}. Using fallback.`, e.message);
    return fallback;
  }
}

function persistJson(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// IDs are passed AS-IS (already prefixed with D). No prepending.
function buildIdsParam(nsuidList) {
  return nsuidList.join(',');
}

function hasEnglishSupport(product) {
  const langs = product?.c_original_specification?.supportLanguages || [];
  const norm = new Set(langs.map(x => String(x).trim()));
  return norm.has('en') || norm.has('en_US');
}

// Normalize different response shapes to a flat product array
function normalizeProductsPayload(payload) {
  if (Array.isArray(payload)) return payload;
  if (payload && Array.isArray(payload.data)) return payload.data;
  if (payload && payload.data && Array.isArray(payload.data.data)) return payload.data.data;
  return [];
}

async function doRequest(idsParam, siteId) {
  const token = readToken(); // re-read every attempt to allow hot-swap

  const fullUrl = `${BASE_URL}?ids=${encodeURIComponent(idsParam)}&currency=JPY&locale=ja-JP&siteId=${siteId}`;
  console.log(`🌐 Requesting: ${fullUrl}`);

  return axios.get(BASE_URL, {
    params: { ...FIXED_PARAMS, siteId, ids: idsParam },
    headers: {
      'Authorization': `${token}`,
      'Accept': 'application/json',
    },
    timeout: 20000,
    validateStatus: s => (s >= 200 && s < 300) || s === 429 || s === 401 || s === 403,
  });
}

async function fetchBatch(nsuidList) {
  const idsParam = buildIdsParam(nsuidList);

  let attempt = 0;
  while (true) {
    try {
      // First try the primary site
      let res = await doRequest(idsParam, DEFAULT_SITE_ID);

      if (res.status >= 200 && res.status < 300) {
        let products = normalizeProductsPayload(res.data);

        // If nothing came back, try the fallback site once
        if (!products.length) {
          const alt = await doRequest(idsParam, FALLBACK_SITE_ID);
          if (alt.status >= 200 && alt.status < 300) {
            products = normalizeProductsPayload(alt.data);
          } else {
            res = alt; // re-use for status handling below
          }
        }

        return products;
      }

      // Handle rate-limit or auth issues gracefully
      let delay = BASE_DELAY_MS * Math.pow(2, attempt) + Math.floor(Math.random() * JITTER_MS);

      if (res.status === 429) {
        const retryAfter = Number(res.headers['retry-after']);
        if (!Number.isNaN(retryAfter) && retryAfter > 0) {
          delay = Math.max(delay, retryAfter * 1000);
        }
        console.warn(`⏳ 429 Rate limited. Backing off ~${Math.round(delay)}ms then retrying...`);
      } else if (res.status === 401 || res.status === 403) {
        console.warn(`🔐 ${res.status} Unauthorized/Forbidden. Re-read token.txt and retrying in ${Math.round(delay)}ms...`);
      } else {
        throw new Error(`Unexpected status: ${res.status}`);
      }

      attempt++;
      if (attempt > MAX_RETRIES) {
        throw new Error(`Exceeded max retries for batch ids=${idsParam.slice(0, 80)}...`);
      }
      await sleep(delay);

    } catch (err) {
      attempt++;
      if (attempt > MAX_RETRIES) {
        throw err;
      }
      const delay = BASE_DELAY_MS * Math.pow(2, attempt) + Math.floor(Math.random() * JITTER_MS);
      console.warn(`⚠️ Error on batch request (attempt ${attempt}/${MAX_RETRIES}). Retrying in ~${Math.round(delay)}ms...`);
      await sleep(delay);
    }
  }
}

// Prefer digital (D) over physical (P) if both come back for the same master NSUID
function indexProductsByNsuid(products) {
  const map = new Map();
  for (const p of Array.isArray(products) ? products : []) {
    const pid = String(p?.id || '');                // e.g., "D7001…" or "P7001…"
    const masterId = pid.length > 1 ? pid.slice(1) : pid; // drop D/P prefix

    let cGroup = p?.c_groupCode ?? null;
    if (cGroup) cGroup = cGroup.replace(/_/g, '');

    const supportsEN = hasEnglishSupport(p);
    const currSrc = pid.startsWith('D') ? 'D' : (pid.startsWith('P') ? 'P' : '?');

    if (!map.has(masterId)) {
      map.set(masterId, {
        productCode_jp: cGroup,
        supportLanguage: supportsEN ? 'en' : undefined,
        _src: currSrc,
      });
    } else {
      const prev = map.get(masterId);
      // Upgrade to digital if previous was physical
      if (prev._src !== 'D' && currSrc === 'D') {
        map.set(masterId, {
          productCode_jp: cGroup,
          supportLanguage: supportsEN ? 'en' : undefined,
          _src: 'D',
        });
      }
    }
  }
  return map;
}

(async () => {
  if (!fs.existsSync(INPUT_PATH)) {
    console.error(`❌ Input not found: ${INPUT_PATH}`);
    process.exit(1);
  }

  const input = safeLoadJson(INPUT_PATH, []);
  if (!Array.isArray(input)) {
    console.error('❌ jp_games.json must be an array of objects.');
    process.exit(1);
  }

  const existing = safeLoadJson(OUTPUT_PATH, null);
  let enriched = Array.isArray(existing) ? existing : input;

  // Index by nsuid (full, prefixed) for resume/merge
  const byNsuid = new Map();
  for (const item of enriched) {
    const nsuid = String(item.nsuid_jp || '').trim();
    if (nsuid) byNsuid.set(nsuid, item);
  }

  // Determine which need fetching (only entries starting with 'D'; skip weird keys like BH_...)
  // Also skip if productCode_jp already present and non-null
  const pendingNsuid = [];
  for (const item of input) {
    const raw = String(item.nsuid_jp || '').trim();
    if (!raw) continue;

    if (!raw.startsWith('D')) {
      // Optional: mark as not applicable if you want
      // const curr = byNsuid.get(raw) || item;
      // if (!Object.prototype.hasOwnProperty.call(curr, 'productCode_jp')) curr.productCode_jp = null;
      // byNsuid.set(raw, curr);
      continue;
    }

    const current = byNsuid.get(raw) || item;
    const already = Object.prototype.hasOwnProperty.call(current, 'productCode_jp') && current.productCode_jp !== null;
    if (!already) {
      // Push AS-IS, including the 'D' prefix
      pendingNsuid.push(raw);
    }
  }

  const total = input.length;
  const toFetch = pendingNsuid.length;
  console.log(`ℹ️ Input: ${total} | Already have: ${total - toFetch} | To fetch: ${toFetch}`);

  const batches = chunk(pendingNsuid, BATCH_SIZE);
  let done = 0;

  for (let i = 0; i < batches.length; i++) {
    const slice = batches[i];
    console.log(`➡️  Batch ${i + 1}/${batches.length} | Fetching ${slice.length} ids...`);

    try {
      const raw = await fetchBatch(slice);
      const products = normalizeProductsPayload(raw);

      if (!products.length) {
        console.warn(`   ⚠️ API returned 0 products for ids: ${slice.join(',')}`);
      } else {
        // Peek a tiny sample to verify shape/content
        const sample = products.slice(0, 3).map(p => ({
          id: p?.id,
          c_groupCode: p?.c_groupCode
        }));
        console.log('   ℹ️ sample returned:', sample);
      }

      const map = indexProductsByNsuid(products);

      // Merge back
      for (const nsuid of slice) {
        const master = (nsuid.startsWith('D') || nsuid.startsWith('P')) ? nsuid.slice(1) : nsuid;
        const found = map.get(master);
        const existingItem = byNsuid.get(nsuid) || { nsuid_jp: nsuid };

        if (found) {
          existingItem.productCode_jp = found.productCode_jp ?? null;
          if (found.supportLanguage) {
            existingItem.supportLanguage = 'en';
          }
          console.log(`   ↳ NSUID ${nsuid}: productCode_jp = ${existingItem.productCode_jp}`);
        } else {
          if (!Object.prototype.hasOwnProperty.call(existingItem, 'productCode_jp')) {
            existingItem.productCode_jp = null;
          }
          console.log(`   ↳ NSUID ${nsuid}: productCode_jp = null (not found)`);
        }

        byNsuid.set(nsuid, existingItem);
      }

      // Rebuild enriched in original order (preserve all fields)
      enriched = input.map(src => {
        const key = String(src.nsuid_jp || '').trim();
        return { ...src, ...(byNsuid.get(key) || {}) };
      });

      persistJson(OUTPUT_PATH, enriched);

      done += slice.length;
      console.log(`✅ Saved to ${OUTPUT_PATH} | Progress: ${done}/${toFetch} (this run)`);

    } catch (err) {
      // Diagnostics only; fetchBatch already handled retries/backoff
      const status = err?.response?.status;
      const code   = err?.code;
      const url    = err?.config?.url;
      console.warn(
        `⚠️ Error on batch processing`,
        status ? `status=${status}` : '',
        code ? `code=${code}` : '',
        url ? `url=${url}` : ''
      );

      if (err?.response?.data) {
        try {
          const body = typeof err.response.data === 'string'
            ? err.response.data.slice(0, 500)
            : JSON.stringify(err.response.data).slice(0, 500);
          console.warn('   body:', body);
        } catch (e) {
          console.warn('   (could not stringify response body)');
        }
      }

      if (err?.message) console.warn('   message:', err.message);
      console.warn('   (no outer retry; fetchBatch already retried this slice)');
    }
  }

  console.log(`🎉 Done. Enriched file: ${OUTPUT_PATH}`);
})();
