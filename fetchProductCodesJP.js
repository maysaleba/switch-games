#!/usr/bin/env node
/**
 * JP enrichment (MASTER + SNAPSHOT) - Playwright version
 *
 * MASTER:   data/jp_games_enriched.json
 * SNAPSHOT: data/jp_games_enriched_current.json
 *
 * Preserves original behavior:
 *  - Fetch per-item HTML, extract c_groupCode -> productCode_jp
 *  - Detect English support (supportLanguage='en') when found in product node
 *  - Fill platform from c_labelPlatform: "BEE"->"Nintendo Switch 2", "HAC"->"Nintendo Switch"
 *  - Only fetch network for ACTIVE items (present in today's base) with D-prefixed nsuid
 *  - Periodic saves + debug HTML dump on misses
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const { chromium } = require('playwright');

// -------- CLI --------
const args = process.argv.slice(2);
function getArg(name, def = undefined) {
  const i = args.findIndex(a => a === `--${name}`);
  if (i === -1) return def;
  const val = args[i + 1];
  if (!val || val.startsWith('--')) return true;
  return val;
}

const INPUT_PATH  = getArg('in', 'data/jp_games.json');
const OUT_MASTER  = getArg('out', 'data/jp_games_enriched.json');
const OUT_CURRENT = 'data/jp_games_enriched_current.json';
const CONCURRENCY = Number(getArg('concurrency', 4));
const FORCE       = !!getArg('force', false);

const REQUEST_DELAY_MS = 200;
const RETRIES = 3;
const NAV_TIMEOUT_MS = 30000;
const WAIT_AFTER_LOAD_MS = 1500;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

function ensureDir(p) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
}

// -------- Browser --------
async function createBrowser() {
  return chromium.launch({ headless: true });
}

async function createContext(browser) {
  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    locale: 'ja-JP',
    extraHTTPHeaders: {
      'Accept-Language': 'ja,en;q=0.9',
      'Cache-Control': 'no-cache',
      'Pragma': 'no-cache',
      'Referer': 'https://store-jp.nintendo.com/',
    },
  });

  await context.route('**/*', async (route) => {
    const type = route.request().resourceType();
    if (type === 'image' || type === 'media' || type === 'font') {
      return route.abort();
    }
    return route.continue();
  });

  return context;
}

async function safeGetWithPage(context, url, retries = RETRIES) {
  for (let attempt = 0; attempt < retries; attempt++) {
    const page = await context.newPage();
    try {
      await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: NAV_TIMEOUT_MS,
      });

      await page.waitForTimeout(WAIT_AFTER_LOAD_MS);

      const html = await page.content();
      await page.close();
      return html;
    } catch (err) {
      await page.close().catch(() => {});
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

// -------- Content helpers --------
function hasEnglishSupport(product) {
  const langs = product?.c_original_specification?.supportLanguages || [];
  const norm = new Set(langs.map(x => String(x).trim()));
  return norm.has('en') || norm.has('en_US');
}

function dfsFindGroupCodeAndProduct(node) {
  if (node == null || typeof node !== 'object') return { code: null, productNode: null };
  if (Array.isArray(node)) {
    for (const v of node) {
      const hit = dfsFindGroupCodeAndProduct(v);
      if (hit.code) return hit;
    }
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
    for (const v of node) {
      const found = dfsFindLabelPlatform(v);
      if (found) return found;
    }
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

function extractJPFromHtml(html) {
  const $ = cheerio.load(html);
  const candidates = [];

  $('script[type="application/json"], script[id], script').each((_, el) => {
    const s = ($(el).contents().text() || '').trim();
    if (!s) return;
    try {
      const looksJson =
        (s.startsWith('{') && s.endsWith('}')) ||
        (s.startsWith('[') && s.endsWith(']'));
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
      const label = dfsFindLabelPlatform(j);
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

// -------- Simple concurrency pool --------
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

// -------- Non-destructive helpers --------
const isNonEmpty = (v) => (typeof v === 'string' ? v.trim() !== '' : v != null);

function pick(a, b) {
  const sa = (a ?? '').toString().trim();
  const sb = (b ?? '').toString().trim();
  return sa ? a : (sb ? b : a);
}

// -------- Main --------
(async function main() {
  const baseInput = readJsonSafe(INPUT_PATH, []);
  if (!Array.isArray(baseInput) || baseInput.length === 0) {
    console.error(`No data in ${INPUT_PATH}. Make sure it exists and is a JSON array.`);
    process.exit(1);
  }

  const inBase = new Map();
  const activeDPref = new Set();
  for (const g of baseInput) {
    const nsuid = String(g?.nsuid_jp || g?.nsuid || '');
    if (!nsuid) continue;
    inBase.set(nsuid, g);
    if (/^D/i.test(nsuid)) activeDPref.add(nsuid);
  }

  const existing = readJsonSafe(OUT_MASTER, []);
  const existingById = new Map();
  for (const row of Array.isArray(existing) ? existing : []) {
    const nsuid = row?.nsuid_jp ? String(row.nsuid_jp) : (row?.nsuid ? String(row.nsuid) : null);
    if (nsuid) existingById.set(nsuid, row);
  }

  const unionIds = new Set([...inBase.keys(), ...existingById.keys()]);
  const working = [];
  const now = nowIso();

  for (const id of unionIds) {
    const base = inBase.get(id) || {};
    const prior = existingById.get(id) || {};

    const merged = { ...prior };

    if (isNonEmpty(base.title)) merged.title = base.title;
    if (isNonEmpty(base.url)) merged.url = base.url;
    if (isNonEmpty(base.urlKey)) merged.urlKey = base.urlKey;
    merged.platform    = pick(base.platform, merged.platform);
    merged.genres      = (Array.isArray(base.genres) && base.genres.length) ? base.genres : (Array.isArray(merged.genres) ? merged.genres : []);
    merged.releaseDate = pick(base.releaseDate, merged.releaseDate);
    merged.imageSquare = pick(base.imageSquare, merged.imageSquare);
    merged.imageKey    = pick(base.imageKey, merged.imageKey);
    merged.publisher   = pick(base.publisher, merged.publisher);
    merged.dlcType     = pick(base.dlcType, merged.dlcType);
    merged.playerCount = pick(base.playerCount, merged.playerCount);

    merged.nsuid_jp = pick(base.nsuid_jp, merged.nsuid_jp) || pick(base.nsuid, merged.nsuid);
    merged.productCode_jp = pick(base.productCode_jp, merged.productCode_jp);

    if (isNonEmpty(base.supportLanguage) && !merged.supportLanguage) {
      merged.supportLanguage = base.supportLanguage;
    }

    merged.active_in_base = inBase.has(id);
    merged.first_seen_at = merged.first_seen_at || prior.first_seen_at || now;
    if (merged.active_in_base) {
      merged.last_seen_at = now;
    } else {
      merged.last_seen_at = merged.last_seen_at || now;
    }

    working.push(merged);
  }

  const toProcess = working.filter(row => {
    if (!row.active_in_base) return false;
    const nsuid = String(row.nsuid_jp || row.nsuid || '');
    if (!/^D/i.test(nsuid)) return false;
    if (!FORCE && typeof row.productCode_jp === 'string' && row.productCode_jp.trim() !== '') {
      if (String(row.platform || '') !== '') return false;
      return true;
    }
    return true;
  });

  console.log(`JP union=${working.length} active=${inBase.size} fetchCandidates=${toProcess.length}`);

  const idxById = new Map();
  for (let i = 0; i < working.length; i++) {
    const id = String(working[i].nsuid_jp || working[i].nsuid || '');
    if (id) idxById.set(id, i);
  }

  let processed = 0, updated = 0, skipped = 0, failed = 0;

  const browser = await createBrowser();
  const context = await createContext(browser);

  try {
    await pool(toProcess, CONCURRENCY, async (row) => {
      const nsuid = String(row.nsuid_jp || row.nsuid || '');
      const needPlatform = String(row.platform || '') === '';
      const haveCode = typeof row.productCode_jp === 'string' && row.productCode_jp.trim() !== '';

      if (!FORCE && haveCode && !needPlatform) {
        skipped++;
        processed++;
        return;
      }

      const url = buildItemUrl(nsuid);

      try {
        const jitter = Math.floor(Math.random() * 600);
        await sleep(REQUEST_DELAY_MS + jitter);

        const html = await safeGetWithPage(context, url);
        const { productCode, english, platform } = extractJPFromHtml(html);

        const i = idxById.get(nsuid);
        if (i != null) {
          if (productCode) {
            working[i].productCode_jp = productCode;
            if (english && !working[i].supportLanguage) working[i].supportLanguage = 'en';
            updated++;
            console.log(`✅ ${nsuid} → ${productCode}${english ? ' (en)' : ''}${platform ? ` [${platform}]` : ''}`);
          } else {
            if (needPlatform && platform) {
              console.log(`ℹ️ Platform-only filled: ${nsuid} [${platform}]`);
            } else {
              ensureDir(`debug_html_jp/${nsuid}.html`);
              fs.writeFileSync(`debug_html_jp/${nsuid}.html`, html);
              console.warn(`⚠️ No productCode_jp found for ${nsuid} (${url}). Saved HTML.`);
              failed++;
            }
          }

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
          writePrettyJson(OUT_MASTER, working);
          const current = working.filter(e => e.active_in_base);
          writePrettyJson(OUT_CURRENT, current);
          console.log(`💾 Progress saved (${processed}/${toProcess.length})`);
        }
      }
    });
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }

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
