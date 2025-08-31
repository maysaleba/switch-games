#!/usr/bin/env node
/**
 * HK enrichment (MASTER + SNAPSHOT, non-destructive merge, "" sentinel preserved)
 *
 * - MASTER (union): keeps everything ever seen; never drops rows when base changes.
 * - CURRENT snapshot: only items present in today's base (active_in_base=true).
 * - Non-destructive merge: blank/empty values from base cannot overwrite
 *   previously enriched non-empty values (e.g., platform, productCode_hk).
 * - PRESERVE SENTINEL: productCode_hk === "" stays "", and is never reprocessed.
 *
 * Reads:
 *   data/hk_games.json                  <-- "current view" (e.g., sale-only)
 *   data/hk_games_enriched.json         <-- MASTER (if exists)
 * Writes:
 *   data/hk_games_enriched.json         <-- MASTER (union, never drops)
 *   data/hk_games_enriched_current.json <-- SNAPSHOT (active only)
 */

const fs = require("fs");
const path = require("path");
const https = require("https");
const { URL } = require("url");

// ---------- PATHS ----------
const IN_PATH     = "data/hk_games.json";
const OUT_MASTER  = "data/hk_games_enriched.json";
const OUT_CURRENT = "data/hk_games_enriched_current.json";

// ---------- CONFIG ----------
const CONFIG = {
  concurrency: 2,
  baseDelayMs: 900,
  retries: 2,
  rps: 0.8,                 // token-bucket average RPS
  cooldownEvery: 40,        // cooldown/warmup every N requests
  cooldownMsRange: [45_000, 90_000],
};

// ---------- HTTP / HEADERS ----------
const HK_HOST = "store.nintendo.com.hk";
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const REFERERS = [
  "https://store.nintendo.com.hk/",
  "https://store.nintendo.com.hk/nintendo-switch.html",
  "https://store.nintendo.com.hk/games.html",
  "https://store.nintendo.com.hk/console.html",
  "https://store.nintendo.com.hk/accessories.html",
];

const httpsAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 30_000,
  maxSockets: Math.max(4, CONFIG.concurrency * 2),
  scheduling: "lifo",
});

const ensureDir = (p) => fs.mkdirSync(path.dirname(p), { recursive: true });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (ms, spread = 0.7) => ms + Math.floor(Math.random() * Math.max(1, ms * spread));
const randomReferer = () => REFERERS[Math.floor(Math.random() * REFERERS.length)];

// ---------- Limiter ----------
class Limiter {
  constructor({ ratePerSec = 0.8, burst = 2 }) {
    this.tokens = burst;
    this.burst = burst;
    this.rate = ratePerSec;
    setInterval(() => {
      this.tokens = Math.min(this.burst, this.tokens + this.rate);
    }, 1000).unref();
  }
  async take() {
    while (this.tokens < 1) await sleep(120 + Math.random() * 180);
    this.tokens -= 1;
  }
}
const limiter = new Limiter({ ratePerSec: Math.max(0.2, CONFIG.rps || 0.8), burst: 2 });

// ---------- Cookie jar & warmup ----------
const cookieJar = new Map();
function setCookiesFrom(res) {
  const set = res.headers["set-cookie"];
  if (!set) return;
  for (const line of (Array.isArray(set) ? set : [set])) {
    const m = /^([^=;,\s]+)=([^;]*)/.exec(line);
    if (m) cookieJar.set(m[1], m[2]);
  }
}
function getCookieHeader() {
  return cookieJar.size ? [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join("; ") : undefined;
}
function makeHeaders(extra = {}) {
  const headers = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-HK,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "sec-ch-ua": `"Chromium";v="124", "Not-A.Brand";v="99"`,
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": `"Windows"`,
    "Referer": "https://store.nintendo.com.hk/",
  };
  const cookie = getCookieHeader();
  if (cookie) headers.Cookie = cookie;
  return Object.assign(headers, extra);
}
async function warmup() {
  await new Promise((resolve, reject) => {
    https
      .get(`https://${HK_HOST}/`, { headers: makeHeaders(), agent: httpsAgent }, (res) => {
        setCookiesFrom(res);
        res.on("data", () => {});
        res.on("end", resolve);
      })
      .on("error", reject);
  });
}

// ---------- Low-level fetchers ----------
const stats = { started: Date.now(), totalRequests: 0, totalAttempts: 0, ok: 0 };

async function fetchText(url, attempt = 1, maxAttempts = 2, maxRedirects = 5, headers = makeHeaders()) {
  await limiter.take();
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers, agent: httpsAgent }, (res) => {
      setCookiesFrom(res);
      const code = res.statusCode || 0;
      const loc = res.headers.location;

      if ([301, 302, 303, 307, 308].includes(code) && loc && maxRedirects > 0) {
        res.resume();
        const nextUrl = /^https?:\/\//i.test(loc) ? loc : new URL(loc, url).toString();
        return resolve(fetchText(nextUrl, attempt, maxAttempts, maxRedirects - 1, headers));
      }

      let html = "";
      res.setEncoding("utf8");
      res.on("data", (c) => (html += c));
      res.on("end", async () => {
        const looksDenied = /Access Denied|Reference #[0-9a-f]+/i.test(html);
        if ((code === 403 || code === 503 || looksDenied) && attempt < maxAttempts) {
          await sleep(attempt + 1 < maxAttempts ? jitter(1500 * attempt) : jitter(45_000, 0.5));
          try { await maybeCooldownAndRefresh(); } catch {}
          const ref = randomReferer();
          return resolve(fetchText(url, attempt + 1, maxAttempts, maxRedirects, makeHeaders({ Referer: ref })));
        }
        if (code >= 400) {
          const err = new Error(`HTTP ${code} :: ${url}`);
          err.status = code;
          return reject(err);
        }
        resolve({ html, status: code });
      });
    });
    req.on("error", reject);
  });
}

async function maybeCooldownAndRefresh() {
  if (stats.totalRequests > 0 && stats.totalRequests % CONFIG.cooldownEvery === 0) {
    const [a, b] = CONFIG.cooldownMsRange;
    const wait = Math.floor(Math.random() * (b - a)) + a;
    console.log(`[cooldown] Sleeping ${wait}ms & refreshing cookies…`);
    await sleep(wait);
    await warmup();
  }
}

// ---------- Extractors ----------
function extractHACAnywhere(text) {
  if (!text) return null;
  const re = /\bHAC[0-9A-Z\-]{4,}\b/i; // HAC-XXXX or HACXXXX…
  const m = String(text).match(re);
  return m ? m[0].toUpperCase() : null;
}

function extractProductCodeFromHtml(html) {
  // itemprop="sku"
  {
    const m1 = html.match(/<meta[^>]+itemprop=["']sku["'][^>]+content=["']([^"']+)["']/i);
    if (m1 && m1[1]) return m1[1].trim();
    const m2 = html.match(/itemprop=["']sku["'][^>]*>\s*([^<>\s][^<]*)\s*</i);
    if (m2 && m2[1]) return m2[1].trim();
  }
  // Magento-ish: .product.attribute.sku .value
  {
    const m = html.match(/class=["'][^"']*\bproduct\b[^"']*\battribute\b[^"']*\bsku\b[^"']*["'][\s\S]*?class=["'][^"']*\bvalue\b[^"']*["'][^>]*>\s*([^<>\s][^<]*)\s*</i);
    if (m && m[1]) return m[1].trim();
  }
  // Label block
  {
    const m = html.match(/>\s*SKU\s*<\/h2>[\s\S]*?([A-Z0-9\-\_]{4,})/i);
    if (m && m[1]) return m[1].trim();
  }
  // Global HAC scan as last resort
  const hac = extractHACAnywhere(html);
  return hac || null;
}

function extractPlatformFromHtml(html) {
  if (!html) return null;

  const block = html.match(
    /<div[^>]*class=["'][^"']*\bproduct-attribute\b[^"']*\blabel_platform_attr\b[^"']*["'][\s\S]*?<div[^>]*class=["'][^"']*\bproduct-attribute-val\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i
  );
  if (block && block[1]) {
    const raw = block[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    if (/Nintendo Switch 2/i.test(raw)) return "Nintendo Switch 2";
    if (/Nintendo Switch/i.test(raw)) return "Nintendo Switch";
  }

  const near = html.match(
    /label_platform_attr[^>]*>[\s\S]*?product-attribute-title[\s\S]*?>[\s\S]*?platform[\s\S]*?product-attribute-val[^>]*>([\s\S]*?)<\/div>/i
  );
  if (near && near[1]) {
    const raw = near[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    if (/Nintendo Switch 2/i.test(raw)) return "Nintendo Switch 2";
    if (/Nintendo Switch/i.test(raw)) return "Nintendo Switch";
  }

  if (/Nintendo Switch 2/i.test(html)) return "Nintendo Switch 2";
  if (/Nintendo Switch/i.test(html)) return "Nintendo Switch";
  return null;
}

// ---------- Concurrency helper ----------
async function mapWithConcurrency(items, limit, fn, baseDelayMs = 0) {
  const results = new Array(items.length);
  let idx = 0, inflight = 0;
  return new Promise((resolve) => {
    const kick = () => {
      if (idx >= items.length && inflight === 0) return resolve(results);
      while (inflight < limit && idx < items.length) {
        const i = idx++; inflight++;
        (async () => {
          try {
            if (baseDelayMs) await sleep(jitter(baseDelayMs, 0.8));
            results[i] = await fn(items[i], i);
          } catch (e) {
            results[i] = { error: String(e) };
          } finally {
            inflight--; kick();
          }
        })();
      }
    };
    kick();
  });
}

// ---------- I/O ----------
function loadJsonSafe(p, def) { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return def; } }
function saveJsonPretty(p, obj) { ensureDir(p); fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8"); }

// ---------- Fetchers ----------
async function getProductCodeForNsuid(nsuid) {
  const url = `https://${HK_HOST}/${encodeURIComponent(nsuid)}`;
  let lastErr = null;
  for (let attempt = 0; attempt <= Math.max(1, CONFIG.retries); attempt++) {
    try {
      await sleep(jitter(120 + attempt * 40, 0.7));
      const headers = makeHeaders({ Referer: randomReferer() });
      stats.totalAttempts++;
      const { html } = await fetchText(url, 1, 3, 5, headers);
      stats.totalRequests++;
      const code = extractProductCodeFromHtml(html);
      const platform = extractPlatformFromHtml(html);
      stats.ok++;
      if (code) return { nsuid, url, productCode: code, platform, result: "FOUND" };
      return { nsuid, url, productCode: "", platform, result: "NO_CODE" };
    } catch (e) {
      lastErr = e;
      if (e && /HTTP 404/.test(String(e))) return { nsuid, url, productCode: "", platform: null, result: "NOT_FOUND" };
      await sleep(Math.min(20_000, 1500 * Math.pow(2, attempt)) + Math.floor(Math.random() * 500));
      if (attempt === Math.max(1, CONFIG.retries)) { try { await maybeCooldownAndRefresh(); } catch {} }
    }
  }
  return { nsuid, url: `https://${HK_HOST}/${encodeURIComponent(nsuid)}`, productCode: null, platform: null, result: "ERROR", error: String(lastErr) };
}

async function getPlatformForNsuid(nsuid) {
  const url = `https://${HK_HOST}/${encodeURIComponent(nsuid)}`;
  try {
    await sleep(jitter(120, 0.7));
    const headers = makeHeaders({ Referer: randomReferer() });
    stats.totalAttempts++;
    const { html } = await fetchText(url, 1, 3, 5, headers);
    stats.totalRequests++;
    const platform = extractPlatformFromHtml(html);
    stats.ok++;
    return { nsuid, url, platform, result: platform ? "FOUND" : "NO_PLATFORM" };
  } catch (e) {
    return { nsuid, url, platform: null, result: "ERROR", error: String(e) };
  }
}

// ---------- Helpers for merge ----------
const nowIso = () => new Date().toISOString();
const isNonEmpty = (v) => typeof v === "string" ? v.trim() !== "" : (v != null);

// Prefer non-empty `a`; else non-empty `b`; else `a` (keeps types)
function pick(a, b) {
  const sa = (a ?? "").toString().trim();
  const sb = (b ?? "").toString().trim();
  return sa ? a : (sb ? b : a);
}

// SPECIAL for productCode: preserve "" sentinel from prior
function pickCode(baseVal, priorVal) {
  if (typeof baseVal === "string" && baseVal.trim() !== "") return baseVal; // new real code
  if (priorVal === "") return "";                                          // keep sentinel
  if (priorVal !== undefined) return priorVal;                             // keep prior (could be null to force retry)
  return baseVal;                                                          // fall back
}

// ---------- MAIN ----------
(async () => {
  ensureDir(OUT_MASTER);
  await warmup();

  const now = nowIso();

  // Load base (today's view)
  const baseGames = loadJsonSafe(IN_PATH, []);
  const inBase = new Map();
  for (const g of Array.isArray(baseGames) ? baseGames : []) {
    const id = String(g.nsuid_hk || g.nsuid || g.nsuid_txt || "");
    if (!id) continue;
    inBase.set(id, g);
  }

  // Load existing MASTER
  const existingMaster = loadJsonSafe(OUT_MASTER, []);
  const byNsuid = new Map();
  for (const g of Array.isArray(existingMaster) ? existingMaster : []) {
    const id = String((g && (g.nsuid_hk || g.nsuid || g.nsuid_txt)) || "");
    if (id) byNsuid.set(id, g);
  }

  // UNION: base ∪ existing  (non-destructive)
  const unionIds = new Set([...inBase.keys(), ...byNsuid.keys()]);
  const working = [];
  for (const id of unionIds) {
    const base = inBase.get(id) || {};
    const prior = byNsuid.get(id) || {};

    // Start from prior; overlay safe fresh bits from base
    let merged = { ...prior };

    if (isNonEmpty(base.title)) merged.title = base.title;
    if (isNonEmpty(base.url))   merged.url   = base.url;

    // IDs
    merged.nsuid_hk = pick(base.nsuid_hk, pick(merged.nsuid_hk, base.nsuid || base.nsuid_txt || prior.nsuid || prior.nsuid_txt));

    // Bookkeeping
    merged.active_in_base = inBase.has(id);
    merged.first_seen_at  = merged.first_seen_at || prior.first_seen_at || now;
    if (merged.active_in_base) merged.last_seen_at = now;
    else merged.last_seen_at = merged.last_seen_at || now;

    // Non-destructive fields
    merged.productCode_hk = pickCode(base.productCode_hk, merged.productCode_hk); // PRESERVE ""
    merged.platform       = pick(base.platform,       merged.platform);
    merged.releaseDate    = pick(base.releaseDate,    merged.releaseDate);
    merged.publisher      = pick(base.publisher,      merged.publisher);
    merged.imageSquare    = pick(base.imageSquare,    merged.imageSquare);
    merged.imageKey       = pick(base.imageKey,       merged.imageKey);
    merged.dlcType        = pick(base.dlcType,        merged.dlcType);
    merged.playerCount    = pick(base.playerCount,    merged.playerCount);
    merged.genres         = (Array.isArray(base.genres) && base.genres.length)
                              ? base.genres
                              : (Array.isArray(merged.genres) ? merged.genres : []);

    working.push(merged);
  }

  // Decide what to fetch (ONLY active)
  const toProcessCode = working.filter((g) => {
    if (!g.active_in_base) return false;
    const id = g && (g.nsuid_hk || g.nsuid || g.nsuid_txt);
    if (!id) return false;
    const code = g.productCode_hk;
    if (code === "") return false;                               // EXPLICIT sentinel → skip
    if (typeof code === "string" && code.trim()) return false;   // already have code
    return true; // undefined or null → process (null means retry)
  });

  const toProcessPlatformOnly = working.filter((g) => {
    if (!g.active_in_base) return false;
    const code = g.productCode_hk;
    const hasCode = typeof code === "string" && code.trim().length > 0;
    const platMissing = !g.platform || String(g.platform).trim() === "";
    return hasCode && platMissing;
  });

  const activeCount = [...inBase.keys()].length;
  console.log(`[store.hk MASTER] union=${working.length} active=${activeCount} codeFetch=${toProcessCode.length} platOnly=${toProcessPlatformOnly.length}`);

  // Index for in-place updates + periodic saves
  const idxByNsuid = new Map();
  for (let i = 0; i < working.length; i++) {
    const id = String(working[i].nsuid_hk || working[i].nsuid || working[i].nsuid_txt || "");
    if (id) idxByNsuid.set(id, i);
  }

  let processed = 0, found = 0, markedEmpty = 0, markedNull = 0;
  let platFilledFromCodeFetch = 0;
  let platOnlyFound = 0, platOnlyMiss = 0;

  // Pass 1: productCode_hk (+ maybe platform)
  await mapWithConcurrency(
    toProcessCode,
    Math.max(1, CONFIG.concurrency),
    async (game) => {
      const nsuid = String(game.nsuid_hk || game.nsuid || game.nsuid_txt);
      const res = await getProductCodeForNsuid(nsuid);

      if (res.result === "FOUND") { found++; console.log(`[OK] ${nsuid} → ${res.productCode}`); }
      else if (res.result === "NO_CODE" || res.result === "NOT_FOUND") { markedEmpty++; console.log(`[MISS] ${nsuid} → "" (${res.result})`); }
      else { markedNull++; console.warn(`[WARN] ${nsuid} → null (${res.error || res.result})`); }

      const i = idxByNsuid.get(nsuid);
      if (i != null) {
        working[i].productCode_hk = res.productCode; // string | "" | null
        if (!working[i].platform && res.platform) {
          working[i].platform = res.platform;
          platFilledFromCodeFetch++;
        }
        working[i].last_checked_at = nowIso();
      }

      processed++;
      if (processed % 10 === 0 || processed === toProcessCode.length) saveJsonPretty(OUT_MASTER, working);
    },
    CONFIG.baseDelayMs
  );

  // Pass 2: platform-only
  await mapWithConcurrency(
    toProcessPlatformOnly,
    Math.max(1, CONFIG.concurrency),
    async (game) => {
      const nsuid = String(game.nsuid_hk || game.nsuid || game.nsuid_txt);
      const res = await getPlatformForNsuid(nsuid);

      const i = idxByNsuid.get(nsuid);
      if (i != null && res.platform) {
        working[i].platform = res.platform;
        platOnlyFound++;
        console.log(`[PLATFORM] ${nsuid} → ${res.platform}`);
      } else if (i != null) {
        platOnlyMiss++;
        console.log(`[PLATFORM] ${nsuid} → (not found)`);
      }

      if ((platOnlyFound + platOnlyMiss) % 10 === 0 || (platOnlyFound + platOnlyMiss) === toProcessPlatformOnly.length) {
        saveJsonPretty(OUT_MASTER, working);
      }
      if (i != null) working[i].last_checked_at = nowIso();
    },
    CONFIG.baseDelayMs
  );

  // Save MASTER
  saveJsonPretty(OUT_MASTER, working);

  // Save CURRENT snapshot (only active)
  const current = working.filter((g) => g.active_in_base);
  saveJsonPretty(OUT_CURRENT, current);

  const secs = ((Date.now() - stats.started) / 1000).toFixed(1);
  console.log(
    `Done in ${secs}s. CodeProcessed=${processed}, FOUND=${found}, EMPTY="${markedEmpty}", NULL=${markedNull}. ` +
    `PlatformFilledFromCodeFetch=${platFilledFromCodeFetch}, PlatformOnly FOUND=${platOnlyFound}, MISS=${platOnlyMiss}. ` +
    `Master: ${OUT_MASTER} | Current: ${OUT_CURRENT}`
  );
})();
