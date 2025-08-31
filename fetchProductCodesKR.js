#!/usr/bin/env node
/**
 * KR enrichment (MASTER + SNAPSHOT, non-destructive merge)
 *
 * - MASTER (union): keeps everything ever seen; never drops rows when base changes.
 * - CURRENT snapshot: only items present in today's base (active_in_base=true).
 * - Non-destructive merge: blank/empty values from base cannot overwrite
 *   previously enriched non-empty values (e.g., platform, productCode_kr).
 *
 * Reads:
 *   data/kr_games.json                  <-- "current view" (e.g., sale-only)
 *   data/kr_games_enriched.json         <-- MASTER (if exists)
 * Writes:
 *   data/kr_games_enriched.json         <-- MASTER (union, never drops)
 *   data/kr_games_enriched_current.json <-- SNAPSHOT (active only)
 */

const fs = require("fs");
const path = require("path");
const https = require("https");
const { URL } = require("url");

// ---------- PATHS ----------
const IN_PATH     = "data/kr_games.json";
const OUT_MASTER  = "data/kr_games_enriched.json";
const OUT_CURRENT = "data/kr_games_enriched_current.json";

// ---------- CONFIG ----------
const CONFIG = {
  concurrency: 4,
  baseDelayMs: 900,
  retries: 2,
  rps: 0.8,                 // token-bucket average RPS
  cooldownEvery: 40,        // cooldown/warmup every N requests
  cooldownMsRange: [45_000, 90_000],
};

// ---------- HTTP BOILERPLATE ----------
const KR_HOST = "store.nintendo.co.kr";
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const REFERERS = [
  "https://store.nintendo.co.kr/",
  "https://store.nintendo.co.kr/all-product",
  "https://store.nintendo.co.kr/digital/sale",
  "https://store.nintendo.co.kr/nintendo-switch.html",
  "https://store.nintendo.co.kr/games.html",
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
const cookieJar = new Map(); // name -> value
function setCookiesFrom(res) {
  const set = res.headers["set-cookie"];
  if (!set) return;
  const arr = Array.isArray(set) ? set : [set];
  for (const line of arr) {
    const m = /^([^=;,\s]+)=([^;]*)/.exec(line);
    if (m) cookieJar.set(m[1], m[2]);
  }
}
function getCookieHeader() {
  if (!cookieJar.size) return undefined;
  return [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join("; ");
}
function makeHeaders(extra = {}) {
  const headers = {
    "User-Agent": UA,
    "Accept":
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
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
    "Referer": "https://store.nintendo.co.kr/",
  };
  const cookie = getCookieHeader();
  if (cookie) headers["Cookie"] = cookie;
  Object.assign(headers, extra);
  return headers;
}
async function warmup() {
  await new Promise((resolve, reject) => {
    https
      .get(`https://${KR_HOST}/`, { headers: makeHeaders(), agent: httpsAgent }, (res) => {
        setCookiesFrom(res);
        res.on("data", () => {});
        res.on("end", resolve);
      })
      .on("error", reject);
  });
}

// ---------- Low-level fetchers ----------
const stats = { started: Date.now(), totalRequests: 0, totalAttempts: 0, ok: 0 };

async function fetchText(
  url,
  attempt = 1,
  maxAttempts = 2,
  maxRedirects = 5,
  headers = makeHeaders()
) {
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
          try {
            await maybeCooldownAndRefresh();
          } catch {}
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
    const m1 = html.match(
      /<meta[^>]+itemprop=["']sku["'][^>]+content=["']([^"']+)["']/i
    );
    if (m1 && m1[1]) return m1[1].trim();
    const m2 = html.match(/itemprop=["']sku["'][^>]*>\s*([^<>\s][^<]*)\s*</i);
    if (m2 && m2[1]) return m2[1].trim();
  }
  // Magento-ish: .product.attribute.sku .value
  {
    const m = html.match(
      /class=["'][^"']*\bproduct\b[^"']*\battribute\b[^"']*\bsku\b[^"']*["'][\s\S]*?class=["'][^"']*\bvalue\b[^"']*["'][^>]*>\s*([^<>\s][^<]*)\s*</i
    );
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

// Try to detect platform from attribute blocks or text
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

  if (/Nintendo Switch 2/i.test(html)) return "Nintendo Switch 2";
  if (/Nintendo Switch/i.test(html)) return "Nintendo Switch";
  return null;
}

// ======= urlKey helpers =======
function slugifyTitle(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}
function platformKeyFromName(name) {
  if (name === "Nintendo Switch 2") return "switch-2";
  return "switch"; // default & "Nintendo Switch"
}
function buildUrlKey(title, platform) {
  const slug = slugifyTitle(title);
  const pkey = platformKeyFromName(platform);
  return slug ? `${slug}-${pkey}` : "";
}

// ---------- Concurrency helper ----------
async function mapWithConcurrency(items, limit, fn, baseDelayMs = 0) {
  const results = new Array(items.length);
  let idx = 0,
    inflight = 0;

  return new Promise((resolve) => {
    const kick = () => {
      if (idx >= items.length && inflight === 0) return resolve(results);
      while (inflight < limit && idx < items.length) {
        const i = idx++;
        inflight++;
        (async () => {
          try {
            if (baseDelayMs) await sleep(jitter(baseDelayMs, 0.8));
            results[i] = await fn(items[i], i);
          } catch (e) {
            results[i] = { error: String(e) };
          } finally {
            inflight--;
            kick();
          }
        })();
      }
    };
    kick();
  });
}

// ---------- I/O ----------
function loadJsonSafe(p, def) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return def;
  }
}
function saveJsonPretty(p, obj) {
  ensureDir(p);
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

// ---------- Fetchers ----------
async function getProductCodeForNsuid(nsuid) {
  const url = `https://${KR_HOST}/${encodeURIComponent(nsuid)}`;
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
      return { nsuid, url, productCode: "", platform, result: "NO_CODE" }; // fetched OK but none visible
    } catch (e) {
      lastErr = e;
      if (e && /HTTP 404/.test(String(e))) {
        return { nsuid, url, productCode: "", platform: null, result: "NOT_FOUND" };
      }
      await sleep(Math.min(20_000, 1500 * Math.pow(2, attempt)) + Math.floor(Math.random() * 500));
      if (attempt === Math.max(1, CONFIG.retries)) {
        try {
          await maybeCooldownAndRefresh();
        } catch {}
      }
    }
  }
  return {
    nsuid,
    url: `https://${KR_HOST}/${encodeURIComponent(nsuid)}`,
    productCode: null,
    platform: null,
    result: "ERROR",
    error: String(lastErr),
  };
}

// Platform-only fetcher (when code is cached but platform is missing)
async function getPlatformForNsuid(nsuid) {
  const url = `https://${KR_HOST}/${encodeURIComponent(nsuid)}`;
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

// ---------- MAIN ----------
(async () => {
  ensureDir(OUT_MASTER);
  await warmup();

  const nowIso = new Date().toISOString();

  // Load current base (may be sale-only or a rebuild)
  const baseGames = loadJsonSafe(IN_PATH, []);
  const inBase = new Map();
  for (const g of Array.isArray(baseGames) ? baseGames : []) {
    const id = String(g.nsuid_kr || g.nsuid || g.nsuid_txt || "");
    if (!id) continue;
    inBase.set(id, g);
  }

  // Load existing MASTER
  const existingMaster = loadJsonSafe(OUT_MASTER, []);
  const byNsuid = new Map();
  for (const g of Array.isArray(existingMaster) ? existingMaster : []) {
    const id = String((g && (g.nsuid_kr || g.nsuid || g.nsuid_txt)) || "");
    if (id) byNsuid.set(id, g);
  }

  // Helper: prefer non-empty a; else non-empty b; else a
  const pick = (a, b) => {
    const sa = (a ?? "").toString().trim();
    const sb = (b ?? "").toString().trim();
    return sa ? a : (sb ? b : a);
  };

  // UNION: base ∪ existing  (non-destructive merge)
  const unionIds = new Set([...inBase.keys(), ...byNsuid.keys()]);
  const working = [];
  for (const id of unionIds) {
    const base = inBase.get(id) || {};
    const prior = byNsuid.get(id) || {};

    // Start from prior, then overlay "safe" base fields
    let merged = { ...prior };

    // Always prefer latest title/url from base if present
    if ((base.title || "").trim()) merged.title = base.title;
    if ((base.url || "").trim()) merged.url = base.url;

    // Carry over identifiers
    merged.nsuid_kr = pick(base.nsuid_kr, pick(merged.nsuid_kr, base.nsuid || base.nsuid_txt || prior.nsuid || prior.nsuid_txt));

    // Master bookkeeping
    merged.active_in_base = inBase.has(id);
    merged.first_seen_at = merged.first_seen_at || prior.first_seen_at || nowIso;
    if (merged.active_in_base) {
      merged.last_seen_at = nowIso;
    } else {
      merged.last_seen_at = merged.last_seen_at || nowIso;
    }

    // Non-destructive for enriched fields: don't let blank base wipe master
    // If you also track these in base and want them to update when non-empty, allow it via pick(base, prior)
    merged.productCode_kr = pick(base.productCode_kr, merged.productCode_kr);
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

    // Keep urlKey in sync when we have title+platform
    const titleForKey = merged.title || "";
    const platForKey  = (merged.platform && String(merged.platform).trim()) ? merged.platform : "Nintendo Switch";
    if (titleForKey) merged.urlKey = buildUrlKey(titleForKey, platForKey);

    working.push(merged);
  }

  // Decide what to fetch:
  // A) product code pass: ONLY for active items that lack a confirmed code
  const toProcessCode = working.filter((g) => {
    if (!g.active_in_base) return false;
    const id = g && (g.nsuid_kr || g.nsuid || g.nsuid_txt);
    if (!id) return false;
    const code = g.productCode_kr;
    if (code === "") return false;                               // explicit prior miss
    if (typeof code === "string" && code.trim()) return false;   // already have code
    return true; // undefined or null → process (null means retry)
  });

  // B) platform-only pass: ONLY for active items with code present but platform missing/empty
  const toProcessPlatformOnly = working.filter((g) => {
    if (!g.active_in_base) return false;
    const code = g.productCode_kr;
    const hasCode = typeof code === "string" && code.trim().length > 0;
    const platMissing = !g.platform || String(g.platform).trim() === "";
    return hasCode && platMissing;
  });

  const activeCount = [...inBase.keys()].length;
  console.log(
    `[store.kr MASTER] union=${working.length} active=${activeCount} codeFetch=${toProcessCode.length} platOnly=${toProcessPlatformOnly.length}`
  );
  console.log({
    union: working.length,
    active: activeCount,
    codeFetch: toProcessCode.length,
    platOnly: toProcessPlatformOnly.length,
    sanity: toProcessCode.length + toProcessPlatformOnly.length === activeCount,
  });

  // Index for in-place updates + periodic saves
  const idxByNsuid = new Map();
  for (let i = 0; i < working.length; i++) {
    const id = String(working[i].nsuid_kr || working[i].nsuid || working[i].nsuid_txt || "");
    if (id) idxByNsuid.set(id, i);
  }

  let processed = 0,
    found = 0,
    markedEmpty = 0,
    markedNull = 0;
  let platFilledFromCodeFetch = 0;
  let platOnlyFound = 0,
    platOnlyMiss = 0;

  // Pass 1: fetch productCode_kr (also fills platform if we saw it)
  await mapWithConcurrency(
    toProcessCode,
    Math.max(1, CONFIG.concurrency),
    async (game) => {
      const nsuid = String(game.nsuid_kr || game.nsuid || game.nsuid_txt);
      const res = await getProductCodeForNsuid(nsuid);

      if (res.result === "FOUND") {
        found++;
        console.log(`[OK] ${nsuid} → ${res.productCode}`);
      } else if (res.result === "NO_CODE" || res.result === "NOT_FOUND") {
        markedEmpty++;
        console.log(`[MISS] ${nsuid} → "" (${res.result})`);
      } else {
        markedNull++;
        console.warn(`[WARN] ${nsuid} → null (${res.error || res.result})`);
      }

      const i = idxByNsuid.get(nsuid);
      if (i != null) {
        // update productCode and platform (non-destructive already handled in merge)
        working[i].productCode_kr = res.productCode; // string | "" | null
        if (!working[i].platform && res.platform) {
          working[i].platform = res.platform;
          platFilledFromCodeFetch++;
        }

        // set urlKey when we have a title and platform
        const titleForKey = working[i].title || game.title || "";
        const platForKey = working[i].platform || res.platform || "Nintendo Switch";
        if (titleForKey) {
          working[i].urlKey = buildUrlKey(titleForKey, platForKey);
        }

        working[i].last_checked_at = new Date().toISOString();
      }

      processed++;
      if (processed % 10 === 0 || processed === toProcessCode.length) {
        saveJsonPretty(OUT_MASTER, working);
      }
    },
    CONFIG.baseDelayMs
  );

  // Pass 2: platform-only fill where productCode_kr is cached
  await mapWithConcurrency(
    toProcessPlatformOnly,
    Math.max(1, CONFIG.concurrency),
    async (game) => {
      const nsuid = String(game.nsuid_kr || game.nsuid || game.nsuid_txt);
      const res = await getPlatformForNsuid(nsuid);

      const i = idxByNsuid.get(nsuid);
      if (i != null && res.platform) {
        working[i].platform = res.platform;
        platOnlyFound++;
        console.log(`[PLATFORM] ${nsuid} → ${res.platform}`);

        // (re)build urlKey now that platform is known
        const titleForKey = working[i].title || game.title || "";
        const platForKey = working[i].platform || res.platform || "Nintendo Switch";
        if (titleForKey) {
          working[i].urlKey = buildUrlKey(titleForKey, platForKey);
        }
      } else if (i != null) {
        platOnlyMiss++;
        console.log(`[PLATFORM] ${nsuid} → (not found)`);
      }

      if (
        (platOnlyFound + platOnlyMiss) % 10 === 0 ||
        platOnlyFound + platOnlyMiss === toProcessPlatformOnly.length
      ) {
        saveJsonPretty(OUT_MASTER, working);
      }

      if (i != null) {
        working[i].last_checked_at = new Date().toISOString();
      }
    },
    CONFIG.baseDelayMs
  );

  // Final sweep: ensure urlKey exists for all entries
  for (let i = 0; i < working.length; i++) {
    const t = working[i].title || "";
    if (!t) continue;
    const p = working[i].platform || "Nintendo Switch";
    const desired = buildUrlKey(t, p);
    if (desired && working[i].urlKey !== desired) {
      working[i].urlKey = desired;
    }
  }

  // Save MASTER (union)
  saveJsonPretty(OUT_MASTER, working);

  // Save CURRENT snapshot (only active items)
  const current = working.filter((g) => g.active_in_base);
  saveJsonPretty(OUT_CURRENT, current);

  const secs = ((Date.now() - stats.started) / 1000).toFixed(1);
  console.log(
    `Done in ${secs}s. CodeProcessed=${processed}, FOUND=${found}, EMPTY="${markedEmpty}", NULL=${markedNull}. ` +
      `PlatformFilledFromCodeFetch=${platFilledFromCodeFetch}, PlatformOnly FOUND=${platOnlyFound}, MISS=${platOnlyMiss}. ` +
      `Master: ${OUT_MASTER} | Current: ${OUT_CURRENT}`
  );
})();
