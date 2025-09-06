#!/usr/bin/env node
/**
 * HK enrichment (single-pass: code + platform + supportLanguage)
 * - MASTER union (never drops)
 * - CURRENT snapshot: active_in_base = true
 * - Non-destructive merge; sentinels preserved:
 *   - productCode_hk === "" stays "" (never reprocess)
 *   - supportLanguage === "" stays "" (processed, NO English)
 * - supportLanguage only set if productCode_hk is non-empty
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
  rps: 0.8,                 // avg RPS, token-bucket
  cooldownEvery: 40,        // every N attempts, long sleep + warmup
  cooldownMsRange: [45_000, 90_000],
  requestTimeoutMs: 20_000,
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

// // Optional GLOBAL circuit breaker (uncomment to enable)
// let globalBackoffUntil = 0;
// const nowMs = () => Date.now();
// async function globalGate() {
//   const wait = globalBackoffUntil - nowMs();
//   if (wait > 0) await sleep(wait);
//   await limiter.take();
// }

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
    "Referer": "https://store.nintendo.com.hk/",
  };
  const cookie = getCookieHeader();
  if (cookie) headers.Cookie = cookie;
  return Object.assign(headers, extra);
}
async function warmup() {
  await new Promise((resolve, reject) => {
    const req = https
      .get(`https://${HK_HOST}/`, { headers: makeHeaders(), agent: httpsAgent }, (res) => {
        setCookiesFrom(res);
        res.on("data", () => {});
        res.on("end", resolve);
      })
      .on("error", reject);
    req.setTimeout(CONFIG.requestTimeoutMs, () => req.destroy(new Error(`Warmup timeout ${CONFIG.requestTimeoutMs}ms`)));
  });
}

// ---------- Low-level fetcher ----------
const stats = { started: Date.now(), totalAttempts: 0 };

async function fetchText(url, attempt = 1, maxAttempts = 2, maxRedirects = 5, headers = makeHeaders()) {
  // await globalGate(); // if using circuit breaker
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
          const back = attempt + 1 < maxAttempts ? jitter(1500 * attempt) : jitter(45_000, 0.5);
          await sleep(back);
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
    req.setTimeout(CONFIG.requestTimeoutMs, () => req.destroy(new Error(`Timeout ${CONFIG.requestTimeoutMs}ms :: ${url}`)));
    req.on("error", reject);
  });
}

async function maybeCooldownAndRefresh() {
  if (stats.totalAttempts > 0 && stats.totalAttempts % CONFIG.cooldownEvery === 0) {
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
  const re = /\bHAC[0-9A-Z\-]{4,}\b/i;
  const m = String(text).match(re);
  return m ? m[0].toUpperCase() : null;
}
function extractProductCodeFromHtml(html) {
  {
    const m1 = html.match(/<meta[^>]+itemprop=["']sku["'][^>]+content=["']([^"']+)["']/i);
    if (m1 && m1[1]) return m1[1].trim();
    const m2 = html.match(/itemprop=["']sku["'][^>]*>\s*([^<>\s][^<]*)\s*</i);
    if (m2 && m2[1]) return m2[1].trim();
  }
  {
    const m = html.match(/class=["'][^"']*\bproduct\b[^"']*\battribute\b[^"']*\bsku\b[^"']*["'][\s\S]*?class=["'][^"']*\bvalue\b[^"']*["'][^>]*>\s*([^<>\s][^<]*)\s*</i);
    if (m && m[1]) return m[1].trim();
  }
  {
    const m = html.match(/>\s*SKU\s*<\/h2>[\s\S]*?([A-Z0-9\-\_]{4,})/i);
    if (m && m[1]) return m[1].trim();
  }
  return extractHACAnywhere(html);
}
function extractPlatformFromHtml(html) {
  const block = html.match(/<div[^>]*class=["'][^"']*\bproduct-attribute\b[^"']*\blabel_platform_attr\b[^"']*["'][\s\S]*?<div[^>]*class=["'][^"']*\bproduct-attribute-val\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
  if (block && block[1]) {
    const raw = block[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    if (/Nintendo Switch 2/i.test(raw)) return "Nintendo Switch 2";
    if (/Nintendo Switch/i.test(raw)) return "Nintendo Switch";
  }
  if (/Nintendo Switch 2/i.test(html)) return "Nintendo Switch 2";
  if (/Nintendo Switch/i.test(html)) return "Nintendo Switch";
  return null;
}
// language text block
function extractSupportedLanguagesText(html) {
  const block =
    html.match(/<div[^>]*class=["'][^"']*\bproduct-attribute\b[^"']*\blabel_supported_languages_attr\b[^"']*["'][\s\S]*?<div[^>]*class=["'][^"']*\bproduct-attribute-val\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i)
    || html.match(/supported[_\- ]languages[\s\S]*?class=["'][^"']*\bproduct-attribute-val\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
  if (block && block[1]) return block[1].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim() || null;
  const near = html.match(/(語言|语言|支援語言|支援语言|Supported\s*Languages?)[:：]?\s*([\s\S]{0,240})<\/(div|li|p|td)>/i);
  if (near) return near[0].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim() || null;
  return null;
}
function textHasEnglishLanguageHK(txt) {
  if (!txt) return false;
  return /\benglish\b/i.test(txt) || /英文|英語|英语/.test(txt) || /\ben\b(?![a-z])/i.test(txt);
}

// ---------- I/O ----------
function loadJsonSafe(p, def) { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return def; } }
function saveJsonPretty(p, obj) { ensureDir(p); fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8"); }

// ---------- Merge helpers ----------
const nowIso = () => new Date().toISOString();
const isNonEmpty = (v) => typeof v === "string" ? v.trim() !== "" : (v != null);
function pick(a, b) {
  const sa = (a ?? "").toString().trim();
  const sb = (b ?? "").toString().trim();
  return sa ? a : (sb ? b : a);
}
// productCode sentinel preserve
function pickCode(baseVal, priorVal) {
  if (typeof baseVal === "string" && baseVal.trim() !== "") return baseVal;
  if (priorVal === "") return "";
  if (priorVal !== undefined) return priorVal;
  return baseVal;
}
// supportLanguage sentinel preserve
function pickSupportLanguage(baseVal, priorVal) {
  if (typeof baseVal === "string" && baseVal.trim() !== "") return baseVal; // 'en'
  if (baseVal === "") return "";
  if (priorVal === "") return "";
  if (priorVal !== undefined) return priorVal;
  return baseVal;
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

// ---------- Single-pass fetcher ----------
async function getAllDetailsForNsuid(nsuid) {
  const url = `https://${HK_HOST}/${encodeURIComponent(nsuid)}`;
  let lastErr = null;
  for (let attempt = 0; attempt <= Math.max(1, CONFIG.retries); attempt++) {
    try {
      await sleep(jitter(120 + attempt * 40, 0.7));
      stats.totalAttempts++;
      const headers = makeHeaders({ Referer: randomReferer() });
      const { html } = await fetchText(url, 1, 3, 5, headers);

      const code = extractProductCodeFromHtml(html);
      const platform = extractPlatformFromHtml(html);
      const langTxt = extractSupportedLanguagesText(html);
      const hasEN = textHasEnglishLanguageHK(langTxt || "");

      // supportLanguage only meaningful if we have code (from this fetch)
      const supportLanguage = hasEN ? "en" : "";

      return {
        nsuid, url,
        productCode: code ?? null,
        platform: platform ?? null,
        supportLanguage, // apply guarded in caller depending on code presence
        result: "OK",
      };
    } catch (e) {
      lastErr = e;
      if (e && /HTTP 404/.test(String(e))) {
        return { nsuid, url, productCode: "", platform: null, supportLanguage: null, result: "NOT_FOUND" };
      }
      await sleep(Math.min(20_000, 1500 * Math.pow(2, attempt)) + Math.floor(Math.random() * 500));
      // // Optional global breaker on final failure:
      // if (attempt === Math.max(1, CONFIG.retries) && lastErr && lastErr.status === 403) {
      //   const globalMs = 60_000 + Math.floor(Math.random() * 30_000);
      //   globalBackoffUntil = Math.max(globalBackoffUntil, nowMs() + globalMs);
      //   console.warn(`[rate] 403 cluster → GLOBAL backoff ${globalMs}ms`);
      // }
    }
  }
  return { nsuid, url: `https://${HK_HOST}/${encodeURIComponent(nsuid)}`, productCode: null, platform: null, supportLanguage: null, result: "ERROR", error: String(lastErr) };
}

// ---------- MAIN ----------
(async () => {
  ensureDir(OUT_MASTER);
  await warmup();

  const now = nowIso();

  // Load base and master
  const baseGames = loadJsonSafe(IN_PATH, []);
  const inBase = new Map();
  for (const g of Array.isArray(baseGames) ? baseGames : []) {
    const id = String(g.nsuid_hk || g.nsuid || g.nsuid_txt || "");
    if (!id) continue;
    inBase.set(id, g);
  }

  const existingMaster = loadJsonSafe(OUT_MASTER, []);
  const byNsuid = new Map();
  for (const g of Array.isArray(existingMaster) ? existingMaster : []) {
    const id = String((g && (g.nsuid_hk || g.nsuid || g.nsuid_txt)) || "");
    if (id) byNsuid.set(id, g);
  }

  // Union
  const unionIds = new Set([...inBase.keys(), ...byNsuid.keys()]);
  const working = [];
  for (const id of unionIds) {
    const base = inBase.get(id) || {};
    const prior = byNsuid.get(id) || {};
    let merged = { ...prior };

    if (isNonEmpty(base.title)) merged.title = base.title;
    if (isNonEmpty(base.url))   merged.url   = base.url;

    merged.nsuid_hk = pick(base.nsuid_hk, pick(merged.nsuid_hk, base.nsuid || base.nsuid_txt || prior.nsuid || prior.nsuid_txt));

    merged.active_in_base = inBase.has(id);
    merged.first_seen_at  = merged.first_seen_at || prior.first_seen_at || now;
    merged.last_seen_at   = merged.active_in_base ? now : (merged.last_seen_at || now);

    merged.productCode_hk = pickCode(base.productCode_hk, merged.productCode_hk);
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

    merged.supportLanguage = pickSupportLanguage(base.supportLanguage, merged.supportLanguage);

    working.push(merged);
  }

  // Build single-pass processing list (ACTIVE ONLY)
  const toProcess = working.filter((g) => {
    if (!g.active_in_base) return false;
    const id = g && (g.nsuid_hk || g.nsuid || g.nsuid_txt);
    if (!id) return false;

    const code = g.productCode_hk;
    const codeMissing = !(typeof code === "string" && code.trim() !== "") && code !== "";
    const platformMissing = !g.platform || String(g.platform).trim() === "";
    const langUnset = g.supportLanguage === undefined || g.supportLanguage === null; // "" means processed-no-English

    // If code is explicitly "", skip further processing entirely
    if (code === "") return false;

    // Fetch if any of the three is missing/unset
    return codeMissing || platformMissing || langUnset;
  });

  const activeCount = [...inBase.keys()].length;
  const codeFetchCount = toProcess.filter(g =>
    !(typeof g.productCode_hk === "string" && g.productCode_hk.trim() !== "") && g.productCode_hk !== ""
  ).length;
  const platMissingCount = toProcess.filter(g => !g.platform || String(g.platform).trim() === "").length;
  const langUnsetCount = toProcess.filter(g => g.supportLanguage === undefined || g.supportLanguage === null).length;

  console.log(
    `[store.hk MASTER] union=${working.length} active=${activeCount} ` +
    `toProcess=${toProcess.length} codeMissing=${codeFetchCount} ` +
    `platMissing=${platMissingCount} langUnset=${langUnsetCount}`
  );


  const idxByNsuid = new Map();
  for (let i = 0; i < working.length; i++) {
    const id = String(working[i].nsuid_hk || working[i].nsuid || working[i].nsuid_txt || "");
    if (id) idxByNsuid.set(id, i);
  }

  let done = 0, foundCode = 0, noCode = 0, errors = 0, langEn = 0, langNoEn = 0, platFill = 0;

  await mapWithConcurrency(
    toProcess,
    Math.max(1, CONFIG.concurrency),
    async (game) => {
      const nsuid = String(game.nsuid_hk || game.nsuid || game.nsuid_txt);
      const res = await getAllDetailsForNsuid(nsuid);

      const i = idxByNsuid.get(nsuid);
      if (i != null) {
        const row = working[i];

        // productCode
        if (res.result === "NOT_FOUND") {
          row.productCode_hk = "";
          noCode++;
          console.log(`[MISS] ${nsuid} → "" (404/Not found)`);
        } else if (res.result === "OK") {
          if (res.productCode) {
            if (!row.productCode_hk || (row.productCode_hk == null)) {
              row.productCode_hk = res.productCode;
              foundCode++;
              console.log(`[OK] ${nsuid} → ${res.productCode}`);
            }
          } else if (row.productCode_hk == null) {
            row.productCode_hk = null; // retry later
          }

          // platform
          if (!row.platform && res.platform) {
            row.platform = res.platform;
            platFill++;
            console.log(`[PLATFORM] ${nsuid} → ${res.platform}`);
          }

          // supportLanguage only if we have non-empty code (prior or fetched)
          const codeNow = row.productCode_hk || res.productCode;
          const hasCodeNow = typeof codeNow === "string" && codeNow.trim() !== "";
          if ((row.supportLanguage === undefined || row.supportLanguage === null) && hasCodeNow) {
            if (res.supportLanguage === "en") { row.supportLanguage = "en"; langEn++; console.log(`[LANG] ${nsuid} → en`); }
            else if (res.supportLanguage === "") { row.supportLanguage = ""; langNoEn++; console.log(`[LANG] ${nsuid} → (no English)`); }
          }

        } else {
          errors++;
          console.warn(`[WARN] ${nsuid} → (error: ${res.error || res.result})`);
        }

        row.last_checked_at = nowIso();
        if (++done % 10 === 0 || done === toProcess.length) saveJsonPretty(OUT_MASTER, working);
      }
    },
    CONFIG.baseDelayMs
  );

  // Save MASTER + CURRENT
  saveJsonPretty(OUT_MASTER, working);
  const current = working.filter((g) => g.active_in_base);
  saveJsonPretty(OUT_CURRENT, current);

  const secs = ((Date.now() - stats.started) / 1000).toFixed(1);
  console.log(
    `Done in ${secs}s. CodeOK=${foundCode}, CodeMISS="${noCode}", ERR=${errors}. ` +
    `PlatformFilled=${platFill}, Lang EN=${langEn}, Lang NoEN=${langNoEn}. ` +
    `Master: ${OUT_MASTER} | Current: ${OUT_CURRENT}`
  );
})();
