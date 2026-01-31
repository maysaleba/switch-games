#!/usr/bin/env node
/**
 * Asia eShop fetcher (SG search.nintendo.jp) that merges TWO feeds:
 *  - Switch 1 feed (HACP + urlKey suffix "-switch" + platform "Nintendo Switch")
 *  - Switch 2 feed (BEEP + urlKey suffix "-switch-2" + platform "Nintendo Switch 2")
 *
 * English gating:
 *  - Keep only items where response lang contains "en" or "en_US" (also accepts en-US, en_GB etc.)
 *  - For kept items, output supportLanguage: "en"
 *
 * Output:
 *  - data/as_games_enriched.json            (MASTER grow-forever union)
 *  - data/as_games_enriched_current.json    (SNAPSHOT: active_in_base === true)
 *
 * active_in_base is true if the game appears in EITHER feed on this run.
 */

const fs = require("fs");
const path = require("path");
const axios = require("axios");

// -------------------- helpers --------------------
const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function hasEnglish(languageField) {
  if (!languageField) return false;

  // lang can be string or array depending on endpoint
  const values = Array.isArray(languageField)
    ? languageField
    : String(languageField).split(/[\s,;|/]+/);

  return values.some((v) => {
    const t = String(v).trim().toLowerCase();
    return t === "en" || t === "en_us" || /^en([-_][a-z]+)?$/.test(t);
  });
}

async function safeGet(url, { params = {}, headers = {} } = {}, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await axios.get(url, { params, headers, timeout: 20000 });
    } catch (err) {
      const isLast = attempt === retries - 1;
      if (isLast) throw err;
      const backoff = 600 * Math.pow(2, attempt);
      console.warn(
        `⚠️  Request failed (attempt ${attempt + 1}/${retries}). Retrying in ${backoff}ms…`
      );
      await delay(backoff);
    }
  }
}

function loadJsonArraySafe(filePath) {
  try {
    if (!fs.existsSync(filePath)) return [];
    const raw = fs.readFileSync(filePath, "utf8").trim();
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

// prefer non-empty a; else non-empty b; else a
function pick(a, b) {
  const sa = (a ?? "").toString().trim();
  const sb = (b ?? "").toString().trim();
  return sa ? a : sb ? b : a;
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeTitleForSlug(s) {
  return (s || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "") // diacritics
    .replace(/[\uff01-\uff5e]/g, (c) =>
      String.fromCharCode(c.charCodeAt(0) - 0xfee0)
    ) // full-width ASCII -> half-width
    .replace(/’/g, "'")
    .replace(/[\u200B-\u200D\uFEFF]/g, "") // zero-width chars
    .toLowerCase();
}

function makeUrlKey(title, platformName) {
  const slugBase = normalizeTitleForSlug(title)
    .trim()
    .replace(/'/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

  let platformKey = "";
  if (platformName === "Nintendo Switch") platformKey = "switch";
  else if (platformName === "Nintendo Switch 2") platformKey = "switch-2";
  else platformKey = platformName.toLowerCase().replace(/\s+/g, "-");

  return slugBase ? `${slugBase}-${platformKey}` : "";
}

// -------------------- mapping --------------------
function mapAsItem(item, productPrefix, platformName) {
  // ✅ SG response uses `lang` (array). Fall back to `language` just in case.
  const langField = item?.lang ?? item?.language ?? null;

  // 🚫 DROP if no English
  if (!hasEnglish(langField)) return null;

  const title = item?.title || "";
  const nsuid = item?.nsuid || "";
  const icode = item?.icode || "";

  return {
    title,
    url: "",
    urlKey: makeUrlKey(title, platformName),
    platform: platformName,
    genres: [],
    releaseDate: item?.pdate || "",
    imageSquare: "",
    imageKey: "",
    publisher: "",
    dlcType: "",
    playerCount: "",
    nsuid_as: nsuid,
    productCode_as: icode ? `${productPrefix}${icode}` : "",

    // ✅ required output
    supportLanguage: "en",
  };
}

// -------------------- paged fetch from one endpoint --------------------
async function fetchASFromEndpoint({
  label,
  sourceUrl,
  productPrefix,
  platformName,
  limitPerPage = 400,
}) {
  console.log(`▶️ Fetching AS feed: ${label}`);

  const u = new URL(sourceUrl);
  const baseUrl = `${u.origin}${u.pathname}`;
  const baseParams = Object.fromEntries(u.searchParams.entries());

  // enforce limit (even if URL already has it)
  baseParams.limit = String(limitPerPage);

  const all = new Map(); // nsuid -> raw item
  const RATE_DELAY_MS = 600;
  let page = 1;

  while (true) {
    // Some Nintendo endpoints like a changing c param; harmless if ignored.
    const params = { ...baseParams, page: String(page), c: String(Date.now()) };

    const { data } = await safeGet(baseUrl, { params });
    const items = data?.result?.items || [];
    if (!Array.isArray(items) || items.length === 0) break;

    for (const item of items) {
      const nsuid = item?.nsuid || "";
      if (!nsuid) continue;
      if (!all.has(nsuid)) all.set(nsuid, item);
    }

    console.log(
      `…${label} page ${page} -> ${items.length} items (unique so far: ${all.size})`
    );
    page += 1;
    await delay(RATE_DELAY_MS);
  }

  const mapped = Array.from(all.values())
    .map((it) => mapAsItem(it, productPrefix, platformName))
    .filter(Boolean); // drops nulls from non-EN titles

  console.log(`✅ ${label} done. Unique (EN only): ${mapped.length}/${all.size}`);
  return mapped;
}

// -------------------- main union-merge --------------------
async function main() {
  const outDir = path.join(__dirname, "data");
  const outPathMaster = path.join(outDir, "as_games_enriched.json");
  const outPathCurrent = path.join(outDir, "as_games_enriched_current.json");

  // Feed 1: Switch 1-ish
  const SWITCH1_SOURCE =
    "https://search.nintendo.jp/nintendo_soft_sg/search.json?opt_sshow=1&fq=ssitu_s%3Aonsale%20OR%20ssitu_s%3Apreorder%20OR%20(%20id%3A70050000031641%20OR%20id%3A3676%20OR%20id%3Aef5bf7785c3eca1ab4f3d46a121c1709%20OR%20id%3A3347%20OR%20id%3A3252%20OR%20id%3A3082%20OR%20id%3Aeb9f0cddb93859d136c45e7064033636%20OR%20id%3A52ae614e85d88158afb6f88cbd43d4f5%20OR%20id%3A70670517efc94e7bb6b2f9a16747a63a%20OR%20id%3A70010000000026_2%20OR%20id%3A5fddd1c1534dbea4938916bca1940d44%20OR%20id%3A3261%20OR%20id%3A50010000017473%20OR%20id%3A20010000019167%20OR%20id%3Asg0001%20OR%20id%3Asg0002%20OR%20id%3Asg0003%20OR%20id%3Asg0004%20OR%20id%3Asg0008%20OR%20id%3Asg0009%20OR%20id%3Asg0010%20OR%20id%3Asg0011%20OR%20id%3Asg0012%20OR%20id%3Asg0013%20OR%20id%3Asg0015%20OR%20id%3Asg0016%20OR%20id%3A70010000110303%20OR%20id%3A70070000031179%20)&limit=400&page=1&c=51351256279814418&opt_osale=1&opt_hard=1_HAC&xopt_id[]=70010000000026&xopt_sform[]=HAC_CARD&xopt_sform[]=BEE_CARD&sort=sodate%20desc%2Ctitlek%20asc%2Chards%20asc%2Csform_s%20asc%2Cscore";

  // Feed 2: Switch 2 / BEE
  const SWITCH2_SOURCE =
    "https://search.nintendo.jp/nintendo_soft_sg/search.json?limit=400&page=1&fq=!(sform_s%3ADLC)%20AND%20!(sform_s%3Ahard)%20AND%20!(sform_s%3Aaccessory)&opt_hard[]=05_BEE&sort=sodate%20asc%2Ctitlek%20asc%2Cscore&opt_sshow=1&opt_sche=1";

  // Fetch both feeds
  const fetchedSwitch1 = await fetchASFromEndpoint({
    label: "Switch 1 (HACP)",
    sourceUrl: SWITCH1_SOURCE,
    productPrefix: "HACP",
    platformName: "Nintendo Switch",
  });

  const fetchedSwitch2 = await fetchASFromEndpoint({
    label: "Switch 2 (BEEP)",
    sourceUrl: SWITCH2_SOURCE,
    productPrefix: "BEEP",
    platformName: "Nintendo Switch 2",
  });

  // Combine both fetched lists into a single "today base" map
  // If a nsuid appears in both feeds, prefer Switch 2 for platform/urlKey,
  // but don't overwrite good values with blanks.
  const fetchedById = new Map(); // nsuid_as -> enriched
  for (const e of [...fetchedSwitch1, ...fetchedSwitch2]) {
    if (!e.nsuid_as) continue;

    if (!fetchedById.has(e.nsuid_as)) {
      fetchedById.set(e.nsuid_as, e);
      continue;
    }

    const prior = fetchedById.get(e.nsuid_as);

    const preferSwitch2 =
      prior.platform === "Nintendo Switch 2" || e.platform === "Nintendo Switch 2";
    const mergedTitle = pick(e.title, prior.title);

    fetchedById.set(e.nsuid_as, {
      ...prior,
      title: mergedTitle,
      releaseDate: pick(e.releaseDate, prior.releaseDate),
      productCode_as: pick(e.productCode_as, prior.productCode_as),

      // ✅ preserve supportLanguage
      supportLanguage: pick(e.supportLanguage, prior.supportLanguage),

      platform: preferSwitch2 ? "Nintendo Switch 2" : "Nintendo Switch",
      urlKey: makeUrlKey(
        mergedTitle,
        preferSwitch2 ? "Nintendo Switch 2" : "Nintendo Switch"
      ),
    });
  }

  const fetchedCount = fetchedById.size;

  // Load existing MASTER
  const existing = loadJsonArraySafe(outPathMaster);
  const existingById = new Map();
  for (const e of existing) {
    if (e && e.nsuid_as) existingById.set(e.nsuid_as, e);
  }

  // UNION ids
  const unionIds = new Set([...fetchedById.keys(), ...existingById.keys()]);

  // Merge union into MASTER (non-destructive, grow forever)
  const now = nowIso();
  const master = [];

  for (const id of unionIds) {
    const base = fetchedById.get(id) || {};
    const prior = existingById.get(id) || {};

    const merged = { ...prior };

    merged.title = pick(base.title, merged.title);
    merged.url = pick(base.url, merged.url);
    merged.urlKey = pick(base.urlKey, merged.urlKey);
    merged.platform = pick(base.platform, merged.platform);

    merged.genres =
      Array.isArray(base.genres) && base.genres.length
        ? base.genres
        : Array.isArray(merged.genres)
        ? merged.genres
        : [];

    merged.releaseDate = pick(base.releaseDate, merged.releaseDate);
    merged.imageSquare = pick(base.imageSquare, merged.imageSquare);
    merged.imageKey = pick(base.imageKey, merged.imageKey);
    merged.publisher = pick(base.publisher, merged.publisher);
    merged.dlcType = pick(base.dlcType, merged.dlcType);
    merged.playerCount = pick(base.playerCount, merged.playerCount);

    merged.nsuid_as = pick(base.nsuid_as, merged.nsuid_as);
    merged.productCode_as = pick(base.productCode_as, merged.productCode_as);

    // ✅ keep supportLanguage in MASTER output
    merged.supportLanguage = pick(base.supportLanguage, merged.supportLanguage);

    // bookkeeping (EU-style)
    merged.active_in_base = fetchedById.has(id);
    merged.first_seen_at = merged.first_seen_at || prior.first_seen_at || now;

    if (merged.active_in_base) {
      merged.last_seen_at = now;
      merged.last_checked_at = now;
    } else {
      merged.last_seen_at = merged.last_seen_at || prior.last_seen_at || now;
      merged.last_checked_at =
        merged.last_checked_at || prior.last_checked_at || undefined;
    }

    master.push(merged);
  }

  // Write outputs
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outPathMaster, JSON.stringify(master, null, 2), "utf8");

  const current = master.filter((e) => e.active_in_base === true);
  fs.writeFileSync(outPathCurrent, JSON.stringify(current, null, 2), "utf8");

  console.log("--------------------------------------------------");
  console.log(`✅ Saved MASTER:   ${outPathMaster}`);
  console.log(`✅ Saved CURRENT:  ${outPathCurrent}`);
  console.log(`ℹ️ Existing master: ${existing.length}`);
  console.log(`ℹ️ Fetched today (unique nsuid): ${fetchedCount}`);
  console.log(`ℹ️ Master total: ${master.length}`);
  console.log(`ℹ️ Active today: ${current.length}`);
  console.log("--------------------------------------------------");
}

if (require.main === module) {
  main().catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    process.exit(1);
  });
}

module.exports = {
  fetchASFromEndpoint,
};
