#!/usr/bin/env node
/**
 * JP eShop scraper (append-only)
 *
 * Hybrid architecture:
 * - Playwright only for capturing a fresh Authorization bearer
 * - Direct API requests for actual pagination/data
 * - Auto-refresh bearer on 401/403
 *
 * Pagination model confirmed by testing:
 * - pageNum=1  -> c_page=0
 * - pageNum=2  -> c_page=1
 * - ...
 * - if maxPage=145, last script pageNum is 146
 */

const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

// =======================
// Config
// =======================
const START_PAGE = 1;
const END_PAGE = null; // null = auto-detect actual last page from API
const AUTO_STOP_ON_EMPTY = true;
const DELAY_MS_BETWEEN_BATCHES = 500;
const CONCURRENCY = 2;
const OUT_PATH = "data/jp_games.json";

const NAV_TIMEOUT_MS = 45000;
const AUTH_CAPTURE_TIMEOUT_MS = 30000;
const REQUEST_TIMEOUT_MS = 45000;
const MAX_AUTH_REFRESH_RETRIES = 1;

const LISTING_SRULE = "most-popular";
const LISTING_PAGE_URL =
  "https://store-jp.nintendo.com/list/software?softType=TITLE&isSale=true&srule=most-popular&page=1";
const API_URL_BASE =
  "https://store-jp.nintendo.com/mobify/proxy/api/custom/search/v1/organizations/f_ecom_bfgj_prd/search";

// =======================
// Utilities
// =======================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function imageKeyFromUrl(u) {
  try {
    const url = new URL(u);
    return url.pathname.replace(/^\/+/, "");
  } catch {
    return "";
  }
}

function readExisting(outPath) {
  try {
    if (!fs.existsSync(outPath)) return [];
    const txt = fs.readFileSync(outPath, "utf8");
    const arr = JSON.parse(txt);
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    console.warn(`[warn] Failed to read existing file "${outPath}": ${e.message}`);
    return [];
  }
}

function dedupeByIdOrFallback(items) {
  const seen = new Set();
  const out = [];

  for (const item of items) {
    const key = item.nsuid_jp || `${item.title}|${item.url}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }

  return out;
}

function mapApiProduct(p) {
  const nsuid = p?.variationMasterId || (p?.id ? `D${String(p.id)}` : "");
  const imageSquare = p?.imageUrl?.squareHeroBanner || "";

  const tags = Array.isArray(p?.displayTags)
    ? p.displayTags.map((t) => t?.displayName).filter(Boolean)
    : [];

  let platform = "Nintendo Switch";
  if (p?.productClassCode === "BEE") {
    platform = "Nintendo Switch 2";
  }

  return {
    title: p?.name || "",
    nsuid_jp: nsuid,
    url: nsuid ? `/products/${nsuid}/` : (p?.url || ""),
    urlKey: "",
    platform,
    genres: tags,
    releaseDate: p?.releaseDate || null,
    imageSquare,
    imageKey: imageKeyFromUrl(imageSquare),
    publisher: p?.manufacturerName || "",
    dlcType: p?.softType && p.softType !== "TITLE" ? p.softType : "",
    playerCount: "",
    productCode_jp: null
  };
}

function buildApiUrl(pageNum) {
  const apiPage = pageNum - 1; // pageNum 1 => c_page 0

  const url = new URL(API_URL_BASE);
  url.searchParams.set("c_cgid", "software");
  url.searchParams.set("c_prefn1", "isSale");
  url.searchParams.set("c_prefv1", "true");
  url.searchParams.set("c_softType", "TITLE");
  url.searchParams.set("c_srule", LISTING_SRULE);
  url.searchParams.set("c_page", String(apiPage));
  url.searchParams.set("siteId", "MNS");
  return url.toString();
}

function extractPageMeta(data, requestedPageNum) {
  const apiPageNumber = Number(data?.productSearch?.pageNumber);
  const currentPage = Number(data?.pagingInfo?.currentPage);
  const maxPageApi = Number(data?.pagingInfo?.maxPage);
  const totalCount = Number(data?.pagingInfo?.totalCount);

  return {
    apiPageNumber: Number.isFinite(apiPageNumber) ? apiPageNumber : requestedPageNum - 1,
    currentPage: Number.isFinite(currentPage) ? currentPage : requestedPageNum,
    maxPageApi: Number.isFinite(maxPageApi) ? maxPageApi : null,
    lastPageNum: Number.isFinite(maxPageApi) ? maxPageApi + 1 : null,
    totalCount: Number.isFinite(totalCount) ? totalCount : null
  };
}

async function createBrowser() {
  return chromium.launch({ headless: true });
}

// =======================
// Auth capture via Playwright
// =======================
async function getFreshAuthorization(browser) {
  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    locale: "ja-JP",
    extraHTTPHeaders: {
      "Accept-Language": "ja,en;q=0.9"
    }
  });

  try {
    const authPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("Timed out while waiting to capture Authorization header"));
      }, AUTH_CAPTURE_TIMEOUT_MS);

      page.on("request", (req) => {
        try {
          const url = req.url();
          if (
            req.method() === "GET" &&
            url.includes("/mobify/proxy/api/custom/search/v1/organizations/f_ecom_bfgj_prd/search")
          ) {
            const headers = req.headers();
            const auth = headers["authorization"];

            if (auth && /^Bearer\s+/i.test(auth)) {
              clearTimeout(timer);
              resolve(auth);
            }
          }
        } catch {}
      });
    });

    await page.goto(LISTING_PAGE_URL, {
      waitUntil: "domcontentloaded",
      timeout: NAV_TIMEOUT_MS
    });

    const authorization = await authPromise;
    console.log("[auth] captured fresh Authorization header");
    return authorization;
  } finally {
    await page.close().catch(() => {});
  }
}

// =======================
// Direct API request
// =======================
async function fetchJsonWithTimeout(url, options = {}, timeoutMs = REQUEST_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    return res;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchListingPage(pageNum, authorization) {
  const url = buildApiUrl(pageNum);

  const res = await fetchJsonWithTimeout(url, {
    method: "GET",
    headers: {
      "accept": "application/json",
      "accept-language": "ja,en;q=0.9",
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
      "authorization": authorization
    }
  });

  if (res.status === 401 || res.status === 403) {
    const body = await res.text().catch(() => "");
    throw new Error(`AUTH_EXPIRED ${res.status} ${body.slice(0, 200)}`);
  }

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} ${body.slice(0, 300)}`);
  }

  const data = await res.json();
  const raw = Array.isArray(data?.resultProducts) ? data.resultProducts : [];
  const mapped = dedupeByIdOrFallback(raw.map(mapApiProduct));
  const meta = extractPageMeta(data, pageNum);

  console.log(
    `[page ${pageNum}] apiPage=${meta.apiPageNumber}` +
      ` | currentPage=${meta.currentPage}` +
      ` | maxPageApi=${meta.maxPageApi ?? "unknown"}` +
      ` | lastPageNum=${meta.lastPageNum ?? "unknown"}` +
      ` | totalCount=${meta.totalCount ?? "unknown"}` +
      ` | api items=${mapped.length}`
  );

  mapped.slice(0, 10).forEach((g, i) => {
    console.log(
      `  ${String(i + 1).padStart(2, " ")}. ${g.title || "NO_TITLE"} | ${g.nsuid_jp || "NO_ID"}`
    );
  });

  return { items: mapped, meta };
}

async function fetchListingPageWithAutoRefresh(pageNum, browser, authState) {
  let attempt = 0;

  while (true) {
    try {
      return await fetchListingPage(pageNum, authState.authorization);
    } catch (err) {
      const msg = String(err?.message || err);

      if ((msg.includes("AUTH_EXPIRED")) && attempt < MAX_AUTH_REFRESH_RETRIES) {
        attempt++;
        console.warn(`[auth] token expired on page ${pageNum}, refreshing...`);
        authState.authorization = await getFreshAuthorization(browser);
        continue;
      }

      throw err;
    }
  }
}

// =======================
// Main
// =======================
async function run() {
  const existing = readExisting(OUT_PATH);
  const existingById = new Set(existing.map((e) => e.nsuid_jp).filter(Boolean));

  const scraped = [];
  const seenBatch = new Set();

  let pageNum = START_PAGE;
  let keepGoing = true;
  let discoveredLastPageNum = END_PAGE;

  const browser = await createBrowser();
  const authState = { authorization: null };

  try {
    authState.authorization = await getFreshAuthorization(browser);

    while (keepGoing) {
      const tasks = [];
      const batchEnd = discoveredLastPageNum
        ? Math.min(pageNum + CONCURRENCY - 1, discoveredLastPageNum)
        : pageNum + CONCURRENCY - 1;

      for (let pn = pageNum; pn <= batchEnd; pn++) {
        tasks.push(
          fetchListingPageWithAutoRefresh(pn, browser, authState)
            .then((result) => ({ pn, ...result }))
            .catch((err) => {
              console.warn(`[warn] page ${pn} failed: ${err?.message || err}`);
              return {
                pn,
                items: [],
                meta: {
                  apiPageNumber: pn - 1,
                  currentPage: pn,
                  maxPageApi: discoveredLastPageNum ? discoveredLastPageNum - 1 : null,
                  lastPageNum: discoveredLastPageNum,
                  totalCount: null
                }
              };
            })
        );
      }

      const results = await Promise.all(tasks);
      let sawEmpty = false;

      for (const { pn, items, meta } of results) {
        if (
          meta?.lastPageNum &&
          (!discoveredLastPageNum || meta.lastPageNum > discoveredLastPageNum)
        ) {
          discoveredLastPageNum = meta.lastPageNum;
        }

        if (items.length === 0) {
          sawEmpty = true;
        }

        const filtered = items.filter((g) => {
          const key = g.nsuid_jp || `${g.title}|${g.url}`;

          if (g.nsuid_jp && existingById.has(g.nsuid_jp)) return false;
          if (seenBatch.has(key)) return false;

          seenBatch.add(key);
          return true;
        });

        scraped.push(...filtered);

        console.log(
          `page ${pn}: +${filtered.length} new` +
            ` (seen this run: ${seenBatch.size})` +
            `${meta?.lastPageNum ? ` | actualLastPage=${meta.lastPageNum}` : ""}`
        );
      }

      pageNum = batchEnd + 1;

      if (discoveredLastPageNum && pageNum > discoveredLastPageNum) {
        keepGoing = false;
      }

      if (!discoveredLastPageNum && AUTO_STOP_ON_EMPTY && sawEmpty) {
        keepGoing = false;
      }

      if (keepGoing && DELAY_MS_BETWEEN_BATCHES > 0) {
        await sleep(DELAY_MS_BETWEEN_BATCHES);
      }
    }
  } finally {
    await browser.close().catch(() => {});
  }

  const newEntries = scraped;
  const merged = existing.concat(newEntries);

  fs.mkdirSync(path.dirname(OUT_PATH), { recursive: true });
  fs.writeFileSync(OUT_PATH, JSON.stringify(merged, null, 2), "utf8");

  console.log(`✅ Saved JP games to ${OUT_PATH}`);
  console.log(`ℹ️ Existing: ${existing.length} | New: ${newEntries.length} | Total: ${merged.length}`);

  if (discoveredLastPageNum) {
    console.log(`ℹ️ Actual last page discovered from API: ${discoveredLastPageNum}`);
  }
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
