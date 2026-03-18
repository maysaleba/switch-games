#!/usr/bin/env node
/**
 * JP eShop scraper (append-only) - Playwright version
 * Source pages:
 *   https://store-jp.nintendo.com/list/software?softType=TITLE&isSale=true&srule=most-popular&page=X
 *
 * - Reads existing OUT_PATH (if any)
 * - Scrapes additional pages
 * - Appends only new items (by nsuid_jp)
 * - Logs: Existing | New | Total
 */

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

// =======================
// Config
// =======================
const START_PAGE = 1;
const END_PAGE = null; // null = auto stop on empty page
const AUTO_STOP_ON_EMPTY = true;
const DELAY_MS_BETWEEN_BATCHES = 500;
const CONCURRENCY = 2;
const OUT_PATH = 'data/jp_games.json';

const NAV_TIMEOUT_MS = 45000;
const WAIT_AFTER_LOAD_MS = 2500;
const DEBUG_SAVE_HTML = false;

// =======================
// Utilities
// =======================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function imageKeyFromUrl(u) {
  try {
    const url = new URL(u);
    return url.pathname.replace(/^\/+/, '');
  } catch {
    return '';
  }
}

function safeJSONParse(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function pickProductsFromMobify(root) {
  const out = [];
  const queries = root?.__PRELOADED_STATE__?.__reactQuery?.queries;
  if (!Array.isArray(queries)) return out;

  for (const q of queries) {
    const data = q?.state?.data;
    if (!data) continue;

    if (Array.isArray(data.resultProducts) && data.resultProducts.length) {
      out.push(...data.resultProducts);
    }

    if (
      data.productSearch &&
      Array.isArray(data.resultProducts) &&
      data.resultProducts.length
    ) {
      out.push(...data.resultProducts);
    }
  }

  return out;
}

function mapRawProduct(p) {
  const id =
    p?.variationMasterId ||
    p?.productId ||
    p?.id ||
    '';

  const title = p?.name || '';
  const square = p?.imageUrl?.squareHeroBanner || '';
  const releaseDate = p?.releaseDate || null;

  return {
    title,
    nsuid_jp: id,
    url: id ? `/item/software/${id}` : (p?.url || ''),
    urlKey: id,
    platform: '',
    genres: [],
    releaseDate,
    imageSquare: square,
    imageKey: imageKeyFromUrl(square),
    publisher: '',
    dlcType: '',
    playerCount: '',
    productCode_jp: null
  };
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

function readExisting(outPath) {
  try {
    if (!fs.existsSync(outPath)) return [];
    const txt = fs.readFileSync(outPath, 'utf8');
    const arr = JSON.parse(txt);
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    console.warn(`[warn] Failed to read existing file "${outPath}": ${e.message}`);
    return [];
  }
}

async function createBrowser() {
  const browser = await chromium.launch({
    headless: true
  });

  return browser;
}

async function fetchListingPage(browser, pageNum) {
  // const url = `https://store-jp.nintendo.com/list/software?srule=new-arrival&page=${pageNum}`;
  const url = `https://store-jp.nintendo.com/list/software?softType=TITLE&isSale=true&srule=most-popular&page=${pageNum}`;

  const page = await browser.newPage({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    locale: 'ja-JP',
    extraHTTPHeaders: {
      'Accept-Language': 'ja,en;q=0.9'
    }
  });

  try {
    await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: NAV_TIMEOUT_MS
    });

    // Give the page time to hydrate/load any client-side data.
    await page.waitForTimeout(WAIT_AFTER_LOAD_MS);

    const html = await page.content();

    if (DEBUG_SAVE_HTML) {
      fs.mkdirSync('debug_html_jp', { recursive: true });
      fs.writeFileSync(`debug_html_jp/list_page_${pageNum}.html`, html, 'utf8');
    }

    const mobifyText = await page.locator('#mobify-data').textContent().catch(() => null);

    console.log(
      `[page ${pageNum}] has mobify tag? ${html.includes('id="mobify-data"')}`
    );
    console.log(
      `[page ${pageNum}] has resultProducts? ${html.includes('"resultProducts"')}`
    );

    let raw = [];

    if (mobifyText && mobifyText.trim()) {
      const mobifyObj = safeJSONParse(mobifyText.trim());
      if (mobifyObj) {
        raw = pickProductsFromMobify(mobifyObj);
      }
    }

    // Fallback: extract rendered links if mobify path fails.
    if (!raw.length) {
      const renderedItems = await page.locator('a[href*="/item/software/"]').evaluateAll((nodes) => {
        const seen = new Map();

        for (const node of nodes) {
          const href = node.getAttribute('href') || '';
          const text = (node.textContent || '').replace(/\s+/g, ' ').trim();

          const match = href.match(/\/item\/software\/(D\d+)/);
          if (!match) continue;

          const id = match[1];
          if (!seen.has(id)) {
            seen.set(id, {
              variationMasterId: id,
              name: text || '',
              url: href
            });
          }
        }

        return [...seen.values()];
      });

      raw = renderedItems;
    }

    const mapped = dedupeByIdOrFallback(raw.map(mapRawProduct));

    console.log(`[page ${pageNum}] raw items=${mapped.length}`);
    mapped.slice(0, 10).forEach((g, i) => {
      console.log(
        `  ${String(i + 1).padStart(2, ' ')}. ${g.title || 'NO_TITLE'} | ${g.nsuid_jp || 'NO_ID'}`
      );
    });

    return mapped;
  } finally {
    await page.close().catch(() => {});
  }
}

async function run() {
  const outPath = OUT_PATH;
  const existing = readExisting(outPath);
  const existingById = new Set(existing.map((e) => e.nsuid_jp).filter(Boolean));

  const scraped = [];
  const seenBatch = new Set();

  let pageNum = START_PAGE;
  let keepGoing = true;

  const browser = await createBrowser();

  try {
    while (keepGoing) {
      const tasks = [];
      const lastInBatch = END_PAGE
        ? Math.min(pageNum + CONCURRENCY - 1, END_PAGE)
        : pageNum + CONCURRENCY - 1;

      for (let pn = pageNum; pn <= lastInBatch; pn++) {
        tasks.push(
          fetchListingPage(browser, pn)
            .then((items) => ({ pn, items }))
            .catch((err) => {
              console.warn(`[warn] page ${pn} failed: ${err?.message || err}`);
              return { pn, items: [] };
            })
        );
      }

      const results = await Promise.all(tasks);

      let sawEmpty = false;

      for (const { pn, items } of results) {
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
        console.log(`page ${pn}: +${filtered.length} new (seen this run: ${seenBatch.size})`);
      }

      pageNum = lastInBatch + 1;

      if (END_PAGE && pageNum > END_PAGE) keepGoing = false;
      if (!END_PAGE && AUTO_STOP_ON_EMPTY && sawEmpty) keepGoing = false;

      if (keepGoing && DELAY_MS_BETWEEN_BATCHES > 0) {
        await sleep(DELAY_MS_BETWEEN_BATCHES);
      }
    }
  } finally {
    await browser.close().catch(() => {});
  }

  const newEntries = scraped;
  const merged = existing.concat(newEntries);

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(merged, null, 2), 'utf8');

  console.log(`✅ Saved JP games to ${outPath}`);
  console.log(`ℹ️ Existing: ${existing.length} | New: ${newEntries.length} | Total: ${merged.length}`);
}

run().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
