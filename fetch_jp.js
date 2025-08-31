#!/usr/bin/env node
/**
 * JP eShop "New Arrivals" scraper (append-only).
 * Source pages:
 *   https://store-jp.nintendo.com/list/software?srule=new-arrival&page=X
 *
 * - Reads existing OUT_PATH (if any)
 * - Scrapes additional pages
 * - Appends only new items (by nsuid_jp)
 * - Logs: Existing | New | Total
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const cheerio = require('cheerio');

// =======================
// Config (edit as needed)
// =======================
const START_PAGE = 1;
const END_PAGE = null;                // null = auto stop on empty page
const AUTO_STOP_ON_EMPTY = true;
const DELAY_MS_BETWEEN_BATCHES = 500;
const CONCURRENCY = 2;
const OUT_PATH = 'data/jp_games.json';

// =======================
// Utilities
// =======================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function imageKeyFromUrl(u) {
  try {
    const url = new URL(u);
    return url.pathname.replace(/^\/+/, '');
  } catch {
    return '';
  }
}

function safeJSONParse(text) {
  try { return JSON.parse(text); } catch { return null; }
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
    if (data.productSearch && Array.isArray(data.resultProducts) && data.resultProducts.length) {
      out.push(...data.resultProducts);
    }
  }
  return out;
}

async function fetchListingPage(pageNum) {
//  const url = `https://store-jp.nintendo.com/list/software?srule=new-arrival&page=${pageNum}`;
  const url = `https://store-jp.nintendo.com/list/software?softType=TITLE&isSale=true&srule=most-popular&page=${pageNum}`;
  
  const res = await axios.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; JPNewArrivalsBot/1.0)',
      'Accept-Language': 'ja,en;q=0.8',
    },
    timeout: 20000,
  });

  const $ = cheerio.load(res.data);
  const mobifyText = $('#mobify-data').text().trim();
  const mobifyObj = safeJSONParse(mobifyText);
  if (!mobifyObj) return [];

  const raw = pickProductsFromMobify(mobifyObj);

  return raw.map(p => {
    const id = p.variationMasterId || '';
    const title = p.name || '';
    const square = p.imageUrl?.squareHeroBanner || '';
    const releaseDate = p.releaseDate || null;

    return {
      title,
      nsuid_jp: id,
      url: `/products/${id}/`,
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
  });
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

async function run() {
  // 1) Load existing
  const outPath = OUT_PATH;
  const existing = readExisting(outPath);
  const existingById = new Set(existing.map(e => e.nsuid_jp).filter(Boolean));

  // 2) Scrape new pages
  const scraped = [];
  const seenBatch = new Set(); // avoid duplicates within the same run

  let page = START_PAGE;
  let keepGoing = true;

  while (keepGoing) {
    const tasks = [];
    const lastInBatch = END_PAGE ? Math.min(page + CONCURRENCY - 1, END_PAGE) : page + CONCURRENCY - 1;

    for (let pn = page; pn <= lastInBatch; pn++) {
      tasks.push(
        fetchListingPage(pn)
          .then(items => ({ pn, items }))
          .catch(err => {
            console.warn(`[warn] page ${pn} failed: ${err?.message || err}`);
            return { pn, items: [] };
          })
      );
    }

    const results = await Promise.all(tasks);

    let sawEmpty = false;
    for (const { pn, items } of results) {
      if (items.length === 0) sawEmpty = true;
      // keep only ones not in existing + not seen in this batch
      const filtered = items.filter(g => {
        const key = g.nsuid_jp || `${g.title}|${g.url}`;
        if (existingById.has(g.nsuid_jp)) return false;
        if (seenBatch.has(key)) return false;
        seenBatch.add(key);
        return true;
      });
      scraped.push(...filtered);
      console.log(`page ${pn}: +${filtered.length} new (seen this run: ${seenBatch.size})`);
    }

    page = lastInBatch + 1;

    if (END_PAGE && page > END_PAGE) keepGoing = false;
    if (!END_PAGE && AUTO_STOP_ON_EMPTY && sawEmpty) keepGoing = false;

    if (keepGoing && DELAY_MS_BETWEEN_BATCHES > 0) {
      await sleep(DELAY_MS_BETWEEN_BATCHES);
    }
  }

  // 3) Merge (existing first to preserve prior ordering, then new)
  const newEntries = scraped;
  const merged = existing.concat(newEntries);

  // 4) Save + log
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(merged, null, 2), 'utf8');

  console.log(`✅ Saved JP games to ${outPath}`);
  console.log(`ℹ️ Existing: ${existing.length} | New: ${newEntries.length} | Total: ${merged.length}`);
}

run().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
