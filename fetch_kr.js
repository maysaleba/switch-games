#!/usr/bin/env node
/**
 * Fetch KR eShop items (48 per page) from:
 *   https://store.nintendo.co.kr/all-product?p=1&product_list_limit=48
 * and append-only save.
 *
 * Output: data/kr_games.json (trimmed fields, aligned with US/EU/HK)
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios').default;
const cheerio = require('cheerio');

// ---- CONFIG ----
//const BASE = 'https://store.nintendo.co.kr/all-product?product_list_limit=48';
const BASE = 'https://store.nintendo.co.kr/digital/sale?am_on_sale=1&product_list_limit=48';
const PAGE_DELAY_MS = 350;
const MAX_PAGES = 200;

// ---- HELPERS ----
const delay = (ms) => new Promise(res => setTimeout(res, ms));

function loadJsonArraySafe(filePath) {
  try {
    if (!fs.existsSync(filePath)) return [];
    const raw = fs.readFileSync(filePath, 'utf8').trim();
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function extractNsuidFromKrUrl(href = '') {
  const url = href.startsWith('http') ? href : `https://store.nintendo.co.kr${href}`;
  const m = url.match(/\/(\d{10,})\b/);
  return m ? m[1] : null;
}

function buildPageUrl(base, page) {
  const u = new URL(base);
  if (page > 1) {
    u.searchParams.set('p', String(page));
  } else {
    u.searchParams.delete('p');
  }
  return u.toString();
}

function platformKeyFromName(name) {
  if (name === 'Nintendo Switch') return 'switch';
  if (name === 'Nintendo Switch 2') return 'switch-2';
  return String(name || 'Nintendo Switch').toLowerCase().replace(/\s+/g, '-');
}

function slugifyTitle(s) {
  return String(s || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

async function scrapePage(page) {
  const url = buildPageUrl(BASE, page);
  const { data: html } = await axios.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0',
      'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
    },
    timeout: 30000,
  });

  const $ = cheerio.load(html);
  const items = [];

  $('.products .product-item, li.product-item').each((_, card) => {
    const $card = $(card);

    let $a = null;
    $card.find('a[href]').each((__, a) => {
      const href = $(a).attr('href') || '';
      if (/\b\/\d{10,}\b/.test(href)) { $a = $(a); return false; }
    });
    if (!$a) return;

    const href = $a.attr('href') || '';
    const nsuid = extractNsuidFromKrUrl(href);
    if (!nsuid) return;

    let title =
      ($card.find('.product-item-details .product.name a').first().text() || '').trim() ||
      ($card.find('a.product-item-link').first().text() || '').trim() ||
      ($card.find('.product-item-link').first().text() || '').trim();

    if (!title) {
      title =
        ($card.find('img[alt]').attr('alt') || '').trim() ||
        ($a.attr('title') || '').trim() ||
        ($a.text() || '').trim();
    }

    const looksLikeInlineBlob = /\bprodImageContainers\b|\bproduct-image-container-\d+\b/.test(title);
    if (looksLikeInlineBlob) {
      const alt = ($card.find('img[alt]').attr('alt') || '').trim();
      const anchorName = ($card.find('.product-item-details .product.name a').first().text() || '').trim();
      title = (anchorName || alt || '').trim();
    }

    title = (title || '').replace(/\s+/g, ' ').trim();
    if (!title) return;

    const absUrl = href.startsWith('http') ? href : `https://store.nintendo.co.kr${href}`;

    console.log(`NSUID ${nsuid} → "${title}"`);

    items.push({ title, nsuid, url: absUrl });
  });

  const map = new Map();
  for (const it of items) if (!map.has(it.nsuid)) map.set(it.nsuid, it);
  return [...map.values()];
}

function mapItemToSchema(it) {
  const platformName = 'Nintendo Switch';
  const urlKey = (() => {
    const slug = slugifyTitle(it.title);
    const pkey = platformKeyFromName(platformName);
    return slug ? `${slug}-${pkey}` : '';
  })();

  return {
    title: it.title || '',
    nsuid_kr: it.nsuid || '',
    url: it.url || '',
    urlKey: '',
    platform: '',
    genres: [],
    releaseDate: '',
    imageSquare: '',
    imageKey: '',
    publisher: '',
    dlcType: '',
    playerCount: ''
  };
}

async function fetchKRGames(outPath, existing) {
  console.log('▶️ Starting KR games fetch...');
  const collected = [];
  const seen = new Set(existing.map(e => e.nsuid_kr).filter(Boolean));
  let page = 1;

  while (page <= MAX_PAGES) {
    try {
      console.log(`🌏 Fetching KR page ${page}…`);
      const items = await scrapePage(page);

      if (!items.length) {
        console.log(`⏹️  No items found on page ${page}. Stopping.`);
        break;
      }

      const newOnPage = items.filter(it => !seen.has(it.nsuid));
      console.log(`  ➕ Found ${items.length} items; new this page: ${newOnPage.length}.`);

      for (const it of newOnPage) {
        collected.push(it);
        seen.add(it.nsuid);
      }

      // 👉 Write progress to disk after every page
      const newEntries = newOnPage.map(mapItemToSchema);
      if (newEntries.length > 0) {
        const merged = existing.concat(newEntries);
        fs.writeFileSync(outPath, JSON.stringify(merged, null, 2));
        console.log(`💾 Progress saved: ${merged.length} total entries so far.`);
        existing = merged; // update baseline for next loop
      }

      if (newOnPage.length === 0) {
        console.log(`⏹️  Page ${page} had no new NSUIDs. Stopping.`);
        break;
      }

      await delay(PAGE_DELAY_MS);
      page += 1;
    } catch (err) {
      console.warn(`⚠️ Page ${page} failed: ${err.message || err}. Skipping…`);
      await delay(1000);
      page += 1;
    }
  }

  console.log(`▶️ Completed KR games fetch.`);
}

async function main() {
  const outDir = path.join(__dirname, 'data');
  const outPath = path.join(outDir, 'kr_games.json');

  fs.mkdirSync(outDir, { recursive: true });
  const existing = loadJsonArraySafe(outPath);

  await fetchKRGames(outPath, existing);

  console.log(`✅ Final save at ${outPath}`);
}

if (require.main === module) {
  main().catch(err => { console.error(err); process.exit(1); });
}

module.exports = { fetchKRGames };
