#!/usr/bin/env node
/**
 * Fetch HK eShop items (current BASE is recent releases) and append-only save.
 * Output: data/hk_games.json (trimmed fields, aligned with US/EU)
 *
 * Console logs aligned with US/EU fetchers.
 */

const fs = require('fs');
const path = require('path');
const axios = require('axios').default;
const cheerio = require('cheerio');

// ---- CONFIG ----
const BASE = "https://store.nintendo.com.hk/digital-games/recent-releases?product_list_order=release-date-asc";
//const BASE = "https://store.nintendo.com.hk/digital-games/current-offers?product_list_limit=24";
const PAGE_DELAY_MS = 350; // polite delay between listing pages

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

// Extract a 10+ digit NSUID from ec.nintendo.com URLs under /titles/ or /bundles/
function extractNsuidFromEcUrl(href) {
  if (!href) return null;
  const url = href.startsWith('http') ? href : `https:${href}`;
  const m = url.match(/\/(?:titles|bundles)\/(\d{10,})/);
  return m ? m[1] : null;
}

// Normalize platform key for urlKey
function platformKeyFromName(name) {
  if (name === 'Nintendo Switch') return 'switch';
  if (name === 'Nintendo Switch 2') return 'switch-2';
  return String(name || 'Nintendo Switch').toLowerCase().replace(/\s+/g, '-');
}

// ---- add this helper ----
function buildPageUrl(base, page) {
  const u = new URL(base);
  if (page > 1) {
    u.searchParams.set('p', String(page));
  } else {
    u.searchParams.delete('p');
  }
  return u.toString();
}

// slugify title
function slugifyTitle(s) {
  return String(s || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

async function scrapePage(page) {
// const url = page > 1 ? `${BASE}?p=${page}` : BASE;
  const url = buildPageUrl(BASE, page);
  const { data: html } = await axios.get(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36',
      'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
    },
    timeout: 30000,
  });

  const $ = cheerio.load(html);

  const items = [];
  // Walk product cards to reliably extract title + EC links
  $('.products .product-item').each((_, card) => {
    const $card = $(card);
    const $ec = $card.find(`a[href*="ec.nintendo.com"][href*="/titles/"], a[href*="ec.nintendo.com"][href*="/bundles/"]`).first();
    const ecHref = $ec.attr('href');
    const nsuid = extractNsuidFromEcUrl(ecHref);
    if (!nsuid) return;

    // title strategies
    let title =
      ($card.find('.product-item-link').text() || '').trim().replace(/\s+/g, ' ') ||
      ($card.find('img[alt]').attr('alt') || '').trim().replace(/\s+/g, ' ') ||
      ($ec.attr('title') || '').trim().replace(/\s+/g, ' ');
    if (!title) title = $card.text().trim().replace(/\s+/g, ' ');

    const url = ecHref ? (ecHref.startsWith('http') ? ecHref : `https:${ecHref}`) : '';

    items.push({ title, nsuid, url });
  });

  // Dedupe within page
  const map = new Map();
  for (const it of items) if (!map.has(it.nsuid)) map.set(it.nsuid, it);
  return [...map.values()];
}

// Map raw (title, nsuid, url) → aligned trimmed schema like US/EU
function mapItemToSchema(it) {
  const platformName = 'Nintendo Switch'; // HK listing is Switch ecosystem
  const urlKey = (() => {
    const slug = slugifyTitle(it.title);
    const pkey = platformKeyFromName(platformName);
    return slug ? `${slug}-${pkey}` : '';
  })();

  return {
    title: it.title || '',
    nsuid_hk: it.nsuid || '',
    url: it.url || '',
    urlKey,
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

async function fetchHKGames() {
  console.log('▶️ Starting HK games fetch...');

  const collected = [];
  const seen = new Set();           // track NSUIDs across pages
  let page = 1;
  const MAX_PAGES = 200;            // optional hard cap

  while (page <= MAX_PAGES) {
    try {
      console.log(`🌏 Fetching HK page ${page}…`);
      const items = await scrapePage(page);

      if (!items.length) {
        console.log(`⏹️  No items found on page ${page}. Stopping.`);
        break;
      }

      // how many are new on this page?
      const newOnPage = items.filter(it => !seen.has(it.nsuid));
      console.log(`  ➕ Found ${items.length} items; new this page: ${newOnPage.length}.`);

      // append only new, and mark seen
      for (const it of newOnPage) {
        collected.push(it);
        seen.add(it.nsuid);
      }

      // SAFETY STOP: if a page returns 0 new items, we’re likely looping same page
      if (newOnPage.length === 0) {
        console.log(`⏹️  Page ${page} had no new NSUIDs. Stopping to avoid infinite loop.`);
        break;
      }

      await delay(PAGE_DELAY_MS);
      page += 1;
    } catch (err) {
      console.warn(`⚠️ Page ${page} failed: ${err.message || err}. Skipping after backoff…`);
      await delay(1000);
      page += 1;
    }
  }

  const uniq = collected; // already de-duped via `seen`
  console.log(`▶️ Completed HK games fetch. Total unique: ${uniq.length}`);
  return uniq.map(mapItemToSchema);
}



async function main() {
  const outDir = path.join(__dirname, 'data');
  const outPath = path.join(outDir, 'hk_games.json');

  const fetched = await fetchHKGames();

  fs.mkdirSync(outDir, { recursive: true });
  const existing = loadJsonArraySafe(outPath);
  const existingKeys = new Set(existing.map(e => e.nsuid_hk).filter(Boolean));

  const newEntries = fetched.filter(e => e.nsuid_hk && !existingKeys.has(e.nsuid_hk));
  const merged = existing.concat(newEntries);

  fs.writeFileSync(outPath, JSON.stringify(merged, null, 2));

  console.log(`✅ Saved HK games to ${outPath}`);
  console.log(`ℹ️ Existing: ${existing.length} | New: ${newEntries.length} | Total: ${merged.length}`);
}

if (require.main === module) {
  main().catch(err => { console.error(err); process.exit(1); });
}

module.exports = { fetchHKGames };
