#!/usr/bin/env node
/**
 * Fetch HK eShop items (current BASE is current offers) and append-only save.
 * Output: data/hk_games.json (trimmed fields, aligned with US/EU)
 *
 * Playwright version
 */

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

// ---- CONFIG ----
// const BASE = "https://store.nintendo.com.hk/digital-games/recent-releases?product_list_order=release-date-asc";
const BASE = "https://store.nintendo.com.hk/digital-games/current-offers?product_list_order=release-date-desc&product_list_limit=24";
const PAGE_DELAY_MS = 350;
const NAV_TIMEOUT_MS = 30000;
const WAIT_AFTER_LOAD_MS = 1500;
const MAX_PAGES = 3;

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

function buildPageUrl(base, page) {
  const u = new URL(base);
  if (page > 1) {
    u.searchParams.set('p', String(page));
  } else {
    u.searchParams.delete('p');
  }
  return u.toString();
}

function slugifyTitle(s) {
  return String(s || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
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

async function createBrowser() {
  return chromium.launch({ headless: true });
}

async function createContext(browser) {
  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    locale: 'zh-HK',
    extraHTTPHeaders: {
      'Accept-Language': 'zh-HK,zh;q=0.9,en;q=0.8',
      'Cache-Control': 'no-cache',
      'Pragma': 'no-cache'
    }
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

async function scrapePage(context, pageNum) {
  const url = buildPageUrl(BASE, pageNum);
  const page = await context.newPage();

  try {
    await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: NAV_TIMEOUT_MS
    });

    // Wait for the product cards to appear, or at least for the page to settle.
    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
    await page.waitForSelector('.products .product-item', { timeout: 10000 }).catch(() => {});
    await page.waitForTimeout(WAIT_AFTER_LOAD_MS);

    const extractItems = async () => {
      return await page.evaluate(() => {
        const normalize = (s) => String(s || '').trim().replace(/\s+/g, ' ');

        const cards = [...document.querySelectorAll('.products .product-item')];
        const out = [];

        for (const card of cards) {
          const ecLink = card.querySelector(
            'a[href*="ec.nintendo.com"][href*="/titles/"], a[href*="ec.nintendo.com"][href*="/bundles/"]'
          );
          const ecHref = ecLink?.getAttribute('href') || '';
          if (!ecHref) continue;

          let title =
            normalize(card.querySelector('.product-item-link')?.textContent) ||
            normalize(card.querySelector('img[alt]')?.getAttribute('alt')) ||
            normalize(ecLink?.getAttribute('title')) ||
            normalize(card.textContent);

          const href = ecHref.startsWith('http') ? ecHref : `https:${ecHref}`;
          out.push({ title, href });
        }

        return out;
      });
    };

    let items;
    try {
      items = await extractItems();
    } catch (err) {
      if (String(err.message || err).includes('Execution context was destroyed')) {
        await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
        await page.waitForTimeout(1000);
        items = await extractItems();
      } else {
        throw err;
      }
    }

    const mapped = [];
    const seen = new Set();

    for (const it of items) {
      const nsuid = extractNsuidFromEcUrl(it.href);
      if (!nsuid) continue;
      if (seen.has(nsuid)) continue;
      seen.add(nsuid);

      mapped.push({
        title: it.title || '',
        nsuid,
        url: it.href || ''
      });
    }

    return mapped;
  } finally {
    await page.close().catch(() => {});
  }
}

async function fetchHKGames() {
  console.log('▶️ Starting HK games fetch...');

  const collected = [];
  const seen = new Set();

  const browser = await createBrowser();
  const context = await createContext(browser);

  try {
    let page = 1;

    while (page <= MAX_PAGES) {
      try {
        console.log(`🌏 Fetching HK page ${page}…`);
        const items = await scrapePage(context, page);

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
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }

  const uniq = collected;
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
  main().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { fetchHKGames };