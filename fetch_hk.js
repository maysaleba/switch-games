#!/usr/bin/env node
/**
 * Fetch HK eShop items from multiple HK sale sources
 * and append-only save.
 *
 * Output: data/hk_games.json
 *
 * Playwright version
 */

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

// ---- CONFIG ----
const SOURCES = [
  {
    name: 'download-code-sale',
    baseUrl:
      'https://store.nintendo.com.hk/download-code/sale?product_list_dir=desc&product_list_order=release_date&supported_languages=257',
    maxPages: 3
  },
  {
    name: 'digital-games-current-offers',
    baseUrl: 'https://store.nintendo.com.hk/digital-games/current-offers',
    maxPages: 1 // only page 1, per your request
  }
];

const PAGE_DELAY_MS = 350;
const NAV_TIMEOUT_MS = 30000;
const WAIT_AFTER_LOAD_MS = 1500;

// ---- HELPERS ----
const delay = (ms) => new Promise((res) => setTimeout(res, ms));

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

/**
 * Extract NSUID from either:
 * - ec.nintendo.com/.../titles/{id}
 * - ec.nintendo.com/.../bundles/{id}
 * - store.nintendo.com.hk/{id}
 */
function extractNsuid(href) {
  if (!href) return null;

  const url = href.startsWith('http') ? href : `https:${href}`;

  let m = url.match(/\/(?:titles|bundles)\/(\d{10,})/);
  if (m) return m[1];

  m = url.match(/store\.nintendo\.com\.hk\/(\d{10,})(?:[/?#]|$)/);
  if (m) return m[1];

  return null;
}

function platformKeyFromName(name) {
  if (name === 'Nintendo Switch') return 'switch';
  if (name === 'Nintendo Switch 2') return 'switch-2';
  return String(name || 'Nintendo Switch').toLowerCase().replace(/\s+/g, '-');
}

function buildPageUrl(base, page) {
  const u = new URL(base);

  // Magento pages usually use ?p=2, ?p=3, etc.
  // Page 1 should not set p.
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

async function scrapePage(context, sourceName, url) {
  const page = await context.newPage();

  try {
    console.log(`🌏 [${sourceName}] Opening: ${url}`);

    await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: NAV_TIMEOUT_MS
    });

    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
    await page.waitForSelector('.products .product-item', { timeout: 10000 }).catch(() => {});
    await page.waitForTimeout(WAIT_AFTER_LOAD_MS);

    const extractItems = async () => {
      return await page.evaluate(() => {
        const normalize = (s) => String(s || '').trim().replace(/\s+/g, ' ');

        const cards = [...document.querySelectorAll('.products .product-item')];
        const out = [];

        for (const card of cards) {
          const titleLink = card.querySelector('a.product-item-link');
          const photoLink = card.querySelector('a.product.photo.product-item-photo');
          const ecLink = card.querySelector(
            'a[href*="ec.nintendo.com"][href*="/titles/"], a[href*="ec.nintendo.com"][href*="/bundles/"]'
          );

          const primaryLink = titleLink || photoLink || ecLink;
          const href =
            primaryLink?.getAttribute('href') ||
            photoLink?.getAttribute('href') ||
            titleLink?.getAttribute('href') ||
            ecLink?.getAttribute('href') ||
            '';

          if (!href) continue;

          const title =
            normalize(titleLink?.textContent) ||
            normalize(card.querySelector('img[alt]')?.getAttribute('alt')) ||
            normalize(primaryLink?.getAttribute('title')) ||
            '';

          const fullHref = href.startsWith('http')
            ? href
            : href.startsWith('//')
              ? `https:${href}`
              : `https://store.nintendo.com.hk${href.startsWith('/') ? '' : '/'}${href}`;

          out.push({ title, href: fullHref });
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
      const nsuid = extractNsuid(it.href);
      if (!nsuid) continue;
      if (seen.has(nsuid)) continue;
      seen.add(nsuid);

      mapped.push({
        title: it.title || '',
        nsuid,
        url: it.href || '',
        source: sourceName
      });
    }

    return mapped;
  } finally {
    await page.close().catch(() => {});
  }
}

async function fetchFromSource(context, source) {
  const collected = [];
  const seen = new Set();

  for (let pageNum = 1; pageNum <= source.maxPages; pageNum++) {
    try {
      const url = buildPageUrl(source.baseUrl, pageNum);
      const items = await scrapePage(context, source.name, url);

      if (!items.length) {
        console.log(`⏹️ [${source.name}] No items found on page ${pageNum}.`);
        break;
      }

      const newOnPage = items.filter((it) => !seen.has(it.nsuid));
      console.log(
        `📦 [${source.name}] Page ${pageNum}: total=${items.length}, new=${newOnPage.length}`
      );

      for (const it of newOnPage) {
        collected.push(it);
        seen.add(it.nsuid);
      }

      if (newOnPage.length === 0) {
        console.log(`⏹️ [${source.name}] Page ${pageNum} had no new NSUIDs. Stopping.`);
        break;
      }

      await delay(PAGE_DELAY_MS);
    } catch (err) {
      console.warn(
        `⚠️ [${source.name}] Page failed: ${err.message || err}. Continuing...`
      );
      await delay(1000);
    }
  }

  return collected;
}

async function fetchHKGames() {
  console.log('▶️ Starting HK games fetch...');

  const browser = await createBrowser();
  const context = await createContext(browser);

  try {
    const allItems = [];
    const globalSeen = new Set();

    for (const source of SOURCES) {
      const items = await fetchFromSource(context, source);

      for (const it of items) {
        if (globalSeen.has(it.nsuid)) continue;
        globalSeen.add(it.nsuid);
        allItems.push(it);
      }
    }

    console.log(`▶️ Completed HK games fetch. Total unique across all sources: ${allItems.length}`);
    return allItems.map(mapItemToSchema);
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

async function main() {
  const outDir = path.join(__dirname, 'data');
  const outPath = path.join(outDir, 'hk_games.json');

  const fetched = await fetchHKGames();

  fs.mkdirSync(outDir, { recursive: true });

  const existing = loadJsonArraySafe(outPath);
  const existingKeys = new Set(existing.map((e) => e.nsuid_hk).filter(Boolean));

  const newEntries = fetched.filter((e) => e.nsuid_hk && !existingKeys.has(e.nsuid_hk));
  const merged = existing.concat(newEntries);

  fs.writeFileSync(outPath, JSON.stringify(merged, null, 2));

  console.log(`✅ Saved HK games to ${outPath}`);
  console.log(`ℹ️ Existing: ${existing.length} | New: ${newEntries.length} | Total: ${merged.length}`);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { fetchHKGames };