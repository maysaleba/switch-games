/**
 * Nintendo Philippines eShop catalog scraper.
 * Fetches every search page and retains catalog items regardless of sales status.
 * Requires: npm install axios
 * Run: node fetch_ph.js
 * Optional: node fetch_ph.js --include-all-languages
 */
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const API = 'https://search.nintendo.jp/nintendo_soft_ph/search.json';
const OUT_DIR = path.join(__dirname, 'data');
const MASTER = path.join(OUT_DIR, 'ph_games_enriched.json');
const CURRENT = path.join(OUT_DIR, 'ph_games_enriched_current.json');
const LIMIT = 100;
const DELAY_MS = 600;
const INCLUDE_ALL_LANGUAGES = process.argv.includes('--include-all-languages');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const present = (v) => v !== undefined && v !== null && String(v).trim() !== '';
const pick = (a, b) => present(a) ? a : b;

function hasEnglish(field) {
  if (field == null) return false;
  const vals = Array.isArray(field) ? field : String(field).split(/[\s,;|/]+/);
  return vals.some(v => /^en(?:[-_][a-z]+)?$/i.test(String(v).trim()));
}

function slug(title) {
  return String(title || '').normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[\uff01-\uff5e]/g, c => String.fromCharCode(c.charCodeAt(0) - 0xfee0))
    .replace(/’/g, "'").replace(/[\u200B-\u200D\uFEFF]/g, '')
    .toLowerCase().replace(/'/g, '').replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function platformOf(item) {
  const values = [item?.hard, item?.hards, item?.sform_s, item?.platform, item?.hardware]
    .flat().filter(Boolean).join(' ').toUpperCase();

  // BEE is Nintendo Switch 2; unknown defaults to Switch.
  return /(?:^|[^A-Z])BEE(?:[^A-Z]|$)|SWITCH\s*2/.test(values)
    ? 'Nintendo Switch 2' : 'Nintendo Switch';
}

function mapItem(item) {
  const language = item?.lang ?? item?.language ?? null;

  if (!INCLUDE_ALL_LANGUAGES && language != null && !hasEnglish(language)) return null;

  const nsuid = String(item?.nsuid || item?.id || '').trim();
  if (!nsuid) return null;

  const title = item?.title || '';
  const platform = platformOf(item);
  const icode = String(item?.icode || '');
  const imageKey = item?.iurl || '';
  const prefix = platform === 'Nintendo Switch 2' ? 'BEEP' : 'HACP';

  return {
    title,
    url: '',
    urlKey: `${slug(title)}-${platform === 'Nintendo Switch 2' ? 'switch-2' : 'switch'}`,
    platform,
    genres: [],
    releaseDate: item?.pdate || '',
    imageSquare: imageKey
      ? `https://img-eshop.cdn.nintendo.net/i/${imageKey}.jpg`
      : '',
    imageKey,
    publisher: '',
    dlcType: '',
    playerCount: '',
    nsuid_as: nsuid,
    productCode_as: icode ? `${prefix}${icode}` : '',
    supportLanguage: hasEnglish(language) ? 'en' : ''
  };
}

function loadArray(filename) {
  if (!fs.existsSync(filename)) return [];

  const parsed = JSON.parse(fs.readFileSync(filename, 'utf8'));

  if (!Array.isArray(parsed)) {
    throw new Error(`Expected array in ${filename}`);
  }

  return parsed;
}

async function fetchPage(page) {
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const { data } = await axios.get(API, {
        params: { limit: LIMIT, page },
        timeout: 30000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; MSB-catalog-fetcher/1.0)'
        }
      });

      if (!Array.isArray(data?.result?.items)) {
        throw new Error('Unexpected API response: result.items is not an array');
      }

      return data;
    } catch (err) {
      if (attempt === 4) throw err;

      console.warn(`Page ${page}: ${err.message}; retry ${attempt}/3`);
      await sleep(1000 * (2 ** (attempt - 1)));
    }
  }
}

async function main() {
  const fetched = new Map();
  let page = 1;
  let received = 0;
  let missingId = 0;
  let filteredLanguage = 0;

  while (true) {
    const data = await fetchPage(page);
    const items = data.result.items;

    if (page === 1) {
      console.log(`API reports total: ${data.result.total ?? 'unknown'}`);
    }

    if (items.length === 0) {
      console.log(`Page ${page}: empty; stopping.`);
      break;
    }

    received += items.length;

    for (const item of items) {
      const id = String(item?.nsuid || item?.id || '').trim();

      if (!id) {
        missingId++;
        continue;
      }

      const mapped = mapItem(item);

      if (!mapped) {
        filteredLanguage++;
        continue;
      }

      if (!fetched.has(id)) {
        fetched.set(id, mapped);
      }
    }

    console.log(`Page ${page}: ${items.length} items; ${fetched.size} unique retained`);

    page++;
    await sleep(DELAY_MS);
  }

  const prior = loadArray(MASTER);
  const previousById = new Map(
    prior.filter(x => x?.nsuid_as).map(x => [String(x.nsuid_as), x])
  );

  const now = new Date().toISOString();
  const output = [];

  for (const id of new Set([...previousById.keys(), ...fetched.keys()])) {
    const old = previousById.get(id) || {};
    const fresh = fetched.get(id);
    const merged = { ...old };

    if (fresh) {
      for (const [key, value] of Object.entries(fresh)) {
        if (key === 'genres') {
          merged[key] = Array.isArray(value) && value.length
            ? value
            : (old.genres || []);
        } else {
          merged[key] = pick(value, old[key]);
        }
      }
    }

    merged.active_in_base = Boolean(fresh);
    merged.first_seen_at = old.first_seen_at || now;
    merged.last_seen_at = fresh ? now : (old.last_seen_at || now);
    merged.last_checked_at = fresh ? now : (old.last_checked_at || now);

    output.push(merged);
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(MASTER, JSON.stringify(output, null, 2), 'utf8');

  const active = output.filter(x => x.active_in_base);
  fs.writeFileSync(CURRENT, JSON.stringify(active, null, 2), 'utf8');

  console.log(`Done. Raw items: ${received}; active PH catalog items: ${active.length}; master: ${output.length}`);
  console.log(`Missing IDs: ${missingId}; excluded by language: ${filteredLanguage}`);
  console.log(`Saved: ${MASTER}\nSaved: ${CURRENT}`);
  console.log('Note: Switch 2 platform detection is heuristic; inspect API fields for precise classification.');
}

main().catch(err => {
  console.error('Scrape failed; output not overwritten:', err.message);
  process.exitCode = 1;
});
