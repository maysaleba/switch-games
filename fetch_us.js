#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const axios = require('axios');

// ---- helpers ----
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function safePost(url, payload, headers, retries = 3) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await axios.post(url, payload, { headers });
    } catch (err) {
      if (attempt === retries - 1) throw err;
      await delay(500 * (attempt + 1));
    }
  }
}

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

function keyOf(hit) {
  return hit && (hit.nsuid || hit.nsuid_us || hit.objectID);
}

// Fetch US games from Algolia (Deals only, Switch + Switch 2)
async function fetchUSGamesOnSale() {
  console.log('▶️ Starting US games fetch...');
//  const indices = ['store_game_en_us'];
//  const indices = ['store_game_en_us_price_asc','store_game_en_us_price_des'];
//  const indices = ['store_game_en_us_release_des'];
  const indices = ['store_game_en_us_title_asc', 'store_game_en_us_title_des'];
  const headers = {
    'Content-Type': 'application/json',
    'x-algolia-agent': 'Algolia for JavaScript (4.23.2); Browser',
    'x-algolia-application-id': 'U3B6GR4UA3',
    'x-algolia-api-key': 'a29c6927638bfd8cee23993e51e721c9'
  };
  const queryPrefixes = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'.split('');
  const hitsPerPage = 100;

  async function fetchGroup(index, query) {
    let page = 0, nbPages = 1;
    const groupHits = [];
    const seen = new Set();

    do {
      const payload = {
        query,
        //filters: '(priceRange:"$40") AND (contentRatingCode:"E10") AND (corePlatforms:"Nintendo Switch")',
        //filters: '(corePlatforms:"Nintendo Switch" OR corePlatforms:"Nintendo Switch 2")',
        filters: '(corePlatforms:"Nintendo Switch" OR corePlatforms:"Nintendo Switch 2") AND (topLevelFilters:"Deals")',
        hitsPerPage,
        page
      };
      const { data } = await safePost(`https://u3b6gr4ua3-dsn.algolia.net/1/indexes/${index}/query`, payload, headers);
      const hits = data.hits || [];
      nbPages = data.nbPages;

      hits.forEach(hit => {
        const k = keyOf(hit);
        if (k && !seen.has(k)) {
          seen.add(k);
          groupHits.push(hit);
        }
      });

      page++;
    } while (page < nbPages);

    await delay(150);
    return groupHits;
  }

  const allResults = await Promise.all(
    indices.flatMap(index => queryPrefixes.map(prefix => fetchGroup(index, prefix)))
  );

  const combined = allResults.flat();

  // --- Deduplicate and map only the fields we want ---
  const unique = Array.from(new Map(combined.map(h => [h.nsuid, h])).values())
    .map(h => ({
      title: h.title || '',
      nsuid_us: h.nsuid || '',
      url: h.url || '',
      urlKey: h.urlKey || '',
      platform: h.platform || 'Nintendo Switch',
      genres: Array.isArray(h.genres) ? h.genres : [],
      releaseDate: h.releaseDate || '',
      imageSquare: h.productImageSquare || '',
      imageKey: h.productImage || '',
      publisher: h.softwarePublisher || '',
      dlcType: h.dlcType || '',
      playerCount: h.playerCount || ''
    }));

  console.log(`▶️ Completed US games fetch. Total unique: ${unique.length}`);
  return unique;
}

async function main() {
  const outDir = path.join(__dirname, 'data');
  const outPath = path.join(outDir, 'us_games.json');

  const fetched = await fetchUSGamesOnSale();
  const existing = loadJsonArraySafe(outPath);
  const existingKeys = new Set(existing.map(e => e.nsuid_us));

  const newEntries = fetched.filter(e => !existingKeys.has(e.nsuid_us));
  const merged = existing.concat(newEntries);

  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(merged, null, 2));

  console.log(`✅ Saved US games to ${outPath}`);
  console.log(`ℹ️ Existing: ${existing.length} | New: ${newEntries.length} | Total: ${merged.length}`);
}

if (require.main === module) {
  main().catch(err => { console.error(err); process.exit(1); });
}

module.exports = { fetchUSGamesOnSale };
