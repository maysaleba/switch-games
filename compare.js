
const fs = require('fs');
const path = require('path');

// Change these paths to your two JSON files
const FILE_1 = path.join(__dirname, 'data', 'ph_games_enriched.json');
const FILE_2 = path.join(__dirname, 'data', 'ph_games_enriched_current.json');

const LOG_FILE = path.join(__dirname, 'logs', 'compare_as_ids.log');

function loadJson(file) {
  const data = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (!Array.isArray(data)) {
    throw new Error(`${file} must contain a JSON array`);
  }
  return data;
}

function groupByUrlKey(games) {
  const map = new Map();

  for (const game of games) {
    if (!game.urlKey) continue;

    if (!map.has(game.urlKey)) {
      map.set(game.urlKey, []);
    }

    map.get(game.urlKey).push(game);
  }

  return map;
}

function normalize(value) {
  return String(value ?? '').trim();
}

function compareFiles() {
  const games1 = loadJson(FILE_1);
  const games2 = loadJson(FILE_2);

  const map1 = groupByUrlKey(games1);
  const map2 = groupByUrlKey(games2);

  const logs = [];
  let matched = 0;
  let differences = 0;

  for (const [urlKey, entries1] of map1) {
    const entries2 = map2.get(urlKey);
    if (!entries2) continue;

    matched++;

    for (const game1 of entries1) {
      for (const game2 of entries2) {
        const nsuidDifferent =
          normalize(game1.nsuid_as) !== normalize(game2.nsuid_as);

        const productCodeDifferent =
          normalize(game1.productCode_as) !== normalize(game2.productCode_as);

        if (!nsuidDifferent && !productCodeDifferent) continue;

        differences++;

        logs.push([
          '='.repeat(80),
          `Title: ${game1.title || game2.title || 'Unknown'}`,
          `urlKey: ${urlKey}`,
          '',
          `nsuid_as:`,
          `  File 1: ${game1.nsuid_as ?? '(missing)'}`,
          `  File 2: ${game2.nsuid_as ?? '(missing)'}`,
          `  Different: ${nsuidDifferent ? 'YES' : 'NO'}`,
          '',
          `productCode_as:`,
          `  File 1: ${game1.productCode_as ?? '(missing)'}`,
          `  File 2: ${game2.productCode_as ?? '(missing)'}`,
          `  Different: ${productCodeDifferent ? 'YES' : 'NO'}`,
          ''
        ].join('\n'));
      }
    }
  }

  const summary = [
    'AS ID COMPARISON REPORT',
    '='.repeat(80),
    `File 1: ${FILE_1}`,
    `File 2: ${FILE_2}`,
    `File 1 games: ${games1.length}`,
    `File 2 games: ${games2.length}`,
    `Matching urlKeys: ${matched}`,
    `Differences found: ${differences}`,
    '='.repeat(80),
    ''
  ].join('\n');

  fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });
  fs.writeFileSync(LOG_FILE, summary + logs.join('\n'), 'utf8');

  console.log(summary);
  console.log(`Report saved to: ${LOG_FILE}`);
}

try {
  compareFiles();
} catch (error) {
  console.error('Comparison failed:', error.message);
  process.exitCode = 1;
}
