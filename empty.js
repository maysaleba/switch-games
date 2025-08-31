#!/usr/bin/env node
/**
 * Reset all "platform" values to "" in hk_games_enriched.json
 */

const fs = require('fs');
const path = require('path');

// ===== CONFIG =====
const INPUT_FILE = path.join(__dirname, 'data/hk_games.json');
const OUTPUT_FILE = path.join(__dirname, 'data/hk_games.json'); 
// overwrite same file; change OUTPUT_FILE if you want separate copy

function main() {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ Input file not found: ${INPUT_FILE}`);
    process.exit(1);
  }

  const raw = fs.readFileSync(INPUT_FILE, 'utf8');
  let data;
  try {
    data = JSON.parse(raw);
  } catch (err) {
    console.error(`❌ Failed to parse JSON: ${err.message}`);
    process.exit(1);
  }

  // Handle both array and object cases
  if (Array.isArray(data)) {
    data.forEach(item => {
      if ('platform' in item) {
        item.platform = "";
      }
    });
  } else {
    console.warn('⚠️ Input JSON is not an array, updating only top-level "platform"');
    if ('platform' in data) {
      data.platform = "";
    }
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(data, null, 2), 'utf8');
  console.log(`✅ Updated all "platform" fields to "" in ${OUTPUT_FILE}`);
}

main();
