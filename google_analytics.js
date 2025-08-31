#!/usr/bin/env node
/**
 * GA4 -> write Popularity into csvjson.json by matching slug from fullPageUrl.
 * Then sort by SCORE (desc) and regenerate csvjson.csv.
 */

const fs = require('fs');
const path = require('path');
const { BetaAnalyticsDataClient } = require('@google-analytics/data');

const PROPERTY_ID = '272381607';

// Adjust these paths for your machine:
const CREDENTIALS_JSON_PATH = 'C:\\Users\\maysa\\Desktop\\bggen\\maysalebaph-e5a6a9b41af1.json';
const JSON_FILE_PATH = 'C:\\Users\\maysa\\Desktop\\switch-games\\csvjson.json';
const CSV_FILE_PATH = path.join(path.dirname(JSON_FILE_PATH), 'csvjson.csv');

// --- helpers ---
function extractSlug(url) {
  if (!url) return '';
  const parts = String(url).split('/').filter(Boolean);
  return parts.length ? parts[parts.length - 1] : '';
}

async function readJsonArray(filePath) {
  const raw = await fs.promises.readFile(filePath, 'utf8');
  const data = JSON.parse(raw);
  if (!Array.isArray(data)) throw new Error(`Expected an array in ${filePath}`);
  return data;
}

async function writeJson(filePath, data) {
  const json = JSON.stringify(data, null, 2);
  await fs.promises.writeFile(filePath, json, 'utf8');
}

function toCsv(rows) {
  // Build ordered header set in first-seen order across all rows
  const headerSet = [];
  const seen = new Set();
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (!seen.has(k)) {
        seen.add(k);
        headerSet.push(k);
      }
    }
  }

  const esc = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    // Quote if contains comma, quote, or newline
    if (/[",\n]/.test(s)) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };

  const headerLine = headerSet.map(esc).join(',');
  const dataLines = rows.map(row =>
    headerSet.map(k => esc(row[k])).join(',')
  );

  return [headerLine, ...dataLines].join('\n');
}

async function writeCsv(filePath, rows) {
  const csv = toCsv(rows);
  await fs.promises.writeFile(filePath, csv, 'utf8');
}

async function run() {
  // Create GA client using the key file (same as Java credentials)
  const analyticsData = new BetaAnalyticsDataClient({ keyFilename: CREDENTIALS_JSON_PATH });

  // Build request equivalent to Java version
  const request = {
    property: `properties/${PROPERTY_ID}`,
    dimensions: [{ name: 'fullPageUrl' }],
    metrics: [{ name: 'screenPageViews' }],
    dateRanges: [{ startDate: 'yesterday', endDate: 'today' }],
  };

  console.log('Running GA4 report…');
  const [response] = await analyticsData.runReport(request);

  // slug -> screenPageViews (string, like Java)
  const slugToViews = new Map();
  if (response.rows) {
    for (const row of response.rows) {
      const dim = row.dimensionValues?.[0]?.value || '';
      const slug = extractSlug(dim);
      const metric = row.metricValues?.[0]?.value || '';
      if (!slugToViews.has(slug)) slugToViews.set(slug, metric);
    }
  }

  console.log('Loading JSON file:', JSON_FILE_PATH);
  const root = await readJsonArray(JSON_FILE_PATH);

  // Update Popularity
  for (const node of root) {
    const slugValue = node?.Slug ?? '';
    const metricValue = slugToViews.get(slugValue) ?? '';
    console.log(`Processing Slug: ${slugValue} -> Popularity: ${metricValue}`);
    node.Popularity = metricValue; // keep as string to mirror Java behavior
  }

  // Sort by SCORE (desc). Missing/NaN -> treat as very small.
  root.sort((a, b) => {
    const aScore = Number(a?.SCORE);
    const bScore = Number(b?.SCORE);
    const aVal = Number.isFinite(aScore) ? aScore : -Infinity;
    const bVal = Number.isFinite(bScore) ? bScore : -Infinity;
    return bVal - aVal; // descending
  });

  console.log('Saving updated JSON…');
  await writeJson(JSON_FILE_PATH, root);

  console.log('Regenerating CSV…');
  await writeCsv(CSV_FILE_PATH, root);

  console.log('Done.');
  console.log('JSON:', JSON_FILE_PATH);
  console.log('CSV :', CSV_FILE_PATH);
}

run().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
