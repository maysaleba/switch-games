# Philippines (PH) catalog and pricing logic

This document describes how the Philippines catalog is fetched, filtered for sales, merged, and used in the generated game data.

## Data flow

1. `fetch_ph.js` downloads the Nintendo Philippines eShop search catalog and writes `data/ph_games_enriched.json` (the master catalog) and `data/ph_games_enriched_current.json` (the current snapshot).
2. `price_ph.js` requests PH prices for the snapshot's NSUIDs, updates the master catalog with price snapshots, and rewrites the current snapshot to contain only games with a currently active discount.
3. `merge_enriched.js` reads `data/ph_games_enriched_current.json` as its `as` region input. PH uses the shared Asia identifier fields (`nsuid_as`, `productCode_as`, etc.); matching PH games can add their PH price snapshot to the merged record.
4. `getPrices.js` preserves the PH snapshot supplied during the PH pre-price step. It deliberately does not fetch PH prices again or restore stale PH prices from the shared cache.
5. `sync_build_csvjson.js` outputs the PH sale amount as `PhilippinesPrice`. PH is also a fallback source for the general `Price` field, after US, AU, and JP, and before HK.

## Fetching the catalog

Run `node fetch_ph.js`. The scraper requests pages from Nintendo's PH search API in batches of 100, retries failed pages with exponential delays, and deduplicates items by NSUID.

By default, if an item provides language metadata, it is retained only when English is listed. Items with no language metadata are not rejected for that reason. To retain all languages, run `node fetch_ph.js --include-all-languages`.

The scraper maps Nintendo's item data to the shared catalog shape:

- `nsuid_as` is populated from the item NSUID (or ID fallback).
- `productCode_as` is built from the item product code, using `HACP` for Switch and `BEEP` for Switch 2.
- `urlKey` is a normalized title slug with `-switch` or `-switch-2` appended.
- `imageSquare` is built from Nintendo's PH image CDN identifier (`iurl`).
- Switch 2 detection is heuristic: API fields containing `BEE` or `Switch 2` identify it; other items default to Switch.

The master catalog preserves items that disappear from the latest fetch, marking them inactive. `first_seen_at`, `last_seen_at`, and `last_checked_at` track catalog observation times. The current snapshot is the set present in the latest catalog fetch before price filtering.

## PH price check and sale filtering

Run `node price_ph.js` after fetching the catalog. It reads `data/ph_games_enriched_current.json`, requests prices from Nintendo's price API with `country=PH` and `lang=en`, and processes NSUIDs in batches of 50.

Each API row is normalized to the shared price shape:

- `regular` and `regular_currency`
- `sale` and `sale_currency`
- `sale_start` and `sale_end`

A listing is considered currently discounted only when both regular and sale prices exist, both are numeric, and the sale price is less than the regular price. A valid future `sale_start` or an elapsed `sale_end` makes it not currently discounted. If either date is absent, that missing boundary does not by itself disqualify the discount.

The result is stored under `prices.PH`, with `ph_price_checked_at` and `ph_on_sale` metadata. The script sets `active_in_base` to the current discount result and writes only currently discounted listings to `data/ph_games_enriched_current.json`, which is the PH input used by the merger. The master file retains the broader catalog and its latest checked price data.

## Merge and output behavior

The merger internally calls the PH source region `as` because it shares the Asia catalog identifier schema. In the merged output it can expose PH aliases such as `nsuid_ph`, `productCode_ph`, `supportLanguage_ph`, and `imageSquare_ph` from their corresponding `*_as` fields. The PH price snapshot itself is attached as `prices.PH`.

The generated CSV/JSON uses `PhilippinesPrice` for the PH sale price when available (falling back to the PH regular price in the region-price mapping). The generic `Price` column is a regular price selected in this order: US → AU → JP → PH → HK. The first non-empty regular price wins, and its regular currency is written to `Currency`.

Image selection in `sync_build_csvjson.js` depends on the first available NSUID, checked in this order: US → EU → JP → HK → AS → PH → KR → generic `nsuid`. If that NSUID begins with `7001`, image priority is `imageSquare_eu` → `imageSquare` → `imageSquare_ph` → `imageSquare_jp` → `imageSquare_as`; otherwise it is `imageSquare` → `imageSquare_ph` → `imageSquare_jp` → `imageSquare_eu` → `imageSquare_as`. `imageKey` is the fallback if none of those image fields is populated. The selected image URL is sent through the weserv image proxy at width 240. Since the PH scraper initially supplies its image as the generic `imageSquare` field, the final image depends on which generic and regional image fields are present on the merged record.

## Typical run order and test

Run these from the repository root:

1. `node fetch_ph.js`
2. `node price_ph.js`
3. `node merge_enriched.js`
4. `node getPrices.js asia` (or `node getPrices.js all`) to refresh other supported Asia prices; the PH snapshot is intentionally handled separately.
5. `node sync_build_csvjson.js` to rebuild the CSV/JSON outputs.

Run the PH pricing tests with `node --test test/ph-price.test.js`.
