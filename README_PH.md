# Philippines (PH) catalog and pricing logic

This document describes how the Philippines catalog is fetched, filtered for sales, merged, and used in the generated game data.

## Data flow

1. `fetch_ph.js` downloads the Nintendo Philippines eShop search catalog, reads `sale_flg`, and writes the full master catalog plus a current snapshot of all items from the latest fetch.
2. `merge_enriched.js` reads `data/ph_games_enriched_current.json` as its `as` region input. PH uses the shared Asia identifier fields (`nsuid_as`, `productCode_as`, etc.), so it can merge PH identifiers even when the PH item itself is not on sale.
3. `getPrices.js` fetches PH prices through Nintendo's standard price API in the same run as the other selected regions, using PH NSUIDs from the merged entries.
4. `sync_build_csvjson.js` outputs the PH sale amount as `PhilippinesPrice`. PH is also a fallback source for the general `Price` field, after US, AU, and JP, and before HK.

## Fetching the catalog

Run `node fetch_ph.js`. The scraper requests pages from Nintendo's PH search API in batches of 100, retries failed pages with exponential delays, and deduplicates items by NSUID.

By default, if an item provides language metadata, it is retained only when English is listed. Items with no language metadata are not rejected for that reason. To retain all languages, run `node fetch_ph.js --include-all-languages`.

The scraper maps Nintendo's item data to the shared catalog shape:

- `nsuid_as` is populated from the item NSUID (or ID fallback).
- `productCode_as` is built from the item product code, using `HACP` for Switch and `BEEP` for Switch 2.
- `urlKey` is a normalized title slug with `-switch` or `-switch-2` appended.
- `imageSquare` is built from Nintendo's PH image CDN identifier (`iurl`).
- Switch 2 detection is heuristic: API fields containing `BEE` or `Switch 2` identify it; other items default to Switch.

The scraper stores the search API's `sale_flg` and a boolean `ph_on_sale` on each mapped item. Only `sale_flg === "1"` makes that PH item's `active_in_base` true; otherwise it is false. The master catalog preserves items that disappear from the latest fetch, marking them inactive; `first_seen_at`, `last_seen_at`, and `last_checked_at` track catalog observation times. The current snapshot contains every item in the latest fetch, regardless of sale status, and is the PH input used by the merger. This lets a non-sale PH listing contribute its NSUID to a matched game while another region's `active_in_base` can keep the merged game active.

## PH price fetching

There is no PH-specific pre-pricing step in the workflow. After `fetch_ph.js` writes the full current snapshot, `merge_enriched.js` merges it and the normal `getPrices.js` run requests PH prices together with the other selected regions. `getPrices.js` only sends NSUIDs for merged entries whose combined `active_in_base` is true. Therefore, a PH item marked not on sale is still price-checked if it matches a game active in at least one other region. Ensure the `PH` key remains in `getPrices.js`'s `regionSets`, and include it in the chosen group (the `asia` group and `all` include it by default). `price_ph.js` is deprecated and now refuses to run so it cannot overwrite the all-items PH merge snapshot.

The shared price API rows are normalized to the standard price shape:

- `regular` and `regular_currency`
- `sale` and `sale_currency`
- `sale_start` and `sale_end`

The search API's `sale_flg` controls the PH source's `active_in_base`, not whether the catalog item is merged. `getPrices.js` obtains detailed PH regular/sale values and start/end timestamps for merged active games that have a PH NSUID and stores them under `prices.PH`. Existing cached PH prices are reused under the same active-sale/cache rules as other regions.

## Merge and output behavior

The merger internally calls the PH source region `as` because it shares the Asia catalog identifier schema. In the merged output it can expose PH aliases such as `nsuid_ph`, `productCode_ph`, `supportLanguage_ph`, and `imageSquare_ph` from their corresponding `*_as` fields. The PH price snapshot itself is attached as `prices.PH`.

The generated CSV/JSON uses `PhilippinesPrice` for the PH sale price when available (falling back to the PH regular price in the region-price mapping). The generic `Price` column is a regular price selected in this order: US → AU → JP → PH → HK. The first non-empty regular price wins, and its regular currency is written to `Currency`.

Image selection in `sync_build_csvjson.js` depends on the first available NSUID, checked in this order: US → EU → JP → HK → AS → PH → KR → generic `nsuid`. If that NSUID begins with `7001`, image priority is `imageSquare_eu` → `imageSquare` → `imageSquare_ph` → `imageSquare_jp` → `imageSquare_as`; otherwise it is `imageSquare` → `imageSquare_ph` → `imageSquare_jp` → `imageSquare_eu` → `imageSquare_as`. `imageKey` is the fallback if none of those image fields is populated. The selected image URL is sent through the weserv image proxy at width 240. Since the PH scraper initially supplies its image as the generic `imageSquare` field, the final image depends on which generic and regional image fields are present on the merged record.

## Typical run order and test

Run these from the repository root:

1. `node fetch_ph.js`
2. `node merge_enriched.js` (merges PH identifiers whether or not PH is on sale)
3. `node getPrices.js asia` (or `node getPrices.js all`) to fetch PH and the other selected region prices.
4. `node sync_build_csvjson.js` to rebuild the CSV/JSON outputs.

Run the PH catalog flag test with `node --test test/ph-catalog.test.js`.
