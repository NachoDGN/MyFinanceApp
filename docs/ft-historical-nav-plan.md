# FT Historical NAV Plan

## Goal

Add a repeatable way to fetch historical fund NAV/share prices from the Financial Times fund tearsheet backend for exact ISIN share classes, then import those prices into `public.security_prices` without disturbing the current stock and ETF quote refresh flow.

This is intended as an internal, low-cost data acquisition path for funds where official issuer history is hard to access. It should be treated as a secondary source, not the highest-trust source in the system.

## Current State

- `packages/market-data/src/index.ts` is Twelve Data only.
- `packages/db/src/market-data-refresh.ts` refreshes only owned `twelve_data` stocks and ETFs.
- `scripts/generate_fund_nav_migration.py` already imports dated fund NAV rows into `public.security_prices`.
- The domain logic already prefers `manual_nav_import` rows and other NAV-shaped rows when choosing fund prices.
- `packages/llm/src/tasks/lookup-historical-fund-price.ts` already names FT tearsheets as an acceptable secondary source once the exact ISIN has been confirmed.
- `scripts/fetch_ft_fund_history.py` is now a working prototype that:
  - resolves the FT internal symbol id from the tearsheet page
  - calls the FT historical-price backend
  - normalizes rows to CSV or JSON

## Reverse-Engineered FT Contract

The public page is:

- `https://markets.ft.com/data/funds/tearsheet/historical?s=<ISIN>:<CURRENCY>`

The page exposes a hidden internal symbol id in the historical-prices module config. The backend request is:

- `https://markets.ft.com/data/equities/ajax/get-historical-prices?startDate=YYYY/MM/DD&endDate=YYYY/MM/DD&symbol=<internal_id>`

Observed behavior:

- The UI says one year at a time.
- The backend accepts multi-year ranges.
- The payload is JSON with an `html` field containing table rows.

## Design Principles

- Keep FT support separate from the Twelve Data live quote path.
- Preserve `manual_nav_import` as the higher-priority source.
- Treat FT as a fund-history ingestion source, not a real-time quote source.
- Require exact security identity before importing rows.
- Keep imports idempotent by upserting on `(security_id, price_date, source_name)`.
- Store enough raw metadata to trace where each row came from.

## Proposed Architecture

### 1. Provider Layer

Add a dedicated FT historical-fund client, separate from `TwelveDataProvider`.

Suggested new module:

- `packages/market-data/src/ft-funds.ts`

Suggested responsibilities:

- build public symbol from `ISIN:CURRENCY`
- fetch and parse the tearsheet page
- extract FT internal symbol id and inception date
- fetch historical rows for a requested date range
- normalize rows into a repo-native structure

Suggested output shape:

- `priceDate`
- `quoteTimestamp`
- `price`
- `currency`
- `sourceName = "ft_markets_nav"`
- `isRealtime = false`
- `isDelayed = true`
- `marketState = "reference_nav"`
- `rawJson` including:
  - `priceType = "nav"`
  - `source = "ft_markets"`
  - `pageUrl`
  - `publicSymbol`
  - `internalSymbol`
  - `requestStartDate`
  - `requestEndDate`

### 2. Import Layer

Do not wire FT directly into `refreshOwnedStockPrices()` first.

Instead, add an explicit fund-history import path:

- `scripts/import_ft_fund_history.py` or
- extend `scripts/generate_fund_nav_migration.py` to accept FT JSON or CSV input

Recommendation:

- keep `scripts/fetch_ft_fund_history.py` as the low-level fetcher
- add a second script that converts FT rows into migration SQL for one security

This keeps scraping, normalization, and database import concerns separate.

### 3. Security Model Strategy

Do not create a new security provider just because the prices came from FT.

Recommendation:

- keep fund securities as `provider_name = "manual_fund_nav"` when they are manually curated fund securities in the dataset
- write FT-derived prices with `source_name = "ft_markets_nav"`
- leave `manual_nav_import` available as the stronger override when we later import issuer-sourced or workbook-sourced data

This avoids creating duplicate securities for the same ISIN while preserving source ranking.

### 4. Optional App Workflow

After the script-based path is stable, add an optional application workflow:

- server action or API route to backfill a specific fund by ISIN and currency
- callable from an investments admin screen or review flow
- restricted to exact ISIN-bound fund securities

This should be phase 2 or 3, not phase 1.

## Implementation Phases

### Phase 1: Harden the Fetch Prototype

Work items:

- move FT parsing helpers into a reusable module under `packages/market-data`
- keep the script as a thin CLI wrapper
- add retries, timeout handling, and friendlier error messages
- add fixture-based parser tests for:
  - tearsheet page config extraction
  - historical table row parsing
  - empty-range responses
- chunk long requests by year even if FT accepts larger ranges

Exit criteria:

- a single command can fetch a deterministic JSON payload for a known ISIN share class
- parser logic is covered without live network dependency

### Phase 2: Import Into the Dataset Safely

Work items:

- add a script that takes:
  - `--isin`
  - `--currency`
  - `--name`
  - `--start`
  - `--end`
  - `--output`
- emit migration SQL compatible with the existing `public.securities` and `public.security_prices` schema
- set:
  - `source_name = "ft_markets_nav"`
  - `market_state = "reference_nav"`
  - `raw_json.priceType = "nav"`
- ensure upserts are idempotent

Exit criteria:

- one command can produce a migration for a backfill window
- applying the migration adds or refreshes FT-sourced historical NAV rows for an existing manual fund security

### Phase 3: Integrate With Repository Services

Work items:

- add a DB service for fund-history imports, likely in `packages/db/src`
- resolve target security by exact ISIN plus quote currency
- reject ambiguous or mismatched targets
- write audit-friendly result summaries:
  - rows fetched
  - rows inserted
  - rows updated
  - skipped reasons

Exit criteria:

- repo code can backfill one known fund without requiring hand-built SQL

### Phase 4: Add UI or API Trigger

Work items:

- add a protected action or route for FT backfills
- surface status and skipped reasons
- revalidate workspace paths after import

Exit criteria:

- a user can request a fund backfill from the app without dropping to the shell

## Testing Plan

Add tests for:

- FT page metadata parsing
- FT HTML row parsing
- range chunking and deduplication
- security resolution by exact ISIN plus currency
- migration generation for FT rows
- source precedence behavior:
  - `manual_nav_import` remains preferred over `ft_markets_nav`
  - FT rows still beat placeholder or missing market data

Likely test files:

- `test/ft-fund-history.test.ts`
- `test/fund-nav-import.test.ts`

## Risks

- FT may change markup or the hidden endpoint without warning.
- FT terms may become relevant if usage expands beyond personal/internal use.
- Some share classes may need exact currency disambiguation even with the same ISIN family.
- HTML-in-JSON responses are brittle compared with official APIs.

## Suggested First Implementation Slice

The first coded slice after this plan should be:

1. Extract the parser and fetch logic from `scripts/fetch_ft_fund_history.py` into `packages/market-data`.
2. Add parser fixture tests.
3. Add an FT-to-migration script that reuses `scripts/generate_fund_nav_migration.py` conventions.

That slice gives immediate value without forcing a UI or repository-service change too early.
