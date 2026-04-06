# MyFinanceApp CLI Surface

## Entry Point

Use the wrapper for normal work:

```bash
python3 scripts/run-myfinance.py --workspace /path/to/MyFinanceApp ...
```

The raw CLI entry point inside the repository is:

```bash
pnpm --dir /path/to/MyFinanceApp --filter @myfinance/cli exec tsx src/index.ts ...
```

The package also declares the `myfinance` bin after build, but the wrapper does not require building the CLI first.

## Shared Read Options

These options are reused across most read commands:

- `--scope <scope>`: `consolidated`, an entity slug, or `account:<accountId>`
- `--currency <currency>`: `EUR` or `USD`
- `--period <period>`: `week`, `mtd`, `ytd`, `24m`, or `custom`
- `--as-of <date>`: reference date in `YYYY-MM-DD`
- `--start <date>` and `--end <date>`: custom range bounds
- `--json`: structured output

Discover real scope values with `scripts/discover-myfinance.py`. The hardcoded help text is seed-specific.

## Canonical Syntax vs Raw Runtime Syntax

The wrapper accepts the canonical syntax below and rewrites the broken raw signatures when needed.

- `dashboard summary [common options]`
  - Raw runtime shape: `dashboard <anything> [common options]`
  - Behavior: the second positional token is ignored by the action

- `metrics get <metricId> [--scope ... --currency ... --as-of ... --json]`
  - Raw runtime shape: `metrics <metricId> <anything> [...]`
  - Behavior: `metrics get net_worth_current` fails because the CLI treats `get` as the metric id

- `insights list [--scope ... --as-of ... --json]`
  - Raw runtime shape: `insights <anything> [...]`

- `transactions list [--scope ... --json]`
  - Raw runtime shape: `transactions <anything> [...]`

- `transaction update <transactionId> [patch flags] [--json]`
  - Raw runtime shape: `transaction <transactionId> <anything> [patch flags] [--json]`
  - Safety: treat as mutating even without `--apply`

Everything under `imports`, `templates`, `rules`, `investments`, `positions`, `prices`, and `jobs` is declared as real nested subcommands and can be forwarded as-is.

## Read-Oriented Commands

### Dashboard

- `dashboard summary`
  - Options: `--scope`, `--currency`, `--period`, `--as-of`, `--start`, `--end`, `--json`
  - Returns: dashboard summary with metrics, quality, insights, portfolio allocation, top holdings, and recent transactions

### Metrics

Supported metric ids from `packages/analytics/src/registry.ts`:

- `net_worth_current`
- `cash_total_current`
- `income_mtd_total`
- `spending_mtd_total`
- `operating_net_cash_flow_mtd`
- `portfolio_market_value_current`
- `portfolio_unrealized_pnl_current`
- `pending_review_count`
- `unclassified_amount_mtd`
- `stale_accounts_count`

Command:

- `metrics get <metricId>`
  - Options: `--scope`, `--currency`, `--as-of`, `--json`
  - Returns: one metric result

### Insights

- `insights list`
  - Options: `--scope`, `--as-of`, `--json`
  - Returns: `schemaVersion`, `insights`, and `generatedAt`

### Transactions

- `transactions list`
  - Options: `--scope`, `--json`
  - Returns: `transactions`, `quality`, `scope`, and `generatedAt`

Use this command to harvest transaction ids, account ids, and existing security ids before any transaction-level work.

### Templates

- `templates list`
  - Options: `--json`
  - Returns: import templates and `generatedAt`

### Rules

- `rules list`
  - Options: `--json`
  - Returns: rules sorted by priority

- `rules drafts`
  - Options: `--json`
  - Returns: rule draft jobs plus `parserConfigured`

### Investments

- `investments holdings`
  - Options: `--scope`, `--json`
  - Returns: holdings, quote freshness, brokerage cash, and `generatedAt`

### Prices

- `prices refresh`
  - Options: `--symbol`, `--apply`, `--json`
  - Returns: quotes from the market data provider
  - Current implementation note: `--apply` only changes the reported `applied` flag in the CLI output; the command does not persist refreshed prices downstream

### Jobs

- `jobs run`
  - Options: `--apply`, `--json`
  - Without `--apply`: preview queued work
  - With `--apply`: process queued jobs

## Mutation-Capable Commands

### Transaction Update

- `transaction update <transactionId>`
  - Patch flags: `--class`, `--category`, `--entity`, `--security`, `--quantity`, `--note`, `--needs-review`, `--create-rule`, `--apply`, `--json`
  - Safety: observed to mutate transaction metadata even when `--apply` is omitted
  - Recommendation: only use after explicit user approval

### Imports

- `imports preview`
  - Required: `--account <accountId> --template <templateId> --file <filePath>`
  - Optional: `--json`
  - Reads a local file through `python/ingest/runner.py`

- `imports commit`
  - Required: `--account <accountId> --template <templateId> --file <filePath>`
  - Optional: `--apply`, `--json`
  - Without `--apply`, the CLI falls back to preview behavior

Import prerequisites:

- Run `pnpm python:setup` if `.venv` does not exist yet
- The repository auto-detects `.venv/bin/python`
- Override with `PYTHON_BIN` or `PYTHON_INGEST_RUNNER` if needed

### Templates Create

- `templates create`
  - Required: `--name`, `--institution`, `--account-type`, `--file-kind`, `--default-currency`
  - Optional:
    - `--map <target=source>` repeated
    - `--sign-mode <mode>`
    - `--invert-sign`
    - `--direction-column`
    - `--debit-column`
    - `--credit-column`
    - `--debit-values`
    - `--credit-values`
    - `--date-day-first`
    - `--date-month-first`
    - `--apply`
    - `--json`

Supported values:

- `accountType`: `checking`, `savings`, `company_bank`, `brokerage_cash`, `brokerage_account`, `credit_card`, `other`
- `fileKind`: `csv`, `xlsx`
- `signMode`: `signed_amount`, `amount_direction_column`, `debit_credit_columns`
- `canonicalFieldKeys`:
  - `transaction_date`
  - `posted_date`
  - `description_raw`
  - `amount_original_signed`
  - `currency_original`
  - `balance_original`
  - `external_reference`
  - `transaction_type_raw`
  - `security_symbol`
  - `security_name`
  - `quantity`
  - `unit_price_original`
  - `fees_original`
  - `fx_rate`

### Rules

- `rules queue-draft`
  - Required: `--text <requestText>`
  - Optional: `--apply`, `--json`
  - Queues an LLM-backed rule parse job

- `rules apply-draft`
  - Required: `--job <jobId>`
  - Optional: `--apply`, `--json`

- `rules create`
  - Required: `--priority`, `--regex`, `--class`, `--category`
  - Optional: `--apply`, `--json`
  - Creates a global rule using `normalized_description_regex`

### Investments

- `investments resolve-security`
  - Required: `--transaction <transactionId> --security <securityId>`
  - Optional: `--apply`, `--json`
  - Clears `needsReview` and `reviewReason`

### Positions

- `positions add-opening`
  - Required: `--account <accountId> --entity <entitySlug> --security <securityId> --date <effectiveDate> --quantity <shareDelta>`
  - Optional: `--cost-basis <costBasisDeltaEur>`, `--apply`, `--json`

- `positions import-fund-history`
  - Required: `--account <accountId> --entity <entitySlug> --file <filePath>`
  - Optional: `--apply`, `--json`
  - Reads a text export, patches matched transactions, creates opening positions, and runs pending jobs when `--apply` is set

## Identifier Discovery Strategy

Use `scripts/discover-myfinance.py` first. If you still need live ids tied to a specific command:

- account ids: `discover-myfinance.py` or `transactions list`
- entity slugs: `discover-myfinance.py`
- template ids: `discover-myfinance.py` or `templates list`
- rule ids: `discover-myfinance.py` or `rules list`
- rule draft job ids: `rules drafts`
- security ids: `discover-myfinance.py`, `investments holdings`, `transactions list`, or `/api/securities/search`
- transaction ids: `transactions list`

## Observed Runtime Quirks

- `dashboard`, `insights`, and `transactions` accept any second positional token because that token is only a placeholder in the Commander declaration.
- `metrics get <metricId>` does not work as written in raw mode. The raw CLI wants `metrics <metricId> <anything>`.
- `transaction update <transactionId>` does not work as written in raw mode. The raw CLI wants `transaction <transactionId> <anything>`.
- The `transaction` preview path was observed to change persisted transaction metadata even without `--apply`.
