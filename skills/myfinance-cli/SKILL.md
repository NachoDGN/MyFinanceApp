---
name: myfinance-cli
description: Use when Codex needs to read or operate MyFinanceApp data through this repository's agent-facing CLI, the Next.js HTTP routes under apps/web/app/api, or the workspace metadata needed to resolve account, template, security, metric, rule, and entity identifiers. Covers the full exposed CLI surface, the HTTP endpoints, helper scripts for safe discovery, and the current Commander/runtime quirks in the CLI.
---

# MyFinance CLI

## Overview

Use the bundled helper scripts instead of guessing raw `myfinance` invocations. The current CLI mixes real nested subcommands with Commander command signatures that were declared as positional placeholders, so several commands do not behave like their help text suggests.

## Quick Start

1. Resolve identifiers first with `scripts/discover-myfinance.py --workspace /path/to/MyFinanceApp`.
2. Invoke the CLI through `scripts/run-myfinance.py --workspace /path/to/MyFinanceApp ...`.
3. Default to `--json` so downstream steps can inspect structured output.
4. Treat every write path as mutating only after explicit user intent. Treat `transaction update` as mutating even when `--apply` is omitted.

## Workflow

### Resolve Workspace Metadata

Run `scripts/discover-myfinance.py` before any command that needs ids or slugs. It reads the dataset through the repository and returns:

- `entities` with both `id` and `slug`
- `accounts` with `id`, `entitySlug`, `accountType`, and `lastImportedAt`
- `templates`, `rules`, and `securities`
- `metricIds`
- `scopeExamples`
- supported template values such as `accountTypes`, `signModes`, and `canonicalFieldKeys`

Use this script instead of hardcoding scope values from the CLI help text. The help string mentions `personal`, `company_a`, and `company_b`, but the real entity slugs come from the dataset.

### Read Data

Prefer these commands for read-heavy agent work:

- `dashboard summary`
- `metrics get <metricId>`
- `insights list`
- `transactions list`
- `templates list`
- `rules list`
- `rules drafts`
- `investments holdings`
- `prices refresh`
- `jobs run`

For raw financial reads, prefer the CLI wrapper over HTTP unless the user specifically asks for endpoint calls or you need a route-only capability.

### Perform Mutations Deliberately

Use `--apply` only after the user clearly wants a persistent change. Mutation-capable surfaces include:

- `transaction update`
- `imports commit`
- `templates create`
- `rules queue-draft`
- `rules apply-draft`
- `rules create`
- `investments resolve-security`
- `positions add-opening`
- `positions import-fund-history`
- `jobs run --apply`

Treat `transaction update` as unsafe for dry runs. In live testing, invoking it without `--apply` still changed transaction metadata such as `classification_status`, `classification_source`, and `updatedAt`.

### Use HTTP Routes When Needed

Load `references/http-surface.md` when the task is route-centric, when the user asks about endpoints, or when you need request-body details for the Next.js API layer. The REST surface covers dashboard, metrics, insights, transactions, imports, rules, holdings, securities, and review jobs.

Some write capabilities are not REST routes. They are exposed as Next.js server actions only:

- account create and delete
- template delete
- workspace reset
- prompt profile updates

## Helper Scripts

### `scripts/run-myfinance.py`

Invoke the CLI using the intended syntax instead of the raw Commander quirks.

Examples:

```bash
python3 scripts/run-myfinance.py --workspace /path/to/MyFinanceApp dashboard summary --json
python3 scripts/run-myfinance.py --workspace /path/to/MyFinanceApp metrics get net_worth_current --json
python3 scripts/run-myfinance.py --workspace /path/to/MyFinanceApp transactions list --scope consolidated --json
python3 scripts/run-myfinance.py --workspace /path/to/MyFinanceApp rules queue-draft --text "Mark Stripe payouts as transfers" --apply --json
```

The wrapper rewrites the broken top-level signatures for `dashboard`, `metrics`, `insights`, `transactions`, and `transaction`, then forwards the call through:

```bash
pnpm --dir <workspace> --filter @myfinance/cli exec tsx src/index.ts ...
```

Use `--show-command` when you want to inspect the exact raw invocation.

### `scripts/discover-myfinance.py`

Read the current dataset and supported values without mutating anything.

Example:

```bash
python3 scripts/discover-myfinance.py --workspace /path/to/MyFinanceApp
```

This is the fastest way to discover account ids, entity slugs, template ids, securities, rules, and metric ids before using the CLI or the HTTP routes.

## Import Prerequisites

The import commands ultimately use `python/ingest/runner.py`. The repository auto-detects `.venv/bin/python` and falls back to `python3`, and it auto-detects `python/ingest/runner.py` unless `PYTHON_BIN` or `PYTHON_INGEST_RUNNER` override them.

Before using import preview or commit flows, ensure the ingest dependencies exist:

```bash
pnpm python:setup
```

## References

- `references/cli-surface.md`: full command inventory, canonical syntax, flags, supported values, and observed CLI quirks
- `references/http-surface.md`: full HTTP route inventory plus the non-REST server actions

## Sharp Edges

- Use the wrapper for `dashboard`, `metrics`, `insights`, `transactions`, and `transaction`. Their raw Commander signatures are misleading.
- Treat `transaction update` as mutating regardless of `--apply`.
- Treat `prices refresh --apply` as non-persistent in the current CLI implementation. It toggles the reported `applied` flag but does not write quotes downstream.
- Use discovery output, not the hardcoded help text, to resolve scope values and identifiers.
