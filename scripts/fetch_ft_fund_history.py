#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

from ft_fund_history import (
  build_public_symbol,
  parse_iso_date,
  fetch_price_rows,
  resolve_ft_security,
)


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description=(
      "Fetch FT historical fund prices for a given ISIN share class symbol "
      "using the same backend endpoint as the markets.ft.com tearsheet."
    ),
  )
  symbol_group = parser.add_mutually_exclusive_group(required=True)
  symbol_group.add_argument(
    "--symbol",
    help="FT public symbol such as IE0031786696:EUR",
  )
  symbol_group.add_argument(
    "--isin",
    help="Fund ISIN. Requires --currency.",
  )
  parser.add_argument(
    "--currency",
    help="Share-class currency for --isin, for example EUR.",
  )
  parser.add_argument(
    "--start",
    help="Inclusive start date in YYYY-MM-DD. Defaults to the FT inception date.",
  )
  parser.add_argument(
    "--end",
    help="Inclusive end date in YYYY-MM-DD. Defaults to today.",
  )
  parser.add_argument(
    "--chunk-years",
    type=int,
    default=1,
    help="Fetch date ranges in year-sized chunks. Default: 1.",
  )
  parser.add_argument(
    "--format",
    choices=["csv", "json"],
    default="csv",
    help="Output format. Default: csv.",
  )
  parser.add_argument(
    "--output",
    type=Path,
    help="Optional output file path. Defaults to stdout.",
  )
  return parser.parse_args()


def normalize_public_symbol(args: argparse.Namespace) -> str:
  if args.symbol:
    return args.symbol.strip().upper()
  if not args.currency:
    raise ValueError("--currency is required when using --isin.")
  return build_public_symbol(args.isin, args.currency)


def write_csv(rows: list[dict[str, str]], output_path: Path | None) -> None:
  header = ["date", "open", "high", "low", "close", "volume"]
  if output_path:
    handle = output_path.open("w", newline="", encoding="utf-8")
  else:
    handle = sys.stdout
  try:
    writer = csv.DictWriter(handle, fieldnames=header)
    writer.writeheader()
    for row in rows:
      writer.writerow(row)
  finally:
    if output_path:
      handle.close()


def write_json(payload: dict[str, object], output_path: Path | None) -> None:
  rendered = json.dumps(payload, indent=2, sort_keys=True)
  if output_path:
    output_path.write_text(rendered + "\n", encoding="utf-8")
    return
  sys.stdout.write(rendered + "\n")


def main() -> int:
  args = parse_args()
  public_symbol = normalize_public_symbol(args)
  security = resolve_ft_security(public_symbol)
  end_date = parse_iso_date(args.end) if args.end else date.today()
  if args.start:
    start_date = parse_iso_date(args.start)
  elif security.inception_date:
    start_date = parse_iso_date(security.inception_date)
  else:
    raise RuntimeError("FT did not expose an inception date, so --start is required.")

  if start_date > end_date:
    raise ValueError("--start must be on or before --end.")

  rows = [asdict(row) for row in fetch_price_rows(
    security,
    start_date,
    end_date,
    chunk_years=args.chunk_years,
  )]
  if args.format == "json":
    write_json(
      {
        "source": "ft_markets",
        "page_url": security.page_url,
        "public_symbol": security.public_symbol,
        "internal_symbol": security.internal_symbol,
        "inception_date": security.inception_date,
        "rows": rows,
      },
      args.output,
    )
  else:
    write_csv(rows, args.output)
  return 0


if __name__ == "__main__":
  try:
    raise SystemExit(main())
  except Exception as exc:
    print(f"error: {exc}", file=sys.stderr)
    raise SystemExit(1)
