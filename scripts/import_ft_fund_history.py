#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from ft_fund_history import (
  build_public_symbol,
  fetch_price_rows,
  parse_iso_date,
  resolve_ft_security,
)
from generate_fund_nav_migration import build_migration_sql, stable_uuid


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description=(
      "Fetch historical fund NAV data from FT Markets and generate a Supabase "
      "migration that imports the rows into public.security_prices."
    ),
  )
  parser.add_argument("--isin", required=True)
  parser.add_argument("--currency", required=True)
  parser.add_argument("--name", required=True)
  parser.add_argument("--output", required=True, type=Path)
  parser.add_argument("--start")
  parser.add_argument("--end")
  parser.add_argument("--chunk-years", type=int, default=1)
  parser.add_argument("--provider-name", default="manual_fund_nav")
  parser.add_argument("--provider-symbol")
  parser.add_argument("--canonical-symbol")
  parser.add_argument("--display-symbol")
  parser.add_argument("--exchange-name", default="Manual NAV")
  parser.add_argument("--asset-type", default="other")
  parser.add_argument("--quote-currency")
  parser.add_argument("--country")
  parser.add_argument("--mic-code")
  parser.add_argument("--figi")
  parser.add_argument("--alias", action="append", default=[])
  parser.add_argument("--metadata-json", default="{}")
  parser.add_argument("--price-source", default="ft_markets_nav")
  parser.add_argument("--market-state", default="reference_nav")
  return parser.parse_args()


def main() -> None:
  args = parse_args()
  public_symbol = build_public_symbol(args.isin, args.currency)
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

  rows = fetch_price_rows(
    security,
    start_date,
    end_date,
    chunk_years=args.chunk_years,
  )
  if not rows:
    raise RuntimeError(
      f"FT returned no historical price rows for {public_symbol} between "
      f"{start_date.isoformat()} and {end_date.isoformat()}."
    )

  provider_symbol = args.provider_symbol or args.isin.strip().upper()
  security_id_key = args.isin.strip().upper() or f"{args.provider_name}:{provider_symbol}"
  security_id = stable_uuid(f"security:{security_id_key}")
  metadata_json = json.loads(args.metadata_json)
  if not isinstance(metadata_json, dict):
    raise ValueError("--metadata-json must decode to a JSON object.")

  security_metadata = {
    "historySource": "ft_markets",
    "ftPageUrl": security.page_url,
    "ftPublicSymbol": security.public_symbol,
    "ftInternalSymbol": security.internal_symbol,
    **metadata_json,
  }
  price_raw_json = {
    "importSource": "ft_markets",
    "priceType": "nav",
    "pageUrl": security.page_url,
    "publicSymbol": security.public_symbol,
    "internalSymbol": security.internal_symbol,
    "requestedStartDate": start_date.isoformat(),
    "requestedEndDate": end_date.isoformat(),
  }
  canonical_symbol = (
    args.canonical_symbol or args.display_symbol or provider_symbol
  )
  display_symbol = args.display_symbol or canonical_symbol
  sql_text = build_migration_sql(
    workbook_path=Path(f"{security.public_symbol.replace(':', '_')}.json"),
    source_label=f"FT Markets {security.public_symbol}",
    security_id=security_id,
    name=args.name,
    provider_name=args.provider_name,
    provider_symbol=provider_symbol,
    canonical_symbol=canonical_symbol,
    display_symbol=display_symbol,
    exchange_name=args.exchange_name,
    asset_type=args.asset_type,
    quote_currency=(args.quote_currency or args.currency).strip().upper(),
    country=args.country,
    mic_code=args.mic_code,
    isin=args.isin.strip().upper(),
    figi=args.figi,
    price_source=args.price_source,
    market_state=args.market_state,
    metadata_json=security_metadata,
    sheet_name=None,
    aliases=args.alias,
    price_rows=[(row.date, Decimal(row.close)) for row in rows],
    price_raw_json=price_raw_json,
    merge_metadata_json_on_conflict=True,
    preserve_existing_security_fields_on_conflict=True,
  )

  output_path = args.output.expanduser().resolve()
  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_path.write_text(sql_text, encoding="utf-8")
  print(
    json.dumps(
      {
        "output": str(output_path),
        "securityId": security_id,
        "providerName": args.provider_name,
        "providerSymbol": provider_symbol,
        "publicSymbol": security.public_symbol,
        "internalSymbol": security.internal_symbol,
        "pageUrl": security.page_url,
        "rows": len(rows),
        "dateRange": {
          "start": rows[0].date,
          "end": rows[-1].date,
        },
      },
      indent=2,
    )
  )


if __name__ == "__main__":
  main()
