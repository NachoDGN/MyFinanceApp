from __future__ import annotations

import json
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

from ft_fund_history import (
  build_ajax_url,
  build_page_url,
  extract_historical_config,
  fetch_ft_history,
  parse_price_rows,
)
from generate_fund_nav_migration import build_migration_sql


SAMPLE_PAGE_HTML = """
<div data-f2-app-id="mod-tearsheet-historical-prices">
  <div
    data-module-name="HistoricalPricesApp"
    data-mod-config="{&quot;inception&quot;:&quot;2014-02-27T00:00:00Z&quot;,&quot;symbol&quot;:&quot;72731963&quot;}"
  ></div>
</div>
"""

SAMPLE_ROWS_HTML = """
<tr>
  <td class="mod-ui-table__cell--text">
    <span class="mod-ui-hide-small-below">Wednesday, January 10, 2024</span>
    <span class="mod-ui-hide-medium-above">Wed, Jan 10, 2024</span>
  </td>
  <td>178.81</td>
  <td>178.81</td>
  <td>178.81</td>
  <td>178.81</td>
  <td><span class="mod-ui-hide-small-below">0</span></td>
</tr>
<tr>
  <td class="mod-ui-table__cell--text">
    <span class="mod-ui-hide-small-below">Tuesday, January 09, 2024</span>
    <span class="mod-ui-hide-medium-above">Tue, Jan 09, 2024</span>
  </td>
  <td>180.34</td>
  <td>180.34</td>
  <td>180.34</td>
  <td>180.34</td>
  <td><span class="mod-ui-hide-small-below">0</span></td>
</tr>
"""


class FtFundHistoryTests(unittest.TestCase):
  def test_extract_historical_config(self) -> None:
    security = extract_historical_config(
      SAMPLE_PAGE_HTML,
      page_url="https://markets.ft.com/data/funds/tearsheet/historical?s=IE0031786696:EUR",
      public_symbol="IE0031786696:EUR",
    )

    self.assertEqual(security.public_symbol, "IE0031786696:EUR")
    self.assertEqual(security.internal_symbol, "72731963")
    self.assertEqual(security.inception_date, "2014-02-27")

  def test_parse_price_rows(self) -> None:
    rows = parse_price_rows(SAMPLE_ROWS_HTML)

    self.assertEqual(
      [(row.date, row.close) for row in rows],
      [("2024-01-10", "178.81"), ("2024-01-09", "180.34")],
    )

  def test_fetch_ft_history_across_chunks(self) -> None:
    public_symbol = "IE0031786696:EUR"
    responses = {
      build_page_url(public_symbol): SAMPLE_PAGE_HTML,
      build_ajax_url("72731963", date(2024, 1, 1), date(2024, 12, 31)): json.dumps(
        {
          "html": """
<tr><td><span class="mod-ui-hide-small-below">Tuesday, December 31, 2024</span></td><td>210.88</td><td>210.88</td><td>210.88</td><td>210.88</td><td><span class="mod-ui-hide-small-below">0</span></td></tr>
<tr><td><span class="mod-ui-hide-small-below">Monday, January 01, 2024</span></td><td>170.00</td><td>170.00</td><td>170.00</td><td>170.00</td><td><span class="mod-ui-hide-small-below">0</span></td></tr>
"""
        }
      ),
      build_ajax_url("72731963", date(2025, 1, 1), date(2025, 1, 3)): json.dumps(
        {
          "html": """
<tr><td><span class="mod-ui-hide-small-below">Friday, January 03, 2025</span></td><td>211.11</td><td>211.11</td><td>211.11</td><td>211.11</td><td><span class="mod-ui-hide-small-below">0</span></td></tr>
"""
        }
      ),
    }

    def fake_fetch(url: str) -> str:
      try:
        return responses[url]
      except KeyError as exc:
        raise AssertionError(f"Unexpected URL requested: {url}") from exc

    result = fetch_ft_history(
      public_symbol,
      start_date=date(2024, 1, 1),
      end_date=date(2025, 1, 3),
      chunk_years=1,
      fetch_text_fn=fake_fetch,
    )

    self.assertEqual(result.security.internal_symbol, "72731963")
    self.assertEqual(
      [row.date for row in result.rows],
      ["2024-01-01", "2024-12-31", "2025-01-03"],
    )

  def test_build_migration_sql_supports_ft_overrides(self) -> None:
    sql_text = build_migration_sql(
      workbook_path=Path("ft_markets.json"),
      source_label="FT Markets IE0031786696:EUR",
      security_id="security-id",
      name="Vanguard Emerging Markets Stock Index Fund EUR Acc",
      provider_name="manual_fund_nav",
      provider_symbol="IE0031786696",
      canonical_symbol="IE0031786696",
      display_symbol="IE0031786696",
      exchange_name="Manual NAV",
      asset_type="other",
      quote_currency="EUR",
      country="IE",
      mic_code=None,
      isin="IE0031786696",
      figi=None,
      price_source="ft_markets_nav",
      market_state="reference_nav",
      metadata_json={"historySource": "ft_markets"},
      sheet_name=None,
      aliases=[],
      price_rows=[
        ("2024-01-09", Decimal("180.34")),
        ("2024-01-10", Decimal("178.81")),
      ],
      price_raw_json={"importSource": "ft_markets", "priceType": "nav"},
      merge_metadata_json_on_conflict=True,
      preserve_existing_security_fields_on_conflict=True,
    )

    self.assertIn("FT Markets IE0031786696:EUR", sql_text)
    self.assertIn("ft_markets_nav", sql_text)
    self.assertIn('"importSource":"ft_markets"', sql_text)
    self.assertIn("metadata_json = coalesce(public.securities.metadata_json", sql_text)
    self.assertIn("canonical_symbol = public.securities.canonical_symbol", sql_text)


if __name__ == "__main__":
  unittest.main()
