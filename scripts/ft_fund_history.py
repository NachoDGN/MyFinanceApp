from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape
from typing import Callable, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

FT_PAGE_URL = "https://markets.ft.com/data/funds/tearsheet/historical"
FT_AJAX_URL = "https://markets.ft.com/data/equities/ajax/get-historical-prices"
USER_AGENT = (
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
)

HTML_TAG_RE = re.compile(r"<[^>]+>")
TABLE_ROW_RE = re.compile(r"<tr>(.*?)</tr>", re.S)
TABLE_CELL_RE = re.compile(r"<td(?:\s+[^>]*)?>(.*?)</td>", re.S)
VISIBLE_TEXT_RE = re.compile(r'mod-ui-hide-small-below">([^<]+)</span>')
HISTORICAL_MODULE_RE = re.compile(
  r'data-f2-app-id="mod-tearsheet-historical-prices".*?data-mod-config="([^"]+)"',
  re.S,
)


@dataclass(frozen=True)
class FtPriceRow:
  date: str
  open: str
  high: str
  low: str
  close: str
  volume: str


@dataclass(frozen=True)
class FtSecurity:
  page_url: str
  public_symbol: str
  internal_symbol: str
  inception_date: str | None


@dataclass(frozen=True)
class FtHistoricalResult:
  security: FtSecurity
  rows: list[FtPriceRow]


FetchText = Callable[[str], str]


def fetch_text(url: str) -> str:
  request = Request(url, headers={"User-Agent": USER_AGENT})
  with urlopen(request, timeout=30) as response:
    charset = response.headers.get_content_charset() or "utf-8"
    return response.read().decode(charset, errors="replace")


def parse_iso_date(value: str) -> date:
  return datetime.strptime(value, "%Y-%m-%d").date()


def build_public_symbol(isin: str, currency: str) -> str:
  normalized_isin = isin.strip().upper()
  normalized_currency = currency.strip().upper()
  if not normalized_isin or not normalized_currency:
    raise ValueError("ISIN and currency are required to build an FT public symbol.")
  return f"{normalized_isin}:{normalized_currency}"


def build_page_url(public_symbol: str) -> str:
  return f"{FT_PAGE_URL}?{urlencode({'s': public_symbol})}"


def add_years(value: date, years: int) -> date:
  try:
    return value.replace(year=value.year + years)
  except ValueError:
    return value.replace(month=2, day=28, year=value.year + years)


def iter_date_ranges(
  start_date: date,
  end_date: date,
  chunk_years: int,
) -> Iterable[tuple[date, date]]:
  if chunk_years < 1:
    raise ValueError("--chunk-years must be at least 1.")
  cursor = start_date
  while cursor <= end_date:
    chunk_end = add_years(cursor, chunk_years) - timedelta(days=1)
    if chunk_end > end_date:
      chunk_end = end_date
    yield cursor, chunk_end
    cursor = chunk_end + timedelta(days=1)


def extract_historical_config(
  page_html: str,
  *,
  page_url: str,
  public_symbol: str,
) -> FtSecurity:
  match = HISTORICAL_MODULE_RE.search(page_html)
  if not match:
    raise RuntimeError(
      f"Could not find FT historical-prices metadata for {public_symbol} at {page_url}."
    )
  config = json.loads(unescape(match.group(1)))
  symbol = str(config.get("symbol") or "").strip()
  if not symbol:
    raise RuntimeError(f"FT page did not expose an internal symbol id for {public_symbol}.")
  inception_raw = str(config.get("inception") or "").strip()
  return FtSecurity(
    page_url=page_url,
    public_symbol=public_symbol,
    internal_symbol=symbol,
    inception_date=inception_raw[:10] if inception_raw else None,
  )


def resolve_ft_security(
  public_symbol: str,
  fetch_text_fn: FetchText = fetch_text,
) -> FtSecurity:
  page_url = build_page_url(public_symbol)
  return extract_historical_config(
    fetch_text_fn(page_url),
    page_url=page_url,
    public_symbol=public_symbol,
  )


def build_ajax_url(
  internal_symbol: str,
  start_date: date,
  end_date: date,
) -> str:
  params = {
    "startDate": start_date.strftime("%Y/%m/%d"),
    "endDate": end_date.strftime("%Y/%m/%d"),
    "symbol": internal_symbol,
  }
  return f"{FT_AJAX_URL}?{urlencode(params)}"


def clean_cell_text(cell_html: str) -> str:
  visible_match = VISIBLE_TEXT_RE.search(cell_html)
  if visible_match:
    return unescape(visible_match.group(1)).strip()
  return " ".join(HTML_TAG_RE.sub(" ", unescape(cell_html)).split())


def parse_price_rows(rows_html: str) -> list[FtPriceRow]:
  rows: list[FtPriceRow] = []
  for raw_row in TABLE_ROW_RE.findall(rows_html):
    cells = TABLE_CELL_RE.findall(raw_row)
    if len(cells) < 6:
      continue
    date_text = clean_cell_text(cells[0])
    price_date = datetime.strptime(date_text, "%A, %B %d, %Y").date().isoformat()
    rows.append(
      FtPriceRow(
        date=price_date,
        open=clean_cell_text(cells[1]),
        high=clean_cell_text(cells[2]),
        low=clean_cell_text(cells[3]),
        close=clean_cell_text(cells[4]),
        volume=clean_cell_text(cells[5]),
      )
    )
  return rows


def fetch_price_rows(
  security: FtSecurity,
  start_date: date,
  end_date: date,
  *,
  chunk_years: int = 1,
  fetch_text_fn: FetchText = fetch_text,
) -> list[FtPriceRow]:
  rows_by_date: dict[str, FtPriceRow] = {}
  for chunk_start, chunk_end in iter_date_ranges(start_date, end_date, chunk_years):
    payload = json.loads(
      fetch_text_fn(build_ajax_url(security.internal_symbol, chunk_start, chunk_end))
    )
    html = str(payload.get("html") or "")
    for row in parse_price_rows(html):
      rows_by_date[row.date] = row
  return sorted(rows_by_date.values(), key=lambda row: row.date)


def fetch_ft_history(
  public_symbol: str,
  *,
  start_date: date,
  end_date: date,
  chunk_years: int = 1,
  fetch_text_fn: FetchText = fetch_text,
) -> FtHistoricalResult:
  security = resolve_ft_security(public_symbol, fetch_text_fn=fetch_text_fn)
  rows = fetch_price_rows(
    security,
    start_date,
    end_date,
    chunk_years=chunk_years,
    fetch_text_fn=fetch_text_fn,
  )
  return FtHistoricalResult(security=security, rows=rows)
