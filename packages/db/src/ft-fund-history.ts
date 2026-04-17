import type { SecurityPrice } from "@myfinance/domain";

const FT_PAGE_URL = "https://markets.ft.com/data/funds/tearsheet/historical";
const FT_AJAX_URL = "https://markets.ft.com/data/equities/ajax/get-historical-prices";
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36";

const HTML_TAG_RE = /<[^>]+>/g;
const TABLE_ROW_RE = /<tr>(.*?)<\/tr>/gs;
const TABLE_CELL_RE = /<td(?:\s+[^>]*)?>(.*?)<\/td>/gs;
const VISIBLE_TEXT_RE = /mod-ui-hide-small-below">([^<]+)<\/span>/;
const HISTORICAL_MODULE_RE =
  /data-f2-app-id="mod-tearsheet-historical-prices".*?data-mod-config="([^"]+)"/s;

export interface FtFundSecurityInfo {
  pageUrl: string;
  publicSymbol: string;
  internalSymbol: string;
  inceptionDate: string | null;
}

export interface FtFundPriceRow {
  priceDate: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
}

export interface FtFundHistoryResult {
  security: FtFundSecurityInfo;
  rows: FtFundPriceRow[];
}

function decodeHtml(value: string) {
  return value
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function toIsoDate(value: Date) {
  return value.toISOString().slice(0, 10);
}

function parseIsoDate(value: string) {
  return new Date(`${value}T00:00:00Z`);
}

function shiftIsoDate(value: string, days: number) {
  const next = parseIsoDate(value);
  next.setUTCDate(next.getUTCDate() + days);
  return toIsoDate(next);
}

function addYears(value: Date, years: number) {
  const next = new Date(value.toISOString());
  next.setUTCFullYear(next.getUTCFullYear() + years);
  if (next.getUTCMonth() !== value.getUTCMonth()) {
    next.setUTCDate(0);
  }
  return next;
}

function iterDateRanges(
  startDate: string,
  endDate: string,
  chunkYears: number,
) {
  if (chunkYears < 1) {
    throw new Error("chunkYears must be at least 1.");
  }

  const ranges: Array<{ startDate: string; endDate: string }> = [];
  let cursor = parseIsoDate(startDate);
  const end = parseIsoDate(endDate);
  while (cursor <= end) {
    const chunkEnd = addYears(cursor, chunkYears);
    chunkEnd.setUTCDate(chunkEnd.getUTCDate() - 1);
    if (chunkEnd > end) {
      chunkEnd.setTime(end.getTime());
    }
    ranges.push({
      startDate: toIsoDate(cursor),
      endDate: toIsoDate(chunkEnd),
    });
    cursor = new Date(chunkEnd.toISOString());
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return ranges;
}

function normalizePublicSymbol(publicSymbol: string) {
  return publicSymbol.trim().toUpperCase();
}

export function buildFtFundPublicSymbol(isin: string, currency: string) {
  const normalizedIsin = isin.trim().toUpperCase();
  const normalizedCurrency = currency.trim().toUpperCase();
  if (!normalizedIsin || !normalizedCurrency) {
    throw new Error("ISIN and currency are required to build an FT public symbol.");
  }
  return `${normalizedIsin}:${normalizedCurrency}`;
}

function buildPageUrl(publicSymbol: string) {
  return `${FT_PAGE_URL}?${new URLSearchParams({ s: publicSymbol }).toString()}`;
}

function buildAjaxUrl(
  internalSymbol: string,
  startDate: string,
  endDate: string,
) {
  return `${FT_AJAX_URL}?${new URLSearchParams({
    startDate: startDate.replaceAll("-", "/"),
    endDate: endDate.replaceAll("-", "/"),
    symbol: internalSymbol,
  }).toString()}`;
}

async function fetchText(url: string) {
  const response = await fetch(url, {
    headers: {
      "user-agent": USER_AGENT,
    },
  });
  if (!response.ok) {
    throw new Error(`FT request failed with HTTP ${response.status} for ${url}.`);
  }
  return await response.text();
}

function cleanCellText(cellHtml: string) {
  const visibleMatch = cellHtml.match(VISIBLE_TEXT_RE);
  if (visibleMatch?.[1]) {
    return decodeHtml(visibleMatch[1]).trim();
  }
  return decodeHtml(cellHtml.replaceAll(HTML_TAG_RE, " "))
    .replace(/\s+/g, " ")
    .trim();
}

export function parseFtFundSecurityPage(
  pageHtml: string,
  input: {
    pageUrl: string;
    publicSymbol: string;
  },
): FtFundSecurityInfo {
  const match = pageHtml.match(HISTORICAL_MODULE_RE);
  if (!match?.[1]) {
    throw new Error(
      `Could not find FT historical metadata for ${input.publicSymbol}.`,
    );
  }
  const config = JSON.parse(decodeHtml(match[1])) as {
    symbol?: string;
    inception?: string;
  };
  if (!config.symbol?.trim()) {
    throw new Error(
      `FT page did not expose an internal symbol for ${input.publicSymbol}.`,
    );
  }
  return {
    pageUrl: input.pageUrl,
    publicSymbol: input.publicSymbol,
    internalSymbol: config.symbol.trim(),
    inceptionDate: config.inception?.slice(0, 10) ?? null,
  };
}

export function parseFtFundPriceRows(rowsHtml: string): FtFundPriceRow[] {
  const rows: FtFundPriceRow[] = [];
  for (const rowMatch of rowsHtml.matchAll(TABLE_ROW_RE)) {
    const cells = [...rowMatch[1].matchAll(TABLE_CELL_RE)].map(
      (match) => match[1],
    );
    if (cells.length < 6) {
      continue;
    }
    const dateText = cleanCellText(cells[0]);
    const parsedDate = new Date(`${dateText} UTC`);
    if (Number.isNaN(parsedDate.getTime())) {
      continue;
    }
    rows.push({
      priceDate: parsedDate.toISOString().slice(0, 10),
      open: cleanCellText(cells[1]),
      high: cleanCellText(cells[2]),
      low: cleanCellText(cells[3]),
      close: cleanCellText(cells[4]),
      volume: cleanCellText(cells[5]),
    });
  }
  return rows;
}

export async function fetchFtFundSecurity(
  publicSymbol: string,
): Promise<FtFundSecurityInfo> {
  const normalized = normalizePublicSymbol(publicSymbol);
  const pageUrl = buildPageUrl(normalized);
  return parseFtFundSecurityPage(await fetchText(pageUrl), {
    pageUrl,
    publicSymbol: normalized,
  });
}

export async function fetchFtFundHistory(input: {
  publicSymbol: string;
  startDate: string;
  endDate: string;
  chunkYears?: number;
}): Promise<FtFundHistoryResult> {
  const security = await fetchFtFundSecurity(input.publicSymbol);
  const rowsByDate = new Map<string, FtFundPriceRow>();
  for (const range of iterDateRanges(
    input.startDate,
    input.endDate,
    input.chunkYears ?? 1,
  )) {
    const rawPayload = await fetchText(
      buildAjaxUrl(security.internalSymbol, range.startDate, range.endDate),
    );
    const payload = JSON.parse(rawPayload) as {
      html?: string | null;
    };
    for (const row of parseFtFundPriceRows(payload.html ?? "")) {
      rowsByDate.set(row.priceDate, row);
    }
  }
  return {
    security,
    rows: [...rowsByDate.values()].sort((left, right) =>
      left.priceDate.localeCompare(right.priceDate),
    ),
  };
}

export async function fetchFtFundPriceAtOrBeforeDate(input: {
  securityId: string;
  isin: string;
  quoteCurrency: string;
  transactionDate: string;
  maxDriftDays: number;
}): Promise<{
  price: SecurityPrice | null;
  security: FtFundSecurityInfo | null;
}> {
  const publicSymbol = buildFtFundPublicSymbol(input.isin, input.quoteCurrency);
  const lookbackStart = shiftIsoDate(input.transactionDate, -input.maxDriftDays);
  const result = await fetchFtFundHistory({
    publicSymbol,
    startDate: lookbackStart,
    endDate: input.transactionDate,
  });
  const row = [...result.rows]
    .filter((candidate) => candidate.priceDate <= input.transactionDate)
    .sort((left, right) => right.priceDate.localeCompare(left.priceDate))[0];
  if (!row) {
    return {
      price: null,
      security: result.security,
    };
  }
  return {
    security: result.security,
    price: {
      securityId: input.securityId,
      priceDate: row.priceDate,
      quoteTimestamp: `${row.priceDate}T16:00:00Z`,
      price: row.close,
      currency: input.quoteCurrency,
      sourceName: "ft_markets_nav",
      isRealtime: false,
      isDelayed: true,
      marketState: "reference_nav",
      rawJson: {
        importSource: "ft_markets",
        priceType: "nav",
        pageUrl: result.security.pageUrl,
        publicSymbol: result.security.publicSymbol,
        internalSymbol: result.security.internalSymbol,
        requestedStartDate: lookbackStart,
        requestedEndDate: input.transactionDate,
      },
      createdAt: new Date().toISOString(),
    } satisfies SecurityPrice,
  };
}
