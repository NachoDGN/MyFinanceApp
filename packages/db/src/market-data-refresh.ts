import {
  getDatasetLatestDate,
  getLatestAccountBalances,
  rebuildInvestmentState,
  type DomainDataset,
  type FxRate,
  type Security,
  type SecurityPrice,
} from "@myfinance/domain";
import {
  isWeekendIso,
  readPayloadBoolean,
  readPayloadString,
  readPayloadTimestamp,
} from "@myfinance/market-data";

import { fetchFtFundPriceAtOrBeforeDate } from "./ft-fund-history";
import { loadDatasetForUser } from "./dataset-loader";
import { withInvestmentMutationLock } from "./investment-mutation-lock";
import { serializeJson } from "./sql-json";
import {
  getDbRuntimeConfig,
  withSeededUserContext,
  type SqlClient,
} from "./sql-runtime";

const MAX_CURRENT_FUND_NAV_DRIFT_DAYS = 15;

function isRefreshableOwnedStockSecurity(security: Security) {
  return (
    security.providerName === "twelve_data" &&
    (security.assetType === "stock" || security.assetType === "etf")
  );
}

function isRefreshableOwnedFundSecurity(security: Security) {
  const instrumentType =
    typeof security.metadataJson?.instrumentType === "string"
      ? security.metadataJson.instrumentType.trim().toLowerCase()
      : "";
  return Boolean(
    security.isin?.trim() &&
    security.quoteCurrency?.trim() &&
    (security.providerName === "manual_fund_nav" ||
      instrumentType.includes("fund")),
  );
}

function buildOwnedSecurityIds(
  dataset: DomainDataset,
  referenceDate = getDatasetLatestDate(dataset),
) {
  const { positions } = rebuildInvestmentState(dataset, referenceDate);
  return new Set(positions.map((position) => position.securityId));
}

export function selectOwnedStockPriceRefreshSecurities(
  dataset: DomainDataset,
  referenceDate = getDatasetLatestDate(dataset),
) {
  const ownedSecurityIds = buildOwnedSecurityIds(dataset, referenceDate);

  return dataset.securities.filter(
    (security) =>
      ownedSecurityIds.has(security.id) &&
      isRefreshableOwnedStockSecurity(security),
  );
}

export function selectOwnedFundNavRefreshSecurities(
  dataset: DomainDataset,
  referenceDate = getDatasetLatestDate(dataset),
) {
  const ownedSecurityIds = buildOwnedSecurityIds(dataset, referenceDate);

  return dataset.securities.filter(
    (security) =>
      ownedSecurityIds.has(security.id) &&
      isRefreshableOwnedFundSecurity(security),
  );
}

export function selectTrackedEurFxPairs(
  dataset: DomainDataset,
  referenceDate = getDatasetLatestDate(dataset),
) {
  const accountsById = new Map(
    dataset.accounts.map((account) => [account.id, account]),
  );
  const balanceCurrencies = getLatestAccountBalances(
    dataset,
    referenceDate,
  ).flatMap((snapshot) => {
    const account = accountsById.get(snapshot.accountId);
    if (!account || account.accountType === "credit_card") {
      return [];
    }

    const currency = (snapshot.balanceCurrency ?? account.defaultCurrency)
      .trim()
      .toUpperCase();
    return currency && currency !== "EUR" ? [currency] : [];
  });
  const holdingCurrencies = selectOwnedStockPriceRefreshSecurities(
    dataset,
    referenceDate,
  )
    .concat(selectOwnedFundNavRefreshSecurities(dataset, referenceDate))
    .map((security) => security.quoteCurrency.trim().toUpperCase())
    .filter((currency) => currency !== "EUR");

  return [...new Set([...balanceCurrencies, ...holdingCurrencies])].sort();
}

async function fetchLatestOwnedStockPrice(
  security: Security,
  apiKey: string,
  requestDate: string,
): Promise<{ quote: SecurityPrice | null; reason: string | null }> {
  const url = new URL("https://api.twelvedata.com/quote");
  url.searchParams.set("symbol", security.providerSymbol);
  url.searchParams.set("apikey", apiKey);
  if (isWeekendIso(requestDate)) {
    url.searchParams.set("eod", "true");
  }

  const response = await fetch(url);
  const payload = (await response.json()) as Record<string, unknown> | string;
  if (!response.ok || typeof payload === "string") {
    return {
      quote: null,
      reason: `HTTP ${response.status} from Twelve Data.`,
    };
  }

  if (payload.status === "error") {
    const message = readPayloadString(payload, ["message"]);
    return {
      quote: null,
      reason: message ?? "Twelve Data returned an error payload.",
    };
  }

  const price = readPayloadString(payload, ["close", "price"]);
  if (!price) {
    return {
      quote: null,
      reason: "Twelve Data did not return a usable quote price.",
    };
  }

  const priceDate =
    readPayloadString(payload, ["datetime"])?.slice(0, 10) ?? requestDate;
  const isMarketOpen =
    readPayloadBoolean(payload, ["is_market_open", "isMarketOpen"]) ?? false;
  const currency =
    readPayloadString(payload, ["currency"]) ?? security.quoteCurrency;

  return {
    quote: {
      securityId: security.id,
      priceDate,
      quoteTimestamp:
        readPayloadTimestamp(payload, [
          "last_quote_at",
          "lastQuoteAt",
          "timestamp",
        ]) ?? `${priceDate}T16:00:00Z`,
      price,
      currency,
      sourceName: "twelve_data",
      isRealtime: isMarketOpen,
      isDelayed: !isMarketOpen,
      marketState: isMarketOpen ? "open" : "closed",
      rawJson: payload,
      createdAt: new Date().toISOString(),
    },
    reason: null,
  };
}

async function fetchLatestOwnedFundNav(
  security: Security,
  requestDate: string,
): Promise<{ quote: SecurityPrice | null; reason: string | null }> {
  if (!security.isin?.trim()) {
    return {
      quote: null,
      reason: "Fund NAV refresh requires an ISIN.",
    };
  }

  try {
    const result = await fetchFtFundPriceAtOrBeforeDate({
      securityId: security.id,
      isin: security.isin,
      quoteCurrency: security.quoteCurrency,
      transactionDate: requestDate,
      maxDriftDays: MAX_CURRENT_FUND_NAV_DRIFT_DAYS,
    });

    if (!result.price) {
      return {
        quote: null,
        reason: `FT Markets returned no NAV within ${MAX_CURRENT_FUND_NAV_DRIFT_DAYS} days.`,
      };
    }

    return {
      quote: result.price,
      reason: null,
    };
  } catch (error) {
    return {
      quote: null,
      reason:
        error instanceof Error ? error.message : "Fund NAV refresh failed.",
    };
  }
}

async function fetchLatestFxRate(
  baseCurrency: string,
  quoteCurrency: string,
  apiKey: string,
  requestDate: string,
): Promise<{ fxRate: FxRate | null; reason: string | null }> {
  const url = new URL("https://api.twelvedata.com/exchange_rate");
  url.searchParams.set("symbol", `${baseCurrency}/${quoteCurrency}`);
  url.searchParams.set("apikey", apiKey);

  const response = await fetch(url);
  const payload = (await response.json()) as Record<string, unknown> | string;
  if (!response.ok || typeof payload === "string") {
    return {
      fxRate: null,
      reason: `HTTP ${response.status} from Twelve Data.`,
    };
  }

  if (payload.status === "error") {
    return {
      fxRate: null,
      reason:
        readPayloadString(payload, ["message"]) ??
        "Twelve Data returned an error payload.",
    };
  }

  const rate = readPayloadString(payload, ["rate", "price", "close"]);
  if (!rate) {
    return {
      fxRate: null,
      reason: "Twelve Data did not return a usable FX rate.",
    };
  }

  const asOfTimestamp =
    readPayloadTimestamp(payload, ["timestamp", "last_quote_at"]) ??
    `${requestDate}T16:00:00Z`;
  const asOfDate =
    readPayloadString(payload, ["datetime", "date"])?.slice(0, 10) ??
    asOfTimestamp.slice(0, 10);

  return {
    fxRate: {
      baseCurrency,
      quoteCurrency,
      asOfDate,
      asOfTimestamp,
      rate,
      sourceName: "twelve_data",
      rawJson: payload,
    },
    reason: null,
  };
}

async function upsertSecurityPriceRow(sql: SqlClient, price: SecurityPrice) {
  await sql`
    insert into public.security_prices ${sql({
      security_id: price.securityId,
      price_date: price.priceDate,
      quote_timestamp: price.quoteTimestamp,
      price: price.price,
      currency: price.currency,
      source_name: price.sourceName,
      is_realtime: price.isRealtime,
      is_delayed: price.isDelayed,
      market_state: price.marketState,
      raw_json: serializeJson(sql, price.rawJson),
      created_at: price.createdAt,
    } as Record<string, unknown>)}
    on conflict (security_id, price_date, source_name)
    do update set
      quote_timestamp = excluded.quote_timestamp,
      price = excluded.price,
      currency = excluded.currency,
      is_realtime = excluded.is_realtime,
      is_delayed = excluded.is_delayed,
      market_state = excluded.market_state,
      raw_json = excluded.raw_json,
      created_at = excluded.created_at
  `;
}

async function upsertFxRateRow(sql: SqlClient, fxRate: FxRate) {
  await sql`
    insert into public.fx_rates ${sql({
      base_currency: fxRate.baseCurrency,
      quote_currency: fxRate.quoteCurrency,
      as_of_date: fxRate.asOfDate,
      as_of_timestamp: fxRate.asOfTimestamp,
      rate: fxRate.rate,
      source_name: fxRate.sourceName,
      raw_json: serializeJson(sql, fxRate.rawJson),
    } as Record<string, unknown>)}
    on conflict (base_currency, quote_currency, as_of_date, source_name)
    do update set
      as_of_timestamp = excluded.as_of_timestamp,
      rate = excluded.rate,
      raw_json = excluded.raw_json
  `;
}

async function updateSecurityPriceRefreshMetadata(
  sql: SqlClient,
  input: {
    securityId: string;
    quoteCurrency: string;
    quoteTimestamp: string;
  },
) {
  await sql`
    update public.securities
    set
      quote_currency = ${input.quoteCurrency},
      last_price_refresh_at = ${input.quoteTimestamp}
    where id = ${input.securityId}
  `;
}

export interface RefreshOwnedStockPricesResult {
  totalTrackedStocks: number;
  totalTrackedFunds: number;
  totalTrackedFxPairs: number;
  refreshedCount: number;
  skippedCount: number;
  refreshedSymbols: string[];
  skippedSymbols: string[];
  skippedDetails: Array<{ symbol: string; reason: string }>;
  refreshedFxPairs: string[];
  skippedFxPairs: Array<{ symbol: string; reason: string }>;
  latestPriceDate: string | null;
  generatedAt: string;
}

export async function refreshOwnedStockPrices(): Promise<RefreshOwnedStockPricesResult> {
  const runtime = getDbRuntimeConfig();
  const apiKey = process.env.TWELVE_DATA_API_KEY?.trim();
  if (!apiKey) {
    throw new Error(
      "TWELVE_DATA_API_KEY is not configured. Add it to .env.local before refreshing prices.",
    );
  }

  return withSeededUserContext(async (sql) => {
    return withInvestmentMutationLock(sql, runtime.seededUserId, async () => {
      const dataset = await loadDatasetForUser(sql, runtime.seededUserId);
      const stockSecurities = selectOwnedStockPriceRefreshSecurities(dataset);
      const fundSecurities = selectOwnedFundNavRefreshSecurities(dataset);
      const trackedFxPairs = selectTrackedEurFxPairs(dataset);
      const requestDate = new Date().toISOString().slice(0, 10);
      const refreshedSymbols: string[] = [];
      const skippedSymbols: string[] = [];
      const skippedDetails: Array<{ symbol: string; reason: string }> = [];
      const refreshedFxPairs: string[] = [];
      const skippedFxPairs: Array<{ symbol: string; reason: string }> = [];
      let latestPriceDate: string | null = null;

      for (const baseCurrency of trackedFxPairs) {
        const pair = `${baseCurrency}/EUR`;
        const { fxRate, reason } = await fetchLatestFxRate(
          baseCurrency,
          "EUR",
          apiKey,
          requestDate,
        );
        if (!fxRate) {
          skippedFxPairs.push({
            symbol: pair,
            reason: reason ?? "FX refresh failed.",
          });
          continue;
        }

        await upsertFxRateRow(sql, fxRate);
        refreshedFxPairs.push(pair);
        if (!latestPriceDate || fxRate.asOfDate > latestPriceDate) {
          latestPriceDate = fxRate.asOfDate;
        }
      }

      for (const security of stockSecurities) {
        const { quote, reason } = await fetchLatestOwnedStockPrice(
          security,
          apiKey,
          requestDate,
        );
        if (!quote) {
          skippedSymbols.push(security.displaySymbol);
          skippedDetails.push({
            symbol: security.displaySymbol,
            reason: reason ?? "Quote refresh failed.",
          });
          continue;
        }

        await upsertSecurityPriceRow(sql, quote);
        await updateSecurityPriceRefreshMetadata(sql, {
          securityId: security.id,
          quoteCurrency: quote.currency,
          quoteTimestamp: quote.quoteTimestamp,
        });
        refreshedSymbols.push(security.displaySymbol);
        if (!latestPriceDate || quote.priceDate > latestPriceDate) {
          latestPriceDate = quote.priceDate;
        }
      }

      for (const security of fundSecurities) {
        const { quote, reason } = await fetchLatestOwnedFundNav(
          security,
          requestDate,
        );
        if (!quote) {
          skippedSymbols.push(security.displaySymbol);
          skippedDetails.push({
            symbol: security.displaySymbol,
            reason: reason ?? "Fund NAV refresh failed.",
          });
          continue;
        }

        await upsertSecurityPriceRow(sql, quote);
        await updateSecurityPriceRefreshMetadata(sql, {
          securityId: security.id,
          quoteCurrency: quote.currency,
          quoteTimestamp: quote.quoteTimestamp,
        });
        refreshedSymbols.push(security.displaySymbol);
        if (!latestPriceDate || quote.priceDate > latestPriceDate) {
          latestPriceDate = quote.priceDate;
        }
      }

      return {
        totalTrackedStocks: stockSecurities.length,
        totalTrackedFunds: fundSecurities.length,
        totalTrackedFxPairs: trackedFxPairs.length,
        refreshedCount: refreshedSymbols.length,
        skippedCount: skippedSymbols.length,
        refreshedSymbols,
        skippedSymbols,
        skippedDetails,
        refreshedFxPairs,
        skippedFxPairs,
        latestPriceDate,
        generatedAt: new Date().toISOString(),
      };
    });
  });
}
