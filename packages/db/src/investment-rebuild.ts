import { randomUUID } from "node:crypto";

import { Decimal } from "decimal.js";

import { parseInvestmentEvent } from "@myfinance/classification";
import {
  rebuildInvestmentState,
  resolveFxRate,
  type DomainDataset,
  type Security,
  type SecurityAlias,
  type SecurityPrice,
  type Transaction,
} from "@myfinance/domain";

type SearchCandidate = {
  providerSymbol: string;
  instrumentName: string;
  exchange: string;
  micCode: string | null;
  instrumentType: string;
  country: string | null;
  currency: string;
};

type ResolvedTransactionPatch = {
  id: string;
  transactionClass?: Transaction["transactionClass"];
  categoryCode?: string | null;
  classificationStatus?: Transaction["classificationStatus"];
  classificationSource?: Transaction["classificationSource"];
  classificationConfidence?: string;
  securityId?: string | null;
  quantity?: string | null;
  unitPriceOriginal?: string | null;
  needsReview?: boolean;
  reviewReason?: string | null;
};

const MAX_HISTORICAL_PRICE_DRIFT_DAYS = 7;

export type InvestmentRebuildArtifacts = {
  transactions: Transaction[];
  transactionPatches: ResolvedTransactionPatch[];
  insertedSecurities: Security[];
  insertedAliases: SecurityAlias[];
  upsertedPrices: SecurityPrice[];
  positions: DomainDataset["investmentPositions"];
  snapshots: DomainDataset["dailyPortfolioSnapshots"];
};

function normalizeSecurityText(value: string | null | undefined) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function dayDistance(start: string, end: string) {
  return Math.max(
    0,
    Math.floor(
      (Date.parse(`${end}T00:00:00Z`) - Date.parse(`${start}T00:00:00Z`)) /
        86400000,
    ),
  );
}

function shiftIsoDate(value: string, days: number) {
  const date = new Date(`${value}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function securityHintFromTransaction(transaction: Transaction) {
  const parsed = parseInvestmentEvent(transaction);
  if (parsed.securityHint) {
    return parsed.securityHint;
  }

  const llmHintCandidate =
    transaction.llmPayload &&
    typeof transaction.llmPayload === "object" &&
    typeof transaction.llmPayload.llm === "object" &&
    transaction.llmPayload.llm &&
    typeof (transaction.llmPayload.llm as Record<string, unknown>)
      .securityHint === "string"
      ? ((transaction.llmPayload.llm as Record<string, unknown>)
          .securityHint as string)
      : null;

  return llmHintCandidate;
}

function buildSearchQueries(hint: string) {
  const normalized = normalizeSecurityText(hint);
  const strippedQuantity = normalized
    .replace(/\s*@\s*\d+(?:\.\d+)?$/, "")
    .trim();
  const queries = new Set<string>([strippedQuantity]);

  if (normalized.includes("ADVANCED MICRO DEVICES")) {
    queries.add("ADVANCED MICRO DEVICES");
  }
  if (normalized.includes("INTEL")) {
    queries.add("INTEL CORP");
    queries.add("INTEL CORPORATION");
  }
  if (normalized.includes("ALPHABET")) {
    queries.add("ALPHABET");
    if (normalized.includes("CL C") || normalized.includes("CLASS C")) {
      queries.add("ALPHABET CLASS C");
    }
  }
  if (normalized.includes("LUMENTUM")) {
    queries.add("LUMENTUM HOLDINGS INC");
  }
  if (normalized.includes("SAMSUNG")) {
    queries.add("SAMSUNG ELECTRONICS");
    if (normalized.includes("GDR")) {
      queries.add("SAMSUNG ELECTRONICS GDR");
    }
  }
  if (normalized.includes("EMERGING MARKETS")) {
    queries.add("EMERGING MARKETS STOCK");
    queries.add("VANGUARD EMERGING MARKETS");
  }
  if (normalized.includes("EUROZONE")) {
    queries.add("VANGUARD EUROZONE");
    queries.add("VANGUARD EUROZONE STOCK INDEX");
  }
  if (normalized.includes("VANGUARD")) {
    if (
      normalized.includes("US 500") ||
      normalized.includes("S&P500") ||
      normalized.includes("S&P 500")
    ) {
      queries.add("VANGUARD S&P 500");
      queries.add("VANGUARD 500");
    }
    if (normalized.includes("JAPA") || normalized.includes("JAPAN")) {
      queries.add("VANGUARD JAPAN");
    }
    if (normalized.includes("SMALL CAP")) {
      queries.add("VANGUARD SMALL CAP");
      queries.add("VANGUARD GLOBAL SMALL-CAP");
      queries.add("VANGUARD GLOBAL SMALL-CAP INDEX FUND");
    }
  }

  return [...queries].filter(Boolean);
}

function hintPrefersEuroShareClass(hint: string) {
  const normalizedHint = normalizeSecurityText(hint);
  return /\b(EU|EUR)\b/.test(normalizedHint);
}

function scoreCandidate(hint: string, candidate: SearchCandidate) {
  const normalizedHint = normalizeSecurityText(hint);
  const candidateText = normalizeSecurityText(
    `${candidate.providerSymbol} ${candidate.instrumentName} ${candidate.exchange} ${candidate.currency}`,
  );
  const hintTokens = normalizedHint.split(/[^A-Z0-9]+/).filter(Boolean);
  const tokenScore = hintTokens.reduce(
    (score, token) => score + (candidateText.includes(token) ? 2 : 0),
    0,
  );

  let score = tokenScore;
  if (candidate.instrumentType.toUpperCase().includes("ETF")) {
    score += 6;
  } else if (candidate.instrumentType.toUpperCase().includes("MUTUAL")) {
    score += 4;
  } else if (candidate.instrumentType.toUpperCase().includes("STOCK")) {
    score += 5;
  }

  if (normalizedHint.includes("CL C") || normalizedHint.includes("CLASS C")) {
    if (candidate.providerSymbol === "GOOG") score += 12;
    if (candidate.providerSymbol === "GOOGL") score -= 6;
  }

  if (
    normalizedHint.includes("ADVANCED MICRO DEVICES") &&
    candidate.providerSymbol === "AMD"
  ) {
    score += 12;
  }
  if (normalizedHint.includes("INTEL") && candidate.providerSymbol === "INTC") {
    score += 12;
  }
  if (
    normalizedHint.includes("LUMENTUM") &&
    candidate.providerSymbol === "LITE"
  ) {
    score += 12;
  }
  if (normalizedHint.includes("SAMSUNG")) {
    if (candidate.instrumentName.toUpperCase().includes("SAMSUNG")) score += 8;
    if (
      normalizedHint.includes("GDR") &&
      candidate.instrumentName.toUpperCase().includes("GDR")
    ) {
      score += 12;
    }
  }
  if (normalizedHint.includes("EMERGING MARKETS")) {
    if (candidate.instrumentName.toUpperCase().includes("EMERGING MARKETS")) {
      score += 10;
    }
    if (
      normalizedHint.includes("STOCK") &&
      candidate.instrumentName.toUpperCase().includes("STOCK INDEX")
    ) {
      score += 12;
    }
    if (candidate.instrumentName.toUpperCase().includes("BOND")) {
      score -= 18;
    }
  }
  if (
    normalizedHint.includes("EUROZONE") &&
    candidate.instrumentName.toUpperCase().includes("EUROZONE")
  ) {
    score += 12;
  }
  if (normalizedHint.includes("SMALL CAP")) {
    if (
      candidate.instrumentName.toUpperCase().includes("SMALL-CAP") ||
      candidate.instrumentName.toUpperCase().includes("SMALL CAP")
    ) {
      score += 12;
    }
    if (
      (normalizedHint.includes("GLOB") || normalizedHint.includes("GLOBAL")) &&
      candidate.instrumentName.toUpperCase().includes("GLOBAL")
    ) {
      score += 8;
    }
  }

  if (hintPrefersEuroShareClass(normalizedHint)) {
    if (candidate.currency === "EUR") score += 8;
    if (candidate.instrumentName.toUpperCase().includes("EUR")) score += 6;
    if (candidate.exchange === "OTC") score -= 6;
    if (candidate.currency !== "EUR") score -= 6;
    if (candidate.country?.toUpperCase() === "UNITED STATES") score -= 4;
  }

  if (normalizedHint.includes("VANGUARD")) {
    if (candidate.instrumentName.toUpperCase().includes("VANGUARD")) score += 8;
    if (
      (normalizedHint.includes("US 500") || normalizedHint.includes("S&P")) &&
      candidate.instrumentName.toUpperCase().includes("500")
    ) {
      score += 10;
    }
    if (
      normalizedHint.includes("JAPAN") &&
      candidate.instrumentName.toUpperCase().includes("JAPAN")
    ) {
      score += 10;
    }
    if (
      normalizedHint.includes("SMALL CAP") &&
      candidate.instrumentName.toUpperCase().includes("SMALL")
    ) {
      score += 10;
    }
    if (hintPrefersEuroShareClass(normalizedHint)) {
      if (candidate.currency === "EUR") score += 12;
      if (candidate.instrumentName.toUpperCase().includes("EUR")) score += 8;
      if (
        candidate.country &&
        candidate.country.toUpperCase() !== "UNITED STATES"
      ) {
        score += 6;
      }
      if (candidate.exchange === "OTC") score -= 10;
      if (candidate.currency !== "EUR") score -= 8;
      if (candidate.country?.toUpperCase() === "UNITED STATES") score -= 8;
      if (candidate.exchange === "NYSE" && candidate.providerSymbol === "VOO")
        score += 4;
    }
  }

  if (
    normalizedHint.includes("STOCK INDEX") &&
    candidate.instrumentName.toUpperCase().includes("STOCK INDEX")
  ) {
    score += 8;
  }

  if (candidate.instrumentType.toUpperCase().includes("WARRANT")) {
    score -= 20;
  }

  return score;
}

function securityConflictsWithHint(hint: string, security: Security) {
  const normalizedHint = normalizeSecurityText(hint);

  if (
    hintPrefersEuroShareClass(normalizedHint) &&
    security.quoteCurrency !== "EUR"
  ) {
    return true;
  }

  if (
    normalizedHint.includes("VANGUARD") &&
    (normalizedHint.includes("US 500") || normalizedHint.includes("S&P")) &&
    hintPrefersEuroShareClass(normalizedHint) &&
    security.exchangeName.toUpperCase() === "OTC"
  ) {
    return true;
  }

  return false;
}

function readPayloadField<T>(
  payload: Record<string, unknown>,
  keys: string[],
): T | null {
  for (const key of keys) {
    if (key in payload) {
      return payload[key] as T;
    }
  }
  return null;
}

function readPayloadString(payload: Record<string, unknown>, keys: string[]) {
  const value = readPayloadField<unknown>(payload, keys);
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return null;
}

function readPayloadBoolean(payload: Record<string, unknown>, keys: string[]) {
  const value = readPayloadField<unknown>(payload, keys);
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (value.toLowerCase() === "true") return true;
    if (value.toLowerCase() === "false") return false;
  }
  return null;
}

function readPayloadTimestamp(
  payload: Record<string, unknown>,
  keys: string[],
) {
  const value = readPayloadField<unknown>(payload, keys);
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value * 1000).toISOString();
  }
  return null;
}

function isWeekendIso(value: string) {
  const day = new Date(`${value}T00:00:00Z`).getUTCDay();
  return day === 0 || day === 6;
}

function expectedLatestQuoteDate(referenceDate: string) {
  const day = new Date(`${referenceDate}T00:00:00Z`).getUTCDay();
  if (day === 6) return shiftIsoDate(referenceDate, -1);
  if (day === 0) return shiftIsoDate(referenceDate, -2);
  return referenceDate;
}

async function fetchSearchCandidates(query: string, apiKey: string) {
  const url = new URL("https://api.twelvedata.com/symbol_search");
  url.searchParams.set("symbol", query);
  url.searchParams.set("apikey", apiKey);

  const response = await fetch(url);
  if (!response.ok) {
    return [];
  }

  const payload = (await response.json()) as {
    data?: Array<{
      symbol: string;
      instrument_name: string;
      exchange: string;
      mic_code?: string | null;
      instrument_type: string;
      country?: string | null;
      currency: string;
    }>;
  };

  return (payload.data ?? []).map((candidate) => ({
    providerSymbol: candidate.symbol,
    instrumentName: candidate.instrument_name,
    exchange: candidate.exchange,
    micCode: candidate.mic_code ?? null,
    instrumentType: candidate.instrument_type,
    country: candidate.country ?? null,
    currency: candidate.currency,
  }));
}

async function fetchHistoricalPrice(
  security: Pick<Security, "id" | "providerSymbol" | "quoteCurrency">,
  transactionDate: string,
  apiKey: string,
) {
  const url = new URL("https://api.twelvedata.com/time_series");
  url.searchParams.set("symbol", security.providerSymbol);
  url.searchParams.set("interval", "1day");
  url.searchParams.set("end_date", transactionDate);
  url.searchParams.set("outputsize", "1");
  url.searchParams.set("apikey", apiKey);

  const response = await fetch(url);
  if (!response.ok) {
    return null;
  }

  const payload = (await response.json()) as {
    values?: Array<{ datetime: string; close: string }>;
    status?: string;
    message?: string;
    code?: number;
  };
  const value = payload.values?.[0];
  if (!value?.close || !value.datetime) {
    return null;
  }
  if (
    dayDistance(value.datetime.slice(0, 10), transactionDate) >
    MAX_HISTORICAL_PRICE_DRIFT_DAYS
  ) {
    return null;
  }

  return {
    securityId: security.id,
    priceDate: value.datetime.slice(0, 10),
    quoteTimestamp: `${value.datetime.slice(0, 10)}T16:00:00Z`,
    price: value.close,
    currency: security.quoteCurrency,
    sourceName: "twelve_data",
    isRealtime: false,
    isDelayed: true,
    marketState: "closed",
    rawJson: payload as Record<string, unknown>,
    createdAt: new Date().toISOString(),
  } satisfies SecurityPrice;
}

async function fetchLatestPrice(
  security: Security,
  apiKey: string,
  referenceDate: string,
) {
  const url = new URL("https://api.twelvedata.com/quote");
  url.searchParams.set("symbol", security.providerSymbol);
  url.searchParams.set("apikey", apiKey);
  if (isWeekendIso(referenceDate)) {
    url.searchParams.set("eod", "true");
  }

  const response = await fetch(url);
  if (!response.ok) {
    return null;
  }

  const payload = (await response.json()) as Record<string, unknown>;
  const price = readPayloadString(payload, ["close", "price"]);
  if (!price) {
    return null;
  }

  const priceDate =
    readPayloadString(payload, ["datetime"])?.slice(0, 10) ?? referenceDate;
  const isMarketOpen =
    readPayloadBoolean(payload, ["is_market_open", "isMarketOpen"]) ?? false;
  const currency =
    readPayloadString(payload, ["currency"]) ?? security.quoteCurrency;

  return {
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
  } satisfies SecurityPrice;
}

function mapAssetType(instrumentType: string): Security["assetType"] {
  const normalized = instrumentType.toUpperCase();
  if (normalized.includes("ETF") || normalized.includes("FUND")) return "etf";
  if (normalized.includes("STOCK")) return "stock";
  if (normalized.includes("CASH")) return "cash";
  return "other";
}

function findSecurityByHint(dataset: DomainDataset, hint: string) {
  const normalizedHint = normalizeSecurityText(hint);
  const directMatch = dataset.securities.find((security) => {
    const candidates = [
      security.providerSymbol,
      security.canonicalSymbol,
      security.displaySymbol,
      security.name,
    ].map((value) => normalizeSecurityText(value));
    if (normalizedHint.includes("CL C") || normalizedHint.includes("CLASS C")) {
      return security.providerSymbol === "GOOG";
    }
    return candidates.some((candidate) => candidate === normalizedHint);
  });
  if (directMatch && !securityConflictsWithHint(hint, directMatch)) {
    return directMatch;
  }

  const aliasMatch = dataset.securityAliases.find(
    (alias) =>
      normalizeSecurityText(alias.aliasTextNormalized) === normalizedHint,
  );
  const aliasedSecurity = aliasMatch
    ? (dataset.securities.find(
        (security) => security.id === aliasMatch.securityId,
      ) ?? null)
    : null;

  if (aliasedSecurity && !securityConflictsWithHint(hint, aliasedSecurity)) {
    return aliasedSecurity;
  }

  return null;
}

async function resolveSecurity(
  dataset: DomainDataset,
  hint: string,
  apiKey: string | null,
) {
  const existing = findSecurityByHint(dataset, hint);
  if (existing) {
    return {
      security: existing,
      insertedSecurity: null,
      insertedAlias: null,
    };
  }

  if (!apiKey) {
    return {
      security: null,
      insertedSecurity: null,
      insertedAlias: null,
    };
  }

  let bestCandidate: SearchCandidate | null = null;
  let bestScore = Number.NEGATIVE_INFINITY;
  const queries = buildSearchQueries(hint);
  for (const query of queries) {
    const candidates = await fetchSearchCandidates(query, apiKey);
    for (const candidate of candidates) {
      const score = scoreCandidate(hint, candidate);
      if (score > bestScore) {
        bestCandidate = candidate;
        bestScore = score;
      }
    }
  }

  if (!bestCandidate || bestScore < 12) {
    return {
      security: null,
      insertedSecurity: null,
      insertedAlias: null,
    };
  }

  const duplicate = dataset.securities.find(
    (security) =>
      security.providerName === "twelve_data" &&
      security.providerSymbol === bestCandidate.providerSymbol,
  );
  if (duplicate) {
    return {
      security: duplicate,
      insertedSecurity: null,
      insertedAlias: null,
    };
  }

  const securityId = randomUUID();
  const security = {
    id: securityId,
    providerName: "twelve_data",
    providerSymbol: bestCandidate.providerSymbol,
    canonicalSymbol: bestCandidate.providerSymbol,
    displaySymbol: bestCandidate.providerSymbol,
    name: bestCandidate.instrumentName,
    exchangeName: bestCandidate.exchange,
    micCode: bestCandidate.micCode,
    assetType: mapAssetType(bestCandidate.instrumentType),
    quoteCurrency: bestCandidate.currency,
    country: bestCandidate.country,
    isin: null,
    figi: null,
    active: true,
    metadataJson: {
      instrumentType: bestCandidate.instrumentType,
    },
    lastPriceRefreshAt: null,
    createdAt: new Date().toISOString(),
  } satisfies Security;
  const alias = {
    id: randomUUID(),
    securityId,
    aliasTextNormalized: normalizeSecurityText(hint),
    aliasSource: "provider",
    templateId: null,
    confidence: "0.9000",
    createdAt: new Date().toISOString(),
  } satisfies SecurityAlias;

  return {
    security,
    insertedSecurity: security,
    insertedAlias: alias,
  };
}

function inferQuantityFromPrice(
  dataset: DomainDataset,
  transaction: Transaction,
  tradePrice: SecurityPrice,
) {
  const amountInQuoteCurrency = amountInSecurityQuoteCurrency(
    dataset,
    transaction,
    tradePrice.currency,
  );

  if (new Decimal(tradePrice.price).eq(0)) {
    return null;
  }

  return amountInQuoteCurrency.div(tradePrice.price).toFixed(8);
}

function amountInSecurityQuoteCurrency(
  dataset: DomainDataset,
  transaction: Transaction,
  quoteCurrency: string,
) {
  return transaction.currencyOriginal === quoteCurrency
    ? new Decimal(transaction.amountOriginal).abs()
    : new Decimal(transaction.amountBaseEur)
        .abs()
        .mul(
          resolveFxRate(
            dataset,
            "EUR",
            quoteCurrency,
            transaction.transactionDate,
          ),
        );
}

function normalizeQuantityValue(quantity: string | null | undefined) {
  if (!quantity) return null;
  const value = new Decimal(quantity);
  return value.eq(0) ? null : value.toFixed(8);
}

function inferImpliedUnitPrice(
  dataset: DomainDataset,
  transaction: Transaction,
  quantity: string,
  quoteCurrency: string,
) {
  const normalizedQuantity = normalizeQuantityValue(quantity);
  if (!normalizedQuantity) {
    return null;
  }

  const amountInQuoteCurrency = amountInSecurityQuoteCurrency(
    dataset,
    transaction,
    quoteCurrency,
  );
  const quantityDecimal = new Decimal(normalizedQuantity);
  if (quantityDecimal.eq(0)) {
    return null;
  }

  return amountInQuoteCurrency.div(quantityDecimal).toFixed(8);
}

function buildQuantityReviewReason(
  hint: string,
  resolvedSecurity: Security | null,
  apiKeyAvailable: boolean,
) {
  if (!resolvedSecurity) {
    return `Quantity could not be derived for "${hint}".`;
  }

  if (!apiKeyAvailable) {
    return `Mapped to ${resolvedSecurity.displaySymbol}, but market-data enrichment is unavailable, so quantity could not be derived for "${hint}".`;
  }

  return `Mapped to ${resolvedSecurity.displaySymbol}, but Twelve Data did not return a usable historical price to derive quantity for "${hint}".`;
}

function buildMarketPriceReviewReason(
  dataset: DomainDataset,
  transaction: Transaction,
  resolvedSecurity: Security,
  quantity: string | null,
  tradePrice: SecurityPrice | null,
) {
  if (!tradePrice || !quantity) {
    return null;
  }

  if (!["stock", "etf"].includes(resolvedSecurity.assetType)) {
    return null;
  }

  const impliedUnitPrice = inferImpliedUnitPrice(
    dataset,
    transaction,
    quantity,
    tradePrice.currency,
  );
  if (!impliedUnitPrice) {
    return null;
  }

  const implied = new Decimal(impliedUnitPrice);
  const market = new Decimal(tradePrice.price);
  if (implied.lte(0) || market.lte(0)) {
    return null;
  }

  const ratio = implied.div(market);
  if (ratio.greaterThanOrEqualTo(0.1) && ratio.lessThanOrEqualTo(10)) {
    return null;
  }

  return `Mapped to ${resolvedSecurity.displaySymbol}, but the implied unit price (${implied.toFixed(2)} ${tradePrice.currency}) diverges from available market data (${market.toFixed(2)} ${tradePrice.currency} on ${tradePrice.priceDate}).`;
}

function findStoredHistoricalPrice(
  dataset: DomainDataset,
  securityId: string,
  transactionDate: string,
) {
  return (
    [...dataset.securityPrices]
      .filter(
        (price) =>
          price.securityId === securityId &&
          price.priceDate <= transactionDate &&
          dayDistance(price.priceDate, transactionDate) <=
            MAX_HISTORICAL_PRICE_DRIFT_DAYS,
      )
      .sort((left, right) =>
        `${right.priceDate}${right.createdAt}`.localeCompare(
          `${left.priceDate}${left.createdAt}`,
        ),
      )[0] ?? null
  );
}

function findRecentStoredQuote(
  dataset: DomainDataset,
  securityId: string,
  referenceDate: string,
) {
  const targetDate = expectedLatestQuoteDate(referenceDate);
  return (
    [...dataset.securityPrices]
      .filter(
        (price) =>
          price.securityId === securityId && price.priceDate === targetDate,
      )
      .sort((left, right) =>
        `${right.priceDate}${right.quoteTimestamp}`.localeCompare(
          `${left.priceDate}${left.quoteTimestamp}`,
        ),
      )[0] ?? null
  );
}

function shouldClearReview(
  transaction: Transaction,
  securityId: string | null,
  quantity: string | null,
) {
  if (!securityId || !quantity) {
    return false;
  }

  if (
    !["investment_trade_buy", "investment_trade_sell"].includes(
      transaction.transactionClass,
    )
  ) {
    return false;
  }

  return (
    transaction.needsReview &&
    /security mapping|quantity could not be derived|twelve data|market-data enrichment|diverges from available market data|lacks details|llm enrichment/i.test(
      transaction.reviewReason ?? "",
    )
  );
}

function shouldClearDeterministicNonTradeReview(
  transaction: Transaction,
  parsedTransactionClass: Transaction["transactionClass"],
) {
  return (
    transaction.needsReview &&
    parsedTransactionClass === transaction.transactionClass &&
    ["interest", "dividend", "fee"].includes(transaction.transactionClass)
  );
}

function upsertTransactionPatch(
  transactionPatches: ResolvedTransactionPatch[],
  patch: ResolvedTransactionPatch,
) {
  const existingIndex = transactionPatches.findIndex(
    (candidate) => candidate.id === patch.id,
  );
  if (existingIndex === -1) {
    transactionPatches.push(patch);
    return;
  }

  transactionPatches[existingIndex] = {
    ...transactionPatches[existingIndex],
    ...patch,
  };
}

function categoryCodeForInvestmentClass(
  transactionClass: Transaction["transactionClass"],
) {
  switch (transactionClass) {
    case "dividend":
      return "dividend";
    case "interest":
      return "interest";
    case "fee":
      return "broker_fee";
    case "investment_trade_buy":
      return "stock_buy";
    default:
      return "uncategorized_investment";
  }
}

export async function prepareInvestmentRebuild(
  dataset: DomainDataset,
  referenceDate: string,
): Promise<InvestmentRebuildArtifacts> {
  const apiKey = process.env.TWELVE_DATA_API_KEY?.trim() || null;
  const workingDataset: DomainDataset = {
    ...dataset,
    transactions: dataset.transactions.map((transaction) => ({
      ...transaction,
    })),
    securities: [...dataset.securities],
    securityAliases: [...dataset.securityAliases],
    securityPrices: [...dataset.securityPrices],
  };

  const transactionPatches: ResolvedTransactionPatch[] = [];
  const insertedSecurities: Security[] = [];
  const insertedAliases: SecurityAlias[] = [];
  const upsertedPrices: SecurityPrice[] = [];
  const historicalPriceCache = new Map<string, SecurityPrice | null>();
  const latestPriceCache = new Map<string, SecurityPrice | null>();
  const trackedPriceKeys = new Set(
    workingDataset.securityPrices.map(
      (price) => `${price.securityId}:${price.priceDate}:${price.sourceName}`,
    ),
  );

  const recordPrice = (price: SecurityPrice | null) => {
    if (!price) return;

    const priceKey = `${price.securityId}:${price.priceDate}:${price.sourceName}`;
    if (!trackedPriceKeys.has(priceKey)) {
      upsertedPrices.push(price);
      trackedPriceKeys.add(priceKey);
    }
    workingDataset.securityPrices = [
      ...workingDataset.securityPrices.filter(
        (row) =>
          !(
            row.securityId === price.securityId &&
            row.priceDate === price.priceDate &&
            row.sourceName === price.sourceName
          ),
      ),
      price,
    ];
  };

  const loadHistoricalPrice = async (
    security: Pick<Security, "id" | "providerSymbol" | "quoteCurrency">,
    transactionDate: string,
  ) => {
    const cacheKey = `${security.id}:${transactionDate}`;
    if (historicalPriceCache.has(cacheKey)) {
      return historicalPriceCache.get(cacheKey) ?? null;
    }

    const storedPrice = findStoredHistoricalPrice(
      workingDataset,
      security.id,
      transactionDate,
    );
    if (storedPrice) {
      historicalPriceCache.set(cacheKey, storedPrice);
      return storedPrice;
    }

    if (apiKey) {
      const fetchedPrice = await fetchHistoricalPrice(
        security,
        transactionDate,
        apiKey,
      );
      if (fetchedPrice) {
        historicalPriceCache.set(cacheKey, fetchedPrice);
        return fetchedPrice;
      }
    }

    historicalPriceCache.set(cacheKey, null);
    return null;
  };

  const loadLatestPrice = async (security: Security) => {
    const cacheKey = security.id;
    if (latestPriceCache.has(cacheKey)) {
      return latestPriceCache.get(cacheKey) ?? null;
    }

    const recentStoredPrice = findRecentStoredQuote(
      workingDataset,
      security.id,
      referenceDate,
    );
    if (recentStoredPrice) {
      latestPriceCache.set(cacheKey, recentStoredPrice);
      return recentStoredPrice;
    }

    const price = apiKey
      ? await fetchLatestPrice(security, apiKey, referenceDate)
      : null;
    latestPriceCache.set(cacheKey, price);
    return price;
  };

  const investmentTransactions = workingDataset.transactions
    .filter((transaction) => {
      const account = workingDataset.accounts.find(
        (candidate) => candidate.id === transaction.accountId,
      );
      return (
        account?.assetDomain === "investment" &&
        transaction.transactionDate <= referenceDate
      );
    })
    .sort((left, right) =>
      `${left.transactionDate}${left.createdAt}`.localeCompare(
        `${right.transactionDate}${right.createdAt}`,
      ),
    );

  for (const transaction of investmentTransactions) {
    const parsed = parseInvestmentEvent(transaction);
    const originalTransactionClass = transaction.transactionClass;
    const originalCategoryCode = transaction.categoryCode ?? null;
    const originalClassificationStatus = transaction.classificationStatus;
    const originalClassificationSource = transaction.classificationSource;
    const originalClassificationConfidence =
      transaction.classificationConfidence;
    const originalQuantity = transaction.quantity ?? null;
    const originalUnitPriceOriginal = transaction.unitPriceOriginal ?? null;
    if (
      transaction.transactionClass === "unknown" &&
      parsed.transactionClass !== "unknown"
    ) {
      transaction.transactionClass = parsed.transactionClass;
      transaction.categoryCode = categoryCodeForInvestmentClass(
        parsed.transactionClass,
      );
      transaction.classificationStatus = "investment_parser";
      transaction.classificationSource = "investment_parser";
      transaction.classificationConfidence = "0.96";
    }

    let resolvedSecurityId = transaction.securityId ?? null;
    let quantity = normalizeQuantityValue(
      transaction.quantity ?? parsed.quantity ?? null,
    );
    let unitPriceOriginal =
      transaction.unitPriceOriginal ?? parsed.unitPriceOriginal ?? null;

    const hint = securityHintFromTransaction(transaction);
    const currentResolvedSecurity = resolvedSecurityId
      ? (workingDataset.securities.find(
          (security) => security.id === resolvedSecurityId,
        ) ?? null)
      : null;

    if (
      hint &&
      apiKey &&
      currentResolvedSecurity &&
      securityConflictsWithHint(hint, currentResolvedSecurity)
    ) {
      resolvedSecurityId = null;
    }

    if (!resolvedSecurityId && hint) {
      const resolved = await resolveSecurity(workingDataset, hint, apiKey);
      if (resolved.insertedSecurity) {
        insertedSecurities.push(resolved.insertedSecurity);
        workingDataset.securities.push(resolved.insertedSecurity);
      }
      if (resolved.insertedAlias) {
        insertedAliases.push(resolved.insertedAlias);
        workingDataset.securityAliases.push(resolved.insertedAlias);
      }
      resolvedSecurityId = resolved.security?.id ?? null;
    }

    const resolvedSecurity = resolvedSecurityId
      ? (workingDataset.securities.find(
          (security) => security.id === resolvedSecurityId,
        ) ?? null)
      : null;
    const isTrade =
      resolvedSecurity &&
      ["investment_trade_buy", "investment_trade_sell"].includes(
        transaction.transactionClass,
      );
    const historicalPrice =
      isTrade && apiKey
        ? await loadHistoricalPrice(
            resolvedSecurity,
            transaction.transactionDate,
          )
        : null;
    recordPrice(historicalPrice);

    if (resolvedSecurity && historicalPrice) {
      quantity =
        quantity ??
        inferQuantityFromPrice(workingDataset, transaction, historicalPrice);
      unitPriceOriginal = unitPriceOriginal ?? historicalPrice.price;
    }

    const marketPriceReviewReason =
      resolvedSecurity && quantity
        ? buildMarketPriceReviewReason(
            workingDataset,
            transaction,
            resolvedSecurity,
            quantity,
            historicalPrice,
          )
        : null;

    const patch: ResolvedTransactionPatch = { id: transaction.id };
    let changed = false;

    if (
      originalTransactionClass !== transaction.transactionClass ||
      originalCategoryCode !== (transaction.categoryCode ?? null)
    ) {
      patch.transactionClass = transaction.transactionClass;
      patch.categoryCode = transaction.categoryCode ?? null;
      patch.classificationStatus =
        transaction.classificationStatus !== originalClassificationStatus
          ? transaction.classificationStatus
          : undefined;
      patch.classificationSource =
        transaction.classificationSource !== originalClassificationSource
          ? transaction.classificationSource
          : undefined;
      patch.classificationConfidence =
        transaction.classificationConfidence !==
        originalClassificationConfidence
          ? transaction.classificationConfidence
          : undefined;
      changed = true;
    }

    if (resolvedSecurityId && resolvedSecurityId !== transaction.securityId) {
      transaction.securityId = resolvedSecurityId;
      patch.securityId = resolvedSecurityId;
      changed = true;
    }
    const normalizedOriginalQuantity = normalizeQuantityValue(originalQuantity);
    if (
      quantity !== normalizedOriginalQuantity ||
      (quantity === null &&
        originalQuantity !== null &&
        normalizedOriginalQuantity === null)
    ) {
      transaction.quantity = quantity;
      patch.quantity = quantity;
      changed = true;
    }
    if ((unitPriceOriginal ?? null) !== originalUnitPriceOriginal) {
      transaction.unitPriceOriginal = unitPriceOriginal;
      patch.unitPriceOriginal = unitPriceOriginal;
      changed = true;
    }

    const reviewHint = hint ?? parsed.securityHint ?? "trade";

    if (marketPriceReviewReason) {
      if (
        transaction.needsReview !== true ||
        transaction.reviewReason !== marketPriceReviewReason
      ) {
        transaction.needsReview = true;
        transaction.reviewReason = marketPriceReviewReason;
        patch.needsReview = true;
        patch.reviewReason = marketPriceReviewReason;
        changed = true;
      }
    } else if (
      shouldClearDeterministicNonTradeReview(
        transaction,
        parsed.transactionClass,
      )
    ) {
      transaction.needsReview = false;
      transaction.reviewReason = null;
      patch.needsReview = false;
      patch.reviewReason = null;
      changed = true;
    } else if (shouldClearReview(transaction, resolvedSecurityId, quantity)) {
      transaction.needsReview = false;
      transaction.reviewReason = null;
      patch.needsReview = false;
      patch.reviewReason = null;
      changed = true;
    } else if (
      ["investment_trade_buy", "investment_trade_sell"].includes(
        transaction.transactionClass,
      ) &&
      resolvedSecurityId &&
      !quantity
    ) {
      const clearerReason = buildQuantityReviewReason(
        reviewHint,
        resolvedSecurity,
        Boolean(apiKey),
      );
      if (
        transaction.needsReview !== true ||
        transaction.reviewReason !== clearerReason
      ) {
        transaction.needsReview = true;
        transaction.reviewReason = clearerReason;
        patch.needsReview = true;
        patch.reviewReason = clearerReason;
        changed = true;
      }
    } else if (
      ["investment_trade_buy", "investment_trade_sell"].includes(
        transaction.transactionClass,
      ) &&
      !resolvedSecurityId &&
      reviewHint
    ) {
      const clearerReason = apiKey
        ? `Security mapping unresolved after Twelve Data symbol search for "${reviewHint}".`
        : `Security mapping unresolved for "${reviewHint}".`;
      if (transaction.reviewReason !== clearerReason) {
        transaction.reviewReason = clearerReason;
        patch.reviewReason = clearerReason;
        patch.needsReview = true;
        changed = true;
      }
    }

    if (changed) {
      upsertTransactionPatch(transactionPatches, patch);
    }
  }

  for (const transaction of workingDataset.transactions) {
    const account = workingDataset.accounts.find(
      (candidate) => candidate.id === transaction.accountId,
    );
    if (
      account?.assetDomain !== "investment" ||
      transaction.transactionDate > referenceDate ||
      !["investment_trade_buy", "investment_trade_sell"].includes(
        transaction.transactionClass,
      ) ||
      !transaction.securityId
    ) {
      continue;
    }

    const security = workingDataset.securities.find(
      (candidate) => candidate.id === transaction.securityId,
    );
    const normalizedQuantity = normalizeQuantityValue(transaction.quantity);
    if (!security || !normalizedQuantity) {
      continue;
    }

    const historicalPrice = await loadHistoricalPrice(
      security,
      transaction.transactionDate,
    );
    recordPrice(historicalPrice);

    const marketPriceReviewReason = buildMarketPriceReviewReason(
      workingDataset,
      transaction,
      security,
      normalizedQuantity,
      historicalPrice,
    );

    if (marketPriceReviewReason) {
      if (
        transaction.needsReview !== true ||
        transaction.reviewReason !== marketPriceReviewReason
      ) {
        transaction.needsReview = true;
        transaction.reviewReason = marketPriceReviewReason;
        upsertTransactionPatch(transactionPatches, {
          id: transaction.id,
          needsReview: true,
          reviewReason: marketPriceReviewReason,
        });
      }
      continue;
    }

    if (
      transaction.needsReview &&
      /diverges from available market data/i.test(
        transaction.reviewReason ?? "",
      )
    ) {
      transaction.needsReview = false;
      transaction.reviewReason = null;
      upsertTransactionPatch(transactionPatches, {
        id: transaction.id,
        needsReview: false,
        reviewReason: null,
      });
    }
  }

  if (apiKey) {
    const currentPositionsSecurityIds = new Set(
      workingDataset.transactions
        .filter(
          (transaction) =>
            ["investment_trade_buy", "investment_trade_sell"].includes(
              transaction.transactionClass,
            ) && transaction.securityId,
        )
        .map((transaction) => transaction.securityId as string),
    );

    for (const securityId of currentPositionsSecurityIds) {
      const security = workingDataset.securities.find(
        (candidate) => candidate.id === securityId,
      );
      if (!security) continue;
      const latestQuote = await loadLatestPrice(security);
      if (!latestQuote) continue;
      recordPrice(latestQuote);
    }
  }

  const rebuilt = rebuildInvestmentState(workingDataset, referenceDate);

  return {
    transactions: workingDataset.transactions,
    transactionPatches,
    insertedSecurities,
    insertedAliases,
    upsertedPrices,
    positions: rebuilt.positions,
    snapshots: rebuilt.snapshots,
  };
}
