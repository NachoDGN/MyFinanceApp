import { randomUUID } from "node:crypto";

import { Decimal } from "decimal.js";

import { parseInvestmentEvent } from "@myfinance/classification";
import {
  extractIsinFromText,
  normalizeSecurityIdentifier,
  normalizeSecurityText,
  rebuildInvestmentState,
  resolveFxRate,
  type DomainDataset,
  type Security,
  type SecurityAlias,
  type SecurityPrice,
  type Transaction,
} from "@myfinance/domain";

import {
  readOptionalNumberAsString,
  readOptionalRecord,
  readOptionalString,
  readRawOutputNumberAsString,
  readRawOutputString,
} from "./sql-json";
import {
  buildFundNavBackfillRequest,
  mergeFundNavBackfillRequests,
  type FundNavBackfillRequest,
} from "./fund-nav-backfill";
import { fetchFtFundPriceAtOrBeforeDate } from "./ft-fund-history";

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
  llmPayload?: Record<string, unknown> | null;
  rebuildEvidence?: Record<string, unknown> | null;
};

const MAX_HISTORICAL_PRICE_DRIFT_DAYS = 7;
const FUND_HISTORICAL_PRICE_OVERRIDE_TOLERANCE = new Decimal("0.10");

function isEnvFlagEnabled(name: string) {
  return /^(1|true|yes)$/i.test(process.env[name] ?? "");
}

function redactApiKey(url: URL) {
  const copy = new URL(url);
  if (copy.searchParams.has("apikey")) {
    copy.searchParams.set("apikey", "***REDACTED***");
  }
  return copy.toString();
}

function logTwelveDataDebug(event: string, details: Record<string, unknown>) {
  if (!isEnvFlagEnabled("TWELVE_DATA_DEBUG")) {
    return;
  }

  console.log(`[twelve-data] ${JSON.stringify({ event, ...details })}`);
}

export type InvestmentRebuildArtifacts = {
  transactions: Transaction[];
  transactionPatches: ResolvedTransactionPatch[];
  insertedSecurities: Security[];
  insertedAliases: SecurityAlias[];
  upsertedPrices: SecurityPrice[];
  fundNavBackfillRequests: FundNavBackfillRequest[];
  positions: DomainDataset["investmentPositions"];
  snapshots: DomainDataset["dailyPortfolioSnapshots"];
};

export interface InvestmentRebuildProgress {
  stage: "historical_price_lookup";
  message: string;
}

function hasNonEmptyPayload(value: unknown): value is Record<string, unknown> {
  return (
    Boolean(value) &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.keys(value as Record<string, unknown>).length > 0
  );
}

function buildHistoricalPriceEvidence(price: SecurityPrice | null) {
  if (!price) {
    return null;
  }

  return {
    sourceName: price.sourceName ?? null,
    priceDate: price.priceDate ?? null,
    quoteTimestamp: price.quoteTimestamp ?? null,
    price: price.price ?? null,
    currency: price.currency ?? null,
    marketState: price.marketState ?? null,
  };
}

function buildTransactionRebuildEvidence(input: {
  resolvedSecurityId: string | null;
  historicalPrice: SecurityPrice | null;
  quantityDerivedFromHistoricalPrice: boolean;
}) {
  if (
    !input.resolvedSecurityId &&
    !input.historicalPrice &&
    !input.quantityDerivedFromHistoricalPrice
  ) {
    return null;
  }

  return {
    resolvedSecurityId: input.resolvedSecurityId,
    historicalPriceUsed: buildHistoricalPriceEvidence(input.historicalPrice),
    quantityDerivedFromHistoricalPrice:
      input.quantityDerivedFromHistoricalPrice,
    rebuiltAt: new Date().toISOString(),
  } satisfies Record<string, unknown>;
}

function isPlaceholderSecurityPrice(price: SecurityPrice) {
  return (
    price.sourceName === "twelve_data" && !hasNonEmptyPayload(price.rawJson)
  );
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

type SecurityResolutionContext = {
  reviewTrigger:
    | "import_classification"
    | "manual_review_update"
    | "review_propagation"
    | null;
  hint: string | null;
  transactionDate: string;
  exactInstrumentName: string | null;
  exactIsin: string | null;
  exactTicker: string | null;
  exactExchange: string | null;
  currentPrice: string | null;
  currentPriceCurrency: string | null;
  currentPriceTimestamp: string | null;
  currentPriceSource: string | null;
  currentPriceType: string | null;
  transactionCurrency: string;
  prefersEuroShareClass: boolean;
  rejectsEtf: boolean;
  prefersMutualFund: boolean;
  prefersEtf: boolean;
};

function normalizeReviewTrigger(value: unknown) {
  return value === "import_classification" ||
    value === "manual_review_update" ||
    value === "review_propagation"
    ? value
    : null;
}

function extractImportedSecurityIsin(transaction: Transaction) {
  const rawPayload = readOptionalRecord(transaction.rawPayload);
  const imported = readOptionalRecord(rawPayload?._import);
  return (
    normalizeSecurityIdentifier(
      readOptionalString(imported?.security_isin) ??
        extractIsinFromText(readOptionalString(imported?.external_reference)),
    ) || null
  );
}

function normalizedSecurityNameMatches(
  left: string | null | undefined,
  right: string | null | undefined,
) {
  const normalizedLeft = normalizeSecurityText(left);
  const normalizedRight = normalizeSecurityText(right);
  if (!normalizedLeft || !normalizedRight) {
    return false;
  }

  return (
    normalizedLeft === normalizedRight ||
    normalizedLeft.includes(normalizedRight) ||
    normalizedRight.includes(normalizedLeft)
  );
}

function buildSecurityResolutionContext(
  transaction: Transaction,
): SecurityResolutionContext {
  const parsed = parseInvestmentEvent(transaction);
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);
  const reviewContext = readOptionalRecord(llmPayload?.reviewContext);
  const reviewTrigger = normalizeReviewTrigger(reviewContext?.trigger);
  const importedSecurityIsin = extractImportedSecurityIsin(transaction);
  const propagatedExactIsinRaw = readRawOutputString(
    rawOutput,
    "resolved_instrument_isin",
  );
  const propagatedExactIsin = normalizeSecurityIdentifier(
    propagatedExactIsinRaw,
  );
  const allowPropagatedGeneratedResolution =
    reviewTrigger !== "review_propagation" || Boolean(propagatedExactIsin);
  const priorReviewUserContext =
    reviewTrigger === "review_propagation"
      ? null
      : readOptionalString(reviewContext?.previousUserContext);
  const currentReviewUserContext =
    reviewTrigger === "review_propagation"
      ? null
      : readOptionalString(reviewContext?.userProvidedContext);

  const llmHintCandidate = allowPropagatedGeneratedResolution
    ? (readOptionalString(llmNode?.securityHint) ??
      readRawOutputString(rawOutput, "security_hint"))
    : null;
  const resolvedInstrumentName = allowPropagatedGeneratedResolution
    ? readRawOutputString(rawOutput, "resolved_instrument_name")
    : null;
  const resolvedInstrumentIsin = normalizeSecurityIdentifier(
    propagatedExactIsinRaw ??
      importedSecurityIsin ??
      extractIsinFromText(
        transaction.manualNotes,
        priorReviewUserContext,
        currentReviewUserContext,
      ),
  );
  const resolvedInstrumentTicker = readRawOutputString(
    rawOutput,
    "resolved_instrument_ticker",
  );
  const resolvedInstrumentExchange = readRawOutputString(
    rawOutput,
    "resolved_instrument_exchange",
  );
  const currentPrice = allowPropagatedGeneratedResolution
    ? readRawOutputNumberAsString(rawOutput, "current_price")
    : null;
  const currentPriceCurrency = allowPropagatedGeneratedResolution
    ? readRawOutputString(rawOutput, "current_price_currency")
    : null;
  const currentPriceTimestamp = allowPropagatedGeneratedResolution
    ? readRawOutputString(rawOutput, "current_price_timestamp")
    : null;
  const currentPriceSource = allowPropagatedGeneratedResolution
    ? readRawOutputString(rawOutput, "current_price_source")
    : null;
  const currentPriceType = allowPropagatedGeneratedResolution
    ? readRawOutputString(rawOutput, "current_price_type")
    : null;
  const reviewText = normalizeSecurityText(
    [
      transaction.manualNotes,
      priorReviewUserContext,
      currentReviewUserContext,
      resolvedInstrumentName,
      currentPriceType,
    ]
      .filter((value): value is string => Boolean(value))
      .join(" "),
  );
  const rejectsEtf = /\bNOT AN ETF\b|\bNOT ETF\b|\bNON ETF\b/.test(reviewText);
  const prefersMutualFund =
    rejectsEtf ||
    /\b(MUTUAL FUND|INDEX FUND|OEIC)\b/.test(reviewText) ||
    /\bNAV\b/.test(reviewText);
  const prefersEtf = !rejectsEtf && /\bETF\b/.test(reviewText);
  const hint =
    resolvedInstrumentName ??
    parsed.securityHint ??
    llmHintCandidate ??
    resolvedInstrumentTicker;

  return {
    reviewTrigger,
    hint,
    transactionDate: transaction.transactionDate,
    exactInstrumentName: resolvedInstrumentName,
    exactIsin: resolvedInstrumentIsin || null,
    exactTicker: resolvedInstrumentTicker,
    exactExchange: resolvedInstrumentExchange,
    currentPrice,
    currentPriceCurrency,
    currentPriceTimestamp,
    currentPriceSource,
    currentPriceType,
    transactionCurrency: transaction.currencyOriginal,
    prefersEuroShareClass:
      hintPrefersEuroShareClass(hint ?? "") ||
      /\b(EURO|EUR)\b/.test(reviewText),
    rejectsEtf,
    prefersMutualFund,
    prefersEtf,
  };
}

function shouldUseWebResolvedSecurity(context?: SecurityResolutionContext) {
  return Boolean(
    context &&
    (context.prefersMutualFund ||
      normalizeSecurityText(context.currentPriceType).includes("NAV")),
  );
}

function supportsTwelveDataMarketData(
  security: Pick<Security, "providerSymbol" | "assetType">,
) {
  return (
    Boolean(security.providerSymbol?.trim()) &&
    ["stock", "etf", "crypto"].includes(security.assetType)
  );
}

function securityLooksLikeFund(
  security: Pick<Security, "assetType" | "name" | "metadataJson">,
  context?: Pick<
    SecurityResolutionContext,
    "prefersMutualFund" | "currentPriceType"
  >,
) {
  const instrumentType = normalizeSecurityText(
    readOptionalString(
      readOptionalRecord(security.metadataJson)?.instrumentType,
    ) ?? security.name,
  );

  return (
    security.assetType !== "etf" &&
    (context?.prefersMutualFund ||
      normalizeSecurityText(context?.currentPriceType).includes("NAV") ||
      instrumentType.includes("MUTUAL") ||
      instrumentType.includes("INDEX FUND") ||
      instrumentType.includes("OEIC") ||
      (instrumentType.includes("FUND") && !instrumentType.includes("ETF")))
  );
}

function buildWebResolvedProviderSymbol(
  context: SecurityResolutionContext,
  hint: string,
) {
  if (context.prefersMutualFund && context.exactIsin) return context.exactIsin;
  if (context.exactTicker && !context.prefersMutualFund) {
    return context.exactTicker;
  }
  if (context.exactIsin) return context.exactIsin;
  if (context.exactTicker) return context.exactTicker;
  const normalizedHint = normalizeSecurityText(
    context.exactInstrumentName ?? hint,
  ).replace(/[^A-Z0-9]+/g, " ");
  return (
    normalizedHint
      .split(" ")
      .filter(Boolean)
      .slice(0, 6)
      .join("_")
      .slice(0, 48) || "WEB_RESOLVED_SECURITY"
  );
}

function buildWebResolvedSecurity(
  hint: string,
  context: SecurityResolutionContext,
) {
  const providerSymbol = buildWebResolvedProviderSymbol(context, hint);
  const createdAt = new Date().toISOString();

  return {
    id: randomUUID(),
    providerName: "llm_web_search",
    providerSymbol,
    canonicalSymbol: providerSymbol,
    displaySymbol: providerSymbol,
    name: context.exactInstrumentName ?? hint,
    exchangeName: context.exactExchange ?? "WEB",
    micCode: null,
    assetType: context.prefersEtf ? "etf" : "other",
    quoteCurrency:
      context.currentPriceCurrency ??
      (context.prefersEuroShareClass ? "EUR" : context.transactionCurrency),
    country: null,
    isin: context.exactIsin,
    figi: null,
    active: true,
    metadataJson: {
      instrumentType:
        context.prefersEtf ||
        normalizeSecurityText(context.currentPriceType).includes("ETF")
          ? "ETF"
          : "Mutual Fund",
      resolutionSource: "llm_web_search",
      exactInstrumentName: context.exactInstrumentName,
      exactExchange: context.exactExchange,
      currentPriceSource: context.currentPriceSource,
      currentPriceType: context.currentPriceType,
    },
    lastPriceRefreshAt: context.currentPriceTimestamp ?? null,
    createdAt,
  } satisfies Security;
}

function buildWebResolvedLatestPrice(
  security: Security,
  context: SecurityResolutionContext,
  referenceDate: string,
) {
  if (!context.currentPrice || !context.currentPriceCurrency) {
    return null;
  }

  const quoteTimestamp =
    context.currentPriceTimestamp ?? `${referenceDate}T16:00:00Z`;
  const priceDate = quoteTimestamp.slice(0, 10);
  const normalizedPriceType = normalizeSecurityText(context.currentPriceType);

  return {
    securityId: security.id,
    priceDate,
    quoteTimestamp,
    price: context.currentPrice,
    currency: context.currentPriceCurrency,
    sourceName: "llm_web_search",
    isRealtime: normalizedPriceType.includes("LIVE"),
    isDelayed:
      normalizedPriceType.includes("DELAY") ||
      normalizedPriceType.includes("CLOSE") ||
      normalizedPriceType.includes("NAV"),
    marketState: context.currentPriceType ?? null,
    rawJson: {
      source: context.currentPriceSource,
      priceType: context.currentPriceType,
      resolvedVia: "llm_web_search",
    },
    createdAt: new Date().toISOString(),
  } satisfies SecurityPrice;
}

function buildSearchQueries(hint: string, context?: SecurityResolutionContext) {
  const normalized = normalizeSecurityText(hint);
  const strippedQuantity = normalized
    .replace(/\s*@\s*\d+(?:\.\d+)?$/, "")
    .trim();
  const queries = new Set<string>([strippedQuantity]);

  if (context?.exactIsin) {
    queries.add(context.exactIsin);
  }
  if (context?.exactTicker && context?.exactExchange) {
    queries.add(`${context.exactTicker} ${context.exactExchange}`);
  }
  if (context?.exactTicker) {
    queries.add(context.exactTicker);
  }
  if (context?.exactInstrumentName) {
    queries.add(context.exactInstrumentName);
  }

  if (context?.prefersMutualFund) {
    queries.add(`${strippedQuantity} INDEX FUND`);
    queries.add(`${strippedQuantity} MUTUAL FUND`);
  }
  if (context?.prefersEtf) {
    queries.add(`${strippedQuantity} ETF`);
  }

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

function candidateLooksLikeEtf(candidate: SearchCandidate) {
  return candidate.instrumentType.toUpperCase().includes("ETF");
}

function candidateLooksLikeMutualFund(candidate: SearchCandidate) {
  const typeText = candidate.instrumentType.toUpperCase();
  const nameText = candidate.instrumentName.toUpperCase();
  return (
    typeText.includes("MUTUAL") ||
    typeText.includes("FUND") ||
    nameText.includes("INDEX FUND") ||
    nameText.includes("MUTUAL FUND") ||
    nameText.includes("OEIC")
  );
}

function scoreCandidate(
  hint: string,
  candidate: SearchCandidate,
  context?: SecurityResolutionContext,
) {
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

  if (context?.prefersMutualFund) {
    if (candidateLooksLikeMutualFund(candidate)) score += 16;
    if (candidateLooksLikeEtf(candidate)) score -= 18;
  }
  if (context?.prefersEtf) {
    if (candidateLooksLikeEtf(candidate)) score += 12;
    if (candidateLooksLikeMutualFund(candidate)) score -= 10;
  }
  if (
    context?.exactTicker &&
    normalizeSecurityText(candidate.providerSymbol) ===
      normalizeSecurityText(context.exactTicker)
  ) {
    score += 18;
  }
  if (
    context?.exactExchange &&
    [candidate.exchange, candidate.micCode]
      .filter((value): value is string => Boolean(value))
      .some(
        (value) =>
          normalizeSecurityText(value) ===
          normalizeSecurityText(context.exactExchange),
      )
  ) {
    score += 10;
  }
  if (
    context?.exactInstrumentName &&
    normalizedSecurityNameMatches(
      candidate.instrumentName,
      context.exactInstrumentName,
    )
  ) {
    score += 18;
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
    if (
      !/\bPREF(?:ERRED)?\b/.test(normalizedHint) &&
      candidate.instrumentName.toUpperCase().includes("PREFERRED")
    ) {
      score -= 24;
    }
    if (normalizedHint.includes("144")) {
      if (
        [candidate.exchange, candidate.micCode]
          .filter((value): value is string => Boolean(value))
          .some((value) => ["LSE", "XLON"].includes(value.toUpperCase()))
      ) {
        score += 18;
      }
      if (candidate.providerSymbol === "SMSN") {
        score += 12;
      }
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

  if (
    context?.prefersEuroShareClass ??
    hintPrefersEuroShareClass(normalizedHint)
  ) {
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
    if (
      context?.prefersEuroShareClass ??
      hintPrefersEuroShareClass(normalizedHint)
    ) {
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
  const instrumentType = normalizeSecurityText(
    readOptionalString(
      readOptionalRecord(security.metadataJson)?.instrumentType,
    ) ?? security.name,
  );
  const securityLooksLikeEtf =
    security.assetType === "etf" || instrumentType.includes("ETF");
  const securityLooksLikeMutualFund =
    instrumentType.includes("MUTUAL") ||
    instrumentType.includes("INDEX FUND") ||
    instrumentType.includes("OEIC");

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

  if (
    /\bNOT AN ETF\b|\bNOT ETF\b|\bNON ETF\b|\bINDEX FUND\b|\bMUTUAL FUND\b|\bOEIC\b/.test(
      normalizedHint,
    ) &&
    securityLooksLikeEtf &&
    !securityLooksLikeMutualFund
  ) {
    return true;
  }

  return false;
}

function securityConflictsWithResolutionContext(
  security: Security,
  context: SecurityResolutionContext,
) {
  const instrumentType = normalizeSecurityText(
    readOptionalString(
      readOptionalRecord(security.metadataJson)?.instrumentType,
    ) ?? security.name,
  );
  const securityLooksLikeEtf =
    security.assetType === "etf" || instrumentType.includes("ETF");
  const securityLooksLikeMutualFund =
    instrumentType.includes("MUTUAL") ||
    instrumentType.includes("INDEX FUND") ||
    instrumentType.includes("OEIC");
  const normalizedSecurityIsin = normalizeSecurityIdentifier(security.isin);
  const normalizedSecuritySymbols = [
    security.providerSymbol,
    security.canonicalSymbol,
    security.displaySymbol,
  ].map((value) => normalizeSecurityText(value));
  const exactTickerMatches = context.exactTicker
    ? normalizedSecuritySymbols.includes(
        normalizeSecurityText(context.exactTicker),
      )
    : false;
  const exactExchangeMatches = context.exactExchange
    ? [security.exchangeName, security.micCode]
        .filter((value): value is string => Boolean(value))
        .some(
          (value) =>
            normalizeSecurityText(value) ===
            normalizeSecurityText(context.exactExchange),
        )
    : false;
  const exactNameMatches = context.exactInstrumentName
    ? normalizedSecurityNameMatches(security.name, context.exactInstrumentName)
    : false;

  if (context.exactIsin) {
    if (normalizedSecurityIsin === context.exactIsin) {
      return false;
    }

    if (
      exactTickerMatches &&
      (!context.exactExchange || exactExchangeMatches || exactNameMatches)
    ) {
      return false;
    }

    if (!exactNameMatches) {
      return true;
    }
  }

  if (context.prefersEuroShareClass && security.quoteCurrency !== "EUR") {
    return true;
  }

  if (
    context.rejectsEtf &&
    securityLooksLikeEtf &&
    !securityLooksLikeMutualFund
  ) {
    return true;
  }

  if (
    context.prefersMutualFund &&
    securityLooksLikeEtf &&
    !securityLooksLikeMutualFund
  ) {
    return true;
  }

  if (
    context.prefersEtf &&
    securityLooksLikeMutualFund &&
    !securityLooksLikeEtf
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

  logTwelveDataDebug("historical.request", {
    symbol: security.providerSymbol,
    transactionDate,
    url: redactApiKey(url),
  });
  const response = await fetch(url);
  const payload = (await response.json()) as
    | {
        values?: Array<{ datetime: string; close: string }>;
        status?: string;
        message?: string;
        code?: number;
      }
    | string;
  logTwelveDataDebug("historical.response", {
    symbol: security.providerSymbol,
    transactionDate,
    status: response.status,
    ok: response.ok,
    body: payload,
  });
  if (!response.ok || typeof payload === "string") {
    return null;
  }
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

  logTwelveDataDebug("quote.request", {
    symbol: security.providerSymbol,
    referenceDate,
    url: redactApiKey(url),
  });
  const response = await fetch(url);
  const payload = (await response.json()) as Record<string, unknown> | string;
  logTwelveDataDebug("quote.response", {
    symbol: security.providerSymbol,
    referenceDate,
    status: response.status,
    ok: response.ok,
    body: payload,
  });
  if (!response.ok || typeof payload === "string") {
    return null;
  }
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
  if (normalized.includes("ETF")) return "etf";
  if (
    normalized.includes("DEPOSITARY") ||
    normalized.includes("ADR") ||
    normalized.includes("GDR")
  ) {
    return "stock";
  }
  if (normalized.includes("MUTUAL") || normalized.includes("FUND"))
    return "other";
  if (normalized.includes("STOCK")) return "stock";
  if (normalized.includes("CRYPTO")) return "crypto";
  if (normalized.includes("CASH")) return "cash";
  return "other";
}

function findSecurityByHint(
  dataset: DomainDataset,
  hint: string,
  context?: SecurityResolutionContext,
) {
  if (context?.exactIsin) {
    const isinMatch =
      dataset.securities
        .filter(
          (security) =>
            normalizeSecurityIdentifier(security.isin) === context.exactIsin,
        )
        .map((security) => {
          let score = 0;

          if (
            !securityConflictsWithHint(hint, security) &&
            !securityConflictsWithResolutionContext(security, context)
          ) {
            score += 1000;
          }
          if (
            findStoredHistoricalPrice(
              dataset,
              security.id,
              context.transactionDate,
            )
          ) {
            score += 500;
          }
          if (
            dataset.securityPrices.some(
              (price) => price.securityId === security.id,
            )
          ) {
            score += 100;
          }
          if (security.providerName === "manual_fund_nav") {
            score += 80;
          }
          if (securityLooksLikeFund(security, context)) {
            score += 40;
          }
          if (
            normalizedSecurityNameMatches(
              security.name,
              context.exactInstrumentName ?? hint,
            )
          ) {
            score += 20;
          }
          if (
            context.prefersEuroShareClass &&
            security.quoteCurrency === "EUR"
          ) {
            score += 10;
          }

          return { security, score };
        })
        .sort((left, right) => right.score - left.score)[0]?.security ?? null;
    if (isinMatch) {
      return isinMatch;
    }
  }

  const normalizedHint = normalizeSecurityText(hint);
  const aliasMatch = dataset.securityAliases.find(
    (alias) =>
      normalizeSecurityText(alias.aliasTextNormalized) === normalizedHint,
  );
  const aliasedSecurity = aliasMatch
    ? (dataset.securities.find(
        (security) => security.id === aliasMatch.securityId,
      ) ?? null)
    : null;

  if (
    aliasedSecurity &&
    !securityConflictsWithHint(hint, aliasedSecurity) &&
    !(
      context &&
      securityConflictsWithResolutionContext(aliasedSecurity, context)
    )
  ) {
    return aliasedSecurity;
  }

  if (context?.exactTicker) {
    const normalizedExactTicker = normalizeSecurityText(context.exactTicker);
    const exactTickerMatch =
      dataset.securities
        .filter((security) =>
          [
            security.providerSymbol,
            security.canonicalSymbol,
            security.displaySymbol,
          ]
            .map((value) => normalizeSecurityText(value))
            .includes(normalizedExactTicker),
        )
        .filter(
          (security) =>
            !securityConflictsWithHint(hint, security) &&
            !securityConflictsWithResolutionContext(security, context),
        )
        .sort((left, right) => {
          const score = (security: Security) => {
            let value = 0;
            if (
              context.exactExchange &&
              [security.exchangeName, security.micCode]
                .filter((entry): entry is string => Boolean(entry))
                .some(
                  (entry) =>
                    normalizeSecurityText(entry) ===
                    normalizeSecurityText(context.exactExchange),
                )
            ) {
              value += 20;
            }
            if (
              context.exactInstrumentName &&
              normalizedSecurityNameMatches(
                security.name,
                context.exactInstrumentName,
              )
            ) {
              value += 10;
            }
            if (
              context.prefersEuroShareClass &&
              security.quoteCurrency === "EUR"
            ) {
              value += 5;
            }
            return value;
          };

          return score(right) - score(left);
        })[0] ?? null;
    if (exactTickerMatch) {
      return exactTickerMatch;
    }
  }

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
  if (
    directMatch &&
    !securityConflictsWithHint(hint, directMatch) &&
    !(context && securityConflictsWithResolutionContext(directMatch, context))
  ) {
    return directMatch;
  }

  return null;
}

function buildConfirmedSecurityAliases(input: {
  transaction: Transaction;
  resolvedSecurityId: string | null;
  resolutionContext: SecurityResolutionContext;
}) {
  if (!input.resolvedSecurityId) {
    return [] as SecurityAlias[];
  }

  const llmPayload = readOptionalRecord(input.transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);
  const reviewContext = readOptionalRecord(llmPayload?.reviewContext);
  const resolutionProcess = readRawOutputString(
    rawOutput,
    "resolution_process",
  );
  const exactIsin = normalizeSecurityIdentifier(
    readRawOutputString(rawOutput, "resolved_instrument_isin"),
  );

  if (!exactIsin && !resolutionProcess) {
    return [] as SecurityAlias[];
  }

  const aliasTexts = new Set(
    [
      input.transaction.descriptionRaw,
      input.transaction.descriptionClean,
      input.resolutionContext.hint,
      readOptionalString(llmNode?.securityHint),
      readRawOutputString(rawOutput, "security_hint"),
      readRawOutputString(rawOutput, "resolved_instrument_name"),
      exactIsin || null,
    ]
      .map((value) => normalizeSecurityText(value))
      .filter((value) => value.length >= 6),
  );
  const aliasSource = readOptionalString(reviewContext?.userProvidedContext)
    ? "manual"
    : "provider";
  const confidence = exactIsin ? "0.9900" : "0.9700";
  const createdAt = new Date().toISOString();

  return [...aliasTexts].map(
    (aliasTextNormalized) =>
      ({
        id: randomUUID(),
        securityId: input.resolvedSecurityId as string,
        aliasTextNormalized,
        aliasSource,
        templateId: null,
        confidence,
        createdAt,
      }) satisfies SecurityAlias,
  );
}

async function resolveSecurity(
  dataset: DomainDataset,
  hint: string,
  apiKey: string | null,
  context?: SecurityResolutionContext,
) {
  const existing = findSecurityByHint(dataset, hint, context);
  if (existing) {
    return {
      security: existing,
      insertedSecurity: null,
      insertedAlias: null,
    };
  }

  if (
    shouldUseWebResolvedSecurity(context) &&
    (context?.exactInstrumentName || context?.exactIsin || context?.exactTicker)
  ) {
    const security = buildWebResolvedSecurity(hint, context);
    const alias = {
      id: randomUUID(),
      securityId: security.id,
      aliasTextNormalized: normalizeSecurityText(hint),
      aliasSource: "provider",
      templateId: null,
      confidence: "0.9500",
      createdAt: new Date().toISOString(),
    } satisfies SecurityAlias;

    return {
      security,
      insertedSecurity: security,
      insertedAlias: alias,
    };
  }

  if (shouldUseWebResolvedSecurity(context)) {
    return {
      security: null,
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
  const queries = buildSearchQueries(hint, context);
  for (const query of queries) {
    const candidates = await fetchSearchCandidates(query, apiKey);
    for (const candidate of candidates) {
      const score = scoreCandidate(hint, candidate, context);
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
    isin: context?.exactIsin ?? null,
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

function shouldReplaceFundQuantityFromHistoricalPrice(input: {
  dataset: DomainDataset;
  transaction: Transaction;
  resolvedSecurity: Security;
  resolutionContext: SecurityResolutionContext;
  quantity: string | null;
  historicalPrice: SecurityPrice | null;
}) {
  if (!input.quantity || !input.historicalPrice) {
    return false;
  }
  if (!securityLooksLikeFund(input.resolvedSecurity, input.resolutionContext)) {
    return false;
  }

  const impliedUnitPrice = inferImpliedUnitPrice(
    input.dataset,
    input.transaction,
    input.quantity,
    input.historicalPrice.currency,
  );
  if (!impliedUnitPrice) {
    return false;
  }

  const implied = new Decimal(impliedUnitPrice);
  const historical = new Decimal(input.historicalPrice.price);
  if (implied.lte(0) || historical.lte(0)) {
    return false;
  }

  const lowerBound = new Decimal(1).minus(
    FUND_HISTORICAL_PRICE_OVERRIDE_TOLERANCE,
  );
  const upperBound = new Decimal(1).plus(
    FUND_HISTORICAL_PRICE_OVERRIDE_TOLERANCE,
  );
  const ratio = implied.div(historical);
  return ratio.lessThan(lowerBound) || ratio.greaterThan(upperBound);
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

function isInvestmentTradeTransaction(
  transactionClass: Transaction["transactionClass"],
) {
  return (
    transactionClass === "investment_trade_buy" ||
    transactionClass === "investment_trade_sell"
  );
}

function normalizeTradeQuantity(
  transactionClass: Transaction["transactionClass"],
  quantity: string | null | undefined,
) {
  const normalizedQuantity = normalizeQuantityValue(quantity);
  if (!normalizedQuantity) {
    return null;
  }

  const absoluteQuantity = new Decimal(normalizedQuantity).abs();
  if (absoluteQuantity.eq(0)) {
    return null;
  }

  if (transactionClass === "investment_trade_sell") {
    return absoluteQuantity.negated().toFixed(8);
  }
  if (transactionClass === "investment_trade_buy") {
    return absoluteQuantity.toFixed(8);
  }

  return null;
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
  const quantityDecimal = new Decimal(normalizedQuantity).abs();
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

  if (securityLooksLikeFund(resolvedSecurity)) {
    return `Mapped to ${resolvedSecurity.displaySymbol}, but no stored NAV was available in security_prices to derive quantity for "${hint}".`;
  }

  if (!supportsTwelveDataMarketData(resolvedSecurity)) {
    return `Mapped to ${resolvedSecurity.displaySymbol}, but no reliable historical price was available to derive quantity for "${hint}".`;
  }

  if (!apiKeyAvailable) {
    return `Mapped to ${resolvedSecurity.displaySymbol}, but market-data enrichment is unavailable, so quantity could not be derived for "${hint}".`;
  }

  return `Mapped to ${resolvedSecurity.displaySymbol}, but Twelve Data did not return a usable historical price to derive quantity for "${hint}".`;
}

function buildUnresolvedSecurityReviewReason(
  reviewHint: string,
  resolutionContext: SecurityResolutionContext,
  apiKeyAvailable: boolean,
) {
  if (shouldUseWebResolvedSecurity(resolutionContext)) {
    return `Security mapping unresolved after analyzer web search for "${reviewHint}".`;
  }
  if (apiKeyAvailable) {
    return `Security mapping unresolved after Twelve Data symbol search for "${reviewHint}".`;
  }
  return `Security mapping unresolved for "${reviewHint}".`;
}

function scoreStoredPriceCandidate(price: SecurityPrice) {
  const rawJson = readOptionalRecord(price.rawJson);
  let score = 0;

  if (!isPlaceholderSecurityPrice(price)) {
    score += 20;
  }
  if (normalizeSecurityText(price.marketState).includes("NAV")) {
    score += 20;
  }
  if (
    normalizeSecurityText(readOptionalString(rawJson?.priceType)).includes(
      "NAV",
    )
  ) {
    score += 10;
  }

  return score;
}

function findStoredHistoricalPriceForSecurityIds(
  dataset: DomainDataset,
  securityIds: readonly string[],
  transactionDate: string,
) {
  const securityIdSet = new Set(securityIds);
  return (
    [...dataset.securityPrices]
      .filter(
        (price) =>
          securityIdSet.has(price.securityId) &&
          price.priceDate <= transactionDate &&
          dayDistance(price.priceDate, transactionDate) <=
            MAX_HISTORICAL_PRICE_DRIFT_DAYS,
      )
      .sort((left, right) => {
        const scoreDelta =
          scoreStoredPriceCandidate(right) - scoreStoredPriceCandidate(left);
        if (scoreDelta !== 0) {
          return scoreDelta;
        }
        return `${right.priceDate}${right.quoteTimestamp}${right.createdAt}`.localeCompare(
          `${left.priceDate}${left.quoteTimestamp}${left.createdAt}`,
        );
      })[0] ?? null
  );
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
  return findStoredHistoricalPriceForSecurityIds(
    dataset,
    [securityId],
    transactionDate,
  );
}

function findStoredHistoricalPriceByIsin(
  dataset: DomainDataset,
  isin: string,
  transactionDate: string,
) {
  const matchingSecurityIds = dataset.securities
    .filter((security) => normalizeSecurityIdentifier(security.isin) === isin)
    .map((security) => security.id);

  if (matchingSecurityIds.length === 0) {
    return null;
  }

  return findStoredHistoricalPriceForSecurityIds(
    dataset,
    matchingSecurityIds,
    transactionDate,
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
          price.securityId === securityId &&
          price.priceDate === targetDate &&
          !isPlaceholderSecurityPrice(price),
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

  if (!isInvestmentTradeTransaction(transaction.transactionClass)) {
    return false;
  }

  return transaction.needsReview;
}

function shouldClearDeterministicNonTradeReview(
  transaction: Transaction,
  parsedTransactionClass: Transaction["transactionClass"],
) {
  return (
    transaction.needsReview &&
    parsedTransactionClass === transaction.transactionClass &&
    [
      "interest",
      "dividend",
      "fee",
      "transfer_internal",
      "balance_adjustment",
    ].includes(transaction.transactionClass)
  );
}

type RebuildReviewDecision = {
  needsReview: boolean;
  reviewReason: string | null;
};

function resolveRebuildReviewDecision(input: {
  transaction: Transaction;
  parsedTransactionClass: Transaction["transactionClass"];
  reviewHint: string;
  resolutionContext: SecurityResolutionContext;
  resolvedSecurityId: string | null;
  resolvedSecurity: Security | null;
  quantity: string | null;
  marketPriceReviewReason: string | null;
  apiKeyAvailable: boolean;
}): RebuildReviewDecision | null {
  if (input.marketPriceReviewReason) {
    return {
      needsReview: true,
      reviewReason: input.marketPriceReviewReason,
    };
  }

  if (
    shouldClearDeterministicNonTradeReview(
      input.transaction,
      input.parsedTransactionClass,
    ) ||
    shouldClearReview(
      input.transaction,
      input.resolvedSecurityId,
      input.quantity,
    )
  ) {
    return {
      needsReview: false,
      reviewReason: null,
    };
  }

  if (
    isInvestmentTradeTransaction(input.transaction.transactionClass) &&
    input.resolvedSecurityId &&
    !input.quantity
  ) {
    return {
      needsReview: true,
      reviewReason: buildQuantityReviewReason(
        input.reviewHint,
        input.resolvedSecurity,
        input.apiKeyAvailable,
      ),
    };
  }

  if (
    isInvestmentTradeTransaction(input.transaction.transactionClass) &&
    !input.resolvedSecurityId &&
    input.reviewHint
  ) {
    return {
      needsReview: true,
      reviewReason: buildUnresolvedSecurityReviewReason(
        input.reviewHint,
        input.resolutionContext,
        input.apiKeyAvailable,
      ),
    };
  }

  return null;
}

function setTransactionReviewState(
  transaction: Transaction,
  patch: ResolvedTransactionPatch,
  decision: RebuildReviewDecision | null,
) {
  if (!decision) {
    return false;
  }

  if (
    transaction.needsReview === decision.needsReview &&
    (transaction.reviewReason ?? null) === decision.reviewReason
  ) {
    return false;
  }

  transaction.needsReview = decision.needsReview;
  transaction.reviewReason = decision.reviewReason;
  patch.needsReview = decision.needsReview;
  patch.reviewReason = decision.reviewReason;
  return true;
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

function applyParsedInvestmentClassificationFallback(
  transaction: Transaction,
  parsedTransactionClass: Transaction["transactionClass"],
) {
  if (
    transaction.transactionClass !== "unknown" ||
    parsedTransactionClass === "unknown"
  ) {
    return;
  }

  transaction.transactionClass = parsedTransactionClass;
  transaction.categoryCode = categoryCodeForInvestmentClass(
    parsedTransactionClass,
  );
  transaction.classificationStatus = "investment_parser";
  transaction.classificationSource = "investment_parser";
  transaction.classificationConfidence = "0.96";
}

function syncConfirmedSecurityAliases(input: {
  dataset: DomainDataset;
  insertedAliases: SecurityAlias[];
  transaction: Transaction;
  resolvedSecurityId: string | null;
  resolutionContext: SecurityResolutionContext;
}) {
  for (const alias of buildConfirmedSecurityAliases({
    transaction: input.transaction,
    resolvedSecurityId: input.resolvedSecurityId,
    resolutionContext: input.resolutionContext,
  })) {
    const alreadyExists = input.dataset.securityAliases.some(
      (candidate) =>
        candidate.securityId === alias.securityId &&
        normalizeSecurityText(candidate.aliasTextNormalized) ===
          normalizeSecurityText(alias.aliasTextNormalized),
    );
    if (alreadyExists) {
      continue;
    }

    input.insertedAliases.push(alias);
    input.dataset.securityAliases.unshift(alias);
  }
}

export async function prepareInvestmentRebuild(
  dataset: DomainDataset,
  referenceDate: string,
  options?: {
    onProgress?: (progress: InvestmentRebuildProgress) => Promise<void> | void;
    historicalLookupTransactionIds?: readonly string[];
  },
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
  const upsertedPriceIndexes = new Map<string, number>();
  const historicalPriceCache = new Map<string, SecurityPrice | null>();
  const latestPriceCache = new Map<string, SecurityPrice | null>();
  const fundNavBackfillRequests: FundNavBackfillRequest[] = [];
  const historicalLookupTransactionIds = options?.historicalLookupTransactionIds
    ? new Set(options.historicalLookupTransactionIds)
    : null;

  const sameRecordedPrice = (left: SecurityPrice, right: SecurityPrice) =>
    left.quoteTimestamp === right.quoteTimestamp &&
    left.price === right.price &&
    left.currency === right.currency &&
    left.isRealtime === right.isRealtime &&
    left.isDelayed === right.isDelayed &&
    left.marketState === right.marketState &&
    JSON.stringify(left.rawJson) === JSON.stringify(right.rawJson);

  const recordPrice = (price: SecurityPrice | null) => {
    if (!price) return;

    const priceKey = `${price.securityId}:${price.priceDate}:${price.sourceName}`;
    const existingStoredPrice =
      workingDataset.securityPrices.find(
        (row) =>
          row.securityId === price.securityId &&
          row.priceDate === price.priceDate &&
          row.sourceName === price.sourceName,
      ) ?? null;
    const needsPersist =
      !existingStoredPrice || !sameRecordedPrice(existingStoredPrice, price);
    if (needsPersist) {
      const existingUpsertIndex = upsertedPriceIndexes.get(priceKey);
      if (existingUpsertIndex === undefined) {
        upsertedPriceIndexes.set(priceKey, upsertedPrices.length);
        upsertedPrices.push(price);
      } else {
        upsertedPrices[existingUpsertIndex] = price;
      }
    }
    if (
      !existingStoredPrice ||
      !sameRecordedPrice(existingStoredPrice, price)
    ) {
      logTwelveDataDebug("price.recorded", {
        symbol:
          workingDataset.securities.find(
            (security) => security.id === price.securityId,
          )?.providerSymbol ?? price.securityId,
        priceDate: price.priceDate,
        quoteTimestamp: price.quoteTimestamp,
        price: price.price,
        currency: price.currency,
      });
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
    security: Pick<
      Security,
      | "id"
      | "providerSymbol"
      | "quoteCurrency"
      | "isin"
      | "assetType"
      | "name"
      | "metadataJson"
    >,
    transactionDate: string,
    context?: SecurityResolutionContext,
    options?: {
      allowExternalLookup?: boolean;
      triggerTransactionId?: string;
    },
  ) => {
    const cacheKey = `${security.id}:${transactionDate}`;
    if (historicalPriceCache.has(cacheKey)) {
      return historicalPriceCache.get(cacheKey) ?? null;
    }

    const normalizedIsin = normalizeSecurityIdentifier(
      context?.exactIsin ?? security.isin,
    );
    const storedPrice =
      findStoredHistoricalPrice(workingDataset, security.id, transactionDate) ??
      (normalizedIsin
        ? findStoredHistoricalPriceByIsin(
            workingDataset,
            normalizedIsin,
            transactionDate,
          )
        : null);
    if (storedPrice) {
      historicalPriceCache.set(cacheKey, storedPrice);
      return storedPrice;
    }

    const securityRecord =
      workingDataset.securities.find(
        (candidate) => candidate.id === security.id,
      ) ?? null;
    if (
      options?.allowExternalLookup !== false &&
      securityRecord &&
      securityLooksLikeFund(securityRecord, context) &&
      options?.triggerTransactionId
    ) {
      const request = buildFundNavBackfillRequest({
        security: securityRecord,
        transactionDate,
        triggerTransactionId: options.triggerTransactionId,
        isin: normalizedIsin,
      });
      if (request) {
        fundNavBackfillRequests.push(request);
      }
    }
    if (
      options?.allowExternalLookup !== false &&
      apiKey &&
      securityRecord &&
      supportsTwelveDataMarketData(securityRecord)
    ) {
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

    if (
      options?.allowExternalLookup !== false &&
      securityRecord &&
      securityLooksLikeFund(securityRecord, context)
    ) {
      const exactIsin = normalizeSecurityIdentifier(
        context?.exactIsin ?? securityRecord.isin,
      );
      if (exactIsin) {
        try {
          const fetchedFtPrice = await fetchFtFundPriceAtOrBeforeDate({
            securityId: securityRecord.id,
            isin: exactIsin,
            quoteCurrency: securityRecord.quoteCurrency,
            transactionDate,
            maxDriftDays: MAX_HISTORICAL_PRICE_DRIFT_DAYS,
          });
          if (fetchedFtPrice.price) {
            historicalPriceCache.set(cacheKey, fetchedFtPrice.price);
            return fetchedFtPrice.price;
          }
        } catch {
          // Let the asynchronous backfill job retry later.
        }
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

    const price =
      apiKey && supportsTwelveDataMarketData(security)
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

  const rebuildInvestmentTransaction = async (transaction: Transaction) => {
    const parsed = parseInvestmentEvent(transaction);
    const originalTransactionClass = transaction.transactionClass;
    const originalCategoryCode = transaction.categoryCode ?? null;
    const originalClassificationStatus = transaction.classificationStatus;
    const originalClassificationSource = transaction.classificationSource;
    const originalClassificationConfidence =
      transaction.classificationConfidence;
    const originalQuantity = transaction.quantity ?? null;
    const originalUnitPriceOriginal = transaction.unitPriceOriginal ?? null;
    applyParsedInvestmentClassificationFallback(
      transaction,
      parsed.transactionClass,
    );

    let resolvedSecurityId = transaction.securityId ?? null;
    let quantity = normalizeQuantityValue(
      transaction.quantity ?? parsed.quantity ?? null,
    );
    let unitPriceOriginal =
      transaction.unitPriceOriginal ?? parsed.unitPriceOriginal ?? null;

    const resolutionContext = buildSecurityResolutionContext(transaction);
    const hint = resolutionContext.hint;
    const hintedSecurity = hint
      ? findSecurityByHint(workingDataset, hint, resolutionContext)
      : null;
    if (hintedSecurity) {
      resolvedSecurityId = hintedSecurity.id;
    }

    const currentResolvedSecurity = resolvedSecurityId
      ? (workingDataset.securities.find(
          (security) => security.id === resolvedSecurityId,
        ) ?? null)
      : null;
    if (
      hint &&
      currentResolvedSecurity &&
      (securityConflictsWithHint(hint, currentResolvedSecurity) ||
        securityConflictsWithResolutionContext(
          currentResolvedSecurity,
          resolutionContext,
        ))
    ) {
      resolvedSecurityId = null;
    }

    if (!resolvedSecurityId && hint) {
      const resolved = await resolveSecurity(
        workingDataset,
        hint,
        apiKey,
        resolutionContext,
      );
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
    syncConfirmedSecurityAliases({
      dataset: workingDataset,
      insertedAliases,
      transaction,
      resolvedSecurityId,
      resolutionContext,
    });

    const isTrade =
      Boolean(resolvedSecurity) &&
      isInvestmentTradeTransaction(transaction.transactionClass);
    const latestWebResolvedPrice = resolvedSecurity
      ? buildWebResolvedLatestPrice(
          resolvedSecurity,
          resolutionContext,
          referenceDate,
        )
      : null;
    recordPrice(latestWebResolvedPrice);
    if (resolvedSecurity && latestWebResolvedPrice) {
      latestPriceCache.set(resolvedSecurity.id, latestWebResolvedPrice);
    }

    const historicalPrice =
      isTrade && resolvedSecurity
        ? await loadHistoricalPrice(
            resolvedSecurity,
            transaction.transactionDate,
            resolutionContext,
            {
              allowExternalLookup:
                !historicalLookupTransactionIds ||
                historicalLookupTransactionIds.has(transaction.id),
              triggerTransactionId: transaction.id,
            },
          )
        : null;
    recordPrice(historicalPrice);

    let quantityDerivedFromHistoricalPrice = false;
    if (resolvedSecurity && historicalPrice) {
      if (
        !quantity ||
        shouldReplaceFundQuantityFromHistoricalPrice({
          dataset: workingDataset,
          transaction,
          resolvedSecurity,
          resolutionContext,
          quantity,
          historicalPrice,
        })
      ) {
        const historicalQuantity = inferQuantityFromPrice(
          workingDataset,
          transaction,
          historicalPrice,
        );
        if (historicalQuantity) {
          quantity = historicalQuantity;
          quantityDerivedFromHistoricalPrice = true;
        }
      }
      unitPriceOriginal = quantityDerivedFromHistoricalPrice
        ? historicalPrice.price
        : (unitPriceOriginal ?? historicalPrice.price);
    }

    if (isInvestmentTradeTransaction(transaction.transactionClass)) {
      quantity = normalizeTradeQuantity(transaction.transactionClass, quantity);
    } else {
      quantity = null;
      unitPriceOriginal = null;
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
    const rebuildEvidence = buildTransactionRebuildEvidence({
      resolvedSecurityId,
      historicalPrice,
      quantityDerivedFromHistoricalPrice,
    });
    const existingLlmPayload = readOptionalRecord(transaction.llmPayload);
    const existingRebuildEvidence = readOptionalRecord(
      existingLlmPayload?.rebuildEvidence,
    );
    const existingAnalysisStatus = readOptionalString(
      existingLlmPayload?.analysisStatus,
    );
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

    if (resolvedSecurityId !== (transaction.securityId ?? null)) {
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
    if (
      rebuildEvidence &&
      JSON.stringify(rebuildEvidence) !==
        JSON.stringify(existingRebuildEvidence)
    ) {
      patch.rebuildEvidence = rebuildEvidence;
      changed = true;
    }

    const reviewHint = hint ?? parsed.securityHint ?? "trade";
    const reviewStateChanged = setTransactionReviewState(
      transaction,
      patch,
      resolveRebuildReviewDecision({
        transaction,
        parsedTransactionClass: parsed.transactionClass,
        reviewHint,
        resolutionContext,
        resolvedSecurityId,
        resolvedSecurity,
        quantity,
        marketPriceReviewReason,
        apiKeyAvailable: Boolean(apiKey),
      }),
    );
    changed = reviewStateChanged || changed;

    if (
      existingAnalysisStatus === "pending" &&
      transaction.needsReview === false
    ) {
      patch.llmPayload = {
        analysisStatus: "skipped",
        completedAt: new Date().toISOString(),
        explanation:
          readOptionalString(existingLlmPayload?.explanation) ??
          "Investment rebuild resolved the transaction before queued enrichment completed.",
      };
      changed = true;
    }

    if (changed) {
      upsertTransactionPatch(transactionPatches, patch);
    }
  };

  const reconcileTradeMarketPriceReview = async (transaction: Transaction) => {
    const security = workingDataset.securities.find(
      (candidate) => candidate.id === transaction.securityId,
    );
    const normalizedQuantity = normalizeQuantityValue(transaction.quantity);
    if (!security || !normalizedQuantity) {
      return;
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
    const reviewDecision: RebuildReviewDecision | null = marketPriceReviewReason
      ? {
          needsReview: true,
          reviewReason: marketPriceReviewReason,
        }
      : transaction.needsReview &&
          /diverges from available market data/i.test(
            transaction.reviewReason ?? "",
          )
        ? {
            needsReview: false,
            reviewReason: null,
          }
        : null;
    const patch: ResolvedTransactionPatch = { id: transaction.id };
    if (setTransactionReviewState(transaction, patch, reviewDecision)) {
      upsertTransactionPatch(transactionPatches, patch);
    }
  };

  for (const transaction of investmentTransactions) {
    await rebuildInvestmentTransaction(transaction);
  }

  for (const transaction of workingDataset.transactions) {
    const account = workingDataset.accounts.find(
      (candidate) => candidate.id === transaction.accountId,
    );
    if (
      account?.assetDomain !== "investment" ||
      transaction.transactionDate > referenceDate ||
      !isInvestmentTradeTransaction(transaction.transactionClass) ||
      !transaction.securityId
    ) {
      continue;
    }

    await reconcileTradeMarketPriceReview(transaction);
  }

  if (apiKey) {
    const currentPositionsSecurityIds = new Set(
      workingDataset.transactions
        .filter(
          (transaction) =>
            isInvestmentTradeTransaction(transaction.transactionClass) &&
            transaction.securityId,
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
    fundNavBackfillRequests: mergeFundNavBackfillRequests(
      fundNavBackfillRequests,
    ),
    positions: rebuilt.positions,
    snapshots: rebuilt.snapshots,
  };
}
