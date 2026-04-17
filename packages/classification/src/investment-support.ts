import {
  createTextEmbeddingClient,
  isTextEmbeddingConfigured,
  type TextEmbeddingClient,
} from "@myfinance/llm";
import type {
  Account,
  AuditEvent,
  DomainDataset,
  LearnedReviewExample,
  Transaction,
} from "@myfinance/domain";
import {
  buildAllowedCategoriesForAccount,
  buildAllowedTransactionClassesForAccount,
  buildLiveHoldingRows,
  extractIsinFromText,
  getAllowedCategoryCodesForAccount,
  getDatasetLatestDate,
  normalizeDescription,
  normalizeSecurityIdentifier,
  normalizeInvestmentMatchingText,
  resolveConstrainedEconomicEntityId,
} from "@myfinance/domain";

import {
  normalizeOptionalText,
  readOptionalBoolean,
  readOptionalRecord,
  readOptionalString,
  readRawOutputString,
  readUnknownArray,
} from "./utils";

export type DeterministicClassification = {
  transactionClass: string;
  categoryCode: string | null;
  merchantNormalized: string | null;
  counterpartyName: string | null;
  economicEntityId: string;
  classificationStatus: Transaction["classificationStatus"];
  classificationSource: Transaction["classificationSource"];
  classificationConfidence: string;
  explanation: string;
  needsReview: boolean;
  reviewReason: string | null;
  securityHint: string | null;
  quantity: string | null;
  unitPriceOriginal: string | null;
};

type HistoricalReviewExample = {
  auditEventId: string;
  objectId: string;
  createdAt: string;
  accountId: string | null;
  institutionName: string | null;
  transaction: {
    transactionDate: string | null;
    postedDate: string | null;
    amountOriginal: string | null;
    currencyOriginal: string | null;
    descriptionRaw: string | null;
    merchantNormalized: string | null;
    counterpartyName: string | null;
    securityId: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
  };
  initialInference: {
    transactionClass: string | null;
    categoryCode: string | null;
    classificationSource: string | null;
    classificationStatus: string | null;
    classificationConfidence: string | null;
    needsReview: boolean | null;
    reviewReason: string | null;
    model: string | null;
    explanation: string | null;
    reason: string | null;
  };
  userFeedback: string;
  correctedOutcome: {
    transactionClass: string | null;
    categoryCode: string | null;
    merchantNormalized: string | null;
    counterpartyName: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
    reviewReason: string | null;
  };
};

type PersistedSecurityMapping = {
  securityId: string;
  matchedAlias: string;
  aliasSource: string;
  confidence: string;
  providerSymbol: string;
  displaySymbol: string;
  securityName: string;
  isin: string | null;
};

export interface SimilarAccountTransactionMatch {
  transaction: Transaction;
  score: number;
}

export interface ReviewPropagationTransactionMatch extends SimilarAccountTransactionMatch {
  semanticSimilarity: number | null;
  lexicalScore: number;
  exactMatch: boolean;
}

const INVESTMENT_DESCRIPTOR_STOPWORDS = new Set([
  "BUY",
  "SELL",
  "FUND",
  "FUNDS",
  "ETF",
  "ETFS",
  "INDEX",
  "STOCK",
  "STOCKS",
  "SHARE",
  "SHARES",
  "UCITS",
  "OEIC",
  "ACC",
  "ACCU",
  "ACCUMULATION",
  "ACCUMULATING",
  "DIST",
  "DISTRIBUTION",
  "DISTRIBUTING",
  "CLASS",
  "CL",
  "EUR",
  "USD",
  "GBP",
  "NAV",
]);

const FUND_BRAND_TOKENS = new Set([
  "VANGUARD",
  "ISHARES",
  "AMUNDI",
  "BLACKROCK",
  "INVESCO",
  "FIDELITY",
  "JPMORGAN",
  "SPDR",
  "XTRACKERS",
  "HSBC",
  "LYXOR",
  "UBS",
  "DWS",
  "FRANKLIN",
]);

function looksLikeFundDescriptor(normalizedText: string) {
  return /\b(FUND|ETF|INDEX|UCITS|OEIC|NAV)\b/.test(normalizedText);
}

function tokenizeInvestmentMatchingText(
  value: string | null | undefined,
  options: { distinctiveOnly?: boolean } = {},
) {
  const normalized = normalizeInvestmentMatchingText(value);
  if (!normalized) {
    return new Set<string>();
  }

  const stopwords = new Set(INVESTMENT_DESCRIPTOR_STOPWORDS);
  if (options.distinctiveOnly && looksLikeFundDescriptor(normalized)) {
    for (const token of FUND_BRAND_TOKENS) {
      stopwords.add(token);
    }
  }

  return new Set(
    normalized
      .split(/[^A-Z0-9]+/)
      .filter((token) => token.length >= 2)
      .filter((token) =>
        options.distinctiveOnly ? !stopwords.has(token) : true,
      ),
  );
}

function tokenizePromptText(value: string | null | undefined) {
  const normalized = normalizeOptionalText(value);
  if (!normalized) {
    return new Set<string>();
  }

  return new Set(
    normalizeDescription(normalized)
      .comparison.split(/[^A-Z0-9]+/)
      .filter((token) => token.length >= 3),
  );
}

export function getReviewPropagationEmbeddingModel() {
  return (
    process.env.REVIEW_PROPAGATION_EMBEDDING_MODEL?.trim() ||
    "gemini-embedding-001"
  );
}

function countOverlappingTokens(left: Set<string>, right: Set<string>) {
  return [...left].filter((token) => right.has(token)).length;
}

function calculateJaccardScore(left: Set<string>, right: Set<string>) {
  const union = new Set([...left, ...right]).size;
  if (union === 0) {
    return 0;
  }
  return countOverlappingTokens(left, right) / union;
}

function calculateCosineSimilarity(left: number[], right: number[]) {
  const length = Math.min(left.length, right.length);
  if (length === 0) {
    return 0;
  }

  let score = 0;
  for (let index = 0; index < length; index += 1) {
    score += left[index]! * right[index]!;
  }
  return score;
}

function buildReviewPropagationContextText(transaction: Transaction) {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);
  const reviewContext = readOptionalRecord(llmPayload?.reviewContext);

  return [
    transaction.descriptionRaw,
    transaction.manualNotes,
    readOptionalString(reviewContext?.previousUserContext),
    readOptionalString(reviewContext?.userProvidedContext),
    readRawOutputString(rawOutput, "resolved_instrument_name"),
    readRawOutputString(rawOutput, "resolved_instrument_isin"),
    readRawOutputString(rawOutput, "current_price_type"),
    readRawOutputString(rawOutput, "reason"),
    readRawOutputString(rawOutput, "explanation"),
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
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

function extractTransactionIsinEvidence(transaction: Transaction) {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);
  const reviewContext = readOptionalRecord(llmPayload?.reviewContext);
  const importedSecurityIsin = extractImportedSecurityIsin(transaction);

  return extractIsinFromText(
    importedSecurityIsin,
    readRawOutputString(rawOutput, "resolved_instrument_isin"),
    transaction.manualNotes,
    readOptionalString(reviewContext?.previousUserContext),
    readOptionalString(reviewContext?.userProvidedContext),
    readRawOutputString(rawOutput, "reason"),
    readRawOutputString(rawOutput, "explanation"),
  );
}

function buildReviewPropagationEvidence(transaction: Transaction) {
  const normalizedDescription = normalizeInvestmentMatchingText(
    transaction.descriptionRaw,
  );
  const contextText = buildReviewPropagationContextText(transaction);
  const combinedText = normalizeInvestmentMatchingText(contextText);
  const allTokens = tokenizeInvestmentMatchingText(transaction.descriptionRaw);
  const distinctiveTokens = tokenizeInvestmentMatchingText(
    transaction.descriptionRaw,
    { distinctiveOnly: true },
  );
  const exactIsin = extractTransactionIsinEvidence(transaction);
  const explicitEtf =
    /\bETF\b/.test(combinedText) &&
    !/\bNOT AN ETF\b|\bNOT ETF\b/.test(combinedText);
  const explicitMutualFund = /\b(MUTUAL FUND|INDEX FUND|OEIC|NAV)\b/.test(
    combinedText,
  );

  return {
    normalizedDescription,
    allTokens,
    distinctiveTokens,
    embeddingText:
      normalizedDescription || normalizeInvestmentMatchingText(contextText),
    exactIsin,
    explicitEtf,
    explicitMutualFund,
  };
}

function extractHistoricalReviewExample(
  auditEvent: AuditEvent,
): HistoricalReviewExample | null {
  if (auditEvent.commandName !== "transactions.review_reanalyze") {
    return null;
  }

  const before = readOptionalRecord(auditEvent.beforeJson);
  const after = readOptionalRecord(auditEvent.afterJson);
  if (!before || !after) {
    return null;
  }

  if (readOptionalBoolean(after.needsReview) === true) {
    return null;
  }

  const afterLlmPayload = readOptionalRecord(after.llmPayload);
  const afterReviewContext = readOptionalRecord(afterLlmPayload?.reviewContext);
  const userFeedback =
    normalizeOptionalText(
      typeof afterReviewContext?.userProvidedContext === "string"
        ? afterReviewContext.userProvidedContext
        : null,
    ) ??
    normalizeOptionalText(
      typeof after.manualNotes === "string" ? after.manualNotes : null,
    );
  if (!userFeedback) {
    return null;
  }

  const beforeLlmPayload = readOptionalRecord(before.llmPayload);
  const beforeLlm = readOptionalRecord(beforeLlmPayload?.llm);
  const afterAccountId =
    normalizeOptionalText(
      typeof after.accountId === "string" ? after.accountId : null,
    ) ??
    normalizeOptionalText(
      typeof before.accountId === "string" ? before.accountId : null,
    );

  return {
    auditEventId: auditEvent.id,
    objectId: auditEvent.objectId,
    createdAt: auditEvent.createdAt,
    accountId: afterAccountId,
    institutionName: normalizeOptionalText(
      typeof after.counterpartyName === "string"
        ? after.counterpartyName
        : null,
    ),
    transaction: {
      transactionDate: normalizeOptionalText(
        typeof before.transactionDate === "string"
          ? before.transactionDate
          : null,
      ),
      postedDate: normalizeOptionalText(
        typeof before.postedDate === "string" ? before.postedDate : null,
      ),
      amountOriginal: normalizeOptionalText(
        typeof before.amountOriginal === "string"
          ? before.amountOriginal
          : null,
      ),
      currencyOriginal: normalizeOptionalText(
        typeof before.currencyOriginal === "string"
          ? before.currencyOriginal
          : null,
      ),
      descriptionRaw: normalizeOptionalText(
        typeof before.descriptionRaw === "string"
          ? before.descriptionRaw
          : null,
      ),
      merchantNormalized: normalizeOptionalText(
        typeof before.merchantNormalized === "string"
          ? before.merchantNormalized
          : null,
      ),
      counterpartyName: normalizeOptionalText(
        typeof before.counterpartyName === "string"
          ? before.counterpartyName
          : null,
      ),
      securityId: normalizeOptionalText(
        typeof before.securityId === "string" ? before.securityId : null,
      ),
      quantity: normalizeOptionalText(
        typeof before.quantity === "string" ? before.quantity : null,
      ),
      unitPriceOriginal: normalizeOptionalText(
        typeof before.unitPriceOriginal === "string"
          ? before.unitPriceOriginal
          : null,
      ),
    },
    initialInference: {
      transactionClass: normalizeOptionalText(
        typeof before.transactionClass === "string"
          ? before.transactionClass
          : null,
      ),
      categoryCode: normalizeOptionalText(
        typeof before.categoryCode === "string" ? before.categoryCode : null,
      ),
      classificationSource: normalizeOptionalText(
        typeof before.classificationSource === "string"
          ? before.classificationSource
          : null,
      ),
      classificationStatus: normalizeOptionalText(
        typeof before.classificationStatus === "string"
          ? before.classificationStatus
          : null,
      ),
      classificationConfidence: normalizeOptionalText(
        typeof before.classificationConfidence === "string"
          ? before.classificationConfidence
          : null,
      ),
      needsReview: readOptionalBoolean(before.needsReview),
      reviewReason: normalizeOptionalText(
        typeof before.reviewReason === "string" ? before.reviewReason : null,
      ),
      model:
        normalizeOptionalText(
          typeof beforeLlm?.model === "string" ? beforeLlm.model : null,
        ) ??
        normalizeOptionalText(
          typeof beforeLlmPayload?.model === "string"
            ? beforeLlmPayload.model
            : null,
        ),
      explanation:
        normalizeOptionalText(
          typeof beforeLlm?.explanation === "string"
            ? beforeLlm.explanation
            : null,
        ) ??
        normalizeOptionalText(
          typeof beforeLlmPayload?.explanation === "string"
            ? beforeLlmPayload.explanation
            : null,
        ),
      reason:
        normalizeOptionalText(
          typeof beforeLlm?.reason === "string" ? beforeLlm.reason : null,
        ) ??
        normalizeOptionalText(
          typeof beforeLlmPayload?.reason === "string"
            ? beforeLlmPayload.reason
            : null,
        ),
    },
    userFeedback,
    correctedOutcome: {
      transactionClass: normalizeOptionalText(
        typeof after.transactionClass === "string"
          ? after.transactionClass
          : null,
      ),
      categoryCode: normalizeOptionalText(
        typeof after.categoryCode === "string" ? after.categoryCode : null,
      ),
      merchantNormalized: normalizeOptionalText(
        typeof after.merchantNormalized === "string"
          ? after.merchantNormalized
          : null,
      ),
      counterpartyName: normalizeOptionalText(
        typeof after.counterpartyName === "string"
          ? after.counterpartyName
          : null,
      ),
      quantity: normalizeOptionalText(
        typeof after.quantity === "string" ? after.quantity : null,
      ),
      unitPriceOriginal: normalizeOptionalText(
        typeof after.unitPriceOriginal === "string"
          ? after.unitPriceOriginal
          : null,
      ),
      reviewReason: normalizeOptionalText(
        typeof after.reviewReason === "string" ? after.reviewReason : null,
      ),
    },
  };
}

function extractLearnedReviewExample(
  example: LearnedReviewExample,
): HistoricalReviewExample | null {
  if (!example.active) {
    return null;
  }

  const transaction = readOptionalRecord(example.sourceTransactionSnapshotJson);
  const initialInference = readOptionalRecord(
    example.initialInferenceSnapshotJson,
  );
  const correctedOutcome = readOptionalRecord(
    example.correctedOutcomeSnapshotJson,
  );
  if (!transaction || !initialInference || !correctedOutcome) {
    return null;
  }

  return {
    auditEventId: example.sourceAuditEventId ?? example.id,
    objectId: example.sourceTransactionId,
    createdAt: example.updatedAt ?? example.createdAt,
    accountId: example.accountId,
    institutionName: null,
    transaction: {
      transactionDate: readOptionalString(transaction.transactionDate),
      postedDate: readOptionalString(transaction.postedDate),
      amountOriginal: readOptionalString(transaction.amountOriginal),
      currencyOriginal: readOptionalString(transaction.currencyOriginal),
      descriptionRaw: readOptionalString(transaction.descriptionRaw),
      merchantNormalized: readOptionalString(transaction.merchantNormalized),
      counterpartyName: readOptionalString(transaction.counterpartyName),
      securityId: readOptionalString(transaction.securityId),
      quantity: readOptionalString(transaction.quantity),
      unitPriceOriginal: readOptionalString(transaction.unitPriceOriginal),
    },
    initialInference: {
      transactionClass: readOptionalString(initialInference.transactionClass),
      categoryCode: readOptionalString(initialInference.categoryCode),
      classificationSource: readOptionalString(
        initialInference.classificationSource,
      ),
      classificationStatus: readOptionalString(
        initialInference.classificationStatus,
      ),
      classificationConfidence: readOptionalString(
        initialInference.classificationConfidence,
      ),
      needsReview: readOptionalBoolean(initialInference.needsReview),
      reviewReason: readOptionalString(initialInference.reviewReason),
      model: readOptionalString(initialInference.model),
      explanation: readOptionalString(initialInference.explanation),
      reason: readOptionalString(initialInference.reason),
    },
    userFeedback: example.userContext,
    correctedOutcome: {
      transactionClass: readOptionalString(correctedOutcome.transactionClass),
      categoryCode: readOptionalString(correctedOutcome.categoryCode),
      merchantNormalized: readOptionalString(
        correctedOutcome.merchantNormalized,
      ),
      counterpartyName: readOptionalString(correctedOutcome.counterpartyName),
      quantity: readOptionalString(correctedOutcome.quantity),
      unitPriceOriginal: readOptionalString(correctedOutcome.unitPriceOriginal),
      reviewReason: readOptionalString(correctedOutcome.reviewReason),
    },
  };
}

function scorePromptReviewExample(
  example: HistoricalReviewExample,
  accountById: Map<string, Account>,
  account: Account,
  transaction: Transaction,
  targetTokens: Set<string>,
) {
  const exampleAccount = example.accountId
    ? accountById.get(example.accountId)
    : null;
  let score = 0;
  if (exampleAccount?.institutionName === account.institutionName) {
    score += 20;
  }
  if (transaction.securityId && example.transaction.securityId === transaction.securityId) {
    score += 30;
  }
  const exampleTokens = tokenizePromptText(example.transaction.descriptionRaw);
  for (const token of targetTokens) {
    if (exampleTokens.has(token)) {
      score += 2;
    }
  }
  return score;
}

export function buildHistoricalReviewExamples(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  limit = 5,
) {
  const accountById = new Map(
    dataset.accounts.map((candidate) => [candidate.id, candidate]),
  );
  const targetTokens = tokenizePromptText(transaction.descriptionRaw);

  return dataset.auditEvents
    .map((auditEvent) => extractHistoricalReviewExample(auditEvent))
    .filter((example): example is HistoricalReviewExample => Boolean(example))
    .filter((example) => example.objectId !== transaction.id)
    .filter((example) => {
      if (!example.accountId) {
        return false;
      }
      const exampleAccount = accountById.get(example.accountId);
      return exampleAccount?.assetDomain === account.assetDomain;
    })
    .sort((left, right) => {
      const scoreDelta =
        scorePromptReviewExample(
          right,
          accountById,
          account,
          transaction,
          targetTokens,
        ) -
        scorePromptReviewExample(
          left,
          accountById,
          account,
          transaction,
          targetTokens,
        );
      if (scoreDelta !== 0) {
        return scoreDelta;
      }

      return (
        new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime()
      );
    })
    .slice(0, limit);
}

export function buildLearnedReviewExamples(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  limit = 5,
) {
  const accountById = new Map(
    dataset.accounts.map((candidate) => [candidate.id, candidate]),
  );
  const targetTokens = tokenizePromptText(transaction.descriptionRaw);
  const promptProfileId =
    account.assetDomain === "investment"
      ? "investment_transaction_analyzer"
      : "cash_transaction_analyzer";
  const eligibleExamples = dataset.learnedReviewExamples.filter(
    (example) =>
      example.active &&
      example.accountId === account.id &&
      example.promptProfileId === promptProfileId &&
      example.sourceTransactionId !== transaction.id,
  );

  return eligibleExamples
    .map((example) => extractLearnedReviewExample(example))
    .filter((example): example is HistoricalReviewExample => Boolean(example))
    .sort((left, right) => {
      const scoreDelta =
        scorePromptReviewExample(
          right,
          accountById,
          account,
          transaction,
          targetTokens,
        ) -
        scorePromptReviewExample(
          left,
          accountById,
          account,
          transaction,
          targetTokens,
        );
      if (scoreDelta !== 0) {
        return scoreDelta;
      }

      return (
        new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime()
      );
    })
    .slice(0, limit);
}

export function buildReviewPromptExamples(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  limit = 5,
) {
  const learnedExamples = buildLearnedReviewExamples(
    dataset,
    account,
    transaction,
    limit,
  );
  if (learnedExamples.length >= limit) {
    return learnedExamples.slice(0, limit);
  }

  const seenObjectIds = new Set(learnedExamples.map((example) => example.objectId));
  const fallbackExamples = buildHistoricalReviewExamples(
    dataset,
    account,
    transaction,
    limit * 2,
  ).filter((example) => !seenObjectIds.has(example.objectId));

  return [...learnedExamples, ...fallbackExamples].slice(0, limit);
}

function scoreSimilarAccountTransaction(
  target: Transaction,
  candidate: Transaction,
) {
  const targetTokens = tokenizePromptText(target.descriptionRaw);
  const candidateTokens = tokenizePromptText(candidate.descriptionRaw);
  const overlappingTokenCount = [...targetTokens].filter((token) =>
    candidateTokens.has(token),
  ).length;
  const unionCount = new Set([...targetTokens, ...candidateTokens]).size;
  const targetMagnitude = Math.abs(Number(target.amountOriginal));
  const candidateMagnitude = Math.abs(Number(candidate.amountOriginal));
  const sameDirection =
    Math.sign(Number(target.amountOriginal)) ===
    Math.sign(Number(candidate.amountOriginal));

  let score = overlappingTokenCount * 2;
  if (unionCount > 0) {
    score += (overlappingTokenCount / unionCount) * 10;
  }
  if (target.securityId && candidate.securityId === target.securityId) {
    score += 20;
  }
  if (
    target.transactionClass !== "unknown" &&
    candidate.transactionClass === target.transactionClass
  ) {
    score += 4;
  }
  if (target.categoryCode && candidate.categoryCode === target.categoryCode) {
    score += 3;
  }
  if (sameDirection) {
    score += 2;
  }
  if (targetMagnitude > 0 && candidateMagnitude > 0) {
    const amountRatio =
      Math.min(targetMagnitude, candidateMagnitude) /
      Math.max(targetMagnitude, candidateMagnitude);
    if (amountRatio >= 0.8) {
      score += 3;
    } else if (amountRatio >= 0.5) {
      score += 1;
    }
  }

  return score;
}

export function rankSimilarAccountTransactions(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  options: {
    limit?: number;
    minScore?: number;
    includeNeedsReview?: boolean;
    requireEarlierDate?: boolean;
  } = {},
): SimilarAccountTransactionMatch[] {
  const limit = options.limit ?? 5;
  const minScore = options.minScore ?? 6;

  return dataset.transactions
    .filter((candidate) => candidate.id !== transaction.id)
    .filter((candidate) => candidate.accountId === account.id)
    .filter((candidate) => !candidate.voidedAt)
    .filter((candidate) =>
      options.includeNeedsReview ? true : candidate.needsReview !== true,
    )
    .filter((candidate) =>
      options.requireEarlierDate
        ? candidate.transactionDate <= transaction.transactionDate
        : true,
    )
    .map((candidate) => ({
      transaction: candidate,
      score: scoreSimilarAccountTransaction(transaction, candidate),
    }))
    .filter((match) => match.score >= minScore)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return (
        new Date(right.transaction.createdAt).getTime() -
        new Date(left.transaction.createdAt).getTime()
      );
    })
    .slice(0, limit);
}

export function parseInvestmentEvent(transaction: Transaction): {
  transactionClass:
    | "investment_trade_buy"
    | "investment_trade_sell"
    | "transfer_internal"
    | "dividend"
    | "interest"
    | "fee"
    | "fx_conversion"
    | "balance_adjustment"
    | "unknown";
  quantity?: string;
  securityHint?: string;
  unitPriceOriginal?: string;
} {
  const comparison = normalizeDescription(
    transaction.descriptionRaw,
  ).comparison;

  if (comparison.includes("DIVIDEND")) {
    return { transactionClass: "dividend" };
  }
  if (/\bTRANSFERENCIAS?\s+ENTRE\s+CUENTAS\b/.test(comparison)) {
    return { transactionClass: "transfer_internal" };
  }
  if (comparison.includes("INTEREST")) {
    return { transactionClass: "interest" };
  }
  if (
    Number(transaction.amountOriginal) === 0 &&
    comparison.includes("IRPF") &&
    comparison.includes("INTERESES")
  ) {
    return { transactionClass: "balance_adjustment" };
  }
  if (
    comparison.startsWith("PERIODO ") &&
    Number(transaction.amountOriginal) > 0
  ) {
    return { transactionClass: "interest" };
  }
  if (comparison.includes("COMMISSION") || comparison.includes("FEE")) {
    return { transactionClass: "fee" };
  }
  if (comparison.includes("FX") || comparison.includes("CONVERSION")) {
    return { transactionClass: "fx_conversion" };
  }

  const quantityMatch = comparison.match(/@\s*([0-9]+(?:\.[0-9]+)?)/);
  const buyMatch = comparison.match(
    /(BUY|SELL)\s+([0-9]+(?:\.[0-9]+)?)\s+(.+)/,
  );

  if (quantityMatch) {
    const quantity = quantityMatch[1];
    const securityHint = comparison.split("@")[0]?.trim() ?? comparison;
    const gross = Math.abs(Number(transaction.amountOriginal));
    const unitPriceOriginal =
      quantity === "0" ? undefined : (gross / Number(quantity)).toFixed(2);
    const transactionClass =
      Number(transaction.amountOriginal) < 0
        ? "investment_trade_buy"
        : "investment_trade_sell";
    return {
      transactionClass,
      quantity:
        quantity === "0"
          ? undefined
          : (normalizeTradeQuantity(transactionClass, quantity) ?? undefined),
      securityHint,
      unitPriceOriginal,
    };
  }

  if (buyMatch) {
    const transactionClass =
      buyMatch[1] === "BUY" ? "investment_trade_buy" : "investment_trade_sell";
    return {
      transactionClass,
      quantity:
        normalizeTradeQuantity(transactionClass, buyMatch[2]) ?? undefined,
      securityHint: buyMatch[3],
    };
  }

  const genericSecurityHint = comparison
    .replace(/\b(?:BUY|SELL)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const looksLikeNamedSecurity =
    /\b(?:ETF|FUND|INDEX|STOCK|SHARES?|UCITS|ADR|GDR|INC|CORP|CORPORATION|HOLDINGS|CLASS|CAP)\b/.test(
      comparison,
    );
  if (looksLikeNamedSecurity && genericSecurityHint) {
    return {
      transactionClass:
        Number(transaction.amountOriginal) < 0
          ? "investment_trade_buy"
          : "investment_trade_sell",
      securityHint: genericSecurityHint,
    };
  }

  return { transactionClass: "unknown" };
}

export function isTradeTransactionClass(
  transactionClass: string,
): transactionClass is "investment_trade_buy" | "investment_trade_sell" {
  return (
    transactionClass === "investment_trade_buy" ||
    transactionClass === "investment_trade_sell"
  );
}

export function normalizeTradeQuantity(
  transactionClass: string,
  quantity: string | null | undefined,
) {
  const normalized = normalizeOptionalText(quantity);
  if (!normalized || !isTradeTransactionClass(transactionClass)) {
    return null;
  }

  const numericQuantity = Number(normalized);
  if (!Number.isFinite(numericQuantity) || numericQuantity === 0) {
    return null;
  }

  const absoluteQuantity = Math.abs(numericQuantity);
  const signedQuantity =
    transactionClass === "investment_trade_sell"
      ? -absoluteQuantity
      : absoluteQuantity;
  return signedQuantity.toFixed(8);
}

export function inferTradeQuantityFromUnitPrice(
  transactionClass: string,
  amountOriginal: string,
  unitPriceOriginal: string | null | undefined,
) {
  if (!isTradeTransactionClass(transactionClass)) {
    return null;
  }

  const normalizedUnitPrice = normalizeOptionalText(unitPriceOriginal);
  if (!normalizedUnitPrice) {
    return null;
  }

  const numericUnitPrice = Number(normalizedUnitPrice);
  const numericAmount = Math.abs(Number(amountOriginal));
  if (
    !Number.isFinite(numericUnitPrice) ||
    numericUnitPrice === 0 ||
    !Number.isFinite(numericAmount) ||
    numericAmount === 0
  ) {
    return null;
  }

  return normalizeTradeQuantity(
    transactionClass,
    (numericAmount / numericUnitPrice).toFixed(8),
  );
}

function buildPersistedInvestmentSecurityMappings(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  deterministic: DeterministicClassification,
) {
  if (
    account.assetDomain !== "investment" ||
    !isTradeTransactionClass(deterministic.transactionClass)
  ) {
    return [] as PersistedSecurityMapping[];
  }

  const importedSecurityIsin = extractImportedSecurityIsin(transaction);
  const candidateAliases = new Set(
    [
      deterministic.securityHint,
      transaction.descriptionRaw,
      transaction.descriptionClean,
      importedSecurityIsin,
    ]
      .map((value) => normalizeDescription(value ?? "").comparison)
      .filter(Boolean),
  );
  const seenSecurityIds = new Set<string>();
  const persistedMappings: PersistedSecurityMapping[] = [];

  if (importedSecurityIsin) {
    const security = dataset.securities.find(
      (candidate) =>
        normalizeSecurityIdentifier(candidate.isin) === importedSecurityIsin,
    );
    if (security) {
      seenSecurityIds.add(security.id);
      persistedMappings.push({
        securityId: security.id,
        matchedAlias: importedSecurityIsin,
        aliasSource: "import_field",
        confidence: "1.00",
        providerSymbol: security.providerSymbol,
        displaySymbol: security.displaySymbol,
        securityName: security.name,
        isin: security.isin ?? null,
      });
    }
  }
  if (candidateAliases.size === 0) {
    return persistedMappings;
  }

  for (const alias of dataset.securityAliases) {
    const normalizedAlias = normalizeDescription(
      alias.aliasTextNormalized,
    ).comparison;
    if (!candidateAliases.has(normalizedAlias)) {
      continue;
    }

    const security =
      dataset.securities.find(
        (candidate) => candidate.id === alias.securityId,
      ) ?? null;
    if (!security || seenSecurityIds.has(security.id)) {
      continue;
    }

    seenSecurityIds.add(security.id);
    persistedMappings.push({
      securityId: security.id,
      matchedAlias: alias.aliasTextNormalized,
      aliasSource: alias.aliasSource,
      confidence: alias.confidence,
      providerSymbol: security.providerSymbol,
      displaySymbol: security.displaySymbol,
      securityName: security.name,
      isin: security.isin ?? null,
    });

    if (persistedMappings.length >= 3) {
      break;
    }
  }

  return persistedMappings;
}

function buildInvestmentPortfolioState(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  deterministic: DeterministicClassification,
) {
  if (account.assetDomain !== "investment") {
    return undefined;
  }

  const asOfDate = getDatasetLatestDate(dataset);
  const holdings = buildLiveHoldingRows(
    dataset,
    { kind: "account", accountId: account.id },
    asOfDate,
  );

  const targetHint = normalizeDescription(
    deterministic.securityHint ??
      transaction.descriptionRaw ??
      transaction.securityId ??
      "",
  ).comparison;
  const matchedHolding =
    holdings.find((holding) => holding.securityId === transaction.securityId) ??
    holdings.find((holding) => {
      if (!targetHint) return false;
      const symbol = normalizeDescription(holding.symbol).comparison;
      const securityName = normalizeDescription(
        holding.securityName,
      ).comparison;
      return (
        targetHint.includes(symbol) ||
        targetHint.includes(securityName) ||
        securityName.includes(targetHint)
      );
    }) ??
    null;

  const normalizedTradeQuantity = normalizeTradeQuantity(
    deterministic.transactionClass,
    transaction.quantity ?? deterministic.quantity ?? null,
  );
  const quantity = Number(normalizedTradeQuantity ?? 0);
  const impliedUnitPrice =
    Number.isFinite(quantity) && Math.abs(quantity) > 0
      ? (
          Math.abs(Number(transaction.amountOriginal)) / Math.abs(quantity)
        ).toFixed(2)
      : null;
  const latestHoldingPrice = matchedHolding?.currentPrice ?? null;
  const sameCurrency =
    matchedHolding?.currentPriceCurrency === transaction.currencyOriginal;
  const priceDeltaPercent =
    impliedUnitPrice &&
    latestHoldingPrice &&
    sameCurrency &&
    Number(latestHoldingPrice) > 0
      ? (
          (Math.abs(Number(impliedUnitPrice) - Number(latestHoldingPrice)) /
            Number(latestHoldingPrice)) *
          100
        ).toFixed(2)
      : null;

  const serializeHolding = (holding: (typeof holdings)[number]) => ({
    securityId: holding.securityId,
    symbol: holding.symbol,
    securityName: holding.securityName,
    quantity: holding.quantity,
    currentPrice: holding.currentPrice,
    currentPriceCurrency: holding.currentPriceCurrency,
    currentValueEur: holding.currentValueEur,
    quoteTimestamp: holding.quoteTimestamp,
    quoteFreshness: holding.quoteFreshness,
  });

  return {
    scope: "account" as const,
    asOfDate,
    holdings: holdings.map(serializeHolding),
    matchedHolding: matchedHolding ? serializeHolding(matchedHolding) : null,
    priceSanityCheck:
      impliedUnitPrice || matchedHolding
        ? {
            impliedUnitPrice,
            impliedUnitPriceCurrency: transaction.currencyOriginal,
            latestHoldingPrice,
            latestHoldingPriceCurrency:
              matchedHolding?.currentPriceCurrency ?? null,
            latestHoldingQuoteTimestamp: matchedHolding?.quoteTimestamp ?? null,
            priceDeltaPercent,
          }
        : null,
  };
}

async function buildReviewPropagationTransactionMatches(
  sourceEvidence: ReturnType<typeof buildReviewPropagationEvidence>,
  candidateMatches: Array<{
    transaction: Transaction;
    candidateEvidence: ReturnType<typeof buildReviewPropagationEvidence>;
    lexicalScore: number;
    exactMatch: boolean;
    distinctiveOverlapCount: number;
    distinctiveJaccard: number;
  }>,
  limit: number,
  embeddingClient: TextEmbeddingClient | null | undefined,
) : Promise<ReviewPropagationTransactionMatch[]> {
  const semanticSimilarityByTransactionId = new Map<string, number>();
  if (embeddingClient) {
    try {
      const embeddingTexts = [
        sourceEvidence.embeddingText || sourceEvidence.normalizedDescription,
        ...candidateMatches.map(
          (match) =>
            match.candidateEvidence.embeddingText ??
            match.candidateEvidence.normalizedDescription,
        ),
      ];
      const embeddings = await embeddingClient.embedTexts({
        texts: embeddingTexts,
        taskType: "SEMANTIC_SIMILARITY",
        outputDimensionality: 768,
      });
      const sourceVector = embeddings[0] ?? [];
      candidateMatches.forEach((match, index) => {
        const candidateVector = embeddings[index + 1] ?? [];
        semanticSimilarityByTransactionId.set(
          match.transaction.id,
          calculateCosineSimilarity(sourceVector, candidateVector),
        );
      });
    } catch {
      semanticSimilarityByTransactionId.clear();
    }
  }

  return candidateMatches
    .map((match) => {
      const semanticSimilarity =
        semanticSimilarityByTransactionId.get(match.transaction.id) ?? null;
      const passesThreshold =
        match.exactMatch ||
        (semanticSimilarity !== null &&
          (semanticSimilarity >= 0.97 ||
            (semanticSimilarity >= 0.9 &&
              (match.distinctiveOverlapCount >= 1 ||
                match.distinctiveJaccard >= 0.5)))) ||
        (semanticSimilarity === null &&
          (match.distinctiveOverlapCount >= 2 ||
            match.distinctiveJaccard >= 0.67));

      return {
        transaction: match.transaction,
        score:
          match.lexicalScore +
          (semanticSimilarity !== null ? semanticSimilarity * 100 : 0),
        lexicalScore: match.lexicalScore,
        semanticSimilarity,
        exactMatch: match.exactMatch,
        passesThreshold,
      };
    })
    .filter((match) => match.passesThreshold)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }
      return (
        new Date(right.transaction.createdAt).getTime() -
        new Date(left.transaction.createdAt).getTime()
      );
    })
    .slice(0, limit)
    .map(({ passesThreshold: _passesThreshold, ...match }) => match);
}

export async function rankReviewPropagationTransactions(
  dataset: DomainDataset,
  account: Account,
  transaction: Transaction,
  options: {
    limit?: number;
    embeddingClient?: TextEmbeddingClient | null;
  } = {},
): Promise<ReviewPropagationTransactionMatch[]> {
  const limit = options.limit ?? 25;

  if (account.assetDomain !== "investment") {
    return rankSimilarAccountTransactions(dataset, account, transaction, {
      includeNeedsReview: true,
      requireEarlierDate: false,
      limit,
      minScore: 6,
    })
      .filter((match) => match.transaction.needsReview)
      .map((match) => ({
        ...match,
        semanticSimilarity: null,
        lexicalScore: match.score,
        exactMatch: false,
      }));
  }

  const sourceEvidence = buildReviewPropagationEvidence(transaction);
  let embeddingClient = options.embeddingClient;
  if (embeddingClient === undefined && isTextEmbeddingConfigured()) {
    try {
      embeddingClient = createTextEmbeddingClient(
        getReviewPropagationEmbeddingModel(),
      );
    } catch {
      embeddingClient = null;
    }
  }
  const sourceMagnitude = Math.abs(Number(transaction.amountOriginal));
  const sourceSign = Math.sign(Number(transaction.amountOriginal));
  const candidateMatches = dataset.transactions
    .filter((candidate) => candidate.id !== transaction.id)
    .filter((candidate) => candidate.accountId === account.id)
    .filter((candidate) => candidate.needsReview)
    .filter((candidate) => !candidate.voidedAt)
    .filter(
      (candidate) =>
        candidate.currencyOriginal === transaction.currencyOriginal,
    )
    .filter((candidate) => {
      const candidateSign = Math.sign(Number(candidate.amountOriginal));
      return (
        candidateSign === 0 || sourceSign === 0 || candidateSign === sourceSign
      );
    })
    .filter((candidate) => {
      if (
        transaction.transactionClass === "unknown" ||
        candidate.transactionClass === "unknown"
      ) {
        return true;
      }
      return candidate.transactionClass === transaction.transactionClass;
    })
    .map((candidate) => {
      const candidateEvidence = buildReviewPropagationEvidence(candidate);
      if (
        sourceEvidence.exactIsin &&
        candidateEvidence.exactIsin &&
        sourceEvidence.exactIsin !== candidateEvidence.exactIsin
      ) {
        return null;
      }
      if (sourceEvidence.explicitMutualFund && candidateEvidence.explicitEtf) {
        return null;
      }
      if (sourceEvidence.explicitEtf && candidateEvidence.explicitMutualFund) {
        return null;
      }

      const commonTokenCount = countOverlappingTokens(
        sourceEvidence.allTokens,
        candidateEvidence.allTokens,
      );
      const distinctiveOverlapCount = countOverlappingTokens(
        sourceEvidence.distinctiveTokens,
        candidateEvidence.distinctiveTokens,
      );
      const distinctiveJaccard = calculateJaccardScore(
        sourceEvidence.distinctiveTokens,
        candidateEvidence.distinctiveTokens,
      );
      const exactDescriptionMatch =
        sourceEvidence.normalizedDescription !== null &&
        sourceEvidence.normalizedDescription ===
          candidateEvidence.normalizedDescription;
      const exactSecurityIdMatch =
        Boolean(transaction.securityId) &&
        transaction.securityId === candidate.securityId;
      const exactIsinMatch =
        Boolean(sourceEvidence.exactIsin) &&
        sourceEvidence.exactIsin === candidateEvidence.exactIsin;
      const candidateMagnitude = Math.abs(Number(candidate.amountOriginal));
      const amountRatio =
        sourceMagnitude > 0 && candidateMagnitude > 0
          ? Math.min(sourceMagnitude, candidateMagnitude) /
            Math.max(sourceMagnitude, candidateMagnitude)
          : 0;
      const lexicalScore =
        commonTokenCount * 2 +
        distinctiveOverlapCount * 10 +
        distinctiveJaccard * 20 +
        (amountRatio >= 0.95 ? 4 : amountRatio >= 0.7 ? 2 : 0) +
        (exactDescriptionMatch ? 40 : 0) +
        (exactSecurityIdMatch ? 50 : 0) +
        (exactIsinMatch ? 80 : 0);

      const exactMatch =
        exactIsinMatch || exactSecurityIdMatch || exactDescriptionMatch;

      return {
        transaction: candidate,
        candidateEvidence,
        lexicalScore,
        exactMatch,
        distinctiveOverlapCount,
        distinctiveJaccard,
      };
    })
    .filter(
      (
        match,
      ): match is {
        transaction: Transaction;
        candidateEvidence: ReturnType<typeof buildReviewPropagationEvidence>;
        lexicalScore: number;
        exactMatch: boolean;
        distinctiveOverlapCount: number;
        distinctiveJaccard: number;
      } => Boolean(match),
    );

  if (candidateMatches.length === 0) {
    return [];
  }

  return buildReviewPropagationTransactionMatches(
    sourceEvidence,
    candidateMatches,
    limit,
    embeddingClient,
  );
}

export { buildInvestmentPortfolioState, buildPersistedInvestmentSecurityMappings };
