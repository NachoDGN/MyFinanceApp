import { GoogleAuth } from "google-auth-library";
import { z } from "zod";

import {
  createTextEmbeddingClient,
  type TextEmbeddingClient,
} from "@myfinance/llm";
import {
  normalizeMatcherText,
  type Account,
  type DomainDataset,
  type PeriodSelection,
  type Scope,
  type Transaction,
} from "@myfinance/domain";

import { mapFromSql } from "./sql-json";
import { isIsoDateString, normalizeSqlDateValue } from "./sql-date";
import type { SqlClient } from "./sql-runtime";
import { serializeVector } from "./transaction-embedding-search";
import {
  fuseTransactionSearchResults,
  type FusedTransactionSearchHit,
  type TransactionKeywordCandidate,
  type TransactionRerankedCandidate,
  type TransactionSearchDirection,
  type TransactionSearchReviewState,
  type TransactionSemanticCandidate,
} from "./transaction-search-fusion";
import { transactionColumnsSql } from "./transaction-columns";

const auth = new GoogleAuth({
  scopes: ["https://www.googleapis.com/auth/cloud-platform"],
});

const TRANSACTION_SEARCH_SEMANTIC_LIMIT = 40;
const TRANSACTION_SEARCH_KEYWORD_LIMIT = 20;
const TRANSACTION_SEARCH_RESULT_LIMIT = 8;
const TRANSACTION_SEARCH_EMBEDDING_DIMENSIONS = 3072;

const transactionSearchQuerySchema = z.object({
  hasExplicitScopeConstraint: z.boolean().default(false),
  hasExplicitTimeConstraint: z.boolean().default(false),
  accountIds: z.array(z.string()).default([]),
  entityIds: z.array(z.string()).default([]),
  accountTypes: z
    .array(
      z.enum([
        "checking",
        "savings",
        "company_bank",
        "brokerage_cash",
        "brokerage_account",
        "credit_card",
        "other",
      ]),
    )
    .default([]),
  entityKinds: z.array(z.enum(["personal", "company"])).default([]),
  reviewStates: z
    .array(
      z.enum(["pending_enrichment", "needs_review", "resolved", "unresolved"]),
    )
    .default([]),
  directions: z.array(z.enum(["credit", "debit"])).default([]),
  dateStart: z.string().nullable().default(null),
  dateEnd: z.string().nullable().default(null),
  explanation: z.string().default(""),
});

export type ParsedTransactionSearchQuery = z.infer<
  typeof transactionSearchQuerySchema
>;

export type ResolvedTransactionSearchFilters = {
  accountIds: string[];
  entityIds: string[];
  accountTypes: Account["accountType"][];
  entityKinds: Array<"personal" | "company">;
  reviewStates: Array<
    "pending_enrichment" | "needs_review" | "resolved" | "unresolved"
  >;
  directions: Array<"credit" | "debit">;
  dateStart: string | null;
  dateEnd: string | null;
  usedScopeFallback: boolean;
  usedPeriodFallback: boolean;
  hasExplicitScopeConstraint: boolean;
  hasExplicitTimeConstraint: boolean;
  explanation: string;
};

export type TransactionSearchResultRow = {
  transaction: Transaction;
  originalText: string;
  contextualizedText: string;
  documentSummary: string;
  searchDiagnostics: {
    sourceBatchKey: string;
    hybridScore: number;
    semanticDistance: number | null;
    rerankScore: number | null;
    bm25Score: number | null;
    semanticRank: number | null;
    rerankRank: number | null;
    keywordRank: number | null;
    matchedBy: Array<"semantic" | "keyword">;
    direction: TransactionSearchDirection;
    reviewState: TransactionSearchReviewState;
  };
};

export type SearchTransactionsResult = {
  query: string;
  rows: TransactionSearchResultRow[];
  semanticCandidateCount: number;
  keywordCandidateCount: number;
  filters: ResolvedTransactionSearchFilters;
  warnings: string[];
};

type TransactionSearchCandidateRecord = {
  transactionId: string;
  batchId: string;
  sourceBatchKey: string;
  transactionDate: string;
  postedAt: string | null;
  amount: string | null;
  currency: string | null;
  merchant: string | null;
  counterparty: string | null;
  category: string | null;
  accountId: string;
  accountName: string | null;
  institutionName: string | null;
  accountType: Account["accountType"] | null;
  economicEntityId: string | null;
  economicEntityName: string | null;
  economicEntityKind: "personal" | "company" | null;
  direction: TransactionSearchDirection;
  reviewState: TransactionSearchReviewState;
  reviewReason: string | null;
  originalText: string;
  contextualizedText: string;
  documentSummary: string;
};

function uniq(values: readonly string[]) {
  return [...new Set(values.filter(Boolean))];
}

function tokenizeNormalizedMatcher(value: string) {
  return normalizeMatcherText(value)
    .split(/[^A-Z0-9_]+/)
    .map((token) => token.trim())
    .filter(
      (token) =>
        token.length >= 3 &&
        !TRANSACTION_SEARCH_QUERY_STOPWORDS.has(token),
    );
}

function tokenizeMatcherText(value: string | null | undefined) {
  if (!value) {
    return [];
  }

  return tokenizeNormalizedMatcher(value);
}

function startOfMonthIso(value: string) {
  return `${value.slice(0, 7)}-01`;
}

function endOfMonthIso(value: string) {
  const [yearText, monthText] = value.slice(0, 7).split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  const nextMonth = new Date(Date.UTC(year, month, 1));
  nextMonth.setUTCDate(0);
  return nextMonth.toISOString().slice(0, 10);
}

const MONTH_INDEX_BY_TOKEN = new Map<string, number>([
  ["JAN", 1],
  ["JANUARY", 1],
  ["FEB", 2],
  ["FEBRUARY", 2],
  ["MAR", 3],
  ["MARCH", 3],
  ["APR", 4],
  ["APRIL", 4],
  ["MAY", 5],
  ["JUN", 6],
  ["JUNE", 6],
  ["JUL", 7],
  ["JULY", 7],
  ["AUG", 8],
  ["AUGUST", 8],
  ["SEP", 9],
  ["SEPT", 9],
  ["SEPTEMBER", 9],
  ["OCT", 10],
  ["OCTOBER", 10],
  ["NOV", 11],
  ["NOVEMBER", 11],
  ["DEC", 12],
  ["DECEMBER", 12],
] as const);

const ACCOUNT_TYPE_KEYWORDS: Array<{
  accountType: Account["accountType"];
  patterns: RegExp[];
}> = [
  {
    accountType: "checking",
    patterns: [/\bCHECKING\b/, /\bCURRENT ACCOUNT\b/],
  },
  { accountType: "savings", patterns: [/\bSAVINGS\b/, /\bSAVING ACCOUNT\b/] },
  {
    accountType: "company_bank",
    patterns: [/\bCOMPANY BANK\b/, /\bBUSINESS BANK\b/, /\bBUSINESS ACCOUNT\b/],
  },
  {
    accountType: "brokerage_cash",
    patterns: [/\bBROKERAGE CASH\b/, /\bBROKER CASH\b/],
  },
  {
    accountType: "brokerage_account",
    patterns: [/\bBROKERAGE\b/, /\bBROKER ACCOUNT\b/, /\bINVESTMENT ACCOUNT\b/],
  },
  {
    accountType: "credit_card",
    patterns: [/\bCREDIT CARD\b/, /\bCARD\b/],
  },
];

const TRANSACTION_SEARCH_QUERY_STOPWORDS = new Set([
  "A",
  "AN",
  "AND",
  "AT",
  "BY",
  "FIND",
  "FOR",
  "FROM",
  "GET",
  "GIVE",
  "I",
  "IN",
  "ME",
  "MY",
  "OF",
  "ON",
  "PLEASE",
  "SEARCH",
  "SHOW",
  "THAT",
  "THE",
  "THESE",
  "THOSE",
  "TO",
  "TRANSACTION",
  "TRANSACTIONS",
  "WAS",
  "WERE",
  "WITH",
]);

const TRANSACTION_SEARCH_WEAK_EVIDENCE_TOKENS = new Set([
  "ACCOUNT",
  "ACCOUNTS",
  "APR",
  "APRIL",
  "AUG",
  "AUGUST",
  "BANK",
  "CARD",
  "CHECKING",
  "CREDIT",
  "CREDITS",
  "DATE",
  "DATES",
  "DEBIT",
  "DEBITS",
  "DEC",
  "DECEMBER",
  "FEB",
  "FEBRUARY",
  "INCOME",
  "INFLOW",
  "JAN",
  "JANUARY",
  "JUL",
  "JULY",
  "JUN",
  "JUNE",
  "MAR",
  "MARCH",
  "MAY",
  "MONTH",
  "NOV",
  "NOVEMBER",
  "OCT",
  "OCTOBER",
  "OUTFLOW",
  "PAID",
  "PAYMENT",
  "PAYMENTS",
  "PERIOD",
  "RECEIVED",
  "RESULT",
  "RESULTS",
  "ROW",
  "ROWS",
  "SAVINGS",
  "SHOW",
  "TODAY",
  "TRANSACTION",
  "TRANSACTIONS",
  "VIEW",
  "YEAR",
  "YEARS",
  "YESTERDAY",
]);

function shiftMonth(referenceDate: string, deltaMonths: number) {
  const date = new Date(`${referenceDate}T00:00:00Z`);
  date.setUTCMonth(date.getUTCMonth() + deltaMonths, 1);
  return date.toISOString().slice(0, 10);
}

function inferDateRangeFromQueryHeuristically(
  query: string,
  referenceDate: string,
) {
  const normalized = normalizeMatcherText(query);
  const exactDate = query.match(/\b(\d{4}-\d{2}-\d{2})\b/);
  if (exactDate?.[1] && isIsoDateString(exactDate[1])) {
    return {
      dateStart: exactDate[1],
      dateEnd: exactDate[1],
    };
  }

  const yearMonth = query.match(/\b(\d{4})-(\d{2})\b/);
  if (yearMonth?.[1] && yearMonth[2]) {
    const monthValue = `${yearMonth[1]}-${yearMonth[2]}-01`;
    if (isIsoDateString(monthValue)) {
      return {
        dateStart: startOfMonthIso(monthValue),
        dateEnd: endOfMonthIso(monthValue),
      };
    }
  }

  const monthMatch = normalized.match(
    /\b(JAN|JANUARY|FEB|FEBRUARY|MAR|MARCH|APR|APRIL|MAY|JUN|JUNE|JUL|JULY|AUG|AUGUST|SEP|SEPT|SEPTEMBER|OCT|OCTOBER|NOV|NOVEMBER|DEC|DECEMBER)\b(?:\s+(\d{4}))?/,
  );
  if (monthMatch?.[1]) {
    const monthIndex = MONTH_INDEX_BY_TOKEN.get(monthMatch[1]);
    const year = monthMatch[2]
      ? Number(monthMatch[2])
      : Number(referenceDate.slice(0, 4));
    if (monthIndex && Number.isFinite(year)) {
      const monthValue = `${year.toString().padStart(4, "0")}-${monthIndex
        .toString()
        .padStart(2, "0")}-01`;
      return {
        dateStart: startOfMonthIso(monthValue),
        dateEnd: endOfMonthIso(monthValue),
      };
    }
  }

  if (/\bTHIS MONTH\b/.test(normalized)) {
    return {
      dateStart: startOfMonthIso(referenceDate),
      dateEnd: referenceDate,
    };
  }

  if (/\bTODAY\b/.test(normalized)) {
    return {
      dateStart: referenceDate,
      dateEnd: referenceDate,
    };
  }

  if (/\bYESTERDAY\b/.test(normalized)) {
    const date = new Date(`${referenceDate}T00:00:00Z`);
    date.setUTCDate(date.getUTCDate() - 1);
    const yesterday = date.toISOString().slice(0, 10);
    return {
      dateStart: yesterday,
      dateEnd: yesterday,
    };
  }

  if (/\bLAST MONTH\b/.test(normalized)) {
    const lastMonthReference = shiftMonth(referenceDate, -1);
    return {
      dateStart: startOfMonthIso(lastMonthReference),
      dateEnd: endOfMonthIso(lastMonthReference),
    };
  }

  if (/\bTHIS YEAR\b/.test(normalized)) {
    return {
      dateStart: `${referenceDate.slice(0, 4)}-01-01`,
      dateEnd: referenceDate,
    };
  }

  if (/\bLAST YEAR\b/.test(normalized)) {
    const previousYear = `${Number(referenceDate.slice(0, 4)) - 1}`;
    return {
      dateStart: `${previousYear}-01-01`,
      dateEnd: `${previousYear}-12-31`,
    };
  }

  return {
    dateStart: null,
    dateEnd: null,
  };
}

function getTransactionSearchEmbeddingModel() {
  return (
    process.env.TRANSACTION_SEARCH_EMBEDDING_MODEL?.trim() ||
    "gemini-embedding-2-preview"
  );
}

function toCandidateBase(
  row: Record<string, unknown>,
): TransactionSearchCandidateRecord {
  return {
    transactionId: String(row.transaction_id ?? ""),
    batchId: String(row.batch_id ?? ""),
    sourceBatchKey: String(row.source_batch_key ?? ""),
    transactionDate: normalizeSqlDateValue(row.transaction_date) ?? "",
    postedAt: normalizeSqlDateValue(row.posted_at),
    amount:
      row.amount === null || row.amount === undefined
        ? null
        : String(row.amount),
    currency: typeof row.currency === "string" ? row.currency : null,
    merchant: typeof row.merchant === "string" ? row.merchant : null,
    counterparty:
      typeof row.counterparty === "string" ? row.counterparty : null,
    category: typeof row.category === "string" ? row.category : null,
    accountId: String(row.account_id ?? ""),
    accountName: typeof row.account_name === "string" ? row.account_name : null,
    institutionName:
      typeof row.institution_name === "string" ? row.institution_name : null,
    accountType:
      typeof row.account_type === "string"
        ? (row.account_type as Account["accountType"])
        : null,
    economicEntityId:
      typeof row.economic_entity_id === "string"
        ? row.economic_entity_id
        : null,
    economicEntityName:
      typeof row.economic_entity_name === "string"
        ? row.economic_entity_name
        : null,
    economicEntityKind:
      row.economic_entity_kind === "personal" ||
      row.economic_entity_kind === "company"
        ? (row.economic_entity_kind as "personal" | "company")
        : null,
    direction:
      row.direction === "debit" ||
      row.direction === "credit" ||
      row.direction === "neutral"
        ? (row.direction as TransactionSearchDirection)
        : "neutral",
    reviewState:
      row.review_state === "pending_enrichment" ||
      row.review_state === "needs_review" ||
      row.review_state === "resolved"
        ? (row.review_state as TransactionSearchReviewState)
        : "resolved",
    reviewReason:
      typeof row.review_reason === "string" ? row.review_reason : null,
    originalText: String(row.original_text ?? ""),
    contextualizedText: String(row.contextualized_text ?? ""),
    documentSummary: String(row.document_summary ?? ""),
  };
}

export function buildDeterministicTransactionSearchQuery(input: {
  query: string;
  dataset: DomainDataset;
  referenceDate: string;
}) {
  const normalizedQuery = normalizeMatcherText(input.query);
  const queryTokens = tokenizeNormalizedMatcher(input.query);
  const queryHasAccountContext =
    /\bACCOUNT\b|\bBANK\b|\bCARD\b|\bBROKER(?:AGE)?\b|\bIBKR\b/.test(
      normalizedQuery,
    );

  function queryMentionsScopeCandidate(
    candidateValue: string | null | undefined,
    options: { requireAccountContext?: boolean } = {},
  ) {
    if (!candidateValue) {
      return false;
    }

    const candidateTokens = tokenizeNormalizedMatcher(candidateValue);
    if (candidateTokens.length === 0) {
      return false;
    }

    if (candidateTokens.length === 1) {
      const [token] = candidateTokens;
      if (!queryTokens.includes(token)) {
        return false;
      }

      if (options.requireAccountContext) {
        return queryHasAccountContext;
      }

      return queryTokens.length === 1 || normalizedQuery === token;
    }

    return candidateTokens.every((token) => queryTokens.includes(token));
  }

  const accountIds = input.dataset.accounts
    .filter((account) => {
      const candidates = [
        account.displayName,
        account.institutionName,
        account.accountSuffix,
        ...account.matchingAliases,
      ];
      return candidates.some((candidate) =>
        queryMentionsScopeCandidate(candidate, { requireAccountContext: true }),
      );
    })
    .map((account) => account.id);
  const entityIds = input.dataset.entities
    .filter((entity) => {
      const candidates = [entity.displayName, entity.legalName, entity.slug];
      return candidates.some((candidate) => queryMentionsScopeCandidate(candidate));
    })
    .map((entity) => entity.id);

  const accountTypes = ACCOUNT_TYPE_KEYWORDS.filter(({ patterns }) =>
    patterns.some((pattern) => pattern.test(normalizedQuery)),
  ).map(({ accountType }) => accountType);
  const entityKinds = ["personal", "company"].filter((entityKind) =>
    normalizedQuery.includes(normalizeMatcherText(entityKind)),
  ) as Array<"personal" | "company">;

  const reviewStates = [
    normalizedQuery.includes("UNRESOLVED") ? "unresolved" : null,
    normalizedQuery.includes("NEEDS REVIEW") ? "needs_review" : null,
    normalizedQuery.includes("PENDING") ? "pending_enrichment" : null,
    normalizedQuery.includes("RESOLVED") ? "resolved" : null,
  ].filter(
    (
      state,
    ): state is
      | "pending_enrichment"
      | "needs_review"
      | "resolved"
      | "unresolved" => Boolean(state),
  );

  const directions = [
    /\bRECEIVED\b|\bINCOME\b|\bCREDIT\b|\bINFLOW\b/.test(normalizedQuery)
      ? "credit"
      : null,
    /\bPAID\b|\bSPENT\b|\bDEBIT\b|\bOUTFLOW\b/.test(normalizedQuery)
      ? "debit"
      : null,
  ].filter((direction): direction is "credit" | "debit" => Boolean(direction));

  const inferredDateRange = inferDateRangeFromQueryHeuristically(
    input.query,
    input.referenceDate,
  );
  const hasExplicitTimeConstraint =
    Boolean(inferredDateRange.dateStart && inferredDateRange.dateEnd) ||
    /\bTODAY\b|\bYESTERDAY\b|\bQ[1-4]\b/.test(normalizedQuery);

  return {
    hasExplicitScopeConstraint:
      accountIds.length > 0 ||
      entityIds.length > 0 ||
      accountTypes.length > 0 ||
      entityKinds.length > 0,
    hasExplicitTimeConstraint,
    accountIds,
    entityIds,
    accountTypes: accountTypes as Account["accountType"][],
    entityKinds,
    reviewStates,
    directions,
    dateStart: inferredDateRange.dateStart,
    dateEnd: inferredDateRange.dateEnd,
    explanation: "deterministic_parser",
  } satisfies ParsedTransactionSearchQuery;
}

function understandTransactionSearchQuery(input: {
  dataset: DomainDataset;
  query: string;
  referenceDate: string;
}): ParsedTransactionSearchQuery {
  return buildDeterministicTransactionSearchQuery(input);
}

export function resolveTransactionSearchFilters(input: {
  parsedQuery: ParsedTransactionSearchQuery;
  scope: Scope;
  period: PeriodSelection;
  applySelectorFallback?: boolean;
}) {
  const filters: ResolvedTransactionSearchFilters = {
    accountIds: uniq(input.parsedQuery.accountIds),
    entityIds: uniq(input.parsedQuery.entityIds),
    accountTypes: [...input.parsedQuery.accountTypes],
    entityKinds: [...input.parsedQuery.entityKinds],
    reviewStates: [...input.parsedQuery.reviewStates],
    directions: [...input.parsedQuery.directions],
    dateStart: isIsoDateString(input.parsedQuery.dateStart)
      ? input.parsedQuery.dateStart
      : null,
    dateEnd: isIsoDateString(input.parsedQuery.dateEnd)
      ? input.parsedQuery.dateEnd
      : null,
    usedScopeFallback: false,
    usedPeriodFallback: false,
    hasExplicitScopeConstraint: input.parsedQuery.hasExplicitScopeConstraint,
    hasExplicitTimeConstraint: input.parsedQuery.hasExplicitTimeConstraint,
    explanation: input.parsedQuery.explanation,
  };

  const applySelectorFallback = input.applySelectorFallback !== false;

  if (applySelectorFallback && !filters.hasExplicitScopeConstraint) {
    if (input.scope.kind === "account" && input.scope.accountId) {
      filters.accountIds = uniq([...filters.accountIds, input.scope.accountId]);
      filters.usedScopeFallback = true;
    } else if (input.scope.kind === "entity" && input.scope.entityId) {
      filters.entityIds = uniq([...filters.entityIds, input.scope.entityId]);
      filters.usedScopeFallback = true;
    }
  }

  if (applySelectorFallback && !filters.hasExplicitTimeConstraint) {
    filters.dateStart = input.period.start;
    filters.dateEnd = input.period.end;
    filters.usedPeriodFallback = true;
  }

  return filters;
}

async function embedTransactionSearchQuery(
  query: string,
  embeddingClient: TextEmbeddingClient | undefined = createTextEmbeddingClient(
    getTransactionSearchEmbeddingModel(),
  ),
) {
  const [queryEmbedding] = await embeddingClient.embedTexts({
    texts: [query],
    taskType: "RETRIEVAL_QUERY",
    outputDimensionality: TRANSACTION_SEARCH_EMBEDDING_DIMENSIONS,
  });

  if (!queryEmbedding || queryEmbedding.length === 0) {
    throw new Error("Transaction search query embedding failed.");
  }

  return queryEmbedding;
}

async function getSemanticCandidates(
  sql: SqlClient,
  userId: string,
  embedding: number[],
  filters: ResolvedTransactionSearchFilters,
) {
  const rows = await sql`
    select *
    from public.search_semantic_transactions(
      ${userId}::uuid,
      ${serializeVector(embedding)}::extensions.vector(3072),
      ${TRANSACTION_SEARCH_SEMANTIC_LIMIT},
      ${filters.accountIds.length > 0 ? filters.accountIds : null}::uuid[],
      ${filters.entityIds.length > 0 ? filters.entityIds : null}::uuid[],
      ${filters.accountTypes.length > 0 ? filters.accountTypes : null}::public.account_type[],
      ${filters.entityKinds.length > 0 ? filters.entityKinds : null}::public.entity_kind[],
      ${filters.reviewStates.length > 0 ? filters.reviewStates : null}::text[],
      ${filters.directions.length > 0 ? filters.directions : null}::text[],
      ${filters.dateStart},
      ${filters.dateEnd}
    )
  `;

  return rows.map((row) => {
    const candidate = toCandidateBase(row as Record<string, unknown>);
    return {
      ...candidate,
      semanticDistance: Number(
        (row as Record<string, unknown>).semantic_distance ?? 0,
      ),
    } satisfies TransactionSemanticCandidate;
  });
}

async function getKeywordCandidates(
  sql: SqlClient,
  userId: string,
  query: string,
  filters: ResolvedTransactionSearchFilters,
) {
  const rows = await sql`
    select *
    from public.search_keyword_transactions(
      ${userId}::uuid,
      ${query},
      ${TRANSACTION_SEARCH_KEYWORD_LIMIT},
      ${filters.accountIds.length > 0 ? filters.accountIds : null}::uuid[],
      ${filters.entityIds.length > 0 ? filters.entityIds : null}::uuid[],
      ${filters.accountTypes.length > 0 ? filters.accountTypes : null}::public.account_type[],
      ${filters.entityKinds.length > 0 ? filters.entityKinds : null}::public.entity_kind[],
      ${filters.reviewStates.length > 0 ? filters.reviewStates : null}::text[],
      ${filters.directions.length > 0 ? filters.directions : null}::text[],
      ${filters.dateStart},
      ${filters.dateEnd}
    )
  `;

  return rows.map((row) => {
    const candidate = toCandidateBase(row as Record<string, unknown>);
    return {
      ...candidate,
      bm25Score: Number((row as Record<string, unknown>).bm25_score ?? 0),
    } satisfies TransactionKeywordCandidate;
  });
}

async function rerankTransactionSemanticCandidates(input: {
  query: string;
  candidates: TransactionSemanticCandidate[];
}) {
  if (input.candidates.length === 0) {
    return [] satisfies TransactionRerankedCandidate[];
  }

  const googleCloudProject = process.env.GOOGLE_CLOUD_PROJECT?.trim();
  const googleCredentials = process.env.GOOGLE_APPLICATION_CREDENTIALS?.trim();

  if (!googleCloudProject || !googleCredentials) {
    return input.candidates.map((candidate, index) => ({
      transactionId: candidate.transactionId,
      score: 1 / (index + 1),
    }));
  }

  const token = await auth.getAccessToken();
  if (!token) {
    throw new Error(
      "Unable to obtain a Google Cloud access token for reranking.",
    );
  }

  const googleCloudLocation =
    process.env.GOOGLE_CLOUD_LOCATION?.trim() || "global";
  const rankingConfig =
    process.env.VERTEX_RANKING_CONFIG?.trim() || "default_ranking_config";
  const response = await fetch(
    `https://discoveryengine.googleapis.com/v1beta/projects/${googleCloudProject}/locations/${googleCloudLocation}/rankingConfigs/${rankingConfig}:rank`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "semantic-ranker-512@latest",
        topN: input.candidates.length,
        query: input.query,
        records: input.candidates.map((candidate) => ({
          id: candidate.transactionId,
          title:
            [
              candidate.institutionName,
              candidate.accountName,
              candidate.merchant,
              candidate.category,
            ]
              .filter((value): value is string => Boolean(value))
              .join(" | ") || `Transaction ${candidate.transactionDate}`,
          content: candidate.contextualizedText.slice(0, 3000),
        })),
        ignoreRecordDetailsInResponse: true,
      }),
      cache: "no-store",
    },
  );

  if (!response.ok) {
    throw new Error(
      `Vertex reranking failed: ${response.status} ${await response.text()}`,
    );
  }

  const payload = (await response.json()) as {
    records?: Array<{ id?: string; score?: number }>;
  };

  const records = (payload.records ?? [])
    .filter(
      (record): record is { id: string; score: number } =>
        Boolean(record.id) && typeof record.score === "number",
    )
    .map((record) => ({
      transactionId: record.id,
      score: record.score,
    }));

  return records.length > 0
    ? records
    : input.candidates.map((candidate, index) => ({
        transactionId: candidate.transactionId,
        score: 1 / (index + 1),
      }));
}

function buildFallbackSemanticReranking(
  candidates: TransactionSemanticCandidate[],
) {
  return candidates.map((candidate, index) => ({
    transactionId: candidate.transactionId,
    score: 1 / (index + 1),
  })) satisfies TransactionRerankedCandidate[];
}

function extractTransactionSearchEvidenceTokens(query: string) {
  return uniq(
    normalizeMatcherText(query)
      .split(/[^A-Z0-9_]+/)
      .map((token) => token.trim())
      .filter(
        (token) =>
          token.length >= 3 &&
          !TRANSACTION_SEARCH_QUERY_STOPWORDS.has(token),
      ),
  );
}

function isWeakTransactionSearchEvidenceToken(token: string) {
  return (
    TRANSACTION_SEARCH_WEAK_EVIDENCE_TOKENS.has(token) ||
    /^\d{4}$/.test(token) ||
    /^\d{1,2}$/.test(token)
  );
}

function collectStructuredConstraintTokens(input: {
  dataset: DomainDataset;
  parsedQuery: ParsedTransactionSearchQuery;
}) {
  const accountTokenSources = input.dataset.accounts
    .filter((account) => input.parsedQuery.accountIds.includes(account.id))
    .flatMap((account) => [
      account.displayName,
      account.institutionName,
      account.accountSuffix,
      ...account.matchingAliases,
    ]);
  const entityTokenSources = input.dataset.entities
    .filter((entity) => input.parsedQuery.entityIds.includes(entity.id))
    .flatMap((entity) => [entity.displayName, entity.legalName, entity.slug]);

  return new Set(
    uniq([
      ...accountTokenSources.flatMap((value) => tokenizeMatcherText(value)),
      ...entityTokenSources.flatMap((value) => tokenizeMatcherText(value)),
      ...input.parsedQuery.accountTypes.flatMap((value) =>
        tokenizeMatcherText(value),
      ),
      ...input.parsedQuery.entityKinds.flatMap((value) =>
        tokenizeMatcherText(value),
      ),
      ...input.parsedQuery.reviewStates.flatMap((value) =>
        tokenizeMatcherText(value),
      ),
      ...input.parsedQuery.directions.flatMap((value) =>
        tokenizeMatcherText(value),
      ),
      ...tokenizeMatcherText(input.parsedQuery.dateStart),
      ...tokenizeMatcherText(input.parsedQuery.dateEnd),
    ]),
  );
}

export function extractDistinctiveTransactionSearchEvidenceTokens(input: {
  query: string;
  dataset: DomainDataset;
  parsedQuery: ParsedTransactionSearchQuery;
}) {
  const structuredConstraintTokens = collectStructuredConstraintTokens(input);

  return extractTransactionSearchEvidenceTokens(input.query).filter(
    (token) =>
      !isWeakTransactionSearchEvidenceToken(token) &&
      !structuredConstraintTokens.has(token),
  );
}

function getRequiredEvidenceMatches(queryTokens: string[]) {
  return queryTokens.length >= 4 ? 2 : 1;
}

function countEvidenceMatches(queryTokens: string[], haystack: string) {
  let count = 0;
  for (const token of queryTokens) {
    if (haystack.includes(token)) {
      count += 1;
    }
  }
  return count;
}

function buildSemanticCandidateEvidenceText(
  candidate: TransactionSemanticCandidate,
) {
  return normalizeMatcherText(
    [
      candidate.contextualizedText,
      candidate.originalText,
      candidate.merchant,
      candidate.counterparty,
      candidate.category,
      candidate.accountName,
      candidate.institutionName,
      candidate.economicEntityName,
    ]
      .filter((value): value is string => Boolean(value))
      .join(" "),
  );
}

export function filterSemanticCandidatesByEvidence(input: {
  query: string;
  semanticCandidates: TransactionSemanticCandidate[];
  keywordCandidates?: TransactionKeywordCandidate[];
  distinctiveTokens?: string[];
}) {
  const queryTokens =
    input.distinctiveTokens ?? extractTransactionSearchEvidenceTokens(input.query);
  if (queryTokens.length === 0) {
    return input.semanticCandidates;
  }

  const requiredMatches = getRequiredEvidenceMatches(queryTokens);
  const keywordIds = new Set(
    (input.keywordCandidates ?? []).map((candidate) => candidate.transactionId),
  );

  return input.semanticCandidates.filter((candidate) => {
    if (keywordIds.has(candidate.transactionId)) {
      return true;
    }

    const evidenceText = buildSemanticCandidateEvidenceText(candidate);
    return countEvidenceMatches(queryTokens, evidenceText) >= requiredMatches;
  });
}

async function selectTransactionsByIds(
  sql: SqlClient,
  userId: string,
  transactionIds: string[],
) {
  if (transactionIds.length === 0) {
    return new Map<string, Transaction>();
  }

  const rows = await sql`
    select ${transactionColumnsSql(sql)}
    from public.transactions
    where user_id = ${userId}
      and id in ${sql(transactionIds)}
  `;

  return new Map(
    rows
      .map((row) => mapFromSql<Transaction>(row))
      .map((transaction) => [transaction.id, transaction] as const),
  );
}

async function collectTransactionSearchIndexWarnings(
  sql: SqlClient,
  userId: string,
) {
  const warnings: string[] = [];

  const [processingStatusRows, failedStatusRows, missingRowStatusRows] =
    await Promise.all([
      sql`
        select count(*)::int as count
        from public.transaction_search_batches
        where user_id = ${userId}
          and status = 'processing'
      `,
      sql`
        select count(*)::int as count
        from public.transaction_search_batches
        where user_id = ${userId}
          and status = 'failed'
      `,
      sql`
        select exists (
          select 1
          from public.transactions as t
          where t.user_id = ${userId}
            and not exists (
              select 1
              from public.transaction_search_rows as r
              where r.transaction_id = t.id
            )
        ) as has_missing_rows
      `,
    ]);

  if ((processingStatusRows[0]?.count ?? 0) > 0) {
    warnings.push(
      "The finder is still indexing recent imports, so some transactions may not appear yet.",
    );
  } else if (missingRowStatusRows[0]?.has_missing_rows === true) {
    warnings.push(
      "Some transactions are not indexed for search yet, so results may be incomplete.",
    );
  }

  if ((failedStatusRows[0]?.count ?? 0) > 0) {
    warnings.push(
      "Some search batches failed to index and need to be retried before results are complete.",
    );
  }

  return warnings;
}

async function runTransactionSearchRetrieval(input: {
  sql: SqlClient;
  userId: string;
  query: string;
  filters: ResolvedTransactionSearchFilters;
  distinctiveTokens?: string[];
  warnings: Set<string>;
}) {
  let keywordCandidates: TransactionKeywordCandidate[] = [];
  try {
    keywordCandidates = await getKeywordCandidates(
      input.sql,
      input.userId,
      input.query,
      input.filters,
    );
  } catch {
    input.warnings.add(
      "Keyword retrieval is temporarily unavailable, so only semantic matches can be shown.",
    );
  }

  let semanticCandidates: TransactionSemanticCandidate[] = [];
  let rerankedSemantic: TransactionRerankedCandidate[] = [];
  try {
    const queryEmbedding = await embedTransactionSearchQuery(input.query);
    const rawSemanticCandidates = await getSemanticCandidates(
      input.sql,
      input.userId,
      queryEmbedding,
      input.filters,
    );
    semanticCandidates = filterSemanticCandidatesByEvidence({
      query: input.query,
      semanticCandidates: rawSemanticCandidates,
      keywordCandidates,
      distinctiveTokens: input.distinctiveTokens,
    });
  } catch {
    input.warnings.add(
      "Semantic retrieval is temporarily unavailable, so the finder is using keyword matches only.",
    );
    semanticCandidates = [];
  }

  if (semanticCandidates.length > 0) {
    try {
      rerankedSemantic = await rerankTransactionSemanticCandidates({
        query: input.query,
        candidates: semanticCandidates,
      });
    } catch {
      rerankedSemantic = buildFallbackSemanticReranking(semanticCandidates);
      input.warnings.add(
        "Semantic reranking is temporarily unavailable, so semantic matches are using raw vector order.",
      );
    }
  }

  return {
    keywordCandidates,
    semanticCandidates,
    rerankedSemantic,
    fusedHits: fuseTransactionSearchResults({
      semanticCandidates,
      rerankedSemantic,
      keywordCandidates,
      limit: TRANSACTION_SEARCH_RESULT_LIMIT,
    }),
  };
}

function mapFusedHitsToRows(
  fusedHits: FusedTransactionSearchHit[],
  transactionsById: Map<string, Transaction>,
) {
  return fusedHits.flatMap((hit) => {
    const transaction = transactionsById.get(hit.transactionId);
    if (!transaction) {
      return [];
    }

    return [
      {
        transaction,
        originalText: hit.originalText,
        contextualizedText: hit.contextualizedText,
        documentSummary: hit.documentSummary,
        searchDiagnostics: {
          sourceBatchKey: hit.sourceBatchKey,
          hybridScore: hit.hybridScore,
          semanticDistance: hit.semanticDistance,
          rerankScore: hit.rerankScore,
          bm25Score: hit.bm25Score,
          semanticRank: hit.semanticRank,
          rerankRank: hit.rerankRank,
          keywordRank: hit.keywordRank,
          matchedBy: hit.matchedBy,
          direction: hit.direction,
          reviewState: hit.reviewState,
        },
      } satisfies TransactionSearchResultRow,
    ];
  });
}

export async function searchTransactions(
  sql: SqlClient,
  userId: string,
  input: {
    dataset: DomainDataset;
    scope: Scope;
    period: PeriodSelection;
    referenceDate: string;
    query: string;
  },
): Promise<SearchTransactionsResult> {
  const query = input.query.trim();
  if (!query) {
    throw new Error("Transaction search query is required.");
  }

  const warnings = new Set(
    await collectTransactionSearchIndexWarnings(sql, userId),
  );
  const parsedQuery = understandTransactionSearchQuery({
    dataset: input.dataset,
    query,
    referenceDate: input.referenceDate,
  });
  const distinctiveEvidenceTokens =
    extractDistinctiveTransactionSearchEvidenceTokens({
      query,
      dataset: input.dataset,
      parsedQuery,
    });
  let filters = resolveTransactionSearchFilters({
    parsedQuery,
    scope: input.scope,
    period: input.period,
    applySelectorFallback: false,
  });
  let retrieval = await runTransactionSearchRetrieval({
    sql,
    userId,
    query,
    filters,
    distinctiveTokens: distinctiveEvidenceTokens,
    warnings,
  });

  if (
    retrieval.fusedHits.length === 0 &&
    !parsedQuery.hasExplicitTimeConstraint &&
    filters.usedPeriodFallback
  ) {
    const broadenedFilters: ResolvedTransactionSearchFilters = {
      ...filters,
      dateStart: null,
      dateEnd: null,
      usedPeriodFallback: false,
    };
    const broadenedRetrieval = await runTransactionSearchRetrieval({
      sql,
      userId,
      query,
      filters: broadenedFilters,
      distinctiveTokens: distinctiveEvidenceTokens,
      warnings,
    });
    if (broadenedRetrieval.fusedHits.length > 0) {
      warnings.add(
        "No matches fell inside the current page period, so the finder broadened to all indexed dates.",
      );
      filters = broadenedFilters;
      retrieval = broadenedRetrieval;
    }
  }

  const transactionsById = await selectTransactionsByIds(
    sql,
    userId,
    retrieval.fusedHits.map((hit) => hit.transactionId),
  );

  return {
    query,
    rows: mapFusedHitsToRows(retrieval.fusedHits, transactionsById),
    semanticCandidateCount: retrieval.semanticCandidates.length,
    keywordCandidateCount: retrieval.keywordCandidates.length,
    filters,
    warnings: [...warnings],
  };
}
