import { Decimal } from "decimal.js";
import { z } from "zod";

import {
  createLLMClient,
  createTextEmbeddingClient,
  ProviderApiError,
  type TextEmbeddingClient,
} from "@myfinance/llm";
import {
  getTransactionReviewState,
  type AccountType,
  type EntityKind,
} from "@myfinance/domain";

import { queueJob, supportsJobType } from "./job-state";
import { isIsoDateString, normalizeSqlDateValue } from "./sql-date";
import { serializeJson } from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { serializeVector } from "./transaction-embedding-search";

const TRANSACTION_SEARCH_EMBEDDING_DIMENSIONS = 3072;
const TRANSACTION_SEARCH_EMBEDDING_BATCH_SIZE = 8;
const MAX_TRANSACTION_SEARCH_CONTEXTUALIZATION_CONCURRENCY = 200;
const DEFAULT_TRANSACTION_SEARCH_CONTEXTUALIZATION_CONCURRENCY_CAP =
  MAX_TRANSACTION_SEARCH_CONTEXTUALIZATION_CONCURRENCY;
const TRANSACTION_SEARCH_SUMMARY_WINDOW_CHAR_LIMIT = 18_000;
const EMPTY_UUID = "00000000-0000-0000-0000-000000000000";
const TRANSACTION_SEARCH_EMBEDDING_ZERO_VECTOR = Array.from(
  { length: TRANSACTION_SEARCH_EMBEDDING_DIMENSIONS },
  () => 0,
);

const transactionBatchSummarySchema = z.object({
  summary: z.string().min(1),
});

const transactionContextualizationSchema = z.object({
  contextualNote: z.string().min(1),
});

type TransactionSearchSourceRow = {
  transactionId: string;
  userId: string;
  accountId: string;
  economicEntityId: string | null;
  importBatchId: string | null;
  transactionDate: string;
  postedDate: string | null;
  amountOriginal: string;
  currencyOriginal: string;
  descriptionRaw: string;
  descriptionClean: string;
  merchantNormalized: string | null;
  counterpartyName: string | null;
  categoryCode: string | null;
  transactionClass: string;
  needsReview: boolean;
  reviewReason: string | null;
  llmPayload: Record<string, unknown> | null;
  creditCardStatementStatus: "not_applicable" | "upload_required" | "uploaded";
  accountName: string;
  institutionName: string;
  accountType: AccountType | null;
  economicEntityName: string | null;
  economicEntityKind: EntityKind | null;
};

type TransactionSearchBatchRow = {
  id: string;
  sourceBatchKey: string;
  accountId: string | null;
  accountName: string | null;
  institutionName: string | null;
  periodStart: string | null;
  periodEnd: string | null;
  status: string;
};

type TransactionSearchBatchGroup = {
  sourceBatchKey: string;
  accountId: string | null;
  accountName: string | null;
  institutionName: string | null;
  periodStart: string | null;
  periodEnd: string | null;
  rows: TransactionSearchSourceRow[];
};

type TransactionSearchContextualizedRow = {
  reviewState: string;
  direction: "debit" | "credit" | "neutral";
  contextualNote: string;
  contextualizedText: string;
  contextualizationModel: string;
  contextualizationPayload: Record<string, unknown>;
};

export type QueueTransactionSearchIndexInput = {
  transactionIds?: string[];
  importBatchIds?: string[];
  accountIds?: string[];
  entityIds?: string[];
  trigger: string;
};

export type SyncTransactionSearchIndexInput = {
  transactionIds?: string[];
  importBatchIds?: string[];
  accountIds?: string[];
  entityIds?: string[];
  onlyStaleOrMissing?: boolean;
};

type QueueTransactionSearchIndexPayload = {
  transactionIds?: string[];
  importBatchIds?: string[];
  accountIds?: string[];
  entityIds?: string[];
  trigger?: string;
};

function uniq(values: readonly string[] | undefined) {
  return [...new Set((values ?? []).filter(Boolean))];
}

function readQueuedIdList(
  payloadJson: Record<string, unknown>,
  keys: readonly string[],
) {
  const values: string[] = [];

  for (const key of keys) {
    const value = payloadJson[key];
    if (typeof value === "string" && value.trim() !== "") {
      values.push(value.trim());
      continue;
    }

    if (Array.isArray(value)) {
      values.push(
        ...value.filter(
          (entry): entry is string =>
            typeof entry === "string" && entry.trim() !== "",
        ),
      );
    }
  }

  return uniq(values);
}

export function normalizeTransactionSearchIndexJobPayload(
  payloadJson: Record<string, unknown>,
) {
  return {
    transactionIds: readQueuedIdList(payloadJson, [
      "transactionIds",
      "transactionId",
    ]),
    importBatchIds: readQueuedIdList(payloadJson, [
      "importBatchIds",
      "importBatchId",
    ]),
    accountIds: readQueuedIdList(payloadJson, ["accountIds", "accountId"]),
    entityIds: readQueuedIdList(payloadJson, ["entityIds", "entityId"]),
    trigger:
      typeof payloadJson.trigger === "string" ? payloadJson.trigger : "unknown",
  } satisfies Required<QueueTransactionSearchIndexPayload>;
}

export function getTransactionSearchContextualizationConcurrency(
  rowCount: number,
) {
  const normalizedCount = Number.isFinite(rowCount)
    ? Math.max(1, Math.floor(rowCount))
    : 1;
  const configuredCap = Number(
    process.env.TRANSACTION_SEARCH_CONTEXTUALIZATION_CONCURRENCY ??
      `${DEFAULT_TRANSACTION_SEARCH_CONTEXTUALIZATION_CONCURRENCY_CAP}`,
  );
  const normalizedCap =
    Number.isFinite(configuredCap) && configuredCap > 0
      ? Math.max(1, Math.floor(configuredCap))
      : DEFAULT_TRANSACTION_SEARCH_CONTEXTUALIZATION_CONCURRENCY_CAP;
  return Math.min(
    normalizedCount,
    normalizedCap,
    MAX_TRANSACTION_SEARCH_CONTEXTUALIZATION_CONCURRENCY,
  );
}

function startOfMonthIso(value: string) {
  const normalized = normalizeSqlDateValue(value);
  return normalized ? `${normalized.slice(0, 7)}-01` : null;
}

function endOfMonthIso(value: string) {
  const normalized = normalizeSqlDateValue(value);
  if (!normalized) {
    return null;
  }

  const [yearText, monthText] = normalized.slice(0, 7).split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  const nextMonth = new Date(Date.UTC(year, month, 1));
  nextMonth.setUTCDate(0);
  return nextMonth.toISOString().slice(0, 10);
}

function formatDateLong(value: string | null | undefined) {
  if (!isIsoDateString(value)) {
    return "unknown date";
  }

  try {
    return new Intl.DateTimeFormat("en-US", {
      month: "long",
      day: "numeric",
      year: "numeric",
      timeZone: "UTC",
    }).format(new Date(`${value}T00:00:00Z`));
  } catch {
    return value;
  }
}

function formatSignedAmount(amount: string, currency: string) {
  const decimalAmount = new Decimal(amount || "0");
  const absolute = decimalAmount.abs().toFixed(2);
  const sign = decimalAmount.greaterThan(0)
    ? "+"
    : decimalAmount.lessThan(0)
      ? "-"
      : "";
  return `${sign}${absolute} ${currency}`;
}

function getDirection(amount: string): "debit" | "credit" | "neutral" {
  const decimalAmount = new Decimal(amount || "0");
  if (decimalAmount.greaterThan(0)) {
    return "credit";
  }
  if (decimalAmount.lessThan(0)) {
    return "debit";
  }
  return "neutral";
}

function getTransactionSearchGenerativeModel() {
  return (
    process.env.TRANSACTION_SEARCH_GENERATION_MODEL?.trim() ||
    process.env.TRANSACTION_SEARCH_CONTEXTUALIZATION_MODEL?.trim() ||
    "gemini-2.5-flash"
  );
}

function getTransactionSearchEmbeddingModel() {
  return (
    process.env.TRANSACTION_SEARCH_EMBEDDING_MODEL?.trim() ||
    "gemini-embedding-2-preview"
  );
}

function buildTransactionSearchBatchKey(row: {
  importBatchId: string | null;
  accountId: string;
  postedDate: string | null;
  transactionDate: string;
}) {
  if (row.importBatchId) {
    return `import_batch:${row.importBatchId}`;
  }

  const monthReference =
    normalizeSqlDateValue(row.postedDate) ??
    normalizeSqlDateValue(row.transactionDate);

  if (!monthReference) {
    return `account_month:${row.accountId}:unknown`;
  }

  return `account_month:${row.accountId}:${monthReference.slice(0, 7)}`;
}

function buildContextualizedTransactionText(input: {
  contextualNote: string;
  originalText: string;
}) {
  return `${input.contextualNote.trim()}\n\n${input.originalText.trim()}`.trim();
}

function isRecoverableTransactionSearchProviderError(error: unknown) {
  const message =
    error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

  return (
    message.includes("gemini") ||
    message.includes("resource_exhausted") ||
    message.includes("monthly spending cap") ||
    message.includes("status 429") ||
    message.includes("service is currently unavailable") ||
    message.includes("timed out") ||
    message.includes("api key") ||
    message.includes("credentials")
  );
}

function buildBatchStats(rows: TransactionSearchSourceRow[]) {
  const merchants = new Map<string, number>();
  const counterparties = new Map<string, number>();
  const categories = new Map<string, number>();
  let creditCount = 0;
  let debitCount = 0;

  for (const row of rows) {
    const merchant = row.merchantNormalized?.trim();
    if (merchant) {
      merchants.set(merchant, (merchants.get(merchant) ?? 0) + 1);
    }

    const counterparty = row.counterpartyName?.trim();
    if (counterparty) {
      counterparties.set(
        counterparty,
        (counterparties.get(counterparty) ?? 0) + 1,
      );
    }

    const category = row.categoryCode?.trim();
    if (category) {
      categories.set(category, (categories.get(category) ?? 0) + 1);
    }

    const direction = getDirection(row.amountOriginal);
    if (direction === "credit") {
      creditCount += 1;
    } else if (direction === "debit") {
      debitCount += 1;
    }
  }

  const sortEntries = (map: Map<string, number>) =>
    [...map.entries()]
      .sort(
        (left, right) => right[1] - left[1] || left[0].localeCompare(right[0]),
      )
      .slice(0, 8)
      .map(([value, count]) => ({ value, count }));

  return {
    transactionCount: rows.length,
    creditCount,
    debitCount,
    topMerchants: sortEntries(merchants),
    topCounterparties: sortEntries(counterparties),
    topCategories: sortEntries(categories),
    recurringMerchants: sortEntries(merchants).filter(
      (entry) => entry.count > 1,
    ),
  };
}

function buildBatchSummaryLines(group: TransactionSearchBatchGroup) {
  return group.rows.map((row) => {
    const direction = getDirection(row.amountOriginal);
    return [
      `date=${row.transactionDate}`,
      `posted_at=${row.postedDate ?? row.transactionDate}`,
      `amount=${formatSignedAmount(row.amountOriginal, row.currencyOriginal)}`,
      `direction=${direction}`,
      `description=${row.descriptionRaw}`,
      `merchant=${row.merchantNormalized ?? "unknown"}`,
      `counterparty=${row.counterpartyName ?? "unknown"}`,
      `category=${row.categoryCode ?? "unknown"}`,
      `class=${row.transactionClass}`,
      `review_state=${getTransactionReviewState({
        needsReview: row.needsReview,
        categoryCode: row.categoryCode,
        llmPayload: row.llmPayload,
        creditCardStatementStatus: row.creditCardStatementStatus,
        descriptionRaw: row.descriptionRaw,
        descriptionClean: row.descriptionClean,
      })}`,
    ].join(" | ");
  });
}

function splitLinesIntoSummaryWindows(lines: string[]) {
  const windows: string[] = [];
  let current = "";

  for (const line of lines) {
    if (
      current.length > 0 &&
      current.length + line.length + 1 >
        TRANSACTION_SEARCH_SUMMARY_WINDOW_CHAR_LIMIT
    ) {
      windows.push(current);
      current = "";
    }

    current = current ? `${current}\n${line}` : line;
  }

  if (current) {
    windows.push(current);
  }

  return windows.length > 0 ? windows : [""];
}

async function summarizeTransactionBatch(
  group: TransactionSearchBatchGroup,
  llm = createLLMClient(),
) {
  const stats = buildBatchStats(group.rows);
  const accountLabel =
    group.accountName && group.institutionName
      ? `${group.accountName} at ${group.institutionName}`
      : (group.accountName ?? group.institutionName ?? "unknown account");
  const windowPayloads = splitLinesIntoSummaryWindows(
    buildBatchSummaryLines(group),
  );
  const modelName = getTransactionSearchGenerativeModel();

  const renderPrompt = (sectionText: string, index: number, total: number) =>
    `
Summarize this transaction batch for retrieval.
Return strict JSON with one key:
- summary: string

Requirements:
- Keep it factual, compact, and retrieval-oriented.
- Mention the account and institution, the period covered, dominant merchants, recurring payments, transfer/payroll/refund patterns, and broad categories when present.
- Do not invent details that are not supported by the rows.
- Write no more than 180 words.

Batch metadata:
- account: ${accountLabel}
- period_start: ${group.periodStart ?? "unknown"}
- period_end: ${group.periodEnd ?? "unknown"}
- transaction_count: ${stats.transactionCount}
- credit_count: ${stats.creditCount}
- debit_count: ${stats.debitCount}
- top_merchants: ${stats.topMerchants.map((entry) => `${entry.value} (${entry.count})`).join(", ") || "none"}
- top_counterparties: ${stats.topCounterparties.map((entry) => `${entry.value} (${entry.count})`).join(", ") || "none"}
- top_categories: ${stats.topCategories.map((entry) => `${entry.value} (${entry.count})`).join(", ") || "none"}

Window ${index + 1} of ${total}:
${sectionText}
  `.trim();

  const partialSummaries = await Promise.all(
    windowPayloads.map(async (windowText, index) => {
      const output = await llm.generateJson({
        modelName,
        systemPrompt:
          "You summarize bank transaction batches for contextual retrieval.",
        userPrompt: renderPrompt(windowText, index, windowPayloads.length),
        responseSchema: transactionBatchSummarySchema,
        responseJsonSchema: {
          type: "object",
          additionalProperties: false,
          required: ["summary"],
          properties: {
            summary: { type: "string" },
          },
        },
        schemaName: "transaction_batch_summary",
        temperature: 0.1,
      });
      return output.summary.trim();
    }),
  );

  const batchSummary =
    partialSummaries.length === 1
      ? partialSummaries[0]
      : (
          await llm.generateJson({
            modelName,
            systemPrompt:
              "You combine partial transaction summaries into one retrieval summary.",
            userPrompt: `
Combine the section summaries for one transaction batch into a final retrieval summary.
Return strict JSON with one key:
- summary: string

Requirements:
- Keep it factual and under 220 words.
- Preserve the account/institution, period, dominant merchants, recurring patterns, transfer/payroll/refund signals, and category distribution.
- Do not invent details beyond the section summaries.

Section summaries:
${partialSummaries.map((summary, index) => `[${index + 1}] ${summary}`).join("\n\n")}
            `.trim(),
            responseSchema: transactionBatchSummarySchema,
            responseJsonSchema: {
              type: "object",
              additionalProperties: false,
              required: ["summary"],
              properties: {
                summary: { type: "string" },
              },
            },
            schemaName: "transaction_batch_summary_final",
            temperature: 0.1,
          })
        ).summary.trim();

  return {
    batchSummary,
    extractedMetadata: {
      accountLabel,
      ...stats,
      sourceBatchKey: group.sourceBatchKey,
      summaryWindowCount: windowPayloads.length,
      modelName,
    },
  };
}

function buildDeterministicTransactionBatchSummary(
  group: TransactionSearchBatchGroup,
) {
  const stats = buildBatchStats(group.rows);
  const accountLabel =
    group.accountName && group.institutionName
      ? `${group.accountName} at ${group.institutionName}`
      : (group.accountName ?? group.institutionName ?? "unknown account");
  const periodLabel =
    group.periodStart && group.periodEnd
      ? group.periodStart === group.periodEnd
        ? `covering ${group.periodStart}`
        : `covering ${group.periodStart} to ${group.periodEnd}`
      : "with an unknown period";
  const topMerchants =
    stats.topMerchants
      .slice(0, 5)
      .map((entry) => `${entry.value} (${entry.count})`)
      .join(", ") || "none";
  const recurringMerchants =
    stats.recurringMerchants
      .slice(0, 5)
      .map((entry) => `${entry.value} (${entry.count})`)
      .join(", ") || "none";
  const topCategories =
    stats.topCategories
      .slice(0, 5)
      .map((entry) => `${entry.value} (${entry.count})`)
      .join(", ") || "none";

  return {
    batchSummary: `${accountLabel} ${periodLabel}. ${stats.transactionCount} transactions with ${stats.creditCount} credits and ${stats.debitCount} debits. Top merchants: ${topMerchants}. Recurring merchants: ${recurringMerchants}. Top categories: ${topCategories}.`,
    extractedMetadata: {
      accountLabel,
      ...stats,
      sourceBatchKey: group.sourceBatchKey,
      summaryWindowCount: 0,
      modelName: "deterministic_fallback",
      summaryStrategy: "deterministic_fallback",
    },
  };
}

async function contextualizeTransactionRow(
  input: {
    row: TransactionSearchSourceRow;
    batchSummary: string;
    recurringMerchantLabels: string[];
  },
  llm = createLLMClient(),
) {
  const reviewState = getTransactionReviewState({
    needsReview: input.row.needsReview,
    categoryCode: input.row.categoryCode,
    llmPayload: input.row.llmPayload,
    creditCardStatementStatus: input.row.creditCardStatementStatus,
    descriptionRaw: input.row.descriptionRaw,
    descriptionClean: input.row.descriptionClean,
  });
  const direction = getDirection(input.row.amountOriginal);
  const modelName = getTransactionSearchGenerativeModel();

  const output = await llm.generateJson({
    modelName,
    systemPrompt:
      "You write short factual contextual notes for bank transactions so retrieval systems rank them accurately.",
    userPrompt: `
Return strict JSON with one key:
- contextualNote: string

Requirements:
- This note is retrieval substrate, not a user-facing explanation.
- Mention merchant aliases only when they are directly supported by the raw description or provided merchant/counterparty metadata.
- Mention what kind of transaction this likely is and the business meaning when supported.
- Mention the account, institution, account type, economic entity, and review state when relevant.
- Mention recurring or payment-network context when present.
- Mention the date, direction, amount, and category when that improves retrieval.
- Do not invent facts that are not supported by the provided data.
- Keep it to 2-4 sentences and under 120 words.
- Do not use markdown or bullet points.

Batch summary:
${input.batchSummary}

Neighboring context:
- recurring_merchants: ${input.recurringMerchantLabels.join(", ") || "none"}

Transaction metadata:
- transaction_date: ${input.row.transactionDate}
- transaction_date_label: ${formatDateLong(input.row.transactionDate)}
- posted_at: ${input.row.postedDate ?? input.row.transactionDate}
- amount: ${formatSignedAmount(input.row.amountOriginal, input.row.currencyOriginal)}
- direction: ${direction}
- account_name: ${input.row.accountName}
- institution_name: ${input.row.institutionName}
- account_type: ${input.row.accountType ?? "unknown"}
- economic_entity_name: ${input.row.economicEntityName ?? "unknown"}
- economic_entity_kind: ${input.row.economicEntityKind ?? "unknown"}
- merchant: ${input.row.merchantNormalized ?? "unknown"}
- counterparty: ${input.row.counterpartyName ?? "unknown"}
- category: ${input.row.categoryCode ?? "unknown"}
- transaction_class: ${input.row.transactionClass}
- review_state: ${reviewState}
- review_reason: ${input.row.reviewReason ?? "none"}

Raw transaction description:
${input.row.descriptionRaw}
    `.trim(),
    responseSchema: transactionContextualizationSchema,
    responseJsonSchema: {
      type: "object",
      additionalProperties: false,
      required: ["contextualNote"],
      properties: {
        contextualNote: { type: "string" },
      },
    },
    schemaName: "transaction_contextualization",
    temperature: 0.1,
  });

  const contextualNote = output.contextualNote.trim();
  return {
    contextualNote,
    contextualizedText: buildContextualizedTransactionText({
      contextualNote,
      originalText: input.row.descriptionRaw,
    }),
    reviewState,
    direction,
    contextualizationModel: modelName,
    contextualizationPayload: {
      strategy: "llm",
      contextualNote,
    },
  };
}

function buildDeterministicTransactionContextualization(input: {
  row: TransactionSearchSourceRow;
  batchSummary: string;
  recurringMerchantLabels: string[];
}): TransactionSearchContextualizedRow {
  const reviewState = getTransactionReviewState({
    needsReview: input.row.needsReview,
    categoryCode: input.row.categoryCode,
    llmPayload: input.row.llmPayload,
    creditCardStatementStatus: input.row.creditCardStatementStatus,
    descriptionRaw: input.row.descriptionRaw,
    descriptionClean: input.row.descriptionClean,
  });
  const direction = getDirection(input.row.amountOriginal);
  const accountBits = [
    input.row.accountName,
    input.row.institutionName,
    input.row.accountType,
    input.row.economicEntityKind,
    input.row.economicEntityName,
  ].filter((value): value is string => Boolean(value));
  const recurringContext =
    input.row.merchantNormalized &&
    input.recurringMerchantLabels.includes(input.row.merchantNormalized)
      ? `${input.row.merchantNormalized} is recurring in this batch.`
      : input.recurringMerchantLabels.length > 0
        ? `Recurring merchants in this batch include ${input.recurringMerchantLabels.slice(0, 3).join(", ")}.`
        : null;
  const contextualNote = [
    `${direction === "credit" ? "Credit" : direction === "debit" ? "Debit" : "Transaction"} ${formatSignedAmount(input.row.amountOriginal, input.row.currencyOriginal)} on ${formatDateLong(input.row.postedDate ?? input.row.transactionDate)} for ${accountBits.join(", ") || "an unknown account"}.`,
    input.row.counterpartyName
      ? `Counterparty ${input.row.counterpartyName}.`
      : input.row.merchantNormalized
        ? `Merchant ${input.row.merchantNormalized}.`
        : null,
    input.row.categoryCode
      ? `Category ${input.row.categoryCode}, review state ${reviewState}.`
      : `Review state ${reviewState}.`,
    recurringContext,
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ")
    .trim();

  return {
    reviewState,
    direction,
    contextualNote,
    contextualizedText: buildContextualizedTransactionText({
      contextualNote,
      originalText: input.row.descriptionRaw,
    }),
    contextualizationModel: "deterministic_fallback",
    contextualizationPayload: {
      strategy: "deterministic_fallback",
      batchSummary: input.batchSummary,
      recurringMerchantLabels: input.recurringMerchantLabels,
    },
  };
}

async function embedTransactionSearchTexts(
  texts: string[],
  embeddingClient: TextEmbeddingClient | undefined = createTextEmbeddingClient(
    getTransactionSearchEmbeddingModel(),
  ),
) {
  const embeddings: number[][] = [];

  for (
    let index = 0;
    index < texts.length;
    index += TRANSACTION_SEARCH_EMBEDDING_BATCH_SIZE
  ) {
    const batch = texts.slice(
      index,
      index + TRANSACTION_SEARCH_EMBEDDING_BATCH_SIZE,
    );
    const batchEmbeddings = await embeddingClient.embedTexts({
      texts: batch,
      taskType: "RETRIEVAL_DOCUMENT",
      outputDimensionality: TRANSACTION_SEARCH_EMBEDDING_DIMENSIONS,
    });
    embeddings.push(...batchEmbeddings);
  }

  return embeddings;
}

function parseSourceRow(
  row: Record<string, unknown>,
): TransactionSearchSourceRow {
  return {
    transactionId: String(row.transaction_id ?? ""),
    userId: String(row.user_id ?? ""),
    accountId: String(row.account_id ?? ""),
    economicEntityId:
      typeof row.economic_entity_id === "string"
        ? row.economic_entity_id
        : null,
    importBatchId:
      typeof row.import_batch_id === "string" ? row.import_batch_id : null,
    transactionDate: normalizeSqlDateValue(row.transaction_date) ?? "",
    postedDate: normalizeSqlDateValue(row.posted_date),
    amountOriginal: String(row.amount_original ?? "0"),
    currencyOriginal: String(row.currency_original ?? "EUR"),
    descriptionRaw: String(row.description_raw ?? ""),
    descriptionClean: String(row.description_clean ?? ""),
    merchantNormalized:
      typeof row.merchant_normalized === "string"
        ? row.merchant_normalized
        : null,
    counterpartyName:
      typeof row.counterparty_name === "string" ? row.counterparty_name : null,
    categoryCode:
      typeof row.category_code === "string" ? row.category_code : null,
    transactionClass: String(row.transaction_class ?? "unknown"),
    needsReview: row.needs_review === true,
    reviewReason:
      typeof row.review_reason === "string" ? row.review_reason : null,
    llmPayload:
      row.llm_payload && typeof row.llm_payload === "object"
        ? (row.llm_payload as Record<string, unknown>)
        : null,
    creditCardStatementStatus:
      row.credit_card_statement_status === "upload_required" ||
      row.credit_card_statement_status === "uploaded"
        ? (row.credit_card_statement_status as
            | "not_applicable"
            | "upload_required"
            | "uploaded")
        : "not_applicable",
    accountName: String(row.account_name ?? ""),
    institutionName: String(row.institution_name ?? ""),
    accountType:
      typeof row.account_type === "string"
        ? (row.account_type as AccountType)
        : null,
    economicEntityName:
      typeof row.economic_entity_name === "string"
        ? row.economic_entity_name
        : null,
    economicEntityKind:
      row.economic_entity_kind === "personal" ||
      row.economic_entity_kind === "company"
        ? (row.economic_entity_kind as EntityKind)
        : null,
  };
}

async function selectTransactionSearchSourceRows(
  sql: SqlClient,
  userId: string,
  input: SyncTransactionSearchIndexInput,
) {
  const transactionIds = uniq(input.transactionIds);
  const importBatchIds = uniq(input.importBatchIds);
  const accountIds = uniq(input.accountIds);
  const entityIds = uniq(input.entityIds);
  const rows = await sql`
    select
      t.id as transaction_id,
      t.user_id,
      t.account_id,
      t.economic_entity_id,
      t.import_batch_id,
      t.transaction_date,
      t.posted_date,
      t.amount_original,
      t.currency_original,
      t.description_raw,
      t.description_clean,
      t.merchant_normalized,
      t.counterparty_name,
      t.category_code,
      t.transaction_class,
      t.needs_review,
      t.review_reason,
      t.llm_payload,
      t.credit_card_statement_status,
      a.display_name as account_name,
      a.institution_name,
      a.account_type,
      e.display_name as economic_entity_name,
      e.entity_kind as economic_entity_kind
    from public.transactions as t
    join public.accounts as a
      on a.id = t.account_id
    left join public.entities as e
      on e.id = t.economic_entity_id
    left join public.transaction_search_rows as sr
      on sr.transaction_id = t.id
    left join public.transaction_search_batches as sb
      on sb.id = sr.batch_id
    where t.user_id = ${userId}
      and (${transactionIds.length === 0} or t.id in ${sql(transactionIds.length > 0 ? transactionIds : [EMPTY_UUID])})
      and (${importBatchIds.length === 0} or t.import_batch_id in ${sql(importBatchIds.length > 0 ? importBatchIds : [EMPTY_UUID])})
      and (${accountIds.length === 0} or t.account_id in ${sql(accountIds.length > 0 ? accountIds : [EMPTY_UUID])})
      and (${entityIds.length === 0} or t.economic_entity_id in ${sql(entityIds.length > 0 ? entityIds : [EMPTY_UUID])})
      and (${input.onlyStaleOrMissing !== true} or (
        sr.transaction_id is null
        or sr.embedding_status <> 'ready'
        or coalesce(sb.status, 'stale') <> 'ready'
      ))
    order by t.transaction_date desc, t.created_at desc
  `;

  return rows.map((row) => parseSourceRow(row as Record<string, unknown>));
}

async function upsertTransactionSearchBatch(
  sql: SqlClient,
  userId: string,
  input: {
    sourceBatchKey: string;
    accountId: string | null;
    accountName: string | null;
    institutionName: string | null;
    periodStart: string | null;
    periodEnd: string | null;
    batchSummary: string;
    extractedMetadata: Record<string, unknown>;
    status: "ready" | "processing" | "stale" | "failed";
  },
) {
  const rows = await sql`
    insert into public.transaction_search_batches (
      user_id,
      source_batch_key,
      account_id,
      account_name,
      institution_name,
      period_start,
      period_end,
      batch_summary,
      extracted_metadata,
      status,
      last_indexed_at
    ) values (
      ${userId},
      ${input.sourceBatchKey},
      ${input.accountId},
      ${input.accountName},
      ${input.institutionName},
      ${input.periodStart},
      ${input.periodEnd},
      ${input.batchSummary},
      ${serializeJson(sql, input.extractedMetadata)}::jsonb,
      ${input.status},
      ${input.status === "ready" ? new Date().toISOString() : null}
    )
    on conflict (source_batch_key)
    do update
    set
      user_id = excluded.user_id,
      account_id = excluded.account_id,
      account_name = excluded.account_name,
      institution_name = excluded.institution_name,
      period_start = excluded.period_start,
      period_end = excluded.period_end,
      batch_summary = excluded.batch_summary,
      extracted_metadata = excluded.extracted_metadata,
      status = excluded.status,
      last_indexed_at = excluded.last_indexed_at,
      updated_at = timezone('utc', now())
    returning
      id,
      source_batch_key,
      account_id,
      account_name,
      institution_name,
      period_start,
      period_end,
      status
  `;

  const row = rows[0] as Record<string, unknown> | undefined;
  if (!row) {
    throw new Error(
      `Failed to upsert transaction search batch ${input.sourceBatchKey}.`,
    );
  }

  return {
    id: String(row.id ?? ""),
    sourceBatchKey: String(row.source_batch_key ?? ""),
    accountId: typeof row.account_id === "string" ? row.account_id : null,
    accountName: typeof row.account_name === "string" ? row.account_name : null,
    institutionName:
      typeof row.institution_name === "string" ? row.institution_name : null,
    periodStart: typeof row.period_start === "string" ? row.period_start : null,
    periodEnd: typeof row.period_end === "string" ? row.period_end : null,
    status: String(row.status ?? ""),
  } satisfies TransactionSearchBatchRow;
}

async function upsertTransactionSearchRows(
  sql: SqlClient,
  input: {
    batchId: string;
    rows: Array<{
      sourceRow: TransactionSearchSourceRow;
      reviewState: string;
      direction: "debit" | "credit" | "neutral";
      contextualNote: string;
      contextualizedText: string;
      batchSummary: string;
      embedding: number[];
      embeddingStatus: "ready" | "missing";
      embeddingModel: string;
      contextualizationModel: string;
      contextualizationPayload: Record<string, unknown>;
    }>;
  },
) {
  for (const row of input.rows) {
    await sql`
      insert into public.transaction_search_rows (
        transaction_id,
        batch_id,
        user_id,
        account_id,
        economic_entity_id,
        transaction_date,
        posted_at,
        amount,
        currency,
        merchant,
        counterparty,
        category,
        account_name,
        institution_name,
        account_type,
        economic_entity_name,
        economic_entity_kind,
        direction,
        review_state,
        review_reason,
        original_text,
        contextualized_text,
        document_summary,
        embedding,
        embedding_model,
        embedding_status,
        embedding_source_text,
        contextualization_model,
        contextualization_payload
      ) values (
        ${row.sourceRow.transactionId},
        ${input.batchId},
        ${row.sourceRow.userId},
        ${row.sourceRow.accountId},
        ${row.sourceRow.economicEntityId},
        ${row.sourceRow.transactionDate},
        ${row.sourceRow.postedDate ?? row.sourceRow.transactionDate},
        ${row.sourceRow.amountOriginal},
        ${row.sourceRow.currencyOriginal},
        ${row.sourceRow.merchantNormalized},
        ${row.sourceRow.counterpartyName},
        ${row.sourceRow.categoryCode},
        ${row.sourceRow.accountName},
        ${row.sourceRow.institutionName},
        ${row.sourceRow.accountType},
        ${row.sourceRow.economicEntityName},
        ${row.sourceRow.economicEntityKind},
        ${row.direction},
        ${row.reviewState},
        ${row.sourceRow.reviewReason},
        ${row.sourceRow.descriptionRaw},
        ${row.contextualizedText},
        ${row.batchSummary},
        ${serializeVector(row.embedding)}::extensions.vector(3072),
        ${row.embeddingModel},
        ${row.embeddingStatus},
        ${"contextualized_text"},
        ${row.contextualizationModel},
        ${serializeJson(sql, {
          contextualNote: row.contextualNote,
          merchant: row.sourceRow.merchantNormalized,
          counterparty: row.sourceRow.counterpartyName,
          category: row.sourceRow.categoryCode,
          accountName: row.sourceRow.accountName,
          institutionName: row.sourceRow.institutionName,
          accountType: row.sourceRow.accountType,
          economicEntityName: row.sourceRow.economicEntityName,
          economicEntityKind: row.sourceRow.economicEntityKind,
          direction: row.direction,
          reviewState: row.reviewState,
          ...row.contextualizationPayload,
        })}::jsonb
      )
      on conflict (transaction_id)
      do update
      set
        batch_id = excluded.batch_id,
        user_id = excluded.user_id,
        account_id = excluded.account_id,
        economic_entity_id = excluded.economic_entity_id,
        transaction_date = excluded.transaction_date,
        posted_at = excluded.posted_at,
        amount = excluded.amount,
        currency = excluded.currency,
        merchant = excluded.merchant,
        counterparty = excluded.counterparty,
        category = excluded.category,
        account_name = excluded.account_name,
        institution_name = excluded.institution_name,
        account_type = excluded.account_type,
        economic_entity_name = excluded.economic_entity_name,
        economic_entity_kind = excluded.economic_entity_kind,
        direction = excluded.direction,
        review_state = excluded.review_state,
        review_reason = excluded.review_reason,
        original_text = excluded.original_text,
        contextualized_text = excluded.contextualized_text,
        document_summary = excluded.document_summary,
        embedding = excluded.embedding,
        embedding_model = excluded.embedding_model,
        embedding_status = excluded.embedding_status,
        embedding_source_text = excluded.embedding_source_text,
        contextualization_model = excluded.contextualization_model,
        contextualization_payload = excluded.contextualization_payload,
        updated_at = timezone('utc', now())
    `;
  }
}

async function cleanupEmptyTransactionSearchBatches(
  sql: SqlClient,
  userId: string,
) {
  await sql`
    delete from public.transaction_search_batches as b
    where b.user_id = ${userId}
      and not exists (
        select 1
        from public.transaction_search_rows as r
        where r.batch_id = b.id
      )
  `;
}

function groupTransactionSearchRows(rows: TransactionSearchSourceRow[]) {
  const groups = new Map<string, TransactionSearchBatchGroup>();

  for (const row of rows) {
    const sourceBatchKey = buildTransactionSearchBatchKey(row);
    const existing = groups.get(sourceBatchKey);
    const transactionDate = normalizeSqlDateValue(row.transactionDate);
    const dateReference =
      normalizeSqlDateValue(row.postedDate) ?? transactionDate;

    if (existing) {
      existing.rows.push(row);
      if (isIsoDateString(transactionDate)) {
        if (
          !existing.periodStart ||
          transactionDate < existing.periodStart
        ) {
          existing.periodStart = transactionDate;
        }
        if (!existing.periodEnd || transactionDate > existing.periodEnd) {
          existing.periodEnd = transactionDate;
        }
      }
      continue;
    }

    groups.set(sourceBatchKey, {
      sourceBatchKey,
      accountId: row.accountId,
      accountName: row.accountName,
      institutionName: row.institutionName,
      periodStart: row.importBatchId ? transactionDate : startOfMonthIso(dateReference ?? ""),
      periodEnd: row.importBatchId ? transactionDate : endOfMonthIso(dateReference ?? ""),
      rows: [row],
    });
  }

  return [...groups.values()].map((group) => ({
    ...group,
    periodStart: group.sourceBatchKey.startsWith("account_month:")
      ? group.periodStart
      : (group.rows
          .map((row) => normalizeSqlDateValue(row.transactionDate))
          .filter(isIsoDateString)
          .sort()[0] ??
        group.periodStart),
    periodEnd: group.sourceBatchKey.startsWith("account_month:")
      ? group.periodEnd
      : (group.rows
          .map((row) => normalizeSqlDateValue(row.transactionDate))
          .filter(isIsoDateString)
          .sort()
          .at(-1) ?? group.periodEnd),
  }));
}

export async function markTransactionSearchRowsStale(
  sql: SqlClient,
  input: {
    userId: string;
    transactionIds?: string[];
    accountIds?: string[];
    entityIds?: string[];
    importBatchIds?: string[];
  },
) {
  const transactionIds = uniq(input.transactionIds);
  const accountIds = uniq(input.accountIds);
  const entityIds = uniq(input.entityIds);
  const importBatchIds = uniq(input.importBatchIds);

  if (
    transactionIds.length === 0 &&
    accountIds.length === 0 &&
    entityIds.length === 0 &&
    importBatchIds.length === 0
  ) {
    return;
  }

  const touchedRows = await sql`
    update public.transaction_search_rows as r
    set embedding_status = 'stale',
        updated_at = timezone('utc', now())
    where r.user_id = ${input.userId}
      and (
        (${transactionIds.length > 0} and r.transaction_id in ${sql(transactionIds.length > 0 ? transactionIds : [EMPTY_UUID])})
        or (${accountIds.length > 0} and r.account_id in ${sql(accountIds.length > 0 ? accountIds : [EMPTY_UUID])})
        or (${entityIds.length > 0} and r.economic_entity_id in ${sql(entityIds.length > 0 ? entityIds : [EMPTY_UUID])})
        or (
          ${importBatchIds.length > 0}
          and exists (
            select 1
            from public.transactions as t
            where t.id = r.transaction_id
              and t.import_batch_id in ${sql(importBatchIds.length > 0 ? importBatchIds : [EMPTY_UUID])}
          )
        )
      )
    returning batch_id
  `;

  const batchIds = uniq(
    touchedRows.flatMap((row) =>
      typeof row.batch_id === "string" ? [row.batch_id] : [],
    ),
  );

  if (batchIds.length > 0) {
    await sql`
      update public.transaction_search_batches
      set status = 'stale',
          updated_at = timezone('utc', now())
      where id in ${sql(batchIds)}
    `;
  }
}

export async function queueTransactionSearchIndexJob(
  sql: SqlClient,
  input: {
    userId: string;
    transactionIds?: string[];
    importBatchIds?: string[];
    accountIds?: string[];
    entityIds?: string[];
    trigger: string;
  },
) {
  if (!(await supportsJobType(sql, "transaction_search_index"))) {
    return null;
  }

  const payload: QueueTransactionSearchIndexPayload = {
    transactionIds: uniq(input.transactionIds),
    importBatchIds: uniq(input.importBatchIds),
    accountIds: uniq(input.accountIds),
    entityIds: uniq(input.entityIds),
    trigger: input.trigger,
  };

  if (
    (payload.transactionIds?.length ?? 0) === 0 &&
    (payload.importBatchIds?.length ?? 0) === 0 &&
    (payload.accountIds?.length ?? 0) === 0 &&
    (payload.entityIds?.length ?? 0) === 0
  ) {
    return null;
  }

  return queueJob(sql, "transaction_search_index", payload);
}

export async function syncTransactionSearchIndex(
  sql: SqlClient,
  userId: string,
  input: SyncTransactionSearchIndexInput = {},
) {
  const sourceRows = await selectTransactionSearchSourceRows(sql, userId, {
    ...input,
    onlyStaleOrMissing: input.onlyStaleOrMissing ?? true,
  });
  if (sourceRows.length === 0) {
    return {
      processedBatches: 0,
      processedTransactions: 0,
    };
  }

  const grouped = groupTransactionSearchRows(sourceRows);
  let llm: ReturnType<typeof createLLMClient> | null = null;
  try {
    llm = createLLMClient();
  } catch {
    llm = null;
  }

  let embeddingClient: TextEmbeddingClient | null = null;
  try {
    embeddingClient = createTextEmbeddingClient(
      getTransactionSearchEmbeddingModel(),
    );
  } catch {
    embeddingClient = null;
  }
  let processedTransactions = 0;

  for (const group of grouped) {
    try {
      await upsertTransactionSearchBatch(sql, userId, {
        sourceBatchKey: group.sourceBatchKey,
        accountId: group.accountId,
        accountName: group.accountName,
        institutionName: group.institutionName,
        periodStart: group.periodStart,
        periodEnd: group.periodEnd,
        batchSummary: "Indexing in progress.",
        extractedMetadata: {
          sourceBatchKey: group.sourceBatchKey,
          transactionCount: group.rows.length,
        },
        status: "processing",
      });

      let batchSummary: string;
      let extractedMetadata: Record<string, unknown>;
      if (llm) {
        try {
          const summaryResult = await summarizeTransactionBatch(group, llm);
          batchSummary = summaryResult.batchSummary;
          extractedMetadata = summaryResult.extractedMetadata;
        } catch (error) {
          if (!isRecoverableTransactionSearchProviderError(error)) {
            throw error;
          }
          const fallbackSummary = buildDeterministicTransactionBatchSummary(group);
          batchSummary = fallbackSummary.batchSummary;
          extractedMetadata = {
            ...fallbackSummary.extractedMetadata,
            summaryFallbackReason:
              error instanceof Error ? error.message : "unknown_error",
          };
        }
      } else {
        const fallbackSummary = buildDeterministicTransactionBatchSummary(group);
        batchSummary = fallbackSummary.batchSummary;
        extractedMetadata = {
          ...fallbackSummary.extractedMetadata,
          summaryFallbackReason: "llm_client_unavailable",
        };
      }
      const recurringMerchantLabels = Array.isArray(
        extractedMetadata.recurringMerchants,
      )
        ? extractedMetadata.recurringMerchants.flatMap((entry) =>
            entry &&
            typeof entry === "object" &&
            "value" in entry &&
            typeof (entry as { value?: unknown }).value === "string"
              ? [(entry as { value: string }).value]
              : [],
          )
        : [];

      const summaryUsedFallback =
        String(extractedMetadata.summaryStrategy ?? "") ===
        "deterministic_fallback";
      const contextualizedRows = summaryUsedFallback || !llm
        ? group.rows.map((row) =>
            buildDeterministicTransactionContextualization({
              row,
              batchSummary,
              recurringMerchantLabels,
            }),
          )
        : await mapWithConcurrency(
            group.rows,
            getTransactionSearchContextualizationConcurrency(group.rows.length),
            async (row) => {
              try {
                return await contextualizeTransactionRow(
                  {
                    row,
                    batchSummary,
                    recurringMerchantLabels,
                  },
                  llm,
                );
              } catch (error) {
                if (!isRecoverableTransactionSearchProviderError(error)) {
                  throw error;
                }
                return buildDeterministicTransactionContextualization({
                  row,
                  batchSummary,
                  recurringMerchantLabels,
                });
              }
            },
          );
      const contextualizationStrategy = contextualizedRows.some(
        (row) => row.contextualizationModel === "deterministic_fallback",
      )
        ? "deterministic_fallback"
        : "llm";

      const embeddingModel = getTransactionSearchEmbeddingModel();
      let embeddingStatus: "ready" | "missing" = "ready";
      let embeddings: number[][] = [];
      if (embeddingClient) {
        try {
          embeddings = await embedTransactionSearchTexts(
            contextualizedRows.map((row) => row.contextualizedText),
            embeddingClient,
          );
          if (embeddings.length !== group.rows.length) {
            throw new Error(
              `Transaction search embeddings were incomplete for batch ${group.sourceBatchKey}.`,
            );
          }
        } catch (error) {
          if (!isRecoverableTransactionSearchProviderError(error)) {
            throw error;
          }
          embeddingStatus = "missing";
          embeddings = group.rows.map(() => [
            ...TRANSACTION_SEARCH_EMBEDDING_ZERO_VECTOR,
          ]);
          extractedMetadata = {
            ...extractedMetadata,
            embeddingFallbackReason:
              error instanceof Error ? error.message : "unknown_error",
            ...(error instanceof ProviderApiError &&
            (error.providerError || error.responseJson)
              ? {
                  embeddingFallbackResponse:
                    error.providerError ?? error.responseJson,
                }
              : {}),
          };
        }
      } else {
        embeddingStatus = "missing";
        embeddings = group.rows.map(() => [
          ...TRANSACTION_SEARCH_EMBEDDING_ZERO_VECTOR,
        ]);
        extractedMetadata = {
          ...extractedMetadata,
          embeddingFallbackReason: "embedding_client_unavailable",
        };
      }

      const batch = await upsertTransactionSearchBatch(sql, userId, {
        sourceBatchKey: group.sourceBatchKey,
        accountId: group.accountId,
        accountName: group.accountName,
        institutionName: group.institutionName,
        periodStart: group.periodStart,
        periodEnd: group.periodEnd,
        batchSummary,
        extractedMetadata: {
          ...extractedMetadata,
          contextualizationStrategy,
          embeddingStrategy:
            embeddingStatus === "ready" ? "gemini_embeddings" : "missing_fallback",
        },
        status: "ready",
      });

      await upsertTransactionSearchRows(sql, {
        batchId: batch.id,
        rows: group.rows.map((row, index) => ({
          sourceRow: row,
          reviewState: contextualizedRows[index].reviewState,
          direction: contextualizedRows[index].direction,
          contextualNote: contextualizedRows[index].contextualNote,
          contextualizedText: contextualizedRows[index].contextualizedText,
          batchSummary,
          embedding: embeddings[index] ?? [],
          embeddingStatus,
          embeddingModel,
          contextualizationModel:
            contextualizedRows[index].contextualizationModel,
          contextualizationPayload:
            contextualizedRows[index].contextualizationPayload,
        })),
      });
      processedTransactions += group.rows.length;
    } catch (error) {
      await upsertTransactionSearchBatch(sql, userId, {
        sourceBatchKey: group.sourceBatchKey,
        accountId: group.accountId,
        accountName: group.accountName,
        institutionName: group.institutionName,
        periodStart: group.periodStart,
        periodEnd: group.periodEnd,
        batchSummary:
          error instanceof Error ? error.message : "Indexing failed.",
        extractedMetadata: {
          sourceBatchKey: group.sourceBatchKey,
          transactionCount: group.rows.length,
          error: error instanceof Error ? error.message : "unknown_error",
        },
        status: "failed",
      });
      throw error;
    }
  }

  await cleanupEmptyTransactionSearchBatches(sql, userId);

  return {
    processedBatches: grouped.length,
    processedTransactions,
  };
}

export async function processTransactionSearchIndexJob(
  sql: SqlClient,
  userId: string,
  payloadJson: Record<string, unknown>,
) {
  const { transactionIds, importBatchIds, accountIds, entityIds, trigger } =
    normalizeTransactionSearchIndexJobPayload(payloadJson);

  const result = await syncTransactionSearchIndex(sql, userId, {
    transactionIds,
    importBatchIds,
    accountIds,
    entityIds,
    onlyStaleOrMissing: false,
  });

  return {
    ...result,
    trigger,
  };
}

async function mapWithConcurrency<TItem, TResult>(
  items: TItem[],
  concurrency: number,
  mapper: (item: TItem, index: number) => Promise<TResult>,
) {
  const results: TResult[] = new Array(items.length);
  let cursor = 0;

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (cursor < items.length) {
        const current = cursor;
        cursor += 1;
        results[current] = await mapper(items[current], current);
      }
    }),
  );

  return results;
}
