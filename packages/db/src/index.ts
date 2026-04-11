import { randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Decimal } from "decimal.js";
import postgres from "postgres";

import {
  enrichImportedTransaction,
  getReviewPropagationEmbeddingModel,
  getInvestmentTransactionClassifierConfig,
  getTransactionClassifierConfig,
  normalizeInvestmentMatchingText,
  rankReviewPropagationTransactions,
  type TransactionEnrichmentDecision,
  type TransactionEnrichmentOptions,
  type SimilarAccountTransactionPromptContext,
} from "@myfinance/classification";
import {
  assertCategoryCodeAllowedForAccount,
  assertEconomicEntityAllowedForAccount,
  assertRuleOutputsAllowedForScope,
  assertTransactionClassAllowedForAccount,
  parseRuleDraftRequest,
  buildImportedTransactions,
  getDatasetLatestDate,
  getImportTemplateInferenceConfig,
  getRuleParserConfig,
  isCreditCardSettlementTransaction,
  normalizeImportExecutionInput,
  rebuildInvestmentState,
  resolveFxRate,
  runDeterministicImport,
  sanitizeImportResult,
  type AddOpeningPositionInput,
  type Account,
  type ApplyRuleDraftInput,
  type AuditEvent,
  type BankAccountLink,
  type BankConnection,
  type CreditCardStatementImportInput,
  type CreditCardStatementImportResult,
  type CreateEntityInput,
  type CreateAccountInput,
  type CreateRuleInput,
  type CreateTemplateInput,
  type DeleteAccountInput,
  type DeleteEntityInput,
  type DeleteHoldingAdjustmentInput,
  type DeleteTemplateInput,
  type DomainDataset,
  type FinanceRepository,
  type ImportExecutionInput,
  type ImportCommitResult,
  type ImportPreviewResult,
  type JobRunResult,
  type QueueRuleDraftInput,
  type ResetWorkspaceInput,
  type ResetWorkspaceResult,
  type Security,
  type SecurityPrice,
  type Transaction,
  type UpdateAccountInput,
  type UpdateEntityInput,
  type UpdateWorkspaceProfileInput,
  type UpdateTransactionInput,
} from "@myfinance/domain";
import {
  buildPromptProfilePreview,
  createTextEmbeddingClient,
  isTextEmbeddingConfigured,
  listPromptProfileDefinitions,
  resolvePromptProfileSections,
  sanitizePromptProfileSectionOverrides,
  type PromptProfileId,
  type PromptProfileOverrides,
  type TextEmbeddingClient,
} from "@myfinance/llm";
import {
  prepareInvestmentRebuild,
  type InvestmentRebuildProgress,
} from "./investment-rebuild";
import {
  buildRevolutAuthorizationUrl,
  buildRevolutProviderContext,
  createSignedRevolutState,
  decryptBankSecret,
  encryptBankSecret,
  exchangeRevolutAuthorizationCode,
  fetchRevolutAccounts,
  fetchRevolutExpenses,
  fetchRevolutTransactions,
  getRevolutRuntimeConfig,
  getRevolutRuntimeStatus,
  refreshRevolutAccessToken,
  verifyRevolutWebhookSignature,
  verifyRevolutWebhookTimestamp,
  verifySignedRevolutState,
  type RevolutAccount,
  type RevolutExpense,
  type RevolutTransaction,
} from "./revolut";

const DEFAULT_APP_USER_ID = "00000000-0000-0000-0000-000000000001";
const DEFAULT_LOCAL_DATABASE_URL =
  "postgresql://postgres:postgres@127.0.0.1:54322/postgres";
const dbPackageDirectory = dirname(fileURLToPath(import.meta.url));
const workspaceRoot = resolve(dbPackageDirectory, "../../..");

let envFilesLoaded = false;

function loadRootEnvFile(filename: string) {
  const filePath = resolve(workspaceRoot, filename);
  if (!existsSync(filePath)) return;

  const contents = readFileSync(filePath, "utf8");
  for (const line of contents.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex <= 0) continue;

    const key = trimmed.slice(0, separatorIndex).trim();
    if (!key || process.env[key]) continue;

    let value = trimmed.slice(separatorIndex + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

function ensureRuntimeEnvLoaded() {
  if (envFilesLoaded) return;
  loadRootEnvFile(".env.local");
  loadRootEnvFile(".env");
  envFilesLoaded = true;
}

export interface DbRuntimeConfig {
  databaseUrl?: string;
  seededUserId: string;
}

export function getDbRuntimeConfig(): DbRuntimeConfig {
  ensureRuntimeEnvLoaded();
  const databaseUrl =
    process.env.DATABASE_URL?.trim() ||
    (process.env.NODE_ENV === "production"
      ? undefined
      : DEFAULT_LOCAL_DATABASE_URL);
  return {
    databaseUrl,
    seededUserId: process.env.APP_SEEDED_USER_ID ?? DEFAULT_APP_USER_ID,
  };
}

export function createSqlClient() {
  const { databaseUrl } = getDbRuntimeConfig();
  if (!databaseUrl) {
    throw new Error(
      "DATABASE_URL is required in production. In local development the app defaults to the local Supabase Postgres URL.",
    );
  }
  return postgres(databaseUrl, {
    max: 1,
    prepare: false,
    transform: {
      undefined: null,
    },
  });
}

type SqlClient = ReturnType<typeof createSqlClient>;

export const TRANSACTION_SELECT_COLUMN_NAMES = [
  "id",
  "user_id",
  "account_id",
  "account_entity_id",
  "economic_entity_id",
  "import_batch_id",
  "provider_name",
  "provider_record_id",
  "source_fingerprint",
  "duplicate_key",
  "transaction_date",
  "posted_date",
  "amount_original",
  "currency_original",
  "amount_base_eur",
  "fx_rate_to_eur",
  "description_raw",
  "description_clean",
  "merchant_normalized",
  "counterparty_name",
  "transaction_class",
  "category_code",
  "subcategory_code",
  "transfer_group_id",
  "related_account_id",
  "related_transaction_id",
  "transfer_match_status",
  "cross_entity_flag",
  "reimbursement_status",
  "classification_status",
  "classification_source",
  "classification_confidence",
  "needs_review",
  "review_reason",
  "exclude_from_analytics",
  "correction_of_transaction_id",
  "voided_at",
  "manual_notes",
  "llm_payload",
  "raw_payload",
  "security_id",
  "quantity",
  "unit_price_original",
  "credit_card_statement_status",
  "linked_credit_card_account_id",
  "created_at",
  "updated_at",
] as const;

export const TRANSACTION_SELECT_COLUMNS =
  TRANSACTION_SELECT_COLUMN_NAMES.join(", ");

const TRANSACTION_DESCRIPTION_EMBEDDING_DIMENSIONS = 768;
const MAX_PROPAGATED_CONTEXT_ENTRIES = 10;
const RESOLVED_REVIEW_SIMILARITY_THRESHOLD = 0.8;
const MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT = 5;
const STALE_RUNNING_JOB_THRESHOLD_MS = 10 * 60_000;
const REVOLUT_PROVIDER_NAME = "revolut_business";
const REVOLUT_CONNECTION_LABEL = "Revolut Business";
const DEFAULT_IMPORT_JOBS_QUEUED = [
  "classification",
  "transfer_rematch",
  "position_rebuild",
  "metric_refresh",
] as const satisfies ImportCommitResult["jobsQueued"];

type TransactionEmbeddingSeedRow = {
  id: string;
  descriptionRaw: string;
  descriptionEmbedding: string | number[] | null;
};

type SimilarUnresolvedTransactionMatch = {
  transactionId: string;
  similarity: number;
};

type SimilarResolvedTransactionMatch = SimilarUnresolvedTransactionMatch;

type ReviewReanalysisMode = "manual_review_update" | "manual_resolved_review";

type CommitPreparedImportBatchOptions = {
  importBatchId?: string;
  importedByActor?: string;
  jobsQueued?: ImportCommitResult["jobsQueued"];
  importBatchExtraValues?: Record<string, unknown>;
};

type CommitPreparedImportBatchResult = {
  preview: ImportCommitResult;
  importBatchId: string;
  jobsQueued: ImportCommitResult["jobsQueued"];
  insertedTransactions: Transaction[];
};

type SyntheticImportBatchCommitResult = {
  importBatchId: string;
  insertedTransactions: Transaction[];
};

type BankSyncTrigger =
  | "oauth_callback"
  | "manual_sync"
  | "webhook"
  | "scheduled";

export type ResolvedSourcePrecedent = {
  sourceTransactionId: string;
  sourceAuditEventId: string | null;
  sourceDescriptionRaw: string;
  userProvidedContext: string | null;
  finalTransaction: {
    transactionClass: string;
    securityId: string | null;
    quantity: string | null;
    unitPriceOriginal: string | null;
    needsReview: boolean;
    reviewReason: string | null;
  };
  llm: {
    model: string | null;
    explanation: string | null;
    reason: string | null;
    resolutionProcess: string | null;
    rawOutput: Record<string, unknown> | null;
  };
  rebuildEvidence: Record<string, unknown> | null;
};

export type PropagatedContextEntry = {
  kind: "unresolved_source_context" | "resolved_source_precedent";
  sourceTransactionId: string;
  sourceAuditEventId: string | null;
  propagatedAt: string;
  similarity: number;
  sourceDescriptionRaw: string;
  sourceTransactionClass: string | null;
  sourceNeedsReview: boolean;
  sourceReviewReason: string | null;
  userProvidedContext: string | null;
  summaryText: string;
  resolvedPrecedent: Record<string, unknown> | null;
};

function transactionColumnsSql(sql: SqlClient, alias?: string) {
  const prefix = alias ? `${alias}.` : "";
  return sql.unsafe(
    TRANSACTION_SELECT_COLUMN_NAMES.map((column) => `${prefix}${column}`).join(
      ", ",
    ),
  );
}

function normalizeCreditCardSettlementText(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

function extractCreditCardContractSuffix(value: string) {
  const normalized = normalizeCreditCardSettlementText(value);
  const contractMatch = normalized.match(/CONTRATO\s+(\d{3,})/);
  if (contractMatch?.[1]) {
    return contractMatch[1];
  }

  const cardMatch = normalized.match(/TARJETAS?\s+DE\s+CREDITO.*?(\d{3,})/);
  return cardMatch?.[1] ?? null;
}

function buildLinkedCreditCardAccountDisplayName(
  account: Pick<Account, "institutionName">,
  contractSuffix: string | null,
) {
  return contractSuffix
    ? `${account.institutionName} Credit Card ${contractSuffix}`
    : `${account.institutionName} Credit Card`;
}

function getReviewPropagationSimilarityThreshold() {
  const value = Number(
    process.env.REVIEW_PROPAGATION_SIMILARITY_THRESHOLD ?? "0.9",
  );
  if (!Number.isFinite(value) || value <= 0) {
    return 0.9;
  }

  // Resolved-source propagation must never become stricter than 0.9.
  return Math.min(value, 0.9);
}

export interface PromptProfileModel {
  id: PromptProfileId;
  title: string;
  description: string;
  modelName: string;
  editableSections: ReturnType<typeof resolvePromptProfileSections>;
  preview: ReturnType<typeof buildPromptProfilePreview>;
}

function normalizePromptOverrides(value: unknown): PromptProfileOverrides {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).filter(
      ([, promptOverrides]) =>
        Boolean(promptOverrides) &&
        typeof promptOverrides === "object" &&
        !Array.isArray(promptOverrides),
    ),
  ) as PromptProfileOverrides;
}

async function loadPromptOverrides(sql: SqlClient, userId: string) {
  const rows = await sql`
    select prompt_overrides_json
    from public.profiles
    where id = ${userId}
    limit 1
  `;

  return normalizePromptOverrides(rows[0]?.prompt_overrides_json);
}

function resolvePromptModelName(promptId: PromptProfileId) {
  switch (promptId) {
    case "cash_transaction_analyzer":
      return getTransactionClassifierConfig().model;
    case "investment_transaction_analyzer":
      return getInvestmentTransactionClassifierConfig().model;
    case "spreadsheet_table_start":
    case "spreadsheet_layout":
      return getImportTemplateInferenceConfig().model;
    case "rule_draft_parser":
      return getRuleParserConfig().model;
  }
}

function buildPromptProfileModels(promptOverrides: PromptProfileOverrides) {
  return listPromptProfileDefinitions().map((definition) => ({
    id: definition.id,
    title: definition.title,
    description: definition.description,
    modelName: resolvePromptModelName(definition.id),
    editableSections: resolvePromptProfileSections(
      definition.id,
      promptOverrides[definition.id],
    ),
    preview: buildPromptProfilePreview(
      definition.id,
      promptOverrides[definition.id],
    ),
  })) satisfies PromptProfileModel[];
}

export async function getPromptOverrides() {
  const userId = getDbRuntimeConfig().seededUserId;
  return withSeededUserContext((sql) => loadPromptOverrides(sql, userId));
}

export async function listPromptProfiles() {
  const userId = getDbRuntimeConfig().seededUserId;
  return withSeededUserContext(async (sql) => {
    const promptOverrides = await loadPromptOverrides(sql, userId);
    return buildPromptProfileModels(promptOverrides);
  });
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
  const date = new Date(`${value}T00:00:00Z`);
  const day = date.getUTCDay();
  return day === 0 || day === 6;
}

function isRefreshableOwnedStockSecurity(security: Security) {
  return (
    security.providerName === "twelve_data" &&
    (security.assetType === "stock" || security.assetType === "etf")
  );
}

export function selectOwnedStockPriceRefreshSecurities(
  dataset: DomainDataset,
  referenceDate = getDatasetLatestDate(dataset),
) {
  const { positions } = rebuildInvestmentState(dataset, referenceDate);
  const ownedSecurityIds = new Set(
    positions.map((position) => position.securityId),
  );

  return dataset.securities.filter(
    (security) =>
      ownedSecurityIds.has(security.id) &&
      isRefreshableOwnedStockSecurity(security),
  );
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
  refreshedCount: number;
  skippedCount: number;
  refreshedSymbols: string[];
  skippedSymbols: string[];
  skippedDetails: Array<{ symbol: string; reason: string }>;
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
      const securities = selectOwnedStockPriceRefreshSecurities(dataset);
      const requestDate = new Date().toISOString().slice(0, 10);
      const refreshedSymbols: string[] = [];
      const skippedSymbols: string[] = [];
      const skippedDetails: Array<{ symbol: string; reason: string }> = [];
      let latestPriceDate: string | null = null;

      for (const security of securities) {
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

      return {
        totalTrackedStocks: securities.length,
        refreshedCount: refreshedSymbols.length,
        skippedCount: skippedSymbols.length,
        refreshedSymbols,
        skippedSymbols,
        skippedDetails,
        latestPriceDate,
        generatedAt: new Date().toISOString(),
      };
    });
  });
}

export async function updatePromptProfile(input: {
  promptId: PromptProfileId;
  sections: Record<string, unknown>;
  actorName: string;
  sourceChannel: AuditEvent["sourceChannel"];
}) {
  const userId = getDbRuntimeConfig().seededUserId;

  return withSeededUserContext(async (sql) => {
    const profileRows = await sql`
      select prompt_overrides_json
      from public.profiles
      where id = ${userId}
      limit 1
    `;
    if (!profileRows[0]) {
      throw new Error(`Profile ${userId} was not found.`);
    }

    const beforeOverrides = normalizePromptOverrides(
      profileRows[0].prompt_overrides_json,
    );
    const nextSections = sanitizePromptProfileSectionOverrides(
      input.promptId,
      input.sections,
    );
    const afterOverrides: PromptProfileOverrides = {
      ...beforeOverrides,
      [input.promptId]: nextSections,
    };

    await sql`
      update public.profiles
      set prompt_overrides_json = ${serializeJson(sql, afterOverrides)}::jsonb
      where id = ${userId}
    `;

    const auditEvent = createAuditEvent(
      input.sourceChannel,
      input.actorName,
      "prompts.update",
      "profile",
      userId,
      { promptOverridesJson: beforeOverrides, promptId: input.promptId },
      { promptOverridesJson: afterOverrides, promptId: input.promptId },
    );
    await sql`
      insert into public.audit_events ${sql({
        actor_type: auditEvent.actorType,
        actor_id: auditEvent.actorId,
        actor_name: auditEvent.actorName,
        source_channel: auditEvent.sourceChannel,
        command_name: auditEvent.commandName,
        object_type: auditEvent.objectType,
        object_id: auditEvent.objectId,
        before_json: auditEvent.beforeJson,
        after_json: auditEvent.afterJson,
        created_at: auditEvent.createdAt,
        notes: `Updated prompt template overrides for ${input.promptId}.`,
      } as Record<string, unknown>)}
    `;

    return buildPromptProfileModels(afterOverrides).find(
      (profile) => profile.id === input.promptId,
    );
  });
}

async function withSeededUserContext<T>(
  runner: (sql: SqlClient) => Promise<T>,
): Promise<T> {
  const sql = createSqlClient();
  const { seededUserId } = getDbRuntimeConfig();
  try {
    const beginTransaction = sql.begin as unknown as (
      callback: (transactionSql: SqlClient) => Promise<T>,
    ) => Promise<T>;
    return await beginTransaction(async (transactionSql) => {
      await transactionSql`select set_config('app.current_user_id', ${seededUserId}, true)`;
      return runner(transactionSql);
    });
  } finally {
    await sql.end({ timeout: 1 });
  }
}

async function withSeededUserSession<T>(
  runner: (sql: SqlClient) => Promise<T>,
): Promise<T> {
  const sql = createSqlClient();
  const { seededUserId } = getDbRuntimeConfig();
  try {
    await sql`select set_config('app.current_user_id', ${seededUserId}, false)`;
    return await runner(sql);
  } finally {
    await sql.end({ timeout: 1 });
  }
}

function camelizeKey(value: string) {
  return value.replace(/_([a-z])/g, (_, character: string) =>
    character.toUpperCase(),
  );
}

const DATE_ONLY_KEYS = new Set([
  "openingBalanceDate",
  "transactionDate",
  "postedDate",
  "asOfDate",
  "priceDate",
  "effectiveDate",
  "lastTradeDate",
  "snapshotDate",
  "month",
]);

function camelizeValue<T>(value: T, key?: string): T {
  if (Array.isArray(value)) {
    return value.map((item) => camelizeValue(item, key)) as T;
  }
  if (typeof value === "string") {
    return value;
  }
  if (value instanceof Date) {
    const iso = value.toISOString();
    return (key && DATE_ONLY_KEYS.has(key) ? iso.slice(0, 10) : iso) as T;
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(
        ([rawKey, nested]) => {
          const nextKey = camelizeKey(rawKey);
          return [nextKey, camelizeValue(nested, nextKey)];
        },
      ),
    ) as T;
  }
  return value;
}

function mapFromSql<T>(value: unknown): T {
  return camelizeValue(value as T);
}

function serializeJson(sql: SqlClient, value: unknown) {
  return sql.json((value ?? {}) as Parameters<SqlClient["json"]>[0]);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function readOptionalRecord(value: unknown) {
  return isRecord(value) ? value : null;
}

function readOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() !== "" ? value.trim() : null;
}

function readOptionalNumberAsString(value: unknown) {
  return typeof value === "number" && Number.isFinite(value)
    ? String(value)
    : null;
}

function readRawOutputField(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  if (!rawOutput) {
    return null;
  }

  if (key in rawOutput) {
    return rawOutput[key];
  }

  const camelizedKey = camelizeKey(key);
  if (camelizedKey in rawOutput) {
    return rawOutput[camelizedKey];
  }

  return null;
}

function readRawOutputString(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  return readOptionalString(readRawOutputField(rawOutput, key));
}

function readRawOutputNumberAsString(
  rawOutput: Record<string, unknown> | null,
  key: string,
) {
  const value = readRawOutputField(rawOutput, key);
  return readOptionalNumberAsString(value) ?? readOptionalString(value);
}

function readUnknownArray(value: unknown) {
  return Array.isArray(value) ? value : null;
}

function readTransactionRawOutput(
  transaction: Transaction,
): Record<string, unknown> | null {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  return readOptionalRecord(llmNode?.rawOutput);
}

function readTransactionReviewContext(
  transaction: Transaction,
): Record<string, unknown> | null {
  return readOptionalRecord(
    readOptionalRecord(transaction.llmPayload)?.reviewContext,
  );
}

function normalizeStoredVectorLiteral(value: unknown) {
  if (typeof value === "string" && value.trim() !== "") {
    return value.trim();
  }
  if (Array.isArray(value)) {
    const numericValues = value.filter(
      (candidate): candidate is number =>
        typeof candidate === "number" && Number.isFinite(candidate),
    );
    return numericValues.length > 0 ? serializeVector(numericValues) : null;
  }
  return null;
}

export function serializeVector(values: number[]) {
  return `[${values
    .filter((value) => Number.isFinite(value))
    .map((value) => value.toString())
    .join(",")}]`;
}

function parseTransactionEmbeddingSeedRow(
  row: Record<string, unknown>,
): TransactionEmbeddingSeedRow {
  return {
    id: typeof row.id === "string" ? row.id : "",
    descriptionRaw:
      typeof row.description_raw === "string" ? row.description_raw : "",
    descriptionEmbedding: normalizeStoredVectorLiteral(
      row.description_embedding,
    ),
  };
}

async function ensureTransactionDescriptionEmbeddings(
  sql: SqlClient,
  userId: string,
  rows: readonly TransactionEmbeddingSeedRow[],
  embeddingClient?: TextEmbeddingClient | null,
) {
  const rowsMissingEmbeddings = rows.filter(
    (row) => !normalizeStoredVectorLiteral(row.descriptionEmbedding),
  );
  if (rowsMissingEmbeddings.length === 0) {
    return {
      generatedCount: 0,
      skippedCount: 0,
      skippedReason: null,
    };
  }

  let client = embeddingClient;
  if (client === undefined) {
    if (!isTextEmbeddingConfigured()) {
      return {
        generatedCount: 0,
        skippedCount: rowsMissingEmbeddings.length,
        skippedReason: "embedding_client_not_configured",
      };
    }

    try {
      client = createTextEmbeddingClient(getReviewPropagationEmbeddingModel());
    } catch {
      return {
        generatedCount: 0,
        skippedCount: rowsMissingEmbeddings.length,
        skippedReason: "embedding_client_unavailable",
      };
    }
  }

  if (!client) {
    return {
      generatedCount: 0,
      skippedCount: rowsMissingEmbeddings.length,
      skippedReason: "embedding_client_unavailable",
    };
  }

  const embeddings = await client.embedTexts({
    texts: rowsMissingEmbeddings.map(
      (row) => normalizeInvestmentMatchingText(row.descriptionRaw) || " ",
    ),
    taskType: "SEMANTIC_SIMILARITY",
    outputDimensionality: TRANSACTION_DESCRIPTION_EMBEDDING_DIMENSIONS,
  });

  for (const [index, row] of rowsMissingEmbeddings.entries()) {
    const vector = embeddings[index];
    if (!vector || vector.length === 0) {
      continue;
    }

    await sql`
      update public.transactions
      set description_embedding = ${serializeVector(vector)}::extensions.vector(768)
      where id = ${row.id}
        and user_id = ${userId}
    `;
  }

  return {
    generatedCount: embeddings.length,
    skippedCount: 0,
    skippedReason: null,
  };
}

export async function findSimilarUnresolvedTransactionsByDescriptionEmbedding(
  sql: SqlClient,
  input: {
    userId: string;
    sourceTransactionId: string;
    accountId: string;
    sourceEmbedding: string;
    threshold?: number;
    limit?: number;
  },
): Promise<SimilarUnresolvedTransactionMatch[]> {
  const threshold =
    input.threshold ?? getReviewPropagationSimilarityThreshold();
  const limit =
    input.limit && Number.isFinite(input.limit) && input.limit > 0
      ? Math.floor(input.limit)
      : 2147483647;
  const rows = await sql`
    select
      id,
      1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) as similarity
    from public.transactions
    where user_id = ${input.userId}
      and account_id = ${input.accountId}
      and id <> ${input.sourceTransactionId}
      and coalesce(needs_review, false) = true
      and voided_at is null
      and description_embedding is not null
      and 1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) >= ${threshold}
    order by
      description_embedding <=>
      ${input.sourceEmbedding}::extensions.vector(768) asc
    limit ${limit}
  `;

  return rows
    .map((row) => ({
      transactionId: typeof row.id === "string" ? row.id : "",
      similarity: Number(row.similarity ?? 0),
    }))
    .filter(
      (row) => row.transactionId !== "" && Number.isFinite(row.similarity),
    );
}

export async function findSimilarResolvedTransactionsByDescriptionEmbedding(
  sql: SqlClient,
  input: {
    userId: string;
    sourceTransactionId: string;
    accountId: string;
    sourceEmbedding: string;
    threshold?: number;
    limit?: number;
  },
): Promise<SimilarResolvedTransactionMatch[]> {
  const threshold =
    typeof input.threshold === "number" && Number.isFinite(input.threshold)
      ? input.threshold
      : RESOLVED_REVIEW_SIMILARITY_THRESHOLD;
  const limit =
    input.limit && Number.isFinite(input.limit) && input.limit > 0
      ? Math.floor(input.limit)
      : MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT;
  const rows = await sql`
    select
      id,
      1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) as similarity
    from public.transactions
    where user_id = ${input.userId}
      and account_id = ${input.accountId}
      and id <> ${input.sourceTransactionId}
      and coalesce(needs_review, false) = false
      and voided_at is null
      and description_embedding is not null
      and 1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) >= ${threshold}
    order by
      description_embedding <=>
      ${input.sourceEmbedding}::extensions.vector(768) asc
    limit ${limit}
  `;

  return rows
    .map((row) => ({
      transactionId: typeof row.id === "string" ? row.id : "",
      similarity: Number(row.similarity ?? 0),
    }))
    .filter(
      (row) => row.transactionId !== "" && Number.isFinite(row.similarity),
    );
}

export async function selectReviewPropagationCandidateMatches(input: {
  dataset: DomainDataset;
  account: DomainDataset["accounts"][number];
  sourceTransaction: Transaction;
  embeddingMatches: SimilarUnresolvedTransactionMatch[];
}) {
  const rankedMatches = await rankReviewPropagationTransactions(
    input.dataset,
    input.account,
    input.sourceTransaction,
    { embeddingClient: null },
  );
  const rankedMatchById = new Map(
    rankedMatches.map((match) => [match.transaction.id, match]),
  );

  if (input.embeddingMatches.length > 0) {
    return input.embeddingMatches.filter((match) =>
      rankedMatchById.has(match.transactionId),
    );
  }

  return rankedMatches.map((match) => ({
    transactionId: match.transaction.id,
    similarity:
      match.semanticSimilarity ??
      Math.min(0.99, Math.max(0, match.lexicalScore) / 100),
  }));
}

function getTransactionUserProvidedContext(transaction: Transaction) {
  const reviewContext = readTransactionReviewContext(transaction);
  return (
    readOptionalString(reviewContext?.userProvidedContext) ??
    transaction.manualNotes ??
    null
  );
}

export function buildResolvedSourcePrecedent(
  sourceTransaction: Transaction,
  sourceAuditEventId: string | null,
): ResolvedSourcePrecedent {
  const llmPayload = readOptionalRecord(sourceTransaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);

  return {
    sourceTransactionId: sourceTransaction.id,
    sourceAuditEventId,
    sourceDescriptionRaw: sourceTransaction.descriptionRaw,
    userProvidedContext: getTransactionUserProvidedContext(sourceTransaction),
    finalTransaction: {
      transactionClass: sourceTransaction.transactionClass,
      securityId: sourceTransaction.securityId ?? null,
      quantity: sourceTransaction.quantity ?? null,
      unitPriceOriginal: sourceTransaction.unitPriceOriginal ?? null,
      needsReview: sourceTransaction.needsReview,
      reviewReason: sourceTransaction.reviewReason ?? null,
    },
    llm: {
      model:
        readOptionalString(llmNode?.model) ??
        readOptionalString(llmPayload?.model) ??
        null,
      explanation:
        readOptionalString(llmNode?.explanation) ??
        readOptionalString(llmPayload?.explanation) ??
        null,
      reason:
        readOptionalString(llmNode?.reason) ??
        readOptionalString(llmPayload?.reason) ??
        null,
      resolutionProcess: readRawOutputString(rawOutput, "resolution_process"),
      rawOutput,
    },
    rebuildEvidence: readOptionalRecord(llmPayload?.rebuildEvidence) ?? null,
  };
}

function buildResolvedReviewSeedTransaction(
  transaction: Transaction,
  assetDomain: "cash" | "investment",
): Transaction {
  return {
    ...transaction,
    merchantNormalized: null,
    counterpartyName: null,
    transactionClass: "unknown",
    categoryCode:
      assetDomain === "investment" ? "uncategorized_investment" : null,
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    needsReview: true,
    reviewReason: "Resolved transaction requested manual reanalysis.",
    manualNotes: null,
    llmPayload: null,
    securityId: null,
    quantity: null,
    unitPriceOriginal: null,
  };
}

function buildResolvedReviewSimilarTransactionContext(
  transaction: Transaction,
  similarity: number,
): SimilarAccountTransactionPromptContext {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);

  return {
    transactionDate: transaction.transactionDate,
    postedDate: transaction.postedDate ?? null,
    amountOriginal: transaction.amountOriginal,
    currencyOriginal: transaction.currencyOriginal,
    descriptionRaw: transaction.descriptionRaw,
    transactionClass: transaction.transactionClass,
    categoryCode: transaction.categoryCode ?? null,
    merchantNormalized: transaction.merchantNormalized ?? null,
    counterpartyName: transaction.counterpartyName ?? null,
    securityId: transaction.securityId ?? null,
    quantity: transaction.quantity ?? null,
    unitPriceOriginal: transaction.unitPriceOriginal ?? null,
    reviewReason: transaction.reviewReason ?? null,
    similarityScore: similarity.toFixed(2),
    userProvidedContext: getTransactionUserProvidedContext(transaction),
    resolvedInstrumentName:
      readRawOutputString(rawOutput, "resolved_instrument_name") ?? null,
    resolvedInstrumentIsin:
      readRawOutputString(rawOutput, "resolved_instrument_isin") ?? null,
    resolvedInstrumentTicker:
      readRawOutputString(rawOutput, "resolved_instrument_ticker") ?? null,
    resolvedInstrumentExchange:
      readRawOutputString(rawOutput, "resolved_instrument_exchange") ?? null,
    currentPrice:
      typeof readRawOutputField(rawOutput, "current_price") === "number"
        ? (readRawOutputField(rawOutput, "current_price") as number)
        : null,
    currentPriceCurrency:
      readRawOutputString(rawOutput, "current_price_currency") ?? null,
    currentPriceTimestamp:
      readRawOutputString(rawOutput, "current_price_timestamp") ?? null,
    currentPriceSource:
      readRawOutputString(rawOutput, "current_price_source") ?? null,
    currentPriceType:
      readRawOutputString(rawOutput, "current_price_type") ?? null,
    resolutionProcess:
      readRawOutputString(rawOutput, "resolution_process") ?? null,
    model:
      readOptionalString(llmNode?.model) ??
      readOptionalString(llmPayload?.model) ??
      null,
  };
}

function buildResolvedSourcePrecedentSummary(
  precedent: ResolvedSourcePrecedent,
) {
  return [
    `A similar transaction in this same account was resolved from "${precedent.sourceDescriptionRaw}".`,
    precedent.userProvidedContext
      ? `User review context: ${precedent.userProvidedContext}.`
      : null,
    `Final class: ${precedent.finalTransaction.transactionClass}.`,
    precedent.finalTransaction.securityId
      ? `Resolved security id: ${precedent.finalTransaction.securityId}.`
      : null,
    precedent.llm.resolutionProcess
      ? `Resolution process: ${precedent.llm.resolutionProcess}.`
      : precedent.llm.reason
        ? `Resolution reason: ${precedent.llm.reason}.`
        : null,
    readOptionalRecord(precedent.rebuildEvidence)
      ?.quantityDerivedFromHistoricalPrice === true
      ? "Quantity was later derived during the rebuild step from a historical price or NAV."
      : null,
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
}

function buildUnresolvedSourceContextSummary(sourceTransaction: Transaction) {
  return [
    `A similar transaction in this same account is still unresolved: "${sourceTransaction.descriptionRaw}".`,
    getTransactionUserProvidedContext(sourceTransaction)
      ? `User review context: ${getTransactionUserProvidedContext(sourceTransaction)}.`
      : null,
    sourceTransaction.reviewReason
      ? `Remaining unresolved reason: ${sourceTransaction.reviewReason}.`
      : "It still remains unresolved after manual review.",
    "Use this as supporting context only when the descriptions appear to refer to the same instrument or event.",
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
}

function normalizePropagatedContextEntry(
  value: unknown,
): PropagatedContextEntry | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const kind =
    record.kind === "unresolved_source_context" ||
    record.kind === "resolved_source_precedent"
      ? record.kind
      : null;
  const sourceTransactionId = readOptionalString(record.sourceTransactionId);
  const propagatedAt = readOptionalString(record.propagatedAt);
  const summaryText = readOptionalString(record.summaryText);
  const sourceDescriptionRaw = readOptionalString(record.sourceDescriptionRaw);
  if (
    !kind ||
    !sourceTransactionId ||
    !propagatedAt ||
    !summaryText ||
    !sourceDescriptionRaw
  ) {
    return null;
  }

  return {
    kind,
    sourceTransactionId,
    sourceAuditEventId: readOptionalString(record.sourceAuditEventId),
    propagatedAt,
    similarity: Number(record.similarity ?? 0),
    sourceDescriptionRaw,
    sourceTransactionClass: readOptionalString(record.sourceTransactionClass),
    sourceNeedsReview: record.sourceNeedsReview === true,
    sourceReviewReason: readOptionalString(record.sourceReviewReason),
    userProvidedContext: readOptionalString(record.userProvidedContext),
    summaryText,
    resolvedPrecedent: readOptionalRecord(record.resolvedPrecedent) ?? null,
  };
}

export function mergePropagatedContextHistory(
  existingEntries: unknown,
  nextEntry: PropagatedContextEntry,
  limit = MAX_PROPAGATED_CONTEXT_ENTRIES,
) {
  const normalizedExisting = (readUnknownArray(existingEntries) ?? [])
    .map((entry) => normalizePropagatedContextEntry(entry))
    .filter((entry): entry is PropagatedContextEntry => Boolean(entry));

  const deduplicatedExisting = normalizedExisting.filter(
    (entry) =>
      !(
        entry.sourceTransactionId === nextEntry.sourceTransactionId &&
        (entry.sourceAuditEventId ?? null) ===
          (nextEntry.sourceAuditEventId ?? null)
      ),
  );

  return [nextEntry, ...deduplicatedExisting].slice(0, limit);
}

export function buildUnresolvedSourcePropagatedContextEntry(input: {
  sourceTransaction: Transaction;
  sourceAuditEventId: string | null;
  similarity: number;
  propagatedAt: string;
}): PropagatedContextEntry {
  return {
    kind: "unresolved_source_context",
    sourceTransactionId: input.sourceTransaction.id,
    sourceAuditEventId: input.sourceAuditEventId,
    propagatedAt: input.propagatedAt,
    similarity: input.similarity,
    sourceDescriptionRaw: input.sourceTransaction.descriptionRaw,
    sourceTransactionClass: input.sourceTransaction.transactionClass ?? null,
    sourceNeedsReview: input.sourceTransaction.needsReview,
    sourceReviewReason: input.sourceTransaction.reviewReason ?? null,
    userProvidedContext: getTransactionUserProvidedContext(
      input.sourceTransaction,
    ),
    summaryText: buildUnresolvedSourceContextSummary(input.sourceTransaction),
    resolvedPrecedent: null,
  };
}

export function buildResolvedSourcePropagatedContextEntry(input: {
  sourceTransaction: Transaction;
  sourceAuditEventId: string | null;
  similarity: number;
  propagatedAt: string;
  precedent: ResolvedSourcePrecedent;
}): PropagatedContextEntry {
  return {
    kind: "resolved_source_precedent",
    sourceTransactionId: input.sourceTransaction.id,
    sourceAuditEventId: input.sourceAuditEventId,
    propagatedAt: input.propagatedAt,
    similarity: input.similarity,
    sourceDescriptionRaw: input.sourceTransaction.descriptionRaw,
    sourceTransactionClass: input.sourceTransaction.transactionClass ?? null,
    sourceNeedsReview: input.sourceTransaction.needsReview,
    sourceReviewReason: input.sourceTransaction.reviewReason ?? null,
    userProvidedContext: getTransactionUserProvidedContext(
      input.sourceTransaction,
    ),
    summaryText: buildResolvedSourcePrecedentSummary(input.precedent),
    resolvedPrecedent: input.precedent as unknown as Record<string, unknown>,
  };
}

export function canSeedReviewPropagationFromTransaction(
  account: { assetDomain: "cash" | "investment" },
  transaction: Pick<
    Transaction,
    "transactionClass" | "needsReview" | "securityId" | "voidedAt"
  >,
) {
  if (transaction.voidedAt || transaction.transactionClass === "unknown") {
    return false;
  }

  if (!transaction.needsReview) {
    return true;
  }

  return (
    account.assetDomain === "investment" && Boolean(transaction.securityId)
  );
}

export function shouldQueueReviewPropagationAfterManualReview(
  account: { assetDomain: "cash" | "investment" },
  transaction: Pick<Transaction, "needsReview">,
) {
  return (
    account.assetDomain === "investment" && transaction.needsReview === true
  );
}

export function buildReviewPropagationUserContext(
  sourceTransaction: Transaction,
) {
  const rawOutput = readTransactionRawOutput(sourceTransaction);
  const llmPayload = readOptionalRecord(sourceTransaction.llmPayload);
  const rebuildEvidence = readOptionalRecord(llmPayload?.rebuildEvidence);
  const instrumentName = readRawOutputString(
    rawOutput,
    "resolved_instrument_name",
  );
  const instrumentIsin = readRawOutputString(
    rawOutput,
    "resolved_instrument_isin",
  );
  const instrumentTicker = readRawOutputString(
    rawOutput,
    "resolved_instrument_ticker",
  );
  const instrumentExchange = readRawOutputString(
    rawOutput,
    "resolved_instrument_exchange",
  );
  const currentPrice = readRawOutputNumberAsString(rawOutput, "current_price");
  const currentPriceCurrency = readRawOutputString(
    rawOutput,
    "current_price_currency",
  );
  const currentPriceTimestamp = readRawOutputString(
    rawOutput,
    "current_price_timestamp",
  );
  const currentPriceSource = readRawOutputString(
    rawOutput,
    "current_price_source",
  );
  const currentPriceType = readRawOutputString(rawOutput, "current_price_type");
  const resolutionProcess = readRawOutputString(
    rawOutput,
    "resolution_process",
  );

  return [
    "A similar unresolved transaction from this same account was manually re-reviewed and should be used as supporting precedent when the evidence matches.",
    `Source transaction description: ${sourceTransaction.descriptionRaw}.`,
    `Source applied class: ${sourceTransaction.transactionClass}.`,
    sourceTransaction.securityId
      ? `Source mapped security id: ${sourceTransaction.securityId}.`
      : null,
    instrumentName ? `Resolved instrument name: ${instrumentName}.` : null,
    instrumentIsin ? `Resolved instrument ISIN: ${instrumentIsin}.` : null,
    instrumentTicker
      ? `Resolved instrument ticker: ${instrumentTicker}${
          instrumentExchange ? ` on ${instrumentExchange}` : ""
        }.`
      : null,
    currentPrice
      ? `Resolved current ${currentPriceType ?? "price"}: ${currentPrice}${
          currentPriceCurrency ? ` ${currentPriceCurrency}` : ""
        }${currentPriceTimestamp ? ` as of ${currentPriceTimestamp}` : ""}${
          currentPriceSource ? ` from ${currentPriceSource}` : ""
        }.`
      : null,
    resolutionProcess ? `Resolution process: ${resolutionProcess}.` : null,
    rebuildEvidence?.quantityDerivedFromHistoricalPrice === true
      ? "Quantity was later derived from a historical price or NAV during rebuild."
      : null,
    sourceTransaction.reviewReason
      ? `The source transaction may still need review for this remaining reason: ${sourceTransaction.reviewReason}.`
      : null,
  ]
    .filter((value): value is string => Boolean(value))
    .join(" ");
}

async function queueJob(
  sql: SqlClient,
  jobType: string,
  payloadJson: Record<string, unknown> = {},
  options: {
    availableAt?: string;
  } = {},
) {
  const jobId = randomUUID();
  await sql`
    insert into public.jobs (
      id,
      job_type,
      payload_json,
      status,
      attempts,
      available_at
    ) values (
      ${jobId},
      ${jobType},
      ${serializeJson(sql, payloadJson)}::jsonb,
      ${"queued"},
      0,
      ${options.availableAt ?? new Date().toISOString()}
    )
  `;
  return jobId;
}

function isInvestmentTradeTransactionClass(transactionClass: string) {
  return (
    transactionClass === "investment_trade_buy" ||
    transactionClass === "investment_trade_sell"
  );
}

export function mergeEnrichmentDecisionWithExistingTransaction(
  existingTransaction: Transaction,
  decision: TransactionEnrichmentDecision,
) {
  if (
    !isInvestmentTradeTransactionClass(decision.transactionClass) ||
    !isInvestmentTradeTransactionClass(existingTransaction.transactionClass) ||
    existingTransaction.transactionClass !== decision.transactionClass ||
    !existingTransaction.securityId ||
    !existingTransaction.quantity ||
    existingTransaction.needsReview
  ) {
    return decision;
  }

  if (
    decision.quantity ||
    decision.unitPriceOriginal ||
    !decision.needsReview
  ) {
    return decision;
  }

  return {
    ...decision,
    quantity: existingTransaction.quantity,
    unitPriceOriginal:
      decision.unitPriceOriginal ??
      existingTransaction.unitPriceOriginal ??
      null,
    needsReview: false,
    reviewReason: null,
  } satisfies TransactionEnrichmentDecision;
}

async function supportsJobType(sql: SqlClient, jobType: string) {
  const rows = await sql`
    select exists (
      select 1
      from pg_enum enum_value
      join pg_type enum_type on enum_type.oid = enum_value.enumtypid
      join pg_namespace enum_namespace
        on enum_namespace.oid = enum_type.typnamespace
      where enum_namespace.nspname = 'public'
        and enum_type.typname = 'job_type'
        and enum_value.enumlabel = ${jobType}
    ) as supported
  `;

  return rows[0]?.supported === true;
}

async function claimNextQueuedJob(sql: SqlClient, workerId: string) {
  const startedAt = new Date().toISOString();
  const claimed = await sql`
    with next_job as (
      select id
      from public.jobs
      where status = 'queued'
        and available_at <= ${startedAt}
      order by
        case job_type
          when 'review_reanalyze' then 0
          when 'rule_parse' then 1
          when 'bank_sync' then 2
          when 'classification' then 3
          when 'transfer_rematch' then 4
          when 'security_resolution' then 5
          when 'price_refresh' then 6
          when 'position_rebuild' then 7
          when 'metric_refresh' then 8
          when 'review_propagation' then 9
          else 99
        end asc,
        available_at asc,
        created_at asc
      limit 1
      for update skip locked
    )
    update public.jobs as job
    set status = 'running',
        started_at = ${startedAt},
        locked_by = ${workerId}
    from next_job
    where job.id = next_job.id
    returning job.*
  `;

  return claimed[0] ?? null;
}

async function refreshFinanceAnalyticsArtifacts(sql: SqlClient) {
  await sql`select public.refresh_finance_analytics()`;
}

function replaceTransactionInDataset(
  dataset: DomainDataset,
  transaction: Transaction,
) {
  const index = dataset.transactions.findIndex(
    (candidate) => candidate.id === transaction.id,
  );
  if (index === -1) {
    return dataset;
  }

  const nextTransactions = [...dataset.transactions];
  nextTransactions[index] = transaction;
  return {
    ...dataset,
    transactions: nextTransactions,
  };
}

async function completeJob(
  sql: SqlClient,
  jobId: string,
  startedAt: string,
  payloadJson: Record<string, unknown>,
) {
  await sql`
    update public.jobs
    set status = 'completed',
        attempts = attempts + 1,
        started_at = ${startedAt},
        finished_at = ${new Date().toISOString()},
        last_error = null,
        locked_by = null,
        payload_json = ${serializeJson(sql, payloadJson)}::jsonb
    where id = ${jobId}
  `;
}

async function failJob(
  sql: SqlClient,
  jobId: string,
  startedAt: string,
  error: unknown,
) {
  await sql`
    update public.jobs
    set status = 'failed',
        attempts = attempts + 1,
        started_at = ${startedAt},
        finished_at = ${new Date().toISOString()},
        last_error = ${
          error instanceof Error ? error.message : "Unknown job failure"
        },
        locked_by = null
    where id = ${jobId}
  `;
}

async function updateRunningJobPayload(
  sql: SqlClient,
  jobId: string,
  payloadJson: Record<string, unknown>,
) {
  await sql`
    update public.jobs
    set payload_json = ${serializeJson(sql, {
      ...payloadJson,
      heartbeatAt: new Date().toISOString(),
    })}::jsonb
    where id = ${jobId}
      and status = 'running'
  `;
}

function parseTimestampMs(value: unknown) {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }

  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function recoverStaleRunningJobs(sql: SqlClient) {
  const rows = await sql`
    select *
    from public.jobs
    where status = 'running'
  `;
  if (rows.length === 0) {
    return [];
  }

  const cutoffMs = Date.now() - STALE_RUNNING_JOB_THRESHOLD_MS;
  const staleJobIds = rows
    .filter((row) => {
      const payloadJson = parseJsonColumn<Record<string, unknown>>(
        row.payload_json ?? {},
      );
      const heartbeatMs = parseTimestampMs(payloadJson.heartbeatAt);
      const startedAtMs = parseTimestampMs(row.started_at);
      const availableAtMs = parseTimestampMs(row.available_at);
      const referenceMs = heartbeatMs ?? startedAtMs ?? availableAtMs;
      return referenceMs !== null && referenceMs <= cutoffMs;
    })
    .map((row) => row.id as string);

  if (staleJobIds.length === 0) {
    return [];
  }

  return sql`
    update public.jobs
    set status = 'queued',
        available_at = ${new Date().toISOString()},
        started_at = null,
        finished_at = null,
        last_error = 'Recovered stale running job after worker interruption.',
        locked_by = null
    where id in ${sql(staleJobIds)}
    returning *
  `;
}

const REVIEW_PROPAGATION_ANALYTICS_FIELDS = [
  "transactionClass",
  "categoryCode",
  "economicEntityId",
] as const;

const REVIEW_PROPAGATION_INVESTMENT_FIELDS = [
  "transactionClass",
  "securityId",
  "quantity",
  "unitPriceOriginal",
] as const;

function hasTransactionFieldChange(
  before: Transaction,
  after: Transaction,
  fields: readonly (keyof Transaction)[],
) {
  return fields.some((field) => before[field] !== after[field]);
}

function buildInvestmentResolutionSignal(transaction: Transaction) {
  const rawOutput = readTransactionRawOutput(transaction);
  return {
    resolvedInstrumentName:
      readRawOutputString(rawOutput, "resolved_instrument_name") ?? null,
    resolvedInstrumentIsin:
      readRawOutputString(rawOutput, "resolved_instrument_isin") ?? null,
    resolvedInstrumentTicker:
      readRawOutputString(rawOutput, "resolved_instrument_ticker") ?? null,
    resolvedInstrumentExchange:
      readRawOutputString(rawOutput, "resolved_instrument_exchange") ?? null,
    currentPriceType:
      readRawOutputString(rawOutput, "current_price_type") ?? null,
  };
}

export function shouldRunInvestmentRebuildAfterReviewPropagation(
  before: Transaction,
  after: Transaction,
) {
  return (
    hasTransactionFieldChange(
      before,
      after,
      REVIEW_PROPAGATION_INVESTMENT_FIELDS,
    ) ||
    JSON.stringify(buildInvestmentResolutionSignal(before)) !==
      JSON.stringify(buildInvestmentResolutionSignal(after))
  );
}

type ReviewPropagationMode =
  | "unresolved_source_context"
  | "resolved_source_rereview";

type ReviewPropagationCandidateResult = {
  afterTransaction: Transaction | null;
  shouldRunInvestmentRebuild: boolean;
  shouldQueueMetricRefresh: boolean;
};

async function applyReviewPropagationToCandidate(
  sql: SqlClient,
  input: {
    userId: string;
    mode: ReviewPropagationMode;
    dataset: DomainDataset;
    account: DomainDataset["accounts"][number];
    currentCandidate: Transaction;
    nextPropagatedContexts: PropagatedContextEntry[];
    promptOverrides: PromptProfileOverrides;
    resolvedSourcePrecedent: ResolvedSourcePrecedent | null;
  },
): Promise<ReviewPropagationCandidateResult> {
  if (input.mode === "unresolved_source_context") {
    const currentLlmPayload =
      readOptionalRecord(input.currentCandidate.llmPayload) ?? {};
    const nextLlmPayload = {
      ...currentLlmPayload,
      reviewContext: {
        ...(readOptionalRecord(currentLlmPayload.reviewContext) ?? {}),
        propagatedContexts: input.nextPropagatedContexts,
      },
    };

    if (JSON.stringify(currentLlmPayload) === JSON.stringify(nextLlmPayload)) {
      return {
        afterTransaction: null,
        shouldRunInvestmentRebuild: false,
        shouldQueueMetricRefresh: false,
      };
    }

    const after = await updateTransactionRecord(sql, {
      userId: input.userId,
      transactionId: input.currentCandidate.id,
      updatePayload: {
        updated_at: new Date().toISOString(),
      },
      llmPayload: nextLlmPayload,
    });
    const afterTransaction = mapFromSql<Transaction>(after);
    const auditEvent = createAuditEvent(
      "worker",
      "job:review_propagation",
      "transactions.review_propagate_context",
      "transaction",
      input.currentCandidate.id,
      input.currentCandidate as unknown as Record<string, unknown>,
      after,
    );
    await insertAuditEventRecord(
      sql,
      auditEvent,
      "Appended propagated unresolved review context from a similar transaction in the same investment account.",
    );

    return {
      afterTransaction,
      shouldRunInvestmentRebuild: false,
      shouldQueueMetricRefresh: false,
    };
  }

  const { afterRow: after, afterTransaction } =
    await executeTransactionEnrichmentPipeline(sql, input.userId, {
      dataset: input.dataset,
      account: input.account,
      transaction: input.currentCandidate,
      enrichmentOptions: {
        trigger: "review_propagation",
        promptOverrides: input.promptOverrides,
        reviewContext: {
          previousReviewReason: input.currentCandidate.reviewReason ?? null,
          previousUserContext: input.currentCandidate.manualNotes ?? null,
          previousLlmPayload:
            input.currentCandidate.llmPayload &&
            typeof input.currentCandidate.llmPayload === "object"
              ? (input.currentCandidate.llmPayload as Record<string, unknown>)
              : null,
          propagatedContexts: input.nextPropagatedContexts,
          resolvedSourcePrecedent: input.resolvedSourcePrecedent,
        },
      },
    });
  const auditEvent = createAuditEvent(
    "worker",
    "job:review_propagation",
    "transactions.review_propagate",
    "transaction",
    input.currentCandidate.id,
    input.currentCandidate as unknown as Record<string, unknown>,
    after,
  );
  await insertAuditEventRecord(
    sql,
    auditEvent,
    "Re-ran LLM classification for a similar unresolved transaction using a resolved precedent from the same investment account.",
  );

  return {
    afterTransaction,
    shouldRunInvestmentRebuild: true,
    shouldQueueMetricRefresh: true,
  };
}

async function processReviewPropagationJob(
  sql: SqlClient,
  userId: string,
  payloadJson: Record<string, unknown>,
  promptOverrides: PromptProfileOverrides,
) {
  const sourceTransactionId =
    typeof payloadJson.sourceTransactionId === "string"
      ? payloadJson.sourceTransactionId
      : "";
  const sourceAuditEventId =
    typeof payloadJson.sourceAuditEventId === "string"
      ? payloadJson.sourceAuditEventId
      : null;
  if (!sourceTransactionId) {
    throw new Error("Review propagation job is missing sourceTransactionId.");
  }

  let dataset = await loadDatasetForUser(sql, userId);
  const sourceTransaction = dataset.transactions.find(
    (candidate) => candidate.id === sourceTransactionId,
  );
  if (!sourceTransaction) {
    throw new Error(
      `Source transaction ${sourceTransactionId} was not found for review propagation.`,
    );
  }

  const account = dataset.accounts.find(
    (candidate) => candidate.id === sourceTransaction.accountId,
  );
  if (!account) {
    throw new Error(
      `Account ${sourceTransaction.accountId} was not found for review propagation.`,
    );
  }

  if (account.assetDomain !== "investment" || sourceTransaction.voidedAt) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode: sourceTransaction.needsReview
        ? "unresolved_source_context"
        : "resolved_source_rereview",
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      skippedReason: "source_transaction_not_eligible",
    };
  }

  const mode =
    sourceTransaction.needsReview === true
      ? "unresolved_source_context"
      : "resolved_source_rereview";
  const candidateSeedRowsRaw = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where user_id = ${userId}
      and account_id = ${account.id}
      and id <> ${sourceTransactionId}
      and coalesce(needs_review, false) = true
      and voided_at is null
  `;
  if (candidateSeedRowsRaw.length === 0) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
    };
  }

  const sourceSeedRowRaw = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where id = ${sourceTransactionId}
      and user_id = ${userId}
    limit 1
  `;
  const sourceSeedRow = sourceSeedRowRaw[0]
    ? parseTransactionEmbeddingSeedRow(
        sourceSeedRowRaw[0] as Record<string, unknown>,
      )
    : null;
  if (!sourceSeedRow) {
    throw new Error(
      `Source transaction ${sourceTransactionId} is missing description embedding context.`,
    );
  }

  const candidateSeedRows = candidateSeedRowsRaw.map((row) =>
    parseTransactionEmbeddingSeedRow(row as Record<string, unknown>),
  );
  let embeddingGeneration = {
    generatedCount: 0,
    skippedCount: 0,
    skippedReason: null as string | null,
  };
  try {
    const sourceEmbeddingResult = await ensureTransactionDescriptionEmbeddings(
      sql,
      userId,
      [sourceSeedRow],
    );
    const candidateEmbeddingResult =
      await ensureTransactionDescriptionEmbeddings(
        sql,
        userId,
        candidateSeedRows,
      );
    embeddingGeneration = {
      generatedCount:
        sourceEmbeddingResult.generatedCount +
        candidateEmbeddingResult.generatedCount,
      skippedCount:
        sourceEmbeddingResult.skippedCount +
        candidateEmbeddingResult.skippedCount,
      skippedReason:
        sourceEmbeddingResult.skippedReason ??
        candidateEmbeddingResult.skippedReason,
    };
  } catch (embeddingError) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      skippedReason: "embedding_generation_failed",
      embeddingError:
        embeddingError instanceof Error
          ? embeddingError.message
          : "Embedding generation failed.",
    };
  }

  const sourceEmbeddingRows = await sql`
    select description_embedding
    from public.transactions
    where id = ${sourceTransactionId}
      and user_id = ${userId}
    limit 1
  `;
  const sourceEmbedding = normalizeStoredVectorLiteral(
    sourceEmbeddingRows[0]?.description_embedding,
  );
  if (!sourceEmbedding) {
    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: 0,
      attemptedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      skippedReason:
        embeddingGeneration.skippedReason ?? "source_embedding_unavailable",
      embeddingGeneration,
    };
  }

  const embeddingMatches =
    await findSimilarUnresolvedTransactionsByDescriptionEmbedding(sql, {
      userId,
      sourceTransactionId,
      accountId: account.id,
      sourceEmbedding,
      threshold: getReviewPropagationSimilarityThreshold(),
    });
  const candidateMatches = await selectReviewPropagationCandidateMatches({
    dataset,
    account,
    sourceTransaction,
    embeddingMatches,
  });

  const appliedTransactionIds: string[] = [];
  const failedTransactionIds: Array<{ transactionId: string; error: string }> =
    [];
  let attemptedCount = 0;
  let appliedCount = 0;
  let skippedCount = 0;
  let shouldRunInvestmentRebuild = false;
  let shouldQueueMetricRefresh = false;
  const propagatedAt = new Date().toISOString();
  const resolvedSourcePrecedent =
    mode === "resolved_source_rereview"
      ? buildResolvedSourcePrecedent(sourceTransaction, sourceAuditEventId)
      : null;

  return withInvestmentMutationLock(sql, userId, async () => {
    for (const match of candidateMatches) {
      const currentCandidate =
        dataset.transactions.find(
          (candidate) => candidate.id === match.transactionId,
        ) ?? null;
      if (
        !currentCandidate ||
        !currentCandidate.needsReview ||
        currentCandidate.voidedAt
      ) {
        skippedCount += 1;
        continue;
      }

      attemptedCount += 1;
      try {
        const existingReviewContext =
          readTransactionReviewContext(currentCandidate) ?? {};
        const propagationEntry =
          mode === "resolved_source_rereview" && resolvedSourcePrecedent
            ? buildResolvedSourcePropagatedContextEntry({
                sourceTransaction,
                sourceAuditEventId,
                similarity: match.similarity,
                propagatedAt,
                precedent: resolvedSourcePrecedent,
              })
            : buildUnresolvedSourcePropagatedContextEntry({
                sourceTransaction,
                sourceAuditEventId,
                similarity: match.similarity,
                propagatedAt,
              });
        const nextPropagatedContexts = mergePropagatedContextHistory(
          existingReviewContext.propagatedContexts,
          propagationEntry,
        );
        const candidateResult = await applyReviewPropagationToCandidate(sql, {
          userId,
          mode,
          dataset,
          account,
          currentCandidate,
          nextPropagatedContexts,
          promptOverrides,
          resolvedSourcePrecedent,
        });

        if (!candidateResult.afterTransaction) {
          skippedCount += 1;
          continue;
        }

        dataset = replaceTransactionInDataset(
          dataset,
          candidateResult.afterTransaction,
        );
        appliedTransactionIds.push(candidateResult.afterTransaction.id);
        appliedCount += 1;
        shouldRunInvestmentRebuild ||=
          candidateResult.shouldRunInvestmentRebuild;
        shouldQueueMetricRefresh ||= candidateResult.shouldQueueMetricRefresh;
      } catch (candidateError) {
        skippedCount += 1;
        failedTransactionIds.push({
          transactionId: currentCandidate.id,
          error:
            candidateError instanceof Error
              ? candidateError.message
              : "Review propagation failed.",
        });
      }
    }

    let rebuilt: Awaited<ReturnType<typeof applyInvestmentRebuild>> | null =
      null;
    if (mode === "resolved_source_rereview" && shouldRunInvestmentRebuild) {
      rebuilt = await applyInvestmentRebuild(sql, userId, {
        historicalLookupTransactionIds: appliedTransactionIds,
      });
    }
    if (mode === "resolved_source_rereview" && shouldQueueMetricRefresh) {
      await queueJob(sql, "metric_refresh", {
        trigger: "review_propagation",
        sourceTransactionId,
        accountId: account.id,
        appliedTransactionIds,
      });
    }

    return {
      sourceTransactionId,
      sourceAuditEventId,
      accountId: account.id,
      mode,
      candidateCount: candidateMatches.length,
      attemptedCount,
      appliedCount,
      skippedCount,
      appliedTransactionIds,
      failedTransactionIds,
      rebuilt,
      embeddingGeneration,
    };
  });
}

function parseJsonColumn<T>(value: unknown): T {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return value as T;
    }
  }
  return value as T;
}

function createAuditEvent(
  sourceChannel: AuditEvent["sourceChannel"],
  actorName: string,
  commandName: string,
  objectType: string,
  objectId: string,
  beforeJson: Record<string, unknown> | null,
  afterJson: Record<string, unknown> | null,
): AuditEvent {
  return {
    id: randomUUID(),
    actorType: "agent",
    actorId: getDbRuntimeConfig().seededUserId,
    actorName,
    sourceChannel,
    commandName,
    objectType,
    objectId,
    beforeJson,
    afterJson,
    createdAt: new Date().toISOString(),
    notes: null,
  };
}

async function insertAuditEventRecord(
  sql: SqlClient,
  auditEvent: AuditEvent,
  notes: string | null = auditEvent.notes ?? null,
) {
  await sql`
    insert into public.audit_events ${sql({
      actor_type: auditEvent.actorType,
      actor_id: auditEvent.actorId,
      actor_name: auditEvent.actorName,
      source_channel: auditEvent.sourceChannel,
      command_name: auditEvent.commandName,
      object_type: auditEvent.objectType,
      object_id: auditEvent.objectId,
      before_json: auditEvent.beforeJson,
      after_json: auditEvent.afterJson,
      created_at: auditEvent.createdAt,
      notes,
    } as Record<string, unknown>)}
  `;
}

async function updateTransactionRecord(
  sql: SqlClient,
  input: {
    userId: string;
    transactionId: string;
    updatePayload: Record<string, unknown>;
    llmPayload?: Record<string, unknown>;
    returning?: boolean;
  },
): Promise<Record<string, unknown> | null> {
  if (input.returning === false) {
    if (input.llmPayload !== undefined) {
      await sql`
        update public.transactions
        set ${sql(input.updatePayload)},
            llm_payload = ${serializeJson(sql, input.llmPayload)}::jsonb
        where id = ${input.transactionId}
          and user_id = ${input.userId}
      `;
      return null;
    }

    await sql`
      update public.transactions
      set ${sql(input.updatePayload)}
      where id = ${input.transactionId}
        and user_id = ${input.userId}
    `;
    return null;
  }

  if (input.llmPayload !== undefined) {
    const rows = await sql`
      update public.transactions
      set ${sql(input.updatePayload)},
          llm_payload = ${serializeJson(sql, input.llmPayload)}::jsonb
      where id = ${input.transactionId}
        and user_id = ${input.userId}
      returning ${transactionColumnsSql(sql)}
    `;
    if (!rows[0]) {
      throw new Error(
        `Transaction ${input.transactionId} was not found for update.`,
      );
    }
    return rows[0];
  }

  const rows = await sql`
    update public.transactions
    set ${sql(input.updatePayload)}
    where id = ${input.transactionId}
      and user_id = ${input.userId}
    returning ${transactionColumnsSql(sql)}
  `;
  if (!rows[0]) {
    throw new Error(
      `Transaction ${input.transactionId} was not found for update.`,
    );
  }
  return rows[0];
}

async function updateTransactionFromEnrichmentDecision(
  sql: SqlClient,
  userId: string,
  transactionId: string,
  decision: TransactionEnrichmentDecision,
  options: {
    manualNotes?: string | null;
  } = {},
) {
  const beforeRow = await selectTransactionRowById(sql, userId, transactionId);
  const mergedDecision = beforeRow
    ? mergeEnrichmentDecisionWithExistingTransaction(
        mapFromSql<Transaction>(beforeRow),
        decision,
      )
    : decision;
  const updatePayload: Record<string, unknown> = {
    transaction_class: mergedDecision.transactionClass,
    category_code: mergedDecision.categoryCode ?? null,
    merchant_normalized: mergedDecision.merchantNormalized ?? null,
    counterparty_name: mergedDecision.counterpartyName ?? null,
    economic_entity_id: mergedDecision.economicEntityId,
    classification_status: mergedDecision.classificationStatus,
    classification_source: mergedDecision.classificationSource,
    classification_confidence: mergedDecision.classificationConfidence,
    quantity: mergedDecision.quantity ?? null,
    unit_price_original: mergedDecision.unitPriceOriginal ?? null,
    needs_review: mergedDecision.needsReview,
    review_reason: mergedDecision.reviewReason ?? null,
    updated_at: new Date().toISOString(),
  };
  if (options.manualNotes !== undefined) {
    updatePayload.manual_notes = options.manualNotes;
  }

  return updateTransactionRecord(sql, {
    userId,
    transactionId,
    updatePayload,
    llmPayload: mergedDecision.llmPayload,
  });
}

async function executeTransactionEnrichmentPipeline(
  sql: SqlClient,
  userId: string,
  input: {
    dataset: DomainDataset;
    account: Account;
    transaction: Transaction;
    enrichmentOptions?: TransactionEnrichmentOptions;
    updateOptions?: {
      manualNotes?: string | null;
    };
  },
) {
  const decision = await enrichImportedTransaction(
    input.dataset,
    input.account,
    input.transaction,
    input.enrichmentOptions,
  );
  const afterRow = await updateTransactionFromEnrichmentDecision(
    sql,
    userId,
    input.transaction.id,
    decision,
    input.updateOptions,
  );

  return {
    decision,
    afterRow,
    afterTransaction: mapFromSql<Transaction>(afterRow),
  };
}

function isUniqueViolation(error: unknown): error is { code: string } {
  return Boolean(
    error &&
    typeof error === "object" &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505",
  );
}

async function selectTransactionRowById(
  sql: SqlClient,
  userId: string,
  transactionId: string,
) {
  const rows = await sql`
    select ${transactionColumnsSql(sql)}
    from public.transactions
    where id = ${transactionId}
      and user_id = ${userId}
    limit 1
  `;

  return rows[0] ?? null;
}

async function selectHoldingAdjustmentRowById(
  sql: SqlClient,
  userId: string,
  adjustmentId: string,
) {
  const rows = await sql`
    select *
    from public.holding_adjustments
    where id = ${adjustmentId}
      and user_id = ${userId}
    limit 1
  `;

  return rows[0] ?? null;
}

async function resolveOrCreateLinkedCreditCardAccount(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    settlementTransaction: Transaction;
    settlementAccount: Account;
    templateId: string;
    actorName: string;
    sourceChannel: AuditEvent["sourceChannel"];
  },
) {
  const template = input.dataset.templates.find(
    (candidate) => candidate.id === input.templateId,
  );
  if (!template) {
    throw new Error(`Template ${input.templateId} was not found.`);
  }
  if (template.compatibleAccountType !== "credit_card") {
    throw new Error(
      `Template ${input.templateId} is not compatible with credit-card statements.`,
    );
  }

  if (input.settlementTransaction.linkedCreditCardAccountId) {
    const linkedAccount = input.dataset.accounts.find(
      (candidate) =>
        candidate.id === input.settlementTransaction.linkedCreditCardAccountId,
    );
    if (linkedAccount) {
      return linkedAccount;
    }
  }

  const contractSuffix = extractCreditCardContractSuffix(
    input.settlementTransaction.descriptionRaw,
  );
  const candidateAccounts = input.dataset.accounts.filter(
    (candidate) =>
      candidate.accountType === "credit_card" &&
      candidate.isActive &&
      candidate.entityId === input.settlementAccount.entityId &&
      candidate.institutionName === input.settlementAccount.institutionName,
  );
  const linkedAccount =
    (contractSuffix
      ? candidateAccounts.find(
          (candidate) =>
            candidate.accountSuffix === contractSuffix ||
            candidate.matchingAliases.includes(contractSuffix),
        )
      : null) ??
    (candidateAccounts.length === 1 ? candidateAccounts[0] : null);

  if (linkedAccount) {
    return linkedAccount;
  }

  const accountId = randomUUID();
  const afterJson = {
    id: accountId,
    userId: input.userId,
    entityId: input.settlementAccount.entityId,
    institutionName: input.settlementAccount.institutionName,
    displayName: buildLinkedCreditCardAccountDisplayName(
      input.settlementAccount,
      contractSuffix,
    ),
    accountType: "credit_card",
    assetDomain: "cash",
    defaultCurrency: input.settlementAccount.defaultCurrency,
    openingBalanceOriginal: null,
    openingBalanceCurrency: null,
    openingBalanceDate: null,
    includeInConsolidation: true,
    isActive: true,
    importTemplateDefaultId: input.templateId,
    matchingAliases: contractSuffix ? [contractSuffix] : [],
    accountSuffix: contractSuffix,
    balanceMode: "computed",
    staleAfterDays: input.settlementAccount.staleAfterDays ?? null,
    lastImportedAt: null,
    createdAt: new Date().toISOString(),
  } satisfies Account;

  await sql`
    insert into public.accounts ${sql({
      id: accountId,
      user_id: input.userId,
      entity_id: input.settlementAccount.entityId,
      institution_name: input.settlementAccount.institutionName,
      display_name: afterJson.displayName,
      account_type: "credit_card",
      asset_domain: "cash",
      default_currency: input.settlementAccount.defaultCurrency,
      opening_balance_original: null,
      opening_balance_currency: null,
      opening_balance_date: null,
      include_in_consolidation: true,
      is_active: true,
      import_template_default_id: input.templateId,
      matching_aliases: contractSuffix ? [contractSuffix] : [],
      account_suffix: contractSuffix,
      balance_mode: "computed",
      stale_after_days: input.settlementAccount.staleAfterDays ?? null,
    } as Record<string, unknown>)}
  `;

  await insertAuditEventRecord(
    sql,
    createAuditEvent(
      input.sourceChannel,
      input.actorName,
      "accounts.create",
      "account",
      accountId,
      null,
      afterJson as unknown as Record<string, unknown>,
    ),
    "Auto-created linked credit-card account from a settlement-row statement upload.",
  );

  return afterJson;
}

function sumPreparedTransactionAmountBaseEur(transactions: Transaction[]) {
  return transactions
    .reduce(
      (sum, transaction) => sum.plus(transaction.amountBaseEur),
      new Decimal(0),
    )
    .toFixed(2);
}

async function insertTransactions(
  sql: SqlClient,
  transactions: readonly Transaction[],
) {
  const insertedTransactions: Transaction[] = [];

  for (const transaction of transactions) {
    try {
      const inserted = await sql`
        insert into public.transactions (
          id,
          user_id,
          account_id,
          account_entity_id,
          economic_entity_id,
          import_batch_id,
          provider_name,
          provider_record_id,
          source_fingerprint,
          duplicate_key,
          transaction_date,
          posted_date,
          amount_original,
          currency_original,
          amount_base_eur,
          fx_rate_to_eur,
          description_raw,
          description_clean,
          merchant_normalized,
          counterparty_name,
          transaction_class,
          category_code,
          subcategory_code,
          transfer_group_id,
          related_account_id,
          related_transaction_id,
          transfer_match_status,
          cross_entity_flag,
          reimbursement_status,
          classification_status,
          classification_source,
          classification_confidence,
          needs_review,
          review_reason,
          exclude_from_analytics,
          correction_of_transaction_id,
          voided_at,
          manual_notes,
          llm_payload,
          raw_payload,
          security_id,
          quantity,
          unit_price_original,
          credit_card_statement_status,
          linked_credit_card_account_id,
          created_at,
          updated_at
        ) values (
          ${transaction.id},
          ${transaction.userId},
          ${transaction.accountId},
          ${transaction.accountEntityId},
          ${transaction.economicEntityId},
          ${transaction.importBatchId ?? null},
          ${transaction.providerName ?? null},
          ${transaction.providerRecordId ?? null},
          ${transaction.sourceFingerprint},
          ${transaction.duplicateKey ?? null},
          ${transaction.transactionDate},
          ${transaction.postedDate ?? null},
          ${transaction.amountOriginal},
          ${transaction.currencyOriginal},
          ${transaction.amountBaseEur},
          ${transaction.fxRateToEur ?? null},
          ${transaction.descriptionRaw},
          ${transaction.descriptionClean},
          ${transaction.merchantNormalized ?? null},
          ${transaction.counterpartyName ?? null},
          ${transaction.transactionClass},
          ${transaction.categoryCode ?? null},
          ${transaction.subcategoryCode ?? null},
          ${transaction.transferGroupId ?? null},
          ${transaction.relatedAccountId ?? null},
          ${transaction.relatedTransactionId ?? null},
          ${transaction.transferMatchStatus},
          ${transaction.crossEntityFlag},
          ${transaction.reimbursementStatus},
          ${transaction.classificationStatus},
          ${transaction.classificationSource},
          ${transaction.classificationConfidence},
          ${transaction.needsReview},
          ${transaction.reviewReason ?? null},
          ${transaction.excludeFromAnalytics},
          ${transaction.correctionOfTransactionId ?? null},
          ${transaction.voidedAt ?? null},
          ${transaction.manualNotes ?? null},
          ${serializeJson(sql, transaction.llmPayload)}::jsonb,
          ${serializeJson(sql, transaction.rawPayload)}::jsonb,
          ${transaction.securityId ?? null},
          ${transaction.quantity ?? null},
          ${transaction.unitPriceOriginal ?? null},
          ${transaction.creditCardStatementStatus},
          ${transaction.linkedCreditCardAccountId ?? null},
          ${transaction.createdAt},
          ${transaction.updatedAt}
        )
        returning id
      `;
      if (inserted.length > 0) {
        insertedTransactions.push(transaction);
      }
    } catch (error) {
      if (isUniqueViolation(error)) {
        continue;
      }
      throw error;
    }
  }

  return insertedTransactions;
}

async function commitPreparedImportBatch(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    normalizedInput: ReturnType<typeof normalizeImportExecutionInput>;
    previewFallback?: () => Promise<ImportPreviewResult>;
    options?: CommitPreparedImportBatchOptions;
  },
): Promise<CommitPreparedImportBatchResult> {
  const normalizedInput = input.normalizedInput;
  const commitResult = normalizedInput.filePath
    ? await runDeterministicImport("commit", normalizedInput, input.dataset)
    : null;
  const importBatchId = input.options?.importBatchId ?? randomUUID();
  const preparedTransactions =
    commitResult && normalizedInput.filePath
      ? buildImportedTransactions(
          input.dataset,
          normalizedInput,
          importBatchId,
          commitResult.normalizedRows ?? [],
        )
      : null;
  const preview =
    commitResult && normalizedInput.filePath
      ? ({
          ...(sanitizeImportResult(commitResult) as ImportCommitResult),
          rowCountDuplicates: preparedTransactions?.duplicateCount ?? 0,
        } satisfies ImportCommitResult)
      : ({
          ...((input.previewFallback
            ? await input.previewFallback()
            : await (async () => {
                throw new Error(
                  "A file path is required to commit this import flow.",
                );
              })()) as ImportCommitResult),
          importBatchId,
          rowCountInserted: 0,
          transactionIds: [],
          jobsQueued: [...DEFAULT_IMPORT_JOBS_QUEUED],
        } satisfies ImportCommitResult);
  const jobsQueued =
    input.options?.jobsQueued ??
    ((commitResult as ImportCommitResult | null)?.jobsQueued as
      | ImportCommitResult["jobsQueued"]
      | undefined) ??
    [...DEFAULT_IMPORT_JOBS_QUEUED];

  await sql`
    insert into public.import_batches ${sql({
      id: importBatchId,
      user_id: input.userId,
      account_id: normalizedInput.accountId,
      template_id: normalizedInput.templateId,
      source_kind: "upload",
      provider_name: null,
      bank_connection_id: null,
      storage_path: normalizedInput.filePath
        ? `private-imports/local/${normalizedInput.originalFilename}`
        : `private-imports/manual/${normalizedInput.originalFilename}`,
      original_filename: normalizedInput.originalFilename,
      file_sha256: randomUUID().replace(/-/g, ""),
      status: "committed",
      row_count_detected: preview.rowCountDetected,
      row_count_parsed: preview.rowCountParsed,
      row_count_inserted:
        preparedTransactions?.inserted.length ?? preview.rowCountParsed,
      row_count_duplicates:
        preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates,
      row_count_failed: preview.rowCountFailed,
      preview_summary_json: serializeJson(sql, {
        sampleRows: preview.sampleRows,
        parseErrors: preview.parseErrors,
        dateRange: preview.dateRange,
      }),
      commit_summary_json: serializeJson(sql, { jobsQueued }),
      imported_by_actor: input.options?.importedByActor ?? "web-cli",
      imported_at: new Date().toISOString(),
      ...(input.options?.importBatchExtraValues ?? {}),
    } as Record<string, unknown>)}
  `;

  for (const jobType of jobsQueued) {
    await queueJob(
      sql,
      jobType,
      {
        importBatchId,
        accountId: normalizedInput.accountId,
      },
      {
        availableAt: new Date().toISOString(),
      },
    );
  }

  const insertedTransactions = preparedTransactions
    ? await insertTransactions(sql, preparedTransactions.inserted)
    : [];

  await sql`
    update public.accounts
    set last_imported_at = ${new Date().toISOString()}
    where id = ${normalizedInput.accountId}
      and user_id = ${input.userId}
  `;

  const commitDuplicates =
    (preparedTransactions?.duplicateCount ?? preview.rowCountDuplicates) +
    ((preparedTransactions?.inserted.length ?? 0) - insertedTransactions.length);
  await sql`
    update public.import_batches
    set row_count_inserted = ${insertedTransactions.length || (preparedTransactions ? 0 : preview.rowCountParsed)},
        row_count_duplicates = ${preparedTransactions ? commitDuplicates : preview.rowCountDuplicates},
        commit_summary_json = ${serializeJson(sql, {
          jobsQueued,
          transactionIds: insertedTransactions.map((transaction) => transaction.id),
        })}::jsonb
    where id = ${importBatchId}
      and user_id = ${input.userId}
  `;

  return {
    preview: {
      ...preview,
      importBatchId,
      rowCountInserted:
        insertedTransactions.length ||
        (preparedTransactions ? 0 : preview.rowCountParsed),
      rowCountDuplicates: preparedTransactions
        ? commitDuplicates
        : preview.rowCountDuplicates,
      transactionIds: insertedTransactions.map((transaction) => transaction.id),
      jobsQueued: [...jobsQueued],
    },
    importBatchId,
    jobsQueued: [...jobsQueued],
    insertedTransactions,
  };
}

async function commitSyntheticImportBatch(
  sql: SqlClient,
  input: {
    userId: string;
    accountId: string;
    originalFilename: string;
    sourceKind: "bank_sync";
    providerName: typeof REVOLUT_PROVIDER_NAME;
    bankConnectionId: string;
    preparedTransactions: Transaction[];
    importedByActor: string;
    jobsQueued?: ImportCommitResult["jobsQueued"];
    dateRange?: { start: string; end: string } | null;
  },
): Promise<SyntheticImportBatchCommitResult> {
  const importBatchId = randomUUID();
  const jobsQueued =
    input.jobsQueued ??
    (input.preparedTransactions.length > 0
      ? [...DEFAULT_IMPORT_JOBS_QUEUED]
      : (["metric_refresh"] satisfies ImportCommitResult["jobsQueued"]));

  await sql`
    insert into public.import_batches ${sql({
      id: importBatchId,
      user_id: input.userId,
      account_id: input.accountId,
      template_id: null,
      source_kind: input.sourceKind,
      provider_name: input.providerName,
      bank_connection_id: input.bankConnectionId,
      storage_path: `bank-sync/${input.providerName}/${input.bankConnectionId}/${input.originalFilename}`,
      original_filename: input.originalFilename,
      file_sha256: randomUUID().replace(/-/g, ""),
      status: "committed",
      row_count_detected: input.preparedTransactions.length,
      row_count_parsed: input.preparedTransactions.length,
      row_count_inserted: input.preparedTransactions.length,
      row_count_duplicates: 0,
      row_count_failed: 0,
      preview_summary_json: serializeJson(sql, {
        dateRange: input.dateRange ?? null,
        sampleRows: input.preparedTransactions
          .slice(0, 3)
          .map((transaction) => ({
            providerRecordId: transaction.providerRecordId,
            transactionDate: transaction.transactionDate,
            amountOriginal: transaction.amountOriginal,
            currencyOriginal: transaction.currencyOriginal,
            descriptionRaw: transaction.descriptionRaw,
          })),
      }),
      commit_summary_json: serializeJson(sql, {
        jobsQueued,
        sourceKind: input.sourceKind,
        providerName: input.providerName,
      }),
      imported_by_actor: input.importedByActor,
      imported_at: new Date().toISOString(),
    } as Record<string, unknown>)}
  `;

  const preparedTransactions = input.preparedTransactions.map((transaction) => ({
    ...transaction,
    importBatchId,
  }));
  const insertedTransactions = await insertTransactions(sql, preparedTransactions);

  for (const jobType of jobsQueued) {
    await queueJob(
      sql,
      jobType,
      {
        importBatchId,
        accountId: input.accountId,
      },
      {
        availableAt: new Date().toISOString(),
      },
    );
  }

  await sql`
    update public.accounts
    set last_imported_at = ${new Date().toISOString()}
    where id = ${input.accountId}
      and user_id = ${input.userId}
  `;

  await sql`
    update public.import_batches
    set row_count_inserted = ${insertedTransactions.length},
        row_count_duplicates = ${Math.max(
          0,
          input.preparedTransactions.length - insertedTransactions.length,
        )},
        commit_summary_json = ${serializeJson(sql, {
          jobsQueued,
          sourceKind: input.sourceKind,
          providerName: input.providerName,
          transactionIds: insertedTransactions.map((transaction) => transaction.id),
        })}::jsonb
    where id = ${importBatchId}
      and user_id = ${input.userId}
  `;

  return {
    importBatchId,
    insertedTransactions,
  };
}

function normalizeDescriptionForSourceImport(value: string) {
  return value.trim().replace(/\s+/g, " ").replace(/\bSEPA\b/gi, "").trim();
}

function humanizeRevolutType(value: string) {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function buildRevolutProviderRecordId(
  transaction: RevolutTransaction,
  legId: string,
) {
  return `${transaction.id}:${legId}`;
}

function buildRevolutSourceFingerprint(accountId: string, providerRecordId: string) {
  return `${REVOLUT_PROVIDER_NAME}:${accountId}:${providerRecordId}`;
}

function sliceIsoDate(value: string | null | undefined) {
  return typeof value === "string" && value.length >= 10
    ? value.slice(0, 10)
    : null;
}

function buildRevolutTransactionDescription(
  transaction: RevolutTransaction,
  merchantName: string | null,
  legDescription: string | null,
) {
  const pieces = [
    merchantName,
    legDescription,
    transaction.reference ?? null,
  ].filter((value): value is string => Boolean(value));
  return (
    [...new Set(pieces)].join(" | ") || humanizeRevolutType(transaction.type)
  );
}

async function upsertAccountBalanceSnapshot(
  sql: SqlClient,
  input: {
    accountId: string;
    asOfDate: string;
    balanceOriginal: string;
    balanceCurrency: string;
    balanceBaseEur: string;
  },
) {
  await sql`
    insert into public.account_balance_snapshots ${sql({
      account_id: input.accountId,
      as_of_date: input.asOfDate,
      balance_original: input.balanceOriginal,
      balance_currency: input.balanceCurrency,
      balance_base_eur: input.balanceBaseEur,
      source_kind: "statement",
      import_batch_id: null,
    } as Record<string, unknown>)}
    on conflict (account_id, as_of_date)
    do update set
      balance_original = excluded.balance_original,
      balance_currency = excluded.balance_currency,
      balance_base_eur = excluded.balance_base_eur,
      source_kind = excluded.source_kind,
      import_batch_id = excluded.import_batch_id
  `;
}

async function createRevolutManagedAccount(
  sql: SqlClient,
  input: {
    userId: string;
    entityId: string;
    revolutAccount: RevolutAccount;
    actorName: string;
    sourceChannel: AuditEvent["sourceChannel"];
  },
): Promise<Account> {
  const accountId = randomUUID();
  const now = new Date().toISOString();
  const matchingAliases = [
    input.revolutAccount.currency,
    input.revolutAccount.name,
  ].filter((value, index, values) => values.indexOf(value) === index);
  const account = {
    id: accountId,
    userId: input.userId,
    entityId: input.entityId,
    institutionName: REVOLUT_CONNECTION_LABEL,
    displayName: input.revolutAccount.name,
    accountType: "company_bank",
    assetDomain: "cash",
    defaultCurrency: input.revolutAccount.currency,
    openingBalanceOriginal: null,
    openingBalanceCurrency: null,
    openingBalanceDate: null,
    includeInConsolidation: true,
    isActive: true,
    importTemplateDefaultId: null,
    matchingAliases,
    accountSuffix: null,
    balanceMode: "statement",
    staleAfterDays: null,
    lastImportedAt: null,
    createdAt: now,
  } satisfies Account;

  await sql`
    insert into public.accounts ${sql({
      id: account.id,
      user_id: account.userId,
      entity_id: account.entityId,
      institution_name: account.institutionName,
      display_name: account.displayName,
      account_type: account.accountType,
      asset_domain: account.assetDomain,
      default_currency: account.defaultCurrency,
      opening_balance_original: null,
      opening_balance_currency: null,
      opening_balance_date: null,
      include_in_consolidation: account.includeInConsolidation,
      is_active: account.isActive,
      import_template_default_id: null,
      matching_aliases: account.matchingAliases,
      account_suffix: null,
      balance_mode: account.balanceMode,
      stale_after_days: null,
      last_imported_at: null,
      created_at: account.createdAt,
    } as Record<string, unknown>)}
  `;

  await insertAuditEventRecord(
    sql,
    createAuditEvent(
      input.sourceChannel,
      input.actorName,
      "accounts.create",
      "account",
      account.id,
      null,
      account as unknown as Record<string, unknown>,
    ),
    "Auto-created a company bank account from a Revolut Business connection.",
  );

  return account;
}

async function resolveOrCreateRevolutAccountLinks(
  sql: SqlClient,
  input: {
    userId: string;
    dataset: DomainDataset;
    connectionId: string;
    entityId: string;
    revolutAccounts: RevolutAccount[];
    actorName: string;
    sourceChannel: AuditEvent["sourceChannel"];
  },
) {
  const now = new Date().toISOString();
  const nextAccounts = [...input.dataset.accounts];
  const nextLinks = [...input.dataset.bankAccountLinks];

  for (const revolutAccount of input.revolutAccounts.filter(
    (account) => account.state === "active",
  )) {
    let linkedAccount =
      nextLinks
        .filter(
          (link) =>
            link.connectionId === input.connectionId &&
            link.externalAccountId === revolutAccount.id,
        )
        .map((link) =>
          nextAccounts.find((account) => account.id === link.accountId) ?? null,
        )
        .find((account): account is Account => Boolean(account)) ?? null;

    if (!linkedAccount) {
      const candidates = nextAccounts.filter(
        (account) =>
          account.entityId === input.entityId &&
          account.accountType === "company_bank" &&
          account.assetDomain === "cash" &&
          account.isActive &&
          account.institutionName === REVOLUT_CONNECTION_LABEL &&
          account.defaultCurrency === revolutAccount.currency,
      );
      linkedAccount =
        candidates.find(
          (account) => account.displayName === revolutAccount.name,
        ) ??
        (candidates.length === 1 ? candidates[0] : null);
    }

    if (!linkedAccount) {
      linkedAccount = await createRevolutManagedAccount(sql, {
        userId: input.userId,
        entityId: input.entityId,
        revolutAccount,
        actorName: input.actorName,
        sourceChannel: input.sourceChannel,
      });
      nextAccounts.push(linkedAccount);
    }

    const linkId =
      nextLinks.find(
        (link) =>
          link.connectionId === input.connectionId &&
          link.externalAccountId === revolutAccount.id,
      )?.id ?? randomUUID();

    await sql`
      insert into public.bank_account_links ${sql({
        id: linkId,
        user_id: input.userId,
        connection_id: input.connectionId,
        account_id: linkedAccount.id,
        provider: REVOLUT_PROVIDER_NAME,
        external_account_id: revolutAccount.id,
        external_account_name: revolutAccount.name,
        external_currency: revolutAccount.currency,
        last_seen_at: now,
        created_at: now,
        updated_at: now,
      } as Record<string, unknown>)}
      on conflict (connection_id, external_account_id)
      do update set
        account_id = excluded.account_id,
        external_account_name = excluded.external_account_name,
        external_currency = excluded.external_currency,
        last_seen_at = excluded.last_seen_at,
        updated_at = excluded.updated_at
    `;

    const nextLink = {
      id: linkId,
      userId: input.userId,
      connectionId: input.connectionId,
      accountId: linkedAccount.id,
      provider: REVOLUT_PROVIDER_NAME,
      externalAccountId: revolutAccount.id,
      externalAccountName: revolutAccount.name,
      externalCurrency: revolutAccount.currency,
      lastSeenAt: now,
      createdAt: now,
      updatedAt: now,
    } satisfies BankAccountLink;
    const existingIndex = nextLinks.findIndex((link) => link.id === linkId);
    if (existingIndex === -1) {
      nextLinks.push(nextLink);
    } else {
      nextLinks[existingIndex] = nextLink;
    }
  }

  return {
    dataset: {
      ...input.dataset,
      accounts: nextAccounts,
      bankAccountLinks: nextLinks,
    } satisfies DomainDataset,
    bankAccountLinks: nextLinks.filter(
      (link) => link.connectionId === input.connectionId,
    ),
  };
}

function buildRevolutSyntheticTransaction(input: {
  dataset: DomainDataset;
  account: Account;
  transaction: RevolutTransaction;
  expense: RevolutExpense | null;
  leg: RevolutTransaction["legs"][number];
  importBatchId: string | null;
}) {
  const providerRecordId = buildRevolutProviderRecordId(
    input.transaction,
    input.leg.leg_id,
  );
  const transactionDate =
    sliceIsoDate(input.transaction.completed_at) ??
    sliceIsoDate(input.transaction.created_at) ??
    new Date().toISOString().slice(0, 10);
  const postedDate =
    sliceIsoDate(input.transaction.completed_at) ??
    sliceIsoDate(input.transaction.updated_at) ??
    transactionDate;
  const amountOriginal = new Decimal(input.leg.amount).toFixed(8);
  const currencyOriginal = input.leg.currency.toUpperCase();
  const fxRateToEur = resolveFxRate(
    input.dataset,
    currencyOriginal,
    "EUR",
    transactionDate,
  ).toFixed(8);
  const amountBaseEur = new Decimal(amountOriginal)
    .times(new Decimal(fxRateToEur))
    .toFixed(8);
  const merchantName = input.transaction.merchant?.name?.trim() || null;
  const descriptionRaw = buildRevolutTransactionDescription(
    input.transaction,
    merchantName,
    input.leg.description?.trim() || null,
  );
  const createdAt = new Date().toISOString();
  const providerContext = buildRevolutProviderContext({
    transaction: input.transaction,
    leg: input.leg,
    expense: input.expense,
  });

  return {
    id: randomUUID(),
    userId: input.dataset.profile.id,
    accountId: input.account.id,
    accountEntityId: input.account.entityId,
    economicEntityId: input.account.entityId,
    importBatchId: input.importBatchId,
    providerName: REVOLUT_PROVIDER_NAME,
    providerRecordId,
    sourceFingerprint: buildRevolutSourceFingerprint(
      input.account.id,
      providerRecordId,
    ),
    duplicateKey: providerRecordId,
    transactionDate,
    postedDate,
    amountOriginal,
    currencyOriginal,
    amountBaseEur,
    fxRateToEur,
    descriptionRaw,
    descriptionClean: normalizeDescriptionForSourceImport(descriptionRaw),
    merchantNormalized: merchantName,
    counterpartyName: null,
    transactionClass: "unknown",
    categoryCode: null,
    subcategoryCode: null,
    transferGroupId: null,
    relatedAccountId: null,
    relatedTransactionId: null,
    transferMatchStatus: "not_transfer",
    crossEntityFlag: false,
    reimbursementStatus: "none",
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    needsReview: true,
    reviewReason: "Queued for automatic transaction analysis.",
    excludeFromAnalytics: false,
    correctionOfTransactionId: null,
    voidedAt: null,
    manualNotes: null,
    llmPayload: {
      analysisStatus: "pending",
      explanation: null,
      model: null,
      error: null,
      queuedAt: createdAt,
      providerContext,
    },
    rawPayload: {
      provider: REVOLUT_PROVIDER_NAME,
      providerContext,
      providerRaw: {
        transaction: input.transaction,
        expense: input.expense,
      },
    },
    securityId: null,
    quantity: null,
    unitPriceOriginal: null,
    creditCardStatementStatus: "not_applicable",
    linkedCreditCardAccountId: null,
    createdAt,
    updatedAt: createdAt,
  } satisfies Transaction;
}

async function queueUniqueRevolutSyncJob(
  sql: SqlClient,
  input: {
    userId: string;
    connectionId: string;
    trigger: BankSyncTrigger;
    availableAt?: string;
  },
) {
  const existing =
    input.trigger === "scheduled"
      ? await sql`
          select id
          from public.jobs
          where job_type = ${"bank_sync"}
            and (status = ${"queued"} or status = ${"running"})
            and payload_json->>'connectionId' = ${input.connectionId}
          limit 1
        `
      : await sql`
          select id
          from public.jobs
          where job_type = ${"bank_sync"}
            and (
              status = ${"running"}
              or (
                status = ${"queued"}
                and available_at <= ${new Date().toISOString()}
              )
            )
            and payload_json->>'connectionId' = ${input.connectionId}
          limit 1
        `;
  if (existing[0]?.id) {
    return {
      queued: false,
      jobId: String(existing[0].id),
    };
  }

  const queuedAt = new Date().toISOString();
  const jobId = await queueJob(
    sql,
    "bank_sync",
    {
      connectionId: input.connectionId,
      trigger: input.trigger,
      queuedAt,
    },
    {
      availableAt: input.availableAt,
    },
  );
  await sql`
    update public.bank_connections
    set last_sync_queued_at = ${queuedAt},
        updated_at = ${queuedAt}
    where id = ${input.connectionId}
      and user_id = ${input.userId}
  `;
  return {
    queued: true,
    jobId,
  };
}

async function runRevolutSyncWithLock<T>(
  sql: SqlClient,
  connectionId: string,
  runner: () => Promise<T>,
) {
  const lockRows = await sql`
    select pg_try_advisory_lock(hashtext(${connectionId}), 814) as locked
  `;
  if (lockRows[0]?.locked !== true) {
    throw new Error(
      `Revolut connection ${connectionId} is already syncing in another worker.`,
    );
  }

  try {
    return await runner();
  } finally {
    await sql`
      select pg_advisory_unlock(hashtext(${connectionId}), 814)
    `;
  }
}

async function processRevolutSyncJob(
  sql: SqlClient,
  userId: string,
  payloadJson: Record<string, unknown>,
) {
  const connectionId =
    typeof payloadJson.connectionId === "string"
      ? payloadJson.connectionId
      : "";
  if (!connectionId) {
    throw new Error("Bank sync job is missing connectionId.");
  }

  return runRevolutSyncWithLock(sql, connectionId, async () => {
    const connectionRows = await sql`
      select *
      from public.bank_connections
      where id = ${connectionId}
        and user_id = ${userId}
        and provider = ${REVOLUT_PROVIDER_NAME}
      limit 1
    `;
    const connectionRow = connectionRows[0];
    if (!connectionRow) {
      throw new Error(`Bank connection ${connectionId} was not found.`);
    }

    const encryptedRefreshToken =
      typeof connectionRow.encrypted_refresh_token === "string"
        ? connectionRow.encrypted_refresh_token
        : "";
    if (!encryptedRefreshToken) {
      throw new Error(
        `Bank connection ${connectionId} is missing an encrypted refresh token.`,
      );
    }

    const config = getRevolutRuntimeConfig();
    const nowIso = new Date().toISOString();
    try {
      const refreshToken = decryptBankSecret(
        config.masterKey,
        encryptedRefreshToken,
      );
      const tokenResponse = await refreshRevolutAccessToken(config, refreshToken);
      const nextEncryptedRefreshToken = tokenResponse.refresh_token
        ? encryptBankSecret(config.masterKey, tokenResponse.refresh_token)
        : encryptedRefreshToken;
      const accessToken = tokenResponse.access_token;
      const revolutAccounts = await fetchRevolutAccounts(config, accessToken);

      let dataset = await loadDatasetForUser(sql, userId);
      const linked = await resolveOrCreateRevolutAccountLinks(sql, {
        userId,
        dataset,
        connectionId,
        entityId: String(connectionRow.entity_id),
        revolutAccounts,
        actorName: "worker-revolut-sync",
        sourceChannel: "worker",
      });
      dataset = linked.dataset;

      const linksByExternalAccountId = new Map(
        linked.bankAccountLinks.map((link) => [link.externalAccountId, link]),
      );
      const accountsById = new Map(
        dataset.accounts.map((account) => [account.id, account]),
      );
      const revolutAccountsById = new Map(
        revolutAccounts.map((account) => [account.id, account]),
      );

      const snapshotAsOfDate = nowIso.slice(0, 10);
      for (const link of linked.bankAccountLinks) {
        const revolutAccount = revolutAccountsById.get(link.externalAccountId);
        if (!revolutAccount) {
          continue;
        }
        const balanceOriginal = new Decimal(revolutAccount.balance).toFixed(8);
        const balanceBaseEur = new Decimal(balanceOriginal)
          .times(
            resolveFxRate(dataset, revolutAccount.currency, "EUR", snapshotAsOfDate),
          )
          .toFixed(8);
        await upsertAccountBalanceSnapshot(sql, {
          accountId: link.accountId,
          asOfDate: snapshotAsOfDate,
          balanceOriginal,
          balanceCurrency: revolutAccount.currency,
          balanceBaseEur,
        });
        await sql`
          update public.accounts
          set last_imported_at = ${nowIso}
          where id = ${link.accountId}
            and user_id = ${userId}
        `;
      }

      const lastCursorCreatedAt =
        typeof connectionRow.last_cursor_created_at === "string"
          ? connectionRow.last_cursor_created_at
          : null;
      const fromDate = lastCursorCreatedAt
        ? new Date(
            Date.parse(lastCursorCreatedAt) -
              config.syncLookbackMinutes * 60_000,
          ).toISOString()
        : new Date(
            Date.now() - config.initialBackfillDays * 24 * 60 * 60_000,
          ).toISOString();

      const fetchedTransactions: RevolutTransaction[] = [];
      let nextToCursor: string | null = null;
      while (true) {
        const page = await fetchRevolutTransactions(config, accessToken, {
          from: fromDate,
          to: nextToCursor,
          count: 1000,
        });
        if (page.length === 0) {
          break;
        }
        fetchedTransactions.push(...page);
        if (page.length < 1000) {
          break;
        }
        const nextCursor = page.at(-1)?.created_at ?? null;
        if (!nextCursor || nextCursor === nextToCursor) {
          break;
        }
        nextToCursor = nextCursor;
      }

      const expenseByTransactionId = new Map<string, RevolutExpense>();
      try {
        let nextExpenseToCursor: string | null = null;
        while (true) {
          const page = await fetchRevolutExpenses(config, accessToken, {
            from: fromDate,
            to: nextExpenseToCursor,
            count: 500,
          });
          if (page.length === 0) {
            break;
          }
          for (const expense of page) {
            if (expense.transaction_id) {
              expenseByTransactionId.set(expense.transaction_id, expense);
            }
          }
          if (page.length < 500) {
            break;
          }
          const nextCursor = page.at(-1)?.expense_date ?? null;
          if (!nextCursor || nextCursor === nextExpenseToCursor) {
            break;
          }
          nextExpenseToCursor = nextCursor;
        }
      } catch (error) {
        console.warn(
          `[revolut-sync] Expenses enrichment skipped for connection ${connectionId}: ${
            error instanceof Error ? error.message : "unknown error"
          }`,
        );
      }

      const linkedAccountIds = linked.bankAccountLinks.map((link) => link.accountId);
      const existingRows =
        linkedAccountIds.length > 0
          ? await sql`
              select ${transactionColumnsSql(sql)}
              from public.transactions
              where user_id = ${userId}
                and provider_name = ${REVOLUT_PROVIDER_NAME}
                and account_id in ${sql(linkedAccountIds)}
            `
          : [];
      const existingTransactionsByProviderRecordId = new Map(
        existingRows
          .map((row) => mapFromSql<Transaction>(row))
          .filter((transaction) => transaction.providerRecordId)
          .map((transaction) => [transaction.providerRecordId as string, transaction]),
      );

      const newTransactionsByAccount = new Map<string, Transaction[]>();
      const touchedAccountIds = new Set<string>();
      let latestSeenCursor = lastCursorCreatedAt;
      let mutatedExistingRows = 0;

      const chronologicalTransactions = [...fetchedTransactions].sort((left, right) =>
        left.created_at.localeCompare(right.created_at),
      );
      for (const revolutTransaction of chronologicalTransactions) {
        if (!latestSeenCursor || revolutTransaction.created_at > latestSeenCursor) {
          latestSeenCursor = revolutTransaction.created_at;
        }
        const expense = expenseByTransactionId.get(revolutTransaction.id) ?? null;
        for (const leg of revolutTransaction.legs) {
          const link = linksByExternalAccountId.get(leg.account_id);
          if (!link) {
            continue;
          }
          const account = accountsById.get(link.accountId);
          if (!account) {
            continue;
          }
          touchedAccountIds.add(account.id);
          const providerRecordId = buildRevolutProviderRecordId(
            revolutTransaction,
            leg.leg_id,
          );
          const nextTransaction = buildRevolutSyntheticTransaction({
            dataset,
            account,
            transaction: revolutTransaction,
            expense,
            leg,
            importBatchId: null,
          });
          const existingTransaction =
            existingTransactionsByProviderRecordId.get(providerRecordId) ?? null;

          if (revolutTransaction.state === "completed") {
            if (existingTransaction) {
              await updateTransactionRecord(sql, {
                userId,
                transactionId: existingTransaction.id,
                updatePayload: {
                  transaction_date: nextTransaction.transactionDate,
                  posted_date: nextTransaction.postedDate,
                  amount_original: nextTransaction.amountOriginal,
                  currency_original: nextTransaction.currencyOriginal,
                  amount_base_eur: nextTransaction.amountBaseEur,
                  fx_rate_to_eur: nextTransaction.fxRateToEur,
                  description_raw: nextTransaction.descriptionRaw,
                  description_clean: nextTransaction.descriptionClean,
                  source_fingerprint: nextTransaction.sourceFingerprint,
                  duplicate_key: nextTransaction.duplicateKey,
                  provider_name: nextTransaction.providerName,
                  provider_record_id: nextTransaction.providerRecordId,
                  raw_payload: nextTransaction.rawPayload,
                  voided_at: null,
                  exclude_from_analytics: false,
                  updated_at: nowIso,
                },
                returning: false,
              });
              mutatedExistingRows += 1;
            } else {
              const accountTransactions =
                newTransactionsByAccount.get(account.id) ?? [];
              accountTransactions.push(nextTransaction);
              newTransactionsByAccount.set(account.id, accountTransactions);
            }
            continue;
          }

          if (revolutTransaction.state === "reverted" && existingTransaction) {
            await updateTransactionRecord(sql, {
              userId,
              transactionId: existingTransaction.id,
              updatePayload: {
                raw_payload: nextTransaction.rawPayload,
                voided_at: nowIso,
                updated_at: nowIso,
              },
              returning: false,
            });
            mutatedExistingRows += 1;
          }
        }
      }

      let insertedTransactions = 0;
      for (const [accountId, preparedTransactions] of newTransactionsByAccount) {
        if (preparedTransactions.length === 0) {
          continue;
        }
        const account = accountsById.get(accountId);
        if (!account) {
          continue;
        }
        const dates = preparedTransactions.map((transaction) => transaction.transactionDate);
        const committed = await commitSyntheticImportBatch(sql, {
          userId,
          accountId,
          originalFilename: `revolut-sync-${account.defaultCurrency}-${snapshotAsOfDate}.json`,
          sourceKind: "bank_sync",
          providerName: REVOLUT_PROVIDER_NAME,
          bankConnectionId: connectionId,
          preparedTransactions,
          importedByActor: "worker-revolut-sync",
          dateRange: {
            start: [...dates].sort()[0] ?? snapshotAsOfDate,
            end: [...dates].sort().at(-1) ?? snapshotAsOfDate,
          },
        });
        insertedTransactions += committed.insertedTransactions.length;
      }

      if (mutatedExistingRows > 0 && insertedTransactions === 0) {
        await queueJob(sql, "metric_refresh", {
          connectionId,
          trigger: "bank_sync_update",
        });
      }

      const nextScheduledSyncAt = new Date(
        Date.now() + config.syncIntervalMinutes * 60_000,
      ).toISOString();
      await sql`
        update public.bank_connections
        set encrypted_refresh_token = ${nextEncryptedRefreshToken},
            status = ${"active"},
            last_cursor_created_at = ${latestSeenCursor ?? lastCursorCreatedAt},
            last_successful_sync_at = ${nowIso},
            auth_expires_at = ${new Date(
              Date.now() + tokenResponse.expires_in * 1000,
            ).toISOString()},
            last_error = null,
            updated_at = ${nowIso}
        where id = ${connectionId}
          and user_id = ${userId}
      `;
      await queueUniqueRevolutSyncJob(sql, {
        userId,
        connectionId,
        trigger: "scheduled",
        availableAt: nextScheduledSyncAt,
      });

      return {
        connectionId,
        fetchedTransactions: fetchedTransactions.length,
        insertedTransactions,
        updatedTransactions: mutatedExistingRows,
        linkedAccountCount: linked.bankAccountLinks.length,
        latestCursorCreatedAt: latestSeenCursor,
        syncedAt: nowIso,
      };
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown Revolut sync failure.";
      const nextStatus =
        /invalid_grant|unauthorized|401/i.test(errorMessage)
          ? "reauthorization_required"
          : "error";
      await sql`
        update public.bank_connections
        set status = ${nextStatus},
            last_error = ${errorMessage},
            updated_at = ${nowIso}
        where id = ${connectionId}
          and user_id = ${userId}
      `;
      throw error;
    }
  });
}

async function loadDatasetForUser(
  sql: SqlClient,
  userId: string,
): Promise<DomainDataset> {
  const [
    profiles,
    entities,
    accounts,
    bankConnections,
    bankAccountLinks,
    templates,
    importBatches,
    transactions,
    categories,
    rules,
    auditEvents,
    jobs,
    accountBalanceSnapshots,
    securities,
    securityAliases,
    securityPrices,
    fxRates,
    holdingAdjustments,
    investmentPositions,
    dailyPortfolioSnapshots,
    monthlyCashFlowRollups,
  ] = await Promise.all([
    sql`select * from public.profiles where id = ${userId} limit 1`,
    sql`select * from public.entities where user_id = ${userId} order by created_at`,
    sql`select * from public.accounts where user_id = ${userId} order by created_at`,
    sql`
      select
        id,
        user_id,
        entity_id,
        provider,
        connection_label,
        status,
        external_business_id,
        last_cursor_created_at,
        last_successful_sync_at,
        last_sync_queued_at,
        last_webhook_at,
        auth_expires_at,
        last_error,
        metadata_json,
        created_at,
        updated_at
      from public.bank_connections
      where user_id = ${userId}
      order by created_at
    `,
    sql`
      select *
      from public.bank_account_links
      where user_id = ${userId}
      order by created_at
    `,
    sql`select * from public.import_templates where user_id = ${userId} order by created_at`,
    sql`select * from public.import_batches where user_id = ${userId} order by imported_at desc`,
    sql`
      select ${transactionColumnsSql(sql)}
      from public.transactions
      where user_id = ${userId}
      order by transaction_date desc, created_at desc
    `,
    sql`select * from public.categories order by sort_order, code`,
    sql`select * from public.classification_rules where user_id = ${userId} order by priority`,
    sql`select * from public.audit_events order by created_at desc limit 200`,
    sql`select * from public.jobs order by created_at desc`,
    sql`select * from public.account_balance_snapshots where account_id in (select id from public.accounts where user_id = ${userId}) order by as_of_date desc`,
    sql`select * from public.securities order by display_symbol`,
    sql`select * from public.security_aliases order by created_at desc`,
    sql`select * from public.security_prices order by price_date desc, quote_timestamp desc`,
    sql`select * from public.fx_rates order by as_of_date desc`,
    sql`select * from public.holding_adjustments where user_id = ${userId} order by effective_date desc`,
    sql`select * from public.investment_positions where user_id = ${userId}`,
    sql`select * from public.daily_portfolio_snapshots where user_id = ${userId} order by snapshot_date desc`,
    sql`
      with income as (
        select entity_id, month, income_total_eur
        from public.mv_monthly_income_totals
        where user_id = ${userId}
      ),
      spending as (
        select entity_id, month, sum(spending_total_eur) as spending_total_eur
        from public.mv_monthly_spending_totals
        where user_id = ${userId}
        group by entity_id, month
      )
      select
        coalesce(income.entity_id, spending.entity_id) as entity_id,
        coalesce(income.month, spending.month) as month,
        coalesce(income.income_total_eur, 0) as income_eur,
        coalesce(spending.spending_total_eur, 0) as spending_eur,
        coalesce(income.income_total_eur, 0) - coalesce(spending.spending_total_eur, 0) as operating_net_eur
      from income
      full outer join spending
        on spending.entity_id = income.entity_id
       and spending.month = income.month
      order by month asc
    `,
  ]);

  if (!profiles[0]) {
    throw new Error(
      `Seeded user ${userId} was not found in the database. Run the seed or set APP_SEEDED_USER_ID correctly.`,
    );
  }

  return {
    schemaVersion: "v1" as const,
    profile: mapFromSql<DomainDataset["profile"]>(profiles[0]),
    entities: mapFromSql<DomainDataset["entities"]>(entities),
    accounts: mapFromSql<DomainDataset["accounts"]>(accounts),
    bankConnections:
      mapFromSql<DomainDataset["bankConnections"]>(bankConnections),
    bankAccountLinks:
      mapFromSql<DomainDataset["bankAccountLinks"]>(bankAccountLinks),
    templates: mapFromSql<DomainDataset["templates"]>(templates),
    importBatches: mapFromSql<DomainDataset["importBatches"]>(importBatches),
    transactions: mapFromSql<DomainDataset["transactions"]>(transactions),
    categories: mapFromSql<DomainDataset["categories"]>(categories),
    rules: mapFromSql<DomainDataset["rules"]>(rules),
    auditEvents: mapFromSql<DomainDataset["auditEvents"]>(auditEvents),
    jobs: mapFromSql<DomainDataset["jobs"]>(jobs),
    accountBalanceSnapshots: mapFromSql<
      DomainDataset["accountBalanceSnapshots"]
    >(accountBalanceSnapshots),
    securities: mapFromSql<DomainDataset["securities"]>(securities),
    securityAliases:
      mapFromSql<DomainDataset["securityAliases"]>(securityAliases),
    securityPrices: mapFromSql<DomainDataset["securityPrices"]>(securityPrices),
    fxRates: mapFromSql<DomainDataset["fxRates"]>(fxRates),
    holdingAdjustments:
      mapFromSql<DomainDataset["holdingAdjustments"]>(holdingAdjustments),
    investmentPositions:
      mapFromSql<DomainDataset["investmentPositions"]>(investmentPositions),
    dailyPortfolioSnapshots: mapFromSql<
      DomainDataset["dailyPortfolioSnapshots"]
    >(dailyPortfolioSnapshots),
    monthlyCashFlowRollups: mapFromSql<DomainDataset["monthlyCashFlowRollups"]>(
      monthlyCashFlowRollups,
    ),
  };
}

export async function beginRevolutAuthorization(input: { entityId: string }) {
  const config = getRevolutRuntimeConfig();
  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const entityRows = await sql`
      select *
      from public.entities
      where id = ${input.entityId}
        and user_id = ${userId}
      limit 1
    `;
    const entity = entityRows[0];
    if (!entity) {
      throw new Error(`Entity ${input.entityId} was not found.`);
    }
    if (entity.entity_kind !== "company") {
      throw new Error(
        "Revolut Business connections can only be attached to company entities.",
      );
    }

    const state = createSignedRevolutState(config, {
      userId,
      entityId: input.entityId,
    });

    return {
      url: buildRevolutAuthorizationUrl(config, state),
      state,
    };
  });
}

export async function completeRevolutAuthorization(input: {
  code: string;
  state: string;
}) {
  const config = getRevolutRuntimeConfig();
  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const statePayload = verifySignedRevolutState(config, input.state);
    const entityId =
      typeof statePayload.entityId === "string" ? statePayload.entityId : "";
    const stateUserId =
      typeof statePayload.userId === "string" ? statePayload.userId : "";
    if (!entityId || stateUserId !== userId) {
      throw new Error("Revolut OAuth state is invalid for this user session.");
    }

    const tokens = await exchangeRevolutAuthorizationCode(config, input.code);
    const revolutAccounts = await fetchRevolutAccounts(
      config,
      tokens.access_token,
    );
    const encryptedRefreshToken = tokens.refresh_token
      ? encryptBankSecret(config.masterKey, tokens.refresh_token)
      : null;
    if (!encryptedRefreshToken) {
      throw new Error("Revolut did not return a refresh token.");
    }

    const upsertedConnections = await sql`
      insert into public.bank_connections ${sql({
        id: randomUUID(),
        user_id: userId,
        entity_id: entityId,
        provider: REVOLUT_PROVIDER_NAME,
        connection_label: REVOLUT_CONNECTION_LABEL,
        status: "active",
        encrypted_refresh_token: encryptedRefreshToken,
        external_business_id: null,
        last_cursor_created_at: null,
        last_successful_sync_at: null,
        last_sync_queued_at: null,
        last_webhook_at: null,
        auth_expires_at: new Date(
          Date.now() + tokens.expires_in * 1000,
        ).toISOString(),
        last_error: null,
        metadata_json: serializeJson(sql, {
          scopes: ["READ"],
          connectedVia: "oauth_callback",
        }),
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      } as Record<string, unknown>)}
      on conflict (user_id, provider, entity_id)
      do update set
        connection_label = excluded.connection_label,
        status = excluded.status,
        encrypted_refresh_token = excluded.encrypted_refresh_token,
        auth_expires_at = excluded.auth_expires_at,
        last_error = excluded.last_error,
        metadata_json = excluded.metadata_json,
        updated_at = excluded.updated_at
      returning id
    `;
    const connectionId = String(upsertedConnections[0]?.id ?? "");
    if (!connectionId) {
      throw new Error("Failed to persist the Revolut bank connection.");
    }

    let dataset = await loadDatasetForUser(sql, userId);
    const linked = await resolveOrCreateRevolutAccountLinks(sql, {
      userId,
      dataset,
      connectionId,
      entityId,
      revolutAccounts,
      actorName: "web-revolut-connect",
      sourceChannel: "web",
    });
    dataset = linked.dataset;

    await queueUniqueRevolutSyncJob(sql, {
      userId,
      connectionId,
      trigger: "oauth_callback",
    });

    return {
      connectionId,
      linkedAccountIds: linked.bankAccountLinks.map((link) => link.accountId),
    };
  });
}

export async function queueRevolutConnectionSync(input: {
  connectionId: string;
  trigger: Exclude<BankSyncTrigger, "oauth_callback" | "scheduled">;
}) {
  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const connectionRows = await sql`
      select id
      from public.bank_connections
      where id = ${input.connectionId}
        and user_id = ${userId}
        and provider = ${REVOLUT_PROVIDER_NAME}
      limit 1
    `;
    if (!connectionRows[0]) {
      throw new Error(`Bank connection ${input.connectionId} was not found.`);
    }

    return queueUniqueRevolutSyncJob(sql, {
      userId,
      connectionId: input.connectionId,
      trigger: input.trigger,
    });
  });
}

export async function processRevolutWebhookEvent(input: {
  headers: Record<string, string | null | undefined>;
  body: string;
}) {
  const config = getRevolutRuntimeConfig();
  const timestamp =
    input.headers["revolut-request-timestamp"] ??
    input.headers["Revolut-Request-Timestamp"] ??
    null;
  const signature =
    input.headers["revolut-signature"] ??
    input.headers["Revolut-Signature"] ??
    null;
  if (!config.webhookSigningSecret) {
    throw new Error(
      "REVOLUT_WEBHOOK_SIGNING_SECRET is required to validate Revolut webhooks.",
    );
  }
  if (!timestamp || !signature) {
    throw new Error("Revolut webhook is missing signature headers.");
  }
  if (!verifyRevolutWebhookTimestamp(timestamp)) {
    throw new Error("Revolut webhook timestamp is outside the allowed window.");
  }
  if (
    !verifyRevolutWebhookSignature({
      signingSecret: config.webhookSigningSecret,
      timestamp,
      signatureHeader: signature,
      body: input.body,
    })
  ) {
    throw new Error("Revolut webhook signature verification failed.");
  }

  return withSeededUserContext(async (sql) => {
    const userId = getDbRuntimeConfig().seededUserId;
    const connectionRows = await sql`
      select id
      from public.bank_connections
      where user_id = ${userId}
        and provider = ${REVOLUT_PROVIDER_NAME}
        and status = ${"active"}
    `;
    const queuedConnectionIds: string[] = [];
    for (const row of connectionRows) {
      const connectionId = String(row.id ?? "");
      if (!connectionId) {
        continue;
      }
      const queued = await queueUniqueRevolutSyncJob(sql, {
        userId,
        connectionId,
        trigger: "webhook",
      });
      if (queued.queued) {
        queuedConnectionIds.push(connectionId);
      }
      await sql`
        update public.bank_connections
        set last_webhook_at = ${new Date().toISOString()},
            updated_at = ${new Date().toISOString()}
        where id = ${connectionId}
          and user_id = ${userId}
      `;
    }

    return {
      accepted: true,
      queuedConnectionIds,
    };
  });
}

async function applyInvestmentRebuild(
  sql: SqlClient,
  userId: string,
  options?: {
    onProgress?: (progress: InvestmentRebuildProgress) => Promise<void> | void;
    historicalLookupTransactionIds?: readonly string[];
  },
) {
  return withInvestmentMutationLock(sql, userId, async () => {
    const latestDataset = await loadDatasetForUser(sql, userId);
    const referenceDate = getDatasetLatestDate(latestDataset);
    const rebuilt = await prepareInvestmentRebuild(
      latestDataset,
      referenceDate,
      {
        onProgress: options?.onProgress,
        historicalLookupTransactionIds: options?.historicalLookupTransactionIds,
      },
    );
    const latestTransactionsById = new Map(
      latestDataset.transactions.map((transaction) => [
        transaction.id,
        transaction,
      ]),
    );

    for (const security of rebuilt.insertedSecurities) {
      await sql`
      insert into public.securities ${sql({
        id: security.id,
        provider_name: security.providerName,
        provider_symbol: security.providerSymbol,
        canonical_symbol: security.canonicalSymbol,
        display_symbol: security.displaySymbol,
        name: security.name,
        exchange_name: security.exchangeName,
        mic_code: security.micCode,
        asset_type: security.assetType,
        quote_currency: security.quoteCurrency,
        country: security.country,
        isin: security.isin,
        figi: security.figi,
        active: security.active,
        metadata_json: serializeJson(sql, security.metadataJson),
        last_price_refresh_at: security.lastPriceRefreshAt,
        created_at: security.createdAt,
      } as Record<string, unknown>)}
      on conflict (provider_name, provider_symbol) do nothing
    `;
    }

    for (const alias of rebuilt.insertedAliases) {
      await sql`
      insert into public.security_aliases ${sql({
        id: alias.id,
        security_id: alias.securityId,
        alias_text_normalized: alias.aliasTextNormalized,
        alias_source: alias.aliasSource,
        template_id: alias.templateId,
        confidence: alias.confidence,
        created_at: alias.createdAt,
      } as Record<string, unknown>)}
      on conflict (security_id, alias_text_normalized) do nothing
    `;
    }

    for (const price of rebuilt.upsertedPrices) {
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

    for (const patch of rebuilt.transactionPatches) {
      const updatePayload: Record<string, unknown> = {
        updated_at: new Date().toISOString(),
      };
      const existingTransaction = latestTransactionsById.get(patch.id) ?? null;
      if (patch.transactionClass !== undefined) {
        updatePayload.transaction_class = patch.transactionClass;
      }
      if (patch.categoryCode !== undefined) {
        updatePayload.category_code = patch.categoryCode;
      }
      if (patch.classificationStatus !== undefined) {
        updatePayload.classification_status = patch.classificationStatus;
      }
      if (patch.classificationSource !== undefined) {
        updatePayload.classification_source = patch.classificationSource;
      }
      if (patch.classificationConfidence !== undefined) {
        updatePayload.classification_confidence =
          patch.classificationConfidence;
      }
      if (patch.securityId !== undefined) {
        updatePayload.security_id = patch.securityId;
      }
      if (patch.quantity !== undefined) {
        updatePayload.quantity = patch.quantity;
      }
      if (patch.unitPriceOriginal !== undefined) {
        updatePayload.unit_price_original = patch.unitPriceOriginal;
      }
      if (patch.needsReview !== undefined) {
        updatePayload.needs_review = patch.needsReview;
      }
      if (patch.reviewReason !== undefined) {
        updatePayload.review_reason = patch.reviewReason;
      }
      const existingLlmPayload =
        readOptionalRecord(existingTransaction?.llmPayload) ?? {};
      const nextLlmPayload =
        patch.llmPayload || patch.rebuildEvidence
          ? {
              ...existingLlmPayload,
              ...(patch.llmPayload ?? {}),
              ...(patch.rebuildEvidence
                ? {
                    rebuildEvidence: {
                      ...(readOptionalRecord(
                        existingLlmPayload.rebuildEvidence,
                      ) ?? {}),
                      ...patch.rebuildEvidence,
                    },
                  }
                : {}),
            }
          : null;
      if (nextLlmPayload) {
        await updateTransactionRecord(sql, {
          userId,
          transactionId: patch.id,
          updatePayload,
          llmPayload: nextLlmPayload,
          returning: false,
        });
      } else {
        await updateTransactionRecord(sql, {
          userId,
          transactionId: patch.id,
          updatePayload,
          returning: false,
        });
      }
    }

    await sql`
    delete from public.daily_portfolio_snapshots
    where user_id = ${userId}
  `;
    await sql`
    delete from public.investment_positions
    where user_id = ${userId}
  `;

    for (const position of rebuilt.positions) {
      await sql`
      insert into public.investment_positions ${sql({
        user_id: position.userId,
        entity_id: position.entityId,
        account_id: position.accountId,
        security_id: position.securityId,
        open_quantity: position.openQuantity,
        open_cost_basis_eur: position.openCostBasisEur,
        avg_cost_eur: position.avgCostEur,
        realized_pnl_eur: position.realizedPnlEur,
        dividends_eur: position.dividendsEur,
        interest_eur: position.interestEur,
        fees_eur: position.feesEur,
        last_trade_date: position.lastTradeDate,
        last_rebuilt_at: position.lastRebuiltAt,
        provenance_json: serializeJson(sql, position.provenanceJson),
        unrealized_complete: position.unrealizedComplete,
      } as Record<string, unknown>)}
    `;
    }

    for (const snapshot of rebuilt.snapshots) {
      await sql`
      insert into public.daily_portfolio_snapshots ${sql({
        snapshot_date: snapshot.snapshotDate,
        user_id: snapshot.userId,
        entity_id: snapshot.entityId,
        account_id: snapshot.accountId,
        security_id: snapshot.securityId,
        market_value_eur: snapshot.marketValueEur,
        cost_basis_eur: snapshot.costBasisEur,
        unrealized_pnl_eur: snapshot.unrealizedPnlEur,
        cash_balance_eur: snapshot.cashBalanceEur,
        total_portfolio_value_eur: snapshot.totalPortfolioValueEur,
        generated_at: snapshot.generatedAt,
      } as Record<string, unknown>)}
    `;
    }

    return {
      referenceDate,
      rebuiltPositions: rebuilt.positions.length,
      rebuiltSnapshots: rebuilt.snapshots.length,
      updatedTransactions: rebuilt.transactionPatches.length,
      insertedSecurities: rebuilt.insertedSecurities.length,
      upsertedPrices: rebuilt.upsertedPrices.length,
    };
  });
}

export interface ReanalyzeTransactionReviewInput {
  transactionId: string;
  reviewContext: string;
  actorName: string;
  sourceChannel: AuditEvent["sourceChannel"];
  reviewMode?: ReviewReanalysisMode;
  onProgress?: (progress: ReviewReanalysisProgress) => Promise<void> | void;
}

export interface ReviewReanalysisProgress {
  stage:
    | "load_context"
    | "llm_reanalysis"
    | "apply_transaction_update"
    | "investment_rebuild"
    | "historical_price_lookup"
    | "metric_refresh"
    | "review_propagation";
  message: string;
  updatedAt?: string;
}

const REVIEW_REANALYZE_COMPARISON_FIELDS = [
  "transactionClass",
  "categoryCode",
  "merchantNormalized",
  "counterpartyName",
  "economicEntityId",
  "classificationStatus",
  "classificationSource",
  "classificationConfidence",
  "securityId",
  "quantity",
  "unitPriceOriginal",
  "needsReview",
  "reviewReason",
] as const;

function getReviewReanalyzeChangedFields(
  before: Transaction,
  after: Transaction,
) {
  return REVIEW_REANALYZE_COMPARISON_FIELDS.filter(
    (field) => before[field] !== after[field],
  );
}

async function loadSimilarResolvedTransactionsForResolvedReview(
  sql: SqlClient,
  input: {
    userId: string;
    sourceTransaction: Transaction;
    dataset: DomainDataset;
  },
): Promise<SimilarAccountTransactionPromptContext[]> {
  const candidateSeedRowsRaw = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where user_id = ${input.userId}
      and account_id = ${input.sourceTransaction.accountId}
      and id <> ${input.sourceTransaction.id}
      and coalesce(needs_review, false) = false
      and voided_at is null
  `;
  if (candidateSeedRowsRaw.length === 0) {
    return [];
  }

  const sourceSeedRows = await sql`
    select id, description_raw, description_embedding
    from public.transactions
    where id = ${input.sourceTransaction.id}
      and user_id = ${input.userId}
    limit 1
  `;
  const sourceSeedRow = sourceSeedRows[0]
    ? parseTransactionEmbeddingSeedRow(
        sourceSeedRows[0] as Record<string, unknown>,
      )
    : null;
  if (!sourceSeedRow) {
    return [];
  }

  const candidateSeedRows = candidateSeedRowsRaw.map((row) =>
    parseTransactionEmbeddingSeedRow(row as Record<string, unknown>),
  );
  try {
    await ensureTransactionDescriptionEmbeddings(sql, input.userId, [
      sourceSeedRow,
    ]);
    await ensureTransactionDescriptionEmbeddings(
      sql,
      input.userId,
      candidateSeedRows,
    );
  } catch {
    return [];
  }

  const sourceEmbeddingRows = await sql`
    select description_embedding
    from public.transactions
    where id = ${input.sourceTransaction.id}
      and user_id = ${input.userId}
    limit 1
  `;
  const sourceEmbedding = normalizeStoredVectorLiteral(
    sourceEmbeddingRows[0]?.description_embedding,
  );
  if (!sourceEmbedding) {
    return [];
  }

  const matches = await findSimilarResolvedTransactionsByDescriptionEmbedding(
    sql,
    {
      userId: input.userId,
      sourceTransactionId: input.sourceTransaction.id,
      accountId: input.sourceTransaction.accountId,
      sourceEmbedding,
      threshold: RESOLVED_REVIEW_SIMILARITY_THRESHOLD,
      limit: MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT,
    },
  );

  return matches
    .map((match) => {
      const transaction =
        input.dataset.transactions.find(
          (candidate) => candidate.id === match.transactionId,
        ) ?? null;
      return transaction
        ? buildResolvedReviewSimilarTransactionContext(
            transaction,
            match.similarity,
          )
        : null;
    })
    .filter((match): match is SimilarAccountTransactionPromptContext =>
      Boolean(match),
    );
}

export async function reanalyzeTransactionReview(
  input: ReanalyzeTransactionReviewInput,
) {
  const userId = getDbRuntimeConfig().seededUserId;
  await input.onProgress?.({
    stage: "load_context",
    message: "Loading transaction context.",
  });

  return withSeededUserContext(async (sql) => {
    const beforeRow = await selectTransactionRowById(
      sql,
      userId,
      input.transactionId,
    );
    if (!beforeRow) {
      throw new Error(`Transaction ${input.transactionId} not found.`);
    }

    const beforeTransaction = mapFromSql<Transaction>(beforeRow);
    const dataset = await loadDatasetForUser(sql, userId);
    const account = dataset.accounts.find(
      (candidate) => candidate.id === beforeTransaction.accountId,
    );
    if (!account) {
      throw new Error(
        `Account ${beforeTransaction.accountId} not found for review reanalysis.`,
      );
    }

    const normalizedReviewContext = input.reviewContext.trim();
    if (!normalizedReviewContext) {
      throw new Error("Review context cannot be empty.");
    }
    const reviewMode =
      input.reviewMode ??
      (beforeTransaction.needsReview
        ? "manual_review_update"
        : "manual_resolved_review");
    const promptOverrides = await loadPromptOverrides(sql, userId);
    const wasPendingReview = beforeTransaction.needsReview;
    const followUpJobs: ReviewReanalysisFollowUpJobRef[] = [];
    const analysisTransaction =
      reviewMode === "manual_resolved_review"
        ? buildResolvedReviewSeedTransaction(
            beforeTransaction,
            account.assetDomain,
          )
        : beforeTransaction;
    const similarResolvedTransactions =
      reviewMode === "manual_resolved_review"
        ? await loadSimilarResolvedTransactionsForResolvedReview(sql, {
            userId,
            sourceTransaction: beforeTransaction,
            dataset,
          })
        : undefined;

    await input.onProgress?.({
      stage: "llm_reanalysis",
      message:
        reviewMode === "manual_resolved_review"
          ? "Running a clean transaction reanalysis with similar resolved history."
          : "Running transaction analyzer with your review context.",
    });
    let afterTransaction: Transaction | null = null;
    let changedFields: string[] = [];
    let auditEvent: AuditEvent | null = null;

    await withInvestmentMutationLock(sql, userId, async () => {
      await input.onProgress?.({
        stage: "apply_transaction_update",
        message: "Applying analyzer results to the transaction.",
      });
      const { decision, afterRow: after } =
        await executeTransactionEnrichmentPipeline(sql, userId, {
          dataset,
          account,
          transaction: analysisTransaction,
          enrichmentOptions: {
            trigger: reviewMode,
            reviewContext: {
              userProvidedContext: normalizedReviewContext,
              previousReviewReason:
                reviewMode === "manual_resolved_review"
                  ? null
                  : (beforeTransaction.reviewReason ?? null),
              previousUserContext:
                reviewMode === "manual_resolved_review"
                  ? null
                  : (beforeTransaction.manualNotes ?? null),
              previousLlmPayload:
                reviewMode === "manual_resolved_review"
                  ? null
                  : beforeTransaction.llmPayload &&
                      typeof beforeTransaction.llmPayload === "object"
                    ? (beforeTransaction.llmPayload as Record<string, unknown>)
                    : null,
            },
            promptOverrides,
            similarAccountTransactions: similarResolvedTransactions,
          },
          updateOptions: {
            manualNotes: normalizedReviewContext,
          },
        });

      if (account.assetDomain === "investment") {
        await input.onProgress?.({
          stage: "investment_rebuild",
          message: "Rebuilding investment positions and fetching dated prices.",
        });
        await applyInvestmentRebuild(sql, userId, {
          onProgress: input.onProgress,
          historicalLookupTransactionIds: [input.transactionId],
        });
      }
      await input.onProgress?.({
        stage: "metric_refresh",
        message: "Refreshing portfolio metrics.",
      });
      const metricRefreshJobId = await queueJob(sql, "metric_refresh", {
        trigger: reviewMode,
        transactionId: input.transactionId,
        accountId: beforeTransaction.accountId,
      });
      followUpJobs.push({
        id: metricRefreshJobId,
        jobType: "metric_refresh",
      });

      const finalRow = await selectTransactionRowById(
        sql,
        userId,
        input.transactionId,
      );
      if (!finalRow) {
        throw new Error(
          `Transaction ${input.transactionId} disappeared after review reanalysis.`,
        );
      }
      afterTransaction = mapFromSql<Transaction>(finalRow);
      changedFields = getReviewReanalyzeChangedFields(
        beforeTransaction,
        afterTransaction,
      );
      auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "transactions.review_reanalyze",
        "transaction",
        input.transactionId,
        beforeRow,
        after,
      );
      await insertAuditEventRecord(
        sql,
        auditEvent,
        reviewMode === "manual_resolved_review"
          ? "Re-ran LLM classification for a previously resolved transaction from a clean baseline with similar resolved precedent context."
          : "Re-ran LLM classification for a single transaction with manual review context.",
      );
      if (
        wasPendingReview &&
        shouldQueueReviewPropagationAfterManualReview(
          account,
          beforeTransaction,
        )
      ) {
        await input.onProgress?.({
          stage: "review_propagation",
          message:
            "Propagating the correction to similar unresolved transactions.",
        });
        const reviewPropagationPayload = {
          sourceTransactionId: afterTransaction.id,
          accountId: afterTransaction.accountId,
          sourceAuditEventId: auditEvent.id,
        };
        if (await supportsJobType(sql, "review_propagation")) {
          const reviewPropagationJobId = await queueJob(
            sql,
            "review_propagation",
            reviewPropagationPayload,
          );
          followUpJobs.push({
            id: reviewPropagationJobId,
            jobType: "review_propagation",
          });
        } else {
          await processReviewPropagationJob(
            sql,
            userId,
            reviewPropagationPayload,
            promptOverrides,
          );
        }
      }
    });

    if (!afterTransaction || !auditEvent) {
      throw new Error(
        `Transaction ${input.transactionId} was not finalized after review reanalysis.`,
      );
    }

    return {
      applied: true,
      changed: changedFields.length > 0,
      changedFields,
      transaction: afterTransaction,
      auditEvent,
      followUpJobs,
    };
  });
}

export interface QueueTransactionReviewReanalysisInput {
  transactionId: string;
  reviewContext: string;
  actorName: string;
  sourceChannel: "web" | "cli" | "worker" | "system";
}

export interface ReviewReanalysisFollowUpJobRef {
  id: string;
  jobType: "metric_refresh" | "review_propagation";
}

export interface ReviewReanalysisFollowUpJobStatus extends ReviewReanalysisFollowUpJobRef {
  status: "queued" | "running" | "completed" | "failed";
  createdAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  lastError?: string | null;
}

export interface ReviewReanalysisJobStatus {
  id: string;
  jobType: string;
  status: "queued" | "running" | "completed" | "failed";
  createdAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  lastError?: string | null;
  payloadJson: Record<string, unknown>;
  followUpJobs: ReviewReanalysisFollowUpJobStatus[];
}

function normalizeJobProgress(value: unknown): ReviewReanalysisProgress | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const stage = typeof record.stage === "string" ? record.stage : null;
  const message = typeof record.message === "string" ? record.message : null;
  if (!stage || !message) {
    return null;
  }

  return {
    stage: stage as ReviewReanalysisProgress["stage"],
    message,
    updatedAt:
      typeof record.updatedAt === "string" ? record.updatedAt : undefined,
  };
}

function normalizeReviewReanalysisFollowUpJobRef(
  value: unknown,
): ReviewReanalysisFollowUpJobRef | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const id = typeof record.id === "string" ? record.id : null;
  const jobType =
    record.jobType === "metric_refresh" ||
    record.jobType === "review_propagation"
      ? record.jobType
      : null;
  if (!id || !jobType) {
    return null;
  }

  return {
    id,
    jobType,
  };
}

function normalizeReviewReanalysisFollowUpJobs(
  value: unknown,
): ReviewReanalysisFollowUpJobRef[] {
  return (readUnknownArray(value) ?? [])
    .map((entry) => normalizeReviewReanalysisFollowUpJobRef(entry))
    .filter((entry): entry is ReviewReanalysisFollowUpJobRef => Boolean(entry));
}

async function readJobById(sql: SqlClient, jobId: string) {
  const rows = await sql`
    select *
    from public.jobs
    where id = ${jobId}
    limit 1
  `;
  return rows[0] ? mapFromSql<DomainDataset["jobs"]>(rows)[0] : null;
}

async function readReviewReanalysisFollowUpJobStatuses(
  sql: SqlClient,
  refs: ReviewReanalysisFollowUpJobRef[],
): Promise<ReviewReanalysisFollowUpJobStatus[]> {
  const jobs: ReviewReanalysisFollowUpJobStatus[] = [];

  for (const ref of refs) {
    const job = await readJobById(sql, ref.id);
    if (!job) {
      continue;
    }

    jobs.push({
      id: job.id,
      jobType: ref.jobType,
      status: job.status,
      createdAt: job.createdAt,
      startedAt: job.startedAt ?? null,
      finishedAt: job.finishedAt ?? null,
      lastError: job.lastError ?? null,
    });
  }

  return jobs;
}

async function acquireReviewReanalysisQueueLock(
  sql: SqlClient,
  transactionId: string,
) {
  await sql`
    select pg_advisory_xact_lock(
      hashtext(${"review_reanalyze"}),
      hashtext(${transactionId})
    )
  `;
}

async function acquireInvestmentMutationLock(sql: SqlClient, userId: string) {
  await sql`
    select pg_advisory_lock(
      hashtext(${"investment_mutation"}),
      hashtext(${userId})
    )
  `;
}

async function releaseInvestmentMutationLock(sql: SqlClient, userId: string) {
  await sql`
    select pg_advisory_unlock(
      hashtext(${"investment_mutation"}),
      hashtext(${userId})
    )
  `;
}

async function withInvestmentMutationLock<T>(
  sql: SqlClient,
  userId: string,
  runner: () => Promise<T>,
) {
  await acquireInvestmentMutationLock(sql, userId);
  try {
    return await runner();
  } finally {
    await releaseInvestmentMutationLock(sql, userId);
  }
}

export async function queueTransactionReviewReanalysis(
  input: QueueTransactionReviewReanalysisInput,
) {
  const userId = getDbRuntimeConfig().seededUserId;

  return withSeededUserContext(async (sql) => {
    const transactionRows = await sql`
      select id, account_id, needs_review
      from public.transactions
      where id = ${input.transactionId}
        and user_id = ${userId}
      limit 1
    `;
    const transactionRow = transactionRows[0];
    if (!transactionRow) {
      throw new Error(`Transaction ${input.transactionId} not found.`);
    }
    const reviewMode: ReviewReanalysisMode =
      transactionRow.needs_review === true
        ? "manual_review_update"
        : "manual_resolved_review";

    const normalizedReviewContext = input.reviewContext.trim();
    if (!normalizedReviewContext) {
      throw new Error("Review context cannot be empty.");
    }

    await acquireReviewReanalysisQueueLock(sql, input.transactionId);

    const existingRows = await sql`
      select *
      from public.jobs
      where job_type = ${"review_reanalyze"}
        and status in (${"queued"}, ${"running"})
        and payload_json->>'transactionId' = ${input.transactionId}
      order by created_at desc
      limit 1
    `;
    const existingJob = existingRows[0]
      ? mapFromSql<DomainDataset["jobs"]>(existingRows)[0]
      : null;
    if (existingJob) {
      return {
        queued: false,
        jobId: existingJob.id,
        status: existingJob.status,
      };
    }

    const jobId = randomUUID();
    await sql`
      insert into public.jobs (
        id,
        job_type,
        payload_json,
        status,
        attempts,
        available_at
      ) values (
        ${jobId},
        ${"review_reanalyze"},
        ${serializeJson(sql, {
          transactionId: input.transactionId,
          reviewContext: normalizedReviewContext,
          reviewMode,
          actorName: input.actorName,
          sourceChannel: input.sourceChannel,
        })}::jsonb,
        ${"queued"},
        0,
        ${new Date().toISOString()}
      )
    `;

    return {
      queued: true,
      jobId,
      status: "queued" as const,
    };
  });
}

export async function getReviewReanalysisJobStatus(jobId: string) {
  const userId = getDbRuntimeConfig().seededUserId;

  return withSeededUserContext(async (sql) => {
    const rows = await sql`
      select *
      from public.jobs
      where id = ${jobId}
        and job_type = ${"review_reanalyze"}
      limit 1
    `;
    const job = rows[0] ? mapFromSql<DomainDataset["jobs"]>(rows)[0] : null;
    if (!job) {
      throw new Error(`Review job ${jobId} not found.`);
    }

    const transactionId =
      typeof job.payloadJson.transactionId === "string"
        ? job.payloadJson.transactionId
        : null;
    if (!transactionId) {
      throw new Error(`Review job ${jobId} is missing transaction context.`);
    }

    const transactionRows = await sql`
      select id
      from public.transactions
      where id = ${transactionId}
        and user_id = ${userId}
      limit 1
    `;
    if (!transactionRows[0]) {
      throw new Error(`Review job ${jobId} is not available for this user.`);
    }

    const followUpJobs = await readReviewReanalysisFollowUpJobStatuses(
      sql,
      normalizeReviewReanalysisFollowUpJobs(job.payloadJson.followUpJobs),
    );

    return {
      ...job,
      payloadJson: {
        ...job.payloadJson,
        progress: normalizeJobProgress(job.payloadJson.progress),
      },
      followUpJobs,
    } satisfies ReviewReanalysisJobStatus;
  });
}

class SqlFinanceRepository implements FinanceRepository {
  private userId = getDbRuntimeConfig().seededUserId;

  async getDataset(): Promise<DomainDataset> {
    return withSeededUserContext((sql) => loadDatasetForUser(sql, this.userId));
  }

  async updateWorkspaceProfile(input: UpdateWorkspaceProfileInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.profiles
        where id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Profile ${this.userId} was not found.`);
      }

      const afterJson = {
        ...mapFromSql<DomainDataset["profile"]>(beforeRow),
        displayName: input.profile.displayName,
        defaultBaseCurrency: input.profile.defaultBaseCurrency,
        timezone: input.profile.timezone,
        workspaceSettingsJson: input.profile.workspaceSettingsJson,
      };

      if (input.apply) {
        await sql`
          update public.profiles
          set
            display_name = ${input.profile.displayName},
            default_base_currency = ${input.profile.defaultBaseCurrency},
            timezone = ${input.profile.timezone},
            workspace_settings_json = ${serializeJson(
              sql,
              input.profile.workspaceSettingsJson,
            )}::jsonb
          where id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "profile.update",
          "profile",
          this.userId,
          mapFromSql<DomainDataset["profile"]>(beforeRow) as unknown as Record<
            string,
            unknown
          >,
          afterJson as unknown as Record<string, unknown>,
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: "Updated workspace profile defaults.",
          } as Record<string, unknown>)}
        `;
      }

      return {
        applied: input.apply,
        profileId: this.userId,
      };
    });
  }

  async createEntity(input: CreateEntityInput) {
    return withSeededUserContext(async (sql) => {
      const existingSlugRows = await sql`
        select id
        from public.entities
        where user_id = ${this.userId}
          and slug = ${input.entity.slug}
        limit 1
      `;
      if (existingSlugRows[0]) {
        throw new Error(
          `Entity slug "${input.entity.slug}" is already in use.`,
        );
      }

      if (input.entity.entityKind === "personal") {
        const personalRows = await sql`
          select id
          from public.entities
          where user_id = ${this.userId}
            and entity_kind = 'personal'
          limit 1
        `;
        if (personalRows[0]) {
          throw new Error(
            "A personal entity already exists. Add companies instead of creating a second personal owner.",
          );
        }
      }

      const entityId = randomUUID();
      const afterJson = {
        id: entityId,
        userId: this.userId,
        slug: input.entity.slug,
        displayName: input.entity.displayName,
        legalName: input.entity.legalName ?? null,
        entityKind: input.entity.entityKind,
        baseCurrency: input.entity.baseCurrency,
        active: true,
      };

      if (input.apply) {
        await sql`
          insert into public.entities (
            id,
            user_id,
            slug,
            display_name,
            legal_name,
            entity_kind,
            base_currency,
            active
          ) values (
            ${entityId},
            ${this.userId},
            ${input.entity.slug},
            ${input.entity.displayName},
            ${input.entity.legalName ?? null},
            ${input.entity.entityKind},
            ${input.entity.baseCurrency},
            true
          )
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "entities.create",
          "entity",
          entityId,
          null,
          afterJson,
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: `Created entity ${input.entity.displayName}.`,
          } as Record<string, unknown>)}
        `;
      }

      return {
        applied: input.apply,
        entityId,
      };
    });
  }

  async updateEntity(input: UpdateEntityInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.entities
        where id = ${input.entityId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Entity ${input.entityId} not found.`);
      }

      const nextSlug = input.patch.slug ?? beforeRow.slug;
      if (nextSlug !== beforeRow.slug) {
        const duplicateSlugRows = await sql`
          select id
          from public.entities
          where user_id = ${this.userId}
            and slug = ${nextSlug}
            and id <> ${input.entityId}
          limit 1
        `;
        if (duplicateSlugRows[0]) {
          throw new Error(`Entity slug "${nextSlug}" is already in use.`);
        }
      }

      const afterJson = {
        ...mapFromSql<DomainDataset["entities"][number]>(beforeRow),
        slug: nextSlug,
        displayName: input.patch.displayName ?? beforeRow.display_name,
        legalName: Object.prototype.hasOwnProperty.call(
          input.patch,
          "legalName",
        )
          ? (input.patch.legalName ?? null)
          : beforeRow.legal_name,
        baseCurrency: input.patch.baseCurrency ?? beforeRow.base_currency,
      };

      if (input.apply) {
        await sql`
          update public.entities
          set
            slug = ${nextSlug},
            display_name = ${input.patch.displayName ?? beforeRow.display_name},
            legal_name = ${
              Object.prototype.hasOwnProperty.call(input.patch, "legalName")
                ? (input.patch.legalName ?? null)
                : beforeRow.legal_name
            },
            base_currency = ${input.patch.baseCurrency ?? beforeRow.base_currency}
          where id = ${input.entityId}
            and user_id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "entities.update",
          "entity",
          input.entityId,
          mapFromSql<DomainDataset["entities"][number]>(
            beforeRow,
          ) as unknown as Record<string, unknown>,
          afterJson as unknown as Record<string, unknown>,
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: `Updated entity ${afterJson.displayName}.`,
          } as Record<string, unknown>)}
        `;
      }

      return {
        applied: input.apply,
        entityId: input.entityId,
      };
    });
  }

  async deleteEntity(input: DeleteEntityInput) {
    return withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.entities
        where id = ${input.entityId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Entity ${input.entityId} not found.`);
      }
      if (beforeRow.entity_kind === "personal") {
        throw new Error(
          "The personal entity cannot be deleted. Keep one personal owner and add or remove company entities around it.",
        );
      }

      const blockers = await sql`
        with target as (
          select ${input.entityId}::uuid as entity_id
        )
        select
          (select count(*)::int from public.accounts where entity_id = target.entity_id) as accounts,
          (
            select count(*)::int
            from public.transactions
            where economic_entity_id = target.entity_id
               or account_entity_id = target.entity_id
          ) as transactions,
          (
            select count(*)::int
            from public.holding_adjustments
            where entity_id = target.entity_id
          ) as holding_adjustments,
          (
            select count(*)::int
            from public.investment_positions
            where entity_id = target.entity_id
          ) as investment_positions,
          (
            select count(*)::int
            from public.daily_portfolio_snapshots
            where entity_id = target.entity_id
          ) as portfolio_snapshots
        from target
      `;
      const blockerRow = blockers[0] as Record<string, number>;
      const activeBlockers = Object.entries(blockerRow).filter(
        ([, count]) => Number(count) > 0,
      );
      if (activeBlockers.length > 0) {
        throw new Error(
          `Entity cannot be removed because it is still referenced by ${activeBlockers
            .map(([key, count]) => `${key.replace(/_/g, " ")} (${count})`)
            .join(", ")}.`,
        );
      }

      if (input.apply) {
        await sql`
          delete from public.entities
          where id = ${input.entityId}
            and user_id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "entities.delete",
          "entity",
          input.entityId,
          mapFromSql<DomainDataset["entities"][number]>(
            beforeRow,
          ) as unknown as Record<string, unknown>,
          null,
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: `Removed entity ${beforeRow.display_name}.`,
          } as Record<string, unknown>)}
        `;
      }

      return {
        applied: input.apply,
        entityId: input.entityId,
      };
    });
  }

  async createAccount(input: CreateAccountInput) {
    const result = await withSeededUserContext(async (sql) => {
      const entityRows = await sql`
        select * from public.entities
        where id = ${input.account.entityId}
          and user_id = ${this.userId}
        limit 1
      `;
      if (!entityRows[0]) {
        throw new Error(`Entity ${input.account.entityId} not found.`);
      }

      if (input.account.importTemplateDefaultId) {
        const templateRows = await sql`
          select compatible_account_type from public.import_templates
          where id = ${input.account.importTemplateDefaultId}
            and user_id = ${this.userId}
          limit 1
        `;
        const template = templateRows[0];
        if (!template) {
          throw new Error(
            `Template ${input.account.importTemplateDefaultId} not found.`,
          );
        }
        if (template.compatible_account_type !== input.account.accountType) {
          throw new Error(
            `Template ${input.account.importTemplateDefaultId} is not compatible with ${input.account.accountType}.`,
          );
        }
      }

      const accountId = randomUUID();
      const afterJson = {
        id: accountId,
        userId: this.userId,
        ...input.account,
        importTemplateDefaultId: input.account.importTemplateDefaultId ?? null,
        openingBalanceOriginal: input.account.openingBalanceOriginal ?? null,
        openingBalanceCurrency: input.account.openingBalanceCurrency ?? null,
        openingBalanceDate: input.account.openingBalanceDate ?? null,
        accountSuffix: input.account.accountSuffix ?? null,
        staleAfterDays: input.account.staleAfterDays ?? null,
        lastImportedAt: null,
      };

      if (input.apply) {
        await sql`
          insert into public.accounts (
            id,
            user_id,
            entity_id,
            institution_name,
            display_name,
            account_type,
            asset_domain,
            default_currency,
            opening_balance_original,
            opening_balance_currency,
            opening_balance_date,
            include_in_consolidation,
            is_active,
            import_template_default_id,
            matching_aliases,
            account_suffix,
            balance_mode,
            stale_after_days
          ) values (
            ${accountId},
            ${this.userId},
            ${input.account.entityId},
            ${input.account.institutionName},
            ${input.account.displayName},
            ${input.account.accountType},
            ${input.account.assetDomain},
            ${input.account.defaultCurrency},
            ${input.account.openingBalanceOriginal ?? null},
            ${input.account.openingBalanceCurrency ?? null},
            ${input.account.openingBalanceDate ?? null},
            ${input.account.includeInConsolidation},
            ${input.account.isActive},
            ${input.account.importTemplateDefaultId ?? null},
            ${input.account.matchingAliases},
            ${input.account.accountSuffix ?? null},
            ${input.account.balanceMode},
            ${input.account.staleAfterDays ?? null}
          )
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "accounts.create",
          "account",
          accountId,
          null,
          afterJson,
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;
      }

      return { applied: input.apply, accountId };
    });

    return result;
  }

  async updateAccount(input: UpdateAccountInput) {
    const result = await withSeededUserContext(async (sql) => {
      const beforeRows = await sql`
        select *
        from public.accounts
        where id = ${input.accountId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = beforeRows[0];
      if (!beforeRow) {
        throw new Error(`Account ${input.accountId} not found.`);
      }

      const hasTemplatePatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "importTemplateDefaultId",
      );
      const nextImportTemplateDefaultId = hasTemplatePatch
        ? (input.patch.importTemplateDefaultId ?? null)
        : beforeRow.import_template_default_id;

      if (nextImportTemplateDefaultId) {
        const templateRows = await sql`
          select compatible_account_type
          from public.import_templates
          where id = ${nextImportTemplateDefaultId}
            and user_id = ${this.userId}
          limit 1
        `;
        const template = templateRows[0];
        if (!template) {
          throw new Error(`Template ${nextImportTemplateDefaultId} not found.`);
        }
        if (template.compatible_account_type !== beforeRow.account_type) {
          throw new Error(
            `Template ${nextImportTemplateDefaultId} is not compatible with ${beforeRow.account_type}.`,
          );
        }
      }

      const hasOpeningBalanceOriginalPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "openingBalanceOriginal",
      );
      const hasOpeningBalanceDatePatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "openingBalanceDate",
      );
      const hasAliasesPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "matchingAliases",
      );
      const hasIncludeInConsolidationPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "includeInConsolidation",
      );
      const hasAccountSuffixPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "accountSuffix",
      );
      const hasStaleAfterDaysPatch = Object.prototype.hasOwnProperty.call(
        input.patch,
        "staleAfterDays",
      );

      const nextInstitutionName =
        input.patch.institutionName ?? beforeRow.institution_name;
      const nextDisplayName = input.patch.displayName ?? beforeRow.display_name;
      const nextDefaultCurrency =
        input.patch.defaultCurrency ?? beforeRow.default_currency;
      const nextOpeningBalanceOriginal = hasOpeningBalanceOriginalPatch
        ? (input.patch.openingBalanceOriginal ?? null)
        : beforeRow.opening_balance_original;
      const nextOpeningBalanceDate = nextOpeningBalanceOriginal
        ? hasOpeningBalanceDatePatch
          ? (input.patch.openingBalanceDate ?? null)
          : beforeRow.opening_balance_date
        : null;
      const nextIncludeInConsolidation = hasIncludeInConsolidationPatch
        ? input.patch.includeInConsolidation
        : beforeRow.include_in_consolidation;
      const nextMatchingAliases = hasAliasesPatch
        ? (input.patch.matchingAliases ?? [])
        : beforeRow.matching_aliases;
      const nextAccountSuffix = hasAccountSuffixPatch
        ? (input.patch.accountSuffix ?? null)
        : beforeRow.account_suffix;
      const nextBalanceMode = input.patch.balanceMode ?? beforeRow.balance_mode;
      const nextStaleAfterDays = hasStaleAfterDaysPatch
        ? (input.patch.staleAfterDays ?? null)
        : beforeRow.stale_after_days;

      const beforeAccount = mapFromSql<DomainDataset["accounts"][number]>(
        beforeRow,
      );
      const afterJson = {
        ...beforeAccount,
        institutionName: nextInstitutionName,
        displayName: nextDisplayName,
        defaultCurrency: nextDefaultCurrency,
        openingBalanceOriginal: nextOpeningBalanceOriginal,
        openingBalanceCurrency: nextOpeningBalanceOriginal
          ? nextDefaultCurrency
          : null,
        openingBalanceDate: nextOpeningBalanceDate,
        includeInConsolidation: nextIncludeInConsolidation,
        importTemplateDefaultId: nextImportTemplateDefaultId,
        matchingAliases: nextMatchingAliases,
        accountSuffix: nextAccountSuffix,
        balanceMode: nextBalanceMode,
        staleAfterDays: nextStaleAfterDays,
      };

      if (input.apply) {
        await sql`
          update public.accounts
          set
            institution_name = ${nextInstitutionName},
            display_name = ${nextDisplayName},
            default_currency = ${nextDefaultCurrency},
            opening_balance_original = ${nextOpeningBalanceOriginal},
            opening_balance_currency = ${
              nextOpeningBalanceOriginal ? nextDefaultCurrency : null
            },
            opening_balance_date = ${nextOpeningBalanceDate},
            include_in_consolidation = ${nextIncludeInConsolidation},
            import_template_default_id = ${nextImportTemplateDefaultId},
            matching_aliases = ${nextMatchingAliases},
            account_suffix = ${nextAccountSuffix},
            balance_mode = ${nextBalanceMode},
            stale_after_days = ${nextStaleAfterDays}
          where id = ${input.accountId}
            and user_id = ${this.userId}
        `;

        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "accounts.update",
          "account",
          input.accountId,
          beforeAccount as unknown as Record<string, unknown>,
          afterJson as unknown as Record<string, unknown>,
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: `Updated account ${afterJson.displayName}.`,
          } as Record<string, unknown>)}
        `;
      }

      return { applied: input.apply, accountId: input.accountId };
    });

    return result;
  }

  async deleteAccount(input: DeleteAccountInput) {
    const result = await withSeededUserContext(async (sql) => {
      const before = await sql`
        select * from public.accounts
        where id = ${input.accountId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = before[0];
      if (!beforeRow) {
        throw new Error(`Account ${input.accountId} not found.`);
      }

      const blockers = await sql`
        with target as (
          select ${input.accountId}::uuid as account_id
        )
        select
          (select count(*)::int from public.import_batches where account_id = target.account_id) as import_batches,
          (
            select count(*)::int
            from public.transactions
            where account_id = target.account_id
               or related_account_id = target.account_id
          ) as transactions,
          (select count(*)::int from public.account_balance_snapshots where account_id = target.account_id) as balance_snapshots,
          (select count(*)::int from public.holding_adjustments where account_id = target.account_id) as holding_adjustments,
          (select count(*)::int from public.investment_positions where account_id = target.account_id) as investment_positions,
          (select count(*)::int from public.daily_portfolio_snapshots where account_id = target.account_id) as portfolio_snapshots
        from target
      `;
      const blockerRow = blockers[0] as Record<string, number>;
      const activeBlockers = Object.entries(blockerRow).filter(
        ([, count]) => Number(count) > 0,
      );
      if (activeBlockers.length > 0) {
        throw new Error(
          `Account cannot be removed because it already has dependent data: ${activeBlockers
            .map(([key, count]) => `${key.replace(/_/g, " ")} (${count})`)
            .join(", ")}.`,
        );
      }

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "accounts.delete",
        "account",
        input.accountId,
        beforeRow,
        null,
      );

      if (input.apply) {
        await sql`
          delete from public.accounts
          where id = ${input.accountId}
            and user_id = ${this.userId}
        `;
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;
      }

      return { applied: input.apply, accountId: input.accountId };
    });

    return result;
  }

  async resetWorkspace(
    input: ResetWorkspaceInput,
  ): Promise<ResetWorkspaceResult> {
    const result = await withSeededUserContext(async (sql) => {
      const [
        portfolioSnapshots,
        investmentPositions,
        holdingAdjustments,
        balanceSnapshots,
      ] = await Promise.all([
        sql`
            delete from public.daily_portfolio_snapshots
            where user_id = ${this.userId}
            returning id
          `,
        sql`
            delete from public.investment_positions
            where user_id = ${this.userId}
            returning account_id
          `,
        sql`
            delete from public.holding_adjustments
            where user_id = ${this.userId}
            returning id
          `,
        sql`
            delete from public.account_balance_snapshots
            where account_id in (
              select id from public.accounts where user_id = ${this.userId}
            )
            returning account_id
          `,
      ]);

      const [transactions, importBatches, rules, accounts, importTemplates] =
        await Promise.all([
          sql`
            delete from public.transactions
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.import_batches
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.classification_rules
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.accounts
            where user_id = ${this.userId}
            returning id
          `,
          sql`
            delete from public.import_templates
            where user_id = ${this.userId}
            returning id
          `,
        ]);

      const jobs = await sql`
          delete from public.jobs
          returning id
      `;

      await sql`
        delete from public.audit_events
        where actor_id = ${this.userId}
           or object_type in ('account', 'classification_rule', 'job', 'import_template', 'transaction')
      `;

      const deleted = {
        accounts: accounts.length,
        importTemplates: importTemplates.length,
        importBatches: importBatches.length,
        transactions: transactions.length,
        balanceSnapshots: balanceSnapshots.length,
        holdingAdjustments: holdingAdjustments.length,
        investmentPositions: investmentPositions.length,
        portfolioSnapshots: portfolioSnapshots.length,
        rules: rules.length,
        jobs: jobs.length,
      };

      if (input.apply) {
        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "workspace.reset",
          "workspace",
          this.userId,
          null,
          deleted,
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes:
              "Cleared seeded demo finance data for a fresh local workspace.",
          } as Record<string, unknown>)}
        `;
      }

      return {
        applied: input.apply,
        deleted,
      };
    });

    return result;
  }

  async updateTransaction(input: UpdateTransactionInput): Promise<{
    applied: boolean;
    transaction: Transaction;
    auditEvent: AuditEvent;
    generatedRuleId?: string;
  }> {
    const result = await withSeededUserContext(async (sql) => {
      const beforeRow = await selectTransactionRowById(
        sql,
        this.userId,
        input.transactionId,
      );
      if (!beforeRow) {
        throw new Error(`Transaction ${input.transactionId} not found.`);
      }

      const patch = camelizeValue(input.patch);
      const requiresClassificationValidation =
        patch.transactionClass !== undefined ||
        patch.categoryCode !== undefined ||
        patch.economicEntityId !== undefined ||
        input.createRuleFromTransaction === true;
      const dataset = requiresClassificationValidation
        ? await loadDatasetForUser(sql, this.userId)
        : null;
      const account =
        dataset?.accounts.find((candidate) => candidate.id === beforeRow.account_id) ??
        null;
      if (requiresClassificationValidation && !account) {
        throw new Error(
          `Account ${beforeRow.account_id} was not found for transaction ${input.transactionId}.`,
        );
      }

      if (account && patch.transactionClass !== undefined) {
        assertTransactionClassAllowedForAccount(
          account,
          patch.transactionClass,
          "Manual transaction class",
        );
      }
      if (dataset && account && patch.categoryCode !== undefined && patch.categoryCode) {
        assertCategoryCodeAllowedForAccount(
          dataset,
          account,
          patch.categoryCode,
          "Manual category",
        );
      }
      if (
        dataset &&
        account &&
        patch.economicEntityId !== undefined &&
        patch.economicEntityId
      ) {
        assertEconomicEntityAllowedForAccount(
          dataset,
          account,
          patch.economicEntityId,
          "Manual economic entity",
        );
      }

      const updatePayload: Record<string, unknown> = {
        updated_at: new Date().toISOString(),
      };
      if (patch.transactionClass !== undefined)
        updatePayload.transaction_class = patch.transactionClass;
      if (patch.categoryCode !== undefined)
        updatePayload.category_code = patch.categoryCode;
      if (patch.economicEntityId !== undefined)
        updatePayload.economic_entity_id = patch.economicEntityId;
      if (patch.merchantNormalized !== undefined)
        updatePayload.merchant_normalized = patch.merchantNormalized;
      if (patch.counterpartyName !== undefined)
        updatePayload.counterparty_name = patch.counterpartyName;
      if (patch.needsReview !== undefined)
        updatePayload.needs_review = patch.needsReview;
      if (patch.reviewReason !== undefined)
        updatePayload.review_reason = patch.reviewReason;
      if (patch.excludeFromAnalytics !== undefined)
        updatePayload.exclude_from_analytics = patch.excludeFromAnalytics;
      if (patch.securityId !== undefined)
        updatePayload.security_id = patch.securityId;
      if (patch.quantity !== undefined) updatePayload.quantity = patch.quantity;
      if (patch.unitPriceOriginal !== undefined)
        updatePayload.unit_price_original = patch.unitPriceOriginal;
      if (patch.manualNotes !== undefined)
        updatePayload.manual_notes = patch.manualNotes;
      if (Object.keys(input.patch).length > 0) {
        updatePayload.classification_status = "manual_override";
        updatePayload.classification_source = "manual";
        updatePayload.classification_confidence = 1;
      }

      const after = input.apply
        ? await sql`
            update public.transactions
            set ${sql(updatePayload)}
            where id = ${input.transactionId}
              and user_id = ${this.userId}
            returning ${transactionColumnsSql(sql)}
          `
        : [{ ...beforeRow, ...updatePayload }];

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "transactions.update",
        "transaction",
        input.transactionId,
        beforeRow,
        after[0],
      );

      let generatedRuleId: string | undefined;

      if (input.apply) {
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;

        const beforeTransaction = mapFromSql<Transaction>(beforeRow);
        const afterTransaction = mapFromSql<Transaction>(after[0]);
        if (
          beforeTransaction.accountId === afterTransaction.accountId &&
          (beforeTransaction.securityId !== afterTransaction.securityId ||
            beforeTransaction.quantity !== afterTransaction.quantity ||
            beforeTransaction.unitPriceOriginal !==
              afterTransaction.unitPriceOriginal ||
            beforeTransaction.transactionClass !==
              afterTransaction.transactionClass ||
            beforeTransaction.needsReview !== afterTransaction.needsReview ||
            beforeTransaction.excludeFromAnalytics !==
              afterTransaction.excludeFromAnalytics ||
            beforeTransaction.economicEntityId !==
              afterTransaction.economicEntityId)
        ) {
          const accountRows = await sql`
            select asset_domain from public.accounts
            where id = ${afterTransaction.accountId}
            limit 1
          `;
          if (accountRows[0]?.asset_domain === "investment") {
            await queueJob(sql, "position_rebuild", {
              accountId: afterTransaction.accountId,
              transactionId: afterTransaction.id,
              trigger: "transaction_update",
            });
          }
        }
        await queueJob(sql, "metric_refresh", {
          trigger: "transaction_update",
          transactionId: afterTransaction.id,
          accountId: afterTransaction.accountId,
        });

        if (input.createRuleFromTransaction) {
          const persistedTransaction = mapFromSql<Transaction>(after[0]);
          generatedRuleId = randomUUID();
          const persistedAccount =
            account ??
            (await loadDatasetForUser(sql, this.userId)).accounts.find(
              (candidate) => candidate.id === persistedTransaction.accountId,
            ) ??
            null;
          const outputsJson: Record<string, unknown> = {
            transaction_class: persistedTransaction.transactionClass,
            category_code: persistedTransaction.categoryCode,
          };
          if (
            persistedAccount &&
            persistedAccount.assetDomain !== "cash" &&
            persistedTransaction.economicEntityId !== persistedAccount.entityId
          ) {
            outputsJson.economic_entity_id_override =
              persistedTransaction.economicEntityId;
          }
          await sql`
            insert into public.classification_rules (
              id,
              user_id,
              priority,
              active,
              scope_json,
              conditions_json,
              outputs_json,
              created_from_transaction_id,
              auto_generated
            ) values (
              ${generatedRuleId},
              ${this.userId},
              50,
              true,
              ${serializeJson(sql, { account_id: persistedTransaction.accountId })}::jsonb,
              ${serializeJson(sql, {
                normalized_description_regex:
                  persistedTransaction.descriptionClean,
              })}::jsonb,
              ${serializeJson(sql, outputsJson)}::jsonb,
              ${persistedTransaction.id},
              true
            )
          `;
        }
      }

      return {
        applied: input.apply,
        transaction: mapFromSql<Transaction>(after[0]),
        auditEvent,
        generatedRuleId,
      };
    });

    return result;
  }

  async createRule(input: CreateRuleInput) {
    const result = await withSeededUserContext(async (sql) => {
      const ruleId = randomUUID();
      const dataset = await loadDatasetForUser(sql, this.userId);
      assertRuleOutputsAllowedForScope(
        dataset,
        input.scopeJson,
        input.outputsJson,
      );
      if (input.apply) {
        await sql`
          insert into public.classification_rules (
            id,
            user_id,
            priority,
            active,
            scope_json,
            conditions_json,
            outputs_json,
            auto_generated
          ) values (
            ${ruleId},
            ${this.userId},
            ${input.priority},
            true,
            ${serializeJson(sql, input.scopeJson)}::jsonb,
            ${serializeJson(sql, input.conditionsJson)}::jsonb,
            ${serializeJson(sql, input.outputsJson)}::jsonb,
            false
          )
        `;
        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "rules.create",
          "classification_rule",
          ruleId,
          null,
          {
            priority: input.priority,
            scopeJson: input.scopeJson,
            conditionsJson: input.conditionsJson,
            outputsJson: input.outputsJson,
          },
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;
      }
      return { applied: input.apply, ruleId };
    });
    return result;
  }

  async createTemplate(input: CreateTemplateInput) {
    const result = await withSeededUserContext(async (sql) => {
      const templateId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.import_templates (
            id,
            user_id,
            name,
            institution_name,
            compatible_account_type,
            file_kind,
            sheet_name,
            header_row_index,
            rows_to_skip_before_header,
            rows_to_skip_after_header,
            delimiter,
            encoding,
            decimal_separator,
            thousands_separator,
            date_format,
            default_currency,
            column_map_json,
            sign_logic_json,
            normalization_rules_json,
            active,
            version
          ) values (
            ${templateId},
            ${this.userId},
            ${input.template.name},
            ${input.template.institutionName},
            ${input.template.compatibleAccountType},
            ${input.template.fileKind},
            ${input.template.sheetName ?? null},
            ${input.template.headerRowIndex},
            ${input.template.rowsToSkipBeforeHeader},
            ${input.template.rowsToSkipAfterHeader},
            ${input.template.delimiter ?? null},
            ${input.template.encoding ?? null},
            ${input.template.decimalSeparator ?? null},
            ${input.template.thousandsSeparator ?? null},
            ${input.template.dateFormat},
            ${input.template.defaultCurrency},
            ${serializeJson(sql, input.template.columnMapJson)}::jsonb,
            ${serializeJson(sql, input.template.signLogicJson)}::jsonb,
            ${serializeJson(sql, input.template.normalizationRulesJson)}::jsonb,
            ${input.template.active},
            1
          )
        `;
      }
      return { applied: input.apply, templateId };
    });
    return result;
  }

  async deleteTemplate(input: DeleteTemplateInput) {
    const result = await withSeededUserContext(async (sql) => {
      const before = await sql`
        select * from public.import_templates
        where id = ${input.templateId}
          and user_id = ${this.userId}
        limit 1
      `;
      const beforeRow = before[0];
      if (!beforeRow) {
        throw new Error(`Template ${input.templateId} not found.`);
      }

      const blockers = await sql`
        with target as (
          select ${input.templateId}::uuid as template_id
        )
        select
          (
            select count(*)::int
            from public.accounts
            where import_template_default_id = target.template_id
              and user_id = ${this.userId}
          ) as default_accounts,
          (
            select count(*)::int
            from public.import_batches
            where template_id = target.template_id
              and user_id = ${this.userId}
          ) as import_batches
        from target
      `;
      const blockerRow = blockers[0] as Record<string, number>;
      const activeBlockers = Object.entries(blockerRow).filter(
        ([, count]) => Number(count) > 0,
      );
      if (activeBlockers.length > 0) {
        throw new Error(
          `Template cannot be removed because it already has dependent data: ${activeBlockers
            .map(([key, count]) => `${key.replace(/_/g, " ")} (${count})`)
            .join(", ")}.`,
        );
      }

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "templates.delete",
        "import_template",
        input.templateId,
        beforeRow,
        null,
      );

      if (input.apply) {
        await sql`
          delete from public.import_templates
          where id = ${input.templateId}
            and user_id = ${this.userId}
        `;
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;
      }

      return { applied: input.apply, templateId: input.templateId };
    });

    return result;
  }

  async addOpeningPosition(input: AddOpeningPositionInput) {
    const result = await withSeededUserContext(async (sql) => {
      const adjustmentId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.holding_adjustments ${sql({
            id: adjustmentId,
            user_id: this.userId,
            entity_id: input.entityId,
            account_id: input.accountId,
            security_id: input.securityId,
            effective_date: input.effectiveDate,
            share_delta: input.shareDelta,
            cost_basis_delta_eur: input.costBasisDeltaEur,
            reason: "opening_position",
            note: "Created from app/CLI.",
          })}
        `;
        await queueJob(sql, "position_rebuild", {
          accountId: input.accountId,
          trigger: "opening_position",
        });
      }
      return { applied: input.apply, adjustmentId };
    });
    return result;
  }

  async deleteHoldingAdjustment(input: DeleteHoldingAdjustmentInput) {
    const result = await withSeededUserContext(async (sql) => {
      const beforeRow = await selectHoldingAdjustmentRowById(
        sql,
        this.userId,
        input.adjustmentId,
      );
      if (!beforeRow) {
        throw new Error(`Holding adjustment ${input.adjustmentId} not found.`);
      }

      const auditEvent = createAuditEvent(
        input.sourceChannel,
        input.actorName,
        "positions.delete-opening",
        "holding_adjustment",
        input.adjustmentId,
        beforeRow,
        null,
      );

      if (input.apply) {
        await sql`
          delete from public.holding_adjustments
          where id = ${input.adjustmentId}
            and user_id = ${this.userId}
        `;
        await insertAuditEventRecord(sql, auditEvent);
        await queueJob(sql, "position_rebuild", {
          accountId: beforeRow.account_id,
          adjustmentId: input.adjustmentId,
          trigger: "opening_position_delete",
        });
      }

      return { applied: input.apply, adjustmentId: input.adjustmentId };
    });

    return result;
  }

  async queueRuleDraft(input: QueueRuleDraftInput) {
    const result = await withSeededUserContext(async (sql) => {
      const jobId = randomUUID();
      if (input.apply) {
        await sql`
          insert into public.jobs (
            id,
            job_type,
            payload_json,
            status,
            attempts,
            available_at
          ) values (
            ${jobId},
            ${"rule_parse"},
            ${serializeJson(sql, { requestText: input.requestText })}::jsonb,
            ${"queued"},
            0,
            ${new Date().toISOString()}
          )
        `;
        const auditEvent = createAuditEvent(
          input.sourceChannel,
          input.actorName,
          "rules.queue-draft",
          "job",
          jobId,
          null,
          { requestText: input.requestText },
        );
        await sql`
          insert into public.audit_events ${sql({
            actor_type: auditEvent.actorType,
            actor_id: auditEvent.actorId,
            actor_name: auditEvent.actorName,
            source_channel: auditEvent.sourceChannel,
            command_name: auditEvent.commandName,
            object_type: auditEvent.objectType,
            object_id: auditEvent.objectId,
            before_json: auditEvent.beforeJson,
            after_json: auditEvent.afterJson,
            created_at: auditEvent.createdAt,
            notes: auditEvent.notes,
          } as Record<string, unknown>)}
        `;
      }
      return { applied: input.apply, jobId };
    });
    return result;
  }

  async applyRuleDraft(input: ApplyRuleDraftInput) {
    const result = await withSeededUserContext(async (sql) => {
      const rows = await sql`
        select * from public.jobs
        where id = ${input.jobId}
          and job_type = 'rule_parse'
        limit 1
      `;
      const job = rows[0];
      if (!job) {
        throw new Error(`Rule draft job ${input.jobId} not found.`);
      }

      const payloadJson = parseJsonColumn<Record<string, unknown>>(
        job.payload_json ?? {},
      );
      const parsedRule =
        payloadJson &&
        typeof payloadJson === "object" &&
        "parsedRule" in payloadJson &&
        typeof payloadJson.parsedRule === "object"
          ? (payloadJson.parsedRule as Record<string, unknown>)
          : null;

      if (!parsedRule) {
        throw new Error("Rule draft has not been parsed yet.");
      }

      const createResult = await this.createRule({
        priority: Number(parsedRule.priority ?? 60),
        scopeJson: (parsedRule.scopeJson ?? {}) as Record<string, unknown>,
        conditionsJson: (parsedRule.conditionsJson ?? {}) as Record<
          string,
          unknown
        >,
        outputsJson: (parsedRule.outputsJson ?? {}) as Record<string, unknown>,
        actorName: input.actorName,
        sourceChannel: input.sourceChannel,
        apply: input.apply,
      });

      if (input.apply) {
        await sql`
          update public.jobs
          set payload_json = ${serializeJson(sql, {
            ...payloadJson,
            appliedRuleId: createResult.ruleId,
          })}::jsonb
          where id = ${input.jobId}
        `;
      }

      return { applied: input.apply, ruleId: createResult.ruleId };
    });
    return result;
  }

  async previewImport(
    input: ImportExecutionInput,
  ): Promise<ImportPreviewResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    if (!normalizedInput.filePath) {
      throw new Error("A file path is required to preview an import.");
    }

    const dataset = await this.getDataset();
    const rawResult = await runDeterministicImport(
      "preview",
      normalizedInput,
      dataset,
    );
    const prepared = buildImportedTransactions(
      dataset,
      normalizedInput,
      "preview-batch",
      rawResult.normalizedRows ?? [],
    );
    const publicResult = sanitizeImportResult(rawResult) as ImportPreviewResult;
    return {
      ...publicResult,
      rowCountDuplicates: prepared.duplicateCount,
    };
  }

  async commitImport(input: ImportExecutionInput): Promise<ImportCommitResult> {
    const normalizedInput = normalizeImportExecutionInput(input);
    const result = await withSeededUserContext(async (sql) => {
      const dataset = await loadDatasetForUser(sql, this.userId);
      const committed = await commitPreparedImportBatch(sql, {
        userId: this.userId,
        dataset,
        normalizedInput,
        previewFallback: () => this.previewImport(normalizedInput),
      });
      return committed.preview;
    });
    return result;
  }

  async commitCreditCardStatementImport(
    input: CreditCardStatementImportInput,
  ): Promise<CreditCardStatementImportResult> {
    if (!input.filePath) {
      throw new Error("A file path is required to upload a credit-card statement.");
    }

    const result = await withSeededUserContext(async (sql) => {
      const dataset = await loadDatasetForUser(sql, this.userId);
      const settlementTransaction = dataset.transactions.find(
        (candidate) => candidate.id === input.settlementTransactionId,
      );
      if (!settlementTransaction) {
        throw new Error(
          `Settlement transaction ${input.settlementTransactionId} was not found.`,
        );
      }
      if (!isCreditCardSettlementTransaction(settlementTransaction)) {
        throw new Error(
          "This row is not recognized as a credit-card settlement payment.",
        );
      }
      if (settlementTransaction.creditCardStatementStatus === "uploaded") {
        throw new Error(
          "This settlement row is already linked to a credit-card statement import.",
        );
      }

      const settlementAccount = dataset.accounts.find(
        (candidate) => candidate.id === settlementTransaction.accountId,
      );
      if (!settlementAccount) {
        throw new Error(
          `Settlement account ${settlementTransaction.accountId} was not found.`,
        );
      }

      const linkedCreditCardAccount = await resolveOrCreateLinkedCreditCardAccount(
        sql,
        {
          userId: this.userId,
          dataset,
          settlementTransaction,
          settlementAccount,
          templateId: input.templateId,
          actorName: "web-credit-card-statement",
          sourceChannel: "web",
        },
      );
      const datasetWithLinkedAccount = dataset.accounts.some(
        (candidate) => candidate.id === linkedCreditCardAccount.id,
      )
        ? dataset
        : {
            ...dataset,
            accounts: [...dataset.accounts, linkedCreditCardAccount],
          };

      const normalizedInput = normalizeImportExecutionInput({
        accountId: linkedCreditCardAccount.id,
        templateId: input.templateId,
        originalFilename: input.originalFilename,
        filePath: input.filePath,
      });
      const previewResult = await runDeterministicImport(
        "preview",
        normalizedInput,
        datasetWithLinkedAccount,
      );
      const validationDataset = {
        ...datasetWithLinkedAccount,
        transactions: [],
      } satisfies DomainDataset;
      const validationPrepared = buildImportedTransactions(
        validationDataset,
        normalizedInput,
        "credit-card-statement-validation",
        previewResult.normalizedRows ?? [],
      );
      if (validationPrepared.inserted.length === 0) {
        throw new Error(
          "The uploaded credit-card statement did not produce any transaction rows.",
        );
      }

      const statementNetAmountBaseEur = sumPreparedTransactionAmountBaseEur(
        validationPrepared.inserted,
      );
      if (
        !new Decimal(statementNetAmountBaseEur).eq(
          new Decimal(settlementTransaction.amountBaseEur),
        )
      ) {
        throw new Error(
          `The statement total (${new Decimal(statementNetAmountBaseEur).toFixed(2)} EUR) must exactly match the settlement row (${new Decimal(settlementTransaction.amountBaseEur).toFixed(2)} EUR).`,
        );
      }

      const duplicatePrepared = buildImportedTransactions(
        datasetWithLinkedAccount,
        normalizedInput,
        "credit-card-statement-duplicate-check",
        previewResult.normalizedRows ?? [],
      );
      if (duplicatePrepared.duplicateCount > 0) {
        throw new Error(
          "This statement contains transactions that are already present in the linked credit-card ledger.",
        );
      }

      const committed = await commitPreparedImportBatch(sql, {
        userId: this.userId,
        dataset: datasetWithLinkedAccount,
        normalizedInput,
        options: {
          importedByActor: "web-credit-card-statement",
          importBatchExtraValues: {
            credit_card_settlement_transaction_id: settlementTransaction.id,
            statement_net_amount_base_eur: statementNetAmountBaseEur,
          },
        },
      });

      const settlementMirrorTransactionId = randomUUID();
      const mirrorCreatedAt = new Date().toISOString();
      const mirrorTransaction = {
        id: settlementMirrorTransactionId,
        userId: this.userId,
        accountId: linkedCreditCardAccount.id,
        accountEntityId: linkedCreditCardAccount.entityId,
        economicEntityId: linkedCreditCardAccount.entityId,
        importBatchId: null,
        providerName: null,
        providerRecordId: null,
        sourceFingerprint: `credit-card-settlement-mirror:${settlementTransaction.id}`,
        duplicateKey: `credit-card-settlement-mirror:${settlementTransaction.id}`,
        transactionDate: settlementTransaction.transactionDate,
        postedDate:
          settlementTransaction.postedDate ?? settlementTransaction.transactionDate,
        amountOriginal: new Decimal(settlementTransaction.amountOriginal)
          .abs()
          .toFixed(8),
        currencyOriginal: settlementTransaction.currencyOriginal,
        amountBaseEur: new Decimal(settlementTransaction.amountBaseEur)
          .abs()
          .toFixed(8),
        fxRateToEur: settlementTransaction.fxRateToEur ?? null,
        descriptionRaw: `Credit card statement payment from ${settlementAccount.displayName}`,
        descriptionClean: normalizeCreditCardSettlementText(
          `Credit card statement payment from ${settlementAccount.displayName}`,
        ),
        merchantNormalized: null,
        counterpartyName: settlementAccount.displayName,
        transactionClass: "transfer_internal",
        categoryCode: null,
        subcategoryCode: null,
        transferGroupId: null,
        relatedAccountId: settlementAccount.id,
        relatedTransactionId: settlementTransaction.id,
        transferMatchStatus: "matched",
        crossEntityFlag: false,
        reimbursementStatus: "none",
        classificationStatus: "transfer_match",
        classificationSource: "transfer_matcher",
        classificationConfidence: "1.00",
        needsReview: false,
        reviewReason: null,
        excludeFromAnalytics: false,
        correctionOfTransactionId: null,
        voidedAt: null,
        manualNotes: null,
        llmPayload: {
          analysisStatus: "skipped",
          explanation: "Synthetic settlement mirror for a linked credit-card statement import.",
          model: null,
          error: null,
        },
        rawPayload: {
          creditCardStatementSettlementMirror: true,
          settlementTransactionId: settlementTransaction.id,
          linkedImportBatchId: committed.importBatchId,
        },
        securityId: null,
        quantity: null,
        unitPriceOriginal: null,
        creditCardStatementStatus: "not_applicable",
        linkedCreditCardAccountId: null,
        createdAt: mirrorCreatedAt,
        updatedAt: mirrorCreatedAt,
      } satisfies Transaction;

      await sql`
        insert into public.transactions ${sql({
          id: mirrorTransaction.id,
          user_id: mirrorTransaction.userId,
          account_id: mirrorTransaction.accountId,
          account_entity_id: mirrorTransaction.accountEntityId,
          economic_entity_id: mirrorTransaction.economicEntityId,
          import_batch_id: mirrorTransaction.importBatchId,
          source_fingerprint: mirrorTransaction.sourceFingerprint,
          duplicate_key: mirrorTransaction.duplicateKey,
          transaction_date: mirrorTransaction.transactionDate,
          posted_date: mirrorTransaction.postedDate,
          amount_original: mirrorTransaction.amountOriginal,
          currency_original: mirrorTransaction.currencyOriginal,
          amount_base_eur: mirrorTransaction.amountBaseEur,
          fx_rate_to_eur: mirrorTransaction.fxRateToEur,
          description_raw: mirrorTransaction.descriptionRaw,
          description_clean: mirrorTransaction.descriptionClean,
          merchant_normalized: mirrorTransaction.merchantNormalized,
          counterparty_name: mirrorTransaction.counterpartyName,
          transaction_class: mirrorTransaction.transactionClass,
          category_code: mirrorTransaction.categoryCode,
          subcategory_code: mirrorTransaction.subcategoryCode,
          transfer_group_id: mirrorTransaction.transferGroupId,
          related_account_id: mirrorTransaction.relatedAccountId,
          related_transaction_id: mirrorTransaction.relatedTransactionId,
          transfer_match_status: mirrorTransaction.transferMatchStatus,
          cross_entity_flag: mirrorTransaction.crossEntityFlag,
          reimbursement_status: mirrorTransaction.reimbursementStatus,
          classification_status: mirrorTransaction.classificationStatus,
          classification_source: mirrorTransaction.classificationSource,
          classification_confidence: mirrorTransaction.classificationConfidence,
          needs_review: mirrorTransaction.needsReview,
          review_reason: mirrorTransaction.reviewReason,
          exclude_from_analytics: mirrorTransaction.excludeFromAnalytics,
          correction_of_transaction_id: mirrorTransaction.correctionOfTransactionId,
          voided_at: mirrorTransaction.voidedAt,
          manual_notes: mirrorTransaction.manualNotes,
          llm_payload: mirrorTransaction.llmPayload,
          raw_payload: mirrorTransaction.rawPayload,
          security_id: mirrorTransaction.securityId,
          quantity: mirrorTransaction.quantity,
          unit_price_original: mirrorTransaction.unitPriceOriginal,
          credit_card_statement_status: mirrorTransaction.creditCardStatementStatus,
          linked_credit_card_account_id: mirrorTransaction.linkedCreditCardAccountId,
          created_at: mirrorTransaction.createdAt,
          updated_at: mirrorTransaction.updatedAt,
        } as Record<string, unknown>)}
      `;

      const afterSettlementRow = await updateTransactionRecord(sql, {
        userId: this.userId,
        transactionId: settlementTransaction.id,
        updatePayload: {
          related_account_id: linkedCreditCardAccount.id,
          related_transaction_id: settlementMirrorTransactionId,
          transfer_match_status: "matched",
          needs_review: false,
          review_reason: null,
          credit_card_statement_status: "uploaded",
          linked_credit_card_account_id: linkedCreditCardAccount.id,
          updated_at: new Date().toISOString(),
        },
      });
      if (!afterSettlementRow) {
        throw new Error(
          `Settlement transaction ${settlementTransaction.id} could not be linked.`,
        );
      }
      const afterSettlementTransaction =
        mapFromSql<Transaction>(afterSettlementRow);

      await insertAuditEventRecord(
        sql,
        createAuditEvent(
          "web",
          "web-credit-card-statement",
          "transactions.credit-card-settlement-mirror",
          "transaction",
          mirrorTransaction.id,
          null,
          mirrorTransaction as unknown as Record<string, unknown>,
        ),
      );
      await insertAuditEventRecord(
        sql,
        createAuditEvent(
          "web",
          "web-credit-card-statement",
          "transactions.link-credit-card-statement",
          "transaction",
          settlementTransaction.id,
          settlementTransaction as unknown as Record<string, unknown>,
          afterSettlementTransaction as unknown as Record<string, unknown>,
        ),
      );
      await sql`
        update public.import_batches
        set commit_summary_json =
          coalesce(commit_summary_json, '{}'::jsonb) ||
          ${serializeJson(sql, {
            settlementMirrorTransactionId,
            linkedCreditCardAccountId: linkedCreditCardAccount.id,
          })}::jsonb
        where id = ${committed.importBatchId}
          and user_id = ${this.userId}
      `;
      await queueJob(sql, "metric_refresh", {
        trigger: "credit_card_statement_import",
        settlementTransactionId: settlementTransaction.id,
        accountId: settlementAccount.id,
        linkedCreditCardAccountId: linkedCreditCardAccount.id,
        importBatchId: committed.importBatchId,
      });

      return {
        ...committed.preview,
        settlementTransactionId: settlementTransaction.id,
        linkedCreditCardAccountId: linkedCreditCardAccount.id,
        linkedCreditCardAccountName: linkedCreditCardAccount.displayName,
        settlementMirrorTransactionId,
        statementNetAmountBaseEur,
      };
    });

    return result;
  }

  async runPendingJobs(apply: boolean): Promise<JobRunResult> {
    const result = await withSeededUserSession(async (sql) => {
      if (apply) {
        await recoverStaleRunningJobs(sql);
      }

      const queued = await sql`
        select * from public.jobs
        where status = 'queued'
          and available_at <= ${new Date().toISOString()}
        order by available_at asc, created_at asc
      `;
      const processedJobs: JobRunResult["processedJobs"] = [];
      if (apply && queued.length > 0) {
        const workerId = `worker:${process.pid}:${randomUUID()}`;
        let cachedPromptOverrides: PromptProfileOverrides | null = null;
        const getPromptOverridesCached = async () => {
          if (!cachedPromptOverrides) {
            cachedPromptOverrides = await loadPromptOverrides(sql, this.userId);
          }
          return cachedPromptOverrides;
        };

        while (true) {
          const job = await claimNextQueuedJob(sql, workerId);
          if (!job) {
            break;
          }

          const startedAt =
            typeof job.started_at === "string"
              ? job.started_at
              : new Date().toISOString();
          const payloadJson = parseJsonColumn<Record<string, unknown>>(
            job.payload_json ?? {},
          );

          try {
            if (job.job_type === "rule_parse") {
              const requestText =
                typeof payloadJson.requestText === "string"
                  ? payloadJson.requestText
                  : "";
              if (!requestText) {
                throw new Error("Rule draft job is missing requestText.");
              }

              const parsedRule = await parseRuleDraftRequest(
                requestText,
                await loadDatasetForUser(sql, this.userId),
                await getPromptOverridesCached(),
              );
              await completeJob(sql, job.id, startedAt, {
                ...payloadJson,
                parsedRule,
              });
              processedJobs.push({
                id: job.id,
                jobType: "rule_parse",
                status: "completed",
              });
              continue;
            }

            if (job.job_type === "classification") {
              const importBatchId =
                typeof payloadJson.importBatchId === "string"
                  ? payloadJson.importBatchId
                  : "";
              if (!importBatchId) {
                throw new Error("Classification job is missing importBatchId.");
              }

              let latestDataset = await loadDatasetForUser(sql, this.userId);
              const promptOverrides = await getPromptOverridesCached();
              const rows = await sql`
                select ${transactionColumnsSql(sql)}
                from public.transactions
                where user_id = ${this.userId}
                  and import_batch_id = ${importBatchId}
                  and coalesce(llm_payload->>'analysisStatus', 'pending') = 'pending'
                order by transaction_date asc, created_at asc
              `;

              let failedTransactions = 0;
              let processedTransactions = 0;
              const investmentAccountIds = new Set<string>();
              let currentJobPayload = { ...payloadJson };
              const reportClassificationProgress = async (
                transactionId: string | null,
              ) => {
                currentJobPayload = {
                  ...currentJobPayload,
                  progress: {
                    totalTransactions: rows.length,
                    processedTransactions,
                    failedTransactions,
                    lastTransactionId: transactionId,
                    updatedAt: new Date().toISOString(),
                  },
                };
                await updateRunningJobPayload(sql, job.id, currentJobPayload);
              };

              await reportClassificationProgress(null);
              for (const row of rows) {
                const transaction = mapFromSql<Transaction>(row);
                const account = latestDataset.accounts.find(
                  (candidate) => candidate.id === transaction.accountId,
                );
                if (!account) {
                  throw new Error(
                    `Account ${transaction.accountId} not found for classification.`,
                  );
                }
                if (account.assetDomain === "investment") {
                  investmentAccountIds.add(account.id);
                }

                try {
                  const { afterTransaction } =
                    await executeTransactionEnrichmentPipeline(
                      sql,
                      this.userId,
                      {
                        dataset: latestDataset,
                        account,
                        transaction,
                        enrichmentOptions: { promptOverrides },
                      },
                    );
                  latestDataset = replaceTransactionInDataset(
                    latestDataset,
                    afterTransaction,
                  );
                } catch (transactionError) {
                  failedTransactions += 1;
                  const failedUpdate = await sql`
                    update public.transactions
                    set needs_review = true,
                        review_reason = ${
                          transactionError instanceof Error
                            ? transactionError.message
                            : "Transaction enrichment failed."
                        },
                        llm_payload = ${serializeJson(sql, {
                          ...(parseJsonColumn<Record<string, unknown>>(
                            row.llm_payload ?? {},
                          ) ?? {}),
                          analysisStatus: "failed",
                          explanation: null,
                          model:
                            account.assetDomain === "investment"
                              ? getInvestmentTransactionClassifierConfig().model
                              : getTransactionClassifierConfig().model,
                          error:
                            transactionError instanceof Error
                              ? transactionError.message
                              : "Transaction enrichment failed.",
                          analyzedAt: new Date().toISOString(),
                        })}::jsonb,
                        updated_at = ${new Date().toISOString()}
                    where id = ${transaction.id}
                      and user_id = ${this.userId}
                    returning ${transactionColumnsSql(sql)}
                  `;
                  latestDataset = replaceTransactionInDataset(
                    latestDataset,
                    mapFromSql<Transaction>(failedUpdate[0]),
                  );
                }

                processedTransactions += 1;
                await reportClassificationProgress(transaction.id);
              }

              await sql`
                update public.import_batches
                set classification_triggered_at = ${new Date().toISOString()}
                where id = ${importBatchId}
                  and user_id = ${this.userId}
              `;
              for (const accountId of investmentAccountIds) {
                await queueJob(sql, "position_rebuild", {
                  importBatchId,
                  accountId,
                  trigger: "classification_completion",
                });
              }
              await completeJob(sql, job.id, startedAt, {
                ...currentJobPayload,
                processedTransactions: rows.length,
                failedTransactions,
                queuedFollowUpPositionRebuilds: investmentAccountIds.size,
              });
              processedJobs.push({
                id: job.id,
                jobType: "classification",
                status: "completed",
              });
              continue;
            }

            if (job.job_type === "bank_sync") {
              const resultPayload = await processRevolutSyncJob(
                sql,
                this.userId,
                payloadJson,
              );
              await completeJob(sql, job.id, startedAt, {
                ...payloadJson,
                ...resultPayload,
              });
              processedJobs.push({
                id: job.id,
                jobType: "bank_sync",
                status: "completed",
              });
              continue;
            }

            if (job.job_type === "position_rebuild") {
              const rebuilt = await applyInvestmentRebuild(sql, this.userId);
              await completeJob(sql, job.id, startedAt, {
                ...payloadJson,
                ...rebuilt,
              });
              processedJobs.push({
                id: job.id,
                jobType: "position_rebuild",
                status: "completed",
              });
              continue;
            }

            if (job.job_type === "metric_refresh") {
              await refreshFinanceAnalyticsArtifacts(sql);
              await completeJob(sql, job.id, startedAt, {
                ...payloadJson,
                refreshedAt: new Date().toISOString(),
              });
              processedJobs.push({
                id: job.id,
                jobType: "metric_refresh",
                status: "completed",
              });
              continue;
            }

            if (job.job_type === "review_reanalyze") {
              const transactionId =
                typeof payloadJson.transactionId === "string"
                  ? payloadJson.transactionId
                  : "";
              const reviewContext =
                typeof payloadJson.reviewContext === "string"
                  ? payloadJson.reviewContext
                  : "";
              const actorName =
                typeof payloadJson.actorName === "string"
                  ? payloadJson.actorName
                  : "worker-review-editor";
              const reviewMode =
                payloadJson.reviewMode === "manual_resolved_review" ||
                payloadJson.reviewMode === "manual_review_update"
                  ? (payloadJson.reviewMode as ReviewReanalysisMode)
                  : undefined;
              const sourceChannel =
                typeof payloadJson.sourceChannel === "string" &&
                ["web", "cli", "worker", "system"].includes(
                  payloadJson.sourceChannel,
                )
                  ? (payloadJson.sourceChannel as
                      | "web"
                      | "cli"
                      | "worker"
                      | "system")
                  : "worker";
              if (!transactionId || !reviewContext) {
                throw new Error(
                  "Review reanalysis job is missing transactionId or reviewContext.",
                );
              }
              const reportProgress = async (
                progress: ReviewReanalysisProgress,
              ) => {
                const nextPayloadJson = {
                  ...payloadJson,
                  progress: {
                    ...progress,
                    updatedAt: new Date().toISOString(),
                  },
                };
                console.log(
                  `[review_reanalyze] ${job.id} ${progress.stage}: ${progress.message}`,
                );
                await updateRunningJobPayload(sql, job.id, nextPayloadJson);
              };
              await reportProgress({
                stage: "load_context",
                message: "Loading transaction context.",
              });
              const resultPayload = await reanalyzeTransactionReview({
                transactionId,
                reviewContext,
                actorName,
                sourceChannel,
                reviewMode,
                onProgress: reportProgress,
              });
              await completeJob(sql, job.id, startedAt, {
                ...payloadJson,
                ...resultPayload,
              });
              processedJobs.push({
                id: job.id,
                jobType: "review_reanalyze",
                status: "completed",
              });
              continue;
            }

            if (job.job_type === "review_propagation") {
              const resultPayload = await processReviewPropagationJob(
                sql,
                this.userId,
                payloadJson,
                await getPromptOverridesCached(),
              );
              await completeJob(sql, job.id, startedAt, {
                ...payloadJson,
                ...resultPayload,
              });
              processedJobs.push({
                id: job.id,
                jobType: "review_propagation",
                status: "completed",
              });
              continue;
            }

            await completeJob(sql, job.id, startedAt, payloadJson);
            processedJobs.push({
              id: job.id,
              jobType: job.job_type,
              status: "completed",
            });
          } catch (error) {
            await failJob(sql, job.id, startedAt, error);
            processedJobs.push({
              id: job.id,
              jobType: job.job_type,
              status: "failed",
            });
          }
        }
      }
      return {
        schemaVersion: "v1" as const,
        applied: apply,
        processedJobs: apply
          ? processedJobs
          : queued.map((job) => ({
              id: job.id,
              jobType: job.job_type,
              status: job.status,
            })),
        generatedAt: new Date().toISOString(),
      };
    });
    return result;
  }
}

export function createFinanceRepository(): FinanceRepository {
  return new SqlFinanceRepository();
}

export { getRevolutRuntimeStatus };
