import { Decimal } from "decimal.js";

import {
  buildDashboardReadModel,
  buildDashboardSummary,
  buildIncomeReadModel,
  buildInvestmentsReadModel,
  buildSpendingReadModel,
} from "@myfinance/analytics";
import { NON_AI_RULE_SUMMARIES } from "@myfinance/classification";
import {
  getRevolutRuntimeStatus,
  listLearnedReviewExamples,
  listPromptProfiles,
} from "@myfinance/db";
import {
  buildLiveHoldingRows,
  type DomainDataset,
  type Entity,
  filterTransactionsByScope,
  getLatestAccountBalances,
  getScopeLatestDate,
  needsTransactionManualReview,
  parseWorkspaceSettings,
  resolveScopeEntityIds,
  resolvePeriodSelection,
  type Scope,
  type Transaction,
} from "@myfinance/domain";
import {
  formatCurrency,
  formatDate,
  formatPercent,
  formatQuantity,
} from "./formatters";
import { domain as domainService, repository } from "./action-service";
import { augmentDatasetWithDiscoveredRevolutLowRiskFund } from "./discovered-revolut-investment";

export type RawSearchParams =
  | Promise<Record<string, string | string[] | undefined>>
  | Record<string, string | string[] | undefined>;

function normalizeParam(
  params: Record<string, string | string[] | undefined>,
  key: string,
): string | undefined {
  const value = params[key];
  if (Array.isArray(value)) return value[0];
  return value;
}

function buildEntityScopeOptions(entities: Entity[]) {
  return [
    { value: "consolidated", label: "Consolidated" },
    ...entities.map((entity) => ({
      value: entity.slug,
      label: entity.displayName,
    })),
  ];
}

function todayIsoInTimezone(timezone: string) {
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: timezone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(new Date());
    const year = parts.find((part) => part.type === "year")?.value;
    const month = parts.find((part) => part.type === "month")?.value;
    const day = parts.find((part) => part.type === "day")?.value;
    if (year && month && day) {
      return `${year}-${month}-${day}`;
    }
  } catch {
    // Fallback to UTC if the configured timezone is invalid.
  }

  return new Date().toISOString().slice(0, 10);
}

function jobImportBatchId(job: { payloadJson: Record<string, unknown> }) {
  return typeof job.payloadJson.importBatchId === "string"
    ? job.payloadJson.importBatchId
    : null;
}

export async function resolveAppState(searchParams: RawSearchParams) {
  const params = await searchParams;
  const dataset = await repository.getDataset();
  const workspaceSettings = parseWorkspaceSettings(
    dataset.profile.workspaceSettingsJson,
    {
      entities: dataset.entities,
      profileDefaultBaseCurrency: dataset.profile.defaultBaseCurrency,
    },
  );
  const requestedScopeParam =
    normalizeParam(params, "scope") ?? workspaceSettings.preferredScope;
  const currencyParam = normalizeParam(params, "currency");
  const currency =
    currencyParam === "USD"
      ? "USD"
      : currencyParam === "EUR"
        ? "EUR"
        : workspaceSettings.defaultDisplayCurrency;
  const periodParam =
    normalizeParam(params, "period") ?? workspaceSettings.defaultPeriodPreset;
  const transactionSearchQuery = normalizeParam(params, "q") ?? "";
  const entityBySlug = new Map(
    dataset.entities.map((entity) => [entity.slug, entity.id]),
  );
  const requestedAccountId = requestedScopeParam.startsWith("account:")
    ? requestedScopeParam.replace("account:", "")
    : null;
  const requestedEntityId = entityBySlug.get(requestedScopeParam);
  const hasRequestedAccount =
    requestedAccountId !== null &&
    dataset.accounts.some((account) => account.id === requestedAccountId);
  const hasRequestedEntity =
    typeof requestedEntityId === "string" &&
    dataset.entities.some((entity) => entity.id === requestedEntityId);
  const scopeParam =
    requestedScopeParam === "consolidated"
      ? "consolidated"
      : hasRequestedAccount
        ? requestedScopeParam
        : hasRequestedEntity
          ? requestedScopeParam
          : "consolidated";
  const scope: Scope =
    scopeParam.startsWith("account:") && requestedAccountId
      ? { kind: "account", accountId: requestedAccountId }
      : scopeParam === "consolidated"
        ? { kind: "consolidated" }
        : { kind: "entity", entityId: requestedEntityId };
  const today = todayIsoInTimezone(dataset.profile.timezone);
  const latestReferenceDate = getScopeLatestDate(dataset, scope, today);
  const referenceDate = normalizeParam(params, "asOf") ?? latestReferenceDate;
  const period = resolvePeriodSelection({
    preset: periodParam,
    start: normalizeParam(params, "start"),
    end: normalizeParam(params, "end"),
    referenceDate,
  });
  const activeEntities = dataset.entities.filter((entity) => entity.active);
  const entityScopeOptions = buildEntityScopeOptions(activeEntities);
  if (
    scope.kind === "entity" &&
    scope.entityId &&
    !activeEntities.some((entity) => entity.id === scope.entityId)
  ) {
    const selectedEntity = dataset.entities.find(
      (entity) => entity.id === scope.entityId,
    );
    if (selectedEntity) {
      entityScopeOptions.push({
        value: selectedEntity.slug,
        label: selectedEntity.displayName,
      });
    }
  }

  const scopeOptions = [
    ...entityScopeOptions,
    ...dataset.accounts.map((account) => ({
      value: `account:${account.id}`,
      label: `${account.displayName} (Account)`,
    })),
  ];

  return {
    dataset,
    domainService,
    scope,
    scopeParam,
    currency,
    referenceDate,
    latestReferenceDate,
    periodParam,
    period,
    scopeOptions,
    workspaceSettings,
    navigationState: {
      scopeParam,
      currency,
      period: period.preset,
      referenceDate,
      latestReferenceDate,
      start: period.preset === "custom" ? period.start : undefined,
      end: period.preset === "custom" ? period.end : undefined,
    },
    transactionSearchQuery,
  };
}

export function buildHref(
  pathname: string,
  current: {
    scopeParam: string;
    currency: string;
    period: string;
    referenceDate?: string;
    start?: string;
    end?: string;
  },
  overrides: Partial<{
    scopeParam: string;
    currency: string;
    period: string;
    referenceDate: string;
    start: string;
    end: string;
  }>,
  extraParams: Record<string, string | undefined> = {},
) {
  const period = overrides.period ?? current.period;
  const query = new URLSearchParams({
    scope: overrides.scopeParam ?? current.scopeParam,
    currency: overrides.currency ?? current.currency,
    period,
  });
  const referenceDate = overrides.referenceDate ?? current.referenceDate;
  if (referenceDate) {
    query.set("asOf", referenceDate);
  }
  const start = overrides.start ?? current.start;
  const end = overrides.end ?? current.end;
  if (period === "custom" && start && end) {
    query.set("start", start);
    query.set("end", end);
  }
  for (const [key, value] of Object.entries(extraParams)) {
    if (typeof value === "string" && value.trim() !== "") {
      query.set(key, value);
    } else {
      query.delete(key);
    }
  }
  return `${pathname}?${query.toString()}`;
}

export { formatCurrency, formatDate, formatPercent, formatQuantity };

function decimalFrom(value: string | null | undefined) {
  if (value === null || value === undefined) {
    return null;
  }

  try {
    return new Decimal(value);
  } catch {
    return null;
  }
}

function isIsoDateString(value: string | null | undefined): value is string {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function maxIsoDate(left: string, right: string) {
  return left >= right ? left : right;
}

function getLatestScopedTransactionDate(
  dataset: DomainDataset,
  scope: Scope,
  fallback: string,
) {
  const latestTransactionDate = filterTransactionsByScope(dataset, scope)
    .map((transaction) => transaction.transactionDate)
    .filter(isIsoDateString)
    .sort()
    .at(-1);

  return latestTransactionDate
    ? maxIsoDate(fallback, latestTransactionDate)
    : fallback;
}

function buildAccountTotalsByDate(
  dataset: DomainDataset,
  scope: Scope,
  referenceDate: string,
) {
  const latestBalancesByAccount = new Map(
    getLatestAccountBalances(dataset, referenceDate).map((balance) => [
      balance.accountId,
      balance,
    ]),
  );
  const holdingTotalsByAccount = new Map<string, Decimal>();

  for (const holding of buildLiveHoldingRows(dataset, scope, referenceDate)) {
    const value = decimalFrom(holding.currentValueEur);
    if (!value) continue;
    holdingTotalsByAccount.set(
      holding.accountId,
      (holdingTotalsByAccount.get(holding.accountId) ?? new Decimal(0)).plus(
        value,
      ),
    );
  }

  return new Map(
    dataset.accounts.map((account) => {
      const latestBalance = latestBalancesByAccount.get(account.id);
      const balanceBaseEur = decimalFrom(latestBalance?.balanceBaseEur);
      const holdingsBaseEur =
        holdingTotalsByAccount.get(account.id) ?? new Decimal(0);
      const totalBaseEur =
        balanceBaseEur || !holdingsBaseEur.isZero()
          ? (balanceBaseEur ?? new Decimal(0)).plus(holdingsBaseEur).toFixed(2)
          : null;

      return [
        account.id,
        {
          latestBalanceDate: latestBalance?.asOfDate ?? null,
          totalBaseEur,
        },
      ];
    }),
  );
}

function buildDashboardAccountBalances(
  dataset: DomainDataset,
  scope: Scope,
  materializedReferenceDate: string,
  projectedReferenceDate: string,
) {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const materializedTotalsByAccount = buildAccountTotalsByDate(
    dataset,
    scope,
    materializedReferenceDate,
  );
  const projectedTotalsByAccount = buildAccountTotalsByDate(
    dataset,
    scope,
    projectedReferenceDate,
  );
  const hasFutureActivity = projectedReferenceDate > materializedReferenceDate;

  const rows = dataset.accounts
    .filter(
      (account) =>
        account.isActive &&
        entityIds.has(account.entityId) &&
        (scope.kind !== "account" || account.id === scope.accountId),
    )
    .map((account) => {
      const materializedTotal = materializedTotalsByAccount.get(account.id);
      const projectedTotal = projectedTotalsByAccount.get(account.id);
      const materializedBaseEur = materializedTotal?.totalBaseEur ?? null;
      const projectedBaseEur = projectedTotal?.totalBaseEur ?? null;
      const projectedValue = decimalFrom(projectedBaseEur);
      const materializedValue = decimalFrom(materializedBaseEur);
      const futureDeltaBaseEur =
        projectedValue && materializedValue
          ? projectedValue.minus(materializedValue).toFixed(2)
          : null;
      const entity = dataset.entities.find(
        (candidate) => candidate.id === account.entityId,
      );

      return {
        id: account.id,
        displayName: account.displayName,
        institutionName: account.institutionName,
        accountType: account.accountType,
        assetDomain: account.assetDomain,
        accountSuffix: account.accountSuffix ?? null,
        entityName: entity?.displayName ?? account.entityId,
        latestBalanceDate: projectedTotal?.latestBalanceDate ?? null,
        materializedBaseEur,
        projectedBaseEur,
        futureDeltaBaseEur,
        totalBaseEur: projectedBaseEur,
      };
    });

  const projectedTotalBaseEur = rows
    .reduce((sum, row) => {
      const value = decimalFrom(row.projectedBaseEur);
      return value ? sum.plus(value) : sum;
    }, new Decimal(0))
    .toFixed(2);
  const materializedTotalBaseEur = rows
    .reduce((sum, row) => {
      const value = decimalFrom(row.materializedBaseEur);
      return value ? sum.plus(value) : sum;
    }, new Decimal(0))
    .toFixed(2);

  return {
    rows,
    hasFutureActivity,
    materializedReferenceDate,
    projectedReferenceDate,
    materializedTotalBaseEur,
    projectedTotalBaseEur,
    totalBaseEur: projectedTotalBaseEur,
  };
}

export async function getDashboardModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const today = todayIsoInTimezone(state.dataset.profile.timezone);
  const materializedReferenceDate = getScopeLatestDate(
    state.dataset,
    state.scope,
    today,
  );
  const projectedReferenceDate =
    state.period.preset === "custom"
      ? state.referenceDate
      : getLatestScopedTransactionDate(
          state.dataset,
          state.scope,
          materializedReferenceDate,
        );
  const dashboardPeriod =
    state.period.preset === "custom"
      ? state.period
      : resolvePeriodSelection({
          preset: state.periodParam,
          referenceDate: projectedReferenceDate,
        });
  const navigationState = {
    ...state.navigationState,
    period: dashboardPeriod.preset,
    referenceDate: projectedReferenceDate,
    latestReferenceDate: projectedReferenceDate,
    start:
      dashboardPeriod.preset === "custom" ? dashboardPeriod.start : undefined,
    end: dashboardPeriod.preset === "custom" ? dashboardPeriod.end : undefined,
  };
  const { summary, summaryBreakdown } = buildDashboardReadModel(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: dashboardPeriod,
    referenceDate: projectedReferenceDate,
  });

  return {
    ...state,
    referenceDate: projectedReferenceDate,
    latestReferenceDate: projectedReferenceDate,
    period: dashboardPeriod,
    navigationState,
    summary,
    summaryBreakdown,
    materializedReferenceDate,
    projectedReferenceDate,
    accountBalances: buildDashboardAccountBalances(
      state.dataset,
      state.scope,
      materializedReferenceDate,
      projectedReferenceDate,
    ),
  };
}

export type DashboardModel = Awaited<ReturnType<typeof getDashboardModel>>;
export type TransactionsModel = Awaited<
  ReturnType<typeof getTransactionsModel>
>;
export type AccountsModel = Awaited<ReturnType<typeof getAccountsModel>>;
export type ImportsModel = Awaited<ReturnType<typeof getImportsModel>>;
export type TemplatesModel = Awaited<ReturnType<typeof getTemplatesModel>>;
export type RulesModel = Awaited<ReturnType<typeof getRulesModel>>;
export type InvestmentsModel = Awaited<ReturnType<typeof getInvestmentsModel>>;
export type SpendingModel = Awaited<ReturnType<typeof getSpendingModel>>;
export type IncomeModel = Awaited<ReturnType<typeof getIncomeModel>>;
export type InsightsModel = Awaited<ReturnType<typeof getInsightsModel>>;
export type PromptsModel = Awaited<ReturnType<typeof getPromptsModel>>;
export type CreditCardStatementModel = Awaited<
  ReturnType<typeof getCreditCardStatementModel>
>;

export async function getTransactionsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const projectedReferenceDate =
    state.period.preset === "custom"
      ? state.referenceDate
      : getLatestScopedTransactionDate(
          state.dataset,
          state.scope,
          state.referenceDate,
        );
  const transactionsPeriod =
    state.period.preset === "custom"
      ? state.period
      : resolvePeriodSelection({
          preset: state.periodParam,
          referenceDate: projectedReferenceDate,
        });
  const navigationState = {
    ...state.navigationState,
    period: transactionsPeriod.preset,
    referenceDate: projectedReferenceDate,
    latestReferenceDate: projectedReferenceDate,
    start:
      transactionsPeriod.preset === "custom"
        ? transactionsPeriod.start
        : undefined,
    end:
      transactionsPeriod.preset === "custom" ? transactionsPeriod.end : undefined,
  };
  const ledger = await domainService.listTransactions(state.scope, {
    referenceDate: projectedReferenceDate,
    period: transactionsPeriod,
    query: state.transactionSearchQuery,
  });
  return {
    ...state,
    referenceDate: projectedReferenceDate,
    latestReferenceDate: projectedReferenceDate,
    period: transactionsPeriod,
    navigationState,
    ledger,
  };
}

export async function getCreditCardStatementModel(
  searchParams: RawSearchParams,
  importBatchId: string,
) {
  const state = await resolveAppState(searchParams);
  const importBatch = state.dataset.importBatches.find(
    (batch) => batch.id === importBatchId,
  );
  if (!importBatch) {
    return { ...state, importBatch: null };
  }

  const settlementTransaction = importBatch.creditCardSettlementTransactionId
    ? (state.dataset.transactions.find(
        (transaction) =>
          transaction.id === importBatch.creditCardSettlementTransactionId,
      ) ?? null)
    : null;

  const statementTransactions = [...state.dataset.transactions]
    .filter((transaction) => transaction.importBatchId === importBatchId)
    .sort((left, right) =>
      `${right.transactionDate}${right.createdAt}`.localeCompare(
        `${left.transactionDate}${left.createdAt}`,
      ),
    );

  const jobs = [...state.dataset.jobs]
    .filter((job) => jobImportBatchId(job) === importBatchId)
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt));

  const linkedAccount =
    state.dataset.accounts.find(
      (account) => account.id === importBatch.accountId,
    ) ?? null;
  const unresolvedCount = statementTransactions.filter((transaction) =>
    needsTransactionManualReview(transaction),
  ).length;
  const llmResolvedCount = statementTransactions.filter(
    (transaction) => transaction.classificationSource === "llm",
  ).length;

  return {
    ...state,
    importBatch,
    linkedAccount,
    settlementTransaction,
    statementTransactions,
    jobs,
    unresolvedCount,
    llmResolvedCount,
  };
}

export async function getAccountsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const accounts = await domainService.listAccounts({
    referenceDate: state.referenceDate,
  });
  return {
    ...state,
    accounts,
    revolutRuntime: getRevolutRuntimeStatus(),
  };
}

export async function getImportsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const templates = await domainService.listTemplates();
  return {
    ...state,
    templates,
    importBatches: state.dataset.importBatches,
  };
}

export async function getRulesModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const rules = await domainService.listRules();
  const drafts = await domainService.listRuleDrafts();
  return {
    ...state,
    rules,
    drafts,
    deterministicSummaries: NON_AI_RULE_SUMMARIES,
  };
}

export async function getTemplatesModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const templates = await domainService.listTemplates();
  return { ...state, templates };
}

export async function getInvestmentsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const dataset = augmentDatasetWithDiscoveredRevolutLowRiskFund(
    state.dataset,
    state.referenceDate,
  );
  return {
    ...state,
    dataset,
    ...buildInvestmentsReadModel(dataset, {
      scope: state.scope,
      displayCurrency: state.currency,
      period: state.period,
      referenceDate: state.referenceDate,
    }),
  };
}

export async function getSpendingModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  return {
    ...state,
    ...buildSpendingReadModel(state.dataset, {
      scope: state.scope,
      displayCurrency: state.currency,
      period: state.period,
      referenceDate: state.referenceDate,
    }),
  };
}

export async function getIncomeModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  return {
    ...state,
    ...buildIncomeReadModel(state.dataset, {
      scope: state.scope,
      displayCurrency: state.currency,
      period: state.period,
      referenceDate: state.referenceDate,
    }),
  };
}

export async function getInsightsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const summary = buildDashboardSummary(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
    referenceDate: state.referenceDate,
  });
  return {
    ...state,
    insights: summary.insights,
    summary,
  };
}

export async function getSettingsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const supportedValuesOf = (
    Intl as typeof Intl & {
      supportedValuesOf?: (key: string) => string[];
    }
  ).supportedValuesOf;
  const timezones =
    typeof supportedValuesOf === "function"
      ? supportedValuesOf("timeZone")
      : [
          "Europe/Madrid",
          "Europe/London",
          "UTC",
          "America/New_York",
          "America/Los_Angeles",
        ];
  return {
    ...state,
    timezones,
  };
}

export async function getPromptsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const profiles = await listPromptProfiles();
  const learnedReviewExamples = await listLearnedReviewExamples();
  return {
    ...state,
    promptProfiles: profiles,
    learnedReviewExamples,
  };
}

export function transactionBadge(transaction: Transaction) {
  if (needsTransactionManualReview(transaction)) return "warning";
  if (
    transaction.transactionClass === "income" ||
    transaction.transactionClass === "dividend"
  ) {
    return "positive";
  }
  return "neutral";
}
