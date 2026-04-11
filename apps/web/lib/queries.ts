import {
  buildDashboardReadModel,
  buildDashboardSummary,
  buildIncomeReadModel,
  buildInvestmentsReadModel,
  buildSpendingReadModel,
} from "@myfinance/analytics";
import { NON_AI_RULE_SUMMARIES } from "@myfinance/classification";
import {
  createFinanceRepository,
  getRevolutRuntimeStatus,
  listPromptProfiles,
} from "@myfinance/db";
import {
  FinanceDomainService,
  getScopeLatestDate,
  needsTransactionManualReview,
  resolvePeriodSelection,
  type Scope,
  type Transaction,
} from "@myfinance/domain";
import {
  buildEntityScopeOptions,
  parseWorkspaceSettings,
} from "./workspace-settings";
import {
  formatCurrency,
  formatDate,
  formatPercent,
  formatQuantity,
} from "./formatters";

export type RawSearchParams =
  | Promise<Record<string, string | string[] | undefined>>
  | Record<string, string | string[] | undefined>;

const repository = createFinanceRepository();
const domainService = new FinanceDomainService(repository);

function normalizeParam(
  params: Record<string, string | string[] | undefined>,
  key: string,
): string | undefined {
  const value = params[key];
  if (Array.isArray(value)) return value[0];
  return value;
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
  const referenceDate =
    normalizeParam(params, "asOf") ?? getScopeLatestDate(dataset, scope, today);
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
    periodParam,
    period,
    scopeOptions,
    workspaceSettings,
    navigationState: {
      scopeParam,
      currency,
      period: period.preset,
      referenceDate,
      start: period.preset === "custom" ? period.start : undefined,
      end: period.preset === "custom" ? period.end : undefined,
    },
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
  return `${pathname}?${query.toString()}`;
}

export { formatCurrency, formatDate, formatPercent, formatQuantity };

export async function getDashboardModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const { summary, summaryBreakdown } = buildDashboardReadModel(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
    referenceDate: state.referenceDate,
  });

  return {
    ...state,
    summary,
    summaryBreakdown,
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
  const ledger = await domainService.listTransactions(state.scope);
  return { ...state, ledger };
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
  const unresolvedCount = statementTransactions.filter(
    (transaction) => transaction.needsReview,
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
  const accounts = await domainService.listAccounts();
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
  return {
    ...state,
    ...buildInvestmentsReadModel(state.dataset, {
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
  return {
    ...state,
    promptProfiles: profiles,
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
