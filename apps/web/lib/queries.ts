import {
  buildDashboardReadModel,
  buildDashboardSummary,
  buildIncomeReadModel,
  buildInvestmentsReadModel,
  buildSpendingReadModel,
} from "@myfinance/analytics";
import { NON_AI_RULE_SUMMARIES } from "@myfinance/classification";
import { createFinanceRepository } from "@myfinance/db";
import {
  FinanceDomainService,
  filterTransactionsByScope,
  getDatasetLatestDate,
  resolvePeriodSelection,
  type Scope,
  type Transaction,
} from "@myfinance/domain";

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

export async function resolveAppState(searchParams: RawSearchParams) {
  const params = await searchParams;
  const dataset = await repository.getDataset();
  const scopeParam = normalizeParam(params, "scope") ?? "consolidated";
  const currency = normalizeParam(params, "currency") === "USD" ? "USD" : "EUR";
  const periodParam = normalizeParam(params, "period") ?? "mtd";
  const entityBySlug = new Map(dataset.entities.map((entity) => [entity.slug, entity.id]));
  const scope: Scope = scopeParam.startsWith("account:")
    ? { kind: "account", accountId: scopeParam.replace("account:", "") }
    : scopeParam === "consolidated"
      ? { kind: "consolidated" }
      : { kind: "entity", entityId: entityBySlug.get(scopeParam) };
  const latestScopedTransactionDate = filterTransactionsByScope(dataset, scope)
    .map((row) => row.transactionDate)
    .sort()
    .at(-1);
  const referenceDate =
    normalizeParam(params, "asOf") ??
    latestScopedTransactionDate ??
    getDatasetLatestDate(dataset);
  const period = resolvePeriodSelection({
    preset: periodParam,
    start: normalizeParam(params, "start"),
    end: normalizeParam(params, "end"),
    referenceDate,
  });

  const scopeOptions = [
    { value: "consolidated", label: "Consolidated" },
    ...dataset.entities.map((entity) => ({ value: entity.slug, label: entity.displayName })),
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

export function formatCurrency(amount: string | null | undefined, currency: string) {
  if (amount === null || amount === undefined) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(Number(amount));
}

export function formatPercent(value: string | null | undefined) {
  if (value === null || value === undefined) return "N/A";
  return `${Number(value).toFixed(2)}%`;
}

export function formatDate(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(`${value}T00:00:00Z`));
}

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
export type TransactionsModel = Awaited<ReturnType<typeof getTransactionsModel>>;
export type AccountsModel = Awaited<ReturnType<typeof getAccountsModel>>;
export type ImportsModel = Awaited<ReturnType<typeof getImportsModel>>;
export type TemplatesModel = Awaited<ReturnType<typeof getTemplatesModel>>;
export type RulesModel = Awaited<ReturnType<typeof getRulesModel>>;
export type InvestmentsModel = Awaited<ReturnType<typeof getInvestmentsModel>>;
export type SpendingModel = Awaited<ReturnType<typeof getSpendingModel>>;
export type IncomeModel = Awaited<ReturnType<typeof getIncomeModel>>;
export type InsightsModel = Awaited<ReturnType<typeof getInsightsModel>>;

export async function getTransactionsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const ledger = await domainService.listTransactions(state.scope);
  return { ...state, ledger };
}

export async function getAccountsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const accounts = await domainService.listAccounts();
  return { ...state, accounts };
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
  return state;
}

export function transactionBadge(transaction: Transaction) {
  if (transaction.needsReview) return "warning";
  if (transaction.transactionClass === "income" || transaction.transactionClass === "dividend") {
    return "positive";
  }
  return "neutral";
}
