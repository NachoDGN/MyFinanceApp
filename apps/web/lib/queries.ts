import {
  buildDashboardSummary,
  buildInsights,
  buildMetricResult,
} from "@myfinance/analytics";
import { NON_AI_RULE_SUMMARIES } from "@myfinance/classification";
import { createFinanceRepository } from "@myfinance/db";
import { FinanceDomainService, type Scope, type Transaction } from "@myfinance/domain";

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
  const period = normalizeParam(params, "period") === "ytd"
    ? { start: "2026-01-01", end: "2026-04-03", preset: "ytd" as const }
    : { start: "2026-04-01", end: "2026-04-03", preset: "mtd" as const };

  const entityBySlug = new Map(dataset.entities.map((entity) => [entity.slug, entity.id]));
  const scope: Scope = scopeParam.startsWith("account:")
    ? { kind: "account", accountId: scopeParam.replace("account:", "") }
    : scopeParam === "consolidated"
      ? { kind: "consolidated" }
      : { kind: "entity", entityId: entityBySlug.get(scopeParam) };

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
    period,
    scopeOptions,
  };
}

export function buildHref(
  pathname: string,
  current: { scopeParam: string; currency: string; period: string },
  overrides: Partial<{ scopeParam: string; currency: string; period: string }>,
) {
  const query = new URLSearchParams({
    scope: overrides.scopeParam ?? current.scopeParam,
    currency: overrides.currency ?? current.currency,
    period: overrides.period ?? current.period,
  });
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
  const summary = buildDashboardSummary(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
  });

  const personalMetrics = buildDashboardSummary(state.dataset, {
    scope: { kind: "entity", entityId: state.dataset.entities[0]?.id },
    displayCurrency: state.currency,
    period: state.period,
  }).metrics;
  const companyMetrics = buildDashboardSummary(state.dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: state.currency,
    period: state.period,
  }).metrics;

  return {
    ...state,
    summary,
    summaryBreakdown: {
      personal: personalMetrics.find((metric) => metric.metricId === "net_worth_current"),
      companies: {
        valueDisplay: (() => {
          const total = Number(
            companyMetrics.find((metric) => metric.metricId === "net_worth_current")?.valueDisplay ?? "0",
          );
          const personal = Number(
            personalMetrics.find((metric) => metric.metricId === "net_worth_current")?.valueDisplay ?? "0",
          );
          return (total - personal).toFixed(2);
        })(),
      },
    },
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
  const dashboard = buildDashboardSummary(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
  });
  return { ...state, accounts, dashboard };
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
  const holdings = await domainService.listHoldings(state.scope);
  const investmentRows = (await domainService.listTransactions(state.scope)).transactions.filter((row) =>
    [
      "investment_trade_buy",
      "investment_trade_sell",
      "dividend",
      "interest",
      "fee",
      "fx_conversion",
      "unknown",
    ].includes(row.transactionClass),
  );

  return {
    ...state,
    holdings,
    investmentRows,
    metrics: {
      portfolioValue: buildMetricResult(
        state.dataset,
        state.scope,
        state.currency,
        "portfolio_market_value_current",
      ),
      unrealized: buildMetricResult(
        state.dataset,
        state.scope,
        state.currency,
        "portfolio_unrealized_pnl_current",
      ),
      incomeYtd: buildMetricResult(state.dataset, state.scope, state.currency, "income_mtd_total"),
    },
  };
}

export async function getSpendingModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const summary = buildDashboardSummary(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
  });
  const transactions = (await domainService.listTransactions(state.scope)).transactions.filter((row) =>
    ["expense", "fee", "refund"].includes(row.transactionClass),
  );
  const merchantSpend = new Map<string, number>();
  for (const row of transactions) {
    const merchant = row.merchantNormalized ?? row.descriptionClean;
    const signed = row.transactionClass === "refund" ? -Number(row.amountBaseEur) : Math.abs(Number(row.amountBaseEur));
    merchantSpend.set(merchant, (merchantSpend.get(merchant) ?? 0) + signed);
  }
  const topMerchant = [...merchantSpend.entries()].sort((a, b) => b[1] - a[1])[0];
  return { ...state, summary, transactions, topMerchant };
}

export async function getIncomeModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  const summary = buildDashboardSummary(state.dataset, {
    scope: state.scope,
    displayCurrency: state.currency,
    period: state.period,
  });
  const transactions = (await domainService.listTransactions(state.scope)).transactions.filter((row) =>
    ["income", "dividend", "interest"].includes(row.transactionClass),
  );
  return { ...state, summary, transactions };
}

export async function getInsightsModel(searchParams: RawSearchParams) {
  const state = await resolveAppState(searchParams);
  return {
    ...state,
    insights: buildInsights(state.dataset, state.scope),
    summary: buildDashboardSummary(state.dataset, {
      scope: state.scope,
      displayCurrency: state.currency,
      period: state.period,
    }),
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
