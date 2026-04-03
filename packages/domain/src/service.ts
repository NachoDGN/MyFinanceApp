import type {
  AccountListResponse,
  ApplyRuleDraftInput,
  CreateAccountInput,
  CreateRuleInput,
  CreateTemplateInput,
  DeleteAccountInput,
  DeleteTemplateInput,
  HoldingsResponse,
  Job,
  QueueRuleDraftInput,
  ResetWorkspaceInput,
  RuleDraft,
  RuleDraftListResponse,
  RuleListResponse,
  Scope,
  TemplateListResponse,
  TransactionListResponse,
  UpdateTransactionInput,
  AddOpeningPositionInput,
} from "./types";
import type { FinanceRepository } from "./repository";
import {
  buildHoldingRows,
  filterTransactionsByPeriod,
  filterTransactionsByScope,
  getLatestBalanceSnapshots,
  getLatestInvestmentCashBalances,
  getDatasetLatestDate,
  resolvePeriodSelection,
  resolveScopeEntityIds,
  todayIso,
} from "./finance";
import { isRuleParserConfigured } from "./rule-drafts";

function toIsoTimestamp(value: string | Date | null | undefined) {
  if (!value) return "";
  return value instanceof Date ? value.toISOString() : value;
}

function ageInDays(
  referenceDate: string,
  timestamp: string | null | undefined,
) {
  if (!timestamp) return null;
  return Math.floor(
    (Date.parse(`${referenceDate}T12:00:00Z`) - new Date(timestamp).getTime()) /
      86400000,
  );
}

function buildQualitySummary(
  dataset: Awaited<ReturnType<FinanceRepository["getDataset"]>>,
  scope: Scope,
) {
  const referenceDate = todayIso();
  const period = resolvePeriodSelection({ preset: "mtd", referenceDate });
  const scopedTransactions = filterTransactionsByScope(dataset, scope);
  const scopedAccounts =
    scope.kind === "consolidated"
      ? dataset.accounts
      : scope.kind === "entity" && scope.entityId
        ? dataset.accounts.filter(
            (account) => account.entityId === scope.entityId,
          )
        : scope.kind === "account" && scope.accountId
          ? dataset.accounts.filter((account) => account.id === scope.accountId)
          : dataset.accounts;
  const staleAccounts = scopedAccounts
    .map((account) => {
      const threshold =
        account.staleAfterDays ??
        (account.assetDomain === "investment" ? 3 : 7);
      const ageDays =
        ageInDays(referenceDate, account.lastImportedAt) ?? threshold + 1;
      return { account, ageDays, threshold };
    })
    .filter((row) => row.ageDays > row.threshold)
    .map((row) => ({
      accountId: row.account.id,
      accountName: row.account.displayName,
      staleSinceDays: row.ageDays,
    }));

  return {
    pendingReviewCount: scopedTransactions.filter((row) => row.needsReview)
      .length,
    unclassifiedAmountMtdEur: filterTransactionsByPeriod(
      scopedTransactions,
      period,
    )
      .filter((row) => row.categoryCode?.startsWith("uncategorized"))
      .reduce((sum, row) => sum + Math.abs(Number(row.amountBaseEur)), 0)
      .toFixed(2),
    staleAccountsCount: staleAccounts.length,
    staleAccounts,
    latestImportDateByAccount: scopedAccounts.map((account) => ({
      accountId: account.id,
      accountName: account.displayName,
      latestImportDate: account.lastImportedAt?.slice(0, 10) ?? null,
    })),
    latestDataDateByScope: getDatasetLatestDate(dataset),
    priceFreshness: dataset.securityPrices.every((row) => row.isDelayed)
      ? "delayed"
      : "fresh",
  } as const;
}

export class FinanceDomainService {
  constructor(private readonly repository: FinanceRepository) {}

  private mapRuleDrafts(drafts: Job[]): RuleDraft[] {
    return drafts
      .filter((job) => job.jobType === "rule_parse")
      .map((job) => ({
        id: job.id,
        requestText:
          typeof job.payloadJson.requestText === "string"
            ? job.payloadJson.requestText
            : "",
        status: job.status,
        attempts: job.attempts,
        createdAt: toIsoTimestamp(job.createdAt),
        finishedAt: toIsoTimestamp(job.finishedAt) || null,
        lastError: job.lastError ?? null,
        parsedRule:
          job.payloadJson.parsedRule &&
          typeof job.payloadJson.parsedRule === "object"
            ? (job.payloadJson.parsedRule as RuleDraft["parsedRule"])
            : null,
        appliedRuleId:
          typeof job.payloadJson.appliedRuleId === "string"
            ? job.payloadJson.appliedRuleId
            : null,
      }))
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
  }

  async listTransactions(scope: Scope): Promise<TransactionListResponse> {
    const dataset = await this.repository.getDataset();
    const transactions = [...filterTransactionsByScope(dataset, scope)].sort(
      (a, b) =>
        `${b.transactionDate}${b.createdAt}`.localeCompare(
          `${a.transactionDate}${a.createdAt}`,
        ),
    );
    return {
      schemaVersion: "v1",
      scope,
      totalCount: transactions.length,
      transactions,
      quality: buildQualitySummary(dataset, scope),
      generatedAt: new Date().toISOString(),
    };
  }

  async listAccounts(): Promise<AccountListResponse> {
    const dataset = await this.repository.getDataset();
    return {
      schemaVersion: "v1",
      accounts: dataset.accounts,
      balances: getLatestBalanceSnapshots(dataset.accountBalanceSnapshots),
      generatedAt: new Date().toISOString(),
    };
  }

  async listTemplates(): Promise<TemplateListResponse> {
    const dataset = await this.repository.getDataset();
    return {
      schemaVersion: "v1",
      templates: dataset.templates,
      generatedAt: new Date().toISOString(),
    };
  }

  async listRules(): Promise<RuleListResponse> {
    const dataset = await this.repository.getDataset();
    return {
      schemaVersion: "v1",
      rules: [...dataset.rules].sort((a, b) => a.priority - b.priority),
      generatedAt: new Date().toISOString(),
    };
  }

  async listRuleDrafts(): Promise<RuleDraftListResponse> {
    const dataset = await this.repository.getDataset();
    return {
      schemaVersion: "v1",
      parserConfigured: isRuleParserConfigured(),
      drafts: this.mapRuleDrafts(dataset.jobs),
      generatedAt: new Date().toISOString(),
    };
  }

  async listHoldings(scope: Scope): Promise<HoldingsResponse> {
    const dataset = await this.repository.getDataset();
    const holdings = buildHoldingRows(dataset, scope);
    const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
    const brokerageCashEur = getLatestInvestmentCashBalances(dataset)
      .filter((row) => {
        const account = dataset.accounts.find(
          (candidate) => candidate.id === row.accountId,
        );
        return (
          account?.assetDomain === "investment" &&
          entityIds.has(account.entityId)
        );
      })
      .reduce((sum, row) => sum + Number(row.balanceBaseEur), 0)
      .toFixed(2);

    return {
      schemaVersion: "v1",
      scope,
      holdings,
      quoteFreshness: holdings.every((row) => row.quoteFreshness === "delayed")
        ? "delayed"
        : holdings.some((row) => row.quoteFreshness === "fresh")
          ? "fresh"
          : "missing",
      brokerageCashEur,
      generatedAt: new Date().toISOString(),
    };
  }

  previewImport(input: Parameters<FinanceRepository["previewImport"]>[0]) {
    return this.repository.previewImport(input);
  }

  commitImport(input: Parameters<FinanceRepository["commitImport"]>[0]) {
    return this.repository.commitImport(input);
  }

  createAccount(input: CreateAccountInput) {
    return this.repository.createAccount(input);
  }

  deleteAccount(input: DeleteAccountInput) {
    return this.repository.deleteAccount(input);
  }

  resetWorkspace(input: ResetWorkspaceInput) {
    return this.repository.resetWorkspace(input);
  }

  updateTransaction(input: UpdateTransactionInput) {
    return this.repository.updateTransaction(input);
  }

  createRule(input: CreateRuleInput) {
    return this.repository.createRule(input);
  }

  queueRuleDraft(input: QueueRuleDraftInput) {
    return this.repository.queueRuleDraft(input);
  }

  applyRuleDraft(input: ApplyRuleDraftInput) {
    return this.repository.applyRuleDraft(input);
  }

  createTemplate(input: CreateTemplateInput) {
    return this.repository.createTemplate(input);
  }

  deleteTemplate(input: DeleteTemplateInput) {
    return this.repository.deleteTemplate(input);
  }

  addOpeningPosition(input: AddOpeningPositionInput) {
    return this.repository.addOpeningPosition(input);
  }

  runPendingJobs(apply: boolean) {
    return this.repository.runPendingJobs(apply);
  }
}
