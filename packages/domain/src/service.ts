import type {
  AccountListResponse,
  AddOpeningPositionInput,
  ApplyRuleDraftInput,
  CreateAccountInput,
  CreateEntityInput,
  CreateManualInvestmentInput,
  CreateRuleInput,
  CreateTemplateInput,
  DeleteAccountInput,
  DeleteEntityInput,
  DeleteHoldingAdjustmentInput,
  DeleteManualInvestmentInput,
  DeleteTemplateInput,
  HoldingsResponse,
  Job,
  QueueRuleDraftInput,
  RecordManualInvestmentValuationInput,
  ResetWorkspaceInput,
  RuleDraft,
  RuleDraftListResponse,
  RuleListResponse,
  Scope,
  TemplateListResponse,
  TransactionListResponse,
  UpdateAccountInput,
  UpdateManualInvestmentInput,
  UpdateEntityInput,
  UpdateWorkspaceProfileInput,
  UpdateTransactionInput,
} from "./types";
import type { FinanceRepository } from "./repository";
import {
  buildCryptoBalanceRows,
  buildLiveHoldingRows,
  filterTransactionsByPeriod,
  filterTransactionsByReferenceDate,
  filterTransactionsByScope,
  getLatestAccountBalances,
  getLatestInvestmentCashBalances,
  getScopeLatestDate,
  resolveScopeQuoteFreshness,
  resolvePeriodSelection,
  resolveScopeEntityIds,
  todayIso,
} from "./finance";
import { isRuleParserConfigured } from "./rule-drafts";
import {
  isTransactionPendingEnrichment,
  needsTransactionManualReview,
} from "./transaction-review";
import { resolveAccountStaleThresholdDays } from "./workspace-settings";

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
  options: {
    referenceDate?: string;
    period?: TransactionListResponse["period"];
  } = {},
) {
  const referenceDate = options.referenceDate ?? todayIso();
  const period =
    options.period ?? resolvePeriodSelection({ preset: "mtd", referenceDate });
  const scopedTransactions = filterTransactionsByReferenceDate(
    filterTransactionsByScope(dataset, scope),
    referenceDate,
  );
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
      const threshold = resolveAccountStaleThresholdDays(
        dataset.profile,
        account.assetDomain,
        account.staleAfterDays,
      );
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
    pendingEnrichmentCount: scopedTransactions.filter((row) =>
      isTransactionPendingEnrichment(row),
    ).length,
    pendingReviewCount: scopedTransactions.filter((row) =>
      needsTransactionManualReview(row),
    ).length,
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
    latestDataDateByScope: getScopeLatestDate(dataset, scope, referenceDate),
    priceFreshness: resolveScopeQuoteFreshness(dataset, scope, referenceDate),
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

  async listTransactions(
    scope: Scope,
    options: {
      referenceDate?: string;
      period?: TransactionListResponse["period"];
      query?: string | null;
    } = {},
  ): Promise<TransactionListResponse> {
    const dataset = await this.repository.getDataset();
    const referenceDate = options.referenceDate ?? todayIso();
    const period =
      options.period ??
      resolvePeriodSelection({ preset: "mtd", referenceDate });
    const defaultTransactions = [
      ...filterTransactionsByPeriod(
        filterTransactionsByReferenceDate(
          filterTransactionsByScope(dataset, scope),
          referenceDate,
        ),
        period,
      ),
    ].sort((a, b) =>
      `${b.transactionDate}${b.createdAt}`.localeCompare(
        `${a.transactionDate}${a.createdAt}`,
      ),
    );
    const query = options.query?.trim() ?? "";
    const search =
      query.length > 0
        ? await this.repository.searchTransactions({
            dataset,
            scope,
            period,
            referenceDate,
            query,
          })
        : null;
    const rows =
      search?.rows ??
      defaultTransactions.map((transaction) => ({
        transaction,
        originalText: transaction.descriptionRaw,
        contextualizedText: transaction.descriptionRaw,
        documentSummary: "",
        searchDiagnostics: null,
      }));
    const transactions = rows.map((row) => row.transaction);

    return {
      schemaVersion: "v1",
      scope,
      period,
      totalCount: transactions.length,
      transactions,
      rows,
      search: search
        ? {
            mode: "hybrid",
            query: search.query,
            semanticCandidateCount: search.semanticCandidateCount,
            keywordCandidateCount: search.keywordCandidateCount,
            warnings: search.warnings,
            filters: search.filters,
          }
        : {
            mode: "default",
            query: query || null,
            semanticCandidateCount: 0,
            keywordCandidateCount: 0,
            warnings: [],
            filters: {
              accountIds: [],
              entityIds: [],
              accountTypes: [],
              entityKinds: [],
              reviewStates: [],
              directions: [],
              dateStart: period.start,
              dateEnd: period.end,
              usedScopeFallback: false,
              usedPeriodFallback: false,
              hasExplicitScopeConstraint: false,
              hasExplicitTimeConstraint: false,
              explanation: "",
            },
          },
      quality: buildQualitySummary(dataset, scope, {
        referenceDate,
        period,
      }),
      generatedAt: new Date().toISOString(),
    };
  }

  async listAccounts(
    options: { referenceDate?: string } = {},
  ): Promise<AccountListResponse> {
    const dataset = await this.repository.getDataset();
    return {
      schemaVersion: "v1",
      accounts: dataset.accounts,
      balances: getLatestAccountBalances(
        dataset,
        options.referenceDate ?? todayIso(),
      ),
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

  async listHoldings(
    scope: Scope,
    referenceDate = todayIso(),
  ): Promise<HoldingsResponse> {
    const dataset = await this.repository.getDataset();
    const holdings = buildLiveHoldingRows(dataset, scope, referenceDate);
    const cryptoBalances = buildCryptoBalanceRows(
      dataset,
      scope,
      referenceDate,
    );
    const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
    const brokerageCashEur = getLatestInvestmentCashBalances(
      dataset,
      referenceDate,
    )
      .filter((row) => {
        const account = dataset.accounts.find(
          (candidate) => candidate.id === row.accountId,
        );
        return (
          account?.assetDomain === "investment" &&
          entityIds.has(account.entityId) &&
          (scope.kind !== "account" || account.id === scope.accountId)
        );
      })
      .reduce((sum, row) => sum + Number(row.balanceBaseEur), 0)
      .toFixed(2);
    const quoteStates = [
      ...holdings.map((row) => row.quoteFreshness),
      ...cryptoBalances.map((row) => row.quoteFreshness),
    ];

    return {
      schemaVersion: "v1",
      scope,
      holdings,
      cryptoBalances,
      quoteFreshness: quoteStates.includes("fresh")
        ? "fresh"
        : quoteStates.includes("delayed")
          ? "delayed"
          : quoteStates.includes("stale")
            ? "stale"
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

  commitCreditCardStatementImport(
    input: Parameters<FinanceRepository["commitCreditCardStatementImport"]>[0],
  ) {
    return this.repository.commitCreditCardStatementImport(input);
  }

  updateWorkspaceProfile(input: UpdateWorkspaceProfileInput) {
    return this.repository.updateWorkspaceProfile(input);
  }

  createEntity(input: CreateEntityInput) {
    return this.repository.createEntity(input);
  }

  updateEntity(input: UpdateEntityInput) {
    return this.repository.updateEntity(input);
  }

  deleteEntity(input: DeleteEntityInput) {
    return this.repository.deleteEntity(input);
  }

  createAccount(input: CreateAccountInput) {
    return this.repository.createAccount(input);
  }

  updateAccount(input: UpdateAccountInput) {
    return this.repository.updateAccount(input);
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

  createCategory(input: Parameters<FinanceRepository["createCategory"]>[0]) {
    return this.repository.createCategory(input);
  }

  deleteCategory(input: Parameters<FinanceRepository["deleteCategory"]>[0]) {
    return this.repository.deleteCategory(input);
  }

  addOpeningPosition(input: AddOpeningPositionInput) {
    return this.repository.addOpeningPosition(input);
  }

  deleteHoldingAdjustment(input: DeleteHoldingAdjustmentInput) {
    return this.repository.deleteHoldingAdjustment(input);
  }

  createManualInvestment(input: CreateManualInvestmentInput) {
    return this.repository.createManualInvestment(input);
  }

  updateManualInvestment(input: UpdateManualInvestmentInput) {
    return this.repository.updateManualInvestment(input);
  }

  recordManualInvestmentValuation(input: RecordManualInvestmentValuationInput) {
    return this.repository.recordManualInvestmentValuation(input);
  }

  deleteManualInvestment(input: DeleteManualInvestmentInput) {
    return this.repository.deleteManualInvestment(input);
  }

  runPendingJobs(apply: boolean) {
    return this.repository.runPendingJobs(apply);
  }
}
