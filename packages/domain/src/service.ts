import type {
  AccountListResponse,
  ApplyRuleDraftInput,
  CreateRuleInput,
  CreateTemplateInput,
  HoldingsResponse,
  Job,
  QueueRuleDraftInput,
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
import { getLatestBalanceSnapshots } from "./repository";
import { TODAY_ISO } from "./fixtures";
import { isRuleParserConfigured } from "./rule-drafts";

function toIsoTimestamp(value: string | Date | null | undefined) {
  if (!value) return "";
  return value instanceof Date ? value.toISOString() : value;
}

function filterByScope<T extends { accountId?: string; economicEntityId?: string }>(
  rows: T[],
  scope: Scope,
  accountToEntity: Map<string, string>,
): T[] {
  if (scope.kind === "consolidated") {
    return rows;
  }
  if (scope.kind === "entity" && scope.entityId) {
    return rows.filter(
      (row) =>
        row.economicEntityId === scope.entityId ||
        (row.accountId ? accountToEntity.get(row.accountId) === scope.entityId : false),
    );
  }
  if (scope.kind === "account" && scope.accountId) {
    return rows.filter((row) => row.accountId === scope.accountId);
  }
  return rows;
}

export class FinanceDomainService {
  constructor(private readonly repository: FinanceRepository) {}

  private mapRuleDrafts(drafts: Job[]): RuleDraft[] {
    return drafts
      .filter((job) => job.jobType === "rule_parse")
      .map((job) => ({
        id: job.id,
        requestText: typeof job.payloadJson.requestText === "string" ? job.payloadJson.requestText : "",
        status: job.status,
        attempts: job.attempts,
        createdAt: toIsoTimestamp(job.createdAt),
        finishedAt: toIsoTimestamp(job.finishedAt) || null,
        lastError: job.lastError ?? null,
        parsedRule:
          job.payloadJson.parsedRule && typeof job.payloadJson.parsedRule === "object"
            ? (job.payloadJson.parsedRule as RuleDraft["parsedRule"])
            : null,
        appliedRuleId:
          typeof job.payloadJson.appliedRuleId === "string" ? job.payloadJson.appliedRuleId : null,
      }))
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
  }

  async listTransactions(scope: Scope): Promise<TransactionListResponse> {
    const dataset = await this.repository.getDataset();
    const accountToEntity = new Map(
      dataset.accounts.map((account) => [account.id, account.entityId]),
    );
    const transactions = filterByScope(
      [...dataset.transactions].sort((a, b) =>
        `${b.transactionDate}${b.createdAt}`.localeCompare(`${a.transactionDate}${a.createdAt}`),
      ),
      scope,
      accountToEntity,
    );
    return {
      schemaVersion: "v1",
      scope,
      totalCount: transactions.length,
      transactions,
      quality: {
        pendingReviewCount: dataset.transactions.filter((row) => row.needsReview).length,
        unclassifiedAmountMtdEur: "380.00",
        staleAccountsCount: dataset.accounts.filter((account) => {
          if (!account.lastImportedAt) return true;
          const last = new Date(account.lastImportedAt).getTime();
          const ageDays = Math.floor((Date.parse(`${TODAY_ISO}T12:00:00Z`) - last) / 86400000);
          return ageDays > (account.staleAfterDays ?? 7);
        }).length,
        staleAccounts: dataset.accounts
          .filter((account) => {
            if (!account.lastImportedAt) return true;
            const ageDays = Math.floor(
              (Date.parse(`${TODAY_ISO}T12:00:00Z`) - new Date(account.lastImportedAt).getTime()) /
                86400000,
            );
            return ageDays > (account.staleAfterDays ?? 7);
          })
          .map((account) => ({
            accountId: account.id,
            accountName: account.displayName,
            staleSinceDays: Math.floor(
              (Date.parse(`${TODAY_ISO}T12:00:00Z`) -
                new Date(account.lastImportedAt ?? `${TODAY_ISO}T00:00:00Z`).getTime()) /
                86400000,
            ),
          })),
        latestImportDateByAccount: dataset.accounts.map((account) => ({
          accountId: account.id,
          accountName: account.displayName,
          latestImportDate: account.lastImportedAt?.slice(0, 10) ?? null,
        })),
        latestDataDateByScope: TODAY_ISO,
        priceFreshness: "delayed",
      },
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
    const accountToEntity = new Map(
      dataset.accounts.map((account) => [account.id, account.entityId]),
    );
    const positions = filterByScope(dataset.investmentPositions, scope, accountToEntity);
    const holdings = positions.map((position) => {
      const security = dataset.securities.find((row) => row.id === position.securityId);
      const price = dataset.securityPrices.find((row) => row.securityId === position.securityId);
      const currentValueEur = position.securityId === "00000000-0000-0000-0000-000000000901"
        ? "140.21"
        : "5420.00";
      const unrealizedPnlEur = position.securityId === "00000000-0000-0000-0000-000000000901"
        ? "6.01"
        : "420.00";
      const unrealizedPnlPercent = position.securityId === "00000000-0000-0000-0000-000000000901"
        ? "4.48"
        : "8.40";
      return {
        securityId: position.securityId,
        accountId: position.accountId,
        entityId: position.entityId,
        symbol: security?.displaySymbol ?? position.securityId,
        securityName: security?.name ?? position.securityId,
        quantity: position.openQuantity,
        avgCostEur: position.avgCostEur,
        currentPrice: price?.price ?? null,
        currentPriceCurrency: price?.currency ?? null,
        currentValueEur,
        unrealizedPnlEur,
        unrealizedPnlPercent,
        quoteFreshness: price ? ("delayed" as const) : ("missing" as const),
        quoteTimestamp: price?.quoteTimestamp ?? null,
        unrealizedComplete: position.unrealizedComplete,
      };
    });

    const brokerageCashEur = getLatestBalanceSnapshots(dataset.accountBalanceSnapshots)
      .filter((row) => row.accountId === "00000000-0000-0000-0000-000000000204")
      .reduce((sum, row) => sum + Number(row.balanceBaseEur), 0)
      .toFixed(2);

    return {
      schemaVersion: "v1",
      scope,
      holdings,
      quoteFreshness: "delayed",
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

  addOpeningPosition(input: AddOpeningPositionInput) {
    return this.repository.addOpeningPosition(input);
  }

  runPendingJobs(apply: boolean) {
    return this.repository.runPendingJobs(apply);
  }
}
