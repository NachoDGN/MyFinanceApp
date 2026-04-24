import type {
  AddOpeningPositionInput,
  ApplyRuleDraftInput,
  AuditEvent,
  CreateAccountInput,
  CreateCategoryInput,
  CreateEntityInput,
  CreateManualInvestmentInput,
  CreateRuleInput,
  CreateTemplateInput,
  CreditCardStatementImportInput,
  CreditCardStatementImportResult,
  DeleteAccountInput,
  DeleteCategoryInput,
  DeleteEntityInput,
  DeleteHoldingAdjustmentInput,
  DeleteManualInvestmentInput,
  DeleteTemplateInput,
  DomainDataset,
  ImportCommitResult,
  ImportExecutionInput,
  ImportPreviewResult,
  JobRunResult,
  PeriodSelection,
  QueueRuleDraftInput,
  RecordManualInvestmentValuationInput,
  ResetWorkspaceInput,
  ResetWorkspaceResult,
  Scope,
  Transaction,
  UpdateAccountInput,
  UpdateEntityInput,
  UpdateManualInvestmentInput,
  UpdateTransactionInput,
  UpdateWorkspaceProfileInput,
} from "./types";

export interface FinanceRepository {
  getDataset(): Promise<DomainDataset>;
  searchTransactions(input: {
    dataset: DomainDataset;
    scope: Scope;
    period: PeriodSelection;
    referenceDate: string;
    query: string;
  }): Promise<{
    query: string;
    rows: Array<{
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
        direction: "debit" | "credit" | "neutral";
        reviewState: "pending_enrichment" | "needs_review" | "resolved";
      } | null;
    }>;
    semanticCandidateCount: number;
    keywordCandidateCount: number;
    warnings: string[];
    filters: {
      accountIds: string[];
      entityIds: string[];
      accountTypes: Array<
        | "checking"
        | "savings"
        | "company_bank"
        | "brokerage_cash"
        | "brokerage_account"
        | "credit_card"
        | "other"
      >;
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
  }>;
  updateWorkspaceProfile(
    input: UpdateWorkspaceProfileInput,
  ): Promise<{ applied: boolean; profileId: string }>;
  createEntity(
    input: CreateEntityInput,
  ): Promise<{ applied: boolean; entityId: string }>;
  updateEntity(
    input: UpdateEntityInput,
  ): Promise<{ applied: boolean; entityId: string }>;
  deleteEntity(
    input: DeleteEntityInput,
  ): Promise<{ applied: boolean; entityId: string }>;
  createAccount(
    input: CreateAccountInput,
  ): Promise<{ applied: boolean; accountId: string }>;
  updateAccount(
    input: UpdateAccountInput,
  ): Promise<{ applied: boolean; accountId: string }>;
  deleteAccount(
    input: DeleteAccountInput,
  ): Promise<{ applied: boolean; accountId: string }>;
  resetWorkspace(input: ResetWorkspaceInput): Promise<ResetWorkspaceResult>;
  updateTransaction(input: UpdateTransactionInput): Promise<{
    applied: boolean;
    transaction: Transaction;
    auditEvent: AuditEvent;
    generatedRuleId?: string;
  }>;
  createRule(
    input: CreateRuleInput,
  ): Promise<{ applied: boolean; ruleId: string }>;
  createTemplate(
    input: CreateTemplateInput,
  ): Promise<{ applied: boolean; templateId: string }>;
  deleteTemplate(
    input: DeleteTemplateInput,
  ): Promise<{ applied: boolean; templateId: string }>;
  createCategory(
    input: CreateCategoryInput,
  ): Promise<{ applied: boolean; categoryCode: string }>;
  deleteCategory(
    input: DeleteCategoryInput,
  ): Promise<{ applied: boolean; categoryCode: string }>;
  addOpeningPosition(
    input: AddOpeningPositionInput,
  ): Promise<{ applied: boolean; adjustmentId: string }>;
  deleteHoldingAdjustment(
    input: DeleteHoldingAdjustmentInput,
  ): Promise<{ applied: boolean; adjustmentId: string }>;
  createManualInvestment(input: CreateManualInvestmentInput): Promise<{
    applied: boolean;
    manualInvestmentId: string;
    valuationId: string;
  }>;
  updateManualInvestment(input: UpdateManualInvestmentInput): Promise<{
    applied: boolean;
    manualInvestmentId: string;
  }>;
  recordManualInvestmentValuation(
    input: RecordManualInvestmentValuationInput,
  ): Promise<{
    applied: boolean;
    manualInvestmentId: string;
    valuationId: string;
  }>;
  deleteManualInvestment(
    input: DeleteManualInvestmentInput,
  ): Promise<{ applied: boolean; manualInvestmentId: string }>;
  queueRuleDraft(
    input: QueueRuleDraftInput,
  ): Promise<{ applied: boolean; jobId: string }>;
  applyRuleDraft(
    input: ApplyRuleDraftInput,
  ): Promise<{ applied: boolean; ruleId: string }>;
  previewImport(input: ImportExecutionInput): Promise<ImportPreviewResult>;
  commitImport(input: ImportExecutionInput): Promise<ImportCommitResult>;
  commitCreditCardStatementImport(
    input: CreditCardStatementImportInput,
  ): Promise<CreditCardStatementImportResult>;
  runPendingJobs(apply: boolean): Promise<JobRunResult>;
}

export function getAccountById(
  accounts: DomainDataset["accounts"],
  accountId: string,
) {
  return accounts.find((account) => account.id === accountId);
}
