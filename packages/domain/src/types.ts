export type CurrencyCode = "EUR" | "USD" | (string & {});
export type EntityKind = "personal" | "company";
export type AccountType =
  | "checking"
  | "savings"
  | "company_bank"
  | "brokerage_cash"
  | "brokerage_account"
  | "credit_card"
  | "other";
export type AssetDomain = "cash" | "investment";
export type FileKind = "csv" | "xlsx";
export type ImportBatchStatus =
  | "previewed"
  | "committed"
  | "failed"
  | "processing"
  | "queued";
export type TransactionClass =
  | "income"
  | "expense"
  | "transfer_internal"
  | "transfer_external"
  | "suspected_internal_transfer_pending"
  | "investment_trade_buy"
  | "investment_trade_sell"
  | "dividend"
  | "interest"
  | "fee"
  | "refund"
  | "reimbursement"
  | "owner_contribution"
  | "owner_draw"
  | "loan_inflow"
  | "loan_principal_payment"
  | "loan_interest_payment"
  | "fx_conversion"
  | "balance_adjustment"
  | "unknown";
export type TransferMatchStatus =
  | "matched"
  | "suspected_pending"
  | "manual"
  | "not_transfer";
export type ReimbursementStatus =
  | "none"
  | "expected"
  | "received"
  | "linked";
export type ClassificationStatus =
  | "manual_override"
  | "rule"
  | "transfer_match"
  | "investment_parser"
  | "llm"
  | "unknown";
export type ClassificationSource =
  | "manual"
  | "user_rule"
  | "transfer_matcher"
  | "investment_parser"
  | "alias_resolver"
  | "llm"
  | "system_fallback";
export type CategoryScopeKind =
  | "personal"
  | "company"
  | "investment"
  | "both"
  | "system";
export type CategoryDirectionKind =
  | "income"
  | "expense"
  | "neutral"
  | "investment";
export type AuditSourceChannel = "web" | "cli" | "worker" | "system";
export type ActorType = "user" | "agent" | "system";
export type JobStatus = "queued" | "running" | "completed" | "failed";
export type JobType =
  | "classification"
  | "transfer_rematch"
  | "security_resolution"
  | "price_refresh"
  | "position_rebuild"
  | "metric_refresh"
  | "insight_refresh"
  | "rule_parse";
export type ScopeKind = "consolidated" | "entity" | "account";
export type MetricUnitType = "currency" | "percent" | "count" | "date";
export type MetricClass = "current_value" | "flow" | "quality";
export type SupportedFilter =
  | "scope"
  | "period"
  | "entity"
  | "account"
  | "category"
  | "merchant"
  | "counterparty";
export type PriceFreshness = "fresh" | "delayed" | "stale" | "missing";

export interface Profile {
  id: string;
  email: string;
  displayName: string;
  defaultBaseCurrency: CurrencyCode;
  timezone: string;
  createdAt: string;
}

export interface Entity {
  id: string;
  userId: string;
  slug: string;
  displayName: string;
  legalName?: string | null;
  entityKind: EntityKind;
  baseCurrency: CurrencyCode;
  active: boolean;
  createdAt: string;
}

export interface Account {
  id: string;
  userId: string;
  entityId: string;
  institutionName: string;
  displayName: string;
  accountType: AccountType;
  assetDomain: AssetDomain;
  defaultCurrency: CurrencyCode;
  openingBalanceOriginal?: string | null;
  openingBalanceCurrency?: CurrencyCode | null;
  openingBalanceDate?: string | null;
  includeInConsolidation: boolean;
  isActive: boolean;
  importTemplateDefaultId?: string | null;
  matchingAliases: string[];
  accountSuffix?: string | null;
  balanceMode: "statement" | "computed";
  staleAfterDays?: number | null;
  lastImportedAt?: string | null;
  createdAt: string;
}

export interface ImportTemplate {
  id: string;
  userId: string;
  name: string;
  institutionName: string;
  compatibleAccountType: AccountType;
  fileKind: FileKind;
  sheetName?: string | null;
  headerRowIndex: number;
  rowsToSkipBeforeHeader: number;
  rowsToSkipAfterHeader: number;
  delimiter?: string | null;
  encoding?: string | null;
  decimalSeparator?: string | null;
  thousandsSeparator?: string | null;
  dateFormat: string;
  defaultCurrency: CurrencyCode;
  columnMapJson: Record<string, string>;
  signLogicJson: Record<string, string | string[] | boolean>;
  normalizationRulesJson: Record<string, string | string[] | boolean>;
  active: boolean;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export interface ImportBatch {
  id: string;
  userId: string;
  accountId: string;
  templateId: string;
  storagePath: string;
  originalFilename: string;
  fileSha256: string;
  status: ImportBatchStatus;
  rowCountDetected: number;
  rowCountParsed: number;
  rowCountInserted: number;
  rowCountDuplicates: number;
  rowCountFailed: number;
  previewSummaryJson: Record<string, unknown>;
  commitSummaryJson: Record<string, unknown>;
  importedByActor: string;
  importedAt: string;
  classificationTriggeredAt?: string | null;
  notes?: string | null;
  detectedDateRange?: { start: string; end: string } | null;
}

export interface Transaction {
  id: string;
  userId: string;
  accountId: string;
  accountEntityId: string;
  economicEntityId: string;
  importBatchId?: string | null;
  sourceFingerprint: string;
  duplicateKey?: string | null;
  transactionDate: string;
  postedDate?: string | null;
  amountOriginal: string;
  currencyOriginal: CurrencyCode;
  amountBaseEur: string;
  fxRateToEur?: string | null;
  descriptionRaw: string;
  descriptionClean: string;
  merchantNormalized?: string | null;
  counterpartyName?: string | null;
  transactionClass: TransactionClass;
  categoryCode?: string | null;
  subcategoryCode?: string | null;
  transferGroupId?: string | null;
  relatedAccountId?: string | null;
  relatedTransactionId?: string | null;
  transferMatchStatus: TransferMatchStatus;
  crossEntityFlag: boolean;
  reimbursementStatus: ReimbursementStatus;
  classificationStatus: ClassificationStatus;
  classificationSource: ClassificationSource;
  classificationConfidence: string;
  needsReview: boolean;
  reviewReason?: string | null;
  excludeFromAnalytics: boolean;
  correctionOfTransactionId?: string | null;
  voidedAt?: string | null;
  manualNotes?: string | null;
  llmPayload?: Record<string, unknown> | null;
  rawPayload: Record<string, unknown>;
  securityId?: string | null;
  quantity?: string | null;
  unitPriceOriginal?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface Category {
  code: string;
  displayName: string;
  parentCode?: string | null;
  scopeKind: CategoryScopeKind;
  directionKind: CategoryDirectionKind;
  sortOrder: number;
  active: boolean;
  metadataJson: Record<string, unknown>;
}

export interface ClassificationRule {
  id: string;
  userId: string;
  priority: number;
  active: boolean;
  scopeJson: Record<string, unknown>;
  conditionsJson: Record<string, unknown>;
  outputsJson: Record<string, unknown>;
  createdFromTransactionId?: string | null;
  autoGenerated: boolean;
  hitCount: number;
  lastHitAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface AuditEvent {
  id: string;
  actorType: ActorType;
  actorId?: string | null;
  actorName?: string | null;
  sourceChannel: AuditSourceChannel;
  commandName: string;
  objectType: string;
  objectId: string;
  beforeJson?: Record<string, unknown> | null;
  afterJson?: Record<string, unknown> | null;
  createdAt: string;
  notes?: string | null;
}

export interface Job {
  id: string;
  jobType: JobType;
  payloadJson: Record<string, unknown>;
  status: JobStatus;
  attempts: number;
  availableAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  lastError?: string | null;
  lockedBy?: string | null;
  createdAt: string;
}

export interface RuleDraftParseResult {
  title: string;
  summary: string;
  priority: number;
  scopeJson: Record<string, unknown>;
  conditionsJson: Record<string, unknown>;
  outputsJson: Record<string, unknown>;
  confidence: string;
  explanation: string[];
  parseSource: "llm" | "fallback";
  model?: string | null;
  generatedAt: string;
}

export interface RuleDraft {
  id: string;
  requestText: string;
  status: JobStatus;
  attempts: number;
  createdAt: string;
  finishedAt?: string | null;
  lastError?: string | null;
  parsedRule?: RuleDraftParseResult | null;
  appliedRuleId?: string | null;
}

export interface AccountBalanceSnapshot {
  accountId: string;
  asOfDate: string;
  balanceOriginal: string;
  balanceCurrency: CurrencyCode;
  balanceBaseEur: string;
  sourceKind: "statement" | "computed";
  importBatchId?: string | null;
}

export interface Security {
  id: string;
  providerName: string;
  providerSymbol: string;
  canonicalSymbol: string;
  displaySymbol: string;
  name: string;
  exchangeName: string;
  micCode?: string | null;
  assetType: "stock" | "etf" | "cash" | "other";
  quoteCurrency: CurrencyCode;
  country?: string | null;
  isin?: string | null;
  figi?: string | null;
  active: boolean;
  metadataJson: Record<string, unknown>;
  lastPriceRefreshAt?: string | null;
  createdAt: string;
}

export interface SecurityAlias {
  id: string;
  securityId: string;
  aliasTextNormalized: string;
  aliasSource: "manual" | "template" | "provider";
  templateId?: string | null;
  confidence: string;
  createdAt: string;
}

export interface SecurityPrice {
  securityId: string;
  priceDate: string;
  quoteTimestamp: string;
  price: string;
  currency: CurrencyCode;
  sourceName: string;
  isRealtime: boolean;
  isDelayed: boolean;
  marketState?: string | null;
  rawJson: Record<string, unknown>;
  createdAt: string;
}

export interface FxRate {
  baseCurrency: CurrencyCode;
  quoteCurrency: CurrencyCode;
  asOfDate: string;
  asOfTimestamp: string;
  rate: string;
  sourceName: string;
  rawJson: Record<string, unknown>;
}

export interface HoldingAdjustment {
  id: string;
  userId: string;
  entityId: string;
  accountId: string;
  securityId: string;
  effectiveDate: string;
  shareDelta: string;
  costBasisDeltaEur?: string | null;
  reason: string;
  note?: string | null;
  createdAt: string;
}

export interface InvestmentPosition {
  userId: string;
  entityId: string;
  accountId: string;
  securityId: string;
  openQuantity: string;
  openCostBasisEur: string;
  avgCostEur: string;
  realizedPnlEur: string;
  dividendsEur: string;
  interestEur: string;
  feesEur: string;
  lastTradeDate?: string | null;
  lastRebuiltAt: string;
  provenanceJson: Record<string, unknown>;
  unrealizedComplete: boolean;
}

export interface DailyPortfolioSnapshot {
  snapshotDate: string;
  userId: string;
  entityId: string;
  accountId?: string | null;
  securityId?: string | null;
  marketValueEur?: string | null;
  costBasisEur?: string | null;
  unrealizedPnlEur?: string | null;
  cashBalanceEur?: string | null;
  totalPortfolioValueEur: string;
  generatedAt: string;
}

export interface MonthlyCashFlowRollup {
  month: string;
  entityId: string;
  incomeEur: string;
  spendingEur: string;
  operatingNetEur: string;
}

export interface Scope {
  kind: ScopeKind;
  entityId?: string;
  accountId?: string;
}

export interface PeriodSelection {
  start: string;
  end: string;
  preset: "mtd" | "ytd" | "custom";
}

export interface MetricDefinition {
  metricId: string;
  displayName: string;
  description: string;
  metricClass: MetricClass;
  unitType: MetricUnitType;
  supportedScopes: ScopeKind[];
  supportedFilters: SupportedFilter[];
  sourceQueryOrView: string;
  defaultComparison: string;
  explanationQuery: string;
  freshnessPolicy: string;
  displayHints: Record<string, unknown>;
}

export interface MetricResult {
  metricId: string;
  displayName: string;
  unitType: MetricUnitType;
  baseCurrency: "EUR";
  displayCurrency: CurrencyCode;
  valueBaseEur: string | null;
  valueDisplay: string | null;
  comparisonValueBaseEur?: string | null;
  comparisonValueDisplay?: string | null;
  deltaDisplay?: string | null;
  deltaPercent?: string | null;
  asOfDate?: string | null;
  explanation: string;
}

export interface QualitySummary {
  pendingReviewCount: number;
  unclassifiedAmountMtdEur: string;
  staleAccountsCount: number;
  staleAccounts: Array<{
    accountId: string;
    accountName: string;
    staleSinceDays: number;
  }>;
  latestImportDateByAccount: Array<{
    accountId: string;
    accountName: string;
    latestImportDate: string | null;
  }>;
  latestDataDateByScope: string | null;
  priceFreshness: PriceFreshness;
}

export interface InsightCard {
  id: string;
  title: string;
  severity: "info" | "warning" | "positive";
  body: string;
  evidence: string[];
}

export interface HoldingRow {
  securityId: string;
  accountId: string;
  entityId: string;
  symbol: string;
  securityName: string;
  quantity: string;
  avgCostEur: string;
  currentPrice: string | null;
  currentPriceCurrency: CurrencyCode | null;
  currentValueEur: string | null;
  unrealizedPnlEur: string | null;
  unrealizedPnlPercent: string | null;
  quoteFreshness: PriceFreshness;
  quoteTimestamp: string | null;
  unrealizedComplete: boolean;
}

export interface DomainDataset {
  schemaVersion: "v1";
  profile: Profile;
  entities: Entity[];
  accounts: Account[];
  templates: ImportTemplate[];
  importBatches: ImportBatch[];
  transactions: Transaction[];
  categories: Category[];
  rules: ClassificationRule[];
  auditEvents: AuditEvent[];
  jobs: Job[];
  accountBalanceSnapshots: AccountBalanceSnapshot[];
  securities: Security[];
  securityAliases: SecurityAlias[];
  securityPrices: SecurityPrice[];
  fxRates: FxRate[];
  holdingAdjustments: HoldingAdjustment[];
  investmentPositions: InvestmentPosition[];
  dailyPortfolioSnapshots: DailyPortfolioSnapshot[];
  monthlyCashFlowRollups: MonthlyCashFlowRollup[];
}

export interface DashboardSummaryResponse {
  schemaVersion: "v1";
  scope: Scope;
  period: PeriodSelection;
  metrics: MetricResult[];
  monthlySeries: Array<{
    month: string;
    incomeEur: string;
    spendingEur: string;
    operatingNetEur: string;
  }>;
  spendingByCategory: Array<{
    categoryCode: string;
    label: string;
    amountEur: string;
  }>;
  portfolioAllocation: Array<{
    label: string;
    amountEur: string;
    allocationPercent: string;
  }>;
  topHoldings: HoldingRow[];
  recentLargeTransactions: Transaction[];
  insights: InsightCard[];
  quality: QualitySummary;
  generatedAt: string;
}

export interface TransactionListResponse {
  schemaVersion: "v1";
  scope: Scope;
  period?: PeriodSelection;
  totalCount: number;
  transactions: Transaction[];
  quality: QualitySummary;
  generatedAt: string;
}

export interface HoldingsResponse {
  schemaVersion: "v1";
  scope: Scope;
  holdings: HoldingRow[];
  quoteFreshness: PriceFreshness;
  brokerageCashEur: string;
  generatedAt: string;
}

export interface RuleListResponse {
  schemaVersion: "v1";
  rules: ClassificationRule[];
  generatedAt: string;
}

export interface RuleDraftListResponse {
  schemaVersion: "v1";
  parserConfigured: boolean;
  drafts: RuleDraft[];
  generatedAt: string;
}

export interface TemplateListResponse {
  schemaVersion: "v1";
  templates: ImportTemplate[];
  generatedAt: string;
}

export interface AccountListResponse {
  schemaVersion: "v1";
  accounts: Account[];
  balances: AccountBalanceSnapshot[];
  generatedAt: string;
}

export interface ImportPreviewResult {
  schemaVersion: "v1";
  accountId: string;
  templateId: string;
  originalFilename: string;
  rowCountDetected: number;
  rowCountParsed: number;
  rowCountDuplicates: number;
  rowCountFailed: number;
  dateRange: { start: string; end: string } | null;
  sampleRows: Array<Record<string, unknown>>;
  parseErrors: Array<{ row: number; message: string }>;
}

export interface ImportExecutionInput {
  accountId: string;
  templateId: string;
  originalFilename?: string;
  filePath?: string | null;
}

export interface ImportCommitResult extends ImportPreviewResult {
  importBatchId: string;
  rowCountInserted: number;
  transactionIds: string[];
  jobsQueued: JobType[];
}

export interface JobRunResult {
  schemaVersion: "v1";
  applied: boolean;
  processedJobs: Array<{
    id: string;
    jobType: JobType;
    status: JobStatus;
  }>;
  generatedAt: string;
}

export interface UpdateTransactionInput {
  transactionId: string;
  patch: Partial<
    Pick<
      Transaction,
      | "transactionClass"
      | "categoryCode"
      | "economicEntityId"
      | "merchantNormalized"
      | "counterpartyName"
      | "needsReview"
      | "reviewReason"
      | "excludeFromAnalytics"
      | "securityId"
      | "manualNotes"
    >
  >;
  createRuleFromTransaction?: boolean;
  sourceChannel: AuditSourceChannel;
  actorName: string;
  apply: boolean;
}

export interface CreateRuleInput {
  priority: number;
  scopeJson: Record<string, unknown>;
  conditionsJson: Record<string, unknown>;
  outputsJson: Record<string, unknown>;
  actorName: string;
  sourceChannel: AuditSourceChannel;
  apply: boolean;
}

export interface QueueRuleDraftInput {
  requestText: string;
  actorName: string;
  sourceChannel: AuditSourceChannel;
  apply: boolean;
}

export interface ApplyRuleDraftInput {
  jobId: string;
  actorName: string;
  sourceChannel: AuditSourceChannel;
  apply: boolean;
}

export interface CreateTemplateInput {
  template: Omit<ImportTemplate, "id" | "createdAt" | "updatedAt" | "version">;
  actorName: string;
  sourceChannel: AuditSourceChannel;
  apply: boolean;
}

export interface AddOpeningPositionInput {
  accountId: string;
  entityId: string;
  securityId: string;
  effectiveDate: string;
  shareDelta: string;
  costBasisDeltaEur?: string | null;
  actorName: string;
  sourceChannel: AuditSourceChannel;
  apply: boolean;
}
