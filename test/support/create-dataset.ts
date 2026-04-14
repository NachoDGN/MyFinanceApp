import type {
  Account,
  AccountBalanceSnapshot,
  ClassificationRule,
  DomainDataset,
  FxRate,
  InvestmentPosition,
  ManualInvestment,
  ManualInvestmentValuation,
  Security,
  SecurityPrice,
  Transaction,
} from "../../packages/domain/src/index.ts";

const profile = {
  id: "user-1",
  email: "dev@example.com",
  displayName: "Developer",
  defaultBaseCurrency: "EUR" as const,
  timezone: "Europe/Madrid",
  createdAt: "2026-01-01T00:00:00Z",
  workspaceSettingsJson: {},
};

const entity = {
  id: "entity-1",
  userId: profile.id,
  slug: "personal",
  displayName: "Personal",
  legalName: null,
  entityKind: "personal" as const,
  baseCurrency: "EUR" as const,
  active: true,
  createdAt: "2026-01-01T00:00:00Z",
};

export function createAccount(overrides: Partial<Account> = {}): Account {
  return {
    id: "account-1",
    userId: profile.id,
    entityId: entity.id,
    institutionName: "Santander",
    displayName: "Main Account",
    accountType: "checking",
    assetDomain: "cash",
    defaultCurrency: "EUR",
    openingBalanceOriginal: null,
    openingBalanceCurrency: null,
    openingBalanceDate: null,
    includeInConsolidation: true,
    isActive: true,
    importTemplateDefaultId: null,
    matchingAliases: [],
    accountSuffix: null,
    balanceMode: "statement",
    staleAfterDays: 7,
    lastImportedAt: "2026-04-03T08:00:00Z",
    createdAt: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function createTransaction(
  overrides: Partial<Transaction> = {},
): Transaction {
  const id = overrides.id ?? "tx-1";
  return {
    id,
    userId: profile.id,
    accountId: "account-1",
    accountEntityId: entity.id,
    economicEntityId: entity.id,
    importBatchId: null,
    providerName: null,
    providerRecordId: null,
    sourceFingerprint: `fp-${id}`,
    duplicateKey: null,
    transactionDate: "2026-04-01",
    postedDate: "2026-04-01",
    amountOriginal: "-100.00",
    currencyOriginal: "EUR",
    amountBaseEur: "-100.00",
    fxRateToEur: "1.00000000",
    descriptionRaw: "Sample Transaction",
    descriptionClean: "SAMPLE TRANSACTION",
    merchantNormalized: null,
    counterpartyName: null,
    transactionClass: "expense",
    categoryCode: "groceries",
    subcategoryCode: null,
    transferGroupId: null,
    relatedAccountId: null,
    relatedTransactionId: null,
    transferMatchStatus: "not_transfer",
    crossEntityFlag: false,
    reimbursementStatus: "none",
    classificationStatus: "rule",
    classificationSource: "user_rule",
    classificationConfidence: "1.00",
    needsReview: false,
    reviewReason: null,
    excludeFromAnalytics: false,
    correctionOfTransactionId: null,
    voidedAt: null,
    manualNotes: null,
    llmPayload: null,
    rawPayload: {},
    securityId: null,
    quantity: null,
    unitPriceOriginal: null,
    creditCardStatementStatus: "not_applicable",
    linkedCreditCardAccountId: null,
    createdAt: "2026-04-01T08:00:00Z",
    updatedAt: "2026-04-01T08:00:00Z",
    ...overrides,
  };
}

export function createInvestmentAccount(
  overrides: Partial<Account> = {},
): Account {
  return createAccount({
    id: "brokerage-1",
    institutionName: "Broker",
    displayName: "Brokerage",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "EUR",
    ...overrides,
  });
}

export function createInvestmentTransaction(
  account: Pick<Account, "id" | "entityId">,
  overrides: Partial<Transaction> = {},
): Transaction {
  return createTransaction({
    accountId: account.id,
    accountEntityId: account.entityId,
    economicEntityId: account.entityId,
    transactionClass: "unknown",
    categoryCode: "uncategorized_investment",
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    needsReview: true,
    reviewReason: "Needs LLM enrichment.",
    ...overrides,
  });
}

export function createSecurity(overrides: Partial<Security> = {}): Security {
  return {
    id: "security-1",
    providerName: "manual",
    providerSymbol: "ABC",
    canonicalSymbol: "ABC",
    displaySymbol: "ABC",
    name: "ABC Corp",
    exchangeName: "NYSE",
    micCode: "XNYS",
    assetType: "stock",
    quoteCurrency: "USD",
    country: "US",
    isin: null,
    figi: null,
    active: true,
    metadataJson: {},
    lastPriceRefreshAt: null,
    createdAt: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function createSecurityPrice(
  overrides: Partial<SecurityPrice> = {},
): SecurityPrice {
  return {
    securityId: "security-1",
    priceDate: "2026-04-03",
    quoteTimestamp: "2026-04-03T15:00:00Z",
    price: "10.00",
    currency: "USD",
    sourceName: "twelve_data",
    isRealtime: false,
    isDelayed: true,
    marketState: "closed",
    rawJson: {},
    createdAt: "2026-04-03T15:00:00Z",
    ...overrides,
  };
}

export function createFxRate(overrides: Partial<FxRate> = {}): FxRate {
  return {
    baseCurrency: "USD",
    quoteCurrency: "EUR",
    asOfDate: "2026-04-03",
    asOfTimestamp: "2026-04-03T15:00:00Z",
    rate: "0.500000",
    sourceName: "ecb",
    rawJson: {},
    ...overrides,
  };
}

export function createAccountBalanceSnapshot(
  overrides: Partial<AccountBalanceSnapshot> = {},
): AccountBalanceSnapshot {
  return {
    accountId: "account-1",
    asOfDate: "2026-04-03",
    balanceOriginal: "100.00",
    balanceCurrency: "EUR",
    balanceBaseEur: "100.00",
    sourceKind: "statement",
    importBatchId: null,
    ...overrides,
  };
}

export function createInvestmentPosition(
  overrides: Partial<InvestmentPosition> = {},
): InvestmentPosition {
  return {
    userId: profile.id,
    entityId: entity.id,
    accountId: "brokerage-1",
    securityId: "security-1",
    openQuantity: "4.00",
    openCostBasisEur: "15.00",
    avgCostEur: "3.75",
    realizedPnlEur: "0.00",
    dividendsEur: "0.00",
    interestEur: "0.00",
    feesEur: "0.00",
    lastTradeDate: "2026-04-01",
    lastRebuiltAt: "2026-04-03T16:00:00Z",
    provenanceJson: {},
    unrealizedComplete: true,
    ...overrides,
  };
}

export function createManualInvestment(
  overrides: Partial<ManualInvestment> = {},
): ManualInvestment {
  return {
    id: "manual-investment-1",
    userId: profile.id,
    entityId: entity.id,
    fundingAccountId: "account-1",
    label: "Manual Investment",
    matcherText: "manual investment",
    note: null,
    createdAt: "2026-04-02T09:00:00Z",
    updatedAt: "2026-04-02T09:00:00Z",
    ...overrides,
  };
}

export function createManualInvestmentValuation(
  overrides: Partial<ManualInvestmentValuation> = {},
): ManualInvestmentValuation {
  return {
    id: "manual-investment-valuation-1",
    userId: profile.id,
    manualInvestmentId: "manual-investment-1",
    snapshotDate: "2026-04-03",
    currentValueOriginal: "1012.50",
    currentValueCurrency: "EUR",
    note: "Manual mark-to-market",
    createdAt: "2026-04-03T10:00:00Z",
    updatedAt: "2026-04-03T10:00:00Z",
    ...overrides,
  };
}

export function createInvestmentDatasetFixture(
  input: Omit<Partial<DomainDataset>, "accounts" | "transactions"> & {
    account?: Partial<Account>;
    transactions?: Array<Partial<Transaction>>;
  } = {},
): DomainDataset {
  const account = createInvestmentAccount(input.account);
  const { account: _account, transactions = [], ...overrides } = input;

  return createDataset({
    ...overrides,
    accounts: [account],
    transactions: transactions.map((transaction) =>
      createInvestmentTransaction(account, transaction),
    ),
  });
}

export function createRule(
  overrides: Partial<ClassificationRule> = {},
): ClassificationRule {
  return {
    id: "rule-1",
    userId: profile.id,
    priority: 10,
    active: true,
    scopeJson: { global: true },
    conditionsJson: { normalized_description_regex: "NOTION" },
    outputsJson: {
      transaction_class: "expense",
      category_code: "software",
      merchant_normalized: "NOTION",
    },
    createdFromTransactionId: null,
    autoGenerated: false,
    hitCount: 0,
    lastHitAt: null,
    createdAt: "2026-01-01T00:00:00Z",
    updatedAt: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

export function createDataset(
  overrides: Partial<DomainDataset> = {},
): DomainDataset {
  return {
    schemaVersion: "v1",
    profile,
    entities: [entity],
    accounts: [createAccount()],
    bankConnections: [],
    bankAccountLinks: [],
    templates: [],
    importBatches: [],
    transactions: [],
    categories: [
      {
        code: "groceries",
        displayName: "Groceries",
        parentCode: null,
        scopeKind: "personal",
        directionKind: "expense",
        sortOrder: 10,
        active: true,
        metadataJson: {},
      },
      {
        code: "software",
        displayName: "Software",
        parentCode: null,
        scopeKind: "both",
        directionKind: "expense",
        sortOrder: 20,
        active: true,
        metadataJson: {},
      },
      {
        code: "salary",
        displayName: "Salary",
        parentCode: null,
        scopeKind: "both",
        directionKind: "income",
        sortOrder: 30,
        active: true,
        metadataJson: {},
      },
      {
        code: "uncategorized_investment",
        displayName: "Uncategorized Investment",
        parentCode: null,
        scopeKind: "system",
        directionKind: "investment",
        sortOrder: 40,
        active: true,
        metadataJson: {},
      },
    ],
    rules: [],
    auditEvents: [],
    jobs: [],
    learnedReviewExamples: [],
    accountBalanceSnapshots: [],
    securities: [],
    securityAliases: [],
    securityPrices: [],
    fxRates: [],
    holdingAdjustments: [],
    manualInvestments: [],
    manualInvestmentValuations: [],
    investmentPositions: [],
    dailyPortfolioSnapshots: [],
    monthlyCashFlowRollups: [],
    ...overrides,
  };
}
