import type {
  Account,
  Category,
  DomainDataset,
  TransactionClass,
} from "./types";

const CASH_ACCOUNT_ALLOWED_TRANSACTION_CLASSES = [
  "income",
  "expense",
  "transfer_internal",
  "transfer_external",
  "suspected_internal_transfer_pending",
  "dividend",
  "interest",
  "fee",
  "refund",
  "reimbursement",
  "owner_contribution",
  "owner_draw",
  "loan_inflow",
  "loan_principal_payment",
  "loan_interest_payment",
  "fx_conversion",
  "balance_adjustment",
  "unknown",
] as const satisfies readonly TransactionClass[];

const INVESTMENT_ACCOUNT_ALLOWED_TRANSACTION_CLASSES = [
  "income",
  "expense",
  "transfer_internal",
  "transfer_external",
  "suspected_internal_transfer_pending",
  "investment_trade_buy",
  "investment_trade_sell",
  "dividend",
  "interest",
  "fee",
  "refund",
  "reimbursement",
  "owner_contribution",
  "owner_draw",
  "loan_inflow",
  "loan_principal_payment",
  "loan_interest_payment",
  "fx_conversion",
  "balance_adjustment",
  "unknown",
] as const satisfies readonly TransactionClass[];

function describeAccount(account: Pick<Account, "displayName" | "institutionName">) {
  return `${account.displayName} (${account.institutionName})`;
}

export function buildAllowedTransactionClassesForAccount(
  account: Pick<Account, "assetDomain">,
): readonly TransactionClass[] {
  return account.assetDomain === "investment"
    ? INVESTMENT_ACCOUNT_ALLOWED_TRANSACTION_CLASSES
    : CASH_ACCOUNT_ALLOWED_TRANSACTION_CLASSES;
}

export function buildAllowedCategoriesForAccount(
  dataset: Pick<DomainDataset, "categories" | "entities">,
  account: Pick<Account, "assetDomain" | "entityId">,
) {
  if (account.assetDomain === "investment") {
    return dataset.categories.filter(
      (category) => category.scopeKind === "investment",
    );
  }

  const entityKind =
    dataset.entities.find((entity) => entity.id === account.entityId)
      ?.entityKind ?? "personal";
  const allowedScopeKinds = new Set<Category["scopeKind"]>([
    entityKind === "company" ? "company" : "personal",
    "system",
    "both",
  ]);

  return dataset.categories.filter((category) =>
    allowedScopeKinds.has(category.scopeKind),
  );
}

export function getAllowedCategoryCodesForAccount(
  dataset: Pick<DomainDataset, "categories" | "entities">,
  account: Pick<Account, "assetDomain" | "entityId">,
) {
  return new Set(
    buildAllowedCategoriesForAccount(dataset, account).map(
      (category) => category.code,
    ),
  );
}

export function resolveConstrainedEconomicEntityId(
  dataset: Pick<DomainDataset, "entities">,
  account: Pick<Account, "assetDomain" | "entityId">,
  requestedEconomicEntityId: string | null | undefined,
  fallbackEconomicEntityId = account.entityId,
) {
  if (account.assetDomain === "cash") {
    return account.entityId;
  }

  if (
    requestedEconomicEntityId &&
    dataset.entities.some((entity) => entity.id === requestedEconomicEntityId)
  ) {
    return requestedEconomicEntityId;
  }

  return fallbackEconomicEntityId;
}

export function assertTransactionClassAllowedForAccount(
  account: Pick<Account, "assetDomain" | "displayName" | "institutionName">,
  transactionClass: string,
  context = "Transaction class",
) {
  if (
    buildAllowedTransactionClassesForAccount(account).includes(
      transactionClass as TransactionClass,
    )
  ) {
    return;
  }

  throw new Error(
    `${context} "${transactionClass}" is not allowed for ${describeAccount(account)}.`,
  );
}

export function assertCategoryCodeAllowedForAccount(
  dataset: Pick<DomainDataset, "categories" | "entities">,
  account: Pick<
    Account,
    "assetDomain" | "entityId" | "displayName" | "institutionName"
  >,
  categoryCode: string,
  context = "Category",
) {
  if (getAllowedCategoryCodesForAccount(dataset, account).has(categoryCode)) {
    return;
  }

  throw new Error(
    `${context} "${categoryCode}" is not allowed for ${describeAccount(account)}.`,
  );
}

export function assertEconomicEntityAllowedForAccount(
  dataset: Pick<DomainDataset, "entities">,
  account: Pick<
    Account,
    "assetDomain" | "entityId" | "displayName" | "institutionName"
  >,
  economicEntityId: string,
  context = "Economic entity",
) {
  const resolved = resolveConstrainedEconomicEntityId(
    dataset,
    account,
    economicEntityId,
    account.entityId,
  );

  if (resolved === economicEntityId) {
    return;
  }

  throw new Error(
    `${context} "${economicEntityId}" is not allowed for ${describeAccount(account)}. Cash-account attribution stays on the owning account.`,
  );
}

export function resolveRuleScopeAccounts(
  dataset: Pick<DomainDataset, "accounts">,
  scopeJson: Record<string, unknown>,
) {
  const scopedAccountId =
    typeof scopeJson.account_id === "string" ? scopeJson.account_id : null;
  const scopedEntityId =
    typeof scopeJson.entity_id === "string" ? scopeJson.entity_id : null;

  if (scopedAccountId) {
    return dataset.accounts.filter((account) => account.id === scopedAccountId);
  }

  if (scopedEntityId) {
    return dataset.accounts.filter((account) => account.entityId === scopedEntityId);
  }

  return dataset.accounts;
}

export function assertRuleOutputsAllowedForScope(
  dataset: Pick<DomainDataset, "accounts" | "categories" | "entities">,
  scopeJson: Record<string, unknown>,
  outputsJson: Record<string, unknown>,
) {
  const scopedAccounts = resolveRuleScopeAccounts(dataset, scopeJson);

  if (scopedAccounts.length === 0) {
    throw new Error("Rule scope did not match any accounts.");
  }

  for (const account of scopedAccounts) {
    if (typeof outputsJson.transaction_class === "string") {
      assertTransactionClassAllowedForAccount(
        account,
        outputsJson.transaction_class,
        "Rule transaction class",
      );
    }

    if (typeof outputsJson.category_code === "string") {
      assertCategoryCodeAllowedForAccount(
        dataset,
        account,
        outputsJson.category_code,
        "Rule category",
      );
    }

    if (typeof outputsJson.economic_entity_id_override === "string") {
      assertEconomicEntityAllowedForAccount(
        dataset,
        account,
        outputsJson.economic_entity_id_override,
        "Rule economic entity override",
      );
    }
  }
}
