import {
  filterTransactionsByReferenceDate,
  filterTransactionsByScope,
  isUncategorizedCategoryCode,
} from "@myfinance/domain";

import { AppShell } from "../../components/app-shell";
import { TransactionCategoryManagementPanel } from "../../components/transaction-category-management-panel";
import { buildHref, resolveAppState } from "../../lib/queries";

export default async function CategoriesPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await resolveAppState(searchParams);
  const entitiesById = new Map(
    model.dataset.entities.map((entity) => [entity.id, entity]),
  );
  const transactionsHref = buildHref("/transactions", model.navigationState, {});
  const scopedTransactions = filterTransactionsByReferenceDate(
    filterTransactionsByScope(model.dataset, model.scope),
    model.referenceDate,
  );
  const categoryPanelAccounts = model.dataset.accounts
    .filter((account) =>
      scopedTransactions.some((transaction) => transaction.accountId === account.id),
    )
    .map((account) => {
      const accountTransactions = scopedTransactions.filter(
        (transaction) => transaction.accountId === account.id,
      );

      return {
        id: account.id,
        displayName: account.displayName,
        institutionName: account.institutionName,
        entityName:
          entitiesById.get(account.entityId)?.displayName ?? account.entityId,
        assetDomain: account.assetDomain,
        totalTransactions: accountTransactions.length,
        categorizedTransactions: accountTransactions.filter(
          (transaction) =>
            Boolean(transaction.categoryCode) &&
            !isUncategorizedCategoryCode(transaction.categoryCode),
        ).length,
        uncategorizedTransactions: accountTransactions.filter(
          (transaction) =>
            !transaction.categoryCode ||
            isUncategorizedCategoryCode(transaction.categoryCode),
        ).length,
      };
    });
  const categoryPanelCategories = model.dataset.categories.map((category) => {
    const matchingTransactions = scopedTransactions.filter(
      (transaction) => transaction.categoryCode === category.code,
    );

    return {
      code: category.code,
      displayName: category.displayName,
      scopeKind: category.scopeKind,
      directionKind: category.directionKind,
      active: category.active,
      totalTransactionCount: matchingTransactions.length,
      lastTransactionDate:
        [...matchingTransactions]
          .sort((left, right) =>
            right.transactionDate.localeCompare(left.transactionDate),
          )
          .at(0)?.transactionDate ?? null,
      accountUsage: categoryPanelAccounts.map((account) => {
        const accountTransactions = matchingTransactions.filter(
          (transaction) => transaction.accountId === account.id,
        );

        return {
          accountId: account.id,
          transactionCount: accountTransactions.length,
          lastTransactionDate:
            [...accountTransactions]
              .sort((left, right) =>
                right.transactionDate.localeCompare(left.transactionDate),
              )
              .at(0)?.transactionDate ?? null,
        };
      }),
    };
  });
  const initialCategoryPanelAccountId =
    model.scope.kind === "account" ? model.scope.accountId ?? null : null;

  return (
    <AppShell
      pathname="/categories"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Categories</h1>
            <p className="page-subtitle">
              Category inventory, account-filtered usage, and direct add/delete
              controls in one place.
            </p>
          </div>
          <a className="btn-ghost" href={transactionsHref}>
            Open Transactions
          </a>
        </div>

        <TransactionCategoryManagementPanel
          accounts={categoryPanelAccounts}
          categories={categoryPanelCategories}
          initialAccountId={initialCategoryPanelAccountId}
          emptyStateCopy="No category definitions are available in the current scope."
        />
      </div>
    </AppShell>
  );
}
