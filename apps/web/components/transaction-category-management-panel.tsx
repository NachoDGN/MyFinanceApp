"use client";

import { useRouter } from "next/navigation";
import { useDeferredValue, useState, useTransition } from "react";

import { createCategoryAction, deleteCategoryAction } from "../app/actions";
import { formatDate } from "../lib/formatters";
import { SectionCard } from "./primitives";

const ALL_ACCOUNTS_ID = "__all_accounts__";

export type TransactionCategoryManagementAccount = {
  id: string;
  displayName: string;
  institutionName?: string | null;
  entityName?: string | null;
  assetDomain?: string | null;
  totalTransactions: number;
  categorizedTransactions: number;
  uncategorizedTransactions: number;
};

export type TransactionCategoryManagementAccountUsage = {
  accountId: string;
  transactionCount: number;
  amountDisplay?: string | null;
  lastTransactionDate?: string | null;
};

export type TransactionCategoryManagementCategory = {
  code: string;
  displayName: string;
  scopeKind: string;
  directionKind: string;
  active?: boolean;
  totalTransactionCount: number;
  totalAmountDisplay?: string | null;
  lastTransactionDate?: string | null;
  manageHref?: string;
  deleteHref?: string;
  accountUsage: TransactionCategoryManagementAccountUsage[];
};

export type TransactionCategoryManagementPanelProps = {
  accounts: TransactionCategoryManagementAccount[];
  categories: TransactionCategoryManagementCategory[];
  initialAccountId?: string | null;
  addCategoryHref?: string;
  addCategoryLabel?: string;
  catalogHref?: string;
  catalogLabel?: string;
  emptyStateCopy?: string;
};

type ScopeSnapshot = {
  totalTransactions: number;
  categorizedTransactions: number;
  uncategorizedTransactions: number;
};

type ScopedCategory = TransactionCategoryManagementCategory & {
  scopedTransactionCount: number;
  scopedAmountDisplay: string | null;
  scopedLastTransactionDate: string | null;
  usedAccountCount: number;
  otherAccountCount: number;
};

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function formatPercent(value: number) {
  return `${Math.round(clamp(value, 0, 1) * 100)}%`;
}

function humanizeToken(value: string) {
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function resolveSelectedAccountId(
  initialAccountId: string | null | undefined,
  accounts: TransactionCategoryManagementAccount[],
) {
  return initialAccountId &&
    accounts.some((account) => account.id === initialAccountId)
    ? initialAccountId
    : ALL_ACCOUNTS_ID;
}

function summarizeAccounts(accounts: TransactionCategoryManagementAccount[]) {
  return accounts.reduce<ScopeSnapshot>(
    (summary, account) => ({
      totalTransactions: summary.totalTransactions + account.totalTransactions,
      categorizedTransactions:
        summary.categorizedTransactions + account.categorizedTransactions,
      uncategorizedTransactions:
        summary.uncategorizedTransactions + account.uncategorizedTransactions,
    }),
    {
      totalTransactions: 0,
      categorizedTransactions: 0,
      uncategorizedTransactions: 0,
    },
  );
}

function getScopeSnapshot(
  selectedAccountId: string,
  accounts: TransactionCategoryManagementAccount[],
) {
  if (selectedAccountId === ALL_ACCOUNTS_ID) {
    return summarizeAccounts(accounts);
  }

  return (
    accounts.find((account) => account.id === selectedAccountId) ?? {
      totalTransactions: 0,
      categorizedTransactions: 0,
      uncategorizedTransactions: 0,
    }
  );
}

function getScopedCategory(
  category: TransactionCategoryManagementCategory,
  selectedAccountId: string,
) {
  if (selectedAccountId === ALL_ACCOUNTS_ID) {
    return {
      ...category,
      scopedTransactionCount: category.totalTransactionCount,
      scopedAmountDisplay: category.totalAmountDisplay ?? null,
      scopedLastTransactionDate: category.lastTransactionDate ?? null,
      usedAccountCount: category.accountUsage.filter(
        (usage) => usage.transactionCount > 0,
      ).length,
      otherAccountCount: 0,
    } satisfies ScopedCategory;
  }

  const scopedUsage =
    category.accountUsage.find(
      (usage) => usage.accountId === selectedAccountId,
    ) ?? null;

  return {
    ...category,
    scopedTransactionCount: scopedUsage?.transactionCount ?? 0,
    scopedAmountDisplay: scopedUsage?.amountDisplay ?? null,
    scopedLastTransactionDate:
      scopedUsage?.lastTransactionDate ?? category.lastTransactionDate ?? null,
    usedAccountCount: category.accountUsage.filter(
      (usage) => usage.transactionCount > 0,
    ).length,
    otherAccountCount: category.accountUsage.filter(
      (usage) =>
        usage.accountId !== selectedAccountId && usage.transactionCount > 0,
    ).length,
  } satisfies ScopedCategory;
}

function accountSubtitle(account: TransactionCategoryManagementAccount) {
  const parts = [
    account.institutionName?.trim() || null,
    account.assetDomain ? humanizeToken(account.assetDomain) : null,
    account.entityName?.trim() || null,
  ].filter(Boolean);

  return parts.join(" · ");
}

function categoryMatchesQuery(category: ScopedCategory, query: string) {
  if (!query) {
    return true;
  }

  const haystack = [
    category.displayName,
    category.code,
    category.scopeKind,
    category.directionKind,
  ]
    .join(" ")
    .toLowerCase();

  return haystack.includes(query);
}

function countUsedCategoriesForAccount(
  accountId: string,
  categories: TransactionCategoryManagementCategory[],
) {
  return categories.filter((category) =>
    category.accountUsage.some(
      (usage) => usage.accountId === accountId && usage.transactionCount > 0,
    ),
  ).length;
}

function countUsedCategoriesOverall(
  categories: TransactionCategoryManagementCategory[],
) {
  return categories.filter((category) => category.totalTransactionCount > 0)
    .length;
}

function toneForDirection(directionKind: string) {
  if (directionKind === "outflow") {
    return {
      background: "rgba(255, 75, 43, 0.12)",
      color: "var(--color-accent)",
    };
  }
  if (directionKind === "inflow") {
    return {
      background: "rgba(26, 26, 26, 0.08)",
      color: "var(--color-text-main)",
    };
  }
  return { background: "var(--color-bg)", color: "var(--color-text-muted)" };
}

function toneForScope(scopeKind: string) {
  if (scopeKind === "investment") {
    return { background: "rgba(12, 64, 255, 0.08)", color: "#0c40ff" };
  }
  if (scopeKind === "system") {
    return {
      background: "rgba(26, 26, 26, 0.08)",
      color: "var(--color-text-main)",
    };
  }
  if (scopeKind === "both") {
    return { background: "rgba(255, 184, 0, 0.12)", color: "#8b5a00" };
  }
  return { background: "rgba(33, 150, 83, 0.12)", color: "#1b7a45" };
}

function CoverageBar({
  value,
  tone = "accent",
}: {
  value: number;
  tone?: "accent" | "neutral";
}) {
  return (
    <div
      aria-hidden="true"
      style={{
        width: "100%",
        height: 10,
        borderRadius: 999,
        background: "rgba(0, 0, 0, 0.06)",
        overflow: "hidden",
      }}
    >
      <div
        style={{
          width: `${Math.round(clamp(value, 0, 1) * 100)}%`,
          height: "100%",
          borderRadius: 999,
          background:
            tone === "accent"
              ? "linear-gradient(90deg, #ff7a59 0%, var(--color-accent) 100%)"
              : "linear-gradient(90deg, rgba(26,26,26,0.45) 0%, rgba(26,26,26,0.85) 100%)",
        }}
      />
    </div>
  );
}

function SummaryMetric({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div
      style={{
        padding: 18,
        borderRadius: 20,
        border: "1px solid rgba(0, 0, 0, 0.08)",
        background: "#ffffff",
        display: "grid",
        gap: 8,
      }}
    >
      <span className="label-sm" style={{ marginBottom: 0 }}>
        {label}
      </span>
      <div style={{ fontSize: 28, fontWeight: 700, letterSpacing: "-0.02em" }}>
        {value}
      </div>
      <div className="metric-nominal">{detail}</div>
    </div>
  );
}

function AccountFilterButton({
  label,
  subtitle,
  coverage,
  transactions,
  uncategorized,
  categoryCount,
  active,
  onSelect,
}: {
  label: string;
  subtitle: string;
  coverage: number;
  transactions: number;
  uncategorized: number;
  categoryCount: number;
  active: boolean;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-pressed={active}
      style={{
        width: "100%",
        textAlign: "left",
        padding: 18,
        borderRadius: 22,
        border: active
          ? "1px solid rgba(255, 75, 43, 0.35)"
          : "1px solid rgba(0, 0, 0, 0.08)",
        background: active
          ? "linear-gradient(180deg, rgba(255, 75, 43, 0.08) 0%, #ffffff 100%)"
          : "#ffffff",
        display: "grid",
        gap: 12,
        cursor: "pointer",
      }}
    >
      <div style={{ display: "grid", gap: 4 }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
          }}
        >
          <span style={{ fontSize: 15, fontWeight: 700 }}>{label}</span>
          <span className="pill">{formatPercent(coverage)}</span>
        </div>
        {subtitle ? (
          <span
            className="metric-nominal"
            style={{ fontSize: 13, lineHeight: 1.4 }}
          >
            {subtitle}
          </span>
        ) : null}
      </div>
      <CoverageBar value={coverage} tone={active ? "accent" : "neutral"} />
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
          gap: 10,
        }}
      >
        <div>
          <div className="label-sm" style={{ marginBottom: 4 }}>
            Transactions
          </div>
          <div style={{ fontWeight: 700 }}>{transactions}</div>
        </div>
        <div>
          <div className="label-sm" style={{ marginBottom: 4 }}>
            Categories
          </div>
          <div style={{ fontWeight: 700 }}>{categoryCount}</div>
        </div>
        <div>
          <div className="label-sm" style={{ marginBottom: 4 }}>
            Gaps
          </div>
          <div style={{ fontWeight: 700 }}>{uncategorized}</div>
        </div>
      </div>
    </button>
  );
}

function CategoryCard({
  category,
  totalCategorizedTransactions,
  selectedAccountId,
  accountNames,
  canDelete,
  isPending,
  onDelete,
}: {
  category: ScopedCategory;
  totalCategorizedTransactions: number;
  selectedAccountId: string;
  accountNames: Map<string, string>;
  canDelete: boolean;
  isPending: boolean;
  onDelete: () => void;
}) {
  const usageRatio =
    totalCategorizedTransactions > 0
      ? category.scopedTransactionCount / totalCategorizedTransactions
      : 0;
  const accountBadges = [...category.accountUsage]
    .filter((usage) =>
      selectedAccountId === ALL_ACCOUNTS_ID
        ? usage.transactionCount > 0
        : usage.accountId !== selectedAccountId && usage.transactionCount > 0,
    )
    .sort((left, right) => right.transactionCount - left.transactionCount)
    .slice(0, 4);
  const lastUsedLabel = category.scopedLastTransactionDate
    ? formatDate(category.scopedLastTransactionDate, { lenient: true })
    : "Not used yet";

  return (
    <article
      style={{
        padding: 24,
        borderRadius: 24,
        border:
          category.active === false
            ? "1px dashed rgba(0, 0, 0, 0.14)"
            : "1px solid rgba(0, 0, 0, 0.08)",
        background:
          category.active === false
            ? "linear-gradient(180deg, rgba(0, 0, 0, 0.02) 0%, #ffffff 100%)"
            : "#ffffff",
        display: "grid",
        gap: 18,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          gap: 16,
          flexWrap: "wrap",
        }}
      >
        <div style={{ minWidth: 0, display: "grid", gap: 8 }}>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              flexWrap: "wrap",
            }}
          >
            <span className="pill">{category.code}</span>
            <span className="pill" style={toneForScope(category.scopeKind)}>
              {humanizeToken(category.scopeKind)}
            </span>
            <span
              className="pill"
              style={toneForDirection(category.directionKind)}
            >
              {humanizeToken(category.directionKind)}
            </span>
            {category.active === false ? (
              <span className="pill warning">Inactive</span>
            ) : null}
          </div>
          <div style={{ display: "grid", gap: 4 }}>
            <h3
              style={{
                margin: 0,
                fontSize: 20,
                lineHeight: 1.1,
                letterSpacing: "-0.02em",
              }}
            >
              {category.displayName}
            </h3>
            <p
              className="metric-nominal"
              style={{ fontSize: 14, lineHeight: 1.5 }}
            >
              {category.scopedTransactionCount > 0
                ? `${category.scopedTransactionCount} transactions in the selected scope.`
                : category.otherAccountCount > 0
                  ? `No usage in the selected scope yet. Active in ${category.otherAccountCount} other account${category.otherAccountCount === 1 ? "" : "s"}.`
                  : "No transactions mapped here yet."}
            </p>
          </div>
        </div>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            flexWrap: "wrap",
          }}
        >
          {category.manageHref ? (
            <a className="btn-ghost" href={category.manageHref}>
              Manage
            </a>
          ) : null}
          {canDelete ? (
            <button
              className="btn-ghost"
              type="button"
              onClick={onDelete}
              disabled={isPending}
            >
              Delete
            </button>
          ) : null}
        </div>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: 14,
        }}
      >
        <SummaryMetric
          label="Usage"
          value={String(category.scopedTransactionCount)}
          detail={
            category.scopedAmountDisplay
              ? category.scopedAmountDisplay
              : "Amount placeholder available from parent"
          }
        />
        <SummaryMetric
          label="Share"
          value={formatPercent(usageRatio)}
          detail="Share of categorized transactions in the selected scope"
        />
        <SummaryMetric
          label="Last Activity"
          value={lastUsedLabel}
          detail={
            category.usedAccountCount > 0
              ? `Used across ${category.usedAccountCount} account${category.usedAccountCount === 1 ? "" : "s"}`
              : "Waiting for first assignment"
          }
        />
      </div>

      <div style={{ display: "grid", gap: 8 }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
          }}
        >
          <span className="label-sm" style={{ marginBottom: 0 }}>
            Coverage Weight
          </span>
          <span className="metric-nominal">{formatPercent(usageRatio)}</span>
        </div>
        <CoverageBar value={usageRatio} />
      </div>

      {accountBadges.length > 0 ? (
        <div style={{ display: "grid", gap: 10 }}>
          <span className="label-sm" style={{ marginBottom: 0 }}>
            {selectedAccountId === ALL_ACCOUNTS_ID
              ? "Top Accounts"
              : "Also Used In"}
          </span>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 10,
              flexWrap: "wrap",
            }}
          >
            {accountBadges.map((usage) => (
              <span
                key={`${category.code}-${usage.accountId}`}
                className="pill"
              >
                {accountNames.get(usage.accountId) ?? usage.accountId}:{" "}
                {usage.transactionCount}
              </span>
            ))}
          </div>
        </div>
      ) : null}
    </article>
  );
}

export function TransactionCategoryManagementPanel({
  accounts,
  categories,
  initialAccountId,
  addCategoryLabel = "Add Category",
  catalogHref,
  catalogLabel = "Open Catalog",
  emptyStateCopy = "Add categories or assign transactions to existing ones to start tracking coverage.",
}: TransactionCategoryManagementPanelProps) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [selectedAccountId, setSelectedAccountId] = useState(
    resolveSelectedAccountId(initialAccountId, accounts),
  );
  const [searchQuery, setSearchQuery] = useState("");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [isAddFormOpen, setIsAddFormOpen] = useState(false);
  const deferredSearchQuery = useDeferredValue(
    searchQuery.trim().toLowerCase(),
  );

  const allScopeSnapshot = summarizeAccounts(accounts);
  const selectedAccount =
    selectedAccountId === ALL_ACCOUNTS_ID
      ? null
      : (accounts.find((account) => account.id === selectedAccountId) ?? null);
  const scopeSnapshot = getScopeSnapshot(selectedAccountId, accounts);
  const categorizedCoverage =
    scopeSnapshot.totalTransactions > 0
      ? scopeSnapshot.categorizedTransactions / scopeSnapshot.totalTransactions
      : 0;
  const accountNames = new Map(
    accounts.map((account) => [account.id, account.displayName]),
  );
  const scopedCategories = categories
    .map((category) => getScopedCategory(category, selectedAccountId))
    .filter((category) => categoryMatchesQuery(category, deferredSearchQuery));
  const activeCategories = scopedCategories
    .filter((category) => category.scopedTransactionCount > 0)
    .sort((left, right) => {
      if (right.scopedTransactionCount !== left.scopedTransactionCount) {
        return right.scopedTransactionCount - left.scopedTransactionCount;
      }
      return left.displayName.localeCompare(right.displayName);
    });
  const dormantCategories = scopedCategories
    .filter((category) => category.scopedTransactionCount === 0)
    .sort((left, right) => {
      if (right.otherAccountCount !== left.otherAccountCount) {
        return right.otherAccountCount - left.otherAccountCount;
      }
      if (right.totalTransactionCount !== left.totalTransactionCount) {
        return right.totalTransactionCount - left.totalTransactionCount;
      }
      return left.displayName.localeCompare(right.displayName);
    });

  function handleCreateCategory(
    formData: FormData,
    form: HTMLFormElement,
  ) {
    startTransition(async () => {
      setFeedback(null);
      try {
        await createCategoryAction({
          code: String(formData.get("code") ?? ""),
          displayName: String(formData.get("displayName") ?? ""),
          parentCode: String(formData.get("parentCode") ?? ""),
          scopeKind: String(formData.get("scopeKind") ?? "both") as
            | "personal"
            | "company"
            | "investment"
            | "both"
            | "system",
          directionKind: String(
            formData.get("directionKind") ?? "expense",
          ) as "income" | "expense" | "neutral" | "investment",
        });
        form.reset();
        setIsAddFormOpen(false);
        setFeedback("Category created.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Category creation failed.",
        );
      }
    });
  }

  function handleDeleteCategory(categoryCode: string) {
    if (!window.confirm(`Delete ${categoryCode}? This cannot be undone.`)) {
      return;
    }

    startTransition(async () => {
      setFeedback(null);
      try {
        await deleteCategoryAction(categoryCode);
        setFeedback(`Deleted ${categoryCode}.`);
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Category deletion failed.",
        );
      }
    });
  }

  return (
    <SectionCard
      title="Category Management"
      subtitle="Account-filtered coverage and category inventory"
      span="span-12"
      actions={
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            flexWrap: "wrap",
          }}
        >
          {catalogHref ? (
            <a className="btn-ghost" href={catalogHref}>
              {catalogLabel}
            </a>
          ) : null}
          <button
            className="btn-pill"
            type="button"
            onClick={() => setIsAddFormOpen((current) => !current)}
            disabled={isPending}
          >
            {isAddFormOpen ? "Close Form" : addCategoryLabel}
          </button>
        </div>
      }
    >
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 24,
          alignItems: "start",
        }}
      >
        <aside
          style={{
            flex: "0 0 280px",
            minWidth: 260,
            display: "grid",
            gap: 16,
          }}
        >
          <AccountFilterButton
            label="All Accounts"
            subtitle="Workspace-wide category coverage"
            coverage={
              allScopeSnapshot.totalTransactions > 0
                ? allScopeSnapshot.categorizedTransactions /
                  allScopeSnapshot.totalTransactions
                : 0
            }
            transactions={allScopeSnapshot.totalTransactions}
            uncategorized={allScopeSnapshot.uncategorizedTransactions}
            categoryCount={countUsedCategoriesOverall(categories)}
            active={selectedAccountId === ALL_ACCOUNTS_ID}
            onSelect={() => setSelectedAccountId(ALL_ACCOUNTS_ID)}
          />
          {accounts.map((account) => {
            const coverage =
              account.totalTransactions > 0
                ? account.categorizedTransactions / account.totalTransactions
                : 0;

            return (
              <AccountFilterButton
                key={account.id}
                label={account.displayName}
                subtitle={accountSubtitle(account)}
                coverage={coverage}
                transactions={account.totalTransactions}
                uncategorized={account.uncategorizedTransactions}
                categoryCount={countUsedCategoriesForAccount(
                  account.id,
                  categories,
                )}
                active={selectedAccountId === account.id}
                onSelect={() => setSelectedAccountId(account.id)}
              />
            );
          })}
        </aside>

        <div
          style={{
            flex: "1 1 720px",
            minWidth: 0,
            display: "grid",
            gap: 24,
          }}
        >
          {feedback ? (
            <div className="metric-nominal">{feedback}</div>
          ) : null}

          {isAddFormOpen ? (
            <form
              onSubmit={(event) => {
                event.preventDefault();
                handleCreateCategory(
                  new FormData(event.currentTarget),
                  event.currentTarget,
                );
              }}
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
                gap: 14,
                padding: 20,
                borderRadius: 24,
                border: "1px solid rgba(255, 75, 43, 0.16)",
                background:
                  "linear-gradient(180deg, rgba(255, 75, 43, 0.06) 0%, #ffffff 100%)",
              }}
            >
              <label className="input-label">
                Code
                <input
                  className="input-field"
                  name="code"
                  placeholder="legal_fees"
                  required
                />
              </label>
              <label className="input-label">
                Display Name
                <input
                  className="input-field"
                  name="displayName"
                  placeholder="Legal Fees"
                  required
                />
              </label>
              <label className="input-label">
                Parent Code
                <input
                  className="input-field"
                  name="parentCode"
                  placeholder="Optional parent"
                />
              </label>
              <label className="input-label">
                Scope
                <select className="input-field" name="scopeKind" defaultValue="both">
                  <option value="personal">Personal</option>
                  <option value="company">Company</option>
                  <option value="investment">Investment</option>
                  <option value="both">Both</option>
                  <option value="system">System</option>
                </select>
              </label>
              <label className="input-label">
                Direction
                <select
                  className="input-field"
                  name="directionKind"
                  defaultValue="expense"
                >
                  <option value="income">Income</option>
                  <option value="expense">Expense</option>
                  <option value="neutral">Neutral</option>
                  <option value="investment">Investment</option>
                </select>
              </label>
              <div
                className="inline-actions"
                style={{ alignSelf: "end", justifyContent: "flex-end" }}
              >
                <button className="btn-pill" type="submit" disabled={isPending}>
                  Create
                </button>
              </div>
            </form>
          ) : null}

          <section
            style={{
              padding: 28,
              borderRadius: 28,
              background:
                "linear-gradient(135deg, rgba(255, 75, 43, 0.1) 0%, rgba(255, 255, 255, 1) 55%)",
              border: "1px solid rgba(255, 75, 43, 0.12)",
              display: "grid",
              gap: 20,
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "flex-start",
                justifyContent: "space-between",
                gap: 20,
                flexWrap: "wrap",
              }}
            >
              <div style={{ display: "grid", gap: 10 }}>
                <span className="label-sm" style={{ marginBottom: 0 }}>
                  Focus Scope
                </span>
                <div
                  style={{
                    margin: 0,
                    fontSize: 34,
                    lineHeight: 1,
                    fontWeight: 700,
                    letterSpacing: "-0.03em",
                  }}
                >
                  {selectedAccount?.displayName ?? "All Accounts"}
                </div>
                <p
                  className="metric-nominal"
                  style={{ fontSize: 15, lineHeight: 1.6, maxWidth: 720 }}
                >
                  {scopeSnapshot.uncategorizedTransactions > 0
                    ? `${scopeSnapshot.uncategorizedTransactions} transactions in this scope still have no category. This panel surfaces where coverage is strong and which category definitions are sitting idle.`
                    : "Category coverage is complete for the current scope. Use this panel to spot concentration, dormant definitions, and cleanup candidates."}
                </p>
              </div>
              <div style={{ minWidth: 220, display: "grid", gap: 8 }}>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    gap: 12,
                  }}
                >
                  <span className="label-sm" style={{ marginBottom: 0 }}>
                    Coverage
                  </span>
                  <span style={{ fontSize: 28, fontWeight: 700 }}>
                    {formatPercent(categorizedCoverage)}
                  </span>
                </div>
                <CoverageBar value={categorizedCoverage} />
              </div>
            </div>

            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
                gap: 14,
              }}
            >
              <SummaryMetric
                label="Categorized"
                value={String(scopeSnapshot.categorizedTransactions)}
                detail="Transactions already mapped to a category"
              />
              <SummaryMetric
                label="Coverage Gaps"
                value={String(scopeSnapshot.uncategorizedTransactions)}
                detail="Transactions still missing a category"
              />
              <SummaryMetric
                label="Active Categories"
                value={String(activeCategories.length)}
                detail="Definitions carrying traffic in the selected scope"
              />
              <SummaryMetric
                label="Dormant Categories"
                value={String(dormantCategories.length)}
                detail="Available definitions with zero usage in the selected scope"
              />
            </div>
          </section>

          <section
            style={{
              display: "flex",
              alignItems: "flex-end",
              justifyContent: "space-between",
              gap: 16,
              flexWrap: "wrap",
            }}
          >
            <div>
              <span className="label-sm">Category Search</span>
              <h3 style={{ margin: 0, fontSize: 22, letterSpacing: "-0.02em" }}>
                Scan the catalog by account, coverage, and usage
              </h3>
            </div>
            <label
              className="input-label"
              style={{ minWidth: 280, flex: "1 1 320px" }}
            >
              Search categories
              <input
                className="input-field"
                type="search"
                placeholder="Search by code, name, scope, or direction"
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
              />
            </label>
          </section>

          {categories.length === 0 ? (
            <div className="table-empty-state">{emptyStateCopy}</div>
          ) : scopedCategories.length === 0 ? (
            <div className="table-empty-state">
              No categories matched this account filter and search query.
            </div>
          ) : (
            <>
              <section style={{ display: "grid", gap: 16 }}>
                <div
                  style={{
                    display: "flex",
                    alignItems: "baseline",
                    justifyContent: "space-between",
                    gap: 16,
                    flexWrap: "wrap",
                  }}
                >
                  <div>
                    <span className="label-sm">In Use</span>
                    <h3
                      style={{
                        margin: 0,
                        fontSize: 22,
                        letterSpacing: "-0.02em",
                      }}
                    >
                      Categories carrying traffic now
                    </h3>
                  </div>
                  <span className="metric-nominal">
                    {activeCategories.length} visible in the current filter
                  </span>
                </div>
                {activeCategories.length === 0 ? (
                  <div className="table-empty-state">
                    No categories match this account filter and search query.
                  </div>
                ) : (
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns:
                        "repeat(auto-fit, minmax(320px, 1fr))",
                      gap: 18,
                    }}
                  >
                    {activeCategories.map((category) => (
                      <CategoryCard
                        key={category.code}
                        category={category}
                        totalCategorizedTransactions={
                          scopeSnapshot.categorizedTransactions
                        }
                        selectedAccountId={selectedAccountId}
                        accountNames={accountNames}
                        canDelete={category.totalTransactionCount === 0}
                        isPending={isPending}
                        onDelete={() => handleDeleteCategory(category.code)}
                      />
                    ))}
                  </div>
                )}
              </section>

              <section style={{ display: "grid", gap: 16 }}>
                <div
                  style={{
                    display: "flex",
                    alignItems: "baseline",
                    justifyContent: "space-between",
                    gap: 16,
                    flexWrap: "wrap",
                  }}
                >
                  <div>
                    <span className="label-sm">Available But Unused</span>
                    <h3
                      style={{
                        margin: 0,
                        fontSize: 22,
                        letterSpacing: "-0.02em",
                      }}
                    >
                      Dormant definitions and cleanup candidates
                    </h3>
                  </div>
                  <span className="metric-nominal">
                    {dormantCategories.length} visible in the current filter
                  </span>
                </div>
                {dormantCategories.length === 0 ? (
                  <div className="table-empty-state">
                    Every visible category is already active in this scope.
                  </div>
                ) : (
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns:
                        "repeat(auto-fit, minmax(320px, 1fr))",
                      gap: 18,
                    }}
                  >
                    {dormantCategories.map((category) => (
                      <CategoryCard
                        key={category.code}
                        category={category}
                        totalCategorizedTransactions={
                          scopeSnapshot.categorizedTransactions
                        }
                        selectedAccountId={selectedAccountId}
                        accountNames={accountNames}
                        canDelete={category.totalTransactionCount === 0}
                        isPending={isPending}
                        onDelete={() => handleDeleteCategory(category.code)}
                      />
                    ))}
                  </div>
                )}
              </section>
            </>
          )}
        </div>
      </div>
    </SectionCard>
  );
}
