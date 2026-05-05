import type { NavigationState } from "../lib/navigation";

import { formatPercentLabel } from "../lib/dashboard";
import { buildHref } from "../lib/navigation";

type FlowCategoryBreakdownRow = {
  categoryCode: string;
  label: string;
  amountEur: string;
};

export function FlowCategoryBreakdownList({
  basePath,
  rows,
  visibleRows,
  totalAmount,
  navigationState,
  emptyLabel,
  page,
  pageCount,
  rangeLabel,
  hasMultiplePages,
  formatAmount,
}: {
  basePath: "/income" | "/spending";
  rows: readonly FlowCategoryBreakdownRow[];
  visibleRows: readonly FlowCategoryBreakdownRow[];
  totalAmount: number;
  navigationState: NavigationState;
  emptyLabel: string;
  page: number;
  pageCount: number;
  rangeLabel: string;
  hasMultiplePages: boolean;
  formatAmount: (amountEur: string) => string;
}) {
  return (
    <>
      {rows.length === 0 ? (
        <div className="table-empty-state">{emptyLabel}</div>
      ) : (
        visibleRows.map((row) => {
          const share =
            totalAmount > 0 ? (Number(row.amountEur) / totalAmount) * 100 : 0;
          return (
            <a
              className="income-breakdown-row spending-category-link-row"
              href={buildHref(
                `${basePath}/${encodeURIComponent(row.categoryCode)}`,
                navigationState,
                {},
              )}
              key={row.categoryCode}
            >
              <div className="source-name">{row.label}</div>
              <div className="source-progress-track">
                <div
                  className="source-progress-fill"
                  style={{ width: `${Math.max(share, 0)}%` }}
                />
              </div>
              <div className="amount">{formatAmount(row.amountEur)}</div>
              <div className="amount">{formatPercentLabel(share)}</div>
            </a>
          );
        })
      )}
      {hasMultiplePages ? (
        <div className="spending-category-pagination">
          <span>{rangeLabel}</span>
          <div>
            <a
              className={
                page <= 1 ? "spending-page-link disabled" : "spending-page-link"
              }
              aria-disabled={page <= 1}
              href={
                page <= 1
                  ? undefined
                  : buildHref(basePath, navigationState, {}, {
                      categoryPage: String(page - 1),
                    })
              }
            >
              Previous
            </a>
            <a
              className={
                page >= pageCount
                  ? "spending-page-link disabled"
                  : "spending-page-link"
              }
              aria-disabled={page >= pageCount}
              href={
                page >= pageCount
                  ? undefined
                  : buildHref(basePath, navigationState, {}, {
                      categoryPage: String(page + 1),
                    })
              }
            >
              Next
            </a>
          </div>
        </div>
      ) : null}
    </>
  );
}
