import type { ReactNode } from "react";

import { SectionCard } from "./primitives";

type CardSpan = "span-12" | "span-8" | "span-6" | "span-4" | "span-3";

export type UnresolvedTransactionsReviewPanelAction = {
  href: string;
  label: string;
  variant?: "primary" | "secondary";
};

export type UnresolvedTransactionsReviewPanelPill = {
  label: ReactNode;
  tone?: "default" | "warning";
};

export type UnresolvedTransactionsReviewPanelRow = {
  id: string;
  href: string;
  date: ReactNode;
  account: ReactNode;
  description: ReactNode;
  amount: ReactNode;
  review: ReactNode;
  ctaLabel?: string;
  secondaryHref?: string;
  secondaryLabel?: string;
};

export function UnresolvedTransactionsReviewPanel({
  title = "Unresolved Transactions",
  subtitle = "Manual review queue",
  span = "span-12",
  rows,
  summaryPills = [],
  helperText,
  footerNote,
  emptyMessage = "No unresolved transactions are waiting for review in the current scope.",
  actions = [],
}: {
  title?: string;
  subtitle?: string;
  span?: CardSpan;
  rows: UnresolvedTransactionsReviewPanelRow[];
  summaryPills?: UnresolvedTransactionsReviewPanelPill[];
  helperText?: ReactNode;
  footerNote?: ReactNode;
  emptyMessage?: ReactNode;
  actions?: UnresolvedTransactionsReviewPanelAction[];
}) {
  const resolvedFooterNote =
    footerNote ??
    (rows.length > 0
      ? `Showing ${rows.length} unresolved transaction${rows.length === 1 ? "" : "s"} for quick review.`
      : null);

  return (
    <SectionCard
      title={title}
      subtitle={subtitle}
      span={span}
      actions={
        actions.length > 0 ? (
          <div
            className="inline-actions"
            style={{ justifyContent: "flex-end" }}
          >
            {actions.map((action) => (
              <a
                key={`${action.label}-${action.href}`}
                className={
                  action.variant === "primary" ? "btn-pill" : "btn-ghost"
                }
                href={action.href}
              >
                {action.label}
              </a>
            ))}
          </div>
        ) : undefined
      }
    >
      <div style={{ display: "grid", gap: 20 }}>
        {summaryPills.length > 0 || helperText ? (
          <div style={{ display: "grid", gap: 12 }}>
            {summaryPills.length > 0 ? (
              <div className="legend-list">
                {summaryPills.map((pill, index) => (
                  <span
                    className={
                      pill.tone === "warning" ? "pill warning" : "pill"
                    }
                    key={`summary-pill-${index}`}
                  >
                    {pill.label}
                  </span>
                ))}
              </div>
            ) : null}
            {helperText ? (
              <p className="muted" style={{ margin: 0, lineHeight: 1.5 }}>
                {helperText}
              </p>
            ) : null}
          </div>
        ) : null}

        {rows.length === 0 ? (
          <div className="table-empty-state">{emptyMessage}</div>
        ) : (
          <>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Account</th>
                    <th>Description</th>
                    <th>Amount</th>
                    <th>Review</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => (
                    <tr key={row.id}>
                      <td
                        style={{ verticalAlign: "top", whiteSpace: "nowrap" }}
                      >
                        {row.date}
                      </td>
                      <td style={{ verticalAlign: "top", minWidth: 140 }}>
                        {row.account}
                      </td>
                      <td style={{ verticalAlign: "top", minWidth: 260 }}>
                        {row.description}
                      </td>
                      <td
                        style={{ verticalAlign: "top", whiteSpace: "nowrap" }}
                      >
                        <div style={{ textAlign: "right" }}>{row.amount}</div>
                      </td>
                      <td style={{ verticalAlign: "top", minWidth: 240 }}>
                        {row.review}
                      </td>
                      <td
                        style={{ verticalAlign: "top", whiteSpace: "nowrap" }}
                      >
                        <div
                          className="inline-actions"
                          style={{ justifyContent: "flex-end" }}
                        >
                          <a className="btn-ghost" href={row.href}>
                            {row.ctaLabel ?? "Review"}
                          </a>
                          {row.secondaryHref && row.secondaryLabel ? (
                            <a className="btn-ghost" href={row.secondaryHref}>
                              {row.secondaryLabel}
                            </a>
                          ) : null}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {resolvedFooterNote ? (
              <p
                className="muted"
                style={{ margin: 0, fontSize: 13, lineHeight: 1.5 }}
              >
                {resolvedFooterNote}
              </p>
            ) : null}
          </>
        )}
      </div>
    </SectionCard>
  );
}
