import type { ReactNode } from "react";

import { formatDate } from "../lib/formatters";

export function TimelinePanel({
  title,
  actions,
  transactions,
  currency,
  viewAllHref,
}: {
  title: string;
  actions?: ReactNode;
  transactions: Array<{
    id: string;
    transactionDate: string;
    descriptionRaw: string;
    amountDisplay: string;
    positive?: boolean;
  }>;
  currency: string;
  viewAllHref?: string;
}) {
  return (
    <div className="details-section">
      <div className="section-header">
        <h2 className="section-title">{title}</h2>
        {actions}
      </div>
      <div className="timeline-list">
        {transactions.map((transaction) => (
          <div className="timeline-item" key={transaction.id}>
            <span className="timeline-date">
              {formatDate(transaction.transactionDate, { lenient: true })}
            </span>
            <div className="timeline-content">
              <div
                className="timeline-dot"
                style={
                  transaction.positive
                    ? {
                        background: "#1A1A1A",
                        boxShadow: "0 0 0 1px #1A1A1A",
                      }
                    : undefined
                }
              />
              <span className="timeline-label">
                {transaction.descriptionRaw}
              </span>
            </div>
            <span
              className="timeline-amount"
              style={
                transaction.positive
                  ? { color: "var(--color-accent)" }
                  : undefined
              }
            >
              {transaction.amountDisplay}
            </span>
          </div>
        ))}
      </div>
      <div className="view-all">
        <span className="timeline-date">
          Showing last {transactions.length} transactions
        </span>
        <a
          href={viewAllHref ?? `/transactions?currency=${currency}`}
          className="view-all-link"
        >
          View Ledger →
        </a>
      </div>
    </div>
  );
}

export function SectionCard({
  title,
  subtitle,
  span = "span-6",
  children,
  actions,
}: {
  title: string;
  subtitle?: string;
  span?: "span-12" | "span-8" | "span-6" | "span-4" | "span-3";
  children: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <section className={`section-card ${span}`}>
      <div className="section-header">
        <div>
          <span className="label-sm">{subtitle ?? "Section"}</span>
          <h2 className="section-title">{title}</h2>
        </div>
        {actions}
      </div>
      <div className="section-card-body">{children}</div>
    </section>
  );
}

export function MultiSeriesChart({
  rows,
}: {
  rows: Array<{
    month: string;
    incomeEur: string;
    spendingEur: string;
    operatingNetEur: string;
  }>;
}) {
  const max = Math.max(
    ...rows.flatMap((row) => [
      Number(row.incomeEur),
      Number(row.spendingEur),
      Math.abs(Number(row.operatingNetEur)),
    ]),
    1,
  );
  return (
    <div className="stack-bars">
      {rows.map((row) => (
        <div className="stack-month" key={row.month}>
          <div
            className="stack-piece income"
            style={{
              height: `${Math.max(8, (Number(row.incomeEur) / max) * 180)}px`,
            }}
          />
          <div
            className="stack-piece spending"
            style={{
              height: `${Math.max(8, (Number(row.spendingEur) / max) * 180)}px`,
            }}
          />
          <div
            className="stack-piece net"
            style={{
              height: `${Math.max(8, (Math.abs(Number(row.operatingNetEur)) / max) * 180)}px`,
            }}
          />
          <div className="stack-label">
            {new Intl.DateTimeFormat("en-US", { month: "short" }).format(
              new Date(`${row.month}T00:00:00Z`),
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

export function SimpleTable({
  headers,
  rows,
  span = "span-12",
}: {
  headers: string[];
  rows: ReactNode[][];
  span?: "span-12" | "span-8" | "span-6" | "span-4" | "span-3";
}) {
  return (
    <div className={`table-card ${span}`}>
      <div className="table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              {headers.map((header) => (
                <th key={header}>{header}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={`row-${rowIndex}`}>
                {row.map((cell, cellIndex) => (
                  <td key={`cell-${rowIndex}-${cellIndex}`}>{cell}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
