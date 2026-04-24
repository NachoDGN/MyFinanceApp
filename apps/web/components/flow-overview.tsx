import { type ReactNode } from "react";

import { formatMonthLabel } from "../lib/dashboard";

export type FlowTrendRow = {
  month: string;
  segments: Array<{ className: string; height: number }>;
};

export function FlowPageHeader({
  watermark,
  watermarkClassName = "",
  title,
  subtitle,
  notice,
}: {
  watermark: string;
  watermarkClassName?: string;
  title: string;
  subtitle: ReactNode;
  notice?: ReactNode;
}) {
  return (
    <>
      <div className={`income-editorial-watermark ${watermarkClassName}`}>
        {watermark}
      </div>
      <div className="income-page-header span-12">
        <div>
          <h1 className="page-title">{title}</h1>
          <p className="page-subtitle">{subtitle}</p>
          {notice}
        </div>
      </div>
    </>
  );
}

export function FlowKpiGrid({
  items,
}: {
  items: Array<{
    title: ReactNode;
    value: ReactNode;
    badge: ReactNode;
    accent?: boolean;
    icon?: ReactNode;
    badgeTone?: "accent" | "neutral";
  }>;
}) {
  return (
    <div className="income-kpi-grid span-12">
      {items.map((item, index) => (
        <article
          className={`income-kpi-card ${item.accent ? "income-kpi-card-accent" : ""}`}
          key={index}
        >
          <div className="income-kpi-title">
            <span>{item.title}</span>
            {item.icon}
          </div>
          <div className="income-kpi-value">{item.value}</div>
          <div className={`income-kpi-badge ${item.badgeTone ?? "neutral"}`}>
            {item.badge}
          </div>
        </article>
      ))}
    </div>
  );
}

export function FlowTrendChart({
  title,
  rangeLabel,
  fallbackFxRangeLabel,
  currency,
  referenceDate,
  axisLabels,
  rows,
}: {
  title: string;
  rangeLabel: string;
  fallbackFxRangeLabel: string | null;
  currency: string;
  referenceDate: string;
  axisLabels: string[];
  rows: FlowTrendRow[];
}) {
  return (
    <section className="income-chart-card span-12">
      <div className="income-chart-header">
        <div>
          <h2 className="income-chart-title">{title}</h2>
        </div>
        <div className="income-kpi-badge neutral">{rangeLabel}</div>
      </div>
      {fallbackFxRangeLabel ? (
        <div className="status-note" style={{ marginTop: 16 }}>
          Historical {currency} conversion was unavailable for{" "}
          {fallbackFxRangeLabel}, so the latest available FX up to{" "}
          {referenceDate} is used to keep the full trend visible.
        </div>
      ) : null}

      <div className="income-chart-body">
        <div className="income-y-axis">
          {axisLabels.map((label) => (
            <span key={label}>{label}</span>
          ))}
        </div>
        <div className="income-grid-lines" aria-hidden="true">
          {axisLabels.map((label) => (
            <div className="income-grid-line" key={label} />
          ))}
        </div>
        <div className="income-chart-bars">
          {rows.map((row, index) => (
            <div className="income-bar-group" key={row.month}>
              {row.segments.map((segment) => (
                <div
                  className={`income-bar-segment ${segment.className}`}
                  key={segment.className}
                  style={{ height: `${Math.max(0, segment.height)}%` }}
                />
              ))}
              <div
                className={`income-bar-label ${
                  index === rows.length - 1 ? "active" : ""
                }`}
              >
                {formatMonthLabel(row.month)}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

export function FlowBreakdownCard({
  title,
  periodLabel,
  description,
  headers,
  children,
}: {
  title: string;
  periodLabel: string;
  description: string;
  headers: [string, string, string, string];
  children: ReactNode;
}) {
  return (
    <article className="income-breakdown-card">
      <div className="income-chart-header">
        <h2 className="income-chart-title">{title}</h2>
        <div className="income-kpi-badge neutral">{periodLabel}</div>
      </div>
      <div className="muted" style={{ marginTop: 8, lineHeight: 1.5 }}>
        {description}
      </div>

      <div className="income-breakdown-table">
        <div className="income-breakdown-head">
          <div>{headers[0]}</div>
          <div>{headers[1]}</div>
          <div className="amount">{headers[2]}</div>
          <div className="amount">{headers[3]}</div>
        </div>
        {children}
      </div>
    </article>
  );
}

export function FlowSummaryCard({ children }: { children: ReactNode }) {
  return (
    <article className="income-summary-card">
      <div className="income-chart-header">
        <h2 className="income-chart-title">Period Summary</h2>
      </div>
      {children}
    </article>
  );
}
