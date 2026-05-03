import { type ReactNode } from "react";

import { formatMonthLabel } from "../lib/dashboard";

export type FlowTrendRow = {
  month: string;
  segments: Array<{
    className?: string;
    color?: string;
    height: number;
    label?: string;
    valueLabel?: string;
    shareLabel?: string;
  }>;
};

type FlowTrendLegendItem = {
  label: string;
  color: string;
  valueLabel?: string;
  shareLabel?: string;
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
  description,
  rangeLabel,
  fallbackFxRangeLabel,
  currency,
  referenceDate,
  axisLabels,
  rows,
  legendItems = [],
  summary,
  emptyLabel = "No spending data is available for this period.",
}: {
  title: string;
  description?: ReactNode;
  rangeLabel: string;
  fallbackFxRangeLabel: string | null;
  currency: string;
  referenceDate: string;
  axisLabels: string[];
  rows: FlowTrendRow[];
  legendItems?: FlowTrendLegendItem[];
  summary?: {
    title: string;
    value: string;
    badge?: ReactNode;
  };
  emptyLabel?: string;
}) {
  const visibleRows = rows.map((row) => ({
    ...row,
    segments: row.segments.filter((segment) => segment.height > 0),
  }));
  const hasData = visibleRows.some((row) => row.segments.length > 0);

  return (
    <section className="income-chart-card span-12">
      <div className="income-chart-header">
        <div>
          <h2 className="income-chart-title">{title}</h2>
          {description ? (
            <p className="income-chart-description">{description}</p>
          ) : null}
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

      <div className="income-chart-layout">
        <div className="income-chart-plot">
          {hasData ? (
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
              <div
                className="income-chart-bars"
                style={{
                  gridTemplateColumns: `repeat(${Math.max(visibleRows.length, 1)}, minmax(32px, 1fr))`,
                }}
              >
                {visibleRows.map((row, index) => (
                  <div className="income-bar-group" key={row.month}>
                    {row.segments.map((segment, segmentIndex) => {
                      const tooltipParts = [
                        segment.label,
                        segment.valueLabel,
                        segment.shareLabel,
                      ].filter(Boolean);

                      return (
                        <div
                          aria-label={tooltipParts.join(", ")}
                          className={`income-bar-segment ${segment.className ?? ""}`}
                          key={`${segment.label ?? segment.className ?? "segment"}-${segmentIndex}`}
                          role="img"
                          style={{
                            height: `${Math.max(0, segment.height)}%`,
                            background: segment.color,
                          }}
                          tabIndex={0}
                        >
                          {tooltipParts.length > 0 ? (
                            <span className="income-bar-tooltip">
                              {segment.label ? (
                                <strong>{segment.label}</strong>
                              ) : null}
                              {segment.valueLabel ? (
                                <span>{segment.valueLabel}</span>
                              ) : null}
                              {segment.shareLabel ? (
                                <small>{segment.shareLabel}</small>
                              ) : null}
                            </span>
                          ) : null}
                        </div>
                      );
                    })}
                    <div
                      className={`income-bar-label ${
                        index === visibleRows.length - 1 ? "active" : ""
                      }`}
                    >
                      {formatMonthLabel(row.month)}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="income-chart-empty-state">
              <span>{emptyLabel}</span>
            </div>
          )}
        </div>

        {(summary || legendItems.length > 0) && hasData ? (
          <aside className="income-chart-side">
            {summary ? (
              <div className="income-chart-total">
                <span>{summary.title}</span>
                <strong>{summary.value}</strong>
                {summary.badge ? (
                  <div className="income-kpi-badge accent">{summary.badge}</div>
                ) : null}
              </div>
            ) : null}
            {legendItems.length > 0 ? (
              <div className="income-chart-side-list">
                <h3>Breakdown</h3>
                {legendItems.map((item, index) => (
                  <div
                    className="income-chart-side-row"
                    key={`${item.label}-${index}`}
                  >
                    <span
                      className="income-chart-legend-swatch"
                      style={{ background: item.color }}
                      aria-hidden="true"
                    />
                    <span>{item.label}</span>
                    {item.shareLabel ? <small>{item.shareLabel}</small> : null}
                    {item.valueLabel ? (
                      <strong>{item.valueLabel}</strong>
                    ) : null}
                  </div>
                ))}
              </div>
            ) : null}
          </aside>
        ) : null}
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
