import {
  getTransactionReviewReason,
  getTransactionReviewState,
} from "@myfinance/domain/client";

import { formatCurrency } from "../lib/formatters";

function formatReviewReason(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    if (
      (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
    ) {
      try {
        return formatReviewReason(JSON.parse(trimmed)) ?? trimmed;
      } catch {
        return trimmed;
      }
    }
    return trimmed;
  }
  if (Array.isArray(value)) {
    const issueMessages = value
      .map((entry) => {
        if (!entry || typeof entry !== "object") {
          return null;
        }
        const path = Array.isArray((entry as { path?: unknown }).path)
          ? (entry as { path: unknown[] }).path
              .filter((segment) => typeof segment === "string")
              .join(".") || null
          : null;
        const message =
          typeof (entry as { message?: unknown }).message === "string"
            ? (entry as { message: string }).message
            : null;
        if (!path && !message) {
          return null;
        }
        return path ? `${path}: ${message ?? "Invalid value"}` : message;
      })
      .filter((entry): entry is string => Boolean(entry));
    if (issueMessages.length > 0) {
      return `LLM output validation failed: ${issueMessages.join("; ")}`;
    }
    return JSON.stringify(value);
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function ReviewStateCell({
  needsReview,
  categoryCode,
  reviewReason,
  transactionClass,
  classificationSource,
  securitySymbol,
  quantity,
  llmPayload,
  creditCardStatementStatus = "not_applicable",
  descriptionRaw = "",
  descriptionClean = "",
  variant = "default",
}: {
  needsReview: boolean;
  categoryCode?: string | null;
  reviewReason?: unknown;
  transactionClass?: string | null;
  classificationSource?: string | null;
  securitySymbol?: string | null;
  quantity?: string | null;
  llmPayload?: unknown;
  creditCardStatementStatus?: "not_applicable" | "upload_required" | "uploaded";
  descriptionRaw?: string;
  descriptionClean?: string;
  variant?: "default" | "statement";
}) {
  const reviewState = getTransactionReviewState({
    needsReview,
    categoryCode,
    llmPayload,
    creditCardStatementStatus,
    descriptionRaw,
    descriptionClean,
  });
  const normalizedReviewReason = formatReviewReason(
    getTransactionReviewReason({
      reviewReason: typeof reviewReason === "string" ? reviewReason : null,
      categoryCode,
      creditCardStatementStatus,
      descriptionRaw,
      descriptionClean,
    }) ?? reviewReason,
  );
  const normalizedPendingReason = normalizedReviewReason?.replace(
    "Pending enrichment pipeline.",
    "Queued for automatic transaction analysis.",
  );
  const normalizedQuantity =
    quantity && Number(quantity) !== 0
      ? Number.isInteger(Number(quantity))
        ? String(Number(quantity))
        : Number(quantity)
            .toFixed(4)
            .replace(/\.?0+$/, "")
      : null;
  const sourceLabel =
    classificationSource === "investment_parser"
      ? "investment parser"
      : classificationSource === "user_rule"
        ? "rule"
        : classificationSource === "llm"
          ? "LLM"
          : classificationSource === "transfer_matcher"
            ? "transfer matcher"
            : classificationSource === "manual_override"
              ? "manual review"
              : null;
  const explanation =
    llmPayload &&
    typeof llmPayload === "object" &&
    "explanation" in llmPayload &&
    typeof (llmPayload as { explanation?: unknown }).explanation === "string"
      ? (llmPayload as { explanation: string }).explanation
      : null;
  const llmReason =
    llmPayload &&
    typeof llmPayload === "object" &&
    "reason" in llmPayload &&
    typeof (llmPayload as { reason?: unknown }).reason === "string"
      ? (llmPayload as { reason: string }).reason
      : null;
  const llmModel =
    llmPayload &&
    typeof llmPayload === "object" &&
    "model" in llmPayload &&
    typeof (llmPayload as { model?: unknown }).model === "string"
      ? (llmPayload as { model: string }).model
      : null;
  const llmCompletedAt =
    llmPayload &&
    typeof llmPayload === "object" &&
    "timing" in llmPayload &&
    typeof (llmPayload as { timing?: unknown }).timing === "object" &&
    (llmPayload as { timing: { completedAt?: unknown } }).timing &&
    typeof (llmPayload as { timing: { completedAt?: unknown } }).timing
      .completedAt === "string"
      ? ((llmPayload as { timing: { completedAt: string } }).timing
          .completedAt ?? null)
      : null;
  const llmDurationMs =
    llmPayload &&
    typeof llmPayload === "object" &&
    "timing" in llmPayload &&
    typeof (llmPayload as { timing?: unknown }).timing === "object" &&
    (llmPayload as { timing: { durationMs?: unknown } }).timing &&
    typeof (llmPayload as { timing: { durationMs?: unknown } }).timing
      .durationMs === "number"
      ? ((llmPayload as { timing: { durationMs: number } }).timing.durationMs ??
        null)
      : null;
  const llmTrigger =
    llmPayload &&
    typeof llmPayload === "object" &&
    "reviewContext" in llmPayload &&
    typeof (llmPayload as { reviewContext?: unknown }).reviewContext ===
      "object" &&
    (llmPayload as { reviewContext: { trigger?: unknown } }).reviewContext &&
    typeof (llmPayload as { reviewContext: { trigger?: unknown } })
      .reviewContext.trigger === "string"
      ? ((llmPayload as { reviewContext: { trigger: string } }).reviewContext
          .trigger ?? null)
      : null;
  const llmLogSummary =
    llmModel || llmCompletedAt || llmDurationMs !== null
      ? [
          llmTrigger === "manual_review_update"
            ? "manual re-review"
            : llmTrigger === "manual_resolved_review"
              ? "resolved re-review"
              : llmTrigger === "import_classification"
                ? "import enrichment"
                : null,
          llmModel,
          llmCompletedAt
            ? new Intl.DateTimeFormat("en-US", {
                month: "short",
                day: "numeric",
                hour: "2-digit",
                minute: "2-digit",
              }).format(new Date(llmCompletedAt))
            : null,
          typeof llmDurationMs === "number"
            ? `${Math.round(llmDurationMs)}ms`
            : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : null;
  const resolvedSummary =
    sourceLabel || transactionClass || securitySymbol || normalizedQuantity
      ? [
          sourceLabel ? `Resolved by ${sourceLabel}` : null,
          transactionClass ? transactionClass.replace(/_/g, " ") : null,
          securitySymbol ? `security ${securitySymbol}` : null,
          normalizedQuantity ? `qty ${normalizedQuantity}` : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : null;
  const defaultContainerStyle =
    variant === "statement"
      ? undefined
      : ({ display: "grid", gap: 6, minWidth: 220 } as const);
  const helperClassName =
    variant === "statement" ? "statement-helper-text" : "muted";
  const helperStyle =
    variant === "statement"
      ? undefined
      : ({ fontSize: 12, lineHeight: 1.4 } as const);

  if (reviewState === "resolved") {
    return (
      <div
        className={
          variant === "statement" ? "statement-review-state" : undefined
        }
        style={defaultContainerStyle}
      >
        <span
          className={
            variant === "statement"
              ? "statement-alert statement-alert-success"
              : "pill"
          }
        >
          Resolved
        </span>
        {resolvedSummary ? (
          <span className={helperClassName} style={helperStyle}>
            {resolvedSummary}
          </span>
        ) : null}
        {explanation && explanation !== resolvedSummary ? (
          <span className={helperClassName} style={helperStyle}>
            {explanation}
          </span>
        ) : null}
        {llmLogSummary ? (
          <span className={helperClassName} style={helperStyle}>
            {llmLogSummary}
          </span>
        ) : null}
      </div>
    );
  }

  if (reviewState === "pending_enrichment") {
    return (
      <div
        className={
          variant === "statement" ? "statement-review-state" : undefined
        }
        style={defaultContainerStyle}
      >
        <span
          className={
            variant === "statement"
              ? "statement-alert statement-alert-neutral"
              : "pill"
          }
        >
          Analyzing
        </span>
        <span
          className={helperClassName}
          style={helperStyle}
          title={
            normalizedPendingReason ??
            "Queued for automatic transaction analysis."
          }
        >
          {normalizedPendingReason ??
            "Queued for automatic transaction analysis."}
        </span>
        {llmLogSummary ? (
          <span className={helperClassName} style={helperStyle}>
            {llmLogSummary}
          </span>
        ) : null}
      </div>
    );
  }

  return (
    <div
      className={variant === "statement" ? "statement-review-state" : undefined}
      style={defaultContainerStyle}
    >
      <span
        className={
          variant === "statement"
            ? "statement-alert statement-alert-warning"
            : "pill warning"
        }
      >
        Needs review
      </span>
      <span
        className={helperClassName}
        style={helperStyle}
        title={normalizedReviewReason ?? "Reason unavailable."}
      >
        {normalizedReviewReason ?? "Reason unavailable."}
      </span>
      {explanation && explanation !== normalizedReviewReason ? (
        <span className={helperClassName} style={helperStyle}>
          {explanation}
        </span>
      ) : null}
      {llmReason &&
      llmReason !== normalizedReviewReason &&
      llmReason !== explanation ? (
        <span className={helperClassName} style={helperStyle}>
          Latest analyzer: {llmReason}
        </span>
      ) : null}
      {llmLogSummary ? (
        <span className={helperClassName} style={helperStyle}>
          {llmLogSummary}
        </span>
      ) : null}
    </div>
  );
}

export function ReviewQueueList({
  rows,
  currency,
}: {
  rows: Array<{
    label: string;
    amountEur: string;
    reviewReason?: unknown;
    securitySymbol?: string | null;
    transactionClass?: string | null;
  }>;
  currency: string;
}) {
  return (
    <div className="review-queue">
      {rows.map((row, index) => (
        <div className="review-queue-item" key={`${row.label}-${index}`}>
          <div className="review-queue-header">
            <div className="review-queue-title">
              <span
                className="legend-swatch"
                style={{
                  background:
                    index % 3 === 0
                      ? "var(--color-accent)"
                      : index % 3 === 1
                        ? "rgba(255,75,43,0.45)"
                        : "rgba(0,0,0,0.12)",
                }}
              />
              <span className="timeline-label">{row.label}</span>
            </div>
            <span className="timeline-amount">
              {formatCurrency(row.amountEur, currency)}
            </span>
          </div>
          {row.securitySymbol || row.transactionClass ? (
            <div className="review-queue-meta">
              {row.securitySymbol ? (
                <span className="pill">{row.securitySymbol}</span>
              ) : null}
              {row.transactionClass ? (
                <span className="pill">
                  {row.transactionClass.replace(/_/g, " ")}
                </span>
              ) : null}
            </div>
          ) : null}
          <p className="review-queue-reason">
            {formatReviewReason(row.reviewReason) ?? "Reason unavailable."}
          </p>
        </div>
      ))}
    </div>
  );
}
