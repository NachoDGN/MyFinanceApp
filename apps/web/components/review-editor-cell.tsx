"use client";

import { useRouter } from "next/navigation";
import { useState, useTransition } from "react";

import { ReviewStateCell } from "./primitives";

type ReviewEditorCellProps = {
  transactionId: string;
  needsReview: boolean;
  reviewReason?: string | null;
  manualNotes?: string | null;
  transactionClass?: string | null;
  classificationSource?: string | null;
  securitySymbol?: string | null;
  quantity?: string | null;
  llmPayload?: unknown;
};

export function ReviewEditorCell({
  transactionId,
  needsReview,
  reviewReason,
  manualNotes,
  transactionClass,
  classificationSource,
  securitySymbol,
  quantity,
  llmPayload,
}: ReviewEditorCellProps) {
  const router = useRouter();
  const [draft, setDraft] = useState(manualNotes ?? "");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const trimmedDraft = draft.trim();

  function handleUpdate() {
    if (!trimmedDraft) {
      setFeedback("Add review context before updating.");
      return;
    }

    startTransition(async () => {
      setFeedback(null);
      try {
        const response = await fetch(
          `/api/transactions/${transactionId}/review`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              reviewContext: trimmedDraft,
            }),
          },
        );
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as
            | { error?: string }
            | null;
          throw new Error(payload?.error || "Review update failed.");
        }

        setFeedback("Transaction re-reviewed.");
        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Review update failed.",
        );
      }
    });
  }

  return (
    <div style={{ display: "grid", gap: 8, minWidth: 260 }}>
      <ReviewStateCell
        needsReview={needsReview}
        reviewReason={reviewReason}
        transactionClass={transactionClass}
        classificationSource={classificationSource}
        securitySymbol={securitySymbol}
        quantity={quantity}
        llmPayload={llmPayload}
      />
      <textarea
        className="input-textarea"
        rows={3}
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
        placeholder={
          reviewReason
            ? `Explain the correction. Current review: ${reviewReason}`
            : "Add context for the next LLM review."
        }
        style={{ minWidth: 260 }}
      />
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <button
          className="btn-pill"
          type="button"
          onClick={handleUpdate}
          disabled={isPending || !trimmedDraft}
        >
          {isPending ? "Updating…" : "Update"}
        </button>
        {feedback ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            {feedback}
          </span>
        ) : null}
      </div>
    </div>
  );
}
