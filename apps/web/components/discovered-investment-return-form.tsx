"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import { recordRevolutLowRiskFundReturnAction } from "../app/actions";

type DiscoveredInvestment = {
  label: string;
  principalDisplay: string;
  currentValueDisplay: string;
  returnDisplay: string;
  returnOriginal: string;
  returnCurrency: string;
  snapshotDate: string;
  matchedTransactionCount: number;
  fundingAccountName: string;
};

export function DiscoveredInvestmentReturnForm({
  investment,
}: {
  investment: DiscoveredInvestment | null;
}) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [feedback, setFeedback] = useState<string | null>(null);

  if (!investment) {
    return null;
  }

  return (
    <section
      className="span-12"
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 16,
        padding: "8px 0",
        borderTop: "1px solid var(--color-border)",
        borderBottom: "1px solid var(--color-border)",
      }}
    >
      <div style={{ display: "grid", gap: 4, minWidth: 0 }}>
        <strong>{investment.label}</strong>
        <span className="muted">
          {investment.principalDisplay} identified from{" "}
          {investment.matchedTransactionCount} Revolut transfers · value{" "}
          {investment.currentValueDisplay}
        </span>
        {feedback ? <span className="muted">{feedback}</span> : null}
      </div>
      <form
        className="inline-actions"
        style={{ flexWrap: "nowrap", justifyContent: "flex-end" }}
        onSubmit={(event) => {
          event.preventDefault();
          const formData = new FormData(event.currentTarget);
          startTransition(async () => {
            setFeedback(null);
            try {
              await recordRevolutLowRiskFundReturnAction({
                snapshotDate: investment.snapshotDate,
                returnOriginal: String(formData.get("returnOriginal") ?? ""),
              });
              setFeedback("Return saved.");
              router.refresh();
            } catch (error) {
              setFeedback(
                error instanceof Error ? error.message : "Return not saved.",
              );
            }
          });
        }}
      >
        <label className="input-label" style={{ minWidth: 160 }}>
          Return
          <input
            className="input-field"
            name="returnOriginal"
            inputMode="decimal"
            defaultValue={investment.returnOriginal}
            placeholder={`0.00 ${investment.returnCurrency}`}
          />
        </label>
        <button className="btn-pill" type="submit" disabled={isPending}>
          {isPending ? "Saving..." : "Save"}
        </button>
      </form>
    </section>
  );
}
