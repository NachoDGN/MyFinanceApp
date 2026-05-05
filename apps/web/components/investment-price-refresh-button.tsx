"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useState, useTransition } from "react";

import { refreshOwnedPricesAction } from "../app/actions";

function formatPlural(count: number, noun: string) {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

function buildRefreshSummary(
  result: Awaited<ReturnType<typeof refreshOwnedPricesAction>>,
) {
  const pieces: string[] = [];
  if (result.refreshedCount > 0) {
    pieces.push(formatPlural(result.refreshedCount, "holding"));
  }
  if (result.refreshedFxPairs.length > 0) {
    pieces.push(formatPlural(result.refreshedFxPairs.length, "FX pair"));
  }
  return pieces.join(" and ");
}

function buildRefreshDetailSummary(
  result: Awaited<ReturnType<typeof refreshOwnedPricesAction>>,
) {
  return [
    ...result.unchangedDetails,
    ...result.skippedDetails,
    ...result.skippedFxPairs,
  ]
    .map((item) => `${item.symbol}: ${item.reason}`)
    .join(" ");
}

export function InvestmentPriceRefreshButton() {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [feedback, setFeedback] = useState<string | null>(null);

  function handleRefresh() {
    startTransition(async () => {
      setFeedback(null);
      try {
        const result = await refreshOwnedPricesAction();
        const totalTrackedHoldings =
          result.totalTrackedStocks + result.totalTrackedFunds;

        if (totalTrackedHoldings === 0 && result.totalTrackedFxPairs === 0) {
          setFeedback(
            "No tracked holdings or FX pairs are available to refresh.",
          );
          router.refresh();
          return;
        }

        if (
          result.refreshedCount === 0 &&
          result.refreshedFxPairs.length === 0
        ) {
          const detailSummary = buildRefreshDetailSummary(result);
          setFeedback(
            detailSummary
              ? `No newer prices were returned. ${detailSummary}`
              : "No newer prices were returned.",
          );
          router.refresh();
          return;
        }

        const detailSummary = buildRefreshDetailSummary(result);
        const detailNote = detailSummary ? ` ${detailSummary}` : "";
        setFeedback(
          `Updated prices for ${buildRefreshSummary(result)}.${detailNote}`,
        );

        const currentAsOf = searchParams.get("asOf");
        if (
          result.latestPriceDate &&
          (!currentAsOf || currentAsOf < result.latestPriceDate)
        ) {
          const nextParams = new URLSearchParams(searchParams.toString());
          nextParams.set("asOf", result.latestPriceDate);
          router.replace(`${pathname}?${nextParams.toString()}`);
          return;
        }

        router.refresh();
      } catch (error) {
        setFeedback(
          error instanceof Error ? error.message : "Price refresh failed.",
        );
      }
    });
  }

  return (
    <div className="investment-price-refresh">
      <button
        className="btn-pill"
        type="button"
        disabled={isPending}
        onClick={handleRefresh}
      >
        {isPending ? "Updating prices..." : "Update Prices"}
      </button>
      {feedback ? <div className="status-note">{feedback}</div> : null}
    </div>
  );
}
