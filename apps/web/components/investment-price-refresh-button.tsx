"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useState, useTransition } from "react";

import { refreshOwnedStockPricesAction } from "../app/actions";

function formatPlural(count: number, noun: string) {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

function buildRefreshSummary(result: Awaited<ReturnType<typeof refreshOwnedStockPricesAction>>) {
  const pieces: string[] = [];
  if (result.refreshedCount > 0) {
    pieces.push(formatPlural(result.refreshedCount, "holding"));
  }
  if (result.refreshedFxPairs.length > 0) {
    pieces.push(formatPlural(result.refreshedFxPairs.length, "FX pair"));
  }
  return pieces.join(" and ");
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
        const result = await refreshOwnedStockPricesAction();

        if (result.totalTrackedStocks === 0 && result.totalTrackedFxPairs === 0) {
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
          const skippedLabel =
            [...result.skippedDetails, ...result.skippedFxPairs].length > 0
              ? ` ${result.skippedDetails
                  .concat(result.skippedFxPairs)
                  .map((item) => `${item.symbol}: ${item.reason}`)
                  .join(" ")}`
              : "";
          setFeedback(
            `No fresh market data was returned.${skippedLabel}`,
          );
          router.refresh();
          return;
        }

        const skippedNote =
          result.skippedCount > 0 || result.skippedFxPairs.length > 0
            ? ` ${result.skippedDetails
                .concat(result.skippedFxPairs)
                .map((item) => `${item.symbol}: ${item.reason}`)
                .join(" ")}`
            : "";
        setFeedback(
          `Updated market data for ${buildRefreshSummary(result)}.${skippedNote}`,
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
          error instanceof Error
            ? error.message
            : "Current price refresh failed.",
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
        {isPending ? "Updating market data..." : "Update market data"}
      </button>
      {feedback ? <div className="status-note">{feedback}</div> : null}
    </div>
  );
}
